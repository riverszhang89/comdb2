/*
   Copyright 2015 Bloomberg Finance L.P.
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
 */

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <arpa/inet.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#endif

#include "db_int.h"
#include "dbinc/db_am.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/dbreg_ext.h"
#include "dbinc_auto/btree_ext.h"

#include "logmsg.h"

/*
 * __db_pgcompact --
 *  Compact page.
 *
 * PUBLIC: int __db_pgcompact __P((DB *, DB_TXN *, DBT *, double, double));
 */
int
__db_pgcompact(dbp, txn, dbt, ff, tgtff)
	DB *dbp;
	DB_TXN *txn;
	DBT *dbt;
	double ff;
	double tgtff;
{
	int ret, t_ret;
	DBC *dbc;
	DB_ENV *dbenv;
	extern int gbl_keycompr;

	dbenv = dbp->dbenv;

	if (F_ISSET(dbp, DB_AM_RECOVER))
		return EINVAL;

	/* We may get here before db is ready. If so, return (this is not an error). */
	if (gbl_keycompr && dbp->compression_flags == 0)
		return EPERM;

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "__db_cursor: %s", strerror(ret));
		return (ret);
	}

	if (dbc->dbtype == DB_BTREE) {
		/* Safeguard __bam_pgcompact(). I want to keep page compaction routine
		   private for now, so don't make it a function pointer of DB struct.  */
		ret = __bam_pgcompact(dbc, dbt, ff, tgtff);
	} else {
		__db_err(dbenv, "__db_pgcompact: %s",
				"Wrong access method or wrong cursor reference.");
		ret = EINVAL;
	}

	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	return (ret);
}

/*
 * __db_ispgcompactible --
 *	Return whether the given page is compactible
 *
 * PUBLIC: int __db_ispgcompactible __P((DB *, db_pgno_t, DBT *, double));
 */
int
__db_ispgcompactible(dbp, pgno, dbt, ff)
	DB *dbp;
	db_pgno_t pgno;
	DBT *dbt;
    double ff;
{
	int ret, t_ret;
    DBC *dbc;
	DB_ENV *dbenv;

	dbenv = dbp->dbenv;

	if ((ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0) {
		__db_err(dbenv, "__db_cursor: %s", strerror(ret));
		return (ret);
	}

    /* Keep pgcompact() to ourselves only. No function pointer
       added to the DBC struct. */
	if (dbc->dbtype == DB_BTREE) {
		ret = __bam_ispgcompactible(dbc, pgno, dbt, ff);
	} else {
		__db_err(dbenv, "__db_pgcompact: %s",
				"Wrong access method. Expect BTREE.");
		ret = EINVAL;
	}

	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static int
pgno_cmp(const void *x, const void *y)
{
	return ((*(db_pgno_t *)x) - (*(db_pgno_t *)y));
}

int __db_dump_freepages(DB *dbp, FILE *out);

/* __db_shrink_redo --
 *  sort the freelist in page order and truncate the file
 *
 * PUBLIC: int __db_shrink_redo __P((DBC *, DB_LSN *, DBMETA *, DB_LSN *, size_t, db_pgno_t *, DB_LSN *, int *));
 */
int
__db_shrink_redo(dbc, lsnp, meta, meta_lsnp, npages, pglist, pglsnlist, pmodified)
	DBC *dbc;
	DB_LSN *lsnp;
	DBMETA *meta;
	DB_LSN *meta_lsnp;
	size_t npages;
	db_pgno_t *pglist;
	DB_LSN *pglsnlist;
	int *pmodified;
{
	int ret = 0;
	size_t ii, notch;

	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB *dbp;
	DBT pgnos, lsns;
	PAGE *h;
	db_pgno_t pgno;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	*pmodified = 0;

	/* If meta page is ahead, just do the truncation. We won't need the truncated pages anymore. */
	if (log_compare(meta_lsnp, lsnp) >= 0)
		goto resize;

	qsort(pglist, npages, sizeof(db_pgno_t), pgno_cmp);

	logmsg(LOGMSG_USER, "freelist after sorting (%zu pages): ", npages);
	for (int i = 0; i != npages; ++i) {
		logmsg(LOGMSG_USER, "%u ", pglist[i]);
	}

	/*
	 * We have a sorted freelist at this point. Truncate continuous free pages that are at the end of the file. eg,
	 * A file has 6 pages (excluding meta page):
	 * 4->5->6->1->2->3 can't be truncated, because 1->2->3 are continuous but aren't at the end of the file;
	 * 1->2->3->5->4->6 can only truncate last page, for 5->4 aren't continuous;
	 * 1->2->3->4->5->6 can all be truncated, for the sequence is continuous and it's the end of the file.
	 */
	for (notch = npages, pgno = meta->last_pgno; notch > 0 && pglist[notch - 1] == pgno && pgno != PGNO_INVALID; --notch, --pgno) ;

	/* pglist[notch] is where in the freelist we can safely truncate. */
	if (notch == npages) {
		logmsg(LOGMSG_USER, "can't truncate: last free page %u last pg %u\n", pglist[notch - 1], meta->last_pgno);
		goto out;
	}

	logmsg(LOGMSG_USER, "last pgno %u truncation point (array index) %zu\n", meta->last_pgno, notch);

	/* rebuild the freelist, in page order */
	for (ii = 0; ii != notch; ++ii) {
		pgno = pglist[ii];
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto out;
		}

		if (ii == notch - 1) /* We're at the last free page. */
			NEXT_PGNO(h) = PGNO_INVALID;
		else
			NEXT_PGNO(h) = pglist[ii + 1];

		LSN(h) = *lsnp;
		if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_DIRTY)) != 0)
			goto out;
	}

	/* discard pages to be truncated from buffer pool */
	for (ii = notch; ii != npages; ++ii) {
		pgno = pglist[ii];
		if ((ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_PROBE, &h)) != 0)
			continue;
		if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_DISCARD)) != 0)
			goto out;
	}

	/* re-point the freelist to the smallest free page */
	meta->free = pglist[0];
	/* pglist[notch] is where we will truncate. so point last_pgno to the page before this one. */
	meta->last_pgno = pglist[notch] - 1;
	*pmodified = 1;

	__db_dump_freepages(dbp, stdout);
    
    /* TODO: can probably release meta page early and do truncation without holding it??? */

resize:
	/* TODO check if this page is still referenced by the oldest log file */
	/* shrink the file */
	if ((ret = __memp_resize(dbmfp, meta->last_pgno)) != 0)
		goto out;

out:
	return (ret);
}

/* __db_shrink_undo --
 *  undo
 *
 * PUBLIC: int __db_shrink_undo __P((DBC *, DB_LSN *, DBMETA *, DB_LSN *, db_pgno_t, size_t, db_pgno_t *, DB_LSN *, int *));
 */
int
__db_shrink_undo(dbc, lsnp, meta, meta_lsnp, last_pgno, npages, pglist, pglsnlist, pmodified)
	DBC *dbc;
	DB_LSN *lsnp;
	DBMETA *meta;
	DB_LSN *meta_lsnp;
	db_pgno_t last_pgno;
	size_t npages;
	db_pgno_t *pglist;
	DB_LSN *pglsnlist;
	int *pmodified;
{
	int ret;
	size_t ii;

	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB *dbp;
	PAGE *h;
	db_pgno_t pgno, next_pgno, mp_lastpg;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	logmsg(LOGMSG_USER, "freelist in its original order (%zu pages): ", npages);
	for (int i = 0; i != npages; ++i) {
		logmsg(LOGMSG_USER, "%u ", pglist[i]);
	}

	/* restore freelist */
	for (ii = 0; ii != npages; ++ii) {
		pgno = pglist[ii];
		next_pgno = (ii == npages - 1) ? PGNO_INVALID : pglist[ii + 1];
		if ((ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_CREATE, &h)) != 0)
			goto out;

		if (log_compare(lsnp, &LSN(h)) == 0 || IS_ZERO_LSN(LSN(h))) {
			P_INIT(h, dbp->pgsize, pgno, PGNO_INVALID, next_pgno, 0, P_INVALID);
			LSN(h) = pglsnlist[ii];
			if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_DIRTY)) != 0)
				goto out;
		} else if ((ret = __memp_fput(dbmfp, h, 0)) != 0) {
				goto out;
		}
	}

    /* Undo meta; If meta page is behind, just do the truncation */
    if (log_compare(meta_lsnp, lsnp) > 0) {
        meta->free = pglist[0];
    }

    /* Make sure we always have the correct last page - */
	__memp_last_pgno(dbmfp, &mp_lastpg);
	if (mp_lastpg > meta->last_pgno)
		meta->last_pgno = mp_lastpg;
	*pmodified = 1;
out:
	return (ret);
}

/*
 * __db_shrink --
 *  Shrink a database
 *
 * PUBLIC: int __db_shrink __P((DB *, DB_TXN *));
 */
int
__db_shrink(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret;
	size_t npages, pglistsz, notch, ii;
	int modified;

    DB_ENV *dbenv;
	DBC *dbc;
	DBMETA *meta;
	DB_LSN prev_metalsn;
	DB_MPOOLFILE *dbmfp;
	DB_LOCK metalock;
	PAGE *h;
	db_pgno_t pgno, *pglist, maxfreepgno;
	DB_LSN *pglsnlist;
	DBT pgnos, lsns;

    dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	LOCK_INIT(metalock);

	pgno = PGNO_BASE_MD;
	pglist = NULL;
	pglsnlist = NULL;
	modified = 0;
	memset(&pgnos, 0, sizeof(DBT));
	memset(&lsns, 0, sizeof(DBT));
	npages = 0;
	pglistsz = 16;
    maxfreepgno = PGNO_INVALID;

	/* gather a list of free pages, sort them */

	if ((ret = __os_malloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
		goto out;
	if ((ret = __os_malloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
		goto out;

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		return (ret);

	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto out;
	if ((ret = __memp_fget(dbmfp, &pgno, 0, &meta)) != 0)
		goto out;

	for (pgno = meta->free; pgno != PGNO_INVALID; ++npages) {
		if (npages == pglistsz) {
			pglistsz <<= 1;
			if ((ret =  __os_realloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
				goto out;
			if ((ret =  __os_realloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
				goto out;
		}

        if (pgno > maxfreepgno)
            maxfreepgno = pgno;

		pglist[npages] = pgno;

		/* We have metalock. No need to lock free pages. */
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0)
			goto out;

		pglsnlist[npages] = LSN(h);
		pgno = NEXT_PGNO(h);

		if ((ret = __memp_fput(dbmfp, h, 0)) != 0)
			goto out;
	}

    printf("free pg list: ");
    for (int i = 0; i != npages; ++i) {
        printf("%d ", pglist[i]);
    }
    printf("\n");

	if (npages == 0) {
        logmsg(LOGMSG_USER, "no free pages at all?\n");
		goto out;
    }

	if (maxfreepgno < meta->last_pgno) {
        logmsg(LOGMSG_USER, "no free pages at the end of the file? maxfreepgno %u last_pgno %u\n", maxfreepgno, meta->last_pgno);
		goto out;
    }

    /* log the change */
	if (!DBC_LOGGING(dbc)) {
		LSN_NOT_LOGGED(LSN(meta));
	} else {
        /* network order */
		for (ii = 0; ii != npages; ++ii) {
			pglist[ii] = htonl(pglist[ii]);
			pglsnlist[ii].file = htonl(pglsnlist[ii].file);
			pglsnlist[ii].offset = htonl(pglsnlist[ii].offset);
		}

		pgnos.size = npages * sizeof(db_pgno_t);
		pgnos.data = pglist;
		lsns.size = npages * sizeof(DB_LSN);
		lsns.data = pglsnlist;

        prev_metalsn = LSN(meta);
		ret = __db_truncate_freelist_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, meta->last_pgno, &pgnos, &lsns);
		if (ret != 0)
			goto out;

        /* host order */
        for (ii = 0; ii != npages; ++ii) {
			pglist[ii] = htonl(pglist[ii]);
			pglsnlist[ii].file = ntohl(pglsnlist[ii].file);
			pglsnlist[ii].offset = ntohl(pglsnlist[ii].offset);
		}
	}

    /* not recovery, but same routine */
	ret = __db_shrink_redo(dbc, &LSN(meta), meta, &prev_metalsn, npages, pglist, pglsnlist, &modified);

out:
	__os_free(dbenv, pglist);
	__os_free(dbenv, pglsnlist);
	if ((t_ret = __memp_fput(dbmfp, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __db_shrink_pp --
 *	DB->shrink pre/post processing.
 *
 * PUBLIC: int __db_shrink_pp __P((DB *, DB_TXN *));
 */

int
__db_shrink_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->shrink", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	/* Shrink the file. */
	ret = __db_shrink(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

/*
 * __db_swap_pages --
 *
 * PUBLIC: int __db_swap_pages __P((DB *, DB_TXN *));
 */
int
__db_swap_pages(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret, do_swap;
	u_int8_t page_type;

	DBC *dbc;
    DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno,
			  newpgno,	/* page number of the new page */
			  ppgno,	/* parent page number */
			  prefpgno;	/* the page number referenced in the parent page */
	db_indx_t prefindx; /* position of `prefpgno' in the parent page */
	PAGE *h,	/* current page */
		 *ph,	/* its previous page */
		 *nh,	/* its next page */
		 *pp,	/* its parent page */
		 *np;	/* new page */

	DB_LOCK hl,		/* page lock for current page */
			pl,		/* page lock for prev */
			nl,		/* page lock for next */
			newl;	/* page lock for new page */
	int got_hl, got_pl, got_nl, got_newl;

	DB_LSN ret_lsn, /* new lsn */
		   *nhlsn,	/* lsn of next */
		   *phlsn,	/* lsn of prev */
		   *pplsn;	/* lsn of parent */

	BTREE_CURSOR *cp;
	EPG *epg;

    dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

    h = ph = nh = pp = np = NULL;
    got_hl = got_pl = got_nl = got_newl = 0;

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto err;
	}

	for (__memp_last_pgno(dbmfp, &pgno); pgno >= 1; --pgno) {

		dbc = NULL;
		h = ph = nh = pp = np = NULL;
		nhlsn = phlsn = pplsn = NULL;
		ppgno = prefpgno = PGNO_INVALID;
		prefindx = 0;

		LOCK_INIT(hl);
		LOCK_INIT(pl);
		LOCK_INIT(nl);
		LOCK_INIT(newl);

		got_hl = got_pl = got_nl = got_newl = 0;

		if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
			__db_err(dbenv, "__db_cursor: rc %d", ret);
			goto err;
		}

		cp = (BTREE_CURSOR *)dbc->internal;

        if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, 0, &hl)) != 0) {
			__db_err(dbenv, "__db_lget(%d): rc %d", pgno, ret);
            goto err;
		}

		got_hl = 1;

        if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
            __db_pgerr(dbp, pgno, ret);
			h = NULL;
            goto err;
        }

		page_type = TYPE(h);

		/* Release the page first. We'll lock the page and its parent again. */
		if ((ret = __memp_fput(dbmfp, h, 0)) != 0) {
			__db_err(dbenv, "__memp_fput(%d): rc %d", pgno, ret);
			h = NULL;
			goto err;
		}

		got_hl = 0;
		if ((ret = __LPUT(dbc, hl)) != 0) {
			__db_err(dbenv, "__LPUT(%d): rc %d", pgno, ret);
			goto err;
		}

		/* Handle only internal and leaf nodes */
		if (page_type != P_LBTREE && page_type != P_IBTREE)
			continue;

		/* Allocate a page from the freelist */
		if ((ret = __db_new_ex(dbc, page_type, &np, 1)) != 0) {
			__db_err(dbenv, "__db_new: rc %d", ret);
			goto err;
		}

		if (np == NULL) {
			logmsg(LOGMSG_INFO, "%s: no free page available at this moment", __func__);
			goto err;
		}

		if (PGNO(np) > pgno) {
			/*
			 * The new page unfortunately has a higher page number than our page,
			 * Since we're scanning from the back of the file, the next page we would look
			 * is even lower-numbered. Just exit.
			 */
			if ((ret = __db_free(dbc, np)) != 0)
				goto err;
		}

		/* Grab a wlock on the new page */
		if ((ret = __db_lget(dbc, 0, PGNO(np), DB_LOCK_WRITE, 0, &newl)) != 0) {
			__db_err(dbenv, "__db_lget(%d): rc %d", PGNO(np), ret);
			goto err;
		}
		got_newl = 1;

		/* lock the page and its parent */
		if ((ret = __bam_locate_page(dbc, pgno)) != 0) {
			__db_err(dbenv, "__bam_locate_page(%d): rc %d", pgno, ret);
			goto err;
		}

		if (cp->sp != cp->csp) { /* Have a parent page */
			epg = &cp->csp[-1];
			pp = epg->page;
			ppgno = PGNO(pp);
			pplsn = &LSN(pp);
			prefindx = epg->indx;
			prefpgno = GET_BINTERNAL(dbc->dbp, pp, prefindx)->pgno;
		}

		h = cp->csp->page;
		/* Now grab prev and next */
		if (h->prev_pgno != PGNO_INVALID) {
			if ((ret = __db_lget(dbc, 0, h->prev_pgno, DB_LOCK_WRITE, 0, &pl)) != 0)
				goto err;
			got_pl = 1;
			if ((ret = __memp_fget(dbmfp, &h->prev_pgno, 0, &ph)) != 0) {
				ret = __db_pgerr(dbp, h->prev_pgno, ret);
				ph = NULL;
				goto err;
			}
			phlsn = &LSN(ph);
		}

		if (h->next_pgno != PGNO_INVALID) {
			if ((ret = __db_lget(dbc, 0, h->next_pgno, DB_LOCK_WRITE, 0, &nl)) != 0)
				goto err;
			got_nl = 1;
			if ((ret = __memp_fget(dbmfp, &h->next_pgno, 0, &nh)) != 0) {
				nh = NULL;
				ret = __db_pgerr(dbp, h->next_pgno, ret);
				goto err;
			}
			nhlsn = &LSN(nh);
		}

		if (DBC_LOGGING(dbc)) {
			ret = __db_pg_swap_log(dbp, txn, &ret_lsn, 0,
					PGNO(h), &LSN(h), /* current page */
					h->next_pgno, nhlsn, /* sibling page, if any */
					h->prev_pgno, phlsn, /* sibling page, if any */
					ppgno, pplsn, /* parent page */
					prefpgno, prefindx, /* old page number and its position in parent page */
					PGNO(np), &LSN(np) /* new page to swap with */);
			/* TODO FIXME XXX handle newsnapisol pglog??? */
		} else {
			LSN_NOT_LOGGED(ret_lsn);
		}

		LSN(h) = ret_lsn;
		LSN(np) = ret_lsn;
		if (nh != NULL)
			LSN(nh) = ret_lsn;
		if (ph != NULL)
			LSN(ph) = ret_lsn;
		if (pp != NULL)
			LSN(pp) = ret_lsn;

		/* copy content to new page and fix pgno */
		newpgno = PGNO(np);
		memcpy(np, h, dbp->pgsize);
		PGNO(np) = newpgno;

		if ((ret = __memp_fset(dbmfp, np, DB_MPOOL_DIRTY)) != 0) {
			__db_err(dbenv, "__memp_fset(%d): rc %d", newpgno, ret);
			goto err;
		}

		/* empty old page, this ensures that we call into the right version of db_free */
		HOFFSET(h) = dbp->pgsize;
		NUM_ENT(h) = 0;

		/* free old page */
		if ((ret = __db_free(dbc, h)) != 0)
			goto err;

		/* relink next */
		if (nh != NULL) {
			nh->prev_pgno = newpgno;
			if ((ret = __memp_fput(dbmfp, nh, DB_MPOOL_DIRTY)) != 0) {
				__db_err(dbenv, "__memp_fput(%d): rc %d", PGNO(nh), ret);
				nh = NULL;
				goto err;
			}
			got_nl = 0;
			if ((ret = __TLPUT(dbc, nl)) != 0) {
				__db_err(dbenv, "__TLPUT(%d): rc %d", PGNO(nh), ret);
				goto err;
			}
		}

		/* relink prev */
		if (ph != NULL) {
			ph->next_pgno = newpgno;
			if ((ret = __memp_fput(dbmfp, ph, DB_MPOOL_DIRTY)) != 0) {
				__db_err(dbenv, "__memp_fput(%d): rc %d", PGNO(ph), ret);
				ph = NULL;
				goto err;
			}
			got_pl = 0;
			if ((ret = __TLPUT(dbc, pl)) != 0) {
				__db_err(dbenv, "__TLPUT(%d): rc %d", PGNO(ph), ret);
				goto err;
			}
		}

		/* update parent */
		if (cp->sp != cp->csp)
			GET_BINTERNAL(dbc->dbp, pp, prefindx)->pgno = newpgno;

		if ((ret = __memp_fset(dbmfp, pp, DB_MPOOL_DIRTY)) != 0) {
			__db_err(dbenv, "__memp_fset(%d): rc %d", PGNO(pp), ret);
			goto err;
		}

		/*
		 * Swap in the new page so that __bam_stkrel will put it back.
		 * We still retain the page lock of the old page in the cursor
		 * stack, and __bam_stkrel will take care of that lock too.
		 * We put the page lock of the new page here.
		 */
		cp->csp->page = np;
		got_newl = 0;
		if ((ret = __TLPUT(dbc, newl)) != 0) {
			__db_err(dbenv, "__TLPUT(%d): rc %d", newpgno, ret);
			goto err;
		}
		if ((ret = __bam_stkrel(dbc, STK_NOLOCK)) != 0) {
			__db_err(dbenv, "__bam_stkrel(): rc %d", ret);
			goto err;
		}
		if ((ret = __db_c_close(dbc)) != 0) {
			__db_err(dbenv, "__db_c_close(): rc %d", ret);
			dbc = NULL;
			goto err;
		}
	}

err:
	if (h != NULL && (t_ret = __memp_fput(dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_hl && (t_ret = __TLPUT(dbc, hl)) != 0 && ret == 0)
		ret = t_ret;
	if (nh != NULL && (t_ret = __memp_fput(dbmfp, nh, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_nl && (t_ret = __TLPUT(dbc, nl)) != 0 && ret == 0)
		ret = t_ret;
	if (ph != NULL && (t_ret = __memp_fput(dbmfp, ph, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_pl && (t_ret = __TLPUT(dbc, pl)) != 0 && ret == 0)
		ret = t_ret;
	if (np != NULL && (t_ret = __memp_fput(dbmfp, np, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_newl && (t_ret = __TLPUT(dbc, newl)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * __db_swap_pages_pp --
 *	DB->swap_pages pre/post processing.
 *
 * PUBLIC: int __db_swap_pages_pp __P((DB *, DB_TXN *));
 */

int
__db_swap_pages_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->swap_pages", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	ret = __db_swap_pages(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}
