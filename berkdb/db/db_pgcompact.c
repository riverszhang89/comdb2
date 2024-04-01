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

/* __db_rebuild_freelist_redo --
 *  sort the freelist in page order and truncate the file
 *
 * PUBLIC: int __db_rebuild_freelist_redo __P((DBC *, DB_LSN *, DBMETA *, DB_LSN *, db_pgno_t, size_t, db_pgno_t *, DB_LSN *, int *));
 */
int
__db_rebuild_freelist_redo(dbc, lsnp, meta, meta_lsnp, endpgno, npages, pglist, pglsnlist, pmodified)
	DBC *dbc;
	DB_LSN *lsnp;
	DBMETA *meta;
	DB_LSN *meta_lsnp;
	db_pgno_t endpgno;
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

#if 0
	/* If meta page is ahead, just do the truncation. No need to restore freelist.
     * If there's any pg_alloc between this LSN and meta page's LSN, pg_alloc_recover()
     * will extend the file */
	if (log_compare(meta_lsnp, lsnp) >= 0)
		goto resize;
#endif

	qsort(pglist, npages, sizeof(db_pgno_t), pgno_cmp);

	logmsg(LOGMSG_USER, "freelist after sorting (%zu pages): ", npages);
	for (int i = 0; i != npages; ++i) {
		logmsg(LOGMSG_USER, "%u ", pglist[i]);
	}
	logmsg(LOGMSG_USER, "\n");

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
	}

	logmsg(LOGMSG_USER, "last pgno %u truncation point (array index) %zu pgno %u\n", meta->last_pgno, notch, pglist[notch]);

	/* rebuild the freelist, in page order */
	for (ii = 0; ii != notch; ++ii) {
		pgno = pglist[ii];
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto out;
		}

		NEXT_PGNO(h) = (ii == notch - 1) ? endpgno : pglist[ii + 1];

		LSN(h) = *lsnp;
		if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_DIRTY)) != 0)
			goto out;
	}

	/* Discard pages to be truncated from buffer pool */
	if (endpgno == PGNO_INVALID) {
		for (ii = notch; ii != npages; ++ii) {
			pgno = pglist[ii];
			if ((ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_PROBE, &h)) != 0)
				continue;
			/* mark clean & discard. we don't want memp_sync to write the page after we truncate */
			if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_CLEAN | DB_MPOOL_DISCARD)) != 0)
				goto out;
		}
	}

	/* Re-point the freelist to the smallest free page passed to us.
     * However if we successfully truncate all pages in this range (yay!),
     * point the freelist to the first page after this range */
    meta->free = notch > 0 ? pglist[0] : endpgno;
	*pmodified = 1;

	if (notch < npages && endpgno == PGNO_INVALID) {
		/* pglist[notch] is where we will truncate. so point last_pgno to the page before this one. */
		meta->last_pgno = pglist[notch] - 1;
//resize:
		/* TODO check if this page is still referenced by the oldest log file */
		if ((ret = __memp_resize(dbmfp, meta->last_pgno)) != 0)
			goto out;
	}

out:
	return (ret);
}

/* __db_rebuild_freelist_undo --
 *  undo
 *
 * PUBLIC: int __db_rebuild_freelist_undo __P((DBC *, DB_LSN *, DBMETA *, DB_LSN *, db_pgno_t, size_t, db_pgno_t *, DB_LSN *, int *));
 */
int
__db_rebuild_freelist_undo(dbc, lsnp, meta, meta_lsnp, last_pgno, npages, pglist, pglsnlist, pmodified)
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

    /* Undo meta if it's ahead */
    if (log_compare(meta_lsnp, lsnp) > 0) {
        meta->free = pglist[0];
    }

	meta->last_pgno = last_pgno;

#if 0
    /* Make sure we always have the correct last page */
	__memp_last_pgno(dbmfp, &mp_lastpg);
	if (mp_lastpg > meta->last_pgno)
		meta->last_pgno = mp_lastpg;
#endif

	*pmodified = 1;
out:
	return (ret);
}

/*
 * __db_rebuild_freelist --
 *  Shrink a database
 *
 * PUBLIC: int __db_rebuild_freelist __P((DB *, DB_TXN *));
 */
int
__db_rebuild_freelist(dbp, txn)
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

    logmsg(LOGMSG_USER, "%s: freelist before sorting (%zu pages): ", __func__, npages);
	for (int i = 0; i != npages; ++i) {
		logmsg(LOGMSG_USER, "%u ", pglist[i]);
	}
	logmsg(LOGMSG_USER, "\n");

	if (npages == 0) {
        logmsg(LOGMSG_USER, "%s: no free pages at all?\n", __func__);
		goto out;
    }

	if (maxfreepgno < meta->last_pgno) {
        logmsg(LOGMSG_WARN, "%s: no free pages at the end of the file? maxfreepgno %u last_pgno %u\n",
				__func__, maxfreepgno, meta->last_pgno);
        logmsg(LOGMSG_WARN, "%s: only going to sort freelist\n", __func__);
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
		ret = __db_rebuild_freelist_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, meta->last_pgno, pgno, &pgnos, &lsns);
		if (ret != 0)
			goto out;

        /* host order */
        for (ii = 0; ii != npages; ++ii) {
			pglist[ii] = htonl(pglist[ii]);
			pglsnlist[ii].file = ntohl(pglsnlist[ii].file);
			pglsnlist[ii].offset = ntohl(pglsnlist[ii].offset);
		}
	}

    /* not in recovery, just reusing the same function */
	ret = __db_rebuild_freelist_redo(dbc, &LSN(meta), meta, &prev_metalsn, pgno, npages, pglist, pglsnlist, &modified);

	logmsg(LOGMSG_USER, "%s: my LSN is now: %d:%d", __func__, LSN(meta).file, LSN(meta).offset);

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
 * __db_rebuild_freelist_pp --
 *	DB->rebuild_freelist pre/post processing.
 *
 * PUBLIC: int __db_rebuild_freelist_pp __P((DB *, DB_TXN *));
 */

int
__db_rebuild_freelist_pp(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	DB_ENV *dbenv;
	int handle_check, ret;

	dbenv = dbp->dbenv;

	PANIC_CHECK(dbenv);

	if (!F_ISSET(dbp, DB_AM_OPEN_CALLED)) {
		ret = __db_mi_open(dbenv, "DB->rebuild_freelist", 0);
		return (ret);
	}

	/* Check for consistent transaction usage. */
	if ((ret = __db_check_txn(dbp, txn, DB_LOCK_INVALIDID, 0)) != 0)
		return (ret);

	handle_check = IS_REPLICATED(dbenv, dbp);
	if (handle_check && (ret = __db_rep_enter(dbp, 1, 0)) != 0)
		return (ret);

	/* Shrink the file. */
	ret = __db_rebuild_freelist(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

int gbl_max_num_pages_swapped_per_txn = 10;

/*
 * __db_swap_pages --
 *
 * PUBLIC: int __db_swap_pages __P((DB *, DB_TXN *));
 */
int
__db_pgswap(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret, ii;
	int pglvl, unused;
	u_int8_t page_type;

	DBC *dbc;
    DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno,
			  cpgno,	/* current page number when descending the btree */
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
	
	int num_pages_swapped = 0;
	int max_num_pages_swapped = gbl_max_num_pages_swapped_per_txn;
	PAGE **lfp; /* list of pages to be placed on freelist */
	db_pgno_t *lpgnofromfl; /* list of page numbers swapped in from freelist*/

	DB_LSN ret_lsn, /* new lsn */
		   *nhlsn,	/* lsn of next */
		   *phlsn,	/* lsn of prev */
		   *pplsn;	/* lsn of parent */

	BTREE_CURSOR *cp;
	EPG *epg;

    DBT hdr, dta, firstkey;

    dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	dbc = NULL;

    h = ph = nh = pp = np = NULL;
    got_hl = got_pl = got_nl = got_newl = 0;

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto err;
	}

	if ((ret = __os_calloc(dbenv, max_num_pages_swapped, sizeof(PAGE *), &lfp)) != 0) {
		__db_err(dbenv, "%s: __os_malloc: rc %d", __func__, ret);
		goto err;
	}

	if ((ret = __os_calloc(dbenv, max_num_pages_swapped, sizeof(db_pgno_t), &lpgnofromfl)) != 0) {
		__db_err(dbenv, "%s: __os_malloc: rc %d", __func__, ret);
		goto err;
	}

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "%s: __db_cursor: rc %d", __func__, ret);
		goto err;
	}
	cp = (BTREE_CURSOR *)dbc->internal;

	for (__memp_last_pgno(dbmfp, &pgno); pgno >= 1; --pgno) {

		if (h != NULL) {
			cpgno = PGNO(h);
			ret = __memp_fput(dbmfp, h, 0);
			h = NULL;
			if (ret != 0) {
				__db_err(dbenv, "%s: __memp_fput(%d): rc %d", __func__, cpgno, ret);
				goto err;
			}
		}

		if (got_hl) {
			got_hl = 0;
			if ((ret = __LPUT(dbc, hl)) != 0) {
				__db_err(dbenv, "%s: __LPUT(%d): rc %d", __func__, cpgno, ret);
				goto err;
			}
		}

		logmsg(LOGMSG_USER, "%s: ------ checking pgno %u ------ \n", __func__, pgno);

		h = ph = nh = pp = np = NULL;
		nhlsn = phlsn = pplsn = NULL;
		ppgno = prefpgno = PGNO_INVALID;
		prefindx = 0;

		LOCK_INIT(hl);
		LOCK_INIT(pl);
		LOCK_INIT(nl);
		LOCK_INIT(newl);

		got_hl = got_pl = got_nl = got_newl = 0;

		if (bsearch(&pgno, lpgnofromfl, num_pages_swapped, sizeof(db_pgno_t), pgno_cmp) != NULL) {
			logmsg(LOGMSG_USER, "pgno %u was just swapped in from freelist, skipping it\n", pgno);
			continue;
		}

		if (num_pages_swapped >= max_num_pages_swapped) {
			logmsg(LOGMSG_USER, "have enough pages to be freed %d\n", num_pages_swapped);
			break;
		}

        if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, 0, &hl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
            goto err;
		}

		got_hl = 1;

        if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
            __db_pgerr(dbp, pgno, ret);
			h = NULL;
            goto err;
        }

		page_type = TYPE(h);

		/* Handle only internal and leaf pages
         * TODO XXX FIXME overflow pages? */
		if (page_type != P_LBTREE && page_type != P_IBTREE) {
			if (page_type != P_INVALID)
				logmsg(LOGMSG_USER, "unsupported page type %d\n", page_type);
			else
				logmsg(LOGMSG_USER, "page already empty\n");
			continue;
		}

		/*
		 * Try allocating a page from the freelist, without extending the file.
		 *
		 * We're holding onto pages to be freed, so the last pgno on the meta page,
		 * as well as the one from the bufferpool do not change. If we run out of
		 * free pages here, a normal __db_new() call would then extend the file
		 * by one more page, leaving a hole in the file after those pages are freed.
		 * Hence it's very important that we do not extend the file here.
		 */
		if ((ret = __db_new_ex(dbc, page_type, &np, 1)) != 0) {
			__db_err(dbenv, "__db_new: rc %d", ret);
			goto err;
		}

		if (np == NULL) {
			logmsg(LOGMSG_INFO, "%s: no free page available", __func__);
			goto err;
		}

		logmsg(LOGMSG_USER, "using new pgno %u\n", PGNO(np));

		if (PGNO(np) > pgno) {
			logmsg(LOGMSG_USER, "greater free page number!\n");
			/*
			 * The new page unfortunately has a higher page number than our page,
			 * Since we're scanning backwards from the back of the file, the next
             * page will be even lower-numbered. It makes no sense to continue.
			 */
			ret = __db_free(dbc, np);
			np = NULL;
			goto err;
		}

		/* Grab a wlock on the new page */
		if ((ret = __db_lget(dbc, 0, PGNO(np), DB_LOCK_WRITE, 0, &newl)) != 0) {
			__db_err(dbenv, "__db_lget(%u): rc %d", PGNO(np), ret);
			goto err;
		}
		got_newl = 1;

		memset(&firstkey, 0, sizeof(DBT));
		pglvl = LEVEL(h);

		/* descend from pgno till we hit a non-internal page */
		while (ret == 0 && ISINTERNAL(h)) {
			cpgno = GET_BINTERNAL(dbp, h, 0)->pgno;
			ret = __memp_fput(dbmfp, h, 0);
			h = NULL;
			if (ret != 0) {
				__db_err(dbenv, "__memp_fput(%u): rc %d", cpgno, ret);
				goto err;
			}

			got_hl = 0;
			if ((ret = __LPUT(dbc, hl)) != 0) {
				__db_err(dbenv, "__LPUT(%u): rc %d", cpgno, ret);
				goto err;
			}

			if ((ret = __db_lget(dbc, 0, cpgno, DB_LOCK_READ, 0, &hl)) != 0) {
				__db_err(dbenv, "__db_lget(%u): rc %d", cpgno, ret);
				goto err;
			}
			got_hl = 1;

			if ((ret = __memp_fget(dbmfp, &cpgno, 0, &h)) != 0) {
				__db_pgerr(dbp, cpgno, ret);
				h = NULL;
				goto err;
			}
		}

		if (ret != 0)
			goto err;
		if (!ISLEAF(h))
			goto err;
		if ((ret = __db_ret(dbp, h, 0, &firstkey, &firstkey.data, &firstkey.ulen)) != 0)
			goto err;
        if ((ret = __bam_search(dbc, PGNO_INVALID, &firstkey, S_WRITE | S_PARENT, pglvl, NULL, &unused)) != 0)
			goto err;

		/* Release my reference to this page, for __bam_search() pins the page */
		cpgno = PGNO(h);
		ret = __memp_fput(dbmfp, h, 0);
		h = NULL;
		if (ret != 0) {
			__db_err(dbenv, "__memp_fput(%u): rc %d", cpgno, ret);
			goto err;
		}

		got_hl = 0;
		if ((ret = __LPUT(dbc, hl)) != 0) {
			__db_err(dbenv, "__LPUT(%u): rc %d", cpgno, ret);
			goto err;
		}

		if (cp->sp != cp->csp) { /* Have a parent page */
			epg = &cp->csp[-1];
			pp = epg->page;
			ppgno = PGNO(pp);
			pplsn = &LSN(pp);
			prefindx = epg->indx;
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

        /* Get old page header and data. */
		memset(&hdr, 0, sizeof(DBT));
		memset(&dta, 0, sizeof(DBT));

		hdr.data = h;
		hdr.size = LOFFSET(dbp, h);
		dta.data = (u_int8_t *)h + HOFFSET(h);
		dta.size = dbp->pgsize - HOFFSET(h);

		if (DBC_LOGGING(dbc)) {
			ret = __db_pg_swap_log(dbp, txn, &ret_lsn, 0,
					PGNO(h), &LSN(h), &hdr, &dta, /* old page */
					h->next_pgno, nhlsn, /* sibling page, if any */
					h->prev_pgno, phlsn, /* sibling page, if any */
					ppgno, pplsn, prefindx, /* parent page, if any */
					PGNO(np), &LSN(np) /* new page to swap with */);
			/* TODO FIXME XXX handle newsnapisol pglog??? */
		} else {
			LSN_NOT_LOGGED(ret_lsn);
		}


        logmsg(LOGMSG_USER, "%s: my LSN is now: %d:%d", __func__, ret_lsn.file, ret_lsn.offset);

        /* update LSN */
		LSN(h) = ret_lsn;
		LSN(np) = ret_lsn;
		if (nh != NULL)
			LSN(nh) = ret_lsn;
		if (ph != NULL)
			LSN(ph) = ret_lsn;
		if (pp != NULL)
			LSN(pp) = ret_lsn;

		logmsg(LOGMSG_USER, "swapping pgno %u with %u\n", PGNO(h), PGNO(np));

		/* copy content to new page and fix pgno */
		newpgno = PGNO(np);
		memcpy(np, h, dbp->pgsize);
		PGNO(np) = newpgno;

		if ((ret = __memp_fset(dbmfp, np, DB_MPOOL_DIRTY)) != 0) {
			__db_err(dbenv, "__memp_fset(%u): rc %d", newpgno, ret);
			goto err;
		}

		/* empty old page, this ensures that we call into the right version of db_free */
		HOFFSET(h) = dbp->pgsize;
		NUM_ENT(h) = 0;

        /* Place the page on the to-be-freed list, that gets freed after the while loop.
         * It ensures higher page numbers won't be placed on the front of the list. */
		lfp[num_pages_swapped] = h;
		h = NULL;

		lpgnofromfl[num_pages_swapped] = newpgno;
		qsort(lpgnofromfl, num_pages_swapped + 1, sizeof(db_pgno_t), pgno_cmp);
		++num_pages_swapped;

		/* relink next */
		if (nh != NULL) {
			logmsg(LOGMSG_USER, "relinking pgno %u to %u\n", PGNO(nh), newpgno);
			nh->prev_pgno = newpgno;
			if ((ret = __memp_fput(dbmfp, nh, DB_MPOOL_DIRTY)) != 0) {
				__db_err(dbenv, "__memp_fput(%u): rc %d", PGNO(nh), ret);
				nh = NULL;
				goto err;
			}
			got_nl = 0;
			if ((ret = __TLPUT(dbc, nl)) != 0) {
				__db_err(dbenv, "__TLPUT(%u): rc %u", PGNO(nh), ret);
				goto err;
			}
		}

		/* relink prev */
		if (ph != NULL) {
			logmsg(LOGMSG_USER, "relinking pgno %u to %u\n", PGNO(ph), newpgno);
			ph->next_pgno = newpgno;
			if ((ret = __memp_fput(dbmfp, ph, DB_MPOOL_DIRTY)) != 0) {
				__db_err(dbenv, "__memp_fput(%u): rc %d", PGNO(ph), ret);
				ph = NULL;
				goto err;
			}
			got_pl = 0;
			if ((ret = __TLPUT(dbc, pl)) != 0) {
				__db_err(dbenv, "__TLPUT(%u): rc %d", PGNO(ph), ret);
				goto err;
			}
		}

		/* update parent */
		if (cp->sp != cp->csp) {
			logmsg(LOGMSG_USER, "update parent %u reference to %u\n", PGNO(pp), newpgno);
			GET_BINTERNAL(dbp, pp, prefindx)->pgno = newpgno;

            if ((ret = __memp_fset(dbmfp, pp, DB_MPOOL_DIRTY)) != 0) {
                __db_err(dbenv, "__memp_fset(%u): rc %d", PGNO(pp), ret);
                goto err;
            }
        }

		/*
		 * Old page is already freed, swap in the new page.
         * We still retain the the old page's lock
         * in the cursor stack, and __bam_stkrel will take care of
         * that lock. We release the new page's lock here.
		 */
		cp->csp->page = np;
		np = NULL;
		got_newl = 0;
		if ((ret = __TLPUT(dbc, newl)) != 0) {
			__db_err(dbenv, "__TLPUT(%u): rc %d", newpgno, ret);
			goto err;
		}
		if ((ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0) {
			__db_err(dbenv, "__bam_stkrel(): rc %d", ret);
			goto err;
		}
	}

err:
	/*
	 * The list is most likely sorted in a descending order of pgno,
	 * for we scanned the file backwards. Free pages from the head of
	 * the list (ie from the largest pgno), so that smaller pages
	 * are placed on the front of the freelist.
	 */
	logmsg(LOGMSG_USER, "num_pages_swapped %u\n", num_pages_swapped);
	for (ii = 0; ii != num_pages_swapped; ++ii) {
		if ((ret = __db_free(dbc, lfp[ii])) != 0) {
			logmsg(LOGMSG_USER, "__db_free %u\n", PGNO(lfp[ii]));
			__db_err(dbenv, "__db_free(%u): rc %d", PGNO(lfp[ii]), ret);
			goto err;
		}
	}

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
 * __db_pgswap_pp --
 *     DB->pgswap pre/post processing.
 *
 * PUBLIC: int __db_pgswap_pp __P((DB *, DB_TXN *));
 */
int
__db_pgswap_pp(dbp, txn)
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

       ret = __db_pgswap(dbp, txn);

       if (handle_check)
               __db_rep_exit(dbenv);
       return (ret);
}
