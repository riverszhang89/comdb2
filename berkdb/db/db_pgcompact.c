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

int gbl_pgmv_verbose = 1;
int gbl_unsafe_db_resize = 1;
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
	int ret, t_ret, force_ckp;
	size_t npages, pglistsz, notch, ii;
	int modified;

	DB_ENV *dbenv;
	DBC *dbc;
	DBMETA *meta;
	DB_LSN prev_metalsn;
	DB_MPOOLFILE *dbmfp;
	DB_LOCK metalock;
	PAGE *h;
	db_pgno_t pgno, *pglist, maxfreepgno, endpgno;
	DB_LSN *pglsnlist;
	DBT pgnos, lsns;

	DB_LOGC *logc = NULL;
	DB_LSN firstlsn;
	DBT firstlog;
	int is_too_young;

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
	pglistsz = 16; /* initial size */
	maxfreepgno = PGNO_INVALID;
	force_ckp = 0;

	if (gbl_unsafe_db_resize) {
		logmsg(LOGMSG_WARN, "%s: unsafe_db_resize is enabled! full-recovery may not work!\n", __func__);
		logmsg(LOGMSG_WARN, "%s: flushing bufferpool\n", __func__);
		(void)__txn_checkpoint(dbenv, 0, 0, DB_FORCE);
	}

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

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: collecting free pages\n", __func__);
	}

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

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: %zu free pages collected:\n", __func__, npages);
		for (int i = 0; i != npages; ++i) {
			logmsg(LOGMSG_WARN, "%u ", pglist[i]);
		}
		logmsg(LOGMSG_WARN, "\n");
	}

	if (npages == 0) {
		logmsg(LOGMSG_WARN, "%s: no free pages. there is nothing for us to do\n", __func__);
		goto out;
	}

	if (gbl_pgmv_verbose && maxfreepgno < meta->last_pgno) {
		logmsg(LOGMSG_WARN, "%s: no free pages at the end of the file. maxfreepgno %u last_pgno %u\n",
				__func__, maxfreepgno, meta->last_pgno);
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

	endpgno = pgno;
	qsort(pglist, npages, sizeof(db_pgno_t), pgno_cmp);

	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_WARN, "%s: freelist after sorting (%zu pages):\n", __func__, npages);
		for (int i = 0; i != npages; ++i) {
			logmsg(LOGMSG_WARN, "%u ", pglist[i]);
		}
		logmsg(LOGMSG_WARN, "\n");
	}

	/*
	 * Get the first log record. Do not truncate a page if it's LSN is greater than
	 * the first log record. It's okay if the actual first log record in the
	 * system advances after this.
	 */
	memset(&firstlog, 0, sizeof(DBT));
	firstlog.flags = DB_DBT_MALLOC;
	ret = dbenv->log_cursor(dbenv, &logc, 0);
	if (ret) {
		__db_err(dbenv, "log_cursor error %d\n", ret);
		goto out;
	}
	ret = logc->get(logc, &firstlsn, &firstlog, DB_FIRST);
	if (ret) {
		__db_err(dbenv, "log_c_get(FIRST) error %d\n", ret);
		goto out;
	}
	__os_free(dbenv, firstlog.data);

	/* Walk the file backwards, and find out where we can safely truncate */
	for (notch = npages, pgno = meta->last_pgno; notch > 0 && pglist[notch - 1] == pgno && pgno != PGNO_INVALID; --notch, --pgno) {
		if (gbl_unsafe_db_resize)
			continue;

		/*
		 * Chech if page LSN is still accessible.
		 * We can't safely truncate the page unless
		 * it's no longer referenced in the log
		 */
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto out;
		}
		is_too_young = (LSN(h).file >= firstlsn.file);
		if ((ret = __memp_fput(dbmfp, h, 0)) != 0)
			goto out;

		if (is_too_young) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: %u is too young\n", __func__, pgno);
			}
			/* This is the pgno that we can truncate to, at most. No point continuing. */
			break;
		}
	}

	/* pglist[notch] is where in the freelist we can safely truncate. */
	if (gbl_pgmv_verbose) {
		if (notch == npages) {
			logmsg(LOGMSG_WARN, "%s: can't truncate: last free page %u last pg %u\n",
					__func__, pglist[notch - 1], meta->last_pgno);
		} else {
			logmsg(LOGMSG_WARN, "%s: last pgno %u truncation point (array index) %zu pgno %u\n",
					__func__, meta->last_pgno, notch, pglist[notch]);
		}
	}

	/* rebuild the freelist, in page order */
	for (ii = 0; ii != notch; ++ii) {
		pgno = pglist[ii];
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			goto out;
		}

		NEXT_PGNO(h) = (ii == notch - 1) ? endpgno : pglist[ii + 1];

		LSN(h) = LSN(meta);
		if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_DIRTY)) != 0)
			goto out;
	}

	/* Discard pages to be truncated from buffer pool */
	for (ii = notch; ii != npages; ++ii) {
		pgno = pglist[ii];
		/*
		 * Probe the page. If it's paged in already, mark the page clean and
		 * discard it we don't want memp_sync to accidentally flush the page
		 * after we truncate, which would create a hole in the file.
		 */
		if ((ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_PROBE, &h)) != 0)
			continue;
		if ((ret = __memp_fput(dbmfp, h, DB_MPOOL_CLEAN | DB_MPOOL_DISCARD)) != 0)
			goto out;
	}

	/*
	 * Re-point the freelist to the smallest free page passed to us.
	 * If all pages in this range can be truncated, instead, point
	 * the freelist to the first free page after this range. It can
	 * be PGNO_INVALID if there is no more free page after this range.
	 */
	meta->free = notch > 0 ? pglist[0] : endpgno;
	modified = 1;

	if (notch < npages) {

		if (!DBC_LOGGING(dbc)) {
			LSN_NOT_LOGGED(LSN(meta));
		} else {
			ret = __db_resize_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, meta->last_pgno, pglist[notch] - 1);
			if (ret != 0)
				goto out;
		}

		/* pglist[notch] is where we will truncate. so point last_pgno to the page before this one. */
		meta->last_pgno = pglist[notch] - 1;

		/* also makes bufferpool aware */
		if ((ret = __memp_resize(dbmfp, meta->last_pgno)) != 0)
			goto out;

		if (gbl_unsafe_db_resize) {
			force_ckp = 1;
		}
	}

out: __os_free(dbenv, pglist);
	__os_free(dbenv, pglsnlist);
	if ((t_ret = __memp_fput(dbmfp, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;

	if (ret == 0 && force_ckp) {
		logmsg(LOGMSG_WARN,
				"%s: unsafe_db_resize is enabled! Forcing a checkpoint so that it's less likely for recovery ",
				"to start recovering from a place where truncated pages are still referenced\n",
				__func__);
		(void)__txn_checkpoint(dbenv, 0, 0, DB_FORCE);
	}

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

int gbl_max_num_pages_swapped_per_txn = 100;
int gbl_only_process_pages_in_bufferpool = 1;

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

	/* Walk the file backwards and swap pages with a lower-numbered free page */
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

		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_USER, "%s: checking PAGE %u\n", __func__, pgno);
		}

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
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: pgno %u was just swapped in from freelist, skip it\n", __func__, pgno);
			}
			continue;
		}

		if (num_pages_swapped >= max_num_pages_swapped) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: have enough pages to be freed %d max %d\n", __func__,
						num_pages_swapped, max_num_pages_swapped);
			}
			break;
		}

		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, 0, &hl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
			goto err;
		}

		got_hl = 1;

		if (gbl_only_process_pages_in_bufferpool) {
			ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_PROBE, &h);
			if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
				if (gbl_pgmv_verbose) {
					logmsg(LOGMSG_WARN, "%s: pgno %u not found in bufferpool\n", __func__, pgno);
				}
				h = NULL;
				continue;
			}
		} else if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0) {
			__db_pgerr(dbp, pgno, ret);
			h = NULL;
			goto err;
		}

		page_type = TYPE(h);

		/* Handle only internal and leaf pages
		 * TODO XXX FIXME overflow pages? */
		if (page_type != P_LBTREE && page_type != P_IBTREE) {
			if (page_type != P_INVALID)
				logmsg(LOGMSG_WARN, "%s: unsupported page type %d\n", __func__, page_type);
			else if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: page already free\n", __func__);
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
			logmsg(LOGMSG_INFO, "%s: free list is empty\n", __func__);
			goto err;
		}

		if (gbl_pgmv_verbose)
			logmsg(LOGMSG_WARN, "%s: use free pgno %u\n", __func__, PGNO(np));

		if (PGNO(np) > pgno) {
			if (gbl_pgmv_verbose)
				logmsg(LOGMSG_WARN, "%s: free page number is greater than this page!\n", __func__);
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
			if (ret != 0)
				goto err;
		} else {
			LSN_NOT_LOGGED(ret_lsn);
		}

		/* update LSN */
		LSN(h) = ret_lsn;
		LSN(np) = ret_lsn;
		if (nh != NULL)
			LSN(nh) = ret_lsn;
		if (ph != NULL)
			LSN(ph) = ret_lsn;
		if (pp != NULL)
			LSN(pp) = ret_lsn;

		if (gbl_pgmv_verbose) {
			logmsg(LOGMSG_WARN, "%s: swapping pgno %u with free page %u\n", __func__, PGNO(h), PGNO(np));
		}

		/* copy content to new page and fix pgno */
		newpgno = PGNO(np);
		memcpy(np, h, dbp->pgsize);
		PGNO(np) = newpgno;

		if ((ret = __memp_fset(dbmfp, np, DB_MPOOL_DIRTY)) != 0) {
			__db_err(dbenv, "__memp_fset(%u): rc %d", newpgno, ret);
			goto err;
		}

		/*
		 * Empty old page and remove prefix. This ensures that
		 * we call into the non-data version of db_free()
		 */
		HOFFSET(h) = dbp->pgsize;
		NUM_ENT(h) = 0;
		CLR_PREFIX(h);

		/* Place the page on the to-be-freed list, that gets freed after the while loop.
		 * It ensures higher page numbers won't be placed on the front of the list. */
		lfp[num_pages_swapped] = h;
		h = NULL;

		lpgnofromfl[num_pages_swapped] = newpgno;
		qsort(lpgnofromfl, num_pages_swapped + 1, sizeof(db_pgno_t), pgno_cmp);
		++num_pages_swapped;

		/* relink next */
		if (nh != NULL) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: relinking pgno %u to %u\n", __func__, PGNO(nh), newpgno);
			}
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
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: relinking pgno %u to %u\n", __func__, PGNO(ph), newpgno);
			}
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
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: update parent %u reference to %u\n", __func__, PGNO(pp), newpgno);
			}
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
	if (gbl_pgmv_verbose) {
		logmsg(LOGMSG_USER, "%s: num_pages_swapped %u\n", __func__, num_pages_swapped);
	}
	for (ii = 0; ii != num_pages_swapped; ++ii) {
		if ((ret = __db_free(dbc, lfp[ii])) != 0) {
			if (gbl_pgmv_verbose) {
				logmsg(LOGMSG_WARN, "%s: __db_free %u\n", __func__, PGNO(lfp[ii]));
			}
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

/*
 * __db_evict_from_cache --
 *
 * PUBLIC: int __db_evict_from_cache __P((DB *, DB_TXN *));
 */
int
__db_evict_from_cache(dbp, txn)
	DB *dbp;
	DB_TXN *txn;
{
	int ret, t_ret, ii;
	int pglvl, unused;
	u_int8_t page_type;

	DBC *dbc;
	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	db_pgno_t pgno, lpgno;
	PAGE *h;

	DB_LOCK hl;
	int got_hl;

	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	dbc = NULL;

	pgno = lpgno = 0;
	h = NULL;
	got_hl = 0;

	if (dbp->type != DB_BTREE) {
		ret = EINVAL;
		goto err;
	}

	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0) {
		__db_err(dbenv, "%s: __db_cursor: rc %d", __func__, ret);
		goto err;
	}

	for (__memp_last_pgno(dbmfp, &lpgno); pgno <= lpgno; ++pgno) {
		if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, 0, &hl)) != 0) {
			__db_err(dbenv, "%s: __db_lget(%u): rc %d", __func__, pgno, ret);
			goto err;
		}
		got_hl = 1;

		ret = __memp_fget(dbmfp, &pgno, DB_MPOOL_PROBE, &h);
		if (ret == DB_FIRST_MISS || ret == DB_PAGE_NOTFOUND) {
			h = NULL;
		} else {
			ret = __memp_fput(dbmfp, h, DB_MPOOL_EVICT);
			h = NULL;
			if (ret != 0) {
				goto err;
			}
        }

		got_hl = 0;
		if ((ret = __LPUT(dbc, hl)) != 0) {
			__db_err(dbenv, "%s: __LPUT(%d): rc %d", __func__, pgno, ret);
			goto err;
		}
	}

err:
	if (h != NULL && (t_ret = __memp_fput(dbmfp, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	if (got_hl && (t_ret = __LPUT(dbc, hl)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}


/*
 * __db_evict_from_cache_pp --
 *     DB->evict_from_cache pre/post processing.
 *
 * PUBLIC: int __db_evict_from_cache_pp __P((DB *, DB_TXN *));
 */
int
__db_evict_from_cache_pp(dbp, txn)
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

	ret = __db_evict_from_cache(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}
