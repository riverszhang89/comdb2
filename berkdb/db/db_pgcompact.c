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

static int pgno_cmp(const void *x, const void *y)
{
	return ((*(db_pgno_t *)x) - (*(db_pgno_t *)y));
}


int __db_dump_freelist(DB *dbp, db_pgno_t first);

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
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0)
			goto out;

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

	__db_dump_freelist(dbp, meta->free);
    
    /* TODO: can probably release meta page early and do truncation without holding it??? */

resize:
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
        printf("meta lsn before: %d:%d, after %d:%d\n", prev_metalsn.file, prev_metalsn.offset, LSN(meta).file, LSN(meta).offset);

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
    puts("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
    sleep(10);
    abort();

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


