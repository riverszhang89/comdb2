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

	/* Remove the file. */
	ret = __db_shrink(dbp, txn);

	if (handle_check)
		__db_rep_exit(dbenv);
	return (ret);
}

/* __db_shrink_redo --
 *  Massage the freelist and truncate the file
 *
 * PUBLIC: int __db_shrink_redo __P((DBC *, DB_TXN *, DBMETA *, size_t, size_t, db_pgno_t *, DB_LSN *));
 */
int
__db_shrink_redo(dbc, txn, meta, npages, notch, pglist, pglsnlist)
	DBC *dbc;
	DB_TXN *txn;
	DBMETA *meta;
	size_t npages, notch;
	db_pgno_t *pglist;
	DB_LSN *pglsnlist;
{
	int ret, modified;
	off_t newsize;
	size_t ii;

	DB_ENV *dbenv;
	DB_MPOOLFILE *dbmfp;
	DB *dbp;
	DBT pgnos, lsns;
	PAGE *h;
	db_pgno_t pgno, *htonpglist;
	DB_LSN *htonpglsnlist;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;
	if (txn == NULL)
		txn = dbc->txn;

	memset(&pgnos, 0, sizeof(DBT));
	memset(&lsns, 0, sizeof(DBT));
	htonpglist = NULL;
	htonpglsnlist = NULL;

	/* log the change */
	if (!DBC_LOGGING(dbc)) {
		LSN_NOT_LOGGED(LSN(meta));
	} else {
		if ((ret = __os_malloc(dbenv, sizeof(db_pgno_t) * npages, &htonpglist)) != 0)
			goto out;
		if ((ret = __os_malloc(dbenv, sizeof(DB_LSN) * npages, &htonpglsnlist)) != 0)
			goto out;

		for (ii = 0; ii != npages; ++ii) {
			htonpglist[ii] = htonl(pglist[ii]);
			htonpglsnlist[ii].file = htonl(pglsnlist[ii].file);
			htonpglsnlist[ii].offset = htonl(pglsnlist[ii].offset);
		}

		pgnos.size = npages * sizeof(db_pgno_t);
		pgnos.data = htonpglist;
		lsns.size = npages * sizeof(DB_LSN);
		lsns.data = htonpglsnlist;

		ret = __db_truncate_freelist_log(dbp, txn, &LSN(meta), 0, &LSN(meta), PGNO_BASE_MD, notch, &pgnos, &lsns);

		if (ret != 0)
			goto out;
	}

	/* rebuild the freelist, in page order */
	for (ii = 0; ii != notch; ++ii) {
		pgno = pglist[ii];
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0)
			goto out;

		if (ii == notch - 1) /* We're at the last free page. */
			NEXT_PGNO(h) = PGNO_INVALID;
		else
			NEXT_PGNO(h) = pglist[ii + 1];

		LSN(h) = LSN(meta);
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

	/* re-point the freelist */
	meta->free = pglist[0];
	/* pglist[notch] is where we will truncate. so point last_pgno to the page before this. */
	meta->last_pgno = pglist[notch] - 1;
	modified = 1;

	/* shrink the file */
	newsize = meta->pagesize * (off_t)(meta->last_pgno + 1); /* +1 for meta page */
	if ((ret = __memp_shrink(dbmfp, newsize)) != 0)
		goto out;

out:
	__os_free(dbenv, htonpglist);
	__os_free(dbenv, htonpglsnlist);

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
	size_t npages, pglistsz, notch;
	int modified;

    DB_ENV *dbenv;
	DBC *dbc;
	DBMETA *meta;
	DB_MPOOLFILE *dbmfp;
	DB_LOCK metalock;
	PAGE *h;
	db_pgno_t pgno, *pglist;
	DB_LSN *pglsnlist;

    dbenv = dbp->dbenv;
	dbmfp = dbp->mpf;

	LOCK_INIT(metalock);

	pgno = PGNO_BASE_MD;
	pglist = NULL;
	pglsnlist = NULL;
	modified = 0;

	/* gather a list of free pages, sort them */
	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		return (ret);

	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WWRITE, 0, &metalock)) != 0)
		goto out;
	if ((ret = __memp_fget(dbmfp, &pgno, 0, &meta)) != 0)
		goto out;

	npages = 0;
	pglistsz = 16;
	if ((ret = __os_malloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
		goto out;

	if ((ret = __os_malloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
		goto out;

	for (pgno = meta->free; pgno != PGNO_INVALID; ++npages) {
		if (npages == pglistsz) {
			pglistsz <<= 1;
			if ((ret =  __os_realloc(dbenv, sizeof(db_pgno_t) * pglistsz, &pglist)) != 0)
				goto out;
			if ((ret =  __os_realloc(dbenv, sizeof(DB_LSN) * pglistsz, &pglsnlist)) != 0)
				goto out;
		}

		pglist[npages] = pgno;

		/* We have metalock. No need to lock free pages. */
		if ((ret = __memp_fget(dbmfp, &pgno, 0, &h)) != 0)
			goto out;

		pglsnlist[npages] = LSN(h);
		pgno = NEXT_PGNO(h);

		if ((ret = __memp_fput(dbmfp, h, 0)) != 0)
			goto out;
	}

	if (npages == 0)
		goto out;

	qsort(pglist, npages, sizeof(db_pgno_t), pgno_cmp);

	logmsg(LOGMSG_WARN, "freelist after sorting (%zu pages): ", npages);
	for (int i = 0; i != npages; ++i) {
		logmsg(LOGMSG_WARN, "%u ", pglist[i]);
	}

	/* We have a sorted freelist at this point. Now see how far we can truncate. */
	for (notch = npages, pgno = meta->last_pgno; notch > 0 && pglist[notch - 1] == pgno && pgno != PGNO_INVALID; --notch, --pgno) ;

	/* pglist[notch] is where in the freelist we can safely truncate. */
	if (notch == npages) {
		logmsg(LOGMSG_WARN, "can't truncate: last free page %u last pg %u\n", pglist[notch - 1], meta->last_pgno);
		goto out;
	}

	logmsg(LOGMSG_WARN, "last pgno %u truncation point (array index) %zu\n", meta->last_pgno, notch);

	ret = __db_shrink_redo(dbc, txn, meta, npages, notch, pglist, pglsnlist);

out:
	__os_free(dbenv, pglist);
	__os_free(dbenv, pglsnlist);
	if ((t_ret = __TLPUT(dbc, metalock)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __memp_fput(dbmfp, meta, modified ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}
