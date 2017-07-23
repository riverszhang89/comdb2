/**
 * @file attr.c
 * @description
 * Set/get environment/connection/statement attributes.
 *      07-Jul-2014
 *          Only a minimal subset is implemented at this moment. The driver shall work well for general applications.
 *      
 * @author Rivers Zhang <hzhang320@bloomberg.net>
 * @history
 * 07-Jul-2014 Created.
 */

#include "driver.h"
#include <stdint.h>

/**
 * FIXME Currently we can only use this function to set transation-related settings.
 */
static SQLRETURN comdb2_SQLSetConnectAttr(
        SQLHDBC       hdbc,
        SQLINTEGER    attr,
        SQLPOINTER    buf,
        SQLINTEGER    str_len)
{
    dbc_t *phdbc = (dbc_t *)hdbc;

    __debug("enters method. attr = %d", attr);

	(void)str_len;

    if(!hdbc)
        return SQL_INVALID_HANDLE;

    if(phdbc->in_txn)
        return DBC_ODBC_ERR_MSG(ERROR_FUNCTION_SEQ_ERR, "A transation is executing.");

    switch(attr) {
        case SQL_ATTR_AUTOCOMMIT:
            /* @buf is a ptr to an SQLUINTEGER value. */
            phdbc->auto_commit = (bool)(intptr_t)buf;
            break;

        case SQL_ATTR_CURRENT_CATALOG:
            /* Unusable for comdb2. Put it here to make JdbcOdbcBridge work. */
            break;

        case SQL_ATTR_TXN_ISOLATION:
            /* @buf is a ptr to a 32bit mask. */
            if((intptr_t)buf & (intptr_t)(SQL_TXN_READ_UNCOMMITTED | SQL_TXN_REPEATABLE_READ))
                return DBC_ODBC_ERR_MSG(ERROR_WTH, "Unsupported transaction isolation mode.");
            phdbc->txn_isolation = (int)(intptr_t)buf;
            phdbc->txn_changed = true;
            break;

        default:
            return DBC_ODBC_ERR(ERROR_UNIMPL_ATTR);
    }

    __debug("leaves method.");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectAttr(
        SQLHDBC       hdbc,
        SQLINTEGER    attr,
        SQLPOINTER    buf,
        SQLINTEGER    str_len)
{
    return comdb2_SQLSetConnectAttr(hdbc, attr, buf, str_len);
}

SQLRETURN SQL_API SQLSetConnectOption(
        SQLHDBC         hdbc,
        SQLUSMALLINT    option,
        SQLULEN         param)
{
    return comdb2_SQLSetConnectAttr(hdbc, option, (SQLPOINTER)(intptr_t)param, 0);
}

static SQLRETURN comdb2_SQLGetConnectAttr(
        SQLHDBC        hdbc,
        SQLINTEGER     attr,
        SQLPOINTER     buf,
        SQLINTEGER     buflen,
        SQLINTEGER     *str_len)
{
    dbc_t *phdbc = (dbc_t *)hdbc;
    int minimum_length_required = -1;
    bool is_str_attr = false;

    __debug("enters method. attr = %d", attr);

    if(!hdbc)
        return SQL_INVALID_HANDLE;

    switch(attr) {
        case SQL_ATTR_AUTOCOMMIT:
            SET_SQLUINT(buf, phdbc->auto_commit, minimum_length_required);
            break;

        case SQL_ATTR_TXN_ISOLATION:
            SET_SQLUINT(buf, (SQL_TXN_READ_UNCOMMITTED | SQL_TXN_REPEATABLE_READ), minimum_length_required);
            break;

        default:
            return DBC_ODBC_ERR(ERROR_UNIMPL_ATTR);
    }

    if(str_len)
        *str_len = minimum_length_required;

    __debug("leaves method.");
    return (is_str_attr && minimum_length_required >= buflen) ? 
        DBC_ODBC_ERR(ERROR_STR_TRUNCATED) : SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectAttr(
        SQLHDBC        hdbc,
        SQLINTEGER     attr,
        SQLPOINTER     buf,
        SQLINTEGER     buflen,
        SQLINTEGER     *str_len)
{
    return comdb2_SQLGetConnectAttr(hdbc, attr, buf, buflen, str_len);
}

SQLRETURN SQL_API SQLGetConnectOption(
        SQLHDBC         hdbc,
        SQLUSMALLINT    option,
        SQLPOINTER      value_ptr)
{
    return comdb2_SQLGetConnectAttr(hdbc, option, value_ptr, 0, NULL);
}

static SQLRETURN comdb2_SQLSetStmtAttr(
        SQLHSTMT      hstmt,
        SQLINTEGER    attr,
        SQLPOINTER    buf,
        SQLINTEGER    str_len)
{
    stmt_t *phstmt = (stmt_t *)hstmt;

    __debug("enters method. attr = %d", attr);
	(void)str_len;

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    switch(attr) {
        case SQL_ATTR_CURSOR_TYPE: /* Currently cusor can only scroll forward. */
            if((SQLULEN)(intptr_t)buf != SQL_CURSOR_FORWARD_ONLY)
                return STMT_ODBC_ERR(ERROR_UNSUPPORTED_OPTION_VALUE);
            break;

        case SQL_ATTR_CONCURRENCY:
            if((SQLULEN)(intptr_t)buf != SQL_CONCUR_READ_ONLY)
                return STMT_ODBC_ERR(ERROR_UNSUPPORTED_OPTION_VALUE);
            break;

        default:
            return STMT_ODBC_ERR(ERROR_UNIMPL_ATTR);
    }

    __debug("leaves method.");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtAttr(
        SQLHSTMT      hstmt,
        SQLINTEGER    attr,
        SQLPOINTER    buf,
        SQLINTEGER    str_len)
{
    return comdb2_SQLSetStmtAttr(hstmt, attr, buf, str_len);
}

SQLRETURN SQL_API SQLSetStmtOption(
        SQLHSTMT        hstmt,
        SQLUSMALLINT    option,
        SQLULEN         param)
{
    return comdb2_SQLSetStmtAttr(hstmt, option, (SQLPOINTER)(intptr_t)param, 0);
}

/**
 * FIXME Currently SQLGetStmtAttr is only for reporting SQL_UB_OFF to driver manager.
 */
static SQLRETURN comdb2_SQLGetStmtAttr(
        SQLHSTMT        stmt,
        SQLINTEGER      attr,
        SQLPOINTER      buf,
        SQLINTEGER      buflen,
        SQLINTEGER      *str_len)
{
    stmt_t *phstmt = (stmt_t *)stmt;

    __debug("enters method. attr = %d", attr);
	(void)buflen;
	(void)str_len;
	size_t len = 0;

    if(!stmt)
        return SQL_INVALID_HANDLE;

    switch(attr) {
        case SQL_ATTR_USE_BOOKMARKS:
            *((SQLULEN *)buf) = SQL_UB_OFF;
			len = sizeof(SQLULEN);
            break;
        
        case SQL_ATTR_CONCURRENCY:
            *((SQLULEN *)buf) = SQL_CONCUR_ROWVER;
			len = sizeof(SQLULEN);
            break;

        case SQL_ATTR_APP_ROW_DESC:
        case SQL_ATTR_APP_PARAM_DESC:
        case SQL_ATTR_IMP_ROW_DESC:
        case SQL_ATTR_IMP_PARAM_DESC:
			*(stmt_t**)buf = NULL;
			len = sizeof(SQLPOINTER);
            break;

        default:
            return STMT_ODBC_ERR(ERROR_UNIMPL_ATTR);
    }

	if (str_len != NULL)
		*str_len = (SQLINTEGER)len;

    __debug("leaves method.");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtAttr(
        SQLHSTMT        stmt,
        SQLINTEGER      attr,
        SQLPOINTER      buf,
        SQLINTEGER      buflen,
        SQLINTEGER      *str_len)
{
    return comdb2_SQLGetStmtAttr(stmt, attr, buf, buflen, str_len);
}

SQLRETURN SQL_API SQLGetStmtOption(
        SQLHSTMT        stmt,
        SQLUSMALLINT    option,
        SQLPOINTER      value_ptr)
{
    return comdb2_SQLGetStmtAttr(stmt, option, value_ptr, 0, NULL);
}

#define SQL_FUNC_SET(arr, id) \
	(*(((SQLSMALLINT*)(arr)) + ((id) >> 4)) |= (1 << ((id) & 0x000F)))
SQLRETURN SQL_API SQLGetFunctions(
		HDBC hdbc,
		SQLUSMALLINT func,
	   	SQLUSMALLINT *supported)
{
    (void)hdbc;

    __debug("enters method. attr = %d", attr);

	if (func == SQL_API_ODBC3_ALL_FUNCTIONS) {
		memset(supported, 0, sizeof(SQLSMALLINT) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
		SQL_FUNC_SET(supported, SQL_API_SQLBINDCOL);
		SQL_FUNC_SET(supported, SQL_API_SQLCOLATTRIBUTE);
		SQL_FUNC_SET(supported, SQL_API_SQLCONNECT);
		SQL_FUNC_SET(supported, SQL_API_SQLDESCRIBECOL);
		SQL_FUNC_SET(supported, SQL_API_SQLDISCONNECT);
		SQL_FUNC_SET(supported, SQL_API_SQLEXECDIRECT);
		SQL_FUNC_SET(supported, SQL_API_SQLEXECUTE);
		SQL_FUNC_SET(supported, SQL_API_SQLFETCH);
		SQL_FUNC_SET(supported, SQL_API_SQLFREECONNECT);
		SQL_FUNC_SET(supported, SQL_API_SQLFREEENV);
		SQL_FUNC_SET(supported, SQL_API_SQLFREESTMT);
		SQL_FUNC_SET(supported, SQL_API_SQLNUMRESULTCOLS);
		SQL_FUNC_SET(supported, SQL_API_SQLPREPARE);
		SQL_FUNC_SET(supported, SQL_API_SQLROWCOUNT);
		SQL_FUNC_SET(supported, SQL_API_SQLTRANSACT);
		SQL_FUNC_SET(supported, SQL_API_SQLCOLUMNS);
		SQL_FUNC_SET(supported, SQL_API_SQLDRIVERCONNECT);
		SQL_FUNC_SET(supported, SQL_API_SQLGETDATA);
		SQL_FUNC_SET(supported, SQL_API_SQLGETFUNCTIONS);
		SQL_FUNC_SET(supported, SQL_API_SQLGETINFO);
		SQL_FUNC_SET(supported, SQL_API_SQLGETSTMTOPTION);
		SQL_FUNC_SET(supported, SQL_API_SQLGETTYPEINFO);
		SQL_FUNC_SET(supported, SQL_API_SQLSETSTMTOPTION);
		SQL_FUNC_SET(supported, SQL_API_SQLSPECIALCOLUMNS);
		SQL_FUNC_SET(supported, SQL_API_SQLSTATISTICS);
		SQL_FUNC_SET(supported, SQL_API_SQLTABLES);
		SQL_FUNC_SET(supported, SQL_API_SQLCOLUMNPRIVILEGES);
		SQL_FUNC_SET(supported, SQL_API_SQLFOREIGNKEYS);
		SQL_FUNC_SET(supported, SQL_API_SQLMORERESULTS);
		SQL_FUNC_SET(supported, SQL_API_SQLNUMPARAMS);
		SQL_FUNC_SET(supported, SQL_API_SQLPRIMARYKEYS);
		SQL_FUNC_SET(supported, SQL_API_SQLPROCEDURECOLUMNS);
		SQL_FUNC_SET(supported, SQL_API_SQLPROCEDURES);
		SQL_FUNC_SET(supported, SQL_API_SQLBINDPARAMETER);
		SQL_FUNC_SET(supported, SQL_API_SQLALLOCHANDLE);
		SQL_FUNC_SET(supported, SQL_API_SQLENDTRAN);
		SQL_FUNC_SET(supported, SQL_API_SQLFREEHANDLE);
		SQL_FUNC_SET(supported, SQL_API_SQLGETCONNECTATTR);
		SQL_FUNC_SET(supported, SQL_API_SQLGETDIAGFIELD);
		SQL_FUNC_SET(supported, SQL_API_SQLGETDIAGREC);
		SQL_FUNC_SET(supported, SQL_API_SQLGETSTMTATTR);
		SQL_FUNC_SET(supported, SQL_API_SQLSETCONNECTATTR);
		SQL_FUNC_SET(supported, SQL_API_SQLSETDESCFIELD);
		SQL_FUNC_SET(supported, SQL_API_SQLSETSTMTATTR);
	} else if (func == SQL_API_ALL_FUNCTIONS) {
		memset(supported, 0, sizeof(SQLSMALLINT) * 100);
		supported[SQL_API_SQLALLOCCONNECT] = TRUE;
		supported[SQL_API_SQLALLOCENV] = TRUE;
		supported[SQL_API_SQLALLOCSTMT] = TRUE;
		supported[SQL_API_SQLBINDCOL] = TRUE;
		supported[SQL_API_SQLCOLATTRIBUTES] = TRUE;
		supported[SQL_API_SQLCONNECT] = TRUE;
		supported[SQL_API_SQLDESCRIBECOL] = TRUE;
		supported[SQL_API_SQLDISCONNECT] = TRUE;
		supported[SQL_API_SQLEXECDIRECT] = TRUE;
		supported[SQL_API_SQLEXECUTE] = TRUE;
		supported[SQL_API_SQLFETCH] = TRUE;
		supported[SQL_API_SQLFREECONNECT] = TRUE;
		supported[SQL_API_SQLFREEENV] = TRUE;
		supported[SQL_API_SQLFREESTMT] = TRUE;
		supported[SQL_API_SQLNUMRESULTCOLS] = TRUE;
		supported[SQL_API_SQLPREPARE] = TRUE;
		supported[SQL_API_SQLROWCOUNT] = TRUE;
		supported[SQL_API_SQLTRANSACT] = TRUE;
		supported[SQL_API_SQLBINDPARAMETER] = TRUE;
		supported[SQL_API_SQLCOLUMNS] = TRUE;
		supported[SQL_API_SQLDRIVERCONNECT] = TRUE;
		supported[SQL_API_SQLGETDATA] = TRUE;
		supported[SQL_API_SQLGETFUNCTIONS] = TRUE;
		supported[SQL_API_SQLGETINFO] = TRUE;
		supported[SQL_API_SQLGETSTMTOPTION] = TRUE;
		supported[SQL_API_SQLGETTYPEINFO] = TRUE;
		supported[SQL_API_SQLSETCONNECTOPTION] = TRUE;
		supported[SQL_API_SQLSETSTMTOPTION] = TRUE;
		supported[SQL_API_SQLSPECIALCOLUMNS] = TRUE;
		supported[SQL_API_SQLSTATISTICS] = TRUE;
		supported[SQL_API_SQLTABLES] = TRUE;
		supported[SQL_API_SQLCOLUMNPRIVILEGES] = FALSE;
		supported[SQL_API_SQLFOREIGNKEYS] = TRUE;
		supported[SQL_API_SQLMORERESULTS] = TRUE;
		supported[SQL_API_SQLNUMPARAMS] = TRUE;
		supported[SQL_API_SQLPRIMARYKEYS] = TRUE;
		supported[SQL_API_SQLPROCEDURECOLUMNS] = TRUE;
		supported[SQL_API_SQLPROCEDURES] = TRUE;
		supported[SQL_API_SQLTABLEPRIVILEGES] = TRUE;
	} else {
		*supported = FALSE;
		switch (func) {
			case SQL_API_SQLALLOCCONNECT:
			case SQL_API_SQLALLOCENV:
			case SQL_API_SQLALLOCSTMT:
			case SQL_API_SQLBINDCOL:
			case SQL_API_SQLCOLATTRIBUTES:
			case SQL_API_SQLCONNECT:
			case SQL_API_SQLDESCRIBECOL:
			case SQL_API_SQLDISCONNECT:
			case SQL_API_SQLEXECDIRECT:
			case SQL_API_SQLEXECUTE:
			case SQL_API_SQLFETCH:
			case SQL_API_SQLFREECONNECT:
			case SQL_API_SQLFREEENV:
			case SQL_API_SQLFREESTMT:
			case SQL_API_SQLNUMRESULTCOLS:
			case SQL_API_SQLPREPARE:
			case SQL_API_SQLROWCOUNT:
			case SQL_API_SQLTRANSACT:
			case SQL_API_SQLBINDPARAMETER:
			case SQL_API_SQLCOLUMNS:
			case SQL_API_SQLDRIVERCONNECT:
			case SQL_API_SQLGETDATA:
			case SQL_API_SQLGETFUNCTIONS:
			case SQL_API_SQLGETINFO:
			case SQL_API_SQLGETSTMTOPTION:
			case SQL_API_SQLGETTYPEINFO:
			case SQL_API_SQLSETCONNECTOPTION:
			case SQL_API_SQLSETSTMTOPTION:
			case SQL_API_SQLSPECIALCOLUMNS:
			case SQL_API_SQLSTATISTICS:
			case SQL_API_SQLTABLES:
			case SQL_API_SQLCOLUMNPRIVILEGES:
			case SQL_API_SQLFOREIGNKEYS:
			case SQL_API_SQLMORERESULTS:
			case SQL_API_SQLNUMPARAMS:
			case SQL_API_SQLPRIMARYKEYS:
			case SQL_API_SQLPROCEDURECOLUMNS:
			case SQL_API_SQLPROCEDURES:
			case SQL_API_SQLTABLEPRIVILEGES:
				*supported = TRUE;
		}
	}

	return SQL_SUCCESS;
}
