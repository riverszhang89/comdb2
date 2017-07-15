#include "driver.h"

#include <string.h>
#include <stdio.h>

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT hstmt, SQLSMALLINT type)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    char metaquery[MAX_INTERNAL_QUERY_LEN + 1];
    size_t pos = 0;
    int variant = 0;
    const char *tn = NULL;

    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    if (type == SQL_CHAR ||
        type == SQL_VARCHAR ||
        type == SQL_LONGVARCHAR ||
        type == SQL_WCHAR ||
        type == SQL_WVARCHAR ||
        type == SQL_WLONGVARCHAR ||
        type == SQL_BINARY ||
        type == SQL_VARBINARY ||
        type == SQL_LONGVARBINARY)
        variant = 1;

    metaquery[MAX_INTERNAL_QUERY_LEN] = 0;
    pos += snprintf(metaquery, MAX_INTERNAL_QUERY_LEN,
            "SELECT tn AS TYPE_NAME,"
            "dt AS DATA_TYPE,"
            "0 AS COLUMN_SIZE,"
            "null AS LITERAL_PREFIX,"
            "null AS LITERAL_SUFFIX,"
            "'%s' as CREATE_PARAMS,"
            "%d as NULLABLE,"
            "%d as CASE_SENSITIVE,"
            "%d as SEARCHABLE,"
            "%d as UNSIGNED_ATTRIBUTE,"
            "%d as FIXED_PREC_SCALE," // <-- true for decimals
            "%d as AUTO_UNIQUE_VALUE,"
            "null as LOCAL_TYPE_NAME,"
            "null as MINIMUM_SCALE,"
            "dt as SQL_DATA_TYPE,"
            "null as SQL_DATETIME_SUB,"
            "10 as NUM_PREC_RADIX,"
            "null as INTERVAL_PRECISION ",
            variant ? "length" : "null",
            SQL_NULLABLE,
            SQL_TRUE,
            SQL_SEARCHABLE,
            SQL_FALSE,
            (type == SQL_DECIMAL || type == SQL_NUMERIC) ? SQL_TRUE : SQL_FALSE,
            SQL_FALSE);

    if (type == SQL_ALL_TYPES) {
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "FROM (SELECT '%s' AS TN, %d AS DT union ",
                "cstring", SQL_CHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "vutf8", SQL_VARCHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "vutf8", SQL_LONGVARCHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "vutf8", SQL_WCHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "vutf8", SQL_WVARCHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "vutf8", SQL_WLONGVARCHAR);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "decimal128", SQL_DECIMAL);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "decimal128", SQL_NUMERIC);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "short", SQL_SMALLINT);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "short", SQL_BIT);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "short", SQL_TINYINT);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "int", SQL_INTEGER);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "longlong", SQL_BIGINT);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "float", SQL_FLOAT);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "double", SQL_REAL);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "double", SQL_DOUBLE);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "byte", SQL_BINARY);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "blob", SQL_VARBINARY);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "blob", SQL_LONGVARBINARY);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "datetime", SQL_TIMESTAMP);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT union ",
                "intervalym", SQL_INTERVAL_YEAR_TO_MONTH);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                "SELECT '%s' AS TN, %d AS DT) ORDER BY TYPE_NAME",
                "intervalds", SQL_INTERVAL_DAY_TO_SECOND);
    } else {
        switch (type) {
            case SQL_CHAR:
                tn = "cstring";
                break;
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
                tn = "vutf8";
                break;
            case SQL_DECIMAL:
            case SQL_NUMERIC:
                /* use the highest */
                tn = "decimal128";
                break;
            case SQL_SMALLINT:
            case SQL_BIT:
            case SQL_TINYINT:
                tn = "short";
                break;
            case SQL_INTEGER:
                tn = "int";
                break;
            case SQL_BIGINT:
                tn = "longlong";
                break;
            case SQL_FLOAT:
                tn = "float";
                break;
            case SQL_DOUBLE:
            case SQL_REAL:
                tn = "double";
                break;
            case SQL_BINARY:
                tn = "byte";
                break;
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
                tn = "blob";
                break;
            case SQL_TIMESTAMP:
                tn = "datetime";
                break;
            case SQL_INTERVAL_YEAR_TO_MONTH:
                tn = "intervalym";
                break;
            case SQL_INTERVAL_DAY_TO_SECOND:
                tn = "intervalds";
                break;
            default:
                tn = NULL;
                break;
        }
        if (tn != NULL) {
            pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                    "FROM (SELECT '%s' AS TN, %d AS DT)",
                    tn, type);
        } else {
            strncpy(&metaquery[pos],
                    "LIMIT 0", MAX_INTERNAL_QUERY_LEN - pos);
        }
    }

    __debug("metaquery is %s", metaquery);
    __debug("leaves method.");
    return comdb2_SQLExecDirect(phstmt, (SQLCHAR *)metaquery, SQL_NTS);
}

/**
 * ODBC API.
 * SQLGetInfo returns general information about the driver and data source associated with a connection.
 * TODO Complete all necessary attributes.
 */

SQLRETURN SQL_API SQLGetInfo(
        SQLHDBC         hdbc,
        SQLUSMALLINT    type,
        SQLPOINTER      value_ptr,
        SQLSMALLINT     buflen,
        SQLSMALLINT     *str_len)
{
    dbc_t *phdbc = (dbc_t *)hdbc;
    int minimum_length_required = -1;
    SQLRETURN ret = SQL_SUCCESS;
    bool handled;
    int t_ret;
    char *dbver;

    __debug("enters method. %d", type);

    if(!hdbc)
        return SQL_INVALID_HANDLE;

    /* First deal with string attributes. */
    switch(type) {
        case SQL_DATABASE_NAME:
            SET_CSTRING(value_ptr, phdbc->ci.database, buflen, minimum_length_required);
            break;

        case SQL_DBMS_NAME:
            SET_CSTRING(value_ptr, DBNAME, buflen, minimum_length_required);
            break;
        
        case SQL_DBMS_VER:
            if (!phdbc->connected && (ret = comdb2_SQLConnect(phdbc)) !=
                    SQL_SUCCESS)
                return ret;
            t_ret = cdb2_run_statement(phdbc->sqlh, "SELECT COMDB2_VERSION()");
            if (t_ret != 0)
                return set_dbc_error(phdbc, ERROR_WTH, cdb2_errstr(phdbc->sqlh), t_ret);
            while ((t_ret = cdb2_next_record(phdbc->sqlh)) == CDB2_OK)
                dbver = cdb2_column_value(phdbc->sqlh, 0);
            if (t_ret != CDB2_OK_DONE)
                return set_dbc_error(phdbc, ERROR_WTH, cdb2_errstr(phdbc->sqlh), t_ret);
            SET_CSTRING(value_ptr, dbver, buflen, minimum_length_required);
            break;
        
        case SQL_DRIVER_NAME:
            SET_CSTRING(value_ptr, DRVNAME, buflen, minimum_length_required);
            break;

        case SQL_DRIVER_VER:
            SET_CSTRING(value_ptr, DRVVER, buflen, minimum_length_required);
            break;

        case SQL_DRIVER_ODBC_VER:
            SET_CSTRING(value_ptr, DRVODBCVER, buflen, minimum_length_required);
            break;
    }

    /* If @minimum_length_required has been altered, set @handled to true. */
    handled = (minimum_length_required != -1) ? true : false;

    if(minimum_length_required >= buflen)
        /* For a string attribute, if the required length exceeds @buflen, give a warning.
           For other types, since @minimum_length_required was initialized to -1, 
           this branch will not be executed. */
        ret = DBC_ODBC_ERR(ERROR_STR_TRUNCATED);

    /* Next deal with fixed length attributes (meaning we assume the buffer is large enough). */
    switch(type) {
        case SQL_BATCH_ROW_COUNT:
            SET_SQLUINT(value_ptr, SQL_BRC_EXPLICIT, minimum_length_required);
            break;

        case SQL_BATCH_SUPPORT:
            SET_SQLUINT(value_ptr, (SQL_BS_SELECT_EXPLICIT | SQL_BS_ROW_COUNT_EXPLICIT), minimum_length_required);
            break;

        case SQL_CATALOG_USAGE:
            SET_SQLUINT(value_ptr, (SQL_CU_DML_STATEMENTS | SQL_CU_PROCEDURE_INVOCATION), minimum_length_required);
            break;

        case SQL_PARAM_ARRAY_ROW_COUNTS:
            SET_SQLUINT(value_ptr, SQL_PARC_NO_BATCH, minimum_length_required);
            break;

        case SQL_SCHEMA_USAGE:
            SET_SQLUINT(value_ptr, (SQL_SU_DML_STATEMENTS | SQL_SU_PROCEDURE_INVOCATION), minimum_length_required);
            break;

        case SQL_SCROLL_OPTIONS:
            SET_SQLUINT(value_ptr, SQL_SO_FORWARD_ONLY, minimum_length_required);
            break;

        case SQL_TIMEDATE_FUNCTIONS:
            SET_SQLUINT(value_ptr, SQL_FN_TD_NOW, minimum_length_required);
            break;
        
        case SQL_TXN_CAPABLE:
            SET_SQLUSMALLINT(value_ptr, SQL_TC_DML, minimum_length_required);
            break;
        
        case SQL_TXN_ISOLATION_OPTION:
            SET_SQLUINT(value_ptr, (SQL_TXN_READ_COMMITTED | SQL_TXN_SERIALIZABLE), minimum_length_required);
            break;

        default:
            if(!handled)
                ret = DBC_ODBC_ERR(ERROR_TYPE_OUT_OF_RANGE);
            break;
    }

    if(str_len)
        *str_len = (SQLSMALLINT)minimum_length_required;

    __debug("leaves method.");
    return ret;
}

SQLRETURN SQL_API SQLTables(
        SQLHSTMT       hstmt,
        SQLCHAR        *catalog,
        SQLSMALLINT    catalog_len,
        SQLCHAR        *schema,
        SQLSMALLINT    schema_len,
        SQLCHAR        *tbl,
        SQLSMALLINT    tbl_len,
        SQLCHAR        *tbl_tp,
        SQLSMALLINT    tbl_tp_len)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    char metaquery[MAX_INTERNAL_QUERY_LEN + 1];
    size_t pos;

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    /* Ignore catalog and schema */
    (void)catalog;
    (void)catalog_len;
    (void)schema;
    (void)schema_len;

    metaquery[MAX_INTERNAL_QUERY_LEN] = 0;
    pos = snprintf(metaquery, MAX_INTERNAL_QUERY_LEN,
                   "SELECT '%s' AS TABLE_CAT, '%s' AS TABLE_SCHEM,"
                   "name as TABLE_NAME, UPPER(type) AS TABLE_TYPE,"
                   "null AS REMARKS FROM sqlite_master WHERE 1=1",
                   phstmt->dbc->ci.database,
                   phstmt->dbc->ci.cluster);

    if (tbl != NULL) {
        if (tbl_len == SQL_NTS)
            tbl_len = (SQLSMALLINT)strlen((const char *)tbl);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                        " AND TABLE_NAME LIKE '%*s'", tbl_len, tbl);
    }

    if (tbl_tp != NULL) {
        if (tbl_tp_len == SQL_NTS)
            tbl_tp_len = (SQLSMALLINT)strlen((const char *)tbl_tp);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                        " AND TABLE_TYPE LIKE '%*s'", tbl_tp_len, tbl_tp);
    }
    return comdb2_SQLExecDirect(phstmt, (SQLCHAR *)metaquery, SQL_NTS);
}

SQLRETURN SQL_API SQLColumns(
        SQLHSTMT       hstmt,
        SQLCHAR *      catalog,
        SQLSMALLINT    catalog_len,
        SQLCHAR *      schema,
        SQLSMALLINT    schema_len,
        SQLCHAR *      tbl,
        SQLSMALLINT    tbl_len,
        SQLCHAR *      column,
        SQLSMALLINT    column_len)
{
    SQLRETURN ret;
    stmt_t *phstmt = (stmt_t *)hstmt;
    char metaquery[MAX_INTERNAL_QUERY_LEN + 1];
    size_t pos = 0;

    __debug("enters method.");
    __debug("table name is %s, len is %d", tbl, tbl_len);

    /* Ignore catalog and schema */
    (void)catalog;
    (void)catalog_len;
    (void)schema;
    (void)schema_len;

    metaquery[MAX_INTERNAL_QUERY_LEN] = 0;
    pos = snprintf(metaquery, MAX_INTERNAL_QUERY_LEN,
                   "SELECT '%s' AS TABLE_CAT, '%s' AS TABLE_SCHEM,"
                   "tablename AS TABLE_NAME, columnname AS COLUMN_NAME,"
                   "0 AS DATA_TYPE," /* <-- We will convert it later */
                   "type AS TYPE_NAME, (size - 1) AS COLUMN_SIZE, "
                   "(size - 1) AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS,"
                   "10 AS NUM_PREC_RADIX, "
                   "(UPPER(isnullable) == 'Y') AS NULLABLE, null AS REMARKS,"
                   "trim(defaultvalue) AS COLUMN_DEF, 0 AS SQL_DATA_TYPE,"
                   "0 AS SQL_DATETIME_SUB, (size - 1) AS CHAR_OCTET_LENGTH,"
                   "0 AS ORDINAL_POSITION," /* <-- We will convert it later */
                   "CASE WHEN (UPPER(isnullable) == 'Y') THEN 'YES' ELSE 'NO' END AS IS_NULLABLE,"
                   "sqltype " /* <-- Convert this to DATA_TYPE */
                   "FROM comdb2sys_columns WHERE 1=1",
                   phstmt->dbc->ci.database,
                   phstmt->dbc->ci.cluster);

    if (tbl != NULL) {
        if (tbl_len == SQL_NTS)
            tbl_len = (SQLSMALLINT)strlen((const char *)tbl);
        pos += snprintf(&(metaquery[pos]), MAX_INTERNAL_QUERY_LEN - pos,
                        " AND TABLE_NAME LIKE '%*s'", tbl_len, tbl);
    }

    if (column != NULL) {
        if (column_len == SQL_NTS)
            column_len = (SQLSMALLINT)strlen((const char *)column);
        pos += snprintf(&metaquery[pos], MAX_INTERNAL_QUERY_LEN - pos,
                        " AND COLUMN_NAME LIKE '%*s'", column_len, column);
    }
    ret = comdb2_SQLExecDirect(phstmt, (SQLCHAR *)metaquery, SQL_NTS);
    if (ret == SQL_SUCCESS) {
        phstmt->status |= STMT_SQLCOLUMNS;
        phstmt->ord_pos = 0;
    }
    return ret;
}
