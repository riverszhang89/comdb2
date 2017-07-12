#include "driver.h"

#include <string.h>
#include <stdio.h>

struct type_info {
    char *type_name;
    SI   data_type;
    int  column_size;
    char *literal_prefix;
    char *literal_suffix;
    char *create_params;
    SI   nullable;
    SI   case_sensitive;
    SI   searchable;
    SI   unsigned_attribute;
    SI   fixed_prec_scale;
    SI   auto_unique_value;
    char *local_type_name;
    SI   minimum_scale;
    SI   maximum_scale;
    SI   sql_data_type;
    SI   sql_datetime_sub;
    int  num_prec_radix;
    SI   interval_precision;
};

SQLRETURN comdb2_SQLExecDirect(stmt_t *phstmt, SQLCHAR *sql, SQLINTEGER len);

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT hstmt, SQLSMALLINT type)
{
    stmt_t *phstmt = (stmt_t *)hstmt;

    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    __debug("leaves method.");
    return SQL_SUCCESS;
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
            SET_CSTRING(value_ptr, DBVER, buflen, minimum_length_required);
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
                   "size AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS,"
                   "10 AS NUM_PREC_RADIX, "
                   "(UPPER(isnullable) == 'Y') AS NULLABLE, null AS REMARKS,"
                   "trim(defaultvalue) AS COLUMN_DEF, 0 AS SQL_DATA_TYPE,"
                   "0 AS SQL_DATETIME_SUB, size AS CHAR_OCTET_LENGTH,"
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
    return comdb2_SQLExecDirect(phstmt, (SQLCHAR *)metaquery, SQL_NTS);
}
