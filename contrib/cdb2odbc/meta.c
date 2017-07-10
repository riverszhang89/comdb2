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

    phstmt->status = STMT_TYPE_INFO;
        
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
    char query_tables[MAX_INTERNAL_QUERY_LEN], clause[MAX_INTERNAL_QUERY_LEN],
         *sel_fmt = "SELECT '%s' AS TABLE_CAT, NULL AS TABLE_SCHEM, %s AS TABLE_NAME, "
             "%s as TABLE_TYPE, NULL AS REMARKS";

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    if(catalog && strcmp((char *)catalog, SQL_ALL_CATALOGS) == 0 ||
            schema && strcmp((char *)schema, SQL_ALL_SCHEMAS) == 0) {
        sprintf(query_tables, sel_fmt, phstmt->dbc->ci.database, "NULL", "NULL");
    } else {
        sprintf(query_tables, sel_fmt, phstmt->dbc->ci.database, "name", "type");
        strcat(query_tables, " FROM sqlite_master");
        clause[0] = '\0';


        if(catalog || schema)
            __warn("Catalog and schema will be ignored.");

        /* Check string length. */
        if(tbl) {
            if(tbl_len == SQL_NTS)
                tbl_len = (SQLSMALLINT)strlen((char *)tbl);

            if(tbl_len < 0)
                return STMT_ODBC_ERR(ERROR_INVALID_LENGTH);

            if(tbl_len > 0) {
                strcat(clause, "name LIKE '");
                strcat(clause, (char *)tbl);
                strcat(clause, "'");
            }
        } 

        if(tbl_tp) {
            if(tbl_tp_len == SQL_NTS)
                tbl_tp_len = (SQLSMALLINT)strlen((char *)tbl_tp);

            if(tbl_tp_len < 0)
                return STMT_ODBC_ERR(ERROR_INVALID_LENGTH);

            if(tbl_tp_len > 0) {
                if(strlen(clause))
                    strcat(clause, " AND ");

                strcat(clause, "type LIKE '");
                strcat(clause, (char *)tbl_tp);
                strcat(clause, "'");
            }
        }

        if(strlen(clause)) {
            strcat(query_tables, " WHERE ");
            strcat(query_tables, clause);
        }
    }

    return comdb2_SQLExecDirect(phstmt, (SQLCHAR *)query_tables, SQL_NTS);
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
    NOT_IMPL_STMT((stmt_t *)hstmt);
}
