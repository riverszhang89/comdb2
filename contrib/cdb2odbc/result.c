/**
 * @file result.c
 *
 * @description
 * Some functions in this file set can be called before calling SQLExecute/SQLExecDirect.
 * A flag is used to label the status of a statement handle. If the statement is ready (meaning prepared),
 * the driver will pre-execute the statement. Next time when an application calls SQLExecute, the prefetched
 * result set will be returned instead of sending the same query again.
 * The behavior shall be transparent to applications.
 *
 * @author Rivers Zhang <hzhang320@bloomberg.net>
 * @history
 * 20-Jun-2014 Created.
 * 11-Jul-2014 Add statement status checks.
 * 14-Jul-2014 Fixed constraint violation error prompt.
 */

#include "driver.h"
#include "convert.h"

conv_resp retrieve_and_convert(cdb2_hndl_tp     *sqlh, 
                              SQLSMALLINT       col, 
                              SQLSMALLINT       c_data_type, 
                              SQLPOINTER        target_ptr, 
                              SQLLEN            target_len, 
                              SQLLEN            *strlen_or_indicator)
{
    void *value;
    int size, cdb2_type;

    if( (cdb2_type = cdb2_column_type(sqlh, col)) == -1 )
        return CONV_UNKNOWN_CDB2_TYPE;

    if( !(value = cdb2_column_value(sqlh, col)) ) {
        *strlen_or_indicator = SQL_NULL_DATA;
        return CONV_NULL;
    }
    
    size = cdb2_column_size(sqlh, col);

    if(cdb2_type < NUM_CDB2_CONVS) /* Just in case. */
        return (*CDB2_CONVS[cdb2_type])(value, size, c_data_type, target_ptr, target_len, strlen_or_indicator);

    return CONV_UNKNOWN_CDB2_TYPE;
}

SQLRETURN comdb2_SQLGetData(SQLHSTMT        hstmt,
                            SQLUSMALLINT    col,
                            SQLSMALLINT     c_data_type,
                            SQLPOINTER      target_ptr,
                            SQLLEN          target_len,
                            SQLLEN          *strlen_or_indicator)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    errid_t eid = ERROR_NA;
    /* because @strlen_or_indicator could be NULL, a non-NULL value is passed in to conversion functions. */
    SQLLEN len_required;

    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    if(target_ptr == NULL)
        eid = ERROR_INVALID_NULL_PTR;
    else if(!(phstmt->status & (STMT_PREMATURE | STMT_FINISHED)))
        eid = ERROR_FUNCTION_SEQ_ERR;
    else if(col > phstmt->col_count)
        eid = ERROR_INVALID_DESC_IDX;
    else if(col == 0) /* column 0 => bookmark */
        eid = ERROR_NOT_IMPL;
    else {
        switch( retrieve_and_convert(phstmt->sqlh, col - 1, c_data_type, target_ptr, target_len, &len_required) ) {
            case CONV_YEAH:
                if(strlen_or_indicator)
                    *strlen_or_indicator = len_required;
                break;

            case CONV_TRUNCATED:
                eid = ERROR_STR_TRUNCATED;
                if(strlen_or_indicator)
                    *strlen_or_indicator = len_required;
                break;

            case CONV_TRUNCATED_WHOLE:
                eid = ERROR_INVALID_STRING_FOR_CASTING;
                if(strlen_or_indicator)
                    *strlen_or_indicator = len_required;
                break;

            case CONV_NULL:
                /* @strlen_or_indicator must not be NULL if the column data is NULL. */
                if(strlen_or_indicator)
                    *strlen_or_indicator = SQL_NULL_DATA;
                else
                    eid = ERROR_IND_REQUIRED;
                break;

            case CONV_UNKNOWN_CDB2_TYPE:
                eid = ERROR_INVALID_DESC_IDX;
                break;

            case CONV_UNKNOWN_C_TYPE:
                eid = ERROR_PROG_OUT_OF_RANGE;
                break;

            case CONV_IMPOSSIBLE:
                eid = ERROR_CANNOT_CONVERT;
                break;

            default:
                eid = ERROR_WTH;
                break;
        }
    }

    __debug("leaves method.");
    return eid == ERROR_NA ? SQL_SUCCESS : STMT_ODBC_ERR(eid);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT       hstmt,
                             SQLUSMALLINT   col,
                             SQLSMALLINT    c_data_type,
                             SQLPOINTER     target_ptr,
                             SQLLEN         target_len,
                             SQLLEN         *strlen_or_ind)
{
    return comdb2_SQLGetData(hstmt, col, c_data_type, target_ptr, target_len, strlen_or_ind);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT hstmt)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    int rc, i;
    data_buffer_t *data;

    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    /* TODO */
    if(phstmt->status == STMT_TYPE_INFO) {
        return SQL_NO_DATA;
    }

    if(phstmt->status != STMT_FINISHED)
        return STMT_ODBC_ERR_MSG(ERROR_FUNCTION_SEQ_ERR, "SQLFetch can only be invoked after "
                "successful execution on a SQL statement.");

    if((rc = cdb2_next_record(phstmt->sqlh)) == CDB2_OK) {
        
        if(phstmt->num_data_buffers > 0 && (data = phstmt->buffers)) {
            /* SQLBindCol is called before. */
            for(i = 1; data < phstmt->buffers + phstmt->num_data_buffers; ++data, ++i) {
                if(!data->used) /* Skip unused data buffer. */
                    continue;
                
                if(SQL_FAILED(comdb2_SQLGetData(hstmt, i, data->c_type, data->buffer, data->buffer_length, data->required)))
                    __warn("Failed to return data in bound column %d", i);
            }
        }

        __debug("leaves method.");
        return SQL_SUCCESS;
    }

    SET_EXTRACTED(phstmt);
    return rc == CDB2_OK_DONE ? SQL_NO_DATA : set_stmt_error(phstmt, ERROR_WTH, cdb2_errstr(phstmt->sqlh), rc);
}

SQLRETURN comdb2_SQLExecute(stmt_t *phstmt);

/* Returns corresponding SQL types of cdb2api types. */
static int cdb2_to_sql(int cdb2_type)
{
    switch(cdb2_type) {
        case CDB2_INTEGER:
            return SQL_BIGINT;
        case CDB2_REAL:
            return SQL_DOUBLE;
        case CDB2_CSTRING:
            return SQL_VARCHAR;
        case CDB2_BLOB:
            return SQL_VARBINARY;
        case CDB2_DATETIME:
            return SQL_TIMESTAMP;
        case CDB2_INTERVALYM:
            return SQL_INTERVAL_YEAR_TO_MONTH;
        case CDB2_INTERVALDS:
            return SQL_INTERVAL_DAY_TO_SECOND;

        default:
            break;
    }

    return SQL_UNKNOWN_TYPE;
}

/* Returns column size of a cdb2api type. Regarding the definition of 'column size', see 
http://msdn.microsoft.com/en-us/library/ms712499(v=vs.85).aspx */

static unsigned int column_size(cdb2_hndl_tp *sqlh, int col)
{
    switch(cdb2_column_type(sqlh, col)) {
        case CDB2_INTEGER:
            /* Max string length for a 64bit number (sign skipped) */
            return MAX_INT64_DIGITS;

        case CDB2_REAL:
            /* Max string length for a double-precision floating number (sign skipped) */
            return MAX_DBL_DIGITS;

        case CDB2_DATETIME:
            /* yyyy-mm-dd hh:MM:ss.fff (TIMEZONE)
               gives 26 + CDB2_MAX_TZNAME */
            return MAX_DATETIME_DISPLAY_SIZE;

        case CDB2_INTERVALYM:
            /* +/-year(10 digits at most)-MM 
               gives 14. */
            return MAX_YM_DISPLAY_SIZE;

        case CDB2_INTERVALDS:
            /* +/-day(10 digits at most) hh:MM:ss.fff 
               gives 24. */
            return MAX_DS_DISPLAY_SIZE;

        default:
            return cdb2_column_size(sqlh, col);
    }
}

static unsigned int display_size(cdb2_hndl_tp *sqlh, int col)
{
    switch(cdb2_column_type(sqlh, col)) {
        case CDB2_INTEGER:
            /* Max string length for a 64bit number (sign skipped) */
            return MAX_INT64_DISPLAY_SIZE;

        case CDB2_REAL:
            /* Max string length for a double-precision floating number (sign skipped) */
            return MAX_DBL_DISPLAY_SIZE;

        case CDB2_DATETIME:
            /* yyyy-mm-dd hh:MM:ss.fff (TIMEZONE)
               gives 26 + CDB2_MAX_TZNAME */
            return MAX_DATETIME_DISPLAY_SIZE;

        case CDB2_INTERVALYM:
            /* +/-year(10 digits at most)-MM 
               gives 14. */
            return MAX_YM_DISPLAY_SIZE;

        case CDB2_INTERVALDS:
            /* +/-day(10 digits at most) hh:MM:ss.fff 
               gives 24. */
            return MAX_DS_DISPLAY_SIZE;

        default:
            return 16;
    }
}

/* 
   ODBC API.
   SQLDescribeCol returns the result descriptor . column name,type, column size, decimal digits, and nullability . for one column in the result set.
   FIXME At this moment, the information is unavailable in IRD.
 */
SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT       hstmt,
                                 SQLUSMALLINT   col,
                                 SQLCHAR        *col_name,
                                 SQLSMALLINT    col_name_max,
                                 SQLSMALLINT    *col_name_len,
                                 SQLSMALLINT    *sql_data_type,
                                 SQLULEN        *col_size,
                                 SQLSMALLINT    *decimal_digits,
                                 SQLSMALLINT    *nullable)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    SQLRETURN ret;
    cdb2_hndl_tp *sqlh;
    const char * _column_name;
    int cdb2_type, length;
    
    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    /* Validate statement status. */

    if(phstmt->status & (STMT_ALLOCATED | STMT_EXECUTING))
        return STMT_ODBC_ERR(ERROR_FUNCTION_SEQ_ERR);

    /* If the statement is ready, we pre-execute the bound SQL. */
    if((phstmt->status & STMT_READY) && SQL_FAILED(ret = comdb2_SQLExecute(hstmt)))
        return ret;

    --col;
    sqlh = phstmt->sqlh;

    /* Column name */
    _column_name = cdb2_column_name(sqlh, col);
    if(col_name_len)
        *col_name_len = strlen(_column_name);
    if(col_name)
        my_strncpy_out_fn((char *)col_name, _column_name, col_name_max);
    if(strlen(_column_name) >= col_name_max)
        STMT_ODBC_ERR(ERROR_STR_TRUNCATED);

    /* SQL data type */
    cdb2_type = cdb2_column_type(sqlh, col);
    if(sql_data_type)
        *sql_data_type = cdb2_to_sql(cdb2_type);

    /* Column size - number of digits. So column size of 123.45 is 5 */
    if(col_size)
        *col_size = column_size(sqlh, col);

    /* Decimal digits - scale */
    if(decimal_digits) {
        if(cdb2_type == CDB2_REAL)
            /* Precision of double */
            *decimal_digits = MAX_DBL_DIGITS;
        else if(cdb2_type == CDB2_INTERVALDS || cdb2_type == CDB2_DATETIME)
            *decimal_digits = 3;
        else
            *decimal_digits = 0;
    }

    /* Nullable FIXME */
    if(nullable)
        *nullable = SQL_NULLABLE_UNKNOWN;

    __debug("leaves method.");

    return SQL_SUCCESS;
}


/*
   ODBC API.
   SQLColAttribute returns descriptor information for a column in a result set. 
   Descriptor information is returned as a character string, a descriptor-dependent value, or an integer value.

   FIXME Currently, only SQL_DESC_LABEL is supported (I implement this function for testing the driver using unixODBC shell).
   Applications should use SQLDescribeCol instead.
 */
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT      hstmt, 
                                  SQLUSMALLINT  col, 
                                  SQLUSMALLINT  field, 
                                  SQLPOINTER    text_attr, 
                                  SQLSMALLINT   attr_max, 
                                  SQLSMALLINT   *attr_len, 
                                  SQLLEN        *num_attr)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    SQLRETURN ret;
    int minimum_length_required = -1;

    __debug("enters method. field = %d", field);

    if(SQL_NULL_HSTMT == hstmt)
        return SQL_INVALID_HANDLE;

    /* Validate statement status. */

    if(phstmt->status & (STMT_ALLOCATED | STMT_EXECUTING))
        return STMT_ODBC_ERR(ERROR_FUNCTION_SEQ_ERR);

    /* If the statement is ready, we pre-execute the bound SQL. */
    if((phstmt->status & STMT_READY) && SQL_FAILED(ret = comdb2_SQLExecute(hstmt)))
        return ret;

    if(col > phstmt->col_count) 
        return STMT_ODBC_ERR(ERROR_INVALID_DESC_IDX);

    /* @col starts at 1. Zero is allowed only when SQL_ATTR_USE_BOOKMARKS is not SQL_UB_OFF. 
       Driver Manager will make sure @col != 0 when SQL_ATTR_USE_BOOKMARKS == SQL_UB_OFF (according to Microsoft, but who knows. LOL). 
       Since I don't have a plan to support bookmark (meaning SQL_ATTR_USE_BOOKMARKS will always set to OFF), so I can assume col will never be 0.  */

    --col; // use 0-based index

    /* Maps ODBC 2.x reserved values to ODBC 3.x reserved values. */
    switch(field) {
        case SQL_COLUMN_SCALE:
            field = SQL_DESC_SCALE;
            break;

        case SQL_COLUMN_PRECISION:
            field = SQL_DESC_PRECISION;
            break;

        case SQL_COLUMN_NULLABLE:
            field = SQL_DESC_NULLABLE;
            break;

        case SQL_COLUMN_LENGTH:
            field = SQL_DESC_OCTET_LENGTH;
            break;

        case SQL_COLUMN_NAME:
            field = SQL_DESC_NAME;
                break;
    }


    switch(field) {

        /* http://msdn.microsoft.com/en-us/library/ms713558(v=vs.85).aspx */

        case SQL_DESC_AUTO_UNIQUE_VALUE:
            break;
        case SQL_DESC_BASE_COLUMN_NAME:
            break;
        case SQL_DESC_BASE_TABLE_NAME:
            break;
        case SQL_DESC_CASE_SENSITIVE:
            break;
        case SQL_DESC_CATALOG_NAME:
            break;
        case SQL_DESC_CONCISE_TYPE:
            SET_SQLLEN(num_attr, cdb2_to_sql(cdb2_column_type(phstmt->sqlh, col)), minimum_length_required);
            break;

        case SQL_DESC_COUNT:
            break;
        case SQL_DESC_DISPLAY_SIZE:
            SET_SQLLEN(num_attr, display_size(phstmt->sqlh, col), minimum_length_required);
            break;
        case SQL_DESC_FIXED_PREC_SCALE:
            break;
        case SQL_DESC_NAME:
        case SQL_DESC_LABEL:
            SET_CSTRING(text_attr, cdb2_column_name(phstmt->sqlh, col), attr_max, minimum_length_required);
            break;
        case SQL_DESC_LENGTH:
            break;
        case SQL_DESC_LITERAL_PREFIX:
            break;
        case SQL_DESC_LITERAL_SUFFIX:
            break;
        case SQL_DESC_LOCAL_TYPE_NAME:
            break;
        case SQL_DESC_NULLABLE:
            break;
        case SQL_DESC_NUM_PREC_RADIX:
            break;
        case SQL_DESC_OCTET_LENGTH:
            break;
        case SQL_DESC_PRECISION:
            break;
        case SQL_DESC_SCALE:
            break;
        case SQL_DESC_SCHEMA_NAME:
            break;
        case SQL_DESC_SEARCHABLE:
            break;
        case SQL_DESC_TABLE_NAME:
            break;
        case SQL_DESC_TYPE:
            break;
        case SQL_DESC_TYPE_NAME:
            break;
        case SQL_DESC_UNNAMED:
            break;
        case SQL_DESC_UNSIGNED:
            break;
        case SQL_DESC_UPDATABLE:
            break;

        default:
            return STMT_ODBC_ERR(ERROR_INVALID_DESC_FIELD_ID);
    }

    __debug("enters method.");

    return SQL_SUCCESS;
}

/*
   ODBC API.
   SQLNumResultCols returns the number of columns in a result set.
 */
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT hstmt, SQLSMALLINT *count)
{
    stmt_t *phstmt = (stmt_t *) hstmt;
    SQLRETURN ret = SQL_SUCCESS;

    __debug("enters method.");

    if(!hstmt)
        ret = SQL_INVALID_HANDLE;
    else if(phstmt->status == STMT_TYPE_INFO)
        /* If @hstmt is a pseudo statement, no need to call cdb2api. TODO */
        *count = 0;
    else if (phstmt->status & (STMT_ALLOCATED | STMT_EXECUTING))
        ret = STMT_ODBC_ERR_MSG(ERROR_FUNCTION_SEQ_ERR, "No query is attached or the statement is still executing."); 
    else if(!(phstmt->status & STMT_READY) || SQL_SUCCEEDED((ret = comdb2_SQLExecute(hstmt))))
        *count = cdb2_numcolumns(phstmt->sqlh);

    __debug("leaves method.");
    return ret;
}

/**
 * SQLRowCount
 * If it's a SELECT (UPDATE) statement, the number of selected (affected) rows is returned.
 * The behavior for a select statement is driver-specific. 
 *
 * TODO Add SQLBulkOperations & SQLSetPos supports.
 */
SQLRETURN SQL_API SQLRowCount(SQLHSTMT hstmt, SQLLEN *count)
{
    stmt_t *phstmt = (stmt_t *)hstmt;
    int rc;
    SQLRETURN ret;

    __debug("enters method.");

    if(!hstmt)
        return SQL_INVALID_HANDLE;

    /* Validate statement status. */

    if(phstmt->status & (STMT_ALLOCATED | STMT_EXECUTING))
        return STMT_ODBC_ERR(ERROR_FUNCTION_SEQ_ERR);

    /* If the statement is ready, we pre-execute the bound SQL. */
    if((phstmt->status & STMT_READY) && SQL_FAILED(ret = comdb2_SQLExecute(hstmt)))
        return ret;

    if( !phstmt->effects && !(phstmt->effects = my_calloc(effects_tp, 1)) ) 
        return STMT_ODBC_ERR(ERROR_MEM_ALLOC_FAIL);

    if(phstmt->sql_type >= STMT_HAS_NO_EFFECT) {
        __warn("SQLRowCount is not available, return 0");
        *count = 0;
    } else {

        if(rc = cdb2_get_effects(phstmt->sqlh, phstmt->effects)) {
            __debug("No effects received.");
            *count = 0;
            return set_stmt_error(phstmt, ERROR_WTH, "Effects were not sent by comdb2 server.", rc);
        }

        if(phstmt->sql_type == STMT_SELECT) 

            /* SQLRowCount returns the count of AFFECTED rows. For a select statement, the behavior is driver-defined. 
               Most of drivers return the number of tuples for a SELECT statement. So we follow the convention. 
               For more details, see http://msdn.microsoft.com/en-us/library/ms711835(v=vs.85).aspx */

            *count = phstmt->effects->num_selected;
        else 
            *count = phstmt->effects->num_affected;
    }

    __debug("leaves method.");

    return SQL_SUCCESS;
}

SQLRETURN SQLMoreResults(SQLHSTMT hstmt)
{
    return hstmt ? SQL_NO_DATA : SQL_INVALID_HANDLE;
}
