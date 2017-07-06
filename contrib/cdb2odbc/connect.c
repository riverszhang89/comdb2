/**
 * @file sqlconnect.c
 * @description
 * @author Rivers Zhang <hzhang320@bloomberg.net>
 *
 * @navigator
 * SQLConnect, SQLDisconnect
 *
 * @history
 * 23-Jun-2014 Created.
 * 25-Jun-2014 SQLDisconnect added.
 */

#include "driver.h"

#if defined(__IODBC__)
# include <iodbcinst.h>
#elif defined(__UNIXODBC__)
# include <odbcinst.h>
#endif

/**
 * Connects to the data source. 
 * @phdbc must have its connection info filled out before being passed in.
 */
static SQLRETURN comdb2_SQLConnect(dbc_t *phdbc)
{
    cdb2_hndl_tp *sqlh;
    conn_info_t *ci = &phdbc->ci;
    int rc;

    if(ci->database[0] == '\0' || ci->cluster[0] == '\0') 
        return DBC_ODBC_ERR(ERROR_NO_CONF);

    if(rc = cdb2_open(&sqlh, ci->database, ci->cluster, ci->flag)) {
        cdb2_close(sqlh);
        return set_dbc_error(phdbc, ERROR_UNABLE_TO_CONN, NULL, rc);
    }

    phdbc->sqlh = sqlh;
    phdbc->sqlh_status = SQLH_IDLE;
    phdbc->connected = true;

    return SQL_SUCCESS;
}

#if defined(__UNIXODBC__) || defined(__IODBC__)

/**
 * Reads connection information from odbc.ini.
 * SQLGetPrivateProfileString, which is a builtin in the driver manager, is used to read configuration.
 */
static void complete_conn_info_by_dm(conn_info_t *ci)
{
    char flag_in_ini[MAX_CONN_ATTR_LEN];
    flag_in_ini[0] = '\0';

    if(ci->database[0] == '\0')
        SQLGetPrivateProfileString(ci->dsn, "DATABASE", "", ci->database, MAX_CONN_ATTR_LEN, "odbc.ini");
    if(ci->cluster[0] == '\0')
        SQLGetPrivateProfileString(ci->dsn, "CLUSTER", "", ci->cluster, MAX_CONN_ATTR_LEN, "odbc.ini");
    if(ci->flag == 0) {
        SQLGetPrivateProfileString(ci->dsn, "FLAG", "0", flag_in_ini, MAX_CONN_ATTR_LEN, "odbc.ini");
        ci->flag = atoi(flag_in_ini);
    }
}

SQLRETURN SQL_API SQLConnect(
        SQLHDBC         hdbc,
        SQLCHAR         *dsn,
        SQLSMALLINT     dsn_len,
        SQLCHAR         *uid,
        SQLSMALLINT     uid_len,
        SQLCHAR         *auth,
        SQLSMALLINT     auth_len)
{
    dbc_t *phdbc = (dbc_t *)hdbc;
    conn_info_t *ci;

    __debug("enters method.");
    __info("Connecting to %s.", dsn);

    if(!phdbc)
        return SQL_INVALID_HANDLE;
    
    if(phdbc->connected)
        return DBC_ODBC_ERR(ERROR_CONN_IN_USE);

    ci = &phdbc->ci;
    strncpy(ci->dsn, (char *)dsn, MAX_CONN_ATTR_LEN - 1);
    complete_conn_info_by_dm(ci);

    __debug("leaves method.");
    return comdb2_SQLConnect(phdbc);
}

#endif

/**
 * Parses values from the connection string.
 */
static void get_conn_attrs(char *str, conn_info_t *ci)
{
    char *pch, *equal, *key, *value, *tail_of_value;
    char flag[MAX_INT64_STR_LEN];

    pch = strtok(str, ";");
    while(pch != NULL) {
        if(equal = strchr(pch, '=')) {

            *equal = '\0';
            key = pch;
            value = equal + 1;
            tail_of_value = &value[strlen(value) - 1];

            for( ; *value == '{'; ++value) ;
            for( ; *tail_of_value == '}'; --tail_of_value) ;

            /* We cannot simply set *tail_of_value to NIL coz it may make strtok() terminate unexpectedly. */

            if(!strcasecmp(key, "dsn"))
                my_strncpy_in(ci->dsn, MAX_CONN_ATTR_LEN, value, tail_of_value - value + 1);
            else if(!strcasecmp(key, "driver"))
                my_strncpy_in(ci->driver, MAX_CONN_ATTR_LEN, value, tail_of_value - value + 1);
            else if(!strcasecmp(key, "database"))
                my_strncpy_in(ci->database, MAX_CONN_ATTR_LEN, value, tail_of_value - value + 1);
            else if(!strcasecmp(key, "cluster"))
                my_strncpy_in(ci->cluster, MAX_CONN_ATTR_LEN, value, tail_of_value - value + 1);
            else if(!strcasecmp(key, "flag")) {
                my_strncpy_in(flag, MAX_INT64_STR_LEN, value, tail_of_value - value + 1);
                ci->flag = atoi(flag);
            }
        }
        pch=strtok(NULL, ";");
    }

    __info("dsn=%s; driver=%s; database=%s; cluster=%s; flag=%d.", 
            ci->dsn, ci->driver, ci->database, ci->cluster, ci->flag);
}

SQLRETURN SQL_API SQLDriverConnect(
        SQLHDBC         hdbc,
        SQLHWND         hwnd,
        SQLCHAR         *in_conn_str,
        SQLSMALLINT     in_conn_strlen,
        SQLCHAR         *out_conn_str,
        SQLSMALLINT     out_conn_str_max,
        SQLSMALLINT     *out_conn_strlen,
        SQLUSMALLINT    drv_completion)
{
    dbc_t *phdbc = (dbc_t *)hdbc;
    char _instr[MAX_CONN_INFO_LEN];
    char _outstr[MAX_CONN_INFO_LEN];
    conn_info_t *ci;

    __debug("enters method.");

    if(!hdbc)
        return SQL_INVALID_HANDLE;

    if(phdbc->connected)
        return DBC_ODBC_ERR(ERROR_CONN_IN_USE);
    
    ci = &phdbc->ci;
    my_strncpy_in_fn(_instr, MAX_CONN_INFO_LEN, (char *)in_conn_str, in_conn_strlen);

    get_conn_attrs(_instr, ci);

    switch(drv_completion) {
#ifdef __UNIX__
        case SQL_DRIVER_PROMPT:
        case SQL_DRIVER_COMPLETE:
        case SQL_DRIVER_COMPLETE_REQUIRED:
#endif      
        case SQL_DRIVER_NOPROMPT:
        break;
    }

#if defined(__UNIXODBC__) || defined(__IODBC__)
    if(ci->database[0] == '\0' || ci->cluster[0] == '\0')
        /* Partial information provided, use .odbc.ini to complete connection info. */
        complete_conn_info_by_dm(ci);
#endif

    if(SQL_FAILED(comdb2_SQLConnect(phdbc)))
        return SQL_ERROR;

    snprintf(_outstr, MAX_CONN_INFO_LEN, "dsn=%s;driver=%s;database=%s;cluster=%s;flag=%d.", 
            ci->dsn, ci->driver, ci->database, ci->cluster, ci->flag);

    if(out_conn_str)
        my_strncpy_out_fn((char *)out_conn_str, _outstr, out_conn_str_max);
    if(out_conn_strlen)
        *out_conn_strlen = strlen(_outstr);

    __debug("leaves method.");

    return (strlen(_outstr) >= out_conn_str_max) ? DBC_ODBC_ERR(ERROR_STR_TRUNCATED) : SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC hdbc)
{
    dbc_t *phdbc = (dbc_t *)hdbc;

    __debug("enters method.");

    if(!hdbc) 
        return SQL_INVALID_HANDLE;
    
    if(!phdbc->connected)
        return DBC_ODBC_ERR(ERROR_CONN_NOT_OPEN);

    if(phdbc->sqlh) {

        if(phdbc->in_txn || phdbc->sqlh_status == SQLH_EXECUTING)
            return DBC_ODBC_ERR(ERROR_INVALID_TRANS_STATE);

        if(phdbc->sqlh_status == SQLH_FINISHED) {
            while(cdb2_next_record(phdbc->sqlh) == CDB2_OK); 
            phdbc->sqlh_status = SQLH_IDLE;
        }

        cdb2_close(phdbc->sqlh);
    }

    phdbc->connected = false;

    __debug("leaves method.");

    return SQL_SUCCESS;
}
