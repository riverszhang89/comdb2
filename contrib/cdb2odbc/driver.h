/**
 * @file driver.h 
 * @description Definitions needed by the driver
 * @author Rivers Zhang <hzhang320@bloomberg.net>

 * @history
 * 16-Jun-2014 Created.
 */

#ifndef _DRIVER_H_
#define _DRIVER_H_

#if defined(unix) || defined(__unix) || defined(__unix__)
# define __UNIX__ 1
#endif

#ifdef __UNIX__
# include <sql.h>
# include <sqlext.h>

# ifndef SQL_API
#  define SQL_API
# endif

# include <unistd.h>
# ifdef __THREADS__
#  include <pthread.h>
# endif /* pthread */
#endif /* __UNIX__ */

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>
#include <wchar.h>

#include <cdb2api.h>

#include "log.h"
#include "list.h"
#include "util.h"
#include "error.h"

/* ================== Who am I? ================== */

#define DRVNAME         "COMDB2-ODBC"
#define DRVVER          "1.0.0"
#define DBNAME          "COMDB2"
#define DBVER           "R5"

#undef ODBCVER
#define ODBCVER         0x0380
#define DRVODBCVER      "03.80"

#define DRV_FILE_NAME   "libcomdb2odbc.so"

/* ================== Limits start ================ */

#define MAX_NUMERIC_LEN 128 
#define MAX_CONN_ATTR_LEN 256
#define MAX_CONN_INFO_LEN 2048 
#define MAX_INTERNAL_QUERY_LEN 2048 

#define MAX_INT64_DIGITS 20
#define MAX_INT64_DISPLAY_SIZE 20
#define MAX_INT64_STR_LEN MAX_INT64_DISPLAY_SIZE + 1
#define MAX_DBL_DIGITS 15
#define MAX_DBL_DISPLAY_SIZE 24
#define MAX_DBL_STR_LEN 25
#define MAX_DATETIME_DISPLAY_SIZE 24 + CDB2_MAX_TZNAME /* yyyy-mm-dd hh:MM:ss.fff */
#define MAX_DATETIME_STR_LEN MAX_DATETIME_DISPLAY_SIZE + 1
#define MAX_YM_DISPLAY_SIZE 14
#define MAX_YM_STR_LEN MAX_YM_DISPLAY_SIZE + 1
#define MAX_DS_DISPLAY_SIZE 24
#define MAX_DS_STR_LEN MAX_DS_DISPLAY_SIZE + 1

/* ================== Limits end ================ */

/* ================== Data Structures start ================ */

typedef unsigned long long int ULL;
typedef signed   long long int LL;
typedef unsigned short int     USI;
typedef signed   short int     SI;
typedef unsigned long int      ULI;
typedef signed   long int      LI;

/* Environment handle */
typedef struct env {
    list_t conns;               /* list of allocated connection handles */
    err_t *error;               /* the previous error of this environment handle (if there's any) */

#ifdef __THREADS__
    pthread_mutex_t lock;
#endif
} env_t;

/* Connection information needed by SQLDriverConnect. */
typedef struct conn_info {
    char dsn[MAX_CONN_ATTR_LEN];            /* datasource name */
    char driver[MAX_CONN_ATTR_LEN];         /* driver name */
    char database[MAX_CONN_ATTR_LEN];       /* database name */
    char cluster[MAX_CONN_ATTR_LEN];        /* cluster name */
    int flag;                               /* used by CDB2API */
} conn_info_t;

typedef enum {
    SQLH_IDLE = 0,
    SQLH_FINISHED,
    SQLH_EXECUTING
} hndl_status_t;

/* Connection handle */
typedef struct dbc {

    /* handle related properties */

    env_t *env;                 /* ptr to its corresponding environment handle */
    list_t stmts;               /* list of allocated statment handles */
    list_head_t list;           /* a node in doubly linkedlist. */
    err_t *error;               /* the previous error of this connection handle (if there's any) */
    conn_info_t ci;             /* conn_info_t */

    hndl_status_t sqlh_status;
    cdb2_hndl_tp *sqlh;         /* comdb2 sql handle */

    /* flags */

    bool connected;             /* is it connected already? */
    bool auto_commit;           /* auto commit? */
    bool txn_changed;
    int txn_isolation;
    bool in_txn;                /* in transaction? */
    bool brand_new;             /* brand new? (meaning no SQL has been executed.) */

#ifdef __THREADS__
    pthread_mutex_t lock;
#endif
} dbc_t;

/* SQL statement types */
typedef enum {
    STMT_SELECT = 0,
    STMT_INSERT,
    STMT_UPDATE,
    STMT_DELETE,
    STMT_HAS_NO_EFFECT, /* used as a boundary. */

    STMT_SET,
    STMT_BEGIN,
    STMT_COMMIT,
    STMT_ROLLBACK,

    /* hmm... seems comdb2 does not support following types */
    STMT_CREATE,
    STMT_ALTER,
    STMT_DROP,
    STMT_GRANT,
    STMT_REVOKE,

    STMT_UNDEFINED
} sql_type_t;

/* Statement status. */
typedef enum {
    STMT_ALLOCATED  = 0x1,      /* allocated, but not used so far */
    STMT_READY      = 0x2,      /* waiting to be executed */
    STMT_PREMATURE  = 0x4,      /* the statement has been executed before a call to SQLExecute but after SQLPrepare. */
    STMT_FINISHED   = 0x8,      /* okay I'm done */
    STMT_EXECUTING  = 0x10,     /* execution is going on, please be patient */
    STMT_EXTRACTED  = 0x20,     /* All rows are fetched. */

    STMT_TYPE_INFO  = 0x40
    
} stmt_status;

/* Parameter struct (See SQLBindParameter). */
typedef struct param {
    bool used;                  /* used? */
    SI io_type;                 /* input-output mode */
    SI c_type;                  /* native c type */
    SI sql_type;                /* odbc sql type */
    SI scale;                   /* scale. may be ignored. */
    unsigned int precision;     /* precision. may be ignored. */
    SQLLEN buflen;              /* maximum length of @buf. */
    SQLLEN *str_len;            /* deferred length of @buf. */
    void *buf;                  /* buffer */
    void *internal_buffer;      /* internal buffer for saving intermediate result. */
    char name[MAX_INT64_DIGITS + 7];              /* name of parameter (e.g, @uuid). the leading `@' is required. */
} param_t;

/* Data buffer (see SQLBindCol) */
typedef struct data_buffer {
    bool used;                  /* used? */
    short int c_type;           /* native c type */
    SQLLEN buffer_length;     /* length of @buffer */
    SQLLEN *required;             /* required length of @buffer */
    void *buffer;               /* buffer */
} data_buffer_t;

/* Statement handle */
typedef struct stmt {

    /* handle related properties */

    dbc_t *dbc;                 /* ptr to its corresponding connection handle */
    list_head_t list;           /* a node in doubly linkedlist */

    /* This is a copy (for avoiding reference) of dbc->sqlh. 
       dbc is responsible for closing the comdb2 handle, not the statement. */
    cdb2_hndl_tp *sqlh;         /* comdb2 sql handle */

    /* statement specific information */

    char *query;                /* passed in by users. it may contains variable markers. */
    char *query_with_params;    /* parsed by the driver. it contains no variable marker. */
    stmt_status status;
    sql_type_t sql_type;        /* Type of statement. */    
    bool changed;

    /* result */
    unsigned int col_count;     /* Don't tell me your column count is more than 4 billion. */
    effects_tp *effects;        /* Numbers of affected, selected, updated, inserted and deleted rows. */
    err_t *error;

    int params_allocated;
    param_t *params;            /* Parameters. */

    int num_data_buffers;
    data_buffer_t *buffers;     /* Data buffers. */

    unsigned int num_params;             /* The number of parameters in this statement. */

#ifdef __THREADS__
    pthread_mutex_t lock;
#endif
} stmt_t;

/* ================== Data Structures end ================ */

/* ================== Macros start ================ */

#define SQL_FAILED !SQL_SUCCEEDED
#define IS_VALID_TXN_MODE(stmt) (stmt->dbc->txn_isolation > 0)
#define TXN_MODE(stmt) (stmt->dbc->txn_isolation == SQL_TXN_READ_COMMITTED ? \
        "SET TRANSACTION READ COMMITTED" : "SET TRANSACTION SNAPSHOT")

#define SET_EXTRACTED(stmt) do {                \
    stmt->status = STMT_EXTRACTED;              \
    stmt->dbc->sqlh_status = SQLH_IDLE;         \
}while(0)

#define SET_EXECUTING(stmt) do {                \
    stmt->status = STMT_EXECUTING;              \
    stmt->dbc->sqlh_status = SQLH_EXECUTING;    \
}while(0)

#define SET_FINISHED(stmt) do {                 \
    stmt->status = STMT_FINISHED;               \
    stmt->dbc->sqlh_status = SQLH_FINISHED;     \
}while(0)

#define SET_SQLH_IDLE(stmt) do { \
    stmt->dbc->sqlh_status = SQLH_IDLE; \
}while(0)

#define SET_SQLH_EXECUTING(stmt) do { \
    stmt->dbc->sqlh_status = SQLH_EXECUTING;    \
}while(0)

#define SET_SQLH_FINISHED(stmt) do { \
    stmt->dbc->sqlh_status = SQLH_FINISHED; \
}while(0)

#define SQLH_STATUS(stmt) (stmt)->dbc->sqlh_status

#define IS_NEW(stmt)        stmt->dbc->brand_new
#define IS_OLD(stmt)        !stmt->dbc->brand_new
#define SET_OLD(stmt)       do { stmt->dbc->brand_new = false; } while(0)
#define AUTO_COMMIT(stmt)   stmt->dbc->auto_commit
#define IS_IN_TXN(stmt)     stmt->dbc->in_txn
#define IS_OUT_OF_TXN(stmt) !stmt->dbc->in_txn
#define IN_TXN(stmt)        do { stmt->dbc->in_txn = true;  } while(0)
#define OUT_OF_TXN(stmt)    do { stmt->dbc->in_txn = false; } while(0)

#ifdef __THREADS__

#define LOCK_STMT(stmt)         pthread_mutex_lock(&stmt->lock)
#define UNLOCK_STMT(stmt)       pthread_mutex_unlock(&stmt->lock)
#define LOCK_STMT_DBC(stmt)     pthread_mutex_lock(&stmt->dbc->lock)
#define UNLOCK_STMT_DBC(stmt)   pthread_mutex_unlock(&stmt->dbc->lock)
#define LOCK_DBC(dbc)           pthread_mutex_lock(&dbc->lock)
#define UNLOCK_DBC(dbc)         pthread_mutex_unlock(&dbc->lock)
#define LOCK_DBC_ENV(dbc)       pthread_mutex_lock(&dbc->env->lock)
#define UNLOCK_DBC_ENV(dbc)     pthread_mutex_unlock(&dbc->env->lock)
#define LOCK_ENV(env)           pthread_mutex_lock(&env->lock)
#define UNLOCK_ENV(env)         pthread_mutex_unlock(&env->lock)

#endif

/* ================== Macros end ================ */

#endif /* _DRIVER_H_ */
