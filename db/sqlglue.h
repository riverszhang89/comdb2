#ifndef SQLGLUE_H
#define SQLGLUE_H

#include <list.h>
#include <stdint.h>

struct Mem;
struct sqlthdstate;
struct sqlclntstate;
struct sqlite3_context;
struct sqlite3_value;
struct sqlite3;
struct dbtable;
struct dbenv;

typedef struct {
    struct sqlthdstate *thd;
    char *name;
} lua_func_arg_t;

struct lua_func_t {
    char *name;
    int flags;
    LINKC_T(struct lua_func_t) lnk;
};
int lua_func_list_free(void * list);

void get_sfuncs(listc_t* funcs);
void get_afuncs(listc_t* funcs);
 
int find_lua_sfunc(const char *);
int find_lua_afunc(const char *);

void lua_func(struct sqlite3_context *, int, struct sqlite3_value **);

void lua_step(struct sqlite3_context *, int, struct sqlite3_value **);
void lua_final(struct sqlite3_context *);

int osql_delrec_qdb(struct sqlclntstate *, char *, uint64_t);
int lua_sfunc_used(const char *func, char**tbl);

int resolve_sfuncs_for_table(struct sqlite3 *db, struct dbtable *tbl);
int resolve_sfuncs_for_db(struct dbenv* thedb);

#endif
