#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <cdb2api.h>

static void *my_simple_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    puts((char *)user_arg);
    return NULL;
}

static void *my_arg_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    printf("SQL is %s\n", (char *)argv[0]);
    printf("Hostname is %s\n", (char *)argv[1]);
    printf("Port is %d\n", (int)(intptr_t)argv[2]);
    return NULL;
}

static void *my_fake_pmux_port_hook(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    int port = (int)(intptr_t)argv[0];
    if (port > 0)
        puts("Got a valid port");
    return (void*)(intptr_t)(-1);
}

static cdb2_event *init_once_event;

static void register_once(void)
{
    init_once_event = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "INIT ONCE", 0);
}

static int init_once_registration(const char *db, const char *tier)
{
    extern void (*cdb2_init_events)(void);
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    cdb2_init_events = register_once;
    cdb2_open(&hndl, db, tier, 0);

    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    cdb2_unregister_event(NULL, init_once_event);
    cdb2_close(hndl);
    if (rc != CDB2_OK_DONE)
        return 1;
    return 0;
}

static int simple_register_unregister(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e1, *e2, *e3, *e4;
    cdb2_hndl_tp *hndl = NULL;

    e1 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "1", 0);
    e2 = cdb2_register_event(NULL, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "2", 0);
    cdb2_open(&hndl, db, tier, 0);
    e3 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "3", 0);
    e4 = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_simple_hook, "4", 0);

    puts("Should see 1 2 3 4");

    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;

    cdb2_unregister_event(hndl, e3);
    puts("Should see 1 2 4");
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE)
        return 1;
    cdb2_close(hndl);
    cdb2_unregister_event(NULL, e1);
    cdb2_unregister_event(NULL, e2);
    return 0;
}

static int arg_events(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e;
    cdb2_hndl_tp *hndl = NULL;

    cdb2_open(&hndl, db, tier, 0);
    e = cdb2_register_event(hndl, CDB2_BEFORE_SEND_QUERY, 0, my_arg_hook, NULL, 3, CDB2_SQL, CDB2_HOSTNAME, CDB2_PORT);
    cdb2_run_statement(hndl, "SELECT 1");
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    cdb2_close(hndl);
    if (rc != CDB2_OK_DONE)
        return 1;
    return 0;
}

static int modify_rc_event(const char *db, const char *tier)
{
    int rc;
    cdb2_event *e;
    cdb2_hndl_tp *hndl = NULL;

    e = cdb2_register_event(NULL, CDB2_BEFORE_PMUX, CDB2_OVERWRITE_RETURN_VALUE, my_fake_pmux_port_hook, NULL, 1, CDB2_RETURN_VALUE);
    cdb2_open(&hndl, db, tier, 0);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    if (rc == 0)
        return 1;
    puts(cdb2_errstr(hndl));
    return 0;
}

int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    char *tier = "local";
    int rc;

    puts("====== INIT ONCE REGISTRATION ======");
    rc = init_once_registration(argv[1], tier);
    if (rc != 0)
        return rc;

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }

    puts("====== SIMPLE REGISTRATION AND UNREGISTRATION ======");
    rc = simple_register_unregister(argv[1], tier);
    if (rc != 0)
        return rc;

    puts("====== EVENT WITH ADDITIONAL INFORMATION ======");
    rc = arg_events(argv[1], tier);
    if (rc != 0)
        return rc;

    puts("====== EVENT THAT INTERCEPTS AND OVERWRITES THE RETURN VALUE ======");
    rc = modify_rc_event(argv[1], tier);
    if (rc != 0)
        return rc;
    return 0;
}
