/* Config reading cost with the inotify cache vs. re-reading every call (the fallback, same as the old behavior).
   Prints: <phase> <cached|uncached> <ns per call> <config files read per call> */
#include <time.h>
#include <cdb2api.c>

static const char *dbname;

static double now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}

static void config_pass(void)
{
    static char hosts[MAX_NODES][CDB2HOSTNAME_LEN], db_hosts[MAX_NODES][CDB2HOSTNAME_LEN];
    int n_hosts, num, n_db_hosts, dbnum;
    read_available_comdb2db_configs(NULL, hosts, COMDB2DB, &n_hosts, &num, dbname, db_hosts, &n_db_hosts, &dbnum, NULL,
                                    NULL);
}

static void query(void)
{
    cdb2_hndl_tp *hndl = NULL;
    if (cdb2_open(&hndl, dbname, "default", 0) || cdb2_run_statement(hndl, "select 1")) {
        fprintf(stderr, "query failed: %s\n", cdb2_errstr(hndl));
        exit(1);
    }
    while (cdb2_next_record(hndl) == CDB2_OK)
        ;
    cdb2_close(hndl);
}

static void run(const char *phase, const char *mode, void (*fn)(void), int n)
{
    fn(); /* warm up; use_inotify takes effect on the pass after it is parsed */
    fn();
    int reads = get_num_cfg_file_reads();
    double start = now_ns();
    for (int i = 0; i < n; ++i)
        fn();
    printf("%s %s %.0f %.2f\n", phase, mode, (now_ns() - start) / n, (double)(get_num_cfg_file_reads() - reads) / n);
    fflush(stdout);
}

int main(int argc, char **argv)
{
    if (argc != 5) {
        fprintf(stderr, "usage: %s <dbname> <comdb2db.cfg> <config passes> <queries>\n", argv[0]);
        return 1;
    }
    dbname = argv[1];
    cdb2_set_comdb2db_config(argv[2]);
    int passes = atoi(argv[3]), queries = atoi(argv[4]);

    run("config", "cached", config_pass, passes);
    run("query", "cached", query, queries);

    pthread_mutex_lock(&cdb2_cfg_lock);
    cdb2_use_inotify = 0;
    cdb2_use_inotify_set_from_env = 1; /* ignore use_inotify in the config */
    pthread_mutex_unlock(&cdb2_cfg_lock);

    run("config", "uncached", config_pass, passes);
    run("query", "uncached", query, queries);
    return 0;
}
