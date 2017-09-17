#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc, ninserts, nselects;
    char *conf = getenv("CDB2_CONFIG");
    void *data;
    unsigned long tm, delta;
    const char *tier;

    if (argc < 2)
        exit(1);

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (argc > 3)
        ninserts = atoi(argv[3]);
    else
        ninserts = 1024;

    if (argc > 3)
        nselects = atoi(argv[4]);
    else
        nselects = 10;

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    /* Open handle */
    rc = cdb2_open(&hndl, argv[1], tier, 0);
    if (rc != 0) {
        fprintf(stderr, "%d: Error opening handle: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    /* Create table */
    rc = cdb2_run_statement(hndl, "create table t { tag ondisk { int i blob b } }");
    if (rc != 0) {
        fprintf(stderr, "%d: Error running query: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "%d: Error next record: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    int tot = ninserts;
    data = malloc(512 * 1024 + tot);

    while (ninserts-- > 0) {
        int integer = ninserts % 10;
        /* Bind params */
        cdb2_bind_param(hndl, "i", CDB2_INTEGER, &integer, sizeof(integer));
        cdb2_bind_param(hndl, "b", CDB2_BLOB, data, 512 * 1024 + ninserts);

        /* insert */
        rc = cdb2_run_statement(hndl, "insert into t values(@i, @b)");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }

        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "%d: Error next record: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }

        cdb2_clearbindings(hndl);
    }

    /* Warm up */
    rc = cdb2_run_statement(hndl, "select b from t order by i");
    if (rc != 0) {
        fprintf(stderr, "%d: Error running query: %d: %s.\n",
                __LINE__, rc, cdb2_errstr(hndl));
        exit(1);
    }

    int cnt;
    for (cnt = 0; cdb2_next_record(hndl) == CDB2_OK; ++cnt);
    if (cnt != tot) {
        fprintf(stderr, "%d: Expecting %d rows, got %d.\n",
                __LINE__, tot, cnt);
        exit(1);
    }

    tm = (unsigned long)time(NULL);

    int i;
    for (i = 0; i != nselects; ++i) {
        rc = cdb2_run_statement(hndl, "select b from t order by i");
        if (rc != 0) {
            fprintf(stderr, "%d: Error running query: %d: %s.\n",
                    __LINE__, rc, cdb2_errstr(hndl));
            exit(1);
        }

        while (cdb2_next_record(hndl) == CDB2_OK);
    }

    delta = ((unsigned long)time(NULL)) - tm;
    fprintf(stderr, "runtime %lu.\n", delta);

    free(data);
    cdb2_close(hndl);
    return 0;
}
