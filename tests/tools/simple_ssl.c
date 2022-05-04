#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <time.h>
#include <sys/time.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc, i, len;
    char *conf = getenv("CDB2_CONFIG");
    struct timeval tv;
    unsigned long long tm1, tm2;
    const char *tier;

    if (argc < 2)
        exit(1);

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (argc > 3)
        len = atoi(argv[3]);
    else
        len = 2000;

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);


    gettimeofday(&tv, NULL);
    tm1 = tv.tv_sec * 1000000ULL + tv.tv_usec;

    /* The race is on! first let's test no ssl cache. */
    for (i = 0; i < len; ++i) {
        if (i > 0 && i % 100 == 0)
            fprintf(stderr, "progress: %d\n", i);
        /* 1st */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_DIRECT_CPU);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);
    }

    gettimeofday(&tv, NULL);
    tm2 = tv.tv_sec * 1000000ULL + tv.tv_usec;

    fprintf(stderr, "Without SSL session cache: %llu.\n", (tm2 - tm1) / len);

    /* Now test ssl cache */
    setenv("SSL_SESSION_CACHE", "1", 1);
    setenv("SSL_KEY", "/tmp/certs/client.key", 1);
    setenv("SSL_CERT", "/tmp/certs/client.crt", 1);
    setenv("SSL_CA", "/tmp/certs/root.crt", 1);
    setenv("SSL_CRL", "/tmp/certs/root.crl", 1);

    setenv("SSL_MODE", "VERIFY_CA", 1);


    gettimeofday(&tv, NULL);
    tm1 = tv.tv_sec * 1000000ULL + tv.tv_usec;

    for (i = 0; i < len; ++i) {
        if (i > 0 && i % 100 == 0)
            fprintf(stderr, "progress: %d\n", i);

        /* 1st */
        rc = cdb2_open(&hndl, argv[1], tier, CDB2_DIRECT_CPU);
        if (rc != 0) {
            fprintf(stderr, "Error opening a handle: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }

        rc = cdb2_run_statement(hndl, "select 1");
        if (rc != 0) {
            fprintf(stderr, "Error running query: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "Error next record: %d: %s.\n",
                    rc, cdb2_errstr(hndl));
            exit(1);
        }
        cdb2_close(hndl);
    }

    gettimeofday(&tv, NULL);
    tm2 = tv.tv_sec * 1000000ULL + tv.tv_usec;
    fprintf(stderr, "With SSL session cache:  %llu.\n", (tm2 - tm1) / len);

    return 0;
}
