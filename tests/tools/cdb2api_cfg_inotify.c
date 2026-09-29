/* Reads step labels from stdin. For each, prints "<label> OK|FAIL <config files read from disk>".
   "fork" probes from a child first, then from the parent. */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

static const char *dbname;

static void probe(const char *label)
{
    int reads = get_num_cfg_file_reads();
    cdb2_hndl_tp *hndl = NULL;
    int ok = cdb2_open(&hndl, dbname, "default", 0) == 0 && cdb2_run_statement(hndl, "select 1") == CDB2_OK;
    if (ok) {
        int rc;
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
            ;
        ok = rc == CDB2_OK_DONE;
    }
    printf("%s %s %d\n", label, ok ? "OK" : "FAIL", get_num_cfg_file_reads() - reads);
    fflush(stdout);
    cdb2_close(hndl);
}

int main(int argc, char **argv)
{
    char label[256];

    if (argc != 3) {
        fprintf(stderr, "usage: %s <dbname> <comdb2db.cfg>\n", argv[0]);
        return 1;
    }
    set_fail_sockpool(-1); /* no pooled connections: every probe connects through pmux */
    dbname = argv[1];
    cdb2_set_comdb2db_config(argv[2]);

    while (fgets(label, sizeof(label), stdin)) {
        label[strcspn(label, "\n")] = '\0';
        if (strcmp(label, "fork") == 0) {
            if (fork() == 0) {
                probe("child");
                _exit(0);
            }
            wait(NULL);
            probe("parent");
        } else {
            probe(label);
        }
    }
    return 0;
}
