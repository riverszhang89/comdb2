#include <stdio.h>
#include "cdb2api.h"

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <db> <cluster>\n", argv[0]);
		exit(1);
	}
	printf("Testing connection to %s@%s\n", argv[1], argv[2]);
	cdb2_hndl_tp *hndl;
	int rc = cdb2_open(&hndl, argv[1], argv[2], 0);
	if (rc) goto err;
	rc = cdb2_run_statement(hndl, "SELECT cast(now(6) as TEXT)");
	if (rc) goto err;

	while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
		printf(">>> Database time: %s\n", (char *)cdb2_column_value(hndl, 0));
	if (rc != CDB2_OK_DONE)
		goto err;
	printf("Success!\n");
	return 0;
err:
	fprintf(stderr, "error %d: %s", rc, cdb2_errstr(hndl));
	return rc;
}
