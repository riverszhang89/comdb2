#include <stdio.h>
#include "cdb2api.h"

int main()
{
	cdb2_hndl_tp *hndl;
	int rc = cdb2_open(&hndl, "mydb", "localhost", 4);
	if (rc) goto err;
	rc = cdb2_run_statement(hndl, "SELECT cast(now(6) as TEXT)");
	if (rc) goto err;

	while (cdb2_next_record(hndl) == CDB2_OK)
		printf(">>> Database time: %s\n", (char *)cdb2_column_value(hndl, 0));
	return 0;

err:
	fprintf(stderr, "error %d: %s", rc, cdb2_errstr(hndl));
	return rc;
}
