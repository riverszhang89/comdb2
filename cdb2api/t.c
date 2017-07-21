#include <stdio.h>
#include "cdb2api.h"

int main()
{
	cdb2_hndl_tp *hndl;
	cdb2_open(&hndl, "mydb", "@127.0.0.1:port=19016", 0);
	cdb2_run_statement(hndl, "SELECT cast(now(6) as TEXT)");
	while (cdb2_next_record(hndl) == CDB2_OK)
		printf(">>> Database time: %s\n", (char *)cdb2_column_value(hndl, 0));
	return 0;
}
