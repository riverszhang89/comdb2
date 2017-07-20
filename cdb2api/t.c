#include <stdio.h>
#include "cdb2api.h"

int main()
{
	cdb2_hndl_tp *hndl;
	cdb2_open(&hndl, "riversdb", "nylxdev2.dev.bloomberg.com", 0);
	cdb2_run_statement(hndl, "SELECT COMDB2_NODE()");
	printf("host is %s\n", (char *)cdb2_column_value(hndl, 0));
	getc(stdin);
	return 0;
}
