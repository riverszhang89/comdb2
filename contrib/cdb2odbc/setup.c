#include "driver.h"

#if defined(__IODBC__)
# include <iodbcinst.h>
#else
# include <odbcinst.h>
#endif

#ifndef INSTAPI
#define INSTAPI
#endif

BOOL INSTAPI ConfigDriver(HWND hwnd,
                          WORD fRequest,
                          LPCSTR lpszDriver,
                          LPCSTR lpszArgs,
                          LPSTR  lpszMsg,
                          WORD   cbMsgMax,
                          WORD  *pcbMsgOut)
{
	if (fRequest != ODBC_INSTALL_DRIVER)
		return FALSE;

	if (lpszDriver == NULL)
		return FALSE;

	if (!SQLWritePrivateProfileString(lpszDriver, "APILevel", "1", ODBCINST_INI))
		return FALSE;
	if (!SQLWritePrivateProfileString(lpszDriver, "ConnectFunctions", "YYN", ODBCINST_INI))
		return FALSE;
	if (!SQLWritePrivateProfileString(lpszDriver, "FileUsage", "0", ODBCINST_INI))
		return FALSE;
	if (!SQLWritePrivateProfileString(lpszDriver, "SQLLevel", "1", ODBCINST_INI))
		return FALSE;
	if (!SQLWritePrivateProfileString(lpszDriver, "DriverODBCVer", DRVODBCVER, ODBCINST_INI))
		return FALSE;
	return TRUE;
}

BOOL INSTAPI ConfigDSN(
     HWND     hwndParent,
     WORD     fRequest,
     LPCSTR   lpszDriver,
     LPCSTR   lpszAttributes)
{
	LPCSTR dsn, db, cluster, kv, pos, *dst;
	dsn = db = cluster = NULL;
    dst = NULL;

	if (lpszDriver == NULL)
		return FALSE;

	for (pos = lpszAttributes; *pos; ++pos) {
        dst = NULL;
		kv = pos;
		for ( ; ; ++pos) {
			if (*pos == '\0')
				return FALSE;
			else if (*pos == '=')
				break;
		}

        if (strncasecmp("dsn", kv, 3) == 0)
            dst = &dsn;
        else if (strncasecmp("database", kv, 8) == 0)
            dst = &db;
        else if (strncasecmp("cluster", kv, 7) == 0)
            dst = &cluster;

        /* Skip values */
        kv = ++pos;
        for (; *pos != '\0'; ++pos) { /* blank */ };
        if (dst != NULL)
            *dst = strdup(kv);
	}

	switch (fRequest) {
		case ODBC_ADD_DSN:
		case ODBC_CONFIG_DSN:
            if (dsn == NULL || db == NULL || cluster == NULL)
                return FALSE;
            if (!SQLWriteDSNToIni(dsn, lpszDriver))
                return FALSE;
            if (!SQLWritePrivateProfileString(dsn, "DATABASE", db, ODBC_INI))
                return FALSE;
            if (!SQLWritePrivateProfileString(dsn, "CLUSTER", cluster, ODBC_INI))
                return FALSE;
            free(dsn);
            free(db);
            free(cluster);
            return TRUE;
		case ODBC_REMOVE_DSN:
            if (dsn == NULL)
                return FALSE;
            return SQLRemoveDSNFromIni(dsn);
	}
    return FALSE;
}
