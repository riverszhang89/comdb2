/*
   Copyright 2017, Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#ifndef _INCLUDED_PORT_WIN32_H_
#define _INCLUDED_PORT_WIN32_H_

#define __LITTLE_ENDIAN__ 1

#define WIN32_LEAN_AND_MEAN
#define _CRT_SECURE_NO_WARNINGS
#include <windows.h>
#include "winsockets.h"

#define cdb2_gethostbyname(hp, nm) do { hp = gethostbyname(nm); } while (0)

#define inline __inline
#ifndef __func__
#define __func__ __FUNCTION__
#endif
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

/* MSVC does not like POSIX. */
#include <string.h>
#ifndef strdup
#define strdup _strdup
#endif
#ifndef strtok_r
#define strtok_r strtok_s
#endif

#include <stdio.h>
#ifndef snprintf
#define snprintf sprintf_s
#endif

#include <stdlib.h>
#ifndef random
#define random() rand()
#endif
#ifndef srandom
#define srandom() srand()
#endif

/* MSVC does not have strndup(). Define our own. */
char *strndup(const char *s, size_t n);

/* Windows-style Paths */
static char CDB2DBCONFIG_NOBBENV[512] = "\\opt\\bb\\etc\\cdb2\\config\\comdb2db.cfg";
/* The real path is COMDB2_ROOT + CDB2DBCONFIG_NOBBENV_PATH */
static char CDB2DBCONFIG_NOBBENV_PATH[] = "\\etc\\cdb2\\config.d\\";
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "\\bb\\bin\\comdb2db.cfg";

/* Temporarily disable sockpool on Windows */
#ifdef _WIN32
#define WITH_SOCK_POOL 0
#endif

#endif
