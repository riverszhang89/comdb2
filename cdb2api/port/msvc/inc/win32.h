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

#ifndef _INCLUDED_PORT_MSVC_WIN32_H_
#define _INCLUDED_PORT_MSVC_WIN32_H_
#define WIN32_LEAN_AND_MEAN
#define _CRT_SECURE_NO_WARNINGS
#include <windows.h>

/* MSVC does not like C99.
   MSVC does not like POSIX. */
#define inline __inline
#ifndef __func__
#define __func__ __FUNCTION__
#endif
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

#include <string.h>
#define strdup _strdup
#define strtok_r strtok_s
#define snprintf sprintf_s

/* MSVC does not have strndup(). Define our own. */
char *strndup(const char *s, size_t n);
#endif
