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

#ifndef _INCLUDED_PORT_AIX_H_
#define _INCLUDED_PORT_AIX_H_

#include "posix.h"

#undef cdb2_gethostbyname
#define cdb2_gethostbyname(hp, nm) do {	\
	struct hostent_data ht_data;	\
	gethostbyname_r(nm, hp, &ht_data);	\
} while (0)

#include <sys/machine.h>
#if BYTE_ORDER == LITTLE_ENDIAN
#define __LITTLE_ENDIAN__ 1
#else
#define __LITTLE_ENDIAN__ 0
#endif

#define HAVE_MSGHDR_MSG_CONTROL

#endif
