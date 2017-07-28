#ifndef _INCLUDED_PORT_MSVC_SYS_TIME_H_
#define _INCLUDED_PORT_MSVC_SYS_TIME_H_
#include <winsock2.h>
int gettimeofday(struct timeval *tv, struct timezone *tz);
#endif
