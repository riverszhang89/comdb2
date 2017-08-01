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

#ifndef _INCLUDED_PORT_WINSOCKETS_H_
#define _INCLUDED_PORT_WINSOCKETS_H_

#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

typedef unsigned long int in_addr_t;

#define close() closesocket()

#define fcntlnonblocking(s, flag) (flag = 1, ioctlsocket(s, FIONBIO, &flag))
#define fcntlblocking(s, flag) (flag = 0, ioctlsocket(s, FIONBIO, &flag))

/* Error codes set by Windows Sockets are
   not made available through the errno variable.
   Use our own. */

#include <error.h>
#define errno WSAGetLastError()
#define seterrno(err) WSASetLastError(err)

#include <string.h>
char *WSAStrError(int err);
#define strerror(err) WSAStrError(err)

#define EINPROGRESS WSAEWOULDBLOCK
#define EINTR WSAEINPROGRESS

#endif
