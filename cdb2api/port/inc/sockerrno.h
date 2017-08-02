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

/* There're multiple errno definitions in MSVC CRT library.
   Hence the file must be included after core CRT headers to
   make sure that errno is redefined to our own. */

#ifndef _INCLUDED_PORT_SOCKERRNO_H_
#define _INCLUDED_PORT_SOCKERRNO_H_

#ifdef _WIN32 /* Windows Sockets */

/* Error codes set by Windows Sockets are
   not made available through the errno variable.
   Use our own. */

#include <errno.h>
#undef errno
#define errno WSAGetLastError()

/* Windows ugliness: WSAGetLastError() is not a valid lvalue thus
   it is impossible to do things like `errno=EINVAL'. */
#define seterrno(err) WSASetLastError(err)

#include <string.h>
char *WSAStrError(int err);
#undef strerror
#define strerror(err) WSAStrError(err)

/* Map WinSock error codes to Berkeley errors */

#undef EINPROGRESS
#define EINPROGRESS WSAEWOULDBLOCK

#undef EINTR
#define EINTR WSAEINPROGRESS

#else /* Berkeley Sockets */
#include <errno.h>
/* To be consistent with the aforementioned Windows-ism. */
#define seterrno(err) do { errno = err; } while (0)
#endif /* _WIN32 */

#endif /* _INCLUDED_PORT_SOCKERRNO_H_ */
