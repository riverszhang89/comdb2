/*
   Copyright 2021 Bloomberg Finance L.P.

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

#ifndef _INCLUDED_SSL_IO_EVBUFFER_H_
#define _INCLUDED_SSL_IO_EVBUFFER_H_

#if WITH_SSL
#include <event2/buffer.h>
#include <event2/event.h>

#include "ssl_io.h"

/* 
   ABOUT THE RETURN VALUE

   A return value of 0 does not necessarily mean EOL in OpenSSL.

   ABOUT DRAIN_PENDING

   OpenSSL processes data in blocks. Therefore OpenSSL has its own buffering layer to store processed but yet unread
   bytes (aka pending bytes). This unfortunately does not work quite well with libevent in the case below.

   Consider a data block of 10 bytes. Caller first reads 5 bytes, process them, and schedule an EV_READ event
   for the rest bytes. This would work fine in plaintext, as the fd would be ready to read from. However in OpenSSL,
   this event will not be fired, as the other 5 bytes are already read into SSL and hence the fd is actually not readable
   at this point.
   
   To solve this problem, `drain_pending' is introduced. When `drain_pending' is set to 1, this function will attempt to drain
   the pending bytes from OpenSSL. The final number of bytes read, as a result, may be larger than `howmuch'. When `drain_pending'
   is set to 0, this function will not drain the pending bytes. The number of bytes read will be precisely `howmuch' (unless an error occurs).
   However, caller must check for `sslio_pending()' and drain the pending bytes, before scheduling an EV_READ event. See 'newsql_sp_cmd()` 
   for an example. */
int evbuffer_read_ssl(struct evbuffer *, sslio *, evutil_socket_t, int howmuch, int drain_pending);

int evbuffer_write_ssl(struct evbuffer *, sslio *, evutil_socket_t);
#else
#define evbuffer_read_ssl(buf, unused1, fd, len, unused2) evbuffer(buf, fd, len)
#define evbuffer_write_ssl(buf, unused, fd) evbuffer_write(buf, fd)
#endif
#endif
