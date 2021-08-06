/*
   Copyright 2016 Bloomberg Finance L.P.

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

int evbuffer_read_ssl(struct evbuffer *, sslio *, evutil_socket_t, int howmuch);
int evbuffer_write_ssl(struct evbuffer *, sslio *, evutil_socket_t);

#else
extern int not_built_with_ssl;
#endif

#endif
