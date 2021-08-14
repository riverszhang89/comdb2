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

#include <ssl_io_evbuffer.h>

#if WITH_SSL

#include <limits.h>

#ifdef my_ssl_println
#undef my_ssl_println
#endif
#ifdef my_ssl_eprintln
#undef my_ssl_eprintln
#endif
#define my_ssl_println(fmt, ...) ssl_println("LIBEVENT-IO", fmt, ##__VA_ARGS__)
#define my_ssl_eprintln(fmt, ...)                                              \
    ssl_eprintln("LIBEVENT-IO", "%s: " fmt, __func__, ##__VA_ARGS__)

int evbuffer_read_ssl(struct evbuffer *buf, sslio *ssl, evutil_socket_t fd, int howmuch, int chunksz)
{
    int nr;
    int ntotal = 0;
    struct iovec v[1];
ssl_downgrade:
    if (!sslio_has_ssl(ssl)) {
        ntotal += evbuffer_read(buf, fd, howmuch);
    } else {
        if (chunksz <= 0)
            chunksz = 4096;

        if (howmuch <= 0) {
            howmuch = INT_MAX;
        }

        do {
            if (howmuch - ntotal < chunksz)
                chunksz = howmuch - ntotal;

            if (evbuffer_reserve_space(buf, chunksz, v, 1) <= 0) {
                ntotal = -1;
                break;
            }
            nr = sslio_read(ssl, v[0].iov_base, chunksz);
            if (nr <= 0) {
                if (nr == 0 && sslio_is_closed_by_peer(ssl)) {
                    howmuch -= ntotal;
                    goto ssl_downgrade;
                } else {
                    char err[256];
                    int sslerr = sslio_get_error(ssl, err, sizeof(err));
                    if (sslerr) { /* protocol error. bail out. */
                        logmsg(LOGMSG_ERROR, "%s\n", err);
                        ntotal = -1;
                    }
                }
                break;
            }

            v[0].iov_len = nr;
            if (evbuffer_commit_space(buf, v, 1) < 0)
                return -1;
            ntotal += nr;
        } while (nr == chunksz && ntotal < howmuch);
    }
    return ntotal;
}

int evbuffer_write_ssl(struct evbuffer *buf, sslio *ssl, evutil_socket_t fd)
{
    size_t len = 0;
    int ntotal = 0;
    int nr = 0;
    int n, i;
    struct evbuffer_iovec *v;

ssl_downgrade:
    if (!sslio_has_ssl(ssl)) {
        ntotal += evbuffer_write(buf, fd);
    } else {
        n = evbuffer_peek(buf, -1, NULL, NULL, 0);
        v = malloc(sizeof(struct evbuffer_iovec) * n);
        if (v != NULL) {
            n = evbuffer_peek(buf, -1, NULL, v, n);
            for (i = 0; i != n && len == nr; ++i) {
                len = v[i].iov_len;
                nr = sslio_write(ssl, v[i].iov_base, len);

                if (nr <= 0) {
                    if (nr == 0 && sslio_is_closed_by_peer(ssl)) {
                        goto ssl_downgrade;
                    } else {
                        char err[256];
                        int sslerr = sslio_get_error(ssl, err, sizeof(err));
                        if (sslerr) { /* protocol error. bail out. */
                            logmsg(LOGMSG_ERROR, "%s\n", err);
                            ntotal = -1;
                        }
                    }
                    break;
                }

                ntotal += nr;

            }

            evbuffer_drain(buf, ntotal);
            free(v);
        }
    }

    return ntotal;
}
#else
/* Some compilers do not like an empty compilation unit. Add a line to please them. */
int this_comdb2_is_not_built_with_ssl = 1;
#endif
