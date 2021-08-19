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

int evbuffer_read_ssl(struct evbuffer *buf, sslio *ssl, evutil_socket_t fd, int howmuch, int drain_pending)
{
    static int NVEC = 2;
    int chunksz = 4096;
    int i, nv;

    int ntotal = 0;
    int nread, nremain;
    struct iovec v[NVEC];
//ssl_downgrade:
    if (!sslio_has_ssl(ssl)) {
        ntotal += evbuffer_read(buf, fd, howmuch);
    } else {
        if (howmuch <= 0)
            howmuch = INT_MAX;

        do {
            /* Adjust iov size if too large. Not critical but may save us a bit of memory down the line. */
            if (howmuch - ntotal < chunksz)
                chunksz = howmuch - ntotal;

            /* If we fail to reserve space in evbuffer for some reason, bail out. */
            if ((nv = evbuffer_reserve_space(buf, chunksz, v, NVEC)) <= 0) {
                ntotal = -1;
                break;
            }

            for (i = 0, nremain = chunksz; i != nv && nremain > 0; ++i) {
                if (v[i].iov_len > nremain)
                    v[i].iov_len = nremain;
                nread = sslio_read_no_retry(ssl, v[i].iov_base, v[i].iov_len);
                if (nread > 0) {
                    ntotal += nread;
                    nremain -= nread;
                } else {
                    if (nread == 0 && sslio_is_closed_by_peer(ssl)) {
                        howmuch -= ntotal;
                        //goto ssl_downgrade;
                        break;
                    } else {
                        char err[256];
                        int sslerr = sslio_get_error(ssl, err, sizeof(err));
                        if (sslerr)
                            logmsg(LOGMSG_ERROR, "%s: %s\n", __func__, err);
                    }
                    return -1;
                }
                v[i].iov_len = nread;
            }

            /* If we fail to commit the space in evbuffer, bail out. */
            if (evbuffer_commit_space(buf, v, nv) < 0)
                return -1;
        } while (nread == chunksz && ntotal < howmuch);

        /* Read the remainder of the last SSL packet. */
        if ((drain_pending || howmuch == INT_MAX) && (nremain = sslio_pending(ssl)) > 0) {
            if ((nv = evbuffer_reserve_space(buf, nremain, v, NVEC)) <= 0)
                return -1;
            for (i = 0; i != nv && nremain > 0; ++i) {
                if (v[i].iov_len > nremain)
                    v[i].iov_len = nremain;
                nread = sslio_read_no_retry(ssl, v[i].iov_base, v[i].iov_len);
                if (nread != v[i].iov_len) {
                    logmsg(LOGMSG_ERROR, "%s: Unexpected SSL_pending error: expected %ld bytes, read %d bytes\n", __func__, v[i].iov_len, nread);
                    return -1;
                }
                ntotal += nread;
                nremain -= nread;
            }
            if (evbuffer_commit_space(buf, v, nv) < 0)
                return -1;
        }
    }
    return ntotal;
}

int evbuffer_write_ssl(struct evbuffer *buf, sslio *ssl, evutil_socket_t fd)
{
    size_t len = 0;
    int ntotal = 0;
    int nread = 0;
    int n, i;
    struct evbuffer_iovec *v;

//ssl_downgrade:
    if (!sslio_has_ssl(ssl)) {
        ntotal += evbuffer_write(buf, fd);
    } else {
        n = evbuffer_peek(buf, -1, NULL, NULL, 0);
        v = malloc(sizeof(struct evbuffer_iovec) * n);
        if (v != NULL) {
            n = evbuffer_peek(buf, -1, NULL, v, n);
            for (i = 0; i != n && len == nread; ++i) {
                len = v[i].iov_len;
                nread = sslio_write_no_retry(ssl, v[i].iov_base, len);
                if (nread <= 0) {
                    if (nread == 0 && sslio_is_closed_by_peer(ssl)) {
                        //goto ssl_downgrade;
                        break;
                    } else {
                        char err[256];
                        int sslerr = sslio_get_error(ssl, err, sizeof(err));
                        if (sslerr) { /* protocol error. bail out. */
                            logmsg(LOGMSG_ERROR, "%s: %s\n", __func__, err);
                            ntotal = -1;
                        }
                    }
                    break;
                }

                ntotal += nread;

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
