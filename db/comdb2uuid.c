/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include "comdb2uuid.h"

#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#if defined(__linux__)
#include <sys/random.h>
#endif

/* uuid_generate() -> libuuid -> glibc random()/rand(), which take a process-
 * global lock and serialize every caller. Generate the v4 uuid ourselves from
 * a per-thread xoshiro256** so there is no shared lock. */
static __thread int uuid_rng_seeded;
static __thread uint64_t uuid_rng_s[4];

static inline uint64_t uuid_rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

static void uuid_rng_seed(void)
{
    int got = 0;
#if defined(__linux__)
    got = (getrandom(uuid_rng_s, sizeof(uuid_rng_s), 0) == (ssize_t)sizeof(uuid_rng_s));
#endif
    if (!got) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t x = (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
        x ^= (uint64_t)(uintptr_t)pthread_self();
        x ^= (uint64_t)getpid() << 32;
        x ^= (uint64_t)(uintptr_t)&ts;
        for (int i = 0; i < 4; i++) { /* splitmix64 */
            uint64_t z = (x += 0x9E3779B97F4A7C15ULL);
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
            uuid_rng_s[i] = z ^ (z >> 31);
        }
    }
    if ((uuid_rng_s[0] | uuid_rng_s[1] | uuid_rng_s[2] | uuid_rng_s[3]) == 0)
        uuid_rng_s[0] = 0x9E3779B97F4A7C15ULL;
    uuid_rng_seeded = 1;
}

static uint64_t uuid_rng_next(void)
{
    uint64_t *s = uuid_rng_s;
    const uint64_t r = uuid_rotl(s[1] * 5, 7) * 9;
    const uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = uuid_rotl(s[3], 45);
    return r;
}

void comdb2uuid(uuid_t u)
{
    if (!uuid_rng_seeded)
        uuid_rng_seed();
    uint64_t a = uuid_rng_next(), b = uuid_rng_next();
    memcpy(u, &a, 8);
    memcpy((char *)u + 8, &b, 8);
    u[6] = (u[6] & 0x0f) | 0x40; /* version 4 */
    u[8] = (u[8] & 0x3f) | 0x80; /* variant */
}

char *comdb2uuidstr(uuid_t u, char out[37]);
inline char *comdb2uuidstr(uuid_t u, char out[37])
{
    uuid_unparse(u, out);
    return out;
}

void comdb2uuid_clear(uuid_t u) { uuid_clear(u); }

int comdb2uuidcmp(uuid_t u1, uuid_t u2) { return uuid_compare(u1, u2); }

void comdb2uuidcpy(uuid_t dst, uuid_t src) { uuid_copy(dst, src); }

int comdb2uuid_is_zero(uuid_t u)
{
    uuid_t zero;
    comdb2uuid_clear(zero);
    return !comdb2uuidcmp(u, zero);
}
