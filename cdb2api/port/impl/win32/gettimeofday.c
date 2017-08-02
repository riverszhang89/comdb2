#include <sys/time.h>
#include <stdint.h>
#include <windows.h>

/* gettimeofday() */
int gettimeofday(struct timeval *tv, void *unused)
{
    FILETIME ft;
    const uint64_t shift = 116444736000000000ULL;
    (void)unused;
    GetSystemTimeAsFileTime(&ft);
    union {
        FILETIME f;
        uint64_t i;
    } caster;
    caster.f = ft;
    caster.i -= shift;
    tv->tv_sec = (long)(caster.i / 10000000);
    tv->tv_usec = (long)((caster.i / 10) % 1000000);
    return 0;
}
