#ifndef _INCLUDED_PORT_MSVC_PTHREAD_H_
#define _INCLUDED_PORT_MSVC_PTHREAD_H_
#include <windows.h>
typedef DWORD pthread_t;
pthread_t pthread_self(void);

typedef DWORD pthread_mutexattr_t;
typedef HANDLE pthread_mutex_t;
#define PTHREAD_MUTEX_INITIALIZER NULL
int pthread_mutex_init(pthread_mutex_t *restrict mutex, const pthread_mutexattr_t *restrict attr);
int pthread_mutex_destroy(pthread_mutex_t *mutex);
int pthread_mutex_lock(pthread_mutex_t *lk);
int pthread_mutex_unlock(pthread_mutex_t *lk);

/* We must expose the struct definition due to
   msvc compiler error C2079. */
typedef struct pthread_once_t {
    HANDLE lk;
    BOOL initd;
} pthread_once_t;
#define PTHREAD_ONCE_INIT {0}

int pthread_once(volatile pthread_once_t *st, void (*rtn)(void));
#endif

/* gettimeofday() */
static int gettimeofday(struct timeval *tv, void *unused)
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
