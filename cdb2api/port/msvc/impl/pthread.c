#include <pthread.h>
int pthread_mutex_lock(pthread_mutex_t *lk)
{
    if (*lk == NULL) {
        HANDLE tmp = CreateMutex(NULL, FALSE, NULL);
        if (InterlockedCompareExchangePointer((PVOID*)lk,
                                              (PVOID)tmp, NULL) != NULL)
            CloseHandle(tmp);
    }
    return (WaitForSingleObject(*lk, INFINITE) == WAIT_FAILED);
}

int pthread_mutex_unlock(pthread_mutex_t *lk)
{
    return (ReleaseMutex(*lk) == 0);
}

int pthread_mutex_init(pthread_mutex_t *restrict lk, const pthread_mutexattr_t *restrict unused)
{
    HANDLE ret = CreateMutex(NULL, FALSE, NULL);
    (void)unused;
    if (ret == NULL)
        return 1;
    *lk = ret;
    return 0;
}

int pthread_mutex_destroy(pthread_mutex_t *lk)
{
    return (CloseHandle(*lk) == 0);
}

int pthread_once(volatile pthread_once_t *st, void (*rtn)(void))
{
    int rc = 0;
    if (!st->initd) {
        if (st->lk == NULL) {
            HANDLE tmp = CreateMutex(NULL, FALSE, NULL);
            if (InterlockedCompareExchangePointer((PVOID*)&st->lk,
                                                  (PVOID)tmp, NULL) != NULL)
                CloseHandle(tmp);
        }

        rc = (WaitForSingleObject(st->lk, INFINITE) == WAIT_FAILED);
        if (!rc) {
            if (!st->initd) {
                rtn();
                st->initd = TRUE;
            }
            ReleaseMutex(st->lk);
        }
    }
    return rc;
}
