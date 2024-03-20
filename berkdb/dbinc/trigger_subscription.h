#ifndef TRIGGER_SUBSCRIPTION_H
#define TRIGGER_SUBSCRIPTION_H

#include <pthread.h>
#include <inttypes.h>

struct __db_trigger_subscription {
	char *name;
	int fresh; /* 1 if just created. */
	int active;
	uint8_t status;
	pthread_cond_t cond;
	pthread_mutex_t lock;
};

struct __db_trigger_subscription *__db_get_trigger_subscription(const char *);
int __db_for_each_trigger_subscription(hashforfunc_t *, int);

#endif //TRIGGER_SUBSCRIPTION_H
