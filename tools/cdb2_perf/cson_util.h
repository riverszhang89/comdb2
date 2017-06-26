#ifndef INCLUDED_CSON_UTIL_H
#define INCLUDED_CSON_UTIL_H

#include "cson_amalgamation_core.h"
#include <stdint.h>

const char *get_strprop(const cson_value *objval, const char *key);
bool get_intprop(const cson_value *objval, const char *key, int64_t *val);


#endif
