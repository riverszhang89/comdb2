#include "cson_amalgamation_core.h"
#include "cson_util.h"
#include <cstdio>

const char *get_strprop(const cson_value *objval, const char *key) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_string(propval))
        return nullptr;
    cson_string *cstr;
    cson_value_fetch_string(propval, &cstr);
    return cson_string_cstr(cstr);
}

bool get_intprop(const cson_value *objval, const char *key, int64_t *val) {
    cson_object *obj;
    cson_value_fetch_object(objval, &obj);
    cson_value *propval = cson_object_get(obj, key);
    if (propval == nullptr || !cson_value_is_integer(propval))
        return false;
    cson_value_fetch_integer(propval, (cson_int_t*) val);
    return true;
}
