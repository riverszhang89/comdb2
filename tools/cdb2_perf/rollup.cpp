#include <iostream>
#include <ctime>
#include <string>
#include <exception>
#include <sstream>
#include <cstring>
#include <climits>
#include <cfloat>
#include <map>
#include <list>
#include <vector>
#include <algorithm>
#include <fstream>

#include <time.h>
#include <sys/time.h>

#include <cdb2api.h>
#include <time.h>
#include <stdint.h>

#include "cson_amalgamation_core.h"
#include "cson_util.h"

struct QueryStats {
    std::string fingerprint;
    std::string dbname;
    std::string context;
    std::map<std::string, double> stats;
};

typedef int64_t dbtime_t;

class cson_exception : public std::exception
{
  public:
    cson_exception(int rc) : _rc(rc) {}

    virtual const char *what() const noexcept
    {
        std::stringstream s;
        s << "cson error " << _rc << ": " << cson_rc_string(_rc);
        return strdup(s.str().c_str());
    }

  protected:
    int _rc;
};

class cdb2_exception : public std::exception
{
  public:
    cdb2_exception(int rc, cdb2_hndl_tp *db, const std::string &message = "")
        : _db(db), _message(message), _rc(rc)
    {
    }

    virtual const char *what() const noexcept
    {
        std::stringstream s;
        s << _message << ": [" << _rc << "] " << cdb2_errstr(_db);
        return strdup(s.str().c_str());
    }

  protected:
    cdb2_hndl_tp *_db;
    const std::string _message;
    int _rc;
};


dbtime_t roundtime(dbtime_t dbt, int granularity)
{
    return dbt / granularity * granularity;
}

cdb2_client_datetimeus_t totimestamp(int64_t timestamp)
{
    struct tm t;
    time_t tval = timestamp / 1000000;
    cdb2_client_datetimeus_t out{0};

    gmtime_r(&tval, &t);

    out.tm.tm_year = t.tm_year;
    out.tm.tm_mon = t.tm_mon;
    out.tm.tm_mday = t.tm_mday;
    out.tm.tm_hour = t.tm_hour;
    out.tm.tm_min = t.tm_min;
    out.tm.tm_sec = t.tm_sec;
    out.usec = timestamp % 1000000;
    strcpy(out.tzname, "Etc/UTC");
    return out;
}

#define SEC2USEC(n) (1000000ll * n)

#define HOUR(n) SEC2USEC(3600ll * n)
#define MINUTE(n) SEC2USEC(60ll * n)
#define SECOND(n) SEC2USEC(n)

// int64_t to bind in cdb2 APIs
struct rollup_rules {
    int64_t age;
    int64_t granularity;
};

/* TODO: read from db? */
rollup_rules rules[] = {
    {HOUR(0), MINUTE(1)}
};

// This is mostly for debugging, since all we really need is a block id.
struct block {
    dbtime_t time, rounded_time;
    std::string blockid;
};

std::ostream &operator<<(std::ostream &out, block &b)
{
    out << "["
        << "time: " << b.time << " rounded: " << b.rounded_time
        << " id: " << b.blockid << "]";
    return out;
}

struct sql_event {
    dbtime_t time;
    std::vector<std::string> contexts;
    int64_t cost;
    std::string fingerprint;
    std::string host;

    int64_t rows;
    int64_t runtime;
    int64_t lockwaits;
    int64_t lockwaittime;
    int64_t reads;
    int64_t readtime;
    int64_t writes;
    int64_t writetime;

    int64_t count;

    sql_event() : time(0), cost(0), rows(0), runtime(0), lockwaits(0), lockwaittime(0), reads(0), readtime(0), writes(0), writetime(0), count(1) {}

};

std::string timestr(int64_t ms) {
    time_t t = ms / 1000000;
    struct tm *tm;
    tm = localtime(&t);
    char out[100];
    sprintf(out, "%4d%02d%02dT%02d:%02d:%02d.%d", 1900 + tm->tm_year, tm->tm_mon+1, tm->tm_mday, 
                                                  tm->tm_hour, tm->tm_min, tm->tm_sec, (int) (ms % 1000000));
    return std::string(out);
}

struct sql_event_key {
    std::string fingerprint;
    std::string host;
    std::vector<std::string> contexts;
    dbtime_t time;

    sql_event_key(const std::string &_fingerprint, const std::string &_host, const std::vector<std::string> &_contexts, dbtime_t _time) : 
        fingerprint(_fingerprint), host(_host), contexts(_contexts), time(_time)
    {
    }

};

std::ostream &operator<<(std::ostream &out, const sql_event_key &e)
{
    out << "[ time:" << e.time << " fp:" << e.fingerprint
        << " host:" << e.host << " contexts:";
    out << "[";

    int first = 1;
    for (auto ai : e.contexts) {
        if (!first)
            out << " ";
        out << ai;
        first = 0;
    }
    out << "] ]";
    return out;
}


struct sql_event_key_cmp {
    bool cmp(const sql_event_key &k1, const sql_event_key &k2) {
        if (k1.time < k2.time)
            return true;
        if (k1.fingerprint < k2.fingerprint)
            return true;
        if (k1.host < k2.host)
            return true;
        if (k1.contexts.size() < k2.contexts.size())
            return true;
        for (size_t i = 0; i < k1.contexts.size(); i++) {
            if (i >= k2.contexts.size())
                return true;
            if (k1.contexts[i] < k2.contexts[i])
                return true;
        }
        return false;
    }
    int operator()(const sql_event_key &k1, const sql_event_key &k2) {
        int c = cmp(k1, k2); 
        // std::cout << "(" << k1 << k2 << " cmp=" << c << ")";
        return c;
    }
};

sql_event parse_event(cson_value *v)
{
    cson_object *obj;
    cson_value_fetch_object(v, &obj);
    sql_event ev;
    get_intprop(v, "time", &ev.time);
    get_intprop(v, "cost", &ev.cost);
    get_intprop(v, "rows", &ev.rows);
    const char *s = get_strprop(v, "fingerprint");
    if (s)
        ev.fingerprint = std::string(s);
    s = get_strprop(v, "host");
    if (s)
        ev.host = std::string(s);
    cson_value *o = cson_object_get_sub2(obj, ".perf.runtime");
    if (o && cson_value_is_integer(o))
        ev.runtime = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.lockwaits");
    if (o && cson_value_is_integer(o))
        ev.lockwaits = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.lockwaittime");
    if (o && cson_value_is_integer(o))
        ev.lockwaittime = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.reads");
    if (o && cson_value_is_integer(o))
        ev.reads = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.readtime");
    if (o && cson_value_is_integer(o))
        ev.readtime = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.writes");
    if (o && cson_value_is_integer(o))
        ev.writes = cson_value_get_integer(o);
    o = cson_object_get_sub2(obj, ".perf.writetime");
    if (o && cson_value_is_integer(o))
        ev.writetime = cson_value_get_integer(o);
    o = cson_object_get(obj, "context");
    if (o && cson_value_is_array(o)) {
        cson_array *ar = cson_value_get_array(o);
        int len = cson_array_length_get(ar);
        for (int i = 0; i < len; i++) {
            o = cson_array_get(ar, i);
            if (cson_value_is_string(o))
                ev.contexts.push_back(
                    std::string(cson_string_cstr(cson_value_get_string(o))));
        }
    }

    return ev;
}

bool operator<(const sql_event &ev1, const sql_event &ev2)
{
    return ev1.time - ev2.time;
}

std::ostream &operator<<(std::ostream &out, const sql_event &e)
{
    out << "[time: " << e.time << " fp: " << e.fingerprint 
        << " host: " << e.host << " count: " << e.count;
    return out;
}

sql_event_key evkey(sql_event &e, int64_t granularity)
{
    sql_event_key k = sql_event_key(e.fingerprint, e.host, e.contexts,
                         roundtime(e.time, granularity));
    std::sort(e.contexts.begin(), e.contexts.end());
    return k;
}

int cson_to_string( void * state, void const * src, unsigned int n ) {
    std::stringstream *s = (std::stringstream*) state;
    (*s) << std::string((char*) src, n);
    return 0;
}

void rollup_block_contents(const std::string &olddata, std::string &newdata_out,
                           int64_t age, int64_t granularity,
                           const std::string &blockid, const std::string &database,
                           std::map<std::string, QueryStats> &querystats)
{
    int rc;
    cson_parse_info info;
    cson_parse_opt opt = {.maxDepth = 32, .allowComments = 0};
    cson_value *olddata_value;
    cson_value *newdata;
    int64_t now;

    struct timeval tv;
    gettimeofday(&tv, NULL);
    now = SEC2USEC(tv.tv_sec) + tv.tv_usec;

    rc = cson_parse_string(&olddata_value, olddata.c_str(), olddata.size(),
                           &opt, &info);
    if (rc || !cson_value_is_array(olddata_value))
        throw cson_exception(rc);
    cson_array *old_ar = cson_value_get_array(olddata_value);
    int nent = cson_array_length_get(old_ar);

    newdata = cson_value_new_array();
    cson_array *new_ar = cson_value_get_array(newdata);
    cson_array_reserve(new_ar, nent);

    std::map<sql_event_key, sql_event, sql_event_key_cmp> sqlevents;
    std::multimap<int64_t, cson_value*> events;

    for (int i = 0; i < nent; i++) {
        cson_value *v;
        v = cson_array_get(old_ar, i);
        /* things we don't expect, or things that aren't sql events - pass right
         * through */
        if (!cson_value_is_object(v)) {
            std::cout << "not an object" << std::endl;
            continue;
        }
        cson_object *obj;
        cson_value_fetch_object(v, &obj);
        if (cson_object_get(obj, "type") == nullptr) {
            std::cout << "no type property" << std::endl;
            continue;
        }
        if (cson_object_get(obj, "time") == nullptr) {
            std::cout << "no time property" << std::endl;
            continue;
        }

        std::string type(get_strprop(v, "type"));
        int64_t eventtime;
        if (!get_intprop(v, "time", &eventtime)) {
            std::cout << "no integer time property" << std::endl;
            continue;
        }

        if (eventtime <= 0) {
            std::cout << "invalid time property" << std::endl;
            continue;
        }

        if (type != "sql") {
            events.insert(std::pair<int64_t, cson_value*>(eventtime, v));
            continue;
        }
        sql_event ev = parse_event(v);

        // std::cout << "time " << timestr(ev.time) << " diff " << (now - ev.time) / 1000000;
        /* New events are preserved as is */
        if (ev.time - ev.runtime >= now - age) {
            // std::cout << " skip " << std::endl;
            events.insert(std::pair<int64_t, cson_value*>(eventtime, v));
            continue;
        }

        sql_event_key k = evkey(ev, granularity);

        // std::cout << " fold to " << roundtime(ev.time, granularity) << " key " << k;

        /* Fold older events */
        auto fnd = sqlevents.find(k);
        if (fnd == sqlevents.end()) {
            // std::cout << " new ";
            ev.time = roundtime(ev.time, granularity);
            sqlevents.insert(std::pair<sql_event_key, sql_event>(k, ev));
        } else {
            fnd->second.count++;
            fnd->second.cost += ev.cost;
            fnd->second.rows += ev.rows;
            fnd->second.runtime += ev.runtime;
            fnd->second.lockwaits += ev.lockwaits;
            fnd->second.lockwaittime += ev.lockwaittime;
            fnd->second.reads += ev.reads;
            fnd->second.readtime += ev.readtime;
            fnd->second.writes += ev.writetime;
        }
        // std::cout << std::endl;
    }

#if 0
    std::vector<sql_event> ev;
    ev.reserve(events.size());
    for (auto it : events) {
        ev.push_back(it.second);
    }
    std::sort(ev.begin(), ev.end());
#endif

    /* Add the summarized events & Update the statistics map */
    for (auto it : sqlevents) {

        cson_value *v = cson_value_new_object();
        cson_object *o  = cson_value_get_object(v);
        const sql_event &ev = it.second;

        cson_object_set(o, "time", cson_new_int(ev.time));
        cson_object_set(o, "fingerprint", cson_value_new_string(ev.fingerprint.c_str(), ev.fingerprint.size()));
        cson_object_set(o, "host", cson_value_new_string(ev.host.c_str(), ev.host.size()));

        cson_object_set(o, "cost", cson_value_new_integer(ev.cost / ev.count));
        cson_object_set(o, "rows", cson_value_new_integer(ev.rows / ev.count));
        cson_object_set(o, "runtime", cson_value_new_integer(ev.runtime / ev.count));
        cson_object_set(o, "lockwaits", cson_value_new_integer(ev.lockwaits / ev.count));
        cson_object_set(o, "lockwaittime", cson_value_new_integer(ev.lockwaittime / ev.count));
        cson_object_set(o, "reads", cson_value_new_integer(ev.reads / ev.count));
        cson_object_set(o, "readtime", cson_value_new_integer(ev.readtime / ev.count));
        cson_object_set(o, "writes", cson_value_new_integer(ev.writes / ev.count));
        cson_object_set(o, "writetime", cson_value_new_integer(ev.writetime / ev.count));
        cson_object_set(o, "summarized", cson_value_new_bool(1));
        cson_object_set(o, "count", cson_value_new_integer(ev.count));

        cson_array *ar;
        ar = cson_new_array();
        cson_object_set(o, "contexts", cson_array_value(ar));
        events.insert(std::pair<int64_t, cson_value*>(ev.time, v));

        std::string querykeypfx = "f=" + ev.fingerprint + "d=" + database;
        for (auto c : ev.contexts) {
            /* Key is f=<fingerprint>,d=<database>,c=<context> */
            std::string querykey = querykeypfx + "c=" + c;
            cson_array_append(ar, cson_value_new_string(c.c_str(), c.size()));

            auto qsit = querystats.find(querykey);
            if (qsit == querystats.end()) {

                std::map<std::string, double> kv;

                kv.insert(std::pair<std::string, double>("totcnt", 0));

                kv.insert(std::pair<std::string, double>("totcost", 0));
                kv.insert(std::pair<std::string, double>("mincost", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxcost", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totrows", 0));
                kv.insert(std::pair<std::string, double>("minrows", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxrows", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totrtm", 0));
                kv.insert(std::pair<std::string, double>("minrtm", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxrtm", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totlkws", 0));
                kv.insert(std::pair<std::string, double>("minlkws", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxlkws", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totlkwtm", 0));
                kv.insert(std::pair<std::string, double>("minlkwtm", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxlkwtm", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totrds", 0));
                kv.insert(std::pair<std::string, double>("minrds", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxrds", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totrdtm", 0));
                kv.insert(std::pair<std::string, double>("minrdtm", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxrdtm", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totwrs", 0));
                kv.insert(std::pair<std::string, double>("minwrs", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxwrs", INT64_MIN));

                kv.insert(std::pair<std::string, double>("totwrtm", 0));
                kv.insert(std::pair<std::string, double>("minwrtm", INT64_MAX));
                kv.insert(std::pair<std::string, double>("maxwrtm", INT64_MIN));

                QueryStats val;
                val.fingerprint = ev.fingerprint;
                val.dbname = database;
                val.context = c;
                val.stats = kv;

                querystats.insert(std::pair<std::string, QueryStats>(querykey, val));
                qsit = querystats.find(querykey);
            }

            for (auto qskvit = qsit->second.stats.begin(), qskvend = qsit->second.stats.end();
                    qskvit != qskvend; ++qskvit) {

#define UPDATE_STATS(attr, key)                             \
                else if (qskvit->first == "tot" key) {      \
                    qskvit->second += ev.attr;              \
                } else if (qskvit->first == "min" key) {    \
                    if (ev.attr < qskvit->second)           \
                        qskvit->second = ev.attr;           \
                } else if (qskvit->first == "max" key) {    \
                    if (ev.attr > qskvit->second)           \
                        qskvit->second = ev.attr;           \
                }

                if (qskvit->first == "totcnt")
                    qskvit->second += ev.count;

                UPDATE_STATS(cost, "cost")
                UPDATE_STATS(rows, "rows")
                UPDATE_STATS(runtime, "rtm")
                UPDATE_STATS(lockwaits, "lkws")
                UPDATE_STATS(lockwaittime, "lkwtm")
                UPDATE_STATS(reads, "rds")
                UPDATE_STATS(readtime, "rdtm")
                UPDATE_STATS(writes, "wrs")
                UPDATE_STATS(writetime, "wrtm")
            }

#if 0
            for (auto elem : qsit->second.stats) {
                if (elem.first == "maxrtm")
                std::cout << elem.first << " -> " << elem.second << std::endl;
            }
#endif
        }
    }

    for (auto it : events) {
        cson_array_append(new_ar, it.second);
    }

    cson_output_opt outopt = { 0, 255, 0, 0, 0, 0 };
    std::cout << nent << " -> " << events.size() << std::endl;
    std::stringstream s;
    cson_output(newdata, cson_to_string, &s, &outopt);
    cson_free_value(newdata);
    newdata_out = s.str();
    cson_free_value(olddata_value);
}

void rollup_block(cdb2_hndl_tp *db, const block &b, int rulenum)
{
    cdb2_clearbindings(db);
    cdb2_bind_param(db, "id", CDB2_CSTRING, b.blockid.c_str(),
                    b.blockid.size());

    int rc = cdb2_run_statement(db, "select block, dbname from blocks where id = @id");
    if (rc)
        throw cdb2_exception(rc, db, "retrieve block");

    rc = cdb2_next_record(db);
    if (rc != CDB2_OK)
        throw cdb2_exception(rc, db, "retrieve block record");
    std::string blockdata = std::string((char *)cdb2_column_value(db, 0));
    std::string database = std::string((char *)cdb2_column_value(db, 1));
    std::string newdata;
    std::cout << b.blockid;

    /* Query stats map */
    std::map<std::string, QueryStats> querystats;
    rollup_block_contents(blockdata, newdata, rules[rulenum].age, rules[rulenum].granularity,
                          b.blockid, database, querystats);

    rc = cdb2_next_record(db);
    if (rc != CDB2_OK_DONE)
        throw cdb2_exception(rc, db, "unexpected block record");

    /* Update block record */
    cdb2_clearbindings(db);
    cdb2_bind_param(db, "block", CDB2_CSTRING, newdata.c_str(), newdata.size());
    cdb2_bind_param(db, "granularity", CDB2_INTEGER, &rules[rulenum].granularity, sizeof(int64_t));
    cdb2_bind_param(db, "id", CDB2_CSTRING, b.blockid.c_str(), b.blockid.size());
    rc = cdb2_run_statement(db, "update blocks set block = @block, granularity = @granularity where id = @id");
    if (rc) 
        throw cdb2_exception(rc, db, "updating section");

    /* Update query record */

    for (auto outit : querystats) {
        char query[2048], updcols[1024];
        size_t updcolnwr = 0;
        cdb2_clearbindings(db);

        QueryStats elem = outit.second;
        std::string fingerprint = elem.fingerprint;
        std::string dbname = elem.dbname;
        std::string context = elem.context;

        auto totcntit = elem.stats.find("totcnt");
        if (totcntit == elem.stats.end()) {
            std::cerr << "error: Missing attribute `totcnt' from block record " << b.blockid << std::endl;
            continue;
        }

        double totcnt = totcntit->second;

        for (auto init : elem.stats) {
#define GEN_PARAM_TOT_TO_AVG(key)                                                       \
            else if (init.first == "tot" #key) {                                        \
                updcolnwr += snprintf(updcols + updcolnwr, sizeof(updcols) - updcolnwr, \
                                      "avg" #key "=%f,", init.second/totcnt);           \
            }

            if (init.first == "totcnt")
                continue;
            GEN_PARAM_TOT_TO_AVG(cost)
            GEN_PARAM_TOT_TO_AVG(rows)
            GEN_PARAM_TOT_TO_AVG(rtm)
            GEN_PARAM_TOT_TO_AVG(lkws)
            GEN_PARAM_TOT_TO_AVG(lkwtm)
            GEN_PARAM_TOT_TO_AVG(rds)
            GEN_PARAM_TOT_TO_AVG(rdtm)
            GEN_PARAM_TOT_TO_AVG(wrs)
            GEN_PARAM_TOT_TO_AVG(wrtm)
            else {
                updcolnwr += snprintf(updcols + updcolnwr, sizeof(updcols) - updcolnwr,
                                      "%s=%f,", init.first.c_str(), init.second);
            }
        }

        if (updcolnwr == 0) {
            std::cerr << "error: Malformed data from block " << b.blockid << std::endl;
            continue;
        }

        snprintf(query, sizeof(query),
                "UPDATE queries SET %s dbname = dbname "
                "WHERE "
                "blockid = '%s' "
                "AND "
                "fingerprint = '%s' "
                "AND "
                "dbname = '%s' "
                "AND "
                "context = '%s'",
                updcols,
                b.blockid.c_str(),
                fingerprint.c_str(),
                dbname.c_str(),
                context.c_str());

        rc = cdb2_run_statement(db, query);
        if (rc)
            std::cerr << "error: Failed to update query stats"
                      << " rc = " << rc
                      << " reason = " << std::string(cdb2_errstr(db))
                      << " blockid = " << b.blockid
                      << " fingerprint = " << fingerprint
                      << " dbname = " << dbname
                      << " context = " << context
                      << std::endl;
    }
}

void rollup(int rulenum, const std::string &blockid = "")
{
    cdb2_hndl_tp *db;
    int rc;

    rc = cdb2_open(&db, "comdb2perfdb", "local" /*TODO*/, 0);
    if (rc) throw cdb2_exception(rc, db, "connect");

    if (!blockid.empty()) {
        block b{0, 0, blockid};
        rollup_block(db, b, 0);
        cdb2_close(db);
        return;
    }

    // we can get the block contents in one shot, but I want to see progress
    cdb2_bind_param(db, "ago", CDB2_INTEGER, &rules[rulenum].age,
                    sizeof(int64_t));
    cdb2_bind_param(db, "granularity", CDB2_INTEGER,
                    &rules[rulenum].granularity, sizeof(int64_t));
    rc = cdb2_run_statement(db,
                            "select id, cast(start as int) from blocks where "
                            "start < now() - cast(@ago as seconds) or end < "
                            "now() - cast(@ago as seconds) and granularity < "
                            "@granularity order by start");
    if (rc) throw cdb2_exception(rc, db, "find blocks");

    // We are going to assign each block we found older than the policy age to a
    // group of similarly aged blocks
    std::list<block> blocks;

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        std::string blockid = std::string((char *)cdb2_column_value(db, 0));
        dbtime_t t = (dbtime_t) * (int64_t *)cdb2_column_value(db, 1);
        dbtime_t dt = roundtime(t, rules[rulenum].granularity);
        blocks.push_back(block{t, dt, blockid});
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        throw cdb2_exception(rc, db, "retrieve block ids");
    }

    for (auto i : blocks)
        rollup_block(db, i, rulenum);

    cdb2_close(db);
}

int main(int argc, char *argv[])
{
    if (argc != 1) {
        for (int i = 1; i < argc; i++)
          rollup(0, std::string(argv[i]));
    } else {
      try {
        rollup(0);
      } catch (cdb2_exception std) {
        std::cerr << "error: " << std.what() << std::endl;
      }
    }
#if 0
    std::fstream f("in");
    std::string in((std::istreambuf_iterator<char>(f)), (std::istreambuf_iterator<char>()));
    std::string out;
    rollup_block_contents(in, out, HOUR(2), MINUTE(15));
    std::cout << out << std::endl;
#endif

    return 0;
}
