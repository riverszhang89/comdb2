#include <iostream>
#include <ctime>
#include <string>
#include <exception>
#include <sstream>
#include <cstring>
#include <map>
#include <list>
#include <vector>
#include <tuple>

#include <cdb2api.h>
#include <time.h>
#include <stdint.h>

#include "cson_amalgamation_core.h"

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

const std::string timestr(dbtime_t t)
{
    time_t tt = t;
    struct tm tm;
    localtime_r(&tt, &tm);
    char s[100];
    snprintf(s, sizeof(s), "%d-%02d-%02dT%02d%02d%02d", 1900 + tm.tm_year,
             tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    return std::string(s);
}

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

#define HOUR(n) (3600 * n)
#define MINUTE(n) (60 * n)

// int64_t to bind in cdb2 APIs
struct rollup_rules {
    int64_t age;
    int64_t granularity;
};

/* TODO: read from db? */
rollup_rules rules[] = {
    {HOUR(8), MINUTE(1)} /* 1 hour intervals after 8 hours */
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
};


typedef std::tuple<std::string /* fingerprint */, std::string /* host */, std::vector<std::string> /* context */> sql_event_key;

sql_event parse_event(cson_value *v) {
    sql_event ev;
    return sql_event();
}

void rollup_block_contents(const std::string &olddata, std::string &newdata_out)
{
    int rc;
    cson_parse_info info;
    cson_parse_opt opt = {.maxDepth = 32, .allowComments = 0};
    cson_value *olddata_value;
    cson_value *newdata;

    rc = cson_parse_string(&olddata_value, olddata.c_str(), olddata.size(), &opt, &info);
    if (rc || !cson_value_is_array(olddata_value))
        throw cson_exception(rc);
    cson_array *old_ar = cson_value_get_array(olddata_value);;
    int nent = cson_array_length_get(old_ar);

    newdata = cson_value_new_array();
    cson_array *new_ar = cson_value_get_array(newdata);
    cson_array_reserve(new_ar, nent);

    std::map<sql_event_key, sql_event> events;

    for (int i = 0; i < nent; i++) {
        cson_value *v;
        v = cson_array_get(old_ar, i);
        /* things we don't expect, or things that aren't sql events - pass right through */
        if (!cson_value_is_object(v)) {
            cson_array_append(new_ar, v);
            continue;
        }

        cson_object *obj;
        cson_value_fetch_object(v, &obj);
        if (cson_object_get(obj, "sql") == nullptr) {
            cson_array_append(new_ar, v);
            continue;
        }

        sql_event ev = parse_event(v);
    }
}

void rollup_block(cdb2_hndl_tp *db, const block &b)
{
    cdb2_clearbindings(db);
    cdb2_bind_param(db, "id", CDB2_CSTRING, b.blockid.c_str(),
                    b.blockid.size());

    int rc = cdb2_run_statement(db, "select block from blocks where id = @id");
    if (rc)
        throw cdb2_exception(rc, db, "retrieve block");

    rc = cdb2_next_record(db);
    if (rc != CDB2_OK)
        throw cdb2_exception(rc, db, "retrieve block record");
    std::string blockdata = std::string((char *)cdb2_column_value(db, 0));
    std::string newdata;
    rollup_block_contents(blockdata, newdata);
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK_DONE)
        throw cdb2_exception(rc, db, "unexpected block record");
}

void rollup(int rulenum)
{
    cdb2_hndl_tp *db;
    int rc;

    rc = cdb2_open(&db, "comdb2perfdb", "local" /*TODO*/, 0);
    if (rc)
        throw cdb2_exception(rc, db, "connect");
    // we can get the block contents in one shot, but I want to see progress
    cdb2_bind_param(db, "ago", CDB2_INTEGER, &rules[rulenum].age,
                    sizeof(int64_t));
    cdb2_bind_param(db, "granularity", CDB2_INTEGER,
                    &rules[rulenum].granularity, sizeof(int64_t));
    rc = cdb2_run_statement(db,
                            "select id, cast(start as int) from blocks where "
                            "start < now() - cast(@ago as seconds) or end < "
                            "now() - cast(@ago as seconds) and granularity = "
                            "@granularity order by start");
    if (rc)
        throw cdb2_exception(rc, db, "find blocks");

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

    int blocknum = 1;
    for (auto i : blocks) {
        printf("%03d/%03d %s\r", blocknum++, static_cast<int>(blocks.size()),
               i.blockid.c_str());
        fflush(stdout);
        rollup_block(db, i);
    }
}

int main(int argc, char *argv[])
{
    try {
        rollup(0);
    } catch (cdb2_exception std) {
        std::cerr << "error: " << std.what() << std::endl;
    }
    return 0;
}
