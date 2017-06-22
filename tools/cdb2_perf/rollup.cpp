#include <iostream>
#include <ctime>
#include <string>

#include <time.h>
#include <stdint.h>

typedef int64_t dbtime_t;

const std::string timestr(dbtime_t t) {
    time_t tt = t;
    struct tm tm;
    localtime_r(&tt, &tm);
    char s[100];
    snprintf(s, sizeof(s), "%d-%02d-%02dT%02d%02d%02d",
            1900 + tm.tm_year, tm.tm_mon+1, tm.tm_mday, 
            tm.tm_hour, tm.tm_min, tm.tm_sec);
    return std::string(s);
}

dbtime_t roundtime(dbtime_t dbt, int granularity) {
    return dbt / granularity * granularity;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: time granularity" << std::endl;
        return 1;
    }
    dbtime_t t = atoll(argv[1]);
    std::cout << "in  " << timestr(t) << " " << t << std::endl;
    int gr = atoi(argv[2]);
    t = roundtime(t, gr);
    std::cout << "out " << timestr(t) << " " << t << std::endl;

    return 0;
}
