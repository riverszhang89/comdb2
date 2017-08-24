/*
   Copyright 2017, Bloomberg Finance L.P.

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

#include <string>
#include <random>
#include <iostream>
#include <fstream>
#include <set>
#include <sstream>

#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <zlib.h>

#include <getopt.h>

std::string randstr(int len) {
    char s[len+1];
    for (int i = 0; i < len; i++)
        s[i] = 'a' + rand() % 26;
    s[len] = 0;
    return std::string(s);
}

std::string randhexstr(int len) {
    char s[len+1];
    static const char *hex = "0123456789abcdef";
    for (int i = 0; i < len; i++)
        s[i] = hex[rand() % 16];
    s[len] = 0;
    return std::string(s); }

std::string randuuid(void) {
    std::stringstream in;
    in << randhexstr(8) << '-' << randhexstr(4) << '-' << randhexstr(4) << '-' << randhexstr(4) << '-' << randhexstr(8);
    return in.str();
}

std::vector<std::string> machines;
std::vector<std::string> contexts;
std::vector<std::string> fingerprints;
std::set<std::string> seen_fingerprint;
int rollsize = 16 * 1024 * 1024;
int nroll = 0;

/* 

 Parameters: 

 nqueries/sec
 nfingerprints
 ncontexts
   nmachines
   ntasks

 */

int qps = 20;            /* queries per second */
int nfp = 10;           /* number of different queries (fingerprints) */
int nmachines = 10;
int ncontexts = 10;
int bytes;
std::string dbname;
std::ofstream f;
int from;
int to;

const char *comma = "";
void query(int second) {
    /* start with a day ago so we can test rolling up stats */
    int64_t querytime = second;
    std::stringstream s;

    if (!f.is_open()) {
        std::stringstream fname;
        fname << dbname << ".events." << querytime;
        f.open(fname.str());
        comma = "";
        f << "[" << std::endl;
    }

    std::string &fp = fingerprints[rand() % fingerprints.size()];
    if (seen_fingerprint.find(fp) == seen_fingerprint.end()) {
        s << comma << "{\"time\": " << querytime;
        s << ", \"type\": \"newsql\", \"sql\": \"query " << fp << "\", \"fingerprint\": \"" << fp << "\"}" << std::endl;
        seen_fingerprint.insert(fp);
        comma = ", ";
    }

    s << comma << "{\"time\": " << querytime << ", \"type\": \"sql\", \"host\": \"me\", \"rows\": " << rand() % 1000 << ", \"fingerprint\": \"" << fp << "\", \"context\":";
    s << "[";
    s << "\"" << machines[rand() % machines.size()] << "\", \"" << contexts[rand() % contexts.size()] << "\"";
    s << "], ";
    s << "\"perf\": {\"runtime\": " << rand() % 10000 << "}, \"cost\": " << rand() % 100 << "}" << std::endl;
    comma = ", ";

    bytes += s.str().size();
    f << s.str();
    if (bytes > rollsize) {
        seen_fingerprint.clear();
        bytes = 0;
        f << "]" << std::endl;
        f.close();
        nroll++;
    }
}

int read_date(char *s) {
    struct tm tm = {0};
    int rc = scanf("%04d%02d%02dT%02d%02d%02d", &tm.tm_year,
            &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, 
            &tm.tm_sec);
    if (rc < 3) {
        return atoi(s);
    }
    tm.tm_year += 1900;
    tm.tm_mon++;
    return mktime(&tm);
}

void usage(void) {
    std::cerr << "Usage: gen\n"
        "\n"
        "--dbname | -d                  database name\n"
        "--queries-per-second | -q      average queries per second\n"
        "--num-queries | -n             number of different queries\n"
        "--num-machines | -m            number of source machines\n"
        "--num-contexts | -c            number of contexts\n"
        "--from | -f                    start time\n"
        "--to | -t                      end time\n" << std::endl;

    exit(1);
}

std::string timestr(time_t t) {
    struct tm *tm = localtime(&t);
    char s[100];
    sprintf(s, "%04d%02d%02dT%02d%02d%02d", 1900 + tm->tm_year, 
                                            tm->tm_mon + 1,
                                            tm->tm_mday, 
                                            tm->tm_hour, 
                                            tm->tm_min, 
                                            tm->tm_sec);
    return std::string(s);
}

/* Generate a test dataset of database performance data */
int main(int argc, char *argv[]) {
    int opt;

    time_t now = time(NULL);
    struct tm *tm;
    tm = localtime(&now);
    tm->tm_hour = 23;
    tm->tm_min = 59;
    tm->tm_sec = 59;
    to = mktime(tm);
    tm->tm_hour = 0;
    tm->tm_min = 0;
    tm->tm_sec = 0;
    from = mktime(tm);

    struct option options[] = {
        { "dbname", required_argument, NULL, 'd' },
        { "queries-per-second", required_argument, NULL, 'q' },
        { "num-queries", required_argument, NULL, 'n' },
        { "num-machines", required_argument, NULL, 'm' },
        { "num-contexts", required_argument, NULL, 'c' },
        { "from", required_argument, NULL, 'f' },
        { "to", required_argument, NULL, 't' },
        { NULL, 0, NULL, 0 }
    };

    while ((opt = getopt_long(argc, argv, "d:q:n:m:c:f:t:", options, NULL)) != -1) {
        switch (opt) {
            case 'd':
               dbname = std::string(optarg); 
               break;

            case 'q':
               qps = atoi(optarg);
               break;

            case 'n':
               nfp = atoi(optarg);
               break;

            case 'm':
               nmachines = atoi(optarg);
               break;

            case 'c':
               ncontexts = atoi(optarg);
               break;

            case 'f':
               from = read_date(optarg);
               break;

            case 't':
               to = read_date(optarg);
               break;

            case '?':
            default:
                usage();
                break;
        }
    }

    if (dbname.empty()) {
        std::cerr << "No dbname specified" << std::endl;
        exit(1);
    }
    std::cout << "dbname " << dbname << "  ";
    std::cout << "qps " << qps << "  ";
    std::cout << "nfp " << nfp << "  ";
    std::cout << "nmachines " << nmachines << "  ";
    std::cout << "ncontexts " << ncontexts << "  ";
    std::cout << "from " << timestr(from) << "(" << from << ")" << "  ";
    std::cout << "to " << timestr(to) << "  " << std::endl;;


    for (int i = 0; i < nfp; i++)
        fingerprints.push_back(randhexstr(32));
    for (int i = 0; i < nmachines ; i++) {
        std::stringstream m;
        m << "m" << i;
        machines.push_back(m.str());
    }
    for (int i = 0; i < ncontexts; i++) {
        std::stringstream c;
        c << "c" << i;
        contexts.push_back(c.str());
    }

    for (int second = from, s = 0; second < to; second++, s++) {
        int nq = rand() % qps + 1;
        if (second % 120 == 0) {
            std::cout << s << "/ 86400 [" << nroll << "]\r";
            std::cout.flush();
        }
        for (int i = 0; i < nq; i++) {
            query(second);
        }
    }
    f << "]" << std::endl;

    return 0;
}
