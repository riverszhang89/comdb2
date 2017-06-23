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
std::string dbname = "mikedb";
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

int qps = 500;
int nfp = 10;
int nmachines = 20;
int ncontexts = 20;
int bytes;
std::ofstream f;

const char *comma = "";
void query(int second) {
    /* start with a day ago so we can test rolling up stats */
    static int64_t start = time(NULL) - 86400;
    int64_t querytime = (start + second) * 1000000 + 1000000 / qps;
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

/* Generate a test dataset of database performance data */
int main(int argc, char *argv[]) {
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

    for (int second = 0; second < 86400; second++) {
        int nq = rand() % qps;
        if (second % 120 == 0) {
            std::cout << second << "/ 86400 [" << nroll << "]\r";
            std::cout.flush();
        }
        for (int i = 0; i < nq; i++) {
            query(second);
        }
    }
    f << "]" << std::endl;

    return 0;
}
