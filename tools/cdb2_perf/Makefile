SRCHOME=../..

CDB2PERF_OBJS=cdb2perf.o
CDB2ROLLUP_OBJS=rollup.o

COMMON_OBJS=cson_util.o

OBJS=$(CDB2PERF_OBJS) $(CDB2ROLLUP_OBJS)
BINARIES=cdb2perf cdb2rollup

DUMBWARN=-Wno-unused-function

LIBS=-L$(SRCHOME)/cson -lcson -lz -L$(SRCHOME)/cdb2api -luuid -l:libcdb2api.a -L$(SRCHOME)/protobuf -lcdb2protobuf -lprotobuf-c -lssl -lcrypto -lpthread
CXXFLAGS=-Wall -g -I$(SRCHOME)/cson -I$(SRCHOME)/cdb2api -std=c++11 $(DUMBWARN)
CFLAGS=-Wall -g -I$(SRCHOME)/cson -I$(SRCHOME)/cdb2api -std=c99 $(DUMBWARN)

all: $(BINARIES)

cdb2perf: $(CDB2PERF_OBJS) $(COMMON_OBJS)
	g++ -o $@ $(CDB2PERF_OBJS) $(COMMON_OBJS) $(LDFLAGS) $(LIBS)

cdb2rollup: $(CDB2ROLLUP_OBJS) $(COMMON_OBJS)
	g++ -o $@ $(CDB2ROLLUP_OBJS) $(COMMON_OBJS) $(LDFLAGS) $(LIBS)

%.o: %.cpp
	g++ -c $(CXXFLAGS) $< -o $@

%.o: %.c
	gcc -c $(CFLAGS) $< -o $@


clean:  
	rm -f $(BINARIES) $(OBJS)
