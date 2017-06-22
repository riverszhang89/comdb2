SRCHOME=../..

CDB2PERF_OBJS=cdb2perf.o
CDB2ROLLUP_OBJS=rollup.o

OBJS=$(CDB2PERF_OBJS) $(CDB2ROLLUP_OBJS)
BINARIES=cdb2perf cdb2rollup

DUMBWARN=-Wno-unused-function

LIBS=-L$(SRCHOME)/cson -lcson -lz -L$(SRCHOME)/cdb2api -luuid -l:libcdb2api.a -L$(SRCHOME)/protobuf -lcdb2protobuf -lprotobuf-c -lssl -lcrypto -lpthread
CXXFLAGS=-Wall -g -I$(SRCHOME)/cson -I$(SRCHOME)/cdb2api -std=c++11 $(DUMBWARN)

all: $(BINARIES)

cdb2perf: $(CDB2PERF_OBJS)
	g++ -o $@ $< $(LDFLAGS) $(LIBS)

cdb2rollup: $(CDB2ROLLUP_OBJS)
	g++ -o $@ $< $(LDFLAGS) $(LIBS)


%.o: %.cpp
	g++ -c $(CXXFLAGS) $< -o $@

clean:  
	rm -f $(BINARIES) $(OBJS)
