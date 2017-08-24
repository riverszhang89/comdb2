SRCHOME=../..

INGEST_OBJS=ingest.o
ROLLUP_OBJS=rollup.o

COMMON_OBJS=cson_util.o cson_amalgamation_core.o

OBJS=$(INGEST_OBJS) $(ROLLUP_OBJS)
BINARIES=ingest rollup

DUMBWARN=-Wno-unused-function

#-L$(SRCHOME)/cson -lcson
LIBS= -lz -L$(SRCHOME)/cdb2api -luuid -l:libcdb2api.a -L$(SRCHOME)/protobuf -lcdb2protobuf -lprotobuf-c -lssl -lcrypto -lpthread
CXXFLAGS=-Wall -g -I$(SRCHOME)/cson -I$(SRCHOME)/cdb2api -std=c++11 $(DUMBWARN)
CFLAGS=-Wall -g -Wno-unused-const-variable

all: $(BINARIES)

ingest: $(INGEST_OBJS) $(COMMON_OBJS)
	g++ -o $@ $(INGEST_OBJS) $(COMMON_OBJS) $(LDFLAGS) $(LIBS)

rollup: $(ROLLUP_OBJS) $(COMMON_OBJS)
	g++ -o $@ $(ROLLUP_OBJS) $(COMMON_OBJS) $(LDFLAGS) $(LIBS)

%.o: %.cpp
	g++ -c $(CXXFLAGS) $< -o $@

clean:  
	rm -f $(BINARIES) $(OBJS) $(COMMON_OBJS)
