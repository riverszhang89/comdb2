OBJS=test.o
BINARY=test

LIBS=
CXXFLAGS=-Wall -g

$(BINARY): $(OBJS)
	g++ -o $(BINARY) $(OBJS) $(LDFLAGS) $(LIBS) -lz

%.o: %.c
	g++ -c $(CXXFLAGS) $< -o $@

clean:  
	rm -f $(BINARY) $(OBJS)
