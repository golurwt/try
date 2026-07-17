CXX = clang++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2

TARGETS = redis-server redis-client

.PHONY: all clean run

all: $(TARGETS)

redis-server: server.cpp redis.h
	$(CXX) $(CXXFLAGS) -o redis-server server.cpp

redis-client: client.cpp
	$(CXX) $(CXXFLAGS) -o redis-client client.cpp

run-server: redis-server
	./redis-server

run-client: redis-client
	./redis-client

clean:
	rm -f $(TARGETS) *.o

help:
	@echo "Available targets:"
	@echo "  make all           - Build server and client"
	@echo "  make redis-server  - Build only server"
	@echo "  make redis-client  - Build only client"
	@echo "  make run-server    - Build and run server"
	@echo "  make run-client    - Build and run client"
	@echo "  make clean         - Remove built files"
