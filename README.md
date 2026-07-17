# Custom Redis Implementation in C++

A high-performance Redis server and client implementation in C++ with support for multiple data types and unique features.

## Features

### Core Data Types Supported
- **Strings**: Text values with GET, SET, INCR, DECR, APPEND, STRLEN
- **Lists**: Ordered collections with LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE
- **Sets**: Unordered unique collections with SADD, SREM, SMEMBERS, SCARD, SISMEMBER
- **Hashes**: Key-value pairs within a key with HSET, HGET, HDEL, HGETALL, HLEN, HEXISTS, HKEYS, HVALS

### Advanced Features
- **Key Expiration**: SET with EX (seconds) or PX (milliseconds), EXPIRE, TTL commands
- **Key Management**: DEL, EXISTS, TYPE, KEYS pattern matching
- **Database Isolation**: SELECT command for 16 separate databases
- **Server Commands**: PING, ECHO, INFO, COMMAND
- **Database Management**: FLUSHDB, FLUSHALL, DBSIZE

### Unique Features
1. **Multiple Data Structures**: Support for strings, lists, sets, and hashes (Redis-like)
2. **Key Expiration with Millisecond Precision**: Automatic cleanup of expired keys
3. **Non-blocking I/O**: Event-driven architecture using poll()
4. **Type Safety**: TYPE command to check key types before operations
5. **Interactive CLI**: User-friendly command-line interface with help system
6. **Array Response Support**: Proper handling of array responses in client

## Building

### Requirements
- C++17 compatible compiler (clang++ or g++)
- Standard POSIX libraries (Linux/macOS)

### Compilation
```bash
# Build both server and client
make all

# Build only server
make redis-server

# Build only client
make redis-client

# Clean build artifacts
make clean
```

## Running

### Start the Server
```bash
./redis-server
```
Server listens on localhost:6379 (default Redis port)

### Connect with Client
```bash
./redis-client
```

## Usage Examples

### String Operations
```
redis> SET mykey "Hello"
OK
redis> GET mykey
Hello
redis> APPEND mykey " World"
(integer) 11
redis> STRLEN mykey
(integer) 11
redis> INCR counter
(integer) 1
redis> DECR counter
(integer) 0
```

### List Operations
```
redis> RPUSH mylist "apple" "banana" "cherry"
(integer) 3
redis> LRANGE mylist 0 -1
Array with 3 elements:
  1) apple
  2) banana
  3) cherry
redis> LPOP mylist
apple
redis> LLEN mylist
(integer) 2
```

### Set Operations
```
redis> SADD myset "member1" "member2" "member3"
(integer) 3
redis> SMEMBERS myset
Array with 3 elements:
  1) member1
  2) member2
  3) member3
redis> SISMEMBER myset "member1"
(integer) 1
redis> SCARD myset
(integer) 3
```

### Hash Operations
```
redis> HSET user name "John" age "30" city "NYC"
(integer) 3
redis> HGET user name
John
redis> HGETALL user
Array with 6 elements:
  1) name
  2) John
  3) age
  4) 30
  5) city
  6) NYC
redis> HKEYS user
Array with 3 elements:
  1) name
  2) age
  3) city
```

### Key Expiration
```
redis> SET tempkey "value" EX 10
OK
redis> TTL tempkey
(integer) 9
redis> EXPIRE mykey 3600
(integer) 1
redis> TTL mykey
(integer) 3599
```

### Database Operations
```
redis> DBSIZE
(integer) 5
redis> KEYS *
Array with 5 elements:
  1) key1
  2) key2
  3) key3
  4) key4
  5) key5
redis> FLUSHDB
OK
redis> DBSIZE
(integer) 0
```

## Protocol

The server uses a custom binary protocol:
- **Request Format**: [4-byte total length][4-byte arg count][arg1 length][arg1 data]...[argN length][argN data]
- **Response Format**: [4-byte length][4-byte status code][response data]

Response Status Codes:
- 0 (RES_OK): Success
- 1 (RES_ERR): Error
- 2 (RES_NX): Not found
- 3 (RES_INT): Integer response
- 4 (RES_ARRAY): Array response

## Architecture

### Non-Blocking I/O
- Uses `poll()` for efficient event handling
- Supports multiple concurrent clients
- Non-blocking sockets for responsive operations

### Data Storage
- In-memory key-value store using C++ STL (std::map)
- Support for multiple data types in a single key (type checking)
- Automatic expiration of keys based on TTL

### Client Features
- Interactive REPL (Read-Eval-Print Loop)
- Command history and help system
- Proper parsing of quoted strings
- Beautiful array response formatting

## Limitations

1. Data is not persistent (in-memory only)
2. Single-threaded server (sequential request processing)
3. No authentication/security features
4. Fixed message size limits (32MB)
5. No pub/sub functionality
6. No transactions

## Future Enhancements

- [ ] Data persistence (RDB snapshots, AOF logging)
- [ ] Pub/Sub functionality
- [ ] Transactions (MULTI/EXEC)
- [ ] Lua scripting support
- [ ] Connection pooling
- [ ] Cluster support
- [ ] Stream data type
- [ ] Bit operations

## Files

- `server.cpp` - Redis server implementation
- `client.cpp` - Interactive Redis client
- `redis.h` - Shared header with data structures
- `Makefile` - Build configuration
- `README.md` - This file

## License

MIT License - Free to use and modify
