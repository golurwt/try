#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

const size_t k_max_msg = 4096;

static int32_t send_req(int fd, const std::vector<std::string> &cmd) {
    uint32_t len = 4;
    for (const std::string &s : cmd) {
        len += 4 + s.size();
    }
    if (len > k_max_msg) {
        return -1;
    }

    char wbuf[4 + k_max_msg];
    memcpy(&wbuf[0], &len, 4);
    uint32_t n = cmd.size();
    memcpy(&wbuf[4], &n, 4);
    size_t cur = 8;
    for (const std::string &s : cmd) {
        uint32_t p = (uint32_t)s.size();
        memcpy(&wbuf[cur], &p, 4);
        memcpy(&wbuf[cur + 4], s.data(), s.size());
        cur += 4 + s.size();
    }
    return write_all(fd, wbuf, 4 + len);
}

static void print_array_response(const char *rbuf, uint32_t len) {
    const uint8_t *cur = (uint8_t *)rbuf;
    const uint8_t *end = cur + len;
    
    uint32_t count = 0;
    if (cur + 4 > end) return;
    memcpy(&count, cur, 4);
    cur += 4;
    
    printf("Array with %u elements:\n", count);
    for (uint32_t i = 0; i < count; i++) {
        if (cur + 4 > end) break;
        uint32_t elem_len = 0;
        memcpy(&elem_len, cur, 4);
        cur += 4;
        
        if (cur + elem_len > end) break;
        printf("  %u) %.*s\n", i + 1, elem_len, (char *)cur);
        cur += elem_len;
    }
}

static int32_t read_res(int fd) {
    char rbuf[4 + k_max_msg];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err) {
        if (errno == 0) {
            msg("EOF");
        } else {
            msg("read() error");
        }
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > k_max_msg) {
        msg("too long");
        return -1;
    }

    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }

    uint32_t rescode = 0;
    if (len < 4) {
        msg("bad response");
        return -1;
    }
    
    memcpy(&rescode, &rbuf[4], 4);
    
    // Handle different response types
    if (rescode == 4) {  // RES_ARRAY
        print_array_response(&rbuf[4], len);
    } else if (rescode == 3) {  // RES_INT
        if (len >= 12) {
            int64_t val = 0;
            memcpy(&val, &rbuf[8], 8);
            printf("(integer) %ld\n", val);
        }
    } else if (rescode == 0) {  // RES_OK
        if (len > 4) {
            printf("%.*s\n", (int)(len - 4), &rbuf[8]);
        } else {
            printf("OK\n");
        }
    } else if (rescode == 2) {  // RES_NX
        printf("(nil)\n");
    } else if (rescode == 1) {  // RES_ERR
        printf("ERR: %.*s\n", (int)(len - 4), &rbuf[8]);
    } else {
        printf("[%u] %.*s\n", rescode, (int)(len - 4), &rbuf[8]);
    }
    
    return 0;
}

static void print_help() {
    printf("\n=== Redis CLI Help ===\n");
    printf("String Commands: GET, SET, INCR, DECR, APPEND, STRLEN\n");
    printf("List Commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE\n");
    printf("Set Commands: SADD, SREM, SMEMBERS, SCARD, SISMEMBER\n");
    printf("Hash Commands: HSET, HGET, HDEL, HGETALL, HLEN, HEXISTS, HKEYS, HVALS\n");
    printf("Key Commands: DEL, EXISTS, EXPIRE, TTL, TYPE, KEYS\n");
    printf("DB Commands: SELECT, FLUSHDB, FLUSHALL, DBSIZE\n");
    printf("Server: PING, ECHO, INFO, COMMAND\n");
    printf("Type 'QUIT' or 'EXIT' to exit\n");
    printf("Type 'HELP' to see this message again\n");
    printf("=====================\n\n");
}

int main(int argc, char **argv) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(6379);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect");
    }

    printf("Connected to Redis server at 127.0.0.1:6379\n");
    print_help();

    std::string line;
    while (true) {
        printf("redis> ");
        fflush(stdout);
        
        if (!std::getline(std::cin, line)) {
            break;
        }

        // Trim whitespace
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) {
            continue;
        }
        line = line.substr(start);

        // Check for exit commands
        std::string cmd_upper = line;
        std::transform(cmd_upper.begin(), cmd_upper.end(), cmd_upper.begin(), ::toupper);
        
        if (cmd_upper == "QUIT" || cmd_upper == "EXIT") {
            printf("Goodbye!\n");
            break;
        }
        
        if (cmd_upper == "HELP") {
            print_help();
            continue;
        }

        // Parse command
        std::vector<std::string> cmd;
        std::istringstream iss(line);
        std::string token;
        while (iss >> token) {
            // Handle quoted strings
            if (token[0] == '"' || token[0] == '\'') {
                char quote = token[0];
                std::string quoted = token.substr(1);
                while (quoted.back() != quote && iss >> token) {
                    quoted += " " + token;
                }
                if (quoted.back() == quote) {
                    quoted = quoted.substr(0, quoted.length() - 1);
                }
                cmd.push_back(quoted);
            } else {
                cmd.push_back(token);
            }
        }

        if (cmd.empty()) {
            continue;
        }

        int32_t err = send_req(fd, cmd);
        if (err) {
            printf("send error\n");
            continue;
        }
        
        err = read_res(fd);
        if (err) {
            printf("read error\n");
            continue;
        }
    }

    close(fd);
    return 0;
}