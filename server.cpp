// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <algorithm>
#include <ctime>
#include <fstream>
#include <sstream>
#include "redis.h"


static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg) {
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

const size_t k_max_msg = 32 << 20;
const size_t k_max_args = 200 * 1000;

struct Conn {
    int fd = -1;
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    std::vector<uint8_t> incoming;
    std::vector<uint8_t> outgoing;
};

// Global database
static std::map<std::string, RedisValue> g_data;
static int g_db_index = 0;

// append to the back
static void buf_append(std::vector<uint8_t> &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}

// remove from the front
static void buf_consume(std::vector<uint8_t> &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

// application callback when the listening socket is ready
static Conn *handle_accept(int fd) {
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0) {
        msg_errno("accept() error");
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    fd_set_nb(connfd);

    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    return conn;
}

static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) {
        return false;
    }
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

static bool read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

static int32_t parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr)) {
        return -1;
    }
    if (nstr > k_max_args) {
        return -1;
    }
    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) {
            return -1;
        }
    }
    if (data != end) {
        return -1;
    }
    return 0;
}

static std::string to_lower(const std::string &s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

static void make_response(const Response &resp, std::vector<uint8_t> &out) {
    if (resp.status == RES_ARRAY) {
        uint32_t count = resp.array_data.size();
        buf_append(out, (const uint8_t *)&count, 4);
        for (const auto &s : resp.array_data) {
            uint32_t len = s.size();
            buf_append(out, (const uint8_t *)&len, 4);
            buf_append(out, (const uint8_t *)s.data(), len);
        }
    } else {
        uint32_t resp_len = 4 + (uint32_t)resp.data.size();
        buf_append(out, (const uint8_t *)&resp_len, 4);
        buf_append(out, (const uint8_t *)&resp.status, 4);
        buf_append(out, resp.data.data(), resp.data.size());
    }
}

static void do_request(std::vector<std::string> &cmd, Response &out) {
    if (cmd.empty()) {
        out.status = RES_ERR;
        return;
    }

    std::string command = to_lower(cmd[0]);

    // String commands
    if (command == "get" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.is_expired()) {
            if (it != g_data.end()) {
                g_data.erase(it);
            }
            out.status = RES_NX;
            return;
        }
        if (it->second.type != TYPE_STRING) {
            out.status = RES_ERR;
            out.data.assign((uint8_t*)"WRONGTYPE Operation against a key holding the wrong kind of value",
                          (uint8_t*)"WRONGTYPE Operation against a key holding the wrong kind of value" + 66);
            return;
        }
        out.data.assign(it->second.str_val.begin(), it->second.str_val.end());
    } 
    else if (command == "set" && cmd.size() >= 3) {
        RedisValue val;
        val.type = TYPE_STRING;
        val.str_val = cmd[2];
        
        for (size_t i = 3; i < cmd.size(); i++) {
            std::string opt = to_lower(cmd[i]);
            if (opt == "ex" && i + 1 < cmd.size()) {
                int seconds = std::stoi(cmd[i + 1]);
                auto now = std::chrono::system_clock::now();
                val.expire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch()).count() + (int64_t)seconds * 1000;
                i++;
            } else if (opt == "px" && i + 1 < cmd.size()) {
                int ms = std::stoi(cmd[i + 1]);
                auto now = std::chrono::system_clock::now();
                val.expire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch()).count() + ms;
                i++;
            }
        }
        
        g_data[cmd[1]] = val;
        out.status = RES_OK;
    }
    else if (command == "del" && cmd.size() >= 2) {
        int deleted = 0;
        for (size_t i = 1; i < cmd.size(); i++) {
            if (g_data.erase(cmd[i]) > 0) {
                deleted++;
            }
        }
        out.status = RES_INT;
        int32_t count = deleted;
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "exists" && cmd.size() >= 2) {
        int count = 0;
        for (size_t i = 1; i < cmd.size(); i++) {
            auto it = g_data.find(cmd[i]);
            if (it != g_data.end() && !it->second.is_expired()) {
                count++;
            } else if (it != g_data.end()) {
                g_data.erase(it);
            }
        }
        out.status = RES_INT;
        int32_t result = count;
        out.data.assign((const uint8_t *)&result, (const uint8_t *)&result + 4);
    }
    else if (command == "incr" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it != g_data.end() && it->second.is_expired()) {
            g_data.erase(it);
            it = g_data.end();
        }
        
        int64_t val = 1;
        if (it != g_data.end()) {
            if (it->second.type != TYPE_STRING) {
                out.status = RES_ERR;
                return;
            }
            try {
                val = std::stoll(it->second.str_val) + 1;
            } catch (...) {
                out.status = RES_ERR;
                return;
            }
        }
        
        RedisValue new_val;
        new_val.type = TYPE_STRING;
        new_val.str_val = std::to_string(val);
        g_data[cmd[1]] = new_val;
        
        out.status = RES_INT;
        out.data.assign((const uint8_t *)&val, (const uint8_t *)&val + 8);
    }
    else if (command == "decr" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it != g_data.end() && it->second.is_expired()) {
            g_data.erase(it);
            it = g_data.end();
        }
        
        int64_t val = -1;
        if (it != g_data.end()) {
            if (it->second.type != TYPE_STRING) {
                out.status = RES_ERR;
                return;
            }
            try {
                val = std::stoll(it->second.str_val) - 1;
            } catch (...) {
                out.status = RES_ERR;
                return;
            }
        }
        
        RedisValue new_val;
        new_val.type = TYPE_STRING;
        new_val.str_val = std::to_string(val);
        g_data[cmd[1]] = new_val;
        
        out.status = RES_INT;
        out.data.assign((const uint8_t *)&val, (const uint8_t *)&val + 8);
    }
    else if (command == "append" && cmd.size() == 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            RedisValue val;
            val.type = TYPE_STRING;
            val.str_val = cmd[2];
            g_data[cmd[1]] = val;
        } else if (it->second.is_expired()) {
            g_data.erase(it);
            RedisValue val;
            val.type = TYPE_STRING;
            val.str_val = cmd[2];
            g_data[cmd[1]] = val;
        } else {
            if (it->second.type != TYPE_STRING) {
                out.status = RES_ERR;
                return;
            }
            it->second.str_val += cmd[2];
        }
        out.status = RES_INT;
        int32_t len = g_data[cmd[1]].str_val.length();
        out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
    }
    else if (command == "strlen" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.is_expired()) {
            out.status = RES_INT;
            int32_t len = 0;
            out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
        } else {
            if (it->second.type != TYPE_STRING) {
                out.status = RES_ERR;
                return;
            }
            out.status = RES_INT;
            int32_t len = it->second.str_val.length();
            out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
        }
    }

    // List commands
    else if (command == "lpush" && cmd.size() >= 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            RedisValue val;
            val.type = TYPE_LIST;
            for (size_t i = 2; i < cmd.size(); i++) {
                val.list_val.insert(val.list_val.begin(), cmd[i]);
            }
            g_data[cmd[1]] = val;
        } else if (it->second.type != TYPE_LIST) {
            out.status = RES_ERR;
            return;
        } else {
            for (size_t i = 2; i < cmd.size(); i++) {
                it->second.list_val.insert(it->second.list_val.begin(), cmd[i]);
            }
        }
        out.status = RES_INT;
        int32_t len = g_data[cmd[1]].list_val.size();
        out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
    }
    else if (command == "rpush" && cmd.size() >= 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            RedisValue val;
            val.type = TYPE_LIST;
            for (size_t i = 2; i < cmd.size(); i++) {
                val.list_val.push_back(cmd[i]);
            }
            g_data[cmd[1]] = val;
        } else if (it->second.type != TYPE_LIST) {
            out.status = RES_ERR;
            return;
        } else {
            for (size_t i = 2; i < cmd.size(); i++) {
                it->second.list_val.push_back(cmd[i]);
            }
        }
        out.status = RES_INT;
        int32_t len = g_data[cmd[1]].list_val.size();
        out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
    }
    else if (command == "lpop" && cmd.size() >= 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_LIST || it->second.list_val.empty()) {
            out.status = RES_NX;
            return;
        }
        out.data.assign(it->second.list_val[0].begin(), it->second.list_val[0].end());
        it->second.list_val.erase(it->second.list_val.begin());
    }
    else if (command == "rpop" && cmd.size() >= 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_LIST || it->second.list_val.empty()) {
            out.status = RES_NX;
            return;
        }
        out.data.assign(it->second.list_val.back().begin(), it->second.list_val.back().end());
        it->second.list_val.pop_back();
    }
    else if (command == "llen" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_LIST) {
            out.status = RES_INT;
            int32_t len = 0;
            out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
        } else {
            out.status = RES_INT;
            int32_t len = it->second.list_val.size();
            out.data.assign((const uint8_t *)&len, (const uint8_t *)&len + 4);
        }
    }
    else if (command == "lrange" && cmd.size() == 4) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_LIST) {
            out.status = RES_ARRAY;
            return;
        }
        try {
            int start = std::stoi(cmd[2]);
            int stop = std::stoi(cmd[3]);
            int size = it->second.list_val.size();
            
            if (start < 0) start = size + start;
            if (stop < 0) stop = size + stop;
            if (start < 0) start = 0;
            if (stop >= size) stop = size - 1;
            
            out.status = RES_ARRAY;
            if (start <= stop && start < size) {
                for (int i = start; i <= stop && i < size; i++) {
                    out.array_data.push_back(it->second.list_val[i]);
                }
            }
        } catch (...) {
            out.status = RES_ERR;
        }
    }

    // Set commands
    else if (command == "sadd" && cmd.size() >= 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            RedisValue val;
            val.type = TYPE_SET;
            for (size_t i = 2; i < cmd.size(); i++) {
                val.set_val.insert(cmd[i]);
            }
            g_data[cmd[1]] = val;
        } else if (it->second.type != TYPE_SET) {
            out.status = RES_ERR;
            return;
        } else {
            for (size_t i = 2; i < cmd.size(); i++) {
                it->second.set_val.insert(cmd[i]);
            }
        }
        out.status = RES_INT;
        int32_t count = g_data[cmd[1]].set_val.size();
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "srem" && cmd.size() >= 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_SET) {
            out.status = RES_INT;
            int32_t count = 0;
            out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
            return;
        }
        int removed = 0;
        for (size_t i = 2; i < cmd.size(); i++) {
            if (it->second.set_val.erase(cmd[i])) {
                removed++;
            }
        }
        out.status = RES_INT;
        int32_t count = removed;
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "smembers" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_ARRAY;
        if (it != g_data.end() && it->second.type == TYPE_SET) {
            for (const auto &member : it->second.set_val) {
                out.array_data.push_back(member);
            }
        }
    }
    else if (command == "scard" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_INT;
        int32_t count = 0;
        if (it != g_data.end() && it->second.type == TYPE_SET) {
            count = it->second.set_val.size();
        }
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "sismember" && cmd.size() == 3) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_INT;
        int32_t result = 0;
        if (it != g_data.end() && it->second.type == TYPE_SET) {
            result = it->second.set_val.count(cmd[2]) > 0 ? 1 : 0;
        }
        out.data.assign((const uint8_t *)&result, (const uint8_t *)&result + 4);
    }

    // Hash commands
    else if (command == "hset" && cmd.size() >= 4 && (cmd.size() % 2 == 0)) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end()) {
            RedisValue val;
            val.type = TYPE_HASH;
            for (size_t i = 2; i < cmd.size(); i += 2) {
                val.hash_val[cmd[i]] = cmd[i + 1];
            }
            g_data[cmd[1]] = val;
        } else if (it->second.type != TYPE_HASH) {
            out.status = RES_ERR;
            return;
        } else {
            for (size_t i = 2; i < cmd.size(); i += 2) {
                it->second.hash_val[cmd[i]] = cmd[i + 1];
            }
        }
        out.status = RES_INT;
        int32_t count = g_data[cmd[1]].hash_val.size();
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "hget" && cmd.size() == 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_HASH) {
            out.status = RES_NX;
            return;
        }
        auto field_it = it->second.hash_val.find(cmd[2]);
        if (field_it == it->second.hash_val.end()) {
            out.status = RES_NX;
            return;
        }
        out.data.assign(field_it->second.begin(), field_it->second.end());
    }
    else if (command == "hdel" && cmd.size() >= 3) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.type != TYPE_HASH) {
            out.status = RES_INT;
            int32_t count = 0;
            out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
            return;
        }
        int deleted = 0;
        for (size_t i = 2; i < cmd.size(); i++) {
            if (it->second.hash_val.erase(cmd[i])) {
                deleted++;
            }
        }
        out.status = RES_INT;
        int32_t count = deleted;
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "hgetall" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_ARRAY;
        if (it != g_data.end() && it->second.type == TYPE_HASH) {
            for (const auto &pair : it->second.hash_val) {
                out.array_data.push_back(pair.first);
                out.array_data.push_back(pair.second);
            }
        }
    }
    else if (command == "hlen" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_INT;
        int32_t count = 0;
        if (it != g_data.end() && it->second.type == TYPE_HASH) {
            count = it->second.hash_val.size();
        }
        out.data.assign((const uint8_t *)&count, (const uint8_t *)&count + 4);
    }
    else if (command == "hexists" && cmd.size() == 3) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_INT;
        int32_t result = 0;
        if (it != g_data.end() && it->second.type == TYPE_HASH) {
            result = it->second.hash_val.count(cmd[2]) > 0 ? 1 : 0;
        }
        out.data.assign((const uint8_t *)&result, (const uint8_t *)&result + 4);
    }
    else if (command == "hkeys" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_ARRAY;
        if (it != g_data.end() && it->second.type == TYPE_HASH) {
            for (const auto &pair : it->second.hash_val) {
                out.array_data.push_back(pair.first);
            }
        }
    }
    else if (command == "hvals" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_ARRAY;
        if (it != g_data.end() && it->second.type == TYPE_HASH) {
            for (const auto &pair : it->second.hash_val) {
                out.array_data.push_back(pair.second);
            }
        }
    }

    // Key management commands
    else if (command == "keys" && cmd.size() == 2) {
        out.status = RES_ARRAY;
        std::string pattern = cmd[1];
        for (auto it = g_data.begin(); it != g_data.end(); ) {
            if (it->second.is_expired()) {
                it = g_data.erase(it);
            } else {
                if (pattern == "*" || it->first.find(pattern) != std::string::npos) {
                    out.array_data.push_back(it->first);
                }
                ++it;
            }
        }
    }
    else if (command == "expire" && cmd.size() == 3) {
        try {
            int seconds = std::stoi(cmd[2]);
            auto it = g_data.find(cmd[1]);
            if (it == g_data.end()) {
                out.status = RES_INT;
                int32_t result = 0;
                out.data.assign((const uint8_t *)&result, (const uint8_t *)&result + 4);
                return;
            }
            auto now = std::chrono::system_clock::now();
            it->second.expire_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count() + (int64_t)seconds * 1000;
            out.status = RES_INT;
            int32_t result = 1;
            out.data.assign((const uint8_t *)&result, (const uint8_t *)&result + 4);
        } catch (...) {
            out.status = RES_ERR;
        }
    }
    else if (command == "ttl" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        out.status = RES_INT;
        int32_t ttl = -2;
        if (it != g_data.end()) {
            if (it->second.is_expired()) {
                g_data.erase(it);
            } else if (it->second.expire_at == -1) {
                ttl = -1;
            } else {
                auto now = std::chrono::system_clock::now();
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch()).count();
                ttl = (it->second.expire_at - now_ms) / 1000;
            }
        }
        out.data.assign((const uint8_t *)&ttl, (const uint8_t *)&ttl + 4);
    }
    else if (command == "type" && cmd.size() == 2) {
        auto it = g_data.find(cmd[1]);
        if (it == g_data.end() || it->second.is_expired()) {
            out.data.assign((uint8_t*)"none", (uint8_t*)"none" + 4);
            return;
        }
        const char *type_str;
        if (it->second.type == TYPE_STRING) type_str = "string";
        else if (it->second.type == TYPE_LIST) type_str = "list";
        else if (it->second.type == TYPE_SET) type_str = "set";
        else if (it->second.type == TYPE_HASH) type_str = "hash";
        else type_str = "unknown";
        out.data.assign((uint8_t*)type_str, (uint8_t*)type_str + strlen(type_str));
    }

    // Database commands
    else if (command == "select" && cmd.size() == 2) {
        try {
            int db = std::stoi(cmd[1]);
            if (db < 0 || db > 15) {
                out.status = RES_ERR;
                return;
            }
            g_db_index = db;
            out.status = RES_OK;
        } catch (...) {
            out.status = RES_ERR;
        }
    }
    else if (command == "flushdb") {
        g_data.clear();
        out.status = RES_OK;
    }
    else if (command == "flushall") {
        g_data.clear();
        out.status = RES_OK;
    }
    else if (command == "dbsize") {
        out.status = RES_INT;
        int32_t size = g_data.size();
        out.data.assign((const uint8_t *)&size, (const uint8_t *)&size + 4);
    }
    else if (command == "info") {
        std::string info = "# Server\r\n";
        info += "redis_version:7.0.0 (Custom)\r\n";
        info += "redis_mode:standalone\r\n";
        info += "# Clients\r\n";
        info += "connected_clients:1\r\n";
        info += "# Memory\r\n";
        info += "used_memory:1024\r\n";
        info += "# Stats\r\n";
        info += "total_connections_received:1\r\n";
        info += "total_commands_processed:1\r\n";
        info += "# Keyspace\r\n";
        info += "db0:keys=" + std::to_string(g_data.size()) + "\r\n";
        out.data.assign(info.begin(), info.end());
    }

    // Server commands
    else if (command == "ping") {
        if (cmd.size() == 1) {
            out.data.assign((uint8_t*)"PONG", (uint8_t*)"PONG" + 4);
        } else {
            out.data.assign(cmd[1].begin(), cmd[1].end());
        }
    }
    else if (command == "echo" && cmd.size() == 2) {
        out.data.assign(cmd[1].begin(), cmd[1].end());
    }
    else if (command == "command") {
        out.status = RES_OK;
        out.data.assign((uint8_t*)"OK", (uint8_t*)"OK" + 2);
    }
    else {
        out.status = RES_ERR;
        std::string err_msg = "ERR unknown command '" + command + "'";
        out.data.assign(err_msg.begin(), err_msg.end());
    }
}

static bool try_one_request(Conn *conn) {
    if (conn->incoming.size() < 4) {
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->want_close = true;
        return false;
    }
    if (4 + len > conn->incoming.size()) {
        return false;
    }

    const uint8_t *request = &conn->incoming[4];

    std::vector<std::string> cmd;
    if (parse_req(request, len, cmd) < 0) {
        msg("bad request");
        conn->want_close = true;
        return false;
    }

    Response resp;
    do_request(cmd, resp);
    make_response(resp, conn->outgoing);

    buf_consume(conn->incoming, 4 + len);
    return true;
}

static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    if (rv < 0) {
        msg_errno("write() error");
        conn->want_close = true;
        return;
    }

    buf_consume(conn->outgoing, (size_t)rv);

    if (conn->outgoing.size() == 0) {
        conn->want_read = true;
        conn->want_write = false;
    }
}

static void handle_read(Conn *conn) {
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return;
    }
    if (rv < 0) {
        msg_errno("read() error");
        conn->want_close = true;
        return;
    }
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return;
    }

    buf_append(conn->incoming, buf, (size_t)rv);

    while (try_one_request(conn)) {}

    if (conn->outgoing.size() > 0) {
        conn->want_read = false;
        conn->want_write = true;
        return handle_write(conn);
    }
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(6379);
    addr.sin_addr.s_addr = ntohl(INADDR_ANY);
    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    fd_set_nb(fd);

    std::vector<Conn *> fd_to_conn;
    std::vector<struct pollfd> poll_args;
    while (true) {
        poll_args.clear();
        poll_args.push_back({fd, POLLIN, 0});
        for (Conn *conn : fd_to_conn) {
            if (!conn) {
                continue;
            }
            struct pollfd pf = {conn->fd, 0, 0};
            if (conn->want_read) {
                pf.events |= POLLIN;
            }
            if (conn->want_write) {
                pf.events |= POLLOUT;
            }
            poll_args.push_back(pf);
        }

        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), 1000);
        if (rv < 0) {
            die("poll");
        }

        for (size_t i = 1; i < poll_args.size(); ++i) {
            if (poll_args[i].revents) {
                Conn *conn = fd_to_conn[i - 1];
                handle_read(conn);
                if (conn->want_write) {
                    handle_write(conn);
                }
                if (conn->want_close) {
                    delete conn;
                    fd_to_conn[i - 1] = NULL;
                }
            }
        }

        if (poll_args[0].revents) {
            (void)handle_accept(fd);
            Conn *conn = handle_accept(fd);
            if (conn) {
                fd_to_conn.push_back(conn);
            }
        }
    }

    return 0;
}