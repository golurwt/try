#ifndef REDIS_H
#define REDIS_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <chrono>
#include <cstdint>
#include <memory>

// Data types in our Redis
enum DataType {
    TYPE_STRING = 0,
    TYPE_LIST = 1,
    TYPE_SET = 2,
    TYPE_HASH = 3,
    TYPE_NONE = 4
};

// Value structure to hold different data types
struct RedisValue {
    DataType type = TYPE_NONE;
    std::string str_val;
    std::vector<std::string> list_val;
    std::set<std::string> set_val;
    std::map<std::string, std::string> hash_val;
    int64_t expire_at = -1;  // -1 means no expiration, else timestamp in ms
    
    RedisValue() = default;
    ~RedisValue() = default;
    
    bool is_expired() const {
        if (expire_at == -1) return false;
        auto now = std::chrono::system_clock::now();
        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()).count();
        return now_ms > expire_at;
    }
};

// Response status codes
enum ResponseStatus {
    RES_OK = 0,      // success
    RES_ERR = 1,     // error
    RES_NX = 2,      // not found
    RES_INT = 3,     // integer response
    RES_ARRAY = 4,   // array response
};

struct Response {
    uint32_t status = RES_OK;
    std::vector<uint8_t> data;
    std::vector<std::string> array_data;  // for array responses
};

#endif  // REDIS_H
