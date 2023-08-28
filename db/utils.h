#ifndef _DB_UTILS_H
#define _DB_UTILS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define LOGS_BLOOM_SIZE 256

#define min(a, b)           \
  ({                        \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a < _b ? _a : _b;      \
  })

#define max(a, b)           \
  ({                        \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a > _b ? _a : _b;      \
  })

typedef uint8_t bloom_t[LOGS_BLOOM_SIZE];

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
uint64_t murmur64A(const void* key, const int len, const uint32_t seed);

void bloom_add(bloom_t* bloom, uint8_t* hash);
bool bloom_check(bloom_t* bloom, uint8_t* hash);
bool bloom_check_array(bloom_t* bloom, uint8_t** hashes, size_t size);

bool includes(uint64_t key, uint64_t* arr, size_t size);

#endif  // _DB_UTILS_H
