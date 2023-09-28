#ifndef _LIBORACLE_COMMON_H
#define _LIBORACLE_COMMON_H

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// =====> Section: lang defines

// DLL utils; reference: https://gcc.gnu.org/wiki/Visibility
#if defined _WIN32 || defined __CYGWIN__
#define rcl_export __declspec(dllexport)
#elif __GNUC__ >= 4
#define rcl_export __attribute__((visibility("default")))
#else
#define rcl_export
#endif

#define rcl_inline static inline __attribute__((always_inline))

// Section: Utils
enum { MAX_FILE_LENGTH = 256 };
typedef char rcl_filename_t[MAX_FILE_LENGTH + 1];

rcl_inline bool includes(uint64_t key, uint64_t* arr, size_t size) {
  while (size-- > 0)
    if (arr[size] == key)
      return true;

  return false;
}

rcl_inline uint64_t murmur64A(const void* key,
                              const uint64_t len,
                              const uint32_t seed) {
  // MurmurHash3 was written by Austin Appleby, and is placed in the public
  // domain. The author hereby disclaims copyright to this source code.

  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = (const uint64_t*)key;
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const uint8_t* data2 = (const uint8_t*)data;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
  switch (len & 7) {
    case 7:
      h ^= (uint64_t)(data2[6]) << 48;
    case 6:
      h ^= (uint64_t)(data2[5]) << 40;
    case 5:
      h ^= (uint64_t)(data2[4]) << 32;
    case 4:
      h ^= (uint64_t)(data2[3]) << 24;
    case 3:
      h ^= (uint64_t)(data2[2]) << 16;
    case 2:
      h ^= (uint64_t)(data2[1]) << 8;
    case 1:
      h ^= (uint64_t)(data2[0]);
      h *= m;
  };
#pragma GCC diagnostic pop

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

// Section: bloom filter
enum { LOGS_BLOOM_SIZE = 256 };

typedef uint8_t bloom_t[LOGS_BLOOM_SIZE];

rcl_inline bool bloom_check_or_add(bloom_t* bloom, uint8_t* hash, bool add) {
  uint32_t mask = (1UL << 11UL) - 1;
  uint32_t a = 0, b = 0, c = 0;

  a = mask - ((((uint32_t)(hash[1]) << 8) + hash[0]) & mask);
  b = mask - ((((uint32_t)(hash[3]) << 8) + hash[2]) & mask);
  c = mask - ((((uint32_t)(hash[5]) << 8) + hash[4]) & mask);

  uint8_t ai = (uint8_t)(a / 8), av = (uint8_t)(1 << (7 - (a % 8)));
  uint8_t bi = (uint8_t)(b / 8), bv = (uint8_t)(1 << (7 - (b % 8)));
  uint8_t ci = (uint8_t)(c / 8), cv = (uint8_t)(1 << (7 - (c % 8)));

  if (add) {
    (*bloom)[ai] |= av;
    (*bloom)[bi] |= bv;
    (*bloom)[ci] |= cv;

    return false;
  } else {
    return ((*bloom)[ai] & av) && ((*bloom)[bi] & bv) && ((*bloom)[ci] & cv);
  }
}

rcl_inline void bloom_add(bloom_t* bloom, uint8_t* hash) {
  bloom_check_or_add(bloom, hash, true);
}

rcl_inline bool bloom_check(bloom_t* bloom, uint8_t* hash) {
  return bloom_check_or_add(bloom, hash, false);
}

#endif  // _LIBORACLE_COMMON_H
