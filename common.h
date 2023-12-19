#ifndef _RCL_COMMON_H
#define _RCL_COMMON_H

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <curl/curl.h>
#include <jansson.h>

#if defined _WIN32 || defined __CYGWIN__
#define rcl_export __declspec(dllexport)
#elif __GNUC__ >= 4
#define rcl_export __attribute__((visibility("default")))
#else
#define rcl_export
#endif

#define rcl_inline static inline __attribute__((always_inline))

#define rcl_expect(c, x) __builtin_expect((long)(x), (c))
#define rcl_unlikely(x) rcl_expect(0, x)
#define rcl_likely(x) rcl_expect(1, x)

#define rcl_pointer_to(p, off) ((void*)((char*)(p) + (off)))

#define rcl_memcpy(dst, src, length) (void)memcpy(dst, src, (size_t)(length))
#define rcl_memmove(dst, src, length) (void)memmove(dst, src, (size_t)(length))

#ifdef DEBUG
#define rcl_debug(...)                                   \
  do {                                                   \
    fprintf(stderr, "DBG [" __FILE__ "]: " __VA_ARGS__); \
    fflush(stderr);                                      \
  } while (false)
#else
#define rcl_debug(...) \
  do {                 \
  } while (false)
#endif

#define rcl_error(...)                                   \
  do {                                                   \
    fprintf(stderr, "ERR [" __FILE__ "]: " __VA_ARGS__); \
    fflush(stderr);                                      \
  } while (false)

// Utils
enum {
  UPSTREAM_LIMIT = 4096,
  MAX_FILE_LENGTH = PATH_MAX,
};
typedef char rcl_filepath_t[MAX_FILE_LENGTH + 1];

rcl_inline bool includes(uint64_t key, uint64_t* arr, size_t size) {
  while (size-- > 0)
    if (arr[size] == key)
      return true;

  return false;
}

int hex2bin(uint8_t* b, const char* str, int bytes);

uint64_t murmur64A(const void* key, const uint64_t len, const uint32_t seed);

uint32_t xorshift32(void);

// bloom filter
enum { LOGS_BLOOM_SIZE = 256 };

typedef uint8_t bloom_t[LOGS_BLOOM_SIZE];

#define bloom_init(bloom) memset(bloom, 0, sizeof(bloom_t));

void bloom_add(bloom_t* bloom, uint8_t* hash);
bool bloom_check(bloom_t* bloom, uint8_t* hash);

#endif  // _RCL_COMMON_H
