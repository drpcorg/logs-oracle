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

#include <cjson/cJSON.h>
#include <curl/curl.h>

#if defined _WIN32 || defined __CYGWIN__
#define rcl_export __declspec(dllexport)
#elif __GNUC__ >= 4
#define rcl_export __attribute__((visibility("default")))
#else
#define rcl_export
#endif

#ifdef __GNUC__  // GCC, Clang, ICC
#define rcl_unreachable() (__builtin_unreachable())
#elif defined(_MSC_VER)  // MSVC
#define rcl_unreachable() (__assume(false))
#else
#error "required 'unreachable' implementation"
#endif

#define rcl_inline static inline __attribute__((always_inline))

#define rcl_expect(c, x) __builtin_expect((long)(x), (c))
#define rcl_unlikely(x) rcl_expect(0, x)
#define rcl_likely(x) rcl_expect(1, x)

#define rcl_pointer_to(p, off) ((void*)((char*)(p) + (off)))

#define rcl_memcpy(dst, src, length) (void)memcpy(dst, src, (size_t)(length))
#define rcl_memmove(dst, src, length) (void)memmove(dst, src, (size_t)(length))

// Logs
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901)
#define CURRENT_FUNC __func__
#else
#define CURRENT_FUNC "(unknown)"
#endif

#define rcl_perror(s)                                                          \
  do {                                                                         \
    fprintf(stderr, "ERR [%s:%d -> %s]:\t", __FILE__, __LINE__, CURRENT_FUNC); \
    perror(s);                                                                 \
    fflush(stderr);                                                            \
  } while (false)

#define rcl_error(...)                                                         \
  do {                                                                         \
    fprintf(stderr, "ERR [%s:%d -> %s]:\t", __FILE__, __LINE__, CURRENT_FUNC); \
    fprintf(stderr, __VA_ARGS__);                                              \
    fflush(stderr);                                                            \
  } while (false)

#define rcl_info(...)                                            \
  do {                                                           \
    fprintf(stdout, "INFO [%s:%d -> %s]:\t", __FILE__, __LINE__, \
            CURRENT_FUNC);                                       \
    fprintf(stdout, __VA_ARGS__);                                \
    fflush(stdout);                                              \
  } while (false)

#ifdef NDEBUG
#define rcl_debug(...)                                                         \
  do {                                                                         \
    fprintf(stderr, "DBG [%s:%d -> %s]:\t", __FILE__, __LINE__, CURRENT_FUNC); \
    fprintf(stderr, __VA_ARGS__);                                              \
    fflush(stderr);                                                            \
  } while (false)
#else
#define rcl_debug(...) \
  do {                 \
  } while (false)
#endif

#define return_err(code, ...) \
  do {                        \
    exit_code = code;         \
    rcl_error(__VA_ARGS__);   \
    goto exit;                \
  } while (false);

// bloom filter
enum { LOGS_BLOOM_SIZE = 256 };

typedef uint8_t bloom_t[LOGS_BLOOM_SIZE];

#define bloom_init(bloom) memset(bloom, 0, sizeof(bloom_t));

void bloom_add(bloom_t* bloom, uint8_t* hash);
bool bloom_check(bloom_t* bloom, uint8_t* hash);

// Utils
int hex2bin(uint8_t* b, const char* str, int bytes);

uint64_t murmur64A(const void* key, const uint64_t len, const uint32_t seed);

#define max(a, b)           \
  ({                        \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a > _b ? _a : _b;      \
  })

#define min(a, b)           \
  ({                        \
    __typeof__(a) _a = (a); \
    __typeof__(b) _b = (b); \
    _a < _b ? _a : _b;      \
  })

#endif  // _RCL_COMMON_H
