#ifndef _RCL_COMMON_H
#define _RCL_COMMON_H

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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

// Section: lang defines

// DLL utils; reference: https://gcc.gnu.org/wiki/Visibility
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
  // MurmurHash was written by Austin Appleby, and is placed in the public
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

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)(data2[6]) << 48;
      /* Fall through. */
    case 6:
      h ^= (uint64_t)(data2[5]) << 40;
      /* Fall through. */
    case 5:
      h ^= (uint64_t)(data2[4]) << 32;
      /* Fall through. */
    case 4:
      h ^= (uint64_t)(data2[3]) << 24;
      /* Fall through. */
    case 3:
      h ^= (uint64_t)(data2[2]) << 16;
      /* Fall through. */
    case 2:
      h ^= (uint64_t)(data2[1]) << 8;
      /* Fall through. */
    case 1:
      h ^= (uint64_t)(data2[0]);
      h *= m;
      /* Fall through. */
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

rcl_inline uint32_t xorshift32(void) {
  static uint32_t randseed = 0;
  if (randseed == 0)
    randseed = (uint32_t)time(NULL);

  uint32_t x = randseed;

  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;

  return randseed = x;
}

rcl_inline int hex2bin(uint8_t* b, const char* str, int bytes) {
  size_t len = strlen(str);

  if (len == bytes * 2 + 2 && str[0] == '0' &&
      (str[1] == 'x' || str[1] == 'X')) {
    str += 2;
  } else if (len != bytes * 2) {
    return -1;
  }

  unsigned int tmp;

  for (int i = 0; i < bytes; ++i) {
    sscanf(str + i * 2, "%02X", &tmp);
    b[i] = (uint8_t)tmp;
  }

  return 0;
}

#endif  // _RCL_COMMON_H
