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

// Section: vector
typedef enum {
  RCL_VECTOR_INITED = 0,
  RCL_VECTOR_DESCRETE,
  RCL_VECTOR_EMBEDDED,
} rcl_vector_type_t;

typedef struct {
  void* start;
  uint64_t items, available, item_size;
  rcl_vector_type_t type : 8;
} rcl_vector_t;

rcl_inline rcl_vector_t* rcl_vector_create(uint64_t items, uint64_t item_size) {
  rcl_vector_t* vector = malloc(sizeof(rcl_vector_t) + items * item_size);

  if (rcl_likely(vector != NULL)) {
    vector->start = rcl_pointer_to(vector, sizeof(rcl_vector_t));
    vector->items = 0;
    vector->item_size = item_size;
    vector->available = items;
    vector->type = RCL_VECTOR_EMBEDDED;
  }

  return vector;
}

rcl_inline void* rcl_vector_init(rcl_vector_t* vector,
                                 uint64_t items,
                                 uint64_t item_size) {
  vector->start = malloc(items * item_size);

  if (rcl_likely(vector->start != NULL)) {
    vector->items = 0;
    vector->item_size = item_size;
    vector->available = items;
    vector->type = RCL_VECTOR_INITED;
  }

  return vector->start;
}

rcl_inline void* rcl_vector_at(rcl_vector_t* vector, uint64_t i) {
  return rcl_pointer_to(vector->start, vector->item_size * i);
}

rcl_inline void* rcl_vector_last(rcl_vector_t* vector) {
  return rcl_pointer_to(vector->start, vector->item_size * (vector->items - 1));
}

rcl_inline void rcl_vector_destroy(rcl_vector_t* vector) {
  switch (vector->type) {
    case RCL_VECTOR_INITED:
      free(vector->start);
#if DEBUG
      vector->start = NULL;
      vector->items = 0;
      vector->avalaible = 0;
#endif
      break;

    case RCL_VECTOR_DESCRETE:
      free(vector->start);
      /* Fall through. */

    case RCL_VECTOR_EMBEDDED:
      free(vector);
      break;
  }
}

rcl_inline void* rcl_vector_add(rcl_vector_t* vector) {
  uint64_t n = vector->available;

  if (n == vector->items) {
    size_t old_size = n * vector->item_size;

    if (n < 16) {
      n *= 2;
    } else {
      n += n / 2;
    }

    size_t size = n * vector->item_size;

    void* start = malloc(size);
    if (rcl_unlikely(start == NULL)) {
      return NULL;
    }

    vector->available = n;

    void* old = vector->start;
    vector->start = start;

    rcl_memcpy(start, old, old_size);

    if (vector->type == RCL_VECTOR_EMBEDDED) {
      vector->type = RCL_VECTOR_DESCRETE;
    } else {
      free(old);
    }
  }

  void* item = rcl_pointer_to(vector->start, vector->item_size * vector->items);

  vector->items++;

  return item;
}

rcl_inline void rcl_vector_remove(rcl_vector_t* vector, void* item) {
  uint64_t item_size = vector->item_size;

  uint8_t* end = rcl_pointer_to(vector->start, item_size * vector->items);
  uint8_t* last = end - item_size;

  if (item != last) {
    uint8_t* next = rcl_pointer_to(item, item_size);
    rcl_memmove(item, next, end - next);
  }

  vector->items--;
}

rcl_inline void* rcl_vector_remove_last(rcl_vector_t* vector) {
  vector->items--;
  return rcl_pointer_to(vector->start, vector->item_size * vector->items);
}

rcl_inline void rcl_vector_reset(rcl_vector_t* vector) {
  vector->items = 0;
}

rcl_inline bool rcl_vector_is_empty(rcl_vector_t* vector) {
  return vector->items == 0;
}

#endif  // _LIBORACLE_COMMON_H
