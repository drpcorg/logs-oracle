#include "common.h"

uint64_t murmur64A(const void* key, const uint64_t len, const uint32_t seed) {
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

uint32_t xorshift32(void) {
  static uint32_t randseed = 0;
  if (randseed == 0)
    randseed = (uint32_t)time(NULL);

  uint32_t x = randseed;

  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;

  return randseed = x;
}

rcl_inline uint8_t ch2int(const char ch) {
  if (ch >= '0' && ch <= '9')
    return ch - '0';
  if (ch >= 'a' && ch <= 'z')
    return ch - 'a' + 10;
  if (ch >= 'A' && ch <= 'Z')
    return ch - 'A' + 10;
  return 0;  // well, ignore it
}

int hex2bin(uint8_t* b, const char* str, int bytes) {
  const char* ptr = str + 2;  // skip '0x' prefix
  for (int i = 0; i < bytes; ++i, ptr += 2)
    b[i] = ch2int(ptr[0]) * 16 + ch2int(ptr[1]);
  return 0;
}

static inline bool bloom_check_or_add(bloom_t* bloom, uint8_t* hash, bool add) {
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

void bloom_add(bloom_t* bloom, uint8_t* hash) {
  bloom_check_or_add(bloom, hash, true);
}

bool bloom_check(bloom_t* bloom, uint8_t* hash) {
  return bloom_check_or_add(bloom, hash, false);
}
