#include "utils.h"

uint64_t murmur64A(const void* key, const uint64_t len, const uint32_t seed) {
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

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

bool bloom_check_or_add(bloom_t* bloom, uint8_t* hash, bool add) {
  uint32_t mask = (1UL << 11UL) - 1;
  uint32_t a = 0, b = 0, c = 0;

  a = mask - ((((uint32_t)(hash[1]) << 8) + hash[0]) & mask);
  b = mask - ((((uint32_t)(hash[3]) << 8) + hash[2]) & mask);
  c = mask - ((((uint32_t)(hash[5]) << 8) + hash[4]) & mask);

  uint8_t ai = a / 8, av = 1 << (7 - (a % 8));
  uint8_t bi = b / 8, bv = 1 << (7 - (b % 8));
  uint8_t ci = c / 8, cv = 1 << (7 - (c % 8));

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

bool bloom_check_array(bloom_t* bloom, uint8_t** hashes, size_t size) {
  while (size-- > 0)
    if (bloom_check(bloom, hashes[size]))
      return true;

  return false;
}

bool includes(uint64_t key, uint64_t* arr, size_t size) {
  while (size-- > 0)
    if (arr[size] == key)
      return true;

  return false;
}