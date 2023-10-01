#include "bloom.h"

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
