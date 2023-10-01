#ifndef _RCL_BLOOM_H_
#define _RCL_BLOOM_H_

#include "common.h"

// Section: bloom filter
enum { LOGS_BLOOM_SIZE = 256 };

typedef uint8_t bloom_t[LOGS_BLOOM_SIZE];

void bloom_add(bloom_t* bloom, uint8_t* hash);
bool bloom_check(bloom_t* bloom, uint8_t* hash);

#endif  // _RCL_BLOOM_H_
