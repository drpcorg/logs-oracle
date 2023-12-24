#ifndef _RCL_LOADER_H
#define _RCL_LOADER_H

#include "common.h"
#include "vector.h"

#define CA_CACHE_TIMEOUT 604800L
#define TEXT_BUFFER_SIZE 4096L
#define MAX_RESPONSE_SIZE (1024 * 1024 * 512)  // 512MB

enum { HASH_LENGTH = 32, ADDRESS_LENGTH = 20, TOPICS_LENGTH = 4 };

typedef uint8_t rcl_address_t[ADDRESS_LENGTH];
typedef uint8_t rcl_hash_t[HASH_LENGTH];

typedef struct {
  uint64_t block_number;
  rcl_address_t address;
  rcl_hash_t topics[TOPICS_LENGTH];
} rcl_log_t;

int rcl_request_logs(const char* upstream,
                     uint64_t from,
                     uint64_t to,
                     vector_t* logs);

#endif  // _RCL_LOADER_H
