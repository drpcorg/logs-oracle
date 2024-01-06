#ifndef _RCL_LOADER_H
#define _RCL_LOADER_H

#include "common.h"
#include "err.h"
#include "vector.h"

enum { HASH_LENGTH = 32, ADDRESS_LENGTH = 20, TOPICS_LENGTH = 4 };

typedef uint8_t rcl_address_t[ADDRESS_LENGTH];
typedef uint8_t rcl_hash_t[HASH_LENGTH];

typedef struct {
  uint64_t block_number;
  rcl_address_t address;
  rcl_hash_t topics[TOPICS_LENGTH];
} rcl_log_t;

typedef rcl_result (*rcl_upstream_callback_t)(vector_t* logs, void* data);

struct rcl_upstream;
typedef struct rcl_upstream rcl_upstream_t;

rcl_result rcl_upstream_init(rcl_upstream_t** self,
                             uint64_t last,
                             rcl_upstream_callback_t callback,
                             void* callback_data);
void rcl_upstream_free(rcl_upstream_t* self);

rcl_result rcl_upstream_set_url(rcl_upstream_t* self, const char* url);
rcl_result rcl_upstream_set_height(rcl_upstream_t* self, uint64_t height);

#endif  // _RCL_LOADER_H
