#ifndef _RCL_H
#define _RCL_H

#include <math.h>
#include <stdint.h>

#include "common.h"
#include "err.h"
#include "upstream.h"

struct rcl_query_address {
  const char* encoded;

  uint64_t _hash;
  rcl_address_t _data;
};

struct rcl_query_topics {
  const char* encoded;

  uint64_t _hash;
  rcl_hash_t _data;
};

typedef struct {
  struct rcl_query_address* address;
  struct rcl_query_topics* topics[TOPICS_LENGTH];

  int64_t from, to, limit;
  size_t alen, tlen[TOPICS_LENGTH];

  bool _has_addresses, _has_topics;
} rcl_query_t;

struct rcl;
typedef struct rcl rcl_t;

rcl_export rcl_result rcl_open(char* dir, int64_t ram_limit, rcl_t** self);
rcl_export void rcl_free(rcl_t* self);

rcl_export rcl_result rcl_update_height(rcl_t* self, int64_t height);
rcl_export rcl_result rcl_set_upstream(rcl_t* self, const char* upstream);

rcl_export rcl_result rcl_query(rcl_t* self,
                                rcl_query_t* query,
                                int64_t* result);
rcl_export rcl_result rcl_query_alloc(rcl_query_t** query,
                                      size_t alen,
                                      size_t tlen[TOPICS_LENGTH]);
rcl_export void rcl_query_free(rcl_query_t* query);
rcl_export rcl_result rcl_insert(rcl_t* self, size_t size, rcl_log_t* logs);

rcl_export rcl_result rcl_logs_count(rcl_t* self, int64_t* result);
rcl_export rcl_result rcl_blocks_count(rcl_t* self, int64_t* result);

#endif  // _RCL_H
