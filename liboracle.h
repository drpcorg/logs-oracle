#ifndef _RCL_H
#define _RCL_H

#include <math.h>
#include <stdint.h>

#include "common.h"
#include "upstream.h"

typedef enum {
  RCL_SUCCESS = 0,

  // User errors
  RCL_ERROR_INSERT_LOGS_TO_OLD_BLOCK,
  RCL_ERROR_INVALID_UPSTREAM,
  RCL_ERROR_TOO_BIG_QUERY,

  // Environment errors
  RCL_ERROR_MEMORY_ALLOCATION,
  RCL_ERROR_FS_IO,
  RCL_ERROR_UNKNOWN,
} rcl_result;

rcl_inline const char* rcl_error_str(rcl_result value) {
  switch (value) {
    case RCL_SUCCESS:
      return "success";
    case RCL_ERROR_INSERT_LOGS_TO_OLD_BLOCK:
      return "insert logs to old block";
    case RCL_ERROR_INVALID_UPSTREAM:
      return "invalid upstream";
    case RCL_ERROR_TOO_BIG_QUERY:
      return "too big query";
    case RCL_ERROR_MEMORY_ALLOCATION:
      return "memory allocation";
    case RCL_ERROR_FS_IO:
      return "fs io";
    case RCL_ERROR_UNKNOWN:
      return "unknown";
  }
}

struct rcl_query_address {
  uint64_t _hash;
  rcl_address_t _data;

  const char* encoded;
};

struct rcl_query_topics {
  uint64_t _hash;
  rcl_hash_t _data;

  const char* encoded;
};

typedef struct {
  uint64_t from, to;
  size_t alen, tlen[TOPICS_LENGTH];

  bool _has_addresses, _has_topics;

  struct rcl_query_address* address;
  struct rcl_query_topics* topics[TOPICS_LENGTH];
} rcl_query_t;

struct db;
typedef struct db rcl_t;

rcl_export rcl_result rcl_open(char* dir, uint64_t ram_limit, rcl_t** db);
rcl_export void rcl_free(rcl_t* db);

rcl_export rcl_result rcl_update_height(rcl_t* db, uint64_t height);
rcl_export rcl_result rcl_set_upstream(rcl_t* db, const char* upstream);

rcl_export rcl_result rcl_query(rcl_t* db,
                                rcl_query_t* query,
                                uint64_t* result);
rcl_export rcl_result rcl_query_alloc(rcl_query_t** query,
                                      size_t alen,
                                      size_t tlen[TOPICS_LENGTH]);
rcl_export void rcl_query_free(rcl_query_t* query);
rcl_export rcl_result rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs);

rcl_export rcl_result rcl_logs_count(rcl_t* db, uint64_t* result);
rcl_export rcl_result rcl_blocks_count(rcl_t* db, uint64_t* result);

#endif  // _RCL_H
