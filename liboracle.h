#ifndef _RCL_H
#define _RCL_H

#include <math.h>
#include <stdint.h>

#include "common.h"
#include "loader.h"

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

struct rcl_query_address {
  size_t len;
  const char** data;
};

struct rcl_query_topics {
  size_t len;
  const char** data;
};

typedef struct {
  uint64_t from_block, to_block;
  struct rcl_query_address address;
  struct rcl_query_topics topics[TOPICS_LENGTH];
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
rcl_export rcl_result rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs);

rcl_export rcl_result rcl_logs_count(rcl_t* db, uint64_t* result);
rcl_export rcl_result rcl_blocks_count(rcl_t* db, uint64_t* result);

#endif  // _RCL_H
