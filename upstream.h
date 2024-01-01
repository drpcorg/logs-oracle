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

typedef int (*rcl_upstream_callback_t)(vector_t* logs, void* data);

typedef struct {
  uint64_t height, last;

  bool closed;

  char* url;
  rcl_upstream_callback_t callback;
  void* callback_data;

  pthread_t* thrd;
} rcl_upstream_t;

void rcl_upstream_init(rcl_upstream_t* self,
                       uint64_t last,
                       rcl_upstream_callback_t callback,
                       void* callback_data);
void rcl_upstream_free(rcl_upstream_t* self);

int rcl_upstream_set_url(rcl_upstream_t* self, const char* url);
void rcl_upstream_set_height(rcl_upstream_t* self, uint64_t height);

#endif  // _RCL_LOADER_H
