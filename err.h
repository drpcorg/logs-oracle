#ifndef _RCL_ERR_H
#define _RCL_ERR_H

#include "common.h"

typedef enum {
  RCLE_OK = 0,

  RCLE_INVALID_UPSTREAM,
  RCLE_TOO_LARGE_QUERY,
  RCLE_NODE_REQUEST,

  RCLE_OUT_OF_MEMORY,
  RCLE_FILESYSTEM,
  RCLE_LIBCURL,
  RCLE_UNKNOWN,
} rcl_result;

rcl_export const char* rcl_strerror(rcl_result value);

#endif  // _RCL_ERR_H
