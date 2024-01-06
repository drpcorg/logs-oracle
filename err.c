#include "err.h"

const char* rcl_strerror(rcl_result value) {
  switch (value) {
    case RCLE_OK:
      return "ok";
    case RCLE_INVALID_UPSTREAM:
      return "invalid upstream";
    case RCLE_TOO_LARGE_QUERY:
      return "too large query";
    case RCLE_NODE_REQUEST:
      return "error when querying the node for logs";
    case RCLE_OUT_OF_MEMORY:
      return "failed memory allocation";
    case RCLE_FILESYSTEM:
      return "filesystem io";
    case RCLE_LIBCURL:
      return "libcurl internal error";
    case RCLE_UNKNOWN:
      return "unknown";
  }
}
