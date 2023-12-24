#include "loader.h"
#include "common.h"

static void rcl_crawler_format_request(char* buffer,
                                       size_t buffer_size,
                                       uint64_t id,
                                       uint64_t from_block,
                                       uint64_t to_block) {
  static const char pattern[] =
      "{"
      "\"id\":%zu,"
      "\"jsonrpc\":\"2.0\","
      "\"method\":\"eth_getLogs\","
      "\"params\":[{\"fromBlock\":\"0x%" PRIx64 "\",\"toBlock\":\"0x%" PRIx64
      "\"}]"
      "}";

  snprintf(buffer, buffer_size, pattern, id, from_block, to_block);
}

typedef struct {
  char* buffer;
  int size;
} response_t;

static size_t response_onsend_callback(void* contents,
                                       size_t size,
                                       size_t nmemb,
                                       void* userp) {
  response_t* response = (response_t*)userp;

  size_t chunksize = size * nmemb;
  size_t datasize = response->size + chunksize + 1;
  if (datasize > MAX_RESPONSE_SIZE) {
    rcl_error("response_onsend_callback; too big response\n");
    return 0;
  }

  response->buffer = realloc(response->buffer, response->size + chunksize + 1);
  if (!response->buffer) {
    rcl_error("response_onsend_callback; not enough memory\n");
    return 0;
  }

  rcl_memcpy(&(response->buffer[response->size]), contents, chunksize);
  response->size += chunksize;
  response->buffer[response->size] = 0;

  return chunksize;
}

static int rcl_fetch_logs(const char* upstream,
                          uint64_t from,
                          uint64_t to,
                          response_t* response) {
  CURL* handle = curl_easy_init();
  if (handle == NULL)
    return -1;

  int64_t request_id = xorshift32();

  char payload[TEXT_BUFFER_SIZE];
  rcl_crawler_format_request(payload, TEXT_BUFFER_SIZE, request_id, from, to);

  curl_easy_setopt(handle, CURLOPT_URL, upstream);
  // curl_easy_setopt(handle, CURLOPT_CA_CACHE_TIMEOUT, CA_CACHE_TIMEOUT);
  curl_easy_setopt(handle, CURLOPT_POSTFIELDS, payload);

  struct curl_slist* headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);

  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, response_onsend_callback);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, (void*)response);

  CURLcode res = curl_easy_perform(handle);
  if (res != CURLE_OK) {
    rcl_error("rcl_fetch_logs; curl_easy_perform() failed: %s\n",
              curl_easy_strerror(res));
    goto error;
  }

  uint64_t response_code = 0;
  curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response_code);
  if (response_code != 200) {
    rcl_error("rcl_fetch_logs; server responded with code %ld\n",
              response_code);
    goto error;
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(handle);
  handle = headers = NULL;

  return 0;

error:
  if (headers)
    curl_slist_free_all(headers);

  if (handle)
    curl_easy_cleanup(handle);

  return -1;
}

int rcl_request_logs(const char* upstream,
                     uint64_t from,
                     uint64_t to,
                     vector_t* logs) {
  rcl_debug("rcl_request_logs: start; %zu, to: %zu\n", from, to);

  // load logs
  response_t response = {.size = 0, .buffer = NULL};
  if (rcl_fetch_logs(upstream, from, to, &response) != 0)
    goto error;

  rcl_debug("rcl_request_logs: successfully loaded\n");

  // parse logs
  json_error_t load_error;
  json_t* root = json_loads(response.buffer, 0, &load_error);

  if (!root) {
    rcl_error(
        "rcl_request_logs: couldn't parse response; error: %s, response: "
        "%.*s\n",
        load_error.text, response.size, response.buffer);
    goto error;
  }
  free(response.buffer);
  response.size = 0;
  response.buffer = NULL;

  if (!json_is_object(root)) {
    rcl_error("rcl_request_logs: root is not an object\n");
    goto error;
  }

  json_t* rid = json_object_get(root, "id");
  if (!json_is_integer(rid)) {
    rcl_error("rcl_request_logs: 'id' is not an integer\n");
    goto error;
  }

  json_t* error = json_object_get(root, "error");
  if (error != NULL) {
    if (json_is_string(error)) {
      rcl_error("rcl_request_logs: RPC error: %s\n", json_string_value(error));
    } else if (json_is_object(error)) {
      json_t* msg = json_object_get(error, "message");
      json_t* code = json_object_get(error, "code");

      rcl_error("rcl_request_logs: RPC error: [message] %s, [code] %lld\n",
                (json_is_string(msg) ? json_string_value(msg) : "unrecognized"),
                (json_is_integer(code) ? json_integer_value(code) : -1));
    } else {
      rcl_error("rcl_request_logs: RPC error: unrecognized\n");
    }

    goto error;
  }

  json_t* result = json_object_get(root, "result");
  if (!json_is_array(result)) {
    rcl_error("rcl_request_logs: result is not an array\n");
    goto error;
  }

  for (size_t i = 0, n = json_array_size(result); i < n; ++i) {
    json_t* item = json_array_get(result, i);
    if (!json_is_object(item)) {
      rcl_error("rcl_request_logs: %zu item is not object\n", i);
      goto error;
    }

    rcl_log_t* log = (rcl_log_t*)vector_add(logs);

    json_t* block_number = json_object_get(item, "blockNumber");
    if (json_is_string(block_number)) {
      errno = 0;

      const char* start = json_string_value(block_number);
      char* end = NULL;

      log->block_number = (uint64_t)strtoll(start, &end, 16);
      if (errno == ERANGE) {
        rcl_error("rcl_request_logs: %zu item, block_number range error\n", i);
        goto error;
      }
    } else {
      rcl_error("rcl_request_logs: %zu item, block_number is not a string\n",
                i);
      goto error;
    }

    json_t* address = json_object_get(item, "address");
    if (json_is_string(address)) {
      hex2bin(log->address, json_string_value(address), sizeof(rcl_address_t));
    } else {
      rcl_error("rcl_request_logs: %zu item, address is not a string\n", i);
      goto error;
    }

    json_t* topics = json_object_get(item, "topics");
    if (json_is_array(topics)) {
      size_t topics_size = json_array_size(topics);
      if (topics_size > TOPICS_LENGTH) {
        fprintf(stderr, "rcl_request_logs: %zu item, too many topics\n", i);
        goto error;
      }

      for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
        memset(log->topics[j], 0, sizeof(rcl_hash_t));

        if (j < topics_size) {
          json_t* topic = json_array_get(topics, j);
          if (!json_is_string(topic)) {
            fprintf(stderr,
                    "rcl_request_logs: %zu item, %zu topic is not a string\n",
                    i, j);
            goto error;
          }

          hex2bin(log->topics[j], json_string_value(topic), sizeof(rcl_hash_t));
        }
      }
    } else {
      rcl_error("rcl_request_logs: %zu item, topics is not an array\n", i);
      goto error;
    }
  }

  rcl_debug("rcl_request_logs: successfully parsed\n");

  json_decref(root);

  return 0;

error:
  if (response.buffer)
    free(response.buffer);

  if (root)
    json_decref(root);

  return -1;
}
