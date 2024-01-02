#include "upstream.h"
#include <unistd.h>
#include "liboracle.h"
#include "vector.h"

enum {
  CONNECTIONS_COUNT = 32,
  BLOCKS_REQUEST_BATCH = 16,
};

typedef struct {
  uint32_t id;
  uint64_t from, to;

  size_t size;
  char* buffer;
  char payload[TEXT_BUFFER_SIZE + 1];

  bool ended;
  vector_t logs;
} req_t;

static void* rcl_fetcher_thread(void* data);
static size_t req_onsend(void* contents,
                         size_t size,
                         size_t nmemb,
                         void* userp);

int rcl_upstream_init(rcl_upstream_t* self,
                      uint64_t last,
                      rcl_upstream_callback_t callback,
                      void* callback_data) {
  srand(time(NULL));
  curl_global_init(CURL_GLOBAL_DEFAULT);

  self->url = NULL;
  self->last = last;
  self->height = 0;
  self->closed = false;

  self->callback = callback;
  self->callback_data = callback_data;

  self->thrd = NULL;

  self->multi_handle = curl_multi_init();
  self->http_headers = NULL;
  self->http_headers =
      curl_slist_append(self->http_headers, "Accept: application/json");
  self->http_headers =
      curl_slist_append(self->http_headers, "Content-Type: application/json");

  vector_init(&(self->requests), CONNECTIONS_COUNT, sizeof(req_t));
  vector_init(&(self->handles), CONNECTIONS_COUNT, sizeof(CURL*));
  for (size_t i = 0; i < CONNECTIONS_COUNT; ++i) {
    CURL** handle = vector_add(&(self->handles));
    if ((*handle = curl_easy_init()) == NULL)
      return -1;

    req_t* req = vector_add(&(self->requests));
    req->id = 0;
    req->from = 0;
    req->to = 0;
    req->size = 0;
    req->buffer = NULL;
    vector_init(&(req->logs), 16, sizeof(rcl_log_t));
  }

  return 0;
}

void rcl_upstream_free(rcl_upstream_t* self) {
  // notify
  self->closed = true;

  // wait enf end of upstream
  if (self->thrd) {
    pthread_join(*(self->thrd), NULL);
    free(self->thrd);
  }

  // clear
  if (self->multi_handle)
    curl_multi_cleanup(self->multi_handle);

  for (size_t i = 0; i < CONNECTIONS_COUNT; ++i) {
    CURL** handle = vector_at(&(self->handles), i);
    curl_easy_cleanup(*handle);

    req_t* req = vector_at(&(self->requests), i);
    vector_destroy(&(req->logs));
    if (req->buffer)
      free(req->buffer);
  }
  vector_destroy(&(self->requests));
  vector_destroy(&(self->handles));

  if (self->url)
    curl_url_cleanup(self->url);
  if (self->http_headers)
    curl_slist_free_all(self->http_headers);

  curl_global_cleanup();
}

void rcl_upstream_set_height(rcl_upstream_t* self, uint64_t height) {
  self->height = height;
}

int rcl_upstream_set_url(rcl_upstream_t* self, const char* url) {
  if (self->url == NULL)
    self->url = curl_url();

  int rc = curl_url_set(self->url, CURLUPART_URL, url, 0);
  if (rc != CURLUE_OK)
    return RCL_ERROR_INVALID_UPSTREAM;

  if (self->thrd == NULL) {
    self->thrd = malloc(sizeof(pthread_t));
    if (self->thrd == NULL) {
      return -1;
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_create(self->thrd, &attr, rcl_fetcher_thread, self);
  }

  return 0;
}

#define BODY                                                      \
  "{\"id\":%" PRId32                                              \
  ",\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{" \
  "\"fromBlock\":\"0x%" PRIx64 "\",\"toBlock\":\"0x%" PRIx64 "\"}]}"

static size_t req_onsend(void* contents,
                         size_t size,
                         size_t nmemb,
                         void* userp) {
  req_t* response = userp;

  size_t chunksize = size * nmemb;
  size_t datasize = response->size + chunksize + 1;
  if (datasize > MAX_RESPONSE_SIZE) {
    rcl_error("too big response, size: %zu, chunksize: %zu\n", response->size,
              chunksize);
    return 0;
  }

  // TODO: allocate memory in blocks of multiples of the page size
  response->buffer = realloc(response->buffer, response->size + chunksize + 1);
  if (!response->buffer) {
    rcl_error("not enough memory\n");
    return 0;
  }

  rcl_memcpy(&(response->buffer[response->size]), contents, chunksize);
  response->size += chunksize;
  response->buffer[response->size] = 0;

  return chunksize;
}

static int req_parse(req_t* req, vector_t* logs) {
  json_error_t load_error;
  json_t* root = json_loads(req->buffer, 0, &load_error);

  if (!root) {
    rcl_error(
        "couldn't parse req; error: %s, req: "
        "%.*s\n",
        load_error.text, (int)req->size, req->buffer);
    return -1;
  }

  if (!json_is_object(root)) {
    rcl_error("root is not an object\n");
    goto error;
  }

  json_t* rid = json_object_get(root, "id");
  if (!json_is_integer(rid)) {
    rcl_error("'id' is not an integer\n");
    goto error;
  }

  json_t* error = json_object_get(root, "error");
  if (error != NULL) {
    if (json_is_string(error)) {
      rcl_error("RPC error: %s\n", json_string_value(error));
    } else if (json_is_object(error)) {
      json_t* msg = json_object_get(error, "message");
      json_t* code = json_object_get(error, "code");

      rcl_error("RPC error: [message] %s, [code] %lld\n",
                (json_is_string(msg) ? json_string_value(msg) : "unrecognized"),
                (json_is_integer(code) ? json_integer_value(code) : -1));
    } else {
      rcl_error("RPC error: unrecognized\n");
    }

    goto error;
  }

  json_t* result = json_object_get(root, "result");
  if (!json_is_array(result)) {
    rcl_error("result is not an array\n");
    goto error;
  }

  for (size_t i = 0, n = json_array_size(result); i < n; ++i) {
    json_t* item = json_array_get(result, i);
    if (!json_is_object(item)) {
      rcl_error("%zu item is not object\n", i);
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
        rcl_error("%zu item, block_number range error\n", i);
        goto error;
      }
    } else {
      rcl_error("%zu item, block_number is not a string\n", i);
      goto error;
    }

    json_t* address = json_object_get(item, "address");
    if (json_is_string(address)) {
      hex2bin(log->address, json_string_value(address), sizeof(rcl_address_t));
    } else {
      rcl_error("%zu item, address is not a string\n", i);
      goto error;
    }

    json_t* topics = json_object_get(item, "topics");
    if (json_is_array(topics)) {
      size_t topics_size = json_array_size(topics);
      if (topics_size > TOPICS_LENGTH) {
        rcl_error("%zu item, too many topics\n", i);
        goto error;
      }

      for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
        memset(log->topics[j], 0, sizeof(rcl_hash_t));

        if (j < topics_size) {
          json_t* topic = json_array_get(topics, j);
          if (!json_is_string(topic)) {
            rcl_error("%zu item, %zu topic is not a string\n", i, j);
            goto error;
          }

          hex2bin(log->topics[j], json_string_value(topic), sizeof(rcl_hash_t));
        }
      }
    } else {
      rcl_error("%zu item, topics is not an array\n", i);
      goto error;
    }
  }

  json_decref(root);
  return 0;

error:
  if (root)
    json_decref(root);
  return -1;
}

static int logscomp(const void* d1, const void* d2) {
  const rcl_log_t *arg1 = d1, *arg2 = d2;
  if (arg1->block_number < arg2->block_number)
    return -1;
  if (arg1->block_number > arg2->block_number)
    return 1;
  return 0;
}

static int req_process_segment(rcl_upstream_t* self,
                               CURLMsg* msg,
                               CURLM* multi) {
  if (msg->data.result != CURLE_OK) {
    rcl_error("curl_perform failed: %s\n",
              curl_easy_strerror(msg->data.result));
    return -1;
  }

  size_t idx = 0, found = false;
  for (; !found && idx < CONNECTIONS_COUNT; ++idx) {
    CURL* handle = *(CURL**)vector_at(&(self->handles), idx);
    if (handle == msg->easy_handle) {
      found = true;
      break;
    }
  }

  if (!found) {
    rcl_error("easy_handle not found\n");
    return -1;
  }

  req_t* req = vector_at(&(self->requests), idx);
  CURL* handle = *(CURL**)vector_at(&(self->handles), idx);

  curl_multi_remove_handle(multi, handle);

  uint64_t code = 0;
  if (curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &code) != CURLE_OK) {
    rcl_error("couldn't get response code\n");
    return -1;
  }

  if (code != 200) {
    rcl_error("server responded with code %ld\n", code);
    return -1;
  }

  req->ended = true;

  return 0;
}

static int req_next(rcl_upstream_t* self) {
  if (self->last >= self->height)
    return 0;

  uint64_t start = self->last == 0 ? 0 : self->last + 1;
  uint64_t available = self->height - self->last;

  // make new requests and add to pool
  size_t used = 0;
  for (; used < CONNECTIONS_COUNT && available > 0; ++used) {
    size_t count = min(BLOCKS_REQUEST_BATCH, available);
    available -= count;

    req_t* req = vector_at(&(self->requests), used);

    req->id = rand();
    req->from = start;
    req->to = start + count;
    req->size = 0;
    req->ended = false;

    start = req->to + 1;

    CURL* handle = *(CURL**)vector_at(&(self->handles), used);

    curl_easy_setopt(handle, CURLOPT_CURLU, self->url);
    curl_easy_setopt(handle, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, self->http_headers);

    snprintf(req->payload, TEXT_BUFFER_SIZE, BODY, req->id, req->from, req->to);
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, req->payload);
    curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, strlen(req->payload));

    curl_easy_setopt(handle, CURLOPT_WRITEDATA, (void*)req);
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, req_onsend);

    curl_multi_add_handle(self->multi_handle, handle);
  };

  // run all requests
  for (int still_running = 1; !self->closed && still_running;) {
    CURLMcode mc = curl_multi_perform(self->multi_handle, &still_running);

    if (still_running)
      mc = curl_multi_poll(self->multi_handle, NULL, 0, 1000, NULL);

    if (mc)
      break;
  }

  int msgs_left, rc;
  for (CURLMsg* msg; !self->closed;) {
    msg = curl_multi_info_read(self->multi_handle, &msgs_left);
    if (!msg) break;

    switch (msg->msg) {
      case CURLMSG_DONE:
        if ((rc = req_process_segment(self, msg, self->multi_handle)) != 0)
          return -1;
        break;

      default:
        rcl_error("invalid CURLMsg\n");
        return -1;
    }
  }

  if (self->closed) return 0;

  for (size_t i = 0; i < used; ++i) {
    req_t* req = vector_at(&(self->requests), i);
    vector_reset(&(req->logs));

    if (req_parse(req, &(req->logs)) != 0) {
      return -1;
    }

    vector_sort(&(req->logs),
                logscomp);  // TODO: create a sorted array in place

    int rc = self->callback(&(req->logs), self->callback_data);
    if (rc != 0) {
      rcl_error("couldn't insert logs; code: %d\n", rc);
      return -1;
    }

    rcl_debug("added %" PRIu64 " logs, last: %zu, height: %zu\n",
              req->logs.size, self->last, self->height);

    if (req->to > self->last)
      self->last = req->to;
  }

  return 0;
}

static void* rcl_fetcher_thread(void* data) {
  rcl_upstream_t* self = data;

  rcl_debug("start fetcher thread\n");

  while (!self->closed) {
    if (self->height == 0 || self->url == NULL) {
      rcl_debug("wait height and URL...\n");
      sleep(1);
      continue;
    }

    if (self->last >= self->height) {
      rcl_debug("nothing to download, height: %zu, last: %zu\n", self->height,
                self->last);
      sleep(1);
      continue;
    }

    int rc = req_next(self);
    if (rc != 0) {
      // There are retrays inside the fetcher
      // If there's a error, we take a break so we don't spam
      rcl_debug("delay for loader after error...");
      sleep(5);
    }
  }

  rcl_debug("exit fetcher thread\n");

  pthread_exit(0);
}
