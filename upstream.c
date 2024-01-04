#include "upstream.h"
#include <cjson/cJSON.h>

enum {
  CONNECTIONS_COUNT = 32,
  BLOCKS_REQUEST_BATCH = 128,
};

enum req_state { available, sent, received };

typedef struct {
  uint32_t id;
  uint64_t from, to;
  enum req_state state;

  CURL* handle;

  size_t response_size;
  char request[TEXT_BUFFER_SIZE + 1], *response;

  vector_t logs;  // rcl_log_t
} req_t;

static void* rcl_upstream_thrd(void* data);
static size_t req_onsend(void* contents,
                         size_t size,
                         size_t nmemb,
                         void* userp);

#define rcl_request_at(self, i)          \
  (req_t*)vector_at(&((self)->requests), \
                    ((self)->requests_head + (i)) % CONNECTIONS_COUNT)

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

  self->requests_head = 0;
  vector_init(&(self->requests), CONNECTIONS_COUNT, sizeof(req_t));
  for (size_t i = 0; i < CONNECTIONS_COUNT; ++i) {
    req_t* req = vector_add(&(self->requests));
    req->id = 0;
    req->from = 0;
    req->to = 0;
    req->response = NULL;
    req->response_size = 0;
    req->state = available;

    vector_init(&(req->logs), 16, sizeof(rcl_log_t));

    if ((req->handle = curl_easy_init()) == NULL)
      return -1;
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
    req_t* req = vector_at(&(self->requests), i);
    if (req->response)
      free(req->response);

    vector_destroy(&(req->logs));

    curl_easy_cleanup(req->handle);
  }
  vector_destroy(&(self->requests));

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
    return -1;

  if (self->thrd == NULL) {
    self->thrd = malloc(sizeof(pthread_t));
    if (self->thrd == NULL) {
      return -1;
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_create(self->thrd, &attr, rcl_upstream_thrd, self);
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
  req_t* req = userp;

  size_t chunksize = size * nmemb;
  size_t datasize = req->response_size + chunksize + 1;
  if (datasize > MAX_RESPONSE_BYTES) {
    rcl_error("too big response, size: %zu, chunksize: %zu\n",
              req->response_size, chunksize);
    return 0;
  }

  // TODO: allocate memory in blocks of multiples of the page size
  req->response = realloc(req->response, req->response_size + chunksize + 1);
  if (!req->response) {
    rcl_error("not enough memory\n");
    return 0;
  }

  rcl_memcpy(&(req->response[req->response_size]), contents, chunksize);
  req->response_size += chunksize;
  req->response[req->response_size] = 0;

  return chunksize;
}

static int req_parse(req_t* req, vector_t* logs) {
  cJSON* root = cJSON_ParseWithLength(req->response, req->response_size);
  if (root == NULL) {
    const char* error_ptr = cJSON_GetErrorPtr();
    if (error_ptr == NULL)
      error_ptr = "unrecognized";

    rcl_error("couldn't parse req, error: %s, req: %.*s\n", error_ptr,
              req->response_size, req->response);
    return -1;
  }

  if (rcl_unlikely(!cJSON_IsObject(root))) {
    rcl_error("root is not an object\n");
    goto error;
  }

  const cJSON* error = cJSON_GetObjectItemCaseSensitive(root, "error");
  if (error != NULL) {
    if (cJSON_IsString(error)) {
      rcl_error("RPC error: %s\n", error->valuestring);
    } else if (cJSON_IsObject(error)) {
      const cJSON* msg = cJSON_GetObjectItemCaseSensitive(error, "message");
      const cJSON* code = cJSON_GetObjectItemCaseSensitive(error, "code");

      rcl_error("RPC error: [message] %s, [code] %i\n",
                (cJSON_IsString(msg) ? msg->valuestring : "unrecognized"),
                (cJSON_IsNumber(code) ? code->valueint : -1));
    } else {
      rcl_error("RPC error: unrecognized\n");
    }

    goto error;
  }

  const cJSON* rid = cJSON_GetObjectItemCaseSensitive(root, "id");
  if (rcl_unlikely(!cJSON_IsNumber(rid))) {
    rcl_error("'id' is not an integer\n");
    goto error;
  }

  const cJSON* result = cJSON_GetObjectItemCaseSensitive(root, "result");
  if (rcl_unlikely(!cJSON_IsArray(result))) {
    rcl_error("result is not an array\n");
    goto error;
  }

  const cJSON* item = NULL;
  cJSON_ArrayForEach(item, result) {
    if (rcl_unlikely(!cJSON_IsObject(item))) {
      rcl_error("logs item is not object\n");
      goto error;
    }

    rcl_log_t* log = (rcl_log_t*)vector_add(logs);

    const cJSON* block_number =
        cJSON_GetObjectItemCaseSensitive(item, "blockNumber");
    if (rcl_unlikely(!cJSON_IsString(block_number) ||
                     block_number->valuestring == NULL)) {
      rcl_error("logs item, block_number is not a string\n");
      goto error;
    }

    const char* start = block_number->valuestring;
    char* end = NULL;

    errno = 0;
    log->block_number = (uint64_t)strtoll(start, &end, 16);
    if (rcl_unlikely(errno == ERANGE)) {
      rcl_error("logs item, block_number range error\n");
      goto error;
    }

    const cJSON* address = cJSON_GetObjectItemCaseSensitive(item, "address");
    if (rcl_unlikely(!cJSON_IsString(address) ||
                     address->valuestring == NULL)) {
      rcl_error("logs item, address is not a string\n");
      goto error;
    }

    hex2bin(log->address, address->valuestring, sizeof(rcl_address_t));

    const cJSON* topics = cJSON_GetObjectItemCaseSensitive(item, "topics");
    if (rcl_unlikely(!cJSON_IsArray(topics))) {
      rcl_error("item, topics is not an array\n");
      goto error;
    }

    size_t topics_size = cJSON_GetArraySize(topics);
    if (rcl_unlikely(topics_size > TOPICS_LENGTH)) {
      rcl_error("logs item, too many topics\n");
      goto error;
    }

    memset(log->topics, 0, sizeof(rcl_hash_t) * TOPICS_LENGTH);

    size_t j = 0;
    const cJSON* topic = NULL;
    cJSON_ArrayForEach(topic, topics) {
      if (rcl_unlikely(!cJSON_IsString(topic) || topic->valuestring == NULL)) {
        rcl_error("item, %zu topic is not a string\n", j);
        goto error;
      }

      hex2bin(log->topics[j++], topic->valuestring, sizeof(rcl_hash_t));
    }
  }

  cJSON_Delete(root);
  return 0;

error:
  if (root)
    cJSON_Delete(root);
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

static int req_process(rcl_upstream_t* self, CURLMsg* msg) {
  if (rcl_unlikely(msg->data.result != CURLE_OK)) {
    rcl_error("curl_perform failed: %s\n",
              curl_easy_strerror(msg->data.result));
    return -1;
  }

  req_t* req = NULL;
  for (size_t i = 0; i < CONNECTIONS_COUNT; ++i) {
    req_t* it = vector_at(&(self->requests), i);
    if (it->handle == msg->easy_handle) {
      req = it;
      break;
    }
  }

  if (rcl_unlikely(req == NULL)) {
    rcl_error("easy_handle not found\n");
    return -1;
  }

  curl_multi_remove_handle(self->multi_handle, req->handle);

  long code = 0;
  if (curl_easy_getinfo(req->handle, CURLINFO_RESPONSE_CODE, &code) !=
      CURLE_OK) {
    rcl_error("couldn't get response code\n");
    return -1;
  }

  if (code != 200) {
    rcl_error("server responded with code %ld\n", code);
    return -1;
  }

  vector_reset(&(req->logs));

  if (req_parse(req, &(req->logs)) != 0) {
    rcl_error("failed to parse the request\n");
    return -1;
  }

  // TODO: create a sorted array in place
  vector_sort(&(req->logs), logscomp);

  req->state = received;

  return 0;
}

static int rcl_upstream_send(rcl_upstream_t* self, uint64_t* start) {
  size_t i = 0;
  for (; i < CONNECTIONS_COUNT; ++i) {
    if ((rcl_request_at(self, i))->state == available)
      break;
  }

  for (; i < CONNECTIONS_COUNT && !self->closed; ++i) {
    req_t* req = rcl_request_at(self, i);

    if (req->state != available) {
      rcl_error("trying to send a request that has already been sent\n");
      return -1;
    }

    size_t count = min(BLOCKS_REQUEST_BATCH, self->height - *start);

    req->id = rand();
    req->from = *start;
    req->to = *start + count;
    req->response_size = 0;
    req->state = sent;

    *start += count + 1;

    if (i % 2 == 0) {
      if (curl_easy_setopt(req->handle, CURLOPT_CURLU, self->url))
        return -1;
    } else {
      if (curl_easy_setopt(req->handle, CURLOPT_CURLU, self->url))
        return -1;
    }

    if (curl_easy_setopt(req->handle, CURLOPT_ACCEPT_ENCODING, ""))
      return -1;

    if (curl_easy_setopt(req->handle, CURLOPT_HTTPHEADER, self->http_headers))
      return -1;

    snprintf(req->request, TEXT_BUFFER_SIZE, BODY, req->id, req->from, req->to);
    if (curl_easy_setopt(req->handle, CURLOPT_POSTFIELDS, req->request))
      return -1;

    if (curl_easy_setopt(req->handle, CURLOPT_POSTFIELDSIZE,
                         strlen(req->request)))
      return -1;

    if (curl_easy_setopt(req->handle, CURLOPT_WRITEDATA, (void*)req))
      return -1;
    if (curl_easy_setopt(req->handle, CURLOPT_WRITEFUNCTION, req_onsend))
      return -1;

    if (curl_multi_add_handle(self->multi_handle, req->handle))
      return -1;
  }

  return 0;
}

static int rcl_upstream_receive(rcl_upstream_t* self) {
  int numfds;
  CURLMsg* msg;

  while ((msg = curl_multi_info_read(self->multi_handle, &numfds))) {
    if (self->closed)
      break;

    switch (msg->msg) {
      case CURLMSG_DONE:
        if (req_process(self, msg) != 0)
          return -1;
        break;

      default:
        rcl_error("invalid CURLMsg\n");
        return -1;
    }
  }

  return 0;
}

static int rcl_upstream_process(rcl_upstream_t* self, bool exact) {
  int rc;

  for (size_t i = 0; i < CONNECTIONS_COUNT && !self->closed; ++i) {
    req_t* req = vector_at(&(self->requests), self->requests_head);

    switch (req->state) {
      case available:
        return 0;

      case sent:
        if (exact) {
          rcl_error("completed request not parsed\n");
          return -1;
        }

        return 0;

      case received:
        rc = self->callback(&(req->logs), self->callback_data);
        if (rc != 0) {
          rcl_error("couldn't insert logs, code: %d\n", rc);
          return -1;
        }

        req->state = available;
        self->requests_head = (self->requests_head + 1) % CONNECTIONS_COUNT;

        if (req->to > self->last)
          self->last = req->to;

        rcl_debug("added %zu logs from: %zu to %zu, last: %zu, height: %zu\n",
                  req->logs.size, req->from, req->to, self->last, self->height);

        break;

      default:
        rcl_error("invalid request state\n");
        rcl_unreachable();
    }
  }

  return 0;
}

static int rcl_upstream_poll(rcl_upstream_t* self) {
  uint64_t start = self->last == 0 ? 0 : self->last + 1;

  int rc, still_running, numfds;
  do {
    if (self->closed || start > self->height)
      break;

    if ((rc = rcl_upstream_send(self, &start)) != 0) {
      rcl_error("couldn't send requests, code: %d\n", rc);
      return -1;
    }

    CURLMcode mc = curl_multi_perform(self->multi_handle, &still_running);
    if (mc != CURLM_OK) {
      rcl_error("curl_multi_perform failed: '%s'\n", curl_multi_strerror(mc));
      return -1;
    }

    mc = curl_multi_poll(self->multi_handle, NULL, 0, 1000, &numfds);
    if (mc != CURLM_OK) {
      rcl_error("curl_multi_pool failed: '%s'\n", curl_multi_strerror(mc));
      return -1;
    }

    if (rcl_upstream_receive(self) != 0)
      return -1;

    if (rcl_upstream_process(self, false) != 0)
      return -1;
  } while (still_running);

  if (self->closed)
    return 0;

  if (rcl_upstream_receive(self) != 0)
    return -1;

  if (rcl_upstream_process(self, true) != 0)
    return -1;

  return 0;
}

static void* rcl_upstream_thrd(void* data) {
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

    int rc = rcl_upstream_poll(self);
    if (rc != 0) {
      // There are retrays inside the fetcher
      // If there's a error, we take a break so we don't spam
      rcl_debug("delay for loader after error...\n");
      sleep(5);
    }
  }

  rcl_debug("exit fetcher thread\n");

  pthread_exit(0);
}
