#include "liboracle.h"

#include "common.h"
#include "file.h"
#include "upstream.h"
#include "vector.h"

enum { RCL_QUERY_SIZE_LIMIT = 4 * 1024 * 1024 };  // 4MB RAM

typedef char rcl_filepath_t[PATH_MAX + 1];

static int64_t LOGS_PAGE_CAPACITY = 1000000;   // 1m
static int64_t BLOCKS_FILE_CAPACITY = 100000;  // 100k

typedef int64_t rcl_cell_address_t;
typedef int64_t rcl_cell_topics_t[TOPICS_LENGTH];

// type: rcl_block_t
typedef struct {
  int64_t logs_count, offset;
  bloom_t logs_bloom;
} rcl_block_t;

static bool rcl_block_check(rcl_block_t* block, rcl_query_t* query) {
  size_t k, match;

  for (match = query->alen == 0, k = 0; !match && k < query->alen; ++k)
    match = bloom_check(&(block->logs_bloom), query->address[k]._data);
  if (!match)
    return false;

  for (size_t i = 0; i < TOPICS_LENGTH; ++i) {
    for (match = query->tlen[i] == 0, k = 0; !match && k < query->tlen[i]; ++k)
      match = bloom_check(&(block->logs_bloom), query->topics[i][k]._data);
    if (!match)
      return false;
  }

  return true;
}

// type: rcl_page_t
typedef struct {
  int64_t index;
  file_t addresses;  // rcl_cell_address_t*
  file_t topics;     // rcl_cell_topics_t*
} rcl_page_t;

#define file_as_blocks(p) ((rcl_block_t*)((p)->buffer))
#define file_as_addresses(p) ((rcl_cell_address_t*)((p).buffer))
#define file_as_topics(p) ((rcl_cell_topics_t*)((p).buffer))

static int rcl_page_filename(rcl_filepath_t filename,
                             const char* dirname,
                             int64_t index,
                             char part) {
  int count = snprintf(filename, PATH_MAX, "%s/%02" PRIx64 ".%c.rcl", dirname,
                       index, part);

  if (rcl_unlikely(count < 0 || count >= PATH_MAX)) {
    return -1;
  }

  return 0;
}

void rcl_page_destroy(rcl_page_t* page) {
  file_close(&(page->addresses));
  file_close(&(page->topics));
}

// type: rcl_t
static uint32_t HASH_SEED = 1907531730ul;

struct rcl {
  pthread_rwlock_t lock;

  // Config
  int64_t ram_limit;
  rcl_filepath_t dir;

  rcl_upstream_t* upstream;

  // DB state
  FILE* manifest;
  int64_t blocks_count, logs_count;

  // Data pages
  vector_t blocks_pages;  // <file_t>
  vector_t data_pages;    // <rcl_page_t>
};

static int rcl_open_blocks_page(rcl_t* self) {
  rcl_filepath_t filename = {0};

  int rc = rcl_page_filename(filename, self->dir, self->blocks_pages.size, 'b');
  if (rcl_unlikely(rc != 0)) {
    return -1;
  }

  file_t* file = (file_t*)vector_add(&(self->blocks_pages));
  if (rcl_unlikely(file == NULL)) {
    return -2;
  }

  size_t filesize = BLOCKS_FILE_CAPACITY * sizeof(rcl_block_t);
  rc = file_open(file, filename, filesize);
  if (rcl_unlikely(rc != 0)) {
    return -3;
  }

  int64_t locked_count = self->ram_limit / filesize;
  for (int64_t i = self->blocks_pages.size - 1; i >= 0; --i) {
    file_t* file = (file_t*)vector_at(&(self->blocks_pages), i);

    if (locked_count > 0) {
      locked_count--;
      if (file_lock(file))
        rcl_error("failed to lock the file with index\n");
    } else {
      if (file_unlock(file))
        rcl_error("failed to unlock the file with index\n");
    }
  }

  return 0;
}

static int rcl_open_data_page(rcl_t* self) {
  size_t index = self->data_pages.size;

  rcl_page_t* page = (rcl_page_t*)vector_add(&(self->data_pages));
  page->index = index;

  rcl_filepath_t addresses_file = {0}, topics_file = {0};

  if ((rcl_page_filename(addresses_file, self->dir, index, 'a') != 0) ||
      (rcl_page_filename(topics_file, self->dir, index, 't') != 0)) {
    return -1;
  }

  int ra = file_open(&(page->addresses), addresses_file,
                     LOGS_PAGE_CAPACITY * sizeof(rcl_cell_address_t));
  int rt = file_open(&(page->topics), topics_file,
                     LOGS_PAGE_CAPACITY * sizeof(rcl_cell_topics_t));
  if (ra != 0 || rt != 0)
    return -1;

  return 0;
}

static void get_position(int64_t target,
                         int64_t size,
                         int64_t* page,
                         int64_t* offset) {
  *page = 0;
  *offset = target;

  while (*offset > size - 1) {
    *offset -= size;
    ++(*page);
  }
}

static rcl_block_t* rcl_get_block(rcl_t* self, int64_t number) {
  int64_t page, offset;
  get_position(number, BLOCKS_FILE_CAPACITY, &page, &offset);

  file_t* file = (file_t*)vector_at(&(self->blocks_pages), page);
  return &(file_as_blocks(file)[offset]);
}

static int rcl_add_block(rcl_t* self, int64_t block_number) {
  for (; self->blocks_count <= block_number;) {
    int64_t page, offset;
    get_position(self->blocks_count, BLOCKS_FILE_CAPACITY, &page, &offset);

    if (self->blocks_pages.size <= page) {
      int status = rcl_open_blocks_page(self);
      if (rcl_unlikely(status != 0))
        return status;
    }

    file_t* file = vector_at(&(self->blocks_pages), page);

    rcl_block_t* blocks = file_as_blocks(file);
    blocks[offset].logs_count = 0;
    blocks[offset].offset = 0;
    bloom_init(blocks[offset].logs_bloom);

    if (self->blocks_count != 0) {
      rcl_block_t* last;

      if (offset == 0) {
        file_t* prev_file = vector_at(&(self->blocks_pages), page - 1);
        rcl_block_t* prev_blocks = file_as_blocks(prev_file);

        last = &(prev_blocks[BLOCKS_FILE_CAPACITY - 1]);
      } else {
        last = &(blocks[offset - 1]);
      }

      blocks[offset].offset = last->offset + last->logs_count;
    }

    ++self->blocks_count;
  }

  return 0;
}

static rcl_result rcl_state_read(rcl_t* t) {
  int err = fseek(t->manifest, 0, SEEK_SET);
  if (err != 0)
    return RCLE_FILESYSTEM;

  int count = fscanf(t->manifest, "%" PRId64 " %" PRId64 "", &t->blocks_count,
                     &t->logs_count);

  if (count != 2)
    return RCLE_FILESYSTEM;

  rcl_debug("readed state: blocks = %zu, logs = %zu\n", t->blocks_count,
            t->logs_count);

  return RCLE_OK;
}

static rcl_result rcl_state_write(rcl_t* t) {
  if (fseek(t->manifest, 0, SEEK_SET) != 0) {
    rcl_perror("state fseek");
    return RCLE_FILESYSTEM;
  }

  int bytes = fprintf(t->manifest, "%" PRId64 " %" PRId64 "", t->blocks_count,
                      t->logs_count);
  if (bytes <= 0)
    return RCLE_FILESYSTEM;

  rcl_debug("writed state: blocks = %zu, logs = %zu\n", t->blocks_count,
            t->logs_count);

  return RCLE_OK;
}

static rcl_result rcl_db_restore(rcl_t* self, const char* state_filename) {
  self->manifest = fopen(state_filename, "r+");
  if (rcl_unlikely(self->manifest == NULL)) {
    return RCLE_FILESYSTEM;
  }

  rcl_result rr = rcl_state_read(self);
  if (rr != RCLE_OK)
    return rr;

  // blocks pages
  int64_t blocks_pages_count = self->blocks_count / BLOCKS_FILE_CAPACITY;
  if (blocks_pages_count * BLOCKS_FILE_CAPACITY < self->blocks_count)
    blocks_pages_count++;

  if (!vector_init(&(self->blocks_pages), blocks_pages_count, sizeof(file_t)))
    return RCLE_UNKNOWN;

  for (int64_t i = 0; i < blocks_pages_count; ++i) {
    if (rcl_open_blocks_page(self) != 0)
      return RCLE_FILESYSTEM;
  }

  if (self->blocks_pages.size == 0)
    if (rcl_open_blocks_page(self) != 0)
      return RCLE_FILESYSTEM;

  // data pages
  int64_t logs_pages_count = self->logs_count / LOGS_PAGE_CAPACITY;
  if (logs_pages_count * LOGS_PAGE_CAPACITY < self->logs_count)
    logs_pages_count++;

  if (!vector_init(&(self->data_pages), logs_pages_count, sizeof(rcl_page_t)))
    return RCLE_UNKNOWN;

  size_t i = 0;
  do {
    if (rcl_open_data_page(self) != 0)
      return RCLE_FILESYSTEM;
  } while (++i < logs_pages_count);

  rcl_debug("restored db from \"%s\", %zu blocks_pages, %zu logs_pages\n",
            self->dir, blocks_pages_count, logs_pages_count);

  return RCLE_OK;
}

static rcl_result rcl_db_init(rcl_t* self, const char* state_filename) {
  self->manifest = fopen(state_filename, "w+");
  if (rcl_unlikely(self->manifest == NULL)) {
    return RCLE_UNKNOWN;
  }

  self->blocks_count = 0;
  self->logs_count = 0;

  if (!vector_init(&(self->blocks_pages), 1, sizeof(file_t)))
    return RCLE_UNKNOWN;
  if (rcl_open_blocks_page(self) != 0)
    return RCLE_FILESYSTEM;

  if (!vector_init(&(self->data_pages), 1, sizeof(rcl_page_t)))
    return RCLE_UNKNOWN;
  if (rcl_open_data_page(self) != 0)
    return RCLE_UNKNOWN;

  rcl_result wr = rcl_state_write(self);
  if (wr != RCLE_OK)
    return wr;

  rcl_debug("init new db in \"%s\"\n", self->dir);

  return RCLE_OK;
}

static rcl_result rcl_upstream_callback(vector_t* logs, void* data) {
  return rcl_insert((rcl_t*)data, logs->size, (rcl_log_t*)(logs->buffer));
}

rcl_result rcl_open(char* dir, int64_t ram_limit, rcl_t** db_ptr) {
  rcl_t* self = (rcl_t*)malloc(sizeof(rcl_t));
  if (self == NULL) {
    rcl_perror("malloc rcl_t");
    return RCLE_OUT_OF_MEMORY;
  }

  *db_ptr = self;

  if (pthread_rwlock_init(&(self->lock), NULL) != 0) {
    return RCLE_UNKNOWN;
  }

  self->ram_limit = ram_limit;

  if (realpath(dir, self->dir) == NULL) {
    rcl_perror("datadir's realpath");
    return RCLE_INVALID_DATADIR;
  }

  rcl_filepath_t state_filename = {0};
  int count = snprintf(state_filename, PATH_MAX, "%s/%s", self->dir, "toc.txt");
  if (rcl_unlikely(count < 0 || count >= PATH_MAX)) {
    return RCLE_UNKNOWN;
  }

  rcl_result result = RCLE_OK;
  if (access(state_filename, F_OK) == 0) {
    result = rcl_db_restore(self, state_filename);
  } else {
    result = rcl_db_init(self, state_filename);
  }

  if (result != RCLE_OK)
    return result;

  rcl_upstream_init(&(self->upstream),
                    self->blocks_count == 0 ? 0 : self->blocks_count - 1,
                    rcl_upstream_callback, self);

  return RCLE_OK;
}

void rcl_free(rcl_t* self) {
  rcl_upstream_free(self->upstream);

  pthread_rwlock_wrlock(&(self->lock));

  (void)rcl_state_write(self);

  if (fflush(self->manifest)) {
    rcl_perror("state fflush");
  }

  if (fclose(self->manifest)) {
    rcl_perror("fclose manifest");
  }

  for (int64_t i = 0; i < self->blocks_pages.size; ++i) {
    file_t* it = (file_t*)(vector_remove_last(&(self->blocks_pages)));
    file_close(it);
  }

  for (int64_t i = 0; i < self->data_pages.size; ++i) {
    rcl_page_t* it = (rcl_page_t*)(vector_remove_last(&(self->data_pages)));
    rcl_page_destroy(it);
  }

  vector_destroy(&(self->blocks_pages));
  vector_destroy(&(self->data_pages));

  pthread_rwlock_unlock(&(self->lock));
  pthread_rwlock_destroy(&(self->lock));

  free(self);
}

rcl_result rcl_update_height(rcl_t* self, int64_t height) {
  return rcl_upstream_set_height(self->upstream, height);
}

rcl_result rcl_set_upstream(rcl_t* self, const char* upstream) {
  return rcl_upstream_set_url(self->upstream, upstream);
}

rcl_result rcl_insert(rcl_t* self, size_t size, rcl_log_t* logs) {
  int rc;
  rcl_result result = RCLE_OK;

  if (size == 0)
    return RCLE_OK;

  if ((rc = pthread_rwlock_wrlock(&(self->lock)))) {
    rcl_error("failed lock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  for (rcl_log_t *log = logs, *end = logs + size; log != end;) {
    int64_t block_number = log->block_number;

    if (rcl_unlikely(self->blocks_count > block_number + 1)) {
      rcl_debug("add to old block, current: %zu, blocks count: %zu\n",
                block_number, self->blocks_count);
      result = RCLE_UNKNOWN;
      goto error;
    }

    if (block_number >= self->blocks_count) {
      if (rcl_add_block(self, block_number) != 0) {
        result = RCLE_UNKNOWN;
        goto error;
      }
    }

    rcl_block_t* block = rcl_get_block(self, block_number);
    assert(block != NULL);

    size_t count = 0;
    for (; log != end && log->block_number == block_number; ++log) {
      int64_t page, offset;
      get_position(self->logs_count + count, LOGS_PAGE_CAPACITY, &page,
                   &offset);

      if (self->data_pages.size <= page) {
        if (rcl_open_data_page(self) != 0) {
          result = RCLE_UNKNOWN;
          goto error;
        }
      }

      rcl_page_t* logs_page = vector_at(&(self->data_pages), page);
      assert(logs_page != NULL);

      bloom_add(&(block->logs_bloom), log->address);
      file_as_addresses(logs_page->addresses)[offset] =
          murmur64A(log->address, sizeof(rcl_address_t), HASH_SEED);

      for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
        bloom_add(&(block->logs_bloom), log->topics[j]);
        file_as_topics(logs_page->topics)[offset][j] =
            murmur64A(log->topics[j], sizeof(rcl_hash_t), HASH_SEED);
      }

      count++;
    }

    block->logs_count += count;
    self->logs_count += count;
  }

error:
  if ((rc = rcl_state_write(self)) != RCLE_OK)
    result = rc;

  if ((rc = pthread_rwlock_unlock(&(self->lock)))) {
    rcl_error("failed unlock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  return result;
}

// Let's select the query as a single block and clear it the same way.
// It is necessary to avoid memory fragmentation and also to limit the size of
// the query.
rcl_result rcl_query_alloc(rcl_query_t** query,
                           size_t alen,
                           size_t tlen[TOPICS_LENGTH]) {
  size_t bytes = sizeof(rcl_query_t) + sizeof(struct rcl_query_address) * alen;
  for (size_t i = 0; i < TOPICS_LENGTH; ++i)
    bytes += sizeof(struct rcl_query_topics[TOPICS_LENGTH]) * tlen[i];

  if (bytes > RCL_QUERY_SIZE_LIMIT)
    return RCLE_TOO_LARGE_QUERY;

  void* ptr = malloc(bytes);
  if (ptr == NULL) {
    rcl_perror("malloc rcl_query_t");
    return RCLE_OUT_OF_MEMORY;
  }

  *query = (void*)ptr;
  ptr += sizeof(rcl_query_t);

  (*query)->address = (void*)ptr;
  ptr += sizeof(struct rcl_query_address) * alen;

  for (size_t i = 0; i < TOPICS_LENGTH; ++i) {
    if (tlen[i] > 0) {
      (*query)->topics[i] = ptr;
      ptr += sizeof(struct rcl_query_topics[TOPICS_LENGTH]) * tlen[i];
    } else {
      (*query)->topics[i] = NULL;
    }
  }

  (*query)->alen = alen;
  memcpy((*query)->tlen, tlen, sizeof(size_t) * TOPICS_LENGTH);

  return RCLE_OK;
}

void rcl_query_free(rcl_query_t* query) {
  free((void*)query);
}

static bool rcl_query_check_data(rcl_t* self,
                                 rcl_query_t* q,
                                 size_t page,
                                 size_t offset) {
  size_t k;
  bool match;

  rcl_page_t* logs_page = vector_at(&(self->data_pages), page);

  int64_t address = file_as_addresses(logs_page->addresses)[offset];
  int64_t* topics = file_as_topics(logs_page->topics)[offset];

  for (match = q->alen == 0, k = 0; !match && k < q->alen; ++k)
    match = q->address[k]._hash == address;

  if (!match)
    return false;

  for (size_t i = 0; i < TOPICS_LENGTH; ++i) {
    for (match = q->tlen[i] == 0, k = 0; !match && k < q->tlen[i]; ++k)
      match = q->topics[i][k]._hash == topics[i];

    if (!match)
      return false;
  }

  return true;
}

rcl_result rcl_query(rcl_t* self, rcl_query_t* query, int64_t* result) {
  *result = 0;

  int rc;

  // pre-check
  if ((rc = pthread_rwlock_rdlock(&(self->lock)))) {
    rcl_error("failed lock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  int64_t blocks_count = self->blocks_count;
  int64_t logs_count = self->logs_count;

  if ((rc = pthread_rwlock_unlock(&(self->lock)))) {
    rcl_error("failed unlock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  if (blocks_count == 0 || logs_count == 0)
    return RCLE_OK;

  // fill
  query->_has_addresses = query->alen > 0;
  query->_has_topics = false;

  for (size_t i = 0; i < query->alen; ++i) {
    rc = hex2bin(query->address[i]._data, query->address[i].encoded,
                 sizeof(rcl_address_t));
    if (rc != 0)
      return RCLE_UNKNOWN;

    query->address[i]._hash =
        murmur64A(query->address[i]._data, sizeof(rcl_address_t), HASH_SEED);
  }

  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    query->tlen[i] = query->tlen[i];
    if (query->tlen[i] > 0)
      query->_has_topics = true;

    for (size_t j = 0; j < query->tlen[i]; ++j) {
      rc = hex2bin(query->topics[i][j]._data, query->topics[i][j].encoded,
                   sizeof(rcl_hash_t));
      if (rc != 0)
        return RCLE_UNKNOWN;

      query->topics[i][j]._hash =
          murmur64A(query->topics[i][j]._data, sizeof(rcl_hash_t), HASH_SEED);
    }
  }

  // calc
  if ((rc = pthread_rwlock_rdlock(&(self->lock)))) {
    rcl_error("failed lock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  int64_t start = query->from, end = query->to;
  if (end >= self->blocks_count)
    end = self->blocks_count - 1;

  for (size_t number = start; number <= end; ++number) {
    rcl_block_t* block = rcl_get_block(self, number);
    assert(block != NULL);

    if (query->limit > 0 && query->limit < *result) {
      return RCLE_QUERY_OVERFLOW;
    }

    if (!query->_has_addresses && !query->_has_topics) {
      *result += block->logs_count;
      continue;
    }

    if (block->logs_count == 0 || !rcl_block_check(block, query))
      continue;

    int64_t l = block->offset, r = block->offset + block->logs_count;
    for (; l < r; ++l) {
      int64_t page, offset;
      get_position(l, LOGS_PAGE_CAPACITY, &page, &offset);

      if (rcl_query_check_data(self, query, page, offset))
        ++(*result);
    }
  }

  if ((rc = pthread_rwlock_unlock(&(self->lock)))) {
    rcl_error("failed unlock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  return RCLE_OK;
}

rcl_result rcl_blocks_count(rcl_t* self, int64_t* result) {
  int rc;

  if ((rc = pthread_rwlock_rdlock(&(self->lock)))) {
    rcl_error("failed lock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  *result = self->blocks_count;

  if ((rc = pthread_rwlock_unlock(&(self->lock)))) {
    rcl_error("failed unlock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  return RCLE_OK;
}

rcl_result rcl_logs_count(rcl_t* self, int64_t* result) {
  int rc;

  if ((rc = pthread_rwlock_rdlock(&(self->lock)))) {
    rcl_error("failed lock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  *result = self->logs_count;

  if ((rc = pthread_rwlock_unlock(&(self->lock)))) {
    rcl_error("failed unlock mutex, code %i\n", rc);
    return RCLE_UNKNOWN;
  }

  return RCLE_OK;
}
