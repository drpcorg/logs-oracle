#include "liboracle.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "file.h"
#include "upstream.h"
#include "vector.h"

enum { RCL_QUERY_SIZE_LIMIT = 4 * 1024 * 1024 };  // 4MB RAM for query

static uint64_t LOGS_PAGE_CAPACITY = 1000000;   // 1m
static uint64_t BLOCKS_FILE_CAPACITY = 100000;  // 100k

typedef uint64_t rcl_cell_address_t;
typedef uint64_t rcl_cell_topics_t[TOPICS_LENGTH];

// type: rcl_block_t
typedef struct {
  uint64_t logs_count, offset;
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
  uint64_t index;

  file_t addresses;  // rcl_cell_address_t*
  file_t topics;     // rcl_cell_topics_t*
} rcl_page_t;

#define file_as_blocks(p) ((rcl_block_t*)((p)->buffer))
#define file_as_addresses(p) ((rcl_cell_address_t*)((p).buffer))
#define file_as_topics(p) ((rcl_cell_topics_t*)((p).buffer))

static int rcl_page_filename(rcl_filepath_t filename,
                             const char* dirname,
                             uint64_t index,
                             char part) {
  int count = snprintf(filename, MAX_FILE_LENGTH, "%s/%02" PRIx64 ".%c.rcl",
                       dirname, index, part);

  if (rcl_unlikely(count < 0 || count >= MAX_FILE_LENGTH)) {
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

struct db {
  pthread_rwlock_t lock;

  // Config
  uint64_t ram_limit;
  rcl_filepath_t dir;

  rcl_upstream_t upstream;

  // DB state
  FILE* manifest;
  uint64_t blocks_count, logs_count;

  // Data pages
  vector_t blocks_pages;  // <file_t>
  vector_t data_pages;    // <rcl_page_t>
};

static int rcl_open_blocks_page(rcl_t* db) {
  rcl_filepath_t filename = {0};

  int rc = rcl_page_filename(filename, db->dir, db->blocks_pages.size, 'b');
  if (rcl_unlikely(rc != 0)) {
    return -1;
  }

  file_t* file = (file_t*)vector_add(&(db->blocks_pages));
  if (rcl_unlikely(file == NULL)) {
    return -2;
  }

  rc = file_open(file, filename, BLOCKS_FILE_CAPACITY * sizeof(rcl_block_t));
  if (rcl_unlikely(rc != 0)) {
    return -3;
  }

  return 0;
}

static int rcl_open_data_page(rcl_t* db) {
  size_t index = db->data_pages.size;

  rcl_page_t* page = (rcl_page_t*)vector_add(&(db->data_pages));
  page->index = index;

  rcl_filepath_t addresses_file = {0}, topics_file = {0};

  if ((rcl_page_filename(addresses_file, db->dir, index, 'a') != 0) ||
      (rcl_page_filename(topics_file, db->dir, index, 't') != 0)) {
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

static void get_position(uint64_t target,
                         uint64_t size,
                         uint64_t* page,
                         uint64_t* offset) {
  *page = 0;
  *offset = target;

  while (*offset > size - 1) {
    *offset -= size;
    ++(*page);
  }
}

static rcl_block_t* rcl_get_block(rcl_t* db, uint64_t number) {
  uint64_t page, offset;
  get_position(number, BLOCKS_FILE_CAPACITY, &page, &offset);

  file_t* file = (file_t*)vector_at(&(db->blocks_pages), page);
  return &(file_as_blocks(file)[offset]);
}

static int rcl_add_block(rcl_t* db, uint64_t block_number) {
  for (; db->blocks_count <= block_number;) {
    uint64_t page, offset;
    get_position(db->blocks_count, BLOCKS_FILE_CAPACITY, &page, &offset);

    if (db->blocks_pages.size <= page) {
      int status = rcl_open_blocks_page(db);
      if (rcl_unlikely(status != 0))
        return status;
    }

    file_t* file = vector_at(&(db->blocks_pages), page);

    rcl_block_t* blocks = file_as_blocks(file);
    blocks[offset].logs_count = 0;
    blocks[offset].offset = 0;
    bloom_init(blocks[offset].logs_bloom);

    if (db->blocks_count != 0) {
      rcl_block_t* last;

      if (offset == 0) {
        file_t* prev_file = vector_at(&(db->blocks_pages), page - 1);
        rcl_block_t* prev_blocks = file_as_blocks(prev_file);

        last = &(prev_blocks[BLOCKS_FILE_CAPACITY - 1]);
      } else {
        last = &(blocks[offset - 1]);
      }

      blocks[offset].offset = last->offset + last->logs_count;
    }

    ++db->blocks_count;
  }

  return 0;
}

static rcl_result rcl_state_read(rcl_t* t) {
  int err = fseek(t->manifest, 0, SEEK_SET);
  if (err != 0)
    return RCL_ERROR_FS_IO;

  int count = fscanf(t->manifest, "%" PRIu64 " %" PRIu64 "", &t->blocks_count,
                     &t->logs_count);

  if (count != 2)
    return RCL_ERROR_FS_IO;

  rcl_debug("readed state: blocks = %zu, logs = %zu\n", t->blocks_count,
            t->logs_count);

  return RCL_SUCCESS;
}

static rcl_result rcl_state_write(rcl_t* t) {
  if (fseek(t->manifest, 0, SEEK_SET) != 0)
    return RCL_ERROR_FS_IO;

  int bytes = fprintf(t->manifest, "%" PRIu64 " %" PRIu64 "", t->blocks_count,
                      t->logs_count);
  if (bytes <= 0)
    return RCL_ERROR_FS_IO;

  if (fflush(t->manifest) != 0)
    return RCL_ERROR_FS_IO;

  rcl_debug("writed state: blocks = %zu, logs = %zu\n", t->blocks_count,
            t->logs_count);

  return RCL_SUCCESS;
}

static rcl_result rcl_db_restore(rcl_t* db, const char* state_filename) {
  db->manifest = fopen(state_filename, "r+");
  if (rcl_unlikely(db->manifest == NULL)) {
    return RCL_ERROR_FS_IO;
  }

  rcl_result rr = rcl_state_read(db);
  if (rr != RCL_SUCCESS)
    return rr;

  // blocks pages
  uint64_t blocks_pages_count = db->blocks_count / BLOCKS_FILE_CAPACITY;
  if (blocks_pages_count * BLOCKS_FILE_CAPACITY < db->blocks_count)
    blocks_pages_count++;

  if (!vector_init(&(db->blocks_pages), blocks_pages_count, sizeof(file_t)))
    return RCL_ERROR_UNKNOWN;

  for (uint64_t i = 0; i < blocks_pages_count; ++i) {
    if (rcl_open_blocks_page(db) != 0)
      return RCL_ERROR_FS_IO;
  }

  if (db->blocks_pages.size == 0)
    if (rcl_open_blocks_page(db) != 0)
      return RCL_ERROR_FS_IO;

  // data pages
  uint64_t logs_pages_count = db->logs_count / LOGS_PAGE_CAPACITY;
  if (logs_pages_count * LOGS_PAGE_CAPACITY < db->logs_count)
    logs_pages_count++;

  if (!vector_init(&(db->data_pages), logs_pages_count, sizeof(rcl_page_t)))
    return RCL_ERROR_UNKNOWN;

  size_t i = 0;
  do {
    if (rcl_open_data_page(db) != 0)
      return RCL_ERROR_FS_IO;
  } while (++i < logs_pages_count);

  rcl_debug("restored new db from \"%s\", %zu blocks_pages, %zu logs_pages\n",
            db->dir, blocks_pages_count, logs_pages_count);

  return RCL_SUCCESS;
}

static rcl_result rcl_db_init(rcl_t* db, const char* state_filename) {
  db->manifest = fopen(state_filename, "w+");
  if (rcl_unlikely(db->manifest == NULL)) {
    return RCL_ERROR_UNKNOWN;
  }

  db->blocks_count = 0;
  db->logs_count = 0;

  if (!vector_init(&(db->blocks_pages), 1, sizeof(file_t)))
    return RCL_ERROR_UNKNOWN;
  if (rcl_open_blocks_page(db) != 0)
    return RCL_ERROR_FS_IO;

  if (!vector_init(&(db->data_pages), 1, sizeof(rcl_page_t)))
    return RCL_ERROR_UNKNOWN;
  if (rcl_open_data_page(db) != 0)
    return RCL_ERROR_UNKNOWN;

  rcl_result wr = rcl_state_write(db);
  if (wr != RCL_SUCCESS)
    return wr;

  rcl_debug("init new db in \"%s\"\n", db->dir);

  return RCL_SUCCESS;
}

static int rcl_upstream_callback(vector_t* logs, void* data) {
  rcl_t* db = data;

  int rc = rcl_insert(db, logs->size, (rcl_log_t*)(logs->buffer));
  if (rc != RCL_SUCCESS) {
    rcl_error("couldn't to insert fetched logs; code %d\n", rc);
    return -1;
  }

  return 0;
}

rcl_result rcl_open(char* dir, uint64_t ram_limit, rcl_t** db_ptr) {
  if (curl_global_init(CURL_GLOBAL_DEFAULT) != 0) {
    return RCL_ERROR_UNKNOWN;
  }

  rcl_t* db = (rcl_t*)malloc(sizeof(rcl_t));
  if (db == NULL) {
    return RCL_ERROR_MEMORY_ALLOCATION;
  }

  *db_ptr = db;

  if (pthread_rwlock_init(&(db->lock), NULL) != 0) {
    return RCL_ERROR_UNKNOWN;
  }

  db->ram_limit = ram_limit;
  strncpy(db->dir, dir, MAX_FILE_LENGTH);

  rcl_filepath_t state_filename = {0};
  int count =
      snprintf(state_filename, MAX_FILE_LENGTH, "%s/%s", db->dir, "toc.txt");
  if (rcl_unlikely(count < 0 || count >= MAX_FILE_LENGTH)) {
    return RCL_ERROR_UNKNOWN;
  }

  rcl_result result = RCL_SUCCESS;
  if (access(state_filename, F_OK) == 0) {
    result = rcl_db_restore(db, state_filename);
  } else {
    result = rcl_db_init(db, state_filename);
  }

  if (result != RCL_SUCCESS)
    return result;

  rcl_upstream_init(&(db->upstream),
                    db->blocks_count == 0 ? 0 : db->blocks_count - 1,
                    rcl_upstream_callback, db);

  return RCL_SUCCESS;
}

void rcl_free(rcl_t* db) {
  // Clear db
  pthread_rwlock_wrlock(&(db->lock));

  rcl_upstream_free(&(db->upstream));

  rcl_result _ = rcl_state_write(db);
  fclose(db->manifest);

  for (uint64_t i = 0; i < db->blocks_pages.size; ++i) {
    file_t* it = (file_t*)(vector_remove_last(&(db->blocks_pages)));
    file_close(it);
  }

  for (uint64_t i = 0; i < db->data_pages.size; ++i) {
    rcl_page_t* it = (rcl_page_t*)(vector_remove_last(&(db->data_pages)));
    rcl_page_destroy(it);
  }

  vector_destroy(&(db->blocks_pages));
  vector_destroy(&(db->data_pages));

  pthread_rwlock_unlock(&(db->lock));
  pthread_rwlock_destroy(&(db->lock));

  free(db);

  // Clear global
  curl_global_cleanup();
}

rcl_result rcl_update_height(rcl_t* db, uint64_t height) {
  rcl_result result = RCL_SUCCESS;

  pthread_rwlock_wrlock(&(db->lock));
  rcl_upstream_set_height(&(db->upstream), height);
  result = rcl_state_write(db);
  pthread_rwlock_unlock(&(db->lock));

  return result;
}

static void* rcl_fetcher_thread(void* data);

rcl_result rcl_set_upstream(rcl_t* db, const char* upstream) {
  size_t n = strlen(upstream);
  if (n > UPSTREAM_LIMIT)
    return RCL_ERROR_INVALID_UPSTREAM;

  if (rcl_upstream_set_url(&(db->upstream), upstream) == 0)
    return RCL_ERROR_UNKNOWN;

  return RCL_SUCCESS;
}

rcl_result rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs) {
  rcl_result result = RCL_SUCCESS;

  if (size == 0)
    return RCL_SUCCESS;

  pthread_rwlock_wrlock(&(db->lock));

  for (rcl_log_t *log = logs, *end = logs + size; log != end; ++log) {
    if (rcl_unlikely(db->blocks_count > log->block_number + 1)) {
      result = RCL_ERROR_INSERT_LOGS_TO_OLD_BLOCK;
      break;
    }

    if (log->block_number >= db->blocks_count) {
      if (rcl_add_block(db, log->block_number) != 0) {
        result = RCL_ERROR_UNKNOWN;
        break;
      }
    }

    uint64_t page, offset;
    get_position(db->logs_count, LOGS_PAGE_CAPACITY, &page, &offset);

    if (db->data_pages.size <= page) {
      if (rcl_open_data_page(db) != 0) {
        result = RCL_ERROR_UNKNOWN;
        break;
      }
    }

    // insert logs
    rcl_block_t* block = rcl_get_block(db, log->block_number);
    assert(block != NULL);

    rcl_page_t* logs_page = vector_at(&(db->data_pages), page);
    assert(logs_page != NULL);

    bloom_add(&(block->logs_bloom), log->address);
    file_as_addresses(logs_page->addresses)[offset] =
        murmur64A(log->address, sizeof(rcl_address_t), HASH_SEED);

    for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
      bloom_add(&(block->logs_bloom), log->topics[j]);
      file_as_topics(logs_page->topics)[offset][j] =
          murmur64A(log->topics[j], sizeof(rcl_hash_t), HASH_SEED);
    }

    block->logs_count++;
    db->logs_count++;
  }

  int wr = rcl_state_write(db);
  if (wr != RCL_SUCCESS)
    result = wr;

  pthread_rwlock_unlock(&(db->lock));
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
    return RCL_ERROR_TOO_BIG_QUERY;

  void* ptr = malloc(bytes);
  if (ptr == NULL)
    return RCL_ERROR_MEMORY_ALLOCATION;

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

  return RCL_SUCCESS;
}

void rcl_query_free(rcl_query_t* query) {
  free((void*)query);
}

static bool rcl_query_check_data(rcl_t* db,
                                 rcl_query_t* q,
                                 size_t page,
                                 size_t offset) {
  size_t k;
  bool match;

  rcl_page_t* logs_page = vector_at(&(db->data_pages), page);

  uint64_t address = file_as_addresses(logs_page->addresses)[offset];
  uint64_t* topics = file_as_topics(logs_page->topics)[offset];

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

rcl_result rcl_query(rcl_t* db, rcl_query_t* query, uint64_t* result) {
  *result = 0;

  // pre-check
  pthread_rwlock_rdlock(&(db->lock));
  uint64_t blocks_count = db->blocks_count;
  uint64_t logs_count = db->logs_count;
  pthread_rwlock_unlock(&(db->lock));

  if (blocks_count == 0 || logs_count == 0)
    return RCL_SUCCESS;

  // fill
  int rc;

  query->_has_addresses = query->alen > 0;
  query->_has_topics = false;

  for (size_t i = 0; i < query->alen; ++i) {
    rc = hex2bin(query->address[i]._data, query->address[i].encoded,
                 sizeof(rcl_address_t));
    if (rc != 0)
      return RCL_ERROR_UNKNOWN;

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
        return RCL_ERROR_UNKNOWN;

      query->topics[i][j]._hash =
          murmur64A(query->topics[i][j]._data, sizeof(rcl_hash_t), HASH_SEED);
    }
  }

  // calc
  pthread_rwlock_rdlock(&(db->lock));
  uint64_t start = query->from, end = query->to;
  if (end >= db->blocks_count)
    end = db->blocks_count - 1;

  for (size_t number = start; number <= end; ++number) {
    rcl_block_t* block = rcl_get_block(db, number);
    assert(block != NULL);

    if (!query->_has_addresses && !query->_has_topics) {
      *result += block->logs_count;
      continue;
    }

    if (block->logs_count == 0 || !rcl_block_check(block, query))
      continue;

    uint64_t l = block->offset, r = block->offset + block->logs_count;
    for (; l < r; ++l) {
      uint64_t page, offset;
      get_position(l, LOGS_PAGE_CAPACITY, &page, &offset);

      if (rcl_query_check_data(db, query, page, offset))
        ++(*result);
    }
  }
  pthread_rwlock_unlock(&(db->lock));

  return RCL_SUCCESS;
}

rcl_result rcl_blocks_count(rcl_t* db, uint64_t* result) {
  pthread_rwlock_rdlock(&(db->lock));
  *result = db->blocks_count;
  pthread_rwlock_unlock(&(db->lock));

  return RCL_SUCCESS;
}

rcl_result rcl_logs_count(rcl_t* db, uint64_t* result) {
  pthread_rwlock_rdlock(&(db->lock));
  *result = db->logs_count;
  pthread_rwlock_unlock(&(db->lock));

  return RCL_SUCCESS;
}
