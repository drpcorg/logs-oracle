#include "liboracle.h"

#include "common.h"
#include "bloom.h"
#include "file.h"
#include "vector.h"
#include "loader.h"

#define UPSTREAM_LIMIT 4096

static uint64_t LOGS_PAGE_CAPACITY = 1000000;   // 1m
static uint64_t BLOCKS_FILE_CAPACITY = 100000;  // 100k

typedef uint64_t rcl_cell_address_t;
typedef uint64_t rcl_cell_topics_t[TOPICS_LENGTH];

typedef struct {
  uint64_t logs_count, offset;
  bloom_t logs_bloom;
} rcl_block_t;

static bool rcl_block_check(rcl_block_t* block, rcl_query_t* query) {
  if (query->addresses.len > 0) {
    bool has = false;

    for (size_t i = 0; i < query->addresses.len; ++i) {
      if (bloom_check(&(block->logs_bloom), query->addresses.data[i])) {
        has = true;
        break;
      }
    }

    if (!has)
      return false;
  }

  for (size_t i = 0; i < TOPICS_LENGTH; ++i) {
    if (query->topics[i].len == 0)
      continue;

    bool current_has = false;

    for (size_t j = 0; j < query->topics[i].len; ++j)
      if (bloom_check(&(block->logs_bloom), query->topics[i].data[j])) {
        current_has = true;
        break;
      }

    if (!current_has)
      return false;
  }

  return true;
}

typedef struct {
  uint64_t index;

  file_t addresses;  // rcl_cell_address_t*
  file_t topics;     // rcl_cell_topics_t*
} rcl_page_t;

#define file_as_blocks(p) ((rcl_block_t*)((p)->buffer))
#define file_as_addresses(p) ((rcl_cell_address_t*)((p)->buffer))
#define file_as_topics(p) ((rcl_cell_topics_t*)((p)->buffer))

static int rcl_page_filename(rcl_filename_t filename,
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

int rcl_page_init(rcl_page_t* page, const char* dirname, uint64_t index) {
  rcl_filename_t addresses_file = {0}, topics_file = {0};

  page->index = index;

  if ((rcl_page_filename(addresses_file, dirname, index, 'a') != 0) ||
      (rcl_page_filename(topics_file, dirname, index, 't') != 0)) {
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
  rcl_filename_t dir;

  bool closed;
  char* upstream;
  pthread_t* fetcher_thread;

  // DB state
  FILE* manifest;
  uint64_t height, blocks_count, logs_count;

  // Data pages
  vector_t blocks_pages;  // <file_t>
  vector_t logs_pages;    // <rcl_page_t>
};

static void rcl_state_read(rcl_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fscanf(t->manifest, "%zu %zu %zu", &t->height, &t->blocks_count,
         &t->logs_count);
}

static void rcl_state_write(rcl_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fprintf(t->manifest, "%zu %zu %zu", t->height, t->blocks_count,
          t->logs_count);
  fflush(t->manifest);
}

static rcl_result rcl_db_restore(rcl_t* db, const char* state_filename) {
  db->manifest = fopen(state_filename, "r+");
  if (rcl_unlikely(db->manifest == NULL)) {
    return RCL_ERROR_FS_IO;
  }

  rcl_state_read(db);

  // blocks_pages
  uint64_t blocks_pages_count = db->blocks_count / BLOCKS_FILE_CAPACITY;
  if (blocks_pages_count * BLOCKS_FILE_CAPACITY < db->blocks_count)
    blocks_pages_count++;

  if (!vector_init(&(db->blocks_pages), blocks_pages_count, sizeof(file_t))) {
    return RCL_ERROR_UNKNOWN;
  }

  rcl_filename_t filename = {0};
  for (uint64_t i = 0; i < blocks_pages_count; ++i) {
    int status = rcl_page_filename(filename, db->dir, i, 'b');
    if (rcl_unlikely(status != 0))
      return RCL_ERROR_UNKNOWN;

    status = file_open(vector_add(&(db->blocks_pages)), filename,
                       BLOCKS_FILE_CAPACITY * sizeof(rcl_block_t));
    if (rcl_unlikely(status != 0))
      return RCL_ERROR_FS_IO;
  }

  // logs_pages
  uint64_t logs_pages_count = db->blocks_count / LOGS_PAGE_CAPACITY;
  if (logs_pages_count * LOGS_PAGE_CAPACITY < db->blocks_count)
    logs_pages_count++;

  if (!vector_init(&(db->logs_pages), logs_pages_count, sizeof(rcl_page_t))) {
    return RCL_ERROR_UNKNOWN;
  }

  for (uint64_t i = 0; i < logs_pages_count; ++i) {
    rcl_page_t* page = (rcl_page_t*)vector_add(&(db->logs_pages));
    if (rcl_page_init(page, db->dir, i) != 0) {
      return RCL_ERROR_UNKNOWN;
    }
  }

  return RCL_SUCCESS;
}

static rcl_result rcl_db_init(rcl_t* db, const char* state_filename) {
  db->manifest = fopen(state_filename, "w+");
  if (rcl_unlikely(db->manifest == NULL)) {
    return RCL_ERROR_UNKNOWN;
  }

  db->height = 0;
  db->blocks_count = 0;
  db->logs_count = 0;

  vector_init(&(db->blocks_pages), 4096, sizeof(file_t));
  vector_init(&(db->logs_pages), 8, sizeof(rcl_page_t));

  rcl_state_write(db);

  return RCL_SUCCESS;
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

  db->closed = false;
  db->upstream = NULL;
  db->fetcher_thread = NULL;
  db->ram_limit = ram_limit;
  strncpy(db->dir, dir, MAX_FILE_LENGTH);

  rcl_filename_t state_filename = {0};
  int count =
      snprintf(state_filename, MAX_FILE_LENGTH, "%s/%s", db->dir, "toc.txt");
  if (rcl_unlikely(count < 0 || count >= MAX_FILE_LENGTH)) {
    return RCL_ERROR_UNKNOWN;
  }

  if (access(state_filename, F_OK) == 0) {
    return rcl_db_restore(db, state_filename);
  } else {
    return rcl_db_init(db, state_filename);
  }
}

rcl_result rcl_update_height(rcl_t* db, uint64_t height) {
  pthread_rwlock_wrlock(&(db->lock));

  db->height = height;
  rcl_state_write(db);

  pthread_rwlock_unlock(&(db->lock));

  return RCL_SUCCESS;
}

void* rcl_fetcher_thread(void* data) {
  rcl_t* db = (rcl_t*)data;
  vector_t logs;
  vector_init(&logs, 16, sizeof(rcl_log_t));

  char upstream[UPSTREAM_LIMIT + 1] = {0};

  for (;; sleep(1)) {
    pthread_rwlock_rdlock(&(db->lock));
    bool closed = db->closed;
    bool height = db->height;
    bool blocks = db->blocks_count;
    strcpy(upstream, db->upstream);
    pthread_rwlock_unlock(&(db->lock));

    if (closed)
      break;

    logs.size = 0;

    for (size_t start = blocks; start <= height;) {
      size_t count = 16;
      if (height - start < count)
        count = height - start;

      int rc = rcl_request_logs(upstream, start, start + count - 1, &logs);
      if (rc != 0) {
        fprintf(stderr, "liboracle: error of fetch logs\n");
        break;
      }

      if (rcl_insert(db, logs.size, (rcl_log_t*)(logs.buffer)) != RCL_SUCCESS) {
        fprintf(stderr, "liboracle: error of insert logs");
        break;
      }

      start += count;
    }
  }

  pthread_exit(0);
}

rcl_result rcl_set_upstream(rcl_t* db, const char* upstream) {
  pthread_rwlock_wrlock(&(db->lock));

  size_t n = strlen(upstream);
  if (n > UPSTREAM_LIMIT)
    return RCL_ERROR_INVALID_UPSTREAM;

  db->upstream = realloc(db->upstream, n + 1);
  strncpy(db->upstream, upstream, n + 1);

  if (db->fetcher_thread == NULL) {
    db->fetcher_thread = malloc(sizeof(pthread_t));

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_create(db->fetcher_thread, &attr, rcl_fetcher_thread, db);
  }

  rcl_state_write(db);

  pthread_rwlock_unlock(&(db->lock));

  return RCL_SUCCESS;
}

static int rcl_add_block(rcl_t* db, uint64_t block_number) {
  rcl_filename_t filename = {0};

  for (; db->blocks_count != block_number + 1; db->blocks_count++) {
    uint64_t offset = db->blocks_count % BLOCKS_FILE_CAPACITY;

    if (offset == 0) {
      int status =
          rcl_page_filename(filename, db->dir, db->blocks_pages.size, 'b');
      if (rcl_unlikely(status != 0)) {
        return -1;
      }

      file_t* file = (file_t*)vector_add(&(db->blocks_pages));
      if (rcl_unlikely(file == NULL)) {
        return -2;
      }

      status =
          file_open(file, filename, BLOCKS_FILE_CAPACITY * sizeof(rcl_block_t));
      if (rcl_unlikely(status != 0)) {
        return -3;
      }
    }

    file_t* file = vector_last(&(db->blocks_pages));
    file_as_blocks(file)[offset].logs_count = 0;

    if (db->blocks_count == 0) {
      file_as_blocks(file)[offset].offset = 0;
    } else {
      rcl_block_t* last;

      if (offset == 0) {
        last = &(file_as_blocks(file - 1)[BLOCKS_FILE_CAPACITY - 1]);

      } else {
        last = &(file_as_blocks(file)[offset - 1]);
      }

      file_as_blocks(file)[offset].offset = last->offset + last->logs_count;
    }
  }

  return 0;
}

static rcl_block_t* rcl_get_block(rcl_t* db, uint64_t number) {
  uint64_t page = (number + 1) / BLOCKS_FILE_CAPACITY;
  uint64_t offset = (number + 1) % BLOCKS_FILE_CAPACITY - 1;

  file_t* file = (file_t*)vector_at(&(db->blocks_pages), page);
  return &(file_as_blocks(file)[offset]);
}

rcl_result rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs) {
  pthread_rwlock_wrlock(&(db->lock));

  for (size_t i = 0; i < size; ++i) {
    rcl_log_t* log = logs + i;

    // store is immutable, operation not supported
    if (rcl_unlikely(db->blocks_count > log->block_number + 1)) {
      pthread_rwlock_unlock(&(db->lock));
      return RCL_ERROR_INSERT_LOGS_TO_OLD_BLOCK;
    }

    // add new block
    if (log->block_number >= db->blocks_count) {
      int err = rcl_add_block(db, log->block_number);
      if (rcl_unlikely(err != 0)) {
        pthread_rwlock_unlock(&(db->lock));
        return RCL_ERROR_UNKNOWN;
      }
    }

    // add logs page
    uint64_t offset = db->logs_count % LOGS_PAGE_CAPACITY;
    if (offset == 0) {
      rcl_page_t* page = (rcl_page_t*)vector_add(&(db->logs_pages));

      int err = rcl_page_init(page, db->dir, db->logs_pages.size - 1);
      if (rcl_unlikely(err != 0)) {
        pthread_rwlock_unlock(&(db->lock));
        return RCL_ERROR_UNKNOWN;
      }
    }

    rcl_block_t* current_block = rcl_get_block(db, log->block_number);
    rcl_page_t* logs_page =
        vector_at(&(db->logs_pages), db->logs_count / LOGS_PAGE_CAPACITY);

    bloom_add(&(current_block->logs_bloom), log->address);
    file_as_addresses(&(logs_page->addresses))[offset] =
        murmur64A(log->address, sizeof(rcl_address_t), HASH_SEED);

    for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
      bloom_add(&(current_block->logs_bloom), log->topics[j]);
      file_as_topics(&(logs_page->topics))[offset][j] =
          murmur64A(log->topics[j], sizeof(rcl_hash_t), HASH_SEED);
    }

    current_block->logs_count++;
    db->logs_count++;
  }

  rcl_state_write(db);

  pthread_rwlock_unlock(&(db->lock));
  return RCL_SUCCESS;
}

rcl_result rcl_query(rcl_t* db, rcl_query_t* query, uint64_t* result) {
  // TODO: use memory pool
  // Prepare internal view
  bool has_addresses = query->addresses.len > 0, has_topics = false;

  rcl_cell_address_t* addresses = NULL;
  if (has_addresses) {
    addresses = malloc(query->addresses.len * sizeof(rcl_cell_address_t));

    for (size_t i = 0; i < query->addresses.len; ++i) {
      addresses[i] =
          murmur64A(query->addresses.data[i], sizeof(rcl_address_t), HASH_SEED);
    }
  }

  uint64_t* topics[TOPICS_LENGTH];
  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    topics[i] = malloc(query->topics[i].len * sizeof(size_t));

    for (size_t j = 0; j < query->topics[i].len; ++j) {
      has_topics = true;
      topics[i][j] =
          murmur64A(query->topics[i].data[j], sizeof(rcl_hash_t), HASH_SEED);
    }
  }

  // Get count
  pthread_rwlock_rdlock(&(db->lock));

  if (db->blocks_count == 0) {
    *result = 0;

    pthread_rwlock_unlock(&(db->lock));
    return RCL_SUCCESS;
  }

  uint64_t start = query->from_block, end = query->to_block;
  if (end >= db->blocks_count) {
    end = db->blocks_count - 1;
  }

  *result = 0;

  for (size_t number = start; number <= end; ++number) {
    rcl_block_t* block = rcl_get_block(db, number);

    if (block->logs_count == 0)
      continue;

    if (!has_addresses && !has_topics) {
      *result += block->logs_count;
      continue;
    }

    if (!rcl_block_check(block, query)) {
      continue;
    }

    for (uint64_t i = block->offset, l = block->offset + block->logs_count;
         i < l; ++i) {
      uint64_t offset = i % LOGS_PAGE_CAPACITY;
      rcl_page_t* logs_page =
          vector_at(&(db->logs_pages), i / LOGS_PAGE_CAPACITY);

      if (query->addresses.len > 0) {
        rcl_cell_address_t address =
            file_as_addresses(&(logs_page->addresses))[offset];

        if (!includes(address, addresses, query->addresses.len))
          continue;
      }

      rcl_cell_topics_t* tpcs = file_as_topics(&(logs_page->topics));

      bool topics_match = true;
      for (size_t j = 0; topics_match && j < TOPICS_LENGTH; ++j) {
        if (query->topics[j].len > 0) {
          topics_match =
              includes(tpcs[offset][j], topics[j], query->topics[j].len);
        }
      }

      if (topics_match)
        ++(*result);
    }
  }

  pthread_rwlock_unlock(&(db->lock));

  if (addresses != NULL)
    free(addresses);

  for (int i = 0; i < TOPICS_LENGTH; ++i)
    if (topics[i] != NULL)
      free(topics[i]);

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

void rcl_free(rcl_t* db) {
  // Close
  pthread_rwlock_wrlock(&(db->lock));
  db->closed = true;
  pthread_rwlock_unlock(&(db->lock));

  // Wait fetcher
  pthread_rwlock_rdlock(&(db->lock));
  if (db->fetcher_thread) {
    pthread_join(*(db->fetcher_thread), NULL);
    free(db->fetcher_thread);
  }
  pthread_rwlock_unlock(&(db->lock));

  // Clear db
  pthread_rwlock_wrlock(&(db->lock));

  rcl_state_write(db);
  fclose(db->manifest);

  for (uint64_t i = 0; i < db->blocks_pages.size; ++i) {
    file_t* it = (file_t*)(vector_remove_last(&(db->blocks_pages)));
    file_close(it);
  }

  for (uint64_t i = 0; i < db->logs_pages.size; ++i) {
    rcl_page_t* it = (rcl_page_t*)(vector_remove_last(&(db->logs_pages)));
    rcl_page_destroy(it);
  }

  vector_destroy(&(db->blocks_pages));
  vector_destroy(&(db->logs_pages));

  pthread_rwlock_unlock(&(db->lock));
  pthread_rwlock_destroy(&(db->lock));

  free(db);

  // Clear global
  curl_global_cleanup();
}
