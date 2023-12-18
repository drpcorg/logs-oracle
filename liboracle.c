#include "liboracle.h"

#include "bloom.h"
#include "common.h"
#include "file.h"
#include "loader.h"
#include "vector.h"

#define UPSTREAM_LIMIT 4096

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
  rcl_hash_t hash;
  rcl_address_t address;

  if (query->address.len > 0) {
    bool has = false;

    for (size_t i = 0; i < query->address.len; ++i) {
      if (hex2bin(address, query->address.data[i], sizeof(rcl_address_t)) !=
          0) {
        continue;  // ignore invalid
      }

      if (bloom_check(&(block->logs_bloom), address)) {
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

    for (size_t j = 0; j < query->topics[i].len; ++j) {
      if (hex2bin(hash, query->topics[i].data[j], sizeof(rcl_hash_t)) != 0) {
        continue;  // ignore invalid
      }

      if (bloom_check(&(block->logs_bloom), hash)) {
        current_has = true;
        break;
      }
    }

    if (!current_has)
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
  vector_t data_pages;    // <rcl_page_t>
};

static int rcl_open_blocks_page(rcl_t* db) {
  rcl_filename_t filename = {0};

  int status = rcl_page_filename(filename, db->dir, db->blocks_pages.size, 'b');
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

  return 0;
}

static int rcl_open_data_page(rcl_t* db) {
  size_t index = db->data_pages.size;

  rcl_page_t* page = (rcl_page_t*)vector_add(&(db->data_pages));
  page->index = index;

  rcl_filename_t addresses_file = {0}, topics_file = {0};

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

  int count = fscanf(t->manifest, "%" PRIu64 " %" PRIu64 " %" PRIu64 "", &t->height, &t->blocks_count,
                     &t->logs_count);

  if (count != 3)
    return RCL_ERROR_FS_IO;

  return RCL_SUCCESS;
}

static rcl_result rcl_state_write(rcl_t* t) {
  if (fseek(t->manifest, 0, SEEK_SET) != 0)
    return RCL_ERROR_FS_IO;

  int bytes = fprintf(t->manifest, "%" PRIu64 " %" PRIu64 " %" PRIu64 "", t->height, t->blocks_count,
                      t->logs_count);
  if (bytes <= 0)
    return RCL_ERROR_FS_IO;

  if (fflush(t->manifest) != 0)
    return RCL_ERROR_FS_IO;

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
  uint64_t logs_pages_count = db->blocks_count / LOGS_PAGE_CAPACITY;
  if (logs_pages_count * LOGS_PAGE_CAPACITY < db->blocks_count)
    logs_pages_count++;

  if (!vector_init(&(db->data_pages), logs_pages_count, sizeof(rcl_page_t)))
    return RCL_ERROR_UNKNOWN;

  for (uint64_t i = 0; i < logs_pages_count; ++i) {
    if (rcl_open_data_page(db) != 0)
      return RCL_ERROR_UNKNOWN;
  }

  if (db->data_pages.size == 0)
    if (rcl_open_data_page(db) != 0)
      return RCL_ERROR_FS_IO;

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

  if (!vector_init(&(db->blocks_pages), 8, sizeof(file_t)))
    return RCL_ERROR_UNKNOWN;
  if (rcl_open_blocks_page(db) != 0)
    return RCL_ERROR_FS_IO;

  if (!vector_init(&(db->data_pages), 8, sizeof(rcl_page_t)))
    return RCL_ERROR_UNKNOWN;
  if (rcl_open_data_page(db) != 0)
    return RCL_ERROR_UNKNOWN;

  rcl_result wr = rcl_state_write(db);
  if (wr != RCL_SUCCESS)
    return wr;

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
  db->height = height;
  result = rcl_state_write(db);
  pthread_rwlock_unlock(&(db->lock));

  return result;
}

static void* rcl_fetcher_thread(void* data);

rcl_result rcl_set_upstream(rcl_t* db, const char* upstream) {
  rcl_result result = RCL_SUCCESS;

  size_t n = strlen(upstream);
  if (n > UPSTREAM_LIMIT)
    return RCL_ERROR_INVALID_UPSTREAM;

  pthread_rwlock_wrlock(&(db->lock));

  db->upstream = realloc(db->upstream, n + 1);
  strncpy(db->upstream, upstream, n + 1);

  if (db->fetcher_thread == NULL) {
    db->fetcher_thread = malloc(sizeof(pthread_t));

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_create(db->fetcher_thread, &attr, rcl_fetcher_thread, db);
  }

  result = rcl_state_write(db);

  pthread_rwlock_unlock(&(db->lock));

  return result;
}

rcl_result rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs) {
  rcl_result result = RCL_SUCCESS;

  if (size == 0)
    return RCL_SUCCESS;

  pthread_rwlock_wrlock(&(db->lock));

  for (size_t i = 0; i < size; ++i) {
    rcl_log_t* log = logs + i;

    // store is immutable, operation not supported
    if (db->blocks_count > log->block_number + 1) {
      result = RCL_ERROR_INSERT_LOGS_TO_OLD_BLOCK;
      break;
    }

    // add new block
    if (log->block_number >= db->blocks_count) {
      int err = rcl_add_block(db, log->block_number);
      if (rcl_unlikely(err != 0)) {
        result = RCL_ERROR_UNKNOWN;
        break;
      }
    }

    // add logs page
    uint64_t page, offset;
    get_position(db->logs_count, LOGS_PAGE_CAPACITY, &page, &offset);

    if (db->data_pages.size <= page) {
      int err = rcl_open_data_page(db);
      if (rcl_unlikely(err != 0)) {
        result = RCL_ERROR_UNKNOWN;
        break;
      }
    }

    // insert logs
    rcl_block_t* current_block = rcl_get_block(db, log->block_number);
    assert(current_block != NULL);

    rcl_page_t* logs_page = vector_at(&(db->data_pages), page);
    assert(logs_page != NULL);

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

  int wr = rcl_state_write(db);
  if (wr != RCL_SUCCESS)
    result = wr;

  pthread_rwlock_unlock(&(db->lock));
  return result;
}


rcl_result rcl_query(rcl_t* db, rcl_query_t* query, uint64_t* result) {
  // TODO: use memory pool
  // Prepare internal view
  bool has_addresses = query->address.len > 0, has_topics = false;

  rcl_cell_address_t* addresses = NULL;
  if (has_addresses) {
    addresses = malloc(query->address.len * sizeof(rcl_cell_address_t));

    for (size_t i = 0; i < query->address.len; ++i) {
      rcl_address_t address;
      if (hex2bin(address, query->address.data[i], sizeof(rcl_address_t)) !=
          0) {
        return RCL_ERROR_UNKNOWN;
      }

      addresses[i] = murmur64A(address, sizeof(rcl_address_t), HASH_SEED);
    }
  }

  uint64_t* topics[TOPICS_LENGTH];
  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    topics[i] = malloc(query->topics[i].len * sizeof(size_t));

    for (size_t j = 0; j < query->topics[i].len; ++j) {
      has_topics = true;

      rcl_hash_t hash;
      if (hex2bin(hash, query->topics[i].data[j], sizeof(rcl_hash_t)) != 0) {
        return RCL_ERROR_UNKNOWN;
      }

      topics[i][j] = murmur64A(hash, sizeof(rcl_hash_t), HASH_SEED);
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
    assert(block != NULL);

    if (block->logs_count == 0)
      continue;

    if (!has_addresses && !has_topics) {
      *result += block->logs_count;
      continue;
    }

    if (!rcl_block_check(block, query)) {
      continue;
    }

    for (uint64_t l = block->offset, r = block->offset + block->logs_count;
         l < r; ++l) {
      uint64_t page, offset;
      get_position(l, LOGS_PAGE_CAPACITY, &page, &offset);

      rcl_page_t* logs_page = vector_at(&(db->data_pages), page);

      if (query->address.len > 0) {
        rcl_cell_address_t address =
            file_as_addresses(&(logs_page->addresses))[offset];

        if (!includes(address, addresses, query->address.len))
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

static void* rcl_fetcher_thread(void* data) {
  enum { LOGS_REQUEST_BATCH = 256 };

  int rc;
  rcl_t* db = (rcl_t*)data;

  vector_t logs;
  vector_init(&logs, 16, sizeof(rcl_log_t));

  rcl_debug("run fetcher thread\n");

  char upstream[UPSTREAM_LIMIT + 1] = {0};

  for (;;) {
    pthread_rwlock_rdlock(&(db->lock));
    bool closed = db->closed;
    uint64_t height = db->height;
    uint64_t blocks = db->blocks_count;
    strcpy(upstream, db->upstream);
    pthread_rwlock_unlock(&(db->lock));

    if (closed)
      break;

    if (blocks > height) {
      rcl_debug("more blocks loaded than available; height: %" PRIu64 ", blocks: %" PRIu64 "\n",
                height, blocks);
      sleep(1);
      continue;
    }

    vector_reset(&logs);

    size_t count = LOGS_REQUEST_BATCH;
    if (blocks + count > height)
      count = height - blocks;

    rc = rcl_request_logs(upstream, blocks, blocks + count, &logs);
    if (rc != 0) {
      rcl_error("couldn't to fetch logs; code: %d\n", rc);
      continue;
    }

    rc = rcl_insert(db, logs.size, (rcl_log_t*)(logs.buffer));
    if (rc != RCL_SUCCESS) {
      rcl_error("couldn't to insert fetched logs; code %d\n", rc);
      continue;
    }

    pthread_rwlock_wrlock(&(db->lock));
    db->blocks_count += count;
    rcl_debug("rcl_fetcher_thread; added %" PRIu64 " logs, blocks: %" PRIu64 "\n", logs.size,
              db->blocks_count);
    pthread_rwlock_unlock(&(db->lock));
  }

  pthread_exit(0);
}
