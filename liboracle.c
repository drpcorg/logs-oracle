#include "liboracle.h"

static uint64_t LOGS_PAGE_CAPACITY = 1000000;   // 1m
static uint64_t BLOCKS_PAGE_CAPACITY = 100000;  // 100k

typedef uint64_t rcl_cell_address_t;
typedef uint64_t rcl_cell_topics_t[TOPICS_LENGTH];

// type: rcl_block_t
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

// type: rcl_block_t
typedef struct {
  int fd;
  size_t bytes;
  void* buffer;
} rcl_file_t;

static uint64_t rcl_file_size_reserve = 2ul << 36;  // 128GB addresses per file

#define rcl_file_as_blocks(p) ((rcl_block_t*)((p)->buffer))
#define rcl_file_as_addresses(p) ((rcl_cell_address_t*)((p)->buffer))
#define rcl_file_as_topics(p) ((rcl_cell_topics_t*)((p)->buffer))

static int rcl_file_open(rcl_file_t* f,
                         const char* filename,
                         uint64_t init_size) {
  int exists = access(filename, F_OK) == 0;
  errno = 0;

  f->fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);
  if (f->fd < 0)
    return -1;

  if (exists) {
    struct stat st;
    if (fstat(f->fd, &st) != 0) {
      return -1;
    }

    f->bytes = (size_t)(st.st_size);
  } else {
    if (ftruncate(f->fd, (off_t)init_size) != 0)
      return -1;
  }

  int flags = PROT_READ | PROT_WRITE;
#ifdef MAP_HUGETLB
  flags = flags | MAP_HUGETLB;
#endif

  f->buffer = mmap(NULL, rcl_file_size_reserve, flags, MAP_SHARED, f->fd, 0);

  if (f->buffer == MAP_FAILED)
    return -1;

  return 0;
}

/*
static int rcl_file_lock(rcl_file_t* f) {
  return mlock2(f->buffer, f->bytes, MLOCK_ONFAULT) == 0 ? 0 : -1;
}

static int rcl_file_unlock(rcl_file_t* f) {
  return munlock(f->buffer, f->bytes) == 0 ? 0 : -1;
}

static int rcl_file_resize(rcl_file_t* f, size_t size) {
  if (size > rcl_file_size_reserve) {
    return -1;
  }

  f->bytes = size;

  return ftruncate(f->fd, size) == 0 ? 0 : -1;
}
*/

int rcl_file_close(rcl_file_t* f) {
  munmap(f->buffer, rcl_file_size_reserve);
  return close(f->fd);
}

// type: rcl_page_t
typedef struct {
  uint64_t index;

  rcl_file_t addresses;  // rcl_cell_address_t*
  rcl_file_t topics;     // rcl_cell_topics_t*
} rcl_page_t;

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

  int ra = rcl_file_open(&(page->addresses), addresses_file,
                         LOGS_PAGE_CAPACITY * sizeof(rcl_cell_address_t));
  int rt = rcl_file_open(&(page->topics), topics_file,
                         LOGS_PAGE_CAPACITY * sizeof(rcl_cell_topics_t));
  if (ra != 0 || rt != 0)
    return -1;

  return 0;
}

void rcl_page_destroy(rcl_page_t* page) {
  rcl_file_close(&(page->addresses));
  rcl_file_close(&(page->topics));
}

// type: rcl_t
static uint32_t HASH_SEED = 1907531730ul;

struct db {
  pthread_mutex_t mu;

  // Config
  uint64_t ram_limit;
  rcl_filename_t dir;

  // DB state
  FILE* manifest;

  uint64_t blocks_count;
  uint64_t logs_count;

  // Data pages
  rcl_vector_t blocks_pages;  // <rcl_file_t>
  rcl_vector_t logs_pages;    // <rcl_page_t>
};

static void rcl_state_read(rcl_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fscanf(t->manifest, "%zu %zu", &t->blocks_count, &t->logs_count);
}

static void rcl_state_write(rcl_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fprintf(t->manifest, "%zu %zu", t->blocks_count, t->logs_count);
}

rcl_t* rcl_new(char* dir, uint64_t ram_limit) {
  rcl_t* db = (rcl_t*)malloc(sizeof(rcl_t));

  if (pthread_mutex_init(&(db->mu), NULL) != 0) {
    return NULL;
  }

  db->ram_limit = ram_limit;
  strncpy(db->dir, dir, MAX_FILE_LENGTH);

  rcl_filename_t stateFilename = {0};
  int count =
      snprintf(stateFilename, MAX_FILE_LENGTH, "%s/%s", db->dir, "toc.txt");
  if (rcl_unlikely(count < 0 || count >= MAX_FILE_LENGTH)) {
    return NULL;
  }

  int stateExists = access(stateFilename, F_OK) == 0;
  errno = 0;

  if (stateExists) {
    db->manifest = fopen(stateFilename, "r+");
    if (rcl_unlikely(db->manifest == NULL)) {
      return NULL;
    }

    rcl_state_read(db);

    // blocks_pages
    uint64_t blocks_pages_count = db->blocks_count / BLOCKS_PAGE_CAPACITY;
    if (blocks_pages_count * BLOCKS_PAGE_CAPACITY < db->blocks_count)
      blocks_pages_count++;

    rcl_vector_init(&(db->blocks_pages), blocks_pages_count,
                    sizeof(rcl_file_t));

    rcl_filename_t filename = {0};
    for (uint64_t i = 0; i < blocks_pages_count; ++i) {
      int status = rcl_page_filename(filename, db->dir, i, 'b');
      if (rcl_unlikely(status != 0)) {
        return NULL;
      }

      status = rcl_file_open(rcl_vector_add(&(db->blocks_pages)), filename,
                             BLOCKS_PAGE_CAPACITY * sizeof(rcl_block_t));
      if (rcl_unlikely(status != 0)) {
        return NULL;
      }
    }

    // logs_pages
    uint64_t logs_pages_count = db->blocks_count / LOGS_PAGE_CAPACITY;
    if (logs_pages_count * LOGS_PAGE_CAPACITY < db->blocks_count)
      logs_pages_count++;

    rcl_vector_init(&(db->logs_pages), logs_pages_count, sizeof(rcl_page_t));

    for (uint64_t i = 0; i < logs_pages_count; ++i) {
      rcl_page_t* page = (rcl_page_t*)rcl_vector_add(&(db->logs_pages));
      if (rcl_page_init(page, db->dir, i) != 0) {
        return 0;
      }
    }
  } else {
    db->manifest = fopen(stateFilename, "w+");
    if (rcl_unlikely(db->manifest == NULL)) {
      return NULL;
    }

    db->blocks_count = 0;
    db->logs_count = 0;

    rcl_vector_init(&(db->blocks_pages), 8, sizeof(rcl_file_t));
    rcl_vector_init(&(db->logs_pages), 8, sizeof(rcl_page_t));

    rcl_state_write(db);
  }

  return db;
}

static int rcl_add_block(rcl_t* db, uint64_t block_number) {
  rcl_filename_t filename = {0};

  for (; db->blocks_count != block_number + 1; db->blocks_count++) {
    uint64_t offset = db->blocks_count % BLOCKS_PAGE_CAPACITY;

    if (offset == 0) {
      int status =
          rcl_page_filename(filename, db->dir, db->blocks_pages.items, 'b');
      if (rcl_unlikely(status != 0)) {
        return -1;
      }

      rcl_file_t* file = (rcl_file_t*)rcl_vector_add(&(db->blocks_pages));

      status = rcl_file_open(file, filename,
                             BLOCKS_PAGE_CAPACITY * sizeof(rcl_block_t));
      if (rcl_unlikely(status != 0)) {
        return -2;
      }
    }

    rcl_file_t* file = rcl_vector_last(&(db->blocks_pages));
    rcl_file_as_blocks(file)[offset].logs_count = 0;

    if (rcl_unlikely(db->blocks_count == 0)) {
      rcl_file_as_blocks(file)[offset].offset = 0;
    } else {
      rcl_block_t* last;

      if (offset == 0) {
        last = &(rcl_file_as_blocks(file - 1)[BLOCKS_PAGE_CAPACITY - 1]);
      } else {
        last = &(rcl_file_as_blocks(file)[offset - 1]);
      }

      rcl_file_as_blocks(file)[offset].offset = last->offset + last->logs_count;
    }
  }

  return 0;
}

static rcl_block_t* rcl_get_block(rcl_t* db, uint64_t number) {
  uint64_t page = (number + 1) / BLOCKS_PAGE_CAPACITY;
  uint64_t offset = (number + 1) % BLOCKS_PAGE_CAPACITY - 1;

  rcl_file_t* file = (rcl_file_t*)rcl_vector_at(&(db->blocks_pages), page);
  return &(rcl_file_as_blocks(file)[offset]);
}

int rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs) {
  for (size_t i = 0; i < size; ++i) {
    rcl_log_t* log = logs + i;

    // store is immutable, operation not supported
    if (rcl_unlikely(db->blocks_count > log->block_number + 1)) {
      return -1;
    }

    // add new block
    if (log->block_number >= db->blocks_count) {
      int err = rcl_add_block(db, log->block_number);
      if (rcl_unlikely(err != 0)) {
        return -2;
      }
    }

    // add logs page
    uint64_t offset = db->logs_count % LOGS_PAGE_CAPACITY;

    if (offset == 0) {
      rcl_page_t* page = (rcl_page_t*)rcl_vector_add(&(db->logs_pages));

      int err = rcl_page_init(page, db->dir, db->logs_pages.items - 1);
      if (rcl_unlikely(err != 0)) {
        return -3;
      }
    }

    rcl_block_t* current_block = rcl_get_block(db, log->block_number);
    rcl_page_t* logs_page =
        rcl_vector_at(&(db->logs_pages), db->logs_count / LOGS_PAGE_CAPACITY);

    bloom_add(&(current_block->logs_bloom), log->address);
    rcl_file_as_addresses(&(logs_page->addresses))[offset] =
        murmur64A(log->address, sizeof(rcl_address_t), HASH_SEED);

    for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
      bloom_add(&(current_block->logs_bloom), log->topics[j]);
      rcl_file_as_topics(&(logs_page->topics))[offset][j] =
          murmur64A(log->topics[j], sizeof(rcl_hash_t), HASH_SEED);
    }

    current_block->logs_count++;
    db->logs_count++;
  }

  rcl_state_write(db);

  return 0;
}

uint64_t rcl_query(rcl_t* db, rcl_query_t* query) {
  if (db->blocks_count == 0) {
    return 0;
  }

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
  uint64_t start = query->from_block, end = query->to_block;
  if (end >= db->blocks_count) {
    end = db->blocks_count - 1;
  }

  uint64_t count = 0;

  for (size_t number = start; number <= end; ++number) {
    rcl_block_t* block = rcl_get_block(db, number);

    if (block->logs_count == 0)
      continue;

    if (!has_addresses && !has_topics) {
      count += block->logs_count;
      continue;
    }

    if (!rcl_block_check(block, query)) {
      continue;
    }

    for (uint64_t i = block->offset, l = block->offset + block->logs_count;
         i < l; ++i) {
      uint64_t offset = i % LOGS_PAGE_CAPACITY;
      rcl_page_t* logs_page =
          rcl_vector_at(&(db->logs_pages), i / LOGS_PAGE_CAPACITY);

      if (query->addresses.len > 0) {
        rcl_cell_address_t address =
            rcl_file_as_addresses(&(logs_page->addresses))[offset];

        if (!includes(address, addresses, query->addresses.len))
          continue;
      }

      rcl_cell_topics_t* tpcs = rcl_file_as_topics(&(logs_page->topics));

      bool topics_match = true;
      for (size_t j = 0; topics_match && j < TOPICS_LENGTH; ++j) {
        if (query->topics[j].len > 0) {
          topics_match =
              includes(tpcs[offset][j], topics[j], query->topics[j].len);
        }
      }

      if (topics_match)
        ++count;
    }
  }

  if (addresses != NULL)
    free(addresses);

  for (int i = 0; i < TOPICS_LENGTH; ++i)
    if (topics[i] != NULL)
      free(topics[i]);

  return count;
}

uint64_t rcl_blocks_count(rcl_t* db) {
  return db->blocks_count;
}

uint64_t rcl_logs_count(rcl_t* db) {
  return db->logs_count;
}

void rcl_free(rcl_t* db) {
  pthread_mutex_lock(&(db->mu));

  rcl_state_write(db);
  fclose(db->manifest);

  for (uint64_t i = 0; i < db->blocks_pages.items; ++i) {
    rcl_file_t* it = (rcl_file_t*)(rcl_vector_remove_last(&(db->blocks_pages)));
    rcl_file_close(it);
  }

  for (uint64_t i = 0; i < db->logs_pages.items; ++i) {
    rcl_page_t* it = (rcl_page_t*)(rcl_vector_remove_last(&(db->logs_pages)));
    rcl_page_destroy(it);
  }

  rcl_vector_destroy(&(db->blocks_pages));
  rcl_vector_destroy(&(db->logs_pages));

  pthread_mutex_unlock(&(db->mu));
  pthread_mutex_destroy(&(db->mu));

  free(db);
}
