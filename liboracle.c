#include "liboracle.h"

typedef uint64_t rcl_cell_address_t;
typedef uint64_t rcl_cell_topic_t[TOPICS_LENGTH];

// type: rcl_query_t
static void rcl_query_free(rcl_query_t* query) {
  free(query->addresses.data);

  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    free(query->topics[i].data);
  }
}

// type: rcl_toc_t
typedef struct {
  FILE* manifest;
  size_t current_block, blocks_capacity;
  size_t logs_count, logs_capacity;
} rcl_toc_t;

static void rcl_toc_read(rcl_toc_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fscanf(t->manifest, "%zu %zu %zu %zu", &t->current_block, &t->blocks_capacity,
         &t->logs_count, &t->logs_capacity);
}

static void rcl_toc_write(rcl_toc_t* t) {
  fseek(t->manifest, 0, SEEK_SET);
  fprintf(t->manifest, "%zu %zu %zu %zu", t->current_block, t->blocks_capacity,
          t->logs_count, t->logs_capacity);
}

static int rcl_toc_init(rcl_toc_t* t, const char* file) {
  int exists = access(file, F_OK) == 0;
  errno = 0;

  if (exists) {
    t->manifest = fopen(file, "r+");
    if (t->manifest == NULL) {
      return -1;
    }

    rcl_toc_read(t);
  } else {
    t->manifest = fopen(file, "w+");
    if (t->manifest == NULL) {
      return -1;
    }

    rcl_toc_write(t);
  }

  return 0;
}

static void rcl_toc_close(rcl_toc_t* t) {
  rcl_toc_write(t);
  fclose(t->manifest);
}

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
    bool current_has = false;

    if (query->topics[i].len < 1)
      continue;

    for (size_t size = 0; size < query->topics[i].len; ++size)
      if (bloom_check(&(block->logs_bloom), query->topics[i].data[size])) {
        current_has = true;
        break;
      }

    if (!current_has)
      return false;
  }

  return true;
}

static size_t rcl_block_query(rcl_block_t* block,
                              rcl_query_t* query,

                              rcl_cell_address_t* addresses,
                              rcl_cell_address_t* addresses_hashed,

                              rcl_cell_topic_t* topics,
                              size_t* topics_hashed[TOPICS_LENGTH]) {
  uint64_t count = 0;

  for (size_t i = block->offset, end = i + block->logs_count; i < end; ++i) {
    if (query->addresses.len > 0 &&
        !includes(addresses[i], addresses_hashed, query->addresses.len)) {
      continue;
    }

    bool topics_match = true;
    for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
      if (query->topics[j].len > 0 &&
          !includes(topics[i][j], topics_hashed[j], query->topics[j].len)) {
        topics_match = false;
        break;
      }
    }

    if (topics_match)
      ++count;
  }

  return count;
}

// type: rcl_block_t
typedef struct {
  int fd;
  size_t bytes;
  void* buffer;
} rcl_file_t;

static uint64_t rcl_file_size_reserve = 2ul << 36;  // 128GB addresses per file

static int rcl_file_open(rcl_file_t* f,
                         const char* filename,
                         int64_t init_size) {
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

    f->bytes = st.st_size;
  } else {
    if (ftruncate(f->fd, init_size) != 0)
      return -1;
  }

  f->buffer = mmap(NULL, rcl_file_size_reserve, PROT_READ | PROT_WRITE | MAP_HUGETLB,
                   MAP_SHARED, f->fd, 0);
  if (f->buffer == MAP_FAILED)
    return -1;

  return 0;
}

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

int rcl_file_close(rcl_file_t* f) {
  munmap(f->buffer, rcl_file_size_reserve);
  return close(f->fd);
}

// type: rcl_t
static uint32_t HASH_SEED = 1907531730ul;

static uint64_t STORE_BLOCKS_RESERVE_STEP = 10000;
static uint64_t STORE_LOGS_RESERVE_STEP = 10000;

struct db {
  pthread_mutex_t mu;

  uint64_t ram_limit;
  rcl_filename_t dir;

  rcl_toc_t toc;

  rcl_file_t blocks;     // rcl_block_t*
  rcl_file_t addresses;  // rcl_cell_address_t*
  rcl_file_t topics;     // rcl_cell_topic_t*
};

#define rcl_db_get_blocks(db) ((rcl_block_t*)(db->blocks.buffer))
#define rcl_db_get_addresses(db) ((rcl_cell_address_t*)(db->addresses.buffer))
#define rcl_db_get_topics(db) ((rcl_cell_topic_t*)(db->topics.buffer))

static int get_filename(rcl_t* db, rcl_filename_t file, const char* name) {
  memset(file, 0, MAX_FILE_LENGTH);

  int count = snprintf(file, MAX_FILE_LENGTH, "%s/%s", db->dir, name);
  if (count < 0 || count >= MAX_FILE_LENGTH) {
    return -1;
  }

  return 0;
}

rcl_t* rcl_new(char* dir, uint64_t ram_limit) {
  rcl_t* db = (rcl_t*)malloc(sizeof(rcl_t));
  db->ram_limit = ram_limit;

  if (pthread_mutex_init(&(db->mu), NULL) != 0) {
    return NULL;
  }

  strncpy(db->dir, dir, MAX_FILE_LENGTH);

  // alloc columns store
  db->toc.logs_count = 0;
  db->toc.logs_capacity = STORE_LOGS_RESERVE_STEP;
  db->toc.blocks_capacity = STORE_BLOCKS_RESERVE_STEP;

  // init data files
  rcl_filename_t blocks_file = {0};
  if (get_filename(db, blocks_file, "blocks.bin") != 0)
    return NULL;

  int rb = rcl_file_open(&(db->blocks), blocks_file,
                         STORE_BLOCKS_RESERVE_STEP * sizeof(rcl_block_t));
  if (rb != 0)
    return NULL;

  rcl_filename_t addresses_file = {0};
  if (get_filename(db, addresses_file, "addresses.bin") != 0)
    return NULL;

  int ra = rcl_file_open(&(db->addresses), addresses_file,
                         STORE_LOGS_RESERVE_STEP * sizeof(rcl_cell_address_t));
  if (ra != 0)
    return NULL;

  rcl_filename_t topics_file = {0};
  if (get_filename(db, topics_file, "topics.bin") != 0)
    return NULL;

  int rt = rcl_file_open(&(db->topics), topics_file,
                         STORE_LOGS_RESERVE_STEP * sizeof(rcl_cell_topic_t));
  if (rt != 0)
    return NULL;

  // alloc blocks index
  db->toc.current_block = 0;
  rcl_db_get_blocks(db)[0].logs_count = 0;
  rcl_db_get_blocks(db)[0].offset = 0;

  // make toc
  rcl_filename_t file = {0};
  if (get_filename(db, file, "toc.bin") != 0) {
    return NULL;
  }

  if (rcl_toc_init(&(db->toc), file) != 0) {
    return NULL;
  }

  if (db->blocks.bytes <= db->ram_limit) {
    if (rcl_file_lock(&(db->blocks)) == -1)
      return NULL;
  }

  return db;
}

uint64_t rcl_query(rcl_t* db, rcl_query_t query) {
  // Prepare internal view
  bool has_addresses = query.addresses.len > 0, has_topics = false;

  rcl_cell_address_t* addresses = NULL;
  if (has_addresses) {
    addresses = (rcl_cell_address_t*)calloc(query.addresses.len,
                                            sizeof(rcl_cell_address_t));

    for (size_t i = 0; i < query.addresses.len; ++i) {
      addresses[i] =
          murmur64A(query.addresses.data[i], sizeof(rcl_address_t), HASH_SEED);
    }
  }

  size_t* topics[TOPICS_LENGTH] = {NULL};
  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    topics[i] = (size_t*)calloc(query.topics[i].len, sizeof(size_t));

    for (size_t j = 0; j < query.topics[i].len; ++j) {
      has_topics = true;
      topics[i][j] =
          murmur64A(query.topics[i].data[j], sizeof(rcl_hash_t), HASH_SEED);
    }
  }

  // Get count
  uint64_t start = query.from_block, end = db->toc.current_block;
  if (end > query.to_block) {
    end = query.to_block;
  }

  uint64_t count = 0;
  for (size_t number = start; number <= end; ++number) {
    rcl_block_t* block = &(rcl_db_get_blocks(db)[number]);

    if (block->logs_count != 0) {
      if (!has_addresses && !has_topics) {
        count += block->logs_count;
      } else if (rcl_block_check(block, &query)) {
        count += rcl_block_query(block, &query, rcl_db_get_addresses(db),
                                 addresses, rcl_db_get_topics(db), topics);
      }
    }
  }

  if (addresses != NULL)
    free(addresses);

  for (int i = 0; i < TOPICS_LENGTH; ++i)
    if (topics[i] != NULL)
      free(topics[i]);

  rcl_query_free(&query);

  return count;
}

int rcl_insert(rcl_t* db, size_t size, rcl_log_t* logs) {
  for (size_t i = 0; i < size; ++i) {
    // store is immutable, operation not supported
    if (db->toc.current_block > logs[i].block_number) {
      return -1;
    }

    // add new block
    if (db->toc.current_block < logs[i].block_number) {
      // rcl_db_get_blocks(db)
      rcl_block_t* last = &(rcl_db_get_blocks(db)[db->toc.current_block]);

      db->toc.current_block = logs[i].block_number;

      if (db->toc.blocks_capacity <= db->toc.current_block) {
        db->toc.blocks_capacity =
            db->toc.current_block + STORE_BLOCKS_RESERVE_STEP;

        if (rcl_file_resize(&(db->blocks), db->toc.blocks_capacity *
                                            sizeof(rcl_block_t)) != 0) {
          return -1;
        }

        if (db->blocks.bytes <= db->ram_limit) {
          if (rcl_file_lock(&(db->blocks)) == -1)
            return NULL;
        }
      }

      rcl_db_get_blocks(db)[db->toc.current_block].logs_count = 0;
      rcl_db_get_blocks(db)[db->toc.current_block].offset =
          last->offset + last->logs_count;
    }

    // resize addreses and topics store
    if (db->toc.logs_capacity == db->toc.logs_count) {
      db->toc.logs_capacity += STORE_LOGS_RESERVE_STEP;

      if (rcl_file_resize(&(db->addresses), db->toc.logs_capacity * sizeof(rcl_address_t)) != 0) {
        return -1;
      }

      if (rcl_file_resize(&(db->topics), db->toc.logs_capacity * sizeof(rcl_hash_t) * TOPICS_LENGTH) != 0) {
        return -1;
      }
    }

    rcl_block_t* current_block =
        &(rcl_db_get_blocks(db)[db->toc.current_block]);

    bloom_add(&(current_block->logs_bloom), logs[i].address);
    rcl_db_get_addresses(db)[db->toc.logs_count] =
        murmur64A(logs[i].address, sizeof(rcl_address_t), HASH_SEED);

    for (size_t j = 0; j < 4; ++j) {
      bloom_add(&(current_block->logs_bloom), logs[i].topics[j]);
      rcl_db_get_topics(db)[db->toc.logs_count][j] =
          murmur64A(logs[i].topics[j], sizeof(rcl_hash_t), HASH_SEED);
    }

    current_block->logs_count++;
    db->toc.logs_count++;
  }

  rcl_toc_write(&(db->toc));

  return 0;
}

uint64_t rcl_current_block(rcl_t* db) {
  return db->toc.current_block;
}

void rcl_status(rcl_t* db, char* buffer, size_t len) {
  snprintf(buffer, len,

           "dir: '%s'\n"
           "block_count: %lu\n"
           "logs_count:  %lu\n",

           db->dir, db->toc.current_block, db->toc.logs_count);
}

void rcl_free(rcl_t* db) {
  pthread_mutex_lock(&(db->mu));

  rcl_toc_close(&(db->toc));

  rcl_file_close(&(db->blocks));
  rcl_file_close(&(db->addresses));
  rcl_file_close(&(db->topics));

  pthread_mutex_unlock(&(db->mu));
  pthread_mutex_destroy(&(db->mu));

  free(db);
}
