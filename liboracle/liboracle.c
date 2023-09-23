#include "liboracle.h"
#include "utils.h"

#define MAX_FILE_LENGTH 256
#define HASH_SEED 1907531730

#define STORE_BLOCKS_RESERVE_STEP 10000
#define STORE_LOGS_RESERVE_STEP 10000

#define STORE_SIZE (2ul << 40)  // bytes

typedef uint64_t db_cell_address_t;
typedef uint64_t db_cell_topic_t[TOPICS_LENGTH];

typedef struct {
  // block_number - index in parrent storage
  uint64_t logs_count, offset;
  bloom_t logs_bloom;
} db_block_t;

struct db {
  uint64_t ram_limit;
  char dir[MAX_FILE_LENGTH];

  size_t current_block, blocks_capacity;
  size_t logs_count, logs_capacity;

  FILE* manifest;
  int blocks_fd, addresses_fd, topics_fd;

  // blocks index
  db_block_t* blocks;

  // memory areas for raw cells
  db_cell_address_t* addresses;
  db_cell_topic_t* topics;
};

void db_query_free(db_query_t* query) {
  free(query->addresses.data);

  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    free(query->topics[i].data);
  }
}

inline void _read_manifest(db_t* db) {
  fseek(db->manifest, 0, SEEK_SET);
  fscanf(db->manifest, "%zd %zd %zd %zd", &db->current_block,
         &db->blocks_capacity, &db->logs_count, &db->logs_capacity);
}

inline void _write_manifest(db_t* db) {
  fseek(db->manifest, 0, SEEK_SET);
  fprintf(db->manifest, "%zd %zd %zd %zd", db->current_block,
          db->blocks_capacity, db->logs_count, db->logs_capacity);
}

int _get_filename(db_t* db, char file[MAX_FILE_LENGTH], const char* name) {
  memset(file, 0, MAX_FILE_LENGTH);

  int count = snprintf(file, MAX_FILE_LENGTH, "%s/%s", db->dir, name);
  if (count < 0 || count >= MAX_FILE_LENGTH) {
    return -1;
  }

  return 0;
}

int _open_file(db_t* db, const char* filename, size_t init_size) {
  char file[MAX_FILE_LENGTH] = {0};
  if (_get_filename(db, file, filename) != 0)
    return -1;

  int exists = access(file, F_OK) == 0;
  errno = 0;

  int fd = open(file, O_RDWR | O_CREAT, (mode_t)0600);
  if (fd < 0)
    return -1;

  if (!exists && ftruncate(fd, init_size) != 0) {
    return -1;
  }

  return fd;
}

void* _map_file(size_t fd) {
  return mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE | MAP_HUGETLB,
              MAP_SHARED, fd, 0);
}

db_t* db_new(char* dir, uint64_t ram_limit) {
  db_t* db = (db_t*)malloc(sizeof(db_t));
  db->ram_limit = ram_limit;

  memset(db->dir, 0, MAX_FILE_LENGTH);
  strncpy(db->dir, dir, MAX_FILE_LENGTH - 1);

  // alloc columns store
  db->logs_count = 0;
  db->logs_capacity = STORE_LOGS_RESERVE_STEP;
  db->blocks_capacity = STORE_BLOCKS_RESERVE_STEP;

  db->blocks_fd = _open_file(db, "blocks.bin",
                             STORE_BLOCKS_RESERVE_STEP * sizeof(db_block_t));
  db->addresses_fd = _open_file(
      db, "addresses.bin", STORE_LOGS_RESERVE_STEP * sizeof(db_cell_address_t));
  db->topics_fd = _open_file(db, "topics.bin",
                             STORE_LOGS_RESERVE_STEP * sizeof(db_cell_topic_t));

  if (db->blocks_fd < 0 || db->addresses_fd < 0 || db->topics_fd < 0) {
    return NULL;
  }

  db->blocks = (db_block_t*)_map_file(db->blocks_fd);
  db->addresses = (db_cell_address_t*)_map_file(db->addresses_fd);
  db->topics = (db_cell_topic_t*)_map_file(db->topics_fd);

  // alloc blocks index
  db->current_block = 0;
  db->blocks[0].logs_count = 0;
  db->blocks[0].offset = 0;

  // make manifest
  char file[MAX_FILE_LENGTH] = {0};
  if (_get_filename(db, file, "manifest.txt") != 0) {
    return NULL;
  }

  int exists = access(file, F_OK) == 0;
  errno = 0;

  if (exists) {
    db->manifest = fopen(file, "r+");
    _read_manifest(db);
  } else {
    db->manifest = fopen(file, "w+");
    _write_manifest(db);
  }

  // Lock cache
  uint64_t limit = db->ram_limit;
  uint64_t blocks_bytes = db->blocks_capacity * sizeof(db_block_t);

  if (blocks_bytes <= limit) {
    limit -= blocks_bytes;
    if (mlock(db->blocks, blocks_bytes) == -1)
      return NULL;
  }

  uint64_t record_bytes = sizeof(db_cell_address_t) + sizeof(db_cell_topic_t);
  if (limit > record_bytes) {
    uint64_t records = limit / record_bytes;

    if (mlock(db->addresses, records * sizeof(db_cell_address_t)) == -1)
      return NULL;

    if (mlock(db->topics, records * sizeof(db_cell_topic_t)) == -1)
      return NULL;
  }

  return db;
}

inline bool _db_check_block_by_query(db_block_t* block, db_query_t* query) {
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

inline size_t _db_block_query(db_t* db,
                              db_block_t* block,
                              db_query_t* query,
                              db_cell_address_t* addresses,
                              size_t* topics[TOPICS_LENGTH]) {
  uint64_t count = 0;

  for (size_t i = block->offset, end = i + block->logs_count; i < end; ++i) {
    if (query->addresses.len > 0 &&
        !includes(db->addresses[i], addresses, query->addresses.len)) {
      continue;
    }

    bool topics_match = true;
    for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
      if (query->topics[j].len > 0 &&
          !includes(db->topics[i][j], topics[j], query->topics[j].len)) {
        topics_match = false;
        break;
      }
    }

    if (topics_match)
      ++count;
  }

  return count;
}

uint64_t db_query(db_t* db, db_query_t query) {
  // Prepare internal view
  bool has_addresses = query.addresses.len > 0, has_topics = false;

  db_cell_address_t* addresses = NULL;
  if (has_addresses) {
    addresses = (db_cell_address_t*)calloc(query.addresses.len,
                                           sizeof(db_cell_address_t));

    for (size_t i = 0; i < query.addresses.len; ++i) {
      addresses[i] =
          murmur64A(query.addresses.data[i], sizeof(db_address_t), HASH_SEED);
    }
  }

  size_t* topics[TOPICS_LENGTH] = {NULL};
  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    topics[i] = (size_t*)calloc(query.topics[i].len, sizeof(size_t));

    for (size_t j = 0; j < query.topics[i].len; ++j) {
      has_topics = true;
      topics[i][j] =
          murmur64A(query.topics[i].data[j], sizeof(db_hash_t), HASH_SEED);
    }
  }

  // Get count
  uint64_t start = query.from_block,
           end = min(db->current_block, query.to_block);

  uint64_t count = 0;
  for (size_t number = start; number <= end; ++number) {
    db_block_t* block = &(db->blocks[number]);

    if (block->logs_count != 0) {
      if (!has_addresses && !has_topics) {
        count += block->logs_count;
      } else if (_db_check_block_by_query(block, &query)) {
        count += _db_block_query(db, block, &query, addresses, topics);
      }
    }
  }

  if (addresses != NULL)
    free(addresses);

  for (int i = 0; i < TOPICS_LENGTH; ++i)
    if (topics[i] != NULL)
      free(topics[i]);

  db_query_free(&query);

  return count;
}

void db_insert(db_t* db, size_t size, db_log_t* logs) {
  for (size_t i = 0; i < size; ++i) {
    // store is immutable, operation not supported
    if (db->current_block > logs[i].block_number) {
      return;
    }

    // add new block
    if (db->current_block < logs[i].block_number) {
      db_block_t* last = &(db->blocks[db->current_block]);

      db->current_block = logs[i].block_number;

      if (db->blocks_capacity <= db->current_block) {
        db->blocks_capacity = db->current_block + STORE_BLOCKS_RESERVE_STEP;

        if (ftruncate(db->blocks_fd,
                      db->blocks_capacity * sizeof(db_block_t)) != 0) {
          return;
        }

        if (mlock(db->blocks, db->blocks_capacity * sizeof(db_block_t)) == -1) {
          return;
        }
      }

      db->blocks[db->current_block].logs_count = 0;
      db->blocks[db->current_block].offset = last->offset + last->logs_count;
    }

    // resize addreses and topics store
    if (db->logs_capacity == db->logs_count) {
      db->logs_capacity += STORE_LOGS_RESERVE_STEP;

      if (ftruncate(db->addresses_fd,
                    db->logs_capacity * sizeof(db_address_t)) != 0 ||
          ftruncate(db->topics_fd, db->logs_capacity * sizeof(db_hash_t) *
                                       TOPICS_LENGTH) != 0) {
        return;
      }

      // if (mlock(db->addresses, db->logs_capacity * sizeof(db_address_t)) ==
      //     -1) {
      //   return;
      // }
    }

    db_block_t* current_block = &(db->blocks[db->current_block]);

    bloom_add(&(current_block->logs_bloom), logs[i].address);
    db->addresses[db->logs_count] =
        murmur64A(logs[i].address, sizeof(db_address_t), HASH_SEED);

    for (size_t j = 0; j < 4; ++j) {
      bloom_add(&(current_block->logs_bloom), logs[i].topics[j]);
      db->topics[db->logs_count][j] =
          murmur64A(logs[i].topics[j], sizeof(db_hash_t), HASH_SEED);
    }

    current_block->logs_count++;
    db->logs_count++;
  }

  _write_manifest(db);
}

uint64_t db_current_block(db_t* db) {
  return db->current_block;
}

void db_status(db_t* db, char* buffer, size_t len) {
  snprintf(buffer, len,

           "dir: '%s'\n"
           "block_count: %lu\n"
           "logs_count:  %lu\n",

           db->dir, db->current_block, db->logs_count);
}

void db_free(db_t* db) {
  _write_manifest(db);
  fclose(db->manifest);

  // flush blocks
  size_t bs = db->blocks_capacity * sizeof(db_block_t);
  munlock(db->blocks, bs);
  msync(db->blocks, bs, MS_SYNC);
  munmap(db->blocks, STORE_SIZE);
  close(db->blocks_fd);

  // flush addresses
  size_t as = db->logs_capacity * sizeof(db_address_t);
  munlock(db->addresses, as);
  msync(db->blocks, as, MS_SYNC);
  munmap(db->addresses, STORE_SIZE);
  close(db->addresses_fd);

  // flush topics
  munmap(db->topics, STORE_SIZE);
  close(db->topics_fd);

  free(db);
  db = NULL;
}
