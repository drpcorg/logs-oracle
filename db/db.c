#include "db.h"

#define MAX_FILE_LENGTH 256
#define HASH_SEED 1907531730

#define STORE_BLOCKS_RESERVE_STEP 10000
#define STORE_LOGS_RESERVE_STEP 10000
#define STORE_SIZE (2ul << 40)  // bytes

#define ERR_DIR_TOO_LONG 1
#define ERR_PREVIOUS_BLOCK 2
#define ERR_FAIL_OPEN_FILES 3
#define ERR_FAILED_REALLOC_FILE 5

typedef uint64_t db_cell_address_t;
typedef uint64_t db_cell_topics_t[TOPICS_LENGTH];

typedef struct {
  uint64_t number, logs_count, offset;
} db_block_t;

struct db {
  char dir[MAX_FILE_LENGTH];

  size_t blocks_count, blocks_capacity;
  size_t logs_count, logs_capacity;

  FILE* manifest;
  int blocks_fd, addresses_fd, topics_fd;

  db_block_t* blocks;
  db_cell_address_t* addresses;
  db_cell_topics_t* topics;
};

void db_free_query(db_query_t* query) {
  free(query->addresses.data);

  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    free(query->topics[i].data);
  }
}

bool _includes(uint64_t key, uint64_t* arr, size_t size) {
  while (size-- > 0)
    if (arr[size] == key)
      return true;

  return false;
}

void _read_manifest(db_t* db) {
  fseek(db->manifest, 0, SEEK_SET);
  fscanf(db->manifest, "%zd %zd %zd %zd", &db->blocks_count,
         &db->blocks_capacity, &db->logs_count, &db->logs_capacity);
}

void _write_manifest(db_t* db) {
  fseek(db->manifest, 0, SEEK_SET);
  fprintf(db->manifest, "%zd %zd %zd %zd", db->blocks_count,
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

db_t* db_new(char* dir) {
  db_t* db = (db_t*)malloc(sizeof(db_t));

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
  db->topics_fd = _open_file(
      db, "topics.bin", STORE_LOGS_RESERVE_STEP * sizeof(db_cell_topics_t));

  if (db->blocks_fd < 0 || db->addresses_fd < 0 || db->topics_fd < 0) {
    errno = ERR_FAIL_OPEN_FILES;
    return NULL;
  }

  db->blocks = (db_block_t*)mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE,
                                 MAP_SHARED, db->blocks_fd, 0);
  db->addresses =
      (db_cell_address_t*)mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE,
                               MAP_SHARED, db->addresses_fd, 0);
  db->topics = (db_cell_topics_t*)mmap(NULL, STORE_SIZE, PROT_READ | PROT_WRITE,
                                       MAP_SHARED, db->topics_fd, 0);

  // alloc blocks index
  db->blocks_count = 1;
  db->blocks[0].number = 0;
  db->blocks[0].logs_count = 0;
  db->blocks[0].offset = 0;

  // make manifest
  char file[MAX_FILE_LENGTH] = {0};
  if (_get_filename(db, file, "manifest.txt") != 0) {
    errno = ERR_FAIL_OPEN_FILES;
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

  return db;
}

void db_close(db_t* db) {
  _write_manifest(db);

  munmap(db->blocks, STORE_SIZE);
  munmap(db->addresses, STORE_SIZE);
  munmap(db->topics, STORE_SIZE);

  close(db->blocks_fd);
  close(db->addresses_fd);
  close(db->topics_fd);

  fclose(db->manifest);

  free(db);

  db = NULL;
}

uint64_t db_query(db_t* db, db_query_t query) {
  // Prepare internal view
  bool has_addresses = query.addresses.len > 0, has_topics = false;

  uint64_t* addresses = NULL;
  if (has_addresses) {
    addresses = (db_cell_address_t*)calloc(query.addresses.len,
                                           sizeof(db_cell_address_t));

    for (size_t j = 0; j < query.addresses.len; ++j) {
      addresses[j] =
          murmur64A(query.addresses.data[j], sizeof(db_address_t), HASH_SEED);
    }
  }

  uint64_t* topics[TOPICS_LENGTH] = {NULL};
  for (int i = 0; i < TOPICS_LENGTH; ++i) {
    topics[i] = (uint64_t*)calloc(query.topics[i].len, sizeof(uint64_t));

    for (size_t j = 0; j < query.topics[i].len; ++j) {
      has_topics = true;
      topics[i][j] =
          murmur64A(query.topics[i].data[j], sizeof(db_hash_t), HASH_SEED);
    }
  }

  // Get count
  uint64_t count = 0;
  for (size_t block_index = 0; block_index < db->blocks_count; ++block_index) {
    db_block_t* block = &(db->blocks[block_index]);

    if (block->number < query.from_block)
      continue;
    else if (block->number > query.to_block)
      break;

    if (!has_addresses && !has_topics) {
      count += block->logs_count;
      continue;
    }

    for (size_t start = block->offset, end = block->offset + block->logs_count;
         start < end; ++start) {
      bool address_match = !has_addresses, topics_match = !has_topics;

      for (size_t j = 0; j < query.addresses.len; ++j) {
        if (db->addresses[start] == addresses[j]) {
          address_match = true;
          break;
        }
      }

      if (!address_match)
        continue;

      if (has_topics) {
        topics_match = true;

        for (size_t j = 0; j < TOPICS_LENGTH; ++j) {
          if (query.topics[j].len > 0) {
            topics_match =
                topics_match &&
                _includes(db->topics[start][j], topics[j], query.topics[j].len);
          }
        }
      }

      if (!topics_match)
        continue;

      ++count;
    }
  }

  if (addresses != NULL)
    free(addresses);

  for (int i = 0; i < TOPICS_LENGTH; ++i)
    if (topics[i] != NULL)
      free(topics[i]);

  db_free_query(&query);

  return count;
}

void db_insert(db_t* db, size_t size, db_log_t* logs) {
  for (size_t i = 0; i < size; ++i) {
    db_block_t* last_block = &(db->blocks[db->blocks_count - 1]);

    // store is immutable, operation not supported
    if (last_block->number > logs[i].block_number) {
      errno = ERR_PREVIOUS_BLOCK;
      return;
    }

    // add new block
    if (last_block->number < logs[i].block_number) {
      if (db->blocks_capacity == db->blocks_count) {
        db->blocks_capacity += STORE_BLOCKS_RESERVE_STEP;

        if (ftruncate(db->blocks_fd,
                      db->blocks_capacity * sizeof(db_block_t)) != 0) {
          errno = ERR_FAILED_REALLOC_FILE;
          return;
        }
      }

      db->blocks[db->blocks_count].number = logs[i].block_number;
      db->blocks[db->blocks_count].logs_count = 0;
      db->blocks[db->blocks_count].offset =
          last_block->offset + last_block->logs_count;

      db->blocks_count++;
      last_block++;
    }

    if (db->logs_capacity == db->logs_count) {
      db->logs_capacity += STORE_LOGS_RESERVE_STEP;

      if (ftruncate(db->addresses_fd,
                    db->logs_capacity * sizeof(db_address_t)) != 0 ||
          ftruncate(db->topics_fd, db->logs_capacity * sizeof(db_hash_t) *
                                       TOPICS_LENGTH) != 0) {
        errno = ERR_FAILED_REALLOC_FILE;
        return;
      }
    }

    db->addresses[db->logs_count] =
        murmur64A(logs[i].address, sizeof(db_address_t), HASH_SEED);

    for (size_t j = 0; j < 4; ++j) {
      db->topics[db->logs_count][j] =
          murmur64A(logs[i].topics[j], sizeof(db_hash_t), HASH_SEED);
    }

    last_block->logs_count++;
    db->logs_count++;
  }

  _write_manifest(db);
}

uint64_t db_last_block(db_t* db) {
  return db->blocks[db->blocks_count - 1].number;
}

void db_status(db_t* db, char* buffer, size_t len) {
  snprintf(buffer, len,

           "dir: '%s'\n"
           "last block:  %" PRIu64
           "\n"
           "block_count: %" PRIu64
           "\n"
           "logs_count:  %" PRIu64 "\n",

           db->dir, db->blocks[db->blocks_count - 1].number, db->blocks_count,
           db->logs_count);
}
