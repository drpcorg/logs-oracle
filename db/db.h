#ifndef _DB_H
#define _DB_H

#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdbool.h>

#include "murmur2.h"

#define MAX_FILE_LENGTH    256

#define HASH_LENGTH    32
#define ADDRESS_LENGTH 20

#define TOPICS_LENGTH 4

typedef uint8_t db_hash_t[HASH_LENGTH];
typedef uint8_t db_address_t[ADDRESS_LENGTH];

typedef struct {
  uint64_t     block_number;
  db_address_t address;
  db_hash_t    topics[TOPICS_LENGTH];
} db_log_t;

typedef struct {
  uint64_t from_block, to_block;

  struct {
    size_t len;
    db_address_t* data;
  } addresses;

  struct {
    size_t len;
    db_hash_t* data;
  } topics[TOPICS_LENGTH];
} db_query_t;

struct db;
typedef struct db db_t;

db_t*    db_new   (char *dir);
void     db_close (db_t *db);
void     db_status(db_t *db, char *buffer, size_t len);

uint64_t db_query     (db_t *db, db_query_t query);
void     db_insert    (db_t *db, size_t size, db_log_t *logs);

uint64_t db_last_block(db_t *db);


#endif  // _DB_H
