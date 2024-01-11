#include <limits.h>
#include <stdlib.h>
#include <time.h>

#include <criterion/criterion.h>
#include <criterion/new/assert.h>

#include "../liboracle.h"
#include "../vector.h"

static int mkdirp(char* path) {
  static const char tempchars[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  char *end = path + strlen(path), *start = end;
  for (; start > path && start[-1] == 'X'; start--)
    ;

  for (uint32_t tries = INT_MAX; tries != 0; --tries) {
    for (char* cp = start; cp != end; ++cp) {
      *cp = tempchars[rand() % (sizeof(tempchars) - 1)];
    }

    if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) == 0)
      return 0;

    if (errno != EEXIST)
      return -1;
  }

  return -1;
}

vector_t make_uint64_vector(size_t n, ...) {
  va_list args;
  va_start(args, n);

  vector_t v;
  vector_init(&v, n, sizeof(int64_t));

  for (size_t i = 0; i < n; ++i) {
    int64_t* it = (int64_t*)vector_add(&v);
    *it = va_arg(args, int64_t);
  }

  va_end(args);

  return v;
}

#define VA_NARGS(...) ((int)(sizeof((int[]){1, __VA_ARGS__}) / sizeof(int)) - 1)
#define v(...) \
  make_uint64_vector(VA_NARGS(__VA_ARGS__) __VA_OPT__(, __VA_ARGS__))

// Samples
static const char* addresses[] = {
    "0xe4e50b96f70aab13a2d7e654d07d7d4173319653",
    "0xe53ec727dbdeb9e2d5456c3be40cff031ab40a55",
    "0xe8da2e3d904e279220a86634aafa4d3be43c89d9",
    "0xe921401d18ed1ea4d64169d1576c32f9a7439694",
    "0xe9a1a323b4c8fd5ce6842edaa0cd8af943cbdf22",
    "0xeae6fd7d8c1740f3f1b03e9a5c35793cd260b9a6",
    "0xf151980e7a781481709e8195744bf2399fb3cba4",
    "0xf203ca1769ca8e9e8fe1da9d147db68b6c919817",
    "0xf411903cbc70a74d22900a5de66a2dda66507255",
    "0xf57e7e7c23978c3caec3c3548e3d615c346e79ff",
};

static const char* topics[] = {
    "0xa8dc30b66c6d4a8aac3d15925bfca09e42cac4a00c50f9949154b045088e2ac2",
    "0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9",
    "0xb84b9c38fdca745491d1f429e19a8e2f07a19bc7f6dffb0003c1abb7cb873509",
    "0xbb123b5c06d5408bbea3c4fef481578175cfb432e3b482c6186f02ed9086585b",
    "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b",
    "0xbd5c436f8c83379009c1962310b8347e561d1900906d3fe4075b1596f8955f88",
    "0xbeee1e6e7fe307ddcf84b0a16137a4430ad5e2480fc4f4a8e250ab56ccd7630d",
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
    "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9",
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
};

// Wrappers
rcl_log_t ml(int64_t number,
             const char* addr,
             const char* t1,
             const char* t2,
             const char* t3,
             const char* t4) {
  rcl_log_t log = {.block_number = number};

  hex2bin(log.address, addr, sizeof(rcl_address_t));
  if (t1)
    hex2bin(log.topics[0], t1, sizeof(rcl_hash_t));
  if (t2)
    hex2bin(log.topics[1], t2, sizeof(rcl_hash_t));
  if (t3)
    hex2bin(log.topics[2], t3, sizeof(rcl_hash_t));
  if (t4)
    hex2bin(log.topics[3], t4, sizeof(rcl_hash_t));

  return log;
}

rcl_t* db_make(void) {
  char tmpl[] = "/tmp/tmpdir.XXXXXX";

  if (mkdirp(tmpl) != 0) {
    perror("mkdirp failed");
    return NULL;
  }

  rcl_t* db = NULL;
  rcl_result status = rcl_open(tmpl, 0, &db);
  cr_assert(status == RCLE_OK, "Expected sucessful db connection");

  return db;
}

rcl_t* db_make_filled(void) {
  rcl_t* db = db_make();

  rcl_log_t s[] = {
      ml(0, addresses[2], NULL, NULL, NULL, NULL),
      ml(0, addresses[2], NULL, NULL, NULL, NULL),
      ml(0, addresses[6], topics[9], topics[0], topics[2], topics[2]),
      ml(3, addresses[8], NULL, NULL, NULL, NULL),
      ml(3, addresses[2], topics[9], NULL, NULL, NULL),
      ml(3, addresses[8], topics[5], topics[8], NULL, NULL),
      ml(3, addresses[8], NULL, NULL, NULL, NULL),
      ml(3, addresses[6], topics[5], topics[7], NULL, NULL),
      ml(4, addresses[1], NULL, NULL, NULL, NULL),
      ml(4, addresses[9], topics[8], topics[7], topics[3], NULL),
      ml(4, addresses[1], topics[0], topics[3], topics[5], topics[4]),
      ml(4, addresses[2], topics[2], topics[1], topics[0], topics[7]),
      ml(5, addresses[7], topics[3], topics[5], NULL, NULL),
      ml(5, addresses[1], topics[1], topics[2], topics[9], NULL),
      ml(5, addresses[1], topics[0], topics[2], topics[3], topics[8]),
      ml(5, addresses[3], topics[2], NULL, NULL, NULL),
      ml(5, addresses[6], NULL, NULL, NULL, NULL),
      ml(5, addresses[0], NULL, NULL, NULL, NULL),
      ml(5, addresses[4], topics[4], topics[2], NULL, NULL),
      ml(6, addresses[4], topics[9], topics[8], NULL, NULL),
  };

  rcl_result err = rcl_insert(db, sizeof(s) / sizeof(s[0]), s);
  cr_expect(err == RCLE_OK, "Expected sucessfull insert");

  int64_t blocks;
  rcl_result result = rcl_blocks_count(db, &blocks);
  cr_expect(result == RCLE_OK && blocks == 7,
            "Expected 7 logs after insert test suite");

  return db;
}

int64_t db_make_query(rcl_t* db,
                      int64_t from,
                      int64_t to,
                      vector_t ad,
                      vector_t tpcs[TOPICS_LENGTH]) {
  size_t tlen[TOPICS_LENGTH] = {0};
  for (size_t i = 0; i < TOPICS_LENGTH; ++i)
    tlen[i] = tpcs[i].size;

  rcl_query_t* q = NULL;
  cr_expect(rcl_query_alloc(&q, ad.size, tlen) == RCLE_OK,
            "Couldn't create query");

  q->from = from;
  q->to = to;

  for (size_t i = 0; i < ad.size; ++i) {
    size_t k = *(int64_t*)vector_at(&ad, i);
    q->address[i].encoded = addresses[k];
  }

  for (size_t i = 0; i < TOPICS_LENGTH; ++i) {
    if (tpcs[i].size == 0)
      continue;

    for (size_t j = 0; j < tpcs[i].size; ++j) {
      int64_t k = *(int64_t*)vector_at(&tpcs[i], j);
      q->topics[i][j].encoded = topics[k];
    }
  }

  // Exec query
  int64_t actual;
  rcl_result result = rcl_query(db, q, &actual);
  cr_expect(result == RCLE_OK, "Expected sucessful query");

  // Clenup
  rcl_query_free(q);

  for (size_t i = 0; i < TOPICS_LENGTH; ++i)
    vector_destroy(&tpcs[i]);
  vector_destroy(&ad);

  return actual;
}

#define expect_query(expected, from, to, address, t1, t2, t3, t4)             \
  do {                                                                        \
    vector_t tpcs[TOPICS_LENGTH] = {t1, t2, t3, t4};                          \
    cr_expect(eq(u64, expected, db_make_query(db, from, to, address, tpcs))); \
  } while (0);

// Tests
Test(liboracle, New) {
  rcl_t* db = db_make();

  int64_t logs;
  rcl_result r1 = rcl_logs_count(db, &logs);
  cr_expect(r1 == RCLE_OK && logs == 0, "Expected 0 blocks in new DB");

  int64_t blocks;
  rcl_result r2 = rcl_blocks_count(db, &blocks);
  cr_expect(r2 == RCLE_OK && blocks == 0, "Expected 0 logs in new DB");

  rcl_free(db);
}

Test(liboracle, EmptyInsert) {
  rcl_t* db = db_make();
  cr_assert(db != NULL, "Expected db connection is a NULL");

  rcl_result err = rcl_insert(db, 0, NULL);
  cr_expect(err == RCLE_OK, "Expected sucessfull insert");

  int64_t blocks;
  rcl_result r = rcl_blocks_count(db, &blocks);
  cr_expect(r == RCLE_OK && blocks == 0, "Expected 0 logs in new DB");

  rcl_free(db);
}

Test(liboracle, QueryFullScan) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 20,
               /* from, to */ 0, 6,
               /* address */ v(),
               /* topics */ v(), v(), v(), v());
  rcl_free(db);
}

Test(liboracle, QuerySegmentTooLarge) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 20,
               /* from, to */ 0, 42,
               /* address */ v(),
               /* topics */ v(), v(), v(), v());
  rcl_free(db);
}

Test(liboracle, QueryOneBlock) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 1,
               /* from, to */ 6, 6,
               /* address */ v(),
               /* topics */ v(), v(), v(), v());
  expect_query(/* expected */ 7,
               /* from, to */ 5, 5,
               /* address */ v(),
               /* topics */ v(), v(), v(), v());
  rcl_free(db);
}

Test(liboracle, QuerySegment) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 9,
               /* from, to */ 2, 4,
               /* address */ v(),
               /* topics */ v(), v(), v(), v());
  rcl_free(db);
}

Test(liboracle, QueryAddress) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 2,
               /* from, to */ 0, 6,
               /* address */ v(4),
               /* topics */ v(), v(), v(), v());
  expect_query(/* expected */ 3,
               /* from, to */ 0, 6,
               /* address */ v(3, 4),
               /* topics */ v(), v(), v(), v());
  rcl_free(db);
}

Test(liboracle, QueryTopics) {
  rcl_t* db = db_make_filled();
  expect_query(/* expected */ 2,
               /* from, to */ 0, 6,
               /* address */ v(),
               /* topics */ v(), v(), v(3), v());
  rcl_free(db);
}
