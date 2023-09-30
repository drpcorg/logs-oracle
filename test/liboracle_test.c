#include <limits.h>
#include <stdlib.h>
#include <time.h>

#include <criterion/criterion.h>

#include "../liboracle.h"

static const char tempchars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

static uint32_t randseed = 0;
static uint32_t xorshift32(void) {
  if (randseed == 0)
    randseed = (uint32_t)time(NULL);

  uint32_t x = randseed;

  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;

  return randseed = x;
}

static int mkdirp(char* path) {
  char *end = path + strlen(path), *start = end;
  for (; start > path && start[-1] == 'X'; start--)
    ;

  for (uint32_t tries = INT_MAX; tries != 0; --tries) {
    for (char* cp = start; cp != end; ++cp) {
      *cp = tempchars[xorshift32() % (sizeof(tempchars) - 1)];
    }

    if (mkdir(path, S_IRUSR | S_IWUSR | S_IXUSR) == 0)
      return 0;

    if (errno != EEXIST)
      return -1;
  }

  return -1;
}

rcl_t* make_db(void) {
  char tmpl[] = "/tmp/tmpdir.XXXXXX";

  if (mkdirp(tmpl) != 0) {
    perror("mkdirp failed");
    return NULL;
  }

  return rcl_new(tmpl, 0);
}

Test(oracle, rcl_new) {
  rcl_t* db = make_db();
  cr_assert(db != NULL);

  rcl_free(db);
}
