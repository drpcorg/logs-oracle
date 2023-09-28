#include <stdlib.h>
#include <unistd.h>

#include <criterion/criterion.h>

#include "../liboracle.h"

rcl_t* make_db(void) {
  char tmpl[] = "/tmp/tmpdir.XXXXXX";
  char* dir_name = mkdtemp(tmpl);

  if (dir_name == NULL) {
    perror("mkdtemp failed: ");
    return NULL;
  }

  return rcl_new(dir_name, 0);
}

Test(oracle, rcl_new) {
  rcl_t* db = make_db();
  cr_assert(db != NULL);

  rcl_free(db);
}
