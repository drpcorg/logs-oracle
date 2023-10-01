#include "file.h"

int file_open(file_t* f, const char* filename, uint64_t init_size) {
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

  f->buffer = mmap(NULL, RCL_FILE_SIZE_RESERVE, flags, MAP_SHARED, f->fd, 0);

  if (f->buffer == MAP_FAILED)
    return -1;

  return 0;
}

/*
int file_lock(file_t* f) {
  return mlock2(f->buffer, f->bytes, MLOCK_ONFAULT) == 0 ? 0 : -1;
}

int file_unlock(file_t* f) {
  return munlock(f->buffer, f->bytes) == 0 ? 0 : -1;
}

int file_resize(file_t* f, size_t size) {
  if (size > RCL_FILE_SIZE_RESERVE) {
    return -1;
  }

  f->bytes = size;

  return ftruncate(f->fd, size) == 0 ? 0 : -1;
}
*/

int file_close(file_t* f) {
  munmap(f->buffer, RCL_FILE_SIZE_RESERVE);
  return close(f->fd);
}
