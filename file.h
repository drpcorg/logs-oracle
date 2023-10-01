#ifndef _RCL_FILE_H
#define _RCL_FILE_H

#include "common.h"

typedef struct {
  int fd;
  size_t bytes;
  void* buffer;
} file_t;

#define RCL_FILE_SIZE_RESERVE (2ul << 36)  // 128GB addresses per file

int file_open(file_t* f, const char* filename, uint64_t init_size);
int file_close(file_t* f);

/*
int file_lock(file_t* f);
int file_unlock(file_t* f);
int file_resize(file_t* f, size_t size);
*/


#endif  // _RCL_FILE_H
