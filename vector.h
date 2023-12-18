#ifndef _RCL_VECTOR_H
#define _RCL_VECTOR_H

#include "common.h"

typedef struct {
  void* buffer;
  uint64_t size;
  uint64_t capacity;
  uint64_t item_size;
} vector_t;

bool vector_init(vector_t* vector, uint64_t capacity, uint64_t item_size);
void vector_destroy(vector_t* vector);

void* vector_add(vector_t* vector);
void vector_remove(vector_t* vector, void* item);

#define vector_at(vector, i) \
  rcl_pointer_to((vector)->buffer, (vector)->item_size*(i))

#define vector_last(vector) vector_at((vector), (vector)->size - 1)

#define vector_reset(vector) (vector)->size = 0

#define vector_is_empty(vector) ((vector)->size == 0)

rcl_inline void* vector_remove_last(vector_t* vector) {
  vector->size--;
  return rcl_pointer_to(vector->buffer, vector->item_size * vector->size);
}

#endif  // _RCL_VECTOR_H
