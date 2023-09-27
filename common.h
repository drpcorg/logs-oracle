#ifndef _LIBORACLE_COMMON_H
#define _LIBORACLE_COMMON_H

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

// Reference: https://gcc.gnu.org/wiki/Visibility
#if defined _WIN32 || defined __CYGWIN__
#define RCL_EXPORT __declspec(dllexport)
#elif __GNUC__ >= 4
#define RCL_EXPORT __attribute__((visibility("default")))
#else
#define RCL_EXPORT
#endif

#endif  // _LIBORACLE_COMMON_H
