# WARN: duplicated in liboracle.go
CFLAGS=-std=c17 -pthread -pedantic -march=native -ffast-math -fPIC -fvisibility=hidden \
       -Wall -Wextra -Wpedantic -Wnull-dereference -Wvla -Wshadow \
       -Wstrict-prototypes -Wwrite-strings -Wfloat-equal -Wconversion -Wdouble-promotion

CFLAGS+=-D_XOPEN_SOURCE=700 -D_GNU_SOURCE

# Find sources
ALL=$(wildcard *.c *.h)

HDR=$(filter %.h, $(ALL))
SRC=$(filter %.c, $(ALL))
OBJ=$(patsubst %.c,%.o,$(SRC))
DEP=$(patsubst %.c,%.d,$(SRC))

TESTS=$(wildcard test/*_test.c)

NODEPS:=clean
ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
    -include $(DEP)
endif

%.d: %.c
	$(CC) $(CFLAGS) -MM -MT '$(patsubst %.c,%.o,$<)' $< -MF $@

%.o: %.c %.d %.h
	$(CC) $(CFLAGS) -c -o $@ $<

liboracle.a: CFLAGS+=-O3 
liboracle.a: $(OBJ)
	ar rcs -o $@ $^

liboracle.so: CFLAGS+=-O3 -flto
liboracle.so: $(OBJ)
	$(CC) $(CFLAGS) -O3 -shared -static-libgcc -o $@ $<

.PHONY: test
libtest: CFLAGS+=-O0 -g3 -fsanitize=address,undefined
ifneq '' '$(findstring clang,$(shell $(CC) --version))'
libtest: CFLAGS+=-fsanitize-trap
endif
libtest: $(OBJ) $(TESTS)
	$(CC) $(CFLAGS) $(shell pkg-config --cflags --libs criterion) -o $@ $^
	./$@

.PHONY: clean
clean:
	rm -rf *.o *.d *.a *.so doracle/doracle libtest

.PHONY: format
format:
	cd go && go fmt ./...
	cd doracle && go fmt ./...
	clang-format -style=Chromium -i *.{c,h}

.PHONY: lint
lint:
	clang-tidy *.c -- $(CFLAGS)

.PHONY: doracle-build
doracle-build: liboracle.a
	$(MAKE) -C doracle build
