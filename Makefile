CC=clang
CFLAGS=-std=c11 -Wall -Wextra -Wpedantic \
       -Wvla -Wshadow -Wstrict-prototypes -Wwrite-strings -Wfloat-equal -Wconversion -Wdouble-promotion

CFLAGS+=-D_XOPEN_SOURCE=700 -D_GNU_SOURCE

liboracle.a: CFLAGS+=-O3 -march=native
liboracle.a: liboracle.o utils.o
	ar rcs -o $@ $^

%.o: %.c %.h
	$(CC) $(CFLAGS) -c -o $@ $<

.PHONY: test
test: CFLAGS+=-O0 -g3 -fsanitize=address,undefined -fsanitize-trap
test:
	$(CC) $(CFLAGS) $$(pkg-config --cflags --libs criterion) -o $@ utils.c liboracle.c liboracle_test.c
	./$@

.PHONY: clean
clean:
	rm -rf *.o *.a *.so doracle/doracle

.PHONY: format
format:
	go fmt ./...
	clang-format -style=Chromium -i **/*.{c,h}

.PHONY: doracle
doracle-build: liboracle.a
	$(MAKE) -C doracle build
