CFLAGS=-std=c11 -pthread -march=native -ffast-math -fvisibility=hidden \
       -Wall -Wextra -Wpedantic -Wnull-dereference -Wvla -Wshadow \
       -Wstrict-prototypes -Wwrite-strings -Wfloat-equal -Wconversion -Wdouble-promotion

CFLAGS=$(shell pkg-config --cflags --libs libzstd libcurl)

# Find sources
ALL=$(wildcard *.c *.h)

HDR=$(filter %.h, $(ALL))
SRC=$(filter %.c, $(ALL))

liboracle.so: CFLAGS+=-O3 -fPIC 
liboracle.so: $(SRC) $(HDR)
	$(CC) $(CFLAGS) -shared -o $@ $(SRC)

# Unit tests
.PHONY: libtest

TESTS=$(wildcard test/*_test.c)

# -fsanitize=address,undefined
libtest: CFLAGS+=-std=c2x -O0 -g3
libtest: $(SRC) $(HDR) $(TESTS)
	$(CC) $(CFLAGS) $(shell pkg-config --cflags --libs criterion) -o $@ $(SRC) $(TESTS)
	./$@

# Doracle (go wrapper server)
.PHONY: doracle-build doracle-dev

doracle-build:
	cd doracle && go build -gcflags "all=-N -l -L" .

doracle-dev: export CGO_CFLAGS=-fsanitize=address,undefined -O0 -g
doracle-dev: export CGO_LDFLAGS=-fsanitize=address,undefined 
doracle-dev: doracle-build
	./doracle/doracle

# Java binding
.PHONY: java-example jextract

JAVA_SRC=$(wildcard java/org/drpc/logsoracle/*.java)
JAVA_CLASS=$(patsubst %.java,%.class,$(JAVA_SRC))

jextract:
	jextract -t org.drpc.logsoracle @java/jextract-includes.txt --source --output java ./liboracle.h

java/LogsOracle.jar: $(JAVA_SRC)
	javac --source=20 --enable-preview $(JAVA_SRC:%='%')
	cd java && \
		jar cf $(@:java/%=%) $(JAVA_CLASS:java/%='%')

java-example: liboracle.so java/LogsOracle.jar
	cp liboracle.so liboracle-$(shell uname -m)-linux.so
	jar uf java/LogsOracle.jar liboracle-$(shell uname -m)-linux.so
	java --source=20 --enable-preview -cp ".:java/LogsOracle.jar" java/Example.java

# Utils
.PHONY: clean format lint

clean:
	find -type f -regex '.*\.\(o\|d\|a\|so\|class\)' -exec rm -f {} \;

format:
	go fmt ./...
	clang-format -style=Chromium -i $$(find -type f -regex '.*\.\(h\|c\)')

lint:
	clang-tidy *.c -- $(CFLAGS)
