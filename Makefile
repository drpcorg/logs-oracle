CFLAGS+=-std=gnu11 -mtune=generic -pipe -pthread

CFLAGS+=$(shell pkg-config --cflags libcurl libcjson)
LDFLAGS+=$(shell pkg-config --libs libcurl libcjson)

CWARN=-Wall -Wextra -Wpedantic -Wnull-dereference -Wvla -Wshadow\
      -Wstrict-prototypes -Wwrite-strings -Wfloat-equal -Wconversion -Wdouble-promotion

DEBUG=

ifdef DEBUG
	CFLAGS+=-O0 -g3 -gdwarf-4 -DDEBUG -DNDEBUG
else
	CFLAGS+=-O3
endif

ifdef ASAN
	CFLAGS+=-fsanitize=address
	LDFLAGS+=-fsanitize=address
endif
ifdef MSAN
	CFLAGS+=-fsanitize=memory
	LDFLAGS+=-fsanitize=memory
endif
ifdef UBSAN
	CFLAGS+=-fsanitize=undefined
	LDFLAGS+=-fsanitize=undefined
endif

# C shared lib
ALL=$(wildcard *.c *.h)

HDR=$(filter %.h, $(ALL))
SRC=$(filter %.c, $(ALL))

liboracle.so: CFLAGS+=-fPIC -fvisibility=hidden -ffast-math $(CWARN)
liboracle.so: $(SRC) $(HDR)
	$(CC) $(CFLAGS) $(LDFLAGS) -shared -o $@ $(SRC)

# Unit tests
.PHONY: libtest

TESTS=$(wildcard test/*_test.c)

libtest: CFLAGS+=$(shell pkg-config --cflags criterion) -std=gnu2x
libtest: LDFLAGS+=$(shell pkg-config --libs criterion) 
libtest: $(SRC) $(HDR) $(TESTS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRC) $(TESTS)
	./$@

# Doracle (go wrapper server)
.PHONY: doracle-build doracle-dev

doracle-build: export CGO_CFLAGS=$(CFLAGS)
doracle-build: export CGO_LDFLAGS=$(LDFLAGS)
doracle-build:
	cd doracle && go build -gcflags "all=-N -l" .

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
	find . -type f -regex '.*\.\(o\|d\|a\|so\|class\)' -exec rm -f {} \;
	rm -f doracle/doracle libtest java/LogsOracle.jar

format:
	go fmt ./...
	clang-format -style=Chromium -i $$(find -type f -regex '.*\.\(h\|c\)')

lint:
	clang-tidy *.c -- $(CFLAGS)
