# WARN: duplicated in liboracle.go
CFLAGS=-std=c11 -pthread -march=native -ffast-math -fvisibility=hidden \
       -Wall -Wextra -Wpedantic -Wnull-dereference -Wvla -Wshadow \
       -Wstrict-prototypes -Wwrite-strings -Wfloat-equal -Wconversion -Wdouble-promotion

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

liboracle.so: CFLAGS+=-O3 -flto -fPIC 
liboracle.so: $(OBJ)
	$(CC) $(CFLAGS) -shared -o $@ $^

.PHONY: libtest
# libtest: CFLAGS+=-O0 -g3 -fsanitize=address,undefined
# ifneq '' '$(findstring clang,$(shell $(CC) --version))'
# libtest: CFLAGS+=-fsanitize-trap
# endif
libtest: CFLAGS+=-std=c2x -O0 -g3
libtest: $(OBJ) $(TESTS)
	$(CC) $(CFLAGS) $(shell pkg-config --cflags --libs criterion) -o $@ $^
	./$@

.PHONY: clean
clean:
	rm -rf *.class *.jar *.o *.d *.a *.so doracle/doracle libtest

.PHONY: format
format:
	go fmt ./...
	clang-format -style=Chromium -i $$(find -type f -regex '.*\.\(h\|c\|java\)')

.PHONY: lint
lint:
	clang-tidy *.c -- $(CFLAGS)


.PHONY: doracle-build
doracle-build:
	$(MAKE) -C doracle build

.PHONY: doracle-dev
doracle-dev: liboracle.so
	$(MAKE) -C doracle run-dev

# TODO: use gradle for build java lib
# Java binding
JAVA_SRC=$(wildcard java/org/drpc/logsoracle/*.java)
JAVA_CLASS=$(patsubst %.java,%.class,$(JAVA_SRC))

.PHONY: jextract
jextract:
	jextract -t org.drpc.logsoracle @java/jextract-includes.txt --source --output java ./liboracle.h

java/LogsOracle.jar: $(JAVA_SRC)
	javac --source=20 --enable-preview $(JAVA_SRC:%='%')
	cd java && \
		jar cf $(@:java/%=%) $(JAVA_CLASS:java/%='%')

.PHONY: java-example
java-example: liboracle.so java/LogsOracle.jar
	cp liboracle.so liboracle-$(shell uname -m)-linux.so
	jar uf java/LogsOracle.jar liboracle-$(shell uname -m)-linux.so
	java --source=20 --enable-preview -cp ".:java/LogsOracle.jar" java/Example.java
