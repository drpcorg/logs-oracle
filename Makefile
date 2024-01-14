TYPE=Release
GENERATOR=Unix Makefiles

BUILD_DIR=build/$(TYPE)

all: cmake
cmake:
	mkdir -p $(BUILD_DIR)
	cmake -S . -B=$(BUILD_DIR) -G"$(GENERATOR)" -DCMAKE_BUILD_TYPE=$(TYPE) -DBUILD_SHARED_LIBS=On

%:
	cmake --build build/$@

# Doracle (go wrapper server)
.PHONY: doracle-dev
doracle-build:
	cd doracle && go build -gcflags "all=-N -l" .

.PHONY: doracle-dev
doracle-dev: doracle-build
	./doracle/doracle

# Java binding
JAVA_SRC=$(wildcard java/org/drpc/logsoracle/*.java)

java/LogsOracle.jar: $(JAVA_SRC)
	javac --source=20 --enable-preview $(JAVA_SRC:%='%')
	cd java && \
		jar cf $(@:java/%=%) org/drpc/logsoracle/*.class

SONAME := $(shell echo liblogsoracle-$(shell uname -m)-$(shell uname -s).so | tr '[:upper:]' '[:lower:]')

.PHONY: java-example
java-example: java/LogsOracle.jar
	cp $(BUILD_DIR)/liblogsoracle.so $(BUILD_DIR)/$(SONAME)
	jar uf java/LogsOracle.jar -C $(BUILD_DIR) $(SONAME)
	mkdir -p _data && java --source=20 --enable-preview -cp ".:java/LogsOracle.jar" java/Example.java

# Utils
.PHONY: jextract
jextract:
	jextract -t org.drpc.logsoracle --source --output jextract ./liboracle.h

.PHONY: clean
clean:
	rm -rf build doracle/doracle libtest java/LogsOracle.jar
	find . -type f -name '*.class' -exec rm -f {} \;

.PHONY: format
format:
	go fmt ./...
	clang-format -style=Chromium -i $$(find -type f -regex '.*\.\(h\|c\|java\)')

.PHONY: lint
lint:
	clang-tidy *.c -- $(CFLAGS)
