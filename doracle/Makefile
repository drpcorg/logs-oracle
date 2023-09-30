.PHONY: build
build:
	go build .

.PHONY: build-dev
build-dev: export CGO_CFLAGS=-fsanitize=address,undefined -O0 -g
build-dev: export CGO_LDFLAGS=-fsanitize=address,undefined 
build-dev:
	go build -gcflags "all=-N -l -L" .

.PHONY: dev
run-dev: build-dev
	./doracle
