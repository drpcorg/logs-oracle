release:
	CC=clang CGO_CFLAGS='-Ofast' go build .

format:
	go fmt ./...
	clang-format -style=Chromium -i **/*.{c,h}
