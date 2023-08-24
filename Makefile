release:
	go build .

format:
	go fmt ./...
	clang-format -style=Chromium -i **/*.{c,h}
