.PHONY: test test-race run build

BINARY := rackforest-snapshot-api

test:
	go test ./src/...

test-race:
	go test -race ./src/...

run:
	go run ./src/main.go

build:
	go build -trimpath -o bin/$(BINARY) ./src/main.go
