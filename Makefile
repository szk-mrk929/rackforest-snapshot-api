.PHONY: test test-race run build up down check compose-test generate fmt

GO ?= go
BINARY ?= rackforest-snapshot-api

test:
	go test ./src/...

test-race:
	go test -race ./src/...

run:
	go run ./src/main.go

build:
	go build -trimpath -o bin/$(BINARY) ./src/main.go

generate:
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen --config api/oapi-codegen.yaml api/openapi.yaml
	gofmt -w src/api/models.gen.go

fmt:
	gofmt -w .

# One command: test the module, then build and start the compose stack.
check: test
	docker compose up --build

up:
	docker compose up --build

down:
	docker compose down --volumes --rmi all

compose-test:
	docker compose --profile test run --rm --build test
