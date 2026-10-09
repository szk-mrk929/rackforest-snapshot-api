.PHONY: test test-race run build generate fmt docker-up docker-up-tmp docker-down docker-test

GO ?= go
BINARY ?= rackforest-snapshot-api

test: Run the tests
	go test ./src/...

test-race: Run the tests in race mode
	go test -race ./src/...

run: Run the binary
	go run ./src/main.go

build: Build the binary
	go build -trimpath -o bin/$(BINARY) ./src/main.go

generate: Generate the code
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen --config api/oapi-codegen.yaml api/openapi.yaml
	gofmt -w src/api/models.gen.go

fmt: Format the code
	gofmt -w .

docker-up: Run a proper container
	docker compose up -d --build --force-recreate

docker-up-tmp: Run a temporary container for testing
	docker compose up --build --force-recreate --remove-orphans --abort-on-container-exit --rmi all

docker-down: Bring down the container
	docker compose down --volumes --rmi all

docker-test: Run the test container
	docker compose --profile test run --rm --build test
