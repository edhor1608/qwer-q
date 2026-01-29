.PHONY: build test run clean lint

BINARY=bin/qwer-q

build:
	go build -o $(BINARY) ./cmd/qwer-q

test:
	go test -v ./...

run: build
	./$(BINARY)

clean:
	rm -rf bin/

lint:
	golangci-lint run

.DEFAULT_GOAL := build
