.PHONY: build test run clean lint docker-up docker-down docker-clean docker-restart bench-test bench bench-all

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

# Docker commands
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-clean:
	docker compose down -v
	@echo "Cleaned: containers stopped, volumes removed"

docker-restart:
	docker compose down -v
	docker compose up -d --build
	@echo "Fresh restart complete"

# Benchmark (runs against fresh container)
bench-test:
	cd bench && go test -race ./...

bench:
	docker compose down -v
	docker compose up -d --build
	@sleep 3
	cd bench && go run ./cmd/stress --queues=qwerq --tests=sustained --duration=30s

bench-all:
	docker compose down -v
	docker compose up -d --build
	@sleep 3
	cd bench && go run ./cmd/stress --queues=qwerq --tests=all --duration=30s

.DEFAULT_GOAL := build
