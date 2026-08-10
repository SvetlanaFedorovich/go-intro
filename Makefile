# Движок compose. По умолчанию Podman; переопределяется, напр.:
#   make compose-up COMPOSE="docker compose"
#   make compose-up COMPOSE=podman-compose
COMPOSE ?= podman compose

.PHONY: compose-up compose-down run-api run-worker grafana-up observability-down load-test load-test-smoke load-test-clean test test-race test-integration tidy

compose-up:
	$(COMPOSE) up -d

compose-down:
	$(COMPOSE) down

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

grafana-up:
	$(COMPOSE) up -d --build grafana

observability-down:
	$(COMPOSE) stop grafana prometheus tempo

load-test:
	./loadtest/run.sh

load-test-smoke:
	RATE=100/s DURATION=5s TARGET_RPS=100 MIN_THROUGHPUT_RPS=95 ./loadtest/run.sh

load-test-clean:
	./loadtest/cleanup.sh

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	./scripts/test-integration.sh

tidy:
	go mod tidy
