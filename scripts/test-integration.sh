#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
	set -a
	# shellcheck disable=SC1091
	source .env
	set +a
fi

POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-pass}"
POSTGRES_DB="${POSTGRES_DB:-test}"
TEST_POSTGRES_DSN="${TEST_POSTGRES_DSN:-postgres://$POSTGRES_USER:$POSTGRES_PASSWORD@localhost:5432/$POSTGRES_DB?sslmode=disable}"
E2E_BASE_URL="${E2E_BASE_URL:-http://localhost:8080}"
# Движок контейнеров переопределяется:
#   CONTAINER_ENGINE=docker COMPOSE_CMD="docker compose" ./scripts/test-integration.sh
CONTAINER_ENGINE="${CONTAINER_ENGINE:-podman}"
read -r -a COMPOSE_CMD <<< "${COMPOSE_CMD:-podman compose}"
COMPOSE=("${COMPOSE_CMD[@]}" -f compose.yml -f compose.integration.yml)

cleanup() {
	"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d postgres kafka tempo

for _ in $(seq 1 60); do
	if "$CONTAINER_ENGINE" exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
"$CONTAINER_ENGINE" exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null

for _ in $(seq 1 60); do
	if "$CONTAINER_ENGINE" exec kafka kafka-topics --list --bootstrap-server localhost:9092 >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
"$CONTAINER_ENGINE" exec kafka kafka-topics --list --bootstrap-server localhost:9092 >/dev/null

TEST_KAFKA_BROKERS="${TEST_KAFKA_BROKERS:-localhost:9892}" \
	go test -count=1 -race -tags=integration ./internal/kafka

"$CONTAINER_ENGINE" exec -i postgres psql \
	-v ON_ERROR_STOP=1 \
	-U "$POSTGRES_USER" \
	-d "$POSTGRES_DB" \
	< migrations/0001_up_schema.sql

TEST_POSTGRES_DSN="$TEST_POSTGRES_DSN" \
	go test -count=1 -race -tags=integration ./internal/store

# Store tests intentionally reset the destructive schema; leave a clean final
# schema before starting applications for the end-to-end test.
"$CONTAINER_ENGINE" exec -i postgres psql \
	-v ON_ERROR_STOP=1 \
	-U "$POSTGRES_USER" \
	-d "$POSTGRES_DB" \
	< migrations/0001_up_schema.sql

"${COMPOSE[@]}" up -d --build worker api
for _ in $(seq 1 60); do
	if curl -fsS "$E2E_BASE_URL/readyz" >/dev/null 2>&1 &&
		curl -fsS http://localhost:8081/readyz >/dev/null 2>&1; then
		break
	fi
	sleep 1
done
curl -fsS "$E2E_BASE_URL/readyz" >/dev/null
curl -fsS http://localhost:8081/readyz >/dev/null

TEST_POSTGRES_DSN="$TEST_POSTGRES_DSN" \
	E2E_BASE_URL="$E2E_BASE_URL" \
	go test -count=1 -race -tags=integration ./e2e
