#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/loadtest/results}"
TARGETS="${TARGETS:-$ROOT_DIR/loadtest/targets.txt}"
RATE="${RATE:-5000/s}"
DURATION="${DURATION:-1m}"
WORKERS="${WORKERS:-256}"
MAX_WORKERS="${MAX_WORKERS:-10000}"
TIMEOUT="${TIMEOUT:-5s}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_BIN="$RESULTS_DIR/vegeta-$STAMP.bin"
RESULTS_JSON="$RESULTS_DIR/vegeta-$STAMP.json"
RESULTS_TXT="$RESULTS_DIR/vegeta-$STAMP.txt"
RESULTS_HTML="$RESULTS_DIR/vegeta-$STAMP.html"

if ! command -v vegeta >/dev/null 2>&1; then
	echo "vegeta is not installed; on macOS run: brew install vegeta" >&2
	exit 1
fi
if ! curl -fsS http://localhost:8080/readyz >/dev/null; then
	echo "API is not ready at http://localhost:8080/readyz; run: make grafana-up" >&2
	exit 1
fi

mkdir -p "$RESULTS_DIR"
cd "$ROOT_DIR"

echo "Running Vegeta: rate=$RATE duration=$DURATION workers=$WORKERS max_workers=$MAX_WORKERS"
vegeta attack \
	-name=go-intro-5000-rps \
	-targets="$TARGETS" \
	-rate="$RATE" \
	-duration="$DURATION" \
	-workers="$WORKERS" \
	-max-workers="$MAX_WORKERS" \
	-connections=10000 \
	-max-connections=10000 \
	-timeout="$TIMEOUT" \
	-max-body=1024 \
	-output="$RESULTS_BIN"

vegeta report "$RESULTS_BIN" | tee "$RESULTS_TXT"
vegeta report -type=json "$RESULTS_BIN" >"$RESULTS_JSON"
vegeta plot "$RESULTS_BIN" >"$RESULTS_HTML"

set +e
python3 "$ROOT_DIR/loadtest/check.py" "$RESULTS_JSON"
CHECK_STATUS=$?
set -e

echo "Binary results: $RESULTS_BIN"
echo "JSON report:    $RESULTS_JSON"
echo "HTML plot:      $RESULTS_HTML"
exit "$CHECK_STATUS"
