#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/compose.local.yaml")
GATEWAY_LOG="$(mktemp)"
OBSERVER_LOG="$(mktemp)"

export SIGNAL_GATEWAY_CONFIG="${SIGNAL_GATEWAY_CONFIG:-$ROOT/configs/example.yaml}"
export SIGNAL_GATEWAY_ADDR="${SIGNAL_GATEWAY_ADDR:-127.0.0.1:18080}"
export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
export SIGNAL_GATEWAY_MANUAL_TOKEN="${SIGNAL_GATEWAY_MANUAL_TOKEN:-local-smoke-token}"
export SIGNAL_OBSERVER_DURABLE="${SIGNAL_OBSERVER_DURABLE:-signal-observer-smoke-$$}"
export SIGNAL_OBSERVER_ONCE=true
export GOCACHE="${GOCACHE:-$ROOT/.gocache}"
export GOMODCACHE="${GOMODCACHE:-$ROOT/.gomodcache}"

gateway_pid=""
observer_pid=""

cleanup() {
  if [[ -n "$gateway_pid" ]]; then
    kill "$gateway_pid" >/dev/null 2>&1 || true
    wait "$gateway_pid" 2>/dev/null || true
  fi
  if [[ -n "$observer_pid" ]]; then
    kill "$observer_pid" >/dev/null 2>&1 || true
    wait "$observer_pid" 2>/dev/null || true
  fi
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  rm -f "$GATEWAY_LOG" "$OBSERVER_LOG"
}
trap cleanup EXIT

"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d nats

(
  cd "$ROOT"
  go run ./cmd/signal-gateway
) >"$GATEWAY_LOG" 2>&1 &
gateway_pid=$!

for _ in {1..60}; do
  if curl -fsS "http://127.0.0.1:18080/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "http://127.0.0.1:18080/readyz" >/dev/null

(
  cd "$ROOT"
  go run ./cmd/signal-observer
) >"$OBSERVER_LOG" 2>&1 &
observer_pid=$!

sleep 1

curl -fsS \
  -X POST "http://127.0.0.1:18080/manual" \
  -H "content-type: application/json" \
  -H "authorization: Bearer $SIGNAL_GATEWAY_MANUAL_TOKEN" \
  -H "x-signal-event: smoke-test" \
  -H "x-signal-action: created" \
  -d '{"message":"hello from local smoke","kind":"manual"}' >/dev/null

deadline=$((SECONDS + 30))
while kill -0 "$observer_pid" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "observer did not receive a signal before timeout" >&2
    echo "--- gateway log ---" >&2
    cat "$GATEWAY_LOG" >&2
    echo "--- observer log ---" >&2
    cat "$OBSERVER_LOG" >&2
    exit 1
  fi
  sleep 0.25
done

wait "$observer_pid"
observer_pid=""

if ! grep -q '"msg":"received signal"' "$OBSERVER_LOG"; then
  echo "observer exited without received signal log" >&2
  cat "$OBSERVER_LOG" >&2
  exit 1
fi

cat "$OBSERVER_LOG"
