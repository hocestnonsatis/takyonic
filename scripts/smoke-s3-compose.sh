#!/usr/bin/env bash
# Faz D4 — MinIO + S3-backed takyonic-server smoke.
#
# Default path (fast): compose MinIO + local `cargo build --features s3` server.
# Optional: SMOKE_COMPOSE_NODE=1 builds/runs compose `node-s3` instead.
#
# Usage:
#   ./scripts/smoke-s3-compose.sh
#   SMOKE_COMPOSE_NODE=1 ./scripts/smoke-s3-compose.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_PORT="${SMOKE_PG_PORT:-15436}"
S3_ENDPOINT="${TAKYONIC_S3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${TAKYONIC_S3_BUCKET:-takyonic-smoke-$(date +%s)}"
ACCESS="${TAKYONIC_S3_ACCESS_KEY:-minioadmin}"
SECRET="${TAKYONIC_S3_SECRET_KEY:-minioadmin}"
DATA_ROOT="${SMOKE_DATA_ROOT:-$(mktemp -d /tmp/takyonic-s3-smoke-XXXXXX)}"
SERVER_LOG="${DATA_ROOT}/server.log"
SERVER_PID=""
COMPOSE_UP=0

pg_ready() {
  # Takyonic requires FROM for SELECT; empty-table scan is enough for readiness.
  PGPASSWORD=password psql -h 127.0.0.1 -p "$1" -U postgres -d postgres \
    -c 'SELECT * FROM users LIMIT 1' >/dev/null 2>&1
}

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ "${COMPOSE_UP}" -eq 1 ]]; then
    if [[ "${SMOKE_COMPOSE_NODE:-0}" == "1" ]]; then
      docker compose --profile s3 down -v >/dev/null 2>&1 || true
    else
      docker compose --profile minio down -v >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

echo "== D4 S3 smoke =="
echo "  data root : ${DATA_ROOT}"
echo "  s3        : ${S3_ENDPOINT} / ${BUCKET}"

docker compose --profile minio up -d minio
COMPOSE_UP=1

echo "waiting for MinIO health…"
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:9000/minio/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "http://127.0.0.1:9000/minio/health/live" >/dev/null \
  || { echo "MinIO did not become healthy"; exit 1; }

if [[ "${SMOKE_COMPOSE_NODE:-0}" == "1" ]]; then
  echo "starting compose node-s3 (docker build may take a while)…"
  docker compose --profile s3 up -d --build node-s3 minio
  PG_PORT=5436
  echo "waiting for pgwire on :${PG_PORT}…"
  for _ in $(seq 1 120); do
    if pg_ready "${PG_PORT}"; then
      break
    fi
    sleep 2
  done
else
  echo "building/running local takyonic-server --features s3…"
  cargo build --features s3 --bin takyonic-server >/dev/null
  ./target/debug/takyonic-server \
    --node-id 1 \
    --data-dir "${DATA_ROOT}/data" \
    --pg-port "${PG_PORT}" \
    --raft-port "$((PG_PORT + 1000))" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${BUCKET}" \
    --s3-access-key "${ACCESS}" \
    --s3-secret-key "${SECRET}" \
    >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!

  echo "waiting for pgwire on :${PG_PORT}…"
  for _ in $(seq 1 90); do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "server exited early; log:"
      tail -n 80 "${SERVER_LOG}" || true
      exit 1
    fi
    if grep -q 'PostgreSQL wire server' "${SERVER_LOG}" 2>/dev/null && pg_ready "${PG_PORT}"; then
      break
    fi
    sleep 1
  done
fi

pg_ready "${PG_PORT}" \
  || { echo "pgwire not ready on :${PG_PORT}"; tail -n 40 "${SERVER_LOG}" 2>/dev/null || true; exit 1; }

PGPASSWORD=password psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO users (id, name, city, status)
VALUES (4242, 'S3Smoke', 'MinIO', 'active');
SELECT id, name, city, status FROM users WHERE id = 4242;
SQL

echo "D4 S3 smoke PASS (pgwire :${PG_PORT}, bucket ${BUCKET})"
