#!/usr/bin/env bash
# Faz 2 — ORM-ish smoke: typed columns Describe path via psql + COPY STDIN/STDOUT.
#
# Usage:
#   ./scripts/smoke-orm-types.sh
# Optional: SMOKE_PG_PORT=15437
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_PORT="${SMOKE_PG_PORT:-15437}"
DATA_ROOT="$(mktemp -d /tmp/takyonic-orm-smoke-XXXXXX)"
SERVER_LOG="${DATA_ROOT}/server.log"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_ROOT}"
}
trap cleanup EXIT

echo "== Faz 2 ORM types / COPY smoke =="
cargo build --bin takyonic-server >/dev/null
./target/debug/takyonic-server \
  --node-id 1 \
  --data-dir "${DATA_ROOT}/data" \
  --pg-port "${PG_PORT}" \
  --no-demo-bootstrap \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

echo "waiting for pgwire on :${PG_PORT}…"
for _ in $(seq 1 60); do
  if PGPASSWORD=password psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -d postgres \
      -c 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  # Takyonic may require FROM — try empty catalog probe
  if PGPASSWORD=password psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -d postgres \
      -c "SELECT current_user" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

export PGPASSWORD=password
PSQL=(psql -h 127.0.0.1 -p "${PG_PORT}" -U postgres -d postgres -v ON_ERROR_STOP=1)

"${PSQL[@]}" -c "
CREATE TABLE orm_smoke (
  id UUID PRIMARY KEY,
  blob BYTEA,
  amount NUMERIC,
  ts TIMESTAMPTZ,
  name TEXT NOT NULL DEFAULT 'x'
);
"

"${PSQL[@]}" -c "
SELECT column_name, data_type, udt_name, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'orm_smoke'
ORDER BY ordinal_position;
" | tee "${DATA_ROOT}/cols.txt"

grep -q uuid "${DATA_ROOT}/cols.txt"
grep -q bytea "${DATA_ROOT}/cols.txt"
grep -q numeric "${DATA_ROOT}/cols.txt"
grep -q timestamptz "${DATA_ROOT}/cols.txt"

"${PSQL[@]}" -c "
INSERT INTO orm_smoke (id, blob, amount, ts) VALUES
  ('550e8400-e29b-41d4-a716-446655440000', '\\xDEAD', 12.5, '2026-08-07 12:00:00+00');
"

# COPY FROM STDIN / TO STDOUT round-trip
"${PSQL[@]}" <<'SQL'
COPY orm_smoke (id, blob, amount, ts, name) FROM STDIN;
551e8400-e29b-41d4-a716-446655440001	\\x00FF	1	2026-08-07 13:00:00+00	Ada
\.
SQL

OUT="$("${PSQL[@]}" -c "COPY orm_smoke TO STDOUT")"
echo "${OUT}" | tee "${DATA_ROOT}/copy.out"
echo "${OUT}" | grep -q Ada

echo "PASS: ORM types + COPY STDIN/STDOUT smoke"
