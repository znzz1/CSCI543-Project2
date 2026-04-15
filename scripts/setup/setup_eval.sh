#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PG_BIN="${PG_BIN:-${ROOT_DIR}/install/bin}"
DB_NAME="evaldb"
ROWS="100000"
KEY_MAX="1000000"
VALUE_MAX="1000000"
SEED="0.424242"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-bin)
      PG_BIN="$2"
      shift 2
      ;;
    --db)
      DB_NAME="$2"
      shift 2
      ;;
    --rows)
      ROWS="$2"
      shift 2
      ;;
    --key-max)
      KEY_MAX="$2"
      shift 2
      ;;
    --value-max)
      VALUE_MAX="$2"
      shift 2
      ;;
    --seed)
      SEED="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -x "${PG_BIN}/psql" ]]; then
  if [[ -x "${ROOT_DIR}/install/bin/psql" ]]; then
    PG_BIN="${ROOT_DIR}/install/bin"
  elif [[ -x "${ROOT_DIR}/bin/psql" ]]; then
    PG_BIN="${ROOT_DIR}/bin"
  elif command -v psql >/dev/null 2>&1; then
    PG_BIN="$(cd "$(dirname "$(command -v psql)")" && pwd)"
  elif [[ -x "${ROOT_DIR}/postgresql/bin/psql" ]]; then
    PG_BIN="${ROOT_DIR}/postgresql/bin"
  else
    echo "Cannot find PostgreSQL binaries. Please pass --pg-bin <path-to-bin>." >&2
    exit 1
  fi
fi

PSQL="${PG_BIN}/psql"
CREATEDB="${PG_BIN}/createdb"
DROPDB="${PG_BIN}/dropdb"

echo "[setup] rebuilding database '${DB_NAME}'"
"${DROPDB}" --if-exists "${DB_NAME}" >/dev/null 2>&1 || true
"${CREATEDB}" "${DB_NAME}"

echo "[setup] creating tables and loading ${ROWS} rows"
"${PSQL}" "${DB_NAME}" <<SQL
DROP TABLE IF EXISTS eval_bw, eval_bt;

CREATE TABLE eval_bw(
  id bigserial PRIMARY KEY,
  k  int NOT NULL,
  v  int NOT NULL
);

CREATE TABLE eval_bt(LIKE eval_bw INCLUDING ALL);

ALTER TABLE eval_bw SET (autovacuum_enabled = off);
ALTER TABLE eval_bt SET (autovacuum_enabled = off);

SELECT setseed(${SEED});

INSERT INTO eval_bw(k, v)
SELECT (random()*${KEY_MAX})::int + 1,
       (random()*${VALUE_MAX})::int
FROM generate_series(1, ${ROWS});

INSERT INTO eval_bt(k, v)
SELECT k, v FROM eval_bw;

CREATE INDEX eval_bw_k_idx ON eval_bw USING bwtree(k);
CREATE INDEX eval_bt_k_idx ON eval_bt USING btree(k);

ANALYZE eval_bw;
ANALYZE eval_bt;
SQL

echo "[setup] done"
