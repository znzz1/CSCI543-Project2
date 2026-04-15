#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PG_BIN="${PG_BIN:-${ROOT_DIR}/bin}"
DB_NAME="evaldb"
TABLE_NAME="eval_bw"
OUT_FILE="${ROOT_DIR}/results/summary/correctness_check.txt"

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
    --table)
      TABLE_NAME="$2"
      shift 2
      ;;
    --out)
      OUT_FILE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -x "${PG_BIN}/psql" ]]; then
  if command -v psql >/dev/null 2>&1; then
    PG_BIN="$(cd "$(dirname "$(command -v psql)")" && pwd)"
  elif [[ -x "${ROOT_DIR}/postgresql/bin/psql" ]]; then
    PG_BIN="${ROOT_DIR}/postgresql/bin"
  else
    echo "Cannot find PostgreSQL binaries. Please pass --pg-bin <path-to-bin>." >&2
    exit 1
  fi
fi

mkdir -p "$(dirname "${OUT_FILE}")"
PSQL="${PG_BIN}/psql"

echo "[check] db=${DB_NAME} table=${TABLE_NAME}"
"${PSQL}" "${DB_NAME}" <<SQL | tee "${OUT_FILE}"
DROP TABLE IF EXISTS idx_eq, idx_ge, idx_gt, idx_rng, seq_eq, seq_ge, seq_gt, seq_rng;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = on;
CREATE TEMP TABLE idx_eq  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k = 42;
CREATE TEMP TABLE idx_ge  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k >= 42000;
CREATE TEMP TABLE idx_gt  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k > 42000;
CREATE TEMP TABLE idx_rng AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k BETWEEN 42000 AND 43000;

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
CREATE TEMP TABLE seq_eq  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k = 42;
CREATE TEMP TABLE seq_ge  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k >= 42000;
CREATE TEMP TABLE seq_gt  AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k > 42000;
CREATE TEMP TABLE seq_rng AS SELECT ctid AS tid_ref FROM ${TABLE_NAME} WHERE k BETWEEN 42000 AND 43000;

SELECT
  (SELECT count(*) FROM ((SELECT * FROM idx_eq  EXCEPT SELECT * FROM seq_eq )
                         UNION ALL
                         (SELECT * FROM seq_eq  EXCEPT SELECT * FROM idx_eq )) s) AS diff_eq,
  (SELECT count(*) FROM ((SELECT * FROM idx_ge  EXCEPT SELECT * FROM seq_ge )
                         UNION ALL
                         (SELECT * FROM seq_ge  EXCEPT SELECT * FROM idx_ge )) s) AS diff_ge,
  (SELECT count(*) FROM ((SELECT * FROM idx_gt  EXCEPT SELECT * FROM seq_gt )
                         UNION ALL
                         (SELECT * FROM seq_gt  EXCEPT SELECT * FROM idx_gt )) s) AS diff_gt,
  (SELECT count(*) FROM ((SELECT * FROM idx_rng EXCEPT SELECT * FROM seq_rng)
                         UNION ALL
                         (SELECT * FROM seq_rng EXCEPT SELECT * FROM idx_rng)) s) AS diff_rng;
SQL

echo "[check] output saved to ${OUT_FILE}"
