#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PG_BIN="${PG_BIN:-${ROOT_DIR}/bin}"
DB_NAME="evaldb"
DURATION="60"
CLIENTS_STR="1 8 16 32 64"
KEY_MAX="1000000"
VALUE_MAX="1000000"

RAW_DIR="${ROOT_DIR}/results/raw"
SUMMARY_DIR="${ROOT_DIR}/results/summary"
FIG_DIR="${ROOT_DIR}/results/figures"

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
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --clients)
      CLIENTS_STR="$2"
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
    --raw-dir)
      RAW_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ ! -x "${PG_BIN}/pgbench" ]]; then
  if command -v pgbench >/dev/null 2>&1; then
    PG_BIN="$(cd "$(dirname "$(command -v pgbench)")" && pwd)"
  elif [[ -x "${ROOT_DIR}/postgresql/bin/pgbench" ]]; then
    PG_BIN="${ROOT_DIR}/postgresql/bin"
  else
    echo "Cannot find PostgreSQL binaries. Please pass --pg-bin <path-to-bin>." >&2
    exit 1
  fi
fi

mkdir -p "${RAW_DIR}" "${SUMMARY_DIR}" "${FIG_DIR}"

PGBENCH="${PG_BIN}/pgbench"
read -r -a CLIENTS <<< "${CLIENTS_STR}"

tmp_insert_bw="$(mktemp)"
tmp_insert_bt="$(mktemp)"
tmp_rmw_bw="$(mktemp)"
tmp_rmw_bt="$(mktemp)"

cleanup() {
  rm -f "${tmp_insert_bw}" "${tmp_insert_bt}" "${tmp_rmw_bw}" "${tmp_rmw_bt}"
}
trap cleanup EXIT

cat > "${tmp_insert_bw}" <<SQL
\set k random(1,${KEY_MAX})
\set v random(1,${VALUE_MAX})
INSERT INTO eval_bw(k,v) VALUES (:k,:v);
SQL

cat > "${tmp_insert_bt}" <<SQL
\set k random(1,${KEY_MAX})
\set v random(1,${VALUE_MAX})
INSERT INTO eval_bt(k,v) VALUES (:k,:v);
SQL

cat > "${tmp_rmw_bw}" <<SQL
\set k random(1,${KEY_MAX})
\set v random(1,${VALUE_MAX})
UPDATE eval_bw
SET v = :v
WHERE ctid = (SELECT ctid FROM eval_bw WHERE k = :k LIMIT 1);
SQL

cat > "${tmp_rmw_bt}" <<SQL
\set k random(1,${KEY_MAX})
\set v random(1,${VALUE_MAX})
UPDATE eval_bt
SET v = :v
WHERE ctid = (SELECT ctid FROM eval_bt WHERE k = :k LIMIT 1);
SQL

echo "[bench] db=${DB_NAME} duration=${DURATION}s clients=${CLIENTS_STR}"

run_case() {
  local engine="$1"
  local workload="$2"
  local clients="$3"
  local sql_file="$4"
  local log_file="${RAW_DIR}/${engine}_${workload}_c${clients}.log"

  echo "=== ${engine} ${workload} c=${clients} ==="
  "${PGBENCH}" -n -M prepared -c "${clients}" -j "${clients}" -T "${DURATION}" \
    -f "${sql_file}" "${DB_NAME}" | tee "${log_file}"
}

for c in "${CLIENTS[@]}"; do
  run_case "bw" "insert" "${c}" "${tmp_insert_bw}"
  run_case "bt" "insert" "${c}" "${tmp_insert_bt}"
done

for c in "${CLIENTS[@]}"; do
  run_case "bw" "rmw" "${c}" "${tmp_rmw_bw}"
  run_case "bt" "rmw" "${c}" "${tmp_rmw_bt}"
done

echo "[bench] extracting key lines"
grep -H "latency average\\|tps =" "${RAW_DIR}"/bw_*.log "${RAW_DIR}"/bt_*.log \
  | tee "${SUMMARY_DIR}/pgbench_key_metrics.txt"

echo "[bench] error/warning scan"
grep -H "ERROR:|WARNING:|aborted" "${RAW_DIR}"/bw_*.log "${RAW_DIR}"/bt_*.log \
  | tee "${SUMMARY_DIR}/pgbench_anomalies.txt" || true

echo "[bench] done"
