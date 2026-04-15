#!/usr/bin/env python3
import argparse
import csv
import re
from pathlib import Path


LOG_RE = re.compile(r"^(bw|bt)_(insert|rmw)_c(\d+)\.log$")
LAT_RE = re.compile(r"latency average =\s*([0-9.]+)\s*ms")
TPS_RE = re.compile(r"tps =\s*([0-9.]+)")
TX_RE = re.compile(r"number of transactions actually processed:\s*([0-9]+)")
FAIL_RE = re.compile(r"ERROR:|WARNING:|aborted", re.IGNORECASE)


def parse_one(log_path: Path):
    m = LOG_RE.match(log_path.name)
    if not m:
        return None

    engine, workload, clients_s = m.groups()
    clients = int(clients_s)

    latency_ms = None
    tps = None
    tx = None
    anomaly = False

    with log_path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if latency_ms is None:
                lm = LAT_RE.search(line)
                if lm:
                    latency_ms = float(lm.group(1))
            if tps is None:
                tm = TPS_RE.search(line)
                if tm:
                    tps = float(tm.group(1))
            if tx is None:
                xm = TX_RE.search(line)
                if xm:
                    tx = int(xm.group(1))
            if FAIL_RE.search(line):
                anomaly = True

    return {
        "engine": engine,
        "workload": workload,
        "clients": clients,
        "latency_ms": latency_ms,
        "tps": tps,
        "tx_processed": tx,
        "anomaly": int(anomaly),
        "log_file": str(log_path),
    }


def main():
    parser = argparse.ArgumentParser(description="Parse pgbench logs into CSV summary.")
    parser.add_argument("--raw-dir", required=True, help="Directory containing pgbench log files")
    parser.add_argument("--out", required=True, help="Output CSV file path")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_csv = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for p in sorted(raw_dir.glob("*.log")):
        r = parse_one(p)
        if r is not None:
            rows.append(r)

    if not rows:
        raise SystemExit(f"No matching log files found in: {raw_dir}")

    rows.sort(key=lambda x: (x["workload"], x["engine"], x["clients"]))

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "engine",
                "workload",
                "clients",
                "latency_ms",
                "tps",
                "tx_processed",
                "anomaly",
                "log_file",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} rows to {out_csv}")


if __name__ == "__main__":
    main()
