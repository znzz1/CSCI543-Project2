#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from pathlib import Path


def load_rows(csv_path: Path):
    rows = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append(
                    {
                        "engine": r["engine"],
                        "workload": r["workload"],
                        "clients": int(r["clients"]),
                        "latency_ms": float(r["latency_ms"]) if r["latency_ms"] else None,
                        "tps": float(r["tps"]) if r["tps"] else None,
                        "anomaly": int(r["anomaly"]) if r["anomaly"] else 0,
                    }
                )
            except (ValueError, KeyError):
                continue
    return rows


def make_plot(rows, out_dir: Path):
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        raise SystemExit(f"matplotlib is required for plotting: {exc}")

    by_workload = defaultdict(list)
    for r in rows:
        by_workload[r["workload"]].append(r)

    for workload, ws in by_workload.items():
        by_engine = defaultdict(list)
        for r in ws:
            by_engine[r["engine"]].append(r)

        for metric, ylabel in [("tps", "TPS"), ("latency_ms", "Latency (ms)")]:
            plt.figure(figsize=(7, 4.5))
            for engine, rs in sorted(by_engine.items()):
                rs = sorted(rs, key=lambda x: x["clients"])
                xs = [x["clients"] for x in rs if x[metric] is not None]
                ys = [x[metric] for x in rs if x[metric] is not None]
                if xs and ys:
                    plt.plot(xs, ys, marker="o", label=engine.upper())

            plt.xlabel("Clients")
            plt.ylabel(ylabel)
            plt.title(f"{workload.upper()} - {ylabel}")
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()

            out_file = out_dir / f"{workload}_{metric}.png"
            plt.savefig(out_file, dpi=150)
            plt.close()
            print(f"wrote {out_file}")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results from CSV summary.")
    parser.add_argument("--input", required=True, help="Input CSV (from parse_pgbench.py)")
    parser.add_argument("--out-dir", required=True, help="Output directory for figures")
    args = parser.parse_args()

    in_csv = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(in_csv)
    if not rows:
        raise SystemExit(f"No valid rows found in {in_csv}")

    make_plot(rows, out_dir)


if __name__ == "__main__":
    main()
