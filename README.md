# BwTree Project 2

PostgreSQL index access method implementation (`USING bwtree`) with reproducible evaluation scripts.

This project is **correctness-first**: semantics and reproducibility are prioritized before throughput tuning.

---

## 0. Container Setup and Folder Layout

### 0.1 Dev Container baseline

This project was developed and tested in a VS Code Dev Container with:

- Base image: `mcr.microsoft.com/devcontainers/base:noble`
- OS: Ubuntu 24.04 (Noble)
- Shell: Bash

### 0.2 Workspace layout used in our environment

In our container workspace, the directory layout is:

```text
<workspace-root>/
|- .devcontainer/
`- CSCI543-Project2/     <-- actual Git repository root
```

Important: run build/evaluation commands **inside** `CSCI543-Project2/`.

If you cloned directly without an outer wrapper, your current folder is already the repository root.

---

## Quick Start

From your current terminal:

```bash
# If this folder contains CSCI543-Project2/ but no configure file, enter repo root
if [ -d CSCI543-Project2 ] && [ ! -f configure ]; then
  cd CSCI543-Project2
fi

./configure --prefix="$PWD/install"
make -j8
make install

"$PWD/install/bin/initdb" -D "$PWD/db"
"$PWD/install/bin/pg_ctl" -D "$PWD/db" -l "$PWD/pg.log" start

bash scripts/setup/setup_eval.sh --pg-bin "$PWD/install/bin" --db evaldb --rows 100000
bash scripts/bench/run_pgbench_eval.sh --pg-bin "$PWD/install/bin" --db evaldb --duration 60
bash scripts/bench/run_correctness_check.sh --pg-bin "$PWD/install/bin" --db evaldb

python3 scripts/analysis/parse_pgbench.py --raw-dir results/raw --out results/summary/pgbench_summary.csv
python3 scripts/plots/plot_results.py --input results/summary/pgbench_summary.csv --out-dir results/figures
```

---

## 1. Reproduce Step by Step

### 1.0 Enter repository root

If you are in an outer workspace wrapper, run:

```bash
cd CSCI543-Project2
```

If `configure` already exists in current folder, skip this step.

### 1.1 Build

```bash
./configure --prefix="$PWD/install"
make -j8
make install
```

### 1.2 Initialize PostgreSQL (first run only)

```bash
"$PWD/install/bin/initdb" -D "$PWD/db"
"$PWD/install/bin/pg_ctl" -D "$PWD/db" -l "$PWD/pg.log" start
```

### 1.3 Prepare dataset and indexes

```bash
bash scripts/setup/setup_eval.sh --pg-bin "$PWD/install/bin" --db evaldb --rows 100000
```

### 1.4 Run benchmark matrix

```bash
bash scripts/bench/run_pgbench_eval.sh --pg-bin "$PWD/install/bin" --db evaldb --duration 60
```

### 1.5 Run correctness validation

```bash
bash scripts/bench/run_correctness_check.sh --pg-bin "$PWD/install/bin" --db evaldb
```

### 1.6 Parse logs and plot figures

```bash
python3 scripts/analysis/parse_pgbench.py --raw-dir results/raw --out results/summary/pgbench_summary.csv
python3 scripts/plots/plot_results.py --input results/summary/pgbench_summary.csv --out-dir results/figures
```

---

## 2. Output Artifacts

- Raw logs: `results/raw/*.log`
- Key summaries: `results/summary/*`
- Figures: `results/figures/*.png`

---

## 3. Evaluation Configuration (Proposal-Aligned)

- Compared engines:
  - BwTree (`USING bwtree`)
  - PostgreSQL nbtree (`USING btree`)
- Workloads:
  - Insert-only
  - Read-Modify-Write
- Concurrency:
  - `1, 8, 16, 32, 64`
- Metrics:
  - TPS
  - Average latency (ms)
  - Scalability vs client count

---

## 4. Scripts Overview

- `scripts/setup/setup_eval.sh`
  - Build evaluation tables, load synthetic data, create BwTree + B-tree indexes.
- `scripts/bench/run_pgbench_eval.sh`
  - Run full benchmark matrix and collect raw logs.
- `scripts/bench/run_correctness_check.sh`
  - SQL diff checks (`diff_eq/diff_ge/diff_gt/diff_rng`).
- `scripts/bench/*.sql`
  - `pgbench` transaction scripts (`insert`, `rmw`) for both engines.
- `scripts/analysis/parse_pgbench.py`
  - Parse `pgbench` logs into structured CSV.
- `scripts/plots/plot_results.py`
  - Plot TPS and latency curves from CSV.

All shell scripts accept `--pg-bin <path-to-bin>`. If omitted, they auto-detect from:
1. `<repo-root>/bin`
2. `psql/pgbench` in `PATH`
3. `<repo-root>/postgresql/bin` (legacy layout)

---

## 5. Implementation Overview

### 5.1 Core Design

- Stable logical node identity via `PID`.
- Mapping table stores `PID -> (base_blkno, delta_blkno)`.
- Logical updates are represented as delta records and published through map CAS.
- Split and consolidation reshape nodes.
- Epoch + GC provide deferred-safe reclamation.

### 5.2 Main Execution Paths

- Insert:
  - search leaf -> install insert delta -> split/consolidate checks -> retire old state
- Split:
  - snapshot -> materialize -> build left/right -> publish -> separator propagation
- Consolidation:
  - rebuild from base + delta chain -> CAS publish new base -> retire old chain/base
- Scan:
  - route leaf -> materialize logical view -> apply predicates -> sibling traversal

### 5.3 Recent Correctness Hardening

- Materialization now replays base + delta into a **dynamic tuple vector** instead of a single `BLCKSZ` scratch page.
- This fixes a previously reproducible failure during heavy index build:
  - `bwtree: failed to apply INSERT delta to base page`

---

## 6. Scope and Current Limitations

Implemented in this stage:

- Core AM callbacks (build/insert/scan/bitmap/vacuum wiring)
- Mapping CAS path
- Delta install/apply/materialization
- Leaf + internal split propagation
- Consolidation
- Epoch-based deferred GC

Not fully implemented in this stage:

- Full `UNIQUE` semantics
- Full delete/vacuum parity with nbtree
- Ordered index-scan feature parity (`ORDER BY`, backward ordered scans)
- Full paper-level lock-free optimization profile

Behavior note:

- `bwtreeinsert()` rejects non-`UNIQUE_CHECK_NO` requests.

---

## 7. Code Map

| File | Responsibility |
|---|---|
| `src/include/access/bwtree.h` | Core types/constants/public internal declarations |
| `src/backend/access/bwtree/bwtree.c` | AM handler + callback registration |
| `src/backend/access/bwtree/bwtreesort.c` | Build path/bootstrap |
| `src/backend/access/bwtree/bwtreeinsert.c` | Insert entry + routing setup |
| `src/backend/access/bwtree/bwtreesearch.c` | Root-to-leaf routing |
| `src/backend/access/bwtree/bwtreescan.c` | Scan lifecycle |
| `src/backend/access/bwtree/bwtreesmo.c` | Split/root/internal SMO logic |
| `src/backend/access/bwtree/bwtreedelta.c` | Delta install/apply/materialize/consolidate |
| `src/backend/access/bwtree/bwtreemap.c` | Mapping lookup/update/CAS/PID allocation |
| `src/backend/access/bwtree/bwtreeepoch.c` | Epoch state/advance |
| `src/backend/access/bwtree/bwtreegc.c` | Retire queue + reclaim |
| `src/backend/access/bwtree/bwtreepage.c` | Buffer/page helpers |

---

## 8. Troubleshooting

- Index build seems slow:
  - check `pg_stat_activity` and wait events
  - `DataFileRead` usually means IO-bound (not deadlock)
- Benchmark aborts:
  - inspect `results/raw/*.log` and `<repo-root>/pg.log`
- Correctness diff not zero:
  - rerun `scripts/bench/run_correctness_check.sh`
  - confirm the correct target DB/table is used

---

## 9. Submission Checklist

- BwTree implementation code
- Setup + benchmark scripts
- Correctness validation script
- Analysis/parsing script
- Plot generation script
- This root `README.md` with complete reproduction steps