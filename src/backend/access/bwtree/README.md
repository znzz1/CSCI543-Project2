# BwTree Access Method: Detailed Architecture and Developer Guide

This directory implements a PostgreSQL index access method named `bwtree`
(`USING bwtree`).

The code is currently **correctness-first**. In several places it intentionally
uses conservative synchronization and retry logic to preserve semantics under
concurrency before optimizing for peak throughput.

---

## 1. What This Module Is Responsible For

### In Scope (Implemented)

- AM callback wiring (`CREATE INDEX`, insert, scan, bitmap scan, vacuum hooks).
- PID-based mapping table (`PID -> (base block, delta head block)`).
- Delta-chain updates for logical mutations.
- Leaf and internal split with parent propagation.
- Consolidation of base + delta chains.
- Epoch-based deferred reclamation and GC queues.
- SQL-visible correctness for non-unique indexing scenarios.

### Out of Scope / Not Fully Implemented Yet

- Full `UNIQUE` index semantics.
- Full paper-level lock-free optimization profile.
- Production-level delete/vacuum integration parity.

---

## 2. Conceptual Model

### 2.1 PID and Mapping Table

- A **PID** is a stable logical node id.
- Physical pages can move/rewrite, but PID identity remains stable.
- The mapping table stores the current physical state for each PID:
  - `base_blkno`: current base page for this PID.
  - `delta_blkno`: current delta chain head (or invalid if none).

### 2.2 Base Page + Delta Chain

- Base page stores materialized tuples for a node.
- Delta pages prepend logical changes (insert/delete/control deltas).
- Logical node state = `base page` + `apply(delta chain from head to tail)`.

### 2.3 Structural Modification (SMO)

- Splits create new right node(s), publish mapping transitions via CAS,
  and propagate separators upward.
- Root split installs a new root PID.

### 2.4 Deferred Reclamation

- Old chains/pages are retired, not immediately freed.
- Epoch rules decide when retired objects become reclaimable.

---

## 3. Key Data Structures (Defined in `src/include/access/bwtree.h`)

### `BWTreeMetaPageData`

Global index metadata on metapage:

- magic/version,
- `bwt_root_pid`,
- current tree level,
- PID allocator cursor (`bwt_next_pid`),
- mapping-page block number array.

### `BWTreeMapEntry`

A mapping-table slot:

- `base_blkno`,
- `delta_blkno`.

### `BWTreePageOpaqueData`

Per-page opaque metadata:

- sibling pointers (`prev`, `next`),
- owning PID,
- level,
- flags (`LEAF`, `ROOT`, `DELTA`, etc.),
- page id signature.

### `BWTreeDeltaRecordData`

Logical delta payload header:

- type (`INSERT`, `DELETE`, `SPLIT`, `SEPARATOR`),
- related PID (for control records),
- target TID (for delete-style semantics),
- trailing tuple payload length.

### Snapshot / View Structures

- `BWTreeNodeSnapshot`: stable `(pid, base, delta, level, flags)` capture.
- `BWTMaterializedPage`: logical tuple array + sibling metadata.
- `BWTreeNodeView`: snapshot + materialized page + split metadata for routing.

---

## 4. File-by-File Responsibility Map

### `bwtree.c`

Top-level AM API entrypoints and handler registration:

- `bwtreehandler`
- build/insert/scan/vacuum/cost callback glue

### `bwtreesort.c`

Build path and initial on-disk layout creation:

- empty tree bootstrap
- mapping page initialization during build
- initial root/base creation and tuple loading

### `bwtreeinsert.c`

Insert entrypoint:

- construct route `ScanKey`
- descend to leaf (+ parent hint)
- delegate to `_bwt_insert_item`
- trigger periodic GC scheduling

### `bwtreesearch.c`

Routing logic:

- root-to-leaf descent
- internal child selection via key comparison
- split move-right handling
- parent path tracking

### `bwtreescan.c`

Scan lifecycle:

- begin/rescan/end
- tuple and bitmap retrieval
- key filtering and leaf traversal safety checks

### `bwtreesmo.c`

SMO logic:

- split detection and execution
- internal-page insert/rewrite
- root install
- parent resolution and upward split propagation

### `bwtreedelta.c`

Delta and materialization core:

- delta install with CAS retry
- delta chain apply
- node snapshot and materialization
- consolidation publish + retire old state

### `bwtreemap.c`

Mapping table primitives:

- lookup
- CAS publish transition
- PID allocation
- map page growth

### `bwtreepage.c`

Buffer/page helpers:

- `_bwt_getbuf`, `_bwt_relbuf`, `_bwt_allocbuf`
- page initialization (`_bwt_initpage`, `_bwt_initmetapage`)

### `bwtreeepoch.c`

Epoch manager:

- shared/global epoch state
- backend local participation state
- min-active epoch and advance logic

### `bwtreegc.c`

GC engine:

- retire object enqueue
- local/shared queue coordination
- reclaim execution under epoch-safe conditions

---

## 5. End-to-End Call Chains

### 5.1 Insert Path

1. `bwtreeinsert` enters epoch and builds route key.
2. `_bwt_search_leaf_with_parent` returns target leaf PID (+ parent hint).
3. `_bwt_insert_item` installs insert delta.
4. Periodic split/consolidation checks run.
5. Publish state changes through `_bwt_map_cas`.
6. Retire old objects for epoch GC.

### 5.2 Leaf/Internal Split Path

1. Snapshot capture (`base`, `delta_head`, metadata).
2. Materialize logical contents.
3. Build left/right outputs and allocate right PID/page.
4. Publish map transition via CAS.
5. Install split/separator control deltas.
6. Propagate separator upward:
   - internal insert or recursive internal split,
   - or install new root.

### 5.3 Consolidation Path

1. Snapshot capture.
2. Materialize base + delta.
3. Write new base page.
4. CAS publish to `(new_base, InvalidDelta)`.
5. Retire old chain/base for epoch-safe reclaim.

### 5.4 Scan Path

1. Start from routed leaf.
2. Materialize logical leaf state.
3. Filter tuples by scan keys.
4. Follow sibling links for range continuation.

---

## 6. Concurrency and Locking Strategy (Current)

### 6.1 Buffer Locks

- Page content mutations occur under PostgreSQL buffer content locks.
- Dirty marking follows lock discipline expected by buffer manager.

### 6.2 CAS Publish

- Mapping transitions use atomic CAS on packed map entries.
- Writers retry on stale expected state.
- Retry outcomes are interpreted carefully:
  - idempotent already-published state is accepted,
  - incompatible observed state forces restart from fresh snapshot.

### 6.3 Revalidation Before Publish

Critical paths re-check mapping state before final publish:

- split,
- internal rewrite,
- consolidation.

This prevents stale-snapshot publication races.

---

## 7. Epoch and GC Design

### 7.1 Epoch Participation

- Each backend tracks local epoch participation and nesting depth.
- Shared state tracks global epoch and active backend epochs.

### 7.2 Retirement

- Mutations retire objects (delta chains / blocks) with a retire epoch.
- Retired objects are queued locally and can spill/share.

### 7.3 Reclamation Rule

- Object is reclaimable when `retire_epoch < min_active_epoch`.

### 7.4 Queue Model

- Backend-local queue: low-overhead local retirement.
- Shared queue: cross-backend spill/recovery path.

---

## 8. Correctness Invariants

The following invariants should always hold:

1. Every live PID maps to a valid base block.
2. Mapping CAS transitions are from observed expected state only.
3. Split/consolidation publication is based on a validated fresh snapshot.
4. Delta chain walks stay in relation bounds and remain acyclic.
5. Search descent preserves key-order routing rules.
6. Reclamation never frees objects still visible to active epochs.
7. Parent/child level relationship is consistent in internal routing.

---

## 9. Defensive Checks and Fail-Fast Behavior

Code includes explicit fail-fast guards for:

- invalid page signatures,
- out-of-range block references,
- malformed delta chains,
- invalid downlinks,
- impossible routing levels,
- retry-bound overflow.

These checks are intentional and prioritized over silent recovery.

---

## 10. Current Trade-offs

- Some operations are serialized more than ideal (correctness first).
- Extra materialization/revalidation can reduce throughput under high write concurrency.
- GC and consolidation policies are conservative to reduce correctness risk.

---

## 11. Extension Points for Future Work

Potential next improvements:

- optimize high-contention map update paths,
- improve parent resolution cost in split propagation,
- implement full unique semantics,
- refine consolidation scheduling heuristics,
- tune epoch/GC triggering strategies for lower write latency.

---

## 12. Validation and Test Playbook

Recommended progression:

1. **Compile check**
   - clean build for module and backend.
2. **Small correctness checks**
   - compare index-path vs seq-path outputs (eq/ge/gt/range diffs).
3. **Concurrent smoke tests**
   - mixed insert/update/read with multiple clients.
4. **Scale tests**
   - increase rows and clients progressively (1, 8, 16, 32, 64).
5. **Observe**
   - transaction aborts,
   - warnings/errors,
   - correctness diffs,
   - throughput collapse points.

Acceptance baseline for correctness:

- no unexpected ERROR/aborts under smoke load,
- result diffs remain zero for core predicates.

---

## 13. Quick Navigation

- Public API/types: `src/include/access/bwtree.h`
- AM callback wiring: `bwtree.c`
- Build path: `bwtreesort.c`
- Insert entry: `bwtreeinsert.c`
- Search route: `bwtreesearch.c`
- Scan path: `bwtreescan.c`
- SMO (split/root/internal): `bwtreesmo.c`
- Delta/consolidation: `bwtreedelta.c`
- Mapping CAS: `bwtreemap.c`
- Buffer/page helpers: `bwtreepage.c`
- Epoch manager: `bwtreeepoch.c`
- GC engine: `bwtreegc.c`
