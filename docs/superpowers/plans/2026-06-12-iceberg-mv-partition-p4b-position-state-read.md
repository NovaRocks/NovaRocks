# Iceberg MV Partition P4b Position State Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use target locator matched `(file, pos)` rows to reduce aggregate MV old-state reads from affected files to the exact matched positions.

**Architecture:** P4a preserves locator matches as `TargetApplyLocatorResult::matched_positions`. P4b must not add a field that cannot reach execution: current Iceberg scans flow through `IcebergDataFileInfo -> IcebergSplit -> THdfsScanRange -> FileScanRange -> ScanRunner`, and `THdfsScanRange` has no include-position payload. The implementation therefore has two gates:

1. add a tested scan-runner include-position filter on `FileScanRange`;
2. wire aggregate MV refresh so locator runs before old-state scan and feeds the filter through a local execution path, or introduce a real range payload that survives scan planning.

**Tech Stack:** Rust, scan runner, Iceberg MV aggregate-state direct execution, target locator results, cargo unit tests.

---

### Task 1: Runner-Level Include Position Filter

**Files:**
- Modify: `src/fs/scan_context.rs`
- Modify: `src/exec/operators/scan/runner.rs`
- Modify: `src/connector/hdfs.rs`

- [x] **Step 1: Write failing scan-runner test**

Add a runner unit test that simulates a file chunk with positions `[0, 1, 2, 3]` and an include-position set `{1, 3}`. It should return only rows at positions `1` and `3`, and `_pos` virtual-column output must remain `[1, 3]`.

- [x] **Step 2: Add local range payload**

Add `included_positions: Option<Vec<i64>>` to `FileScanRange` and carry it through `ScanMorsel::FileRange`.

- [x] **Step 3: Apply include filter after MoR deletes**

In `ScanRunner`, apply the include-position mask after existing Iceberg delete/equality delete filtering and before virtual-column synthesis. Keep counters aligned with raw file positions.

### Task 2: Planner Payload Gate

**Files:**
- Modify: `src/sql/catalog.rs`
- Modify: `src/connector/iceberg/scan_planner.rs`
- Modify: `src/sql/codegen/nodes.rs`

- [x] **Step 1: Verify transport boundary**

Confirm whether `included_positions` can be carried through the existing local connector path without changing generated Thrift. If not, do not fake support through an unused field; document the required transport change in this plan before implementation proceeds.

Verified: existing transport cannot carry the payload. Iceberg target-state scans
are lowered from `IcebergDataFileInfo -> IcebergSplit -> THdfsScanRange ->
FileScanRange -> ScanRunner`; `THdfsScanRange` currently ends at optional fields
37/38 (`first_row_id`, `data_sequence_number`), and
`lower::node::hdfs_scan` initializes `FileScanRange::included_positions` to
`None`. Continuing P4-b requires adding a real optional Thrift field and wiring
it through codegen/lowering, rather than attaching an unused local-only field.

- [x] **Step 2: Add data-file position binding only if transport exists**

If transport is available, add `included_positions` to `IcebergDataFileInfo` and make `build_hdfs_scan_range_params_for_file` disable byte splitting for position-bound files.

### Task 3: Locator Before Old-State Scan

**Files:**
- Modify: `src/exec/operators/aggregate_state_merge.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [x] **Step 1: Restructure aggregate direct execution**

Materialize delta state first, derive touched row ids, run target locator with the same partition filter, and then execute old-state scan with the resulting file/position binding.

- [x] **Step 2: Preserve fallback matrix**

If locator-before-read fails or position binding cannot be used, fall back to the current P3 matrix: affected-partition file pruning plus row-id filtering, then full old-state scan when thresholds require it.

### Task 4: Verification

**Files:**
- Test only.

- [x] **Step 1: Unit tests**

Run:

```bash
cargo test --lib include_position_filter
cargo test --lib position_bound_file_carries_included_positions_without_splitting
cargo test --lib bind_target_state_file_positions
cargo test --lib aggregate_state_merge_direct_codegen
cargo test --lib bind_scan_ranges_to_target_positions_collapses_split_ranges_for_position_bound_file
cargo test --lib exec::operators::scan::runner
cargo test --lib engine::mv::iceberg_target_apply
cargo test --lib exec::operators::aggregate_state_merge
```

- [x] **Step 2: MV regression**

Run:

```bash
cargo test --lib engine::mv
```

- [x] **Step 3: Diff hygiene**

Run:

```bash
git diff --check
```

- [x] **Step 4: Dev-opt compile check**

Run:

```bash
cargo check --profile dev-opt --lib
```
