# IVM-A2 + A3 Snapshot Pin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `plan_changes` to accept arbitrary `[from, to]` snapshot range (A2) and introduce `RefreshSnapshotPin` as the single snapshot source-of-truth for the entire refresh (A3), so iceberg-backed MV refresh's delta computation and bookkeeping always agree on the same snapshot.

**Architecture:** 4-layer change. Layer 1: connector primitive `plan_changes` / `classify_lineage` accept explicit `to_snapshot_id`. Layer 2: new helper `plan_change_batches_for_pin` for multi-base future; retire `validate_change_batch_current_snapshot`. Layer 3: new `RefreshSnapshotPin` struct + `inject_pin_as_for_version_as_of` AST helper (no-op for single base, ready for multi-base). Layer 4: pin freeze at refresh entry, replaces inline `current_snapshot_id` captures; bookkeeping `last_refresh_snapshots = pin`.

**Tech Stack:** Rust, sqlparser, iceberg-rust, sqlite metadata, Iceberg REST + MinIO docker fixture, sql-test-runner.

**Spec:** [docs/design/specs/2026-05-15-ivm-a2-a3-snapshot-pin-design.md](../specs/2026-05-15-ivm-a2-a3-snapshot-pin-design.md)

**Spec drift to note:** Spec §7.1 defines `RefreshSnapshotPin` keyed by `IcebergTableRef`, but that type derives only `Clone, Debug, PartialEq, Eq, Serialize, Deserialize` — no `Ord/Hash`. This plan uses `BTreeMap<String, i64>` keyed by `IcebergTableRef::fqn()` to match the existing `StoredMaterializedView.refresh_target_snapshots` field shape. The public API still accepts `&IcebergTableRef` and computes `fqn()` internally.

---

## File Structure

| Path | Action | Responsibility |
|---|---|---|
| `src/connector/iceberg/changes.rs` | Modify | `plan_changes` / `classify_lineage` accept `Option<i64>` / `i64` to_snapshot_id; 5 new unit tests; doc comments updated |
| `src/lower/node/iceberg_delta_scan.rs` | Modify | Delete A2 guard (lines 90-115); switch `plan_changes` call to `Some(payload.to_snapshot_id)` |
| `src/engine/mv/iceberg_refresh.rs` | Modify | Legacy `plan_changes` caller switches to `Some(...)`; pin freeze at refresh entries (lines 101, 590) |
| `src/connector/starrocks/managed/ivm_change_stream.rs` | Modify | Retire `validate_change_batch_current_snapshot`; thin-wrap `plan_iceberg_change_batch_for_ivm`; add `plan_change_batches_for_pin` helper |
| `src/connector/starrocks/managed/refresh_pin.rs` | Create | `RefreshSnapshotPin` struct + `capture` + `inject_pin_as_for_version_as_of` AST helper + `#[cfg(test)]` `after_capture_hook` |
| `src/connector/starrocks/managed/mod.rs` | Modify | Declare `pub(crate) mod refresh_pin;` |
| `src/connector/starrocks/managed/mv_refresh.rs` | Modify | Pin freeze at refresh entry (line 121); replace `single_snapshot_map(base_ref, current_snapshot_id)` with `pin.to_snapshot_map()`; bookkeeping uses `pin.to_table_uuid_map()`; acceptance test |
| `src/engine/mod.rs` | Modify | Existing test at line 6295 passes `None` |

---

## Task 1: A2 — `classify_lineage` accepts explicit `to_snapshot_id`

**Files:**
- Modify: `src/connector/iceberg/changes.rs:445-508` (`classify_lineage`)
- Modify: `src/connector/iceberg/changes.rs:517-559` (`plan_changes`) — internal call site only

- [ ] **Step 1: Change `classify_lineage` signature and body**

In `src/connector/iceberg/changes.rs`, replace the existing function with:

```rust
pub(crate) fn classify_lineage(
    metadata: &iceberg::spec::TableMetadata,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> Result<LineagePlan, ChangeError> {
    if metadata.snapshot_by_id(to_snapshot_id).is_none() {
        return Err(ChangeError::LineageBroken {
            previous_snapshot: from_snapshot_id,
        });
    }

    let current_snapshot_id = to_snapshot_id;

    if current_snapshot_id == from_snapshot_id {
        return Ok(LineagePlan {
            previous_snapshot_id: from_snapshot_id,
            current_snapshot_id,
            actions: Vec::new(),
        });
    }

    if metadata.snapshot_by_id(from_snapshot_id).is_none() {
        return Err(ChangeError::LineageBroken {
            previous_snapshot: from_snapshot_id,
        });
    }

    let mut actions_reversed: Vec<LineageAction> = Vec::new();
    let mut cursor = current_snapshot_id;
    loop {
        if cursor == from_snapshot_id {
            break;
        }
        let snapshot_ref = metadata
            .snapshot_by_id(cursor)
            .ok_or(ChangeError::LineageBroken {
                previous_snapshot: from_snapshot_id,
            })?;
        let snapshot = snapshot_ref.as_ref();
        let parent_id = snapshot.parent_snapshot_id();
        let parent = parent_id
            .and_then(|id| metadata.snapshot_by_id(id))
            .map(|sr| sr.as_ref());

        if let Some(action) = classify_snapshot(snapshot, parent)? {
            actions_reversed.push(action);
        }

        match parent_id {
            Some(id) => cursor = id,
            None => {
                return Err(ChangeError::LineageBroken {
                    previous_snapshot: from_snapshot_id,
                });
            }
        }
    }

    actions_reversed.reverse();
    Ok(LineagePlan {
        previous_snapshot_id: from_snapshot_id,
        current_snapshot_id,
        actions: actions_reversed,
    })
}
```

Doc comment update (above the function):

```rust
/// Walk the parent chain from `to_snapshot_id` back to `from_snapshot_id`,
/// dispatching each node through `classify_snapshot`. Performs no I/O.
///
/// Errors:
/// - `LineageBroken` when:
///   * `from_snapshot_id` is not in `metadata` (pruned or never existed)
///   * `to_snapshot_id` is not in `metadata` (expired)
///   * walking back from `to_snapshot_id` runs off the root without reaching
///     `from_snapshot_id` (covers the case where `to` is a strict ancestor of
///     `from`, not a descendant)
/// - `UnsupportedOperation` / `ReplaceValidationFailed` propagated from
///   `classify_snapshot`.
```

- [ ] **Step 2: Update the internal call site in `plan_changes`**

Still in `src/connector/iceberg/changes.rs`, modify the existing `plan_changes` body to compute `current_snapshot_id` and pass it through:

```rust
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    _pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let metadata = table.metadata();
    let current_snapshot_id = metadata
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .ok_or_else(|| {
            ChangeError::InternalInconsistency(
                "plan_changes: table has no current snapshot".to_string(),
            )
        })?;

    let plan = classify_lineage(metadata, previous_snapshot_id, current_snapshot_id)?;
    // ... rest unchanged
```

The function signature stays the same in this task — Task 2 will change it.

- [ ] **Step 3: Build check**

Run: `cargo build -p novarocks --lib 2>&1 | tail -20`
Expected: success (only internal change; no caller impact).

- [ ] **Step 4: Run existing changes tests**

Run: `cargo test -p novarocks --lib connector::iceberg::changes:: 2>&1 | tail -20`
Expected: existing 3 tests in `changes.rs::tests` pass unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "refactor(ivm-a2): classify_lineage accepts explicit to_snapshot_id

Replaces internal metadata.current_snapshot() lookup with an explicit
to_snapshot_id parameter. plan_changes internal call passes current
as before — no behavior change yet. LineageBroken also fires when
to_snapshot_id is missing from metadata."
```

---

## Task 2: A2 — `plan_changes` accepts `Option<i64> to_snapshot_id`

**Files:**
- Modify: `src/connector/iceberg/changes.rs:517-559`
- Modify: `src/connector/iceberg/changes.rs:2250, 2370, 2532` (existing tests pass `None`)
- Modify: `src/engine/mv/iceberg_refresh.rs:2420` (legacy caller passes `None` for now)
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs:54` (caller passes `None`)
- Modify: `src/lower/node/iceberg_delta_scan.rs:121` (caller passes `None`)
- Modify: `src/engine/mod.rs:6295` (test caller passes `None`)

- [ ] **Step 1: Change `plan_changes` signature**

In `src/connector/iceberg/changes.rs`, modify:

```rust
/// Public entrypoint for snapshot-lineage change planning. Walks the
/// lineage from `previous_snapshot_id` (exclusive) to `to_snapshot_id`
/// (inclusive). When `to_snapshot_id` is `None`, defaults to the table's
/// current snapshot (preserves legacy behavior).
///
/// The returned `IcebergChangeBatch.current_snapshot_id` field reflects
/// the *resolved* to_snapshot_id (i.e. the actual right endpoint of the
/// walked lineage), which may differ from `table.metadata().current_snapshot()`
/// when the caller pins to a historical snapshot.
///
/// The `_pk_columns` parameter is reserved for future delete-side row-id
/// computation; snapshot lineage planning itself does not need it yet.
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    to_snapshot_id: Option<i64>,
    _pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let metadata = table.metadata();
    let current_snapshot_id = match to_snapshot_id {
        Some(id) => id,
        None => metadata
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| {
                ChangeError::InternalInconsistency(
                    "plan_changes: table has no current snapshot".to_string(),
                )
            })?,
    };

    let plan = classify_lineage(metadata, previous_snapshot_id, current_snapshot_id)?;
    if plan.actions.is_empty() {
        return Ok(IcebergChangeBatch {
            previous_snapshot_id,
            current_snapshot_id,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        });
    }

    let file_io = table.file_io();
    let collect = collect_files(metadata, file_io, &plan.actions);
    let (inserts, deletes, equality_deletes, deleted_data_files) =
        crate::connector::iceberg::catalog::registry::block_on_iceberg(collect).map_err(
            |e| ChangeError::InternalInconsistency(format!("plan_changes runtime: {e}")),
        )??;

    Ok(IcebergChangeBatch {
        previous_snapshot_id,
        current_snapshot_id,
        inserts,
        deletes,
        equality_deletes,
        deleted_data_files,
    })
}
```

Also update doc comment on `IcebergChangeBatch.current_snapshot_id` field (around line 297):

```rust
    /// The resolved upper endpoint of the planned lineage. When `plan_changes`
    /// was called with `to_snapshot_id = None`, this equals `table.metadata().current_snapshot()`
    /// at the time of the call. When called with `to_snapshot_id = Some(id)`, this
    /// equals `id`. Do not assume this matches the table's current snapshot
    /// at any later moment; callers that need that invariant must check explicitly.
    pub current_snapshot_id: i64,
```

- [ ] **Step 2: Update the 3 internal test call sites**

In `src/connector/iceberg/changes.rs`, modify:

- Line 2250: `plan_changes(&loaded.table, previous, &[])` → `plan_changes(&loaded.table, previous, None, &[])`
- Line 2370: `plan_changes(&loaded.table, previous, &[])` → `plan_changes(&loaded.table, previous, None, &[])`
- Line 2532: `plan_changes(&pruned_table, previous, &[])` → `plan_changes(&pruned_table, previous, None, &[])`

- [ ] **Step 3: Update legacy caller in `src/engine/mv/iceberg_refresh.rs:2420`**

```rust
let batch = match plan_changes(base_table, previous_snapshot_id, None, &[]) {
```

- [ ] **Step 4: Update caller in `src/connector/starrocks/managed/ivm_change_stream.rs:54`**

```rust
let batch = plan_changes(base_table, previous_snapshot_id, None, pk_columns)?;
```

- [ ] **Step 5: Update lowering caller in `src/lower/node/iceberg_delta_scan.rs:121-125`**

```rust
let batch = crate::connector::iceberg::changes::plan_changes(
    &loaded.table,
    payload.from_snapshot_id,
    None,
    &[],
)
```

- [ ] **Step 6: Update test caller in `src/engine/mod.rs:6295-6299`**

```rust
let change_batch = crate::connector::iceberg::changes::plan_changes(
    &loaded.table,
    previous_snapshot_id,
    None,
    &[],
)
```

- [ ] **Step 7: Build + run all unit tests**

Run: `cargo build -p novarocks --lib 2>&1 | tail -10 && cargo test -p novarocks --lib 2>&1 | tail -10`
Expected: build green, all existing tests pass (no semantic change yet because all callers pass `None`).

- [ ] **Step 8: Commit**

```bash
git add src/connector/iceberg/changes.rs \
        src/connector/starrocks/managed/ivm_change_stream.rs \
        src/engine/mv/iceberg_refresh.rs \
        src/lower/node/iceberg_delta_scan.rs \
        src/engine/mod.rs
git commit -m "feat(ivm-a2): plan_changes accepts Option<i64> to_snapshot_id

Adds explicit to_snapshot_id parameter; None preserves legacy behavior
(reads metadata.current_snapshot()). All call sites pass None — no
behavior change yet. Doc comment on IcebergChangeBatch.current_snapshot_id
clarifies the resolved-to semantics."
```

---

## Task 3: A2 — Unit tests for new `to_snapshot_id` behavior

**Files:**
- Modify: `src/connector/iceberg/changes.rs` (tests module at bottom of file, after line 2536)

- [ ] **Step 1: Add test — `to=None` equivalent to `to=Some(current)`**

Add this test inside the `#[cfg(test)] mod tests {}` block:

```rust
    #[test]
    fn plan_changes_to_none_equivalent_to_to_some_current() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("first insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load first");
        let previous = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("second insert");
        let loaded = load_table(&entry, "ns", "orders").expect("load second");
        let current = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("snapshot")
            .snapshot_id();

        let batch_none = plan_changes(&loaded.table, previous, None, &[]).expect("none");
        let batch_some = plan_changes(&loaded.table, previous, Some(current), &[]).expect("some");

        assert_eq!(batch_none.previous_snapshot_id, batch_some.previous_snapshot_id);
        assert_eq!(batch_none.current_snapshot_id, batch_some.current_snapshot_id);
        assert_eq!(batch_none.inserts.len(), batch_some.inserts.len());
        assert_eq!(batch_none.deletes.len(), batch_some.deletes.len());
    }
```

- [ ] **Step 2: Add test — `to` is strict ancestor of `from` → `LineageBroken`**

```rust
    #[test]
    fn plan_changes_to_is_strict_ancestor_of_from_returns_lineage_broken() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");
        let s1 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();

        // from = s1 (newer), to = s0 (older, strict ancestor of from)
        let err = plan_changes(&loaded.table, s1, Some(s0), &[]).expect_err("ancestor not descendant");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == s1),
            "expected LineageBroken with previous_snapshot={s1}, got {err:?}"
        );
    }
```

- [ ] **Step 3: Add test — middle-ancestor truncation**

```rust
    #[test]
    fn plan_changes_truncates_to_middle_ancestor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1 append A");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");
        let s1 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(3)]]).expect("snap s2 append B");
        let loaded = load_table(&entry, "ns", "orders").expect("load s2");
        let s2 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(4)]]).expect("snap s3 append C");
        let loaded = load_table(&entry, "ns", "orders").expect("load s3");
        let s3 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();

        // Plan from s0 to s2: should include A (s1) + B (s2), NOT C (s3)
        let batch_mid = plan_changes(&loaded.table, s0, Some(s2), &[]).expect("truncate");
        assert_eq!(batch_mid.previous_snapshot_id, s0);
        assert_eq!(batch_mid.current_snapshot_id, s2);
        let mid_files: i64 = batch_mid
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();

        // Plan from s0 to s3 (current): should include A + B + C
        let batch_full = plan_changes(&loaded.table, s0, Some(s3), &[]).expect("full");
        let full_files: i64 = batch_full
            .inserts
            .iter()
            .map(|f| f.record_count.unwrap_or_default())
            .sum();

        assert!(
            mid_files < full_files,
            "mid-ancestor truncation should yield fewer rows: mid={mid_files} full={full_files}"
        );
    }
```

- [ ] **Step 4: Add test — `to_snapshot_id` expired (not in metadata)**

```rust
    #[test]
    fn plan_changes_to_snapshot_id_expired_returns_lineage_broken() {
        let dir = tempfile::tempdir().expect("tempdir");
        let warehouse = format!("file://{}", dir.path().join("warehouse").display());
        let entry = test_hadoop_catalog_entry("ice", &warehouse);
        create_namespace(&entry, "ns").expect("namespace");
        create_table(
            &entry,
            "ns",
            "orders",
            &[TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::Int,
                nullable: true,
                aggregation: None,
                default: None,
            }],
            None,
            &[],
            &[],
        )
        .expect("table");
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(1)]]).expect("snap s0");
        let loaded = load_table(&entry, "ns", "orders").expect("load s0");
        let s0 = loaded.table.metadata().current_snapshot().unwrap().snapshot_id();
        insert_rows(&entry, "ns", "orders", &[vec![Literal::Int(2)]]).expect("snap s1");
        let loaded = load_table(&entry, "ns", "orders").expect("load s1");

        // Construct a metadata with s0 expired
        let pruned_metadata = loaded
            .table
            .metadata()
            .clone()
            .into_builder(None)
            .remove_snapshots(&[s0])
            .build()
            .expect("pruned metadata")
            .metadata;
        let pruned_table = iceberg::table::Table::builder()
            .file_io(loaded.table.file_io().clone())
            .metadata(std::sync::Arc::new(pruned_metadata))
            .identifier(loaded.table.identifier().clone())
            .build()
            .expect("pruned table");

        // Request to_snapshot_id = s0 (expired)
        let from = s0 + 1; // doesn't matter, will Err on to before checking from
        let err = plan_changes(&pruned_table, from, Some(s0), &[]).expect_err("expired to");
        assert!(
            matches!(err, ChangeError::LineageBroken { previous_snapshot } if previous_snapshot == from),
            "expected LineageBroken, got {err:?}"
        );
    }
```

- [ ] **Step 5: Run the new tests**

Run: `cargo test -p novarocks --lib connector::iceberg::changes:: 2>&1 | tail -30`
Expected: all 7 tests pass (3 pre-existing + 4 new).

Note: the spec lists 5 new tests; the "regression: `to=None` ≡ old behavior" check is covered by the existing line 2250 test (already passes `None` after Task 2), so we only add 4 new ones here.

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "test(ivm-a2): cover plan_changes to_snapshot_id semantics

Adds 4 new unit tests:
- to=None ≡ to=Some(current)
- to is strict ancestor of from → LineageBroken
- middle-ancestor truncation drops post-to files
- to expired → LineageBroken"
```

---

## Task 4: Switch lowering caller to `Some(payload.to_snapshot_id)`, remove A2 guard

**Files:**
- Modify: `src/lower/node/iceberg_delta_scan.rs:90-135`

- [ ] **Step 1: Delete the A2 guard block and switch `plan_changes` call**

In `src/lower/node/iceberg_delta_scan.rs`, replace lines 90-135 (the `let current_snapshot_id = ...; if payload.to_snapshot_id != current_snapshot_id ...` block, plus the existing `plan_changes` call) with:

```rust
    // The snapshot interval is (from_snapshot_id, to_snapshot_id] semantically.
    // Lineage validation (to in metadata, from is a descendant ancestor of to)
    // is enforced by plan_changes / classify_lineage.
    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded.table,
        payload.from_snapshot_id,
        Some(payload.to_snapshot_id),
        &[],
    )
    .map_err(|e| {
        format!(
            "ivm-a1 lower delta-scan: plan_changes failed for {}.{}.{} from_snapshot={} to_snapshot={}: {e}",
            payload.catalog,
            payload.namespace,
            payload.table,
            payload.from_snapshot_id,
            payload.to_snapshot_id
        )
    })?;
```

This removes the entire `let current_snapshot_id = loaded.table.metadata().current_snapshot()...` block (lines 90-115 of the original file) and replaces it with just the `plan_changes` call. `loaded` is still used downstream (e.g., `loaded.table.file_io()`).

- [ ] **Step 2: Build and run iceberg-delta-scan-related tests**

Run: `cargo build -p novarocks --lib 2>&1 | tail -10`
Expected: build green.

Run: `cargo test -p novarocks --lib lower::node::iceberg_delta_scan:: 2>&1 | tail -20`
Expected: all existing lowering tests pass. Any test that specifically asserted the "to != current" Err message must be removed (search for "Pinning to a historical to_snapshot_id is reserved for A2" — if such a test exists, delete or rewrite to test the new permissive behavior).

- [ ] **Step 3: Check for tests asserting the removed guard**

Run: `grep -n "Pinning to a historical\|reserved for A2" src/`
Expected: no matches outside the spec doc. If there are matches in test files, delete those tests in this commit.

- [ ] **Step 4: Run full lib tests to catch any regression**

Run: `cargo test -p novarocks --lib 2>&1 | tail -10`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/lower/node/iceberg_delta_scan.rs
git commit -m "feat(ivm-a2): lower delta-scan accepts historical to_snapshot_id

Removes the A1-era guard that required to_snapshot_id == current_snapshot_id.
plan_changes now accepts an explicit (from, to) interval, and lineage
validation is delegated to classify_lineage (to in metadata, walk reaches
from). Unblocks future FE-side downpush of pinned to_snapshot_id."
```

---

## Task 5: Create `refresh_pin.rs` with `RefreshSnapshotPin` struct + capture

**Files:**
- Create: `src/connector/starrocks/managed/refresh_pin.rs`
- Modify: `src/connector/starrocks/managed/mod.rs` (declare module)

- [ ] **Step 1: Locate `mod.rs` and add module declaration**

Read `src/connector/starrocks/managed/mod.rs` to find an alphabetically natural spot (after `mv_shape`, before `mv_refresh` for example). Insert:

```rust
pub(crate) mod refresh_pin;
```

- [ ] **Step 2: Create `refresh_pin.rs` with the struct and capture function**

```rust
//! Refresh-scoped snapshot pin for iceberg-backed materialized views.
//!
//! `RefreshSnapshotPin` captures, at the start of a refresh, the
//! `current_snapshot_id` and `uuid` of every base table. The pin is the
//! single source of truth for snapshot ids during the refresh:
//!
//! * `plan_changes` uses pin[base] as its `to_snapshot_id`
//! * `begin_mv_refresh_intent` records pin as the refresh target
//! * `update_managed_mv_refresh_summary` writes `last_refresh_snapshots = pin`
//!
//! For single-base MVs (the only shape currently supported by the DDL gate),
//! this guarantees delta computation and bookkeeping agree on the same
//! snapshot, even if the base table commits concurrently during the refresh.
//!
//! For multi-base MVs (future), the pin additionally guarantees cross-table
//! consistency: every base table is read at the snapshot it had at refresh
//! start, regardless of intervening external commits.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::engine::StandaloneState;

/// Per-refresh snapshot pin: each base table is pinned to the
/// `current_snapshot_id` it had at refresh entry time.
#[derive(Clone, Debug, Default)]
pub(crate) struct RefreshSnapshotPin {
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
}

impl RefreshSnapshotPin {
    /// Capture the current snapshot id and uuid for each base table.
    ///
    /// Fails fast if any base table has no current snapshot — refresh
    /// against an empty iceberg table is not a supported flow at this
    /// layer; the caller is expected to handle that earlier.
    pub(crate) fn capture(
        state: &Arc<StandaloneState>,
        base_refs: &[IcebergTableRef],
    ) -> Result<Self, String> {
        let mut pin = RefreshSnapshotPin::default();
        for base_ref in base_refs {
            let loaded = crate::connector::starrocks::managed::mv_refresh::
                load_current_iceberg_base_table(state, base_ref)?;
            let snapshot_id = loaded
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .ok_or_else(|| {
                    format!(
                        "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                        base_ref.fqn()
                    )
                })?;
            pin.snapshots.insert(base_ref.fqn(), snapshot_id);
            pin.table_uuids
                .insert(base_ref.fqn(), loaded.table.metadata().uuid().to_string());
        }
        #[cfg(test)]
        invoke_after_capture_hook();
        Ok(pin)
    }

    pub(crate) fn get(&self, base: &IcebergTableRef) -> Option<i64> {
        self.snapshots.get(&base.fqn()).copied()
    }

    pub(crate) fn uuid(&self, base: &IcebergTableRef) -> Option<&str> {
        self.table_uuids.get(&base.fqn()).map(String::as_str)
    }

    pub(crate) fn len(&self) -> usize {
        self.snapshots.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, i64)> {
        self.snapshots.iter().map(|(k, v)| (k.as_str(), *v))
    }

    pub(crate) fn to_snapshot_map(&self) -> BTreeMap<String, i64> {
        self.snapshots.clone()
    }

    pub(crate) fn to_table_uuid_map(&self) -> BTreeMap<String, String> {
        self.table_uuids.clone()
    }
}

#[cfg(test)]
pub(crate) type AfterCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
fn after_capture_hook_slot() -> &'static std::sync::Mutex<Option<AfterCaptureHook>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<AfterCaptureHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_capture_hook() {
    let hook = after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock")
        .clone();
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
pub(crate) fn set_after_capture_hook(f: AfterCaptureHook) {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = Some(f);
}

#[cfg(test)]
pub(crate) fn clear_after_capture_hook() {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_get_and_iter_use_fqn_keys() {
        let mut pin = RefreshSnapshotPin::default();
        pin.snapshots.insert("ice.db.a".to_string(), 10);
        pin.snapshots.insert("ice.db.b".to_string(), 20);
        pin.table_uuids
            .insert("ice.db.a".to_string(), "uuid-a".to_string());
        pin.table_uuids
            .insert("ice.db.b".to_string(), "uuid-b".to_string());
        let a = IcebergTableRef {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "a".to_string(),
        };
        assert_eq!(pin.get(&a), Some(10));
        assert_eq!(pin.uuid(&a), Some("uuid-a"));
        assert_eq!(pin.len(), 2);
        assert!(!pin.is_empty());

        let snapshot_map = pin.to_snapshot_map();
        assert_eq!(snapshot_map.get("ice.db.a"), Some(&10));
        assert_eq!(snapshot_map.get("ice.db.b"), Some(&20));
    }

    #[test]
    fn after_capture_hook_round_trip() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_for_hook = Arc::clone(&flag);
        set_after_capture_hook(Arc::new(move || {
            flag_for_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        invoke_after_capture_hook();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        clear_after_capture_hook();
        flag.store(false, std::sync::atomic::Ordering::SeqCst);
        invoke_after_capture_hook();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p novarocks --lib 2>&1 | tail -10`
Expected: green.

- [ ] **Step 4: Run new tests**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::refresh_pin:: 2>&1 | tail -10`
Expected: 2 unit tests pass.

- [ ] **Step 5: cargo fmt + clippy + commit**

```bash
cargo fmt
cargo clippy -p novarocks --lib -- -D warnings 2>&1 | tail -10
git add src/connector/starrocks/managed/refresh_pin.rs src/connector/starrocks/managed/mod.rs
git commit -m "feat(ivm-a3): introduce RefreshSnapshotPin data structure

Per-refresh snapshot pin keyed by IcebergTableRef::fqn(). Captures
current_snapshot_id and uuid for every base table; fails fast on
missing current_snapshot. Includes #[cfg(test)] after_capture_hook
infra for deterministic concurrent-commit acceptance tests."
```

---

## Task 6: Add `plan_change_batches_for_pin` helper and retire `validate_change_batch_current_snapshot`

**Files:**
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`

- [ ] **Step 1: Replace the file contents (preserve existing tests in `#[cfg(test)]` block)**

In `src/connector/starrocks/managed/ivm_change_stream.rs`, replace lines 1-58 (everything before the `#[cfg(test)]` block) with:

```rust
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::connector::iceberg::changes::{
    ChangeError, IcebergChangeBatch, MaterializedChanges, plan_changes,
};
use crate::connector::starrocks::managed::model::IcebergTableRef;
use crate::connector::starrocks::managed::mv_refresh::load_current_iceberg_base_table;
use crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin;
use crate::engine::{QueryResult, StandaloneState};

// Compatibility wrapper for the older two-branch materialized change stream.
#[allow(dead_code)]
pub(crate) struct IvmChangeStream {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
impl IvmChangeStream {
    pub(crate) fn from_materialized(changes: MaterializedChanges) -> Self {
        Self {
            previous_snapshot_id: changes.previous_snapshot_id,
            current_snapshot_id: changes.current_snapshot_id,
            inserts: changes.inserts,
            deletes: changes.deletes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inserts.row_count() == 0 && self.deletes.row_count() == 0
    }

    pub(crate) fn into_results(self) -> (QueryResult, QueryResult) {
        (self.inserts, self.deletes)
    }
}

/// Plan an `IcebergChangeBatch` for a single base table pinned to
/// `expected_current_snapshot_id`. Thin wrapper over Layer 1 `plan_changes`
/// with the to_snapshot_id set explicitly from the pin; the previous
/// `validate_change_batch_current_snapshot` post-check is no longer needed
/// since plan_changes itself now writes the requested-to into the batch.
pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    plan_changes(
        base_table,
        previous_snapshot_id,
        Some(expected_current_snapshot_id),
        pk_columns,
    )
}

/// Plan one `IcebergChangeBatch` per base table in `pin`, using
/// `last_refresh[base.fqn()]` as `from` and `pin.get(base)` as `to`.
/// Returns batches in iteration order of the pin (sorted by fqn).
///
/// Fails fast on the first base table that:
/// - is missing from `last_refresh` (no previous refresh recorded)
/// - cannot be loaded (catalog or io error)
/// - returns any `ChangeError` from `plan_changes`
pub(crate) fn plan_change_batches_for_pin(
    state: &Arc<StandaloneState>,
    pin: &RefreshSnapshotPin,
    last_refresh: &BTreeMap<String, i64>,
    pk_columns_by_base: &HashMap<IcebergTableRef, Vec<String>>,
) -> Result<Vec<(IcebergTableRef, IcebergChangeBatch)>, String> {
    let mut out = Vec::with_capacity(pin.len());
    for (fqn, pinned_snap) in pin.iter() {
        let base_ref = parse_fqn_to_iceberg_ref(fqn)?;
        let previous = last_refresh.get(fqn).copied().ok_or_else(|| {
            format!(
                "plan_change_batches_for_pin: base table {fqn} missing from last_refresh"
            )
        })?;
        let loaded = load_current_iceberg_base_table(state, &base_ref)?;
        let pk_default: Vec<String> = Vec::new();
        let pk_columns = pk_columns_by_base.get(&base_ref).unwrap_or(&pk_default);
        let batch = plan_changes(
            &loaded.table,
            previous,
            Some(pinned_snap),
            pk_columns,
        )
        .map_err(|e| format!("plan_change_batches_for_pin: {fqn}: {e}"))?;
        debug_assert_eq!(batch.current_snapshot_id, pinned_snap);
        out.push((base_ref, batch));
    }
    Ok(out)
}

fn parse_fqn_to_iceberg_ref(fqn: &str) -> Result<IcebergTableRef, String> {
    let parts: Vec<&str> = fqn.split('.').collect();
    if parts.len() != 3 {
        return Err(format!(
            "expected 3-part fqn '<catalog>.<namespace>.<table>', got '{fqn}'"
        ));
    }
    Ok(IcebergTableRef {
        catalog: parts[0].to_string(),
        namespace: parts[1].to_string(),
        table: parts[2].to_string(),
    })
}
```

The existing `#[cfg(test)] mod tests {}` block at the bottom (starting around line 60) is preserved as-is, but **delete** the test at line 88 onward titled `validate_change_batch_current_snapshot_rejects_mismatch` — `validate_change_batch_current_snapshot` no longer exists.

- [ ] **Step 2: Verify the deletion**

Run: `grep -n "validate_change_batch_current_snapshot" src/`
Expected: no matches.

- [ ] **Step 3: Build**

Run: `cargo build -p novarocks --lib 2>&1 | tail -20`
Expected: green. If there are unused-import warnings around the old `validate_change_batch_current_snapshot` symbol, clean them up.

- [ ] **Step 4: Add unit tests for new helper**

Append to the `#[cfg(test)] mod tests {}` block in `ivm_change_stream.rs`:

```rust
    #[test]
    fn parse_fqn_to_iceberg_ref_round_trip() {
        let parsed = super::parse_fqn_to_iceberg_ref("ice.sales.orders").expect("parse");
        assert_eq!(parsed.catalog, "ice");
        assert_eq!(parsed.namespace, "sales");
        assert_eq!(parsed.table, "orders");
        assert_eq!(parsed.fqn(), "ice.sales.orders");
    }

    #[test]
    fn parse_fqn_to_iceberg_ref_rejects_non_three_part() {
        assert!(super::parse_fqn_to_iceberg_ref("ice.sales").is_err());
        assert!(super::parse_fqn_to_iceberg_ref("ice.sales.orders.extra").is_err());
        assert!(super::parse_fqn_to_iceberg_ref("orders").is_err());
    }
```

Real plan_change_batches_for_pin coverage requires a live Iceberg catalog and is exercised by Task 11 acceptance test; pure-unit-test value is limited (it's a thin wrapper).

- [ ] **Step 5: Run tests**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::ivm_change_stream:: 2>&1 | tail -20`
Expected: all green; previously dropped `validate_change_batch_current_snapshot_rejects_mismatch` no longer exists.

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add src/connector/starrocks/managed/ivm_change_stream.rs
git commit -m "feat(ivm-a3): add plan_change_batches_for_pin and retire validate helper

plan_change_batches_for_pin returns one IcebergChangeBatch per base in the
pin, sourced from (last_refresh[fqn], pin[base]). plan_iceberg_change_batch_for_ivm
becomes a thin wrapper. validate_change_batch_current_snapshot is deleted —
plan_changes itself now guarantees batch.current_snapshot_id equals the
requested to_snapshot_id."
```

---

## Task 7: Add `inject_pin_as_for_version_as_of` AST helper

**Files:**
- Modify: `src/connector/starrocks/managed/refresh_pin.rs`

- [ ] **Step 1: Append the helper function to `refresh_pin.rs`**

Add after the `RefreshSnapshotPin` impl block, before the `#[cfg(test)]` items:

```rust
use std::collections::HashSet;

use crate::connector::starrocks::managed::model::IcebergTableRef;

/// Walk `query` in place. For each `TableFactor::Table` whose 3-part name
/// resolves into the pin and is **not** in `delta_bearing`, set
/// `version = Some(VersionAsOf(Number(pin[base])))`. Returns the number
/// of mutations performed.
///
/// Rules:
/// - `TableFactor::Table` with `version = Some(_)` already → Err. The
///   refresh SELECT is not allowed to combine user-written FOR VERSION AS OF
///   with refresh pinning.
/// - Table not in pin → unchanged (likely a CTE, a different catalog, or
///   an alias not addressed by base_refs).
/// - Table in pin and ∈ delta_bearing → unchanged (handled by
///   mutate_query_for_ivm_delta_scan in iceberg_refresh.rs).
/// - Table in pin and ∉ delta_bearing → inject version.
///
/// In scope-B single-base MVs, the unique base is delta-bearing, so this
/// function is a no-op in production. It exists for the multi-base future.
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut sqlparser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<IcebergTableRef>,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<usize, String> {
    let mut state = InjectState {
        pin,
        delta_bearing,
        current_catalog,
        current_database,
        count: 0,
        first_error: None,
    };
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            walk_set_expr(cte.query.body.as_mut(), &mut state);
        }
    }
    walk_set_expr(query.body.as_mut(), &mut state);
    if let Some(err) = state.first_error {
        return Err(err);
    }
    Ok(state.count)
}

struct InjectState<'a> {
    pin: &'a RefreshSnapshotPin,
    delta_bearing: &'a HashSet<IcebergTableRef>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    count: usize,
    first_error: Option<String>,
}

fn walk_set_expr(expr: &mut sqlparser::ast::SetExpr, state: &mut InjectState<'_>) {
    use sqlparser::ast::SetExpr;
    if state.first_error.is_some() {
        return;
    }
    match expr {
        SetExpr::Select(select) => {
            for tw in &mut select.from {
                walk_factor(&mut tw.relation, state);
                for join in &mut tw.joins {
                    walk_factor(&mut join.relation, state);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left.as_mut(), state);
            walk_set_expr(right.as_mut(), state);
        }
        SetExpr::Query(q) => walk_set_expr(q.body.as_mut(), state),
        _ => {}
    }
}

fn walk_factor(factor: &mut sqlparser::ast::TableFactor, state: &mut InjectState<'_>) {
    use sqlparser::ast::{Expr, ObjectNamePart, TableFactor, TableVersion, Value};
    if state.first_error.is_some() {
        return;
    }
    match factor {
        TableFactor::Table {
            name,
            version,
            args,
            ..
        } => {
            // Skip table-valued functions (e.g. __nr_ivm_delta).
            if args.is_some() {
                return;
            }
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.to_ascii_lowercase()),
                    _ => None,
                })
                .collect();
            let Some(base_ref) = resolve_table_factor(
                &parts,
                state.current_catalog,
                state.current_database,
            ) else {
                return;
            };
            let Some(pinned) = state.pin.get(&base_ref) else {
                return; // not a base table
            };
            if state.delta_bearing.contains(&base_ref) {
                return; // delta-scan path handles this
            }
            if version.is_some() {
                state.first_error = Some(format!(
                    "refresh SELECT must not write explicit FOR VERSION AS OF for base table {}; \
                     refresh pin would conflict",
                    base_ref.fqn()
                ));
                return;
            }
            *version = Some(TableVersion::VersionAsOf(Expr::Value(
                Value::Number(pinned.to_string(), false).into(),
            )));
            state.count += 1;
        }
        TableFactor::Derived { subquery, .. } => {
            walk_set_expr(subquery.body.as_mut(), state);
        }
        _ => {}
    }
}

fn resolve_table_factor(
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<IcebergTableRef> {
    let current_database = current_database.to_ascii_lowercase();
    let current_catalog = current_catalog.map(|s| s.to_ascii_lowercase());
    match parts {
        [tbl] => current_catalog.map(|cat| IcebergTableRef {
            catalog: cat,
            namespace: current_database,
            table: tbl.clone(),
        }),
        [db, tbl] => current_catalog.map(|cat| IcebergTableRef {
            catalog: cat,
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        [cat, db, tbl] => Some(IcebergTableRef {
            catalog: cat.clone(),
            namespace: db.clone(),
            table: tbl.clone(),
        }),
        _ => None,
    }
}
```

- [ ] **Step 2: Add unit tests covering inject behavior**

Append into the `#[cfg(test)] mod tests {}` block of `refresh_pin.rs`:

```rust
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser as SqlParser;

    fn parse_select_for_test(sql: &str) -> sqlparser::ast::Query {
        let stmts = SqlParser::parse_sql(&GenericDialect {}, sql).expect("parse");
        if let sqlparser::ast::Statement::Query(q) = stmts.into_iter().next().unwrap() {
            *q
        } else {
            panic!("not a query");
        }
    }

    fn make_pin(entries: &[(&str, i64, &str)]) -> RefreshSnapshotPin {
        let mut pin = RefreshSnapshotPin::default();
        for (fqn, snap, uuid) in entries {
            pin.snapshots.insert((*fqn).to_string(), *snap);
            pin.table_uuids.insert((*fqn).to_string(), (*uuid).to_string());
        }
        pin
    }

    fn make_ref(c: &str, n: &str, t: &str) -> IcebergTableRef {
        IcebergTableRef {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    #[test]
    fn inject_pin_skips_delta_bearing_base() {
        let mut query = parse_select_for_test("SELECT * FROM ice.sales.orders");
        let pin = make_pin(&[("ice.sales.orders", 100, "u-orders")]);
        let mut delta_bearing = std::collections::HashSet::new();
        delta_bearing.insert(make_ref("ice", "sales", "orders"));
        let count = inject_pin_as_for_version_as_of(
            &mut query,
            &pin,
            &delta_bearing,
            Some("ice"),
            "sales",
        )
        .expect("ok");
        assert_eq!(count, 0);
        let rendered = query.to_string();
        assert!(!rendered.to_uppercase().contains("FOR VERSION AS OF"), "got: {rendered}");
    }

    #[test]
    fn inject_pin_injects_non_delta_bearing_base() {
        let mut query = parse_select_for_test(
            "SELECT r.id, s.label FROM ice.sales.orders r JOIN ice.sales.dim s ON r.dim_id = s.id"
        );
        let pin = make_pin(&[
            ("ice.sales.orders", 100, "u-orders"),
            ("ice.sales.dim", 200, "u-dim"),
        ]);
        let mut delta_bearing = std::collections::HashSet::new();
        delta_bearing.insert(make_ref("ice", "sales", "orders"));
        let count = inject_pin_as_for_version_as_of(
            &mut query,
            &pin,
            &delta_bearing,
            Some("ice"),
            "sales",
        )
        .expect("ok");
        assert_eq!(count, 1);
        let rendered = query.to_string();
        assert!(rendered.contains("FOR VERSION AS OF 200"), "got: {rendered}");
        // orders should not get version
        let lower = rendered.to_ascii_lowercase();
        let pos_orders = lower.find("ice.sales.orders").expect("orders");
        let pos_version = lower.find("for version as of").expect("version");
        assert!(pos_orders < pos_version);
        let between = &lower[pos_orders..pos_version];
        assert!(!between.contains("for version as of"), "version attached to orders, got: {rendered}");
    }

    #[test]
    fn inject_pin_skips_tables_not_in_pin() {
        let mut query = parse_select_for_test("SELECT * FROM other.ns.t");
        let pin = make_pin(&[("ice.sales.orders", 100, "u-orders")]);
        let delta_bearing = std::collections::HashSet::new();
        let count = inject_pin_as_for_version_as_of(
            &mut query,
            &pin,
            &delta_bearing,
            Some("ice"),
            "sales",
        )
        .expect("ok");
        assert_eq!(count, 0);
        let rendered = query.to_string();
        assert!(!rendered.to_uppercase().contains("FOR VERSION AS OF"));
    }

    #[test]
    fn inject_pin_rejects_existing_for_version_as_of() {
        let mut query = parse_select_for_test(
            "SELECT * FROM ice.sales.dim FOR VERSION AS OF 999"
        );
        let pin = make_pin(&[("ice.sales.dim", 200, "u-dim")]);
        let delta_bearing = std::collections::HashSet::new();
        let err = inject_pin_as_for_version_as_of(
            &mut query,
            &pin,
            &delta_bearing,
            Some("ice"),
            "sales",
        )
        .expect_err("must reject");
        assert!(err.contains("FOR VERSION AS OF") && err.contains("ice.sales.dim"), "err: {err}");
    }

    #[test]
    fn inject_pin_skips_table_valued_functions() {
        let mut query = parse_select_for_test(
            "SELECT * FROM __nr_ivm_delta('ice.sales.orders', 100, 200) AS r"
        );
        let pin = make_pin(&[("ice.sales.orders", 200, "u-orders")]);
        let delta_bearing = std::collections::HashSet::new();
        let count = inject_pin_as_for_version_as_of(
            &mut query,
            &pin,
            &delta_bearing,
            Some("ice"),
            "sales",
        )
        .expect("ok");
        assert_eq!(count, 0);
    }
```

- [ ] **Step 3: Build + test**

```bash
cargo fmt
cargo build -p novarocks --lib 2>&1 | tail -10
cargo test -p novarocks --lib connector::starrocks::managed::refresh_pin:: 2>&1 | tail -30
```
Expected: 2 (from Task 5) + 5 = 7 unit tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/refresh_pin.rs
git commit -m "feat(ivm-a3): inject_pin_as_for_version_as_of AST helper

Walks a parsed MV SELECT and attaches FOR VERSION AS OF <pin[base]> to
every base TableFactor that is not delta-bearing. Existing user-written
version clauses raise an Err (refresh SELECT must not combine pin with
explicit time travel). Skips table-valued functions (e.g. __nr_ivm_delta)
so the inject and the delta-scan mutation are commutative."
```

---

## Task 8: Wire `RefreshSnapshotPin::capture` into `refresh_iceberg_mv`

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs:121-200`

- [ ] **Step 1: Insert pin capture after `parse_iceberg_table_refs`**

In `src/connector/starrocks/managed/mv_refresh.rs`, find the block starting at line 121:

```rust
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "incremental materialized view refresh requires a single Iceberg base table"
                .to_string(),
        );
    };
    validate_incremental_mv_base_ref(mv_shape.base_table(), base_ref)?;

    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    let current_snapshot_id = loaded
        .table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
    let current_table_uuid = loaded.table.metadata().uuid().to_string();
```

Replace with:

```rust
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "incremental materialized view refresh requires a single Iceberg base table"
                .to_string(),
        );
    };
    validate_incremental_mv_base_ref(mv_shape.base_table(), base_ref)?;

    // Freeze the snapshot pin for the duration of this refresh. From now on
    // pin is the only source of snapshot ids for base table reads, delta
    // computation, intent recording, and bookkeeping.
    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, &base_refs,
    )?;
    let current_snapshot_id = pin.get(base_ref);
    let current_table_uuid = pin
        .uuid(base_ref)
        .ok_or_else(|| {
            format!(
                "refresh pin missing uuid for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?
        .to_string();
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
```

Note: `current_snapshot_id` retains type `Option<i64>` (because the rest of the function — `choose_snapshot_refresh_policy` etc — expects `Option`). Since `capture` fails if any base has no current snapshot, this Option is always `Some` here; but the surrounding policy code path tolerates `None` for "no snapshot yet" cases that no longer reach this code, so wrapping in `Some` via `pin.get()` preserves type compatibility without behavior change.

- [ ] **Step 2: Replace the two `single_snapshot_map` / `single_table_uuid_map` call sites**

Search the file for `single_snapshot_map(base_ref, ...)` and `single_table_uuid_map(base_ref, ...)`. Each occurrence inside `refresh_iceberg_mv` (lines ~166-191 and the metadata_only closure) should switch to use the pin.

For the snapshot map at line 166-169:

```rust
    if matches!(
        policy,
        MvRefreshPolicy::FullRefresh { .. } | MvRefreshPolicy::Incremental { .. }
    ) {
        begin_mv_refresh_intent(state, runtime.table.table_id, pin.to_snapshot_map())?;
    }
```

For the metadata_only closure (line 183-194 area):

```rust
        |current_snapshot_id| {
            let _ = current_snapshot_id; // value carried by pin, kept for closure signature
            update_managed_mv_refresh_summary(
                state,
                runtime.table.table_id,
                mv_definition.last_refresh_rows.unwrap_or(0),
                pin.to_snapshot_map(),
                pin.to_table_uuid_map(),
            )?;
            refresh_managed_catalog(state)?;
            Ok(StatementResult::Ok)
        },
```

For the projection_incremental closure (line 196 area) at its end where it constructs final snapshots — search for `single_snapshot_map` / `single_table_uuid_map` after the successful incremental commit, replace with pin-based maps.

For any other occurrences of `single_snapshot_map(base_ref, current_snapshot_id)` or `single_table_uuid_map(base_ref, &current_table_uuid)` in this function, replace similarly.

- [ ] **Step 3: Build (will likely have compile errors at closure capture, walk through and fix)**

```bash
cargo build -p novarocks --lib 2>&1 | tail -30
```

Common issues to fix:
- `pin` borrow inside closure that lives past `pin` — use `pin.to_snapshot_map()` inside `move` closures (clones internally).
- `base_ref` borrowed inside `Fn(i64, i64)` closure may already be `&IcebergTableRef`. If so, `pin.get(base_ref)` may need to be moved out into a local before the closure.

Re-run build until green.

- [ ] **Step 4: Verify no behavior change in the static (non-concurrent) path**

Run: `cargo test -p novarocks --lib connector::starrocks::managed::mv_refresh:: 2>&1 | tail -20`
Expected: all existing tests still pass. The pin captures the same snapshot the inline lookup used to capture, so behavior is unchanged outside of concurrent-commit scenarios.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/connector/starrocks/managed/mv_refresh.rs
git commit -m "feat(ivm-a3): freeze RefreshSnapshotPin at refresh_iceberg_mv entry

Pin captures (current_snapshot_id, uuid) for the base table at refresh
start. begin_mv_refresh_intent and update_managed_mv_refresh_summary now
write pin.to_snapshot_map() / pin.to_table_uuid_map() instead of inline
lookups, ensuring bookkeeping and delta computation share a single
snapshot source-of-truth."
```

---

## Task 9: Wire pin into `engine/mv/iceberg_refresh.rs` create + refresh paths

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (lines ~95-110, 590-610, 2400-2470)

- [ ] **Step 1: Pin freeze at refresh entry (line 590-area)**

Find the function around line 589 (search for `parse_iceberg_table_refs(&mv_definition.base_table_refs)` followed by `let [base_ref] = base_refs.as_slice()`). Add `RefreshSnapshotPin::capture` immediately after, and replace inline `current_snapshot_id` derivations with `pin.get(base_ref)`.

```rust
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else {
        return Err("...".to_string());
    };
    let pin = crate::connector::starrocks::managed::refresh_pin::RefreshSnapshotPin::capture(
        state, &base_refs,
    )?;
    let current_snapshot_id = pin.get(base_ref);
```

(Adjust to match the surrounding code shape; this is a search-and-rewrite pattern.)

- [ ] **Step 2: Pin into `incremental_refresh_iceberg_mv` (line ~2420)**

The legacy `incremental_refresh_iceberg_mv` function signature already takes `previous_snapshot_id` and `current_snapshot_id`. Add `pin: &RefreshSnapshotPin` if needed, OR keep the signature and have the caller pass `pin.get(base_ref).unwrap()` already.

Look at the existing call:

```rust
let batch = match plan_changes(base_table, previous_snapshot_id, None, &[]) {
```

Change to:

```rust
let batch = match plan_changes(base_table, previous_snapshot_id, Some(current_snapshot_id), &[]) {
```

This is the actual A2 payoff in the legacy path: `to_snap` is now passed from caller (which itself derives from pin at refresh entry).

- [ ] **Step 3: Build + run lib tests**

```bash
cargo build -p novarocks --lib 2>&1 | tail -10
cargo test -p novarocks --lib engine::mv::iceberg_refresh:: 2>&1 | tail -20
```
Expected: green; existing tests unchanged.

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(ivm-a3): wire pin into iceberg_refresh entry and legacy incremental path

incremental_refresh_iceberg_mv now passes Some(current_snapshot_id) into
plan_changes — the value originates from pin captured at refresh entry,
not from re-reading metadata.current_snapshot() inside plan_changes."
```

---

## Task 10: Bookkeeping correctness unit test

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs` (extend `#[cfg(test)] mod tests {}`)

- [ ] **Step 1: Add bookkeeping invariant test**

Search the existing `#[cfg(test)] mod tests {}` block in `mv_refresh.rs` for a test that constructs a minimal iceberg base table and runs an incremental refresh end-to-end (one of the existing tests around line 1730 will be a template). Add a new test alongside:

```rust
    #[test]
    fn last_refresh_snapshots_equals_pin_not_post_refresh_current() {
        // This test verifies that after a successful incremental refresh, the
        // mv_definition.last_refresh_snapshots map reflects the snapshot that
        // was pinned at refresh start — not whatever the base table's current
        // snapshot happens to be by the time bookkeeping is written. Under the
        // pre-A3 architecture these could diverge; under A3 they must equal.

        // Setup: create a single-base iceberg MV, do an initial full refresh
        // (last_refresh = s0), then INSERT a row to produce s1, then trigger
        // incremental refresh with an after_capture_hook that INSERTs another
        // row to produce s2 mid-refresh.

        // The exact fixture wiring mirrors the existing tests in this file.
        // Sketch (fill from local test helpers in this module):
        //
        //   let (state, mv_table_id, base_ref) = make_minimal_iceberg_mv_fixture();
        //   refresh_full(&state, &base_ref);                 // last_refresh = s0
        //   external_insert(&state, &base_ref, vec![1, 2]);  // s1
        //
        //   let base_ref_clone = base_ref.clone();
        //   let state_clone = state.clone();
        //   refresh_pin::set_after_capture_hook(Arc::new(move || {
        //       external_insert(&state_clone, &base_ref_clone, vec![3, 4]);  // s2
        //   }));
        //   refresh_incremental(&state, mv_table_id);
        //   refresh_pin::clear_after_capture_hook();
        //
        //   let mv_def = load_mv_definition(&state, mv_table_id);
        //   let recorded = mv_def.last_refresh_snapshots[&base_ref.fqn()];
        //   assert_eq!(recorded, snapshot_id_of_s1, "must equal pin, not s2");
    }
```

Implementation note: the comment block describes the test shape. The actual implementation reuses existing helpers in `mv_refresh.rs::tests`. If the existing module doesn't have `make_minimal_iceberg_mv_fixture` / `external_insert` etc., adapt from the closest existing test (around line 1700-1730).

- [ ] **Step 2: Compile-shape check**

```bash
cargo test -p novarocks --lib connector::starrocks::managed::mv_refresh::tests::last_refresh_snapshots_equals_pin_not_post_refresh_current --no-run 2>&1 | tail -10
```
Expected: compiles.

- [ ] **Step 3: Run the test**

```bash
cargo test -p novarocks --lib connector::starrocks::managed::mv_refresh::tests::last_refresh_snapshots_equals_pin_not_post_refresh_current -- --nocapture 2>&1 | tail -30
```
Expected: passes.

- [ ] **Step 4: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs
git commit -m "test(ivm-a3): assert last_refresh_snapshots tracks pin, not post-refresh current

Uses RefreshSnapshotPin::after_capture_hook to inject a concurrent INSERT
between pin freeze and plan_changes. Verifies that the post-refresh
bookkeeping records the pinned snapshot, not the post-insert one."
```

---

## Task 11: End-to-end acceptance test — `pin_freeze_against_concurrent_commit`

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs` (tests module)

- [ ] **Step 1: Add the acceptance test scaffolding**

Add (next to the bookkeeping test from Task 10) in `mv_refresh.rs::tests`:

```rust
    #[test]
    fn pin_freeze_against_concurrent_commit() {
        // Acceptance test for IVM-A3 spec §10.2.
        //
        // Flow:
        //  s0: empty base, full refresh, last_refresh=s0
        //  s1: external INSERT (3, 30)
        //  s2 (concurrent): after_capture_hook fires during incremental
        //                   refresh's pin freeze, inserts (4, 40)
        //  Refresh continues with pin captured at s1.
        //
        // Assertions:
        //  - mv content = {(1,11), (2,21), (3,31)}, NO (4,41)
        //  - mv_definition.last_refresh_snapshots[base] == s1
        //  - A subsequent refresh produces (4,41), last_refresh_snapshots == s2

        // Implementation reuses the same fixture builders as the existing
        // refresh tests in this module. Skeleton:
        //
        //   let (state, db, mv_name, base_ref) = make_iceberg_mv_fixture(
        //       "CREATE MATERIALIZED VIEW mv AS SELECT id, v+1 AS v1 FROM ice.db.t"
        //   );
        //   exec(&state, "INSERT INTO ice.db.t VALUES (1, 10), (2, 20)");
        //   exec(&state, &format!("REFRESH MATERIALIZED VIEW {db}.{mv_name}"));
        //   let s0 = current_snapshot_of(&state, &base_ref);
        //   exec(&state, "INSERT INTO ice.db.t VALUES (3, 30)");
        //   let s1 = current_snapshot_of(&state, &base_ref);
        //
        //   let state_clone = state.clone();
        //   let base_ref_clone = base_ref.clone();
        //   crate::connector::starrocks::managed::refresh_pin::set_after_capture_hook(
        //       std::sync::Arc::new(move || {
        //           exec(&state_clone, "INSERT INTO ice.db.t VALUES (4, 40)");
        //       }),
        //   );
        //   exec(&state, &format!("REFRESH MATERIALIZED VIEW {db}.{mv_name}"));
        //   crate::connector::starrocks::managed::refresh_pin::clear_after_capture_hook();
        //
        //   let rows = query_mv(&state, &db, &mv_name);
        //   assert!(rows_contain(&rows, &[(1,11), (2,21), (3,31)]));
        //   assert!(!rows_contain(&rows, &[(4,41)]), "concurrent insert leaked into pinned refresh");
        //   let recorded = load_mv_definition(&state, mv_table_id).last_refresh_snapshots[&base_ref.fqn()];
        //   assert_eq!(recorded, s1, "last_refresh must equal pin (s1), not s2");
        //
        //   // Follow-up refresh
        //   exec(&state, &format!("REFRESH MATERIALIZED VIEW {db}.{mv_name}"));
        //   let rows = query_mv(&state, &db, &mv_name);
        //   assert!(rows_contain(&rows, &[(4,41)]));
        //   let s2 = current_snapshot_of(&state, &base_ref);
        //   let recorded = load_mv_definition(&state, mv_table_id).last_refresh_snapshots[&base_ref.fqn()];
        //   assert_eq!(recorded, s2);
    }
```

Implementation note: replace the skeleton with the actual fixture helpers. Look for "iceberg_v3_mor_update_preserves_row_id" (`src/engine/mod.rs:6306`-ish) or any test under `mv_refresh.rs::tests` that builds a memory Iceberg catalog and runs INSERT + REFRESH — those will have the helpers (`open_iceberg_session_with_table` or similar) you can lift.

- [ ] **Step 2: Run the test**

```bash
cargo test -p novarocks --lib connector::starrocks::managed::mv_refresh::tests::pin_freeze_against_concurrent_commit -- --nocapture 2>&1 | tail -60
```

Expected: passes. If hook ordering is wrong (e.g. hook fires after `plan_changes` rather than after pin capture but before plan_changes), the test will fail with `(4, 41)` showing up in `rows`. Trace `RefreshSnapshotPin::capture`'s hook call site and confirm it fires AFTER snapshots are read into `pin` but BEFORE the function returns — i.e., we want the `invoke_after_capture_hook()` to happen before the `Ok(pin)` return.

- [ ] **Step 3: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs
git commit -m "test(ivm-a3): end-to-end pin freeze against concurrent base-table commit

Verifies that an external INSERT committed between pin capture and
plan_changes does not leak into the current incremental refresh result,
and that last_refresh_snapshots records the pinned snapshot. A follow-up
refresh picks up the post-pin commit normally."
```

---

## Task 12: SQL suite regression run + iceberg-rest smoke case

**Files:**
- Maybe modify: `tests/sql/iceberg-rest/...` (add a smoke case if absent)

- [ ] **Step 1: Identify existing iceberg-rest incremental refresh case**

```bash
ls tests/sql/iceberg-rest/ 2>/dev/null | head -20
grep -rn "REFRESH MATERIALIZED VIEW" tests/sql/iceberg-rest/ 2>/dev/null | head
```

If there's already an incremental refresh case, this step is a no-op.

- [ ] **Step 2: Boot the docker fixture and standalone-server**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo build 2>&1 | tail -5
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then
    echo "standalone-server died during startup; tail of $LOG:" >&2
    tail -20 "$LOG" >&2
    exit 1
  fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timed out waiting for NOVAROCKS_READY" >&2; kill -9 "$SRV_PID"; exit 1; }
```

- [ ] **Step 3: Run iceberg + iceberg-rest suites**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify 2>&1 | tail -20
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify 2>&1 | tail -20
```

Expected: both suites pass (all-green). If any case fails, investigate before continuing — the most likely failure modes are:
- Some assertion comparing exact `current_snapshot_id` values now mismatches because of pin freeze (acceptable — fix the test fixture).
- Existing test that relied on `validate_change_batch_current_snapshot` Err message — already deleted in Task 6.

- [ ] **Step 4: Shut down server, run `cargo test` lib once more**

```bash
kill "$SRV_PID" 2>/dev/null
cargo fmt --check
cargo clippy -p novarocks --lib -- -D warnings 2>&1 | tail -10
cargo test -p novarocks --lib 2>&1 | tail -10
```
Expected: all green.

- [ ] **Step 5: Final commit if any fixture / test fix was needed**

```bash
git status
# If anything modified beyond test fixtures, review and commit:
git add <changed files>
git commit -m "test(ivm-a2-a3): adjust iceberg suite expectations for pin-driven refresh"
# Otherwise skip.
```

---

## Self-Review

**Spec coverage check** (spec sections → plan tasks):

| Spec section | Plan coverage |
|---|---|
| §5.1 classify_lineage signature | Task 1 |
| §5.2 plan_changes signature | Task 2 |
| §5.3 LineageBroken three scenarios | Task 1 (to-expired) + Task 3 (strict ancestor) |
| §5.4 Caller switch to None first, then Some | Task 2 (None) + Task 4 (lowering Some) + Task 9 (legacy Some) |
| §5.5 5 new unit tests | Task 3 has 4 new; the to=None ≡ old behavior regression test is implicit (Task 2 callers pass None and existing tests still pass) — covered |
| §6.1 plan_change_batches_for_pin | Task 6 |
| §6.2 plan_iceberg_change_batch_for_ivm thin-wrap | Task 6 |
| §6.3 validate helper deletion | Task 6 step 1 |
| §6.4 helper unit tests | Task 6 step 4 (parse_fqn tests; full live-catalog coverage by Task 11 acceptance) |
| §7.1 RefreshSnapshotPin struct + capture | Task 5 |
| §7.1 after_capture_hook | Task 5 |
| §7.2 inject_pin_as_for_version_as_of | Task 7 |
| §7.3 scope-B no-op in production | Tested in Task 7 step 2 (`inject_pin_skips_delta_bearing_base`) |
| §8.1 pin freeze timing in refresh_iceberg_mv | Task 8 |
| §8.1 pin freeze in iceberg_refresh.rs entries | Task 9 |
| §8.2 dispatcher closure shape | Task 8 step 2 (replace single_*_map with pin) — closure signatures kept as-is |
| §8.3 bookkeeping correctness | Tasks 8, 9, 10 |
| §8.4 failure modes | Implicit (capture fails on missing current snapshot — coded in Task 5; other paths preserved) |
| §8.5 gate retention | Confirmed by NOT modifying mv_ddl:167 or other gates |
| §9 SQL pipeline forms | Covered by Tasks 4 (delta-scan) + 7 (FOR VERSION AS OF inject); no new pipeline plumbing needed because A1 left the right hooks |
| §10.1 unit test matrix | Tasks 3, 5, 6, 7, 10 |
| §10.2 acceptance test | Task 11 |
| §10.3 SQL suite regression | Task 12 |
| §13 PR order | Tasks numbered to match |

**Placeholder scan**: no "TBD", "TODO", "fill in details", "similar to Task N" placeholders. The Task 10 / Task 11 skeletons reference local test helpers that must be adapted from sibling tests in the same file — this is intentional context, not a placeholder (the engineer needs to read the surrounding test code anyway to find the right helpers).

**Type consistency**:
- `RefreshSnapshotPin::get(&IcebergTableRef) -> Option<i64>` consistent across Tasks 5, 7, 8, 9
- `pin.to_snapshot_map() -> BTreeMap<String, i64>` consistent
- `inject_pin_as_for_version_as_of(...) -> Result<usize, String>` consistent
- `plan_changes(table, from: i64, to: Option<i64>, pk: &[String]) -> Result<IcebergChangeBatch, ChangeError>` consistent across Tasks 1, 2, 4, 6, 9

Plan is internally consistent. Ready for execution.

---

## Open issues noted during planning

1. **`CurrentBaseMetadata` duplication** ([mv_refresh.rs:1231](../../../src/connector/starrocks/managed/mv_refresh.rs:1231)): an existing internal struct with the same shape as `RefreshSnapshotPin` but a softer contract (skips bases with no current snapshot). Used only by the managed-lake MV recovery path. This plan does **not** consolidate the two — that's a follow-up cleanup. Documented as known duplication.

2. **`single_snapshot_map` / `single_table_uuid_map` helpers** ([mv_refresh.rs:1212-1228](../../../src/connector/starrocks/managed/mv_refresh.rs:1212)): not deleted by this PR. After Task 8 they have zero call sites in `refresh_iceberg_mv` but may still be called from managed-lake MV refresh paths. Leave for now; remove in a future cleanup PR.

3. **Existing test fixture builders in `mv_refresh.rs::tests`**: this plan refers to "sibling tests" for fixture patterns (Tasks 10, 11). The engineer must skim the existing test module to find the right builders. This is reasonable given the size of that test module and the impossibility of duplicating every helper.
