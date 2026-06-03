# IV3-2 Snapshot Summary `total-*` Carry-Forward — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Iceberg commit-action write a complete, correctly carried-forward set of the six snapshot-summary `total-*` fields (`total-data-files`, `total-delete-files`, `total-records`, `total-files-size`, `total-position-deletes`, `total-equality-deletes`) and stamp NovaRocks engine identity (`engine-name`/`engine-version`).

**Architecture:** Add one shared helper `finalize_snapshot_summary` in `src/connector/iceberg/commit/helpers.rs` that computes `total = previous_total + added − removed` from the canonical `added-*`/`removed-*`/`deleted-*` keys each action already emits (mirroring Iceberg-Java/Spark `SnapshotSummary` semantics, including legacy-omit and TRUNCATE-reset). Each commit-action call-site wraps its existing summary map with this helper (passing the previous snapshot's `Summary`), after normalizing a few non-canonical keys and adding the few missing `removed-*` size/record counts so totals compute correctly.

**Tech Stack:** Rust, vendored `iceberg-rust 0.9.0` (`iceberg::spec::{Summary, Operation}`), existing commit-action unit-test harness (`commit/test_helpers.rs`), SQL regression runner (`sql-tests/iceberg`).

**Scope note:** This plan is **Part A1** of the approved combined spec `docs/superpowers/specs/2026-06-03-iv3-2-iv3-8-iceberg-metadata-design.md`. It covers IV3-2 §A1–A3 + A7 (summary totals + engine identity + error handling) and the IV3-2 self-consistency verification. IV3-2 §A4 (`last-partition-id`/`last-column-id` monotonic assertion) and §A5 (sort-order consistency) are a small follow-up plan; IV3-8 (`$files`/`$manifests`/`$entries`) is Plan 2.

---

## Canonical key reference (used throughout)

The helper reads these keys (Iceberg standard names). Each `total-X` is computed from its `added`/`removed` pair:

| total key | added key | removed key |
|---|---|---|
| `total-data-files` | `added-data-files` | `deleted-data-files` |
| `total-delete-files` | `added-delete-files` | `removed-delete-files` |
| `total-records` | `added-records` | `deleted-records` |
| `total-files-size` | `added-files-size` | `removed-files-size` |
| `total-position-deletes` | `added-position-deletes` | `removed-position-deletes` |
| `total-equality-deletes` | `added-equality-deletes` | `removed-equality-deletes` |

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/connector/iceberg/commit/helpers.rs` | Shared commit utilities | **Add** key constants + `finalize_snapshot_summary` + `carry_total`/`parse_u64` + unit tests |
| `src/connector/iceberg/commit/fast_append.rs` | APPEND summary | Wrap call-site with helper |
| `src/connector/iceberg/commit/overwrite.rs` | OVERWRITE summary | Add `removed-files-size`; wrap |
| `src/connector/iceberg/commit/row_delta.rs` | row-delta (v2 delete) summary | Drop direct `total-equality-deletes`; wrap |
| `src/connector/iceberg/commit/row_delta_dv.rs` | v3 DV delete summary | Thread replaced-DV size; wrap |
| `src/connector/iceberg/commit/overwrite_partitions.rs` | dynamic-overwrite summary | Rename `removed-data-files`→`deleted-data-files`; add removed delete-record/file counts; wrap |
| `src/connector/iceberg/commit/rewrite_manifests.rs` | rewrite-manifests summary | Wrap inline summary |
| `src/connector/iceberg/commit/rewrite_data_files.rs` | rewrite-data-files summary | Drop wrong `total-records`; add files-size; wrap |
| `src/connector/iceberg/commit/truncate.rs` | TRUNCATE summary | Drop hard-coded `total-records`; wrap (truncate path) |
| `sql-tests/iceberg/sql/iceberg_snapshot_summary_totals.sql` + `result/…` | SQL self-consistency golden | **Create** |

---

## Task 1: Shared `finalize_snapshot_summary` helper

**Files:**
- Modify: `src/connector/iceberg/commit/helpers.rs` (append at end of file, before nothing — file currently ends at line 156)
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests**

Append to `src/connector/iceberg/commit/helpers.rs`:

```rust
#[cfg(test)]
mod summary_tests {
    use super::*;
    use iceberg::spec::{Operation, Summary};
    use std::collections::HashMap;

    fn prev(props: &[(&str, &str)]) -> Summary {
        Summary {
            operation: Operation::Append,
            additional_properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn first_snapshot_establishes_totals_from_added() {
        let out = finalize_snapshot_summary(
            props(&[
                ("added-data-files", "3"),
                ("added-records", "30"),
                ("added-files-size", "300"),
            ]),
            None,
            false,
        );
        assert_eq!(out.get("total-data-files").unwrap(), "3");
        assert_eq!(out.get("total-records").unwrap(), "30");
        assert_eq!(out.get("total-files-size").unwrap(), "300");
        assert_eq!(out.get("total-delete-files").unwrap(), "0");
        assert_eq!(out.get("total-position-deletes").unwrap(), "0");
        assert_eq!(out.get("total-equality-deletes").unwrap(), "0");
        assert_eq!(out.get("engine-name").unwrap(), "novarocks");
        assert!(out.get("engine-version").unwrap().starts_with("novarocks-"));
    }

    #[test]
    fn carry_forward_adds_and_subtracts() {
        let previous = prev(&[
            ("total-data-files", "10"),
            ("total-records", "100"),
            ("total-files-size", "1000"),
            ("total-delete-files", "0"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "0"),
        ]);
        let out = finalize_snapshot_summary(
            props(&[
                ("added-data-files", "2"),
                ("deleted-data-files", "1"),
                ("added-records", "20"),
                ("deleted-records", "5"),
                ("added-files-size", "200"),
                ("removed-files-size", "100"),
            ]),
            Some(&previous),
            false,
        );
        assert_eq!(out.get("total-data-files").unwrap(), "11"); // 10 + 2 - 1
        assert_eq!(out.get("total-records").unwrap(), "115"); // 100 + 20 - 5
        assert_eq!(out.get("total-files-size").unwrap(), "1100"); // 1000 + 200 - 100
    }

    #[test]
    fn legacy_missing_total_is_omitted_not_fabricated() {
        // previous snapshot predates totals: it has total-records but NOT
        // total-data-files. We must carry total-records and OMIT total-data-files.
        let previous = prev(&[("total-records", "100")]);
        let out = finalize_snapshot_summary(
            props(&[("added-data-files", "2"), ("added-records", "20")]),
            Some(&previous),
            false,
        );
        assert!(
            !out.contains_key("total-data-files"),
            "must not fabricate a total we cannot resume"
        );
        assert_eq!(out.get("total-records").unwrap(), "120");
    }

    #[test]
    fn truncate_resets_all_totals_to_zero() {
        let previous = prev(&[
            ("total-data-files", "10"),
            ("total-records", "100"),
            ("total-files-size", "1000"),
        ]);
        let out = finalize_snapshot_summary(
            props(&[("deleted-data-files", "10"), ("deleted-records", "100")]),
            Some(&previous),
            true,
        );
        for k in [
            "total-data-files",
            "total-delete-files",
            "total-records",
            "total-files-size",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            assert_eq!(out.get(k).map(String::as_str), Some("0"), "{k} must be 0");
        }
    }

    #[test]
    fn removed_below_zero_saturates() {
        let previous = prev(&[("total-records", "5")]);
        let out = finalize_snapshot_summary(
            props(&[("deleted-records", "9")]),
            Some(&previous),
            false,
        );
        assert_eq!(out.get("total-records").unwrap(), "0");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p novarocks --lib commit::helpers::summary_tests 2>&1 | tail -20`
Expected: FAIL to compile — `cannot find function finalize_snapshot_summary in this scope`.

- [ ] **Step 3: Implement the helper**

Append to `src/connector/iceberg/commit/helpers.rs` (above the `#[cfg(test)] mod summary_tests` block you just added). Note `use std::collections::HashMap;` and `use iceberg::spec::Summary;` — add them to the existing `use` block at the top of the file (the file already has `use iceberg::spec::{FormatVersion, ManifestFile, ManifestListWriter};` at line 22; extend it to `use iceberg::spec::{FormatVersion, ManifestFile, ManifestListWriter, Summary};` and add `use std::collections::HashMap;`).

```rust
// ---------------------------------------------------------------------------
// Snapshot-summary `total-*` carry-forward (IV3-2).
//
// Canonical Iceberg summary key names. Mirrors the constants in
// `vendor/iceberg-0.9.0/src/spec/snapshot_summary.rs`.
// ---------------------------------------------------------------------------
const TOTAL_DATA_FILES: &str = "total-data-files";
const TOTAL_DELETE_FILES: &str = "total-delete-files";
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";
const TOTAL_POSITION_DELETES: &str = "total-position-deletes";
const TOTAL_EQUALITY_DELETES: &str = "total-equality-deletes";

const ADDED_DATA_FILES: &str = "added-data-files";
const DELETED_DATA_FILES: &str = "deleted-data-files";
const ADDED_DELETE_FILES: &str = "added-delete-files";
const REMOVED_DELETE_FILES: &str = "removed-delete-files";
const ADDED_RECORDS: &str = "added-records";
const DELETED_RECORDS: &str = "deleted-records";
const ADDED_FILE_SIZE: &str = "added-files-size";
const REMOVED_FILE_SIZE: &str = "removed-files-size";
const ADDED_POSITION_DELETES: &str = "added-position-deletes";
const REMOVED_POSITION_DELETES: &str = "removed-position-deletes";
const ADDED_EQUALITY_DELETES: &str = "added-equality-deletes";
const REMOVED_EQUALITY_DELETES: &str = "removed-equality-deletes";

const ENGINE_NAME_KEY: &str = "engine-name";
const ENGINE_VERSION_KEY: &str = "engine-version";
const ENGINE_NAME_VALUE: &str = "novarocks";

/// Carry forward the six Iceberg `total-*` summary fields and stamp NovaRocks
/// engine identity, returning the finalized snapshot-summary property map.
///
/// For each category, `total = previous_total + added - removed`, reading the
/// canonical `added-*` / `removed-*` / `deleted-*` keys the caller already
/// populated. Semantics mirror Iceberg-Java `SnapshotSummary` (and therefore
/// Spark), the cross-engine reference:
///
/// * First snapshot (`previous == None`): base 0, so `total == added`.
/// * `previous` present but missing a given `total-*` (legacy / foreign
///   writer): that total is OMITTED — we never fabricate a total we cannot
///   resume. (This intentionally differs from iceberg-rust 0.9.0
///   `update_totals`, which treats a missing previous total as 0.)
/// * `truncate_full_table`: every `total-*` resets to 0.
///
/// Engine identity (`engine-name`/`engine-version`) is always stamped.
pub(super) fn finalize_snapshot_summary(
    mut props: HashMap<String, String>,
    previous: Option<&Summary>,
    truncate_full_table: bool,
) -> HashMap<String, String> {
    if truncate_full_table {
        for key in [
            TOTAL_DATA_FILES,
            TOTAL_DELETE_FILES,
            TOTAL_RECORDS,
            TOTAL_FILE_SIZE,
            TOTAL_POSITION_DELETES,
            TOTAL_EQUALITY_DELETES,
        ] {
            props.insert(key.to_string(), "0".to_string());
        }
    } else {
        carry_total(&mut props, previous, TOTAL_DATA_FILES, ADDED_DATA_FILES, DELETED_DATA_FILES);
        carry_total(&mut props, previous, TOTAL_DELETE_FILES, ADDED_DELETE_FILES, REMOVED_DELETE_FILES);
        carry_total(&mut props, previous, TOTAL_RECORDS, ADDED_RECORDS, DELETED_RECORDS);
        carry_total(&mut props, previous, TOTAL_FILE_SIZE, ADDED_FILE_SIZE, REMOVED_FILE_SIZE);
        carry_total(
            &mut props,
            previous,
            TOTAL_POSITION_DELETES,
            ADDED_POSITION_DELETES,
            REMOVED_POSITION_DELETES,
        );
        carry_total(
            &mut props,
            previous,
            TOTAL_EQUALITY_DELETES,
            ADDED_EQUALITY_DELETES,
            REMOVED_EQUALITY_DELETES,
        );
    }
    props.insert(ENGINE_NAME_KEY.to_string(), ENGINE_NAME_VALUE.to_string());
    props.insert(
        ENGINE_VERSION_KEY.to_string(),
        crate::version::short_version().to_string(),
    );
    props
}

fn parse_u64_prop(props: &HashMap<String, String>, key: &str) -> u64 {
    props
        .get(key)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

fn carry_total(
    props: &mut HashMap<String, String>,
    previous: Option<&Summary>,
    total_key: &str,
    added_key: &str,
    removed_key: &str,
) {
    let base = match previous {
        // First snapshot ever: establish totals from this commit's deltas.
        None => 0u64,
        Some(prev) => match prev.additional_properties.get(total_key) {
            Some(value) => match value.parse::<u64>() {
                Ok(parsed) => parsed,
                // Unparseable previous total -> cannot resume; omit.
                Err(_) => return,
            },
            // Legacy / foreign snapshot without this total -> omit going forward.
            None => return,
        },
    };
    let added = parse_u64_prop(props, added_key);
    let removed = parse_u64_prop(props, removed_key);
    let total = base.saturating_add(added).saturating_sub(removed);
    props.insert(total_key.to_string(), total.to_string());
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p novarocks --lib commit::helpers::summary_tests 2>&1 | tail -20`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/helpers.rs
git commit -m "feat(iceberg-commit): add finalize_snapshot_summary total-* carry-forward helper"
```

---

## Task 2: Wire FastAppend through the helper

**Files:**
- Modify: `src/connector/iceberg/commit/fast_append.rs:39` (import) and the `append_summary` call-site (~line 362)

- [ ] **Step 1: Add an assertion to the existing append test**

Find the existing fast_append test that asserts on `snap.summary().additional_properties` (around lines 541–567). Add these assertions for a single-batch append of files totalling a known record count (the existing test already commits an append; reuse its `p` binding to the committed snapshot summary):

```rust
        assert_eq!(p.get("total-data-files").map(String::as_str), Some("1"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
        assert!(
            p.get("total-files-size").is_some(),
            "append must carry total-files-size"
        );
```

(Adjust the `total-data-files` expected value to match however many files the existing test appends.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::fast_append 2>&1 | tail -20`
Expected: FAIL — `total-data-files` / `engine-name` assertion fails (helper not wired yet).

- [ ] **Step 3: Wire the helper at the call-site**

In the `use super::helpers::{ ... };` block (starts line 39), add `finalize_snapshot_summary` to the imported items.

Replace the `append_summary` call-site (~lines 362–366):

```rust
    let additional_properties = merge_snapshot_summary_properties(
        append_summary(&self.written, total_records),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

with:

```rust
    let additional_properties = merge_snapshot_summary_properties(
        finalize_snapshot_summary(
            append_summary(&self.written, total_records),
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

(`m` is `table.metadata()`, already bound at line 341. `append_summary` keeps emitting `total-records`; the helper recomputes the identical value and adds the other five totals + engine identity.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::fast_append 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/fast_append.rs
git commit -m "feat(iceberg-commit): carry total-* through FastAppend summary"
```

---

## Task 3: Wire Overwrite + emit `removed-files-size`

**Files:**
- Modify: `src/connector/iceberg/commit/overwrite.rs` — `overwrite_summary` (lines 608–640) and call-site (~lines 198–202), import line 57

- [ ] **Step 1: Add assertions to the existing overwrite test**

Find the overwrite test that commits and binds the committed snapshot summary `p`. Add (adjust expected numbers to the test's data: `A` added data files with `R_a` total added records and `S_a` added bytes, replacing `D` existing files with `R_d` records and `S_d` bytes, on a table whose prior snapshot had `total-records = R0`, `total-data-files = F0`, `total-files-size = SZ0`):

```rust
        assert_eq!(p.get("removed-files-size").is_some(), true);
        // total-records = R0 + R_a - R_d ; total-data-files = F0 + A - D
        assert_eq!(p.get("total-records").map(String::as_str), Some("<R0 + R_a - R_d>"));
        assert_eq!(p.get("total-data-files").map(String::as_str), Some("<F0 + A - D>"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::overwrite 2>&1 | tail -20`
Expected: FAIL — totals/removed-files-size missing.

- [ ] **Step 3: Add `removed-files-size` to `overwrite_summary` and wire the helper**

In `overwrite_summary` (lines 608–640), before the final `p` return, add:

```rust
    p.insert(
        "removed-files-size".to_string(),
        deleted
            .iter()
            .map(|(df, _, _)| df.file_size_in_bytes())
            .sum::<u64>()
            .to_string(),
    );
```

In the `use super::helpers::{ ... };` block (line 57) add `finalize_snapshot_summary`.

Replace the call-site (~lines 198–202):

```rust
    let additional_properties = merge_snapshot_summary_properties(
        overwrite_summary(&self.written, &existing),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

with:

```rust
    let additional_properties = merge_snapshot_summary_properties(
        finalize_snapshot_summary(
            overwrite_summary(&self.written, &existing),
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

(`m` is `table.metadata()`, bound at line 167.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::overwrite 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/overwrite.rs
git commit -m "feat(iceberg-commit): carry total-* through Overwrite summary"
```

---

## Task 4: Wire RowDelta (v2 delete); drop direct `total-equality-deletes`

**Files:**
- Modify: `src/connector/iceberg/commit/row_delta.rs` — `row_delta_summary` (lines 425–473) and call-site (~lines 251–254), imports

- [ ] **Step 1: Add assertions to the existing row_delta test**

Find the row_delta test that commits a delete and binds summary `p`. Add (for a delete adding `E` equality-delete records on a table whose prior snapshot had `total-equality-deletes = Q0`):

```rust
        // total-equality-deletes = Q0 + E (carried forward, not just = added)
        assert_eq!(p.get("total-equality-deletes").map(String::as_str), Some("<Q0 + E>"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::row_delta:: 2>&1 | tail -20`
Expected: FAIL.

- [ ] **Step 3: Drop the direct total + wire the helper**

In `row_delta_summary`, delete these three lines (the direct, non-carried total):

```rust
        p.insert(
            "total-equality-deletes".to_string(),
            equality_records.to_string(),
        );
```

In the import block at the top of `row_delta.rs`, ensure `finalize_snapshot_summary` is imported from `super::helpers` (add it; if `row_delta.rs` does not yet import from `super::helpers`, add `use super::helpers::finalize_snapshot_summary;`). Also ensure `merge_snapshot_summary_properties` handling matches the existing pattern — `row_delta.rs` builds the summary inline (lines 251–254), so wrap the map directly.

Replace (~lines 251–254):

```rust
    let snapshot_summary = Summary {
        operation: Operation::Delete,
        additional_properties: row_delta_summary(&self.written),
    };
```

with:

```rust
    let snapshot_summary = Summary {
        operation: Operation::Delete,
        additional_properties: finalize_snapshot_summary(
            row_delta_summary(&self.written),
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
    };
```

(`m` is `table.metadata()`, bound at line 161. RowDelta only adds delete files, so the previous snapshot's `total-equality-deletes`/`total-position-deletes` — established as `0` by the base append's `finalize_snapshot_summary` — carries forward and adds this delta.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::row_delta:: 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/row_delta.rs
git commit -m "feat(iceberg-commit): carry total-* through RowDelta summary"
```

---

## Task 5: Wire RowDeltaDv; thread replaced-DV `removed-files-size`

**Files:**
- Modify: `src/connector/iceberg/commit/row_delta_dv.rs` — `SnapshotIndex` (lines 461–475), `build_snapshot_index` (the `replaced_delete_files += 1;` at line 580 and the struct literal at lines 614–620), call-site (~lines 396–417), imports

- [ ] **Step 1: Add assertions to the existing DV test**

Find a row_delta_dv test that commits and binds summary `p` (e.g. around lines 998–1044). Add (table prior snapshot had `total-records = R0`; this DELETE removes `Del` records and adds no data files):

```rust
        assert_eq!(p.get("total-records").map(String::as_str), Some("<R0 - Del>"));
        assert!(p.get("total-files-size").is_some());
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::row_delta_dv 2>&1 | tail -20`
Expected: FAIL.

- [ ] **Step 3: Add `replaced_delete_files_size` to the index, accumulate it, thread it, and wire the helper**

(a) In `struct SnapshotIndex` (lines 461–475) add a field after `replaced_delete_records: u64,`:

```rust
    /// Total byte size of replaced (removed) DV files.
    replaced_delete_files_size: u64,
```

(b) In `build_snapshot_index`, add a local accumulator next to `let mut replaced_delete_files = 0usize;` (line 487):

```rust
    let mut replaced_delete_files_size = 0u64;
```

and immediately after `replaced_delete_files += 1;` (line 580) add:

```rust
                        replaced_delete_files_size += file.file_size_in_bytes();
```

(`file` is `entry.data_file().clone()`, in scope at line 553.)

(c) In the returned struct literal (lines 614–620) add `replaced_delete_files_size,` after `replaced_delete_records,`.

(d) In the `use super::helpers::{ ... };` block add `finalize_snapshot_summary`.

(e) Replace the call-site (~lines 396–409). Current:

```rust
    let summary_props = merge_snapshot_summary_properties(
        dv_summary(
            &written_dvs,
            &self.written,
            total_records,
            newly_deleted_records,
            index.replaced_delete_files,
            index.replaced_delete_records,
        ),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

with:

```rust
    let mut dv_props = dv_summary(
        &written_dvs,
        &self.written,
        total_records,
        newly_deleted_records,
        index.replaced_delete_files,
        index.replaced_delete_records,
    );
    if index.replaced_delete_files_size > 0 {
        dv_props.insert(
            "removed-files-size".to_string(),
            index.replaced_delete_files_size.to_string(),
        );
    }
    let summary_props = merge_snapshot_summary_properties(
        finalize_snapshot_summary(
            dv_props,
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
        &self.snapshot_properties,
    )
    .map_err(to_iceberg_unexpected)?;
```

(`m` is `table.metadata()`, bound at line 193. `dv_summary` keeps emitting `total-records`; the helper recomputes the identical value and adds the other five.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::row_delta_dv 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/row_delta_dv.rs
git commit -m "feat(iceberg-commit): carry total-* through RowDeltaDv summary"
```

---

## Task 6: Wire OverwritePartitions; normalize keys

**Files:**
- Modify: `src/connector/iceberg/commit/overwrite_partitions.rs` — `overwrite_partitions_summary` (lines 693–741) and call-site (~lines 454–461), imports

- [ ] **Step 1: Add assertions to the existing overwrite_partitions test**

The existing test (lines ~800–810) already asserts `removed-data-files`. Change that assertion to the canonical key and add totals:

```rust
        // renamed from non-canonical "removed-data-files"
        assert_eq!(p.get("deleted-data-files").map(String::as_str), Some("<D>"));
        assert!(p.get("removed-data-files").is_none(), "non-canonical key must be gone");
        assert_eq!(p.get("total-data-files").map(String::as_str), Some("<F0 + A - D>"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::overwrite_partitions 2>&1 | tail -20`
Expected: FAIL.

- [ ] **Step 3: Normalize keys + add removed delete counts + wire the helper**

In `overwrite_partitions_summary`:

(a) Rename the data-file removal key. Replace:

```rust
    p.insert(
        "removed-data-files".to_string(),
        deleted_data.len().to_string(),
    );
```

with:

```rust
    p.insert(
        "deleted-data-files".to_string(),
        deleted_data.len().to_string(),
    );
```

(b) Before the final `p` return, add the delete-file totals inputs (so `total-delete-files` / `total-position-deletes` / `total-equality-deletes` carry correctly):

```rust
    p.insert(
        "removed-delete-files".to_string(),
        deleted_deletes.len().to_string(),
    );
    p.insert(
        "removed-position-deletes".to_string(),
        deleted_deletes
            .iter()
            .filter(|(df, _, _)| df.content_type() == DataContentType::PositionDeletes)
            .map(|(df, _, _)| df.record_count())
            .sum::<u64>()
            .to_string(),
    );
    p.insert(
        "removed-equality-deletes".to_string(),
        deleted_deletes
            .iter()
            .filter(|(df, _, _)| df.content_type() == DataContentType::EqualityDeletes)
            .map(|(df, _, _)| df.record_count())
            .sum::<u64>()
            .to_string(),
    );
```

(`DataContentType` is already imported in this file — it's used by the existing `removed-position-delete-files` filter.)

(c) Add `finalize_snapshot_summary` to the `use super::helpers::{ ... };` block.

(d) Replace the call-site (~lines 454–461):

```rust
    let summary = Summary {
        operation: Operation::Overwrite,
        additional_properties: overwrite_partitions_summary(
            &self.written,
            &deleted_data,
            &deleted_deletes,
        ),
    };
```

with:

```rust
    let summary = Summary {
        operation: Operation::Overwrite,
        additional_properties: finalize_snapshot_summary(
            overwrite_partitions_summary(&self.written, &deleted_data, &deleted_deletes),
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
    };
```

(`m` is `table.metadata()`, bound at line 189.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::overwrite_partitions 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/overwrite_partitions.rs
git commit -m "feat(iceberg-commit): normalize keys + carry total-* through OverwritePartitions"
```

---

## Task 7: Wire RewriteManifests

**Files:**
- Modify: `src/connector/iceberg/commit/rewrite_manifests.rs` — inline summary (lines 182–193) and import

- [ ] **Step 1: Add assertions to the existing rewrite_manifests test**

Find the rewrite_manifests test that commits and binds summary `p`. Add (rewrite does not change data, so totals carry the prior snapshot's values `F0`/`R0` unchanged):

```rust
        assert_eq!(p.get("total-data-files").map(String::as_str), Some("<F0>"));
        assert_eq!(p.get("total-records").map(String::as_str), Some("<R0>"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::rewrite_manifests 2>&1 | tail -20` (or `cargo test -p novarocks rewrite_manifests` if the module is `pub mod`).
Expected: FAIL.

- [ ] **Step 3: Wire the helper around the inline summary**

Add `use super::helpers::finalize_snapshot_summary;` to the imports.

Replace (lines 182–193):

```rust
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: [
            (
                "replaced-manifests-count".to_string(),
                replaced_count.to_string(),
            ),
            ("added-manifests-count".to_string(), added_count.to_string()),
        ]
        .into_iter()
        .collect(),
    };
```

with:

```rust
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: finalize_snapshot_summary(
            [
                (
                    "replaced-manifests-count".to_string(),
                    replaced_count.to_string(),
                ),
                ("added-manifests-count".to_string(), added_count.to_string()),
            ]
            .into_iter()
            .collect(),
            metadata.current_snapshot().map(|s| s.summary()),
            false,
        ),
    };
```

(Here the metadata variable is named `metadata` — bound at line 72.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::rewrite_manifests 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/rewrite_manifests.rs
git commit -m "feat(iceberg-commit): carry total-* through RewriteManifests"
```

---

## Task 8: Wire RewriteDataFiles; fix the broken `total-records`

**Files:**
- Modify: `src/connector/iceberg/commit/rewrite_data_files.rs` — `rewrite_summary` (lines 528–587) and call-site (~lines 325–328), import

- [ ] **Step 1: Add assertions to the existing rewrite_data_files test**

Find the rewrite_data_files test that commits and binds summary `p`. Add (rewrite replaces `D` files / `R0` records with `A` files / `R0` records — same row count; total-records must equal the prior `R0`, NOT `added_records`):

```rust
        assert_eq!(p.get("total-records").map(String::as_str), Some("<R0>"));
        assert!(p.get("total-files-size").is_some());
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::rewrite_data_files 2>&1 | tail -20`
Expected: FAIL — current code sets `total-records = added_records`, which is wrong when the table had prior data.

- [ ] **Step 3: Remove the wrong total, add files-size inputs, wire the helper**

In `rewrite_summary`:

(a) Delete the wrong line:

```rust
    p.insert("total-records".to_string(), added_records.to_string());
```

(b) Add files-size accounting before the final `p` return:

```rust
    p.insert(
        "added-files-size".to_string(),
        added.iter().map(|f| f.file_size_in_bytes).sum::<u64>().to_string(),
    );
    p.insert(
        "removed-files-size".to_string(),
        live.data_files
            .iter()
            .chain(live.delete_files.iter())
            .map(|e| e.data_file.file_size_in_bytes())
            .sum::<u64>()
            .to_string(),
    );
```

(c) Add `finalize_snapshot_summary` to the `use super::helpers::{ ... };` block.

(d) Replace the call-site (~lines 325–328):

```rust
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: rewrite_summary(&self.written, &live),
    };
```

with:

```rust
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: finalize_snapshot_summary(
            rewrite_summary(&self.written, &live),
            m.current_snapshot().map(|s| s.summary()),
            false,
        ),
    };
```

(`m` is `table.metadata()`, bound at line 173.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::rewrite_data_files 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/rewrite_data_files.rs
git commit -m "fix(iceberg-commit): correct total-records carry-forward in RewriteDataFiles"
```

---

## Task 9: Wire Truncate through the truncate path

**Files:**
- Modify: `src/connector/iceberg/commit/truncate.rs` — `truncate_summary` (lines 291–364) and call-site (~lines 243–246), import

- [ ] **Step 1: Add assertions to the existing truncate test**

Find the truncate test that commits and binds summary `p`. Add:

```rust
        assert_eq!(p.get("total-data-files").map(String::as_str), Some("0"));
        assert_eq!(p.get("total-records").map(String::as_str), Some("0"));
        assert_eq!(p.get("total-files-size").map(String::as_str), Some("0"));
        assert_eq!(p.get("total-delete-files").map(String::as_str), Some("0"));
        assert_eq!(p.get("engine-name").map(String::as_str), Some("novarocks"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p novarocks --lib commit::truncate 2>&1 | tail -20`
Expected: FAIL — only `total-records=0` is currently set; the other four totals are absent.

- [ ] **Step 3: Drop the hard-coded total + wire the helper (truncate path)**

In `truncate_summary`, delete the hard-coded line near the end:

```rust
    // After TRUNCATE every row is gone, so total-records is 0.
    p.insert("total-records".to_string(), "0".to_string());
```

Add `finalize_snapshot_summary` to the `use super::helpers::{ ... };` block.

Replace the call-site (~lines 243–246):

```rust
    let summary = Summary {
        operation: Operation::Delete,
        additional_properties: truncate_summary(&data_entries, &delete_entries),
    };
```

with:

```rust
    let summary = Summary {
        operation: Operation::Delete,
        additional_properties: finalize_snapshot_summary(
            truncate_summary(&data_entries, &delete_entries),
            m.current_snapshot().map(|s| s.summary()),
            true,
        ),
    };
```

(`m` is `table.metadata()`, bound at line 126. `truncate_full_table = true` makes the helper reset all six totals to `0`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p novarocks --lib commit::truncate 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/commit/truncate.rs
git commit -m "feat(iceberg-commit): reset all total-* on Truncate via helper"
```

---

## Task 10: Full commit-module test sweep + fmt/clippy

**Files:** none (verification only)

- [ ] **Step 1: Run the whole commit module test suite**

Run: `cargo test -p novarocks --lib commit:: 2>&1 | tail -30`
Expected: PASS (all commit-action tests, including the five `summary_tests`).

- [ ] **Step 2: fmt + clippy**

Run: `cargo fmt && cargo clippy -p novarocks --lib 2>&1 | tail -30`
Expected: no fmt diff; no new clippy warnings in the edited files.

- [ ] **Step 3: Commit any fmt fixes**

```bash
git add -A && git commit -m "chore(iceberg-commit): fmt after total-* carry-forward" || echo "nothing to commit"
```

---

## Task 11: SQL self-consistency golden

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_snapshot_summary_totals.sql`
- Create: `sql-tests/iceberg/result/iceberg_snapshot_summary_totals.result`

This golden verifies, end-to-end through the standalone server + REST catalog, that the `$snapshots` summary carries `total-*` and `engine-name` across append → delete → overwrite. The `summary` column is surfaced as text, so we assert with `LIKE`/`INSTR` substring checks (precise numeric carry-forward is covered by the Task 1–9 unit tests).

- [ ] **Step 1: Write the SQL fixture**

Create `sql-tests/iceberg/sql/iceberg_snapshot_summary_totals.sql`:

```sql
-- @order_sensitive=true
-- IV3-2: validate that snapshot summaries carry total-* and engine identity.
-- Numeric carry-forward correctness is unit-tested in commit/*.rs; here we
-- assert the keys are present in the surfaced summary across operations.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES (3, 30);

-- query 5
-- Latest append summary carries all expected total-* keys + engine-name.
SELECT
  summary LIKE '%total-data-files%'        AS has_total_data_files,
  summary LIKE '%total-records%'           AS has_total_records,
  summary LIKE '%total-files-size%'        AS has_total_files_size,
  summary LIKE '%engine-name%'             AS has_engine_name
FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
ORDER BY committed_at DESC
LIMIT 1;

-- query 6
-- @skip_result_check=true
DELETE FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} WHERE id = 1;

-- query 7
-- Delete snapshot also carries totals + engine-name.
SELECT
  summary LIKE '%total-records%'   AS has_total_records,
  summary LIKE '%engine-name%'     AS has_engine_name
FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0}$snapshots
ORDER BY committed_at DESC
LIMIT 1;

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};
```

- [ ] **Step 2: Start the standalone server + run the suite in record mode**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-iv32.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then echo "server died"; tail -20 "$LOG"; exit 1; fi
  sleep 1
done
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_snapshot_summary_totals --mode record
kill -9 "$SRV_PID" 2>/dev/null || true
```

Expected: the runner writes `sql-tests/iceberg/result/iceberg_snapshot_summary_totals.result`.

- [ ] **Step 3: Verify the recorded result shows all `true`**

Run: `cat sql-tests/iceberg/result/iceberg_snapshot_summary_totals.result`
Expected: query 5 row is `1	1	1	1` (all `true`); query 7 row is `1	1`.

- [ ] **Step 4: Re-run in verify mode**

```bash
# (restart server as in Step 2, then:)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --only iceberg_snapshot_summary_totals --mode verify
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg/sql/iceberg_snapshot_summary_totals.sql sql-tests/iceberg/result/iceberg_snapshot_summary_totals.result
git commit -m "test(iceberg): snapshot summary total-* + engine-name self-consistency golden"
```

---

## Self-Review (completed during planning)

**Spec coverage (vs design §4 / §6 / §11):**
- §A1 shared carry-forward helper → Task 1 ✓
- §A2 all 8 actions接入 + key normalization (overwrite_partitions rename, rewrite_data_files fix, truncate, row_delta direct-total removal) → Tasks 2–9 ✓
- §A3 engine identity → Task 1 (`finalize_snapshot_summary` always stamps) ✓
- §A7 error handling (legacy omit, saturating) → Task 1 tests `legacy_missing_total_is_omitted_not_fabricated`, `removed_below_zero_saturates` ✓
- IV3-2 verification (acceptance #1 per-action totals; #3 engine identity) → unit tests in Tasks 2–9 + golden Task 11 ✓
- **Deferred (separate follow-up plan):** §A4 `last-partition-id`/`last-column-id` monotonic assertion; §A5 sort-order consistency; §A6 `encryption_key_id` (no-op, nothing to implement). The cross-engine Spark compatibility check for `total-*` (acceptance #2) is folded into IV3-8 Plan 2's compat suite, since it shares the `$snapshots` read path there.

**Placeholder scan:** The only `<…>` placeholders are expected numeric values in test assertions (Tasks 2–9), which depend on each existing test's specific fixture data — the executing agent fills them from the test it is extending. The helper, all call-site edits, and the SQL golden are fully concrete.

**Type/name consistency:** `finalize_snapshot_summary(props: HashMap<String,String>, previous: Option<&Summary>, truncate_full_table: bool) -> HashMap<String,String>` is used identically at all nine call-sites; the metadata variable is `m` everywhere except `rewrite_manifests.rs` (where it is `metadata`) — noted per task.
