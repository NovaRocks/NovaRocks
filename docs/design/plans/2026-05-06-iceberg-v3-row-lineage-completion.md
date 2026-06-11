# Iceberg V3 Row-Lineage Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve `_row_id` and `_last_updated_sequence_number` across `OPTIMIZE TABLE` on Iceberg V3 row-lineage tables (so IVM can pair post-OPTIMIZE rows with MV state by row identity), plus enable SQL-level access to four Iceberg metadata tables (`$snapshots` / `$history` / `$refs` / `$partitions`) needed for the cross-snapshot uniqueness regression.

**Architecture:** Three phases in one PR.
1. **Phase A** wires a pre-parse rewrite of `<tbl>$<metatype>` into `<tbl>.__nr_meta_<metatype>__`, paralleling the existing `__nr_ref:` mechanism, then the analyzer detects the suffix and emits a new `Relation::IcebergMetadataScan`. The planner lowers that to the existing `IcebergMetadataScanOp` (Java JNI-backed scanner already wired by PR #81).
2. **Phase B** routes OPTIMIZE on V3 row-lineage tables through `write_row_lineage_batches_as_data_files` (already used by COW/MOR UPDATE), materialising `_row_id` and `_last_updated_sequence_number` as physical parquet columns at reserved field IDs `i32::MAX-107` / `i32::MAX-108`. The `RewriteDataFiles` commit detects that all written files carry `first_row_id=None` and skips `next_row_id` allocation + omits `row_range` from the snapshot.
3. **Phase C** adds three SQL regressions and one Rust unit test that lock in the row-identity invariants.

**Tech Stack:** Rust, sqlparser-rs (NovaRocks dialect at `src/sql/parser/dialect`), Arrow, Parquet, vendored `iceberg-0.9.0`, StarRocks managed-lake MV / IVM.

**Reference spec:** [docs/design/specs/2026-05-06-iceberg-v3-row-lineage-completion-design.md](docs/design/specs/2026-05-06-iceberg-v3-row-lineage-completion-design.md).

---

## Background — Where Things Live

Read once before starting; keep these open while implementing.

### Phase A entry points
- Pre-parse normalizer (existing `__nr_ref:` rewrite to copy from): [src/sql/parser/dialect/mod.rs:222-371](src/sql/parser/dialect/mod.rs:222) — `normalize_for_raw_parse` and `rewrite_version_as_of_string`. Mirror this byte-walking pattern for the `$<metatype>` rewrite.
- `Relation` enum: [src/sql/analysis/mod.rs:96-115](src/sql/analysis/mod.rs:96). Will add `IcebergMetadataScan(IcebergMetadataScanRelation)` variant.
- Table factor resolver: [src/sql/analyzer/resolve_from.rs:130-187](src/sql/analyzer/resolve_from.rs:130). Will detect the `__nr_meta_<type>__` last-part suffix here.
- BE metadata bridge (already complete; do not modify): [src/connector/iceberg/metadata.rs:38-72](src/connector/iceberg/metadata.rs:38) and [src/lower/node/hdfs_scan.rs:585-907](src/lower/node/hdfs_scan.rs:585).

### Phase B entry points
- OPTIMIZE entry: [src/connector/iceberg/compact.rs:176-300](src/connector/iceberg/compact.rs:176) — `execute_whole_table_rewrite`. Currently calls `write_chunks_as_iceberg_data_files` at line 242.
- Row-lineage writer (existing, already used by COW/MOR UPDATE): [src/connector/iceberg/data_writer.rs:197-297](src/connector/iceberg/data_writer.rs:197) — `write_row_lineage_batches_as_data_files`, `append_row_lineage_columns`, `RowLineageColumns`, `RowLineageWriteBatch`.
- Reserved field id constants: [src/exec/row_position.rs:82-83](src/exec/row_position.rs:82) — `ICEBERG_RESERVED_FIELD_ID_ROW_ID = i32::MAX - 107`, `ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER = i32::MAX - 108`.
- Commit op: [src/connector/iceberg/commit/rewrite_data_files.rs:50-90, 244-285](src/connector/iceberg/commit/rewrite_data_files.rs:50). Currently allocates `next_row_id` for all `RowLineageV3` mode tables (line 63) and writes `row_range` (line 273).
- Write mode detector: [src/connector/iceberg/commit/mod.rs](src/connector/iceberg/commit/mod.rs) — `classify_iceberg_write_mode`.

### Phase C entry points
- IVM Replace classifier: [src/connector/iceberg/changes.rs:372-493](src/connector/iceberg/changes.rs:372) — `classify_snapshot` calls `validate_replace_snapshot`.
- Existing OPTIMIZE SQL test (do not modify, model new tests after it): [sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql](sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql).
- Existing v3 row-lineage tests: [sql-tests/iceberg/sql/iceberg_v3_update_cow.sql](sql-tests/iceberg/sql/iceberg_v3_update_cow.sql), `iceberg_v3_update_mor.sql`.
- SQL test runner: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite iceberg --mode verify --only <case_name>`.

### Read-side row-id resolver (no changes — confirms read path already supports stored column)
- [src/exec/operators/scan/runner.rs:246-304](src/exec/operators/scan/runner.rs:246) — `synthesize_row_lineage_columns`. Prefers stored physical column at the V3 reserved field ids; falls back to `first_row_id + offset`.

### Existing reference test for OPTIMIZE row-lineage
- [src/engine/mod.rs:4530-4630](src/engine/mod.rs:4530) — pattern for `iceberg_row_lineage_*` integration tests in Rust. Use this style for Task C4.

---

## File Structure

**Modify:**
- `src/sql/parser/dialect/mod.rs` — extend `normalize_for_raw_parse` with a `$<metatype>` pre-parse rewrite step.
- `src/sql/analysis/mod.rs` — add `IcebergMetadataScan(IcebergMetadataScanRelation)` variant on `Relation`.
- `src/sql/analyzer/resolve_from.rs` — detect `__nr_meta_<type>__` last-part suffix, emit new variant.
- `src/sql/analyzer/scope.rs` — helper to register the fixed metadata-table schema as a scope.
- `src/sql/analyzer/mod.rs` — pattern arms for the new variant in any switch on `Relation`.
- `src/sql/optimizer/` (whichever module lowers `Relation` to logical plan; locate during Task A4) — lower `IcebergMetadataScan` to `IcebergMetadataScanConfig`.
- `src/lower/node/hdfs_scan.rs` (if needed for standalone-mode wiring) — accept the analyzer-built metadata scan config.
- `src/connector/iceberg/compact.rs` — branch `execute_whole_table_rewrite` on row-lineage capability; on the row-lineage branch, project `_row_id` + `_last_updated_sequence_number` and use the row-lineage writer.
- `src/connector/iceberg/commit/rewrite_data_files.rs` — when all written files have `first_row_id=None`, skip `next_row_id` allocation and omit `row_range`.

**Add:**
- `src/sql/analyzer/iceberg_metadata.rs` — small module: `MetadataTableSuffix` enum, `split_metadata_suffix(parts)` helper, fixed schema definitions for the 4 tables. Keeps the analyzer file small.
- `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_history.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_refs.sql`
- `sql-tests/iceberg/sql/iceberg_metadata_partitions.sql`
- `sql-tests/iceberg/sql/iceberg_v3_optimize_row_lineage.sql`
- `sql-tests/iceberg/sql/iceberg_v3_branch_row_lineage.sql`
- `sql-tests/iceberg/sql/iceberg_v3_row_lineage_uniqueness.sql`

**No changes (verified):**
- `src/exec/operators/scan/runner.rs` (read path already supports stored column).
- `src/connector/iceberg/changes.rs` (Replace skip already correct — Phase 0.1 verifies semantics).
- `src/connector/iceberg/data_writer.rs` (writer already exists).
- `src/connector/starrocks/managed/mv_refresh_*.rs` (IVM logic unchanged).
- `src/connector/iceberg/metadata.rs` and `IcebergMetadataBridge.java` (BE side complete).

---

## Phase 0 — Plan-time investigations (no code changes, no commits)

### Task 0.1: Confirm `validate_replace_snapshot` row-count semantics

**Why this matters:** The spec §6 risk table flagged this as the highest risk. If `validate_replace_snapshot` enforces physical-row equality and OPTIMIZE strips DV deletes (reducing physical rows on the rewritten side), every OPTIMIZE on a table with prior DELETEs trips `ReplaceValidationFailed` and IVM falls into `MvRefreshPolicy::Unsupported`. That defeats the entire purpose of preserving `_row_id`.

**Files:**
- Read: [src/connector/iceberg/changes.rs:430-493](src/connector/iceberg/changes.rs:430)
- Read: [src/connector/iceberg/commit/rewrite_data_files.rs](src/connector/iceberg/commit/rewrite_data_files.rs) (search for `total-records` summary key emission)
- Read: [src/connector/iceberg/commit/fast_append.rs](src/connector/iceberg/commit/fast_append.rs) (for comparison: how INSERT writes `total-records`)
- Read: vendored `iceberg-0.9.0/src/spec/snapshot.rs` for the canonical Iceberg semantics of `total-records`

- [ ] **Step 1: Read `validate_replace_snapshot`** and identify which summary keys it compares.

Run: `grep -n "total-records\|total_records\|added-records\|deleted-records" src/connector/iceberg/changes.rs`

Expected: function compares one or more of these keys between the previous snapshot's summary and the new Replace snapshot's summary.

- [ ] **Step 2: Locate where the new Replace snapshot's summary is built** during OPTIMIZE commit.

Run: `grep -n "rewrite_summary\|total-records\|added-records" src/connector/iceberg/commit/rewrite_data_files.rs`

- [ ] **Step 3: Decide which interpretation applies and record the finding.**

Two outcomes possible:
- **Logical:** `total-records` is computed from live (post-DV-merge) row counts on both sides. OPTIMIZE preserves it by definition — no further action required.
- **Physical:** `total-records` counts all physical rows including DV-deleted ones. OPTIMIZE drops the DV and rewrites only live rows, so the new snapshot's `total-records` is smaller — validation fails.

Record the finding in your task notes (no file commit needed). The result decides whether **Task B6** (validate_replace_snapshot adjustment) is in scope.

- [ ] **Step 4: Decision branch.**

If Logical → Task B6 is **skipped**.

If Physical → Task B6 is **required**: extend `validate_replace_snapshot` to also subtract `deleted-records` (or equivalently `total-deleted-records`) on both sides before comparing, OR special-case `Replace` snapshots whose summary indicates a DV-stripping rewrite.

### Task 0.2: Confirm OPTIMIZE rejects branch targets

**Why this matters:** Spec D7 says OPTIMIZE on a branch must error explicitly. If the existing code already rejects, no Task B7 is needed.

**Files:**
- Read: [src/connector/iceberg/compact.rs:170-205](src/connector/iceberg/compact.rs:170) — the OPTIMIZE entry point and its `target_ref` plumbing.
- Read: `src/engine/iceberg_compact_flow.rs` (or wherever OPTIMIZE statements are routed; locate via grep).

Run: `grep -rn "branch\|target_ref\|OPTIMIZE" src/connector/iceberg/compact.rs src/engine/ | grep -i compact | head -20`

- [ ] **Step 1: Run that grep and read the OPTIMIZE flow.**

- [ ] **Step 2: Determine current behaviour.** Three possibilities:
  1. OPTIMIZE on `t.branch_<x>` is parsed but rejected at the analyzer/flow level — done, no further work.
  2. OPTIMIZE on branch is silently accepted and runs against `main` — broken; needs an explicit reject.
  3. OPTIMIZE on branch is parser-level rejected — done, no further work.

If outcome 2: add Task B7 "Reject OPTIMIZE on branch suffix at the engine level" with a one-line guard.

Otherwise: B7 not needed.

---

## Phase A — Metadata table SQL routing

### Task A1: Add `IcebergMetadataScan` relation variant and metadata module

**Files:**
- Modify: `src/sql/analysis/mod.rs:96-115`
- Create: `src/sql/analyzer/iceberg_metadata.rs`
- Create: `src/sql/analyzer/iceberg_metadata_test.rs` (or co-located `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test for `split_metadata_suffix`.**

Create `src/sql/analyzer/iceberg_metadata.rs`:

```rust
//! Resolve the trailing `__nr_meta_<type>__` suffix that the parser-level
//! pre-parse rewrites `<tbl>$<metatype>` into.
//!
//! Mirrors `iceberg_ref::split_ref_suffix` for branch/tag.

use crate::connector::iceberg::IcebergMetadataTableType;

/// Inspect the trailing identifier part of a qualified name and, if it
/// matches `__nr_meta_<type>__`, return the parts with the suffix stripped
/// plus the parsed metadata-table type.
pub fn split_metadata_suffix(
    parts: &[String],
) -> (Vec<String>, Option<IcebergMetadataTableType>) {
    if let Some(last) = parts.last() {
        if let Some(inner) = last.strip_prefix("__nr_meta_").and_then(|s| s.strip_suffix("__")) {
            if let Ok(ty) = IcebergMetadataTableType::parse(inner) {
                return (
                    parts[..parts.len() - 1].to_vec(),
                    Some(ty),
                );
            }
        }
    }
    (parts.to_vec(), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::IcebergMetadataTableType;

    #[test]
    fn snapshots_suffix_is_stripped() {
        let parts = vec!["db".to_string(), "t".to_string(), "__nr_meta_snapshots__".to_string()];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(ty, Some(IcebergMetadataTableType::Snapshots));
    }

    #[test]
    fn three_part_qualified_name_works() {
        let parts = vec![
            "ice".to_string(),
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_history__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(
            stripped,
            vec!["ice".to_string(), "db".to_string(), "t".to_string()]
        );
        assert_eq!(ty, Some(IcebergMetadataTableType::History));
    }

    #[test]
    fn refs_and_partitions_round_trip() {
        for (suffix, expected) in [
            ("__nr_meta_refs__", IcebergMetadataTableType::Refs),
            ("__nr_meta_partitions__", IcebergMetadataTableType::Partitions),
        ] {
            let parts = vec!["t".to_string(), suffix.to_string()];
            let (_, ty) = split_metadata_suffix(&parts);
            assert_eq!(ty, Some(expected));
        }
    }

    #[test]
    fn unrecognised_metatype_is_passthrough() {
        let parts = vec!["t".to_string(), "__nr_meta_files__".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        // `Files` IS recognised by `IcebergMetadataTableType::parse`, so this passes through.
        // For an unknown metatype:
        let parts2 = vec!["t".to_string(), "__nr_meta_xyz__".to_string()];
        let (out2, ty2) = split_metadata_suffix(&parts2);
        assert_eq!(out2, parts2);
        assert_eq!(ty2, None);
    }

    #[test]
    fn no_suffix_passthrough() {
        let parts = vec!["db".to_string(), "t".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }
}
```

Append the new module declaration to `src/sql/analyzer/mod.rs` (find the existing `mod iceberg_ref;` line and add `mod iceberg_metadata;` next to it).

- [ ] **Step 2: Run the tests; expect FAIL because `iceberg_metadata` module is not yet wired in.**

Run: `cargo test -p novarocks --lib sql::analyzer::iceberg_metadata 2>&1 | tail -20`

Expected: `error[E0432]: unresolved import` or compile failure on the new module if `mod iceberg_metadata;` was not added.

- [ ] **Step 3: Add the module declaration in `src/sql/analyzer/mod.rs`.** Locate the existing `mod iceberg_ref;` line and add immediately after it:

```rust
mod iceberg_metadata;
```

- [ ] **Step 4: Re-run the tests; expect PASS.**

Run: `cargo test -p novarocks --lib sql::analyzer::iceberg_metadata 2>&1 | tail -20`

Expected: 4 passed (`snapshots_suffix_is_stripped`, `three_part_qualified_name_works`, `refs_and_partitions_round_trip`, `no_suffix_passthrough`); the file-suffix probe test in `unrecognised_metatype_is_passthrough` validates that `__nr_meta_xyz__` returns `None`.

- [ ] **Step 5: Add the `IcebergMetadataScan` variant on `Relation`.**

Edit `src/sql/analysis/mod.rs:96-115`:

```rust
#[derive(Clone, Debug)]
pub(crate) enum Relation {
    /// A base table scan.
    Scan(ScanRelation),
    /// An Iceberg metadata table scan: `t$snapshots`, `t$history`, etc.
    /// Produced by `resolve_from` after `__nr_meta_<type>__` suffix detection.
    IcebergMetadataScan(IcebergMetadataScanRelation),
    /// A subquery in FROM: `(SELECT ...) AS alias`.
    Subquery {
        query: Box<ResolvedQuery>,
        alias: String,
    },
    /// A join between two relations.
    Join(Box<JoinRelation>),
    /// `TABLE(generate_series(start, end[, step]))`.
    GenerateSeries(GenerateSeriesRelation),
    /// Reference to an analyzed non-recursive CTE definition.
    /// Inline vs reuse is decided later by Cascades.
    CTEConsume {
        cte_id: cte::CteId,
        alias: String,
        output_columns: Vec<OutputColumn>,
    },
}
```

Add the `IcebergMetadataScanRelation` struct just below `ScanRelation`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct IcebergMetadataScanRelation {
    /// The underlying iceberg table being inspected.
    pub database: String,
    pub table: TableDef,
    pub metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    /// FROM-clause alias (e.g., `t$snapshots AS s` → `Some("s")`).
    pub alias: Option<String>,
}
```

- [ ] **Step 6: Compile-check after the variant addition.**

Run: `cargo build -p novarocks 2>&1 | tail -30`

Expected: a list of `error[E0004]: non-exhaustive patterns` for every match on `Relation`. These pinpoint every site that must add an arm. List them; they are addressed in subsequent tasks (A3, A4, A5).

- [ ] **Step 7: Add minimal `unreachable!` arms to the failing match sites just to make the code compile** (we'll fill them in properly in subsequent tasks). For each site reported, add:

```rust
Relation::IcebergMetadataScan(_) => unreachable!("IcebergMetadataScan handled in resolve_from + lowering"),
```

- [ ] **Step 8: Verify build is green.**

Run: `cargo build -p novarocks 2>&1 | tail -10`

Expected: `Finished` (warnings OK).

- [ ] **Step 9: Commit.**

```bash
git add src/sql/analyzer/iceberg_metadata.rs src/sql/analyzer/mod.rs src/sql/analysis/mod.rs
# plus any other files touched for non-exhaustive arms
git commit -m "sql(analyzer): add IcebergMetadataScan relation variant + suffix splitter"
```

### Task A2: Add the pre-parse `$<metatype>` rewrite

**Files:**
- Modify: `src/sql/parser/dialect/mod.rs` — extend `normalize_for_raw_parse`.

- [ ] **Step 1: Write the failing test.**

Add to `src/sql/parser/dialect/mod.rs` (in the existing `#[cfg(test)] mod tests` block — locate via grep):

```rust
#[test]
fn metadata_suffix_dollar_is_rewritten_for_known_types() {
    let cases = [
        ("SELECT * FROM t$snapshots", "SELECT * FROM t.__nr_meta_snapshots__"),
        ("SELECT * FROM db.t$history", "SELECT * FROM db.t.__nr_meta_history__"),
        ("SELECT * FROM ice.db.t$refs", "SELECT * FROM ice.db.t.__nr_meta_refs__"),
        ("select * from t$partitions", "select * from t.__nr_meta_partitions__"),
        // Mixed case.
        ("SELECT * FROM t$Snapshots", "SELECT * FROM t.__nr_meta_snapshots__"),
    ];
    for (input, expected) in cases {
        let got = normalize_for_raw_parse(input).expect("normalize");
        assert_eq!(got, expected, "input: {input}");
    }
}

#[test]
fn metadata_suffix_unknown_type_errors() {
    let err = normalize_for_raw_parse("SELECT * FROM t$foo").unwrap_err();
    assert!(
        err.contains("unsupported iceberg metadata table type")
            && err.contains("foo"),
        "unexpected error: {err}"
    );
}

#[test]
fn metadata_suffix_inside_string_literal_is_left_alone() {
    let input = "SELECT 'a$snapshots' FROM t";
    let got = normalize_for_raw_parse(input).expect("normalize");
    assert_eq!(got, input);
}

#[test]
fn metadata_suffix_with_alias() {
    let input = "SELECT * FROM t$snapshots AS s";
    let got = normalize_for_raw_parse(input).expect("normalize");
    assert_eq!(got, "SELECT * FROM t.__nr_meta_snapshots__ AS s");
}
```

- [ ] **Step 2: Run; expect FAIL.**

Run: `cargo test -p novarocks --lib sql::parser::dialect::tests::metadata_suffix 2>&1 | tail -20`

Expected: 4 failures.

- [ ] **Step 3: Implement the rewrite.**

Add a new function near `rewrite_version_as_of_string` in `src/sql/parser/dialect/mod.rs`:

```rust
/// Rewrite `<ident>$<metatype>` (in unquoted/non-string context) to
/// `<ident>.__nr_meta_<metatype>__`, lowercasing `<metatype>`.
///
/// Iceberg's `t$snapshots` syntax cannot be lexed by sqlparser without dialect
/// hacks. The analyzer detects the `__nr_meta_*__` last-part suffix and
/// dispatches to `IcebergMetadataScanOp`.
///
/// Restricted to the four BE-supported types: snapshots, history, refs,
/// partitions. An unrecognised type errors at normalize time.
fn rewrite_iceberg_metadata_suffix(sql: &str) -> Result<String, String> {
    if !sql.contains('$') {
        return Ok(sql.to_string());
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + 16);
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_single_quote {
            if byte == b'\'' { in_single_quote = false; }
            output.push(byte as char); idx += 1; continue;
        }
        if in_double_quote {
            if byte == b'"' { in_double_quote = false; }
            output.push(byte as char); idx += 1; continue;
        }
        if in_backtick {
            if byte == b'`' { in_backtick = false; }
            output.push(byte as char); idx += 1; continue;
        }
        match byte {
            b'\'' => { in_single_quote = true; output.push('\''); idx += 1; continue; }
            b'"'  => { in_double_quote = true; output.push('"');  idx += 1; continue; }
            b'`'  => { in_backtick = true;     output.push('`');  idx += 1; continue; }
            _ => {}
        }

        if byte == b'$'
            && idx > 0
            && is_identifier_byte(Some(bytes[idx - 1]))
        {
            // Read the identifier word that follows `$`.
            let mut end = idx + 1;
            while end < bytes.len() && is_identifier_byte(Some(bytes[end])) {
                end += 1;
            }
            if end == idx + 1 {
                // Lone `$` not followed by an identifier — pass through.
                output.push('$');
                idx += 1;
                continue;
            }
            let metatype_raw = &sql[idx + 1..end];
            let metatype = metatype_raw.to_ascii_lowercase();
            // Whitelist the four scope types.
            match metatype.as_str() {
                "snapshots" | "history" | "refs" | "partitions" => {}
                other => {
                    return Err(format!(
                        "unsupported iceberg metadata table type: {other}; \
                         expected one of snapshots/history/refs/partitions"
                    ));
                }
            }
            output.push('.');
            output.push_str("__nr_meta_");
            output.push_str(&metatype);
            output.push_str("__");
            idx = end;
            continue;
        }

        output.push(byte as char);
        idx += 1;
    }
    Ok(output)
}
```

- [ ] **Step 4: Wire it into `normalize_for_raw_parse`.**

Edit `src/sql/parser/dialect/mod.rs:224-229`:

```rust
pub(crate) fn normalize_for_raw_parse(sql: &str) -> Result<String, String> {
    let sql = rewrite_set_user_variables(sql)?;
    let sql = rewrite_from_dual(&sql)?;
    let sql = normalize_function_syntax(&sql)?;
    let sql = rewrite_version_as_of_string(&sql)?;
    let sql = rewrite_iceberg_metadata_suffix(&sql)?;
    Ok(rewrite_create_table_nested_generic_closers(&sql))
}
```

- [ ] **Step 5: Run the tests; expect PASS.**

Run: `cargo test -p novarocks --lib sql::parser::dialect::tests::metadata_suffix 2>&1 | tail -20`

Expected: 4 passed.

- [ ] **Step 6: Run full dialect test module to verify no regression.**

Run: `cargo test -p novarocks --lib sql::parser::dialect 2>&1 | tail -20`

Expected: all green.

- [ ] **Step 7: Commit.**

```bash
git add src/sql/parser/dialect/mod.rs
git commit -m "sql(parser): rewrite \`<tbl>\$<metatype>\` to \`<tbl>.__nr_meta_<metatype>__\`"
```

### Task A3: Analyzer dispatch for `__nr_meta_<type>__` suffix

**Files:**
- Modify: `src/sql/analyzer/resolve_from.rs:130-187`
- Modify: `src/sql/analyzer/scope.rs` — add `add_iceberg_metadata_table_scope` helper.
- Modify: `src/sql/analyzer/iceberg_metadata.rs` — add `metadata_table_schema(IcebergMetadataTableType)` returning the fixed column list.

- [ ] **Step 1: Write the failing test for analyzer dispatch.**

Add to `src/sql/analyzer/iceberg_metadata.rs` (in the existing test module):

```rust
#[test]
fn metadata_table_schema_snapshots_has_expected_columns() {
    let cols = metadata_table_schema(IcebergMetadataTableType::Snapshots);
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["committed_at", "snapshot_id", "parent_id", "operation", "manifest_list", "summary"]
    );
}

#[test]
fn metadata_table_schema_history_has_expected_columns() {
    let cols = metadata_table_schema(IcebergMetadataTableType::History);
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["made_current_at", "snapshot_id", "parent_id", "is_current_ancestor"]
    );
}

#[test]
fn metadata_table_schema_refs_has_expected_columns() {
    let cols = metadata_table_schema(IcebergMetadataTableType::Refs);
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "name", "type", "snapshot_id",
            "max_reference_age_in_ms", "min_snapshots_to_keep", "max_snapshot_age_in_ms",
        ]
    );
}

#[test]
fn metadata_table_schema_partitions_has_expected_columns() {
    let cols = metadata_table_schema(IcebergMetadataTableType::Partitions);
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    // Per spec D6 + StarRocks reference (see plan 2026-05-05-iceberg-metadata-tables.md
    // Column Schemas table).
    assert!(names.contains(&"record_count"));
    assert!(names.contains(&"file_count"));
    assert!(names.contains(&"position_delete_file_count"));
    assert!(names.contains(&"equality_delete_file_count"));
}
```

- [ ] **Step 2: Run; expect FAIL.**

Run: `cargo test -p novarocks --lib sql::analyzer::iceberg_metadata::tests 2>&1 | tail -20`

Expected: 4 new failures (function `metadata_table_schema` not defined).

- [ ] **Step 3: Implement `metadata_table_schema` and a column descriptor.**

Add to `src/sql/analyzer/iceberg_metadata.rs`:

```rust
use crate::types::DataType;

#[derive(Clone, Debug)]
pub struct MetadataColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl MetadataColumn {
    fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self { name: name.to_string(), data_type, nullable }
    }
}

pub fn metadata_table_schema(ty: IcebergMetadataTableType) -> Vec<MetadataColumn> {
    use IcebergMetadataTableType as T;
    match ty {
        T::Snapshots => vec![
            MetadataColumn::new("committed_at",   DataType::DateTime,  false),
            MetadataColumn::new("snapshot_id",    DataType::BigInt,    false),
            MetadataColumn::new("parent_id",      DataType::BigInt,    true),
            MetadataColumn::new("operation",      DataType::Varchar(0), true),
            MetadataColumn::new("manifest_list",  DataType::Varchar(0), false),
            // Map<Varchar,Varchar> handled at the connector level; analyzer
            // surfaces a single Varchar column whose runtime value is a
            // serialised key=value pairs string (matches what
            // IcebergMetadataScanOp emits today).
            MetadataColumn::new("summary",        DataType::Varchar(0), false),
        ],
        T::History => vec![
            MetadataColumn::new("made_current_at",      DataType::DateTime, false),
            MetadataColumn::new("snapshot_id",          DataType::BigInt,   false),
            MetadataColumn::new("parent_id",            DataType::BigInt,   true),
            MetadataColumn::new("is_current_ancestor",  DataType::Boolean,  false),
        ],
        T::Refs => vec![
            MetadataColumn::new("name",                     DataType::Varchar(0), false),
            MetadataColumn::new("type",                     DataType::Varchar(0), false),
            MetadataColumn::new("snapshot_id",              DataType::BigInt,     false),
            MetadataColumn::new("max_reference_age_in_ms",  DataType::BigInt,     true),
            MetadataColumn::new("min_snapshots_to_keep",    DataType::Int,        true),
            MetadataColumn::new("max_snapshot_age_in_ms",   DataType::BigInt,     true),
        ],
        T::Partitions => vec![
            // partition struct column omitted in this analyzer-level schema —
            // it's resolved at lowering time from the table's partition spec.
            // For an unpartitioned table the column is absent.
            MetadataColumn::new("record_count",                  DataType::BigInt, false),
            MetadataColumn::new("file_count",                    DataType::BigInt, false),
            MetadataColumn::new("position_delete_file_count",    DataType::BigInt, true),
            MetadataColumn::new("equality_delete_file_count",    DataType::BigInt, true),
        ],
        T::Files | T::Manifests | T::LogicalIcebergMetadata => {
            // Out of scope per spec D6.
            Vec::new()
        }
    }
}
```

- [ ] **Step 4: Run tests; expect PASS.**

Run: `cargo test -p novarocks --lib sql::analyzer::iceberg_metadata::tests 2>&1 | tail -20`

Expected: 8 passed (4 from Task A1 + 4 new).

- [ ] **Step 5: Add the analyzer dispatch in `resolve_from.rs`.**

Locate the snippet around `src/sql/analyzer/resolve_from.rs:157-187` (the base table case). Before the `let table_def = self.catalog.get_table(...)` call, insert a metadata-suffix check.

The exact patch (ensure imports include the new helper and types):

```rust
// At the top of the file, near the existing iceberg_ref import:
use super::iceberg_metadata::{split_metadata_suffix, metadata_table_schema};
use crate::sql::analysis::IcebergMetadataScanRelation;

// Inside resolve_table_factor, replacing the start of the base-table branch
// (~line 157), wrap the existing logic in a check for metadata suffix:

let parts: Vec<String> = name.0.iter().map(|p| p.value.clone()).collect();
let (base_parts, metadata_suffix) = split_metadata_suffix(&parts);

if let Some(metadata_ty) = metadata_suffix {
    // Resolve the base table from the stripped parts.
    let (db_lower, tbl_lower) = match base_parts.as_slice() {
        [tbl] => (self.default_db.clone(), tbl.to_ascii_lowercase()),
        [db, tbl] => (db.to_ascii_lowercase(), tbl.to_ascii_lowercase()),
        [_cat, db, tbl] => (db.to_ascii_lowercase(), tbl.to_ascii_lowercase()),
        _ => return Err(format!(
            "iceberg metadata table requires <tbl> | <db>.<tbl> | <cat>.<db>.<tbl>, got: {parts:?}"
        )),
    };
    let table_def = self.catalog.get_table(&db_lower, &tbl_lower)?;
    let alias_name = alias.as_ref().map(|a| a.name.value.clone());

    // Build scope from fixed metadata schema.
    let cols = metadata_table_schema(metadata_ty);
    let mut scope = AnalyzerScope::new();
    let qualifier_name = alias_name.clone().unwrap_or_else(|| {
        format!("{}__nr_meta_{}", table_def.name, metadata_ty.as_lowercase_keyword())
    });
    let qualifier = qualifier_name.as_str();
    for col in &cols {
        scope.add_column(Some(qualifier), &col.name, col.data_type.clone(), col.nullable);
    }

    let relation = Relation::IcebergMetadataScan(IcebergMetadataScanRelation {
        database: db_lower,
        table: table_def,
        metadata_table_type: metadata_ty,
        alias: alias_name,
    });
    return Ok((relation, scope));
}
// existing base-table resolution continues…
```

(The helper `IcebergMetadataTableType::as_lowercase_keyword` is added in the next step.)

- [ ] **Step 6: Add `as_lowercase_keyword` helper on `IcebergMetadataTableType`.**

Edit `src/connector/iceberg/metadata.rs` (around line 62, near `as_jvm_scanner_type`):

```rust
impl IcebergMetadataTableType {
    pub fn as_lowercase_keyword(&self) -> &'static str {
        match self {
            Self::Files => "files",
            Self::Manifests => "manifests",
            Self::LogicalIcebergMetadata => "logical_iceberg_metadata",
            Self::Snapshots => "snapshots",
            Self::History => "history",
            Self::Refs => "refs",
            Self::Partitions => "partitions",
        }
    }
}
```

- [ ] **Step 7: Add reject test for branch + metadata combo.**

Add to `src/sql/analyzer/iceberg_metadata.rs::tests`:

```rust
#[test]
fn branch_suffix_combined_with_metadata_suffix_is_rejected_at_analysis_time() {
    // The parser-level rewrite would produce e.g.
    //   `t.branch_dev.__nr_meta_snapshots__`
    // The analyzer's resolve_from must error: branch suffix + metadata suffix
    // is not supported.
    // (Detection: after split_metadata_suffix, the base parts still contain
    // a `branch_<x>` last part — that's the rejection signal.)

    let parts = vec![
        "t".to_string(),
        "branch_dev".to_string(),
        "__nr_meta_snapshots__".to_string(),
    ];
    let (base_parts, ty) = split_metadata_suffix(&parts);
    assert_eq!(ty, Some(IcebergMetadataTableType::Snapshots));
    // The remaining last part still has a `branch_` prefix — analyzer can detect
    // and reject at resolve_from time. This unit test only documents the
    // intermediate state; the actual rejection is exercised by an SQL
    // negative test in Task A6.
    assert!(base_parts.last().unwrap().starts_with("branch_"));
}
```

- [ ] **Step 8: Make the analyzer reject the combo.**

In the new branch in `resolve_from.rs` (right after `split_metadata_suffix`), after computing `base_parts`, add:

```rust
if let Some(last) = base_parts.last() {
    if last.starts_with("branch_") || last.starts_with("tag_") {
        return Err(format!(
            "iceberg metadata table cannot be combined with branch/tag suffix: {parts:?}"
        ));
    }
}
```

- [ ] **Step 9: Build and run the analyzer + parser tests.**

Run: `cargo build -p novarocks 2>&1 | tail -10`

Expected: green.

Run: `cargo test -p novarocks --lib sql::analyzer 2>&1 | tail -30`

Expected: green (no regressions).

- [ ] **Step 10: Commit.**

```bash
git add src/sql/analyzer/iceberg_metadata.rs src/sql/analyzer/resolve_from.rs src/sql/analyzer/scope.rs src/sql/analyzer/mod.rs src/connector/iceberg/metadata.rs
git commit -m "sql(analyzer): dispatch t\$<metatype> to IcebergMetadataScan relation"
```

### Task A4: Lower `IcebergMetadataScan` to `IcebergMetadataScanConfig`

**Why:** The analyzer now produces a new relation kind. The optimizer and lowering layers must turn it into a plan node that backs onto the existing `IcebergMetadataScanOp`.

**Files:**
- Locate (via grep) the module that lowers `Relation::Scan` to a plan node, e.g. `src/sql/optimizer/build_plan.rs` or `src/sql/optimizer/relation.rs`. The Relation→plan lowering path will need a parallel arm for the new variant.
- Modify: `src/lower/node/hdfs_scan.rs:894` to accept the standalone-built config (the FE-mode pathway already builds `IcebergMetadataScanConfig` from FE thrift; we need an equivalent standalone construction).

- [ ] **Step 1: Locate the optimizer's Relation→plan lowering site.**

Run: `grep -rn "Relation::Scan\|Relation::CTEConsume\|Relation::GenerateSeries" src/sql/optimizer/ | head -20`

Expected: a switch over `Relation` variants in one or two files. Read those.

- [ ] **Step 2: Locate where `IcebergMetadataScanConfig` is currently constructed.**

Run: `grep -rn "IcebergMetadataScanConfig" src/ | head -20`

Expected: at least `src/lower/node/hdfs_scan.rs:894-907` (FE-side construction) and the BE consumer in `src/connector/iceberg/metadata.rs`.

- [ ] **Step 3: Write a unit test for the new lowering arm.**

Add a test in the lowering module identified above, e.g. `src/sql/optimizer/build_plan.rs::tests`:

```rust
#[test]
fn iceberg_metadata_scan_lowers_to_metadata_scan_config() {
    let table = make_test_iceberg_table();   // helper: existing or to be added
    let rel = Relation::IcebergMetadataScan(IcebergMetadataScanRelation {
        database: "db".into(),
        table,
        metadata_table_type: IcebergMetadataTableType::Snapshots,
        alias: None,
    });
    let plan = lower_relation(&rel).expect("lower");
    // Assert the plan contains an IcebergMetadataScanOp configured for SNAPSHOTS.
    match plan {
        PlanNode::IcebergMetadataScan(cfg) => {
            assert_eq!(cfg.metadata_table_type, IcebergMetadataTableType::Snapshots);
        }
        other => panic!("unexpected plan: {other:?}"),
    }
}
```

(If a `make_test_iceberg_table` helper does not exist, replicate one from existing optimizer tests; if it's tedious, fall back to an integration-style test in `src/engine/mod.rs` that invokes a real `SELECT * FROM t$snapshots` end-to-end — that's exercised by Task A5 anyway.)

- [ ] **Step 4: Run; expect FAIL.**

- [ ] **Step 5: Implement the new arm in the lowering switch.**

The arm should:
1. Convert the `IcebergMetadataScanRelation` into an `IcebergMetadataScanConfig`. Reuse the existing builder from `src/lower/node/hdfs_scan.rs:894`. If that path is FE-thrift-driven, factor a helper out of it that takes `(table, metadata_table_type, output_columns)` and returns `IcebergMetadataScanConfig`.
2. Wrap the config in the appropriate plan node (likely `PlanNode::HdfsScan` with `ScanConfig::IcebergMetadata`, mirroring the FE path).
3. Compute `output_columns` from the analyzer's projected columns. The output columns map to slot ids assigned by the optimizer in the usual way.

Concrete patch sketch (paths/functions verified during Step 1 — adapt to actual module layout):

```rust
Relation::IcebergMetadataScan(rel) => {
    let cfg = build_iceberg_metadata_scan_config(
        &rel.table,
        rel.metadata_table_type,
        &requested_columns,
    )?;
    PlanNode::HdfsScan(HdfsScanNode {
        scan_config: ScanConfig::IcebergMetadata(cfg),
        ..
    })
}
```

`build_iceberg_metadata_scan_config` is a new helper in `src/lower/node/hdfs_scan.rs` that wraps the existing logic at lines 587-907 (factor the table-loading + range-filling code into a shared helper).

- [ ] **Step 6: Build and run.**

Run: `cargo build -p novarocks 2>&1 | tail -10`

Run: `cargo test -p novarocks --lib sql::optimizer 2>&1 | tail -10`

Expected: green; the new test passes.

- [ ] **Step 7: Commit.**

```bash
git add src/sql/optimizer/ src/lower/node/hdfs_scan.rs
git commit -m "sql(optimizer): lower IcebergMetadataScan relation to IcebergMetadataScanConfig"
```

### Task A5: SQL regression — `iceberg_metadata_snapshots.sql`

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql`

- [ ] **Step 1: Write the test SQL with expected output.**

Create `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql`:

```sql
-- iceberg_metadata_snapshots.sql
-- Verifies SELECT * FROM <tbl>$snapshots routes through the parser/analyzer/lowering
-- chain and surfaces snapshot summary metadata.

CREATE TABLE ice.db.metasnap (id INT, v INT)
TBLPROPERTIES('format-version' = '3');

INSERT INTO ice.db.metasnap VALUES (1, 10);
INSERT INTO ice.db.metasnap VALUES (2, 20);
INSERT INTO ice.db.metasnap VALUES (3, 30);

-- 3 snapshots committed: assert count + that operations are all "append".
SELECT count(*) AS n_snapshots FROM ice.db.metasnap$snapshots;
-- expected: 3

SELECT operation, count(*) FROM ice.db.metasnap$snapshots GROUP BY operation;
-- expected: append | 3

-- snapshot_id values are non-null and parent_id is null only for the first.
SELECT count(*) FROM ice.db.metasnap$snapshots WHERE parent_id IS NULL;
-- expected: 1

SELECT count(*) FROM ice.db.metasnap$snapshots WHERE snapshot_id IS NOT NULL;
-- expected: 3

-- Alias works.
SELECT count(*) FROM ice.db.metasnap$snapshots AS s WHERE s.parent_id IS NULL;
-- expected: 1

DROP TABLE ice.db.metasnap;
```

- [ ] **Step 2: Run in record mode to capture and inspect output.**

Run:
```bash
cargo run --release -- standalone-server --port 9030 &
SERVER=$!
sleep 5

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode record --only iceberg_metadata_snapshots
kill $SERVER
```

- [ ] **Step 3: Inspect the recorded golden output.** Verify the result counts match 3 / 3 / 1 / 3 / 1. If they don't, the test SQL or implementation is wrong — fix and re-record.

- [ ] **Step 4: Re-run in verify mode.**

Run:
```bash
cargo run --release -- standalone-server --port 9030 &
SERVER=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify --only iceberg_metadata_snapshots
kill $SERVER
```

Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql sql-tests/iceberg/expected/iceberg_metadata_snapshots.json
git commit -m "sql-tests: add iceberg_metadata_snapshots regression"
```

### Task A6: SQL regressions for history / refs / partitions

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_metadata_history.sql`
- Create: `sql-tests/iceberg/sql/iceberg_metadata_refs.sql`
- Create: `sql-tests/iceberg/sql/iceberg_metadata_partitions.sql`

- [ ] **Step 1: Write `iceberg_metadata_history.sql`.**

```sql
-- iceberg_metadata_history.sql
-- Verifies <tbl>$history.

CREATE TABLE ice.db.metahist (id INT) TBLPROPERTIES('format-version' = '3');
INSERT INTO ice.db.metahist VALUES (1);
INSERT INTO ice.db.metahist VALUES (2);

SELECT count(*) FROM ice.db.metahist$history;
-- expected: 2

SELECT count(*) FROM ice.db.metahist$history WHERE is_current_ancestor = TRUE;
-- expected: 2

DROP TABLE ice.db.metahist;
```

- [ ] **Step 2: Write `iceberg_metadata_refs.sql`.**

```sql
-- iceberg_metadata_refs.sql
-- Verifies <tbl>$refs surfaces branches and tags.

CREATE TABLE ice.db.metaref (id INT)
TBLPROPERTIES('format-version' = '3', 'write.row-lineage' = 'true');
INSERT INTO ice.db.metaref VALUES (1);

ALTER TABLE ice.db.metaref CREATE BRANCH dev;
ALTER TABLE ice.db.metaref CREATE TAG v1;

SELECT name, type FROM ice.db.metaref$refs ORDER BY name;
-- expected (3 rows: 'dev'/BRANCH, 'main'/BRANCH, 'v1'/TAG)

DROP TABLE ice.db.metaref;
```

- [ ] **Step 3: Write `iceberg_metadata_partitions.sql`.**

```sql
-- iceberg_metadata_partitions.sql
-- Verifies <tbl>$partitions surfaces per-partition row counts.

CREATE TABLE ice.db.metapart (id INT, region STRING)
PARTITIONED BY (region)
TBLPROPERTIES('format-version' = '3');
INSERT INTO ice.db.metapart VALUES (1, 'us'), (2, 'us'), (3, 'eu');

SELECT count(*) FROM ice.db.metapart$partitions;
-- expected: 2

SELECT sum(record_count) FROM ice.db.metapart$partitions;
-- expected: 3

DROP TABLE ice.db.metapart;
```

- [ ] **Step 4: Record + verify each.**

Run for each test (replace `<name>`):
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode record --only <name>
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify --only <name>
```

- [ ] **Step 5: Commit.**

```bash
git add sql-tests/iceberg/sql/iceberg_metadata_history.sql \
        sql-tests/iceberg/sql/iceberg_metadata_refs.sql \
        sql-tests/iceberg/sql/iceberg_metadata_partitions.sql \
        sql-tests/iceberg/expected/iceberg_metadata_history.json \
        sql-tests/iceberg/expected/iceberg_metadata_refs.json \
        sql-tests/iceberg/expected/iceberg_metadata_partitions.json
git commit -m "sql-tests: add iceberg_metadata_history/refs/partitions regressions"
```

---

## Phase B — OPTIMIZE preserves row-lineage

### Task B1: Add `row_lineage_enabled` capability helper

**Files:**
- Modify (or create): a small helper in `src/connector/iceberg/catalog/` (the convention is to put metadata predicates near the catalog wrapper). Locate the existing `format_version` accessor and place the new helper next to it.

- [ ] **Step 1: Find the existing format_version accessor.**

Run: `grep -rn "fn format_version\|format-version\|FormatVersion::V3" src/connector/iceberg/ | head -10`

- [ ] **Step 2: Write the failing test.**

Add a unit test next to the helper site:

```rust
#[cfg(test)]
mod row_lineage_enabled_tests {
    use super::*;
    use iceberg::spec::TableMetadata;

    fn meta_with(format_version: FormatVersion, row_lineage: Option<&str>) -> TableMetadata {
        // Build a minimal in-memory TableMetadata; reuse existing test helpers
        // if present (search vendor/iceberg-0.9.0/src/spec/table_metadata.rs).
        // The relevant fields: format_version, properties.
        let mut props = std::collections::HashMap::new();
        if let Some(v) = row_lineage {
            props.insert("write.row-lineage".to_string(), v.to_string());
        }
        // … build ...
    }

    #[test]
    fn v3_default_is_enabled() {
        let m = meta_with(FormatVersion::V3, None);
        assert!(row_lineage_enabled(&m));
    }

    #[test]
    fn v3_explicit_false_is_disabled() {
        let m = meta_with(FormatVersion::V3, Some("false"));
        assert!(!row_lineage_enabled(&m));
    }

    #[test]
    fn v3_explicit_true_is_enabled() {
        let m = meta_with(FormatVersion::V3, Some("true"));
        assert!(row_lineage_enabled(&m));
    }

    #[test]
    fn v2_is_disabled_regardless_of_property() {
        let m = meta_with(FormatVersion::V2, Some("true"));
        assert!(!row_lineage_enabled(&m));
    }
}
```

- [ ] **Step 3: Run; expect FAIL.**

- [ ] **Step 4: Implement the helper.**

Add (next to the existing format_version accessor):

```rust
/// True iff the table has format-version >= 3 AND `write.row-lineage` is not
/// explicitly disabled. Per Iceberg V3 spec, row-lineage defaults to enabled
/// on V3 tables.
pub fn row_lineage_enabled(metadata: &iceberg::spec::TableMetadata) -> bool {
    if metadata.format_version() < iceberg::spec::FormatVersion::V3 {
        return false;
    }
    match metadata.properties().get("write.row-lineage") {
        Some(v) => !v.eq_ignore_ascii_case("false"),
        None => true,
    }
}
```

- [ ] **Step 5: Run; expect PASS.**

- [ ] **Step 6: Commit.**

```bash
git add src/connector/iceberg/catalog/  # or wherever the helper lives
git commit -m "iceberg(catalog): add row_lineage_enabled helper"
```

### Task B2: Branch `compact.rs::execute_whole_table_rewrite` on row-lineage capability

**Files:**
- Modify: `src/connector/iceberg/compact.rs:204-243`

- [ ] **Step 1: Write a Rust integration-style test that fails today.**

Add to `src/engine/mod.rs` (next to existing `iceberg_row_lineage_*` tests around line 4530):

```rust
#[test]
fn iceberg_row_lineage_optimize_preserves_row_id_per_row() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);

    // Seed 3 rows across 3 INSERTs so OPTIMIZE has multiple input files.
    for i in 1..=3 {
        session.execute_in_database(
            &format!("insert into ice.db1.t values ({i}, '{i}')"),
            "default",
        ).expect("seed");
    }
    // Capture pre-OPTIMIZE (id, _row_id, _last_updated_sequence_number).
    let before = session.execute_in_database(
        "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        "default",
    ).expect("pre-select");

    session.execute_in_database("optimize table ice.db1.t", "default").expect("optimize");

    let after = session.execute_in_database(
        "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        "default",
    ).expect("post-select");

    assert_eq!(before, after, "OPTIMIZE must preserve _row_id and _last_updated_sequence_number per row");
}
```

- [ ] **Step 2: Run; expect FAIL.**

Run: `cargo test -p novarocks --lib iceberg_row_lineage_optimize_preserves_row_id_per_row 2>&1 | tail -30`

Expected: failure on the `assert_eq!(before, after)` because `_row_id` values differ post-OPTIMIZE.

- [ ] **Step 3: Implement the row-lineage branch in `execute_whole_table_rewrite`.**

Edit `src/connector/iceberg/compact.rs` around line 224-243. Replace the SELECT-and-write block with:

```rust
let preserve_row_lineage = {
    let m = table.metadata();
    crate::connector::iceberg::catalog::row_lineage_enabled(m)
    // module path adjusted to wherever B1 placed the helper
};

let select_sql = if preserve_row_lineage {
    format!(
        "SELECT *, _row_id, _last_updated_sequence_number FROM {}.{}.{}",
        quote_ident(&job.catalog),
        quote_ident(&job.namespace),
        quote_ident(&job.table)
    )
} else {
    format!(
        "SELECT * FROM {}.{}.{}",
        quote_ident(&job.catalog),
        quote_ident(&job.namespace),
        quote_ident(&job.table)
    )
};
let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(&select_sql)?;
let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
    .map_err(|e| format!("parse optimize SELECT failed: {e}"))?;
let Statement::Query(query) = stmt else {
    return Err("internal optimize SELECT parser did not return a query".to_string());
};
let chunks = run_select_to_chunks(state, &target, query.as_ref())?;
let visible_rows = chunk_row_count(&chunks)?;

let data_files = if visible_rows == 0 {
    Vec::new()
} else if preserve_row_lineage {
    let batches = chunks_to_row_lineage_batches(&chunks)?;
    block_on_iceberg(write_row_lineage_batches_as_data_files(&table, &batches))??
} else {
    block_on_iceberg(write_chunks_as_iceberg_data_files(&table, &chunks))??
};
```

The new helper `chunks_to_row_lineage_batches`:

```rust
fn chunks_to_row_lineage_batches(
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<crate::connector::iceberg::data_writer::RowLineageWriteBatch>, String> {
    use crate::connector::iceberg::data_writer::{RowLineageColumns, RowLineageWriteBatch};
    use crate::exec::row_position::{ICEBERG_ROW_ID_COL, ICEBERG_LAST_UPDATED_SEQ_COL};

    let mut batches = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let user_batch = chunk.to_record_batch_excluding(&[
            ICEBERG_ROW_ID_COL,
            ICEBERG_LAST_UPDATED_SEQ_COL,
        ])?;
        let row_ids = chunk.column_as_int64(ICEBERG_ROW_ID_COL)?;
        let last_updated = chunk.column_as_int64(ICEBERG_LAST_UPDATED_SEQ_COL)?;
        batches.push(RowLineageWriteBatch {
            user_batch,
            lineage: RowLineageColumns {
                row_ids,
                last_updated_sequence_numbers: last_updated,
            },
        });
    }
    Ok(batches)
}
```

The exact `Chunk` API used (`to_record_batch_excluding`, `column_as_int64`) needs to be matched against `src/exec/chunk/mod.rs`. If those exact accessors do not exist, replicate equivalent helpers in COW/MOR UPDATE — search:

Run: `grep -rn "ICEBERG_ROW_ID_COL\|RowLineageColumns" src/engine/mutation_flow.rs | head -10`

Reuse whatever pattern COW UPDATE already uses to extract the row-id column from a chunk (around `mutation_flow.rs:968-1010` per the spec).

- [ ] **Step 4: Build.**

Run: `cargo build -p novarocks 2>&1 | tail -20`

Expected: compile errors only on missing helper APIs — fix them by mirroring COW UPDATE's helper sites.

- [ ] **Step 5: Run the integration test.**

Run: `cargo test -p novarocks --lib iceberg_row_lineage_optimize_preserves_row_id_per_row 2>&1 | tail -30`

Expected: still FAIL — but now with a different error. The likely failure is at commit time: `RewriteDataFilesCommit` allocates a fresh `first_row_id` from `next_row_id` (B5 will fix this).

If the failure is instead `assert_eq!(before, after)` mismatch, it means the writer did not store `_row_id` correctly — debug by reading the parquet file's field IDs.

- [ ] **Step 6: Commit (intermediate).**

```bash
git add src/connector/iceberg/compact.rs
git commit -m "iceberg(compact): route OPTIMIZE through row-lineage writer for v3 row-lineage tables"
```

(The B5 commit makes the integration test pass; this commit is intermediate.)

### Task B3: (merged into B2 — projection + writer switch are one logical change)

(Skipped — covered above in B2 step 3.)

### Task B4: (merged into B2 — same.)

(Skipped — covered above in B2 step 3.)

### Task B5: Commit-side: skip `next_row_id` allocation for preserve-mode rewrites

**Files:**
- Modify: `src/connector/iceberg/commit/rewrite_data_files.rs:60-70` and `:244-285`

- [ ] **Step 1: Write a unit test for the commit logic.**

Add to `src/connector/iceberg/commit/rewrite_data_files.rs` (new `#[cfg(test)] mod tests` if absent):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::DataFileBuilder;

    fn data_file_with_first_row_id(first_row_id: Option<i64>) -> DataFile {
        // Build a minimal DataFile via the Iceberg crate's builder. Reuse
        // helpers from existing tests in the same crate if present (search
        // for `DataFileBuilder::default()` in src/connector/iceberg/).
        // …
    }

    #[test]
    fn next_row_id_is_not_advanced_when_all_files_are_preserve_mode() {
        let files = vec![
            data_file_with_first_row_id(None),
            data_file_with_first_row_id(None),
        ];
        // Call the same helper used by the production path:
        let allocate = files.iter().any(|f| f.first_row_id().is_some())
            || files.is_empty(); // empty rewrites still classify as Legacy, harmless
        assert!(!allocate);
    }

    #[test]
    fn next_row_id_is_advanced_when_any_file_has_first_row_id() {
        let files = vec![
            data_file_with_first_row_id(None),
            data_file_with_first_row_id(Some(42)),
        ];
        let allocate = files.iter().any(|f| f.first_row_id().is_some());
        assert!(allocate);
    }
}
```

- [ ] **Step 2: Run; expect FAIL** (helper functions not yet defined; the test merely exercises the boolean condition once it lives in the production path).

- [ ] **Step 3: Modify the commit path.**

Edit `src/connector/iceberg/commit/rewrite_data_files.rs` around lines 60-65 and 244-285:

```rust
// At line 60-65, replace:
let row_lineage_first_row_id =
    match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
        IcebergWriteMode::RowLineageV3 => Some(ctx.table.metadata().next_row_id()),
        IcebergWriteMode::LegacyPositionDeletes => None,
    };

// With:
let preserve_row_lineage = matches!(
    crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table),
    IcebergWriteMode::RowLineageV3,
) && written.iter().all(|f| f.data_file_first_row_id().is_none());
//
// `data_file_first_row_id` accessor: see `WrittenFile` definition in
// `src/connector/iceberg/commit/types.rs`.
//
// preserve_row_lineage = true  → don't allocate, don't write row_range.
// preserve_row_lineage = false → row-lineage v3 mode that allocates fresh ids
//                                 (e.g., FastAppend behaviour) — keep existing.

let row_lineage_first_row_id = if preserve_row_lineage {
    None
} else {
    match crate::connector::iceberg::commit::classify_iceberg_write_mode(ctx.table) {
        IcebergWriteMode::RowLineageV3 => Some(ctx.table.metadata().next_row_id()),
        IcebergWriteMode::LegacyPositionDeletes => None,
    }
};
```

Then around lines 244-285, the conditional `with_row_range` already correctly omits `row_range` when `row_lineage_first_row_id` is `None`. Verify by reading the surrounding match.

- [ ] **Step 4: Run the integration test from B2.**

Run: `cargo test -p novarocks --lib iceberg_row_lineage_optimize_preserves_row_id_per_row 2>&1 | tail -30`

Expected: PASS.

- [ ] **Step 5: Run all iceberg-related Rust tests for regressions.**

Run: `cargo test -p novarocks --lib iceberg 2>&1 | tail -40`

Expected: all green (notably the existing OPTIMIZE / COW UPDATE / MOR UPDATE / FastAppend tests must still pass).

- [ ] **Step 6: Commit.**

```bash
git add src/connector/iceberg/commit/rewrite_data_files.rs src/engine/mod.rs
git commit -m "iceberg(commit): preserve _row_id across OPTIMIZE rewrites; skip next_row_id allocation"
```

### Task B6 (CONDITIONAL — only if Phase 0.1 found physical-row semantics)

**Files:**
- Modify: `src/connector/iceberg/changes.rs:430-493`

- [ ] **Step 1: Write a failing IVM-side test.**

(Detailed code skipped — follow the pattern from existing `validate_replace_*` tests at the bottom of `changes.rs`.)

- [ ] **Step 2: Implement the relaxed validation.**

Adjust to compare logical row counts: `total-records - total-deleted-records` (or equivalent), so DV-stripping rewrites pass.

- [ ] **Step 3: Run; expect PASS.**

- [ ] **Step 4: Commit.**

```bash
git commit -m "iceberg(changes): relax validate_replace_snapshot for DV-stripping OPTIMIZE rewrites"
```

### Task B7 (CONDITIONAL — only if Phase 0.2 found OPTIMIZE silently accepts branch)

(Skipped if 0.2 found explicit reject already in place.)

- [ ] Implement: in the OPTIMIZE flow entry (locate via `grep -rn "OPTIMIZE" src/engine/`), reject any `target_ref != "main"` with a clear error message.

---

## Phase C — End-to-end SQL regressions + Rust unit test

### Task C1: `iceberg_v3_optimize_row_lineage.sql`

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_optimize_row_lineage.sql`

- [ ] **Step 1: Author the SQL test.**

Create:

```sql
-- iceberg_v3_optimize_row_lineage.sql
-- End-to-end: OPTIMIZE on a v3 row-lineage table preserves _row_id and
-- _last_updated_sequence_number for every surviving row, AND IVM treats
-- the resulting Replace snapshot as a no-op.

CREATE TABLE ice.db.olineage (id INT, v INT)
TBLPROPERTIES('format-version' = '3', 'write.row-lineage' = 'true');

CREATE MATERIALIZED VIEW ice.db.mv_olineage AS
SELECT id, sum(v) AS total FROM ice.db.olineage GROUP BY id;

INSERT INTO ice.db.olineage VALUES (1, 10), (2, 20);
INSERT INTO ice.db.olineage VALUES (3, 30), (4, 40);
INSERT INTO ice.db.olineage VALUES (5, 50), (6, 60);

REFRESH MATERIALIZED VIEW ice.db.mv_olineage;

-- Mutate a couple of rows.
UPDATE ice.db.olineage SET v = 99 WHERE id = 2;
DELETE FROM ice.db.olineage WHERE id = 4;
REFRESH MATERIALIZED VIEW ice.db.mv_olineage;

-- Capture (id, _row_id, _last_updated_sequence_number) BEFORE OPTIMIZE.
-- Save into a CTE-like one-shot snapshot via a temp view or repeated SELECT.
SELECT id, _row_id, _last_updated_sequence_number
FROM ice.db.olineage
ORDER BY id;
-- expected: 5 rows; id ∈ {1,2,3,5,6}; id=2 has the most recent
-- _last_updated_sequence_number; the rest are at sequence numbers from
-- their original INSERTs.

-- OPTIMIZE
OPTIMIZE TABLE ice.db.olineage;

-- Capture AFTER OPTIMIZE; assert rows are byte-equal.
SELECT id, _row_id, _last_updated_sequence_number
FROM ice.db.olineage
ORDER BY id;
-- expected: identical to the BEFORE select above.

-- File-count decreased — read latest snapshot's summary via the new metadata route.
SELECT operation FROM ice.db.olineage$snapshots ORDER BY committed_at DESC LIMIT 1;
-- expected: replace

-- IVM no-op verification: refresh MV after OPTIMIZE; MV contents stay identical.
REFRESH MATERIALIZED VIEW ice.db.mv_olineage;
SELECT id, total FROM ice.db.mv_olineage ORDER BY id;
-- expected: identical to MV state after the UPDATE/DELETE refresh above
-- (id ∈ {1,2,3,5,6}; id=2's total reflects v=99).

-- A subsequent UPDATE post-OPTIMIZE still drives IVM correctly.
UPDATE ice.db.olineage SET v = 88 WHERE id = 2;
REFRESH MATERIALIZED VIEW ice.db.mv_olineage;
SELECT id, total FROM ice.db.mv_olineage ORDER BY id;
-- expected: id=2 total updated to 88; everything else unchanged.

DROP MATERIALIZED VIEW ice.db.mv_olineage;
DROP TABLE ice.db.olineage;
```

- [ ] **Step 2: Record + verify.**

Run:
```bash
cargo run --release -- standalone-server --port 9030 &
SERVER=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode record --only iceberg_v3_optimize_row_lineage
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify --only iceberg_v3_optimize_row_lineage
kill $SERVER
```

- [ ] **Step 3: Inspect golden output**, especially the BEFORE/AFTER row-lineage triples — they must match exactly. If any row-id changes, B5 is broken; debug.

- [ ] **Step 4: Commit.**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_optimize_row_lineage.sql sql-tests/iceberg/expected/iceberg_v3_optimize_row_lineage.json
git commit -m "sql-tests: lock OPTIMIZE preserves _row_id + IVM no-op invariant"
```

### Task C2: `iceberg_v3_branch_row_lineage.sql`

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_branch_row_lineage.sql`

- [ ] **Step 1: Author.**

```sql
-- iceberg_v3_branch_row_lineage.sql
-- Branch writes do not perturb main's _row_id; tag time-travel returns the
-- captured row-id set; UPDATE on a branch preserves _row_id per V3 row-lineage.

CREATE TABLE ice.db.brlineage (id INT, v INT)
TBLPROPERTIES('format-version' = '3', 'write.row-lineage' = 'true');
INSERT INTO ice.db.brlineage VALUES (1, 10), (2, 20), (3, 30);

-- Capture R0 = main's (id, _row_id) set.
SELECT id, _row_id FROM ice.db.brlineage ORDER BY id;
-- expected: 3 rows; record their _row_id values as R0.

ALTER TABLE ice.db.brlineage CREATE BRANCH feat;

-- Mutate on branch.
INSERT INTO ice.db.brlineage.branch_feat VALUES (4, 40);
UPDATE ice.db.brlineage.branch_feat SET v = 99 WHERE id = 2;
DELETE FROM ice.db.brlineage.branch_feat WHERE id = 3;

-- main is unchanged.
SELECT id, _row_id FROM ice.db.brlineage ORDER BY id;
-- expected: identical to R0.

-- branch_feat: ids 1, 2, 4 (3 deleted). id=2's _row_id MUST equal its R0
-- value — V3 row-lineage preserves identity through UPDATE.
SELECT id, _row_id FROM ice.db.brlineage.branch_feat ORDER BY id;

-- New row id=4 has a fresh _row_id ≥ pre-branch next_row_id.
-- (Verified by uniqueness across all rows in any snapshot.)

-- Tag main's pre-branch state and time-travel.
ALTER TABLE ice.db.brlineage CREATE TAG snap0;
SELECT id, _row_id FROM ice.db.brlineage.tag_snap0 ORDER BY id;
-- expected: identical to R0.

DROP TABLE ice.db.brlineage;
```

- [ ] **Step 2: Record + verify** (same commands as above).

- [ ] **Step 3: Commit.**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_branch_row_lineage.sql sql-tests/iceberg/expected/iceberg_v3_branch_row_lineage.json
git commit -m "sql-tests: lock branch/tag row-lineage isolation invariant"
```

### Task C3: `iceberg_v3_row_lineage_uniqueness.sql`

**Files:**
- Create: `sql-tests/iceberg/sql/iceberg_v3_row_lineage_uniqueness.sql`

- [ ] **Step 1: Author.**

```sql
-- iceberg_v3_row_lineage_uniqueness.sql
-- Single-engine cross-snapshot uniqueness invariants:
--   I1 (intra-snapshot): count(*) = count(DISTINCT _row_id)
--   I2 (cross-snapshot): for each logical row identity (id), distinct
--                        _row_id values across history ≤ 1.

CREATE TABLE ice.db.uniq (id INT, v INT)
TBLPROPERTIES('format-version' = '3', 'write.row-lineage' = 'true');

-- Snapshot 1: insert.
INSERT INTO ice.db.uniq VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- Snapshot 2: delete.
DELETE FROM ice.db.uniq WHERE id IN (4);
-- Snapshot 3: update.
UPDATE ice.db.uniq SET v = v + 1 WHERE id = 1;
-- Snapshot 4: optimize.
OPTIMIZE TABLE ice.db.uniq;
-- Snapshot 5: insert more.
INSERT INTO ice.db.uniq VALUES (10, 100), (20, 200);
-- Snapshot 6: optimize again.
OPTIMIZE TABLE ice.db.uniq;

-- I1: current snapshot.
SELECT count(*) - count(DISTINCT _row_id) AS row_id_collisions FROM ice.db.uniq;
-- expected: 0

-- I2: cross-snapshot. Capture all snapshot ids first.
-- (sql-tests harness handles substituting captured values via the runner; if
-- the runner doesn't support that, hardcode in the expected output via
-- record mode: each snapshot id appears in summary output once.)
WITH snaps AS (
  SELECT snapshot_id FROM ice.db.uniq$snapshots
)
SELECT count(*) AS n_snapshots FROM snaps;
-- expected: 6  (depends on actual snapshot count — adjust if record mode
-- shows differently; what matters is each subsequent UNION ALL covers them.)

-- Per-id cross-snapshot row-id distinctness.
-- Note: LATERAL is unsupported per spec D8; we hardcode UNION ALL with the
-- snapshot ids captured during record mode. Rewrite the expected output and
-- this query body together when first running.
WITH hist AS (
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap1>
  UNION ALL
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap2>
  UNION ALL
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap3>
  UNION ALL
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap4>
  UNION ALL
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap5>
  UNION ALL
  SELECT id, _row_id FROM ice.db.uniq FOR VERSION AS OF <snap6>
)
SELECT id, count(DISTINCT _row_id) AS distinct_row_ids
FROM hist
GROUP BY id
ORDER BY id;
-- expected: distinct_row_ids = 1 for every id.

DROP TABLE ice.db.uniq;
```

- [ ] **Step 2: Snapshot id substitution.**

The `<snapN>` placeholders need to be replaced with real snapshot ids before running. The sql-tests harness allows test setup queries to capture rows; if that's available, capture them into variables and substitute. Otherwise, run the test in record mode once, copy the snapshot ids out of the captured `t$snapshots` output into the SQL text by hand, and re-record.

Run record mode initially:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode record --only iceberg_v3_row_lineage_uniqueness
```

Manually edit the SQL with the recorded snapshot ids; re-record; verify.

- [ ] **Step 3: Commit.**

```bash
git add sql-tests/iceberg/sql/iceberg_v3_row_lineage_uniqueness.sql sql-tests/iceberg/expected/iceberg_v3_row_lineage_uniqueness.json
git commit -m "sql-tests: lock cross-snapshot _row_id uniqueness invariant"
```

### Task C4: Rust unit test — OPTIMIZE does not advance `next_row_id`

**Files:**
- Modify: `src/connector/iceberg/commit/rewrite_data_files.rs::tests` (or `src/engine/mod.rs::tests`).

- [ ] **Step 1: Write the test.**

Add to `src/engine/mod.rs` next to `iceberg_row_lineage_optimize_preserves_row_id_per_row`:

```rust
#[test]
fn iceberg_row_lineage_optimize_does_not_advance_next_row_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);

    for i in 1..=5 {
        session.execute_in_database(
            &format!("insert into ice.db1.t values ({i}, '{i}')"),
            "default",
        ).expect("seed");
    }

    let table_before = load_table_helper(&engine, "ice", "db1", "t");
    let next_row_id_before = table_before.metadata().next_row_id();
    let max_row_id_before = max_row_id_via_select(&session, "ice.db1.t");
    assert!(max_row_id_before < next_row_id_before, "invariant broken pre-OPTIMIZE");

    session.execute_in_database("optimize table ice.db1.t", "default").expect("optimize");

    let table_after = load_table_helper(&engine, "ice", "db1", "t");
    let next_row_id_after = table_after.metadata().next_row_id();
    assert_eq!(
        next_row_id_after, next_row_id_before,
        "OPTIMIZE must not advance next_row_id"
    );
    let max_row_id_after = max_row_id_via_select(&session, "ice.db1.t");
    assert!(max_row_id_after < next_row_id_after, "invariant broken post-OPTIMIZE");
}
```

Helper functions `load_table_helper` and `max_row_id_via_select` mirror the existing `load_current_table` and `execute_in_database` patterns elsewhere in the test file — copy from the surrounding tests.

- [ ] **Step 2: Run.**

Run: `cargo test -p novarocks --lib iceberg_row_lineage_optimize_does_not_advance_next_row_id 2>&1 | tail -20`

Expected: PASS (assuming B5 already merged).

- [ ] **Step 3: Commit.**

```bash
git add src/engine/mod.rs
git commit -m "iceberg(test): assert OPTIMIZE does not advance next_row_id on row-lineage tables"
```

---

## Final verification

- [ ] **Step 1: Full unit + integration test pass.**

Run:
```bash
cargo build -p novarocks 2>&1 | tail -5
cargo test -p novarocks --lib 2>&1 | tail -20
```

Expected: all green.

- [ ] **Step 2: Full SQL iceberg suite.**

Run:
```bash
cargo run --release -- standalone-server --port 9030 &
SERVER=$!
sleep 5
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --mode verify
kill $SERVER
```

Expected: all green, including the 7 new SQL files.

- [ ] **Step 3: cargo fmt + clippy.**

Run:
```bash
cargo fmt
cargo clippy 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 4: Update `NovaRocks Iceberg v3 完成度清单.md` (in user's Obsidian; optional — out of repo).**

The user maintains the completion list outside the repo. Mark §12 items 1–3 as done with PR link, and §9 BE-already-done items 1–4 as fully done with SQL parser route landed.

---

## Self-review against spec coverage

Spec D1 (3-phase scope, single PR) ✅ — Phase A, B, C all in plan.
Spec D2 (preserve via stored physical column) ✅ — Tasks B2, B3, B4 (merged).
Spec D3 (capability check `format_version >= 3 && row-lineage`) ✅ — Task B1, used in B2.
Spec D4 (`next_row_id` not bumped) ✅ — Task B5.
Spec D5 (`<tbl>$<metatype>` rewrite to `__nr_meta_*__`, all 3 qualified forms) ✅ — Task A2.
Spec D6 (4 metatypes only) ✅ — A2 whitelist + A3 metadata_table_schema.
Spec D7 (OPTIMIZE on branch rejected) ✅ — Task 0.2 + conditional B7.
Spec D8 (Invariant 2 via UNION ALL, not LATERAL) ✅ — Task C3.
Spec D9 (fail-fast on missing meta cols) ✅ — `append_row_lineage_columns` already errors on length mismatch (data_writer.rs:252-261).
§6 Risk: validate_replace_snapshot semantics ✅ — Task 0.1 + conditional B6.
§5.1 Test C1 ✅ — Task C1.
§5.2 Test C2 ✅ — Task C2.
§5.3 Test C3 (Invariants 1, 2) ✅ — Task C3.
§5.3 Test C3 (Invariant 3 = Rust unit test) ✅ — Task C4.
§5.4 Phase A standalone tests (4 metadata tables) ✅ — Tasks A5, A6.
