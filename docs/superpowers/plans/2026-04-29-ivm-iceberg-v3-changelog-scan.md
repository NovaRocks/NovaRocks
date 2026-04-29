# IVM-on-Iceberg-v3 Changelog Scan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend NovaRocks IVM Phase 2 (`plan_changes` + `scan_deletes`) to accept Iceberg v3 row-lineage base tables and Puffin deletion-vector deletes, end-to-end打通 internal-MV-on-iceberg-base 在 INSERT + DELETE 上的增量刷新。

**Architecture:** Extend `PositionDeleteRef` with optional DV metadata fields; `collect_files::CollectDeletes` dispatches on `DataFile::file_format()` (Parquet vs Puffin); new `read_dv_positions_per_data_file` helper in `scan_deletes` reads Puffin DV bytes via existing `commit::puffin_dv::read_deletion_vector_puffin`; `read_data_file_at_positions` is reused unchanged because it operates on raw parquet row offsets independent of the delete-file format.

**Tech Stack:** Rust 2024, vendored iceberg-rust 0.9 (Phase 2a Patch 4 already exposes Puffin DV reader internally), arrow/parquet 57, roaring 0.11.

---

## File Structure

| Path | Responsibility |
|---|---|
| `src/connector/iceberg/changes.rs` | Add 3 new fields to `PositionDeleteRef`; dispatch `collect_files::CollectDeletes` by file_format; update `materialize_changes` to pass `FileIO` to scan_deletes; improve OVERWRITE error wording |
| `src/connector/iceberg/scan_deletes.rs` | Add `read_dv_positions_per_data_file`; partition `scan_deletes` input by file_format; add `file_io` parameter |
| `src/connector/iceberg/commit/puffin_dv.rs` | Add `DeletionVector::to_roaring_treemap` |
| `src/connector/starrocks/managed/mv_ddl.rs` | Relax `validate_ivm_primary_key` to accept format_version 2 and 3 |
| `src/connector/starrocks/managed/mv_refresh.rs` | Add row-lineage end-to-end integration test |

---

### Task 1: Extend `PositionDeleteRef` data model + add `DeletionVector::to_roaring_treemap`

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/iceberg/scan_deletes.rs` (test struct literals only)
- Modify: `src/connector/iceberg/commit/puffin_dv.rs`

- [ ] **Step 1: Write failing test for `DeletionVector::to_roaring_treemap`**

In `src/connector/iceberg/commit/puffin_dv.rs` `mod tests`, add:

```rust
#[test]
fn to_roaring_treemap_round_trips_positions() {
    let mut dv = DeletionVector::new();
    dv.insert(0).unwrap();
    dv.insert(7).unwrap();
    dv.insert(u32::MAX as u64 + 3).unwrap();
    let treemap = dv.to_roaring_treemap();
    assert_eq!(treemap.len(), 3);
    assert!(treemap.contains(0));
    assert!(treemap.contains(7));
    assert!(treemap.contains(u32::MAX as u64 + 3));
}

#[test]
fn to_roaring_treemap_empty_for_empty_dv() {
    let dv = DeletionVector::new();
    assert!(dv.to_roaring_treemap().is_empty());
}
```

- [ ] **Step 2: Run tests and verify they fail to compile**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: FAIL — `to_roaring_treemap` is not defined on `DeletionVector`.

- [ ] **Step 3: Implement `DeletionVector::to_roaring_treemap`**

In `src/connector/iceberg/commit/puffin_dv.rs`, add to the `impl DeletionVector` block (after `is_empty` is a good spot):

```rust
/// Convert this deletion vector into a flat [`RoaringTreemap`] over the
/// full 64-bit position space. Used by the IVM-changelog-scan path
/// (`scan_deletes`) to reuse the v2-style `RoaringTreemap`-based
/// position-set machinery without having to introduce a new bitmap type.
pub fn to_roaring_treemap(&self) -> roaring::RoaringTreemap {
    let mut out = roaring::RoaringTreemap::new();
    for (high_key, bitmap) in &self.bitmaps {
        let high = (*high_key as u64) << 32;
        for low in bitmap {
            out.insert(high | low as u64);
        }
    }
    out
}
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: PASS — both new tests + all 7 existing tests green.

- [ ] **Step 5: Extend `PositionDeleteRef` with v3 DV fields**

In `src/connector/iceberg/changes.rs`, find the existing `PositionDeleteRef` struct (currently around line 138) and replace:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
}
```

with:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,
    /// `Parquet` for v2 position-delete files, `Puffin` for v3 deletion-vector
    /// files. Other variants are rejected at construction.
    pub file_format: iceberg::spec::DataFileFormat,
    /// Required when `file_format == Puffin`: byte offset of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_offset: Option<i64>,
    /// Required when `file_format == Puffin`: byte length of the
    /// `deletion-vector-v1` blob inside the Puffin file. Must be `None` when
    /// `file_format == Parquet`.
    pub content_size_in_bytes: Option<i64>,
}

impl PositionDeleteRef {
    /// Verify the file_format / content_offset / content_size_in_bytes /
    /// referenced_data_file fields are mutually consistent. Returns
    /// `ChangeError::InternalInconsistency` on any mismatch.
    pub(crate) fn validate_invariants(&self) -> Result<(), ChangeError> {
        use iceberg::spec::DataFileFormat;
        match self.file_format {
            DataFileFormat::Parquet => {
                if self.content_offset.is_some() || self.content_size_in_bytes.is_some() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "PositionDeleteRef {} has Parquet file_format but content_offset/size set",
                        self.delete_file_path
                    )));
                }
            }
            DataFileFormat::Puffin => {
                if self.referenced_data_file.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing referenced_data_file",
                        self.delete_file_path
                    )));
                }
                if self.content_offset.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_offset",
                        self.delete_file_path
                    )));
                }
                if self.content_size_in_bytes.is_none() {
                    return Err(ChangeError::InternalInconsistency(format!(
                        "Puffin DV {} missing content_size_in_bytes",
                        self.delete_file_path
                    )));
                }
            }
            other => {
                return Err(ChangeError::InternalInconsistency(format!(
                    "PositionDeleteRef {} has unsupported file_format {:?}",
                    self.delete_file_path, other
                )));
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 6: Update `collect_files` to populate new fields with Parquet defaults**

In `src/connector/iceberg/changes.rs`, find the `LineageAction::CollectDeletes` arm in `collect_files` (around line 601) and update the `DataContentType::PositionDeletes` branch to populate the new fields. Replace:

```rust
DataContentType::PositionDeletes => {
    deletes.push(PositionDeleteRef {
        delete_file_path: df.file_path().to_string(),
        delete_file_size: i64::try_from(df.file_size_in_bytes())
            .unwrap_or(i64::MAX),
        record_count: Some(
            i64::try_from(df.record_count()).unwrap_or(i64::MAX),
        ),
        referenced_data_file: df.referenced_data_file(),
    });
}
```

with:

```rust
DataContentType::PositionDeletes => {
    deletes.push(PositionDeleteRef {
        delete_file_path: df.file_path().to_string(),
        delete_file_size: i64::try_from(df.file_size_in_bytes())
            .unwrap_or(i64::MAX),
        record_count: Some(
            i64::try_from(df.record_count()).unwrap_or(i64::MAX),
        ),
        referenced_data_file: df.referenced_data_file(),
        file_format: iceberg::spec::DataFileFormat::Parquet,
        content_offset: None,
        content_size_in_bytes: None,
    });
}
```

(Note: this commit keeps the v2-only behavior; Task 2 will add the Puffin dispatch.)

- [ ] **Step 7: Update existing `PositionDeleteRef` test literals in `scan_deletes.rs`**

In `src/connector/iceberg/scan_deletes.rs` `mod tests`, update the four test cases that construct `PositionDeleteRef { ... }` literally. The pattern: append `file_format: iceberg::spec::DataFileFormat::Parquet, content_offset: None, content_size_in_bytes: None,` to each struct literal.

Specifically update these tests (search by name):
- `read_delete_positions_groups_by_file_path` (currently around line 381)
- `scan_deletes_projects_rows_for_single_data_file` (around line 446)
- `scan_deletes_projects_across_multiple_data_files` (around line 468)

Each construction must look like:

```rust
let refs = vec![PositionDeleteRef {
    delete_file_path: "deletes.parquet".to_string(),
    delete_file_size: 0,
    record_count: Some(3),
    referenced_data_file: None,
    file_format: iceberg::spec::DataFileFormat::Parquet,
    content_offset: None,
    content_size_in_bytes: None,
}];
```

(Adjust the field values per existing test; only the three new fields are appended.)

- [ ] **Step 8: Add invariant tests in `changes.rs`**

In `src/connector/iceberg/changes.rs` `mod tests`, add:

```rust
#[test]
fn position_delete_ref_validates_parquet_with_no_content_offset() {
    let r = super::PositionDeleteRef {
        delete_file_path: "/tmp/x.parquet".to_string(),
        delete_file_size: 0,
        record_count: None,
        referenced_data_file: None,
        file_format: iceberg::spec::DataFileFormat::Parquet,
        content_offset: None,
        content_size_in_bytes: None,
    };
    r.validate_invariants().expect("ok");
}

#[test]
fn position_delete_ref_rejects_parquet_with_content_offset() {
    let r = super::PositionDeleteRef {
        delete_file_path: "/tmp/x.parquet".to_string(),
        delete_file_size: 0,
        record_count: None,
        referenced_data_file: None,
        file_format: iceberg::spec::DataFileFormat::Parquet,
        content_offset: Some(0),
        content_size_in_bytes: None,
    };
    let err = r.validate_invariants().expect_err("must reject");
    assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
}

#[test]
fn position_delete_ref_validates_puffin_with_full_metadata() {
    let r = super::PositionDeleteRef {
        delete_file_path: "/tmp/dv.puffin".to_string(),
        delete_file_size: 0,
        record_count: None,
        referenced_data_file: Some("/tmp/data.parquet".to_string()),
        file_format: iceberg::spec::DataFileFormat::Puffin,
        content_offset: Some(4),
        content_size_in_bytes: Some(120),
    };
    r.validate_invariants().expect("ok");
}

#[test]
fn position_delete_ref_rejects_puffin_missing_offset() {
    let r = super::PositionDeleteRef {
        delete_file_path: "/tmp/dv.puffin".to_string(),
        delete_file_size: 0,
        record_count: None,
        referenced_data_file: Some("/tmp/data.parquet".to_string()),
        file_format: iceberg::spec::DataFileFormat::Puffin,
        content_offset: None,
        content_size_in_bytes: Some(120),
    };
    let err = r.validate_invariants().expect_err("must reject");
    assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
}

#[test]
fn position_delete_ref_rejects_puffin_missing_referenced_data_file() {
    let r = super::PositionDeleteRef {
        delete_file_path: "/tmp/dv.puffin".to_string(),
        delete_file_size: 0,
        record_count: None,
        referenced_data_file: None,
        file_format: iceberg::spec::DataFileFormat::Puffin,
        content_offset: Some(4),
        content_size_in_bytes: Some(120),
    };
    let err = r.validate_invariants().expect_err("must reject");
    assert!(matches!(err, super::ChangeError::InternalInconsistency(_)));
}
```

- [ ] **Step 9: Run the focused tests to verify everything compiles and passes**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::changes -- --nocapture
cargo test -p novarocks --lib connector::iceberg::scan_deletes -- --nocapture
cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv -- --nocapture
```

Expected: all PASS. Existing tests still green; 5 new invariant tests + 2 new to_roaring_treemap tests pass.

- [ ] **Step 10: Commit**

```bash
git add src/connector/iceberg/commit/puffin_dv.rs src/connector/iceberg/changes.rs src/connector/iceberg/scan_deletes.rs
git commit -m "$(cat <<'EOF'
feat: extend PositionDeleteRef with v3 deletion-vector fields

Adds optional content_offset / content_size_in_bytes / file_format
fields on PositionDeleteRef so Puffin DV manifest entries can travel
through plan_changes alongside v2 Parquet position-delete entries.
PositionDeleteRef::validate_invariants asserts mutual consistency
between file_format and the DV-specific fields. DeletionVector gains
a to_roaring_treemap method to bridge to the existing scan_deletes
position-set machinery. collect_files still treats every entry as
Parquet today; Puffin dispatch lands in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Dispatch `collect_files` by file_format + relax format-version check + improve OVERWRITE message

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/starrocks/managed/mv_ddl.rs`

- [ ] **Step 1: Write failing test for v3 acceptance in `validate_ivm_primary_key`**

In `src/connector/starrocks/managed/mv_ddl.rs` `mod tests` (or wherever `validate_ivm_primary_key` is currently tested — see line ~1396 for the existing v1-rejection test), add next to it:

```rust
#[test]
fn validate_ivm_primary_key_accepts_v3_base() {
    let base = super::BaseTableDescriptor {
        format_version: 3,
        columns: vec![super::BaseColumnDescriptor {
            name: "id".to_string(),
            sql_type: "BIGINT".to_string(),
            nullable: false,
        }],
    };
    super::validate_ivm_primary_key(&["id".to_string()], &base).expect("v3 must be accepted");
}

#[test]
fn validate_ivm_primary_key_rejects_v1_base_with_clear_message() {
    let base = super::BaseTableDescriptor {
        format_version: 1,
        columns: vec![],
    };
    let err = super::validate_ivm_primary_key(&["id".to_string()], &base)
        .expect_err("v1 must be rejected");
    assert!(matches!(
        err,
        crate::connector::iceberg::changes::ChangeError::IcebergFormatUnsupported {
            format_version: 1,
        }
    ));
}
```

- [ ] **Step 2: Run tests and verify the v3 case fails**

Run:

```bash
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::validate_ivm_primary_key -- --nocapture
```

Expected: `validate_ivm_primary_key_accepts_v3_base` FAILS (current code rejects format_version != 2). The v1 test PASSES.

- [ ] **Step 3: Relax format-version check in `validate_ivm_primary_key`**

In `src/connector/starrocks/managed/mv_ddl.rs`, find:

```rust
if base.format_version != 2 {
    return Err(ChangeError::IcebergFormatUnsupported {
        format_version: base.format_version,
    });
}
```

Replace with:

```rust
if base.format_version != 2 && base.format_version != 3 {
    return Err(ChangeError::IcebergFormatUnsupported {
        format_version: base.format_version,
    });
}
```

- [ ] **Step 4: Update `IcebergFormatUnsupported` error message in `changes.rs`**

In `src/connector/iceberg/changes.rs`, find the Display impl for `IcebergFormatUnsupported` (around line 102):

```rust
ChangeError::IcebergFormatUnsupported { format_version } => write!(
    f,
    "iceberg base table format-version {format_version} is not supported; IVM Phase 2 requires v2"
),
```

Replace the trailing wording:

```rust
ChangeError::IcebergFormatUnsupported { format_version } => write!(
    f,
    "iceberg base table format-version {format_version} is not supported; IVM requires v2 or v3"
),
```

- [ ] **Step 5: Run focused tests to verify v3 acceptance**

Run:

```bash
cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::validate_ivm_primary_key -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Note on unit-test coverage for the Puffin dispatch**

Pure unit-level coverage of `collect_files`'s Puffin branch requires hand-rolling iceberg manifest-avro fixtures with deletion-vector entries — not worth the maintenance cost. The Puffin dispatch is exercised end-to-end by Task 4's integration test against a real `RowDeltaDvCommit`-produced DV. The `validate_invariants` unit tests in Task 1 already cover the structural invariants of every constructed `PositionDeleteRef`. **Skip writing a unit test here and proceed to Step 7.**

- [ ] **Step 7: Update `collect_files::CollectDeletes` to dispatch by file_format**

In `src/connector/iceberg/changes.rs`, find the `LineageAction::CollectDeletes` branch's `match df.content_type()` block (around line 623) and replace the `DataContentType::PositionDeletes` arm. Current code:

```rust
DataContentType::PositionDeletes => {
    deletes.push(PositionDeleteRef {
        delete_file_path: df.file_path().to_string(),
        delete_file_size: i64::try_from(df.file_size_in_bytes())
            .unwrap_or(i64::MAX),
        record_count: Some(
            i64::try_from(df.record_count()).unwrap_or(i64::MAX),
        ),
        referenced_data_file: df.referenced_data_file(),
        file_format: iceberg::spec::DataFileFormat::Parquet,
        content_offset: None,
        content_size_in_bytes: None,
    });
}
```

Replace with:

```rust
DataContentType::PositionDeletes => {
    use iceberg::spec::DataFileFormat;
    let r = match df.file_format() {
        DataFileFormat::Parquet => PositionDeleteRef {
            delete_file_path: df.file_path().to_string(),
            delete_file_size: i64::try_from(df.file_size_in_bytes())
                .unwrap_or(i64::MAX),
            record_count: Some(
                i64::try_from(df.record_count()).unwrap_or(i64::MAX),
            ),
            referenced_data_file: df.referenced_data_file(),
            file_format: DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
        },
        DataFileFormat::Puffin => {
            let referenced = df.referenced_data_file().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} in snapshot {snapshot_id} missing referenced_data_file",
                    df.file_path()
                ))
            })?;
            let offset = df.content_offset().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} in snapshot {snapshot_id} missing content_offset",
                    df.file_path()
                ))
            })?;
            let length = df.content_size_in_bytes().ok_or_else(|| {
                ChangeError::InternalInconsistency(format!(
                    "Puffin DV {} in snapshot {snapshot_id} missing content_size_in_bytes",
                    df.file_path()
                ))
            })?;
            PositionDeleteRef {
                delete_file_path: df.file_path().to_string(),
                delete_file_size: i64::try_from(df.file_size_in_bytes())
                    .unwrap_or(i64::MAX),
                record_count: Some(
                    i64::try_from(df.record_count()).unwrap_or(i64::MAX),
                ),
                referenced_data_file: Some(referenced),
                file_format: DataFileFormat::Puffin,
                content_offset: Some(offset),
                content_size_in_bytes: Some(length),
            }
        }
        other => {
            return Err(ChangeError::InternalInconsistency(format!(
                "delete manifest in snapshot {snapshot_id} has unsupported file_format {:?}: {}",
                other,
                df.file_path()
            )));
        }
    };
    r.validate_invariants()?;
    deletes.push(r);
}
```

- [ ] **Step 8: Improve OVERWRITE error message**

In `src/connector/iceberg/changes.rs`, find the Display arm for `UnsupportedOperation` (around line 65):

```rust
ChangeError::UnsupportedOperation { snapshot_id, op } => write!(
    f,
    "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
),
```

Replace with:

```rust
ChangeError::UnsupportedOperation { snapshot_id, op } => {
    if op == "overwrite" {
        write!(
            f,
            "iceberg snapshot {snapshot_id} is an INSERT OVERWRITE; IVM cannot bridge across an overwrite snapshot. \
             Either rewrite the workload as DELETE + INSERT, or DROP and re-CREATE the materialized view to reset its lineage."
        )
    } else {
        write!(
            f,
            "iceberg snapshot {snapshot_id} has unsupported operation `{op}`"
        )
    }
}
```

- [ ] **Step 9: Run focused tests**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::changes -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_ddl -- --nocapture
```

Expected: all PASS. The OVERWRITE Display change is exercised by the existing OVERWRITE error tests; if there isn't one yet, no test breaks (Display is opaque). The v3 acceptance test in Step 1 is now passing.

- [ ] **Step 10: Run full lib test to confirm no regressions**

Run:

```bash
cargo build -p novarocks --lib
```

Expected: PASS — no compilation errors from the new dispatch.

- [ ] **Step 11: Commit**

```bash
git add src/connector/iceberg/changes.rs src/connector/starrocks/managed/mv_ddl.rs
git commit -m "$(cat <<'EOF'
feat: classify v3 deletion-vector entries in plan_changes

collect_files::CollectDeletes now dispatches on DataFile::file_format:
Parquet entries continue to feed the v2 position-delete path; Puffin
entries pull referenced_data_file / content_offset /
content_size_in_bytes off the manifest entry into the new fields on
PositionDeleteRef. validate_ivm_primary_key accepts both
format-version 2 and 3 so PRIMARY KEY MVs can sit on top of v3
row-lineage base tables. The OVERWRITE rejection error now explains
the user remediation (DELETE+INSERT or DROP+CREATE). Puffin reverse
projection in scan_deletes lands in the next commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add Puffin DV reverse-projection path in `scan_deletes`

**Files:**
- Modify: `src/connector/iceberg/scan_deletes.rs`
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/iceberg/commit/mod.rs` (re-export `read_deletion_vector_puffin` if not already)

- [ ] **Step 1: Verify `read_deletion_vector_puffin` is reachable from `scan_deletes`**

Run:

```bash
grep -n "pub use puffin_dv::" src/connector/iceberg/commit/mod.rs
grep -n "pub async fn read_deletion_vector_puffin" src/connector/iceberg/commit/puffin_dv.rs
```

Expected: `read_deletion_vector_puffin` is `pub async fn`. The `commit/mod.rs` only re-exports `DeletionVector`. The function is reachable as `crate::connector::iceberg::commit::puffin_dv::read_deletion_vector_puffin` even without a re-export because `puffin_dv` is a `mod` (private) — wait, check.

```bash
grep -n "mod puffin_dv\|pub mod puffin_dv" src/connector/iceberg/commit/mod.rs
```

If it's `mod puffin_dv;` (private), make it accessible by changing the re-export in `src/connector/iceberg/commit/mod.rs`:

```rust
// before
pub use puffin_dv::DeletionVector;

// after
pub use puffin_dv::{DeletionVector, read_deletion_vector_puffin};
```

- [ ] **Step 2: Write failing test for Puffin DV position read**

In `src/connector/iceberg/scan_deletes.rs` `mod tests`, add at the top of the existing test imports:

```rust
use crate::connector::iceberg::commit::{
    DeletionVector, read_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
```

(If `write_single_deletion_vector_puffin` is not already re-exported in `commit/mod.rs`, add it there: `pub use puffin_dv::{DeletionVector, read_deletion_vector_puffin, write_single_deletion_vector_puffin};`)

Add helper for building a real Puffin DV file in tempdir:

```rust
async fn write_puffin_dv_file(
    dir: &std::path::Path,
    name: &str,
    referenced_data_file: &str,
    positions: &[u64],
) -> crate::connector::iceberg::commit::WrittenPuffinDv {
    use iceberg::io::FileIOBuilder;
    let path = format!("{}/{}", dir.display(), name);
    let file_io = FileIOBuilder::new_fs_io().build().expect("file_io");
    let mut dv = DeletionVector::new();
    for p in positions {
        dv.insert(*p).unwrap();
    }
    write_single_deletion_vector_puffin(&file_io, &path, referenced_data_file, &dv)
        .await
        .expect("write puffin dv")
}
```

Wait, `WrittenPuffinDv` may not be re-exported. Add to `commit/mod.rs`:

```rust
pub use puffin_dv::{
    DeletionVector, WrittenPuffinDv, read_deletion_vector_puffin,
    write_single_deletion_vector_puffin,
};
```

Then add the test:

```rust
#[tokio::test]
async fn dv_path_reads_positions_from_puffin_file() {
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::DataFileFormat;
    use roaring::RoaringTreemap;

    let dir = tempfile::tempdir().expect("tempdir");
    let data_file = format!("file://{}/data.parquet", dir.path().display());
    let written = write_puffin_dv_file(dir.path(), "dv-1.puffin", &data_file, &[1, 3, 5]).await;
    let file_io = FileIOBuilder::new_fs_io().build().expect("file_io");
    let refs = vec![PositionDeleteRef {
        delete_file_path: written.path.clone(),
        delete_file_size: written.file_size_in_bytes as i64,
        record_count: Some(written.cardinality as i64),
        referenced_data_file: Some(data_file.clone()),
        file_format: DataFileFormat::Puffin,
        content_offset: Some(written.content_offset),
        content_size_in_bytes: Some(written.content_size_in_bytes),
    }];
    let map = read_dv_positions_per_data_file(&refs, &file_io)
        .await
        .expect("read DV positions");
    assert_eq!(map.len(), 1);
    let positions = &map[&data_file];
    let mut expected = RoaringTreemap::new();
    expected.insert(1);
    expected.insert(3);
    expected.insert(5);
    assert_eq!(positions, &expected);
}
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::scan_deletes::tests::dv_path_reads_positions_from_puffin_file -- --nocapture
```

Expected: FAIL — `read_dv_positions_per_data_file` is not defined.

- [ ] **Step 4: Implement `read_dv_positions_per_data_file`**

In `src/connector/iceberg/scan_deletes.rs`, after the existing `read_delete_positions_per_data_file` function, add:

```rust
/// v3 deletion-vector counterpart of `read_delete_positions_per_data_file`.
/// Reads each Puffin `deletion-vector-v1` blob and folds its positions into
/// the per-data-file `RoaringTreemap`.
///
/// Caller must guarantee every entry in `delete_files` has
/// `file_format == Puffin` and the DV-specific fields populated; mixed-format
/// input must be split by the caller (`scan_deletes` handles this).
pub(crate) async fn read_dv_positions_per_data_file(
    delete_files: &[PositionDeleteRef],
    file_io: &iceberg::io::FileIO,
) -> Result<HashMap<String, RoaringTreemap>, ChangeError> {
    use crate::connector::iceberg::commit::read_deletion_vector_puffin;
    use iceberg::spec::DataFileFormat;

    let mut out: HashMap<String, RoaringTreemap> = HashMap::new();
    for r in delete_files {
        if r.file_format != DataFileFormat::Puffin {
            return Err(ChangeError::InternalInconsistency(format!(
                "read_dv_positions_per_data_file received non-Puffin entry: {}",
                r.delete_file_path
            )));
        }
        r.validate_invariants()?;
        let referenced = r.referenced_data_file.as_ref().ok_or_else(|| {
            ChangeError::InternalInconsistency(format!(
                "Puffin DV {} missing referenced_data_file after invariant check",
                r.delete_file_path
            ))
        })?;
        let offset = r.content_offset.expect("invariant-checked");
        let length = r.content_size_in_bytes.expect("invariant-checked");
        let dv = read_deletion_vector_puffin(file_io, &r.delete_file_path, offset, length)
            .await
            .map_err(|e| {
                ChangeError::InternalInconsistency(format!(
                    "read Puffin DV {}: {e}",
                    r.delete_file_path
                ))
            })?;
        let treemap = dv.to_roaring_treemap();
        out.entry(referenced.clone())
            .or_insert_with(RoaringTreemap::new)
            .extend(&treemap);
    }
    Ok(out)
}
```

(Note: `RoaringTreemap::extend` from `&RoaringTreemap` may not exist; use `|=` operator instead. If `extend` is missing, replace `out.entry(...).or_insert_with(RoaringTreemap::new).extend(&treemap);` with `*out.entry(referenced.clone()).or_insert_with(RoaringTreemap::new) |= treemap;`)

- [ ] **Step 5: Run test and verify it passes**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::scan_deletes::tests::dv_path_reads_positions_from_puffin_file -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Update `scan_deletes` top-level signature to take FileIO + dispatch**

Replace the existing `scan_deletes` function with:

```rust
/// Top-level: take a slice of `PositionDeleteRef`s (mixed v2 Parquet and
/// v3 Puffin DV) and produce `Vec<RecordBatch>` containing the original
/// deleted base rows.
///
/// Internal flow:
/// 1. Partition by `file_format`.
/// 2. v2 Parquet entries: read positions via `read_delete_positions_per_data_file`.
/// 3. v3 Puffin entries: read positions via `read_dv_positions_per_data_file`.
/// 4. Merge per-data-file position sets.
/// 5. For each data file, project rows at the union position set via
///    `read_data_file_at_positions` (works on raw parquet, format-agnostic).
pub(crate) fn scan_deletes<F>(
    delete_files: &[PositionDeleteRef],
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    file_io: &iceberg::io::FileIO,
    data_file_size_lookup: F,
) -> Result<Vec<RecordBatch>, ChangeError>
where
    F: Fn(&str) -> Option<u64>,
{
    use iceberg::spec::DataFileFormat;
    use crate::connector::iceberg::catalog::registry::block_on_iceberg;

    if delete_files.is_empty() {
        return Ok(Vec::new());
    }

    let (parquet_dels, puffin_dels): (Vec<_>, Vec<_>) = delete_files
        .iter()
        .cloned()
        .partition(|r| r.file_format == DataFileFormat::Parquet);

    let mut positions_per_file = read_delete_positions_per_data_file(&parquet_dels, factory)?;
    if !puffin_dels.is_empty() {
        let dv_positions =
            block_on_iceberg(read_dv_positions_per_data_file(&puffin_dels, file_io))
                .map_err(|e| ChangeError::InternalInconsistency(format!(
                    "scan_deletes: block_on_iceberg for Puffin DV: {e}"
                )))??;
        for (path, treemap) in dv_positions {
            *positions_per_file
                .entry(path)
                .or_insert_with(RoaringTreemap::new) |= treemap;
        }
    }

    let mut out: Vec<RecordBatch> = Vec::new();
    let mut data_file_paths: Vec<&String> = positions_per_file.keys().collect();
    data_file_paths.sort();
    for data_file_path in data_file_paths {
        let positions = &positions_per_file[data_file_path];
        let size = data_file_size_lookup(data_file_path);
        let batches = read_data_file_at_positions(data_file_path, size, positions, factory)?;
        out.extend(batches);
    }
    Ok(out)
}
```

- [ ] **Step 7: Update `materialize_changes` in `changes.rs` to pass FileIO**

In `src/connector/iceberg/changes.rs`, find the `scan_deletes` call inside `materialize_changes` (around line 485):

```rust
let deleted_rows = crate::connector::iceberg::scan_deletes::scan_deletes(
    &batch.deletes,
    &factory,
    size_lookup,
)
.map_err(|e| e.to_string())?;
```

Replace with:

```rust
let deleted_rows = crate::connector::iceberg::scan_deletes::scan_deletes(
    &batch.deletes,
    &factory,
    base_table.file_io(),
    size_lookup,
)
.map_err(|e| e.to_string())?;
```

- [ ] **Step 8: Update existing scan_deletes test calls to pass FileIO**

In `src/connector/iceberg/scan_deletes.rs` `mod tests`, the existing tests `scan_deletes_returns_empty_for_empty_input`, `scan_deletes_projects_rows_for_single_data_file`, `scan_deletes_projects_across_multiple_data_files` all call `scan_deletes(...)` with three positional args. Each call site needs a `file_io` argument inserted between the factory and the size_lookup closure.

Add a helper at the top of the test module:

```rust
fn dummy_file_io() -> iceberg::io::FileIO {
    iceberg::io::FileIOBuilder::new_fs_io()
        .build()
        .expect("dummy file_io")
}
```

Then update each `scan_deletes(...)` call. For example:

```rust
// before
let batches = scan_deletes(&[], &factory_for_dir(dir.path()), |_| None).expect("ok");
// after
let batches = scan_deletes(&[], &factory_for_dir(dir.path()), &dummy_file_io(), |_| None).expect("ok");
```

Repeat for the other two test calls.

- [ ] **Step 9: Add a mixed-format integration test in scan_deletes**

In `src/connector/iceberg/scan_deletes.rs` `mod tests`, add:

```rust
#[tokio::test]
async fn scan_deletes_merges_v2_parquet_and_v3_puffin_against_same_data_file() {
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::DataFileFormat;

    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("data.parquet");
    write_data_parquet(&data_path, &[10, 20, 30, 40, 50], &["a", "b", "c", "d", "e"]);
    let data_uri = "data.parquet";

    let v2_delete_path = dir.path().join("v2-deletes.parquet");
    write_delete_parquet(&v2_delete_path, &[data_uri], &[1]); // delete pos 1 -> id=20

    let dv_written = write_puffin_dv_file(
        dir.path(),
        "dv.puffin",
        data_uri,
        &[3], // delete pos 3 -> id=40
    )
    .await;

    let refs = vec![
        PositionDeleteRef {
            delete_file_path: "v2-deletes.parquet".to_string(),
            delete_file_size: 0,
            record_count: Some(1),
            referenced_data_file: Some(data_uri.to_string()),
            file_format: DataFileFormat::Parquet,
            content_offset: None,
            content_size_in_bytes: None,
        },
        PositionDeleteRef {
            delete_file_path: dv_written.path.clone(),
            delete_file_size: dv_written.file_size_in_bytes as i64,
            record_count: Some(dv_written.cardinality as i64),
            referenced_data_file: Some(data_uri.to_string()),
            file_format: DataFileFormat::Puffin,
            content_offset: Some(dv_written.content_offset),
            content_size_in_bytes: Some(dv_written.content_size_in_bytes),
        },
    ];

    let factory = factory_for_dir(dir.path());
    let file_io = FileIOBuilder::new_fs_io().build().expect("file_io");
    let batches = scan_deletes(&refs, &factory, &file_io, |_| None).expect("scan_deletes ok");
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "merged v2 + DV must yield exactly 2 deleted rows");
    let mut all_ids: Vec<i32> = Vec::new();
    for b in &batches {
        let id = b.column(0).as_any().downcast_ref::<Int32Array>().expect("id col");
        for i in 0..id.len() {
            all_ids.push(id.value(i));
        }
    }
    all_ids.sort();
    assert_eq!(all_ids, vec![20, 40]);
}
```

- [ ] **Step 10: Run scan_deletes tests**

Run:

```bash
cargo test -p novarocks --lib connector::iceberg::scan_deletes -- --nocapture
```

Expected: PASS — all existing v2 tests + 2 new Puffin tests + 1 mixed-format test green.

- [ ] **Step 11: Run full library compile + IVM Phase-2 tests as regression check**

Run:

```bash
cargo build -p novarocks --lib
cargo test -p novarocks --lib connector::iceberg::changes -- --nocapture
cargo test -p novarocks --lib connector::iceberg::commit -- --nocapture
cargo test -p novarocks --lib starrocks::managed::mv_refresh -- --nocapture
```

Expected: all PASS. (Note: `starrocks::managed::mv_refresh::aggregate_mv_incremental_refresh_handles_base_delete` may be skipped if minio is unreachable; that's acceptable.)

- [ ] **Step 12: Commit**

```bash
git add src/connector/iceberg/commit/mod.rs src/connector/iceberg/scan_deletes.rs src/connector/iceberg/changes.rs
git commit -m "$(cat <<'EOF'
feat: read positions from puffin deletion vectors in scan_deletes

scan_deletes now partitions PositionDeleteRef inputs by file_format:
v2 Parquet entries continue through read_delete_positions_per_data_file;
new v3 Puffin entries route through read_dv_positions_per_data_file
which calls commit::puffin_dv::read_deletion_vector_puffin and folds
the decoded positions into the same RoaringTreemap-keyed map. The
downstream `read_data_file_at_positions` is reused unchanged because
it operates on raw parquet row offsets independent of the
delete-file format. materialize_changes now passes the base table's
FileIO through to scan_deletes so the Puffin reader has a handle.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: End-to-end integration test on v3 row-lineage base + Puffin DV

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`

- [ ] **Step 1: Locate the existing reference test**

Run:

```bash
grep -n "fn aggregate_mv_incremental_refresh_handles_base_delete" src/connector/starrocks/managed/mv_refresh.rs
```

Expected: one match around line 1069. This is our reference pattern.

- [ ] **Step 2: Add the row-lineage variant test**

In `src/connector/starrocks/managed/mv_refresh.rs` `mod tests`, immediately after `aggregate_mv_incremental_refresh_handles_base_delete`, add:

```rust
#[test]
fn aggregate_mv_incremental_refresh_handles_v3_row_lineage_base_delete() {
    // End-to-end variant of `aggregate_mv_incremental_refresh_handles_base_delete`
    // that puts the base on Iceberg v3 with `write.row-lineage=true`. The DELETE
    // therefore writes a Puffin deletion-vector file (Phase 2a RowDeltaDvCommit)
    // instead of a v2 position-delete Parquet. The IVM incremental refresh path
    // exercised here is `plan_changes` -> `materialize_changes` -> `scan_deletes`,
    // with scan_deletes routing the Puffin DV through the new
    // `read_dv_positions_per_data_file` helper.
    //
    // Skipped when the minio object-store endpoint is unreachable, matching
    // `aggregate_mv_incremental_refresh_handles_base_delete`.
    let _runtime_guard = lock_runtime_test_state();
    let Some((_config_dir, config_path)) = maybe_managed_lake_config_path() else {
        return;
    };
    let iceberg_dir = tempfile::tempdir().expect("iceberg warehouse tempdir");
    let iceberg_warehouse = format!("file://{}", iceberg_dir.path().display());

    let engine = match crate::engine::StandaloneNovaRocks::open(
        crate::engine::StandaloneOptions {
            config_path: Some(config_path),
            metadata_db_path: None,
        },
    ) {
        Ok(engine) => engine,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable: {err}"
                );
                return;
            }
            panic!("open standalone engine: {err}");
        }
    };
    let session = engine.session();
    let create_catalog_sql = format!(
        r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{iceberg_warehouse}")"#
    );
    session
        .execute_in_database(&create_catalog_sql, "default")
        .expect("create iceberg catalog");
    session
        .execute_in_database("create database ice.ns", "default")
        .expect("create iceberg namespace");
    // The only delta vs `_handles_base_delete`: tblproperties enables row-lineage.
    session
        .execute_in_database(
            r#"create table ice.ns.orders (id bigint not null, customer string, amount bigint) tblproperties("format-version"="3","write.row-lineage"="true")"#,
            "default",
        )
        .expect("create row-lineage iceberg orders table");
    session
        .execute_in_database(
            "insert into ice.ns.orders values (1, 'A', 10), (2, 'A', 20), (3, 'B', 30)",
            "default",
        )
        .expect("seed iceberg base rows");

    session
        .execute_in_database("create database analytics", "default")
        .expect("create analytics database");
    if let Err(err) = session.execute_in_database(
        "create materialized view agg_mv \
         distributed by hash(customer) buckets 2 \
         as select customer, count(*) as c, sum(amount) as s \
         from ice.ns.orders group by customer",
        "analytics",
    ) {
        if is_unavailable_object_store_error(&err) {
            eprintln!(
                "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on create: {err}"
            );
            return;
        }
        panic!("create materialized view: {err}");
    }

    if let Err(err) =
        session.execute_in_database("refresh materialized view agg_mv", "analytics")
    {
        if is_unavailable_object_store_error(&err) {
            eprintln!(
                "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on full refresh: {err}"
            );
            return;
        }
        panic!("first (full) refresh materialized view: {err}");
    }

    let pre_state = match collect_agg_mv_state(&session) {
        Ok(rows) => rows,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on pre-delete select: {err}"
                );
                return;
            }
            panic!("select pre-delete agg_mv state: {err}");
        }
    };
    assert_eq!(
        pre_state,
        vec![
            ("A".to_string(), 2_i64, 30_i64),
            ("B".to_string(), 1_i64, 30_i64),
        ],
        "row-lineage MV state after full refresh must reflect the 3 seeded base rows"
    );

    // Trigger a DELETE — this writes a Puffin DV (Phase 2a RowDeltaDvCommit).
    session
        .execute_in_database("delete from ice.ns.orders where id = 1", "default")
        .expect("delete base row id=1 (writes puffin dv)");

    if let Err(err) =
        session.execute_in_database("refresh materialized view agg_mv", "analytics")
    {
        if is_unavailable_object_store_error(&err) {
            eprintln!(
                "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on incremental refresh: {err}"
            );
            return;
        }
        panic!("second (incremental, DV-bearing) refresh materialized view: {err}");
    }

    let post_state = match collect_agg_mv_state(&session) {
        Ok(rows) => rows,
        Err(err) => {
            if is_unavailable_object_store_error(&err) {
                eprintln!(
                    "skipping v3 row-lineage MV incremental DELETE test: object store unavailable on post-delete select: {err}"
                );
                return;
            }
            panic!("select post-delete agg_mv state: {err}");
        }
    };
    assert_eq!(
        post_state,
        vec![
            ("A".to_string(), 1_i64, 20_i64),
            ("B".to_string(), 1_i64, 30_i64),
        ],
        "row-lineage MV state after DV-bearing incremental refresh must drop \
         count and sum for the affected group; other groups untouched"
    );

    drop(engine);
}
```

- [ ] **Step 3: Run the new integration test in isolation**

Run:

```bash
cargo test -p novarocks --lib starrocks::managed::mv_refresh::tests::aggregate_mv_incremental_refresh_handles_v3_row_lineage_base_delete -- --nocapture
```

Expected: PASS, OR cleanly skipped with "object store unavailable" message if minio not running.

If FAIL with anything other than the unavailable-object-store skip, investigate. Common failure modes and remedies:
- "DeletionVectorUnsupported" → Task 2 didn't land; rerun the file_format dispatch step.
- "Failed to load Parquet metadata" → Task 3 didn't route Puffin properly; check `partition` and the `file_format == Puffin` branch.
- "lineage broken" → `RowDeltaDvCommit` snapshots are misclassified; verify `Operation::Delete` (not Overwrite) is set in `commit/row_delta_dv.rs`.

- [ ] **Step 4: Run full focused regression**

Run:

```bash
cargo fmt --check
cargo build -p novarocks --lib
cargo test -p novarocks --lib connector::iceberg -- --nocapture
cargo test -p novarocks --lib starrocks::managed -- --nocapture
cargo test -p novarocks --lib engine::tests::iceberg_ -- --nocapture
```

Expected: all PASS. Phase 2a tests (commit/iceberg_/etc.) keep passing. Phase 2 IVM tests keep passing. New row-lineage MV test passes (or skips on missing minio).

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/mv_refresh.rs
git commit -m "$(cat <<'EOF'
test: cover v3 row-lineage mv incremental refresh end-to-end

Mirror of aggregate_mv_incremental_refresh_handles_base_delete with
the only delta being write.row-lineage=true on the base. The DELETE
writes a Puffin deletion-vector instead of a v2 position-delete
Parquet, exercising classify_lineage's CollectDeletes -> Puffin
dispatch and scan_deletes -> read_dv_positions_per_data_file.
Asserts the materialized view ends in the same logical state as the
v2 path: count and sum decrease for the affected aggregation group,
other groups untouched.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final Review Checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo build -p novarocks` passes
- [ ] `cargo test -p novarocks --lib connector::iceberg::commit::puffin_dv` passes (includes new `to_roaring_treemap` tests)
- [ ] `cargo test -p novarocks --lib connector::iceberg::changes` passes (includes new invariant tests)
- [ ] `cargo test -p novarocks --lib connector::iceberg::scan_deletes` passes (includes new Puffin DV tests + mixed-format test)
- [ ] `cargo test -p novarocks --lib starrocks::managed::mv_ddl::tests::validate_ivm_primary_key` passes for v2 and v3
- [ ] `cargo test -p novarocks --lib engine::tests::iceberg_` passes (Phase 2a regression)
- [ ] `cargo test -p novarocks --lib starrocks::managed::mv_refresh -- --nocapture` either passes or skips cleanly when minio is unavailable
- [ ] `PositionDeleteRef::validate_invariants` is called at every place that constructs a Puffin variant (Task 2 step 7 already does this)
- [ ] No references to `ChangeError::DeletionVectorUnsupported` are constructed (the variant remains in the enum as `#[allow(dead_code)]`)
- [ ] OVERWRITE error message mentions "DELETE + INSERT" remediation
- [ ] All four commits are present and atomic; each compiles and tests independently
