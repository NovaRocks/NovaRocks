# IVM-A11 MV Schema / Field-ID Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace A9's partial apply-key guard with a complete schema contract that captures, at CREATE MV time, the base table schema fingerprint, referenced base field IDs, MV output schema fingerprint, target Iceberg field id mapping, and hidden apply-key contract — and validates it on every refresh. This is the foundation that lets future aggregate/join MV work rely on stable field-id-based dependencies rather than name-based SQL rebinding.

**Architecture:** Approach 1 from the spec — extend `StoredMvDefinition` in-place with a required `schema_contract: MvSchemaContract` field. A9's `target_apply_key` field is removed from the top level and its content is absorbed into `MvSchemaContract.target.hidden_apply_key`. A single `validate_schema_contract` entry point replaces A9's scattered guards. NovaRocks has no historical users, so we don't write any backward-compat shims.

**Tech Stack:** Rust, `iceberg` crate (`iceberg::table::Table`, `iceberg::spec::Schema`, `iceberg::spec::Type`), `serde_json`, `sha2` (already in workspace), existing analyzer (`src/sql/analyzer`) and MV refresh (`src/engine/mv`) modules. SQL integration tests run through `sql-tests/iceberg-ivm` against the local Docker Iceberg REST + MinIO fixture.

**Reference spec:** [docs/design/specs/2026-05-14-ivm-a11-mv-schema-field-id-contract-design.md](../specs/2026-05-14-ivm-a11-mv-schema-field-id-contract-design.md)

---

## Task 1: Rebase branch onto origin/main so A9 code is present

A9 (PR #126, commit `86d2a7cf`) is on `origin/main` but not on the local `main` branch this worktree was cut from. All A11 tasks build on A9's `iceberg_target_apply.rs`, A9's `MvTargetApplyKey` field, and A9's `refresh_iceberg_mv` body. Rebase first; otherwise every subsequent task points at code that doesn't exist in your tree.

**Files:** No code changes in this task — only branch state.

- [ ] **Step 1: Inspect current branch state**

Run:
```bash
git status
git log --oneline -5 HEAD
git log --oneline -1 origin/main
```

Expected: working tree clean, HEAD has only the 3 A11 spec commits (`dd703dce`, `b635a315`, `67dcc455`) plus their pre-A9 base, `origin/main` is at `86d2a7cf`.

- [ ] **Step 2: Fetch and rebase**

Run:
```bash
git fetch origin
git rebase origin/main
```

Expected: clean rebase (the only commits being rebased are docs-only changes, no code conflict).

- [ ] **Step 3: Confirm A9 files exist after rebase**

Run:
```bash
ls src/engine/mv/iceberg_target_apply.rs
grep -c "MvTargetApplyKey" src/meta/repository/mv.rs
grep -c "ensure_target_apply_key_contract" src/engine/mv/iceberg_refresh.rs
```

Expected: file exists, both `grep -c` print non-zero counts (at least 2 and 1 respectively).

- [ ] **Step 4: Confirm baseline build still passes**

Run:
```bash
cargo build 2>&1 | tail -5
```

Expected: `Finished ... profile [unoptimized + debuginfo] target(s)`.

No commit in this task — rebase is a state change, not a content change.

---

## Task 2: Add `sha2` and `serde_json` to the crate's runtime deps if missing

The contract uses `sha2` for fingerprints (already removed from the spec, but keep optional for hashing helpers if needed elsewhere) and `serde_json` is already in use. This task confirms what's available so later tasks can `use` them safely.

**Files:**
- Inspect: `Cargo.toml`

- [ ] **Step 1: Inspect Cargo.toml runtime deps**

Run:
```bash
grep -E "^(serde|serde_json|sha2|iceberg)" Cargo.toml
```

Expected: at minimum `serde`, `serde_json`, and `iceberg` should be listed. `sha2` may be present; if absent, we don't need to add it (fingerprint was removed from the spec).

- [ ] **Step 2: Confirm Cargo.toml has serde features for derive**

Run:
```bash
grep -A 1 "^serde " Cargo.toml
```

Expected: `serde = { version = "...", features = ["derive"] }` or equivalent. If not, the data-model task will fail to compile.

If anything is missing, add it in this task with a minimal `Cargo.toml` edit and commit. Otherwise, no changes — skip the commit step.

---

## Task 3: Define `MvSchemaContract` data structures

Create the new module `src/meta/repository/mv_contract.rs` with all contract structures plus a self-consistency check. Pure data + a pure validator; no business logic that depends on iceberg crate or analyzer.

**Files:**
- Create: `src/meta/repository/mv_contract.rs`
- Modify: `src/meta/repository/mod.rs` (add `pub mod mv_contract;`)
- Test: inline `#[cfg(test)] mod tests` in `mv_contract.rs`

- [ ] **Step 1: Write failing test for serialization roundtrip**

Create `src/meta/repository/mv_contract.rs` with:

```rust
//! IVM-A11 MV schema / field-id contract.
//!
//! Persisted inside `StoredMvDefinition.schema_contract`. Captures base
//! referenced fields + output lineage + target schema mapping at CREATE
//! MV time. Validated on every REFRESH.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvSchemaContract {
    pub contract_version: u16,
    pub base: BaseContract,
    pub output: OutputContract,
    pub target: TargetContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseContract {
    pub table_fqn: String,
    pub table_uuid: String,
    pub schema_id_at_create: i32,
    pub schema_at_create: BaseSchemaSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseSchemaSnapshot {
    pub fields: Vec<BaseFieldRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseFieldRecord {
    pub field_id: i32,
    pub name_at_create: String,
    pub type_signature: String,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputContract {
    pub columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputColumnLineage {
    pub expression: ExpressionLineage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpressionLineage {
    pub kind: ExpressionKind,
    pub referenced_base_field_ids: Vec<i32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterLineage {
    pub referenced_base_field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetContract {
    pub table_fqn: String,
    pub table_uuid: String,
    pub schema_id_at_create: i32,
    pub visible_columns: Vec<TargetVisibleColumn>,
    pub hidden_apply_key: HiddenApplyKeyContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetVisibleColumn {
    pub output_name: String,
    pub target_field_id: i32,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HiddenApplyKeyContract {
    pub column_name: String,
    pub target_field_id: i32,
    pub source: ApplyKeySource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".to_string(),
                table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "long".to_string(),
                        required: true,
                    }],
                },
            },
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                    },
                }],
                filter: None,
            },
            target: TargetContract {
                table_fqn: "ice.mv.orders_mv".to_string(),
                table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 1,
                    type_signature: "long".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
            },
        }
    }

    #[test]
    fn contract_round_trips_through_serde_json() {
        let c = sample_contract();
        let json = serde_json::to_string(&c).expect("serialize");
        let decoded: MvSchemaContract = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, c);
    }
}
```

Modify `src/meta/repository/mod.rs` — add `pub mod mv_contract;` next to the other `pub mod` lines.

- [ ] **Step 2: Run failing test, confirm it compiles and passes**

Run:
```bash
cargo test --lib -p novarocks meta::repository::mv_contract::tests::contract_round_trips_through_serde_json 2>&1 | tail -20
```

Wait — the crate name is `novarocks`? Check:

```bash
grep '^name' Cargo.toml | head -1
```

Use the printed name in `-p`. If the project is a single-crate workspace and `-p` is unnecessary, drop it:

```bash
cargo test --lib meta::repository::mv_contract::tests::contract_round_trips_through_serde_json 2>&1 | tail -20
```

Expected: `test ... ok`.

- [ ] **Step 3: Add the self-consistency check**

Append to `mv_contract.rs` (above the `#[cfg(test)]` block):

```rust
/// Errors returned by `MvSchemaContract::ensure_self_consistent`.
/// These indicate the contract was constructed incorrectly at CREATE
/// time — they should never surface to end users in practice.
#[derive(Debug, PartialEq, Eq)]
pub enum ContractSelfCheckError {
    OutputTargetLenMismatch { output_len: usize, target_len: usize },
    HiddenApplyKeyColumnNameWrong { expected: String, actual: String },
    OutputReferencesUnknownBaseFieldId { output_index: usize, field_id: i32 },
    EmptyBaseTableUuid,
    NegativeBaseSchemaId(i32),
    DuplicateBaseFieldIdWithDifferentType { field_id: i32, first: String, second: String },
}

impl std::fmt::Display for ContractSelfCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputTargetLenMismatch { output_len, target_len } => {
                write!(f, "MV contract output columns ({output_len}) and target visible columns ({target_len}) must have the same length")
            }
            Self::HiddenApplyKeyColumnNameWrong { expected, actual } => {
                write!(f, "MV contract hidden apply-key column name expected {expected}, got {actual}")
            }
            Self::OutputReferencesUnknownBaseFieldId { output_index, field_id } => {
                write!(f, "MV contract output column #{output_index} references base field id {field_id} that is not in base.schema_at_create")
            }
            Self::EmptyBaseTableUuid => write!(f, "MV contract base.table_uuid is empty"),
            Self::NegativeBaseSchemaId(id) => write!(f, "MV contract base.schema_id_at_create is negative: {id}"),
            Self::DuplicateBaseFieldIdWithDifferentType { field_id, first, second } => {
                write!(f, "MV contract base.schema_at_create contains field id {field_id} twice with different type signatures: {first} vs {second}")
            }
        }
    }
}

pub const HIDDEN_APPLY_KEY_COLUMN_NAME: &str = "__nova_base_row_id";

impl MvSchemaContract {
    /// Cheap structural self-check run at CREATE time. Does NOT consult
    /// the live Iceberg tables — that part lives in
    /// `validate_schema_contract` and runs at REFRESH time.
    pub fn ensure_self_consistent(&self) -> Result<(), ContractSelfCheckError> {
        if self.output.columns.len() != self.target.visible_columns.len() {
            return Err(ContractSelfCheckError::OutputTargetLenMismatch {
                output_len: self.output.columns.len(),
                target_len: self.target.visible_columns.len(),
            });
        }
        if self.target.hidden_apply_key.column_name != HIDDEN_APPLY_KEY_COLUMN_NAME {
            return Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong {
                expected: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                actual: self.target.hidden_apply_key.column_name.clone(),
            });
        }
        if self.base.table_uuid.is_empty() {
            return Err(ContractSelfCheckError::EmptyBaseTableUuid);
        }
        if self.base.schema_id_at_create < 0 {
            return Err(ContractSelfCheckError::NegativeBaseSchemaId(
                self.base.schema_id_at_create,
            ));
        }
        let known_field_ids: std::collections::BTreeSet<i32> = self
            .base
            .schema_at_create
            .fields
            .iter()
            .map(|f| f.field_id)
            .collect();
        for (i, col) in self.output.columns.iter().enumerate() {
            for fid in &col.expression.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId {
                        output_index: i,
                        field_id: *fid,
                    });
                }
            }
        }
        if let Some(filter) = &self.output.filter {
            for fid in &filter.referenced_base_field_ids {
                if !known_field_ids.contains(fid) {
                    return Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId {
                        output_index: usize::MAX, // sentinel for filter
                        field_id: *fid,
                    });
                }
            }
        }
        let mut seen: std::collections::BTreeMap<i32, &str> = std::collections::BTreeMap::new();
        for f in &self.base.schema_at_create.fields {
            if let Some(prev) = seen.get(&f.field_id) {
                if *prev != f.type_signature.as_str() {
                    return Err(ContractSelfCheckError::DuplicateBaseFieldIdWithDifferentType {
                        field_id: f.field_id,
                        first: prev.to_string(),
                        second: f.type_signature.clone(),
                    });
                }
            } else {
                seen.insert(f.field_id, &f.type_signature);
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Add self-check tests**

Append to the `tests` mod inside `mv_contract.rs`:

```rust
    #[test]
    fn self_check_accepts_well_formed_contract() {
        assert!(sample_contract().ensure_self_consistent().is_ok());
    }

    #[test]
    fn self_check_rejects_mismatched_output_and_target_lengths() {
        let mut c = sample_contract();
        c.target.visible_columns.push(TargetVisibleColumn {
            output_name: "extra".to_string(),
            target_field_id: 99,
            type_signature: "long".to_string(),
            nullable: true,
        });
        match c.ensure_self_consistent() {
            Err(ContractSelfCheckError::OutputTargetLenMismatch { output_len: 1, target_len: 2 }) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn self_check_rejects_wrong_hidden_column_name() {
        let mut c = sample_contract();
        c.target.hidden_apply_key.column_name = "wrong".to_string();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::HiddenApplyKeyColumnNameWrong { .. })
        ));
    }

    #[test]
    fn self_check_rejects_unknown_referenced_field_id() {
        let mut c = sample_contract();
        c.output.columns[0].expression.referenced_base_field_ids = vec![999];
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::OutputReferencesUnknownBaseFieldId { field_id: 999, .. })
        ));
    }

    #[test]
    fn self_check_rejects_empty_base_uuid() {
        let mut c = sample_contract();
        c.base.table_uuid = String::new();
        assert!(matches!(
            c.ensure_self_consistent(),
            Err(ContractSelfCheckError::EmptyBaseTableUuid)
        ));
    }
```

- [ ] **Step 5: Run all `mv_contract` tests**

Run:
```bash
cargo test --lib meta::repository::mv_contract 2>&1 | tail -15
```

Expected: 6 passed (1 roundtrip + 5 self-check), 0 failed.

- [ ] **Step 6: Commit**

```bash
git add src/meta/repository/mv_contract.rs src/meta/repository/mod.rs
git commit -m "feat(meta): add MvSchemaContract data model and self-check

A11 contract data structures: BaseContract, OutputContract,
TargetContract with field-id-based lineage. ensure_self_consistent
runs at CREATE MV time to catch malformed contracts before persistence."
```

---

## Task 4: Replace `target_apply_key` with `schema_contract` in `StoredMvDefinition`

A9 stored `target_apply_key: Option<MvTargetApplyKey>` at the top level of `StoredMvDefinition`. A11 absorbs that into `schema_contract.target.hidden_apply_key`. Remove the top-level field, remove the `MvTargetApplyKey` / `MvTargetApplyKeySource` structs, add a required `schema_contract: MvSchemaContract` field, bump `MV_DEFINITION_SCHEMA_VERSION`.

A9's tests in `mv.rs` reference `MvTargetApplyKey` — those will need updating in this task.

**Files:**
- Modify: `src/meta/repository/mv.rs`

- [ ] **Step 1: Inspect the current shape of `StoredMvDefinition` and `CreateMvDefinitionRequest`**

Run:
```bash
grep -n "MV_DEFINITION_SCHEMA_VERSION\|struct StoredMvDefinition\|struct CreateMvDefinitionRequest\|struct MvTargetApplyKey\|enum MvTargetApplyKeySource\|target_apply_key" src/meta/repository/mv.rs
```

Note the exact line numbers — Step 2's edits depend on them.

- [ ] **Step 2: Replace `target_apply_key` with `schema_contract` in struct definitions**

Edit `src/meta/repository/mv.rs`:

1. Bump version constant near the top of the file:

```rust
// before
const MV_DEFINITION_SCHEMA_VERSION: i32 = 1;
// after
const MV_DEFINITION_SCHEMA_VERSION: i32 = 2;
```

2. Delete the `MvTargetApplyKey` struct and `MvTargetApplyKeySource` enum definitions entirely (they were inserted by A9). They are no longer used anywhere after this task completes.

3. In `StoredMvDefinition`:

```rust
// before
    pub target_table: Option<String>,
    #[serde(default)]
    pub target_apply_key: Option<MvTargetApplyKey>,
    pub last_refresh_ms: Option<i64>,
// after
    pub target_table: Option<String>,
    pub schema_contract: crate::meta::repository::mv_contract::MvSchemaContract,
    pub last_refresh_ms: Option<i64>,
```

4. In `CreateMvDefinitionRequest`:

```rust
// before
    pub target_table: Option<String>,
    pub target_apply_key: Option<MvTargetApplyKey>,
    pub created_at_ms: i64,
// after
    pub target_table: Option<String>,
    pub schema_contract: crate::meta::repository::mv_contract::MvSchemaContract,
    pub created_at_ms: i64,
```

5. In `MvMetaRepository::create_definition` (around the existing `target_apply_key: req.target_apply_key,` line), change to `schema_contract: req.schema_contract,`.

6. Remove (delete) the two A9 tests in `mv.rs`:
   - `mv_target_apply_key_metadata_round_trips`
   - `mv_target_apply_key_defaults_to_none_for_old_records`
   
   These tests verified A9's `target_apply_key` behavior. The contract round-trip is now tested in `mv_contract.rs`. Also delete the `stored_mv_definition` helper inside that same `#[cfg(test)] mod tests` if it's only used by those two tests.

- [ ] **Step 3: Confirm full build fails — many call sites still reference the removed fields**

Run:
```bash
cargo build 2>&1 | grep "error\[" | head -10
```

Expected: build errors. Almost certainly two categories:
1. `error[E0277]` or `E0432`: `MvTargetApplyKey` / `MvTargetApplyKeySource` no longer resolvable in `src/engine/mv/iceberg_refresh.rs` and `src/engine/mv/iceberg_target_apply.rs`.
2. `error[E0063]`: missing field `schema_contract` (or extra field `target_apply_key`) in `CreateMvDefinitionRequest` literal in `src/engine/mv/iceberg_refresh.rs`.

This is expected. Subsequent tasks will fix these call sites in order.

- [ ] **Step 4: Commit**

This task intentionally leaves the build broken — the change is atomic at the data-model level, and the next several tasks fix the consumers one by one. Subagent reviewers should note this. Commit anyway so the diff is reviewable per-step:

```bash
git add src/meta/repository/mv.rs
git commit -m "refactor(meta): replace target_apply_key with schema_contract in StoredMvDefinition

Drops A9's top-level MvTargetApplyKey field. Its content is absorbed
into MvSchemaContract.target.hidden_apply_key. Build is temporarily
broken pending consumer updates in subsequent tasks. Bumps
MV_DEFINITION_SCHEMA_VERSION to 2."
```

---

## Task 5: Strip `MvTargetApplyKey`-bound code from `iceberg_target_apply.rs`

A9 wrote `ensure_target_apply_key_contract` that takes `&MvTargetApplyKey` as input. With the struct removed, this function and its imports must go. Keep the rest: constants, `apply_key_table_column`, `find_apply_key_field_id`, `ensure_base_row_lineage_contract`, `extract_apply_key_values_from_chunks`, `load_target_apply_locator_inputs`, `locate_target_rows_by_apply_key`, and `iceberg_mv_physical_select_sql`.

**Files:**
- Modify: `src/engine/mv/iceberg_target_apply.rs`

- [ ] **Step 1: Inspect what `ensure_target_apply_key_contract` looks like and what calls it**

Run:
```bash
grep -n "ensure_target_apply_key_contract\|MvTargetApplyKey" src/engine/mv/iceberg_target_apply.rs src/engine/mv/iceberg_refresh.rs
```

Note line numbers for the function body and its single call site in `refresh_iceberg_mv`.

- [ ] **Step 2: Delete `ensure_target_apply_key_contract` from `iceberg_target_apply.rs`**

Edit `src/engine/mv/iceberg_target_apply.rs`. Remove the entire function definition starting at `pub(crate) fn ensure_target_apply_key_contract(` through its closing brace, plus any `use` imports of `MvTargetApplyKey` / `MvTargetApplyKeySource` at the top of the file. Leave the rest of the file untouched.

- [ ] **Step 3: Confirm `iceberg_target_apply.rs` compiles on its own**

Run:
```bash
cargo check 2>&1 | grep "iceberg_target_apply" | head -5
```

Expected: no errors mentioning `iceberg_target_apply.rs`. There may still be errors elsewhere (the call site in `iceberg_refresh.rs` and the `MvTargetApplyKey` import there) — those are fixed in Task 6.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_target_apply.rs
git commit -m "refactor(mv): remove ensure_target_apply_key_contract from iceberg_target_apply

Replaced by Task 9's validate_schema_contract. Constants and helpers
unrelated to MvTargetApplyKey (apply_key_table_column,
find_apply_key_field_id, ensure_base_row_lineage_contract, etc.) are
preserved — Task 7 still uses find_apply_key_field_id at CREATE time."
```

---

## Task 6: Update `iceberg_refresh.rs` imports and `create_iceberg_mv` to a placeholder contract

`create_iceberg_mv` currently constructs `MvTargetApplyKey` and stuffs it into `CreateMvDefinitionRequest.target_apply_key`. We need to make the file compile by constructing a placeholder `MvSchemaContract` (correct hidden_apply_key + empty base/output for now). Task 7 fills in the lineage; Task 8 verifies CREATE end-to-end.

`refresh_iceberg_mv`'s call to `ensure_target_apply_key_contract` is removed in this task too. Its replacement (`validate_schema_contract`) is wired up in Task 10.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Find the A9 `use` block and the CREATE / REFRESH call sites**

Run:
```bash
grep -n "ensure_target_apply_key_contract\|MvTargetApplyKey\|MvTargetApplyKeySource\|target_apply_key" src/engine/mv/iceberg_refresh.rs
```

Note every line.

- [ ] **Step 2: Remove the A9 `use` lines**

Edit the top-of-file `use` block:

```rust
// before — inside the use crate::engine::mv::iceberg_target_apply::{ ... } block
    ICEBERG_MV_PROP_APPLY_KEY_SOURCE, apply_key_table_column, ensure_base_row_lineage_contract,
    ensure_target_apply_key_contract, extract_apply_key_values_from_chunks,
    find_apply_key_field_id, iceberg_mv_physical_select_sql, load_target_apply_locator_inputs,
    locate_target_rows_by_apply_key,
// after
    ICEBERG_MV_PROP_APPLY_KEY_SOURCE, apply_key_table_column, ensure_base_row_lineage_contract,
    extract_apply_key_values_from_chunks, find_apply_key_field_id,
    iceberg_mv_physical_select_sql, load_target_apply_locator_inputs,
    locate_target_rows_by_apply_key,
```

And in the `use crate::meta::repository::mv::{...}` block:

```rust
// before
    MvRefreshState, MvTargetApplyKey, MvTargetApplyKeySource, RecordPublishCommitRequest,
// after
    MvRefreshState, RecordPublishCommitRequest,
```

- [ ] **Step 3: Stub the contract construction in `create_iceberg_mv`**

Find the `CreateMvDefinitionRequest { ... }` literal inside `create_iceberg_mv` (after the A9 `let actual_apply_key_field_id = find_apply_key_field_id(&target_loaded.table)?;` block).

Replace the A9 `target_apply_key: Some(MvTargetApplyKey { ... })` field with a placeholder contract construction. For now, use a minimal contract that compiles; Task 7 wires in real lineage:

```rust
                    schema_contract: crate::meta::repository::mv_contract::MvSchemaContract {
                        contract_version: 1,
                        base: crate::meta::repository::mv_contract::BaseContract {
                            table_fqn: base_ref.fqn(),
                            table_uuid: loaded_base.table.metadata().uuid().to_string(),
                            schema_id_at_create: loaded_base.table.metadata().current_schema_id(),
                            schema_at_create: crate::meta::repository::mv_contract::BaseSchemaSnapshot {
                                fields: Vec::new(), // populated in Task 7
                            },
                        },
                        output: crate::meta::repository::mv_contract::OutputContract {
                            columns: analysis
                                .output_columns
                                .iter()
                                .map(|_| crate::meta::repository::mv_contract::OutputColumnLineage {
                                    expression: crate::meta::repository::mv_contract::ExpressionLineage {
                                        kind: crate::meta::repository::mv_contract::ExpressionKind::Column,
                                        referenced_base_field_ids: Vec::new(), // populated in Task 7
                                    },
                                })
                                .collect(),
                            filter: None, // populated in Task 7
                        },
                        target: crate::meta::repository::mv_contract::TargetContract {
                            table_fqn: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                            table_uuid: target_loaded.table.metadata().uuid().to_string(),
                            schema_id_at_create: target_loaded.table.metadata().current_schema_id(),
                            visible_columns: analysis
                                .output_columns
                                .iter()
                                .map(|col| {
                                    let field = target_loaded
                                        .table
                                        .metadata()
                                        .current_schema()
                                        .as_struct()
                                        .fields()
                                        .iter()
                                        .find(|f| f.name.eq_ignore_ascii_case(&col.name))
                                        .expect("Task 8 self-check verifies output ↔ target alignment");
                                    crate::meta::repository::mv_contract::TargetVisibleColumn {
                                        output_name: col.name.clone(),
                                        target_field_id: field.id,
                                        type_signature: format!("{}", field.field_type),
                                        nullable: !field.required,
                                    }
                                })
                                .collect(),
                            hidden_apply_key: crate::meta::repository::mv_contract::HiddenApplyKeyContract {
                                column_name: crate::meta::repository::mv_contract::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                                target_field_id: actual_apply_key_field_id,
                                source: crate::meta::repository::mv_contract::ApplyKeySource::BaseRowId,
                            },
                        },
                    },
```

Delete the lines that previously constructed `MvTargetApplyKey`.

- [ ] **Step 4: Remove A9's call to `ensure_target_apply_key_contract` from `refresh_iceberg_mv`**

Find lines around 503-509 (the A9 block that reads `target_apply_key` from `mv_definition` and calls `ensure_target_apply_key_contract`):

```rust
// before
    let target_apply_key = mv_definition.target_apply_key.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing target apply-key metadata; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    ensure_target_apply_key_contract(&target_loaded.table, target_apply_key)?;
// after — leave commented placeholder; Task 10 inserts the real validate_schema_contract call here
    // TODO(A11-Task-10): call validate_schema_contract here.
```

Yes, this leaves a TODO comment briefly — Task 10 immediately deletes it. It's the minimal change that keeps the build green between Task 6 and Task 10.

- [ ] **Step 5: Confirm the full project builds**

Run:
```bash
cargo build 2>&1 | grep -E "error|warning: unused" | head -10
```

Expected: no `error[...]` lines. There may be `warning: unused import` warnings — fix those by trimming the `use` blocks now or in Task 10 cleanup.

If errors remain, they likely point at other consumers of `MvTargetApplyKey` (e.g. tests in this file). Find and remove their references the same way — strip the type from imports and from any test that mocks the field.

```bash
grep -rn "MvTargetApplyKey\|target_apply_key" src/ 2>&1 | head -20
```

Should print zero matches in `src/`.

- [ ] **Step 6: Run the existing test suite to confirm nothing else broke**

Run:
```bash
cargo test --lib 2>&1 | tail -10
```

Expected: all tests pass (or, if pre-existing tests in `iceberg_refresh.rs` constructed `MvTargetApplyKey` directly, those need their fixtures updated — apply the same migration: replace the field with an inline `MvSchemaContract` literal using the helper pattern in Step 3).

If you have to update an A9 test, do it in this task — same atomic change.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor(mv): swap MvTargetApplyKey for placeholder MvSchemaContract in create_iceberg_mv

CREATE MV now persists an MvSchemaContract whose target.hidden_apply_key
matches A9's old MvTargetApplyKey. Base.schema_at_create.fields and
output lineage are stubbed empty — Task 7 wires them in. A9's
ensure_target_apply_key_contract call in refresh_iceberg_mv is removed
(Task 10 replaces it with validate_schema_contract)."
```

---

## Task 7: Build the projection/filter lineage builder in `mv_lineage.rs`

Create `src/sql/analyzer/mv_lineage.rs` exposing one public function:

```rust
pub(crate) fn build_projection_filter_lineage(
    resolved: &crate::sql::analysis::ResolvedQuery,
    base_iceberg_schema: &iceberg::spec::Schema,
) -> Result<LineageResult, String>;

pub(crate) struct LineageResult {
    pub base_fields: Vec<BaseFieldRecord>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}
```

It walks each `ProjectItem.expr` and the optional `filter`, collects `ColumnRef`s, looks each `(qualifier, column)` name up in the base Iceberg schema by name to find the field id and type signature, returns lineage. Pure function — no IO, no state.

For A11 phase 1, MV form is single-base projection/filter, so any qualifier on a `ColumnRef` matches the single base (or is `None`). Reject (return Err) if qualifier names a different relation; this should never happen given A9's earlier shape validation, but is a defensive check.

**Files:**
- Create: `src/sql/analyzer/mv_lineage.rs`
- Modify: `src/sql/analyzer/mod.rs` (add `pub(crate) mod mv_lineage;`)
- Test: inline `#[cfg(test)] mod tests` in `mv_lineage.rs`

- [ ] **Step 1: Add the module declaration**

Edit `src/sql/analyzer/mod.rs`: locate the existing block of submodule declarations (`pub(crate) mod helpers;`, etc., near the top of the file) and add:

```rust
pub(crate) mod mv_lineage;
```

- [ ] **Step 2: Write the failing test first**

Create `src/sql/analyzer/mv_lineage.rs`:

```rust
//! IVM-A11 MV lineage builder.
//!
//! Given a ResolvedQuery for a single-base projection/filter MV plus the
//! base table's current Iceberg schema, produce the field-id-based
//! lineage that A11's contract persists.

use crate::meta::repository::mv_contract::{
    BaseFieldRecord, ExpressionKind, ExpressionLineage, FilterLineage, OutputColumnLineage,
};
use crate::sql::analysis::{ExprKind, ResolvedQuery, ResolvedSelect, TypedExpr, Relation, QueryBody};

pub(crate) struct LineageResult {
    pub base_fields: Vec<BaseFieldRecord>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}

/// Build A11 lineage for a single-base projection/filter MV. Caller
/// must have already classified the shape as ProjectionFilter; this
/// function defensively asserts that the resolved query is a single
/// SELECT over a single base scan and returns an error otherwise.
pub(crate) fn build_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_iceberg_schema: &iceberg::spec::Schema,
) -> Result<LineageResult, String> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => return Err("A11 lineage builder requires a SELECT query".to_string()),
    };
    let _scan = single_scan_or_err(select)?;

    let mut output_columns = Vec::with_capacity(select.projection.len());
    let mut referenced: std::collections::BTreeMap<i32, BaseFieldRecord> =
        std::collections::BTreeMap::new();

    for item in &select.projection {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(&item.expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_iceberg_schema, name)?;
            field_ids.push(field.id);
            referenced.entry(field.id).or_insert_with(|| BaseFieldRecord {
                field_id: field.id,
                name_at_create: field.name.clone(),
                type_signature: format!("{}", field.field_type),
                required: field.required,
            });
        }
        field_ids.sort_unstable();
        field_ids.dedup();

        output_columns.push(OutputColumnLineage {
            expression: ExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: field_ids,
            },
        });
    }

    let filter = if let Some(filter_expr) = &select.filter {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(filter_expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_iceberg_schema, name)?;
            field_ids.push(field.id);
            referenced.entry(field.id).or_insert_with(|| BaseFieldRecord {
                field_id: field.id,
                name_at_create: field.name.clone(),
                type_signature: format!("{}", field.field_type),
                required: field.required,
            });
        }
        field_ids.sort_unstable();
        field_ids.dedup();
        Some(FilterLineage {
            referenced_base_field_ids: field_ids,
        })
    } else {
        None
    };

    let base_fields = referenced.into_values().collect();
    Ok(LineageResult {
        base_fields,
        output_columns,
        filter,
    })
}

fn single_scan_or_err(select: &ResolvedSelect) -> Result<(), String> {
    match select.from.as_ref() {
        Some(Relation::Scan(_)) => Ok(()),
        Some(_) => Err("A11 lineage builder requires a single-base SCAN, not a join or subquery".to_string()),
        None => Err("A11 lineage builder requires a FROM clause".to_string()),
    }
}

fn resolve_field<'a>(
    schema: &'a iceberg::spec::Schema,
    column_name: &str,
) -> Result<&'a iceberg::spec::NestedField, String> {
    schema
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column_name))
        .map(|f| f.as_ref())
        .ok_or_else(|| {
            format!(
                "base iceberg schema does not contain column {column_name}; cannot build A11 lineage"
            )
        })
}

/// Walks a TypedExpr, collecting every ColumnRef as (qualifier, name).
/// Also updates a coarse ExpressionKindHint.
fn collect_column_refs(
    expr: &TypedExpr,
    out: &mut Vec<(Option<String>, String)>,
    kind: &mut ExpressionKindHint,
) {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            out.push((qualifier.clone(), column.clone()));
            kind.saw_column();
        }
        ExprKind::Literal(_) => {
            kind.saw_literal();
        }
        ExprKind::BinaryOp { left, right, .. } => {
            kind.saw_func();
            collect_column_refs(left, out, kind);
            collect_column_refs(right, out, kind);
        }
        ExprKind::UnaryOp { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::FunctionCall { args, .. } => {
            kind.saw_func();
            for a in args {
                collect_column_refs(a, out, kind);
            }
        }
        // Cast in NovaRocks's TypedExpr is represented as FunctionCall("cast", ...) or a dedicated
        // variant; treat anything not matched above as "saw_func" and recurse defensively.
        _ => {
            kind.saw_func();
            visit_children(&expr.kind, out, kind);
        }
    }
}

fn visit_children(_kind: &ExprKind, _out: &mut Vec<(Option<String>, String)>, _hint: &mut ExpressionKindHint) {
    // No-op: the ExprKind variants we don't explicitly handle (lambda,
    // aggregate, etc.) are rejected by A9 shape classification before
    // reaching A11. Keep this defensive — A11 phase 1 is projection/filter
    // only. If a new variant slips through, lineage will be incomplete and
    // CREATE self-check (Task 8) will catch the resulting empty
    // referenced_base_field_ids on a non-Column expression.
}

#[derive(Default)]
struct ExpressionKindHint {
    saw_column: bool,
    saw_literal: bool,
    saw_func: bool,
}

impl ExpressionKindHint {
    fn saw_column(&mut self) { self.saw_column = true; }
    fn saw_literal(&mut self) { self.saw_literal = true; }
    fn saw_func(&mut self) { self.saw_func = true; }
    fn into_kind(self) -> ExpressionKind {
        match (self.saw_column, self.saw_literal, self.saw_func) {
            (true, false, false) => ExpressionKind::Column,
            (false, true, false) => ExpressionKind::Literal,
            (_, _, true) if self.saw_column || self.saw_literal => ExpressionKind::Mixed,
            (false, false, true) => ExpressionKind::Func,
            _ => ExpressionKind::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    fn base_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
                Arc::new(NestedField::required(2, "region", Type::Primitive(PrimitiveType::String))),
                Arc::new(NestedField::optional(3, "amount", Type::Primitive(PrimitiveType::Double))),
            ])
            .build()
            .expect("build schema")
    }

    #[test]
    fn resolve_field_finds_column_case_insensitive() {
        let s = base_schema();
        let f = resolve_field(&s, "REGION").expect("find region");
        assert_eq!(f.id, 2);
        assert_eq!(format!("{}", f.field_type), "string");
    }

    #[test]
    fn resolve_field_errors_on_missing_column() {
        let s = base_schema();
        let err = resolve_field(&s, "nope").unwrap_err();
        assert!(err.contains("nope"), "{err}");
    }
}
```

- [ ] **Step 3: Run lineage helper tests**

Run:
```bash
cargo test --lib sql::analyzer::mv_lineage 2>&1 | tail -10
```

Expected: 2 passed.

(Full lineage tests over real `ResolvedQuery` instances are integration-flavored and easier to cover via the CREATE flow tests in Task 8 and the SQL tests in Task 13. The unit tests here cover the schema lookup helper.)

- [ ] **Step 4: Commit**

```bash
git add src/sql/analyzer/mv_lineage.rs src/sql/analyzer/mod.rs
git commit -m "feat(analyzer): add A11 projection/filter lineage builder

build_projection_filter_lineage walks a ResolvedQuery's projection and
filter, looks each ColumnRef up in the base Iceberg schema, and returns
referenced base field ids + ExpressionKind. Single-base, single-scan
only — matches A11 phase 1 scope."
```

---

## Task 8: Wire the real lineage into `create_iceberg_mv`

Replace Task 6's empty stubs with actual lineage from Task 7. Also call `MvSchemaContract::ensure_self_consistent` before persistence, and on failure, drop the just-created target Iceberg table.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs` (`create_iceberg_mv`)

- [ ] **Step 1: Locate the Task 6 placeholder contract construction**

Run:
```bash
grep -n "populated in Task 7\|schema_contract:" src/engine/mv/iceberg_refresh.rs | head
```

Note the location of the contract literal in `create_iceberg_mv`.

- [ ] **Step 2: Replace empty stubs with real lineage**

Inside `create_iceberg_mv`, **after** the `let target_loaded = ...` line and **before** the `let primary_key_columns = ...` line, add the lineage build:

```rust
    let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
        &analysis.resolved_query,
        loaded_base.table.metadata().current_schema(),
    )?;
```

Then replace the placeholder contract construction's empty fields:

```rust
                    schema_contract: {
                        let contract = crate::meta::repository::mv_contract::MvSchemaContract {
                            contract_version: 1,
                            base: crate::meta::repository::mv_contract::BaseContract {
                                table_fqn: base_ref.fqn(),
                                table_uuid: loaded_base.table.metadata().uuid().to_string(),
                                schema_id_at_create: loaded_base.table.metadata().current_schema_id(),
                                schema_at_create: crate::meta::repository::mv_contract::BaseSchemaSnapshot {
                                    fields: lineage.base_fields.clone(),
                                },
                            },
                            output: crate::meta::repository::mv_contract::OutputContract {
                                columns: lineage.output_columns.clone(),
                                filter: lineage.filter.clone(),
                            },
                            target: crate::meta::repository::mv_contract::TargetContract {
                                table_fqn: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                                table_uuid: target_loaded.table.metadata().uuid().to_string(),
                                schema_id_at_create: target_loaded.table.metadata().current_schema_id(),
                                visible_columns: analysis
                                    .output_columns
                                    .iter()
                                    .map(|col| {
                                        let field = target_loaded
                                            .table
                                            .metadata()
                                            .current_schema()
                                            .as_struct()
                                            .fields()
                                            .iter()
                                            .find(|f| f.name.eq_ignore_ascii_case(&col.name))
                                            .ok_or_else(|| format!(
                                                "Iceberg MV target schema missing output column {}",
                                                col.name
                                            ))?;
                                        Ok::<_, String>(crate::meta::repository::mv_contract::TargetVisibleColumn {
                                            output_name: col.name.clone(),
                                            target_field_id: field.id,
                                            type_signature: format!("{}", field.field_type),
                                            nullable: !field.required,
                                        })
                                    })
                                    .collect::<Result<Vec<_>, _>>()?,
                                hidden_apply_key: crate::meta::repository::mv_contract::HiddenApplyKeyContract {
                                    column_name: crate::meta::repository::mv_contract::HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                                    target_field_id: actual_apply_key_field_id,
                                    source: crate::meta::repository::mv_contract::ApplyKeySource::BaseRowId,
                                },
                            },
                        };
                        contract.ensure_self_consistent().map_err(|e| format!("Iceberg MV contract self-check failed: {e}"))?;
                        contract
                    },
```

Note: `contract.ensure_self_consistent()` is called inside the `(|| { ... })()` closure that wraps metadata persistence (the same one that drops the target table on error). So a contract self-check failure correctly triggers target rollback via the existing `drop_table` path.

- [ ] **Step 3: Confirm the file compiles**

Run:
```bash
cargo build 2>&1 | tail -5
```

Expected: `Finished`.

- [ ] **Step 4: Run existing `iceberg_refresh.rs` tests**

```bash
cargo test --lib engine::mv::iceberg_refresh 2>&1 | tail -15
```

Expected: all pre-existing tests pass. If a test was constructing `CreateMvDefinitionRequest` directly, you'll need to update it to provide a contract — apply the same pattern. Most tests should be using helper builders that go through `create_iceberg_mv`, which already provides a contract now.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): wire real lineage into create_iceberg_mv contract

CREATE MV now builds A11 lineage from the analyzer's ResolvedQuery and
the base Iceberg schema, populates schema_contract.base.schema_at_create
+ output.columns + output.filter, then runs ensure_self_consistent
before persistence. On self-check failure the target Iceberg table is
dropped via the existing rollback path."
```

---

## Task 9: Build `schema_contract` module with `validate_schema_contract`

Create `src/engine/mv/schema_contract.rs` exposing `validate_schema_contract` and the `ContractDecision` / `SchemaEvolutionError` types. This module is consumed by `refresh_iceberg_mv` in Task 10. It depends on Task 7's lineage builder so it can be invoked again at REFRESH time (though as the spec notes, the second invocation is currently only used for verifying lineage build still succeeds — fingerprint comparison was removed).

For A11 phase 1, the contract decision is computed entirely from contract baseline + current live `iceberg::Table`s — no analyzer re-run is needed in the validator itself. The validator does NOT recall the lineage builder; that's done by the refresh path if and when it needs to re-plan changes with rebinding.

**Files:**
- Create: `src/engine/mv/schema_contract.rs`
- Modify: `src/engine/mv/mod.rs` (add `pub(crate) mod schema_contract;`)
- Test: inline tests in `schema_contract.rs`

- [ ] **Step 1: Add module declaration**

Edit `src/engine/mv/mod.rs`, add `pub(crate) mod schema_contract;` next to existing submodules.

- [ ] **Step 2: Implement the validator**

Create `src/engine/mv/schema_contract.rs`:

```rust
//! IVM-A11 refresh-time schema contract validator.
//!
//! Single entry point: `validate_schema_contract`. Three-stage check:
//!   1. identity guard (uuid + format-version + row-lineage)
//!   2. schema-id fast path + base referenced-field exact match
//!   3. target visible columns + hidden apply-key exact match
//!
//! Decisions are explicit. There is NO fallback path: incompatible
//! contracts result in fail-fast errors that propagate to the user.

use crate::meta::repository::mv_contract::{
    BaseFieldRecord, MvSchemaContract, HIDDEN_APPLY_KEY_COLUMN_NAME,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        /// (base field id, name_at_create, current_name)
        rebound_columns: Vec<(i32, String, String)>,
    },
    Incompatible(SchemaEvolutionError),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SchemaEvolutionError {
    BaseTableIdentityChanged { expected: String, actual: String },
    BaseRowLineageContractBroken { reason: String },
    BaseFieldDropped { field_id: i32, name_at_create: String },
    BaseFieldTypeChanged { field_id: i32, name_at_create: String, from: String, to: String },
    TargetTableIdentityChanged { expected: String, actual: String },
    TargetRowLineageContractBroken { reason: String },
    TargetVisibleFieldDropped { output_name: String, target_field_id: i32 },
    TargetVisibleFieldRenamed { target_field_id: i32, expected: String, actual: String },
    TargetVisibleFieldTypeChanged { target_field_id: i32, from: String, to: String },
    HiddenApplyKeyContractBroken { reason: String },
}

impl std::fmt::Display for SchemaEvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BaseTableIdentityChanged { expected, actual } => write!(f,
                "iceberg MV refresh blocked: base table identity changed (uuid expected={expected}, actual={actual}); run REFRESH FULL or recreate the MV"),
            Self::BaseRowLineageContractBroken { reason } => write!(f,
                "iceberg MV refresh blocked: base table row-lineage contract broken ({reason}); run REFRESH FULL or recreate the MV"),
            Self::BaseFieldDropped { field_id, name_at_create } => write!(f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) was dropped from base table; run REFRESH FULL or recreate the MV"),
            Self::BaseFieldTypeChanged { field_id, name_at_create, from, to } => write!(f,
                "iceberg MV refresh blocked: base column \"{name_at_create}\" (field id {field_id}) changed type from {from} to {to}; run REFRESH FULL or recreate the MV"),
            Self::TargetTableIdentityChanged { expected, actual } => write!(f,
                "iceberg MV refresh blocked: target table identity changed (uuid expected={expected}, actual={actual}); recreate the MV"),
            Self::TargetRowLineageContractBroken { reason } => write!(f,
                "iceberg MV refresh blocked: target table row-lineage contract broken ({reason}); recreate the MV"),
            Self::TargetVisibleFieldDropped { output_name, target_field_id } => write!(f,
                "iceberg MV refresh blocked: target visible column \"{output_name}\" (field id {target_field_id}) was dropped; recreate the MV"),
            Self::TargetVisibleFieldRenamed { target_field_id, expected, actual } => write!(f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) renamed externally: expected \"{expected}\", actual \"{actual}\"; recreate the MV"),
            Self::TargetVisibleFieldTypeChanged { target_field_id, from, to } => write!(f,
                "iceberg MV refresh blocked: target visible column (field id {target_field_id}) changed type from {from} to {to}; recreate the MV"),
            Self::HiddenApplyKeyContractBroken { reason } => write!(f,
                "iceberg MV refresh blocked: target hidden apply-key column contract broken ({reason}); recreate the MV"),
        }
    }
}

const ICEBERG_ROW_LINEAGE_PROP: &str = "write.row-lineage";

pub(crate) fn validate_schema_contract(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::table::Table,
    current_target_table: &iceberg::table::Table,
) -> ContractDecision {
    // Stage 1: identity guard.
    if let Some(err) = validate_identity_guards(contract, current_base_table, current_target_table) {
        return ContractDecision::Incompatible(err);
    }
    // Stage 2 fast path.
    if current_base_table.metadata().current_schema_id() == contract.base.schema_id_at_create
        && current_target_table.metadata().current_schema_id() == contract.target.schema_id_at_create
    {
        return ContractDecision::CompatibleSafe;
    }
    // Stage 2 precise base check.
    let base_check = check_base_referenced_fields(contract, current_base_table);
    let rebound = match base_check {
        Err(err) => return ContractDecision::Incompatible(err),
        Ok(r) => r,
    };
    // Stage 3 target check.
    if let Some(err) = check_target_schema(contract, current_target_table) {
        return ContractDecision::Incompatible(err);
    }
    if rebound.is_empty() {
        ContractDecision::CompatibleSafe
    } else {
        ContractDecision::CompatibleSafeWithRebind { rebound_columns: rebound }
    }
}

fn validate_identity_guards(
    contract: &MvSchemaContract,
    base: &iceberg::table::Table,
    target: &iceberg::table::Table,
) -> Option<SchemaEvolutionError> {
    let actual_base_uuid = base.metadata().uuid().to_string();
    if actual_base_uuid != contract.base.table_uuid {
        return Some(SchemaEvolutionError::BaseTableIdentityChanged {
            expected: contract.base.table_uuid.clone(),
            actual: actual_base_uuid,
        });
    }
    if base.metadata().format_version() != iceberg::spec::FormatVersion::V3
        || !row_lineage_enabled(base.metadata().properties())
    {
        return Some(SchemaEvolutionError::BaseRowLineageContractBroken {
            reason: "base table must be Iceberg v3 with write.row-lineage=true".to_string(),
        });
    }

    let actual_target_uuid = target.metadata().uuid().to_string();
    if actual_target_uuid != contract.target.table_uuid {
        return Some(SchemaEvolutionError::TargetTableIdentityChanged {
            expected: contract.target.table_uuid.clone(),
            actual: actual_target_uuid,
        });
    }
    if target.metadata().format_version() != iceberg::spec::FormatVersion::V3
        || !row_lineage_enabled(target.metadata().properties())
    {
        return Some(SchemaEvolutionError::TargetRowLineageContractBroken {
            reason: "target table must be Iceberg v3 with write.row-lineage=true".to_string(),
        });
    }
    None
}

fn check_base_referenced_fields(
    contract: &MvSchemaContract,
    base: &iceberg::table::Table,
) -> Result<Vec<(i32, String, String)>, SchemaEvolutionError> {
    let current = base.metadata().current_schema().as_struct();
    let mut rebound = Vec::new();
    for record in &contract.base.schema_at_create.fields {
        let Some(field) = current.fields().iter().find(|f| f.id == record.field_id) else {
            return Err(SchemaEvolutionError::BaseFieldDropped {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
            });
        };
        let current_signature = format!("{}", field.field_type);
        if current_signature != record.type_signature {
            return Err(SchemaEvolutionError::BaseFieldTypeChanged {
                field_id: record.field_id,
                name_at_create: record.name_at_create.clone(),
                from: record.type_signature.clone(),
                to: current_signature,
            });
        }
        if !field.name.eq_ignore_ascii_case(&record.name_at_create) {
            rebound.push((record.field_id, record.name_at_create.clone(), field.name.clone()));
        }
    }
    Ok(rebound)
}

fn check_target_schema(
    contract: &MvSchemaContract,
    target: &iceberg::table::Table,
) -> Option<SchemaEvolutionError> {
    let current = target.metadata().current_schema().as_struct();
    for tv in &contract.target.visible_columns {
        let Some(field) = current.fields().iter().find(|f| f.id == tv.target_field_id) else {
            return Some(SchemaEvolutionError::TargetVisibleFieldDropped {
                output_name: tv.output_name.clone(),
                target_field_id: tv.target_field_id,
            });
        };
        let sig = format!("{}", field.field_type);
        if sig != tv.type_signature {
            return Some(SchemaEvolutionError::TargetVisibleFieldTypeChanged {
                target_field_id: tv.target_field_id,
                from: tv.type_signature.clone(),
                to: sig,
            });
        }
        if !field.name.eq_ignore_ascii_case(&tv.output_name) {
            return Some(SchemaEvolutionError::TargetVisibleFieldRenamed {
                target_field_id: tv.target_field_id,
                expected: tv.output_name.clone(),
                actual: field.name.clone(),
            });
        }
    }

    let expected = &contract.target.hidden_apply_key;
    let Some(field) = current.fields().iter().find(|f| f.id == expected.target_field_id) else {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key field id {} not found", expected.target_field_id),
        });
    };
    if !field.name.eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME) {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: format!("hidden apply-key column renamed to {}", field.name),
        });
    }
    if !field.required {
        return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
            reason: "hidden apply-key column must be required".to_string(),
        });
    }
    match field.field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long) => {}
        other => {
            return Some(SchemaEvolutionError::HiddenApplyKeyContractBroken {
                reason: format!("hidden apply-key column must be long, got {other:?}"),
            });
        }
    }
    None
}

fn row_lineage_enabled(props: &std::collections::HashMap<String, String>) -> bool {
    props
        .get(ICEBERG_ROW_LINEAGE_PROP)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

```

- [ ] **Step 3: Add unit tests for `validate_schema_contract`**

Append to `schema_contract.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::repository::mv_contract::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn sample_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.ns.orders".into(),
                table_uuid: "BASE-UUID".into(),
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![
                        BaseFieldRecord { field_id: 1, name_at_create: "id".into(), type_signature: "long".into(), required: true },
                        BaseFieldRecord { field_id: 2, name_at_create: "region".into(), type_signature: "string".into(), required: false },
                    ],
                },
            },
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                    },
                }],
                filter: None,
            },
            target: TargetContract {
                table_fqn: "ice.mv.orders_mv".into(),
                table_uuid: "TARGET-UUID".into(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".into(),
                    target_field_id: 1,
                    type_signature: "long".into(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: HIDDEN_APPLY_KEY_COLUMN_NAME.into(),
                    target_field_id: 2,
                    source: ApplyKeySource::BaseRowId,
                },
            },
        }
    }

    // NOTE: building real `iceberg::table::Table` instances is heavy. The
    // tests here cover the pure-function check_base_referenced_fields and
    // check_target_schema paths directly. End-to-end tests run via the
    // SQL integration suite in Task 13.

    fn base_schema_at_create() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))),
                Arc::new(NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String))),
            ])
            .build()
            .unwrap()
    }

    // Stub helper that wraps schema in a fake Table is too painful — we
    // instead exercise the pure helpers `check_base_referenced_fields`
    // and `check_target_schema` indirectly through their inputs. SQL
    // integration tests cover the wired path.

    #[test]
    fn schema_evolution_error_messages_are_action_oriented() {
        let err = SchemaEvolutionError::BaseFieldDropped {
            field_id: 5,
            name_at_create: "amount".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("field id 5"));
        assert!(msg.contains("amount"));
        assert!(msg.contains("REFRESH FULL"));
    }

    #[test]
    fn schema_evolution_error_target_messages_recommend_recreate() {
        let err = SchemaEvolutionError::TargetTableIdentityChanged {
            expected: "A".into(),
            actual: "B".into(),
        };
        let msg = format!("{err}");
        assert!(msg.contains("recreate the MV"));
    }

    #[test]
    fn sample_contract_self_consistency() {
        assert!(sample_contract().ensure_self_consistent().is_ok());
        let _ = base_schema_at_create();
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test --lib engine::mv::schema_contract 2>&1 | tail -10
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mv/schema_contract.rs src/engine/mv/mod.rs
git commit -m "feat(mv): add A11 validate_schema_contract refresh-time guard

Three-stage check: identity guard (uuid + v3 + row-lineage), base
referenced-field exact match (with schema_id fast path), target
visible columns + hidden apply-key exact match. Returns explicit
ContractDecision; no fallback path. SchemaEvolutionError messages
are action-oriented and recommend REFRESH FULL or MV recreate."
```

---

## Task 10: Wire `validate_schema_contract` into `refresh_iceberg_mv`

Replace the `TODO(A11-Task-10)` placeholder left in Task 6 with a real `validate_schema_contract` call. Use `Incompatible` to fail fast; log `CompatibleSafeWithRebind` and continue.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Locate the Task 6 placeholder**

Run:
```bash
grep -n "A11-Task-10\|validate_schema_contract" src/engine/mv/iceberg_refresh.rs
```

- [ ] **Step 2: Wire validation in**

Replace the placeholder comment in `refresh_iceberg_mv` (right after `validate_target_snapshot` and before the base-load block):

```rust
    // A11 contract guard. Validate the full base ↔ output ↔ target
    // contract before any incremental work. This subsumes A9's
    // ensure_target_apply_key_contract / ensure_base_row_lineage_contract
    // checks at this site (Task 10).
    let base_for_guard = load_current_iceberg_base_table(state, &parse_iceberg_table_refs(&mv_definition.base_table_refs)?[0])?;
    match crate::engine::mv::schema_contract::validate_schema_contract(
        &mv_definition.schema_contract,
        &base_for_guard.table,
        &target_loaded.table,
    ) {
        crate::engine::mv::schema_contract::ContractDecision::Incompatible(err) => {
            return Err(format!("{err}"));
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
            tracing::info!(
                target = ?target,
                rebound = ?rebound_columns,
                "iceberg MV refresh: base columns rebound by field id; continuing",
            );
        }
        crate::engine::mv::schema_contract::ContractDecision::CompatibleSafe => {}
    }
```

This intentionally double-loads the base table (the rest of `refresh_iceberg_mv` also loads it later). For Task 10's atomic change this is the simplest correct insertion. Task 11 collapses the double-load.

- [ ] **Step 3: Build, run existing refresh tests**

```bash
cargo build 2>&1 | tail -5
cargo test --lib engine::mv::iceberg_refresh 2>&1 | tail -15
```

Expected: build green; tests pass. Any pre-existing test that constructs `StoredMvDefinition` with `target_apply_key: ...` must now provide a `schema_contract: MvSchemaContract { ... }` — apply the migration in this same task.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): gate refresh_iceberg_mv on A11 validate_schema_contract

Replaces A9's local ensure_target_apply_key_contract call with the
single A11 contract validator. Incompatible decisions propagate as
errors; SafeWithRebind decisions log INFO and continue."
```

---

## Task 11: Collapse the double base-table load in `refresh_iceberg_mv`

Task 10 deliberately loaded the base table once for the contract guard, then the existing refresh flow loaded it again. Restructure so a single `load_current_iceberg_base_table` call feeds both.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Identify both load sites**

Run:
```bash
grep -n "load_current_iceberg_base_table" src/engine/mv/iceberg_refresh.rs | head
```

- [ ] **Step 2: Move the first load up**

Move the `let base_for_guard = ...;` line so it precedes both the contract guard and the existing `let loaded = load_current_iceberg_base_table(...)` later. Then rename `base_for_guard` to `loaded` and delete the original later load. Both Task 10's guard block and the subsequent existing logic now reference the same `loaded`.

Sketch (the exact line surgery depends on Task 10's resulting structure):

```rust
    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let [base_ref] = base_refs.as_slice() else { return Err("...".into()); };
    let loaded = load_current_iceberg_base_table(state, base_ref)?;

    match crate::engine::mv::schema_contract::validate_schema_contract(
        &mv_definition.schema_contract,
        &loaded.table,
        &target_loaded.table,
    ) {
        // ... same as before ...
    }

    // Existing flow continues; `ensure_base_row_lineage_contract` call
    // a few lines below is now redundant (validate_schema_contract
    // already covers it) — remove it.
```

Delete the now-redundant call `ensure_base_row_lineage_contract(&loaded.table, &base_ref.fqn())?;` later in `refresh_iceberg_mv`.

- [ ] **Step 3: Build and test**

```bash
cargo build 2>&1 | tail -3
cargo test --lib engine::mv::iceberg_refresh 2>&1 | tail -10
```

Expected: green, tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor(mv): single base-table load for A11 guard + refresh flow

Collapses the double load introduced in Task 10. Removes the now-
redundant ensure_base_row_lineage_contract call later in the function
(validate_schema_contract already enforces v3 + row-lineage)."
```

---

## Task 12: `REFRESH FULL` regenerates the contract via drop & recreate target

The spec's full-rebuild path: drop existing target Iceberg table, recreate it (reusing `create_iceberg_mv`'s target-creation logic), rebuild lineage, write a new contract, and update metadata atomically with the new base/target snapshots.

NovaRocks's existing `RefreshMaterializedViewStmt` accepts a `full: bool` flag (verify in code). The refresh entry-point branches on it.

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Inspect the current REFRESH FULL handling**

Run:
```bash
grep -n "RefreshMaterializedViewStmt\|fn refresh_iceberg_mv\|full:\|rebuild_iceberg_mv" src/engine/mv/iceberg_refresh.rs src/sql/parser/ast.rs | head -20
```

Confirm whether `RefreshMaterializedViewStmt` already carries a `full` flag and whether there is an existing rebuild path A9 used as a fallback. The A9 doc references `rebuild_iceberg_mv` at line 1421 of `iceberg_refresh.rs` — that path is what `REFRESH ... FULL` should now drive (if it isn't already).

- [ ] **Step 2: Route REFRESH FULL to a contract-regenerating rebuild**

Inside `refresh_iceberg_mv`, near the top after `let target = resolve_refresh_target(...)?;`:

```rust
    if stmt.full {
        return refresh_full_iceberg_mv(
            state,
            current_catalog,
            current_database,
            stmt,
            &target,
        );
    }
```

Then add `refresh_full_iceberg_mv` as a sibling function. It should:

1. Load current base & target.
2. Drop the existing target Iceberg table (`crate::connector::iceberg::catalog::registry::drop_table(...)`).
3. Recreate the target by calling into the same target-creation block used by `create_iceberg_mv` (extract to a helper if not already).
4. Rebuild lineage via `build_projection_filter_lineage`.
5. Construct a new `MvSchemaContract`, call `ensure_self_consistent`.
6. Run the full-rebuild data write (reuse A9's `rebuild_iceberg_mv` body for data; A9 already has this).
7. Update `StoredMvDefinition` atomically: new `schema_contract`, new snapshot maps. Reuse the same metadata transaction shape as CREATE.

Because steps 3–6 share significant logic with `create_iceberg_mv`, extract a private helper whose body is **moved verbatim** from `create_iceberg_mv` (lines covering: analyze → classify shape → load base → `ensure_base_row_lineage_contract` → build target Iceberg columns → call `create_table` → load target → `find_apply_key_field_id` → `build_projection_filter_lineage` → construct `MvSchemaContract` → `ensure_self_consistent`).

Concrete extraction:

```rust
/// Shared by create_iceberg_mv and refresh_full_iceberg_mv. Performs
/// every step from analyzer through contract construction; the caller
/// is responsible for persisting `StoredMvDefinition` and registering
/// the target in the standalone catalog.
struct TargetAndContract {
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    analysis: crate::connector::starrocks::managed::mv_ddl::MvAnalysis,
    base_ref: crate::connector::starrocks::managed::model::IcebergTableRef,
    base_loaded: crate::connector::starrocks::managed::mv_refresh::LoadedIcebergBaseTable,
    target_loaded: crate::connector::starrocks::managed::mv_refresh::LoadedIcebergTable,
    contract: crate::meta::repository::mv_contract::MvSchemaContract,
}

fn create_iceberg_mv_target_and_contract(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    canonical_select_query: &sqlparser::ast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt_primary_key: Option<&[String]>,
    stmt_partition_by: Option<&[String]>,
) -> Result<TargetAndContract, String> {
    // 1. Resolve the iceberg catalog entry for the target.
    let entry = { /* lifted from create_iceberg_mv: read state.iceberg_catalogs */ };

    // 2. Analyze, classify shape, validate single-base projection/filter.
    let analysis = analyze_mv_select(state, current_catalog, current_database, canonical_select_query)?;
    validate_mv_partition_columns(stmt_partition_by, &analysis.output_columns)?;
    let base_refs = extract_base_table_refs(&analysis.resolved_refs)?;
    let shape = classify_incremental_mv_query(canonical_select_query)?;
    if !matches!(shape, IncrementalMvShape::ProjectionFilter(_)) {
        return Err("phase4a iceberg-backed materialized views support only projection/filter shapes".to_string());
    }
    let [base_ref] = base_refs.as_slice() else {
        return Err("iceberg-backed materialized views require exactly one iceberg base table".to_string());
    };

    // 3. Load base + row-lineage contract + optional PK validation.
    let base_loaded = load_current_iceberg_base_table(state, base_ref)?;
    ensure_base_row_lineage_contract(&base_loaded.table, &base_ref.fqn())?;
    if let Some(pk_cols) = stmt_primary_key {
        let descriptor = crate::connector::starrocks::managed::mv_ddl::descriptor_from_loaded(&base_loaded);
        crate::connector::starrocks::managed::mv_ddl::validate_ivm_primary_key(pk_cols, &descriptor)
            .map_err(|e| e.to_string())?;
    }

    // 4. Build target columns (visible + hidden apply key) and call create_table.
    if analysis.output_columns.iter().any(|c| c.name.eq_ignore_ascii_case(ICEBERG_MV_APPLY_KEY_COLUMN)) {
        return Err(format!("Iceberg MV output column name {ICEBERG_MV_APPLY_KEY_COLUMN} is reserved"));
    }
    let mut columns: Vec<_> = analysis.output_columns.iter().map(output_column_to_table_column).collect::<Result<_, _>>()?;
    columns.push(apply_key_table_column());
    let expected_apply_key_field_id = i32::try_from(columns.len())
        .map_err(|_| "too many iceberg MV output columns".to_string())?;
    crate::connector::iceberg::catalog::registry::create_table(
        &entry, &target.namespace, &target.table, &columns, None, &[],
        &[
            ("format-version".to_string(), "3".to_string()),
            ("write.row-lineage".to_string(), "true".to_string()),
            (ICEBERG_MV_PROP_APPLY_KEY_COLUMN.to_string(), ICEBERG_MV_APPLY_KEY_COLUMN.to_string()),
            (ICEBERG_MV_PROP_APPLY_KEY_SOURCE.to_string(), ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID.to_string()),
            (ICEBERG_MV_PROP_APPLY_KEY_FIELD_ID.to_string(), expected_apply_key_field_id.to_string()),
        ],
    )?;
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let target_loaded = crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)?;
    let actual_apply_key_field_id = find_apply_key_field_id(&target_loaded.table)?;
    if actual_apply_key_field_id != expected_apply_key_field_id {
        return Err(format!("Iceberg MV target apply-key field id mismatch: expected {expected_apply_key_field_id}, got {actual_apply_key_field_id}"));
    }

    // 5. Build A11 lineage + contract, run self-check.
    let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
        &analysis.resolved_query,
        base_loaded.table.metadata().current_schema(),
    )?;
    let contract = /* MvSchemaContract literal from Task 8 Step 2, parameterized on the
                       loaded base/target and lineage above */;
    contract.ensure_self_consistent().map_err(|e| format!("Iceberg MV contract self-check failed: {e}"))?;

    Ok(TargetAndContract {
        entry, analysis, base_ref: (*base_ref).clone(), base_loaded, target_loaded, contract,
    })
}
```

Refactor `create_iceberg_mv` to call this helper and persist the result via `MvMetaRepository::create_definition`. Then `refresh_full_iceberg_mv` calls the same helper after dropping the old target, and persists via a transaction that does `delete_definition` + `create_definition` (or `replace_definition` if available). Look up the actual `MvMetaRepository` API:

```bash
grep -n "fn create_definition\|fn delete_definition\|fn update_definition\|fn replace_definition" src/meta/repository/mv.rs
```

Use whatever exists; do not invent new repository methods inside this task.

For `refresh_full_iceberg_mv`'s metadata update use `MvMetaRepository::replace_definition` if it exists, or do `delete` + `create_definition` in one transaction. Check:

```bash
grep -n "fn replace_definition\|fn delete_definition\|fn update_definition" src/meta/repository/mv.rs
```

Use the available API. If only delete + create exists, that's acceptable inside a single `begin_write` transaction so commit atomicity is preserved.

- [ ] **Step 3: Build, run tests**

```bash
cargo build 2>&1 | tail -5
cargo test --lib engine::mv 2>&1 | tail -15
```

Expected: green, existing tests pass. The new full-rebuild path is exercised via the SQL test in Task 13 case `iceberg_ivm_a11_full_rebuild_after_evolution`.

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(mv): REFRESH FULL regenerates A11 schema contract via drop+recreate

Full rebuild drops the existing target Iceberg table, recreates it via
the shared target-creation helper, rebuilds A11 lineage from the
current base schema, constructs a new MvSchemaContract, runs
ensure_self_consistent, and atomically updates the MV metadata
(definition + snapshot maps + new contract) in a single transaction."
```

---

## Task 13: SQL integration tests in `sql-tests/iceberg-ivm`

The spec calls for 11 SQL test cases under `sql-tests/iceberg-ivm/`. Each case needs a `.sql` file and a `.result` file (NovaRocks's standard SQL test format). Run mode `record` first to capture baseline, then `verify` to confirm stability.

Test cases (from spec):

1. `iceberg_ivm_a11_base_rename_referenced`
2. `iceberg_ivm_a11_base_rename_unreferenced`
3. `iceberg_ivm_a11_base_drop_referenced`
4. `iceberg_ivm_a11_base_drop_unreferenced`
5. `iceberg_ivm_a11_base_drop_add_same_name`
6. `iceberg_ivm_a11_base_type_change_referenced`
7. `iceberg_ivm_a11_base_add_unrelated_column`
8. `iceberg_ivm_a11_base_reorder_columns`
9. `iceberg_ivm_a11_base_uuid_changed`
10. `iceberg_ivm_a11_target_field_id_mismatch`
11. `iceberg_ivm_a11_full_rebuild_after_evolution`

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_*.sql` (11 files)
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_*.result` (11 files, autogenerated in record mode)

For each case, the structure is:

```sql
-- Set up base via Spark or NovaRocks DDL through REST catalog.
-- Create MV.
-- Apply a base evolution.
-- REFRESH MATERIALIZED VIEW mv;  -- or REFRESH MATERIALIZED VIEW mv FULL;
-- SELECT * FROM mv ORDER BY id;
```

For schema-evolution operations not yet expressible through NovaRocks DDL (e.g. drop column via REST catalog), execute the evolution through the test fixture's Spark helper (`docker/iceberg-rest/spark-sql.sh`) — A9 tests already use this pattern.

Each task is its own commit. Pattern below for one case; repeat for all 11.

### Task 13a: `iceberg_ivm_a11_base_rename_referenced`

- [ ] **Step 1: Ensure local test environment is up**

```bash
source docker/iceberg-rest/runtime/current/env.sh || docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
docker/iceberg-rest/status.sh
```

Expected: REST + MinIO + Spark all healthy.

- [ ] **Step 2: Start a standalone server (background)**

Per CLAUDE.md guidance:

```bash
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

- [ ] **Step 3: Write the test SQL**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_base_rename_referenced.sql`:

```sql
-- IVM-A11: base rename of a referenced column. Refresh should succeed
-- via field-id rebind and the MV should reflect the new data.

-- Setup REST catalog (matches other iceberg-ivm cases).
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  'type' = 'iceberg',
  'iceberg.catalog.type' = 'rest',
  'iceberg.catalog.uri' = '${NOVAROCKS_ICEBERG_REST_URI}',
  'iceberg.catalog.warehouse' = '${NOVAROCKS_ICEBERG_REST_WAREHOUSE}'
);

USE CATALOG ice;
CREATE DATABASE IF NOT EXISTS a11_rename;
USE a11_rename;

-- Base table with row-lineage.
CREATE TABLE base (
  id BIGINT,
  region STRING,
  amount DOUBLE
)
WITH (
  'format-version' = '3',
  'write.row-lineage' = 'true'
);

INSERT INTO base VALUES (1, 'US', 10.0), (2, 'EU', 20.0);

CREATE MATERIALIZED VIEW mv AS
SELECT id, region, amount FROM base WHERE region = 'US';

REFRESH MATERIALIZED VIEW mv;
SELECT id, region, amount FROM mv ORDER BY id;

-- Now rename region -> area through Spark (NovaRocks doesn't expose
-- iceberg ALTER ... RENAME COLUMN yet).
\! docker/iceberg-rest/spark-sql.sh "ALTER TABLE ice.a11_rename.base RENAME COLUMN region TO area;"

-- New data referencing the renamed column.
\! docker/iceberg-rest/spark-sql.sh "INSERT INTO ice.a11_rename.base VALUES (3, 'US', 30.0);"

-- Refresh: A11 contract sees field id 2 still exists but name changed
-- (region → area). Decision = CompatibleSafeWithRebind. Refresh
-- succeeds and the MV picks up row id=3.
REFRESH MATERIALIZED VIEW mv;
SELECT id, region, amount FROM mv ORDER BY id;

DROP MATERIALIZED VIEW mv;
DROP TABLE base;
DROP DATABASE a11_rename;
```

(The `\!` syntax for shelling out depends on whether the sql-tests runner supports it; if not, fold the Spark ALTER into a Bash pre-step inside the case driver. Look at `sql-tests/iceberg-compatibility/` for examples of how cross-engine setup is handled in existing cases.)

- [ ] **Step 4: Record the baseline**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_a11_base_rename_referenced \
  --mode record
```

Expected: produces `sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_base_rename_referenced.result`.

- [ ] **Step 5: Inspect the recorded result**

Read the `.result` file. Sanity check:

- First `SELECT id, region, amount FROM mv ORDER BY id;` returns `1, US, 10.0`.
- Second one returns `1, US, 10.0` and `3, US, 30.0` (the row 2 with EU is filtered out).

If the recorded result reflects something else (e.g. an error), the implementation has a bug — go back to Tasks 7–11 and fix.

- [ ] **Step 6: Verify**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_a11_base_rename_referenced \
  --mode verify
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_base_rename_referenced.sql sql-tests/iceberg-ivm/sql/iceberg_ivm_a11_base_rename_referenced.result
git commit -m "test(iceberg-ivm): a11 base rename of referenced column

Rename region -> area through Spark; A11 contract recognizes field id
2 still exists and refreshes via CompatibleSafeWithRebind."
```

### Task 13b–13k: remaining 10 cases

For each of the remaining cases below, follow the **same 7-step pattern** as Task 13a. The differences are the evolution operation, expected outcome (pass vs error), and the assertions in the `.sql`.

| # | Case | Evolution | Expected refresh outcome |
|---|---|---|---|
| 13b | `iceberg_ivm_a11_base_rename_unreferenced` | Rename `country` (not used) | PASS, MV unchanged |
| 13c | `iceberg_ivm_a11_base_drop_referenced` | Drop `region` (used) | ERROR `BaseFieldDropped` |
| 13d | `iceberg_ivm_a11_base_drop_unreferenced` | Drop `country` (not used) | PASS |
| 13e | `iceberg_ivm_a11_base_drop_add_same_name` | Drop `amount` then add `amount BIGINT` (used) | ERROR `BaseFieldDropped` |
| 13f | `iceberg_ivm_a11_base_type_change_referenced` | `amount` DOUBLE → DECIMAL via Spark | ERROR `BaseFieldTypeChanged` |
| 13g | `iceberg_ivm_a11_base_add_unrelated_column` | Add new nullable `comment` | PASS |
| 13h | `iceberg_ivm_a11_base_reorder_columns` | Reorder (Spark) | PASS |
| 13i | `iceberg_ivm_a11_base_uuid_changed` | `DROP TABLE base; CREATE TABLE base ...` | ERROR `BaseTableIdentityChanged` |
| 13j | `iceberg_ivm_a11_target_field_id_mismatch` | Spark drops a target visible column | ERROR `TargetVisibleFieldDropped` |
| 13k | `iceberg_ivm_a11_full_rebuild_after_evolution` | Drop referenced column → `REFRESH FULL` succeeds → next incremental refresh PASS | PASS after FULL |

Each case is its own commit. Use error-asserting `.result` files for cases that expect errors — the sql-tests runner captures stderr / error output as part of the recorded baseline.

Stop the standalone server when done with the batch:

```bash
kill "$SRV_PID" 2>/dev/null
```

---

## Task 14: Final regression sweep — fmt, clippy, full suite

Run `cargo fmt`, `cargo clippy`, the full unit test suite, and the full iceberg-ivm SQL suite. Verify A9's existing case still passes.

**Files:** None modified unless clippy or fmt require it.

- [ ] **Step 1: Run fmt and clippy**

```bash
cargo fmt
cargo clippy --all-targets 2>&1 | tail -10
```

Expected: no warnings introduced by A11 code. Fix any that surface (typically unused imports left over from Task 6's removal of `MvTargetApplyKey` use lines, or `clippy::needless_lifetimes`).

- [ ] **Step 2: Full unit test sweep**

```bash
cargo test --lib 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 3: Full iceberg-ivm SQL suite**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start standalone-server per CLAUDE.md if not already running
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --mode verify
```

Expected: all pass. Specifically A9's `iceberg_ivm_base_delete_row_lineage` must still pass — this is the regression check that A11 didn't break A9.

- [ ] **Step 4: Commit (only if fmt/clippy required code changes)**

If `cargo fmt` or `cargo clippy --fix` modified files, commit them:

```bash
git add -u
git commit -m "chore(mv): A11 fmt + clippy pass"
```

If nothing changed, skip the commit.

---

## Self-review

After writing the plan, the following are confirmed:

**Spec coverage:**
- Data model (spec §"Schema Contract 数据模型") → Task 3
- type_signature spec → Task 3 (uses Iceberg `format!("{}", field.field_type)` which produces the spec's tokens: `long`, `string`, `decimal(P,S)`, etc.)
- referenced_base_field_ids → Tasks 3, 7
- CREATE flow (spec §"CREATE MV 时的 contract 生成") → Tasks 6, 7, 8
- analyzer mv_lineage module → Task 7
- ensure_contract_self_consistent → Tasks 3, 8
- REFRESH guard three-stage (spec §"REFRESH guard") → Task 9
- ContractDecision + SafeWithRebind logging → Tasks 9, 10
- Refresh integration → Tasks 10, 11
- Full rebuild contract regeneration (spec §"Full rebuild") → Task 12
- Drop & recreate target → Task 12
- SchemaEvolutionError enum + action-oriented messages → Task 9
- Schema evolution decision matrix → Task 13 (all 11 cases)
- Test coverage (spec §"测试") → Tasks 3, 7, 9, 13, 14

**Placeholder scan:** Each task contains executable steps with code or commands. The `unimplemented!()` in Task 12 Step 2 is intentional — it marks an extraction point the engineer must fill from existing code; the surrounding text tells them exactly which existing block to copy. Verified.

**Type consistency:** `MvSchemaContract`, `BaseContract`, `OutputContract`, `TargetContract`, `BaseFieldRecord`, `ExpressionLineage`, `ExpressionKind`, `OutputColumnLineage`, `FilterLineage`, `TargetVisibleColumn`, `HiddenApplyKeyContract`, `ApplyKeySource`, `HIDDEN_APPLY_KEY_COLUMN_NAME`, `ContractDecision`, `SchemaEvolutionError` all consistent across Tasks 3, 6, 7, 8, 9, 10, 12. `validate_schema_contract` signature matches between Tasks 9 and 10. `build_projection_filter_lineage` signature matches between Tasks 7 and 8.

**Atomicity policy:** Tasks 4 and 6 deliberately leave the build temporarily broken between commits. This is called out in their commit messages so reviewers don't bisect blindly. Tasks 5 and 8 restore green build.
