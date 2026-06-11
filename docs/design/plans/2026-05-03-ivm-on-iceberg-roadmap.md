# IVM on Iceberg Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 NovaRocks「内表 MV + Iceberg v3 base」从当前 DELETE/object-store 能力推进到有统一 refresh strategy、row identity、apply 语义矩阵和生产化验收门槛的 IVM 路线。

**Architecture:** 按 R0-R5 里程碑推进。先把当前能力收口成基线，再把 refresh 决策抽成独立 strategy 层；随后让 change stream 携带 base row identity，并把 Projection/filter 与 Aggregate MV 的 apply 语义显式化；最后集中处理 Iceberg evolution 和生产化矩阵。执行层继续复用 `src/connector/starrocks/managed/*` 与 `src/connector/iceberg/*`，不引入新的 MV 存储模型。

**Tech Stack:** Rust, Arrow `RecordBatch`, Iceberg 0.9 vendor patch, SQLite metadata store, OpenDAL object store, MinIO-backed `sql-tests`.

---

## Current State

Current working branch at planning time:

```bash
git status --short --branch
```

Expected:

```text
## codex/iceberg-ivm-object-delete-projection
```

Recent base capabilities:

- `82c80b8 Support Iceberg IVM delete projection on object stores`
- `d0a3b39 [codex] Support Iceberg v3 row-lineage delete reads (#70)`
- `7ef9dad Support aggregate MV AVG/MIN/MAX IVM (#69)`
- `0bd945e feat: IVM-on-Iceberg-v3 row-lineage + Puffin DV changelog scan (#67)`

Global roadmap updated alongside this plan:

```text
/Users/harbor/Documents/Obsidian/NovaRocks IVM on Iceberg Roadmap.md
```

## File Structure

Planned code boundaries:

- Create: `src/connector/starrocks/managed/mv_refresh_strategy.rs`
  - Owns `MvRefreshPolicy`, policy reasons, and snapshot-window classification.
  - Keeps policy decisions out of `mv_refresh.rs`.
- Modify: `src/connector/starrocks/managed/mod.rs`
  - Exports the strategy module.
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
  - Becomes orchestration: load metadata, call strategy, dispatch execution.
  - Stops embedding overwrite/evolution policy in execution branches.
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`
  - Adds row-identity-aware change stream primitives in R2.
- Create: `src/connector/starrocks/managed/ivm_row_identity.rs`
  - Owns `_row_id`-based identity normalization and synthetic update detection.
- Create: `src/connector/starrocks/managed/mv_apply_policy.rs`
  - Owns Projection/filter vs Aggregate apply capability matrix in R3.
- Modify: `src/connector/iceberg/changes.rs`
  - Exposes classification details needed by strategy, instead of only returning display strings.
- Modify: `src/connector/starrocks/managed/store.rs`
  - Adds metadata only when a phase needs durable state; each schema bump must include migration tests.
- Add SQL cases under `sql-tests/mv-on-iceberg/sql/`
  - External-object-store coverage belongs here when it depends on MinIO/config.

## Roadmap

### Task 1: R0 Baseline Closeout

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks IVM on Iceberg Roadmap.md`
- Create: `docs/design/notes/2026-05-03-ivm-on-iceberg-r0-summary.md`

- [ ] **Step 1: Write the R0 status note**

Create `docs/design/notes/2026-05-03-ivm-on-iceberg-r0-summary.md`:

```markdown
# R0 - IVM on Iceberg Baseline Summary

**Branch:** `codex/iceberg-ivm-object-delete-projection`
**Baseline commit:** `82c80b8 Support Iceberg IVM delete projection on object stores`

## Supported

- Aggregate MV full refresh over managed-lake storage.
- Aggregate MV incremental refresh for Iceberg append snapshots.
- Aggregate MV DELETE retract for Iceberg v2 Parquet position deletes.
- Aggregate MV DELETE retract for Iceberg v3 Puffin deletion vectors.
- Object-store delete reverse projection for S3/S3A-style paths through configured catalog credentials.
- Empty change stream metadata advance without writing data chunks.
- `_row_id` and `_last_updated_sequence_number` reads on Iceberg v3 row-lineage base tables.

## Explicitly Unsupported

- `INSERT OVERWRITE` incremental bridging.
- Projection/filter MV DELETE apply.
- Equality deletes.
- Schema evolution where the MV uses the changed column.
- Partition evolution policy.
- Concurrent refresh on the same MV.

## Verification

- `cargo test --lib`
- `cargo fmt --check`
- `git diff --check`
```

- [ ] **Step 2: Verify baseline commands**

Run:

```bash
cargo test --lib
cargo fmt --check
git diff --check
```

Expected:

```text
test result: ok
```

`cargo fmt --check` and `git diff --check` should produce no output and exit 0.

- [ ] **Step 3: Commit R0 docs**

Run:

```bash
git add docs/design/notes/2026-05-03-ivm-on-iceberg-r0-summary.md
git commit -m "docs: summarize IVM on Iceberg R0 baseline"
```

### Task 2: R1 Strategy Types and Unit Tests

**Files:**
- Create: `src/connector/starrocks/managed/mv_refresh_strategy.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Test: `src/connector/starrocks/managed/mv_refresh_strategy.rs`

- [ ] **Step 1: Add the strategy module export**

In `src/connector/starrocks/managed/mod.rs`, add:

```rust
pub(crate) mod mv_refresh_strategy;
```

- [ ] **Step 2: Add policy types**

Create `src/connector/starrocks/managed/mv_refresh_strategy.rs`:

```rust
use crate::connector::iceberg::changes::ChangeError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvRefreshPolicy {
    NoOp {
        current_snapshot_id: i64,
    },
    FullRefresh {
        target_snapshot_id: Option<i64>,
        reason: FullRefreshReason,
    },
    Incremental {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
    },
    Unsupported {
        reason: UnsupportedRefreshReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FullRefreshReason {
    InitialRefresh,
    InsertOverwrite { snapshot_id: i64 },
    LineageExpired { previous_snapshot_id: i64 },
    SchemaEvolutionSafeFallback { detail: String },
    MinMaxDeleteRetractUnsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum UnsupportedRefreshReason {
    EqualityDelete { snapshot_id: i64 },
    SchemaEvolution { detail: String },
    ReplaceValidationFailed { snapshot_id: i64, reason: String },
    InternalInconsistency { detail: String },
}

pub(crate) fn choose_snapshot_refresh_policy(
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> Result<MvRefreshPolicy, String> {
    match (previous_snapshot_id, current_snapshot_id) {
        (None, current) => Ok(MvRefreshPolicy::FullRefresh {
            target_snapshot_id: current,
            reason: FullRefreshReason::InitialRefresh,
        }),
        (Some(previous), Some(current)) if previous == current => Ok(MvRefreshPolicy::NoOp {
            current_snapshot_id: current,
        }),
        (Some(previous), Some(current)) => Ok(MvRefreshPolicy::Incremental {
            previous_snapshot_id: previous,
            current_snapshot_id: current,
        }),
        (Some(previous), None) => Err(format!(
            "materialized view refresh cannot advance from snapshot {previous}: base table has no current snapshot"
        )),
    }
}

pub(crate) fn policy_from_change_error(err: ChangeError) -> MvRefreshPolicy {
    match err {
        ChangeError::UnsupportedOperation { snapshot_id, op } if op == "overwrite" => {
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(snapshot_id),
                reason: FullRefreshReason::InsertOverwrite { snapshot_id },
            }
        }
        ChangeError::LineageBroken { previous_snapshot } => MvRefreshPolicy::FullRefresh {
            target_snapshot_id: None,
            reason: FullRefreshReason::LineageExpired {
                previous_snapshot_id: previous_snapshot,
            },
        },
        ChangeError::EqualityDeleteUnsupported { snapshot_id } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::EqualityDelete { snapshot_id },
        },
        ChangeError::SchemaEvolutionUnsupported { detail } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::SchemaEvolution { detail },
        },
        ChangeError::ReplaceValidationFailed {
            snapshot_id,
            reason,
        } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::ReplaceValidationFailed {
                snapshot_id,
                reason,
            },
        },
        ChangeError::InternalInconsistency(detail) => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency { detail },
        },
        ChangeError::PrimaryKeyMissingFromBase { pk_col }
        | ChangeError::PrimaryKeyNullable { pk_col }
        | ChangeError::PrimaryKeyTypeUnsupported { pk_col, .. } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("CREATE-time primary key validation reached refresh path: {pk_col}"),
            },
        },
        ChangeError::PrimaryKeyValueNull { row_info } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("primary key value became NULL during refresh: {row_info}"),
            },
        },
        ChangeError::IcebergFormatUnsupported { format_version } => MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::InternalInconsistency {
                detail: format!("unsupported Iceberg format reached refresh path: {format_version}"),
            },
        },
    }
}
```

- [ ] **Step 3: Add unit tests**

Append tests in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_is_full_for_initial_refresh() {
        assert_eq!(
            choose_snapshot_refresh_policy(None, Some(10)).expect("policy"),
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(10),
                reason: FullRefreshReason::InitialRefresh,
            }
        );
    }

    #[test]
    fn policy_is_noop_for_same_snapshot() {
        assert_eq!(
            choose_snapshot_refresh_policy(Some(10), Some(10)).expect("policy"),
            MvRefreshPolicy::NoOp {
                current_snapshot_id: 10,
            }
        );
    }

    #[test]
    fn policy_is_incremental_for_advanced_snapshot() {
        assert_eq!(
            choose_snapshot_refresh_policy(Some(10), Some(12)).expect("policy"),
            MvRefreshPolicy::Incremental {
                previous_snapshot_id: 10,
                current_snapshot_id: 12,
            }
        );
    }

    #[test]
    fn overwrite_error_maps_to_full_refresh() {
        assert_eq!(
            policy_from_change_error(ChangeError::UnsupportedOperation {
                snapshot_id: 22,
                op: "overwrite".to_string(),
            }),
            MvRefreshPolicy::FullRefresh {
                target_snapshot_id: Some(22),
                reason: FullRefreshReason::InsertOverwrite { snapshot_id: 22 },
            }
        );
    }

    #[test]
    fn equality_delete_error_maps_to_unsupported() {
        assert_eq!(
            policy_from_change_error(ChangeError::EqualityDeleteUnsupported { snapshot_id: 33 }),
            MvRefreshPolicy::Unsupported {
                reason: UnsupportedRefreshReason::EqualityDelete { snapshot_id: 33 },
            }
        );
    }
}
```

- [ ] **Step 4: Run strategy tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_strategy::tests -- --nocapture
```

Expected:

```text
test result: ok. 5 passed
```

- [ ] **Step 5: Commit R1 strategy skeleton**

Run:

```bash
git add src/connector/starrocks/managed/mod.rs src/connector/starrocks/managed/mv_refresh_strategy.rs
git commit -m "Add MV refresh strategy policy types"
```

### Task 3: R1 Route Refresh Through Policy and Add Overwrite Full Refresh

**Files:**
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`
- Test: `src/connector/starrocks/managed/mv_refresh.rs`

- [ ] **Step 1: Make change planning return typed errors**

Change `plan_iceberg_change_batch_for_ivm` in `src/connector/starrocks/managed/ivm_change_stream.rs` from `Result<IcebergChangeBatch, String>` to:

```rust
pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, crate::connector::iceberg::changes::ChangeError> {
    let batch = plan_changes(base_table, previous_snapshot_id, pk_columns)?;
    validate_change_batch_current_snapshot(&batch, expected_current_snapshot_id).map_err(
        crate::connector::iceberg::changes::ChangeError::InternalInconsistency,
    )?;
    Ok(batch)
}
```

At call sites that still need `String`, convert with `map_err(|e| e.to_string())`.

- [ ] **Step 2: Dispatch overwrite policy to full refresh**

In `src/connector/starrocks/managed/mv_refresh.rs`, import:

```rust
use super::mv_refresh_strategy::{
    MvRefreshPolicy, choose_snapshot_refresh_policy, policy_from_change_error,
};
```

Replace the initial `choose_refresh_strategy(previous_snapshot_id, current_snapshot_id)?` call with `choose_snapshot_refresh_policy`. In the incremental branch, handle planner errors:

```rust
let batch = match plan_iceberg_change_batch_for_ivm(
    &loaded.table,
    previous_snapshot_id,
    current_snapshot_id,
    &mv_row.primary_key_columns,
) {
    Ok(batch) => batch,
    Err(err) => match policy_from_change_error(err) {
        MvRefreshPolicy::FullRefresh { .. } => {
            return refresh_mv_full_with_executor(
                state,
                &db_name,
                &mv_name,
                run_mv_select_and_chunks,
            );
        }
        MvRefreshPolicy::Unsupported { reason } => {
            return Err(format!("iceberg materialized view refresh unsupported: {reason:?}"));
        }
        other => {
            return Err(format!(
                "iceberg materialized view refresh produced invalid policy from change planner: {other:?}"
            ));
        }
    },
};
```

For aggregate MV, route `FullRefresh` to `refresh_aggregate_mv_full(state, &db_name, &mv_name, shape)`.

- [ ] **Step 3: Add overwrite fallback tests**

Add two focused tests in `src/connector/starrocks/managed/mv_refresh.rs`:

```rust
#[test]
fn overwrite_change_error_routes_projection_mv_to_full_refresh_policy() {
    use super::mv_refresh_strategy::{policy_from_change_error, FullRefreshReason, MvRefreshPolicy};
    let policy = policy_from_change_error(crate::connector::iceberg::changes::ChangeError::UnsupportedOperation {
        snapshot_id: 99,
        op: "overwrite".to_string(),
    });
    assert_eq!(
        policy,
        MvRefreshPolicy::FullRefresh {
            target_snapshot_id: Some(99),
            reason: FullRefreshReason::InsertOverwrite { snapshot_id: 99 },
        }
    );
}

#[test]
fn equality_delete_change_error_stays_unsupported_policy() {
    use super::mv_refresh_strategy::{policy_from_change_error, MvRefreshPolicy, UnsupportedRefreshReason};
    let policy = policy_from_change_error(
        crate::connector::iceberg::changes::ChangeError::EqualityDeleteUnsupported { snapshot_id: 101 },
    );
    assert_eq!(
        policy,
        MvRefreshPolicy::Unsupported {
            reason: UnsupportedRefreshReason::EqualityDelete { snapshot_id: 101 },
        }
    );
}
```

- [ ] **Step 4: Run R1 tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_refresh_strategy::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh::tests::overwrite_change_error_routes_projection_mv_to_full_refresh_policy -- --exact
cargo test --lib connector::starrocks::managed::mv_refresh::tests::equality_delete_change_error_stays_unsupported_policy -- --exact
cargo test --lib connector::starrocks::managed::mv_refresh::tests -- --nocapture
```

Expected:

```text
test result: ok
```

- [ ] **Step 5: Commit R1 routing**

Run:

```bash
git add src/connector/starrocks/managed/mv_refresh.rs src/connector/starrocks/managed/ivm_change_stream.rs
git commit -m "Route MV refresh through strategy policy"
```

### Task 4: R2 Row Identity Event Model

**Files:**
- Create: `src/connector/starrocks/managed/ivm_row_identity.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Modify: `src/connector/starrocks/managed/ivm_change_stream.rs`
- Test: `src/connector/starrocks/managed/ivm_row_identity.rs`

- [ ] **Step 1: Add module export**

In `src/connector/starrocks/managed/mod.rs`, add:

```rust
pub(crate) mod ivm_row_identity;
```

- [ ] **Step 2: Add identity/event types**

Create `src/connector/starrocks/managed/ivm_row_identity.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum BaseRowIdentity {
    IcebergRowId(i64),
    Position {
        file_path: String,
        pos: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BaseRowChangeKind {
    Insert,
    Delete,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseRowChange {
    pub(crate) identity: BaseRowIdentity,
    pub(crate) kind: BaseRowChangeKind,
}

pub(crate) fn normalize_insert_delete_pairs(
    inserts: impl IntoIterator<Item = BaseRowIdentity>,
    deletes: impl IntoIterator<Item = BaseRowIdentity>,
) -> Vec<BaseRowChange> {
    use std::collections::BTreeSet;

    let insert_set: BTreeSet<_> = inserts.into_iter().collect();
    let delete_set: BTreeSet<_> = deletes.into_iter().collect();
    let mut out = Vec::new();

    for identity in delete_set.intersection(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Update,
        });
    }
    for identity in delete_set.difference(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Delete,
        });
    }
    for identity in insert_set.difference(&delete_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Insert,
        });
    }
    out
}
```

- [ ] **Step 3: Add identity tests**

Append:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_row_id_insert_and_delete_becomes_update() {
        let changes = normalize_insert_delete_pairs(
            [BaseRowIdentity::IcebergRowId(7)],
            [BaseRowIdentity::IcebergRowId(7)],
        );
        assert_eq!(
            changes,
            vec![BaseRowChange {
                identity: BaseRowIdentity::IcebergRowId(7),
                kind: BaseRowChangeKind::Update,
            }]
        );
    }

    #[test]
    fn different_row_ids_remain_insert_and_delete() {
        let changes = normalize_insert_delete_pairs(
            [BaseRowIdentity::IcebergRowId(8)],
            [BaseRowIdentity::IcebergRowId(7)],
        );
        assert_eq!(
            changes,
            vec![
                BaseRowChange {
                    identity: BaseRowIdentity::IcebergRowId(7),
                    kind: BaseRowChangeKind::Delete,
                },
                BaseRowChange {
                    identity: BaseRowIdentity::IcebergRowId(8),
                    kind: BaseRowChangeKind::Insert,
                },
            ]
        );
    }
}
```

- [ ] **Step 4: Run identity tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::ivm_row_identity::tests -- --nocapture
```

Expected:

```text
test result: ok. 2 passed
```

- [ ] **Step 5: Commit R2 model**

Run:

```bash
git add src/connector/starrocks/managed/mod.rs src/connector/starrocks/managed/ivm_row_identity.rs
git commit -m "Add base row identity model for Iceberg IVM"
```

### Task 5: R3 MV Apply Policy Matrix

**Files:**
- Create: `src/connector/starrocks/managed/mv_apply_policy.rs`
- Modify: `src/connector/starrocks/managed/mod.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`
- Test: `src/connector/starrocks/managed/mv_apply_policy.rs`

- [ ] **Step 1: Add module export**

In `src/connector/starrocks/managed/mod.rs`, add:

```rust
pub(crate) mod mv_apply_policy;
```

- [ ] **Step 2: Add apply policy matrix**

Create `src/connector/starrocks/managed/mv_apply_policy.rs`:

```rust
use super::mv_shape::{AggregateFunctionKind, AggregateMvShape, IncrementalMvShape};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvApplyPolicy {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn apply_policy_for_change(
    shape: &IncrementalMvShape,
    has_inserts: bool,
    has_deletes: bool,
    row_identity_available: bool,
) -> MvApplyPolicy {
    match shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            if has_deletes && !row_identity_available {
                MvApplyPolicy::Unsupported {
                    reason: "projection/filter MV DELETE requires base row identity".to_string(),
                }
            } else {
                MvApplyPolicy::Incremental
            }
        }
        IncrementalMvShape::Aggregate(aggregate) => aggregate_policy(
            aggregate,
            has_inserts,
            has_deletes,
        ),
    }
}

fn aggregate_policy(
    aggregate: &AggregateMvShape,
    _has_inserts: bool,
    has_deletes: bool,
) -> MvApplyPolicy {
    if has_deletes
        && aggregate
            .aggregates
            .iter()
            .any(|call| matches!(call.function, AggregateFunctionKind::Min | AggregateFunctionKind::Max))
    {
        return MvApplyPolicy::FullRefresh {
            reason: "MIN/MAX aggregate cannot retract DELETE state incrementally".to_string(),
        };
    }
    MvApplyPolicy::Incremental
}
```

- [ ] **Step 3: Add policy tests**

Add tests that build minimal shapes and verify:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::mv_shape::{
        AggregateCallShape, AggregateFunctionKind, AggregateInput, AggregateMvShape,
        IncrementalMvShape, ProjectionFilterMvShape,
    };

    fn projection_shape() -> IncrementalMvShape {
        IncrementalMvShape::ProjectionFilter(ProjectionFilterMvShape {
            base_table: sqlparser::ast::ObjectName {
                parts: vec!["ice".to_string(), "ns".to_string(), "orders".to_string()],
            },
        })
    }

    fn aggregate_shape(function: AggregateFunctionKind) -> IncrementalMvShape {
        IncrementalMvShape::Aggregate(AggregateMvShape {
            base_table: sqlparser::ast::ObjectName {
                parts: vec!["ice".to_string(), "ns".to_string(), "orders".to_string()],
            },
            group_keys: Vec::new(),
            aggregates: vec![AggregateCallShape {
                output_name: "a".to_string(),
                function,
                input: AggregateInput::Star,
            }],
            visible_outputs: Vec::new(),
        })
    }

    #[test]
    fn projection_delete_without_row_identity_is_unsupported() {
        assert_eq!(
            apply_policy_for_change(&projection_shape(), false, true, false),
            MvApplyPolicy::Unsupported {
                reason: "projection/filter MV DELETE requires base row identity".to_string(),
            }
        );
    }

    #[test]
    fn projection_delete_with_row_identity_is_incremental() {
        assert_eq!(
            apply_policy_for_change(&projection_shape(), false, true, true),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn sum_delete_is_incremental() {
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Sum),
                false,
                true,
                false
            ),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn max_delete_falls_back_to_full_refresh() {
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Max),
                false,
                true,
                false
            ),
            MvApplyPolicy::FullRefresh {
                reason: "MIN/MAX aggregate cannot retract DELETE state incrementally".to_string(),
            }
        );
    }
}
```

- [ ] **Step 4: Route aggregate MIN/MAX fallback through policy**

In `mv_refresh.rs`, replace the local `layout_has_min_or_max` branch with `apply_policy_for_change`. Keep the same behavior:

- `MvApplyPolicy::Incremental` continues current merge path.
- `MvApplyPolicy::FullRefresh { reason }` calls `refresh_aggregate_mv_full`.
- `MvApplyPolicy::Unsupported { reason }` returns `Err(reason)`.

- [ ] **Step 5: Run R3 tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_apply_policy::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh::tests -- --nocapture
```

Expected:

```text
test result: ok
```

- [ ] **Step 6: Commit R3 policy**

Run:

```bash
git add src/connector/starrocks/managed/mod.rs src/connector/starrocks/managed/mv_apply_policy.rs src/connector/starrocks/managed/mv_refresh.rs
git commit -m "Add MV apply policy matrix"
```

### Task 6: R4 Iceberg Evolution Matrix

**Files:**
- Modify: `src/connector/iceberg/changes.rs`
- Modify: `src/connector/starrocks/managed/mv_refresh_strategy.rs`
- Test: `src/connector/iceberg/changes.rs`
- Test: `src/connector/starrocks/managed/mv_refresh_strategy.rs`

- [ ] **Step 1: Add explicit evolution reason types**

In `src/connector/iceberg/changes.rs`, add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IcebergChangePolicySignal {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}
```

Add a converter:

```rust
pub(crate) fn policy_signal_from_change_error(err: &ChangeError) -> IcebergChangePolicySignal {
    match err {
        ChangeError::UnsupportedOperation { op, .. } if op == "overwrite" => {
            IcebergChangePolicySignal::FullRefresh {
                reason: "insert overwrite requires full refresh".to_string(),
            }
        }
        ChangeError::LineageBroken { .. } => IcebergChangePolicySignal::FullRefresh {
            reason: "previous snapshot is not reachable".to_string(),
        },
        ChangeError::EqualityDeleteUnsupported { .. } => IcebergChangePolicySignal::Unsupported {
            reason: "equality delete is not supported by IVM".to_string(),
        },
        ChangeError::SchemaEvolutionUnsupported { detail } => IcebergChangePolicySignal::Unsupported {
            reason: format!("schema evolution is not supported by IVM: {detail}"),
        },
        ChangeError::ReplaceValidationFailed { reason, .. } => IcebergChangePolicySignal::Unsupported {
            reason: format!("replace snapshot is not a safe compaction: {reason}"),
        },
        other => IcebergChangePolicySignal::Unsupported {
            reason: other.to_string(),
        },
    }
}
```

- [ ] **Step 2: Add evolution classification tests**

Add tests in `changes.rs`:

```rust
#[test]
fn overwrite_error_policy_signal_is_full_refresh() {
    let err = ChangeError::UnsupportedOperation {
        snapshot_id: 1,
        op: "overwrite".to_string(),
    };
    assert_eq!(
        policy_signal_from_change_error(&err),
        IcebergChangePolicySignal::FullRefresh {
            reason: "insert overwrite requires full refresh".to_string(),
        }
    );
}

#[test]
fn equality_delete_policy_signal_is_unsupported() {
    let err = ChangeError::EqualityDeleteUnsupported { snapshot_id: 2 };
    assert_eq!(
        policy_signal_from_change_error(&err),
        IcebergChangePolicySignal::Unsupported {
            reason: "equality delete is not supported by IVM".to_string(),
        }
    );
}
```

- [ ] **Step 3: Wire strategy through policy signal**

Update `mv_refresh_strategy.rs::policy_from_change_error` to call `policy_signal_from_change_error(&err)` first, then map:

- `FullRefresh` to `MvRefreshPolicy::FullRefresh`.
- `Unsupported` to `MvRefreshPolicy::Unsupported`.
- `Incremental` is invalid for an error path and returns `UnsupportedRefreshReason::InternalInconsistency`.

- [ ] **Step 4: Run R4 tests**

Run:

```bash
cargo test --lib connector::iceberg::changes::tests -- --nocapture
cargo test --lib connector::starrocks::managed::mv_refresh_strategy::tests -- --nocapture
```

Expected:

```text
test result: ok
```

- [ ] **Step 5: Commit R4 classification**

Run:

```bash
git add src/connector/iceberg/changes.rs src/connector/starrocks/managed/mv_refresh_strategy.rs
git commit -m "Classify Iceberg MV refresh evolution policy"
```

### Task 7: R5 SQL-Test and Production Gate Matrix

**Files:**
- Create: `sql-tests/mv-on-iceberg/sql/managed_lake_mv_iceberg_ivm_strategy.sql`
- Create: `sql-tests/mv-on-iceberg/result/managed_lake_mv_iceberg_ivm_strategy.result`
- Modify: `src/connector/starrocks/managed/mv_refresh.rs`

- [ ] **Step 1: Add strategy SQL case**

Create `sql-tests/mv-on-iceberg/sql/managed_lake_mv_iceberg_ivm_strategy.sql`:

```sql
-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,strategy
-- Test Objective:
-- 1. Validate aggregate MV full refresh over a v3 row-lineage Iceberg base table.
-- 2. Validate append snapshots refresh incrementally.
-- 3. Validate INSERT OVERWRITE refresh falls back to full refresh and advances metadata.

-- query 1
-- @skip_result_check=true
-- Use a local-FS Iceberg warehouse here because S3-backed Iceberg
-- INSERT OVERWRITE abort cleanup is not wired through the standalone SQL path yet.
CREATE EXTERNAL CATALOG mv_strategy_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "file:///tmp/novarocks-mv-strategy-${uuid0}"
);
CREATE DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
CREATE TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20),
  (3, 'B', 30);
CREATE MATERIALIZED VIEW ${case_db}.orders_strategy_mv
DISTRIBUTED BY HASH(customer) BUCKETS 2
AS SELECT
  customer,
  COUNT(*) AS c,
  SUM(amount) AS s
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 3
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 4
-- @skip_result_check=true
INSERT INTO mv_strategy_ice_${uuid0}.ns_${uuid0}.orders VALUES
  (4, 'A', 100);

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 6
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 7
-- @skip_result_check=true
INSERT OVERWRITE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
SELECT id, customer, amount + 100
FROM mv_strategy_ice_${uuid0}.ns_${uuid0}.orders
WHERE id >= 2;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ${case_db}.orders_strategy_mv;

-- query 9
SELECT customer, c, s
FROM ${case_db}.orders_strategy_mv
ORDER BY customer;

-- query 10
-- @skip_result_check=true
DROP MATERIALIZED VIEW ${case_db}.orders_strategy_mv;
DROP TABLE mv_strategy_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_strategy_ice_${uuid0}.ns_${uuid0};
DROP CATALOG mv_strategy_ice_${uuid0};
```

Create `sql-tests/mv-on-iceberg/result/managed_lake_mv_iceberg_ivm_strategy.result` with the expected visible query output:

```text
-- query 3
customer	c	s
A	2	30
B	1	30

-- query 6
customer	c	s
A	3	130
B	1	30

-- query 9
customer	c	s
A	2	320
B	1	130
```

- [ ] **Step 2: Record the SQL case**

Run with the managed-lake config:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg --only managed_lake_mv_iceberg_ivm_strategy --mode record --query-timeout 60
```

Expected:

```text
fail=0
```

- [ ] **Step 3: Verify the SQL case**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg --only managed_lake_mv_iceberg_ivm_strategy --mode verify --query-timeout 60
```

Expected:

```text
fail=0
```

- [ ] **Step 4: Add refresh policy logging**

In `src/connector/starrocks/managed/mv_refresh.rs`, log one structured line after policy selection:

```rust
tracing::info!(
    target: "mv_refresh",
    mv = %format!("{}.{}", db_name, mv_name),
    base = %base_ref.fqn(),
    previous_snapshot_id = ?previous_snapshot_id,
    current_snapshot_id = ?current_snapshot_id,
    policy = ?policy,
    "selected materialized view refresh policy"
);
```

- [ ] **Step 5: Run final gate**

Run:

```bash
cargo fmt
cargo test --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite mv-on-iceberg --only managed_lake_mv_iceberg_ivm_strategy --mode verify --query-timeout 60
git diff --check
```

Expected:

```text
test result: ok
fail=0
```

`git diff --check` exits 0.

- [ ] **Step 6: Commit R5 gate**

Run:

```bash
git add sql-tests/mv-on-iceberg src/connector/starrocks/managed/mv_refresh.rs docs/design/plans/2026-05-03-ivm-on-iceberg-roadmap.md
git commit -m "Add IVM on Iceberg strategy SQL gate"
```

## Execution Order

Recommended branch sequence:

1. `codex/iceberg-ivm-r0-baseline`
2. `codex/iceberg-ivm-r1-refresh-strategy`
3. `codex/iceberg-ivm-r2-row-identity`
4. `codex/iceberg-ivm-r3-apply-policy`
5. `codex/iceberg-ivm-r4-evolution-policy`
6. `codex/iceberg-ivm-r5-production-gates`

Each branch should be small enough to review independently and should land in order.

## Self-Review

- Spec coverage: R0 closes current object-store DELETE work; R1 covers strategy and overwrite fallback; R2 covers row identity; R3 covers MV apply behavior; R4 covers Iceberg evolution classification; R5 covers SQL-test and production gate.
- Completeness scan: every task names exact files, commands, and expected outputs.
- Type consistency: `MvRefreshPolicy`, `FullRefreshReason`, `UnsupportedRefreshReason`, `BaseRowIdentity`, `BaseRowChange`, and `MvApplyPolicy` are introduced once and reused consistently.
