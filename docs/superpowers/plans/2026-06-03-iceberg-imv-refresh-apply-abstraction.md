# Iceberg IMV Refresh Contract Abstraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Derive a stable Iceberg IMV refresh contract from analyzed query structure and use that contract to drive unified refresh/apply orchestration.

**Architecture:** CREATE and REFRESH already parse/analyze MV SQL. Add an `ImvRefreshContract` deriver over `MvAnalysis.resolved_query`, persist/validate stable contract fields through existing MV metadata contracts, then route refresh through `RefreshStrategy` and a common driver. Do not persist full logical plans, and do not implement B-family execution in this task.

**Tech Stack:** Rust; `ResolvedQuery` / `QueryBody` / `Relation` structures under `src/sql/analysis/`; Iceberg MV lifecycle in `src/engine/mv/iceberg_refresh.rs`; MV metadata contracts in `src/meta/repository/mv_contract.rs`; existing Rust and SQL regression tests.

**Spec:** `docs/superpowers/specs/2026-06-03-iceberg-imv-v2-unified-delta-apply-engine-design.md`

---

## Scope Check

This plan covers one subsystem: Iceberg-backed IMV refresh contract derivation and physical refresh/apply orchestration. It intentionally does not implement B-family logical rewrite, branch-scoped aggregate row identity, PCT fallback, or CREATE-time trial rewrite for arbitrary unsupported SQL.

The important architectural boundary:

```text
CREATE/REFRESH SQL
  -> analyze into MvAnalysis.resolved_query
  -> derive_imv_refresh_contract
  -> ImvRefreshContract
  -> RefreshStrategy + IcebergMvRefreshDriver
```

`src/connector/starrocks/table/mv_shape.rs` can remain during migration, but it must stop being the refresh decision source by the end of this plan.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `src/engine/mv/refresh_contract.rs` | Derive `ImvRefreshContract` from analyzed query structure | Create |
| `src/engine/mv/refresh_driver.rs` | Snapshot decision policy, `ApplyKeyContract`, lifecycle helpers | Create |
| `src/engine/mv/mod.rs` | MV module registry | Add new modules |
| `src/engine/mv/iceberg_refresh.rs` | CREATE/REFRESH integration, driver migration | Modify |
| `src/meta/repository/mv_contract.rs` | Existing stable schema/apply/branch/aggregate contract definitions | Reference only in this plan |
| `src/connector/starrocks/table/mv_shape.rs` | Legacy classifier | Demote from refresh decision source; keep until callers are removed |

---

## Task 1: Add ImvRefreshContract Types and Derivation Tests

**Files:**
- Create: `src/engine/mv/refresh_contract.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Write failing derivation tests**

Create `src/engine/mv/refresh_contract.rs` with the tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::table::mv_ddl::analyze_mv_select;
    use crate::sql::parser::parse_query;

    fn derive_for_sql(sql: &str) -> Result<ImvRefreshContract, String> {
        let env = crate::engine::mv::iceberg_refresh::tests::open_test_state_with_hadoop_iceberg_catalog(
            "ice",
            "sales",
        );
        crate::engine::mv::iceberg_refresh::tests::create_aggregate_fact_table(
            &env.state,
            "ice",
            "sales",
            "t1",
        );
        crate::engine::mv::iceberg_refresh::tests::create_aggregate_fact_table(
            &env.state,
            "ice",
            "sales",
            "t2",
        );
        let query = parse_query(sql)?;
        let analysis = analyze_mv_select(&env.state, Some("ice"), "sales", &query)?;
        derive_imv_refresh_contract(&analysis)
    }

    #[test]
    fn derives_single_aggregate_contract_from_analyzed_query() {
        let contract = derive_for_sql(
            "select region, count(*) as c from ice.sales.t1 group by region",
        )
        .expect("derive contract");
        assert_eq!(contract.strategy, RefreshStrategy::SingleAggregate);
        assert_eq!(contract.base_refs.len(), 1);
        assert!(contract.aggregate.is_some());
        assert_eq!(contract.apply_key, ApplyKeyContract::aggregate_group_row());
    }

    #[test]
    fn derives_fan_in_aggregate_contract_from_aggregate_over_union() {
        let contract = derive_for_sql(
            "select region, count(*) as c from (
               select region from ice.sales.t1
               union all
               select region from ice.sales.t2
             ) u group by region",
        )
        .expect("derive contract");
        assert_eq!(contract.strategy, RefreshStrategy::FanInAggregate);
        assert_eq!(contract.base_refs.len(), 2);
        assert!(contract.aggregate.is_some());
        assert!(contract.branch.is_some());
    }

    #[test]
    fn recognizes_b_family_but_keeps_it_unsupported() {
        let contract = derive_for_sql(
            "select region, count(*) as c from ice.sales.t1 group by region
             union all
             select region, count(*) as c from ice.sales.t2 group by region",
        )
        .expect("derive contract");
        assert_eq!(
            contract.strategy,
            RefreshStrategy::UnsupportedBranchUnionAggregate
        );
        assert!(contract.branch.is_some());
    }
}
```

Place these tests in `src/engine/mv/iceberg_refresh.rs` if the existing Iceberg
MV test fixtures are private. Keep `derive_imv_refresh_contract` public within
`pub(crate)` module boundaries and do not change test-fixture visibility.

- [ ] **Step 2: Export the module and run the failing tests**

Modify `src/engine/mv/mod.rs`:

```rust
pub(crate) mod refresh_contract;
```

Run:

```bash
cargo test --lib derives_single_aggregate_contract_from_analyzed_query -- --nocapture
```

Expected: compile fails because `ImvRefreshContract`, `RefreshStrategy`,
`ApplyKeyContract`, and `derive_imv_refresh_contract` do not exist.

- [ ] **Step 3: Add contract types**

Add to `src/engine/mv/refresh_contract.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshStrategy {
    ProjectionFilter,
    JoinProjectionFilter,
    UnionProjectionFilter,
    SingleAggregate,
    FanInAggregate,
    JoinAggregate,
    UnsupportedBranchUnionAggregate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApplyKeyContract {
    pub(crate) column_name: &'static str,
    pub(crate) value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType,
    pub(crate) rewrite_evidence: RewriteEvidence,
    pub(crate) allow_full_rebuild_on_policy_full_refresh: bool,
    pub(crate) preload_locator_for_change_stream_deletes: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvRefreshContract {
    pub(crate) strategy: RefreshStrategy,
    pub(crate) base_refs: Vec<crate::connector::starrocks::table::model::IcebergTableRef>,
    pub(crate) apply_key: ApplyKeyContract,
    pub(crate) aggregate: Option<AggregateRefreshContract>,
    pub(crate) join: Option<JoinRefreshContract>,
    pub(crate) branch: Option<BranchRefreshContract>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AggregateRefreshContract {
    pub(crate) group_key_count: usize,
    pub(crate) aggregate_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshContract {
    pub(crate) join_key_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchRefreshContract {
    pub(crate) branch_count: usize,
}
```

Add predefined apply keys:

```rust
impl ApplyKeyContract {
    pub(crate) fn projection_filter() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Int64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: true,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn union_projection_filter() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::BranchInt64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn aggregate_group_row() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::Aggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }

    pub(crate) fn join_aggregate_group_row() -> Self {
        Self {
            column_name: crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN,
            value_type: crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::JoinAggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }
}
```

- [ ] **Step 4: Implement minimal derivation over analyzed query**

Implement `derive_imv_refresh_contract` using `MvAnalysis.resolved_query`.

```rust
pub(crate) fn derive_imv_refresh_contract(
    analysis: &crate::connector::starrocks::table::mv_ddl::MvAnalysis,
) -> Result<ImvRefreshContract, String> {
    let base_refs = analysis
        .resolved_refs
        .iter()
        .map(iceberg_ref_from_resolved)
        .collect::<Result<Vec<_>, _>>()?;
    let derived = derive_from_query_body(&analysis.resolved_query.body)?;
    Ok(derived.into_contract(base_refs))
}
```

Implement `iceberg_ref_from_resolved` so non-Iceberg refs fail-fast:

```rust
fn iceberg_ref_from_resolved(
    resolved: &crate::connector::starrocks::table::mv_ddl::ResolvedTableRef,
) -> Result<crate::connector::starrocks::table::model::IcebergTableRef, String> {
    match resolved {
        crate::connector::starrocks::table::mv_ddl::ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } => Ok(crate::connector::starrocks::table::model::IcebergTableRef {
            catalog: catalog.clone(),
            namespace: namespace.clone(),
            table: table.clone(),
        }),
        _ => Err("Iceberg IMV refresh contract requires Iceberg base tables".to_string()),
    }
}
```

Use a private `DerivedStructure` enum for recursion over `QueryBody` / `Relation`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum DerivedStructure {
    ProjectionFilter,
    JoinProjection { join_key_count: usize },
    UnionProjection { branch_count: usize },
    SingleAggregate { group_key_count: usize, aggregate_count: usize },
    FanInAggregate { branch_count: usize, group_key_count: usize, aggregate_count: usize },
    JoinAggregate { join_key_count: usize, group_key_count: usize, aggregate_count: usize },
    BranchUnionAggregate { branch_count: usize },
}
```

Derive from `MvAnalysis.resolved_query` using `ResolvedQuery`, `QueryBody`, and
`Relation`. Do not call `classify_incremental_mv_query` from this module.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test --lib derives_single_aggregate_contract_from_analyzed_query derives_fan_in_aggregate_contract_from_aggregate_over_union recognizes_b_family_but_keeps_it_unsupported -- --nocapture
cargo fmt
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/mod.rs src/engine/mv/refresh_contract.rs
git commit -m "Add Iceberg IMV refresh contract derivation"
```

---

## Task 2: Add Snapshot Decision and Lifecycle Primitives

**Files:**
- Create: `src/engine/mv/refresh_driver.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Add snapshot decision tests**

Create `src/engine/mv/refresh_driver.rs` with tests for:

```rust
#[test]
fn all_bases_required_partial_initial_current_fails() {
    let decision = decide_refresh(
        BaseSnapshotPolicy::AllBasesRequired,
        &[
            BaseSnapshotStatus::new("ice.db.t1", None, Some(10)),
            BaseSnapshotStatus::new("ice.db.t2", None, None),
        ],
        "fan-in aggregate",
    );
    assert!(matches!(decision, RefreshDecision::FailFast { .. }));
}

#[test]
fn join_pair_partial_initial_current_skips() {
    let decision = decide_refresh(
        BaseSnapshotPolicy::JoinPairPartialInitialSkip,
        &[
            BaseSnapshotStatus::new("ice.db.left", None, Some(10)),
            BaseSnapshotStatus::new("ice.db.right", None, None),
        ],
        "join aggregate",
    );
    assert_eq!(decision, RefreshDecision::SkipEmpty);
}
```

- [ ] **Step 2: Implement policy primitives**

Add:

```rust
pub(crate) enum BaseSnapshotPolicy {
    SingleBase,
    AllBasesRequired,
    JoinPairPartialInitialSkip,
}

pub(crate) struct BaseSnapshotStatus {
    pub(crate) fqn: String,
    pub(crate) previous_snapshot_id: Option<i64>,
    pub(crate) current_snapshot_id_before_pin: Option<i64>,
}

pub(crate) enum RefreshDecision {
    SkipEmpty,
    FirstRefresh,
    MetadataOnly,
    Incremental,
    FailFast { reason: String },
}
```

Implement `decide_refresh` with current behavior:

- single base: empty skip, previous+missing current fail-fast;
- all bases required: all empty skip, partial initial current fail-fast;
- join pair: partial initial current skip;
- unchanged metadata-only;
- changed incremental.

- [ ] **Step 3: Run tests and commit**

```bash
cargo test --lib engine::mv::refresh_driver::tests -- --nocapture
cargo fmt
git add src/engine/mv/mod.rs src/engine/mv/refresh_driver.rs
git commit -m "Add Iceberg IMV refresh decision primitives"
```

---

## Task 3: Integrate Contract Derivation into CREATE MV

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add CREATE-time unsupported B-family test**

Add a test that creates B-family MV and expects a clear unsupported error:

```rust
#[test]
fn create_b_family_union_aggregate_reports_refresh_contract_unsupported() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "sales");
    create_aggregate_fact_table(&env.state, "ice", "sales", "t1");
    create_aggregate_fact_table(&env.state, "ice", "sales", "t2");
    let err = create_iceberg_mv_sql(
        &env.state,
        "ice",
        "sales",
        "create materialized view mv_b storage_engine='iceberg' as
         select region, count(*) as c from ice.sales.t1 group by region
         union all
         select region, count(*) as c from ice.sales.t2 group by region",
    )
    .expect_err("B-family is contract-recognized but execution-unsupported");
    assert!(err.contains("UNION ALL of aggregate branches"));
}
```

Use existing create-MV test helpers in `iceberg_refresh.rs`; keep the error text
stable and explicit.

- [ ] **Step 2: Derive contract in `create_iceberg_mv`**

After `analyze_mv_select(...)` returns `MvAnalysis`, call:

```rust
let refresh_contract =
    crate::engine::mv::refresh_contract::derive_imv_refresh_contract(&analysis)?;
if refresh_contract.strategy
    == crate::engine::mv::refresh_contract::RefreshStrategy::UnsupportedBranchUnionAggregate
{
    return Err(
        "Iceberg MV UNION ALL of aggregate branches is recognized but refresh execution is not supported in this build"
            .to_string(),
    );
}
```

For this task, continue building schema contract through existing functions. The
new contract is a CREATE-time guard and evidence source; persistence is Task 4.

- [ ] **Step 3: Run tests and commit**

```bash
cargo test --lib create_b_family_union_aggregate_reports_refresh_contract_unsupported -- --nocapture
cargo fmt
git add src/engine/mv/iceberg_refresh.rs
git commit -m "Use IMV refresh contract derivation during create"
```

---

## Task 4: Replace Refresh Strategy Selection

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add refresh strategy selection tests**

Add focused tests that derive contract for existing supported SQL and assert the
strategy used by refresh path:

```rust
#[test]
fn refresh_contract_selects_fan_in_aggregate_for_a_family() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "sales");
    create_aggregate_fact_table(&env.state, "ice", "sales", "t1");
    create_aggregate_fact_table(&env.state, "ice", "sales", "t2");
    let query = parse_mv_select_query(
        "select region, count(*) as c from (
           select region from ice.sales.t1
           union all
           select region from ice.sales.t2
         ) u group by region",
    )
    .expect("parse");
    let analysis = analyze_mv_select(&env.state, Some("ice"), "sales", &query).expect("analyze");
    let contract =
        crate::engine::mv::refresh_contract::derive_imv_refresh_contract(&analysis)
            .expect("derive");
    assert_eq!(
        contract.strategy,
        crate::engine::mv::refresh_contract::RefreshStrategy::FanInAggregate
    );
}
```

- [ ] **Step 2: Stop using `classify_incremental_mv_query` as refresh decision source**

In `refresh_iceberg_mv_with_planned_partitions`, replace:

```rust
let shape = classify_incremental_mv_query(&canonical_select_query)?;
```

with:

```rust
let analysis = analyze_mv_select(
    state,
    current_catalog,
    current_database,
    &parse_mv_select_query(&mv_definition.select_sql)?,
)?;
let refresh_contract =
    crate::engine::mv::refresh_contract::derive_imv_refresh_contract(&analysis)?;
```

Then dispatch on `refresh_contract.strategy`. Keep temporary calls to legacy shape
classifier only where an existing helper still needs `AggregateMvShape` or
join-shape data. Mark those call sites as temporary with a precise comment:

```rust
// Temporary: the refresh strategy is contract-derived. This legacy shape is
// still used only to feed the existing first-refresh/layout helper.
```

- [ ] **Step 3: Run tests and commit**

```bash
cargo test --lib refresh_contract_selects_fan_in_aggregate_for_a_family -- --nocapture
cargo test --lib iceberg_refresh -- --nocapture
cargo fmt
git add src/engine/mv/iceberg_refresh.rs
git commit -m "Drive Iceberg MV refresh strategy from derived contract"
```

---

## Task 5: Cut Over ApplyKeyContract

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/refresh_contract.rs`

- [ ] **Step 1: Replace `RewriteMergeRefreshOptions` fields**

Change:

```rust
struct RewriteMergeRefreshOptions {
    apply_key_column: &'static str,
    apply_key_value_type: ApplyKeyValueType,
    allow_full_rebuild_on_policy_full_refresh: bool,
    rewrite_evidence: RewriteMergeRefreshEvidence,
    preload_locator_for_change_stream_deletes: bool,
}
```

to:

```rust
struct RewriteMergeRefreshOptions {
    apply_key: crate::engine::mv::refresh_contract::ApplyKeyContract,
}
```

Update all reads inside `incremental_refresh_iceberg_mv_with_changes` to use
`options.apply_key`.

- [ ] **Step 2: Use contract apply key at call sites**

Where refresh dispatch has `refresh_contract`, pass:

```rust
RewriteMergeRefreshOptions {
    apply_key: refresh_contract.apply_key,
}
```

For temporary helper paths without the contract in scope, call the predefined
constructors from `ApplyKeyContract`.

- [ ] **Step 3: Run validation tests and commit**

```bash
cargo test --lib aggregate_refresh_rewrite_validation_tests -- --nocapture
cargo test --lib iceberg_refresh -- --nocapture
cargo fmt
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/refresh_contract.rs
git commit -m "Use derived apply key contract for Iceberg MV refresh"
```

---

## Task 6: Extract Refresh Driver Lifecycle

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/refresh_driver.rs`

- [ ] **Step 1: Add lifecycle routing tests**

Add tests for first / metadata-only / incremental closure dispatch in
`refresh_driver.rs`.

- [ ] **Step 2: Add lifecycle helper**

Implement:

```rust
pub(crate) struct IcebergMvRefreshLifecycle;

impl IcebergMvRefreshLifecycle {
    pub(crate) fn run(
        decision: RefreshDecision,
        first_refresh: impl FnOnce() -> Result<StatementResult, String>,
        metadata_only: impl FnOnce() -> Result<StatementResult, String>,
        incremental: impl FnOnce() -> Result<StatementResult, String>,
    ) -> Result<StatementResult, String> {
        match decision {
            RefreshDecision::SkipEmpty => Ok(StatementResult::Ok),
            RefreshDecision::FirstRefresh => first_refresh(),
            RefreshDecision::MetadataOnly => metadata_only(),
            RefreshDecision::Incremental => incremental(),
            RefreshDecision::FailFast { reason } => Err(reason),
        }
    }
}
```

- [ ] **Step 3: Migrate paths gradually**

For each existing refresh path, replace local first/metadata/incremental branching
with `IcebergMvRefreshLifecycle::run`, preserving current first-refresh helper
and incremental apply body.

Order:

1. projection/filter;
2. UNION projection/filter;
3. single aggregate;
4. fan-in aggregate;
5. join aggregate;
6. join projection/filter.

- [ ] **Step 4: Run tests and commit**

```bash
cargo test --lib iceberg_refresh -- --nocapture
cargo fmt
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/refresh_driver.rs
git commit -m "Extract Iceberg MV refresh lifecycle driver"
```

---

## Task 7: Final Regression

**Files:**
- No planned code changes in this task. If verification finds a regression, fix
  the regression in the affected implementation task before continuing.

- [ ] **Step 1: Run Rust checks**

```bash
cargo fmt
cargo clippy --lib
cargo test --lib engine::mv::refresh_contract -- --nocapture
cargo test --lib engine::mv::refresh_driver -- --nocapture
cargo test --lib iceberg_refresh -- --nocapture
```

Expected: all pass.

- [ ] **Step 2: Run Iceberg IMV SQL suite**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --profile dev-opt
LOG=/tmp/novarocks-imv-refresh-contract-server.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  if grep -q '^NOVAROCKS_READY ' "$LOG"; then break; fi
  if ! kill -0 "$SRV_PID" 2>/dev/null; then tail -40 "$LOG" >&2; exit 1; fi
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { tail -40 "$LOG" >&2; kill -9 "$SRV_PID"; exit 1; }
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
kill "$SRV_PID"
wait "$SRV_PID" 2>/dev/null || true
```

Expected: suite passes. B-family ignored Rust test remains ignored.

---

## Execution Notes

- Do not persist full logical plans.
- Do not use raw AST classifier as the final refresh decision source.
- Keep `src/connector/starrocks/table/mv_shape.rs` until all legacy helper dependencies are removed; demotion is enough for this PR.
- Do not implement B-family execution in this task.
- Use `MvAnalysis.resolved_query` as the derivation input. Do not reintroduce
  raw SQL shape classification as the refresh decision source.
