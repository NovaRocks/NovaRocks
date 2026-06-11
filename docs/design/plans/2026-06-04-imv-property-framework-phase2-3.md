# IMV Property Framework — Phase 2 + 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the IMV refresh shape enums (`RefreshStrategy`, Iceberg-path `IncrementalMvShape`) with a single **capability property algebra**: CREATE derives the contract by a recursive structural pass that synthesizes `TargetIdentity`/`StateContract`; REFRESH dispatches the driver on capabilities read off the persisted contract. End-to-end refresh of composed shapes (e.g. `UNION ALL` of aggregate-over-join) becomes supported; `RefreshStrategy` is deleted.

**Architecture:** One property algebra module is the single source of identity/state truth (no second classifier — decision below). At CREATE (`iceberg_refresh.rs::create_iceberg_mv`) a recursive pass over `analysis.resolved_query` produces a `RefreshFragmentProperty`; that property drives target-column construction and `build_iceberg_mv_schema_contract`, replacing the flat `DerivedStructure`/`into_contract` classifier. The persisted `MvSchemaContract`, its serde/Avro persistence, the refresh-side load, and the target-schema builders are **already decoupled** from the classifier and are reused unchanged. At REFRESH the driver dispatches on capabilities derived from the loaded contract's sub-contract presence (the same `(join, aggregate, branch).is_some()` discriminant `stored_refresh_strategy_for_plan` already reads) collapsed to **3 snapshot-policy paths × {has-agg-state}**; `RefreshStrategy` and the Iceberg-path `IncrementalMvShape` usages + cross-checks are deleted.

**Tech Stack:** Rust. New module `src/engine/mv/refresh_property.rs`. Touches `src/engine/mv/refresh_contract.rs`, `src/engine/mv/iceberg_refresh.rs` (CREATE + dispatch + builders), `src/engine/mv/refresh_context.rs`, `src/connector/starrocks/table/mv_shape.rs` (Iceberg callers only). Tests: `cargo test --lib`, `sql-tests` `iceberg-ivm` suite (standalone server + Iceberg REST/MinIO, env per CLAUDE.md §7.3).

**Canonical design spec:** [docs/design/specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md](../specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md) — §6 types, §7 per-operator derivation, §11 invariants, §12 difficulties, §15 phasing. This plan implements Phases 2 and 3 together. **Builds on Phase 1** (merged in PR #254): `RewriteBranchUnion` is already compositional; the `branch_scope` inherited marker exists.

---

## Scope Check & Locked Decisions

This is a deliberately combined 2+3 plan (user decision): doing them together avoids a transitional "new contract + old enum dispatch + bridge" state, which the project's no-compat-shim policy disfavors. It is ONE plan but executes as task-sized commits with review gates (subagent-driven), not one giant commit.

**Decisions locked (do not re-litigate):**
- **Unified property algebra, NOT a second classifier** (user decision 2026-06-04): identity/state synthesis lives in ONE module (`refresh_property.rs`). CREATE runs a structural pass over the *resolved query* calling it (it needs no target table and no snapshot window, sidestepping the "rewrite runs at REFRESH" obstacle). The REFRESH rewrite rules' identity decisions are anti-drift-checked against the same algebra (Task A2 cross-check / Task B5).
- **Persisted `MvSchemaContract` + its serde/Avro persistence (`meta/repository/mv.rs`) + refresh-load (`refresh_context.rs:612`) + target-schema builders (`iceberg_refresh.rs:1819-2037`) are REUSED unchanged.** They already consume the persisted shape / live target schema, not the classifier.
- **`RefreshStrategy` is deleted** (not kept as a bridge). CREATE column gating + REFRESH dispatch both move to property/capability. The reverse-derivation `stored_refresh_strategy_for_plan` (`iceberg_refresh.rs:4753`) and `stored_strategy_matches_legacy_shape` (4797) are deleted (reconciliation-only; only callers are each other + `plan_iceberg_aggregate_mv_refresh:5545`).
- **Iceberg-path `IncrementalMvShape` is retired (84 production lines: iceberg_refresh.rs 79 + refresh_context.rs 5).** The **StarRocks non-Iceberg path keeps it** (mv_ddl.rs / mv_refresh.rs / mv_apply_policy.rs); boundary is hard — `mv_ddl.rs::create_mv` rejects `storage_engine='iceberg'` at line 116. `classify_incremental_mv_query` + the inner `*MvShape` structs stay (used by StarRocks).
- **`refresh_driver.rs` (BaseSnapshotPolicy/RefreshDecision/closures) and the kernel `incremental_refresh_iceberg_mv_with_changes` (10063) are unchanged** — already the capability layer.

---

## File Structure

- **Create** `src/engine/mv/refresh_property.rs` — the property algebra: `TargetIdentity`, `StateContract`, `RefreshFragmentProperty`, `derive_fragment_property(&ResolvedQuery)`, and `RefreshCapabilities` (the refresh-time capability view derived from `MvSchemaContract`). Single source of identity/state truth.
- **Modify** `src/engine/mv/refresh_contract.rs` — `derive_imv_refresh_contract` produces the contract via `refresh_property` instead of `DerivedStructure`/`into_contract`; delete `DerivedStructure` + `into_contract` + `derive_from_*` once unused; **delete `RefreshStrategy`** (Part B).
- **Modify** `src/engine/mv/iceberg_refresh.rs` — CREATE: drive target columns + `build_iceberg_mv_schema_contract` from the property (Part A); REFRESH: replace the two `match strategy` dispatch sites with capability dispatch + collapse wrappers + delete reverse-derivation/cross-checks + retire Iceberg `IncrementalMvShape` callers (Part B).
- **Modify** `src/engine/mv/refresh_context.rs` — `aggregate_shape_and_layout_for_execution` reads layout from `AggregateStateContract` instead of re-classifying via `IncrementalMvShape`.
- **Tests** — unit in `refresh_property.rs` + `refresh_contract.rs`; pipeline/e2e in `iceberg-ivm` suite.

`refresh_property.rs` owns the algebra; `refresh_contract.rs` becomes a thin projection of it; `iceberg_refresh.rs` CREATE/REFRESH consume capabilities.

---

# PART A — Phase 2: Property algebra + CREATE-time derivation

### Task A1: The property algebra module + recursive synthesis

**Files:**
- Create: `src/engine/mv/refresh_property.rs`
- Modify: `src/engine/mv/mod.rs` (add `pub(crate) mod refresh_property;`)

- [ ] **Step 1: Write failing tests** for the per-operator synthesis (in `refresh_property.rs` tests). Cover: Scan→BaseRowId/Stateless; Aggregate(Scan)→GroupRowId/AggregateState; Join(Scan,Scan)→JoinRowKey/Stateless; UnionAll(Agg,Agg)→BranchScoped(GroupRowId); Project/Filter passthrough; and the composed `UnionAll(Agg(Join), Agg(Join))`→`BranchScoped(GroupRowId)` (homogeneous on property). Build `ResolvedQuery` fixtures mirroring those in `refresh_contract.rs` tests (reuse its helpers or `crate::sql::analysis` builders).

```rust
#[test]
fn union_of_aggregate_over_join_is_branch_scoped_group_row() {
    let q = resolved_union_all(vec![
        resolved_aggregate_over_join("t1", "t2"),
        resolved_aggregate_over_join("t3", "t4"),
    ]);
    let prop = derive_fragment_property(&q).expect("derive");
    assert!(matches!(prop.identity, TargetIdentity::BranchScoped(inner) if matches!(*inner, TargetIdentity::GroupRowId(_))));
    assert!(matches!(prop.state, StateContract::AggregateState { .. }));
    assert_eq!(prop.base_refs.len(), 4);
}
```

- [ ] **Step 2: Run, expect FAIL** (`derive_fragment_property` undefined). `cargo test --lib engine::mv::refresh_property -- --nocapture`.

- [ ] **Step 3: Define the types + synthesis.** Types per spec §6:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetIdentity {
    BaseRowId,
    JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>),
    GroupRowId(Vec<String>),                 // group key output names
    BranchScoped(Box<TargetIdentity>),        // flattened: BranchScoped(BranchScoped(x)) == BranchScoped(x)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StateContract {
    Stateless,
    AggregateState { group_key_count: usize, aggregate_count: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshFragmentProperty {
    pub identity: TargetIdentity,
    pub state: StateContract,
    pub base_refs: Vec<IcebergTableRef>,     // monoid: union of children
    pub branch_count: Option<usize>,         // Some(n) iff identity top is BranchScoped
}
```

`derive_fragment_property(&ResolvedQuery) -> Result<RefreshFragmentProperty, String>` is the recursive synthesis. **Reuse the existing structural validation logic from `refresh_contract.rs`** (`derive_from_relation` 405-439, `derive_from_select` 324-393, `derive_from_set_operation` 441-502, `collect_union_all_branches` 508) — it already rejects unsupported shapes (non-inner joins, non-UNION-ALL set ops, metadata/delta/CTE relations, DISTINCT). Port that walk to emit `RefreshFragmentProperty` per the §7 table:
- `Relation::Scan` → `{ BaseRowId, Stateless, [ref], None }`
- `Relation::Join(l,r)` inner-equi → `{ JoinRowKey(id(l), id(r)), compose-state, l.refs∪r.refs, None }`
- `Select` with aggregation over child → `{ GroupRowId(group_key_names), AggregateState{group_key_count, aggregate_count}, child.refs, child.branch_count }`
- `Select` projection/filter passthrough → child property
- `SetOp` UNION ALL of children → require all children's `(identity ctor, state ctor)` equal (homogeneity on property; reject otherwise with a precise error naming the divergent branch); → `{ BranchScoped(children[0].identity), children[0].state, ∪refs, Some(n) }`; flatten if a child is already BranchScoped.

- [ ] **Step 4: Run, expect PASS.** All A1 tests green.

- [ ] **Step 5: Commit.**
```bash
git add src/engine/mv/refresh_property.rs src/engine/mv/mod.rs
git commit -m "feat(imv): add refresh property algebra (TargetIdentity/StateContract synthesis)"
```

---

### Task A2: Derive `ImvRefreshContract` from the property (bridge + cross-check)

Make `derive_imv_refresh_contract` produce the contract via the property algebra, asserting parity with the legacy flat classifier for currently-supported shapes (so this step is behavior-preserving and proves the algebra subsumes the classifier).

**Files:** Modify `src/engine/mv/refresh_contract.rs` (~122-153).

- [ ] **Step 1: Write a parity test** (in `refresh_contract.rs` tests): for each currently-supported shape fixture already present (projection, join-projection, single-agg, fan-in, join-agg, branch-union), assert the property-derived `ImvRefreshContract` (apply_key, aggregate/join/branch presence + counts, base_refs) equals the legacy `into_contract`-derived one. (Keep `RefreshStrategy` comparison for now.)

- [ ] **Step 2: Run, expect FAIL** (no property→contract path yet).

- [ ] **Step 3: Add `RefreshFragmentProperty::into_refresh_contract(base_refs) -> ImvRefreshContract`** in `refresh_property.rs`, mapping identity/state → `apply_key` (the same `ApplyKeyContract::*` constructors at `refresh_contract.rs:35-93`, selected by identity: `BaseRowId`→`projection_filter` or `union_projection_filter` if BranchScoped; `JoinRowKey`→`join_projection_filter`/`join_aggregate_group_row`; `GroupRowId`→`aggregate_group_row`/`branch_union_aggregate_group_row` if BranchScoped) + `aggregate`/`join`/`branch` sub-contracts from the property. In `derive_imv_refresh_contract` (refresh_contract.rs:122), replace `derive_from_query(...).into_contract(...)` with `derive_fragment_property(&analysis.resolved_query)?.into_refresh_contract(base_refs)`. Keep `RefreshStrategy` as a field derived inside `into_refresh_contract` (deleted in Part B).

- [ ] **Step 4: Run, expect PASS** (parity holds). Then `cargo test --lib engine::mv:: -- --nocapture` (incl. all existing refresh_contract tests).

- [ ] **Step 5: Delete the dead flat classifier** — `DerivedStructure` (177-205), `into_contract` (207-295), `derive_from_query`/`_select`/`_set_operation`/`_relation` (297-502) — keeping only the helpers the property algebra reuses (move shared validation into `refresh_property.rs` in A1 if needed). Build to confirm no references remain. Re-run A1+A2 tests green.

- [ ] **Step 6: Commit.**
```bash
git add src/engine/mv/refresh_property.rs src/engine/mv/refresh_contract.rs
git commit -m "refactor(imv): derive ImvRefreshContract from the property algebra; drop flat classifier"
```

---

### Task A3: Drive CREATE target-column gating + `build_iceberg_mv_schema_contract` from the property (retire create-time IncrementalMvShape)

The CREATE path (`iceberg_refresh.rs::create_iceberg_mv` 80-342) currently: classifies the SELECT into `IncrementalMvShape` (143), cross-checks it vs the contract (`validate_refresh_contract_matches_legacy_shape` 144), gates physical apply-key / `__branch_id__` columns via `create_strategy_needs_*` (210-215), and builds the persisted contract via `build_iceberg_mv_schema_contract` (which `match`es on strategy AND consumes `shape`, 1383-1797). Make all of this property/contract-driven and **delete the create-time `IncrementalMvShape` classification + cross-check**.

**Files:** Modify `src/engine/mv/iceberg_refresh.rs` (CREATE region 80-342, gating fns 1249-1263, `build_iceberg_mv_schema_contract` 1383-1797, `create_target_columns_from_refresh_contract` 1020-1068, `aggregate_state_hidden_columns_from_refresh_contract` 1070-1121).

- [ ] **Step 1: Write a CREATE test** that a composed-shape MV (`UNION ALL` of two `GROUP BY ... FROM (a JOIN b)` selects) derives a contract with `branch = Some`, `aggregate = Some`, and a target column set including `__branch_id__` + aggregate-state columns — i.e. `build_iceberg_mv_schema_contract` succeeds for the composed shape (it currently would mis-handle `BranchUnionAggregate` branches that are agg-over-join). Use the existing CREATE unit-test harness in `iceberg_refresh.rs` tests (or a focused `build_iceberg_mv_schema_contract` test with a synthesized `ImvRefreshContract` whose property is `BranchScoped(GroupRowId)` over join branches).

- [ ] **Step 2: Run, expect FAIL** (current `build_iceberg_mv_schema_contract` branch-union arm assumes simple-aggregate branches; agg-over-join inner lineage not handled).

- [ ] **Step 3: Make gating + contract-building property-driven.**
  - Replace `create_strategy_needs_physical_apply_key_column(strategy)` (1249) and `create_strategy_needs_branch_id_column(strategy)` (1258) with property predicates: physical apply-key column needed iff identity is `BaseRowId`/`JoinRowKey` (not `GroupRowId`); `__branch_id__` needed iff identity top is `BranchScoped`. (These read `refresh_contract.apply_key`/identity, not strategy.)
  - Remove `let shape = classify_incremental_mv_query(...)` (143) and `validate_refresh_contract_matches_legacy_shape(...)` (144) from CREATE.
  - Generalize `build_iceberg_mv_schema_contract` (1383): replace the `match strategy` with property-recursive contract building. For `BranchScoped(inner)`, build the inner contract by recursing on the inner identity/state (so agg-over-join branches get join lineage inside each branch) and attach `BranchUnionContract`. Reuse `target_contract`/`aggregate_contract`/`base_contract`/`build_*_lineage` builders (1819-2037) unchanged — only the dispatch becomes property-recursive.
  - Same for `create_target_columns_from_refresh_contract` (1020) and `aggregate_state_hidden_columns_from_refresh_contract` (1070): drive off identity/state, drop the `shape` parameter.

- [ ] **Step 4: Run, expect PASS.** `cargo test --lib engine::mv::iceberg_refresh -- --nocapture` (CREATE tests). Confirm currently-supported CREATE shapes still build identical contracts (parity from A2 + these tests).

- [ ] **Step 5: Commit.**
```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor(imv): drive CREATE target schema + contract building from the property (retire create-time IncrementalMvShape)"
```

---

### Task A4: End-to-end CREATE of a composed-shape MV

**Files:** Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregate_over_join_create.sql` (+ recorded result).

- [ ] **Step 1: Add a sql-test** that `CREATE MATERIALIZED VIEW` over `UNION ALL` of two `GROUP BY` aggregates each over an inner join succeeds (previously rejected at classify), and `EXPLAIN`/metadata shows the target has `__branch_id__` + aggregate-state columns. Follow `iceberg_ivm_*` conventions (`@sequential`, `@explain_contains`, `@expect_error` for the still-unsupported negatives).

- [ ] **Step 2: Record + verify** against the standalone server (env per CLAUDE.md §7.3; record with `--mode record --record-from target`):
```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start standalone-server gated on NOVAROCKS_READY (CLAUDE.md §7.3 recipe)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm \
  --only iceberg_ivm_union_of_aggregate_over_join_create --mode verify
```
Expected: PASS (CREATE succeeds). Note: end-to-end *refresh* of this MV is enabled by Part B; this task only validates CREATE + target schema + persisted contract.

- [ ] **Step 3: Run the full `iceberg-ivm` suite** for CREATE regressions → all green.

- [ ] **Step 4: Commit.**

---

# PART B — Phase 3: Capability-driven driver + enum retirement

### Task B1: `RefreshCapabilities` from the loaded contract (replace strategy reverse-derivation)

**Files:** Modify `src/engine/mv/refresh_property.rs` (add `RefreshCapabilities`), `src/engine/mv/iceberg_refresh.rs` (4753-4821).

- [ ] **Step 1: Write a test** that `RefreshCapabilities::from_schema_contract(&MvSchemaContract)` yields the right `(snapshot_policy, has_agg_state, target_identity, apply_key)` for each persisted shape (project/join-proj/single-agg/fan-in/join-agg/branch-union), and that each maps 1:1 with the legacy `RefreshStrategy` for parity. snapshot_policy: `join.is_some()`→`JoinPairPartialInitialSkip`; else `bases.len()>1 || branch.is_some()`→`AllBasesRequired`; else `SingleBase`. has_agg_state = `aggregate.is_some()`.

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement `RefreshCapabilities` + `from_schema_contract`** reading the same `(join, aggregate, branch).is_some()` + `bases` discriminant that `stored_refresh_strategy_for_plan` (4764-4787) reads, but producing capabilities directly (no `RefreshStrategy`). Keep `stored_refresh_strategy_for_plan` temporarily for parity assertion in the test only.

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit.** `feat(imv): derive RefreshCapabilities from persisted contract`

---

### Task B2: Capability-dispatch the refresh driver + collapse wrappers

Replace the two `match strategy` sites (execute `iceberg_refresh.rs:2445`, plan `4861`) — and the Tier-2 `refresh_iceberg_aggregate_mv` shape re-dispatch (3125) — with dispatch on `RefreshCapabilities`, collapsing the 6 wrappers to 3 snapshot-policy paths × {has-agg-state}.

**Files:** Modify `src/engine/mv/iceberg_refresh.rs` (2398-2803 execute, 4823-4916 plan, wrappers 3214/3455/3724/4353/2806, first-refresh 6749/6784/7123).

- [ ] **Step 1: Characterization tests first** — for each currently-supported shape, a test (or reuse `iceberg-ivm` cases via Task B5) pinning refresh behavior. The unit-level guard: the capability dispatch selects the same `(BaseSnapshotPolicy, first-refresh fn, kernel options)` triple the old strategy match did. Add a `dispatch_for(capabilities) -> (BaseSnapshotPolicy, FirstRefreshKind)` pure fn and test it against the known mapping.

- [ ] **Step 2: Implement capability dispatch.** Replace the `match refresh_contract.strategy` arms with a single capability-driven path: compute `RefreshCapabilities`, select `BaseSnapshotPolicy` from `snapshot_policy`, run the shared `decide_refresh`/`IcebergMvRefreshLifecycle::run`, and inside the incremental closure call the unified kernel `incremental_refresh_iceberg_mv_with_changes` (already capability-driven via `ApplyKeyContract`). Fold `refresh_branch_union_aggregate_iceberg_mv` into `refresh_fan_in_aggregate_iceberg_mv` (gate the branch-contract validation + branch_id first-refresh on `branch.is_some()`); fold `refresh_single_aggregate`/`refresh_join_aggregate` into the agg path parameterized by `BaseSnapshotPolicy`. Move the first-refresh `__branch_id__` injection (`append_branch_id_to_first_refresh_chunks` 7123) behind an identity check (`BranchScoped` ⇒ per-branch loop + branch_id column), unifying with the aggregate first-refresh. Mirror the collapse on the plan side (4861).

- [ ] **Step 3: Run** unit + `cargo test --lib engine::mv:: -- --nocapture` → green.

- [ ] **Step 4: Commit.** `refactor(imv): dispatch refresh driver on capabilities; collapse strategy wrappers`

---

### Task B3: Delete `RefreshStrategy` + reverse-derivation + cross-checks

**Files:** Modify `src/engine/mv/refresh_contract.rs` (delete enum 8-16 + any remaining refs), `src/engine/mv/iceberg_refresh.rs` (delete `stored_refresh_strategy_for_plan` 4753, `stored_strategy_matches_legacy_shape` 4797, `validate_refresh_contract_matches_legacy_shape` 848, and the `RewriteEvidence`/strategy fields no longer read).

- [ ] **Step 1:** Delete `RefreshStrategy` and all now-dead reverse-derivation/cross-check fns. `ImvRefreshContract` drops its `strategy` field (it was only consumed by the deleted dispatch + builders, now capability-driven).
- [ ] **Step 2: Build** `cargo build --lib` — fix any remaining references (compiler-driven, like Phase-1 Task 2). The kernel's `rewrite_merge_refresh_evidence` (9998) keys on `apply_key.value_type` not strategy — confirm untouched.
- [ ] **Step 3: Run** `cargo test --lib engine::mv:: -- --nocapture` → green.
- [ ] **Step 4: Commit.** `refactor(imv): delete RefreshStrategy enum and strategy reverse-derivation/cross-checks`

---

### Task B4: Retire Iceberg-path `IncrementalMvShape` (84 sites); keep StarRocks path

**Files:** Modify `src/engine/mv/iceberg_refresh.rs` (79 production lines per the map), `src/engine/mv/refresh_context.rs` (`aggregate_shape_and_layout_for_execution` 274-315). Do NOT touch `mv_ddl.rs`/`mv_refresh.rs`/`mv_apply_policy.rs` (StarRocks) or `mv_shape.rs`'s definition/classifier (shared, StarRocks keeps it).

- [ ] **Step 1: Write a test** that `aggregate_shape_and_layout_for_execution` (used by the `AggregateStateMerge` operator at execution) produces the correct `AggregateMvLayout` from `MvSchemaContract.aggregate` (`AggregateStateContract`) **without** calling `classify_incremental_mv_query`.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Replace** `refresh_context.rs:274-315` to build the layout from `AggregateStateContract` (it has `state_layout_version`, `row_id_column_name`, `state_columns` with roles — the layout's data). In `iceberg_refresh.rs`, remove the remaining `classify_incremental_mv_query` calls + `IncrementalMvShape` matches (the dispatch ones were already removed in B2; remaining are layout/first-refresh feeders — replace with contract reads). Remove the `mv_shape` imports (34-35) that are now unused on the Iceberg side.
- [ ] **Step 4: Build + run** `cargo test --lib engine::mv:: -- --nocapture` → green. Confirm `mv_shape.rs` + StarRocks callers still compile (they retain their uses).
- [ ] **Step 5: Commit.** `refactor(imv): retire Iceberg-path IncrementalMvShape; derive agg layout from contract`

---

### Task B5: End-to-end — composed-shape refresh + capability round-trip + regression

**Files:** `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_of_aggregate_over_join.sql` (+ result); unit test in `refresh_property.rs`.

- [ ] **Step 1: Add the end-to-end refresh sql-test** for the composed MV created in A4: insert into a base, refresh, assert results equal the full-recompute query (the `iceberg-ivm` cross-check convention). This exercises CREATE (property) → REFRESH (capability driver) → kernel for `BranchScoped(GroupRowId)` over join branches — the new capability.
- [ ] **Step 2: Add a capability round-trip unit test**: every persisted `MvSchemaContract` shape → `RefreshCapabilities` → a unique driver path, with no `RefreshStrategy` in the loop (it's deleted).
- [ ] **Step 3: Run the full `iceberg-ivm` suite** `--mode verify` (env per §7.3) → all green (no regression; the 4 union/branch cases + the new composed cases). Also `cargo test --lib sql::optimizer::rewrite::imv:: engine::mv::` → green.
- [ ] **Step 4: Commit.** `test(imv): end-to-end refresh for union-of-aggregate-over-join; capability round-trip`

---

## Self-Review

**Spec coverage:** Phase 2 (§15) = property algebra (A1) + CREATE derivation replacing the flat classifier (A2-A3) + composed-shape CREATE (A4). Phase 3 (§15) = capability dispatch (B1-B2) + delete `RefreshStrategy` (B3) + retire Iceberg `IncrementalMvShape` (B4) + end-to-end (B5). The unified-algebra decision (no second classifier) is realized by A1's single module + A2's parity + B5's round-trip. §12 difficulties: synth→inherited feedback (delete_handling) is carried in `RefreshFragmentProperty` but only fully exercised when MIN/MAX-on-delete lands — noted, not blocking; `JoinRowKey` physical representability (§12.2) is the gate for *executing* join-of-aggregates end-to-end — A4/B5 cover union-of-(agg-over-join) which produces `BranchScoped(GroupRowId)` (representable as `BranchUtf8`), NOT a bare `JoinRowKey(GroupRowId,GroupRowId)` apply key, so this plan does not require extending `ApplyKeyValueType`; a true top-level join-of-aggregates remains future work (flag if A1 fixtures hit it).

**Placeholder scan:** A3/B2/B4 are transformation-over-mapped-sites tasks (legitimate refactor steps, authority = the investigation's file:line maps), each with a concrete test gate — not vague placeholders. New-artifact tasks (A1, B1) carry full type/code. The 16k-line-file edits (A3, B2, B4) specify the exact transformation + the line ranges + the verification; exact final code is finalized against the compiler/tests during execution (these are refactors of existing intricate code, not greenfield).

**Type consistency:** `TargetIdentity`/`StateContract`/`RefreshFragmentProperty` (A1) are consumed by `into_refresh_contract` (A2), CREATE gating + `build_iceberg_mv_schema_contract` (A3), `RefreshCapabilities::from_schema_contract` (B1), and the dispatch (B2). `RefreshCapabilities` fields `(snapshot_policy: BaseSnapshotPolicy, has_agg_state: bool, identity, apply_key)` are used consistently in B1/B2. `ApplyKeyContract` constructors referenced in A2 match `refresh_contract.rs:35-93`.

---

## Execution Handoff

**Plan complete and saved to `docs/design/plans/2026-06-04-imv-property-framework-phase2-3.md`.** Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review (spec then quality) between tasks. Part A before Part B (B consumes A's contract). The sql-test tasks (A4, B5) need the standalone server + Iceberg REST env (per CLAUDE.md §7.3); the controller orchestrates those.
2. **Inline Execution** — execute in this session with checkpoints.

**Which approach — and start now, or review the plan first?** Note A1–A2 are the highest-leverage (the algebra + parity); B3–B4 are the satisfying deletions but depend on A+B2.
