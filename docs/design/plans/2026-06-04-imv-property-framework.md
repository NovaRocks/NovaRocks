# IMV Property Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple IMV refresh from the `RefreshStrategy` / `IncrementalMvShape` shape enums by deriving the refresh contract as a *property* synthesized during logical-plan rewrite, so the refresh driver consumes capabilities instead of recognizing SQL shapes.

**Architecture:** IMV refresh capabilities form an attribute grammar over the logical plan: `TargetIdentity` / `StateContract` / delta-ability are *synthesized* bottom-up; `branch_scope` (and later other identity context) is *inherited* top-down via the delta marker; `snapshot_policy` / `apply` / `schema_binding` are computed at a root *finalize*. Rewrite rules emit these properties as a byproduct of the rewrite (into `ImvPlanAnnotation`), so plan and contract cannot drift. The refresh driver dispatches on the derived capabilities, not on a shape enum.

**Tech Stack:** Rust, the standalone SQL optimizer rewrite pipeline (`src/sql/optimizer/rewrite/imv/**`), the Iceberg IMV refresh engine (`src/engine/mv/**`), and the persisted MV contract (`src/meta/repository/mv_contract.rs`). Tests: `cargo test --lib` for rewrite/unit, the `sql-tests` runner (`optimizer` + `iceberg-rest` suites) for plan-golden and end-to-end.

**Canonical design spec:** [docs/design/specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md](../specs/2026-06-04-iceberg-imv-refresh-property-framework-design.md). This plan implements **Phase 1** (spec §15). Type definitions (§6), per-operator derivation rules (§7), invariants (§11), and known difficulties (§12) live in the spec — read it first; the decisions repeated below are an executor convenience, the spec is authoritative.

---

## Scope Check (read first)

This spec spans three subsystems that the `writing-plans` skill says should each be their own plan, because each produces working, testable software on its own and the later ones depend on facts only learned by doing the earlier ones:

1. **Phase 1 — Compositional `RewriteBranchUnion` (rewrite layer only).** Fully detailed below. Self-contained: regression-green for currently-supported branch-union MVs, and `UNION ALL` branches that are `Aggregate(Join)` / `Aggregate(Union)` stop orphaning a marker. No contract or refresh-driver change.
2. **Phase 2 — `RefreshFragmentProperty` into `ImvPlanAnnotation` + root finalize + persist.** Scoped roadmap below; gets its own detailed plan once Phase 1 lands.
3. **Phase 3 — Refresh driver becomes capability-driven; retire `RefreshStrategy` and (Iceberg-path) `IncrementalMvShape`.** Scoped roadmap below; gets its own detailed plan, written against the parts of the 16k-line `iceberg_refresh.rs` that must be read at that time.

**This document is the executable plan for Phase 1** plus a roadmap spine for 2–3. Do not implement 2–3 from the roadmap text; expand each into its own plan first.

**Design decisions locked during brainstorming (do not re-litigate):**
- `BranchUnionAggregate` is not a shape — it is `TargetIdentity = BranchScoped(GroupRowId)`. Confirmed by an executed probe: `RewriteBranchUnion` over `Union(Aggregate(Join), Aggregate(Join))` returns `Changed` but leaves an `ImvDelta` orphaned over a `Join` (no downstream rule binds it), so the plan is rejected at validation.
- The refresh contract is a *byproduct of rewrite*, not a second classifier. `ImvPlanAnnotation` (`src/sql/optimizer/rewrite/imv/annotation.rs:16`) is the (currently-empty) vehicle; its doc comment already names "branch identity / marker node ids / action column refs" as intended fields.
- "Homogeneous branch" (UNION ALL) is defined on the **synthesized property** `(identity ctor, state ctor)`, not on SQL shape. Under that definition, `Union(Agg over t1, Agg over (t2 JOIN t3))` is homogeneous and supported. Heterogeneous branches are rejected for now; per-branch contracts are a future direction.
- It is a *typing discipline*, not a cost-based property: deterministic, not in the CBO memo.
- Persisted contract is the source of truth at refresh time; the rewrite re-run at refresh produces the executable plan and asserts runtime preconditions, it does **not** re-derive-and-equality-check the contract. Engine upgrades that change derivation imply MV recompile (no compat shim — matches project policy).

---

## File Structure (Phase 1)

- Modify `src/sql/optimizer/rewrite/imv/marker.rs` — add the inherited `branch_scope` field to `ImvDeltaNode`; update its constructors/sites.
- Modify `src/sql/optimizer/rewrite/imv/branch_union.rs` — change `RewriteBranchUnionRule` from "inline `build_aggregate_state_merge` per branch" to "tag each branch core with `ImvDelta{is_root, branch_scope}` and delegate decomposition"; update its rule-level unit tests (intermediate shape changes from `AggregateStateMerge` to `ImvDelta`).
- Modify `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs` — `RewriteAggregateStateRule::apply` passes `delta.branch_scope` (not `None`) to `build_aggregate_state_merge`.
- Modify `src/sql/optimizer/rewrite/imv/join_delta.rs` — `RewriteJoinAggregateDeltaRule::apply` preserves `delta.branch_scope` on the rewritten root delta.
- Modify `src/sql/optimizer/rewrite/imv/union_delta.rs` — both rules preserve `delta.branch_scope` on rewritten root deltas (fan-in branch under a branch-scoped aggregate).
- Test (rewrite, in-crate): `src/sql/optimizer/rewrite/imv/branch_union.rs` tests module — pipeline-level characterization + acceptance tests.
- Test (plan-golden): `sql-tests/optimizer/imv_branch_union_aggregate_over_join.sql` (new).

Responsibility split: `branch_union.rs` owns *only* "UNION ALL → branch-scoped identity + delegate"; it stops owning aggregate-state-merge construction (that returns to `aggregate_rewrite.rs`, the single owner). `marker.rs` owns the marker shape including its inherited attributes.

---

## Phase 1 Tasks

### Task 1: Characterize the current branch-union final plan at the pipeline level

Lock today's *final* (post-full-pipeline) plan shape for a supported branch-union MV, so the Task 4 refactor is provably behavior-preserving. The existing rule-level tests assert the *intermediate* output of `RewriteBranchUnion::apply` (they expect `AggregateStateMerge` directly); those will legitimately change in Task 4, so the durable regression guard must be at the pipeline level.

**Files:**
- Test: `src/sql/optimizer/rewrite/imv/branch_union.rs` (tests module, reuse `build_ctx`, `aggregate_over`, `scan`, `root_delta`, `output_column`)

- [ ] **Step 1: Write the characterization test**

Add to the `tests` module. It runs the whole IMV pipeline and asserts the final per-branch shape is `Project(AggregateStateMerge)` with a `__branch_id__` column and no surviving marker.

```rust
#[test]
fn pipeline_branch_union_of_aggregates_final_shape_is_stable() {
    use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
    use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;

    let mut ctx = build_ctx();
    let plan = LogicalPlan::Union(UnionNode {
        inputs: vec![aggregate_over(scan("t1", 1)), aggregate_over(scan("t2", 10))],
        all: true,
        output_columns: vec![output_column(1, "region"), output_column(3, "s")],
        required_output_columns: None,
    });

    let out = build_imv_pipeline().rewrite(plan, &mut ctx).expect("pipeline must succeed");

    // Top is a Union whose branches each end in Project over AggregateStateMerge,
    // carrying a __branch_id__ column, with no IMV marker left anywhere.
    assert!(!plan_contains_imv_marker(&out), "no marker may survive validation");
    let LogicalPlan::Union(union) = &out else { panic!("expected top Union, got {out:?}") };
    assert_eq!(union.inputs.len(), 2);
    assert!(
        union.output_columns.iter().any(|c| c.name.eq_ignore_ascii_case("__branch_id__")),
        "union output must expose __branch_id__"
    );
    for branch in &union.inputs {
        let LogicalPlan::Project(p) = branch else { panic!("expected Project branch, got {branch:?}") };
        assert!(matches!(p.input.as_ref(), LogicalPlan::AggregateStateMerge(_)));
        assert!(p.items.iter().any(|i| i.output_name.eq_ignore_ascii_case("__branch_id__")));
    }
}
```

- [ ] **Step 2: Run it to confirm it passes against today's code**

Run: `cargo test --lib pipeline_branch_union_of_aggregates_final_shape_is_stable -- --nocapture`
Expected: PASS. (If it fails because `build_imv_pipeline` is not importable from the tests module, make `pub(crate) fn build_imv_pipeline` already is — confirm the `use` path compiles; the only legal fix is the import path, not the assertions.)

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/branch_union.rs
git commit -m "test(imv): characterize branch-union final plan shape at pipeline level"
```

---

### Task 2: Add the inherited `branch_scope` attribute to `ImvDeltaNode`

The delta marker carries the inherited identity context downward. Adding the field is mechanical; every existing construction site keeps today's behavior with `branch_scope: None`.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs:22-27` (struct def)
- Modify (add `branch_scope: None` to each `ImvDeltaNode { .. }` literal): `join_delta.rs:124,190`, `union_delta.rs:84,610,708`, `scan_binding.rs:313`, `delta_pushdown.rs:87,96,184`, `aggregate_rewrite.rs:613,1454,1464,1501`, `action_propagation.rs:717,759,789`, `branch_union.rs:411,573`, and any `ImvDeltaNode { .. }` in tests.

- [ ] **Step 1: Add the field**

In `src/sql/optimizer/rewrite/imv/marker.rs`, extend the struct (keep existing derives):

```rust
pub(crate) struct ImvDeltaNode {
    pub input: Box<LogicalPlan>,
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
    /// Inherited identity context threaded top-down by `RewriteBranchUnion`.
    /// `Some(scope)` means this delta sub-problem belongs to UNION ALL branch
    /// `scope.branch_id`; the eventual aggregate-state merge scopes the target
    /// state read and the apply key by it. `None` for the ordinary single root.
    pub branch_scope: Option<crate::sql::catalog::BranchScope>,
}
```

- [ ] **Step 2: Build to get the exhaustive list of construction sites**

Run: `cargo build --lib 2>&1 | grep -A2 "missing field .branch_scope"`
Expected: FAIL listing each `ImvDeltaNode { .. }` literal missing `branch_scope`. This is the authoritative site list (grep estimate above may drift).

- [ ] **Step 3: Add `branch_scope: None` to every reported site**

For each site the compiler reports, add `branch_scope: None,` to the literal. Do **not** change any other field. (`BranchScope` derives `Clone, Debug, PartialEq, Eq` — matches `ImvDeltaNode`'s derives, so no derive changes are needed.)

- [ ] **Step 4: Build and run the existing IMV rewrite tests**

Run: `cargo test --lib sql::optimizer::rewrite::imv:: -- --nocapture`
Expected: PASS (behavior unchanged; field is inert at `None`).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): add inherited branch_scope attribute to ImvDeltaNode"
```

---

### Task 3: Consume/propagate `branch_scope` in the structural rules

`build_aggregate_state_merge` already takes `branch_scope`; route the marker's value into it, and preserve the marker's value across join/union delta expansion so it reaches the aggregate underneath.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs:79`
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs:124-130`
- Modify: `src/sql/optimizer/rewrite/imv/union_delta.rs:84,610` (rewritten root deltas)
- Test: `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs` (tests module)

- [ ] **Step 1: Write a failing test for branch_scope consumption by aggregate-state**

Add to `aggregate_rewrite.rs` tests. A root delta carrying `branch_scope: Some(..)` over an `Aggregate(Scan)` must produce an `AggregateStateMerge` whose old input is branch-filtered (the `Project(Filter(Scan))` wrapper that `branch_scoped_old_input` builds).

```rust
#[test]
fn aggregate_state_rule_threads_marker_branch_scope() {
    let rule = RewriteAggregateStateRule;
    let mut ctx = build_ctx(); // existing aggregate_rewrite test fixture
    let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
        input: Box::new(aggregate_over(scan("b", 1))),
        is_root: true,
        action_column: None,
        branch_scope: Some(crate::sql::catalog::BranchScope {
            branch_id_column_name:
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
            branch_id: 1,
        }),
    });
    let RewriteResult::Changed(LogicalPlan::AggregateStateMerge(merge)) =
        rule.apply(plan, &mut ctx).expect("rewrite")
    else { panic!("expected AggregateStateMerge") };
    // Branch scope manifests as Project(Filter(Scan)) on the old input.
    assert!(matches!(merge.old_input.as_ref(), LogicalPlan::Project(_)),
        "branch-scoped old input must be wrapped in a passthrough Project over a Filter");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib aggregate_state_rule_threads_marker_branch_scope -- --nocapture`
Expected: FAIL — old input is a bare `Scan` because the rule still passes `None`.

- [ ] **Step 3: Route the marker's branch_scope into the merge builder**

In `aggregate_rewrite.rs`, `RewriteAggregateStateRule::apply` (around line 79), change:

```rust
let merge = build_aggregate_state_merge(aggregate, delta.action_column, None, &ext)?;
```
to:
```rust
let merge = build_aggregate_state_merge(aggregate, delta.action_column, delta.branch_scope, &ext)?;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib aggregate_state_rule_threads_marker_branch_scope -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Write a failing test for join-delta preserving branch_scope**

```rust
#[test]
fn join_delta_preserves_branch_scope() {
    let rule = RewriteJoinAggregateDeltaRule;
    let mut ctx = build_ctx(); // join_delta test fixture
    let scope = crate::sql::catalog::BranchScope {
        branch_id_column_name:
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        branch_id: 2,
    };
    let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
        input: Box::new(aggregate_over(join_of(scan("l", 1), scan("r", 10)))),
        is_root: true,
        action_column: None,
        branch_scope: Some(scope.clone()),
    });
    let RewriteResult::Changed(LogicalPlan::ImvDelta(out)) =
        rule.apply(plan, &mut ctx).expect("rewrite")
    else { panic!("expected rewritten root ImvDelta") };
    assert_eq!(out.branch_scope, Some(scope), "join-delta must carry branch_scope onto the rewritten root delta");
}
```
(Reuse / add a `join_of` + `aggregate_over` helper in the join_delta test module mirroring the ones used in the Task-5 acceptance test.)

- [ ] **Step 6: Run it to verify it fails**

Run: `cargo test --lib join_delta_preserves_branch_scope -- --nocapture`
Expected: FAIL — `out.branch_scope` is `None`.

- [ ] **Step 7: Preserve branch_scope in join-delta**

In `join_delta.rs`, the `apply` returns (around line 124):

```rust
Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(ImvDeltaNode {
    input: Box::new(LogicalPlan::Aggregate(aggregate)),
    is_root: true,
    action_column: Some(action_column),
    branch_scope: delta.branch_scope,
})))
```
(Bind `delta` so `delta.branch_scope` is in scope; the function already destructures `delta.input`/`delta.action_column`, so capture `branch_scope` before moving `delta.input`.)

- [ ] **Step 8: Mirror the same one-line preservation in `union_delta.rs`**

In both `RewriteUnionAggregateDeltaRule` and `RewriteTopLevelUnionDeltaRule`, the rewritten **root** `ImvDeltaNode { is_root: true, .. }` (lines ~84 and ~610) gets `branch_scope: delta.branch_scope`. Non-root markers they emit on branch scans keep `branch_scope: None` (branch identity is carried by the root delta of the sub-problem, not by leaf scans).

- [ ] **Step 9: Run join + union delta tests**

Run: `cargo test --lib "sql::optimizer::rewrite::imv::join_delta" "sql::optimizer::rewrite::imv::union_delta" -- --nocapture`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): thread branch_scope through aggregate-state and join/union delta rules"
```

---

### Task 4: Refactor `RewriteBranchUnion` to tag-and-delegate

Stop calling `build_aggregate_state_merge` inline. For each branch, peel the optional passthrough `Project`, wrap the aggregate **core** in `ImvDelta { is_root: true, branch_scope: Some(branch_id) }`, re-apply the project, append the `__branch_id__` literal, and rebuild the union. The existing structural rules (aggregate-state in Task 3; join/union delta) then decompose each branch core in later pipeline stages.

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/branch_union.rs:46-132` (the `apply` body) and its tests (lines ~304-383, which assert the old intermediate `AggregateStateMerge` shape).

- [ ] **Step 1: Update the two rule-level unit tests to the new intermediate shape**

`rewrites_top_union_of_aggregates_into_branch_scoped_merges` and `rewrites_project_over_aggregate_branches_into_branch_scoped_merges` currently assert `project.input` is `AggregateStateMerge`. After the refactor, `RewriteBranchUnion::apply` leaves a delegated marker, so assert the new intermediate shape instead:

```rust
// In rewrites_top_union_of_aggregates_into_branch_scoped_merges, replace the
// `matches!(project.input.as_ref(), LogicalPlan::AggregateStateMerge(_))` assertion with:
let LogicalPlan::ImvDelta(d) = project.input.as_ref() else {
    panic!("branch core must be a delegated ImvDelta, got {:?}", project.input)
};
assert!(d.is_root, "branch sub-problem delta must be a root delta");
assert_eq!(d.branch_scope.as_ref().map(|s| s.branch_id), Some(idx as i32));
assert!(matches!(d.input.as_ref(), LogicalPlan::Aggregate(_)),
    "delta must sit directly over the Aggregate core");
```

- [ ] **Step 2: Run them to verify they fail against current code**

Run: `cargo test --lib "rewrites_top_union_of_aggregates" "rewrites_project_over_aggregate_branches" -- --nocapture`
Expected: FAIL — current code produces `AggregateStateMerge`, not `ImvDelta`.

- [ ] **Step 3: Rewrite `apply` to tag-and-delegate**

Replace the per-branch loop body in `RewriteBranchUnionRule::apply` (`branch_union.rs:88-124`). Keep `extract_branch_union_aggregate_branch`, `append_branch_id_to_project`, `append_branch_id_project`, `branch_union_output_columns` unchanged.

```rust
let mut rewritten_inputs = Vec::with_capacity(inputs.len());
for (idx, branch) in inputs.into_iter().enumerate() {
    let branch_id = i32::try_from(idx)
        .map_err(|_| "Iceberg IMV branch UNION branch index overflow".to_string())?;
    let branch_kind = plan_kind(&branch);
    let branch = extract_branch_union_aggregate_branch(branch).ok_or_else(|| {
        format!(
            "Iceberg IMV branch UNION rewrite supports only aggregate or Project-over-Aggregate branches, got {}",
            branch_kind
        )
    })?;
    // Tag the aggregate core as an independent, branch-scoped delta sub-problem.
    // aggregate-state (and join/union-delta beneath it) decompose it in later stages.
    let scope = crate::sql::catalog::BranchScope {
        branch_id_column_name: ICEBERG_MV_BRANCH_ID_COLUMN.to_string(),
        branch_id,
    };
    let core = LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
        input: Box::new(LogicalPlan::Aggregate(branch.aggregate)),
        is_root: true,
        action_column: delta.action_column,
        branch_scope: Some(scope),
    });
    let rewritten = match branch.post_project {
        Some(project) => append_branch_id_to_project(
            ProjectNode {
                input: Box::new(core),
                items: project.items,
                output_qualifier: project.output_qualifier,
                required_output_columns: None,
            },
            branch_id,
            branch_id_column,
        ),
        None => append_branch_id_project(core, branch_id, branch_id_column),
    }?;
    rewritten_inputs.push(rewritten);
}
```

Remove the now-unused `use ...::build_aggregate_state_merge;` import (line 7) — the compiler will flag it as unused; delete it.

- [ ] **Step 4: Run the rule-level tests to verify they pass**

Run: `cargo test --lib "rewrites_top_union_of_aggregates" "rewrites_project_over_aggregate_branches" "rejects_non_aggregate_branch" "does_not_match" -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Run the Task-1 pipeline characterization test — the real regression guard**

Run: `cargo test --lib pipeline_branch_union_of_aggregates_final_shape_is_stable -- --nocapture`
Expected: PASS — the *final* plan is byte-for-byte the same `Union(Project(AggregateStateMerge))` with `__branch_id__`, even though the *intermediate* output of `RewriteBranchUnion` changed. This proves the refactor preserves behavior for currently-supported branch-union MVs.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/branch_union.rs
git commit -m "refactor(imv): make RewriteBranchUnion tag-and-delegate instead of inlining state merge"
```

---

### Task 5: Acceptance — `Union(Aggregate(Join))` now composes through the pipeline

The previously-orphaned case becomes a resolvable plan because each branch core is delegated to join-delta + aggregate-state, with `branch_scope` threaded through.

**Files:**
- Test: `src/sql/optimizer/rewrite/imv/branch_union.rs` (tests module). Reintroduce the `join_of` + `delta_over_join_exists` helpers from the earlier diagnostic, but now assert *clean composition*.

- [ ] **Step 1: Write the acceptance test (the flipped diagnostic)**

```rust
#[test]
fn pipeline_branch_union_of_aggregate_over_join_composes() {
    use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
    use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;

    let mut ctx = build_ctx();
    let plan = LogicalPlan::Union(UnionNode {
        inputs: vec![
            aggregate_over(join_of(scan("t1", 1), scan("t2", 10))),
            aggregate_over(join_of(scan("t3", 20), scan("t4", 30))),
        ],
        all: true,
        output_columns: vec![output_column(1, "region"), output_column(3, "s")],
        required_output_columns: None,
    });

    let out = build_imv_pipeline().rewrite(plan, &mut ctx)
        .expect("branch union of aggregate-over-join must compose");
    assert!(!plan_contains_imv_marker(&out),
        "no marker may survive: the inner joins must be delta-expanded and bound");
}

fn join_of(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Join(crate::sql::planner::plan::JoinNode {
        left: Box::new(left),
        right: Box::new(right),
        join_type: crate::sql::analysis::JoinKind::Inner,
        condition: Some(TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_expr(1, "region")),
                op: BinOp::Eq,
                right: Box::new(col_expr(10, "region")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }),
        required_output_columns: None,
    })
}
```

- [ ] **Step 2: Run it**

Run: `cargo test --lib pipeline_branch_union_of_aggregate_over_join_composes -- --nocapture`
Expected: One of two outcomes —
  - **PASS:** join-delta + aggregate-state + scan-binding fully consumed the per-branch markers. Proceed to Task 6.
  - **FAIL** with a marker surviving or an `Err` from `apply-key` / `action-propagation`: the top-of-union plumbing does not yet handle an N-branch decomposed union. This is expected to surface here, not earlier — handle it in Task 6 before this test can pass.

- [ ] **Step 3: Commit the test (red or green)**

```bash
git add src/sql/optimizer/rewrite/imv/branch_union.rs
git commit -m "test(imv): acceptance for branch union of aggregate-over-join"
```

---

### Task 6: Make the top-of-union plumbing handle decomposed branches (apply-key / action propagation)

Only do the sub-steps the Task-5 failure requires. The candidates, in the order the pipeline runs them after the branches decompose:

**Files (as needed):**
- `src/sql/optimizer/rewrite/imv/action_propagation.rs` — `PropagateActionColumnRule` (`is_supported_branch_union*`, `branch_delta_union_needs_row_id_output`).
- `src/sql/optimizer/rewrite/imv/apply_key.rs` — `root_row_id_ref` / `is_branch_delta_union` (the Union-root case).

- [ ] **Step 1: Read the Task-5 failure and localize**

If the failure is a surviving marker: a branch core did not match any structural rule. Print the offending subtree:
Run: `cargo test --lib pipeline_branch_union_of_aggregate_over_join_composes -- --nocapture 2>&1 | sed -n '1,40p'`
Determine which node kind the orphaned `ImvDelta` sits over. For `Aggregate(Join)` it should be consumed by join-delta then aggregate-state; if a marker survives over a `Join`, re-check Task 3 Step 7/8 (branch_scope preservation) and that join-delta's `matches` (`is_root && Aggregate(Join)`) fires on the per-branch root delta — it must, since traversal visits union children.

- [ ] **Step 2: If `PropagateActionColumnRule` rejects the decomposed union**

Its Union arm only accepts `is_supported_join_delta_union | is_supported_fan_in_delta_union | is_supported_branch_union`. A branch-union whose branches are themselves join-delta-expanded is a new combination. Extend `is_supported_branch_union` (action_propagation.rs) to accept a branch that is a `Project` over an already-decomposed delta subtree (i.e. recurse into the branch and accept if its core is any supported delta form), rather than requiring the branch to be a literal aggregate. Write the predicate change behind a focused unit test first (construct the post-join-delta branch shape and assert the predicate returns true).

- [ ] **Step 3: If `InjectApplyKeyProjectRule` fails at the union root**

`root_row_id_ref` must find the per-branch `_row_id` output for the Union-root branch-delta case (`is_branch_delta_union`). Confirm the decomposed branch still exposes `_row_id` in its output columns; if the join-delta path names it differently, normalize it in the branch projection. Add a unit test that runs `InjectApplyKeyProjectRule` over the decomposed union and asserts the `__nova_base_row_id` apply-key projection is injected once at the union root.

- [ ] **Step 4: Re-run the acceptance test until green**

Run: `cargo test --lib pipeline_branch_union_of_aggregate_over_join_composes -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Run the full IMV rewrite test module to confirm no regressions**

Run: `cargo test --lib sql::optimizer::rewrite::imv:: -- --nocapture`
Expected: PASS (all existing + new tests).

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/
git commit -m "feat(imv): support join-delta-expanded branches under branch union"
```

---

### Task 7: Plan-golden coverage + Phase 1 wrap

**Files:**
- Create: `sql-tests/optimizer/imv_branch_union_aggregate_over_join.sql`

- [ ] **Step 1: Add a plan-golden case**

A `CREATE MATERIALIZED VIEW` whose body is `UNION ALL` of two `GROUP BY` aggregates each over a two-table `INNER JOIN`, with `EXPLAIN` of the refresh rewrite. Use the `-- @explain_contains=` assertion (per CLAUDE.md §9) to lock that the plan reaches `AggregateStateMerge` per branch with a `__branch_id__` column and no residual marker. Model the file on existing `sql-tests/optimizer/aggregate_pushdown_*.sql` structure.

- [ ] **Step 2: Record / verify against the standalone server**

Run (with the local Iceberg REST env active per CLAUDE.md §7.3):
```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer \
  --only imv_branch_union_aggregate_over_join --mode record --record-from target
```
Then re-run with `--mode verify`. Expected: PASS. (Record-from-target per memory: NovaRocks-only golden.)

- [ ] **Step 3: Run the broader rewrite + optimizer suites for regressions**

Run:
```bash
cargo test --lib sql::optimizer::rewrite::imv:: 
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer --mode verify
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add sql-tests/optimizer/imv_branch_union_aggregate_over_join.sql sql-tests/optimizer/imv_branch_union_aggregate_over_join.result
git commit -m "test(imv): plan-golden for branch union of aggregate-over-join"
```

**Phase 1 exit criteria:** branch-union is compositional at the rewrite layer; `Union(Agg(scan))` MVs produce identical final plans; `Union(Agg(Join))` (and structurally, `Union(Agg(Union))`) decompose to resolvable plans. No contract or refresh-driver change yet — end-to-end *refresh* of the new shapes is enabled by Phases 2–3.

---

## Phase 2 — `RefreshFragmentProperty` into the annotation + root finalize (roadmap)

> Expand into its own plan (`docs/design/plans/<date>-imv-refresh-property-derivation.md`) before implementing.

**Goal:** Populate `ImvPlanAnnotation` with the synthesized `RefreshFragmentProperty` as rules fire, finalize it at the root into a `RefreshContract`, and persist that contract (replacing the DDL-time `derive_imv_refresh_contract` shape classifier and the `IncrementalMvShape` cross-check on the Iceberg path).

**Key structures (locked in brainstorming):**
- `TargetIdentity = BaseRowId | JoinRowKey(Box, Box) | GroupRowId(keys) | BranchScoped(Box<inner>)` (recursive; closed under composition — `BranchScoped(BranchScoped(x))` flattens; `JoinRowKey` of arbitrary arity must be representable as an apply key).
- `StateContract = Stateless | AggregateState(layout, per_function_roles)`.
- `RefreshFragmentProperty { identity, state, produces_signed_delta, delete_handling, base_refs, obligations }` — synthesized bottom-up.
- Root finalize → `RefreshContract { root_fragment, snapshot_policy, apply_contract, schema_binding, commit_contract }`.

**Key tasks (high level):**
1. Define the property/contract types (a new module under `src/sql/optimizer/rewrite/imv/` or `src/engine/mv/`).
2. Replace `ImvPlanAnnotation { _private }` with the accumulator; have each rule write its fragment as it fires (scan→`BaseRowId`; aggregate→`GroupRowId`+`AggregateState`; branch-union→`BranchScoped`; join→`JoinRowKey`).
3. Root finalize: fold base_refs into `snapshot_policy` (structural part synthesizable as a semilattice; runtime `initial-skip` deferred to refresh time), derive `apply_contract` from `root_fragment.identity`, and bind/produce target schema.
4. Per-function aggregate proof in the aggregate fragment (`SUM/COUNT`→signed; `AVG`→sum+count; `MIN/MAX`→detail/recompute via `delete_handling=NeedsFullRecompute` which re-injects as an inherited read requirement on the source — note the synthesized→inherited feedback edge; `DISTINCT/HLL`→special state). Unsupported function ⇒ CREATE-time fail-fast.
5. Persist `RefreshContract`; keep `RefreshStrategy` derivable from it for one transition (assert-equal as a *bug* check, deleted in Phase 3).

**Risks to resolve in that plan:** the synthesized→inherited feedback (delete_handling widening the snapshot read range) couples the two passes — design the pass ordering/fixpoint explicitly; `JoinRowKey` physical apply-key representability (extend `ApplyKeyValueType` or encode composite as `Utf8`).

**Tests:** unit tests per fragment rule asserting the synthesized property; a property-derivation golden over representative plans; CREATE-time fail-fast tests for `MIN/MAX`-under-delete and non-whitelisted functions.

---

## Phase 3 — Refresh driver becomes capability-driven; retire the shape enums (roadmap)

> Expand into its own plan (`docs/design/plans/<date>-imv-refresh-capability-driver.md`) before implementing, after reading the relevant slices of `src/engine/mv/iceberg_refresh.rs`.

**Goal:** Replace the two `match refresh_contract.strategy` dispatch sites (`iceberg_refresh.rs:2445` execute, `:4861` plan) with branching on the derived capabilities; collapse the 6 strategy wrappers toward ~3 snapshot-policy-keyed paths; delete `RefreshStrategy` and (Iceberg-path) `IncrementalMvShape` plus the `stored_strategy_matches_legacy_shape` cross-check.

**Key tasks (high level):**
1. Fold `refresh_branch_union_aggregate_*` into the fan-in aggregate path (they differ only by 2 params / 3 validator lines / log strings — see brainstorming evidence), gated on `apply_contract` having a branch dimension.
2. Unify first-refresh `__branch_id__` injection into the projection layer (driven by `TargetIdentity`), matching the incremental path's `append_branch_id_project`; delete `append_branch_id_to_first_refresh_chunks` (`iceberg_refresh.rs:7123`).
3. De-duplicate the execute vs plan fan-out into one capability-driven function.
4. Delete `RefreshStrategy` (or demote to an EXPLAIN/telemetry label derived from capabilities) and the Iceberg-path `IncrementalMvShape` usage + cross-validation. Leave `IncrementalMvShape` only in the StarRocks (non-Iceberg) MV path (`mv_apply_policy.rs` / `mv_refresh.rs` / `mv_ddl.rs`) — explicitly out of scope.
5. (Optional follow-on) surface the derived `RefreshContract` in `EXPLAIN` (reuse the `src/sql/explain.rs` trailer mechanism) and a CREATE-time linter for precise rejection reasons.

**Risks:** snapshot orchestration genuinely differs by source policy (single / all-bases / join-pair) — target ~3 paths, not 1; commit/state are already unified (brainstorming-confirmed), so they should need no per-capability branching.

**Tests:** end-to-end `iceberg-rest` suite cases for each identity×state combination (base_row_id+stateless, join_row_key+stateless, group_row_id+agg-state, fan-in, join-aggregate, **branch-scoped group_row_id**); a capability round-trip unit test (every contract → capability tuple → unique driver path, no strategy enum); and the new end-to-end refresh of `UNION ALL` of aggregate-over-join MVs (the Phase-1 plan-shape, now executed).

---

## Self-Review (Phase 1)

**Spec coverage:** Phase 1 of the brainstormed design = "make branch-union compositional so homogeneity is property-defined and `Union(Agg(Join))` works." Covered by Tasks 1–7. The `branch_scope`-as-inherited-marker-attribute mechanism (the core brainstorming conclusion) is Tasks 2–4. The diagnostic→regression flip is Task 1 (characterize) + Task 5 (acceptance). ✓

**Placeholder scan:** Task 6 is intentionally conditional ("only the sub-steps the Task-5 failure requires") — this is not a vague placeholder but a localized branch with exact files, exact predicates (`is_supported_branch_union`, `root_row_id_ref`), and a test-first instruction for each candidate. It is conditional because whether apply-key/action-propagation need changes is genuinely determined by running Task 5, which is the honest TDD sequence. All other steps carry concrete code/commands.

**Type consistency:** `ImvDeltaNode.branch_scope: Option<crate::sql::catalog::BranchScope>` (Task 2) is read as `delta.branch_scope` in Task 3 (aggregate-state, join-delta, union-delta) and written as `Some(BranchScope { branch_id_column_name, branch_id })` in Task 4 — field names match `BranchScope` (`catalog.rs:161`: `branch_id_column_name`, `branch_id`). `build_aggregate_state_merge(aggregate, action_column, branch_scope, ext)` signature (`aggregate_rewrite.rs:84`) matches the Task-3 call. `ICEBERG_MV_BRANCH_ID_COLUMN` is the shared constant used in both branch_union.rs (already imported) and the new tests. ✓

---

## Execution Handoff

**Plan complete and saved to `docs/design/plans/2026-06-04-imv-property-framework.md`** (Phase 1 executable; Phases 2–3 are roadmap spines to expand into their own plans). Two execution options:

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — execute Phase 1 tasks in this session via executing-plans, batch execution with checkpoints.

**Which approach — and do you want to start Phase 1 now, or review the plan first?**
