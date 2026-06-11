# PR-β: IMV Delta / Version Marker Operators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the second half of the combined IMV-optimizer-foundation spec — introduce `ImvDelta` / `ImvVersion` logical marker operators, the root-plan `Delta(root)` wrap rule, and the Validation-phase convergence check — on top of PR-α's empty pipeline. After PR-β, every Iceberg MV refresh runs the IMV pipeline against a wrapped plan, the Validation phase rejects unresolved markers with a deterministic error, and the outcome is still discarded by `try_run_imv_rewrite_pipeline` so refresh semantics are unchanged.

**Architecture:** Pick representation **R1** from spec §8.3: extend `LogicalPlan` with two new variants. Exhaustive-match arms in non-IMV layers panic on marker leakage (compiler-enforced trip-wire). The `imv-delta-marker` stage (StructuralRewrite, TopDown) wraps a non-marker root in `ImvDelta`; the `imv-validation` stage (Validation, BottomUp) walks the plan and rejects when any `ImvDelta` / `ImvVersion` remains. `tree.rs` rewrite traversal gets two new pass-through arms, and its existing variant-coverage test is updated. The refresh-path glue keeps PR-α's log-and-continue swallow — Validation rejections are anticipated and non-fatal until task 4+ consumes the outcome.

**Tech Stack:** Rust 2021, NovaRocks `novarocks` crate, existing `src/sql/optimizer/rewrite/` framework (RewritePipeline / LogicalRewriteRule / RewriteContext / RewriteResult / RewriteTrace).

---

## Pre-flight

- This plan operates inside the worktree `/Users/harbor/.claude/worktrees/NovaRocks/quirky-greider-ba7bf0` on branch `claude/quirky-greider-ba7bf0`. Base is `50dd413e` (PR-α merge commit).
- Spec: [docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md](../specs/2026-05-26-incremental-mv-optimizer-foundation-design.md), sections §8, §11 (PR-β), §3 (split rationale).
- TODO: [Logical Delta / Version marker operators](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/logical-delta-version-marker-operators.md).
- PR-α handles already in place:
  - `src/sql/optimizer/rewrite/imv/{mod,entrypoint,annotation,pipeline}.rs`
  - Four named stages: `imv-logical-normalize` / `imv-delta-marker` / `imv-marker-cleanup` / `imv-validation` (all empty rule lists today).
  - `try_run_imv_rewrite_pipeline` at [src/engine/mv/iceberg_refresh.rs:6289](../../../src/engine/mv/iceberg_refresh.rs:6289) — log-and-continue, outcome discarded.
  - Trace summary helpers (`stage_names`, `changed_rules_count`, `rejected_rules_count`, `failed_rules_count`) already on `RewriteTrace`.
  - Variant-coverage tripwire test `rewrite_visits_all_logical_plan_variants` at [src/sql/optimizer/rewrite/tree.rs:425](../../../src/sql/optimizer/rewrite/tree.rs:425).

## Out of scope (documented in spec §8.2 / §11 / §12)

- **Filling `action_column`.** PR-β initializes it to `None`. Task 5 (Action column propagation) fills it.
- **Scan-side binding of `ImvVersion`.** PR-β defines `ImvVersionNode` but does **not** add a rule that emits it from a Scan. Task 4 (`Iceberg scan delta/version binding`) does that. PR-β only needs `ImvVersion` to exist so that the Validation check is provably symmetric with `ImvDelta`, and the variant-coverage tripwire is updated once.
- **Consuming the outcome.** `try_run_imv_rewrite_pipeline` keeps swallowing `Err` results. Task 4+ takes ownership of the outcome.
- **`sql-tests/optimizer/imv_marker_unresolved.sql` golden.** Spec §11 PR-β acceptance lists this, but spec §7.5 explicitly defers EXPLAIN-IMV-REFRESH integration to the lifecycle hardening task (roadmap task 12). Without an EXPLAIN entry point, the Validation reject message has no SQL surface to render in a golden. PR-β instead locks the message in a Rust unit test (`marker_unresolved_yields_rejected_outcome` below). The SQL golden moves to roadmap task 12.

## File Structure

**Create:**

- `src/sql/optimizer/rewrite/imv/marker.rs` — `ImvDeltaNode`, `ImvVersionNode`, `plan_contains_imv_marker`, `collect_marker_kinds`, `WrapRootInImvDeltaRule`, `UnresolvedMarkerCheckRule`, and all PR-β unit tests as `#[cfg(test)] mod tests` (mirrors PR-α's actual layout — PR-α did not create the `tests/` subdir from spec §4.1).

**Modify:**

- `src/sql/planner/plan.rs` — add `ImvDelta(ImvDeltaNode)` and `ImvVersion(ImvVersionNode)` variants to `LogicalPlan` (lines ~17-52). Node structs live in `imv/marker.rs` and are re-exported through here.
- `src/sql/optimizer/rewrite/tree.rs` — add `ImvDelta` / `ImvVersion` arms to `rewrite_children` (lines ~87-239) and to `assert_variant_handled` exhaustive-match tripwire (lines ~469-490).
- `src/sql/optimizer/rewrite/imv/mod.rs` — add `pub(crate) mod marker;`.
- `src/sql/optimizer/rewrite/imv/pipeline.rs` — register `WrapRootInImvDeltaRule` in `imv-delta-marker`, `UnresolvedMarkerCheckRule` in `imv-validation`.
- All other crate files that do exhaustive `match plan { LogicalPlan::... }` against the full enum — get a panic arm. The compiler enumerates these for free (the enum is non-`#[non_exhaustive]`); we run `cargo build` and patch every reported site. From a survey, the likely surface is in `src/sql/optimizer/convert.rs`, `src/sql/explain.rs`, `src/sql/optimizer/cte_rewrite.rs`, `src/sql/optimizer/mod.rs`, `src/sql/optimizer/stats.rs`, `src/sql/optimizer/rewrite/rules/**`, `src/sql/planner/mod.rs`, and `src/engine/mod.rs`. We rely on the compiler to enumerate every site rather than pre-grepping.
- `src/engine/mv/iceberg_refresh.rs` — update the doc-comment on `try_run_imv_rewrite_pipeline` (lines ~6277-6288) to reflect that the pipeline now actively wraps + validates instead of being a no-op; behavior is unchanged.

---

## Task 1: Define marker node types

**Files:**
- Create: `src/sql/optimizer/rewrite/imv/marker.rs`
- Modify: `src/sql/planner/plan.rs:17-52`
- Modify: `src/sql/optimizer/rewrite/imv/mod.rs`

The two marker variants must compile in isolation before any rule references them. `ImvDeltaNode.action_column` is `Option<crate::sql::column_id::ColumnId>` — `ColumnId` is the established stable identifier; spec §8.5 mandates `None` in PR-β and defers shape to task 5.

`ImvVersionNode` carries a `version_ref: ImvVersionRef` describing the snapshot window. PR-β only needs the type to exist for the Validation check; task 4 fills out the contents. We define a stub `ImvVersionRef` that can be expanded without changing call sites.

- [ ] **Step 1: Write the failing test**

Add to `src/sql/optimizer/rewrite/imv/marker.rs` (new file):

```rust
//! Logical marker operators for Incremental MV (IMV) rewrite. See
//! docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md §8.
//!
//! These markers must never reach physical lowering. The `imv-delta-marker`
//! stage of the IMV pipeline wraps the root; the `imv-validation` stage
//! rejects any plan that still carries a marker afterwards.

use crate::sql::column_id::ColumnId;
use crate::sql::planner::plan::LogicalPlan;

/// `Delta(plan)` — "compute the incremental of plan". Typically wraps the
/// root of an IMV refresh plan exactly once. `action_column` is the column
/// that will eventually carry the per-row INSERT / DELETE / UPDATE marker
/// once task 5 (Action column propagation) fills it; in PR-β it is always
/// `None`.
#[derive(Clone, Debug)]
pub(crate) struct ImvDeltaNode {
    pub input: Box<LogicalPlan>,
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
}

/// `Version(plan, version_ref)` — "scan plan over the snapshot window
/// described by `version_ref`". Task 4 (Iceberg scan delta/version binding)
/// emits this from Scan-replacing rules; PR-β only needs the type to exist.
#[derive(Clone, Debug)]
pub(crate) struct ImvVersionNode {
    pub input: Box<LogicalPlan>,
    pub version_ref: ImvVersionRef,
}

/// Snapshot window descriptor used by `ImvVersionNode`. PR-β leaves the
/// concrete fields to task 4; we only need a constructible placeholder so
/// the type is reachable from tests.
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvVersionRef {
    _private: (),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::planner::plan::{LogicalPlan, ValuesNode};

    fn empty_values_plan() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![],
        })
    }

    #[test]
    fn imv_delta_node_constructs_with_none_action_column() {
        let node = ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        };
        assert!(node.is_root);
        assert!(node.action_column.is_none());
    }

    #[test]
    fn imv_version_node_constructs_with_default_ref() {
        let node = ImvVersionNode {
            input: Box::new(empty_values_plan()),
            version_ref: ImvVersionRef::default(),
        };
        assert!(matches!(*node.input, LogicalPlan::Values(_)));
    }
}
```

Add to `src/sql/optimizer/rewrite/imv/mod.rs`:

```rust
pub(crate) mod marker;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: FAIL — `cannot find type 'ImvDeltaNode' in module` or `cannot find variant LogicalPlan::ImvDelta` — because step 3 has not added the enum variants yet. (If compile is clean, the test setup is wrong; re-check the imports.)

- [ ] **Step 3: Extend `LogicalPlan` enum with the two variants**

Modify `src/sql/planner/plan.rs:17-52`. Append two arms to the enum and add a `use` re-export of the node types so external crates don't have to know about the `imv` module. Replace the existing `enum LogicalPlan { ... CTEConsume(CTEConsumeNode), Decode(DecodeNode), }` with:

```rust
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Intersect(IntersectNode),
    Except(ExceptNode),
    Values(ValuesNode),
    GenerateSeries(GenerateSeriesNode),
    TableFunction(TableFunctionNode),
    Window(WindowNode),
    SubqueryAlias(SubqueryAliasNode),
    Repeat(RepeatPlanNode),
    CTEAnchor(CTEAnchorNode),
    CTEProduce(CTEProduceNode),
    CTEConsume(CTEConsumeNode),
    Decode(DecodeNode),
    /// IMV marker: "compute the incremental of input". Emitted by the
    /// `imv-delta-marker` stage; rejected by `imv-validation` if not
    /// consumed. Must never reach physical lowering. See
    /// `src/sql/optimizer/rewrite/imv/marker.rs`.
    ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode),
    /// IMV marker: "scan input over a snapshot window". Emitted by task 4
    /// scan-binding rules; consumed before lowering. Same panic-on-leak
    /// rule as `ImvDelta`.
    ImvVersion(crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode),
}
```

Adding these variants will break dozens of exhaustive matches across the crate. **Do not fix them in this task** — tasks 2 and 3 address them deliberately. The marker tests added in step 1 only depend on these variants being constructible.

- [ ] **Step 4: Run the marker tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: the two tests pass. The rest of the crate does **not** compile yet (every exhaustive match against `LogicalPlan` is now incomplete); `cargo test -p novarocks --lib` would fail. That's expected — task 2 and task 3 fix it.

If, against expectation, the rest of the crate already compiles, that means there are no exhaustive matches on `LogicalPlan` — verify by running `cargo build -p novarocks --lib` and confirming. In that case skip task 3.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/marker.rs \
        src/sql/optimizer/rewrite/imv/mod.rs \
        src/sql/planner/plan.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): add ImvDelta / ImvVersion marker variants to LogicalPlan

PR-β step 1 of the IMV optimizer foundation spec. Introduces two
logical-only marker operator variants on LogicalPlan. The wrap rule and
Validation check come in later tasks; this commit only adds the types.

Note: the crate-wide build is intentionally broken after this commit —
every exhaustive match on LogicalPlan must be updated, which is the
explicit work of the next two tasks. This is a tracked intermediate
state; do not merge this commit on its own.

Spec: docs/design/specs/2026-05-26-incremental-mv-optimizer-foundation-design.md §8.2 R1.
EOF
)"
```

---

## Task 2: Update tree.rs traversal for marker variants

**Files:**
- Modify: `src/sql/optimizer/rewrite/tree.rs:87-239` (`rewrite_children`)
- Modify: `src/sql/optimizer/rewrite/tree.rs:469-490` (`assert_variant_handled` in `rewrite_visits_all_logical_plan_variants`)

Markers wrap a single child plan, so traversal pushes the rule through the child just like `Filter` / `Project`. The pre-existing `rewrite_visits_all_logical_plan_variants` test has a compile-time tripwire — its exhaustive match must list every variant or compilation fails. We update both.

- [ ] **Step 1: Write the failing test**

In `src/sql/optimizer/rewrite/tree.rs`, append a new test inside the existing `#[cfg(test)] mod tests { ... }` block at the bottom:

```rust
#[test]
fn rewrite_traverses_into_imv_delta_child() {
    use crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode;
    use crate::sql::planner::plan::{LogicalPlan, ScanNode};

    let inner = LogicalPlan::Scan(ScanNode {
        database: "db".to_string(),
        table: table_def("before"),
        alias: None,
        columns: vec![output_column("c1")],
        predicates: vec![],
        required_columns: None,
        dict_columns: vec![],
    });

    let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
        input: Box::new(inner),
        is_root: true,
        action_column: None,
    });

    let mut ctx = RewriteContext::for_query(Vec::<String>::new());
    let (rewritten, changed) =
        rewrite_with_rule(plan, &RenameScanRule, &mut ctx).unwrap();

    assert!(changed, "RenameScanRule should rewrite the wrapped Scan");
    let LogicalPlan::ImvDelta(delta) = rewritten else {
        panic!("expected ImvDelta to remain at root after child rewrite");
    };
    let LogicalPlan::Scan(scan) = *delta.input else {
        panic!("expected Scan inside ImvDelta");
    };
    assert_eq!(scan.table.name, "after");
}
```

- [ ] **Step 2: Run test to verify it fails (and the whole module fails to compile)**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::tree -v`
Expected: FAIL — `rewrite_children` is a non-exhaustive match on `LogicalPlan` after task 1, so the file does not compile. The compiler error will list `ImvDelta` and `ImvVersion` as missing arms.

- [ ] **Step 3: Add traversal arms for both markers**

In `src/sql/optimizer/rewrite/tree.rs`, locate `rewrite_children` (starts around line 87) and add the two arms before the last existing arm (`LogicalPlan::Decode(node)`). They are structurally identical to `Filter` / `Project`:

```rust
        LogicalPlan::ImvDelta(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
        LogicalPlan::ImvVersion(node) => {
            let (input, changed) = rewrite_with_rule(*node.input, rule, ctx)?;
            Ok((
                LogicalPlan::ImvVersion(crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode {
                    input: Box::new(input),
                    ..node
                }),
                changed,
            ))
        }
```

Also update `assert_variant_handled` inside `rewrite_visits_all_logical_plan_variants` (lines ~469-490). Append `| LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_)` to the exhaustive `match variant { ... }`:

```rust
        fn assert_variant_handled(variant: &LogicalPlan) {
            match variant {
                LogicalPlan::Scan(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Project(_)
                | LogicalPlan::Aggregate(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Union(_)
                | LogicalPlan::Intersect(_)
                | LogicalPlan::Except(_)
                | LogicalPlan::Values(_)
                | LogicalPlan::GenerateSeries(_)
                | LogicalPlan::TableFunction(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Repeat(_)
                | LogicalPlan::CTEAnchor(_)
                | LogicalPlan::CTEProduce(_)
                | LogicalPlan::CTEConsume(_)
                | LogicalPlan::Decode(_)
                | LogicalPlan::ImvDelta(_)
                | LogicalPlan::ImvVersion(_) => {}
            }
        }
```

- [ ] **Step 4: Run the tree tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::tree -v`
Expected: all tree tests pass (5 + 1 = 6 tests), including the new `rewrite_traverses_into_imv_delta_child`.

The rest of the crate still fails to compile — task 3 closes that.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/tree.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): traverse children of ImvDelta / ImvVersion markers

Two new arms in rewrite_children plus the exhaustive-match tripwire in
the variant-coverage test. Markers are otherwise indistinguishable from
unary parent nodes (Filter / Project) during rewrite walks.

PR-β step 2.
EOF
)"
```

---

## Task 3: Add panic guards in non-IMV exhaustive matches

**Files:**
- Modify: every file the compiler reports as having a non-exhaustive `match` on `LogicalPlan`. From a survey, these likely sit under `src/sql/optimizer/convert.rs`, `src/sql/optimizer/cte_rewrite.rs`, `src/sql/optimizer/stats.rs`, `src/sql/optimizer/mod.rs`, `src/sql/optimizer/rewrite/rules/**`, `src/sql/explain.rs`, `src/sql/planner/mod.rs`, `src/engine/mod.rs`. Do not pre-list — let the compiler enumerate.

Spec §8.2 R1 mandates "exhaustive-match arms with `panic!('imv marker leaked into non-IMV plan')` are the most reliable guard against marker leak". The panic message is fixed text — same message everywhere — so a grep can find every guard later.

- [ ] **Step 1: Run cargo build to enumerate every broken site**

Run: `cargo build -p novarocks --lib 2>&1 | tee /tmp/imv-build-errors.log`
Expected: a list of "non-exhaustive patterns: `ImvDelta` and `ImvVersion` not covered" errors, one per offending match.

- [ ] **Step 2: For each reported site, add a panic arm**

The fix per site is mechanical. For an exhaustive `match plan { LogicalPlan::Scan(_) => ..., ... }`, add at the bottom:

```rust
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
```

Use the **exact same string** `"imv marker leaked into non-IMV plan"` everywhere — task 4's `plan_contains_imv_marker` test in task 6 grep-asserts this string is centralized, and `grep` becomes a maintenance audit.

If a site is a partial match (e.g. `if let LogicalPlan::Scan(...) = plan`, or `match plan { LogicalPlan::Scan(...) => ..., _ => ... }`), leave it alone — the compiler did not flag it because the `_` arm covers markers, and inserting a panic there would change behavior.

If a site is a structural traversal (e.g. another `rewrite_children`-like helper), add a panic arm with the same message **even though** it would be plausible to traverse. Reason: the markers must never appear in those traversals; if they ever do, a panic is the loudest signal we can emit.

- [ ] **Step 3: Rebuild until clean**

Run: `cargo build -p novarocks --lib`
Expected: clean compile, zero new warnings (panic arms are reachable from any `LogicalPlan` value, so no `unreachable_code` warning).

Iterate step 2 until the build is clean. If a site is hard to interpret, prefer the panic arm over `unreachable!()` — `unreachable!()` is a softer signal and the spec specifically calls out `panic!` as the trip-wire.

- [ ] **Step 4: Run the full lib test suite**

Run: `cargo test -p novarocks --lib`
Expected: every test that was passing on PR-α still passes (≈2832 tests). Tests must not trip a panic arm in normal operation, because no rule emits markers yet outside the IMV pipeline.

If any test panics with `"imv marker leaked into non-IMV plan"`, this is a real bug — task 3's panic arms are catching unintended marker construction. Stop and investigate before proceeding.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(optimizer): panic-arm guards against ImvDelta/ImvVersion leakage

Every non-IMV exhaustive match on LogicalPlan now panics with the
canonical message 'imv marker leaked into non-IMV plan' when it
encounters a marker. Spec §8.2 R1 designates this as the primary
trip-wire complementing the imv-validation pipeline stage.

PR-β step 3.
EOF
)"
```

---

## Task 4: Add marker helpers

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs` (extend, do not replace)

The wrap rule (task 5) and Validation rule (task 6) both need predicates that recognize markers. Centralize them now so both rules import the same helper and the unit tests can target them directly.

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` in `src/sql/optimizer/rewrite/imv/marker.rs`:

```rust
    #[test]
    fn plan_contains_imv_marker_false_for_plain_plan() {
        let plan = empty_values_plan();
        assert!(!plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_root_delta() {
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        });
        assert!(plan_contains_imv_marker(&plan));
    }

    #[test]
    fn plan_contains_imv_marker_true_for_nested_version() {
        use crate::sql::planner::plan::{FilterNode, ProjectItem, ProjectNode};
        // Build Project(Filter(ImvVersion(Values))). The marker is
        // deeply nested; the helper must recurse.
        let nested = LogicalPlan::ImvVersion(ImvVersionNode {
            input: Box::new(empty_values_plan()),
            version_ref: ImvVersionRef::default(),
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(nested),
            predicates: vec![],
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(filter),
            items: Vec::<ProjectItem>::new(),
        });
        assert!(plan_contains_imv_marker(&project));
    }

    #[test]
    fn collect_marker_kinds_reports_each_distinct_kind() {
        let delta = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(LogicalPlan::ImvVersion(ImvVersionNode {
                input: Box::new(empty_values_plan()),
                version_ref: ImvVersionRef::default(),
            })),
            is_root: true,
            action_column: None,
        });
        let mut kinds = collect_marker_kinds(&delta);
        kinds.sort();
        assert_eq!(kinds, vec!["ImvDelta", "ImvVersion"]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: FAIL — `plan_contains_imv_marker` and `collect_marker_kinds` undefined.

- [ ] **Step 3: Implement the helpers**

Append to `src/sql/optimizer/rewrite/imv/marker.rs` (above `#[cfg(test)] mod tests`):

```rust
/// Returns true if `plan` contains any `ImvDelta` or `ImvVersion` node at
/// any depth. The Validation stage uses this to detect unresolved markers.
pub(crate) fn plan_contains_imv_marker(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => true,
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => false,
        LogicalPlan::Filter(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Project(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Aggregate(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Sort(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Limit(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Window(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::TableFunction(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::SubqueryAlias(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Repeat(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::CTEProduce(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Decode(n) => plan_contains_imv_marker(&n.input),
        LogicalPlan::Join(n) => {
            plan_contains_imv_marker(&n.left) || plan_contains_imv_marker(&n.right)
        }
        LogicalPlan::CTEAnchor(n) => {
            plan_contains_imv_marker(&n.produce) || plan_contains_imv_marker(&n.consumer)
        }
        LogicalPlan::Union(n) => n.inputs.iter().any(plan_contains_imv_marker),
        LogicalPlan::Intersect(n) => n.inputs.iter().any(plan_contains_imv_marker),
        LogicalPlan::Except(n) => n.inputs.iter().any(plan_contains_imv_marker),
    }
}

/// Returns the distinct kinds of marker present in `plan`, in stable
/// order. Used by the Validation stage's error message.
pub(crate) fn collect_marker_kinds(plan: &LogicalPlan) -> Vec<&'static str> {
    let mut found: Vec<&'static str> = Vec::new();
    collect_into(plan, &mut found);
    found.sort();
    found.dedup();
    found
}

fn collect_into(plan: &LogicalPlan, found: &mut Vec<&'static str>) {
    match plan {
        LogicalPlan::ImvDelta(n) => {
            found.push("ImvDelta");
            collect_into(&n.input, found);
        }
        LogicalPlan::ImvVersion(n) => {
            found.push("ImvVersion");
            collect_into(&n.input, found);
        }
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => {}
        LogicalPlan::Filter(n) => collect_into(&n.input, found),
        LogicalPlan::Project(n) => collect_into(&n.input, found),
        LogicalPlan::Aggregate(n) => collect_into(&n.input, found),
        LogicalPlan::Sort(n) => collect_into(&n.input, found),
        LogicalPlan::Limit(n) => collect_into(&n.input, found),
        LogicalPlan::Window(n) => collect_into(&n.input, found),
        LogicalPlan::TableFunction(n) => collect_into(&n.input, found),
        LogicalPlan::SubqueryAlias(n) => collect_into(&n.input, found),
        LogicalPlan::Repeat(n) => collect_into(&n.input, found),
        LogicalPlan::CTEProduce(n) => collect_into(&n.input, found),
        LogicalPlan::Decode(n) => collect_into(&n.input, found),
        LogicalPlan::Join(n) => {
            collect_into(&n.left, found);
            collect_into(&n.right, found);
        }
        LogicalPlan::CTEAnchor(n) => {
            collect_into(&n.produce, found);
            collect_into(&n.consumer, found);
        }
        LogicalPlan::Union(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
        LogicalPlan::Intersect(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
        LogicalPlan::Except(n) => n.inputs.iter().for_each(|p| collect_into(p, found)),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: PASS (6 tests in this file now).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/marker.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): marker detection helpers for IMV validation

Adds plan_contains_imv_marker and collect_marker_kinds so the upcoming
wrap rule and Validation rule share a single notion of 'is this plan
marker-free?'. Both are pure recursive walks over LogicalPlan.

PR-β step 4.
EOF
)"
```

---

## Task 5: WrapRootInImvDeltaRule (imv-delta-marker stage)

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs` (extend)

The wrap rule registers in `imv-delta-marker` (StructuralRewrite phase). Traversal is **TopDown** with a `matches` that only fires at the root by checking whether the plan is already an `ImvDelta { is_root: true, .. }`. After it wraps, fixed-point iteration calls it again on the new root — at which point `matches` returns false (already wrapped), guaranteeing idempotency.

Mutation-guard guarantee comes for free: `apply` returns `RewriteResult::Changed(new_plan)` constructed from the input; if it ever returns `Err`, Rust value semantics keep the original binding in the caller's stack intact (already verified by PR-α's `failing_imv_rule_does_not_mutate_input_plan`).

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` in `src/sql/optimizer/rewrite/imv/marker.rs`:

```rust
    #[test]
    fn wrap_rule_wraps_plain_root_once() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::phase::RewritePhase;

        let plan = empty_values_plan();
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());

        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule)],
        )]);

        let out = pipeline.rewrite(plan, &mut ctx).unwrap();

        let LogicalPlan::ImvDelta(delta) = out else {
            panic!("expected ImvDelta at root");
        };
        assert!(delta.is_root);
        assert!(delta.action_column.is_none());
        assert!(matches!(*delta.input, LogicalPlan::Values(_)));
    }

    #[test]
    fn wrap_rule_is_idempotent_on_already_wrapped_plan() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::phase::RewritePhase;

        let already = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(empty_values_plan()),
            is_root: true,
            action_column: None,
        });
        let before = format!("{already:?}");

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule)],
        )]);

        let out = pipeline.rewrite(already, &mut ctx).unwrap();
        assert_eq!(format!("{out:?}"), before, "wrap must not double-wrap");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker::tests::wrap -v`
Expected: FAIL — `WrapRootInImvDeltaRule` undefined.

- [ ] **Step 3: Implement the rule**

The wrap rule must produce exactly one `ImvDelta { is_root: true, .. }` at the outermost position. Two facts shape the implementation:

1. The framework's TopDown traversal calls `apply_rule_to_node` at the root, then descends into children. Without a guard, the same rule would also fire at the `Values` child of the freshly-wrapped root and wrap *that*.
2. `RewriteContext` exposes no parent pointer, so the rule cannot infer "am I at the root" from the node alone.

We solve it with a one-shot `AtomicBool` flag inside the rule struct: set the flag when we wrap, and have `matches()` short-circuit on subsequent visits. Each `build_imv_pipeline()` call constructs a fresh rule instance, so the flag resets per refresh. Within a single refresh, fixed-point iteration of the `imv-delta-marker` stage re-enters `matches()` with the flag set, returns false, `phase_changed` stays false, the loop exits.

Append to `src/sql/optimizer/rewrite/imv/marker.rs` (above `#[cfg(test)] mod tests`):

```rust
use std::sync::atomic::{AtomicBool, Ordering};

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};

/// Wraps the root of an IMV refresh plan in `ImvDelta { is_root: true }`.
/// One-shot per pipeline run: the `wrapped` flag is set on the first
/// apply and short-circuits every subsequent `matches()` so the rule
/// fires exactly once even though TopDown traversal would otherwise
/// revisit the child of the new `ImvDelta`.
pub(crate) struct WrapRootInImvDeltaRule {
    wrapped: AtomicBool,
}

impl WrapRootInImvDeltaRule {
    pub(crate) fn new() -> Self {
        Self {
            wrapped: AtomicBool::new(false),
        }
    }
}

impl LogicalRewriteRule for WrapRootInImvDeltaRule {
    fn name(&self) -> &'static str {
        "WrapRootInImvDelta"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        if self.wrapped.load(Ordering::SeqCst) {
            return false;
        }
        // Plan was already wrapped before the pipeline ran (e.g. re-entry
        // on a previously wrapped plan): record the fact and skip.
        if matches!(
            plan,
            LogicalPlan::ImvDelta(ImvDeltaNode { is_root: true, .. })
        ) {
            self.wrapped.store(true, Ordering::SeqCst);
            return false;
        }
        true
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        self.wrapped.store(true, Ordering::SeqCst);
        Ok(RewriteResult::Changed(LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(plan),
            is_root: true,
            action_column: None,
        })))
    }
}
```

Update the two test bodies you added in Step 1 to instantiate the rule via `WrapRootInImvDeltaRule::new()` rather than `WrapRootInImvDeltaRule`. Both `vec![Box::new(WrapRootInImvDeltaRule)]` lines become `vec![Box::new(WrapRootInImvDeltaRule::new())]`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: all 8 marker tests pass (2 node, 4 helper, 2 wrap).

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/marker.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): WrapRootInImvDelta rule for imv-delta-marker stage

TopDown wrap rule with an AtomicBool one-shot guard — wraps the root in
ImvDelta { is_root: true, action_column: None } exactly once per
pipeline run. Idempotent across pipeline iterations and across re-runs
on an already-wrapped plan.

PR-β step 5.
EOF
)"
```

---

## Task 6: UnresolvedMarkerCheckRule (imv-validation stage)

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs` (extend)

The Validation rule must abort the pipeline with a deterministic message. Spec §8.4 specifies `format!("IVM rewrite failed to resolve incremental markers: {:?}", markers)` and uses `RewriteResult::Rejected`. With `RewriteContext::for_mv_refresh()`'s `FailFast` policy, `Rejected` flips into `Err(message)` inside `apply_rule_to_node`.

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` in `src/sql/optimizer/rewrite/imv/marker.rs`:

```rust
    #[test]
    fn marker_unresolved_yields_rejected_outcome() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::phase::RewritePhase;
        use crate::sql::optimizer::rewrite::trace::RewriteTraceEvent;

        // imv-delta-marker wraps; imv-validation rejects. Build a minimal
        // two-stage pipeline that mirrors the production stage names.
        let pipeline = RewritePipeline::from_stages(vec![
            RewriteStage::new(
                "imv-delta-marker",
                RewritePhase::StructuralRewrite,
                vec![Box::new(WrapRootInImvDeltaRule::new())],
            ),
            RewriteStage::new(
                "imv-validation",
                RewritePhase::Validation,
                vec![Box::new(UnresolvedMarkerCheckRule)],
            ),
        ]);

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let err = pipeline
            .rewrite(empty_values_plan(), &mut ctx)
            .expect_err("Validation must reject the wrapped-but-unconsumed plan");
        assert!(
            err.starts_with("IVM rewrite failed to resolve incremental markers:"),
            "unexpected error message: {err}"
        );
        assert!(err.contains("\"ImvDelta\""), "kind list missing: {err}");

        // Trace must record the rejection under the rule's name.
        assert!(ctx.trace().events().iter().any(|e| matches!(
            e,
            RewriteTraceEvent::RuleRejected { rule, .. }
                if *rule == "UnresolvedMarkerCheck"
        )));
    }

    #[test]
    fn validation_passes_when_no_marker_present() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
        use crate::sql::optimizer::rewrite::phase::RewritePhase;

        // Validation alone, no wrap rule. Plain plan → no marker → pass.
        let pipeline = RewritePipeline::from_stages(vec![RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![Box::new(UnresolvedMarkerCheckRule)],
        )]);

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        let out = pipeline
            .rewrite(empty_values_plan(), &mut ctx)
            .expect("plain plan must pass validation");
        assert!(matches!(out, LogicalPlan::Values(_)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: FAIL — `UnresolvedMarkerCheckRule` undefined.

- [ ] **Step 3: Implement the rule**

Append to `src/sql/optimizer/rewrite/imv/marker.rs` (above `#[cfg(test)] mod tests`):

```rust
use crate::sql::optimizer::rewrite::result::RewriteDiagnostic;

/// Validation-stage rule. Rejects any plan that still carries an IMV
/// marker (`ImvDelta` or `ImvVersion`) by the time the pipeline reaches
/// the `imv-validation` stage. The for-MV-refresh policy is FailFast,
/// so rejection becomes `Err(message)` for `run_imv_rewrite`'s caller.
pub(crate) struct UnresolvedMarkerCheckRule;

impl LogicalRewriteRule for UnresolvedMarkerCheckRule {
    fn name(&self) -> &'static str {
        "UnresolvedMarkerCheck"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::Validation
    }

    fn traversal(&self) -> RewriteTraversal {
        // We only need to fire once at the root. BottomUp means children
        // are visited first; matches() returns false for them (they are
        // not the outermost), and true only for the outermost node *if*
        // it carries a marker anywhere inside.
        //
        // For symmetry with the wrap rule we use TopDown — the rule
        // examines the whole plan once at the root and then rejects.
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        plan_contains_imv_marker(plan)
    }

    fn apply(
        &self,
        plan: LogicalPlan,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, String> {
        let markers = collect_marker_kinds(&plan);
        Ok(RewriteResult::Rejected(RewriteDiagnostic::rejected(
            "UnresolvedMarkerCheck",
            format!(
                "IVM rewrite failed to resolve incremental markers: {markers:?}"
            ),
        )))
    }
}
```

**Note on traversal**: TopDown means the rule fires at the root, then recurses into children. Once `apply` rejects, the framework aborts the phase (FailFast policy), so children are never visited. The same plan body would be rejected by BottomUp too — TopDown just rejects faster.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker -v`
Expected: all 10 marker tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/marker.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): UnresolvedMarkerCheck rule for imv-validation stage

Detects ImvDelta / ImvVersion remaining at Validation and rejects with
'IVM rewrite failed to resolve incremental markers: [..kinds..]'. Under
the for_mv_refresh FailFast policy this surfaces as Err to the
run_imv_rewrite caller, completing PR-β's marker-leak trip-wire.

PR-β step 6.
EOF
)"
```

---

## Task 7: Register rules in build_imv_pipeline

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`

The two new rules wire into the existing four-stage pipeline. Stage names and phases stay fixed.

- [ ] **Step 1: Write the failing test**

Append to `src/sql/optimizer/rewrite/imv/entrypoint.rs` inside the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn pr_beta_pipeline_runs_wrap_and_validation_against_plain_plan() {
        // End-to-end through run_imv_rewrite. Plain plan → wrap → validation
        // rejects → Err propagated to caller. This is PR-β's headline
        // behavior; iceberg-ivm continues to pass because
        // try_run_imv_rewrite_pipeline swallows the Err.
        let err = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect_err("PR-β pipeline must Reject on plain plan");
        assert!(
            err.starts_with("IVM rewrite failed to resolve incremental markers:"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pr_beta_pipeline_passes_when_wrap_rule_disabled() {
        // If the user disables WrapRootInImvDelta, no marker is produced,
        // and Validation has nothing to reject. Confirms the disable
        // wire-up reaches the new rule.
        let outcome = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: vec!["WrapRootInImvDelta".to_string()],
            deadline: None,
        })
        .expect("disabled wrap rule must let the pipeline succeed");

        // outcome.plan must still be the original (no marker added).
        assert!(matches!(outcome.plan, LogicalPlan::Values(_)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::entrypoint::tests::pr_beta -v`
Expected: FAIL — both tests pass through `run_imv_rewrite` but the pipeline is still empty (PR-α state), so the wrap rule never fires.
The first test fails because `expect_err` flips to "expected Err, got Ok".
The second test passes by accident (no rule = no marker = Validation accepts). That's fine — we'll let it pass through to step 3.

- [ ] **Step 3: Register the rules**

Replace `src/sql/optimizer/rewrite/imv/pipeline.rs` entirely with:

```rust
//! IMV rewrite pipeline construction. PR-α: four named no-op stages.
//! PR-β: register marker rules in `imv-delta-marker` and `imv-validation`.

use crate::sql::optimizer::rewrite::imv::marker::{
    UnresolvedMarkerCheckRule, WrapRootInImvDeltaRule,
};
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::pipeline::{RewritePipeline, RewriteStage};
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) fn build_imv_pipeline() -> RewritePipeline {
    RewritePipeline::from_stages(vec![
        RewriteStage::new(
            "imv-logical-normalize",
            RewritePhase::LogicalNormalize,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-delta-marker",
            RewritePhase::StructuralRewrite,
            vec![Box::new(WrapRootInImvDeltaRule::new()) as Box<dyn LogicalRewriteRule>],
        ),
        RewriteStage::new(
            "imv-marker-cleanup",
            RewritePhase::SemanticRewrite,
            Vec::new(),
        ),
        RewriteStage::new(
            "imv-validation",
            RewritePhase::Validation,
            vec![Box::new(UnresolvedMarkerCheckRule) as Box<dyn LogicalRewriteRule>],
        ),
    ])
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv -v`
Expected: all IMV tests pass — the PR-α tests (6 from entrypoint, 1 from annotation if present, 8 from marker after step 6) plus the 2 new PR-β entrypoint tests = 10+ pass.

**Note:** PR-α's `empty_imv_pipeline_returns_input_plan_verbatim` test will now FAIL because the pipeline is no longer a no-op. We need to **rename** it and reframe its assertion. Same for `empty_pipeline_traces_all_four_stage_names` — the stage list is unchanged, so this test should still pass.

Edit `src/sql/optimizer/rewrite/imv/entrypoint.rs` `tests` module:

```rust
    #[test]
    fn imv_pipeline_returns_err_on_plain_plan_in_pr_beta() {
        // PR-α: pipeline was identity. PR-β: wrap+validation rejects.
        // This test preserves the spirit of the original
        // empty_imv_pipeline_returns_input_plan_verbatim test by checking
        // the marker-rejection contract rather than identity.
        let err = run_imv_rewrite(ImvRewriteInput {
            plan: empty_values_plan(),
            mv_ctx: dummy_mv_ctx(),
            disabled_rules: Vec::new(),
            deadline: None,
        })
        .expect_err("PR-β pipeline rejects plain plans");
        assert!(err.starts_with("IVM rewrite failed to resolve incremental markers:"));
    }
```

Delete the old `empty_imv_pipeline_returns_input_plan_verbatim` definition.

- [ ] **Step 5: Run the full lib test suite**

Run: `cargo test -p novarocks --lib`
Expected: ≈2832 + ~4 new tests pass. Crucially, no test that was green on PR-α now fails (apart from the deliberately renamed `empty_imv_pipeline_*` above).

If any pre-existing test fails, the most likely cause is task 3's panic arms catching unintended marker construction somewhere — investigate before continuing.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/pipeline.rs \
        src/sql/optimizer/rewrite/imv/entrypoint.rs
git commit -m "$(cat <<'EOF'
feat(optimizer): register PR-β marker rules in build_imv_pipeline

WrapRootInImvDelta lands in imv-delta-marker (StructuralRewrite),
UnresolvedMarkerCheck lands in imv-validation (Validation). Updates the
PR-α 'empty pipeline returns input verbatim' test to match the new
PR-β contract: plain plans are wrapped, then rejected by Validation,
returning Err to run_imv_rewrite's caller.

PR-β step 7.
EOF
)"
```

---

## Task 8: Regression test — regular SELECT path doesn't emit markers

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs` (extend `#[cfg(test)]`)

Spec §11 PR-β acceptance: "A regular SELECT query optimizer run (non-IMV pipeline) does not produce markers in its output plan." This is a defense-in-depth test against accidental cross-wiring of the wrap rule into `RewritePipeline::new`.

- [ ] **Step 1: Write the failing test**

Append to `#[cfg(test)] mod tests` in `src/sql/optimizer/rewrite/imv/marker.rs`:

```rust
    #[test]
    fn regular_query_pipeline_does_not_produce_markers() {
        use crate::sql::optimizer::rewrite::context::RewriteContext;
        use crate::sql::optimizer::rewrite::pipeline::RewritePipeline;
        use crate::sql::optimizer::rewrite::phase::RewritePhase;

        // RewritePipeline::new is the constructor used by the query
        // rewrite path. With no rules, it must leave the plan unchanged
        // and never introduce a marker.
        let pipeline = RewritePipeline::new(
            vec![
                RewritePhase::LogicalNormalize,
                RewritePhase::StructuralRewrite,
                RewritePhase::SemanticRewrite,
                RewritePhase::Validation,
            ],
            Vec::new(),
        );
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        let out = pipeline
            .rewrite(empty_values_plan(), &mut ctx)
            .expect("query pipeline must not error on plain plan");
        assert!(
            !plan_contains_imv_marker(&out),
            "non-IMV pipeline must not emit markers, got {out:?}"
        );
    }
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p novarocks --lib sql::optimizer::rewrite::imv::marker::tests::regular_query -v`
Expected: PASS on first try — the assertion is a regression guard, not a behavior change.

- [ ] **Step 3: Commit**

```bash
git add src/sql/optimizer/rewrite/imv/marker.rs
git commit -m "$(cat <<'EOF'
test(optimizer): non-IMV pipeline must not produce IMV markers

Defensive regression check: RewritePipeline::new with no rules must
return a marker-free plan. Locks the spec §11 PR-β acceptance criterion.

PR-β step 8.
EOF
)"
```

---

## Task 9: Update try_run_imv_rewrite_pipeline doc-comment

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:6277-6334`

The function's behavior does **not** change in PR-β — it still log-and-continues on `Err`. But the doc-comment says "PR-α no-op pipeline" and now lies. Update only the docstring.

- [ ] **Step 1: Read the existing docstring**

Run: `sed -n '6277,6290p' src/engine/mv/iceberg_refresh.rs`
Expected output: the existing doc-comment that mentions "PR-α no-op IMV optimizer pipeline".

- [ ] **Step 2: Update the docstring**

Modify `src/engine/mv/iceberg_refresh.rs` at line ~6277. Replace the existing doc-comment block immediately before `fn try_run_imv_rewrite_pipeline` with:

```rust
/// Run the IMV optimizer pipeline against `ctx`, discarding the outcome.
/// Logs a structured summary on success and a warning on failure.
///
/// PR-β state: the pipeline now actively wraps the root in `ImvDelta`
/// (imv-delta-marker stage) and rejects unresolved markers in
/// imv-validation. Until task 4+ adds rules that consume the marker,
/// every refresh attempt produces a `Validation` Reject — that is
/// expected and non-fatal here. Refresh continues with the hand-built
/// path; this function only logs the failure as a warning so a future
/// task can audit IMV-pipeline progress without breaking refresh
/// behavior.
///
/// The original `?`-fail-fast wiring was tightened to log-and-continue
/// in PR-α after the iceberg-ivm suite exposed an A11 schema-evolution
/// case (renamed referenced column) where re-planning the canonical
/// select against the latest base schema fails by design even though
/// the hand-built refresh path handles the rename correctly. PR-β
/// inherits that swallow.
```

- [ ] **Step 3: Verify the file still builds**

Run: `cargo build -p novarocks --lib`
Expected: clean build (docstring changes do not affect codegen).

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "$(cat <<'EOF'
docs(mv): update try_run_imv_rewrite_pipeline doc for PR-β state

The pipeline is no longer no-op — it wraps and validates. Behavior of
this swallow-and-warn helper is unchanged; only the docstring is
updated to reflect the new pipeline contents.

PR-β step 9.
EOF
)"
```

---

## Task 10: Verification gates

**Files:** none modified — all of these are checks.

These match spec §9.4 PR-α suite gates, with two adjustments:
- the lib test count grows by ~4–6 from PR-α baseline (tests added in tasks 1, 4, 5, 6, 7, 8);
- `iceberg-ivm` still 61/61 (PR-β must not regress; the swallow guarantees this).

- [ ] **Step 1: `cargo fmt --check`**

Run: `cargo fmt -- --check`
Expected: clean. If not, run `cargo fmt` then re-run check.

- [ ] **Step 2: `cargo clippy`**

Run: `cargo clippy -p novarocks --lib --all-targets 2>&1 | tee /tmp/clippy-prbeta.log`
Expected: zero new warnings vs. PR-α baseline. (PR-α has known "field never read" warnings on `ImvPlanAnnotation::_private` and similar dead-code-by-design items — those are inherited and acceptable. New `ImvVersionRef::_private` will produce one more in the same family; that is acceptable too.)

If clippy flags `match` redundancy in the panic arms (`|`-collapsible patterns), keep them as written — the goal is explicit `ImvDelta(_) | ImvVersion(_)` not stylistic minimization. Mark with `#[allow(clippy::match_same_arms)]` only if necessary.

- [ ] **Step 3: `cargo build` (binary + lib)**

Run: `cargo build -p novarocks --lib && cargo build --bin novarocks`
Expected: clean. The binary build catches any non-`lib` exhaustive matches the lib build missed (e.g. in `src/main.rs` or `src/bin/`).

- [ ] **Step 4: `cargo test -p novarocks --lib`**

Run: `cargo test -p novarocks --lib 2>&1 | tail -40`
Expected: roughly 2832 + 4–6 new = 2836–2838 pass; the same handful of pre-existing ignored tests; no new failures.

- [ ] **Step 5: `iceberg-ivm` suite (this is the critical gate)**

Set up the standalone server per the project's iceberg-rest fixture:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
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

Then run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
```

Expected: **61/61 PASS** (unchanged from PR-α). If any case fails, the most likely cause is that `try_run_imv_rewrite_pipeline`'s swallow is not catching a panic (panic propagation from the panic arms added in task 3) — investigate the panic message in the server log.

- [ ] **Step 6: `iceberg` and `iceberg-rest` baseline checks**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --mode verify
```

Expected: baselines unchanged (`iceberg` typically 24/24, `iceberg-rest` 9/9 — verify against PR-α gate numbers).

- [ ] **Step 7: Stop the server**

```bash
kill "$SRV_PID" 2>/dev/null || true
wait "$SRV_PID" 2>/dev/null || true
```

- [ ] **Step 8: PR readiness — confirm the chain of commits is clean**

Run: `git log --oneline 50dd413e..HEAD`
Expected: 9 commits, one per task 1–9, plus task 10 if anything was tweaked. No `WIP` or `fixup` commits.

If task 10 surfaced changes (e.g. clippy fixes), commit them separately:

```bash
git add -A
git commit -m "chore(optimizer): PR-β verification fixups"
```

---

## Self-Review (filled in at plan-writing time)

**1. Spec coverage:**
- ✅ §8.1 Marker semantics — tasks 1, 5, 6 cover Delta wrap and Version type.
- ✅ §8.2 R1 representation — task 1 extends `LogicalPlan`; task 3 adds panic guards.
- ✅ §8.3 Default starting position — R1 confirmed in task 1.
- ✅ §8.4 Convergence check rule — task 6 implements `UnresolvedMarkerCheckRule` with the exact error message.
- ✅ §8.5 Action column placeholder — task 1 stores `Option<ColumnId>` with `None` default.
- ✅ §8.6 Tests — all four named tests are present:
  - `marker_unresolved_yields_rejected_outcome` → task 6 step 1.
  - `marker_wrap_idempotent` → task 5 step 1 (named `wrap_rule_is_idempotent_on_already_wrapped_plan`; rename if strict naming match required).
  - `mutation_guard_on_apply_error` → covered by PR-α's pre-existing `failing_imv_rule_does_not_mutate_input_plan`; spec-mandated rename not done because PR-α's variant is identical in spirit. If strict naming is required, add a thin wrapper test in task 6 that names itself `mutation_guard_on_apply_error`.
  - `regular_query_pipeline_does_not_produce_markers` → task 8.
- ✅ §11 PR-β acceptance — all bullets except the SQL golden (explicitly deferred to roadmap task 12; rationale in the "Out of scope" preamble).
- ✅ §3 split rationale — PR-β preserves refresh semantics via `try_run_imv_rewrite_pipeline`'s swallow.

**2. Placeholder scan:** no "TODO", no "implement later", no "similar to". Every step contains real code.

**3. Type consistency:**
- `ImvDeltaNode` / `ImvVersionNode` / `ImvVersionRef` consistently named.
- `WrapRootInImvDeltaRule::new()` constructor used in every test instantiation.
- Panic message `"imv marker leaked into non-IMV plan"` is the single string used everywhere in task 3.
- Validation reject message `"IVM rewrite failed to resolve incremental markers: {markers:?}"` is the single string used in task 6 and asserted in tests.

---

## Execution Handoff

Plan complete and saved. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task with two-stage review between tasks. Best when task 3 (panic-arm rollout across the crate) is uncertain in scope.
2. **Inline Execution** — execute tasks in this session with checkpoints. Faster turnaround; review surface is the diff after each commit.
