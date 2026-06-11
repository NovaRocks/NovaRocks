# Query Rewrite Framework Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make `src/sql/optimizer/rewrite` the only query logical rewrite driver by migrating the existing RBO query rules into the new ordered rewrite pipeline.

**Architecture:** Keep the existing rule implementations local and mechanical, but change the execution contract to `LogicalRewriteRule`. The new query pipeline owns stage ordering, fixed-point execution, rule disable checks, and trace events. `optimize()` must not call `rbo::driver::rewrite_to_fixed_point` or directly invoke the old RBO driver path after this migration.

**Tech Stack:** Rust, NovaRocks logical plans, existing optimizer rule implementations, `cargo test`.

---

## File Structure

- Modify `src/sql/optimizer/rewrite/pipeline.rs`: add ordered stage support and expose stage-level fixed-point execution.
- Modify `src/sql/optimizer/rewrite/rule.rs`: add a local-rule adapter surface or a compatible result helper for migrated rules.
- Modify `src/sql/optimizer/rewrite/context.rs`: carry table statistics for query rewrite rules that need cost/cardinality information.
- Modify `src/sql/optimizer/rewrite/registry.rs`: build the full query rewrite pipeline in the legacy-safe order.
- Modify `src/sql/optimizer/mod.rs`: replace the manual RBO sequence with one `query_rewrite_pipeline(table_stats)` call.
- Modify `src/sql/optimizer/rbo/rule.rs`: remove or reduce the old production-only trait after migrated rules no longer use it.
- Modify `src/sql/optimizer/rbo/driver.rs`: remove from production use; delete if no tests require it.
- Modify `src/sql/optimizer/rbo/rules/**`: implement `LogicalRewriteRule` directly for predicate pushdown, join reorder, aggregate pushdown, column pruning, and UK/FK rules.

## Query Rewrite Order

The new framework must preserve the existing semantic order exactly:

1. `PredicatePushdownPreJoin`: fixed-point predicate pushdown rules.
2. `JoinReorder`: one fixed-point stage containing the join reorder rule.
3. `PredicatePushdownPostJoin`: fixed-point predicate pushdown rules again.
4. `AggregatePushdown`: fixed-point aggregate pushdown rules using table statistics.
5. `ColumnPruning`: fixed-point column pruning and UK/FK rules.

`ColumnPruning` must remain last. Predicate pushdown and join reorder must not be collapsed into a single unordered rule set.

## Task 1: Stage-Ordered Pipeline Contract

**Files:**
- Modify: `src/sql/optimizer/rewrite/pipeline.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [x] **Step 1: Add a failing test for duplicate phase stages**

Add a test proving two stages with the same `RewritePhase` run independently and in order. The first stage rewrites a `Values` node into a marker shape, and the second stage observes that marker. Expected failure before implementation: `RewritePipeline::new` cannot model ordered stages with labels.

- [x] **Step 2: Implement `RewriteStage`**

Add a `RewriteStage { name, phase, rules }` type and let `RewritePipeline` store `Vec<RewriteStage>` instead of one global rule list. Preserve `rule_names()` by flattening stages.

- [x] **Step 3: Run focused rewrite tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite -- --nocapture
```

Expected: all rewrite framework tests pass.

## Task 2: Query Statistics in Rewrite Context

**Files:**
- Modify: `src/sql/optimizer/rewrite/context.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [x] **Step 1: Add a failing test for query table stats access**

Add a test that builds `RewriteContext::for_query(...)`, attaches a `HashMap<String, TableStatistics>`, and reads it back from a test rule. Expected failure before implementation: no table-statistics accessor exists.

- [x] **Step 2: Add query stats accessors**

Store query table statistics in `RewriteContext` as optional read-only metadata. Keep MV refresh context behavior unchanged and do not make stats mandatory for MV.

- [x] **Step 3: Run focused context tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite::context -- --nocapture
```

Expected: context tests pass.

## Task 3: Migrate Existing RBO Rules to `LogicalRewriteRule`

**Files:**
- Modify: `src/sql/optimizer/rbo/rules/predicate_pushdown/*.rs`
- Modify: `src/sql/optimizer/rbo/rules/column_pruning.rs`
- Modify: `src/sql/optimizer/rbo/rules/ukfk.rs`
- Modify: `src/sql/optimizer/rbo/rules/aggregate_pushdown/rule.rs`
- Modify: `src/sql/optimizer/rbo/rules/join_reorder/rule.rs`
- Modify: `src/sql/optimizer/rbo/rules/mod.rs`

- [x] **Step 1: Add a failing registry test for migrated rule names**

Update the rewrite registry test so `query_rewrite_pipeline(...).rule_names()` contains all existing query rewrite rules:

```text
AggregatePushdown
EliminateUniqueAggregate
JoinReorder
PruneColumns
PruneUkFkJoin
PushDownPredicateAggregate
PushDownPredicateJoin
PushDownPredicateProject
PushDownPredicateScan
PushSemiAntiRightOnlyCondition
```

Expected failure before implementation: query rewrite registry is empty.

- [x] **Step 2: Change migrated rule implementations**

For each old `RewriteRule`, implement `LogicalRewriteRule` with:

- `phase()` set to `RewritePhase::StructuralRewrite` for pushdown, join reorder, aggregate pushdown, and column pruning.
- `traversal()` left as bottom-up unless the rule already requires root-first behavior.
- `matches(&self, plan, ctx)` delegating to the old local predicate.
- `apply(&self, plan, ctx)` returning `RewriteResult::Changed(next)` when the old rule produced `Some(next)` and `RewriteResult::Unchanged` otherwise.

- [x] **Step 3: Run focused RBO rule tests**

Run:

```bash
cargo test --lib sql::optimizer::rbo::rules -- --nocapture
```

Expected: existing rule tests pass under the new trait.

## Task 4: Replace `optimize()` Production Driver

**Files:**
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [x] **Step 1: Add a failing test proving query pipeline is not empty**

Update `optimize_accepts_empty_query_rewrite_pipeline` into a test that verifies the query pipeline includes migrated rules and accepts table stats.

- [x] **Step 2: Replace manual RBO sequence**

In `optimize()`, call the new query rewrite pipeline once:

```rust
let mut rewrite_ctx =
    rewrite::context::RewriteContext::for_query(session_settings.disabled_rules.clone());
rewrite_ctx.set_query_table_stats(table_stats.clone());
let rewritten = rewrite::registry::query_rewrite_pipeline(table_stats)
    .rewrite(plan, &mut rewrite_ctx)?;
```

Remove direct calls to `rbo::driver::rewrite_to_fixed_point` from `optimize()`.

- [x] **Step 3: Run optimizer RBO tests**

Run:

```bash
cargo test --lib sql::optimizer::rbo -- --nocapture
cargo test --lib sql::optimizer::is_known_rule_name_tests -- --nocapture
```

Expected: tests pass.

## Task 5: Remove Old Driver Surface

**Files:**
- Modify or delete: `src/sql/optimizer/rbo/driver.rs`
- Modify: `src/sql/optimizer/rbo/mod.rs`
- Modify: `src/sql/optimizer/rbo/rule.rs`

- [x] **Step 1: Search for old driver uses**

Run:

```bash
rg -n "rewrite_to_fixed_point|rbo::driver|trait RewriteRule|dyn RewriteRule" src/sql/optimizer
```

Expected: no production use remains after the query pipeline migration.

- [x] **Step 2: Delete or demote old driver code**

Delete `rbo::driver` if it is fully unused. If some tests still need local helpers, move those helpers into rewrite tests instead of keeping a second production driver.

- [x] **Step 3: Run compile-focused optimizer tests**

Run:

```bash
cargo test --lib sql::optimizer -- --nocapture
```

Expected: optimizer tests pass.

## Task 6: Final Verification

**Files:**
- Verify: all touched files

- [x] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: no formatting failures.

- [x] **Step 2: Run targeted tests**

Run:

```bash
cargo test --lib sql::optimizer::rewrite -- --nocapture
cargo test --lib sql::optimizer::rbo -- --nocapture
cargo test --lib sql::optimizer::is_known_rule_name_tests -- --nocapture
```

Expected: all tests pass.

- [x] **Step 3: Check for whitespace errors**

Run:

```bash
git diff --check
```

Expected: no output and exit code 0.
