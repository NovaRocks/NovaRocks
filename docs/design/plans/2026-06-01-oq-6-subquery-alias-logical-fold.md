# OQ-6 SubqueryAlias Logical Fold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `SubqueryAlias` from NovaRocks logical, memo, physical, and codegen plan layers while preserving derived-table and CTE alias semantics through analyzer output metadata and ordinary `Project` adapters.

**Architecture:** Alias remains an analysis-layer scope concept only. Planner adapts derived-table and inline-CTE outputs with a reusable `adapt_plan_output` helper, then optimizer and codegen operate only on real relational operators.

**Tech Stack:** Rust, NovaRocks standalone SQL analyzer/planner/optimizer/codegen, `sql-tests` optimizer suite.

---

## File Structure

- Modify `src/sql/planner/mod.rs`: add `plan_output_columns` and `adapt_plan_output`; stop producing `LogicalPlan::SubqueryAlias`; update planner unit tests.
- Modify `src/sql/planner/plan.rs`: remove `LogicalPlan::SubqueryAlias` and `SubqueryAliasNode`; update comments and tests.
- Modify `src/sql/optimizer/cte_rewrite.rs`: return `Result` from CTE inline cleanup and use `adapt_plan_output` for single-use CTE replacement.
- Modify `src/sql/optimizer/mod.rs`: propagate `cte_rewrite::inline_single_use_ctes` errors with `?`.
- Modify `src/sql/optimizer/operator.rs`: remove logical and physical subquery alias operators.
- Modify `src/sql/optimizer/convert.rs`: remove conversion for `LogicalPlan::SubqueryAlias`.
- Modify `src/sql/optimizer/cascades_rules/implement.rs` and `src/sql/optimizer/cascades_rules/mod.rs`: remove `SubqueryAliasToPhysical`.
- Modify `src/sql/optimizer/stats.rs`, `src/sql/optimizer/logical_props.rs`, `src/sql/optimizer/cost.rs`, `src/sql/optimizer/derive/mod.rs`, and `src/sql/optimizer/derive/passthrough.rs`: remove alias handling.
- Modify `src/sql/explain.rs`: remove logical and physical alias formatting.
- Modify `src/sql/codegen/fragment_builder.rs`: remove `PhysicalSubqueryAliasOp` import, dispatch branch, and `visit_subquery_alias`.
- Modify `src/sql/optimizer/rewrite/tree.rs`, `src/sql/optimizer/rewrite/required_columns.rs`, `src/sql/optimizer/rewrite/registry.rs`, `src/sql/optimizer/rewrite/rules/mod.rs`, and affected rewrite rule modules: remove alias traversal and rule registration.
- Delete `src/sql/optimizer/rewrite/rules/column_pruning/prune_subquery_alias.rs`.
- Modify `src/engine/mod.rs`: remove alias traversal in scan-stat collection.
- Modify `src/sql/column_id.rs` and `src/sql/analyzer/scope.rs`: update stale comments so they describe derived-table output metadata, not a plan node.
- Create `sql-tests/optimizer/sql/subquery_alias_fold.sql`.
- Create `sql-tests/optimizer/result/subquery_alias_fold.result` by recording the new optimizer case.
- Modify `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md`: mark OQ-6 complete after verification.

## Task 1: Planner Output Adapter

**Files:**
- Modify: `src/sql/planner/mod.rs`

- [ ] **Step 1: Add failing unit tests for output adaptation**

Append these tests inside the existing `#[cfg(test)] mod tests` in `src/sql/planner/mod.rs`:

```rust
#[test]
fn adapt_plan_output_passthrough_when_outputs_match() {
    let source_id = ColumnId::new_for_test(10);
    let input = LogicalPlan::Values(ValuesNode {
        rows: vec![],
        columns: vec![OutputColumn {
            column_id: source_id,
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        required_output_columns: None,
    });
    let target = vec![OutputColumn {
        column_id: source_id,
        name: "k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
    assert!(matches!(adapted, LogicalPlan::Values(_)));
}

#[test]
fn adapt_plan_output_renames_and_rebinds_with_project() {
    let source_id = ColumnId::new_for_test(10);
    let target_id = ColumnId::new_for_test(20);
    let input = LogicalPlan::Values(ValuesNode {
        rows: vec![],
        columns: vec![OutputColumn {
            column_id: source_id,
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        required_output_columns: None,
    });
    let target = vec![OutputColumn {
        column_id: target_id,
        name: "alias_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
    let LogicalPlan::Project(project) = adapted else {
        panic!("expected Project adapter");
    };
    assert_eq!(project.items.len(), 1);
    assert_eq!(project.items[0].output_name, "alias_k");
    assert_eq!(project.items[0].output_column_id, target_id);
    let ExprKind::ColumnRef { column_id, column, .. } = &project.items[0].expr.kind else {
        panic!("expected adapter item to read child column");
    };
    assert_eq!(*column_id, source_id);
    assert_eq!(column, "k");
}

#[test]
fn adapt_plan_output_rejects_shape_mismatch() {
    let input = LogicalPlan::Values(ValuesNode {
        rows: vec![],
        columns: vec![],
        required_output_columns: None,
    });
    let target = vec![OutputColumn {
        column_id: ColumnId::new_for_test(20),
        name: "alias_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let err = adapt_plan_output(input, &target).expect_err("adapter should reject arity mismatch");
    assert!(
        err.contains("output column count mismatch"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib sql::planner::tests::adapt_plan_output_ -- --nocapture
```

Expected: compile failure mentioning `cannot find function adapt_plan_output`.

- [ ] **Step 3: Implement `plan_output_columns` and `adapt_plan_output`**

Add these helpers in `src/sql/planner/mod.rs` after `plan_body_scoped` and before `plan_select_scoped`:

```rust
pub(crate) fn plan_output_columns(plan: &LogicalPlan) -> Result<Vec<OutputColumn>, String> {
    match plan {
        LogicalPlan::Scan(node) => Ok(node.columns.clone()),
        LogicalPlan::Filter(node) => plan_output_columns(&node.input),
        LogicalPlan::Project(node) => Ok(node
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
            .collect()),
        LogicalPlan::Aggregate(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Join(node) => {
            let left = plan_output_columns(&node.left)?;
            let right = plan_output_columns(&node.right)?;
            Ok(join_output_columns(node.join_type, left, right))
        }
        LogicalPlan::Sort(node) => plan_output_columns(&node.input),
        LogicalPlan::Limit(node) => plan_output_columns(&node.input),
        LogicalPlan::Union(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Intersect(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Except(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Values(node) => Ok(node.columns.clone()),
        LogicalPlan::GenerateSeries(node) => Ok(vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: node.column_name.clone(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }]),
        LogicalPlan::TableFunction(node) => {
            let mut columns = plan_output_columns(&node.input)?;
            columns.extend(node.output_columns.clone());
            Ok(columns)
        }
        LogicalPlan::Window(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Repeat(node) => plan_output_columns(&node.input),
        LogicalPlan::CTEAnchor(node) => plan_output_columns(&node.consumer),
        LogicalPlan::CTEProduce(node) => Ok(node.output_columns.clone()),
        LogicalPlan::CTEConsume(node) => Ok(node.output_columns.clone()),
        LogicalPlan::Decode(node) => Ok(node.output_columns.clone()),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            Err("imv marker leaked into non-IMV planner output adaptation".to_string())
        }
    }
}

pub(crate) fn adapt_plan_output(
    input: LogicalPlan,
    target_output_columns: &[OutputColumn],
) -> Result<LogicalPlan, String> {
    let source_output_columns = plan_output_columns(&input)?;
    if source_output_columns.len() != target_output_columns.len() {
        return Err(format!(
            "output column count mismatch while adapting subquery/CTE output: child has {}, target has {}",
            source_output_columns.len(),
            target_output_columns.len()
        ));
    }

    if source_output_columns
        .iter()
        .zip(target_output_columns.iter())
        .all(|(source, target)| output_column_metadata_equal(source, target))
    {
        return Ok(input);
    }

    let mut items = Vec::with_capacity(target_output_columns.len());
    for (source, target) in source_output_columns.iter().zip(target_output_columns.iter()) {
        if source.data_type != target.data_type {
            return Err(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name, source.data_type, target.data_type
            ));
        }
        if source.nullable != target.nullable {
            return Err(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name, source.nullable, target.nullable
            ));
        }
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: source.column_id,
                    qualifier: None,
                    column: source.name.clone(),
                },
                data_type: source.data_type.clone(),
                nullable: source.nullable,
            },
            output_name: target.name.clone(),
            output_column_id: target.column_id,
        });
    }

    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(input),
        items,
        required_output_columns: None,
    }))
}

fn output_column_metadata_equal(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.data_type == right.data_type
        && left.nullable == right.nullable
        && left.is_internal == right.is_internal
}

fn join_output_columns(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(make_nullable(right));
            out
        }
        JoinKind::RightOuter => {
            let mut out = make_nullable(left);
            out.extend(right);
            out
        }
        JoinKind::FullOuter => {
            let mut out = make_nullable(left);
            out.extend(make_nullable(right));
            out
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

fn make_nullable(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}
```

- [ ] **Step 4: Run adapter tests**

Run:

```bash
cargo test --lib sql::planner::tests::adapt_plan_output_ -- --nocapture
```

Expected: all three adapter tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql/planner/mod.rs
git commit -m "feat(planner): add output adapter for alias-free subqueries"
```

## Task 2: Stop Planning Derived Tables As `SubqueryAlias`

**Files:**
- Modify: `src/sql/planner/mod.rs`

- [ ] **Step 1: Add failing planner tests for derived table folding**

Append these tests inside `#[cfg(test)] mod tests` in `src/sql/planner/mod.rs`:

```rust
#[test]
fn derived_table_plans_without_subquery_alias_node() {
    let plan = parse_analyze_and_plan(
        "SELECT s.o_orderkey FROM (SELECT o_orderkey FROM orders) s",
    )
    .expect("planner should succeed");

    let debug = format!("{plan:?}");
    assert!(
        !debug.contains("SubqueryAlias"),
        "derived table must not create SubqueryAlias: {debug}"
    );
}

#[test]
fn derived_table_column_alias_uses_project_adapter() {
    let plan = parse_analyze_and_plan(
        "SELECT s.ok FROM (SELECT o_orderkey FROM orders) s(ok)",
    )
    .expect("planner should succeed");

    let debug = format!("{plan:?}");
    assert!(
        !debug.contains("SubqueryAlias"),
        "column alias derived table must not create SubqueryAlias: {debug}"
    );

    let lines =
        crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Verbose);
    assert!(
        lines.iter().any(|line| line.contains("PROJECT [ok]")),
        "expected Project adapter to expose column alias ok: {lines:?}"
    );
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib sql::planner::tests::derived_table_ -- --nocapture
```

Expected: first test fails because debug output contains `SubqueryAlias`.

- [ ] **Step 3: Change `Relation::Subquery` planning**

In `src/sql/planner/mod.rs`, replace the current `Relation::Subquery` arm in `plan_relation_scoped` with:

```rust
        Relation::Subquery {
            query,
            alias: _,
            output_columns,
        } => {
            let inner_plan = plan_scoped_query(*query, cte_registry, factory)?;
            adapt_plan_output(inner_plan, &output_columns)
        }
```

- [ ] **Step 4: Remove alias-aware window ordering branch**

In `logical_plan_satisfies_window_ordering`, replace the match with:

```rust
    match input {
        LogicalPlan::Sort(sort) => {
            logical_sort_satisfies_window_ordering(sort, required_items, partition_by)
        }
        _ => false,
    }
```

- [ ] **Step 5: Update planner tests that expected `SubqueryAlias`**

In `src/sql/planner/mod.rs`, update tests whose names or assertions mention `SubqueryAlias`:

```rust
fn find_subquery_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Project(node) => find_subquery_input(&node.input),
        LogicalPlan::Sort(node) => find_subquery_input(&node.input),
        LogicalPlan::Limit(node) => find_subquery_input(&node.input),
        other => Some(other),
    }
}
```

For set-op tests, replace alias navigation with Project-adapter or direct set-op navigation:

```rust
fn unwrap_project_input(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Project(project) => project.input.as_ref(),
        other => other,
    }
}
```

Use this assertion pattern:

```rust
let child = unwrap_project_input(&plan);
let union_node = match child {
    LogicalPlan::Union(n) => n,
    other => panic!("expected Union without SubqueryAlias, got {other:?}"),
};
```

- [ ] **Step 6: Run planner tests**

Run:

```bash
cargo test --lib sql::planner::tests::derived_table_ sql::planner::tests::window_reuses_ordering_through_subquery_alias -- --nocapture
```

Expected: derived table tests pass, and the existing window-ordering test passes after its assertions are updated to require no alias.

- [ ] **Step 7: Commit**

```bash
git add src/sql/planner/mod.rs
git commit -m "feat(planner): fold derived table aliases before optimization"
```

## Task 3: Inline Single-Use CTEs Without `SubqueryAlias`

**Files:**
- Modify: `src/sql/optimizer/cte_rewrite.rs`
- Modify: `src/sql/optimizer/mod.rs`

- [ ] **Step 1: Add failing CTE inline tests**

In `src/sql/optimizer/cte_rewrite.rs`, replace `test_inline_single_use_cte_removes_anchor` with:

```rust
#[test]
fn test_inline_single_use_cte_removes_anchor_without_alias_node() {
    let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
        cte_id: 1,
        produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: 1,
            input: Box::new(scan_plan()),
            output_columns: output_columns(),
            required_output_columns: None,
        })),
        consumer: Box::new(consume_plan(1, "t")),
        required_output_columns: None,
    });

    let ctx = collect_cte_counts(&plan);
    let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");
    assert!(
        !format!("{rewritten:?}").contains("SubqueryAlias"),
        "single-use CTE inline must not create SubqueryAlias: {rewritten:?}"
    );
    assert!(matches!(rewritten, LogicalPlan::Scan(_) | LogicalPlan::Project(_)));
}
```

Replace `test_inline_single_use_cte_preserves_alias_namespace` with:

```rust
#[test]
fn test_inline_single_use_cte_preserves_consumer_output_columns_with_project() {
    let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
        cte_id: 1,
        produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: 1,
            input: Box::new(scan_plan()),
            output_columns: output_columns(),
            required_output_columns: None,
        })),
        consumer: Box::new(consume_plan(1, "x")),
        required_output_columns: None,
    });

    let ctx = collect_cte_counts(&plan);
    let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

    let output = crate::sql::planner::plan_output_columns(&rewritten)
        .expect("rewritten output columns should be derivable");
    assert_output_columns_match(&output);
    assert!(
        !format!("{rewritten:?}").contains("SubqueryAlias"),
        "inline result must not contain SubqueryAlias: {rewritten:?}"
    );
}
```

Update these existing CTE rewrite tests so the new `Result` return type is unwrapped explicitly:

```rust
// In test_inline_single_use_cte_keeps_multi_use_anchor:
let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

// In test_inline_single_use_cte_inlines_nested_cte_inside_later_produce:
let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

// In test_replace_cte_consume_only_rewrites_targeted_cte_id:
let rewritten = replace_cte_consume(plan, 1, &scan_plan()).expect("replace should succeed");
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test --lib sql::optimizer::cte_rewrite::tests::test_inline_single_use_cte_ -- --nocapture
```

Expected: compile failure because `inline_single_use_ctes` still returns `LogicalPlan`, and existing implementation still creates `SubqueryAlias`.

- [ ] **Step 3: Change CTE rewrite functions to return `Result`**

In `src/sql/optimizer/cte_rewrite.rs`, change the public signature:

```rust
pub(crate) fn inline_single_use_ctes(
    plan: LogicalPlan,
    ctx: &CTEContext,
) -> Result<LogicalPlan, String> {
```

Update recursive calls with `?`. For example:

```rust
LogicalPlan::Project(node) => Ok(LogicalPlan::Project(ProjectNode {
    input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
    items: node.items,
    required_output_columns: node.required_output_columns,
})),
```

For `CTEAnchor`, use:

```rust
let produce = inline_single_use_ctes(*node.produce, ctx)?;
let consumer = inline_single_use_ctes(*node.consumer, ctx)?;
let consume_count = ctx.consume_count.get(&node.cte_id).copied().unwrap_or(0);

if ctx.produces.contains(&node.cte_id) && consume_count <= 1 {
    let produce_input = match produce {
        LogicalPlan::CTEProduce(produce_node) if produce_node.cte_id == node.cte_id => {
            *produce_node.input
        }
        other => other,
    };
    replace_cte_consume(consumer, node.cte_id, &produce_input)
} else {
    Ok(LogicalPlan::CTEAnchor(CTEAnchorNode {
        cte_id: node.cte_id,
        produce: Box::new(produce),
        consumer: Box::new(consumer),
        required_output_columns: node.required_output_columns,
    }))
}
```

Change `replace_cte_consume` to return `Result<LogicalPlan, String>`:

```rust
fn replace_cte_consume(
    plan: LogicalPlan,
    cte_id: CteId,
    replacement: &LogicalPlan,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::CTEConsume(node) if node.cte_id == cte_id => {
            crate::sql::planner::adapt_plan_output(replacement.clone(), &node.output_columns)
        }
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => Ok(plan),
        LogicalPlan::TableFunction(node) => Ok(LogicalPlan::TableFunction(TableFunctionNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            function_name: node.function_name,
            args: node.args,
            output_columns: node.output_columns,
            alias: node.alias,
            is_left_join: node.is_left_join,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Filter(node) => Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            predicate: node.predicate,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Project(node) => Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            items: node.items,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Aggregate(node) => Ok(LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
            already_pushed: node.already_pushed,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Sort(node) => Ok(LogicalPlan::Sort(SortNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            items: node.items,
            analytic_partition_by: node.analytic_partition_by,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Limit(node) => Ok(LogicalPlan::Limit(LimitNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            limit: node.limit,
            offset: node.offset,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Window(node) => Ok(LogicalPlan::Window(WindowNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::SubqueryAlias(node) => {
            let input = replace_cte_consume(*node.input, cte_id, replacement)?;
            crate::sql::planner::adapt_plan_output(input, &node.output_columns)
        },
        LogicalPlan::Repeat(node) => Ok(LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_key_aliases: node.grouping_key_aliases,
            grouping_fn_args: node.grouping_fn_args,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Join(node) => Ok(LogicalPlan::Join(JoinNode {
            left: Box::new(replace_cte_consume(*node.left, cte_id, replacement)?),
            right: Box::new(replace_cte_consume(*node.right, cte_id, replacement)?),
            join_type: node.join_type,
            condition: node.condition,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Union(node) => Ok(LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            all: node.all,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Intersect(node) => Ok(LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Except(node) => Ok(LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEProduce(node) => Ok(LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEAnchor(node) => Ok(LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: node.cte_id,
            produce: Box::new(replace_cte_consume(*node.produce, cte_id, replacement)?),
            consumer: Box::new(replace_cte_consume(*node.consumer, cte_id, replacement)?),
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Decode(node) => Ok(LogicalPlan::Decode(DecodeNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            mappings: node.mappings,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}
```

When converting branches, do not recreate `LogicalPlan::SubqueryAlias`; there should be no such branch after Task 4.

- [ ] **Step 4: Propagate the new `Result` in optimizer entry**

In `src/sql/optimizer/mod.rs`, replace:

```rust
let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx);
```

with:

```rust
let rewritten = cte_rewrite::inline_single_use_ctes(rewritten, &cte_ctx)?;
```

- [ ] **Step 5: Run CTE rewrite tests**

Run:

```bash
cargo test --lib sql::optimizer::cte_rewrite::tests -- --nocapture
```

Expected: CTE rewrite tests pass and debug output contains no `SubqueryAlias` in single-use inline cases.

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/cte_rewrite.rs src/sql/optimizer/mod.rs
git commit -m "feat(optimizer): inline CTEs with output adapters"
```

## Task 4: Delete Plan-Layer Alias Types And Branches

**Files:**
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/cascades_rules/mod.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/logical_props.rs`
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/derive/mod.rs`
- Modify: `src/sql/optimizer/derive/passthrough.rs`
- Modify: `src/sql/explain.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/sql/column_id.rs`
- Modify: `src/sql/analyzer/scope.rs`

- [ ] **Step 1: Remove the logical plan variant**

In `src/sql/planner/plan.rs`, delete this enum variant:

```rust
SubqueryAlias(SubqueryAliasNode),
```

Delete the entire `SubqueryAliasNode` struct:

```rust
pub(crate) struct SubqueryAliasNode {
    pub input: Box<LogicalPlan>,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
    pub required_output_columns: Option<HashSet<ColumnId>>,
}
```

Update the enum comment so it no longer says subquery aliases are logical plan nodes.

- [ ] **Step 2: Remove optimizer operator structs and enum variants**

In `src/sql/optimizer/operator.rs`, delete:

```rust
pub(crate) struct LogicalSubqueryAliasOp {
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

pub(crate) struct PhysicalSubqueryAliasOp {
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}
```

Delete these `Operator` variants:

```rust
LogicalSubqueryAlias(LogicalSubqueryAliasOp),
PhysicalSubqueryAlias(PhysicalSubqueryAliasOp),
```

Remove `Operator::LogicalSubqueryAlias(_)` from `Operator::is_logical`.

- [ ] **Step 3: Remove memo conversion and implementation rule**

In `src/sql/optimizer/convert.rs`, remove `LogicalSubqueryAliasOp` from the import list and delete the `LogicalPlan::SubqueryAlias(node)` match arm.

In `src/sql/optimizer/cascades_rules/implement.rs`, delete the complete `SubqueryAliasToPhysical` rule block.

In `src/sql/optimizer/cascades_rules/mod.rs`, remove:

```rust
Box::new(implement::SubqueryAliasToPhysical),
```

- [ ] **Step 4: Remove stats, cost, property, and derive branches**

Remove every `LogicalSubqueryAlias` and `PhysicalSubqueryAlias` arm from:

```text
src/sql/optimizer/stats.rs
src/sql/optimizer/logical_props.rs
src/sql/optimizer/cost.rs
src/sql/optimizer/derive/mod.rs
src/sql/optimizer/derive/passthrough.rs
```

In `src/sql/optimizer/derive/passthrough.rs`, remove `PhysicalSubqueryAliasOp` from imports and passthrough macro invocations. Update module docs to list `Filter / Project / CTEProduce / Repeat` without `SubqueryAlias`.

- [ ] **Step 5: Remove explain and codegen branches**

In `src/sql/explain.rs`, delete the `LogicalPlan::SubqueryAlias(node)` branch from `format_node` and the `Operator::PhysicalSubqueryAlias(op)` branch from `format_physical_node`.

In `src/sql/codegen/fragment_builder.rs`:

1. Remove `PhysicalSubqueryAliasOp` from imports.
2. Remove the dispatcher branch:

```rust
Operator::PhysicalSubqueryAlias(op) => self.visit_subquery_alias(op, node),
```

3. Delete the entire `visit_subquery_alias` function.

- [ ] **Step 6: Remove engine traversal and stale comments**

In `src/engine/mod.rs`, remove the `LogicalPlan::SubqueryAlias(n)` branch from `collect_scan_stats`.

In `src/sql/column_id.rs`, replace the invariant comment with:

```rust
/// Invariant: `Project` and `Window` operators do **not** allocate new ids
/// for pass-through columns. Derived-table aliases are resolved in the analyzer
/// and represented through output metadata or ordinary Project adapters before
/// the optimizer sees the plan.
```

In `src/sql/analyzer/scope.rs`, replace the `add_column_with_id` comment with:

```rust
/// Register a single column with an already-allocated ColumnId.
/// Used when constructing derived-table and CTE-consume output scopes
/// from already analyzed query output.
```

- [ ] **Step 7: Compile-check expected remaining references**

Run:

```bash
rg -n "SubqueryAlias|SubqueryAliasNode|LogicalSubqueryAlias|PhysicalSubqueryAlias|visit_subquery_alias" src
cargo check
```

Expected: `rg` may still show rewrite-layer references before Task 5. `cargo check` fails on rewrite-layer references only.

- [ ] **Step 8: Commit only if cargo check reaches rewrite-layer failures**

If `cargo check` reports only rewrite-layer alias references, do not commit this partial state. Continue directly to Task 5. The commit happens after Task 5 when the code compiles.

## Task 5: Remove Alias From Rewrite Pipelines

**Files:**
- Modify: `src/sql/optimizer/rewrite/tree.rs`
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Delete: `src/sql/optimizer/rewrite/rules/column_pruning/prune_subquery_alias.rs`
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs`
- Modify: `src/sql/optimizer/rewrite/imv/marker.rs`
- Modify: `src/sql/optimizer/rewrite/imv/apply_key.rs`
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`
- Modify: `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rule.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/collector.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/rule.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`

- [ ] **Step 1: Delete column-pruning alias rule**

Run:

```bash
rm src/sql/optimizer/rewrite/rules/column_pruning/prune_subquery_alias.rs
```

In `src/sql/optimizer/rewrite/rules/column_pruning/mod.rs`, remove:

```rust
pub(crate) mod prune_subquery_alias;
Box::new(prune_subquery_alias::PruneSubqueryAliasColumns),
```

Update the doc comment from `18 per-operator Phase-2 column pruning rules` to `17 per-operator Phase-2 column pruning rules`.

- [ ] **Step 2: Remove registry names and expected counts**

In `src/sql/optimizer/rewrite/registry.rs`, remove `"PruneSubqueryAliasColumns"` from expected rule-name vectors.

In `src/sql/optimizer/rewrite/rules/mod.rs`, remove `"PruneSubqueryAliasColumns"` from expected names and update the count comment:

```rust
// 17 v2 pruning rules + 2 ukfk + 1 JoinReorder + 1 AggregatePushdown
// + 1 LowCardinalityDictionaryRewrite + 5 predicate pushdown rules + 1 DeriveJoinNotNullPredicate = 28
```

Change:

```rust
assert_eq!(rules.len(), 29);
```

to:

```rust
assert_eq!(rules.len(), 28);
```

- [ ] **Step 3: Remove alias traversal from generic rewrite tree**

In `src/sql/optimizer/rewrite/tree.rs`, remove `SubqueryAliasNode` from imports and delete the `LogicalPlan::SubqueryAlias(node)` reconstruction branch.

Remove `LogicalPlan::SubqueryAlias(_)` from leaf/pass-through match groups.

- [ ] **Step 4: Remove alias from required-column tagging**

In `src/sql/optimizer/rewrite/required_columns.rs`:

1. Remove the top-level match arm:

```rust
LogicalPlan::SubqueryAlias(_) => tag_subquery_alias(plan, parent_needed),
```

2. Delete the entire `tag_subquery_alias` function.
3. Remove alias traversal from `collect_cte_consumer_needs`, `walk_consume_position_map`, and `subtree_untagged`.
4. Remove `SubqueryAliasNode` from test imports.
5. Delete the `SubqueryAlias` unit test that constructs `SubqueryAliasNode`.

- [ ] **Step 5: Remove alias from specialized rewrite rules**

Apply these exact branch removals:

```text
src/sql/optimizer/rewrite/imv/marker.rs:
  remove plan_contains_imv_marker and collect_into branches for LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/imv/apply_key.rs:
  remove "SubqueryAlias" from plan kind display

src/sql/optimizer/rewrite/rules/utils.rs:
  remove collect_output_columns, collect_output_ids, and collect_qualified_output_columns branches for LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/rules/derive_join_not_null.rs:
  remove spine_not_null_inner branch for LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/rules/low_cardinality_dict/rule.rs:
  remove contains_scan branch for LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs:
  remove blocking mention in docs, remove match arms that preserve or read LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/rules/low_cardinality_dict/collector.rs:
  remove collect_blocklist and walk branches for LogicalPlan::SubqueryAlias

src/sql/optimizer/rewrite/rules/join_reorder/rule.rs:
  remove SubqueryAlias from comments that enumerate pass-through nodes

src/sql/optimizer/rewrite/rules/join_reorder/reorder.rs:
  remove LogicalPlan::SubqueryAlias reconstruction branches

src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs:
  remove estimate_statistics branch for LogicalPlan::SubqueryAlias
```

- [ ] **Step 6: Remove any remaining source references**

Run:

```bash
rg -n "SubqueryAlias|SubqueryAliasNode|LogicalSubqueryAlias|PhysicalSubqueryAlias|PruneSubqueryAliasColumns|visit_subquery_alias" src
```

Expected: only comments in the new spec or no matches under `src`. If matches remain in `src`, remove or rewrite them before continuing.

- [ ] **Step 7: Compile and run focused Rust tests**

Run:

```bash
cargo check
cargo test --lib sql::optimizer::rewrite::registry::tests -- --nocapture
cargo test --lib sql::optimizer::rewrite::rules::tests::registry_contains_expected_rules -- --nocapture
cargo test --lib sql::optimizer::cte_rewrite::tests -- --nocapture
cargo test --lib sql::planner::tests::derived_table_ -- --nocapture
```

Expected: all commands pass.

- [ ] **Step 8: Commit alias deletion**

```bash
git add src
git add -u src/sql/optimizer/rewrite/rules/column_pruning/prune_subquery_alias.rs
git commit -m "feat(optimizer): remove subquery alias plan operator"
```

## Task 6: Optimizer SQL Golden Coverage

**Files:**
- Create: `sql-tests/optimizer/sql/subquery_alias_fold.sql`
- Create: `sql-tests/optimizer/result/subquery_alias_fold.result`

- [ ] **Step 1: Add SQL case**

Create `sql-tests/optimizer/sql/subquery_alias_fold.sql` with:

```sql
-- OQ-6: Subquery aliases are analysis metadata, not logical or physical plan nodes.
DROP TABLE IF EXISTS ${case_db}.oq6_alias_base;
CREATE TABLE ${case_db}.oq6_alias_base (k INT, v INT);
INSERT INTO ${case_db}.oq6_alias_base VALUES (1, 10), (2, 20), (3, 30);

-- @result_not_contains=SUBQUERY ALIAS
-- @explain_contains=PROJECT [k]
-- @explain_contains=SCAN ${case_db}.oq6_alias_base
EXPLAIN VERBOSE
SELECT s.k
FROM (SELECT k, v FROM ${case_db}.oq6_alias_base) s
WHERE s.v > 10;

-- @result_not_contains=SUBQUERY ALIAS
-- @explain_contains=PROJECT [renamed_k]
SELECT renamed_k
FROM (SELECT k FROM ${case_db}.oq6_alias_base) s(renamed_k)
ORDER BY renamed_k;

-- @result_not_contains=SUBQUERY ALIAS
-- @explain_contains=HASH JOIN
WITH w AS (
    SELECT k, v FROM ${case_db}.oq6_alias_base WHERE k < 3
)
EXPLAIN VERBOSE
SELECT count(*)
FROM ${case_db}.oq6_alias_base b
JOIN w w_alias ON b.k = w_alias.k;
```

- [ ] **Step 2: Record the result file**

Start or reuse the standalone server per `AGENTS.md`, then run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only subquery_alias_fold --mode record
```

Expected: the runner creates `sql-tests/optimizer/result/subquery_alias_fold.result`.

- [ ] **Step 3: Verify the new SQL case**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --only subquery_alias_fold --mode verify
```

Expected: pass, with no `SUBQUERY ALIAS` in the result file.

- [ ] **Step 4: Commit SQL golden**

```bash
git add sql-tests/optimizer/sql/subquery_alias_fold.sql sql-tests/optimizer/result/subquery_alias_fold.result
git commit -m "test(optimizer): lock subquery alias fold plans"
```

## Task 7: Final Validation And Roadmap Update

**Files:**
- Modify: `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md`

- [ ] **Step 1: Run formatting and Rust validation**

Run:

```bash
cargo fmt --check
cargo test --lib
```

Expected: both pass.

- [ ] **Step 2: Run optimizer suite**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: optimizer suite passes.

- [ ] **Step 3: Run targeted CTE and join smoke**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite cte --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite join --mode verify -j 1
```

Expected: both suites pass. If `join` is too slow in debug, keep the failure output and rerun only the q22-shaped case used in `subquery_alias_fold.sql`; do not mark full join suite as passed unless the full command passes.

- [ ] **Step 4: Update roadmap progress**

In `/Users/harbor/Documents/Obsidian/NovaRocks TODO/NovaRocks Roadmap.md`, change the OQ progress bullets from:

```markdown
- ⏳ OQ-2 起：未开工。
```

to a current status that includes OQ-6, preserving existing completed OQ-4/OQ-5 notes if the file has advanced:

```markdown
- ✅ OQ-6：SubqueryAlias logical fold — planner/transformer parity 级别完成。Derived table / 单引用 CTE inline 不再生成计划层 alias operator；`LogicalPlan::SubqueryAlias`、optimizer logical/physical alias operator、`SubqueryAliasToPhysical` 与 codegen alias visitor 已移除。
```

In the OQ task table row for `OQ-6`, replace the status text with:

```markdown
SubqueryAlias logical fold — 已完成：alias 保留在 analyzer scope / output metadata，计划层 operator 已删除
```

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git status --short
git diff --stat
rg -n "SubqueryAlias|LogicalSubqueryAlias|PhysicalSubqueryAlias|PruneSubqueryAliasColumns|visit_subquery_alias" src sql-tests/optimizer
```

Expected: `rg` has no matches under `src`; matches in docs/specs are acceptable; new SQL case uses only `SUBQUERY ALIAS` in `@result_not_contains`.

- [ ] **Step 6: Commit roadmap if it is in a git repo**

Check:

```bash
git -C "/Users/harbor/Documents/Obsidian/NovaRocks TODO" status --short
```

If the Obsidian folder is a git repo and only the roadmap has changed, commit there:

```bash
git -C "/Users/harbor/Documents/Obsidian/NovaRocks TODO" add "NovaRocks Roadmap.md"
git -C "/Users/harbor/Documents/Obsidian/NovaRocks TODO" commit -m "docs: mark OQ-6 complete"
```

If it is not a git repo, leave the roadmap file modified and mention that in the final implementation summary.

- [ ] **Step 7: Commit final repo changes if any remain**

If the NovaRocks repo has only validation-driven doc or golden updates left:

```bash
git add .
git commit -m "docs: mark OQ-6 validation complete"
```

If `git status --short` is clean, skip this commit.
