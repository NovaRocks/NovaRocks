# Iceberg Target Join Aggregate IMV Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持 `storage_engine='iceberg'` 的 aggregate MV target state，并在同一套 Iceberg target state apply 机制上支持两表 inner equi-join aggregate IMV。

**Architecture:** 先扩展 MV shape，使 aggregate surface 能区分 single-base aggregate 和 join aggregate；再把 managed-lake 已有 aggregate state layout/merge 数学复用到 Iceberg target，但 target apply 改成 Iceberg row-delta。Join aggregate incremental refresh 使用已有 join telescoping branch 基础，分支输出 signed aggregate state，最后按 group `__row_id__` 合并并替换 Iceberg target group rows。

**Tech Stack:** Rust, sqlparser AST, Arrow `RecordBatch`/`Chunk`, NovaRocks standalone SQL analyzer/planner, Iceberg v3 row-lineage, existing `__nr_ivm_delta` source, Iceberg commit collector, `sql-tests/iceberg-ivm`.

---

## File Structure

- Modify `src/connector/starrocks/managed/mv_shape.rs`
  - Add `JoinAggregateMvShape`.
  - Classify aggregate+join before single-base aggregate.
  - Keep managed-lake single aggregate classification unchanged.
- Modify `src/connector/starrocks/managed/ivm_delta_aggregate.rs`
  - Add qualified signed-state rewrite for join aggregate branch SQL.
  - Keep existing unqualified single-base rewrite stable.
- Modify `src/meta/repository/mv_contract.rs`
  - Add `ApplyKeySource::GroupRowId`.
  - Add aggregate state layout contract fields with serde defaults.
- Modify `src/engine/mv/schema_contract.rs`
  - Validate aggregate state layout and `GroupRowId` target contract during refresh.
- Modify `src/engine/mv/iceberg_target_apply.rs`
  - Add group-row apply-key constants and string-key target locator.
- Modify `src/engine/mv/iceberg_merge_sink.rs`
  - Route delete chunks with either Int64 base-row apply keys or UTF8 group-row apply keys.
- Modify `src/connector/starrocks/managed/mv_agg_state.rs`
  - Reuse state materialization for Iceberg target physical chunks; expose only helper functions needed by the Iceberg path.
- Create `src/engine/mv/iceberg_aggregate_state.rs`
  - Own Iceberg aggregate target state scan, old+delta merge orchestration, change-op chunk construction, and group-row locator input glue.
- Modify `src/engine/mv/mod.rs`
  - Register `iceberg_aggregate_state`.
- Modify `src/engine/mv/iceberg_refresh.rs`
  - Open CREATE and REFRESH dispatch for `Aggregate` and `JoinAggregate`.
  - Build physical Iceberg target schema from aggregate layout.
  - Run first refresh state materialization.
  - Run single aggregate incremental refresh.
  - Run join aggregate incremental refresh with telescoping branches.
- Add SQL tests:
  - `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql`
  - `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_target.result`
  - `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql`
  - `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate.result`

## Task 1: Shape Model And Classifier

**Files:**
- Modify: `src/connector/starrocks/managed/mv_shape.rs`

- [ ] **Step 1: Add failing tests for join aggregate classification**

Append these tests to the existing `mod tests` in `src/connector/starrocks/managed/mv_shape.rs`:

```rust
fn as_join_aggregate_shape(shape: IncrementalMvShape) -> JoinAggregateMvShape {
    match shape {
        IncrementalMvShape::JoinAggregate(shape) => shape,
        other => panic!("expected join aggregate shape, got {other:?}"),
    }
}

#[test]
fn join_aggregate_accepts_two_table_inner_equi_join() {
    let shape = as_join_aggregate_shape(
        classify_sql(
            "select d.region, count(*) as c, sum(f.amount) as s \
             from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
             group by d.region",
        )
        .expect("classify join aggregate"),
    );

    assert_eq!(shape.join.left_alias, "f");
    assert_eq!(shape.join.right_alias, "d");
    assert_eq!(shape.join.join_keys.len(), 1);
    assert_eq!(shape.group_keys.len(), 1);
    assert_eq!(shape.aggregates.len(), 2);
    assert_eq!(shape.visible_outputs.len(), 3);
}

#[test]
fn join_aggregate_does_not_fall_into_join_projection_shape() {
    let shape = classify_sql(
        "select d.region, count(*) as c \
         from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
         group by d.region",
    )
    .expect("classify join aggregate");

    assert!(matches!(shape, IncrementalMvShape::JoinAggregate(_)));
}

#[test]
fn join_aggregate_rejects_outer_join() {
    let err = classify_sql(
        "select d.region, count(*) as c \
         from ice.ns.fact f left join ice.ns.dim d on f.dim_id = d.id \
         group by d.region",
    )
    .expect_err("outer join rejected");
    assert!(err.contains("two-table inner equi-join"), "err={err}");
}

#[test]
fn join_aggregate_rejects_missing_projected_group_key() {
    let err = classify_sql(
        "select count(*) as c \
         from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
         group by d.region",
    )
    .expect_err("missing projected group key rejected");
    assert!(err.contains("projection must include every GROUP BY key"), "err={err}");
}

#[test]
fn join_aggregate_rejects_three_table_join() {
    let err = classify_sql(
        "select d.region, count(*) as c \
         from ice.ns.fact f \
         join ice.ns.dim d on f.dim_id = d.id \
         join ice.ns.extra e on e.id = d.id \
         group by d.region",
    )
    .expect_err("three-table join rejected");
    assert!(err.contains("exactly two"), "err={err}");
}
```

- [ ] **Step 2: Run the focused classifier tests and confirm failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests::join_aggregate -- --nocapture
```

Expected: fails to compile because `JoinAggregateMvShape` and `IncrementalMvShape::JoinAggregate` are not defined.

- [ ] **Step 3: Add the shape type and enum variant**

Add the variant and struct near the existing shape structs:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
    JoinAggregate(JoinAggregateMvShape),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinAggregateMvShape {
    pub(crate) join: JoinProjectionFilterMvShape,
    pub(crate) group_keys: Vec<GroupKeyShape>,
    pub(crate) aggregates: Vec<AggregateCallShape>,
    pub(crate) visible_outputs: Vec<VisibleAggregateOutput>,
}

impl JoinAggregateMvShape {
    pub(crate) fn as_aggregate_shape_for_layout(&self) -> AggregateMvShape {
        AggregateMvShape {
            base_table: self.join.left_table.clone(),
            group_keys: self.group_keys.clone(),
            aggregates: self.aggregates.clone(),
            visible_outputs: self.visible_outputs.clone(),
        }
    }
}
```

Update the `base_table()` and `base_tables()` matches:

```rust
impl IncrementalMvShape {
    pub(crate) fn base_table(&self) -> &sqlparser::ast::ObjectName {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => &shape.base_table,
            IncrementalMvShape::Aggregate(shape) => &shape.base_table,
            IncrementalMvShape::JoinProjectionFilter(_) | IncrementalMvShape::JoinAggregate(_) => {
                panic!("base_table() is only valid for single-base MV shapes")
            }
        }
    }

    pub(crate) fn base_tables(&self) -> Vec<&sqlparser::ast::ObjectName> {
        match self {
            IncrementalMvShape::ProjectionFilter(shape) => vec![&shape.base_table],
            IncrementalMvShape::Aggregate(shape) => vec![&shape.base_table],
            IncrementalMvShape::JoinProjectionFilter(shape) => {
                vec![&shape.left_table, &shape.right_table]
            }
            IncrementalMvShape::JoinAggregate(shape) => {
                vec![&shape.join.left_table, &shape.join.right_table]
            }
        }
    }
}
```

- [ ] **Step 4: Implement `classify_join_aggregate_mv_query`**

Add this helper beside `classify_aggregate_mv_query`:

```rust
fn classify_join_aggregate_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<JoinAggregateMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| join_aggregate_error())?;

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(join_aggregate_error());
    };
    reject_unsupported_aggregate_select_clauses(select)?;

    let join = classify_join_projection_filter_mv_query_for_select(select)?;
    if let Some(selection) = &select.selection {
        reject_unsupported_expr(selection).map_err(aggregate_expr_error)?;
    }

    let group_by_exprs = aggregate_group_by_exprs(&select.group_by)?;
    for expr in group_by_exprs {
        reject_unsupported_expr(expr).map_err(aggregate_expr_error)?;
    }

    let mut group_keys = group_by_exprs
        .iter()
        .cloned()
        .map(|expr| GroupKeyShape {
            output_name: String::new(),
            expr,
        })
        .collect::<Vec<_>>();
    let mut aggregates = Vec::new();
    let mut visible_outputs = Vec::with_capacity(select.projection.len());
    let mut projected_group_keys = vec![false; group_keys.len()];

    for item in &select.projection {
        let (expr, output_name) = projection_expr_and_output_name(item)?;
        if let Some(group_key_index) = group_keys.iter().position(|group_key| group_key.expr == *expr)
        {
            if group_keys[group_key_index].output_name.is_empty() {
                group_keys[group_key_index].output_name = output_name;
            }
            projected_group_keys[group_key_index] = true;
            visible_outputs.push(VisibleAggregateOutput::GroupKey(group_key_index));
            continue;
        }

        let aggregate = classify_aggregate_call(expr, output_name)?;
        let aggregate_index = aggregates.len();
        aggregates.push(aggregate);
        visible_outputs.push(VisibleAggregateOutput::Aggregate(aggregate_index));
    }

    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "incremental aggregate MV projection must include every GROUP BY key".to_string(),
        );
    }
    if aggregates.is_empty() {
        return Err("incremental aggregate MV requires at least one aggregate output".to_string());
    }

    Ok(JoinAggregateMvShape {
        join,
        group_keys,
        aggregates,
        visible_outputs,
    })
}

fn join_aggregate_error() -> String {
    "incremental join aggregate MV query must be a two-table inner equi-join SELECT with non-empty GROUP BY and only count/sum/avg/min/max aggregate outputs".to_string()
}
```

Split the existing join classifier so it can reuse a parsed `Select`:

```rust
fn classify_join_projection_filter_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<JoinProjectionFilterMvShape, String> {
    reject_unsupported_query_clauses(query).map_err(|_| join_projection_filter_error())?;
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err(join_projection_filter_error());
    };
    reject_unsupported_select_clauses(select).map_err(|_| join_projection_filter_error())?;
    reject_match_against_before_from_shape_check(select)
        .map_err(|_| join_projection_filter_error())?;
    reject_unsupported_projection_filter_exprs(select)
        .map_err(|_| join_projection_filter_error())?;
    classify_join_projection_filter_mv_query_for_select(select)
}

fn classify_join_projection_filter_mv_query_for_select(
    select: &sqlparser::ast::Select,
) -> Result<JoinProjectionFilterMvShape, String> {
    let [from] = select.from.as_slice() else {
        return Err(join_projection_filter_error());
    };
    let [join] = from.joins.as_slice() else {
        return Err("incremental join MV requires exactly two Iceberg base tables".to_string());
    };
    if !matches!(
        join.join_operator,
        sqlparser::ast::JoinOperator::Join(_) | sqlparser::ast::JoinOperator::Inner(_)
    ) {
        return Err("incremental join MV supports only two-table inner equi-join".to_string());
    }
    let (left_table, left_alias) = table_factor_name_and_alias(&from.relation)?;
    let (right_table, right_alias) = table_factor_name_and_alias(&join.relation)?;
    if left_alias.eq_ignore_ascii_case(&right_alias) {
        return Err("incremental join MV requires distinct join aliases".to_string());
    }
    let condition = match &join.join_operator {
        sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr))
        | sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr)) => expr,
        _ => return Err("incremental join MV requires JOIN ... ON equi predicates".to_string()),
    };
    let mut join_keys = Vec::new();
    collect_equi_join_keys(condition, &left_alias, &right_alias, &mut join_keys)?;
    if join_keys.is_empty() {
        return Err("incremental join MV requires at least one equi-join predicate".to_string());
    }
    Ok(JoinProjectionFilterMvShape {
        left_table,
        left_alias,
        right_table,
        right_alias,
        join_keys,
    })
}
```

- [ ] **Step 5: Update classifier dispatch order**

Replace `classify_incremental_mv_query` with:

```rust
pub(crate) fn classify_incremental_mv_query(
    query: &sqlparser::ast::Query,
) -> Result<IncrementalMvShape, String> {
    if is_probably_aggregate_query(query) {
        if is_probably_join_query(query) {
            return classify_join_aggregate_mv_query(query).map(IncrementalMvShape::JoinAggregate);
        }
        return classify_aggregate_mv_query(query).map(IncrementalMvShape::Aggregate);
    }

    match classify_join_projection_filter_mv_query(query) {
        Ok(shape) => return Ok(IncrementalMvShape::JoinProjectionFilter(shape)),
        Err(err) if is_probably_join_query(query) => return Err(err),
        Err(_) => {}
    }

    classify_projection_filter_mv_query(query).map(IncrementalMvShape::ProjectionFilter)
}
```

- [ ] **Step 6: Run classifier tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape::tests::join_aggregate -- --nocapture
cargo test --lib connector::starrocks::managed::mv_shape -- --nocapture
```

Expected: all `mv_shape` tests pass, with existing single aggregate tests still returning `IncrementalMvShape::Aggregate`.

- [ ] **Step 7: Commit**

```bash
git add src/connector/starrocks/managed/mv_shape.rs
git commit -m "feat: classify iceberg join aggregate mv shape"
```

## Task 2: Signed Aggregate Rewrite For Join Branches

**Files:**
- Modify: `src/connector/starrocks/managed/ivm_delta_aggregate.rs`

- [ ] **Step 1: Add failing tests for qualified change-op rewrite**

Append tests in `mod tests`:

```rust
#[test]
fn join_signed_delta_rewrite_qualifies_change_op_to_delta_alias() {
    let sql = "select d.region, count(*) as c, sum(f.amount) as s \
               from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
               group by d.region";
    let shape = match crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(
        &parse_query(sql),
    )
    .expect("classify")
    {
        crate::connector::starrocks::managed::mv_shape::IncrementalMvShape::JoinAggregate(shape) => {
            shape
        }
        other => panic!("expected join aggregate, got {other:?}"),
    };

    let rewritten = rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
        sql,
        &shape.as_aggregate_shape_for_layout(),
        Some("f"),
    )
    .expect("rewrite");
    let upper = rewritten.to_uppercase();

    assert!(upper.contains("SUM(F.__CHANGE_OP) AS C"), "got: {rewritten}");
    assert!(
        upper.contains("SUM(F.AMOUNT * F.__CHANGE_OP)")
            || upper.contains("SUM((F.AMOUNT * F.__CHANGE_OP))"),
        "got: {rewritten}"
    );
}

#[test]
fn single_signed_delta_rewrite_keeps_unqualified_change_op() {
    let sql = "select k1, count(*) as c from ice.ns.orders group by k1";
    let shape = parse_aggregate_shape(sql);
    let rewritten = rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
        sql,
        &shape,
        None,
    )
    .expect("rewrite");
    let upper = rewritten.to_uppercase();

    assert!(upper.contains("SUM(__CHANGE_OP) AS C"), "got: {rewritten}");
}
```

If the test module does not expose `parse_query`, add:

```rust
fn parse_query(sql: &str) -> sqlparser::ast::Query {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
    let sqlparser::ast::Statement::Query(query) = stmt else {
        panic!("expected query");
    };
    *query
}
```

- [ ] **Step 2: Run rewrite tests and confirm failure**

Run:

```bash
cargo test --lib connector::starrocks::managed::ivm_delta_aggregate::tests::join_signed_delta -- --nocapture
```

Expected: fails because `rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier` does not exist.

- [ ] **Step 3: Add qualified rewrite entrypoint**

Keep the existing function as a wrapper and add a qualified version:

```rust
pub(crate) fn rewrite_select_sql_for_signed_delta_state(
    select_sql: &str,
    shape: &AggregateMvShape,
) -> Result<String, String> {
    rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(select_sql, shape, None)
}

pub(crate) fn rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
    select_sql: &str,
    shape: &AggregateMvShape,
    change_op_qualifier: Option<&str>,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("rewrite_select_sql_for_signed_delta_state parse error: {e}"))?;

    let Statement::Query(query) = &mut stmt else {
        return Err(
            "rewrite_select_sql_for_signed_delta_state: expected Query statement".to_string(),
        );
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err("rewrite_select_sql_for_signed_delta_state: expected SELECT body".to_string());
    };

    if shape.aggregates.iter().any(|agg| {
        matches!(
            agg.function,
            AggregateFunctionKind::Min | AggregateFunctionKind::Max
        )
    }) {
        return Err(
            "MIN/MAX aggregate outputs are not reversible: delete-bearing signed delta state cannot be consumed incrementally"
                .to_string(),
        );
    }

    let change_op = ChangeOpExpr {
        qualifier: change_op_qualifier.map(str::to_string),
    };
    select.projection = signed_delta_projection(shape, &change_op)?;

    Ok(stmt.to_string())
}
```

Add helpers:

```rust
struct ChangeOpExpr {
    qualifier: Option<String>,
}

impl ChangeOpExpr {
    fn expr(&self) -> Expr {
        match &self.qualifier {
            Some(qualifier) => Expr::CompoundIdentifier(vec![
                Ident::new(qualifier),
                Ident::new(CHANGE_OP_COLUMN),
            ]),
            None => Expr::Identifier(Ident::new(CHANGE_OP_COLUMN)),
        }
    }
}

fn signed_delta_projection(
    shape: &AggregateMvShape,
    change_op: &ChangeOpExpr,
) -> Result<Vec<SelectItem>, String> {
    let mut projection = Vec::with_capacity(shape.visible_outputs.len() + shape.aggregates.len());
    for output in &shape.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let group_key = shape.group_keys.get(*group_key_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: group key index {group_key_index} out of range"
                    )
                })?;
                projection.push(SelectItem::ExprWithAlias {
                    expr: group_key.expr.clone(),
                    alias: Ident::new(group_key.output_name.clone()),
                });
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let aggregate = shape.aggregates.get(*aggregate_index).ok_or_else(|| {
                    format!(
                        "rewrite_select_sql_for_signed_delta_state: aggregate index {aggregate_index} out of range"
                    )
                })?;
                push_signed_aggregate_state_projection(&mut projection, aggregate, change_op)?;
            }
        }
    }
    if aggregate_shape_needs_retraction_count_state(shape) {
        projection.push(make_aggregate_select_item(
            "SUM",
            change_op.expr(),
            AGG_RETRACTION_COUNT_STATE_COLUMN,
        ));
    }
    Ok(projection)
}

fn push_signed_aggregate_state_projection(
    projection: &mut Vec<SelectItem>,
    aggregate: &crate::connector::starrocks::managed::mv_shape::AggregateCallShape,
    change_op: &ChangeOpExpr,
) -> Result<(), String> {
    match aggregate.function {
        AggregateFunctionKind::Count => match &aggregate.input {
            AggregateInput::Star => projection.push(make_aggregate_select_item(
                "SUM",
                change_op.expr(),
                &aggregate.output_name,
            )),
            AggregateInput::Expr(expr) => projection.push(make_aggregate_select_item(
                "SUM",
                count_expr_signed_delta_arg(expr.as_ref().clone(), change_op),
                &aggregate.output_name,
            )),
        },
        AggregateFunctionKind::Sum => {
            let AggregateInput::Expr(expr) = &aggregate.input else {
                return Err(
                    "rewrite_select_sql_for_signed_delta_state: SUM requires an expression input"
                        .to_string(),
                );
            };
            projection.push(make_aggregate_select_item(
                "SUM",
                signed_value_expr(expr.as_ref().clone(), change_op),
                &aggregate.output_name,
            ));
        }
        AggregateFunctionKind::Avg => {
            let AggregateInput::Expr(expr) = &aggregate.input else {
                return Err(
                    "rewrite_select_sql_for_signed_delta_state: AVG requires an expression input"
                        .to_string(),
                );
            };
            let sanitized = sanitize_state_column_name(&aggregate.output_name);
            projection.push(make_aggregate_select_item(
                "SUM",
                signed_value_expr(expr.as_ref().clone(), change_op),
                &format!("__agg_state_{sanitized}__sum"),
            ));
            projection.push(make_aggregate_select_item(
                "SUM",
                count_expr_signed_delta_arg(expr.as_ref().clone(), change_op),
                &format!("__agg_state_{sanitized}__count"),
            ));
        }
        AggregateFunctionKind::Min | AggregateFunctionKind::Max => unreachable!(
            "MIN/MAX aggregate functions are rejected before projection rewrite"
        ),
    }
    Ok(())
}

fn signed_value_expr(expr: Expr, change_op: &ChangeOpExpr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(expr),
        op: BinaryOperator::Multiply,
        right: Box::new(change_op.expr()),
    }
}

fn count_expr_signed_delta_arg(expr: Expr, change_op: &ChangeOpExpr) -> Expr {
    Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: vec![CaseWhen {
            condition: Expr::IsNotNull(Box::new(expr)),
            result: change_op.expr(),
        }],
        else_result: Some(Box::new(Expr::Value(
            Value::Number("0".to_string(), false).into(),
        ))),
    }
}
```

Remove or update the old zero-argument `change_op_expr`, `signed_value_expr`, and `count_expr_signed_delta_arg` helpers so there is only one rewrite path.

- [ ] **Step 4: Run rewrite tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::ivm_delta_aggregate -- --nocapture
```

Expected: all aggregate delta rewrite tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/connector/starrocks/managed/ivm_delta_aggregate.rs
git commit -m "feat: qualify join aggregate delta change op"
```

## Task 3: Aggregate Contract And Group Apply Key

**Files:**
- Modify: `src/meta/repository/mv_contract.rs`
- Modify: `src/engine/mv/schema_contract.rs`
- Modify: `src/engine/mv/iceberg_target_apply.rs`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`

- [ ] **Step 1: Add failing contract tests**

Add tests in `src/meta/repository/mv_contract.rs`:

```rust
#[test]
fn aggregate_contract_accepts_group_row_id_with_join_contract() {
    let mut contract = sample_contract();
    contract.contract_version = 3;
    contract.bases = vec![contract.base.clone(), contract.base.clone()];
    contract.join = Some(JoinContract {
        kind: JoinContractKind::InnerEquiJoin,
        predicates: vec![JoinPredicateLineage {
            left: QualifiedFieldLineage {
                table_fqn: contract.base.table_fqn.clone(),
                qualifier_at_create: "f".to_string(),
                field_id: 1,
            },
            right: QualifiedFieldLineage {
                table_fqn: contract.base.table_fqn.clone(),
                qualifier_at_create: "d".to_string(),
                field_id: 1,
            },
        }],
    });
    contract.aggregate = Some(AggregateStateContract {
        state_layout_version: 1,
        row_id_column_name: "__row_id__".to_string(),
        state_columns: vec![AggregateStateColumnContract {
            column_name: "__agg_state_c".to_string(),
            target_field_id: 3,
            type_signature: "long".to_string(),
            nullable: false,
            role: AggregateStateRoleContract::Single,
        }],
    });
    contract.target.hidden_apply_key = HiddenApplyKeyContract {
        column_name: "__row_id__".to_string(),
        target_field_id: 1,
        source: ApplyKeySource::GroupRowId,
    };

    contract.ensure_self_consistent().expect("self check");
}

#[test]
fn group_row_id_apply_key_requires_aggregate_contract() {
    let mut contract = sample_contract();
    contract.contract_version = 3;
    contract.target.hidden_apply_key = HiddenApplyKeyContract {
        column_name: "__row_id__".to_string(),
        target_field_id: 1,
        source: ApplyKeySource::GroupRowId,
    };

    let err = contract.ensure_self_consistent().expect_err("rejected");
    assert!(err.to_string().contains("GroupRowId"), "err={err}");
}
```

- [ ] **Step 2: Run contract tests and confirm failure**

Run:

```bash
cargo test --lib meta::repository::mv_contract::tests::aggregate_contract -- --nocapture
```

Expected: fails to compile because the aggregate contract types and `GroupRowId` variant do not exist.

- [ ] **Step 3: Extend persisted contract types**

Add these fields and types:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvSchemaContract {
    pub contract_version: u16,
    pub base: BaseContract,
    #[serde(default)]
    pub bases: Vec<BaseContract>,
    pub output: OutputContract,
    #[serde(default)]
    pub join: Option<JoinContract>,
    #[serde(default)]
    pub aggregate: Option<AggregateStateContract>,
    pub target: TargetContract,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateStateContract {
    pub state_layout_version: u16,
    pub row_id_column_name: String,
    pub state_columns: Vec<AggregateStateColumnContract>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateStateColumnContract {
    pub column_name: String,
    pub target_field_id: i32,
    pub type_signature: String,
    pub nullable: bool,
    pub role: AggregateStateRoleContract,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AggregateStateRoleContract {
    Single,
    AvgSum,
    AvgCount,
    RetractionCount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
}

pub const GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME: &str = "__row_id__";
```

Update every in-repo `MvSchemaContract { ... }` literal by adding `aggregate: None` unless that literal is testing aggregate state. Existing serde JSON fixtures do not need a JSON field because `#[serde(default)]` supplies `None`.

Add a self-check error:

```rust
GroupRowIdRequiresAggregateContract,
```

Add display text:

```rust
Self::GroupRowIdRequiresAggregateContract => {
    write!(
        f,
        "MV contract GroupRowId apply-key source requires an aggregate state contract"
    )
}
```

Update `ensure_self_consistent` apply-key checks:

```rust
let expected_hidden_apply_key_column = match self.target.hidden_apply_key.source {
    ApplyKeySource::BaseRowId => HIDDEN_APPLY_KEY_COLUMN_NAME,
    ApplyKeySource::JoinRowKey => JOIN_APPLY_KEY_COLUMN_NAME,
    ApplyKeySource::GroupRowId => GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
};

match self.target.hidden_apply_key.source {
    ApplyKeySource::JoinRowKey => match &self.join {
        Some(join) if join.predicates.is_empty() => {
            return Err(ContractSelfCheckError::EmptyJoinPredicates);
        }
        Some(_) => {}
        None => return Err(ContractSelfCheckError::JoinRowKeyRequiresJoinContract),
    },
    ApplyKeySource::BaseRowId => {
        if self.join.is_some() {
            return Err(ContractSelfCheckError::BaseRowIdRejectsJoinContract);
        }
    }
    ApplyKeySource::GroupRowId => {
        if self.aggregate.is_none() {
            return Err(ContractSelfCheckError::GroupRowIdRequiresAggregateContract);
        }
    }
}
```

- [ ] **Step 4: Add apply-key constants and string locator signatures**

In `src/engine/mv/iceberg_target_apply.rs`, add:

```rust
pub(crate) const ICEBERG_MV_GROUP_APPLY_KEY_COLUMN: &str = "__row_id__";
pub(crate) const ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID: &str = "GroupRowId";
```

Add a string-key locator entrypoint reusing the same locator input policy as the existing Int64 locator:

```rust
pub(crate) async fn locate_target_rows_by_string_apply_key(
    target_table: &iceberg::table::Table,
    apply_key_column: &str,
    requested_keys: &[String],
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    locate_target_rows_by_apply_key_impl(
        target_table,
        apply_key_column,
        ApplyKeyRequest::Utf8(requested_keys),
        existing_deletes_by_file,
        referenced_data_file_partitions,
    )
    .await
}
```

Refactor the existing `locate_target_rows_by_apply_key` into an implementation that accepts either `Int64` or `Utf8` requested keys:

```rust
enum ApplyKeyRequest<'a> {
    Int64(&'a [i64]),
    Utf8(&'a [String]),
}
```

The implementation must preserve existing Int64 behavior and add exact UTF8 matching for `__row_id__`. It must return an error when the target scan contains duplicate requested keys.

- [ ] **Step 5: Extend merge sink delete routing for string apply keys**

In `src/engine/mv/iceberg_merge_sink.rs`, add:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyKeyValueType {
    Int64,
    Utf8,
}

pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub locator_state: Option<TargetLocatorState>,
    pub apply_key_column: String,
    pub apply_key_value_type: ApplyKeyValueType,
}
```

Update existing call sites to pass `ApplyKeyValueType::Int64`. Aggregate target refresh will pass `ApplyKeyValueType::Utf8`.

Replace delete-key extraction in `handle_delete_batch` with:

```rust
match self.plan.apply_key_value_type {
    ApplyKeyValueType::Int64 => {
        let apply_keys =
            extract_i64_apply_key_values_from_record_batch(&batch, &self.plan.apply_key_column)?;
        let groups_result = data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_apply_key(
                &self.plan.target_table,
                &apply_keys,
                &locator_state.existing_deletes_by_file,
                &locator_state.referenced_data_file_partitions,
            ),
        )?;
        let groups = groups_result?;
        for group in groups {
            self.plan.collector.inject_delete_group(group);
        }
    }
    ApplyKeyValueType::Utf8 => {
        let apply_keys =
            extract_utf8_apply_key_values_from_record_batch(&batch, &self.plan.apply_key_column)?;
        let groups_result = data_block_on(
            crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                &self.plan.target_table,
                &self.plan.apply_key_column,
                &apply_keys,
                &locator_state.existing_deletes_by_file,
                &locator_state.referenced_data_file_partitions,
            ),
        )?;
        let groups = groups_result?;
        for group in groups {
            self.plan.collector.inject_delete_group(group);
        }
    }
}
```

Rename the existing extractor to `extract_i64_apply_key_values_from_record_batch` and add:

```rust
fn extract_utf8_apply_key_values_from_record_batch(
    batch: &RecordBatch,
    apply_key_column: &str,
) -> Result<Vec<String>, String> {
    let idx = batch.schema().index_of(apply_key_column).map_err(|_| {
        format!("merge sink: DELETE batch missing apply-key column {apply_key_column}")
    })?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| format!("merge sink: apply-key column {apply_key_column} must be Utf8"))?;
    arr.iter()
        .map(|v| {
            v.map(str::to_string).ok_or_else(|| {
                format!("merge sink: null value in apply-key column {apply_key_column}")
            })
        })
        .collect()
}
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib meta::repository::mv_contract -- --nocapture
cargo test --lib engine::mv::iceberg_target_apply -- --nocapture
cargo test --lib engine::mv::iceberg_merge_sink -- --nocapture
```

Expected: contract tests pass, existing apply-key tests pass, and merge sink tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/meta/repository/mv_contract.rs src/engine/mv/schema_contract.rs src/engine/mv/iceberg_target_apply.rs src/engine/mv/iceberg_merge_sink.rs
git commit -m "feat: add iceberg aggregate mv group apply key contract"
```

## Task 4: Iceberg Aggregate Target State Module

**Files:**
- Create: `src/engine/mv/iceberg_aggregate_state.rs`
- Modify: `src/engine/mv/mod.rs`

- [ ] **Step 1: Add failing unit tests for merge-to-change-op chunks**

Create `src/engine/mv/iceberg_aggregate_state.rs` with the module tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn chunk(batch: RecordBatch) -> crate::exec::chunk::Chunk {
        crate::engine::record_batch_to_chunk(batch).expect("chunk")
    }

    #[test]
    fn merge_result_marks_replaced_and_removed_groups() {
        let layout = test_count_sum_layout();
        let old = vec![chunk(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["r1", "r2"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2, 1])) as ArrayRef,
            ],
        )
        .expect("old batch"))];
        let delta = vec![chunk(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__row_id__", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, true),
                Field::new("c", DataType::Int64, false),
                Field::new("__agg_state_c", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2", "r3"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["r1", "r2", "r3"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, -1, 5])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, -1, 5])) as ArrayRef,
            ],
        )
        .expect("delta batch"))];

        let result = merge_aggregate_target_state(&layout, &old, &delta).expect("merge");

        assert_eq!(result.delete_row_ids, vec!["r1".to_string(), "r2".to_string()]);
        assert_eq!(result.insert_chunks.iter().map(|c| c.batch.num_rows()).sum::<usize>(), 2);
        assert_eq!(result.new_total_rows, 2);
    }
}
```

Define the test layout helper in the same test module:

```rust
fn test_count_sum_layout() -> crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout {
    use crate::connector::starrocks::managed::ddl::managed_physical_column;
    use crate::connector::starrocks::managed::mv_agg_state::{
        AggregateMvLayout, AggregateStateColumn, AggregateStateRole, AggregateVisibleColumn,
    };
    use crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind;
    use crate::sql::parser::ast::SqlType;

    AggregateMvLayout {
        row_id_column: managed_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        ),
        visible_columns: vec![
            AggregateVisibleColumn {
                name: "region".to_string(),
                data_type: DataType::Utf8,
                sql_type: SqlType::String,
                nullable: true,
                source_index: 0,
            },
            AggregateVisibleColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                sql_type: SqlType::BigInt,
                nullable: false,
                source_index: 1,
            },
        ],
        state_columns: vec![AggregateStateColumn {
            name: "__agg_state_c".to_string(),
            data_type: DataType::Int64,
            sql_type: SqlType::BigInt,
            nullable: false,
            visible_source_index: 1,
            aggregate_index: 0,
            function: AggregateFunctionKind::Count,
            state_role: AggregateStateRole::Single,
            count_star: true,
        }],
        group_key_source_indexes: vec![0],
        physical_columns: vec![
            managed_physical_column("__row_id__".to_string(), SqlType::String, false, false, true),
            managed_physical_column("region".to_string(), SqlType::String, true, true, false),
            managed_physical_column("c".to_string(), SqlType::BigInt, false, true, false),
            managed_physical_column("__agg_state_c".to_string(), SqlType::BigInt, false, false, false),
        ],
    }
}
```

- [ ] **Step 2: Register module and run tests to confirm failure**

Add to `src/engine/mv/mod.rs`:

```rust
pub(crate) mod iceberg_aggregate_state;
```

Run:

```bash
cargo test --lib engine::mv::iceberg_aggregate_state -- --nocapture
```

Expected: fails because `merge_aggregate_target_state` is missing.

- [ ] **Step 3: Implement merge result and merge function**

Add implementation:

```rust
pub(crate) struct IcebergAggregateMergeResult {
    pub(crate) delete_row_ids: Vec<String>,
    pub(crate) insert_chunks: Vec<crate::exec::chunk::Chunk>,
    pub(crate) new_total_rows: i64,
}

pub(crate) fn merge_aggregate_target_state(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    old_chunks: &[crate::exec::chunk::Chunk],
    delta_chunks: &[crate::exec::chunk::Chunk],
) -> Result<IcebergAggregateMergeResult, String> {
    let old_rows =
        crate::connector::starrocks::managed::mv_agg_state::build_old_state_map(old_chunks, layout)?;
    let old_row_ids = old_rows.keys().cloned().collect::<std::collections::BTreeSet<_>>();
    let touched_row_ids = delta_row_ids(delta_chunks, layout)?;
    let merge_result =
        crate::connector::starrocks::managed::mv_agg_state::merge_aggregate_state_batches_with_retractions(
            &old_rows,
            delta_chunks,
            layout,
        )?;
    let merged_row_map =
        crate::connector::starrocks::managed::mv_agg_state::load_aggregate_physical_rows(
            &merge_result.upsert_chunks,
            layout,
        )?;
    let insert_chunks = filter_physical_chunks_by_row_ids(
        &merge_result.upsert_chunks,
        layout,
        &touched_row_ids,
    )?;
    let delete_row_ids = touched_row_ids
        .iter()
        .filter(|row_id| old_row_ids.contains(*row_id))
        .cloned()
        .collect::<Vec<_>>();
    let new_total_rows = i64::try_from(merged_row_map.len())
        .map_err(|_| "aggregate MV target row count exceeds i64".to_string())?;

    Ok(IcebergAggregateMergeResult {
        delete_row_ids,
        insert_chunks,
        new_total_rows,
    })
}
```

Add `delta_row_ids` by scanning `layout.row_id_column.column.name` in each delta batch and collecting UTF8 values into a `BTreeSet<String>`. Add `filter_physical_chunks_by_row_ids` by taking rows whose `__row_id__` appears in the set and rebuilding batches with the original physical schema. This prevents unchanged target groups from being reinserted.

- [ ] **Step 4: Add change-op chunk builder**

Add:

```rust
pub(crate) fn build_aggregate_change_chunks(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    merge: IcebergAggregateMergeResult,
) -> Result<Vec<crate::exec::chunk::Chunk>, String> {
    let mut chunks = Vec::new();
    if !merge.delete_row_ids.is_empty() {
        chunks.push(delete_row_ids_chunk(layout, &merge.delete_row_ids)?);
    }
    for chunk in merge.insert_chunks {
        chunks.push(append_insert_change_op(chunk)?);
    }
    Ok(chunks)
}
```

The delete chunk schema must include `__row_id__` plus `__change_op`; the insert chunk schema must include the full physical aggregate schema plus `__change_op`. Delete chunks are only consumed by the group-row locator, while insert chunks are stripped by the merge sink before writing data files.

- [ ] **Step 5: Add current target state scan**

Add:

```rust
pub(crate) fn load_current_aggregate_target_state(
    target_table: &iceberg::table::Table,
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<crate::exec::chunk::Chunk>, String> {
    let columns = layout
        .physical_columns
        .iter()
        .map(|column| column.column.name.clone())
        .collect::<Vec<_>>();
    let batches = crate::runtime::global_async_runtime::data_block_on(async {
        use futures::StreamExt;
        use iceberg::arrow::ArrowReaderBuilder;

        let scan = target_table
            .scan()
            .select(columns)
            .build()
            .map_err(|e| format!("build aggregate MV target state scan failed: {e}"))?;
        let task_stream = scan
            .plan_files()
            .await
            .map_err(|e| format!("plan aggregate MV target state files failed: {e}"))?;
        let cleaned_tasks = task_stream.map(|task_result| {
            task_result.map(|mut task| {
                task.predicate = None;
                task
            })
        });
        let arrow_reader = ArrowReaderBuilder::new(target_table.file_io().clone())
            .with_row_group_filtering_enabled(false)
            .with_row_selection_enabled(false)
            .build();
        let mut stream = arrow_reader
            .read(Box::pin(cleaned_tasks))
            .map_err(|e| format!("read aggregate MV target state scan failed: {e}"))?;
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| format!("aggregate MV target state scan error: {e}"))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
        Ok::<_, String>(batches)
    })?;
    batches
        .into_iter()
        .map(crate::engine::record_batch_to_chunk)
        .collect()
}
```

This function returns physical aggregate target chunks containing `__row_id__`, visible columns, and state columns in `layout.physical_columns` order.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_aggregate_state -- --nocapture
```

Expected: aggregate state module tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/mod.rs src/engine/mv/iceberg_aggregate_state.rs
git commit -m "feat: add iceberg aggregate target state merge"
```

## Task 5: CREATE Iceberg Aggregate Target Tables

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/schema_contract.rs`
- Modify: `src/meta/repository/mv_contract.rs`

- [ ] **Step 1: Add failing CREATE tests**

In `src/engine/mv/iceberg_refresh.rs` test module, add a unit test that builds an aggregate shape and verifies the physical columns:

```rust
#[test]
fn iceberg_aggregate_target_columns_use_state_layout() {
    let query = parse_query(
        "select region, count(*) as c, sum(amount) as s \
         from ice.ns.fact group by region",
    );
    let shape = match classify_incremental_mv_query(&query).expect("shape") {
        IncrementalMvShape::Aggregate(shape) => shape,
        other => panic!("expected aggregate shape, got {other:?}"),
    };
    let output_columns = vec![
        output_column("region", arrow::datatypes::DataType::Utf8, true),
        output_column("c", arrow::datatypes::DataType::Int64, false),
        output_column("s", arrow::datatypes::DataType::Int64, true),
    ];

    let columns = iceberg_aggregate_target_columns(&shape, &output_columns).expect("columns");
    let names = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();

    assert_eq!(names, vec!["__row_id__", "region", "c", "s", "__agg_state_c", "__agg_state_s"]);
}
```

Add the helper if it is not present:

```rust
fn output_column(
    name: &str,
    data_type: arrow::datatypes::DataType,
    nullable: bool,
) -> crate::sql::analysis::OutputColumn {
    crate::sql::analysis::OutputColumn {
        name: name.to_string(),
        data_type,
        nullable,
    }
}
```

- [ ] **Step 2: Run the CREATE helper test and confirm failure**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::iceberg_aggregate_target_columns_use_state_layout -- --nocapture
```

Expected: fails because `iceberg_aggregate_target_columns` is missing.

- [ ] **Step 3: Add aggregate shape helpers in `iceberg_refresh.rs`**

Add:

```rust
fn aggregate_shape_for_layout(
    shape: &IncrementalMvShape,
) -> Option<crate::connector::starrocks::managed::mv_shape::AggregateMvShape> {
    match shape {
        IncrementalMvShape::Aggregate(shape) => Some(shape.clone()),
        IncrementalMvShape::JoinAggregate(shape) => Some(shape.as_aggregate_shape_for_layout()),
        _ => None,
    }
}

fn is_aggregate_mv_shape(shape: &IncrementalMvShape) -> bool {
    matches!(
        shape,
        IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_)
    )
}

fn iceberg_aggregate_target_columns(
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
    let layout =
        crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            shape,
            output_columns,
        )?;
    Ok(crate::connector::starrocks::managed::ddl::table_columns_from_physical_columns(
        &layout.physical_columns,
    ))
}
```

- [ ] **Step 4: Open CREATE gate for aggregate shapes**

In `create_iceberg_mv`, update the `loaded_bases` match:

```rust
IncrementalMvShape::Aggregate(_) => {
    let [base_ref] = base_refs.as_slice() else {
        return Err(
            "iceberg-backed aggregate materialized views require exactly one iceberg base table"
                .to_string(),
        );
    };
    let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
    ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
    vec![(base_ref.clone(), loaded_base)]
}
IncrementalMvShape::JoinAggregate(join_shape) => {
    if base_refs.len() != 2 {
        return Err(
            "iceberg-backed join aggregate materialized views require exactly two iceberg base tables"
                .to_string(),
        );
    }
    validate_join_shape_base_refs(&join_shape.join, &base_refs)?;
    base_refs
        .iter()
        .map(|base_ref| {
            let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
            ensure_base_row_lineage_contract(&loaded_base.table, &base_ref.fqn())?;
            Ok((base_ref.clone(), loaded_base))
        })
        .collect::<Result<Vec<_>, String>>()?
}
```

Reject `PRIMARY KEY` for both aggregate shapes:

```rust
IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
    return Err(
        "iceberg-backed aggregate materialized views do not support PRIMARY KEY".to_string(),
    );
}
```

Reject `MIN/MAX` for Iceberg target aggregate shapes:

```rust
fn reject_min_max_for_iceberg_target_aggregate(
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
) -> Result<(), String> {
    if shape.aggregates.iter().any(|agg| {
        matches!(
            agg.function,
            crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind::Min
                | crate::connector::starrocks::managed::mv_shape::AggregateFunctionKind::Max
        )
    }) {
        return Err(
            "iceberg-backed aggregate materialized views do not support MIN/MAX in incremental mode"
                .to_string(),
        );
    }
    Ok(())
}
```

Call this helper after classification when `aggregate_shape_for_layout(&shape)` returns `Some`.

- [ ] **Step 5: Build physical target schema and properties**

Replace the apply-key selection with:

```rust
let apply_key_column_name = match &shape {
    IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_COLUMN,
    IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_JOIN_APPLY_KEY_COLUMN,
    IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_GROUP_APPLY_KEY_COLUMN
    }
};
let apply_key_source_property = match &shape {
    IncrementalMvShape::ProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_BASE_ROW_ID,
    IncrementalMvShape::JoinProjectionFilter(_) => ICEBERG_MV_APPLY_KEY_SOURCE_JOIN_ROW_KEY,
    IncrementalMvShape::Aggregate(_) | IncrementalMvShape::JoinAggregate(_) => {
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_SOURCE_GROUP_ROW_ID
    }
};
```

Build columns:

```rust
let columns = match aggregate_shape_for_layout(&shape) {
    Some(aggregate_shape) => iceberg_aggregate_target_columns(&aggregate_shape, &analysis.output_columns)?,
    None => {
        let mut columns = analysis
            .output_columns
            .iter()
            .map(output_column_to_table_column)
            .collect::<Result<Vec<_>, _>>()?;
        columns.push(match &shape {
            IncrementalMvShape::ProjectionFilter(_) => apply_key_table_column(),
            IncrementalMvShape::JoinProjectionFilter(_) => join_apply_key_table_column(),
            _ => unreachable!("aggregate handled above"),
        });
        columns
    }
};
```

- [ ] **Step 6: Build aggregate schema contract**

Extend `build_iceberg_mv_schema_contract`:

```rust
// Existing ProjectionFilter and JoinProjectionFilter arms must set `aggregate: None`.

IncrementalMvShape::Aggregate(aggregate_shape) => {
    let [(base_ref, loaded_base)] = loaded_bases else {
        return Err("aggregate iceberg MV schema contract requires one loaded base".to_string());
    };
    let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
        &analysis.resolved_query,
        loaded_base.table.metadata().current_schema(),
    )?;
    let layout =
        crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            aggregate_shape,
            &analysis.output_columns,
        )?;
    crate::meta::repository::mv_contract::MvSchemaContract {
        contract_version: 3,
        base: base_contract(base_ref, loaded_base, None, lineage.base_fields.clone()),
        bases: vec![],
        output: crate::meta::repository::mv_contract::OutputContract {
            columns: lineage.output_columns,
            filter: lineage.filter,
        },
        join: None,
        aggregate: Some(aggregate_contract(&layout, target_loaded)?),
        target: target_contract(
            analysis,
            target,
            target_loaded,
            actual_apply_key_field_id,
            crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId,
        ),
    }
}
```

Add the `JoinAggregate` arm:

```rust
IncrementalMvShape::JoinAggregate(join_aggregate_shape) => {
    let join_shape = &join_aggregate_shape.join;
    let (left_ref, left_loaded) =
        loaded_base_for_shape_table(loaded_bases, &join_shape.left_table)?;
    let (right_ref, right_loaded) =
        loaded_base_for_shape_table(loaded_bases, &join_shape.right_table)?;
    let left_schema = left_loaded.table.metadata().current_schema();
    let right_schema = right_loaded.table.metadata().current_schema();
    let left_fqn = left_ref.fqn();
    let right_fqn = right_ref.fqn();
    let join_lineage =
        crate::sql::analyzer::mv_lineage::build_join_projection_filter_lineage(
            &analysis.resolved_query,
            &[
                (&left_fqn, &join_shape.left_alias, left_schema.as_ref()),
                (&right_fqn, &join_shape.right_alias, right_schema.as_ref()),
            ],
        )?;
    let left_fields = join_lineage
        .base_fields_by_table
        .get(&left_fqn)
        .cloned()
        .unwrap_or_default();
    let right_fields = join_lineage
        .base_fields_by_table
        .get(&right_fqn)
        .cloned()
        .unwrap_or_default();
    let left_contract = base_contract(
        left_ref,
        left_loaded,
        Some(join_shape.left_alias.clone()),
        left_fields,
    );
    let right_contract = base_contract(
        right_ref,
        right_loaded,
        Some(join_shape.right_alias.clone()),
        right_fields,
    );
    let aggregate_shape = join_aggregate_shape.as_aggregate_shape_for_layout();
    let layout =
        crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            &aggregate_shape,
            &analysis.output_columns,
        )?;
    crate::meta::repository::mv_contract::MvSchemaContract {
        contract_version: 3,
        base: left_contract.clone(),
        bases: vec![left_contract, right_contract],
        output: crate::meta::repository::mv_contract::OutputContract {
            columns: join_lineage.output_columns,
            filter: join_lineage.filter,
        },
        join: Some(join_lineage.join),
        aggregate: Some(aggregate_contract(&layout, target_loaded)?),
        target: target_contract(
            analysis,
            target,
            target_loaded,
            actual_apply_key_field_id,
            crate::meta::repository::mv_contract::GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            crate::meta::repository::mv_contract::ApplyKeySource::GroupRowId,
        ),
    }
}
```

Add helper:

```rust
fn aggregate_contract(
    layout: &crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
    target_loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> Result<crate::meta::repository::mv_contract::AggregateStateContract, String> {
    let fields = target_loaded.table.metadata().current_schema().as_struct().fields();
    let field_id = |name: &str| -> Result<i32, String> {
        fields
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(name))
            .map(|field| field.id)
            .ok_or_else(|| format!("aggregate MV target schema missing state column {name}"))
    };
    Ok(crate::meta::repository::mv_contract::AggregateStateContract {
        state_layout_version: 1,
        row_id_column_name: layout.row_id_column.column.name.clone(),
        state_columns: layout
            .state_columns
            .iter()
            .map(|column| {
                Ok(crate::meta::repository::mv_contract::AggregateStateColumnContract {
                    column_name: column.name.clone(),
                    target_field_id: field_id(&column.name)?,
                    type_signature: iceberg_field_type_signature(fields, &column.name)?,
                    nullable: column.nullable,
                    role: aggregate_state_role_contract(column.state_role),
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    })
}
```

Add helpers:

```rust
fn iceberg_field_type_signature(
    fields: &[std::sync::Arc<iceberg::spec::NestedField>],
    name: &str,
) -> Result<String, String> {
    fields
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(name))
        .map(|field| format!("{}", field.field_type))
        .ok_or_else(|| format!("aggregate MV target schema missing state column {name}"))
}

fn aggregate_state_role_contract(
    role: crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole,
) -> crate::meta::repository::mv_contract::AggregateStateRoleContract {
    match role {
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::Single => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::Single
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::AvgSum => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::AvgSum
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::AvgCount => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::AvgCount
        }
        crate::connector::starrocks::managed::mv_agg_state::AggregateStateRole::RetractionCount => {
            crate::meta::repository::mv_contract::AggregateStateRoleContract::RetractionCount
        }
    }
}
```

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::iceberg_aggregate_target_columns_use_state_layout -- --nocapture
cargo test --lib meta::repository::mv_contract -- --nocapture
```

Expected: tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/schema_contract.rs src/meta/repository/mv_contract.rs
git commit -m "feat: create iceberg aggregate mv target state schema"
```

## Task 6: First Refresh For Aggregate Targets

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`

- [ ] **Step 1: Add failing first-refresh rewrite test**

Add test:

```rust
#[test]
fn aggregate_first_refresh_uses_state_shaped_select() {
    let sql = "select region, avg(amount) as a \
               from ice.ns.fact group by region";
    let query = parse_query(sql);
    let shape = match classify_incremental_mv_query(&query).expect("shape") {
        IncrementalMvShape::Aggregate(shape) => shape,
        other => panic!("expected aggregate shape, got {other:?}"),
    };

    let state_sql = iceberg_aggregate_first_refresh_select_sql(sql, &shape).expect("rewrite");
    let upper = state_sql.to_uppercase();

    assert!(upper.contains("SUM(AMOUNT) AS __AGG_STATE_A__SUM"), "sql={state_sql}");
    assert!(upper.contains("COUNT(AMOUNT) AS __AGG_STATE_A__COUNT"), "sql={state_sql}");
    assert!(!upper.contains("__ROW_ID__"), "sql={state_sql}");
}
```

- [ ] **Step 2: Run first-refresh test and confirm failure**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_first_refresh_uses_state_shaped_select -- --nocapture
```

Expected: fails because `iceberg_aggregate_first_refresh_select_sql` is missing.

- [ ] **Step 3: Add first-refresh SELECT rewrite helper**

Add:

```rust
fn iceberg_aggregate_first_refresh_select_sql(
    select_sql: &str,
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
) -> Result<String, String> {
    crate::connector::starrocks::managed::mv_shape::rewrite_select_sql_for_state(
        select_sql,
        shape,
    )
}
```

`__row_id__` is not part of the executable SELECT. It is added by `materialize_aggregate_result_chunks`, which uses the same group-key row-id derivation as managed-lake aggregate refresh.

- [ ] **Step 4: Dispatch first refresh for aggregate shapes**

In `refresh_iceberg_mv`, after mode calculation and before projection-only first refresh dispatch, add:

```rust
if let Some(aggregate_shape) = aggregate_shape_for_layout(&shape) {
    return refresh_iceberg_aggregate_mv(
        state,
        &target,
        &target_entry,
        &iceberg_catalog,
        &target_loaded.table,
        expected_main_snapshot_id_from_table(&target_loaded.table),
        current_database,
        &mv_definition,
        &base_refs,
        &shape,
        &aggregate_shape,
    );
}
```

Add the aggregate refresh dispatcher:

```rust
#[allow(clippy::too_many_arguments)]
fn refresh_iceberg_aggregate_mv(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    target_table: &iceberg::table::Table,
    expected_main_snapshot_id: Option<i64>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[IcebergTableRef],
    shape: &IncrementalMvShape,
    aggregate_shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
) -> Result<StatementResult, String> {
    match shape {
        IncrementalMvShape::Aggregate(_) => {
            refresh_iceberg_single_aggregate_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                target_table,
                expected_main_snapshot_id,
                current_database,
                mv_definition,
                base_refs,
                aggregate_shape,
            )
        }
        IncrementalMvShape::JoinAggregate(join_shape) => {
            refresh_iceberg_join_aggregate_mv(
                state,
                target,
                target_entry,
                iceberg_catalog,
                target_table,
                expected_main_snapshot_id,
                current_database,
                mv_definition,
                base_refs,
                join_shape,
                aggregate_shape,
            )
        }
        _ => unreachable!("aggregate dispatcher called with non-aggregate shape"),
    }
}
```

- [ ] **Step 5: Implement first refresh writer path**

Add `first_refresh_iceberg_aggregate_mv` patterned after `first_refresh_iceberg_mv`, with these differences:

```rust
let state_sql = iceberg_aggregate_first_refresh_select_sql(pinned_full_select_sql, aggregate_shape)?;
let result = run_mv_full_select_result(state, current_database, &state_sql)?;
let layout =
    crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
        aggregate_shape,
        &analyze_mv_select(state, None, current_database, &parse_mv_select_query(&mv_definition.select_sql)?)?.output_columns,
    )?;
let chunks =
    crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
        result,
        &layout,
        aggregate_shape,
    )?;
```

Add `run_mv_full_select_result` near `first_refresh_iceberg_mv`:

```rust
fn run_mv_full_select_result(
    state: &Arc<StandaloneState>,
    database: &str,
    select_sql: &str,
) -> Result<crate::engine::QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)?;
    let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = stmt else {
        return Err("MV full refresh SQL must be a SELECT query".to_string());
    };
    let catalog = state
        .catalog
        .read()
        .map_err(|e| format!("standalone catalog read lock: {e}"))?
        .clone();
    crate::engine::execute_query(
        query.as_ref(),
        &catalog,
        database,
        state.exchange_port,
        None,
    )
}
```

Then write `chunks` with `write_chunks_as_iceberg_data_files`, commit staging, publish, and finalize using the same metadata flow as `first_refresh_iceberg_mv`.

- [ ] **Step 6: Run focused tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_first_refresh -- --nocapture
```

Expected: first-refresh helper tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: refresh iceberg aggregate mv initial state"
```

## Task 7: Single-Base Aggregate Incremental Refresh

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`

- [ ] **Step 1: Add failing incremental helper tests**

Add a test in `src/engine/mv/iceberg_refresh.rs`:

```rust
#[test]
fn aggregate_incremental_rewrite_uses_signed_state() {
    let sql = "select region, count(*) as c, sum(amount) as s \
               from ice.ns.fact group by region";
    let query = parse_query(sql);
    let shape = match classify_incremental_mv_query(&query).expect("shape") {
        IncrementalMvShape::Aggregate(shape) => shape,
        other => panic!("expected aggregate shape, got {other:?}"),
    };

    let rewritten = iceberg_aggregate_incremental_delta_select_sql(sql, &shape, None)
        .expect("rewrite");
    let upper = rewritten.to_uppercase();

    assert!(upper.contains("SUM(__CHANGE_OP) AS C"), "sql={rewritten}");
    assert!(upper.contains("SUM(AMOUNT * __CHANGE_OP) AS S"), "sql={rewritten}");
}
```

- [ ] **Step 2: Run and confirm failure**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_incremental_rewrite_uses_signed_state -- --nocapture
```

Expected: fails because `iceberg_aggregate_incremental_delta_select_sql` is missing.

- [ ] **Step 3: Implement signed delta SQL helper**

Add:

```rust
fn iceberg_aggregate_incremental_delta_select_sql(
    select_sql: &str,
    shape: &crate::connector::starrocks::managed::mv_shape::AggregateMvShape,
    change_op_qualifier: Option<&str>,
) -> Result<String, String> {
    crate::connector::starrocks::managed::ivm_delta_aggregate::rewrite_select_sql_for_signed_delta_state_with_change_op_qualifier(
        select_sql,
        shape,
        change_op_qualifier,
    )
}
```

- [ ] **Step 4: Implement single aggregate refresh flow**

In `refresh_iceberg_single_aggregate_mv`, use the existing projection/filter incremental planning pieces:

```rust
let batch = plan_changes(base_table, previous_snapshot_id, Some(current_snapshot_id), &[])?;
let source_files = crate::connector::starrocks::managed::ivm_delta_source::build_delta_source_files(
    crate::connector::starrocks::managed::ivm_delta_source::IvmDeltaSourceInput {
        state,
        current_database,
        base_ref,
        loaded: &loaded_base,
    },
    batch,
)?;
let signed_sql =
    iceberg_aggregate_incremental_delta_select_sql(pinned_full_select_sql, aggregate_shape, None)?;
let delta_result =
    crate::connector::starrocks::managed::ivm_delta_source::execute_delta_source_query(
        crate::connector::starrocks::managed::ivm_delta_source::IvmDeltaSourceInput {
            state,
            current_database,
            base_ref,
            loaded: &loaded_base,
        },
        &signed_sql,
        source_files,
    )?;
let delta_chunks =
    crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
        delta_result,
        &layout,
        aggregate_shape,
    )?;
```

Then:

```rust
let old_chunks =
    crate::engine::mv::iceberg_aggregate_state::load_current_aggregate_target_state(
        &target_loaded.table,
        &layout,
    )?;
let merge =
    crate::engine::mv::iceberg_aggregate_state::merge_aggregate_target_state(
        &layout,
        &old_chunks,
        &delta_chunks,
    )?;
let change_chunks =
    crate::engine::mv::iceberg_aggregate_state::build_aggregate_change_chunks(&layout, merge)?;
```

Write `change_chunks` through `IcebergMergeSinkFactory` configured with `apply_key_column = "__row_id__"` and `apply_key_value_type = ApplyKeyValueType::Utf8`. Commit with row-delta when deletes exist and fast-append when only inserts exist.

- [ ] **Step 5: Handle empty deltas**

If the change batch has no insert/delete/equality-delete/deleted-data-file changes, advance lineage exactly like existing projection/filter refresh:

```rust
finalize_iceberg_mv_refresh(
    state,
    refresh_id,
    target_row_count_from_definition_or_snapshot(target, mv_definition)?,
    snapshots,
    table_uuids,
    target_snapshot_id,
)?;
```

Do not commit an empty Iceberg target snapshot.

- [ ] **Step 6: Run unit tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::aggregate_incremental -- --nocapture
cargo test --lib engine::mv::iceberg_aggregate_state -- --nocapture
```

Expected: aggregate incremental helper tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/iceberg_aggregate_state.rs
git commit -m "feat: refresh iceberg aggregate mv incrementally"
```

## Task 8: Join Aggregate Incremental Refresh

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs`
- Modify: `src/engine/mv/iceberg_join_branch.rs`
- Modify: `src/engine/mv/iceberg_aggregate_state.rs`

- [ ] **Step 1: Add failing join branch rewrite tests**

In `src/engine/mv/iceberg_refresh.rs`, add:

```rust
#[test]
fn join_aggregate_branch_rewrite_uses_delta_side_change_op() {
    let sql = "select d.region, count(*) as c, sum(f.amount) as s \
               from ice.ns.fact f join ice.ns.dim d on f.dim_id = d.id \
               group by d.region";
    let query = parse_query(sql);
    let shape = match classify_incremental_mv_query(&query).expect("shape") {
        IncrementalMvShape::JoinAggregate(shape) => shape,
        other => panic!("expected join aggregate shape, got {other:?}"),
    };
    let branch_sql = iceberg_join_aggregate_branch_delta_sql(
        sql,
        &shape,
        crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Left,
    )
    .expect("branch rewrite");
    let upper = branch_sql.to_uppercase();

    assert!(upper.contains("SUM(F.__CHANGE_OP) AS C"), "sql={branch_sql}");
    assert!(!upper.contains("SUM(__CHANGE_OP) AS C"), "sql={branch_sql}");
}
```

- [ ] **Step 2: Run and confirm failure**

Run:

```bash
cargo test --lib engine::mv::iceberg_refresh::tests::join_aggregate_branch_rewrite_uses_delta_side_change_op -- --nocapture
```

Expected: fails because the join aggregate branch helper is missing.

- [ ] **Step 3: Add explicit branch delta side helper**

In `src/engine/mv/iceberg_join_branch.rs`, add:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchDeltaSide {
    Left,
    Right,
}

impl JoinDeltaBranchPlan {
    pub(crate) fn delta_side(&self) -> Result<BranchDeltaSide, String> {
        match (self.left, self.right) {
            (BranchSide::Delta(_), BranchSide::Snapshot(_)) => Ok(BranchDeltaSide::Left),
            (BranchSide::Snapshot(_), BranchSide::Delta(_)) => Ok(BranchDeltaSide::Right),
            _ => Err("join branch plan must contain exactly one delta side".to_string()),
        }
    }
}
```

- [ ] **Step 4: Implement join aggregate branch SQL helper**

In `iceberg_refresh.rs`, add:

```rust
fn iceberg_join_aggregate_branch_delta_sql(
    select_sql: &str,
    shape: &crate::connector::starrocks::managed::mv_shape::JoinAggregateMvShape,
    delta_side: crate::engine::mv::iceberg_join_branch::BranchDeltaSide,
) -> Result<String, String> {
    let delta_alias = match delta_side {
        crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Left => {
            shape.join.left_alias.as_str()
        }
        crate::engine::mv::iceberg_join_branch::BranchDeltaSide::Right => {
            shape.join.right_alias.as_str()
        }
    };
    iceberg_aggregate_incremental_delta_select_sql(
        select_sql,
        &shape.as_aggregate_shape_for_layout(),
        Some(delta_alias),
    )
}
```

- [ ] **Step 5: Implement join aggregate refresh flow**

In `refresh_iceberg_join_aggregate_mv`:

1. Load previous and current snapshots for both base refs.
2. Call `plan_changes` for each side.
3. Determine `left_has_changes` and `right_has_changes` from insert/delete/equality-delete/deleted-data-file collections.
4. Use existing `plan_join_delta_branches`.
5. For each branch:

```rust
let branch_query = crate::engine::mv::iceberg_join_branch::rewrite_join_branch_query(
    &canonical_select_query,
    &branch_plan,
    &join_shape.join.left_alias,
    &join_shape.join.right_alias,
)?;
let branch_sql = branch_query.to_string();
let delta_side = branch_plan.delta_side()?;
let signed_sql = iceberg_join_aggregate_branch_delta_sql(&branch_sql, join_shape, delta_side)?;
let branch_result = run_mv_full_select_result(state, current_database, &signed_sql)?;
let branch_chunks =
    crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
        branch_result,
        &layout,
        aggregate_shape,
    )?;
all_delta_chunks.extend(branch_chunks);
```

6. Merge `all_delta_chunks` into current target state with `merge_aggregate_target_state`.
7. Apply row-delta to the Iceberg target using `__row_id__` string locator.
8. Finalize `last_refresh_snapshots` for both base refs.

- [ ] **Step 6: Preserve telescoping semantics**

Use the existing branch windows:

```rust
let branch_plans = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
    left_ref,
    right_ref,
    SnapshotWindow {
        from: left_from,
        to: left_to,
    },
    SnapshotWindow {
        from: right_from,
        to: right_to,
    },
    left_has_changes,
    right_has_changes,
);
```

This encodes:

```text
DeltaL(L0->L1) join R0
L1 join DeltaR(R0->R1)
```

Do not add a third branch for `DeltaL join DeltaR`; it would double-count when both sides changed.

- [ ] **Step 7: Run join aggregate unit tests**

Run:

```bash
cargo test --lib engine::mv::iceberg_join_branch -- --nocapture
cargo test --lib engine::mv::iceberg_refresh::tests::join_aggregate -- --nocapture
```

Expected: join branch tests and join aggregate helper tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/iceberg_join_branch.rs src/engine/mv/iceberg_aggregate_state.rs
git commit -m "feat: refresh iceberg join aggregate mv incrementally"
```

## Task 9: SQL Regression Tests

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_target.result`
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate.result`

- [ ] **Step 1: Add aggregate target SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,aggregate,target_state
-- Test Point: Iceberg-backed aggregate MV stores aggregate state and refreshes incrementally.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg' aggregate MV, refresh after insert/delete/update, and verify hidden state isolation.
-- Scope: Iceberg target MV, single-base aggregate, COUNT/SUM/AVG, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_agg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, COUNT(amount) AS c_amount, SUM(amount) AS s, AVG(amount) AS a
FROM ice_ivm_agg_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', NULL);
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c, c_amount, s, a
FROM agg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_${uuid0}.ns_${uuid0}.orders VALUES ('east', 30), ('north', 5);
DELETE FROM ice_ivm_agg_${uuid0}.ns_${uuid0}.orders WHERE region = 'west';
UPDATE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders SET amount = 40 WHERE region = 'east' AND amount = 10;
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 5
SELECT region, c, c_amount, s, a
FROM agg_mv_${uuid0}
ORDER BY region;

-- query 6
-- @expect_error=Column '__row_id__' cannot be resolved
SELECT __row_id__ FROM agg_mv_${uuid0};

-- query 7
-- @expect_error=Column '__agg_state_c' cannot be resolved
SELECT __agg_state_c FROM agg_mv_${uuid0};

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_${uuid0};
```

- [ ] **Step 2: Add join aggregate SQL case**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql`:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,target_state
-- Test Point: Iceberg-backed join aggregate IMV supports two-sided base retract changes.
-- Method: Create fact/dim v3 row-lineage tables, refresh join aggregate MV, mutate both bases, and compare MV with base query.
-- Scope: Iceberg target MV, two-table inner equi-join aggregate, telescoping delta, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_agg_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  dim_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim VALUES
  (10, 'east'),
  (20, 'west');
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES
  (1, 10, 100),
  (2, 10, 200),
  (3, 20, 50);
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 3
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact VALUES (4, 20, 70);
UPDATE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim SET region = 'north' WHERE id = 10;
DELETE FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact WHERE id = 3;
REFRESH MATERIALIZED VIEW join_agg_mv_${uuid0};

-- query 5
SELECT region, c, s
FROM join_agg_mv_${uuid0}
ORDER BY region;

-- query 6
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
GROUP BY d.region
ORDER BY d.region;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_agg_mv_${uuid0};
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_join_agg_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_join_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_agg_${uuid0};
```

- [ ] **Step 3: Run record mode for the two cases**

Start or source the generated environment:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode record \
  --only iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate
```

Expected: creates the two `.result` files with deterministic query results.

- [ ] **Step 4: Run verify mode**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify \
  --only iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate
```

Expected: both cases pass.

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_target.result \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate.result
git commit -m "test: cover iceberg aggregate mv target refresh"
```

## Task 10: Final Verification And Cleanup

**Files:**
- Review all files changed by previous tasks.

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt -- --check
```

Expected: exits 0. If it fails, run `cargo fmt`, inspect the diff, and commit formatting with the feature change that introduced the formatting issue.

- [ ] **Step 2: Run focused Rust tests**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_shape -- --nocapture
cargo test --lib connector::starrocks::managed::ivm_delta_aggregate -- --nocapture
cargo test --lib engine::mv::iceberg_target_apply -- --nocapture
cargo test --lib engine::mv::iceberg_aggregate_state -- --nocapture
cargo test --lib engine::mv::iceberg_refresh -- --nocapture
cargo test --lib meta::repository::mv_contract -- --nocapture
```

Expected: every command exits 0.

- [ ] **Step 3: Run focused SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify \
  --only iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate
```

Expected: both SQL cases pass.

- [ ] **Step 4: Run existing managed-lake aggregate guard test**

Run:

```bash
cargo test --lib connector::starrocks::managed::mv_agg_state -- --nocapture
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --mode verify \
  --only managed_lake_mv_aggregate_ivm
```

Expected: existing managed-lake aggregate behavior remains green.

- [ ] **Step 5: Inspect diff and commit final cleanup**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0 and `git status --short` contains only intentional files. Commit any final cleanup:

```bash
git add src/connector/starrocks/managed/mv_shape.rs \
        src/connector/starrocks/managed/ivm_delta_aggregate.rs \
        src/meta/repository/mv_contract.rs \
        src/engine/mv/schema_contract.rs \
        src/engine/mv/iceberg_target_apply.rs \
        src/engine/mv/iceberg_aggregate_state.rs \
        src/engine/mv/mod.rs \
        src/engine/mv/iceberg_refresh.rs \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_target.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_target.result \
        sql-tests/iceberg-ivm/sql/iceberg_ivm_join_aggregate.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_join_aggregate.result
git commit -m "feat: support iceberg target join aggregate imv"
```

If each implementation task already committed its files, this step should leave no staged changes.
