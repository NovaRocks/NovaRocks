//! `ScalarApplyToJoin` — ports StarRocks `ScalarApply2JoinRule`.
//!
//! Matches a scalar `Apply` and lowers it to joins:
//!
//! - **Uncorrelated** → `CROSS JOIN(left, inner)` (inner wrapped in
//!   `AssertOneRow` unless it is provably ≤1 row), then a `Project`.
//!
//! - **Correlated, `need_check_max_rows=false`** (PushDownApplyAggFilter already
//!   ran; inner is a vector aggregate): `LEFT OUTER JOIN(left, right) ON
//!   correlation_conjuncts`, then a `Project`.
//!
//! - **Correlated, `need_check_max_rows=true`** (PushDownApplyFilter already
//!   ran; inner is NOT an aggregate): build a `GROUP BY corr-key` aggregate with
//!   `count(1)` and `any_value(scalar)`, `LEFT OUTER JOIN` on the correlation,
//!   then a `Project` that maps the Apply output column to `anyval` and adds an
//!   internal `assert_true(cnt IS NULL OR cnt <= 1, ...)` per-group row-check.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use super::scalar_utils;
use crate::sql::column_id::ColumnId;
use crate::sql::common::ApplyKind;
use crate::sql::common::{BinOp, JoinKind, OutputColumn};
use crate::sql::optimizer::operator::{
    AssertOneRowOp, LogicalAggregateOp, Operator, ProjectOp, ScalarProjectItem,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

pub(crate) struct ScalarApplyToJoin;

impl LogicalRewriteRule for ScalarApplyToJoin {
    fn name(&self) -> &'static str {
        "ScalarApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, expr: &OptExpr, ctx: &RewriteContext) -> bool {
        let _ = ctx;
        matches!(&expr.op, Operator::LogicalApply(a) if a.kind == ApplyKind::Scalar)
    }

    fn apply(&self, expr: OptExpr, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let arena = ctx.scalar_arena();
        let mut arena = arena.borrow_mut();
        match apply_opt(expr, ctx, &mut arena)? {
            Some(new_expr) => Ok(RewriteResult::Changed(new_expr)),
            None => Ok(RewriteResult::Unchanged),
        }
    }
}

fn apply_opt(
    expr: OptExpr,
    ctx: &mut RewriteContext,
    arena: &mut ScalarArena,
) -> Result<Option<OptExpr>, String> {
    let OptExpr {
        op,
        mut children,
        required_output_columns: _,
    } = expr;
    let Operator::LogicalApply(a) = op else {
        return Ok(None);
    };
    if a.kind != ApplyKind::Scalar {
        return Ok(None);
    }
    if children.len() != 2 {
        return Ok(None);
    }
    let right = children.remove(1);
    let left = children.remove(0);

    // --- Uncorrelated arm ---
    if a.correlation_column_ids.is_empty() {
        let provably_le_one_row = inner_is_provably_le_one_row(&right);
        let project_items = build_output_project_items(
            arena,
            &left,
            &right,
            a.inner_output_column_id,
            &a.output_column,
        )?;

        let inner_plan = if provably_le_one_row {
            right
        } else {
            OptExpr::new(
                Operator::LogicalAssertOneRow(AssertOneRowOp {
                    subquery_text: String::new(),
                }),
                vec![right],
            )
        };
        let join = scalar_utils::join(left, inner_plan, JoinKind::Cross, None);
        let project = scalar_utils::simple_project(join, project_items);
        return Ok(Some(project));
    }

    // --- Correlation-not-yet-hoisted guard ---
    // If there are correlation ids but correlation_conjuncts is still empty,
    // the push-down rule hasn't fired yet — leave unchanged so it fires first.
    if a.correlation_conjuncts.is_empty() {
        return Ok(None);
    }

    // --- Correlated, no-check arm (PushDownApplyAggFilter ran) ---
    if !a.need_check_max_rows {
        let cond = scalar_utils::combine_and(arena, a.correlation_conjuncts.clone());
        let project_items = build_output_project_items(
            arena,
            &left,
            &right,
            a.inner_output_column_id,
            &a.output_column,
        )?;

        let join = scalar_utils::join(left, right, JoinKind::LeftOuter, cond);
        let project = scalar_utils::simple_project(join, project_items);
        return Ok(Some(project));
    }

    // --- Correlated, with-check arm (PushDownApplyFilter ran) ---
    // Extract group keys from the correlation conjuncts' inner sides.
    let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
    let mut group_by: Vec<ScalarId> = Vec::new();
    let mut seen_gk_ids: HashSet<ColumnId> = HashSet::new();
    for conj in &a.correlation_conjuncts {
        let Some((_, inner_side)) = scalar_utils::orient_eq(arena, *conj, &corr_ids) else {
            // Cannot orient the conjunct — fall back to ApplyException.
            return Ok(None);
        };
        let Some(column_id) = scalar_utils::is_column_ref(arena, inner_side) else {
            // Non-ColumnRef inner side is out of M1b scope.
            return Ok(None);
        };
        if seen_gk_ids.insert(column_id) {
            group_by.push(inner_side);
        }
    }

    // Mint cnt and anyval output column ids.
    let factory = ctx
        .column_ref_factory()
        .ok_or_else(|| "ScalarApplyToJoin requires ColumnRefFactory".to_string())?;
    let mut factory = factory.borrow_mut();

    let inner_scalar_type = scalar_utils::find_column_type(&right, arena, a.inner_output_column_id)
        .unwrap_or(DataType::Null);
    let inner_scalar_nullable =
        scalar_utils::find_column_nullable(&right, arena, a.inner_output_column_id).unwrap_or(true);

    let cnt_id = factory.create(None, "count(1)".to_string(), DataType::Int64, false);
    let anyval_id = factory.create(
        None,
        "any_value".to_string(),
        inner_scalar_type.clone(),
        true,
    );
    // Mint internal assertion column id.
    let assert_id = factory.create(
        None,
        "__subquery_assertion".to_string(),
        DataType::Boolean,
        false,
    );
    drop(factory);

    // Ensure the agg input exposes both group keys and the inner scalar output.
    // If a leading Project doesn't include the group key columns, extend it.
    // The analyzer's leading Project always selects the inner scalar column,
    // so any_value's arg (a.inner_output_column_id) always resolves without
    // needing explicit enforcement here.
    let agg_input = ensure_exposes_columns(right, &group_by, arena)?;

    // Build group-key OutputColumns (reuse existing column ids, do NOT mint).
    let agg_input_columns = scalar_utils::opt_output_columns(&agg_input, arena)?;
    let gk_output_cols: Vec<OutputColumn> = group_by
        .iter()
        .map(|expr| {
            let column_id = scalar_utils::is_column_ref(arena, *expr)
                .expect("group key was verified as ColumnRef above");
            scalar_utils::find_output_column(&agg_input_columns, column_id)
                .cloned()
                .ok_or_else(|| format!("missing group key output column {:?}", column_id))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Build the vector aggregate: group by corr-key, count(1), any_value(scalar).
    let inner_scalar_ref = arena.intern(
        ScalarNode::ColumnRef(a.inner_output_column_id),
        inner_scalar_type.clone(),
        inner_scalar_nullable,
    );
    if let Some(inner_column) =
        scalar_utils::find_output_column(&agg_input_columns, a.inner_output_column_id)
    {
        arena.remember_project_output_display(
            inner_column.column_id,
            None,
            inner_column.name.clone(),
        );
    };

    let mut agg_output_cols = gk_output_cols.clone();
    let cnt_output = OutputColumn {
        column_id: cnt_id,
        name: "count(1)".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    };
    let anyval_output = OutputColumn {
        column_id: anyval_id,
        name: "any_value".to_string(),
        data_type: inner_scalar_type.clone(),
        nullable: true,
        is_internal: false,
    };
    agg_output_cols.push(cnt_output.clone());
    agg_output_cols.push(anyval_output.clone());

    let vector_agg = OptExpr::new(
        Operator::LogicalAggregate(LogicalAggregateOp::single(
            group_by,
            vec![
                scalar_utils::count_one_spec(arena),
                scalar_utils::any_value_spec(inner_scalar_ref),
            ],
            agg_output_cols,
        )),
        vec![agg_input],
    );

    // LEFT OUTER JOIN on the correlation conjuncts.
    let cond = scalar_utils::combine_and(arena, a.correlation_conjuncts.clone());
    let mut items = scalar_utils::left_project_items(&left, arena)?;
    let join = scalar_utils::join(left, vector_agg, JoinKind::LeftOuter, cond);

    // Build the output project.
    // Items: all left columns (pass-through) + anyval item (scalar output) +
    // internal assert_true item (row-check).
    // Map output_column to anyval (the scalar subquery result).
    let anyval_ref = scalar_utils::column_ref(arena, &anyval_output);
    items.push(ScalarProjectItem {
        expr: anyval_ref,
        output_name: a.output_column.name.clone(),
        output_column_id: a.output_column.column_id,
        expr_display: None,
    });

    // Build the assert_true condition: cnt IS NULL OR cnt <= 1
    let cnt_ref = scalar_utils::column_ref(arena, &cnt_output);
    let cnt_is_null = arena.intern(
        ScalarNode::IsNull {
            child: cnt_ref,
            negated: false,
        },
        DataType::Boolean,
        false,
    );
    let one = scalar_utils::int_literal(arena, 1);
    let cnt_le_1 =
        scalar_utils::binary_op(arena, BinOp::Le, cnt_ref, one, DataType::Boolean, false);
    let assert_cond = scalar_utils::binary_op(
        arena,
        BinOp::Or,
        cnt_is_null,
        cnt_le_1,
        DataType::Boolean,
        false,
    );
    let assert_expr = scalar_utils::assert_true(
        arena,
        assert_cond,
        "correlate scalar subquery result must 1 row",
    );
    items.push(ScalarProjectItem {
        expr: assert_expr,
        output_name: "__subquery_assertion".to_string(),
        output_column_id: assert_id,
        expr_display: None,
    });

    let project = scalar_utils::simple_project(join, items);

    // The assertion project item is a regular Project item. PruneProjectColumns
    // preserves it via the assert_true carve-out: items whose expr is an
    // assert_true FunctionCall are never dropped, even when their output_column_id
    // is not referenced upstream. tag_required_columns also unions the assert_true
    // item's column refs (cnt) into child_needed so the count column survives
    // through the aggregate below.

    Ok(Some(project))
}

/// Returns true iff the plan is provably at most 1 row:
/// - A global aggregate (empty group_by), possibly under a leading Project.
/// - A Values node with at most 1 row.
fn inner_is_provably_le_one_row(plan: &OptExpr) -> bool {
    match &plan.op {
        Operator::LogicalAggregate(agg) => agg.group_by.is_empty(),
        Operator::LogicalProject(_) => inner_is_provably_le_one_row(plan.unary_input()),
        Operator::LogicalValues(v) => v.rows.len() <= 1,
        _ => false,
    }
}

fn build_output_project_items(
    arena: &mut ScalarArena,
    left: &OptExpr,
    right: &OptExpr,
    inner_output_column_id: ColumnId,
    output_col: &OutputColumn,
) -> Result<Vec<ScalarProjectItem>, String> {
    let mut items = scalar_utils::left_project_items(left, arena)?;
    let inner_out_type = scalar_utils::find_column_type(right, arena, inner_output_column_id)
        .unwrap_or(DataType::Null);
    let inner_nullable =
        scalar_utils::find_column_nullable(right, arena, inner_output_column_id).unwrap_or(true);

    let inner_col_ref = arena.intern(
        ScalarNode::ColumnRef(inner_output_column_id),
        inner_out_type.clone(),
        inner_nullable,
    );

    let scalar_expr =
        if scalar_utils::is_count_aggregate_result(right, arena, inner_output_column_id) {
            // ifnull(count_result, 0): count(1) with LEFT OUTER returns NULL when no
            // match; normalize to 0 (SQL COUNT semantics).
            scalar_utils::ifnull_zero(arena, inner_col_ref, inner_out_type)
        } else {
            inner_col_ref
        };

    items.push(ScalarProjectItem {
        expr: scalar_expr,
        output_name: output_col.name.clone(),
        output_column_id: output_col.column_id,
        expr_display: None,
    });

    Ok(items)
}

/// Ensure the plan exposes the given group-key columns.
///
/// If the plan is a `Project` that already exposes all needed columns, return it
/// as-is. If the Project is missing some group keys, add pass-through items for them.
/// If there is no leading Project, return the plan as-is (Scan/Filter expose all
/// columns anyway).
///
/// The inner scalar output column need not be checked here: the analyzer's
/// leading Project always selects it, so any_value's arg resolves without
/// explicit enforcement.
fn ensure_exposes_columns(
    plan: OptExpr,
    group_by: &[ScalarId],
    arena: &mut ScalarArena,
) -> Result<OptExpr, String> {
    let Operator::LogicalProject(project) = &plan.op else {
        return Ok(plan);
    };
    let projected_ids: HashSet<ColumnId> = project
        .items
        .iter()
        .map(|item| item.output_column_id)
        .collect();
    let child = plan.unary_input();
    let child_columns = scalar_utils::opt_output_columns(child, arena)?;

    let mut new_items = project.items.clone();
    for group_key in group_by {
        let Some(column_id) = scalar_utils::is_column_ref(arena, *group_key) else {
            continue;
        };
        if projected_ids.contains(&column_id) {
            continue;
        }
        let output_column = scalar_utils::find_output_column(&child_columns, column_id)
            .cloned()
            .unwrap_or_else(|| OutputColumn {
                column_id,
                name: format!("col_{}", column_id.0),
                data_type: arena.data_type(*group_key).clone(),
                nullable: arena.nullable(*group_key),
                is_internal: false,
            });
        new_items.push(ScalarProjectItem {
            expr: *group_key,
            output_name: output_column.name,
            output_column_id: column_id,
            expr_display: None,
        });
    }

    if new_items.len() == project.items.len() {
        return Ok(plan);
    }

    let mut children = plan.children;
    let child = children
        .pop()
        .ok_or_else(|| "LogicalProject must have one child".to_string())?;
    Ok(OptExpr::new(
        Operator::LogicalProject(ProjectOp {
            items: new_items,
            output_qualifier: project.output_qualifier.clone(),
        }),
        vec![child],
    ))
}

#[cfg(test)]
mod tests {
    use crate::sql::planner::plan::*;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::convert::logical_plan_to_opt_expr;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rules::subquery::bridge::opt_expr_to_plan;
    use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::plan::{
        AggregateCall, ApplyKind, LogicalAggregateNode, LogicalApplyNode, LogicalPlanNode,
        LogicalScanNode, LogicalValuesNode, PlanNodeKind,
    };

    // ---- Column ID constants --------------------------------------------------
    const T1_K: ColumnId = ColumnId(1); // left (outer) key column
    const T2_K: ColumnId = ColumnId(2); // inner correlation column
    const T2_V2: ColumnId = ColumnId(3); // inner value column
    const MAX_RESULT: ColumnId = ColumnId(10); // output_column_id for max(v2)
    const APPLY_OUT: ColumnId = ColumnId(20); // the Apply's output column

    fn col_ref(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: false,
        }
    }

    fn col_ref_nullable(id: ColumnId, name: &str, dt: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: dt,
            nullable: true,
        }
    }

    fn eq_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn ctx_with_factory() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx.set_scalar_arena(Rc::new(RefCell::new(ScalarArena::new())));
        ctx
    }

    fn to_opt_expr(
        plan: LogicalPlanNode,
        ctx: &mut RewriteContext,
    ) -> crate::sql::optimizer::opt_expr::OptExpr {
        let arena = ctx.scalar_arena();
        logical_plan_to_opt_expr(&plan, &mut arena.borrow_mut())
    }

    fn make_left_values() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: T1_K,
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            vec![],
            None,
        )
    }

    fn make_t2_scan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: "t2".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: T2_K,
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: T2_V2,
                        name: "v2".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    /// Build a scalar-aggregate inner: `Aggregate{group_by:[], max(v2)}(Scan t2)`.
    fn make_scalar_agg_inner() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "max".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: MAX_RESULT,
                }],
                output_columns: vec![OutputColumn {
                    column_id: MAX_RESULT,
                    name: "max(v2)".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                }],
                already_pushed: false,
            }),
            vec![make_t2_scan()],
            None,
        )
    }

    // ---- Test (a): uncorrelated scalar-agg → CrossJoin (no AssertOneRow) -----

    /// An uncorrelated scalar aggregate Apply should rewrite to a CROSS JOIN
    /// directly (no AssertOneRow because the scalar agg is provably ≤1 row),
    /// wrapped in a Project mapping `output_column` → inner agg result.
    #[test]
    fn scalar_apply_to_join_uncorrelated_agg_no_assert_one_row() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![],
                // uncorrelated
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_left_values(), make_scalar_agg_inner()],
            None,
        );

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");

        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        // Outer shape: Project
        let PlanNodeKind::Project(proj) = &plan.kind else {
            panic!("expected Project, got: {plan:?}");
        };

        // Project input: CrossJoin
        let join_plan = plan.unary_input();
        let PlanNodeKind::Join(join) = &join_plan.kind else {
            panic!("expected Join under Project, got: {:?}", join_plan);
        };
        assert_eq!(join.join_type, JoinKind::Cross, "must be CROSS JOIN");
        assert!(
            join.condition.is_none(),
            "CROSS JOIN must have no condition"
        );

        // The join's right side must be the Aggregate directly (no AssertOneRow).
        assert!(
            matches!(&join_plan.right().kind, PlanNodeKind::Aggregate(_)),
            "right side must be Aggregate (no AssertOneRow for scalar agg); got: {:?}",
            join_plan.right()
        );

        // Project items: T1_K (pass-through) + APPLY_OUT → MAX_RESULT
        assert_eq!(proj.items.len(), 2, "project must have 2 items");
        assert_eq!(proj.items[0].output_column_id, T1_K);
        assert_eq!(proj.items[1].output_column_id, APPLY_OUT);
        let ExprKind::ColumnRef { column_id, .. } = &proj.items[1].expr.kind else {
            panic!("scalar project item must be ColumnRef");
        };
        assert_eq!(
            *column_id, MAX_RESULT,
            "scalar item must reference MAX_RESULT"
        );
    }

    // ---- Test (b): uncorrelated non-agg → CrossJoin + AssertOneRow -----------

    /// An uncorrelated non-aggregate inner should be wrapped in AssertOneRow
    /// before the CROSS JOIN.
    #[test]
    fn scalar_apply_to_join_uncorrelated_nonagg_wraps_assert_one_row() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        // Inner: Project(v2) over Scan — not provably ≤1 row.
        let inner = LogicalPlanNode::new(
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![ProjectItem {
                    expr: col_ref(T2_V2, "v2", DataType::Int64),
                    output_name: "v2".to_string(),
                    output_column_id: T2_V2,
                }],
                output_qualifier: None,
            }),
            vec![make_t2_scan()],
            None,
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: T2_V2,
                correlation_column_ids: vec![],
                // uncorrelated
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_left_values(), inner],
            None,
        );

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");

        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        let PlanNodeKind::Project(proj) = &plan.kind else {
            panic!("expected Project, got: {plan:?}");
        };

        let join_plan = plan.unary_input();
        let PlanNodeKind::Join(join) = &join_plan.kind else {
            panic!("expected Join, got: {:?}", join_plan);
        };
        assert_eq!(join.join_type, JoinKind::Cross, "must be CROSS JOIN");

        // The right side must be AssertOneRow wrapping the Project.
        let assert_plan = join_plan.right();
        let PlanNodeKind::AssertOneRow(_assert_node) = &assert_plan.kind else {
            panic!("right side must be AssertOneRow; got: {:?}", assert_plan);
        };
        assert!(
            matches!(&assert_plan.unary_input().kind, PlanNodeKind::Project(_)),
            "AssertOneRow input must be Project; got: {:?}",
            assert_plan.unary_input()
        );

        // Project items: T1_K + APPLY_OUT → T2_V2
        assert_eq!(proj.items.len(), 2);
        assert_eq!(proj.items[1].output_column_id, APPLY_OUT);
        let ExprKind::ColumnRef { column_id, .. } = &proj.items[1].expr.kind else {
            panic!("scalar project item must be ColumnRef");
        };
        assert_eq!(*column_id, T2_V2);
    }

    // ---- Test (c): correlated, no-check → LeftOuterJoin + Project ------------

    /// A correlated Apply with `need_check_max_rows=false` (after PushDownApplyAggFilter)
    /// should produce: `Project(LeftOuterJoin(left, vector_agg, ON cond))`.
    #[test]
    fn scalar_apply_to_join_correlated_without_check() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        // The vector aggregate after PushDownApplyAggFilter:
        // Aggregate{group_by:[t2.k], max(v2)}(Scan t2)
        let vector_agg = LogicalPlanNode::new(
            PlanNodeKind::Aggregate(LogicalAggregateNode {
                group_by: vec![col_ref(T2_K, "k", DataType::Int64)],
                aggregates: vec![AggregateCall {
                    name: "max".to_string(),
                    args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: MAX_RESULT,
                }],
                output_columns: vec![
                    OutputColumn {
                        column_id: T2_K,
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: MAX_RESULT,
                        name: "max(v2)".to_string(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
                already_pushed: false,
            }),
            vec![make_t2_scan()],
            None,
        );

        // Correlation conjunct: T1_K == T2_K
        let corr_conjunct = eq_expr(
            col_ref(T1_K, "k", DataType::Int64),
            col_ref(T2_K, "k", DataType::Int64),
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: MAX_RESULT,
                correlation_column_ids: vec![T1_K],
                correlation_conjuncts: vec![corr_conjunct],
                residual_predicate: None,
                need_check_max_rows: false,
                // PushDownApplyAggFilter already ran
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_left_values(), vector_agg],
            None,
        );

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");

        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        let PlanNodeKind::Project(proj) = &plan.kind else {
            panic!("expected Project, got: {plan:?}");
        };

        let join_plan = plan.unary_input();
        let PlanNodeKind::Join(join) = &join_plan.kind else {
            panic!("expected Join, got: {:?}", join_plan);
        };
        assert_eq!(
            join.join_type,
            JoinKind::LeftOuter,
            "must be LEFT OUTER JOIN"
        );
        assert!(join.condition.is_some(), "join must have a condition");

        // Condition references T1_K and T2_K.
        let cond = join.condition.as_ref().unwrap();
        let cond_ids = collect_column_id_refs(cond);
        assert!(cond_ids.contains(&T1_K), "condition must reference T1_K");
        assert!(cond_ids.contains(&T2_K), "condition must reference T2_K");

        // Project items: T1_K (pass-through) + APPLY_OUT → MAX_RESULT
        assert_eq!(proj.items.len(), 2, "project must have 2 items");
        assert_eq!(proj.items[0].output_column_id, T1_K);
        assert_eq!(proj.items[1].output_column_id, APPLY_OUT);
        let ExprKind::ColumnRef { column_id, .. } = &proj.items[1].expr.kind else {
            panic!("scalar project item must be ColumnRef");
        };
        assert_eq!(
            *column_id, MAX_RESULT,
            "scalar item must reference MAX_RESULT"
        );
    }

    // ---- Test (d): correlated, with-check → LeftOuterJoin over Agg + assert_true

    /// The most complex arm: correlated, `need_check_max_rows=true`.
    /// Input: Apply{ left=Values(t1.k), right=Scan(t2.k, t2.v2),
    ///               correlation_conjuncts=[T1_K==T2_K], need_check_max_rows=true }
    /// Expected output:
    ///   Project(
    ///     LeftOuterJoin(left, Agg{group_by:[t2.k], count(1)→cnt, any_value(v2)→anyval}(Scan)),
    ///     items: [T1_K, APPLY_OUT→anyval, __assertion→assert_true(cnt IS NULL OR cnt<=1, msg)]
    ///   )
    #[test]
    fn scalar_apply_to_join_correlated_with_check() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        // Inner after PushDownApplyFilter: Scan(t2.k, t2.v2) directly
        // (the filter was removed since only the correlation conjunct was there).
        // inner_output_column_id = T2_V2 (the scalar we want).
        let corr_conjunct = eq_expr(
            col_ref(T1_K, "k", DataType::Int64),
            col_ref(T2_K, "k", DataType::Int64),
        );

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                // simple scan, no agg
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: T2_V2,
                correlation_column_ids: vec![T1_K],
                correlation_conjuncts: vec![corr_conjunct],
                residual_predicate: None,
                need_check_max_rows: true,
                // PushDownApplyFilter ran; non-agg needs row check
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_left_values(), make_t2_scan()],
            None,
        );

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");

        let new_expr = match result {
            RewriteResult::Changed(e) => e,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let arena = ctx.scalar_arena();
        let plan = opt_expr_to_plan(&new_expr, &arena.borrow());

        // Outer shape: Project
        let PlanNodeKind::Project(proj) = &plan.kind else {
            panic!("expected Project, got: {plan:?}");
        };

        // Join: LeftOuter
        let join_plan = plan.unary_input();
        let PlanNodeKind::Join(join) = &join_plan.kind else {
            panic!("expected Join, got: {:?}", join_plan);
        };
        assert_eq!(
            join.join_type,
            JoinKind::LeftOuter,
            "must be LEFT OUTER JOIN"
        );
        assert!(join.condition.is_some(), "join must have condition");

        let cond = join.condition.as_ref().unwrap();
        let cond_ids = collect_column_id_refs(cond);
        assert!(cond_ids.contains(&T1_K), "condition must reference T1_K");
        assert!(cond_ids.contains(&T2_K), "condition must reference T2_K");

        // Right side: Aggregate with group_by=[T2_K], count(1), any_value(T2_V2)
        let PlanNodeKind::Aggregate(agg) = &join_plan.right().kind else {
            panic!("right side must be Aggregate; got: {:?}", join_plan.right());
        };

        // group_by must contain T2_K.
        assert_eq!(agg.group_by.len(), 1, "group_by must have 1 key");
        let ExprKind::ColumnRef {
            column_id: gk_id, ..
        } = &agg.group_by[0].kind
        else {
            panic!("group_by must be ColumnRef");
        };
        assert_eq!(*gk_id, T2_K, "group_by key must be T2_K");

        // Aggregates: count(1) and any_value(T2_V2).
        assert_eq!(agg.aggregates.len(), 2, "must have 2 aggregates");
        let cnt_call = &agg.aggregates[0];
        let anyval_call = &agg.aggregates[1];
        assert_eq!(cnt_call.name, "count", "first agg must be count");
        assert_eq!(
            anyval_call.name, "any_value",
            "second agg must be any_value"
        );

        // any_value's arg must reference T2_V2 (inner_output_column_id).
        let ExprKind::ColumnRef {
            column_id: av_arg_id,
            ..
        } = &anyval_call.args[0].kind
        else {
            panic!("any_value arg must be ColumnRef");
        };
        assert_eq!(
            *av_arg_id, T2_V2,
            "any_value must aggregate inner_output_column_id (T2_V2)"
        );

        let cnt_id = cnt_call.output_column_id;
        let anyval_id = anyval_call.output_column_id;

        // output_columns: [T2_K (group key), cnt, anyval]
        assert_eq!(
            agg.output_columns.len(),
            3,
            "output_columns must have 3 entries"
        );
        let out_ids: Vec<ColumnId> = agg.output_columns.iter().map(|c| c.column_id).collect();
        assert_eq!(out_ids[0], T2_K, "first output must be group key T2_K");
        assert_eq!(out_ids[1], cnt_id, "second output must be cnt");
        assert_eq!(out_ids[2], anyval_id, "third output must be anyval");

        // Project items: T1_K (pass-through) + APPLY_OUT→anyval + __assertion
        assert_eq!(proj.items.len(), 3, "project must have 3 items");

        // Pass-through
        assert_eq!(proj.items[0].output_column_id, T1_K);

        // Scalar output: APPLY_OUT → anyval
        assert_eq!(
            proj.items[1].output_column_id, APPLY_OUT,
            "second item must map APPLY_OUT"
        );
        let ExprKind::ColumnRef {
            column_id: scalar_id,
            ..
        } = &proj.items[1].expr.kind
        else {
            panic!(
                "scalar item must be ColumnRef; got: {:?}",
                proj.items[1].expr.kind
            );
        };
        assert_eq!(*scalar_id, anyval_id, "scalar item must reference anyval");

        // Internal assertion item
        let assert_item = &proj.items[2];
        assert_eq!(assert_item.output_name, "__subquery_assertion");
        // Expression must be assert_true(...)
        let ExprKind::FunctionCall { name, args, .. } = &assert_item.expr.kind else {
            panic!(
                "assertion item must be FunctionCall; got: {:?}",
                assert_item.expr.kind
            );
        };
        assert_eq!(name, "assert_true", "assertion must call assert_true");
        assert_eq!(args.len(), 2, "assert_true must have 2 args");

        // First arg: cnt IS NULL OR cnt <= 1
        let cond_arg = &args[0];
        let ExprKind::BinaryOp {
            op: BinOp::Or,
            left: left_or,
            right: right_or,
        } = &cond_arg.kind
        else {
            panic!("assert_true first arg must be OR; got: {:?}", cond_arg.kind);
        };
        // Left: cnt IS NULL
        let ExprKind::IsNull {
            expr: isnull_expr,
            negated: false,
        } = &left_or.kind
        else {
            panic!("left OR branch must be IS NULL; got: {:?}", left_or.kind);
        };
        let ExprKind::ColumnRef {
            column_id: isnull_id,
            ..
        } = &isnull_expr.kind
        else {
            panic!("IS NULL expr must be ColumnRef");
        };
        assert_eq!(*isnull_id, cnt_id, "IS NULL must check cnt column");

        // Right: cnt <= 1
        let ExprKind::BinaryOp {
            op: BinOp::Le,
            left: le_left,
            right: le_right,
        } = &right_or.kind
        else {
            panic!("right OR branch must be <=; got: {:?}", right_or.kind);
        };
        let ExprKind::ColumnRef {
            column_id: le_id, ..
        } = &le_left.kind
        else {
            panic!("<= left must be ColumnRef");
        };
        assert_eq!(*le_id, cnt_id, "<= must check cnt column");
        let ExprKind::Literal(LiteralValue::Int(1)) = &le_right.kind else {
            panic!("<= right must be Literal(1)");
        };

        // Second arg: the error message string
        let ExprKind::Literal(LiteralValue::String(msg)) = &args[1].kind else {
            panic!(
                "assert_true second arg must be String literal; got: {:?}",
                args[1].kind
            );
        };
        assert_eq!(
            msg, "correlate scalar subquery result must 1 row",
            "error message must match StarRocks' message"
        );
    }

    // ---- Guard test: correlated but conjuncts not yet hoisted → Unchanged ----

    #[test]
    fn scalar_apply_to_join_guard_correlation_not_hoisted_returns_unchanged() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        let apply = LogicalPlanNode::new(
            PlanNodeKind::Apply(LogicalApplyNode {
                kind: ApplyKind::Scalar,
                subquery_expr: col_ref_nullable(APPLY_OUT, "subq", DataType::Int64),
                output_column: OutputColumn {
                    column_id: APPLY_OUT,
                    name: "subq".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: true,
                },
                inner_output_column_id: T2_V2,
                // correlation_column_ids non-empty but correlation_conjuncts is empty
                // (push-down rule hasn't run yet)
                correlation_column_ids: vec![T1_K],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: true,
                use_semi_anti: false,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![make_left_values(), make_t2_scan()],
            None,
        );

        let expr = to_opt_expr(apply, &mut ctx);

        assert!(rule.matches(&expr, &ctx));
        let result = rule.apply(expr, &mut ctx).expect("apply must not error");
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must return Unchanged when correlation not yet hoisted"
        );
    }
}
