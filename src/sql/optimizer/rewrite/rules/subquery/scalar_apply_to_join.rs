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

use super::decorrelate_util::orient_eq;
use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::combine_and;
use crate::sql::planner::plan::{
    AggregateCall, AggregateNode, ApplyKind, AssertOneRowNode, JoinNode, LogicalPlan, ProjectNode,
};
use crate::sql::planner::plan_output_columns;

pub(crate) struct ScalarApplyToJoin;

impl LogicalRewriteRule for ScalarApplyToJoin {
    fn name(&self) -> &'static str {
        "ScalarApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(a) if a.kind == ApplyKind::Scalar)
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };

        // --- Uncorrelated arm ---
        if a.correlation_column_ids.is_empty() {
            let provably_le_one_row = inner_is_provably_le_one_row(&a.right);
            // Capture info needed for output project before consuming a.
            let left_ref = a.left.clone();
            let inner_output_column_id = a.inner_output_column_id;
            let output_col = a.output_column.clone();
            let is_count = is_count_aggregate_result(&a.right, inner_output_column_id);
            let inner_out_type =
                find_column_type(&a.right, inner_output_column_id).unwrap_or(DataType::Null);

            let inner_plan = if provably_le_one_row {
                *a.right
            } else {
                LogicalPlan::AssertOneRow(AssertOneRowNode {
                    input: a.right,
                    subquery_text: String::new(),
                    required_output_columns: None,
                })
            };
            let join = LogicalPlan::Join(JoinNode {
                left: a.left,
                right: Box::new(inner_plan),
                join_type: crate::sql::analysis::JoinKind::Cross,
                condition: None,
                required_output_columns: None,
            });
            let project = build_output_project_from_parts(
                join,
                &left_ref,
                inner_output_column_id,
                &inner_out_type,
                is_count,
                &output_col,
            )?;
            return Ok(RewriteResult::Changed(project));
        }

        // --- Correlation-not-yet-hoisted guard ---
        // If there are correlation ids but correlation_conjuncts is still empty,
        // the push-down rule hasn't fired yet — leave unchanged so it fires first.
        if a.correlation_conjuncts.is_empty() {
            return Ok(RewriteResult::Unchanged);
        }

        // --- Correlated, no-check arm (PushDownApplyAggFilter ran) ---
        if !a.need_check_max_rows {
            let cond = combine_and(a.correlation_conjuncts.clone());
            // Capture info needed before consuming a.
            let left_ref = a.left.clone();
            let inner_output_column_id = a.inner_output_column_id;
            let output_col = a.output_column.clone();
            let is_count = is_count_aggregate_result(&a.right, inner_output_column_id);
            let inner_out_type =
                find_column_type(&a.right, inner_output_column_id).unwrap_or(DataType::Null);

            let join = LogicalPlan::Join(JoinNode {
                left: a.left,
                right: a.right,
                join_type: crate::sql::analysis::JoinKind::LeftOuter,
                condition: Some(cond),
                required_output_columns: None,
            });
            let project = build_output_project_from_parts(
                join,
                &left_ref,
                inner_output_column_id,
                &inner_out_type,
                is_count,
                &output_col,
            )?;
            return Ok(RewriteResult::Changed(project));
        }

        // --- Correlated, with-check arm (PushDownApplyFilter ran) ---
        // Extract group keys from the correlation conjuncts' inner sides.
        let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
        let mut gk_exprs: Vec<TypedExpr> = Vec::new();
        let mut seen_gk_ids: HashSet<ColumnId> = HashSet::new();
        for conj in &a.correlation_conjuncts {
            let Some((_, inner_side)) = orient_eq(conj, &corr_ids) else {
                // Cannot orient the conjunct — fall back to ApplyException.
                return Ok(RewriteResult::Unchanged);
            };
            let ExprKind::ColumnRef { column_id, .. } = &inner_side.kind else {
                // Non-ColumnRef inner side is out of M1b scope.
                return Ok(RewriteResult::Unchanged);
            };
            if seen_gk_ids.insert(*column_id) {
                gk_exprs.push(inner_side.clone());
            }
        }

        // Mint cnt and anyval output column ids.
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "ScalarApplyToJoin requires ColumnRefFactory".to_string())?;
        let mut factory = factory.borrow_mut();

        // Find the inner scalar column's type from the inner plan.
        let inner_scalar_type =
            find_column_type(&a.right, a.inner_output_column_id).unwrap_or(DataType::Null);

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
        let agg_input = ensure_exposes_columns(&a.right, &gk_exprs)?;

        // Build group-key OutputColumns (reuse existing column ids, do NOT mint).
        let gk_output_cols: Vec<OutputColumn> = gk_exprs
            .iter()
            .map(|e| {
                let ExprKind::ColumnRef {
                    column_id,
                    column: col_name,
                    ..
                } = &e.kind
                else {
                    unreachable!("verified as ColumnRef above");
                };
                OutputColumn {
                    column_id: *column_id,
                    name: col_name.clone(),
                    data_type: e.data_type.clone(),
                    nullable: e.nullable,
                    is_internal: false,
                }
            })
            .collect();

        // Build the vector aggregate: group by corr-key, count(1), any_value(scalar).
        let cnt_agg = AggregateCall {
            name: "count".to_string(),
            args: vec![TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(1)),
                data_type: DataType::Int64,
                nullable: false,
            }],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: cnt_id,
        };
        let anyval_col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: a.inner_output_column_id,
                qualifier: None,
                column: "inner_scalar".to_string(),
            },
            data_type: inner_scalar_type.clone(),
            nullable: true,
        };
        let anyval_agg = AggregateCall {
            name: "any_value".to_string(),
            args: vec![anyval_col_ref],
            distinct: false,
            result_type: inner_scalar_type.clone(),
            order_by: vec![],
            output_column_id: anyval_id,
        };

        let mut agg_output_cols = gk_output_cols.clone();
        agg_output_cols.push(OutputColumn {
            column_id: cnt_id,
            name: "count(1)".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        });
        agg_output_cols.push(OutputColumn {
            column_id: anyval_id,
            name: "any_value".to_string(),
            data_type: inner_scalar_type.clone(),
            nullable: true,
            is_internal: false,
        });

        let vector_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(agg_input),
            group_by: gk_exprs,
            aggregates: vec![cnt_agg, anyval_agg],
            output_columns: agg_output_cols,
            already_pushed: false,
            required_output_columns: None,
        });

        // LEFT OUTER JOIN on the correlation conjuncts.
        let cond = combine_and(a.correlation_conjuncts.clone());
        let join = LogicalPlan::Join(JoinNode {
            left: a.left.clone(),
            right: Box::new(vector_agg),
            join_type: crate::sql::analysis::JoinKind::LeftOuter,
            condition: Some(cond),
            required_output_columns: None,
        });

        // Build the output project.
        // Items: all left columns (pass-through) + anyval item (scalar output) +
        // internal assert_true item (row-check).
        let left_cols = plan_output_columns(&a.left)?;
        let mut items: Vec<ProjectItem> = left_cols
            .iter()
            .map(|c| ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: c.column_id,
                        qualifier: None,
                        column: c.name.clone(),
                    },
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                },
                output_name: c.name.clone(),
                output_column_id: c.column_id,
            })
            .collect();

        // Map output_column to anyval (the scalar subquery result).
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: anyval_id,
                    qualifier: None,
                    column: "any_value".to_string(),
                },
                data_type: inner_scalar_type.clone(),
                nullable: true,
            },
            output_name: a.output_column.name.clone(),
            output_column_id: a.output_column.column_id,
        });

        // Build the assert_true condition: cnt IS NULL OR cnt <= 1
        let cnt_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cnt_id,
                qualifier: None,
                column: "count(1)".to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let cnt_is_null = TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(cnt_ref.clone()),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let cnt_le_1 = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(cnt_ref),
                op: BinOp::Le,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let assert_cond = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(cnt_is_null),
                op: BinOp::Or,
                right: Box::new(cnt_le_1),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let assert_expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "assert_true".to_string(),
                args: vec![
                    assert_cond,
                    TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String(
                            "correlate scalar subquery result must 1 row".to_string(),
                        )),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                ],
                distinct: false,
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        items.push(ProjectItem {
            expr: assert_expr,
            output_name: "__subquery_assertion".to_string(),
            output_column_id: assert_id,
        });

        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(join),
            items,
            output_qualifier: None,
            required_output_columns: None,
        });

        // The assertion project item is a regular Project item. PruneProjectColumns
        // preserves it via the assert_true carve-out: items whose expr is an
        // assert_true FunctionCall are never dropped, even when their output_column_id
        // is not referenced upstream. tag_required_columns also unions the assert_true
        // item's column refs (cnt) into child_needed so the count column survives
        // through the aggregate below.

        Ok(RewriteResult::Changed(project))
    }
}

/// Returns true iff the plan is provably at most 1 row:
/// - A global aggregate (empty group_by), possibly under a leading Project.
/// - A Values node with at most 1 row.
fn inner_is_provably_le_one_row(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(agg) => agg.group_by.is_empty(),
        LogicalPlan::Project(p) => inner_is_provably_le_one_row(&p.input),
        LogicalPlan::Values(v) => v.rows.len() <= 1,
        _ => false,
    }
}

/// Build the output project for uncorrelated and no-check correlated arms.
///
/// Takes pre-computed pieces to avoid borrow-after-partial-move in the caller:
/// - `left` — the original left plan (to get pass-through columns)
/// - `inner_output_column_id` — the inner scalar result column id
/// - `inner_out_type` — data type of the inner scalar column
/// - `is_count` — whether the inner column is produced by a `count` aggregate
///   (if true, wrap in `ifnull(..., 0)`)
/// - `output_col` — the Apply's minted output column (id and name)
fn build_output_project_from_parts(
    child: LogicalPlan,
    left: &LogicalPlan,
    inner_output_column_id: ColumnId,
    inner_out_type: &DataType,
    is_count: bool,
    output_col: &OutputColumn,
) -> Result<LogicalPlan, String> {
    let left_cols = plan_output_columns(left)?;
    let mut items: Vec<ProjectItem> = left_cols
        .iter()
        .map(|c| ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: c.column_id,
                    qualifier: None,
                    column: c.name.clone(),
                },
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            },
            output_name: c.name.clone(),
            output_column_id: c.column_id,
        })
        .collect();

    let inner_col_ref = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: inner_output_column_id,
            qualifier: None,
            column: "inner_scalar".to_string(),
        },
        data_type: inner_out_type.clone(),
        nullable: true,
    };

    let scalar_expr = if is_count {
        // ifnull(count_result, 0): count(1) with LEFT OUTER returns NULL when no
        // match; normalize to 0 (SQL COUNT semantics).
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "ifnull".to_string(),
                args: vec![
                    inner_col_ref,
                    TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(0)),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                ],
                distinct: false,
            },
            data_type: inner_out_type.clone(),
            nullable: false,
        }
    } else {
        inner_col_ref
    };

    items.push(ProjectItem {
        expr: scalar_expr,
        output_name: output_col.name.clone(),
        output_column_id: output_col.column_id,
    });

    Ok(LogicalPlan::Project(ProjectNode {
        input: Box::new(child),
        items,
        output_qualifier: None,
        required_output_columns: None,
    }))
}

/// Walk the plan to find the data type of a column with the given id.
/// Looks at Aggregate output_columns, Project items, Scan columns, and Values columns.
fn find_column_type(plan: &LogicalPlan, col_id: ColumnId) -> Option<DataType> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Check output_columns.
            for oc in &agg.output_columns {
                if oc.column_id == col_id {
                    return Some(oc.data_type.clone());
                }
            }
            // Also check input for column type (for group-key sourced columns).
            find_column_type(&agg.input, col_id)
        }
        LogicalPlan::Project(p) => {
            for item in &p.items {
                if item.output_column_id == col_id {
                    return Some(item.expr.data_type.clone());
                }
            }
            find_column_type(&p.input, col_id)
        }
        LogicalPlan::Filter(f) => find_column_type(&f.input, col_id),
        LogicalPlan::Scan(s) => {
            for oc in &s.columns {
                if oc.column_id == col_id {
                    return Some(oc.data_type.clone());
                }
            }
            None
        }
        LogicalPlan::Values(v) => {
            for oc in &v.columns {
                if oc.column_id == col_id {
                    return Some(oc.data_type.clone());
                }
            }
            None
        }
        LogicalPlan::AssertOneRow(n) => find_column_type(&n.input, col_id),
        // Note: does not model outer-join null-extension (nullability may be
        // understated for columns from the null-extended side). This function
        // is only called on the inner subquery plan, never on a join, so this
        // is not a live issue in the current call sites.
        LogicalPlan::Join(j) => {
            find_column_type(&j.left, col_id).or_else(|| find_column_type(&j.right, col_id))
        }
        _ => None,
    }
}

/// Returns true iff `col_id` is the output column of a `count` aggregate call
/// in the inner plan.
///
/// When traversing a Project, the outer `col_id` must be mapped to the
/// inner column id via the Project's items (a Project may rename a count
/// aggregate output from id X to outer id Y; the Aggregate carries X, not Y).
fn is_count_aggregate_result(plan: &LogicalPlan, col_id: ColumnId) -> bool {
    match plan {
        LogicalPlan::Aggregate(agg) => agg
            .aggregates
            .iter()
            .any(|call| call.output_column_id == col_id && call.name == "count"),
        LogicalPlan::Project(p) => {
            // Translate col_id through the Project: find the ProjectItem whose
            // output_column_id == col_id, then follow its expr ColumnRef into
            // the child. This is necessary because apply_query_modifiers wraps
            // the inner Aggregate in a Project with fresh output ColumnIds.
            let inner_id = p.items.iter().find_map(|item| {
                if item.output_column_id == col_id {
                    if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind {
                        Some(*column_id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            });
            if let Some(translated_id) = inner_id {
                is_count_aggregate_result(&p.input, translated_id)
            } else {
                false
            }
        }
        LogicalPlan::Filter(f) => is_count_aggregate_result(&f.input, col_id),
        _ => false,
    }
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
    plan: &LogicalPlan,
    gk_exprs: &[TypedExpr],
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Project(proj) => {
            let projected_ids: HashSet<ColumnId> =
                proj.items.iter().map(|i| i.output_column_id).collect();

            let mut new_items = proj.items.clone();
            for gk in gk_exprs {
                let ExprKind::ColumnRef {
                    column_id,
                    column: col_name,
                    ..
                } = &gk.kind
                else {
                    continue;
                };
                if !projected_ids.contains(column_id) {
                    new_items.push(ProjectItem {
                        expr: gk.clone(),
                        output_name: col_name.clone(),
                        output_column_id: *column_id,
                    });
                }
            }

            if new_items.len() == proj.items.len() {
                // Nothing added — return original.
                Ok(plan.clone())
            } else {
                Ok(LogicalPlan::Project(ProjectNode {
                    input: proj.input.clone(),
                    items: new_items,
                    output_qualifier: proj.output_qualifier.clone(),
                    required_output_columns: None,
                }))
            }
        }
        // No leading Project: Scan/Filter already expose all columns.
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, LogicalPlan, ScanNode,
        ValuesNode,
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

    fn make_left_values() -> LogicalPlan {
        LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: T1_K,
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        })
    }

    fn make_t2_scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
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
            required_output_columns: None,
        })
    }

    /// Build a scalar-aggregate inner: `Aggregate{group_by:[], max(v2)}(Scan t2)`.
    fn make_scalar_agg_inner() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(make_t2_scan()),
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
            required_output_columns: None,
        })
    }

    fn ctx_with_factory() -> RewriteContext {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::new(RefCell::new(ColumnRefFactory::new())));
        ctx
    }

    // ---- Test (a): uncorrelated scalar-agg → CrossJoin (no AssertOneRow) -----

    /// An uncorrelated scalar aggregate Apply should rewrite to a CROSS JOIN
    /// directly (no AssertOneRow because the scalar agg is provably ≤1 row),
    /// wrapped in a Project mapping `output_column` → inner agg result.
    #[test]
    fn scalar_apply_to_join_uncorrelated_agg_no_assert_one_row() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_left_values()),
            right: Box::new(make_scalar_agg_inner()),
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
            correlation_column_ids: vec![], // uncorrelated
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&apply, &ctx));
        let result = rule.apply(apply, &mut ctx).expect("apply must not error");

        let plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        // Outer shape: Project
        let LogicalPlan::Project(proj) = &plan else {
            panic!("expected Project, got: {plan:?}");
        };

        // Project input: CrossJoin
        let LogicalPlan::Join(join) = proj.input.as_ref() else {
            panic!("expected Join under Project, got: {:?}", proj.input);
        };
        assert_eq!(join.join_type, JoinKind::Cross, "must be CROSS JOIN");
        assert!(
            join.condition.is_none(),
            "CROSS JOIN must have no condition"
        );

        // The join's right side must be the Aggregate directly (no AssertOneRow).
        assert!(
            matches!(join.right.as_ref(), LogicalPlan::Aggregate(_)),
            "right side must be Aggregate (no AssertOneRow for scalar agg); got: {:?}",
            join.right
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
        let inner = LogicalPlan::Project(ProjectNode {
            input: Box::new(make_t2_scan()),
            items: vec![ProjectItem {
                expr: col_ref(T2_V2, "v2", DataType::Int64),
                output_name: "v2".to_string(),
                output_column_id: T2_V2,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_left_values()),
            right: Box::new(inner),
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
            correlation_column_ids: vec![], // uncorrelated
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&apply, &ctx));
        let result = rule.apply(apply, &mut ctx).expect("apply must not error");

        let plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlan::Project(proj) = &plan else {
            panic!("expected Project, got: {plan:?}");
        };

        let LogicalPlan::Join(join) = proj.input.as_ref() else {
            panic!("expected Join, got: {:?}", proj.input);
        };
        assert_eq!(join.join_type, JoinKind::Cross, "must be CROSS JOIN");

        // The right side must be AssertOneRow wrapping the Project.
        let LogicalPlan::AssertOneRow(assert_node) = join.right.as_ref() else {
            panic!("right side must be AssertOneRow; got: {:?}", join.right);
        };
        assert!(
            matches!(assert_node.input.as_ref(), LogicalPlan::Project(_)),
            "AssertOneRow input must be Project; got: {:?}",
            assert_node.input
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
        let vector_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(make_t2_scan()),
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
            required_output_columns: None,
        });

        // Correlation conjunct: T1_K == T2_K
        let corr_conjunct = eq_expr(
            col_ref(T1_K, "k", DataType::Int64),
            col_ref(T2_K, "k", DataType::Int64),
        );

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_left_values()),
            right: Box::new(vector_agg),
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
            need_check_max_rows: false, // PushDownApplyAggFilter already ran
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&apply, &ctx));
        let result = rule.apply(apply, &mut ctx).expect("apply must not error");

        let plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlan::Project(proj) = &plan else {
            panic!("expected Project, got: {plan:?}");
        };

        let LogicalPlan::Join(join) = proj.input.as_ref() else {
            panic!("expected Join, got: {:?}", proj.input);
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

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_left_values()),
            right: Box::new(make_t2_scan()), // simple scan, no agg
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
            need_check_max_rows: true, // PushDownApplyFilter ran; non-agg needs row check
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(rule.matches(&apply, &ctx));
        let result = rule.apply(apply, &mut ctx).expect("apply must not error");

        let plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        // Outer shape: Project
        let LogicalPlan::Project(proj) = &plan else {
            panic!("expected Project, got: {plan:?}");
        };

        // Join: LeftOuter
        let LogicalPlan::Join(join) = proj.input.as_ref() else {
            panic!("expected Join, got: {:?}", proj.input);
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
        let LogicalPlan::Aggregate(agg) = join.right.as_ref() else {
            panic!("right side must be Aggregate; got: {:?}", join.right);
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
            "error message must match StarRocks'  message"
        );
    }

    // ---- Guard test: correlated but conjuncts not yet hoisted → Unchanged ----

    #[test]
    fn scalar_apply_to_join_guard_correlation_not_hoisted_returns_unchanged() {
        let rule = ScalarApplyToJoin;
        let mut ctx = ctx_with_factory();

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(make_left_values()),
            right: Box::new(make_t2_scan()),
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
            required_output_columns: None,
        });

        assert!(rule.matches(&apply, &ctx));
        let result = rule.apply(apply, &mut ctx).expect("apply must not error");
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "must return Unchanged when correlation not yet hoisted"
        );
    }
}
