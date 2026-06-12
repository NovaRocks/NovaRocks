//! `PushDownApplyAggFilter` — ports StarRocks `PushDownApplyAggFilterRule`.
//!
//! Matches a correlated scalar `Apply` whose inner is:
//!   `[Project?] Aggregate{group_by: []}( Filter(corr_pred)(inner_scan) )`
//! Rewrites the inner to a **vector** aggregate grouped by the correlation key,
//! hoists the correlated EQ conjuncts onto `Apply.correlation_conjuncts`, keeps
//! residual conjuncts as a `Filter` below the aggregate, and sets
//! `need_check_max_rows = false`.

use std::collections::HashSet;

use super::decorrelate_util::{all_binary_eq, orient_eq, partition_conjuncts};
use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::combine_and;
use crate::sql::planner::plan::{
    AggregateNode, ApplyKind, ApplyNode, FilterNode, LogicalPlan, ProjectNode,
};

pub(crate) struct PushDownApplyAggFilter;

impl LogicalRewriteRule for PushDownApplyAggFilter {
    fn name(&self) -> &'static str {
        "PushDownApplyAggFilter"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        let LogicalPlan::Apply(a) = plan else {
            return false;
        };
        if a.kind != ApplyKind::Scalar {
            return false;
        }
        if a.correlation_column_ids.is_empty() {
            return false;
        }
        if !a.need_check_max_rows {
            return false;
        }
        let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();
        inner_is_correlated_scalar_agg(&a.right, &corr_ids)
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let corr_ids: HashSet<ColumnId> = a.correlation_column_ids.iter().copied().collect();

        // Peel the optional leading Project and destructure the inner.
        let (leading_project, agg_node, filter_node) = peel_inner(&a.right, &corr_ids)
            .ok_or_else(|| "PushDownApplyAggFilter: inner shape mismatch".to_string())?;

        // Split the Filter predicate into (correlated, residual).
        let predicate = filter_node.predicate.clone();
        let (correlated, residual) = partition_conjuncts(predicate, &corr_ids);

        if correlated.is_empty() {
            return Err(
                "correlated subquery without correlation predicate is not supported".to_string(),
            );
        }
        if !all_binary_eq(&correlated) {
            return Err(
                "non-EQ correlated predicate in correlated subquery is not supported".to_string(),
            );
        }

        // For each correlated EQ conjunct, orient it as (outer, inner) and
        // collect distinct inner-side ColumnRef expressions as new group-by keys.
        // Require each inner side to be a ColumnRef (non-column inner is M1c).
        let mut inner_key_exprs: Vec<TypedExpr> = Vec::new();
        let mut seen_inner_ids: HashSet<ColumnId> = HashSet::new();

        for conj in &correlated {
            let Some((_, inner_side)) = orient_eq(conj, &corr_ids) else {
                // Cannot orient the EQ — both/neither side is outer — fall through.
                return Ok(RewriteResult::Unchanged);
            };
            // Require the inner side to be a ColumnRef.
            let ExprKind::ColumnRef { column_id, .. } = &inner_side.kind else {
                // Non-column inner side — fall back (M1c concern).
                return Ok(RewriteResult::Unchanged);
            };
            if seen_inner_ids.insert(*column_id) {
                inner_key_exprs.push(inner_side.clone());
            }
        }

        // Build OutputColumn entries for the new group-by keys.
        // Reuse the inner key columns' existing ColumnIds and types — do NOT mint.
        let new_group_key_output_columns: Vec<OutputColumn> = inner_key_exprs
            .iter()
            .map(|expr| {
                let ExprKind::ColumnRef {
                    column_id,
                    column: col_name,
                    ..
                } = &expr.kind
                else {
                    unreachable!("verified above that inner_key_exprs are ColumnRefs");
                };
                OutputColumn {
                    column_id: *column_id,
                    name: col_name.clone(),
                    data_type: expr.data_type.clone(),
                    nullable: expr.nullable,
                    is_internal: false,
                }
            })
            .collect();

        // Rebuild the filter input: either drop the Filter entirely (all conjuncts
        // were correlated) or keep a Filter with just the residual conjuncts.
        let new_filter_input: LogicalPlan = if residual.is_empty() {
            *filter_node.input.clone()
        } else {
            LogicalPlan::Filter(FilterNode {
                predicate: combine_and(residual),
                input: filter_node.input.clone(),
                required_output_columns: None,
            })
        };

        // Rebuild the Aggregate: group_by = new_key_exprs (since the original was []),
        // output_columns = [group_key_cols..., original_agg_result_cols...].
        let new_group_by = inner_key_exprs.clone();
        let mut new_output_columns = new_group_key_output_columns.clone();
        new_output_columns.extend(agg_node.output_columns.clone());

        let new_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(new_filter_input),
            group_by: new_group_by,
            aggregates: agg_node.aggregates.clone(),
            output_columns: new_output_columns,
            already_pushed: agg_node.already_pushed,
            required_output_columns: None,
        });

        // Re-wrap in the leading Project if present, extending it to pass through
        // the new group-key columns so they're visible to the join condition.
        // The join condition (built by ScalarApplyToJoin, Task 3) needs the inner
        // key columns to be in the Project's output.
        let new_inner: LogicalPlan = if let Some(proj) = leading_project {
            let projected_ids: HashSet<ColumnId> = proj
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect();

            let mut new_items = proj.items.clone();
            for out_col in &new_group_key_output_columns {
                if !projected_ids.contains(&out_col.column_id) {
                    new_items.push(ProjectItem {
                        expr: TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: out_col.column_id,
                                qualifier: None,
                                column: out_col.name.clone(),
                            },
                            data_type: out_col.data_type.clone(),
                            nullable: out_col.nullable,
                        },
                        output_name: out_col.name.clone(),
                        output_column_id: out_col.column_id,
                    });
                }
            }

            LogicalPlan::Project(ProjectNode {
                input: Box::new(new_agg),
                items: new_items,
                output_qualifier: proj.output_qualifier.clone(),
                required_output_columns: None,
            })
        } else {
            new_agg
        };

        // Return the rewritten Apply: correlation_conjuncts = the correlated EQ
        // conjuncts (outer == inner), need_check_max_rows = false.
        Ok(RewriteResult::Changed(LogicalPlan::Apply(ApplyNode {
            right: Box::new(new_inner),
            correlation_conjuncts: correlated,
            need_check_max_rows: false,
            ..a
        })))
    }
}

/// Returns true iff the given plan has the shape:
///   `[Project?] Aggregate{group_by: []}( Filter{corr_pred}(inner) )`
/// where the Filter's predicate has at least one conjunct referencing `corr_ids`.
fn inner_is_correlated_scalar_agg(plan: &LogicalPlan, corr_ids: &HashSet<ColumnId>) -> bool {
    let after_project = match plan {
        LogicalPlan::Project(p) => p.input.as_ref(),
        other => other,
    };
    check_agg_over_corr_filter(after_project, corr_ids)
}

fn check_agg_over_corr_filter(plan: &LogicalPlan, corr_ids: &HashSet<ColumnId>) -> bool {
    let LogicalPlan::Aggregate(agg) = plan else {
        return false;
    };
    if !agg.group_by.is_empty() {
        return false;
    }
    let LogicalPlan::Filter(filter) = agg.input.as_ref() else {
        return false;
    };
    // At least one conjunct must reference a corr_id.
    use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};
    split_and(filter.predicate.clone())
        .iter()
        .any(|c| !collect_column_id_refs(c).is_disjoint(corr_ids))
}

/// Destructures the inner plan into `(leading_project, agg_node, filter_node)`.
/// Returns `None` if the shape doesn't match (no group-by Aggregate over a Filter).
fn peel_inner<'a>(
    plan: &'a LogicalPlan,
    corr_ids: &HashSet<ColumnId>,
) -> Option<(Option<&'a ProjectNode>, &'a AggregateNode, &'a FilterNode)> {
    match plan {
        LogicalPlan::Project(proj) => {
            let (agg, filter) = peel_agg_over_filter(&proj.input, corr_ids)?;
            Some((Some(proj), agg, filter))
        }
        other => {
            let (agg, filter) = peel_agg_over_filter(other, corr_ids)?;
            Some((None, agg, filter))
        }
    }
}

fn peel_agg_over_filter<'a>(
    plan: &'a LogicalPlan,
    _corr_ids: &HashSet<ColumnId>,
) -> Option<(&'a AggregateNode, &'a FilterNode)> {
    let LogicalPlan::Aggregate(agg) = plan else {
        return None;
    };
    if !agg.group_by.is_empty() {
        return None;
    }
    let LogicalPlan::Filter(filter) = agg.input.as_ref() else {
        return None;
    };
    Some((agg, filter))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::planner::plan::{
        AggregateCall, AggregateNode, ApplyKind, ApplyNode, FilterNode, LogicalPlan, ScanNode,
        ValuesNode,
    };

    // ---- Column ID constants ------------------------------------------------
    const T2_K: ColumnId = ColumnId(1); // t2.k  (inner correlation column)
    const T2_V2: ColumnId = ColumnId(2); // t2.v2 (inner value column)
    const OUTER_K: ColumnId = ColumnId(100); // t1.k as seen inside the subquery
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

    /// Build the inner plan: Aggregate{group_by:[], max(v2)}(Filter(t2.k==OUTER)(Scan t2))
    fn inner_correlated_agg() -> LogicalPlan {
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(make_t2_scan()),
            predicate: corr_pred,
            required_output_columns: None,
        });

        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(filter),
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

    fn correlated_scalar_agg_apply() -> LogicalPlan {
        let outer_values = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: OUTER_K,
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            required_output_columns: None,
        });

        LogicalPlan::Apply(ApplyNode {
            left: Box::new(outer_values),
            right: Box::new(inner_correlated_agg()),
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
            correlation_column_ids: vec![OUTER_K],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    /// Core correctness test: the rule decorrelates a scalar agg Apply.
    /// Input:  Apply{ right: Agg{group_by:[]}(Filter(t2.k==OUTER)(Scan t2)), need_check=true }
    /// Output: Apply{ right: Agg{group_by:[t2.k]}(Scan t2), need_check=false,
    ///                correlation_conjuncts=[OUTER==t2.k] }
    #[test]
    fn push_down_apply_agg_filter_decorrelates_scalar_agg() {
        let rule = PushDownApplyAggFilter;
        let plan = correlated_scalar_agg_apply();
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        assert!(
            rule.matches(&plan, &ctx),
            "rule must match a correlated scalar agg Apply"
        );

        let result = rule
            .apply(plan, &mut ctx)
            .expect("rule apply must not error");
        let new_plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlan::Apply(new_apply) = &new_plan else {
            panic!("expected Apply, got: {new_plan:?}");
        };

        // need_check_max_rows must be false.
        assert!(
            !new_apply.need_check_max_rows,
            "need_check_max_rows must be false after pushdown"
        );

        // correlation_conjuncts must be the OUTER == t2.k EQ.
        assert_eq!(new_apply.correlation_conjuncts.len(), 1);
        let conj = &new_apply.correlation_conjuncts[0];
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = &conj.kind
        else {
            panic!("correlation conjunct must be a BinaryOp(Eq), got: {conj:?}");
        };
        use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;
        let all_ids: HashSet<ColumnId> = collect_column_id_refs(left)
            .union(&collect_column_id_refs(right))
            .copied()
            .collect();
        assert!(all_ids.contains(&OUTER_K), "must reference OUTER_K");
        assert!(all_ids.contains(&T2_K), "must reference T2_K");

        // right child: Aggregate{group_by:[t2.k]}(Scan t2)
        let LogicalPlan::Aggregate(new_agg) = new_apply.right.as_ref() else {
            panic!("right child must be Aggregate, got: {:?}", new_apply.right);
        };

        // group_by must be [t2.k].
        assert_eq!(
            new_agg.group_by.len(),
            1,
            "group_by must have exactly one key"
        );
        let ExprKind::ColumnRef { column_id, .. } = &new_agg.group_by[0].kind else {
            panic!("group_by expression must be a ColumnRef");
        };
        assert_eq!(*column_id, T2_K, "group_by key must be T2_K");

        // max(v2) aggregate is preserved.
        assert_eq!(new_agg.aggregates.len(), 1);
        assert_eq!(new_agg.aggregates[0].name, "max");
        assert_eq!(new_agg.aggregates[0].output_column_id, MAX_RESULT);

        // output_columns: [T2_K (group key), MAX_RESULT (agg result)].
        assert_eq!(new_agg.output_columns.len(), 2);
        assert_eq!(new_agg.output_columns[0].column_id, T2_K);
        assert_eq!(new_agg.output_columns[1].column_id, MAX_RESULT);

        // Correlated Filter was removed; agg input is the Scan directly.
        let LogicalPlan::Scan(scan) = new_agg.input.as_ref() else {
            panic!(
                "agg input must be a Scan (correlated Filter removed), got: {:?}",
                new_agg.input
            );
        };
        assert_eq!(scan.table.name, "t2");
    }

    /// Residual-filter test: when the inner Filter has both correlated AND residual
    /// conjuncts, only the correlated ones are hoisted; residual stays as a Filter.
    #[test]
    fn push_down_apply_agg_filter_keeps_residual_filter() {
        use crate::sql::analysis::LiteralValue;

        // Filter: (t2.k == OUTER) AND (t2.v2 > 5)
        let corr_pred = eq_expr(
            col_ref(T2_K, "k", DataType::Int64),
            col_ref(OUTER_K, "k", DataType::Int64),
        );
        let residual_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(T2_V2, "v2", DataType::Int64)),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(5)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let combined_pred = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(corr_pred),
                op: BinOp::And,
                right: Box::new(residual_pred),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        let inner = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(make_t2_scan()),
                predicate: combined_pred,
                required_output_columns: None,
            })),
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
        });

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
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
            inner_output_column_id: MAX_RESULT,
            correlation_column_ids: vec![OUTER_K],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let rule = PushDownApplyAggFilter;
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        let result = rule
            .apply(apply, &mut ctx)
            .expect("rule apply must not error");
        let new_plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlan::Apply(new_apply) = &new_plan else {
            panic!("expected Apply");
        };
        assert!(!new_apply.need_check_max_rows);
        assert_eq!(new_apply.correlation_conjuncts.len(), 1);

        let LogicalPlan::Aggregate(new_agg) = new_apply.right.as_ref() else {
            panic!("right must be Aggregate");
        };
        // Residual filter must remain below the agg.
        let LogicalPlan::Filter(residual_filter) = new_agg.input.as_ref() else {
            panic!("agg input must be a Filter (residual) when there are non-correlated preds");
        };
        // The residual filter's input must be the scan.
        assert!(matches!(
            residual_filter.input.as_ref(),
            LogicalPlan::Scan(_)
        ));
    }

    #[test]
    fn push_down_apply_agg_filter_no_match_uncorrelated() {
        let rule = PushDownApplyAggFilter;
        let ctx = RewriteContext::for_query(Vec::<String>::new());

        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            right: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            kind: ApplyKind::Scalar,
            subquery_expr: col_ref(APPLY_OUT, "subq", DataType::Int64),
            output_column: OutputColumn {
                column_id: APPLY_OUT,
                name: "subq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: APPLY_OUT,
            correlation_column_ids: vec![], // uncorrelated
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(
            !rule.matches(&plan, &ctx),
            "must not match uncorrelated Apply"
        );
    }

    #[test]
    fn push_down_apply_agg_filter_no_match_already_decorrelated() {
        let rule = PushDownApplyAggFilter;
        let ctx = RewriteContext::for_query(Vec::<String>::new());

        let plan = LogicalPlan::Apply(ApplyNode {
            left: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            right: Box::new(inner_correlated_agg()),
            kind: ApplyKind::Scalar,
            subquery_expr: col_ref(APPLY_OUT, "subq", DataType::Int64),
            output_column: OutputColumn {
                column_id: APPLY_OUT,
                name: "subq".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: true,
            },
            inner_output_column_id: MAX_RESULT,
            correlation_column_ids: vec![OUTER_K],
            correlation_conjuncts: vec![eq_expr(
                col_ref(OUTER_K, "k", DataType::Int64),
                col_ref(T2_K, "k", DataType::Int64),
            )],
            residual_predicate: None,
            need_check_max_rows: false, // already decorrelated
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        assert!(
            !rule.matches(&plan, &ctx),
            "must not match when need_check_max_rows is already false"
        );
    }

    /// Multi-correlated-conjunct test: when the inner Filter has TWO correlated EQ
    /// conjuncts (`t2.a == OUTER1 AND t2.b == OUTER2`), the rule must:
    ///   - add both `t2.a` and `t2.b` to `group_by`
    ///   - include both group keys plus the original agg result in `output_columns`
    ///   - set `correlation_conjuncts.len() == 2`
    #[test]
    fn push_down_apply_agg_filter_promotes_multiple_correlation_keys() {
        use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

        // Additional column IDs for the two-key scenario.
        const T2_A: ColumnId = ColumnId(3); // t2.a  (first correlation key)
        const T2_B: ColumnId = ColumnId(4); // t2.b  (second correlation key)
        const OUTER1: ColumnId = ColumnId(101); // outer ref for t2.a
        const OUTER2: ColumnId = ColumnId(102); // outer ref for t2.b
        const SUM_RESULT: ColumnId = ColumnId(11); // output_column_id for sum(v2)

        // Build an inner plan:
        //   Aggregate{group_by:[]}(Filter(t2.a==OUTER1 AND t2.b==OUTER2)(Scan t2))
        let corr_pred_a = eq_expr(
            col_ref(T2_A, "a", DataType::Int64),
            col_ref(OUTER1, "a", DataType::Int64),
        );
        let corr_pred_b = eq_expr(
            col_ref(T2_B, "b", DataType::Int64),
            col_ref(OUTER2, "b", DataType::Int64),
        );
        let combined = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(corr_pred_a),
                op: BinOp::And,
                right: Box::new(corr_pred_b),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };

        // Extend make_t2_scan() with the two extra columns.
        let scan = LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: crate::sql::catalog::TableDef {
                name: "t2".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: T2_A,
                    name: "a".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: T2_B,
                    name: "b".to_string(),
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
        });

        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: combined,
            required_output_columns: None,
        });

        let inner = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(filter),
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_ref(T2_V2, "v2", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: SUM_RESULT,
            }],
            output_columns: vec![OutputColumn {
                column_id: SUM_RESULT,
                name: "sum(v2)".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            already_pushed: false,
            required_output_columns: None,
        });

        let outer_values = LogicalPlan::Values(ValuesNode {
            rows: vec![],
            columns: vec![
                OutputColumn {
                    column_id: OUTER1,
                    name: "a".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: OUTER2,
                    name: "b".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            required_output_columns: None,
        });

        let apply = LogicalPlan::Apply(ApplyNode {
            left: Box::new(outer_values),
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
            inner_output_column_id: SUM_RESULT,
            correlation_column_ids: vec![OUTER1, OUTER2],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        });

        let rule = PushDownApplyAggFilter;
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());

        assert!(rule.matches(&apply, &ctx), "rule must match two-key Apply");

        let result = rule
            .apply(apply, &mut ctx)
            .expect("rule apply must not error");
        let new_plan = match result {
            RewriteResult::Changed(p) => p,
            other => panic!("expected Changed, got: {other:?}"),
        };

        let LogicalPlan::Apply(new_apply) = &new_plan else {
            panic!("expected Apply");
        };

        // correlation_conjuncts must carry both correlated EQ predicates.
        assert_eq!(
            new_apply.correlation_conjuncts.len(),
            2,
            "must have exactly 2 correlation conjuncts"
        );

        let LogicalPlan::Aggregate(new_agg) = new_apply.right.as_ref() else {
            panic!("right child must be Aggregate");
        };

        // group_by must contain both T2_A and T2_B.
        let group_ids: HashSet<ColumnId> = new_agg
            .group_by
            .iter()
            .filter_map(|e| {
                if let ExprKind::ColumnRef { column_id, .. } = &e.kind {
                    Some(*column_id)
                } else {
                    None
                }
            })
            .collect();
        assert!(group_ids.contains(&T2_A), "group_by must include T2_A");
        assert!(group_ids.contains(&T2_B), "group_by must include T2_B");
        assert_eq!(group_ids.len(), 2, "group_by must have exactly 2 keys");

        // output_columns must include both group keys plus the original agg result.
        let out_ids: HashSet<ColumnId> =
            new_agg.output_columns.iter().map(|c| c.column_id).collect();
        assert!(out_ids.contains(&T2_A), "output_columns must include T2_A");
        assert!(out_ids.contains(&T2_B), "output_columns must include T2_B");
        assert!(
            out_ids.contains(&SUM_RESULT),
            "output_columns must include SUM_RESULT"
        );
        assert_eq!(
            new_agg.output_columns.len(),
            3,
            "output_columns must have 3 entries (2 group keys + 1 agg)"
        );

        // need_check_max_rows must be false after the rewrite.
        assert!(!new_apply.need_check_max_rows);

        // Each correlation conjunct must reference one outer id and one inner id.
        let outer_ids: HashSet<ColumnId> = [OUTER1, OUTER2].into();
        let inner_ids: HashSet<ColumnId> = [T2_A, T2_B].into();
        for conj in &new_apply.correlation_conjuncts {
            let all_refs: HashSet<ColumnId> = {
                let ExprKind::BinaryOp { left, right, .. } = &conj.kind else {
                    panic!("conjunct must be BinaryOp");
                };
                collect_column_id_refs(left)
                    .union(&collect_column_id_refs(right))
                    .copied()
                    .collect()
            };
            assert!(
                all_refs.iter().any(|id| outer_ids.contains(id)),
                "conjunct must reference an outer id"
            );
            assert!(
                all_refs.iter().any(|id| inner_ids.contains(id)),
                "conjunct must reference an inner id"
            );
        }
    }
}
