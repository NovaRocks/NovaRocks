//! Shared helpers for the EXISTS/IN to-join rules (ExistentialApplyToJoin,
//! QuantifiedApplyToJoin). Locates the inner subquery's correlated WHERE and
//! lifts it into a join ON condition, leaving an outer-reference-free `right`
//! subtree.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;
use crate::sql::planner::plan::{FilterNode, LogicalPlan, ProjectNode};
use crate::sql::planner::plan_output_columns;

/// Result of lifting a correlated subquery's WHERE into a join ON.
#[allow(dead_code)] // Consumed by unregistered Task 4/Task 5 rules until Task 6.
pub(super) struct LiftedInner {
    /// The outer subtree's right child for the join (no outer references).
    pub right: LogicalPlan,
    /// The predicate lifted out of the inner Filter (correlation + residual),
    /// or None when the inner had no Filter to lift.
    pub on_predicate: Option<TypedExpr>,
}

/// For a correlated Apply.right of shape `[Project?] Filter(<rel>)`, return the
/// `<rel>` (with the Project re-applied if present) plus the Filter predicate to
/// move into the ON. Returns None if the expected shape is absent or if the
/// projected right side cannot expose the inner columns referenced by the lifted
/// predicate (caller -> Unchanged). For an uncorrelated inner, callers should
/// NOT call this - they keep `right` intact and build the ON from the IN key /
/// `true`.
#[allow(dead_code)] // Consumed by unregistered Task 4/Task 5 rules until Task 6.
pub(super) fn lift_correlated_inner(
    inner: LogicalPlan,
    outer_correlation_column_ids: &[ColumnId],
) -> Option<LiftedInner> {
    match inner {
        LogicalPlan::Project(p) => {
            let ProjectNode {
                input,
                items,
                output_qualifier,
                required_output_columns,
            } = p;
            match *input {
                LogicalPlan::Filter(f) => {
                    let FilterNode {
                        input, predicate, ..
                    } = f;
                    let predicate =
                        normalize_correlated_on_predicate(predicate, outer_correlation_column_ids);
                    let items = expose_predicate_inner_columns(
                        items,
                        &predicate,
                        input.as_ref(),
                        outer_correlation_column_ids,
                    )?;
                    Some(LiftedInner {
                        right: LogicalPlan::Project(ProjectNode {
                            input,
                            items,
                            output_qualifier,
                            required_output_columns,
                        }),
                        on_predicate: Some(predicate),
                    })
                }
                _ => None,
            }
        }
        LogicalPlan::Filter(f) => {
            let FilterNode {
                input, predicate, ..
            } = f;
            let predicate =
                normalize_correlated_on_predicate(predicate, outer_correlation_column_ids);
            if !predicate_inner_refs_available(
                &predicate,
                input.as_ref(),
                outer_correlation_column_ids,
            )? {
                return None;
            }
            Some(LiftedInner {
                right: *input,
                on_predicate: Some(predicate),
            })
        }
        _ => None,
    }
}

fn normalize_correlated_on_predicate(
    predicate: TypedExpr,
    outer_correlation_column_ids: &[ColumnId],
) -> TypedExpr {
    let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
    normalize_correlated_on_predicate_inner(predicate, &outer_ids)
}

fn normalize_correlated_on_predicate_inner(
    predicate: TypedExpr,
    outer_ids: &HashSet<ColumnId>,
) -> TypedExpr {
    let TypedExpr {
        kind,
        data_type,
        nullable,
    } = predicate;
    match kind {
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::And | BinOp::Or) => {
            TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(normalize_correlated_on_predicate_inner(*left, outer_ids)),
                    op,
                    right: Box::new(normalize_correlated_on_predicate_inner(*right, outer_ids)),
                },
                data_type,
                nullable,
            }
        }
        ExprKind::BinaryOp { left, op, right }
            if matches!(
                op,
                BinOp::Eq
                    | BinOp::EqForNull
                    | BinOp::Ne
                    | BinOp::Lt
                    | BinOp::Le
                    | BinOp::Gt
                    | BinOp::Ge
            ) =>
        {
            let left_outer_only = expr_refs_outer_only(&left, outer_ids);
            let right_outer_only = expr_refs_outer_only(&right, outer_ids);
            let left_has_outer = expr_refs_any_outer(&left, outer_ids);
            let right_has_outer = expr_refs_any_outer(&right, outer_ids);
            match (
                left_outer_only && !right_has_outer,
                right_outer_only && !left_has_outer,
            ) {
                (true, false) => TypedExpr {
                    kind: ExprKind::BinaryOp { left, op, right },
                    data_type,
                    nullable,
                },
                (false, true) => {
                    let inner_type = left.data_type.clone();
                    let inner_expr = *left;
                    let mut outer_expr = *right;
                    if outer_expr.data_type != inner_type {
                        outer_expr = TypedExpr {
                            data_type: inner_type.clone(),
                            nullable: outer_expr.nullable,
                            kind: ExprKind::Cast {
                                expr: Box::new(outer_expr),
                                target: inner_type,
                            },
                        };
                    }
                    TypedExpr {
                        kind: ExprKind::BinaryOp {
                            left: Box::new(outer_expr),
                            op: reverse_comparison_op(op),
                            right: Box::new(inner_expr),
                        },
                        data_type,
                        nullable,
                    }
                }
                _ => TypedExpr {
                    kind: ExprKind::BinaryOp { left, op, right },
                    data_type,
                    nullable,
                },
            }
        }
        kind => TypedExpr {
            kind,
            data_type,
            nullable,
        },
    }
}

fn reverse_comparison_op(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        _ => op,
    }
}

fn expr_refs_any_outer(expr: &TypedExpr, outer_ids: &HashSet<ColumnId>) -> bool {
    collect_column_id_refs(expr)
        .into_iter()
        .any(|column_id| outer_ids.contains(&column_id))
}

fn expr_refs_outer_only(expr: &TypedExpr, outer_ids: &HashSet<ColumnId>) -> bool {
    let refs = collect_column_id_refs(expr);
    !refs.is_empty()
        && refs
            .into_iter()
            .all(|column_id| outer_ids.contains(&column_id))
}

fn expose_predicate_inner_columns(
    mut items: Vec<ProjectItem>,
    predicate: &TypedExpr,
    child: &LogicalPlan,
    outer_correlation_column_ids: &[ColumnId],
) -> Option<Vec<ProjectItem>> {
    let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
    let projected_ids: HashSet<ColumnId> = items.iter().map(|item| item.output_column_id).collect();
    let mut missing_project_ids: HashSet<ColumnId> = collect_column_id_refs(predicate)
        .into_iter()
        .filter(|column_id| !outer_ids.contains(column_id))
        .filter(|column_id| !projected_ids.contains(column_id))
        .collect();

    if missing_project_ids.is_empty() {
        return Some(items);
    }

    for output in plan_output_columns(child).ok()? {
        if missing_project_ids.remove(&output.column_id) {
            items.push(ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: output.column_id,
                        qualifier: None,
                        column: output.name.clone(),
                    },
                    data_type: output.data_type.clone(),
                    nullable: output.nullable,
                },
                output_name: output.name,
                output_column_id: output.column_id,
            });
        }
    }

    if missing_project_ids.is_empty() {
        Some(items)
    } else {
        None
    }
}

fn predicate_inner_refs_available(
    predicate: &TypedExpr,
    child: &LogicalPlan,
    outer_correlation_column_ids: &[ColumnId],
) -> Option<bool> {
    let outer_ids: HashSet<ColumnId> = outer_correlation_column_ids.iter().copied().collect();
    let inner_ids: HashSet<ColumnId> = collect_column_id_refs(predicate)
        .into_iter()
        .filter(|column_id| !outer_ids.contains(column_id))
        .collect();
    if inner_ids.is_empty() {
        return Some(true);
    }

    let child_output_ids: HashSet<ColumnId> = plan_output_columns(child)
        .ok()?
        .into_iter()
        .map(|output| output.column_id)
        .collect();
    Some(
        inner_ids
            .into_iter()
            .all(|column_id| child_output_ids.contains(&column_id)),
    )
}

/// `coalesce(pred, false)` as a Boolean TypedExpr - used for NOT IN's lifted
/// predicate when it is nullable (legacy NAAJ semantics).
#[allow(dead_code)]
pub(super) fn coalesce_false(pred: TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![
                pred,
                TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Bool(false)),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
            ],
            distinct: false,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

/// `Literal(true)` Boolean expr (uncorrelated EXISTS join ON).
#[allow(dead_code)] // Consumed by unregistered Task 4 rule until Task 6.
pub(super) fn literal_true() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Bool(true)),
        data_type: DataType::Boolean,
        nullable: false,
    }
}

/// Build a bare `Eq` Boolean predicate `left = right` (the IN join key).
#[allow(dead_code)]
pub(super) fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{OutputColumn, ProjectItem};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{FilterNode, ProjectNode, ScanNode};

    const INNER_K: ColumnId = ColumnId(1);
    const OUTER_K: ColumnId = ColumnId(2);
    const CONST_ONE: ColumnId = ColumnId(3);
    const MISSING_INNER: ColumnId = ColumnId(4);

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: INNER_K,
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn typed_col_ref(id: ColumnId, name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable: false,
        }
    }

    fn predicate() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_K, "k")),
                op: BinOp::Eq,
                right: Box::new(col_ref(OUTER_K, "k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn typed_inner_left_predicate(inner_type: DataType, outer_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(typed_col_ref(INNER_K, "k", inner_type)),
                op: BinOp::Eq,
                right: Box::new(typed_col_ref(OUTER_K, "k", outer_type)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn missing_inner_predicate() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(MISSING_INNER, "missing_k")),
                op: BinOp::Eq,
                right: Box::new(col_ref(OUTER_K, "k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn project(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: vec![ProjectItem {
                expr: col_ref(INNER_K, "k"),
                output_name: "k".to_string(),
                output_column_id: INNER_K,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn project_literal(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "1".to_string(),
                output_column_id: CONST_ONE,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn filter(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate: predicate(),
            required_output_columns: None,
        })
    }

    fn filter_missing_inner(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate: missing_inner_predicate(),
            required_output_columns: None,
        })
    }

    fn filter_with_predicate(input: LogicalPlan, predicate: TypedExpr) -> LogicalPlan {
        LogicalPlan::Filter(FilterNode {
            input: Box::new(input),
            predicate,
            required_output_columns: None,
        })
    }

    #[test]
    fn lift_project_filter_returns_rel_and_pred() {
        let lifted =
            lift_correlated_inner(project(filter(scan())), &[OUTER_K]).expect("inner must lift");

        assert!(lifted.on_predicate.is_some());
        let LogicalPlan::Project(project) = lifted.right else {
            panic!("expected project");
        };
        assert!(matches!(*project.input, LogicalPlan::Scan(_)));
    }

    #[test]
    fn lift_project_filter_adds_missing_inner_predicate_columns() {
        let lifted = lift_correlated_inner(project_literal(filter(scan())), &[OUTER_K])
            .expect("inner must lift");

        let LogicalPlan::Project(project) = lifted.right else {
            panic!("expected project");
        };
        assert!(
            project
                .items
                .iter()
                .any(|item| item.output_column_id == INNER_K),
            "lifted right project must expose INNER_K for the join ON"
        );
    }

    #[test]
    fn lift_project_scan_returns_none() {
        assert!(lift_correlated_inner(project(scan()), &[OUTER_K]).is_none());
    }

    #[test]
    fn lift_bare_filter() {
        let lifted = lift_correlated_inner(filter(scan()), &[OUTER_K]).expect("inner must lift");

        assert!(lifted.on_predicate.is_some());
        assert!(matches!(lifted.right, LogicalPlan::Scan(_)));
    }

    #[test]
    fn lift_normalizes_correlation_predicate_and_casts_outer_key() {
        let inner_type = DataType::Utf8;
        let outer_type = DataType::Int64;
        let lifted = lift_correlated_inner(
            filter_with_predicate(
                scan(),
                typed_inner_left_predicate(inner_type.clone(), outer_type),
            ),
            &[OUTER_K],
        )
        .expect("inner must lift");
        let predicate = lifted
            .on_predicate
            .expect("lifted predicate must be present");
        let ExprKind::BinaryOp { left, op, right } = &predicate.kind else {
            panic!("expected binary predicate, got: {predicate:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        let ExprKind::Cast { expr, target } = &left.kind else {
            panic!("expected outer key cast, got: {left:?}");
        };
        assert_eq!(target, &inner_type);
        assert_column_ref(expr, OUTER_K);
        assert_column_ref(right, INNER_K);
    }

    #[test]
    fn lift_bare_filter_missing_inner_ref_returns_none() {
        assert!(lift_correlated_inner(filter_missing_inner(scan()), &[OUTER_K]).is_none());
    }

    #[test]
    fn lift_scan_returns_none() {
        assert!(lift_correlated_inner(scan(), &[OUTER_K]).is_none());
    }

    fn assert_column_ref(expr: &TypedExpr, expected: ColumnId) {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected ColumnRef, got: {expr:?}");
        };
        assert_eq!(*column_id, expected);
    }
}
