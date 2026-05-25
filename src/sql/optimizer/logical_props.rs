//! Logical-property derivation for optimizer Memo groups.

use super::memo::{GroupId, LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use super::property::ColumnIdSet;
use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use arrow::datatypes::DataType;

pub(crate) fn derive_for_group(
    memo: &Memo,
    group_idx: GroupId,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
) -> LogicalProperties {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());
    let Some(expr) = expr else {
        return LogicalProperties::new(output_columns, row_count);
    };
    derive_for_expr(expr, memo, output_columns, row_count)
}

pub(crate) fn derive_for_expr(
    expr: &MExpr,
    memo: &Memo,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
) -> LogicalProperties {
    let output_ids = output_id_set(&output_columns);
    let mut props = LogicalProperties::new(output_columns, row_count);

    match &expr.op {
        Operator::LogicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_column_equalities(&filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::PhysicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_column_equalities(&filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::LogicalJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                if let Some(condition) = &join.condition {
                    for (left, right) in collect_column_equalities(condition) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::PhysicalHashJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                for eq in &join.eq_conditions {
                    if let (Some(left), Some(right)) = (
                        column_id_from_expr(&eq.left),
                        column_id_from_expr(&eq.right),
                    ) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
                if let Some(condition) = &join.other_condition {
                    for (left, right) in collect_column_equalities(condition) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::LogicalAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::PhysicalHashAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::LogicalProject(_)
        | Operator::PhysicalProject(_)
        | Operator::LogicalSubqueryAlias(_)
        | Operator::PhysicalSubqueryAlias(_)
        | Operator::LogicalSort(_)
        | Operator::PhysicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::PhysicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::PhysicalTopN(_)
        | Operator::LogicalWindow(_)
        | Operator::PhysicalWindow(_)
        | Operator::LogicalTableFunction(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalDistribution(_) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
        }
        _ => {}
    }

    props.equivalence_classes.retain_subset_of(&output_ids);
    props
        .unique_columns
        .retain(|key| !key.is_empty() && key.is_subset(&output_ids));
    props
}

fn inherit_from_child(
    memo: &Memo,
    expr: &MExpr,
    child_slot: usize,
    output_ids: &ColumnIdSet,
    props: &mut LogicalProperties,
) {
    let Some(child_group_id) = expr.children.get(child_slot).copied() else {
        return;
    };
    let Some(child_props) = memo.groups[child_group_id].logical_props.as_ref() else {
        return;
    };
    props
        .equivalence_classes
        .extend_from(&child_props.equivalence_classes);
    for key in &child_props.unique_columns {
        if key.is_subset(output_ids) {
            props.unique_columns.push(key.clone());
        }
    }
    props.equivalence_classes.retain_subset_of(output_ids);
}

fn output_id_set(output_columns: &[OutputColumn]) -> ColumnIdSet {
    ColumnIdSet::from_columns(output_columns.iter().map(|column| column.column_id))
}

pub(crate) fn column_id_from_expr(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        ExprKind::Nested(inner) => column_id_from_expr(inner),
        _ => None,
    }
}

pub(crate) fn collect_column_equalities(expr: &TypedExpr) -> Vec<(ColumnId, ColumnId)> {
    let mut out = Vec::new();
    collect_column_equalities_inner(expr, &mut out);
    out
}

fn collect_column_equalities_inner(expr: &TypedExpr, out: &mut Vec<(ColumnId, ColumnId)>) {
    match &expr.kind {
        ExprKind::Nested(inner) => collect_column_equalities_inner(inner, out),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_column_equalities_inner(left, out);
            collect_column_equalities_inner(right, out);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::EqForNull,
            right,
        } => {
            if let (Some(left_id), Some(right_id)) =
                (column_id_from_expr(left), column_id_from_expr(right))
            {
                out.push((left_id, right_id));
            }
        }
        _ => {}
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LiteralEquality {
    pub(crate) column_id: ColumnId,
    pub(crate) literal: TypedExpr,
}

pub(crate) fn collect_literal_equalities(expr: &TypedExpr) -> Vec<LiteralEquality> {
    let mut out = Vec::new();
    collect_literal_equalities_inner(expr, &mut out);
    out
}

fn collect_literal_equalities_inner(expr: &TypedExpr, out: &mut Vec<LiteralEquality>) {
    match &expr.kind {
        ExprKind::Nested(inner) => collect_literal_equalities_inner(inner, out),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_literal_equalities_inner(left, out);
            collect_literal_equalities_inner(right, out);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (&left.kind, &right.kind) {
            (ExprKind::ColumnRef { column_id, .. }, ExprKind::Literal(_))
                if *column_id != ColumnId::UNSET =>
            {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: right.as_ref().clone(),
                });
            }
            (ExprKind::Literal(_), ExprKind::ColumnRef { column_id, .. })
                if *column_id != ColumnId::UNSET =>
            {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: left.as_ref().clone(),
                });
            }
            _ => {}
        },
        _ => {}
    }
}

pub(crate) fn make_column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

pub(crate) fn make_eq_literal_predicate(column: &OutputColumn, literal: TypedExpr) -> TypedExpr {
    TypedExpr {
        nullable: column.nullable || literal.nullable,
        data_type: DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(make_column_ref_expr(column)),
            op: BinOp::Eq,
            right: Box::new(literal),
        },
    }
}

pub(crate) fn combine_with_and(mut predicates: Vec<TypedExpr>) -> Option<TypedExpr> {
    let first = predicates.drain(..1).next()?;
    Some(predicates.into_iter().fold(first, |left, right| TypedExpr {
        nullable: left.nullable || right.nullable,
        data_type: DataType::Boolean,
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op: BinOp::And,
            right: Box::new(right),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::LiteralValue;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::operator::{LogicalFilterOp, LogicalJoinOp, LogicalScanOp};
    use std::path::PathBuf;

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
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

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    storage: TableStorage::ManagedLake,
                },
                alias: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
            }),
            children: Vec::new(),
        })
    }

    #[test]
    fn collect_column_equalities_reads_top_level_and() {
        let predicate = and(eq(col(1, "a"), col(2, "b")), eq(col(1, "a"), lit(10)));
        assert_eq!(
            collect_column_equalities(&predicate),
            vec![(ColumnId(1), ColumnId(2))]
        );
        assert_eq!(collect_literal_equalities(&predicate).len(), 1);
    }

    #[test]
    fn filter_derivation_merges_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp {
                predicate: eq(col(1, "a"), col(2, "b")),
            }),
            children: vec![child],
        });
        let props = derive_for_group(&memo, filter, vec![output(1, "a"), output(2, "b")], 50.0);
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .expect("filter equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    #[test]
    fn inner_join_derivation_merges_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(eq(col(1, "lk"), col(2, "rk"))),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(&memo, join, vec![output(1, "lk"), output(2, "rk")], 10.0);
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(2))
            .expect("join equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    #[test]
    fn left_join_derivation_does_not_merge_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(eq(col(1, "lk"), col(2, "rk"))),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(&memo, join, vec![output(1, "lk"), output(2, "rk")], 10.0);
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none()
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(2))
                .is_none()
        );
    }
}
