//! Logical-property derivation for optimizer Memo groups.

use std::collections::HashMap;

use super::memo::{GroupId, LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use super::property::ColumnIdSet;
use super::statistics::{ColumnStatistic, Confidence};
use crate::sql::column_id::ColumnId;
use crate::sql::common::{BinOp, JoinKind, OutputColumn};
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use arrow::datatypes::DataType;

pub(crate) fn derive_for_group(
    memo: &Memo,
    group_idx: GroupId,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    row_count_confidence: Confidence,
    column_statistics: HashMap<ColumnId, ColumnStatistic>,
) -> LogicalProperties {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());
    let Some(expr) = expr else {
        let mut props = LogicalProperties::new(output_columns, row_count);
        props.row_count_confidence = row_count_confidence;
        props.column_statistics = column_statistics;
        return props;
    };
    derive_for_expr(
        expr,
        memo,
        output_columns,
        row_count,
        row_count_confidence,
        column_statistics,
    )
}

pub(crate) fn derive_for_expr(
    expr: &MExpr,
    memo: &Memo,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    row_count_confidence: Confidence,
    column_statistics: HashMap<ColumnId, ColumnStatistic>,
) -> LogicalProperties {
    let output_ids = output_id_set(&output_columns);
    let mut props = LogicalProperties::new(output_columns, row_count);
    props.row_count_confidence = row_count_confidence;
    props.column_statistics = column_statistics;

    match &expr.op {
        Operator::LogicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_strict_column_equalities(&memo.scalars, filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::PhysicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_strict_column_equalities(&memo.scalars, filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::LogicalJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                if let Some(condition) = &join.condition {
                    for (left, right) in collect_strict_column_equalities(&memo.scalars, *condition)
                    {
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
                    if eq.null_safe {
                        continue;
                    }
                    if let (Some(left), Some(right)) = (
                        column_id_from_scalar(&memo.scalars, eq.left),
                        column_id_from_scalar(&memo.scalars, eq.right),
                    ) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
                if let Some(condition) = &join.other_condition {
                    for (left, right) in collect_strict_column_equalities(&memo.scalars, *condition)
                    {
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
        | Operator::PhysicalDistribution(_)
        | Operator::LogicalAssertOneRow(_)
        | Operator::PhysicalAssertOneRow(_) => {
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

pub(crate) fn column_id_from_scalar(scalars: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match scalars.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        ScalarNode::Nested(inner) => column_id_from_scalar(scalars, *inner),
        _ => None,
    }
}

pub(crate) fn collect_strict_column_equalities(
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Vec<(ColumnId, ColumnId)> {
    let mut out = Vec::new();
    collect_strict_column_equalities_inner(scalars, expr, &mut out);
    out
}

fn collect_strict_column_equalities_inner(
    scalars: &ScalarArena,
    expr: ScalarId,
    out: &mut Vec<(ColumnId, ColumnId)>,
) {
    match scalars.node(expr) {
        ScalarNode::Nested(inner) => collect_strict_column_equalities_inner(scalars, *inner, out),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_strict_column_equalities_inner(scalars, *left, out);
            collect_strict_column_equalities_inner(scalars, *right, out);
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if let (Some(left_id), Some(right_id)) = (
                column_id_from_scalar(scalars, *left),
                column_id_from_scalar(scalars, *right),
            ) {
                out.push((left_id, right_id));
            }
        }
        _ => {}
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LiteralEquality {
    pub(crate) column_id: ColumnId,
    pub(crate) literal: ScalarId,
}

pub(crate) fn collect_literal_equalities(
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Vec<LiteralEquality> {
    let mut out = Vec::new();
    collect_literal_equalities_inner(scalars, expr, &mut out);
    out
}

fn collect_literal_equalities_inner(
    scalars: &ScalarArena,
    expr: ScalarId,
    out: &mut Vec<LiteralEquality>,
) {
    match scalars.node(expr) {
        ScalarNode::Nested(inner) => collect_literal_equalities_inner(scalars, *inner, out),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_literal_equalities_inner(scalars, *left, out);
            collect_literal_equalities_inner(scalars, *right, out);
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (scalars.node(*left), scalars.node(*right)) {
            (ScalarNode::ColumnRef(column_id), ScalarNode::Literal(_))
                if *column_id != ColumnId::UNSET =>
            {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: *right,
                });
            }
            (ScalarNode::Literal(_), ScalarNode::ColumnRef(column_id))
                if *column_id != ColumnId::UNSET =>
            {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: *left,
                });
            }
            _ => {}
        },
        _ => {}
    }
}

pub(crate) fn make_column_ref_expr(arena: &mut ScalarArena, column: &OutputColumn) -> ScalarId {
    arena.remember_source_column_display(column.column_id, None, column.name.clone());
    arena.intern(
        ScalarNode::ColumnRef(column.column_id),
        column.data_type.clone(),
        column.nullable,
    )
}

pub(crate) fn make_eq_literal_predicate(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    literal: ScalarId,
) -> ScalarId {
    let left = make_column_ref_expr(arena, column);
    arena.intern(
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right: literal,
        },
        DataType::Boolean,
        column.nullable || arena.nullable(literal),
    )
}

pub(crate) fn combine_with_and(
    arena: &mut ScalarArena,
    mut predicates: Vec<ScalarId>,
) -> Option<ScalarId> {
    let first = predicates.drain(..1).next()?;
    Some(predicates.into_iter().fold(first, |left, right| {
        arena.intern(
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                right,
            },
            DataType::Boolean,
            arena.nullable(left) || arena.nullable(right),
        )
    }))
}

pub(crate) fn literal_signature(arena: &ScalarArena, literal: ScalarId) -> String {
    match arena.node(literal) {
        ScalarNode::Literal(HashableLiteral(value)) => {
            format!("{:?}:{:?}", arena.data_type(literal), value)
        }
        other => format!("{:?}:{:?}", arena.data_type(literal), other),
    }
}

#[cfg(test)]
pub(crate) fn make_eq_literal_predicate_for_test(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    literal: crate::sql::analysis::TypedExpr,
) -> crate::sql::analysis::TypedExpr {
    let literal = crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, &literal);
    let predicate = make_eq_literal_predicate(arena, column, literal);
    crate::sql::planner::optimizer_bridge::scalar::materialize(arena, predicate)
}

#[cfg(test)]
pub(crate) fn combine_with_and_for_test(
    arena: &mut ScalarArena,
    predicates: Vec<crate::sql::analysis::TypedExpr>,
) -> Option<crate::sql::analysis::TypedExpr> {
    let predicates = predicates
        .iter()
        .map(|predicate| {
            crate::sql::planner::optimizer_bridge::scalar::intern_typed(arena, predicate)
        })
        .collect();
    combine_with_and(arena, predicates).map(|predicate| {
        crate::sql::planner::optimizer_bridge::scalar::materialize(arena, predicate)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::optimizer::memo::MExpr;
    use crate::sql::optimizer::operator::{
        FilterOp, JoinDistribution, LogicalJoinOp, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        ScanOp,
    };
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use crate::sql::planner::plan::*;
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

    fn eq_for_null(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::EqForNull,
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
            is_internal: false,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
                dict_columns: Vec::new(),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
        })
    }

    #[test]
    fn collect_strict_column_equalities_reads_top_level_and() {
        let predicate = and(eq(col(1, "a"), col(2, "b")), eq(col(1, "a"), lit(10)));
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(&mut arena, &predicate);
        assert_eq!(
            collect_strict_column_equalities(&arena, predicate),
            vec![(ColumnId(1), ColumnId(2))]
        );
        assert_eq!(collect_literal_equalities(&arena, predicate).len(), 1);
    }

    #[test]
    fn collect_strict_column_equalities_ignores_null_safe_equality() {
        let predicate = and(
            eq(col(1, "a"), col(2, "b")),
            eq_for_null(col(3, "c"), col(4, "d")),
        );
        let mut arena = ScalarArena::new();
        let predicate = intern_typed(&mut arena, &predicate);
        assert_eq!(
            collect_strict_column_equalities(&arena, predicate),
            vec![(ColumnId(1), ColumnId(2))]
        );
    }

    #[test]
    fn filter_derivation_merges_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let predicate = intern_typed(&mut memo.scalars, &eq(col(1, "a"), col(2, "b")));
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp { predicate }),
            children: vec![child],
        });
        let props = derive_for_group(
            &memo,
            filter,
            vec![output(1, "a"), output(2, "b")],
            50.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
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
    fn filter_derivation_ignores_null_safe_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let predicate = intern_typed(&mut memo.scalars, &eq_for_null(col(1, "a"), col(2, "b")));
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp { predicate }),
            children: vec![child],
        });
        let props = derive_for_group(
            &memo,
            filter,
            vec![output(1, "a"), output(2, "b")],
            50.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none(),
            "null-safe equality must not populate the strict equivalence store"
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
        let condition = intern_typed(&mut memo.scalars, &eq(col(1, "lk"), col(2, "rk")));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
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
        let condition = intern_typed(&mut memo.scalars, &eq(col(1, "lk"), col(2, "rk")));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(condition),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .is_none());
        assert!(props
            .equivalence_classes
            .class_containing(ColumnId(2))
            .is_none());
    }

    #[test]
    fn physical_hash_join_derivation_skips_null_safe_hash_key() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let left_key = intern_typed(&mut memo.scalars, &col(1, "lk"));
        let right_key = intern_typed(&mut memo.scalars, &col(2, "rk"));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: true,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none(),
            "null-safe hash join key must not populate the strict equivalence store"
        );
    }

    #[test]
    fn physical_hash_join_derivation_keeps_strict_hash_key() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let left_key = intern_typed(&mut memo.scalars, &col(1, "lk"));
        let right_key = intern_typed(&mut memo.scalars, &col(2, "rk"));
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .expect("strict hash join key equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }
}
