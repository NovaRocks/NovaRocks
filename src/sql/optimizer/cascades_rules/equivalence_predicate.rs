//! Inner join equivalence predicate propagation.

use crate::sql::column_id::ColumnId;
use crate::sql::common::{JoinKind, OutputColumn};
use crate::sql::optimizer::logical_props::{
    collect_literal_equalities, collect_strict_column_equalities, combine_with_and,
    literal_signature, make_eq_literal_predicate,
};
use crate::sql::optimizer::memo::{GroupId, LogicalProperties, MExpr, Memo};
use crate::sql::optimizer::operator::{FilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::scalar::ScalarId;
use std::collections::{HashMap, HashSet};

pub(crate) struct InnerJoinEquivalencePredicateRule;

impl Rule for InnerJoinEquivalencePredicateRule {
    fn name(&self) -> &str {
        "JoinPredicateMoveAround"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(
            op,
            Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                ..
            })
        )
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(join) = &expr.op else {
            return Vec::new();
        };
        if join.join_type != JoinKind::Inner || expr.children.len() != 2 {
            return Vec::new();
        }

        let left_group = expr.children[0];
        let right_group = expr.children[1];
        let Some(left_props) = memo.groups[left_group].logical_props.clone() else {
            return Vec::new();
        };
        let Some(right_props) = memo.groups[right_group].logical_props.clone() else {
            return Vec::new();
        };

        let left_columns = columns_by_id(&left_props.output_columns);
        let right_columns = columns_by_id(&right_props.output_columns);
        let mut literal_by_column = literal_equalities_from_join(memo, join)
            .into_iter()
            .chain(literal_equalities_from_group(memo, left_group))
            .chain(literal_equalities_from_group(memo, right_group))
            .collect::<HashMap<ColumnId, ScalarId>>();
        expand_literals_with_equivalence(&left_props, &mut literal_by_column);
        expand_literals_with_equivalence(&right_props, &mut literal_by_column);

        let join_literals = literal_equalities_from_join(memo, join);
        let mut left_new = Vec::new();
        let mut right_new = Vec::new();
        for (raw_left, raw_right) in join_column_pairs(memo, join) {
            let Some((left_id, right_id)) =
                orient_pair(raw_left, raw_right, &left_columns, &right_columns)
            else {
                continue;
            };

            if let Some(literal) = literal_by_column.get(&left_id).cloned() {
                if !has_literal_equality_in_side(
                    memo,
                    right_group,
                    &join_literals,
                    right_id,
                    literal,
                ) {
                    if let Some(column) = right_columns.get(&right_id) {
                        right_new.push(make_eq_literal_predicate(
                            &mut memo.scalars,
                            column,
                            literal,
                        ));
                    }
                }
            }
            if let Some(literal) = literal_by_column.get(&right_id).cloned() {
                if !has_literal_equality_in_side(memo, left_group, &join_literals, left_id, literal)
                {
                    if let Some(column) = left_columns.get(&left_id) {
                        left_new.push(make_eq_literal_predicate(
                            &mut memo.scalars,
                            column,
                            literal,
                        ));
                    }
                }
            }
        }

        if left_new.is_empty() && right_new.is_empty() {
            return Vec::new();
        }

        let new_left = if left_new.is_empty() {
            left_group
        } else {
            add_filter_group(memo, left_group, left_new)
        };
        let new_right = if right_new.is_empty() {
            right_group
        } else {
            add_filter_group(memo, right_group, right_new)
        };

        vec![NewExpr {
            op: Operator::LogicalJoin(join.clone()),
            children: vec![new_left, new_right],
        }]
    }
}

fn columns_by_id(columns: &[OutputColumn]) -> HashMap<ColumnId, OutputColumn> {
    columns
        .iter()
        .filter(|column| column.column_id != ColumnId::UNSET)
        .map(|column| (column.column_id, column.clone()))
        .collect()
}

fn expand_literals_with_equivalence(
    props: &LogicalProperties,
    literal_by_column: &mut HashMap<ColumnId, ScalarId>,
) {
    let mut additions = Vec::new();
    for class in props.equivalence_classes.classes() {
        let literal = class
            .iter()
            .find_map(|column_id| literal_by_column.get(&column_id).copied());
        if let Some(literal) = literal {
            for column_id in class.iter() {
                additions.push((column_id, literal));
            }
        }
    }
    for (column_id, literal) in additions {
        literal_by_column.entry(column_id).or_insert(literal);
    }
}

fn join_column_pairs(memo: &Memo, join: &LogicalJoinOp) -> Vec<(ColumnId, ColumnId)> {
    join.condition
        .as_ref()
        .map(|condition| collect_strict_column_equalities(&memo.scalars, *condition))
        .unwrap_or_default()
}

fn literal_equalities_from_join(memo: &Memo, join: &LogicalJoinOp) -> Vec<(ColumnId, ScalarId)> {
    join.condition
        .as_ref()
        .map(|condition| collect_literal_equalities(&memo.scalars, *condition))
        .map(|equalities| {
            equalities
                .into_iter()
                .map(|eq| (eq.column_id, eq.literal))
                .collect()
        })
        .unwrap_or_default()
}

fn literal_equalities_from_group(memo: &Memo, group_id: GroupId) -> Vec<(ColumnId, ScalarId)> {
    let mut visited = HashSet::new();
    literal_equalities_from_group_inner(memo, group_id, &mut visited)
}

fn literal_equalities_from_group_inner(
    memo: &Memo,
    group_id: GroupId,
    visited: &mut HashSet<GroupId>,
) -> Vec<(ColumnId, ScalarId)> {
    if !visited.insert(group_id) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let Some(group) = memo.groups.get(group_id) else {
        return out;
    };
    for expr in &group.logical_exprs {
        match &expr.op {
            Operator::LogicalFilter(filter) => {
                out.extend(
                    collect_literal_equalities(&memo.scalars, filter.predicate)
                        .into_iter()
                        .map(|eq| (eq.column_id, eq.literal)),
                );
                for child in &expr.children {
                    out.extend(literal_equalities_from_group_inner(memo, *child, visited));
                }
            }
            Operator::LogicalScan(scan) => {
                for predicate in &scan.predicates {
                    out.extend(
                        collect_literal_equalities(&memo.scalars, *predicate)
                            .into_iter()
                            .map(|eq| (eq.column_id, eq.literal)),
                    );
                }
            }
            Operator::LogicalJoin(join)
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) =>
            {
                if let Some(condition) = &join.condition {
                    out.extend(
                        collect_literal_equalities(&memo.scalars, *condition)
                            .into_iter()
                            .map(|eq| (eq.column_id, eq.literal)),
                    );
                }
                for child in &expr.children {
                    out.extend(literal_equalities_from_group_inner(memo, *child, visited));
                }
            }
            _ => {}
        }
    }
    out
}

fn orient_pair(
    left: ColumnId,
    right: ColumnId,
    left_columns: &HashMap<ColumnId, OutputColumn>,
    right_columns: &HashMap<ColumnId, OutputColumn>,
) -> Option<(ColumnId, ColumnId)> {
    if left_columns.contains_key(&left) && right_columns.contains_key(&right) {
        Some((left, right))
    } else if left_columns.contains_key(&right) && right_columns.contains_key(&left) {
        Some((right, left))
    } else {
        None
    }
}

fn has_literal_equality_in_side(
    memo: &Memo,
    group_id: GroupId,
    join_literals: &[(ColumnId, ScalarId)],
    column_id: ColumnId,
    literal: ScalarId,
) -> bool {
    let signature = literal_signature(&memo.scalars, literal);
    if join_literals
        .iter()
        .any(|(existing_column, existing_literal)| {
            *existing_column == column_id
                && literal_signature(&memo.scalars, *existing_literal) == signature
        })
    {
        return true;
    }
    let props = memo
        .groups
        .get(group_id)
        .and_then(|group| group.logical_props.as_ref());
    literal_equalities_from_group(memo, group_id)
        .into_iter()
        .any(|(existing_column, existing_literal)| {
            let same_or_equivalent = existing_column == column_id
                || props
                    .and_then(|props| props.equivalence_classes.class_containing(column_id))
                    .is_some_and(|class| class.contains(existing_column));
            same_or_equivalent && literal_signature(&memo.scalars, existing_literal) == signature
        })
}

fn add_filter_group(memo: &mut Memo, child_group: GroupId, predicates: Vec<ScalarId>) -> GroupId {
    let predicate = combine_with_and(&mut memo.scalars, predicates)
        .expect("filter group needs at least one predicate");
    let filter_expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalFilter(FilterOp { predicate }),
        children: vec![child_group],
    };
    let new_group = memo.new_group(filter_expr);
    if let Some(child_props) = memo.groups[child_group].logical_props.as_ref() {
        let row_count = (child_props.row_count * 0.1).max(1.0);
        let output_columns = child_props.output_columns.clone();
        let column_statistics = child_props.column_statistics.clone();
        let props = crate::sql::optimizer::logical_props::derive_for_group(
            memo,
            new_group,
            output_columns,
            row_count,
            crate::sql::optimizer::statistics::Confidence::Estimated,
            column_statistics,
        );
        memo.groups[new_group].logical_props = Some(props);
    }
    new_group
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::optimizer::operator::ScanOp;
    use crate::sql::optimizer::scalar::ScalarId;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

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

    fn intern(memo: &mut Memo, expr: &TypedExpr) -> ScalarId {
        intern_typed(&mut memo.scalars, expr)
    }

    fn join_mexpr(
        memo: &mut Memo,
        join_type: JoinKind,
        condition: TypedExpr,
        children: Vec<GroupId>,
    ) -> MExpr {
        let id = memo.next_expr_id();
        let condition = Some(intern(memo, &condition));
        MExpr {
            id,
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type,
                condition,
            }),
            children,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        scan_group_with_predicates(memo, id, name, Vec::new())
    }

    fn scan_group_with_predicates(
        memo: &mut Memo,
        id: u32,
        name: &str,
        predicates: Vec<TypedExpr>,
    ) -> GroupId {
        let id_expr = memo.next_expr_id();
        let predicates = predicates.iter().map(|expr| intern(memo, expr)).collect();
        let group = memo.new_group(MExpr {
            id: id_expr,
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
                predicates,
                required_columns: None,
                dict_columns: Vec::new(),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
        });
        memo.groups[group].logical_props =
            Some(LogicalProperties::new(vec![output(id, name)], 10.0));
        group
    }

    fn inner_join_group(
        memo: &mut Memo,
        left: GroupId,
        right: GroupId,
        condition: TypedExpr,
        outputs: Vec<OutputColumn>,
    ) -> GroupId {
        let id = memo.next_expr_id();
        let condition_id = intern(memo, &condition);
        let group = memo.new_group(MExpr {
            id,
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition_id),
            }),
            children: vec![left, right],
        });
        let mut props = LogicalProperties::new(outputs, 10.0);
        for (left, right) in collect_strict_column_equalities(&memo.scalars, condition_id) {
            props.equivalence_classes.merge_pair(left, right);
        }
        memo.groups[group].logical_props = Some(props);
        group
    }

    #[test]
    fn propagates_literal_from_left_to_right() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = join_mexpr(
            &mut memo,
            JoinKind::Inner,
            and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10))),
            vec![left, right],
        );

        let out = InnerJoinEquivalencePredicateRule.apply(&join, &mut memo);
        assert_eq!(out.len(), 1);
        let new_right = out[0].children[1];
        let filter = memo.groups[new_right]
            .logical_exprs
            .iter()
            .find_map(|expr| match &expr.op {
                Operator::LogicalFilter(filter) => Some(filter),
                _ => None,
            })
            .expect("right child filter");
        assert_eq!(
            collect_literal_equalities(&memo.scalars, filter.predicate).len(),
            1
        );
    }

    #[test]
    fn does_not_propagate_literal_across_null_safe_join_pair() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = join_mexpr(
            &mut memo,
            JoinKind::Inner,
            and(
                eq_for_null(col(1, "lk"), col(2, "rk")),
                eq(col(1, "lk"), lit(10)),
            ),
            vec![left, right],
        );

        assert!(
            InnerJoinEquivalencePredicateRule
                .apply(&join, &mut memo)
                .is_empty(),
            "strict-only pass must not use a null-safe join pair for literal propagation"
        );
    }

    #[test]
    fn does_not_fire_for_left_outer_join() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = join_mexpr(
            &mut memo,
            JoinKind::LeftOuter,
            and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10))),
            vec![left, right],
        );
        assert!(!InnerJoinEquivalencePredicateRule.matches(&join.op));
        assert!(
            InnerJoinEquivalencePredicateRule
                .apply(&join, &mut memo)
                .is_empty()
        );
    }

    #[test]
    fn does_not_duplicate_existing_target_predicate() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right_scan = scan_group(&mut memo, 2, "rk");
        let right_filter_id = memo.next_expr_id();
        let right_filter_predicate = intern(&mut memo, &eq(col(2, "rk"), lit(10)));
        let right_filter = memo.new_group(MExpr {
            id: right_filter_id,
            op: Operator::LogicalFilter(FilterOp {
                predicate: right_filter_predicate,
            }),
            children: vec![right_scan],
        });
        memo.groups[right_filter].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 5.0));
        let join = join_mexpr(
            &mut memo,
            JoinKind::Inner,
            and(eq(col(1, "lk"), col(2, "rk")), eq(col(1, "lk"), lit(10))),
            vec![left, right_filter],
        );
        assert!(
            InnerJoinEquivalencePredicateRule
                .apply(&join, &mut memo)
                .is_empty()
        );
    }

    #[test]
    fn does_not_duplicate_literal_already_present_below_inner_join_side() {
        let mut memo = Memo::new();
        let left_a = scan_group_with_predicates(&mut memo, 1, "ak", vec![eq(col(1, "ak"), lit(7))]);
        let left_b = scan_group_with_predicates(&mut memo, 2, "bk", vec![eq(col(2, "bk"), lit(7))]);
        let left_join = inner_join_group(
            &mut memo,
            left_a,
            left_b,
            eq(col(1, "ak"), col(2, "bk")),
            vec![output(1, "ak"), output(2, "bk")],
        );
        let right = scan_group_with_predicates(&mut memo, 3, "ck", vec![eq(col(3, "ck"), lit(7))]);
        let join = join_mexpr(
            &mut memo,
            JoinKind::Inner,
            eq(col(2, "bk"), col(3, "ck")),
            vec![left_join, right],
        );

        assert!(
            InnerJoinEquivalencePredicateRule
                .apply(&join, &mut memo)
                .is_empty()
        );
    }

    #[test]
    fn add_filter_group_propagates_column_statistics() {
        use crate::sql::optimizer::statistics::ColumnStatistic;

        let mut memo = Memo::new();
        // Build a scan group with non-empty column_statistics in its logical_props.
        let child = scan_group(&mut memo, 1, "a");
        let mut child_props = LogicalProperties::new(vec![output(1, "a")], 100.0);
        child_props.column_statistics.insert(
            ColumnId::new_for_test(1),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 99.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: 50.0,
                ..Default::default()
            },
        );
        memo.groups[child].logical_props = Some(child_props);

        // Call add_filter_group to synthesize a filter group above the scan.
        let predicate = intern(&mut memo, &eq(col(1, "a"), lit(42)));
        let filter_group = add_filter_group(&mut memo, child, vec![predicate]);

        // The filter group's logical_props must carry the child's column stats.
        let filter_props = memo.groups[filter_group]
            .logical_props
            .as_ref()
            .expect("filter group must have logical_props");
        assert!(
            !filter_props.column_statistics.is_empty(),
            "column_statistics must not be empty after add_filter_group"
        );
        assert!(
            filter_props
                .column_statistics
                .contains_key(&ColumnId::new_for_test(1)),
            "column_statistics must contain the child column 'a'"
        );
    }
}
