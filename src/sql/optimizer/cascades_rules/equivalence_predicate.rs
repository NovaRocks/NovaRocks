//! Inner join equivalence predicate propagation.

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::logical_props::{
    collect_column_equalities, collect_literal_equalities, combine_with_and,
    make_eq_literal_predicate,
};
use crate::sql::optimizer::memo::{GroupId, LogicalProperties, MExpr, Memo};
use crate::sql::optimizer::operator::{LogicalFilterOp, LogicalJoinOp, Operator};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use arrow::datatypes::DataType;
use std::collections::HashMap;

pub(crate) struct InnerJoinEquivalencePredicateRule;

impl Rule for InnerJoinEquivalencePredicateRule {
    fn name(&self) -> &str {
        "InnerJoinEquivalencePredicateRule"
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
        let mut literal_by_column = literal_equalities_from_join(join)
            .into_iter()
            .chain(literal_equalities_from_group(memo, left_group))
            .chain(literal_equalities_from_group(memo, right_group))
            .collect::<HashMap<ColumnId, TypedExpr>>();
        expand_literals_with_equivalence(&left_props, &mut literal_by_column);
        expand_literals_with_equivalence(&right_props, &mut literal_by_column);

        let join_literals = literal_equalities_from_join(join);
        let mut left_new = Vec::new();
        let mut right_new = Vec::new();
        for (raw_left, raw_right) in join_column_pairs(join) {
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
                    &literal,
                ) {
                    if let Some(column) = right_columns.get(&right_id) {
                        right_new.push(make_eq_literal_predicate(column, literal));
                    }
                }
            }
            if let Some(literal) = literal_by_column.get(&right_id).cloned() {
                if !has_literal_equality_in_side(
                    memo,
                    left_group,
                    &join_literals,
                    left_id,
                    &literal,
                ) {
                    if let Some(column) = left_columns.get(&left_id) {
                        left_new.push(make_eq_literal_predicate(column, literal));
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
    literal_by_column: &mut HashMap<ColumnId, TypedExpr>,
) {
    let mut additions = Vec::new();
    for class in props.equivalence_classes.classes() {
        let literal = class
            .iter()
            .find_map(|column_id| literal_by_column.get(&column_id).cloned());
        if let Some(literal) = literal {
            for column_id in class.iter() {
                additions.push((column_id, literal.clone()));
            }
        }
    }
    for (column_id, literal) in additions {
        literal_by_column.entry(column_id).or_insert(literal);
    }
}

fn join_column_pairs(join: &LogicalJoinOp) -> Vec<(ColumnId, ColumnId)> {
    join.condition
        .as_ref()
        .map(collect_column_equalities)
        .unwrap_or_default()
}

fn literal_equalities_from_join(join: &LogicalJoinOp) -> Vec<(ColumnId, TypedExpr)> {
    join.condition
        .as_ref()
        .map(|condition| {
            collect_literal_equalities(condition)
                .into_iter()
                .map(|eq| (eq.column_id, eq.literal))
                .collect()
        })
        .unwrap_or_default()
}

fn literal_equalities_from_group(memo: &Memo, group_id: GroupId) -> Vec<(ColumnId, TypedExpr)> {
    let mut out = Vec::new();
    let Some(group) = memo.groups.get(group_id) else {
        return out;
    };
    for expr in &group.logical_exprs {
        match &expr.op {
            Operator::LogicalFilter(filter) => {
                out.extend(
                    collect_literal_equalities(&filter.predicate)
                        .into_iter()
                        .map(|eq| (eq.column_id, eq.literal)),
                );
            }
            Operator::LogicalScan(scan) => {
                for predicate in &scan.predicates {
                    out.extend(
                        collect_literal_equalities(predicate)
                            .into_iter()
                            .map(|eq| (eq.column_id, eq.literal)),
                    );
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
    join_literals: &[(ColumnId, TypedExpr)],
    column_id: ColumnId,
    literal: &TypedExpr,
) -> bool {
    let signature = literal_signature(literal);
    if join_literals
        .iter()
        .any(|(existing_column, existing_literal)| {
            *existing_column == column_id && literal_signature(existing_literal) == signature
        })
    {
        return true;
    }
    literal_equalities_from_group(memo, group_id)
        .into_iter()
        .any(|(existing_column, existing_literal)| {
            existing_column == column_id && literal_signature(&existing_literal) == signature
        })
}

fn literal_signature(expr: &TypedExpr) -> String {
    format!("{:?}:{:?}", expr.data_type, expr.kind)
}

fn add_filter_group(memo: &mut Memo, child_group: GroupId, predicates: Vec<TypedExpr>) -> GroupId {
    let predicate =
        combine_with_and(predicates).expect("filter group needs at least one predicate");
    let filter_expr = MExpr {
        id: memo.next_expr_id(),
        op: Operator::LogicalFilter(LogicalFilterOp { predicate }),
        children: vec![child_group],
    };
    let new_group = memo.new_group(filter_expr);
    if let Some(child_props) = memo.groups[child_group].logical_props.as_ref() {
        let row_count = (child_props.row_count * 0.1).max(1.0);
        let output_columns = child_props.output_columns.clone();
        let props = crate::sql::optimizer::logical_props::derive_for_group(
            memo,
            new_group,
            output_columns,
            row_count,
        );
        memo.groups[new_group].logical_props = Some(props);
    }
    new_group
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::optimizer::operator::LogicalScanOp;
    use std::path::PathBuf;

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
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

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        let group = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    iceberg_table: None,
                    source: ScanSource::StarRocks,
                },
                alias: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
            }),
            children: Vec::new(),
        });
        memo.groups[group].logical_props =
            Some(LogicalProperties::new(vec![output(id, name)], 10.0));
        group
    }

    #[test]
    fn propagates_literal_from_left_to_right() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(and(
                    eq(col(1, "lk"), col(2, "rk")),
                    eq(col(1, "lk"), lit(10)),
                )),
            }),
            children: vec![left, right],
        };

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
        assert_eq!(collect_literal_equalities(&filter.predicate).len(), 1);
    }

    #[test]
    fn does_not_fire_for_left_outer_join() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::LeftOuter,
                condition: Some(and(
                    eq(col(1, "lk"), col(2, "rk")),
                    eq(col(1, "lk"), lit(10)),
                )),
            }),
            children: vec![left, right],
        };
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
        let right_filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(LogicalFilterOp {
                predicate: eq(col(2, "rk"), lit(10)),
            }),
            children: vec![right_scan],
        });
        memo.groups[right_filter].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 5.0));
        let join = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(and(
                    eq(col(1, "lk"), col(2, "rk")),
                    eq(col(1, "lk"), lit(10)),
                )),
            }),
            children: vec![left, right_filter],
        };
        assert!(
            InnerJoinEquivalencePredicateRule
                .apply(&join, &mut memo)
                .is_empty()
        );
    }
}
