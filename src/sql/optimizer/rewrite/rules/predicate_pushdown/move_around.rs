use std::collections::HashSet;

use crate::sql::analysis::JoinKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::deriver::derive_inner_join_predicates;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateGroup, PredicateKey, PredicateOrigin, predicate_key, split_and_refs,
};
use crate::sql::optimizer::rewrite::rules::utils::{collect_output_ids, combine_and};
use crate::sql::planner::plan::*;

pub(crate) struct JoinPredicateMoveAround;

impl PlanRewriteRule for JoinPredicateMoveAround {
    fn name(&self) -> &'static str {
        "JoinPredicateMoveAround"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Join(join)
                if matches!(join.join_type, JoinKind::Inner | JoinKind::Cross)
                    && join.condition.is_some()
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        if !matches!(join.join_type, JoinKind::Inner | JoinKind::Cross) {
            return None;
        }

        let condition = join.condition.clone()?;
        let left_ids = collect_output_ids(&join.left);
        let right_ids = collect_output_ids(&join.right);
        let join_groups = PredicateGroup::from_predicate(condition, PredicateOrigin::JoinCondition);
        let mut child_groups = Vec::new();
        collect_child_predicate_groups(&join.left, &mut child_groups);
        collect_child_predicate_groups(&join.right, &mut child_groups);

        let derived =
            derive_inner_join_predicates(&left_ids, &right_ids, &join_groups, &child_groups);
        let left_existing = existing_child_predicate_keys(&join.left);
        let right_existing = existing_child_predicate_keys(&join.right);
        let mut left_fresh = Vec::new();
        let mut right_fresh = Vec::new();

        for group in derived {
            match classify_group_side(&group, &left_ids, &right_ids) {
                Some(ChildSide::Left) if !left_existing.contains(&group.key) => {
                    left_fresh.push(group.expr);
                }
                Some(ChildSide::Right) if !right_existing.contains(&group.key) => {
                    right_fresh.push(group.expr);
                }
                _ => {}
            }
        }

        if left_fresh.is_empty() && right_fresh.is_empty() {
            return None;
        }

        let new_left = if left_fresh.is_empty() {
            *join.left
        } else {
            LogicalPlan::Filter(FilterNode {
                input: join.left,
                predicate: combine_and(left_fresh),
                required_output_columns: None,
            })
        };
        let new_right = if right_fresh.is_empty() {
            *join.right
        } else {
            LogicalPlan::Filter(FilterNode {
                input: join.right,
                predicate: combine_and(right_fresh),
                required_output_columns: None,
            })
        };

        Some(LogicalPlan::Join(JoinNode {
            left: Box::new(new_left),
            right: Box::new(new_right),
            join_type: join.join_type,
            condition: join.condition,
            required_output_columns: join.required_output_columns,
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChildSide {
    Left,
    Right,
}

fn classify_group_side(
    group: &PredicateGroup,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<ChildSide> {
    if group.referenced_ids.is_empty() {
        return None;
    }

    let all_left = group.referenced_ids.iter().all(|id| left_ids.contains(id));
    let all_right = group.referenced_ids.iter().all(|id| right_ids.contains(id));
    match (all_left, all_right) {
        (true, false) => Some(ChildSide::Left),
        (false, true) => Some(ChildSide::Right),
        _ => None,
    }
}

fn collect_child_predicate_groups(plan: &LogicalPlan, out: &mut Vec<PredicateGroup>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            out.extend(PredicateGroup::from_predicate(
                filter.predicate.clone(),
                PredicateOrigin::Filter,
            ));
            collect_child_predicate_groups(&filter.input, out);
        }
        LogicalPlan::Scan(scan) => {
            for predicate in &scan.predicates {
                out.extend(PredicateGroup::from_predicate(
                    predicate.clone(),
                    PredicateOrigin::Filter,
                ));
            }
        }
        LogicalPlan::Project(project) => collect_child_predicate_groups(&project.input, out),
        LogicalPlan::Sort(sort) => collect_child_predicate_groups(&sort.input, out),
        LogicalPlan::Limit(limit) => collect_child_predicate_groups(&limit.input, out),
        _ => {}
    }
}

fn existing_child_predicate_keys(plan: &LogicalPlan) -> HashSet<PredicateKey> {
    let mut keys = HashSet::new();
    collect_existing_child_predicate_keys(plan, &mut keys);
    keys
}

fn collect_existing_child_predicate_keys(plan: &LogicalPlan, out: &mut HashSet<PredicateKey>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            collect_top_level_conjunct_keys(&filter.predicate, out);
            collect_existing_child_predicate_keys(&filter.input, out);
        }
        LogicalPlan::Scan(scan) => {
            for predicate in &scan.predicates {
                collect_top_level_conjunct_keys(predicate, out);
            }
        }
        LogicalPlan::Project(project) => collect_existing_child_predicate_keys(&project.input, out),
        LogicalPlan::Sort(sort) => collect_existing_child_predicate_keys(&sort.input, out),
        LogicalPlan::Limit(limit) => collect_existing_child_predicate_keys(&limit.input, out),
        LogicalPlan::Join(join) => {
            if let Some(condition) = &join.condition {
                collect_top_level_conjunct_keys(condition, out);
            }
        }
        _ => {}
    }
}

fn collect_top_level_conjunct_keys(
    expr: &crate::sql::analysis::TypedExpr,
    out: &mut HashSet<PredicateKey>,
) {
    for conjunct in split_and_refs(expr) {
        out.insert(predicate_key(conjunct));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
    use arrow::datatypes::DataType;

    fn scan(alias: &str, cols: &[(&str, u32)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: alias.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: Some(alias.to_string()),
            columns: cols
                .iter()
                .map(|(name, id)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
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
            nullable: true,
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
            nullable: true,
        }
    }

    #[test]
    fn derives_opposite_side_filter_from_child_filter_and_join_equality() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(scan("r", &[("b", 2)])),
            join_type: JoinKind::Inner,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        let out = JoinPredicateMoveAround
            .apply(plan)
            .expect("move-around should derive right filter");
        let LogicalPlan::Join(join) = out else {
            panic!("expected Join");
        };
        assert!(matches!(*join.right, LogicalPlan::Filter(_)));
    }

    #[test]
    fn skips_left_outer_nullable_side_derivation() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(scan("r", &[("b", 2)])),
            join_type: JoinKind::LeftOuter,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }

    #[test]
    fn skips_when_derived_filter_already_exists_on_child() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let right_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("r", &[("b", 2)])),
            predicate: eq(col("r", "b", 2), int_lit(5)),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(right_filter),
            join_type: JoinKind::Inner,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }

    #[test]
    fn skips_when_derived_filter_exists_as_top_level_and_conjunct() {
        let left_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("l", &[("a", 1)])),
            predicate: eq(col("l", "a", 1), int_lit(5)),
            required_output_columns: None,
        });
        let right_filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan("r", &[("b", 2), ("c", 3)])),
            predicate: and(
                eq(col("r", "b", 2), int_lit(5)),
                eq(col("r", "c", 3), int_lit(9)),
            ),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(left_filter),
            right: Box::new(right_filter),
            join_type: JoinKind::Inner,
            condition: Some(eq(col("l", "a", 1), col("r", "b", 2))),
            required_output_columns: None,
        });

        assert!(JoinPredicateMoveAround.apply(plan).is_none());
    }
}
