//! Extract the best physical plan from the Memo after top-down search.
//!
//! Walks the winner map starting from the root group with the required
//! physical properties, recursively building a `PhysicalPlanNode` tree.

use std::collections::HashMap;

use super::memo::{GroupId, Memo};
use super::operator::{JoinDistribution, Operator, PhysicalDistributionOp, SortOp};
use super::physical_plan::{JoinExecutionDistribution, PhysicalPlanNode, PlanExecutionProps};
use super::property::{OrderingSpec, PhysicalPropertySet};
use super::search::{EnforcerKind, Winner};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode, SortKey};
use crate::sql::optimizer::statistics::Statistics;
use arrow::datatypes::DataType;

/// Extract the best physical plan tree from the Memo.
///
/// Walks the winner map starting from `root_group` with `required` properties.
/// For each winner, if it has an enforcer, an enforcer PhysicalPlanNode is
/// created wrapping the recursive extraction with the enforcer's child props.
/// Otherwise, the winner's physical expression is used directly with children
/// extracted according to the child properties recorded by search.
pub(crate) fn extract_best(
    memo: &mut Memo,
    root_group: GroupId,
    required: &PhysicalPropertySet,
    winners: &HashMap<(GroupId, PhysicalPropertySet), Winner>,
) -> Result<PhysicalPlanNode, String> {
    let cache_key = (root_group, required.clone());
    let winner = winners.get(&cache_key).ok_or_else(|| {
        format!(
            "no winner for group {} with props {:?}",
            root_group, required
        )
    })?;

    if winner.total_cost.is_infinite() {
        return Err(format!(
            "no feasible plan for group {} with props {:?}",
            root_group, required
        ));
    }

    let (group_stats, output_columns, expr) = {
        let group = &memo.groups[root_group];
        let group_stats = group_statistics(group);
        let output_columns = group
            .logical_props
            .as_ref()
            .map(|lp| lp.output_columns.clone())
            .unwrap_or_default();

        // Extract the underlying physical expression (the winner's expr_index).
        // After G3, the new search loop optimises children with `child_reqs` derived
        // from (op, required) directly — i.e. there is no separate cached winner
        // for (group, provided). The enforcer, if present, simply wraps this node.
        let expr = group
            .physical_exprs
            .get(winner.expr_index)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "winner expr_index {} out of bounds for group {} (has {} physical exprs)",
                    winner.expr_index,
                    root_group,
                    group.physical_exprs.len()
                )
            })?;
        (group_stats, output_columns, expr)
    };

    let child_reqs = winner.child_props.clone();
    if child_reqs.len() != expr.children.len() {
        return Err(format!(
            "winner child_props arity mismatch for group {} expr_index {}: expected {}, got {}",
            root_group,
            winner.expr_index,
            expr.children.len(),
            child_reqs.len()
        ));
    }

    // Recursively extract children.
    let mut children = Vec::with_capacity(expr.children.len());
    for (i, &child_group_id) in expr.children.iter().enumerate() {
        let child_req = child_reqs[i].clone();
        let child_node = extract_best(memo, child_group_id, &child_req, winners)?;
        children.push(child_node);
    }

    let mut op = match &expr.op {
        Operator::PhysicalCTEAnchor(op) => Operator::PhysicalCTEAnchor(op.clone()),
        other => other.clone(),
    };
    let join_distribution = if matches!(op, Operator::PhysicalHashJoin(_)) {
        crate::sql::optimizer::derive::hash_join::join_execution_distribution_for_alternative(
            &winner.alt_kind,
        )
    } else {
        None
    };
    if let (Operator::PhysicalHashJoin(join), Some(distribution)) = (&mut op, join_distribution) {
        join.distribution = match distribution {
            JoinExecutionDistribution::Broadcast => JoinDistribution::Broadcast,
            JoinExecutionDistribution::Partitioned => JoinDistribution::Shuffle,
            JoinExecutionDistribution::Colocate => JoinDistribution::Colocate,
        };
    }
    let inner_output_property = winner
        .enforcer
        .as_ref()
        .map(|enforcer| enforcer.child_props.clone())
        .unwrap_or_else(|| winner.output.clone());

    let inner_node = PhysicalPlanNode {
        op,
        children,
        stats: group_stats.clone(),
        output_columns: output_columns.clone(),
        execution_props: PlanExecutionProps {
            output_property: inner_output_property.clone(),
            child_output_properties: winner.child_outputs.clone(),
            join_distribution,
            scalar_arena: None,
        },
        build_runtime_filters: Vec::new(),
        probe_runtime_filters: Vec::new(),
    };

    // If the winner has an enforcer, wrap the inner node.
    if let Some(ref enforcer_info) = winner.enforcer {
        let enforcer_op = match &enforcer_info.kind {
            EnforcerKind::Distribution(spec) => {
                Operator::PhysicalDistribution(PhysicalDistributionOp { spec: spec.clone() })
            }
            EnforcerKind::Sort(ordering) => {
                let items = ordering_spec_to_sort_keys(&mut memo.scalars, ordering);
                // Sort enforcers inserted by the property-derivation pass are
                // pure ORDER BY enforcers, not analytic precursor sorts —
                // those come from `WindowToPhysical`. Leave the analytic
                // partition tag empty so this Sort still requires Gather.
                Operator::PhysicalSort(SortOp {
                    items,
                    analytic_partition_exprs: Vec::new(),
                    partition_limit: None,
                    topn_type: None,
                })
            }
        };

        return Ok(PhysicalPlanNode {
            op: enforcer_op,
            children: vec![inner_node],
            stats: group_stats,
            output_columns,
            execution_props: PlanExecutionProps {
                output_property: required.clone(),
                child_output_properties: vec![inner_output_property],
                join_distribution: None,
                scalar_arena: None,
            },
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        });
    }

    Ok(inner_node)
}

/// Build a `Statistics` from a group's logical properties.
fn group_statistics(group: &super::memo::Group) -> Statistics {
    if let Some(ref lp) = group.logical_props {
        Statistics {
            output_row_count: lp.row_count,
            row_count_confidence: lp.row_count_confidence,
            column_statistics: lp.column_statistics.clone(),
        }
    } else {
        Statistics {
            output_row_count: 1.0,
            row_count_confidence: crate::sql::optimizer::statistics::Confidence::Fallback,
            column_statistics: HashMap::new(),
        }
    }
}

/// Convert an `OrderingSpec` to scalar sort keys for the enforcer PhysicalSort node.
fn ordering_spec_to_sort_keys(arena: &mut ScalarArena, ordering: &OrderingSpec) -> Vec<SortKey> {
    match ordering {
        OrderingSpec::Any => vec![],
        OrderingSpec::Required(sort_keys) => sort_keys
            .iter()
            .map(|sk| SortKey {
                expr: arena.intern(ScalarNode::ColumnRef(sk.column), DataType::Null, true),
                asc: sk.asc,
                nulls_first: sk.nulls_first,
                display: None,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::cost::CostOptions;
    use crate::sql::optimizer::derive::PropertyAlternativeKind;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::operator::{
        JoinDistribution, LimitOp, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        ScanOp, ValuesOp,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::search::{EnforcerInfo, Winner, cost_estimate_for_total};
    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;

    fn test_col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn scan_op(table: &str) -> Operator {
        Operator::PhysicalScan(ScanOp {
            database: "db".into(),
            table: crate::sql::catalog::TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::sql::catalog::ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    fn winner_for_test(
        group_id: GroupId,
        expr_index: usize,
        total_cost: f64,
        enforcer: Option<EnforcerInfo>,
        output: PhysicalPropertySet,
        alt_kind: PropertyAlternativeKind,
        child_props: Vec<PhysicalPropertySet>,
        child_outputs: Vec<PhysicalPropertySet>,
    ) -> Winner {
        let cost_options = CostOptions::default();
        Winner::new(
            group_id,
            expr_index,
            cost_estimate_for_total(total_cost, &cost_options),
            &cost_options,
            enforcer,
            output,
            alt_kind,
            child_props,
            child_outputs,
        )
    }

    #[test]
    fn winner_for_test_preserves_total_cost_argument() {
        let total_cost = 6.0e299;
        let winner = winner_for_test(
            7,
            3,
            total_cost,
            None,
            PhysicalPropertySet::gather(),
            PropertyAlternativeKind::Default,
            vec![],
            vec![],
        );

        let tolerance = total_cost * 1.0e-12;
        assert!(
            (winner.total_cost - total_cost).abs() <= tolerance,
            "test fixture winner total {} should preserve argument {}",
            winner.total_cost,
            total_cost
        );
    }

    fn make_hash_join_winner_with_shuffle_child_props_for_test() -> (
        Memo,
        GroupId,
        HashMap<(GroupId, PhysicalPropertySet), Winner>,
        PhysicalPropertySet,
    ) {
        let mut memo = Memo::new();
        let left_key = intern_typed(&mut memo.scalars, &test_col(10));
        let right_key = intern_typed(&mut memo.scalars, &test_col(20));
        let eq_condition = PhysicalHashJoinEqCondition {
            left: left_key,
            right: right_key,
            null_safe: false,
        };
        let left = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let right = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq_condition],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![left, right],
        });

        let required = PhysicalPropertySet::gather();
        let left_req = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_req = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let root_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };

        let mut winners = HashMap::new();
        winners.insert(
            (left, left_req.clone()),
            winner_for_test(
                left,
                0,
                1.0,
                None,
                left_req.clone(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );
        winners.insert(
            (right, right_req.clone()),
            winner_for_test(
                right,
                0,
                1.0,
                None,
                right_req.clone(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );
        winners.insert(
            (root, required.clone()),
            winner_for_test(
                root,
                0,
                3.0,
                None,
                root_output,
                PropertyAlternativeKind::ShuffleJoin,
                vec![left_req.clone(), right_req.clone()],
                vec![left_req, right_req],
            ),
        );

        (memo, root, winners, required)
    }

    fn make_enforced_limit_winner_for_test() -> (
        Memo,
        GroupId,
        HashMap<(GroupId, PhysicalPropertySet), Winner>,
        PhysicalPropertySet,
        PhysicalPropertySet,
    ) {
        let mut memo = Memo::new();
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalLimit(LimitOp {
                limit: Some(1),
                offset: None,
            }),
            children: vec![child],
        });

        let required = PhysicalPropertySet::gather();
        let child_req = PhysicalPropertySet::any();
        let child_output = PhysicalPropertySet::any();
        let pre_enforcer_output = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };

        let mut winners = HashMap::new();
        winners.insert(
            (child, child_req.clone()),
            winner_for_test(
                child,
                0,
                1.0,
                None,
                child_output.clone(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );
        winners.insert(
            (root, required.clone()),
            winner_for_test(
                root,
                0,
                3.0,
                Some(EnforcerInfo {
                    kind: EnforcerKind::Distribution(required.distribution.clone()),
                    child_props: pre_enforcer_output.clone(),
                }),
                required.clone(),
                PropertyAlternativeKind::Default,
                vec![child_req],
                vec![child_output],
            ),
        );

        (memo, root, winners, required, pre_enforcer_output)
    }

    fn make_colocate_hash_join_winner_for_test() -> (
        Memo,
        GroupId,
        HashMap<(GroupId, PhysicalPropertySet), Winner>,
        PhysicalPropertySet,
    ) {
        let mut memo = Memo::new();
        let left_key = intern_typed(&mut memo.scalars, &test_col(10));
        let right_key = intern_typed(&mut memo.scalars, &test_col(20));
        let eq_condition = PhysicalHashJoinEqCondition {
            left: left_key,
            right: right_key,
            null_safe: false,
        };
        let left = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let right = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![eq_condition],
                other_condition: None,
                distribution: JoinDistribution::Colocate,
            }),
            children: vec![left, right],
        });

        let required = PhysicalPropertySet::any();
        let mut winners = HashMap::new();
        for child in [left, right] {
            winners.insert(
                (child, PhysicalPropertySet::any()),
                winner_for_test(
                    child,
                    0,
                    1.0,
                    None,
                    PhysicalPropertySet::any(),
                    PropertyAlternativeKind::Default,
                    vec![],
                    vec![],
                ),
            );
        }
        winners.insert(
            (root, required.clone()),
            winner_for_test(
                root,
                0,
                3.0,
                None,
                PhysicalPropertySet::any(),
                PropertyAlternativeKind::Default,
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()],
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()],
            ),
        );

        (memo, root, winners, required)
    }

    #[test]
    fn extract_uses_winner_child_props_instead_of_rederiving() {
        let (mut memo, root, winners, required) =
            make_hash_join_winner_with_shuffle_child_props_for_test();

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");
        let winner = winners
            .get(&(root, required.clone()))
            .expect("fixture should record root winner");

        assert_eq!(
            plan.execution_props.join_distribution,
            Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned)
        );
        assert_eq!(plan.execution_props.child_output_properties.len(), 2);
        assert_eq!(plan.execution_props.output_property, winner.output);
        assert_eq!(
            plan.execution_props.child_output_properties,
            winner.child_outputs
        );
    }

    #[test]
    fn extract_preserves_pre_enforcer_execution_output_property() {
        let (mut memo, root, winners, required, pre_enforcer_output) =
            make_enforced_limit_winner_for_test();

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");

        assert_eq!(plan.execution_props.output_property, required);
        assert_eq!(
            plan.execution_props.child_output_properties,
            vec![pre_enforcer_output.clone()]
        );
        assert_eq!(
            plan.children[0].execution_props.output_property,
            pre_enforcer_output
        );
    }

    #[test]
    fn extract_keeps_colocate_hash_join_distribution_when_default_metadata() {
        let (mut memo, root, winners, required) = make_colocate_hash_join_winner_for_test();

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");

        let Operator::PhysicalHashJoin(join) = &plan.op else {
            panic!("expected hash join");
        };
        assert_eq!(join.distribution, JoinDistribution::Colocate);
        assert_eq!(plan.execution_props.join_distribution, None);
    }

    #[test]
    fn extract_rejects_winner_child_prop_arity_mismatch() {
        let mut memo = Memo::new();
        let child = memo.new_group(MExpr {
            id: 0,
            op: scan_op("child"),
            children: vec![],
        });
        let root = memo.new_group(MExpr {
            id: 1,
            op: Operator::PhysicalLimit(LimitOp {
                limit: Some(1),
                offset: None,
            }),
            children: vec![child],
        });

        let required = PhysicalPropertySet::any();
        let mut winners = HashMap::new();
        winners.insert(
            (child, PhysicalPropertySet::any()),
            winner_for_test(
                child,
                0,
                1.0,
                None,
                PhysicalPropertySet::any(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );
        winners.insert(
            (root, required.clone()),
            winner_for_test(
                root,
                0,
                2.0,
                None,
                PhysicalPropertySet::any(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );

        let err = extract_best(&mut memo, root, &required, &winners)
            .expect_err("extract should reject missing child properties");
        assert!(
            err.contains("child_props") && err.contains("expected 1") && err.contains("got 0"),
            "unexpected error: {err}"
        );
    }
}
