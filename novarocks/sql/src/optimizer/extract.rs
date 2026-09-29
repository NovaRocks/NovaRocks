// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Extract the best optimizer physical operator tree from the Memo after top-down search.
//!
//! Walks the winner map starting from the root group with the required
//! physical properties, recursively building an `OptimizedOperatorNode` tree.

use std::collections::HashMap;

use super::memo::{GroupId, Memo};
use super::operator::{JoinDistribution, Operator, PhysicalDistributionOp, ProjectOp, SortOp};
use super::optimized_tree::{
    JoinExecutionDistribution, OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
};
use super::property::{OrderingSpec, PhysicalPropertySet};
use super::search::{EnforcerKind, Winner};
use crate::common::OutputColumn;
use crate::optimizer::scalar::{ScalarArena, ScalarNode, SortKey};
use crate::optimizer::statistics::Statistics;

/// Extract the best optimizer physical operator tree from the Memo.
///
/// Walks the winner map starting from `root_group` with `required` properties.
/// For each winner, if it has an enforcer, an enforcer OptimizedOperatorNode is
/// created wrapping the recursive extraction with the enforcer's child props.
/// Otherwise, the winner's physical expression is used directly with children
/// extracted according to the child properties recorded by search.
pub(crate) fn extract_best(
    memo: &mut Memo,
    root_group: GroupId,
    required: &PhysicalPropertySet,
    winners: &HashMap<(GroupId, PhysicalPropertySet), Winner>,
) -> Result<OptimizedOperatorNode, String> {
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
        let logical_props = group.logical_props.as_ref().ok_or_else(|| {
            format!(
                "optimizer extraction invariant violated: group {} has no logical properties",
                root_group
            )
        })?;
        let group_stats = Statistics {
            output_row_count: logical_props.row_count,
            row_count_confidence: logical_props.row_count_confidence,
            column_statistics: logical_props.column_statistics.clone(),
        };
        let output_columns = logical_props.output_columns.clone();

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
        crate::optimizer::derive::hash_join::join_execution_distribution_for_alternative(
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
            JoinExecutionDistribution::Singleton => JoinDistribution::Singleton,
        };
    }
    let output_columns =
        output_columns_for_physical_expr(&op, &memo.scalars, output_columns, &children)?;
    let inner_output_property = winner
        .enforcer
        .as_ref()
        .map(|enforcer| enforcer.child_props.clone())
        .unwrap_or_else(|| winner.output.clone());

    let inner_node = OptimizedOperatorNode {
        op,
        children,
        stats: group_stats.clone(),
        explain_stats: OptimizerExplainStats {
            cost_estimate: Some(winner.operator_cost_estimate.clone()),
            broadcast_decision: winner.operator_broadcast_decision,
        },
        output_columns: output_columns.clone(),
        execution_props: PlanExecutionProps {
            output_property: inner_output_property.clone(),
            child_output_properties: winner.child_outputs.clone(),
            join_distribution,
            scalar_arena: None,
        },
    };

    // If the winner has an enforcer, wrap the inner node.
    if let Some(ref enforcer_info) = winner.enforcer {
        let enforcer_op = match &enforcer_info.kind {
            EnforcerKind::Distribution(spec) => {
                Operator::PhysicalDistribution(PhysicalDistributionOp { spec: spec.clone() })
            }
            EnforcerKind::Sort(ordering) => {
                let items = ordering_spec_to_sort_keys(
                    &mut memo.scalars,
                    ordering,
                    &inner_node.output_columns,
                )?;
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

        return Ok(OptimizedOperatorNode {
            op: enforcer_op,
            children: vec![inner_node],
            stats: group_stats,
            explain_stats: OptimizerExplainStats {
                cost_estimate: winner.enforcer_cost_estimate.clone(),
                broadcast_decision: None,
            },
            output_columns,
            execution_props: PlanExecutionProps {
                output_property: required.clone(),
                child_output_properties: vec![inner_output_property],
                join_distribution: None,
                scalar_arena: None,
            },
        });
    }

    Ok(inner_node)
}

fn output_columns_for_physical_expr(
    op: &Operator,
    scalars: &ScalarArena,
    group_output_columns: Vec<OutputColumn>,
    children: &[OptimizedOperatorNode],
) -> Result<Vec<OutputColumn>, String> {
    match op {
        Operator::PhysicalScan(scan) => scan_output_columns(scan),
        Operator::PhysicalProject(project) => {
            project_output_columns(project, scalars, &group_output_columns, children)
        }
        Operator::PhysicalHashJoin(join) => join_output_columns(join.join_type, children),
        Operator::PhysicalNestLoopJoin(join) => join_output_columns(join.join_type, children),
        // An operator that emits its input unchanged emits exactly what its
        // child produces, which is not the same list as the logical group's:
        // the group describes the columns the group has, while a scan under
        // it may be pruned to the ones something actually reads. Reading the
        // group here let a pushed-down TopN claim its scan's pruned-away
        // columns, and the join above it then demanded producers that no
        // longer existed.
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalAssertOneRow(_) => passthrough_output_columns(op, children),
        _ => Ok(group_output_columns),
    }
}

fn passthrough_output_columns(
    op: &Operator,
    children: &[OptimizedOperatorNode],
) -> Result<Vec<OutputColumn>, String> {
    let [child] = children else {
        return Err(format!(
            "optimizer extraction requires one exact input occurrence map for {op:?}, got {}",
            children.len()
        ));
    };
    Ok(child.output_columns.clone())
}

fn join_output_columns(
    join_type: crate::analysis::JoinKind,
    children: &[OptimizedOperatorNode],
) -> Result<Vec<OutputColumn>, String> {
    let [left, right] = children else {
        return Err(format!(
            "optimizer extraction requires two exact join input occurrence maps, got {}",
            children.len()
        ));
    };
    Ok(match join_type {
        crate::analysis::JoinKind::LeftSemi
        | crate::analysis::JoinKind::LeftAnti
        | crate::analysis::JoinKind::NullAwareLeftAnti => left.output_columns.clone(),
        crate::analysis::JoinKind::RightSemi | crate::analysis::JoinKind::RightAnti => {
            right.output_columns.clone()
        }
        crate::analysis::JoinKind::Inner | crate::analysis::JoinKind::Cross => {
            let mut columns = left.output_columns.clone();
            columns.extend(right.output_columns.clone());
            columns
        }
        crate::analysis::JoinKind::LeftOuter => {
            let mut columns = left.output_columns.clone();
            columns.extend(nullable_output_columns(right.output_columns.clone()));
            columns
        }
        crate::analysis::JoinKind::RightOuter => {
            let mut columns = nullable_output_columns(left.output_columns.clone());
            columns.extend(right.output_columns.clone());
            columns
        }
        crate::analysis::JoinKind::FullOuter => {
            let mut columns = nullable_output_columns(left.output_columns.clone());
            columns.extend(nullable_output_columns(right.output_columns.clone()));
            columns
        }
    })
}

fn nullable_output_columns(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

fn project_output_columns(
    project: &ProjectOp,
    scalars: &ScalarArena,
    group_output_columns: &[OutputColumn],
    children: &[OptimizedOperatorNode],
) -> Result<Vec<OutputColumn>, String> {
    project
        .items
        .iter()
        .enumerate()
        .map(|(ordinal, item)| {
            let mut matches = group_output_columns
                .iter()
                .filter(|column| column.column_id == item.output_column_id);
            let inherited = matches.next();
            // Pass-through projections retain their value id, so `name,
            // name AS path` legitimately has two occurrences of one id.
            // Their display names may differ; their value metadata must not.
            if let Some(inherited) = inherited
                && matches.any(|column| !same_value_metadata(column, inherited))
            {
                return Err(format!(
                    "optimizer extraction project output occurrence {ordinal} for ColumnId({}) is ambiguous in logical output metadata",
                    item.output_column_id.0
                ));
            }
            let source_internal = match scalars.node(item.expr) {
                ScalarNode::ColumnRef(source_id) => {
                    let mut sources = children
                        .iter()
                        .flat_map(|child| child.output_columns.iter())
                        .filter(|column| column.column_id == *source_id);
                    let source = sources.next();
                    if let Some(source) = source
                        && sources.any(|column| !same_value_metadata(column, source))
                    {
                        return Err(format!(
                            "optimizer extraction project output occurrence {ordinal} has ambiguous source ColumnId({})",
                            source_id.0
                        ));
                    }
                    source.map(|column| column.is_internal)
                }
                _ => None,
            };
            Ok(OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: scalars.data_type(item.expr).clone(),
                nullable: scalars.nullable(item.expr),
                is_internal: inherited
                    .map(|column| column.is_internal)
                    .or(source_internal)
                    .unwrap_or(false),
            })
        })
        .collect()
}

fn same_value_metadata(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && left.is_internal == right.is_internal
}

fn scan_output_columns(scan: &super::operator::ScanOp) -> Result<Vec<OutputColumn>, String> {
    let Some(required_columns) = &scan.required_columns else {
        return Ok(scan.columns.clone());
    };
    let mut output = Vec::with_capacity(required_columns.len());
    for (ordinal, required) in required_columns.iter().enumerate() {
        let mut matches = scan
            .columns
            .iter()
            .filter(|column| column.column_id == *required);
        let column = matches.next().ok_or_else(|| {
            format!(
                "optimizer extraction scan required-column occurrence {ordinal} ColumnId({}) has no exact source occurrence",
                required.0
            )
        })?;
        if matches.next().is_some() {
            return Err(format!(
                "optimizer extraction scan required-column occurrence {ordinal} ColumnId({}) is ambiguous",
                required.0
            ));
        }
        output.push(column.clone());
    }
    Ok(output)
}

/// Convert an `OrderingSpec` to scalar sort keys for the enforcer PhysicalSort node.
fn ordering_spec_to_sort_keys(
    arena: &mut ScalarArena,
    ordering: &OrderingSpec,
    child_outputs: &[OutputColumn],
) -> Result<Vec<SortKey>, String> {
    match ordering {
        OrderingSpec::Any => Ok(Vec::new()),
        OrderingSpec::Required(sort_keys) => sort_keys
            .iter()
            .enumerate()
            .map(|(ordinal, sk)| {
                let mut matches = child_outputs
                    .iter()
                    .filter(|column| column.column_id == sk.column);
                let column = matches.next().ok_or_else(|| {
                    format!(
                        "sort enforcer key occurrence {ordinal} ColumnId({}) is absent from the exact child output map",
                        sk.column.0
                    )
                })?;
                if matches.next().is_some() {
                    return Err(format!(
                        "sort enforcer key occurrence {ordinal} ColumnId({}) is ambiguous in the exact child output map",
                        sk.column.0
                    ));
                }
                Ok(SortKey {
                    expr: arena.intern(
                        ScalarNode::ColumnRef(sk.column),
                        column.data_type.clone(),
                        column.nullable,
                    ),
                    asc: sk.asc,
                    nulls_first: sk.nulls_first,
                    display: None,
                })
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::cost::CostOptions;
    use crate::optimizer::derive::PropertyAlternativeKind;
    use crate::optimizer::memo::{MExpr, Memo};
    use crate::optimizer::operator::{
        JoinDistribution, LimitOp, Operator, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        ProjectOp, ScalarProjectItem, ScanOp, ValuesOp,
    };
    use crate::optimizer::property::DistributionSpec;
    use crate::optimizer::search::{EnforcerInfo, Winner};
    use crate::planner::optimizer_bridge::scalar::intern_typed;
    use arrow::datatypes::DataType;

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
            table: crate::planner::table::TableDef {
                name: table.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "These are distinct frozen SQL planning facts and grouping them would obscure the compiler boundary."
    )]
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
        Winner::from_legacy_total(
            group_id,
            expr_index,
            total_cost,
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

    #[test]
    fn project_output_metadata_preserves_repeated_value_aliases() {
        let mut scalars = ScalarArena::new();
        let source_id = ColumnId(2);
        let expr = scalars.intern(ScalarNode::ColumnRef(source_id), DataType::Utf8, true);
        let columns: Vec<_> = ["name", "path"]
            .into_iter()
            .map(|name| OutputColumn {
                column_id: source_id,
                name: name.to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                is_internal: true,
            })
            .collect();
        let project = ProjectOp {
            items: columns
                .iter()
                .map(|column| ScalarProjectItem {
                    expr,
                    output_name: column.name.clone(),
                    output_column_id: source_id,
                    expr_display: None,
                })
                .collect(),
            output_qualifier: None,
        };
        let child = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: OptimizerExplainStats::default(),
            output_columns: columns.clone(),
            execution_props: PlanExecutionProps::default(),
        };

        for inherited in [&columns[..], &[]] {
            let outputs =
                project_output_columns(&project, &scalars, inherited, std::slice::from_ref(&child))
                    .expect("repeated references to one value retain both output aliases");
            assert_eq!(outputs.len(), 2);
            for (output, expected) in outputs.iter().zip(&columns) {
                assert_eq!(output.column_id, expected.column_id);
                assert_eq!(output.name, expected.name);
                assert_eq!(output.data_type, expected.data_type);
                assert_eq!(output.nullable, expected.nullable);
                assert_eq!(output.is_internal, expected.is_internal);
            }
        }

        for mutation in 0..3 {
            let mut conflicting = columns.clone();
            match mutation {
                0 => conflicting[1].is_internal = false,
                1 => conflicting[1].nullable = false,
                _ => conflicting[1].data_type = DataType::Int64,
            }
            assert!(project_output_columns(&project, &scalars, &conflicting, &[]).is_err());
            let mut conflicting_child = child.clone();
            conflicting_child.output_columns = conflicting;
            assert!(project_output_columns(&project, &scalars, &[], &[conflicting_child]).is_err());
        }
    }

    #[test]
    fn extract_project_output_columns_follow_project_items_not_stale_group_props() {
        let mut memo = Memo::new();
        let source_col = OutputColumn {
            column_id: ColumnId(1),
            name: "__change_op_source".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        let child = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![source_col.clone()],
            }),
            children: vec![],
        });
        memo.groups[child].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
            vec![source_col.clone()],
            0.0,
        ));

        let output_id = ColumnId(14);
        let stale_id = ColumnId(13);
        let project_expr = memo.scalars.intern(
            ScalarNode::ColumnRef(source_col.column_id),
            DataType::Int8,
            false,
        );
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: project_expr,
                    output_name: "__change_op".to_string(),
                    output_column_id: output_id,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            children: vec![child],
        });
        memo.groups[root].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
            vec![OutputColumn {
                column_id: stale_id,
                name: "__change_op".to_string(),
                data_type: DataType::Int8,
                nullable: false,
                is_internal: true,
            }],
            0.0,
        ));

        let required = PhysicalPropertySet::any();
        let mut winners = HashMap::new();
        winners.insert(
            (child, required.clone()),
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
                vec![PhysicalPropertySet::any()],
                vec![PhysicalPropertySet::any()],
            ),
        );

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");

        assert_eq!(plan.output_columns.len(), 1);
        assert_eq!(plan.output_columns[0].column_id, output_id);
        assert_eq!(plan.output_columns[0].name, "__change_op");
        assert_eq!(plan.output_columns[0].data_type, DataType::Int8);
        assert!(!plan.output_columns[0].nullable);
        assert!(plan.output_columns[0].is_internal);
    }

    #[test]
    fn extract_join_output_columns_follow_children_not_stale_group_props() {
        let mut memo = Memo::new();
        let left_key_col = OutputColumn {
            column_id: ColumnId(1),
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let action_col = OutputColumn {
            column_id: ColumnId(14),
            name: "__change_op".to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        };
        let right_key_col = OutputColumn {
            column_id: ColumnId(2),
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let left_columns = vec![left_key_col.clone(), action_col.clone()];
        let right_columns = vec![right_key_col.clone()];
        let left = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: left_columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[left].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
            left_columns.clone(),
            0.0,
        ));
        let right = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: right_columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[right].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
            right_columns.clone(),
            0.0,
        ));

        let left_key = intern_typed(&mut memo.scalars, &test_col(1));
        let right_key = intern_typed(&mut memo.scalars, &test_col(2));
        let root = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: left_key,
                    right: right_key,
                    null_safe: false,
                }],
                other_condition: None,
                build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![left, right],
        });
        memo.groups[root].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
            vec![
                left_key_col.clone(),
                right_key_col.clone(),
                OutputColumn {
                    column_id: ColumnId(15),
                    name: "__change_op".to_string(),
                    data_type: DataType::Int8,
                    nullable: false,
                    is_internal: true,
                },
            ],
            0.0,
        ));

        let required = PhysicalPropertySet::any();
        let mut winners = HashMap::new();
        winners.insert(
            (left, required.clone()),
            winner_for_test(
                left,
                0,
                1.0,
                None,
                required.clone(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        );
        winners.insert(
            (right, required.clone()),
            winner_for_test(
                right,
                0,
                1.0,
                None,
                required.clone(),
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
                required.clone(),
                PropertyAlternativeKind::Default,
                vec![required.clone(), required.clone()],
                vec![required.clone(), required.clone()],
            ),
        );

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");
        let ids: Vec<_> = plan
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect();

        assert_eq!(ids, vec![ColumnId(1), ColumnId(14), ColumnId(2)]);
        assert_eq!(
            plan.output_columns
                .iter()
                .find(|column| column.name == "__change_op")
                .map(|column| column.column_id),
            Some(ColumnId(14))
        );
    }

    #[test]
    fn extract_join_output_columns_widen_outer_nullable_side() {
        let left_col = output_column_for_test(1, "l_k", false);
        let right_col = output_column_for_test(2, "r_k", false);
        let children = vec![
            physical_node_with_outputs(vec![left_col.clone()]),
            physical_node_with_outputs(vec![right_col.clone()]),
        ];

        let left_outer =
            join_output_columns(JoinKind::LeftOuter, &children).expect("left outer outputs");
        assert_eq!(left_outer.len(), 2);
        assert!(!left_outer[0].nullable);
        assert!(left_outer[1].nullable);

        let right_outer =
            join_output_columns(JoinKind::RightOuter, &children).expect("right outer outputs");
        assert_eq!(right_outer.len(), 2);
        assert!(right_outer[0].nullable);
        assert!(!right_outer[1].nullable);

        let full_outer =
            join_output_columns(JoinKind::FullOuter, &children).expect("full outer outputs");
        assert_eq!(full_outer.len(), 2);
        assert!(full_outer[0].nullable);
        assert!(full_outer[1].nullable);
    }

    #[test]
    fn extract_join_output_columns_preserve_repeated_occurrences() {
        let shared = output_column_for_test(7, "shared", false);
        let children = vec![
            physical_node_with_outputs(vec![shared.clone()]),
            physical_node_with_outputs(vec![shared.clone()]),
        ];

        let output = join_output_columns(JoinKind::Inner, &children)
            .expect("join extraction must preserve both input occurrences");
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].column_id, shared.column_id);
        assert_eq!(output[1].column_id, shared.column_id);
    }

    #[test]
    fn extract_scan_output_columns_reject_missing_required_occurrence() {
        let present = output_column_for_test(1, "present", false);
        let Operator::PhysicalScan(mut scan) = scan_op("t") else {
            unreachable!()
        };
        scan.columns = vec![present];
        scan.required_columns = Some(vec![ColumnId::new_for_test(99)]);

        let error = scan_output_columns(&scan)
            .expect_err("missing scan pruning metadata must fail closed during extraction");
        assert!(error.contains("has no exact source occurrence"));
    }

    fn output_column_for_test(id: u32, name: &str, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable,
            is_internal: false,
        }
    }

    fn physical_node_with_outputs(output_columns: Vec<OutputColumn>) -> OptimizedOperatorNode {
        OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: output_columns.clone(),
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        }
    }

    fn install_empty_logical_props(memo: &mut Memo, groups: &[GroupId]) {
        for &group in groups {
            memo.groups[group].logical_props =
                Some(crate::optimizer::memo::LogicalProperties::new(vec![], 0.0));
        }
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
                build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                distribution: JoinDistribution::Unknown,
            }),
            children: vec![left, right],
        });
        install_empty_logical_props(&mut memo, &[left, right, root]);

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
        install_empty_logical_props(&mut memo, &[child, root]);

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
                build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                distribution: JoinDistribution::Colocate,
            }),
            children: vec![left, right],
        });
        install_empty_logical_props(&mut memo, &[left, right, root]);

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
            Some(crate::optimizer::optimized_tree::JoinExecutionDistribution::Partitioned)
        );
        assert_eq!(plan.execution_props.child_output_properties.len(), 2);
        assert_eq!(plan.execution_props.output_property, winner.output);
        assert_eq!(
            plan.execution_props.child_output_properties,
            winner.child_outputs
        );
    }

    #[test]
    fn extract_freezes_gathered_hash_join_as_singleton() {
        let (mut memo, root, _, required) =
            make_hash_join_winner_with_shuffle_child_props_for_test();
        let children = memo.groups[root].physical_exprs[0].children.clone();
        let singleton = PhysicalPropertySet::gather();
        let mut winners = HashMap::new();
        for child in &children {
            winners.insert(
                (*child, singleton.clone()),
                winner_for_test(
                    *child,
                    0,
                    1.0,
                    None,
                    singleton.clone(),
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
                singleton.clone(),
                PropertyAlternativeKind::SingletonJoin,
                vec![singleton.clone(), singleton.clone()],
                vec![singleton.clone(), singleton],
            ),
        );

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");
        let Operator::PhysicalHashJoin(join) = &plan.op else {
            panic!("expected hash join")
        };
        assert_eq!(join.distribution, JoinDistribution::Singleton);
        assert_eq!(
            plan.execution_props.join_distribution,
            Some(crate::optimizer::optimized_tree::JoinExecutionDistribution::Singleton)
        );
    }

    #[test]
    fn extracted_singleton_outer_expression_join_reaches_one_final_finish() {
        for (seed, join_type) in [(51_u8, JoinKind::RightOuter), (52_u8, JoinKind::FullOuter)] {
            let mut memo = Memo::new();
            let left_column = OutputColumn {
                column_id: ColumnId(10),
                name: "left_key".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            };
            let right_column = OutputColumn {
                column_id: ColumnId(20),
                name: "right_key".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            };
            let left_key = memo.scalars.intern(
                ScalarNode::ColumnRef(left_column.column_id),
                DataType::Int64,
                false,
            );
            let right_input = memo.scalars.intern(
                ScalarNode::ColumnRef(right_column.column_id),
                DataType::Int32,
                false,
            );
            let right_key = memo.scalars.intern(
                ScalarNode::Cast {
                    child: right_input,
                    target: DataType::Int64,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                DataType::Int64,
                false,
            );
            let left = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::PhysicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![left_column.clone()],
                }),
                children: vec![],
            });
            let right = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::PhysicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![right_column.clone()],
                }),
                children: vec![],
            });
            let root = memo.new_group(MExpr {
                id: memo.next_expr_id(),
                op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                    join_type,
                    eq_conditions: vec![PhysicalHashJoinEqCondition {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }],
                    other_condition: None,
                    build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                    distribution: JoinDistribution::Unknown,
                }),
                children: vec![left, right],
            });
            memo.groups[left].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
                vec![left_column.clone()],
                0.0,
            ));
            memo.groups[right].logical_props = Some(
                crate::optimizer::memo::LogicalProperties::new(vec![right_column.clone()], 0.0),
            );
            let mut output_left = left_column;
            output_left.nullable = true;
            let mut output_right = right_column;
            if join_type == JoinKind::FullOuter {
                output_right.nullable = true;
            }
            memo.groups[root].logical_props = Some(crate::optimizer::memo::LogicalProperties::new(
                vec![output_left, output_right],
                0.0,
            ));

            let singleton = PhysicalPropertySet::gather();
            let required = singleton.clone();
            let mut winners = HashMap::new();
            for child in [left, right] {
                winners.insert(
                    (child, singleton.clone()),
                    winner_for_test(
                        child,
                        0,
                        1.0,
                        None,
                        singleton.clone(),
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
                    singleton.clone(),
                    PropertyAlternativeKind::SingletonJoin,
                    vec![singleton.clone(), singleton.clone()],
                    vec![singleton.clone(), singleton],
                ),
            );

            let mut extracted =
                extract_best(&mut memo, root, &required, &winners).expect("extract");
            crate::optimizer::optimized_tree::attach_scalar_arena(
                &mut extracted,
                Arc::new(memo.scalars.clone()),
            );
            let physical = crate::planner::optimizer_bridge::to_physical_plan(&extracted)
                .expect("materialize physical plan");
            let crate::planner::physical::PhysicalPlanKind::HashJoin(join) = &physical.kind else {
                panic!("expected hash join")
            };
            assert_eq!(
                join.execution_mode,
                Some(crate::planner::physical::JoinExecutionMode::Singleton)
            );
            let final_plan = crate::planner::distributed::build::lower_final_physical_plan(
                &physical,
                novarocks_physical_plan::PlanVersionId::try_new([seed; 16]).unwrap(),
                novarocks_physical_plan::PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
            )
            .expect("lower final physical plan")
            .finish()
            .expect("finish final physical plan exactly once");
            let fragment = final_plan
                .fragments()
                .get(&novarocks_physical_plan::FragmentId::new(0))
                .unwrap();
            let final_root = fragment.nodes().get(&fragment.root()).unwrap();
            assert!(matches!(
                final_root.kind,
                novarocks_physical_plan::NodeKind::HashJoin {
                    distribution: novarocks_physical_plan::JoinDistribution::Singleton,
                    ..
                }
            ));
        }
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
    fn extract_freezes_inner_and_enforcer_explain_stats_from_winner() {
        let (mut memo, root, mut winners, required, _) = make_enforced_limit_winner_for_test();
        let winner = winners
            .get_mut(&(root, required.clone()))
            .expect("fixture should record root winner");
        winner.operator_cost_estimate = crate::optimizer::statistics::CostEstimate {
            cpu_cost: 11.0,
            memory_cost: 2.0,
            network_cost: 0.0,
        };
        winner.enforcer_cost_estimate = Some(crate::optimizer::statistics::CostEstimate {
            cpu_cost: 0.0,
            memory_cost: 0.0,
            network_cost: 7.0,
        });

        let plan = extract_best(&mut memo, root, &required, &winners).expect("extract");

        assert_eq!(
            plan.explain_stats
                .cost_estimate
                .as_ref()
                .expect("enforcer explain cost")
                .network_cost,
            7.0
        );
        assert!(plan.explain_stats.broadcast_decision.is_none());
        assert_eq!(
            plan.children[0]
                .explain_stats
                .cost_estimate
                .as_ref()
                .expect("inner explain cost")
                .cpu_cost,
            11.0
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
        install_empty_logical_props(&mut memo, &[child, root]);

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

    #[test]
    fn extract_rejects_selected_group_without_logical_properties() {
        let mut memo = Memo::new();
        let root = memo.new_group(MExpr {
            id: 0,
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        });
        let required = PhysicalPropertySet::any();
        let winners = HashMap::from([(
            (root, required.clone()),
            winner_for_test(
                root,
                0,
                1.0,
                None,
                required.clone(),
                PropertyAlternativeKind::Default,
                vec![],
                vec![],
            ),
        )]);

        let error = extract_best(&mut memo, root, &required, &winners)
            .expect_err("a selected group without logical properties must fail closed");
        assert!(
            error.contains("group 0 has no logical properties"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn sort_enforcer_binds_exact_child_output_type_and_nullability() {
        let mut arena = ScalarArena::new();
        let column = OutputColumn {
            column_id: ColumnId(42),
            name: "nullable_decimal".to_string(),
            data_type: DataType::Decimal128(18, 4),
            nullable: true,
            is_internal: false,
        };
        let ordering = OrderingSpec::Required(vec![crate::optimizer::property::SortKey {
            column: column.column_id,
            asc: false,
            nulls_first: true,
        }]);

        let keys = ordering_spec_to_sort_keys(&mut arena, &ordering, &[column])
            .expect("exact child output should bind the sort enforcer key");

        assert_eq!(keys.len(), 1);
        assert_eq!(arena.data_type(keys[0].expr), &DataType::Decimal128(18, 4));
        assert!(arena.nullable(keys[0].expr));
        assert!(!keys[0].asc);
        assert!(keys[0].nulls_first);
    }

    #[test]
    fn sort_enforcer_rejects_key_absent_from_exact_child_outputs() {
        let mut arena = ScalarArena::new();
        let ordering = OrderingSpec::Required(vec![crate::optimizer::property::SortKey {
            column: ColumnId(42),
            asc: true,
            nulls_first: false,
        }]);

        let error = ordering_spec_to_sort_keys(&mut arena, &ordering, &[])
            .expect_err("an absent sort key must fail closed");

        assert!(
            error.contains("ColumnId(42)") && error.contains("absent"),
            "unexpected error: {error}"
        );
    }
}
