#![allow(dead_code)]
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

use super::scalar::{
    materialize, materialize_aggregate_calls, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};
use crate::analysis::{ExprKind, ProjectItem, TypedExpr};
use crate::common::{JoinKind, OutputColumn};
use crate::optimizer::operator::{Operator, PhysicalDistributionOp};
use crate::optimizer::optimized_tree::{JoinExecutionDistribution, OptimizedOperatorNode};
use crate::optimizer::property::DistributionSpec;
use crate::optimizer::scalar::ScalarArena;
use crate::optimizer::statistics::{ColumnStatistic, Confidence, DistinctValueCount};
use crate::planner::payload::*;
use crate::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalPlanStats, PlannerBroadcastDecision, PlannerColumnStatistic, PlannerConfidence,
    PlannerCostEstimate, TopNPhase,
};
use crate::planner::physical::{
    DistributedChangeEventExpandNode, DistributedChangeEventOutputExpr, DistributedChangeEventSpec,
    PhysicalHashAggregateNode, PhysicalHashJoinEqCondition, PhysicalHashJoinNode,
    PhysicalNestLoopJoinNode, PhysicalPlanKind, PhysicalPlanNode, PhysicalSetOpNode,
    PhysicalTopNNode, PlanSetOpKind, RedistributeMode, RedistributeNode,
};

struct BridgeCtx<'a> {
    scalars: &'a ScalarArena,
    next_scan_column: std::cell::Cell<u32>,
}

impl BridgeCtx<'_> {
    fn convert_node(&self, node: &OptimizedOperatorNode) -> Result<PhysicalPlanNode, String> {
        if node.op.is_logical() {
            return Err(format!(
                "Bridge 2a expected a physical operator, got logical operator {:?}",
                node.op
            ));
        }
        validate_shape(node)?;

        let children = node
            .children
            .iter()
            .map(|child| self.convert_node(child))
            .collect::<Result<Vec<_>, _>>()?;
        let kind = self.convert_kind(node)?;

        let output_columns = physical_node_output_columns(node)?;

        let plan = PhysicalPlanNode {
            kind,
            children,
            output_columns,
            stats: planner_stats(node),
            probe_runtime_filters: vec![],
        };
        self.materialize_change_window_sign(node, plan)
    }

    /// The private IMV algebra uses Int8. When that internal action is
    /// declared, materialize the provider's exact Int32 schema and restore
    /// the original internal identity with a real cast. Ordinary Delta TVF
    /// projections do not declare this optimizer-private column.
    fn materialize_change_window_sign(
        &self,
        original: &OptimizedOperatorNode,
        mut plan: PhysicalPlanNode,
    ) -> Result<PhysicalPlanNode, String> {
        use crate::planner::imv_rewrite::action_column::ImvActionColumn;
        use crate::planner::table::{ScanSource, SqlScanKind};
        use arrow::datatypes::DataType;
        let PhysicalPlanKind::Scan(scan) = &mut plan.kind else {
            return Ok(plan);
        };
        let ScanSource::Sql(source) = &scan.table.source;
        if !matches!(source.kind, SqlScanKind::Delta { .. }) {
            return Ok(plan);
        }
        let internal = match ImvActionColumn::declared(&scan.columns) {
            Ok(None) => return Ok(plan),
            Ok(Some(internal)) => internal.clone(),
            Err(crate::planner::imv_rewrite::action_column::ImvActionColumnDeclarationError::Duplicate) => {
                return Err("ChangeWindow scan must declare exactly one internal action column".into());
            }
            Err(_) => return Err("ChangeWindow internal action must be non-null Int8 before physical materialization".into()),
        };
        let raw_id = crate::column_id::ColumnId(self.next_scan_column.get());
        self.next_scan_column.set(
            raw_id.0.checked_add(1).ok_or_else(|| {
                "ChangeWindow physical column identity space exhausted".to_string()
            })?,
        );
        let mut raw = internal.clone();
        raw.column_id = raw_id;
        raw.value_type = novarocks_type_contract::FunctionValueType::new(DataType::Int32, false);
        for column in &mut scan.columns {
            if column.column_id == internal.column_id {
                *column = raw.clone();
            }
        }
        if let Some(required) = &mut scan.required_columns {
            for column in required {
                if *column == internal.column_id {
                    *column = raw_id;
                }
            }
        }
        // A predicate on the internal sign belongs after its cast. Other scan
        // predicates retain their original provider pushdown opportunity.
        let Operator::PhysicalScan(scan_op) = &original.op else {
            unreachable!()
        };
        let mut retained = Vec::new();
        let mut residual = Vec::new();
        for (scalar, predicate) in scan_op
            .predicates
            .iter()
            .zip(std::mem::take(&mut scan.predicates))
        {
            let refs =
                crate::optimizer::scalar_expr::collect_column_ids_strict(self.scalars, *scalar)
                    .ok_or_else(|| {
                        "ChangeWindow scan predicate has unresolved column references".to_string()
                    })?;
            if refs.contains(&internal.column_id) {
                residual.push(predicate);
            } else {
                retained.push(predicate);
            }
        }
        scan.predicates = retained;
        let requested_outputs = plan.output_columns.clone();
        let hidden_action = !requested_outputs
            .iter()
            .any(|column| column.column_id == internal.column_id);
        if hidden_action && residual.is_empty() {
            return Ok(plan);
        }
        let mut internal_outputs = requested_outputs.clone();
        if hidden_action {
            // A pushed predicate may need a sign that the scan's consumer did
            // not request. Retain it through the residual, then project it away.
            internal_outputs.push(internal.clone());
            plan.output_columns.push(raw.clone());
            if let Some(required) = &mut scan.required_columns
                && !required.contains(&raw_id)
            {
                required.push(raw_id);
            }
        }
        for column in &mut plan.output_columns {
            if column.column_id == internal.column_id {
                *column = raw.clone();
            }
        }
        let items = internal_outputs
            .iter()
            .map(|column| {
                let is_action = column.column_id == internal.column_id;
                let input = if is_action { &raw } else { column };
                let source = TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: input.column_id,
                        qualifier: None,
                        column: input.name.clone(),
                    },
                    value_type: input.value_type.clone(),
                };
                ProjectItem {
                    expr: if is_action {
                        TypedExpr {
                            kind: ExprKind::Cast {
                                expr: Box::new(source),
                                target: DataType::Int8,
                                decimal_overflow_policy:
                                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                            },
                            value_type: internal.value_type.clone(),
                        }
                    } else {
                        source
                    },
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                }
            })
            .collect();
        let mut projected = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items,
                output_qualifier: None,
            }),
            stats: plan.stats.clone(),
            probe_runtime_filters: Vec::new(),
            output_columns: internal_outputs,
            children: vec![plan],
        };
        for predicate in residual {
            projected = PhysicalPlanNode {
                kind: PhysicalPlanKind::Filter(PlanFilterNode { predicate }),
                stats: projected.stats.clone(),
                probe_runtime_filters: Vec::new(),
                output_columns: projected.output_columns.clone(),
                children: vec![projected],
            };
        }
        if hidden_action {
            let items = requested_outputs
                .iter()
                .map(|column| ProjectItem {
                    expr: output_column_ref(column),
                    output_name: column.name.clone(),
                    output_column_id: column.column_id,
                })
                .collect();
            projected = PhysicalPlanNode {
                kind: PhysicalPlanKind::Project(PlanProjectNode {
                    items,
                    output_qualifier: None,
                }),
                stats: projected.stats.clone(),
                probe_runtime_filters: Vec::new(),
                output_columns: requested_outputs,
                children: vec![projected],
            };
        }
        Ok(projected)
    }

    fn convert_kind(&self, node: &OptimizedOperatorNode) -> Result<PhysicalPlanKind, String> {
        match &node.op {
            Operator::PhysicalScan(op) => Ok(PhysicalPlanKind::Scan(
                PlanScanNode {
                    database: op.database.clone(),
                    table: op.table.clone(),
                    alias: op.alias.clone(),
                    columns: op.columns.clone(),
                    predicates: materialize_exprs(self.scalars, &op.predicates),
                    required_columns: op.required_columns.clone(),
                    variant_columns: op.variant_columns.clone(),
                    mv_rewritten_from: op.mv_rewritten_from.clone(),
                }
                .into(),
            )),
            Operator::PhysicalFilter(op) => Ok(PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: materialize(self.scalars, op.predicate),
            })),
            Operator::PhysicalProject(op) => Ok(PhysicalPlanKind::Project(PlanProjectNode {
                items: materialize_project_items(self.scalars, &op.items),
                output_qualifier: op.output_qualifier.clone(),
            })),
            Operator::PhysicalSort(op) => Ok(PhysicalPlanKind::Sort(PlanSortNode {
                items: materialize_sort_keys(self.scalars, &op.items),
                analytic_partition_by: materialize_exprs(
                    self.scalars,
                    &op.analytic_partition_exprs,
                ),
                output_columns: node.output_columns.clone(),
                offset: None,
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            })),
            Operator::PhysicalLimit(op) => Ok(PhysicalPlanKind::Limit(PlanLimitNode {
                limit: op.limit,
                offset: op.offset,
            })),
            Operator::PhysicalTopN(op) => Ok(PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: materialize_sort_keys(self.scalars, &op.items),
                limit: op.limit,
                offset: op.offset,
                phase: map_topn_phase(op.phase),
                is_split: op.is_split,
            })),
            Operator::PhysicalHashAggregate(op) => {
                let group_by = materialize_exprs(self.scalars, &op.group_by);
                Ok(PhysicalPlanKind::HashAggregate(Box::new(
                    PhysicalHashAggregateNode {
                        mode: map_agg_mode(op.mode),
                        group_by,
                        aggregates: materialize_aggregate_calls(
                            self.scalars,
                            &op.aggregates,
                            &op.output_layout,
                        ),
                        output_layout: map_aggregate_output_layout(&op.output_layout),
                        is_merge: op.is_merge.clone(),
                        output_columns: op.output_columns.clone(),
                        topn_runtime_filter_builds: Vec::new(),
                    },
                )))
            }
            Operator::PhysicalHashJoin(op) => {
                let expected_build_side =
                    crate::optimizer::operator::exact_hash_join_build_side(op.join_type)
                        .ok_or_else(|| {
                            "PhysicalHashJoin cannot carry a cross join kind".to_string()
                        })?;
                if op.build_side != expected_build_side {
                    return Err(format!(
                        "PhysicalHashJoin build side {:?} differs from planner decision {:?}",
                        op.build_side, expected_build_side
                    ));
                }
                let execution_mode = join_execution_mode(node.execution_props.join_distribution);
                Ok(PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                    join_type: op.join_type,
                    eq_conditions: op
                        .eq_conditions
                        .iter()
                        .map(|cond| PhysicalHashJoinEqCondition {
                            left: materialize(self.scalars, cond.left),
                            right: materialize(self.scalars, cond.right),
                            null_safe: cond.null_safe,
                        })
                        .collect(),
                    other_condition: op
                        .other_condition
                        .map(|expr| materialize(self.scalars, expr)),
                    build_side: map_hash_join_build_side(op.build_side),
                    distribution: map_join_distribution(op.distribution.clone()),
                    execution_mode,
                    build_runtime_filters: vec![],
                    output_columns: physical_join_output_columns(op.join_type, node)?,
                })))
            }
            Operator::PhysicalNestLoopJoin(op) => {
                Ok(PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
                    join_type: op.join_type,
                    condition: op.condition.map(|expr| materialize(self.scalars, expr)),
                    output_columns: physical_join_output_columns(op.join_type, node)?,
                }))
            }
            Operator::PhysicalValues(op) => Ok(PhysicalPlanKind::Values(PlanValuesNode {
                rows: op
                    .rows
                    .iter()
                    .map(|row| materialize_exprs(self.scalars, row))
                    .collect(),
                columns: op.columns.clone(),
            })),
            Operator::PhysicalAssertOneRow(op) => Ok(PhysicalPlanKind::AssertOneRow(
                PlanAssertOneRowNode::global_at_most_one(op.subquery_text.clone()),
            )),
            Operator::PhysicalRepeat(op) => Ok(PhysicalPlanKind::Repeat(PlanRepeatNode {
                repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
                grouping_ids: op.grouping_ids.clone(),
                all_rollup_columns: op.all_rollup_columns.clone(),
                all_rollup_column_ids: op.all_rollup_column_ids.clone(),
                grouping_key_aliases: op.grouping_key_aliases.clone(),
                grouping_fn_args: op.grouping_fn_args.clone(),
                grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: op.grouping_fn_ids.clone(),
                virtual_tuple_id: None,
            })),
            Operator::PhysicalChangeEventExpand(op) => {
                let events = op
                    .events
                    .iter()
                    .map(|event| DistributedChangeEventSpec {
                        predicate: event
                            .predicate
                            .map(|predicate| materialize(self.scalars, predicate)),
                        effect: event.effect,
                        assignments: event
                            .assignments
                            .iter()
                            .map(|assignment| DistributedChangeEventOutputExpr {
                                output_column_id: assignment.output_column_id,
                                expr: assignment.expr.map(|expr| materialize(self.scalars, expr)),
                            })
                            .collect(),
                    })
                    .collect();
                Ok(PhysicalPlanKind::ChangeEventExpand(
                    DistributedChangeEventExpandNode {
                        events,
                        output_columns: op.output_columns.clone(),
                        effect_column_id: op.effect_column_id,
                    },
                ))
            }
            Operator::PhysicalWindow(op) => Ok(PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: materialize_window_exprs(
                    self.scalars,
                    &op.window_exprs,
                    &op.output_columns,
                ),
                output_columns: op.output_columns.clone(),
            })),
            Operator::PhysicalUnion(op) => {
                // PIR-4 M1 only carries the UNION DISTINCT semantic marker through Bridge 2a.
                // The optimizer rewrite to aggregate is the real fix; builder-side expansion is M3e.
                let kind = if op.all {
                    PlanSetOpKind::UnionAll
                } else {
                    PlanSetOpKind::UnionDistinct
                };
                Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                    kind,
                    output_columns: op.output_columns.clone(),
                    child_output_columns: op.child_output_columns.clone(),
                }))
            }
            Operator::PhysicalIntersect(op) => Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::Intersect,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            })),
            Operator::PhysicalExcept(op) => Ok(PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::Except,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            })),
            Operator::PhysicalGenerateSeries(op) => {
                Ok(PhysicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                    start: op.start,
                    end: op.end,
                    step: op.step,
                    column_name: op.column_name.clone(),
                    alias: op.alias.clone(),
                    output_column_id: op.output_column_id,
                }))
            }
            Operator::PhysicalTableFunction(op) => {
                Ok(PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                    function_name: op.function_name.clone(),
                    args: materialize_exprs(self.scalars, &op.args),
                    binding: op.binding.clone(),
                    output_columns: op.output_columns.clone(),
                    alias: op.alias.clone(),
                    is_left_join: op.is_left_join,
                }))
            }
            Operator::PhysicalCTEAnchor(op) => Ok(PhysicalPlanKind::CTEAnchor(PlanCTEAnchorNode {
                cte_id: op.cte_id,
            })),
            Operator::PhysicalCTEProduce(op) => {
                Ok(PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
                    cte_id: op.cte_id,
                    output_columns: op.output_columns.clone(),
                }))
            }
            Operator::PhysicalCTEConsume(op) => {
                Ok(PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                    cte_id: op.cte_id,
                    alias: op.alias.clone(),
                    output_columns: op.output_columns.clone(),
                    producer_column_ids: op.producer_column_ids.clone(),
                }))
            }
            Operator::PhysicalDistribution(op) => {
                let mode = redistribute_mode(op)?;
                let child = node.children.first().ok_or_else(|| {
                    "Bridge 2a invalid PhysicalDistribution shape: expected 1 children, got 0"
                        .to_string()
                })?;
                let partition_exprs = redistribute_partition_exprs(&mode, child)?;
                Ok(PhysicalPlanKind::Redistribute(RedistributeNode {
                    mode,
                    partition_exprs,
                    output_columns: physical_distribution_output_columns(node)?,
                }))
            }
            op if op.is_logical() => Err(format!(
                "Bridge 2a expected a physical operator, got logical operator {op:?}"
            )),
            op => Err(format!("Bridge 2a cannot convert physical operator {op:?}")),
        }
    }
}

fn physical_node_output_columns(node: &OptimizedOperatorNode) -> Result<Vec<OutputColumn>, String> {
    match &node.op {
        Operator::PhysicalScan(scan) => exact_scan_output_columns(scan, &node.output_columns),
        Operator::PhysicalDistribution(_) => physical_distribution_output_columns(node),
        Operator::PhysicalHashJoin(join) => physical_join_output_columns(join.join_type, node),
        Operator::PhysicalNestLoopJoin(join) => physical_join_output_columns(join.join_type, node),
        Operator::PhysicalFilter(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalAssertOneRow(_) => physical_passthrough_output_columns(node),
        _ => Ok(node.output_columns.clone()),
    }
}

/// Returns the columns physically produced by an optimizer node. Bridge 2a
/// stores this contract in fragment-cut owner nodes so distributed planning
/// can consume it without walking the physical subtree again.
///
/// Aggregate completeness comes from `AggregateOutputLayout`, not from the
/// aggregate's planner-visible output projection.
fn physical_node_materialized_output_columns(
    node: &OptimizedOperatorNode,
) -> Result<Vec<OutputColumn>, String> {
    match &node.op {
        Operator::PhysicalHashAggregate(aggregate) => {
            Ok(aggregate.output_layout.full_output_columns())
        }
        _ => physical_node_output_columns(node),
    }
}

/// Materializes the child producer contract onto the distribution boundary.
/// The enclosing `PhysicalPlanNode::output_columns` is the source layout;
/// `RedistributeNode::output_columns` remains the requested exchange order.
fn physical_distribution_output_columns(
    node: &OptimizedOperatorNode,
) -> Result<Vec<OutputColumn>, String> {
    let child = node.children.first().ok_or_else(|| {
        "Bridge 2a PhysicalDistribution has no child output occurrence map".to_string()
    })?;
    physical_node_materialized_output_columns(child)
}

fn physical_passthrough_output_columns(
    node: &OptimizedOperatorNode,
) -> Result<Vec<OutputColumn>, String> {
    let child = node
        .children
        .first()
        .ok_or_else(|| format!("Bridge 2a {:?} has no child output occurrence map", node.op))?;
    physical_node_materialized_output_columns(child)
}

fn physical_join_output_columns(
    join_type: JoinKind,
    node: &OptimizedOperatorNode,
) -> Result<Vec<OutputColumn>, String> {
    let [left, right] = node.children.as_slice() else {
        return Err(format!(
            "Bridge 2a join has no exact two-input output occurrence map: got {} children",
            node.children.len()
        ));
    };
    let left = physical_node_materialized_output_columns(left)?;
    let right = physical_node_materialized_output_columns(right)?;
    let derived = join_output_columns_from_children(join_type, left, right);
    project_requested_output_columns(&node.output_columns, &derived, "join")
}

fn project_requested_output_columns(
    requested: &[OutputColumn],
    available: &[OutputColumn],
    context: &str,
) -> Result<Vec<OutputColumn>, String> {
    let mut consumed = vec![false; available.len()];
    let mut projected = Vec::with_capacity(requested.len());
    for (ordinal, column) in requested.iter().enumerate() {
        let Some((available_ordinal, available_column)) =
            available.iter().enumerate().find(|(index, candidate)| {
                !consumed[*index] && candidate.column_id == column.column_id
            })
        else {
            return Err(format!(
                "Bridge 2a {context} output occurrence {ordinal} for ColumnId({}) has no exact producer occurrence; requested [{}] from producers [{}]",
                column.column_id.0,
                requested
                    .iter()
                    .map(|column| format!("{}:{}", column.column_id.0, column.name))
                    .collect::<Vec<_>>()
                    .join(", "),
                available
                    .iter()
                    .map(|column| format!("{}:{}", column.column_id.0, column.name))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        };
        if column.name != available_column.name
            || column.value_type != available_column.value_type
            || column.is_internal != available_column.is_internal
        {
            return Err(format!(
                "Bridge 2a {context} output occurrence {ordinal} for ColumnId({}) differs from producer occurrence {available_ordinal}",
                column.column_id.0
            ));
        }
        consumed[available_ordinal] = true;
        projected.push(available_column.clone());
    }
    Ok(projected)
}

fn join_output_columns_from_children(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut output = left;
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::RightOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(right);
            output
        }
        JoinKind::FullOuter => {
            let mut output = nullable_output_columns(left);
            output.extend(nullable_output_columns(right));
            output
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut output = left;
            output.extend(right);
            output
        }
    }
}

fn nullable_output_columns(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.value_type = column
            .value_type
            .with_nullable(true)
            .expect("validated root NULL contract");
    }
    columns
}

fn exact_scan_output_columns(
    scan: &crate::optimizer::operator::ScanOp,
    output_columns: &[OutputColumn],
) -> Result<Vec<OutputColumn>, String> {
    let output = project_requested_output_columns(output_columns, &scan.columns, "scan")?;
    match &scan.required_columns {
        None if output.len() != scan.columns.len() => Err(format!(
            "Bridge 2a unpruned scan declares {} output occurrences for {} source occurrences",
            output.len(),
            scan.columns.len()
        )),
        None => Ok(output),
        Some(required) => {
            let actual = output
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>();
            if actual != *required {
                return Err(format!(
                    "Bridge 2a scan output occurrence map does not match its required-column producer contract: expected {required:?}, got {actual:?}"
                ));
            }
            Ok(output)
        }
    }
}

fn validate_shape(node: &OptimizedOperatorNode) -> Result<(), String> {
    match &node.op {
        Operator::PhysicalScan(_)
        | Operator::PhysicalValues(_)
        | Operator::PhysicalGenerateSeries(_)
        | Operator::PhysicalCTEConsume(_) => expect_arity(node, operator_name(&node.op), 0),

        Operator::PhysicalFilter(_)
        | Operator::PhysicalProject(_)
        | Operator::PhysicalSort(_)
        | Operator::PhysicalLimit(_)
        | Operator::PhysicalTopN(_)
        | Operator::PhysicalHashAggregate(_)
        | Operator::PhysicalAssertOneRow(_)
        | Operator::PhysicalRepeat(_)
        | Operator::PhysicalChangeEventExpand(_)
        | Operator::PhysicalWindow(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalDistribution(_) => expect_arity(node, operator_name(&node.op), 1),

        Operator::PhysicalHashJoin(_)
        | Operator::PhysicalNestLoopJoin(_)
        | Operator::PhysicalCTEAnchor(_) => expect_arity(node, operator_name(&node.op), 2),

        Operator::PhysicalUnion(op) => {
            validate_set_op_shape(node, "PhysicalUnion", &op.child_output_columns)
        }
        Operator::PhysicalIntersect(op) => {
            validate_set_op_shape(node, "PhysicalIntersect", &op.child_output_columns)
        }
        Operator::PhysicalExcept(op) => {
            validate_set_op_shape(node, "PhysicalExcept", &op.child_output_columns)
        }

        op if op.is_logical() => Ok(()),
        op => Err(format!(
            "Bridge 2a has no shape contract for physical operator {op:?}"
        )),
    }
}

fn expect_arity(
    node: &OptimizedOperatorNode,
    operator: &'static str,
    expected: usize,
) -> Result<(), String> {
    let got = node.children.len();
    if got == expected {
        Ok(())
    } else {
        Err(format!(
            "Bridge 2a invalid {operator} shape: expected {expected} children, got {got}"
        ))
    }
}

fn validate_set_op_shape(
    node: &OptimizedOperatorNode,
    operator: &'static str,
    child_output_columns: &[Vec<crate::common::OutputColumn>],
) -> Result<(), String> {
    let got = node.children.len();
    if got == 0 {
        return Err(format!(
            "Bridge 2a invalid {operator} shape: expected at least 1 child, got 0"
        ));
    }
    if child_output_columns.len() != got {
        return Err(format!(
            "Bridge 2a invalid {operator} shape: child_output_columns metadata expected {got} entries, got {}",
            child_output_columns.len()
        ));
    }
    Ok(())
}

fn operator_name(op: &Operator) -> &'static str {
    match op {
        Operator::PhysicalScan(_) => "PhysicalScan",
        Operator::PhysicalFilter(_) => "PhysicalFilter",
        Operator::PhysicalProject(_) => "PhysicalProject",
        Operator::PhysicalHashJoin(_) => "PhysicalHashJoin",
        Operator::PhysicalNestLoopJoin(_) => "PhysicalNestLoopJoin",
        Operator::PhysicalHashAggregate(_) => "PhysicalHashAggregate",
        Operator::PhysicalSort(_) => "PhysicalSort",
        Operator::PhysicalLimit(_) => "PhysicalLimit",
        Operator::PhysicalTopN(_) => "PhysicalTopN",
        Operator::PhysicalWindow(_) => "PhysicalWindow",
        Operator::PhysicalDistribution(_) => "PhysicalDistribution",
        Operator::PhysicalCTEAnchor(_) => "PhysicalCTEAnchor",
        Operator::PhysicalCTEProduce(_) => "PhysicalCTEProduce",
        Operator::PhysicalCTEConsume(_) => "PhysicalCTEConsume",
        Operator::PhysicalRepeat(_) => "PhysicalRepeat",
        Operator::PhysicalChangeEventExpand(_) => "PhysicalChangeEventExpand",
        Operator::PhysicalUnion(_) => "PhysicalUnion",
        Operator::PhysicalIntersect(_) => "PhysicalIntersect",
        Operator::PhysicalExcept(_) => "PhysicalExcept",
        Operator::PhysicalValues(_) => "PhysicalValues",
        Operator::PhysicalGenerateSeries(_) => "PhysicalGenerateSeries",
        Operator::PhysicalTableFunction(_) => "PhysicalTableFunction",
        Operator::PhysicalAssertOneRow(_) => "PhysicalAssertOneRow",
        _ => "LogicalOperator",
    }
}

fn join_execution_mode(
    distribution: Option<JoinExecutionDistribution>,
) -> Option<JoinExecutionMode> {
    distribution.map(|distribution| match distribution {
        JoinExecutionDistribution::Broadcast => JoinExecutionMode::Broadcast,
        JoinExecutionDistribution::Partitioned => JoinExecutionMode::Partitioned,
        JoinExecutionDistribution::Colocate => JoinExecutionMode::Colocate,
        JoinExecutionDistribution::Singleton => JoinExecutionMode::Singleton,
    })
}

fn map_agg_mode(mode: crate::optimizer::operator::AggMode) -> AggMode {
    use crate::optimizer::operator::AggMode as O;
    match mode {
        O::Single => AggMode::Single,
        O::Local => AggMode::Local,
        O::Global => AggMode::Global,
        O::DistinctGlobal => AggMode::DistinctGlobal,
        O::DistinctLocal => AggMode::DistinctLocal,
    }
}

fn map_topn_phase(phase: crate::optimizer::operator::TopNPhase) -> TopNPhase {
    use crate::optimizer::operator::TopNPhase as O;
    match phase {
        O::Partial => TopNPhase::Partial,
        O::Final => TopNPhase::Final,
    }
}

fn map_join_distribution(
    distribution: crate::optimizer::operator::JoinDistribution,
) -> JoinDistribution {
    use crate::optimizer::operator::JoinDistribution as O;
    match distribution {
        O::Unknown => JoinDistribution::Unknown,
        O::Shuffle => JoinDistribution::Shuffle,
        O::Broadcast => JoinDistribution::Broadcast,
        O::Colocate => JoinDistribution::Colocate,
        O::Singleton => JoinDistribution::Singleton,
    }
}

fn map_hash_join_build_side(
    build_side: crate::optimizer::operator::HashJoinBuildSide,
) -> crate::planner::physical::PhysicalHashJoinBuildSide {
    match build_side {
        crate::optimizer::operator::HashJoinBuildSide::Left => {
            crate::planner::physical::PhysicalHashJoinBuildSide::Left
        }
        crate::optimizer::operator::HashJoinBuildSide::Right => {
            crate::planner::physical::PhysicalHashJoinBuildSide::Right
        }
    }
}

fn map_hash_source(source: crate::optimizer::property::HashSource) -> HashSource {
    use crate::optimizer::property::HashSource as O;
    match source {
        O::ShuffleAgg => HashSource::ShuffleAgg,
        O::ShuffleJoin => HashSource::ShuffleJoin,
    }
}

fn map_aggregate_output_layout(
    layout: &crate::optimizer::operator::AggregateOutputLayout,
) -> AggregateOutputLayout {
    AggregateOutputLayout::new(
        layout.group_key_columns.clone(),
        layout.aggregate_columns.clone(),
    )
}

fn redistribute_mode(op: &PhysicalDistributionOp) -> Result<RedistributeMode, String> {
    match &op.spec {
        DistributionSpec::Gather => Ok(RedistributeMode::Gather),
        DistributionSpec::Broadcast => Ok(RedistributeMode::Broadcast),
        DistributionSpec::HashPartitioned { cols, source } => Ok(RedistributeMode::Hash {
            cols: cols.clone(),
            source: map_hash_source(*source),
        }),
        DistributionSpec::Any => Err(
            "Bridge 2a cannot convert PhysicalDistribution with DistributionSpec::Any".to_string(),
        ),
    }
}

fn redistribute_partition_exprs(
    mode: &RedistributeMode,
    child: &OptimizedOperatorNode,
) -> Result<Vec<TypedExpr>, String> {
    let RedistributeMode::Hash { cols, .. } = mode else {
        return Ok(Vec::new());
    };
    let child_outputs = physical_node_materialized_output_columns(child)?;
    let mut exprs = Vec::with_capacity(cols.len());
    for (ordinal, col_id) in cols.iter().enumerate() {
        let mut matches = child_outputs
            .iter()
            .filter(|column| column.column_id == *col_id);
        let Some(column) = matches.next() else {
            return Err(format!(
                "Bridge 2a hash distribution key occurrence {ordinal} ColumnId({}) is absent from the exact child output map",
                col_id.0
            ));
        };
        if matches.next().is_some() {
            return Err(format!(
                "Bridge 2a hash distribution key occurrence {ordinal} ColumnId({}) is ambiguous in the exact child output map",
                col_id.0
            ));
        }
        exprs.push(output_column_ref(column));
    }
    Ok(exprs)
}

fn output_column_ref(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
    }
}

fn planner_confidence(confidence: Confidence) -> PlannerConfidence {
    match confidence {
        Confidence::Fallback => PlannerConfidence::Fallback,
        Confidence::Estimated => PlannerConfidence::Estimated,
        Confidence::Exact => PlannerConfidence::Exact,
        Confidence::Measured => PlannerConfidence::Measured,
    }
}

fn planner_column_statistic(stat: &ColumnStatistic) -> PlannerColumnStatistic {
    PlannerColumnStatistic {
        min_value: stat.min_value,
        max_value: stat.max_value,
        nulls_fraction: stat.nulls_fraction,
        average_row_size: stat.average_row_size,
        ndv: match &stat.ndv {
            DistinctValueCount::Known { value, .. } => Some(*value),
            DistinctValueCount::Unknown { .. } => None,
        },
        confidence: planner_confidence(stat.confidence),
    }
}

fn planner_stats(node: &OptimizedOperatorNode) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: node.stats.output_row_count,
        row_count_confidence: planner_confidence(node.stats.row_count_confidence),
        column_statistics: node
            .stats
            .column_statistics
            .iter()
            .map(|(column, stat)| (*column, planner_column_statistic(stat)))
            .collect(),
        cost_estimate: node
            .explain_stats
            .cost_estimate
            .as_ref()
            .map(|cost| PlannerCostEstimate {
                cpu_cost: cost.cpu_cost,
                memory_cost: cost.memory_cost,
                network_cost: cost.network_cost,
            }),
        broadcast_decision: node
            .explain_stats
            .broadcast_decision
            .as_ref()
            .map(|decision| PlannerBroadcastDecision {
                feasible: decision.feasible,
                forced: decision.forced,
                build_bytes: decision.build_bytes,
                hash_table_bytes: decision.hash_table_bytes,
                effective_backend_count: decision.effective_backend_count,
                risk_adj_fanout_bytes: decision.risk_adj_fanout_bytes,
                per_node_budget_bytes: decision.per_node_budget_bytes,
                cluster_network_budget_bytes: decision.cluster_network_budget_bytes,
                risk_multiplier: decision.risk_multiplier,
                reject_reason: decision
                    .reject_reason
                    .as_ref()
                    .map(|reason| format!("{reason:?}")),
            }),
    }
}

pub(super) fn materialize_physical_plan(
    root: &OptimizedOperatorNode,
) -> Result<PhysicalPlanNode, String> {
    let scalars = root
        .execution_props
        .scalar_arena
        .as_deref()
        .ok_or_else(|| "Bridge 2a requires OptimizedOperatorNode.scalar_arena".to_string())?;
    fn max_column(node: &OptimizedOperatorNode) -> u32 {
        let outputs = node
            .output_columns
            .iter()
            .map(|column| column.column_id.0)
            .max()
            .unwrap_or(0);
        let scan = match &node.op {
            Operator::PhysicalScan(scan) => scan
                .columns
                .iter()
                .map(|column| column.column_id.0)
                .max()
                .unwrap_or(0),
            _ => 0,
        };
        node.children
            .iter()
            .map(max_column)
            .fold(outputs.max(scan), u32::max)
    }
    let next_scan_column = max_column(root)
        .checked_add(1)
        .ok_or_else(|| "ChangeWindow physical column identity space exhausted".to_string())?;
    BridgeCtx {
        scalars,
        next_scan_column: std::cell::Cell::new(next_scan_column),
    }
    .convert_node(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, LiteralValue, ProjectItem, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::common::{JoinKind, OutputColumn};
    use crate::optimizer::operator::{
        AggMode, AggregateOutputLayout, ChangeEventExpandOp, ChangeEventOutputExpr,
        ChangeEventSpec, JoinDistribution, Operator, PhysicalDistributionOp,
        PhysicalHashAggregateOp, PhysicalHashJoinOp, ProjectOp, ScanOp, ScanVariantColumn, UnionOp,
        ValuesOp,
    };
    use crate::optimizer::optimized_tree::{
        OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::optimizer::property::{
        DistributionSpec, HashSource as OptimizerHashSource, PhysicalPropertySet,
    };
    use crate::optimizer::scalar::ScalarArena;
    use crate::optimizer::statistics::{Confidence, CostEstimate, Statistics};
    use crate::planner::optimizer_bridge::scalar::intern_project_items;
    use crate::planner::physical::PhysicalPlanKind;
    use crate::planner::table::TableDef;
    use novarocks_types::schema::ColumnDef;
    use std::sync::Arc;

    fn int_expr(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        }
    }

    fn attach_arena(
        mut node: OptimizedOperatorNode,
        arena: Arc<ScalarArena>,
    ) -> OptimizedOperatorNode {
        node.execution_props.scalar_arena = Some(arena);
        node
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: false,
        }
    }

    #[test]
    fn physical_output_occurrence_preserves_full_domain_and_rejects_same_carrier_drift() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        let mut source = output_column(701, "payload");
        source.value_type =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let projected = project_requested_output_columns(
            &[source.clone()],
            &[source.clone()],
            "full-domain fixture",
        )
        .unwrap();
        assert_eq!(projected[0].value_type, source.value_type);
        assert_eq!(
            output_column_ref(&projected[0]).value_type,
            source.value_type
        );
        let mut wrong_domain = source.clone();
        wrong_domain.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                wrong_domain.value_type.data_type().clone(),
                wrong_domain.value_type.nullable(),
                ValueLogicalType::Physical,
            )
            .unwrap();
        assert!(
            project_requested_output_columns(
                &[wrong_domain],
                &[source.clone()],
                "full-domain fixture",
            )
            .is_err()
        );

        source.value_type = FunctionValueType::new(
            DataType::Struct(
                vec![Field::new("nested", DataType::Utf8, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.field-id".into(),
                        "17".into(),
                    )]),
                )]
                .into(),
            ),
            false,
        );
        let mut source_attributes_changed = source.clone();
        source_attributes_changed.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Struct(
                    vec![Field::new("nested", DataType::Utf8, false).with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider.field-id".into(),
                            "18".into(),
                        )]),
                    )]
                    .into(),
                ),
                source_attributes_changed.value_type.nullable(),
                source_attributes_changed.value_type.logical_type(),
            )
            .unwrap();
        assert!(
            project_requested_output_columns(
                &[source_attributes_changed],
                &[source.clone()],
                "full-domain fixture",
            )
            .is_ok()
        );
        let mut wrong_name = source.clone();
        wrong_name.value_type =
            crate::test_value_types::string_structure(&[("renamed", false, false)], false);
        assert!(
            project_requested_output_columns(
                &[wrong_name],
                &[source.clone()],
                "semantic name drift fixture",
            )
            .is_err()
        );
        let widened = nullable_output_columns(vec![source.clone()]);
        let mut expected = source.value_type.clone();
        expected = expected.with_nullable(true).unwrap();
        assert_eq!(widened[0].value_type, expected);
        assert_eq!(output_column_ref(&widened[0]).value_type, expected);
    }

    fn assert_output_columns_eq(actual: &[OutputColumn], expected: &[OutputColumn]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert_eq!(actual.column_id, expected.column_id);
            assert_eq!(actual.name, expected.name);
            assert_eq!(
                actual.value_type.data_type(),
                expected.value_type.data_type()
            );
            assert_eq!(actual.value_type.nullable(), expected.value_type.nullable());
            assert_eq!(actual.is_internal, expected.is_internal);
        }
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        }
    }

    fn assert_column_ref(expr: &TypedExpr, expected_id: u32, expected_name: &str) {
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &expr.kind
        else {
            panic!("expected ColumnRef, got {:?}", expr.kind);
        };
        assert_eq!(*column_id, ColumnId::new_for_test(expected_id));
        assert_eq!(column, expected_name);
    }

    fn base_node(op: Operator) -> OptimizedOperatorNode {
        OptimizedOperatorNode {
            op,
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                row_count_confidence: Confidence::Exact,
                ..Default::default()
            },
            explain_stats: Default::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: PhysicalPropertySet::gather(),
                child_output_properties: vec![],
                join_distribution: None,
                scalar_arena: None,
            },
        }
    }

    fn raw_values_node() -> OptimizedOperatorNode {
        base_node(Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    fn table_def(columns: &[OutputColumn]) -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: columns
                .iter()
                .map(|column| ColumnDef {
                    name: column.name.clone(),
                    data_type: column.value_type.data_type().clone(),
                    nullable: column.value_type.nullable(),
                    write_default: None,
                    logical_type: None,
                })
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    fn values_node() -> OptimizedOperatorNode {
        attach_arena(
            base_node(Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "v".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int32,
                        false,
                    ),

                    is_internal: false,
                }],
            })),
            Arc::new(ScalarArena::new()),
        )
    }

    #[test]
    fn bridge_converts_values_without_optimizer_types() {
        let physical = materialize_physical_plan(&values_node()).expect("bridge should convert");
        assert!(matches!(physical.kind, PhysicalPlanKind::Values(_)));
        assert!(physical.probe_runtime_filters.is_empty());
        assert_eq!(physical.stats.output_row_count, 1.0);
    }

    #[test]
    fn public_bridge_entry_verifies_and_materializes_optimizer_physical_plan() {
        let physical = super::super::to_physical_plan(&values_node())
            .expect("public optimizer bridge should convert");
        assert!(matches!(physical.kind, PhysicalPlanKind::Values(_)));
        assert!(physical.probe_runtime_filters.is_empty());
    }

    #[test]
    fn physical_scan_output_columns_follow_required_columns() {
        let columns = vec![output_column(1, "k"), output_column(2, "s")];
        let mut node = base_node(Operator::PhysicalScan(ScanOp {
            database: "db".to_string(),
            table: table_def(&columns),
            alias: None,
            stats_ref: None,
            columns: columns.clone(),
            predicates: vec![],
            required_columns: Some(vec![ColumnId::new_for_test(2)]),
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        node.output_columns = vec![output_column(2, "s")];
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");

        assert!(matches!(physical.kind, PhysicalPlanKind::Scan(_)));
        assert_output_columns_eq(&physical.output_columns, &[output_column(2, "s")]);
    }

    fn delta_sign_scan(
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> OptimizedOperatorNode {
        let mut action = crate::planner::imv_rewrite::action_column::ImvActionColumn::output_column(
            ColumnId::new_for_test(11),
        );
        action.value_type = value_type;
        let columns = vec![output_column(1, "id"), action];
        let mut table = table_def(&columns[..1]);
        crate::planner::imv_rewrite::action_column::ImvActionColumn::ensure_metadata_column(
            &mut table.iceberg_row_lineage_metadata_columns,
        );
        table.source = crate::compiler::mv_rewrite::test_scan_source(
            crate::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 7,
                to_snapshot_id: 11,
            },
        );
        let mut node = base_node(Operator::PhysicalScan(ScanOp {
            database: "db".into(),
            table,
            alias: None,
            stats_ref: None,
            columns: columns.clone(),
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        node.output_columns = columns;
        attach_arena(node, Arc::new(ScalarArena::new()))
    }

    #[test]
    fn ordinary_change_window_without_private_action_keeps_exact_provider_projection() {
        use arrow::datatypes::DataType;
        let mut source = delta_sign_scan(novarocks_type_contract::FunctionValueType::new(
            DataType::Int8,
            false,
        ));
        let Operator::PhysicalScan(scan) = &mut source.op else {
            unreachable!()
        };
        scan.columns.retain(|column| !column.is_internal);
        scan.table
            .iceberg_row_lineage_metadata_columns
            .retain(|column| {
                !column
                    .name
                    .eq_ignore_ascii_case(crate::common::CHANGE_OP_COLUMN)
            });
        scan.required_columns = Some(vec![ColumnId(1)]);
        source.output_columns.retain(|column| !column.is_internal);
        let plan = super::super::to_physical_plan(&source).unwrap();
        let PhysicalPlanKind::Scan(scan) = &plan.kind else {
            panic!("ordinary Delta projection needs no private sign cast")
        };
        assert_eq!(scan.required_columns, Some(vec![ColumnId(1)]));
        assert_eq!(scan.columns.len(), 1);
        assert_eq!(plan.output_columns.len(), 1);
        assert_eq!(plan.output_columns[0].column_id, ColumnId(1));
        assert!(!plan.output_columns[0].is_internal);
        let crate::planner::table::ScanSource::Sql(binding) = &scan.table.source;
        assert!(matches!(
            binding.kind,
            crate::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 7,
                to_snapshot_id: 11
            }
        ));
    }

    #[test]
    fn change_window_rejects_duplicate_private_action_declarations() {
        use arrow::datatypes::DataType;
        let mut source = delta_sign_scan(novarocks_type_contract::FunctionValueType::new(
            DataType::Int8,
            false,
        ));
        let Operator::PhysicalScan(scan) = &mut source.op else {
            unreachable!()
        };
        let duplicate = crate::planner::imv_rewrite::action_column::ImvActionColumn::output_column(
            ColumnId(12),
        );
        scan.columns.push(duplicate.clone());
        source.output_columns.push(duplicate);
        let error = super::super::to_physical_plan(&source).unwrap_err();
        assert!(
            error.contains("exactly one internal action column"),
            "{error}"
        );
    }

    #[test]
    fn change_window_materializes_exact_int32_scan_and_explicit_int8_identity() {
        use arrow::datatypes::DataType;
        let source = delta_sign_scan(novarocks_type_contract::FunctionValueType::new(
            DataType::Int8,
            false,
        ));
        let plan = super::super::to_physical_plan(&source).unwrap();
        let PhysicalPlanKind::Project(project) = &plan.kind else {
            panic!("expected physical sign cast")
        };
        let PhysicalPlanKind::Scan(scan) = &plan.children[0].kind else {
            panic!("expected raw provider scan")
        };
        assert_eq!(scan.columns[1].column_id, ColumnId::new_for_test(12));
        assert_eq!(scan.columns[1].value_type.data_type(), &DataType::Int32);
        assert!(!scan.columns[1].value_type.nullable());
        assert_eq!(
            plan.children[0].output_columns[1].column_id,
            ColumnId::new_for_test(12)
        );
        assert_eq!(plan.output_columns[1].column_id, ColumnId::new_for_test(11));
        assert_eq!(
            project.items[1].output_column_id,
            ColumnId::new_for_test(11)
        );
        assert!(matches!(&project.items[1].expr.kind,
            ExprKind::Cast { target: DataType::Int8, expr, .. }
            if expr.value_type.data_type() == &DataType::Int32
            && matches!(expr.kind, ExprKind::ColumnRef { column_id: ColumnId(12), .. })));
    }

    #[test]
    fn change_window_keeps_a_pruned_sign_until_its_residual_predicate() {
        use arrow::datatypes::DataType;
        let mut source = delta_sign_scan(novarocks_type_contract::FunctionValueType::new(
            DataType::Int8,
            false,
        ));
        let mut arena = source
            .execution_props
            .scalar_arena
            .as_deref()
            .unwrap()
            .clone();
        let sign = arena.intern(
            crate::optimizer::scalar::ScalarNode::ColumnRef(ColumnId(11)),
            novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        );
        let zero = arena.intern(
            crate::optimizer::scalar::ScalarNode::Literal(
                crate::optimizer::scalar::HashableLiteral(LiteralValue::Int(0)),
            ),
            novarocks_type_contract::FunctionValueType::new(DataType::Int8, false),
        );
        let predicate = arena.intern(
            crate::optimizer::scalar::ScalarNode::BinaryOp {
                op: crate::common::BinOp::Gt,
                left: sign,
                right: zero,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        );
        let Operator::PhysicalScan(scan) = &mut source.op else {
            unreachable!()
        };
        scan.predicates = vec![predicate];
        scan.required_columns = Some(vec![ColumnId(1)]);
        source.output_columns.truncate(1);
        source.execution_props.scalar_arena = Some(Arc::new(arena));
        let plan = super::super::to_physical_plan(&source).unwrap();
        assert_eq!(
            plan.output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId(1)]
        );
        let filter = &plan.children[0];
        assert!(matches!(filter.kind, PhysicalPlanKind::Filter(_)));
        assert_eq!(filter.output_columns[1].column_id, ColumnId(11));
        let cast = &filter.children[0];
        assert!(matches!(cast.kind, PhysicalPlanKind::Project(_)));
        let PhysicalPlanKind::Scan(scan) = &cast.children[0].kind else {
            panic!("expected provider scan")
        };
        assert!(scan.predicates.is_empty());
        assert_eq!(scan.required_columns, Some(vec![ColumnId(1), ColumnId(12)]));
        assert_eq!(scan.columns[1].value_type.data_type(), &DataType::Int32);
    }

    #[test]
    fn change_window_rejects_wrong_or_nullable_internal_sign_declarations() {
        use arrow::datatypes::DataType;
        for declaration in [
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            novarocks_type_contract::FunctionValueType::new(DataType::Int8, true),
        ] {
            let error = super::super::to_physical_plan(&delta_sign_scan(declaration)).unwrap_err();
            assert!(error.contains("non-null Int8"), "{error}");
        }
    }

    #[test]
    fn physical_scan_rejects_missing_required_output_occurrence() {
        let columns = vec![output_column(1, "k"), output_column(2, "s")];
        let mut node = base_node(Operator::PhysicalScan(ScanOp {
            database: "db".to_string(),
            table: table_def(&columns),
            alias: None,
            stats_ref: None,
            columns,
            predicates: vec![],
            required_columns: Some(vec![ColumnId::new_for_test(99)]),
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        node.output_columns = vec![];
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let error = super::super::to_physical_plan(&node)
            .expect_err("missing scan pruning output must fail closed");
        assert!(error.contains("required-column producer contract"));
    }

    #[test]
    fn physical_scan_materialized_outputs_keep_variant_and_extended_columns() {
        let payload = output_column(1, "payload");
        let synthetic = OutputColumn {
            column_id: ColumnId::new_for_test(101),
            name: "__variant_payload_0".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                true,
            ),

            is_internal: true,
        };
        let extended = OutputColumn {
            column_id: ColumnId::new_for_test(102),
            name: "_row_id".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: true,
        };
        let table_columns = vec![payload.clone()];
        let scan_columns = vec![payload.clone(), synthetic.clone(), extended.clone()];
        let mut node = base_node(Operator::PhysicalScan(ScanOp {
            database: "db".to_string(),
            table: table_def(&table_columns),
            alias: None,
            stats_ref: None,
            columns: scan_columns.clone(),
            predicates: vec![],
            required_columns: Some(vec![
                payload.column_id,
                synthetic.column_id,
                extended.column_id,
            ]),
            variant_columns: vec![ScanVariantColumn::test_fixture(
                payload.column_id,
                payload.name.clone(),
                synthetic.column_id,
                synthetic.name.clone(),
                "$.k".to_string(),
                arrow::datatypes::DataType::Utf8,
                "string".to_string(),
                true,
                payload.value_type.clone(),
            )],
            mv_rewritten_from: None,
        }));
        node.output_columns = scan_columns;
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");

        assert_output_columns_eq(&physical.output_columns, &[payload, synthetic, extended]);
    }

    #[test]
    fn bridge_materializes_values_rows() {
        let mut arena = ScalarArena::new();
        let one = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &int_expr(1),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let node = attach_arena(
            base_node(Operator::PhysicalValues(ValuesOp {
                rows: vec![vec![one]],
                columns: vec![],
            })),
            Arc::new(arena),
        );

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Values(values) = physical.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.rows.len(), 1);
        assert_eq!(values.rows[0].len(), 1);
        assert!(matches!(
            values.rows[0][0].kind,
            ExprKind::Literal(LiteralValue::Int(1))
        ));
        assert_eq!(
            values.rows[0][0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
        assert!(!values.rows[0][0].value_type.nullable());
    }

    #[test]
    fn physical_distribution_becomes_redistribute_hash() {
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![ColumnId::new_for_test(7)],
                source: OptimizerHashSource::ShuffleJoin,
            },
        }));
        node.output_columns = vec![output_column(7, "parent_k")];
        let mut child = raw_values_node();
        child.output_columns = vec![output_column(7, "child_k")];
        node.children.push(child);
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };
        assert_eq!(
            redistribute.mode,
            RedistributeMode::Hash {
                cols: vec![ColumnId::new_for_test(7)],
                source: HashSource::ShuffleJoin,
            }
        );
        assert_eq!(redistribute.partition_exprs.len(), 1);
        assert_column_ref(&redistribute.partition_exprs[0], 7, "child_k");
        assert_output_columns_eq(&redistribute.output_columns, &[output_column(7, "child_k")]);
        assert_eq!(physical.children.len(), 1);
        assert!(matches!(
            physical.children[0].kind,
            PhysicalPlanKind::Values(_)
        ));
    }

    #[test]
    fn physical_distribution_over_pruned_scan_uses_materialized_outputs() {
        let columns = vec![output_column(1, "k"), output_column(2, "s")];
        let mut child = base_node(Operator::PhysicalScan(ScanOp {
            database: "db".to_string(),
            table: table_def(&columns),
            alias: None,
            stats_ref: None,
            columns: columns.clone(),
            predicates: vec![],
            required_columns: Some(vec![ColumnId::new_for_test(2)]),
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        child.output_columns = vec![output_column(2, "s")];

        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Gather,
        }));
        node.output_columns = columns;
        node.children.push(child);
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };
        assert_output_columns_eq(&redistribute.output_columns, &[output_column(2, "s")]);
    }

    #[test]
    fn physical_hash_distribution_rejects_missing_child_key() {
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![ColumnId::new_for_test(99)],
                source: OptimizerHashSource::ShuffleJoin,
            },
        }));
        let mut child = raw_values_node();
        child.output_columns = vec![output_column(7, "child_k")];
        node.output_columns = child.output_columns.clone();
        node.children.push(child);
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let error = materialize_physical_plan(&node)
            .expect_err("missing hash distribution key must fail in the bridge");
        assert!(error.contains("ColumnId(99) is absent from the exact child output map"));
    }

    #[test]
    fn physical_distribution_over_aggregate_carries_group_key_partition_expr() {
        let mut arena = ScalarArena::new();
        let group_expr = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(7),
                    qualifier: Some("a".to_string()),
                    column: "k".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let group_column = output_column(7, "k");
        let mut aggregate = base_node(Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![group_expr],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![group_column.clone()], vec![]),
            output_columns: vec![group_column.clone()],
            is_merge: vec![],
        }));
        aggregate.children.push(raw_values_node());
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![ColumnId::new_for_test(7)],
                source: OptimizerHashSource::ShuffleAgg,
            },
        }));
        node.output_columns = vec![group_column];
        node.children.push(aggregate);
        let node = attach_arena(node, Arc::new(arena));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };

        assert_eq!(redistribute.partition_exprs.len(), 1);
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &redistribute.partition_exprs[0].kind
        else {
            panic!("expected partition ColumnRef");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(7));
        assert_eq!(qualifier.as_deref(), None);
        assert_eq!(column, "k");
    }

    #[test]
    fn physical_distribution_over_aggregate_uses_materialized_layout_outputs() {
        let mut arena = ScalarArena::new();
        let group_expr = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &col_expr(7, "map2"),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let distinct_expr = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &col_expr(2, "s2"),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let map2_column = output_column(7, "map2");
        let s2_column = output_column(2, "s2");
        let mut aggregate = base_node(Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![group_expr, distinct_expr],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(
                vec![map2_column.clone(), s2_column.clone()],
                vec![],
            ),
            output_columns: vec![map2_column.clone()],
            is_merge: vec![],
        }));
        aggregate.output_columns = vec![output_column(1, "stale_visible")];
        aggregate.children.push(raw_values_node());
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![ColumnId::new_for_test(7), ColumnId::new_for_test(2)],
                source: OptimizerHashSource::ShuffleAgg,
            },
        }));
        node.output_columns = vec![output_column(1, "stale_parent")];
        node.children.push(aggregate);
        let node = attach_arena(node, Arc::new(arena));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };

        assert_output_columns_eq(
            &redistribute.output_columns,
            &[map2_column.clone(), s2_column.clone()],
        );
        assert_output_columns_eq(&physical.output_columns, &[map2_column, s2_column]);
    }

    #[test]
    fn physical_distribution_over_aggregate_partitions_by_group_key_output_id() {
        let mut arena = ScalarArena::new();
        let group_expr = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(7),
                    qualifier: Some("a".to_string()),
                    column: "k".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            },
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let group_column = output_column(8, "k_group");
        let mut aggregate = base_node(Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![group_expr],
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(vec![group_column.clone()], vec![]),
            output_columns: vec![group_column.clone()],
            is_merge: vec![],
        }));
        aggregate.children.push(raw_values_node());
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::HashPartitioned {
                cols: vec![ColumnId::new_for_test(8)],
                source: OptimizerHashSource::ShuffleAgg,
            },
        }));
        node.output_columns = vec![group_column];
        node.children.push(aggregate);
        let node = attach_arena(node, Arc::new(arena));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };

        assert_eq!(redistribute.partition_exprs.len(), 1);
        assert_column_ref(&redistribute.partition_exprs[0], 8, "k_group");
    }

    #[test]
    fn bridge_converts_change_event_expand() {
        let mut arena = ScalarArena::new();
        let predicate = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &col_expr(1, "matched"),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let assignment = crate::planner::optimizer_bridge::scalar::intern_typed(
            &mut arena,
            &int_expr(42),
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let mut node = base_node(Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
            events: vec![ChangeEventSpec {
                predicate: Some(predicate),
                effect: novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
                assignments: vec![ChangeEventOutputExpr {
                    output_column_id: ColumnId::new_for_test(2),
                    expr: Some(assignment),
                }],
            }],
            output_columns: vec![output_column(2, "payload")],
            effect_column_id: ColumnId::new_for_test(3),
        }));
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(arena));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::ChangeEventExpand(expand) = physical.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(physical.children.len(), 1);
        assert!(matches!(
            physical.children[0].kind,
            PhysicalPlanKind::Values(_)
        ));
        assert_eq!(expand.events.len(), 1);
        assert_eq!(
            expand.events[0].effect,
            novarocks_spi::connector::ConnectorRowMutationEffect::Insert
        );
        assert_eq!(expand.events[0].assignments.len(), 1);
        assert_eq!(
            expand.events[0].assignments[0].output_column_id,
            ColumnId::new_for_test(2)
        );
        assert!(matches!(
            expand.events[0].assignments[0]
                .expr
                .as_ref()
                .expect("assignment should materialize")
                .kind,
            ExprKind::Literal(LiteralValue::Int(42))
        ));
        assert_column_ref(
            expand.events[0]
                .predicate
                .as_ref()
                .expect("predicate should materialize"),
            1,
            "matched",
        );
        assert_eq!(expand.output_columns.len(), 1);
        assert_eq!(
            expand.output_columns[0].column_id,
            ColumnId::new_for_test(2)
        );
        assert_eq!(expand.output_columns[0].name, "payload");
        assert_eq!(expand.effect_column_id, ColumnId::new_for_test(3));
    }

    #[test]
    fn root_gather_distribution_is_preserved_as_redistribute() {
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Gather,
        }));
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::Redistribute(redistribute) = physical.kind else {
            panic!("expected Redistribute");
        };
        assert_eq!(redistribute.mode, RedistributeMode::Gather);
        assert_eq!(physical.children.len(), 1);
        assert!(matches!(
            physical.children[0].kind,
            PhysicalPlanKind::Values(_)
        ));
    }

    #[test]
    fn physical_distribution_any_is_rejected() {
        let mut node = base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: DistributionSpec::Any,
        }));
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let err = materialize_physical_plan(&node).expect_err("Any should be rejected");
        assert!(err.contains("DistributionSpec::Any"));
    }

    #[test]
    fn malformed_distribution_arity_is_rejected() {
        let node = attach_arena(
            base_node(Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            })),
            Arc::new(ScalarArena::new()),
        );

        let err = materialize_physical_plan(&node).expect_err("distribution needs one child");
        assert!(err.contains("PhysicalDistribution"));
        assert!(err.contains("expected 1 children, got 0"));
    }

    #[test]
    fn malformed_hash_join_arity_is_rejected() {
        let mut node = base_node(Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Broadcast,
        }));
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let err = materialize_physical_plan(&node).expect_err("hash join needs two children");
        assert!(err.contains("PhysicalHashJoin"));
        assert!(err.contains("expected 2 children, got 1"));
    }

    #[test]
    fn physical_hash_join_rejects_stale_output_occurrence_map() {
        let left_a = output_column(1, "left_a");
        let left_b = output_column(2, "left_b");
        let right_c = output_column(3, "right_c");
        let mut left = raw_values_node();
        left.output_columns = vec![left_a.clone(), left_b.clone()];
        let mut right = raw_values_node();
        right.output_columns = vec![right_c.clone()];
        let mut node = base_node(Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Broadcast,
        }));
        node.output_columns = vec![output_column(10, "stale_parent_state")];
        node.children.push(left);
        node.children.push(right);
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let error = materialize_physical_plan(&node)
            .expect_err("bridge must reject a stale join output occurrence map");
        assert!(error.contains("ColumnId(10) has no exact producer occurrence"));
    }

    #[test]
    fn physical_outer_join_outputs_widen_nullable_side() {
        let left_column = output_column(1, "left_k");
        let right_column = output_column(2, "right_k");
        let convert = |join_type| {
            let mut left = raw_values_node();
            left.output_columns = vec![left_column.clone()];
            let mut right = raw_values_node();
            right.output_columns = vec![right_column.clone()];
            let mut node = base_node(Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type,
                eq_conditions: vec![],
                other_condition: None,
                build_side: crate::optimizer::operator::exact_hash_join_build_side(join_type)
                    .expect("bridge fixture uses a legal hash join kind"),
                distribution: JoinDistribution::Broadcast,
            }));
            let mut expected_left = left_column.clone();
            let mut expected_right = right_column.clone();
            match join_type {
                JoinKind::LeftOuter => {
                    expected_right.value_type =
                        expected_right.value_type.with_nullable(true).unwrap()
                }
                JoinKind::RightOuter => {
                    expected_left.value_type = expected_left.value_type.with_nullable(true).unwrap()
                }
                JoinKind::FullOuter => {
                    expected_left.value_type =
                        expected_left.value_type.with_nullable(true).unwrap();
                    expected_right.value_type =
                        expected_right.value_type.with_nullable(true).unwrap();
                }
                _ => {}
            }
            node.output_columns = vec![expected_left, expected_right];
            node.children = vec![left, right];
            let node = attach_arena(node, Arc::new(ScalarArena::new()));
            materialize_physical_plan(&node).expect("bridge should convert")
        };

        let left_outer = convert(JoinKind::LeftOuter);
        assert!(!left_outer.output_columns[0].value_type.nullable());
        assert!(left_outer.output_columns[1].value_type.nullable());

        let right_outer = convert(JoinKind::RightOuter);
        assert!(right_outer.output_columns[0].value_type.nullable());
        assert!(!right_outer.output_columns[1].value_type.nullable());

        let full_outer = convert(JoinKind::FullOuter);
        assert!(full_outer.output_columns[0].value_type.nullable());
        assert!(full_outer.output_columns[1].value_type.nullable());
        assert_eq!(
            full_outer.output_columns[0].column_id,
            left_column.column_id
        );
        assert_eq!(
            full_outer.output_columns[1].column_id,
            right_column.column_id
        );
    }

    #[test]
    fn physical_self_join_preserves_repeated_output_occurrences() {
        let shared = output_column(41, "shared_key");
        let mut left = raw_values_node();
        left.output_columns = vec![shared.clone()];
        let mut right = raw_values_node();
        right.output_columns = vec![shared.clone()];
        let mut node = base_node(Operator::PhysicalHashJoin(PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![],
            other_condition: None,
            build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
            distribution: JoinDistribution::Broadcast,
        }));
        node.output_columns = vec![shared.clone(), shared.clone()];
        node.children = vec![left, right];
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = super::super::to_physical_plan(&node)
            .expect("exact duplicate occurrences must survive the public bridge");
        assert_output_columns_eq(&physical.output_columns, &[shared.clone(), shared.clone()]);
        let PhysicalPlanKind::HashJoin(join) = physical.kind else {
            panic!("expected HashJoin");
        };
        assert_output_columns_eq(&join.output_columns, &[shared.clone(), shared]);
    }

    #[test]
    fn set_op_child_output_metadata_mismatch_is_rejected() {
        let mut node = base_node(Operator::PhysicalUnion(UnionOp {
            all: true,
            output_columns: vec![output_column(1, "u")],
            child_output_columns: vec![vec![output_column(2, "c0")]],
        }));
        node.children.push(raw_values_node());
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let err = materialize_physical_plan(&node).expect_err("set-op metadata must match");
        assert!(err.contains("PhysicalUnion"));
        assert!(err.contains("child_output_columns metadata expected 2 entries, got 1"));
    }

    #[test]
    fn set_op_empty_child_output_metadata_is_rejected() {
        let mut node = base_node(Operator::PhysicalUnion(UnionOp {
            all: true,
            output_columns: vec![output_column(1, "u")],
            child_output_columns: vec![],
        }));
        node.children.push(raw_values_node());
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let err = materialize_physical_plan(&node).expect_err("set-op metadata is required");
        assert!(err.contains("PhysicalUnion"));
        assert!(err.contains("child_output_columns metadata expected 2 entries, got 0"));
    }

    #[test]
    fn bridge_translates_union_distinct_to_setop_marker() {
        let output_columns = vec![output_column(1, "u")];
        let child_output_columns = vec![vec![output_column(2, "c0")], vec![output_column(3, "c1")]];
        let mut node = base_node(Operator::PhysicalUnion(UnionOp {
            all: false,
            output_columns: output_columns.clone(),
            child_output_columns: child_output_columns.clone(),
        }));
        node.children.push(raw_values_node());
        node.children.push(raw_values_node());
        let node = attach_arena(node, Arc::new(ScalarArena::new()));

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let PhysicalPlanKind::SetOp(set_op) = physical.kind else {
            panic!("expected SetOp");
        };
        assert_eq!(set_op.kind, PlanSetOpKind::UnionDistinct);
        assert_output_columns_eq(&set_op.output_columns, &output_columns);
        assert_eq!(
            set_op.child_output_columns.len(),
            child_output_columns.len()
        );
        for (actual, expected) in set_op
            .child_output_columns
            .iter()
            .zip(child_output_columns.iter())
        {
            assert_output_columns_eq(actual, expected);
        }
        assert_eq!(physical.children.len(), 2);
        assert!(
            physical
                .children
                .iter()
                .all(|child| matches!(child.kind, PhysicalPlanKind::Values(_)))
        );
    }

    #[test]
    fn frozen_cost_stats_are_copied_without_recompute() {
        let mut node = values_node();
        node.explain_stats.cost_estimate = Some(CostEstimate {
            cpu_cost: 11.0,
            memory_cost: 12.0,
            network_cost: 13.0,
        });
        node.explain_stats.broadcast_decision = None;

        let physical = materialize_physical_plan(&node).expect("bridge should convert");
        let cost = physical
            .stats
            .cost_estimate
            .expect("cost estimate should be copied");
        assert_eq!(cost.cpu_cost, 11.0);
        assert_eq!(cost.memory_cost, 12.0);
        assert_eq!(cost.network_cost, 13.0);
        assert!(physical.stats.broadcast_decision.is_none());
    }

    #[test]
    fn bridge_rejects_logical_operator() {
        let node = attach_arena(
            base_node(Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            })),
            Arc::new(ScalarArena::new()),
        );

        let err = materialize_physical_plan(&node).expect_err("logical op should be rejected");
        assert!(err.contains("Bridge 2a expected a physical operator"));
    }

    #[test]
    fn bridge_rejects_unbound_project_column_before_distributed_build() {
        let input_id = ColumnId::new_for_test(1);
        let missing_id = ColumnId::new_for_test(99);
        let output_id = ColumnId::new_for_test(2);
        let mut scalars = ScalarArena::new();
        let items = intern_project_items(
            &mut scalars,
            &[ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: missing_id,
                        qualifier: None,
                        column: "missing".to_string(),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                },
                output_name: "p".to_string(),
                output_column_id: output_id,
            }],
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let input = OutputColumn {
            column_id: input_id,
            name: "v".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: false,
        };
        let output = OutputColumn {
            column_id: output_id,
            name: "p".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: false,
        };
        let mut plan = OptimizedOperatorNode {
            op: Operator::PhysicalProject(ProjectOp {
                items,
                output_qualifier: None,
            }),
            children: vec![OptimizedOperatorNode {
                op: Operator::PhysicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![input.clone()],
                }),
                children: vec![],
                output_columns: vec![input],
                stats: Statistics::default(),
                explain_stats: Default::default(),
                execution_props: PlanExecutionProps::default(),
            }],
            output_columns: vec![output],
            stats: Statistics::default(),
            explain_stats: Default::default(),
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));

        let err = super::super::to_physical_plan(&plan)
            .expect_err("unbound project ColumnId must fail at optimizer bridge");
        assert!(
            err.contains("not produced by child scope"),
            "unexpected err={err}"
        );
    }
}
