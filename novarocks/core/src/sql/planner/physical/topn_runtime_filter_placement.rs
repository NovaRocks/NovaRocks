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

//! Placement tests for Aggregate TopN runtime-filter intents.

use std::num::NonZeroU32;

use arrow::datatypes::DataType;

use crate::runtime_filter::model::contract::{NullOrder, SortDirection};
use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::planner::physical::runtime_filter::{
    AggregateTopNRuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{
    AggMode, PhysicalPlanKind, PhysicalPlanNode, PlanSetOpKind, TopNPhase,
};

struct TopNPlacementProof {
    producer_path: Vec<usize>,
    build_intent: AggregateTopNRuntimeFilterBuildIntent,
    probes: Vec<ProvenSourceProbe>,
}

struct ProvenSourceProbe {
    node_path: Vec<usize>,
    probe_expr: TypedExpr,
}

pub(super) fn place_aggregate_topn_runtime_filters(
    root: &mut PhysicalPlanNode,
    next_filter_id: &mut i32,
    max_count: usize,
) {
    let mut candidates = Vec::new();
    collect_topn_candidates(root, &mut Vec::new(), &mut candidates);

    for candidate_path in candidates {
        let Some(mut proof) = prove_topn_placement(root, &candidate_path) else {
            continue;
        };
        let Ok(filter_id) = usize::try_from(*next_filter_id) else {
            return;
        };
        if filter_id >= max_count {
            return;
        }
        proof.build_intent.filter_id = *next_filter_id;
        let probe_paths = proof
            .probes
            .iter()
            .map(|probe| {
                let mut node_path = candidate_path.clone();
                node_path.extend_from_slice(&probe.node_path);
                node_path
            })
            .collect::<Vec<_>>();
        let mut producer_path = candidate_path.clone();
        producer_path.extend_from_slice(&proof.producer_path);
        if probe_paths
            .iter()
            .any(|path| node_at_path(root, path).is_none())
            || !matches!(
                node_at_path(root, &producer_path).map(|node| &node.kind),
                Some(PhysicalPlanKind::HashAggregate(_))
            )
        {
            return;
        }
        for (probe, node_path) in proof.probes.iter().zip(probe_paths) {
            let node = node_at_path_mut(root, &node_path)
                .expect("validated Aggregate TopN probe path must remain stable");
            node.probe_runtime_filters.push(RuntimeFilterProbeIntent {
                filter_id: *next_filter_id,
                probe_expr: probe.probe_expr.clone(),
            });
        }
        let producer = node_at_path_mut(root, &producer_path)
            .expect("validated Aggregate TopN producer path must remain stable");
        let PhysicalPlanKind::HashAggregate(aggregate) = &mut producer.kind else {
            unreachable!("validated Aggregate TopN producer must remain an aggregate");
        };
        aggregate
            .topn_runtime_filter_builds
            .push(proof.build_intent);
        *next_filter_id += 1;
    }
}

fn collect_topn_candidates(
    node: &PhysicalPlanNode,
    path: &mut Vec<usize>,
    out: &mut Vec<Vec<usize>>,
) {
    if matches!(node.kind, PhysicalPlanKind::TopN(_)) {
        out.push(path.clone());
    }
    for (index, child) in node.children.iter().enumerate() {
        path.push(index);
        collect_topn_candidates(child, path, out);
        path.pop();
    }
}

fn prove_topn_placement(
    root: &PhysicalPlanNode,
    candidate_path: &[usize],
) -> Option<TopNPlacementProof> {
    let topn_node = node_at_path(root, candidate_path)?;
    let PhysicalPlanKind::TopN(topn) = &topn_node.kind else {
        return None;
    };
    if topn.phase != TopNPhase::Partial || !topn.is_split || topn.items.len() != 1 {
        return None;
    }
    let limit = topn.limit.and_then(|limit| u32::try_from(limit).ok())?;
    let limit = NonZeroU32::new(limit)?;
    if !matches!(topn.offset, None | Some(0)) {
        return None;
    }
    let order_expr = &topn.items[0].expr;
    if !is_supported_topn_key_type(&order_expr.data_type) {
        return None;
    }
    let direction = if topn.items[0].asc {
        SortDirection::Ascending
    } else {
        SortDirection::Descending
    };
    let null_order = if topn.items[0].nulls_first {
        NullOrder::First
    } else {
        NullOrder::Last
    };
    if topn_node.children.len() != 1 {
        return None;
    }
    let aggregate_node = &topn_node.children[0];
    let PhysicalPlanKind::HashAggregate(aggregate) = &aggregate_node.kind else {
        return None;
    };
    if aggregate.mode != AggMode::Local
        || aggregate_node.children.len() != 1
        || aggregate.group_by.len() != 1
        || aggregate.output_layout.group_key_columns.len() != 1
    {
        return None;
    }
    let group_output = &aggregate.output_layout.group_key_columns[0];
    if !is_exact_column_ref(order_expr, group_output)
        || aggregate_node
            .output_columns
            .iter()
            .filter(|column| is_same_column(column, group_output))
            .count()
            != 1
        || aggregate
            .output_columns
            .iter()
            .filter(|column| is_same_column(column, group_output))
            .count()
            != 1
    {
        return None;
    }
    let group_input = bind_exact_column_ref(
        &aggregate.group_by[0],
        &aggregate_node.children.first()?.output_columns,
    )?;
    let probes = trace_exact_source_probes(
        aggregate_node.children.first()?,
        &group_input,
        &mut vec![0, 0],
    )?;
    if probes.is_empty() {
        return None;
    }
    Some(TopNPlacementProof {
        producer_path: vec![0],
        build_intent: AggregateTopNRuntimeFilterBuildIntent {
            filter_id: -1,
            group_key_expr: group_input,
            group_key_ordinal: 0,
            limit,
            direction,
            null_order,
        },
        probes,
    })
}

fn trace_exact_source_probes(
    node: &PhysicalPlanNode,
    probe_expr: &TypedExpr,
    node_path: &mut Vec<usize>,
) -> Option<Vec<ProvenSourceProbe>> {
    let probe_expr = bind_exact_column_ref(probe_expr, &node.output_columns)?;
    if node.children.is_empty() {
        if !matches!(
            node.kind,
            PhysicalPlanKind::Scan(_) | PhysicalPlanKind::Values(_)
        ) {
            return None;
        }
        return Some(vec![ProvenSourceProbe {
            node_path: node_path.clone(),
            probe_expr,
        }]);
    }
    match &node.kind {
        PhysicalPlanKind::Filter(_) | PhysicalPlanKind::Redistribute(_)
            if node.children.len() == 1 =>
        {
            node_path.push(0);
            let probes = trace_exact_source_probes(&node.children[0], &probe_expr, node_path);
            node_path.pop();
            probes
        }
        PhysicalPlanKind::Project(project) if node.children.len() == 1 => {
            let ExprKind::ColumnRef { column_id, .. } = &probe_expr.kind else {
                return None;
            };
            let item = project
                .items
                .iter()
                .find(|item| item.output_column_id == *column_id)?;
            let input_expr = bind_exact_column_ref(&item.expr, &node.children[0].output_columns)?;
            if !same_column_ref(&probe_expr, &input_expr) {
                return None;
            }
            node_path.push(0);
            let probes = trace_exact_source_probes(&node.children[0], &input_expr, node_path);
            node_path.pop();
            probes
        }
        PhysicalPlanKind::SetOp(set_op)
            if set_op.kind == PlanSetOpKind::UnionAll
                && set_op.child_output_columns.len() == node.children.len() =>
        {
            let target_index = node
                .output_columns
                .iter()
                .position(|column| is_exact_column_ref(&probe_expr, column))?;
            let mut probes = Vec::new();
            for (child_index, child) in node.children.iter().enumerate() {
                let branch_column = set_op.child_output_columns[child_index].get(target_index)?;
                if branch_column.data_type != probe_expr.data_type
                    || branch_column.nullable != probe_expr.nullable
                {
                    return None;
                }
                let branch_expr = TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: branch_column.column_id,
                        qualifier: None,
                        column: branch_column.name.clone(),
                    },
                    data_type: branch_column.data_type.clone(),
                    nullable: branch_column.nullable,
                };
                let branch_expr = bind_exact_column_ref(&branch_expr, &child.output_columns)?;
                node_path.push(child_index);
                let branch_probes = trace_exact_source_probes(child, &branch_expr, node_path)?;
                node_path.pop();
                probes.extend(branch_probes);
            }
            Some(probes)
        }
        _ => None,
    }
}

fn bind_exact_column_ref(expr: &TypedExpr, columns: &[OutputColumn]) -> Option<TypedExpr> {
    let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
        return None;
    };
    let column = columns
        .iter()
        .find(|column| column.column_id == *column_id)?;
    if !is_exact_column_ref(expr, column) {
        return None;
    }
    Some(expr.clone())
}

fn is_exact_column_ref(expr: &TypedExpr, column: &OutputColumn) -> bool {
    matches!(&expr.kind, ExprKind::ColumnRef { column_id, .. } if *column_id == column.column_id)
        && expr.data_type == column.data_type
        && expr.nullable == column.nullable
}

fn same_column_ref(left: &TypedExpr, right: &TypedExpr) -> bool {
    match (&left.kind, &right.kind) {
        (
            ExprKind::ColumnRef {
                column_id: left_id, ..
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                ..
            },
        ) => {
            left_id == right_id
                && left.data_type == right.data_type
                && left.nullable == right.nullable
        }
        _ => false,
    }
}

fn is_same_column(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.data_type == right.data_type
        && left.nullable == right.nullable
}

fn is_supported_topn_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::FixedSizeBinary(16)
    )
}

fn node_at_path<'a>(root: &'a PhysicalPlanNode, path: &[usize]) -> Option<&'a PhysicalPlanNode> {
    let mut node = root;
    for index in path {
        node = node.children.get(*index)?;
    }
    Some(node)
}

fn node_at_path_mut<'a>(
    root: &'a mut PhysicalPlanNode,
    path: &[usize],
) -> Option<&'a mut PhysicalPlanNode> {
    let mut node = root;
    for index in path {
        node = node.children.get_mut(*index)?;
    }
    Some(node)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{is_exact_column_ref, place_aggregate_topn_runtime_filters};
    use crate::runtime_filter::model::contract::{NullOrder, SortDirection};
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::JoinKind;
    use crate::sql::optimizer::options::{
        SessionOptimizerSettings, with_session_optimizer_settings,
    };
    use crate::sql::planner::payload::{
        PlanCTEConsumeNode, PlanCTEProduceNode, PlanFilterNode, PlanProjectNode, PlanValuesNode,
        PlanWindowNode,
    };
    use crate::sql::planner::physical::runtime_filter::AggregateTopNRuntimeFilterBuildIntent;
    use crate::sql::planner::physical::runtime_filter_placement::{
        RUNTIME_FILTER_RULE, place_runtime_filters,
    };
    use crate::sql::planner::physical::{
        AggMode, AggregateOutputLayout, HashSource, PhysicalHashAggregateNode,
        PhysicalHashJoinEqCondition, PhysicalHashJoinNode, PhysicalPlanKind, PhysicalPlanNode,
        PhysicalPlanStats, PhysicalSetOpNode, PhysicalTopNNode, PlanSetOpKind, RedistributeMode,
        RedistributeNode, TopNPhase,
    };
    use crate::sql::planner::physical::{JoinDistribution, JoinExecutionMode};

    #[test]
    fn places_single_key_partial_topn_on_local_aggregate_atomically() {
        let mut plan = eligible_topn_plan(DataType::Int32, true, false);
        let mut next_filter_id = 0;

        place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1024);

        assert_eq!(aggregate_topn_builds(&plan).len(), 1);
        assert_eq!(source_probes(&plan).len(), 1);
        assert_eq!(aggregate_topn_builds(&plan)[0].filter_id, 0);
        assert_eq!(source_probes(&plan)[0].filter_id, 0);
        assert_eq!(next_filter_id, 1);
    }

    #[test]
    fn preserves_supported_key_type_and_order_contracts() {
        let cases = [
            (
                DataType::Int8,
                true,
                false,
                SortDirection::Ascending,
                NullOrder::Last,
            ),
            (
                DataType::Int16,
                true,
                true,
                SortDirection::Ascending,
                NullOrder::First,
            ),
            (
                DataType::Int32,
                false,
                false,
                SortDirection::Descending,
                NullOrder::Last,
            ),
            (
                DataType::Int64,
                false,
                true,
                SortDirection::Descending,
                NullOrder::First,
            ),
            (
                DataType::Utf8,
                true,
                false,
                SortDirection::Ascending,
                NullOrder::Last,
            ),
            (
                DataType::Date32,
                false,
                true,
                SortDirection::Descending,
                NullOrder::First,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
                false,
                SortDirection::Ascending,
                NullOrder::Last,
            ),
            (
                DataType::Decimal128(18, 2),
                false,
                true,
                SortDirection::Descending,
                NullOrder::First,
            ),
            (
                DataType::FixedSizeBinary(16),
                true,
                false,
                SortDirection::Ascending,
                NullOrder::Last,
            ),
        ];

        for (data_type, asc, nulls_first, direction, null_order) in cases {
            let mut plan = eligible_topn_plan(data_type, asc, nulls_first);
            let mut next_filter_id = 0;

            place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1);

            let intent = &aggregate_topn_builds(&plan)[0];
            assert_eq!(intent.direction, direction);
            assert_eq!(intent.null_order, null_order);
            assert_eq!(source_probes(&plan).len(), 1);
        }
    }

    #[test]
    fn traces_only_exact_passthrough_paths_to_every_union_all_source() {
        let mut plan = eligible_topn_plan(DataType::Int64, true, false);
        let aggregate = aggregate_child_mut(&mut plan);
        aggregate.children[0] = filter_node(project_node(redistribute_node(union_all_node(
            leaf(1, "left", DataType::Int64, false),
            leaf(1, "right", DataType::Int64, false),
        ))));
        let mut next_filter_id = 0;

        place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1024);

        assert_eq!(aggregate_topn_builds(&plan).len(), 1);
        assert_eq!(source_probes(&plan).len(), 2);
        assert!(
            source_probes(&plan)
                .iter()
                .all(|probe| probe.filter_id == 0)
        );
    }

    #[test]
    fn remaps_union_all_probe_to_each_distinct_branch_column() {
        let mut plan = eligible_topn_plan(DataType::Int64, true, false);
        let aggregate = aggregate_child_mut(&mut plan);
        aggregate.children[0] = union_all_with_distinct_column_ids(
            leaf(11, "left_key", DataType::Int64, false),
            leaf(12, "right_key", DataType::Int64, false),
            output(10, "union_key", DataType::Int64, false),
        );
        let PhysicalPlanKind::HashAggregate(aggregate_kind) = &mut aggregate.kind else {
            panic!("expected aggregate")
        };
        aggregate_kind.group_by[0] = column(10, "union_key", DataType::Int64, false);
        let mut next_filter_id = 0;

        place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1024);

        assert_eq!(aggregate_topn_builds(&plan).len(), 1);
        let probes = source_probes(&plan);
        assert_eq!(probes.len(), 2);
        assert!(is_exact_column_ref(
            &probes[0].probe_expr,
            &output(11, "left_key", DataType::Int64, false),
        ));
        assert!(is_exact_column_ref(
            &probes[1].probe_expr,
            &output(12, "right_key", DataType::Int64, false),
        ));
    }

    #[test]
    fn rejects_ineligible_or_inexact_shapes_atomically() {
        let cases: Vec<(&str, Box<dyn Fn(&mut PhysicalPlanNode)>)> = vec![
            (
                "zero limit",
                Box::new(|plan| topn_mut(plan).limit = Some(0)),
            ),
            (
                "limit above u32",
                Box::new(|plan| topn_mut(plan).limit = Some(i64::from(u32::MAX) + 1)),
            ),
            (
                "nonzero offset",
                Box::new(|plan| topn_mut(plan).offset = Some(1)),
            ),
            (
                "multiple order keys",
                Box::new(|plan| {
                    topn_mut(plan).items.push(sort_item(
                        column(2, "second", DataType::Int64, false),
                        true,
                        false,
                    ))
                }),
            ),
            (
                "final topn",
                Box::new(|plan| topn_mut(plan).phase = TopNPhase::Final),
            ),
            (
                "unsplit topn",
                Box::new(|plan| topn_mut(plan).is_split = false),
            ),
            (
                "nonlocal aggregate",
                Box::new(|plan| aggregate_mut(plan).mode = AggMode::Global),
            ),
            (
                "multiple group keys",
                Box::new(|plan| {
                    aggregate_mut(plan)
                        .group_by
                        .push(column(2, "second", DataType::Int64, false))
                }),
            ),
            (
                "order expression is not a column",
                Box::new(|plan| {
                    topn_mut(plan).items[0].expr = TypedExpr {
                        kind: ExprKind::Cast {
                            expr: Box::new(column(1, "key", DataType::Int64, false)),
                            target: DataType::Int64,
                        },
                        data_type: DataType::Int64,
                        nullable: false,
                    }
                }),
            ),
            (
                "order aggregate output",
                Box::new(|plan| {
                    topn_mut(plan).items[0].expr = column(9, "sum", DataType::Int64, false)
                }),
            ),
            (
                "float key",
                Box::new(|plan| replace_key_type(plan, DataType::Float64)),
            ),
            (
                "boolean key",
                Box::new(|plan| replace_key_type(plan, DataType::Boolean)),
            ),
            (
                "project cast",
                Box::new(wrap_aggregate_input_in_cast_project),
            ),
            (
                "column id drift",
                Box::new(|plan| {
                    aggregate_mut(plan).output_columns[0].column_id = ColumnId::new_for_test(99)
                }),
            ),
            (
                "type drift",
                Box::new(|plan| aggregate_mut(plan).output_columns[0].data_type = DataType::Int32),
            ),
            (
                "nullability drift",
                Box::new(|plan| aggregate_mut(plan).output_columns[0].nullable = true),
            ),
            (
                "window boundary",
                Box::new(|plan| {
                    aggregate_child_mut(plan).children[0].kind =
                        PhysicalPlanKind::Window(PlanWindowNode {
                            window_exprs: Vec::new(),
                            output_columns: Vec::new(),
                        })
                }),
            ),
            (
                "hash join boundary",
                Box::new(|plan| {
                    aggregate_child_mut(plan).children[0] = unsupported_hash_join_node()
                }),
            ),
            (
                "cte producer boundary",
                Box::new(|plan| {
                    aggregate_child_mut(plan).children[0] =
                        cte_producer_node(leaf(1, "key", DataType::Int64, false))
                }),
            ),
            (
                "cte consumer boundary",
                Box::new(|plan| aggregate_child_mut(plan).children[0] = cte_consumer_node()),
            ),
            (
                "union distinct",
                Box::new(|plan| {
                    aggregate_child_mut(plan).children[0] = union_distinct_node(
                        leaf(1, "left", DataType::Int64, false),
                        leaf(1, "right", DataType::Int64, false),
                    )
                }),
            ),
            (
                "unreachable union source",
                Box::new(|plan| {
                    aggregate_child_mut(plan).children[0] =
                        union_all_node(leaf(1, "left", DataType::Int64, false), values_node())
                }),
            ),
        ];

        for (name, mutate) in cases {
            let mut plan = eligible_topn_plan(DataType::Int64, true, false);
            mutate(&mut plan);
            let mut next_filter_id = 7;

            place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1024);

            assert!(aggregate_topn_builds(&plan).is_empty(), "{name}");
            assert!(source_probes(&plan).is_empty(), "{name}");
            assert_eq!(next_filter_id, 7, "{name}");
        }
    }

    #[test]
    fn rejects_local_aggregate_with_multiple_inputs_atomically() {
        let mut plan = eligible_topn_plan(DataType::Int64, true, false);
        aggregate_child_mut(&mut plan).children.push(leaf(
            1,
            "second_input",
            DataType::Int64,
            false,
        ));
        let mut next_filter_id = 0;

        place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1024);

        assert!(aggregate_topn_builds(&plan).is_empty());
        assert!(source_probes(&plan).is_empty());
        assert_eq!(next_filter_id, 0);
    }

    #[test]
    fn leaves_plan_unannotated_when_budget_is_exhausted() {
        let mut plan = eligible_topn_plan(DataType::Int64, true, false);
        let mut next_filter_id = 1;

        place_aggregate_topn_runtime_filters(&mut plan, &mut next_filter_id, 1);

        assert!(aggregate_topn_builds(&plan).is_empty());
        assert!(source_probes(&plan).is_empty());
        assert_eq!(next_filter_id, 1);
    }

    #[test]
    fn session_switches_disable_both_join_and_aggregate_topn_annotations() {
        for settings in [
            SessionOptimizerSettings {
                enable_global_runtime_filter: Some(false),
                ..SessionOptimizerSettings::default()
            },
            SessionOptimizerSettings {
                enable_global_runtime_filter: Some(true),
                disabled_rules: vec![RUNTIME_FILTER_RULE.to_string()],
                ..SessionOptimizerSettings::default()
            },
        ] {
            let mut plan = join_with_eligible_topn();
            with_session_optimizer_settings(settings, || place_runtime_filters(&mut plan));

            assert!(hash_join(&plan).build_runtime_filters.is_empty());
            assert!(aggregate_topn_builds(&plan.children[0]).is_empty());
            assert!(source_probes(&plan).is_empty());
        }
    }

    #[test]
    fn joins_allocate_before_aggregate_topn_in_one_query_local_id_space() {
        let mut plan = join_with_eligible_topn();
        let settings = SessionOptimizerSettings {
            enable_global_runtime_filter: Some(true),
            ..SessionOptimizerSettings::default()
        };

        with_session_optimizer_settings(settings, || place_runtime_filters(&mut plan));

        assert_eq!(hash_join(&plan).build_runtime_filters[0].filter_id, 0);
        assert_eq!(aggregate_topn_builds(&plan.children[0])[0].filter_id, 1);
        let ids = source_probes(&plan)
            .into_iter()
            .map(|probe| probe.filter_id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids, std::collections::BTreeSet::from([0, 1]));
    }

    fn eligible_topn_plan(data_type: DataType, asc: bool, nulls_first: bool) -> PhysicalPlanNode {
        let key = output(1, "key", data_type.clone(), false);
        let source = leaf(1, "key", data_type.clone(), false);
        let aggregate = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Local,
                group_by: vec![column(1, "key", data_type.clone(), false)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: AggregateOutputLayout::new(vec![key.clone()], Vec::new()),
                output_columns: vec![key.clone()],
                topn_runtime_filter_builds: Vec::new(),
            })),
            children: vec![source],
            output_columns: vec![key.clone()],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        };
        PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(
                    column(1, "key", data_type, false),
                    asc,
                    nulls_first,
                )],
                limit: Some(10),
                offset: Some(0),
                phase: TopNPhase::Partial,
                is_split: true,
            }),
            children: vec![aggregate],
            output_columns: vec![key],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn join_with_eligible_topn() -> PhysicalPlanNode {
        let probe = eligible_topn_plan(DataType::Int64, true, false);
        let build = leaf(2, "build_key", DataType::Int64, false);
        let output_columns = probe
            .output_columns
            .iter()
            .chain(build.output_columns.iter())
            .cloned()
            .collect::<Vec<_>>();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: column(1, "key", DataType::Int64, false),
                    right: column(2, "build_key", DataType::Int64, false),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: Some(JoinExecutionMode::Broadcast),
                build_runtime_filters: Vec::new(),
                output_columns: output_columns.clone(),
            })),
            children: vec![probe, build],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn aggregate_topn_builds(plan: &PhysicalPlanNode) -> &[AggregateTopNRuntimeFilterBuildIntent] {
        let PhysicalPlanKind::HashAggregate(aggregate) = &plan.children[0].kind else {
            panic!("expected aggregate")
        };
        &aggregate.topn_runtime_filter_builds
    }

    fn hash_join(plan: &PhysicalPlanNode) -> &PhysicalHashJoinNode {
        let PhysicalPlanKind::HashJoin(join) = &plan.kind else {
            panic!("expected hash join")
        };
        join
    }

    fn source_probes(
        plan: &PhysicalPlanNode,
    ) -> Vec<&crate::sql::planner::physical::runtime_filter::RuntimeFilterProbeIntent> {
        fn collect<'a>(
            node: &'a PhysicalPlanNode,
            probes: &mut Vec<
                &'a crate::sql::planner::physical::runtime_filter::RuntimeFilterProbeIntent,
            >,
        ) {
            probes.extend(node.probe_runtime_filters.iter());
            for child in &node.children {
                collect(child, probes);
            }
        }
        let mut probes = Vec::new();
        collect(plan, &mut probes);
        probes
    }

    fn topn_mut(plan: &mut PhysicalPlanNode) -> &mut PhysicalTopNNode {
        let PhysicalPlanKind::TopN(topn) = &mut plan.kind else {
            panic!("expected topn")
        };
        topn
    }

    fn aggregate_mut(plan: &mut PhysicalPlanNode) -> &mut PhysicalHashAggregateNode {
        let PhysicalPlanKind::HashAggregate(aggregate) = &mut plan.children[0].kind else {
            panic!("expected aggregate")
        };
        aggregate
    }

    fn aggregate_child_mut(plan: &mut PhysicalPlanNode) -> &mut PhysicalPlanNode {
        &mut plan.children[0]
    }

    fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn output(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn sort_item(expr: TypedExpr, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr,
            asc,
            nulls_first,
        }
    }

    fn leaf(id: u32, name: &str, data_type: DataType, nullable: bool) -> PhysicalPlanNode {
        let output = output(id, name, data_type, nullable);
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![output.clone()],
            }),
            children: Vec::new(),
            output_columns: vec![output],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn values_node() -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
            children: Vec::new(),
            output_columns: Vec::new(),
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn filter_node(child: PhysicalPlanNode) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: column(1, "key", DataType::Int64, false),
            }),
            output_columns: child.output_columns.clone(),
            children: vec![child],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn project_node(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let key = child.output_columns[0].clone();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column(1, &key.name, key.data_type.clone(), key.nullable),
                    output_name: key.name.clone(),
                    output_column_id: key.column_id,
                }],
                output_qualifier: None,
            }),
            output_columns: vec![key],
            children: vec![child],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn cast_project_node(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let key = child.output_columns[0].clone();
        let expr = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column(1, &key.name, key.data_type.clone(), key.nullable)),
                target: DataType::Int32,
            },
            data_type: DataType::Int32,
            nullable: key.nullable,
        };
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr,
                    output_name: key.name.clone(),
                    output_column_id: key.column_id,
                }],
                output_qualifier: None,
            }),
            output_columns: vec![output(1, &key.name, DataType::Int32, key.nullable)],
            children: vec![child],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn wrap_aggregate_input_in_cast_project(plan: &mut PhysicalPlanNode) {
        let aggregate = aggregate_child_mut(plan);
        let child = aggregate.children.remove(0);
        aggregate.children.push(cast_project_node(child));
    }

    fn unsupported_hash_join_node() -> PhysicalPlanNode {
        let left = leaf(1, "key", DataType::Int64, false);
        let right = leaf(2, "other", DataType::Int64, false);
        let output_columns = left
            .output_columns
            .iter()
            .chain(right.output_columns.iter())
            .cloned()
            .collect::<Vec<_>>();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: column(1, "key", DataType::Int64, false),
                    right: column(2, "other", DataType::Int64, false),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: Some(JoinExecutionMode::Broadcast),
                build_runtime_filters: Vec::new(),
                output_columns: output_columns.clone(),
            })),
            children: vec![left, right],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn cte_producer_node(child: PhysicalPlanNode) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(PlanCTEProduceNode {
                cte_id: 1,
                output_columns: child.output_columns.clone(),
            }),
            children: vec![child.clone()],
            output_columns: child.output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn cte_consumer_node() -> PhysicalPlanNode {
        let output_columns = vec![output(1, "key", DataType::Int64, false)];
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(PlanCTEConsumeNode {
                cte_id: 1,
                alias: "cte".to_string(),
                output_columns: output_columns.clone(),
                producer_column_ids: vec![ColumnId::new_for_test(1)],
            }),
            children: Vec::new(),
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn redistribute_node(child: PhysicalPlanNode) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(1)],
                    source: HashSource::ShuffleAgg,
                },
                partition_exprs: Vec::new(),
                output_columns: child.output_columns.clone(),
            }),
            output_columns: child.output_columns.clone(),
            children: vec![child],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn union_all_node(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        set_op_node(PlanSetOpKind::UnionAll, left, right)
    }

    fn union_all_with_distinct_column_ids(
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        output_column: OutputColumn,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: vec![output_column.clone()],
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            children: vec![left, right],
            output_columns: vec![output_column],
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }
    fn union_distinct_node(left: PhysicalPlanNode, right: PhysicalPlanNode) -> PhysicalPlanNode {
        set_op_node(PlanSetOpKind::UnionDistinct, left, right)
    }
    fn set_op_node(
        kind: PlanSetOpKind,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
    ) -> PhysicalPlanNode {
        let output_columns = left.output_columns.clone();
        PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind,
                output_columns: output_columns.clone(),
                child_output_columns: vec![
                    left.output_columns.clone(),
                    right.output_columns.clone(),
                ],
            }),
            children: vec![left, right],
            output_columns,
            stats: stats(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn replace_key_type(plan: &mut PhysicalPlanNode, data_type: DataType) {
        topn_mut(plan).items[0].expr.data_type = data_type.clone();
        let aggregate = aggregate_mut(plan);
        aggregate.group_by[0].data_type = data_type.clone();
        aggregate.output_columns[0].data_type = data_type.clone();
        aggregate.output_layout.group_key_columns[0].data_type = data_type.clone();
        plan.output_columns[0].data_type = data_type;
    }

    fn stats() -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: crate::sql::planner::physical::PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }
}
