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
use super::*;

fn operation(
    kind: PlanSetOpKind,
    left: PhysicalPlanNode,
    right: PhysicalPlanNode,
    result: OutputColumn,
) -> PhysicalPlanNode {
    PhysicalPlanNode {
        kind: PhysicalPlanKind::SetOp(crate::planner::physical::PhysicalSetOpNode {
            kind,
            output_columns: vec![result.clone()],
            child_output_columns: vec![left.output_columns.clone(), right.output_columns.clone()],
        }),
        children: vec![left, right],
        output_columns: vec![result],
        stats: stats(),
        probe_runtime_filters: Vec::new(),
    }
}

fn hash_values(column: OutputColumn, rows: Vec<Vec<TypedExpr>>) -> PhysicalPlanNode {
    let column_id = column.column_id;
    redistribute(
        values(vec![column], rows),
        RedistributeMode::Hash {
            cols: vec![column_id],
            source: crate::planner::physical::HashSource::ShuffleJoin,
        },
    )
}

#[test]
fn comparison_repartitions_the_aligned_output_of_a_prepartitioned_set_branch() {
    let prior = operation(
        PlanSetOpKind::Except,
        hash_values(
            column(1, "a", DataType::Int32, false),
            vec![vec![literal_int32(7)]],
        ),
        hash_values(
            column(2, "b", DataType::Int32, false),
            vec![vec![literal_int32(9)]],
        ),
        column(3, "prior", DataType::Int32, false),
    );
    let current = operation(
        PlanSetOpKind::Intersect,
        prior,
        hash_values(
            column(4, "wider", DataType::Int64, false),
            vec![vec![literal_int(7)]],
        ),
        column(5, "result", DataType::Int64, false),
    );

    let plan = finish_for_test(&current).unwrap();
    let (fragment_id, node) = plan
        .fragments()
        .iter()
        .find_map(|(fragment_id, fragment)| {
            fragment.nodes().values().find_map(|node| {
                matches!(
                    &node.kind,
                    NodeKind::SetOp {
                        kind: SetOperationKind::Intersect,
                        ..
                    }
                )
                .then_some((fragment_id, node))
            })
        })
        .expect("outer INTERSECT");
    let edge = plan
        .edges()
        .values()
        .find(|edge| {
            edge.destination.fragment == *fragment_id && edge.destination.node == node.inputs[0]
        })
        .expect("re-shuffle of the left branch");
    let source = &plan.fragments()[&edge.source.fragment];
    let projection = &source.nodes()[&source.root()];
    assert!(matches!(&projection.kind, NodeKind::Project { .. }));
    let comparison = &source.nodes()[&projection.inputs[0]];
    assert!(matches!(
        &comparison.kind,
        NodeKind::SetOp {
            kind: SetOperationKind::Except,
            ..
        }
    ));
    let original = comparison.output.columns[0];
    let converted = projection.output.columns[0];
    assert_ne!(original, converted);
    assert_eq!(source.values()[&original].ty.data_type(), &DataType::Int32);
    assert_eq!(source.values()[&converted].ty.data_type(), &DataType::Int64);
    let Distribution::Hash { keys, .. } = &edge.partitioning.source else {
        panic!("comparison must hash the converted value");
    };
    assert_eq!(keys.as_ref(), &[converted]);
    assert_eq!(edge.source.projection.as_ref(), &[converted]);
}

#[test]
fn singleton_comparison_aligns_types_without_an_exchange() {
    let comparison = operation(
        PlanSetOpKind::Except,
        values(
            vec![column(1, "a", DataType::Int32, false)],
            vec![vec![literal_int32(7)]],
        ),
        values(
            vec![column(2, "b", DataType::Int64, false)],
            vec![vec![literal_int(9)]],
        ),
        column(3, "result", DataType::Int64, false),
    );
    let plan = finish_for_test(&comparison).unwrap();
    assert!(plan.edges().is_empty());
    let fragment = &plan.fragments()[&ROOT_FRAGMENT_ID];
    let set_op = &fragment.nodes()[&fragment.root()];
    let NodeKind::SetOp {
        kind: SetOperationKind::Except,
        input_mappings,
    } = &set_op.kind
    else {
        panic!("local EXCEPT must remain in its original singleton fragment");
    };
    assert_eq!(
        set_op.output_properties.distribution,
        Distribution::Singleton
    );
    assert_eq!(
        fragment.values()[&input_mappings[0][0]].ty.data_type(),
        &DataType::Int64
    );
    assert!(matches!(
        &fragment.nodes()[&set_op.inputs[0]].kind,
        NodeKind::Project { .. }
    ));
}

#[test]
fn all_constant_union_all_needs_no_branch_exchange() {
    let union = operation(
        PlanSetOpKind::UnionAll,
        values(
            vec![column(1, "a", DataType::Int64, false)],
            vec![vec![literal_int(7)]],
        ),
        values(
            vec![column(2, "b", DataType::Int64, false)],
            vec![vec![literal_int(9)]],
        ),
        column(3, "result", DataType::Int64, false),
    );
    let plan = finish_for_test(&union).unwrap();
    assert!(plan.edges().is_empty());
    assert_eq!(plan.fragments().len(), 1);
    let fragment = &plan.fragments()[&ROOT_FRAGMENT_ID];
    let node = &fragment.nodes()[&fragment.root()];
    assert_eq!(node.output_properties.distribution, Distribution::Singleton);
    assert_eq!(
        fragment.stage_placement(),
        &novarocks_physical_plan::StagePlacementPolicy::Singleton
    );
}

#[test]
fn split_union_branch_aligns_selected_null_at_the_sender() {
    let mut union = union_with_selected_variant_constant(1, false);
    let left = union.children.remove(0);
    let left_column = left.output_columns[0].column_id;
    union.children.insert(
        0,
        redistribute(
            left,
            RedistributeMode::Hash {
                cols: vec![left_column],
                source: crate::planner::physical::HashSource::ShuffleJoin,
            },
        ),
    );

    let plan = finish_for_test(&union).unwrap();
    let edge = plan
        .edges()
        .values()
        .find(|edge| edge.partitioning.source == Distribution::RoundRobin)
        .expect("singleton branch was split");
    let source = &plan.fragments()[&edge.source.fragment];
    let aligned = &source.values()[&edge.source.projection[0]];
    assert_eq!(aligned.ty, ValueType::new(DataType::Utf8, true));
    let ValueOrigin::Expr { node, root } = aligned.origin else {
        panic!("expected checked NULL to be retyped in the source projection");
    };
    let expr = source.root_expression(node, root).unwrap();
    assert!(
        checked_constant_for_test(&plan, expr)
            .is_null_observed(
                CompilePhase::Validate,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
    );
}
