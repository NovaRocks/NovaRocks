// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact grouped-state installation, independent use sites and fresh channels.
//! Existing Rows tests do not exercise intermediate call scope or state output.
//! The oracle is the declared physical binding/source and explicit key order,
//! not runtime arithmetic. Inputs/catalog are local; no process/fixture cleanup
//! or timing gate is needed. LocalCompiler owns this short affected-change lane.

use super::*;
use crate::expressions::checked_reference_tests::values_lowering_tests::{
    build, checked_pool, compile_options, providers as value_providers,
};
use arrow_array::{ArrayRef, Int64Array};
use novarocks_local_program::{ProgramAggregateCallScope, ProgramCallSite, ProgramStateTemplate};

fn grouped_package() -> (Arc<FragmentPackage>, PureEngineFunctionCatalog) {
    let catalog = count_catalog();
    let count = CountStar::bind(&catalog);
    let pool = checked_pool(
        Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef,
        true,
        ValueLogicalType::Physical,
    );
    let base = build(
        &[pool],
        &[vec![(0, 0), (0, 2)], vec![(0, 1), (0, 0)]],
        &[0, 0],
        false,
    );
    let mut builder = FragmentBuilder::new(base.fragment().id());
    for value in base.fragment().values().values() {
        builder.insert_value(value.clone()).unwrap();
    }
    for (_, expression) in base.fragment().expressions().iter() {
        builder.insert_expression(expression.clone()).unwrap();
    }
    for node in base.fragment().nodes().values() {
        builder.insert_node_unchecked(node.clone()).unwrap();
    }
    let values = base.fragment().root();
    let keys = base.fragment().nodes()[&values].output.columns.to_vec();
    let partial = NodeId::new(201);
    let grouped = NodeId::new(202);
    let final_aggregate = NodeId::new(203);
    let final_topn = NodeId::new(204);
    let sequence = AggregateSequenceId::new(41);
    let mut input = values;
    let mut prior = None;
    let mut intermediate = None;
    for (node, id, phase) in [
        (partial, 51, AggregatePhase::Partial { sequence }),
        (grouped, 52, AggregatePhase::Intermediate { sequence }),
        (final_aggregate, 53, AggregatePhase::Final { sequence }),
    ] {
        let mut groups = Vec::new();
        for key in &keys {
            let ty = builder.value(*key).unwrap().ty.clone();
            groups.push((
                builder
                    .add_expression(node, ty, ExprKind::Value(*key))
                    .unwrap(),
                *key,
            ));
        }
        let arguments = prior
            .map(|value| {
                let ty = builder.value(value).unwrap().ty.clone();
                Box::from([builder
                    .add_expression(node, ty, ExprKind::Value(value))
                    .unwrap()])
            })
            .unwrap_or_default();
        let output = builder
            .add_value(
                count.output_type(phase),
                if phase.produces_final_result() {
                    ValueOrigin::AggregateResult {
                        call: AggregateCallId::new(id),
                    }
                } else {
                    ValueOrigin::AggregateState {
                        call: AggregateCallId::new(id),
                        phase,
                    }
                },
            )
            .unwrap();
        let call = count.call(id, phase, arguments, output);
        let required = builder.node_output_properties(input).unwrap().clone();
        let mut produced = required.clone();
        produced.ordering = Box::default();
        let kind = if node == grouped {
            let order = [1usize, 0].map(|ordinal| SortExpr {
                expr: groups[ordinal].0,
                direction: if ordinal == 1 {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
                null_ordering: if ordinal == 1 {
                    NullOrdering::First
                } else {
                    NullOrdering::Last
                },
            });
            produced.ordering = order
                .iter()
                .zip([1usize, 0])
                .map(|(key, ordinal)| OrderingKey {
                    value: keys[ordinal],
                    direction: key.direction,
                    null_ordering: key.null_ordering,
                })
                .collect();
            intermediate = Some(output);
            NodeKind::TopN {
                order_by: Box::from(order),
                limit: 2,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(42),
                },
                reduction: TopNReduction::GroupedStates {
                    group_by: groups.into_boxed_slice(),
                    calls: Box::from([call]),
                    comparator:
                        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            }
        } else {
            NodeKind::Aggregate {
                group_by: groups.into_boxed_slice(),
                calls: Box::from([call]),
                grouping: if node == partial {
                    AggregateGrouping::Partial
                } else {
                    AggregateGrouping::Complete
                },
            }
        };
        let mut outputs = keys.clone();
        outputs.push(output);
        builder
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::from([input]),
                required_inputs: Box::from([required]),
                output_properties: produced,
                output: OutputPort {
                    node,
                    columns: outputs.into_boxed_slice(),
                },
                kind,
            })
            .unwrap();
        input = node;
        prior = Some(output);
    }
    let order = [1usize, 0].map(|ordinal| {
        let ty = builder.value(keys[ordinal]).unwrap().ty.clone();
        SortExpr {
            expr: builder
                .add_expression(final_topn, ty, ExprKind::Value(keys[ordinal]))
                .unwrap(),
            direction: if ordinal == 1 {
                SortDirection::Descending
            } else {
                SortDirection::Ascending
            },
            null_ordering: if ordinal == 1 {
                NullOrdering::First
            } else {
                NullOrdering::Last
            },
        }
    });
    builder
        .add_top_n(
            final_topn,
            final_aggregate,
            Box::from(order),
            2,
            0,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(42),
            },
        )
        .unwrap();
    let fragment = finish_with_requests(builder, final_topn, FragmentSink::Result);
    let (expression_uses, calls) = freeze_counts(&fragment, &catalog);
    let mut input = base.as_ref().clone().into_input();
    let output = fragment.nodes()[&final_topn].output.clone();
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: output.clone(),
        fields: output
            .columns
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: ResultValueDomain::Plain,
                name: format!("field-{ordinal}").into(),
                alias: None,
                value: *value,
                ty: fragment.values()[value].ty.clone(),
            })
            .collect(),
    });
    input.fragment = fragment;
    input.expression_uses = expression_uses;
    input.calls = calls;
    let package =
        Arc::new(FragmentPackage::try_new(input, package_admission(), &FixtureControl).unwrap());
    assert_ne!(intermediate.unwrap(), prior.unwrap());
    (package, catalog)
}

#[test]
fn grouped_topn_installs_intermediate_scope_independent_roots_and_fresh_state_channels() {
    let (package, catalog) = grouped_package();
    let program = compile_fragment(
        value_providers(Arc::clone(&package)),
        &catalog,
        compile_options(),
        &FixtureControl,
    )
    .unwrap();
    let (index, node) = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ProgramNodeKind::GroupedTopN { .. }))
        .unwrap();
    let id = ProgramNodeId::new(index);
    let ProgramNodeKind::GroupedTopN {
        input,
        group_by,
        order_by,
        rank_group_ordinals,
        functions,
        comparator,
        limit,
    } = node.kind()
    else {
        unreachable!()
    };
    assert_eq!(rank_group_ordinals, &[1, 0]);
    assert_eq!(*limit, 2);
    assert_eq!(
        *comparator,
        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1
    );
    assert!(!order_by[0].asc && order_by[0].nulls_first);
    assert!(order_by[1].asc && !order_by[1].nulls_first);
    assert_eq!(group_by.len(), 2);
    assert_eq!(functions.len(), 1);
    let site = ProgramCallSite::TopNState { node: id, call: 0 };
    let Some(ProgramStateTemplate::Aggregate { scope, kernel }) = program.state_template(site)
    else {
        panic!("exact grouped-state template");
    };
    assert_eq!(
        scope,
        ProgramAggregateCallScope::TopNState { node: id, call: 0 }
    );
    assert_eq!(
        kernel.contract().phase(),
        AggregateKernelPhase::Intermediate
    );
    assert_eq!(kernel.contract().call().selected().argument_types.len(), 0);
    assert_eq!(functions[0].inputs.len(), 1);
    let child = &program.graph().nodes()[input.index()];
    assert_ne!(
        node.output_layout().slots()[2],
        child.output_layout().slots()[2]
    );
    assert_eq!(
        node.output_layout().schema().field(2).data_type(),
        &kernel.contract().intermediate_type().data_type
    );
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let mut uses = Vec::new();
    for role in [
        ProgramNodeExpressionRole::TopNGroup { group: 0 },
        ProgramNodeExpressionRole::TopNGroup { group: 1 },
        ProgramNodeExpressionRole::TopNOrder { key: 0 },
        ProgramNodeExpressionRole::TopNOrder { key: 1 },
        ProgramNodeExpressionRole::TopNStateArgument {
            call: 0,
            argument: 0,
        },
    ] {
        let root = ProgramExpressionRootSite::Node { node: id, role };
        uses.push(snapshot.bindings().get(&root).copied().unwrap());
        assert!(
            matches!(novarocks_local_program::root_input_layout(program.graph(), root).unwrap(), novarocks_local_program::ProgramRootInput::Layout { node: source, role: ProgramChannelLayoutRole::NodeOutput } if source == *input)
        );
    }
    assert_eq!(
        uses.iter().collect::<std::collections::BTreeSet<_>>().len(),
        5
    );
}
