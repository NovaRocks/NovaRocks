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

//! Real compiled exchange/Partial/gather/Grouped stages and joined execution.
//! Local literal rows and original loopback ports supply declared inputs, not
//! Native process evidence. Execution owns this affected-change component lane.
//! The independent complete-group oracle protects all cross-instance states;
//! actual per-family pipeline DOP protects parallel placement without a source
//! token/count guard, scheduler assumption, extra controller or performance gate.

use super::super::aggregate_fixture::{
    Bound, LoopbackTransmitter, destination, edge, hash, receive, register, result_sink,
    stream_sink, try_run_with_tracker,
};
use super::*;
use crate::exec::operators::{ResultSinkHandle, partition_chunk_by_hash_arrays};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::builder::build_compiled_pipeline_graph;
use crate::exec::pipeline::dependency::DependencyManager;
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use novarocks_physical_plan::{Distribution, EdgeId, Fragment, PhysicalPlan, ValueId};
use novarocks_types::UniqueId;

const SOURCE: FragmentId = FragmentId::new(1);
const INNER: FragmentId = FragmentId::new(2);
const OUTER: FragmentId = FragmentId::new(3);
const TO_INNER: EdgeId = EdgeId::new(7);
const TO_OUTER: EdgeId = EdgeId::new(8);
const SOURCE_INSTANCE: UniqueId = UniqueId::new(0xc29, 1);
const INNER_INSTANCES: [UniqueId; 2] = [UniqueId::new(0xc29, 2), UniqueId::new(0xc29, 3)];
const OUTER_INSTANCE: UniqueId = UniqueId::new(0xc29, 4);
fn exchange_rows(staged: bool) -> Vec<Input> {
    let mut rows = ROWS.to_vec();
    if staged {
        // Explicit valid input contributions for the selected keys. The
        // original routing helper below checks that they really span both
        // declared destinations instead of assuming hash placement.
        for key in [None, Some(1), Some(2)] {
            rows.extend([(key, Some(0)), (key, Some(1))]);
        }
    }
    rows
}
fn expected(staged: bool, limit: usize) -> Vec<Output> {
    let rows = exchange_rows(staged)
        .iter()
        .map(|(key, value)| vec![cell(*key), cell(*value)])
        .collect::<Vec<_>>();
    complete_key_oracle(&rows, &[(0, true, true)], limit)
        .into_iter()
        .map(|(key, (count, sum, min, max))| {
            let key = key.into_iter().next().unwrap().map(|value| match value {
                KeyCell::Integer(value) => i64::try_from(value).unwrap(),
                _ => unreachable!("declared Int64 group key"),
            });
            (key, count, sum, min, max)
        })
        .collect()
}

fn sequence(ordinal: usize) -> AggregateSequenceId {
    AggregateSequenceId::new(41 + ordinal as u32)
}

/// Test-only checked physical author. Every new stage reads the immediate
/// predecessor's state and creates fresh Intermediate identities; all calls
/// keep their original logical binding/sequence. No computation lives here.
fn grouped_stage(
    builder: &mut FragmentBuilder,
    input: NodeId,
    columns: &[ValueId],
    bounds: &[Bound],
    limit: usize,
    call_base: u32,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let key = builder
        .add_expression(node, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    let mut output = vec![columns[0]];
    let mut calls = Vec::new();
    for (ordinal, bound) in bounds.iter().enumerate() {
        let id = AggregateCallId::new(call_base + ordinal as u32);
        let phase = AggregatePhase::Intermediate {
            sequence: sequence(ordinal),
        };
        let argument = builder
            .add_expression(
                node,
                bound.intermediate_type(),
                ExprKind::Value(columns[ordinal + 1]),
            )
            .unwrap();
        let value = builder
            .add_value(
                bound.output_type(phase),
                ValueOrigin::AggregateState { call: id, phase },
            )
            .unwrap();
        output.push(value);
        calls.push(AggregateCall {
            id,
            binding: bound.binding(phase),
            arguments: Box::from([argument]),
            distinct: false,
            order_by: Box::default(),
            output: value,
        });
    }
    let required = builder.node_output_properties(input).unwrap().clone();
    let mut produced = required.clone();
    produced.ordering = Box::from([OrderingKey {
        value: columns[0],
        direction: SortDirection::Ascending,
        null_ordering: NullOrdering::First,
    }]);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties: produced,
            output: OutputPort {
                node,
                columns: output.clone().into_boxed_slice(),
            },
            kind: NodeKind::TopN {
                order_by: Box::from([SortExpr {
                    expr: key,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::First,
                }]),
                limit: limit as u64,
                offset: 0,
                phase: TopNPhase::Partial {
                    sequence: TopNSequenceId::new(42),
                },
                reduction: TopNReduction::GroupedStates {
                    group_by: Box::from([(key, columns[0])]),
                    calls: calls.into_boxed_slice(),
                    comparator:
                        novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                },
            },
        })
        .unwrap();
    (node, output)
}
fn final_stage(
    builder: &mut FragmentBuilder,
    input: NodeId,
    columns: &[ValueId],
    bounds: &[Bound],
    limit: usize,
) -> NodeId {
    let calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(30 + ordinal as u32),
            phase: AggregatePhase::Final {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: vec![columns[ordinal + 1]],
        })
        .collect::<Vec<_>>();
    let (aggregate, _) = add_aggregate(
        builder,
        input,
        &columns[..1],
        &calls,
        AggregateGrouping::Complete,
    );
    let node = builder.reserve_node_id().unwrap();
    let key = builder
        .add_expression(node, int64(true), ExprKind::Value(columns[0]))
        .unwrap();
    builder
        .add_top_n(
            node,
            aggregate,
            Box::from([SortExpr {
                expr: key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::First,
            }]),
            limit as u64,
            0,
            TopNPhase::Final {
                sequence: TopNSequenceId::new(42),
            },
        )
        .unwrap();
    node
}
fn result_port(fragment: &Fragment) -> ResultPort {
    let output = fragment.nodes()[&fragment.root()].output.clone();
    let fields = output
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, value)| {
            let ty = fragment.values()[value].ty.clone();
            ResultField {
                domain: crate::test_result_domain::result_value_domain(&ty),
                name: format!("field-{ordinal}").into(),
                alias: None,
                value: *value,
                ty,
            }
        })
        .collect();
    ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output,
        fields,
    }
}
fn plan(
    staged: bool,
    limit: usize,
) -> (PhysicalPlan, novarocks_functions::PureEngineFunctionCatalog) {
    let catalog = aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("sum", PureKernelAbi::AggregateWindowV1),
        ("min", PureKernelAbi::AggregateWindowV1),
        ("max", PureKernelAbi::AggregateWindowV1),
    ]);
    let bounds = [
        bind(&catalog, "count", &[]),
        bind(&catalog, "sum", &[int64(true)]),
        bind(&catalog, "min", &[int64(true)]),
        bind(&catalog, "max", &[int64(true)]),
    ];
    let mut source = FragmentBuilder::new(SOURCE);
    let leaf = source.reserve_node_id().unwrap();
    let columns = values(
        &mut source,
        leaf,
        &[int64(true), int64(true)],
        &exchange_rows(staged)
            .iter()
            .map(|(key, value)| vec![cell(*key), cell(*value)])
            .collect::<Vec<_>>(),
    );
    let source = finish(source, leaf, FragmentSink::Stream { edge: TO_INNER }, 1);
    let mut inner = FragmentBuilder::new(INNER);
    let receive_inner = inner.reserve_node_id().unwrap();
    let imports = receive(
        &mut inner,
        receive_inner,
        TO_INNER,
        &[(columns[0], int64(true)), (columns[1], int64(true))],
        |values| {
            if staged {
                hash(&values[1..])
            } else {
                Distribution::Singleton
            }
        },
    );
    let calls = bounds
        .iter()
        .enumerate()
        .map(|(ordinal, bound)| CallSpec {
            bound,
            id: AggregateCallId::new(10 + ordinal as u32),
            phase: AggregatePhase::Partial {
                sequence: sequence(ordinal),
            },
            distinct: false,
            arguments: if ordinal == 0 {
                vec![]
            } else {
                vec![imports[1]]
            },
        })
        .collect::<Vec<_>>();
    let (partial, states) = add_aggregate(
        &mut inner,
        receive_inner,
        &imports[..1],
        &calls,
        AggregateGrouping::Partial,
    );
    let (first, first_output) = grouped_stage(&mut inner, partial, &states, &bounds, limit, 20);
    let mut result = PlanBuilder::new(PlanVersionId::try_new([123; 16]).unwrap());
    result.add_fragment(source).unwrap();
    if staged {
        result
            .add_fragment(finish(
                inner,
                first,
                FragmentSink::Stream { edge: TO_OUTER },
                2,
            ))
            .unwrap();
        let mut outer = FragmentBuilder::new(OUTER);
        let receive_outer = outer.reserve_node_id().unwrap();
        let mut sent = vec![(first_output[0], int64(true))];
        sent.extend(
            first_output[1..]
                .iter()
                .zip(&bounds)
                .map(|(value, bound)| (*value, bound.intermediate_type())),
        );
        // The original unpartitioned edge explicitly gathers both inner
        // instances into this one outer relation, before its global FinalRows.
        let received = receive(&mut outer, receive_outer, TO_OUTER, &sent, |_| {
            Distribution::Singleton
        });
        let (second, second_output) =
            grouped_stage(&mut outer, receive_outer, &received, &bounds, limit, 40);
        let root = final_stage(&mut outer, second, &second_output, &bounds, limit);
        let outer = finish(outer, root, FragmentSink::Result, 2);
        let port = result_port(&outer);
        result.add_fragment(outer).unwrap();
        result
            .add_edge(edge(
                TO_OUTER,
                (INNER, &first_output),
                (OUTER, receive_outer, &received),
                Distribution::Singleton,
                Distribution::Singleton,
            ))
            .unwrap();
        result.set_result_port(port).unwrap();
    } else {
        let root = final_stage(&mut inner, first, &first_output, &bounds, limit);
        let inner = finish(inner, root, FragmentSink::Result, 2);
        let port = result_port(&inner);
        result.add_fragment(inner).unwrap();
        result.set_result_port(port).unwrap();
    }
    result
        .add_edge(edge(
            TO_INNER,
            (SOURCE, &columns),
            (INNER, receive_inner, &imports),
            if staged {
                hash(&columns[1..])
            } else {
                Distribution::Singleton
            },
            if staged {
                hash(&imports[1..])
            } else {
                Distribution::Singleton
            },
        ))
        .unwrap();
    (result.finish_observed(&FixtureControl).unwrap(), catalog)
}
fn geometry(program: &Arc<LocalProgram>, bindings: ExchangeBindings, partial_dop: Option<i32>) {
    let error = Arc::new(RuntimeErrorState::default());
    let graph = build_compiled_pipeline_graph(
        program,
        bindings,
        ScanBindings::default(),
        crate::runtime::fragment::CompiledWriterBindings::default(),
        None,
        DependencyManager::new(),
        2,
        program
            .graph()
            .profile()
            .root_sink_dop()
            .map(|dop| dop.get() as i32),
        crate::runtime::execution_runtime::test_execution_function_set(),
        error.clone(),
    )
    .unwrap();
    let dop_of = |name: &str| {
        graph
            .pipelines
            .iter()
            .find(|pipeline| {
                pipeline
                    .factories
                    .iter()
                    .any(|factory| factory.name() == name)
            })
            .expect("the concrete fixture family is installed")
            .dop
    };
    for (index, node) in program.graph().nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        match node.kind() {
            ProgramNodeKind::Aggregate {
                need_finalize: false,
                ..
            } => {
                assert_eq!(
                    program.aggregates()[&id].grouping,
                    novarocks_local_program::CompiledAggregateGrouping::Partial
                );
                let factory =
                    CompiledAggregateProcessorFactory::try_new(program.clone(), id, error.clone())
                        .unwrap();
                assert_eq!(
                    dop_of(factory.name()),
                    partial_dop.expect("fixture retains upstream Partial")
                );
            }
            ProgramNodeKind::GroupedTopN { .. } => {
                let factory = CompiledGroupedTopNProcessorFactory::try_new(
                    program.clone(),
                    id,
                    error.clone(),
                )
                .unwrap();
                assert_eq!(dop_of(factory.name()), 1);
            }
            _ => (),
        }
    }
}
/// Qualify this declared input's cross-instance trigger through the original
/// routing helper. This is separate from the independent aggregate oracle and
/// does not assert a frame count or any driver scheduling/arrival order.
fn cross_instance_input(program: &LocalProgram, destinations: usize, limit: usize) {
    let node = program
        .graph()
        .nodes()
        .iter()
        .find(|node| matches!(node.kind(), ProgramNodeKind::Values { .. }))
        .unwrap();
    let ProgramNodeKind::Values { values } = node.kind() else {
        unreachable!()
    };
    let input = Chunk::new_with_chunk_schema(
        values.batch().unwrap().clone(),
        ChunkSchema::from_compiled_layout(node.output_layout()).unwrap(),
    );
    let parts = partition_chunk_by_hash_arrays(
        &input,
        &[input.batch.column(1).clone()],
        destinations,
        false,
    )
    .unwrap();
    for (key, _, _, _, _) in expected(true, limit) {
        for part in &parts {
            let keys = part
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!(
                (0..keys.len()).any(|row| (!keys.is_null(row)).then(|| keys.value(row)) == key),
                "every retained key has an actual source contribution in each routed inner input: {key:?}"
            );
        }
    }
}

fn execute(staged: bool, limit: usize) -> Vec<Output> {
    let (plan, catalog) = plan(staged, limit);
    let mut packages = packages(&plan, &catalog);
    let source = compile(packages.remove(&SOURCE).unwrap(), &catalog, 1, false);
    let inner = compile(packages.remove(&INNER).unwrap(), &catalog, 2, !staged);
    let outer = staged.then(|| compile(packages.remove(&OUTER).unwrap(), &catalog, 2, true));
    let port = in_process_test_exchange_receiver_port();
    let transmitter = LoopbackTransmitter::new(port.clone());
    let instances = if staged {
        &INNER_INSTANCES[..]
    } else {
        &INNER_INSTANCES[..1]
    };
    if staged {
        cross_instance_input(&source, instances.len(), limit);
    }
    let bindings = instances
        .iter()
        .map(|instance| register(&inner, *instance, 1, &port))
        .collect::<Vec<_>>();
    geometry(&inner, bindings[0].clone(), Some(2));
    let outer_bindings = outer
        .as_ref()
        .map(|program| register(program, OUTER_INSTANCE, instances.len(), &port));
    if let Some(program) = &outer {
        geometry(program, outer_bindings.as_ref().unwrap().clone(), None);
    }
    let tracker = MemTracker::new_root("grouped-exchange-component");
    let source_sink = stream_sink(
        &source,
        SOURCE_INSTANCE,
        instances
            .iter()
            .map(|instance| destination(*instance, SOURCE_INSTANCE, 0, 1))
            .collect(),
        &transmitter,
    );
    try_run_with_tracker(
        &source,
        source_sink,
        ExchangeBindings::default(),
        SOURCE_INSTANCE,
        tracker.clone(),
    )
    .unwrap();
    let output = ResultSinkHandle::new();
    for (ordinal, (instance, binding)) in instances.iter().zip(bindings).enumerate() {
        let sink = if staged {
            stream_sink(
                &inner,
                *instance,
                vec![destination(
                    OUTER_INSTANCE,
                    *instance,
                    ordinal as u32,
                    instances.len() as u32,
                )],
                &transmitter,
            )
        } else {
            result_sink(&inner, *instance, &output)
        };
        try_run_with_tracker(&inner, sink, binding, *instance, tracker.clone()).unwrap();
    }
    if let Some(program) = outer {
        try_run_with_tracker(
            &program,
            result_sink(&program, OUTER_INSTANCE, &output),
            outer_bindings.unwrap(),
            OUTER_INSTANCE,
            tracker.clone(),
        )
        .unwrap();
    }
    let rows = read_final_rows(output.take_chunks());
    drop(source);
    drop(inner);
    drop(port);
    drop(transmitter);
    assert_eq!(
        tracker.current(),
        0,
        "joined drivers and consumed outputs release real tracked owners"
    );
    rows
}
#[test]
fn compiled_grouped_topn_exchange_keeps_partial_parallel_before_real_gather() {
    for limit in [0, 1, 2] {
        assert_eq!(execute(false, limit), expected(false, limit));
    }
}
#[test]
fn compiled_grouped_topn_two_intermediate_levels_merge_both_instance_contributions() {
    for limit in [1, 2, 3] {
        assert_eq!(execute(true, limit), expected(true, limit));
    }
}
