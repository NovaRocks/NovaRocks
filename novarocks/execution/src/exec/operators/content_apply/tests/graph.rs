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

use std::{collections::BTreeMap, sync::OnceLock, time::Duration};

use arrow::{
    array::{FixedSizeBinaryArray, Int32Array, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_connector_contract::{ConnectorRowMutationEffect as Effect, WriteTargetOrdinal};
use novarocks_execution_contract::{DataStreamPartitionType, ExchangeDeliveryRequirement};
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ScopeColumn, ScopeLayout, ScopeLayouts,
    compile_expr_set_pure,
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
};
use novarocks_local_program::{
    BindingRequirement, BindingRequirements, ChangeEventOutputExpr, ChangeEventSpec,
    CompileProfile, CompiledProgramFacts, DiagnosticSourceNodeId, InputCompletion,
    KernelAbiVersion, LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance, LocalProgram,
    LocalProgramGraph, MetricAggregation, OperatorMetricAggregation, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramContentApply, ProgramContentApplyTarget, ProgramNode, ProgramNodeId,
    ProgramNodeKind, ProgramOutputRole, ProgramRootRef, ProgramUnitId,
    ProgramWriteLayoutPreference, StaticLayout, StaticRouterRoute, StaticSinkProgram,
    StaticStreamBranch, StaticStreamContract, StaticStreamRouting, StaticSummaryAuthor,
    StaticSummaryBranch, StaticValues,
};
use novarocks_result_contract::{LogicalChangeCounts, SummaryKind, SummaryWireType};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
    SemanticParameters, ValueTypeStorageLifetime,
};
use novarocks_types::SlotId;

use super::*;
use crate::{
    exec::{
        operators::{
            compiled_change_router::CompiledChangeRouterFactory,
            logical_change_summary::SummarySourceFactory,
            result_sink::{ResultSinkBuffer, ResultSinkFactory},
        },
        pipeline::executor::{
            PreparedPipelineExecution, prepare_compiled_program_with_sink_materializer,
        },
    },
    runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig,
        runtime_state::RuntimeState,
        task::{
            io::NoopTaskEventSink,
            sink::{IndependentSummaryPipeline, MaterializedCompiledSink},
        },
    },
};

const TAIL_DOP: usize = 6;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn layout(start: u32, types: &[(DataType, bool)]) -> StaticLayout {
    let slots = (start..start + types.len() as u32)
        .map(SlotId::new)
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        types
            .iter()
            .zip(&slots)
            .map(|((ty, nullable), slot)| {
                Field::new(format!("column_{}", slot.as_u32()), ty.clone(), *nullable)
            })
            .collect::<Vec<_>>(),
    ));
    let types = types
        .iter()
        .map(|(ty, nullable)| FunctionValueType::new(ty.clone(), *nullable))
        .collect::<Vec<_>>();
    typed_fixture::layout(schema, &slots, &types)
}

fn values(id: usize, layout: &StaticLayout, arrays: Vec<ArrayRef>) -> ProgramNode {
    let batch = RecordBatch::try_new(layout.schema().clone(), arrays).unwrap();
    node(
        id,
        ProgramNodeKind::Values {
            values: StaticValues::try_new(batch, layout.clone()).unwrap(),
        },
        layout,
    )
}

fn node(id: usize, kind: ProgramNodeKind, layout: &StaticLayout) -> ProgramNode {
    ProgramNode::new_local(
        ProgramNodeId::new(id),
        vec![DiagnosticSourceNodeId::new(id as u32)],
        kind,
        layout.clone(),
    )
}

fn ty(layout: &StaticLayout, ordinal: usize) -> FunctionValueType {
    FunctionValueType::try_from_typed_binding(layout.value_binding_at(ordinal).unwrap().clone())
        .unwrap()
}

fn events(
    input: &StaticLayout,
    output: &StaticLayout,
) -> (Arc<CompiledExprSet>, Vec<ChangeEventSpec>) {
    let scope = RootScope::Port(0);
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    let content = ValueId::new(0);
    let position = ValueId::new(1);
    let visible = builder
        .add(None, ty(input, 0), ExprKind::Value(content))
        .unwrap();
    let position_input = builder
        .add(None, ty(input, 2), ExprKind::Value(position))
        .unwrap();
    let visible_root = builder
        .root(visible, EvaluationDemand::Value, scope)
        .unwrap();
    let event_specs = [Effect::Insert, Effect::Delete]
        .into_iter()
        .map(|effect| {
            let predicate = builder
                .add(
                    None,
                    FunctionValueType::new(DataType::Boolean, false),
                    ExprKind::IsNull {
                        expr: position_input,
                        negated: effect == Effect::Delete,
                    },
                )
                .unwrap();
            let root = builder
                .root(predicate, EvaluationDemand::TruthOnly, scope)
                .unwrap();
            ChangeEventSpec {
                predicate: Some(ProgramRootRef::new(ProgramUnitId::new(0), root)),
                effect,
                assignments: vec![ChangeEventOutputExpr {
                    write_input: None,
                    output_slot_id: output.slots()[0],
                    expr: Some(ProgramRootRef::new(ProgramUnitId::new(0), visible_root)),
                }],
            }
        })
        .collect::<Vec<_>>();
    let mut validate = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let bound = builder
        .seal(
            vec![ExprPort {
                values: Box::from([(content, ty(input, 0)), (position, ty(input, 2))]),
            }],
            &mut validate,
        )
        .unwrap()
        .finish(BTreeMap::new(), &mut validate)
        .unwrap();
    validate.finish().unwrap();
    let mut compile = CompileCheckpoints::try_new(&Control, CompilePhase::LowerProgram).unwrap();
    let layout = ScopeLayout::try_new(
        input.schema().clone(),
        (0..input.slots().len())
            .map(|ordinal| ty(input, ordinal))
            .collect(),
        Arc::from([
            ScopeColumn {
                value: content,
                ordinal: 0,
                ty: ty(input, 0),
            },
            ScopeColumn {
                value: position,
                ordinal: 2,
                ty: ty(input, 2),
            },
        ]),
        ValueTypeStorageLifetime::pure(),
        &mut compile,
    )
    .unwrap();
    let compiled = compile_expr_set_pure(
        &bound,
        EvaluationEnvironment {
            constants: &novarocks_physical_plan::ConstantPools::empty(),
            parameters: &SemanticParameters::try_new([]).unwrap(),
        },
        ScopeLayouts {
            ports: &[layout],
            all_ports: None,
        },
        novarocks_functions::ConstantValidation::INPUT_DERIVED,
        &novarocks_functions::testkit::rng_subset(),
        &mut compile,
    )
    .unwrap();
    compile.finish().unwrap();
    (compiled, event_specs)
}

fn stream(role: ProgramOutputRole, destination: i32, layout: &StaticLayout) -> StaticStreamBranch {
    StaticStreamBranch::try_new(
        destination,
        DataStreamPartitionType::Unpartitioned,
        vec![],
        layout.slots().to_vec(),
        None,
        StaticStreamContract {
            role,
            delivery: ExchangeDeliveryRequirement::MustDrain,
            routing: StaticStreamRouting::Ordinary,
        },
    )
    .unwrap()
}

fn program() -> Arc<LocalProgram> {
    let content = (DataType::Int64, false);
    let weight = (DataType::Int8, false);
    let position = [
        DataType::Utf8,
        DataType::Int64,
        DataType::Int32,
        DataType::Struct(Default::default()),
    ];
    let weighted = layout(0, &[content.clone(), weight.clone()]);
    let target = layout(
        2,
        &std::iter::once(content.clone())
            .chain(position.iter().cloned().map(|ty| (ty, false)))
            .collect::<Vec<_>>(),
    );
    let applied = layout(
        7,
        &std::iter::once(content.clone())
            .chain(position.into_iter().map(|ty| (ty, true)))
            .chain(std::iter::once(weight.clone()))
            .collect::<Vec<_>>(),
    );
    let provider_output = layout(13, std::slice::from_ref(&content));
    let expanded = layout(13, &[content, weight]);
    let summary_layout = |start, kind: SummaryKind| {
        layout(
            start,
            &kind
                .fields()
                .iter()
                .map(|field| {
                    let ty = match field.wire_type() {
                        SummaryWireType::Int64 => DataType::Int64,
                        SummaryWireType::FixedSizeBinary(width) => {
                            DataType::FixedSizeBinary(i32::from(width))
                        }
                    };
                    (ty, false)
                })
                .collect::<Vec<_>>(),
        )
    };
    let counts = summary_layout(15, SummaryKind::WriteCounts);
    let verification = summary_layout(18, SummaryKind::DeleteVerification);
    let apply = ProgramNodeId::new(2);
    let root = ProgramNodeId::new(3);
    let (unit, events) = events(&applied, &expanded);
    let nodes = vec![
        values(
            0,
            &weighted,
            vec![
                Arc::new(Int64Array::from(vec![10, 10, 20, 30, 30, 40])),
                Arc::new(Int8Array::from(vec![-1, -1, -1, 1, 1, 1])),
            ],
        ),
        values(
            1,
            &target,
            vec![
                Arc::new(Int64Array::from(vec![10, 10, 10, 50])),
                Arc::new(StringArray::from(vec![
                    "file-a", "file-a", "file-b", "file-c",
                ])),
                Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
                Arc::new(Int32Array::from(vec![0; 4])),
                Arc::new(StructArray::new_empty_fields(4, None)),
            ],
        ),
        node(
            2,
            ProgramNodeKind::ContentApply(ProgramContentApply {
                demand_filters: Box::default(),
                weighted_input: ProgramNodeId::new(0),
                target_input: ProgramNodeId::new(1),
                weighted_content: Box::from([weighted.slots()[0]]),
                target_content: Box::from([target.slots()[0]]),
                route_keys: Box::from([0]),
                weight: weighted.slots()[1],
                target: ProgramContentApplyTarget {
                    file: target.slots()[1],
                    position: target.slots()[2],
                    spec_id: target.slots()[3],
                    partition: target.slots()[4],
                },
                summary_slots: verification.slots().try_into().unwrap(),
            }),
            &applied,
        ),
        node(
            3,
            ProgramNodeKind::ChangeEventExpand {
                input: apply,
                events,
                output_slot_ids: expanded.slots().to_vec(),
                effect_slot_id: expanded.slots()[1],
            },
            &expanded,
        ),
    ];
    let units = vec![unit];
    let role = ProgramOutputRole::ProviderRoute {
        target: WriteTargetOrdinal::try_new(0).unwrap(),
    };
    let route = stream(role, 1, &provider_output);
    let sink = StaticSinkProgram::try_change_stream_router(
        vec![route],
        &units,
        expanded.slots()[1],
        vec![StaticRouterRoute {
            branch: 0,
            layout: ProgramWriteLayoutPreference::None,
            accepted_effects: Arc::from([Effect::Insert, Effect::Delete]),
        }],
    )
    .unwrap();
    let summaries = [
        (
            SummaryKind::WriteCounts,
            StaticSummaryAuthor::RouterSink,
            counts,
        ),
        (
            SummaryKind::DeleteVerification,
            StaticSummaryAuthor::Node(apply),
            verification,
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (kind, author, layout))| {
        StaticSummaryBranch::try_new(
            kind,
            author,
            stream(ProgramOutputRole::Summary(kind), index as i32 + 2, &layout),
            layout,
        )
        .unwrap()
    })
    .collect::<Vec<_>>();
    let requirements = std::iter::once((role, &provider_output))
        .chain(
            summaries
                .iter()
                .map(|summary| (ProgramOutputRole::Summary(summary.kind()), summary.layout())),
        )
        .enumerate()
        .map(
            |(branch, (role, layout))| BindingRequirement::ExchangeOutput {
                role,
                delivery: ExchangeDeliveryRequirement::MustDrain,
                branch,
                layout: layout.clone(),
            },
        )
        .collect();
    let graph = LocalProgramGraph::try_new_with_outputs(
        nodes,
        root,
        &units,
        CompileProfile::new(
            NonZeroUsize::new(TAIL_DOP).unwrap(),
            None,
            expanded.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(requirements).unwrap(),
        Some(sink),
        summaries,
    )
    .unwrap();
    let channels = graph
        .nodes()
        .iter()
        .flat_map(|node| {
            let layout = node.output_layout();
            (0..layout.slots().len()).map(move |ordinal| {
                (
                    ProgramChannelSite::Layout {
                        node: node.local_id().unwrap(),
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: ordinal as u32,
                    },
                    ty(layout, ordinal),
                )
            })
        })
        .collect();
    let operators = graph
        .nodes()
        .iter()
        .map(|node| {
            let id = LocalOperatorId::new(node.local_id().unwrap().index() as u32);
            LocalOperatorProvenance {
                id,
                lowered_nodes: Box::from([node.local_id().unwrap()]),
                sources: node.physical_sources().into(),
                origin: LocalOperatorOrigin::Direct,
                cost_owner: id,
                metrics: OperatorMetricAggregation {
                    cpu_time: MetricAggregation::Sum,
                    wall_time: MetricAggregation::Maximum,
                    peak_retained_bytes: MetricAggregation::Maximum,
                },
            }
        })
        .collect();
    let sources = graph
        .nodes()
        .iter()
        .flat_map(|node| node.physical_sources().iter().copied())
        .collect();
    Arc::new(
        LocalProgram::try_new(
            graph,
            units,
            vec![],
            channels,
            operators,
            &sources,
            CompiledProgramFacts {
                input_completion: InputCompletion::MustDrain,
                ..Default::default()
            },
            &Control,
        )
        .unwrap(),
    )
}

struct Outputs {
    rows: ResultSinkBuffer,
    counts: ResultSinkBuffer,
    verification: ResultSinkBuffer,
}

fn state() -> RuntimeState {
    static RUNTIME: OnceLock<Arc<ExecutionRuntime>> = OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 2,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: 1024,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .unwrap(),
        )
    });
    RuntimeState::for_local_execution(
        Some(crate::runtime::query_options::QueryOptions {
            query_timeout: Some(15),
            ..Default::default()
        }),
        None,
        None,
        None,
        None,
        Some(MemTracker::new_root("content apply Task")),
        Some(runtime.clone()),
    )
}

fn prepare(state: RuntimeState) -> (PreparedPipelineExecution, Outputs) {
    let program = program();
    let state = Arc::new(state);
    let outputs = Outputs {
        rows: ResultSinkBuffer::new(),
        counts: ResultSinkBuffer::new(),
        verification: ResultSinkBuffer::new(),
    };
    let prepared = prepare_compiled_program_with_sink_materializer(
        program.clone(),
        Duration::from_millis(10),
        |actual_root_dop, mut authors| {
            let change_counts = authors
                .claim(SummaryKind::WriteCounts, StaticSummaryAuthor::RouterSink)
                .unwrap()
                .declare_units(actual_root_dop.get(), state.mem_tracker().unwrap())
                .unwrap();
            let states = authors.seal().unwrap();
            states
                .get(SummaryKind::DeleteVerification)
                .unwrap()
                .validate_units(TAIL_DOP)
                .unwrap();
            let StaticSinkProgram::ChangeStreamRouter {
                effect_slot,
                routes,
                ..
            } = program.graph().sink().unwrap()
            else {
                unreachable!()
            };
            let router = CompiledChangeRouterFactory::new(
                *effect_slot,
                routes.clone(),
                vec![Arc::new(ResultSinkFactory::new(outputs.rows.clone()))],
                Some(change_counts),
            )
            .unwrap();
            let summaries = program
                .graph()
                .summaries()
                .iter()
                .map(|branch| {
                    let buffer = match branch.kind() {
                        SummaryKind::WriteCounts => &outputs.counts,
                        SummaryKind::DeleteVerification => &outputs.verification,
                    };
                    IndependentSummaryPipeline {
                        source: Box::new(
                            SummarySourceFactory::new(
                                states.get(branch.kind()).unwrap().clone(),
                                branch.layout().clone(),
                            )
                            .unwrap(),
                        ),
                        sink: Box::new(ResultSinkFactory::new(buffer.clone())),
                    }
                })
                .collect();
            Ok(MaterializedCompiledSink {
                root: Box::new(router),
                summaries,
            })
        },
        Default::default(),
        Default::default(),
        Default::default(),
        None,
        None,
        TAIL_DOP as i32,
        std::num::NonZeroU32::new(1).unwrap(),
        state.clone(),
        Arc::new(NoopTaskEventSink),
        None,
    )
    .unwrap();
    (prepared, outputs)
}

fn summary(buffer: &ResultSinkBuffer) -> crate::exec::chunk::Chunk {
    let mut chunks = buffer.take_chunks();
    assert_eq!(chunks.len(), 1);
    let chunk = chunks.pop().unwrap();
    assert_eq!(chunk.len(), 1);
    chunk
}

fn decode_counts(chunk: &crate::exec::chunk::Chunk) -> [i64; 3] {
    std::array::from_fn(|column| {
        chunk.columns()[column]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    })
}

#[test]
fn compiled_content_apply_runs_tail_units_and_both_summaries_on_one_task() {
    let (prepared, outputs) = prepare(state());
    let installed = prepared.driver_count();
    assert!(installed >= TAIL_DOP + 5);
    let running = prepared.start();
    running.join().unwrap();
    assert_eq!(running.submitted_driver_count(), installed);
    assert!(running.stopped_fact().is_some());

    let chunks = outputs.rows.take_chunks();
    let mut rows = Vec::new();
    for chunk in chunks {
        assert_eq!(chunk.columns().len(), 2);
        let content = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let effects = chunk.columns()[1]
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        rows.extend(
            content
                .values()
                .iter()
                .copied()
                .zip(effects.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![
            (10, Effect::Delete as i8),
            (10, Effect::Delete as i8),
            (30, Effect::Insert as i8),
            (30, Effect::Insert as i8),
            (40, Effect::Insert as i8)
        ]
    );
    assert_eq!(
        decode_counts(&summary(&outputs.counts)),
        LogicalChangeCounts::try_new(3, 0, 2).unwrap().into_wire()
    );
    let verification = summary(&outputs.verification);
    assert_eq!(decode_counts(&verification), [3, 2, 1]);
    let fingerprint = verification.columns()[3]
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_ne!(fingerprint.value(0), &[0; 16]);
}

#[test]
fn cancelled_content_apply_task_joins_every_installed_driver_without_a_summary() {
    let (prepared, outputs) = prepare(state());
    let installed = prepared.driver_count();
    let running = prepared.start_failed(KernelFailure::Cancelled);
    assert!(running.join().is_err());
    assert_eq!(running.submitted_driver_count(), installed);
    assert!(running.stopped_fact().is_some());
    assert!(outputs.rows.take_chunks().is_empty());
    assert!(outputs.counts.take_chunks().is_empty());
    assert!(outputs.verification.take_chunks().is_empty());
}

#[cfg(novarocks_dev_controls)]
#[test]
fn injected_drop_drops_one_real_insert_and_preserves_delete_verification() {
    use std::sync::atomic::AtomicBool;

    let dropped = Arc::new(AtomicBool::new(false));
    let mut state = state();
    let fault = dropped.clone();
    state.install_content_apply_insert_control(Some(Arc::new(move |_| {
        Ok(!fault.swap(true, Ordering::AcqRel))
    })));
    let (prepared, outputs) = prepare(state);
    prepared.start().join().unwrap();
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(
        outputs
            .rows
            .take_chunks()
            .iter()
            .map(|chunk| chunk.len())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        decode_counts(&summary(&outputs.counts)),
        LogicalChangeCounts::try_new(2, 0, 2).unwrap().into_wire()
    );
    assert_eq!(decode_counts(&summary(&outputs.verification)), [3, 2, 1]);
}

#[test]
fn withdrawing_after_a_target_page_cannot_publish_delete_verification() {
    use crate::exec::{
        chunk::Chunk,
        operators::{
            local_exchanger::LocalExchanger,
            logical_change_summary::{SummaryFailure, TaskSummaryState, summary_failure},
        },
        pipeline::{
            input::RawInput,
            operator::Operator,
            operator_factory::OperatorFactory,
            step::{InputResult, NextStep},
        },
        shared_stage::stage_completion::StageParticipants,
    };
    use novarocks_failure_contract::FailureProjection;

    let program = program();
    let state = state();
    let error = state.error_state();
    let tracker = state.mem_tracker().unwrap();
    let construction = try_runtime_input_construction_factory(
        representation_construction_profile().unwrap(),
        tracker.clone(),
        error.clone(),
    )
    .unwrap();
    let handoff = LocalExchanger::new_handoff(2, 1, 1);
    use crate::runtime_filter as rf;
    let schema = rf::RuntimeFilterMembershipSchema::new(
        &DataType::Int64,
        rf::RuntimeFilterNullSemantics::NullSafeEqual,
    )
    .unwrap();
    let (session, report_demand) =
        crate::exec::pipeline::builder::observe_int64_membership_producer(11, schema.digest());
    let contract = rf::RuntimeFilterProducerContract::membership(
        rf::RuntimeFilterBindingId::new(11),
        rf::RuntimeFilterChannelId::new(11),
        rf::RuntimeFilterExecutionContract::Membership(schema),
    )
    .unwrap();
    let demand = crate::exec::operators::hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory::from_content_demand(
        &[(0, contract)], |_| Ok(DataType::Int64), session,
    ).unwrap();
    let kind = SummaryKind::DeleteVerification;
    let summary_state = TaskSummaryState::new(kind, 1, tracker.clone()).unwrap();
    let stages = Arc::new(
        ContentApplyStages::new(
            program.clone(),
            ProgramNodeId::new(2),
            StageParticipants {
                inputs: 1,
                finalizers: NonZeroUsize::MIN,
                probes: 1,
                tails: 1,
            },
            handoff.clone(),
            summary_state.clone(),
            tracker,
            std::num::NonZeroU32::new(1).unwrap(),
            error,
        )
        .with_demand_producer(Some(demand)),
    );
    let bind = |mut operator: Box<dyn Operator>| {
        operator.set_construction_factory(construction.clone());
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        operator.activate(&state).unwrap();
        operator
    };
    let phase = |phase| bind(ContentApplyFactory::new(stages.clone(), phase).create(1, 0));
    let mut capture = phase(ContentApplyPhase::Capture);
    let mut finalize = phase(ContentApplyPhase::Finalize);
    let mut probe = phase(ContentApplyPhase::Probe);
    let mut tail = phase(ContentApplyPhase::Tail);
    let summary_layout = program
        .graph()
        .summaries()
        .iter()
        .find(|summary| summary.kind() == kind)
        .unwrap()
        .layout();
    let mut summary = bind(
        SummarySourceFactory::new(summary_state, summary_layout.clone())
            .unwrap()
            .create(1, 0),
    );
    assert_eq!(report_demand(), (Default::default(), false, None));

    let input = |id: usize| {
        let node = &program.graph().nodes()[id];
        let ProgramNodeKind::Values { values } = node.kind() else {
            unreachable!()
        };
        let novarocks_local_program::StaticValuesBacking::Batch(batch) = values.backing() else {
            unreachable!()
        };
        let layout = values.layout();
        let types = (0..layout.slots().len())
            .map(|ordinal| ty(layout, ordinal))
            .collect::<Vec<_>>();
        typed_fixture::chunk(batch.clone(), layout.slots(), &types)
    };
    assert!(matches!(
        capture.try_push_for_test(&state, RawInput::for_test(input(0))),
        InputResult::Accepted(Ok(_))
    ));
    assert!(matches!(
        capture.drive_for_test(&state).unwrap().next,
        NextStep::NeedInput
    ));
    capture.finish_for_test(&state).unwrap();
    assert!(matches!(
        capture.drive_for_test(&state).unwrap().next,
        NextStep::Finished
    ));
    assert!((0..4096).any(|_| matches!(
        finalize.drive_for_test(&state).unwrap().next,
        NextStep::Finished
    )));
    assert_eq!(
        report_demand(),
        ([10, 20].into_iter().collect(), false, Some((0, 2)))
    );

    assert!(matches!(
        probe.try_push_for_test(&state, RawInput::for_test(input(1))),
        InputResult::Accepted(Ok(_))
    ));
    let delete: Chunk = (0..16)
        .find_map(|_| probe.drive_for_test(&state).unwrap().output)
        .unwrap();
    assert_eq!(delete.columns().len(), 6);
    assert_eq!(
        delete.columns()[5]
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(0),
        -1
    );
    assert!(matches!(
        tail.drive_for_test(&state).unwrap().next,
        NextStep::Wait(_)
    ));
    assert!(matches!(
        summary.drive_for_test(&state).unwrap().next,
        NextStep::Wait(_)
    ));

    handoff.close_consumer(0);
    probe.begin_exit().unwrap();
    tail.begin_exit().unwrap();
    let cause = summary.drive_for_test(&state).unwrap_err();
    assert_eq!(
        cause.failure_fact(),
        summary_failure(SummaryFailure::MissingContributor(kind)).failure_fact()
    );
}
