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

//! A two-fragment compiled plan run end to end in process: the producer's
//! compiled stream sink encodes real exchange frames, a loopback transmitter
//! delivers them to the consumer's positional receiver, and the consumer's
//! compiled ExchangeSource feeds its Result sink.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::ConstantPolicy;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{
    DataStreamPartitionType, KernelAbiVersion, LocalProgram, ProgramNodeKind, StaticSinkProgram,
};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprKind,
    Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, HashDefinition, HashPartitionScheme, LiteralValue,
    NodeId, PartitionCountDomain, PartitionCountParameter, PartitionCountParameterId,
    PartitionSpaceId, PhysicalExpressionRoots, PhysicalPlan, PhysicalRootUses, PipelineDopDomain,
    PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits, ResultField, ResultPort,
    RowMultiplicity, ValueId, ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, PureCompileControl,
};
use novarocks_types::UniqueId;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle, partition_chunk_by_hash_arrays};
use crate::exec::pipeline::binding::{ExchangeBinding, ExchangeBindings};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::fragment::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentSinkAssignment,
};
use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
use crate::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeReceiverFrame, ExchangeReceiverKey,
    ExchangeReceiverPort, ExchangeTransmitRejection, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation, NoopFragmentEventSink,
};
use crate::runtime::fragment::sink::materialize_compiled_sink;
use crate::runtime::runtime_state::RuntimeState;

const PRODUCER_FINST: UniqueId = UniqueId::new(0x71, 0x01);
const CONSUMER_FINST: UniqueId = UniqueId::new(0x72, 0x02);
/// Literal rows `(a, b)` authored by the producer's Values node.
const ROWS: [(i64, i64); 3] = [(1, 10), (2, 20), (3, 30)];
const CONSUMER_FINST_2: UniqueId = UniqueId::new(0x72, 0x03);
const HASH_ROWS: [(i64, i64); 8] = [
    (1, 10),
    (2, 20),
    (3, 30),
    (4, 40),
    (5, 50),
    (6, 60),
    (7, 70),
    (8, 80),
];

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn int64() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}

/// Producer `Values(a, b) -> Stream(Gather)` whose edge projects `(b, a)`;
/// consumer `ExchangeSource(b', a') -> Result`. Both fragments allocate local
/// slots from zero, so the sender's wire slots are `[slot(b), slot(a)] =
/// [1, 0]` while the receiver expects `[slot(b'), slot(a')] = [0, 1]`: every
/// wire id exists in the receiver namespace in the other order.
fn plan(hashed: bool, values_rows: &[(i64, i64)]) -> (PhysicalPlan, NodeId) {
    plan_with_key_order(hashed, values_rows, false)
}

fn plan_with_key_order(
    hashed: bool,
    values_rows: &[(i64, i64)],
    reverse_keys: bool,
) -> (PhysicalPlan, NodeId) {
    let edge = EdgeId::new(5);
    let producer_id = FragmentId::new(1);
    let consumer_id = FragmentId::new(2);

    let mut producer = FragmentBuilder::new(producer_id);
    let values = producer.reserve_node_id().unwrap();
    let a = producer
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let b = producer
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let mut rows = Vec::new();
    for &(a_value, b_value) in values_rows {
        let a_cell = producer
            .add_expression(
                values,
                int64(),
                ExprKind::Literal(LiteralValue::Int64(a_value)),
            )
            .unwrap();
        let b_cell = producer
            .add_expression(
                values,
                int64(),
                ExprKind::Literal(LiteralValue::Int64(b_value)),
            )
            .unwrap();
        rows.push(vec![a_cell, b_cell].into_boxed_slice());
    }
    producer
        .add_values(values, rows.into_boxed_slice(), Box::from([a, b]))
        .unwrap();
    let producer = producer
        .finish_definition(
            values,
            FragmentSink::Stream { edge },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut consumer = FragmentBuilder::new(consumer_id);
    let exchange = consumer.reserve_node_id().unwrap();
    let b_import = consumer
        .add_value(
            int64(),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: b,
            },
        )
        .unwrap();
    let a_import = consumer
        .add_value(
            int64(),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: a,
            },
        )
        .unwrap();
    consumer
        .add_exchange_source(
            exchange,
            edge,
            Box::from([(b, b_import), (a, a_import)]),
            Box::from([b_import, a_import]),
            if hashed {
                hash(if reverse_keys {
                    [a_import, b_import]
                } else {
                    [b_import, a_import]
                })
            } else {
                Distribution::Singleton
            },
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let consumer = consumer
        .finish_definition(
            exchange,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (source_distribution, destination_distribution) = if hashed {
        (
            hash(if reverse_keys { [a, b] } else { [b, a] }),
            hash(if reverse_keys {
                [a_import, b_import]
            } else {
                [b_import, a_import]
            }),
        )
    } else {
        (Distribution::Singleton, Distribution::Singleton)
    };
    let output = consumer.nodes()[&exchange].output.clone();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([73; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: producer_id,
            projection: Box::from([b, a]),
        },
        destination: EdgeDestination {
            fragment: consumer_id,
            node: exchange,
            receive_mapping: Box::from([(b, b_import), (a, a_import)]),
        },
        partitioning: EdgePartitioning {
            source: source_distribution,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: destination_distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: consumer_id,
        output,
        fields: Box::from([
            ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64()),
                name: "b".into(),
                alias: None,
                value: b_import,
                ty: int64(),
            },
            ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64()),
                name: "a".into(),
                alias: None,
                value: a_import,
                ty: int64(),
            },
        ]),
    })
    .unwrap();
    (plan.finish().unwrap(), exchange)
}

/// Literal cells are the only roots; each gets one eager use.
fn root_uses(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap()
}

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

fn packages(plan: &PhysicalPlan) -> BTreeMap<FragmentId, FragmentPackage> {
    let uses = plan
        .fragments()
        .iter()
        .map(|(id, fragment)| (*id, root_uses(fragment)))
        .collect::<BTreeMap<_, _>>();
    let calls = plan
        .fragments()
        .iter()
        .map(|(id, fragment)| {
            (
                *id,
                FrozenFragmentCalls::try_new(fragment, &uses[id], vec![], &FixtureControl).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let pruning = plan
        .fragments()
        .keys()
        .map(|id| {
            (
                *id,
                FrozenFragmentPruning::try_new(*id, vec![], &FixtureControl).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let admissions = plan
        .fragments()
        .keys()
        .map(|id| (*id, admission()))
        .collect::<BTreeMap<_, _>>();
    extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap()
}

fn options(root_sink_dop: Option<NonZeroUsize>) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop,
        kernel_abi: KernelAbiVersion::CURRENT,
        // Explicit fixture admission; these values are not production defaults.
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
        exchange_wait: Duration::from_secs(120),
    }
}

fn compile(package: FragmentPackage, root_sink_dop: Option<NonZeroUsize>) -> Arc<LocalProgram> {
    // Literal-only fragments bind no function; the catalog is the real
    // sealed RAND subset because an empty catalog is refused.
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &functions,
            options(root_sink_dop),
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("fixture fragment compiles: {error}")),
    )
}

struct Programs {
    producer: Arc<LocalProgram>,
    consumer: Arc<LocalProgram>,
    receiver: NodeId,
}

/// A native-exchange hash scheme over the given ordered key columns.
fn hash(keys: [ValueId; 2]) -> Distribution {
    Distribution::Hash {
        keys: Box::from(keys),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([41; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([42; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 64,
                    requires_power_of_two: true,
                },
            },
            definition: HashDefinition::native_exchange(),
        },
    }
}

fn programs() -> Programs {
    programs_shaped(false, &ROWS)
}

#[test]
fn compiled_receivers_preserve_exact_destination_hash_key_slots() {
    let compiled = programs_shaped(true, &HASH_ROWS);
    let (id, receiver) = compiled.consumer.exchange_inputs().iter().next().unwrap();
    let slots = compiled.consumer.graph().nodes()[id.index()]
        .output_layout()
        .slots();
    // The wire order is (b, a), independently of the producer's value order.
    assert_eq!(receiver.hash_partition_slots.as_ref(), slots);
    assert_eq!(slots.len(), 2);
    let (plan, _) = plan_with_key_order(true, &HASH_ROWS, true);
    let mut cut_packages = packages(&plan);
    let reversed = compile(
        cut_packages.remove(&FragmentId::new(2)).unwrap(),
        Some(NonZeroUsize::new(1).unwrap()),
    );
    let (id, receiver) = reversed.exchange_inputs().iter().next().unwrap();
    let slots = reversed.graph().nodes()[id.index()].output_layout().slots();
    assert_eq!(
        receiver.hash_partition_slots.as_ref(),
        &[slots[1], slots[0]]
    );
    let unpartitioned = programs();
    assert!(
        unpartitioned
            .consumer
            .exchange_inputs()
            .values()
            .all(|input| input.hash_partition_slots.is_empty())
    );
}

fn programs_shaped(hashed: bool, values_rows: &[(i64, i64)]) -> Programs {
    let (plan, receiver) = plan(hashed, values_rows);
    let mut packages = packages(&plan);
    let producer = packages.remove(&FragmentId::new(1)).unwrap();
    let consumer = packages.remove(&FragmentId::new(2)).unwrap();
    Programs {
        // The producer's sink is a stream: no root result placement.
        producer: compile(producer, None),
        consumer: compile(consumer, Some(NonZeroUsize::new(1).unwrap())),
        receiver,
    }
}

/// Delivers every encoded frame to the receiver port, as the native data
/// plane does after its wire hop.
struct LoopbackTransmitter {
    port: Arc<dyn ExchangeReceiverPort>,
}

impl ExchangeFrameTransmitter for LoopbackTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        self.port
            .push(
                ExchangeReceiverKey {
                    fragment_instance_id: frame.destination_fragment_instance_id,
                    node_id: frame.destination_node_id,
                },
                ExchangeReceiverFrame {
                    stream: frame.stream.unwrap_or_else(|| {
                        crate::runtime::fragment::io::exchange::fixture_stream(
                            frame.sender_fragment_instance_id,
                            frame.destination_fragment_instance_id,
                            frame.destination_node_id,
                            frame.sender_ordinal,
                        )
                    }),
                    source_fragment_instance_id: frame.sender_fragment_instance_id,
                    sender_ordinal: frame.sender_ordinal,
                    sender_count: frame.sender_count,
                    sender_id: frame.sender_id,
                    backend_number: frame.backend_number,
                    sequence: frame.sequence,
                    eos: frame.eos,
                    payload: frame.payload,
                },
            )
            .map_err(|error| {
                ExchangeTransmitRejection::Failed(FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::RemoteRejected,
                    error.to_string(),
                ))
            })
    }
}

fn runtime_state() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled exchange tests",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ))
}

fn finst(id: UniqueId) -> Option<(i64, i64)> {
    Some((id.high(), id.low()))
}

fn assignments(receiver: NodeId) -> ExchangeInputAssignments {
    ExchangeInputAssignments::new(BTreeMap::from([(
        FragmentNodeId::new(i32::try_from(receiver.get()).unwrap()),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]))
}

fn stream_assignment() -> FragmentSinkAssignment {
    FragmentSinkAssignment::StreamDestinations {
        destinations: vec![
            FragmentDestination::new(
                CONSUMER_FINST,
                RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
                PRODUCER_FINST,
                0,
                1,
            )
            .expect("destination"),
        ],
        sender_id: None,
    }
}

fn receivers(
    programs: &Programs,
    port: &Arc<dyn ExchangeReceiverPort>,
) -> CompiledExchangeReceivers {
    receivers_at(programs, port, CONSUMER_FINST)
}

fn receivers_at(
    programs: &Programs,
    port: &Arc<dyn ExchangeReceiverPort>,
    instance: UniqueId,
) -> CompiledExchangeReceivers {
    materialize_compiled_exchange_receivers(
        &programs.consumer,
        instance,
        &assignments(programs.receiver),
        Arc::clone(port),
        Some(&crate::runtime::fragment::exchange::test_receiver_creation_factory()),
    )
    .expect("compiled receivers")
}

fn run(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) {
    prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        bindings,
        finst(instance),
        1,
        runtime_state(),
        Arc::new(NoopFragmentEventSink),
    )
    .expect("compiled program prepares drivers")
    .start()
    .join()
    .expect("compiled program runs");
}

fn prepare_error(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) -> String {
    match prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        bindings,
        finst(instance),
        1,
        runtime_state(),
        Arc::new(NoopFragmentEventSink),
    ) {
        Ok(_) => panic!("compiled program must be refused"),
        Err(error) => error.to_string(),
    }
}

fn int64_column(chunk: &Chunk, column: usize) -> Vec<i64> {
    chunk
        .record_batch()
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .values()
        .to_vec()
}

// Values -> Stream(Gather) | ExchangeSource -> Result, both compiled by
// local-compiler and executed through compiled operators only. The receiver
// binds wire columns by position; by slot id it would swap `a` and `b`.
#[test]
fn compiled_gather_stream_reaches_compiled_exchange_source_positionally() {
    let programs = programs();

    // The producer routes to the consumer's physical exchange node, and its
    // wire slot ids collide with the receiver's in the opposite order: the
    // exact shape a slot-id binding would silently swap.
    let Some(StaticSinkProgram::DataStream { branch, .. }) = programs.producer.graph().sink()
    else {
        panic!("the producer compiles to a single-branch stream sink");
    };
    let (&source, input) = programs
        .consumer
        .exchange_inputs()
        .iter()
        .next()
        .expect("one compiled exchange input");
    assert_eq!(programs.consumer.exchange_inputs().len(), 1);
    assert_eq!(input.receiver_node, programs.receiver.get());
    assert_eq!((input.edge, input.source_fragment), (5, 1));
    assert_eq!(
        branch.dest_node_id(),
        i32::try_from(programs.receiver.get()).unwrap()
    );
    let receiver_slots = programs.consumer.graph().nodes()[source.index()]
        .output_layout()
        .slots()
        .to_vec();
    let mut wire = branch.output_columns().to_vec();
    let mut expected = receiver_slots.clone();
    wire.sort();
    expected.sort();
    assert_eq!(wire, expected, "every wire slot id exists in the receiver");
    assert_ne!(
        branch.output_columns(),
        receiver_slots.as_slice(),
        "sender and receiver namespaces disagree on order"
    );

    let port = in_process_test_exchange_receiver_port();
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = receivers(&programs, &port);
    for registration in registrations {
        port.register(registration).expect("register receiver");
    }

    let producer_sink = materialize_compiled_sink(
        &programs.producer,
        &stream_assignment(),
        PRODUCER_FINST,
        Arc::new(LoopbackTransmitter {
            port: Arc::clone(&port),
        }),
        None,
        None,
    )
    .expect("compiled stream sink");
    run(
        &programs.producer,
        producer_sink,
        ExchangeBindings::default(),
        PRODUCER_FINST,
    );

    let output = ResultSinkHandle::new();
    let consumer_sink = materialize_compiled_sink(
        &programs.consumer,
        &FragmentSinkAssignment::None,
        CONSUMER_FINST,
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink");
    run(&programs.consumer, consumer_sink, bindings, CONSUMER_FINST);

    let chunks = output.take_chunks();
    let mut rows = Vec::new();
    for chunk in &chunks {
        let b = int64_column(chunk, 0);
        let a = int64_column(chunk, 1);
        rows.extend(b.into_iter().zip(a));
    }
    let expected = ROWS.iter().map(|(a, b)| (*b, *a)).collect::<Vec<_>>();
    assert_eq!(rows, expected, "result rows are (b, a) in literal order");
}

#[test]
fn compiled_exchange_source_requires_exactly_its_own_receiver_binding() {
    let programs = programs();
    let port = in_process_test_exchange_receiver_port();
    let result_sink = || -> Box<dyn OperatorFactory> {
        Box::new(ResultSinkFactory::new(ResultSinkHandle::new()))
    };

    let missing = prepare_error(
        &programs.consumer,
        result_sink(),
        ExchangeBindings::default(),
        CONSUMER_FINST,
    );
    assert!(missing.contains("missing exchange binding"), "{missing}");

    let receiver = i32::try_from(programs.receiver.get()).unwrap();
    let binding = |node_id: i32, instance: UniqueId| ExchangeBinding {
        key: ExchangeKey {
            finst_id_hi: instance.high(),
            finst_id_lo: instance.low(),
            node_id,
        },
        expected_senders: 1,
        receiver_port: Arc::clone(&port),
    };

    let mut extra = ExchangeBindings::default();
    extra.insert(receiver, binding(receiver, CONSUMER_FINST));
    extra.insert(receiver + 97, binding(receiver + 97, CONSUMER_FINST));
    let extra = prepare_error(&programs.consumer, result_sink(), extra, CONSUMER_FINST);
    assert!(extra.contains("has no compiled exchange source"), "{extra}");

    let mut miskeyed = ExchangeBindings::default();
    miskeyed.insert(receiver, binding(receiver + 1, CONSUMER_FINST));
    let miskeyed = prepare_error(&programs.consumer, result_sink(), miskeyed, CONSUMER_FINST);
    assert!(miskeyed.contains("is keyed to node"), "{miskeyed}");

    let mut foreign = ExchangeBindings::default();
    foreign.insert(receiver, binding(receiver, PRODUCER_FINST));
    let foreign = prepare_error(&programs.consumer, result_sink(), foreign, CONSUMER_FINST);
    assert!(
        foreign.contains("belongs to fragment instance"),
        "{foreign}"
    );

    // The receiver projection itself refuses assignments that do not cover
    // exactly the compiled receivers.
    let uncovered = match materialize_compiled_exchange_receivers(
        &programs.consumer,
        CONSUMER_FINST,
        &ExchangeInputAssignments::default(),
        Arc::clone(&port),
        Some(&crate::runtime::fragment::exchange::test_receiver_creation_factory()),
    ) {
        Ok(_) => panic!("a receiver without an assignment must be refused"),
        Err(error) => error,
    };
    assert!(
        uncovered.detail().contains("missing exchange assignment"),
        "{uncovered}"
    );
    let mut surplus = BTreeMap::from([(
        FragmentNodeId::new(receiver),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]);
    surplus.insert(
        FragmentNodeId::new(receiver + 97),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    );
    let surplus = match materialize_compiled_exchange_receivers(
        &programs.consumer,
        CONSUMER_FINST,
        &ExchangeInputAssignments::new(surplus),
        Arc::clone(&port),
        Some(&crate::runtime::fragment::exchange::test_receiver_creation_factory()),
    ) {
        Ok(_) => panic!("an assignment without a receiver must be refused"),
        Err(error) => error,
    };
    assert!(
        surplus.detail().contains("has no compiled exchange source"),
        "{surplus}"
    );
}

#[test]
fn compiled_sink_materialization_refuses_mismatched_capabilities() {
    let programs = programs();
    let transmitter = crate::runtime::fragment::io::exchange::discard_exchange_transmitter;
    let detail = |result: Result<
        Box<dyn OperatorFactory>,
        crate::runtime::fragment::FragmentLaunchError,
    >| match result {
        Ok(_) => panic!("compiled sink must be refused"),
        Err(error) => error.detail().to_string(),
    };

    let no_result_sink = detail(materialize_compiled_sink(
        &programs.consumer,
        &FragmentSinkAssignment::None,
        CONSUMER_FINST,
        transmitter(),
        None,
        None,
    ));
    assert!(
        no_result_sink.contains("requires a result sink"),
        "{no_result_sink}"
    );

    let stray_result_sink = detail(materialize_compiled_sink(
        &programs.producer,
        &stream_assignment(),
        PRODUCER_FINST,
        transmitter(),
        Some(Box::new(ResultSinkFactory::new(ResultSinkHandle::new()))),
        None,
    ));
    assert!(
        stray_result_sink.contains("cannot take a result sink"),
        "{stray_result_sink}"
    );

    let no_destinations = detail(materialize_compiled_sink(
        &programs.producer,
        &FragmentSinkAssignment::None,
        PRODUCER_FINST,
        transmitter(),
        None,
        None,
    ));
    assert!(
        no_destinations.contains("cannot be materialized with assignment none"),
        "{no_destinations}"
    );
}

fn destination(instance: UniqueId) -> FragmentDestination {
    FragmentDestination::new(
        instance,
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        PRODUCER_FINST,
        0,
        1,
    )
    .expect("destination")
}

/// The producer's Values chunk exactly as its root output port presents it.
fn producer_chunk(program: &LocalProgram) -> Chunk {
    let root = program.graph().root();
    let ProgramNodeKind::Values { values } = program.graph().nodes()[root.index()].kind() else {
        panic!("the producer root is its Values node");
    };
    Chunk::new_with_chunk_schema(
        values.batch().expect("constant producer Values").clone(),
        ChunkSchema::from_compiled_layout(values.layout()).expect("compiled layout"),
    )
}

fn sorted(mut rows: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    rows.sort_unstable();
    rows
}

// Values -> Stream(Hash[b, a]) to two consumer instances. The partition keys
// are compiled SlotId roots over the sink's input port; each destination must
// receive exactly the rows the native-exchange placement assigns to it when
// applied to the same key columns in key order.
#[test]
fn compiled_hash_stream_places_rows_by_the_exchange_hash_of_its_key_roots() {
    let programs = programs_shaped(true, &HASH_ROWS);
    let Some(StaticSinkProgram::DataStream { branch, .. }) = programs.producer.graph().sink()
    else {
        panic!("the producer compiles to a single-branch stream sink");
    };
    assert_eq!(
        branch.partition_type(),
        DataStreamPartitionType::HashPartitioned
    );
    assert_eq!(branch.partition_exprs().len(), 2);

    let chunk = producer_chunk(&programs.producer);
    assert_eq!(
        int64_column(&chunk, 0),
        HASH_ROWS.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
        "the Values port is (a, b)"
    );
    let keys: Vec<ArrayRef> = vec![
        Arc::clone(chunk.record_batch().column(1)),
        Arc::clone(chunk.record_batch().column(0)),
    ];
    let expected = partition_chunk_by_hash_arrays(&chunk, &keys, 2, false)
        .expect("oracle placement")
        .iter()
        .map(|part| {
            if part.is_empty() {
                return Vec::new();
            }
            sorted(
                int64_column(part, 1)
                    .into_iter()
                    .zip(int64_column(part, 0))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        expected.iter().all(|rows| !rows.is_empty()),
        "fixture rows reach both destinations: {expected:?}"
    );

    let port = in_process_test_exchange_receiver_port();
    let mut consumer_bindings = Vec::new();
    for instance in [CONSUMER_FINST, CONSUMER_FINST_2] {
        let CompiledExchangeReceivers {
            registrations,
            bindings,
        } = receivers_at(&programs, &port, instance);
        for registration in registrations {
            port.register(registration).expect("register receiver");
        }
        consumer_bindings.push((instance, bindings));
    }

    let producer_sink = materialize_compiled_sink(
        &programs.producer,
        &FragmentSinkAssignment::StreamDestinations {
            destinations: vec![destination(CONSUMER_FINST), destination(CONSUMER_FINST_2)],
            sender_id: None,
        },
        PRODUCER_FINST,
        Arc::new(LoopbackTransmitter {
            port: Arc::clone(&port),
        }),
        None,
        None,
    )
    .expect("compiled hash stream sink");
    run(
        &programs.producer,
        producer_sink,
        ExchangeBindings::default(),
        PRODUCER_FINST,
    );

    for (index, (instance, bindings)) in consumer_bindings.into_iter().enumerate() {
        let output = ResultSinkHandle::new();
        let consumer_sink = materialize_compiled_sink(
            &programs.consumer,
            &FragmentSinkAssignment::None,
            instance,
            crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            Some(Box::new(ResultSinkFactory::new(output.clone()))),
            None,
        )
        .expect("compiled result sink");
        run(&programs.consumer, consumer_sink, bindings, instance);
        let mut rows = Vec::new();
        for chunk in &output.take_chunks() {
            rows.extend(
                int64_column(chunk, 0)
                    .into_iter()
                    .zip(int64_column(chunk, 1)),
            );
        }
        assert_eq!(
            sorted(rows),
            expected[index],
            "destination {index} receives exactly its hash placement as (b, a)"
        );
    }
}

// Intended append to compiled_exchange_tests.rs; reuses its private fixture helpers.
fn drop_distribution_key_project_plan(
    hashed: bool,
    values_rows: &[(i64, i64)],
) -> (PhysicalPlan, NodeId) {
    let edge = EdgeId::new(5);
    let producer_id = FragmentId::new(1);
    let consumer_id = FragmentId::new(2);

    let mut producer = FragmentBuilder::new(producer_id);
    let values = producer.reserve_node_id().unwrap();
    let a = producer
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let b = producer
        .add_value(
            int64(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let mut rows = Vec::new();
    for &(a_value, b_value) in values_rows {
        let a_cell = producer
            .add_expression(
                values,
                int64(),
                ExprKind::Literal(LiteralValue::Int64(a_value)),
            )
            .unwrap();
        let b_cell = producer
            .add_expression(
                values,
                int64(),
                ExprKind::Literal(LiteralValue::Int64(b_value)),
            )
            .unwrap();
        rows.push(vec![a_cell, b_cell].into_boxed_slice());
    }
    producer
        .add_values(values, rows.into_boxed_slice(), Box::from([a, b]))
        .unwrap();
    let producer = producer
        .finish_definition(
            values,
            FragmentSink::Stream { edge },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut consumer = FragmentBuilder::new(consumer_id);
    let exchange = consumer.reserve_node_id().unwrap();
    let b_import = consumer
        .add_value(
            int64(),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: b,
            },
        )
        .unwrap();
    let a_import = consumer
        .add_value(
            int64(),
            ValueOrigin::ExchangeImport {
                edge,
                source_value: a,
            },
        )
        .unwrap();
    consumer
        .add_exchange_source(
            exchange,
            edge,
            Box::from([(b, b_import), (a, a_import)]),
            Box::from([b_import, a_import]),
            if hashed {
                hash([b_import, a_import])
            } else {
                Distribution::Unconstrained
            },
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    let project = consumer.reserve_node_id().unwrap();
    let selected = consumer
        .add_expression(project, int64(), ExprKind::Value(b_import))
        .unwrap();
    consumer
        .add_project(
            project,
            exchange,
            Box::from([(selected, b_import)]),
            Box::from([b_import]),
        )
        .unwrap();
    let consumer = consumer
        .finish_definition(
            project,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    assert_eq!(
        consumer.nodes()[&project].output_properties.distribution,
        Distribution::Unconstrained
    );
    if hashed {
        assert!(matches!(
            consumer.nodes()[&exchange].output_properties.distribution,
            Distribution::Hash { .. }
        ));
    } else {
        assert_eq!(
            consumer.nodes()[&exchange].output_properties.distribution,
            Distribution::Unconstrained
        );
    }
    let (source_distribution, destination_distribution) = if hashed {
        (hash([b, a]), hash([b_import, a_import]))
    } else {
        (Distribution::Unconstrained, Distribution::Unconstrained)
    };
    let output = consumer.nodes()[&project].output.clone();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([73; 16]).unwrap());
    plan.add_fragment(producer).unwrap();
    plan.add_fragment(consumer).unwrap();
    plan.add_edge(Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: producer_id,
            projection: Box::from([b, a]),
        },
        destination: EdgeDestination {
            fragment: consumer_id,
            node: exchange,
            receive_mapping: Box::from([(b, b_import), (a, a_import)]),
        },
        partitioning: EdgePartitioning {
            source: source_distribution,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: destination_distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: consumer_id,
        output,
        fields: Box::from([ResultField {
            domain: crate::test_result_domain::result_value_domain(&int64()),
            name: "b".into(),
            alias: None,
            value: b_import,
            ty: int64(),
        }]),
    })
    .unwrap();
    (plan.finish().unwrap(), exchange)
}

/// Literal cells are the only roots; each gets one eager use.
#[test]
fn checked_hash_receiver_project_may_drop_a_distribution_key() {
    let (plan, _) = drop_distribution_key_project_plan(true, &ROWS);
    let mut checked = packages(&plan);
    let consumer = compile(
        checked.remove(&FragmentId::new(2)).unwrap(),
        Some(NonZeroUsize::new(1).unwrap()),
    );
    assert!(
        consumer
            .graph()
            .nodes()
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::Project { .. }))
    );
}

#[test]
fn unknown_exchange_source_does_not_gain_project_placement() {
    let (plan, receiver) = drop_distribution_key_project_plan(false, &ROWS);
    let mut checked = packages(&plan);
    let consumer = checked.remove(&FragmentId::new(2)).unwrap();
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(consumer), &providers, &FixtureControl).unwrap();
    let result = compile_fragment(
        validated,
        &functions,
        options(Some(NonZeroUsize::new(1).unwrap())),
        &FixtureControl,
    );
    assert!(
        matches!(result, Err(novarocks_local_compiler::FragmentCompileError::Unsupported { node: Some(node), feature: "unconstrained distribution without a runtime-split scan placement" }) if node == receiver)
    );
}

#[test]
fn compiled_receiver_schema_uses_real_creator_until_last_slot_after_program_exit() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    let programs = programs();
    let weak = Arc::downgrade(&programs.consumer);
    let port = in_process_test_exchange_receiver_port();
    let assignment = assignments(programs.receiver);
    let missing = match materialize_compiled_exchange_receivers(
        &programs.consumer,
        CONSUMER_FINST,
        &assignment,
        port.clone(),
        None,
    ) {
        Ok(_) => panic!("receiver schema needs its actual creator"),
        Err(cause) => cause,
    };
    assert!(matches!(missing, ConstructionFailure::MissingOwner));
    let tracker = MemTracker::new_root("compiled receiver independent schema");
    let factory = super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    );
    let receivers = materialize_compiled_exchange_receivers(
        &programs.consumer,
        CONSUMER_FINST,
        &assignment,
        port,
        Some(&factory),
    )
    .unwrap();
    let schema = receivers.registrations[0].expected_chunk_schema.clone();
    let slot = schema.slots()[0].clone();
    assert!(schema.is_typed());
    assert!(
        schema
            .slots()
            .iter()
            .all(|slot| slot.field_ref().metadata().as_arc().is_none())
    );
    drop(receivers);
    drop(factory);
    drop(programs);
    assert!(weak.upgrade().is_none());
    assert!(tracker.current() > 0);
    drop(schema);
    assert!(tracker.current() > 0);
    drop(slot);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn compiled_receiver_schema_first_memory_refusal_rolls_back_without_registration() {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeErrorState;
    let programs = programs();
    let port = in_process_test_exchange_receiver_port();
    let tracker = MemTracker::new_root("compiled receiver first schema refusal");
    let factory = super::aggregate_fixture::runtime_construction_factory(
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    );
    let baseline = tracker.current();
    assert!(baseline > 0);
    tracker.install_limit_once(baseline + 1).unwrap();
    let error = match materialize_compiled_exchange_receivers(
        &programs.consumer,
        CONSUMER_FINST,
        &assignments(programs.receiver),
        port,
        Some(&factory),
    ) {
        Ok(_) => panic!("actual schema allocation crosses its installed tracker limit"),
        Err(cause) => cause,
    };
    assert!(matches!(error, ConstructionFailure::Memory(_)));
    assert_eq!(tracker.current(), baseline);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

// The retained DataStream path must bind the outbound gate at materialization,
// before an operator can send. Multi/split legacy fixture materializers retire
// with their old evaluator; this uses an actual compiled single-branch sink.
#[test]
fn compiled_stream_materialization_keeps_its_exact_edge_gate() {
    let programs = programs();
    let gates =
        crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates::try_new(std::iter::empty())
            .unwrap();
    for gated in [false, true] {
        let factory = materialize_compiled_sink(
            &programs.producer,
            &stream_assignment(),
            PRODUCER_FINST,
            crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
            None,
            gated.then(|| Arc::clone(&gates)),
        )
        .unwrap();
        assert_eq!(factory.is_edge_gated(), gated);
        let expected = format!(
            "EXCHANGE_SINK (id={})",
            programs.producer.graph().root().index()
        );
        assert_eq!(factory.name(), expected);
        assert_eq!(factory.create(1, 0).name(), expected);
    }
}

include!("compiled_router_runtime_tests.rs");
