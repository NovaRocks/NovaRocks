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

//! A two-phase row-count TopN split run end to end through compiled programs
//! only. A Values fragment hashes its rows to two Partial TopN instances, each
//! running two drivers; each instance keeps its own top `limit + offset` rows
//! and a gather stream carries both prefixes to one Final TopN, which
//! publishes the frozen window. The whole three-fragment plan is validated, so
//! the sequence pairing is a checked physical fact.
//!
//! The frames travel unmodified through the real compiled stream sinks and
//! positional receivers, which present each receiver's frozen fields.
//!
//! The oracle is an independent stable Rust sort. Equal keys may resolve to
//! different rows than a one-stage stable sort, because each partial instance
//! breaks its own ties, so a window is checked by its exact key sequence and
//! by drawing every key group from the input rows with that key: exactly that
//! group whenever the window covers the whole group.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram, ProgramNodeKind};
use novarocks_physical_plan::{
    Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning, EdgeSource, ExprKind,
    Fragment, FragmentBuilder, FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink,
    FrozenFragmentCalls, FrozenFragmentPruning, HashDefinition, HashPartitionScheme, NodeId,
    NullOrdering, PartitionCountDomain, PartitionCountParameter, PartitionCountParameterId,
    PartitionSpaceId, PhysicalExpressionRoots, PhysicalPlan, PhysicalRootUses, PipelineDopDomain,
    PlanBuilder, PlanLimits, PlanVersionId, PropertyProofProjectionLimits, ResultField, ResultPort,
    RowMultiplicity, SortDirection, SortExpr, TopNPhase, TopNSequenceId, ValueId, ValueOrigin,
    extract_fragment_packages,
};
use novarocks_type_contract::{
    ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
};
use novarocks_types::UniqueId;

use super::aggregate_fixture::PreparedUnitRecording;
use super::family_fixture::{FixtureControl, cell, constant_policy, int64, values};
use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{RuntimeEndpoint, TaskDestination};
use crate::runtime::task::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::task::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, TaskSinkAssignment,
};
use crate::runtime::task::io::exchange::test_support::{Fixture, UnitLoopback};
use crate::runtime::task::io::{ExchangeFrameTransmitter, ExchangeReceiverPort, NoopTaskEventSink};
use crate::runtime::task::sink::materialize_compiled_sink;

const SOURCE: FragmentId = FragmentId::new(1);
const PARTIAL: FragmentId = FragmentId::new(2);
const FINAL: FragmentId = FragmentId::new(3);
const HASH_EDGE: EdgeId = EdgeId::new(5);
const GATHER_EDGE: EdgeId = EdgeId::new(6);
const SEQUENCE: TopNSequenceId = TopNSequenceId::new(9);
/// Drivers of each partial instance: the TopN gathers them to one.
const PARTIAL_DOP: usize = 2;

const SOURCE_FINST: UniqueId = UniqueId::new(0x81, 0x01);
const PARTIAL_FINSTS: [UniqueId; 2] = [UniqueId::new(0x82, 0x01), UniqueId::new(0x82, 0x02)];
const FINAL_FINST: UniqueId = UniqueId::new(0x83, 0x01);

/// One literal row `(a, b)`: `a` has NULLs and ties, `b` is unique, so `b`
/// alone identifies a row.
type Row = (Option<i64>, i64);

const ROWS: [Row; 16] = [
    (Some(5), 1),
    (None, 2),
    (Some(3), 3),
    (Some(5), 4),
    (Some(-1), 5),
    (Some(3), 6),
    (None, 7),
    (Some(8), 8),
    (Some(5), 9),
    (Some(0), 10),
    (Some(3), 11),
    (None, 12),
    (Some(8), 13),
    (Some(2), 14),
    (Some(5), 15),
    (Some(-7), 16),
];

#[derive(Clone, Copy, Debug)]
struct Key {
    /// 0 reads `a`, 1 reads `b`.
    column: usize,
    ascending: bool,
    nulls_first: bool,
}

const A_DESC_NULLS_FIRST: Key = Key {
    column: 0,
    ascending: false,
    nulls_first: true,
};
const A_ASC_NULLS_LAST: Key = Key {
    column: 0,
    ascending: true,
    nulls_first: false,
};
const A_ASC_NULLS_FIRST: Key = Key {
    column: 0,
    ascending: true,
    nulls_first: true,
};
const B_DESC: Key = Key {
    column: 1,
    ascending: false,
    nulls_first: false,
};

fn pick(row: &Row, key: Key) -> Option<i64> {
    if key.column == 0 { row.0 } else { Some(row.1) }
}

fn compare(left: Option<i64>, right: Option<i64>, key: Key) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) if key.nulls_first => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) if key.nulls_first => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(l), Some(r)) if key.ascending => l.cmp(&r),
        (Some(l), Some(r)) => r.cmp(&l),
    }
}

fn key_of(row: &Row, keys: &[Key]) -> Vec<Option<i64>> {
    keys.iter().map(|key| pick(row, *key)).collect()
}

/// Independent stable sort of `rows` by `keys`, then `[offset, offset+limit)`.
fn oracle(rows: &[Row], keys: &[Key], limit: usize, offset: usize) -> Vec<Row> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|left, right| {
        keys.iter()
            .map(|key| compare(pick(left, *key), pick(right, *key), *key))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    sorted.into_iter().skip(offset).take(limit).collect()
}

/// `actual` is exactly a `[offset, offset+limit)` window of `input` under
/// `keys`: the oracle's key sequence, in order, drawn from distinct input rows
/// with those keys, and every key group the window covers whole is that group.
fn assert_window(actual: &[Row], input: &[Row], keys: &[Key], limit: usize, offset: usize) {
    let expected = oracle(input, keys, limit, offset);
    assert_eq!(
        actual
            .iter()
            .map(|row| key_of(row, keys))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|row| key_of(row, keys))
            .collect::<Vec<_>>(),
        "window key sequence: actual {actual:?}, oracle {expected:?}"
    );
    let input_rows = input.iter().copied().collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for row in actual {
        assert!(input_rows.contains(row), "row {row:?} is not an input row");
        assert!(seen.insert(row.1), "row {row:?} is emitted twice");
    }
    let mut groups = BTreeMap::<Vec<Option<i64>>, usize>::new();
    for row in &expected {
        *groups.entry(key_of(row, keys)).or_default() += 1;
    }
    for (group, count) in groups {
        let whole = input
            .iter()
            .filter(|row| key_of(row, keys) == group)
            .copied()
            .collect::<BTreeSet<_>>();
        if whole.len() == count {
            let kept = actual
                .iter()
                .filter(|row| key_of(row, keys) == group)
                .copied()
                .collect::<BTreeSet<_>>();
            assert_eq!(kept, whole, "a covered tie group {group:?} is kept whole");
        }
    }
}

fn sort_exprs(
    builder: &mut FragmentBuilder,
    node: NodeId,
    columns: [ValueId; 2],
    keys: &[Key],
) -> Box<[SortExpr]> {
    keys.iter()
        .map(|key| SortExpr {
            expr: builder
                .add_expression(
                    node,
                    int64(key.column == 0),
                    ExprKind::Value(columns[key.column]),
                )
                .unwrap(),
            direction: if key.ascending {
                SortDirection::Ascending
            } else {
                SortDirection::Descending
            },
            null_ordering: if key.nulls_first {
                NullOrdering::First
            } else {
                NullOrdering::Last
            },
        })
        .collect()
}

fn dop(max: u32) -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max,
        requires_power_of_two: false,
    }
}

/// A native-exchange hash scheme over `b` alone.
fn hash(key: ValueId) -> Distribution {
    Distribution::Hash {
        keys: Box::from([key]),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([51; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([52; 32]).unwrap(),
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

/// Imports `(a, b)` of `edge` into `builder` as an ExchangeSource at `node`.
fn receive(
    builder: &mut FragmentBuilder,
    node: NodeId,
    edge: EdgeId,
    sources: [ValueId; 2],
    distribution: impl Fn([ValueId; 2]) -> Distribution,
) -> [ValueId; 2] {
    let imports = [0, 1].map(|column| {
        builder
            .add_value(
                int64(column == 0),
                ValueOrigin::ExchangeImport {
                    edge,
                    source_value: sources[column],
                },
            )
            .unwrap()
    });
    builder
        .add_exchange_source(
            node,
            edge,
            Box::from([(sources[0], imports[0]), (sources[1], imports[1])]),
            Box::from(imports),
            distribution(imports),
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    imports
}

fn edge(
    id: EdgeId,
    from: (FragmentId, [ValueId; 2]),
    to: (FragmentId, NodeId, [ValueId; 2]),
    source: Distribution,
    destination: Distribution,
) -> Edge {
    Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: from.0,
            projection: Box::from(from.1),
        },
        destination: EdgeDestination {
            fragment: to.0,
            node: to.1,
            receive_mapping: Box::from([(from.1[0], to.2[0]), (from.1[1], to.2[1])]),
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    }
}

/// `Values(a, b) -> Stream(Hash[b])` | `ExchangeSource -> Partial TopN ->
/// Stream(Gather)` | `ExchangeSource -> Final TopN -> Result`.
fn plan(keys: &[Key], limit: u64, offset: u64) -> PhysicalPlan {
    let mut source = FragmentBuilder::new(SOURCE);
    let values_node = source.reserve_node_id().unwrap();
    let rows = ROWS
        .iter()
        .map(|(a, b)| vec![cell(*a), cell(Some(*b))])
        .collect::<Vec<_>>();
    let columns = values(
        &mut source,
        values_node,
        &[int64(true), int64(false)],
        &rows,
    );
    let columns = [columns[0], columns[1]];
    let source = source
        .finish_definition(
            values_node,
            FragmentSink::Stream { edge: HASH_EDGE },
            dop(1),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut partial = FragmentBuilder::new(PARTIAL);
    let partial_receiver = partial.reserve_node_id().unwrap();
    let partial_columns = receive(&mut partial, partial_receiver, HASH_EDGE, columns, |c| {
        hash(c[1])
    });
    let partial_topn = partial.reserve_node_id().unwrap();
    let order = sort_exprs(&mut partial, partial_topn, partial_columns, keys);
    // A partial carries the whole final window as its row budget, no offset.
    partial
        .add_top_n(
            partial_topn,
            partial_receiver,
            order,
            limit + offset,
            0,
            TopNPhase::Partial { sequence: SEQUENCE },
        )
        .unwrap();
    let partial = partial
        .finish_definition(
            partial_topn,
            FragmentSink::Stream { edge: GATHER_EDGE },
            dop(u32::try_from(PARTIAL_DOP).unwrap()),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut last = FragmentBuilder::new(FINAL);
    let final_receiver = last.reserve_node_id().unwrap();
    let final_columns = receive(
        &mut last,
        final_receiver,
        GATHER_EDGE,
        partial_columns,
        |_| Distribution::Singleton,
    );
    let final_topn = last.reserve_node_id().unwrap();
    let order = sort_exprs(&mut last, final_topn, final_columns, keys);
    last.add_top_n(
        final_topn,
        final_receiver,
        order,
        limit,
        offset,
        TopNPhase::Final { sequence: SEQUENCE },
    )
    .unwrap();
    let last = last
        .finish_definition(
            final_topn,
            FragmentSink::Result,
            dop(1),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let output = last.nodes()[&final_topn].output.clone();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([83; 16]).unwrap());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(partial).unwrap();
    plan.add_fragment(last).unwrap();
    plan.add_edge(edge(
        HASH_EDGE,
        (SOURCE, columns),
        (PARTIAL, partial_receiver, partial_columns),
        hash(columns[1]),
        hash(partial_columns[1]),
    ))
    .unwrap();
    // Gather: the edge carries every partial row to the one Final.
    plan.add_edge(edge(
        GATHER_EDGE,
        (PARTIAL, partial_columns),
        (FINAL, final_receiver, final_columns),
        Distribution::Singleton,
        Distribution::Singleton,
    ))
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: FINAL,
        output,
        fields: Box::from([
            ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64(true)),
                name: "a".into(),
                alias: None,
                value: final_columns[0],
                ty: int64(true),
            },
            ResultField {
                domain: crate::test_result_domain::result_value_domain(&int64(false)),
                name: "b".into(),
                alias: None,
                value: final_columns[1],
                ty: int64(false),
            },
        ]),
    })
    .unwrap();
    plan.finish()
        .unwrap_or_else(|error| panic!("the split TopN plan validates: {error:?}"))
}

/// Every root is a literal cell or a value read; each gets one eager use.
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
        novarocks_type_contract::CompilePhase::Validate,
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

fn compile(package: FragmentPackage, pipeline_dop: usize, result: bool) -> Arc<LocalProgram> {
    let functions = crate::exec::expr::compiled_program::tests::rng_subset();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    let options = LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
        // Only a Result root has a frozen placement; a stream sink has none.
        root_sink_dop: result.then(|| NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
    };
    Arc::new(
        compile_fragment(validated, &functions, options, &FixtureControl)
            .unwrap_or_else(|error| panic!("split fragment compiles: {error}")),
    )
}

/// Delivers every encoded frame to the receiver port, as the native data plane
/// does after its wire hop, and records the rows each frame carried between
/// each sender and destination instance.
struct RecordingTransmitter {
    unit: Arc<UnitLoopback>,
    rows: Mutex<BTreeMap<(UniqueId, UniqueId), Vec<Row>>>,
    recordings: Mutex<BTreeMap<(UniqueId, i32), Arc<PreparedUnitRecording>>>,
}

impl RecordingTransmitter {
    fn rows(&self, sender: UniqueId, destination: UniqueId) -> Vec<Row> {
        self.rows
            .lock()
            .unwrap()
            .get(&(sender, destination))
            .cloned()
            .unwrap_or_default()
    }
}

impl ExchangeFrameTransmitter for RecordingTransmitter {
    fn deliver_unit(
        &self,
        frame: crate::runtime::task::io::exchange_unit::ExchangeUnitFrame,
        stop: tokio_util::sync::CancellationToken,
        activity: crate::runtime::task::io::exchange_unit::ExchangeCallActivity,
        done: crate::runtime::task::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        let mut carried = Vec::new();
        if !frame.receiver.payload.is_empty() {
            let key = (
                frame.receiver.stream.source().task_key(),
                frame.receiver.stream.destination_node().get(),
            );
            let recording = self
                .recordings
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .expect("sender installed its exact branch before Unit delivery");
            for chunk in &recording.chunks(&frame.receiver.payload) {
                carried.extend(rows_of(chunk));
            }
        }
        self.rows
            .lock()
            .unwrap()
            .entry((
                frame.receiver.stream.source().task_key(),
                frame.receiver.stream.destination_task_key(),
            ))
            .or_default()
            .extend(carried);
        self.unit.deliver_unit(frame, stop, activity, done);
    }
    fn register_unit_demands(
        &self,
        destination: RuntimeEndpoint,
        entries: Vec<crate::runtime::task::io::exchange_unit::ExchangeDemandEntry>,
        stop: tokio_util::sync::CancellationToken,
        activity: crate::runtime::task::io::exchange_unit::ExchangeCallActivity,
        done: crate::runtime::task::io::exchange_unit::ExchangeDemandCallback,
    ) {
        self.unit
            .register_unit_demands(destination, entries, stop, activity, done)
    }
    fn watch_unit_facts(
        &self,
        destination: RuntimeEndpoint,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        wake: crate::runtime::task::io::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        self.unit.watch_unit_facts(destination, stream, wake)
    }
    fn forget_unit_facts(&self, stream: novarocks_execution_contract::ExchangeStreamIdentity) {
        self.unit.forget_unit_facts(stream)
    }
    fn schedule_unit_retry(
        &self,
        delay: Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        self.unit.schedule_unit_retry(delay, task)
    }
}

/// Rows of a chunk whose two columns are `(a, b)`.
fn rows_of(chunk: &Chunk) -> Vec<Row> {
    let column = |index: usize| {
        chunk
            .record_batch()
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column")
            .clone()
    };
    let (a, b) = (column(0), column(1));
    (0..chunk.len())
        .map(|row| ((!a.is_null(row)).then(|| a.value(row)), b.value(row)))
        .collect()
}

fn run(
    fixture: &Fixture,
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) {
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        bindings,
        Some((instance.high(), instance.low())),
        dop,
        fixture.state(instance),
        Arc::new(NoopTaskEventSink),
    )
    .expect("compiled split fragment prepares drivers")
    .start()
    .join()
    .expect("compiled split fragment runs");
}

/// The single receiver of `program`, registered for `instance` with
/// `senders` expected senders.
fn register(
    fixture: &Fixture,
    program: &LocalProgram,
    instance: UniqueId,
    sources: &[UniqueId],
    port: &Arc<dyn ExchangeReceiverPort>,
) -> ExchangeBindings {
    let (_, input) = program
        .exchange_inputs()
        .iter()
        .next()
        .expect("one compiled exchange input");
    let receiver = FragmentNodeId::new(i32::try_from(input.receiver_node).unwrap());
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = materialize_compiled_exchange_receivers(
        program,
        instance,
        &ExchangeInputAssignments::new(BTreeMap::from([(
            receiver,
            ExchangeInputAssignment::new(NonZeroUsize::new(sources.len()).unwrap()),
        )])),
        Arc::clone(port),
        Some(&fixture.creation(instance)),
    )
    .expect("compiled receivers");
    for registration in registrations {
        let key = registration.key;
        port.register(registration).expect("register receiver");
        fixture.bind(key, sources);
    }
    bindings
}

fn destination(instance: UniqueId, sender: UniqueId, ordinal: u32, count: u32) -> TaskDestination {
    TaskDestination::new(
        instance,
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        sender,
        ordinal,
        count,
    )
    .expect("destination")
}

fn stream_sink(
    fixture: &Fixture,
    program: &Arc<LocalProgram>,
    instance: UniqueId,
    destinations: Vec<TaskDestination>,
    transmitter: &Arc<RecordingTransmitter>,
) -> Box<dyn OperatorFactory> {
    let (node, recording) = PreparedUnitRecording::new(program, fixture.creation(instance));
    assert!(
        transmitter
            .recordings
            .lock()
            .unwrap()
            .insert((instance, node), Arc::new(recording))
            .is_none(),
        "each source branch has one original recording declaration"
    );
    fixture
        .sink(
            program,
            &TaskSinkAssignment::StreamDestinations {
                destinations,
                sender_id: None,
            },
            instance,
            Arc::clone(transmitter) as Arc<dyn ExchangeFrameTransmitter>,
            None,
            None,
        )
        .expect("compiled stream sink")
}

/// What one split run observed.
struct SplitRun {
    /// Rows each partial instance received from the hash stream.
    partial_inputs: [Vec<Row>; 2],
    /// Rows each partial instance sent over the gather stream, in order.
    partial_outputs: [Vec<Row>; 2],
    /// The Final's published rows, in order.
    result: Vec<Row>,
}

fn run_split(keys: &[Key], limit: u64, offset: u64) -> SplitRun {
    let plan = plan(keys, limit, offset);
    let mut packages = packages(&plan);
    let source = compile(packages.remove(&SOURCE).unwrap(), 1, false);
    let partial = compile(packages.remove(&PARTIAL).unwrap(), PARTIAL_DOP, false);
    let last = compile(packages.remove(&FINAL).unwrap(), 1, true);

    // Both phases lower to the ordinary local TopN: the partial with its
    // whole-window budget and no offset, the final with the frozen window.
    let topn = |program: &LocalProgram| {
        let root = program.graph().root();
        match program.graph().nodes()[root.index()].kind() {
            ProgramNodeKind::Sort {
                use_top_n: true,
                limit,
                offset,
                ..
            } => (*limit, *offset),
            other => panic!("the split root is a local TopN, got {other:?}"),
        }
    };
    let window = usize::try_from(limit + offset).unwrap();
    assert_eq!(topn(&partial), (Some(window), 0));
    assert_eq!(
        topn(&last),
        (
            Some(usize::try_from(limit).unwrap()),
            usize::try_from(offset).unwrap()
        )
    );
    assert_eq!(partial.graph().profile().pipeline_dop().get(), PARTIAL_DOP);

    let fixture = Fixture::new();
    let port = fixture.port.clone();
    let transmitter = Arc::new(RecordingTransmitter {
        unit: fixture.transport.clone(),
        rows: Mutex::default(),
        recordings: Mutex::default(),
    });
    let partial_bindings = PARTIAL_FINSTS
        .map(|instance| register(&fixture, &partial, instance, &[SOURCE_FINST], &port));
    // Both partial instances send to the one Final.
    let final_bindings = register(&fixture, &last, FINAL_FINST, &PARTIAL_FINSTS, &port);

    let sink = stream_sink(
        &fixture,
        &source,
        SOURCE_FINST,
        PARTIAL_FINSTS
            .iter()
            .map(|instance| destination(*instance, SOURCE_FINST, 0, 1))
            .collect(),
        &transmitter,
    );
    run(
        &fixture,
        &source,
        sink,
        ExchangeBindings::default(),
        SOURCE_FINST,
    );

    for (ordinal, (instance, bindings)) in
        PARTIAL_FINSTS.into_iter().zip(partial_bindings).enumerate()
    {
        let sink = stream_sink(
            &fixture,
            &partial,
            instance,
            vec![destination(
                FINAL_FINST,
                instance,
                u32::try_from(ordinal).unwrap(),
                u32::try_from(PARTIAL_FINSTS.len()).unwrap(),
            )],
            &transmitter,
        );
        run(&fixture, &partial, sink, bindings, instance);
    }

    let output = ResultSinkBuffer::new();
    let sink = materialize_compiled_sink(
        &last,
        &TaskSinkAssignment::None,
        FINAL_FINST,
        crate::runtime::task::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink");
    run(&fixture, &last, sink, final_bindings, FINAL_FINST);

    SplitRun {
        partial_inputs: PARTIAL_FINSTS.map(|instance| transmitter.rows(SOURCE_FINST, instance)),
        partial_outputs: PARTIAL_FINSTS.map(|instance| transmitter.rows(instance, FINAL_FINST)),
        result: output.take_chunks().iter().flat_map(rows_of).collect(),
    }
}

/// Runs the split and checks every observed stage against the oracle.
fn assert_split(keys: &[Key], limit: usize, offset: usize) -> SplitRun {
    let run = run_split(keys, limit as u64, offset as u64);
    // The hash stream placed every row on exactly one partial instance, and
    // both instances hold rows, so the Final really merges two prefixes.
    let mut placed = run.partial_inputs.concat();
    placed.sort_by_key(|row| row.1);
    assert_eq!(placed, ROWS.to_vec());
    assert!(
        run.partial_inputs.iter().all(|rows| !rows.is_empty()),
        "both partial instances receive rows: {:?}",
        run.partial_inputs
    );
    // Each partial emits its own instance's top `limit + offset` rows as one
    // ordered stream, which is exactly what it declares.
    for (input, output) in run.partial_inputs.iter().zip(&run.partial_outputs) {
        assert_window(output, input, keys, limit + offset, 0);
    }
    // The Final publishes the frozen window of the whole relation.
    assert_window(&run.result, &ROWS, keys, limit, offset);
    run
}

#[test]
fn split_topn_with_ties_and_nulls_publishes_the_frozen_window_through_two_partials() {
    // `a DESC NULLS FIRST`: three NULLs, then two 8s, then four 5s. The
    // window [2, 5) takes one of the NULL rows and both 8s.
    let run = assert_split(&[A_DESC_NULLS_FIRST], 3, 2);
    assert_eq!(
        run.result.iter().map(|row| row.0).collect::<Vec<_>>(),
        [None, Some(8), Some(8)]
    );
    // The window [1, 6) cuts both the NULL and the 5s tie groups: any of
    // their rows are a correct answer, checked by key and membership.
    let run = assert_split(&[A_DESC_NULLS_FIRST], 5, 1);
    assert_eq!(
        run.result.iter().map(|row| row.0).collect::<Vec<_>>(),
        [None, None, Some(8), Some(8), Some(5)]
    );
}

#[test]
fn split_topn_with_a_total_order_matches_the_oracle_row_for_row() {
    // `b` breaks every `a` tie, so the window is a unique row sequence.
    let keys = [A_ASC_NULLS_LAST, B_DESC];
    let run = assert_split(&keys, 4, 1);
    assert_eq!(run.result, oracle(&ROWS, &keys, 4, 1));
    assert_eq!(
        run.result,
        [(Some(-1), 5), (Some(0), 10), (Some(2), 14), (Some(3), 11)]
    );
}

#[test]
fn split_topn_window_past_the_input_keeps_only_the_rows_that_exist() {
    // NULLS FIRST ascending: the last two rows are the two 8s.
    let run = assert_split(&[A_ASC_NULLS_FIRST], 4, 14);
    assert_eq!(
        run.result.iter().map(|row| row.0).collect::<Vec<_>>(),
        [Some(8), Some(8)]
    );
    // An offset beyond every row publishes nothing, though each partial
    // still forwards its whole instance input under the larger budget.
    let run = assert_split(&[A_ASC_NULLS_FIRST], 2, 20);
    assert!(run.result.is_empty());
    assert_eq!(
        run.partial_outputs.iter().map(Vec::len).sum::<usize>(),
        ROWS.len()
    );
}
