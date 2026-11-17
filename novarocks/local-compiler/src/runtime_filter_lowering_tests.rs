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

//! Runtime-filter plans built through the real physical builders, package
//! extraction (whose cuts carry each fragment's slice of the plan-global
//! binding numbering) and pure provider validation, then compiled into the
//! final LocalProgram owner.

use novarocks_expression::StaticExprKind;

use super::scan_lowering_tests::{
    FixtureControl, Trace, binding, functions, int64, options, package_admission, payload,
    providers, public_schema,
};
use super::*;
use crate::test_units::PureUnitFixture;
use arrow_schema::DataType;
use novarocks_connector_contract::*;
use novarocks_expression_contract::{
    BoundFunction, ExprKind, LegacyBindingMetadata, PhysicalCallRequest, RootOrdinal, RootScope,
};
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregatePreparationOptions,
    AggregateStateFormatIdentity, CallArgumentUses, CallEffectInput, EngineFunctionCatalogBuilder,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedRelationalEffects,
};
use novarocks_local_program::{
    BindingRequirement, FilterConsumerActivation, FilterNullSemantics, FilterProducerKind,
    FilterReduction, LocalProgram, ProgramNodeId, ProgramNodeKind, StaticFilterContract,
};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    CallProofScope, DecimalOverflowPolicy, EvaluationDemand, ExpressionEffects, RelationCallId,
    RelationEffectContext, SemanticParameters, ValueLogicalType,
};
use std::num::NonZeroU64;

fn zero_port_root(
    builder: &mut FragmentBuilder,
    node: NodeId,
    ty: ValueType,
    kind: ExprKind,
) -> RootOrdinal {
    let definition = builder.fixture_definition(node, ty, kind).unwrap();
    builder
        .fixture_occurrence(
            node,
            definition,
            EvaluationDemand::Value,
            RootScope::AllPorts,
        )
        .unwrap()
}
fn seal_fixture_units(builder: &mut FragmentBuilder) {
    let nodes = builder
        .construction_nodes()
        .filter_map(|(&id, _)| builder.construction_unit(id).is_some().then_some(id))
        .collect::<Vec<_>>();
    for node in nodes {
        builder.finish_fixture_unit(node, &FixtureControl).unwrap();
    }
}
trait FixtureFinish {
    fn finish_fixture_definition(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Result<Fragment, ValidationErrors>;
    fn finish_fixture_structure(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Fragment;
}
impl FixtureFinish for FragmentBuilder {
    fn finish_fixture_definition(
        mut self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Result<Fragment, ValidationErrors> {
        seal_fixture_units(&mut self);
        self.finish_definition(root, sink, dop, placement)
    }
    fn finish_fixture_structure(
        mut self,
        root: NodeId,
        sink: FragmentSink,
        dop: PipelineDopDomain,
        placement: StagePlacementPolicy,
    ) -> Fragment {
        seal_fixture_units(&mut self);
        self.finish_structure(
            root,
            sink,
            dop,
            PlanLimits::FROZEN,
            &FixtureControl,
            placement,
        )
        .unwrap()
    }
}

/// The scan fragment of every shape; it also hosts the join when the join
/// is not partitioned.
const PROBE: FragmentId = FragmentId::new(1);
const GATHER: FragmentId = FragmentId::new(2);
const BUILD: FragmentId = FragmentId::new(3);
/// The partitioned join fragment.
const JOINER: FragmentId = FragmentId::new(4);
const EDGE: EdgeId = EdgeId::new(5);
const BUILD_EDGE: EdgeId = EdgeId::new(6);
const PROBE_EDGE: EdgeId = EdgeId::new(7);
const SCAN: NodeId = NodeId::new(10);
const BUILD_RECEIVER: NodeId = NodeId::new(12);
const JOIN: NodeId = NodeId::new(13);
const PROBE_RECEIVER: NodeId = NodeId::new(14);
const AGGREGATE: NodeId = NodeId::new(15);
const TOPN: NodeId = NodeId::new(16);
const PROJECT: NodeId = NodeId::new(17);
const FINAL: NodeId = NodeId::new(18);
const RECEIVER: NodeId = NodeId::new(20);
const VALUES: NodeId = NodeId::new(30);
const FILTER: RuntimeFilterId = RuntimeFilterId::new(31);
const EQUALITY: RuntimeFilterEqualityWitnessId = RuntimeFilterEqualityWitnessId::new(41);
const WITNESS: RuntimeFilterWitnessId = RuntimeFilterWitnessId::new(51);

fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}

fn nullable_int64(nullable: bool) -> ValueType {
    ValueType::new(DataType::Int64, nullable)
}

fn hash(keys: &[ValueId]) -> Distribution {
    Distribution::Hash {
        keys: Box::from(keys),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([61; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([62; 32]).unwrap(),
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

fn properties(distribution: Distribution, multiplicity: RowMultiplicity) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: multiplicity,
        ordering: Box::default(),
    }
}

fn relation_payload() -> ConnectorReadRelationPayload {
    let binding = binding();
    ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(&binding, ConnectorCodecCategory::ReadTable, b"table"),
        payload(&binding, ConnectorCodecCategory::ReadView, b"view"),
    )
}

/// The first `width` provider columns `c0, c1`, read as `v0, v1`.
fn columns(width: usize) -> Vec<ProviderColumnReference> {
    let binding = binding();
    [b"c0" as &'static [u8], b"c1"][..width]
        .iter()
        .map(|bytes| ProviderColumnReference {
            column_payload: payload(&binding, ConnectorCodecCategory::ReadColumn, bytes),
        })
        .collect()
}

/// `SELECT v0, v1 FROM t`: one runtime-split read whose output is its two
/// provider columns.
fn add_scan(builder: &mut FragmentBuilder, distribution: Distribution) -> [ValueId; 2] {
    let [v0, v1] = add_scan_columns(builder, distribution, 2)[..] else {
        unreachable!("two provider columns")
    };
    [v0, v1]
}

/// One runtime-split read whose output is its first `width` provider columns.
fn add_scan_columns(
    builder: &mut FragmentBuilder,
    distribution: Distribution,
    width: usize,
) -> Vec<ValueId> {
    add_scan_typed(builder, distribution, &vec![int64(); width])
}

fn add_scan_typed(
    builder: &mut FragmentBuilder,
    distribution: Distribution,
    types: &[ValueType],
) -> Vec<ValueId> {
    add_scan_typed_at(
        builder,
        distribution,
        types,
        SCAN,
        ProviderReadOccurrenceId::new(0),
    )
}

fn add_scan_typed_at(
    builder: &mut FragmentBuilder,
    distribution: Distribution,
    types: &[ValueType],
    node: NodeId,
    occurrence: ProviderReadOccurrenceId,
) -> Vec<ValueId> {
    let columns = columns(types.len());
    let provider = columns
        .iter()
        .zip(types)
        .map(|(column, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::ProviderField {
                        scan_node: node,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_scan(
            node,
            NodeKind::Scan {
                occurrence,
                relation: Box::new(Relation::Data(DataRelation {
                    read: ProviderReadReference {
                        binding: binding(),
                        input_version: ExactInputVersion::try_new([9]).unwrap(),
                        relation: relation_payload(),
                    },

                    selection_digest: [7; 32],
                    schema: columns
                        .iter()
                        .zip(types)
                        .map(|(column, ty)| RelationField {
                            column: column.clone(),
                            ty: ty.clone(),
                        })
                        .collect(),
                    predicate_guarantees: Box::default(),
                    provided_properties: properties(distribution, RowMultiplicity::SingleCopy),
                })),
                read_budget: ScanReadBudget {
                    max_batch_rows: 100,
                    max_batch_bytes: 4096,
                },
                provider_outputs: columns.iter().cloned().zip(provider.clone()).collect(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
            provider.clone().into_boxed_slice(),
        )
        .unwrap();
    provider
}

/// The complete frozen read of the scan, whose dynamic filters name the
/// runtime filters its scan-source consumers read.
fn frozen_read(
    singleton: bool,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
) -> FrozenConnectorRead {
    frozen_read_columns(singleton, 2, dynamic_filters)
}

/// The frozen read of a scan of the first `width` provider columns.
fn frozen_read_columns(
    singleton: bool,
    width: usize,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
) -> FrozenConnectorRead {
    let schema = public_schema();
    frozen_read_typed(
        singleton,
        &vec![ConnectorValueType::BigInt; width],
        arrow_schema::Schema::new(schema.fields()[..width].to_vec()),
        dynamic_filters,
    )
}

fn frozen_read_typed(
    singleton: bool,
    types: &[ConnectorValueType],
    schema: arrow_schema::Schema,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
) -> FrozenConnectorRead {
    let values = schema
        .fields()
        .iter()
        .map(|field| ValueType::new(field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    frozen_read_declared_typed(singleton, types, schema, values, dynamic_filters)
}

fn frozen_read_declared_typed(
    singleton: bool,
    types: &[ConnectorValueType],
    schema: arrow_schema::Schema,
    values: Box<[ValueType]>,
    dynamic_filters: Vec<StaticScanDynamicFilter>,
) -> FrozenConnectorRead {
    let width = types.len();
    let draft = ConnectorReadRelationRecipeDraft::try_new(
        binding(),
        relation_payload(),
        columns(width)
            .iter()
            .map(|column| column.column_payload.clone())
            .collect(),
    )
    .unwrap();
    let scan = FrozenConnectorScan::try_new(
        draft,
        ["v0", "v1"][..width]
            .iter()
            .zip(types)
            .map(|(name, ty)| StaticScanAssignment::new(Arc::from(*name), *ty))
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        dynamic_filters,
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
    )
    .unwrap();
    let source = ConnectorReadStaticFacts::try_new(
        ConnectorReadInputVersion::try_new([9]).unwrap(),
        [7; 32],
        ConnectorReadProperties::try_new(
            if singleton {
                ConnectorReadDistribution::Singleton
            } else {
                ConnectorReadDistribution::Unconstrained
            },
            vec![],
        )
        .unwrap(),
        ConnectorReadArtifactCoverage::NoArtifactInputs,
        vec![],
    )
    .unwrap();
    let public =
        crate::scan_lowering_tests::typed_public_schema_with_types(source, None, schema, values);
    FrozenConnectorRead::try_new(scan, public).unwrap()
}

/// `VALUES (7), (8), (20)` as the one-column build source of `edge`.
fn build_fragment(edge: EdgeId, nullable: bool) -> (Fragment, ValueId, ConstantPools) {
    let mut constants = ConstantPools::empty();
    let mut build = FragmentBuilder::new(BUILD);
    let column = build
        .add_value(
            nullable_int64(nullable),
            ValueOrigin::NodeOutput {
                node: VALUES,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let rows = [7, 8, 20]
        .map(|value| {
            Box::from([zero_port_root(
                &mut build,
                VALUES,
                nullable_int64(nullable),
                crate::test_constants::i64(
                    &mut constants,
                    &nullable_int64(nullable),
                    value,
                    options(None).constants,
                ),
            )])
        })
        .into_iter()
        .collect();
    build.add_values(VALUES, rows, Box::from([column])).unwrap();
    let fragment = build
        .finish_fixture_definition(
            VALUES,
            FragmentSink::Stream { edge },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    (fragment, column, constants)
}

fn receive(
    builder: &mut FragmentBuilder,
    node: NodeId,
    edge: EdgeId,
    sources: &[ValueId],
    ty: ValueType,
    distribution: impl Fn(&[ValueId]) -> Distribution,
    multiplicity: RowMultiplicity,
) -> Vec<ValueId> {
    let imports = sources
        .iter()
        .map(|&source_value| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::ExchangeImport { edge, source_value },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_exchange_source(
            node,
            edge,
            sources
                .iter()
                .copied()
                .zip(imports.iter().copied())
                .collect(),
            imports.clone().into_boxed_slice(),
            distribution(&imports),
            multiplicity,
        )
        .unwrap();
    imports
}

fn edge(
    id: EdgeId,
    from: (FragmentId, &[ValueId]),
    to: (FragmentId, NodeId, &[ValueId]),
    source: Distribution,
    destination: Distribution,
) -> Edge {
    let replicated = destination == Distribution::Broadcast;
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
            receive_mapping: from.1.iter().copied().zip(to.2.iter().copied()).collect(),
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: if replicated {
                RowMultiplicity::Replicated
            } else {
                RowMultiplicity::SingleCopy
            },
            routing_only: Box::default(),
        },
    }
}

/// `JOIN ON probe = build` with the build on the right, publishing the probe
/// columns.
fn add_join(
    builder: &mut FragmentBuilder,
    probe: (NodeId, &[ValueId]),
    build: (NodeId, ValueId),
    distribution: JoinDistribution,
    null_safe: bool,
) {
    let key_type = |builder: &FragmentBuilder, value| builder.value(value).unwrap().ty.clone();
    let left_type = key_type(builder, probe.1[0]);
    let right_type = key_type(builder, build.1);
    let key = JoinKey {
        left: builder
            .fixture_value_root(JOIN, left_type, ExprKind::Value(probe.1[0]))
            .unwrap(),
        right: {
            let definition = builder
                .fixture_definition(JOIN, right_type, ExprKind::Value(build.1))
                .unwrap();
            builder
                .fixture_occurrence(
                    JOIN,
                    definition,
                    EvaluationDemand::Value,
                    RootScope::Port(1),
                )
                .unwrap()
        },
        null_safe,
    };
    let probe_properties = builder.node_output_properties(probe.0).unwrap().clone();
    let build_properties = builder.node_output_properties(build.0).unwrap().clone();
    let output_distribution = probe_properties.distribution.clone();
    builder
        .add_join(
            JOIN,
            [probe.0, build.0],
            Box::from([probe_properties, build_properties]),
            Box::from(probe.1),
            output_distribution,
            NodeKind::HashJoin {
                kind: JoinKind::Inner,
                build_side: JoinSide::Right,
                distribution,
                null_extended: Box::default(),
                condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                    keys: Box::from([key]),
                    residual: None,
                },
            },
        )
        .unwrap();
}

/// The single-producer coverage each join mode requires.
fn coverage(distribution: JoinDistribution) -> RuntimeFilterCoverage {
    match distribution {
        JoinDistribution::BroadcastBuild => RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::Witness(WITNESS),
                RuntimeFilterCoverageNode::AnyOf {
                    children: Box::from([0]),
                },
            ]),
            root: 1,
        },
        JoinDistribution::Partitioned => RuntimeFilterCoverage {
            nodes: Box::from([
                RuntimeFilterCoverageNode::Witness(WITNESS),
                RuntimeFilterCoverageNode::AllOf {
                    children: Box::from([0]),
                },
            ]),
            root: 1,
        },
        JoinDistribution::Colocated | JoinDistribution::Singleton => RuntimeFilterCoverage {
            nodes: Box::from([RuntimeFilterCoverageNode::Witness(WITNESS)]),
            root: 0,
        },
    }
}

/// Which consumer the join's filter has.
#[derive(Clone, Copy, PartialEq)]
enum Consumer {
    /// A blocking scan-source consumer of the probe scan's `v0`.
    BlockingScan,
    /// The same scan consumer under a non-blocking activation.
    LiveScan,
    StartUnfilteredScan,
    /// A blocking consumer of the join's own probe key.
    ProbeKey,
}

#[derive(Clone, Copy)]
pub(super) struct Rf {
    null_safe: bool,
    consumer: Consumer,
    /// An ordered-hull domain instead of a membership domain.
    ordered: bool,
}
impl Rf {
    pub(super) const fn blocking() -> Self {
        Self {
            null_safe: false,
            consumer: Consumer::BlockingScan,
            ordered: false,
        }
    }
    const fn scan_consumer(self) -> bool {
        !matches!(self.consumer, Consumer::ProbeKey)
    }
}

/// The filter of the build-key producer at `JOIN` in `producer_fragment`
/// and its one consumer, built from the build-side equality witness.
fn join_filter(
    rf: Rf,
    distribution: JoinDistribution,
    producer_fragment: FragmentId,
    build_value: ValueId,
    progress: RuntimeFilterProducerProgress,
    consumer_fragment: FragmentId,
    consumer_value: ValueId,
    lineage: Box<[RuntimeFilterLineageStep]>,
) -> RuntimeFilter {
    let late_apply = LateApplyGranularity::Batch;
    let consumer = match rf.consumer {
        Consumer::ProbeKey => RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: producer_fragment,
                node: JOIN,
                values: Box::from([consumer_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            capabilities: Box::default(),
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::JoinProbeKey { equality: EQUALITY },
        },
        other => RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: consumer_fragment,
                node: SCAN,
                values: Box::from([consumer_value]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::default(),
            activation: match other {
                Consumer::LiveScan => {
                    RuntimeFilterConsumerActivation::NonBlockingLive { late_apply }
                }
                Consumer::StartUnfilteredScan => {
                    RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply }
                }
                _ => RuntimeFilterConsumerActivation::BlockingSnapshot,
            },
            target: RuntimeFilterConsumerTarget::ScanField {
                equality: EQUALITY,
                lineage,
            },
        },
    };
    let (domain, reduction, contributions, completion, capabilities, coverage) = if rf.ordered {
        let all_of = coverage(JoinDistribution::Partitioned);
        (
            RuntimeFilterDomain::Ordered {
                key: RuntimeFilterOrderKey {
                    ty: int64(),
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::Last,
                },
                inclusive: true,
                comparator:
                    novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
            },
            RuntimeFilterReduction::UnionOrderedHull,
            [
                RuntimeFilterContributionKind::FinalOrderedHullShard,
                RuntimeFilterContributionKind::ProducerClosed,
            ],
            RuntimeFilterCompletion::FencedCommittedDomain,
            [
                RuntimeFilterArtifactCapability::OrderedRange,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ],
            all_of,
        )
    } else {
        (
            RuntimeFilterDomain::Membership {
                ty: nullable_int64(rf.null_safe),
                null_semantics: if rf.null_safe {
                    RuntimeFilterNullSemantics::NullSafeEqual
                } else {
                    RuntimeFilterNullSemantics::NeverMatches
                },
            },
            RuntimeFilterReduction::SetUnion,
            [
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::ProducerClosed,
            ],
            RuntimeFilterCompletion::ProducerClosed,
            [
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ],
            coverage(distribution),
        )
    };
    let mut consumer = consumer;
    consumer.capabilities = Box::from(capabilities);
    RuntimeFilter {
        id: FILTER,
        kind: if rf.ordered {
            RuntimeFilterKind::MinMax
        } else {
            RuntimeFilterKind::InList
        },
        domain,
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction,
        availability_coverage: coverage.clone(),
        terminal_coverage: coverage,
        equality_witnesses: Box::from([RuntimeFilterEqualityWitness {
            id: EQUALITY,
            fragment: producer_fragment,
            join: JOIN,
            key_ordinal: 0,
            domain_side: JoinSide::Right,
        }]),
        producers: Box::from([RuntimeFilterProducer {
            witness: WITNESS,
            endpoint: RuntimeFilterEndpoint {
                fragment: producer_fragment,
                node: JOIN,
                values: Box::from([build_value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 1 },
            contribution_kinds: Box::from(contributions),
            completion,
            progress,
            target: RuntimeFilterProducerTarget::JoinBuildKey { equality: EQUALITY },
        }]),
        consumers: Box::from([consumer]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    }
}

/// Extract every fragment package of `plan` whose one scan reads `read`.
fn packages(
    plan: &PhysicalPlan,
    read: Option<FrozenConnectorRead>,
) -> BTreeMap<FragmentId, Arc<FragmentPackage>> {
    let scans = read
        .map(|read| BTreeMap::from([(ProviderReadOccurrenceId::new(0), read)]))
        .unwrap_or_default();
    packages_with_reads(plan, scans)
}

fn packages_with_reads(
    plan: &PhysicalPlan,
    scans: BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
) -> BTreeMap<FragmentId, Arc<FragmentPackage>> {
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for &id in plan.fragments().keys() {
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    extract_fragment_packages(
        plan,
        &scans,
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("runtime-filter packages extract: {error:?}"))
    .into_iter()
    .map(|(id, package)| (id, Arc::new(package)))
    .collect()
}

fn dynamic_filter() -> StaticScanDynamicFilter {
    StaticScanDynamicFilter::new(FILTER.get(), Arc::from("v0"))
}

/// `SELECT v0, v1 FROM t JOIN broadcast(VALUES (7), (8), (20)) ON v0 = c0`
/// in one fragment that streams to a gather. The join produces the filter
/// and, unless `rf` names another consumer, its probe scan consumes it.
pub(super) fn broadcast(rf: Rf) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan(&mut builder, Distribution::Unconstrained);
    let (build, build_column, constants) = build_fragment(BUILD_EDGE, rf.null_safe);
    let imported = receive(
        &mut builder,
        BUILD_RECEIVER,
        BUILD_EDGE,
        &[build_column],
        nullable_int64(rf.null_safe),
        |_| Distribution::Broadcast,
        RowMultiplicity::Replicated,
    )[0];
    add_join(
        &mut builder,
        (SCAN, &scan),
        (BUILD_RECEIVER, imported),
        JoinDistribution::BroadcastBuild,
        rf.null_safe,
    );
    builder.attach_runtime_filter(FILTER).unwrap();
    let probe = builder
        .finish_fixture_definition(
            JOIN,
            FragmentSink::Stream { edge: EDGE },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut gather = FragmentBuilder::new(GATHER);
    let received = receive(
        &mut gather,
        RECEIVER,
        EDGE,
        &scan,
        int64(),
        |_| Distribution::Singleton,
        RowMultiplicity::SingleCopy,
    );
    let gather = gather
        .finish_fixture_definition(
            RECEIVER,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(
        PlanVersionId::try_new([7; 16]).unwrap(),
        options(None).constants,
    );
    for (&id, pool) in constants.entries() {
        plan.insert_constant_pool(id, pool.clone()).unwrap();
    }
    plan.add_fragment(probe).unwrap();
    plan.add_fragment(gather).unwrap();
    plan.add_fragment(build).unwrap();
    plan.add_edge(edge(
        BUILD_EDGE,
        (BUILD, &[build_column]),
        (PROBE, BUILD_RECEIVER, &[imported]),
        Distribution::Broadcast,
        Distribution::Broadcast,
    ))
    .unwrap();
    plan.add_edge(edge(
        EDGE,
        (PROBE, &scan),
        (GATHER, RECEIVER, &received),
        Distribution::Singleton,
        Distribution::Singleton,
    ))
    .unwrap();
    plan.set_result_port(result_port(GATHER, RECEIVER, &received))
        .unwrap();
    plan.add_runtime_filter(join_filter(
        rf,
        JoinDistribution::BroadcastBuild,
        PROBE,
        imported,
        RuntimeFilterProducerProgress {
            build_edges: Box::from([BUILD_EDGE]),
            non_build_edges: Box::default(),
        },
        PROBE,
        scan[0],
        Box::default(),
    ))
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the broadcast runtime-filter plan validates: {error:?}"));
    let dynamic = if rf.scan_consumer() {
        vec![dynamic_filter()]
    } else {
        vec![]
    };
    packages(&plan, Some(frozen_read(false, dynamic)))
        .remove(&PROBE)
        .unwrap()
}

fn result_port(fragment: FragmentId, node: NodeId, output: &[ValueId]) -> ResultPort {
    ResultPort {
        scalar_schema: None,
        fragment,
        output: OutputPort {
            node,
            columns: Box::from(output),
        },
        fields: output
            .iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("c{ordinal}").into(),
                alias: None,
                value: *value,
                ty: int64(),
            })
            .collect(),
    }
}

/// `t` hash-partitioned on `v0` and `VALUES (7), (8), (20)` hash-partitioned
/// on `c0` into a partitioned join fragment that publishes the result. The
/// join fragment produces the filter; the scan fragment consumes it through
/// its exact exchange lineage.
fn partitioned() -> (Arc<FragmentPackage>, Arc<FragmentPackage>) {
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan(&mut builder, Distribution::Unconstrained);
    builder.attach_runtime_filter(FILTER).unwrap();
    let probe = builder
        .finish_fixture_definition(
            SCAN,
            FragmentSink::Stream { edge: PROBE_EDGE },
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let (build, build_column, constants) = build_fragment(BUILD_EDGE, false);

    let mut joiner = FragmentBuilder::new(JOINER);
    let probe_columns = receive(
        &mut joiner,
        PROBE_RECEIVER,
        PROBE_EDGE,
        &scan,
        int64(),
        |imports| hash(&imports[..1]),
        RowMultiplicity::SingleCopy,
    );
    let build_columns = receive(
        &mut joiner,
        BUILD_RECEIVER,
        BUILD_EDGE,
        &[build_column],
        int64(),
        |imports| hash(&imports[..1]),
        RowMultiplicity::SingleCopy,
    );
    add_join(
        &mut joiner,
        (PROBE_RECEIVER, &probe_columns),
        (BUILD_RECEIVER, build_columns[0]),
        JoinDistribution::Partitioned,
        false,
    );
    joiner.attach_runtime_filter(FILTER).unwrap();
    let joiner = joiner
        .finish_fixture_definition(
            JOIN,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();

    let mut plan = PlanBuilder::new(
        PlanVersionId::try_new([7; 16]).unwrap(),
        options(None).constants,
    );
    for (&id, pool) in constants.entries() {
        plan.insert_constant_pool(id, pool.clone()).unwrap();
    }
    plan.add_fragment(probe).unwrap();
    plan.add_fragment(build).unwrap();
    plan.add_fragment(joiner).unwrap();
    plan.add_edge(edge(
        PROBE_EDGE,
        (PROBE, &scan),
        (JOINER, PROBE_RECEIVER, &probe_columns),
        hash(&scan[..1]),
        hash(&probe_columns[..1]),
    ))
    .unwrap();
    plan.add_edge(edge(
        BUILD_EDGE,
        (BUILD, &[build_column]),
        (JOINER, BUILD_RECEIVER, &build_columns),
        hash(&[build_column]),
        hash(&build_columns),
    ))
    .unwrap();
    plan.set_result_port(result_port(JOINER, JOIN, &probe_columns))
        .unwrap();
    plan.add_runtime_filter(join_filter(
        Rf::blocking(),
        JoinDistribution::Partitioned,
        JOINER,
        build_columns[0],
        RuntimeFilterProducerProgress {
            build_edges: Box::from([BUILD_EDGE]),
            non_build_edges: Box::from([PROBE_EDGE]),
        },
        PROBE,
        scan[0],
        Box::from([RuntimeFilterLineageStep::ExchangeMapping {
            edge: PROBE_EDGE,
            mapping_ordinal: 0,
        }]),
    ))
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the partitioned runtime-filter plan validates: {error:?}"));
    let mut packages = packages(&plan, Some(frozen_read(false, vec![dynamic_filter()])));
    (
        packages.remove(&PROBE).unwrap(),
        packages.remove(&JOINER).unwrap(),
    )
}

fn compile_with(
    package: &Arc<FragmentPackage>,
    root_sink_dop: Option<usize>,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    let validated =
        validate_fragment_providers(Arc::clone(package), &providers(), &FixtureControl).unwrap();
    compile_fragment(validated, &functions(), options(root_sink_dop), control)
}

fn compile(
    package: &Arc<FragmentPackage>,
    root_sink_dop: Option<usize>,
) -> Result<LocalProgram, FragmentCompileError> {
    compile_with(package, root_sink_dop, &FixtureControl)
}

fn refused(
    package: &Arc<FragmentPackage>,
    root_sink_dop: Option<usize>,
    node: NodeId,
    expected: &str,
) {
    match compile(package, root_sink_dop) {
        Err(FragmentCompileError::Unsupported {
            node: Some(actual),
            feature,
        }) => {
            assert_eq!(feature, expected);
            assert_eq!(actual, node, "the refusal names its endpoint node");
        }
        Err(other) => panic!("expected refusal {expected:?}, got {other:?}"),
        Ok(_) => panic!("expected refusal {expected:?}, got a program"),
    }
}

fn node_of(program: &LocalProgram, pick: fn(&ProgramNodeKind) -> bool) -> ProgramNodeId {
    ProgramNodeId::new(
        program
            .graph()
            .nodes()
            .iter()
            .position(|node| pick(node.kind()))
            .expect("the program has the node"),
    )
}

fn filter_requirements(program: &LocalProgram) -> Vec<i32> {
    program
        .graph()
        .requirements()
        .entries()
        .iter()
        .filter_map(|requirement| match requirement {
            BindingRequirement::RuntimeFilter { binding_id } => Some(*binding_id),
            _ => None,
        })
        .collect()
}

fn membership(null_semantics: FilterNullSemantics) -> StaticFilterContract {
    StaticFilterContract::membership(
        &nullable_int64(matches!(null_semantics, FilterNullSemantics::NullSafeEqual)),
        null_semantics,
    )
    .unwrap()
}

/// The join's one membership producer: binding `binding`, channel FILTER,
/// over build key 0.
fn assert_producer(program: &LocalProgram, binding: u32, null_semantics: FilterNullSemantics) {
    let join = node_of(program, |kind| matches!(kind, ProgramNodeKind::Join { .. }));
    let ProgramNodeKind::Join {
        condition,
        runtime_filters,
        ..
    } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let [producer] = runtime_filters.as_slice() else {
        panic!("one producer: {runtime_filters:?}");
    };
    assert_eq!(
        producer.expr_id,
        condition.keys()[0].build,
        "it observes build key 0"
    );
    assert_eq!(producer.key_ordinal, 0);
    assert_eq!(producer.producer.binding_id(), binding);
    assert_eq!(producer.producer.channel_id(), FILTER.get());
    assert_eq!(producer.producer.kind(), FilterProducerKind::Membership);
    assert_eq!(producer.producer.reduction(), FilterReduction::SetUnion);
    assert_eq!(producer.producer.contract(), &membership(null_semantics));
    // The producer borrows the actual build-key root, with no extra scalar occurrence.
    let (unit, root) = crate::test_units::compiled_root(program, producer.expr_id);
    assert_eq!(producer.expr_id, condition.keys()[0].build);
    assert_eq!(
        unit.control_flow().uses()[&root.use_id].definition,
        root.definition
    );
    assert_eq!(unit.roots().len(), 2);
}

/// The scan's one blocking membership consumer: binding `binding`, channel
/// FILTER, keyed by its `RuntimeFilter { binding: 0 }` slot read of output 0.
fn assert_consumer(program: &LocalProgram, binding: u32, null_semantics: FilterNullSemantics) {
    let scan = node_of(program, |kind| matches!(kind, ProgramNodeKind::Scan { .. }));
    let node = &program.graph().nodes()[scan.index()];
    let ProgramNodeKind::Scan {
        source,
        runtime_filters,
        ..
    } = node.kind()
    else {
        unreachable!()
    };
    let [consumer] = runtime_filters.as_slice() else {
        panic!("one consumer: {runtime_filters:?}");
    };
    assert_eq!(consumer.consumer.binding_id(), binding);
    assert_eq!(consumer.consumer.channel_id(), FILTER.get());
    assert_eq!(
        consumer.consumer.activation(),
        FilterConsumerActivation::BlockingSnapshot
    );
    assert_eq!(consumer.consumer.reduction(), FilterReduction::SetUnion);
    assert_eq!(consumer.consumer.contract(), &membership(null_semantics));
    // The frozen dynamic filter stays with the compiled seal: it names the
    // consumer's channel over the column its key reads.
    let dynamic = source
        .compiled()
        .expect("compiled provider seal")
        .frozen()
        .scan()
        .dynamic_filters();
    assert_eq!(dynamic.len(), 1);
    assert_eq!(dynamic[0].filter_id(), consumer.consumer.channel_id());
    assert_eq!(dynamic[0].variable(), "v0");
    // The compiler-authored read retains its actual output position and complete type.
    let (unit, root) = crate::test_units::compiled_root(program, consumer.expr_id);
    assert!(matches!(
        unit.definitions().node(root.definition).unwrap().kind(),
        StaticExprKind::Input
    ));
    let invocation = &unit.control_flow().uses()[&root.use_id];
    assert_eq!(invocation.definition, root.definition);
    assert_eq!(invocation.context.demand, EvaluationDemand::Value);
    assert!(invocation.arguments.is_empty());
    let input = unit.input(root.use_id).unwrap();
    assert_eq!(input.ordinal, 0);
    assert_eq!(
        unit.scope(input.scope).unwrap().schema().as_ref(),
        node.output_layout().schema().as_ref()
    );
}

#[test]
fn broadcast_join_produces_the_filter_its_probe_scan_consumes_in_one_fragment() {
    let package = broadcast(Rf::blocking());
    // The package's own slice of the plan numbering: the producer, then the
    // consumer, under consecutive identities.
    let bindings = &package.cuts().runtime_filter_bindings;
    assert_eq!(
        bindings.as_ref(),
        [
            RuntimeFilterBindingCut {
                binding_id: 1,
                filter: FILTER,
                role: RuntimeFilterBindingRole::Producer(0),
            },
            RuntimeFilterBindingCut {
                binding_id: 2,
                filter: FILTER,
                role: RuntimeFilterBindingRole::Consumer(0),
            },
        ]
    );
    let program = compile(&package, None).unwrap();
    assert_producer(&program, 1, FilterNullSemantics::NeverMatches);
    assert_consumer(&program, 2, FilterNullSemantics::NeverMatches);
    // Exactly one requirement per site.
    let mut required = filter_requirements(&program);
    required.sort_unstable();
    assert_eq!(required, vec![1, 2]);
}

// A null-safe key equality fixes the NULL semantics of both sites' contract.
#[test]
fn a_null_safe_key_lowers_null_safe_membership_sites() {
    let program = compile(
        &broadcast(Rf {
            null_safe: true,
            ..Rf::blocking()
        }),
        None,
    )
    .unwrap();
    assert_producer(&program, 1, FilterNullSemantics::NullSafeEqual);
    assert_consumer(&program, 2, FilterNullSemantics::NullSafeEqual);
}

#[test]
fn partitioned_producer_and_consumer_compile_in_their_own_fragments() {
    let (probe, joiner) = partitioned();
    // Fragments are numbered in id order: the scan fragment's consumer first.
    assert_eq!(
        probe.cuts().runtime_filter_bindings.as_ref(),
        [RuntimeFilterBindingCut {
            binding_id: 1,
            filter: FILTER,
            role: RuntimeFilterBindingRole::Consumer(0),
        }]
    );
    assert_eq!(
        joiner.cuts().runtime_filter_bindings.as_ref(),
        [RuntimeFilterBindingCut {
            binding_id: 2,
            filter: FILTER,
            role: RuntimeFilterBindingRole::Producer(0),
        }]
    );

    let consumer = compile(&probe, None).unwrap();
    assert_consumer(&consumer, 1, FilterNullSemantics::NeverMatches);
    assert_eq!(filter_requirements(&consumer), vec![1]);
    assert!(
        !consumer
            .graph()
            .nodes()
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::Join { .. })),
        "the consumer-only fragment has no producer site"
    );

    let producer = compile(&joiner, Some(1)).unwrap();
    assert_producer(&producer, 2, FilterNullSemantics::NeverMatches);
    assert_eq!(filter_requirements(&producer), vec![2]);
    assert!(
        producer.scan_inputs().is_empty(),
        "the producer-only fragment has no consumer site"
    );
}

// Admitting, keying and rooting runtime-filter sites is observed work: a
// refusal at any callback is the compile's one primary cause.
#[test]
fn every_runtime_filter_compile_callback_keeps_the_original_control_cause() {
    let package = broadcast(Rf::blocking());
    let baseline = Trace::new(None);
    compile_with(&package, None, &baseline).unwrap();
    let expected = baseline.events.lock().unwrap().clone();
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace::new(Some((at, cause.clone())));
            assert!(matches!(
                compile_with(&package, None, &control),
                Err(FragmentCompileError::Control(actual)) if actual == cause
            ));
            assert_eq!(*control.events.lock().unwrap(), expected[..=at]);
        }
    }
}

#[test]
fn runtime_filter_shapes_outside_m1_are_refused_by_name() {
    for (consumer, feature) in [
        (
            Consumer::LiveScan,
            "non-blocking live runtime-filter consumer activation",
        ),
        (
            Consumer::StartUnfilteredScan,
            "start-unfiltered runtime-filter consumer activation",
        ),
    ] {
        refused(
            &broadcast(Rf {
                consumer,
                ..Rf::blocking()
            }),
            None,
            SCAN,
            feature,
        );
    }
    // A checked blocking probe-key endpoint stays on its actual Join owner.
    // It reuses the key definition but has an independent invocation source.
    let program = compile(
        &broadcast(Rf {
            consumer: Consumer::ProbeKey,
            ..Rf::blocking()
        }),
        None,
    )
    .expect("the exact membership probe-key contract is executable");
    let join = node_of(&program, |kind| {
        matches!(kind, ProgramNodeKind::Join { .. })
    });
    let ProgramNodeKind::Join {
        condition,
        runtime_filter_consumers,
        ..
    } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let [consumer] = runtime_filter_consumers.as_slice() else {
        panic!("one join-owned consumer: {runtime_filter_consumers:?}");
    };
    assert_eq!(consumer.expr_id, condition.keys()[0].probe);
    assert_eq!(consumer.key_ordinal, 0);
    assert_eq!(consumer.consumer.binding_id(), 2);
    assert_eq!(consumer.consumer.channel_id(), FILTER.get());
    assert_eq!(
        consumer.consumer.activation(),
        FilterConsumerActivation::BlockingSnapshot
    );
    assert_eq!(consumer.consumer.reduction(), FilterReduction::SetUnion);
    assert_eq!(
        consumer.consumer.contract(),
        &membership(FilterNullSemantics::NeverMatches)
    );
    let mut required = filter_requirements(&program);
    required.sort_unstable();
    assert_eq!(required, vec![1, 2]);
    // The consumer borrows this exact probe root, never another node's use bank.
    let (unit, root) = crate::test_units::compiled_root(&program, consumer.expr_id);
    assert_eq!(consumer.expr_id, condition.keys()[0].probe);
    let invocation = &unit.control_flow().uses()[&root.use_id];
    assert_eq!(invocation.definition, root.definition);
    assert_eq!(invocation.context.demand, EvaluationDemand::Value);
    assert!(invocation.arguments.is_empty());
    assert_eq!(unit.input(root.use_id).unwrap().ordinal, 0);
    let ProgramNodeKind::Join { left_layout, .. } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    assert_eq!(
        unit.scope(root.scope).unwrap().schema().as_ref(),
        left_layout.schema().as_ref()
    );
    assert!(
        !program
            .graph()
            .nodes()
            .iter()
            .any(|node| matches!(node.kind(), ProgramNodeKind::RuntimeFilterConsumer { .. }))
    );
    // An ordered hull over the build key is refused at its producer, which
    // the numbering lists first.
    refused(
        &broadcast(Rf {
            ordered: true,
            consumer: Consumer::StartUnfilteredScan,
            ..Rf::blocking()
        }),
        None,
        JOIN,
        "ordered-domain runtime filter",
    );
    let program = compile(&aggregate_topn(), Some(1))
        .expect("Complete zero-call ordered aggregation is installed");
    let id = node_of(&program, |kind| {
        matches!(kind, ProgramNodeKind::Aggregate { .. })
    });
    let ProgramNodeKind::Aggregate {
        functions,
        topn_filters,
        ..
    } = program.graph().nodes()[id.index()].kind()
    else {
        unreachable!()
    };
    assert!(functions.is_empty());
    assert_eq!(topn_filters.len(), 1);
    assert_eq!(topn_filters[0].limit.get(), 5);
}

/// `SELECT v0 FROM t GROUP BY v0 ORDER BY v0 LIMIT 5` whose aggregate
/// produces an ordered bound its scan consumes.
fn ordered_filter(
    fragment: FragmentId,
    aggregate: NodeId,
    scan: NodeId,
    topn: NodeId,
    phase: TopNPhase,
    value: ValueId,
    ty: ValueType,
    direction: SortDirection,
    null_ordering: NullOrdering,
    limit: u64,
) -> RuntimeFilter {
    let bound = RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(WITNESS),
            RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    };
    RuntimeFilter {
        id: FILTER,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty,
                direction,
                null_ordering,
            },
            inclusive: true,
            comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
        },
        lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
        reduction: RuntimeFilterReduction::TightenOrderedBound,
        availability_coverage: bound.clone(),
        terminal_coverage: bound,
        equality_witnesses: Box::default(),
        producers: Box::from([RuntimeFilterProducer {
            witness: WITNESS,
            endpoint: RuntimeFilterEndpoint {
                fragment,
                node: aggregate,
                values: Box::from([value]),
            },
            apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
            contribution_kinds: Box::from([
                RuntimeFilterContributionKind::OrderedBoundUpdate,
                RuntimeFilterContributionKind::ProducerClosed,
            ]),
            completion: RuntimeFilterCompletion::ProducerClosed,
            progress: RuntimeFilterProducerProgress {
                build_edges: Box::default(),
                non_build_edges: Box::default(),
            },
            target: RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 0,
                topn,
                phase,
                order_key_ordinal: 0,
                limit,
                offset: 0,
                direction,
                null_ordering,
            },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment,
                node: scan,
                values: Box::from([value]),
            },
            apply_point: RuntimeFilterApplyPoint::ScanSource,
            capabilities: Box::from([RuntimeFilterArtifactCapability::OrderedRange]),
            activation: RuntimeFilterConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            target: RuntimeFilterConsumerTarget::AggregateTopNScanField {
                producer: WITNESS,
                lineage: Box::default(),
            },
        }]),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 100,
            max_retries: 1,
        },
    }
}

fn aggregate_topn() -> Arc<FragmentPackage> {
    aggregate_topn_typed(int64(), int64())
}

fn aggregate_topn_typed(ty: ValueType, domain: ValueType) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan_typed(
        &mut builder,
        Distribution::Singleton,
        std::slice::from_ref(&ty),
    );
    // The group key republishes the scan value itself.
    let grouped = scan[0];
    let group_key = builder
        .fixture_value_root(AGGREGATE, ty.clone(), ExprKind::Value(grouped))
        .unwrap();
    builder
        .add_row_consuming(
            AGGREGATE,
            Box::from([SCAN]),
            RequiredInputs::Singleton,
            Distribution::Singleton,
            Box::from([grouped]),
            NodeKind::Aggregate {
                group_by: Box::from([(group_key, grouped)]),
                calls: Box::default(),
                grouping: AggregateGrouping::Complete,
            },
        )
        .unwrap();
    let order_key = builder
        .fixture_value_root(TOPN, ty.clone(), ExprKind::Value(grouped))
        .unwrap();
    builder
        .add_top_n(
            TOPN,
            AGGREGATE,
            Box::from([SortExpr {
                expr: order_key,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            5,
            0,
            TopNPhase::Single,
        )
        .unwrap();
    builder.attach_runtime_filter(FILTER).unwrap();
    let fragment = builder
        .finish_fixture_definition(
            TOPN,
            FragmentSink::Result,
            dop(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut result = result_port(PROBE, TOPN, &[grouped]);
    result.fields[0].ty = ty.clone();
    let mut plan = PlanBuilder::new(
        PlanVersionId::try_new([7; 16]).unwrap(),
        options(None).constants,
    );
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(result).unwrap();
    plan.add_runtime_filter(ordered_filter(
        PROBE,
        AGGREGATE,
        SCAN,
        TOPN,
        TopNPhase::Single,
        scan[0],
        domain,
        SortDirection::Ascending,
        NullOrdering::Last,
        5,
    ))
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the aggregate TopN filter plan validates: {error:?}"));
    let read = frozen_read_declared_typed(
        true,
        &[connector_type_for_value_type(&ty).unwrap()],
        arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "v0",
            ty.data_type().clone(),
            ty.nullable(),
        )]),
        Box::from([ty]),
        vec![dynamic_filter()],
    );
    packages(&plan, Some(read)).remove(&PROBE).unwrap()
}

// ---------------------------------------------------------------------------
// A runtime filter beside a relational call the FE froze with a small use.

/// COUNT, as the real builtin catalogue installs it.
fn count_catalog() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("count", FunctionKind::Aggregate)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.aggregate/count/v1").unwrap(),
            kind: FunctionKind::Aggregate,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.aggregate/count/derived-v1")
                    .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.aggregate/count/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::AggregateWindowV1,
            },
            aggregate_state_format: Some(
                AggregateStateFormatIdentity::try_new("novarocks/count/state-v1").unwrap(),
            ),
        }])
        .unwrap()
}

/// `COUNT(*)` as the real resolver binds it.
struct CountStar {
    function_id: FunctionId,
    selected: FunctionBindingSelection,
    semantics: novarocks_functions::FunctionSemantics,
}

impl CountStar {
    fn bind(catalog: &PureEngineFunctionCatalog) -> Self {
        let bound = catalog
            .metadata()
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &[],
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
                &FixtureControl,
            )
            .unwrap();
        Self {
            function_id: bound.function_id,
            selected: bound.selected,
            semantics: bound.semantics,
        }
    }

    fn aggregate(&self) -> &AggregateBindingSelection {
        self.selected.aggregate.as_ref().unwrap()
    }

    fn output_type(&self, phase: AggregatePhase) -> ValueType {
        match (&self.selected.result_type, phase.produces_final_result()) {
            (FunctionResultType::Scalar(result), true) => result.clone(),
            (_, false) => self.aggregate().intermediate_type.clone(),
            (other, true) => panic!("COUNT returns a scalar, got {other:?}"),
        }
    }

    /// The call `id` in `phase` over `arguments`, published under `output`.
    fn call(
        &self,
        id: u32,
        phase: AggregatePhase,
        arguments: Box<[RootOrdinal]>,
        output: ValueId,
    ) -> AggregateCall {
        let mut function = BoundFunction::from_exact_signature(
            self.function_id.clone(),
            self.selected.overload.clone(),
            FunctionKind::Aggregate,
            self.selected.argument_types.clone(),
            self.output_type(AggregatePhase::Single),
        );
        function.legacy_metadata = Some(LegacyBindingMetadata {
            volatility: self.semantics.volatility,
            argument_evaluation: self.semantics.argument_evaluation,
            failure_behavior: self.semantics.failure_behavior,
            intrinsic_row_error: self.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        });
        AggregateCall {
            id: AggregateCallId::new(id),
            binding: AggregateBinding {
                state_interpretation: None,
                state_argument_contract: self.aggregate().state_argument_contract,
                function,
                phase,
                logical_argument_count: 0,
                intermediate_type: self.aggregate().intermediate_type.clone(),
                state_format: self.aggregate().state_format.clone(),
            },
            arguments,
            distinct: false,
            order_by: Box::default(),
            output,
        }
    }
}

/// Add a group-less one-call aggregate over `input`.
fn add_count(
    builder: &mut FragmentBuilder,
    count: &CountStar,
    (node, input): (NodeId, NodeId),
    (id, phase): (u32, AggregatePhase),
    state: Option<ValueId>,
    grouping: AggregateGrouping,
) -> ValueId {
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
    let arguments = state
        .map(|state| {
            let ty = builder.value(state).unwrap().ty.clone();
            Box::from([builder
                .fixture_value_root(node, ty, ExprKind::Value(state))
                .unwrap()])
        })
        .unwrap_or_default();
    let input_properties = builder.node_output_properties(input).unwrap().clone();
    let distribution = match input_properties.distribution {
        Distribution::Singleton => Distribution::Singleton,
        _ => Distribution::Unconstrained,
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([input_properties]),
            output_properties: properties(distribution, RowMultiplicity::SingleCopy),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: Box::from([count.call(id, phase, arguments, output)]),
                grouping,
            },
        })
        .unwrap();
    output
}

/// Finish a fragment with one original request per relational call.
fn finish_with_requests(
    builder: FragmentBuilder,
    root: NodeId,
    sink: FragmentSink,
    catalog: &PureEngineFunctionCatalog,
) -> Fragment {
    let fragment = builder.finish_fixture_structure(
        root,
        sink,
        dop(),
        novarocks_physical_plan::StagePlacementPolicy::Singleton,
    );
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    visit_relational_calls_observed::<FrozenCallError>(&fragment, &mut work, |site, _, _| {
        entries.push((
            site,
            PhysicalCallRequest {
                arguments: Box::default(),
                logical_argument_count: 0,
                expected_result_type: None,
                constant_policy: options(None).constants,
            },
        ));
        Ok(())
    })
    .unwrap();
    work.finish().unwrap();
    let fragment = fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap();
    let calls = freeze_counts(&fragment, catalog);
    fragment
        .with_calls_observed(calls, &FixtureControl)
        .unwrap()
}

/// Freeze actual COUNT relations independently of the scalar units' local use identities.
fn freeze_counts(
    fragment: &Fragment,
    catalog: &PureEngineFunctionCatalog,
) -> Vec<FrozenPhysicalCall> {
    let mut sites = Vec::new();
    for node in fragment.nodes().values() {
        let (calls, topn) = match &node.kind {
            NodeKind::Aggregate { calls, .. } => (calls, false),
            NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            } => (calls, true),
            _ => continue,
        };
        for ordinal in 0..calls.len() {
            let call = ordinal as u32;
            let site = if topn {
                PhysicalCallSite::TopNState {
                    node: node.id,
                    call,
                }
            } else {
                PhysicalCallSite::Aggregate {
                    node: node.id,
                    call,
                }
            };
            let context = RelationEffectContext::call(RelationCallId::new(sites.len() as u32));
            sites.push((node.id, call, context, site));
        }
    }
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut frozen = Vec::new();
    for (node, ordinal, context, site) in sites {
        let calls = match &fragment.nodes()[&node].kind {
            NodeKind::Aggregate { calls, .. }
            | NodeKind::TopN {
                reduction: TopNReduction::GroupedStates { calls, .. },
                ..
            } => calls,
            _ => unreachable!(),
        };
        let call = &calls[ordinal as usize];
        let binding = &call.binding;
        let selection = Arc::new(FunctionBindingSelection {
            overload: binding.function.overload.clone(),
            argument_types: binding.function.argument_types.clone(),
            result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
            aggregate: Some(AggregateBindingSelection {
                state_argument_contract: binding.state_argument_contract,
                intermediate_type: binding.intermediate_type.clone(),
                state_format: binding.state_format.clone(),
            }),
        });
        let phase = match binding.phase {
            AggregatePhase::Single => AggregateKernelPhase::Single,
            AggregatePhase::Partial { .. } => AggregateKernelPhase::Partial,
            AggregatePhase::Intermediate { .. } => AggregateKernelPhase::Intermediate,
            AggregatePhase::Final { .. } => AggregateKernelPhase::Final,
        };
        let state_type;
        let (argument_uses, options) = if phase.consumes_logical_arguments() {
            (
                CallArgumentUses::SelectedChannels(&[]),
                AggregatePreparationOptions {
                    state_interpretation: None,
                    phase,
                    distinct: call.distinct,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
            )
        } else {
            let unit = fragment.nodes()[&node]
                .expressions
                .as_ref()
                .expect("actual state-input unit");
            let state_root = &unit.roots()[call.arguments[0].get() as usize];
            let state_use = unit.root_use(call.arguments[0]).unwrap();
            state_type = unit.definition(state_root.definition).unwrap().ty.clone();
            (
                CallArgumentUses::AggregateMerge {
                    phase,
                    state_context: unit.control_flow().uses()[&state_use].context.into(),
                    state_input_type: &state_type,
                },
                AggregatePreparationOptions {
                    state_interpretation: None,
                    phase,
                    distinct: call.distinct,
                    order_keys: Arc::from([]),
                    state_input_type: Some(state_type.clone()),
                },
            )
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: context.into(),
                    argument_uses,
                    function_id: &binding.function.function_id,
                    kind: FunctionKind::Aggregate,
                    selected: selection.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &[],
                        logical_argument_count: 0,
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                Arc::clone(&selection),
                PureCallPreparation::Aggregate {
                    arguments: ScopedRelationalEffects::primitive(
                        context,
                        ExpressionEffects::PURE_VALUE,
                    ),
                    options,
                },
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("COUNT(*) prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            site,
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        });
    }
    frozen
}

/// `SELECT count(*) FROM t a JOIN broadcast(VALUES (7), (8), (20)) b ON
/// a.k = b.k`: the probe scan outputs only its key, a zero-width Project
/// sits above the join and a Partial COUNT(*) streams to a gathered Final.
/// The join produces the filter its probe scan consumes. Returns the probe
/// package and the COUNT catalogue that compiles it.
fn count_star_join() -> (Arc<FragmentPackage>, PureEngineFunctionCatalog) {
    let catalog = count_catalog();
    let count = CountStar::bind(&catalog);
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan_columns(&mut builder, Distribution::Unconstrained, 1);
    let (build, build_column, constants) = build_fragment(BUILD_EDGE, false);
    let imported = receive(
        &mut builder,
        BUILD_RECEIVER,
        BUILD_EDGE,
        &[build_column],
        int64(),
        |_| Distribution::Broadcast,
        RowMultiplicity::Replicated,
    )[0];
    add_join(
        &mut builder,
        (SCAN, &scan),
        (BUILD_RECEIVER, imported),
        JoinDistribution::BroadcastBuild,
        false,
    );
    builder
        .add_project(PROJECT, JOIN, Box::default(), Box::default())
        .unwrap();
    let partial = AggregatePhase::Partial {
        sequence: AggregateSequenceId::new(1),
    };
    let state = add_count(
        &mut builder,
        &count,
        (AGGREGATE, PROJECT),
        (1, partial),
        None,
        AggregateGrouping::Partial,
    );
    builder.attach_runtime_filter(FILTER).unwrap();
    let probe = finish_with_requests(
        builder,
        AGGREGATE,
        FragmentSink::Stream { edge: EDGE },
        &catalog,
    );

    let mut gather = FragmentBuilder::new(GATHER);
    let received = receive(
        &mut gather,
        RECEIVER,
        EDGE,
        &[state],
        count.output_type(partial),
        |_| Distribution::Singleton,
        RowMultiplicity::SingleCopy,
    );
    let final_phase = AggregatePhase::Final {
        sequence: AggregateSequenceId::new(1),
    };
    let total = add_count(
        &mut gather,
        &count,
        (FINAL, RECEIVER),
        (2, final_phase),
        Some(received[0]),
        AggregateGrouping::Complete,
    );
    let gather = finish_with_requests(gather, FINAL, FragmentSink::Result, &catalog);

    let mut plan = PlanBuilder::new(
        PlanVersionId::try_new([7; 16]).unwrap(),
        options(None).constants,
    );
    for (&id, pool) in constants.entries() {
        plan.insert_constant_pool(id, pool.clone()).unwrap();
    }
    plan.add_fragment(probe).unwrap();
    plan.add_fragment(gather).unwrap();
    plan.add_fragment(build).unwrap();
    plan.add_edge(edge(
        BUILD_EDGE,
        (BUILD, &[build_column]),
        (PROBE, BUILD_RECEIVER, &[imported]),
        Distribution::Broadcast,
        Distribution::Broadcast,
    ))
    .unwrap();
    plan.add_edge(edge(
        EDGE,
        (PROBE, &[state]),
        (GATHER, RECEIVER, &received),
        Distribution::Singleton,
        Distribution::Singleton,
    ))
    .unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: GATHER,
        output: OutputPort {
            node: FINAL,
            columns: Box::from([total]),
        },
        fields: Box::from([ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "count".into(),
            alias: None,
            value: total,
            ty: count.output_type(final_phase),
        }]),
    })
    .unwrap();
    plan.add_runtime_filter(join_filter(
        Rf::blocking(),
        JoinDistribution::BroadcastBuild,
        PROBE,
        imported,
        RuntimeFilterProducerProgress {
            build_edges: Box::from([BUILD_EDGE]),
            non_build_edges: Box::default(),
        },
        PROBE,
        scan[0],
        Box::default(),
    ))
    .unwrap();
    let plan = plan
        .finish_observed(&FixtureControl)
        .unwrap_or_else(|error| panic!("the COUNT(*) runtime-filter plan validates: {error:?}"));

    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for &id in plan.fragments().keys() {
        pruning.insert(
            id,
            FrozenFragmentPruning::try_new(id, vec![], &FixtureControl).unwrap(),
        );
        admissions.insert(id, package_admission());
    }
    let mut packages = extract_fragment_packages(
        &plan,
        &BTreeMap::from([(
            ProviderReadOccurrenceId::new(0),
            frozen_read_columns(false, 1, vec![dynamic_filter()]),
        )]),
        &BTreeMap::new(),
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("the COUNT(*) packages extract: {error:?}"));
    (Arc::new(packages.remove(&PROBE).unwrap()), catalog)
}

// Relation contexts and scalar root uses have independent owners and identity types.
#[test]
fn a_runtime_filter_key_root_never_reuses_a_relational_call_use() {
    let (package, catalog) = count_star_join();
    let relational = package
        .fragment()
        .calls()
        .entries()
        .iter()
        .find_map(|(site, call)| {
            matches!(site, PhysicalCallSite::Aggregate { .. }).then_some(call.context)
        })
        .expect("the probe fragment freezes its actual COUNT relation");
    assert_eq!(
        relational.identity.role,
        novarocks_type_contract::RelationEffectRole::Call
    );
    let validated =
        validate_fragment_providers(Arc::clone(&package), &providers(), &FixtureControl).unwrap();
    let program = compile_fragment(validated, &catalog, options(None), &FixtureControl)
        .unwrap_or_else(|error| panic!("the COUNT(*) probe fragment compiles: {error}"));
    assert_producer(&program, 1, FilterNullSemantics::NeverMatches);
    let scan = node_of(&program, |kind| {
        matches!(kind, ProgramNodeKind::Scan { .. })
    });
    let ProgramNodeKind::Scan {
        runtime_filters, ..
    } = program.graph().nodes()[scan.index()].kind()
    else {
        unreachable!()
    };
    let key = runtime_filters[0].expr_id;
    let (unit, root) = crate::test_units::compiled_root(&program, key);
    assert_eq!(
        unit.control_flow().uses()[&root.use_id].definition,
        root.definition
    );
    let aggregate = node_of(&program, |kind| {
        matches!(kind, ProgramNodeKind::Aggregate { .. })
    });
    let prepared = program
        .resolved_calls()
        .calls()
        .get(&novarocks_local_program::ProgramCallSite::Aggregate {
            node: aggregate,
            call: 0,
        })
        .expect("actual prepared relation");
    assert_eq!(prepared.call_contract().context(), relational.into());
    assert!(
        unit.calls().is_empty(),
        "the pure key unit contains no scalar invocation of COUNT"
    );
}

#[path = "grouped_topn_lowering_tests.rs"]
mod grouped_topn_lowering_tests;

pub(super) use grouped_topn_lowering_tests::ordered_scan_package;

#[path = "complete_topn_lowering_tests.rs"]
mod complete_topn_lowering_tests;
