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

use super::scan_lowering_tests::{
    FixtureControl, Trace, binding, functions, int64, options, package_admission, payload,
    providers, public_schema, root_uses,
};
use super::*;
use arrow_schema::DataType;
use novarocks_connector_contract::*;
use novarocks_functions::{
    AggregateBindingSelection, AggregateKernelPhase, AggregatePreparationOptions,
    AggregateStateFormatIdentity, CallArgumentUses, CallEffectInput, EngineFunctionCatalogBuilder,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_program::{
    BindingRequirement, FilterConsumerActivation, FilterNullSemantics, FilterProducerKind,
    FilterReduction, LocalProgram, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExpressionArena, ProgramExpressionRootSite, ProgramLexicalSource,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramUseRef, StaticExprKind,
    StaticFilterContract,
};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    CallProofScope, ControlShape, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEffects, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, SemanticParameters, ValueLogicalType,
};
use std::num::NonZeroU64;

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
    let columns = columns(width);
    let provider = columns
        .iter()
        .map(|column| {
            builder
                .add_value(
                    int64(),
                    ValueOrigin::ProviderField {
                        scan_node: SCAN,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_scan(
            SCAN,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(Relation::Data(DataRelation {
                    read: ProviderReadReference {
                        binding: binding(),
                        input_version: ExactInputVersion::try_new([9]).unwrap(),
                        relation: relation_payload(),
                    },
                    work_source: ConnectorReadWorkSource::RuntimeSplits,
                    selection_digest: [7; 32],
                    schema: columns
                        .iter()
                        .map(|column| RelationField {
                            column: column.clone(),
                            ty: int64(),
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
            .map(|name| StaticScanAssignment::new(Arc::from(*name), ConnectorValueType::BigInt))
            .collect(),
        TupleDomain::all(),
        TupleDomain::all(),
        None,
        dynamic_filters,
        NonZeroU64::new(100).unwrap(),
        NonZeroU64::new(4096).unwrap(),
        ConnectorReadWorkSource::RuntimeSplits,
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
    let schema = public_schema();
    let public = ConnectorReadPublicFacts::try_new(
        source,
        None,
        arrow_schema::Schema::new_with_metadata(
            schema.fields()[..width].to_vec(),
            schema.metadata().clone(),
        ),
        vec![ValueLogicalType::Physical; width],
    )
    .unwrap();
    FrozenConnectorRead::try_new(scan, public).unwrap()
}

/// `VALUES (7), (8), (20)` as the one-column build source of `edge`.
fn build_fragment(edge: EdgeId, nullable: bool) -> (Fragment, ValueId) {
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
            Box::from([build
                .add_expression(
                    VALUES,
                    nullable_int64(nullable),
                    ExprKind::Literal(LiteralValue::Int64(value)),
                )
                .unwrap()])
        })
        .into_iter()
        .collect();
    build.add_values(VALUES, rows, Box::from([column])).unwrap();
    let fragment = build
        .finish_definition(VALUES, FragmentSink::Stream { edge }, dop())
        .unwrap();
    (fragment, column)
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
        id,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
            .add_expression(JOIN, left_type, ExprKind::Value(probe.1[0]))
            .unwrap(),
        right: builder
            .add_expression(JOIN, right_type, ExprKind::Value(build.1))
            .unwrap(),
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
                keys: Box::from([key]),
                build_side: JoinSide::Right,
                distribution,
                residual: None,
                null_extended: Box::default(),
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
struct Rf {
    null_safe: bool,
    consumer: Consumer,
    /// An ordered-hull domain instead of a membership domain.
    ordered: bool,
}
impl Rf {
    const fn blocking() -> Self {
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
#[expect(
    clippy::too_many_arguments,
    reason = "each endpoint fact is independent"
)]
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
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let root_uses = root_uses(fragment);
        calls.insert(
            id,
            FrozenFragmentCalls::try_new(fragment, &root_uses, vec![], &FixtureControl).unwrap(),
        );
        uses.insert(id, root_uses);
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
        &uses,
        &calls,
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
fn broadcast(rf: Rf) -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan(&mut builder, Distribution::Unconstrained);
    let (build, build_column) = build_fragment(BUILD_EDGE, rf.null_safe);
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
        .finish_definition(JOIN, FragmentSink::Stream { edge: EDGE }, dop())
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
        .finish_definition(RECEIVER, FragmentSink::Result, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
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
        .finish()
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
        .finish_definition(SCAN, FragmentSink::Stream { edge: PROBE_EDGE }, dop())
        .unwrap();
    let (build, build_column) = build_fragment(BUILD_EDGE, false);

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
        .finish_definition(JOIN, FragmentSink::Result, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
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
        .finish()
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
    StaticFilterContract::membership(&DataType::Int64, null_semantics).unwrap()
}

/// The join's one membership producer: binding `binding`, channel FILTER,
/// over build key 0.
fn assert_producer(program: &LocalProgram, binding: u32, null_semantics: FilterNullSemantics) {
    let join = node_of(program, |kind| matches!(kind, ProgramNodeKind::Join { .. }));
    let ProgramNodeKind::Join {
        build_keys,
        runtime_filters,
        ..
    } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let [producer] = runtime_filters.as_slice() else {
        panic!("one producer: {runtime_filters:?}");
    };
    assert_eq!(producer.expr_id, build_keys[0], "it observes build key 0");
    assert_eq!(producer.key_ordinal, 0);
    assert_eq!(producer.producer.binding_id(), binding);
    assert_eq!(producer.producer.channel_id(), FILTER.get());
    assert_eq!(producer.producer.kind(), FilterProducerKind::Membership);
    assert_eq!(producer.producer.reduction(), FilterReduction::SetUnion);
    assert_eq!(producer.producer.contract(), &membership(null_semantics));
    // The key's existing JoinBuildKey root is the producer key; no other
    // root names the join's runtime filter.
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let build_root = snapshot.roots().sites()[&ProgramExpressionRootSite::Node {
        node: join,
        role: ProgramNodeExpressionRole::JoinBuildKey { key: 0 },
    }]
        .definition;
    assert_eq!(build_root, producer.expr_id);
    assert!(!snapshot.bindings().keys().any(|site| matches!(
        site,
        ProgramExpressionRootSite::Node {
            node,
            role: ProgramNodeExpressionRole::RuntimeFilter { .. },
        } if *node == join
    )));
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
    // The key is a compiler-authored slot read of the scan's output 0 ...
    let checked = program.checked();
    let snapshot = checked.channels().expressions().resolved_calls().snapshot();
    let arena = &snapshot.roots().arenas()[&ProgramExpressionArena::Main];
    let key = arena.node(consumer.expr_id).unwrap();
    assert!(matches!(
        key.kind(),
        StaticExprKind::SlotId(slot) if *slot == node.output_layout().slots()[0]
    ));
    // ... rooted at the scan's RuntimeFilter { binding: 0 } as one Value use
    // whose lexical source is that exact output occurrence.
    let site = ProgramExpressionRootSite::Node {
        node: scan,
        role: ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
    };
    let use_id = snapshot.bindings()[&site];
    let root = &snapshot.flows()[&ProgramExpressionArena::Main].uses()[&use_id];
    assert_eq!(root.definition, consumer.expr_id);
    assert_eq!(root.context.demand, EvaluationDemand::Value);
    assert!(root.arguments.is_empty());
    assert_eq!(
        checked.slots()[&ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id,
        }],
        ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: scan,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        })
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
            let control = Trace::new(Some((at, cause)));
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
        probe_keys,
        runtime_filter_consumers,
        ..
    } = program.graph().nodes()[join.index()].kind()
    else {
        unreachable!()
    };
    let [consumer] = runtime_filter_consumers.as_slice() else {
        panic!("one join-owned consumer: {runtime_filter_consumers:?}");
    };
    assert_eq!(consumer.expr_id, probe_keys[0]);
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
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let consumer_site = ProgramExpressionRootSite::Node {
        node: join,
        role: ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
    };
    let probe_site = ProgramExpressionRootSite::Node {
        node: join,
        role: ProgramNodeExpressionRole::JoinProbeKey { key: 0 },
    };
    let use_id = snapshot.bindings()[&consumer_site];
    assert_ne!(use_id, snapshot.bindings()[&probe_site]);
    let invocation = &snapshot.flows()[&ProgramExpressionArena::Main].uses()[&use_id];
    assert_eq!(invocation.definition, consumer.expr_id);
    assert_eq!(invocation.context.demand, EvaluationDemand::Value);
    assert!(invocation.arguments.is_empty());
    assert_eq!(
        program.checked().slots()[&ProgramUseRef {
            arena: ProgramExpressionArena::Main,
            use_id,
        }],
        ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: join,
            role: ProgramChannelLayoutRole::JoinLeft,
            ordinal: 0,
        })
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
    refused(
        &aggregate_topn(),
        Some(1),
        AGGREGATE,
        "Aggregate TopN runtime-filter producer",
    );
}

/// `SELECT v0 FROM t GROUP BY v0 ORDER BY v0 LIMIT 5` whose aggregate
/// produces an ordered bound its scan consumes.
fn aggregate_topn() -> Arc<FragmentPackage> {
    let mut builder = FragmentBuilder::new(PROBE);
    let scan = add_scan(&mut builder, Distribution::Singleton);
    // The group key republishes the scan value itself.
    let grouped = scan[0];
    let group_key = builder
        .add_expression(AGGREGATE, int64(), ExprKind::Value(grouped))
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
        .add_expression(TOPN, int64(), ExprKind::Value(grouped))
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
        .finish_definition(TOPN, FragmentSink::Result, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(result_port(PROBE, TOPN, &[grouped]))
        .unwrap();
    let bound = RuntimeFilterCoverage {
        nodes: Box::from([
            RuntimeFilterCoverageNode::Witness(WITNESS),
            RuntimeFilterCoverageNode::AllOf {
                children: Box::from([0]),
            },
        ]),
        root: 1,
    };
    plan.add_runtime_filter(RuntimeFilter {
        id: FILTER,
        kind: RuntimeFilterKind::MinMax,
        domain: RuntimeFilterDomain::Ordered {
            key: RuntimeFilterOrderKey {
                ty: int64(),
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
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
                fragment: PROBE,
                node: AGGREGATE,
                values: Box::from([scan[0]]),
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
                topn: TOPN,
                phase: TopNPhase::Single,
                order_key_ordinal: 0,
                limit: 5,
                offset: 0,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            },
        }]),
        consumers: Box::from([RuntimeFilterConsumer {
            endpoint: RuntimeFilterEndpoint {
                fragment: PROBE,
                node: SCAN,
                values: Box::from([scan[0]]),
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
    })
    .unwrap();
    let plan = plan
        .finish()
        .unwrap_or_else(|error| panic!("the aggregate TopN filter plan validates: {error:?}"));
    packages(&plan, Some(frozen_read(true, vec![dynamic_filter()])))
        .remove(&PROBE)
        .unwrap()
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
        arguments: Box<[ExprId]>,
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
                .add_expression(node, ty, ExprKind::Value(state))
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
fn finish_with_requests(builder: FragmentBuilder, root: NodeId, sink: FragmentSink) -> Fragment {
    let fragment = builder.finish_definition(root, sink, dop()).unwrap();
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    visit_relational_calls_observed::<FrozenCallError>(&fragment, &mut work, |site, _, _| {
        entries.push((
            PhysicalCallDefinition::Relational(site),
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
    fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap()
}

/// Root uses and frozen calls of one fragment. Every expression root is a
/// leaf with one eager use in domain 0; aggregate call `k` owns domain
/// `k + 1` and the relational use right after the expression uses, the
/// smallest identity the FE can give it.
fn freeze_counts(
    fragment: &Fragment,
    catalog: &PureEngineFunctionCatalog,
) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain: EvaluationDomainId::new(0),
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
    let base = u32::try_from(uses.len()).unwrap();
    let mut domains = vec![ExpressionEvaluationDomain {
        id: EvaluationDomainId::new(0),
        parent: None,
        guard: None,
    }];
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
            let k = u32::try_from(sites.len()).unwrap();
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(base + k),
                domain: EvaluationDomainId::new(k + 1),
                demand: EvaluationDemand::Value,
            };
            domains.push(ExpressionEvaluationDomain {
                id: context.domain,
                parent: None,
                guard: None,
            });
            let call = u32::try_from(ordinal).unwrap();
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
            sites.push((node.id, call, context, site));
        }
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap();
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
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
            )
        } else {
            let state_use = root_uses.bindings()[&ExpressionRootSite {
                node,
                role: match site {
                    PhysicalCallSite::TopNState { .. } => ExpressionRootRole::TopNStateArgument {
                        call: ordinal,
                        argument: 0,
                    },
                    _ => ExpressionRootRole::AggregateArgument {
                        call: ordinal,
                        argument: 0,
                    },
                },
            }];
            state_type = fragment
                .expressions()
                .get(call.arguments[0])
                .unwrap()
                .ty
                .clone();
            (
                CallArgumentUses::AggregateMerge {
                    phase,
                    state_context: root_uses.flow().uses()[&state_use].context,
                    state_input_type: &state_type,
                },
                AggregatePreparationOptions {
                    state_interpretation: None,
                    phase,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: Some(state_type.clone()),
                },
            )
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context,
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
                    proof_scope: CallProofScope::Domain(context.domain),
                },
                Arc::clone(&selection),
                PureCallPreparation::Aggregate {
                    arguments: ScopedExpressionEffects::primitive(
                        context,
                        ExpressionEffects::PURE_VALUE,
                    ),
                    options,
                },
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("COUNT(*) prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site,
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        });
    }
    let calls = FrozenFragmentCalls::try_new(fragment, &root_uses, frozen, &FixtureControl)
        .unwrap_or_else(|error| panic!("frozen COUNT(*) calls validate: {error}"));
    (root_uses, calls)
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
    let (build, build_column) = build_fragment(BUILD_EDGE, false);
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
    let probe = finish_with_requests(builder, AGGREGATE, FragmentSink::Stream { edge: EDGE });

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
    let gather = finish_with_requests(gather, FINAL, FragmentSink::Result);

    let mut plan = PlanBuilder::new(PlanVersionId::try_new([7; 16]).unwrap());
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

    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (&id, fragment) in plan.fragments() {
        let (root_uses, frozen) = freeze_counts(fragment, &catalog);
        calls.insert(id, frozen);
        uses.insert(id, root_uses);
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
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("the COUNT(*) packages extract: {error:?}"));
    (Arc::new(packages.remove(&PROBE).unwrap()), catalog)
}

// The FE numbers a relational call's context right after the expression
// uses, which is the first identity a fresh occurrence could take. The scan's
// runtime-filter key root never takes it, so the relational COUNT(*) call and
// the derived key root each keep their own occurrence.
#[test]
fn a_runtime_filter_key_root_never_reuses_a_relational_call_use() {
    let (package, catalog) = count_star_join();
    let relational = package
        .calls()
        .entries()
        .iter()
        .find_map(|(site, call)| {
            matches!(site, PhysicalCallSite::Aggregate { .. }).then_some(call.context.use_id)
        })
        .expect("the probe fragment freezes its COUNT(*)");
    let flow_uses = package.expression_uses().flow().uses().len();
    assert_eq!(
        relational.get() as usize,
        flow_uses,
        "the relational use is the first identity after the flow's uses"
    );
    let validated =
        validate_fragment_providers(Arc::clone(&package), &providers(), &FixtureControl).unwrap();
    let program = compile_fragment(validated, &catalog, options(None), &FixtureControl)
        .unwrap_or_else(|error| panic!("the COUNT(*) probe fragment compiles: {error}"));
    assert_producer(&program, 1, FilterNullSemantics::NeverMatches);
    let scan = node_of(&program, |kind| {
        matches!(kind, ProgramNodeKind::Scan { .. })
    });
    let snapshot = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .snapshot();
    let key = snapshot.bindings()[&ProgramExpressionRootSite::Node {
        node: scan,
        role: ProgramNodeExpressionRole::RuntimeFilter { binding: 0 },
    }];
    assert_ne!(key, relational, "the key root has its own occurrence");
    assert!(
        !snapshot.flows()[&ProgramExpressionArena::Main]
            .uses()
            .contains_key(&relational),
        "no Main occurrence takes the relational call's identity"
    );
}

#[path = "grouped_topn_lowering_tests.rs"]
mod grouped_topn_lowering_tests;
