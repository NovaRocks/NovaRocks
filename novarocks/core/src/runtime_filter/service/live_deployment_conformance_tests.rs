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

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::connector::iceberg::scan_model::{
    IcebergColumnStats, IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef,
    IcebergSchemaFieldDef, IcebergTableInfo,
};
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::{FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope};
use crate::coordinator::execution::{CoordinatedQueryResult, ExecutionCoordinator};
use crate::coordinator::ports::{
    CoordinatorExecutionPorts, CoordinatorObserver, RuntimeFilterDeploymentControlPort,
};
use crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider;
use crate::coordinator::scheduler::FragmentScheduler;
use crate::coordinator::write::handle_fragment_report_exec_status;
use crate::coordinator::write::report::FragmentExecStatusReport;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::runtime_filter::{
    NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
};
use crate::exec::operators::runtime_filter::NativeRuntimeFilterConsumerSet;
use crate::exec::operators::runtime_filter::tests_support::chunk;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::profile::{
    ProfileNode, RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS, RuntimeProfileTree,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::deployment::participant_id_for_backend;
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
    ContributionKind, CoverageWitnessId, LateApplyGranularity, NullOrder, NullSemantics,
    OrderContract, OrderKeyContract, PlanFragmentId, PlanNodeId, ReductionRequirement,
    RuntimeFilterLifecycle, RuntimeFilterLogicalDomain, RuntimeFilterPolicyRequirement,
    SortDirection,
};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{
    ApplyPoint, ConsumerBindingTarget, ConsumerRequirementData, PlanLocation, ProducerRequirement,
    RuntimeFilterBindingRoleData, RuntimeFilterBindingSpecData, RuntimeFilterChannelSpec,
};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    ResidentMembershipIndexView,
};
use crate::runtime_filter::port::events::RuntimeFilterEventSink;
use crate::runtime_filter::port::events::{
    RuntimeFilterEvent, TransportEventKind, TransportFailOpenReason,
};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, RouteEdgeId,
    RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    ConsumerDeployment, MaterializationPolicy, ProducerDeployment, RuntimeFilterChannelDeployment,
    RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    local_participant_install_for_test,
};
use crate::runtime_filter::port::producer::{ProducerPortKind, SubmitOutcome};
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRoutePeer, RuntimeFilterRouteRole,
    RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
};
use crate::runtime_filter::port::subscription::{LivePollOutcome, LiveTerminal, SubscriptionKind};
use crate::runtime_filter::port::support::{
    MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::transport::{
    RuntimeFilterAcceptStatus, RuntimeFilterEnvelopeKind,
};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};
use crate::service::grpc_fragment_dispatcher::{
    GrpcRuntimeFilterDeploymentControl, RemoteDispatcher,
};
use crate::service::grpc_server::IndependentGrpcRuntimeFilterNode;
use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::options::{SessionOptimizerSettings, with_session_optimizer_settings};
use crate::sql::planner::distributed::test_support::{
    DistributedPlanDraftBuilder, draft_builder_from_plan,
};
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFragmentSink, IcebergWriteInputBinding,
};
use crate::sql::planner::distributed::{
    ActivationConstraint, ActivationFallback, DraftRuntimeFilterGraph, RequiredLiveReason,
};
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentStreamKind, PlanFragment,
};
use crate::sql::planner::payload::{PlanLimitNode, PlanScanNode, PlanSortNode};
use crate::sql::planner::physical::runtime_filter::JoinExecutionMode;
use crate::sql::planner::physical::{
    AggMode, AggregateOutputLayout, JoinDistribution, PhysicalHashAggregateNode,
    PhysicalHashJoinNode, PhysicalPlanKind, PhysicalPlanNode, PhysicalPlanStats, PhysicalTopNNode,
    PlannerConfidence, RedistributeMode, RedistributeNode, TopNPhase,
};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

use super::RuntimeFilterService;

const CHANNEL: ChannelId = ChannelId::new(80);
const PRODUCER_BINDING: BindingId = BindingId::new(81);
const CONSUMER_BINDING: BindingId = BindingId::new(82);
const WITNESS: CoverageWitnessId = CoverageWitnessId::new(83);
const PRODUCER_FRAGMENT: u32 = 0;
const PRODUCER_NODE: i32 = 810;
const CONSUMER_FRAGMENT: u32 = 1;
const CONSUMER_NODE: i32 = 811;
const MAX_WAIT: Duration = Duration::from_secs(5);
static LIVE_CONFORMANCE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug)]
enum ConformanceTopology {
    AnyOfDirect,
    AllOfAggregate,
}

impl ConformanceTopology {
    fn coverage(self) -> Coverage {
        match self {
            Self::AnyOfDirect => Coverage::AnyOf(vec![Coverage::Leaf(WITNESS)]),
            Self::AllOfAggregate => Coverage::AllOf(vec![Coverage::Leaf(WITNESS)]),
        }
    }
}

#[derive(Default)]
struct RecordingFragmentDispatcher {
    submit_count: AtomicUsize,
    submissions: Mutex<Vec<(usize, u32, UniqueId)>>,
}

impl RecordingFragmentDispatcher {
    fn submit_count(&self) -> usize {
        self.submit_count.load(Ordering::SeqCst)
    }

    fn submissions(&self) -> Vec<(usize, u32, UniqueId)> {
        self.submissions.lock().unwrap().clone()
    }
}

impl FragmentDispatcher for RecordingFragmentDispatcher {
    fn submit_fragment(
        &self,
        backend_idx: usize,
        submission: NativeFragmentEnvelope,
    ) -> Result<(), String> {
        let query_id = submission.query_id()?;
        let finst_id = submission.fragment_instance_id()?;
        let backend_num = submission.instance_params_for_test().backend_num;
        self.submissions
            .lock()
            .unwrap()
            .push((backend_idx, submission.fragment_id(), finst_id));
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        if submission.fragment_id() == PRODUCER_FRAGMENT {
            handle_fragment_report_exec_status(FragmentExecStatusReport {
                query_id,
                fragment_instance_id: finst_id,
                backend_num,
                done: true,
                status: crate::proto::common::Status {
                    code: 0,
                    message: String::new(),
                },
                iceberg_commits: vec![crate::proto::novarocks::IcebergCommitInfo {
                    iceberg_data_file: Some(crate::proto::novarocks::IcebergDataFile {
                        path: Some(format!("s3://live-conformance/be-{backend_idx}.parquet")),
                        record_count: Some(1),
                        file_size_in_bytes: Some(1),
                        file_content: crate::proto::novarocks::IcebergFileContent::Data as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                load_counters: BTreeMap::new(),
                loaded_rows: 1,
                loaded_bytes: 1,
                filtered_rows: 0,
            })
            .map_err(|error| format!("recording dispatcher writer completion failed: {error}"))?;
        }
        Ok(())
    }

    fn fetch_result(
        &self,
        _backend_idx: usize,
        _finst_id: UniqueId,
        _max_wait_ms: i64,
        _expected_chunk_schema: Option<&crate::exec::chunk::ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String> {
        Ok(FetchOutcome::Eof)
    }

    fn cancel_fragments(&self, _backend_idx: usize, _finst_ids: &[UniqueId]) {}

    fn backend_count(&self) -> usize {
        3
    }
}

#[derive(Default)]
struct RecordingCoordinatorObserver(AtomicUsize);

impl RecordingCoordinatorObserver {
    fn scheduled_count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl CoordinatorObserver for RecordingCoordinatorObserver {
    fn fragment_scheduled(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct InstallObservation {
    query_id: UniqueId,
    participant: RuntimeFilterParticipantId,
    install: RuntimeFilterParticipantInstall,
}

struct AckGatedDeploymentControl {
    inner: Arc<GrpcRuntimeFilterDeploymentControl>,
    installed: mpsc::SyncSender<InstallObservation>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait::async_trait]
impl RuntimeFilterDeploymentControlPort for AckGatedDeploymentControl {
    async fn install(
        &self,
        query_id: UniqueId,
        lifecycle: crate::protocol::native::RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String> {
        let observation = InstallObservation {
            query_id,
            participant,
            install: install.clone(),
        };
        self.inner
            .install(query_id, lifecycle, deadline, participant, install)
            .await?;
        self.installed
            .send(observation)
            .map_err(|_| "live install ACK observation receiver closed".to_string())?;
        tokio::time::timeout(MAX_WAIT, self.release.acquire())
            .await
            .map_err(|_| "live install ACK gate timed out".to_string())?
            .map_err(|_| "live install ACK gate closed".to_string())?
            .forget();
        Ok(())
    }

    async fn abort(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
    ) -> Result<(), String> {
        self.inner
            .abort(query_id, epoch, deadline, participant)
            .await
    }
}

fn stats() -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: 0.0,
        row_count_confidence: PlannerConfidence::Fallback,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn output_column() -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(1),
        name: "k".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    }
}

fn expression() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(1)),
        data_type: DataType::Int32,
        nullable: false,
    }
}

fn runtime_filter_graph(topology: ConformanceTopology) -> DraftRuntimeFilterGraph {
    let coverage = topology.coverage();
    let contributions = BTreeSet::from([
        ContributionKind::ValueDomainDelta,
        ContributionKind::ProducerClosed,
    ]);
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let mut graph = DraftRuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::Membership {
                value_type: DataType::Int32,
                null_semantics: NullSemantics::NeverMatches,
            },
            lifecycle: RuntimeFilterLifecycle::CompleteOnce,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::SetUnion,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 4_000,
                max_retries: 2,
            },
        })
        .expect("insert conformance channel");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: PRODUCER_BINDING,
            channel_id: CHANNEL,
            coverage_witness_id: Some(WITNESS),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(PRODUCER_FRAGMENT),
                node_id: PlanNodeId::new(PRODUCER_NODE),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                contribution_kinds: contributions,
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: crate::runtime_filter::model::graph::ProducerBindingTarget::JoinBuildKey {
                    ordinal: 0,
                },
            }),
        })
        .expect("insert conformance producer binding");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: CONSUMER_BINDING,
            channel_id: CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(CONSUMER_FRAGMENT),
                node_id: PlanNodeId::new(CONSUMER_NODE),
            },
            expression: expression(),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                capabilities,
                activation: ActivationConstraint::BlockingOrBatchLive {
                    fallback: ActivationFallback::BlockingSnapshot,
                },
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .expect("insert conformance consumer binding");
    graph
}

fn source_column() -> ColumnDef {
    ColumnDef {
        name: "k".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        write_default: None,
        logical_type: None,
    }
}

fn iceberg_table() -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "live_conformance".to_string(),
        namespace: "default".to_string(),
        table: "source".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-00000000006a".to_string()),
        current_snapshot_id: Some(1),
        schema_id: 1,
        location: "s3://live-conformance/source".to_string(),
        schema: IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "k".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            }],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

fn sealed_plan(topology: ConformanceTopology) -> crate::sql::planner::distributed::DistributedPlan {
    let column = output_column();
    let table = iceberg_table();
    let source = TableDef {
        name: "source".to_string(),
        columns: vec![source_column()],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::IcebergDataFiles {
            table,
            files: (0..3)
                .map(|index| {
                    IcebergDataFileInfo::for_test(
                        &format!("s3://live-conformance/source/file-{index}.parquet"),
                        1,
                        1,
                    )
                })
                .collect(),
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::ExplicitFiles,
        },
    };
    let exchange = |node_id| DistributedNode {
        node_id,
        fragment_id: PRODUCER_FRAGMENT,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Exchange(ExchangeReceiver {
            partition: DataPartition::unpartitioned(),
            source_fragment_id: CONSUMER_FRAGMENT,
            output_columns: vec![column.clone()],
            output_qualifier: None,
            flavor: ExchangeFlavor::CteMulticast {
                cte_id: CONSUMER_FRAGMENT,
                receive_producer_column_ids: vec![column.column_id],
            },
        }),
    };
    let producer_scan = DistributedNode {
        node_id: 814,
        fragment_id: PRODUCER_FRAGMENT,
        tuple_ids: vec![814],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: stats(),
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table: source.clone(),
            alias: None,
            columns: vec![column.clone()],
            predicates: Vec::new(),
            required_columns: Some(vec!["k".to_string()]),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    };
    // The consumer multicast feeds both the probe subtree (812) and build
    // frontier (813). Blocking on the producer therefore closes the real
    // consumer -> build-ready -> consumer SCC, while this scan preserves the
    // three-way producer placement exercised by the deployment conformance.
    let probe_join = DistributedNode {
        node_id: 815,
        fragment_id: PRODUCER_FRAGMENT,
        tuple_ids: vec![815],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: vec![exchange(812), producer_scan],
        stats: stats(),
        payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: Vec::new(),
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
            execution_mode: Some(JoinExecutionMode::Partitioned),
            build_runtime_filters: Vec::new(),
            output_columns: vec![column.clone()],
        })),
    };
    let producer_fragment = PlanFragment {
        fragment_id: PRODUCER_FRAGMENT,
        root: DistributedNode {
            node_id: PRODUCER_NODE,
            fragment_id: PRODUCER_FRAGMENT,
            tuple_ids: vec![PRODUCER_NODE],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: vec![PRODUCER_BINDING],
            children: vec![probe_join, exchange(813)],
            stats: stats(),
            payload: DistributedNodeKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: Vec::new(),
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
                execution_mode: Some(JoinExecutionMode::Partitioned),
                build_runtime_filters: Vec::new(),
                output_columns: vec![column.clone()],
            })),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::IcebergWrite(IcebergWriteFragmentSink {
            descriptor_database: "default".to_string(),
            spec: crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec(),
            input: IcebergWriteInputBinding::RootOutputByOrdinal,
        }),
        output_exprs: None,
        output_columns: vec![column.clone()],
        cte_id: None,
        cte_exchange_nodes: vec![
            (CONSUMER_FRAGMENT, 812, vec![ColumnId::new_for_test(1)]),
            (CONSUMER_FRAGMENT, 813, vec![ColumnId::new_for_test(1)]),
        ],
    };
    let consumer_fragment = PlanFragment {
        fragment_id: CONSUMER_FRAGMENT,
        root: DistributedNode {
            node_id: CONSUMER_NODE,
            fragment_id: CONSUMER_FRAGMENT,
            tuple_ids: vec![CONSUMER_NODE],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: vec![CONSUMER_BINDING],
            children: Vec::new(),
            stats: stats(),
            payload: DistributedNodeKind::Scan(PlanScanNode {
                database: "default".to_string(),
                table: source,
                alias: None,
                columns: vec![column.clone()],
                predicates: Vec::new(),
                required_columns: Some(vec!["k".to_string()]),
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Noop,
        output_exprs: None,
        output_columns: vec![column.clone()],
        cte_id: Some(CONSUMER_FRAGMENT),
        cte_exchange_nodes: Vec::new(),
    };
    let edge = |target_exchange_node_id| FragmentEdge {
        source_fragment_id: CONSUMER_FRAGMENT,
        target_fragment_id: PRODUCER_FRAGMENT,
        target_exchange_node_id,
        output_partition: DataPartition::unpartitioned(),
        stream_kind: FragmentStreamKind::Gather,
        edge_kind: FragmentEdgeKind::CteMulticast {
            cte_id: CONSUMER_FRAGMENT,
            receive_producer_column_ids: vec![ColumnId::new_for_test(1)],
        },
        output_slot_ids: vec![1],
    };
    let plan = DistributedPlanDraftBuilder::new(
        vec![producer_fragment, consumer_fragment],
        Some(PRODUCER_FRAGMENT),
        vec![edge(812), edge(813)],
        runtime_filter_graph(topology),
    )
    .seal()
    .expect("conformance fixture must pass the production distributed-plan seal");
    let RuntimeFilterBindingRoleData::Consumer(requirement) = &plan
        .runtime_filter_graph()
        .binding(CONSUMER_BINDING)
        .expect("conformance consumer binding")
        .role
    else {
        panic!("conformance consumer binding remains a consumer");
    };
    assert_eq!(
        requirement.activation,
        ConsumerActivation::NonBlockingLive {
            late_apply: LateApplyGranularity::Batch,
        },
        "the real feedback cycle forces the ordinary membership consumer live"
    );
    plan
}

fn wait_for_transport_ack(
    query_id: UniqueId,
    sender: &RuntimeFilterService,
    sender_participant: RuntimeFilterParticipantId,
    route_edge_ids: &BTreeSet<crate::runtime_filter::port::identity::RouteEdgeId>,
) {
    let deadline = std::time::Instant::now() + MAX_WAIT;
    let query = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
    loop {
        let accepted = RuntimeFilterLifecycleRegistry::global()
            .snapshot(query)
            .is_some_and(|snapshot| {
                snapshot.channel_events.values().flatten().any(|event| {
                    matches!(
                        event,
                        RuntimeFilterEvent::TransportEnvelope {
                            identity,
                            kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                            ..
                        } if identity.common().participant_id() == sender_participant
                            && identity.common().channel_id() == CHANNEL
                            && route_edge_ids.contains(&identity.route_edge_id())
                    )
                })
            });
        if sender.transport_pending_len_for_test() == 0 && accepted {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sender transport did not reach pending=0 plus Acked(Accepted) within {MAX_WAIT:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn producer_route_edges(
    install: &RuntimeFilterParticipantInstall,
) -> BTreeSet<crate::runtime_filter::port::identity::RouteEdgeId> {
    install.routing_shard().channels()[&CHANNEL]
        .outbound_edges()
        .iter()
        .filter(|edge| {
            edge.source().role() == RuntimeFilterRouteRole::Producer(PRODUCER_BINDING)
                && edge
                    .allowed_kinds()
                    .contains(&RuntimeFilterEnvelopeKind::Contribution)
        })
        .map(|edge| edge.route_edge_id())
        .collect()
}

fn assert_remote_producer_envelopes(
    service: &RuntimeFilterService,
    install: &RuntimeFilterParticipantInstall,
    fragment_instance_id: UniqueId,
) {
    let expected_edges = producer_route_edges(install);
    assert_eq!(
        expected_edges.len(),
        1,
        "remote producer has one installed route"
    );
    let observed = service
        .admitted_transport_envelopes_for_test()
        .into_iter()
        .filter_map(|(route, envelope)| {
            let identity = envelope.route_identity().as_contribution()?;
            (identity.producer_binding_id() == PRODUCER_BINDING
                && identity.fragment_instance_id() == fragment_instance_id)
                .then_some((
                    envelope.kind(),
                    route.route_edge_id(),
                    identity.partition_id(),
                    identity.sequence(),
                ))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            (
                RuntimeFilterEnvelopeKind::Contribution,
                *expected_edges.first().unwrap(),
                PartitionId::new(0),
                ProducerSequence::new(0),
            ),
            (
                RuntimeFilterEnvelopeKind::ProducerClosed,
                *expected_edges.first().unwrap(),
                PartitionId::new(0),
                ProducerSequence::new(1),
            ),
        ],
        "Contribution(seq=0) and ProducerClosed(terminal_seq=1) use the same installed route"
    );
}

fn membership_i32_values(bundle: &ArtifactBundle) -> BTreeSet<i32> {
    let artifact = bundle
        .artifacts()
        .iter()
        .find_map(|(_, artifact)| artifact.membership_index().map(|index| (artifact, index)))
        .expect("published membership artifact carries a resident index");
    match artifact.1.view() {
        ResidentMembershipIndexView::Fixed {
            values,
            count,
            width,
            ..
        } => {
            assert_eq!(width, std::mem::size_of::<i32>());
            assert_eq!(count, 3);
            artifact.0.canonical_bytes()[values.clone()]
                .chunks_exact(width)
                .map(|bytes| i32::from_be_bytes(bytes.try_into().expect("i32 bytes")))
                .collect()
        }
        other => panic!("expected fixed-width membership index, got {other:?}"),
    }
}

fn assert_zero_fragment_submits(dispatcher: &RecordingFragmentDispatcher) {
    assert_eq!(
        dispatcher.submit_count(),
        0,
        "the coordinator submitted a fragment before the install barrier ACKed"
    );
}

fn drain_final_reports_before_node_shutdown(query_id: QueryId) {
    assert!(
        crate::service::standalone_exec_state_reporter::wait_for_final_reports_for_query_for_test(
            query_id, MAX_WAIT,
        ),
        "final profile reports drain before temporary live BE endpoints shut down"
    );
}

fn query_id_from_live_nodes(nodes: &[IndependentGrpcRuntimeFilterNode]) -> QueryId {
    let query_ids = nodes
        .first()
        .expect("live deployment owns at least one BE")
        .manager()
        .query_ids_for_test();
    assert_eq!(
        query_ids.len(),
        1,
        "temporary live deployment BEs retain exactly one query before shutdown"
    );
    query_ids[0]
}

fn run_live_conformance(topology: ConformanceTopology) {
    let _serial = LIVE_CONFORMANCE_LOCK.lock().unwrap();
    let mut nodes = [
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE zero"),
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE one"),
        IndependentGrpcRuntimeFilterNode::start().expect("start independent BE two"),
    ];
    let endpoints = [
        nodes[0].endpoint(),
        nodes[1].endpoint(),
        nodes[2].endpoint(),
    ];
    let backends = LiveBackendSnapshot::new(endpoints.into_iter().enumerate().collect());
    let sealed = sealed_plan(topology);
    let mut connectors = crate::connector::ConnectorRegistry::new();
    connectors.register_scan_planner(Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    let prepared = crate::coordinator::prepare::prepare_fragments(&sealed, &connectors, None)
        .expect("prepare the sealed live conformance plan");
    let expected_prepared =
        crate::coordinator::prepare::prepare_fragments(&sealed, &connectors, None)
            .expect("prepare the expected live scheduling projection");
    let native_bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&sealed, &prepared)
            .expect("encode the sealed live conformance plan");
    let scheduler = Arc::new(FragmentScheduler::from_live_backend_snapshot(
        backends.clone(),
    ));
    let dispatcher = Arc::new(RecordingFragmentDispatcher::default());
    let observer = Arc::new(RecordingCoordinatorObserver::default());
    let install_ack_observations = Arc::new(AtomicUsize::new(0));
    for node in &nodes {
        let dispatcher = Arc::clone(&dispatcher);
        let observations = Arc::clone(&install_ack_observations);
        node.manager()
            .set_before_runtime_filter_installed_publish_hook_for_test(Arc::new(move || {
                assert_zero_fragment_submits(dispatcher.as_ref());
                observations.fetch_add(1, Ordering::SeqCst);
            }));
    }

    let grpc_control = Arc::new(
        GrpcRuntimeFilterDeploymentControl::new(backends.entries())
            .expect("construct production gRPC deployment control"),
    );
    let (installed_tx, installed_rx) = mpsc::sync_channel(3);
    let ack_release = Arc::new(tokio::sync::Semaphore::new(0));
    let control = Arc::new(AckGatedDeploymentControl {
        inner: grpc_control,
        installed: installed_tx,
        release: Arc::clone(&ack_release),
    });
    let mut execution_ports = CoordinatorExecutionPorts::new(
        dispatcher.clone(),
        RuntimeEndpoint::from_socket_addr(endpoints[0]),
        observer.clone(),
        control,
    );
    execution_ports.runtime_filter_policy_provider =
        Arc::new(NativeRuntimeFilterDeploymentPolicyProvider::new(2));
    let coordinator = ExecutionCoordinator::new(
        prepared,
        native_bundle,
        execution_ports,
        Arc::clone(&scheduler),
        None,
    );
    let (coordinator_done_tx, coordinator_done_rx) = mpsc::sync_channel(1);
    let coordinator_thread = std::thread::spawn(move || {
        let _ = coordinator_done_tx.send(coordinator.execute());
    });

    let mut query_id = None;
    let mut expected_installs = BTreeMap::new();
    for _ in 0..3 {
        let observation = installed_rx
            .recv_timeout(MAX_WAIT)
            .expect("real coordinator install RPC completes before the ACK gate");
        match query_id {
            Some(expected) => assert_eq!(observation.query_id, expected),
            None => query_id = Some(observation.query_id),
        }
        assert_eq!(
            observation.install.local_participant_id(),
            observation.participant
        );
        assert!(
            expected_installs
                .insert(observation.participant, observation.install)
                .is_none(),
            "coordinator installs every participant exactly once"
        );
    }
    let query_id = query_id.expect("coordinator generated query id");
    let lifecycle_query = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
    assert_eq!(
        expected_installs.len(),
        3,
        "all three live BEs participate in the compiled install"
    );
    assert_eq!(
        expected_installs
            .values()
            .map(RuntimeFilterParticipantInstall::epoch)
            .collect::<BTreeSet<_>>()
            .len(),
        1,
        "all participants install the same deployment epoch"
    );
    assert_zero_fragment_submits(dispatcher.as_ref());
    assert_eq!(observer.scheduled_count(), 0);
    assert_eq!(
        install_ack_observations.load(Ordering::SeqCst),
        3,
        "every install handler observed the zero-submit pre-ACK invariant"
    );
    ack_release.add_permits(3);
    coordinator_done_rx
        .recv_timeout(MAX_WAIT)
        .expect("production coordinator terminates within the bound")
        .expect("production coordinator crosses install, assembly, submit, and write completion");
    coordinator_thread.join().expect("coordinator thread joins");

    let scheduling = scheduler
        .schedule(expected_prepared.scheduling_view(), query_id)
        .expect("replay the production scheduler projection for exact comparison");
    let mut actual_submissions = dispatcher.submissions();
    actual_submissions.sort_unstable();
    let mut expected_submissions = scheduling
        .by_fragment
        .iter()
        .flat_map(|(fragment_id, placements)| {
            placements
                .iter()
                .map(|placement| (placement.backend_idx, *fragment_id, placement.finst_id))
        })
        .collect::<Vec<_>>();
    expected_submissions.sort_unstable();
    assert_eq!(
        actual_submissions, expected_submissions,
        "ExecutionCoordinator dispatches every production-scheduled placement exactly once"
    );
    assert_eq!(
        actual_submissions.len(),
        6,
        "both producer and consumer fragments submit on every live BE"
    );
    assert_eq!(observer.scheduled_count(), 6);
    assert_eq!(
        actual_submissions
            .iter()
            .map(|(backend_idx, _, _)| *backend_idx)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2]),
        "the three scheduled scan placements cover all live BEs"
    );

    let query = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let mut services = Vec::new();
    for (backend_idx, node) in nodes.iter().enumerate() {
        assert_eq!(
            node.manager().fragment_counts_for_test(query),
            Some((0, 0)),
            "install must not register fragments on backend {backend_idx}"
        );
        let service = node
            .manager()
            .runtime_filter_service_for_ingress(query)
            .expect("install ACK exposes the query-scoped service");
        let participant = participant_id_for_backend(backend_idx).expect("valid backend id");
        assert_eq!(
            service
                .installed_participant_install_for_test()
                .expect("service has an active installation"),
            expected_installs[&participant],
            "backend {backend_idx} installed only its compiler-authorized Core/routing roles"
        );
        services.push(service);
    }

    let producer_finsts = scheduling.by_fragment[&PRODUCER_FRAGMENT]
        .iter()
        .map(|placement| (placement.backend_idx, placement.finst_id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(producer_finsts.len(), 3);
    let consumer_finsts = scheduling.by_fragment[&CONSUMER_FRAGMENT]
        .iter()
        .map(|placement| (placement.backend_idx, placement.finst_id))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(consumer_finsts.len(), 3);
    let aggregator_idx = (0..3).find(|backend_idx| {
        let participant = participant_id_for_backend(*backend_idx).unwrap();
        expected_installs[&participant].routing_shard().channels()[&CHANNEL]
            .local_roles()
            .contains(&RuntimeFilterRouteRole::Aggregator)
    });
    let aggregator_idx = match topology {
        ConformanceTopology::AllOfAggregate => {
            aggregator_idx.expect("exactly one participant owns the aggregator role")
        }
        ConformanceTopology::AnyOfDirect => {
            assert!(
                aggregator_idx.is_none(),
                "AnyOf direct has no aggregator role"
            );
            0
        }
    };
    let consumer_finst = consumer_finsts[&2];
    let mut producers = Vec::new();
    for backend_idx in 0..3 {
        let producer = services[backend_idx]
            .open_producer(
                PRODUCER_BINDING,
                producer_finsts[&backend_idx],
                1,
                ProducerPortKind::Membership,
            )
            .expect("producer role is authorized on every scheduled BE")
            .into_membership()
            .expect("membership producer port");
        producers.push(producer);
    }
    let subscription = services[2]
        .subscribe(
            CONSUMER_BINDING,
            consumer_finst,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("consumer role is authorized on BE two")
        .into_live()
        .expect("Batch Live consumer");

    if matches!(topology, ConformanceTopology::AllOfAggregate) {
        for remote_idx in (0..3).filter(|idx| *idx != aggregator_idx) {
            let remote_finst = producer_finsts[&remote_idx];
            let error = services[aggregator_idx]
                .open_producer(
                    PRODUCER_BINDING,
                    remote_finst,
                    1,
                    ProducerPortKind::Membership,
                )
                .expect_err("aggregator public API cannot open a remote-owned finst");
            assert_eq!(
                error.kind(),
                crate::runtime_filter::port::producer::RuntimeContractViolationKind::UnauthorizedFragmentInstance,
            );
            assert!(
                !services[aggregator_idx]
                    .core_producer_handle_exists_for_test(PRODUCER_BINDING, remote_finst,),
                "rejected public open must not mutate aggregator Core"
            );
        }
    }

    let submit_and_close =
        |producer: &Arc<dyn crate::runtime_filter::port::producer::ProducerAdapter>, value: i32| {
            let submit = producer
                .submit(
                    PartitionId::new(0),
                    ProducerSequence::new(0),
                    ValueDomainDelta::new(MembershipValues::int32([value]), false),
                )
                .expect("inject contribution through the installed producer Service");
            assert!(matches!(
                submit,
                SubmitOutcome::Applied
                    | SubmitOutcome::Published
                    | SubmitOutcome::StreamAcceptedNoGlobalChange
            ));
            let _close = producer
                .close_partition(PartitionId::new(0), ProducerSequence::new(1))
                .expect("close installed producer stream");
        };
    match topology {
        ConformanceTopology::AnyOfDirect => submit_and_close(&producers[0], 11),
        ConformanceTopology::AllOfAggregate => {
            let mut remote_indices = (0..3)
                .filter(|idx| *idx != aggregator_idx)
                .collect::<Vec<_>>();
            remote_indices.sort_unstable();
            let first = remote_indices[0];
            submit_and_close(&producers[first], [11, 22, 33][first]);
            let participant = participant_id_for_backend(first).unwrap();
            wait_for_transport_ack(
                query_id,
                &services[first],
                participant,
                &producer_route_edges(&expected_installs[&participant]),
            );
            assert_remote_producer_envelopes(
                &services[first],
                &expected_installs[&participant],
                producer_finsts[&first],
            );
            assert!(
                subscription.snapshot().is_none(),
                "AllOf must not publish after one shard"
            );
            let second = remote_indices[1];
            submit_and_close(&producers[second], [11, 22, 33][second]);
            let participant = participant_id_for_backend(second).unwrap();
            wait_for_transport_ack(
                query_id,
                &services[second],
                participant,
                &producer_route_edges(&expected_installs[&participant]),
            );
            assert_remote_producer_envelopes(
                &services[second],
                &expected_installs[&participant],
                producer_finsts[&second],
            );
            assert!(
                subscription.snapshot().is_none(),
                "AllOf must not publish before the last shard"
            );
            submit_and_close(&producers[aggregator_idx], [11, 22, 33][aggregator_idx]);
        }
    }

    let deadline = Instant::now() + MAX_WAIT;
    let bundle = loop {
        match subscription.poll_after(None) {
            LivePollOutcome::Updated {
                bundle,
                terminal: Some(LiveTerminal::Completed),
            } => break bundle,
            LivePollOutcome::Idle { terminal: None, .. } if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1))
            }
            outcome => panic!(
                "remote Batch Live consumer did not receive one completed final artifact within {MAX_WAIT:?}: {outcome:?}"
            ),
        }
    };
    assert!(
        !bundle.artifacts().is_empty(),
        "the remotely delivered artifact bundle is nonempty"
    );
    if matches!(topology, ConformanceTopology::AllOfAggregate) {
        assert_eq!(membership_i32_values(&bundle), BTreeSet::from([11, 22, 33]));
    }
    let sender_participant = participant_id_for_backend(0).expect("valid sender backend id");
    let sender_routes = expected_installs[&sender_participant]
        .core_view()
        .channels()[&CHANNEL]
        .outbound_materialization_groups()
        .values()
        .flat_map(|group| group.route_edge_ids().iter().copied())
        .collect::<BTreeSet<_>>();
    assert!(!sender_routes.is_empty(), "sender owns a delivery route");
    wait_for_transport_ack(query_id, &services[0], sender_participant, &sender_routes);
    let rejected = RuntimeFilterLifecycleRegistry::global()
        .snapshot(lifecycle_query)
        .is_some_and(|snapshot| {
            snapshot.channel_events.values().flatten().any(|event| {
                matches!(
                    event,
                    RuntimeFilterEvent::TransportEnvelope {
                        identity,
                        kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Rejected),
                        ..
                    } if identity.common().participant_id() == sender_participant
                        && identity.common().channel_id() == CHANNEL
                        && sender_routes.contains(&identity.route_edge_id())
                )
            })
        });
    assert!(
        !rejected,
        "the sender route must not complete with Rejected"
    );
    drain_final_reports_before_node_shutdown(QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    });
    for node in &mut nodes {
        node.shutdown().expect("shutdown independent gRPC BE");
    }
    RuntimeFilterLifecycleRegistry::global().remove_query(lifecycle_query);
}

#[test]
fn live_three_be_anyof_direct_install_ack_and_delivery() {
    run_live_conformance(ConformanceTopology::AnyOfDirect);
}

#[test]
fn live_three_be_allof_aggregate_install_ack_and_delivery() {
    run_live_conformance(ConformanceTopology::AllOfAggregate);
}

#[test]
fn live_join_complete_once_operator_applies_real_service_final_artifact() {
    struct WallClock;
    impl RuntimeFilterClock for WallClock {
        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    struct NoopEvents;
    impl RuntimeFilterEventSink for NoopEvents {
        fn record(&self, _event: RuntimeFilterEvent) {}
    }

    struct UnlimitedMemory;
    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    let query_id = UniqueId { hi: 620, lo: 621 };
    let producer_finst = UniqueId { hi: 620, lo: 622 };
    let consumer_finst = UniqueId { hi: 620, lo: 623 };
    let epoch = DeploymentEpoch::new(624);
    let participant = RuntimeFilterParticipantId::new(625);
    let channel_id = ChannelId::new(626);
    let producer_binding = BindingId::new(627);
    let consumer_binding = BindingId::new(628);
    let route_edge = RouteEdgeId::new(629);
    let witness = CoverageWitnessId::new(630);
    let activation = ConsumerActivation::NonBlockingLive {
        late_apply: LateApplyGranularity::Batch,
    };
    let capabilities = BTreeSet::from([
        ArtifactCapability::Membership,
        ArtifactCapability::EmptyDomain,
    ]);
    let profile = ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap();
    let coverage = Coverage::Leaf(witness);
    let deployment = RuntimeFilterChannelDeployment::new(
        channel_id,
        RuntimeFilterLogicalDomain::Membership {
            value_type: DataType::Int32,
            null_semantics: NullSemantics::NeverMatches,
        },
        RuntimeFilterLifecycle::CompleteOnce,
        coverage.clone(),
        coverage,
        ReductionRequirement::SetUnion,
        BTreeSet::from([
            ContributionKind::ValueDomainDelta,
            ContributionKind::ProducerClosed,
        ]),
        CompletionRequirement::ProducerClosed,
        RuntimeFilterPolicyRequirement {
            max_contribution_bytes: 1024,
            max_artifact_bytes: 1024,
            deadline_ms: 1_000,
            max_retries: 2,
        },
        RuntimeFilterCoreBudget::new(8192),
        MaterializationPolicy::for_test(),
        BTreeMap::from([(
            producer_binding,
            ProducerDeployment::new(witness, BTreeSet::from([producer_finst])),
        )]),
        BTreeMap::from([(
            consumer_binding,
            ConsumerDeployment::with_profile(
                activation,
                capabilities.clone(),
                profile,
                BTreeSet::from([route_edge]),
                BTreeSet::from([consumer_finst]),
            ),
        )]),
    );
    let service = Arc::new(RuntimeFilterService::new_for_lifecycle_test(
        query_id,
        Arc::new(WallClock),
        Arc::new(NoopEvents),
        Arc::new(UnlimitedMemory),
    ));
    service
        .install(local_participant_install_for_test(
            RuntimeFilterInstallView::new(
                epoch,
                participant,
                BTreeMap::from([(channel_id, deployment)]),
            ),
        ))
        .expect("install the real CompleteOnce Batch Live deployment");

    let mut arena = ExprArena::default();
    let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
    let schema =
        ArtifactMembershipSchema::new(&DataType::Int32, NullSemantics::NeverMatches).unwrap();
    let spec = NativeRuntimeFilterConsumerSpec {
        binding_id: consumer_binding.get(),
        channel_id: channel_id.get(),
        expr_id,
        activation,
        capabilities,
        contract: NativeRuntimeFilterContract::Membership {
            canonical_schema: Arc::from(schema.canonical_bytes()),
            schema_digest: schema.digest().bytes(),
        },
        reduction: NativeRuntimeFilterReduction::SetUnion,
    };
    let consumers = NativeRuntimeFilterConsumerSet::from_plan(&[spec], Arc::new(arena))
        .expect("build the operator consumer from the plan");
    let state = RuntimeState::default().with_native_runtime_filter_context(Some(
        super::NativeRuntimeFilterExecutionContext::new(
            Arc::clone(&service),
            query_id,
            epoch,
            consumer_finst,
        ),
    ));
    consumers
        .bind(&state)
        .expect("bind through RuntimeState into the installed service");
    consumers
        .acquire_configured()
        .expect("an all-live consumer has no blocking acquire work");

    let before = consumers
        .apply_chunk(chunk(&[1, 2]))
        .expect("pending live apply")
        .expect("pending live keeps the whole batch");
    assert_eq!(before.len(), 2);

    let producer = service
        .open_producer(
            producer_binding,
            producer_finst,
            1,
            ProducerPortKind::Membership,
        )
        .expect("open the installed real producer")
        .into_membership()
        .expect("resolve the membership producer port");
    let submit = producer
        .submit(
            PartitionId::new(0),
            ProducerSequence::new(0),
            ValueDomainDelta::new(MembershipValues::int32([2]), false),
        )
        .expect("submit the unique membership contribution");
    assert!(matches!(
        submit,
        SubmitOutcome::Applied
            | SubmitOutcome::Published
            | SubmitOutcome::StreamAcceptedNoGlobalChange
    ));
    producer
        .close_partition(PartitionId::new(0), ProducerSequence::new(1))
        .expect("close the unique producer stream");

    let after = consumers
        .apply_chunk(chunk(&[1, 2]))
        .expect("completed live apply")
        .expect("the final membership artifact keeps one row");
    assert_eq!(
        after.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[2]
    );
    let again = consumers
        .apply_chunk(chunk(&[2, 3]))
        .expect("active predicate applies without a second live version")
        .expect("the active predicate keeps one row");
    assert_eq!(
        again.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[2]
    );

    let public_live = service
        .subscribe(
            consumer_binding,
            consumer_finst,
            SubscriptionKind::NonBlockingLive,
        )
        .expect("the service exposes the installed live subscription")
        .into_live()
        .expect("the public subscription is live");
    let LivePollOutcome::Updated { bundle, terminal } = public_live.poll_after(None) else {
        panic!("the service must expose its unique final bundle")
    };
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_eq!(terminal, Some(LiveTerminal::Completed));
    assert!(matches!(
        public_live.poll_after(Some(LogicalVersion::FIRST)),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed),
        }
    ));
}

#[test]
fn remote_membership_contribution_uses_installed_route() {
    run_live_conformance(ConformanceTopology::AllOfAggregate);
}

#[test]
fn remote_producer_closed_uses_same_route_and_terminal_sequence() {
    run_live_conformance(ConformanceTopology::AllOfAggregate);
}

const MANUAL_TOPN_CHANNEL: ChannelId = ChannelId::new(180);
const MANUAL_TOPN_PRODUCER_BINDING: BindingId = BindingId::new(181);
const MANUAL_TOPN_CONSUMER_BINDING: BindingId = BindingId::new(182);
const MANUAL_TOPN_WITNESS: CoverageWitnessId = CoverageWitnessId::new(183);
const TOPN_LIMIT: u32 = 2;
const TOPN_ROWS_PER_PUBLISH: usize = 4096;

#[derive(Clone)]
struct TopNInstallObservation {
    query_id: UniqueId,
    participant: RuntimeFilterParticipantId,
    install: RuntimeFilterParticipantInstall,
}

struct TopNRecordingDeploymentControl {
    inner: Arc<GrpcRuntimeFilterDeploymentControl>,
    observations: Arc<Mutex<Vec<TopNInstallObservation>>>,
    failed_remote_endpoint: Option<RuntimeEndpoint>,
}

#[async_trait::async_trait]
impl RuntimeFilterDeploymentControlPort for TopNRecordingDeploymentControl {
    async fn install(
        &self,
        query_id: UniqueId,
        lifecycle: crate::protocol::native::RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String> {
        let install = match self.failed_remote_endpoint.as_ref() {
            Some(endpoint) => redirect_remote_routes(install, endpoint)?,
            None => install,
        };
        self.inner
            .install(
                query_id,
                lifecycle,
                deadline.min(MAX_WAIT),
                participant,
                install.clone(),
            )
            .await?;
        self.observations
            .lock()
            .unwrap()
            .push(TopNInstallObservation {
                query_id,
                participant,
                install,
            });
        Ok(())
    }

    async fn abort(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
    ) -> Result<(), String> {
        self.inner
            .abort(query_id, epoch, deadline.min(MAX_WAIT), participant)
            .await
    }
}

fn redirect_remote_routes(
    install: RuntimeFilterParticipantInstall,
    failed_endpoint: &RuntimeEndpoint,
) -> Result<RuntimeFilterParticipantInstall, String> {
    fn redirect_edge(
        edge: &RuntimeFilterRoutingEdgeView,
        failed_endpoint: &RuntimeEndpoint,
    ) -> Result<RuntimeFilterRoutingEdgeView, String> {
        let peer = match edge.peer() {
            RuntimeFilterRoutePeer::Loopback => RuntimeFilterRoutePeer::Loopback,
            RuntimeFilterRoutePeer::Remote { participant_id, .. } => {
                RuntimeFilterRoutePeer::Remote {
                    participant_id: *participant_id,
                    endpoint: failed_endpoint.clone(),
                }
            }
        };
        RuntimeFilterRoutingEdgeView::new(
            edge.channel_id(),
            edge.route_edge_id(),
            edge.source().clone(),
            edge.target().clone(),
            peer,
            edge.allowed_kinds().clone(),
        )
        .map_err(|error| format!("redirect live TopN route: {error}"))
    }

    let (core, routing) = install.into_parts();
    let channels = routing
        .channels()
        .iter()
        .map(|(channel_id, channel)| {
            let inbound = channel
                .inbound_edges()
                .iter()
                .map(|edge| redirect_edge(edge, failed_endpoint))
                .collect::<Result<Vec<_>, _>>()?;
            let outbound = channel
                .outbound_edges()
                .iter()
                .map(|edge| redirect_edge(edge, failed_endpoint))
                .collect::<Result<Vec<_>, _>>()?;
            let channel = RuntimeFilterChannelRoutingView::new(
                *channel_id,
                channel.local_roles().clone(),
                channel.producer_instances().clone(),
                inbound,
                outbound,
            )
            .map_err(|error| format!("rebuild failed live TopN channel route: {error}"))?;
            Ok((*channel_id, channel))
        })
        .collect::<Result<BTreeMap<_, _>, String>>()?;
    let routing = RuntimeFilterRoutingShard::new(
        routing.deployment_epoch(),
        routing.local_participant_id(),
        channels,
    )
    .map_err(|error| format!("rebuild failed live TopN routing shard: {error}"))?;
    Ok(RuntimeFilterParticipantInstall::new(core, routing))
}

struct LocalTopNFiles {
    _dir: tempfile::TempDir,
    files: Vec<IcebergDataFileInfo>,
}

impl LocalTopNFiles {
    fn new(remote: bool) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-live-topn-")
            .tempdir()
            .expect("create live TopN tempdir");
        let leading = if remote {
            vec![
                vec![1, 2],
                vec![1, 2],
                vec![1, 2],
                vec![1, 2],
                vec![1, 2],
                vec![1, 2],
            ]
        } else {
            vec![vec![20, 30], vec![5]]
        };
        let mut files = leading
            .into_iter()
            .enumerate()
            .map(|(index, values)| {
                let repeated = values
                    .into_iter()
                    .cycle()
                    .take(TOPN_ROWS_PER_PUBLISH)
                    .collect::<Vec<_>>();
                write_topn_file(dir.path(), index, &repeated)
            })
            .collect::<Vec<_>>();
        let tail_count = if remote { 72 } else { 24 };
        let tail_start = files.len();
        files.extend((0..tail_count).map(|tail| {
            let value = 100 + i32::try_from(tail).unwrap();
            write_topn_file(
                dir.path(),
                tail_start + tail,
                &vec![value; TOPN_ROWS_PER_PUBLISH],
            )
        }));
        Self { _dir: dir, files }
    }
}

fn write_topn_file(dir: &std::path::Path, index: usize, values: &[i32]) -> IcebergDataFileInfo {
    let path = dir.join(format!("topn-{index:03}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("build live TopN parquet batch");
    crate::formats::parquet::local_io::write_parquet_to_path(&path, &batch)
        .expect("write live TopN parquet");
    let size = i64::try_from(std::fs::metadata(&path).unwrap().len()).unwrap();
    let mut file = IcebergDataFileInfo::for_test(
        &format!("file://{}", path.display()),
        size,
        i64::try_from(values.len()).unwrap(),
    );
    let min = *values.iter().min().expect("nonempty TopN file");
    let max = *values.iter().max().expect("nonempty TopN file");
    file.column_stats = Some(std::collections::HashMap::from([(
        "k".to_string(),
        IcebergColumnStats {
            null_count: Some(0),
            value_count: Some(i64::try_from(values.len()).unwrap()),
            column_size: None,
            lower_bound: Some(min.to_le_bytes().to_vec()),
            upper_bound: Some(max.to_le_bytes().to_vec()),
        },
    )]));
    file
}

fn topn_output_column(id: u32, name: &str) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    }
}

fn topn_column_expr(id: u32, qualifier: Option<&str>, name: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(id),
            qualifier: qualifier.map(str::to_string),
            column: name.to_string(),
        },
        data_type: DataType::Int32,
        nullable: false,
    }
}

fn topn_stats(rows: f64) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: rows,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn topn_table_info(location: &str) -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "live_topn".to_string(),
        namespace: "default".to_string(),
        table: "source".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-000000000180".to_string()),
        current_snapshot_id: Some(1),
        schema_id: 1,
        location: location.to_string(),
        schema: IcebergSchemaDef {
            fields: vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "k".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            }],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

fn topn_physical_plan(files: &[IcebergDataFileInfo], remote: bool) -> PhysicalPlanNode {
    let scan_column = topn_output_column(1, "k");
    let scan = PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "source".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: topn_table_info(
                        files
                            .first()
                            .map(|file| file.path.as_str())
                            .unwrap_or("file:///empty"),
                    ),
                    files: files.to_vec(),
                    cloud_properties: BTreeMap::new(),
                    binding: IcebergDataFileBinding::ExplicitFiles,
                },
            },
            alias: Some("source".to_string()),
            columns: vec![scan_column.clone()],
            predicates: Vec::new(),
            required_columns: Some(vec!["k".to_string()]),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        children: Vec::new(),
        output_columns: vec![scan_column],
        stats: topn_stats(files.len() as f64 * TOPN_ROWS_PER_PUBLISH as f64),
        probe_runtime_filters: Vec::new(),
    };
    if remote {
        return manual_remote_topn_physical_plan(scan, files.len());
    }
    let group_output = topn_output_column(10, "k");
    let aggregate = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Local,
            group_by: vec![topn_column_expr(1, Some("source"), "k")],
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(vec![group_output.clone()], Vec::new()),
            output_columns: vec![group_output.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
        children: vec![scan],
        output_columns: vec![group_output.clone()],
        stats: topn_stats(files.len() as f64),
        probe_runtime_filters: Vec::new(),
    };
    let topn = PhysicalPlanNode {
        kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![crate::sql::analysis::SortItem {
                expr: topn_column_expr(10, None, "k"),
                asc: true,
                nulls_first: false,
            }],
            limit: Some(i64::from(TOPN_LIMIT)),
            offset: Some(0),
            phase: TopNPhase::Partial,
            is_split: true,
        }),
        children: vec![aggregate],
        output_columns: vec![group_output.clone()],
        stats: topn_stats(TOPN_LIMIT as f64),
        probe_runtime_filters: Vec::new(),
    };
    topn
}

fn manual_remote_topn_physical_plan(scan: PhysicalPlanNode, file_count: usize) -> PhysicalPlanNode {
    let group_output = topn_output_column(10, "k");
    let aggregate = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: vec![topn_column_expr(1, Some("source"), "k")],
            aggregates: Vec::new(),
            is_merge: Vec::new(),
            output_layout: AggregateOutputLayout::new(vec![group_output.clone()], Vec::new()),
            output_columns: vec![group_output.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
        children: vec![scan],
        output_columns: vec![group_output.clone()],
        stats: topn_stats(file_count as f64),
        probe_runtime_filters: Vec::new(),
    };
    let gather = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: Vec::new(),
            output_columns: vec![group_output.clone()],
        }),
        children: vec![aggregate],
        output_columns: vec![group_output.clone()],
        stats: topn_stats(file_count as f64),
        probe_runtime_filters: Vec::new(),
    };
    let sort = PhysicalPlanNode {
        kind: PhysicalPlanKind::Sort(PlanSortNode {
            items: vec![SortItem {
                expr: topn_column_expr(10, None, "k"),
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_by: Vec::new(),
            output_columns: vec![group_output.clone()],
            offset: None,
            partition_limit: None,
            topn_type: None,
        }),
        children: vec![gather],
        output_columns: vec![group_output.clone()],
        stats: topn_stats(TOPN_LIMIT as f64),
        probe_runtime_filters: Vec::new(),
    };
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Limit(PlanLimitNode {
            limit: Some(i64::from(TOPN_LIMIT)),
            offset: None,
        }),
        children: vec![sort],
        output_columns: vec![group_output],
        stats: topn_stats(TOPN_LIMIT as f64),
        probe_runtime_filters: Vec::new(),
    }
}

fn find_manual_topn_binding_locations(fragments: &[PlanFragment]) -> ((u32, i32), (u32, i32)) {
    fn walk(
        node: &DistributedNode,
        producer: &mut Option<(u32, i32)>,
        consumer: &mut Option<(u32, i32)>,
    ) {
        match &node.payload {
            DistributedNodeKind::HashAggregate(aggregate) if !aggregate.group_by.is_empty() => {
                assert!(
                    producer.replace((node.fragment_id, node.node_id)).is_none(),
                    "manual remote TopN fixture has one producer aggregate"
                );
            }
            DistributedNodeKind::Scan(_) => {
                assert!(
                    consumer.replace((node.fragment_id, node.node_id)).is_none(),
                    "manual remote TopN fixture has one source scan"
                );
            }
            _ => {}
        }
        for child in &node.children {
            walk(child, producer, consumer);
        }
    }

    let mut producer = None;
    let mut consumer = None;
    for fragment in fragments {
        walk(&fragment.root, &mut producer, &mut consumer);
    }
    (
        producer.expect("manual remote TopN producer aggregate location"),
        consumer.expect("manual remote TopN consumer scan location"),
    )
}

fn attach_manual_topn_binding(
    node: &mut DistributedNode,
    location: (u32, i32),
    binding: BindingId,
) {
    if (node.fragment_id, node.node_id) == location {
        node.runtime_filter_binding_ids.push(binding);
        node.runtime_filter_binding_ids.sort_unstable();
        node.runtime_filter_binding_ids.dedup();
    }
    for child in &mut node.children {
        attach_manual_topn_binding(child, location, binding);
    }
}

fn manual_remote_topn_runtime_filter_graph(
    producer: (u32, i32),
    consumer: (u32, i32),
    deadline_ms: u64,
) -> DraftRuntimeFilterGraph {
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int32,
        direction: SortDirection::Ascending,
        null_order: NullOrder::Last,
    }];
    let comparator_digest = crate::runtime_filter::port::ordered_bound::comparator_digest_for_test(
        &keys,
        crate::runtime_filter::port::ordered_bound::COMPARATOR_ALGORITHM_VERSION,
    );
    let contributions = BTreeSet::from([
        ContributionKind::OrderedBoundUpdate,
        ContributionKind::ProducerClosed,
    ]);
    let capabilities = BTreeSet::from([ArtifactCapability::OrderedRange]);
    let coverage = Coverage::Leaf(MANUAL_TOPN_WITNESS);
    let mut graph = DraftRuntimeFilterGraph::default();
    graph
        .insert_channel(RuntimeFilterChannelSpec {
            channel_id: MANUAL_TOPN_CHANNEL,
            logical_domain: RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
                keys,
                inclusive: true,
                comparator_digest,
            }),
            lifecycle: RuntimeFilterLifecycle::MonotonicUpdates,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            reduction_requirement: ReductionRequirement::TightenOrderedBound,
            allowed_contribution_kinds: contributions.clone(),
            required_consumer_capabilities: capabilities.clone(),
            policy: RuntimeFilterPolicyRequirement {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms,
                max_retries: 2,
            },
        })
        .expect("insert manual remote TopN channel");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: MANUAL_TOPN_PRODUCER_BINDING,
            channel_id: MANUAL_TOPN_CHANNEL,
            coverage_witness_id: Some(MANUAL_TOPN_WITNESS),
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(producer.0),
                node_id: PlanNodeId::new(producer.1),
            },
            expression: topn_column_expr(1, Some("source"), "k"),
            apply_point: ApplyPoint::NodeOutput,
            role: RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                contribution_kinds: contributions,
                completion_requirement: CompletionRequirement::ProducerClosed,
                target:
                    crate::runtime_filter::model::graph::ProducerBindingTarget::AggregateTopNKey {
                        group_key_ordinal: 0,
                        limit: NonZeroU32::new(TOPN_LIMIT).unwrap(),
                    },
            }),
        })
        .expect("insert manual remote TopN producer");
    graph
        .insert_binding(RuntimeFilterBindingSpecData {
            binding_id: MANUAL_TOPN_CONSUMER_BINDING,
            channel_id: MANUAL_TOPN_CHANNEL,
            coverage_witness_id: None,
            location: PlanLocation {
                fragment_id: PlanFragmentId::new(consumer.0),
                node_id: PlanNodeId::new(consumer.1),
            },
            expression: topn_column_expr(1, Some("source"), "k"),
            apply_point: ApplyPoint::NodeInput,
            role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                capabilities,
                activation: ActivationConstraint::LiveOnly {
                    late_apply: LateApplyGranularity::Split,
                    reason: RequiredLiveReason::OrderedBoundContract,
                },
                target: ConsumerBindingTarget::SourceBoundary,
            }),
        })
        .expect("insert manual remote TopN consumer");
    graph
}

fn sealed_topn_plan(
    files: &[IcebergDataFileInfo],
    remote: bool,
    runtime_filter: bool,
    deadline_ms: u64,
) -> crate::sql::planner::distributed::DistributedPlan {
    let physical = topn_physical_plan(files, remote);
    if remote {
        let base = crate::sql::planner::distributed::build::build_distributed_plan(&physical)
            .expect("build manual remote TopN distributed plan");
        if !runtime_filter {
            return base;
        }
        let mut draft = draft_builder_from_plan(&base, Default::default());
        let (producer, consumer) = find_manual_topn_binding_locations(draft.fragments());
        draft.set_runtime_filter_graph(manual_remote_topn_runtime_filter_graph(
            producer,
            consumer,
            deadline_ms,
        ));
        for fragment in draft.fragments_mut() {
            attach_manual_topn_binding(&mut fragment.root, producer, MANUAL_TOPN_PRODUCER_BINDING);
            attach_manual_topn_binding(&mut fragment.root, consumer, MANUAL_TOPN_CONSUMER_BINDING);
        }
        return draft
            .seal()
            .expect("manual remote TopN fixture passes the production plan seal");
    }
    let plan = with_session_optimizer_settings(
        SessionOptimizerSettings {
            enable_global_runtime_filter: Some(runtime_filter),
            ..Default::default()
        },
        || crate::sql::planner::pipeline::build_distributed_plan(physical),
    )
    .expect("build planner-generated live TopN distributed plan");
    if runtime_filter {
        let graph = plan.runtime_filter_graph();
        assert_eq!(
            graph.channel_count(),
            1,
            "eligible live TopN fixture must generate one ordered runtime-filter channel"
        );
        assert_eq!(
            graph.bindings()
                .filter(|binding| matches!(
                    binding.role,
                    crate::runtime_filter::model::graph::RuntimeFilterBindingRoleData::Producer(
                        crate::runtime_filter::model::graph::ProducerRequirement {
                            target: crate::runtime_filter::model::graph::ProducerBindingTarget::AggregateTopNKey { .. },
                            ..
                        }
                    )
                ))
                .count(),
            1,
            "eligible live TopN fixture must generate one AggregateTopN producer"
        );
        assert!(
            graph.bindings().any(|binding| matches!(
                binding.role,
                crate::runtime_filter::model::graph::RuntimeFilterBindingRoleData::Consumer(_)
            )),
            "eligible live TopN fixture must generate a source consumer"
        );
        planner_generated_topn_ids(&plan)
            .expect("planner attaches discoverable live TopN channel and binding ids");
    } else {
        assert!(
            plan.runtime_filter_graph().is_empty(),
            "runtime-filter-off live TopN fixture must not generate a graph"
        );
    }
    plan
}

#[derive(Clone, Debug)]
struct PlannerGeneratedTopNIds {
    channel: ChannelId,
    consumers: Vec<BindingId>,
}

fn planner_generated_topn_ids(
    plan: &crate::sql::planner::distributed::DistributedPlan,
) -> Result<PlannerGeneratedTopNIds, String> {
    let graph = plan.runtime_filter_graph();
    if graph.channel_count() != 1 {
        return Err(format!(
            "planner-generated live TopN graph must contain exactly one channel, found {}",
            graph.channel_count()
        ));
    }
    let channel = graph
        .channels()
        .find(|channel| {
            matches!(
                channel.logical_domain,
                RuntimeFilterLogicalDomain::OrderedBound(_)
            )
        })
        .ok_or_else(|| "planner-generated live TopN graph has no ordered channel".to_string())?;
    let producers = graph
        .bindings()
        .filter(|binding| {
            binding.channel_id == channel.channel_id
                && matches!(
                    &binding.role,
                    RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                        target: crate::runtime_filter::model::graph::ProducerBindingTarget::AggregateTopNKey { .. },
                        ..
                    })
                )
        })
        .map(|binding| binding.binding_id)
        .collect::<Vec<_>>();
    if producers.len() != 1 {
        return Err(format!(
            "planner-generated live TopN graph must contain exactly one AggregateTopN producer, found {}",
            producers.len()
        ));
    }
    let consumers = graph
        .bindings()
        .filter(|binding| {
            binding.channel_id == channel.channel_id
                && matches!(&binding.role, RuntimeFilterBindingRoleData::Consumer(_))
        })
        .map(|binding| binding.binding_id)
        .collect::<Vec<_>>();
    if consumers.is_empty() {
        return Err("planner-generated live TopN graph has no source consumer".to_string());
    }
    Ok(PlannerGeneratedTopNIds {
        channel: channel.channel_id,
        consumers,
    })
}

struct TopNNodeEvidence {
    participant: RuntimeFilterParticipantId,
    pending_transport: usize,
    envelopes: Vec<TopNAdmittedEnvelopeEvidence>,
    transport_events: Vec<TopNTransportEventEvidence>,
    events: Vec<RuntimeFilterEvent>,
}

struct LiveTopNRun {
    outcome: CoordinatedQueryResult,
    planner_generated_ids: Option<PlannerGeneratedTopNIds>,
    observations: Vec<TopNInstallObservation>,
    node_evidence: Vec<TopNNodeEvidence>,
    hub_snapshot_empty: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TopNAdmittedEnvelopeEvidence {
    route_edge_id: RouteEdgeId,
    target: RuntimeFilterRouteRole,
    kind: RuntimeFilterEnvelopeKind,
    bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TopNTransportEventEvidence {
    route_edge_id: RouteEdgeId,
    kind: TransportEventKind,
    bytes: usize,
}

fn has_accepted_data_frame(
    admitted: &[TopNAdmittedEnvelopeEvidence],
    transport_events: &[TopNTransportEventEvidence],
    route_edge_id: RouteEdgeId,
    kind: RuntimeFilterEnvelopeKind,
) -> bool {
    admitted.iter().any(|envelope| {
        envelope.route_edge_id == route_edge_id
            && envelope.kind == kind
            && envelope.bytes > 0
            && transport_events.iter().any(|event| {
                event.route_edge_id == route_edge_id
                    && event.kind == TransportEventKind::Sent
                    && event.bytes == envelope.bytes
            })
            && transport_events.iter().any(|event| {
                event.route_edge_id == route_edge_id
                    && event.kind == TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted)
                    && event.bytes == envelope.bytes
            })
    })
}

fn has_deadline_failed_open(
    transport_events: &[TopNTransportEventEvidence],
    route_edge_ids: &BTreeSet<RouteEdgeId>,
) -> bool {
    transport_events.iter().any(|event| {
        route_edge_ids.contains(&event.route_edge_id)
            && event.kind == TransportEventKind::FailedOpen(TransportFailOpenReason::Deadline)
    })
}

fn topn_success_order_is_valid(
    availability_position: usize,
    terminal_positions: &[usize],
    completed_position: usize,
    expected_terminals: usize,
) -> bool {
    terminal_positions.len() == expected_terminals
        && terminal_positions
            .iter()
            .any(|position| *position > availability_position)
        && terminal_positions
            .iter()
            .all(|position| *position < completed_position)
}

#[test]
fn live_topn_remote_data_ack_requires_route_kind_and_nonzero_frame_bytes() {
    let route = RouteEdgeId::new(900);
    let admitted = vec![TopNAdmittedEnvelopeEvidence {
        route_edge_id: route,
        target: RuntimeFilterRouteRole::Aggregator,
        kind: RuntimeFilterEnvelopeKind::Contribution,
        bytes: 37,
    }];
    let terminal_only = vec![
        TopNTransportEventEvidence {
            route_edge_id: route,
            kind: TransportEventKind::Sent,
            bytes: 0,
        },
        TopNTransportEventEvidence {
            route_edge_id: route,
            kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
            bytes: 0,
        },
    ];
    assert!(
        !has_accepted_data_frame(
            &admitted,
            &terminal_only,
            route,
            RuntimeFilterEnvelopeKind::Contribution,
        ),
        "a zero-byte ProducerClosed ACK cannot prove Contribution delivery"
    );
}

#[test]
fn live_topn_remote_failure_evidence_requires_the_target_compiled_route() {
    let target = RouteEdgeId::new(901);
    let unrelated = vec![TopNTransportEventEvidence {
        route_edge_id: RouteEdgeId::new(902),
        kind: TransportEventKind::FailedOpen(TransportFailOpenReason::Deadline),
        bytes: 37,
    }];
    assert!(
        !has_deadline_failed_open(&unrelated, &BTreeSet::from([target])),
        "a different route cannot prove the redirected compiled route failed open"
    );
}

#[test]
fn live_topn_remote_completion_order_requires_early_availability_and_late_completion() {
    assert!(!topn_success_order_is_valid(9, &[1, 2, 3], 10, 3));
    assert!(!topn_success_order_is_valid(1, &[2, 8, 9], 7, 3));
    assert!(topn_success_order_is_valid(1, &[2, 8, 9], 10, 3));
}

fn run_live_topn(
    backend_count: usize,
    runtime_filter: bool,
    deadline_ms: u64,
    fail_remote_transport: bool,
) -> LiveTopNRun {
    let _serial = LIVE_CONFORMANCE_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let remote = backend_count > 1;
    let files = LocalTopNFiles::new(remote);
    let mut nodes = (0..backend_count)
        .map(|index| {
            IndependentGrpcRuntimeFilterNode::start()
                .unwrap_or_else(|error| panic!("start live TopN BE {index}: {error}"))
        })
        .collect::<Vec<_>>();
    let endpoints = nodes.iter().map(|node| node.endpoint()).collect::<Vec<_>>();
    let backends = LiveBackendSnapshot::new(endpoints.iter().copied().enumerate().collect());
    let sealed = sealed_topn_plan(&files.files, remote, runtime_filter, deadline_ms);
    let planner_generated_ids = (!remote && runtime_filter).then(|| {
        planner_generated_topn_ids(&sealed)
            .expect("planner-generated live TopN graph remains discoverable before deployment")
    });
    let mut connectors = crate::connector::ConnectorRegistry::new();
    connectors.register_scan_planner(Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    let prepared = crate::coordinator::prepare::prepare_fragments(&sealed, &connectors, None)
        .expect("prepare live TopN fragments");
    let bundle = crate::protocol::native::encode::encode_native_fragment_bundle(&sealed, &prepared)
        .expect("encode live TopN native bundle");
    let scheduler = Arc::new(FragmentScheduler::from_live_backend_snapshot(
        backends.clone(),
    ));
    let dispatcher: Arc<dyn FragmentDispatcher> = Arc::new(
        RemoteDispatcher::new_with_backend_ids_and_rpc_timeout_for_test(
            backends.entries(),
            MAX_WAIT,
        )
        .expect("create live TopN dispatcher"),
    );
    let observations = Arc::new(Mutex::new(Vec::new()));
    let failed_remote_endpoint = fail_remote_transport.then(|| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("reserve an unavailable live TopN transport endpoint");
        let endpoint = RuntimeEndpoint::from_socket_addr(
            listener
                .local_addr()
                .expect("read unavailable live TopN endpoint"),
        );
        drop(listener);
        endpoint
    });
    let control = Arc::new(TopNRecordingDeploymentControl {
        inner: Arc::new(
            GrpcRuntimeFilterDeploymentControl::new(backends.entries())
                .expect("create live TopN deployment control"),
        ),
        observations: Arc::clone(&observations),
        failed_remote_endpoint,
    });
    let mut ports = CoordinatorExecutionPorts::new(
        dispatcher,
        RuntimeEndpoint::from_socket_addr(endpoints[0]),
        Arc::new(RecordingCoordinatorObserver::default()),
        control,
    );
    ports.runtime_filter_policy_provider =
        Arc::new(NativeRuntimeFilterDeploymentPolicyProvider::new(2));
    let options = QueryOptions {
        query_timeout: Some(10),
        query_delivery_timeout: Some(5),
        runtime_filter_wait_timeout_ms: Some(5_000),
        pipeline_dop: Some(1),
        connector_io_tasks_per_scan_operator: Some(1),
        enable_profile: true,
        ..Default::default()
    };
    let started = Instant::now();
    let outcome = ExecutionCoordinator::new(prepared, bundle, ports, scheduler, Some(options))
        .execute_with_profiles_for_test()
        .unwrap_or_else(|error| panic!("execute production-shaped live TopN: {error}"));
    assert!(
        started.elapsed() <= Duration::from_secs(10),
        "live TopN execution exceeded ten seconds"
    );
    let query_id = query_id_from_live_nodes(&nodes);
    let observations = observations.lock().unwrap().clone();
    let mut node_evidence = Vec::new();
    let mut hub_snapshot_empty = true;
    if let Some(first) = observations.first() {
        let query_id = QueryId {
            hi: first.query_id.hi,
            lo: first.query_id.lo,
        };
        let services = nodes
            .iter()
            .map(|node| {
                node.manager()
                    .runtime_filter_service_for_ingress(query_id)
                    .expect("completed live TopN retains query Service evidence")
            })
            .collect::<Vec<_>>();
        let drain_deadline = Instant::now() + MAX_WAIT;
        while services
            .iter()
            .any(|service| service.transport_pending_len_for_test() != 0)
            && Instant::now() < drain_deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        if runtime_filter && !fail_remote_transport && remote {
            let remote_producer_routes = observations
                .iter()
                .flat_map(|observation| {
                    observation.install.routing_shard().channels()[&MANUAL_TOPN_CHANNEL]
                        .outbound_edges()
                        .iter()
                        .filter_map(|edge| {
                            (matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. })
                                && matches!(
                                    edge.source().role(),
                                    RuntimeFilterRouteRole::Producer(MANUAL_TOPN_PRODUCER_BINDING)
                                )
                                && edge.target().role() == RuntimeFilterRouteRole::Aggregator)
                                .then_some(edge.route_edge_id())
                        })
                })
                .collect::<BTreeSet<_>>();
            let evidence_deadline = Instant::now() + MAX_WAIT;
            let evidence_ready = loop {
                let ready = services.iter().any(|service| {
                    let envelopes = service.admitted_transport_envelopes_for_test();
                    let events = service.lifecycle_events_for_test();
                    remote_producer_routes.iter().any(|route_edge_id| {
                        envelopes.iter().any(|(route, envelope)| {
                            route.route_edge_id() == *route_edge_id
                                && envelope.kind() == RuntimeFilterEnvelopeKind::Contribution
                                && !envelope.payload().is_empty()
                        }) && events.iter().any(|event| matches!(
                            event,
                            RuntimeFilterEvent::TransportEnvelope {
                                identity,
                                kind: TransportEventKind::Sent,
                                bytes,
                            } if identity.route_edge_id() == *route_edge_id && *bytes > 0
                        )) && events.iter().any(|event| matches!(
                            event,
                            RuntimeFilterEvent::TransportEnvelope {
                                identity,
                                kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                                bytes,
                            } if identity.route_edge_id() == *route_edge_id && *bytes > 0
                        ))
                    })
                });
                if ready || Instant::now() >= evidence_deadline {
                    break ready;
                }
                std::thread::sleep(Duration::from_millis(5));
            };
            assert!(
                evidence_ready,
                "the live TopN snapshot waits for its asserted nonterminal Contribution Sent/Accepted evidence"
            );
        }
        for service in services {
            let events = service.lifecycle_events_for_test();
            let participant = events
                .iter()
                .find_map(|event| match event {
                    RuntimeFilterEvent::DeploymentInstalled { participant_id, .. } => {
                        Some(*participant_id)
                    }
                    _ => None,
                })
                .expect("installed service records its authoritative participant");
            let transport_events = events
                .iter()
                .filter_map(|event| match event {
                    RuntimeFilterEvent::TransportEnvelope {
                        identity,
                        kind,
                        bytes,
                    } => Some(TopNTransportEventEvidence {
                        route_edge_id: identity.route_edge_id(),
                        kind: *kind,
                        bytes: *bytes,
                    }),
                    _ => None,
                })
                .collect();
            node_evidence.push(TopNNodeEvidence {
                participant,
                pending_transport: service.transport_pending_len_for_test(),
                envelopes: service
                    .admitted_transport_envelopes_for_test()
                    .into_iter()
                    .map(|(route, envelope)| TopNAdmittedEnvelopeEvidence {
                        route_edge_id: route.route_edge_id(),
                        target: route.target_role(),
                        kind: envelope.kind(),
                        bytes: envelope.payload().len(),
                    })
                    .collect(),
                transport_events,
                events,
            });
        }
        let query = QueryKey::from_hi_lo(first.query_id.hi, first.query_id.lo);
        if let Some(snapshot) = RuntimeFilterLifecycleRegistry::global().snapshot(query) {
            hub_snapshot_empty = snapshot.filters.is_empty();
        }
        RuntimeFilterLifecycleRegistry::global().remove_query(query);
    }
    drain_final_reports_before_node_shutdown(query_id);
    for node in &mut nodes {
        node.shutdown().expect("shutdown live TopN BE");
    }
    LiveTopNRun {
        outcome,
        planner_generated_ids,
        observations,
        node_evidence,
        hub_snapshot_empty,
    }
}

fn topn_result_values(outcome: &CoordinatedQueryResult) -> Vec<i32> {
    outcome
        .query_result
        .chunks
        .iter()
        .flat_map(|chunk| {
            chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("live TopN returns Int32")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn topn_counter(profiles: &[RuntimeProfileTree], name: &str) -> i64 {
    fn sum(node: &ProfileNode, name: &str) -> i64 {
        node.counters
            .iter()
            .filter(|counter| counter.name == name)
            .map(|counter| counter.value)
            .sum::<i64>()
            + node
                .children
                .iter()
                .map(|child| sum(child, name))
                .sum::<i64>()
    }
    profiles
        .iter()
        .map(|profile| sum(&profile.root, name))
        .sum()
}

fn assert_planner_loopback_topn_common(on: &LiveTopNRun, off: &LiveTopNRun, expected: &[i32]) {
    let ids = on
        .planner_generated_ids
        .as_ref()
        .expect("runtime-filter-on result carries planner-generated ids");
    assert!(
        !ids.consumers.is_empty(),
        "planner-generated live TopN contract retains at least one source consumer"
    );
    assert_eq!(topn_result_values(&on.outcome), expected);
    assert_eq!(
        topn_result_values(&on.outcome),
        topn_result_values(&off.outcome),
        "runtime filtering does not change the query fingerprint"
    );
    assert!(
        topn_counter(&on.outcome.fragment_profiles, RUNTIME_FILTER_INPUT_ROWS)
            > topn_counter(&on.outcome.fragment_profiles, RUNTIME_FILTER_OUTPUT_ROWS),
        "the published ordered bound tightens already-read chunks for the Batch live consumer, \
         rather than skipping unopened FileRange morsels"
    );
    assert!(
        on.hub_snapshot_empty,
        "native TopN never records legacy Hub activity"
    );
    assert!(
        on.observations.iter().all(|observation| {
            observation.install.core_view().channels()[&ids.channel]
                .allowed_contribution_kinds()
                .contains(&ContributionKind::OrderedBoundUpdate)
                && !observation.install.core_view().channels()[&ids.channel]
                    .allowed_contribution_kinds()
                    .contains(&ContributionKind::TopKSummary)
        }),
        "installed channels accept OrderedBound updates and reject TopKSummary"
    );
    assert!(
        on.node_evidence
            .iter()
            .all(|evidence| evidence.pending_transport == 0),
        "all reliable transports drain"
    );
}

fn assert_manual_remote_topn_common(on: &LiveTopNRun, off: &LiveTopNRun, expected: &[i32]) {
    assert!(
        on.planner_generated_ids.is_none(),
        "the preserved remote fixture installs its explicit manual runtime-filter graph"
    );
    assert_eq!(topn_result_values(&on.outcome), expected);
    assert_eq!(
        topn_result_values(&on.outcome),
        topn_result_values(&off.outcome),
        "runtime filtering does not change the query fingerprint"
    );
    assert!(
        topn_counter(
            &on.outcome.fragment_profiles,
            "NativeOrderedRuntimeFilterLatePrunedUnits"
        ) > 0,
        "the Split live consumer prunes at least one unopened scan unit"
    );
    assert!(
        on.hub_snapshot_empty,
        "native TopN never records legacy Hub activity"
    );
    assert!(
        on.observations.iter().all(|observation| {
            observation.install.core_view().channels()[&MANUAL_TOPN_CHANNEL]
                .allowed_contribution_kinds()
                .contains(&ContributionKind::OrderedBoundUpdate)
                && !observation.install.core_view().channels()[&MANUAL_TOPN_CHANNEL]
                    .allowed_contribution_kinds()
                    .contains(&ContributionKind::TopKSummary)
        }),
        "installed channels accept OrderedBound updates and reject TopKSummary"
    );
    assert!(
        on.node_evidence
            .iter()
            .all(|evidence| evidence.pending_transport == 0),
        "all reliable transports drain"
    );
}

#[test]
fn live_topn_loopback_executes_aggregate_service_and_live_scan_chain() {
    let on = run_live_topn(1, true, 5_000, false);
    let off = run_live_topn(1, false, 5_000, false);
    assert_planner_loopback_topn_common(&on, &off, &[5, 20]);
    let events = &on
        .node_evidence
        .first()
        .expect("loopback service evidence")
        .events;
    let first_idle = events
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::LiveSubscriptionIdle { .. }))
        .expect("scan polls before the first TopN version");
    let first_publish = events
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::LogicalVersionPublished { .. }))
        .expect("TopN candidates publish an ordered bound");
    assert!(
        first_idle < first_publish,
        "the live scan makes progress without waiting for v1"
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, RuntimeFilterEvent::LiveSubscriptionUpdated { .. })),
        "the loopback scan consumer observes a published ordered bound"
    );
    let ids = on
        .planner_generated_ids
        .as_ref()
        .expect("loopback run retains planner-generated ids");
    assert!(on.observations.iter().any(|observation| {
        observation.install.routing_shard().channels()[&ids.channel]
            .outbound_edges()
            .iter()
            .any(|edge| {
                matches!(
                    edge.peer(),
                    crate::runtime_filter::port::routing::RuntimeFilterRoutePeer::Loopback
                )
            })
    }));
}

#[test]
fn live_topn_remote_uses_contribution_ack_and_artifact_delivery() {
    let on = run_live_topn(3, true, 5_000, false);
    let off = run_live_topn(3, false, 5_000, false);
    assert_manual_remote_topn_common(&on, &off, &[1, 1]);
    assert_eq!(on.node_evidence.len(), 3);
    let aggregator_install = on
        .observations
        .iter()
        .find(|observation| {
            observation.install.routing_shard().channels()[&MANUAL_TOPN_CHANNEL]
                .outbound_edges()
                .iter()
                .any(|edge| {
                    edge.source().participant_id() == observation.participant
                        && matches!(edge.source().role(), RuntimeFilterRouteRole::Aggregator)
                })
        })
        .expect("one participant owns the ordered aggregate");
    let aggregator_evidence = on
        .node_evidence
        .iter()
        .find(|evidence| evidence.participant == aggregator_install.participant)
        .expect("aggregate owner service evidence");
    let remote_producer_routes = on
        .observations
        .iter()
        .flat_map(|observation| {
            observation.install.routing_shard().channels()[&MANUAL_TOPN_CHANNEL]
                .outbound_edges()
                .iter()
                .filter_map(|edge| {
                    (edge.source().participant_id() == observation.participant
                        && edge.source().participant_id() != edge.target().participant_id()
                        && matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. })
                        && matches!(
                            edge.source().role(),
                            RuntimeFilterRouteRole::Producer(MANUAL_TOPN_PRODUCER_BINDING)
                        )
                        && edge.target().role() == RuntimeFilterRouteRole::Aggregator)
                        .then_some((observation.participant, edge.route_edge_id()))
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        remote_producer_routes.len(),
        2,
        "the compiler emits two cross-participant producer-to-aggregate routes"
    );
    for (participant, route_edge_id) in &remote_producer_routes {
        let evidence = on
            .node_evidence
            .iter()
            .find(|evidence| evidence.participant == *participant)
            .expect("remote producer service evidence");
        assert!(
            evidence.envelopes.iter().any(|envelope| {
                envelope.route_edge_id == *route_edge_id
                    && envelope.target == RuntimeFilterRouteRole::Aggregator
                    && envelope.kind == RuntimeFilterEnvelopeKind::ProducerClosed
                    && envelope.bytes == 0
            }),
            "remote producer sends its terminal on the same compiled route"
        );
        assert!(
            evidence.transport_events.iter().any(|event| {
                event.route_edge_id == *route_edge_id
                    && event.kind == TransportEventKind::Sent
                    && event.bytes == 0
            }) && evidence.transport_events.iter().any(|event| {
                event.route_edge_id == *route_edge_id
                    && event.kind == TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted)
                    && event.bytes == 0
            }),
            "remote ProducerClosed frame is sent and ACKed on its compiled route"
        );
    }
    assert!(
        remote_producer_routes
            .iter()
            .any(|(participant, route_edge_id)| {
                on.node_evidence
                    .iter()
                    .find(|evidence| evidence.participant == *participant)
                    .is_some_and(|evidence| {
                        has_accepted_data_frame(
                            &evidence.envelopes,
                            &evidence.transport_events,
                            *route_edge_id,
                            RuntimeFilterEnvelopeKind::Contribution,
                        )
                    })
            }),
        "a nonterminal Contribution has a Sent/Accepted lifecycle correlated by compiled route and nonzero frame bytes"
    );
    let remote_artifact_routes = aggregator_install.install.routing_shard().channels()
        [&MANUAL_TOPN_CHANNEL]
        .outbound_edges()
        .iter()
        .filter_map(|edge| {
            (matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. })
                && edge.source().participant_id() != edge.target().participant_id()
                && matches!(edge.source().role(), RuntimeFilterRouteRole::Aggregator)
                && matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_)))
            .then_some(edge.route_edge_id())
        })
        .collect::<Vec<_>>();
    assert!(
        !remote_artifact_routes.is_empty(),
        "the same consumer implementation receives a remote aggregate artifact"
    );
    assert!(
        remote_artifact_routes.iter().any(|route_edge_id| {
            [
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::FinalArtifact,
            ]
            .into_iter()
            .any(|kind| {
                has_accepted_data_frame(
                    &aggregator_evidence.envelopes,
                    &aggregator_evidence.transport_events,
                    *route_edge_id,
                    kind,
                )
            })
        }),
        "an Artifact/FinalArtifact has a Sent/Accepted lifecycle correlated by compiled route and nonzero frame bytes"
    );
    let expected_instances = aggregator_install.install.core_view().channels()
        [&MANUAL_TOPN_CHANNEL]
        .producers()[&MANUAL_TOPN_PRODUCER_BINDING]
        .expected_fragment_instances()
        .clone();
    let expected_producers = expected_instances.len();
    assert_eq!(
        expected_producers, 3,
        "the installed completion contract requires all three producers"
    );
    let availability_position = aggregator_evidence
        .events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeFilterEvent::OrderedAvailabilityReached { identity }
                    if identity.channel_id() == MANUAL_TOPN_CHANNEL
            )
        })
        .expect("one sound producer contribution reaches availability");
    let terminal_admissions = aggregator_evidence
        .events
        .iter()
        .enumerate()
        .filter_map(|(position, event)| {
            let RuntimeFilterEvent::ProducerInstanceClosed { identity } = event else {
                return None;
            };
            (identity.common().channel_id() == MANUAL_TOPN_CHANNEL
                && identity.producer_binding_id() == MANUAL_TOPN_PRODUCER_BINDING)
                .then_some((position, identity.fragment_instance_id()))
        })
        .collect::<Vec<_>>();
    let terminal_positions = terminal_admissions
        .iter()
        .map(|(position, _)| *position)
        .collect::<Vec<_>>();
    let terminal_instances = terminal_admissions
        .iter()
        .map(|(_, fragment_instance_id)| *fragment_instance_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        terminal_instances, expected_instances,
        "aggregate owner admits exactly the compiler-required producer instances"
    );
    let completed_position = aggregator_evidence
        .events
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeFilterEvent::ChannelCompleted { identity, .. }
                    if identity.channel_id() == MANUAL_TOPN_CHANNEL
            )
        })
        .expect("aggregate owner completes");
    assert!(
        topn_success_order_is_valid(
            availability_position,
            &terminal_positions,
            completed_position,
            expected_producers,
        ),
        "availability precedes a remaining producer close and ChannelCompleted follows all three ProducerInstanceClosed admissions: availability={availability_position} terminals={terminal_positions:?} completed={completed_position} expected={expected_producers}"
    );
    assert!(
        on.node_evidence
            .iter()
            .flat_map(|evidence| &evidence.events)
            .all(|event| !matches!(event, RuntimeFilterEvent::TopKSummaryApplied { .. })),
        "no TopKSummary contribution enters the live TopN chain"
    );
}

#[test]
fn live_topn_remote_timeout_fails_open_with_correct_results() {
    let timed_out = run_live_topn(3, true, 500, true);
    let off = run_live_topn(3, false, 5_000, false);
    assert_eq!(
        topn_result_values(&timed_out.outcome),
        topn_result_values(&off.outcome),
        "deadline failure leaves the live scan in pass-through mode"
    );
    let failed_remote_routes = timed_out
        .observations
        .iter()
        .flat_map(|observation| {
            observation.install.routing_shard().channels()[&MANUAL_TOPN_CHANNEL]
                .outbound_edges()
                .iter()
                .filter_map(|edge| {
                    (edge.source().participant_id() == observation.participant
                        && edge.source().participant_id() != edge.target().participant_id()
                        && matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. }))
                    .then_some(edge.route_edge_id())
                })
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    assert!(
        !failed_remote_routes.is_empty(),
        "failure fixture redirects compiler-produced remote routes"
    );
    assert!(
        timed_out
            .node_evidence
            .iter()
            .any(|evidence| has_deadline_failed_open(
                &evidence.transport_events,
                &failed_remote_routes,
            )),
        "a redirected compiler-produced route reaches exact Deadline FailedOpen"
    );
}
