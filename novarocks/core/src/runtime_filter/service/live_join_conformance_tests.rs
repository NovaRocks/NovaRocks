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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::types::UniqueId;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef,
    IcebergTableInfo,
};
use crate::coordinator::cluster::LiveBackendSnapshot;
use crate::coordinator::dispatch::{FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope};
use crate::coordinator::execution::{CoordinatedQueryResult, ExecutionCoordinator};
use crate::coordinator::ports::{
    CoordinatorExecutionPorts, CoordinatorObserver, RuntimeFilterDeploymentControlPort,
};
use crate::coordinator::runtime_filter_deployment::NativeRuntimeFilterDeploymentPolicyProvider;
use crate::coordinator::scheduler::FragmentScheduler;
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::operators::hashjoin::native_runtime_filter::{
    NativeProducerCloseGateGuard, install_native_producer_close_gate_for_test,
};
use crate::protocol::native::RuntimeFilterQueryLifecycleOptions;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::profile::{ProfileNode, RuntimeProfileTree};
use crate::runtime::query_options::QueryOptions;
use crate::runtime_filter::model::contract::BindingId;
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::model::graph::{ConsumerBindingTarget, RuntimeFilterBindingRole};
use crate::runtime_filter::port::events::{RuntimeFilterEvent, TransportEventKind};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::install::RuntimeFilterParticipantInstall;
use crate::runtime_filter::port::routing::{RuntimeFilterRoutePeer, RuntimeFilterRouteRole};
use crate::runtime_filter::port::transport::{
    RuntimeFilterAcceptStatus, RuntimeFilterEnvelopeKind,
};
use crate::runtime_filter::service::{
    NativeAcquireGateGuard, install_native_acquire_gate_for_test,
};
use crate::service::grpc_client::NovaRocksGrpcRemoteClient;
use crate::service::grpc_fragment_dispatcher::GrpcRuntimeFilterDeploymentControl;
use crate::service::grpc_fragment_dispatcher::RemoteDispatcher;
use crate::service::grpc_server::IndependentGrpcRuntimeFilterNode;
use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::payload::{AggregateCall, PlanScanNode, PlanValuesNode};
use crate::sql::planner::physical::runtime_filter::{
    RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalHashAggregateNode, PhysicalHashJoinEqCondition, PhysicalHashJoinNode, PhysicalPlanKind,
    PhysicalPlanNode, PhysicalPlanStats, PlannerConfidence, RedistributeMode, RedistributeNode,
};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

const MAX_WAIT: Duration = Duration::from_secs(5);
const FILTER_ID: i32 = 701;
static LIVE_JOIN_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinTopology {
    Broadcast,
    Partitioned,
}

#[derive(Clone)]
struct InstallObservation {
    query_id: UniqueId,
    install: RuntimeFilterParticipantInstall,
}

struct RecordingDeploymentControl {
    inner: Arc<GrpcRuntimeFilterDeploymentControl>,
    observations: Arc<Mutex<Vec<InstallObservation>>>,
    acquire_gate: Option<Arc<Mutex<Option<NativeAcquireGateGuard>>>>,
    broadcast_loser_gates: Option<Arc<BroadcastLoserGateController>>,
}

struct BroadcastLoserGateController {
    managers: Vec<Arc<crate::runtime::query_context::QueryContextManager>>,
    initialized: AtomicBool,
    guards: Mutex<Vec<NativeProducerCloseGateGuard>>,
    watcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    losers_entered: AtomicBool,
    released_after_delivery: AtomicBool,
}

impl BroadcastLoserGateController {
    fn new(managers: Vec<Arc<crate::runtime::query_context::QueryContextManager>>) -> Arc<Self> {
        Arc::new(Self {
            managers,
            initialized: AtomicBool::new(false),
            guards: Mutex::new(Vec::new()),
            watcher: Mutex::new(None),
            losers_entered: AtomicBool::new(false),
            released_after_delivery: AtomicBool::new(false),
        })
    }

    fn initialize(self: &Arc<Self>, query_id: UniqueId, install: &RuntimeFilterParticipantInstall) {
        if self.initialized.swap(true, Ordering::SeqCst) {
            return;
        }
        let producers = install
            .routing_shard()
            .channels()
            .values()
            .flat_map(|channel| channel.producer_instances().keys().copied())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(producers.len(), 3, "broadcast gate sees three replicas");
        let mut producers = producers.into_iter();
        let _winner = producers.next().expect("broadcast has a winner");
        let guards = producers
            .map(|(binding_id, finst_id)| {
                install_native_producer_close_gate_for_test(query_id, binding_id, finst_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(guards.len(), 2, "broadcast gates exactly two losers");
        *self.guards.lock().unwrap() = guards;

        let controller = Arc::clone(self);
        let watcher = std::thread::spawn(move || {
            let deadline = Instant::now() + MAX_WAIT;
            let all_entered = controller.guards.lock().unwrap().iter().all(|guard| {
                guard.wait_entered(deadline.saturating_duration_since(Instant::now()))
            });
            controller
                .losers_entered
                .store(all_entered, Ordering::SeqCst);

            let query = crate::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
                query_id.hi,
                query_id.lo,
            );
            let mut published_and_delivered = false;
            while Instant::now() < deadline {
                let events = crate::runtime::runtime_filter_observability::RuntimeFilterLifecycleRegistry::global()
                    .snapshot(query)
                    .map(|snapshot| {
                        snapshot
                            .channel_events
                            .into_values()
                            .flatten()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let published = events
                    .iter()
                    .any(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }));
                let query_id = crate::runtime::query_context::QueryId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                };
                let delivered = controller.managers.iter().any(|manager| {
                    manager
                        .runtime_filter_service_for_ingress(query_id)
                        .is_some_and(|service| {
                            service
                                .admitted_transport_envelopes_for_test()
                                .into_iter()
                                .any(|(route, envelope)| {
                                    matches!(
                                        route.target_role(),
                                        RuntimeFilterRouteRole::Consumer(_)
                                    ) && matches!(
                                        envelope.kind(),
                                        RuntimeFilterEnvelopeKind::Artifact
                                            | RuntimeFilterEnvelopeKind::FinalArtifact
                                    )
                                })
                        })
                });
                if published && delivered {
                    published_and_delivered = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            controller
                .released_after_delivery
                .store(published_and_delivered, Ordering::SeqCst);
            for guard in controller.guards.lock().unwrap().iter() {
                guard.release();
            }
        });
        *self.watcher.lock().unwrap() = Some(watcher);
    }

    fn join(&self) {
        if let Some(watcher) = self.watcher.lock().unwrap().take() {
            watcher.join().expect("broadcast loser gate watcher");
        }
    }
}

#[async_trait::async_trait]
impl RuntimeFilterDeploymentControlPort for RecordingDeploymentControl {
    async fn install(
        &self,
        query_id: UniqueId,
        lifecycle: RuntimeFilterQueryLifecycleOptions,
        deadline: Duration,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), String> {
        if let Some(slot) = self.acquire_gate.as_ref() {
            let mut slot = slot.lock().unwrap();
            if slot.is_none() {
                *slot = Some(install_native_acquire_gate_for_test(
                    query_id,
                    BindingId::new(2),
                ));
            }
        }
        if let Some(controller) = self.broadcast_loser_gates.as_ref() {
            controller.initialize(query_id, &install);
        }
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
            .push(InstallObservation { query_id, install });
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

#[derive(Default)]
struct CountingObserver(AtomicUsize);

impl CoordinatorObserver for CountingObserver {
    fn fragment_scheduled(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct CancelAfterJoinDispatcher {
    inner: Arc<RemoteDispatcher>,
    managers: Vec<Arc<crate::runtime::query_context::QueryContextManager>>,
    acquire_gate: Arc<Mutex<Option<NativeAcquireGateGuard>>>,
    captured_services: Mutex<Vec<Arc<super::RuntimeFilterService>>>,
    submitted: Mutex<BTreeMap<usize, Vec<UniqueId>>>,
    join_submissions: AtomicUsize,
    source_submissions: AtomicUsize,
    triggered: AtomicBool,
    ownership_checked: AtomicBool,
    wrong_fetch_rejected: AtomicBool,
    wrong_cancel_rejected: AtomicBool,
}

impl CancelAfterJoinDispatcher {
    fn new(
        inner: Arc<RemoteDispatcher>,
        managers: Vec<Arc<crate::runtime::query_context::QueryContextManager>>,
        acquire_gate: Arc<Mutex<Option<NativeAcquireGateGuard>>>,
    ) -> Self {
        Self {
            inner,
            managers,
            acquire_gate,
            captured_services: Mutex::new(Vec::new()),
            submitted: Mutex::new(BTreeMap::new()),
            join_submissions: AtomicUsize::new(0),
            source_submissions: AtomicUsize::new(0),
            triggered: AtomicBool::new(false),
            ownership_checked: AtomicBool::new(false),
            wrong_fetch_rejected: AtomicBool::new(false),
            wrong_cancel_rejected: AtomicBool::new(false),
        }
    }

    fn cancel_submitted(&self) {
        for (backend, finsts) in self.submitted.lock().unwrap().iter() {
            self.inner.cancel_fragments(*backend, finsts);
        }
    }
}

impl FragmentDispatcher for CancelAfterJoinDispatcher {
    fn submit_fragment(
        &self,
        backend_idx: usize,
        submission: NativeFragmentEnvelope,
    ) -> Result<(), String> {
        let is_join = wire_contains_hash_join(submission.plan_for_test().root.as_ref());
        let source_ordinal = if !is_join && self.join_submissions.load(Ordering::SeqCst) == 3 {
            self.source_submissions.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            0
        };
        let trigger_after_submit =
            source_ordinal == 1 && !self.triggered.swap(true, Ordering::SeqCst);
        let query = trigger_after_submit
            .then(|| submission.query_id())
            .transpose()?;
        let submission_query = submission.query_id()?;
        let finst = submission.fragment_instance_id()?;
        self.inner.submit_fragment(backend_idx, submission)?;
        self.submitted
            .lock()
            .unwrap()
            .entry(backend_idx)
            .or_default()
            .push(finst);
        if is_join {
            self.join_submissions.fetch_add(1, Ordering::SeqCst);
            if !self.ownership_checked.swap(true, Ordering::SeqCst) {
                let wrong_backend = (backend_idx + 1) % self.managers.len();
                let wrong_addr = self
                    .inner
                    .addr_of(wrong_backend)
                    .expect("wrong backend has an endpoint");
                let client = NovaRocksGrpcRemoteClient::new(wrong_addr)?;
                let fetch = client.blocking_fetch_result_with_timeout(
                    crate::proto::novarocks::FetchResultRequest {
                        finst_id: Some(crate::proto::common::UniqueId {
                            hi: finst.hi,
                            lo: finst.lo,
                        }),
                        max_wait_ms: 0,
                    },
                    MAX_WAIT,
                )?;
                let fetch_rejected = fetch.status
                    == crate::proto::novarocks::fetch_result_response::Status::Error as i32
                    && fetch.message.contains("not owned by this endpoint");
                self.wrong_fetch_rejected
                    .store(fetch_rejected, Ordering::SeqCst);
                if !fetch_rejected {
                    return Err(format!(
                        "wrong-node fetch was not rejected: status={} message={}",
                        fetch.status, fetch.message
                    ));
                }
                let cancel = client.blocking_cancel_fragment_with_timeout(
                    crate::proto::novarocks::CancelFragmentRequest {
                        finst_ids: vec![crate::proto::common::UniqueId {
                            hi: finst.hi,
                            lo: finst.lo,
                        }],
                        reason: "wrong-node ownership probe".to_string(),
                        start_epoch: 0,
                    },
                    MAX_WAIT,
                )?;
                let cancel_rejected = cancel.status_code != 0;
                self.wrong_cancel_rejected
                    .store(cancel_rejected, Ordering::SeqCst);
                if !cancel_rejected {
                    return Err("wrong-node cancel was not rejected".to_string());
                }
                let query_id = crate::runtime::query_context::QueryId {
                    hi: submission_query.hi,
                    lo: submission_query.lo,
                };
                if self.managers[backend_idx].is_query_canceled(query_id) {
                    return Err("wrong-node cancel reached the owning manager".to_string());
                }
            }
        }
        if let Some(query) = query {
            let query_id = crate::runtime::query_context::QueryId {
                hi: query.hi,
                lo: query.lo,
            };
            *self.captured_services.lock().unwrap() = self
                .managers
                .iter()
                .map(|manager| {
                    manager
                        .runtime_filter_service_for_ingress(query_id)
                        .expect("cancel gate captures every query-owned service before RPC")
                })
                .collect();
            let gate = self.acquire_gate.lock().unwrap();
            let gate = gate
                .as_ref()
                .expect("deployment install creates the query/binding acquire gate");
            assert!(
                gate.wait_entered(MAX_WAIT),
                "probe consumer did not enter blocking acquire before its deadline"
            );
            self.cancel_submitted();
            return Err(
                "injected cancellation after live Join producer and consumer became active"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_chunk_schema: Option<&ChunkSchemaRef>,
    ) -> Result<FetchOutcome, String> {
        self.inner.fetch_result(
            backend_idx,
            finst_id,
            max_wait_ms.min(i64::try_from(MAX_WAIT.as_millis()).unwrap()),
            expected_chunk_schema,
        )
    }

    fn cancel_fragments(&self, backend_idx: usize, finst_ids: &[UniqueId]) {
        self.inner.cancel_fragments(backend_idx, finst_ids);
    }

    fn backend_count(&self) -> usize {
        self.inner.backend_count()
    }

    fn needs_fragment_status_report(&self) -> bool {
        self.inner.needs_fragment_status_report()
    }
}

fn wire_contains_hash_join(node: Option<&crate::proto::plan::DistributedNode>) -> bool {
    let Some(node) = node else {
        return false;
    };
    let is_join = node.payload.as_ref().is_some_and(|payload| {
        matches!(
            payload,
            crate::proto::plan::distributed_node::Payload::Physical(physical)
                if matches!(
                    physical.kind,
                    Some(crate::proto::plan::plan_node::Kind::HashJoin(_))
                )
        )
    });
    is_join
        || node
            .children
            .iter()
            .any(|child| wire_contains_hash_join(Some(child)))
}

struct LiveJoinRun {
    outcome: CoordinatedQueryResult,
    observations: Vec<InstallObservation>,
    // Concatenated query-owned service recorders. All node facts come from here.
    lifecycle_events: Vec<RuntimeFilterEvent>,
    // Supplemental process-wide order only for assertions that span services.
    global_order_events: Vec<RuntimeFilterEvent>,
    node_evidence: Vec<NodeEvidence>,
    scheduled_fragments: usize,
    backend_count: usize,
    direct_input_zero: bool,
    broadcast_losers_entered: bool,
    broadcast_losers_released_after_delivery: bool,
    partitioned_distinct_shards: bool,
}

struct CancelRun {
    error: String,
    triggered: bool,
    manager_cancel_errors: Vec<Option<String>>,
    pending_transport: Vec<usize>,
    producer_handles_remaining: usize,
    lifecycle_events: Vec<RuntimeFilterEvent>,
    expected_producers: BTreeMap<UniqueId, RuntimeFilterParticipantId>,
    participant_events: BTreeMap<RuntimeFilterParticipantId, Vec<RuntimeFilterEvent>>,
    wrong_fetch_rejected: bool,
    wrong_cancel_rejected: bool,
}

struct NodeEvidence {
    participant: RuntimeFilterParticipantId,
    pending_transport: usize,
    admitted_envelopes: Vec<(RuntimeFilterRouteRole, RuntimeFilterEnvelopeKind)>,
    lifecycle_events: Vec<RuntimeFilterEvent>,
}

struct LocalJoinFiles {
    _dir: tempfile::TempDir,
    probe: Vec<IcebergDataFileInfo>,
    build: Vec<IcebergDataFileInfo>,
}

impl LocalJoinFiles {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("novarocks-live-join-")
            .tempdir()
            .expect("create live Join tempdir");
        let probe = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
            .into_iter()
            .enumerate()
            .map(|(index, values)| write_int32_file(dir.path(), "probe", index, &values))
            .collect();
        let build = [[2], [5], [8]]
            .into_iter()
            .enumerate()
            .map(|(index, values)| write_int32_file(dir.path(), "build", index, &values))
            .collect();
        Self {
            _dir: dir,
            probe,
            build,
        }
    }
}

fn write_int32_file(
    dir: &std::path::Path,
    prefix: &str,
    index: usize,
    values: &[i32],
) -> IcebergDataFileInfo {
    let path = dir.join(format!("{prefix}-{index}.parquet"));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values.to_vec()))])
        .expect("build live Join parquet batch");
    crate::formats::parquet::local_io::write_parquet_to_path(&path, &batch)
        .expect("write live Join parquet");
    let size = i64::try_from(std::fs::metadata(&path).unwrap().len()).unwrap();
    IcebergDataFileInfo::for_test(
        &format!("file://{}", path.display()),
        size,
        i64::try_from(values.len()).unwrap(),
    )
}

fn output_column(id: u32, name: &str) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_internal: false,
    }
}

fn column_expr(id: u32, qualifier: &str, name: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId::new_for_test(id),
            qualifier: Some(qualifier.to_string()),
            column: name.to_string(),
        },
        data_type: DataType::Int32,
        nullable: false,
    }
}

fn stats(rows: f64) -> PhysicalPlanStats {
    PhysicalPlanStats {
        output_row_count: rows,
        row_count_confidence: PlannerConfidence::Exact,
        column_statistics: Default::default(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn table_info(name: &str, location: &str) -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "live_join".to_string(),
        namespace: "default".to_string(),
        table: name.to_string(),
        table_uuid: Some(format!("00000000-0000-0000-0000-{:012}", name.len())),
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

fn scan_node(id: u32, alias: &str, files: Vec<IcebergDataFileInfo>) -> PhysicalPlanNode {
    let column = output_column(id, "k");
    let location = files
        .first()
        .map(|file| file.path.clone())
        .unwrap_or_else(|| "file:///empty".to_string());
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: alias.to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: table_info(alias, &location),
                    files,
                    cloud_properties: BTreeMap::new(),
                    binding: IcebergDataFileBinding::ExplicitFiles,
                },
            },
            alias: Some(alias.to_string()),
            columns: vec![column.clone()],
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        children: Vec::new(),
        output_columns: vec![column],
        stats: stats(9.0),
        probe_runtime_filters: Vec::new(),
    }
}

fn redistribute_hash(input: PhysicalPlanNode, expr: TypedExpr) -> PhysicalPlanNode {
    let output_columns = input.output_columns.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Hash {
                cols: vec![expr_column_id(&expr)],
                source: HashSource::ShuffleJoin,
            },
            partition_exprs: vec![expr],
            output_columns: output_columns.clone(),
        }),
        children: vec![input],
        output_columns,
        stats: stats(9.0),
        probe_runtime_filters: Vec::new(),
    }
}

fn expr_column_id(expr: &TypedExpr) -> ColumnId {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => unreachable!("live Join hash expression is a column"),
    }
}

fn values_build() -> PhysicalPlanNode {
    let column = output_column(2, "k");
    let rows = [2_i64, 5]
        .into_iter()
        .map(|value| {
            vec![TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }]
        })
        .collect();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(PlanValuesNode {
            rows,
            columns: vec![column.clone()],
        }),
        children: Vec::new(),
        output_columns: vec![column],
        stats: stats(2.0),
        probe_runtime_filters: Vec::new(),
    }
}

fn join_plan(
    topology: JoinTopology,
    runtime_filter: bool,
    files: &LocalJoinFiles,
) -> PhysicalPlanNode {
    let probe_expr = column_expr(1, "probe", "k");
    let build_expr = column_expr(2, "build", "k");
    let probe = redistribute_hash(
        scan_node(1, "probe", files.probe.clone()),
        probe_expr.clone(),
    );
    let build = match topology {
        JoinTopology::Broadcast => values_build(),
        JoinTopology::Partitioned => redistribute_hash(
            scan_node(2, "build", files.build.clone()),
            build_expr.clone(),
        ),
    };
    let execution_mode = match topology {
        JoinTopology::Broadcast => JoinExecutionMode::Broadcast,
        JoinTopology::Partitioned => JoinExecutionMode::Partitioned,
    };
    let distribution = match topology {
        JoinTopology::Broadcast => JoinDistribution::Broadcast,
        JoinTopology::Partitioned => JoinDistribution::Shuffle,
    };
    let join_columns = vec![output_column(1, "probe_k"), output_column(2, "build_k")];
    let join = PhysicalPlanNode {
        kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: probe_expr.clone(),
                right: build_expr.clone(),
                null_safe: false,
            }],
            other_condition: None,
            distribution,
            execution_mode: Some(execution_mode),
            build_runtime_filters: runtime_filter
                .then_some(RuntimeFilterBuildIntent {
                    filter_id: FILTER_ID,
                    build_expr,
                    probe_expr: probe_expr.clone(),
                    expr_order: 0,
                    execution_mode,
                })
                .into_iter()
                .collect(),
            output_columns: join_columns.clone(),
        })),
        children: vec![probe, build],
        output_columns: join_columns.clone(),
        stats: stats(3.0),
        probe_runtime_filters: runtime_filter
            .then_some(RuntimeFilterProbeIntent {
                filter_id: FILTER_ID,
                probe_expr: probe_expr.clone(),
            })
            .into_iter()
            .collect(),
    };
    let gather = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: Vec::new(),
            output_columns: join_columns.clone(),
        }),
        children: vec![join],
        output_columns: join_columns,
        stats: stats(3.0),
        probe_runtime_filters: Vec::new(),
    };
    let count_column = OutputColumn {
        column_id: ColumnId::new_for_test(20),
        name: "count".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    };
    PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: Vec::new(),
            aggregates: vec![AggregateCall {
                name: "count".to_string(),
                args: Vec::new(),
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
                output_column_id: ColumnId::new_for_test(20),
            }],
            is_merge: vec![false],
            output_layout: AggregateOutputLayout::new(Vec::new(), vec![count_column.clone()]),
            output_columns: vec![count_column.clone()],
            topn_runtime_filter_builds: Vec::new(),
        })),
        children: vec![gather],
        output_columns: vec![count_column],
        stats: stats(1.0),
        probe_runtime_filters: Vec::new(),
    }
}

fn cancel_join_plan(files: &LocalJoinFiles) -> PhysicalPlanNode {
    let mut plan = join_plan(JoinTopology::Partitioned, true, files);
    let gather = plan
        .children
        .first_mut()
        .expect("cancel aggregate has gather");
    let join_node = gather.children.first_mut().expect("cancel gather has Join");
    join_node.children.swap(0, 1);
    let PhysicalPlanKind::HashJoin(join) = &mut join_node.kind else {
        panic!("cancel fixture owns a HashJoin")
    };
    join.join_type = JoinKind::RightSemi;
    let condition = &mut join.eq_conditions[0];
    std::mem::swap(&mut condition.left, &mut condition.right);
    let probe_output = vec![output_column(1, "probe_k")];
    join.output_columns = probe_output.clone();
    join_node.output_columns = probe_output.clone();
    let PhysicalPlanKind::Redistribute(redistribute) = &mut gather.kind else {
        panic!("cancel fixture owns a Gather")
    };
    redistribute.output_columns = probe_output.clone();
    gather.output_columns = probe_output;
    plan
}

fn loopback_join_plan(files: &LocalJoinFiles) -> PhysicalPlanNode {
    let mut plan = join_plan(JoinTopology::Broadcast, true, files);
    let mut gather = plan.children.remove(0);
    let mut join = gather.children.remove(0);
    let mut probe_redistribute = join.children.remove(0);
    let probe_scan = probe_redistribute.children.remove(0);
    join.children.insert(0, probe_scan);
    plan.children.push(join);
    plan
}

fn drain_final_reports_before_node_shutdown(query_id: crate::runtime::query_context::QueryId) {
    assert!(
        crate::service::standalone_exec_state_reporter::wait_for_final_reports_for_query_for_test(
            query_id, MAX_WAIT,
        ),
        "final profile reports drain before temporary live BE endpoints shut down"
    );
}

fn drain_final_reports_for_submitted_finsts_before_node_shutdown(
    query_id: crate::runtime::query_context::QueryId,
    finst_ids: &[UniqueId],
) {
    assert!(
        crate::service::standalone_exec_state_reporter::wait_for_final_reports_for_finsts_for_test(
            query_id, finst_ids, MAX_WAIT,
        ),
        "every submitted fragment enqueues and drains its final profile report before temporary live BE endpoints shut down"
    );
}

fn query_id_from_live_nodes(
    nodes: &[IndependentGrpcRuntimeFilterNode],
) -> crate::runtime::query_context::QueryId {
    let query_ids = nodes
        .first()
        .expect("live Join owns at least one BE")
        .manager()
        .query_ids_for_test();
    assert_eq!(
        query_ids.len(),
        1,
        "temporary live Join BEs retain exactly one query before shutdown"
    );
    query_ids[0]
}

fn run_live_join(
    topology: JoinTopology,
    runtime_filter: bool,
    backend_count: usize,
) -> LiveJoinRun {
    let _serial = LIVE_JOIN_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let files = LocalJoinFiles::new();
    assert!(backend_count > 0);
    let mut nodes = (0..backend_count)
        .map(|index| {
            IndependentGrpcRuntimeFilterNode::start()
                .unwrap_or_else(|error| panic!("start BE {index}: {error}"))
        })
        .collect::<Vec<_>>();
    let endpoints = nodes.iter().map(|node| node.endpoint()).collect::<Vec<_>>();
    let backends = LiveBackendSnapshot::new(endpoints.iter().copied().enumerate().collect());
    let physical = if backend_count == 1 && topology == JoinTopology::Broadcast && runtime_filter {
        loopback_join_plan(&files)
    } else {
        join_plan(topology, runtime_filter, &files)
    };
    let distributed = crate::sql::planner::distributed::build::build_distributed_plan(&physical)
        .expect("build live Join distributed plan");
    let direct_input_zero = if runtime_filter {
        let consumers = distributed
            .runtime_filter_graph()
            .bindings()
            .filter_map(|binding| match &binding.role {
                RuntimeFilterBindingRole::Consumer(requirement) => Some(requirement.target),
                RuntimeFilterBindingRole::Producer(_) => None,
            })
            .collect::<Vec<_>>();
        !consumers.is_empty()
            && consumers.iter().all(|target| {
                matches!(
                    target,
                    ConsumerBindingTarget::DirectInput { input_ordinal: 0 }
                )
            })
    } else {
        false
    };
    let mut connectors = crate::connector::ConnectorRegistry::new();
    connectors.register_scan_planner(Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    let prepared = crate::coordinator::prepare::prepare_fragments(&distributed, &connectors, None)
        .expect("prepare live Join fragments");
    let bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&distributed, &prepared)
            .expect("encode live Join native bundle");
    let scheduler = Arc::new(FragmentScheduler::from_live_backend_snapshot(
        backends.clone(),
    ));
    let partitioned_distinct_shards = if topology == JoinTopology::Partitioned {
        let preview = scheduler
            .schedule(
                prepared.scheduling_view(),
                UniqueId {
                    hi: 0x52464436,
                    lo: 0x53484152,
                },
            )
            .expect("preview partitioned Join shard placement");
        let build_shards = preview
            .fragment_ids()
            .flat_map(|fragment_id| {
                preview
                    .placements_for_fragment_for_test(fragment_id)
                    .into_iter()
                    .flatten()
            })
            .flat_map(|placement| {
                placement
                    .scan_ranges
                    .values()
                    .flatten()
                    .filter_map(move |range| {
                        let crate::runtime::scan_range::ScanRange::File(file) = &range.range else {
                            return None;
                        };
                        file.full_path
                            .as_ref()
                            .filter(|path| path.contains("/build-"))
                            .map(|path| (placement.backend_idx, path.clone()))
                    })
            })
            .collect::<Vec<_>>();
        build_shards.len() == 3
            && build_shards
                .iter()
                .map(|(backend, _)| *backend)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3
            && build_shards
                .iter()
                .map(|(_, path)| path)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3
    } else {
        false
    };
    let dispatcher: Arc<dyn FragmentDispatcher> = Arc::new(
        RemoteDispatcher::new_with_backend_ids_and_rpc_timeout_for_test(
            backends.entries(),
            MAX_WAIT,
        )
        .expect("create live Join remote dispatcher"),
    );
    let observations = Arc::new(Mutex::new(Vec::new()));
    let grpc_control = Arc::new(
        GrpcRuntimeFilterDeploymentControl::new(backends.entries())
            .expect("create live Join deployment control"),
    );
    let broadcast_loser_gates =
        (topology == JoinTopology::Broadcast && backend_count == 3).then(|| {
            BroadcastLoserGateController::new(
                nodes
                    .iter()
                    .map(|node| Arc::clone(node.manager()))
                    .collect(),
            )
        });
    let control = Arc::new(RecordingDeploymentControl {
        inner: grpc_control,
        observations: Arc::clone(&observations),
        acquire_gate: None,
        broadcast_loser_gates: broadcast_loser_gates.clone(),
    });
    let observer = Arc::new(CountingObserver::default());
    let mut ports = CoordinatorExecutionPorts::new(
        dispatcher,
        RuntimeEndpoint::from_socket_addr(endpoints[0]),
        observer.clone(),
        control,
    );
    ports.runtime_filter_policy_provider =
        Arc::new(NativeRuntimeFilterDeploymentPolicyProvider::new(2));
    let options = QueryOptions {
        query_timeout: Some(5),
        query_delivery_timeout: Some(5),
        runtime_filter_wait_timeout_ms: Some(5_000),
        pipeline_dop: Some(1),
        enable_profile: true,
        ..Default::default()
    };
    let started = Instant::now();
    let outcome = match ExecutionCoordinator::new(prepared, bundle, ports, scheduler, Some(options))
        .execute_with_profiles_for_test()
    {
        Ok(outcome) => outcome,
        Err(error) => {
            let query_id = observations.lock().unwrap().first().map(|observation| {
                crate::runtime::query_context::QueryId {
                    hi: observation.query_id.hi,
                    lo: observation.query_id.lo,
                }
            });
            let diagnostics = query_id.map_or_else(
                || "no runtime-filter install observation".to_string(),
                |query_id| {
                    nodes
                        .iter()
                        .enumerate()
                        .map(|(index, node)| {
                            format!(
                                "BE{index}: counts={:?}, cancelled={}, error={:?}",
                                node.manager().fragment_counts_for_test(query_id),
                                node.manager().is_query_canceled(query_id),
                                node.manager()
                                    .runtime_filter_query_cancellation_error_for_test(query_id)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("; ")
                },
            );
            panic!("execute production-shaped live Join: {error}; {diagnostics}");
        }
    };
    assert!(
        started.elapsed() <= MAX_WAIT,
        "live Join execution exceeded {MAX_WAIT:?}"
    );
    let query_id_from_execution = query_id_from_live_nodes(&nodes);
    let observations = observations.lock().unwrap().clone();
    if let Some(controller) = broadcast_loser_gates.as_ref() {
        controller.join();
    }
    let mut lifecycle_events = Vec::new();
    let mut global_order_events = Vec::new();
    let mut node_evidence = Vec::new();
    if let Some(first) = observations.first() {
        let query_id = crate::runtime::query_context::QueryId {
            hi: first.query_id.hi,
            lo: first.query_id.lo,
        };
        let services = nodes
            .iter()
            .map(|node| {
                node.manager()
                    .runtime_filter_service_for_ingress(query_id)
                    .expect("completed live Join retains query-local service evidence")
            })
            .collect::<Vec<_>>();
        let drain_deadline = Instant::now() + MAX_WAIT;
        while services
            .iter()
            .any(|service| service.transport_pending_len_for_test() != 0)
            && Instant::now() < drain_deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        let query = crate::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
            first.query_id.hi,
            first.query_id.lo,
        );
        let expected_producers = producer_assignments_from_observations(&observations);
        let evidence_deadline = Instant::now() + MAX_WAIT;
        let local_events = loop {
            let local_events = services
                .iter()
                .map(|service| service.lifecycle_events_for_test())
                .collect::<Vec<_>>();
            let flattened = local_events.iter().flatten().cloned().collect::<Vec<_>>();
            let terminal_finsts = producer_terminal_positions(&flattened);
            let all_terminal = expected_producers
                .keys()
                .all(|finst_id| terminal_finsts.contains_key(finst_id));
            let artifact_published = flattened
                .iter()
                .any(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }));
            if all_terminal && artifact_published || Instant::now() >= evidence_deadline {
                break local_events;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        lifecycle_events = local_events.iter().flatten().cloned().collect();
        // Keep the process-global registry only as a supplemental cross-service
        // ordering timeline. Node-local evidence below comes directly from each
        // captured query-owned RuntimeFilterService.
        global_order_events =
            crate::runtime::runtime_filter_observability::RuntimeFilterLifecycleRegistry::global()
                .snapshot(query)
                .map(|snapshot| snapshot.channel_events.into_values().flatten().collect())
                .unwrap_or_default();
        for (backend_idx, service) in services.into_iter().enumerate() {
            let participant =
                crate::runtime_filter::deployment::participant_id_for_backend(backend_idx)
                    .expect("valid live Join backend participant");
            let install = service
                .installed_participant_install_for_test()
                .expect("completed live Join retains participant install evidence");
            assert_eq!(install.local_participant_id(), participant);
            node_evidence.push(NodeEvidence {
                participant,
                pending_transport: service.transport_pending_len_for_test(),
                admitted_envelopes: service
                    .admitted_transport_envelopes_for_test()
                    .into_iter()
                    .map(|(route, envelope)| (route.target_role(), envelope.kind()))
                    .collect(),
                lifecycle_events: local_events[backend_idx].clone(),
            });
        }
    }
    drain_final_reports_before_node_shutdown(query_id_from_execution);
    for node in &mut nodes {
        node.shutdown().expect("shutdown live Join BE");
    }
    LiveJoinRun {
        outcome,
        observations,
        lifecycle_events,
        global_order_events,
        node_evidence,
        scheduled_fragments: observer.0.load(Ordering::SeqCst),
        backend_count,
        direct_input_zero,
        broadcast_losers_entered: broadcast_loser_gates
            .as_ref()
            .is_some_and(|controller| controller.losers_entered.load(Ordering::SeqCst)),
        broadcast_losers_released_after_delivery: broadcast_loser_gates
            .as_ref()
            .is_some_and(|controller| controller.released_after_delivery.load(Ordering::SeqCst)),
        partitioned_distinct_shards,
    }
}

fn run_live_join_cancel() -> CancelRun {
    let _serial = LIVE_JOIN_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let files = LocalJoinFiles::new();
    let mut nodes = [
        IndependentGrpcRuntimeFilterNode::start().expect("start cancel BE zero"),
        IndependentGrpcRuntimeFilterNode::start().expect("start cancel BE one"),
        IndependentGrpcRuntimeFilterNode::start().expect("start cancel BE two"),
    ];
    let endpoints = [
        nodes[0].endpoint(),
        nodes[1].endpoint(),
        nodes[2].endpoint(),
    ];
    let backends = LiveBackendSnapshot::new(endpoints.into_iter().enumerate().collect());
    let physical = cancel_join_plan(&files);
    let distributed = crate::sql::planner::distributed::build::build_distributed_plan(&physical)
        .expect("build cancellable live Join distributed plan");
    let mut connectors = crate::connector::ConnectorRegistry::new();
    connectors.register_scan_planner(Arc::new(
        crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
    ));
    let prepared = crate::coordinator::prepare::prepare_fragments(&distributed, &connectors, None)
        .expect("prepare cancellable live Join fragments");
    let bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&distributed, &prepared)
            .expect("encode cancellable live Join bundle");
    let scheduler = Arc::new(FragmentScheduler::from_live_backend_snapshot(
        backends.clone(),
    ));
    let remote = Arc::new(
        RemoteDispatcher::new_with_backend_ids_and_rpc_timeout_for_test(
            backends.entries(),
            MAX_WAIT,
        )
        .expect("create cancellable live Join dispatcher"),
    );
    let acquire_gate = Arc::new(Mutex::new(None));
    let dispatcher = Arc::new(CancelAfterJoinDispatcher::new(
        remote,
        nodes
            .iter()
            .map(|node| Arc::clone(node.manager()))
            .collect(),
        Arc::clone(&acquire_gate),
    ));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let control = Arc::new(RecordingDeploymentControl {
        inner: Arc::new(
            GrpcRuntimeFilterDeploymentControl::new(backends.entries())
                .expect("create cancellable live Join deployment control"),
        ),
        observations: Arc::clone(&observations),
        acquire_gate: Some(Arc::clone(&acquire_gate)),
        broadcast_loser_gates: None,
    });
    let mut ports = CoordinatorExecutionPorts::new(
        dispatcher.clone(),
        RuntimeEndpoint::from_socket_addr(endpoints[0]),
        Arc::new(CountingObserver::default()),
        control,
    );
    ports.runtime_filter_policy_provider =
        Arc::new(NativeRuntimeFilterDeploymentPolicyProvider::new(2));
    let options = QueryOptions {
        query_timeout: Some(4),
        query_delivery_timeout: Some(5),
        runtime_filter_wait_timeout_ms: Some(5_000),
        pipeline_dop: Some(1),
        enable_profile: true,
        ..Default::default()
    };
    let started = Instant::now();
    let error = ExecutionCoordinator::new(prepared, bundle, ports, scheduler, Some(options))
        .execute_with_profiles_for_test()
        .expect_err("live Join cancellation must interrupt execution");
    assert!(
        started.elapsed() <= MAX_WAIT,
        "cancelled live Join exceeded {MAX_WAIT:?}"
    );
    assert!(dispatcher.triggered.load(Ordering::SeqCst));
    let observation = observations
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("cancellable live Join installs before submission");
    let query_id = crate::runtime::query_context::QueryId {
        hi: observation.query_id.hi,
        lo: observation.query_id.lo,
    };
    let cancel_deadline = Instant::now() + MAX_WAIT;
    while nodes.iter().any(|node| {
        node.manager()
            .runtime_filter_query_cancellation_error_for_test(query_id)
            .is_none()
    }) && Instant::now() < cancel_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let manager_cancel_errors = nodes
        .iter()
        .map(|node| {
            node.manager()
                .runtime_filter_query_cancellation_error_for_test(query_id)
        })
        .collect::<Vec<_>>();
    let services = dispatcher.captured_services.lock().unwrap().clone();
    assert_eq!(
        services.len(),
        3,
        "cancel gate captures three node-local services"
    );
    let drain_deadline = Instant::now() + MAX_WAIT;
    while services
        .iter()
        .any(|service| service.transport_pending_len_for_test() != 0)
        && Instant::now() < drain_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let submitted = dispatcher.submitted.lock().unwrap().clone();
    let producer_handles_remaining = services
        .iter()
        .map(|service| {
            submitted
                .values()
                .flatten()
                .filter(|finst| {
                    service.producer_handle_is_live_for_test(BindingId::new(1), **finst)
                })
                .count()
        })
        .sum();
    let pending_transport = services
        .iter()
        .map(|service| service.transport_pending_len_for_test())
        .collect();
    let expected_producers = producer_assignments_from_observations(&observations.lock().unwrap());
    let evidence_deadline = Instant::now() + MAX_WAIT;
    let participant_events = loop {
        let events = services
            .iter()
            .enumerate()
            .map(|(backend_idx, service)| {
                let participant =
                    crate::runtime_filter::deployment::participant_id_for_backend(backend_idx)
                        .expect("valid cancel backend participant");
                (participant, service.lifecycle_events_for_test())
            })
            .collect::<BTreeMap<_, _>>();
        let flattened = events.values().flatten().cloned().collect::<Vec<_>>();
        let terminals = producer_terminal_positions(&flattened);
        if expected_producers
            .keys()
            .all(|finst_id| terminals.contains_key(finst_id))
            || Instant::now() >= evidence_deadline
        {
            break events;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let lifecycle_events = participant_events.values().flatten().cloned().collect();
    let submitted_finsts = submitted.values().flatten().copied().collect::<Vec<_>>();
    drain_final_reports_for_submitted_finsts_before_node_shutdown(query_id, &submitted_finsts);
    for node in &mut nodes {
        node.shutdown().expect("shutdown cancelled live Join BE");
    }
    CancelRun {
        error,
        triggered: dispatcher.triggered.load(Ordering::SeqCst),
        manager_cancel_errors,
        pending_transport,
        producer_handles_remaining,
        lifecycle_events,
        expected_producers,
        participant_events,
        wrong_fetch_rejected: dispatcher.wrong_fetch_rejected.load(Ordering::SeqCst),
        wrong_cancel_rejected: dispatcher.wrong_cancel_rejected.load(Ordering::SeqCst),
    }
}

fn result_count(outcome: &CoordinatedQueryResult) -> i64 {
    let chunks = &outcome.query_result.chunks;
    assert_eq!(chunks.len(), 1, "COUNT returns one chunk");
    let values = chunks[0]
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.len(), 1, "COUNT returns one row");
    values.value(0)
}

fn counter_in_subtree(node: &ProfileNode, name: &str) -> i64 {
    node.counters
        .iter()
        .filter(|counter| counter.name == name)
        .map(|counter| counter.value)
        .sum::<i64>()
        + node
            .children
            .iter()
            .map(|child| counter_in_subtree(child, name))
            .sum::<i64>()
}

fn runtime_filter_rows(profiles: &[RuntimeProfileTree]) -> (i64, i64, usize) {
    fn visit(node: &ProfileNode, totals: &mut (i64, i64, usize)) {
        if node.name.starts_with("NativeRuntimeFilter (id=") {
            totals.0 += counter_in_subtree(node, "PushRowNum");
            totals.1 += counter_in_subtree(node, "PullRowNum");
            totals.2 += 1;
            return;
        }
        for child in &node.children {
            visit(child, totals);
        }
    }
    let mut totals = (0, 0, 0);
    for profile in profiles {
        visit(&profile.root, &mut totals);
    }
    totals
}

fn producer_assignments(run: &LiveJoinRun) -> BTreeMap<UniqueId, RuntimeFilterParticipantId> {
    producer_assignments_from_observations(&run.observations)
}

fn producer_assignments_from_observations(
    observations: &[InstallObservation],
) -> BTreeMap<UniqueId, RuntimeFilterParticipantId> {
    let mut assignments = BTreeMap::new();
    for observation in observations {
        for channel in observation.install.routing_shard().channels().values() {
            for ((_, finst_id), participant) in channel.producer_instances() {
                if let Some(previous) = assignments.insert(*finst_id, *participant) {
                    assert_eq!(previous, *participant, "producer ownership is stable");
                }
            }
        }
    }
    assignments
}

fn producer_terminal_positions(events: &[RuntimeFilterEvent]) -> BTreeMap<UniqueId, usize> {
    let mut positions = BTreeMap::new();
    for (position, event) in events.iter().enumerate() {
        let identity = match event {
            RuntimeFilterEvent::ProducerInstanceClosed { identity }
            | RuntimeFilterEvent::ProducerInstanceFailed { identity, .. } => identity,
            _ => continue,
        };
        assert!(
            positions
                .insert(identity.fragment_instance_id(), position)
                .is_none(),
            "a producer instance has exactly one terminal event"
        );
    }
    positions
}

fn assert_participant_local_producer_terminals(run: &LiveJoinRun) {
    for (finst_id, participant) in producer_assignments(run) {
        let evidence = run
            .node_evidence
            .iter()
            .find(|evidence| evidence.participant == participant)
            .expect("producer participant has node-local evidence");
        assert!(
            evidence.lifecycle_events.iter().any(|event| matches!(
                event,
                RuntimeFilterEvent::ProducerInstanceClosed { identity }
                    | RuntimeFilterEvent::ProducerInstanceFailed { identity, .. }
                    if identity.fragment_instance_id() == finst_id
                        && identity.common().participant_id() == participant
            )),
            "participant {} owns a typed terminal event for producer {finst_id}",
            participant.get()
        );
    }
}

fn assert_partitioned_participant_evidence(run: &LiveJoinRun) {
    let aggregator = run
        .observations
        .iter()
        .find(|observation| {
            observation
                .install
                .routing_shard()
                .channels()
                .values()
                .any(|channel| {
                    channel
                        .local_roles()
                        .contains(&RuntimeFilterRouteRole::Aggregator)
                })
        })
        .map(|observation| observation.install.local_participant_id())
        .expect("AllOf deployment has an aggregate owner");
    let aggregate_evidence = run
        .node_evidence
        .iter()
        .find(|evidence| evidence.participant == aggregator)
        .expect("aggregate owner has node-local evidence");

    for finst_id in producer_assignments(run).keys() {
        assert!(
            aggregate_evidence
                .lifecycle_events
                .iter()
                .any(|event| matches!(
                    event,
                    RuntimeFilterEvent::ProducerInstanceClosed { identity }
                        | RuntimeFilterEvent::ProducerInstanceFailed { identity, .. }
                        if identity.fragment_instance_id() == *finst_id
                            && identity.common().participant_id() == aggregator
                )),
            "aggregate participant {} terminalizes producer {finst_id}",
            aggregator.get()
        );
    }

    for participant in producer_assignments(run)
        .into_values()
        .collect::<std::collections::BTreeSet<_>>()
    {
        let observation = run
            .observations
            .iter()
            .find(|observation| observation.install.local_participant_id() == participant)
            .expect("producer participant has a local install");
        let producer_routes = observation
            .install
            .routing_shard()
            .channels()
            .values()
            .flat_map(|channel| channel.outbound_edges())
            .filter(|edge| {
                matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
                    && edge.target().role() == RuntimeFilterRouteRole::Aggregator
            })
            .map(|edge| edge.route_edge_id())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            !producer_routes.is_empty(),
            "participant {} owns a Producer-to-Aggregator route",
            participant.get()
        );
        if participant == aggregator {
            continue;
        }
        let evidence = run
            .node_evidence
            .iter()
            .find(|evidence| evidence.participant == participant)
            .expect("producer participant has node-local evidence");
        assert!(
            evidence.lifecycle_events.iter().any(|event| matches!(
                event,
                RuntimeFilterEvent::TransportEnvelope {
                    identity,
                    kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                    ..
                } if producer_routes.contains(&identity.route_edge_id())
                    && identity.common().participant_id() == participant
            )),
            "participant {} receives an accepted ACK for its Producer-to-Aggregator route",
            participant.get()
        );
    }
}

fn assert_active_pruning(run: &LiveJoinRun, expected_count: i64) {
    assert_eq!(result_count(&run.outcome), expected_count);
    let (input_rows, output_rows, operators) = runtime_filter_rows(&run.outcome.fragment_profiles);
    assert!(
        operators > 0,
        "RF-on execution has native consumer operators"
    );
    assert!(input_rows > 0, "RF-on consumer sees nonzero probe rows");
    assert!(
        output_rows < input_rows,
        "RF-on consumer must prune rows: input={input_rows} output={output_rows}"
    );
    assert_eq!(
        run.observations.len(),
        run.backend_count,
        "every independent BE installs"
    );
    assert_eq!(
        run.node_evidence.len(),
        run.backend_count,
        "every node-local service retains evidence"
    );
    assert!(
        run.node_evidence
            .iter()
            .all(|evidence| evidence.pending_transport == 0),
        "every node-local reliable transport drains before completion"
    );
    assert!(
        run.scheduled_fragments >= run.backend_count,
        "real native fragments execute on every backend"
    );
}

#[test]
fn native_join_loopback_operator_applies_blocking_subscription() {
    let run = run_live_join(JoinTopology::Broadcast, true, 1);
    assert_active_pruning(&run, 2);
    assert!(
        run.direct_input_zero,
        "consumer wraps HashJoin DirectInput(0)"
    );
    assert!(run.observations.iter().any(|observation| {
        observation
            .install
            .routing_shard()
            .channels()
            .values()
            .flat_map(|channel| channel.outbound_edges())
            .any(|edge| {
                matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
                    && matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_))
                    && matches!(edge.peer(), RuntimeFilterRoutePeer::Loopback)
            })
    }));
    assert!(
        run.lifecycle_events
            .iter()
            .any(|event| matches!(event, RuntimeFilterEvent::LoopbackDelivered { .. })),
        "one-BE execution delivers the artifact through the loopback route"
    );
    assert!(
        run.lifecycle_events
            .iter()
            .any(|event| matches!(event, RuntimeFilterEvent::SubscriptionAcquired { .. }))
    );
}

#[test]
fn live_three_be_broadcast_join_anyof_direct_executes_and_applies() {
    let run = run_live_join(JoinTopology::Broadcast, true, 3);
    assert_active_pruning(&run, 2);
    assert!(
        run.broadcast_losers_entered,
        "both losing replicas reach their deterministic close gates"
    );
    assert!(
        run.broadcast_losers_released_after_delivery,
        "losing replicas release only after the winner publishes and delivers"
    );
    assert!(run.observations.iter().all(|observation| {
        observation
            .install
            .core_view()
            .channels()
            .values()
            .all(|channel| matches!(channel.availability_coverage(), Coverage::AnyOf(_)))
    }));
    assert!(run.observations.iter().all(|observation| {
        observation
            .install
            .routing_shard()
            .channels()
            .values()
            .all(|channel| {
                !channel
                    .local_roles()
                    .contains(&RuntimeFilterRouteRole::Aggregator)
            })
    }));
    assert!(
        run.observations.iter().any(|observation| {
            observation
                .install
                .routing_shard()
                .channels()
                .values()
                .flat_map(|channel| channel.outbound_edges())
                .any(|edge| {
                    matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
                        && matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_))
                        && matches!(edge.peer(), RuntimeFilterRoutePeer::Remote { .. })
                })
        }),
        "broadcast installs a remote ReplicaDirect producer-to-consumer edge"
    );
    assert!(
        run.node_evidence.iter().any(|evidence| {
            evidence.admitted_envelopes.iter().any(|(target, kind)| {
                matches!(target, RuntimeFilterRouteRole::Consumer(_))
                    && matches!(
                        kind,
                        RuntimeFilterEnvelopeKind::Artifact
                            | RuntimeFilterEnvelopeKind::FinalArtifact
                    )
            })
        }),
        "AnyOf delivers a real artifact directly to a consumer replica"
    );
    assert!(
        run.lifecycle_events.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::TransportEnvelope {
                kind: TransportEventKind::Acked(RuntimeFilterAcceptStatus::Accepted),
                ..
            }
        )),
        "AnyOf direct delivery receives an accepted ACK"
    );
    let assignments = producer_assignments(&run);
    assert_eq!(
        assignments.len(),
        3,
        "broadcast schedules three distinct producers"
    );
    let terminals = producer_terminal_positions(&run.lifecycle_events);
    assert_eq!(terminals.len(), assignments.len());
    assert!(
        run.lifecycle_events
            .iter()
            .any(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. })),
        "the ungated winner publishes while both losing close paths are gated"
    );
    assert_participant_local_producer_terminals(&run);
}

#[test]
fn live_three_be_partitioned_join_allof_aggregate_executes_and_applies() {
    let run = run_live_join(JoinTopology::Partitioned, true, 3);
    assert_active_pruning(&run, 3);
    assert!(
        run.partitioned_distinct_shards,
        "the three scheduled build inputs own three distinct Parquet shards"
    );
    assert!(run.observations.iter().all(|observation| {
        observation
            .install
            .core_view()
            .channels()
            .values()
            .all(|channel| matches!(channel.availability_coverage(), Coverage::AllOf(_)))
    }));
    assert_eq!(
        run.observations
            .iter()
            .filter(
                |observation| observation.install.routing_shard().channels().values().any(
                    |channel| {
                        channel
                            .local_roles()
                            .contains(&RuntimeFilterRouteRole::Aggregator)
                    }
                )
            )
            .count(),
        1,
        "AllOf owns one aggregator"
    );
    assert_eq!(
        run.lifecycle_events
            .iter()
            .filter(|event| matches!(event, RuntimeFilterEvent::ProducerInstanceClosed { .. }))
            .count(),
        3,
        "AllOf closes every scheduled producer instance before publication"
    );
    assert!(
        run.node_evidence.iter().any(|evidence| {
            evidence.admitted_envelopes.iter().any(|(target, kind)| {
                *target == RuntimeFilterRouteRole::Aggregator
                    && matches!(
                        kind,
                        RuntimeFilterEnvelopeKind::Contribution
                            | RuntimeFilterEnvelopeKind::ProducerClosed
                    )
            })
        }),
        "AllOf sends producer traffic to the aggregate owner"
    );
    assert!(
        run.node_evidence.iter().any(|evidence| {
            evidence.admitted_envelopes.iter().any(|(target, kind)| {
                matches!(target, RuntimeFilterRouteRole::Consumer(_))
                    && matches!(
                        kind,
                        RuntimeFilterEnvelopeKind::Artifact
                            | RuntimeFilterEnvelopeKind::FinalArtifact
                    )
            })
        }),
        "AllOf aggregate owner publishes an artifact to consumers"
    );
    let assignments = producer_assignments(&run);
    assert_eq!(
        assignments.len(),
        3,
        "AllOf schedules three producer shards"
    );
    assert_eq!(
        assignments
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3,
        "AllOf producer shards belong to three distinct participants"
    );
    let terminals = producer_terminal_positions(&run.lifecycle_events);
    assert_eq!(
        terminals
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        assignments
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
    );
    let global_terminals = producer_terminal_positions(&run.global_order_events);
    let last_terminal = *global_terminals.values().max().unwrap();
    let first_publish = run
        .global_order_events
        .iter()
        .position(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
        .expect("AllOf publishes after complete aggregation");
    assert!(
        last_terminal < first_publish,
        "AllOf cannot publish until all three producer shards close: terminals={terminals:?} events={:?}",
        run.global_order_events
    );
    assert_partitioned_participant_evidence(&run);
}

#[test]
fn native_join_cancel_fails_open_and_closes_producer_streams() {
    let run = run_live_join_cancel();
    assert!(
        run.triggered,
        "cancel is issued only after the real consumer acquire wait is entered"
    );
    assert!(run.wrong_fetch_rejected, "wrong-node fetch is rejected");
    assert!(run.wrong_cancel_rejected, "wrong-node cancel is rejected");
    assert!(
        run.error
            .contains("injected cancellation after live Join producer and consumer became active"),
        "coordinator surfaces the deliberate cancellation boundary: {}",
        run.error
    );
    assert!(
        run.manager_cancel_errors.iter().all(|error| {
            error
                .as_deref()
                .is_some_and(|error| error.starts_with("query canceled by FE: finst="))
        }),
        "the real cancel RPC reaches every independent node manager: {:?}",
        run.manager_cancel_errors
    );
    assert_eq!(
        run.producer_handles_remaining, 0,
        "cancel closes every producer handle"
    );
    assert_eq!(
        run.pending_transport,
        vec![0, 0, 0],
        "cancel drains all transports"
    );
    assert!(
        run.lifecycle_events.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::ProducerInstanceFailed { .. }
                | RuntimeFilterEvent::ChannelCancelled { .. }
        )),
        "cancel terminalizes producer/channel lifecycle"
    );
    assert!(
        run.lifecycle_events.iter().any(|event| matches!(
            event,
            RuntimeFilterEvent::SubscriptionCancelled { .. }
                | RuntimeFilterEvent::SubscriptionUnavailable { .. }
        )),
        "cancel permanently releases blocking consumers into fail-open passthrough"
    );
    assert_eq!(
        run.expected_producers.len(),
        3,
        "cancel fixture schedules three producer instances"
    );
    let terminal_finsts = producer_terminal_positions(&run.lifecycle_events)
        .into_keys()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        terminal_finsts,
        run.expected_producers
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "every expected producer has a closed/failed terminal and no unknown producer appears"
    );
    for (finst_id, participant) in &run.expected_producers {
        assert!(
            run.participant_events
                .get(participant)
                .expect("cancel participant has local typed evidence")
                .iter()
                .any(|event| matches!(
                    event,
                    RuntimeFilterEvent::ProducerInstanceClosed { identity }
                        | RuntimeFilterEvent::ProducerInstanceFailed { identity, .. }
                        if identity.fragment_instance_id() == *finst_id
                            && identity.common().participant_id() == *participant
                )),
            "participant {} terminalizes owned producer {finst_id}",
            participant.get()
        );
    }
}

#[test]
fn native_join_rf_on_off_fingerprints_match() {
    let on = run_live_join(JoinTopology::Partitioned, true, 3);
    let off = run_live_join(JoinTopology::Partitioned, false, 3);
    assert_eq!(result_count(&on.outcome), result_count(&off.outcome));
    assert_active_pruning(&on, 3);
    let (_, _, off_operators) = runtime_filter_rows(&off.outcome.fragment_profiles);
    assert_eq!(off_operators, 0, "RF-off has no native consumer operator");
    assert!(off.observations.is_empty(), "RF-off installs no deployment");
}
