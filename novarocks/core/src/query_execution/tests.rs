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

use crate::query_execution::artifact::{
    BackendPlacement, ConnectorBindingInstallBarrier, ConnectorBindingInstallLease,
    FragmentScheduleDraft, ValidatedFragmentSchedule,
};
use crate::query_execution::backend::BackendTopologySnapshot;
use crate::query_execution::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use crate::query_execution::contract::{
    DistributedQueryCoordinator, DistributedQueryError, DistributedQueryErrorKind,
    DistributedQueryIntent, DistributedQueryOutcome, DistributedQueryRequest,
    build_distributed_query_request_with_execution,
};
use crate::query_execution::lifecycle::{AttemptId, ParticipantRole, QueryExecutionId};
use crate::query_execution::lifecycle::{
    QueryInitBarrier, QueryInitOptions, QueryInitPlan, QueryLaunchBarrier, QueryLifecycleLease,
    QueryLifecycleLeaseGuard,
};
use crate::query_execution::outcome::QueryOutcomeFactory;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::statistics::{
    StatisticsExecutionMode, StatisticsExecutionPolicy, ThetaSketchPartial,
};
use crate::query_execution::write::{WriteAbortInput, WriteCommitInput};
use crate::runtime::query_options::QueryOptions;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, PlanFragment,
};
use crate::sql::planner::payload::PlanValuesNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn test_execution(cancellation: QueryCancellationView) -> QueryExecutionContext {
    QueryExecutionContext::new(
        crate::common::app_config::ClusterRole::AllInOne,
        BackendTopologySnapshot::empty(0),
        None,
        cancellation,
        crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    )
}

struct RecordingQueryLifecycleGuard {
    finalizes: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
    armed: bool,
}

impl QueryLifecycleLeaseGuard for RecordingQueryLifecycleGuard {
    fn finalize(
        mut self: Box<Self>,
    ) -> Result<crate::query_execution::lifecycle::QueryTerminalSet, DistributedQueryError> {
        self.armed = false;
        self.finalizes.fetch_add(1, Ordering::SeqCst);
        Ok(
            crate::query_execution::lifecycle::QueryTerminalSet::new(Vec::new())
                .expect("an empty test terminal set is valid"),
        )
    }

    fn abort_preserving(
        mut self: Box<Self>,
        primary_error: String,
    ) -> crate::query_execution::lifecycle::QueryLifecycleAbortOutcome {
        self.armed = false;
        self.aborts.fetch_add(1, Ordering::SeqCst);
        crate::query_execution::lifecycle::QueryLifecycleAbortOutcome::new(
            format!("{primary_error}; query lifecycle rollback completed"),
            None,
        )
    }
}

struct NoopConnectorBindingBarrier;

impl ConnectorBindingInstallBarrier for NoopConnectorBindingBarrier {
    fn install_all(
        &self,
        _execution_id: crate::query_execution::lifecycle::QueryExecutionId,
        _plan: crate::query_execution::artifact::ConnectorBindingInstallPlan,
    ) -> Result<ConnectorBindingInstallLease, DistributedQueryError> {
        Ok(ConnectorBindingInstallLease)
    }
}

impl Drop for RecordingQueryLifecycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.aborts.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct RecordingQueryInitBarrier {
    calls: Arc<AtomicUsize>,
    participants: Arc<AtomicUsize>,
    finalizes: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
}

struct CapturingQueryInitBarrier {
    plan: Arc<Mutex<Option<QueryInitPlan>>>,
    finalizes: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
}

impl QueryInitBarrier for CapturingQueryInitBarrier {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError> {
        *self.plan.lock().expect("capture plan") = Some(plan);
        Ok(QueryLifecycleLease::new(Box::new(
            RecordingQueryLifecycleGuard {
                finalizes: self.finalizes.clone(),
                aborts: self.aborts.clone(),
                armed: true,
            },
        )))
    }
}

impl QueryInitBarrier for RecordingQueryInitBarrier {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.participants
            .store(plan.participant_count(), Ordering::SeqCst);
        Ok(QueryLifecycleLease::new(Box::new(
            RecordingQueryLifecycleGuard {
                finalizes: self.finalizes.clone(),
                aborts: self.aborts.clone(),
                armed: true,
            },
        )))
    }
}

struct RecordingQueryLaunchBarrier;

impl QueryLaunchBarrier for RecordingQueryLaunchBarrier {
    fn stage_all(
        &self,
        _batches: &[crate::query_execution::lifecycle::StageBatch],
    ) -> Result<(), DistributedQueryError> {
        Ok(())
    }

    fn start_all(
        &self,
        _batches: &[crate::query_execution::lifecycle::StageBatch],
    ) -> Result<(), DistributedQueryError> {
        Ok(())
    }
}

fn real_execution_artifacts() -> (
    crate::query_execution::preparation::PreparedFragmentSet,
    crate::protocol::native::encode::NativeFragmentBundle,
) {
    let fragment = PlanFragment {
        fragment_id: 7,
        root: DistributedNode {
            node_id: 70,
            fragment_id: 7,
            tuple_ids: vec![70],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: PhysicalPlanStats {
                output_row_count: 0.0,
                row_count_confidence: PlannerConfidence::Fallback,
                column_statistics: Default::default(),
                cost_estimate: None,
                broadcast_decision: None,
            },
            payload: DistributedNodeKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
        },
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: Vec::new(),
        cte_id: None,
        cte_exchange_nodes: Vec::new(),
    };
    let plan = crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![fragment],
        root_fragment_id: 7,
        edges: Vec::new(),
        runtime_filter_graph: Default::default(),
    };
    let registry = crate::connector::ConnectorRegistry::new();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &plan,
        &registry,
        &controls,
        &crate::connector::test_request_context(),
        None,
        crate::query_execution::preparation::ScanPreparationOptions::default(),
    )
    .expect("prepare production execution artifact");
    let native_bundle =
        crate::protocol::native::encode::encode_native_fragment_bundle(&plan, &prepared)
            .expect("encode production execution artifact");
    (prepared, native_bundle)
}

fn execution_id(query_id: crate::query_execution::contract::QueryId) -> QueryExecutionId {
    QueryExecutionId::new(query_id, AttemptId::new(9).expect("nonzero attempt"))
        .expect("valid execution id")
}

#[test]
fn request_owns_prepared_and_native_artifacts() {
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        Some(QueryOptions {
            pipeline_dop: Some(3),
            ..Default::default()
        }),
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("valid production artifacts form an owned request");

    assert_eq!(
        request
            .artifacts()
            .scheduling_view()
            .fragment_ids()
            .collect::<Vec<_>>(),
        [7]
    );
    assert_eq!(
        request.options().native_submission_options().pipeline_dop(),
        3
    );
    let parts = request.into_parts();
    let cancellation = parts.cancellation;
    let completion = parts.completion;
    assert!(!cancellation.is_cancelled());
    assert_eq!(completion.intent(), DistributedQueryIntent::Result);
}

#[test]
fn query_control_typestate_initializes_before_native_assembly() {
    let calls = Arc::new(AtomicUsize::new(0));
    let participants = Arc::new(AtomicUsize::new(0));
    let finalizes = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let barrier = RecordingQueryInitBarrier {
        calls: calls.clone(),
        participants: participants.clone(),
        finalizes: finalizes.clone(),
        aborts: aborts.clone(),
    };
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("build request");
    let parts = request.into_parts();
    let query_id = crate::query_execution::contract::QueryId::new(41, 73);
    let execution_id = execution_id(query_id);
    let endpoint = "127.0.0.1:19031".parse().expect("valid endpoint");
    let mut draft = FragmentScheduleDraft::new();
    draft
        .freeze_live_backends(vec![
            crate::query_execution::backend::LiveBackendTarget::new(3, endpoint, 11),
        ])
        .expect("freeze live topology");
    draft
        .assign_fragment(7, vec![BackendPlacement::new(3, endpoint)])
        .expect("assign fragment");
    let schedule =
        ValidatedFragmentSchedule::validate(parts.artifacts.scheduling_view(), execution_id, draft)
            .expect("validate schedule");
    let options = QueryInitOptions::new(
        execution_id,
        vec![crate::query_execution::backend::LiveBackendTarget::new(
            3, endpoint, 11,
        )],
        2,
        parts.options.runtime_filter_lifecycle(),
        &parts.options,
        1_000,
        std::time::Duration::from_secs(30),
        crate::query_execution::backend::CoordinatorReportEndpoint::from_socket_addr(
            "127.0.0.1:19030".parse().expect("valid report endpoint"),
        ),
    )
    .expect("valid init options");

    let execution = parts
        .artifacts
        .bind_schedule(schedule)
        .expect("bind schedule")
        .initialize_query(options, &barrier)
        .expect("initialize query")
        .prepare_connector_bindings(&NoopConnectorBindingBarrier)
        .expect("install empty connector bindings")
        .prepare_stage()
        .expect("prepare exact stage batches")
        .stage(&RecordingQueryLaunchBarrier)
        .expect("stage after control ready")
        .start(&RecordingQueryLaunchBarrier)
        .expect("start after all participants stage")
        .into_parts();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(participants.load(Ordering::SeqCst), 1);
    assert_eq!(execution.root_fetch.fragment_id(), 7);
    assert_eq!(aborts.load(Ordering::SeqCst), 0);
    execution
        .query_lifecycle_lease
        .finalize()
        .expect("finalize lifecycle");
    execution.connector_binding_lease.release();
    assert_eq!(finalizes.load(Ordering::SeqCst), 1);
}

#[test]
#[cfg(any())]
fn retired_runtime_filter_contribution_compiler_binds_outer_attempt_into_real_participant_manifests()
 {
    let fixture =
        crate::query_execution::contract_test_support::non_empty_runtime_filter_contract_fixture();
    let live_backends = fixture
        .backends()
        .iter()
        .enumerate()
        .map(|(ordinal, (backend_idx, endpoint))| {
            crate::query_execution::backend::LiveBackendTarget::new(
                *backend_idx,
                *endpoint,
                100 + ordinal as u64,
            )
        })
        .collect::<Vec<_>>();
    let request = fixture.into_request();
    let parts = request.into_parts();
    let execution_id = QueryExecutionId::new(
        crate::query_execution::contract::QueryId::new(41, 73),
        AttemptId::new(17).expect("nonzero attempt"),
    )
    .expect("valid execution id");
    let mut draft = FragmentScheduleDraft::new();
    draft
        .freeze_live_backends(live_backends.clone())
        .expect("freeze live topology");
    draft
        .assign_fragment(
            11,
            vec![BackendPlacement::new(
                live_backends[0].backend_idx(),
                live_backends[0].endpoint(),
            )],
        )
        .expect("schedule producer fragment");
    draft
        .assign_fragment(
            19,
            vec![BackendPlacement::new(
                live_backends[0].backend_idx(),
                live_backends[0].endpoint(),
            )],
        )
        .expect("schedule consumer fragment");
    let schedule =
        ValidatedFragmentSchedule::validate(parts.artifacts.scheduling_view(), execution_id, draft)
            .expect("validate schedule");
    assert_eq!(
        schedule
            .lifecycle_projection()
            .instances_by_backend
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![3],
        "all scheduled fragments intentionally use a strict subset of the live backends"
    );
    let options = QueryInitOptions::new(
        execution_id,
        live_backends,
        2,
        parts.options.runtime_filter_lifecycle(),
        &parts.options,
        5_000,
        std::time::Duration::from_secs(30),
        crate::query_execution::backend::CoordinatorReportEndpoint::from_socket_addr(
            "127.0.0.1:19030".parse().expect("valid report endpoint"),
        ),
    )
    .expect("valid init options");
    let captured = Arc::new(Mutex::new(None));
    let barrier = CapturingQueryInitBarrier {
        plan: captured.clone(),
        finalizes: Arc::new(AtomicUsize::new(0)),
        aborts: Arc::new(AtomicUsize::new(0)),
    };

    let ready = parts
        .artifacts
        .bind_schedule(schedule)
        .expect("bind schedule")
        .initialize_query(options, &barrier)
        .expect("compile real RF contributions and initialize");
    {
        let captured = captured.lock().expect("read captured plan");
        let plan = captured.as_ref().expect("captured init plan");
        assert_eq!(plan.backend_ids(), vec![3, 8]);
        let scheduled = plan.participant(3).expect("scheduled RF participant");
        assert_eq!(
            scheduled.manifest().roles(),
            &std::collections::BTreeSet::from([
                ParticipantRole::FragmentExecutor,
                ParticipantRole::RuntimeFilterService,
            ])
        );
        assert!(
            !scheduled
                .manifest()
                .expected_fragment_instance_ids()
                .is_empty()
        );

        let service_only = plan
            .participant(8)
            .expect("compiler-added service-only participant");
        assert_eq!(
            service_only.manifest().roles(),
            &std::collections::BTreeSet::from([ParticipantRole::RuntimeFilterService])
        );
        assert_eq!(
            service_only.manifest().expected_fragment_instance_ids(),
            &std::collections::BTreeSet::new()
        );

        for participant in [scheduled, service_only] {
            let contribution = participant
                .manifest()
                .runtime_filter()
                .expect("real compiled RF contribution");
            assert_eq!(contribution.install().epoch().get(), 17);
            assert_ne!(contribution.digest(), &[0; 32]);
            assert_eq!(participant.digest(), participant.manifest().digest());
        }
        assert_ne!(scheduled.digest(), service_only.digest());
    }
    drop(ready);
}

#[test]
#[cfg(any())]
fn retired_exchange_route_projection_binds_exact_instances_and_canonical_sender_order() {
    let fixture =
        crate::query_execution::contract_test_support::non_empty_result_contract_fixture();
    let live_backends = fixture
        .backends()
        .iter()
        .enumerate()
        .map(|(ordinal, (backend_idx, endpoint))| {
            crate::query_execution::backend::LiveBackendTarget::new(
                *backend_idx,
                *endpoint,
                100 + ordinal as u64,
            )
        })
        .collect::<Vec<_>>();
    let request = fixture.into_request();
    let parts = request.into_parts();
    let mut draft = FragmentScheduleDraft::new();
    draft
        .freeze_live_backends(live_backends.clone())
        .expect("freeze live topology");
    draft
        .assign_fragment(
            11,
            live_backends
                .iter()
                .map(|target| BackendPlacement::new(target.backend_idx(), target.endpoint()))
                .collect(),
        )
        .expect("schedule source fragment");
    draft
        .assign_fragment(
            19,
            vec![BackendPlacement::new(
                live_backends[0].backend_idx(),
                live_backends[0].endpoint(),
            )],
        )
        .expect("schedule destination fragment");
    let schedule = ValidatedFragmentSchedule::validate(
        parts.artifacts.scheduling_view(),
        execution_id(crate::query_execution::contract::QueryId::new(41, 73)),
        draft,
    )
    .expect("validate schedule");

    let routes = &schedule.lifecycle_projection().exchange_routes;
    assert_eq!(routes.len(), 2);
    assert_eq!(
        routes[0].source_fragment_instance_id(),
        crate::common::types::UniqueId {
            hi: -468_035_725_852_328_221,
            lo: 6_732_931_633_094_041_032,
        }
    );
    assert_eq!(
        routes[0].destination_fragment_instance_id(),
        crate::common::types::UniqueId {
            hi: 2_973_306_339_434_288_066,
            lo: -8_117_117_705_014_581_208,
        }
    );
    assert_eq!(routes[0].destination_node_id(), 190);
    assert_eq!(routes[0].sender_ordinal(), 0);
    assert_eq!(routes[0].sender_count(), 2);
    assert_eq!(
        routes[1].source_fragment_instance_id(),
        crate::common::types::UniqueId {
            hi: 8_069_940_229_124_169_845,
            lo: 713_546_644_952_921_691,
        }
    );
    assert_eq!(
        routes[1].destination_fragment_instance_id(),
        routes[0].destination_fragment_instance_id()
    );
    assert_eq!(routes[1].destination_node_id(), 190);
    assert_eq!(routes[1].sender_ordinal(), 1);
    assert_eq!(routes[1].sender_count(), 2);
}

#[test]
fn cancellation_view_observes_injected_flag() {
    let cancelled = QueryCancellationSource::new();
    let view = cancelled.view();

    assert!(!view.is_cancelled());
    let _ = cancelled.request(QueryCancellationReason::ClientDisconnected);
    assert!(view.is_cancelled());
}

#[test]
fn outcome_factory_rejects_intent_mismatch() {
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Result).write(
        crate::runtime::query_result::QueryResult::empty(),
        None,
        None,
    );

    let Err(error) = result else {
        panic!("Result intent must reject a Write outcome");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "distributed query outcome intent mismatch: expected Result, received Write"
    );
}

fn statistics_program() -> crate::query_execution::statistics::StatisticsCollectionProgram {
    let table = novarocks_spi::connector::ConnectorTableHandle::try_new(
        novarocks_spi::connector::ConnectorInstanceId::parse("statistics-test")
            .expect("instance ID"),
        Bytes::from_static(b"pinned-table"),
    )
    .expect("table handle");
    let data_version = novarocks_spi::connector::StatisticsDataVersion::try_new(
        Bytes::from_static(b"snapshot-42"),
    )
    .expect("data version");
    let evidence_revision = novarocks_spi::connector::StatisticsEvidenceRevision::try_new(
        Bytes::from_static(b"collection-42"),
    )
    .expect("evidence revision");
    let metrics = novarocks_spi::connector::StatisticsMetricRequest::try_new(vec![
        novarocks_spi::connector::StatisticsMetric::RowCount,
    ])
    .expect("metrics");
    let plan = novarocks_spi::connector::StatisticsCollectionPlan::try_new(
        table,
        data_version,
        evidence_revision,
        metrics,
        Vec::new(),
        Bytes::from_static(b"provider-plan"),
    )
    .expect("plan");
    crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
        plan,
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::DurableJobAttempt,
            std::time::Duration::from_secs(60),
        )
        .expect("policy"),
    )
    .expect("program")
}

fn statistics_result(
    program: &crate::query_execution::statistics::StatisticsCollectionProgram,
    data_version: novarocks_spi::connector::StatisticsDataVersion,
    metrics: std::collections::BTreeMap<
        novarocks_spi::connector::StatisticsMetric,
        novarocks_spi::connector::StatisticsMetricState,
    >,
) -> novarocks_spi::connector::StatisticsCollectionResult {
    novarocks_spi::connector::StatisticsCollectionResult::try_new(
        novarocks_spi::connector::StatisticsEvidence {
            data_version,
            evidence_revision: novarocks_spi::connector::StatisticsEvidenceRevision::try_new(
                Bytes::from_static(b"evidence-1"),
            )
            .expect("revision"),
            coverage: novarocks_spi::connector::StatisticsCoverage::Full,
            accuracy: novarocks_spi::connector::StatisticsAccuracy::Exact,
            interval: None,
            provenance: novarocks_spi::connector::StatisticsProvenance::VisibleRows,
            metrics,
        },
        program.plan().provider_payload().clone(),
    )
    .expect("result")
}

#[test]
fn statistics_outcome_is_typed_and_never_carries_query_rows() {
    let program = statistics_program();
    let metric = novarocks_spi::connector::StatisticsMetric::RowCount;
    let result = statistics_result(
        &program,
        program.plan().data_version.clone(),
        std::collections::BTreeMap::from([(
            metric.clone(),
            novarocks_spi::connector::StatisticsMetricState::Available(
                novarocks_spi::connector::StatisticsMetricValue::U64(7),
            ),
        )]),
    );

    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Statistics)
        .statistics(&program, result)
        .expect("statistics completion");
    let collection = outcome
        .into_statistics()
        .expect("statistics outcome variant")
        .into_collection_result();
    assert_eq!(
        collection.evidence.metrics.get(&metric),
        Some(&novarocks_spi::connector::StatisticsMetricState::Available(
            novarocks_spi::connector::StatisticsMetricValue::U64(7)
        ))
    );
}

#[test]
fn statistics_sink_rejects_version_drift_and_metric_expansion() {
    let program = statistics_program();
    let mut sink = program.result_sink();
    let drifted_version = novarocks_spi::connector::StatisticsDataVersion::try_new(
        Bytes::from_static(b"snapshot-43"),
    )
    .expect("drifted data version");
    let drifted = statistics_result(
        &program,
        drifted_version,
        std::collections::BTreeMap::from([(
            novarocks_spi::connector::StatisticsMetric::RowCount,
            novarocks_spi::connector::StatisticsMetricState::Available(
                novarocks_spi::connector::StatisticsMetricValue::U64(7),
            ),
        )]),
    );
    assert_eq!(
        sink.accept(drifted)
            .expect_err("version drift must fail")
            .kind(),
        DistributedQueryErrorKind::ContractViolation
    );

    let expanded = statistics_result(
        &program,
        program.plan().data_version.clone(),
        std::collections::BTreeMap::from([
            (
                novarocks_spi::connector::StatisticsMetric::RowCount,
                novarocks_spi::connector::StatisticsMetricState::Available(
                    novarocks_spi::connector::StatisticsMetricValue::U64(7),
                ),
            ),
            (
                novarocks_spi::connector::StatisticsMetric::ThetaNdv {
                    column: Arc::from("id"),
                },
                novarocks_spi::connector::StatisticsMetricState::Available(
                    novarocks_spi::connector::StatisticsMetricValue::U64(7),
                ),
            ),
        ]),
    );
    assert_eq!(
        sink.accept(expanded)
            .expect_err("metric expansion must fail")
            .kind(),
        DistributedQueryErrorKind::ContractViolation
    );
}

#[test]
fn durable_statistics_attempt_ignores_statement_cancellation_and_is_bounded() {
    let policy = StatisticsExecutionPolicy::try_new(
        StatisticsExecutionMode::DurableJobAttempt,
        std::time::Duration::from_secs(30 * 60),
    )
    .expect("maximum durable policy");
    assert!(!policy.mode().statement_cancellation_terminates_execution());
    assert_eq!(
        policy.attempt_timeout(),
        std::time::Duration::from_secs(30 * 60)
    );
    assert!(
        StatisticsExecutionPolicy::try_new(
            StatisticsExecutionMode::DurableJobAttempt,
            std::time::Duration::from_secs(30 * 60 + 1),
        )
        .is_err()
    );
    assert!(StatisticsExecutionMode::SynchronousWait.statement_cancellation_terminates_execution());
}

#[test]
fn statistics_theta_partials_union_without_exposing_a_sql_aggregate() {
    let left = ThetaSketchPartial::try_from_i64_values(12, [1, 2]).expect("left partial");
    let right = ThetaSketchPartial::try_from_i64_values(12, [2, 3]).expect("right partial");
    let merged = ThetaSketchPartial::try_union([left, right]).expect("two-phase union");
    assert_eq!(merged.finalize().estimate(), 3.0);
}

#[test]
fn write_outcome_preserves_commit_or_abort() {
    let write_id = crate::common::types::UniqueId { hi: 41, lo: 73 };
    let commit = WriteCommitInput {
        write_id,
        writers: Vec::new(),
    };
    let commit_outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Write)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "status",
                vec!["committed".to_string()],
            )
            .expect("commit result"),
            write_commit: Some(commit.clone()),
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Write intent accepts a commit payload");
    let (result, actual_commit, actual_abort) = commit_outcome
        .into_write()
        .expect("write outcome variant")
        .into_parts();
    assert_eq!(result.row_count(), 1);
    assert_eq!(actual_commit, Some(commit));
    assert_eq!(actual_abort, None);

    let abort = WriteAbortInput {
        write_id,
        reason: "writer failed".to_string(),
        completed_writer_outputs: Vec::new(),
        incomplete_writers: Vec::new(),
    };
    let abort_outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Write)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: None,
            write_abort: Some(abort.clone()),
            connector_completion: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Write intent accepts an abort payload");
    let (_, actual_commit, actual_abort) = abort_outcome
        .into_write()
        .expect("write outcome variant")
        .into_parts();
    assert_eq!(actual_commit, None);
    assert_eq!(actual_abort, Some(abort));
}

#[test]
fn profile_outcome_preserves_fragment_profiles() {
    let profile = crate::runtime::profile::Profiler::new("fragment-7").to_native_tree();
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Profile)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "status",
                vec!["profiled".to_string()],
            )
            .expect("profile result"),
            write_commit: None,
            write_abort: None,
            connector_completion: None,
            fragment_profiles: vec![profile.clone()],
        })
        .expect("Profile intent accepts fragment profiles");

    let (result, profiles) = outcome
        .into_profile()
        .expect("profile outcome variant")
        .into_parts();
    assert_eq!(result.row_count(), 1);
    assert_eq!(profiles.into_profiles(), vec![profile]);
}

#[test]
fn result_outcome_preserves_query_result() {
    let outcome = QueryOutcomeFactory::new(DistributedQueryIntent::Result)
        .from_execution_result(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::build_string_query_result(
                "value",
                vec!["kept".to_string()],
            )
            .expect("result payload"),
            write_commit: None,
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        })
        .expect("Result intent accepts a plain query result");

    assert_eq!(
        outcome
            .into_result()
            .expect("result outcome variant")
            .into_query_result()
            .row_count(),
        1
    );
}

#[test]
fn write_outcome_rejects_commit_and_abort_together() {
    let write_id = crate::common::types::UniqueId { hi: 8, lo: 13 };
    let result = QueryOutcomeFactory::new(DistributedQueryIntent::Write).write(
        crate::runtime::query_result::QueryResult::empty(),
        Some(WriteCommitInput {
            write_id,
            writers: Vec::new(),
        }),
        Some(WriteAbortInput {
            write_id,
            reason: "ambiguous".to_string(),
            completed_writer_outputs: Vec::new(),
            incomplete_writers: Vec::new(),
        }),
    );

    let Err(error) = result else {
        panic!("Write outcome must reject simultaneous commit and abort payloads");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert_eq!(
        error.message(),
        "Write outcome cannot contain both commit and abort payloads"
    );
}

struct RecordingCoordinator {
    calls: Arc<AtomicUsize>,
}

impl DistributedQueryCoordinator for RecordingCoordinator {
    fn execute(
        &self,
        request: DistributedQueryRequest,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        request
            .into_parts()
            .completion
            .result(crate::runtime::query_result::QueryResult::empty())
    }
}

#[test]
fn query_execution_service_uses_explicitly_injected_coordinator() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = QueryExecutionService::new(Arc::new(RecordingCoordinator {
        calls: calls.clone(),
    }));
    let (prepared, native_bundle) = real_execution_artifacts();
    let request = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Result,
        &test_execution(QueryCancellationSource::new().view()),
    )
    .expect("build service request");

    let outcome = service
        .execute(request)
        .expect("injected coordinator result");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.intent(), DistributedQueryIntent::Result);
}

#[test]
fn generic_request_builder_rejects_statistics_without_a_typed_program() {
    let (prepared, native_bundle) = real_execution_artifacts();
    let result = build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        None,
        DistributedQueryIntent::Statistics,
        &test_execution(QueryCancellationSource::new().view()),
    );
    let Err(error) = result else {
        panic!("statistics must use the typed request builder");
    };
    assert_eq!(error.kind(), DistributedQueryErrorKind::ContractViolation);
    assert!(error.message().contains("StatisticsCollectionProgram"));
}
