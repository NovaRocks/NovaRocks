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

//! SAME production Launcher, preparation, qualifier and Native/AP/LP Sources.
//! Two finite RPC peers supply explicit external facts, not BE execution/M5.
use super::*;
use crate::fixtures::native_recipe::{
    OriginalRecipeProcessPolicy, PreparedOriginalRecipeProcess, ReadCloseGate,
    prepare_original_recipe_process_with_policy,
};
use crate::native::{data_runtime::FrontendDataRuntime, task_transport::tests::Loopback};
use crate::query::statement_computation::StatementStageAdmission;
use crate::query_execution::{
    contract::ResolvedQueryOptions,
    lifecycle_diagnostics::FrontendLifecycleDiagnostics,
    logical_execution::{
        ComputationDiagnostics, LogicalExecutionLauncher, OriginalComputationStart,
        PreparedLogicalComputation,
    },
    maintenance::io_admission::MetadataIoAdmission,
    native_execution_adapter::{
        FrontendNativeLogicalExecutionLauncher, FrontendNativeLogicalExecutionRuntime,
        original_install_fixture::OriginalInstallProbe,
    },
};
use novarocks_coordination_contract::{AttemptFailureClass, MonotonicClock};
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_execution_contract::{
    AdmissionEpochCapability, BackendProcessDescriptor, RuntimeEndpoint, failure::FailureEvidence,
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
        BackendTopologyPort, BackendTopologySnapshot, BackendTopologyValidationError,
        ExecutionOutputObligation, LiveBackendTarget,
    },
    coordination::{LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig},
    execution_error::{ExecutionFailureSite, QueryFailureReason},
};
use novarocks_task_codec::operation::{DecodedOperation, DecodedUpdateParticipation};
use novarocks_types::{BackendProcessId, identity::QueryProcessNamespace};
use novarocks_workload_control::{ResultCapacityConfig, RootWork, WorkClass, WorkRequest};
use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::OnceLock,
    task::{Context, Waker},
};

/// Actual independent peer processes remain Current at their frozen endpoints.
/// Future eligibility moves to B without claiming that A exited or was replaced.
struct OriginalPeers {
    descriptors: [BackendProcessDescriptor; 2],
    snapshot: Mutex<BackendTopologySnapshot>,
    changes: tokio::sync::watch::Sender<u64>,
    process_changes: tokio::sync::watch::Sender<u64>,
}
impl OriginalPeers {
    fn new(descriptors: [BackendProcessDescriptor; 2]) -> Arc<Self> {
        let (changes, _) = tokio::sync::watch::channel(1);
        let (process_changes, _) = tokio::sync::watch::channel(0);
        let snapshot = Self::snapshot_for(1, &descriptors[0]);
        Arc::new(Self {
            descriptors,
            snapshot: Mutex::new(snapshot),
            changes,
            process_changes,
        })
    }
    fn snapshot_for(
        revision: u64,
        descriptor: &BackendProcessDescriptor,
    ) -> BackendTopologySnapshot {
        BackendTopologySnapshot::try_new(
            revision,
            vec![LiveBackendTarget::new(
                0,
                descriptor.clone(),
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
            )],
        )
        .unwrap()
    }
    fn admit_only_successor(&self) {
        *self.snapshot.lock().unwrap() = Self::snapshot_for(2, &self.descriptors[1]);
        self.changes.send_replace(2);
    }
}
impl BackendTopologyPort for OriginalPeers {
    fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        Ok(self.snapshot.lock().unwrap().clone())
    }
    fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }
    fn validate_snapshot(
        &self,
        expected: &BackendTopologySnapshot,
    ) -> Result<(), BackendTopologyValidationError> {
        let current = self.snapshot.lock().unwrap();
        if expected.places_the_same_work_as(&current) {
            Ok(())
        } else {
            Err(BackendTopologyValidationError::RevisionChanged {
                captured_revision: expected.revision(),
                current_revision: current.revision(),
            })
        }
    }
    fn wait_for_eligible_after(
        &self,
        revision: u64,
        deadline: std::time::Instant,
    ) -> Result<BackendTopologySnapshot, BackendTopologyError> {
        let current = self.snapshot.lock().unwrap();
        if current.revision() > revision
            && !current.targets().is_empty()
            && std::time::Instant::now() < deadline
        {
            Ok(current.clone())
        } else {
            Err(BackendTopologyError::Unavailable {
                message: "the explicit peer topology has no newer eligible cut".into(),
            })
        }
    }
    fn record_successful_stage(&self, _: usize, _: usize) {}
}
impl BackendProcessObservationPort for OriginalPeers {
    fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.process_changes.subscribe()
    }
    fn observe_process_at_endpoint(
        &self,
        process: BackendProcessId,
        endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        let original = self
            .descriptors
            .iter()
            .find(|descriptor| descriptor.process_id() == process)
            .ok_or_else(|| BackendTopologyError::Unavailable {
                message: "foreign finite peer process".into(),
            })?;
        if original.endpoint() != endpoint {
            return Err(BackendTopologyError::Unavailable {
                message: "foreign finite peer endpoint".into(),
            });
        }
        Ok(BackendProcessObservation::Current)
    }
}

struct OriginalRecoveryRun {
    bootstrap: PreparedOriginalRecipeProcess,
    data: FrontendDataRuntime,
    peers: [Loopback; 2],
    root: Option<RootWork>,
    metadata: Option<Arc<MetadataIoAdmission>>,
    stage: Option<StatementStageAdmission>,
    binding: Option<QueryResultCapacityBinding>,
    prepared: Option<PreparedLogicalComputation>,
    start: OriginalComputationStart,
    output: Option<crate::query_execution::internal_consumer::profile::ProfileComputationOutput>,
    receiving: Option<ExecutionOutputObligation>,
    ready: Option<ReadyProfileComputation>,
}
struct RetainOriginalRecovery(Option<Box<OriginalRecoveryRun>>);
impl Drop for RetainOriginalRecovery {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            // Assertion unwind preserves SAME unresolved originals, without
            // asynchronous cleanup, forgotten owners or any claimed exit.
            static UNKNOWN: OnceLock<Mutex<Vec<Box<OriginalRecoveryRun>>>> = OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
async fn drive<F: Future>(
    phase: &'static str,
    probe: Option<&OriginalInstallProbe>,
    originals: Option<(&OriginalComputationStart, &[Loopback; 2])>,
    original: &mut PreparedOriginalRecipeProcess,
    metadata: &Arc<MetadataIoAdmission>,
    advance_indexed_clock: bool,
    future: F,
) -> F::Output {
    let mut future = Box::pin(future);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
                &mut original.joint,
            );
            metadata
                .poll_actual_progress_continuing(&mut Context::from_waker(Waker::noop()))
                .unwrap();
            if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
                return value;
            }
            let actual = original
                .fixture
                .progress(
                    crate::fixtures::ProgressSelection::All,
                    crate::fixtures::ProgressBudget::default(),
                )
                .await;
            if advance_indexed_clock
                && actual.actors == 0
                && actual.hosts == 0
                && let Some(next) = original.fixture.runtime().next_deadline()
                && std::time::Duration::from_nanos(next.0) > original.clock.now().since_origin()
            {
                original.clock.advance(Duration::from_nanos(
                    next.0 - u64::try_from(original.clock.now().since_origin().as_nanos()).unwrap(),
                ));
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if result.is_err() {
        eprintln!(
            "original production Profile recovery wait expired: phase={phase}, clock={:?}, indexed_deadline={:?}, advance_indexed_clock={}, unresolved_work={:?}, ledger_empty={}",
            original.clock.now(),
            original.fixture.runtime().next_deadline(),
            advance_indexed_clock,
            original.fixture.ledger().unresolved_work(),
            original.fixture.ledger().is_empty(),
        );
        eprintln!(
            "original production Profile preparation owners: phase={phase}, metadata_failure={:?}, metadata_cleanup={:?}, metadata_health_failed={}, supervisor_exited={}, process_failure={}, process_registries_empty={}, actual_workload={:?}, runtime_observation={:?}",
            metadata.failure(),
            metadata.cleanup_failure(),
            metadata.health_failure().is_some(),
            original.supervisor.service_has_exited(),
            original.supervisor.process_failure().is_some(),
            original.process.original_registries_empty_for_fixture(),
            original.workload.snapshot(),
            original.fixture.runtime().observation_snapshot(),
        );
        if let Some((start, peers)) = originals {
            // Borrow the original exit view only. Do not poll/take/drop Ready,
            // advance preparation, clone a raw cause, or manufacture a receipt.
            let actual = start.exit.as_ref().and_then(|observer| {
                observer.view().map(|view| {
                    let preparation = view
                        .native_preparation_failure()
                        .and_then(|raw| {
                            raw.downcast_ref::<novarocks_coordination_contract::ExecutionFailure>()
                        })
                        .map(|failure| format!("{failure:?}"));
                    (
                        view.actor_phase(),
                        view.role_ended(),
                        view.role_failed(),
                        format!("{:?}", view.runtime_actor_result()),
                        view.native_preparation_failure().is_some(),
                        preparation,
                        view.native_dormant_failed(),
                        view.native_preparation_exited(),
                        view.native_action_started(),
                        view.pending_native_returns(),
                        view.native_closure_observed(),
                        view.native_return_disposition(),
                    )
                })
            });
            eprintln!(
                "original production Profile fixed start: reservation={}, request={}, input={}, ready={}, exit={}, original_exit={actual:?}",
                start.reservation.is_some(),
                start.request.is_some(),
                start.input.is_some(),
                start.ready.is_some(),
                start.exit.is_some(),
            );
            let mut acquired = 0usize;
            let mut invalid_operations = 0usize;
            for request in peers[1].applied_requests() {
                for operation in request.operations {
                    match novarocks_task_codec::operation::decode_operation(
                        &operation,
                        novarocks_proto_codec::FieldPath::root("original_successor_timeout"),
                    ) {
                        Ok(DecodedOperation::AcquireParticipationAdmissionTicket(_)) => {
                            acquired += 1;
                        }
                        Ok(_) => {}
                        Err(_) => invalid_operations += 1,
                    }
                }
            }
            eprintln!(
                "original production Profile qualification boundary: a_abort={}, a_release={}, b_actual_acquire_ticket_operations={}, undecodable_b_operations={}",
                peers[0].single_root_abort_observed(),
                peers[0].single_root_release_observed(),
                acquired,
                invalid_operations,
            );
        }
        if let Some(probe) = probe {
            for ordinal in 0..2 {
                if let Some(row) = probe.row(ordinal) {
                    eprintln!(
                        "original production Profile installed row: ordinal={ordinal}, activation={:?}, root={:?}, tasks={:?}, participations={:?}, actual_exit={}, original_read_failure={:?}, admissions={:?}, closed={:?}",
                        row.activation,
                        row.root,
                        row.tasks,
                        row.participations,
                        row.actual_exit_observed(),
                        row.read_observation.failure(),
                        row.admissions,
                        row.closed,
                    );
                    // This identity comes from the SAME successful AP install,
                    // never from actor numbering or the requested activation.
                    if let Some(actor) = row.actual_actor() {
                        let runtime = original.fixture.runtime();
                        eprintln!(
                            "original production Profile actual AP: ordinal={ordinal}, actor={actor:?}, snapshot={:?}",
                            runtime.snapshot(actor),
                        );
                        if let Ok(inputs) = runtime.input_inventory(actor, None, 8) {
                            let standing = runtime.standing_input_inventory(actor, None, 32);
                            for input in inputs {
                                eprintln!(
                                    "original production Profile actual AP input: actor={actor:?}, input={input:?}, preflight={:?}, header_authors={:?}, matching_standing={:?}",
                                    runtime.input_retirement_status(actor, input.input),
                                    runtime.input_header_authors(actor, input.input, 8),
                                    standing.as_ref().ok().and_then(|rows| rows.iter().find(
                                        |row| {
                                            Some(row.standing_input_id) == input.standing_input_id
                                        }
                                    )),
                                );
                            }
                        }
                        if let Ok(batches) = runtime.committed_batches(actor, None, 8) {
                            for (ordinal, input) in batches {
                                eprintln!(
                                    "original production Profile actual AP effects: actor={actor:?}, batch={ordinal:?}, input={input:?}, effects={:?}",
                                    runtime.effect_inventory(actor, input, None, 16),
                                );
                            }
                        }
                    }
                } else {
                    eprintln!(
                        "original production Profile installed row: ordinal={ordinal}, not_captured=true"
                    );
                }
            }
            eprintln!(
                "original production Profile actual isolation finishes: {:?}",
                probe.isolation_finished(),
            );
            for qualification in probe.qualifications() {
                eprintln!(
                    "original production Profile qualification: identity={:?}, operation={:?}, failed={:?}, replacement={:?}, issued_at={:?}, expiry={:?}",
                    qualification.identity,
                    qualification.operation,
                    qualification.failed,
                    qualification.replacement,
                    qualification.issued_at,
                    qualification.conservative_expiry,
                );
            }
            if let Some(candidate) = probe.candidate(0) {
                eprintln!(
                    "original production Profile candidate: execution={:?}, root={:?}, tasks={:?}, participations={:?}",
                    candidate.execution, candidate.root, candidate.tasks, candidate.participations,
                );
            }
        }
    }
    result.unwrap_or_else(|_| {
        panic!("Profile component watchdog: same production Profile recovery Source")
    })
}
fn configure_original_peer(peer: &Loopback, root: TaskIdentity, tasks: &[TaskIdentity]) {
    peer.enable_single_root_lifecycle(root, novarocks_result_contract::RootOutputKind::CountOnly);
    assert!(!tasks.is_empty());
    peer.enable_original_task_roster(tasks).unwrap();
    for &task in tasks {
        peer.enable_original_final_task_info(task, Vec::new(), false)
            .unwrap();
    }
}

pub(super) async fn run() {
    let data = FrontendDataRuntime::new(tokio::runtime::Handle::current());
    let first_peer = Loopback::start_with_original_root_connection_control().await;
    let next_peer = Loopback::start().await;
    let descriptors = [
        first_peer.bounded_root_descriptor(BackendProcessId::new_v7()),
        next_peer.bounded_root_descriptor(BackendProcessId::new_v7()),
    ];
    let topology = OriginalPeers::new(descriptors.clone());
    let config = LogicalExecutionSupervisorConfig::new(
        NonZeroUsize::MIN,
        NonZeroUsize::MIN,
        LogicalExecutionRowsConfig::new(
            NonZeroUsize::MIN,
            NonZeroU32::new(2).unwrap(),
            Duration::from_secs(30),
        ),
    )
    .with_diagnostic_timeout(Duration::from_secs(1))
    .with_remote_cleanup_timeout(Duration::from_secs(5));
    let bootstrap = prepare_original_recipe_process_with_policy(
        &data,
        descriptors[0].clone(),
        novarocks_control_runtime::history::HistoryCapture::new(65_536),
        ResultCapacityConfig::V1,
        None,
        OriginalRecipeProcessPolicy {
            supervisor: config,
            namespace: QueryProcessNamespace::new(712),
            observation_budget: None,
            process_observation: topology.clone(),
        },
    );
    let mut retained = RetainOriginalRecovery(Some(Box::new(OriginalRecoveryRun {
        bootstrap,
        data,
        peers: [first_peer, next_peer],
        root: None,
        metadata: None,
        stage: None,
        binding: None,
        prepared: None,
        start: OriginalComputationStart::default(),
        output: None,
        receiving: None,
        ready: None,
    })));
    let original = retained.0.as_mut().unwrap();
    original.root = Some(
        original
            .bootstrap
            .workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap(),
    );
    let root = original.root.as_ref().unwrap();
    original.metadata =
        Some(MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap());
    let metadata = original.metadata.as_ref().unwrap().clone();
    watchdog("same positive original metadata Root", async {
        loop {
            crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
                &mut original.bootstrap.joint,
            );
            if metadata
                .poll_progress(&root.owner, &mut Context::from_waker(Waker::noop()))
                .unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let carrier = crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
        novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
        novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
    );
    let (description, plan, encoded) = crate::query_execution::internal_consumer::test_fixture::internal_profile_recovery_encoded_description(
        &root.owner.scope(), &carrier, crate::application::test_constant_policy(), &novarocks_sql::compiler::SqlCompileControl::unbounded()).await.unwrap();
    assert_eq!(
        description.kind(),
        novarocks_query_application::api::QueryExecutionKind::Read
    );
    assert_eq!(
        description.effect(),
        novarocks_coordination_contract::ExecutionEffect::None
    );
    assert!(description.matches_plan_seal(plan));
    assert_eq!(
        description.recovery(),
        novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility
    );
    let novarocks_query_application::api::PlanSeal::Version(version) = plan;
    let template = encoded.into_attempt_template(version);
    original.stage = Some(StatementStageAdmission::new(root.owner.scope(), 0));
    let stage = original.stage.as_mut().unwrap();
    drive(
        "original metadata stage admission",
        None,
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        std::future::poll_fn(|cx| stage.poll_admit(&metadata, &root.owner, cx)),
    )
    .await
    .unwrap();
    stage
        .install_internal_read_capacity_in(&metadata, &mut original.binding)
        .unwrap();
    let (consumer, offers, output) = profile_computation_consumer(&description, plan).unwrap();
    original.output = Some(output);
    let options = Arc::new(ResolvedQueryOptions::from_upstream(Some(
        novarocks_proto_codec::lifecycle::QueryOptions::from_proto(
            novarocks_proto_models::novarocks::QueryOptions {
                pipeline_dop: 1,
                query_timeout: 60,
                enable_profile: true,
                ..Default::default()
            },
        ),
    )));
    original.prepared = Some(
        PreparedLogicalComputation::new(
            description,
            template,
            options,
            consumer,
            offers,
            ComputationDiagnostics::Profile,
        )
        .unwrap(),
    );
    let probe = Arc::new(OriginalInstallProbe::default());
    let runtime = FrontendNativeLogicalExecutionRuntime::new(
        topology.clone(),
        topology.clone(),
        original.data.clone(),
        original.bootstrap.process.clone(),
        descriptors[0].native_compatibility_id(),
        NonZeroUsize::MIN,
        novarocks_attempt_protocol::config::TaskUpdateRetryPolicy::default(),
        Duration::from_millis(1),
        novarocks_attempt_protocol::config::CoordinationBudgets::DEFAULT,
        Arc::new(FrontendLifecycleDiagnostics::default()),
        original.bootstrap.clock.clone(),
    )
    .with_original_install_probe(probe.clone());
    let launcher =
        FrontendNativeLogicalExecutionLauncher::new(original.bootstrap.client.clone(), runtime);
    launcher
        .start_computation_in(
            &mut original.prepared,
            original.stage.as_mut().unwrap().owner_slot(),
            original.binding.as_ref().unwrap(),
            &metadata,
            &mut original.start,
            None,
        )
        .unwrap()
        .finish()
        .unwrap();
    let first_candidate = drive(
        "same request initial candidate",
        Some(&probe),
        Some((&original.start, &original.peers)),
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            loop {
                if let Some(candidate) = probe.candidate(0) {
                    break candidate;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(
        first_candidate.root.query_execution_id(),
        first_candidate.execution
    );
    assert_eq!(
        first_candidate.root.backend_process_id(),
        descriptors[0].process_id()
    );
    assert!(probe.row(0).is_none());
    // This original candidate pause precedes control binding/initialization.
    // It is not the later row pause at the actual Source installation seam.
    first_candidate.release();
    let first = drive(
        "actual A Source install",
        Some(&probe),
        Some((&original.start, &original.peers)),
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            loop {
                if let Some(row) = probe.row(0) {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(first.root.backend_process_id(), descriptors[0].process_id());
    assert_eq!(first.activation.execution(), first_candidate.execution);
    assert_eq!(first.root, first_candidate.root);
    assert_eq!(
        first
            .tasks
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        first_candidate.tasks.iter().copied().collect()
    );
    assert_eq!(first.participations, first_candidate.participations);
    configure_original_peer(&original.peers[0], first.root, &first.tasks);
    original.peers[0].defer_single_root_first_reply().unwrap();
    original.peers[0]
        .close_original_stopped_roster_on_abort()
        .unwrap();
    let held_originals = Arc::new(ReadCloseGate::default());
    let _release_on_unwind = held_originals.release_guard();
    first.hold_original_disposal(&held_originals);
    first.release();
    let peer = &original.peers[0];
    drive(
        "actual A Root RPC entered",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            while !peer.single_root_first_reply_waiting() {
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(peer.single_root_result_progress(), (false, 0));
    assert!(
        futures::poll!(original.start.ready.as_mut().unwrap()).is_pending(),
        "no Root End, no original Ready"
    );
    topology.admit_only_successor();
    peer.disconnect_original_root_connection();
    drive(
        "actual A original BackendLost read",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            while first.read_observation.failure().is_none() {
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    let actual_failure = first.read_observation.failure().unwrap();
    assert_eq!(
        actual_failure.fact(),
        QueryFailureReason::BackendLost.failure_fact()
    );
    assert_eq!(
        actual_failure.fact().evidence(),
        FailureEvidence::Originating
    );
    assert_eq!(
        actual_failure.site(),
        ExecutionFailureSite::Backend {
            process: first.root.backend_process_id()
        }
    );
    assert_eq!(
        actual_failure.attempt_class(),
        AttemptFailureClass::RecoverableInfrastructure
    );
    let candidate = drive(
        "same request successor candidate",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            loop {
                if let Some(row) = probe.candidate(1) {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert!(!Arc::ptr_eq(&candidate, &first_candidate));
    assert_eq!(
        candidate.execution.query_id(),
        first.activation.execution().query_id()
    );
    assert_ne!(candidate.execution, first.activation.execution());
    assert_eq!(
        candidate.root.backend_process_id(),
        descriptors[1].process_id()
    );
    configure_original_peer(&original.peers[1], candidate.root, &candidate.tasks);
    original.peers[1].defer_single_root_terminal().unwrap();
    candidate.release();
    let next = drive(
        "actual B Source install",
        Some(&probe),
        Some((&original.start, &original.peers)),
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            loop {
                if let Some(row) = probe.row(1) {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(next.root, candidate.root);
    assert_eq!(
        next.tasks
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        candidate.tasks.iter().copied().collect()
    );
    assert_eq!(next.participations, candidate.participations);
    assert_eq!(first.activation.actor(), next.activation.actor());
    assert_eq!(
        first.activation.execution().query_id(),
        next.activation.execution().query_id()
    );
    assert!(first.activation.generation() < next.activation.generation());
    drive(
        "same A original Native disposal body entered",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        true,
        async {
            while !held_originals.entered() {
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert!(
        !first.actual_exit_observed(),
        "SAME A Native captures and their actual disposal Join are still held"
    );
    for participation in &first.participations {
        assert!(
            next.closed.contains(participation),
            "SAME actual ClosedParticipations, not Current process or HostExit"
        );
    }
    assert!(!next.admissions.is_empty());
    let qualification = probe.qualifications();
    assert_eq!(qualification.len(), 1);
    let qualification = &qualification[0];
    assert_eq!(qualification.identity.actor(), first.activation.actor());
    assert_eq!(
        qualification.identity.failed(),
        first.activation.execution()
    );
    assert_eq!(
        qualification.identity.replacement(),
        next.activation.execution()
    );
    assert_eq!(qualification.failed, first.participations);
    assert_eq!(qualification.replacement, next.participations);
    assert_eq!(
        qualification.conservative_expiry,
        qualification
            .issued_at
            .saturating_add(Duration::from_secs(30))
    );
    let old_credentials = first.credentials.upgrade().unwrap();
    let next_credentials = next.credentials.upgrade().unwrap();
    assert!(!Arc::ptr_eq(&old_credentials, &next_credentials));
    next.release();
    let peer = &original.peers[1];
    let original_statuses = drive(
        "actual B Creates and leaf Finished before Root terminal",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            loop {
                let statuses = peer.original_created_task_statuses();
                if statuses.len() == next.tasks.len()
                    && statuses.iter().all(|status| {
                        next.tasks.contains(&status.identity())
                            && status.state()
                                == if status.identity() == next.root {
                                    novarocks_execution_contract::TaskState::Running
                                } else {
                                    novarocks_execution_contract::TaskState::Finished
                                }
                    })
                {
                    break statuses;
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(original_statuses.len(), next.tasks.len());
    peer.publish_single_root_terminal(None).unwrap();
    let ready_future = original.start.ready.take().unwrap();
    original.receiving = Some(
        drive(
            "actual B computation Ready",
            Some(&probe),
            None,
            &mut original.bootstrap,
            &metadata,
            false,
            ready_future,
        )
        .await
        .unwrap(),
    );
    let receiving = original.receiving.as_mut().unwrap();
    let token = *receiving
        .ready_token()
        .expect("SAME production Relay/Internal consumer accepted B");
    original.ready = Some(original.output.as_mut().unwrap().take_ready(token).unwrap());
    assert!(original.output.as_mut().unwrap().take_ready(token).is_err());
    let received = original.ready.as_mut().unwrap();
    assert_eq!(token.activation(), next.activation);
    assert_eq!(token.root(), next.root);
    assert_eq!(
        token.diagnostic_deadline(),
        token.cut().saturating_add(Duration::from_secs(1))
    );
    assert_eq!(
        token.convergence_deadline(),
        token.cut().saturating_add(Duration::from_secs(5))
    );
    assert_eq!(received.output_rows(), 0);
    assert_eq!(received.expected_tasks(), next.tasks.as_slice());
    assert!(Arc::ptr_eq(
        received.original_credentials().unwrap(),
        &next_credentials
    ));
    let observer = received.original_observer_for_fixture();
    let snapshot = drive(
        "actual B immutable diagnostics",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        received.wait_diagnostics(),
    )
    .await
    .unwrap();
    assert!(
        snapshot.complete(),
        "original B diagnostic snapshot incomplete: ready={:?}, closure={:?}, selected_at={:?}, root={:?}, expected_tasks={:?}, tasks={:?}, actual_final_info_reads={:?}, split={:?}, runtime_filters_complete={}, runtime_filter_available={:?}",
        snapshot.ready,
        snapshot.closure,
        snapshot.selected_at,
        next.root,
        next.tasks,
        snapshot.tasks.iter().map(|(task, fact)| {
            let observation = match fact {
                ProfileTaskDiagnostic::Available(info) => {
                    format!("Available({:?})", info.final_status().identity())
                }
                ProfileTaskDiagnostic::Missing(reason) => format!("Missing({reason:?})"),
            };
            (*task, observation)
        }).collect::<Vec<_>>(),
        original.peers[1].original_final_task_info_reads(),
        snapshot.split,
        snapshot.runtime_filters.is_complete(),
        snapshot.runtime_filters.contributions().iter().map(|(backend, telemetry)| {
            (*backend, matches!(telemetry.as_proto().telemetry,
                Some(novarocks_proto_models::novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Available(_))))
        }).collect::<Vec<_>>(),
    );
    assert_eq!(snapshot.ready, token);
    assert_eq!(snapshot.tasks.len(), next.tasks.len());
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|(task, info)| next.tasks.contains(task)
                && matches!(info, ProfileTaskDiagnostic::Available(_)))
    );
    let actual_reads = original.peers[1].original_final_task_info_reads();
    assert_eq!(actual_reads.len(), next.tasks.len());
    assert_eq!(
        actual_reads
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        next.tasks.iter().copied().collect()
    );
    assert!(
        original.peers[0]
            .original_final_task_info_reads()
            .is_empty(),
        "AP never prefetches diagnostic reads before genuine Ready"
    );
    // Exact original admission/ticket/operation: B's Establish consumes the
    // ONE real qualification Acquire, never a replacement initial acquisition.
    let mut acquired = Vec::new();
    let mut established = Vec::new();
    for request in original.peers[1].applied_requests() {
        for operation in request.operations {
            match novarocks_task_codec::operation::decode_operation(
                &operation,
                novarocks_proto_codec::FieldPath::root("original_successor"),
            )
            .unwrap()
            {
                DecodedOperation::AcquireParticipationAdmissionTicket(value) => {
                    acquired.push(value)
                }
                DecodedOperation::UpdateParticipation(DecodedUpdateParticipation::Establish(
                    value,
                )) => established.push(value),
                _ => {}
            }
        }
    }
    assert_eq!(acquired.len(), next.participations.len());
    assert_eq!(established.len(), next.participations.len());
    for (participation, receipt) in &next.admissions {
        assert_eq!(*participation, receipt.participation());
        let acquire = acquired
            .iter()
            .find(|value| value.participation() == *participation)
            .unwrap();
        assert_eq!(acquire.valid_for(), receipt.valid_for());
        assert_eq!(
            acquire.native_compatibility_id(),
            descriptors[1].native_compatibility_id()
        );
        assert_eq!(
            acquire.admission_epoch_capability(),
            topology.snapshot().unwrap().targets()[0].admission_epoch_capability()
        );
        assert_ne!(acquire.envelope().operation_id(), qualification.operation);
        let establish = established
            .iter()
            .find(|value| value.participation() == *participation)
            .unwrap();
        assert_eq!(establish.admission_ticket_id(), receipt.ticket_id());
    }
    assert!(
        !first.actual_exit_observed(),
        "B actual Ready does not destroy A's original Native captures"
    );
    held_originals.release();
    drive(
        "actual A Source exit after gate release",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        false,
        async {
            while !first.actual_exit_observed() {
                tokio::task::yield_now().await;
            }
        },
    )
    .await;
    assert_eq!(held_originals.calls(), 1);
    assert!(
        Arc::ptr_eq(&snapshot, &observer.current(token).unwrap().unwrap()),
        "late original A result/Join/exit cannot replace B's first immutable snapshot"
    );
    let mut exit = original
        .receiving
        .as_mut()
        .unwrap()
        .take_exit_observer()
        .or_else(|| original.start.exit.take())
        .unwrap();
    drop(original.ready.take());
    drop(original.receiving.take());
    drop(original.output.take());
    drop((old_credentials, next_credentials));
    let receipt = drive(
        "same logical actual exit receipt",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        true,
        std::future::poll_fn(|cx| exit.poll_exit(cx)),
    )
    .await
    .unwrap();
    assert!(first.actual_exit_observed() && next.actual_exit_observed());
    assert!(
        original.peers[0].single_root_abort_observed()
            && original.peers[1].single_root_release_observed(),
        "original failed A Abort and successful B Release were observed, separately from their exact Worker closure and actual FE exits"
    );
    assert_eq!(
        probe.isolation_finished(),
        vec![next.activation.execution()],
        "the SAME production isolation owner actually finished exactly once"
    );
    assert!(matches!(
        receipt.view().runtime_actor_result(),
        Some(novarocks_query_application::api::logical_runtime::LogicalExit::Completed)
    ));
    assert!(Arc::ptr_eq(
        &snapshot,
        &observer.current(token).unwrap().unwrap()
    ));
    assert_eq!(original.peers[1].single_root_result_progress(), (true, 1));
    drop((receipt, exit, observer, snapshot));
    original.stage.as_mut().unwrap().release_capacity_alias();
    original.stage.as_mut().unwrap().retire_unlaunched();
    drop(original.stage.take());
    drop(original.binding.take());
    drop(launcher);
    let root_owner = &original.root.as_ref().unwrap().owner;
    drive(
        "original metadata quiescence",
        Some(&probe),
        None,
        &mut original.bootstrap,
        &metadata,
        true,
        std::future::poll_fn(|cx| metadata.poll_quiescent(root_owner, cx)),
    )
    .await
    .unwrap();
    drop(original.metadata.take());
    drop(metadata);
    let PreparedOriginalRecipeProcess {
        fixture,
        joint,
        supervisor,
        clock,
        ..
    } = &mut original.bootstrap;
    drive_original_profile_source(
        fixture,
        joint,
        clock.as_ref(),
        true,
        supervisor.shutdown_until(std::time::Instant::now() + Duration::from_secs(5)),
    )
    .await
    .unwrap();
    original
        .bootstrap
        .process
        .close_original_host_registration_for_fixture();
    original.bootstrap.fixture.finish().await;
    let history = crate::fixtures::history::CapturedHistory::drain(
        &original.bootstrap.fixture.history(),
        usize::MAX,
    );
    history
        .check_completed_runtime(
            original.bootstrap.fixture.runtime(),
            original.bootstrap.fixture.ledger(),
        )
        .unwrap();
    let root = original.root.take().unwrap();
    let scope = root.owner.scope();
    root.owner.complete();
    root.business.release();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
        &mut original.bootstrap.joint,
    );
    tokio::time::timeout(Duration::from_secs(5), scope.wait_released())
        .await
        .unwrap();
    original.bootstrap.workload.close_admission();
    crate::query_execution::native_execution_adapter::source_cohort::tests::drive(
        &mut original.bootstrap.joint,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    original
        .data
        .drain_original_apply_until(deadline)
        .await
        .unwrap();
    original
        .data
        .drain_native_outgoing_until(deadline)
        .await
        .unwrap();
    for peer in &mut original.peers {
        assert!(peer.settle().await);
    }
    assert!(
        original
            .bootstrap
            .process
            .original_registries_empty_for_fixture()
    );
    // All actual Native/peer cleanup completed before consuming the SAME WC
    // owner and disarming the assertion-unwind custody guard.
    drop(scope);
    let completed = *retained.0.take().unwrap();
    assert!(completed.bootstrap.workload.shutdown().is_ok());
}
