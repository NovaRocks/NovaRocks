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

//! Real Worker context/admission tests for the transport-independent reader.
//! These tests install no RPC and make no HTTP/H2 ownership claim.

use std::any::Any;
use std::future::{Future, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use novarocks_execution::runtime::fragment::io::{
    ResultWriteAdmission, ResultWriteCredit, RootResultWriteSpec,
};
use novarocks_execution_contract::TaskOutputFacts;
use novarocks_execution_contract::failure::{
    FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts, ResourceScope,
    domain,
};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_execution_contract::root_result::{
    RootReadDeferral, RootReadOutcome, RootReadRefusal, RootReadResponse, RootResultRead,
};
use novarocks_execution_contract::task_execution::creation::{
    CreationContent, FrozenBytes, PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::{
    ExchangeTopology, FragmentSinkKind, TaskDescriptor,
};
use novarocks_execution_contract::task_execution::domain::{
    CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch, CredentialLeaseId,
    PlanNodeId,
};
use novarocks_execution_contract::task_execution::identity::{
    QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::lease::{LeaseSequence, LeaseValidFor};
use novarocks_execution_contract::task_execution::operation::{
    AcquireQueryContextAdmissionTicket, CreateTask, CredentialUpdate, EstablishQueryContext,
    OperationOutcome, QuiesceQueryContext, ReleaseQueryContext, RenewQueryExecutionLease,
    TaskDomainUpdate, UpdateQueryContext,
};
use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
use novarocks_execution_contract::task_execution::transition::QueryContextState;
use novarocks_native_adapter::root_result_reader::{
    NativeRootReadResponse, NativeRootResultReader, NativeRootResultReply,
};
use novarocks_result_contract::{
    ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
    RenderPresentation, RootOutputContract, RootOutputKind, RootProfileId, RootProfileV1,
};
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
    QueryId, StageId, TaskId, UniqueId,
};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use novarocks_worker::root_result_channel::RootResultChannel;
use novarocks_worker::{
    HostRejection, ManualClock, PreparedTaskInstallation, QueryContextHost,
    ReleasedContextEvidence, RunnableTask, SharedFactsRequest, TaskExecutionHost,
    TaskExecutionMetrics, TaskExecutionPorts, TaskExecutionRegistry, TaskExecutionRegistryConfig,
    TaskProtocolEvent, TaskProtocolObserver, TaskResultLifecycle, TaskStatusReporter,
    WorkerMonotonicClock, WorkerResultRetainedLimits,
};

const FIXED: usize = RootProfileV1::SCHEMA_BACKING_BYTES;
const SEGMENT: usize = RootProfileV1::SEGMENT_BYTES + RootProfileV1::ENVELOPE_BYTES;
const COPY: usize = 2 * SEGMENT;
const PROCESS: usize = 512 * 1024 * 1024;

#[derive(Debug)]
struct Assignment;
impl CreationContent for Assignment {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
    }
    fn encoded_len(&self) -> usize {
        8
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
#[derive(Debug)]
struct Content;
impl CodecOwnedContent for Content {
    fn fingerprint(&self) -> ContentFingerprint {
        ContentFingerprint::from_bytes([1; 16])
    }
    fn encoded_len(&self) -> usize {
        16
    }
}
struct Secret;
impl ConfidentialContent for Secret {
    fn encoded_len(&self) -> usize {
        16
    }
    fn matches(&self, other: &dyn ConfidentialContent) -> bool {
        other.encoded_len() == 16
    }
}
struct ContextHost;
impl QueryContextHost for ContextHost {
    fn materialize(&self, _: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        Ok(())
    }
    fn release(&self, _: QueryContextRef) -> ReleasedContextEvidence {
        ReleasedContextEvidence::none()
    }
    fn advance_shared_domain(
        &self,
        _: QueryContextRef,
        _: &novarocks_execution_contract::task_execution::operation::QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        Ok(())
    }
}
struct Noop;
impl TaskProtocolObserver for Noop {
    fn observe(&self, _: TaskProtocolEvent) {}
}
impl TaskResultLifecycle for Noop {
    fn discard_task(&self, _: TaskIdentity) {}
    fn retire_task_result(&self, _: TaskIdentity) {}
}
impl TaskExecutionMetrics for Noop {
    fn record_task_created(&self) {}
}
#[derive(Debug)]
struct Runnable;
impl RunnableTask for Runnable {
    fn commit_creation(&self) {}
    fn cancel(&self, _: CancelReason) {}
    fn abort(&self, _: AbortCause) {}
}
#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |s| !s.0)
            .unwrap();
        assert!(state.0 && !timeout.timed_out());
    }
    fn open(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
struct Host {
    root: Mutex<Option<Arc<RootResultChannel>>>,
    reporter: Mutex<Option<TaskStatusReporter>>,
    gate: Option<Arc<Gate>>,
}
impl TaskExecutionHost for Host {
    fn close_context_admission(&self, _: QueryContextRef) {}
    fn retire_context_execution(&self, _: QueryContextRef) {}
    fn forget_context_admission(&self, _: QueryContextRef) {}
    fn install_receiver(
        &self,
        _: &TaskDescriptor,
        _: TaskCreationInput,
        _preparation_stop: novarocks_worker::TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        if let Some(gate) = &self.gate {
            gate.wait();
        }
        PreparedTaskInstallation::new(
            PreparedTaskFacts::new(FragmentSinkKind::Result),
            self.root.lock().unwrap().take(),
        )
    }
    fn remove_receiver(&self, _: &TaskDescriptor) {}
    fn install_inbound_capability(&self, _: &TaskDescriptor) -> Result<(), HostRejection> {
        Ok(())
    }
    fn remove_inbound_capability(&self, _: &TaskDescriptor) {}
    fn submit_runnable(
        &self,
        _: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        *self.reporter.lock().unwrap() = Some(reporter);
        Ok(Arc::new(Runnable))
    }
    fn apply_task_domain(
        &self,
        _: &TaskDescriptor,
        _: &TaskDomainUpdate,
        _operation: novarocks_execution_contract::TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        Ok(None)
    }
}
struct Fixture {
    registry: Arc<TaskExecutionRegistry>,
    reader: NativeRootResultReader,
    root: Arc<RootResultChannel>,
    budget: Arc<ResultRetainedBudget>,
    host: Arc<Host>,
    clock: Arc<ManualClock>,
    context: QueryContextRef,
}
impl Fixture {
    fn new(count_only: bool) -> Self {
        let fixture = Self::uninstalled(count_only, None);
        fixture.install();
        fixture
    }
    fn uninstalled(count_only: bool, gate: Option<Arc<Gate>>) -> Self {
        let backend = BackendProcessId::new_v7();
        let execution =
            QueryExecutionId::new(QueryId::new(32100, 32101), AttemptId::new(1).unwrap()).unwrap();
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        );
        let output = if count_only {
            FrozenRootOutput::CountOnly
        } else {
            FrozenRootOutput::ClientRows(
                ClientRenderSchema::try_new(
                    novarocks_result_contract::ClientPresentationFamily::MysqlText,
                    vec![RenderColumn {
                        source_ordinal: 0,
                        source_slot: Some(1),
                        name: "v".into(),
                        field: RenderField {
                            presentation: RenderPresentation::ScalarText,
                            nullable: false,
                            native_type: NativeRenderType::SignedInteger(64),
                        },
                    }],
                    1,
                )
                .unwrap(),
            )
        };
        let limits = WorkerResultRetainedLimits::try_new(256 * 1024 * 1024, PROCESS).unwrap();
        let budget = ResultRetainedBudget::new(limits.per_process());
        let health =
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default());
        let root = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: identity,
                contract: Arc::new(RootOutputContract::new(RootProfileId::V1, output)),
            },
            Arc::clone(&budget),
            limits,
            Arc::clone(&health),
        )
        .unwrap();
        let host = Arc::new(Host {
            root: Mutex::new(Some(Arc::clone(&root))),
            reporter: Mutex::new(None),
            gate,
        });
        let clock = Arc::new(ManualClock::new());
        let registry = TaskExecutionRegistry::new(
            TaskExecutionRegistryConfig::for_process(backend, 17, 9),
            Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
            Arc::new(ContextHost),
            Arc::clone(&host) as Arc<dyn TaskExecutionHost>,
            TaskExecutionPorts::new(Arc::new(Noop), Arc::new(Noop), Arc::new(Noop), health),
        );
        let lease = || LeaseValidFor::new(Duration::from_secs(10)).unwrap();
        let ticket = registry
            .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
                TaskOperationId::new_v7(),
                context,
                lease(),
                NativeCompatibilityId::new([0x71; 32]),
                registry.admission_epoch_capability(),
            ))
            .acknowledgement()
            .unwrap()
            .ticket_id();
        assert_eq!(
            registry
                .update_query_context(&UpdateQueryContext::Establish(EstablishQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    ticket,
                    Arc::new(Content),
                    Arc::new(Content),
                    Arc::new(Content),
                    CredentialUpdate::new(
                        CredentialLeaseId::new(1),
                        CredentialEpoch::FIRST,
                        Arc::new(Secret)
                    ),
                    lease()
                )))
                .outcome(),
            OperationOutcome::Accepted
        );
        Self {
            reader: NativeRootResultReader::new(Arc::clone(&registry)),
            registry,
            root,
            budget,
            host,
            clock,
            context,
        }
    }
    fn install(&self) {
        let descriptor = TaskDescriptor::try_new(
            self.root.spec().task,
            UniqueId::new(1, 1),
            NonZeroUsize::new(1).unwrap(),
            vec![PlanNodeId::new(1).unwrap()],
            ExchangeTopology::default(),
        )
        .unwrap();
        let create = CreateTask::try_new(
            TaskOperationId::new_v7(),
            self.context,
            descriptor,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            self.registry
                .create_task(
                    &create,
                    TaskCreationInput::new(
                        FrozenBytes::freeze(Bytes::from_static(b"root")),
                        Box::new(Assignment)
                    )
                )
                .outcome(),
            OperationOutcome::Accepted
        );
    }
    fn request(&self, wanted: Option<u64>, consumed: u64, wait: u64) -> RootResultRead {
        RootResultRead::try_new(
            self.root.spec().task,
            self.root.spec().contract.profile(),
            self.root.spec().contract.kind(),
            wanted.map(|w| NonZeroU64::new(w).unwrap()),
            consumed,
            Duration::from_millis(wait),
        )
        .unwrap()
    }
    fn finish(&self) {
        let producer = self.root.start_producer().unwrap();
        self.root.note_rows(1).unwrap();
        self.root.request_finish().unwrap();
        if self.root.spec().contract.kind() == RootOutputKind::CountOnly {
            self.root.publish_end().unwrap();
        } else {
            let mut builder = self.root.try_segment().unwrap().unwrap();
            builder.output()[..6].copy_from_slice(b"\x02\0\0\0\x011");
            self.root.publish_segment(builder, 6, true).unwrap();
        }
        producer.finish_encoder();
        let reporter = self.host.reporter.lock().unwrap().as_ref().unwrap().clone();
        reporter.running();
        reporter.finished(TaskOutputFacts::new(true));
        reporter.release_output();
        reporter.note_actual_stopped();
        reporter.note_resources_converged();
        assert_eq!(self.registry.advance_deadlines().tasks_retired, 1);
    }
    fn fill_process(&self, bytes: usize) -> ResultWriteCredit {
        granted(self.budget.try_reserve_process(bytes).unwrap())
    }
    fn seal(&self) {
        assert_eq!(
            self.registry
                .quiesce_query_context(&QuiesceQueryContext::new(
                    TaskOperationId::new_v7(),
                    self.context
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
        assert_eq!(
            self.registry
                .release_query_context(&ReleaseQueryContext::new(
                    TaskOperationId::new_v7(),
                    self.context
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
    }
}
fn granted(admission: ResultWriteAdmission) -> ResultWriteCredit {
    match admission {
        ResultWriteAdmission::Granted(credit) => credit,
        ResultWriteAdmission::Blocked => panic!("expected capacity"),
    }
}
fn owned(response: NativeRootReadResponse) -> NativeRootResultReply {
    match response {
        NativeRootReadResponse::Owned(reply) => reply,
        NativeRootReadResponse::Deferred(reason) => panic!("read deferred: {reason:?}"),
        NativeRootReadResponse::Refused(reason) => panic!("read refused: {reason:?}"),
        NativeRootReadResponse::AwaitTerminalControl { accepted_consumed } => {
            panic!("sealed at {accepted_consumed}")
        }
    }
}
fn assert_binding(response: &RootReadResponse, request: &RootResultRead) {
    assert_eq!(response.root_task(), request.root_task());
    assert_eq!(response.profile(), request.profile());
    assert_eq!(response.kind(), request.kind());
    response.validate().unwrap();
}
fn deferred(response: NativeRootReadResponse, request: &RootResultRead) -> RootReadDeferral {
    let NativeRootReadResponse::Deferred(deferred) = response else {
        panic!("expected an actual deferral without an ACK receipt");
    };
    let response = RootReadResponse::Deferred(deferred);
    assert_binding(&response, request);
    let RootReadResponse::Deferred(deferred) = response else {
        unreachable!()
    };
    deferred.reason()
}
fn refused(response: NativeRootReadResponse, request: &RootResultRead) -> RootReadRefusal {
    let NativeRootReadResponse::Refused(refused) = response else {
        panic!("expected a domain refusal without an ACK receipt");
    };
    assert_binding(&RootReadResponse::Refused(refused.clone()), request);
    *refused
}
fn read_holders_busy() -> RootReadDeferral {
    let positions = NativeResultSupportGeometry::V1.root_live_send_holders;
    RootReadDeferral::ReadHoldersBusy(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: Some(ResourceScope::Task),
        dimension: ResourceDimension::Positions,
        requested: Some(1),
        required_growth: None,
        limit: Some(positions),
        live: None,
        committed: Some(positions),
    })
}
fn send_backing_busy() -> RootReadDeferral {
    RootReadDeferral::SendBackingBusy(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: None,
        dimension: ResourceDimension::Bytes,
        requested: Some(COPY as u64),
        required_growth: None,
        limit: None,
        live: None,
        committed: None,
    })
}
fn send_metadata_busy(requested: usize, committed: usize) -> RootReadDeferral {
    RootReadDeferral::SendMetadataBusy(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: Some(ResourceScope::Task),
        dimension: ResourceDimension::Bytes,
        requested: Some(requested as u64),
        required_growth: None,
        limit: Some(FIXED as u64),
        live: None,
        committed: Some(committed as u64),
    })
}
fn root_refusal_fact(code: u16) -> FailureFact {
    FailureFact::new(
        FailureClass::Data,
        FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(code).unwrap()),
    )
}

#[cfg(all(novarocks_dev_controls, unix))]
mod root_observation_tests {
    use super::*;
    use novarocks_execution::runtime::exchange::TestEventObserver;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession},
        scope::ProcessBinding,
        session::{DomainGrant, SessionDeclaration, SessionRole},
    };
    use novarocks_native_adapter::test_observation_controls::{DOMAIN, NativeObservationControls};
    use serde_json::json;

    struct Session {
        _directory: tempfile::TempDir,
        role: RoleSessionOwner,
        runner: RunnerSession,
        controls: Arc<NativeObservationControls>,
        observer: Arc<dyn TestEventObserver>,
    }

    impl Session {
        fn new(task: TaskIdentity) -> Self {
            let directory = tempfile::Builder::new()
                .prefix("nr-root-ob-")
                .tempdir_in("/tmp")
                .unwrap();
            let root = directory.path();
            let execution = task.query_execution_id();
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "root-observation-be".into(),
                launch_id: "root-observation-launch".into(),
                role: SessionRole::Be,
                lifetime_millis: 3000,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "r".repeat(32),
                domain_grants: vec![DomainGrant {
                    domain: DOMAIN.into(),
                    grant_id: "root-observation".into(),
                    parameters: json!({
                        "action": "grpc-fragment", "root": root, "backend_index": 0,
                        "target_session_id": "root-observation-be", "max_events": 1,
                        "query_execution": {
                            "query_id_hi": execution.query_id().high(),
                            "query_id_lo": execution.query_id().low(),
                            "attempt_id": execution.attempt_id().get(),
                        },
                    }),
                }],
            };
            let path = root.join("session.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Be).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let role = prepared
                .bind(ProcessBinding::Backend(task.backend_process_id()))
                .unwrap();
            let controls = Arc::new(
                NativeObservationControls::from_grants(role.scope().grants_for(DOMAIN)).unwrap(),
            );
            let observer = controls
                .observation_for("grpc-fragment", execution)
                .unwrap()
                .unwrap();
            Self {
                _directory: directory,
                role,
                runner,
                controls,
                observer,
            }
        }

        fn reader(&self, fixture: &Fixture) -> NativeRootResultReader {
            NativeRootResultReader::new(Arc::clone(&fixture.registry))
                .with_observation_controls(Some(Arc::clone(&self.controls)))
        }

        fn assert_observed_once(&self) {
            assert!(
                !self.observer.is_active(),
                "the one finite event was consumed"
            );
            assert_eq!(
                self.role
                    .scope()
                    .observations()
                    .iter()
                    .filter(|event| event.event == "hit")
                    .count(),
                1,
                "a real selected observation, rather than session expiry, consumed the grant",
            );
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }

    #[tokio::test]
    async fn root_end_read_uses_original_authenticated_observation_grant() {
        let fixture = Fixture::new(true);
        fixture.finish();
        let session = Session::new(fixture.root.spec().task);
        let reply = owned(
            session
                .reader(&fixture)
                .read(&fixture.request(Some(1), 0, 1))
                .await,
        );
        assert!(matches!(reply.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 1));
        session.assert_observed_once();
        drop(reply);
        assert!(fixture.root.physical_idle());
    }

    #[tokio::test]
    async fn root_data_read_preserves_real_payload_and_attached_end_while_observing() {
        let fixture = Fixture::new(false);
        fixture.finish();
        let session = Session::new(fixture.root.spec().task);
        let reply = owned(
            session
                .reader(&fixture)
                .read(&fixture.request(Some(1), 0, 1))
                .await,
        );
        let RootReadOutcome::Data(data) = &reply.reply().outcome else {
            panic!("expected original Data")
        };
        assert_eq!(data.body().as_ref(), b"\x02\0\0\0\x011");
        assert_eq!(data.end_after_data().unwrap().output_rows, 1);
        session.assert_observed_once();
        drop(reply);
    }

    #[tokio::test]
    async fn root_refusal_neither_observes_nor_replaces_original_failure() {
        let fixture = Fixture::new(false);
        let task = fixture.root.spec().task;
        let session = Session::new(task);
        let request = RootResultRead::try_new(
            task,
            RootProfileId::V1,
            RootOutputKind::CountOnly,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1),
        )
        .unwrap();
        let refusal = refused(session.reader(&fixture).read(&request).await, &request);
        assert_eq!(refusal.fact(), root_refusal_fact(43));
        assert!(session.observer.is_active());
        assert!(session.role.scope().observations().is_empty());
        assert!(fixture.root.physical_idle());
    }

    #[tokio::test]
    async fn admitted_and_late_await_control_do_not_consume_stream_observation() {
        let fixture = Fixture::new(false);
        fixture.finish();
        drop(owned(
            fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
        ));
        drop(owned(
            fixture.reader.read(&fixture.request(None, 1, 1)).await,
        ));
        let session = Session::new(fixture.root.spec().task);
        let reader = session.reader(&fixture);
        let request = fixture.request(Some(3), 1, 1000);
        let mut read = Box::pin(reader.read(&request));
        pending(read.as_mut()).await;
        fixture.seal();
        assert_eq!(
            fixture.registry.context_state(fixture.context),
            QueryContextState::Releasing
        );
        let reply = owned(
            tokio::time::timeout(Duration::from_millis(100), read)
                .await
                .unwrap(),
        );
        assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
        assert_eq!(reply.reply().accepted_consumed, 1);
        assert!(session.observer.is_active());
        assert!(session.role.scope().observations().is_empty());
        assert!(matches!(
            reader.read(&fixture.request(Some(3), 2, 1)).await,
            NativeRootReadResponse::AwaitTerminalControl {
                accepted_consumed: 1
            }
        ));
        assert!(session.observer.is_active());
        assert!(session.role.scope().observations().is_empty());
        assert_eq!(fixture.root.snapshot().consumed_through, 1);
        drop(reply);
        fixture.registry.advance_deadlines();
        assert_eq!(
            fixture.registry.context_state(fixture.context),
            QueryContextState::TerminalRetained
        );
        assert!(fixture.root.physical_idle());
    }

    #[tokio::test]
    async fn root_not_ready_and_ack_only_do_not_consume_an_eos_observation() {
        let fixture = Fixture::new(false);
        let session = Session::new(fixture.root.spec().task);
        let reader = session.reader(&fixture);
        let reply = owned(reader.read(&fixture.request(Some(1), 0, 1)).await);
        assert_eq!(reply.reply().outcome, RootReadOutcome::NotReady);
        drop(reply);
        let reply = owned(reader.read(&fixture.request(None, 0, 1)).await);
        assert_eq!(reply.reply().outcome, RootReadOutcome::AckOnly);
        drop(reply);
        assert!(session.observer.is_active());
        assert!(session.role.scope().observations().is_empty());
        assert!(fixture.root.physical_idle());
    }

    #[tokio::test]
    async fn root_observation_does_not_widen_to_another_backend_or_attempt() {
        let fixture = Fixture::new(true);
        fixture.finish();
        let task = fixture.root.spec().task;
        let foreign_backend = TaskIdentity::new(
            task.query_execution_id(),
            task.stage_id(),
            task.task_id(),
            BackendProcessId::new_v7(),
        );
        let foreign_attempt = TaskIdentity::new(
            QueryExecutionId::new(
                task.query_execution_id().query_id(),
                AttemptId::new(2).unwrap(),
            )
            .unwrap(),
            task.stage_id(),
            task.task_id(),
            task.backend_process_id(),
        );
        for foreign in [foreign_backend, foreign_attempt] {
            let session = Session::new(foreign);
            let reply = owned(
                session
                    .reader(&fixture)
                    .read(&fixture.request(Some(1), 0, 1))
                    .await,
            );
            assert!(matches!(reply.reply().outcome, RootReadOutcome::End(_)));
            assert!(session.observer.is_active());
            assert!(session.role.scope().observations().is_empty());
            drop(reply);
        }
    }
}
async fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn context_read_survives_task_retirement_and_horizon() {
    let fixture = Fixture::new(false);
    fixture.finish();
    for sequence in 1..=25 {
        fixture.clock.advance(Duration::from_secs(5));
        assert_eq!(
            fixture
                .registry
                .update_query_context(&UpdateQueryContext::RenewLease(
                    RenewQueryExecutionLease::new(
                        TaskOperationId::new_v7(),
                        fixture.context,
                        LeaseSequence::new(sequence),
                        LeaseValidFor::new(Duration::from_secs(10)).unwrap()
                    )
                ))
                .outcome(),
            OperationOutcome::Accepted
        );
        fixture.registry.advance_deadlines();
    }
    let request = fixture.request(Some(1), 0, 1);
    for _ in 0..2 {
        let reply = owned(fixture.reader.read(&request).await);
        let RootReadOutcome::Data(data) = &reply.reply().outcome else {
            panic!("expected Data")
        };
        assert_eq!(data.body().as_ref(), b"\x02\0\0\0\x011");
        assert_eq!(data.end_after_data().unwrap().output_rows, 1);
        assert_eq!(reply.reply().accepted_consumed, 0);
    }
    let ack = owned(fixture.reader.read(&fixture.request(None, 2, 1)).await);
    assert_eq!(ack.reply().accepted_consumed, 2);
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
}

#[tokio::test]
async fn count_only_end_and_ack_only_use_typed_reply() {
    let fixture = Fixture::new(true);
    fixture.finish();
    let end = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    assert!(matches!(end.reply().outcome, RootReadOutcome::End(e) if e.output_rows == 1));
    drop(end);
    let ack = owned(fixture.reader.read(&fixture.request(None, 1, 1)).await);
    assert_eq!(
        ack.ownership().backing_capacity_bytes(),
        2 * RootProfileV1::ENVELOPE_BYTES
    );
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
}

#[tokio::test]
async fn long_poll_cancellation_releases_original_admission_and_pregrants() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1000);
    let mut read = Box::pin(fixture.reader.read(&request));
    pending(read.as_mut()).await;
    assert!(!fixture.root.physical_idle());
    drop(read);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(PROCESS - FIXED));
}

#[tokio::test]
async fn two_send_positions_follow_last_exported_owner() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1);
    let first = owned(fixture.reader.read(&request).await);
    let owner = first.ownership();
    let second = owned(fixture.reader.read(&request).await);
    drop(first);
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        read_holders_busy()
    );
    drop(owner);
    let third = owned(fixture.reader.read(&request).await);
    drop(second);
    drop(third);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn full_copy_credit_stays_until_actual_owner_drop() {
    let fixture = Fixture::new(false);
    let reply = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    let owner = reply.ownership();
    assert_eq!(owner.backing_capacity_bytes(), COPY);
    drop(reply);
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    assert!(matches!(
        fixture.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    assert!(!fixture.root.physical_idle());
    drop(owner);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn shared_process_and_root_pregrants_refuse_without_leaking_read_positions() {
    let fixture = Fixture::new(false);
    let request = fixture.request(Some(1), 0, 1);
    let process = fixture.fill_process(PROCESS - FIXED - COPY + 1);
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        send_backing_busy()
    );
    assert!(fixture.root.physical_idle());
    drop(process);
    let root = granted(
        fixture
            .root
            .try_reserve(256 * 1024 * 1024 - FIXED - COPY + 1)
            .unwrap(),
    );
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        send_backing_busy()
    );
    drop(root);
    drop(owned(fixture.reader.read(&request).await));
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn full_budget_ack_only_uses_fixed_control_metadata() {
    let fixture = Fixture::new(false);
    fixture.finish();
    let data = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    drop(data);
    let filler = fixture.fill_process(PROCESS - FIXED - SEGMENT);
    let ack = owned(fixture.reader.read(&fixture.request(None, 1, 1)).await);
    assert_eq!(ack.reply().accepted_consumed, 1);
    assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
    assert_eq!(
        ack.ownership().backing_capacity_bytes(),
        2 * RootProfileV1::ENVELOPE_BYTES
    );
    drop(ack);
    drop(fixture.fill_process(SEGMENT));
    drop(filler);
}

#[tokio::test]
async fn fixed_metadata_refusal_does_not_apply_ack_or_keep_admission() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    // Reserve almost all fixed metadata, independently of data capacity.
    let metadata = fixture.root.try_reserve_metadata(FIXED - 8192).unwrap();
    let request = fixture.request(None, 1, 1);
    let committed = fixture
        .root
        .try_ownership_snapshot()
        .unwrap()
        .unwrap()
        .metadata_bytes;
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        send_metadata_busy(
            novarocks_native_adapter::root_result_reader::native_root_send_metadata_bytes()
                + 2 * RootProfileV1::ENVELOPE_BYTES,
            committed,
        )
    );
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
    drop(metadata);
    assert!(
        !fixture.root.physical_idle(),
        "unacknowledged Data remains owned"
    );
    let ack = owned(fixture.reader.read(&request).await);
    assert_eq!(ack.reply().accepted_consumed, 1);
    drop(ack);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(PROCESS - FIXED));
}

#[tokio::test]
async fn two_ack_only_sends_share_one_fixed_reservation_each() {
    let fixture = Fixture::new(false);
    let filler = fixture.fill_process(PROCESS - FIXED);
    let request = fixture.request(None, 0, 1);
    let first = owned(fixture.reader.read(&request).await);
    let second = owned(fixture.reader.read(&request).await);
    assert_eq!(first.ownership().backing_capacity_bytes(), 8192);
    assert_eq!(second.ownership().backing_capacity_bytes(), 8192);
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        read_holders_busy()
    );
    drop(first);
    drop(second);
    drop(filler);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn ack_and_fetch_retire_original_backing_before_copy_pregrant() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    let end = owned(fixture.reader.read(&fixture.request(Some(2), 1, 1)).await);
    assert_eq!(end.reply().accepted_consumed, 1);
    assert!(matches!(end.reply().outcome, RootReadOutcome::End(_)));
    drop(end);
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn acknowledged_original_alias_still_blocks_copy_pregrant() {
    let fixture = Fixture::new(false);
    fixture.finish();
    let reply = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    let alias = match &reply.reply().outcome {
        RootReadOutcome::Data(data) => data.body().clone(),
        _ => panic!("Data"),
    };
    drop(reply);
    let filler = fixture.fill_process(PROCESS - FIXED - COPY);
    let request = fixture.request(Some(2), 1, 1);
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        send_backing_busy()
    );
    assert_eq!(
        fixture.root.snapshot().consumed_through,
        1,
        "ACK applied despite copy refusal"
    );
    drop(alias);
    let end = owned(fixture.reader.read(&request).await);
    assert_eq!(end.reply().accepted_consumed, 1);
    drop(end);
    drop(filler);
}

#[tokio::test]
async fn release_wakes_admitted_long_poll_and_late_route_uses_real_ack() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    drop(owned(
        fixture.reader.read(&fixture.request(None, 1, 1)).await,
    ));
    let request = fixture.request(Some(3), 1, 1000);
    let mut read = Box::pin(fixture.reader.read(&request));
    pending(read.as_mut()).await;
    fixture.seal();
    assert_eq!(
        fixture.registry.context_state(fixture.context),
        QueryContextState::Releasing
    );
    let reply = owned(
        tokio::time::timeout(Duration::from_millis(100), read)
            .await
            .unwrap(),
    );
    assert_eq!(reply.reply().outcome, RootReadOutcome::AwaitTerminalControl);
    assert_eq!(reply.reply().accepted_consumed, 1);
    assert!(matches!(
        fixture.reader.read(&fixture.request(Some(3), 2, 1)).await,
        NativeRootReadResponse::AwaitTerminalControl {
            accepted_consumed: 1
        }
    ));
    assert_eq!(fixture.root.snapshot().consumed_through, 1);
    drop(reply);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(fixture.context),
        QueryContextState::TerminalRetained
    );
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn ack_retirement_callback_seals_before_send_growth_and_returns_actual_frontier() {
    let fixture = Fixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let fired = Arc::new(AtomicBool::new(false));
    let once = Arc::clone(&fired);
    let registry = Arc::downgrade(&fixture.registry);
    let context = fixture.context;
    let subscription = fixture
        .root
        .writable_observable()
        .try_subscribe(Arc::new(move || {
            if once.swap(true, Ordering::AcqRel) {
                return;
            }
            let registry = registry.upgrade().unwrap();
            assert_eq!(
                registry
                    .quiesce_query_context(&QuiesceQueryContext::new(
                        TaskOperationId::new_v7(),
                        context
                    ))
                    .outcome(),
                OperationOutcome::Accepted
            );
            assert_eq!(
                registry
                    .release_query_context(&ReleaseQueryContext::new(
                        TaskOperationId::new_v7(),
                        context
                    ))
                    .outcome(),
                OperationOutcome::Accepted
            );
        }))
        .unwrap();
    assert!(matches!(
        fixture.reader.read(&fixture.request(Some(2), 1, 1)).await,
        NativeRootReadResponse::AwaitTerminalControl {
            accepted_consumed: 1
        }
    ));
    assert!(fired.load(Ordering::Acquire));
    assert_eq!(fixture.root.snapshot().consumed_through, 1);
    assert!(
        fixture.root.physical_idle(),
        "original admission and metadata rolled back"
    );
    drop(subscription);
    fixture.registry.advance_deadlines();
    assert_eq!(
        fixture.registry.context_state(context),
        QueryContextState::TerminalRetained
    );
}

#[tokio::test]
async fn identity_and_output_mismatch_are_typed_without_fallback() {
    let fixture = Fixture::new(false);
    let task = fixture.root.spec().task;
    let unknown = TaskIdentity::new(
        task.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(2).unwrap(),
        task.backend_process_id(),
    );
    let request = RootResultRead::try_new(
        unknown,
        RootProfileId::V1,
        RootOutputKind::ClientRows,
        NonZeroU64::new(1),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    let refusal = refused(fixture.reader.read(&request).await, &request);
    assert_eq!(refusal.fact(), root_refusal_fact(165));
    assert!(refusal.detail().as_str().len() <= 512);
    let request = RootResultRead::try_new(
        task,
        RootProfileId::V1,
        RootOutputKind::CountOnly,
        NonZeroU64::new(1),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    let refusal = refused(fixture.reader.read(&request).await, &request);
    assert_eq!(refusal.fact(), root_refusal_fact(43));
    assert!(refusal.detail().as_str().len() <= 512);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn installation_in_progress_is_preparing_without_new_read_holder() {
    let gate = Arc::new(Gate::default());
    let fixture = Arc::new(Fixture::uninstalled(false, Some(Arc::clone(&gate))));
    let creator = Arc::clone(&fixture);
    let job = std::thread::spawn(move || creator.install());
    gate.entered();
    let request = fixture.request(Some(1), 0, 1);
    assert_eq!(
        deferred(fixture.reader.read(&request).await, &request),
        RootReadDeferral::Preparing,
    );
    assert!(fixture.root.physical_idle());
    let mut body = root_result_unary(&fixture.reader, unary_request(&request))
        .await
        .into_body();
    let data = unary_data(&mut body).await;
    assert_eq!(
        unary_decoded_response(&data, &request, 0),
        RootReadResponse::Deferred(
            novarocks_execution_contract::root_result::RootReadDeferred::for_request(
                &request,
                RootReadDeferral::Preparing,
            )
        ),
    );
    assert!(fixture.root.physical_idle());
    drop(data);
    drop(body);
    gate.open();
    job.join().unwrap();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
}

// These probes concern Tonic's post-root-admission allocations. No listener
// or H2 connection is installed, so they make no pre-decode lane claim.
use hyper::body::Body as HttpBody;
use novarocks_native_adapter::root_result_unary::{NativeRootUnaryBody, root_result_unary};
use novarocks_proto_models::novarocks as wire;
use prost::Message;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

// Response header maps are ordinary upstream Tonic/HTTP allocations. They are
// third-party internals bounded by the listener's header configuration and the
// transport measurement gate, not by a root grant, so this fixture charges none.
struct UnaryFixture {
    inner: Fixture,
}
impl std::ops::Deref for UnaryFixture {
    type Target = Fixture;
    fn deref(&self) -> &Fixture {
        &self.inner
    }
}
impl UnaryFixture {
    fn new(count_only: bool) -> Self {
        Self {
            inner: Fixture::new(count_only),
        }
    }
    fn header_bytes(&self) -> usize {
        0
    }
}

// Same-thread component oracle only. No allocator hook allocates, logs, locks,
// panics, or retains any body bytes. Eight actual allocations are the hard cap.
const PHYSICAL_RECORD_CAP: usize = 8;
#[derive(Clone, Copy, Debug, Default)]
struct PhysicalAllocation {
    pointer: usize,
    bytes: usize,
    align: usize,
    dealloc_returned: bool,
}
impl PhysicalAllocation {
    const EMPTY: Self = Self {
        pointer: 0,
        bytes: 0,
        align: 0,
        dealloc_returned: false,
    };
}
#[derive(Clone, Copy, Debug)]
struct PhysicalProbe {
    selected_size: usize,
    records: [PhysicalAllocation; PHYSICAL_RECORD_CAP],
    used: usize,
    invalid: bool,
}
impl PhysicalProbe {
    const EMPTY: Self = Self {
        selected_size: 0,
        records: [PhysicalAllocation::EMPTY; PHYSICAL_RECORD_CAP],
        used: 0,
        invalid: false,
    };
    fn allocated(&mut self, pointer: *mut u8, layout: Layout) {
        if self.selected_size == 0 || self.selected_size != layout.size() {
            return;
        }
        if pointer.is_null()
            || self.used == PHYSICAL_RECORD_CAP
            || self.records[..self.used]
                .iter()
                .any(|record| record.pointer == pointer as usize && !record.dealloc_returned)
        {
            self.invalid = true;
            return;
        }
        self.records[self.used] = PhysicalAllocation {
            pointer: pointer as usize,
            bytes: layout.size(),
            align: layout.align(),
            dealloc_returned: false,
        };
        self.used += 1;
    }
    fn live_pointer(&self, pointer: *mut u8) -> Option<usize> {
        self.records[..self.used]
            .iter()
            .position(|record| record.pointer == pointer as usize && !record.dealloc_returned)
    }
    fn reallocated(
        &mut self,
        pointer: *mut u8,
        result: *mut u8,
        layout: Layout,
        size: usize,
    ) -> bool {
        let Some(index) = self.live_pointer(pointer) else {
            return false;
        };
        // Reallocation is never accepted as the no-growth proof. Keep enough
        // exact identity to observe the eventual free, including realloc fail.
        self.invalid = true;
        if !result.is_null() {
            self.records[index].pointer = result as usize;
            self.records[index].bytes = size;
            self.records[index].align = layout.align();
        }
        true
    }
    fn deallocated_after_return(&mut self, pointer: *mut u8, layout: Layout) {
        let Some(index) = self.live_pointer(pointer) else {
            return;
        };
        let record = &mut self.records[index];
        if record.bytes != layout.size() || record.align != layout.align() {
            self.invalid = true;
            return;
        }
        record.dealloc_returned = true;
    }
    fn all_deallocated(&self) -> bool {
        !self.invalid
            && self.used > 0
            && self.records[..self.used]
                .iter()
                .all(|record| record.dealloc_returned)
    }
    fn live_alias(&self, pointer: *const u8, bytes: usize) -> usize {
        assert!(
            !self.invalid,
            "physical oracle overflow or invalid allocation"
        );
        assert!(bytes > 0, "empty alias cannot identify backing");
        let start = pointer as usize;
        let end = start.checked_add(bytes).expect("alias address overflow");
        let mut matched = None;
        for (index, record) in self.records[..self.used].iter().enumerate() {
            if !record.dealloc_returned
                && start >= record.pointer
                && end
                    <= record
                        .pointer
                        .checked_add(record.bytes)
                        .expect("backing address overflow")
            {
                assert!(
                    matched.replace(index).is_none(),
                    "ambiguous physical backing"
                );
            }
        }
        matched.expect("actual alias must belong to one captured live allocation")
    }
}

thread_local! {
    static SEND_PHYSICAL: Cell<PhysicalProbe> = const { Cell::new(PhysicalProbe::EMPTY) };
    static SEND_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    static SEND_REALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    // Compatibility aggregate for existing tests: every selected allocation
    // must have returned from System.dealloc, not merely the last pointer.
    static SEND_FREED: Cell<bool> = const { Cell::new(false) };
}
struct SendAllocationProbe;
#[global_allocator]
static SEND_ALLOCATOR: SendAllocationProbe = SendAllocationProbe;
unsafe impl GlobalAlloc for SendAllocationProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        let _ = SEND_PHYSICAL.try_with(|cell| {
            let mut probe = cell.get();
            let selected = probe.selected_size != 0 && probe.selected_size == layout.size();
            probe.allocated(pointer, layout);
            cell.set(probe);
            if selected {
                let _ = SEND_ALLOC_COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
                let _ = SEND_FREED.try_with(|freed| freed.set(false));
            }
        });
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        let _ = SEND_PHYSICAL.try_with(|cell| {
            let mut probe = cell.get();
            if probe.reallocated(pointer, result, layout, size) {
                let _ =
                    SEND_REALLOC_COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
                let _ = SEND_FREED.try_with(|freed| freed.set(false));
            }
            cell.set(probe);
        });
        result
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        let _ = SEND_PHYSICAL.try_with(|cell| {
            let mut probe = cell.get();
            probe.deallocated_after_return(pointer, layout);
            cell.set(probe);
            let _ = SEND_FREED.try_with(|freed| freed.set(probe.all_deallocated()));
        });
    }
}
fn start_send_probe(capacity: usize) {
    SEND_PHYSICAL.with(|cell| {
        let mut probe = PhysicalProbe::EMPTY;
        probe.selected_size = capacity;
        cell.set(probe);
    });
    SEND_ALLOC_COUNT.with(|value| value.set(0));
    SEND_REALLOC_COUNT.with(|value| value.set(0));
    SEND_FREED.with(|value| value.set(false));
}
fn stop_send_probe() {
    SEND_PHYSICAL.with(|cell| {
        let mut probe = cell.get();
        probe.selected_size = 0;
        cell.set(probe);
    });
}
fn send_snapshot() -> PhysicalProbe {
    SEND_PHYSICAL.with(Cell::get)
}
struct SendCapture;
impl SendCapture {
    fn begin(bytes: usize) -> Self {
        start_send_probe(bytes);
        Self
    }
    fn stop(&self) {
        stop_send_probe();
    }
}
impl Drop for SendCapture {
    fn drop(&mut self) {
        stop_send_probe();
    }
}
fn unary_request(read: &RootResultRead) -> axum::http::Request<axum::body::Body> {
    let message = novarocks_task_codec::root_result::encode_read(read);
    let mut bytes = Vec::with_capacity(5 + message.encoded_len());
    bytes.push(0);
    bytes.extend_from_slice(&(message.encoded_len() as u32).to_be_bytes());
    message.encode(&mut bytes).unwrap();
    axum::http::Request::builder()
        .uri(
            novarocks_proto_codec::native_rpc::NativeRpcMethod::FetchRootResult
                .contract()
                .path,
        )
        .header("content-type", "application/grpc")
        .body(axum::body::Body::from(bytes))
        .unwrap()
}
async fn unary_data(body: &mut NativeRootUnaryBody) -> Bytes {
    poll_fn(|cx| std::pin::Pin::new(&mut *body).poll_frame(cx))
        .await
        .expect("one unary frame")
        .expect("valid encoded frame")
        .into_data()
        .expect("first frame is DATA")
}
fn unary_message(data: &Bytes) -> wire::FetchRootResultResponse {
    assert_eq!(data[0], 0, "compression is disabled");
    assert_eq!(
        u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize,
        data.len() - 5
    );
    Message::decode(&data[5..]).unwrap()
}

fn unary_receipt(message: wire::FetchRootResultResponse) -> wire::RootReadReceipt {
    match message.result {
        Some(wire::fetch_root_result_response::Result::Reply(receipt)) => receipt,
        other => panic!("expected a wire ACK receipt, got {other:?}"),
    }
}
fn unary_decoded_response(
    data: &Bytes,
    request: &RootResultRead,
    maximum_delivered_consumed: u64,
) -> RootReadResponse {
    let raw = &data[5..];
    novarocks_task_codec::resource_preflight::check_root_result_reply(raw).unwrap();
    let response = novarocks_task_codec::root_result::decode_response(
        unary_message(data),
        request,
        maximum_delivered_consumed,
        novarocks_proto_codec::FieldPath::root("root_reply"),
    )
    .unwrap();
    assert_binding(&response, request);
    response
}

#[tokio::test]
async fn unary_unpolled_body_keeps_preallocated_buffer_and_full_credit() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    start_send_probe(SEGMENT);
    let response = root_result_unary(
        &fixture.reader,
        unary_request(&fixture.request(Some(1), 0, 1)),
    )
    .await;
    stop_send_probe();
    assert_eq!(
        SEND_ALLOC_COUNT.with(Cell::get),
        1,
        "one buffer before first body poll"
    );
    assert_eq!(SEND_REALLOC_COUNT.with(Cell::get), 0);
    assert!(!SEND_FREED.with(Cell::get));
    let filler = fixture.fill_process(PROCESS - FIXED - SEGMENT - COPY - fixture.header_bytes());
    assert!(matches!(
        fixture.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(response);
    assert!(
        SEND_FREED.with(Cell::get),
        "actual buffer exits before credit can be reused"
    );
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn unary_last_data_slice_outlives_body_and_context_seal() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    start_send_probe(SEGMENT);
    let response = root_result_unary(
        &fixture.reader,
        unary_request(&fixture.request(Some(1), 0, 1)),
    )
    .await;
    stop_send_probe();
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    let message = unary_message(&data);
    let RootReadResponse::Reply(decoded) = novarocks_task_codec::root_result::decode_response(
        message,
        &fixture.request(Some(1), 0, 1),
        0,
        novarocks_proto_codec::FieldPath::root("root_reply"),
    )
    .unwrap() else {
        panic!("expected an actual ACK receipt");
    };
    let RootReadOutcome::Data(payload) = decoded.outcome else {
        panic!("Data")
    };
    assert_eq!(payload.body().as_ref(), b"\x02\0\0\0\x011");
    assert_eq!(payload.end_after_data().unwrap().output_rows, 1);
    drop(payload);
    let tail = data.slice(5..);
    drop(data);
    fixture.seal();
    drop(body);
    assert!(!SEND_FREED.with(Cell::get));
    assert!(!fixture.root.physical_idle());
    let filler = fixture.fill_process(PROCESS - FIXED - COPY - fixture.header_bytes());
    assert!(matches!(
        fixture.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(tail);
    assert!(SEND_FREED.with(Cell::get));
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn unary_ack_only_encodes_from_metadata_when_process_data_pool_is_full() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let filler = fixture.fill_process(PROCESS - FIXED - SEGMENT - fixture.header_bytes());
    start_send_probe(RootProfileV1::ENVELOPE_BYTES);
    let response =
        root_result_unary(&fixture.reader, unary_request(&fixture.request(None, 1, 1))).await;
    stop_send_probe();
    // This size also selects the separate pre-admission request decoder.
    assert_eq!(SEND_ALLOC_COUNT.with(Cell::get), 2);
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    let message = unary_receipt(unary_message(&data));
    assert_eq!(message.accepted_consumed_sequence, 1);
    assert_eq!(
        message.outcome,
        Some(wire::root_read_receipt::Outcome::AckOnly(true))
    );
    drop(body);
    assert!(!fixture.root.physical_idle());
    drop(data);
    assert!(SEND_FREED.with(Cell::get));
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(SEGMENT));
    drop(filler);
}

#[tokio::test]
async fn unary_extra_metadata_is_pregranted_before_ack_or_projection() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let reader_metadata =
        novarocks_native_adapter::root_result_reader::native_root_send_metadata_bytes()
            + 2 * RootProfileV1::ENVELOPE_BYTES;
    // Core owner layout is independent of the wire envelope. Account the
    // actual already reserved metadata at this cut, then leave exactly the
    // reader-only demand; the unary transport's additional demand must fail.
    let committed_metadata = fixture
        .root
        .try_ownership_snapshot()
        .unwrap()
        .unwrap()
        .metadata_bytes;
    let metadata = fixture
        .root
        .try_reserve_metadata(FIXED - committed_metadata - reader_metadata)
        .unwrap();
    drop(owned(
        fixture.reader.read(&fixture.request(None, 0, 1)).await,
    ));
    let committed = fixture
        .root
        .try_ownership_snapshot()
        .unwrap()
        .unwrap()
        .metadata_bytes;
    let read = fixture.request(None, 1, 1);
    let response = root_result_unary(&fixture.reader, unary_request(&read)).await;
    assert!(!response.headers().contains_key("grpc-status"));
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    let response = unary_decoded_response(&data, &read, 1);
    assert_eq!(response, RootReadResponse::Deferred(
        novarocks_execution_contract::root_result::RootReadDeferred::for_request(
            &read,
            send_metadata_busy(
                reader_metadata + novarocks_native_adapter::root_result_unary::native_root_unary_metadata_bytes(),
                committed,
            ),
        ),
    ));
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
    drop(data);
    drop(body);
    drop(metadata);
    let mut body = root_result_unary(&fixture.reader, unary_request(&fixture.request(None, 1, 1)))
        .await
        .into_body();
    assert_eq!(
        unary_receipt(unary_message(&unary_data(&mut body).await)).accepted_consumed_sequence,
        1
    );
    drop(body);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn unary_cancellation_of_admitted_long_poll_exits_all_original_grants() {
    let fixture = UnaryFixture::new(false);
    let request = unary_request(&fixture.request(Some(1), 0, 300));
    let mut future = Box::pin(root_result_unary(&fixture.reader, request));
    pending(future.as_mut()).await;
    assert!(!fixture.root.physical_idle());
    let filler = fixture.fill_process(PROCESS - FIXED - COPY - fixture.header_bytes());
    assert!(matches!(
        fixture.budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(future);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn unary_closed_route_preserves_actual_ack_without_new_root_holder() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    // Keep the existing context in Releasing rather than its already retired
    // horizon, where lookup correctly becomes UnknownRoot.
    let old_credit = granted(fixture.root.try_reserve(1).unwrap());
    fixture.seal();
    assert_eq!(
        fixture.registry.context_state(fixture.context),
        QueryContextState::Releasing
    );
    let filler = fixture.fill_process(PROCESS - FIXED - 1 - fixture.header_bytes());
    let mut body = root_result_unary(&fixture.reader, unary_request(&fixture.request(None, 1, 1)))
        .await
        .into_body();
    let message = unary_receipt(unary_message(&unary_data(&mut body).await));
    assert_eq!(message.accepted_consumed_sequence, 0);
    assert_eq!(
        message.outcome,
        Some(wire::root_read_receipt::Outcome::AwaitTerminalControl(true))
    );
    drop(old_credit);
    assert!(
        fixture.root.physical_idle(),
        "closed route never reopens root resources"
    );
    drop(body);
    drop(filler);
}

#[tokio::test]
async fn unary_two_bodies_hold_two_read_positions_before_first_poll() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    let read = fixture.request(Some(1), 0, 1);
    let first = root_result_unary(&fixture.reader, unary_request(&read)).await;
    let second = root_result_unary(&fixture.reader, unary_request(&read)).await;
    let third = root_result_unary(&fixture.reader, unary_request(&read)).await;
    assert!(!third.headers().contains_key("grpc-status"));
    let mut third = third.into_body();
    let third_data = unary_data(&mut third).await;
    assert_eq!(
        unary_decoded_response(&third_data, &read, 0),
        RootReadResponse::Deferred(
            novarocks_execution_contract::root_result::RootReadDeferred::for_request(
                &read,
                read_holders_busy(),
            )
        ),
    );
    drop(third_data);
    drop(third);
    drop(first);
    let replacement = root_result_unary(&fixture.reader, unary_request(&read)).await;
    assert!(!replacement.headers().contains_key("grpc-status"));
    drop(second);
    drop(replacement);
    fixture.seal();
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn unary_maximum_segment_is_one_copy_without_buffer_growth() {
    let fixture = UnaryFixture::new(false);
    let producer = fixture.root.start_producer().unwrap();
    fixture.root.note_rows(1).unwrap();
    fixture.root.request_finish().unwrap();
    let mut builder = fixture.root.try_segment().unwrap().unwrap();
    let count = RootProfileV1::SEGMENT_BYTES;
    for offset in (0..count).step_by(RootProfileV1::EMIT_BYTES_PER_TURN) {
        builder.output_at(offset).fill(b'x');
    }
    builder.output()[..4].copy_from_slice(&((count - 4) as u32).to_le_bytes());
    fixture.root.publish_segment(builder, count, true).unwrap();
    producer.finish_encoder();
    start_send_probe(SEGMENT);
    let mut body = root_result_unary(
        &fixture.reader,
        unary_request(&fixture.request(Some(1), 0, 1)),
    )
    .await
    .into_body();
    stop_send_probe();
    let data = unary_data(&mut body).await;
    assert_eq!(SEND_ALLOC_COUNT.with(Cell::get), 1);
    assert_eq!(SEND_REALLOC_COUNT.with(Cell::get), 0);
    assert!(data.len() > RootProfileV1::SEGMENT_BYTES && data.len() <= SEGMENT);
    let message = unary_receipt(unary_message(&data));
    let Some(wire::root_read_receipt::Outcome::Data(payload)) = message.outcome else {
        panic!("Data")
    };
    assert_eq!(payload.body.len(), count);
    assert_eq!(payload.end_after_data.unwrap().output_rows, 1);
    let trailers = poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap()
        .into_trailers()
        .unwrap();
    assert_eq!(trailers["grpc-status"], "0");
    assert!(
        poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx))
            .await
            .is_none()
    );
    drop(body);
    drop(data);
    assert!(SEND_FREED.with(Cell::get));
}

#[tokio::test]
async fn unary_oversized_request_is_rejected_before_root_admission() {
    let fixture = UnaryFixture::new(false);
    let size = RootProfileV1::ENVELOPE_BYTES + 1;
    let mut payload = Vec::with_capacity(size + 5);
    payload.push(0);
    payload.extend_from_slice(&(size as u32).to_be_bytes());
    payload.resize(size + 5, 0);
    let request = axum::http::Request::builder()
        .header("content-type", "application/grpc")
        .body(axum::body::Body::from(payload))
        .unwrap();
    let response = root_result_unary(&fixture.reader, request).await;
    assert_eq!(response.headers()["grpc-status"], "11");
    assert!(fixture.root.physical_idle());
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
}

#[tokio::test]
async fn unary_count_only_end_uses_the_same_frozen_codec_without_row_payload() {
    let fixture = UnaryFixture::new(true);
    fixture.finish();
    let read = fixture.request(Some(1), 0, 1);
    let mut body = root_result_unary(&fixture.reader, unary_request(&read))
        .await
        .into_body();
    let message = unary_message(&unary_data(&mut body).await);
    let RootReadResponse::Reply(reply) = novarocks_task_codec::root_result::decode_response(
        message,
        &read,
        0,
        novarocks_proto_codec::FieldPath::root("root_reply"),
    )
    .unwrap() else {
        panic!("expected an actual ACK receipt");
    };
    let RootReadOutcome::End(end) = reply.outcome else {
        panic!("End")
    };
    assert_eq!(end.output_rows, 1);
    drop(body);
    assert!(fixture.root.physical_idle());
}

#[tokio::test]
async fn unary_actual_large_data_backing_survives_all_nonfinal_clone_and_short_slice_drops() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    let capture = SendCapture::begin(SEGMENT);
    let response = root_result_unary(
        &fixture.reader,
        unary_request(&fixture.request(Some(1), 0, 1)),
    )
    .await;
    capture.stop();
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    let open = fixture.root.try_ownership_snapshot().unwrap().unwrap();
    assert_eq!((open.segments, open.deliveries), (1, 1));
    assert!(open.retained_reservations > 0 && open.metadata_holders > 0 && open.metadata_bytes > 0);
    let before = send_snapshot();
    assert!(!before.invalid);
    assert_eq!(before.used, 1);
    assert_eq!(SEND_REALLOC_COUNT.with(Cell::get), 0);
    let allocation = before.live_alias(data.as_ptr(), data.len());
    assert_eq!(before.records[allocation].bytes, SEGMENT);
    // The visible protobuf/wire bytes are genuinely short, but their original
    // encoder allocation is the full granted SEGMENT-sized backing.
    assert!(data.len() > 5 && data.len() < SEGMENT / 1024);
    assert_eq!(before.live_alias(data.as_ptr(), 1), allocation);
    let message = unary_message(&data);
    let RootReadResponse::Reply(decoded) = novarocks_task_codec::root_result::decode_response(
        message,
        &fixture.request(Some(1), 0, 1),
        0,
        novarocks_proto_codec::FieldPath::root("root_reply"),
    )
    .unwrap() else {
        panic!("expected an actual ACK receipt");
    };
    let RootReadOutcome::Data(payload) = decoded.outcome else {
        panic!("Data")
    };
    assert_eq!(payload.body().as_ref(), b"\x02\0\0\0\x011");
    drop(payload);
    let clone = data.clone();
    let short = data.slice(5..6);
    let last = short.clone();
    assert_eq!(last.len(), 1);
    assert_eq!(before.live_alias(last.as_ptr(), last.len()), allocation);
    drop(data);
    fixture.seal();
    drop(body);
    // Encoding already dropped its original segment alias. Seal releases the
    // queued segment while the independent full-copy DATA remains owned.
    let sealed = fixture.root.try_ownership_snapshot().unwrap().unwrap();
    assert_eq!((sealed.segments, sealed.deliveries), (0, 1));
    assert!(
        sealed.retained_reservations > 0
            && sealed.metadata_holders > 0
            && sealed.metadata_bytes > 0
    );
    let filler = fixture.fill_process(PROCESS - FIXED - COPY - fixture.header_bytes());
    for alias in [clone, short] {
        drop(alias);
        let state = send_snapshot();
        assert!(!state.invalid);
        assert!(!state.records[allocation].dealloc_returned);
        assert!(!fixture.root.physical_idle());
        let held = fixture.root.try_ownership_snapshot().unwrap().unwrap();
        assert_eq!((held.segments, held.deliveries), (0, 1));
        assert!(
            held.retained_reservations > 0 && held.metadata_holders > 0 && held.metadata_bytes > 0
        );
        assert!(matches!(
            fixture.budget.try_reserve_process(1).unwrap(),
            ResultWriteAdmission::Blocked
        ));
    }
    assert_eq!(last.len(), 1);
    drop(last);
    let after = send_snapshot();
    assert!(!after.invalid);
    assert!(after.records[allocation].dealloc_returned);
    assert!(after.all_deallocated());
    assert!(fixture.root.physical_idle());
    let exited = fixture.root.try_ownership_snapshot().unwrap().unwrap();
    assert_eq!(
        (
            exited.segments,
            exited.deliveries,
            exited.retained_reservations,
            exited.metadata_holders
        ),
        (0, 0, 0, 0)
    );
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn unary_ack_same_size_decoder_and_send_backings_are_distinct_until_last_data_alias_exit() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    drop(owned(
        fixture.reader.read(&fixture.request(Some(1), 0, 1)).await,
    ));
    let filler = fixture.fill_process(PROCESS - FIXED - SEGMENT - fixture.header_bytes());
    let capture = SendCapture::begin(RootProfileV1::ENVELOPE_BYTES);
    let response =
        root_result_unary(&fixture.reader, unary_request(&fixture.request(None, 1, 1))).await;
    capture.stop();
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    let before = send_snapshot();
    assert!(!before.invalid);
    assert_eq!(
        before.used, 2,
        "actual decoder and send buffers are both recorded"
    );
    assert_eq!(SEND_REALLOC_COUNT.with(Cell::get), 0);
    let send_id = before.live_alias(data.as_ptr(), data.len());
    let decoder_id = 1 - send_id;
    assert!(before.records[decoder_id].dealloc_returned);
    assert!(!before.records[send_id].dealloc_returned);
    // Freed decoder and live send storage may legally reuse the same address.
    // Their distinct ordinal/lifecycle facts, not pointer inequality, identify
    // this response's actual DATA allocation.
    let message = unary_receipt(unary_message(&data));
    assert_eq!(message.accepted_consumed_sequence, 1);
    assert_eq!(
        message.outcome,
        Some(wire::root_read_receipt::Outcome::AckOnly(true))
    );
    let clone = data.clone();
    let last = data.slice(5..6);
    assert_eq!(before.live_alias(last.as_ptr(), last.len()), send_id);
    drop(body);
    drop(data);
    drop(clone);
    let middle = send_snapshot();
    assert!(middle.records[decoder_id].dealloc_returned);
    assert!(!middle.records[send_id].dealloc_returned);
    assert!(!middle.all_deallocated());
    assert!(!fixture.root.physical_idle());
    // ACK retires the data position before projection, so its backing is held
    // by the original fixed metadata reservation, not by process data credit.
    // Do not assert that the already-released process data slot is blocked.
    drop(last);
    let after = send_snapshot();
    assert!(!after.invalid);
    assert!(after.all_deallocated());
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(SEGMENT));
    drop(filler);
}

#[tokio::test]
async fn unary_backing_deferral_has_no_ack_receipt_after_actual_ack_applies() {
    let fixture = UnaryFixture::new(false);
    fixture.finish();
    let original = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
    let RootReadOutcome::Data(data) = &original.reply().outcome else {
        panic!("Data")
    };
    let alias = data.body().clone();
    drop(original);
    let filler = fixture.fill_process(PROCESS - FIXED - COPY - fixture.header_bytes());
    let read = fixture.request(Some(2), 1, 1);
    let response = root_result_unary(&fixture.reader, unary_request(&read)).await;
    assert!(!response.headers().contains_key("grpc-status"));
    let mut body = response.into_body();
    let raw = unary_data(&mut body).await;
    assert!(matches!(
        unary_message(&raw).result,
        Some(wire::fetch_root_result_response::Result::Deferred(_)),
    ));
    assert_eq!(
        unary_decoded_response(&raw, &read, 1),
        RootReadResponse::Deferred(
            novarocks_execution_contract::root_result::RootReadDeferred::for_request(
                &read,
                send_backing_busy(),
            )
        ),
    );
    assert_eq!(
        fixture.root.snapshot().consumed_through,
        1,
        "the real ACK applied before copy admission"
    );
    assert!(
        !fixture.root.physical_idle(),
        "the actual original Data alias remains live"
    );
    assert!(matches!(
        fixture.budget.try_reserve_process(COPY).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(raw);
    drop(body);
    drop(alias);
    let replay = owned(fixture.reader.read(&read).await);
    assert_eq!(
        replay.reply().accepted_consumed,
        1,
        "replaying the same cumulative ACK is idempotent"
    );
    assert!(matches!(replay.reply().outcome, RootReadOutcome::End(_)));
    drop(replay);
    assert!(fixture.root.physical_idle());
    drop(fixture.fill_process(COPY));
    drop(filler);
}

#[tokio::test]
async fn unary_unknown_root_refusal_carries_original_domain_fact_without_ack() {
    let fixture = UnaryFixture::new(false);
    let actual = fixture.root.spec().task;
    let unknown = TaskIdentity::new(
        actual.query_execution_id(),
        actual.stage_id(),
        TaskId::new(2).unwrap(),
        actual.backend_process_id(),
    );
    let read = RootResultRead::try_new(
        unknown,
        RootProfileId::V1,
        RootOutputKind::ClientRows,
        NonZeroU64::new(1),
        0,
        Duration::from_millis(1),
    )
    .unwrap();
    let response = root_result_unary(&fixture.reader, unary_request(&read)).await;
    assert!(!response.headers().contains_key("grpc-status"));
    let mut body = response.into_body();
    let data = unary_data(&mut body).await;
    assert!(matches!(
        unary_message(&data).result,
        Some(wire::fetch_root_result_response::Result::Refused(_))
    ));
    let RootReadResponse::Refused(refusal) = unary_decoded_response(&data, &read, 0) else {
        panic!("expected the real unknown-root domain refusal");
    };
    assert_eq!(refusal.fact(), root_refusal_fact(165));
    assert!(refusal.detail().as_str().len() <= 512);
    assert_eq!(fixture.root.snapshot().consumed_through, 0);
    assert!(
        fixture.root.physical_idle(),
        "no Root read position was minted for the unknown task"
    );
    drop(data);
    drop(body);
    drop(fixture.fill_process(PROCESS - FIXED - fixture.header_bytes()));
}

#[cfg(all(novarocks_dev_controls, unix))]
mod end_observation {
    use super::*;
    use novarocks_failpoint::{
        liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession},
        scope::ProcessBinding,
        session::{DomainGrant, SessionDeclaration, SessionRole},
        target_binding::StatementExecutionBinding,
    };
    use novarocks_native_adapter::test_observation_controls::{DOMAIN, NativeObservationControls};
    use serde_json::json;

    struct ObservationSession {
        _directory: tempfile::TempDir,
        role: RoleSessionOwner,
        runner: RunnerSession,
        controls: Arc<NativeObservationControls>,
    }
    impl ObservationSession {
        fn new(task: TaskIdentity, execution: QueryExecutionId) -> Self {
            let directory = tempfile::Builder::new()
                .prefix("nr-end-")
                .tempdir_in("/tmp")
                .unwrap();
            let root = directory.path();
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "root-end-be".into(),
                launch_id: "root-end-launch".into(),
                role: SessionRole::Be,
                lifetime_millis: 3000,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "e".repeat(32),
                domain_grants: vec![DomainGrant {
                    domain: DOMAIN.into(),
                    grant_id: "root-end-grant".into(),
                    parameters: json!({
                        "action": "grpc-fragment", "root": root,
                        "backend_index": 0, "statement_run_token": "root-end",
                        "target_session_id": "root-end-be", "max_events": 2,
                        "attempt_id": execution.attempt_id().get(),
                    }),
                }],
            };
            let file = root.join("session.json");
            declaration.write_private(&file).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&file, SessionRole::Be).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let role = prepared
                .bind(ProcessBinding::Backend(task.backend_process_id()))
                .unwrap();
            StatementExecutionBinding::new(
                "root-end-be".into(),
                "root-end".into(),
                0,
                task.backend_process_id(),
                execution,
            )
            .unwrap()
            .write_private(
                &novarocks_native_adapter::test_observation_controls::attempt_binding_path(
                    root,
                    "grpc-fragment",
                    0,
                    "root-end",
                    Some(execution.attempt_id().get()),
                ),
            )
            .unwrap();
            let controls = Arc::new(
                NativeObservationControls::from_grants(role.scope().grants_for(DOMAIN)).unwrap(),
            );
            Self {
                _directory: directory,
                role,
                runner,
                controls,
            }
        }
        fn hits(&self) -> usize {
            self.role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count()
        }
    }
    impl Drop for ObservationSession {
        fn drop(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }

    #[tokio::test]
    async fn actual_empty_root_end_claims_the_exact_existing_observation() {
        let mut fixture = Fixture::new(false);
        let task = fixture.root.spec().task;
        let session = ObservationSession::new(task, task.query_execution_id());
        assert_eq!(session.hits(), 0);
        fixture.reader = fixture
            .reader
            .with_observation_controls(Some(Arc::clone(&session.controls)));
        let producer = fixture.root.start_producer().unwrap();
        fixture.root.request_finish().unwrap();
        fixture.root.publish_end().unwrap();
        producer.finish_encoder();
        let reply = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
        assert!(
            matches!(reply.reply().outcome, RootReadOutcome::End(end) if end.sequence.get() == 1 && end.output_rows == 0)
        );
        assert_eq!(session.hits(), 1);
        drop(reply);
        assert!(fixture.root.physical_idle());
    }

    #[tokio::test]
    async fn fused_end_is_observed_but_ack_refusal_and_foreign_attempt_are_not() {
        let mut fixture = Fixture::new(false);
        let task = fixture.root.spec().task;
        let session = ObservationSession::new(task, task.query_execution_id());
        fixture.reader = fixture
            .reader
            .with_observation_controls(Some(Arc::clone(&session.controls)));
        let observer = session
            .controls
            .observation_for("grpc-fragment", task.query_execution_id())
            .unwrap()
            .unwrap();
        fixture.finish();
        let data = owned(fixture.reader.read(&fixture.request(Some(1), 0, 1)).await);
        assert!(
            matches!(&data.reply().outcome, RootReadOutcome::Data(data) if data.sequence().get() == 1 && data.body().len() == 6 && data.end_after_data().is_some_and(|end| end.sequence.get() == 2 && end.output_rows == 1))
        );
        drop(data);
        assert_eq!(session.hits(), 1);
        assert!(observer.is_active());
        let ack = owned(fixture.reader.read(&fixture.request(None, 2, 1)).await);
        assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
        assert_eq!(ack.reply().accepted_consumed, 2);
        drop(ack);
        assert_eq!(session.hits(), 1);
        assert!(observer.is_active());
        let wrong_task = TaskIdentity::new(
            task.query_execution_id(),
            task.stage_id(),
            TaskId::new(2).unwrap(),
            task.backend_process_id(),
        );
        let request = RootResultRead::try_new(
            wrong_task,
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            NonZeroU64::new(1),
            0,
            Duration::from_millis(1),
        )
        .unwrap();
        let refusal = refused(fixture.reader.read(&request).await, &request);
        assert_eq!(refusal.fact(), root_refusal_fact(165));
        assert_eq!(session.hits(), 1);
        assert!(observer.is_active());

        let mut foreign = Fixture::new(true);
        let task = foreign.root.spec().task;
        let execution = QueryExecutionId::new(
            task.query_execution_id().query_id(),
            AttemptId::new(2).unwrap(),
        )
        .unwrap();
        let foreign_session = ObservationSession::new(task, execution);
        foreign.reader = foreign
            .reader
            .with_observation_controls(Some(Arc::clone(&foreign_session.controls)));
        foreign.finish();
        let end = owned(foreign.reader.read(&foreign.request(Some(1), 0, 1)).await);
        assert!(matches!(end.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 1));
        assert_eq!(foreign_session.hits(), 0);
        drop(end);
    }

    #[tokio::test]
    async fn first_data_and_later_fused_end_preserve_the_finite_marker_policy() {
        let mut fixture = Fixture::new(false);
        let task = fixture.root.spec().task;
        let session = ObservationSession::new(task, task.query_execution_id());
        let observer = session
            .controls
            .observation_for("grpc-fragment", task.query_execution_id())
            .unwrap()
            .unwrap();
        fixture.reader = fixture
            .reader
            .with_observation_controls(Some(Arc::clone(&session.controls)));
        assert_eq!(session.hits(), 0);
        assert!(observer.is_active());
        let producer = fixture.root.start_producer().unwrap();
        for sequence in 1..=2 {
            fixture.root.note_rows(1).unwrap();
            let mut builder = fixture.root.try_segment().unwrap().unwrap();
            builder.output()[..6].copy_from_slice(b"\x02\0\0\0\x011");
            fixture.root.publish_segment(builder, 6, false).unwrap();
            let reply = owned(
                fixture
                    .reader
                    .read(&fixture.request(Some(sequence), sequence - 1, 1))
                    .await,
            );
            assert!(
                matches!(&reply.reply().outcome, RootReadOutcome::Data(data) if data.sequence().get() == sequence && data.body().len() == 6 && data.end_after_data().is_none())
            );
            drop(reply);
            assert_eq!(session.hits(), 1);
            // Exactly one of the two finite events has been used: the first
            // Data is observed; the ordinary second Data is not another event.
            assert!(observer.is_active());
            let ack = owned(
                fixture
                    .reader
                    .read(&fixture.request(None, sequence, 1))
                    .await,
            );
            assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
            drop(ack);
            assert!(observer.is_active());
        }
        fixture.root.note_rows(1).unwrap();
        fixture.root.request_finish().unwrap();
        let mut builder = fixture.root.try_segment().unwrap().unwrap();
        builder.output()[..6].copy_from_slice(b"\x02\0\0\0\x011");
        fixture.root.publish_segment(builder, 6, true).unwrap();
        producer.finish_encoder();
        let data = owned(fixture.reader.read(&fixture.request(Some(3), 2, 1)).await);
        assert!(
            matches!(&data.reply().outcome, RootReadOutcome::Data(data) if data.sequence().get() == 3 && data.body().len() == 6 && data.end_after_data().is_some_and(|end| end.sequence.get() == 4 && end.output_rows == 3))
        );
        // A non-first Data with an actual fused End consumes the second event.
        // No standalone End read is performed before the original ACK.
        assert!(!observer.is_active());
        assert_eq!(session.hits(), 1);
        drop(data);
        let ack = owned(fixture.reader.read(&fixture.request(None, 4, 1)).await);
        assert_eq!(ack.reply().outcome, RootReadOutcome::AckOnly);
        drop(ack);
        assert!(fixture.root.physical_idle());
    }
}
