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

//! Feature-gated integration harness for Native role-adapter tests.
//!
//! The harness exposes observed behavior while retaining every registry,
//! actor, attempt, output, join, and retirement authority inside the query
//! application crate. Production adapters cannot use this module because the
//! production dependency does not enable `test-support`.

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, QueryContextRef, ResultPacketSequence,
};
use novarocks_types::QueryExecutionId;
use novarocks_workload_control::{
    CancellationReason, LocalResourceAuthority, ResourceConfig, RootWork, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
};
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::sync::watch;

use crate::api::{
    ExecutionHandle, ExecutionOutput, QueryExecutionError, QueryResultStream, ResultField,
    ResultSchema,
};
use crate::coordination::{
    AdmissionIssueReceipt, AdmissionIssueSettlement, ContextStandDownSnapshot,
    LogicalExecutionActor, LogicalExecutionActorConfig, LogicalExecutionActorError,
    LogicalExecutionActorSnapshot, LogicalExecutionJoinReadiness, LogicalExecutionOutputTransfer,
    LogicalExecutionRegistration, LogicalExecutionRuntimeRegistry,
    LogicalExecutionRuntimeRegistryError, LogicalExecutionRuntimeRegistryHandle,
    LogicalExecutionRuntimeShutdownError, RunningAttemptPermit,
};
use novarocks_coordination_contract::AttemptActivationIdentity;

use crate::api::result::{
    EndDelivery, QueryResultTransport, ResultDelivery, ResultDeliveryDisposition,
    ResultDeliveryReceipt,
};

/// Test-only observation of one move-only protocol delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestResultDeliveryDisposition {
    Completed,
    Failed(QueryExecutionError),
    Dropped,
}

/// Test-only receipt for the actor side of one protocol delivery.
pub struct TestResultDeliveryReceipt(ResultDeliveryReceipt);

impl TestResultDeliveryReceipt {
    pub async fn wait(self) -> TestResultDeliveryDisposition {
        match self
            .0
            .await
            .expect("test result delivery owner remains alive")
        {
            ResultDeliveryDisposition::Completed => TestResultDeliveryDisposition::Completed,
            ResultDeliveryDisposition::Failed(error) => {
                TestResultDeliveryDisposition::Failed(*error)
            }
            ResultDeliveryDisposition::Dropped => TestResultDeliveryDisposition::Dropped,
        }
    }
}

/// Feature-gated producer for exercising a real [`QueryResultStream`] from a
/// role adapter without exposing production construction authority.
pub struct ResultStreamTestProducer {
    execution_id: QueryExecutionId,
    transport: QueryResultTransport,
    failure: watch::Sender<Option<QueryExecutionError>>,
    _workload: WorkloadControl,
    root: Option<RootWork>,
    window: Option<novarocks_workload_control::ResultWindowGrant>,
    capacity: Option<novarocks_workload_control::ResultCapacityHandle>,
}

impl ResultStreamTestProducer {
    pub fn open(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
    ) -> Result<
        (
            Self,
            ExecutionHandle,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        Self::open_with_carrier(
            execution_id,
            fields,
            delivery_capacity,
            resource_config,
            crate::api::ResultRowCarrier::relayed(
                novarocks_result_contract::RootOutputKind::ClientRows,
                Some(
                    novarocks_result_contract::ClientRowProfile::try_new(
                        novarocks_result_contract::RootProfileV1::SEGMENT_BYTES,
                        novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES,
                    )
                    .expect("frozen client profile"),
                ),
            )?,
        )
    }

    pub fn open_with_carrier(
        execution_id: QueryExecutionId,
        fields: Vec<ResultField>,
        delivery_capacity: usize,
        resource_config: ResourceConfig,
        carrier: crate::api::ResultRowCarrier,
    ) -> Result<
        (
            Self,
            ExecutionHandle,
            LocalResourceAuthority,
            TestResultDeliveryReceipt,
        ),
        QueryExecutionError,
    > {
        let workload = WorkloadControl::try_new(WorkloadConfig::default(), resource_config)
            .expect("test result workload config must be valid");
        let capacity = Some({
            workload
                .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
                .expect("test result capacity")
        });
        workload
            .mark_ready()
            .expect("test result workload becomes ready");
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("test result root work is admitted");
        let window = capacity.as_ref().map(|capacity| {
            let class = match carrier {
                crate::api::ResultRowCarrier::Relayed {
                    kind: novarocks_result_contract::RootOutputKind::ClientRows,
                    ..
                } => novarocks_workload_control::ResultWindowClass::Client,
                _ => novarocks_workload_control::ResultWindowClass::Internal,
            };
            capacity
                .try_acquire(&root.owner.scope(), class)
                .expect("test root window")
        });
        let schema = ResultSchema::new(fields);
        let (transport, schema_receipt, failure, stream) = QueryResultStream::try_channel(
            execution_id.query_id(),
            schema,
            carrier,
            delivery_capacity,
        )?;
        let handle = ExecutionHandle::new(
            root.owner.cancellation_requester(),
            ExecutionOutput::Rows(stream),
        );
        let resources = workload.resources().unwrap();
        Ok((
            Self {
                execution_id,
                transport,
                failure,
                _workload: workload,
                root: Some(root),
                window,
                capacity,
            },
            handle,
            resources,
            TestResultDeliveryReceipt(schema_receipt),
        ))
    }

    pub fn result_capacity(&self) -> novarocks_workload_control::ResultCapacityHandle {
        self.capacity
            .as_ref()
            .expect("test relayed capacity")
            .clone()
    }

    /// Encoded ClientRows fixture transferred through the real V1 validator.
    pub async fn enqueue_client_body(
        &self,
        sequence: u64,
        body: Vec<u8>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use novarocks_execution_contract::TaskIdentity;
        use novarocks_execution_contract::root_result::{
            RootReadOutcome, RootResultData, RootResultReply,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        use novarocks_types::{BackendProcessId, StageId, TaskId};
        let data = RootResultData::try_new(
            RootOutputKind::ClientRows,
            std::num::NonZeroU64::new(sequence.checked_add(1).ok_or_else(|| {
                crate::api::QueryExecutionError::new(
                    crate::api::QueryExecutionErrorKind::InvalidRequest,
                    "fixture sequence overflow",
                )
            })?)
            .unwrap(),
            bytes::Bytes::from(body),
            None,
        )
        .map_err(|error| {
            crate::api::QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        self.enqueue_segment(
            sequence,
            RootResultReply {
                root_task: TaskIdentity::new(
                    self.execution_id,
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind: RootOutputKind::ClientRows,
                accepted_consumed: sequence,
                outcome: RootReadOutcome::Data(data),
            },
            Some((
                ClientRowProfile::try_new(
                    RootProfileV1::SEGMENT_BYTES,
                    RootProfileV1::ROW_PAYLOAD_BYTES,
                )
                .unwrap(),
                ClientRowStreamCursor::default(),
            )),
            rows,
        )
        .await
    }

    /// Test-only transfer through the real move-only segment/receipt boundary.
    pub async fn enqueue_segment(
        &self,
        sequence: u64,
        reply: novarocks_execution_contract::root_result::RootResultReply,
        client_rows: Option<(
            novarocks_result_contract::ClientRowProfile,
            novarocks_result_contract::ClientRowStreamCursor,
        )>,
        rows: u64,
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        let window = self
            .window
            .as_ref()
            .expect("relayed test stream has a window");
        let backing = match &reply.outcome {
            novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                data.body().len() as u64 + 4096
            }
            _ => 4096,
        };
        let retained = crate::api::RetainedRootReply::try_new(
            novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
            window.retain_alias(),
            backing,
        )
        .map_err(|error| {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                error.to_string(),
            )
        })?;
        let (delivery, receipt) = crate::api::RootSegmentDelivery::try_new(
            self.execution_id,
            ResultPacketSequence::new(sequence),
            retained,
            client_rows,
            rows,
        )?;
        let permit = self.transport.reserve_owned().await?;
        self.transport
            .enqueue(permit, ResultDelivery::Segment(delivery));
        Ok(TestResultDeliveryReceipt(receipt))
    }

    /// Component-only two-item resident window, using the original publish/take/freeze owners.
    /// This performs no Native fetch and proves no Native actor or task lifecycle.
    /// Both items have frontier zero and no piggyback End; no delivery receipt has completed.
    pub async fn enqueue_resident_client_pair(
        &self,
        replies: [novarocks_execution_contract::root_result::RootResultReply; 2],
    ) -> Result<TestResultDeliveryReceipt, QueryExecutionError> {
        use crate::api::{
            ResidentRootSegment, RetainedRootReply, RootRelayResidentWindow, RootReplyView,
        };
        use novarocks_result_contract::{
            ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
        };
        let invalid = || {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::InvalidRequest,
                "component resident pair differs from one contiguous ClientRows task",
            )
        };
        let window = self.window.as_ref().expect("relayed test window");
        let [first, next] = replies;
        if first.root_task != next.root_task
            || first.root_task.query_execution_id() != self.execution_id
            || [&first, &next].iter().any(|reply| {
                reply.kind != RootOutputKind::ClientRows
                    || reply.profile != RootProfileId::V1
                    || reply.accepted_consumed != 0
            })
        {
            return Err(invalid());
        }
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let resident = RootRelayResidentWindow::default();
        let mut cursor = ClientRowStreamCursor::new();
        let mut delivery = None;
        for (index, reply) in [first, next].into_iter().enumerate() {
            let backing = match &reply.outcome {
                novarocks_execution_contract::root_result::RootReadOutcome::Data(data) => {
                    if data.end_after_data().is_some() {
                        return Err(invalid());
                    }
                    data.body().len() as u64 + 4096
                }
                _ => return Err(invalid()),
            };
            let retained = std::sync::Arc::new(
                RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(reply),
                    window.retain_alias(),
                    backing,
                )
                .map_err(|_| invalid())?,
            );
            let RootReplyView::Data { sequence, body, .. } = retained.outcome() else {
                return Err(invalid());
            };
            if sequence.get() != index as u64 + 1 {
                return Err(invalid());
            }
            let after = cursor
                .validate_body(profile, body)
                .map_err(|_| invalid())?
                .after();
            let rows = after.completed_rows() - cursor.completed_rows();
            if !resident.publish(ResidentRootSegment {
                sequence,
                reply: retained.clone(),
                client_rows: Some((profile, cursor)),
                rows,
            }) {
                return Err(invalid());
            }
            if index == 0 {
                let _original_delivering = resident.take().ok_or_else(invalid)?;
                delivery = Some(crate::api::RootSegmentDelivery::try_new(
                    self.execution_id,
                    ResultPacketSequence::new(0),
                    retained,
                    Some((profile, cursor)),
                    rows,
                )?);
            }
            cursor = after;
        }
        cursor.validate_end().map_err(|_| invalid())?;
        let (delivery, receipt) = delivery.expect("first resident delivery");
        let permit = self.transport.reserve_owned().await?;
        self.transport.enqueue(
            permit,
            ResultDelivery::Segment(delivery.with_resident_window(resident)),
        );
        Ok(TestResultDeliveryReceipt(receipt))
    }

    pub async fn enqueue_end(&self, sequence: u64) -> TestResultDeliveryReceipt {
        let (delivery, receipt) =
            EndDelivery::success_eof(self.execution_id, ResultPacketSequence::new(sequence));
        let permit = self
            .transport
            .reserve_owned()
            .await
            .expect("test result stream remains connected");
        self.transport
            .enqueue(permit, ResultDelivery::End(delivery));
        TestResultDeliveryReceipt(receipt)
    }

    pub fn fail(&self, error: QueryExecutionError) {
        self.failure.send_replace(Some(error));
    }

    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        self.root
            .as_ref()
            .expect("test result root remains active")
            .owner
            .scope()
            .cancellation()
            .expect("test result scope remains observable")
            .reason()
    }

    pub fn finish(mut self) {
        drop(self.transport);
        drop(self.failure);
        let root = self.root.take().expect("test result root finishes once");
        root.owner.complete();
        root.business.release();
    }
}

/// Complete Query Application ownership for one actor-driven integration test.
#[must_use = "the test logical execution must converge and call finish"]
pub struct LogicalExecutionTestHarness {
    registry_owner: Option<LogicalExecutionRuntimeRegistry>,
    registry: LogicalExecutionRuntimeRegistryHandle,
    registration: Option<LogicalExecutionRegistration>,
    actor: Option<LogicalExecutionActor>,
    initial: Option<crate::coordination::AttemptInstantiationPermit>,
    running: Option<RunningAttemptPermit>,
    output: Option<LogicalExecutionOutputTransfer>,
}

impl std::fmt::Debug for LogicalExecutionTestHarness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalExecutionTestHarness")
            .field(
                "initial_execution",
                &self
                    .registration
                    .as_ref()
                    .map(LogicalExecutionRegistration::initial_execution),
            )
            .field("initial_pending", &self.initial.is_some())
            .field("running", &self.running.is_some())
            .finish_non_exhaustive()
    }
}

impl LogicalExecutionTestHarness {
    pub fn install(
        runtime: Handle,
        config: LogicalExecutionActorConfig,
        initial_execution: novarocks_types::QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> Result<Self, LogicalExecutionRuntimeRegistryError> {
        let registry_owner = LogicalExecutionRuntimeRegistry::new(runtime);
        let registry = registry_owner.handle();
        let (registration, initial, output) = registry
            .reserve(initial_execution, contexts)?
            .spawn_and_install(config)?
            .into_parts();
        let actor = registry.actor(&registration)?;
        Ok(Self {
            registry_owner: Some(registry_owner),
            registry,
            registration: Some(registration),
            actor: Some(actor),
            initial: Some(initial),
            running: None,
            output: Some(output),
        })
    }

    /// Installs the real internal-result actor for role-local integration tests.
    /// The description, clock and budgets follow the production validation path;
    /// this helper does not construct a readiness token or a decoded receipt.
    #[allow(clippy::too_many_arguments)]
    pub fn install_internal(
        runtime: Handle,
        config: LogicalExecutionActorConfig,
        description: &crate::preparation::FrozenExecutionDescription,
        delivery_capacity: std::num::NonZeroUsize,
        clock: std::sync::Arc<dyn novarocks_coordination_contract::MonotonicClock>,
        budgets: crate::api::ComputationObservationBudgets,
        initial_execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> Result<Self, LogicalExecutionRuntimeRegistryError> {
        let config = config
            .with_internal_computation(description, delivery_capacity)?
            .with_clock(clock)
            .with_internal_observation_budgets(budgets);
        Self::install(runtime, config, initial_execution, contexts)
    }

    /// Exposes the original effect-free recovery configuration only to the
    /// existing integration harness. install_internal still validates the
    /// actual description's effect, recovery policy, and internal carrier.
    #[allow(clippy::too_many_arguments)]
    pub fn internal_pre_visibility_recovery_config(
        initial_execution: QueryExecutionId,
        mailbox_capacity: std::num::NonZeroUsize,
        contexts: Vec<QueryContextRef>,
        max_admission_issues: std::num::NonZeroUsize,
        max_establish_authorizations: std::num::NonZeroUsize,
        max_attempts: std::num::NonZeroU32,
        qualification: std::sync::Arc<dyn crate::coordination::ReplacementQualificationEffectPort>,
        owner: novarocks_workload_control::WorkOwner,
        stage: novarocks_workload_control::StagePermit,
        reservation_valid_for: std::time::Duration,
    ) -> Result<LogicalExecutionActorConfig, LogicalExecutionActorError> {
        LogicalExecutionActorConfig::read_only_pre_visibility_recovery(
            initial_execution,
            mailbox_capacity,
            contexts,
            max_admission_issues,
            max_establish_authorizations,
            max_attempts,
            qualification,
            owner,
            stage,
            reservation_valid_for,
        )
    }

    /// Consume the same original running authority through the actor's real
    /// failure/visibility/attempt-budget gate, qualification and activation.
    /// Failure/cancellation leaves the original actor/registry owned by self;
    /// this helper never reconstructs a consumed permit or a Ready token.
    pub async fn replace_original_running_attempt(
        &mut self,
        failure: novarocks_coordination_contract::AttemptFailureClass,
        successor: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> Result<AttemptActivationIdentity, LogicalExecutionActorError> {
        let running = self
            .running
            .take()
            .ok_or(LogicalExecutionActorError::WrongPhase)?;
        let qualification = self
            .actor()
            .begin_replacement(running, failure, successor, contexts)
            .await?;
        let initial = self.actor().activate_replacement(qualification).await?;
        let running = self.actor().activate(initial.ready()).await?;
        let identity = running.identity();
        self.running = Some(running);
        Ok(identity)
    }

    /// Replays the fixture's original accepted admission and Establish facts
    /// through the real actor-owned permit/transport/Worker-settlement path.
    /// The caller supplies the already-observed operation identities and ACKs;
    /// this helper never clears required contexts or writes a ledger state.
    pub async fn settle_original_applied_establish(
        &self,
        admission_request: novarocks_execution_contract::AcquireQueryContextAdmissionTicket,
        admission_settlement: crate::coordination::AdmissionIssueSettlement,
        establish_request: std::sync::Arc<novarocks_execution_contract::EstablishQueryContext>,
        establish_ack_operation: novarocks_execution_contract::TaskOperationId,
        establish_outcome: novarocks_execution_contract::OperationOutcome,
    ) -> Result<(), LogicalExecutionActorError> {
        self.settle_original_establish(
            admission_request,
            Some(admission_settlement),
            establish_request,
            establish_ack_operation,
            establish_outcome,
        )
        .await
    }

    /// A replacement's admission was already installed by its actual qualified
    /// reservation. Reuse that exact grant without issuing admission twice.
    pub async fn settle_original_qualified_establish(
        &self,
        admission_request: novarocks_execution_contract::AcquireQueryContextAdmissionTicket,
        establish_request: std::sync::Arc<novarocks_execution_contract::EstablishQueryContext>,
        establish_ack_operation: novarocks_execution_contract::TaskOperationId,
        establish_outcome: novarocks_execution_contract::OperationOutcome,
    ) -> Result<(), LogicalExecutionActorError> {
        self.settle_original_establish(
            admission_request,
            None,
            establish_request,
            establish_ack_operation,
            establish_outcome,
        )
        .await
    }

    async fn settle_original_establish(
        &self,
        admission_request: novarocks_execution_contract::AcquireQueryContextAdmissionTicket,
        admission_settlement: Option<crate::coordination::AdmissionIssueSettlement>,
        establish_request: std::sync::Arc<novarocks_execution_contract::EstablishQueryContext>,
        establish_ack_operation: novarocks_execution_contract::TaskOperationId,
        establish_outcome: novarocks_execution_contract::OperationOutcome,
    ) -> Result<(), LogicalExecutionActorError> {
        use crate::coordination::{
            EstablishIssueIdentity, EstablishIssueSubmit, EstablishTransportAdmission,
            EstablishTransportReservation, EstablishTransportSink, EstablishTransportSubmission,
        };
        use std::sync::{Arc, Mutex};
        #[derive(Debug, Default)]
        struct FixtureTransport(Arc<Mutex<Option<EstablishTransportSubmission>>>);
        #[derive(Debug)]
        struct FixtureReservation(Arc<Mutex<Option<EstablishTransportSubmission>>>);
        impl EstablishTransportReservation for FixtureReservation {
            fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
                assert!(self.0.lock().unwrap().replace(submission).is_none());
            }
        }
        impl EstablishTransportSink for FixtureTransport {
            fn try_reserve(&self, _: EstablishIssueIdentity) -> EstablishTransportAdmission {
                EstablishTransportAdmission::Admitted(Box::new(FixtureReservation(Arc::clone(
                    &self.0,
                ))))
            }
        }
        if establish_request.envelope().operation_id() != establish_ack_operation
            || establish_request.context() != admission_request.context()
            || !matches!(
                establish_outcome,
                novarocks_execution_contract::OperationOutcome::Accepted
                    | novarocks_execution_contract::OperationOutcome::Idempotent
            )
        {
            return Err(LogicalExecutionActorError::InvariantViolation);
        }
        let running = self.running.as_ref().expect("fixture attempt is active");
        if let Some(admission_settlement) = admission_settlement {
            let receipt = running.begin_admission_issue(admission_request).await?;
            running
                .settle_admission_issue(receipt, admission_settlement)
                .await?;
        }
        let permit = running
            .authorize_establish(
                establish_request,
                admission_request.native_compatibility_id(),
            )
            .await?;
        let transport = FixtureTransport::default();
        assert_eq!(
            permit
                .try_submit(&transport)
                .map_err(LogicalExecutionActorError::Establish)?,
            EstablishIssueSubmit::Accepted
        );
        transport
            .0
            .lock()
            .unwrap()
            .take()
            .expect("the original request crossed transport ownership")
            .worker_settled(establish_outcome)
            .map_err(LogicalExecutionActorError::Establish)?;
        // Actor mailbox roundtrip is an observation/barrier, never a direct
        // assignment to the Establish ledger or the Ready state.
        self.actor()
            .establish_snapshot(admission_request.context())
            .await?;
        Ok(())
    }

    /// Binds the real current running permit; no source or receipt is forged.
    pub async fn internal_consumer_activation(
        &self,
        root: novarocks_execution_contract::TaskIdentity,
    ) -> Result<crate::api::InternalConsumerActivation, LogicalExecutionActorError> {
        self.running
            .as_ref()
            .expect("the internal test attempt is active")
            .bind_internal_consumer(root)
            .await
    }

    /// Feature-gated borrow for component tests that retain the whole original
    /// activation until the real End path consumes it.
    pub fn internal_consumer_source(
        activation: &crate::api::InternalConsumerActivation,
    ) -> &crate::api::ComputationReadySource {
        &activation.source
    }

    pub async fn accept_internal_end(
        &self,
        root: novarocks_execution_contract::TaskIdentity,
        status: novarocks_execution_contract::TaskStatus,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> Result<crate::api::ComputationReadyToken, QueryExecutionError> {
        let activation = self
            .internal_consumer_activation(root)
            .await
            .map_err(|error| {
                QueryExecutionError::new(
                    crate::api::QueryExecutionErrorKind::Failed,
                    format!("internal integration fixture actor failed: {error}"),
                )
            })?;
        self.accept_internal_end_from(activation, root, status, end)
            .await
    }

    /// Feeds the original End through the actual actor delivery/decoding path
    /// and submits the real registered source after its root status is observed.
    pub async fn accept_internal_end_from(
        &self,
        activation: crate::api::InternalConsumerActivation,
        root: novarocks_execution_contract::TaskIdentity,
        status: novarocks_execution_contract::TaskStatus,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> Result<crate::api::ComputationReadyToken, QueryExecutionError> {
        fn actor_error(error: LogicalExecutionActorError) -> QueryExecutionError {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::Failed,
                format!("internal integration fixture actor failed: {error}"),
            )
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let running = self
                .running
                .as_ref()
                .expect("the internal test attempt is active");
            let exit = activation.supervise_exit().await.map_err(actor_error)?;
            let (mut stream, source) = activation.into_parts();
            let observer = running.bind_root_result(root).await.map_err(actor_error)?;
            observer
                .observe_local_root_end(root, end)
                .await
                .map_err(actor_error)?;
            observer
                .observe_status_with_failure(
                    status,
                    crate::coordination::RootTerminalFailure::Unspecified,
                )
                .await
                .map_err(actor_error)?;
            let (delivery, decoded) =
                tokio::join!(running.deliver_internal_end(root, end), async {
                    match stream.next().await? {
                        Some(crate::api::InternalResultDelivery::End(end)) => end.complete().await,
                        _ => Err(QueryExecutionError::new(
                            crate::api::QueryExecutionErrorKind::Failed,
                            "internal integration fixture did not receive its original End",
                        )),
                    }
                },);
            let outcome = match delivery {
                Ok(()) => match decoded {
                    Ok(decoded) => source.accept(decoded).await,
                    Err(error) => Err(error),
                },
                Err(error) => Err(actor_error(error)),
            };
            exit.complete(outcome.as_ref().map(|_| ()).map_err(Clone::clone));
            outcome
        })
        .await
        .expect("test-only watchdog: original End delivery, decoding and Ready acceptance")
    }

    /// Drives the real registered consumer and reports its actual future exit.
    /// No End receipt or Ready proposal is authored by this harness helper.
    pub async fn run_registered_internal_consumer(
        &self,
        consumer: std::sync::Arc<dyn crate::api::InternalComputationConsumer>,
        activation: crate::api::InternalConsumerActivation,
    ) -> Result<(), QueryExecutionError> {
        let exit = activation.supervise_exit().await.map_err(|error| {
            QueryExecutionError::new(
                crate::api::QueryExecutionErrorKind::Failed,
                format!("internal consumer fixture supervision failed: {error}"),
            )
        })?;
        let result = consumer.consume(activation).await;
        exit.complete(result.clone());
        result
    }

    /// Delivers only the original input. The fixed consumer must decode End
    /// and propose readiness through its own original source.
    pub async fn deliver_original_internal_end(
        &self,
        root: novarocks_execution_contract::TaskIdentity,
        status: novarocks_execution_contract::TaskStatus,
        end: novarocks_execution_contract::root_result::RootResultEnd,
    ) -> Result<(), LogicalExecutionActorError> {
        let running = self.running.as_ref().expect("fixture attempt is active");
        let observer = running.bind_root_result(root).await?;
        observer.observe_local_root_end(root, end).await?;
        observer
            .observe_status_with_failure(
                status,
                crate::coordination::RootTerminalFailure::Unspecified,
            )
            .await?;
        running.deliver_internal_end(root, end).await
    }

    /// Transfers one original retained internal segment through its real
    /// actor delivery receipt. The concrete consumer performs actual decoding.
    pub async fn deliver_original_internal_segment(
        &self,
        sequence: novarocks_execution_contract::ResultPacketSequence,
        segment: std::sync::Arc<crate::api::RetainedRootReply>,
        rows: u64,
    ) -> Result<(), LogicalExecutionActorError> {
        self.running
            .as_ref()
            .expect("fixture attempt is active")
            .deliver_root_segment(
                sequence,
                segment,
                None,
                rows,
                crate::api::RootRelayResidentWindow::default(),
            )
            .await
    }

    /// The exact same failure-freeze entry used by the real root pump. This
    /// helper cannot replace its running authority or conclude the statement.
    pub async fn freeze_original_internal_failure(
        &self,
        error: QueryExecutionError,
    ) -> Result<(), LogicalExecutionActorError> {
        self.running
            .as_ref()
            .expect("fixture attempt is active")
            .freeze_result_attempt_failure(error)
            .await
    }

    /// Completes the real Native pump's NoRecovery decision with its original
    /// running authority. Freezing an attempt error alone is not a conclusion.
    pub async fn fail_original_running_attempt(
        &mut self,
        error: QueryExecutionError,
    ) -> Result<crate::coordination::LogicalConclusion, LogicalExecutionActorError> {
        let running = self
            .running
            .take()
            .ok_or(LogicalExecutionActorError::WrongPhase)?;
        self.actor().fail_attempt_with_error(running, error).await
    }

    /// The original actor's logical outcome watch, retained across activation
    /// retirement. This accessor cannot manufacture a cut or decoded receipt.
    pub fn original_internal_outcome(
        &self,
    ) -> watch::Receiver<Option<Result<crate::api::ComputationReadyToken, QueryExecutionError>>>
    {
        self.actor()
            .internal_outcome()
            .expect("the fixture installed the real internal computation actor")
    }

    pub async fn activate_initial(
        &mut self,
    ) -> Result<AttemptActivationIdentity, LogicalExecutionActorError> {
        let initial = self
            .initial
            .take()
            .expect("the initial attempt may be activated only once");
        let running = self.actor().activate(initial.ready()).await?;
        let identity = running.identity();
        self.running = Some(running);
        Ok(identity)
    }

    /// Borrows the same narrow Native drive authority production active
    /// attempts receive. The running permit remains owned by this harness.
    pub fn native_attempt_drive(&self) -> crate::coordination::NativeAttemptDrive {
        crate::coordination::NativeAttemptDrive::new(
            self.running
                .as_ref()
                .expect("the test attempt must be activated before Native drive"),
        )
    }

    /// Abandons the active attempt so tests can drive its real stand-down and
    /// registry-shutdown path after exercising a borrowed Native drive.
    pub fn abandon_running_attempt(&mut self) {
        drop(
            self.running
                .take()
                .expect("the test attempt must be activated before abandonment"),
        );
    }

    /// Begins the exact admission issue used by Native Abort adapter tests and
    /// abandons the running permit so the actor must supervise stand-down.
    pub async fn begin_admission_issue_and_abandon(
        &mut self,
        request: AcquireQueryContextAdmissionTicket,
    ) -> Result<(AttemptActivationIdentity, AdmissionIssueReceipt), LogicalExecutionActorError>
    {
        let running = self
            .running
            .take()
            .expect("the test attempt must be activated before admission");
        let activation = running.identity();
        let result = running.begin_admission_issue(request).await;
        match result {
            Ok(receipt) => {
                drop(running);
                Ok((activation, receipt))
            }
            Err(error) => {
                self.running = Some(running);
                Err(error)
            }
        }
    }

    pub async fn settle_late_admission_issue(
        &self,
        activation: AttemptActivationIdentity,
        receipt: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
    ) -> Result<crate::coordination::AdmissionIssueDisposition, LogicalExecutionActorError> {
        self.actor()
            .settle_late_admission_issue(activation, receipt, settlement)
            .await
    }

    pub async fn stand_down_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<ContextStandDownSnapshot>, LogicalExecutionActorError> {
        self.actor().stand_down_snapshot(context).await
    }

    pub async fn actor_snapshot(
        &self,
    ) -> Result<LogicalExecutionActorSnapshot, LogicalExecutionActorError> {
        self.actor().snapshot().await
    }

    pub async fn establish_snapshot(
        &self,
        context: QueryContextRef,
    ) -> Result<Option<crate::coordination::EstablishIssueSnapshot>, LogicalExecutionActorError>
    {
        self.actor().establish_snapshot(context).await
    }

    pub async fn join_readiness(
        &self,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.registry.join_readiness(self.registration()).await
    }

    pub async fn observe_worker_stopped_and_context_fenced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_stopped_and_context_fenced(self.registration(), context)
            .await
    }

    pub async fn observe_worker_process_replaced(
        &self,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.registry
            .observe_worker_process_replaced(self.registration(), context)
            .await
    }

    /// Starts bounded registry shutdown while retaining the owner in `self`.
    /// A timeout, runtime error, or cancellation of this future leaves the
    /// harness available for an exact retry with a later deadline.
    pub async fn finish_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionRuntimeShutdownError> {
        drop(self.output.take());
        drop(self.running.take());
        drop(self.initial.take());
        drop(self.actor.take());
        let _ = self.registration.take();
        self.registry_owner
            .as_mut()
            .expect("the test registry owner may be shut down only once")
            .shutdown_until(deadline)
            .await
    }

    fn actor(&self) -> &LogicalExecutionActor {
        self.actor
            .as_ref()
            .expect("the test actor remains owned until finish")
    }

    fn registration(&self) -> &LogicalExecutionRegistration {
        self.registration
            .as_ref()
            .expect("the test registration remains owned until finish")
    }
}

/// Build a real QA schedule for a role adapter's exact frozen fixture facts.
/// This test-support module is absent from production composition.
pub fn qualification_schedule(
    execution: novarocks_types::QueryExecutionId,
    frontend: novarocks_types::identity::FrontendProcessId,
    eligible: &[novarocks_types::identity::BackendProcessId],
    facts: &crate::api::ExecutionSchedulingFacts,
) -> crate::coordination::AttemptSchedule {
    crate::coordination::build_attempt_schedule(
        execution,
        frontend,
        eligible,
        facts,
        #[cfg(novarocks_dev_controls)]
        Ok(None),
    )
    .expect("fixture uses the original QA schedule builder")
}

/// Schedule only the explicit placements of a finite role protocol fixture.
/// No actor, Root, request ticket, runtime or execution capability is created.
pub fn protocol_fixture_schedule(
    execution: novarocks_types::QueryExecutionId,
    frontend: novarocks_types::identity::FrontendProcessId,
    facts: &crate::api::ExecutionSchedulingFacts,
    placements: &std::collections::BTreeMap<u32, Vec<novarocks_types::identity::BackendProcessId>>,
) -> Result<crate::coordination::AttemptSchedule, String> {
    crate::coordination::protocol_fixture_schedule(execution, frontend, facts, placements)
        .map_err(|error| error.to_string())
}
