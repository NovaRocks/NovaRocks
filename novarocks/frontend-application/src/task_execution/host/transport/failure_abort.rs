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

//! One frozen failure Abort through the shared, actor-independent Native send.

use super::{
    QueueCustody, QueueWindow, SendContext, SendKey,
    queue_input::AdmitQueuedInput,
    quote::{SourceLayouts, original_run_with_reports, queued_input_for_factory},
    send::{self, BoundSend, SendJob},
    send_binding::ResolvedBatch,
    send_reports::{AbortReply, SendReports},
    send_source::{
        APPLY_DISPOSAL_ROLES, cleanup_role, send_queue_resources, shared_source_layouts,
    },
    source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg},
};
use crate::{
    native::apply_send_owner::RegistrationFailure,
    native::transport_capacity::TransportCapacityLane,
    task_execution::{
        host::custody::{
            CustodyRefusal, OriginalTransportJoinObligation, TransportRetirement,
            TransportRetirementError, TransportRetirementStep,
        },
        host::failure_supervisor::{FailureCleanupPort, FailureCleanupRequest},
        intent::{AckPayload, OperationAcknowledgement, OperationIntent},
        operation::OperationDispatchResult,
    },
};
use novarocks_attempt_protocol::input::SendGeneration;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_work::{HandoffCell, PanicPayload, PanicSink},
    reported_work::ReportedWork,
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{identity::ReservationId, scope::WorkKind};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{
    AbortParticipation, ParticipationState, TaskOperationId, operation::OperationResult,
};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::WorkError;
use std::{
    alloc::Layout,
    collections::BTreeMap,
    convert::Infallible,
    future::{Future, poll_fn},
    num::NonZeroU64,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    runtime::Handle,
    task::JoinError,
    time::{Instant, timeout_at},
};

/// A one-operation projection of its actual reply. It keeps the Worker header
/// and participation state without retaining a second canonical reply owner.
pub(crate) struct AbortReceipt {
    pub(crate) operation: TaskOperationId,
    pub(crate) dispatch: OperationDispatchResult,
    pub(crate) worker: Option<OperationResult<Infallible>>,
    pub(crate) participation_state: Option<ParticipationState>,
    pub(crate) governance: Option<WorkError>,
}
impl AbortReceipt {
    fn observe(ack: &OperationAcknowledgement) -> Self {
        Self {
            operation: ack.operation_id(),
            dispatch: ack.dispatch_result(),
            worker: ack.worker_header(),
            participation_state: match ack.payload() {
                AckPayload::Participation(receipt) => Some(receipt.state()),
                _ => None,
            },
            governance: ack.governance_failure().cloned(),
        }
    }
}

pub(crate) struct AbortSendResult {
    pub(crate) receipt: Option<AbortReceipt>,
    pub(crate) body: Option<BodyOutcome>,
    pub(crate) rejected_attachment: Option<RegistrationFailure>,
    pub(crate) handoff_failure: Option<PanicPayload>,
    pub(crate) join_failure: Option<JoinError>,
    pub(crate) work_panic: Option<PanicPayload>,
    pub(crate) cleanup_panic: Option<PanicPayload>,
}

#[must_use = "retain the original refusal or unresolved transport ownership"]
pub(crate) enum AbortSendError {
    WrongOwner,
    Governance(WorkError),
    Runtime(RuntimeError),
    Prepare(CustodyRefusal, QueueWindow, super::OriginalQueuedInput),
    Retire(
        TransportRetirementError,
        Box<TransportRetirement>,
        AbortSendResult,
    ),
    Header(
        Option<RuntimeError>,
        Box<TransportRetirement>,
        AbortSendResult,
    ),
}
impl std::fmt::Debug for AbortSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => f.write_str("WrongOwner"),
            Self::Governance(error) => f.debug_tuple("Governance").field(error).finish(),
            Self::Runtime(error) => f.debug_tuple("Runtime").field(error).finish(),
            Self::Prepare(error, ..) => f.debug_tuple("Prepare").field(&error.error).finish(),
            Self::Header(error, ..) => f.debug_tuple("Header").field(error).finish(),
            Self::Retire(error, ..) => {
                use TransportRetirementError::*;
                match error {
                    Custody(error) => f.debug_tuple("RetireCustody").field(error).finish(),
                    JoinFunding(error, _) => {
                        f.debug_tuple("RetireJoinFunding").field(error).finish()
                    }
                    NativeJoin(..) => f.write_str("RetireNativeJoin"),
                    MissingCleanup(..) => f.write_str("RetireMissingCleanup"),
                    StartCleanup(error, _) => f
                        .debug_tuple("RetireStartCleanup")
                        .field(&error.error)
                        .finish(),
                    NativeCleanup(..) => f.write_str("RetireNativeCleanup"),
                    Physical(..) => f.write_str("RetirePhysical"),
                    ReportInput(error) => f.debug_tuple("RetireReportInput").field(error).finish(),
                }
            }
        }
    }
}

pub(crate) struct FailureAbortAttempt {
    pub(crate) request: AbortParticipation,
    pub(crate) result: Result<AbortSendResult, AbortSendError>,
}

/// The original logical owner takes raw local failures and exact Native
/// verdicts together with the conservative remote residual.
pub(crate) trait FailureResidualPort: Send + 'static {
    fn finish(
        self: Box<Self>,
        original: FailureCleanupRequest,
        attempts: Vec<FailureAbortAttempt>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

pub(crate) struct NativeFailureCleanup {
    targets: Arc<BTreeMap<BackendProcessId, SendContext>>,
    parent: ReservationId,
    executor: Handle,
    attempts: Vec<FailureAbortAttempt>,
    residual: Box<dyn FailureResidualPort>,
}

impl NativeFailureCleanup {
    pub(crate) fn new(
        targets: Arc<BTreeMap<BackendProcessId, SendContext>>,
        parent: ReservationId,
        executor: Handle,
        residual: Box<dyn FailureResidualPort>,
    ) -> Self {
        let attempts = Vec::with_capacity(targets.len());
        Self {
            targets,
            parent,
            executor,
            attempts,
            residual,
        }
    }
}

impl FailureCleanupPort for NativeFailureCleanup {
    fn abort_once(
        &mut self,
        request: AbortParticipation,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let result = if let Some(context) = self
                .targets
                .get(&request.participation().backend_process_id())
            {
                NativeAbortSender {
                    context,
                    parent: self.parent,
                    executor: &self.executor,
                }
                .send(request)
                .await
            } else {
                Err(AbortSendError::WrongOwner)
            };
            self.attempts.push(FailureAbortAttempt { request, result });
        })
    }

    fn finish(
        self: Box<Self>,
        original: FailureCleanupRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.residual.finish(original, self.attempts)
    }
}

struct AbortBodyReport(Arc<AbortReply>);
impl TransportCompletionObligation for AbortBodyReport {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome) {
        assert!(self.0.outcome.lock().unwrap().replace(outcome).is_none());
    }
}

/// Uses the original target's authenticated control client and a live cleanup
/// payer in the failed Scope. There is no business input or retry author here.
struct NativeAbortSender<'a> {
    context: &'a SendContext,
    parent: ReservationId,
    executor: &'a Handle,
}
impl NativeAbortSender<'_> {
    async fn send(&self, request: AbortParticipation) -> Result<AbortSendResult, AbortSendError> {
        let ctx = &self.context;
        let backend = request.participation().backend_process_id();
        let gates = ctx.scope.gates().map_err(AbortSendError::Runtime)?;
        if backend != ctx.client.backend_process_id()
            || !ctx.scope.belongs_to_ledger(&ctx.ledger)
            || !ctx.registration.matches_scope(&ctx.scope)
            || !gates.failed
            || !ctx.layout.accepts_transport_budget(ctx.transport)
        {
            return Err(AbortSendError::WrongOwner);
        }
        let deadline = Instant::now() + request.envelope().max_wait().get();
        let operation = OperationIntent::AbortParticipation(request);
        let (original_quote, encoding) =
            quote(&operation, ctx.transport).map_err(AbortSendError::Governance)?;
        let key = SendKey {
            operation: request.envelope().operation_id(),
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
        };
        let queued_quote = queued_input_for_factory(
            0,
            Layout::new::<AdmitQueuedInput>(),
            SourceLayouts {
                original: &[super::proposal_wait::ProposalWait::owned_future_layout()],
            },
        )
        .map_err(AbortSendError::Governance)?;
        let admission = AdmitQueuedInput::new(key, &ctx.authority, true, queued_quote)
            .map_err(AbortSendError::Governance)?;
        let input = timeout_at(deadline, admission)
            .await
            .map_err(|_| AbortSendError::Governance(WorkError::CapacityWaitTimeout))?
            .map_err(AbortSendError::Governance)?;
        let lane = TransportCapacityLane::Control;
        let bytes = operation.queued_bytes();
        let resources =
            send_queue_resources(&ctx.layout, backend, lane, 1, bytes).map_err(|_| {
                AbortSendError::Governance(WorkError::Capacity("Native failure Abort queue"))
            })?;
        let guard = ctx
            .scope
            .reserve_child_next(
                gates.generation,
                self.parent,
                resources,
                WorkKind::Settlement,
            )
            .map_err(AbortSendError::Runtime)?;
        let (work, mut capacity) = guard
            .bind(WorkKind::Settlement)
            .expect("original cleanup send guard");
        let window =
            QueueWindow::split(&mut capacity, ctx.layout.clone(), backend, lane, 1, bytes).unwrap();
        let report = ReportSlot::reserve_exit_observer_for_admitted(&mut capacity, ctx.observer)
            .expect("paid abort exit report");
        let mut cleanup = std::collections::VecDeque::with_capacity(APPLY_DISPOSAL_ROLES);
        let original = ReportedWork::try_new(work, capacity, report).expect("original abort Work");
        let header = original.work().retirement_observer();
        let panic = PanicSink::new();
        let (owner, exit, launch) =
            match OriginalTransportJoinObligation::prepare(original, ctx.observer, panic.clone()) {
                Ok(original) => original,
                Err(error) => return Err(AbortSendError::Prepare(error, window, input)),
            };
        for _ in 0..APPLY_DISPOSAL_ROLES {
            cleanup.push_back(cleanup_role(
                &ctx.scope,
                window.parent_reservation(),
                ctx.observer,
            ));
        }
        let keys: Arc<[SendKey]> = Arc::new([key]);
        let registration = owner.registration();
        let queue = QueueCustody::try_new(window, registration.clone(), keys.clone())
            .unwrap_or_else(|_| panic!("original Abort queue"));
        let loan = input
            .bind_send(registration.clone(), 0)
            .unwrap_or_else(|_| panic!("original Abort loan"));
        let reply = Arc::new(AbortReply {
            key,
            value: Mutex::new(None),
            outcome: Mutex::new(None),
        });
        let bound = Box::new(BoundSend {
            backend,
            control_progress: true,
            scope_generation: gates.generation,
            ledger: ctx.ledger.clone(),
            scope: ctx.scope.clone(),
            layout: ctx.layout.clone(),
            transport: ctx.transport,
            queue,
            resolved: ResolvedBatch::cleanup_abort(request, loan, keys),
            authority: ctx.authority.clone(),
            original_quote,
            encoding,
            wire: ctx.wire.clone(),
            client: ctx.client.clone(),
            apply: ctx.apply.clone(),
            reports: SendReports::Abort(reply.clone()),
            clock: ctx.clock.clone(),
            lifecycle: None,
            deadline: Some(deadline),
            original_captures: Box::new(()),
        });
        let mut retirement = Box::new(TransportRetirement::new(owner, cleanup, ctx.observer));
        let _ = launch.spawn(&self.executor, exit, |exit, registration| {
            send::run(SendJob {
                leg: TransportLeg {
                    exit,
                    handoff: Arc::new(HandoffCell::new()),
                    panic: panic.clone(),
                    result: Box::new(AbortBodyReport(reply.clone())),
                },
                registration,
                bound,
            })
        });
        let mut result = AbortSendResult {
            receipt: None,
            body: None,
            rejected_attachment: None,
            handoff_failure: None,
            join_failure: None,
            work_panic: None,
            cleanup_panic: None,
        };
        loop {
            let step = poll_fn(|cx| retirement.poll_step(&self.executor, cx)).await;
            result.rejected_attachment = result
                .rejected_attachment
                .take()
                .or_else(|| retirement.take_rejected_attachment());
            result.handoff_failure = result
                .handoff_failure
                .take()
                .or_else(|| retirement.original().take_handoff_failure());
            result.join_failure = result
                .join_failure
                .take()
                .or_else(|| retirement.original().take_join_failure());
            result.work_panic = result
                .work_panic
                .take()
                .or_else(|| retirement.original().take_work_panic());
            result.cleanup_panic = result.cleanup_panic.take().or_else(|| panic.take_cleanup());
            if let Some(ack) = reply.value.lock().unwrap().take() {
                assert!(
                    result
                        .receipt
                        .replace(AbortReceipt::observe(&ack))
                        .is_none()
                );
            }
            if let Some(body) = reply.outcome.lock().unwrap().take() {
                assert!(result.body.replace(body).is_none());
            }
            match step {
                Ok(TransportRetirementStep::Retired) => {
                    return match ctx.scope.reap_observed_header(&header) {
                        Ok(true) => Ok(result),
                        Ok(false) => Err(AbortSendError::Header(None, retirement, result)),
                        Err(error) => Err(AbortSendError::Header(Some(error), retirement, result)),
                    };
                }
                Ok(TransportRetirementStep::Advanced | TransportRetirementStep::Scanned) => {}
                Ok(TransportRetirementStep::Report(original)) => {
                    std::panic::resume_unwind(Box::new(original))
                }
                Err(error) => return Err(AbortSendError::Retire(error, retirement, result)),
            }
        }
    }
}

fn quote(
    operation: &OperationIntent,
    budget: novarocks_task_codec::TransportBudget,
) -> Result<
    (
        novarocks_workload_control::joint_admission::NativeAsyncActionQuote,
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    ),
    WorkError,
> {
    let arc = |layout| arc_layout(layout).map_err(|_| WorkError::ArithmeticOverflow);
    let mut layouts = Vec::from([
        arc(Layout::new::<HandoffCell>())?,
        Layout::new::<AbortBodyReport>(),
        Layout::new::<TransportRetirement>(),
        Layout::new::<BoundSend>(),
    ]);
    layouts.extend(shared_source_layouts()?);
    original_run_with_reports(
        std::slice::from_ref(operation),
        budget,
        SourceLayouts { original: &layouts },
        arc(Layout::new::<AbortReply>())?.size(),
    )
}
