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

//! Credentialed single-operation Native sends and failure Abort supervision.

use super::{
    QueueCustody, QueueWindow, SendContext, SendKey,
    queue_input::AdmitQueuedInput,
    quote::{SourceLayouts, original_run_with_reports, queued_input_for_factory},
    send::{self, BoundSend, SendJob},
    send_binding::ResolvedBatch,
    send_reports::{SendReports, SingleSendReply},
    send_source::{
        APPLY_DISPOSAL_ROLES, cleanup_role, send_queue_resources, shared_source_layouts,
    },
    source_ports::{BodyOutcome, TransportCompletionObligation, TransportLeg},
};
use crate::{
    native::{
        apply_send_owner::{ApplySendPort, RegistrationFailure},
        task_transport::wire::AttemptWireFacts,
        transport::Client,
        transport_capacity::{NativeTransportCapacity, TransportCapacityLane},
    },
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
    obligation::StepRegistration,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ConsumerId, Generation, ReservationId},
    scope::WorkKind,
};
use novarocks_coordination_contract::{AttemptActionId, MonotonicClock};
use novarocks_execution_contract::{
    AbortParticipation, ParticipationState, TaskOperationId, operation::OperationResult,
};
use novarocks_type_contract::owned_resources::layout::arc_layout;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{NativeAsyncActionAuthority, NativeAsyncActionQuote},
};
use std::{
    alloc::Layout,
    collections::{BTreeMap, VecDeque},
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
#[allow(
    dead_code,
    reason = "Failure convergence retains the original typed abort verdict with the attempt."
)]
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

pub(crate) struct SingleSendResult<R> {
    pub(crate) receipt: Option<R>,
    pub(crate) body: Option<BodyOutcome>,
    pub(crate) rejected_attachment: Option<RegistrationFailure>,
    pub(crate) handoff_failure: Option<PanicPayload>,
    pub(crate) join_failure: Option<JoinError>,
    pub(crate) work_panic: Option<PanicPayload>,
    pub(crate) cleanup_panic: Option<PanicPayload>,
}

impl<R> SingleSendResult<R> {
    pub(crate) fn preparation_failure(&self) -> Option<&super::PreparationFailure> {
        match self.body.as_ref()? {
            BodyOutcome::RawFailure(original) => original.downcast_ref(),
            _ => None,
        }
    }
}

#[must_use = "retain the original refusal or unresolved transport ownership"]
#[allow(
    dead_code,
    reason = "Unproven sends retain original results, cleanup work and panic custody."
)]
pub(crate) enum SingleSendError<R> {
    WrongOwner,
    Governance(WorkError),
    Runtime(RuntimeError),
    Prepare(CustodyRefusal, QueueWindow, super::OriginalQueuedInput),
    Retire(
        TransportRetirementError,
        Box<TransportRetirement>,
        SingleSendResult<R>,
    ),
    Header(
        Option<RuntimeError>,
        Box<TransportRetirement>,
        SingleSendResult<R>,
    ),
    DrivePanic(PanicPayload, Box<TransportRetirement>, SingleSendResult<R>),
}
impl<R> SingleSendError<R> {
    pub(crate) fn exit_unproven(&self) -> bool {
        matches!(
            self,
            Self::Prepare(..) | Self::Retire(..) | Self::Header(..) | Self::DrivePanic(..)
        )
    }
}
impl<R> std::fmt::Debug for SingleSendError<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => f.write_str("WrongOwner"),
            Self::Governance(error) => f.debug_tuple("Governance").field(error).finish(),
            Self::Runtime(error) => f.debug_tuple("Runtime").field(error).finish(),
            Self::Prepare(error, ..) => f.debug_tuple("Prepare").field(&error.error).finish(),
            Self::Header(error, ..) => f.debug_tuple("Header").field(error).finish(),
            Self::DrivePanic(..) => f.write_str("DrivePanic"),
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

type AbortSendResult = SingleSendResult<AbortReceipt>;
type AbortSendError = SingleSendError<AbortReceipt>;

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

use crate::task_execution::bridge::abort::{AbortSettlementOwner, DrainedAbortSubmissions};

pub(crate) struct NativeFailureCleanup {
    targets: Arc<BTreeMap<BackendProcessId, SendContext>>,
    parent: ReservationId,
    executor: Handle,
    attempts: Vec<FailureAbortAttempt>,
    residual: Box<dyn FailureResidualPort>,
    abort_drain: Option<(
        Arc<AbortSettlementOwner>,
        tokio::sync::oneshot::Sender<DrainedAbortSubmissions>,
    )>,
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
            abort_drain: None,
        }
    }

    pub(crate) fn with_abort_drain(
        mut self,
        owner: Arc<AbortSettlementOwner>,
        handoff: tokio::sync::oneshot::Sender<DrainedAbortSubmissions>,
    ) -> Self {
        assert!(self.abort_drain.replace((owner, handoff)).is_none());
        self
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
        Box::pin(async move {
            if let Some((owner, handoff)) = self.abort_drain
                && let Err(drained) = handoff.send(owner.drain_failed())
            {
                std::panic::resume_unwind(Box::new(drained));
            }
            self.residual.finish(original, self.attempts).await;
        })
    }
}

struct SingleSendBodyReport(Arc<SingleSendReply>);
impl TransportCompletionObligation for SingleSendBodyReport {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome) {
        assert!(self.0.outcome.lock().unwrap().replace(outcome).is_none());
    }
}

/// Uses the original target's authenticated control client and a live cleanup
/// payer in the failed Scope.
struct NativeAbortSender<'a> {
    context: &'a SendContext,
    parent: ReservationId,
    executor: &'a Handle,
}
impl NativeAbortSender<'_> {
    async fn send(&self, request: AbortParticipation) -> Result<AbortSendResult, AbortSendError> {
        let ctx = self.context;
        let gates = ctx.scope.gates().map_err(AbortSendError::Runtime)?;
        if !ctx.registration.matches_scope(&ctx.scope) || !gates.failed {
            return Err(AbortSendError::WrongOwner);
        }
        let deadline = Instant::now() + request.envelope().max_wait().get();
        let operation = OperationIntent::AbortParticipation(request);
        let key = SendKey {
            operation: request.envelope().operation_id(),
            generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::MIN)),
        };
        let context = SingleSendContext {
            ledger: &ctx.ledger,
            scope: &ctx.scope,
            layout: &ctx.layout,
            transport: ctx.transport,
            authority: &ctx.authority,
            wire: &ctx.wire,
            client: &ctx.client,
            apply: &ctx.apply,
            clock: &ctx.clock,
            observer: ctx.observer,
            executor: self.executor,
        };
        let prepared = PreparedSingleSend::new(context, operation, key)?;
        let admission = prepared.admit().map_err(AbortSendError::Governance)?;
        let input = timeout_at(deadline, admission)
            .await
            .map_err(|_| AbortSendError::Governance(WorkError::CapacityWaitTimeout))?
            .map_err(AbortSendError::Governance)?;
        let resources = send_queue_resources(
            &ctx.layout,
            request.participation().backend_process_id(),
            TransportCapacityLane::Control,
            1,
            prepared.operation.queued_bytes(),
        )
        .map_err(|_| {
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
        prepared
            .run(
                guard,
                input,
                gates.generation,
                deadline,
                std::future::pending(),
                AbortReceipt::observe,
            )
            .await
    }
}

/// The same transport capabilities are borrowed by a non-actor qualification
/// or by the failure supervisor. Neither needs a Core mailbox registration.
pub(crate) struct SingleSendContext<'a> {
    pub(crate) ledger: &'a RuntimeLedger,
    pub(crate) scope: &'a ScopeHandle,
    pub(crate) layout: &'a NativeTransportCapacity,
    pub(crate) transport: novarocks_task_codec::TransportBudget,
    pub(crate) authority: &'a NativeAsyncActionAuthority,
    pub(crate) wire: &'a AttemptWireFacts,
    pub(crate) client: &'a Client,
    pub(crate) apply: &'a ApplySendPort,
    pub(crate) clock: &'a Arc<dyn MonotonicClock>,
    pub(crate) observer: ConsumerId,
    pub(crate) executor: &'a Handle,
}

pub(crate) struct PreparedSingleSend<'a> {
    context: SingleSendContext<'a>,
    operation: OperationIntent,
    key: SendKey,
    quote: NativeAsyncActionQuote,
    encoding: novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
}

impl<'a> PreparedSingleSend<'a> {
    pub(crate) fn new<R>(
        context: SingleSendContext<'a>,
        operation: OperationIntent,
        key: SendKey,
    ) -> Result<Self, SingleSendError<R>> {
        let allowed = match &operation {
            OperationIntent::AbortParticipation(_) => true,
            OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                request.native_compatibility_id() == context.wire.native_compatibility_id
            }
            _ => false,
        };
        if !allowed
            || operation.operation_id() != key.operation
            || operation.backend_process_id() != context.client.backend_process_id()
            || !context.scope.belongs_to_ledger(context.ledger)
            || !context.layout.accepts_transport_budget(context.transport)
        {
            return Err(SingleSendError::WrongOwner);
        }
        let (quote, encoding) =
            quote(&operation, context.transport).map_err(SingleSendError::Governance)?;
        Ok(Self {
            context,
            operation,
            key,
            quote,
            encoding,
        })
    }

    pub(crate) fn admit(&self) -> Result<AdmitQueuedInput, WorkError> {
        let queued_quote = queued_input_for_factory(
            0,
            Layout::new::<AdmitQueuedInput>(),
            SourceLayouts {
                original: &[super::proposal_wait::ProposalWait::owned_future_layout()],
            },
        )?;
        AdmitQueuedInput::new(
            self.key,
            self.context.authority,
            self.operation.requires_control_progress(),
            queued_quote,
        )
    }

    /// The caller owns this one driver until its actual Join and original
    /// physical and financial retirement. The stop future can withdraw the
    /// same pending handoff. Once the gate starts, that work and its result
    /// still leave through the original Apply row.
    pub(crate) async fn run<R>(
        self,
        guard: StepRegistration,
        input: super::OriginalQueuedInput,
        generation: Generation,
        deadline: Instant,
        stop: impl Future<Output = ()>,
        observe: fn(&OperationAcknowledgement) -> R,
    ) -> Result<SingleSendResult<R>, SingleSendError<R>> {
        let ctx = self.context;
        if !guard.scope().same_scope(ctx.scope) || input.key() != self.key {
            drop(guard);
            drop(input);
            return Err(SingleSendError::WrongOwner);
        }
        let backend = self.operation.backend_process_id();
        let control_progress = self.operation.requires_control_progress();
        let (lane, kind) = if control_progress {
            (TransportCapacityLane::Control, WorkKind::Settlement)
        } else {
            (TransportCapacityLane::Ordinary, WorkKind::Business)
        };
        let (work, mut capacity) = guard.bind(kind).expect("original single send guard");
        let window = QueueWindow::split(
            &mut capacity,
            ctx.layout.clone(),
            backend,
            lane,
            1,
            self.operation.queued_bytes(),
        )
        .expect("original single send queue geometry");
        let report = ReportSlot::reserve_exit_observer_for_admitted(&mut capacity, ctx.observer)
            .expect("paid single send exit report");
        let original =
            ReportedWork::try_new(work, capacity, report).expect("original single send Work");
        let header = original.work().retirement_observer();
        let panic = PanicSink::new();
        let (owner, exit, launch) =
            match OriginalTransportJoinObligation::prepare(original, ctx.observer, panic.clone()) {
                Ok(original) => original,
                Err(error) => return Err(SingleSendError::Prepare(error, window, input)),
            };
        let mut cleanup = VecDeque::with_capacity(APPLY_DISPOSAL_ROLES);
        for _ in 0..APPLY_DISPOSAL_ROLES {
            cleanup.push_back(cleanup_role(
                ctx.scope,
                window.parent_reservation(),
                ctx.observer,
            ));
        }
        let keys: Arc<[SendKey]> = Arc::new([self.key]);
        let registration = owner.registration();
        let queue = QueueCustody::try_new(window, registration.clone(), keys.clone())
            .unwrap_or_else(|_| panic!("original single send queue"));
        let loan = input
            .bind_send(registration.clone(), 0)
            .unwrap_or_else(|_| panic!("original single send loan"));
        let reply = Arc::new(SingleSendReply {
            key: self.key,
            kind: self.operation.kind(),
            value: Mutex::new(None),
            outcome: Mutex::new(None),
        });
        let bound = Box::new(BoundSend {
            backend,
            control_progress,
            scope_generation: generation,
            ledger: ctx.ledger.clone(),
            scope: ctx.scope.clone(),
            layout: ctx.layout.clone(),
            transport: ctx.transport,
            queue,
            resolved: ResolvedBatch::single(self.operation, loan, keys),
            authority: ctx.authority.clone(),
            original_quote: self.quote,
            encoding: self.encoding,
            wire: ctx.wire.clone(),
            client: ctx.client.clone(),
            apply: ctx.apply.clone(),
            reports: SendReports::Single(reply.clone()),
            clock: ctx.clock.clone(),
            lifecycle: None,
            deadline: Some(deadline),
            original_captures: Box::new(()),
        });
        let mut retirement = Box::new(TransportRetirement::new(owner, cleanup, ctx.observer));
        let handoff = Arc::new(HandoffCell::new());
        let _ = launch.spawn(ctx.executor, exit, |exit, registration| {
            send::run(SendJob {
                leg: TransportLeg {
                    exit,
                    handoff: handoff.clone(),
                    panic: panic.clone(),
                    result: Box::new(SingleSendBodyReport(reply.clone())),
                },
                registration,
                bound,
            })
        });
        let mut result = SingleSendResult {
            receipt: None,
            body: None,
            rejected_attachment: None,
            handoff_failure: None,
            join_failure: None,
            work_panic: None,
            cleanup_panic: None,
        };
        // The driver borrows the original retirement row. A panic while
        // advancing it returns that same owner with the raw cause.
        let drive = async {
            let mut stop = std::pin::pin!(stop);
            let mut stopped = false;
            loop {
                let step = tokio::select! {
                    biased;
                    () = &mut stop, if !stopped && result.receipt.is_none() => {
                        stopped = true;
                        handoff.withdraw();
                        continue;
                    }
                    step = poll_fn(|cx| retirement.poll_step(ctx.executor, cx)) => step,
                };
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
                    assert!(result.receipt.replace(observe(&ack)).is_none());
                }
                if let Some(body) = reply.outcome.lock().unwrap().take() {
                    assert!(result.body.replace(body).is_none());
                }
                match step {
                    Ok(TransportRetirementStep::Retired) => {
                        break Ok(());
                    }
                    Ok(TransportRetirementStep::Advanced | TransportRetirementStep::Scanned) => {}
                    Ok(TransportRetirementStep::Report(original)) => {
                        std::panic::resume_unwind(Box::new(original))
                    }
                    Err(error) => break Err(error),
                }
            }
        };
        match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(drive)).await {
            Ok(Ok(())) => match ctx.scope.reap_observed_header(&header) {
                Ok(true) => Ok(result),
                Ok(false) => Err(SingleSendError::Header(None, retirement, result)),
                Err(error) => Err(SingleSendError::Header(Some(error), retirement, result)),
            },
            Ok(Err(error)) => Err(SingleSendError::Retire(error, retirement, result)),
            Err(panic) => Err(SingleSendError::DrivePanic(panic, retirement, result)),
        }
    }
}

fn quote(
    operation: &OperationIntent,
    budget: novarocks_task_codec::TransportBudget,
) -> Result<
    (
        NativeAsyncActionQuote,
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    ),
    WorkError,
> {
    let arc = |layout| arc_layout(layout).map_err(|_| WorkError::ArithmeticOverflow);
    let mut layouts = Vec::from([
        arc(Layout::new::<HandoffCell>())?,
        Layout::new::<SingleSendBodyReport>(),
        Layout::new::<TransportRetirement>(),
        Layout::new::<BoundSend>(),
    ]);
    layouts.extend(shared_source_layouts()?);
    original_run_with_reports(
        std::slice::from_ref(operation),
        budget,
        SourceLayouts { original: &layouts },
        arc(Layout::new::<SingleSendReply>())?.size(),
    )
}
