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

//! One immutable RPC per explicit core generation. This executor connects,
//! reads, validates and reports facts. Reconnect, backoff, liveness timeout and
//! generation selection remain in the pure attempt owner.

use super::observations::{ObservationError, OrderedObservationInput, StreamKey};
use crate::native::task_transport::wire::{
    self, CoveredEventValidationError, CoveredRequestedVersions,
};
#[cfg(test)]
use crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort;
use novarocks_attempt_protocol::{
    input::{
        AttemptInput, ExecutorFact, ObservationEvent, ObservationFact, ObservationFailure,
        SubscriptionPhaseSnapshot,
    },
    observation::SubscriptionRequest,
};
use novarocks_control_runtime::{
    actor::Actor,
    actor_turn::{ActorTurnError, StandingInputObligation},
    coalescing_input::{CoalescingInput, Merge},
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::ParticipationRef;
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::novarocks::TaskStatusStreamEvent;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, DecodedCoveredSubscription,
    QuiesceObservationCursor,
};
#[cfg(test)]
use novarocks_workload_control::WorkScope;
use novarocks_workload_control::{WorkError, WorkOwner};
use std::{
    collections::BTreeMap,
    future::Future,
    num::NonZeroU64,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::watch;

#[path = "subscription/failure.rs"]
mod failure;
pub(crate) use failure::{OriginalSubscriptionFailure, SubscriptionFailureRecord};

#[path = "subscription/native_io.rs"]
mod native_io;

pub(in crate::task_execution::host) use native_io::native_subscription_io_backing_bytes;
pub(crate) use native_io::{NativeSubscriptionFailure, NativeSubscriptionFailureCustody};

pub(crate) type SubscriptionIoFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// The native owner implements this thin port with its SAME cached channel,
/// deployment authentication and exact typed connection-loss classifier. An
/// error already references the retained original cause; no string classifier
/// or second client exists in this executor. Constructing the returned future
/// performs no I/O/start; its actual first poll owns channel/dial/ready work.
pub(crate) trait CoveredSubscriptionIo: Send + Sync + 'static {
    fn open(
        &self,
        request: DecodedCoveredSubscription,
    ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>;
}
pub(crate) trait CoveredSubscriptionStream: Send {
    fn next(
        &mut self,
    ) -> SubscriptionIoFuture<'_, Result<Option<TaskStatusStreamEvent>, ObservationFailure>>;
}
/// Pure projection cannot retain rich decode errors in AP. The original host
/// failure registry keeps each exact error and returns its typed neutral fact.
pub(crate) trait SubscriptionFailureCustody: Send + Sync + 'static {
    fn malformed(&self, key: StreamKey, original: ProtocolError) -> ObservationFailure;
    fn ingress_rejected(&self, key: StreamKey, original: ObservationError) -> ObservationFailure;
    fn conflicting(
        &self,
        key: StreamKey,
        original: CoveredEventValidationError,
    ) -> ObservationFailure;
}
struct PhaseUpdate<A: Actor> {
    snapshot: Option<SubscriptionPhaseSnapshot>,
    project: Option<fn(AttemptInput) -> A::Input>,
}
impl<A: Actor> Merge for PhaseUpdate<A> {
    fn merge(&mut self, newer: Self) {
        let Some(newer_snapshot) = newer.snapshot else {
            return;
        };
        match self.snapshot.as_mut() {
            None => {
                self.snapshot = Some(newer_snapshot);
                self.project = newer.project;
            }
            Some(old) => {
                assert_eq!(
                    (old.participation, old.generation, old.opening_started_at),
                    (
                        newer_snapshot.participation,
                        newer_snapshot.generation,
                        newer_snapshot.opening_started_at
                    ),
                    "one original phase input cannot merge other generations or starts"
                );
                if let Some(at) = newer_snapshot.opened_at {
                    assert!(
                        old.opened_at.is_none_or(|original| original == at),
                        "original open time cannot change"
                    );
                    old.opened_at = Some(at);
                }
            }
        }
    }
    // The merge identity is not a timestamped phase. This private publisher
    // never exposes CoalescingInput::waker, which cannot mint such a phase.
    fn unit() -> Self {
        Self {
            snapshot: None,
            project: None,
        }
    }
}
fn phase_input<A: Actor>(update: &PhaseUpdate<A>) -> A::Input {
    let snapshot = update
        .snapshot
        .expect("a real original phase precedes projection");
    update.project.expect("original phase input projection")(AttemptInput::Executor(
        ExecutorFact::SubscriptionProgress(snapshot),
    ))
}
/// Independent original Lifecycle standing; start/open facts never wait for
/// the generation's actual ExitCustody. Only same-generation complete phase
/// snapshots coalesce, retaining both original execution instants.
pub(crate) struct SubscriptionPhasePublisher<A: Actor> {
    input: CoalescingInput<A, PhaseUpdate<A>>,
    key: StreamKey,
    project: fn(AttemptInput) -> A::Input,
    closed: AtomicBool,
}
impl<A: Actor> SubscriptionPhasePublisher<A> {
    pub(crate) fn new(
        input: StandingInputObligation<A>,
        key: StreamKey,
        project: fn(AttemptInput) -> A::Input,
    ) -> Arc<Self> {
        Arc::new(Self {
            input: CoalescingInput::new(input, phase_input::<A>),
            key,
            project,
            closed: AtomicBool::new(false),
        })
    }
    fn report(
        &self,
        started: MonotonicInstant,
        opened: Option<MonotonicInstant>,
    ) -> Result<(), ObservationError> {
        self.input
            .notify(PhaseUpdate {
                snapshot: Some(SubscriptionPhaseSnapshot {
                    participation: self.key.participation,
                    generation: self.key.generation,
                    opening_started_at: started,
                    opened_at: opened,
                }),
                project: Some(self.project),
            })
            .map(|_| ())
            .map_err(ObservationError::Runtime)
    }
    pub(crate) fn role_id(&self) -> novarocks_control_runtime::actor_turn::StandingInputId {
        self.input.id()
    }
    /// The Source routes only this original standing role's actual retire
    /// receipts here, using the pre-retirement runtime custody metadata.
    #[cfg(test)]
    pub(crate) fn current_input(&self) -> Result<Option<InputId>, ObservationError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        self.input.in_flight().map_err(ObservationError::Runtime)
    }
    #[cfg(test)]
    pub(crate) fn input_retired(&self, input: InputId) -> Result<(), ObservationError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(current) = self.input.in_flight().map_err(ObservationError::Runtime)? {
            // A newer occurrence means a prior idle check already refilled it.
            if current > input {
                return Ok(());
            }
            return Err(ObservationError::Busy);
        }
        self.input
            .after_retire()
            .map(|_| ())
            .map_err(ObservationError::Runtime)
    }
    pub(crate) fn retire_indexed_after_exit(&self, input: InputId) -> Result<bool, ActorTurnError> {
        self.input.retire_indexed_after_exit(input)
    }

    pub(crate) fn retire_after_failure(&self) -> Result<bool, ActorTurnError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.input.abandon_pending_after_failure();
        match self.input.take_failed_role_payload() {
            Ok(Some(original)) => {
                original.dispose()?;
                Ok(true)
            }
            Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {
                self.input.retire_failed_after_exit()?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn close_after_exit(&self) -> Result<(), ObservationError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if self
            .input
            .in_flight()
            .map_err(ObservationError::Runtime)?
            .is_some()
        {
            return Err(ObservationError::Busy);
        }
        // The original body has exited, so no further reports can race this.
        // If retirement's Source callback is pending, flush the complete latest
        // phase before closing. A final opened_at must never be discarded.
        if self
            .input
            .after_retire()
            .map_err(ObservationError::Runtime)?
            .is_some()
        {
            return Err(ObservationError::Busy);
        }
        self.input
            .close_after_exit()
            .map_err(ObservationError::Runtime)?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GenerationPhase {
    Admitted,
    Running,
    Exited,
    Unknown,
}
struct GenerationRecord {
    phase: GenerationPhase,
    stop: watch::Sender<bool>,
    applied: Arc<SubscriptionRequest>,
    // The erased holder contains no Core policy, only the actual original
    // Lifecycle role and its complete timestamped phase snapshots.
    phase_exit: Arc<dyn PhaseExit>,
}
trait PhaseExit: Send + Sync {
    fn close_after_exit(&self) -> Result<(), ObservationError>;
}
impl<A: Actor> PhaseExit for SubscriptionPhasePublisher<A> {
    fn close_after_exit(&self) -> Result<(), ObservationError> {
        SubscriptionPhasePublisher::close_after_exit(self)
    }
}
struct SourceState {
    owner: Option<WorkOwner>,
    open: bool,
    max_generation: Option<NonZeroU64>,
    jobs: BTreeMap<NonZeroU64, GenerationRecord>,
}
/// One logical Participation consumes ONE child of the original WC Source
/// cohort. Generations borrow this stable Source; closing one immutable RPC
/// cannot complete the parent child or consume another cohort position.
pub(crate) struct SubscriptionSource<A: Actor> {
    participation: ParticipationRef,
    ingress: OrderedObservationInput<A>,
    state: Mutex<SourceState>,
}
#[derive(Debug)]
#[allow(
    dead_code,
    reason = "Source refusal retains the original work or request reason with the exact stream."
)]
pub(crate) enum SubscriptionSourceError {
    Work(WorkError),
    Ingress(ObservationError),
    Request(ProtocolError),
    Closed,
    WrongParticipation,
    ReusedGeneration,
    UnknownGeneration,
    JobsStillLive,
}
impl<A: Actor> SubscriptionSource<A> {
    #[cfg(test)]
    pub(crate) fn from_cohort(
        participation: ParticipationRef,
        parent: &WorkScope,
        cohort: &NativeSourceCohort,
        ingress: OrderedObservationInput<A>,
    ) -> Result<Arc<Self>, SubscriptionSourceError> {
        let mut owner = None;
        let notification = cohort
            .take_child_in(parent, &mut owner)
            .map_err(SubscriptionSourceError::Work)?;
        let source = Self::from_owner(participation, owner.expect("original WC child"), ingress);
        notification.notify();
        Ok(source)
    }

    pub(crate) fn from_owner(
        participation: ParticipationRef,
        owner: WorkOwner,
        ingress: OrderedObservationInput<A>,
    ) -> Arc<Self> {
        Arc::new(Self {
            participation,
            ingress,
            state: Mutex::new(SourceState {
                owner: Some(owner),
                open: true,
                max_generation: None,
                jobs: BTreeMap::new(),
            }),
        })
    }
    #[cfg(test)]
    pub(crate) fn scope(&self) -> Result<WorkScope, SubscriptionSourceError> {
        self.state
            .lock()
            .expect("covered original Source")
            .owner
            .as_ref()
            .map(WorkOwner::scope)
            .ok_or(SubscriptionSourceError::Closed)
    }
    pub(crate) fn matches(
        &self,
        participation: ParticipationRef,
        root: &super::observations::RootControlBinding,
    ) -> bool {
        self.participation == participation && self.ingress.root_binding().same_original(root)
    }

    pub(crate) fn stream_limit(&self) -> usize {
        self.ingress.stream_limit()
    }

    pub(crate) fn generation_backing_bytes() -> Result<usize, ObservationError> {
        use super::observations::backing;
        backing::sum([
            backing::tree::<NonZeroU64, GenerationRecord>(1)?,
            backing::arc::<SubscriptionPhasePublisher<A>>()?.size(),
            novarocks_type_contract::owned_resources::layout::arc_layout(CoalescingInput::<
                A,
                PhaseUpdate<A>,
            >::backing_payload_layout(
            ))
            .map_err(|_| ObservationError::Budget)?
            .size(),
        ])
    }

    pub(crate) fn can_register_generation(&self) -> bool {
        self.ingress.can_register_stream()
    }

    /// The caller has already admitted this generation's original common-
    /// runtime Work/ExitCustody. This method adds only its exact Source record;
    /// the original exit authority stays with the execution carrier.
    pub(crate) fn prepare(
        self: &Arc<Self>,
        request: Arc<SubscriptionRequest>,
        phase: &Arc<SubscriptionPhasePublisher<A>>,
    ) -> Result<SubscriptionGeneration<A>, SubscriptionSourceError> {
        if request.participation != self.participation {
            return Err(SubscriptionSourceError::WrongParticipation);
        }
        let key = StreamKey {
            participation: request.participation,
            generation: request.generation,
        };
        if phase.key != key {
            return Err(SubscriptionSourceError::WrongParticipation);
        }
        let mut state = self.state.lock().expect("covered original Source");
        if !state.open || state.owner.is_none() {
            return Err(SubscriptionSourceError::Closed);
        }
        if state
            .max_generation
            .is_some_and(|old| request.generation <= old)
        {
            return Err(SubscriptionSourceError::ReusedGeneration);
        }
        self.ingress
            .register_stream(key)
            .map_err(SubscriptionSourceError::Ingress)?;
        let (stop, receiver) = watch::channel(false);
        state.max_generation = Some(request.generation);
        state.jobs.insert(
            request.generation,
            GenerationRecord {
                phase: GenerationPhase::Admitted,
                stop,
                applied: request.clone(),
                phase_exit: phase.clone(),
            },
        );
        drop(state);
        Ok(SubscriptionGeneration {
            request: Some(request),
            receiver: Some(receiver),
            phase: Some(phase.clone()),
            guard: Some(GenerationExitObligation {
                source: self.clone(),
                key,
                consumed: false,
            }),
        })
    }
    /// This is the original update_applied_request contract. The current RPC
    /// stays immutable; AP must explicitly Close/Open to reconcile another cut.
    pub(crate) fn adjust_read_window(
        &self,
        request: Arc<SubscriptionRequest>,
    ) -> Result<(), SubscriptionSourceError> {
        if request.participation != self.participation {
            return Err(SubscriptionSourceError::WrongParticipation);
        }
        novarocks_task_codec::operation::encode_covered_subscribe_task_status(&decode_request(
            &request,
        ))
        .map_err(SubscriptionSourceError::Request)?;
        let old = {
            let mut state = self.state.lock().expect("covered original Source");
            let old = state
                .max_generation
                .is_some_and(|original| request.generation <= original);
            let Some(record) = state.jobs.get_mut(&request.generation) else {
                return if old {
                    Ok(())
                } else {
                    Err(SubscriptionSourceError::UnknownGeneration)
                };
            };
            std::mem::replace(&mut record.applied, request)
        };
        drop(old);
        Ok(())
    }
    /// Source-qualified observed applied facts, never a received-frame cursor.
    #[cfg(test)]
    pub(crate) fn applied_request(
        &self,
        generation: NonZeroU64,
    ) -> Result<Arc<SubscriptionRequest>, SubscriptionSourceError> {
        self.state
            .lock()
            .expect("covered original Source")
            .jobs
            .get(&generation)
            .map(|record| record.applied.clone())
            .ok_or(SubscriptionSourceError::UnknownGeneration)
    }
    pub(crate) fn close_generation(
        &self,
        generation: NonZeroU64,
    ) -> Result<(), SubscriptionSourceError> {
        let stop = {
            let state = self.state.lock().expect("covered original Source");
            match state.jobs.get(&generation) {
                Some(record) => record.stop.clone(),
                None if state.max_generation.is_some_and(|old| generation <= old) => return Ok(()),
                None => return Err(SubscriptionSourceError::UnknownGeneration),
            }
        };
        // A cancellation wake is not actual exit. Keep the same record and its
        // WorkOwner until the original future/captures really leave.
        stop.send_replace(true);
        Ok(())
    }
    /// Reap only after the caller consumed this exact generation's actual
    /// common-runtime exit/report and all independent confirmation captures.
    pub(crate) fn reap_generation(
        &self,
        generation: NonZeroU64,
    ) -> Result<Arc<SubscriptionRequest>, SubscriptionSourceError> {
        let phase = {
            let state = self.state.lock().expect("covered original Source");
            let record = state
                .jobs
                .get(&generation)
                .filter(|r| r.phase == GenerationPhase::Exited)
                .ok_or(SubscriptionSourceError::JobsStillLive)?;
            record.phase_exit.clone()
        };
        phase
            .close_after_exit()
            .map_err(SubscriptionSourceError::Ingress)?;
        self.ingress
            .retire_stream(StreamKey {
                participation: self.participation,
                generation,
            })
            .map_err(SubscriptionSourceError::Ingress)?;
        let record = self
            .state
            .lock()
            .expect("covered original Source")
            .jobs
            .remove(&generation)
            .expect("same original retired generation");
        Ok(record.applied)
    }
    pub(crate) fn close_admission(&self) {
        self.state.lock().expect("covered original Source").open = false;
    }

    #[cfg(test)]
    pub(crate) fn close_new_jobs(&self) {
        let stops = {
            let mut state = self.state.lock().expect("covered original Source");
            state.open = false;
            state
                .jobs
                .values()
                .map(|record| record.stop.clone())
                .collect::<Vec<_>>()
        };
        for stop in stops {
            stop.send_replace(true);
        }
    }
    /// Actual futures/captures exit before the SAME WC child is completed.
    /// Pending or Unknown preserves the original owner for later convergence.
    pub(crate) fn take_owner_after_jobs(
        &self,
    ) -> Result<Option<WorkOwner>, SubscriptionSourceError> {
        let mut state = self.state.lock().expect("covered original Source");
        if state.open || !state.jobs.is_empty() {
            return Err(SubscriptionSourceError::JobsStillLive);
        }
        Ok(state.owner.take())
    }

    #[cfg(test)]
    pub(crate) fn close_after_jobs(&self) -> Result<(), SubscriptionSourceError> {
        if let Some(owner) = self.take_owner_after_jobs()? {
            owner.complete_after_terminal_cancel_settled();
        }
        Ok(())
    }
}
/// Checked by the stable Source registry: an unconsumed Drop records Unknown
/// and prevents generation reaping and original WC-child completion.
struct GenerationExitObligation<A: Actor> {
    source: Arc<SubscriptionSource<A>>,
    key: StreamKey,
    consumed: bool,
}
impl<A: Actor> GenerationExitObligation<A> {
    fn started(&self) {
        self.source
            .state
            .lock()
            .expect("covered original Source")
            .jobs
            .get_mut(&self.key.generation)
            .expect("same admitted generation")
            .phase = GenerationPhase::Running;
    }
    fn finish(mut self) {
        self.source
            .state
            .lock()
            .expect("covered original Source")
            .jobs
            .get_mut(&self.key.generation)
            .expect("same original generation")
            .phase = GenerationPhase::Exited;
        self.consumed = true;
        self.source.ingress.publisher_exited(self.key);
    }
}
impl<A: Actor> Drop for GenerationExitObligation<A> {
    fn drop(&mut self) {
        if !self.consumed {
            self.source
                .state
                .lock()
                .expect("covered original Source")
                .jobs
                .get_mut(&self.key.generation)
                .expect("same unresolved generation")
                .phase = GenerationPhase::Unknown;
            self.source.ingress.publisher_exit_unknown(self.key);
        }
    }
}
/// A metadata-admitted generation has not started its RPC or Work. Its caller
/// wraps run() in the original GatedWork/run_future credentialed execution.
pub(crate) struct SubscriptionGeneration<A: Actor> {
    request: Option<Arc<SubscriptionRequest>>,
    receiver: Option<watch::Receiver<bool>>,
    guard: Option<GenerationExitObligation<A>>,
    phase: Option<Arc<SubscriptionPhasePublisher<A>>>,
}
pub(crate) enum SubscriptionRunError {
    Ingress(ObservationError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubscriptionEnd {
    CoreClosed,
    RemoteClosed,
    Failed,
}
impl<A: Actor> SubscriptionGeneration<A> {
    /// No timeout, retry, generation mint or recovery policy runs in this body.
    /// ConfirmReadState is handled independently through ingress.confirm(), so
    /// it can answer while this body is stopped on either capacity or network.
    pub(crate) fn run(
        mut self,
        io: Arc<dyn CoveredSubscriptionIo>,
        failures: Arc<dyn SubscriptionFailureCustody>,
    ) -> impl Future<Output = Result<SubscriptionEnd, SubscriptionRunError>> + Send {
        let request = self
            .request
            .take()
            .expect("original immutable subscription request");
        let mut receiver = self.receiver.take().expect("original generation stop view");
        let guard = self
            .guard
            .take()
            .expect("original generation exit obligation");
        let phase = self
            .phase
            .take()
            .expect("original generation phase publisher");
        let key = guard.key;
        let ingress = guard.source.ingress.clone();
        let body = async move {
            let mut credit = Some(tokio::select! {
                biased;
                _ = stopped(&mut receiver) => { ingress.job_closed(key).map_err(SubscriptionRunError::Ingress)?; return Ok(SubscriptionEnd::CoreClosed); }
                credit = ingress.reserve_read(key) => credit.map_err(SubscriptionRunError::Ingress)?,
            });
            let request = decode_request(&request);
            let mut opening = io.open(request.clone());
            let mut published_phase = None;
            let stream = tokio::select! {
                _ = stopped(&mut receiver) => { ingress.job_closed(key).map_err(SubscriptionRunError::Ingress)?; return Ok(SubscriptionEnd::CoreClosed); }
                opened = std::future::poll_fn(|cx| {
                    let polled = match ingress.poll_open(key, &mut credit, opening.as_mut(), cx) {
                        Ok(polled) => polled,
                        Err(error) => return Poll::Ready(Err(SubscriptionRunError::Ingress(error))),
                    };
                    let actual_phase = (polled.started_at, polled.opened_at);
                    if published_phase != Some(actual_phase) {
                        if let Err(error) = phase.report(polled.started_at, polled.opened_at) {
                            return Poll::Ready(Err(SubscriptionRunError::Ingress(error)));
                        }
                        published_phase = Some(actual_phase);
                    }
                    polled.result.map(Ok)
                }) => opened?,
            };
            drop(opening);
            let mut stream = match stream {
                Ok(stream) => stream,
                Err((receipt, failure)) => {
                    ingress
                        .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
                        .map_err(SubscriptionRunError::Ingress)?;
                    return Ok(SubscriptionEnd::Failed);
                }
            };
            let requested = CoveredRequestedVersions::from_request(&request);
            loop {
                let (receipt, message) = {
                    let mut next = stream.next();
                    tokio::select! {
                        _ = stopped(&mut receiver) => { ingress.job_closed(key).map_err(SubscriptionRunError::Ingress)?; return Ok(SubscriptionEnd::CoreClosed); }
                        received = std::future::poll_fn(|cx| ingress.poll_receive(key, &mut credit, next.as_mut(), cx)) =>
                            received.map_err(SubscriptionRunError::Ingress)?,
                    }
                };
                let wire = match message {
                    Ok(Some(wire)) => wire,
                    Ok(None) => {
                        ingress
                            .complete_receive(&receipt, ObservationEvent::Closed, 0)
                            .map_err(SubscriptionRunError::Ingress)?;
                        return Ok(SubscriptionEnd::RemoteClosed);
                    }
                    Err(failure) => {
                        ingress
                            .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
                            .map_err(SubscriptionRunError::Ingress)?;
                        return Ok(SubscriptionEnd::Failed);
                    }
                };
                let encoded = prost::Message::encoded_len(&wire);
                let event = match wire::decode_covered_status_event(
                    &wire,
                    FieldPath::root("covered_status_stream_event"),
                ) {
                    Ok(event) => event,
                    Err(original) => {
                        drop(wire);
                        let failure = failures.malformed(key, original);
                        ingress
                            .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
                            .map_err(SubscriptionRunError::Ingress)?;
                        return Ok(SubscriptionEnd::Failed);
                    }
                };
                drop(wire);
                if let Err(original) =
                    wire::validate_covered_event_participation(&request, &requested, &event)
                {
                    drop(event);
                    let failure = failures.conflicting(key, original);
                    ingress
                        .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
                        .map_err(SubscriptionRunError::Ingress)?;
                    return Ok(SubscriptionEnd::Failed);
                }
                if let Err(error) =
                    ingress.complete_receive(&receipt, project_event(event), encoded)
                {
                    if matches!(error, ObservationError::Oversized { .. }) {
                        let failure = failures.ingress_rejected(key, error);
                        ingress
                            .complete_receive(&receipt, ObservationEvent::Failed(failure), 0)
                            .map_err(SubscriptionRunError::Ingress)?;
                        return Ok(SubscriptionEnd::Failed);
                    }
                    return Err(SubscriptionRunError::Ingress(error));
                }
                credit = Some(tokio::select! {
                    _ = stopped(&mut receiver) => { ingress.job_closed(key).map_err(SubscriptionRunError::Ingress)?; return Ok(SubscriptionEnd::CoreClosed); }
                    credit = ingress.reserve_read(key) => credit.map_err(SubscriptionRunError::Ingress)?,
                });
            }
        };
        SubscriptionJobFuture {
            inner: Some(Box::pin(body)),
            guard: Some(guard),
            started: false,
        }
    }
}
impl<A: Actor> Drop for SubscriptionGeneration<A> {
    fn drop(&mut self) {
        // A never-started generation still destroys the actual request and
        // stop-view captures before reporting its local Source job exited.
        drop(self.request.take());
        drop(self.receiver.take());
        drop(self.phase.take());
        if let Some(guard) = self.guard.take() {
            let _ = guard.source.ingress.job_closed(guard.key);
            guard.finish();
        }
    }
}
struct SubscriptionJobFuture<F, A: Actor> {
    inner: Option<Pin<Box<F>>>,
    guard: Option<GenerationExitObligation<A>>,
    started: bool,
}
impl<F: Future, A: Actor> Future for SubscriptionJobFuture<F, A> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        if !this.started {
            this.guard
                .as_ref()
                .expect("original generation exit")
                .started();
            this.started = true;
        }
        this.inner
            .as_mut()
            .expect("live original subscription job")
            .as_mut()
            .poll(cx)
    }
}
impl<F, A: Actor> Drop for SubscriptionJobFuture<F, A> {
    fn drop(&mut self) {
        // If actual F destruction panics, the unconsumed guard records Unknown;
        // the outer original ObligatedFuture keeps that raw panic and custody.
        drop(self.inner.take());
        if let Some(guard) = self.guard.take() {
            let _ = guard.source.ingress.job_closed(guard.key);
            guard.finish();
        }
    }
}
async fn stopped(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
fn decode_request(request: &SubscriptionRequest) -> DecodedCoveredSubscription {
    DecodedCoveredSubscription {
        participation: request.participation,
        generation: request.generation,
        status_cursors: request.status_cursors.to_vec(),
        task_convergence_cursors: request.task_convergence_cursors.to_vec(),
        participation_cursor: request.participation_cursor,
        quiesce_cursor: request.quiesce_cursor.map(|v| QuiesceObservationCursor {
            participation: v.participation,
            fence_version: v.fence_version,
        }),
        required_identities: request.required_identities.to_vec(),
    }
}
fn project_event(event: CoveredStatusStreamEvent) -> ObservationEvent {
    let fact = match event.fact {
        CoveredStatusStreamFact::Status(value) => ObservationFact::Status(value),
        CoveredStatusStreamFact::Gone(value) => ObservationFact::Gone(value),
        CoveredStatusStreamFact::ParticipationConvergence(value) => {
            ObservationFact::ParticipationConvergence(value)
        }
        CoveredStatusStreamFact::TaskConvergence(value) => ObservationFact::TaskConvergence(value),
        CoveredStatusStreamFact::Quiesce(value) => ObservationFact::Quiesce(Arc::new(value)),
        CoveredStatusStreamFact::StatusUnchanged(value) => ObservationFact::StatusUnchanged(value),
        CoveredStatusStreamFact::TaskConvergenceUnchanged(value) => {
            ObservationFact::TaskConvergenceUnchanged(value)
        }
        CoveredStatusStreamFact::Unknown(value) => ObservationFact::Unknown(value),
        CoveredStatusStreamFact::CatchUpComplete(value) => ObservationFact::CatchUpComplete {
            generation: value.generation,
            initial_cut: value.initial_cut,
        },
        CoveredStatusStreamFact::Bookmark(value) => ObservationFact::Bookmark {
            generation: value.generation,
            sequence: value.sequence,
            covered_prefix: value.covered_prefix,
            source_cut: value.source_cut,
        },
    };
    ObservationEvent::Fact {
        source_revision: event.source_revision,
        fact,
    }
}
