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

//! One original paid ingress serializes all streams and root controls. Its
//! accepted order is its actual FIFO fill order, including across mailbox
//! classes. Rich storage/byte loans stay here until the runtime input retires.

pub(crate) mod credits;
pub(crate) mod host_turn;
pub(crate) mod read_progress;
pub(crate) mod source;

use novarocks_attempt_protocol::{
    failure::AttemptFailure,
    input::{
        AttemptInput, ExecutorFact, ObservationBatch, ObservationEvent, ObservationFact,
        ObservationFailure, ObservationOpening, ReadPauseCoverage, ReadProgressSnapshot,
        ReadStateSnapshot, ReceivedObservation, ReceptionCoverage,
    },
};
use novarocks_control_runtime::{
    actor::Actor,
    actor_turn::{
        ActorRegistration, ActorTurnError, FailedSourcePayload, InputAuthor, StandingInputId,
        StandingInputObligation,
    },
    causal_input::{CausalFillFailure, CausalInputObligation},
    obligation::OwnedPositionObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptRequestToken, LogicalToAttempt, LogicalToAttemptBody,
    MonotonicClock, MonotonicInstant,
};
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use std::alloc::Layout;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    num::NonZeroU64,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};
use std::{future::Future, pin::Pin};
use tokio::sync::{Notify, futures::Notified};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct StreamKey {
    pub(crate) participation: ParticipationRef,
    pub(crate) generation: NonZeroU64,
}
/// The actual registration witness establishes one original Source origin.
/// Its activation/root are frozen data; none of these fields grants storage,
/// bytes, canonical access or permission to issue a second request.
struct RootControlOrigin {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    reply_author: OnceLock<InputAuthor>,
}
#[derive(Clone)]
pub(crate) struct RootControlBinding(Arc<RootControlOrigin>);
impl RootControlBinding {
    pub(crate) fn for_registered_source(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Result<Self, ObservationError> {
        if activation.execution() != root.query_execution_id() {
            return Err(ObservationError::InvalidRootControl);
        }
        Ok(Self(Arc::new(RootControlOrigin {
            registration,
            activation,
            root,
            reply_author: OnceLock::new(),
        })))
    }
    pub(crate) fn registration(&self) -> &ActorRegistration {
        &self.0.registration
    }
    pub(crate) fn activation(&self) -> AttemptActivationIdentity {
        self.0.activation
    }
    pub(crate) fn root(&self) -> TaskIdentity {
        self.0.root
    }

    pub(crate) fn bind_reply_author(&self, author: InputAuthor) {
        assert!(self.0.registration.matches_input_author(&author));
        assert_ne!(self.0.registration.scope_id(), author.work().scope);
        assert!(self.0.reply_author.set(author).is_ok());
    }

    pub(crate) fn reply_author(&self) -> Option<&InputAuthor> {
        self.0.reply_author.get()
    }

    pub(crate) fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    /// Only the original canonical owner issues this after installing the real
    /// request. The ref is passive; the owner must still validate its live row.
    pub(crate) fn reference(&self, token: AttemptRequestToken) -> RootControlRef {
        RootControlRef {
            token,
            origin: self.0.clone(),
        }
    }
}
/// A compact, source-qualified correlation reference. The original rich QA
/// request/oneshot remains in its canonical owner; neither it nor the complete
/// LogicalToAttempt enum is copied into the ordered queue.
pub(crate) struct RootControlRef {
    token: AttemptRequestToken,
    origin: Arc<RootControlOrigin>,
}
impl RootControlRef {
    pub(crate) fn token(&self) -> AttemptRequestToken {
        self.token
    }
    pub(crate) fn belongs_to(&self, binding: &RootControlBinding) -> bool {
        Arc::ptr_eq(&self.origin, &binding.0)
    }
}

/// Exact inline/carrier geometries for the original metadata author. This is a
/// description, NEVER an allocation or byte grant. The existing Source author
/// must cover real container/backing allocations, admitted stream inventory,
/// queued/deferred Entry storage, the batch (at most max_batch_items), and the
/// old+new coexistence of logical -> AP -> actual Actor input projections. It
/// must not infer byte funding from an OwnedPosition or enlarge any window.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservationMetadataLayout {
    pub(crate) entry: Layout,
    pub(crate) root_reference: Layout,
    pub(crate) actor_input: Layout,
    pub(crate) ap_projection: Layout,
    pub(crate) logical_projection: Layout,
    pub(crate) batch_item: Layout,
    pub(crate) in_flight: Layout,
    pub(crate) failed_fill: Layout,
    pub(crate) read_state: Layout,
    pub(crate) read_confirmation: Layout,
    pub(crate) confirmation_waiter: Layout,
    pub(crate) read_progress: Layout,
    pub(crate) total_positions: usize,
    pub(crate) max_batch_items: usize,
}
/// Checked by the original metadata author. This object retains its actual
/// already-paid backing/claim, not a bool, layout, numeric id or lazy ref.
/// Only retire_after_exit may positively consume it. Unconsumed Drop MUST keep
/// the original backing unresolved/UNKNOWN; it must not manufacture exit.
/// There is deliberately no default implementation or P10c-side constructor.
pub(crate) trait ObservationMetadataObligation: Send {
    fn retire_after_exit(self: Box<Self>) -> Result<(), MetadataRetirementRefusal>;
}
pub(crate) struct MetadataRetirementRefusal {
    pub(crate) error: ObservationError,
    pub(crate) original: Box<dyn ObservationMetadataObligation>,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservationBudget {
    pub(crate) queued_frames: usize,
    pub(crate) total_positions: usize,
    pub(crate) total_bytes: usize,
    pub(crate) max_frame_bytes: usize,
    pub(crate) control_bytes: usize,
}
impl ObservationBudget {
    /// Same original intake recipe; these are claims against the Source's paid
    /// pool, not a second grant or a new configurable transport quota.
    pub(crate) fn for_task_attempt() -> Self {
        let max_frame_bytes =
            novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES + 4096;
        // This is ONLY the original canonical-request share. Its owner retains
        // that one share through real request/reply custody. Queue wrappers and
        // transient projections require the separate original metadata author.
        let control_bytes = std::mem::size_of::<
            novarocks_query_application::coordination::AcceptedRootControlRequest,
        >()
        .max(1);
        Self {
            queued_frames: 4096,
            total_positions: 4098,
            total_bytes: max_frame_bytes * 4 + control_bytes,
            max_frame_bytes,
            control_bytes,
        }
    }
    fn validate(self) -> Result<(), ObservationError> {
        if self.queued_frames == 0
            || self.total_positions < self.queued_frames.saturating_add(2)
            || self.max_frame_bytes == 0
            || self.control_bytes == 0
            || self.total_bytes < self.max_frame_bytes.saturating_add(self.control_bytes)
        {
            return Err(ObservationError::Budget);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservationError {
    Budget,
    Closed,
    Busy,
    UnknownStream,
    GenerationAlreadyRegistered,
    StreamClosed,
    ClockRegression,
    ConfirmationBeforeRequest,
    InvalidRootControl,
    InvalidOpening,
    ResumeRevisionExhausted,
    Oversized { charged: usize, reserved: usize },
    Runtime(ActorTurnError),
}

impl std::fmt::Display for ObservationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "observation source refused: {self:?}")
    }
}

impl std::error::Error for ObservationError {}

/// A real bound Input pool slot is lent to a read/queued/applying object. Its
/// Drop returns that SAME obligation to the original Source pool; it never
/// retires the Source's physical position. Only actual Source exit may do so.
pub(crate) struct InputSlotLoan {
    original: Option<OwnedPositionObligation>,
    refund: Option<Box<dyn FnOnce(OwnedPositionObligation) + Send>>,
}
impl InputSlotLoan {
    pub(crate) fn from_source(
        original: OwnedPositionObligation,
        refund: impl FnOnce(OwnedPositionObligation) + Send + 'static,
    ) -> Self {
        assert_eq!(
            original.kind(),
            novarocks_control_runtime_core::owned_storage::OwnedSlotKind::Input
        );
        Self {
            original: Some(original),
            refund: Some(Box::new(refund)),
        }
    }
}
impl Drop for InputSlotLoan {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            self.refund
                .take()
                .expect("original observation slot refund")(original);
        }
    }
}
/// An uncloneable claim in the SAME existing observation Source byte window.
/// Actual holder Drop refunds that original window. It does not fund WC bytes
/// or transport-ledger dimensions. Refusal preserves the same live claim.
pub(crate) trait ObservationByteLoan: Send {
    fn reserved_bytes(&self) -> usize;
    fn shrink_to(&mut self, charged_bytes: usize) -> Result<(), ObservationError>;
}
/// Field order releases bytes before the pool slot becomes available again.
pub(crate) struct ObservationCredit {
    bytes: Option<Box<dyn ObservationByteLoan>>,
    slot: Option<InputSlotLoan>,
    returned: Option<Box<dyn FnOnce() + Send>>,
}
impl ObservationCredit {
    /// returned is the original Source's metadata-only final return commit.
    /// Refund notifications/foreign calls belong to the prior byte/slot steps;
    /// they must not run after acknowledging the original claim.
    pub(crate) fn from_source(
        slot: InputSlotLoan,
        bytes: Box<dyn ObservationByteLoan>,
        returned: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            bytes: Some(bytes),
            slot: Some(slot),
            returned: Some(Box::new(returned)),
        }
    }
    fn charge(&mut self, actual: usize) -> Result<(), ObservationError> {
        let bytes = self
            .bytes
            .as_mut()
            .expect("original observation byte claim");
        if actual > bytes.reserved_bytes() {
            return Err(ObservationError::Oversized {
                charged: actual,
                reserved: bytes.reserved_bytes(),
            });
        }
        bytes.shrink_to(actual)
    }
}
impl Drop for ObservationCredit {
    fn drop(&mut self) {
        // This explicit sequence is the only positive return receipt. If either
        // actual destructor panics, returned is merely dropped, NEVER invoked;
        // the original Source retains its unacknowledged claim as UNKNOWN.
        drop(self.bytes.take());
        drop(self.slot.take());
        if let Some(returned) = self.returned.take() {
            returned();
        }
    }
}
/// The actual registration Source owns the 4098 bound pool positions and the
/// existing observation-byte window. It atomically loans both to a read,
/// preserving a distinct control position/byte share. Pending registers cx's
/// real waker in that owner; no timer or private semaphore grants capacity.
pub(crate) trait ObservationSourcePool: Send + Sync + 'static {
    fn budget(&self) -> ObservationBudget;
    fn fixed_root_control_binding(&self) -> RootControlBinding;
    /// Fence new root controls and retain the original writer until all
    /// already admitted requests have actually left canonical custody.
    fn close_root_controls(&self) -> Result<(), ObservationError>;
    /// Retain the SAME already-admitted metadata author/backing for this real
    /// layout and its containers. No new quota/wallet or descriptor-only grant.
    /// P10a must supply this actual author; P10c provides no fallback.
    fn retain_metadata(
        &self,
        required: ObservationMetadataLayout,
    ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError>;
    /// These two metadata-only operations run under the writer's stream
    /// registration lock. They must never notify or drop publisher captures.
    fn register_stream(&self, key: StreamKey) -> Result<(), ObservationError>;
    fn retire_stream(&self, key: StreamKey) -> Result<(), ObservationError>;
    /// Validate the original live canonical row and source origin, then project
    /// using its fixed activation/root. Called only by the unique fill writer,
    /// with the original metadata obligation still held. The canonical owner
    /// keeps the real QA request/oneshot; the ref alone authorizes nothing.
    fn project_root_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<LogicalToAttempt<AttemptFailure>, ObservationError>;
    fn poll_read(
        &self,
        stream: StreamKey,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ObservationCredit, ObservationError>>;
    fn cancel_read_wait(&self, stream: StreamKey);
    /// Publish only a real Some -> None read suspension transition, outside the
    /// ingress lock, through this SAME original Source's prepaid per-participant
    /// Ordinary coalescing role. Its existing metadata author must cover the
    /// actual publisher, backing and projected input (not only the snapshot).
    /// No default, new loan, capacity wallet or timeout policy is permitted.
    /// Refusal retains the original publisher's unresolved debt/custody.
    fn read_resumed(&self, progress: ReadProgressSnapshot) -> Result<(), ObservationError>;
    /// Retain the canonical owner's EXISTING control byte share exactly once;
    /// do not allocate another payload charge for the same original request.
    fn try_control(
        &self,
        reference: &RootControlRef,
    ) -> Result<ObservationCredit, ObservationError>;
    /// Positive only after EVERY original read/queued/applying claim for this
    /// generation consumed its real return receipt AND its read-progress
    /// payload/debt has actually retired. The reusable standing belongs to the
    /// stable parent Source, not a generation RPC/WC child. It must not strongly
    /// retain that child. Job exit/zero occupancy cannot substitute, including
    /// an internal async-body Drop panic; failed publication retains UNKNOWN.
    fn stream_claims_exited(&self, stream: StreamKey) -> bool;
}

struct ReadState {
    opening_started_at: Option<MonotonicInstant>,
    opened_at: Option<MonotonicInstant>,
    last_liveness_at: MonotonicInstant,
    last_observation_at: MonotonicInstant,
    bookmark: u64,
    catch_up_seen: bool,
    paused_from: Option<MonotonicInstant>,
    paused: Duration,
    resume_revision: u64,
    closed: bool,
    pending: bool,
    accepted_unretired: usize,
    polling: bool,
    unregistered: bool,
    poisoned: bool,
}
impl ReadState {
    fn new(at: MonotonicInstant) -> Self {
        Self {
            opening_started_at: None,
            opened_at: None,
            last_liveness_at: at,
            last_observation_at: at,
            bookmark: 0,
            catch_up_seen: false,
            paused_from: None,
            paused: Duration::ZERO,
            resume_revision: 0,
            closed: false,
            pending: false,
            accepted_unretired: 0,
            polling: false,
            unregistered: false,
            poisoned: false,
        }
    }
    fn check_time(&self, at: MonotonicInstant) -> Result<(), ObservationError> {
        if at < self.last_observation_at || self.opened_at.is_some_and(|opened| at < opened) {
            Err(ObservationError::ClockRegression)
        } else {
            Ok(())
        }
    }
    fn pause(&mut self, at: MonotonicInstant) -> Result<(), ObservationError> {
        self.check_time(at)?;
        self.last_observation_at = at;
        self.paused_from.get_or_insert(at);
        Ok(())
    }
    fn resume(&mut self, at: MonotonicInstant) -> Result<Option<NonZeroU64>, ObservationError> {
        self.check_time(at)?;
        let revision = if self.paused_from.is_some() {
            Some(
                NonZeroU64::new(
                    self.resume_revision
                        .checked_add(1)
                        .ok_or(ObservationError::ResumeRevisionExhausted)?,
                )
                .expect("positive checked resume revision"),
            )
        } else {
            None
        };
        self.last_observation_at = at;
        if let Some(start) = self.paused_from.take() {
            self.paused = self
                .paused
                .saturating_add(at.saturating_duration_since(start));
            self.resume_revision = revision.expect("actual suspension transition").get();
        }
        Ok(revision)
    }
    fn receive(
        &mut self,
        key: StreamKey,
        at: MonotonicInstant,
        event: &ObservationEvent,
    ) -> Result<(), ObservationError> {
        self.check_time(at)?;
        if self.closed {
            return Err(ObservationError::StreamClosed);
        }
        self.last_observation_at = at;
        let fresh = match event {
            ObservationEvent::Fact {
                fact:
                    ObservationFact::Bookmark {
                        generation,
                        sequence,
                        ..
                    },
                ..
            } if *generation == key.generation.get() && *sequence > self.bookmark => {
                self.bookmark = *sequence;
                true
            }
            ObservationEvent::Fact {
                fact: ObservationFact::CatchUpComplete { generation, .. },
                ..
            } if *generation == key.generation.get() && !self.catch_up_seen => {
                self.catch_up_seen = true;
                true
            }
            ObservationEvent::Closed | ObservationEvent::Failed(_) => {
                self.closed = true;
                false
            }
            _ => false,
        };
        if fresh {
            self.last_liveness_at = at;
            self.paused = Duration::ZERO;
            if self.paused_from.is_some() {
                self.paused_from = Some(at);
            }
        }
        Ok(())
    }
    fn opening_for(
        &self,
        event: &ObservationEvent,
    ) -> Result<ObservationOpening, ObservationError> {
        let started_at = self
            .opening_started_at
            .ok_or(ObservationError::InvalidOpening)?;
        if self.opened_at.is_none() && !matches!(event, ObservationEvent::Failed(_)) {
            return Err(ObservationError::InvalidOpening);
        }
        Ok(ObservationOpening {
            started_at,
            opened_at: self.opened_at,
        })
    }
    fn snapshot(
        &self,
        key: StreamKey,
        request: AttemptRequestToken,
        at: MonotonicInstant,
    ) -> Result<ReadStateSnapshot, ObservationError> {
        self.check_time(at)?;
        let active = self
            .paused_from
            .map_or(Duration::ZERO, |from| at.saturating_duration_since(from));
        Ok(ReadStateSnapshot {
            request,
            participation: key.participation,
            generation: key.generation,
            snapshot_at: at,
            opening_started_at: self.opening_started_at,
            opened_at: self.opened_at,
            had_liveness: self.catch_up_seen || self.bookmark != 0,
            last_liveness_at: self.last_liveness_at,
            pauses: ReadPauseCoverage {
                from: self.last_liveness_at,
                through: at,
                paused: self.paused.saturating_add(active),
            },
            reading_paused: self.paused_from.is_some(),
            resume_revision: self.resume_revision,
            closed: self.closed,
            received_through_snapshot: if self.polling || self.unregistered || self.poisoned {
                ReceptionCoverage::Incomplete
            } else {
                ReceptionCoverage::Complete
            },
        })
    }
}

enum EntryValue {
    Pending {
        key: StreamKey,
        received_at: MonotonicInstant,
        receipt: Arc<()>,
    },
    Observation(ReceivedObservation),
    Control(RootControlRef),
}
struct Entry {
    value: EntryValue,
    credit: Option<ObservationCredit>,
    pending: bool,
}
struct InFlight {
    input: InputId,
    control: bool,
    processed: bool,
    retired: bool,
    abandoned: bool,
    // The payload has moved into the real runtime input. These same original
    // loans remain counted until its actual input retirement is acknowledged.
    _credits: Vec<ObservationCredit>,
    streams: Vec<StreamKey>,
}
impl InFlight {
    fn finished(&self) -> bool {
        self.retired && (!self.control || self.processed || self.abandoned)
    }
}

enum RuntimeObservationReceipt {
    RootProcessed,
    Retired,
    AbandonedAndRetired,
}

#[derive(Default)]
struct EarlyConsumption {
    input: Option<InputId>,
    processed: bool,
    retired: bool,
    abandoned: bool,
}
struct FailedFill<I> {
    _value: I,
    _credits: Vec<ObservationCredit>,
    streams: Vec<StreamKey>,
}
struct FailedControlProjection {
    _reference: RootControlRef,
    _credits: Vec<ObservationCredit>,
}
enum PreparedValue {
    Control(RootControlRef),
    Observations(ObservationBatch),
}
struct State<I> {
    streams: BTreeMap<StreamKey, ReadState>,
    queue: VecDeque<Entry>,
    deferred_controls: VecDeque<Entry>,
    queued_frames: usize,
    writer_busy: bool,
    early: EarlyConsumption,
    in_flight: Option<InFlight>,
    failed: Option<FailedFill<I>>,
    failed_control: Option<FailedControlProjection>,
    observations_closed: bool,
    controls_closed: bool,
    metadata_retired: bool,
    closed: bool,
}
struct Inner<A: Actor> {
    state: Mutex<State<A::Input>>,
    observations: Mutex<StandingInputObligation<A>>,
    controls: Mutex<StandingInputObligation<A>>,
    source: Arc<dyn ObservationSourcePool>,
    root_binding: RootControlBinding,
    metadata: Mutex<Option<Box<dyn ObservationMetadataObligation>>>,
    budget: ObservationBudget,
    clock: Arc<dyn MonotonicClock>,
    project: fn(AttemptInput) -> A::Input,
    changed: Notify,
    progress: Waker,
}
/// A non-cloneable original Ready receipt. Its exact placeholder already
/// occupies the common FIFO, before decoding or any later root control.
pub(crate) struct ReceivedFrameReceipt {
    key: StreamKey,
    identity: Arc<()>,
}
/// One actual opening poll and its original phase observations. Pending is a
/// completed short poll, not an incomplete long-lived network wait. A failure
/// already has a paid common-FIFO receipt before later controls can proceed.
pub(crate) struct OpeningPoll<T> {
    pub(crate) started_at: MonotonicInstant,
    pub(crate) opened_at: Option<MonotonicInstant>,
    pub(crate) result: Poll<Result<T, (ReceivedFrameReceipt, ObservationFailure)>>,
}
pub(crate) struct OrderedObservationInput<A: Actor>(Arc<Inner<A>>);
impl<A: Actor> Clone for OrderedObservationInput<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
pub(crate) struct WeakOrderedObservationInput<A: Actor>(Weak<Inner<A>>);

impl<A: Actor> WeakOrderedObservationInput<A> {
    pub(crate) fn upgrade(&self) -> Option<OrderedObservationInput<A>> {
        self.0.upgrade().map(OrderedObservationInput)
    }
}

impl<A: Actor> OrderedObservationInput<A> {
    /// Both standings are prepaid for the same target incarnation and source:
    /// Lifecycle for observations, Control for root controls. Their authority
    /// is supplied by the original admitted Source, never reconstructed here.
    pub(crate) fn new(
        observations: StandingInputObligation<A>,
        controls: StandingInputObligation<A>,
        source: Arc<dyn ObservationSourcePool>,
        root_binding: RootControlBinding,
        clock: Arc<dyn MonotonicClock>,
        project: fn(AttemptInput) -> A::Input,
        progress: Waker,
    ) -> Result<
        Self,
        (
            ObservationError,
            StandingInputObligation<A>,
            StandingInputObligation<A>,
        ),
    > {
        let budget = source.budget();
        let prepared = (|| {
            budget.validate()?;
            if observations
                .in_flight()
                .map_err(ObservationError::Runtime)?
                .is_some()
                || controls
                    .in_flight()
                    .map_err(ObservationError::Runtime)?
                    .is_some()
            {
                return Err(ObservationError::Busy);
            }
            source.retain_metadata(ObservationMetadataLayout {
                entry: Layout::new::<Entry>(),
                root_reference: Layout::new::<RootControlRef>(),
                actor_input: Layout::new::<A::Input>(),
                ap_projection: Layout::new::<AttemptInput>(),
                logical_projection: Layout::new::<LogicalToAttempt<AttemptFailure>>(),
                batch_item: Layout::new::<ReceivedObservation>(),
                in_flight: Layout::new::<InFlight>(),
                failed_fill: Layout::new::<FailedFill<A::Input>>(),
                read_state: Layout::new::<ReadState>(),
                read_confirmation: Layout::new::<ReadConfirmationFuture<'static, A>>(),
                confirmation_waiter: Layout::new::<Notified<'static>>(),
                read_progress: Layout::new::<ReadProgressSnapshot>(),
                total_positions: budget.total_positions,
                max_batch_items: ObservationBatch::MAX_ITEMS,
            })
        })();
        let metadata = match prepared {
            Ok(metadata) => metadata,
            Err(error) => return Err((error, observations, controls)),
        };
        Ok(Self(Arc::new(Inner {
            state: Mutex::new(State {
                streams: BTreeMap::new(),
                queue: VecDeque::new(),
                deferred_controls: VecDeque::new(),
                queued_frames: 0,
                writer_busy: false,
                early: EarlyConsumption::default(),
                in_flight: None,
                failed: None,
                failed_control: None,
                observations_closed: false,
                controls_closed: false,
                metadata_retired: false,
                closed: false,
            }),
            observations: Mutex::new(observations),
            controls: Mutex::new(controls),
            source,
            root_binding,
            metadata: Mutex::new(Some(metadata)),
            budget,
            clock,
            project,
            changed: Notify::new(),
            progress,
        })))
    }
    pub(crate) fn standing_ids(
        &self,
    ) -> [novarocks_control_runtime::actor_turn::StandingInputId; 2] {
        [
            self.0
                .observations
                .lock()
                .expect("ordered observation position")
                .id(),
            self.0
                .controls
                .lock()
                .expect("ordered control position")
                .id(),
        ]
    }
    /// An explicit core Close stops the job; unlike remote EOF it is already a
    /// core-known decision. Retain closed read state until actual job retirement.
    pub(crate) fn job_closed(&self, key: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.0.state.lock().expect("ordered observation state");
        let at = self.0.clock.now();
        let stream = state
            .streams
            .get_mut(&key)
            .ok_or(ObservationError::UnknownStream)?;
        stream.check_time(at)?;
        stream.last_observation_at = at;
        stream.closed = true;
        drop(state);
        self.0.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn register_stream(
        &self,
        key: StreamKey,
    ) -> Result<MonotonicInstant, ObservationError> {
        let mut state = self.0.state.lock().expect("ordered observation state");
        if state.closed {
            return Err(ObservationError::Closed);
        }
        if state.streams.contains_key(&key) {
            return Err(ObservationError::GenerationAlreadyRegistered);
        }
        let at = self.0.clock.now();
        self.0.source.register_stream(key)?;
        state.streams.insert(key, ReadState::new(at));
        Ok(at)
    }
    /// Called only after the original generation job/confirmations and all
    /// held read objects have actually exited. It never substitutes for join.
    pub(crate) fn retire_stream(&self, key: StreamKey) -> Result<(), ObservationError> {
        let mut state = self.0.state.lock().expect("ordered observation state");
        let stream = state
            .streams
            .get(&key)
            .ok_or(ObservationError::UnknownStream)?;
        if !stream.closed || stream.pending || stream.accepted_unretired != 0 || state.queue.iter().any(|entry| matches!(&entry.value,
            EntryValue::Observation(v) if v.participation == key.participation && v.generation == key.generation)) {
            return Err(ObservationError::Busy);
        }
        self.0.source.retire_stream(key)?;
        state.streams.remove(&key);
        Ok(())
    }
    pub(crate) async fn reserve_read(
        &self,
        key: StreamKey,
    ) -> Result<ObservationCredit, ObservationError> {
        struct Wait<'a> {
            source: &'a dyn ObservationSourcePool,
            key: StreamKey,
            active: bool,
        }
        impl Drop for Wait<'_> {
            fn drop(&mut self) {
                if self.active {
                    self.source.cancel_read_wait(self.key);
                }
            }
        }
        let mut wait = Wait {
            source: self.0.source.as_ref(),
            key,
            active: true,
        };
        let credit = loop {
            // Persist this enabled waiter across suspension; a stack-local
            // Notified dropped after Poll::Pending would unregister its wake.
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pending = {
                let mut state = self.0.state.lock().expect("ordered observation state");
                if state.closed {
                    return Err(ObservationError::Closed);
                }
                let stream = state
                    .streams
                    .get_mut(&key)
                    .ok_or(ObservationError::UnknownStream)?;
                if stream.closed {
                    return Err(ObservationError::StreamClosed);
                }
                stream.pause(self.0.clock.now())?;
                stream.pending
            };
            if pending {
                notified.await;
                continue;
            }
            let polled = tokio::select! {
                result = std::future::poll_fn(|cx| self.0.source.poll_read(key, cx)) => result,
                _ = notified.as_mut() => continue,
            };
            let credit = polled?;
            let resumed = {
                let mut state = self.0.state.lock().expect("ordered observation state");
                if state.closed {
                    Err(ObservationError::Closed)
                } else if let Some(stream) = state.streams.get_mut(&key) {
                    if stream.closed {
                        Err(ObservationError::StreamClosed)
                    } else {
                        let at = self.0.clock.now();
                        stream
                            .resume(at)
                            .map(|revision| revision.map(|revision| (at, revision)))
                    }
                } else {
                    Err(ObservationError::UnknownStream)
                }
            };
            if let Some((resumed_at, resume_revision)) = resumed? {
                // No owner mutex spans the original publisher/runtime fill or
                // its synchronous wakers. This is a fact hint, never policy.
                self.0.source.read_resumed(ReadProgressSnapshot {
                    participation: key.participation,
                    generation: key.generation,
                    resumed_at,
                    resume_revision,
                })?;
            }
            break credit;
        };
        wait.active = false;
        Ok(credit)
    }
    /// Register the actual first open poll and successful response with the
    /// SAME snapshot author. No source mutex spans Future::poll or a foreign
    /// waker. Opening timing policy remains entirely in AP. A failed Ready
    /// reserves its original common FIFO position before root controls resume.
    pub(crate) fn poll_open<F, T>(
        &self,
        key: StreamKey,
        credit: &mut Option<ObservationCredit>,
        future: std::pin::Pin<&mut F>,
        cx: &mut Context<'_>,
    ) -> Result<OpeningPoll<T>, ObservationError>
    where
        F: std::future::Future<Output = Result<T, ObservationFailure>> + ?Sized,
    {
        let started_at = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state.closed {
                return Err(ObservationError::Closed);
            }
            let stream = state
                .streams
                .get_mut(&key)
                .ok_or(ObservationError::UnknownStream)?;
            if stream.closed {
                return Err(ObservationError::StreamClosed);
            }
            if stream.polling || stream.unregistered || stream.opened_at.is_some() {
                return Err(ObservationError::Busy);
            }
            let at = self.0.clock.now();
            stream.check_time(at)?;
            stream.polling = true;
            *stream.opening_started_at.get_or_insert(at)
        };
        struct OpeningGuard<'a, A: Actor> {
            ingress: &'a OrderedObservationInput<A>,
            key: StreamKey,
            complete: bool,
        }
        impl<A: Actor> Drop for OpeningGuard<'_, A> {
            fn drop(&mut self) {
                if !self.complete {
                    let mut state = self
                        .ingress
                        .0
                        .state
                        .lock()
                        .expect("ordered observation state");
                    if let Some(stream) = state.streams.get_mut(&self.key) {
                        stream.polling = false;
                        stream.poisoned = true;
                    }
                }
            }
        }
        let mut guard = OpeningGuard {
            ingress: self,
            key,
            complete: false,
        };
        let polled = future.poll(cx);
        // Capture the actual Ready observation before lock reacquisition. A
        // concurrent confirmation still sees polling=true and is Incomplete.
        let observed_at = self.0.clock.now();
        let (result, opened_at) = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            let stream = state
                .streams
                .get_mut(&key)
                .expect("live original opening generation");
            stream.check_time(observed_at)?;
            let result = match polled {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(value)) => {
                    stream.opened_at = Some(observed_at);
                    stream.last_liveness_at = observed_at;
                    stream.last_observation_at = observed_at;
                    stream.paused = Duration::ZERO;
                    stream.paused_from = None;
                    Poll::Ready(Ok(value))
                }
                Poll::Ready(Err(failure)) => {
                    stream.unregistered = true;
                    stream.accepted_unretired += 1;
                    let pending = state.queued_frames >= self.0.budget.queued_frames;
                    state
                        .streams
                        .get_mut(&key)
                        .expect("original opening generation")
                        .pending = pending;
                    let identity = Arc::new(());
                    state.queue.push_back(Entry {
                        value: EntryValue::Pending {
                            key,
                            received_at: observed_at,
                            receipt: identity.clone(),
                        },
                        credit: Some(credit.take().expect("opening owns original preread credit")),
                        pending,
                    });
                    if !pending {
                        state.queued_frames += 1;
                    }
                    Poll::Ready(Err((ReceivedFrameReceipt { key, identity }, failure)))
                }
            };
            let stream = state
                .streams
                .get_mut(&key)
                .expect("original opening generation");
            // opened_at/failed Ready registration precedes clearing this gate.
            stream.polling = false;
            let opened_at = stream.opened_at;
            if !state.streams.values().any(|stream| stream.polling) {
                let controls = std::mem::take(&mut state.deferred_controls);
                state.queue.extend(controls);
            }
            (result, opened_at)
        };
        guard.complete = true;
        self.0.changed.notify_waiters();
        self.0.progress.wake_by_ref();
        self.drive()?;
        Ok(OpeningPoll {
            started_at,
            opened_at,
            result,
        })
    }
    /// Wrap exactly one network Future::poll. Pending clears this short gate
    /// immediately; no gate survives an ordinary long-poll suspension. Ready
    /// first occupies the common FIFO with its original read credit. Decoding
    /// then fills that SAME placeholder; snapshots in that gap are Incomplete.
    pub(crate) fn poll_receive<F: std::future::Future + ?Sized>(
        &self,
        key: StreamKey,
        credit: &mut Option<ObservationCredit>,
        future: std::pin::Pin<&mut F>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(ReceivedFrameReceipt, F::Output), ObservationError>> {
        {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state.closed {
                return Poll::Ready(Err(ObservationError::Closed));
            }
            let Some(stream) = state.streams.get_mut(&key) else {
                return Poll::Ready(Err(ObservationError::UnknownStream));
            };
            if stream.polling || stream.unregistered {
                return Poll::Ready(Err(ObservationError::Busy));
            }
            stream.polling = true;
        }
        struct PollGuard<'a, A: Actor> {
            ingress: &'a OrderedObservationInput<A>,
            key: StreamKey,
            complete: bool,
        }
        impl<A: Actor> Drop for PollGuard<'_, A> {
            fn drop(&mut self) {
                if !self.complete {
                    let mut state = self
                        .ingress
                        .0
                        .state
                        .lock()
                        .expect("ordered observation state");
                    if let Some(stream) = state.streams.get_mut(&self.key) {
                        stream.polling = false;
                        stream.poisoned = true;
                    }
                }
            }
        }
        let mut guard = PollGuard {
            ingress: self,
            key,
            complete: false,
        };
        // Never hold an owner mutex across a poll that may synchronously wake.
        let polled = future.poll(cx);
        let observed_at = self.0.clock.now();
        let result = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            let stream = state
                .streams
                .get_mut(&key)
                .expect("live original read generation");
            stream.polling = false;
            let result = match polled {
                Poll::Pending => Poll::Pending,
                Poll::Ready(value) => {
                    stream.unregistered = true;
                    stream.accepted_unretired += 1;
                    let pending = state.queued_frames >= self.0.budget.queued_frames;
                    state
                        .streams
                        .get_mut(&key)
                        .expect("original read generation")
                        .pending = pending;
                    let identity = Arc::new(());
                    state.queue.push_back(Entry {
                        value: EntryValue::Pending {
                            key,
                            received_at: observed_at,
                            receipt: identity.clone(),
                        },
                        credit: Some(
                            credit
                                .take()
                                .expect("network read owns its original credit"),
                        ),
                        pending,
                    });
                    if !pending {
                        state.queued_frames += 1;
                    }
                    Poll::Ready(Ok((ReceivedFrameReceipt { key, identity }, value)))
                }
            };
            if !state.streams.values().any(|stream| stream.polling) {
                let controls = std::mem::take(&mut state.deferred_controls);
                state.queue.extend(controls);
            }
            result
        };
        guard.complete = true;
        self.0.changed.notify_waiters();
        self.0.progress.wake_by_ref();
        // Pending may have just admitted a deferred control; Ready's undecoded
        // placeholder intentionally blocks everything after its original cut.
        if let Err(error) = self.drive() {
            return Poll::Ready(Err(error));
        }
        result
    }
    /// Charge/decode completion keeps the original receipt position. On error
    /// it remains pending, so the caller can replace malformed/oversized data
    /// with its typed failure AT THE SAME FIFO POSITION, never remove the cut.
    pub(crate) fn complete_receive(
        &self,
        receipt: &ReceivedFrameReceipt,
        event: ObservationEvent,
        encoded: usize,
    ) -> Result<(), ObservationError> {
        struct Completion {
            event: Option<ObservationEvent>,
            credit: Option<ObservationCredit>,
        }
        let charged = observation_charge(&event, encoded);
        if charged > self.0.budget.max_frame_bytes {
            return Err(ObservationError::Oversized {
                charged,
                reserved: self.0.budget.max_frame_bytes,
            });
        }
        let (credit, received_at) = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            let entry = state.queue.iter_mut().find(|entry| matches!(&entry.value,
                EntryValue::Pending { receipt: identity, .. } if Arc::ptr_eq(identity, &receipt.identity)))
                .ok_or(ObservationError::UnknownStream)?;
            let EntryValue::Pending { received_at, .. } = &entry.value else {
                unreachable!()
            };
            (
                entry.credit.take().ok_or(ObservationError::Busy)?,
                *received_at,
            )
        };
        let mut completion = Completion {
            event: Some(event),
            credit: Some(credit),
        };
        if let Err(error) = completion
            .credit
            .as_mut()
            .expect("original Ready credit")
            .charge(charged)
        {
            let credit = completion.credit.take().expect("same refused Ready credit");
            let mut state = self.0.state.lock().expect("ordered observation state");
            let entry = state.queue.iter_mut().find(|entry| matches!(&entry.value,
                EntryValue::Pending { receipt: identity, .. } if Arc::ptr_eq(identity, &receipt.identity)))
                .expect("same pending receipt survives charge refusal");
            entry.credit = Some(credit);
            return Err(error);
        }
        {
            let mut state = self.0.state.lock().expect("ordered observation state");
            let stream = state
                .streams
                .get_mut(&receipt.key)
                .ok_or(ObservationError::UnknownStream)?;
            let event = completion.event.as_ref().expect("completed Ready event");
            let registered = stream.opening_for(event).and_then(|opening| {
                stream
                    .receive(receipt.key, received_at, event)
                    .map(|()| opening)
            });
            let opening = match registered {
                Ok(opening) => opening,
                Err(error) => {
                    let entry = state.queue.iter_mut().find(|entry| matches!(&entry.value,
                    EntryValue::Pending { receipt: identity, .. } if Arc::ptr_eq(identity, &receipt.identity)))
                    .expect("refused receive retains its original pending position");
                    entry.credit = completion.credit.take();
                    return Err(error);
                }
            };
            state
                .streams
                .get_mut(&receipt.key)
                .expect("original completed generation")
                .unregistered = false;
            let entry = state.queue.iter_mut().find(|entry| matches!(&entry.value,
                EntryValue::Pending { receipt: identity, .. } if Arc::ptr_eq(identity, &receipt.identity)))
                .expect("same completed receipt remains in common FIFO");
            entry.value = EntryValue::Observation(ReceivedObservation {
                participation: receipt.key.participation,
                generation: receipt.key.generation,
                received_at,
                opening,
                event: completion.event.take().expect("completed Ready event"),
            });
            entry.credit = completion.credit.take();
        }
        self.0.changed.notify_waiters();
        self.0.progress.wake_by_ref();
        self.drive()
    }
    /// Acceptance, receive time and liveness update commit under one lock.
    /// No caller assigns an ordinal then races another caller's runtime fill.
    pub(crate) fn accept(
        &self,
        key: StreamKey,
        event: ObservationEvent,
        encoded_bytes: usize,
        credit: ObservationCredit,
    ) -> Result<(), ObservationError> {
        struct Acceptance {
            event: Option<ObservationEvent>,
            credit: Option<ObservationCredit>,
        }
        let mut accepted = Acceptance {
            event: Some(event),
            credit: Some(credit),
        };
        let charged = observation_charge(
            accepted.event.as_ref().expect("pending observation"),
            encoded_bytes,
        );
        if charged > self.0.budget.max_frame_bytes {
            return Err(ObservationError::Oversized {
                charged,
                reserved: self.0.budget.max_frame_bytes,
            });
        }
        accepted
            .credit
            .as_mut()
            .expect("pending observation credit")
            .charge(charged)?;
        {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state.closed {
                return Err(ObservationError::Closed);
            }
            let at = self.0.clock.now();
            let pending = state.queued_frames >= self.0.budget.queued_frames;
            let stream = state
                .streams
                .get_mut(&key)
                .ok_or(ObservationError::UnknownStream)?;
            let event = accepted.event.as_ref().expect("pending observation");
            let opening = stream.opening_for(event)?;
            stream.receive(key, at, event)?;
            stream.pending = pending;
            stream.accepted_unretired += 1;
            state.queue.push_back(Entry {
                value: EntryValue::Observation(ReceivedObservation {
                    participation: key.participation,
                    generation: key.generation,
                    received_at: at,
                    opening,
                    event: accepted.event.take().expect("accepted observation"),
                }),
                credit: accepted.credit.take(),
                pending,
            });
            if !pending {
                state.queued_frames += 1;
            }
        }
        self.drive()
    }
    pub(crate) fn weak(&self) -> WeakOrderedObservationInput<A> {
        WeakOrderedObservationInput(Arc::downgrade(&self.0))
    }

    pub(crate) fn root_binding(&self) -> &RootControlBinding {
        &self.0.root_binding
    }

    pub(crate) fn root_control(&self, reference: RootControlRef) -> Result<(), ObservationError> {
        self.queue_root_control(reference)?;
        self.drive()
    }

    /// Success transfers the actual original request into the ordered cut.
    /// A subsequent writer error cannot turn that transfer into a refusal.
    pub(crate) fn queue_root_control(
        &self,
        reference: RootControlRef,
    ) -> Result<(), ObservationError> {
        if !reference.belongs_to(&self.0.root_binding) {
            return Err(ObservationError::InvalidRootControl);
        }
        let mut credit = self.0.source.try_control(&reference)?;
        credit.charge(self.0.budget.control_bytes)?;
        {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state.closed {
                return Err(ObservationError::Closed);
            }
            let entry = Entry {
                value: EntryValue::Control(reference),
                credit: Some(credit),
                pending: false,
            };
            if state.streams.values().any(|stream| stream.polling) {
                state.deferred_controls.push_back(entry);
            } else {
                state.queue.push_back(entry);
            }
        }
        Ok(())
    }
    /// A snapshot is independent of the full ingress and includes received but
    /// not-yet-applied liveness. One lock covers all receive/pause registration.
    fn snapshot_read_state(
        &self,
        key: StreamKey,
        request: AttemptRequestToken,
        requested_at: MonotonicInstant,
    ) -> Result<ReadStateSnapshot, ObservationError> {
        let state = self.0.state.lock().expect("ordered observation state");
        let at = self.0.clock.now();
        if at < requested_at {
            return Err(ObservationError::ConfirmationBeforeRequest);
        }
        state
            .streams
            .get(&key)
            .ok_or(ObservationError::UnknownStream)?
            .snapshot(key, request, at)
    }
    #[cfg(test)]
    fn read_state(
        &self,
        key: StreamKey,
        request: AttemptRequestToken,
        requested_at: MonotonicInstant,
    ) -> Result<ReadStateSnapshot, ObservationError> {
        self.snapshot_read_state(key, request, requested_at)
    }
    /// The caller keeps the SAME credentialed Work, prepaid causal input and
    /// generation retention for this entire future and actual capture exit.
    /// Incomplete never consumes the result loan: the original waiter registers
    /// before taking a snapshot, then waits only for real Source change. No
    /// clock, sleep, retry policy, extra loan or private request queue is used.
    pub(crate) fn confirm<'a>(
        &'a self,
        key: StreamKey,
        request: AttemptRequestToken,
        requested_at: MonotonicInstant,
        result: &'a mut CausalInputObligation<A>,
    ) -> ReadConfirmationFuture<'a, A> {
        ReadConfirmationFuture {
            ingress: self,
            key,
            request,
            requested_at,
            result,
            waiting: None,
            done: false,
        }
    }
    pub(crate) fn root_control_processed(&self, input: InputId) -> Result<(), ObservationError> {
        self.consumed(input, RuntimeObservationReceipt::RootProcessed)
    }
    /// Only the actual runtime retire receipt may call this hook. A control
    /// still waits for its first COMPLETE core processing, not its final reply.
    pub(crate) fn input_retired(&self, input: InputId) -> Result<(), ObservationError> {
        self.consumed(input, RuntimeObservationReceipt::Retired)
    }
    fn failed_input_retired(&self, input: InputId) -> Result<(), ObservationError> {
        self.consumed(input, RuntimeObservationReceipt::AbandonedAndRetired)
    }

    fn consumed(
        &self,
        input: InputId,
        receipt: RuntimeObservationReceipt,
    ) -> Result<(), ObservationError> {
        let processed = matches!(receipt, RuntimeObservationReceipt::RootProcessed);
        let abandoned = matches!(receipt, RuntimeObservationReceipt::AbandonedAndRetired);
        {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if let Some(flight) = state
                .in_flight
                .as_mut()
                .filter(|flight| flight.input == input)
            {
                if processed {
                    if !flight.control {
                        return Err(ObservationError::InvalidRootControl);
                    }
                    flight.processed = true;
                } else {
                    flight.retired = true;
                    flight.abandoned |= abandoned;
                }
            } else if state.writer_busy {
                // A synchronous runtime notification may finish the real input
                // before fill returns its id. Preserve that one original receipt.
                if state.early.input.is_some_and(|id| id != input) {
                    return Err(ObservationError::Busy);
                }
                state.early.input = Some(input);
                if processed {
                    state.early.processed = true;
                } else {
                    state.early.retired = true;
                    state.early.abandoned |= abandoned;
                }
            } else {
                return Err(ObservationError::Busy);
            }
        }
        self.release_if_finished()
    }
    fn release_if_finished(&self) -> Result<(), ObservationError> {
        let done = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state.in_flight.as_ref().is_some_and(InFlight::finished) {
                let done = state.in_flight.take().expect("finished original input");
                Some(done)
            } else {
                None
            }
        };
        if let Some(done) = done {
            self.return_credits(done);
        }
        self.0.changed.notify_waiters();
        self.0.progress.wake_by_ref();
        self.drive()
    }
    fn return_credits(&self, done: InFlight) {
        let InFlight {
            _credits, streams, ..
        } = done;
        // No guard decrements these counts on unwind. If payload/refund Drop
        // panics, the original generation remains retained and UNKNOWN.
        drop(_credits);
        self.retention_returned(&streams);
    }
    fn retention_returned(&self, streams: &[StreamKey]) {
        let mut state = self.0.state.lock().expect("ordered observation state");
        for key in streams {
            state
                .streams
                .get_mut(key)
                .expect("held observation generation")
                .accepted_unretired -= 1;
        }
    }
    pub(crate) fn drive(&self) -> Result<(), ObservationError> {
        loop {
            let prepared = {
                let mut state = self.0.state.lock().expect("ordered observation state");
                if state.closed
                    || state.writer_busy
                    || state.in_flight.is_some()
                    || state.queue.is_empty()
                    || matches!(
                        state.queue.front().map(|entry| &entry.value),
                        Some(EntryValue::Pending { .. })
                    )
                {
                    return Ok(());
                }
                state.writer_busy = true;
                let control = matches!(
                    state.queue.front().map(|entry| &entry.value),
                    Some(EntryValue::Control(_))
                );
                let mut credits = Vec::new();
                let mut streams = Vec::new();
                let input = if control {
                    let entry = state.queue.pop_front().expect("control at queue head");
                    credits.push(entry.credit.expect("accepted original input credit"));
                    let EntryValue::Control(value) = entry.value else {
                        unreachable!()
                    };
                    PreparedValue::Control(value)
                } else {
                    let mut frames = Vec::new();
                    while frames.len() < ObservationBatch::MAX_ITEMS
                        && matches!(
                            state.queue.front().map(|entry| &entry.value),
                            Some(EntryValue::Observation(_))
                        )
                    {
                        let entry = state.queue.pop_front().expect("observation at queue head");
                        let EntryValue::Observation(value) = entry.value else {
                            unreachable!()
                        };
                        if entry.pending {
                            if let Some(stream) = state.streams.get_mut(&StreamKey {
                                participation: value.participation,
                                generation: value.generation,
                            }) {
                                stream.pending = false;
                            }
                        } else {
                            state.queued_frames -= 1;
                        }
                        credits.push(entry.credit.expect("accepted original input credit"));
                        streams.push(StreamKey {
                            participation: value.participation,
                            generation: value.generation,
                        });
                        frames.push(value);
                    }
                    PreparedValue::Observations(
                        ObservationBatch::new(frames)
                            .expect("bounded nonempty common observation batch"),
                    )
                };
                (control, input, credits, streams)
            };
            let (control, input, credits, streams) = prepared;
            // Exactly this writer owns filling across both classes. No owner mutex
            // is held while runtime/ledger notification can synchronously reenter.
            let input = match input {
                PreparedValue::Observations(batch) => {
                    AttemptInput::Executor(ExecutorFact::Observations(batch))
                }
                PreparedValue::Control(reference) => {
                    let projected =
                        self.0
                            .source
                            .project_root_control(&reference)
                            .and_then(|request| {
                                let valid = request.activation == self.0.root_binding.0.activation
                                    && match &request.body {
                                        LogicalToAttemptBody::SealRequest { token } => {
                                            *token == reference.token
                                        }
                                        LogicalToAttemptBody::TerminalControlDemand {
                                            token,
                                            root,
                                        } => {
                                            *token == reference.token
                                                && *root == self.0.root_binding.0.root
                                        }
                                        _ => false,
                                    };
                                if valid {
                                    Ok(request)
                                } else {
                                    Err(ObservationError::InvalidRootControl)
                                }
                            });
                    match projected {
                        Ok(request) => AttemptInput::Port(request),
                        Err(error) => {
                            let mut state = self.0.state.lock().expect("ordered observation state");
                            state.writer_busy = false;
                            state.closed = true;
                            state.failed_control = Some(FailedControlProjection {
                                _reference: reference,
                                _credits: credits,
                            });
                            return Err(error);
                        }
                    }
                }
            };
            let value = (self.0.project)(input);
            let result = if control {
                self.0
                    .controls
                    .lock()
                    .expect("ordered control position")
                    .fill(value)
            } else {
                self.0
                    .observations
                    .lock()
                    .expect("ordered observation position")
                    .fill(value)
            };
            let error = {
                let mut state = self.0.state.lock().expect("ordered observation state");
                assert!(state.writer_busy && state.in_flight.is_none());
                state.writer_busy = false;
                match result {
                    Ok(input) => {
                        assert!(
                            state.early.input.is_none_or(|id| id == input),
                            "same original early input receipt"
                        );
                        let early = std::mem::take(&mut state.early);
                        state.in_flight = Some(InFlight {
                            input,
                            control,
                            processed: !control || early.processed,
                            retired: early.retired,
                            abandoned: early.abandoned,
                            _credits: credits,
                            streams,
                        });
                        None
                    }
                    Err((error, value)) => {
                        state.closed = true;
                        state.failed = Some(FailedFill {
                            _value: value,
                            _credits: credits,
                            streams,
                        });
                        Some(error)
                    }
                }
            };
            self.0.changed.notify_waiters();
            self.0.progress.wake_by_ref();
            if let Some(error) = error {
                return Err(ObservationError::Runtime(error));
            }
            let done = {
                let mut state = self.0.state.lock().expect("ordered observation state");
                if state.in_flight.as_ref().is_some_and(InFlight::finished) {
                    let done = state
                        .in_flight
                        .take()
                        .expect("finished early original input");
                    Some(done)
                } else {
                    None
                }
            };
            let more = done.is_some();
            if let Some(done) = done {
                self.return_credits(done);
            }
            if !more {
                return Ok(());
            }
        }
    }
    /// Stops new ingress; this is not a producer/body/report exit proof.
    pub(crate) fn close(&self) {
        self.0
            .state
            .lock()
            .expect("ordered observation state")
            .closed = true;
        self.0.changed.notify_waiters();
    }
    fn is_retired(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("ordered observation state")
            .metadata_retired
    }

    fn failed_current_input(&self) -> Option<InputId> {
        let state = self.0.state.lock().expect("ordered observation state");
        state.in_flight.as_ref().map(|original| original.input)
    }

    fn fence_failed_writer(&self) {
        self.0
            .state
            .lock()
            .expect("ordered observation state")
            .closed = true;
        self.0.changed.notify_waiters();
    }

    fn take_failed_source_payload(
        &self,
        roles: &BTreeSet<StandingInputId>,
    ) -> Result<Option<FailedSourcePayload<A>>, ActorTurnError> {
        self.0
            .observations
            .lock()
            .expect("ordered observation position")
            .take_failed_source_payload(roles)
    }

    pub(crate) fn notify_source_progress(&self) {
        self.0.progress.wake_by_ref();
    }

    /// Call only after original stream/bridge holders actually exited and all
    /// in-flight inputs retired. Refusal preserves every original loan/standing.
    pub(crate) fn close_after_exit(&self) -> Result<(), ObservationError> {
        self.0.source.close_root_controls()?;
        let (retired, streams) = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            state.closed = true;
            if state.writer_busy
                || state.in_flight.is_some()
                || state.streams.values().any(|s| s.polling)
            {
                return Err(ObservationError::Busy);
            }
            let mut streams = state
                .queue
                .iter()
                .filter_map(|entry| match &entry.value {
                    EntryValue::Pending { key, .. } => Some(*key),
                    EntryValue::Observation(value) => Some(StreamKey {
                        participation: value.participation,
                        generation: value.generation,
                    }),
                    EntryValue::Control(_) => None,
                })
                .collect::<Vec<_>>();
            if let Some(failed) = state.failed.as_ref() {
                streams.extend_from_slice(&failed.streams);
            }
            (
                (
                    std::mem::take(&mut state.queue),
                    std::mem::take(&mut state.deferred_controls),
                    state.failed.take(),
                    state.failed_control.take(),
                ),
                streams,
            )
        };
        // Actual frame values exit before their byte claims/pool loans. Only a
        // successful complete refund may release this generation's retention.
        drop(retired);
        self.retention_returned(&streams);
        let keys = self
            .0
            .state
            .lock()
            .expect("ordered observation state")
            .streams
            .keys()
            .copied()
            .collect::<Vec<_>>();
        // A preread claim may outlive its generation future. The original pool
        // receipt is a separate real-exit gate, including destructor UNKNOWN.
        if keys
            .iter()
            .any(|key| !self.0.source.stream_claims_exited(*key))
        {
            return Err(ObservationError::Busy);
        }
        let reads = {
            let mut state = self.0.state.lock().expect("ordered observation state");
            if state
                .streams
                .values()
                .any(|stream| stream.accepted_unretired != 0)
            {
                return Err(ObservationError::Busy);
            }
            std::mem::take(&mut state.streams)
        };
        // The real read-state/container backing also exits before the metadata
        // author may acknowledge release. No owner lock spans foreign Drop.
        drop(reads);
        drop(keys);
        let observations_closed = self
            .0
            .state
            .lock()
            .expect("ordered observation state")
            .observations_closed;
        if !observations_closed {
            self.0
                .observations
                .lock()
                .expect("ordered observation position")
                .close_after_exit()
                .map_err(ObservationError::Runtime)?;
            self.0
                .state
                .lock()
                .expect("ordered observation state")
                .observations_closed = true;
        }
        let controls_closed = self
            .0
            .state
            .lock()
            .expect("ordered observation state")
            .controls_closed;
        if !controls_closed {
            self.0
                .controls
                .lock()
                .expect("ordered control position")
                .close_after_exit()
                .map_err(ObservationError::Runtime)?;
            self.0
                .state
                .lock()
                .expect("ordered observation state")
                .controls_closed = true;
        }
        let metadata = self
            .0
            .metadata
            .lock()
            .expect("original observation metadata")
            .take();
        let Some(original) = metadata else {
            return if self.is_retired() {
                Ok(())
            } else {
                Err(ObservationError::Busy)
            };
        };
        if let Err(refused) = original.retire_after_exit() {
            *self
                .0
                .metadata
                .lock()
                .expect("original observation metadata") = Some(refused.original);
            return Err(refused.error);
        }
        self.0
            .state
            .lock()
            .expect("ordered observation state")
            .metadata_retired = true;
        self.0.progress.wake_by_ref();
        Ok(())
    }
}
/// A single original confirmation continuation. The sole Notified allocation
/// is covered by the original metadata obligation; it is not a request queue.
/// A completed/dropped future neither retires the causal role nor proves its
/// Work/captures exited. Those remain the original executor's responsibility.
pub(crate) struct ReadConfirmationFuture<'a, A: Actor> {
    ingress: &'a OrderedObservationInput<A>,
    key: StreamKey,
    request: AttemptRequestToken,
    requested_at: MonotonicInstant,
    result: &'a mut CausalInputObligation<A>,
    waiting: Option<Pin<Box<Notified<'a>>>>,
    done: bool,
}
impl<A: Actor> Future for ReadConfirmationFuture<'_, A> {
    type Output = Result<InputId, ConfirmationFailure<A::Input>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(
            !this.done,
            "completed original read confirmation polled again"
        );
        loop {
            if let Some(waiting) = this.waiting.as_mut() {
                if waiting.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                // Exit this exact original waiter before allocating its next
                // continuation; old+new Notified backing never coexists.
                this.waiting = None;
            }
            let mut notified = Box::pin(this.ingress.0.changed.notified());
            notified.as_mut().enable();
            let snapshot =
                match this
                    .ingress
                    .snapshot_read_state(this.key, this.request, this.requested_at)
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        this.done = true;
                        return Poll::Ready(Err(ConfirmationFailure::Snapshot(error)));
                    }
                };
            if snapshot.received_through_snapshot == ReceptionCoverage::Incomplete {
                this.waiting = Some(notified);
                continue;
            }
            drop(notified);
            let input =
                (this.ingress.0.project)(AttemptInput::Executor(ExecutorFact::ReadState(snapshot)));
            this.done = true;
            return Poll::Ready(
                this.result
                    .fill_with_receipt(None, input)
                    .map(|(_, id)| id)
                    .map_err(ConfirmationFailure::Fill),
            );
        }
    }
}
pub(crate) enum ConfirmationFailure<I> {
    Snapshot(ObservationError),
    Fill(CausalFillFailure<I>),
}
fn observation_charge(event: &ObservationEvent, encoded: usize) -> usize {
    let dynamic = match event {
        ObservationEvent::Fact {
            fact: ObservationFact::Status(status),
            ..
        } => match status.termination() {
            Some(novarocks_execution_contract::TerminationDetail::Failed(failure)) => {
                failure.detail().as_str().len()
            }
            _ => 0,
        },
        ObservationEvent::Fact {
            fact: ObservationFact::Quiesce(receipt),
            ..
        } => {
            let receipt_layout = novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<novarocks_execution_contract::QuiesceParticipationReceipt>(
                ),
            );
            receipt_layout
                .map_or(usize::MAX, |layout| layout.size())
                .saturating_add(std::mem::size_of_val(receipt.accepted_tasks()))
        }
        _ => 0,
    };
    std::mem::size_of::<ReceivedObservation>().saturating_add(encoded.max(dynamic))
}

#[cfg(test)]
#[path = "subscription/observation_tests.rs"]
pub(super) mod tests;
