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

//! Custody of the same active Native Box under its original actor registration.
//! No new WorkOwner, governance obligation, actor, admission, or dispatch policy.

use super::LogicalExecutionRuntimeRegistryHandle;
use super::runtime_registry::health::LogicalExecutionHealth;
use crate::api::{
    ActiveNativeAttemptOwner, NativeActiveConvergenceClosure, NativeActiveConvergenceRequest,
    NativeAttemptConvergence, QueryExecutionError, QueryExecutionErrorKind,
    StatementNativeHealthFailure,
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use novarocks_workload_control::CancellationView;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NativeActiveCustodyKey {
    pub(crate) initial: QueryExecutionId,
    pub(crate) generation: u64,
    pub(crate) execution: QueryExecutionId,
}
impl NativeActiveCustodyKey {
    fn registration_lower_bound(initial: QueryExecutionId, generation: u64) -> Self {
        // QueryExecutionId orders (i64 query high, i64 query low, AttemptId).
        // Both signed-min halves are valid; AttemptId forbids only zero. This
        // is the lowest valid execution value, used solely as a search bound.
        let execution = QueryExecutionId::new(
            QueryId::new(i64::MIN, i64::MIN),
            AttemptId::new(1).expect("one is the lowest nonzero attempt"),
        )
        .expect("signed-min query halves are nonzero");
        Self {
            initial,
            generation,
            execution,
        }
    }
}
struct RetainedNativeActiveOwner {
    _owner: Box<dyn ActiveNativeAttemptOwner>,
    failure: StatementNativeHealthFailure,
}
enum ActiveSlotOwner {
    // Successful activation has not crossed dormant destruction yet. The
    // ORIGINAL slot, not a Source -> Registry strong cycle, owns this Box.
    Activated(Box<dyn ActiveNativeAttemptOwner>),
    Retained(RetainedNativeActiveOwner),
}
struct ActiveSlot {
    original_exit: Weak<super::original_exit::OriginalLogicalExit>,
    contexts: BTreeSet<QueryContextRef>,
    owner: Mutex<Option<ActiveSlotOwner>>,
    convergence_failure: NativeConvergenceFailureSlot,
    root_ack: Mutex<RootAckTailState>,
    consumer: Mutex<ConsumerTailState>,
}

/// The original slot receives the primary before the borrowed convergence
/// future is destroyed. A later destructor panic cannot replace this source.
#[derive(Default)]
pub(crate) struct NativeConvergenceFailureSlot {
    original: Mutex<Option<StatementNativeHealthFailure>>,
}
impl NativeConvergenceFailureSlot {
    fn is_failed(&self) -> bool {
        self.original
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
    fn failure(&self) -> Option<StatementNativeHealthFailure> {
        self.original
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    fn retain(&self, failure: StatementNativeHealthFailure) {
        let mut original = self.original.lock().unwrap_or_else(|e| e.into_inner());
        assert!(original.is_none(), "one original convergence failure");
        *original = Some(failure);
    }
}

/// A temporary alias of the SAME pre-reserved slot; no new allocation, owner
/// or registry edge is introduced by observing its convergence source.
pub(crate) struct NativeConvergenceFailureCustody {
    slot: Arc<ActiveSlot>,
}
impl NativeConvergenceFailureCustody {
    pub(crate) fn original(&self) -> &NativeConvergenceFailureSlot {
        &self.slot.convergence_failure
    }
}
type RootAckFuture = Pin<
    Box<
        dyn Future<Output = Result<crate::api::RetainedRootReply, super::RootResultFetchFailure>>
            + Send,
    >,
>;

#[derive(Default)]
struct RootAckTailState {
    native_closed: bool,
    returned: bool,
    // One original ready waker shared by the ACK and consumer joins.
    tail_waker: Option<Waker>,
    reservation: Option<(AttemptActivationIdentity, TaskIdentity)>,
    phase: RootAckTailPhase,
}
#[derive(Default)]
enum RootAckTailPhase {
    #[default]
    Unreserved,
    Reserved,
    Running {
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        join: tokio::task::JoinHandle<Result<(), super::RootResultFetchFailure>>,
        waker: Waker,
    },
    NeverIssued,
    Joined,
}
impl RootAckTailState {
    fn settled(&self) -> bool {
        self.native_closed
            && matches!(
                self.phase,
                RootAckTailPhase::NeverIssued | RootAckTailPhase::Joined
            )
    }
}

/// Original per-attempt consumer input, not an execution grant or identity.
pub(super) struct OriginalConsumerStart {
    pub(super) consumer: Arc<dyn crate::api::InternalComputationConsumer>,
    pub(super) activation: crate::api::InternalConsumerActivation,
    pub(super) exit: crate::api::InternalConsumerExitReporter,
    pub(super) activity: Option<crate::admitted_query_context::ResultCapacityActivityLease>,
}
#[derive(Default)]
struct ConsumerTailState {
    phase: ConsumerTailPhase,
    input: Option<OriginalConsumerStart>,
    // Outside the destructible factory/F, until its actual cleanup cut.
    activity: Option<crate::admitted_query_context::ResultCapacityActivityLease>,
    // The original factory returned no F, or explicit F destruction returned.
    // Raw primary/notification/Join health is independent of this actual cut.
    cleanup_returned: bool,
    // Joined is not settled while its completed handle/lease is loaned outside.
    finishing: bool,
    panic: Option<Box<dyn std::any::Any + Send>>,
    cleanup_panic: Option<Box<dyn std::any::Any + Send>>,
    // The SAME task's join owns an uncaught receipt/outer-tail panic or cancellation.
    join_failure: Option<tokio::task::JoinError>,
}
#[derive(Default)]
enum ConsumerTailPhase {
    #[default]
    NotIssued,
    Reserved,
    Running(tokio::task::JoinHandle<Result<(), QueryExecutionError>>),
    Joined,
}
impl ConsumerTailState {
    fn pending(&self) -> bool {
        self.finishing
            || matches!(
                self.phase,
                ConsumerTailPhase::Reserved | ConsumerTailPhase::Running(_)
            )
    }
    fn failed(&self) -> bool {
        self.panic.is_some() || self.cleanup_panic.is_some() || self.join_failure.is_some()
    }
}
impl ActiveSlot {
    fn retain_consumer_panic(&self, payload: Box<dyn std::any::Any + Send>, cleanup: bool) {
        let mut incoming = Some(payload);
        {
            let mut state = self.consumer.lock().unwrap_or_else(|e| e.into_inner());
            let target = if cleanup {
                &mut state.cleanup_panic
            } else {
                &mut state.panic
            };
            if target.is_none() {
                *target = incoming.take();
            }
        }
        if let Some(rejected) = incoming {
            // Illegal duplicate intake cannot silently destroy an original cause.
            std::panic::panic_any(std::mem::ManuallyDrop::new(rejected));
        }
        self.mark_consumer_failure();
    }
    fn mark_consumer_failure(&self) {
        if let Some(source) = self.original_exit.upgrade() {
            source.consumer_failed_silent();
        }
    }
}

fn retain_original_consumer_panic(
    slot: &Weak<ActiveSlot>,
    payload: Box<dyn std::any::Any + Send>,
    cleanup: bool,
) {
    if let Some(original) = slot.upgrade() {
        original.retain_consumer_panic(payload, cleanup);
    } else {
        std::panic::panic_any(std::mem::ManuallyDrop::new(payload));
    }
}

async fn run_original_consumer(slot: Weak<ActiveSlot>) -> Result<(), QueryExecutionError> {
    // Input stays in the original slot even if this task never receives a poll.
    let OriginalConsumerStart {
        consumer,
        activation,
        exit,
        activity,
    } = {
        let original = slot
            .upgrade()
            .expect("canonical consumer slot remains until its actual join");
        let mut state = original.consumer.lock().unwrap_or_else(|e| e.into_inner());
        state
            .input
            .take()
            .expect("the original reserved consumer input")
    };
    // start_consumer_in moved the lease to the original slot before spawn.
    assert!(activity.is_none());
    let mut cleanup_returned = false;
    let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        consumer.consume(activation)
    })) {
        Err(payload) => {
            retain_original_consumer_panic(&slot, payload, false);
            cleanup_returned = true;
            Err(super::supervisor::internal_consumer_panicked(
                "internal computation consumer panicked",
            ))
        }
        Ok(mut future) => {
            let outcome = std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    future.as_mut().poll(cx)
                })) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(outcome)) => Poll::Ready(outcome),
                    Err(payload) => {
                        retain_original_consumer_panic(&slot, payload, false);
                        Poll::Ready(Err(super::supervisor::internal_consumer_panicked(
                            "internal computation consumer panicked",
                        )))
                    }
                }
            })
            .await;
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(future))) {
                Ok(()) => cleanup_returned = true,
                Err(payload) => retain_original_consumer_panic(&slot, payload, true),
            }
            // A cleanup panic is health; it cannot overwrite a returned domain verdict.
            outcome
        }
    };
    if cleanup_returned {
        slot.upgrade()
            .expect("canonical consumer slot remains until its actual join")
            .consumer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cleanup_returned = true;
    }
    // A notification panic here is kept by the original slot's actual JoinError.
    // The two raw slots already own any earlier factory/poll and F-Drop causes.
    exit.complete(outcome.clone());
    outcome
}

const ROOT_ACK_TAIL_TURN_BUDGET: usize = 64;

/// Only scheduling metadata lives here. The original join always stays in its
/// ActiveSlot. The ready set is deduplicated and bounded by registered tails;
/// a rotating key cursor provides fairness without full scans or tombstones.
#[derive(Default)]
struct RootAckReadyIndex {
    state: Mutex<RootAckReadyState>,
    changed: Notify,
}
#[derive(Default)]
struct RootAckReadyState {
    registered: BTreeSet<NativeActiveCustodyKey>,
    ready: BTreeSet<NativeActiveCustodyKey>,
    cursor: Option<NativeActiveCustodyKey>,
}
impl RootAckReadyIndex {
    fn register(&self, key: NativeActiveCustodyKey) {
        assert!(
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .registered
                .insert(key)
        );
    }
    fn enqueue(&self, key: NativeActiveCustodyKey) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.registered.contains(&key) && state.ready.insert(key) {
            drop(state);
            self.changed.notify_one();
        }
    }
    fn pop_ready(&self) -> Option<NativeActiveCustodyKey> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let key = state
            .cursor
            .and_then(|cursor| {
                state
                    .ready
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| state.ready.first().copied())?;
        state.ready.remove(&key);
        state.cursor = Some(key);
        Some(key)
    }
    fn complete(&self, key: NativeActiveCustodyKey) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.registered.remove(&key);
        state.ready.remove(&key);
        drop(state);
        self.changed.notify_one();
    }
    fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .registered
            .len()
    }
    fn notify_if_ready(&self) {
        if !self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ready
            .is_empty()
        {
            self.changed.notify_one();
        }
    }
    fn abandon_for_process_exit(&self) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = RootAckReadyState::default();
        self.changed.notify_one();
    }
}
struct RootAckWake {
    key: NativeActiveCustodyKey,
    ready: Weak<RootAckReadyIndex>,
}
impl Wake for RootAckWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(ready) = self.ready.upgrade() {
            ready.enqueue(self.key);
        }
    }
}

/// The single final-ACK position in an original attempt's pre-reserved custody.
/// Dropping an unused reservation explicitly closes it without issuing work.
/// Once issued, the unique join stays in the same stable slot, not this token.
pub(crate) struct RootAckTailReservation {
    storage: NativeActiveCustodyRegistry,
    key: NativeActiveCustodyKey,
    slot: Arc<ActiveSlot>,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    issued: bool,
}
impl RootAckTailReservation {
    pub(crate) fn start(mut self, original: RootAckFuture) {
        let mut state = self.slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
        assert!(matches!(state.phase, RootAckTailPhase::Reserved));
        // Spawn and publish the only join in one synchronous operation. The
        // task owns the original port future and its physical window until exit.
        // A completed task may race this assignment, but its join still owns
        // the completion; no enclosing logical future owns or drops the join.
        let join = tokio::spawn(async move { original.await.map(drop) });
        let waker = state
            .tail_waker
            .as_ref()
            .expect("original ready waker")
            .clone();
        state.phase = RootAckTailPhase::Running {
            activation: self.activation,
            root: self.root,
            join,
            waker,
        };
        self.issued = true;
        drop(state);
        self.storage.root_ack_ready.enqueue(self.key);
    }
}
impl Drop for RootAckTailReservation {
    fn drop(&mut self) {
        if self.issued {
            return;
        }
        let mut state = self.slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
        assert!(matches!(state.phase, RootAckTailPhase::Reserved));
        state.phase = RootAckTailPhase::NeverIssued;
        drop(state);
        self.storage.remove_if_settled(self.key, &self.slot);
        self.storage
            .complete_tail_ready_if_joined(self.key, &self.slot);
    }
}

/// This typed storage is a member of the ORIGINAL RuntimeRegistry. It cannot
/// create a query, conclusion, WorkOwner, obligation, or scheduling authority.
#[derive(Clone)]
pub(crate) struct NativeActiveCustodyRegistry {
    slots: Arc<Mutex<BTreeMap<NativeActiveCustodyKey, Arc<ActiveSlot>>>>,
    health: LogicalExecutionHealth,
    progress: Arc<Notify>,
    root_ack_ready: Arc<RootAckReadyIndex>,
}
impl NativeActiveCustodyRegistry {
    pub(super) fn new(health: LogicalExecutionHealth, progress: Arc<Notify>) -> Self {
        Self {
            slots: Arc::new(Mutex::new(BTreeMap::new())),
            health,
            progress,
            root_ack_ready: Arc::new(RootAckReadyIndex::default()),
        }
    }
    pub(super) fn reserve_with_original_exit(
        &self,
        key: NativeActiveCustodyKey,
        contexts: &[QueryContextRef],
        registry: LogicalExecutionRuntimeRegistryHandle,
        original_exit: Weak<super::original_exit::OriginalLogicalExit>,
    ) -> Option<NativeActiveCustodyReservation> {
        let exact = contexts.iter().copied().collect::<BTreeSet<_>>();
        if exact.is_empty()
            || exact.len() != contexts.len()
            || exact
                .iter()
                .any(|c| c.query_execution_id() != key.execution)
        {
            return None;
        }
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if slots.contains_key(&key) {
            return None;
        }
        let slot = Arc::new(ActiveSlot {
            original_exit,
            contexts: exact,
            owner: Mutex::new(None),
            convergence_failure: NativeConvergenceFailureSlot::default(),
            root_ack: Mutex::new(RootAckTailState::default()),
            consumer: Mutex::new(ConsumerTailState::default()),
        });
        slots.insert(key, Arc::clone(&slot));
        Some(NativeActiveCustodyReservation {
            custody: Some(NativeActiveCustodyHandle {
                storage: self.clone(),
                registry,
                key,
                slot,
            }),
        })
    }
    fn reserve_root_ack(
        &self,
        key: NativeActiveCustodyKey,
        slot: &Arc<ActiveSlot>,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Result<RootAckTailReservation, QueryExecutionError> {
        let mut state = slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
        if activation.execution() != key.execution
            || root.query_execution_id() != key.execution
            || !slot
                .contexts
                .iter()
                .any(|context| context.backend_process_id() == root.backend_process_id())
            || state.native_closed
            || !matches!(state.phase, RootAckTailPhase::Unreserved)
        {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "root ACK tail does not belong to an unused original attempt slot",
            ));
        }
        self.root_ack_ready.register(key);
        state.tail_waker = Some(Waker::from(Arc::new(RootAckWake {
            key,
            ready: Arc::downgrade(&self.root_ack_ready),
        })));
        state.reservation = Some((activation, root));
        state.phase = RootAckTailPhase::Reserved;
        Ok(RootAckTailReservation {
            storage: self.clone(),
            key,
            slot: Arc::clone(slot),
            activation,
            root,
            issued: false,
        })
    }
    fn record_native_closed(&self, key: NativeActiveCustodyKey, slot: &Arc<ActiveSlot>) {
        let mut state = slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
        state.native_closed = true;
        // Metadata-only attempts never reserve a root tail. This is their
        // explicit no-issue closure; Reserved belongs solely to its token.
        if matches!(state.phase, RootAckTailPhase::Unreserved) {
            state.phase = RootAckTailPhase::NeverIssued;
        }
        drop(state);
        self.remove_if_settled(key, slot);
    }
    fn remove_if_settled(&self, key: NativeActiveCustodyKey, slot: &Arc<ActiveSlot>) {
        let (returned, removed) = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            if !slots
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, slot))
            {
                return;
            }
            let mut ack = slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
            let consumer = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
            if !ack.settled() || consumer.pending() {
                return;
            }
            let returned = !ack.returned;
            ack.returned = true;
            // Health-only backing stays canonical after all real sources joined.
            let retain = consumer.failed() || slot.convergence_failure.is_failed();
            drop(consumer);
            drop(ack);
            (returned, (!retain).then(|| slots.remove(&key)).flatten())
        };
        if returned && let Some(source) = slot.original_exit.upgrade() {
            source.native_return_silent(
                key,
                super::LocalExecutionDisposition::OriginalPaidOwnerReturn,
            );
        }
        drop(removed);
        if returned {
            self.progress.notify_waiters();
        }
    }
    fn complete_tail_ready_if_joined(&self, key: NativeActiveCustodyKey, slot: &Arc<ActiveSlot>) {
        let ack_joined = matches!(
            slot.root_ack
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .phase,
            RootAckTailPhase::NeverIssued | RootAckTailPhase::Joined
        );
        if ack_joined
            && !slot
                .consumer
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pending()
        {
            self.root_ack_ready.complete(key);
        }
    }
    fn start_consumer_in(
        &self,
        key: NativeActiveCustodyKey,
        slot: &Arc<ActiveSlot>,
        incoming: &mut Option<OriginalConsumerStart>,
    ) -> Result<(), QueryExecutionError> {
        {
            let ack = slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
            let mut state = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
            let valid = incoming.as_ref().is_some_and(|input| {
                ack.reservation
                    == Some((
                        input.activation.source.activation(),
                        input.activation.source.root(),
                    ))
                    && input.activation.stream.activation().execution() == key.execution
                    && input.activation.source.activation() == input.activation.stream.activation()
                    && input.activation.source.root().query_execution_id() == key.execution
                    && slot.contexts.iter().any(|context| {
                        context.backend_process_id()
                            == input.activation.source.root().backend_process_id()
                    })
            });
            if !valid
                || ack.native_closed
                || !matches!(ack.phase, RootAckTailPhase::Reserved)
                || !matches!(state.phase, ConsumerTailPhase::NotIssued)
            {
                return Err(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    "consumer does not belong to an unused original attempt tail",
                ));
            }
            state.phase = ConsumerTailPhase::Reserved;
            let mut input = incoming.take().expect("validated original consumer input");
            state.activity = input.activity.take();
            state.input = Some(input);
        }
        // The original slot already owns input/causes before any task or factory runs.
        let join = tokio::spawn(run_original_consumer(Arc::downgrade(slot)));
        {
            let mut state = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
            assert!(matches!(state.phase, ConsumerTailPhase::Reserved));
            state.phase = ConsumerTailPhase::Running(join);
        }
        self.root_ack_ready.enqueue(key);
        Ok(())
    }
    pub(crate) fn root_ack_tail_count(&self) -> usize {
        self.root_ack_ready.len()
    }
    pub(crate) fn poll_root_ack_tails(&self) -> usize {
        let mut visited = 0;
        for _ in 0..ROOT_ACK_TAIL_TURN_BUDGET {
            let Some(key) = self.root_ack_ready.pop_ready() else {
                break;
            };
            visited += 1;
            let slot = self
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .cloned();
            let Some(slot) = slot else { continue };
            let ack_joined = {
                let mut state = slot.root_ack.lock().unwrap_or_else(|e| e.into_inner());
                match &mut state.phase {
                    RootAckTailPhase::Running {
                        activation,
                        root,
                        join,
                        waker,
                    } => {
                        let mut cx = Context::from_waker(waker);
                        match Pin::new(join).poll(&mut cx) {
                            Poll::Pending => None,
                            Poll::Ready(outcome) => {
                                let joined = (*activation, *root, outcome);
                                state.phase = RootAckTailPhase::Joined;
                                Some(joined)
                            }
                        }
                    }
                    _ => None,
                }
            };
            let waker = slot
                .root_ack
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .tail_waker
                .clone();
            let (consumer_joined, domain_error, completed_consumer_join) = {
                let mut state = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
                if let ConsumerTailPhase::Running(join) = &mut state.phase {
                    let mut cx = Context::from_waker(waker.as_ref().expect("original tail waker"));
                    match Pin::new(join).poll(&mut cx) {
                        Poll::Pending => (false, None, None),
                        Poll::Ready(outcome) => {
                            state.finishing = true;
                            let completed =
                                std::mem::replace(&mut state.phase, ConsumerTailPhase::Joined);
                            let domain_error = match outcome {
                                Ok(Ok(())) => None,
                                Ok(Err(error)) => Some(error),
                                Err(original) => {
                                    assert!(state.join_failure.is_none());
                                    state.join_failure = Some(original);
                                    None
                                }
                            };
                            (true, domain_error, Some(completed))
                        }
                    }
                } else {
                    (false, None, None)
                }
            };
            if consumer_joined {
                let failed = slot
                    .consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed();
                if failed {
                    slot.mark_consumer_failure();
                }
            }
            // Keep finishing=true through every actual completed-handle and
            // activity destructor. Another poll cannot settle/remove this slot.
            if consumer_joined {
                let completed_handle_dropped =
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drop(completed_consumer_join);
                    })) {
                        Ok(()) => true,
                        Err(payload) => {
                            slot.retain_consumer_panic(payload, true);
                            false
                        }
                    };
                let activity = {
                    let mut state = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
                    if completed_handle_dropped && state.cleanup_returned {
                        state.activity.take()
                    } else {
                        None
                    }
                };
                if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    drop(activity);
                })) {
                    slot.retain_consumer_panic(payload, true);
                }
                slot.consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .finishing = false;
            }
            if let Some((activation, root, outcome)) = ack_joined {
                match outcome {
                    Ok(Ok(())) => {}
                    Ok(Err(failure)) => {
                        tracing::debug!(?activation, %root, failure = ?failure.input(), "optional root ACK exited with a failure")
                    }
                    Err(error) => self.health.record_root_ack_join_failure(error),
                }
            }
            if let Some(error) = domain_error {
                tracing::debug!(%error, "internal consumer exited with its domain verdict");
            }
            self.remove_if_settled(key, &slot);
            self.complete_tail_ready_if_joined(key, &slot);
            if consumer_joined {
                self.progress.notify_waiters();
            }
        }
        self.root_ack_ready.notify_if_ready();
        visited
    }
    pub(crate) async fn root_ack_tail_changed(&self) {
        let changed = self.root_ack_ready.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self
            .root_ack_ready
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ready
            .is_empty()
        {
            return;
        }
        changed.await;
    }
    #[cfg(test)]
    pub(crate) async fn root_ack_tail_progress(&self) {
        let changed = self.root_ack_ready.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if self.poll_root_ack_tails() != 0 {
            return;
        }
        changed.await;
    }
    #[cfg(test)]
    pub(crate) fn contains_registration(&self, initial: QueryExecutionId, generation: u64) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .next()
            .is_some_and(|(key, _)| key.initial == initial && key.generation == generation)
    }
    pub(crate) fn blocks_registration(&self, initial: QueryExecutionId, generation: u64) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .take_while(|(key, _)| key.initial == initial && key.generation == generation)
            .any(|(_, slot)| {
                !slot
                    .root_ack
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .returned
            })
    }
    pub(crate) fn consumers_pending(&self, initial: QueryExecutionId, generation: u64) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .take_while(|(key, _)| key.initial == initial && key.generation == generation)
            .any(|(_, slot)| {
                slot.consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .pending()
            })
    }
    pub(crate) fn contexts(&self, key: NativeActiveCustodyKey) -> Option<Vec<QueryContextRef>> {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .map(|s| s.contexts.iter().copied().collect())
    }
    pub(crate) fn len(&self) -> usize {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub(crate) fn abandon_for_process_exit(&self) {
        self.root_ack_ready.abandon_for_process_exit();
        let slots = std::mem::take(&mut *self.slots.lock().unwrap_or_else(|e| e.into_inner()));
        drop(slots);
    }
    fn remove(&self, key: NativeActiveCustodyKey) {
        let removed = {
            let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            if slots.get(&key).is_some_and(|slot| {
                let consumer = slot.consumer.lock().unwrap_or_else(|e| e.into_inner());
                slot.convergence_failure.is_failed() || consumer.failed() || consumer.pending()
            }) {
                // Neither a later closure nor an empty owner field disposes
                // this original failed source. Explicit process exit does.
                return;
            }
            slots.remove(&key)
        };
        if let Some(removed) = removed {
            if let Some(source) = removed.original_exit.upgrade() {
                source.native_return_silent(
                    key,
                    super::LocalExecutionDisposition::ActualSourcesExited,
                );
            }
            drop(removed);
            self.progress.notify_waiters();
        }
    }
    pub(super) fn consumer_task_frame_layout() -> std::alloc::Layout {
        fn actual<F: Future<Output = Result<(), QueryExecutionError>>>(
            _: fn(Weak<ActiveSlot>) -> F,
        ) -> std::alloc::Layout {
            std::alloc::Layout::new::<F>()
        }
        actual(run_original_consumer)
    }
    pub(super) fn tail_wake_layout() -> std::alloc::Layout {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            RootAckWake,
        >())
        .expect("original shared tail wake layout")
    }
    pub(super) fn slot_layout() -> std::alloc::Layout {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            ActiveSlot,
        >())
        .expect("fixed original Native slot layout")
    }
    pub(crate) fn registration_health(
        &self,
        initial: QueryExecutionId,
        generation: u64,
    ) -> Option<QueryExecutionError> {
        let slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .take_while(|(key, _)| key.initial == initial && key.generation == generation)
            .find_map(|(_, slot)| {
                if slot
                    .consumer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed()
                {
                    return Some(super::supervisor::internal_consumer_panicked(
                        "original consumer failure remains in custody",
                    ));
                }
                if let Some(failure) = slot.convergence_failure.failure() {
                    return Some(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        failure.detail(),
                    ));
                }
                slot.owner
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .map(|held| {
                        QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            match held {
                                ActiveSlotOwner::Activated(_) => {
                                    "successful Native activation still awaits its original handoff"
                                }
                                ActiveSlotOwner::Retained(held) => held.failure.detail(),
                            },
                        )
                    })
            })
    }
}
struct NativeActiveCustodyHandle {
    storage: NativeActiveCustodyRegistry,
    registry: LogicalExecutionRuntimeRegistryHandle,
    key: NativeActiveCustodyKey,
    slot: Arc<ActiveSlot>,
}
impl NativeActiveCustodyHandle {
    fn retain(
        &self,
        owner: Box<dyn ActiveNativeAttemptOwner>,
        failure: StatementNativeHealthFailure,
    ) {
        // The same pre-reserved slot is held directly even during explicit
        // process-exit teardown. Normal retirement cannot remove its map entry.
        let mut held = self.slot.owner.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            held.is_none(),
            "original active owner returns to custody once"
        );
        *held = Some(ActiveSlotOwner::Retained(RetainedNativeActiveOwner {
            _owner: owner,
            failure: failure.clone(),
        }));
        drop(held);
        self.storage.health.record_unproven_exit(failure);
    }
}
/// Same Source output slot. The staged receipt has no strong Registry/health
/// edge and cannot mint an owner or change the exact pre-reserved slot.
pub(crate) struct PendingNativeActivation {
    state: PendingNativeActivationState,
}
enum PendingNativeActivationState {
    Unbound(crate::api::ActivatedNativeAttempt),
    Staged {
        key: NativeActiveCustodyKey,
        slot: Weak<ActiveSlot>,
        rows: Option<crate::api::NativeRowsAttemptRuntime>,
    },
}
impl PendingNativeActivation {
    pub(crate) fn new(original: crate::api::ActivatedNativeAttempt) -> Self {
        Self {
            state: PendingNativeActivationState::Unbound(original),
        }
    }
    #[cfg(test)]
    pub(crate) fn has_original_rows_and_slot_owner(&self) -> bool {
        match &self.state {
            PendingNativeActivationState::Staged { slot, rows, .. } => {
                let Some(slot) = slot.upgrade() else {
                    return false;
                };
                let held = slot.owner.lock().unwrap_or_else(|e| e.into_inner());
                rows.is_some() && matches!(&*held, Some(ActiveSlotOwner::Activated(_)))
            }
            PendingNativeActivationState::Unbound(_) => false,
        }
    }
}

/// Reserve BEFORE activate. On an activation failure, dropping this unbound
/// reservation just removes its empty custody record, not any live Native owner.
pub(crate) struct NativeActiveCustodyReservation {
    custody: Option<NativeActiveCustodyHandle>,
}
impl NativeActiveCustodyReservation {
    /// Move the already stable successful output into its ORIGINAL pre-reserved
    /// slot. All refusal checks precede mutation; no callbacks/drop/notification.
    pub(crate) fn stage_activated_in(
        &self,
        source: &mut Option<PendingNativeActivation>,
    ) -> Result<(), QueryExecutionError> {
        let custody = self.custody.as_ref().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation reservation is absent",
            )
        })?;
        if !source
            .as_ref()
            .is_some_and(|source| matches!(&source.state, PendingNativeActivationState::Unbound(_)))
        {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original successful activation source is absent or already staged",
            ));
        }
        let mut held = custody.slot.owner.lock().unwrap_or_else(|e| e.into_inner());
        if held.is_some() {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation slot already contains an owner",
            ));
        }
        let Some(PendingNativeActivation {
            state: PendingNativeActivationState::Unbound(original),
        }) = source.take()
        else {
            unreachable!()
        };
        let (owner, rows) = original.into_parts();
        *held = Some(ActiveSlotOwner::Activated(owner));
        *source = Some(PendingNativeActivation {
            state: PendingNativeActivationState::Staged {
                key: custody.key,
                slot: Arc::downgrade(&custody.slot),
                rows,
            },
        });
        drop(held);
        Ok(())
    }

    /// Rejoin the same original Box/rows only after dormant actual Drop. Error
    /// preserves source and reservation. This is a silent same-slot transfer.
    pub(crate) fn finish_activated_in(
        &mut self,
        source: &mut Option<PendingNativeActivation>,
    ) -> Result<
        (
            SupervisedActiveNativeOwner,
            Option<crate::api::NativeRowsAttemptRuntime>,
        ),
        QueryExecutionError,
    > {
        let custody = self.custody.as_ref().ok_or_else(|| {
            QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation reservation is absent",
            )
        })?;
        let Some(PendingNativeActivation {
            state: PendingNativeActivationState::Staged { key, slot, .. },
        }) = source.as_ref()
        else {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation output is not staged",
            ));
        };
        if *key != custody.key || slot.as_ptr() != Arc::as_ptr(&custody.slot) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation output belongs to another exact slot",
            ));
        }
        let mut held = custody.slot.owner.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(&*held, Some(ActiveSlotOwner::Activated(_))) {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::Failed,
                "original activation slot has no staged owner",
            ));
        }
        let Some(ActiveSlotOwner::Activated(owner)) = held.take() else {
            unreachable!()
        };
        let Some(PendingNativeActivation {
            state: PendingNativeActivationState::Staged { rows, slot, .. },
        }) = source.take()
        else {
            unreachable!()
        };
        drop(held);
        drop(slot);
        let original = SupervisedActiveNativeOwner {
            owner: Some(owner),
            custody: self.custody.take().expect("validated original reservation"),
            terminal_cut: None,
            remote_deadline: None,
            computation_ready: None,
            closed: false,
        };
        Ok((original, rows))
    }

    #[cfg(test)]
    pub(crate) fn bind(
        mut self,
        original: Box<dyn ActiveNativeAttemptOwner>,
    ) -> SupervisedActiveNativeOwner {
        SupervisedActiveNativeOwner {
            owner: Some(original),
            custody: self
                .custody
                .take()
                .expect("original reservation binds once"),
            terminal_cut: None,
            remote_deadline: None,
            computation_ready: None,
            closed: false,
        }
    }
}
impl Drop for NativeActiveCustodyReservation {
    fn drop(&mut self) {
        if let Some(custody) = self.custody.take() {
            let empty = custody
                .slot
                .owner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_none();
            if empty {
                custody.storage.remove(custody.key);
            }
            // A successful staged owner remains in this SAME canonical slot.
            // It is not an unused reservation and cannot return its source.
        }
    }
}
/// One move-only wrapper around the ORIGINAL Box. Every run/converge operation
/// borrows it. An unpolled residual task or any enclosing unwind returns this
/// same Box to its pre-reserved original registry record.
pub(crate) struct SupervisedActiveNativeOwner {
    owner: Option<Box<dyn ActiveNativeAttemptOwner>>,
    custody: NativeActiveCustodyHandle,
    terminal_cut: Option<Instant>,
    remote_deadline: Option<Instant>,
    computation_ready: Option<crate::api::ComputationReadyToken>,
    closed: bool,
}
impl fmt::Debug for SupervisedActiveNativeOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupervisedActiveNativeOwner")
            .field("key", &self.custody.key)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
impl SupervisedActiveNativeOwner {
    pub(super) fn original_exit(&self) -> Weak<super::original_exit::OriginalLogicalExit> {
        self.custody.slot.original_exit.clone()
    }
    pub(crate) fn convergence_failure_custody(&self) -> NativeConvergenceFailureCustody {
        NativeConvergenceFailureCustody {
            slot: Arc::clone(&self.custody.slot),
        }
    }
    pub(crate) fn as_mut(&mut self) -> &mut dyn ActiveNativeAttemptOwner {
        self.owner
            .as_deref_mut()
            .expect("same active owner has not entered unresolved custody")
    }
    pub(super) fn start_consumer_in(
        &self,
        input: &mut Option<OriginalConsumerStart>,
    ) -> Result<(), QueryExecutionError> {
        self.custody
            .storage
            .start_consumer_in(self.custody.key, &self.custody.slot, input)
    }
    pub(crate) fn reserve_root_ack_tail(
        &mut self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Result<RootAckTailReservation, QueryExecutionError> {
        self.custody.storage.reserve_root_ack(
            self.custody.key,
            &self.custody.slot,
            activation,
            root,
        )
    }
    pub(crate) fn begin_terminal_cut(&mut self, cut: Instant) {
        self.terminal_cut.get_or_insert(cut);
    }
    pub(crate) fn request(
        &mut self,
        cancellation: CancellationView,
        budget: Duration,
    ) -> NativeActiveConvergenceRequest {
        // This fallback is for terminal actor refusals before run was entered.
        // Run/row terminals freeze their cut immediately, before replacement
        // preparation or client/cleanup waits can lend a new observation budget.
        let cut = *self.terminal_cut.get_or_insert_with(Instant::now);
        let deadline = *self.remote_deadline.get_or_insert_with(|| cut + budget);
        NativeActiveConvergenceRequest::new(cancellation, deadline)
            .with_computation_ready(self.computation_ready)
    }
    pub(crate) fn retain_computation_ready(
        &mut self,
        token: crate::api::ComputationReadyToken,
    ) -> Result<(), QueryExecutionError> {
        if token.activation().execution() != self.custody.key.execution
            || token.root().query_execution_id() != self.custody.key.execution
            || self
                .computation_ready
                .is_some_and(|original| original != token)
        {
            return Err(QueryExecutionError::new(
                QueryExecutionErrorKind::InvalidRequest,
                "computation readiness does not belong to the original Native owner",
            ));
        }
        self.computation_ready = Some(token);
        Ok(())
    }
    pub(crate) fn shorten_remote_deadline(&mut self, deadline: Instant) {
        self.remote_deadline = Some(
            self.remote_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }
    pub(crate) fn retain_unresolved(
        &mut self,
        failure: StatementNativeHealthFailure,
    ) -> QueryExecutionError {
        let error = QueryExecutionError::new(QueryExecutionErrorKind::Failed, failure.detail());
        if let Some(owner) = self.owner.take() {
            self.custody.retain(owner, failure);
        }
        error
    }
    pub(crate) async fn record_and_close(
        &mut self,
        closure: NativeActiveConvergenceClosure,
    ) -> Result<(), QueryExecutionError> {
        if let Some(failure) = self.custody.slot.convergence_failure.failure() {
            return Err(self.retain_unresolved(failure));
        }
        let expected = self.custody.slot.contexts.clone();
        let (observed, count) = match &closure {
            NativeActiveConvergenceClosure::Evidence(
                NativeAttemptConvergence::AllWorkersStoppedAndContextsFenced,
            ) => (expected.clone(), expected.len()),
            NativeActiveConvergenceClosure::Evidence(NativeAttemptConvergence::Contexts(facts)) => {
                (facts.iter().map(|f| f.context()).collect(), facts.len())
            }
            NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts } => {
                if self.remote_deadline.is_none_or(|d| Instant::now() < d) {
                    return Err(self.retain_unresolved(
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "active Native remote tracking ended before its original deadline",
                            Arc::new(ActiveCustodyInvariant),
                        ),
                    ));
                }
                (
                    exact_contexts.iter().copied().collect(),
                    exact_contexts.len(),
                )
            }
        };
        if expected != observed || observed.len() != count {
            return Err(self.retain_unresolved(
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native closure differs from its original exact context set",
                    Arc::new(ActiveCustodyInvariant),
                ),
            ));
        }
        // Only the original registry/actor receives these closure facts. Local
        // custody stays held until every original observation was accepted.
        if let Err(error) = self
            .custody
            .registry
            .record_native_custody_closure(self.custody.key, &closure)
            .await
        {
            return Err(self.retain_unresolved(
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native closure could not settle its original registry",
                    Arc::new(error),
                ),
            ));
        }
        self.custody
            .storage
            .record_native_closed(self.custody.key, &self.custody.slot);
        self.closed = true;
        Ok(())
    }
}
#[derive(Debug)]
struct ActiveCustodyInvariant;
impl fmt::Display for ActiveCustodyInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original active Native custody invariant failed")
    }
}
impl std::error::Error for ActiveCustodyInvariant {}
impl Drop for SupervisedActiveNativeOwner {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Some(owner) = self.owner.take() {
            self.custody.retain(
                owner,
                StatementNativeHealthFailure::actual_exit_unproven(
                    "active Native owner left supervision before actual local closure",
                    Arc::new(ActiveCustodyInvariant),
                ),
            );
        }
    }
}

struct NativeConvergencePanic {
    _payload: Mutex<Box<dyn std::any::Any + Send>>,
}
impl fmt::Debug for NativeConvergencePanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeConvergencePanic")
    }
}
impl fmt::Display for NativeConvergencePanic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native convergence future panicked")
    }
}
impl std::error::Error for NativeConvergencePanic {}

pub(crate) async fn catch_native_convergence_panic(
    owner: &mut dyn ActiveNativeAttemptOwner,
    request: NativeActiveConvergenceRequest,
    original: &NativeConvergenceFailureSlot,
) -> Result<
    Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure>,
    StatementNativeHealthFailure,
> {
    if original.is_failed() {
        // CancellationView::drop notifies the original workload authority and
        // may invoke an external waker. Keep the primary solely in its stable
        // slot until that notification has returned.
        drop(request);
        return Err(original.failure().expect("same failed convergence source"));
    }
    // Poll the actual trait future. An async wrapper would destroy that inner
    // future before its Ready(Err) reaches our original receiving slot.
    let mut future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        owner.converge(request)
    })) {
        Ok(future) => future,
        Err(payload) => {
            original.retain(convergence_panic(payload));
            return Err(original
                .failure()
                .expect("original constructor panic received"));
        }
    };
    let outcome = std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(std::task::Poll::Ready(Err(failure))) => {
                original.retain(failure);
                std::task::Poll::Ready(Err(false))
            }
            Ok(std::task::Poll::Ready(Ok(value))) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(payload) => {
                original.retain(convergence_panic(payload));
                std::task::Poll::Ready(Err(true))
            }
        }
    })
    .await;
    // Do not place a clone of the primary in an about-to-return async output
    // before this foreign destructor. Its only cause stays in the original
    // stable slot if Drop unwinds; the outer role retains the secondary.
    drop(future);
    match outcome {
        Ok(value) => Ok(Ok(value)),
        Err(panicked) => {
            let failure = original
                .failure()
                .expect("original convergence failure received");
            if panicked {
                Err(failure)
            } else {
                Ok(Err(failure))
            }
        }
    }
}

fn convergence_panic(payload: Box<dyn std::any::Any + Send>) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "active Native convergence panicked before actual local closure",
        Arc::new(NativeConvergencePanic {
            _payload: Mutex::new(payload),
        }),
    )
}

#[cfg(test)]
pub(crate) struct RootAckTestOwner {
    registry: NativeActiveCustodyRegistry,
    key: NativeActiveCustodyKey,
    slot: Arc<ActiveSlot>,
}
#[cfg(test)]
impl RootAckTestOwner {
    pub(super) fn start_consumer_in(&self, input: &mut Option<OriginalConsumerStart>) {
        self.registry
            .start_consumer_in(self.key, &self.slot, input)
            .unwrap();
    }
    pub(crate) async fn join_consumer(&self) {
        loop {
            let pending = self.slot.consumer.lock().unwrap().pending();
            if !pending {
                return;
            }
            self.registry.root_ack_tail_progress().await;
        }
    }
    pub(crate) fn consumer_pending(&self) -> bool {
        self.slot.consumer.lock().unwrap().pending()
    }
    pub(crate) fn consumer_failed(&self) -> bool {
        self.slot.consumer.lock().unwrap().failed()
    }
    pub(crate) fn consumer_join_panicked(&self) -> bool {
        self.slot
            .consumer
            .lock()
            .unwrap()
            .join_failure
            .as_ref()
            .is_some_and(|e| e.is_panic())
    }
    pub(crate) fn live_registration(&self) -> bool {
        self.registry
            .blocks_registration(self.key.initial, self.key.generation)
    }
    pub(crate) fn dispose_for_process_exit(self) {
        self.registry.abandon_for_process_exit();
    }
    pub(crate) fn close_native(&self) {
        self.registry.record_native_closed(self.key, &self.slot);
    }
    pub(crate) async fn drain(&self) {
        while self.registry.root_ack_tail_count() != 0 {
            self.registry.root_ack_tail_progress().await;
        }
    }
    pub(crate) fn retained(&self) -> bool {
        self.registry
            .contains_registration(self.key.initial, self.key.generation)
    }
}
#[cfg(test)]
pub(crate) fn root_ack_test_fixture(
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
) -> (RootAckTestOwner, RootAckTailReservation) {
    let registry = NativeActiveCustodyRegistry::new(
        LogicalExecutionHealth::new(Arc::new(Notify::new())),
        Arc::new(Notify::new()),
    );
    root_ack_test_fixture_in(registry, activation, root)
}
#[cfg(test)]
fn root_ack_test_fixture_in(
    registry: NativeActiveCustodyRegistry,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
) -> (RootAckTestOwner, RootAckTailReservation) {
    let key = NativeActiveCustodyKey {
        initial: activation.execution(),
        generation: 1,
        execution: activation.execution(),
    };
    root_ack_test_fixture_for_key(registry, key, activation, root)
}
#[cfg(test)]
pub(crate) fn root_ack_successor_fixture(
    previous: &RootAckTestOwner,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
) -> (RootAckTestOwner, RootAckTailReservation) {
    assert_eq!(
        previous.key.initial.query_id(),
        activation.execution().query_id()
    );
    root_ack_test_fixture_for_key(
        previous.registry.clone(),
        NativeActiveCustodyKey {
            initial: previous.key.initial,
            generation: previous.key.generation,
            execution: activation.execution(),
        },
        activation,
        root,
    )
}
#[cfg(test)]
fn root_ack_test_fixture_for_key(
    registry: NativeActiveCustodyRegistry,
    key: NativeActiveCustodyKey,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
) -> (RootAckTestOwner, RootAckTailReservation) {
    let slot = Arc::new(ActiveSlot {
        original_exit: Weak::new(),
        contexts: BTreeSet::from([QueryContextRef::new(
            key.execution,
            novarocks_types::FrontendProcessId::new_v7(),
            root.backend_process_id(),
        )]),
        owner: Mutex::new(None),
        convergence_failure: NativeConvergenceFailureSlot::default(),
        root_ack: Mutex::new(RootAckTailState::default()),
        consumer: Mutex::new(ConsumerTailState::default()),
    });
    registry
        .slots
        .lock()
        .unwrap()
        .insert(key, Arc::clone(&slot));
    let reservation = registry
        .reserve_root_ack(key, &slot, activation, root)
        .unwrap();
    (
        RootAckTestOwner {
            registry,
            key,
            slot,
        },
        reservation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{BackendProcessId, FrontendProcessId};

    #[derive(Debug)]
    struct PrimaryConvergenceCause(Arc<std::sync::atomic::AtomicUsize>);
    impl fmt::Display for PrimaryConvergenceCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original convergence fixture cause")
        }
    }
    impl std::error::Error for PrimaryConvergenceCause {}
    impl Drop for PrimaryConvergenceCause {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct FailedConvergenceFuture {
        reported: Option<StatementNativeHealthFailure>,
        cause_drops: Arc<std::sync::atomic::AtomicUsize>,
        polls: Arc<std::sync::atomic::AtomicUsize>,
        future_drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Future for FailedConvergenceFuture {
        type Output = Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            this.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(failure) = this.reported.take() {
                Poll::Ready(Err(failure))
            } else {
                std::panic::panic_any(PrimaryConvergenceCause(Arc::clone(&this.cause_drops)));
            }
        }
    }
    impl Drop for FailedConvergenceFuture {
        fn drop(&mut self) {
            self.future_drops
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("secondary convergence future destructor panic");
        }
    }

    struct PanicOnceConvergenceWake(std::sync::atomic::AtomicBool);
    impl Wake for PanicOnceConvergenceWake {
        fn wake(self: Arc<Self>) {
            if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
                panic!("failed convergence request-drop notification panic");
            }
        }
    }

    #[derive(Debug)]
    struct FailedConvergenceOwner {
        reported: bool,
        cause_drops: Arc<std::sync::atomic::AtomicUsize>,
        polls: Arc<std::sync::atomic::AtomicUsize>,
        future_drops: Arc<std::sync::atomic::AtomicUsize>,
        factory_calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl ActiveNativeAttemptOwner for FailedConvergenceOwner {
        fn run<'a>(
            &'a mut self,
            _: &'a crate::coordination::NativeAttemptDrive,
            _: CancellationView,
        ) -> crate::api::NativeAttemptRunFuture<'a> {
            panic!("the convergence fixture must not run an attempt");
        }
        fn converge<'a>(
            &'a mut self,
            _: NativeActiveConvergenceRequest,
        ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
            self.factory_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let reported = self.reported.then(|| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "reported convergence fixture failure",
                    Arc::new(PrimaryConvergenceCause(Arc::clone(&self.cause_drops))),
                )
            });
            Box::pin(FailedConvergenceFuture {
                reported,
                cause_drops: Arc::clone(&self.cause_drops),
                polls: Arc::clone(&self.polls),
                future_drops: Arc::clone(&self.future_drops),
            })
        }
    }

    #[test]
    fn original_convergence_cause_is_in_the_same_slot_before_future_drop_panics() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for reported_mode in [false, true] {
            let (control, work, capacity, window) = tail_work();
            let request = NativeActiveConvergenceRequest::new(
                work.owner.scope().cancellation().unwrap(),
                Instant::now(),
            );
            let (activation, root) = tail_identity(490 + i64::from(reported_mode));
            let (owner, reservation) = root_ack_test_fixture(activation, root);
            drop(reservation);
            let cause_drops = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let future_drops = Arc::new(AtomicUsize::new(0));
            let factory_calls = Arc::new(AtomicUsize::new(0));
            let mut actual = FailedConvergenceOwner {
                reported: reported_mode,
                cause_drops: Arc::clone(&cause_drops),
                polls: Arc::clone(&polls),
                future_drops: Arc::clone(&future_drops),
                factory_calls: Arc::clone(&factory_calls),
            };
            let mut caught = Box::pin(catch_native_convergence_panic(
                &mut actual,
                request.clone(),
                &owner.slot.convergence_failure,
            ));
            let secondary = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                caught
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }))
            .unwrap_err();
            assert_eq!(
                secondary.downcast_ref::<&str>(),
                Some(&"secondary convergence future destructor panic")
            );
            drop(caught);
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            let failure = owner.slot.convergence_failure.failure().unwrap();
            let original_cause = failure.clone().into_parts().1;
            if reported_mode {
                assert!(original_cause.is::<PrimaryConvergenceCause>());
            } else {
                let panic = original_cause
                    .downcast_ref::<NativeConvergencePanic>()
                    .unwrap();
                assert!(
                    panic
                        ._payload
                        .lock()
                        .unwrap()
                        .is::<PrimaryConvergenceCause>()
                );
            }
            // A failed source refuses before constructing another trait future.
            let mut retry = Box::pin(catch_native_convergence_panic(
                &mut actual,
                request,
                &owner.slot.convergence_failure,
            ));
            let Poll::Ready(Err(repeated)) =
                retry.as_mut().poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("the original failed source must refuse another poll");
            };
            assert!(std::ptr::eq(failure.cause(), repeated.cause()));
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(future_drops.load(Ordering::SeqCst), 1);
            drop((retry, repeated, failure, original_cause));
            // The failed-entry request has a real notifying CancellationView.
            // No diagnostic alias retains the primary across that callback.
            let request = NativeActiveConvergenceRequest::new(
                work.owner.scope().cancellation().unwrap(),
                Instant::now(),
            );
            let wake = Waker::from(Arc::new(PanicOnceConvergenceWake(
                std::sync::atomic::AtomicBool::new(true),
            )));
            let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
            assert!(
                progress
                    .as_mut()
                    .poll(&mut Context::from_waker(&wake))
                    .is_pending()
            );
            let mut rejected = Box::pin(catch_native_convergence_panic(
                &mut actual,
                request,
                &owner.slot.convergence_failure,
            ));
            let notification = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rejected
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
            }))
            .unwrap_err();
            assert_eq!(
                notification.downcast_ref::<&str>(),
                Some(&"failed convergence request-drop notification panic")
            );
            drop((rejected, notification, progress));
            assert_eq!(factory_calls.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            owner.close_native();
            assert!(owner.retained());
            assert!(
                owner
                    .registry
                    .registration_health(owner.key.initial, owner.key.generation)
                    .is_some()
            );
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            owner.registry.abandon_for_process_exit();
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            drop(owner);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
            drop((window, capacity, actual));
            work.owner.complete();
            work.business.release();
        }
    }

    fn execution(high: i64, low: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(high, low), AttemptId::new(attempt).unwrap()).unwrap()
    }

    #[derive(Debug)]
    struct MetadataOnlyOwner;
    impl ActiveNativeAttemptOwner for MetadataOnlyOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a crate::coordination::NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> crate::api::NativeAttemptRunFuture<'a> {
            panic!("prefix metadata fixture must not dispatch work")
        }
        fn converge<'a>(
            &'a mut self,
            _request: NativeActiveConvergenceRequest,
        ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
            panic!("prefix metadata fixture must not converge work")
        }
    }
    #[derive(Debug)]
    struct MetadataHealthCause;
    impl fmt::Display for MetadataHealthCause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original prefix metadata cause")
        }
    }
    impl std::error::Error for MetadataHealthCause {}

    fn tail_identity(tag: i64) -> (AttemptActivationIdentity, TaskIdentity) {
        let execution = execution(93, tag, 1);
        let root = TaskIdentity::new(
            execution,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        (
            AttemptActivationIdentity::from_parts(318, execution, 1),
            root,
        )
    }
    fn tail_work() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_workload_control::ResultCapacityHandle,
        novarocks_workload_control::ResultWindowGrant,
    ) {
        use novarocks_workload_control::*;
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: 1 << 18,
            },
        )
        .unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let window = capacity
            .try_acquire(&work.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        (control, work, capacity, window)
    }
    fn tail_failure(root: TaskIdentity) -> super::super::RootResultFetchFailure {
        use crate::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_execution_contract::failure::FailureProjection;
        super::super::RootResultFetchFailure::from_input(ExecutionFailureInput::new(
            QueryFailureReason::TransportDeadline.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating("optional ACK fixture deadline"),
            ExecutionFailureSite::RootRead { root },
        ))
    }
    struct TailDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for TailDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn early_remote_tracking_keeps_same_owner_and_window_without_renewing_terminal_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct HeldWindowOwner {
            _window: novarocks_workload_control::ResultWindowGrant,
            drops: Arc<AtomicUsize>,
        }
        impl fmt::Debug for HeldWindowOwner {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("HeldWindowOwner").finish_non_exhaustive()
            }
        }
        impl Drop for HeldWindowOwner {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl ActiveNativeAttemptOwner for HeldWindowOwner {
            fn run<'a>(
                &'a mut self,
                _: &'a crate::coordination::NativeAttemptDrive,
                _: CancellationView,
            ) -> crate::api::NativeAttemptRunFuture<'a> {
                panic!("an early tracking proof must not dispatch Native work")
            }
            fn converge<'a>(
                &'a mut self,
                _: NativeActiveConvergenceRequest,
            ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
                panic!("a rejected tracking proof must not restart convergence")
            }
        }

        let (control, work, capacity, window) = tail_work();
        let scope = work.owner.scope();
        let (activation, root) = tail_identity(431);
        let key = NativeActiveCustodyKey {
            initial: activation.execution(),
            generation: 1,
            execution: activation.execution(),
        };
        let context = QueryContextRef::new(
            key.execution,
            FrontendProcessId::new_v7(),
            root.backend_process_id(),
        );
        let mut runtime =
            super::super::LogicalExecutionRuntimeRegistry::new(tokio::runtime::Handle::current());
        let storage = NativeActiveCustodyRegistry::new(
            LogicalExecutionHealth::new(Arc::new(Notify::new())),
            Arc::new(Notify::new()),
        );
        // Exercise the real exact-context reservation and owner binding. This
        // rejection precedes forwarding any closure to the actor registry.
        let reservation = storage
            .reserve_with_original_exit(key, &[context], runtime.handle(), Weak::new())
            .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let original: Box<dyn ActiveNativeAttemptOwner> = Box::new(HeldWindowOwner {
            _window: window,
            drops: Arc::clone(&drops),
        });
        let address = &*original as *const dyn ActiveNativeAttemptOwner as *const () as usize;
        let mut active = reservation.bind(original);
        let ack = active.reserve_root_ack_tail(activation, root).unwrap();
        drop(ack); // No ACK was issued; this supplies no Native exit evidence.

        let cut = Instant::now();
        let budget = Duration::from_secs(30);
        active.begin_terminal_cut(cut);
        let first = active.request(scope.cancellation().unwrap(), budget);
        assert_eq!(first.remote_observation_deadline(), cut + budget);
        active.begin_terminal_cut(cut + Duration::from_secs(1));
        let repeated = active.request(scope.cancellation().unwrap(), budget + budget);
        assert_eq!(
            repeated.remote_observation_deadline(),
            first.remote_observation_deadline()
        );
        assert!(Instant::now() < first.remote_observation_deadline());
        drop((first, repeated));

        let error = active
            .record_and_close(NativeActiveConvergenceClosure::RemoteTrackingEnded {
                exact_contexts: vec![context].into_boxed_slice(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            error.message(),
            "active Native remote tracking ended before its original deadline",
        );
        assert!(!active.closed);
        assert!(
            active.owner.is_none(),
            "the same Box moves to unresolved custody"
        );
        assert_eq!(storage.len(), 1);
        {
            let held = active.custody.slot.owner.lock().unwrap();
            let Some(ActiveSlotOwner::Retained(original)) = held.as_ref() else {
                panic!("an early tracking proof must retain the original owner");
            };
            assert_eq!(
                &*original._owner as *const dyn ActiveNativeAttemptOwner as *const () as usize,
                address,
            );
            assert!(original.failure.cause().is::<ActiveCustodyInvariant>());
            assert_eq!(original.failure.detail(), error.message());
        }
        assert!(!active.custody.slot.root_ack.lock().unwrap().native_closed);
        assert!(storage.blocks_registration(key.initial, key.generation));
        assert_eq!(storage.health.health().unwrap().message(), error.message());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        work.owner.complete();
        work.business.release();
        assert_eq!(control.snapshot().root_responsibilities, 1);
        drop(active);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert_eq!(
            control
                .snapshot()
                .obligation_endings
                .tracking_ended_remote_unknown,
            0
        );

        // Only the explicit process-exit disposition abandons this failed
        // owner. No error, unused ACK, or borrowed wrapper drop certified exit.
        storage.abandon_for_process_exit();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(control.snapshot().root_responsibilities, 0);
        assert_eq!(
            control.snapshot().obligation_endings.settled_with_evidence,
            0
        );
        runtime.abandon_for_process_exit();
    }

    #[tokio::test]
    async fn original_ack_survives_driver_unwind_before_and_after_first_poll() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for after_first_poll in [false, true] {
            let (_control, work, capacity, window) = tail_work();
            let (activation, root) = tail_identity(401 + i64::from(after_first_poll));
            let (owner, reservation) = root_ack_test_fixture(activation, root);
            let (release, gate) = tokio::sync::oneshot::channel();
            let (entered, observed) = tokio::sync::oneshot::channel();
            let exited = Arc::new(AtomicBool::new(false));
            let drop_witness = TailDrop(Arc::clone(&exited));
            let physical_guard = window.retain_alias();
            let failure = tail_failure(root);
            let original: RootAckFuture = Box::pin(async move {
                let (_physical_guard, _drop_witness) = (physical_guard, drop_witness);
                let _ = entered.send(());
                gate.await.unwrap();
                Err(failure)
            });
            let driver = tokio::spawn(async move {
                reservation.start(original);
                if after_first_poll {
                    observed.await.unwrap();
                }
                panic!("original logical driver unwound after synchronous ACK handoff");
            });
            assert!(driver.await.unwrap_err().is_panic());
            owner.close_native();
            drop(window);
            assert!(owner.retained());
            assert!(!exited.load(Ordering::SeqCst));
            assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
            assert!(owner.registry.health.health().is_none());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(2), owner.drain())
                .await
                .unwrap();
            assert!(exited.load(Ordering::SeqCst));
            assert!(!owner.retained());
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert!(
                owner.registry.health.health().is_none(),
                "ordinary ACK Err is diagnostic only"
            );
            drop(work);
        }
    }

    #[tokio::test]
    async fn tail_ready_index_is_deduplicated_fair_and_bounded_per_turn() {
        let (activation, root) = tail_identity(500);
        let (first, first_reservation) = root_ack_test_fixture(activation, root);
        let registry = first.registry.clone();
        let mut tails = vec![(first, first_reservation)];
        for tag in 501..(501 + ROOT_ACK_TAIL_TURN_BUDGET as i64) {
            let (activation, root) = tail_identity(tag);
            tails.push(root_ack_test_fixture_in(registry.clone(), activation, root));
        }
        let mut releases = Vec::new();
        let mut owners = Vec::new();
        for (owner, reservation) in tails {
            let (release, gate) = tokio::sync::oneshot::channel();
            let root = reservation.root;
            reservation.start(Box::pin(async move {
                gate.await.unwrap();
                Err(tail_failure(root))
            }));
            owner.close_native();
            releases.push(release);
            owners.push(owner);
        }
        let count = ROOT_ACK_TAIL_TURN_BUDGET + 1;
        assert_eq!(registry.root_ack_tail_count(), count);
        let first_key = owners[0].key;
        for _ in 0..1024 {
            registry.root_ack_ready.enqueue(first_key);
        }
        assert_eq!(
            registry.root_ack_ready.state.lock().unwrap().ready.len(),
            count
        );
        assert_eq!(registry.poll_root_ack_tails(), ROOT_ACK_TAIL_TURN_BUDGET);
        assert_eq!(registry.root_ack_ready.state.lock().unwrap().ready.len(), 1);
        // An early key becoming ready again cannot displace the unvisited last
        // key; the original rotating cursor is retained across bounded turns.
        registry.root_ack_ready.enqueue(first_key);
        assert_eq!(
            registry.root_ack_ready.pop_ready(),
            Some(owners[count - 1].key)
        );
        registry.root_ack_ready.enqueue(owners[count - 1].key);
        for release in releases {
            release.send(()).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), owners[0].drain())
            .await
            .unwrap();
        assert_eq!(registry.root_ack_tail_count(), 0);
        assert!(owners.iter().all(|owner| !owner.retained()));
        // Late completion wakes cannot recreate already joined ownership or
        // leave an ever-growing list of stale queue entries.
        for owner in &owners {
            registry.root_ack_ready.enqueue(owner.key);
        }
        assert!(
            registry
                .root_ack_ready
                .state
                .lock()
                .unwrap()
                .ready
                .is_empty()
        );
    }

    #[tokio::test]
    async fn unused_ack_reservation_closes_explicitly_and_rejects_overwrite() {
        let (activation, root) = tail_identity(410);
        let (owner, reservation) = root_ack_test_fixture(activation, root);
        assert!(
            owner
                .registry
                .reserve_root_ack(owner.key, &owner.slot, activation, root)
                .is_err()
        );
        let (foreign, foreign_root) = tail_identity(411);
        assert!(
            owner
                .registry
                .reserve_root_ack(owner.key, &owner.slot, foreign, foreign_root)
                .is_err()
        );
        owner.close_native();
        assert!(
            owner.retained(),
            "Native closure cannot close a live reservation"
        );
        drop(reservation);
        assert!(!owner.retained(), "unused reservation records NeverIssued");
    }

    #[tokio::test]
    async fn original_ack_join_panic_closes_only_that_tail_and_keeps_pending_peer() {
        let (activation, root) = tail_identity(420);
        let (pending, reservation) = root_ack_test_fixture(activation, root);
        let (peer_activation, peer_root) = tail_identity(421);
        let (panicked, panic_reservation) =
            root_ack_test_fixture_in(pending.registry.clone(), peer_activation, peer_root);
        let (release, gate) = tokio::sync::oneshot::channel();
        reservation.start(Box::pin(async move {
            gate.await.unwrap();
            Err(tail_failure(root))
        }));
        panic_reservation.start(Box::pin(async {
            panic!("original optional ACK future panic")
        }));
        pending.close_native();
        panicked.close_native();
        tokio::time::timeout(Duration::from_secs(2), async {
            while panicked.retained() {
                pending.registry.root_ack_tail_progress().await;
            }
        })
        .await
        .unwrap();
        assert!(
            pending.retained(),
            "pending peer keeps its own original join"
        );
        assert_eq!(pending.registry.root_ack_tail_count(), 1);
        assert_eq!(
            pending.registry.health.health().unwrap().message(),
            "optional root ACK task panicked after actual local exit"
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), pending.drain())
            .await
            .unwrap();
        assert!(!pending.retained());
    }

    #[tokio::test]
    async fn cancelled_original_ack_join_is_actual_exit_with_its_original_health_cause() {
        let (_control, work, capacity, window) = tail_work();
        let (activation, root) = tail_identity(425);
        let (owner, reservation) = root_ack_test_fixture(activation, root);
        let physical_guard = window.retain_alias();
        reservation.start(Box::pin(async move {
            let _physical_guard = physical_guard;
            std::future::pending().await
        }));
        owner.close_native();
        drop(window);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        // Only this adversarial test cancels the actual Tokio child. Production
        // cancellation and remote observation deadlines never abort this tail.
        {
            let state = owner.slot.root_ack.lock().unwrap();
            let RootAckTailPhase::Running { join, .. } = &state.phase else {
                panic!("tail owns join")
            };
            join.abort();
        }
        assert!(owner.retained(), "requesting abort is not an exit receipt");
        tokio::time::timeout(Duration::from_secs(2), owner.drain())
            .await
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert!(!owner.retained());
        assert_eq!(
            owner.registry.health.health().unwrap().message(),
            "optional root ACK task was cancelled after actual local exit"
        );
        drop(work);
    }

    #[tokio::test]
    async fn tail_exit_alone_cannot_remove_unclosed_native_owner() {
        let (activation, root) = tail_identity(430);
        let (owner, reservation) = root_ack_test_fixture(activation, root);
        reservation.start(Box::pin(async move { Err(tail_failure(root)) }));
        tokio::time::timeout(Duration::from_secs(2), owner.drain())
            .await
            .unwrap();
        assert!(
            owner.retained(),
            "tail join supplies no Native closure fact"
        );
        owner.close_native();
        assert!(!owner.retained());
    }

    #[test]
    fn health_search_stays_in_exact_prefix_and_keeps_first_held_source_order() {
        let registry = NativeActiveCustodyRegistry::new(
            LogicalExecutionHealth::new(Arc::new(Notify::new())),
            Arc::new(Notify::new()),
        );
        let initial = execution(91, 71, 3);
        let key = |generation, attempt| NativeActiveCustodyKey {
            initial,
            generation,
            execution: execution(91, 71, attempt),
        };
        let before = key(6, 3);
        let unheld = key(7, 3);
        let first_held = key(7, 4);
        let later_held = key(7, 5);
        let after = key(8, 3);
        let cause: Arc<dyn std::error::Error + Send + Sync> = Arc::new(MetadataHealthCause);
        {
            let mut slots = registry.slots.lock().unwrap();
            for (key, detail) in [
                (before, Some("foreign earlier prefix health")),
                (unheld, None),
                (first_held, Some("first exact prefix health")),
                (later_held, Some("later exact prefix health")),
                (after, Some("foreign later prefix health")),
            ] {
                let held = detail.map(|detail| {
                    ActiveSlotOwner::Retained(RetainedNativeActiveOwner {
                        _owner: Box::new(MetadataOnlyOwner),
                        failure: StatementNativeHealthFailure::actual_exit_unproven(
                            detail,
                            Arc::clone(&cause),
                        ),
                    })
                });
                slots.insert(
                    key,
                    Arc::new(ActiveSlot {
                        original_exit: Weak::new(),
                        contexts: BTreeSet::from([QueryContextRef::new(
                            key.execution,
                            FrontendProcessId::new_v7(),
                            BackendProcessId::new_v7(),
                        )]),
                        owner: Mutex::new(held),
                        convergence_failure: NativeConvergenceFailureSlot::default(),
                        root_ack: Mutex::new(RootAckTailState::default()),
                        consumer: Mutex::new(ConsumerTailState::default()),
                    }),
                );
            }
        }
        assert!(
            registry.registration_health(initial, 5).is_none(),
            "the next prefix cannot supply missing-prefix health"
        );
        assert!(
            registry
                .registration_health(execution(91, 71, 2), 7)
                .is_none(),
            "the same QueryId with another initial attempt is a different registration"
        );
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "first exact prefix health"
        );
        let retained = registry
            .slots
            .lock()
            .unwrap()
            .get(&first_held)
            .cloned()
            .unwrap();
        let retained_cause = {
            let held = retained.owner.lock().unwrap();
            let Some(ActiveSlotOwner::Retained(original)) = held.as_ref() else {
                panic!("the exact retained owner must keep its original failure");
            };
            original.failure.clone().into_parts().1
        };
        assert!(
            Arc::ptr_eq(&retained_cause, &cause),
            "read-only health lookup retains the original typed source Arc"
        );
        registry.remove(first_held);
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "later exact prefix health"
        );
        registry.remove(later_held);
        assert!(
            registry.contains_registration(initial, 7),
            "the same unheld reservation remains"
        );
        assert!(
            registry.registration_health(initial, 7).is_none(),
            "an unheld own slot cannot leak the next prefix's health"
        );
    }

    #[test]
    fn exact_registration_prefix_membership_covers_adjacent_and_extreme_keys() {
        let registry = NativeActiveCustodyRegistry::new(
            LogicalExecutionHealth::new(Arc::new(Notify::new())),
            Arc::new(Notify::new()),
        );
        let low = execution(i64::MIN, i64::MIN, 1);
        let initial = execution(91, 71, 3);
        let same_query_other_initial = execution(91, 71, 4);
        let high = execution(i64::MAX, i64::MAX, u64::MAX);
        assert!(!registry.contains_registration(initial, 7));
        let keys = [
            NativeActiveCustodyKey {
                initial: low,
                generation: 1,
                execution: low,
            },
            NativeActiveCustodyKey {
                initial,
                generation: 7,
                execution: execution(91, 71, 3),
            },
            NativeActiveCustodyKey {
                initial,
                generation: 7,
                execution: execution(91, 71, u64::MAX),
            },
            NativeActiveCustodyKey {
                initial,
                generation: 9,
                execution: execution(91, 71, 4),
            },
            NativeActiveCustodyKey {
                initial: same_query_other_initial,
                generation: 7,
                execution: same_query_other_initial,
            },
            NativeActiveCustodyKey {
                initial: high,
                generation: u64::MAX,
                execution: high,
            },
        ];
        // These are the same private empty pre-reservation slots: valid exact
        // contexts, no live owner, no WorkOwner/obligation, no exit evidence.
        {
            let mut slots = registry.slots.lock().unwrap();
            for key in keys {
                slots.insert(
                    key,
                    Arc::new(ActiveSlot {
                        original_exit: Weak::new(),
                        contexts: BTreeSet::from([QueryContextRef::new(
                            key.execution,
                            FrontendProcessId::new_v7(),
                            BackendProcessId::new_v7(),
                        )]),
                        owner: Mutex::new(None),
                        convergence_failure: NativeConvergenceFailureSlot::default(),
                        root_ack: Mutex::new(RootAckTailState::default()),
                        consumer: Mutex::new(ConsumerTailState::default()),
                    }),
                );
            }
        }
        let probes = [
            (low, 0),
            (low, 1),
            (low, u64::MAX),
            (execution(91, 71, 2), 7),
            (initial, 0),
            (initial, 6),
            (initial, 7),
            (initial, 8),
            (initial, 9),
            (initial, 10),
            (same_query_other_initial, 7),
            (same_query_other_initial, 8),
            (execution(91, 72, 1), 7),
            (high, u64::MAX - 1),
            (high, u64::MAX),
        ];
        for (initial, generation) in probes {
            let expected = keys
                .iter()
                .any(|key| key.initial == initial && key.generation == generation);
            assert_eq!(
                registry.contains_registration(initial, generation),
                expected,
                "membership must retain the exact initial execution and generation prefix"
            );
        }
        let mut slots = registry.slots.lock().unwrap();
        slots.remove(&keys[1]);
        drop(slots);
        assert!(
            registry.contains_registration(initial, 7),
            "another exact attempt still holds original custody"
        );
        registry.remove(keys[2]);
        assert!(
            !registry.contains_registration(initial, 7),
            "an adjacent generation cannot keep the removed prefix live"
        );
    }
}
