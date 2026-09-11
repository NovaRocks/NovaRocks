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

//! Bounded frontend intake for exact query-context convergence receipts.
//!
//! An async transport owns only a cloneable [`ContextConvergenceIntakeHandle`].
//! It can retain a complete receipt and learn whether that exact observation
//! may advance its subscription cursor. It cannot apply the observation to a
//! logical execution. The move-only [`ContextConvergenceIntake`] is the single
//! serial owner that drains retained observations.
//!
//! Capacity is counted in distinct contexts waiting to be drained. A newer
//! complete receipt replaces the older receipt in the context's existing slot,
//! so one noisy context cannot consume additional capacity or move itself ahead
//! of other contexts.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use novarocks_execution::task_execution::{
    QueryContextConvergenceReceipt, QueryContextConvergenceState, QueryContextRef,
};
use tokio::sync::watch;

/// The observable state of the intake's distinct-context capacity.
///
/// `epoch` advances only when acknowledgement removes a retained context and
/// therefore makes one fixed slot reusable. Closing the intake changes only
/// `closed`, while still waking every capacity waiter.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ContextConvergenceCapacityState {
    epoch: u64,
    closed: bool,
}

impl ContextConvergenceCapacityState {
    pub const fn epoch(self) -> u64 {
        self.epoch
    }

    pub const fn is_closed(self) -> bool {
        self.closed
    }
}

#[derive(Debug)]
struct ContextConvergenceIntakeState {
    open: bool,
    pending: BTreeMap<QueryContextRef, QueryContextConvergenceReceipt>,
    order: VecDeque<QueryContextRef>,
    leased: Option<(QueryContextRef, QueryContextConvergenceReceipt)>,
}

#[derive(Debug)]
struct ContextConvergenceIntakeInner {
    capacity: NonZeroUsize,
    state: Mutex<ContextConvergenceIntakeState>,
    wake: tokio::sync::Notify,
    capacity_state: watch::Sender<ContextConvergenceCapacityState>,
}

impl ContextConvergenceIntakeInner {
    fn release_capacity(&self) {
        self.capacity_state.send_modify(|state| {
            state.epoch = state
                .epoch
                .checked_add(1)
                .expect("context convergence capacity epoch exhausted");
        });
    }

    fn close_capacity(&self) {
        self.capacity_state.send_modify(|state| state.closed = true);
    }
}

/// A successful publication decision.
///
/// Only these two decisions authorize the transport to advance its exact
/// context cursor. Every [`ContextConvergencePublishError`] leaves the cursor
/// unchanged.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[must_use = "a transport must inspect admission before advancing its context cursor"]
pub enum ContextConvergencePublishAdmission {
    /// The complete receipt was first retained or replaced an older version.
    Retained,
    /// The same complete receipt was already retained.
    Idempotent,
}

impl ContextConvergencePublishAdmission {
    pub const fn authorizes_cursor_advance(self) -> bool {
        matches!(self, Self::Retained | Self::Idempotent)
    }
}

/// Why a convergence receipt was not retained.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextConvergencePublishError {
    /// The decoded receipt names a different exact context than the stream.
    ContextMismatch,
    /// The receipt does not report a closed convergence state.
    InvalidClosureState,
    /// The serial intake owner was dropped.
    Closed,
    /// Another receipt already occupies the same version with different facts.
    ConflictingVersion,
    /// The receipt is older than the complete observation already retained.
    StaleVersion,
    /// No slot exists for this context and every fixed slot is occupied.
    Overflow,
}

impl fmt::Display for ContextConvergencePublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ContextMismatch => {
                "query context convergence receipt names a different exact context"
            }
            Self::InvalidClosureState => {
                "query context convergence receipt does not report a closed state"
            }
            Self::Closed => "query context convergence intake is closed",
            Self::ConflictingVersion => {
                "query context convergence version conflicts with the retained receipt"
            }
            Self::StaleVersion => {
                "query context convergence receipt is older than the retained receipt"
            }
            Self::Overflow => "query context convergence intake capacity is exhausted",
        })
    }
}

impl std::error::Error for ContextConvergencePublishError {}

/// The cloneable capability held by async transport callbacks.
#[derive(Clone, Debug)]
pub struct ContextConvergenceIntakeHandle {
    inner: Arc<ContextConvergenceIntakeInner>,
}

impl ContextConvergenceIntakeHandle {
    /// Returns the current capacity epoch and closure state.
    pub fn capacity_state(&self) -> ContextConvergenceCapacityState {
        *self.inner.capacity_state.borrow()
    }

    /// Waits until a distinct-context slot is released or the intake closes.
    ///
    /// The caller supplies the last epoch it observed before an overflow. The
    /// watch value preserves a release that races with waiter registration, so
    /// the wait cannot lose the transition.
    pub async fn wait_for_capacity_change(
        &self,
        observed_epoch: u64,
    ) -> ContextConvergenceCapacityState {
        let mut receiver = self.inner.capacity_state.subscribe();
        loop {
            let current = *receiver.borrow_and_update();
            if current.epoch != observed_epoch || current.closed {
                return current;
            }
            if receiver.changed().await.is_err() {
                return *receiver.borrow();
            }
        }
    }

    /// Retains one complete receipt for the stream's exact context.
    ///
    /// A transport may advance its cursor only after this returns
    /// [`ContextConvergencePublishAdmission::Retained`] or
    /// [`ContextConvergencePublishAdmission::Idempotent`]. In particular,
    /// overflow returns before any receipt or cursor position changes.
    pub fn publish(
        &self,
        stream_context: QueryContextRef,
        receipt: QueryContextConvergenceReceipt,
    ) -> Result<ContextConvergencePublishAdmission, ContextConvergencePublishError> {
        if receipt.context() != stream_context {
            return Err(ContextConvergencePublishError::ContextMismatch);
        }
        if receipt.state() != QueryContextConvergenceState::WorkerStoppedAndContextFenced {
            return Err(ContextConvergencePublishError::InvalidClosureState);
        }

        let admission = {
            let mut state = self.inner.state.lock().expect("context convergence intake");
            if !state.open {
                return Err(ContextConvergencePublishError::Closed);
            }

            match state.pending.get(&stream_context).copied() {
                Some(current) if receipt.version() < current.version() => {
                    return Err(ContextConvergencePublishError::StaleVersion);
                }
                Some(current) if receipt.version() == current.version() => {
                    if receipt == current {
                        ContextConvergencePublishAdmission::Idempotent
                    } else {
                        return Err(ContextConvergencePublishError::ConflictingVersion);
                    }
                }
                Some(_) => {
                    state.pending.insert(stream_context, receipt);
                    ContextConvergencePublishAdmission::Retained
                }
                None => {
                    if state.pending.len() >= self.inner.capacity.get() {
                        return Err(ContextConvergencePublishError::Overflow);
                    }
                    state.pending.insert(stream_context, receipt);
                    state.order.push_back(stream_context);
                    ContextConvergencePublishAdmission::Retained
                }
            }
        };

        if admission == ContextConvergencePublishAdmission::Retained {
            self.inner.wake.notify_one();
        }
        Ok(admission)
    }
}

/// The move-only, single serial owner of retained convergence observations.
#[derive(Debug)]
pub struct ContextConvergenceIntake {
    inner: Arc<ContextConvergenceIntakeInner>,
}

impl ContextConvergenceIntake {
    pub fn bounded(capacity: NonZeroUsize) -> Self {
        let (capacity_state, _) = watch::channel(ContextConvergenceCapacityState {
            epoch: 0,
            closed: false,
        });
        Self {
            inner: Arc::new(ContextConvergenceIntakeInner {
                capacity,
                state: Mutex::new(ContextConvergenceIntakeState {
                    open: true,
                    pending: BTreeMap::new(),
                    order: VecDeque::new(),
                    leased: None,
                }),
                wake: tokio::sync::Notify::new(),
                capacity_state,
            }),
        }
    }

    pub fn handle(&self) -> ContextConvergenceIntakeHandle {
        ContextConvergenceIntakeHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn capacity(&self) -> NonZeroUsize {
        self.inner.capacity
    }

    pub fn pending(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("context convergence intake")
            .pending
            .len()
    }

    /// Waits until a retained observation may be available.
    ///
    /// The notification is only a pacing aid. The serial owner always drains
    /// and checks the bounded state, so cancellation or a coalesced wake cannot
    /// lose a receipt.
    pub async fn notified(&self) {
        loop {
            let notified = self.inner.wake.notified();
            if self.pending() != 0 {
                return;
            }
            notified.await;
            if self.pending() != 0 {
                return;
            }
        }
    }

    /// Leases the oldest retained receipt without removing it or releasing its
    /// capacity slot.
    ///
    /// At most one lease exists because this move-only intake is the single
    /// serial drain owner. Dropping the returned lease, including through task
    /// cancellation, keeps the receipt retained for a later turn.
    pub fn peek_retained(&mut self) -> Option<ContextConvergenceRetainedLease> {
        let (context, receipt) = {
            let mut state = self.inner.state.lock().expect("context convergence intake");
            if state.leased.is_some() {
                return None;
            }
            let context = state.order.front().copied()?;
            let receipt = state
                .pending
                .get(&context)
                .copied()
                .expect("an ordered convergence context has a retained receipt");
            state.leased = Some((context, receipt));
            (context, receipt)
        };
        Some(ContextConvergenceRetainedLease {
            inner: Arc::clone(&self.inner),
            context,
            receipt,
            settled: false,
        })
    }

    /// Applies at most `max` complete receipts in first-context order.
    ///
    /// A receipt remains retained until `apply` returns success. This closes
    /// the gap between a transport cursor advancing after `publish` and the
    /// registry durably accepting the observation: cancellation, an owner
    /// error, or a failed registry write leaves the same receipt available for
    /// the next serial turn. A concurrently published newer version also
    /// remains queued after the older version was applied.
    pub fn apply_retained<E>(
        &mut self,
        max: usize,
        mut apply: impl FnMut(QueryContextConvergenceReceipt) -> Result<(), E>,
    ) -> Result<usize, E> {
        let mut applied = 0;
        while applied < max {
            let Some(lease) = self.peek_retained() else {
                break;
            };
            apply(lease.receipt())?;
            let _ = lease.ack();
            applied += 1;
        }
        Ok(applied)
    }
}

/// The result of acknowledging a successfully applied retained receipt.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextConvergenceRetainedAck {
    /// The leased receipt was still current and its context slot was released.
    Released,
    /// A newer receipt replaced the leased version and remains retained.
    NewerRetained,
    /// The serial intake owner closed before acknowledgement.
    Closed,
}

/// A move-only lease over one complete retained convergence receipt.
///
/// The lease owns no lock and is safe to hold across an asynchronous registry
/// application. Only [`Self::ack`] may remove its receipt. Dropping it leaves
/// the receipt and its capacity slot intact.
#[derive(Debug)]
#[must_use = "dropping a convergence lease keeps its receipt retained"]
pub struct ContextConvergenceRetainedLease {
    inner: Arc<ContextConvergenceIntakeInner>,
    context: QueryContextRef,
    receipt: QueryContextConvergenceReceipt,
    settled: bool,
}

impl ContextConvergenceRetainedLease {
    pub const fn receipt(&self) -> QueryContextConvergenceReceipt {
        self.receipt
    }

    pub fn ack(mut self) -> ContextConvergenceRetainedAck {
        let (outcome, released) = {
            let mut state = self.inner.state.lock().expect("context convergence intake");
            if !state.open {
                state.leased = None;
                self.settled = true;
                return ContextConvergenceRetainedAck::Closed;
            }
            assert_eq!(
                state.leased,
                Some((self.context, self.receipt)),
                "only the exact outstanding convergence lease may acknowledge"
            );
            assert_eq!(
                state.order.front().copied(),
                Some(self.context),
                "only the serial intake owner may advance convergence order"
            );
            let retained = state.pending.get(&self.context).copied().expect(
                "an acknowledged convergence context remains retained until acknowledgement",
            );
            state.leased = None;
            if retained == self.receipt {
                state.pending.remove(&self.context);
                state.order.pop_front();
                (ContextConvergenceRetainedAck::Released, true)
            } else {
                assert!(
                    retained.version() > self.receipt.version(),
                    "publish only replaces a retained convergence receipt with a newer version"
                );
                (ContextConvergenceRetainedAck::NewerRetained, false)
            }
        };
        self.settled = true;
        if released {
            self.inner.release_capacity();
        }
        outcome
    }
}

impl Drop for ContextConvergenceRetainedLease {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let mut state = self.inner.state.lock().expect("context convergence intake");
        if state.leased == Some((self.context, self.receipt)) {
            state.leased = None;
        }
    }
}

impl Drop for ContextConvergenceIntake {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().expect("context convergence intake");
            state.open = false;
            state.pending.clear();
            state.order.clear();
            state.leased = None;
        }
        self.inner.close_capacity();
    }
}

#[cfg(test)]
mod tests {
    use novarocks_execution::task_execution::{QueryContextConvergenceVersion, QueryContextRef};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    use super::*;

    fn context(query: i64) -> QueryContextRef {
        QueryContextRef::new(
            QueryExecutionId::new(
                QueryId::new(query, 1),
                AttemptId::new(1).expect("positive attempt"),
            )
            .expect("query execution identity"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn receipt(context: QueryContextRef, version: u64) -> QueryContextConvergenceReceipt {
        QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::new(version).expect("positive version"),
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        )
    }

    #[test]
    fn coalesces_one_context_to_its_latest_complete_receipt() {
        let exact = context(1);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();

        assert_eq!(
            transport.publish(exact, receipt(exact, 1)),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
        assert_eq!(
            transport.publish(exact, receipt(exact, 3)),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
        assert_eq!(intake.pending(), 1);
        let mut applied = Vec::new();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![receipt(exact, 3)]);
        assert_eq!(intake.pending(), 0);
    }

    #[test]
    fn exact_replay_is_idempotent_and_stale_versions_fail_closed() {
        let exact = context(2);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let current = receipt(exact, 2);

        assert_eq!(
            transport.publish(exact, current),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
        let replay = transport
            .publish(exact, current)
            .expect("an exact replay is idempotent");
        assert_eq!(replay, ContextConvergencePublishAdmission::Idempotent);
        assert!(replay.authorizes_cursor_advance());
        assert_eq!(
            transport.publish(exact, receipt(exact, 1)),
            Err(ContextConvergencePublishError::StaleVersion)
        );
        let mut applied = Vec::new();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![current]);
    }

    #[test]
    fn overflow_and_context_mismatch_retain_nothing_and_authorize_no_cursor() {
        let first = context(3);
        let second = context(4);
        let stranger = context(5);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();

        let retained = transport
            .publish(first, receipt(first, 1))
            .expect("the first context owns the fixed slot");
        assert!(retained.authorizes_cursor_advance());
        assert_eq!(
            transport.publish(second, receipt(second, 1)),
            Err(ContextConvergencePublishError::Overflow)
        );
        assert_eq!(
            transport.publish(second, receipt(stranger, 1)),
            Err(ContextConvergencePublishError::ContextMismatch)
        );
        assert_eq!(intake.pending(), 1);
        let mut applied = Vec::new();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![receipt(first, 1)]);
    }

    #[test]
    fn drain_is_bounded_and_preserves_first_context_order_after_updates() {
        let first = context(6);
        let second = context(7);
        let third = context(8);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::new(3).unwrap());
        let transport = intake.handle();

        for exact in [first, second, third] {
            assert_eq!(
                transport.publish(exact, receipt(exact, 1)),
                Ok(ContextConvergencePublishAdmission::Retained)
            );
        }
        assert_eq!(
            transport.publish(first, receipt(first, 2)),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
        let mut applied = Vec::new();
        assert_eq!(
            intake.apply_retained(2, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(2)
        );
        assert_eq!(applied, vec![receipt(first, 2), receipt(second, 1)]);
        applied.clear();
        assert_eq!(
            intake.apply_retained(usize::MAX, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![receipt(third, 1)]);
    }

    #[tokio::test]
    async fn a_partial_drain_does_not_park_while_another_receipt_is_pending() {
        let first = context(10);
        let second = context(11);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::new(2).unwrap());
        let transport = intake.handle();
        let first_admission = transport
            .publish(first, receipt(first, 1))
            .expect("retain first context");
        assert!(first_admission.authorizes_cursor_advance());
        let second_admission = transport
            .publish(second, receipt(second, 1))
            .expect("retain second context");
        assert!(second_admission.authorizes_cursor_advance());

        let mut applied = Vec::new();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![receipt(first, 1)]);
        tokio::time::timeout(std::time::Duration::from_millis(50), intake.notified())
            .await
            .expect("pending state makes notification immediately observable");
        applied.clear();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                applied.push(receipt);
                Ok::<_, ()>(())
            }),
            Ok(1)
        );
        assert_eq!(applied, vec![receipt(second, 1)]);
    }

    #[tokio::test]
    async fn overflow_waiter_observes_the_next_released_slot_without_a_lost_wake() {
        let first = context(12);
        let second = context(13);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let _ = transport
            .publish(first, receipt(first, 1))
            .expect("retain the first context");
        let observed = transport.capacity_state();
        assert_eq!(observed.epoch(), 0);
        assert!(!observed.is_closed());
        assert_eq!(
            transport.publish(second, receipt(second, 1)),
            Err(ContextConvergencePublishError::Overflow)
        );

        let lease = intake.peek_retained().expect("lease the retained context");
        assert_eq!(lease.ack(), ContextConvergenceRetainedAck::Released);

        let changed = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            transport.wait_for_capacity_change(observed.epoch()),
        )
        .await
        .expect("a release racing waiter registration remains observable");
        assert_eq!(changed.epoch(), 1);
        assert!(!changed.is_closed());
        assert_eq!(
            transport.publish(second, receipt(second, 1)),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
    }

    #[test]
    fn acknowledgement_removes_the_same_version_and_releases_capacity() {
        let exact = context(14);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let retained = receipt(exact, 1);
        let _ = transport
            .publish(exact, retained)
            .expect("retain convergence receipt");

        let lease = intake.peek_retained().expect("lease retained receipt");
        assert_eq!(lease.receipt(), retained);
        assert_eq!(intake.pending(), 1);
        assert_eq!(lease.ack(), ContextConvergenceRetainedAck::Released);
        assert_eq!(intake.pending(), 0);
        assert_eq!(transport.capacity_state().epoch(), 1);
    }

    #[test]
    fn dropping_a_lease_keeps_the_receipt_and_capacity_slot() {
        let exact = context(15);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let retained = receipt(exact, 1);
        let _ = transport
            .publish(exact, retained)
            .expect("retain convergence receipt");

        let lease = intake.peek_retained().expect("lease retained receipt");
        drop(lease);
        assert_eq!(intake.pending(), 1);
        assert_eq!(transport.capacity_state().epoch(), 0);

        let retry = intake
            .peek_retained()
            .expect("a dropped lease can be acquired again");
        assert_eq!(retry.receipt(), retained);
        assert_eq!(retry.ack(), ContextConvergenceRetainedAck::Released);
    }

    #[test]
    fn acknowledgement_keeps_a_newer_version_published_while_lease_is_held() {
        let exact = context(16);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let _ = transport
            .publish(exact, receipt(exact, 1))
            .expect("retain first version");

        let lease = intake.peek_retained().expect("lease first version");
        assert_eq!(
            transport.publish(exact, receipt(exact, 2)),
            Ok(ContextConvergencePublishAdmission::Retained)
        );
        assert_eq!(lease.ack(), ContextConvergenceRetainedAck::NewerRetained);
        assert_eq!(intake.pending(), 1);
        assert_eq!(transport.capacity_state().epoch(), 0);

        let newer = intake.peek_retained().expect("lease the newer version");
        assert_eq!(newer.receipt(), receipt(exact, 2));
        assert_eq!(newer.ack(), ContextConvergenceRetainedAck::Released);
        assert_eq!(intake.pending(), 0);
        assert_eq!(transport.capacity_state().epoch(), 1);
    }

    #[tokio::test]
    async fn closing_the_intake_wakes_capacity_waiters_without_advancing_epoch() {
        let intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let observed = transport.capacity_state();
        let waiter = {
            let transport = transport.clone();
            tokio::spawn(async move { transport.wait_for_capacity_change(observed.epoch()).await })
        };
        tokio::task::yield_now().await;
        drop(intake);

        let closed = tokio::time::timeout(std::time::Duration::from_millis(50), waiter)
            .await
            .expect("closing the intake wakes capacity waiters")
            .expect("capacity waiter task succeeds");
        assert_eq!(closed.epoch(), observed.epoch());
        assert!(closed.is_closed());
    }

    #[test]
    fn failed_application_keeps_the_receipt_retained_for_retry() {
        let exact = context(17);
        let mut intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let retained = receipt(exact, 1);
        let _ = transport
            .publish(exact, retained)
            .expect("retain convergence receipt");

        assert_eq!(
            intake.apply_retained(1, |_| Err("registry unavailable")),
            Err("registry unavailable")
        );
        assert_eq!(intake.pending(), 1);

        let mut retried = Vec::new();
        assert_eq!(
            intake.apply_retained(1, |receipt| {
                retried.push(receipt);
                Ok::<_, &str>(())
            }),
            Ok(1)
        );
        assert_eq!(retried, vec![retained]);
        assert_eq!(intake.pending(), 0);
    }

    #[test]
    fn dropping_the_move_only_owner_closes_every_cloned_handle() {
        let exact = context(9);
        let intake = ContextConvergenceIntake::bounded(NonZeroUsize::MIN);
        let transport = intake.handle();
        let cloned = transport.clone();
        drop(intake);

        assert_eq!(
            transport.publish(exact, receipt(exact, 1)),
            Err(ContextConvergencePublishError::Closed)
        );
        assert_eq!(
            cloned.publish(exact, receipt(exact, 1)),
            Err(ContextConvergencePublishError::Closed)
        );
    }

    #[test]
    fn transport_handle_is_cloneable_send_and_sync() {
        fn assert_handle<T: Clone + Send + Sync>() {}
        fn assert_lease<T: Send>() {}
        assert_handle::<ContextConvergenceIntakeHandle>();
        assert_lease::<ContextConvergenceRetainedLease>();
    }
}
