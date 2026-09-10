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

#[derive(Debug)]
struct ContextConvergenceIntakeState {
    open: bool,
    pending: BTreeMap<QueryContextRef, QueryContextConvergenceReceipt>,
    order: VecDeque<QueryContextRef>,
}

#[derive(Debug)]
struct ContextConvergenceIntakeInner {
    capacity: NonZeroUsize,
    state: Mutex<ContextConvergenceIntakeState>,
    wake: tokio::sync::Notify,
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
        Self {
            inner: Arc::new(ContextConvergenceIntakeInner {
                capacity,
                state: Mutex::new(ContextConvergenceIntakeState {
                    open: true,
                    pending: BTreeMap::new(),
                    order: VecDeque::new(),
                }),
                wake: tokio::sync::Notify::new(),
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
            let next = {
                let state = self.inner.state.lock().expect("context convergence intake");
                let Some(context) = state.order.front().copied() else {
                    break;
                };
                let receipt = state
                    .pending
                    .get(&context)
                    .copied()
                    .expect("an ordered convergence context has a retained receipt");
                (context, receipt)
            };

            apply(next.1)?;

            let mut state = self.inner.state.lock().expect("context convergence intake");
            assert_eq!(
                state.order.front().copied(),
                Some(next.0),
                "only the serial intake owner may advance convergence order"
            );
            let retained =
                state.pending.get(&next.0).copied().expect(
                    "an applied convergence context remains retained until acknowledgement",
                );
            if retained == next.1 {
                state.pending.remove(&next.0);
                state.order.pop_front();
            } else {
                assert!(
                    retained.version() > next.1.version(),
                    "publish only replaces a retained convergence receipt with a newer version"
                );
            }
            applied += 1;
        }
        Ok(applied)
    }
}

impl Drop for ContextConvergenceIntake {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().expect("context convergence intake");
        state.open = false;
        state.pending.clear();
        state.order.clear();
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

    #[test]
    fn failed_application_keeps_the_receipt_retained_for_retry() {
        let exact = context(12);
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
        assert_handle::<ContextConvergenceIntakeHandle>();
    }
}
