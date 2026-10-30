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

//! Canonical receipt cells embedded in the original Apply reservation backing.
use super::*;
use crate::task_execution::intent::{AckPayload, OperationAcknowledgement};
use novarocks_proto_codec::lifecycle::terminal::CanonicalTerminalTelemetry;
use novarocks_workload_control::joint_admission::{
    JointClaim, JointGrantedClaims, JointLocalGrant, JointLocalOffer, NativeAsyncActionAdmission,
    NativeAsyncActionGrant, NativeDispatchFrozen, NativeDispatchSource, NativeReplyGrant,
};
use std::sync::{
    OnceLock, Weak,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) struct ReservationBacking {
    // Physical/input custody is retired independently after original F exits.
    pub(super) physical: Mutex<Option<ApplyReservations>>,
    pub(super) action: Mutex<Option<NativeAsyncActionGrant>>,
    next: AtomicUsize,
    // Actual generated/typed data exits before the original reply pin.
    acknowledgements: OnceLock<Box<[OnceLock<OperationAcknowledgement>]>>,
    reply: Mutex<Option<NativeReplyGrant>>,
    dispatch_failure: OnceLock<novarocks_workload_control::WorkError>,
    dispatch_frozen: OnceLock<NativeDispatchFrozen>,
    core: Weak<Core>,
}
impl std::fmt::Debug for ReservationBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalNativeReplyBacking")
            .field(
                "cells",
                &self.acknowledgements.get().map_or(0, |cells| cells.len()),
            )
            .finish_non_exhaustive()
    }
}
impl ReservationBacking {
    pub(super) fn new(
        core: &Arc<Core>,
        items: usize,
        reply: Option<NativeReplyGrant>,
        action: Option<NativeAsyncActionGrant>,
    ) -> Self {
        let cells = OnceLock::new();
        if items != 0 {
            let mut values = Vec::with_capacity(items);
            values.resize_with(items, OnceLock::new);
            assert!(cells.set(values.into_boxed_slice()).is_ok());
        }
        Self {
            physical: Mutex::new(None),
            action: Mutex::new(action),
            next: AtomicUsize::new(0),
            acknowledgements: cells,
            reply: Mutex::new(reply),
            dispatch_failure: OnceLock::new(),
            dispatch_frozen: OnceLock::new(),
            core: Arc::downgrade(core),
        }
    }
    fn notify_after_alias_exit(self: Arc<Self>) {
        let core = self.core.clone();
        drop(self); // Actual Arc exit precedes the original source wake.
        if let Some(core) = core.upgrade() {
            core.changed.notify_one();
        }
    }
}
impl CanonicalTerminalTelemetry for ReservationBacking {
    fn terminal_telemetry(
        &self,
        ordinal: usize,
    ) -> &novarocks_proto_models::novarocks::QueryTerminalProfileContributionTelemetry {
        let ack = self
            .acknowledgements
            .get()
            .expect("installed original reply cells")[ordinal]
            .get()
            .expect("published canonical acknowledgement");
        let AckPayload::Release {
            runtime_filter: Some(telemetry),
            ..
        } = ack.payload()
        else {
            panic!("terminal view comes from its original Release acknowledgement");
        };
        telemetry.as_proto()
    }
    fn release_alias(self: Arc<Self>) {
        self.notify_after_alias_exit();
    }
}
#[derive(Debug)]
pub(crate) struct CanonicalAcknowledgement {
    backing: Option<Arc<ReservationBacking>>,
    ordinal: usize,
}
impl CanonicalAcknowledgement {
    pub(crate) fn governance_failure(&self) -> Option<&novarocks_workload_control::WorkError> {
        self.backing.as_ref()?.dispatch_failure.get()
    }
    pub(crate) fn original(&self) -> &OperationAcknowledgement {
        self.backing
            .as_ref()
            .expect("live canonical acknowledgement")
            .acknowledgements
            .get()
            .expect("installed original reply cells")[self.ordinal]
            .get()
            .expect("published original acknowledgement")
    }
}
impl Clone for CanonicalAcknowledgement {
    fn clone(&self) -> Self {
        Self {
            backing: Some(Arc::clone(
                self.backing
                    .as_ref()
                    .expect("live canonical acknowledgement"),
            )),
            ordinal: self.ordinal,
        }
    }
}
impl Drop for CanonicalAcknowledgement {
    fn drop(&mut self) {
        if let Some(backing) = self.backing.take() {
            backing.notify_after_alias_exit();
        }
    }
}
#[derive(Clone)]
pub(crate) struct OriginalReplyPublisher {
    backing: Arc<ReservationBacking>,
}
impl OriginalReplyPublisher {
    pub(crate) fn dispatch_rejected(&self) -> bool {
        self.backing.dispatch_failure.get().is_some()
    }

    pub(crate) fn record_dispatch_failure(&self, cause: novarocks_workload_control::WorkError) {
        assert!(
            self.backing.dispatch_failure.set(cause).is_ok(),
            "one original Native dispatch gate failure"
        );
    }

    /// The same OriginalInput owns this prefix and the unpolled RPC body.
    /// Its physical/input/action retirement follows actual prefix/body exit.
    pub(crate) async fn freeze_dispatch(&self, expires_at: tokio::time::Instant) -> Result<(), ()> {
        use novarocks_workload_control::WorkError;
        let source = (|| {
            // All capability vtable calls happen before capture takes WC
            // State. No backing guard survives the first possible await.
            let action = self
                .backing
                .action
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let reply = self.backing.reply.lock().unwrap_or_else(|e| e.into_inner());
            let physical = self
                .backing
                .physical
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let physical = physical.as_ref().ok_or(WorkError::Released)?;
            NativeDispatchSource::capture(
                action.as_ref().ok_or(WorkError::Released)?,
                reply.as_ref().ok_or(WorkError::Released)?,
                physical
                    ._queue
                    .iter()
                    .map(|permit| permit.native_dispatch_input()),
            )
        })();
        let source = match source {
            Ok(source) => source,
            Err(cause) => {
                self.record_dispatch_failure(cause);
                return Err(());
            }
        };
        let mut source = std::pin::pin!(source);
        let mut deadline = std::pin::pin!(tokio::time::sleep_until(expires_at));
        std::future::poll_fn(|cx| {
            self.poll_dispatch_freeze(source.as_mut(), deadline.as_mut(), cx, |frozen| {
                assert!(self.backing.dispatch_frozen.set(frozen).is_ok());
            })
        })
        .await
    }

    fn poll_dispatch_freeze<T>(
        &self,
        mut source: Pin<
            &mut impl Future<Output = Result<T, novarocks_workload_control::WorkError>>,
        >,
        mut deadline: Pin<&mut tokio::time::Sleep>,
        cx: &mut std::task::Context<'_>,
        install_frozen: impl FnOnce(T),
    ) -> Poll<Result<(), ()>> {
        match source.as_mut().poll(cx) {
            Poll::Ready(Ok(frozen)) => {
                // Each actual verdict precedes the actual child F Drop.
                install_frozen(frozen);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(cause)) => {
                self.record_dispatch_failure(cause);
                Poll::Ready(Err(()))
            }
            Poll::Pending => match deadline.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    // Persist the no-RPC timeout before source destruction
                    // can unwind. The timer consumes the original deadline.
                    self.record_dispatch_failure(
                        novarocks_workload_control::WorkError::CapacityWaitTimeout,
                    );
                    Poll::Ready(Err(()))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }

    pub(crate) fn protocol_tail(
        &self,
    ) -> Result<
        novarocks_resource_contract::ExistingOwnerRetentionLease,
        novarocks_resource_contract::RetentionPinError,
    > {
        self.backing
            .action
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .ok_or(novarocks_resource_contract::RetentionPinError::Released)?
            .retain_protocol_tail()
    }
    pub(crate) fn canonicalize(
        &self,
        mut ack: OperationAcknowledgement,
    ) -> OperationAcknowledgement {
        let ordinal = self.backing.next.fetch_add(1, Ordering::Relaxed);
        assert!(
            ordinal
                < self
                    .backing
                    .acknowledgements
                    .get()
                    .expect("installed original reply cells")
                    .len(),
            "one canonical cell per original batch item"
        );
        let owner = Arc::clone(&self.backing) as Arc<dyn CanonicalTerminalTelemetry>;
        ack.bind_canonical_telemetry(Arc::downgrade(&owner), ordinal);
        drop(owner);
        self.backing
            .acknowledgements
            .get()
            .expect("installed original reply cells")[ordinal]
            .set(ack)
            .unwrap_or_else(|_| panic!("the original canonical receipt cell is written once"));
        let original = self
            .backing
            .acknowledgements
            .get()
            .expect("installed original reply cells")[ordinal]
            .get()
            .unwrap();
        OperationAcknowledgement::from_canonical(
            original,
            CanonicalAcknowledgement {
                backing: Some(Arc::clone(&self.backing)),
                ordinal,
            },
        )
    }
}

/// Fixed original Apply row, not another queue or registry. The same value is
/// returned to Core by NativeRowLease even when a foreign callback unwinds.
pub(super) struct OriginalNativeAcceptance {
    accepted: Option<JointLocalGrant<super::super::task_transport::NativePhysicalOffer>>,
    physical: Option<super::super::task_transport::NativePhysicalOffer>,
    claims: Option<JointGrantedClaims>,
    action_source: Option<JointClaim>,
    reply_source: Option<JointClaim>,
    action: Option<NativeAsyncActionGrant>,
    reply: Option<NativeReplyGrant>,
}
// An actual physical destructor unwind leaves the SAME original row's
// remaining claims in this opaque cause. The reaper stops at that first
// unproven exit; it must never advance to physical/action refund.
struct NativeExitUnproven {
    _cause: Box<dyn std::any::Any + Send>,
    _remaining: std::mem::ManuallyDrop<OriginalNativeAcceptance>,
}
impl Drop for OriginalNativeAcceptance {
    fn drop(&mut self) {
        let original = std::mem::replace(self, Self::new());
        let mut original = std::mem::ManuallyDrop::new(original);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(original.accepted.take());
            drop(original.physical.take());
            drop(original.claims.take());
            drop(original.action_source.take());
            drop(original.reply_source.take());
            drop(original.action.take());
            drop(original.reply.take());
        }));
        match result {
            Ok(()) => std::mem::forget(std::mem::ManuallyDrop::into_inner(original)),
            Err(cause) => std::panic::resume_unwind(Box::new(NativeExitUnproven {
                _cause: cause,
                _remaining: original,
            })),
        }
    }
}
impl OriginalNativeAcceptance {
    pub(super) fn new() -> Self {
        Self {
            accepted: None,
            physical: None,
            claims: None,
            action_source: None,
            reply_source: None,
            action: None,
            reply: None,
        }
    }
}
/// A single short publication lease. No poll, callback, or foreign destructor
/// executes under Core. Loaned remains a live original record during the call.
struct NativeRowLease<'a> {
    slot: &'a ApplySlot,
    original: Option<OriginalRecord>,
}
impl Drop for NativeRowLease<'_> {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            let mut state = self
                .slot
                .core
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let placeholder = state.records[self.slot.index].replace(original);
            drop(state);
            // The only retired value is an empty internal Loaned placeholder.
            drop(placeholder);
        }
    }
}
impl ApplySlot {
    fn native_row(&self) -> Result<NativeRowLease<'_>, novarocks_workload_control::WorkError> {
        use novarocks_workload_control::WorkError;
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        let record = state.records[self.index]
            .as_mut()
            .ok_or(WorkError::Released)?;
        if !matches!(record.state, OriginalState::Reserved) {
            return Err(WorkError::Conflict);
        }
        // Header has independent fixed process custody before any claim is
        // accepted. Variable canonical cells are not allocated here.
        if record.reservations.is_none() {
            record.reservations =
                Some(Arc::new(ReservationBacking::new(&self.core, 0, None, None)));
        }
        let original = std::mem::replace(
            record,
            OriginalRecord {
                state: OriginalState::Loaned,
                native: OriginalNativeAcceptance::new(),
                reservations: None,
            },
        );
        drop(state);
        Ok(NativeRowLease {
            slot: self,
            original: Some(original),
        })
    }
    pub(crate) fn accept_native_in(
        &self,
        offer: &mut JointLocalOffer<'_>,
        physical: &mut Option<super::super::task_transport::NativePhysicalOffer>,
    ) -> Result<
        novarocks_workload_control::WorkloadProgressNotification,
        novarocks_workload_control::WorkError,
    > {
        let mut row = self.native_row()?;
        offer.accept_in(
            physical,
            &mut row.original.as_mut().unwrap().native.accepted,
        )
    }
    pub(crate) fn shrink_native_encoding(
        &self,
        bytes: usize,
    ) -> Result<(), novarocks_workload_control::WorkError> {
        let mut row = self.native_row()?;
        row.original
            .as_mut()
            .unwrap()
            .native
            .physical
            .as_mut()
            .and_then(|physical| physical.encoding.as_mut())
            .ok_or(novarocks_workload_control::WorkError::Released)?
            .shrink_to(bytes);
        Ok(())
    }
    pub(crate) fn take_native_physical(
        &self,
    ) -> Result<
        super::super::task_transport::NativePhysicalOffer,
        novarocks_workload_control::WorkError,
    > {
        let mut row = self.native_row()?;
        row.original
            .as_mut()
            .unwrap()
            .native
            .physical
            .take()
            .ok_or(novarocks_workload_control::WorkError::Released)
    }
    pub(crate) fn prepare_native_in(
        &self,
        admission: &NativeAsyncActionAdmission,
        items: usize,
    ) -> Result<
        (
            OriginalReplyPublisher,
            novarocks_workload_control::WorkloadProgressNotification,
            novarocks_workload_control::WorkloadProgressNotification,
        ),
        novarocks_workload_control::WorkError,
    > {
        use novarocks_workload_control::WorkError;
        let mut row = self.native_row()?;
        let original = row.original.as_mut().unwrap();
        let native = &mut original.native;
        if let Some(accepted) = native.accepted.take()
            && let Err(accepted) = accepted.transfer_in(&mut native.physical, &mut native.claims)
        {
            native.accepted = Some(accepted);
            return Err(WorkError::Conflict);
        }
        let claims = native.claims.as_mut().ok_or(WorkError::Released)?;
        if native.reply_source.is_none() && native.reply.is_none() {
            if claims.len() != 2 {
                return Err(WorkError::Conflict);
            }
            native.reply_source = claims.pop();
            native.action_source = claims.pop();
        }
        let action_notice =
            admission.adopt_action_in(&mut native.action_source, &mut native.action)?;
        let reply_notice = admission.adopt_reply_in(&mut native.reply_source, &mut native.reply)?;
        if native
            .reply
            .as_ref()
            .ok_or(WorkError::Released)?
            .reply_records()?
            != items as u64
        {
            return Err(WorkError::Conflict);
        }
        let backing = original.reservations.as_ref().ok_or(WorkError::Released)?;
        // Stable paid pins precede the first variable cell allocation.
        *backing.action.lock().unwrap_or_else(|e| e.into_inner()) = native.action.take();
        *backing.reply.lock().unwrap_or_else(|e| e.into_inner()) = native.reply.take();
        let mut cells = Vec::with_capacity(items);
        cells.resize_with(items, OnceLock::new);
        if backing
            .acknowledgements
            .set(cells.into_boxed_slice())
            .is_err()
        {
            return Err(WorkError::Conflict);
        }
        let publisher = OriginalReplyPublisher {
            backing: Arc::clone(backing),
        };
        if native.physical.is_none() {
            return Err(WorkError::Released);
        }
        Ok((publisher, action_notice, reply_notice))
    }
}

/// Only the canonical cell producer for domain component fixtures. It grants
/// no dispatch capability, Worker verdict, physical permit or context fence.
#[cfg(test)]
pub(crate) fn canonical_dispatch_rejection_fixture(
    operation_id: novarocks_execution::task_execution::TaskOperationId,
    kind: novarocks_execution::task_execution::OperationKind,
    cause: novarocks_workload_control::WorkError,
) -> OperationAcknowledgement {
    let cells = OnceLock::new();
    assert!(cells.set(vec![OnceLock::new()].into_boxed_slice()).is_ok());
    let publisher = OriginalReplyPublisher {
        backing: Arc::new(ReservationBacking {
            physical: Mutex::new(None),
            action: Mutex::new(None),
            next: AtomicUsize::new(0),
            acknowledgements: cells,
            reply: Mutex::new(None),
            dispatch_failure: OnceLock::new(),
            dispatch_frozen: OnceLock::new(),
            core: Weak::new(),
        }),
    };
    publisher.record_dispatch_failure(cause);
    publisher.canonicalize(OperationAcknowledgement::from_dispatch_result(
        operation_id,
        kind,
        crate::task_execution::operation::OperationDispatchResult::NonWorkerRejected,
        AckPayload::None,
    ))
}

#[cfg(test)]
mod dispatch_gate_tests {
    use super::*;
    use novarocks_workload_control::WorkError;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Waker};

    struct PendingDispatchDropGuard {
        backing: Arc<ReservationBacking>,
        observed: Arc<AtomicBool>,
    }
    impl Future for PendingDispatchDropGuard {
        type Output = Result<(), WorkError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for PendingDispatchDropGuard {
        fn drop(&mut self) {
            assert_eq!(
                self.backing.dispatch_failure.get(),
                Some(&WorkError::CapacityWaitTimeout),
                "the original no-RPC timeout must be installed before child Drop",
            );
            self.observed.store(true, Ordering::SeqCst);
            panic!("actual pending gate child Drop failed after observing its timeout");
        }
    }

    #[tokio::test]
    async fn actual_gate_timer_installs_timeout_before_pending_child_drop_panics() {
        // This tests the production polling cut and canonical backing. The
        // child is a Drop guard, not a fabricated WC Source panic capability.
        let (owner, port) =
            ApplySendOwner::new(Handle::current(), TransportBudget::DEFAULT).unwrap();
        let publisher = OriginalReplyPublisher {
            backing: Arc::new(ReservationBacking::new(&owner.core, 0, None, None)),
        };
        let observed = Arc::new(AtomicBool::new(false));
        let mut source = Box::pin(PendingDispatchDropGuard {
            backing: Arc::clone(&publisher.backing),
            observed: Arc::clone(&observed),
        });
        let mut deadline = std::pin::pin!(tokio::time::sleep_until(tokio::time::Instant::now()));
        // Await this actual zero-delay timer once, so this component does not
        // depend on timer-driver scheduling or add a duration to the deadline.
        deadline.as_mut().await;
        let result = publisher.poll_dispatch_freeze(
            source.as_mut(),
            deadline.as_mut(),
            &mut Context::from_waker(Waker::noop()),
            |()| panic!("a pending child cannot install a frozen result"),
        );
        assert!(matches!(result, Poll::Ready(Err(()))));
        let actual_drop = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(source)));
        assert!(actual_drop.is_err());
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(
            publisher.backing.dispatch_failure.get(),
            Some(&WorkError::CapacityWaitTimeout),
        );
        assert!(publisher.dispatch_rejected());
        assert!(publisher.backing.dispatch_frozen.get().is_none());
        drop((publisher, port, owner));
    }
}
