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

//! Explicit custody in the original ledger, independent of consumer exit.
//!
//! Private substrate only: a Frontend receiver must separately keep its exact
//! cohort/proof and this unique capability in a finite, funded stable location.

use super::*;
use crate::WorkloadProgressNotification;

/// Drop never releases a retained responsibility. This is nevertheless the
/// sole positive settlement capability; no numeric-ID lookup can recreate it.
/// Its original receiving location and Inner weak lifetime require custody.
#[must_use = "Retained responsibility requires explicit evidence-based settlement"]
pub(crate) struct OwnerRetainedClaimReceipt {
    inner: Weak<Inner>,
    work: WorkId,
    claim: u64,
    structure_revision: u64,
    release_revision: u64,
}

/// Both slots belong to a stable receiver before this call. Refusal leaves
/// them untouched and invokes no callbacks. Successful handoff installs the
/// receipt and disarms the original guard under the same original State lock.
/// No callback runs here; notify the returned progress after all receiver locks exit.
pub(crate) fn retain_claim_in(
    owner: &WorkOwner,
    source: &mut Option<JointClaim>,
    target: &mut Option<OwnerRetainedClaimReceipt>,
) -> Result<WorkloadProgressNotification, WorkError> {
    if target.is_some() {
        return Err(WorkError::Conflict);
    }
    let original = source.as_mut().ok_or(WorkError::Released)?;
    let claim = original.id.ok_or(WorkError::Released)?;
    let scope = owner.scope();
    if !Arc::ptr_eq(&scope.inner, &original.inner) {
        return Err(WorkError::ForeignAuthority);
    }
    let inner = Arc::clone(&original.inner);
    let retired = {
        let mut state = inner.state.lock().unwrap();
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        // Retention preserves an existing charge even after cancellation. It
        // supplies no permission to dispatch new work or bypass the stop gate.
        if node.owner != crate::OwnerState::Active || node.completed {
            return Err(WorkError::Conflict);
        }
        let record = state
            .joint
            .as_mut()
            .ok_or(WorkError::NotReady)?
            .claims
            .get_mut(&claim)
            .ok_or(WorkError::Released)?;
        if !record.custody.is_raw()
            || !record.active_facts.key.source_kind.is_empty()
            || record.owner != ClaimOwner::PhysicalWork(scope.id)
            || record.pending.is_some()
            || record.amounts != record.active_amounts
            || record.facts.key != record.active_facts.key
        {
            return Err(WorkError::Conflict);
        }
        let structure_revision = record
            .structure_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let receipt = OwnerRetainedClaimReceipt {
            inner: Arc::downgrade(&inner),
            work: scope.id,
            claim,
            structure_revision,
            release_revision: record.release_revision,
        };
        record.owner = ClaimOwner::OwnerRetained(scope.id);
        record.structure_revision = structure_revision;
        *target = Some(receipt);
        original.id = None;
        let retired = source.take();
        state.advance_progress_revision();
        retired
    };
    // This guard is inert; its Inner alias leaves only after State unlocks.
    drop(retired);
    Ok(WorkloadProgressNotification::new(inner))
}

/// The receipt itself supplies the unique settlement capability. Cancellation,
/// logical completion, closure and driver exit do not prevent real release.
/// The domain receiver validates and stores its original evidence first; WC
/// validates only this exact original capacity record and consumes its slot.
/// Carry the returned progress out of every receiver lock before notifying.
pub(crate) fn settle_retained_claim_in(
    slot: &mut Option<OwnerRetainedClaimReceipt>,
) -> Result<WorkloadProgressNotification, WorkError> {
    let receipt = slot.as_ref().ok_or(WorkError::Released)?;
    let inner = receipt.inner.upgrade().ok_or(WorkError::Released)?;
    let retired = {
        let mut state = inner.state.lock().unwrap();
        let record = state
            .joint
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .claims
            .get(&receipt.claim)
            .ok_or(WorkError::Released)?;
        if !record.custody.is_raw()
            || !record.active_facts.key.source_kind.is_empty()
            || record.owner != ClaimOwner::OwnerRetained(receipt.work)
            || record.structure_revision != receipt.structure_revision
            || record.release_revision != receipt.release_revision
            || record.pending.is_some()
        {
            return Err(WorkError::Conflict);
        }
        let node = state.nodes.get(&receipt.work).ok_or(WorkError::Conflict)?;
        if node.resource_holders == 0 {
            return Err(WorkError::Conflict);
        }
        remove_claim(&mut state, receipt.claim);
        let retired = slot.take();
        state.advance_progress_revision();
        retired
    };
    drop(retired);
    Ok(WorkloadProgressNotification::new(inner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joint_admission::tests::{
        adjustment, bytes, control, dependency, drive, granted, held, request, roomy_control,
    };
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn exact_original_charge_outlives_completion_until_unique_settlement() {
        let (control, handle, mut driver) = roomy_control();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = work.owner.scope();
        let a = dependency(&handle, "a");
        let b = dependency(&handle, "b");
        let mut initial = request(&handle, &scope, &[&a, &b], JointLane::Foreground, 10);
        let mut peer = request(&handle, &scope, &[&a], JointLane::Foreground, 7);
        drive(&mut driver);
        let mut source = granted(&mut initial).pop();
        let peer = granted(&mut peer).pop().unwrap();
        let id = source.as_ref().unwrap().id.unwrap();
        let old_view = source.as_ref().unwrap().view().unwrap();
        let (facts, revision, release) = {
            let state = control.inner.state.lock().unwrap();
            let record = &state.joint.as_ref().unwrap().claims[&id];
            assert_eq!(record.facts.dependencies.len(), 2);
            assert_eq!(state.nodes[&scope.id()].resource_holders, 2);
            (
                Arc::clone(&record.facts),
                record.structure_revision,
                record.release_revision,
            )
        };
        let mut receipt = None;
        retain_claim_in(&work.owner, &mut source, &mut receipt)
            .unwrap()
            .notify();
        assert!(source.is_none());
        assert_eq!(held(&control), (2, 17));
        {
            let state = control.inner.state.lock().unwrap();
            let record = &state.joint.as_ref().unwrap().claims[&id];
            assert!(Arc::ptr_eq(&facts, &record.facts));
            assert!(Arc::ptr_eq(&facts, &record.active_facts));
            assert_eq!(record.owner, ClaimOwner::OwnerRetained(scope.id()));
            assert_eq!(record.structure_revision, revision + 1);
            assert_eq!(record.release_revision, release);
            assert_eq!(state.nodes[&scope.id()].resource_holders, 2);
        }
        assert!(matches!(
            handle.request(
                &scope,
                vec![JointClaimChange::grow_by(old_view, bytes(1), vec![])]
            ),
            Err(JointAdmissionFailure {
                error: WorkError::Conflict
            })
        ));
        work.business.release();
        work.owner.complete();
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id())
        );
        settle_retained_claim_in(&mut receipt).unwrap().notify();
        assert!(receipt.is_none());
        assert_eq!(held(&control), (1, 7));
        assert_eq!(peer.amounts().unwrap(), bytes(7));
        assert!(matches!(
            settle_retained_claim_in(&mut receipt),
            Err(WorkError::Released)
        ));
        drop(peer);
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id())
        );
    }

    #[test]
    fn adjustment_in_every_phase_refuses_handoff_without_mutation() {
        for phase in 0..3 {
            let (control, handle, mut driver) = roomy_control();
            let work = control
                .try_begin_root(WorkRequest::new(WorkClass::Management))
                .unwrap();
            let scope = work.owner.scope();
            let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
            drive(&mut driver);
            let mut source = granted(&mut initial).pop();
            let mut pending = handle
                .request(
                    &scope,
                    vec![JointClaimChange::grow_by(
                        source.as_ref().unwrap().view().unwrap(),
                        bytes(4),
                        vec![],
                    )],
                )
                .unwrap();
            if phase > 0 {
                drive(&mut driver);
            }
            let delivered = (phase == 2).then(|| adjustment(&mut pending));
            let before = held(&control);
            let revision = control.progress_revision();
            let mut receipt = None;
            assert!(matches!(
                retain_claim_in(&work.owner, &mut source, &mut receipt),
                Err(WorkError::Conflict)
            ));
            assert!(source.is_some() && receipt.is_none());
            assert_eq!(held(&control), before);
            assert_eq!(control.progress_revision(), revision);
            drop(delivered);
            drop(pending);
            drive(&mut driver);
            retain_claim_in(&work.owner, &mut source, &mut receipt)
                .unwrap()
                .notify();
            assert_eq!(held(&control), (1, 10));
            settle_retained_claim_in(&mut receipt).unwrap().notify();
            work.business.release();
            work.owner.complete();
        }
    }

    #[test]
    fn wrong_owner_full_slot_and_revision_overflow_preserve_original_guards() {
        let (control, handle, mut driver) = roomy_control();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let other = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let (foreign_control, _, _) = self::control();
        let foreign = foreign_control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = work.owner.scope();
        let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut source = granted(&mut initial).pop();
        let id = source.as_ref().unwrap().id.unwrap();
        let mut receipt = None;
        let revision = control.progress_revision();
        assert!(matches!(
            retain_claim_in(&other.owner, &mut source, &mut receipt),
            Err(WorkError::Conflict)
        ));
        assert!(matches!(
            retain_claim_in(&foreign.owner, &mut source, &mut receipt),
            Err(WorkError::ForeignAuthority)
        ));
        assert_eq!(control.progress_revision(), revision);
        let original_revision = {
            let mut state = control.inner.state.lock().unwrap();
            let record = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
            let old = record.structure_revision;
            record.structure_revision = u64::MAX;
            old
        };
        assert!(matches!(
            retain_claim_in(&work.owner, &mut source, &mut receipt),
            Err(WorkError::ArithmeticOverflow)
        ));
        assert_eq!(control.progress_revision(), revision);
        assert!(source.is_some() && receipt.is_none());
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_mut()
            .unwrap()
            .claims
            .get_mut(&id)
            .unwrap()
            .structure_revision = original_revision;
        retain_claim_in(&work.owner, &mut source, &mut receipt)
            .unwrap()
            .notify();
        let mut another = request(&handle, &scope, &[], JointLane::Foreground, 3);
        drive(&mut driver);
        let mut another = granted(&mut another).pop();
        assert!(matches!(
            retain_claim_in(&work.owner, &mut another, &mut receipt),
            Err(WorkError::Conflict)
        ));
        assert_eq!(another.as_ref().unwrap().amounts().unwrap(), bytes(3));
        settle_retained_claim_in(&mut receipt).unwrap().notify();
        drop(another);
        for work in [work, other, foreign] {
            work.business.release();
            work.owner.complete();
        }
    }

    struct PanicWake(Weak<Inner>);
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            assert!(
                self.0.upgrade().unwrap().state.try_lock().is_ok(),
                "callbacks must run outside State"
            );
            panic!("intentional retained-claim progress callback failure");
        }
    }

    #[test]
    fn callback_panics_leave_each_committed_handoff_in_its_stable_slot() {
        let (control, handle, mut driver) = roomy_control();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = work.owner.scope();
        let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut source = granted(&mut initial).pop();
        let mut receipt = None;
        let waker = Waker::from(Arc::new(PanicWake(Arc::downgrade(&control.inner))));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                retain_claim_in(&work.owner, &mut source, &mut receipt)
                    .unwrap()
                    .notify()
            }))
            .is_err()
        );
        drop(waiter);
        assert!(source.is_none() && receipt.is_some());
        assert_eq!(held(&control), (1, 10));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                settle_retained_claim_in(&mut receipt).unwrap().notify()
            }))
            .is_err()
        );
        drop(waiter);
        assert!(receipt.is_none());
        assert_eq!(held(&control), (0, 0));
        work.business.release();
        work.owner.complete();
    }

    #[test]
    fn consumer_drop_never_settles_and_weak_receipt_never_keeps_realm_alive() {
        let mut receipt = {
            let (control, handle, mut driver) = roomy_control();
            let work = control
                .try_begin_root(WorkRequest::new(WorkClass::Management))
                .unwrap();
            let scope = work.owner.scope();
            let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
            drive(&mut driver);
            let mut source = granted(&mut initial).pop();
            let mut lost = None;
            retain_claim_in(&work.owner, &mut source, &mut lost)
                .unwrap()
                .notify();
            drop(lost);
            assert_eq!(held(&control), (1, 10));
            let mut next = request(&handle, &scope, &[], JointLane::Foreground, 3);
            drive(&mut driver);
            let mut source = granted(&mut next).pop();
            let mut receipt = None;
            retain_claim_in(&work.owner, &mut source, &mut receipt)
                .unwrap()
                .notify();
            work.business.release();
            work.owner.complete();
            assert_eq!(held(&control), (2, 13));
            receipt
        };
        assert!(receipt.as_ref().unwrap().inner.upgrade().is_none());
        assert!(matches!(
            settle_retained_claim_in(&mut receipt),
            Err(WorkError::Released)
        ));
        assert!(receipt.is_some());
    }

    #[test]
    fn cancellation_closure_and_orphan_exit_do_not_create_completion_evidence() {
        let (control, handle, mut driver) = roomy_control();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = work.owner.scope();
        let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut source = granted(&mut initial).pop();
        work.owner.cancel(crate::CancellationReason::Requested);
        control.close_admission();
        let mut receipt = None;
        retain_claim_in(&work.owner, &mut source, &mut receipt)
            .unwrap()
            .notify();
        work.business.release();
        drop(work.owner);
        drop(driver);
        settle_retained_claim_in(&mut receipt).unwrap().notify();
        {
            let state = control.inner.state.lock().unwrap();
            let node = &state.nodes[&scope.id()];
            assert_eq!(node.owner, crate::OwnerState::Orphaned);
            assert!(!node.completed);
            assert_eq!(node.resource_holders, 0);
        }
        control
            .adopt(scope.id())
            .unwrap()
            .complete_after_terminal_cancel_settled();
        assert!(
            !control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&scope.id())
        );
    }

    #[test]
    fn stable_receiver_unlocks_before_callbacks_and_notification_drop_is_not_release() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        struct Slots {
            source: Option<JointClaim>,
            receipt: Option<OwnerRetainedClaimReceipt>,
        }
        struct ReceiverWake {
            slots: Weak<Mutex<Slots>>,
            inner: Weak<Inner>,
            calls: Arc<AtomicUsize>,
        }
        impl std::task::Wake for ReceiverWake {
            fn wake(self: Arc<Self>) {
                assert!(
                    self.slots.upgrade().unwrap().try_lock().is_ok(),
                    "the stable receiver must be unlocked before notification"
                );
                assert!(self.inner.upgrade().unwrap().state.try_lock().is_ok());
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (control, handle, mut driver) = roomy_control();
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = work.owner.scope();
        let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 10);
        drive(&mut driver);
        let slots = Arc::new(Mutex::new(Slots {
            source: granted(&mut initial).pop(),
            receipt: None,
        }));
        let calls = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(ReceiverWake {
            slots: Arc::downgrade(&slots),
            inner: Arc::downgrade(&control.inner),
            calls: Arc::clone(&calls),
        }));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let progress = {
            let mut guard = slots.lock().unwrap();
            let Slots { source, receipt } = &mut *guard;
            let progress = retain_claim_in(&work.owner, source, receipt).unwrap();
            assert!(source.is_none() && receipt.is_some());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            progress
        };
        progress.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(held(&control), (1, 10));
        drop(waiter);
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let progress = {
            let mut guard = slots.lock().unwrap();
            let progress = settle_retained_claim_in(&mut guard.receipt).unwrap();
            assert!(guard.receipt.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            progress
        };
        progress.notify();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(held(&control), (0, 0));
        drop(waiter);

        let mut another = request(&handle, &scope, &[], JointLane::Foreground, 3);
        drive(&mut driver);
        let progress = {
            let mut guard = slots.lock().unwrap();
            guard.source = granted(&mut another).pop();
            let Slots { source, receipt } = &mut *guard;
            retain_claim_in(&work.owner, source, receipt).unwrap()
        };
        drop(progress);
        assert_eq!(
            held(&control),
            (1, 3),
            "dropping a notification cannot release its committed charge"
        );
        let progress = settle_retained_claim_in(&mut slots.lock().unwrap().receipt).unwrap();
        drop(progress);
        assert_eq!(held(&control), (0, 0));
        work.business.release();
        work.owner.complete();
    }

    #[test]
    fn retained_full_domains_survive_owner_completion_until_explicit_settlement() {
        let (control, handle, mut driver) = control();
        let producing = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = producing.owner.scope();
        let a = dependency(&handle, "a");
        let b = dependency(&handle, "b");
        let c = dependency(&handle, "c");
        // A two-domain charge of two cannot fit this fixture's protected
        // progress cut. Start with the admissible complete one-unit vector.
        let mut initial = request(&handle, &scope, &[&a, &b], JointLane::Foreground, 1);
        drive(&mut driver);
        let mut source = granted(&mut initial).pop();
        let mut receipt = None;
        retain_claim_in(&producing.owner, &mut source, &mut receipt)
            .unwrap()
            .notify();
        producing.business.release();
        producing.owner.complete();
        let consuming = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let consumer = consuming.owner.scope();
        let mut blocked = request(&handle, &consumer, &[&a], JointLane::Foreground, 2);
        let mut healthy = request(&handle, &consumer, &[&c], JointLane::Foreground, 1);
        let mut settlement = request(&handle, &consumer, &[&a], JointLane::Settlement, 1);
        drive(&mut driver);
        let released = granted(&mut blocked);
        let healthy = granted(&mut healthy);
        let settlement = granted(&mut settlement);
        assert_eq!(held(&control), (4, 5));
        drop(healthy);
        settle_retained_claim_in(&mut receipt).unwrap().notify();
        drive(&mut driver);
        assert_eq!(held(&control), (2, 3));
        drop((released, settlement));
        consuming.business.release();
        consuming.owner.complete();
        assert_eq!(held(&control), (0, 0));
    }
}
