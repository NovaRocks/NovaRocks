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

//! Original paid builder/proof/claim behavior; no production profile claim.
use super::*;
use crate::{ControlStorageConfig, WorkloadConfig};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let (control, handle, driver, binding) = fixture_with_optional_closed_factory(None);
    assert!(binding.is_none());
    (control, handle, driver)
}
fn fixture_with_optional_closed_factory(
    descriptor: Option<InternalResultClosedFactoryDescriptor>,
) -> (
    WorkloadControl,
    JointAdmissionHandle,
    JointProgressDriver,
    Option<InternalCpuFactoryBinding>,
) {
    let count = 8;
    let storage = ControlStorageConfig::new(count, &[]);
    let config = ResponsibilityConfig {
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let binding = descriptor.map(|d| {
        control
            .bind_internal_result_closed_factory_for_composition(d)
            .unwrap()
    });
    let (handle, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    control
        .configure_result_capacity(crate::ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver, binding)
}
fn drive(driver: &mut JointProgressDriver) {
    crate::joint_admission::tests::drive(driver);
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("expected original paid reply"),
    }
}
fn fact(name: &str) -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn bind(scope: &WorkScope, facts: &[SharedDependency], driver: &mut JointProgressDriver) {
    let mut proposal = scope.begin_control_binding().unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut reply = receive(&mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(scope)
        .unwrap()
        .notify();
    drop(reply);
    drive(driver);
}
fn amounts(resource: JointResource, amount: u64) -> JointResources {
    JointResources::ZERO
        .with(JointResource::ClaimRecords, 1)
        .with(JointResource::MetadataBytes, 0)
        .with(resource, amount)
}
fn request(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
) -> JointAdmission {
    let mut proposal = handle.begin_paid_proposal(work).unwrap();
    proposal.begin_new(lane, amount).unwrap();
    for fact in facts {
        proposal.push_dependency(fact).unwrap();
    }
    proposal.end_change().unwrap();
    proposal.submit().unwrap()
}
fn claim(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    lane: JointLane,
    amount: JointResources,
    driver: &mut JointProgressDriver,
) -> JointClaim {
    let mut pending = request(handle, work, facts, lane, amount);
    drive(driver);
    let mut reply = receive(&mut pending);
    let result = reply.new_claims.pop().unwrap();
    drop(reply);
    drive(driver);
    result
}
fn held(control: &WorkloadControl, resource: JointResource) -> u64 {
    control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .values()
        .map(|row| row.active_amounts.get(resource))
        .sum()
}
fn finish(work: crate::RootWork, driver: &mut JointProgressDriver) {
    work.business.release();
    work.owner.complete();
    drive(driver);
}

#[test]
fn adoption_seals_old_public_view_without_changing_the_original_paid_charge() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("sealed")];
    bind(&scope, &facts, &mut driver);
    let mut source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let old = source.as_ref().unwrap().view().unwrap();
    let before = held(&control, JointResource::MetadataBytes);
    let mut target = None;
    let notification = RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target).unwrap();
    assert!(source.is_none() && target.is_some());
    assert_eq!(held(&control, JointResource::MetadataBytes), before);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    assert_eq!(
        proposal.begin_grow(old, JointResources::ZERO),
        Err(WorkError::Conflict)
    );
    drop(proposal);
    drop(notification);
    drop(target);
    finish(work, &mut driver);
}

#[test]
fn source_adjustment_refuses_adoption_in_waiting_granted_and_delivered_phases() {
    for phase in 0..3 {
        let (control, handle, mut driver) = fixture();
        let work = root(&control);
        let scope = work.owner.scope();
        let facts = [fact("pending")];
        bind(&scope, &facts, &mut driver);
        let mut source = Some(claim(
            &handle,
            &work.owner,
            &facts,
            JointLane::Custody,
            amounts(JointResource::PayloadBytes, 8),
            &mut driver,
        ));
        let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
        proposal
            .begin_grow(
                source.as_ref().unwrap().view().unwrap(),
                JointResources::ZERO.with(JointResource::PayloadBytes, 1),
            )
            .unwrap();
        proposal.end_change().unwrap();
        let mut pending = proposal.submit().unwrap();
        if phase > 0 {
            drive(&mut driver);
        }
        let delivered = (phase == 2).then(|| receive(&mut pending));
        let mut target = None;
        assert!(matches!(
            RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target),
            Err(WorkError::Conflict)
        ));
        assert!(source.is_some() && target.is_none());
        drop(delivered);
        drop(pending);
        drive(&mut driver);
        RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target)
            .unwrap()
            .notify();
        assert_eq!(
            target
                .as_ref()
                .unwrap()
                .retain_alias()
                .unwrap()
                .capacity_bytes()
                .unwrap(),
            8
        );
        drop(target);
        finish(work, &mut driver);
    }
}

#[test]
fn foreign_scope_cancel_and_close_refusals_leave_custody_unchanged() {
    for case in 0..4 {
        let (control, handle, mut driver) = fixture();
        let (foreign, _, mut foreign_driver) = fixture();
        let work = root(&control);
        let other = root(&control);
        let stranger = root(&foreign);
        let scope = work.owner.scope();
        let facts = [fact("owner")];
        bind(&scope, &facts, &mut driver);
        let mut source = Some(claim(
            &handle,
            &work.owner,
            &facts,
            JointLane::Custody,
            amounts(JointResource::PayloadBytes, 8),
            &mut driver,
        ));
        let mut target = None;
        if case == 0 {
            assert!(matches!(
                RetainedPayloadGrant::adopt_in(&stranger.owner.scope(), &mut source, &mut target),
                Err(WorkError::ForeignAuthority)
            ));
        }
        if case == 1 {
            assert!(
                RetainedPayloadGrant::adopt_in(&other.owner.scope(), &mut source, &mut target)
                    .is_err()
            );
        }
        if case == 2 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
            assert!(RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target).is_err());
        }
        if case == 3 {
            control.close_admission();
            assert!(matches!(
                RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target),
                Err(WorkError::Closed)
            ));
        }
        assert!(source.is_some() && target.is_none());
        drop(source);
        finish(work, &mut driver);
        finish(other, &mut driver);
        finish(stranger, &mut foreign_driver);
    }
}

#[test]
fn notification_panic_observes_stable_target_and_preserves_original_charge() {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Mutex, Weak,
            atomic::{AtomicBool, Ordering},
        },
        task::Wake,
    };
    #[derive(Default)]
    struct Slots {
        source: Option<JointClaim>,
        payload: Option<RetainedPayloadGrant>,
    }
    struct Observe {
        slots: Weak<Mutex<Slots>>,
        inner: Weak<Inner>,
        panic_once: AtomicBool,
    }
    impl Wake for Observe {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let slots = self.slots.upgrade().unwrap();
            let slots = slots.try_lock().unwrap();
            assert!(slots.source.is_none() && slots.payload.is_some());
            drop(slots);
            assert!(self.inner.upgrade().unwrap().state.try_lock().is_ok());
            if self.panic_once.swap(false, Ordering::SeqCst) {
                panic!("intentional adopted-target wake panic");
            }
        }
    }
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("wake")];
    bind(&scope, &facts, &mut driver);
    let slots = Arc::new(Mutex::new(Slots {
        source: Some(claim(
            &handle,
            &work.owner,
            &facts,
            JointLane::Custody,
            amounts(JointResource::PayloadBytes, 8),
            &mut driver,
        )),
        payload: None,
    }));
    let wake = Arc::new(Observe {
        slots: Arc::downgrade(&slots),
        inner: Arc::downgrade(&control.inner),
        panic_once: AtomicBool::new(true),
    });
    let waker = Waker::from(wake);
    let marker = control.progress_revision();
    let mut waiting = Box::pin(control.wait_progress(marker));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let progress = {
        let mut locked = slots.lock().unwrap();
        let Slots { source, payload } = &mut *locked;
        RetainedPayloadGrant::adopt_in(&scope, source, payload).unwrap()
    };
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    assert!(catch_unwind(AssertUnwindSafe(|| progress.notify())).is_err());
    assert!(slots.lock().unwrap().source.is_none());
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(waiting);
    drop(slots);
    finish(work, &mut driver);
}

#[test]
fn non_cancelled_orphan_and_revision_overflow_cannot_become_new_typed_authority() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("orphan")];
    bind(&scope, &facts, &mut driver);
    let mut source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let id = source.as_ref().unwrap().id.unwrap();
    let mut target = None;
    let old_revision = {
        let mut state = control.inner.state.lock().unwrap();
        let row = state.joint.as_mut().unwrap().claims.get_mut(&id).unwrap();
        let old = row.structure_revision;
        row.structure_revision = u64::MAX;
        old
    };
    assert!(matches!(
        RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target),
        Err(WorkError::ArithmeticOverflow)
    ));
    assert!(source.is_some() && target.is_none());
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
        .structure_revision = old_revision;
    work.owner.success_sealer().seal().unwrap();
    drop(work.owner);
    assert!(
        scope.check().is_ok(),
        "the independent Active-owner check is necessary"
    );
    assert!(matches!(
        RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target),
        Err(WorkError::Conflict)
    ));
    assert!(source.is_some() && target.is_none());
    drop(source);
    work.business.release();
    control.adopt(scope.id()).unwrap().complete();
    drive(&mut driver);
}

#[test]
fn occupied_target_and_mixed_computation_refusals_preserve_both_originals() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("occupied")];
    bind(&scope, &facts, &mut driver);
    let mut first = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut first, &mut payload)
        .unwrap()
        .notify();
    let original = payload.as_ref().unwrap().retain_alias().unwrap();
    let mut second = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    assert!(matches!(
        RetainedPayloadGrant::adopt_in(&scope, &mut second, &mut payload),
        Err(WorkError::Conflict)
    ));
    assert!(second.is_some());
    assert!(original.shares_payload_with(&payload.as_ref().unwrap().retain_alias().unwrap()));
    let mixed = amounts(JointResource::Computations, 1).with(JointResource::PayloadBytes, 8);
    let mut computation_source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Foreground,
        mixed,
        &mut driver,
    ));
    let mut computation = None;
    assert!(matches!(
        InternalComputationGrant::adopt_in(&scope, &mut computation_source, &mut computation),
        Err(WorkError::Conflict)
    ));
    assert!(computation_source.is_some() && computation.is_none());
    drop(computation_source);
    drop(second);
    drop(payload);
    drop(original);
    finish(work, &mut driver);
}

#[test]
fn checked_pin_overflow_keeps_original_alias_and_paid_record_unchanged() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("overflow")];
    bind(&scope, &facts, &mut driver);
    let mut source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut target = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target)
        .unwrap()
        .notify();
    let alias = target.as_ref().unwrap().retain_alias().unwrap();
    let key = alias.pin.key.unwrap();
    {
        let mut state = control.inner.state.lock().unwrap();
        let row = state
            .joint
            .as_mut()
            .unwrap()
            .claims
            .get_mut(&key[0])
            .unwrap();
        let pins = Kind::Payload.pins(&mut row.custody, key[1]).unwrap();
        assert_eq!(*pins, 2);
        *pins = usize::MAX;
    }
    assert!(matches!(
        alias.try_clone(),
        Err(RetentionPinError::Overflow)
    ));
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    {
        let mut state = control.inner.state.lock().unwrap();
        let row = state
            .joint
            .as_mut()
            .unwrap()
            .claims
            .get_mut(&key[0])
            .unwrap();
        let pins = Kind::Payload.pins(&mut row.custody, key[1]).unwrap();
        assert_eq!(*pins, usize::MAX);
        *pins = 2;
    }
    let before_owner_aliases = Arc::strong_count(&control.inner);
    let lease = alias.into_retention();
    assert_eq!(Arc::strong_count(&control.inner), before_owner_aliases);
    finish(work, &mut driver);
    let copy = lease.try_clone().unwrap();
    assert!(copy.shares_record_with(&lease));
    drop(target);
    drop(lease);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(copy);
    drive(&mut driver);
    assert_eq!(held(&control, JointResource::PayloadBytes), 0);
}

#[test]
fn typed_record_rejects_even_internal_raw_source_and_retained_entry() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("raw-fence")];
    bind(&scope, &facts, &mut driver);
    let mut source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let id = source.as_ref().unwrap().id.unwrap();
    let mut target = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target)
        .unwrap()
        .notify();
    // Only this descendant unit test can build the impossible public raw alias.
    // Exercise the actual original source and Drop authors, not a copied check.
    let mut raw = Some(JointClaim {
        inner: Arc::clone(&control.inner),
        id: Some(id),
    });
    assert!(matches!(
        raw.as_ref().unwrap().view(),
        Err(WorkError::Conflict)
    ));
    assert!(matches!(
        raw.as_ref().unwrap().amounts(),
        Err(WorkError::Conflict)
    ));
    assert_eq!(
        raw.as_mut().unwrap().release_part(JointResources::ZERO),
        Err(WorkError::Conflict)
    );
    let mut retained = None;
    assert!(matches!(
        super::super::retained_claim::retain_claim_in(&work.owner, &mut raw, &mut retained),
        Err(WorkError::Conflict)
    ));
    assert!(raw.is_some() && retained.is_none());
    drop(raw);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(target);
    finish(work, &mut driver);
    assert_eq!(held(&control, JointResource::PayloadBytes), 0);
}

fn cpu_stage(
    handle: &JointAdmissionHandle,
    work: &WorkOwner,
    facts: &[SharedDependency],
    driver: &mut JointProgressDriver,
) -> InternalComputationGrant {
    let scope = work.scope();
    bind(&scope, facts, driver);
    let mut source = Some(claim(
        handle,
        work,
        facts,
        JointLane::Foreground,
        amounts(JointResource::Computations, 1),
        driver,
    ));
    let mut grant = None;
    InternalComputationGrant::adopt_in(&scope, &mut source, &mut grant)
        .unwrap()
        .notify();
    grant.unwrap()
}

#[test]
fn cpu_action_inherits_every_stage_domain_and_retires_after_original_physical_drop() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("cpu-a"), fact("cpu-b")];
    let computation = cpu_stage(&handle, &work.owner, &facts, &mut driver);
    let mut action = computation
        .cpu_action_source(&scope)
        .unwrap()
        .begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap(),
        )
        .unwrap();
    {
        let state = control.inner.state.lock().unwrap();
        let request = state
            .joint
            .as_ref()
            .unwrap()
            .requests
            .values()
            .next()
            .unwrap();
        assert_eq!(request.changes.len(), 1);
        let change = &request.changes[0];
        assert_eq!(change.claim.facts.dependencies.len(), 2);
        assert_eq!(change.claim.facts.lane, JointLane::Foreground);
        for original in &facts {
            assert!(
                change
                    .claim
                    .facts
                    .dependencies
                    .iter()
                    .any(|d| d.fact == *original)
            );
        }
        assert_eq!(change.claim.amounts.get(JointResource::MetadataBytes), 17);
        assert_eq!(
            change.claim.amounts.get(JointResource::PhysicalQueueBytes),
            23
        );
    }
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
    drive(&mut driver);
    struct Physical<'a> {
        control: &'a WorkloadControl,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Physical<'_> {
        fn drop(&mut self) {
            // The real local holder must exit while its original charge lives.
            assert_eq!(held(self.control, JointResource::InternalCpuJobs), 1);
            self.dropped.store(true, Ordering::Release);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let mut physical = Some(Physical {
        control: &control,
        dropped: Arc::clone(&dropped),
    });
    let mut received = None;
    let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("expected same finite CPU action offer"),
    };
    offer
        .accept_in(&mut physical, &mut received)
        .unwrap()
        .notify();
    assert!(physical.is_none());
    assert_eq!(held(&control, JointResource::Computations), 1);
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 1);
    drop(received);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
    assert_eq!(held(&control, JointResource::PhysicalQueueRecords), 0);
    assert_eq!(held(&control, JointResource::Computations), 1);
    drop(computation);
    finish(work, &mut driver);
}

#[test]
fn cpu_action_refuses_foreign_scope_and_source_exit_at_both_physical_cuts() {
    for exit_after_offer in [false, true] {
        let (control, handle, mut driver) = fixture();
        let work = root(&control);
        let scope = work.owner.scope();
        let computation = cpu_stage(&handle, &work.owner, &[fact("exact")], &mut driver);
        let foreign = root(&control);
        assert!(matches!(
            computation.cpu_action_source(&foreign.owner.scope()),
            Err(WorkError::Conflict)
        ));
        let mut action = computation
            .cpu_action_source(&scope)
            .unwrap()
            .begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap(),
            )
            .unwrap();
        drive(&mut driver);
        if exit_after_offer {
            let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(Ok(offer)) => offer,
                _ => panic!("expected same original offer"),
            };
            drop(computation);
            let mut physical = Some(());
            let mut received = None;
            assert_eq!(
                offer.accept_in(&mut physical, &mut received).err(),
                Some(WorkError::Released)
            );
            assert!(physical.is_some() && received.is_none());
            // Explicit original local destruction, then original reply rollback.
            offer.reobserve_after_drop(&mut physical).unwrap().notify();
            assert!(physical.is_none());
        } else {
            drop(computation);
            assert!(matches!(
                action.poll_local_offer(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Released
                }))
            ));
        }
        drop(action);
        assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
        finish(foreign, &mut driver);
        finish(work, &mut driver);
    }
}

#[test]
fn original_settlement_cpu_action_survives_new_business_cancel_at_each_local_cut() {
    for phase in 0..4 {
        let (control, handle, mut driver) = fixture();
        let work = root(&control);
        let scope = work.owner.scope();
        let facts = [fact("settlement")];
        bind(&scope, &facts, &mut driver);
        let mut raw = Some(claim(
            &handle,
            &work.owner,
            &facts,
            JointLane::Settlement,
            amounts(JointResource::Computations, 1),
            &mut driver,
        ));
        let mut computation = None;
        InternalComputationGrant::adopt_in(&scope, &mut raw, &mut computation)
            .unwrap()
            .notify();
        let computation = computation.unwrap();
        let source = computation.cpu_action_source(&scope).unwrap();
        if phase == 0 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
        }
        let mut action = source
            .begin(
                std::num::NonZeroU64::new(1).unwrap(),
                std::num::NonZeroU64::new(1).unwrap(),
            )
            .unwrap();
        if phase == 1 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
        }
        drive(&mut driver);
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("existing settlement CPU action must remain admitted"),
        };
        if phase == 2 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
        }
        if phase == 3 {
            control.close_admission();
        }
        let mut physical = Some(());
        let mut target = None;
        offer
            .accept_in(&mut physical, &mut target)
            .unwrap()
            .notify();
        assert!(physical.is_none());
        assert_eq!(held(&control, JointResource::InternalCpuJobs), 1);
        drop(target);
        assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
        drop(action);
        drop(computation);
        work.owner.complete_after_terminal_cancel_settled();
        work.business.release();
        drive(&mut driver);
    }
}

#[test]
fn foreground_cpu_source_keeps_the_original_cancellation_refusal() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let computation = cpu_stage(&handle, &work.owner, &[fact("foreground")], &mut driver);
    let source = computation.cpu_action_source(&scope).unwrap();
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(matches!(
        source.begin(
            std::num::NonZeroU64::new(1).unwrap(),
            std::num::NonZeroU64::new(1).unwrap()
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Cancelled(_)
        })
    ));
    drop(computation);
    work.owner.complete_after_terminal_cancel_settled();
    work.business.release();
    drive(&mut driver);
}

#[test]
fn unknown_production_survives_last_handle_and_received_keeps_publication_until_disposition() {
    let (control, _handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("writer")], &mut driver);
    let quote = UnreceivedProductionQuote::new(
        std::num::NonZeroU64::new(2).unwrap(),
        std::num::NonZeroUsize::new(16).unwrap(),
    )
    .unwrap();
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let mut pending = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote)
        .unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original production grant"),
    };
    let mut facts = Some(vec![7; 16].into_boxed_slice());
    let mut grant = None;
    ready.register_in(&mut facts, &mut grant).unwrap().notify();
    drop((ready, pending));
    let owner = grant.as_ref().unwrap().owner();
    assert_eq!(owner.pending_records().unwrap(), 2);
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 2);
    assert_eq!(held(&control, JointResource::Publications), 1);
    assert!(matches!(
        owner.settle_received_in(),
        Err(WorkError::Conflict)
    ));
    grant.as_ref().unwrap().check_receive().unwrap();
    grant.as_ref().unwrap().receive_in().unwrap().notify();
    drop(grant.take());
    assert_eq!(owner.pending_records().unwrap(), 0);
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 0);
    assert_eq!(held(&control, JointResource::Publications), 1);
    assert!(owner.retains_facts(&[7; 16]).unwrap());
    owner.settle_received_in().unwrap().notify();
    assert_eq!(held(&control, JointResource::Publications), 0);
    assert!(!owner.retains_facts(&[7; 16]).unwrap());

    // The second actual record is deliberately missing End/receive. Neither
    // last grant Drop nor the original logical terminal may refund it.
    let mut pending = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote)
        .unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original second production grant"),
    };
    let mut facts = Some(vec![9; 16].into_boxed_slice());
    ready.register_in(&mut facts, &mut grant).unwrap().notify();
    drop((ready, pending));
    let retirement = work.owner.retirement_observer().unwrap();
    drop(grant.take());
    drop(origin);
    finish(work, &mut driver);
    assert_eq!(owner.pending_records().unwrap(), 2);
    assert!(owner.retains_facts(&[9; 16]).unwrap());
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 2);
    assert!(!retirement.is_reclaimed().unwrap());
}

#[test]
fn production_registration_rechecks_cancel_after_real_grant_before_metadata_move() {
    let (control, _handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("writer")], &mut driver);
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let quote = UnreceivedProductionQuote::new(
        std::num::NonZeroU64::new(2).unwrap(),
        std::num::NonZeroUsize::new(16).unwrap(),
    )
    .unwrap();
    let mut pending = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote)
        .unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original grant before cancellation"),
    };
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    let mut facts = Some(vec![3; 16].into_boxed_slice());
    let mut grant = None;
    assert!(matches!(
        ready.register_in(&mut facts, &mut grant),
        Err(WorkError::Cancelled(_))
    ));
    assert!(grant.is_none() && facts.is_some());
    drop(facts);
    drop((ready, pending));
    drive(&mut driver);
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 0);
    assert_eq!(held(&control, JointResource::Publications), 0);
    finish(work, &mut driver);
}

fn registered_production(
    scope: &WorkScope,
    origin: &Arc<dyn Send + Sync>,
    driver: &mut JointProgressDriver,
) -> UnreceivedProductionGrant {
    let quote = UnreceivedProductionQuote::new(
        std::num::NonZeroU64::new(1).unwrap(),
        std::num::NonZeroUsize::new(8).unwrap(),
    )
    .unwrap();
    let mut admission = scope
        .begin_unreceived_productions(Arc::downgrade(origin), quote)
        .unwrap();
    drive(driver);
    let mut ready = match Pin::new(&mut admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("actual production Ready"),
    };
    let mut facts = Some(vec![31; 8].into_boxed_slice());
    let mut target = None;
    ready.register_in(&mut facts, &mut target).unwrap().notify();
    drop((ready, admission));
    target.unwrap()
}
fn production_rows(
    scope: &WorkScope,
) -> Vec<(u64, u64, usize, usize, JointResources, JointResources)> {
    let state = scope.inner.state.lock().unwrap();
    state
        .joint
        .as_ref()
        .unwrap()
        .claims
        .iter()
        .filter_map(|(&id, row)| {
            let ClaimCustody::Production { epoch } = row.custody else {
                return None;
            };
            (row.owner == ClaimOwner::PhysicalWork(scope.id)).then_some((
                id,
                epoch,
                row.facts.dependencies.len(),
                row.active_facts.dependencies.len(),
                row.amounts,
                row.active_amounts,
            ))
        })
        .collect()
}
#[test]
fn control_growth_installs_unknown_and_received_productions_in_one_original_receipt() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("original")], &mut driver);
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let received = registered_production(&scope, &origin, &mut driver);
    received.receive_in().unwrap().notify();
    let unknown = registered_production(&scope, &origin, &mut driver);
    let before = production_rows(&scope);
    assert_eq!(scope.control_binding_change_records().unwrap(), 3);
    assert_eq!(before.len(), 2);
    let mut growth = scope.begin_control_binding().unwrap();
    growth.push_dependency(&fact("additional")).unwrap();
    growth.end_change().unwrap();
    let mut admission = growth.submit().unwrap();
    assert!(matches!(unknown.check_receive(), Err(WorkError::Conflict)));
    drive(&mut driver);
    let mut ready = receive(&mut admission);
    let provisional = production_rows(&scope);
    assert!(provisional.iter().all(|row| row.2 == 2 && row.3 == 1));
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 1);
    assert_eq!(held(&control, JointResource::Publications), 2);
    ready
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    let after = production_rows(&scope);
    for (old, new) in before.iter().zip(&after) {
        assert_eq!(
            (old.0, old.1, old.4, old.5),
            (new.0, new.1, new.4, new.5),
            "same row/epoch/charges"
        );
        assert_eq!((new.2, new.3), (2, 2));
    }
    unknown.receive_in().unwrap().notify(); // Same unique original grant survives D Grow.
    assert_eq!(unknown.owner().pending_records().unwrap(), 0);
    drop((ready, admission, received));
    let owner = unknown.owner();
    drop(unknown);
    assert_eq!(held(&control, JointResource::Publications), 2);
    owner.settle_received_in().unwrap().notify();
    finish(work, &mut driver);
}
#[test]
fn pending_received_production_refuses_settlement_and_drop_rolls_back_all_domain_delta() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("original")], &mut driver);
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let grant = registered_production(&scope, &origin, &mut driver);
    grant.receive_in().unwrap().notify();
    let before = production_rows(&scope);
    let mut growth = scope.begin_control_binding().unwrap();
    growth.push_dependency(&fact("additional")).unwrap();
    growth.end_change().unwrap();
    let mut admission = growth.submit().unwrap();
    drive(&mut driver);
    let reply = receive(&mut admission);
    assert!(matches!(
        grant.owner().settle_received_in(),
        Err(WorkError::Conflict)
    ));
    assert_eq!(production_rows(&scope)[0].2, 2);
    drop((reply, admission));
    drive(&mut driver);
    assert_eq!(production_rows(&scope), before);
    assert_eq!(held(&control, JointResource::Publications), 1);
    grant.owner().settle_received_in().unwrap().notify();
    drop(grant);
    finish(work, &mut driver);
}

#[test]
fn an_already_granted_new_record_cannot_register_across_pending_control_growth() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("original")], &mut driver);
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let quote = UnreceivedProductionQuote::new(
        std::num::NonZeroU64::new(1).unwrap(),
        std::num::NonZeroUsize::new(8).unwrap(),
    )
    .unwrap();
    let mut registration = scope
        .begin_unreceived_productions(Arc::downgrade(&origin), quote)
        .unwrap();
    drive(&mut driver);
    let mut raw_ready =
        match Pin::new(&mut registration).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("actual earlier raw New grant"),
        };
    let mut growth = scope.begin_control_binding().unwrap();
    growth.push_dependency(&fact("additional")).unwrap();
    growth.end_change().unwrap();
    let mut admission = growth.submit().unwrap();
    let mut facts = Some(vec![55; 8].into_boxed_slice());
    let mut destination = None;
    assert!(raw_ready.register_in(&mut facts, &mut destination).is_err());
    assert!(facts.is_some() && destination.is_none());
    assert!(production_rows(&scope).is_empty());
    drop(facts); // Metadata actual exit precedes its unregistered raw grant.
    drop((raw_ready, registration));
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&scope)
        .unwrap()
        .notify();
    assert_eq!(held(&control, JointResource::UnreceivedProductions), 0);
    assert_eq!(held(&control, JointResource::Publications), 0);
    let grant = registered_production(&scope, &origin, &mut driver);
    assert_eq!(
        production_rows(&scope)[0].3,
        2,
        "new registration captures the actual installed full union"
    );
    grant.receive_in().unwrap().notify();
    grant.owner().settle_received_in().unwrap().notify();
    drop((grant, reply, admission));
    finish(work, &mut driver);
}

#[test]
fn original_payload_disposal_preserves_live_alias_metadata_domains_and_next_capacity() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("formation")];
    bind(&scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 64),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let payload = payload.unwrap();
    let kept = (vec![9u8; 8], payload.retain_alias().unwrap());
    let forming = vec![7u8; 56];
    let key = payload.pin.key.unwrap();
    let (old_facts, old_metadata, old_epoch) = {
        let state = control.inner.state.lock().unwrap();
        let row = &state.joint.as_ref().unwrap().claims[&key[0]];
        (
            Arc::clone(&row.facts),
            row.amounts.get(JointResource::MetadataBytes),
            key[1],
        )
    };
    assert!(matches!(
        payload.retire_unused_to(std::num::NonZeroU64::new(65).unwrap()),
        Err(WorkError::Conflict)
    ));
    assert_eq!(kept.1.capacity_bytes().unwrap(), 64);
    drop(forming); // Actual original formation backing exits first.
    payload
        .retire_unused_to(std::num::NonZeroU64::new(8).unwrap())
        .unwrap()
        .notify();
    assert_eq!(kept.0.len(), 8);
    kept.1.check_backing_total(8).unwrap();
    assert!(kept.1.check_backing_total(9).is_err());
    {
        let state = control.inner.state.lock().unwrap();
        let row = &state.joint.as_ref().unwrap().claims[&key[0]];
        assert!(Arc::ptr_eq(&old_facts, &row.facts));
        assert_eq!(row.amounts.get(JointResource::MetadataBytes), old_metadata);
        assert!(matches!(row.custody, ClaimCustody::Payload { epoch, pins:2 } if epoch==old_epoch));
        assert_eq!(row.active_amounts.get(JointResource::PayloadBytes), 8);
    }
    let next = claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 32),
        &mut driver,
    );
    assert_eq!(held(&control, JointResource::PayloadBytes), 40);
    drop((next, kept, payload));
    finish(work, &mut driver);
    assert_eq!(held(&control, JointResource::PayloadBytes), 0);
}

#[test]
fn provider_control_cpu_source_uses_exact_bound_node_without_computation_custody() {
    // This neutral identity fixture proves fences/accounting, not a Provider
    // callback's finiteness or the physical QA factory. FA's actual fixed-pool
    // consumer exercises the same real production factory separately.
    let pool: Arc<dyn Send + Sync> = Arc::new(());
    let descriptor = InternalResultClosedFactoryDescriptor::for_composition(
        Arc::downgrade(&pool),
        std::any::TypeId::of::<fn()>(),
        std::num::NonZeroU64::new(23).unwrap(),
    );
    let (control, _handle, mut driver, binding) =
        fixture_with_optional_closed_factory(Some(descriptor));
    let binding = binding.unwrap();
    let work = root(&control);
    let scope = work.owner.scope();
    assert!(binding.provider_creator_source(&scope).is_err()); // unbound D
    let facts = [fact("creator-original-a"), fact("creator-original-b")];
    bind(&scope, &facts, &mut driver);
    let source = binding.provider_creator_source(&scope).unwrap();
    let mut action = source
        .begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap(),
        )
        .unwrap();
    {
        let state = control.inner.state.lock().unwrap();
        let request = state
            .joint
            .as_ref()
            .unwrap()
            .requests
            .values()
            .next()
            .unwrap();
        let claim = &request.changes[0].claim;
        assert_eq!(claim.facts.dependencies.len(), 2);
        assert_eq!(claim.facts.lane, JointLane::Foreground);
        assert_eq!(claim.amounts.get(JointResource::Computations), 0);
        assert_eq!(claim.amounts.get(JointResource::InternalCpuJobs), 1);
        assert_eq!(claim.amounts.get(JointResource::PhysicalQueueRecords), 1);
    }
    drive(&mut driver);
    let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("expected exact original CPU offer"),
    };
    let mut physical = Some(());
    let mut installed = None;
    offer
        .accept_in(&mut physical, &mut installed)
        .unwrap()
        .notify();
    assert!(physical.is_none());
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 1);
    assert_eq!(held(&control, JointResource::Computations), 0);
    // Occupied original receiving slots cannot be overwritten, even by a
    // second independently valid source. This is not a physical-pool test.
    let mut second = binding
        .provider_creator_source(&scope)
        .unwrap()
        .begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap(),
        )
        .unwrap();
    drive(&mut driver);
    let mut second_offer = match second.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("same original source offer"),
    };
    let mut second_physical = Some(());
    assert!(matches!(
        second_offer.accept_in(&mut second_physical, &mut installed),
        Err(WorkError::Conflict)
    ));
    assert!(second_physical.is_some());
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 2);
    second_offer
        .reobserve_after_drop(&mut second_physical)
        .unwrap()
        .notify();
    drop(second_offer);
    drop(second);
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 1);
    drop(installed);
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
    let stale = binding.provider_creator_source(&scope).unwrap();
    work.owner.complete();
    assert!(
        stale
            .begin(
                std::num::NonZeroU64::new(17).unwrap(),
                std::num::NonZeroU64::new(23).unwrap()
            )
            .is_err()
    );
    work.business.release();
    drive(&mut driver);
}

#[test]
fn ordinary_positive_grant_does_not_certify_a_closed_cpu_factory() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("shape-a"), fact("shape-b")];
    let computation = cpu_stage(&handle, &work.owner, &facts, &mut driver);
    let external = claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Foreground,
        amounts(JointResource::PhysicalQueueRecords, 1).with(JointResource::PhysicalQueueBytes, 23),
        &mut driver,
    );
    let mut forged = handle.begin_paid_proposal(&work.owner).unwrap();
    assert!(matches!(
        forged.begin_new(
            JointLane::Foreground,
            amounts(JointResource::InternalCpuJobs, 1)
        ),
        Err(WorkError::Conflict)
    ));
    drop(forged);
    // The original progress service retires the rejected constructor's cell
    // before this same work constructs another independently rejected action.
    drive(&mut driver);
    let mut growth = handle.begin_paid_proposal(&work.owner).unwrap();
    assert!(matches!(
        growth.begin_grow(
            external.view().unwrap(),
            JointResources::ZERO.with(JointResource::InternalCpuJobs, 1)
        ),
        Err(WorkError::Conflict)
    ));
    drop(growth);
    drive(&mut driver);
    let mut action = computation
        .cpu_action_source(&scope)
        .unwrap()
        .begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap(),
        )
        .unwrap();
    drive(&mut driver);
    let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original unregistered source"),
    };
    offer
        .reobserve_after_drop(&mut None::<()>)
        .unwrap()
        .notify();
    drop(offer);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(
            state.joint.as_ref().unwrap().claims.values().all(|row| row
                .facts
                .key
                .source_kind
                .is_empty())
        );
    }
    // No invented physical value is accepted. The actual queue/factory
    // integration regression lives with the original QA worker author.
    drop((action, external, computation));
    finish(work, &mut driver);
}

#[test]
fn cancelled_cpu_action_keeps_original_refusal_after_the_producer_source_exits() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let computation = cpu_stage(
        &handle,
        &work.owner,
        &[fact("original-refusal")],
        &mut driver,
    );
    let mut action = computation
        .cpu_action_source(&scope)
        .unwrap()
        .begin(
            std::num::NonZeroU64::new(1).unwrap(),
            std::num::NonZeroU64::new(65).unwrap(),
        )
        .unwrap();
    work.owner.cancel(crate::CancellationReason::Requested);
    drive(&mut driver);
    drop(computation);
    assert!(matches!(
        action.poll_local_offer(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Cancelled(crate::CancellationReason::Requested)
        }))
    ));
    assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
    drop(action);
    finish(work, &mut driver);
}

#[test]
fn wrong_shape_full_domain_and_record_refusals_keep_the_original_move_only_claim() {
    for case in 0..4 {
        let (control, handle, mut driver) = fixture();
        let work = root(&control);
        let scope = work.owner.scope();
        let facts = [fact("full-a"), fact("full-b")];
        bind(&scope, &facts, &mut driver);
        let mut amount = amounts(JointResource::PayloadBytes, 8);
        if case == 0 {
            amount = amount.with(JointResource::UnreceivedProductions, 1);
        }
        if case == 1 {
            amount = amount.with(JointResource::ClaimRecords, 2);
        }
        if case == 2 {
            amount = amount.with(JointResource::ClaimRecords, 0);
        }
        let selected = if case == 3 { &facts[..1] } else { &facts[..] };
        let mut source = Some(claim(
            &handle,
            &work.owner,
            selected,
            JointLane::Custody,
            amount,
            &mut driver,
        ));
        let original = source.as_ref().unwrap().view().unwrap();
        let mut target = None;
        let before = control.progress_revision();
        assert!(matches!(
            RetainedPayloadGrant::adopt_in(&scope, &mut source, &mut target),
            Err(WorkError::Conflict)
        ));
        assert!(target.is_none());
        assert_eq!(source.as_ref().unwrap().id, Some(original.id));
        assert_eq!(source.as_ref().unwrap().amounts().unwrap(), amount);
        assert_eq!(control.progress_revision(), before);
        drop(source);
        finish(work, &mut driver);
    }
}

#[test]
fn computation_and_payload_pins_retire_independently_at_their_actual_last_alias() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("union")];
    bind(&scope, &facts, &mut driver);
    let mut payload_source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut payload_source, &mut payload)
        .unwrap()
        .notify();
    struct ActualPayload {
        bytes: Vec<u8>,
        _paid: RetainedPayloadAlias,
    }
    let output = ActualPayload {
        bytes: vec![7; 8],
        _paid: payload.as_ref().unwrap().retain_alias().unwrap(),
    };
    let same_union = output._paid.try_clone().unwrap();
    assert!(same_union.shares_payload_with(&output._paid));
    assert_eq!(same_union.capacity_bytes().unwrap(), 8);
    assert!(same_union.check_backing_total(9).is_err());
    let mut compute_source = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Foreground,
        amounts(JointResource::Computations, 1),
        &mut driver,
    ));
    let mut computation = None;
    InternalComputationGrant::adopt_in(&scope, &mut compute_source, &mut computation)
        .unwrap()
        .notify();
    let producer = computation.as_ref().unwrap().retain_alias().unwrap();
    drop(computation.take());
    assert_eq!(
        held(&control, JointResource::Computations),
        1,
        "the original producer alias still retains computation responsibility"
    );
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(producer);
    drive(&mut driver);
    assert_eq!(held(&control, JointResource::Computations), 0);
    let mut next = request(
        &handle,
        &work.owner,
        &facts,
        JointLane::Foreground,
        amounts(JointResource::Computations, 1),
    );
    drive(&mut driver);
    let next = receive(&mut next);
    assert_eq!(held(&control, JointResource::Computations), 1);
    assert_eq!(output.bytes, vec![7; 8]);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(next);
    drop(payload.take());
    finish(work, &mut driver);
    assert_eq!(held(&control, JointResource::Computations), 0);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(output);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    let spi_holder = same_union.into_retention();
    drive(&mut driver);
    assert_eq!(held(&control, JointResource::PayloadBytes), 8);
    drop(spi_holder);
    drive(&mut driver);
    assert_eq!(held(&control, JointResource::PayloadBytes), 0);
}

#[test]
fn positive_payload_metadata_cpu_continuation_keeps_original_root_facts_after_cancel_and_close() {
    // This WC component uses a neutral physical value. The real QA queue and
    // worker/footer regression is owned by InternalResultCpu's component.
    for cancel_at in 0..3 {
        let pool: Arc<dyn Send + Sync> = Arc::new(());
        let descriptor = InternalResultClosedFactoryDescriptor::for_composition(
            Arc::downgrade(&pool),
            std::any::TypeId::of::<fn()>(),
            std::num::NonZeroU64::new(23).unwrap(),
        );
        let (control, handle, mut driver, factory) =
            fixture_with_optional_closed_factory(Some(descriptor));
        let factory = factory.unwrap();
        let work = root(&control);
        let scope = work.owner.scope();
        let facts = [
            fact("metadata-obligation-catalog"),
            fact("metadata-obligation-store"),
        ];
        bind(&scope, &facts, &mut driver);
        let mut raw = Some(claim(
            &handle,
            &work.owner,
            &facts,
            JointLane::Custody,
            amounts(JointResource::PayloadBytes, 8),
            &mut driver,
        ));
        let mut payload = None;
        RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
            .unwrap()
            .notify();
        let payload = payload.unwrap();
        if cancel_at == 0 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
            control.close_admission();
        }
        let source = factory
            .provider_metadata_continuation_source(&payload, &scope)
            .unwrap();
        let mut action = source
            .begin(
                std::num::NonZeroU64::new(17).unwrap(),
                std::num::NonZeroU64::new(23).unwrap(),
            )
            .unwrap();
        if cancel_at == 1 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
            control.close_admission();
        }
        {
            let state = control.inner.state.lock().unwrap();
            let request = state
                .joint
                .as_ref()
                .unwrap()
                .requests
                .values()
                .next()
                .unwrap();
            assert_eq!(request.changes[0].claim.facts.lane, JointLane::Settlement);
            assert_eq!(request.changes[0].claim.facts.dependencies.len(), 2);
            assert_eq!(
                request.changes[0]
                    .claim
                    .amounts
                    .get(JointResource::Computations),
                0
            );
        }
        drive(&mut driver);
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("same original metadata obligation offer"),
        };
        if cancel_at == 2 {
            work.owner.cancel(crate::CancellationReason::ServerShutdown);
            control.close_admission();
        }
        assert!(
            factory.provider_creator_source(&scope).is_err(),
            "ordinary creator never upgrades itself"
        );
        let mut physical = Some(());
        let mut target = None;
        offer
            .accept_in(&mut physical, &mut target)
            .unwrap()
            .notify();
        assert!(physical.is_none());
        assert_eq!(held(&control, JointResource::InternalCpuJobs), 1);
        assert_eq!(held(&control, JointResource::Computations), 0);
        drop(offer);
        drop(action);
        assert_eq!(
            held(&control, JointResource::InternalCpuJobs),
            1,
            "the actual local holder retains its claim"
        );
        drop(target);
        assert_eq!(held(&control, JointResource::InternalCpuJobs), 0);
        drop(payload);
        work.owner.complete_after_terminal_cancel_settled();
        work.business.release();
        drive(&mut driver);
        assert!(control.shutdown().is_ok());
    }
}

#[test]
fn metadata_cpu_continuation_rejects_foreign_root_released_payload_and_closed_driver() {
    let pool: Arc<dyn Send + Sync> = Arc::new(());
    let descriptor = InternalResultClosedFactoryDescriptor::for_composition(
        Arc::downgrade(&pool),
        std::any::TypeId::of::<fn()>(),
        std::num::NonZeroU64::new(23).unwrap(),
    );
    let (control, handle, mut driver, factory) =
        fixture_with_optional_closed_factory(Some(descriptor));
    let factory = factory.unwrap();
    let work = root(&control);
    let other = root(&control);
    let scope = work.owner.scope();
    let facts = [fact("metadata-positive-root")];
    bind(&scope, &facts, &mut driver);
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let payload = payload.unwrap();
    assert!(matches!(
        factory.provider_metadata_continuation_source(&payload, &other.owner.scope()),
        Err(WorkError::Conflict)
    ));
    let (foreign, _, foreign_driver) = fixture();
    let foreign_work = root(&foreign);
    assert!(matches!(
        factory.provider_metadata_continuation_source(&payload, &foreign_work.owner.scope()),
        Err(WorkError::ForeignAuthority)
    ));
    foreign_work.owner.complete();
    foreign_work.business.release();
    drop(foreign_driver);
    drop(foreign);
    let stale = factory
        .provider_metadata_continuation_source(&payload, &scope)
        .unwrap();
    drop(payload);
    assert!(matches!(
        stale.begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap()
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Released
        })
    ));
    // A live positive payload never overrides the real driver's closure.
    let mut raw = Some(claim(
        &handle,
        &work.owner,
        &facts,
        JointLane::Custody,
        amounts(JointResource::PayloadBytes, 8),
        &mut driver,
    ));
    let mut payload = None;
    RetainedPayloadGrant::adopt_in(&scope, &mut raw, &mut payload)
        .unwrap()
        .notify();
    let payload = payload.unwrap();
    let source = factory
        .provider_metadata_continuation_source(&payload, &scope)
        .unwrap();
    drop(driver);
    assert!(matches!(
        source.begin(
            std::num::NonZeroU64::new(17).unwrap(),
            std::num::NonZeroU64::new(23).unwrap()
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Closed
        })
    ));
    drop(payload);
    work.owner.complete();
    work.business.release();
    other.owner.complete();
    other.business.release();
}
