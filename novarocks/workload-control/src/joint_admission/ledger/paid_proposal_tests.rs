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

//! Paid construction through the actual original queue/proof/receipt authors.

use super::*;
use novarocks_resource_contract::NetworkServiceOrigin;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn network(name: usize) -> SharedDependency {
    SharedDependency::Network(
        NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://paid-{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("bounded original driver must reach idle");
}
fn receive(request: &mut JointAdmission) -> JointGrantedBatch {
    let Poll::Ready(Ok(batch)) = Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("actual original grant reply")
    };
    batch
}
fn published(handle: &JointAdmissionHandle, fact: &SharedDependency) -> Option<bool> {
    let state = handle.inner.state.lock().unwrap();
    let ledger = state.joint.as_ref().unwrap();
    if !ledger.dependencies.as_ref().unwrap().contains(fact) {
        return None;
    }
    Some(
        ledger
            .claims
            .values()
            .any(|row| row.facts.dependencies.iter().any(|d| &d.fact == fact)),
    )
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn finish(root: crate::RootWork) {
    root.business.release();
    root.owner.complete();
}

#[test]
fn shared_provisional_identity_survives_one_withdrawal_and_publishes_only_on_grant() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let a = root(&control);
    let b = root(&control);
    let fact = network(0);
    let mut first = handle.begin_paid_proposal(&a.owner).unwrap();
    first
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    first.push_dependency(&fact).unwrap();
    let identity = first.current.as_ref().unwrap().selected[0].ordinal;
    first.end_change().unwrap();
    let mut second = handle.begin_paid_proposal(&b.owner).unwrap();
    second
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    second.push_dependency(&fact).unwrap();
    assert_eq!(
        second.current.as_ref().unwrap().selected[0].ordinal,
        identity
    );
    second.end_change().unwrap();
    assert_eq!(published(&handle, &fact), Some(false));
    let first = first.submit().unwrap();
    let mut second = second.submit().unwrap();
    drop(first);
    assert_eq!(published(&handle, &fact), Some(false));
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), Some(true));
    let reply = receive(&mut second);
    drop(reply);
    finish(a);
    finish(b);
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), None);
}

#[test]
fn paid_grow_reauthenticates_and_keeps_full_old_and_new_domains() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let a = network(0);
    let b = network(1);
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&a).unwrap();
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let old = work.owner.control_claim_view().unwrap();
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(old.clone(), JointResources::ZERO).unwrap();
    grow.push_dependency(&b).unwrap();
    grow.end_change().unwrap();
    let mut grow = grow.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut grow);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    {
        let state = handle.inner.state.lock().unwrap();
        let node = &state.nodes[&work.owner.scope().id()];
        let claim = state.cancellations.slots[node.cancellation.0]
            .claim
            .unwrap();
        assert!(
            state
                .joint
                .as_ref()
                .unwrap()
                .control_has_domains_for_test(claim, &[a, b])
        );
    }
    let mut stale = handle.begin_paid_proposal(&work.owner).unwrap();
    assert_eq!(
        stale.begin_grow(old, JointResources::ZERO),
        Err(WorkError::Conflict)
    );
    drop(stale);
    finish(work);
    drive(&mut driver);
}

struct PanicOnceWake {
    panicked: std::sync::atomic::AtomicBool,
    inner: Weak<Inner>,
    before_panic: std::sync::Mutex<Option<crate::WorkloadProgressRevision>>,
}
impl std::task::Wake for PanicOnceWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let inner = self.inner.upgrade().unwrap();
        assert!(
            inner.state.try_lock().is_ok(),
            "wake must run after State unlock"
        );
        if !self
            .panicked
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let marker = {
                let state = inner.state.lock().unwrap();
                inner.capture_progress(&state)
            };
            *self.before_panic.lock().unwrap() = Some(marker);
            panic!("one actual post-State wake panic");
        }
    }
}

#[test]
fn caught_push_wake_panic_cannot_submit_and_temporary_last_exit_is_observed() {
    assert_caught_push_exit(false);
}
#[test]
fn selected_route_output_exits_before_wake_even_when_the_builder_survives_panic() {
    assert_caught_push_exit(true);
}
fn assert_caught_push_exit(selected_route: bool) {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let fact = network(0);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    let marker = control.progress_revision();
    let mut waiter = Box::pin(control.wait_progress(marker));
    let wake = Arc::new(PanicOnceWake {
        panicked: std::sync::atomic::AtomicBool::new(false),
        inner: Arc::downgrade(&handle.inner),
        before_panic: std::sync::Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let selected = url::Url::parse("https://paid-0.example/selected/path?private=secret").unwrap();
    let view = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if selected_route {
            proposal.push_selected_network(view)
        } else {
            proposal.push_dependency(&fact)
        }
    }));
    assert!(outcome.is_err());
    assert!(wake.panicked.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(proposal.current.as_ref().unwrap().selected.len(), 0);
    assert_eq!(proposal.failed, Some(WorkError::Conflict));
    let before_exit = wake
        .before_panic
        .lock()
        .unwrap()
        .expect("actual callback marker");
    assert_ne!(
        control.progress_revision(),
        before_exit,
        "temporary data reference exits before the per-call reclaim generation"
    );
    drive(&mut driver);
    assert_eq!(
        published(&handle, &fact),
        None,
        "raw callback result exited before per-call wake"
    );
    let before = control.progress_revision();
    assert_eq!(proposal.end_change(), Err(WorkError::Conflict));
    assert_eq!(
        before,
        control.progress_revision(),
        "reading a sealed error must not publish false progress"
    );
    assert!(proposal.submit().is_err());
    drop(waiter);
    finish(work);
    drive(&mut driver);
}

#[test]
fn paid_selected_origin_deduplicates_before_owned_output_and_preserves_full_union() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let selected =
        url::Url::parse("https://user:secret@PAID-0.example:443/current?secret=private").unwrap();
    let alias = url::Url::parse("https://paid-0.example/other").unwrap();
    let first = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
    let same = SelectedNetworkOrigin::from_selected_url(&alias).unwrap();
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_selected_network(first).unwrap();
    let before = handle.inner.state.lock().unwrap().next_id;
    bind.push_selected_network(same).unwrap();
    assert_eq!(handle.inner.state.lock().unwrap().next_id, before);
    assert_eq!(bind.current.as_ref().unwrap().selected.len(), 1);
    assert_eq!(bind.current.as_ref().unwrap().selected[0].fact, network(0));
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(
        work.owner.control_claim_view().unwrap(),
        JointResources::ZERO,
    )
    .unwrap();
    let b = url::Url::parse("https://paid-1.example/object-store").unwrap();
    grow.push_selected_network(SelectedNetworkOrigin::from_selected_url(&b).unwrap())
        .unwrap();
    grow.end_change().unwrap();
    let mut grow = grow.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut grow);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut overflow = handle.begin_paid_proposal(&work.owner).unwrap();
    overflow
        .begin_grow(
            work.owner.control_claim_view().unwrap(),
            JointResources::ZERO,
        )
        .unwrap();
    let c = url::Url::parse("https://new.example/not-yet-owned").unwrap();
    overflow
        .push_selected_network(SelectedNetworkOrigin::from_selected_url(&c).unwrap())
        .unwrap();
    overflow.end_change().unwrap();
    let mut pending = overflow.submit().unwrap();
    drive(&mut driver);
    receive(&mut pending)
        .adjustment
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    finish(work);
    drive(&mut driver);
}

#[test]
fn multiple_changes_share_the_complete_request_union_without_new_canonical_facts() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 2);
    let work = root(&control);
    let a = network(0);
    let b = network(1);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    proposal.push_dependency(&a).unwrap();
    proposal.push_dependency(&b).unwrap();
    proposal.end_change().unwrap();
    let next = handle.inner.state.lock().unwrap().next_id;
    proposal
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    proposal.push_dependency(&b).unwrap();
    proposal.push_dependency(&a).unwrap();
    proposal.end_change().unwrap();
    assert_eq!(handle.inner.state.lock().unwrap().next_id, next);
    let mut request = proposal.submit().unwrap();
    drive(&mut driver);
    let reply = receive(&mut request);
    assert_eq!(reply.new_claims.len(), 2);
    drop((reply, request));
    finish(work);
    drive(&mut driver);
    assert_eq!(published(&handle, &a), None);
    assert_eq!(published(&handle, &b), None);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn responsibility_records_and_complete_dependencies_have_no_cancelled_q_or_k_gate() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
    let work = root(&control);
    let facts: Vec<_> = (0..16).map(network).collect();
    let mut proposals = Vec::new();
    for _ in 0..24 {
        let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
        proposal
            .begin_new(
                JointLane::Custody,
                JointResources::ZERO.with(JointResource::ClaimRecords, 1),
            )
            .unwrap();
        for fact in &facts {
            proposal.push_dependency(fact).unwrap();
        }
        proposal.end_change().unwrap();
        proposals.push(proposal);
    }
    // This is a responsibility-kernel component, not an allocation/CPU grant.
    let mut pending: Vec<_> = proposals.into_iter().map(|p| p.submit().unwrap()).collect();
    drive(&mut driver);
    let mut claims = Vec::new();
    for request in &mut pending {
        let mut batch = receive(request);
        claims.push(batch.new_claims.pop().unwrap());
    }
    {
        let state = control.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        for claim in &claims {
            assert_eq!(
                ledger.claims[&claim.id.unwrap()]
                    .active_facts
                    .dependencies
                    .len(),
                16
            );
        }
    }
    drop((pending, claims, facts));
    finish(work);
    drive(&mut driver);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn fair_turn_checks_at_most_the_original_service_budget_and_does_not_extend_its_pass() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
    let work = root(&control);
    let mut pending = Vec::new();
    for _ in 0..20 {
        let mut p = handle.begin_paid_proposal(&work.owner).unwrap();
        p.begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
        p.push_dependency(&SharedDependency::LocalHost).unwrap();
        p.end_change().unwrap();
        pending.push(p.submit().unwrap());
    }
    let before = driver.request_checks;
    assert!(matches!(driver.step(), JointProgress::More));
    assert_eq!(driver.request_checks - before, 7);
    drop(pending);
    finish(work);
    drive(&mut driver);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn first_bind_deduplicates_but_new_duplicate_poisons_entire_proposal() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 2);
    let work = root(&control);
    let fact = network(0);
    let mut duplicate = handle.begin_paid_proposal(&work.owner).unwrap();
    duplicate
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    duplicate.push_dependency(&fact).unwrap();
    assert_eq!(duplicate.push_dependency(&fact), Err(WorkError::Conflict));
    assert!(duplicate.submit().is_err());
    drive(&mut driver);
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&fact).unwrap();
    bind.push_dependency(&fact).unwrap();
    assert_eq!(bind.current.as_ref().unwrap().selected.len(), 1);
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut bind);
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    finish(work);
    drive(&mut driver);
}

#[test]
fn rejection_of_a_later_change_poison_cannot_publish_an_earlier_complete_change() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 2);
    let work = root(&control);
    let a = network(0);
    let b = network(1);
    let mut p = handle.begin_paid_proposal(&work.owner).unwrap();
    p.begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    p.push_dependency(&a).unwrap();
    p.end_change().unwrap();
    p.begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    p.push_dependency(&b).unwrap();
    let next = handle.inner.state.lock().unwrap().next_id;
    assert_eq!(p.push_dependency(&b), Err(WorkError::Conflict));
    assert_eq!(handle.inner.state.lock().unwrap().next_id, next);
    assert!(p.end_change().is_err());
    assert!(p.submit().is_err());
    assert!(
        handle
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .requests
            .is_empty()
    );
    finish(work);
    drive(&mut driver);
    assert_eq!(published(&handle, &a), None);
    assert_eq!(published(&handle, &b), None);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn rejected_push_after_complete_growth_does_not_mint_or_partially_bind_a_new_domain() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
    let work = root(&control);
    let a = network(0);
    let b = network(1);
    let c = network(2);
    let mut bind = work.owner.scope().begin_control_binding().unwrap();
    bind.push_dependency(&a).unwrap();
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    receive(&mut bind)
        .adjustment
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    let mut grow = handle.begin_paid_proposal(&work.owner).unwrap();
    grow.begin_grow(
        work.owner.control_claim_view().unwrap(),
        JointResources::ZERO,
    )
    .unwrap();
    grow.push_dependency(&b).unwrap();
    grow.end_change().unwrap();
    let next = handle.inner.state.lock().unwrap().next_id;
    assert_eq!(grow.push_dependency(&c), Err(WorkError::Conflict));
    assert_eq!(handle.inner.state.lock().unwrap().next_id, next);
    assert_eq!(published(&handle, &c), None);
    assert!(grow.submit().is_err());
    let state = handle.inner.state.lock().unwrap();
    let slot = state.nodes[&work.owner.scope().id()].cancellation;
    let id = state.cancellations.slots[slot.0].claim.unwrap();
    assert!(
        state
            .joint
            .as_ref()
            .unwrap()
            .control_has_domains_for_test(id, std::slice::from_ref(&a))
    );
    drop(state);
    finish(work);
    drive(&mut driver);
}

#[test]
fn production_builder_requires_original_control_storage_and_has_no_legacy_fallback() {
    let control = WorkloadControl::try_new_counted(crate::WorkloadConfig::default())
        .unwrap()
        .owner;
    let (handle, _driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control.mark_ready().unwrap();
    let work = root(&control);
    assert!(matches!(
        handle.begin_paid_proposal(&work.owner),
        Err(WorkError::NotReady)
    ));
    finish(work);
}

#[test]
fn provisional_dependency_identity_does_not_publish_or_mutate_other_original_rows() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let a = root(&control);
    let b = root(&control);
    let mut first = handle.begin_paid_proposal(&a.owner).unwrap();
    first
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    first.push_dependency(&network(0)).unwrap();
    first.end_change().unwrap();
    let mut first = first.submit().unwrap();
    drive(&mut driver);
    let batch = receive(&mut first);
    let before = {
        let state = handle.inner.state.lock().unwrap();
        let l = state.joint.as_ref().unwrap();
        (l.facts_revision, l.claims.len())
    };
    let mut second = handle.begin_paid_proposal(&b.owner).unwrap();
    second
        .begin_new(JointLane::Custody, JointResources::ZERO)
        .unwrap();
    second.push_dependency(&network(1)).unwrap();
    {
        let state = handle.inner.state.lock().unwrap();
        let l = state.joint.as_ref().unwrap();
        assert_eq!((l.facts_revision, l.claims.len()), before);
    }
    assert_eq!(published(&handle, &network(1)), Some(false));
    drop(second);
    drive(&mut driver);
    assert_eq!(published(&handle, &network(1)), None);
    drop(batch);
    finish(a);
    finish(b);
    drive(&mut driver);
}

#[test]
fn paid_submission_wake_panic_withdraws_original_request_and_reclaims_unpublished_facts() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 2, 1);
    let work = root(&control);
    let fact = network(0);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    proposal.push_dependency(&fact).unwrap();
    proposal.end_change().unwrap();
    let wake = Arc::new(PanicOnceWake {
        panicked: std::sync::atomic::AtomicBool::new(false),
        inner: Arc::downgrade(&handle.inner),
        before_panic: std::sync::Mutex::new(None),
    });
    let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
    let waker = Waker::from(Arc::clone(&wake));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| proposal.submit()));
    assert!(outcome.is_err());
    {
        let state = handle.inner.state.lock().unwrap();
        assert!(state.joint.as_ref().unwrap().requests.is_empty());
        assert_eq!(state.nodes[&work.owner.scope().id()].resource_waiters, 0);
    }
    assert_eq!(
        published(&handle, &fact),
        None,
        "unwound submission drops its original request and uncommitted facts"
    );
    drive(&mut driver);
    assert_eq!(published(&handle, &fact), None);
    drop(waiter);
    finish(work);
    drive(&mut driver);
}
