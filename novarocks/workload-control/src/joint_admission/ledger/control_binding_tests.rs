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

//! Behavior of the restricted delegate through the original fixed ledger.
use super::*;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    crate::scope::fixed_control_storage_tests::prototype_profile(8, 1, 2, 2)
}
fn network(name: &str) -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original control driver failed to become idle");
}
fn root(control: &WorkloadControl) -> crate::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}
fn receive(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(reply)) => reply,
        other => panic!("expected actual original control reply: {other:?}"),
    }
}
fn binding(scope: &WorkScope, fact: &SharedDependency) -> JointAdmission {
    let mut proposal = scope.begin_control_binding().unwrap();
    proposal.push_dependency(fact).unwrap();
    proposal.end_change().unwrap();
    proposal.submit().unwrap()
}
fn install(scope: &WorkScope, admission: &mut JointAdmission, driver: &mut JointProgressDriver) {
    drive(driver);
    let mut reply = receive(admission);
    assert!(reply.new_claims.is_empty());
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
fn domains(handle: &JointAdmissionHandle, scope: &WorkScope, facts: &[SharedDependency]) -> bool {
    let state = handle.inner.state.lock().unwrap();
    let node = &state.nodes[&scope.id];
    let id = state.cancellations.slots[node.cancellation.0]
        .claim
        .unwrap();
    state
        .joint
        .as_ref()
        .unwrap()
        .control_has_domains_for_test(id, facts)
}
fn finish(work: crate::RootWork, driver: &mut JointProgressDriver) {
    work.business.release();
    work.owner.complete();
    drive(driver);
}

#[test]
fn delegated_network_bind_union_and_original_child_full_domains() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let original = work.owner.control_claim_view().unwrap();
    let original_id = original.id;
    let a = network("control-a");
    let b = network("control-b");
    let mut initial = binding(&scope, &a);
    install(&scope, &mut initial, &mut driver);
    let selected = url::Url::parse("https://CONTROL-B.example:443/signed/path?part=1").unwrap();
    let mut union = scope.begin_control_binding().unwrap();
    union
        .push_selected_network(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
        .unwrap();
    // An already-admitted fact keeps the same canonical union, not a new ID.
    union.push_dependency(&a).unwrap();
    union.end_change().unwrap();
    let mut union = union.submit().unwrap();
    install(&scope, &mut union, &mut driver);
    assert!(domains(&handle, &scope, &[a.clone(), b.clone()]));
    assert_eq!(work.owner.control_claim_view().unwrap().id, original_id);
    let mut child = scope.request_joint_child().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut child);
    let receipt = reply.adjustment.as_mut().unwrap();
    assert!(matches!(
        receipt.install_control_scope_in(&scope),
        Err(WorkError::Conflict)
    ));
    let mut installed = None;
    receipt
        .install_child_in(&scope, &mut installed)
        .unwrap()
        .notify();
    let child = installed.take().unwrap();
    assert!(domains(&handle, &child.scope(), &[a, b]));
    drop(reply);
    child.complete();
    finish(work, &mut driver);
}

#[test]
fn delegated_scope_install_is_borrowed_exact_and_preserves_foreign_refusal() {
    let (control, _, mut driver) = fixture();
    let (foreign, _, mut foreign_driver) = fixture();
    let work = root(&control);
    let other = root(&control);
    let stranger = root(&foreign);
    let scope = work.owner.scope();
    let mut admission = binding(&scope, &network("exact"));
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    let receipt = reply.adjustment.as_mut().unwrap();
    let id = receipt.id;
    assert!(matches!(
        receipt.install_control_scope_in(&stranger.owner.scope()),
        Err(WorkError::ForeignAuthority)
    ));
    assert_eq!(receipt.id, id);
    assert!(matches!(
        receipt.install_control_scope_in(&other.owner.scope()),
        Err(WorkError::Conflict)
    ));
    assert_eq!(receipt.id, id);
    receipt.install_control_scope_in(&scope).unwrap().notify();
    assert!(receipt.id.is_none());
    drop(reply);
    finish(work, &mut driver);
    finish(other, &mut driver);
    finish(stranger, &mut foreign_driver);
}

#[test]
fn delegated_cancel_and_success_sealed_orphan_cannot_install_or_mint() {
    let (control, _, mut driver) = fixture();
    let cancelled = root(&control);
    let scope = cancelled.owner.scope();
    let mut admission = binding(&scope, &network("cancelled"));
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    cancelled
        .owner
        .cancel(crate::CancellationReason::ServerShutdown);
    drive(&mut driver);
    let receipt = reply.adjustment.as_mut().unwrap();
    assert!(matches!(
        receipt.install_control_scope_in(&scope),
        Err(WorkError::Cancelled(
            crate::CancellationReason::ServerShutdown
        ))
    ));
    assert!(receipt.id.is_some());
    assert!(matches!(
        scope.begin_control_binding(),
        Err(WorkError::Cancelled(
            crate::CancellationReason::ServerShutdown
        ))
    ));
    drop(reply);
    finish(cancelled, &mut driver);
    let orphan = root(&control);
    let scope = orphan.owner.scope();
    let mut admission = binding(&scope, &network("orphan"));
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    orphan.owner.success_sealer().seal().unwrap();
    drop(orphan.owner);
    // This scope is not cancelled: the separate Active-owner check is required.
    assert!(scope.check().is_ok());
    assert!(matches!(
        scope.begin_control_binding(),
        Err(WorkError::Conflict)
    ));
    assert!(matches!(
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope),
        Err(WorkError::Conflict)
    ));
    assert!(reply.adjustment.as_ref().unwrap().id.is_some());
    drop(reply);
    orphan.business.release();
    control.adopt(scope.id()).unwrap().complete();
    drive(&mut driver);
}

#[test]
fn delegated_binding_rechecks_active_owner_at_original_queue_publication() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let mut proposal = scope.begin_control_binding().unwrap();
    proposal.push_dependency(&network("before-orphan")).unwrap();
    proposal.end_change().unwrap();
    work.owner.success_sealer().seal().unwrap();
    drop(work.owner);
    assert!(scope.check().is_ok());
    assert!(matches!(
        proposal.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    work.business.release();
    control.adopt(scope.id()).unwrap().complete();
    drive(&mut driver);
}

#[test]
fn delegated_silent_install_survives_real_wake_panic_without_pinning_consumed_body() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct PanicOnce(AtomicBool);
    impl std::task::Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            if self.0.swap(false, Ordering::AcqRel) {
                panic!("intentional delegated binding notification panic")
            }
        }
    }
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let observer = work.owner.retirement_observer().unwrap();
    let fact = network("actual-body");
    let mut admission = binding(&scope, &fact);
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    let receipt = reply.adjustment.as_mut().unwrap();
    let notice = receipt.install_control_scope_in(&scope).unwrap();
    assert!(receipt.id.is_none());
    assert!(domains(&handle, &scope, &[fact]));
    assert!(catch_unwind(AssertUnwindSafe(|| notice.notify())).is_err());
    drop(progress);
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(
        observer.is_reclaimed(),
        Ok(true),
        "an installed reply has no remaining proposal borrow or physical claim"
    );
    drop(reply);
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(matches!(
        scope.begin_control_binding(),
        Err(WorkError::Released)
    ));
}

#[test]
fn delegated_parent_union_does_not_remint_an_old_child_fence() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let a = network("parent-old");
    let b = network("parent-new");
    let mut initial = binding(&scope, &a);
    install(&scope, &mut initial, &mut driver);
    let mut child_request = scope.request_joint_child().unwrap();
    drive(&mut driver);
    let mut old_child = receive(&mut child_request);
    let mut grow = binding(&scope, &b);
    install(&scope, &mut grow, &mut driver);
    let mut destination = None;
    assert!(matches!(
        old_child
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(&scope, &mut destination),
        Err(WorkError::Conflict)
    ));
    assert!(destination.is_none());
    drop(old_child);
    drive(&mut driver);
    let mut fresh = scope.request_joint_child().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut fresh);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&scope, &mut destination)
        .unwrap()
        .notify();
    let child = destination.take().unwrap();
    assert!(domains(&handle, &child.scope(), &[a, b]));
    drop(reply);
    child.complete();
    finish(work, &mut driver);
}

#[test]
fn delegated_retained_builder_cannot_submit_after_original_push_wake_unwind() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct PanicOnce(AtomicBool);
    impl std::task::Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            if self.0.swap(false, Ordering::AcqRel) {
                panic!("intentional delegated push wake panic")
            }
        }
    }
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let mut proposal = scope.begin_control_binding().unwrap();
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    let selected = url::Url::parse("https://push-wake.example/private?selected=1").unwrap();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        proposal.push_selected_network(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
    }));
    assert!(outcome.is_err());
    assert_eq!(proposal.end_change(), Err(WorkError::Conflict));
    assert!(matches!(
        proposal.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drop(progress);
    drive(&mut driver);
    assert!(domains(&handle, &scope, &[]));
    finish(work, &mut driver);
}

#[test]
fn root_drain_keeps_original_control_binding_at_each_publication_cut() {
    for cut in 0..3 {
        let (control, handle, mut driver) = fixture();
        let work = root(&control);
        let scope = work.owner.scope();
        let original_id = work.owner.control_claim_view().unwrap().id;
        let fact = network("draining-original");
        if cut == 0 {
            control.close_root_admission();
        }
        let mut admission = binding(&scope, &fact);
        if cut == 1 {
            control.close_root_admission();
        }
        drive(&mut driver);
        if cut == 2 {
            control.close_root_admission();
        }
        let mut reply = receive(&mut admission);
        assert!(reply.new_claims.is_empty());
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop(reply);
        drive(&mut driver);
        assert!(domains(&handle, &scope, &[fact]));
        assert_eq!(work.owner.control_claim_view().unwrap().id, original_id);
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::Closed)
        ));
        finish(work, &mut driver);
    }
}

#[test]
fn delegated_closed_install_keeps_receipt_and_original_fixed_base() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let mut admission = binding(&scope, &network("close-after-grant"));
    drive(&mut driver);
    let mut reply = receive(&mut admission);
    let base = {
        let state = handle.inner.state.lock().unwrap();
        let node = &state.nodes[&scope.id];
        state.cancellations.slots[node.cancellation.0].base
    };
    control.close_admission();
    let receipt = reply.adjustment.as_mut().unwrap();
    assert!(matches!(
        receipt.install_control_scope_in(&scope),
        Err(WorkError::Closed)
    ));
    assert!(receipt.id.is_some());
    {
        let state = handle.inner.state.lock().unwrap();
        let node = &state.nodes[&scope.id];
        let cell = &state.cancellations.slots[node.cancellation.0];
        let claim = &state.joint.as_ref().unwrap().claims[&cell.claim.unwrap()];
        assert_eq!(cell.base, base);
        assert_eq!(claim.amounts, base);
        assert_eq!(claim.active_amounts, base);
    }
    drop(reply);
    finish(work, &mut driver);
}

#[test]
fn delegated_original_revision_and_complete_monotone_union_are_preserved() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let mut stale = scope.begin_control_binding().unwrap();
    stale.push_dependency(&network("stale")).unwrap();
    stale.end_change().unwrap();
    let mut current = binding(&scope, &network("current"));
    install(&scope, &mut current, &mut driver);
    assert!(matches!(
        stale.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drive(&mut driver);
    let mut grow = binding(&scope, &network("second"));
    install(&scope, &mut grow, &mut driver);
    let mut third = binding(&scope, &network("third"));
    install(&scope, &mut third, &mut driver);
    assert!(domains(
        &handle,
        &scope,
        &[network("current"), network("second"), network("third")]
    ));
    finish(work, &mut driver);
}

#[test]
fn delegated_builders_poison_partial_repeated_end_and_push_after_end() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let mut empty = scope.begin_control_binding().unwrap();
    assert_eq!(empty.end_change(), Err(WorkError::Conflict));
    assert!(matches!(
        empty.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drive(&mut driver);
    let unfinished = scope.begin_control_binding().unwrap();
    assert!(matches!(
        unfinished.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drive(&mut driver);
    let mut ended = scope.begin_control_binding().unwrap();
    ended.push_dependency(&network("one")).unwrap();
    ended.end_change().unwrap();
    assert_eq!(ended.end_change(), Err(WorkError::Conflict));
    assert!(matches!(
        ended.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    drive(&mut driver);
    let mut after_end = scope.begin_control_binding().unwrap();
    after_end.push_dependency(&network("one")).unwrap();
    after_end.end_change().unwrap();
    assert_eq!(
        after_end.push_dependency(&network("two")),
        Err(WorkError::Conflict)
    );
    assert!(matches!(
        after_end.submit(),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    finish(work, &mut driver);
}
