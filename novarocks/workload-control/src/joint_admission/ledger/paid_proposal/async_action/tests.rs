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

//! Real original fixed ledger/proof; no physical worker/profile qualification.
use super::*;
use crate::{ControlStorageConfig, WorkloadConfig};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let count = 8;
    let storage = ControlStorageConfig::new(count, &[]);
    let config = ResponsibilityConfig {
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (handle, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
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

fn install(
    action: &mut AsyncActionAdmission,
    driver: &mut JointProgressDriver,
) -> AsyncActionGrant {
    drive(driver);
    let mut target = None;
    assert!(matches!(
        action.poll_install_in(&mut Context::from_waker(Waker::noop()), &mut target),
        Poll::Ready(Ok(()))
    ));
    target.unwrap()
}
#[test]
fn async_source_preserves_complete_facts_and_occupied_destination() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let a = fact("catalog");
    let b = fact("storage");
    bind(&scope, &[a.clone(), b.clone()], &mut driver);
    let mut first = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let mut target = Some(install(&mut first, &mut driver));
    let id = target.as_ref().unwrap().claims[0].id;
    let mut second = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    assert!(matches!(
        second.poll_install_in(&mut Context::from_waker(Waker::noop()), &mut target),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    assert_eq!(target.as_ref().unwrap().claims[0].id, id);
    let state = control.inner.state.lock().unwrap();
    let row = &state.joint.as_ref().unwrap().claims[&id.unwrap()];
    assert_eq!(row.owner, ClaimOwner::PhysicalWork(scope.id));
    assert_eq!(row.active_amounts.get(JointResource::AsyncIo), 1);
    assert_eq!(row.active_facts.dependencies.len(), 2);
    drop(state);
    drop(target);
    drop(second);
    drive(&mut driver);
}
#[test]
fn original_async_fence_refuses_stale_foreground_and_preserves_settlement() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let stale = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap();
    bind(&scope, &[fact("storage")], &mut driver);
    assert!(matches!(
        stale.begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap()),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(
        work.owner
            .async_action_source(JointLane::Foreground)
            .is_err()
    );
    let mut admitted = work
        .owner
        .async_action_source(JointLane::Settlement)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let grant = install(&mut admitted, &mut driver);
    assert_eq!(grant.claims.len(), 1);
    drop(grant);
    drive(&mut driver);
}
#[test]
fn root_payload_rekeys_live_async_rows_without_child_or_payload_pin_replacement() {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let mut initial = work
        .owner
        .begin_root_payload(NonZeroU64::new(1).unwrap(), NonZeroU64::new(8).unwrap())
        .unwrap();
    drive(&mut driver);
    let mut payload = None;
    match initial.poll_install_in(
        &mut Context::from_waker(Waker::noop()),
        &scope,
        &mut payload,
    ) {
        Poll::Ready(Ok(n)) => n.notify(),
        _ => panic!("initial original payload"),
    }
    drop(initial);
    drive(&mut driver);
    let payload = payload.unwrap();
    let alias = payload.retain_alias().unwrap();
    let mut action = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    let grant = install(&mut action, &mut driver);
    drop(action);
    drive(&mut driver);
    let id = grant.claims[0].id.unwrap();
    let before = {
        let state = control.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .iter()
            .find_map(|(id, r)| match r.custody {
                ClaimCustody::Payload { epoch, pins } => Some((*id, epoch, pins)),
                _ => None,
            })
            .unwrap()
    };
    let active = [&grant];
    let mut proposal = payload
        .root_payload_source(&scope, JointLane::Foreground, &active)
        .unwrap()
        .begin(4)
        .unwrap();
    proposal.push_dependency(&fact("new-storage")).unwrap();
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original Root batch"),
    };
    ready
        .install_in(&scope, &payload, &active)
        .unwrap()
        .notify();
    assert_eq!(alias.capacity_bytes().unwrap(), 12);
    let state = control.inner.state.lock().unwrap();
    let ledger = state.joint.as_ref().unwrap();
    let row = &ledger.claims[&before.0];
    assert!(
        matches!(row.custody,ClaimCustody::Payload{epoch,pins} if epoch==before.1&&pins==before.2)
    );
    assert_eq!(ledger.claims[&id].active_facts.dependencies.len(), 2);
    assert_eq!(state.nodes.len(), 1);
    drop(state);
    drop(ready);
    drop(pending);
    drop(grant);
    drop(alias);
    drop(payload);
    drive(&mut driver);
}

#[test]
fn eight_original_async_requests_keep_independent_grants_without_a_q_gate() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let source = work
        .owner
        .async_action_source(JointLane::Foreground)
        .unwrap();
    let mut admissions = Vec::new();
    for _ in 0..8 {
        admissions.push(
            match source
                .try_begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
                .unwrap()
            {
                JointTryBegin::Started(value) => value,
                JointTryBegin::Full(_) => panic!("responsibility construction has no fixed Q gate"),
            },
        );
    }
    let marker = control.progress_revision();
    let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake));
    let mut cx = Context::from_waker(&waker);
    // Register all original reply wakers, then observe them again without
    // inventing a slot wait or changing the original service revision.
    for _ in 0..2 {
        for admission in &mut admissions {
            let mut target = None;
            assert!(admission.poll_install_in(&mut cx, &mut target).is_pending());
        }
    }
    assert_eq!(control.progress_revision(), marker);
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    drive(&mut driver);
    assert!(wake.0.load(Ordering::SeqCst) > 0);
    let mut grants = Vec::new();
    for admission in &mut admissions {
        grants.push(install(admission, &mut driver));
    }
    // Completed admission objects remain alive while all eight exact grants
    // retain their independent I/O responsibilities.
    let async_count = || {
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
            .map(|row| row.active_amounts.get(JointResource::AsyncIo))
            .sum::<u64>()
    };
    assert_eq!(grants.len(), 8);
    assert_eq!(async_count(), 8);
    drop(grants);
    drive(&mut driver);
    assert_eq!(async_count(), 0);
    drop(admissions);
    work.business.release();
    work.owner.complete();
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn root_initial_and_growth_preserve_original_epoch_and_aliases_without_a_q_gate() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    bind(&scope, &[fact("catalog")], &mut driver);
    let first = handle.begin_paid_proposal(&work.owner).unwrap();
    let second = handle.begin_paid_proposal(&work.owner).unwrap();
    let source = work.owner.root_payload_initial_source().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut initial = match source
        .try_begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(8).unwrap())
        .unwrap()
    {
        JointTryBegin::Started(value) => value,
        JointTryBegin::Full(_) => panic!("same initial source after real reap"),
    };
    drive(&mut driver);
    let mut payload = None;
    match initial.poll_install_in(&mut cx, &scope, &mut payload) {
        Poll::Ready(Ok(notification)) => notification.notify(),
        _ => panic!("real initial receipt"),
    }
    drop(initial);
    drive(&mut driver);
    let payload = payload.unwrap();
    let alias = payload.retain_alias().unwrap();
    let before = {
        let state = control.inner.state.lock().unwrap();
        state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .iter()
            .find_map(|(id, row)| match row.custody {
                ClaimCustody::Payload { epoch, pins } => Some((*id, epoch, pins)),
                _ => None,
            })
            .unwrap()
    };
    let third = handle.begin_paid_proposal(&work.owner).unwrap();
    // Overflow is an actual typed input refusal, never a synthetic Q Full.
    assert!(matches!(
        payload
            .root_payload_source(&scope, JointLane::Foreground, &[])
            .unwrap()
            .try_begin(u64::MAX),
        Err(WorkError::ArithmeticOverflow)
    ));
    drop(third);
    let mut proposal = match payload
        .root_payload_source(&scope, JointLane::Foreground, &[])
        .unwrap()
        .try_begin(4)
        .unwrap()
    {
        JointTryBegin::Started(value) => value,
        JointTryBegin::Full(_) => panic!("same stable payload after real reap"),
    };
    proposal.push_dependency(&fact("new-storage")).unwrap();
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut cx) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("real growth receipt"),
    };
    ready.install_in(&scope, &payload, &[]).unwrap().notify();
    assert_eq!(alias.capacity_bytes().unwrap(), 12);
    let state = control.inner.state.lock().unwrap();
    assert!(
        matches!(state.joint.as_ref().unwrap().claims[&before.0].custody,
        ClaimCustody::Payload {epoch,pins} if epoch == before.1 && pins == before.2)
    );
    drop(state);
    drop(ready);
    drop(pending);
    drop((first, second));
    drop(alias);
    drop(payload);
    drive(&mut driver);
}
