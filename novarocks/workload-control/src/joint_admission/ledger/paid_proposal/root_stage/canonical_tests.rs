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

//! Real paid builders, original driver and typed grants; no invented grant.
use super::*;
use crate::joint_admission::{
    NativeAsyncActionAdmission, NativeAsyncActionGrant, NativeAsyncActionQuote,
    NativeDispatchSource, NativeQueuedInputGrant, NativeQueuedInputQuote, NativeReplyGrant,
    RootDomainBinding, RootPayloadAdmission, RootPayloadReady,
};
use std::num::NonZeroU64;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

fn canonical_setup() -> (
    WorkloadControl,
    JointProgressDriver,
    crate::RootWork,
    WorkOwner,
    RootDomainBinding,
    RetainedPayloadGrant,
) {
    let (control, _, mut driver) = fixture();
    let work = root(&control);
    let domains = work.owner.scope().prepare_root_domains().unwrap();
    let source = domains
        .initial_payload_source(&work.owner.scope(), JointLane::Custody)
        .unwrap();
    let mut proposal = match source.try_begin(nz(1), nz(8)).unwrap() {
        JointTryBegin::Started(p) => p,
        _ => panic!("original initial Q available"),
    };
    proposal.push_dependency(&fact("a")).unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut payload = None;
    let notification = match pending.poll_install_in(
        &mut Context::from_waker(Waker::noop()),
        &work.owner.scope(),
        &mut payload,
    ) {
        Poll::Ready(Ok(n)) => n,
        _ => panic!("original Root initial proof"),
    };
    notification.notify();
    drop(pending);
    drive(&mut driver);
    let stage = child(&work.owner.scope(), &mut driver);
    let mut binding = stage
        .scope()
        .begin_inherited_control_binding(&domains, &[fact("s")])
        .unwrap();
    binding.end_change().unwrap();
    let mut pending = binding.submit().unwrap();
    drive(&mut driver);
    let mut reply = receive(&mut pending);
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&stage.scope())
        .unwrap()
        .notify();
    drop((reply, pending));
    drive(&mut driver);
    (control, driver, work, stage, domains, payload.unwrap())
}
fn growth(payload: &RetainedPayloadGrant, root: &WorkScope, name: &str) -> RootPayloadAdmission {
    let source = payload
        .root_domain_source(root, JointLane::Foreground)
        .unwrap();
    let mut proposal = match source.try_begin(0).unwrap() {
        JointTryBegin::Started(p) => p,
        _ => panic!("same original Root Q available"),
    };
    proposal.push_dependency(&fact(name)).unwrap();
    proposal.end_change().unwrap();
    proposal.submit().unwrap()
}
fn growth_ready(pending: &mut RootPayloadAdmission) -> RootPayloadReady {
    match Pin::new(pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(r)) => r,
        _ => panic!("original Root growth proof"),
    }
}
fn domain_epoch(control: &WorkloadControl, root: &WorkScope) -> u64 {
    control.inner.state.lock().unwrap().nodes[&root.id]
        .root_domains
        .as_ref()
        .unwrap()
        .epoch
}
fn effective(control: &WorkloadControl, id: u64) -> Vec<u64> {
    let mut capture = {
        let state = control.inner.state.lock().unwrap();
        root_domains::FactCapture::capture(
            &state,
            Arc::clone(&state.joint.as_ref().unwrap().claims[&id].facts),
            None,
        )
        .unwrap()
    };
    loop {
        if let Poll::Ready(facts) = capture.step().unwrap() {
            return facts.key.dependencies.clone();
        }
    }
}
fn control_id(control: &WorkloadControl, work: &WorkScope) -> u64 {
    let state = control.inner.state.lock().unwrap();
    state.cancellations.slots[state.nodes[&work.id].cancellation.0]
        .claim
        .unwrap()
}
fn positive_native(
    control: &WorkloadControl,
    driver: &mut JointProgressDriver,
    work: &crate::RootWork,
    stage: &WorkOwner,
    payload: &RetainedPayloadGrant,
) -> (
    NativeAsyncActionAdmission,
    JointGrantedClaims,
    NativeAsyncActionGrant,
    NativeReplyGrant,
    NativeQueuedInputGrant,
    u64,
    u64,
) {
    let alias = crate::ResultWindowAlias::from_internal_payload(
        &work.owner.scope(),
        &stage.scope(),
        payload,
    )
    .unwrap();
    let authority = alias.native_action_authority().unwrap();
    let mut input_pending = authority
        .begin_queued_input(
            JointLane::Foreground,
            NativeQueuedInputQuote::for_original_native_input(nz(1), 1),
        )
        .unwrap();
    drive(driver);
    let mut input_ready = match input_pending
        .poll_with_slot_observer(&mut Context::from_waker(Waker::noop()), &mut |_, _| {
            Poll::Pending
        }) {
        Poll::Ready(Ok(r)) => r,
        _ => panic!("original queued input grant"),
    };
    let mut input = None;
    input_ready.install_in(&mut input).unwrap().notify();
    drop((input_ready, input_pending));
    drive(driver);
    let quote = NativeAsyncActionQuote::for_original_native_transport(
        nz(1),
        nz(NativeDispatchSource::metadata_backing_bytes(1).unwrap() as u64),
        nz(1),
        nz(1),
        nz(1),
        nz(1),
    );
    let mut pending = authority
        .action_source(JointLane::Foreground, quote)
        .unwrap()
        .begin()
        .unwrap();
    drive(driver);
    let original_ticket = {
        let state = control.inner.state.lock().unwrap();
        *state
            .joint
            .as_ref()
            .unwrap()
            .requests
            .keys()
            .next_back()
            .unwrap()
    };
    let mut offer = match pending
        .poll_local_offer(&mut Context::from_waker(Waker::noop()), &mut |_, _| {
            Poll::Pending
        }) {
        Poll::Ready(Ok(o)) => o,
        _ => panic!("original Native local offer"),
    };
    let mut physical = Some(());
    let mut accepted = None;
    offer
        .accept_in(&mut physical, &mut accepted)
        .unwrap()
        .notify();
    drop(offer);
    let mut physical = None;
    let mut claims = None;
    assert!(
        accepted
            .take()
            .unwrap()
            .transfer_in(&mut physical, &mut claims)
            .is_ok()
    );
    let mut claims = claims.unwrap();
    // Original all-New body order is Action, Reply; pop takes Reply first.
    let mut reply_raw = claims.pop();
    let mut action_raw = claims.pop();
    let action_id = action_raw.as_ref().unwrap().view().unwrap().id;
    let mut action = None;
    let mut reply = None;
    pending
        .adopt_action_in(&mut action_raw, &mut action)
        .unwrap()
        .notify();
    pending
        .adopt_reply_in(&mut reply_raw, &mut reply)
        .unwrap()
        .notify();
    (
        pending,
        claims,
        action.unwrap(),
        reply.unwrap(),
        input.unwrap(),
        action_id,
        original_ticket,
    )
}
fn remaining(control: &WorkloadControl, action: u64) -> u64 {
    control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims[&action]
        .native_dispatch
        .unwrap()
        .ticket
}

#[test]
fn canonical_root_growth_keeps_stage_local_s_and_rolls_back_live_groups() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let root_id = control_id(&control, &work.owner.scope());
    let stage_id = control_id(&control, &stage.scope());
    let old_root = effective(&control, root_id);
    let old_stage = effective(&control, stage_id);
    assert_eq!(old_root.len(), 1);
    assert_eq!(
        old_stage.len(),
        2,
        "original Stage-only S is preserved at birth"
    );
    let original_epoch = domain_epoch(&control, &work.owner.scope());
    let mut pending = growth(&payload, &work.owner.scope(), "b");
    drive(&mut driver);
    let proof = growth_ready(&mut pending);
    assert_eq!(effective(&control, root_id).len(), 2);
    assert_eq!(effective(&control, stage_id).len(), 3);
    assert_eq!(domain_epoch(&control, &work.owner.scope()), original_epoch);
    drop(proof);
    drop(pending);
    drive(&mut driver);
    assert_eq!(effective(&control, root_id), old_root);
    assert_eq!(effective(&control, stage_id), old_stage);
    let mut pending = growth(&payload, &work.owner.scope(), "b");
    drive(&mut driver);
    let mut proof = growth_ready(&mut pending);
    proof
        .install_domains_in(&work.owner.scope(), &payload)
        .unwrap()
        .notify();
    assert_eq!(
        domain_epoch(&control, &work.owner.scope()),
        original_epoch + 1
    );
    assert_eq!(effective(&control, stage_id).len(), 3);
    // No unselected C was invented by the new canonical author.
    let c = fact("c");
    let state = control.inner.state.lock().unwrap();
    assert!(
        !state.nodes[&work.owner.scope().id]
            .root_domains
            .as_ref()
            .unwrap()
            .active
            .iter()
            .any(|d| d.fact == c)
    );
}

#[test]
fn canonical_pending_root_does_not_authorize_new_inherited_birth_before_rollback() {
    let (control, mut driver, work, _stage, _domains, payload) = canonical_setup();
    let mut pending = growth(&payload, &work.owner.scope(), "b");
    drive(&mut driver);
    let proof = growth_ready(&mut pending);
    let mut child = work.owner.scope().request_joint_child().unwrap();
    let ticket = {
        let state = control.inner.state.lock().unwrap();
        *state.joint.as_ref().unwrap().requests.keys().max().unwrap()
    };
    drive(&mut driver);
    assert!(
        Pin::new(&mut child)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop((proof, pending));
    drive(&mut driver);
    let mut reply = receive(&mut child);
    let mut installed = None;
    reply
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&work.owner.scope(), &mut installed)
        .unwrap()
        .notify();
    drop((reply, child));
    drive(&mut driver);
    assert!(
        !control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .requests
            .contains_key(&ticket)
    );
    let id = control_id(&control, &installed.as_ref().unwrap().scope());
    assert_eq!(
        effective(&control, id).len(),
        1,
        "same Q is reproved against restored active A"
    );
}

#[test]
fn native_dispatch_official_epoch_reobserve_keeps_original_ticket() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let (_admission, _body, action, reply, input, id, original) =
        positive_native(&control, &mut driver, &work, &stage, &payload);
    let mut source =
        NativeDispatchSource::capture(&action, &reply, [Some(&input)].into_iter()).unwrap();
    assert!(
        Pin::new(&mut source)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let before_epoch = remaining(&control, id);
    assert_eq!(before_epoch, original);
    let mut pending = growth(&payload, &work.owner.scope(), "b");
    drive(&mut driver);
    let mut proof = growth_ready(&mut pending);
    assert!(
        Pin::new(&mut source)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(
        remaining(&control, id),
        before_epoch,
        "original event wait does not remint the request"
    );
    proof
        .install_domains_in(&work.owner.scope(), &payload)
        .unwrap()
        .notify();
    drop((proof, pending));
    let frozen = loop {
        match Pin::new(&mut source).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(frozen)) => break frozen,
            Poll::Ready(Err(error)) => panic!("official epoch reobserve: {error:?}"),
            Poll::Pending => assert_eq!(remaining(&control, id), before_epoch),
        }
    };
    assert_eq!(frozen.epoch(), domain_epoch(&control, &work.owner.scope()));
    assert_eq!(remaining(&control, id), before_epoch);
    assert_eq!(
        effective(&control, id).len(),
        3,
        "freeze contains A, B and original Stage S"
    );
    let frozen_d = effective(&control, id);
    // B's reply aliases exited above; its original fixed Q still needs the
    // driver to reap that backing. Keep Native's original body until exit.
    drive(&mut driver);
    let mut pending = growth(&payload, &work.owner.scope(), "c");
    drive(&mut driver);
    let mut proof = growth_ready(&mut pending);
    proof
        .install_domains_in(&work.owner.scope(), &payload)
        .unwrap()
        .notify();
    assert_eq!(
        effective(&control, id),
        frozen_d,
        "the already dispatched finite action remains at actual B"
    );
    assert!(NativeDispatchSource::capture(&action, &reply, [Some(&input)].into_iter()).is_err());
}

#[test]
fn canonical_manifest_updates_existing_computation_only_at_original_install() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let mut proposal = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap()
        .begin(0, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut reply = ready(&mut pending);
    let mut compute = None;
    reply
        .install_in(&work.owner.scope(), &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop((reply, pending));
    drive(&mut driver);
    let alias = crate::ResultWindowAlias::from_internal_computation(
        &work.owner.scope(),
        &stage,
        &payload,
        compute.as_ref().unwrap(),
    )
    .unwrap();
    let binding = alias.internal_manifest_binding().unwrap().unwrap();
    let compute_id = {
        let state = control.inner.state.lock().unwrap();
        *state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .iter()
            .find(|(_, row)| matches!(row.custody, ClaimCustody::Computation { .. }))
            .unwrap()
            .0
    };
    let before = effective(&control, compute_id);
    let epoch = domain_epoch(&control, &work.owner.scope());
    for commit in [false, true] {
        let mut proposal = match binding.try_begin().unwrap() {
            JointTryBegin::Started(p) => p,
            _ => panic!("original manifest Q"),
        };
        proposal.end_payload().unwrap();
        proposal.push_stage_dependency(&fact("backend-b")).unwrap();
        proposal.end_stage().unwrap();
        let mut pending = proposal.submit().unwrap();
        drive(&mut driver);
        let mut reply = ready(&mut pending);
        assert_eq!(domain_epoch(&control, &work.owner.scope()), epoch);
        assert_eq!(capacity(&control, JointResource::Computations), 1);
        if commit {
            reply.install_binding_in(binding).unwrap().notify();
        }
        drop((reply, pending));
        drive(&mut driver);
        if commit {
            assert_eq!(domain_epoch(&control, &work.owner.scope()), epoch + 1);
            assert_eq!(effective(&control, compute_id).len(), before.len() + 1);
        } else {
            assert_eq!(effective(&control, compute_id), before);
        }
        assert_eq!(capacity(&control, JointResource::Computations), 1);
    }
}

#[test]
fn canonical_retired_root_graph_survives_until_original_fixed_backing_reset() {
    let (control, mut driver, work, stage, domains, payload) = canonical_setup();
    let root_id = work.owner.scope().id;
    let claim = control_id(&control, &work.owner.scope());
    let observer = work.owner.scope().cancellation().unwrap();
    let before = effective(&control, claim);
    drop((payload, domains));
    stage.complete();
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert!(
        !control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&root_id),
        "observer does not keep logical Work alive"
    );
    assert_eq!(
        effective(&control, claim),
        before,
        "actual retired backing remains charged to original D"
    );
    drop(observer);
    drive(&mut driver);
    assert!(
        effective(&control, claim).is_empty(),
        "only actual reset returns the empty process pool slot"
    );
}

#[test]
fn canonical_uninstalled_computation_cancel_retires_private_new_before_root_rollback() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let root_id = control_id(&control, &work.owner.scope());
    let before = effective(&control, root_id);
    let mut proposal = payload
        .root_stage_source(&work.owner.scope(), &stage)
        .unwrap()
        .begin(0, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.push_stage_dependency(&fact("backend-b")).unwrap();
    proposal.end_stage().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let ready = ready(&mut pending);
    assert_eq!(capacity(&control, JointResource::Computations), 1);
    // Driver shutdown selects the actual same request failure while the sealed
    // Ready still holds its raw. Ordinary externally delivered New is unchanged.
    drop(driver);
    assert_eq!(capacity(&control, JointResource::Computations), 0);
    assert_eq!(effective(&control, root_id), before);
    drop((ready, pending));
}

fn original_manifest_binding(
    driver: &mut JointProgressDriver,
    root: &WorkScope,
    stage: &WorkOwner,
    payload: &RetainedPayloadGrant,
) -> (
    crate::joint_admission::InternalComputationGrant,
    InternalManifestBinding,
) {
    let mut proposal = payload
        .root_stage_source(root, stage)
        .unwrap()
        .begin(0, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal.end_stage().unwrap();
    let mut admission = proposal.submit().unwrap();
    drive(driver);
    let mut ready = ready(&mut admission);
    let mut compute = None;
    ready
        .install_in(root, payload, stage, &mut compute)
        .unwrap()
        .notify();
    drop((ready, admission));
    drive(driver);
    let compute = compute.unwrap();
    let binding = InternalManifestBinding::from_grants(root, stage, payload, &compute).unwrap();
    (compute, binding)
}

#[derive(Default)]
struct RootWriterWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for RootWriterWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn canonical_mio_charged_payload_parks_same_manifest_binding_until_install() {
    let (_control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let root = work.owner.scope();
    let (_compute, binding) = original_manifest_binding(&mut driver, &root, &stage, &payload);
    let mut mio = match payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap()
        .try_begin(1)
        .unwrap()
    {
        JointTryBegin::Started(p) => p,
        _ => panic!("original MIO constructor is available"),
    };
    let mut building_wait = match binding.try_begin().unwrap() {
        JointTryBegin::Full(signal) => signal.into_wait(),
        _ => panic!("manifest must not overtake MIO Building"),
    };
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut building_wait).poll(&mut cx).is_pending());
    mio.push_dependency(&fact("fs-b")).unwrap();
    mio.end_change().unwrap();
    let mut admission = mio.submit().unwrap();
    drive(&mut driver);
    let mut payload_ready = growth_ready(&mut admission);
    // This capture happens AFTER payload charge changed its structure/facts.
    let charged_signal = match binding.try_begin().unwrap() {
        JointTryBegin::Full(signal) => signal,
        _ => panic!("charged original payload is not another writer grant"),
    };
    assert!(building_wait.is_for_signal(&charged_signal));
    assert!(Pin::new(&mut building_wait).poll(&mut cx).is_pending());
    payload_ready
        .install_domains_in(&root, &payload)
        .unwrap()
        .notify();
    assert!(matches!(
        Pin::new(&mut building_wait).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop((payload_ready, admission, building_wait));
    drive(&mut driver);
    assert_eq!(payload.capacity_bytes().unwrap(), 9);
    let mut manifest = match binding.try_begin().unwrap() {
        JointTryBegin::Started(p) => p,
        _ => panic!("same binding reobserves the installed original payload"),
    };
    manifest
        .push_payload_dependency(&fact("backend-c"))
        .unwrap();
    manifest.end_payload().unwrap();
    manifest.end_stage().unwrap();
    let mut admission = manifest.submit().unwrap();
    drive(&mut driver);
    let mut ready = ready(&mut admission);
    ready.install_binding_in(&binding).unwrap().notify();
    assert_eq!(payload.capacity_bytes().unwrap(), 9);
}

#[test]
fn canonical_root_writer_drop_and_completed_capture_reobserve_keep_original_fences() {
    let (_control, mut driver, work, _stage, _domains, payload) = canonical_setup();
    let root = work.owner.scope();
    let old_source = payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap();
    let building = match payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap()
        .try_begin(0)
        .unwrap()
    {
        JointTryBegin::Started(p) => p,
        _ => panic!("original writer slot"),
    };
    let mut wait = match payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap()
        .try_begin(0)
        .unwrap()
    {
        JointTryBegin::Full(wait) => wait,
        _ => panic!("Building owns the same writer position"),
    };
    let wakes = Arc::new(RootWriterWake::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    drop(building);
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(matches!(
        Pin::new(&mut wait).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    // Even an abandoned official construction advances the capture stamp.
    let mut reobserve = match old_source.try_begin(0).unwrap() {
        JointTryBegin::Full(wait) => wait,
        _ => panic!("an old capture must reobserve, not retain a stale writer stamp"),
    };
    assert!(matches!(
        Pin::new(&mut reobserve).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop((wait, reobserve));
    drive(&mut driver);
    let original = payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap();
    drop(payload);
    assert!(
        matches!(original.try_begin(0), Err(WorkError::Released)),
        "ordinary original-holder loss remains Released"
    );
}

#[test]
fn canonical_root_writer_wait_is_independent_of_generic_constructors_and_cancellation_wins() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let root = work.owner.scope();
    let (_compute, binding) = original_manifest_binding(&mut driver, &root, &stage, &payload);
    let building = match binding.try_begin().unwrap() {
        JointTryBegin::Started(p) => p,
        _ => panic!("first paid Q"),
    };
    let handle = JointAdmissionHandle {
        inner: Arc::clone(&control.inner),
    };
    let ordinary = handle.begin_paid_proposal(&work.owner).unwrap();
    let independent = match handle
        .try_begin_paid_proposal_for_scope_signal(root.clone())
        .unwrap()
    {
        JointTryBegin::Started(value) => value,
        JointTryBegin::Full(_) => panic!("generic construction has no Q gate"),
    };
    drop(independent);
    let writer_signal = match binding.try_begin().unwrap() {
        JointTryBegin::Full(signal) => signal,
        _ => panic!("same original writer still Building"),
    };
    let mut writer_wait = writer_signal.into_wait();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut writer_wait).poll(&mut cx).is_pending());
    drop(ordinary);
    drive(&mut driver);
    assert!(
        Pin::new(&mut writer_wait).poll(&mut cx).is_pending(),
        "free Q does not grant the occupied Root writer"
    );
    work.owner.cancel(crate::CancellationReason::Requested);
    assert!(matches!(
        Pin::new(&mut writer_wait).poll(&mut cx),
        Poll::Ready(Err(WorkError::Cancelled(
            crate::CancellationReason::Requested
        )))
    ));
    drop(building);
}

#[test]
fn canonical_manifest_building_and_pending_serialize_same_mio_root_without_selfwake() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let root = work.owner.scope();
    let (_compute, binding) = original_manifest_binding(&mut driver, &root, &stage, &payload);
    let mut manifest = match binding.try_begin().unwrap() {
        JointTryBegin::Started(p) => p,
        _ => panic!("original manifest constructor is available"),
    };
    let mut wait = match payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap()
        .try_begin(0)
        .unwrap()
    {
        JointTryBegin::Full(wait) => wait,
        _ => panic!("the same Root writer is still Building"),
    };
    let wakes = Arc::new(RootWriterWake::default());
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.joint.as_ref().unwrap().requests.is_empty());
    }
    for _ in 0..4 {
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    }
    assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    manifest
        .push_payload_dependency(&fact("backend-b"))
        .unwrap();
    manifest.end_payload().unwrap();
    manifest.end_stage().unwrap();
    let mut admission = manifest.submit().unwrap();
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    drive(&mut driver);
    let proof = ready(&mut admission);
    assert!(
        Pin::new(&mut wait).poll(&mut cx).is_pending(),
        "Ready is not installed Root D"
    );
    let before_drop = wakes.0.load(std::sync::atomic::Ordering::SeqCst);
    drop((proof, admission));
    assert!(
        wakes.0.load(std::sync::atomic::Ordering::SeqCst) > before_drop,
        "original rollback notifies its parked source"
    );
    assert!(matches!(
        Pin::new(&mut wait).poll(&mut cx),
        Poll::Ready(Ok(()))
    ));
    drop(wait);
    drive(&mut driver);
    let mut mio = growth(&payload, &root, "fs-c");
    drive(&mut driver);
    let mut ready = growth_ready(&mut mio);
    ready.install_domains_in(&root, &payload).unwrap().notify();
}

#[test]
fn native_dispatch_preserves_original_ticket_and_drop_retains_original_action() {
    let (control, mut driver, work, stage, _domains, payload) = canonical_setup();
    let (_admission, _body, action, reply, input, id, original) =
        positive_native(&control, &mut driver, &work, &stage, &payload);
    assert_eq!(remaining(&control, id), original);
    let mut source =
        NativeDispatchSource::capture(&action, &reply, [Some(&input)].into_iter()).unwrap();
    let after_capture = remaining(&control, id);
    assert_eq!(after_capture, original);
    assert!(
        Pin::new(&mut source)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(remaining(&control, id), after_capture);
    drop(source);
    let state = control.inner.state.lock().unwrap();
    let row = &state.joint.as_ref().unwrap().claims[&id];
    assert_eq!(row.dispatch_staging, None);
    assert!(!row.native_dispatch.unwrap().frozen);
    assert_eq!(
        row.active_amounts.get(JointResource::AsyncIo),
        1,
        "preparer exit cannot refund the real action"
    );
}
