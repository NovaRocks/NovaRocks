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

//! Actual canonical Root/Stage receipts and the closed Production author.
use super::*;
use crate::{ControlStorageConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig};
use std::task::Waker;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}
fn route(name: &str) -> SharedDependency {
    SharedDependency::Network(
        novarocks_resource_contract::NetworkServiceOrigin::from_selected_url(
            &url::Url::parse(&format!("https://{name}.example")).unwrap(),
        )
        .unwrap(),
    )
}
fn drive(driver: &mut JointProgressDriver) {
    crate::joint_admission::tests::drive(driver);
}
fn reply(admission: &mut JointAdmission) -> JointGrantedBatch {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(batch)) => batch,
        _ => panic!("original paid reply"),
    }
}
fn fixture() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let storage = ControlStorageConfig::new(8, &[]);
    let config = ResponsibilityConfig {
        work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
    };
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 8,
        business_limit: 8,
        query_concurrency_limit: 8,
        scope_records_limit: 8,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (handle, driver) = control.configure_responsibility(config).unwrap();
    control.configure_control_storage(storage).unwrap();
    // The canonical production fixture creates actual RootStage computations.
    // Install their original result owner, independently of responsibility rows.
    control
        .configure_result_capacity(crate::ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}

struct Producer {
    work: RootWork,
    stage: WorkOwner,
    payload: RetainedPayloadGrant,
    compute: InternalComputationGrant,
}
fn producer(
    control: &WorkloadControl,
    driver: &mut JointProgressDriver,
    provider: &str,
) -> Producer {
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let root = work.owner.scope();
    // This is a real FirstBind before canonical preparation, not a test-only
    // mutation of control facts. Both catalogs use the same healthy Host S.
    let mut bind = root.begin_control_binding().unwrap();
    bind.push_dependency(&SharedDependency::LocalHost).unwrap();
    bind.end_change().unwrap();
    let mut pending = bind.submit().unwrap();
    drive(driver);
    let mut batch = reply(&mut pending);
    batch
        .adjustment
        .as_mut()
        .unwrap()
        .install_control_scope_in(&root)
        .unwrap()
        .notify();
    drop((batch, pending));
    drive(driver);

    let domains = root.prepare_root_domains().unwrap();
    let source = domains
        .initial_payload_source(&root, JointLane::Custody)
        .unwrap();
    let mut proposal = match source.try_begin(nz(1), nz(8)).unwrap() {
        JointTryBegin::Started(proposal) => proposal,
        _ => panic!("initial original paid position"),
    };
    proposal.push_dependency(&route(provider)).unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut payload = None;
    match pending.poll_install_in(&mut Context::from_waker(Waker::noop()), &root, &mut payload) {
        Poll::Ready(Ok(notification)) => notification.notify(),
        _ => panic!("positive original Root payload"),
    }
    drop(pending);
    drive(driver);
    let payload = payload.unwrap();

    let mut pending = root.request_joint_child().unwrap();
    drive(driver);
    let mut batch = reply(&mut pending);
    let mut stage = None;
    batch
        .adjustment
        .as_mut()
        .unwrap()
        .install_child_in(&root, &mut stage)
        .unwrap()
        .notify();
    drop((batch, pending));
    drive(driver);
    let stage = stage.unwrap();

    // Same actual canonical RootStage author used by manifest binding.
    let mut proposal = payload
        .root_stage_source(&root, &stage)
        .unwrap()
        .begin(0, JointLane::Foreground)
        .unwrap();
    proposal.end_payload().unwrap();
    proposal
        .push_stage_dependency(&route("shared-native-data"))
        .unwrap();
    proposal.end_stage().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("actual manifest RootStage proof"),
    };
    let mut compute = None;
    ready
        .install_in(&root, &payload, &stage, &mut compute)
        .unwrap()
        .notify();
    drop((ready, pending, domains));
    drive(driver);
    Producer {
        work,
        stage,
        payload,
        compute: compute.unwrap(),
    }
}
fn quote() -> UnreceivedProductionQuote {
    UnreceivedProductionQuote::new(nz(1), NonZeroUsize::new(8).unwrap()).unwrap()
}
fn receive(admission: &mut UnreceivedProductionAdmission) -> UnreceivedProductionReady {
    match Pin::new(admission).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("closed Production proof"),
    }
}
fn register(
    scope: &WorkScope,
    origin: &Arc<dyn Send + Sync>,
    driver: &mut JointProgressDriver,
) -> UnreceivedProductionGrant {
    let mut admission = scope
        .begin_unreceived_productions(Arc::downgrade(origin), quote())
        .unwrap();
    drive(driver);
    let mut ready = receive(&mut admission);
    let mut bytes = Some(vec![19; 8].into_boxed_slice());
    let mut target = None;
    ready.register_in(&mut bytes, &mut target).unwrap().notify();
    assert!(bytes.is_none());
    drop((ready, admission));
    drive(driver);
    target.unwrap()
}
fn effective(control: &WorkloadControl, id: u64) -> Arc<ResponsibilityFacts> {
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
            return facts;
        }
    }
}
fn check_full(
    control: &WorkloadControl,
    grant: &UnreceivedProductionGrant,
    provider: &str,
    domains: usize,
) {
    let full = effective(control, grant.id);
    assert_eq!(full.class, WorkClass::Query);
    assert_eq!(full.lane, JointLane::Custody);
    assert_eq!(full.dependencies.len(), domains);
    for fact in [
        SharedDependency::LocalHost,
        route(provider),
        route("shared-native-data"),
    ] {
        assert!(full.dependencies.iter().any(|d| d.fact == fact));
    }
    let state = control.inner.state.lock().unwrap();
    let row = &state.joint.as_ref().unwrap().claims[&grant.id];
    assert!(matches!(
        row.facts.key.origin,
        root_domains::GroupOrigin::Inherited(_)
    ));
    assert_eq!(row.facts.key, row.active_facts.key);
    assert_eq!(
        row.facts.dependencies.len(),
        1,
        "only local Host S is inline"
    );
    assert_eq!(row.amounts, row.active_amounts);
    assert_eq!(row.amounts.get(JointResource::Publications), 1);
}
fn finish(producer: Producer) {
    drop((producer.compute, producer.payload));
    producer.stage.complete();
    producer.work.business.release();
    producer.work.owner.complete();
}

#[test]
fn canonical_unknown_and_received_follow_grow_and_hold_root_until_exact_settlement() {
    let (control, _handle, mut driver) = fixture();
    let a = producer(&control, &mut driver, "provider-a");
    let root = a.work.owner.scope();
    let stage = a.stage.scope();
    let root_id = root.id;
    let stage_id = stage.id;
    let retirement = a.work.owner.retirement_observer().unwrap();
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    let received = register(&stage, &origin, &mut driver);
    received.receive_in().unwrap().notify();
    let unknown = register(&stage, &origin, &mut driver);
    let before = {
        let state = control.inner.state.lock().unwrap();
        [received.id, unknown.id].map(|id| {
            let row = &state.joint.as_ref().unwrap().claims[&id];
            (row.structure_revision, row.release_revision, row.amounts)
        })
    };
    let mut proposal = match a
        .payload
        .root_domain_source(&root, JointLane::Foreground)
        .unwrap()
        .try_begin(0)
        .unwrap()
    {
        JointTryBegin::Started(proposal) => proposal,
        _ => panic!("original canonical Grow position"),
    };
    proposal
        .push_dependency(&route("new-native-control"))
        .unwrap();
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(ready)) => ready,
        _ => panic!("original canonical Grow proof"),
    };
    ready
        .install_domains_in(&root, &a.payload)
        .unwrap()
        .notify();
    drop((ready, pending));
    for (index, grant) in [&received, &unknown].into_iter().enumerate() {
        check_full(&control, grant, "provider-a", 4);
        let state = control.inner.state.lock().unwrap();
        let row = &state.joint.as_ref().unwrap().claims[&grant.id];
        assert_eq!(
            (row.structure_revision, row.release_revision, row.amounts),
            before[index]
        );
        assert!(matches!(row.custody, ClaimCustody::Production { epoch } if epoch == grant.epoch));
    }
    finish(a);
    drive(&mut driver);
    assert!(!retirement.is_reclaimed().unwrap());
    {
        let state = control.inner.state.lock().unwrap();
        assert!(state.nodes.contains_key(&root_id));
        assert!(state.nodes.contains_key(&stage_id));
    }
    check_full(&control, &unknown, "provider-a", 4);
    assert_eq!(unknown.owner().pending_records().unwrap(), 1);
    assert!(matches!(
        unknown.owner().settle_received_in(),
        Err(WorkError::Conflict)
    ));
    // The same original typed epoch still accepts complete End after Grow
    // and after both WorkOwners actually completed; no grant is reminted.
    unknown.receive_in().unwrap().notify();
    assert_eq!(unknown.owner().pending_records().unwrap(), 0);
    let owner = unknown.owner();
    drop((received, unknown, origin));
    assert!(owner.retains_facts(&[19; 8]).unwrap());
    owner.settle_received_in().unwrap().notify();
    assert!(!owner.retains_facts(&[19; 8]).unwrap());
    drop((root, stage));
    drive(&mut driver);
    assert!(retirement.is_reclaimed().unwrap());
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

#[test]
fn registration_refuses_frozen_local_s_against_canonical_control_without_taking_metadata() {
    let (control, handle, mut driver) = fixture();
    let a = producer(&control, &mut driver, "provider-a");
    let scope = a.stage.scope();
    let origin: Arc<dyn Send + Sync> = Arc::new(());
    // Reproduce exactly the old defective New row with an actual positive
    // paid receipt. The closed constructor itself must never make this row.
    let mut proposal = handle.begin_paid_proposal(&a.stage).unwrap();
    proposal
        .begin_new(JointLane::Custody, quote().required_progress())
        .unwrap();
    proposal
        .push_dependency(&SharedDependency::LocalHost)
        .unwrap();
    proposal.end_change().unwrap();
    let mut pending = proposal.submit().unwrap();
    drive(&mut driver);
    let mut ready = UnreceivedProductionReady {
        batch: reply(&mut pending),
        scope,
        origin: Arc::downgrade(&origin),
        quote: quote(),
    };
    let mut bytes = Some(vec![7; 8].into_boxed_slice());
    let mut target = None;
    assert!(matches!(
        ready.register_in(&mut bytes, &mut target),
        Err(WorkError::Conflict)
    ));
    assert_eq!(bytes.as_deref(), Some(&[7; 8][..]));
    assert!(target.is_none());
    drop((bytes, ready, pending, origin));
    finish(a);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}

#[test]
fn canonical_productions_preserve_each_complete_catalog_native_and_shared_host_origin() {
    let (control, _handle, mut driver) = fixture();
    let a = producer(&control, &mut driver, "provider-a");
    let b = producer(&control, &mut driver, "provider-b");
    let origin_a: Arc<dyn Send + Sync> = Arc::new(());
    let origin_b: Arc<dyn Send + Sync> = Arc::new(());
    let a1 = register(&a.stage.scope(), &origin_a, &mut driver);
    let a2 = register(&a.stage.scope(), &origin_a, &mut driver);
    check_full(&control, &a1, "provider-a", 3);
    check_full(&control, &a2, "provider-a", 3);
    let a3 = register(&a.stage.scope(), &origin_a, &mut driver);
    check_full(&control, &a3, "provider-a", 3);
    let b1 = register(&b.stage.scope(), &origin_b, &mut driver);
    check_full(&control, &b1, "provider-b", 3);
    assert_eq!(a1.owner().pending_records().unwrap(), 3);
    for grant in [&a1, &a2, &a3, &b1] {
        grant.receive_in().unwrap().notify();
    }
    a1.owner().settle_received_in().unwrap().notify();
    b1.owner().settle_received_in().unwrap().notify();
    drop((a1, a2, a3, b1, origin_a, origin_b));
    finish(a);
    finish(b);
    control.close_admission();
    drive(&mut driver);
    assert!(control.shutdown().is_ok());
}
