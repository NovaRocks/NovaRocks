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

#[test]
fn original_owner_source_preserves_full_d_and_rejects_stale_or_cancelled_business() {
    let (control, handle, mut driver) = fixture();
    let work = root(&control);
    let scope = work.owner.scope();
    let a = fact("actual-catalog");
    let b = fact("actual-storage");
    bind(&scope, &[a], &mut driver);
    let stale = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap();
    bind(&scope, &[b], &mut driver);
    assert!(matches!(
        stale.begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap()),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));

    let mut action = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap()
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    drive(&mut driver);
    {
        let state = handle.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        let record = &ledger.requests[&action.admission.id.unwrap()];
        assert_eq!(record.owner, scope.id);
        let [change] = &*record.changes else {
            panic!("one original source action")
        };
        assert_eq!(change.claim.facts.dependencies.len(), 2);
        assert!(change.claim.facts.key.source_kind.is_empty());
        assert_eq!(change.claim.facts.class, WorkClass::Query);
        assert_eq!(change.claim.facts.lane, JointLane::Foreground);
        assert_eq!(change.claim.amounts.get(JointResource::BlockingJobs), 1);
        assert_eq!(
            change
                .claim
                .amounts
                .get(JointResource::PhysicalQueueRecords),
            1
        );
        assert_eq!(change.claim.amounts.get(JointResource::Computations), 0);
        assert_eq!(change.claim.amounts.get(JointResource::InternalCpuJobs), 0);
    }
    {
        let mut offer = match action.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("expected the original foreground offer"),
        };
        // No physical call has been attempted. Explicitly report that fact
        // before withdrawing the same ticket; Drop alone proves no such exit.
        let mut physical = None::<std::convert::Infallible>;
        offer.reobserve_after_drop(&mut physical).unwrap().notify();
    }
    drop(action);
    drive(&mut driver);
    let cancelled = work
        .owner
        .blocking_action_source(JointLane::Foreground)
        .unwrap();
    let settlement = work
        .owner
        .blocking_action_source(JointLane::Settlement)
        .unwrap();
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    assert!(
        cancelled
            .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
            .is_err()
    );
    let mut settlement = settlement
        .begin(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    drive(&mut driver);
    {
        let mut offer = match settlement.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("expected the original settlement offer"),
        };
        let mut physical = None::<std::convert::Infallible>;
        offer.reobserve_after_drop(&mut physical).unwrap().notify();
    }
    // No physical capability was manufactured or accepted. The explicit
    // no-attempt report permits this original paid ticket to be withdrawn.
    drop(settlement);
    drive(&mut driver);
    work.business.release();
    work.owner.complete_after_terminal_cancel_settled();
    drive(&mut driver);
    assert!(control.snapshot().scopes.is_empty());
}
