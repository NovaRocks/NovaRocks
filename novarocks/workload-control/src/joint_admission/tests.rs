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

use super::*;
use crate::{WorkRequest, WorkScope, WorkloadConfig, WorkloadControl};
use novarocks_resource_contract::{NetworkServiceOrigin, SharedDependency};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

pub(super) fn bytes(amount: u64) -> JointResources {
    JointResources::ZERO.with(JointResource::PayloadBytes, amount)
}
pub(super) fn control() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let (handle, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(17).unwrap(),
        })
        .unwrap();
    control.mark_ready().unwrap();
    (control, handle, driver)
}
pub(super) fn dependency(handle: &JointAdmissionHandle, name: &str) -> JointDependency {
    handle
        .register_dependency(SharedDependency::Network(
            NetworkServiceOrigin::from_selected_url(
                &url::Url::parse(&format!("https://{name}.example")).unwrap(),
            )
            .unwrap(),
        ))
        .unwrap()
}
pub(super) fn request(
    handle: &JointAdmissionHandle,
    scope: &WorkScope,
    deps: &[&JointDependency],
    lane: JointLane,
    amount: u64,
) -> JointAdmission {
    handle
        .request(
            scope,
            vec![JointClaimChange::New {
                dependencies: deps.iter().map(|dep| (*dep).clone()).collect(),
                lane,
                amounts: bytes(amount),
            }],
        )
        .unwrap()
}
pub(super) fn poll(
    request: &mut JointAdmission,
) -> Poll<Result<JointGrantedBatch, JointAdmissionFailure>> {
    Pin::new(request).poll(&mut Context::from_waker(Waker::noop()))
}
pub(super) fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..200_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("joint progression did not become idle");
}
pub(super) fn granted(request: &mut JointAdmission) -> JointGrantedClaims {
    match poll(request) {
        Poll::Ready(Ok(batch)) => {
            assert!(batch.adjustment.is_none());
            batch.new_claims
        }
        other => panic!("expected original grant: {other:?}"),
    }
}
pub(super) fn held(control: &WorkloadControl) -> (usize, u64) {
    let state = control.inner.state.lock().unwrap();
    let ledger = state.joint.as_ref().unwrap();
    (
        ledger.claims.len(),
        ledger
            .claims
            .values()
            .map(|claim| claim.amounts.get(JointResource::PayloadBytes))
            .sum(),
    )
}

#[test]
fn concurrent_release_is_not_resurrected_by_positive_shadow_commit() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut second = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    assert_eq!(driver.step(), JointProgress::More);
    assert_eq!(driver.step(), JointProgress::More); // Arena allocated.
    assert_eq!(driver.step(), JointProgress::More); // Original copied before release.
    drop(original);
    drive(&mut driver);
    let second = granted(&mut second);
    assert_eq!(held(&control), (1, 1));
    drop(second);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn granted_but_unreceived_drop_returns_exact_capacity_without_releasing_another_claim() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut first);
    let replace = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original[0].view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(held(&control), (1, 2));
    drop(replace);
    assert_eq!(held(&control), (1, 1));
    assert_eq!(original[0].amounts().unwrap(), bytes(1));
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn foreign_authority_rejection_leaves_original_capability_in_place() {
    let (control, handle, mut driver) = control();
    let (foreign, foreign_handle, _foreign_driver) = self::control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let other = foreign
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let a = dependency(&handle, "a");
    let foreign_a = dependency(&foreign_handle, "a");
    let foreign_request = handle.request(
        &scope,
        vec![JointClaimChange::New {
            dependencies: vec![foreign_a],
            lane: JointLane::Foreground,
            amounts: bytes(1),
        }],
    );
    assert!(matches!(
        foreign_request,
        Err(JointAdmissionFailure {
            error: WorkError::ForeignAuthority,
            ..
        })
    ));
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let claims = granted(&mut first);
    let failed = handle.request(
        &other.owner.scope(),
        vec![JointClaimChange::grow_by(
            claims[0].view().unwrap(),
            bytes(1),
            vec![],
        )],
    );
    let Err(failed) = failed else {
        panic!("foreign scope accepted");
    };
    assert_eq!(failed.error, WorkError::ForeignAuthority);
    assert_eq!(claims[0].amounts().unwrap(), bytes(1));
}

#[test]
fn request_withdrawal_does_not_hold_a_removed_proof_fence_or_other_claims() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let request = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    driver.step();
    drop(request);
    let independently_registered = handle
        .register_dependency(SharedDependency::LocalHost)
        .unwrap();
    assert_eq!(held(&control), (0, 0));
    drop(independently_registered);
    drive(&mut driver);
    handle
        .register_dependency(SharedDependency::LocalHost)
        .unwrap();
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn driver_exit_keeps_original_holder_and_never_mints_growth() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let claims = granted(&mut first);
    let mut pending = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                claims[0].view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    driver.step();
    drop(driver);
    match poll(&mut pending) {
        Poll::Ready(Err(failed)) => {
            assert_eq!(failed.error, WorkError::Closed);
            assert_eq!(claims[0].amounts().unwrap(), bytes(1));
        }
        _ => panic!("driver exit did not reject"),
    }
    assert_eq!(held(&control), (1, 1));
    drop(claims);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn cancelled_queued_request_retires_without_creating_new_permission() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut blocked = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    root.owner.cancel(crate::CancellationReason::Requested);
    // Selection and the next turn observe cancellation before rebuilding input.
    driver.step();
    driver.step();
    match poll(&mut blocked) {
        Poll::Ready(Err(failed)) => assert_eq!(
            failed.error,
            WorkError::Cancelled(crate::CancellationReason::Requested)
        ),
        other => panic!("cancelled capacity request was not retired: {other:?}"),
    }
    assert_eq!(held(&control), (1, 2));
    let mut settlement = request(&handle, &scope, &[&a], JointLane::Settlement, 1);
    drive(&mut driver);
    let settlement = granted(&mut settlement);
    drop((original, settlement));
}

#[tokio::test]
async fn idle_revision_has_no_self_wake_and_covers_both_notify_orderings() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let JointProgress::Idle(revision) = driver.step() else {
        panic!("empty authority was not idle");
    };
    assert_eq!(
        control.progress_revision(),
        revision,
        "an idle observation must not publish its own event"
    );
    let expiry = control.deadline_expiry_handle();
    let waiting = expiry.wait_state_change(revision);
    tokio::pin!(waiting);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let first = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(first);
    drive(&mut driver);
    let JointProgress::Idle(revision) = driver.step() else {
        panic!("retired request did not become idle");
    };
    // Event between the same-lock idle verdict and subscription cannot be lost.
    let second = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let waiting = expiry.wait_state_change(revision);
    tokio::pin!(waiting);
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    drop(second);
    drive(&mut driver);
}

fn batch(request: &mut JointAdmission) -> JointGrantedBatch {
    match poll(request) {
        Poll::Ready(Ok(batch)) => batch,
        other => panic!("expected granted batch: {other:?}"),
    }
}
pub(super) fn adjustment(request: &mut JointAdmission) -> JointAdjustmentReceipt {
    let batch = batch(request);
    assert!(batch.new_claims.is_empty());
    batch.adjustment.expect("adjustment receipt")
}
pub(super) fn roomy_control() -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
    control()
}

#[test]
fn waiting_future_drop_and_unwind_preserve_the_stable_holder() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pending = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
            )
            .unwrap();
        drive(&mut driver);
        panic!("caller unwinds while a complete growth vector is blocked");
    }));
    assert!(unwound.is_err());
    assert_eq!(original.amounts().unwrap(), bytes(2));
    assert_eq!(held(&control), (1, 2));
    let next = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![],
            )],
        )
        .unwrap();
    drop(next);
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn receipt_drop_uses_live_active_after_partial_release() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let old_view = original.view().unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![b],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut growth);
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(held(&control), (1, 14));
    original.release_part(bytes(3)).unwrap();
    assert_eq!(held(&control), (1, 11));
    drop(receipt);
    assert_eq!(original.amounts().unwrap(), bytes(7));
    assert_eq!(held(&control), (1, 7));
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(old_view, bytes(1), vec![])]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    let mut next = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut next).install(&[&original]).unwrap();
    assert_eq!(original.amounts().unwrap(), bytes(8));
}

#[test]
fn zero_delta_dependency_extension_preserves_full_original_amount_and_union() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut extend = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![b.clone()],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut extend);
    receipt.install(&[&original]).unwrap();
    assert_eq!(original.active_dependency_count(), 2);
    assert_eq!(held(&control), (1, 2));
    assert_eq!(original.amounts().unwrap(), bytes(2));
    let mut healthy = request(&handle, &scope, &[&b], JointLane::Foreground, 1);
    drive(&mut driver);
    let healthy = granted(&mut healthy);
    assert_eq!(held(&control), (2, 3));
    drop((extend, healthy, original));
}

#[test]
fn single_pending_and_queued_partial_release_have_exact_conflict() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let mut grow = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(0),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    // Release while the original request is genuinely Waiting, before any driver turn.
    original.release_part(bytes(1)).unwrap();
    drive(&mut driver);
    assert!(matches!(
        poll(&mut grow),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    assert_eq!(original.amounts().unwrap(), bytes(0));
    assert_eq!(held(&control), (1, 0));
}

#[test]
fn delivered_new_survives_source_drop_and_late_receipt_withdrawal() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut mixed = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(5),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let delivered = batch(&mut mixed);
    assert_eq!(held(&control), (2, 19));
    drop(original);
    assert_eq!(held(&control), (1, 5));
    let receipt = delivered.adjustment.unwrap();
    assert_eq!(receipt.install(&[]), Err(WorkError::Released));
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    drop(delivered.new_claims);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn batch_install_is_atomic_and_does_not_return_replacement_guards() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 20);
    drive(&mut driver);
    let first = granted(&mut first).pop().unwrap();
    let second = granted(&mut second).pop().unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(first.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::grow_by(second.view().unwrap(), bytes(5), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(
        adjustment(&mut growth).install(&[&first]),
        Err(WorkError::Conflict)
    );
    assert_eq!(held(&control), (2, 30));
    assert_eq!(first.amounts().unwrap(), bytes(10));
    assert_eq!(second.amounts().unwrap(), bytes(20));
}

#[test]
fn cancel_before_install_rolls_back_delta_but_install_before_cancel_preserves_actual_custody() {
    for cancel_first in [true, false] {
        let (control, handle, mut driver) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let mut growth = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
            )
            .unwrap();
        drive(&mut driver);
        let receipt = adjustment(&mut growth);
        if cancel_first {
            root.owner.cancel(crate::CancellationReason::Requested);
            assert!(receipt.install(&[&original]).is_err());
            assert_eq!(original.amounts().unwrap(), bytes(1));
        } else {
            receipt.install(&[&original]).unwrap();
            root.owner.cancel(crate::CancellationReason::Requested);
            assert_eq!(original.amounts().unwrap(), bytes(2));
        }
        assert_eq!(held(&control).0, 1);
        drop(original);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn granted_partial_release_then_install_adopts_only_live_base_and_delta() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let mut original = granted(&mut initial).pop().unwrap();
    let mut grow = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut grow);
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Conflict
        })
    ));
    original.release_part(bytes(3)).unwrap();
    receipt.install(&[&original]).unwrap();
    assert_eq!(original.amounts().unwrap(), bytes(11));
    assert_eq!(held(&control), (1, 11));
    drop(original);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn source_drop_before_reply_refunds_only_unstarted_batch_and_reports_released() {
    for grant_first in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let mut mixed = handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a],
                        lane: JointLane::Foreground,
                        amounts: bytes(5),
                    },
                ],
            )
            .unwrap();
        if grant_first {
            drive(&mut driver);
        } else {
            driver.step();
        }
        drop(original);
        assert!(matches!(
            poll(&mut mixed),
            Poll::Ready(Err(JointAdmissionFailure {
                error: WorkError::Released
            }))
        ));
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn driver_exit_after_delivery_refunds_delta_and_preserves_delivered_new() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut mixed = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(original.view().unwrap(), bytes(4), vec![]),
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(5),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let delivered = batch(&mut mixed);
    drop(driver);
    assert_eq!(held(&control), (2, 15));
    assert_eq!(
        delivered.adjustment.unwrap().install(&[&original]),
        Err(WorkError::Closed)
    );
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    drop((delivered.new_claims, original));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn request_publication_rechecks_sources_after_outside_lock_construction() {
    for release_source in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut original = granted(&mut initial).pop().unwrap();
        let changes = vec![JointClaimChange::grow_by(
            original.view().unwrap(),
            bytes(4),
            vec![],
        )];
        let prepared = handle.prepare_request(&scope, &changes).unwrap();
        if release_source {
            original.release_part(bytes(3)).unwrap();
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Conflict)
            ));
            assert_eq!(held(&control), (1, 7));
        } else {
            let pending = handle.request(&scope, changes).unwrap();
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Conflict)
            ));
            assert_eq!(held(&control), (1, 10));
            assert_eq!(
                control
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .joint
                    .as_ref()
                    .unwrap()
                    .requests
                    .len(),
                1
            );
            drop(pending);
        }
        drop(original);
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn request_publication_preserves_original_cancel_and_drop_errors() {
    for drop_source in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let original = granted(&mut initial).pop().unwrap();
        let changes = [JointClaimChange::grow_by(
            original.view().unwrap(),
            bytes(4),
            vec![],
        )];
        let prepared = handle.prepare_request(&scope, &changes).unwrap();
        if drop_source {
            drop(original);
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Released)
            ));
        } else {
            root.owner.cancel(crate::CancellationReason::Requested);
            assert!(matches!(
                handle.queue_request(prepared),
                Err(WorkError::Cancelled(crate::CancellationReason::Requested))
            ));
            assert_eq!(original.amounts().unwrap(), bytes(10));
            drop(original);
        }
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
        assert!(
            control
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
    }
}

#[test]
fn a_late_view_cannot_address_a_later_claim_in_the_same_authority() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let original = granted(&mut first).pop().unwrap();
    let stale = original.view().unwrap();
    let original_id = *control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .first_key_value()
        .unwrap()
        .0;
    drop(original);
    let mut later = request(&handle, &scope, &[&a], JointLane::Foreground, 20);
    drive(&mut driver);
    let later = granted(&mut later).pop().unwrap();
    let later_id = *control
        .inner
        .state
        .lock()
        .unwrap()
        .joint
        .as_ref()
        .unwrap()
        .claims
        .first_key_value()
        .unwrap()
        .0;
    assert!(later_id > original_id);
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(stale, bytes(1), vec![])]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::Released
        })
    ));
    assert_eq!(later.amounts().unwrap(), bytes(20));
    assert_eq!(held(&control), (1, 20));
}

#[test]
fn matching_numeric_claim_ids_do_not_cross_original_authorities() {
    let (control, handle, mut driver) = roomy_control();
    let (foreign, foreign_handle, mut foreign_driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let other = foreign
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let foreign_a = dependency(&foreign_handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(
        &foreign_handle,
        &other.owner.scope(),
        &[&foreign_a],
        JointLane::Foreground,
        20,
    );
    drive(&mut driver);
    drive(&mut foreign_driver);
    let original = granted(&mut first).pop().unwrap();
    let other = granted(&mut second).pop().unwrap();
    let ids = |control: &WorkloadControl| {
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .claims
            .keys()
            .copied()
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&control), ids(&foreign));
    assert!(matches!(
        handle.request(
            &scope,
            vec![JointClaimChange::grow_by(
                other.view().unwrap(),
                bytes(1),
                vec![]
            )]
        ),
        Err(JointAdmissionFailure {
            error: WorkError::ForeignAuthority
        })
    ));
    let mut growth = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(4),
                vec![],
            )],
        )
        .unwrap();
    drive(&mut driver);
    assert_eq!(
        adjustment(&mut growth).install(&[&other]),
        Err(WorkError::ForeignAuthority)
    );
    assert_eq!(original.amounts().unwrap(), bytes(10));
    assert_eq!(other.amounts().unwrap(), bytes(20));
    assert_eq!(held(&control), (1, 10));
    assert_eq!(held(&foreign), (1, 20));
}

struct QueueHandoffPanicWake(std::sync::atomic::AtomicBool);
impl std::task::Wake for QueueHandoffPanicWake {
    fn wake(self: std::sync::Arc<Self>) {
        if self.0.swap(false, std::sync::atomic::Ordering::AcqRel) {
            panic!("intentional progress wake panic during queue handoff");
        }
    }
}

#[test]
fn queue_handoff_panic_withdraws_the_original_ticket() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    for fixed in [false, true] {
        let (control, handle, mut driver) = if fixed {
            crate::scope::fixed_control_storage_tests::prototype_profile(3, 1, 1, 1)
        } else {
            control()
        };
        let work = control
            .try_begin_root(WorkRequest::new(if fixed {
                WorkClass::Query
            } else {
                WorkClass::Management
            }))
            .unwrap();
        let scope = work.owner.scope();
        let changes = vec![JointClaimChange::New {
            dependencies: Vec::new(),
            lane: if fixed {
                JointLane::Custody
            } else {
                JointLane::Foreground
            },
            amounts: if fixed {
                JointResources::ZERO
            } else {
                bytes(1)
            },
        }];
        let prepared = handle.prepare_request(&scope, &changes).unwrap();
        let wake = Waker::from(std::sync::Arc::new(QueueHandoffPanicWake(
            std::sync::atomic::AtomicBool::new(true),
        )));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        assert!(catch_unwind(AssertUnwindSafe(|| handle.queue_request(prepared))).is_err());
        drop(waiter);
        let (requests, waiters) = {
            let state = control.inner.state.lock().unwrap();
            (
                state.joint.as_ref().unwrap().requests.len(),
                state.nodes[&scope.id()].resource_waiters,
            )
        };
        assert_eq!(
            requests, 0,
            "unreceived admission must withdraw its original ticket"
        );
        assert_eq!(waiters, 0);
        work.business.release();
        work.owner.complete();
        drive(&mut driver);
        if fixed {
            assert!(control.inner.state.lock().unwrap().cancellations.drained());
        }
    }
}

#[test]
fn reply_handoff_panic_preserves_an_existing_physical_claim() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = work.owner.scope();
    let mut initial = request(&handle, &scope, &[], JointLane::Foreground, 1);
    drive(&mut driver);
    let original = granted(&mut initial).pop().unwrap();
    let mut mixed = handle
        .request(
            &scope,
            vec![
                JointClaimChange::New {
                    dependencies: Vec::new(),
                    lane: JointLane::Foreground,
                    amounts: bytes(1),
                },
                JointClaimChange::grow_by(original.view().unwrap(), bytes(1), Vec::new()),
            ],
        )
        .unwrap();
    drive(&mut driver);
    let wake = Waker::from(std::sync::Arc::new(QueueHandoffPanicWake(
        std::sync::atomic::AtomicBool::new(true),
    )));
    let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| poll(&mut mixed))).is_err());
    drop((waiter, mixed));
    assert_eq!(
        original.amounts().unwrap(),
        bytes(1),
        "the delivered original remains installed without the withdrawn delta"
    );
    let (claims, requests, holders, waiters) = {
        let state = control.inner.state.lock().unwrap();
        let ledger = state.joint.as_ref().unwrap();
        let node = &state.nodes[&scope.id()];
        (
            ledger.claims.len(),
            ledger.requests.len(),
            node.resource_holders,
            node.resource_waiters,
        )
    };
    assert_eq!((claims, requests, holders, waiters), (1, 0, 1, 0));
    drop(original);
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
}

struct LocalPhysicalDrop {
    control: std::sync::Arc<crate::scope::Inner>,
    seen: std::sync::Arc<std::sync::Mutex<Vec<(bool, usize)>>>,
}
impl Drop for LocalPhysicalDrop {
    fn drop(&mut self) {
        // This is the actual physical destructor. Its observation proves that
        // State was not held and the original claim had not yet been refunded.
        let fact = match self.control.state.try_lock() {
            Ok(state) => (
                true,
                state.joint.as_ref().map_or(0, |ledger| ledger.claims.len()),
            ),
            Err(_) => (false, usize::MAX),
        };
        // Never assert while another physical destructor is already unwinding.
        if let Ok(mut seen) = self.seen.lock() {
            seen.push(fact);
        }
    }
}

fn original_local_request(
    handle: &JointAdmissionHandle,
    scope: &WorkScope,
    dep: &JointDependency,
) -> JointAdmission {
    request(handle, scope, &[dep], JointLane::Foreground, 1)
}
fn local_request_state(control: &WorkloadControl) -> (u64, u64, bool) {
    let state = control.inner.state.lock().unwrap();
    let (&ticket, row) = state
        .joint
        .as_ref()
        .unwrap()
        .requests
        .iter()
        .next()
        .unwrap();
    (ticket, row.changes.len() as u64, row.local_blocked)
}

#[test]
fn local_refusal_keeps_original_ticket_shape_and_waits_for_original_timer_pass() {
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let dep = dependency(&handle, "local");
    let mut request = original_local_request(&handle, &work.owner.scope(), &dep);
    let (ticket, initial, _) = local_request_state(&control);
    drive(&mut driver);
    let (_, first, _) = local_request_state(&control);
    assert_eq!(first, initial);
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut physical = Some(LocalPhysicalDrop {
        control: std::sync::Arc::clone(&control.inner),
        seen: seen.clone(),
    });
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original finite grant"),
    };
    offer.reobserve_after_drop(&mut physical).unwrap().notify();
    drop(offer);
    assert_eq!(seen.lock().unwrap().as_slice(), &[(true, 1)]);
    assert!(physical.is_none());
    assert_eq!(held(&control), (0, 0));
    assert_eq!(local_request_state(&control), (ticket, first, true));
    drive(&mut driver);
    assert_eq!(local_request_state(&control), (ticket, first, true));
    assert!(
        request
            .poll_local_offer(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    driver.recheck_retired_backing();
    drive(&mut driver);
    let (same, second, blocked) = local_request_state(&control);
    assert_eq!(same, ticket);
    assert_eq!(second, first, "same request shape is reoffered");
    assert!(!blocked);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("same ticket granted again"),
    };
    let mut physical = Some(LocalPhysicalDrop {
        control: std::sync::Arc::clone(&control.inner),
        seen: seen.clone(),
    });
    let mut destination = None;
    offer
        .accept_in(&mut physical, &mut destination)
        .unwrap()
        .notify();
    drop(offer);
    assert!(physical.is_none());
    assert!(
        control
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
    assert_eq!(destination.as_ref().unwrap().claims().len(), 1);
    drop(destination);
    assert_eq!(seen.lock().unwrap().as_slice(), &[(true, 1), (true, 1)]);
    assert_eq!(held(&control), (0, 0));
    work.business.release();
    work.owner.complete();
}

#[test]
fn local_accept_stable_destination_survives_real_post_state_wake_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let dep = dependency(&handle, "local");
    let mut request = original_local_request(&handle, &work.owner.scope(), &dep);
    drive(&mut driver);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original finite grant"),
    };
    let wake = Waker::from(std::sync::Arc::new(QueueHandoffPanicWake(
        std::sync::atomic::AtomicBool::new(true),
    )));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut physical = Some(LocalPhysicalDrop {
        control: control.inner.clone(),
        seen: seen.clone(),
    });
    let mut destination = None;
    let notification = offer.accept_in(&mut physical, &mut destination).unwrap();
    drop(offer);
    assert!(catch_unwind(AssertUnwindSafe(|| notification.notify())).is_err());
    assert!(physical.is_none());
    assert!(seen.lock().unwrap().is_empty());
    assert_eq!(destination.as_ref().unwrap().claims().len(), 1);
    assert_eq!(held(&control), (1, 1));
    drop(progress);
    // A second accept cannot overwrite the exact original receiver.
    let mut second = original_local_request(&handle, &work.owner.scope(), &dep);
    drive(&mut driver);
    let mut offer = match second.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("second finite grant"),
    };
    let mut rejected = Some(LocalPhysicalDrop {
        control: control.inner.clone(),
        seen: seen.clone(),
    });
    assert!(matches!(
        offer.accept_in(&mut rejected, &mut destination),
        Err(WorkError::Conflict)
    ));
    assert!(rejected.is_some());
    assert!(seen.lock().unwrap().is_empty());
    offer.reobserve_after_drop(&mut rejected).unwrap().notify();
    drop(offer);
    drop(second);
    // Both originals move without callbacks into the original first-party
    // physical owner's stable slots. Its explicit cleanup is physical first.
    let mut physical_owner = None;
    let mut claim_owner = None;
    assert!(
        destination
            .take()
            .unwrap()
            .transfer_in(&mut physical_owner, &mut claim_owner)
            .is_ok()
    );
    assert_eq!(claim_owner.as_ref().unwrap().len(), 1);
    assert_eq!(held(&control), (1, 1));
    drop(physical_owner.take());
    assert_eq!(held(&control), (1, 1));
    drop(claim_owner.take());
    assert_eq!(held(&control), (0, 0));
    work.business.release();
    work.owner.complete();
}

#[test]
fn local_cancel_records_failure_but_cannot_free_an_in_flight_physical_offer() {
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let dep = dependency(&handle, "local");
    let mut request = original_local_request(&handle, &work.owner.scope(), &dep);
    drive(&mut driver);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original finite grant"),
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut physical = Some(LocalPhysicalDrop {
        control: control.inner.clone(),
        seen: seen.clone(),
    });
    work.owner.cancel(crate::CancellationReason::ServerShutdown);
    drive(&mut driver);
    assert_eq!(held(&control), (1, 1));
    let mut destination = None;
    assert!(matches!(
        offer.accept_in(&mut physical, &mut destination),
        Err(WorkError::Cancelled(
            crate::CancellationReason::ServerShutdown
        ))
    ));
    assert!(destination.is_none());
    assert!(physical.is_some());
    offer.reobserve_after_drop(&mut physical).unwrap().notify();
    drop(offer);
    assert_eq!(held(&control), (0, 0));
    assert_eq!(seen.lock().unwrap().as_slice(), &[(true, 1)]);
    assert!(matches!(
        request.poll_local_offer(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Cancelled(crate::CancellationReason::ServerShutdown)
        }))
    ));
    drop(request);
    assert!(
        control
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
    work.business.release();
    work.owner.complete();
}

#[test]
fn local_physical_drop_unwind_keeps_same_offer_and_cannot_retry_or_refund() {
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    #[derive(Debug)]
    struct OriginalPhysicalExitPanic;
    struct PanickingPhysical {
        _physical: LocalPhysicalDrop,
    }
    impl Drop for PanickingPhysical {
        fn drop(&mut self) {
            panic_any(OriginalPhysicalExitPanic)
        }
    }
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let dep = dependency(&handle, "local");
    let mut request = original_local_request(&handle, &work.owner.scope(), &dep);
    drive(&mut driver);
    let before = local_request_state(&control);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original finite grant"),
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut physical = Some(PanickingPhysical {
        _physical: LocalPhysicalDrop {
            control: control.inner.clone(),
            seen: seen.clone(),
        },
    });
    let primary = catch_unwind(AssertUnwindSafe(|| {
        let _ = offer.reobserve_after_drop(&mut physical);
    }))
    .expect_err("real physical destructor panic");
    assert!(primary.is::<OriginalPhysicalExitPanic>());
    assert_eq!(seen.lock().unwrap().as_slice(), &[(true, 1)]);
    assert!(physical.is_none());
    assert!(
        matches!(
            offer.reobserve_after_drop(&mut physical),
            Err(WorkError::Conflict)
        ),
        "never retry an unwound physical destructor"
    );
    drop(offer);
    assert_eq!(local_request_state(&control), before);
    assert_eq!(held(&control), (1, 1));
    drop(request);
    assert_eq!(
        held(&control),
        (1, 1),
        "implicit admission Drop cannot refund unknown physical exit"
    );
    assert_eq!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .joint
            .as_ref()
            .unwrap()
            .requests
            .len(),
        1
    );
    // This is the illegal-exit fail-closed boundary, not normal shutdown. The
    // same source/cause remains present until this test's whole authority exits.
    work.business.release();
    work.owner.complete();
    drop(primary);
}

#[test]
fn fixed_paid_local_offer_releases_proposal_body_but_retains_physical_and_reply_claims() {
    let (control, handle, mut driver) =
        crate::scope::fixed_control_storage_tests::prototype_profile(4, 1, 1, 1);
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let observer = work.owner.retirement_observer().unwrap();
    let mut bind = handle.begin_paid_proposal(&work.owner).unwrap();
    bind.begin_first_bind(work.owner.control_claim_view().unwrap())
        .unwrap();
    bind.push_dependency(&SharedDependency::LocalHost).unwrap();
    bind.end_change().unwrap();
    let mut bind = bind.submit().unwrap();
    drive(&mut driver);
    let Poll::Ready(Ok(mut reply)) = poll(&mut bind) else {
        panic!("paid original FirstBind")
    };
    reply
        .adjustment
        .take()
        .unwrap()
        .install_control(&work.owner, &[])
        .unwrap();
    drop(reply);
    drive(&mut driver);
    let mut proposal = handle.begin_paid_proposal(&work.owner).unwrap();
    proposal
        .begin_new(
            JointLane::Custody,
            JointResources::ZERO.with(JointResource::ClaimRecords, 1),
        )
        .unwrap();
    proposal
        .push_dependency(&SharedDependency::LocalHost)
        .unwrap();
    proposal.end_change().unwrap();
    let mut request = proposal.submit().unwrap();
    let (ticket, budget, _) = local_request_state(&control);
    drive(&mut driver);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("original paid all-New offer"),
    };
    offer
        .reobserve_after_drop(&mut None::<()>)
        .unwrap()
        .notify();
    drop(offer);
    let (same, remaining, blocked) = local_request_state(&control);
    assert_eq!(same, ticket);
    assert_eq!(
        remaining, budget,
        "physical Full preserves the same original shape"
    );
    assert!(blocked);
    driver.recheck_retired_backing();
    drive(&mut driver);
    let mut offer = match request.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(offer)) => offer,
        _ => panic!("same paid body reoffered"),
    };
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut physical = Some(LocalPhysicalDrop {
        control: control.inner.clone(),
        seen: seen.clone(),
    });
    let mut destination = None;
    offer
        .accept_in(&mut physical, &mut destination)
        .unwrap()
        .notify();
    drop(offer);
    assert_eq!(destination.as_ref().unwrap().claims().len(), 1);
    drive(&mut driver);
    // accept_in installed the original physical/claim bundle. Its response
    // lifetime cannot occupy either of the two temporary constructor cells.
    let a = handle.reserve_proposal(&work.owner.scope()).unwrap();
    let b = handle.reserve_proposal(&work.owner.scope()).unwrap();
    assert_eq!(
        destination.as_ref().unwrap().claims()[0]
            .amounts()
            .unwrap()
            .get(JointResource::ClaimRecords),
        1
    );
    drop((a, b));
    drive(&mut driver);
    work.business.release();
    work.owner.complete();
    drive(&mut driver);
    assert_eq!(
        observer.is_reclaimed(),
        Ok(false),
        "real local physical/reply still owns original Work backing"
    );
    drop(destination);
    assert_eq!(seen.lock().unwrap().as_slice(), &[(true, 5)]);
    drive(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    control.close_admission();
    drive(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[test]
fn closed_before_reply_refuses_new_permission_without_a_driver_turn() {
    for receive_first in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "a");
        let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        let mut trigger = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
        drive(&mut driver);
        let first = granted(&mut first).pop().unwrap();
        let mut trigger = granted(&mut trigger).pop().unwrap();
        let mut mixed = handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(first.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a],
                        lane: JointLane::Foreground,
                        amounts: bytes(5),
                    },
                ],
            )
            .unwrap();
        drive(&mut driver);
        let delivered = receive_first.then(|| batch(&mut mixed));
        control.inner.update(|state| {
            state.joint.as_mut().unwrap().facts_revision = u64::MAX;
        });
        trigger.release_part(bytes(1)).unwrap();
        // Poll races the close before the driver observes it. A previously
        // delivered New is already owned; an unreceived grant is not permission.
        if let Some(delivered) = delivered {
            assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
            assert_eq!(
                delivered.adjustment.unwrap().install(&[&first]),
                Err(WorkError::Closed)
            );
            assert_eq!(held(&control), (3, 15));
            drop(delivered.new_claims);
        } else {
            assert!(matches!(
                poll(&mut mixed),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Closed
                }))
            ));
            assert_eq!(held(&control), (2, 10));
        }
        assert_eq!(first.amounts().unwrap(), bytes(10));
        drop((first, trigger));
        drive(&mut driver);
        assert_eq!(held(&control), (0, 0));
    }
}

#[test]
fn epoch_overflow_without_active_turn_settles_all_tickets_without_refunding_delivered_new() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    let mut third = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
    drive(&mut driver);
    let first = granted(&mut first).pop().unwrap();
    let second = granted(&mut second).pop().unwrap();
    let mut third = granted(&mut third).pop().unwrap();
    let mixed = |source: &JointClaim, amount| {
        handle
            .request(
                &scope,
                vec![
                    JointClaimChange::grow_by(source.view().unwrap(), bytes(4), vec![]),
                    JointClaimChange::New {
                        dependencies: vec![a.clone()],
                        lane: JointLane::Foreground,
                        amounts: bytes(amount),
                    },
                ],
            )
            .unwrap()
    };
    let mut delivered = mixed(&first, 5);
    let mut unreceived = mixed(&second, 6);
    drive(&mut driver);
    let delivered = batch(&mut delivered);
    let mut waiting = mixed(&third, 7);
    control.inner.update(|state| {
        let ledger = state.joint.as_mut().unwrap();
        ledger.facts_revision = u64::MAX;
    });
    third.release_part(bytes(1)).unwrap();
    drive(&mut driver);
    assert_eq!(held(&control), (4, 34));
    assert!(matches!(
        poll(&mut unreceived),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Closed
        }))
    ));
    assert!(matches!(
        poll(&mut waiting),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Closed
        }))
    ));
    assert_eq!(
        delivered.adjustment.unwrap().install(&[&first]),
        Err(WorkError::Closed)
    );
    assert_eq!(delivered.new_claims[0].amounts().unwrap(), bytes(5));
    assert_eq!(third.amounts().unwrap(), bytes(9));
    drop((first, second, third, delivered.new_claims));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn same_domain_other_owner_release_is_not_restored_by_transaction_commit() {
    let (control, handle, mut driver) = control();
    let first = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let second = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let a = dependency(&handle, "a");
    let mut initial = request(
        &handle,
        &first.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    drive(&mut driver);
    let initial = granted(&mut initial);
    let mut next = request(
        &handle,
        &second.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    driver.step();
    driver.step();
    driver.step(); // Same-key old amount copied.
    drop(initial);
    first.owner.complete();
    first.business.release();
    drive(&mut driver);
    let next = granted(&mut next);
    assert_eq!(held(&control), (1, 1));
    // A stale aggregate would still contain 2 and falsely block this request.
    let third = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let mut last = request(
        &handle,
        &third.owner.scope(),
        &[&a],
        JointLane::Foreground,
        1,
    );
    drive(&mut driver);
    let last = granted(&mut last);
    assert_eq!(held(&control), (2, 2));
    drop((next, last));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn stable_growth_at_u64_boundary_uses_actual_release_and_preserves_both_holders() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut initial = handle
        .request(
            &scope,
            vec![
                JointClaimChange::New {
                    dependencies: vec![a.clone()],
                    lane: JointLane::Foreground,
                    amounts: bytes(0),
                },
                JointClaimChange::New {
                    dependencies: vec![a],
                    lane: JointLane::Foreground,
                    amounts: bytes(u64::MAX),
                },
            ],
        )
        .unwrap();
    drive(&mut driver);
    let mut original = granted(&mut initial).into_iter();
    let zero = original.next().unwrap();
    let mut full = original.next().unwrap();
    // Reaffirm both original rows without adding a second logical amount.
    let mut reaffirm = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(zero.view().unwrap(), bytes(0), vec![]),
                JointClaimChange::grow_by(full.view().unwrap(), bytes(0), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut reaffirm).install(&[&zero, &full]).unwrap();
    assert_eq!(held(&control), (2, u64::MAX));
    full.release_part(bytes(u64::MAX)).unwrap();
    let mut growth = handle
        .request(
            &scope,
            vec![
                JointClaimChange::grow_by(zero.view().unwrap(), bytes(u64::MAX), vec![]),
                JointClaimChange::grow_by(full.view().unwrap(), bytes(0), vec![]),
            ],
        )
        .unwrap();
    drive(&mut driver);
    adjustment(&mut growth).install(&[&zero, &full]).unwrap();
    assert_eq!(zero.amounts().unwrap(), bytes(u64::MAX));
    assert_eq!(full.amounts().unwrap(), bytes(0));
    assert_eq!(held(&control), (2, u64::MAX));
    drop((zero, full));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn release_between_original_transactions_is_not_cached_or_resurrected() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 2);
    drive(&mut driver);
    let original = granted(&mut first);
    let mut second = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    driver.step();
    drop(original);
    drive(&mut driver);
    let second = granted(&mut second);
    assert_eq!(held(&control), (1, 1));
    drop(second);
}

#[test]
fn transaction_rechecks_actual_cancel_and_source_revision_before_commit() {
    for cancel in [false, true] {
        let (control, handle, mut driver) = roomy_control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "source");
        let mut initial = request(&handle, &scope, &[&a], JointLane::Foreground, 10);
        drive(&mut driver);
        let mut original = granted(&mut initial).pop().unwrap();
        let mut growth = handle
            .request(
                &scope,
                vec![JointClaimChange::grow_by(
                    original.view().unwrap(),
                    bytes(1),
                    vec![],
                )],
            )
            .unwrap();
        if cancel {
            root.owner.cancel(crate::CancellationReason::Requested);
        } else {
            original.release_part(bytes(1)).unwrap();
        }
        drive(&mut driver);
        let Poll::Ready(Err(failure)) = poll(&mut growth) else {
            panic!("original rejected transaction");
        };
        assert_eq!(
            failure.error,
            if cancel {
                WorkError::Cancelled(crate::CancellationReason::Requested)
            } else {
                WorkError::Conflict
            }
        );
        assert_eq!(
            original.amounts().unwrap(),
            bytes(if cancel { 10 } else { 9 })
        );
    }
}

#[test]
fn continuous_new_arrivals_cannot_extend_the_original_fair_pass_forever() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "original");
    let mut earlier: Vec<_> = (0..40)
        .map(|_| request(&handle, &scope, &[&a], JointLane::Foreground, 0))
        .collect();
    let mut target = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let mut arrivals = Vec::new();
    let mut received = None;
    for _ in 0..12 {
        arrivals.push(request(&handle, &scope, &[&a], JointLane::Foreground, 0));
        let before = driver.request_checks;
        driver.step();
        assert!(driver.request_checks - before <= 17);
        if let Poll::Ready(value) = poll(&mut target) {
            received = Some(value.unwrap());
            break;
        }
    }
    assert!(
        received.is_some(),
        "new arrivals must not extend the finite original pass"
    );
    for request in &mut earlier {
        drop(granted(request));
    }
    drop((received, target, arrivals, earlier));
    drive(&mut driver);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn fair_pass_end_cannot_swallow_new_ticket_or_cancellation() {
    for cancel in [false, true] {
        let (control, handle, mut driver) = control();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Management))
            .unwrap();
        let scope = root.owner.scope();
        let a = dependency(&handle, "original");
        let mut blocked = original_local_request(&handle, &scope, &a);
        drive(&mut driver);
        let mut offer = match blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(offer)) => offer,
            _ => panic!("actual original local offer"),
        };
        offer
            .reobserve_after_drop(&mut None::<()>)
            .unwrap()
            .notify();
        drop(offer);
        let later: Vec<_> = (0..20)
            .map(|_| request(&handle, &scope, &[], JointLane::Settlement, 0))
            .collect();
        // More than one actual turn is needed for the original request pass.
        let mut newer = request(&handle, &scope, &[], JointLane::Settlement, 0);
        driver.step();
        if cancel {
            root.owner.cancel(crate::CancellationReason::Requested);
        } else {
            drop(newer);
            newer = request(&handle, &scope, &[], JointLane::Settlement, 0);
        }
        drive(&mut driver);
        if cancel {
            assert!(matches!(
                blocked.poll_local_offer(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Err(JointAdmissionFailure {
                    error: WorkError::Cancelled(crate::CancellationReason::Requested)
                }))
            ));
        }
        drop(granted(&mut newer));
        drop((blocked, later));
    }
}

#[test]
fn rollback_rekeys_only_its_original_row_and_preserves_an_independent_full_domain() {
    let (control, handle, mut driver) = roomy_control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut first = request(&handle, &scope, &[&a], JointLane::Foreground, 60);
    drive(&mut driver);
    let first = granted(&mut first).pop().unwrap();
    let mut extend = handle
        .request(
            &scope,
            vec![JointClaimChange::grow_by(
                first.view().unwrap(),
                bytes(0),
                vec![b.clone()],
            )],
        )
        .unwrap();
    drive(&mut driver);
    let receipt = adjustment(&mut extend);
    let mut independent = request(&handle, &scope, &[&b], JointLane::Foreground, 50);
    drop(receipt);
    drive(&mut driver);
    let independent = granted(&mut independent);
    assert_eq!(first.amounts().unwrap(), bytes(60));
    assert_eq!(held(&control), (2, 110));
    let state = control.inner.state.lock().unwrap();
    let rows = &state.joint.as_ref().unwrap().claims;
    assert_eq!(
        rows.values()
            .filter(|row| row.facts.dependencies.len() == 1
                && row.facts.dependencies[0].fact == *a.fact())
            .count(),
        1
    );
    assert_eq!(
        rows.values()
            .filter(|row| row.facts.dependencies.len() == 1
                && row.facts.dependencies[0].fact == *b.fact())
            .count(),
        1
    );
    drop(state);
    drop((first, independent));
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn distinct_dependencies_and_zero_amount_claims_keep_independent_actual_lifetimes() {
    let (control, handle, mut driver) = control();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let scope = root.owner.scope();
    let a = dependency(&handle, "a");
    let b = dependency(&handle, "b");
    let mut one = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let mut two = request(&handle, &scope, &[&a], JointLane::Foreground, 1);
    let mut zero = request(&handle, &scope, &[&b], JointLane::Foreground, 0);
    drive(&mut driver);
    let one = granted(&mut one);
    let two = granted(&mut two);
    let zero = granted(&mut zero);
    assert_eq!(held(&control), (3, 2));
    {
        let state = control.inner.state.lock().unwrap();
        let rows = &state.joint.as_ref().unwrap().claims;
        assert_eq!(
            rows.values()
                .filter(|row| row.facts.dependencies[0].ordinal == a.record.ordinal)
                .count(),
            2
        );
        assert_eq!(
            rows.values()
                .filter(|row| row.facts.dependencies[0].ordinal == b.record.ordinal
                    && row.amounts == bytes(0))
                .count(),
            1
        );
    }
    drop(one);
    assert_eq!(held(&control), (2, 1));
    drop(zero);
    assert_eq!(held(&control), (1, 1));
    drop(two);
    assert_eq!(held(&control), (0, 0));
}

#[test]
fn local_offer_waits_for_original_transaction_and_refuses_adjustment_without_mutating_sources() {
    let (control, handle, mut driver) = control();
    let work = control
        .try_begin_root(WorkRequest::new(WorkClass::Management))
        .unwrap();
    let dep = dependency(&handle, "local");
    let mut waiting = request(
        &handle,
        &work.owner.scope(),
        &[&dep],
        JointLane::Foreground,
        1,
    );
    assert!(matches!(
        waiting.poll_local_offer(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(held(&control), (0, 0));
    drive(&mut driver);
    let original = granted(&mut waiting).pop().unwrap();
    let mut growth = handle
        .request(
            &work.owner.scope(),
            vec![JointClaimChange::grow_by(
                original.view().unwrap(),
                bytes(1),
                vec![],
            )],
        )
        .unwrap();
    assert!(matches!(
        growth.poll_local_offer(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(JointAdmissionFailure {
            error: WorkError::Conflict
        }))
    ));
    assert_eq!(original.amounts().unwrap(), bytes(1));
    drop(growth);
    assert_eq!(original.amounts().unwrap(), bytes(1));
    assert_eq!(held(&control), (1, 1));
    drop(original);
    work.business.release();
    work.owner.complete();
}
