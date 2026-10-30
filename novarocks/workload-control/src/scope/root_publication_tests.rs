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

//! Actual root custody is installed before arbitrary post-State wakes.

use super::*;
use crate::{ResultCapacityConfig, ResultWindowClass, joint_admission::JointProgress};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::AtomicBool,
    task::{Context, Waker},
};

struct PanicOnce(AtomicBool);
impl std::task::Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional root publication wake panic");
        }
    }
}

#[test]
fn stable_root_and_warehouse_receivers_keep_the_original_owner_on_wake_unwind() {
    for warehouse in [false, true] {
        let (control, _, mut driver) = fixed_control_storage_tests::prototype_profile(2, 1, 1, 1);
        let handle = control.root_admission();
        let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
        let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&wake))
                .is_pending()
        );
        // These are the original receiver locations, outside the unwinding call.
        let mut root = None;
        let mut pending = None;
        let panic = catch_unwind(AssertUnwindSafe(|| {
            if warehouse {
                handle.begin_warehouse_root_in(WorkRequest::new(WorkClass::Query), &mut pending)
            } else {
                handle.try_begin_root_in(WorkRequest::new(WorkClass::Query), &mut root)
            }
        }));
        assert!(panic.is_err());
        drop(waiter);
        let owner = if warehouse {
            assert!(root.is_none());
            pending.as_ref().unwrap().owner.scope()
        } else {
            assert!(pending.is_none());
            root.as_ref().unwrap().owner.scope()
        };
        let id = owner.id();
        let last_view = owner.cancellation().unwrap();
        let slot = {
            let state = control.inner.state.lock().unwrap();
            assert_eq!(state.nodes[&id].owner, OwnerState::Active);
            assert_eq!(state.roots, 1);
            assert_eq!(state.businesses, usize::from(!warehouse));
            state.nodes[&id].cancellation
        };
        if warehouse {
            pending.take().unwrap().owner.complete();
        } else {
            let root = root.take().unwrap();
            root.business.release();
            root.owner.complete();
        }
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                break;
            }
        }
        {
            let state = control.inner.state.lock().unwrap();
            assert!(!state.nodes.contains_key(&id));
            assert_eq!(
                state.cancellations.slots[slot.0].phase,
                crate::control_storage::SlotPhase::Retired
            );
        }
        drop((last_view, owner));
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                break;
            }
        }
        assert!(control.inner.state.lock().unwrap().cancellations.drained());
    }
}

#[test]
fn stable_result_receiver_keeps_both_original_grants_on_wake_unwind() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    let mut waiter = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    let mut destination = None;
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            control.root_admission().try_begin_root_with_result_in(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
                &mut destination,
            )
        }))
        .is_err()
    );
    drop(waiter);
    let (root, window) = destination.take().unwrap();
    let id = root.owner.scope().id();
    assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&id].owner,
        OwnerState::Active
    );
    root.business.release();
    root.owner.complete();
    assert!(control.inner.state.lock().unwrap().nodes.contains_key(&id));
    drop(window);
    let state = control.inner.state.lock().unwrap();
    assert!(!state.nodes.contains_key(&id));
    assert_eq!(state.roots, 0);
    drop(state);
    assert_eq!(capacity.snapshot().held_positions, [0; 4]);
}

#[test]
fn publication_refusal_never_replaces_an_existing_receiver_or_publishes_partial_window() {
    let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
        .unwrap()
        .owner;
    let handle = control.root_admission();
    let mut root = None;
    assert_eq!(
        handle.try_begin_root_in(WorkRequest::new(WorkClass::Management), &mut root),
        Err(WorkError::NotReady)
    );
    assert!(root.is_none());
    control.mark_ready().unwrap();
    let mut window = None;
    assert_eq!(
        handle.try_begin_root_with_result_in(
            WorkRequest::new(WorkClass::Management),
            ResultWindowClass::Local,
            &mut window
        ),
        Err(WorkError::NotReady)
    );
    assert!(window.is_none());
    assert_eq!(control.inner.state.lock().unwrap().roots, 0);
    handle
        .try_begin_root_in(WorkRequest::new(WorkClass::Management), &mut root)
        .unwrap();
    let original = root.as_ref().unwrap().owner.scope().id();
    assert_eq!(
        handle.try_begin_root_in(WorkRequest::new(WorkClass::Management), &mut root),
        Err(WorkError::Conflict)
    );
    assert_eq!(root.as_ref().unwrap().owner.scope().id(), original);
    assert_eq!(control.inner.state.lock().unwrap().roots, 1);
    let received = root.take().unwrap();
    received.business.release();
    received.owner.complete();
    let mut pending = None;
    assert_eq!(
        handle.begin_warehouse_root_in(WorkRequest::new(WorkClass::Management), &mut pending),
        Err(WorkError::Conflict)
    );
    assert!(pending.is_none());
    control.close_admission();
    assert_eq!(
        handle.try_begin_root_in(WorkRequest::new(WorkClass::Query), &mut root),
        Err(WorkError::Closed)
    );
}
