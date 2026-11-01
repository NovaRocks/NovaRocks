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

//! Actual fixed frame reuse, output custody, and silent stale-waker retirement.

use super::*;
use crate::{
    WorkClass, WorkRequest, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver},
    scope::fixed_control_storage_tests::prototype_profile_with_roles,
};
use std::sync::Mutex;

fn budget() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn drain(driver: &mut JointProgressDriver) {
    for _ in 0..10000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original fixed reaper did not reach idle");
}
struct Input {
    ready: Arc<AtomicBool>,
    wake: Arc<Mutex<Option<Waker>>>,
}
fn new_input(ready: bool) -> Input {
    Input {
        ready: Arc::new(AtomicBool::new(ready)),
        wake: Arc::new(Mutex::new(None)),
    }
}
async fn run(owner: WorkOwner, input: Input) -> u8 {
    std::future::poll_fn(|cx| {
        if input.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *input.wake.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    })
    .await;
    owner.complete();
    7
}
fn new_root(control: &WorkloadControl) -> crate::scope::RootWork {
    control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap()
}

#[tokio::test]
async fn original_frame_and_anchor_reuse_wait_for_output_and_silent_stale_waker_exit() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let observer = root.owner.retirement_observer().unwrap();
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    let anchor =
        Arc::as_ptr(&control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].wake)
            as usize;
    let input = new_input(false);
    let ready = Arc::clone(&input.ready);
    let saved = Arc::clone(&input.wake);
    root.business.release();
    bank.mount_owned(root.owner, input)
        .unwrap_or_else(|_| panic!("original mount must fit"));
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    let frame = std::ptr::from_ref(
        bank.positions[slot.0]
            .frame
            .as_ref()
            .unwrap()
            .as_ref()
            .get_ref(),
    ) as usize;
    let stale = saved.lock().unwrap().take().unwrap();
    ready.store(true, Ordering::Release);
    stale.wake_by_ref();
    let completion = bank
        .progress_turn(NonZeroUsize::new(2).unwrap())
        .map(|turn| turn.into_parts().1)
        .unwrap();
    let PollCompletion::Ready(output) = completion else {
        panic!("actual future must complete");
    };
    assert_eq!(*output.value(), 7);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .values()
            .any(|node| node.completed)
    );
    drop(output);
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    drain(&mut driver);
    let marker = control.progress_revision();
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(
        control.progress_revision(),
        marker,
        "negative uniqueness checks cannot fake progress"
    );
    drop(stale);
    assert_eq!(
        control.progress_revision(),
        marker,
        "std Arc Waker drop is silent"
    );
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert_eq!(
        std::ptr::from_ref(
            bank.positions[slot.0]
                .frame
                .as_ref()
                .unwrap()
                .as_ref()
                .get_ref(),
        ) as usize,
        frame
    );
    assert_eq!(
        Arc::as_ptr(&control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].wake)
            as usize,
        anchor
    );
    let next = new_root(&control);
    let next_slot =
        control.inner.state.lock().unwrap().nodes[&next.owner.scope().id()].cancellation;
    assert_eq!(next_slot, slot);
    next.business.release();
    bank.mount_owned(next.owner, new_input(true))
        .unwrap_or_else(|_| panic!("same original frame must be reusable"));
    drop(
        bank.progress_turn(NonZeroUsize::new(2).unwrap())
            .map(|turn| turn.into_parts().1)
            .unwrap(),
    );
    assert_eq!(
        std::ptr::from_ref(
            bank.positions[slot.0]
                .frame
                .as_ref()
                .unwrap()
                .as_ref()
                .get_ref(),
        ) as usize,
        frame,
        "the next actual Work uses the same original frame allocation"
    );
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn bounded_round_robin_polls_a_ready_peer_while_the_first_future_is_pending() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let first = new_root(&control);
    let second = new_root(&control);
    let blocked = new_input(false);
    let ready = Arc::clone(&blocked.ready);
    let wake = Arc::clone(&blocked.wake);
    first.business.release();
    second.business.release();
    bank.mount_owned(first.owner, blocked)
        .unwrap_or_else(|_| panic!("first mount"));
    bank.mount_owned(second.owner, new_input(true))
        .unwrap_or_else(|_| panic!("second mount"));
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    let PollCompletion::Ready(peer) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("ready peer must not wait for first");
    };
    assert_eq!(*peer.value(), 7);
    drop(peer);
    ready.store(true, Ordering::Release);
    wake.lock().unwrap().take().unwrap().wake();
    drop(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap(),
    );
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct PanicOnce(AtomicBool);
impl Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        if self.0.swap(false, Ordering::AcqRel) {
            panic!("intentional post-mount progress wake panic");
        }
    }
}
#[tokio::test]
async fn post_state_wake_unwind_keeps_the_actual_owner_and_input_in_the_bank() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    root.business.release();
    let work = root.owner.scope().id();
    let wake = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(
            || bank.mount_owned(root.owner, new_input(true))
        ))
        .is_err()
    );
    drop(progress);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Active
    );
    let PollCompletion::Ready(output) = bank
        .progress_turn(NonZeroUsize::new(2).unwrap())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("same retained input must run");
    };
    assert_eq!(*output.value(), 7);
    drop(output);
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn foreign_owner_rejection_returns_the_same_unexecuted_owner_and_input() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let foreign = WorkloadControl::try_new_counted(crate::WorkloadConfig::default())
        .unwrap()
        .owner;
    foreign.mark_ready().unwrap();
    let root = new_root(&foreign);
    let id = root.owner.scope().id();
    let original = new_input(false);
    let actual = Arc::clone(&original.ready);
    let rejected = match bank.mount_owned(root.owner, original) {
        Err(rejected) => rejected,
        Ok(()) => panic!("foreign owner must reject"),
    };
    assert_eq!(rejected.error, WorkError::Conflict);
    assert_eq!(rejected.owner.scope().id(), id);
    assert!(Arc::ptr_eq(&rejected.input.ready, &actual));
    root.business.release();
    rejected.owner.complete();
    drop(rejected.input);
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    assert!(foreign.inner.state.lock().unwrap().nodes.is_empty());
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct ExitCheck {
    inner: Arc<Inner>,
    actual_drop: Arc<AtomicBool>,
}
impl Drop for ExitCheck {
    fn drop(&mut self) {
        self.actual_drop
            .store(self.inner.state.try_lock().is_ok(), Ordering::Release);
    }
}
struct FailureInput {
    panic: bool,
    exit: ExitCheck,
}
async fn run_failure(owner: WorkOwner, input: FailureInput) -> u8 {
    let _exit = input.exit;
    owner.complete();
    if input.panic {
        panic!("intentional isolated role poll panic");
    }
    9
}
#[tokio::test]
async fn poll_panic_clears_the_real_frame_outside_state_and_preserves_peer_progress() {
    let roles = [PollRoleGeometry::for_factory(&run_failure).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run_failure).unwrap();
    control.mark_ready().unwrap();
    let first = new_root(&control);
    let peer = new_root(&control);
    let work = first.owner.scope().id();
    let first_exit = Arc::new(AtomicBool::new(false));
    let peer_exit = Arc::new(AtomicBool::new(false));
    first.business.release();
    peer.business.release();
    bank.mount_owned(
        first.owner,
        FailureInput {
            panic: true,
            exit: ExitCheck {
                inner: Arc::clone(&control.inner),
                actual_drop: Arc::clone(&first_exit),
            },
        },
    )
    .unwrap_or_else(|_| panic!("first mount"));
    bank.mount_owned(
        peer.owner,
        FailureInput {
            panic: false,
            exit: ExitCheck {
                inner: Arc::clone(&control.inner),
                actual_drop: Arc::clone(&peer_exit),
            },
        },
    )
    .unwrap_or_else(|_| panic!("peer mount"));
    let PollCompletion::Panicked(failed) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("panic must be an isolated completion");
    };
    assert!(
        first_exit.load(Ordering::Acquire),
        "actual frame fields exit outside State"
    );
    assert!(control.inner.state.lock().unwrap().nodes[&work].completed);
    let PollCompletion::Ready(healthy) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("healthy peer still progresses");
    };
    assert_eq!(*healthy.value(), 9);
    assert!(peer_exit.load(Ordering::Acquire));
    drop((failed, healthy));
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn factory_panic_does_not_destroy_an_owner_already_received_by_its_stable_holder() {
    let held = Arc::new(Mutex::new(None::<WorkOwner>));
    let receiver = Arc::clone(&held);
    let make = move |owner: WorkOwner, (): ()| -> std::future::Ready<()> {
        *receiver.lock().unwrap() = Some(owner);
        panic!("intentional role factory panic after original owner handoff");
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, make).unwrap();
    control.mark_ready().unwrap();
    let mut original_frame = None;
    for _ in 0..2 {
        let root = new_root(&control);
        let work = root.owner.scope().id();
        let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
        root.business.release();
        bank.mount_owned(root.owner, ())
            .unwrap_or_else(|_| panic!("original mount"));
        let PollCompletion::Panicked(failed) = bank
            .progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap()
        else {
            panic!("factory panic must be isolated");
        };
        assert_eq!(
            failed.value().primary().downcast_ref::<&'static str>(),
            Some(&"intentional role factory panic after original owner handoff")
        );
        let frame = bank.positions[slot.0]
            .frame
            .as_ref()
            .unwrap()
            .as_ref()
            .get_ref();
        assert!(frame.is_none());
        let address = (slot, std::ptr::from_ref(frame) as usize);
        if let Some(first) = original_frame {
            assert_eq!(
                address, first,
                "factory failure keeps the original empty frame"
            );
        } else {
            original_frame = Some(address);
        }
        assert!(failed.value().cleanup().is_none());
        assert!(failed.value().output().is_none());
        assert_eq!(held.lock().unwrap().as_ref().unwrap().scope().id(), work);
        assert_eq!(
            control.inner.state.lock().unwrap().nodes[&work].owner,
            crate::OwnerState::Active
        );
        let owner = held.lock().unwrap().take().unwrap();
        owner.complete();
        assert!(
            control
                .inner
                .state
                .lock()
                .unwrap()
                .nodes
                .contains_key(&work),
            "original footer is still actual custody"
        );
        drop(failed);
        drain(&mut driver);
    }
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn original_driver_reclaims_output_after_bank_exit_including_silent_stale_waker() {
    for retain_stale in [false, true] {
        let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
        let bank_observer = bank.retirement_observer();
        control.mark_ready().unwrap();
        let root = new_root(&control);
        let observer = root.owner.retirement_observer().unwrap();
        let input = new_input(false);
        let ready = Arc::clone(&input.ready);
        let saved = Arc::clone(&input.wake);
        root.business.release();
        bank.mount_owned(root.owner, input)
            .unwrap_or_else(|_| panic!("original mount"));
        assert!(
            bank.progress_turn(NonZeroUsize::new(2).unwrap())
                .map(|turn| turn.into_parts().1)
                .is_none()
        );
        let stale = saved.lock().unwrap().take().unwrap();
        ready.store(true, Ordering::Release);
        stale.wake_by_ref();
        let PollCompletion::Ready(output) = bank
            .progress_turn(NonZeroUsize::new(2).unwrap())
            .map(|turn| turn.into_parts().1)
            .unwrap()
        else {
            panic!("original completion");
        };
        let stale = retain_stale.then_some(stale);
        drop(bank);
        assert_eq!(control.mark_ready(), Err(WorkError::Closed));
        assert!(matches!(
            control.try_begin_root(WorkRequest::new(WorkClass::Query)),
            Err(WorkError::Closed)
        ));
        drain(&mut driver);
        assert_eq!(
            observer.is_reclaimed(),
            Ok(false),
            "the real output still holds its footer"
        );
        assert_eq!(bank_observer.is_reclaimed(), Ok(false));
        assert_eq!(*output.value(), 7);
        drop(output);
        drain(&mut driver);
        if retain_stale {
            assert_eq!(observer.is_reclaimed(), Ok(false));
            assert_eq!(bank_observer.is_reclaimed(), Ok(false));
            let marker = control.progress_revision();
            driver.recheck_retired_backing();
            drain(&mut driver);
            assert_eq!(
                control.progress_revision(),
                marker,
                "negative timer pass is silent"
            );
            drop(stale);
            assert_eq!(
                control.progress_revision(),
                marker,
                "actual last Waker drop is silent"
            );
            driver.recheck_retired_backing();
            drain(&mut driver);
        }
        assert_eq!(observer.is_reclaimed(), Ok(true));
        assert_eq!(bank_observer.is_reclaimed(), Ok(true));
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}

#[tokio::test]
async fn startup_bank_exit_revokes_registration_and_allows_only_serial_reinstallation() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let first = bank.retirement_observer();
    assert_eq!(first.is_reclaimed(), Ok(false));
    assert!(matches!(
        RolePollBank::install(&mut control, 0, run),
        Err(WorkError::Conflict)
    ));
    let old_notify = Arc::as_ptr(&bank.ready);
    drop(bank);
    assert_eq!(first.is_reclaimed(), Ok(true));
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .slots
            .iter()
            .all(|slot| slot.polls[0].wake.changed.as_ptr() != old_notify),
        "all original weak anchors leave before replacement allocation"
    );
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let second = bank.retirement_observer();
    assert!(second.generation > first.generation);
    assert_eq!(first.is_reclaimed(), Ok(true));
    assert_eq!(second.is_reclaimed(), Ok(false));
    control.mark_ready().unwrap();
    drop(bank);
    assert_eq!(control.mark_ready(), Err(WorkError::Closed));
    assert_eq!(first.is_reclaimed(), Ok(true));
    assert_eq!(second.is_reclaimed(), Ok(true));
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn dropping_a_mounted_bank_preserves_orphaned_work_until_the_original_owner_recovers_it() {
    let called = Arc::new(AtomicBool::new(false));
    let actual_called = Arc::clone(&called);
    let make = move |owner, input| {
        actual_called.store(true, Ordering::Release);
        run(owner, input)
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, make).unwrap();
    let bank_observer = bank.retirement_observer();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let observer = root.owner.retirement_observer().unwrap();
    root.business.release();
    bank.mount_owned(root.owner, new_input(true))
        .unwrap_or_else(|_| panic!("original mount"));
    // Leave the actual owner in the unpolled input cell, then destroy the bank.
    assert!(
        bank.positions
            .iter()
            .all(|position| position.frame.is_none())
    );
    drop(bank);
    assert!(
        !called.load(Ordering::Acquire),
        "unpolled teardown never constructs F"
    );
    drain(&mut driver);
    {
        let state = control.inner.state.lock().unwrap();
        assert_eq!(state.nodes[&work].owner, crate::OwnerState::Orphaned);
        assert!(!state.nodes[&work].completed);
        assert_eq!(state.cancellations.retired_polls, 0);
    }
    assert_eq!(bank_observer.is_reclaimed(), Ok(true));
    assert_eq!(observer.is_reclaimed(), Ok(false));
    control.adopt(work).unwrap().complete();
    drain(&mut driver);
    assert_eq!(
        observer.is_reclaimed(),
        Ok(false),
        "ordinary completion retains the original Cancel intent"
    );
    let cancel = control.next_control().unwrap();
    assert_eq!(cancel.scope().id(), work);
    assert!(cancel.intents().contains(crate::ControlIntent::Cancel));
    cancel.acknowledge();
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn startup_teardown_refuses_readiness_while_actual_factory_destructor_is_paused() {
    struct PausedDrop {
        entered: Arc<std::sync::Barrier>,
        resume: Arc<std::sync::Barrier>,
    }
    impl Drop for PausedDrop {
        fn drop(&mut self) {
            self.entered.wait();
            self.resume.wait();
        }
    }
    let entered = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));
    let held = PausedDrop {
        entered: Arc::clone(&entered),
        resume: Arc::clone(&resume),
    };
    let make = move |owner: WorkOwner, input: Input| {
        let _actual_factory_custody = &held;
        run(owner, input)
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, make).unwrap();
    let observer = bank.retirement_observer();
    std::thread::scope(|threads| {
        let dropping = threads.spawn(move || drop(bank));
        entered.wait();
        assert_eq!(observer.is_reclaimed(), Ok(false));
        assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
        {
            let state = control.inner.state.lock().unwrap();
            assert!(!state.ready);
            assert!(
                !state.closed,
                "startup teardown does not fake a ready-service loss"
            );
            assert!(
                state
                    .cancellations
                    .slots
                    .iter()
                    .all(|slot| slot.polls[0].registration == RegistrationPhase::Stopping)
            );
        }
        resume.wait();
        dropping.join().unwrap();
    });
    assert_eq!(control.mark_ready(), Err(WorkError::NotReady));
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .slots
            .iter()
            .all(|slot| slot.polls[0].registration == RegistrationPhase::Absent)
    );
    assert_eq!(observer.is_reclaimed(), Ok(true));
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn one_high_slot_wake_is_ready_without_scanning_the_empty_prefix() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(512, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let mut roots: Vec<_> = (0..501).map(|_| new_root(&control)).collect();
    let root = roots.pop().unwrap();
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    assert_eq!(slot.0, 500);
    root.business.release();
    bank.mount_owned(root.owner, new_input(true))
        .unwrap_or_else(|_| panic!("original mount"));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(bank.poll_ready(&mut cx).is_ready());
    let output = bank
        .progress_turn(NonZeroUsize::new(7).unwrap())
        .map(|turn| turn.into_parts().1)
        .expect("one real hint reaches slot 500");
    drop(output);
    for root in roots {
        root.business.release();
        root.owner.complete();
    }
    drain(&mut driver);
}

#[tokio::test]
async fn coalesced_burst_remains_ready_after_the_notification_is_consumed() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(4, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(bank.poll_ready(&mut cx).is_pending());
    for _ in 0..3 {
        let root = new_root(&control);
        root.business.release();
        bank.mount_owned(root.owner, new_input(true))
            .unwrap_or_else(|_| panic!("original mount"));
    }
    // Consume the actual coalesced event and replace its one owned wait frame.
    // Queue readiness must remain independently visible without another wake.
    assert!(bank.waiter.as_mut().poll(&mut cx).is_ready());
    bank.waiter
        .as_mut()
        .set(Arc::clone(&bank.changed).notified_owned());
    for _ in 0..3 {
        assert!(bank.poll_ready(&mut cx).is_ready());
        drop(
            bank.progress_turn(budget())
                .map(|turn| turn.into_parts().1)
                .expect("remaining original queued role"),
        );
    }
    assert!(bank.ready.is_empty());
    drain(&mut driver);
}

struct SelfWakeInput {
    pending_polls: usize,
    wake_on_ready: bool,
    order: Arc<Mutex<Vec<WorkId>>>,
}
struct SelfWakeRole {
    owner: Option<WorkOwner>,
    input: SelfWakeInput,
}
fn make_self_wake(owner: WorkOwner, input: SelfWakeInput) -> SelfWakeRole {
    SelfWakeRole {
        owner: Some(owner),
        input,
    }
}
impl Future for SelfWakeRole {
    type Output = WorkId;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<WorkId> {
        let work = self.owner.as_ref().unwrap().scope().id();
        self.input.order.lock().unwrap().push(work);
        if self.input.pending_polls > 0 {
            self.input.pending_polls -= 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if self.input.wake_on_ready {
            cx.waker().wake_by_ref();
        }
        self.owner.take().unwrap().complete();
        Poll::Ready(work)
    }
}

#[tokio::test]
async fn self_wake_requeues_at_the_tail_before_pending_and_preserves_peer_turns() {
    let roles = [PollRoleGeometry::for_factory(&make_self_wake).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, make_self_wake).unwrap();
    control.mark_ready().unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let hot = new_root(&control);
    let peer = new_root(&control);
    let hot_id = hot.owner.scope().id();
    let peer_id = peer.owner.scope().id();
    hot.business.release();
    peer.business.release();
    bank.mount_owned(
        hot.owner,
        SelfWakeInput {
            pending_polls: 2,
            wake_on_ready: false,
            order: Arc::clone(&order),
        },
    )
    .unwrap_or_else(|_| panic!("original hot mount"));
    bank.mount_owned(
        peer.owner,
        SelfWakeInput {
            pending_polls: 0,
            wake_on_ready: false,
            order: Arc::clone(&order),
        },
    )
    .unwrap_or_else(|_| panic!("original peer mount"));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    assert!(bank.poll_ready(&mut cx).is_ready());
    let PollCompletion::Ready(peer) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("peer completion")
    };
    assert_eq!(*peer.value(), peer_id);
    drop(peer);
    assert_eq!(*order.lock().unwrap(), [hot_id, peer_id]);
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    assert!(bank.poll_ready(&mut cx).is_ready());
    drop(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap(),
    );
    assert_eq!(*order.lock().unwrap(), [hot_id, peer_id, hot_id, hot_id]);
    drain(&mut driver);
}

#[tokio::test]
async fn queued_completion_wake_keeps_the_original_anchor_retired_until_consumed() {
    let roles = [PollRoleGeometry::for_factory(&make_self_wake).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, make_self_wake).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let observer = root.owner.retirement_observer().unwrap();
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    root.business.release();
    bank.mount_owned(
        root.owner,
        SelfWakeInput {
            pending_polls: 0,
            wake_on_ready: true,
            order: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .unwrap_or_else(|_| panic!("original mount"));
    drop(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap(),
    );
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].phase,
        PollPhase::Retired
    );
    assert!(
        bank.poll_ready(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none(),
        "stale item does not repoll the completed future"
    );
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
}

#[tokio::test]
async fn root_drain_keeps_the_admitted_owner_mountable_until_its_actual_exit() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    control.close_root_admission();
    root.business.release();
    bank.mount_owned(root.owner, new_input(true))
        .unwrap_or_else(|_| panic!("admitted owner retains its original role"));
    assert!(
        bank.poll_ready(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
    let output = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .expect("original admitted role ran");
    assert_eq!(control.snapshot().root_responsibilities, 1);
    drop(output);
    drain(&mut driver);
    assert_eq!(control.snapshot().root_responsibilities, 0);
    drop(bank);
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

#[tokio::test]
async fn closed_bank_mount_returns_the_original_owner_and_input_without_queueing() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let input = new_input(true);
    let original = Arc::clone(&input.ready);
    control.close_admission();
    let rejected = bank
        .mount_owned(root.owner, input)
        .expect_err("closed mount refusal");
    assert_eq!(rejected.error, WorkError::Closed);
    assert_eq!(rejected.owner.scope().id(), work);
    assert!(Arc::ptr_eq(&rejected.input.ready, &original));
    assert!(bank.ready.is_empty());
    rejected.owner.complete();
    root.business.release();
    drain(&mut driver);
}

#[tokio::test]
async fn closed_mount_refusal_never_calls_unrelated_waiter_wakes() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
    control.close_admission();
    let panic = Arc::new(PanicOnce(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&panic));
    let mut cx = Context::from_waker(&waker);
    assert!(bank.poll_ready(&mut cx).is_pending());
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(progress.as_mut().poll(&mut cx).is_pending());
    let attempt = catch_unwind(AssertUnwindSafe(|| {
        bank.mount_owned(root.owner, new_input(true))
    }));
    let panicked = attempt.is_err();
    // Failure cleanup uses the real retained owner if this regression fires.
    // It does not hide the final assertion or reconstruct authority from an ID.
    panic.0.store(false, Ordering::Release);
    drop(progress);
    let owner = match attempt {
        Ok(Err(rejected)) => {
            assert_eq!(rejected.error, WorkError::Closed);
            assert_eq!(rejected.owner.scope().id(), work);
            rejected.owner
        }
        Err(_) => {
            bank.shared.inputs.lock().unwrap()[slot.0]
                .take()
                .expect("stranded original owner")
                .owner
        }
        Ok(Ok(())) => panic!("closed mount must refuse"),
    };
    owner.complete();
    root.business.release();
    drain(&mut driver);
    assert!(
        !panicked,
        "an unpublished refusal must return before any notification callback"
    );
    assert!(bank.ready.is_empty());
}

struct TrackedRolePanic {
    stage: &'static str,
    inner: Arc<Inner>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for TrackedRolePanic {
    fn drop(&mut self) {
        assert!(
            self.inner.state.try_lock().is_ok(),
            "original panic exits outside State"
        );
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
struct TrackedRoleOutput {
    inner: Arc<Inner>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for TrackedRoleOutput {
    fn drop(&mut self) {
        assert!(
            self.inner.state.try_lock().is_ok(),
            "original output exits outside State"
        );
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
struct TrackedFailureInput {
    poll_panics: bool,
    cleanup_panics: bool,
    inner: Arc<Inner>,
    poll_drops: Arc<std::sync::atomic::AtomicUsize>,
    cleanup_drops: Arc<std::sync::atomic::AtomicUsize>,
    output_drops: Arc<std::sync::atomic::AtomicUsize>,
}
struct TrackedFailureRole {
    owner: Option<WorkOwner>,
    input: TrackedFailureInput,
}
fn make_tracked_failure(owner: WorkOwner, input: TrackedFailureInput) -> TrackedFailureRole {
    TrackedFailureRole {
        owner: Some(owner),
        input,
    }
}
impl Future for TrackedFailureRole {
    type Output = TrackedRoleOutput;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.owner.take().unwrap().complete();
        if self.input.poll_panics {
            std::panic::panic_any(TrackedRolePanic {
                stage: "poll",
                inner: Arc::clone(&self.input.inner),
                drops: Arc::clone(&self.input.poll_drops),
            });
        }
        Poll::Ready(TrackedRoleOutput {
            inner: Arc::clone(&self.input.inner),
            drops: Arc::clone(&self.input.output_drops),
        })
    }
}
impl Drop for TrackedFailureRole {
    fn drop(&mut self) {
        if self.input.cleanup_panics {
            std::panic::panic_any(TrackedRolePanic {
                stage: "cleanup",
                inner: Arc::clone(&self.input.inner),
                drops: Arc::clone(&self.input.cleanup_drops),
            });
        }
    }
}

#[tokio::test]
async fn original_poll_and_cleanup_causes_and_ready_output_keep_the_same_footer() {
    use std::sync::atomic::AtomicUsize;
    for poll_panics in [true, false] {
        let roles = [PollRoleGeometry::for_factory(&make_tracked_failure).unwrap()];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::install(&mut control, 0, make_tracked_failure).unwrap();
        control.mark_ready().unwrap();
        let failed = new_root(&control);
        let observer = failed.owner.retirement_observer().unwrap();
        let peer = new_root(&control);
        let poll_drops = Arc::new(AtomicUsize::new(0));
        let cleanup_drops = Arc::new(AtomicUsize::new(0));
        let output_drops = Arc::new(AtomicUsize::new(0));
        let input = |poll_panics, cleanup_panics| TrackedFailureInput {
            poll_panics,
            cleanup_panics,
            inner: Arc::clone(&control.inner),
            poll_drops: Arc::clone(&poll_drops),
            cleanup_drops: Arc::clone(&cleanup_drops),
            output_drops: Arc::clone(&output_drops),
        };
        failed.business.release();
        peer.business.release();
        bank.mount_owned(failed.owner, input(poll_panics, true))
            .unwrap_or_else(|_| panic!("original failed role"));
        bank.mount_owned(peer.owner, input(false, false))
            .unwrap_or_else(|_| panic!("original peer"));
        let PollCompletion::Panicked(failed) = bank
            .progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap()
        else {
            panic!("original failure packet")
        };
        let packet = failed.value();
        let primary = packet.primary().downcast_ref::<TrackedRolePanic>().unwrap();
        assert_eq!(primary.stage, if poll_panics { "poll" } else { "cleanup" });
        if poll_panics {
            assert_eq!(
                packet
                    .cleanup()
                    .unwrap()
                    .downcast_ref::<TrackedRolePanic>()
                    .unwrap()
                    .stage,
                "cleanup"
            );
            assert!(packet.output().is_none());
        } else {
            assert!(packet.cleanup().is_none());
            assert!(
                packet.output().is_some(),
                "Ready output survives its frame's failing destructor"
            );
        }
        assert_eq!(poll_drops.load(Ordering::Acquire), 0);
        assert_eq!(cleanup_drops.load(Ordering::Acquire), 0);
        assert_eq!(output_drops.load(Ordering::Acquire), 0);
        let PollCompletion::Ready(peer) = bank
            .progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap()
        else {
            panic!("peer still runs")
        };
        drop(peer);
        assert_eq!(output_drops.load(Ordering::Acquire), 1);
        drain(&mut driver);
        assert_eq!(
            observer.is_reclaimed(),
            Ok(false),
            "original packet still owns the footer"
        );
        drop(failed);
        assert_eq!(poll_drops.load(Ordering::Acquire), usize::from(poll_panics));
        assert_eq!(cleanup_drops.load(Ordering::Acquire), 1);
        assert_eq!(
            output_drops.load(Ordering::Acquire),
            if poll_panics { 1 } else { 2 }
        );
        control.close_admission();
        drain(&mut driver);
        assert_eq!(observer.is_reclaimed(), Ok(true));
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}

#[tokio::test]
async fn registration_generation_max_refuses_replacement_without_wrapping() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    {
        let mut state = control.inner.state.lock().unwrap();
        for slot in &mut state.cancellations.slots {
            slot.polls[0].registration_generation = u64::MAX - 1;
        }
    }
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let observer = bank.retirement_observer();
    assert_eq!(observer.generation, u64::MAX);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    drop(bank);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    let revision = control.progress_revision();
    assert!(matches!(
        RolePollBank::install(&mut control, 0, run),
        Err(WorkError::ArithmeticOverflow)
    ));
    assert_eq!(control.progress_revision(), revision);
    assert!(
        control
            .inner
            .state
            .lock()
            .unwrap()
            .cancellations
            .slots
            .iter()
            .all(|slot| {
                let role = &slot.polls[0];
                role.registration_generation == u64::MAX
                    && role.registration == RegistrationPhase::Absent
                    && role.phase == PollPhase::Free
            })
    );
    control.close_admission();
    drain(&mut driver);
}

#[tokio::test]
async fn vanished_original_authority_is_not_a_role_backing_exit_proof() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let observer = bank.retirement_observer();
    drop(bank);
    assert_eq!(observer.is_reclaimed(), Ok(true));
    control.close_admission();
    drain(&mut driver);
    drop(driver);
    drop(control);
    assert_eq!(observer.is_reclaimed(), Err(WorkError::Released));
}

#[tokio::test]
async fn backing_exit_does_not_claim_the_registration_notification_tail_has_joined() {
    struct PausedWake {
        entered: std::sync::Barrier,
        resume: std::sync::Barrier,
    }
    impl Wake for PausedWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.entered.wait();
            self.resume.wait();
        }
    }
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let observer = bank.retirement_observer();
    let paused = Arc::new(PausedWake {
        entered: std::sync::Barrier::new(2),
        resume: std::sync::Barrier::new(2),
    });
    let waker = Waker::from(Arc::clone(&paused));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    std::thread::scope(|threads| {
        let original = threads.spawn(move || drop(bank));
        paused.entered.wait();
        let reclaimed = observer.is_reclaimed();
        let actually_exited = original.is_finished();
        // Always let the real notification finish before asserting the join
        // boundary, so a failing assertion cannot strand the scoped thread.
        paused.resume.wait();
        original.join().unwrap();
        assert_eq!(reclaimed, Ok(true));
        assert!(
            !actually_exited,
            "backing retirement alone is not actual root join"
        );
    });
    drop(progress);
    control.close_admission();
    drain(&mut driver);
}

#[tokio::test]
async fn prepaid_frame_materializes_only_at_first_real_use_and_stays_for_reuse() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(8, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    assert!(
        bank.positions
            .iter()
            .all(|position| position.frame.is_none())
    );
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    root.business.release();
    bank.mount_owned(root.owner, new_input(true))
        .unwrap_or_else(|_| panic!("original mount"));
    assert!(
        bank.positions
            .iter()
            .all(|position| position.frame.is_none())
    );
    let PollCompletion::Ready(output) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("actual ready role");
    };
    assert_eq!(*output.value(), 7);
    for (index, position) in bank.positions.iter().enumerate() {
        assert_eq!(position.frame.is_some(), index == slot.0);
    }
    let frame = std::ptr::from_ref(
        bank.positions[slot.0]
            .frame
            .as_ref()
            .unwrap()
            .as_ref()
            .get_ref(),
    );
    drop(output);
    drain(&mut driver);
    let next = new_root(&control);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&next.owner.scope().id()].cancellation,
        slot
    );
    next.business.release();
    bank.mount_owned(next.owner, new_input(true))
        .unwrap_or_else(|_| panic!("same original frame"));
    let PollCompletion::Ready(output) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("actual reused role");
    };
    assert_eq!(*output.value(), 7);
    assert_eq!(
        std::ptr::from_ref(
            bank.positions[slot.0]
                .frame
                .as_ref()
                .unwrap()
                .as_ref()
                .get_ref(),
        ),
        frame
    );
    drop(output);
    control.close_admission();
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().is_drained());
}

struct IngressSources {
    owner: Option<WorkOwner>,
    input: Option<Input>,
}
struct IngressCallbackWake {
    sources: Weak<Mutex<IngressSources>>,
    shared: Weak<SharedIngress<Input>>,
    inner: Weak<Inner>,
    calls: std::sync::atomic::AtomicUsize,
    panic_once: AtomicBool,
}
impl Wake for IngressCallbackWake {
    fn wake(self: Arc<Self>) {
        assert!(self.sources.upgrade().unwrap().try_lock().is_ok());
        assert!(self.inner.upgrade().unwrap().state.try_lock().is_ok());
        if let Some(shared) = self.shared.upgrade() {
            assert!(shared.inputs.try_lock().is_ok());
            assert!(shared.ready.lock_is_available());
        }
        self.calls.fetch_add(1, Ordering::AcqRel);
        if self.panic_once.swap(false, Ordering::AcqRel) {
            panic!("intentional original ingress sender-tail callback failure");
        }
    }
}

#[tokio::test]
async fn synchronous_ingress_moves_original_sources_before_unlocked_notifications() {
    use std::future::Future;
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let port = bank.ingress().unwrap();
    let producer = port.clone();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let input = new_input(true);
    let original = Arc::clone(&input.ready);
    root.business.release();
    let sources = Arc::new(Mutex::new(IngressSources {
        owner: Some(root.owner),
        input: Some(input),
    }));
    let wake = Arc::new(IngressCallbackWake {
        sources: Arc::downgrade(&sources),
        shared: Arc::downgrade(&bank.shared),
        inner: Arc::downgrade(&control.inner),
        calls: std::sync::atomic::AtomicUsize::new(0),
        panic_once: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let attempt = {
        let mut sources = sources.lock().unwrap();
        let IngressSources { owner, input } = &mut *sources;
        let attempt = producer.try_mount_in(owner, input, &mut Some(()));
        assert_eq!(attempt.result(), &Ok(()));
        assert!(owner.is_none() && input.is_none());
        assert_eq!(wake.calls.load(Ordering::Acquire), 0);
        attempt
    };
    let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
    {
        let inputs = bank.shared.inputs.lock().unwrap();
        let mounted = inputs[slot.0].as_ref().unwrap();
        assert_eq!(mounted.owner.scope().id(), work);
        assert!(Arc::ptr_eq(&mounted.input.ready, &original));
    }
    drop(producer);
    attempt.finish().unwrap();
    assert!(wake.calls.load(Ordering::Acquire) > 0);
    drop(progress);
    assert!(
        bank.positions
            .iter()
            .all(|position| position.frame.is_none())
    );
    let PollCompletion::Ready(output) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("original factory must run");
    };
    assert_eq!(*output.value(), 7);
    drop(output);
    drain(&mut driver);
    assert!(
        !control
            .inner
            .state
            .lock()
            .unwrap()
            .nodes
            .contains_key(&work)
    );
}

#[tokio::test]
async fn dropping_a_successful_sender_wakes_the_waiting_root_and_preserves_input_on_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    for panic_callback in [false, true] {
        let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
        let port = bank.ingress().unwrap();
        control.mark_ready().unwrap();
        let root = new_root(&control);
        let work = root.owner.scope().id();
        let input = new_input(true);
        let original = Arc::clone(&input.ready);
        root.business.release();
        let sources = Arc::new(Mutex::new(IngressSources {
            owner: Some(root.owner),
            input: Some(input),
        }));
        let wake = Arc::new(IngressCallbackWake {
            sources: Arc::downgrade(&sources),
            shared: Arc::downgrade(&bank.shared),
            inner: Arc::downgrade(&control.inner),
            calls: std::sync::atomic::AtomicUsize::new(0),
            panic_once: AtomicBool::new(panic_callback),
        });
        let waker = Waker::from(Arc::clone(&wake));
        assert!(
            bank.poll_ready(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let attempt = {
            let mut sources = sources.lock().unwrap();
            let IngressSources { owner, input } = &mut *sources;
            let attempt = port.try_mount_in(owner, input, &mut Some(()));
            assert_eq!(attempt.result(), &Ok(()));
            assert!(owner.is_none() && input.is_none());
            assert_eq!(wake.calls.load(Ordering::Acquire), 0);
            attempt
        };
        // No finish or bank teardown supplies the missing notification.
        assert_eq!(
            catch_unwind(AssertUnwindSafe(|| drop(attempt))).is_err(),
            panic_callback
        );
        assert_eq!(wake.calls.load(Ordering::Acquire), 1);
        let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
        {
            let inputs = bank.shared.inputs.lock().unwrap();
            let mounted = inputs[slot.0].as_ref().unwrap();
            assert_eq!(mounted.owner.scope().id(), work);
            assert!(Arc::ptr_eq(&mounted.input.ready, &original));
        }
        assert!(bank.poll_ready(&mut Context::from_waker(&waker)).is_ready());
        let PollCompletion::Ready(output) = bank
            .progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .unwrap()
        else {
            panic!("accepted input must run through its original factory");
        };
        assert_eq!(*output.value(), 7);
        drop(output);
        drain(&mut driver);
        assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    }
}

#[tokio::test]
async fn refused_sender_tail_exits_after_external_source_unlock_even_if_it_panics() {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
    };
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let port = bank.ingress().unwrap();
    let observer = bank.retirement_observer();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let input = new_input(true);
    let original = Arc::clone(&input.ready);
    root.business.release();
    let sources = Arc::new(Mutex::new(IngressSources {
        owner: Some(root.owner),
        input: Some(input),
    }));
    bank.stop_ingress();
    let wake = Arc::new(IngressCallbackWake {
        sources: Arc::downgrade(&sources),
        shared: Arc::downgrade(&bank.shared),
        inner: Arc::downgrade(&control.inner),
        calls: std::sync::atomic::AtomicUsize::new(0),
        panic_once: AtomicBool::new(true),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let mut progress = Box::pin(control.wait_progress(control.progress_revision()));
    assert!(
        progress
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let attempt = {
        let mut sources = sources.lock().unwrap();
        let IngressSources { owner, input } = &mut *sources;
        let attempt = port.try_mount_in(owner, input, &mut Some(()));
        assert_eq!(attempt.result(), &Err(WorkError::Closed));
        assert!(owner.is_some() && input.is_some());
        drop(bank);
        assert_eq!(
            observer.is_reclaimed(),
            Ok(false),
            "the original sender still retains late registration"
        );
        assert_eq!(wake.calls.load(Ordering::Acquire), 0);
        attempt
    };
    assert!(catch_unwind(AssertUnwindSafe(|| attempt.finish())).is_err());
    drop(progress);
    let mut sources = sources.lock().unwrap();
    assert_eq!(sources.owner.as_ref().unwrap().scope().id(), work);
    assert!(Arc::ptr_eq(
        &sources.input.as_ref().unwrap().ready,
        &original
    ));
    sources.owner.take().unwrap().complete();
    drop(sources);
    drain(&mut driver);
    assert_eq!(observer.is_reclaimed(), Ok(true));
}

#[tokio::test]
async fn accepted_unpolled_input_survives_root_exit_until_original_sender_exits() {
    let called = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&called);
    let make = move |owner: WorkOwner, input: Input| {
        seen.store(true, Ordering::Release);
        run(owner, input)
    };
    let roles = [PollRoleGeometry::for_factory(&make).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, make).unwrap();
    let port = bank.ingress().unwrap();
    let observer = bank.retirement_observer();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    root.business.release();
    let mut owner = Some(root.owner);
    let mut input = Some(new_input(true));
    let attempt = port.try_mount_in(&mut owner, &mut input, &mut Some(()));
    assert_eq!(attempt.result(), &Ok(()));
    drop(bank);
    assert_eq!(observer.is_reclaimed(), Ok(false));
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Active
    );
    attempt.finish().unwrap();
    drain(&mut driver);
    assert!(!called.load(Ordering::Acquire));
    assert_eq!(observer.is_reclaimed(), Ok(true));
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Orphaned
    );
    control
        .adopt(work)
        .unwrap()
        .complete_after_terminal_cancel_settled();
    drain(&mut driver);
}

#[tokio::test]
async fn stopping_is_idempotent_and_drains_only_previously_accepted_inputs() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let port = bank.ingress().unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    root.business.release();
    let mut owner = Some(root.owner);
    let mut input = Some(new_input(true));
    port.try_mount_in(&mut owner, &mut input, &mut Some(()))
        .finish()
        .unwrap();
    bank.stop_ingress();
    let revision = control.progress_revision();
    bank.stop_ingress();
    assert_eq!(control.progress_revision(), revision);
    let PollCompletion::Ready(output) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("accepted input must drain after stop");
    };
    assert_eq!(*output.value(), 7);
    drop(output);
    drain(&mut driver);
    assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    drop(bank);
}

#[tokio::test]
async fn exporting_a_weak_ingress_permanently_refuses_same_authority_reinstallation() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let port = bank.ingress().unwrap();
    let observer = bank.retirement_observer();
    drop(bank);
    drain(&mut driver);
    assert_eq!(
        observer.is_reclaimed(),
        Ok(true),
        "this observer covers value backing, not exported weak headers"
    );
    assert!(matches!(
        RolePollBank::install(&mut control, 0, run),
        Err(WorkError::Conflict)
    ));
    drop(port);
    assert!(matches!(
        RolePollBank::install(&mut control, 0, run),
        Err(WorkError::Conflict)
    ));
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[0].polls[0].registration_generation,
        1
    );
}

#[tokio::test]
async fn stopped_bank_observes_its_sources_without_waiting_for_delivered_footer() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::install(&mut control, 0, run).unwrap();
    let retired = bank.retirement_observer();
    let ingress = bank.ingress().unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    root.business.release();
    let pending = new_input(false);
    let ready = Arc::clone(&pending.ready);
    let saved_waker = Arc::clone(&pending.wake);
    let mut owner = Some(root.owner);
    let mut input = Some(pending);
    ingress
        .try_mount_in(&mut owner, &mut input, &mut Some(()))
        .finish()
        .unwrap();

    assert_eq!(
        bank.has_owned_sources_after_stop(),
        Err(WorkError::NotReady)
    );
    bank.stop_ingress();
    assert_eq!(
        bank.has_owned_sources_after_stop(),
        Ok(true),
        "queued original input"
    );
    assert!(
        bank.progress_turn(budget())
            .map(|turn| turn.into_parts().1)
            .is_none()
    );
    assert_eq!(
        bank.has_owned_sources_after_stop(),
        Ok(true),
        "original pending frame"
    );
    ready.store(true, Ordering::Release);
    saved_waker.lock().unwrap().take().unwrap().wake();
    let PollCompletion::Ready(output) = bank
        .progress_turn(budget())
        .map(|turn| turn.into_parts().1)
        .unwrap()
    else {
        panic!("original frame must finish");
    };
    assert_eq!(*output.value(), 7);
    assert_eq!(
        bank.has_owned_sources_after_stop(),
        Ok(false),
        "external output owns its original footer, not a bank-owned source"
    );
    drop(bank);
    drain(&mut driver);
    assert_eq!(
        retired.is_reclaimed(),
        Ok(false),
        "output is still held by receiver"
    );
    drop(output);
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
}

mod context;
