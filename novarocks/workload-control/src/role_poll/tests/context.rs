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

//! Context custody in the same original input, frame position and completion.

use super::*;
use std::sync::atomic::AtomicUsize;

struct SourceCell {
    packet: Mutex<Option<PollCompletion<u8>>>,
    drops: Arc<AtomicUsize>,
}
impl SourceCell {
    fn new(drops: &Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            packet: Mutex::new(None),
            drops: Arc::clone(drops),
        })
    }
}
impl Drop for SourceCell {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn context_survives_unpolled_root_loss_until_original_sender_exit() {
    let roles =
        [PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut control, 0, run)
        .unwrap();
    let port = bank.ingress().unwrap();
    let retired = bank.retirement_observer();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    root.business.release();
    let drops = Arc::new(AtomicUsize::new(0));
    let original = SourceCell::new(&drops);
    let weak = Arc::downgrade(&original);
    let mut owner = Some(root.owner);
    let mut input = Some(new_input(true));
    let mut context = Some(original);
    let sender = port.try_mount_in(&mut owner, &mut input, &mut context);
    assert_eq!(sender.result(), &Ok(()));
    assert!(owner.is_none() && input.is_none() && context.is_none());
    drop(bank);
    assert!(weak.upgrade().is_some());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(retired.is_reclaimed(), Ok(false));
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Active,
    );
    sender.finish().unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
    // Source destruction is not actor join or successful Work completion.
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&work].owner,
        crate::OwnerState::Orphaned,
    );
    control
        .adopt(work)
        .unwrap()
        .complete_after_terminal_cancel_settled();
    drain(&mut driver);
}

struct EarlyInput {
    source: Weak<SourceCell>,
    identity: Arc<()>,
}
struct OriginalRolePanic {
    source: Weak<SourceCell>,
    identity: Arc<()>,
    phase: &'static str,
}
fn constructor_panic(owner: WorkOwner, input: EarlyInput) -> std::future::Ready<u8> {
    assert!(input.source.upgrade().is_some());
    owner.complete();
    std::panic::panic_any(OriginalRolePanic {
        source: input.source,
        identity: input.identity,
        phase: "factory",
    });
}

#[tokio::test]
async fn constructor_panic_preserves_same_context_before_any_source_observer() {
    let roles = [
        PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&constructor_panic)
            .unwrap(),
    ];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank = RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(
        &mut control,
        0,
        constructor_panic,
    )
    .unwrap();
    let port = bank.ingress().unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let retired = root.owner.retirement_observer().unwrap();
    root.business.release();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = SourceCell::new(&drops);
    let weak = Arc::downgrade(&source);
    let identity = Arc::new(());
    let mut owner = Some(root.owner);
    let mut input = Some(EarlyInput {
        source: Weak::clone(&weak),
        identity: Arc::clone(&identity),
    });
    let mut context = Some(source);
    port.try_mount_in(&mut owner, &mut input, &mut context)
        .finish()
        .unwrap();
    assert!(context.is_none());
    let (source, packet) = bank.progress_turn(budget()).unwrap().into_parts();
    assert!(Weak::ptr_eq(&Arc::downgrade(&source), &weak));
    let PollCompletion::Panicked(failed) = &packet else {
        panic!("original factory panic must remain a whole packet");
    };
    let original = failed
        .value()
        .primary()
        .downcast_ref::<OriginalRolePanic>()
        .unwrap();
    assert_eq!(original.phase, "factory");
    assert!(Arc::ptr_eq(&original.identity, &identity));
    assert!(Weak::ptr_eq(&original.source, &weak));
    assert!(failed.value().cleanup().is_none());
    assert!(failed.value().output().is_none());
    assert_eq!(retired.is_reclaimed(), Ok(false));
    // Strip C first; the exact original packet has only a Weak back-reference.
    *source.packet.lock().unwrap() = Some(packet);
    drop(source);
    assert!(weak.upgrade().is_none(), "whole packet must not retain C");
    assert_eq!(drops.load(Ordering::Acquire), 1);
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
}

struct FailingInput {
    source: Weak<SourceCell>,
    primary: Arc<()>,
    cleanup: Arc<()>,
    panic_poll: bool,
}
struct FailingFrame {
    owner: Option<WorkOwner>,
    input: FailingInput,
}
fn failing_frame(owner: WorkOwner, input: FailingInput) -> FailingFrame {
    FailingFrame {
        owner: Some(owner),
        input,
    }
}
impl Future for FailingFrame {
    type Output = u8;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u8> {
        assert!(self.input.source.upgrade().is_some());
        if self.input.panic_poll {
            std::panic::panic_any(OriginalRolePanic {
                source: Weak::clone(&self.input.source),
                identity: Arc::clone(&self.input.primary),
                phase: "poll",
            });
        }
        self.owner.take().unwrap().complete();
        Poll::Ready(23)
    }
}
impl Drop for FailingFrame {
    fn drop(&mut self) {
        assert!(self.input.source.upgrade().is_some());
        if let Some(owner) = self.owner.take() {
            owner.complete();
        }
        std::panic::panic_any(OriginalRolePanic {
            source: Weak::clone(&self.input.source),
            identity: Arc::clone(&self.input.cleanup),
            phase: "cleanup",
        });
    }
}

#[tokio::test]
async fn poll_and_cleanup_panics_preserve_context_primary_cleanup_and_ready_output() {
    for panic_poll in [true, false] {
        let roles = [
            PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&failing_frame)
                .unwrap(),
        ];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(
            &mut control,
            0,
            failing_frame,
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = new_root(&control);
        root.business.release();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = SourceCell::new(&drops);
        let weak = Arc::downgrade(&source);
        let primary = Arc::new(());
        let cleanup = Arc::new(());
        bank.mount_owned_with_context(
            root.owner,
            FailingInput {
                source: Weak::clone(&weak),
                primary: Arc::clone(&primary),
                cleanup: Arc::clone(&cleanup),
                panic_poll,
            },
            source,
        )
        .unwrap_or_else(|_| panic!("same paid input and context must fit"));
        let (source, packet) = bank.progress_turn(budget()).unwrap().into_parts();
        assert!(Weak::ptr_eq(&Arc::downgrade(&source), &weak));
        let PollCompletion::Panicked(failed) = &packet else {
            panic!("original cleanup panic must remain a whole packet");
        };
        let first = failed
            .value()
            .primary()
            .downcast_ref::<OriginalRolePanic>()
            .unwrap();
        if panic_poll {
            assert_eq!(first.phase, "poll");
            assert!(Arc::ptr_eq(&first.identity, &primary));
            let second = failed
                .value()
                .cleanup()
                .unwrap()
                .downcast_ref::<OriginalRolePanic>()
                .unwrap();
            assert_eq!(second.phase, "cleanup");
            assert!(Arc::ptr_eq(&second.identity, &cleanup));
            assert!(failed.value().output().is_none());
        } else {
            assert_eq!(first.phase, "cleanup");
            assert!(Arc::ptr_eq(&first.identity, &cleanup));
            assert!(failed.value().cleanup().is_none());
            assert_eq!(failed.value().output(), Some(&23));
        }
        *source.packet.lock().unwrap() = Some(packet);
        drop(source);
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::Acquire), 1);
        driver.recheck_retired_backing();
        drain(&mut driver);
        assert!(control.inner.state.lock().unwrap().nodes.is_empty());
    }
}

struct TailContext {
    inner: Weak<Inner>,
    slot: crate::control_storage::SlotId,
    exited: Arc<AtomicBool>,
}
impl Drop for TailContext {
    fn drop(&mut self) {
        let inner = self.inner.upgrade().unwrap();
        let mut state = inner
            .state
            .try_lock()
            .expect("context Drop is outside State");
        let backing = &mut state.cancellations.slots[self.slot.0].polls[0];
        assert_eq!(backing.phase, PollPhase::Retired);
        assert!(
            !backing.try_reap(),
            "the same outer Wake must cover the actual context destructor tail",
        );
        self.exited.store(true, Ordering::Release);
    }
}

#[tokio::test]
async fn dropping_outer_completion_keeps_original_anchor_through_context_destructor() {
    let roles = [PollRoleGeometry::for_factory_with_context::<_, _, _, TailContext>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank =
        RolePollBank::<_, _, _, TailContext>::install_with_context(&mut control, 0, run).unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let retired = root.owner.retirement_observer().unwrap();
    let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
    root.business.release();
    let exited = Arc::new(AtomicBool::new(false));
    bank.mount_owned_with_context(
        root.owner,
        new_input(true),
        TailContext {
            inner: Arc::downgrade(&control.inner),
            slot,
            exited: Arc::clone(&exited),
        },
    )
    .unwrap_or_else(|_| panic!("context mount must fit"));
    let outer = bank.progress_turn(budget()).unwrap();
    assert!(!exited.load(Ordering::Acquire));
    drop(outer);
    assert!(exited.load(Ordering::Acquire));
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
}

#[tokio::test]
async fn stable_context_receiver_keeps_original_footer_until_actual_packet_exit() {
    let roles =
        [PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank =
        RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut control, 0, run)
            .unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    let slot = control.inner.state.lock().unwrap().nodes[&work].cancellation;
    let retired = root.owner.retirement_observer().unwrap();
    root.business.release();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = SourceCell::new(&drops);
    bank.mount_owned_with_context(root.owner, new_input(true), source)
        .unwrap_or_else(|_| panic!("context mount must fit"));
    let (source, packet) = bank.progress_turn(budget()).unwrap().into_parts();
    *source.packet.lock().unwrap() = Some(packet);
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(false));
    assert_eq!(
        control.inner.state.lock().unwrap().cancellations.slots[slot.0].phase,
        SlotPhase::Node(work),
    );
    assert!(control.inner.state.lock().unwrap().nodes[&work].completed);
    drop(source);
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert_eq!(retired.is_reclaimed(), Ok(true));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    let next = new_root(&control);
    assert_eq!(
        control.inner.state.lock().unwrap().nodes[&next.owner.scope().id()].cancellation,
        slot,
    );
    next.business.release();
    next.owner.complete();
    drain(&mut driver);
}

#[tokio::test]
async fn refused_context_mount_preserves_each_original_source() {
    let roles =
        [PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let bank = RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut control, 0, run)
        .unwrap();
    let port = bank.ingress().unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    let work = root.owner.scope().id();
    root.business.release();
    let original_input = new_input(true);
    let original_ready = Arc::clone(&original_input.ready);
    let drops = Arc::new(AtomicUsize::new(0));
    let source = SourceCell::new(&drops);
    let weak = Arc::downgrade(&source);
    let mut owner = Some(root.owner);
    let mut input = Some(original_input);
    let mut context = None;
    assert_eq!(
        port.try_mount_in(&mut owner, &mut input, &mut context)
            .finish(),
        Err(WorkError::Released),
    );
    assert_eq!(owner.as_ref().unwrap().scope().id(), work);
    assert!(Arc::ptr_eq(&input.as_ref().unwrap().ready, &original_ready));
    context = Some(source);
    let original = input.take();
    assert_eq!(
        port.try_mount_in(&mut owner, &mut input, &mut context)
            .finish(),
        Err(WorkError::Released),
    );
    assert_eq!(owner.as_ref().unwrap().scope().id(), work);
    assert!(Weak::ptr_eq(
        &Arc::downgrade(context.as_ref().unwrap()),
        &weak
    ));
    input = original;
    bank.stop_ingress();
    assert_eq!(
        port.try_mount_in(&mut owner, &mut input, &mut context)
            .finish(),
        Err(WorkError::Closed),
    );
    assert_eq!(owner.as_ref().unwrap().scope().id(), work);
    assert!(Arc::ptr_eq(&input.as_ref().unwrap().ready, &original_ready));
    assert!(Weak::ptr_eq(
        &Arc::downgrade(context.as_ref().unwrap()),
        &weak
    ));
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(bank.has_owned_sources_after_stop(), Ok(false));
    drop(input.take());
    drop(context.take());
    owner.take().unwrap().complete();
    drop(bank);
    drain(&mut driver);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn context_is_stable_before_a_successful_sender_notification_panics() {
    let roles =
        [PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<SourceCell>>(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    let mut bank =
        RolePollBank::<_, _, _, Arc<SourceCell>>::install_with_context(&mut control, 0, run)
            .unwrap();
    let port = bank.ingress().unwrap();
    control.mark_ready().unwrap();
    let root = new_root(&control);
    root.business.release();
    let drops = Arc::new(AtomicUsize::new(0));
    let source = SourceCell::new(&drops);
    let weak = Arc::downgrade(&source);
    let mut owner = Some(root.owner);
    let mut input = Some(new_input(true));
    let mut context = Some(source);
    let waker = Waker::from(Arc::new(PanicOnce(AtomicBool::new(true))));
    assert!(
        bank.poll_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let sender = port.try_mount_in(&mut owner, &mut input, &mut context);
    assert!(owner.is_none() && input.is_none() && context.is_none());
    let result = catch_unwind(AssertUnwindSafe(|| sender.finish()));
    assert!(result.is_err());
    assert!(weak.upgrade().is_some());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let (source, packet) = bank.progress_turn(budget()).unwrap().into_parts();
    assert!(Weak::ptr_eq(&Arc::downgrade(&source), &weak));
    let PollCompletion::Ready(output) = &packet else {
        panic!("original input must still run after notification panic");
    };
    assert_eq!(*output.value(), 7);
    *source.packet.lock().unwrap() = Some(packet);
    drop(source);
    driver.recheck_retired_backing();
    drain(&mut driver);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn context_install_requires_the_same_actual_context_geometry() {
    let roles = [PollRoleGeometry::for_factory(&run).unwrap()];
    let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
    assert!(matches!(
        RolePollBank::<_, _, _, [u8; 128]>::install_with_context(&mut control, 0, run),
        Err(WorkError::Conflict),
    ));
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
                slot.polls[0].registration == RegistrationPhase::Absent
                    && slot.polls[0].phase == PollPhase::Free
            })
    );
    // A failed context installation did not replace the original unit profile.
    let bank = RolePollBank::install(&mut control, 0, run).unwrap();
    drop(bank);
    control.close_admission();
    drain(&mut driver);
}

mod retention;
