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

//! Fixed ready hints over the original role anchors, not another inventory.

use super::PollWake;
use crate::WorkError;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

struct Ring {
    entries: Vec<Option<Arc<PollWake>>>,
    head: usize,
    len: usize,
}

pub(super) struct ReadyQueue {
    ring: Mutex<Ring>,
    changed: Arc<Notify>,
}

impl ReadyQueue {
    #[cfg(test)]
    pub(super) fn lock_is_available(&self) -> bool {
        self.ring.try_lock().is_ok()
    }

    pub(super) fn new(count: usize, changed: Arc<Notify>) -> Result<Arc<Self>, WorkError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| WorkError::Capacity("fixed role ready ring"))?;
        entries.resize_with(count, || None);
        let queue = Arc::new(Self {
            ring: Mutex::new(Ring {
                entries,
                head: 0,
                len: 0,
            }),
            changed,
        });
        drop(queue.ring.lock().unwrap());
        Ok(queue)
    }

    // CAS reserves this original anchor's sole pending hint. The publisher
    // retains the real Wake until insertion and the later lock-free notify.
    // A queued/publishing Wake cannot pass the original Arc::get_mut reaper.
    pub(super) fn publish(&self, wake: &Arc<PollWake>) -> bool {
        if wake.queued.swap(true, Ordering::AcqRel) {
            return false;
        }
        let mut ring = self.ring.lock().unwrap();
        assert!(
            ring.len < ring.entries.len(),
            "one ready hint per original anchor"
        );
        let index =
            ring.head.checked_add(ring.len).expect("bounded ring index") % ring.entries.len();
        assert!(ring.entries[index].is_none());
        ring.entries[index] = Some(Arc::clone(wake));
        ring.len += 1;
        true
    }

    pub(super) fn pop(&self) -> Option<Arc<PollWake>> {
        let wake = {
            let mut ring = self.ring.lock().unwrap();
            if ring.len == 0 {
                return None;
            }
            let index = ring.head;
            let wake = ring.entries[index]
                .take()
                .expect("occupied ready ring head");
            ring.head = if index + 1 == ring.entries.len() {
                0
            } else {
                index + 1
            };
            ring.len -= 1;
            wake
        };
        // A wake before this store merges into the imminent poll. A wake
        // during/after poll must enqueue again, including a self-wake Pending.
        // Acquire the last merged producer too: it may have published after
        // the ring lock was released without taking that lock itself.
        let _ = wake.queued.swap(false, Ordering::AcqRel);
        Some(wake)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.ring.lock().unwrap().len == 0
    }

    pub(super) fn notify(&self) {
        self.changed.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::super::{PollPhase, PollRoleGeometry, RolePollBank};
    use super::*;
    use crate::scope::fixed_control_storage_tests::prototype_profile_with_roles;
    use crate::{
        WorkClass, WorkOwner, WorkRequest,
        joint_admission::{JointProgress, JointProgressDriver},
    };
    use std::{
        num::NonZeroUsize,
        sync::mpsc,
        task::{Poll, Waker},
        time::{Duration, Instant},
    };

    fn drain(driver: &mut JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original driver must reach finite idle");
    }

    async fn completed_owner_parked_role(owner: WorkOwner, saved: Arc<Mutex<Option<Waker>>>) {
        owner.complete();
        std::future::poll_fn(|cx| {
            *saved.lock().unwrap() = Some(cx.waker().clone());
            Poll::<()>::Pending
        })
        .await;
    }

    #[tokio::test]
    async fn paused_real_publisher_prevents_reset_through_bank_exit() {
        let roles = [PollRoleGeometry::for_factory(&completed_owner_parked_role).unwrap()];
        let (mut control, _, mut driver) = prototype_profile_with_roles(2, 1, 1, 1, &roles);
        let mut bank = RolePollBank::install(&mut control, 0, completed_owner_parked_role).unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let observer = root.owner.retirement_observer().unwrap();
        let slot = control.inner.state.lock().unwrap().nodes[&root.owner.scope().id()].cancellation;
        let saved = Arc::new(Mutex::new(None));
        root.business.release();
        bank.mount_owned(root.owner, Arc::clone(&saved))
            .unwrap_or_else(|_| panic!("original mount"));
        assert!(bank.progress_turn(NonZeroUsize::new(1).unwrap()).is_none());
        let publisher = saved.lock().unwrap().take().unwrap();
        let probe = Arc::clone(
            &control.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].wake,
        );
        let queue = Arc::clone(&bank.ready);
        let original = &control;
        let (mut driver, observer) = std::thread::scope(|threads| {
            // Force the real wake to pause after its queued CAS, before push.
            // This thread holds only the queue lock, never the original State.
            let ring = queue.ring.lock().unwrap();
            let publishing = threads.spawn(move || publisher.wake());
            let deadline = Instant::now() + Duration::from_secs(10);
            while !probe.queued.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "real publisher must reach its CAS"
                );
                std::thread::yield_now();
            }
            assert_eq!(ring.len, 0);
            // Remove the test's extra Wake alias before observing uniqueness.
            drop(probe);
            let (observed_tx, observed_rx) = mpsc::sync_channel(1);
            let exiting = threads.spawn(move || {
                // The real frame has completed its owner and has no waking
                // destructor. Footer/State exit therefore need not take ring.
                drop(bank);
                driver.recheck_retired_backing();
                drain(&mut driver);
                let phase =
                    original.inner.state.lock().unwrap().cancellations.slots[slot.0].polls[0].phase;
                observed_tx.send((observer.is_reclaimed(), phase)).unwrap();
                (driver, observer)
            });
            let (reclaimed, phase) = observed_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_eq!(reclaimed, Ok(false));
            assert_eq!(phase, PollPhase::Retired);
            // No test-held queue alias survives to explain final reclamation.
            drop(ring);
            drop(queue);
            publishing.join().unwrap();
            exiting.join().unwrap()
        });
        driver.recheck_retired_backing();
        drain(&mut driver);
        assert_eq!(observer.is_reclaimed(), Ok(true));
        assert!(control.inner.state.lock().unwrap().is_drained());
    }
}
