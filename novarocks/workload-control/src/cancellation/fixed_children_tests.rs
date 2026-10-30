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

//! Fixed-index regressions exercise the original Cancellation and Arc lifetime.

use super::*;
use std::sync::Barrier;

fn cells(count: usize, limit: usize) -> Vec<Arc<Cancellation>> {
    let shared = new_shared_child_index();
    (0..count)
        .map(|slot| Cancellation::preallocated_fixed(Arc::clone(&shared), slot, limit))
        .collect()
}

#[test]
fn parents_share_one_index_and_detach_only_the_exact_original_edge() {
    let mut cells = cells(7, 3);
    let right = Arc::clone(&cells[1]);
    let left = Arc::clone(&cells[0]);
    Cancellation::reuse_child(&mut cells[2], &left, None, WorkId(10)).unwrap();
    Cancellation::reuse_child(&mut cells[3], &left, None, WorkId(u64::MAX)).unwrap();
    Cancellation::reuse_child(&mut cells[4], &right, None, WorkId(20)).unwrap();
    Cancellation::reuse_child(&mut cells[5], &right, None, WorkId(30)).unwrap();
    let grandparent = Arc::clone(&cells[4]);
    Cancellation::reuse_child(&mut cells[6], &grandparent, None, WorkId(40)).unwrap();
    let (_, shared, edges) = left.fixed_child_facts_for_test();
    assert_eq!(edges, 5);
    for cell in &cells {
        assert_eq!(cell.fixed_child_facts_for_test(), (3, shared, edges));
    }
    assert!(Arc::ptr_eq(
        &left.next_child_after(None).unwrap(),
        &cells[2]
    ));
    assert!(Arc::ptr_eq(
        &left.next_child_after(Some(WorkId(10))).unwrap(),
        &cells[3]
    ));
    assert!(left.next_child_after(Some(WorkId(u64::MAX))).is_none());
    // A mismatching original pointer and a different sequence are both pure.
    {
        let mut state = left.state.lock().unwrap();
        state
            .children
            .detach_exact(WorkId(10), &Arc::downgrade(&cells[4]));
        state
            .children
            .detach_exact(WorkId(11), &Arc::downgrade(&cells[2]));
    }
    assert_eq!(left.linked_children_for_test(), 2);
    assert_eq!(left.fixed_child_facts_for_test().2, 5);
    cells[2].detach();
    cells[2].detach();
    assert_eq!(left.linked_children_for_test(), 1);
    assert_eq!(right.linked_children_for_test(), 2);
    assert_eq!(right.fixed_child_facts_for_test().2, 4);
    assert!(Arc::ptr_eq(
        &left.next_child_after(None).unwrap(),
        &cells[3]
    ));
    assert_eq!(
        left.request(CancellationReason::Requested),
        CancellationRequestOutcome::Requested
    );
    assert_eq!(cells[3].check_reason(), Some(CancellationReason::Requested));
    assert_eq!(
        cells[4].check_reason(),
        None,
        "another parent prefix stays independent"
    );
    assert_eq!(cells[6].check_reason(), None);
}

#[test]
fn detached_old_child_and_its_view_keep_the_original_parent_unreusable() {
    let mut cells = cells(2, 1);
    let parent = Arc::clone(&cells[0]);
    Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(1)).unwrap();
    drop(parent);
    let view = cells[1].view_for_test();
    cells[1].detach();
    assert_eq!(cells[0].fixed_child_facts_for_test().2, 0);
    assert!(
        Arc::get_mut(&mut cells[0]).is_none(),
        "detached child still owns its real parent path"
    );
    assert!(
        Arc::get_mut(&mut cells[1]).is_none(),
        "old view forbids child reset"
    );
    drop(view);
    let deferred = Arc::get_mut(&mut cells[1]).unwrap().reset_for_reuse();
    assert!(
        Arc::get_mut(&mut cells[0]).is_none(),
        "deferred actual parent exit is required"
    );
    drop(deferred);
    drop(Arc::get_mut(&mut cells[0]).unwrap().reset_for_reuse());
    let parent = Arc::clone(&cells[0]);
    Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(2)).unwrap();
    let stale = Arc::downgrade(&cells[1]);
    parent
        .state
        .lock()
        .unwrap()
        .children
        .detach_exact(WorkId(1), &stale);
    assert_eq!(parent.linked_children_for_test(), 1);
    assert!(Arc::ptr_eq(
        &parent.next_child_after(Some(WorkId(1))).unwrap(),
        &cells[1]
    ));
}

#[test]
fn registration_racing_cancellation_keeps_inheritance_and_exact_detach() {
    for _ in 0..64 {
        let mut cells = cells(2, 1);
        let parent = Arc::clone(&cells[0]);
        let barrier = Barrier::new(2);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                barrier.wait();
                assert_eq!(
                    parent.request(CancellationReason::ServerShutdown),
                    CancellationRequestOutcome::Requested
                );
            });
            barrier.wait();
            Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(1)).unwrap();
        });
        assert_eq!(
            cells[1].check_reason(),
            Some(CancellationReason::ServerShutdown)
        );
        assert_eq!(
            cells[1].state.lock().unwrap().reason,
            Some(CancellationReason::ServerShutdown)
        );
        cells[1].detach();
        assert_eq!(parent.fixed_child_facts_for_test().2, 0);
        assert!(Arc::get_mut(&mut cells[1]).is_some());
    }
}

#[test]
fn foreign_inventory_and_duplicate_sequence_refuse_before_child_mutation() {
    let mut cells = cells(3, 2);
    let parent = Arc::clone(&cells[0]);
    let mut foreign = Cancellation::preallocated_fixed(new_shared_child_index(), 1, 1);
    assert_eq!(
        Cancellation::reuse_child(&mut foreign, &parent, None, WorkId(1)),
        Err(crate::WorkError::Conflict)
    );
    assert!(foreign.parent.is_none());
    assert_eq!(foreign.check_reason(), None);
    Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(1)).unwrap();
    assert_eq!(
        Cancellation::reuse_child(&mut cells[2], &parent, None, WorkId(1)),
        Err(crate::WorkError::Conflict)
    );
    assert!(cells[2].parent.is_none());
    assert_eq!(parent.fixed_child_facts_for_test().2, 1);
    assert_eq!(cells[2].fixed_child_facts_for_test().2, 1);
}

#[test]
fn zero_fanout_refuses_without_allocating_or_changing_the_child() {
    let mut cells = cells(2, 0);
    let parent = Arc::clone(&cells[0]);
    assert_eq!(
        Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(1)),
        Err(crate::WorkError::Capacity("prepaid parent child backing"))
    );
    assert!(cells[1].parent.is_none());
    assert_eq!(parent.fixed_child_facts_for_test().2, 0);
    assert!(parent.next_child_after(None).is_none());
}

#[test]
fn paused_dfs_keeps_its_detached_anchor_and_visits_old_and_late_siblings() {
    use std::{
        future::Future,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        task::{Context, Wake, Waker},
        time::Duration,
    };
    struct Pause {
        armed: AtomicBool,
        entered: mpsc::SyncSender<()>,
        resume: Mutex<mpsc::Receiver<()>>,
    }
    impl Wake for Pause {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            if self.armed.swap(false, Ordering::AcqRel) {
                self.entered.send(()).unwrap();
                self.resume
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
            }
        }
    }
    let mut cells = cells(4, 3);
    let parent = Arc::clone(&cells[0]);
    Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(10)).unwrap();
    Cancellation::reuse_child(&mut cells[2], &parent, None, WorkId(20)).unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let waker = Waker::from(Arc::new(Pause {
        armed: AtomicBool::new(true),
        entered: entered_tx,
        resume: Mutex::new(resume_rx),
    }));
    let view = cells[1].view_for_test();
    let mut observer = Box::pin(async move { view.cancelled().await });
    assert!(
        observer
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    std::thread::scope(|threads| {
        let request = threads.spawn(|| parent.request(CancellationReason::ServerShutdown));
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // The real first child is the DFS cursor. Its row can disappear, but
        // its Arc and strong ParentLink cannot be reset into another Work.
        drop(observer);
        cells[1].detach();
        assert!(Arc::get_mut(&mut cells[1]).is_none());
        assert_eq!(cells[2].state.lock().unwrap().reason, None);
        // This registration occurs strictly after the parent's decision and
        // scan. It inherits that decision even before DFS reaches its key.
        Cancellation::reuse_child(&mut cells[3], &parent, None, WorkId(30)).unwrap();
        assert_eq!(
            cells[3].state.lock().unwrap().reason,
            Some(CancellationReason::ServerShutdown)
        );
        resume_tx.send(()).unwrap();
        assert_eq!(
            request.join().unwrap(),
            CancellationRequestOutcome::Requested
        );
    });
    assert_eq!(
        cells[2].state.lock().unwrap().reason,
        Some(CancellationReason::ServerShutdown)
    );
    assert_eq!(parent.fixed_child_facts_for_test().2, 2);
    drop(Arc::get_mut(&mut cells[1]).unwrap().reset_for_reuse());
    Cancellation::reuse_child(&mut cells[1], &parent, None, WorkId(40)).unwrap();
    assert_eq!(parent.fixed_child_facts_for_test().2, 3);
    assert!(Arc::ptr_eq(
        &parent.next_child_after(Some(WorkId(30))).unwrap(),
        &cells[1]
    ));
}
