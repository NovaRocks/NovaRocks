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

mod children;

#[cfg(test)]
mod fixed_children_tests;

use crate::WorkId;
use children::{Children, SharedChildIndex};
pub(crate) use children::{new_shared_child_index, shared_child_index_backing_bytes};
use std::sync::{Arc, Mutex, Weak};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancellationReason {
    Requested,
    ExplicitKill { requester_session: u64 },
    ExplicitKillConnection { requester_session: u64 },
    ClientCancelRequested,
    ClientDisconnected,
    DeadlineExceeded,
    FrontendDrainDeadlineExceeded,
    ServerShutdown,
    OwnerDropped,
}

#[derive(Default)]
struct State {
    reason: Option<CancellationReason>,
    success_sealed: bool,
    children: Children,
}

struct ChildLink {
    sequence: WorkId,
    child: Weak<Cancellation>,
}

struct ParentLink {
    parent: Arc<Cancellation>,
    sequence: WorkId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancellationRequestOutcome {
    Requested,
    AlreadyRequested(CancellationReason),
    SuccessSealed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CancellationSuccessSealOutcome {
    Sealed,
    AlreadySealed,
    Cancelled(CancellationReason),
}

pub(crate) struct Cancellation {
    state: Mutex<State>,
    changed: Notify,
    deadline: Option<Instant>,
    parent: Option<ParentLink>,
    #[cfg(test)]
    fixture_next_sequence: std::sync::atomic::AtomicU64,
}

impl Cancellation {
    pub(crate) fn preallocated_fixed(
        shared: SharedChildIndex,
        parent_slot: usize,
        limit: usize,
    ) -> Arc<Self> {
        let mut cell = Self::root(None);
        Arc::get_mut(&mut cell)
            .unwrap()
            .state
            .get_mut()
            .unwrap()
            .children = Children::fixed(shared, parent_slot, limit);
        // Initialize the supported platform's lazy mutexes before publication.
        let _ = cell.check_reason();
        {
            let mut notified = std::pin::pin!(cell.changed.notified());
            notified.as_mut().enable();
        }
        cell
    }

    pub(crate) fn reserve_fixed_children(&self, count: usize) -> Result<(), crate::WorkError> {
        self.state.lock().unwrap().children.reserve_fixed(count)
    }

    pub(crate) fn release_fixed_children(&self, count: usize) {
        self.state.lock().unwrap().children.release_fixed(count);
    }

    pub(crate) fn reuse_root(&mut self, deadline: Option<Instant>) {
        assert!(self.parent.is_none(), "Free cell has no previous parent");
        let state = self.state.get_mut().unwrap();
        assert!(
            state.reason.is_none() && !state.success_sealed,
            "Free cell was exclusively reset"
        );
        state.children.assert_empty_for_reuse();
        self.deadline = deadline;
    }

    pub(crate) fn reuse_child(
        cell: &mut Arc<Self>,
        parent: &Arc<Self>,
        deadline: Option<Instant>,
        sequence: WorkId,
    ) -> Result<(), crate::WorkError> {
        let inherited = parent.check_reason();
        let deadline = match (parent.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (parent, child) => parent.or(child),
        };
        let mut state = parent.state.lock().unwrap();
        let Children::Fixed(children) = &mut state.children else {
            return Err(crate::WorkError::Conflict);
        };
        let child = Arc::get_mut(cell).ok_or(crate::WorkError::Conflict)?;
        children.preflight(&child.state.get_mut().unwrap().children, sequence)?;
        child.reuse_root(deadline);
        child.parent = Some(ParentLink {
            parent: Arc::clone(parent),
            sequence,
        });
        child.state.get_mut().unwrap().reason = state.reason.clone().or(inherited);
        let Children::Fixed(children) = &mut state.children else {
            unreachable!()
        };
        children.insert_preflighted(sequence, Arc::downgrade(cell));
        Ok(())
    }

    pub(crate) fn reset_for_reuse(&mut self) -> CancellationReset {
        let state = self.state.get_mut().unwrap();
        state.reason = None;
        state.success_sealed = false;
        state.children.reset(); // Retain the same original shared index and logical limit.
        self.deadline = None;
        // Only notify_waiters is used here. With no old Notified borrower,
        // the existing Notify can be retained together with its paid mutex.
        CancellationReset {
            _parent: self.parent.take().map(|link| link.parent),
        }
    }

    pub(crate) fn root(deadline: Option<Instant>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            deadline,
            parent: None,
            #[cfg(test)]
            fixture_next_sequence: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Production sequences come from the original State's checked WorkId.
    pub(crate) fn child_with_sequence(
        self: &Arc<Self>,
        deadline: Option<Instant>,
        sequence: WorkId,
    ) -> Arc<Self> {
        assert!(
            matches!(self.state.lock().unwrap().children, Children::Dynamic(_)),
            "fixed children require the original prepaid child slot"
        );
        let inherited = self.check_reason();
        let deadline = match (self.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (parent, child) => parent.or(child),
        };
        let child = Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            deadline,
            parent: Some(ParentLink {
                parent: Arc::clone(self),
                sequence,
            }),
            #[cfg(test)]
            fixture_next_sequence: std::sync::atomic::AtomicU64::new(1),
        });
        // Registration and inheritance share the parent lock. Production holds
        // State across checked ID creation and registration. Sorted insertion
        // also supports fixtures whose concurrent registrations arrive out of order.
        let mut state = self.state.lock().unwrap();
        let Children::Dynamic(children) = &mut state.children else {
            unreachable!()
        };
        children.retain(|link| link.child.strong_count() != 0);
        let index = children.partition_point(|link| link.sequence < sequence);
        assert!(
            children
                .get(index)
                .is_none_or(|link| link.sequence != sequence),
            "Cancellation child sequences must be unique"
        );
        children.insert(
            index,
            ChildLink {
                sequence,
                child: Arc::downgrade(&child),
            },
        );
        // The unpublished child has no subscribers or descendants. Wake only
        // after all cancellation and State locks have been released.
        child.state.lock().unwrap().reason = state.reason.clone().or(inherited);
        child
    }

    #[cfg(test)]
    fn child(self: &Arc<Self>, deadline: Option<Instant>) -> Arc<Self> {
        use std::sync::atomic::Ordering;
        let sequence = self
            .fixture_next_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("Cancellation fixture child sequence exhausted");
        self.child_with_sequence(deadline, WorkId(sequence))
    }

    #[cfg(test)]
    pub(crate) fn linked_children_for_test(&self) -> usize {
        self.state.lock().unwrap().children.len()
    }

    #[cfg(test)]
    pub(crate) fn fixed_child_facts_for_test(&self) -> (usize, *const (), usize) {
        let (limit, shared, edges) = self.state.lock().unwrap().children.fixed_facts();
        (limit, shared.cast(), edges)
    }

    /// Commit one local first-wins decision without walking or waking.
    fn record_request(
        &self,
        inherited: CancellationReason,
    ) -> (CancellationRequestOutcome, Option<CancellationReason>) {
        let mut state = self.state.lock().unwrap();
        if state.success_sealed {
            return (CancellationRequestOutcome::SuccessSealed, None);
        }
        let outcome = match state.reason.as_ref() {
            Some(existing) => CancellationRequestOutcome::AlreadyRequested(existing.clone()),
            None => {
                state.reason = Some(inherited);
                CancellationRequestOutcome::Requested
            }
        };
        (outcome, state.reason.clone())
    }

    /// The process batch owner records an original root decision under State.
    /// This does not walk descendants or invoke a callback; propagation follows
    /// through the original Node notification custody after releasing State.
    pub(crate) fn record_root_request(
        &self,
        reason: CancellationReason,
    ) -> (CancellationRequestOutcome, Option<CancellationReason>) {
        assert!(
            self.parent.is_none(),
            "batch cancellation requires an original root"
        );
        self.record_request(reason)
    }

    /// Sequence cursors survive concurrent detach/retain; no Vec index escapes.
    fn next_child_after(&self, after: Option<WorkId>) -> Option<Arc<Self>> {
        self.state.lock().unwrap().children.next_after(after)
    }

    pub(crate) fn request(&self, reason: CancellationReason) -> CancellationRequestOutcome {
        let inherited = self.ancestor_reason().unwrap_or(reason);
        let (outcome, effective) = self.record_request(inherited);
        let Some(effective) = effective else {
            return outcome;
        };
        // Idempotent notification also completes a decision already recorded
        // by the process batch owner. Repeated requests can help a first caller
        // paused before notification; cached/negative/sealed reads stay silent.
        self.changed.notify_waiters();
        // Each cursor retains its ancestor path through strong parent links.
        // Children remain weak; this does not retain any Work or completion right.
        // Repeated requests still help a first request paused inside a wake.
        let mut current = self.next_child_after(None).map(|child| (child, effective));
        while let Some((node, inherited)) = current.take() {
            let (decision, effective) = node.record_request(inherited);
            if decision == CancellationRequestOutcome::Requested {
                node.changed.notify_waiters();
            }
            if let Some(effective) = effective
                && let Some(child) = node.next_child_after(None)
            {
                current = Some((child, effective));
                continue;
            }
            // A sealed node prunes its entire subtree. Stop only when the
            // original root itself is exhausted, after its remaining siblings
            // have been considered; never continue outside the request root.
            let mut exhausted = node;
            loop {
                if std::ptr::eq(exhausted.as_ref(), self) {
                    return outcome;
                }
                let link = exhausted.parent.as_ref().expect("DFS child parent");
                let sequence = link.sequence;
                let parent = Arc::clone(&link.parent);
                let sibling = parent.next_child_after(Some(sequence));
                drop(exhausted);
                if let Some(sibling) = sibling {
                    // This parent was visited before descending. Its local
                    // first-wins reason is immutable; do not rescan ancestors.
                    let inherited = parent
                        .state
                        .lock()
                        .unwrap()
                        .reason
                        .clone()
                        .expect("DFS parent has a committed reason");
                    current = Some((sibling, inherited));
                    break;
                }
                exhausted = parent;
            }
        }
        // Return only this original node's first-wins result.
        outcome
    }

    pub(crate) fn seal_success(&self) -> CancellationSuccessSealOutcome {
        let inherited = self.ancestor_reason();
        let cancelled = {
            let mut state = self.state.lock().unwrap();
            if state.success_sealed {
                return CancellationSuccessSealOutcome::AlreadySealed;
            }
            if let Some(reason) = state.reason.clone() {
                return CancellationSuccessSealOutcome::Cancelled(reason);
            }
            let deadline_reason = self
                .deadline
                .filter(|deadline| Instant::now() >= *deadline)
                .map(|_| CancellationReason::DeadlineExceeded);
            if let Some(reason) = inherited.or(deadline_reason) {
                state.reason = Some(reason.clone());
                Some(reason)
            } else {
                state.success_sealed = true;
                None
            }
        };
        if let Some(reason) = cancelled {
            // Propagate the reason installed while winning the seal/request race.
            let _ = self.request(reason.clone());
            self.changed.notify_waiters();
            CancellationSuccessSealOutcome::Cancelled(reason)
        } else {
            CancellationSuccessSealOutcome::Sealed
        }
    }

    pub(crate) fn detach(self: &Arc<Self>) {
        if let Some(link) = &self.parent {
            let this = Arc::downgrade(self);
            link.parent
                .state
                .lock()
                .unwrap()
                .children
                .detach_exact(link.sequence, &this);
        }
    }

    fn reason(&self, reclaim: &Weak<crate::scope::Inner>) -> Option<CancellationReason> {
        {
            let state = self.state.lock().unwrap();
            if let Some(reason) = state.reason.clone() {
                return Some(reason);
            }
            if state.success_sealed {
                return None;
            }
        }
        let reason = self.check_reason()?;
        // The view already retains this cell's entire ancestor chain. Only
        // propagation below can temporarily retain other descendant cells.
        // Publish after those aliases exit, including unwind, and keep plain
        // negative/cached/sealed observations silent.
        let _reclaim = crate::scope::ReclaimWakeOnExit::observe(reclaim);
        let _ = self.request(reason);
        self.state.lock().unwrap().reason.clone()
    }

    /// Inspect under a different owner's lock without invoking wake callbacks.
    pub(crate) fn check_reason(&self) -> Option<CancellationReason> {
        let local = self.state.lock().unwrap().reason.clone();
        local.or_else(|| self.ancestor_reason()).or_else(|| {
            self.deadline
                .filter(|deadline| Instant::now() >= *deadline)
                .map(|_| CancellationReason::DeadlineExceeded)
        })
    }

    fn ancestor_reason(&self) -> Option<CancellationReason> {
        let mut ancestor = self.parent.as_ref().map(|link| Arc::clone(&link.parent));
        while let Some(node) = ancestor {
            if let Some(reason) = node.state.lock().unwrap().reason.clone() {
                return Some(reason);
            }
            ancestor = node.parent.as_ref().map(|link| Arc::clone(&link.parent));
        }
        None
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Production views always notify their original process authority on exit.
    pub(crate) fn view(self: &Arc<Self>, authority: &Arc<crate::scope::Inner>) -> CancellationView {
        CancellationView {
            inner: Some(Arc::clone(self)),
            reclaim: Arc::downgrade(authority),
        }
    }

    #[cfg(test)]
    fn view_for_test(self: &Arc<Self>) -> CancellationView {
        CancellationView {
            inner: Some(Arc::clone(self)),
            reclaim: Weak::new(),
        }
    }
}

/// One fixed deferred position returned from the locked reset. Its actual
/// values must be dropped outside State before the slot is published Free.
pub(crate) struct CancellationReset {
    _parent: Option<Arc<Cancellation>>,
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        let mut parent = self.parent.take().map(|link| link.parent);
        while let Some(ancestor) = parent {
            // Unlike try_unwrap(...).ok(), into_inner cannot race another final
            // clone into recursively destroying the remaining parent chain.
            let Some(mut value) = Arc::into_inner(ancestor) else {
                break;
            };
            parent = value.parent.take().map(|link| link.parent);
            // value has no parent now, so its own Drop is a constant-depth call.
        }
    }
}

/// Read-only cancellation capability. Cancelling never releases resources.
/// The view retains cancellation facts, never a Work node or its authority.
pub struct CancellationView {
    inner: Option<Arc<Cancellation>>,
    reclaim: Weak<crate::scope::Inner>,
}

impl Clone for CancellationView {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            reclaim: self.reclaim.clone(),
        }
    }
}

impl Drop for CancellationView {
    fn drop(&mut self) {
        // Publish only after this alias has left the actual cancellation cell.
        // An observer must never pin a logically collected Work node for wakeup.
        drop(self.inner.take());
        if let Some(inner) = self.reclaim.upgrade() {
            inner.notify_reclaim_progress();
        }
    }
}

impl CancellationView {
    fn cancellation(&self) -> &Cancellation {
        self.inner
            .as_deref()
            .expect("cancellation view retains its original capability")
    }

    /// Exact original cell identity only; this grants no cancellation or Work.
    pub fn is_for_scope(&self, scope: &crate::WorkScope) -> bool {
        if !std::sync::Weak::ptr_eq(&self.reclaim, &std::sync::Arc::downgrade(&scope.inner)) {
            return false;
        }
        let state = scope.inner.state.lock().unwrap();
        let Some(node) = state.nodes.get(&scope.id) else {
            return false;
        };
        self.inner.as_ref().is_some_and(|cell| {
            std::sync::Arc::ptr_eq(cell, state.cancellations.cell(node.cancellation))
        })
    }

    pub fn reason(&self) -> Option<CancellationReason> {
        self.cancellation().reason(&self.reclaim)
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.cancellation().deadline()
    }

    pub async fn cancelled(&self) -> CancellationReason {
        loop {
            let notified = self.cancellation().changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reason) = self.reason() {
                return reason;
            }
            let deadline = {
                let state = self.cancellation().state.lock().unwrap();
                // A sealed success cannot acquire a cancellation reason. Its
                // original deadline remains a fact, but must no longer wake
                // this observer repeatedly after that deadline has elapsed.
                if state.success_sealed {
                    None
                } else {
                    self.deadline()
                }
            };
            if let Some(deadline) = deadline {
                tokio::select! {
                    _ = notified => {},
                    _ = tokio::time::sleep_until(deadline) => {},
                }
            } else {
                notified.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Wake, Waker},
    };

    #[derive(Default)]
    struct CountWakes(AtomicUsize);

    impl Wake for CountWakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retained_success_observer_does_not_spin_after_its_original_deadline() {
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let cancellation = Cancellation::root(Some(deadline));
        let view = cancellation.view_for_test();
        let mut observer = Box::pin(view.cancelled());
        let wakes = Arc::new(CountWakes::default());
        let waker = Waker::from(Arc::clone(&wakes));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(
            cancellation.seal_success(),
            CancellationSuccessSealOutcome::Sealed
        );

        // The already-registered timer may wake once when sealing races its
        // wait. The retained observer must then park without rearming it.
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        wakes.0.store(0, Ordering::Relaxed);
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert_eq!(view.reason(), None);
        assert_eq!(view.deadline(), Some(deadline));
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
        assert_eq!(
            cancellation.request(CancellationReason::ServerShutdown),
            CancellationRequestOutcome::SuccessSealed
        );
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn unsealed_observer_keeps_deadline_cancellation() {
        let cancellation =
            Cancellation::root(Some(Instant::now() + std::time::Duration::from_secs(10)));
        let view = cancellation.view_for_test();
        let mut observer = Box::pin(view.cancelled());
        let waker = Waker::from(Arc::new(CountWakes::default()));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        assert_eq!(
            observer.as_mut().poll(&mut Context::from_waker(&waker)),
            std::task::Poll::Ready(CancellationReason::DeadlineExceeded)
        );
        assert_eq!(
            cancellation.seal_success(),
            CancellationSuccessSealOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );
    }

    struct PausePropagation {
        entered: mpsc::SyncSender<()>,
        resume: Arc<Barrier>,
    }

    impl Wake for PausePropagation {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            self.resume.wait();
        }
    }

    fn check_paused_propagation(help_from_ancestor: bool) {
        let parent = Cancellation::root(None);
        let child = parent.child(None);
        let grandchild = child.child(None);
        let child_view = child.view_for_test();
        let mut child_cancelled = Box::pin(child_view.cancelled());
        let (entered, paused) = mpsc::sync_channel(1);
        let resume = Arc::new(Barrier::new(2));
        let waker = Waker::from(Arc::new(PausePropagation {
            entered,
            resume: Arc::clone(&resume),
        }));
        assert!(
            child_cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let (visible_before_help, installed_after_help, changed) = std::thread::scope(|threads| {
            let first_request = threads.spawn(|| child.request(CancellationReason::Requested));
            paused.recv().unwrap();
            // The child flag is committed, but its request is paused inside the
            // wake callback, before it can visit the grandchild.
            let visible_before_help = grandchild.check_reason();
            let changed = if help_from_ancestor {
                parent.request(CancellationReason::ServerShutdown)
            } else {
                child.request(CancellationReason::ServerShutdown)
            };
            let installed_after_help = grandchild.state.lock().unwrap().reason.clone();
            // Resume before assertions so a failing oracle cannot strand a thread.
            resume.wait();
            assert_eq!(
                first_request.join().unwrap(),
                CancellationRequestOutcome::Requested
            );
            (visible_before_help, installed_after_help, changed)
        });
        assert_eq!(visible_before_help, Some(CancellationReason::Requested));
        assert_eq!(installed_after_help, Some(CancellationReason::Requested));
        assert_eq!(
            changed,
            if help_from_ancestor {
                CancellationRequestOutcome::Requested
            } else {
                CancellationRequestOutcome::AlreadyRequested(CancellationReason::Requested)
            }
        );
    }

    #[test]
    fn ancestor_request_completes_a_cancelled_childs_paused_propagation() {
        check_paused_propagation(true);
    }

    #[test]
    fn duplicate_request_completes_its_own_paused_propagation() {
        check_paused_propagation(false);
    }

    #[derive(Default)]
    struct CountingWaker(std::sync::atomic::AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_sealed_success_waits_without_a_timer_once_its_deadline_passed() {
        let root = Cancellation::root(Some(Instant::now() + std::time::Duration::from_millis(10)));
        assert_eq!(root.seal_success(), CancellationSuccessSealOutcome::Sealed);
        tokio::time::advance(std::time::Duration::from_millis(20)).await;
        let view = root.view_for_test();
        assert_eq!(view.reason(), None, "a sealed success is never cancelled");

        let wakes = Arc::new(CountingWaker::default());
        let waker = Waker::from(Arc::clone(&wakes));
        let mut cancelled = Box::pin(view.cancelled());
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        // Neither an elapsed timer nor a cooperative-budget yield may wake a
        // wait that nothing can ever end.
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unsealed_wait_still_ends_at_its_deadline() {
        let root = Cancellation::root(Some(Instant::now() + std::time::Duration::from_millis(10)));
        let view = root.view_for_test();
        let reason = tokio::time::timeout(std::time::Duration::from_secs(1), view.cancelled())
            .await
            .expect("the deadline ends the wait");
        assert_eq!(reason, CancellationReason::DeadlineExceeded);
    }

    #[test]
    fn subtree_request_visits_root_siblings_without_leaving_its_root() {
        let outer = Cancellation::root(None);
        let root = outer.child(None);
        let outside = outer.child(None);
        let first = root.child(None);
        let second = root.child(None);
        let third = root.child(None);
        let original_cause = first.child(None);
        assert_eq!(
            first.request(CancellationReason::OwnerDropped),
            CancellationRequestOutcome::Requested
        );
        assert_eq!(
            root.request(CancellationReason::ServerShutdown),
            CancellationRequestOutcome::Requested
        );
        assert_eq!(
            first.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::OwnerDropped)
        );
        assert_eq!(
            original_cause.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::OwnerDropped)
        );
        assert_eq!(
            second.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::ServerShutdown)
        );
        assert_eq!(
            third.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::ServerShutdown)
        );
        assert!(outer.state.lock().unwrap().reason.is_none());
        assert!(outside.state.lock().unwrap().reason.is_none());
    }

    #[test]
    fn sealed_descendant_prunes_its_subtree_but_not_its_siblings() {
        let root = Cancellation::root(None);
        let sealed = root.child(None);
        let grandchild = sealed.child(None);
        let sibling = root.child(None);
        assert_eq!(
            sealed.seal_success(),
            CancellationSuccessSealOutcome::Sealed
        );
        assert_eq!(
            root.request(CancellationReason::ServerShutdown),
            CancellationRequestOutcome::Requested
        );
        assert!(sealed.state.lock().unwrap().reason.is_none());
        // Inspect the propagation fact without triggering lazy ancestor pulls.
        assert!(grandchild.state.lock().unwrap().reason.is_none());
        assert_eq!(
            sibling.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::ServerShutdown)
        );
    }

    struct DetachDuringWake {
        current: Arc<Cancellation>,
        ancestor: Mutex<Option<Arc<Cancellation>>>,
        calls: AtomicUsize,
    }

    impl Wake for DetachDuringWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.current.detach();
            if let Some(ancestor) = self.ancestor.lock().unwrap().take() {
                ancestor.detach();
                drop(ancestor);
            }
        }
    }

    #[tokio::test]
    async fn wake_detach_and_last_external_parent_drop_do_not_lose_siblings() {
        let root = Cancellation::root(None);
        let branch = root.child(None);
        let first = branch.child(None);
        let second = branch.child(None);
        let grandchild = first.child(None);
        let outer_sibling = root.child(None);
        let view = first.view_for_test();
        let mut cancelled = Box::pin(view.cancelled());
        let wake = Arc::new(DetachDuringWake {
            current: Arc::clone(&first),
            ancestor: Mutex::new(Some(branch)),
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(Arc::clone(&wake));
        assert!(
            cancelled
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(
            root.request(CancellationReason::Requested),
            CancellationRequestOutcome::Requested
        );
        assert_eq!(wake.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            grandchild.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::Requested)
        );
        assert_eq!(
            second.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::Requested)
        );
        assert_eq!(
            outer_sibling.state.lock().unwrap().reason.clone(),
            Some(CancellationReason::Requested)
        );
    }

    #[test]
    fn fixture_child_sequences_remain_sorted_under_concurrent_registration() {
        let root = Cancellation::root(None);
        let children = std::thread::scope(|threads| {
            let start = Arc::new(Barrier::new(9));
            let mut joins = Vec::new();
            for _ in 0..8 {
                let root = Arc::clone(&root);
                let start = Arc::clone(&start);
                joins.push(threads.spawn(move || {
                    start.wait();
                    root.child(None)
                }));
            }
            start.wait();
            joins
                .into_iter()
                .map(|join| join.join().unwrap())
                .collect::<Vec<_>>()
        });
        {
            let state = root.state.lock().unwrap();
            let Children::Dynamic(children) = &state.children else {
                unreachable!()
            };
            assert!(
                children
                    .windows(2)
                    .all(|pair| pair[0].sequence < pair[1].sequence)
            );
        }
        assert_eq!(
            root.request(CancellationReason::Requested),
            CancellationRequestOutcome::Requested
        );
        for child in children {
            assert_eq!(
                child.state.lock().unwrap().reason.clone(),
                Some(CancellationReason::Requested)
            );
        }
    }

    #[test]
    fn out_of_order_child_registration_does_not_skip_a_sibling() {
        let root = Cancellation::root(None);
        let third = root.child_with_sequence(None, WorkId(3));
        let first = root.child_with_sequence(None, WorkId(1));
        let second = root.child_with_sequence(None, WorkId(2));
        let _ = root.request(CancellationReason::Requested);
        for child in [&first, &second, &third] {
            assert_eq!(
                child.state.lock().unwrap().reason.clone(),
                Some(CancellationReason::Requested)
            );
        }
    }

    #[test]
    fn concurrent_final_alias_drop_reclaims_a_deep_parent_chain_without_recursion() {
        let mut leaf = Cancellation::root(None);
        let root = Arc::downgrade(&leaf);
        // Build only the real parent custody links: inheritance lookup during
        // normal child creation is unrelated to this destructor regression.
        for _ in 0..50_000 {
            let mut next = Cancellation::root(None);
            Arc::get_mut(&mut next).unwrap().parent = Some(ParentLink {
                parent: leaf,
                sequence: WorkId(1),
            });
            leaf = next;
        }
        let other = Arc::clone(&leaf);
        let start = Arc::new(Barrier::new(2));
        std::thread::scope(|threads| {
            let second_start = Arc::clone(&start);
            let second = threads.spawn(move || {
                second_start.wait();
                drop(other);
            });
            start.wait();
            drop(leaf);
            second.join().unwrap();
        });
        assert!(root.upgrade().is_none());
    }

    #[test]
    fn deep_tree_request_installs_every_local_reason_without_recursion() {
        let root = Cancellation::root(None);
        let mut leaf = Arc::clone(&root);
        for _ in 0..2_048 {
            leaf = leaf.child(None);
        }
        assert_eq!(
            root.request(CancellationReason::Requested),
            CancellationRequestOutcome::Requested
        );
        let mut current = Some(leaf);
        let mut visited = 0;
        while let Some(node) = current {
            assert_eq!(
                node.state.lock().unwrap().reason,
                Some(CancellationReason::Requested)
            );
            visited += 1;
            current = node.parent.as_ref().map(|link| Arc::clone(&link.parent));
        }
        assert_eq!(visited, 2_049);
    }
}
