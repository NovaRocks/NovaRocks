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

use std::sync::{Arc, Mutex, Weak};
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancellationReason {
    Requested,
    ExplicitKill { requester_connection_id: u64 },
    ExplicitKillConnection { requester_connection_id: u64 },
    ClientDisconnected,
    DeadlineExceeded,
    FrontendDrainDeadlineExceeded,
    ServerShutdown,
    OwnerDropped,
}

#[derive(Default)]
struct State {
    reason: Option<CancellationReason>,
    children: Vec<Weak<Cancellation>>,
}

pub(crate) struct Cancellation {
    state: Mutex<State>,
    changed: Notify,
    deadline: Option<Instant>,
    parent: Option<Weak<Cancellation>>,
}

impl Cancellation {
    pub(crate) fn root(deadline: Option<Instant>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            deadline,
            parent: None,
        })
    }

    pub(crate) fn child(self: &Arc<Self>, deadline: Option<Instant>) -> Arc<Self> {
        let inherited = self.check_reason();
        let deadline = match (self.deadline, deadline) {
            (Some(parent), Some(child)) => Some(parent.min(child)),
            (parent, child) => parent.or(child),
        };
        let child = Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            deadline,
            parent: Some(Arc::downgrade(self)),
        });
        // Register and inspect cancellation under one lock. A parent request
        // cannot fall between the child's inheritance and subscription.
        let mut state = self.state.lock().unwrap();
        state.children.retain(|child| child.strong_count() != 0);
        state.children.push(Arc::downgrade(&child));
        // The child is unpublished and has no descendants or subscribers yet.
        // Do not request cancellation while holding the parent's state lock.
        child.state.lock().unwrap().reason = state.reason.clone().or(inherited);
        child
    }

    pub(crate) fn request(&self, reason: CancellationReason) -> bool {
        let inherited = self.ancestor_reason();
        let (changed, mut children) = {
            let mut state = self.state.lock().unwrap();
            let changed = state.reason.is_none();
            let effective_reason = state
                .reason
                .get_or_insert_with(|| inherited.unwrap_or(reason))
                .clone();
            let children = state
                .children
                .iter()
                .filter_map(Weak::upgrade)
                .map(|child| (child, effective_reason.clone()))
                .collect::<Vec<_>>();
            (changed, children)
        };
        if changed {
            self.changed.notify_waiters();
        }
        // Deep responsibility trees must not recurse on the cancellation stack.
        // An already-cancelled node may still be propagating on another thread;
        // help traverse it instead of treating its local flag as a subtree proof.
        while let Some((child, inherited_reason)) = children.pop() {
            let (changed, descendants) = {
                let mut state = child.state.lock().unwrap();
                let changed = state.reason.is_none();
                let effective_reason = state.reason.get_or_insert(inherited_reason).clone();
                let descendants = state
                    .children
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|descendant| (descendant, effective_reason.clone()))
                    .collect::<Vec<_>>();
                (changed, descendants)
            };
            if changed {
                child.changed.notify_waiters();
            }
            children.extend(descendants);
        }
        // Keep the return value about this node's first-wins decision only.
        changed
    }

    pub(crate) fn detach(self: &Arc<Self>) {
        if let Some(parent) = self.parent.as_ref().and_then(Weak::upgrade) {
            let this = Arc::downgrade(self);
            parent
                .state
                .lock()
                .unwrap()
                .children
                .retain(|child| !Weak::ptr_eq(child, &this));
        }
    }

    pub(crate) fn reason(&self) -> Option<CancellationReason> {
        if let Some(reason) = self.state.lock().unwrap().reason.clone() {
            return Some(reason);
        }
        let reason = self.check_reason()?;
        self.request(reason);
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
        let mut ancestor = self.parent.as_ref().and_then(Weak::upgrade);
        while let Some(node) = ancestor {
            if let Some(reason) = node.state.lock().unwrap().reason.clone() {
                return Some(reason);
            }
            ancestor = node.parent.as_ref().and_then(Weak::upgrade);
        }
        None
    }

    pub(crate) fn view(self: &Arc<Self>) -> CancellationView {
        CancellationView {
            inner: Arc::clone(self),
        }
    }
}

/// Read-only cancellation capability. Cancelling never releases resources.
#[derive(Clone)]
pub struct CancellationView {
    inner: Arc<Cancellation>,
}

impl CancellationView {
    pub fn reason(&self) -> Option<CancellationReason> {
        self.inner.reason()
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.inner.deadline
    }

    pub async fn cancelled(&self) -> CancellationReason {
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reason) = self.reason() {
                return reason;
            }
            if let Some(deadline) = self.deadline() {
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
        sync::{Barrier, mpsc},
        task::{Context, Wake, Waker},
    };

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
        let child_view = child.view();
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
            assert!(first_request.join().unwrap());
            (visible_before_help, installed_after_help, changed)
        });
        assert_eq!(visible_before_help, Some(CancellationReason::Requested));
        assert_eq!(installed_after_help, Some(CancellationReason::Requested));
        assert_eq!(changed, help_from_ancestor);
    }

    #[test]
    fn ancestor_request_completes_a_cancelled_childs_paused_propagation() {
        check_paused_propagation(true);
    }

    #[test]
    fn duplicate_request_completes_its_own_paused_propagation() {
        check_paused_propagation(false);
    }
}
