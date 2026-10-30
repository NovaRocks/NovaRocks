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

//! Original Node custody for process-owned cancellation notifications.

use super::*;
use crate::cancellation::Cancellation;
use std::ops::Bound::{Excluded, Included};

pub(super) enum Notification {
    Idle,
    Pending(CancellationReason),
    Delivering,
}
impl Notification {
    pub(super) fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

// Actual cancellation/DFS references exit before the original Node pin.
// There is no notification batch, extra registry, or State-owned Inner guard.
struct Delivery<'a> {
    cell: Arc<Cancellation>,
    pin: DeliveryPin<'a>,
}
struct DeliveryPin<'a> {
    inner: &'a Inner,
    work: WorkId,
    reason: Option<CancellationReason>,
    completed: bool,
}
impl Delivery<'_> {
    fn run(mut self) {
        // request itself retains first-wins/success-seal and repeated-help
        // semantics. A partial callback unwind must not mark it delivered.
        self.cell.request(self.pin.reason.as_ref().unwrap().clone());
        self.pin.completed = true;
    }
}
impl Drop for DeliveryPin<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            let node = state
                .nodes
                .get_mut(&self.work)
                .expect("original notification pin retains its Node");
            assert!(matches!(
                node.cancellation_notification,
                Notification::Delivering
            ));
            if self.completed {
                node.cancellation_notification = Notification::Idle;
                node.cancellation_signalled = true;
            } else {
                // Do not roll back another requester's first-wins, success,
                // terminal-settlement, or already-signalled facts.
                node.cancellation_notification = Notification::Pending(self.reason.take().unwrap());
            }
            state.advance_progress_revision();
            state.collect(self.work);
        }
        self.inner.notify_reclaim_progress();
    }
}

fn candidate(state: &State, node: &Node) -> Option<(CancellationReason, bool)> {
    match &node.cancellation_notification {
        Notification::Pending(reason) => Some((reason.clone(), false)),
        Notification::Delivering => None,
        Notification::Idle if !node.completed && !node.cancellation_signalled => state
            .cancellations
            .cell(node.cancellation)
            .check_reason()
            .map(|reason| (reason, true)),
        Notification::Idle => None,
    }
}
fn has_ready_notifications(state: &State) -> bool {
    state
        .nodes
        .values()
        .any(|node| candidate(state, node).is_some())
}

// Both process cancellation authors hand their finite original-ID frontier
// to the same delivery path. Pending/Delivering remain original Node custody.
fn drain_notifications(inner: &Inner, frontier: WorkId) {
    let mut after = WorkId(0);
    loop {
        let mut delivery = None;
        inner.update_facts(|state| {
            let next = state
                .nodes
                .range((Excluded(after), Included(frontier)))
                .find_map(|(&id, node)| match &node.cancellation_notification {
                    Notification::Pending(reason) => Some((id, reason.clone())),
                    _ => None,
                });
            let Some((id, reason)) = next else {
                return;
            };
            after = id;
            // Discovery already queued the original intent. A retry must
            // not recreate an acknowledged or terminal-settled intent.
            let slot = state.nodes[&id].cancellation;
            let cell = Arc::clone(state.cancellations.cell(slot));
            state.nodes.get_mut(&id).unwrap().cancellation_notification = Notification::Delivering;
            // The real receiving guard exists before update's callbacks.
            delivery = Some(Delivery {
                cell,
                pin: DeliveryPin {
                    inner,
                    work: id,
                    reason: Some(reason),
                    completed: false,
                },
            });
        });
        let Some(delivery) = delivery else {
            return;
        };
        delivery.run();
    }
}

pub(super) fn cancel_active_roots(inner: &Inner, reason: CancellationReason) -> usize {
    let (frontier, requested) = inner.update(|state| {
        let frontier = state.nodes.last_key_value().map(|(&id, _)| id);
        let mut after = WorkId(0);
        let mut requested = 0;
        while let Some((&id, node)) = state
            .nodes
            .range((Excluded(after), std::ops::Bound::Unbounded))
            .find(|(_, node)| node.parent.is_none() && !node.completed)
        {
            after = id;
            let class = node.class;
            let cancellation = state.cancellations.cell(node.cancellation);
            let (outcome, effective) = cancellation.record_root_request(reason.clone());
            if outcome == CancellationRequestOutcome::Requested {
                requested += 1;
                if reason == CancellationReason::FrontendDrainDeadlineExceeded {
                    state
                        .root_lifecycle
                        .frontend_drain_deadline_cancelled
                        .increment(class);
                }
            }
            let Some(effective) = effective else {
                continue; // Success-sealed roots never acquire cancellation.
            };
            let node = state.nodes.get_mut(&id).unwrap();
            // A real in-flight delivery already owns the same immutable cell
            // decision. It cannot finish its request without notifying that cell.
            if node.cancellation_notification.is_idle() {
                node.cancellation_notification = Notification::Pending(effective);
            }
            if !node.cancellation_signalled {
                crate::observation::queue_control(state, id, crate::ControlIntent::Cancel)
                    .expect("selected root retains its original Node");
            }
        }
        (frontier, requested)
    });
    if let Some(frontier) = frontier {
        drain_notifications(inner, frontier);
    }
    requested
}

impl DeadlineExpiryHandle {
    /// Drive a finite original-ID frontier. A callback may help another Node,
    /// but cannot take a Node whose actual notification is already in flight.
    /// Unfinished propagation retains the original Node even after completion.
    pub fn expire_deadlines(&self) {
        // Discover the complete original frontier before any post-State wake.
        // Admission policy runs once for the batch, not once per notification.
        let frontier = self.inner.update(|state| {
            let frontier = state.nodes.last_key_value().map(|(&id, _)| id)?;
            let mut after = WorkId(0);
            loop {
                let next = state
                    .nodes
                    .range((Excluded(after), Included(frontier)))
                    .find_map(|(&id, node)| match candidate(state, node) {
                        Some((reason, true)) => Some((id, reason)),
                        _ => None,
                    });
                let Some((id, reason)) = next else {
                    break;
                };
                after = id;
                crate::observation::queue_control(state, id, crate::ControlIntent::Cancel)
                    .expect("selected notification retains its original Node");
                state.nodes.get_mut(&id).unwrap().cancellation_notification =
                    Notification::Pending(reason);
            }
            Some(frontier)
        });
        if let Some(frontier) = frontier {
            drain_notifications(&self.inner, frontier);
        }
    }

    /// An existing Pending continuation is immediately runnable. A Delivering
    /// Node contributes no old deadline while another caller owns its callback.
    pub fn next_deadline(&self) -> Option<Instant> {
        let now = Instant::now();
        let state = self.inner.state.lock().unwrap();
        state
            .nodes
            .values()
            .filter_map(|node| {
                match &node.cancellation_notification {
                    Notification::Pending(_) => Some(now),
                    Notification::Delivering => None,
                    Notification::Idle if !node.completed && !node.cancellation_signalled => {
                        let cell = state.cancellations.cell(node.cancellation);
                        // Also expose ready inherited cancellation without a local
                        // deadline: it may arrive before the owner's marker capture.
                        if cell.check_reason().is_some() {
                            Some(now)
                        } else {
                            cell.deadline()
                        }
                    }
                    Notification::Idle => None,
                }
            })
            .chain(
                state
                    .requests
                    .values()
                    .filter(|request| {
                        !request.retired
                            && !matches!(
                                request.state,
                                crate::admission::AdmissionState::Rejected(_)
                            )
                    })
                    .map(|request| request.wait_deadline),
            )
            .min()
    }

    /// Capture the original event revision before checking continuation readiness.
    pub fn progress_revision(&self) -> WorkloadProgressRevision {
        self.inner
            .capture_progress(&self.inner.state.lock().unwrap())
    }

    /// Wait for a state revision or runnable cancellation continuation. Ready
    /// work may predate `after`; return it to the original expiry owner instead
    /// of spinning in this waiter or inventing a new progress revision.
    pub async fn wait_state_change(&self, after: WorkloadProgressRevision) {
        loop {
            crate::admission::drain_wakes(&self.inner);
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let advanced = {
                let state = self.inner.state.lock().unwrap();
                if self.inner.capture_progress(&state) != after || has_ready_notifications(&state) {
                    true
                } else if crate::admission::has_pending_wakes(&state) {
                    continue;
                } else {
                    false
                }
            };
            if advanced {
                return;
            }
            changed.await;
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod root_tests;
