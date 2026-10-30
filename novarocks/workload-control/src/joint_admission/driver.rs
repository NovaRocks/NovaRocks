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

//! Unique fair responsibility-transaction driver. Physical offers remain with
//! their original executor/transport owner, never with a feasibility proof.
use super::{
    ledger::{RequestPhase, fail_request, grant_request},
    *,
};
use crate::{
    WorkloadProgressRevision,
    control_storage::{ReapPreparation, SlotId, SlotPhase},
    scope::Inner,
};
use std::{
    ops::Bound::{Excluded, Included, Unbounded},
    sync::Arc,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JointProgress {
    Idle(WorkloadProgressRevision),
    More,
}
pub struct JointProgressDriver {
    inner: Arc<Inner>,
    config: Arc<ResponsibilityConfig>,
    cursor: Option<u64>,
    scan_left: usize,
    scan_end: Option<u64>,
    scan_revision: Option<WorkloadProgressRevision>,
    scan_idle: Option<WorkloadProgressRevision>,
    dependency_revision: Option<WorkloadProgressRevision>,
    local_cursor: Option<u64>,
    local_left: usize,
    local_end: Option<u64>,
    reclaim_cursor: usize,
    reclaim_left: usize,
    reclaim_revision: Option<WorkloadProgressRevision>,
    reclaim_idle: Option<WorkloadProgressRevision>,
    reclaim_parent: Option<SlotId>,
    last_reclaim_was_hint: bool,
    #[cfg(test)]
    pub(crate) reclaim_checks: usize,
    #[cfg(test)]
    pub(crate) request_checks: usize,
}
impl JointProgressDriver {
    pub(super) fn new(inner: Arc<Inner>, config: Arc<ResponsibilityConfig>) -> Self {
        Self {
            inner,
            config,
            cursor: None,
            scan_left: 0,
            scan_end: None,
            scan_revision: None,
            scan_idle: None,
            dependency_revision: None,
            local_cursor: None,
            local_left: 0,
            local_end: None,
            reclaim_cursor: 0,
            reclaim_left: 0,
            reclaim_revision: None,
            reclaim_idle: None,
            reclaim_parent: None,
            last_reclaim_was_hint: false,
            #[cfg(test)]
            reclaim_checks: 0,
            #[cfg(test)]
            request_checks: 0,
        }
    }
    pub fn step(&mut self) -> JointProgress {
        let local_more = self.reobserve_local_one();
        let reclaim_more = self.reclaim_one();
        let dependency_more = {
            let mut state = self.inner.state.lock().unwrap();
            let marker = self.inner.capture_progress(&state);
            if self.dependency_revision != Some(marker) {
                state.joint.as_mut().unwrap().arm_dependency_sweep();
                self.dependency_revision = Some(marker);
            }
            let reclaimed = state
                .joint
                .as_mut()
                .is_some_and(|ledger| ledger.reap_dependency());
            if reclaimed {
                state.advance_progress_revision();
            }
            reclaimed
        };
        let mut changed = false;
        for _ in 0..self.config.work_per_turn.get() {
            let result = {
                let mut state = self.inner.state.lock().unwrap();
                let marker = self.inner.capture_progress(&state);
                if self.scan_left == 0 {
                    if self.scan_idle == Some(marker) {
                        break;
                    }
                    let ledger = state.joint.as_ref().unwrap();
                    self.scan_left = ledger.requests.len();
                    self.scan_end = ledger.requests.last_key_value().map(|(&id, _)| id);
                    self.scan_revision = Some(marker);
                    self.cursor = None;
                    if self.scan_left == 0 {
                        self.scan_idle = Some(marker);
                        break;
                    }
                }
                let end = self.scan_end.expect("nonempty original transaction pass");
                let ledger = state.joint.as_ref().unwrap();
                let next = ledger
                    .requests
                    .range((self.cursor.map_or(Unbounded, Excluded), Included(end)))
                    .next()
                    .map(|(&id, _)| id);
                let Some(id) = next else {
                    self.scan_left = 0;
                    break;
                };
                self.cursor = Some(id);
                self.scan_left -= 1;
                #[cfg(test)]
                {
                    self.request_checks += 1;
                }
                let request = &ledger.requests[&id];
                if matches!(
                    request.phase,
                    RequestPhase::Failed(_)
                        | RequestPhase::LocalOffered {
                            failure: Some(_),
                            ..
                        }
                ) {
                    continue;
                }
                let waiting = matches!(request.phase, RequestPhase::Waiting);
                let stopped = if ledger.driver_closed {
                    Some(WorkError::Closed)
                } else if request.requires_business_active() {
                    if state.closed {
                        Some(WorkError::Closed)
                    } else {
                        state
                            .nodes
                            .get(&request.owner)
                            .ok_or(WorkError::Released)
                            .and_then(|node| node.check(&state.cancellations))
                            .err()
                    }
                } else {
                    None
                };
                if stopped.is_none() && !waiting {
                    continue;
                }
                if stopped.is_none()
                    && (request.local_blocked
                        || super::ledger::root_domains::request_blocked(&state, id))
                {
                    continue;
                }
                let readiness =
                    stopped.map_or_else(|| super::ledger::request_compute_room(&state, id), Err);
                if matches!(readiness, Ok(false)) {
                    continue;
                }
                let result = readiness.and_then(|_| grant_request(&mut state, id));
                let wake = match result {
                    Ok(()) => state
                        .joint
                        .as_mut()
                        .unwrap()
                        .requests
                        .get_mut(&id)
                        .unwrap()
                        .waker
                        .take(),
                    Err(error) => fail_request(&mut state, id, error),
                };
                crate::admission::dispatch(&mut state, &self.inner.config);
                crate::observation::fill_control(&mut state, &self.inner.config);
                state.advance_progress_revision();
                Some(wake)
            };
            if let Some(wake) = result {
                // A callback unwind still notifies the same role after State.
                let _exit = crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&self.inner));
                crate::admission::drain_wakes(&self.inner);
                if let Some(wake) = wake {
                    wake.wake();
                }
                changed = true;
            }
            if self.scan_left == 0 {
                break;
            }
        }
        let state = self.inner.state.lock().unwrap();
        let marker = self.inner.capture_progress(&state);
        let scan_more = if self.scan_left != 0 {
            true
        } else if self
            .scan_revision
            .is_some_and(|observed| observed != marker)
        {
            self.scan_idle = None;
            true
        } else {
            self.scan_idle = Some(marker);
            false
        };
        if changed
            || scan_more
            || local_more
            || reclaim_more
            || dependency_more
            || state
                .joint
                .as_ref()
                .is_some_and(|ledger| ledger.has_reclaimable_dependencies())
            || (state.cancellations.fixed
                && (state.cancellations.retired != 0 || state.cancellations.retired_polls != 0)
                && self.reclaim_idle != Some(marker))
        {
            JointProgress::More
        } else {
            JointProgress::Idle(marker)
        }
    }
    /// Reobserve original physical Full and actual last-alias exit. This arms
    /// one finite scan; it never grants resources or renews a request deadline.
    pub fn recheck_retired_backing(&mut self) {
        self.reclaim_idle = None;
        self.reclaim_left = 0;
        if self.local_left == 0 {
            let state = self.inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().unwrap();
            self.local_left = if ledger.local_waiters == 0 {
                0
            } else {
                ledger.requests.len()
            };
            self.local_end = ledger.requests.last_key_value().map(|(&id, _)| id);
            self.local_cursor = None;
        }
    }
    fn reobserve_local_one(&mut self) -> bool {
        if self.local_left == 0 {
            return false;
        }
        let mut state = self.inner.state.lock().unwrap();
        let ledger = state.joint.as_mut().unwrap();
        let end = self.local_end.expect("nonempty original local pass");
        let next = ledger
            .requests
            .range((self.local_cursor.map_or(Unbounded, Excluded), Included(end)))
            .next()
            .map(|(&id, _)| id);
        let Some(id) = next else {
            self.local_left = 0;
            return false;
        };
        self.local_cursor = Some(id);
        self.local_left -= 1;
        let request = ledger.requests.get_mut(&id).unwrap();
        let unblocked = request.local_blocked && matches!(request.phase, RequestPhase::Waiting);
        if unblocked {
            request.local_blocked = false;
            ledger.local_waiters -= 1;
            state.advance_progress_revision();
        }
        unblocked || self.local_left != 0
    }

    /// One cell at most, alongside the original proof step. A successful leaf
    /// gives one parent hint, alternating with the finite fair storage scan.
    fn reclaim_one(&mut self) -> bool {
        let prepared = {
            let mut state = self.inner.state.lock().unwrap();
            let marker = self.inner.capture_progress(&state);
            if !state.cancellations.fixed
                || (state.cancellations.retired == 0 && state.cancellations.retired_polls == 0)
            {
                self.reclaim_left = 0;
                self.reclaim_parent = None;
                self.reclaim_idle = Some(marker);
                return false;
            }
            if self.reclaim_left == 0 {
                if self.reclaim_idle == Some(marker) {
                    return false;
                }
                self.reclaim_left = state.cancellations.slots.len();
                self.reclaim_revision = Some(marker);
                self.reclaim_idle = None;
            }
            let hint = if self.last_reclaim_was_hint {
                None
            } else {
                self.reclaim_parent.take()
            };
            let slot = match hint {
                Some(slot) => {
                    self.last_reclaim_was_hint = true;
                    slot
                }
                None => {
                    let count = state.cancellations.slots.len();
                    let slot = SlotId(self.reclaim_cursor % count);
                    self.reclaim_cursor = (self.reclaim_cursor + 1) % count;
                    self.reclaim_left -= 1;
                    self.last_reclaim_was_hint = false;
                    slot
                }
            };
            #[cfg(test)]
            {
                self.reclaim_checks += 1;
            }
            state.cancellations.prepare_reap(slot).map(|prepared| {
                if matches!(prepared, ReapPreparation::PollRole) {
                    state.advance_progress_revision();
                }
                (slot, prepared)
            })
        };
        if matches!(prepared, Some((_, ReapPreparation::PollRole))) {
            self.reclaim_idle = None;
            self.inner.notify_reclaim_progress();
            return true;
        }
        if let Some((
            id,
            ReapPreparation::Cancellation {
                reset,
                parent,
                generation,
            },
        )) = prepared
        {
            // Parent strong references and obsolete values actually leave here,
            // outside State. Only then may the exact original claim be rekeyed.
            drop(reset);
            let retired_root = {
                let mut state = self.inner.state.lock().unwrap();
                let slot = &state.cancellations.slots[id.0];
                assert_eq!(slot.phase, SlotPhase::Retired);
                assert_eq!(slot.generation, generation);
                let claim = slot.claim.unwrap();
                let base = slot.base;
                state
                    .joint
                    .as_mut()
                    .unwrap()
                    .reset_control_slot(id, claim, base);
                if !state.cancellations.finish_reaped(id) {
                    state.joint.as_mut().unwrap().driver_closed = true;
                }
                let retired = state.cancellations.slots[id.0].retired_root_domains.take();
                state.advance_progress_revision();
                retired
            };
            drop(retired_root);
            self.reclaim_parent = parent;
            self.reclaim_idle = None;
            self.inner.notify_reclaim_progress();
            return true;
        }
        let state = self.inner.state.lock().unwrap();
        let marker = self.inner.capture_progress(&state);
        if self.reclaim_left != 0 || self.reclaim_parent.is_some() {
            return true;
        }
        if self.reclaim_revision != Some(marker) {
            return true;
        }
        self.reclaim_idle = Some(marker);
        false
    }
}
impl Drop for JointProgressDriver {
    fn drop(&mut self) {
        let wakers = self.inner.update(|state| {
            let ledger = state.joint.as_mut().unwrap();
            ledger.driver_closed = true;
            let count = ledger.requests.len();
            let mut ids = Vec::with_capacity(count);
            ids.extend(ledger.requests.keys().copied());
            let mut wakers = Vec::with_capacity(count);
            for id in ids {
                if let Some(waker) = fail_request(state, id, WorkError::Closed) {
                    wakers.push(waker);
                }
            }
            wakers
        });
        for waker in wakers {
            waker.wake();
        }
    }
}
