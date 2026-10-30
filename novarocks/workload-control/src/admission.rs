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

use crate::{
    CancellationReason, QueryConcurrencyPermit, ResultWindowClass, ResultWindowGrant, WorkError,
    WorkId, WorkScope,
    result_window::{adopt_window, count_window, uncount_window, window_room},
    scope::{Inner, State, WorkloadConfig},
};
use std::{
    future::Future,
    ops::Bound::{Excluded, Unbounded},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tokio::time::Instant;

mod delivery;
pub(crate) use delivery::{drain_wakes, has_pending_wakes};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage {
    Preparation,
    Execution,
}

#[derive(Clone, Copy, Debug)]
pub struct StageRequest {
    pub stage: Stage,
    /// Actual retained capacity attributed to this waiting request. Shared
    /// objects must be charged by one owner. This policy declaration does not
    /// replace an allocation charge from the local resource authority.
    pub retained_bytes: u64,
}

#[derive(Clone)]
pub(crate) enum AdmissionState {
    Waiting,
    Granted,
    Rejected(WorkError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionKind {
    Query,
    Stage(Stage),
}

pub(crate) struct PendingAdmission {
    pub work: WorkId,
    pub root: WorkId,
    pub kind: AdmissionKind,
    pub bytes: u64,
    pub wait_deadline: Instant,
    pub state: AdmissionState,
    pub waker: Option<Waker>,
    // Logical receipt does not reclaim a row whose callback is still alive.
    pub retired: bool,
    pub delivery_in_flight: bool,
    pub registration_in_flight: bool,
    /// The complete result window a query takes together with its permit,
    /// and whether dispatch has counted it.
    pub window: Option<ResultWindowClass>,
    pub window_counted: bool,
}

fn capacity(state: &State, config: &WorkloadConfig, root: WorkId, kind: AdmissionKind) -> bool {
    match kind {
        AdmissionKind::Query => state.admitted_queries < config.query_concurrency_limit,
        AdmissionKind::Stage(Stage::Preparation) => state.preparation < config.preparation_limit,
        AdmissionKind::Stage(Stage::Execution) => {
            state.execution < config.execution_limit
                && state.nodes[&root].root_executions < config.executions_per_root
        }
    }
}

fn grant(state: &mut State, root: WorkId, kind: AdmissionKind) {
    match kind {
        AdmissionKind::Query => {
            state.admitted_queries += 1;
            state.nodes.get_mut(&root).unwrap().query_admitted = true;
        }
        AdmissionKind::Stage(Stage::Preparation) => state.preparation += 1,
        AdmissionKind::Stage(Stage::Execution) => {
            state.execution += 1;
            state.nodes.get_mut(&root).unwrap().root_executions += 1;
        }
    }
}

fn release(state: &mut State, work: WorkId, root: WorkId, kind: AdmissionKind) {
    match kind {
        AdmissionKind::Query => {
            state.admitted_queries -= 1;
            state.nodes.get_mut(&root).unwrap().query_admitted = false;
        }
        AdmissionKind::Stage(Stage::Preparation) => state.preparation -= 1,
        AdmissionKind::Stage(Stage::Execution) => {
            state.execution -= 1;
            state.nodes.get_mut(&root).unwrap().root_executions -= 1;
        }
    }
    if let AdmissionKind::Stage(stage) = kind {
        state.nodes.get_mut(&work).unwrap().stages.remove(&stage);
    }
}

fn remove_queue(state: &mut State, root: WorkId, id: u64, kind: AdmissionKind) {
    match kind {
        AdmissionKind::Query => state.query_queue.remove(root, id),
        AdmissionKind::Stage(Stage::Preparation) => state.preparation_queue.remove(root, id),
        AdmissionKind::Stage(Stage::Execution) => state.execution_queue.remove(root, id),
    }
}

pub(crate) fn dispatch(state: &mut State, config: &WorkloadConfig) {
    // Reject before granting, using the original ordered map rather than a
    // detached allocation. No callback or row insertion can run under State.
    let mut after = 0;
    while let Some((&id, request)) = state.requests.range((Excluded(after), Unbounded)).next() {
        after = id;
        if request.retired || matches!(request.state, AdmissionState::Rejected(_)) {
            continue;
        }
        let Some(error) = state.nodes[&request.work]
            .check(&state.cancellations)
            .err()
            .or_else(|| {
                (Instant::now() >= request.wait_deadline).then_some(WorkError::CapacityWaitTimeout)
            })
        else {
            continue;
        };
        let request = &state.requests[&id];
        let (work, root, kind, granted, window) = (
            request.work,
            request.root,
            request.kind,
            matches!(request.state, AdmissionState::Granted),
            request.window.filter(|_| request.window_counted),
        );
        remove_queue(state, root, id, kind);
        if granted {
            release(state, work, root, kind);
            if let Some(class) = window {
                uncount_window(state, root, class);
                state.requests.get_mut(&id).unwrap().window_counted = false;
            }
        } else if let AdmissionKind::Stage(stage) = kind {
            state.nodes.get_mut(&work).unwrap().stages.remove(&stage);
        }
        let request = state.requests.get_mut(&id).unwrap();
        request.state = AdmissionState::Rejected(error);
    }
    for kind in [
        AdmissionKind::Query,
        AdmissionKind::Stage(Stage::Preparation),
        AdmissionKind::Stage(Stage::Execution),
    ] {
        loop {
            let available = match kind {
                AdmissionKind::Query => state.admitted_queries < config.query_concurrency_limit,
                AdmissionKind::Stage(Stage::Preparation) => {
                    state.preparation < config.preparation_limit
                }
                AdmissionKind::Stage(Stage::Execution) => state.execution < config.execution_limit,
            };
            if !available {
                break;
            }
            let nodes = &state.nodes;
            let capacity = (&state.result_capacity, &state.result_windows);
            // A query whose result window class is full stays queued without
            // taking the permit; others behind it may still proceed.
            let ready = |root: WorkId| match kind {
                AdmissionKind::Query => nodes[&root]
                    .pending_query_window
                    .is_none_or(|class| window_room(capacity.0, capacity.1, class)),
                AdmissionKind::Stage(Stage::Execution) => {
                    nodes[&root].root_executions < config.executions_per_root
                }
                AdmissionKind::Stage(Stage::Preparation) => true,
            };
            let next = match kind {
                AdmissionKind::Query => state.query_queue.pop_runnable(ready),
                AdmissionKind::Stage(Stage::Preparation) => {
                    state.preparation_queue.pop_runnable(ready)
                }
                AdmissionKind::Stage(Stage::Execution) => state.execution_queue.pop_runnable(ready),
            };
            let Some(id) = next else {
                break;
            };
            let root = state.requests[&id].root;
            grant(state, root, kind);
            // Permit and complete window are one transaction: room was
            // checked by `ready` under this same authority lock.
            if let Some(class) = state.requests[&id].window {
                count_window(state, root, class).expect("ready checked complete window room");
                state.requests.get_mut(&id).unwrap().window_counted = true;
            }
            let request = state.requests.get_mut(&id).unwrap();
            request.state = AdmissionState::Granted;
        }
    }
}

fn take_request(state: &mut State, id: u64, receive: bool) {
    let request = state.requests.get_mut(&id).unwrap();
    assert!(!request.retired, "an admission has one logical receipt");
    request.retired = true;
    let (work, root, kind, admission, window) = (
        request.work,
        request.root,
        request.kind,
        request.state.clone(),
        request.window.filter(|_| request.window_counted),
    );
    let node = state.nodes.get_mut(&work).unwrap();
    if kind == AdmissionKind::Query {
        node.pending_query = false;
        node.pending_query_window = None;
    }
    match admission {
        AdmissionState::Granted if !receive => {
            release(state, work, root, kind);
            if let Some(class) = window {
                uncount_window(state, root, class);
            }
        }
        AdmissionState::Waiting => {
            remove_queue(state, root, id, kind);
            if let AdmissionKind::Stage(stage) = kind {
                state.nodes.get_mut(&work).unwrap().stages.remove(&stage);
            }
        }
        _ => {}
    }
    finish_retired(state, id);
}

fn finish_retired(state: &mut State, id: u64) -> bool {
    let request = &state.requests[&id];
    if !request.retired
        || request.delivery_in_flight
        || request.registration_in_flight
        || request.waker.is_some()
    {
        return false;
    }
    let request = state.requests.remove(&id).unwrap();
    state.waiting_bytes -= request.bytes;
    state
        .nodes
        .get_mut(&request.work)
        .unwrap()
        .pending_admissions -= 1;
    state.collect(request.work);
    true
}

impl WorkScope {
    /// Creates a scope-bound lifecycle marker without queueing or reserving
    /// FE capacity. Query concurrency is admitted once at the warehouse
    /// boundary; execution keeps this marker only to retain the existing
    /// ownership checks while the legacy stage quota is removed.
    pub fn track_stage(&self, stage: Stage) -> Result<StagePermit, WorkError> {
        let root = self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            Ok(node.root)
        })?;
        Ok(StagePermit {
            scope: Some(self.clone()),
            root,
            stage,
            capacity_admission: false,
        })
    }

    /// Queue a stage without holding another stage or a business object lock.
    /// For two-condition acquisition, try the other condition without waiting;
    /// on failure drop the stage permit and requeue. Product locks stay external.
    pub fn acquire(&self, request: StageRequest) -> Result<StageAdmission, WorkError> {
        let cancellation = self.cancellation()?;
        let wait_deadline = Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let mut pending = StageAdmission {
            scope: self.clone(),
            id: None,
            cancelled: Box::pin(async move { cancellation.cancelled().await }),
            capacity_timeout: Box::pin(
                async move { tokio::time::sleep_until(wait_deadline).await },
            ),
        };
        self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.stages.contains(&request.stage) {
                return Err(WorkError::AlreadyAdmitted);
            }
            let root = node.root;
            if state.waiting_records() >= self.inner.config.waiting_limit {
                return Err(WorkError::Capacity("waiting entries"));
            }
            let bytes = state
                .waiting_bytes
                .checked_add(request.retained_bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            if bytes > self.inner.config.waiting_bytes {
                return Err(WorkError::Capacity("waiting bytes"));
            }
            let id = state.next_id()?;
            state.requests.insert(
                id,
                PendingAdmission {
                    work: self.id,
                    root,
                    kind: AdmissionKind::Stage(request.stage),
                    bytes: request.retained_bytes,
                    wait_deadline,
                    state: AdmissionState::Waiting,
                    waker: None,
                    retired: false,
                    delivery_in_flight: false,
                    registration_in_flight: false,
                    window: None,
                    window_counted: false,
                },
            );
            state.waiting_bytes = bytes;
            state.peak_waiting = state.peak_waiting.max(state.requests.len());
            state.record_waiting_peak();
            state.peak_waiting_bytes = state.peak_waiting_bytes.max(bytes);
            let node = state.nodes.get_mut(&self.id).unwrap();
            node.stages.insert(request.stage);
            node.pending_admissions += 1;
            match request.stage {
                Stage::Preparation => state.preparation_queue.push(root, id),
                Stage::Execution => state.execution_queue.push(root, id),
            }
            pending.id = Some(id);
            Ok(())
        })?;
        Ok(pending)
    }

    /// Non-blocking acquisition never jumps ahead of an existing queue.
    pub fn try_acquire(&self, stage: Stage) -> Result<StagePermit, WorkError> {
        self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.stages.contains(&stage) {
                return Err(WorkError::AlreadyAdmitted);
            }
            let root = node.root;
            let queue_empty = match stage {
                Stage::Preparation => state.preparation_queue.is_empty(),
                Stage::Execution => state.execution_queue.is_empty(),
            };
            if !queue_empty
                || !capacity(state, &self.inner.config, root, AdmissionKind::Stage(stage))
            {
                return Err(WorkError::Capacity("stage admission"));
            }
            grant(state, root, AdmissionKind::Stage(stage));
            state.nodes.get_mut(&self.id).unwrap().stages.insert(stage);
            Ok(StagePermit {
                scope: Some(self.clone()),
                root,
                stage,
                capacity_admission: true,
            })
        })
    }
}

/// Dropping an unreceived grant returns its stage capacity exactly once.
#[must_use = "A stage admission must be awaited or dropped"]
pub struct StageAdmission {
    scope: WorkScope,
    id: Option<u64>,
    cancelled: Pin<Box<dyn Future<Output = CancellationReason> + Send>>,
    capacity_timeout: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Future for StageAdmission {
    type Output = Result<StagePermit, WorkError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id.expect("Stage admission polled after completion");
        if let Poll::Ready(reason) = self.cancelled.as_mut().poll(cx) {
            self.id = None;
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
            return Poll::Ready(Err(WorkError::Cancelled(reason)));
        }
        if self.capacity_timeout.as_mut().poll(cx).is_ready() {
            self.id = None;
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
            return Poll::Ready(Err(WorkError::CapacityWaitTimeout));
        }
        let inner = Arc::clone(&self.scope.inner);
        let mut registration = delivery::Registration::new(&inner, id, cx.waker());
        let mut receipt = None;
        inner.update(|state| {
            let request = state.requests.get_mut(&id).unwrap();
            let result = match &request.state {
                AdmissionState::Waiting => {
                    registration.install(request);
                    return;
                }
                AdmissionState::Granted => match request.kind {
                    AdmissionKind::Stage(stage) => Ok((request.root, stage)),
                    AdmissionKind::Query => Err(WorkError::Conflict),
                },
                AdmissionState::Rejected(error) => Err(error.clone()),
            }
            .and_then(|grant| {
                state.nodes[&self.scope.id].check(&state.cancellations)?;
                if Instant::now() >= state.requests[&id].wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                Ok(grant)
            });
            take_request(state, id, result.is_ok());
            self.id = None;
            // The original permit exists before post-State callbacks. Unwind
            // destroys it outside State and returns its actual grant once.
            receipt = Some(result.map(|(root, stage)| StagePermit {
                scope: Some(self.scope.clone()),
                root,
                stage,
                capacity_admission: true,
            }));
        });
        receipt.map_or(Poll::Pending, Poll::Ready)
    }
}

impl WorkScope {
    /// Await the warehouse-wide slot for this registered compute-producing
    /// root. The scope must be the root, so scalar children and preparation
    /// steps cannot create a nested queue entry while their parent is running.
    pub fn admit_query(&self) -> Result<QueryAdmission, WorkError> {
        self.queue_query(None)
    }

    /// Await the warehouse slot together with one complete result window of
    /// `class`. Both are taken in the same dequeue transaction, before any
    /// dispatch; neither is held while waiting for the other.
    pub fn admit_query_with_result(
        &self,
        class: ResultWindowClass,
    ) -> Result<ResultQueryAdmission, WorkError> {
        if class == ResultWindowClass::Closing {
            return Err(WorkError::Conflict);
        }
        Ok(ResultQueryAdmission {
            inner: self.queue_query(Some(class))?,
        })
    }

    fn queue_query(&self, window: Option<ResultWindowClass>) -> Result<QueryAdmission, WorkError> {
        let cancellation = self.cancellation()?;
        let wait_deadline = Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let mut pending = QueryAdmission {
            scope: self.clone(),
            id: None,
            cancelled: Box::pin(async move { cancellation.cancelled().await }),
            capacity_timeout: Box::pin(
                async move { tokio::time::sleep_until(wait_deadline).await },
            ),
        };
        self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check(&state.cancellations)?;
            if node.parent.is_some()
                || node.root != self.id
                || !node.class.uses_warehouse_concurrency()
            {
                return Err(WorkError::Conflict);
            }
            if node.query_admitted {
                return Err(WorkError::AlreadyAdmitted);
            }
            if node.pending_query {
                return Err(WorkError::Conflict);
            }
            if window.is_some() && state.result_capacity.is_none() {
                return Err(WorkError::NotReady);
            }
            if state.waiting_records() >= self.inner.config.waiting_limit {
                return Err(WorkError::Capacity("waiting entries"));
            }
            let id = state.next_id()?;
            state.requests.insert(
                id,
                PendingAdmission {
                    work: self.id,
                    root: self.id,
                    kind: AdmissionKind::Query,
                    bytes: 0,
                    wait_deadline,
                    state: AdmissionState::Waiting,
                    waker: None,
                    retired: false,
                    delivery_in_flight: false,
                    registration_in_flight: false,
                    window,
                    window_counted: false,
                },
            );
            state.peak_waiting = state.peak_waiting.max(state.requests.len());
            state.record_waiting_peak();
            let node = state.nodes.get_mut(&self.id).unwrap();
            node.pending_admissions += 1;
            node.pending_query = true;
            node.pending_query_window = window;
            state.query_queue.push(self.id, id);
            pending.id = Some(id);
            Ok(())
        })?;
        Ok(pending)
    }
}

/// A pending warehouse query admission. Its cancellation and timeout rules
/// match stage admission, but it grants a lifetime permit rather than a stage
/// permit.
#[must_use = "A query admission must be awaited or dropped"]
pub struct QueryAdmission {
    scope: WorkScope,
    id: Option<u64>,
    cancelled: Pin<Box<dyn Future<Output = CancellationReason> + Send>>,
    capacity_timeout: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl Future for QueryAdmission {
    type Output = Result<QueryConcurrencyPermit, WorkError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_admitted(cx).map(|result| {
            result.map(|admitted| {
                debug_assert!(admitted.window.is_none());
                admitted.permit
            })
        })
    }
}

/// A query permit and, for a result admission, its complete window.
struct AdmittedQuery {
    permit: QueryConcurrencyPermit,
    window: Option<ResultWindowGrant>,
}

/// A pending warehouse query admission that also takes a complete result
/// window in the same dequeue transaction.
#[must_use = "A query admission must be awaited or dropped"]
pub struct ResultQueryAdmission {
    inner: QueryAdmission,
}

impl Future for ResultQueryAdmission {
    type Output = Result<(QueryConcurrencyPermit, ResultWindowGrant), WorkError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.poll_admitted(cx).map(|result| {
            result.map(|admitted| {
                let window = admitted
                    .window
                    .expect("a result admission counts its window with its permit");
                (admitted.permit, window)
            })
        })
    }
}

impl QueryAdmission {
    fn poll_admitted(&mut self, cx: &mut Context<'_>) -> Poll<Result<AdmittedQuery, WorkError>> {
        let id = self.id.expect("Query admission polled after completion");
        if let Poll::Ready(reason) = self.cancelled.as_mut().poll(cx) {
            self.id = None;
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
            return Poll::Ready(Err(WorkError::Cancelled(reason)));
        }
        if self.capacity_timeout.as_mut().poll(cx).is_ready() {
            self.id = None;
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
            return Poll::Ready(Err(WorkError::CapacityWaitTimeout));
        }
        let inner = Arc::clone(&self.scope.inner);
        let mut registration = delivery::Registration::new(&inner, id, cx.waker());
        let mut receipt = None;
        inner.update(|state| {
            let request = state.requests.get_mut(&id).unwrap();
            let window = request.window.filter(|_| request.window_counted);
            let result = match &request.state {
                AdmissionState::Waiting => {
                    registration.install(request);
                    return;
                }
                AdmissionState::Granted => Ok(()),
                AdmissionState::Rejected(error) => Err(error.clone()),
            }
            .and_then(|grant| {
                state.nodes[&self.scope.id].check(&state.cancellations)?;
                if Instant::now() >= state.requests[&id].wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                Ok(grant)
            });
            take_request(state, id, result.is_ok());
            self.id = None;
            receipt = Some(result.map(|()| AdmittedQuery {
                permit: QueryConcurrencyPermit {
                    scope: Some(self.scope.clone()),
                },
                window: window.map(|class| {
                    let config = state
                        .result_capacity
                        .expect("a counted window has its capacity profile");
                    adopt_window(&self.scope, class, config.all_objects_bytes[class.index()])
                }),
            }));
        });
        receipt.map_or(Poll::Pending, Poll::Ready)
    }
}

impl Drop for QueryAdmission {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
        }
    }
}

impl Drop for StageAdmission {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.scope.inner.update(|state| {
                take_request(state, id, false);
            });
        }
    }
}

/// Permission to advance one stage. Release is not a Task stop or allocation
/// release; those facts remain represented by their own obligations/charges.
pub struct StagePermit {
    scope: Option<WorkScope>,
    root: WorkId,
    stage: Stage,
    capacity_admission: bool,
}

impl StagePermit {
    pub fn check(&self, scope: &WorkScope, required: Stage) -> Result<(), WorkError> {
        let owner = self.scope.as_ref().unwrap();
        if !Arc::ptr_eq(&owner.inner, &scope.inner) || owner.id != scope.id {
            return Err(WorkError::ForeignAuthority);
        }
        if self.stage != required {
            return Err(WorkError::Conflict);
        }
        scope.check()
    }

    pub fn stage(&self) -> Stage {
        self.stage
    }
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for StagePermit {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            if !self.capacity_admission {
                return;
            }
            scope.inner.update(|state| {
                release(state, scope.id, self.root, AdmissionKind::Stage(self.stage));
                state.collect(scope.id);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkClass, WorkRequest, WorkloadConfig, WorkloadControl};

    #[tokio::test]
    async fn a_query_without_a_result_window_has_one_pending_request_and_reply() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control.mark_ready().unwrap();
        let blocker = control
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_permit = blocker.owner.scope().admit_query().unwrap().await.unwrap();
        let queued = control
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = queued.owner.scope();
        let pending = scope.admit_query().unwrap();
        assert!(matches!(scope.admit_query(), Err(WorkError::Conflict)));
        assert_eq!(control.snapshot().admission_records, 1);
        drop(pending);
        assert_eq!(control.snapshot().admission_records, 0);
        let replacement = scope.admit_query().unwrap();
        assert!(matches!(scope.admit_query(), Err(WorkError::Conflict)));
        drop(blocker_permit);
        let admitted = replacement.await.unwrap();
        assert!(matches!(
            scope.admit_query(),
            Err(WorkError::AlreadyAdmitted)
        ));
        assert_eq!(control.snapshot().admission_records, 0);
        blocker.owner.complete();
        queued.owner.complete();
        drop(admitted);
        assert_eq!(control.snapshot().root_responsibilities, 0);
    }
}

#[cfg(test)]
mod delivery_tests;
