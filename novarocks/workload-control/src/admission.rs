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
    CancellationReason, WorkError, WorkId, WorkScope,
    scope::{State, WorkloadConfig},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use tokio::time::Instant;

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

pub(crate) struct PendingAdmission {
    pub work: WorkId,
    pub root: WorkId,
    pub stage: Stage,
    pub bytes: u64,
    pub wait_deadline: Instant,
    pub state: AdmissionState,
    pub waker: Option<Waker>,
}

fn capacity(state: &State, config: &WorkloadConfig, root: WorkId, stage: Stage) -> bool {
    match stage {
        Stage::Preparation => state.preparation < config.preparation_limit,
        Stage::Execution => {
            state.execution < config.execution_limit
                && state.nodes[&root].root_executions < config.executions_per_root
        }
    }
}

fn grant(state: &mut State, root: WorkId, stage: Stage) {
    match stage {
        Stage::Preparation => state.preparation += 1,
        Stage::Execution => {
            state.execution += 1;
            state.nodes.get_mut(&root).unwrap().root_executions += 1;
        }
    }
}

fn release(state: &mut State, work: WorkId, root: WorkId, stage: Stage) {
    match stage {
        Stage::Preparation => state.preparation -= 1,
        Stage::Execution => {
            state.execution -= 1;
            state.nodes.get_mut(&root).unwrap().root_executions -= 1;
        }
    }
    state.nodes.get_mut(&work).unwrap().stages.remove(&stage);
}

fn remove_queue(state: &mut State, root: WorkId, id: u64, stage: Stage) {
    match stage {
        Stage::Preparation => state.preparation_queue.remove(root, id),
        Stage::Execution => state.execution_queue.remove(root, id),
    }
}

pub(crate) fn dispatch(state: &mut State, config: &WorkloadConfig) -> Vec<Waker> {
    let mut wake = Vec::new();
    let rejected = state
        .requests
        .iter()
        .filter_map(|(&id, request)| {
            if matches!(request.state, AdmissionState::Rejected(_)) {
                return None;
            }
            state.nodes[&request.work]
                .check()
                .err()
                .or_else(|| {
                    (Instant::now() >= request.wait_deadline)
                        .then_some(WorkError::CapacityWaitTimeout)
                })
                .map(|error| (id, error))
        })
        .collect::<Vec<_>>();
    for (id, error) in rejected {
        let request = &state.requests[&id];
        let (work, root, stage, granted) = (
            request.work,
            request.root,
            request.stage,
            matches!(request.state, AdmissionState::Granted),
        );
        remove_queue(state, root, id, stage);
        if granted {
            release(state, work, root, stage);
        } else {
            state.nodes.get_mut(&work).unwrap().stages.remove(&stage);
        }
        let request = state.requests.get_mut(&id).unwrap();
        request.state = AdmissionState::Rejected(error);
        if let Some(waker) = request.waker.take() {
            wake.push(waker);
        }
    }
    for stage in [Stage::Preparation, Stage::Execution] {
        loop {
            let available = match stage {
                Stage::Preparation => state.preparation < config.preparation_limit,
                Stage::Execution => state.execution < config.execution_limit,
            };
            if !available {
                break;
            }
            let nodes = &state.nodes;
            let ready = |root: WorkId| {
                stage == Stage::Preparation
                    || nodes[&root].root_executions < config.executions_per_root
            };
            let next = match stage {
                Stage::Preparation => state.preparation_queue.pop_runnable(ready),
                Stage::Execution => state.execution_queue.pop_runnable(ready),
            };
            let Some(id) = next else {
                break;
            };
            let root = state.requests[&id].root;
            grant(state, root, stage);
            let request = state.requests.get_mut(&id).unwrap();
            request.state = AdmissionState::Granted;
            if let Some(waker) = request.waker.take() {
                wake.push(waker);
            }
        }
    }
    wake
}

fn take_request(state: &mut State, id: u64, receive: bool) -> PendingAdmission {
    let request = state.requests.remove(&id).unwrap();
    state.waiting_bytes -= request.bytes;
    state
        .nodes
        .get_mut(&request.work)
        .unwrap()
        .pending_admissions -= 1;
    match request.state {
        AdmissionState::Granted if !receive => {
            release(state, request.work, request.root, request.stage)
        }
        AdmissionState::Waiting => {
            remove_queue(state, request.root, id, request.stage);
            state
                .nodes
                .get_mut(&request.work)
                .unwrap()
                .stages
                .remove(&request.stage);
        }
        _ => {}
    }
    state.collect(request.work);
    request
}

impl WorkScope {
    /// Queue a stage without holding another stage or a business object lock.
    /// For two-condition acquisition, try the other condition without waiting;
    /// on failure drop the stage permit and requeue. Product locks stay external.
    pub fn acquire(&self, request: StageRequest) -> Result<StageAdmission, WorkError> {
        let cancellation = self.cancellation()?;
        let wait_deadline = Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let id = self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check()?;
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
                    stage: request.stage,
                    bytes: request.retained_bytes,
                    wait_deadline,
                    state: AdmissionState::Waiting,
                    waker: None,
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
            Ok(id)
        })?;
        Ok(StageAdmission {
            scope: self.clone(),
            id: Some(id),
            cancelled: Box::pin(async move { cancellation.cancelled().await }),
            capacity_timeout: Box::pin(
                async move { tokio::time::sleep_until(wait_deadline).await },
            ),
        })
    }

    /// Non-blocking acquisition never jumps ahead of an existing queue.
    pub fn try_acquire(&self, stage: Stage) -> Result<StagePermit, WorkError> {
        self.inner.update(|state| {
            let node = state.nodes.get(&self.id).ok_or(WorkError::Released)?;
            node.check()?;
            if node.stages.contains(&stage) {
                return Err(WorkError::AlreadyAdmitted);
            }
            let root = node.root;
            let queue_empty = match stage {
                Stage::Preparation => state.preparation_queue.is_empty(),
                Stage::Execution => state.execution_queue.is_empty(),
            };
            if !queue_empty || !capacity(state, &self.inner.config, root, stage) {
                return Err(WorkError::Capacity("stage admission"));
            }
            grant(state, root, stage);
            state.nodes.get_mut(&self.id).unwrap().stages.insert(stage);
            Ok(StagePermit {
                scope: Some(self.clone()),
                root,
                stage,
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
        let result = inner.update(|state| {
            let request = state.requests.get_mut(&id).unwrap();
            let result = match &request.state {
                AdmissionState::Waiting => {
                    if request
                        .waker
                        .as_ref()
                        .is_none_or(|waker| !waker.will_wake(cx.waker()))
                    {
                        request.waker = Some(cx.waker().clone());
                    }
                    return None;
                }
                AdmissionState::Granted => Ok((request.root, request.stage)),
                AdmissionState::Rejected(error) => Err(error.clone()),
            }
            .and_then(|grant| {
                state.nodes[&self.scope.id].check()?;
                if Instant::now() >= state.requests[&id].wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                Ok(grant)
            });
            // Receipt and cancellation share this lock. A granted request
            // cannot be revoked between observing it and creating its permit.
            take_request(state, id, result.is_ok());
            Some(result)
        });
        let Some(result) = result else {
            return Poll::Pending;
        };
        self.id = None;
        Poll::Ready(result.map(|(root, stage)| StagePermit {
            scope: Some(self.scope.clone()),
            root,
            stage,
        }))
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
            scope.inner.update(|state| {
                release(state, scope.id, self.root, self.stage);
                state.collect(scope.id);
            });
        }
    }
}
