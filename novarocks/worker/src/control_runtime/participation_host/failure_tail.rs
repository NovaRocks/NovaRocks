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

//! Physical destruction of the failed owner's original rich projection or
//! final panic carrier, serially sharing one preadmitted capture position. The
//! supervisor keeps its original failure effect Started throughout this job.
//! This cell grants no Work, completion or replacement failure publication.

use crate::control_runtime::participation_ready::ParticipationReadyNotice;
use novarocks_control_runtime_core::identity::{ScopeId, WorkId};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::failure::{FailureClass, FailureFact};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

pub(super) const FAILURE_TAIL_CAPTURES: usize = 1;

struct State {
    // Stable backing survives a destructor panic without trying the same
    // partially destroyed object again. Successful Drop releases this backing
    // on the physical worker as part of the same observation.
    payload: Option<Box<ManuallyDrop<FailureCapture>>>,
    started: bool,
    exited: bool,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
enum FailureCapture {
    Panic {
        _original: Box<dyn Any + Send>,
    },
    Projection {
        _original: Arc<crate::task_port::TaskProjection>,
    },
    Neutral {
        _original: Box<crate::control_runtime::task::FailedTaskNeutral>,
    },
}
struct Custody {
    key: (ScopeId, WorkId),
    notice: ParticipationReadyNotice,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
}

#[derive(Clone)]
pub(super) struct FailureTail(Arc<Custody>);
#[must_use = "The original failure effect must retain its physical disposal job"]
pub(crate) struct FailureTailJob(Arc<Custody>);

impl FailureTail {
    pub(super) fn original(
        key: (ScopeId, WorkId),
        payload: Box<dyn Any + Send>,
        notice: ParticipationReadyNotice,
        health: Arc<ExecutionHealth>,
    ) -> (Self, FailureTailJob) {
        Self::capture(
            key,
            Some(FailureCapture::Panic { _original: payload }),
            notice,
            health,
        )
    }

    pub(super) fn projection(
        key: (ScopeId, WorkId),
        projection: Arc<crate::task_port::TaskProjection>,
        notice: ParticipationReadyNotice,
        health: Arc<ExecutionHealth>,
    ) -> (Self, FailureTailJob) {
        Self::capture(
            key,
            Some(FailureCapture::Projection {
                _original: projection,
            }),
            notice,
            health,
        )
    }

    pub(super) fn neutral(
        key: (ScopeId, WorkId),
        neutral: crate::control_runtime::task::FailedTaskNeutral,
        notice: ParticipationReadyNotice,
        health: Arc<ExecutionHealth>,
    ) -> (Self, FailureTailJob) {
        Self::capture(
            key,
            Some(FailureCapture::Neutral {
                _original: Box::new(neutral),
            }),
            notice,
            health,
        )
    }

    fn capture(
        key: (ScopeId, WorkId),
        payload: Option<FailureCapture>,
        notice: ParticipationReadyNotice,
        health: Arc<ExecutionHealth>,
    ) -> (Self, FailureTailJob) {
        let original = Arc::new(Custody {
            key,
            notice,
            health,
            state: Mutex::new(State {
                payload: payload.map(|value| Box::new(ManuallyDrop::new(value))),
                started: false,
                exited: false,
                panic: None,
            }),
        });
        (Self(original.clone()), FailureTailJob(original))
    }

    pub(super) fn actual_exit_observed(&self) -> bool {
        self.0.state.lock().expect("original failure tail").exited
    }
}

impl FailureTailJob {
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        self.0.key
    }

    pub(crate) fn run(self) {
        let mut payload = {
            let mut state = self.0.state.lock().expect("original failure tail start");
            assert!(
                !state.started,
                "the original panic carrier is disposed once"
            );
            state.started = true;
            state.payload.take()
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(value) = payload.as_mut() {
                // SAFETY: this job exclusively took the original stable cell;
                // started prevents a second destruction after either outcome.
                unsafe { ManuallyDrop::drop(value.as_mut()) };
            }
        }));
        match result {
            Ok(()) => {
                drop(payload);
                self.0
                    .state
                    .lock()
                    .expect("original failure tail exit")
                    .exited = true;
            }
            Err(panic) => {
                let mut state = self.0.state.lock().expect("original failure tail panic");
                state.payload = payload;
                state.panic = Some(ManuallyDrop::new(panic));
                drop(state);
                self.0.health.report_unresolved(
                    ExecutionExitOrigin::Process,
                    FailureFact::new(
                        FailureClass::Unexpected,
                        crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved
                            .reason(),
                    ),
                );
            }
        }
        self.0.notice.notify();
    }
}
