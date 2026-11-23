// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! FE serving admission owned by the query application.
//!
//! Role orchestration alone drives Ready, Draining, and Stopping.  This type
//! owns the one mutex that makes that transition and session registration
//! linearizable, so an idle connection cannot race a drain into a new session.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use serde::Serialize;

/// Monotonic, process-local serving state for frontend workload admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendServingState {
    Starting,
    Ready,
    Draining,
    Stopping,
}

impl FrontendServingState {
    pub const fn as_metric_label(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
        }
    }

    pub const fn node_state_name(self) -> &'static str {
        match self {
            Self::Starting => "inactive",
            Self::Ready => "active",
            Self::Draining => "draining",
            Self::Stopping => "shutting_down",
        }
    }
}

/// Typed rejection before a session can mutate query-application state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendAdmissionError {
    NotReady { state: FrontendServingState },
    Draining,
    Stopping,
}

/// Read-only facts needed by role-local serving management observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrontendServingAdmissionSnapshot {
    pub state: FrontendServingState,
    pub rejected_sessions: u64,
    pub drain_started_at: Option<SystemTime>,
    pub drain_deadline: Option<SystemTime>,
}

struct Inner {
    state: FrontendServingState,
    rejected_sessions: u64,
    drain_started_at: Option<SystemTime>,
    drain_deadline: Option<SystemTime>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            state: FrontendServingState::Starting,
            rejected_sessions: 0,
            drain_started_at: None,
            drain_deadline: None,
        }
    }
}

/// The process-runtime owner of session admission across a frontend drain.
#[derive(Clone, Default)]
pub struct FrontendServingAdmission {
    inner: Arc<Mutex<Inner>>,
}

impl FrontendServingAdmission {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> FrontendServingAdmissionSnapshot {
        let inner = self
            .inner
            .lock()
            .expect("frontend serving admission lock poisoned");
        FrontendServingAdmissionSnapshot {
            state: inner.state,
            rejected_sessions: inner.rejected_sessions,
            drain_started_at: inner.drain_started_at,
            drain_deadline: inner.drain_deadline,
        }
    }

    /// Returns the current rejection without mutating admission accounting.
    pub fn admission_error(&self) -> Option<FrontendAdmissionError> {
        let state = self
            .inner
            .lock()
            .expect("frontend serving admission lock poisoned")
            .state;
        (state != FrontendServingState::Ready).then_some(admission_error(state))
    }

    /// Opens admission after the role's exact startup barrier has completed.
    pub fn mark_ready(&self) -> Result<(), FrontendAdmissionError> {
        let mut inner = self
            .inner
            .lock()
            .expect("frontend serving admission lock poisoned");
        match inner.state {
            FrontendServingState::Starting => {
                inner.state = FrontendServingState::Ready;
                Ok(())
            }
            FrontendServingState::Ready => Ok(()),
            FrontendServingState::Draining => Err(FrontendAdmissionError::Draining),
            FrontendServingState::Stopping => Err(FrontendAdmissionError::Stopping),
        }
    }

    /// Atomically closes session admission and fixes the first drain deadline.
    pub fn begin_drain(&self, timeout: Duration) -> FrontendServingState {
        let mut inner = self
            .inner
            .lock()
            .expect("frontend serving admission lock poisoned");
        if inner.state == FrontendServingState::Ready {
            let started = SystemTime::now();
            inner.state = FrontendServingState::Draining;
            inner.drain_started_at = Some(started);
            inner.drain_deadline = Some(started + timeout);
        }
        inner.state
    }

    /// Closes admission permanently during role teardown.
    pub fn mark_stopping(&self) {
        self.inner
            .lock()
            .expect("frontend serving admission lock poisoned")
            .state = FrontendServingState::Stopping;
    }

    /// Runs registration under the same lock as drain closure.
    pub fn register_session<T>(
        &self,
        registration: impl FnOnce() -> T,
    ) -> Result<T, FrontendAdmissionError> {
        let mut inner = self
            .inner
            .lock()
            .expect("frontend serving admission lock poisoned");
        match inner.state {
            FrontendServingState::Ready => Ok(registration()),
            state => {
                inner.rejected_sessions = inner.rejected_sessions.saturating_add(1);
                Err(admission_error(state))
            }
        }
    }
}

fn admission_error(state: FrontendServingState) -> FrontendAdmissionError {
    match state {
        FrontendServingState::Draining => FrontendAdmissionError::Draining,
        FrontendServingState::Stopping => FrontendAdmissionError::Stopping,
        state => FrontendAdmissionError::NotReady { state },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{FrontendAdmissionError, FrontendServingAdmission, FrontendServingState};

    #[test]
    fn session_registration_is_closed_by_the_same_linearization_domain_as_drain() {
        let admission = FrontendServingAdmission::new();
        admission.mark_ready().expect("mark ready");
        assert_eq!(admission.register_session(|| 7), Ok(7));
        assert_eq!(
            admission.begin_drain(Duration::from_secs(1)),
            FrontendServingState::Draining
        );
        assert_eq!(
            admission.register_session(|| 8),
            Err(FrontendAdmissionError::Draining)
        );
        assert_eq!(admission.snapshot().rejected_sessions, 1);
    }

    #[test]
    fn ready_is_one_way_and_first_drain_deadline_is_retained() {
        let admission = FrontendServingAdmission::new();
        admission.mark_ready().expect("mark ready");
        admission.begin_drain(Duration::from_secs(1));
        let first = admission.snapshot();
        admission.begin_drain(Duration::from_secs(60));
        assert_eq!(admission.snapshot().drain_deadline, first.drain_deadline);
        assert_eq!(
            admission.mark_ready(),
            Err(FrontendAdmissionError::Draining)
        );
    }
}
