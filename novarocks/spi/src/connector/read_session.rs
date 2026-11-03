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

//! Provider-neutral, frontend-local ownership of prepared connector reads.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSessionOutcome {
    Completed,
    Aborted,
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectorReadSessionFinalizationContext {
    deadline: Instant,
}

impl ConnectorReadSessionFinalizationContext {
    pub const fn deadline(self) -> Instant {
        self.deadline
    }
}

/// Provider callback for a remote read prepared while planning splits.
pub trait ConnectorReadSession: Send + Sync {
    fn start(&self, context: &ConnectorRequestContext) -> Result<(), ConnectorError>;

    fn finish(
        &self,
        outcome: ConnectorReadSessionOutcome,
        context: ConnectorReadSessionFinalizationContext,
    ) -> Result<(), ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionState {
    Prepared,
    Started,
    Finished,
}

struct ReadSessionInner {
    session: Arc<dyn ConnectorReadSession>,
    start_context: ConnectorRequestContext,
    cleanup_timeout: Duration,
    state: Mutex<SessionState>,
}

/// A cloneable FE-local lease whose provider side effects remain at-most-once.
#[must_use = "a prepared connector read session must be finished or aborted on drop"]
#[derive(Clone)]
pub struct SharedConnectorReadSession {
    inner: Arc<ReadSessionInner>,
}

impl SharedConnectorReadSession {
    pub fn try_new(
        session: Arc<dyn ConnectorReadSession>,
        start_context: ConnectorRequestContext,
        cleanup_timeout: Duration,
    ) -> Result<Self, ConnectorError> {
        if cleanup_timeout.is_zero() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector read-session cleanup timeout must not be zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(ReadSessionInner {
                session,
                start_context,
                cleanup_timeout,
                state: Mutex::new(SessionState::Prepared),
            }),
        })
    }

    pub fn start(&self) -> Result<(), ConnectorError> {
        self.ensure_active()?;
        let mut state = self.lock_state()?;
        match *state {
            SessionState::Prepared => {
                self.inner.session.start(&self.inner.start_context)?;
                *state = SessionState::Started;
                Ok(())
            }
            SessionState::Started => Ok(()),
            SessionState::Finished => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector read session has already finished",
            )),
        }
    }

    pub fn finish(&self, outcome: ConnectorReadSessionOutcome) -> Result<(), ConnectorError> {
        let mut state = self.lock_state()?;
        if *state == SessionState::Finished {
            return Ok(());
        }
        let context = ConnectorReadSessionFinalizationContext {
            deadline: Instant::now() + self.inner.cleanup_timeout,
        };
        // A failed cleanup is still terminal: another clone must not repeat an
        // uncertain remote side effect.
        let result = self.inner.session.finish(outcome, context);
        *state = SessionState::Finished;
        result
    }

    pub fn abort_preserving(&self, primary: impl Into<String>) -> String {
        let primary = primary.into();
        match self.finish(ConnectorReadSessionOutcome::Aborted) {
            Ok(()) => primary,
            Err(cleanup) => format!("{primary} (connector read-session cleanup: {cleanup})"),
        }
    }

    fn ensure_active(&self) -> Result<(), ConnectorError> {
        if self.inner.start_context.is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled before read-session start",
            ));
        }
        if Instant::now() >= self.inner.start_context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed before read-session start",
            ));
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, SessionState>, ConnectorError> {
        self.inner.state.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector read-session state lock was poisoned",
            )
        })
    }
}

impl fmt::Debug for SharedConnectorReadSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .inner
            .state
            .lock()
            .map(|state| *state)
            .unwrap_or(SessionState::Finished);
        formatter
            .debug_struct("SharedConnectorReadSession")
            .field("state", &state)
            .finish()
    }
}

impl Drop for SharedConnectorReadSession {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.finish(ConnectorReadSessionOutcome::Aborted);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::*;

    #[derive(Default)]
    struct Session {
        starts: AtomicUsize,
        finishes: AtomicUsize,
        outcomes: Mutex<Vec<ConnectorReadSessionOutcome>>,
        fail_finish: AtomicBool,
    }

    impl ConnectorReadSession for Session {
        fn start(&self, _: &ConnectorRequestContext) -> Result<(), ConnectorError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn finish(
            &self,
            outcome: ConnectorReadSessionOutcome,
            _: ConnectorReadSessionFinalizationContext,
        ) -> Result<(), ConnectorError> {
            self.finishes.fetch_add(1, Ordering::SeqCst);
            self.outcomes.lock().expect("outcomes").push(outcome);
            if self.fail_finish.load(Ordering::SeqCst) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "cleanup failed",
                ));
            }
            Ok(())
        }
    }

    fn lease(session: Arc<Session>) -> SharedConnectorReadSession {
        SharedConnectorReadSession::try_new(
            session,
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(10),
                crate::connector::ConnectorStopOwner::new().view(),
                1024,
                1024,
            )
            .expect("context"),
            Duration::from_millis(10),
        )
        .expect("lease")
    }

    #[test]
    fn connector_read_session_clone_starts_and_finishes_once() {
        let session = Arc::new(Session::default());
        let first = lease(Arc::clone(&session));
        let second = first.clone();
        first.start().expect("start");
        second.start().expect("idempotent start");
        first
            .finish(ConnectorReadSessionOutcome::Completed)
            .expect("finish");
        second
            .finish(ConnectorReadSessionOutcome::Aborted)
            .expect("idempotent finish");
        assert_eq!(session.starts.load(Ordering::SeqCst), 1);
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        assert_eq!(
            *session.outcomes.lock().expect("outcomes"),
            vec![ConnectorReadSessionOutcome::Completed]
        );
    }

    #[test]
    fn connector_read_session_drop_aborts_once() {
        let session = Arc::new(Session::default());
        let first = lease(Arc::clone(&session));
        let second = first.clone();
        drop(first);
        assert_eq!(session.finishes.load(Ordering::SeqCst), 0);
        drop(second);
        assert_eq!(session.finishes.load(Ordering::SeqCst), 1);
        assert_eq!(
            *session.outcomes.lock().expect("outcomes"),
            vec![ConnectorReadSessionOutcome::Aborted]
        );
    }

    #[test]
    fn connector_read_session_cleanup_failure_preserves_primary_error() {
        let session = Arc::new(Session::default());
        session.fail_finish.store(true, Ordering::SeqCst);
        let lease = lease(session);
        let message = lease.abort_preserving("query failed");
        assert!(message.starts_with("query failed"));
        assert!(message.contains("cleanup failed"));
    }
}
