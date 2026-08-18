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

//! Core-only aggregation of frontend-local connector read-session leases.

use novarocks_spi::connector::{
    ConnectorError, ConnectorReadSessionLease, ConnectorReadSessionOutcome,
};

use crate::query_execution::preparation::PreparedFragmentSet;

/// Owns all prepared remote-read sessions for one distributed query attempt.
/// It is deliberately separate from native artifacts and execution carriers.
pub struct ConnectorReadSessionSet {
    sessions: Vec<ConnectorReadSessionLease>,
}

impl ConnectorReadSessionSet {
    pub(crate) fn from_prepared(prepared: &PreparedFragmentSet) -> Self {
        Self {
            sessions: prepared
                .scan_bindings()
                .connector_reads()
                .filter_map(|read| read.read_session.clone())
                .collect(),
        }
    }

    pub fn start_all(&self) -> Result<(), ConnectorError> {
        for session in &self.sessions {
            if let Err(error) = session.start() {
                let context = self.abort_preserving(error.to_string());
                return Err(ConnectorError::new(error.kind(), context));
            }
        }
        Ok(())
    }

    pub fn finish_completed(&self) -> Result<(), ConnectorError> {
        let mut failure = None;
        for session in &self.sessions {
            if let Err(error) = session.finish(ConnectorReadSessionOutcome::Completed)
                && failure.is_none()
            {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub fn abort_preserving(&self, primary: impl Into<String>) -> String {
        self.sessions
            .iter()
            .fold(primary.into(), |message, session| {
                session.abort_preserving(message)
            })
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorErrorKind, ConnectorReadSession,
        ConnectorReadSessionFinalizationContext, ConnectorRequestContext,
    };

    use super::*;

    struct Cancellation(AtomicBool);

    impl ConnectorCancellation for Cancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    struct Session {
        starts: AtomicUsize,
        finishes: Mutex<Vec<ConnectorReadSessionOutcome>>,
        fail_start: bool,
    }

    impl ConnectorReadSession for Session {
        fn start(&self, _: &ConnectorRequestContext) -> Result<(), ConnectorError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            if self.fail_start {
                Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "remote start failed",
                ))
            } else {
                Ok(())
            }
        }

        fn finish(
            &self,
            outcome: ConnectorReadSessionOutcome,
            _: ConnectorReadSessionFinalizationContext,
        ) -> Result<(), ConnectorError> {
            self.finishes.lock().expect("finishes").push(outcome);
            Ok(())
        }
    }

    fn lease(session: Arc<Session>) -> ConnectorReadSessionLease {
        ConnectorReadSessionLease::try_new(
            session,
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(10),
                Arc::new(Cancellation(AtomicBool::new(false))),
                1024,
                1024,
            )
            .expect("context"),
            Duration::from_millis(10),
        )
        .expect("lease")
    }

    #[test]
    fn native_connector_read_session_start_failure_aborts_all_sessions() {
        let first = Arc::new(Session {
            starts: AtomicUsize::new(0),
            finishes: Mutex::new(Vec::new()),
            fail_start: false,
        });
        let second = Arc::new(Session {
            starts: AtomicUsize::new(0),
            finishes: Mutex::new(Vec::new()),
            fail_start: true,
        });
        let set = ConnectorReadSessionSet {
            sessions: vec![lease(Arc::clone(&first)), lease(Arc::clone(&second))],
        };

        let error = set.start_all().expect_err("second start fails");

        assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(first.starts.load(Ordering::SeqCst), 1);
        assert_eq!(second.starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            *first.finishes.lock().expect("finishes"),
            vec![ConnectorReadSessionOutcome::Aborted]
        );
        assert_eq!(
            *second.finishes.lock().expect("finishes"),
            vec![ConnectorReadSessionOutcome::Aborted]
        );
    }
}
