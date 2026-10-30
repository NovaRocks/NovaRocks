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

//! Request-scoped, first-wins cancellation capability.

use std::sync::{Arc, OnceLock};

#[derive(Default)]
struct QueryCancellationState {
    reason: OnceLock<QueryCancellationReason>,
    changed: tokio::sync::Notify,
}

/// The actor that first requested cancellation for a statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCancellationReason {
    ExecutionCancellationRequested,
    ExecutionOwnerDropped,
    ExplicitKill {
        requester_session: crate::session_control::ClientSessionKey,
    },
    ExplicitKillConnection {
        requester_session: crate::session_control::ClientSessionKey,
    },
    ClientCancelRequested,
    ClientDisconnected,
    DeadlineExceeded {
        timeout_ms: u64,
    },
    /// The bounded FE-local drain deadline expired before the attempt
    /// completed. This remains distinct from an ordinary statement deadline.
    FrontendDrainDeadlineExceeded {
        timeout_ms: u64,
    },
    ServerShutdown,
}

/// The result of attempting to cancel a statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCancellationRequestResult {
    Requested,
    AlreadyRequested(QueryCancellationReason),
}

/// The write capability for one statement cancellation lifetime.
#[derive(Clone, Default)]
pub struct QueryCancellationSource {
    state: Arc<QueryCancellationState>,
}

impl QueryCancellationSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view(&self) -> QueryCancellationView {
        QueryCancellationView {
            inner: QueryCancellationViewInner::Legacy(Arc::clone(&self.state)),
        }
    }

    pub fn request(&self, reason: QueryCancellationReason) -> QueryCancellationRequestResult {
        match self.state.reason.set(reason) {
            Ok(()) => {
                self.state.changed.notify_waiters();
                QueryCancellationRequestResult::Requested
            }
            Err(_) => QueryCancellationRequestResult::AlreadyRequested(
                self.state
                    .reason
                    .get()
                    .expect("cancellation reason is present after rejected set")
                    .clone(),
            ),
        }
    }
}

/// A cloned, read-only observation capability for one statement.
///
/// CLS-R2 boundary: the cancellation *authority* (who admits a statement and
/// who trips it) moves to the frontend; this read-only observation value stays
/// with the aggregate package because `connector` and `server` still observe
/// it. Those consumers leave with CLS-R5 and CLS-R4 respectively.
#[derive(Clone)]
pub struct QueryCancellationView {
    inner: QueryCancellationViewInner,
}

#[derive(Clone)]
enum QueryCancellationViewInner {
    Legacy(Arc<QueryCancellationState>),
    Governed {
        view: novarocks_workload_control::CancellationView,
        timeout_ms: Option<u64>,
    },
}

impl QueryCancellationView {
    pub fn governed(
        view: novarocks_workload_control::CancellationView,
        timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            inner: QueryCancellationViewInner::Governed { view, timeout_ms },
        }
    }

    pub(crate) fn governed_timeout_for_scope(
        &self,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<Option<u64>, novarocks_workload_control::WorkError> {
        match &self.inner {
            QueryCancellationViewInner::Governed { view, timeout_ms }
                if view.is_for_scope(scope) =>
            {
                Ok(*timeout_ms)
            }
            _ => Err(novarocks_workload_control::WorkError::Conflict),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    pub fn reason(&self) -> Option<QueryCancellationReason> {
        match &self.inner {
            QueryCancellationViewInner::Legacy(state) => state.reason.get().cloned(),
            QueryCancellationViewInner::Governed { view, timeout_ms } => view
                .reason()
                .map(|reason| governed_reason(reason, *timeout_ms)),
        }
    }

    /// Wait for the first cancellation without losing a transition that races
    /// subscription setup.
    pub async fn cancelled(&self) -> QueryCancellationReason {
        match &self.inner {
            QueryCancellationViewInner::Legacy(state) => loop {
                let changed = state.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(reason) = state.reason.get().cloned() {
                    return reason;
                }
                changed.await;
            },
            QueryCancellationViewInner::Governed { view, timeout_ms } => {
                governed_reason(view.cancelled().await, *timeout_ms)
            }
        }
    }
}

fn governed_reason(
    reason: novarocks_workload_control::CancellationReason,
    timeout_ms: Option<u64>,
) -> QueryCancellationReason {
    match reason {
        novarocks_workload_control::CancellationReason::ExplicitKill { requester_session } => {
            QueryCancellationReason::ExplicitKill {
                requester_session: crate::session_control::ClientSessionKey::new(requester_session)
                    .expect("registered requester session is nonzero"),
            }
        }
        novarocks_workload_control::CancellationReason::ExplicitKillConnection {
            requester_session,
        } => QueryCancellationReason::ExplicitKillConnection {
            requester_session: crate::session_control::ClientSessionKey::new(requester_session)
                .expect("registered requester session is nonzero"),
        },
        novarocks_workload_control::CancellationReason::ClientCancelRequested => {
            QueryCancellationReason::ClientCancelRequested
        }
        novarocks_workload_control::CancellationReason::ClientDisconnected => {
            QueryCancellationReason::ClientDisconnected
        }
        novarocks_workload_control::CancellationReason::DeadlineExceeded => {
            QueryCancellationReason::DeadlineExceeded {
                timeout_ms: timeout_ms.unwrap_or(0),
            }
        }
        novarocks_workload_control::CancellationReason::FrontendDrainDeadlineExceeded => {
            QueryCancellationReason::FrontendDrainDeadlineExceeded {
                timeout_ms: timeout_ms.unwrap_or(0),
            }
        }
        novarocks_workload_control::CancellationReason::ServerShutdown => {
            QueryCancellationReason::ServerShutdown
        }
        novarocks_workload_control::CancellationReason::Requested => {
            QueryCancellationReason::ExecutionCancellationRequested
        }
        novarocks_workload_control::CancellationReason::OwnerDropped => {
            QueryCancellationReason::ExecutionOwnerDropped
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn source_is_first_wins_and_views_keep_the_reason() {
        let source = QueryCancellationSource::new();
        let view = source.view();
        assert!(!view.is_cancelled());
        assert_eq!(
            source.request(QueryCancellationReason::DeadlineExceeded { timeout_ms: 10 }),
            QueryCancellationRequestResult::Requested
        );
        assert_eq!(
            source.request(QueryCancellationReason::ExplicitKillConnection {
                requester_session: crate::session_control::ClientSessionKey::new(9)
                    .expect("nonzero fixture requester"),
            }),
            QueryCancellationRequestResult::AlreadyRequested(
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 10 }
            )
        );
        assert_eq!(
            view.reason(),
            Some(QueryCancellationReason::DeadlineExceeded { timeout_ms: 10 })
        );
    }

    #[test]
    fn concurrent_requests_have_one_winner() {
        let source = Arc::new(QueryCancellationSource::new());
        let mut workers = Vec::new();
        for session_key in 1..=16 {
            let source = Arc::clone(&source);
            workers.push(std::thread::spawn(move || {
                source.request(QueryCancellationReason::ExplicitKill {
                    requester_session: crate::session_control::ClientSessionKey::new(session_key)
                        .expect("nonzero fixture requester"),
                })
            }));
        }
        let requested = workers
            .into_iter()
            .filter_map(|worker| match worker.join().expect("request worker") {
                QueryCancellationRequestResult::Requested => Some(()),
                QueryCancellationRequestResult::AlreadyRequested(_) => None,
            })
            .count();
        assert_eq!(requested, 1);
        assert!(source.view().is_cancelled());
    }

    #[tokio::test]
    async fn async_observer_sees_completion_before_subscription() {
        let source = QueryCancellationSource::new();
        let view = source.view();
        source.request(QueryCancellationReason::ServerShutdown);

        assert_eq!(
            view.cancelled().await,
            QueryCancellationReason::ServerShutdown
        );
    }

    #[tokio::test]
    async fn async_observer_wakes_after_subscription() {
        let source = QueryCancellationSource::new();
        let view = source.view();
        let observer = tokio::spawn(async move { view.cancelled().await });
        tokio::task::yield_now().await;

        source.request(QueryCancellationReason::ClientDisconnected);

        assert_eq!(
            observer.await.expect("cancellation observer"),
            QueryCancellationReason::ClientDisconnected
        );
    }
}
