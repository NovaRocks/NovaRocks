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

/// The actor that first requested cancellation for a statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCancellationReason {
    ExplicitKill { requester_connection_id: u32 },
    ClientDisconnected,
    DeadlineExceeded { timeout_ms: u64 },
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
    reason: Arc<OnceLock<QueryCancellationReason>>,
}

impl QueryCancellationSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view(&self) -> QueryCancellationView {
        QueryCancellationView {
            reason: Arc::clone(&self.reason),
        }
    }

    pub fn request(&self, reason: QueryCancellationReason) -> QueryCancellationRequestResult {
        match self.reason.set(reason) {
            Ok(()) => QueryCancellationRequestResult::Requested,
            Err(_) => QueryCancellationRequestResult::AlreadyRequested(
                self.reason
                    .get()
                    .expect("cancellation reason is present after rejected set")
                    .clone(),
            ),
        }
    }
}

/// A cloned, read-only observation capability for one statement.
#[derive(Clone)]
pub struct QueryCancellationView {
    reason: Arc<OnceLock<QueryCancellationReason>>,
}

impl QueryCancellationView {
    pub(crate) fn never_cancelled() -> Self {
        QueryCancellationSource::new().view()
    }

    pub fn is_cancelled(&self) -> bool {
        self.reason.get().is_some()
    }

    pub fn reason(&self) -> Option<QueryCancellationReason> {
        self.reason.get().cloned()
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
            source.request(QueryCancellationReason::ClientDisconnected),
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
        for connection_id in 0..16 {
            let source = Arc::clone(&source);
            workers.push(std::thread::spawn(move || {
                source.request(QueryCancellationReason::ExplicitKill {
                    requester_connection_id: connection_id,
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
}
