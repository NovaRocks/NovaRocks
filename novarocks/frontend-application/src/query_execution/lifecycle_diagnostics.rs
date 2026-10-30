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

#[cfg(any(test, novarocks_dev_controls))]
use std::sync::Mutex;

use crate::query_execution::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;
use novarocks_types::QueryExecutionId;

/// Immutable, query-scoped terminal convergence evidence. It is intentionally
/// produced by the attempt that owns the execution id, never reconstructed
/// from process metrics or logs.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryLifecycleConvergenceSnapshot {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) primary_error: Option<String>,
    /// Original attempt terminal input, not a second logical failure acceptor.
    pub(crate) failure: Option<novarocks_query_application::execution_error::ExecutionFailureInput>,
    /// Set only after the original Native convergence future returned proof.
    pub(crate) worker_contexts_converged: bool,
    /// Runtime Filter terminal facts are normalized only from a complete set
    /// of participant contributions. The unavailable variant records why no
    /// such set existed for this attempt.
    pub(crate) runtime_filter: RuntimeFilterTerminalRollupSnapshot,
}

#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The available variant owns the complete terminal runtime-filter snapshot."
)]
pub(crate) enum RuntimeFilterTerminalRollupSnapshot {
    Available(RuntimeFilterTerminalRollup),
    Unavailable(RuntimeFilterTerminalRollupUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterTerminalRollupUnavailable {
    TerminalOutcomesIncomplete,
}

/// Read-only diagnostic seam for frozen terminal evidence. It carries no
/// admission, scheduling, attempt, or query-outcome authority.
pub(crate) trait QueryLifecycleConvergenceReader: Send + Sync {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot>;
}

/// FE-local retained diagnostic projection shared by every task-protocol
/// producer. Query execution owners publish only after their own convergence;
/// management endpoints can observe this projection but cannot mutate it.
#[derive(Default)]
pub(crate) struct FrontendLifecycleDiagnostics {
    #[cfg(any(test, novarocks_dev_controls))]
    latest: Mutex<Option<Box<QueryLifecycleConvergenceSnapshot>>>,
    #[cfg(novarocks_dev_controls)]
    observer: std::sync::OnceLock<
        std::sync::Weak<crate::preparation_diagnostics::StatementObservationOwner>,
    >,
    #[cfg(test)]
    whitebox: std::sync::atomic::AtomicBool,
}
impl FrontendLifecycleDiagnostics {
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn close_observation(&self) {
        if let Ok(mut latest) = self.latest.lock() {
            *latest = None;
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn activate(
        &self,
        owner: &std::sync::Arc<crate::preparation_diagnostics::StatementObservationOwner>,
    ) -> Result<(), String> {
        self.observer
            .set(std::sync::Arc::downgrade(owner))
            .map_err(|_| "lifecycle observation already installed".into())
    }
    pub(crate) fn is_enabled(&self) -> bool {
        #[cfg(test)]
        if self.whitebox.load(std::sync::atomic::Ordering::Acquire) {
            return true;
        }
        #[cfg(novarocks_dev_controls)]
        {
            self.observer
                .get()
                .and_then(std::sync::Weak::upgrade)
                .is_some_and(|owner| owner.is_active())
        }
        #[cfg(not(novarocks_dev_controls))]
        false
    }
    pub(crate) fn publish(&self, snapshot: QueryLifecycleConvergenceSnapshot) {
        if !self.is_enabled() {
            return;
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(owner) = self.observer.get().and_then(std::sync::Weak::upgrade) {
            owner.publish(&snapshot);
        }
        #[cfg(any(test, novarocks_dev_controls))]
        {
            *self
                .latest
                .lock()
                .expect("frontend lifecycle diagnostics lock") = Some(Box::new(snapshot));
        }
        #[cfg(not(any(test, novarocks_dev_controls)))]
        let _ = snapshot;
    }
}
impl QueryLifecycleConvergenceReader for FrontendLifecycleDiagnostics {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        #[cfg(any(test, novarocks_dev_controls))]
        {
            let mut latest = self
                .latest
                .lock()
                .expect("frontend lifecycle diagnostics lock");
            if !self.is_enabled() {
                *latest = None;
                return None;
            }
            latest.as_deref().cloned()
        }
        #[cfg(not(any(test, novarocks_dev_controls)))]
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FrontendLifecycleDiagnostics, QueryLifecycleConvergenceReader,
        QueryLifecycleConvergenceSnapshot, RuntimeFilterTerminalRollupSnapshot,
        RuntimeFilterTerminalRollupUnavailable,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};

    #[test]
    fn retains_the_latest_frozen_attempt_without_execution_authority() {
        let diagnostics = FrontendLifecycleDiagnostics::default();
        diagnostics
            .whitebox
            .store(true, std::sync::atomic::Ordering::Release);
        let first =
            QueryExecutionId::new(QueryId::new(41, 42), AttemptId::new(1).expect("attempt"))
                .expect("execution id");
        let second =
            QueryExecutionId::new(QueryId::new(41, 43), AttemptId::new(1).expect("attempt"))
                .expect("execution id");

        assert!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&diagnostics).is_none()
        );
        for execution_id in [first, second] {
            diagnostics.publish(QueryLifecycleConvergenceSnapshot {
                execution_id,
                primary_error: None,
                failure: None,
                worker_contexts_converged: false,
                runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
                ),
            });
        }

        assert_eq!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&diagnostics)
                .expect("latest diagnostic")
                .execution_id,
            second
        );
    }
}
