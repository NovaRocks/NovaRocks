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

use std::sync::Mutex;

use crate::query_execution::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;
use novarocks_types::QueryExecutionId;

/// Typed origin of a query failure retained by a terminal diagnostic record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecycleConvergenceErrorSource {
    BackendAttestation,
    FrontendLiveness,
    NoOutcome,
}

/// Immutable, query-scoped terminal convergence evidence. It is intentionally
/// produced by the attempt that owns the execution id, never reconstructed
/// from process metrics or logs.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryLifecycleConvergenceSnapshot {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) error_source: Option<QueryLifecycleConvergenceErrorSource>,
    pub(crate) primary_error: Option<String>,
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
    latest: Mutex<Option<Box<QueryLifecycleConvergenceSnapshot>>>,
}

impl FrontendLifecycleDiagnostics {
    pub(crate) fn publish(&self, snapshot: QueryLifecycleConvergenceSnapshot) {
        *self
            .latest
            .lock()
            .expect("frontend lifecycle diagnostics lock") = Some(Box::new(snapshot));
    }
}

impl QueryLifecycleConvergenceReader for FrontendLifecycleDiagnostics {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        self.latest
            .lock()
            .expect("frontend lifecycle diagnostics lock")
            .as_deref()
            .cloned()
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
                error_source: None,
                primary_error: None,
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
