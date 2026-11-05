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
//! Sticky process-supervision facts for an unresolved execution exit.
//! This is independent from Task conclusion acceptance and owns no Task port,
//! original failure source, work resource, callback or recovery authority.

use novarocks_execution_contract::{TaskFailurePhase, TaskIdentity};
use novarocks_failure_contract::FailureFact;
use std::{fmt, sync::OnceLock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionExitOrigin {
    Task {
        identity: TaskIdentity,
        phase: TaskFailurePhase,
    },
    Local,
    Process,
}

impl fmt::Display for ExecutionExitOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task { identity, phase } => {
                // TaskIdentity's current Display omits the attempt ordinal.
                // Supervision must distinguish the complete exact identity.
                let query = identity.query_execution_id();
                write!(
                    formatter,
                    "task(query={},attempt={},stage={},task={},backend={},phase={})",
                    query.query_id(),
                    query.attempt_id().get(),
                    identity.stage_id(),
                    identity.task_id(),
                    identity.backend_process_id(),
                    phase
                )
            }
            Self::Local => formatter.write_str("local"),
            Self::Process => formatter.write_str("process"),
        }
    }
}

/// Neutral facts only. Copying this record cannot prolong an originating
/// failure graph or any execution owner, and grants no Task conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionExitFault {
    origin: ExecutionExitOrigin,
    fact: FailureFact,
}

impl ExecutionExitFault {
    pub const fn origin(self) -> ExecutionExitOrigin {
        self.origin
    }
    pub const fn fact(self) -> FailureFact {
        self.fact
    }
}

impl fmt::Display for ExecutionExitFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unresolved execution exit: origin={} class={:?} evidence={:?} reason={} row={:?} resource={:?} representation={:?}",
            self.origin,
            self.fact.class(),
            self.fact.evidence(),
            self.fact.reason(),
            self.fact.row_position(),
            self.fact.resource(),
            self.fact.representation()
        )
    }
}

/// One runtime-owned health latch. An unhealthy observation is sticky; it
/// does not select a Task cause, declare a work exit, or release a resource.
#[derive(Debug, Default)]
pub struct ExecutionHealth {
    first: OnceLock<ExecutionExitFault>,
}

impl ExecutionHealth {
    pub const fn new() -> Self {
        Self {
            first: OnceLock::new(),
        }
    }

    /// The producing exit owner supplies already projected facts. A losing
    /// report drops only this Copy record; no foreign destructor or formatter
    /// runs here. The first successful publication remains until runtime exit.
    pub fn report_unresolved(&self, origin: ExecutionExitOrigin, fact: FailureFact) {
        let _ = self.first.set(ExecutionExitFault { origin, fact });
    }

    pub fn failure(&self) -> Option<ExecutionExitFault> {
        self.first.get().copied()
    }

    /// A cheap observation, not admission, a resource receipt, or a fence
    /// against a concurrent later report.
    pub fn is_healthy(&self) -> bool {
        self.first.get().is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task::ExecutionFailure;
    use novarocks_failure_contract::{
        FailureClass, FailureProjection, FailureReason, ResourceDimension, ResourceFacts,
        ResourceScope, domain,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        num::NonZeroU16,
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn fact(code: u16) -> FailureFact {
        // Fixture-only reason; production domains supply their own reason.
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::EXECUTION, NonZeroU16::new(code).unwrap()),
        )
    }
    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(741, 742), AttemptId::new(23).unwrap()).unwrap(),
            StageId::new(7).unwrap(),
            TaskId::new(19).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn first_record_is_sticky_and_keeps_all_neutral_facts() {
        let health = ExecutionHealth::new();
        assert!(health.is_healthy());
        assert_eq!(health.failure(), None);
        let first = fact(65000).with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Task),
            constraint_scope: Some(ResourceScope::Process),
            dimension: ResourceDimension::Positions,
            requested: Some(2),
            required_growth: None,
            limit: Some(1),
            live: Some(1),
            committed: Some(1),
        });
        health.report_unresolved(ExecutionExitOrigin::Process, first);
        let original = health.failure().unwrap();
        assert_eq!(original.origin(), ExecutionExitOrigin::Process);
        assert_eq!(original.fact(), first);
        assert!(!health.is_healthy());
        health.report_unresolved(ExecutionExitOrigin::Local, fact(65001));
        health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: identity(),
                phase: TaskFailurePhase::Preparation,
            },
            fact(65002),
        );
        assert_eq!(health.failure(), Some(original));
        assert!(!health.is_healthy());
    }

    #[test]
    fn concurrent_reporters_observe_one_actual_winner_without_overwrite() {
        const REPORTERS: usize = 16;
        let health = Arc::new(ExecutionHealth::new());
        let start = Arc::new(Barrier::new(REPORTERS));
        let reports = (0..REPORTERS)
            .map(|index| (ExecutionExitOrigin::Local, fact(64000 + index as u16)))
            .collect::<Vec<_>>();
        let handles = reports
            .iter()
            .copied()
            .map(|(origin, fact)| {
                let health = Arc::clone(&health);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    health.report_unresolved(origin, fact);
                    health.failure().expect("one reporter has published")
                })
            })
            .collect::<Vec<_>>();
        // Join every real reporter before any winner assertion can unwind.
        let joined = handles
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Vec<_>>();
        let observed = joined.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        let winner = health.failure().unwrap();
        assert!(reports.contains(&(winner.origin(), winner.fact())));
        assert!(observed.iter().all(|record| *record == winner));
        assert!(!health.is_healthy());
    }

    #[derive(Debug)]
    struct OriginalTaskSource {
        drops: Arc<AtomicUsize>,
        formats: Arc<AtomicUsize>,
        _scope: Arc<()>,
    }
    impl fmt::Display for OriginalTaskSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.formats.fetch_add(1, Ordering::SeqCst);
            formatter.write_str("private-task-source-marker")
        }
    }
    impl std::error::Error for OriginalTaskSource {}
    impl FailureProjection for OriginalTaskSource {
        fn failure_fact(&self) -> FailureFact {
            fact(65003)
        }
    }
    impl Drop for OriginalTaskSource {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn task_origin_fact_does_not_retain_the_actual_execution_source() {
        let drops = Arc::new(AtomicUsize::new(0));
        let formats = Arc::new(AtomicUsize::new(0));
        let scope = Arc::new(());
        let weak_scope = Arc::downgrade(&scope);
        let source = ExecutionFailure::from_domain(OriginalTaskSource {
            drops: Arc::clone(&drops),
            formats: Arc::clone(&formats),
            _scope: scope,
        });
        let alias = source.clone();
        let projected = source.failure_fact();
        let original_formats = formats.load(Ordering::SeqCst);
        let task = identity();
        let health = ExecutionHealth::new();
        health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: task,
                phase: TaskFailurePhase::Execution,
            },
            projected,
        );
        let copy = health.failure().unwrap();
        let second_copy = copy;
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(weak_scope.upgrade().is_some());
        drop(alias);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(weak_scope.upgrade().is_none());
        // The live health and copied snapshots retain the exact identity/facts
        // after the real original source and its last owned scope have exited.
        assert_eq!(health.failure(), Some(second_copy));
        assert_eq!(copy.fact(), projected);
        assert_eq!(
            copy.origin(),
            ExecutionExitOrigin::Task {
                identity: task,
                phase: TaskFailurePhase::Execution
            }
        );
        let rendered = format!("{copy}");
        assert!(rendered.contains("attempt=23"));
        assert!(rendered.contains(&format!("backend={}", task.backend_process_id())));
        assert!(!rendered.contains("private-task-source-marker"));
        assert_eq!(formats.load(Ordering::SeqCst), original_formats);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
