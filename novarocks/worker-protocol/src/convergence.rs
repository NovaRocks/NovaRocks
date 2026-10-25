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

//! Worker-local convergence facts for one task.
//!
//! A stable task conclusion is allowed to precede physical convergence. The
//! worker therefore retains one monotonic ledger after the conclusion and
//! only permits retirement once execution, output, and runtime resources have
//! each supplied their own positive evidence.

/// One immutable observation of a task's worker-local convergence.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskConvergenceSnapshot {
    version: u64,
    conclusion_stable: bool,
    actual_stopped: bool,
    output_released: bool,
    resources_converged: bool,
    retired: bool,
}

impl TaskConvergenceSnapshot {
    pub const fn version(self) -> u64 {
        self.version
    }

    pub const fn conclusion_stable(self) -> bool {
        self.conclusion_stable
    }

    pub const fn actual_stopped(self) -> bool {
        self.actual_stopped
    }

    pub const fn output_released(self) -> bool {
        self.output_released
    }

    pub const fn resources_converged(self) -> bool {
        self.resources_converged
    }

    pub const fn retired(self) -> bool {
        self.retired
    }

    /// Retirement may discard executable state only after every independent
    /// responsibility has supplied positive evidence.
    pub const fn retirement_ready(self) -> bool {
        self.conclusion_stable
            && self.actual_stopped
            && self.output_released
            && self.resources_converged
            && !self.retired
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TaskConvergenceAdvance {
    Advanced(TaskConvergenceSnapshot),
    Idempotent(TaskConvergenceSnapshot),
}

impl TaskConvergenceAdvance {
    pub const fn snapshot(self) -> TaskConvergenceSnapshot {
        match self {
            Self::Advanced(snapshot) | Self::Idempotent(snapshot) => snapshot,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TaskConvergenceRejection {
    ResourcesBeforeActualStop,
    RetirementNotReady,
    AlreadyRetired,
    VersionExhausted,
}

/// The Worker-owned mutable convergence ledger for one task.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskConvergence {
    snapshot: TaskConvergenceSnapshot,
}

#[derive(Copy, Clone)]
enum ConvergenceFact {
    ConclusionStable,
    ActualStopped,
    OutputReleased,
    ResourcesConverged,
    Retired,
}

impl TaskConvergence {
    pub const fn new() -> Self {
        Self {
            snapshot: TaskConvergenceSnapshot {
                version: 0,
                conclusion_stable: false,
                actual_stopped: false,
                output_released: false,
                resources_converged: false,
                retired: false,
            },
        }
    }

    pub const fn snapshot(&self) -> TaskConvergenceSnapshot {
        self.snapshot
    }

    pub fn note_conclusion_stable(
        &mut self,
    ) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        self.advance(ConvergenceFact::ConclusionStable)
    }

    pub fn note_actual_stopped(
        &mut self,
    ) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        self.advance(ConvergenceFact::ActualStopped)
    }

    pub fn note_output_released(
        &mut self,
    ) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        self.advance(ConvergenceFact::OutputReleased)
    }

    pub fn note_resources_converged(
        &mut self,
    ) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        if self.snapshot.resources_converged {
            return self.advance(ConvergenceFact::ResourcesConverged);
        }
        if !self.snapshot.actual_stopped {
            return Err(TaskConvergenceRejection::ResourcesBeforeActualStop);
        }
        self.advance(ConvergenceFact::ResourcesConverged)
    }

    pub fn retire(&mut self) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        if self.snapshot.retired {
            return Err(TaskConvergenceRejection::AlreadyRetired);
        }
        if !self.snapshot.retirement_ready() {
            return Err(TaskConvergenceRejection::RetirementNotReady);
        }
        self.advance(ConvergenceFact::Retired)
    }

    fn advance(
        &mut self,
        fact: ConvergenceFact,
    ) -> Result<TaskConvergenceAdvance, TaskConvergenceRejection> {
        let current = match fact {
            ConvergenceFact::ConclusionStable => self.snapshot.conclusion_stable,
            ConvergenceFact::ActualStopped => self.snapshot.actual_stopped,
            ConvergenceFact::OutputReleased => self.snapshot.output_released,
            ConvergenceFact::ResourcesConverged => self.snapshot.resources_converged,
            ConvergenceFact::Retired => self.snapshot.retired,
        };
        if current {
            return Ok(TaskConvergenceAdvance::Idempotent(self.snapshot));
        }
        if self.snapshot.retired {
            return Err(TaskConvergenceRejection::AlreadyRetired);
        }
        let Some(version) = self.snapshot.version.checked_add(1) else {
            return Err(TaskConvergenceRejection::VersionExhausted);
        };
        self.snapshot.version = version;
        match fact {
            ConvergenceFact::ConclusionStable => self.snapshot.conclusion_stable = true,
            ConvergenceFact::ActualStopped => self.snapshot.actual_stopped = true,
            ConvergenceFact::OutputReleased => self.snapshot.output_released = true,
            ConvergenceFact::ResourcesConverged => self.snapshot.resources_converged = true,
            ConvergenceFact::Retired => self.snapshot.retired = true,
        }
        Ok(TaskConvergenceAdvance::Advanced(self.snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskConvergence, TaskConvergenceAdvance, TaskConvergenceRejection};

    #[test]
    fn a_stable_conclusion_does_not_claim_physical_convergence() {
        let mut convergence = TaskConvergence::new();
        let after_conclusion = convergence
            .note_conclusion_stable()
            .expect("a first conclusion is accepted")
            .snapshot();

        assert!(after_conclusion.conclusion_stable());
        assert!(!after_conclusion.actual_stopped());
        assert!(!after_conclusion.output_released());
        assert!(!after_conclusion.resources_converged());
        assert!(!after_conclusion.retirement_ready());
    }

    #[test]
    fn post_conclusion_facts_advance_independently_and_monotonically() {
        let mut convergence = TaskConvergence::new();
        convergence
            .note_conclusion_stable()
            .expect("conclusion is accepted");
        let stopped = convergence
            .note_actual_stopped()
            .expect("actual stop is accepted after the conclusion")
            .snapshot();
        let output = convergence
            .note_output_released()
            .expect("output release is independent")
            .snapshot();
        let resources = convergence
            .note_resources_converged()
            .expect("resource convergence follows actual stop")
            .snapshot();

        assert!(stopped.version() < output.version());
        assert!(output.version() < resources.version());
        assert!(resources.retirement_ready());
        assert!(!resources.retired());
    }

    #[test]
    fn resource_release_requires_positive_stop_evidence() {
        let mut convergence = TaskConvergence::new();
        convergence
            .note_conclusion_stable()
            .expect("conclusion is accepted");

        assert_eq!(
            convergence.note_resources_converged(),
            Err(TaskConvergenceRejection::ResourcesBeforeActualStop)
        );
        assert!(!convergence.snapshot().resources_converged());
    }

    #[test]
    fn retirement_is_a_distinct_irreversible_transition() {
        let mut convergence = TaskConvergence::new();
        convergence
            .note_actual_stopped()
            .expect("execution may stop before its conclusion is published");
        convergence
            .note_resources_converged()
            .expect("stopped execution may release runtime resources");
        convergence
            .note_output_released()
            .expect("output may independently drain");
        assert_eq!(
            convergence.retire(),
            Err(TaskConvergenceRejection::RetirementNotReady)
        );
        convergence
            .note_conclusion_stable()
            .expect("the stable conclusion can arrive last");

        let retired = convergence.retire().expect("all facts permit retirement");
        assert!(matches!(retired, TaskConvergenceAdvance::Advanced(_)));
        assert!(retired.snapshot().retired());
        assert!(matches!(
            convergence
                .note_output_released()
                .expect("duplicate retained evidence remains idempotent"),
            TaskConvergenceAdvance::Idempotent(_)
        ));
    }

    #[test]
    fn duplicate_positive_evidence_is_idempotent() {
        let mut convergence = TaskConvergence::new();
        let first = convergence
            .note_actual_stopped()
            .expect("first stop evidence is accepted");
        let replay = convergence
            .note_actual_stopped()
            .expect("duplicate evidence is accepted");

        assert!(matches!(first, TaskConvergenceAdvance::Advanced(_)));
        assert!(matches!(replay, TaskConvergenceAdvance::Idempotent(_)));
        assert_eq!(first.snapshot(), replay.snapshot());
    }
}
