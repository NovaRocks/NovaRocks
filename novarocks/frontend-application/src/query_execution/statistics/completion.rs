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

//! Statistics publication requires the original complete success-compatible Task set.

use novarocks_execution_contract::TaskIdentity;

/// Incremental observation of the frozen statistics Task set. The existing
/// success-compatible rule below remains the sole publication predicate.
#[derive(Debug)]
pub(crate) struct StatisticsProtocolTracker {
    provenance: crate::task_execution::protocol_facts::AttemptProtocolProvenance,
    revision: u64,
    facts: Box<[StatisticsTaskCompletionFact]>,
    nonterminal: usize,
    incompatible: usize,
    selected_failure: Option<novarocks_coordination_contract::SelectedTaskTermination>,
}
impl StatisticsProtocolTracker {
    pub(crate) fn from_initial(
        changes: &crate::task_execution::protocol_facts::AcceptedAttemptProtocolChanges,
    ) -> Result<Self, crate::task_execution::error::TaskExecutionError> {
        let provenance =
            crate::task_execution::protocol_facts::AttemptProtocolProvenance::from_initial(
                changes,
            )?;
        let facts: Box<[_]> = provenance
            .tasks()
            .iter()
            .map(|&identity| StatisticsTaskCompletionFact {
                identity,
                terminal: false,
                success_compatible_terminal: false,
            })
            .collect();
        let mut tracker = Self {
            nonterminal: facts.len(),
            incompatible: facts.len(),
            facts,
            provenance,
            revision: 0,
            selected_failure: None,
        };
        tracker.observe(changes)?;
        Ok(tracker)
    }
    pub(crate) fn observe(
        &mut self,
        changes: &crate::task_execution::protocol_facts::AcceptedAttemptProtocolChanges,
    ) -> Result<(), crate::task_execution::error::TaskExecutionError> {
        use crate::task_execution::error::TaskExecutionError;
        self.provenance.verify(changes)?;
        if changes.revision() < self.revision {
            return Err(TaskExecutionError::Schedule(
                "statistics protocol revision moved backwards".into(),
            ));
        }
        if changes.revision() == self.revision {
            return Ok(());
        }
        for change in changes.tasks() {
            let fact = self
                .facts
                .get_mut(change.index())
                .filter(|fact| fact.identity == change.identity())
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(
                        "statistics protocol delta names a foreign frozen slot".into(),
                    )
                })?;
            self.nonterminal -= usize::from(!fact.terminal);
            self.incompatible -= usize::from(!statistics_task_is_success_compatible(fact));
            fact.terminal = change
                .status()
                .is_some_and(|status| status.state().is_terminal());
            fact.success_compatible_terminal = change
                .status()
                .is_some_and(|status| status.is_success_compatible_terminal());
            self.nonterminal += usize::from(!fact.terminal);
            self.incompatible += usize::from(!statistics_task_is_success_compatible(fact));
        }
        self.selected_failure = changes.selected_failure().cloned();
        self.revision = changes.revision();
        Ok(())
    }
    pub(crate) const fn all_terminal(&self) -> bool {
        self.nonterminal == 0
    }
    pub(crate) fn all_success_compatible(&self) -> bool {
        self.incompatible == 0 && self.selected_failure.is_none()
    }
    pub(crate) fn selected_failure(
        &self,
    ) -> Option<&novarocks_coordination_contract::SelectedTaskTermination> {
        self.selected_failure.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StatisticsTaskCompletionFact {
    identity: TaskIdentity,
    terminal: bool,
    success_compatible_terminal: bool,
}

/// Whether this task's terminal is compatible with the already-complete Root
/// result remaining successful.
///
/// The Root reached `FINISHED` before this predicate can run. A non-root task
/// may then be stood down with `UPSTREAM_NO_LONGER_NEEDED`: the Root aggregate
/// has already consumed every sender through exchange EOS, so that terminal
/// closes work the result no longer depends on. An abort, a failure, a missing
/// status, or a task that has not reached a terminal still refuses statistics
/// publication.
fn statistics_task_is_success_compatible(fact: &StatisticsTaskCompletionFact) -> bool {
    fact.terminal && fact.success_compatible_terminal
}
