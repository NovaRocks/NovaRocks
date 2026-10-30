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

use novarocks_execution_contract::{TaskIdentity, TerminationDetail};

use novarocks_coordination_contract::SelectedTaskTermination;

/// Query coordination's first failure-conclusion latch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TerminationLatch {
    first: Option<SelectedTaskTermination>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LatchOutcome {
    Won,
    Lost(SelectedTaskTermination),
    Refined(SelectedTaskTermination),
}

impl TerminationLatch {
    pub const fn open() -> Self {
        Self { first: None }
    }

    pub const fn cause(&self) -> Option<&TerminationDetail> {
        match &self.first {
            Some(selected) => Some(selected.detail()),
            None => None,
        }
    }

    pub const fn selected(&self) -> Option<&SelectedTaskTermination> {
        self.first.as_ref()
    }

    #[cfg(test)]
    pub const fn is_latched(&self) -> bool {
        self.first.is_some()
    }

    pub fn latch(&mut self, identity: TaskIdentity, detail: TerminationDetail) -> LatchOutcome {
        let cause = SelectedTaskTermination::new(identity, detail);
        match &self.first {
            Some(existing) if existing.detail().is_derived() && !cause.detail().is_derived() => {
                let replaced = existing.clone();
                self.first = Some(cause);
                LatchOutcome::Refined(replaced)
            }
            Some(existing) => LatchOutcome::Lost(existing.clone()),
            None => {
                self.first = Some(cause);
                LatchOutcome::Won
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason};
    use novarocks_execution_contract::{AbortCause, SafeDetail, TaskFailure, TaskFailurePhase};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU16;

    #[test]
    fn original_task_and_cause_refine_together_and_later_observations_cannot_replace_them() {
        let execution =
            QueryExecutionId::new(QueryId::new(61, 19), AttemptId::new(1).unwrap()).unwrap();
        let backend = BackendProcessId::new_v7();
        let task = |id| {
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(id).unwrap(),
                backend,
            )
        };
        let peer = task(1);
        let original = task(2);
        let late = task(3);
        let failure = TerminationDetail::Failed(TaskFailure::new_in_phase(
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::truncating("original preparation cause"),
            TaskFailurePhase::Preparation,
        ));
        let mut latch = TerminationLatch::open();
        assert_eq!(
            latch.latch(peer, TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            LatchOutcome::Won
        );
        assert!(
            matches!(latch.latch(original, failure.clone()), LatchOutcome::Refined(replaced) if replaced.identity() == peer && replaced.detail().is_derived())
        );
        let selected = latch.selected().unwrap().clone();
        assert_eq!(selected.identity(), original);
        assert_eq!(selected.detail(), &failure);
        assert!(
            matches!(latch.latch(late, TerminationDetail::Aborted(AbortCause::QueryFailed)), LatchOutcome::Lost(held) if held == selected)
        );
        assert_eq!(latch.selected(), Some(&selected));
        let mut canceled = TerminationLatch::open();
        canceled.latch(peer, TerminationDetail::Aborted(AbortCause::QueryFailed));
        canceled.latch(original, failure);
        assert_eq!(canceled.selected().unwrap().identity(), peer);
        assert_eq!(
            canceled.cause(),
            Some(&TerminationDetail::Aborted(AbortCause::QueryFailed))
        );
    }
}
