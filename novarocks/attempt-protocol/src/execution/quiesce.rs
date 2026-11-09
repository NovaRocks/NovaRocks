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

//! Complete one immutable Quiesce fence before releasing its input barrier.

use super::QueryTaskExecution;
use crate::{capacity::StepContext, error::TaskExecutionError};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{ParticipationRef, QuiesceParticipationReceipt, TaskIdentity};
use std::{
    collections::BTreeSet,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
enum Source {
    Ack,
    Observed {
        gap_before: bool,
        deadline_before: Option<MonotonicInstant>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    CompareOwner(usize),
    CompareObservation(usize),
    Collect(usize),
    Verify(Option<TaskIdentity>),
    Apply(usize),
    Clear,
}

#[derive(Debug)]
pub(super) struct QuiescePass {
    participation: ParticipationRef,
    receipt: Arc<QuiesceParticipationReceipt>,
    source: Source,
    phase: Phase,
    accepted: BTreeSet<TaskIdentity>,
    error: Option<TaskExecutionError>,
}

impl QueryTaskExecution {
    pub(super) fn begin_ack_quiesce(
        &mut self,
        participation: ParticipationRef,
        receipt: Arc<QuiesceParticipationReceipt>,
        install: bool,
    ) -> Result<(), TaskExecutionError> {
        self.begin_quiesce(
            participation,
            receipt,
            Source::Ack,
            if install {
                Phase::CompareOwner(0)
            } else {
                Phase::Collect(0)
            },
        )
    }

    pub(super) fn begin_observed_quiesce(
        &mut self,
        participation: ParticipationRef,
        receipt: Arc<QuiesceParticipationReceipt>,
        gap_before: bool,
        deadline_before: Option<MonotonicInstant>,
    ) -> Result<(), TaskExecutionError> {
        if receipt.participation() != participation {
            return Err(TaskExecutionError::Schedule(
                "Quiesce observation names another participation".to_owned(),
            ));
        }
        self.begin_quiesce(
            participation,
            receipt,
            Source::Observed {
                gap_before,
                deadline_before,
            },
            Phase::CompareObservation(0),
        )
    }

    fn begin_quiesce(
        &mut self,
        participation: ParticipationRef,
        receipt: Arc<QuiesceParticipationReceipt>,
        source: Source,
        phase: Phase,
    ) -> Result<(), TaskExecutionError> {
        if self.quiesce_pass.is_some() {
            return Err(TaskExecutionError::Schedule(
                "Quiesce crossed its original input barrier".to_owned(),
            ));
        }
        self.quiesce_pass = Some(QuiescePass {
            participation,
            receipt,
            source,
            phase,
            accepted: BTreeSet::new(),
            error: None,
        });
        Ok(())
    }

    pub(crate) fn has_pending_quiesce(&self) -> bool {
        self.quiesce_pass.is_some()
    }

    /// None means that this same primary input still owns the continuation.
    pub(crate) fn advance_quiesce(
        &mut self,
        context: &mut dyn StepContext,
    ) -> Option<Result<(), TaskExecutionError>> {
        let mut pass = self
            .quiesce_pass
            .take()
            .expect("selected Quiesce continuation");
        while context.charge(1) {
            match self.advance_quiesce_row(&mut pass) {
                Ok(false) => {}
                Ok(true) => return Some(pass.error.map_or(Ok(()), Err)),
                Err(error) => {
                    pass.error = Some(error);
                    pass.phase = Phase::Clear;
                }
            }
        }
        self.quiesce_pass = Some(pass);
        None
    }

    fn advance_quiesce_row(&mut self, pass: &mut QuiescePass) -> Result<bool, TaskExecutionError> {
        let participation = pass.participation;
        match pass.phase {
            Phase::CompareOwner(index) => {
                let owner = self
                    .owners
                    .get_mut(&participation)
                    .expect("original participation");
                let changed = match pass.source {
                    Source::Ack => "a replayed Quiesce changed its cumulative accepted set",
                    Source::Observed { .. } => "observed Quiesce conflicts with the settled fence",
                };
                if compare_member(owner.quiesce_receipt(), &pass.receipt, index)
                    .map_err(|()| TaskExecutionError::DomainReceipt(changed.to_owned()))?
                {
                    if matches!(pass.source, Source::Observed { .. }) {
                        owner.verify_observed_quiesce()?;
                    }
                    owner.install_quiesce(Arc::clone(&pass.receipt));
                    match pass.source {
                        Source::Ack => pass.phase = Phase::Collect(0),
                        Source::Observed {
                            gap_before,
                            deadline_before,
                        } => {
                            self.covered_observation
                                .get_mut(&participation)
                                .expect("original participation")
                                .quiesce = Some(Arc::clone(&pass.receipt));
                            self.finish_covered_frame(participation, gap_before, deadline_before);
                            pass.phase = Phase::Clear;
                        }
                    }
                } else {
                    pass.phase = Phase::CompareOwner(index + 1);
                }
            }
            Phase::CompareObservation(index) => {
                let previous = self.covered_observation[&participation].quiesce.as_ref();
                if compare_member(previous, &pass.receipt, index).map_err(|()| {
                    TaskExecutionError::Schedule(
                        "Quiesce observation changed its accepted membership".to_owned(),
                    )
                })? {
                    pass.phase = Phase::Collect(0);
                } else {
                    pass.phase = Phase::CompareObservation(index + 1);
                }
            }
            Phase::Collect(index) => {
                if let Some(&identity) = pass.receipt.accepted_tasks().get(index) {
                    if !pass.accepted.insert(identity) {
                        return Err(TaskExecutionError::DomainReceipt(
                            "quiesce membership contains duplicate task identities".to_owned(),
                        ));
                    }
                    pass.phase = Phase::Collect(index + 1);
                } else {
                    pass.phase = Phase::Verify(None);
                }
            }
            Phase::Verify(after) => {
                let next = match after {
                    Some(after) => pass.accepted.range((Excluded(after), Unbounded)).next(),
                    None => pass.accepted.first(),
                };
                if let Some(&identity) = next {
                    let Some(task) = self.task_by_identity_ref(identity) else {
                        return Err(TaskExecutionError::DomainReceipt(format!(
                            "quiesce membership names an unscheduled task {identity}"
                        )));
                    };
                    if task.participation() != participation {
                        return Err(TaskExecutionError::DomainReceipt(format!(
                            "quiesce membership names task {identity} in another participation"
                        )));
                    }
                    pass.phase = Phase::Verify(Some(identity));
                } else {
                    pass.phase = Phase::Apply(0);
                }
            }
            Phase::Apply(index) => {
                let tasks = self
                    .tasks_by_participation
                    .get(&participation)
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                if let Some(&identity) = tasks.get(index) {
                    self.apply_quiesce_task(
                        participation,
                        identity,
                        pass.accepted.contains(&identity),
                    )?;
                    pass.phase = Phase::Apply(index + 1);
                } else {
                    pass.phase = match pass.source {
                        Source::Ack => Phase::Clear,
                        Source::Observed { .. } => Phase::CompareOwner(0),
                    };
                }
            }
            Phase::Clear => return Ok(pass.accepted.pop_first().is_none()),
        }
        Ok(false)
    }

    #[cfg(test)]
    pub(super) fn finish_quiesce_for_test(&mut self) -> Result<(), TaskExecutionError> {
        struct Unlimited;
        impl StepContext for Unlimited {
            fn charge(&mut self, _: usize) -> bool {
                true
            }
            fn reserve_queue_stage(
                &mut self,
                _: crate::config::DispatchLane,
                _: bool,
                _: novarocks_types::BackendProcessId,
                _: usize,
                _: usize,
            ) -> crate::capacity::QueueStage {
                panic!("Quiesce membership cannot reserve transport")
            }
        }
        if self.has_pending_quiesce() {
            self.advance_quiesce(&mut Unlimited)
                .expect("unlimited Quiesce fixture budget")
        } else {
            Ok(())
        }
    }
}

/// The original comparison is ordered and ignores only the later state.
fn compare_member(
    previous: Option<&Arc<QuiesceParticipationReceipt>>,
    current: &Arc<QuiesceParticipationReceipt>,
    index: usize,
) -> Result<bool, ()> {
    let Some(previous) = previous else {
        return Ok(true);
    };
    if Arc::ptr_eq(previous, current) {
        return Ok(true);
    }
    let left = previous.accepted_tasks();
    let right = current.accepted_tasks();
    if previous.fence_version() != current.fence_version()
        || left.len() != right.len()
        || left.get(index) != right.get(index)
    {
        return Err(());
    }
    Ok(index >= left.len())
}
