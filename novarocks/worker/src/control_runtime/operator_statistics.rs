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

//! One Task's prepaid periodic statistics producer. Samples replace one
//! pending snapshot; an in-flight input retains its own original occurrence.
//! Final observation seals this producer and captures its actual latest data.

use super::{
    actors::{ContextMessage, WorkerActor, WorkerMessage},
    admitted_role::adopt_reported_work,
};
use crate::{task_completion_roles::AdmittedCompletionRole, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, InputPhase, InputRole},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, ConsumerId, InputId, WorkId},
};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{FINAL_TASK_INFO_MAX_OPERATORS, TaskIdentity};
use novarocks_worker_protocol::context::{ContextInput, TerminalStatistics};
use std::sync::{Arc, Mutex};

pub(crate) const STATISTICS_INPUT_POSITIONS: usize = 1;

struct State {
    pending: Option<Arc<TerminalStatistics>>,
    latest: Option<Arc<TerminalStatistics>>,
    truncated: bool,
    sealed: bool,
}
struct Custody {
    task: TaskIdentity,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    role: ExitRole,
    report: ReportSlot,
    state: Mutex<State>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
}
struct Producer {
    custody: Arc<Custody>,
    authority: Option<ExitAuthority>,
}
/// Clones borrow the same actual producer. They reserve neither Work nor
/// another queue position, and their last exit remains an independent fact.
#[derive(Clone)]
pub struct TaskStatisticsProducer(Arc<Producer>);
/// One terminal owner may freeze this actual producer. Sampling clones hold
/// no finalization authority and cannot discard later required samples.
#[must_use = "Transfer the original statistics finalizer to the terminal owner"]
pub struct TaskStatisticsFinalizer {
    producer: TaskStatisticsProducer,
}
impl TaskStatisticsFinalizer {
    pub fn identity(&self) -> TaskIdentity {
        self.producer.identity()
    }
    pub fn seal_for_terminal(
        self,
        final_snapshot: Option<TerminalStatistics>,
    ) -> Option<TerminalStatistics> {
        self.producer.seal(final_snapshot)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsSubmission {
    Pending,
    Sealed,
}
fn normalize_statistics(mut snapshot: TerminalStatistics) -> TerminalStatistics {
    snapshot.truncated |= snapshot.statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS;
    snapshot.statistics.truncate(FINAL_TASK_INFO_MAX_OPERATORS);
    if snapshot.statistics.capacity() > FINAL_TASK_INFO_MAX_OPERATORS {
        // Truncating entries alone retains an arbitrarily large caller's
        // backing. Box conversion relinquishes excess capacity before the
        // snapshot enters this Task's fixed custody; no lock spans that Drop.
        snapshot.statistics = snapshot.statistics.into_boxed_slice().into_vec();
    }
    snapshot
}
impl TaskStatisticsProducer {
    pub fn identity(&self) -> TaskIdentity {
        self.0.custody.task
    }
    pub fn submit(&self, snapshot: TerminalStatistics) -> StatisticsSubmission {
        let mut snapshot = normalize_statistics(snapshot);
        let mut state = self
            .0
            .custody
            .state
            .lock()
            .expect("original Task statistics");
        if state.sealed {
            return StatisticsSubmission::Sealed;
        }
        state.truncated |= snapshot.truncated;
        snapshot.truncated = state.truncated;
        let snapshot = Arc::new(snapshot);
        let old_pending = state.pending.replace(snapshot.clone());
        let old_latest = state.latest.replace(snapshot);
        drop(state);
        drop((old_pending, old_latest));
        self.0.custody.pending.pending();
        StatisticsSubmission::Pending
    }
    /// This is the actual terminal producer boundary. An absent final profile
    /// preserves the latest real sample, including one not yet dispatched.
    /// No sample may overwrite this final observation after the seal.
    fn seal(&self, final_snapshot: Option<TerminalStatistics>) -> Option<TerminalStatistics> {
        let final_snapshot = final_snapshot.map(normalize_statistics);
        let (old_pending, latest, truncated) = {
            let mut state = self
                .0
                .custody
                .state
                .lock()
                .expect("original Task statistics");
            state.sealed = true;
            let old_pending = state.pending.take();
            (old_pending, state.latest.take(), state.truncated)
        };
        let mut actual = final_snapshot.or_else(|| latest.as_deref().cloned());
        if let Some(snapshot) = actual.as_mut() {
            snapshot.truncated |= truncated;
        }
        drop((old_pending, latest));
        self.0.custody.pending.pending();
        actual
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        if let Err(_error) = self
            .authority
            .take()
            .expect("one original statistics producer")
            .exited()
        {
            self.custody.health.report_unresolved(
                ExecutionExitOrigin::Task {
                    identity: self.custody.task,
                    phase: novarocks_execution_contract::TaskFailurePhase::Execution,
                },
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CausalInputUnresolved.reason(),
                ),
            );
        }
        self.custody.pending.pending();
    }
}
struct InFlight {
    input: InputId,
    consumption: Consumption,
}
#[derive(Debug)]
pub(crate) enum StatisticsError {
    Driver(DriverError),
    Runtime(RuntimeError),
    Role(ExitRoleError),
    MissingOccurrence,
    RefusedInput,
    Unresolved,
    OwnerFailed { input: InputId },
    FailurePublicationRequired,
}
impl From<DriverError> for StatisticsError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<RuntimeError> for StatisticsError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
#[derive(Default, Debug)]
pub(crate) struct StatisticsProgress {
    pub submitted: bool,
    pub consumed: bool,
    pub retired: bool,
}
pub(crate) struct StatisticsFailureProgress {
    pub units: usize,
    pub progressed: bool,
    pub retired: bool,
}
/// The actual Task owner retains this same fixed input and original report.
#[must_use = "Drain actual statistics input and producer exit before retiring this inventory"]
pub(crate) struct TaskStatisticsInbox {
    custody: Arc<Custody>,
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    work: WorkId,
    input: InputRole<WorkerActor>,
    in_flight: Option<InFlight>,
    rejected: Option<WorkerMessage>,
    input_closed: bool,
    closed: bool,
}
/// An original snapshot moved out of the pending slot for this owner turn.
/// Its physical-exit observation precedes that take and cannot be refreshed
/// against an older empty-slot observation after the last submit races it.
struct StatisticsTurn {
    snapshot: Option<Arc<TerminalStatistics>>,
    producer_exited: bool,
}
pub(crate) struct StatisticsBinding {
    pub task: TaskIdentity,
    pub runtime: ActorRuntime<WorkerActor>,
    pub actor: ActorId,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}
impl TaskStatisticsInbox {
    pub(crate) fn from_admitted(
        role: AdmittedCompletionRole,
        input: InputRole<WorkerActor>,
        binding: StatisticsBinding,
    ) -> (Self, TaskStatisticsProducer, TaskStatisticsFinalizer) {
        let scope = role.work.scope().clone();
        let work = role.work.id();
        let report = role.report.clone();
        let (role, authority) =
            ExitRole::from_admitted(adopt_reported_work(role.work, role.capacity, role.report));
        let custody = Arc::new(Custody {
            task: binding.task,
            scope,
            role,
            report,
            state: Mutex::new(State {
                pending: None,
                latest: None,
                truncated: false,
                sealed: false,
            }),
            pending: binding.pending,
            health: binding.health,
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("original statistics retention") = Some(custody.clone());
        let producer = TaskStatisticsProducer(Arc::new(Producer {
            custody: custody.clone(),
            authority: Some(authority),
        }));
        (
            Self {
                custody,
                runtime: binding.runtime,
                actor: binding.actor,
                work,
                input,
                in_flight: None,
                rejected: None,
                input_closed: false,
                closed: false,
            },
            producer.clone(),
            TaskStatisticsFinalizer { producer },
        )
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.closed
    }
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.custody.role.phase()
    }
    pub(crate) fn report(&self) -> &ReportSlot {
        &self.custody.report
    }
    pub(crate) fn advance(&mut self) -> Result<StatisticsProgress, StatisticsError> {
        if !self.closed && self.custody.scope.gates()?.failed {
            return Err(StatisticsError::FailurePublicationRequired);
        }
        let (progress, turn) = self.prepare_turn()?;
        match turn {
            Some(turn) => self.apply_turn(progress, turn),
            None => Ok(progress),
        }
    }
    /// Failure seals only this original producer's intake. Neutral samples
    /// can be discarded boundedly, while the last producer/capture exit and
    /// exact runtime input retirement remain independent mandatory gates.
    pub(crate) fn advance_failed(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        original_generation: novarocks_control_runtime_core::identity::Generation,
        budget: usize,
    ) -> Result<StatisticsFailureProgress, StatisticsError> {
        stage.validate_scope(&self.runtime, self.actor, &self.custody.scope)?;
        let mut result = StatisticsFailureProgress {
            units: 0,
            progressed: false,
            retired: self.closed,
        };
        if budget == 0 || self.closed {
            return Ok(result);
        }
        result.units = 1;
        let discarded = {
            let mut state = self
                .custody
                .state
                .lock()
                .expect("original failed statistics");
            result.progressed |= !state.sealed;
            state.sealed = true;
            (state.pending.take(), state.latest.take())
        };
        result.progressed |= discarded.0.is_some() || discarded.1.is_some();
        drop(discarded);
        if self.phase() == ExitRolePhase::Unresolved {
            return Err(StatisticsError::Unresolved);
        }
        // A refused WorkerMessage is bounded neutral statistics metadata, not
        // a source/callback whose disposal could authorize physical exit.
        let rejected = self.rejected.take();
        result.progressed |= rejected.is_some();
        drop(rejected);
        if let Some(flight) = self.in_flight.as_mut() {
            if flight.consumption == Consumption::Pending {
                flight.consumption = match self
                    .runtime
                    .input_custody(self.actor, flight.input)?
                    .ok_or(StatisticsError::MissingOccurrence)?
                    .phase
                {
                    InputPhase::Committed => Consumption::Consumed,
                    InputPhase::AbandonedByOwnerFailure => Consumption::AbandonedByOwnerFailure,
                    _ => Consumption::Pending,
                };
            }
            if flight.consumption != Consumption::Pending {
                match self.runtime.retire_input(self.actor, flight.input) {
                    Ok(()) => {
                        self.in_flight = None;
                        result.progressed = true;
                    }
                    Err(
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
                    ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let core = ConsumerId::new(1).unwrap();
        if self.custody.report.consumption(core)? == Consumption::Pending {
            self.custody.report.abandon_failed_owner(
                core,
                &self.custody.scope,
                original_generation,
            )?;
            result.progressed = true;
        }
        if self
            .custody
            .role
            .actual_exit_ready()
            .map_err(StatisticsError::Role)?
        {
            let observer = ConsumerId::new(2).unwrap();
            if self.custody.report.consumption(observer)? == Consumption::Pending {
                self.custody.report.consume(observer)?;
                result.progressed = true;
            }
            if self.in_flight.is_none() && !self.input_closed {
                self.input.close_after_exit()?;
                self.input_closed = true;
                result.progressed = true;
            }
            if self.input_closed
                && self
                    .custody
                    .role
                    .try_retire()
                    .map_err(StatisticsError::Role)?
            {
                self.closed = true;
                let retained = self
                    .custody
                    .retained
                    .lock()
                    .expect("original statistics retention")
                    .take();
                drop(retained);
                result.progressed = true;
                result.retired = true;
            }
        }
        Ok(result)
    }
    fn prepare_turn(
        &mut self,
    ) -> Result<(StatisticsProgress, Option<StatisticsTurn>), StatisticsError> {
        if self.closed {
            return Ok((
                StatisticsProgress {
                    retired: true,
                    ..Default::default()
                },
                None,
            ));
        }
        // An exact physical observation is independent of a refused or still
        // pending Core input. Failure cannot suppress this report leg.
        let producer_exited = self
            .custody
            .role
            .actual_exit_ready()
            .map_err(StatisticsError::Role)?;
        let observer = ConsumerId::new(2).expect("original physical observer");
        if producer_exited && self.custody.report.consumption(observer)? == Consumption::Pending {
            self.custody.report.consume(observer)?;
        }
        if self.phase() == ExitRolePhase::Unresolved {
            return Err(StatisticsError::Unresolved);
        }
        if self.rejected.is_some() {
            return Err(StatisticsError::RefusedInput);
        }
        let mut progress = StatisticsProgress::default();
        if let Some(flight) = self.in_flight.as_mut() {
            if flight.consumption == Consumption::Pending {
                let phase = self
                    .runtime
                    .input_custody(self.actor, flight.input)?
                    .ok_or(StatisticsError::MissingOccurrence)?
                    .phase;
                if phase == InputPhase::AbandonedByOwnerFailure {
                    return Err(StatisticsError::OwnerFailed {
                        input: flight.input,
                    });
                }
                if phase != InputPhase::Committed {
                    return Ok((progress, None));
                }
                flight.consumption = Consumption::Consumed;
            }
            match self.runtime.retire_input(self.actor, flight.input) {
                Ok(()) => {
                    self.in_flight = None;
                    progress.consumed = true;
                }
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok((progress, None)),
                Err(error) => return Err(error.into()),
            }
        }
        let snapshot = self
            .custody
            .state
            .lock()
            .expect("original Task statistics")
            .pending
            .take();
        Ok((
            progress,
            Some(StatisticsTurn {
                snapshot,
                producer_exited,
            }),
        ))
    }
    fn apply_turn(
        &mut self,
        mut progress: StatisticsProgress,
        turn: StatisticsTurn,
    ) -> Result<StatisticsProgress, StatisticsError> {
        let StatisticsTurn {
            snapshot,
            producer_exited,
        } = turn;
        if let Some(snapshot) = snapshot {
            let input = WorkerMessage::Context(ContextMessage::Command {
                work: self.work,
                request: None,
                command: ContextInput::OperatorStatistics {
                    task: self.custody.task,
                    statistics: snapshot.statistics.clone(),
                    truncated: snapshot.truncated,
                },
            });
            match self.input.fill(input) {
                Ok(input) => {
                    self.in_flight = Some(InFlight {
                        input,
                        consumption: Consumption::Pending,
                    });
                    progress.submitted = true;
                }
                Err((error, original)) => {
                    self.rejected = Some(original);
                    return Err(error.into());
                }
            }
        }
        // This exact exit was observed before taking pending. Observing a
        // later exit here could race the last producer submitting a sample
        // after that take and incorrectly retire an unconsumed snapshot.
        if self.in_flight.is_none() && producer_exited {
            let latest = self
                .custody
                .state
                .lock()
                .expect("original Task statistics")
                .latest
                .take();
            drop(latest);
            // Last actual producer exit prevents any new snapshot, so an empty
            // pending slot and drained original input prove the Core leg.
            for consumer in [1, 2] {
                let consumer = ConsumerId::new(consumer).expect("original report consumer");
                if self.custody.report.consumption(consumer)? == Consumption::Pending {
                    self.custody.report.consume(consumer)?;
                }
            }
            if !self.input_closed {
                self.input.close_after_exit()?;
                self.input_closed = true;
            }
            if self
                .custody
                .role
                .try_retire()
                .map_err(StatisticsError::Role)?
            {
                self.closed = true;
                drop(
                    self.custody
                        .retained
                        .lock()
                        .expect("original statistics retention")
                        .take(),
                );
                progress.retired = true;
            }
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests;
