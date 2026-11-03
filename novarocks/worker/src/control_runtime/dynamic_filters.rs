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

//! One prepaid coalescing RF source. Rich values have unique store owners;
//! only real publisher/host threads dispose them. Reporter clones issue no RF
//! capability, and a Participation-held feedback sink keeps only a weak writer.

mod read;
pub(crate) use read::TaskDynamicFilterLoan;

use super::{
    actors::{ParticipationMessage, WorkerActor, WorkerMessage},
    reported_work_admission::adopt_reported_work,
};
use crate::{task_completion_set::AdmittedCompletionTransfer, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, ActorTurnError, InputPhase, StandingInputObligation},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitObligation},
    read_resource::{ReadResourceError, ReadResourceOwner, ReadResourceStore},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, ConsumerId, Generation, InputId, ScopeId, SlotId, WorkId},
};
use novarocks_execution_contract::task_execution::domain::CodecOwnedContent;
use novarocks_execution_contract::{DomainVersion, TaskIdentity};
use novarocks_worker_protocol::creation::PayloadId;
use novarocks_worker_protocol::participation::ParticipationInput;
use std::{
    any::Any,
    mem::ManuallyDrop,
    sync::{Arc, Mutex, Weak},
};

// Coalesced candidate, published flight, and previous current row. A pinned
// previous current delays promotion; it never creates a history row. Taking
// a row transfers its original value to the one supervised physical job.
// Readers share these exact rows using their original admitted R authority.
pub(crate) const RF_FIXED_PAYLOAD_POSITIONS: usize = 3;
pub(crate) const RF_INPUT_POSITIONS: usize = 1;

pub trait TaskDynamicFilterCommittedNotice: Send {
    fn committed(self: Box<Self>, task: TaskIdentity, version: DomainVersion);
}
pub struct TaskDynamicFilterProducer(Arc<Publisher>);
#[derive(Clone)]
pub struct TaskDynamicFilterWriter(Weak<Publisher>);
struct Publisher {
    custody: Arc<Custody>,
}
impl TaskDynamicFilterProducer {
    pub fn identity(&self) -> TaskIdentity {
        self.0.custody.task
    }
    pub fn writer(&self) -> TaskDynamicFilterWriter {
        TaskDynamicFilterWriter(Arc::downgrade(&self.0))
    }
}
impl Drop for Publisher {
    fn drop(&mut self) {
        self.custody
            .state
            .lock()
            .expect("original RF source")
            .producer_exited = true;
        self.custody.pending.pending();
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFilterSubmission {
    Pending,
    Sealed,
    Busy,
    VersionExhausted,
    Unresolved,
}
struct OpaquePayload {
    value: ManuallyDrop<Arc<dyn CodecOwnedContent>>,
}
impl OpaquePayload {
    fn dispose(self) {
        drop(ManuallyDrop::into_inner(self.value));
    }
}
struct Snapshot {
    version: DomainVersion,
    domains: u32,
    row: ReadResourceOwner<OpaquePayload>,
    notice: Option<Box<dyn TaskDynamicFilterCommittedNotice>>,
}
struct Flight {
    input: InputId,
    snapshot: Snapshot,
    published: bool,
    consumption: Consumption,
    input_retired: bool,
}
struct State {
    pending_value: Option<Snapshot>,
    latest: Option<Snapshot>,
    flight: Option<Flight>,
    next_payload: u64,
    last_version: u64,
    producer_exited: bool,
    read_admission_closed: bool,
    sealed: bool,
    job_active: bool,
    rejected: Option<WorkerMessage>,
    unresolved: bool,
    failed_job: Option<ManuallyDrop<HostPayload>>,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
struct Custody {
    task: TaskIdentity,
    work: WorkId,
    scope: ScopeHandle,
    generation: Generation,
    exit_custody: ExitCustody,
    report: ReportSlot,
    authority: Mutex<Option<ExitObligation>>,
    store: ReadResourceStore<OpaquePayload>,
    state: Mutex<State>,
    pending: Arc<dyn TaskPendingNotice>,
    retained: Mutex<Option<Arc<Self>>>,
}
impl TaskDynamicFilterWriter {
    pub fn identity(&self) -> Option<TaskIdentity> {
        self.0.upgrade().map(|publisher| publisher.custody.task)
    }
    /// The caller is the actual RF publisher body. Refused/unadvertised values
    /// leave here on that same physical caller, never a control poller.
    pub fn submit(
        &self,
        version: DomainVersion,
        domains: u32,
        payload: Arc<dyn CodecOwnedContent>,
        notice: Option<Box<dyn TaskDynamicFilterCommittedNotice>>,
    ) -> DynamicFilterSubmission {
        // Keep the original publisher alive through every candidate/rejected
        // capture destructor. A panic is an unknown original call, not exit.
        let Some(publisher) = self.0.upgrade() else {
            return DynamicFilterSubmission::Sealed;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.submit_inner(version, domains, payload, notice)
        }));
        match result {
            Ok(outcome) => outcome,
            Err(panic) => {
                let mut state = publisher
                    .custody
                    .state
                    .lock()
                    .expect("original RF publisher panic");
                state.unresolved = true;
                state.panic = Some(ManuallyDrop::new(panic));
                drop(state);
                publisher.custody.pending.pending();
                DynamicFilterSubmission::Unresolved
            }
        }
    }
    fn submit_inner(
        &self,
        version: DomainVersion,
        domains: u32,
        payload: Arc<dyn CodecOwnedContent>,
        notice: Option<Box<dyn TaskDynamicFilterCommittedNotice>>,
    ) -> DynamicFilterSubmission {
        let Some(publisher) = self.0.upgrade() else {
            return DynamicFilterSubmission::Sealed;
        };
        let custody = &publisher.custody;
        if custody.exit_custody.phase()
            == novarocks_control_runtime::exit_custody::ExitCustodyPhase::Admitted
        {
            let mut authority = custody.authority.lock().expect("original RF authority");
            if custody.exit_custody.phase()
                == novarocks_control_runtime::exit_custody::ExitCustodyPhase::Admitted
                && authority
                    .as_mut()
                    .expect("original RF author")
                    .start_in_generation(custody.generation)
                    .is_err()
            {
                return DynamicFilterSubmission::Sealed;
            }
        }
        let gates = match custody.scope.gates() {
            Ok(gates) => gates,
            Err(_) => return DynamicFilterSubmission::Sealed,
        };
        let mut state = custody.state.lock().expect("original RF source");
        if state.sealed
            || state.producer_exited
            || state.unresolved
            || gates.failed
            || version.get() <= state.last_version
        {
            return DynamicFilterSubmission::Sealed;
        }

        // Detach the previous unexposed candidate before inserting another.
        // It has never been loaned, published or captured by a Core occurrence.
        let old = state.pending_value.take();
        drop(state);
        if let Some(old) = old
            && let Err(original) = dispose_unexposed(old)
        {
            let mut state = custody.state.lock().expect("original RF source");
            state.pending_value = Some(original);
            state.unresolved = true;
            return DynamicFilterSubmission::Sealed;
        }
        let mut state = custody.state.lock().expect("original RF source");
        // A concurrent writer may have filled this one slot while the previous
        // candidate was actually disposed. It owns that candidate; do not drop
        // or replace it under this metadata lock.
        if state.pending_value.is_some()
            || state.sealed
            || state.producer_exited
            || state.unresolved
        {
            return DynamicFilterSubmission::Busy;
        }
        let Some(next) = state.next_payload.checked_add(1) else {
            return DynamicFilterSubmission::VersionExhausted;
        };
        if version.get() <= state.last_version {
            return DynamicFilterSubmission::Busy;
        }
        let id = SlotId::new(next).expect("positive RF payload position");
        let value = OpaquePayload {
            value: ManuallyDrop::new(payload),
        };
        let row = match custody
            .store
            .insert(id, &custody.scope, custody.generation, value)
        {
            Ok(row) => row,
            Err((_error, original)) => {
                drop(state);
                original.dispose();
                return DynamicFilterSubmission::Busy;
            }
        };
        state.next_payload = next;
        state.last_version = version.get();
        state.pending_value = Some(Snapshot {
            version,
            domains,
            row,
            notice,
        });
        drop(state);
        custody.pending.pending();
        DynamicFilterSubmission::Pending
    }
}
fn dispose_unexposed(snapshot: Snapshot) -> Result<(), Snapshot> {
    let Snapshot {
        version,
        domains,
        row,
        notice,
    } = snapshot;
    if row.seal().is_err() {
        return Err(Snapshot {
            version,
            domains,
            row,
            notice,
        });
    }
    match row.try_take() {
        Ok(value) => {
            value.dispose();
            drop(notice);
            Ok(())
        }
        Err((_, row)) => Err(Snapshot {
            version,
            domains,
            row,
            notice,
        }),
    }
}
#[derive(Debug)]
pub(crate) enum DynamicFilterError {
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    Role(ExitCustodyError),
    Resource(ReadResourceError),
    Unresolved,
    MissingInput,
    WrongPublication,
    WrongReadSelection,
    ReadAdmissionClosed,
    FailurePublicationRequired,
}
impl std::fmt::Display for DynamicFilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "dynamic filter runtime failed: {error}"),
            Self::ActorTurn(error) => write!(f, "dynamic filter control driver failed: {error}"),
            Self::Role(error) => write!(f, "dynamic filter role failed: {error}"),
            Self::Resource(error) => write!(f, "dynamic filter read resource failed: {error}"),
            Self::Unresolved => f.write_str("dynamic filter responsibility remains unresolved"),
            Self::MissingInput => f.write_str("dynamic filter publication input is missing"),
            Self::WrongPublication => {
                f.write_str("dynamic filter publication differs from its original binding")
            }
            Self::WrongReadSelection => {
                f.write_str("dynamic filter read selection differs from its original binding")
            }
            Self::ReadAdmissionClosed => f.write_str("dynamic filter read admission is closed"),
            Self::FailurePublicationRequired => {
                f.write_str("dynamic filter failure publication is required")
            }
        }
    }
}
impl std::error::Error for DynamicFilterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::Role(error) => Some(error),
            Self::Resource(error) => Some(error),
            Self::Unresolved
            | Self::MissingInput
            | Self::WrongPublication
            | Self::WrongReadSelection
            | Self::ReadAdmissionClosed
            | Self::FailurePublicationRequired => None,
        }
    }
}
impl From<RuntimeError> for DynamicFilterError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<ActorTurnError> for DynamicFilterError {
    fn from(e: ActorTurnError) -> Self {
        Self::ActorTurn(e)
    }
}
impl From<ExitCustodyError> for DynamicFilterError {
    fn from(e: ExitCustodyError) -> Self {
        Self::Role(e)
    }
}
impl From<ReadResourceError> for DynamicFilterError {
    fn from(e: ReadResourceError) -> Self {
        Self::Resource(e)
    }
}
pub(crate) struct DynamicFilterBinding {
    pub task: TaskIdentity,
    pub scope: ScopeHandle,
    pub generation: Generation,
    pub runtime: ActorRuntime<WorkerActor>,
    pub actor: ActorId,
    pub pending: Arc<dyn TaskPendingNotice>,
}
pub(crate) struct TaskDynamicFilterInbox {
    custody: Arc<Custody>,
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    work: WorkId,
    input: StandingInputObligation<WorkerActor>,
    closed: bool,
    input_closed: bool,
}
pub(crate) struct DynamicFilterProgress {
    pub progressed: bool,
    pub job: Option<TaskDynamicFilterHostJob>,
}
impl TaskDynamicFilterInbox {
    pub(crate) fn from_admitted(
        completion_transfer: AdmittedCompletionTransfer,
        input: StandingInputObligation<WorkerActor>,
        binding: DynamicFilterBinding,
    ) -> (Self, TaskDynamicFilterProducer) {
        let work = completion_transfer.work.id();
        let report = completion_transfer.report.clone();
        let (exit_custody, authority) = ExitCustody::from_admitted(adopt_reported_work(
            completion_transfer.work,
            completion_transfer.capacity,
            completion_transfer.report,
        ));
        let custody = Arc::new(Custody {
            task: binding.task,
            work,
            scope: binding.scope.clone(),
            generation: binding.generation,
            exit_custody,
            report,
            authority: Mutex::new(Some(authority)),
            store: ReadResourceStore::new(binding.scope, RF_FIXED_PAYLOAD_POSITIONS),
            state: Mutex::new(State {
                pending_value: None,
                latest: None,
                flight: None,
                next_payload: 0,
                last_version: 0,
                producer_exited: false,
                read_admission_closed: false,
                sealed: false,
                job_active: false,
                rejected: None,
                unresolved: false,
                failed_job: None,
                panic: None,
            }),
            pending: binding.pending,
            retained: Mutex::new(None),
        });
        *custody.retained.lock().expect("original RF retention") = Some(custody.clone());
        let producer = TaskDynamicFilterProducer(Arc::new(Publisher {
            custody: custody.clone(),
        }));
        (
            Self {
                custody,
                runtime: binding.runtime,
                actor: binding.actor,
                work,
                input,
                closed: false,
                input_closed: false,
            },
            producer,
        )
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.closed
    }
    /// Called only after commit_task_projection succeeded for this exact
    /// original PublishTask effect, before its Task lane is released.
    pub(crate) fn publication_committed(
        &self,
        version: DomainVersion,
    ) -> Result<(), DynamicFilterError> {
        let mut state = self.custody.state.lock().expect("original RF source");
        if let Some(flight) = &mut state.flight
            && flight.snapshot.version == version
        {
            flight.published = true;
            drop(state);
            self.custody.pending.pending();
            return Ok(());
        }
        // Other Task publications legitimately repeat the already committed
        // advertisement; they mint no new receipt or callback invocation.
        if state
            .latest
            .as_ref()
            .is_some_and(|latest| latest.version == version)
        {
            return Ok(());
        }
        Err(DynamicFilterError::WrongPublication)
    }
    pub(crate) fn advance(&mut self) -> Result<DynamicFilterProgress, DynamicFilterError> {
        if !self.closed && self.custody.scope.gates()?.failed {
            return Err(DynamicFilterError::FailurePublicationRequired);
        }
        let mut result = DynamicFilterProgress {
            progressed: false,
            job: None,
        };
        if self.closed {
            return Ok(result);
        }
        let mut state = self.custody.state.lock().expect("original RF source");
        if state.unresolved || state.rejected.is_some() {
            return Err(DynamicFilterError::Unresolved);
        }
        if let Some(flight) = state.flight.as_mut() {
            if flight.consumption == Consumption::Pending {
                let phase = self
                    .runtime
                    .input_custody(self.actor, flight.input)?
                    .ok_or(DynamicFilterError::MissingInput)?
                    .phase;
                if phase == InputPhase::AbandonedByOwnerFailure {
                    return Err(DynamicFilterError::Unresolved);
                }
                if phase != InputPhase::Committed {
                    return Ok(result);
                }
                flight.consumption = Consumption::Consumed;
            }
            if !flight.input_retired {
                match self.runtime.retire_input(self.actor, flight.input) {
                    Ok(()) => {
                        flight.input_retired = true;
                        result.progressed = true;
                    }
                    Err(
                        ActorTurnError::InputStillQueued
                        | ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain,
                    ) => return Ok(result),
                    Err(error) => return Err(error.into()),
                }
            }
        }
        if state.job_active {
            return Ok(result);
        }
        if let Some(flight) = &state.flight {
            if flight.published {
                // Keep the completed flight in its own original cell while an
                // old read loan pins latest. Busy must not overwrite pending.
                if let Some(original) = state.latest.take() {
                    match self.take_disposal(&mut state, original) {
                        Ok(job) => {
                            result.job = Some(job);
                            result.progressed = true;
                            return Ok(result);
                        }
                        Err((error, original)) => {
                            state.latest = Some(original);
                            return match error {
                                DynamicFilterError::Resource(ReadResourceError::Busy) => Ok(result),
                                error => Err(error),
                            };
                        }
                    }
                }
                let flight = state.flight.take().expect("same original RF occurrence");
                state.latest = Some(flight.snapshot);
                result.progressed = true;
            } else {
                let flight = state.flight.take().expect("same original RF occurrence");
                let Flight {
                    input,
                    snapshot,
                    published,
                    consumption,
                    input_retired,
                } = flight;
                match self.take_disposal(&mut state, snapshot) {
                    Ok(job) => {
                        result.job = Some(job);
                        result.progressed = true;
                        return Ok(result);
                    }
                    Err((error, snapshot)) => {
                        state.flight = Some(Flight {
                            input,
                            snapshot,
                            published,
                            consumption,
                            input_retired,
                        });
                        return match error {
                            DynamicFilterError::Resource(ReadResourceError::Busy) => Ok(result),
                            error => Err(error),
                        };
                    }
                }
            }
        }
        // Producer exit closes writes, not the committed selector. Only the
        // original Task retirement lane (or actual failed-publication stage)
        // closes reads after previously accepted mechanical pins completed.
        if state.read_admission_closed {
            state.sealed = true;
            let pending = state.pending_value.is_some();
            if let Some(original) = if pending {
                state.pending_value.take()
            } else {
                state.latest.take()
            } {
                match self.take_disposal(&mut state, original) {
                    Ok(job) => {
                        result.job = Some(job);
                        result.progressed = true;
                    }
                    Err((error, original)) => {
                        if pending {
                            state.pending_value = Some(original);
                        } else {
                            state.latest = Some(original);
                        }
                        if !matches!(error, DynamicFilterError::Resource(ReadResourceError::Busy)) {
                            return Err(error);
                        }
                    }
                }
                return Ok(result);
            }
            if !state.producer_exited {
                return Ok(result);
            }
            drop(state);
            if let Some(authority) = self
                .custody
                .authority
                .lock()
                .expect("original RF authority")
                .take()
            {
                authority.exited()?;
                result.progressed = true;
            }
            if !self.input_closed {
                self.input.close_after_exit()?;
                self.input_closed = true;
                result.progressed = true;
            }
            for id in [1, 2] {
                let id = ConsumerId::new(id).expect("original RF report leg");
                if self.custody.report.consumption(id)? == Consumption::Pending {
                    self.custody.report.consume(id)?;
                    result.progressed = true;
                }
            }
            if self.custody.exit_custody.try_retire()? {
                self.closed = true;
                let retained = self
                    .custody
                    .retained
                    .lock()
                    .expect("original RF retention")
                    .take();
                drop(retained);
                result.progressed = true;
            }
            return Ok(result);
        }
        if let Some(latest) = &mut state.latest
            && let Some(notice) = latest.notice.take()
        {
            let version = latest.version;
            state.job_active = true;
            result.job = Some(TaskDynamicFilterHostJob {
                custody: self.custody.clone(),
                original: Some(ManuallyDrop::new(HostPayload::Notice { version, notice })),
            });
            result.progressed = true;
            return Ok(result);
        }
        if let Some(snapshot) = state.pending_value.take() {
            let command = WorkerMessage::Participation(ParticipationMessage::Command {
                work: novarocks_control_runtime_core::identity::ScopedWorkRef {
                    scope: self.custody.scope.id(),
                    work: self.work,
                },
                request: None,
                command: ParticipationInput::DynamicFilterPublished {
                    task: self.custody.task,
                    version: snapshot.version,
                    payload: PayloadId(snapshot.row.id().get()),
                    domain_count: snapshot.domains,
                },
            });
            match self.input.fill(command) {
                Ok(input) => {
                    state.flight = Some(Flight {
                        input,
                        snapshot,
                        published: false,
                        consumption: Consumption::Pending,
                        input_retired: false,
                    });
                    result.progressed = true;
                }
                Err((error, original)) => {
                    state.pending_value = Some(snapshot);
                    state.rejected = Some(original);
                    return Err(error.into());
                }
            }
        }
        Ok(result)
    }
    /// Only a real publication drain may stop this dead Core's producer. A
    /// host job still owns every rich destructor; failure never advertises a
    /// candidate, invents an empty payload or turns a held loan into exit.
    pub(crate) fn advance_failed(
        &mut self,
        stage: &super::failure::ScopePublicationsDrained<'_>,
        budget: usize,
    ) -> Result<DynamicFilterProgress, DynamicFilterError> {
        stage.validate_scope(&self.runtime, self.actor, &self.custody.scope)?;
        let mut result = DynamicFilterProgress {
            progressed: false,
            job: None,
        };
        if budget == 0 || self.closed {
            return Ok(result);
        }
        let mut state = self
            .custody
            .state
            .lock()
            .expect("original failed RF source");
        result.progressed |= !state.sealed || !state.read_admission_closed;
        state.sealed = true;
        state.read_admission_closed = true;
        if state.unresolved {
            return Err(DynamicFilterError::Unresolved);
        }
        // This body contains only the exact neutral Core input. Its rich row
        // remains in pending/flight/latest and is never dropped here.
        let rejected = state.rejected.take();
        result.progressed |= rejected.is_some();
        drop(state);
        drop(rejected);
        let core = ConsumerId::new(1).unwrap();
        if self.custody.report.consumption(core)? == Consumption::Pending {
            self.custody.report.abandon_failed_owner(
                core,
                &self.custody.scope,
                self.custody.generation,
            )?;
            result.progressed = true;
        }
        let mut state = self
            .custody
            .state
            .lock()
            .expect("original failed RF source");
        if let Some(flight) = state.flight.as_mut() {
            if flight.consumption == Consumption::Pending {
                flight.consumption = match self
                    .runtime
                    .input_custody(self.actor, flight.input)?
                    .ok_or(DynamicFilterError::MissingInput)?
                    .phase
                {
                    InputPhase::Committed => Consumption::Consumed,
                    InputPhase::AbandonedByOwnerFailure => Consumption::AbandonedByOwnerFailure,
                    _ => Consumption::Pending,
                };
            }
            if flight.consumption == Consumption::Pending {
                return Ok(result);
            }
            if !flight.input_retired {
                match self.runtime.retire_input(self.actor, flight.input) {
                    Ok(()) => {
                        flight.input_retired = true;
                        result.progressed = true;
                    }
                    Err(
                        ActorTurnError::InputStillQueued
                        | ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain,
                    ) => return Ok(result),
                    Err(error) => return Err(error.into()),
                }
            }
        }
        if state.job_active {
            return Ok(result);
        }
        if let Some(flight) = state.flight.take() {
            let Flight {
                input,
                snapshot,
                published,
                consumption,
                input_retired,
            } = flight;
            match self.take_disposal(&mut state, snapshot) {
                Ok(job) => {
                    result.job = Some(job);
                    result.progressed = true;
                }
                Err((error, snapshot)) => {
                    state.flight = Some(Flight {
                        input,
                        snapshot,
                        published,
                        consumption,
                        input_retired,
                    });
                    if !matches!(error, DynamicFilterError::Resource(ReadResourceError::Busy)) {
                        return Err(error);
                    }
                }
            }
            return Ok(result);
        }
        let pending = state.pending_value.is_some();
        if let Some(original) = if pending {
            state.pending_value.take()
        } else {
            state.latest.take()
        } {
            match self.take_disposal(&mut state, original) {
                Ok(job) => {
                    result.job = Some(job);
                    result.progressed = true;
                }
                Err((error, original)) => {
                    if pending {
                        state.pending_value = Some(original);
                    } else {
                        state.latest = Some(original);
                    }
                    if !matches!(error, DynamicFilterError::Resource(ReadResourceError::Busy)) {
                        return Err(error);
                    }
                }
            }
            return Ok(result);
        }
        if !state.producer_exited {
            return Ok(result);
        }
        drop(state);
        if let Some(authority) = self
            .custody
            .authority
            .lock()
            .expect("original RF authority")
            .take()
        {
            authority.exited()?;
            result.progressed = true;
        }
        if !self.input_closed {
            self.input.close_after_exit()?;
            self.input_closed = true;
            result.progressed = true;
        }
        if self.custody.exit_custody.actual_exit_ready()? {
            let observer = ConsumerId::new(2).unwrap();
            if self.custody.report.consumption(observer)? == Consumption::Pending {
                self.custody.report.consume(observer)?;
                result.progressed = true;
            }
        }
        if self.input_closed && self.custody.exit_custody.try_retire()? {
            self.closed = true;
            let retained = self
                .custody
                .retained
                .lock()
                .expect("original RF retention")
                .take();
            drop(retained);
            result.progressed = true;
        }
        Ok(result)
    }
    /// Refusal returns the whole same snapshot for restoration to its exact
    /// original cell. A loan's Busy cannot consume another pending candidate.
    fn take_disposal(
        &self,
        state: &mut State,
        original: Snapshot,
    ) -> Result<TaskDynamicFilterHostJob, (DynamicFilterError, Snapshot)> {
        if state.job_active {
            return Err((DynamicFilterError::Unresolved, original));
        }
        if let Err(error) = original.row.seal() {
            return Err((error.into(), original));
        }
        let Snapshot {
            version,
            domains,
            row,
            notice,
        } = original;
        let value = match row.try_take() {
            Ok(value) => value,
            Err((error, row)) => {
                return Err((
                    error.into(),
                    Snapshot {
                        version,
                        domains,
                        row,
                        notice,
                    },
                ));
            }
        };
        state.job_active = true;
        Ok(TaskDynamicFilterHostJob {
            custody: self.custody.clone(),
            original: Some(ManuallyDrop::new(HostPayload::Dispose { value, notice })),
        })
    }
}
enum HostPayload {
    Dispose {
        value: OpaquePayload,
        notice: Option<Box<dyn TaskDynamicFilterCommittedNotice>>,
    },
    Notice {
        version: DomainVersion,
        notice: Box<dyn TaskDynamicFilterCommittedNotice>,
    },
}
pub(crate) struct TaskDynamicFilterHostJob {
    custody: Arc<Custody>,
    original: Option<ManuallyDrop<HostPayload>>,
}
impl TaskDynamicFilterHostJob {
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.custody.scope.id(), self.custody.work)
    }
    pub(crate) fn run(mut self) {
        let original =
            ManuallyDrop::into_inner(self.original.take().expect("original RF host capture"));
        let task = self.custody.task;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match original {
            HostPayload::Dispose { value, notice } => {
                value.dispose();
                drop(notice);
            }
            HostPayload::Notice { version, notice } => notice.committed(task, version),
        }));
        let mut state = self.custody.state.lock().expect("original RF host exit");
        match outcome {
            Ok(()) => state.job_active = false,
            Err(payload) => {
                state.unresolved = true;
                state.panic = Some(ManuallyDrop::new(payload));
            }
        }
        drop(state);
        self.custody.pending.pending();
    }
}
impl Drop for TaskDynamicFilterHostJob {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            let mut state = self
                .custody
                .state
                .lock()
                .expect("original RF failed capture");
            state.unresolved = true;
            state.failed_job = Some(original);
            drop(state);
            self.custody.pending.pending();
        }
    }
}
