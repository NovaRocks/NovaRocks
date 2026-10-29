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
//! capability, and a Context-held feedback sink keeps only a weak writer.

use super::{
    actors::{ContextMessage, WorkerActor, WorkerMessage},
    admitted_role::adopt_reported_work,
};
use crate::{task_completion_roles::AdmittedCompletionRole, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, InputPhase, InputRole},
    exit_role::{ExitAuthority, ExitRole, ExitRoleError},
    read_resource::{ReadResourceError, ReadResourceOwner, ReadResourceStore},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, ConsumerId, Generation, InputId, ScopeId, SlotId, WorkId},
};
use novarocks_execution_contract::task_execution::domain::CodecOwnedContent;
use novarocks_execution_contract::{DomainVersion, TaskIdentity};
use novarocks_worker_protocol::context::ContextInput;
use novarocks_worker_protocol::creation::PayloadId;
use std::{
    any::Any,
    mem::ManuallyDrop,
    sync::{Arc, Mutex, Weak},
};

// Pending, in-flight/current, and one supervised physical disposal. Read loans
// are added only by the original R issuer; no unbounded per-Task history exists.
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
    version: DomainVersion,
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
}
struct State {
    pending_value: Option<Snapshot>,
    latest: Option<Snapshot>,
    flight: Option<Flight>,
    next_payload: u64,
    last_version: u64,
    producer_exited: bool,
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
    role: ExitRole,
    report: ReportSlot,
    authority: Mutex<Option<ExitAuthority>>,
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
        if custody.role.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Admitted {
            let mut authority = custody.authority.lock().expect("original RF authority");
            if custody.role.phase() == novarocks_control_runtime::exit_role::ExitRolePhase::Admitted
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
        if let Some(old) = old {
            if let Err(original) = dispose_unexposed(old) {
                let mut state = custody.state.lock().expect("original RF source");
                state.pending_value = Some(original);
                state.unresolved = true;
                return DynamicFilterSubmission::Sealed;
            }
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
            version,
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
    Driver(DriverError),
    Role(ExitRoleError),
    Resource(ReadResourceError),
    Unresolved,
    MissingInput,
    WrongPublication,
}
impl From<RuntimeError> for DynamicFilterError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}
impl From<DriverError> for DynamicFilterError {
    fn from(e: DriverError) -> Self {
        Self::Driver(e)
    }
}
impl From<ExitRoleError> for DynamicFilterError {
    fn from(e: ExitRoleError) -> Self {
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
    input: InputRole<WorkerActor>,
    closed: bool,
    input_closed: bool,
}
pub(crate) struct DynamicFilterProgress {
    pub progressed: bool,
    pub job: Option<TaskDynamicFilterHostJob>,
}
impl TaskDynamicFilterInbox {
    pub(crate) fn from_admitted(
        role: AdmittedCompletionRole,
        input: InputRole<WorkerActor>,
        binding: DynamicFilterBinding,
    ) -> (Self, TaskDynamicFilterProducer) {
        let work = role.work.id();
        let report = role.report.clone();
        let (role, authority) =
            ExitRole::from_admitted(adopt_reported_work(role.work, role.capacity, role.report));
        let custody = Arc::new(Custody {
            task: binding.task,
            work,
            scope: binding.scope.clone(),
            generation: binding.generation,
            role,
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
        if let Some(flight) = &mut state.flight {
            if flight.snapshot.version == version {
                flight.published = true;
                drop(state);
                self.custody.pending.pending();
                return Ok(());
            }
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
        if let Some(flight) = &state.flight {
            let input = self
                .runtime
                .input_custody(self.actor, flight.input)?
                .ok_or(DynamicFilterError::MissingInput)?;
            if input.phase == InputPhase::AbandonedByOwnerFailure {
                return Err(DynamicFilterError::Unresolved);
            }
            if input.phase != InputPhase::Committed {
                return Ok(result);
            }
            match self.runtime.retire_input(self.actor, flight.input) {
                Ok(()) => {}
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(result),
                Err(e) => return Err(e.into()),
            }
            let flight = state.flight.take().expect("same original RF occurrence");
            let dispose = if flight.published {
                state.latest.replace(flight.snapshot)
            } else {
                Some(flight.snapshot)
            };
            result.progressed = true;
            if let Some(original) = dispose {
                result.job = Some(self.take_disposal(&mut state, original)?);
                return Ok(result);
            }
        }
        if state.job_active {
            return Ok(result);
        }
        if state.producer_exited {
            state.sealed = true;
            if let Some(original) = state.pending_value.take().or_else(|| state.latest.take()) {
                result.job = Some(self.take_disposal(&mut state, original)?);
                result.progressed = true;
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
            if self.custody.role.try_retire()? {
                self.closed = true;
                self.custody
                    .retained
                    .lock()
                    .expect("original RF retention")
                    .take();
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
            let command = WorkerMessage::Context(ContextMessage::Command {
                work: self.work,
                request: None,
                command: ContextInput::DynamicFilterPublished {
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
    fn take_disposal(
        &self,
        state: &mut State,
        original: Snapshot,
    ) -> Result<TaskDynamicFilterHostJob, DynamicFilterError> {
        if state.job_active {
            state.unresolved = true;
            std::mem::forget(original);
            return Err(DynamicFilterError::Unresolved);
        }
        original.row.seal()?;
        let Snapshot {
            version,
            domains,
            row,
            notice,
        } = original;
        let value = match row.try_take() {
            Ok(value) => value,
            Err((error, row)) => {
                state.pending_value = Some(Snapshot {
                    version,
                    domains,
                    row,
                    notice,
                });
                return Err(error.into());
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
