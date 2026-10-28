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

//! One elected loader borrows the Task-owned input outside all metadata locks.
//! Its actual work/report and the Task's retained input have different exits.

use crate::{task_completion_roles::AdmittedCompletionRole, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
};
use novarocks_control_runtime_core::identity::{ConsumerId, ScopeId, WorkId};
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::{
    CreateTask, TaskCreationInput, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use novarocks_worker_protocol::creation::{AdmissionRefusal, CreationCost, PayloadId};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

// Task lifetime, loader body, and its original input cleanup child. The two
// reliable reports belong to the loader and input holder respectively.
pub(crate) const LOADING_WORK_POSITIONS: usize = 3;
pub(crate) const LOADING_REPORT_POSITIONS: usize = 2;
type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

pub(crate) struct TaskCreatePayload {
    pub id: PayloadId,
    pub request: Arc<CreateTask>,
    pub input: TaskCreationInput,
    pub ingress_retained_bytes: usize,
}
/// The two original opaque carriers move together. Neither is implicitly
/// destroyed while a metadata holder is replaced or a call unwinds.
pub(crate) struct LoadingSources {
    request: ManuallyDrop<Arc<CreateTask>>,
    input: ManuallyDrop<TaskCreationInput>,
}
impl LoadingSources {
    pub(crate) fn new(request: Arc<CreateTask>, input: TaskCreationInput) -> Self {
        Self {
            request: ManuallyDrop::new(request),
            input: ManuallyDrop::new(input),
        }
    }
    pub(crate) fn into_parts(self) -> (Arc<CreateTask>, TaskCreationInput) {
        (
            ManuallyDrop::into_inner(self.request),
            ManuallyDrop::into_inner(self.input),
        )
    }
}
struct Custody {
    task: TaskIdentity,
    work: WorkId,
    scope: ScopeId,
    ingress_retained_bytes: usize,
    sources: Mutex<Option<LoadingSources>>,
    role: ExitRole,
    report: ReportSlot,
    input_role: ExitRole,
    input_report: ReportSlot,
    input_authority: Mutex<Option<ExitAuthority>>,
    canceled: AtomicBool,
    measured: AtomicBool,
    panic: Mutex<[Option<PanicPayload>; 3]>,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
pub(crate) struct LoaderCustody(Arc<Custody>);
#[must_use = "The original loader's body/captures must exit or remain unresolved"]
pub(crate) struct LoaderJob {
    custody: LoaderCustody,
    authority: Option<ExitAuthority>,
}
#[must_use = "The actual input destructor must run on a cleanup host worker"]
pub(crate) struct InputDisposalJob {
    custody: LoaderCustody,
    sources: Option<LoadingSources>,
    active: bool,
}
pub(crate) enum LoaderOutcome {
    Measured(Result<CreationCost, AdmissionRefusal>),
    Canceled,
    StartRefused(ExitRoleError),
    Unresolved,
}
impl LoaderCustody {
    pub(crate) fn from_admitted(
        payload: TaskCreatePayload,
        admitted: AdmittedCompletionRole,
        input_admitted: AdmittedCompletionRole,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, LoaderJob) {
        let work = admitted.work.id();
        let scope = admitted.work.scope().id();
        let report = admitted.report.clone();
        let (role, authority) = ExitRole::from_admitted(super::admitted_role::adopt_reported_work(
            admitted.work,
            admitted.capacity,
            admitted.report,
        ));
        let input_report = input_admitted.report.clone();
        let (input_role, input_authority) =
            ExitRole::from_admitted(super::admitted_role::adopt_reported_work(
                input_admitted.work,
                input_admitted.capacity,
                input_admitted.report,
            ));
        let custody = Arc::new(Custody {
            task: payload.request.identity(),
            work,
            scope,
            ingress_retained_bytes: payload.ingress_retained_bytes,
            sources: Mutex::new(Some(LoadingSources::new(payload.request, payload.input))),
            role,
            report,
            input_role,
            input_report,
            input_authority: Mutex::new(Some(input_authority)),
            canceled: AtomicBool::new(false),
            measured: AtomicBool::new(false),
            panic: Mutex::new([None, None, None]),
            pending,
            health,
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("loader original custody retention") = Some(custody.clone());
        let owner = Self(custody);
        let job = LoaderJob {
            custody: owner.clone(),
            authority: Some(authority),
        };
        (owner, job)
    }
    pub(crate) fn cancel(&self) {
        self.0.canceled.store(true, Ordering::Release);
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.0.task
    }
    pub(crate) fn work_id(&self) -> WorkId {
        self.0.work
    }
    pub(crate) fn scope_id(&self) -> ScopeId {
        self.0.scope
    }
    pub(crate) fn owns_job(&self, job: &LoaderJob) -> bool {
        Arc::ptr_eq(&self.0, &job.custody.0)
    }
    pub(crate) fn notify_pending(&self) {
        self.0.pending.pending();
    }
    pub(crate) fn report(&self) -> &ReportSlot {
        &self.0.report
    }
    pub(crate) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    pub(crate) fn is_retired(&self) -> bool {
        self.phase() == ExitRolePhase::Retired
            && self.0.input_role.phase() == ExitRolePhase::Retired
            && self
                .0
                .sources
                .lock()
                .expect("loader source transfer lock")
                .is_none()
    }
    pub(crate) fn ready_for_exit(&self) -> Result<bool, ExitRoleError> {
        self.0.role.actual_exit_ready()
    }
    fn take_sources(&self) -> Option<LoadingSources> {
        self.0
            .sources
            .lock()
            .expect("loader source transfer lock")
            .take()
    }
    pub(crate) fn take_measured_sources(&self) -> Option<LoadingSources> {
        if !self.0.measured.load(Ordering::Acquire)
            || !matches!(self.phase(), ExitRolePhase::Exited | ExitRolePhase::Retired)
        {
            return None;
        }
        self.take_sources()
    }
    pub(crate) fn restore_sources(&self, sources: LoadingSources) {
        let mut slot = self.0.sources.lock().expect("loader source transfer lock");
        assert!(slot.is_none(), "one original Task creation source holder");
        *slot = Some(sources);
    }
    /// The same input is already in the bound preparation job. Only now does
    /// its original loader source holder positively exit; no new quota is
    /// acquired, and the new preparation retains the actual graph.
    pub(crate) fn input_transferred(&self) {
        assert!(
            self.0
                .sources
                .lock()
                .expect("loader source transfer lock")
                .is_none()
        );
        self.finish_input_holder();
    }
    fn finish_input_holder(&self) {
        self.0
            .input_authority
            .lock()
            .expect("original input authority lock")
            .take()
            .expect("one original input holder")
            .exited()
            .expect("actual input holder exit");
        self.0
            .input_report
            .consume(ConsumerId::new(2).unwrap())
            .expect("actual host input-exit observation");
        assert!(
            self.0
                .input_role
                .try_retire()
                .expect("input exit report retirement")
        );
        self.0.pending.pending();
        self.release_retention_if_transferred();
    }
    /// A rejected/canceled Task keeps input custody until this separately paid
    /// cleanup body runs. Removing a queue entry is never its exit evidence.
    pub(crate) fn take_input_disposal(&self) -> Option<InputDisposalJob> {
        if !matches!(self.phase(), ExitRolePhase::Exited | ExitRolePhase::Retired) {
            return None;
        }
        self.take_sources().map(|sources| InputDisposalJob {
            custody: self.clone(),
            sources: Some(sources),
            active: true,
        })
    }
    pub(crate) fn try_retire(&self) -> Result<bool, ExitRoleError> {
        let retired = self.phase() == ExitRolePhase::Retired || self.0.role.try_retire()?;
        if retired {
            self.release_retention_if_transferred();
        }
        Ok(retired)
    }
    fn release_retention_if_transferred(&self) {
        if self.phase() == ExitRolePhase::Retired
            && self.0.input_role.phase() == ExitRolePhase::Retired
            && self
                .0
                .sources
                .lock()
                .expect("loader source transfer lock")
                .is_none()
        {
            let retained = self
                .0
                .retained
                .lock()
                .expect("loader original custody retention")
                .take();
            drop(retained);
        }
    }
    fn unresolved(&self, panic: Option<(usize, Box<dyn Any + Send>)>) {
        let (class, reason) = if panic.is_some() {
            (
                FailureClass::Unexpected,
                crate::failure_reason::WorkerFailureReason::PreparationPanicked,
            )
        } else {
            (
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved,
            )
        };
        if let Some((boundary, panic)) = panic {
            let mut slot = self.0.panic.lock().expect("loader panic custody lock");
            assert!(
                slot[boundary].is_none(),
                "one original panic per loader boundary"
            );
            slot[boundary] = Some(ManuallyDrop::new(panic));
        }
        self.0.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.0.task,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(class, reason.reason()),
        );
        self.0.pending.pending();
    }
}
impl LoaderJob {
    /// Actual host work. A getter can block, panic or reenter metadata; the
    /// input has been moved out of its loan lock before any foreign call.
    pub(crate) fn run(mut self) -> LoaderOutcome {
        let result = if self.custody.0.canceled.load(Ordering::Acquire) {
            Ok(LoaderOutcome::Canceled)
        } else {
            match self
                .authority
                .as_mut()
                .expect("original loader authority")
                .start()
            {
                Err(error) => Ok(LoaderOutcome::StartRefused(error)),
                Ok(()) if self.custody.0.canceled.load(Ordering::Acquire) => {
                    Ok(LoaderOutcome::Canceled)
                }
                Ok(()) => {
                    let sources = self
                        .custody
                        .take_sources()
                        .expect("one original loading source holder");
                    let result = catch_unwind(AssertUnwindSafe(|| {
                        crate::task_creation_cost::measure(
                            &sources.request,
                            &sources.input,
                            self.custody.0.ingress_retained_bytes,
                        )
                    }));
                    self.custody.restore_sources(sources);
                    result.map(|cost| {
                        self.custody.0.measured.store(true, Ordering::Release);
                        LoaderOutcome::Measured(cost)
                    })
                }
            }
        };
        match result {
            Ok(outcome) => {
                self.authority
                    .take()
                    .expect("original loader authority")
                    .exited()
                    .expect("true loader call and loan exit");
                self.custody.0.pending.pending();
                outcome
            }
            Err(panic) => {
                drop(self.authority.take());
                self.custody.unresolved(Some((0, panic)));
                LoaderOutcome::Unresolved
            }
        }
    }
}
impl InputDisposalJob {
    pub(crate) fn run(mut self) -> bool {
        self.custody
            .0
            .input_authority
            .lock()
            .expect("original input authority lock")
            .as_mut()
            .expect("unique original input authority")
            .start()
            .expect("prepaid input cleanup starts after stop");
        let (request, input) = self
            .sources
            .take()
            .expect("one original disposal source holder")
            .into_parts();
        // These are two independent foreign destruction boundaries. A panic
        // in the request must not make input Drop run during unwinding.
        let request_panic = catch_unwind(AssertUnwindSafe(|| drop(request))).err();
        let input_panic = catch_unwind(AssertUnwindSafe(|| drop(input))).err();
        let exited = request_panic.is_none() && input_panic.is_none();
        if exited {
            self.custody.finish_input_holder();
        } else {
            drop(
                self.custody
                    .0
                    .input_authority
                    .lock()
                    .expect("original input authority lock")
                    .take(),
            );
            if let Some(panic) = request_panic {
                self.custody.unresolved(Some((2, panic)));
            }
            if let Some(panic) = input_panic {
                self.custody.unresolved(Some((1, panic)));
            }
        }
        self.active = false;
        exited
    }
}
impl Drop for InputDisposalJob {
    fn drop(&mut self) {
        if self.active {
            if let Some(sources) = self.sources.take() {
                self.custody.restore_sources(sources);
            }
            drop(
                self.custody
                    .0
                    .input_authority
                    .lock()
                    .expect("original input authority lock")
                    .take(),
            );
            self.custody.unresolved(None);
        }
    }
}
impl Drop for LoaderJob {
    fn drop(&mut self) {
        if self.authority.is_some() {
            drop(self.authority.take());
            self.custody.unresolved(None);
        }
    }
}

#[cfg(test)]
mod tests;
