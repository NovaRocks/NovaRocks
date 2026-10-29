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

//! A host refusal can retain the original creation input in its rich source.
//! Its original preparation child therefore accompanies the returned error;
//! returning from the host does not prove that these captures have exited.

use super::preparation::PreparationParent;
use crate::{
    HostRejection, task_completion_roles::AdmittedCompletionRole, task_port::TaskPendingNotice,
};
use novarocks_control_runtime::{completion::ReportSlot, transaction::RuntimeError};
use novarocks_control_runtime_core::{
    completion::ConsumerKind, identity::ConsumerId, scope::WorkKind,
};
use novarocks_execution::runtime::{
    execution_health::{ExecutionExitOrigin, ExecutionHealth},
    fragment::ExecutionFailure,
};
use novarocks_execution_contract::{
    OperationDomainFailure, TaskFailureCategory, TaskFailurePhase, TaskIdentity,
    failure::{FailureClass, FailureFact},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

pub(crate) const HOST_REFUSAL_WORK_POSITIONS: usize = 1;
pub(crate) const HOST_REFUSAL_REPORT_POSITIONS: usize = 1;
pub(crate) const HOST_REFUSAL_CAPTURE_POSITIONS: usize = 2;
const HOST: ConsumerId = ConsumerId::new(2).unwrap();

/// Issued before invoking a host that may retain input in its returned error.
/// Drop before binding a refusal is a real unused-holder exit, not a failed
/// callback or a canceled preparation assumed to have released its captures.
pub(crate) struct RefusalPosition {
    role: Option<AdmittedCompletionRole>,
}
impl RefusalPosition {
    pub(crate) fn reserve(parent: &PreparationParent) -> Result<Self, RuntimeError> {
        let guard = parent.derive(WorkKind::Cleanup)?;
        let scope = guard.scope();
        let report = ReportSlot::reserve_next(
            scope,
            scope.gates()?.generation,
            guard.work_id(),
            1,
            [(HOST, ConsumerKind::Observer)],
        )?;
        let (work, capacity) = match guard.bind(WorkKind::Cleanup) {
            Ok(bound) => bound,
            Err(error) => {
                report.revoke_unused()?;
                report.body_exited()?;
                report.captures_exited()?;
                assert!(report.try_retire()?);
                return Err(error);
            }
        };
        Ok(Self {
            role: Some(AdmittedCompletionRole {
                work,
                capacity,
                report,
            }),
        })
    }
    pub(crate) fn retain(
        mut self,
        rejection: HostRejection,
        task: TaskIdentity,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> HostPreparationRefusal {
        // Projection is a foreign boundary and runs before any custody lock.
        let neutral = rejection.operation_failure();
        let category = rejection.category();
        let closed_queue = rejection.is_closed_queue();
        let custody = Arc::new(RefusalCustody {
            task,
            pending,
            health,
            state: Mutex::new(State {
                rejection: Some(ManuallyDrop::new(rejection)),
                role: self.role.take(),
                panic: None,
            }),
            retained: Mutex::new(None),
        });
        *custody.retained.lock().expect("host refusal retention") = Some(custody.clone());
        HostPreparationRefusal {
            neutral,
            category,
            closed_queue,
            custody,
            active: true,
        }
    }
}
fn exit_role(mut role: AdmittedCompletionRole) {
    role.work
        .body_exited_with_report(&role.report)
        .expect("original host refusal body exit");
    role.work
        .captures_exited()
        .expect("original host refusal captures exit");
    role.report
        .body_exited()
        .expect("host refusal report producer exit");
    role.report
        .captures_exited()
        .expect("host refusal report capture exit");
    role.report
        .consume(HOST)
        .expect("actual host disposal consumes its observer leg");
    assert!(
        role.report
            .try_retire()
            .expect("host refusal report retirement")
    );
    role.capacity
        .release(
            &role
                .capacity
                .resources()
                .expect("original zero-resource header"),
        )
        .expect("original host refusal header release");
}
impl Drop for RefusalPosition {
    fn drop(&mut self) {
        if let Some(role) = self.role.take() {
            exit_role(role);
        }
    }
}
struct State {
    rejection: Option<ManuallyDrop<HostRejection>>,
    role: Option<AdmittedCompletionRole>,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
struct RefusalCustody {
    task: TaskIdentity,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
impl RefusalCustody {
    fn unresolved(&self) {
        if let Some(role) = &self.state.lock().expect("host refusal custody").role {
            role.work.mark_unresolved();
        }
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.task,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
            ),
        );
        self.pending.pending();
    }
    fn release_retention(&self) {
        let retained = self.retained.lock().expect("host refusal retention").take();
        drop(retained);
    }
}

/// The source's canonical occurrence, including aliases made before attachment,
/// owns this one funded child. There is no strong edge back to that source from
/// RefusalCustody: its self-retention holds only the original role and Unknown.
struct RefusalSourceRetention {
    custody: Arc<RefusalCustody>,
    started: bool,
}
impl std::fmt::Debug for RefusalSourceRetention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefusalSourceRetention")
            .field("task", &self.custody.task)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}
impl novarocks_execution_contract::SourceRetentionCapability for RefusalSourceRetention {}

/// Finish only the original source holder after its real canonical last Drop.
/// Keeping this borrowed preserves the complete original role on every error.
fn finish_retained_role(role: &mut AdmittedCompletionRole) -> Result<bool, RuntimeError> {
    role.work.body_exited_with_report(&role.report)?;
    role.work.captures_exited()?;
    role.report.body_exited()?;
    role.report.captures_exited()?;
    role.report.consume(HOST)?;
    if !role.report.try_retire()? {
        return Ok(false);
    }
    let resources = role.capacity.resources()?;
    role.capacity.release(&resources)?;
    Ok(true)
}
impl Drop for RefusalSourceRetention {
    fn drop(&mut self) {
        if !self.started || std::thread::panicking() {
            // A source destructor can unwind before releasing this marker.
            // A later normal alias must never turn that Unknown into exit.
            self.custody.unresolved();
            return;
        }
        let mut role = {
            let mut state = self.custody.state.lock().expect("canonical refusal role");
            state
                .role
                .take()
                .expect("one original canonical source role")
        };
        // No custody/publication lock surrounds source exit or raw panic
        // custody. Common errors also preserve this exact role, not a retry.
        let exited = catch_unwind(AssertUnwindSafe(|| finish_retained_role(&mut role)));
        match exited {
            Ok(Ok(true)) => {
                drop(role);
                self.custody.pending.pending();
                self.custody.release_retention();
            }
            result => {
                let mut state = self
                    .custody
                    .state
                    .lock()
                    .expect("canonical refusal Unknown");
                state.role = Some(role);
                if let Err(panic) = result {
                    state.panic = Some(ManuallyDrop::new(panic));
                }
                drop(state);
                self.custody.unresolved();
            }
        }
    }
}

#[must_use = "The original refusal source retains preparation work until transfer or actual disposal"]
pub(crate) struct HostPreparationRefusal {
    neutral: OperationDomainFailure,
    category: TaskFailureCategory,
    closed_queue: bool,
    custody: Arc<RefusalCustody>,
    active: bool,
}
impl HostPreparationRefusal {
    pub(crate) fn neutral(&self) -> &OperationDomainFailure {
        &self.neutral
    }
    pub(crate) fn category(&self) -> TaskFailureCategory {
        self.category
    }
    pub(crate) fn is_closed_queue(&self) -> bool {
        self.closed_queue
    }
    /// Move the original rich source into its canonical source node, together
    /// with this exact already paid preparation child. No second error clone
    /// is retained by the Task. Aliases of an inner shared cause keep the same
    /// marker until that canonical cause's actual last destructor returns.
    ///
    /// A start refusal or unwinding source remains Unknown in the original
    /// self-retained custody; dropping a marker cannot invent a positive exit.
    pub(crate) fn into_retained_source(mut self) -> ExecutionFailure {
        let (rejection, mut role) = {
            let mut state = self
                .custody
                .state
                .lock()
                .expect("original canonical refusal transfer");
            (
                state.rejection.take().expect("original refusal source"),
                state
                    .role
                    .take()
                    .expect("original refusal preparation child"),
            )
        };
        let start = catch_unwind(AssertUnwindSafe(|| role.work.start()));
        let started = matches!(&start, Ok(Ok(())));
        {
            let mut state = self
                .custody
                .state
                .lock()
                .expect("original canonical refusal role");
            state.role = Some(role);
            if let Err(panic) = start {
                state.panic = Some(ManuallyDrop::new(panic));
            }
        }
        self.active = false;
        if !started {
            self.custody.unresolved();
        }
        // Move and attach outside every metadata/custody lock. The shared
        // source author's attachment forwards through known canonical owners;
        // it neither allocates another Work nor manufactures a source ID.
        let source = ManuallyDrop::into_inner(rejection).into_failure();
        source.retain_source_lifetime(Box::new(RefusalSourceRetention {
            custody: self.custody.clone(),
            started,
        }));
        source
    }
    /// Transfers both original artifacts to the real source/disposal owner.
    /// The receiver must keep the work until its source capture actually exits;
    /// it cannot substitute a wire failure or release only the error carrier.
    /// This remains a preparation child: the live source owner must finalize
    /// its rich projections after executable/control users exit and before
    /// PreparationExited/RetireTask. Neutral terminal retention is independent.
    /// Keeping this source until Task retirement would create a wait cycle.
    pub(crate) fn into_source(mut self) -> (ExecutionFailure, AdmittedCompletionRole) {
        let (rejection, role) = {
            let mut state = self.custody.state.lock().expect("host refusal custody");
            (
                state.rejection.take().expect("original host refusal"),
                state.role.take().expect("original host refusal child work"),
            )
        };
        self.active = false;
        self.custody.release_retention();
        (ManuallyDrop::into_inner(rejection).into_failure(), role)
    }
    /// Only a real host disposal observes source exit. Any panic keeps the
    /// original work, prepaid header and opaque panic in bounded custody.
    pub(crate) fn dispose(mut self) -> bool {
        let (rejection, mut role) = {
            let mut state = self.custody.state.lock().expect("host refusal custody");
            (
                state
                    .rejection
                    .take()
                    .expect("one original host refusal disposal"),
                state.role.take().expect("original disposal work"),
            )
        };
        if role.work.start().is_err() {
            let mut state = self.custody.state.lock().expect("host refusal custody");
            state.rejection = Some(rejection);
            state.role = Some(role);
            drop(state);
            self.active = false;
            self.custody.unresolved();
            return false;
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            drop(ManuallyDrop::into_inner(rejection))
        }));
        self.active = false;
        match result {
            Ok(()) => {
                exit_role(role);
                self.custody.pending.pending();
                self.custody.release_retention();
                true
            }
            Err(payload) => {
                let mut state = self.custody.state.lock().expect("host refusal custody");
                state.panic = Some(ManuallyDrop::new(payload));
                state.role = Some(role);
                drop(state);
                self.custody.unresolved();
                false
            }
        }
    }
}
impl Drop for HostPreparationRefusal {
    fn drop(&mut self) {
        if self.active {
            self.custody.unresolved();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control_admission::Dimension,
        control_runtime::preparation::{
            PREPARATION_CORE_CONSUMER, PREPARATION_OBSERVER_CONSUMER, PreparationBinding,
            PreparationCustody, PreparationOutcome,
        },
        task_port::TaskStopSignal,
    };
    use novarocks_control_runtime::transaction::RuntimeLedger;
    use novarocks_control_runtime_core::{
        capacity::ResourceVector, identity::Generation, scope::WorkAdmission,
    };
    use novarocks_execution_contract::{
        CreationContent, FrozenBytes, TaskCreationInput, TaskFailureCategory,
        failure::FailureProjection,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Pending;
    impl TaskPendingNotice for Pending {
        fn pending(&self) {}
    }
    #[derive(Debug)]
    struct Input(Arc<AtomicUsize>);
    impl CreationContent for Input {
        fn encoded_len(&self) -> usize {
            1
        }
        fn retained_bytes(&self) -> usize {
            42
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[derive(Debug)]
    struct RefusedInput {
        _input: TaskCreationInput,
    }
    impl std::fmt::Display for RefusedInput {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("host refusal retains its original creation input")
        }
    }
    impl std::error::Error for RefusedInput {}
    impl FailureProjection for RefusedInput {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Invariant,
                crate::failure_reason::WorkerFailureReason::PreparationOwnerUnavailable.reason(),
            )
        }
    }
    #[test]
    fn returned_refusal_keeps_original_preparation_bytes_until_its_input_source_really_exits() {
        let generation = Generation::new(1).unwrap();
        let resources = ResourceVector::new([
            (Dimension::PreparingTasks.id(), 1),
            (Dimension::PreparingBytes.id(), 100),
        ])
        .unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 4, 1);
        let scope = ledger.open_scope_next(None, generation, 4, 4).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let report = || {
            ReportSlot::reserve_next(
                &scope,
                generation,
                work.id(),
                2,
                [
                    (
                        PREPARATION_CORE_CONSUMER,
                        ConsumerKind::Owner {
                            scope: scope.id(),
                            generation,
                        },
                    ),
                    (PREPARATION_OBSERVER_CONSUMER, ConsumerKind::Observer),
                ],
            )
            .unwrap()
        };
        let reports = [report(), report()];
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(9971, 9972), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let pending: Arc<dyn TaskPendingNotice> = Arc::new(Pending);
        let health = Arc::new(ExecutionHealth::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let input = TaskCreationInput::new(
            FrozenBytes::freeze(bytes::Bytes::new()),
            Box::new(Input(drops.clone())),
        );
        let (custody, job) = PreparationCustody::from_admitted(
            PreparationBinding {
                stop: TaskStopSignal::initial(task),
                pending: pending.clone(),
                health: health.clone(),
            },
            work,
            capacity,
            reports,
            input,
        );
        let refused = match job.run(|input, parent| {
            let original = RefusalPosition::reserve(&parent).unwrap();
            original.retain(
                HostRejection::from_domain(
                    TaskFailureCategory::Internal,
                    RefusedInput { _input: input },
                ),
                task,
                pending,
                health,
            )
        }) {
            PreparationOutcome::Returned(refused) => refused,
            _ => panic!("the host returned its actual rich refusal"),
        };
        custody
            .body_report()
            .consume(PREPARATION_CORE_CONSUMER)
            .unwrap();
        custody
            .body_report()
            .consume(PREPARATION_OBSERVER_CONSUMER)
            .unwrap();
        ledger.reap_scope_ready(scope.id(), 4).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(
            !custody.publish_whole_exit().unwrap(),
            "a returned error still holds the original input child"
        );
        assert!(custody.take_after_exit().unwrap().is_none());
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 1);
        assert_eq!(ledger.used(Dimension::PreparingBytes.id()), 100);
        assert!(refused.dispose());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        ledger.reap_scope_ready(scope.id(), 4).unwrap();
        assert!(custody.publish_whole_exit().unwrap());
        custody
            .exit_report()
            .consume(PREPARATION_CORE_CONSUMER)
            .unwrap();
        custody
            .exit_report()
            .consume(PREPARATION_OBSERVER_CONSUMER)
            .unwrap();
        let remainder = custody.take_after_exit().unwrap().unwrap();
        assert_eq!(ledger.used(Dimension::PreparingTasks.id()), 0);
        assert_eq!(ledger.used(Dimension::PreparingBytes.id()), 0);
        assert!(
            remainder.is_released(),
            "this bundle held only the P/count and original preparation bytes"
        );
        drop(remainder);
        ledger.reap_scope_ready(scope.id(), 4).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
