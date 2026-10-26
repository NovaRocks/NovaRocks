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

//! The host's actual elected Task owner. The Core alone decides creation and
//! lifecycle; this record holds its original reservations, published port and
//! one finite set of original fragment capabilities outside the actor.

use super::{
    actors::{WorkerAction, WorkerActor},
    causal::{CausalError, TaskCausalInbox},
    loading::{LoaderCustody, LoaderJob, LoadingSources, TaskCreatePayload},
    preparation::{
        PreparationAdoptionError, PreparationBinding, PreparationCustody, PreparationJob,
        PreparationOutcome, PreparationParent, QueuedPreparation, RunningPreparation,
    },
};
use crate::{
    settlement::RegistrationNotice,
    task_completion_roles::{AdmittedCompletionRole, TaskCompletionRoles},
    task_port::{TaskPendingNotice, TaskRuntimePort},
    task_proposal_ingress::TaskProposalIngress,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError, EffectTicket},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ActorId, ConsumerId, Generation, ReservationId},
    scope::WorkKind,
};
use novarocks_execution::runtime::{execution_health::ExecutionHealth, fragment::ExecutionFailure};
use novarocks_execution_contract::{AdmittedFragmentRoles, QueryContextRef, TaskIdentity};
use novarocks_worker_protocol::{context::ContextEffect, creation::PayloadId};
use std::sync::Arc;

pub(crate) struct TaskHostBinding {
    pub context: QueryContextRef,
    /// One host-issued source identity shared by this Task's causal authors.
    /// It is neither a WorkId cast nor one independent source per role.
    pub task_source: ActorId,
    pub protocol_generation: Generation,
    pub max_cuts: usize,
    pub pending: Arc<dyn TaskPendingNotice>,
    pub health: Arc<ExecutionHealth>,
}

#[derive(Debug)]
pub(crate) enum TaskHostError {
    WrongEffect,
    WrongTaskOrPayload,
    InvalidGeometry,
    DuplicatePreparation,
    LoaderAlreadyIssued,
    InputNotReady,
    CompletionBeforeAccepted,
    CompletionAlreadyIssued,
    Driver(DriverError),
    Runtime(RuntimeError),
    Preparation(PreparationAdoptionError),
}
impl From<RuntimeError> for TaskHostError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl std::fmt::Display for TaskHostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongEffect => {
                f.write_str("Task host adoption requires its original LoadInput effect")
            }
            Self::WrongTaskOrPayload => {
                f.write_str("Task host effect differs from the elected Task or payload")
            }
            Self::InvalidGeometry => f.write_str("Task host storage geometry overflowed"),
            Self::DuplicatePreparation => f.write_str("Task already owns its original preparation"),
            Self::LoaderAlreadyIssued => f.write_str("Task already issued its original loader"),
            Self::InputNotReady => {
                f.write_str("Task preparation requires its actual measured loading input")
            }
            Self::CompletionBeforeAccepted => {
                f.write_str("fragment completion roles require actual Accepted custody")
            }
            Self::CompletionAlreadyIssued => {
                f.write_str("original fragment completion roles were already issued")
            }
            Self::Driver(error) => write!(f, "Task host responsibility transfer failed: {error}"),
            Self::Runtime(error) => write!(f, "Task host work admission failed: {error}"),
            Self::Preparation(error) => write!(f, "Task host preparation transfer failed: {error}"),
        }
    }
}
impl std::error::Error for TaskHostError {}

/// This is the original resource inventory, not a copied obligation ledger.
/// Taking it moves custody to a host teardown job; it proves no physical exit.
pub(crate) struct TaskHostResources {
    pub lifetime: WorkLease,
    pub active: CapacityBundle,
    pub port: Arc<TaskRuntimePort>,
    pub loader: Option<LoaderCustody>,
    pub preparation: Option<PreparationCustody>,
    pub preparation_remainder: Option<CapacityBundle>,
    pub completion: Option<Arc<TaskCompletionRoles>>,
    pub causal: TaskCausalInbox,
}

/// The original request is a real preparation capture, not retained Task
/// metadata. Its last reference exits inside the same supervised host call
/// as the input, before P/bytes can be returned.
#[must_use = "The original preparation request and input require actual exit"]
pub(crate) struct TaskPreparationJob {
    job: Option<PreparationJob>,
    request: Option<Arc<novarocks_execution_contract::CreateTask>>,
}
impl TaskPreparationJob {
    pub(crate) fn run<T>(
        mut self,
        host: impl FnOnce(
            &novarocks_execution_contract::CreateTask,
            novarocks_execution_contract::TaskCreationInput,
            PreparationParent,
        ) -> T,
    ) -> PreparationOutcome<T> {
        let request = self.request.take().expect("one original Create carrier");
        self.job
            .take()
            .expect("one original preparation body")
            .run(move |input, parent| host(&request, input, parent))
    }
}

impl QueuedPreparation for TaskPreparationJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        self.job
            .as_ref()
            .expect("original preparation queue custody")
            .key()
    }
}
impl RunningPreparation<TaskPreparationJob> {
    pub(crate) fn run<T>(
        self,
        host: impl FnOnce(
            &novarocks_execution_contract::CreateTask,
            novarocks_execution_contract::TaskCreationInput,
            PreparationParent,
        ) -> T,
    ) -> PreparationOutcome<T> {
        self.run_body(|job| job.run(host))
    }
}

pub(crate) struct TaskHostRecord {
    task: TaskIdentity,
    context: QueryContextRef,
    payload: PayloadId,
    scope: ScopeHandle,
    // A cleanup producer may use a later failed scope epoch. Its consumer is
    // still the original Core, whose exact leg the supervisor abandons.
    core_generation: Generation,
    pending: Arc<dyn TaskPendingNotice>,
    health: Arc<ExecutionHealth>,
    resources: TaskHostResources,
    // One original fragment factory, including a failed partial admission.
    // Replaying a request or cloning the port never reissues these positions.
    completion_issued: bool,
    settlement_notice_issued: bool,
    loader_issued: bool,
}
impl TaskHostRecord {
    /// Electing the Task and paying its active/loading storage happened in
    /// the Core step. This takes the same guard once; it reserves no new quota.
    pub(crate) fn adopt_loading(
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
        binding: TaskHostBinding,
    ) -> Result<Self, TaskHostError> {
        let (task, payload, reservation) = match &ticket.effect {
            WorkerAction::Context(ContextEffect::LoadInput {
                task,
                payload,
                reservation,
            }) => (*task, *payload, *reservation),
            _ => return Err(TaskHostError::WrongEffect),
        };
        task.verify_query_context(binding.context)
            .map_err(|_| TaskHostError::WrongTaskOrPayload)?;
        let ingress = Arc::new(
            TaskProposalIngress::new(binding.max_cuts).ok_or(TaskHostError::InvalidGeometry)?,
        );
        let guard = runtime
            .take_step_reservation(ticket, reservation)
            .map_err(TaskHostError::Driver)?;
        let scope = guard.scope().clone();
        let (lifetime, active) = guard.bind(WorkKind::Business)?;
        let port = Arc::new(TaskRuntimePort::from_admitted(
            task,
            &scope,
            ingress.clone(),
            binding.pending.clone(),
            binding.health.clone(),
        ));
        Ok(Self {
            task,
            context: binding.context,
            payload,
            scope,
            core_generation: ticket.generation,
            pending: binding.pending,
            health: binding.health,
            resources: TaskHostResources {
                lifetime,
                active,
                port,
                loader: None,
                preparation: None,
                preparation_remainder: None,
                completion: None,
                causal: TaskCausalInbox::new(
                    runtime.clone(),
                    ticket.actor,
                    binding.task_source,
                    task,
                    binding.protocol_generation,
                    ingress,
                ),
            },
            completion_issued: false,
            settlement_notice_issued: false,
            loader_issued: false,
        })
    }
    pub(crate) fn port(&self) -> Arc<TaskRuntimePort> {
        self.resources.port.clone()
    }
    pub(crate) fn admit_settlement_notice(
        &mut self,
        consumers: u64,
    ) -> Result<
        (
            novarocks_worker_protocol::proposal_summary::CutId,
            Box<dyn RegistrationNotice>,
        ),
        CausalError,
    > {
        if self.resources.preparation.is_none() {
            return Err(CausalError::BeforeAccepted);
        }
        if self.settlement_notice_issued {
            return Err(CausalError::AlreadyAdmitted);
        }
        let admitted = self
            .resources
            .causal
            .admit_settlement(consumers, self.pending.clone())?;
        self.settlement_notice_issued = true;
        Ok(admitted)
    }
    pub(crate) fn causal(&mut self) -> &mut TaskCausalInbox {
        &mut self.resources.causal
    }
    pub(crate) fn begin_loader(
        &mut self,
        payload: TaskCreatePayload,
    ) -> Result<LoaderJob, (TaskHostError, TaskCreatePayload)> {
        if payload.id != self.payload
            || payload.request.identity() != self.task
            || payload.request.context() != self.context
        {
            return Err((TaskHostError::WrongTaskOrPayload, payload));
        }
        if self.loader_issued {
            return Err((TaskHostError::LoaderAlreadyIssued, payload));
        }
        self.loader_issued = true;
        let loader = match reserve_reported_role(
            &self.scope,
            self.resources.active.id(),
            WorkKind::Business,
            Some(self.core_generation),
        ) {
            Ok(loader) => loader,
            Err(error) => return Err((TaskHostError::Runtime(error), payload)),
        };
        // The input holder exists before the loader captures its graph. It is
        // a cleanup child, so a delayed destructor also keeps loader whole-exit
        // false; normal Accepted handoff positively exits this unused holder.
        let input =
            match reserve_reported_role(&self.scope, loader.capacity.id(), WorkKind::Cleanup, None)
            {
                Ok(input) => input,
                Err(error) => {
                    dispose_unused_completion(loader);
                    return Err((TaskHostError::Runtime(error), payload));
                }
            };
        let (custody, job) = LoaderCustody::from_admitted(
            payload,
            loader,
            input,
            self.pending.clone(),
            self.health.clone(),
        );
        self.resources.loader = Some(custody);
        Ok(job)
    }
    pub(crate) fn loader(&self) -> Option<&LoaderCustody> {
        self.resources.loader.as_ref()
    }
    pub(crate) fn adopt_preparation(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<TaskPreparationJob, TaskHostError> {
        if !matches!(&ticket.effect, WorkerAction::Context(ContextEffect::EnqueuePreparation { task, payload, .. }) if *task == self.task && *payload == self.payload)
        {
            return Err(TaskHostError::WrongTaskOrPayload);
        }
        if self.resources.preparation.is_some() {
            return Err(TaskHostError::DuplicatePreparation);
        }
        let loader = self
            .resources
            .loader
            .as_ref()
            .ok_or(TaskHostError::InputNotReady)?
            .clone();
        let (request, input) = loader
            .take_measured_sources()
            .ok_or(TaskHostError::InputNotReady)?
            .into_parts();
        let (owner, job) = match PreparationCustody::adopt(
            runtime,
            ticket,
            input,
            PreparationBinding {
                stop: self.resources.port.stop_signal(),
                pending: self.pending.clone(),
                health: self.health.clone(),
            },
        ) {
            Ok(admitted) => admitted,
            Err((error, input)) => {
                loader.restore_sources(LoadingSources::new(request, input));
                return Err(TaskHostError::Preparation(error));
            }
        };
        self.resources.preparation = Some(owner);
        let job = TaskPreparationJob {
            job: Some(job),
            request: Some(request),
        };
        loader.input_transferred();
        Ok(job)
    }
    pub(crate) fn preparation(&self) -> Option<&PreparationCustody> {
        self.resources.preparation.as_ref()
    }
    pub(crate) fn collect_preparation_remainder(&mut self) -> Result<bool, RuntimeError> {
        if self.resources.preparation_remainder.is_some() {
            return Ok(true);
        }
        let Some(preparation) = self.resources.preparation.as_ref() else {
            return Ok(false);
        };
        self.resources.preparation_remainder = preparation.take_after_exit()?;
        Ok(self.resources.preparation_remainder.is_some())
    }
    /// Called once before the dormant fragment captures any original callback.
    /// Each role consumes a header from the already paid Task storage; the
    /// zero-resource reservations add real child work, not duplicate quota.
    pub(crate) fn issue_completion_roles(
        &mut self,
        registration: Box<dyn RegistrationNotice>,
    ) -> Result<AdmittedFragmentRoles<ExecutionFailure>, (TaskHostError, Box<dyn RegistrationNotice>)>
    {
        if self.resources.preparation.is_none() {
            return Err((TaskHostError::CompletionBeforeAccepted, registration));
        }
        if self.completion_issued {
            return Err((TaskHostError::CompletionAlreadyIssued, registration));
        }
        self.completion_issued = true;
        let mut roles = Vec::<AdmittedCompletionRole>::with_capacity(4);
        for kind in [
            WorkKind::Cleanup,
            WorkKind::Cleanup,
            WorkKind::Cleanup,
            WorkKind::Settlement,
        ] {
            let parent = if kind == WorkKind::Settlement {
                roles[0].capacity.id()
            } else {
                self.resources.active.id()
            };
            match reserve_reported_role(&self.scope, parent, kind, Some(self.core_generation)) {
                Ok(role) => roles.push(role),
                Err(error) => {
                    while let Some(role) = roles.pop() {
                        dispose_unused_completion(role);
                    }
                    return Err((TaskHostError::Runtime(error), registration));
                }
            }
        }
        let child = roles.pop().expect("one original settlement child");
        let publisher = roles.pop().expect("one original terminal publisher");
        let wall = roles.pop().expect("one original wall-time finisher");
        let finisher = roles.pop().expect("one original fragment finisher");
        let (owner, admitted) = TaskCompletionRoles::from_admitted(
            self.task,
            [finisher, wall, publisher],
            child,
            registration,
            self.pending.clone(),
            self.health.clone(),
        );
        self.resources.completion = Some(Arc::new(owner));
        Ok(admitted)
    }
    pub(crate) fn completion(&self) -> Option<&Arc<TaskCompletionRoles>> {
        self.resources.completion.as_ref()
    }
    /// Host ownership transfer only. The receiving cleanup job must dispose
    /// opaque sources/captures and observe real body/children/report exits
    /// before releasing these bundles; Core terminal is no substitute.
    pub(crate) fn into_resources(self) -> TaskHostResources {
        self.resources
    }
}

fn reserve_reported_role(
    scope: &ScopeHandle,
    parent: ReservationId,
    kind: WorkKind,
    core_generation: Option<Generation>,
) -> Result<AdmittedCompletionRole, RuntimeError> {
    let generation = scope.gates()?.generation;
    let guard = scope.reserve_child_next(generation, parent, ResourceVector::default(), kind)?;
    let mut consumers = Vec::with_capacity(2);
    if let Some(core_generation) = core_generation {
        consumers.push((
            ConsumerId::new(1).unwrap(),
            ConsumerKind::Owner {
                scope: scope.id(),
                generation: core_generation,
            },
        ));
    }
    consumers.push((ConsumerId::new(2).unwrap(), ConsumerKind::Observer));
    let report = ReportSlot::reserve_next(scope, generation, guard.work_id(), 2, consumers)?;
    let (work, capacity) = match guard.bind(kind) {
        Ok(bound) => bound,
        Err(error) => {
            revoke_report(&report);
            return Err(error);
        }
    };
    Ok(AdmittedCompletionRole {
        work,
        capacity,
        report,
    })
}
fn revoke_report(report: &ReportSlot) {
    report.revoke_unused().expect("unbound original report");
    report.body_exited().expect("unbound report producer exit");
    report
        .captures_exited()
        .expect("unbound report has no captures");
    assert!(report.try_retire().expect("unbound report retirement"));
}
fn dispose_unused_completion(mut role: AdmittedCompletionRole) {
    // No capability/capture was handed to the fragment factory. This exact
    // unused holder exits now; shared bounded reaping handles its parent link.
    revoke_report(&role.report);
    role.work.body_exited().expect("unused original role body");
    role.work
        .captures_exited()
        .expect("unused original role captures");
    role.capacity
        .release(&ResourceVector::default())
        .expect("zero-resource original header release");
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_control_runtime::{exit_role::ExitRole, transaction::RuntimeLedger};
    use novarocks_control_runtime_core::{
        identity::{DimensionId, Generation},
        scope::WorkAdmission,
    };
    #[test]
    fn cleanup_role_admitted_after_failure_keeps_the_original_core_consumer_generation() {
        let generation = Generation::new(1).unwrap();
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 4, 1);
        let scope = ledger.open_scope_next(None, generation, 4, 4).unwrap();
        let (mut parent, mut capacity) = scope
            .reserve_next(
                generation,
                resources.clone(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let failed = scope.fail().unwrap();
        assert!(failed > generation);
        let role =
            reserve_reported_role(&scope, capacity.id(), WorkKind::Cleanup, Some(generation))
                .unwrap();
        let report = role.report.clone();
        report
            .abandon_failed_owner(ConsumerId::new(1).unwrap(), &scope, generation)
            .expect(
                "old Core consumer is abandoned, including cleanup headers issued after failure",
            );
        let (role, mut authority) = ExitRole::from_admitted(role.work, role.capacity, role.report);
        authority.start().unwrap();
        authority.exited().unwrap();
        assert!(
            !role.try_retire().unwrap(),
            "actual observer consumption remains necessary"
        );
        report.consume(ConsumerId::new(2).unwrap()).unwrap();
        assert!(role.try_retire().unwrap());
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        ledger.reap_scope_ready(scope.id(), 4).unwrap();
        capacity.release(&resources).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
