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

//! Actual domain R funding, physical jobs and original one-shot feedback.

use super::super::task_driver::{TaskDriverHostJob, TaskDriverPorts};
use super::domain::{
    ContextDomainAuthor, ContextDomainReceipt, DomainCustody, DomainHostBinding, DomainHostError,
    DomainReturn, DomainTaskTarget,
};
use super::*;
use crate::control_runtime::{
    admitted_role::{adopt_reported_work, reserve_reported_role},
    causal::CausalError,
    causal_input::CausalInputCustody,
    preparation::QueuedPreparation,
};
use crate::{TaskExecutionHost, task_completion_roles::AdmittedCompletionRole};
use novarocks_execution_contract::{AdvanceQueryContextDomain, UpdateTask};
use novarocks_worker_protocol::{context::ContextEffect, creation::PayloadId};
use std::sync::Mutex;

pub(super) struct DomainRequest {
    pub(super) custody: Option<DomainCustody>,
    funding: DomainFunding,
    projection: Arc<Mutex<Option<Result<ContextCommand, DomainHostError>>>>,
    pending: Option<TaskDriverHostJob>,
    ticket: Option<EffectTicket<WorkerAction>>,
    projected: bool,
    feedback_inputs: [Option<InputId>; 2],
    feedback_observer: Option<WorkRetirementObserver>,
    pub(super) retired: bool,
}
enum DomainFunding {
    Partials(Vec<ContextRequestPartial>),
    Task {
        pending: Option<Box<TaskRequestFunding>>,
        author: Option<DomainReturn>,
        custody: Option<CausalInputCustody>,
        outer: Option<ReservationBundle>,
        unused: Option<ContextRequestPartial>,
        direct: Option<Box<DirectReturn>>,
        unused_sources: Option<SourceSlotIssuePartial>,
    },
    Context {
        roles: Vec<DirectReturn>,
        unused: ContextRequestPartial,
        outer: Option<ReservationBundle>,
    },
}
struct DirectReturn {
    work: ScopedWorkRef,
    author: Option<DomainReturn>,
    receipt: ContextDomainReceipt,
    report: ReportSlot,
    observer: WorkRetirementObserver,
    permit: Option<InputPermit<WorkerActor>>,
    input: Option<InputId>,
    returned: Option<ContextInput>,
    core: bool,
    physical: bool,
    retired: bool,
    unused: bool,
}
#[must_use = "The original incoming R retains this projection until actual host return"]
pub(crate) struct DomainProjectionJob {
    custody: DomainCustody,
    result: Arc<Mutex<Option<Result<ContextCommand, DomainHostError>>>>,
    key: (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ),
    active: bool,
}
impl QueuedPreparation for DomainProjectionJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        self.key
    }
    fn fairness_group(
        &self,
        _: novarocks_control_runtime_core::identity::ScopeId,
    ) -> crate::control_runtime::preparation::HostFairnessGroup {
        crate::control_runtime::preparation::HostFairnessGroup::Context(self.custody.context())
    }
}
impl DomainProjectionJob {
    pub(crate) fn run_on_host(mut self) {
        let result = self.custody.project_on_host();
        *self
            .result
            .lock()
            .expect("original domain projection return") = Some(result);
        self.active = false;
        self.custody.notify();
    }
}
impl Drop for DomainProjectionJob {
    fn drop(&mut self) {
        if self.active {
            self.custody.projection_abandoned();
        }
    }
}

fn partial(ledger: &RuntimeLedger, original: AdmittedCompletionRole) -> ContextRequestPartial {
    let observer = original.work.retirement_observer();
    ContextRequestPartial {
        ledger: ledger.clone(),
        work: original.work,
        capacity: original.capacity,
        report: Some(original.report),
        permit: None,
        work_retirement: observer,
        report_revoked: false,
        exited: false,
        unresolved: false,
        outer: None,
        sources: None,
    }
}

impl DomainRequest {
    pub(super) fn is_uncaptured_unused(&self) -> bool {
        self.custody.is_none()
            && self.pending.is_none()
            && self.ticket.is_none()
            && self.projected
            && self.feedback_inputs.iter().all(Option::is_none)
            && self.feedback_observer.is_none()
    }
}

impl ContextHostRecord {
    fn retain_domain_partial(
        &mut self,
        work: ScopedWorkRef,
        error: ContextRequestError,
        partial: Option<ContextRequestPartial>,
    ) -> ContextRequestAdmissionRefusal {
        if let Some(partial) = partial {
            let request = self
                .resources
                .requests
                .get_mut(&work)
                .expect("same admitted domain R");
            let domain = request.domain.get_or_insert_with(|| DomainRequest {
                custody: None,
                funding: DomainFunding::Partials(Vec::new()),
                projection: Arc::new(Mutex::new(None)),
                pending: None,
                ticket: None,
                projected: true,
                feedback_inputs: [None, None],
                feedback_observer: None,
                retired: false,
            });
            let DomainFunding::Partials(originals) = &mut domain.funding else {
                std::mem::forget(partial);
                std::process::abort();
            };
            originals.push(partial);
            request.endpoint.external().notify();
        }
        ContextRequestAdmissionRefusal {
            error,
            partial: None,
        }
    }
    fn reserve_context_domain_funding(
        &mut self,
        request_work: ScopedWorkRef,
    ) -> Result<DomainFunding, ContextRequestAdmissionRefusal> {
        let scope = self.scope().clone();
        let generation = scope
            .gates()
            .map_err(|error| {
                self.retain_domain_partial(request_work, ContextRequestError::Runtime(error), None)
            })?
            .generation;
        let resources = StorageNeed::role(false).resources().map_err(|error| {
            self.retain_domain_partial(request_work, ContextRequestError::Policy(error), None)
        })?;
        let guard = scope
            .reserve_child_next(
                generation,
                self.resources.roles[super::super::LIFETIME].capacity.id(),
                resources,
                WorkKind::Settlement,
            )
            .map_err(|error| {
                self.retain_domain_partial(request_work, ContextRequestError::Runtime(error), None)
            })?;
        let (work, capacity) = guard
            .bind(WorkKind::Settlement)
            .unwrap_or_else(|_| std::process::abort());
        let observer = work.retirement_observer();
        let mut first = ContextRequestPartial {
            ledger: self.ledger.clone(),
            work,
            capacity,
            report: None,
            permit: None,
            work_retirement: observer,
            report_revoked: false,
            exited: false,
            unresolved: false,
            outer: None,
            sources: None,
        };
        let author = StorageNeed {
            work: 1,
            completions: 1,
            captures: 1,
            ..StorageNeed::default()
        };
        let outer = StorageNeed::role(false)
            .checked_sub(author)
            .and_then(StorageNeed::resources)
            .unwrap_or_else(|_| std::process::abort());
        first.outer = match first.capacity.split_next(&outer) {
            Ok(capacity) => Some(capacity),
            Err(error) => {
                return Err(self.retain_domain_partial(
                    request_work,
                    ContextRequestError::Runtime(error),
                    Some(first),
                ));
            }
        };
        first.report = match ReportSlot::reserve_next(
            &mut first.capacity,
            generation,
            2,
            [
                (
                    core_consumer(),
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: self.initial_generation,
                    },
                ),
                (external_consumer(), ConsumerKind::Observer),
            ],
        ) {
            Ok(report) => Some(report),
            Err(error) => {
                return Err(self.retain_domain_partial(
                    request_work,
                    ContextRequestError::Runtime(error),
                    Some(first),
                ));
            }
        };
        let mut originals = Vec::with_capacity(3);
        // Preserve the full remaining original before any later admission.
        let mut outer = first.outer.take();
        originals.push(first);
        for _ in 0..2 {
            match reserve_reported_role(
                &scope,
                outer
                    .as_mut()
                    .expect("original Context domain source inventory"),
                &self.resources.roles[super::super::LIFETIME].work,
                crate::control_runtime::source_positions::SOURCE_SLOT_STORAGE
                    .resources()
                    .expect("fixed original domain return role storage"),
                WorkKind::Settlement,
                Some((scope.id(), self.initial_generation)),
            ) {
                Ok(role) => originals.push(partial(&self.ledger, role)),
                Err(error) => {
                    originals[0].outer = outer;
                    for original in originals {
                        self.retain_domain_partial(
                            request_work,
                            ContextRequestError::Runtime(error),
                            Some(original),
                        );
                    }
                    return Err(ContextRequestAdmissionRefusal {
                        error: ContextRequestError::Runtime(error),
                        partial: None,
                    });
                }
            }
        }
        // Reserve both direct input positions before capture. Failure leaves
        // each actual original and the outer payment in the Context inventory.
        for index in 0..2 {
            let original = &mut originals[index];
            match self.control_runtime.reserve_admitted_input(
                self.actor,
                self.actor,
                InputClass::Lifecycle,
                &original.work,
            ) {
                Ok(permit) => {
                    // Bind only when the actual returned command fills this
                    // exact input. Untouched funding stays truly Reserved.
                    original.permit = Some(permit);
                }
                Err(error) => {
                    originals[0].outer = outer;
                    for original in originals {
                        self.retain_domain_partial(
                            request_work,
                            ContextRequestError::Driver(error),
                            Some(original),
                        );
                    }
                    return Err(ContextRequestAdmissionRefusal {
                        error: ContextRequestError::Driver(error),
                        partial: None,
                    });
                }
            }
        }
        let unused = originals.pop().expect("third original unused domain role");
        let roles =
            originals
                .into_iter()
                .map(|mut original| {
                    let work = original.work.reference();
                    let report = original.report.take().expect("original domain report");
                    let (receipt, author) = ContextDomainAuthor::from_reported(
                        adopt_reported_work(original.work, original.capacity, report.clone()),
                    )
                    .unwrap_or_else(|_| std::process::abort());
                    DirectReturn {
                        work,
                        author: Some(DomainReturn::Context(author)),
                        receipt,
                        report,
                        observer: original.work_retirement,
                        permit: original.permit,
                        input: None,
                        returned: None,
                        core: false,
                        physical: false,
                        retired: false,
                        unused: false,
                    }
                })
                .collect();
        Ok(DomainFunding::Context {
            roles,
            unused,
            outer,
        })
    }
    pub(crate) fn capture_task_domain(
        &mut self,
        work: ScopedWorkRef,
        original: UpdateTask,
        host: Arc<dyn TaskExecutionHost>,
        payload: PayloadId,
    ) -> Result<(), (ContextRequestError, UpdateTask)> {
        let task_id = original.identity();
        let Some(task) = self.task(task_id) else {
            return Err((ContextRequestError::UnknownTask, original));
        };
        let descriptor = task.production_domain_descriptor();
        let target = DomainTaskTarget {
            descriptor,
            port: task.port(),
            host,
        };
        let funding = match self.try_reserve_task_request_funding(false) {
            Ok(funding) => DomainFunding::Task {
                pending: Some(Box::new(funding)),
                author: None,
                custody: None,
                outer: None,
                unused: None,
                direct: None,
                unused_sources: None,
            },
            Err((error, partial)) => {
                self.retain_domain_partial(work, error, partial);
                return Err((ContextRequestError::SubmissionUnresolved, original));
            }
        };
        let binding = self.domain_binding(work, payload);
        let custody = match DomainCustody::capture_task(binding, original, target) {
            Ok(custody) => custody,
            Err((error, _, original, target)) => {
                // This prevalidated binding is a shared invariant failure.
                // Preserve all actual rich captures before fail-stop.
                std::mem::forget((error, target, funding, original));
                std::process::abort();
            }
        };
        self.task_driver
            .domain_requests
            .entry(task_id)
            .or_default()
            .insert(work);
        // Captured input stays in its original R, never reconstructed.
        self.install_domain(work, custody, funding)
            .unwrap_or_else(|_| std::process::abort());
        Ok(())
    }
    pub(crate) fn capture_context_domain(
        &mut self,
        work: ScopedWorkRef,
        original: AdvanceQueryContextDomain,
        payload: PayloadId,
    ) -> Result<(), (ContextRequestError, AdvanceQueryContextDomain)> {
        let funding = match self.reserve_context_domain_funding(work) {
            Ok(funding) => funding,
            Err(error) => return Err((error.error, original)),
        };
        let Some(jobs) = self.resources.host_jobs.as_ref() else {
            self.retain_unused_domain_funding(work, funding);
            return Err((ContextRequestError::WrongPhase, original));
        };
        let loan = match jobs.borrow_domain_instance() {
            Ok(loan) => loan,
            Err(_) => {
                self.retain_unused_domain_funding(work, funding);
                return Err((ContextRequestError::AdmissionClosed, original));
            }
        };
        let credential_resource = match &funding {
            DomainFunding::Context { roles, .. } => {
                roles
                    .first()
                    .expect("original Context domain feedback role")
                    .work
            }
            DomainFunding::Task { .. } | DomainFunding::Partials(_) => {
                unreachable!("original Context domain funding")
            }
        };
        let binding = self.domain_binding(work, payload);
        let custody =
            match DomainCustody::capture_context(binding, original, loan, credential_resource) {
                Ok(custody) => custody,
                Err((error, _, original, loan)) => {
                    std::mem::forget((error, loan, funding, original));
                    std::process::abort();
                }
            };
        self.install_domain(work, custody, funding)
            .unwrap_or_else(|_| std::process::abort());
        Ok(())
    }
    fn domain_binding(&self, work: ScopedWorkRef, payload: PayloadId) -> DomainHostBinding {
        let request = self
            .resources
            .requests
            .get(&work)
            .expect("same admitted domain R");
        DomainHostBinding {
            runtime: self.control_runtime.clone(),
            registration: self.registration().clone(),
            scope: self.scope().clone(),
            generation: self
                .scope()
                .gates()
                .unwrap_or_else(|_| std::process::abort())
                .generation,
            context: self.identity,
            work,
            credential_resource: None,
            operation: request.endpoint.metadata().operation,
            payload,
            ready: request.endpoint.external().slot().clone(),
        }
    }
    fn retain_unused_domain_funding(&mut self, work: ScopedWorkRef, funding: DomainFunding) {
        self.resources
            .requests
            .get_mut(&work)
            .expect("same original refused domain R")
            .domain = Some(DomainRequest {
            custody: None,
            funding,
            projection: Arc::new(Mutex::new(None)),
            pending: None,
            ticket: None,
            projected: true,
            feedback_inputs: [None, None],
            feedback_observer: None,
            retired: false,
        });
    }
    fn install_domain(
        &mut self,
        work: ScopedWorkRef,
        custody: DomainCustody,
        funding: DomainFunding,
    ) -> Result<(), ContextRequestError> {
        let request = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or(ContextRequestError::UnknownRequest)?;
        if request.domain.is_some() || request.submitted {
            return Err(ContextRequestError::WrongPhase);
        }
        // This is the actual incoming R's body, not another business grant.
        request
            .authority
            .as_mut()
            .expect("original domain incoming authority")
            .start()
            .map_err(ContextRequestError::Exit)?;
        request.started = true;
        let projection = Arc::new(Mutex::new(None));
        let job = DomainProjectionJob {
            custody: custody.clone(),
            result: projection.clone(),
            key: (work.scope, work.work),
            active: true,
        };
        request.domain = Some(DomainRequest {
            custody: Some(custody),
            funding,
            projection,
            pending: Some(TaskDriverHostJob::DomainProjection(job)),
            ticket: None,
            projected: false,
            feedback_inputs: [None, None],
            feedback_observer: None,
            retired: false,
        });
        request.endpoint.external().notify();
        Ok(())
    }
    pub(crate) fn advance_domain_request(
        &mut self,
        work: ScopedWorkRef,
        ports: &TaskDriverPorts,
    ) -> Result<(), ContextRequestError> {
        let Some(mut domain) = self
            .resources
            .requests
            .get_mut(&work)
            .and_then(|request| request.domain.take())
        else {
            return Ok(());
        };
        let result = self.advance_domain_inner(work, &mut domain, ports);
        self.resources
            .requests
            .get_mut(&work)
            .expect("same original domain R after bounded step")
            .domain = Some(domain);
        result
    }
    /// The real drained-stage transition queues only this Context's original
    /// Domain R addresses. It grants no input, author or physical exit.
    pub(in crate::control_runtime::context_host) fn notify_failed_domain_request(
        &self,
        work: ScopedWorkRef,
    ) {
        if let Some(original) = self.resources.requests.get(&work)
            && original.domain.is_some()
        {
            original.endpoint.external().notify();
        }
    }
    fn prepare_failed_domain(
        &mut self,
        work: ScopedWorkRef,
        domain: &mut DomainRequest,
    ) -> Result<bool, ContextRequestError> {
        let failed = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextRequestError::Driver)?
            .failed;
        let Some(custody) = domain.custody.as_ref().cloned() else {
            return Ok(false);
        };
        if !failed {
            if custody.awaiting_failure() {
                return Err(ContextRequestError::Domain(DomainHostError::Role(
                    custody
                        .start_refusal()
                        .ok_or(ContextRequestError::WrongPhase)?,
                )));
            }
            return Ok(false);
        }
        // A failed flag is not the publication-drain authority. The actual
        // transition performs one frozen, bounded exact-notice pass.
        if !self
            .resources
            .failure
            .as_ref()
            .is_some_and(|owner| owner.is_drained())
        {
            return Ok(true);
        }
        let owner = self
            .resources
            .failure
            .take()
            .expect("same original failed Domain owner");
        let result = (|| {
            let stage = owner.drained().map_err(|error| {
                ContextRequestError::Domain(match error {
                    super::super::failure::FailurePublicationError::Driver(error) => {
                        DomainHostError::Driver(error)
                    }
                    _ => DomainHostError::WrongPhase,
                })
            })?;
            stage
                .validate(&self.control_runtime, self.actor)
                .map_err(ContextRequestError::Driver)?;
            custody
                .abandon_failed_core(&stage)
                .map_err(ContextRequestError::Domain)?;
            self.resources
                .requests
                .get_mut(&work)
                .expect("same failed Domain R")
                .abandon_failed_domain_message(&custody, &stage)?;
            // No job may race an original queued/running projection or body.
            // Its actual return wakes this same R, which will revisit here.
            if domain.pending.is_none()
                && domain.projected
                && let Some(retained_author) = custody.failed_disposal_author()
            {
                let supplied = if retained_author {
                    None
                } else {
                    if !custody.request_bound() {
                        custody
                            .withdraw_before_request()
                            .map_err(ContextRequestError::Domain)?;
                    }
                    Some(self.take_domain_author(domain, None)?)
                };
                let job = match custody.take_failed_disposal_job(&stage, supplied) {
                    Ok(job) => job,
                    Err((error, original)) => {
                        if let Some(original) = original {
                            self.restore_domain_author(domain, original, None);
                        }
                        return Err(ContextRequestError::Domain(error));
                    }
                };
                let author = job.author_work();
                // A reused comparison author also withdraws its own return;
                // select the original receipt, not another unused role.
                match &mut domain.funding {
                    DomainFunding::Context { roles, .. } => {
                        let Some(original) = roles.iter_mut().find(|role| role.work == author)
                        else {
                            // The actual Job already owns rich captures. A
                            // broken trusted receipt binding must not Drop it
                            // on the poller while reporting an ordinary error.
                            std::mem::forget(job);
                            std::process::abort();
                        };
                        original.unused = true;
                    }
                    DomainFunding::Task {
                        direct: Some(original),
                        ..
                    } if original.work == author => {
                        original.unused = true;
                    }
                    _ => {}
                }
                domain.pending = Some(TaskDriverHostJob::Domain(job));
            }
            self.advance_failed_domain_feedback(domain, &custody, &stage)?;
            Ok(true)
        })();
        self.resources.failure = Some(owner);
        result
    }
    fn advance_failed_domain_feedback(
        &mut self,
        domain: &mut DomainRequest,
        custody: &DomainCustody,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<(), ContextRequestError> {
        stage
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextRequestError::Driver)?;
        match &mut domain.funding {
            DomainFunding::Context { roles, .. } => {
                for original in roles {
                    self.abandon_failed_direct_domain_return(original, custody, stage)?;
                }
            }
            DomainFunding::Task {
                direct: Some(original),
                ..
            } => {
                self.abandon_failed_direct_domain_return(original, custody, stage)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn abandon_failed_direct_domain_return(
        &mut self,
        original: &mut DirectReturn,
        custody: &DomainCustody,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<(), ContextRequestError> {
        if original.retired || original.unused {
            return Ok(());
        }
        if original.input.is_none() && original.returned.is_none() && original.author.is_none() {
            original.returned = original
                .receipt
                .reader
                .try_take()
                .map_err(ContextRequestError::Runtime)?;
        }
        let Some(command) = &original.returned else {
            return Ok(());
        };
        if !custody.matches_feedback(command) {
            return Err(ContextRequestError::WrongRequest);
        }
        if let Some(input) = original.input {
            let actual = self
                .control_runtime
                .input_custody(self.actor, input)
                .map_err(ContextRequestError::Driver)?
                .ok_or(ContextRequestError::MissingCoreReceipt)?;
            if actual.class != InputClass::Lifecycle
                || actual.source != self.actor
                || actual.role.is_some()
                || actual.runtime_metadata
                || !matches!(
                    actual.phase,
                    InputPhase::Bound | InputPhase::AbandonedByOwnerFailure
                )
            {
                return Err(ContextRequestError::WrongRequest);
            }
            self.control_runtime
                .abandon_failed_input(self.actor, input)
                .map_err(ContextRequestError::Driver)?;
            if self
                .control_runtime
                .input_custody(self.actor, input)
                .map_err(ContextRequestError::Driver)?
                .is_none_or(|actual| actual.phase != InputPhase::AbandonedByOwnerFailure)
            {
                return Err(ContextRequestError::MissingCoreReceipt);
            }
        } else {
            // A real Reserved input never transferred this taken neutral fact.
            // Revoke that exact permit; the stage abandons only its report leg.
            original
                .permit
                .take()
                .ok_or(ContextRequestError::WrongPhase)?
                .revoke_unused()
                .map_err(ContextRequestError::Driver)?;
            let proof = self
                .control_runtime
                .failure_proof_for(stage.registration())
                .map_err(ContextRequestError::Driver)?;
            original
                .report
                .bind_actor_consumer(&self.control_runtime, stage.registration(), core_consumer())
                .map_err(ContextRequestError::Driver)?
                .abandon(&proof)
                .map_err(ContextRequestError::Driver)?;
            original.core = true;
        }
        // These selected variants are closed pure protocol values. Their
        // rich captures remain under the original DomainCustody/host body.
        original.returned.take();
        Ok(())
    }
    fn advance_domain_inner(
        &mut self,
        work: ScopedWorkRef,
        domain: &mut DomainRequest,
        ports: &TaskDriverPorts,
    ) -> Result<(), ContextRequestError> {
        if domain.retired {
            return Ok(());
        }
        if domain
            .custody
            .as_ref()
            .is_some_and(DomainCustody::unresolved)
        {
            return Err(ContextRequestError::Domain(DomainHostError::Unresolved));
        }
        let projected = domain
            .projection
            .lock()
            .expect("actual original projection return")
            .take();
        if let Some(projected) = projected {
            let command = projected.map_err(ContextRequestError::Domain)?;
            let custody = domain.custody.as_ref().expect("original domain source");
            if !custody.matches_projection(&command) {
                return Err(ContextRequestError::WrongRequest);
            }
            let failed = self
                .control_runtime
                .snapshot(self.actor)
                .map_err(ContextRequestError::Driver)?
                .failed;
            if !failed {
                let original = self
                    .resources
                    .requests
                    .get(&work)
                    .expect("same original domain R");
                if original.task_route.is_none() {
                    custody
                        .bind_request(original.request().token)
                        .map_err(ContextRequestError::Domain)?;
                }
                if let Err(refusal) = self.submit_request(work, command) {
                    let failed = self
                        .control_runtime
                        .snapshot(self.actor)
                        .map_err(ContextRequestError::Driver)?
                        .failed;
                    if !failed
                        || !matches!(
                            refusal.error,
                            ContextRequestError::OwnerFailed
                                | ContextRequestError::Driver(
                                    DriverError::ActorFailed
                                        | DriverError::Closed
                                        | DriverError::FailurePublicationRequired
                                )
                        )
                    {
                        return Err(refusal.error);
                    }
                    // A refused filled input is retained in its original R;
                    // only the original pre-fill neutral command may be here.
                    if let Some(command) = refusal.command
                        && !domain
                            .custody
                            .as_ref()
                            .expect("original projection")
                            .matches_projection(&command)
                    {
                        return Err(ContextRequestError::WrongRequest);
                    }
                }
            }
            domain.projected = true;
        }
        let failed = self.prepare_failed_domain(work, domain)?;
        if !failed {
            self.advance_domain_feedback(work, domain)?;
            self.prepare_pending_domain_application(domain)?;
            let needs_disposal = domain
                .custody
                .as_ref()
                .is_some_and(DomainCustody::needs_disposal);
            if needs_disposal && domain.pending.is_none() {
                let author = self.take_domain_author(domain, None)?;
                let custody = domain.custody.as_ref().expect("original Domain disposal");
                let job = custody
                    .take_disposal_job(author)
                    .map_err(|(error, author)| {
                        self.restore_domain_author(domain, author, None);
                        ContextRequestError::Domain(error)
                    })?;
                domain.pending = Some(TaskDriverHostJob::Domain(job));
            }
        }
        if let Some(job) = domain.pending.take() {
            let Some(executor) = ports.lifecycle.as_ref() else {
                domain.pending = Some(job);
                return Err(ContextRequestError::MissingNotificationPort);
            };
            let before = ports.capacity.as_ref().map(|signal| signal.revision());
            match executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Context(self.identity),
            ) {
                Ok(()) => {
                    if let Some(ticket) = domain.ticket.take() {
                        self.complete_host_effect(ticket).map_err(|refusal| {
                            domain.ticket = Some(refusal.ticket);
                            ContextRequestError::Driver(refusal.error)
                        })?;
                    }
                }
                Err((error, job)) => {
                    domain.pending = Some(job);
                    let original = self
                        .resources
                        .requests
                        .get(&work)
                        .expect("same domain pending address");
                    if let Some(before) = before {
                        original
                            .endpoint
                            .ready()
                            .park_capacity(original.endpoint.external().slot(), before)
                            .map_err(ContextRequestError::Ready)?;
                    }
                    if let (Some(signal), Some(before)) = (&ports.capacity, before) {
                        signal
                            .register_waiter(self.registration(), before)
                            .map_err(|_| ContextRequestError::MissingNotificationPort)?;
                    }
                    if !crate::control_runtime::request_endpoint::notification_submit_busy(&error) {
                        return Err(ContextRequestError::NotificationSubmit(error));
                    }
                }
            }
        }
        if domain
            .custody
            .as_ref()
            .is_none_or(DomainCustody::actual_exit_ready)
            && self.retire_domain_funding(work, domain)?
        {
            if let Some(custody) = &domain.custody {
                assert!(custody.release_retention());
            }
            domain.retired = true;
        }
        Ok(())
    }
    fn prepare_pending_domain_application(
        &mut self,
        domain: &mut DomainRequest,
    ) -> Result<(), ContextRequestError> {
        if domain.pending.is_some() || domain.ticket.is_none() {
            return Ok(());
        }
        let custody = domain
            .custody
            .as_ref()
            .expect("same pending Domain Apply owner")
            .clone();
        if matches!(
            &domain
                .ticket
                .as_ref()
                .expect("original pending effect")
                .effect,
            WorkerAction::Context(ContextEffect::ApplyTaskDomains { .. })
        ) {
            let task = self
                .task(custody.task().expect("original Task Apply"))
                .ok_or(ContextRequestError::UnknownTask)?;
            if let Some(descriptor) = task.production_domain_descriptor() {
                custody
                    .bind_task_descriptor(descriptor)
                    .map_err(ContextRequestError::Domain)?;
            }
            if !custody.task_application_ready() {
                return Ok(());
            }
        }
        let ticket = domain
            .ticket
            .take()
            .expect("same pending original Domain effect");
        let compare = Some(matches!(
            &ticket.effect,
            WorkerAction::Context(ContextEffect::RecompareContextCredential { .. })
        ));
        let author = match self.take_domain_author(domain, compare) {
            Ok(author) => author,
            Err(error) => {
                domain.ticket = Some(ticket);
                return Err(error);
            }
        };
        match custody.take_job(&ticket, author) {
            Ok(job) => {
                domain.pending = Some(TaskDriverHostJob::Domain(job));
                domain.ticket = Some(ticket);
                Ok(())
            }
            Err((error, author)) => {
                self.restore_domain_author(domain, author, compare);
                domain.ticket = Some(ticket);
                Err(ContextRequestError::Domain(error))
            }
        }
    }
    fn take_domain_author(
        &mut self,
        domain: &mut DomainRequest,
        compare: Option<bool>,
    ) -> Result<DomainReturn, ContextRequestError> {
        match &mut domain.funding {
            DomainFunding::Partials(_) => Err(ContextRequestError::WrongPhase),
            DomainFunding::Task {
                pending,
                author,
                custody,
                outer,
                direct,
                unused_sources,
                ..
            } => {
                if compare.is_none() && pending.is_some() {
                    let funding = pending.take().expect("same unused Domain return funding");
                    let report = funding.author.report.clone();
                    let observer = funding.author.work.retirement_observer();
                    let work = funding.author.work.reference();
                    let (receipt, original) =
                        ContextDomainAuthor::from_reported(adopt_reported_work(
                            funding.author.work,
                            funding.author.capacity,
                            funding.author.report,
                        ))
                        .unwrap_or_else(|_| std::process::abort());
                    *unused_sources = Some(SourceSlotIssuePartial::from_admissions(
                        self.scope(),
                        funding.sources,
                    ));
                    *outer = Some(funding.outer);
                    *direct = Some(Box::new(DirectReturn {
                        work,
                        author: None,
                        receipt,
                        report,
                        observer,
                        permit: None,
                        input: None,
                        returned: None,
                        core: false,
                        physical: false,
                        retired: false,
                        unused: true,
                    }));
                    return Ok(DomainReturn::Context(original));
                }
                if let Some(funding) = pending.take() {
                    let task_id = domain
                        .custody
                        .as_ref()
                        .expect("original Task domain source")
                        .task()
                        .expect("Task domain identity");
                    let Some(task) = self.task(task_id) else {
                        *pending = Some(funding);
                        return Err(ContextRequestError::UnknownTask);
                    };
                    let observer = funding.author.work.retirement_observer();
                    match task.bind_domain_return(
                        funding.author,
                        crate::task_proposal_ingress::CutSourceAdmission::Fresh(funding.sources),
                    ) {
                        Ok((owner, original)) => {
                            self.task_driver.domain_feedback_cuts.insert(
                                (task_id, owner.cut()),
                                domain.custody.as_ref().expect("original R").work(),
                            );
                            domain.feedback_observer = Some(observer);
                            *custody = Some(owner);
                            *author = Some(DomainReturn::Task(original));
                            *outer = Some(funding.outer);
                        }
                        Err(refusal) => {
                            let Ok(sources) = refusal.admission.into_fresh() else {
                                std::process::abort();
                            };
                            *pending = Some(Box::new(TaskRequestFunding {
                                author: refusal.role,
                                sources,
                                outer: funding.outer,
                                header_ready: funding.header_ready,
                            }));
                            return Err(ContextRequestError::Causal(refusal.error));
                        }
                    }
                }
                author.take().ok_or(ContextRequestError::WrongPhase)
            }
            DomainFunding::Context { roles, .. } => {
                let original = &mut roles[if compare == Some(true) { 0 } else { 1 }];
                if compare.is_none() {
                    original.unused = true;
                }
                original
                    .author
                    .take()
                    .ok_or(ContextRequestError::WrongPhase)
            }
        }
    }
    fn restore_domain_author(
        &mut self,
        domain: &mut DomainRequest,
        original: DomainReturn,
        compare: Option<bool>,
    ) {
        match &mut domain.funding {
            DomainFunding::Partials(_) => {
                std::mem::forget(original);
                std::process::abort();
            }
            DomainFunding::Task { author, .. } => {
                assert!(author.replace(original).is_none());
            }
            DomainFunding::Context { roles, .. } => {
                assert!(
                    roles[if compare == Some(true) { 0 } else { 1 }]
                        .author
                        .replace(original)
                        .is_none()
                );
            }
        }
    }
    fn advance_domain_feedback(
        &mut self,
        work: ScopedWorkRef,
        domain: &mut DomainRequest,
    ) -> Result<(), ContextRequestError> {
        match &mut domain.funding {
            DomainFunding::Task {
                custody: Some(custody),
                ..
            } => {
                let task_id = custody.task();
                let before = custody.phase();
                let task = self.task(task_id).ok_or(ContextRequestError::UnknownTask)?;
                custody
                    .observe_consumers(task.causal())
                    .map_err(|_| ContextRequestError::Domain(DomainHostError::Unresolved))?;
                if before != ExitRolePhase::Retired && custody.phase() == ExitRolePhase::Retired {
                    task.pending_notice().pending();
                }
            }
            DomainFunding::Context { roles, .. } => {
                for original in roles.iter_mut() {
                    if original.input.is_none()
                        && original.returned.is_none()
                        && original.author.is_none()
                    {
                        original.returned = original
                            .receipt
                            .reader
                            .try_take()
                            .map_err(ContextRequestError::Runtime)?;
                    }
                    if let Some(input) = original.returned.take() {
                        let permit = original
                            .permit
                            .take()
                            .ok_or(ContextRequestError::WrongPhase)?;
                        let message = WorkerMessage::Context(ContextMessage::Command {
                            work: original.work,
                            request: None,
                            command: input,
                        });
                        let id = permit.id();
                        if let Err(error) = permit.bind() {
                            original.permit = Some(permit);
                            if let WorkerMessage::Context(ContextMessage::Command {
                                command, ..
                            }) = message
                            {
                                original.returned = Some(command);
                            }
                            return Err(ContextRequestError::Driver(error));
                        }
                        match permit.fill(message) {
                            Ok(actual) => {
                                assert_eq!(actual, id);
                                original.input = Some(actual);
                                assert!(self.request_inputs.insert(actual, work).is_none());
                            }
                            Err((error, message)) => {
                                original.input = Some(id);
                                assert!(self.request_inputs.insert(id, work).is_none());
                                if let WorkerMessage::Context(ContextMessage::Command {
                                    command,
                                    ..
                                }) = message
                                {
                                    original.returned = Some(command);
                                }
                                if self
                                    .control_runtime
                                    .snapshot(self.actor)
                                    .map_err(ContextRequestError::Driver)?
                                    .failed
                                    && matches!(
                                        error,
                                        DriverError::ActorFailed
                                            | DriverError::Closed
                                            | DriverError::FailurePublicationRequired
                                    )
                                {
                                    domain
                                        .custody
                                        .as_ref()
                                        .expect("original refused feedback")
                                        .notify();
                                    return Ok(());
                                }
                                return Err(ContextRequestError::Driver(error));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn retire_domain_funding(
        &mut self,
        work: ScopedWorkRef,
        domain: &mut DomainRequest,
    ) -> Result<bool, ContextRequestError> {
        match &mut domain.funding {
            DomainFunding::Partials(originals) => {
                for original in originals.iter_mut() {
                    if !original.try_retire()? {
                        self.resources
                            .requests
                            .get(&work)
                            .expect("same domain R funding owner")
                            .endpoint
                            .external()
                            .notify();
                        return Ok(false);
                    }
                }
                originals.clear();
            }
            DomainFunding::Task {
                pending,
                author,
                custody,
                outer,
                unused,
                direct,
                unused_sources,
            } => {
                if let Some(funding) = pending.take() {
                    *unused = Some(ContextRequestPartial::from_task_funding(
                        *funding,
                        self.ledger.clone(),
                    ));
                }
                if let Some(unused) = unused
                    && !unused.try_retire()?
                {
                    return Ok(false);
                }
                if let Some(original) = author.take()
                    && let Err(original) = original.withdraw_unused()
                {
                    *author = original;
                    return Err(ContextRequestError::Domain(DomainHostError::Unresolved));
                }
                if let Some(custody) = custody {
                    let task_id = custody.task();
                    let task = self.task(task_id).ok_or(ContextRequestError::UnknownTask)?;
                    custody
                        .observe_consumers(task.causal())
                        .map_err(|_| ContextRequestError::Domain(DomainHostError::Unresolved))?;
                    if custody.phase() != ExitRolePhase::Retired {
                        return Ok(false);
                    }
                    match task.causal().rpc_cut(custody.cut()) {
                        Err(CausalError::UnknownRole) => {
                            self.task_driver
                                .domain_feedback_cuts
                                .remove(&(task_id, custody.cut()));
                            for input in &mut domain.feedback_inputs {
                                if let Some(input) = input.take() {
                                    self.request_inputs.remove(&input);
                                }
                            }
                        }
                        Ok(_) => {
                            task.pending_notice().pending();
                            return Ok(false);
                        }
                        Err(error) => return Err(ContextRequestError::Causal(error)),
                    }
                }
                if let Some(observer) = &domain.feedback_observer
                    && !observer.is_retired()
                {
                    self.ledger
                        .reap_scope_ready(self.scope().id(), 1)
                        .map_err(ContextRequestError::Runtime)?;
                    if !observer.is_retired() {
                        domain.custody.as_ref().map(DomainCustody::notify);
                        return Ok(false);
                    }
                }
                if let Some(sources) = unused_sources
                    && !sources
                        .try_retire(&self.ledger)
                        .map_err(ContextRequestError::SourceSlots)?
                {
                    return Ok(false);
                }
                if let Some(original) = direct
                    && !self.retire_direct_domain_return(original)?
                {
                    return Ok(false);
                }
                if let Some(outer) = outer
                    && !outer.is_released()
                {
                    outer
                        .release_quota(
                            &outer
                                .resources()
                                .map_err(ContextRequestError::Runtime)?
                                .quota,
                        )
                        .and_then(|()| outer.finish())
                        .map_err(ContextRequestError::Runtime)?;
                }
            }
            DomainFunding::Context {
                roles,
                unused,
                outer,
            } => {
                if !unused.try_retire()? {
                    return Ok(false);
                }
                for original in roles.iter_mut() {
                    if !self.retire_direct_domain_return(original)? {
                        domain.custody.as_ref().map(DomainCustody::notify);
                        return Ok(false);
                    }
                }
                if let Some(outer) = outer
                    && !outer.is_released()
                {
                    outer
                        .release_quota(
                            &outer
                                .resources()
                                .map_err(ContextRequestError::Runtime)?
                                .quota,
                        )
                        .and_then(|()| outer.finish())
                        .map_err(ContextRequestError::Runtime)?;
                }
            }
        }
        Ok(true)
    }
    fn retire_direct_domain_return(
        &mut self,
        original: &mut DirectReturn,
    ) -> Result<bool, ContextRequestError> {
        if original.retired {
            return Ok(true);
        }
        if let Some(author) = original.author.take() {
            if let Err(author) = author.withdraw_unused() {
                original.author = author;
                return Err(ContextRequestError::Domain(DomainHostError::Unresolved));
            }
            original.unused = true;
        }
        if !original
            .receipt
            .role
            .actual_exit_ready()
            .map_err(ContextRequestError::Exit)?
        {
            return Ok(false);
        }
        if original.unused {
            if let Some(permit) = original.permit.take() {
                permit
                    .revoke_unused()
                    .map_err(ContextRequestError::Driver)?;
            }
            original.core = true;
            original.physical = true;
        }
        if !original.physical {
            original
                .report
                .consume(external_consumer())
                .map_err(ContextRequestError::Runtime)?;
            original.physical = true;
        }
        if let Some(input) = original.input {
            let custody = self
                .control_runtime
                .input_custody(self.actor, input)
                .map_err(ContextRequestError::Driver)?
                .ok_or(ContextRequestError::MissingCoreReceipt)?;
            if !original.core {
                match custody.phase {
                    InputPhase::Committed => original
                        .report
                        .consume(core_consumer())
                        .map_err(ContextRequestError::Runtime)?,
                    InputPhase::AbandonedByOwnerFailure => original
                        .report
                        .abandon_failed_owner(
                            core_consumer(),
                            self.scope(),
                            self.initial_generation,
                        )
                        .map_err(ContextRequestError::Runtime)?,
                    _ => return Ok(false),
                }
                original.core = true;
            }
            match self.control_runtime.retire_input(self.actor, input) {
                Ok(()) => {
                    self.request_inputs.remove(&input);
                    original.input = None;
                }
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(ContextRequestError::Driver(error)),
            }
        }
        if !original.core {
            return Ok(false);
        }
        if !original.observer.is_retired() {
            self.ledger
                .reap_scope_ready(self.scope().id(), 1)
                .map_err(ContextRequestError::Runtime)?;
            if !original.observer.is_retired() {
                return Ok(false);
            }
        }
        original.retired = original
            .receipt
            .role
            .try_retire()
            .map_err(ContextRequestError::Exit)?;
        Ok(original.retired)
    }
    pub(crate) fn handoff_domain_effect(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
        ports: &TaskDriverPorts,
    ) -> Result<(), super::super::task_driver::TaskDriverRefusal> {
        use super::super::task_driver::{TaskDriverError, TaskDriverRefusal};
        let reject = |error, ticket| TaskDriverRefusal {
            error: TaskDriverError::Request(error),
            ticket: Some(ticket),
        };
        let token = match &ticket.effect {
            WorkerAction::Context(
                ContextEffect::ApplyTaskDomains { request, .. }
                | ContextEffect::ApplyContextDomain { request, .. }
                | ContextEffect::RecompareContextCredential { request, .. }
                | ContextEffect::DomainRequestCompleted { request, .. }
                | ContextEffect::WaitTaskDomain { request, .. }
                | ContextEffect::WaitContextDomain { request, .. },
            ) => *request,
            WorkerAction::Context(
                ContextEffect::WakeContextDomains | ContextEffect::WakeTaskDomains { .. },
            ) => {
                return self.complete_host_effect(ticket).map_err(|refusal| {
                    reject(ContextRequestError::Driver(refusal.error), refusal.ticket)
                });
            }
            _ => return Err(reject(ContextRequestError::WrongRequest, ticket)),
        };
        let Some(&work) = self.request_inputs.get(&token) else {
            return Err(reject(ContextRequestError::UnknownRequest, ticket));
        };
        let Some(mut domain) = self
            .resources
            .requests
            .get_mut(&work)
            .and_then(|original| original.domain.take())
        else {
            return Err(reject(ContextRequestError::WrongRequest, ticket));
        };
        let result = (|| {
            let custody = domain
                .custody
                .as_ref()
                .expect("original rich Domain effect")
                .clone();
            if matches!(
                &ticket.effect,
                WorkerAction::Context(ContextEffect::DomainRequestCompleted { .. })
            ) {
                if let Err(error) = custody.complete_core(&ticket) {
                    return Err(reject(ContextRequestError::Domain(error), ticket));
                }
                return self.complete_host_effect(ticket).map_err(|refusal| {
                    reject(ContextRequestError::Driver(refusal.error), refusal.ticket)
                });
            }
            if matches!(
                &ticket.effect,
                WorkerAction::Context(
                    ContextEffect::WaitTaskDomain { .. } | ContextEffect::WaitContextDomain { .. }
                )
            ) {
                return self.complete_host_effect(ticket).map_err(|refusal| {
                    reject(ContextRequestError::Driver(refusal.error), refusal.ticket)
                });
            }
            assert!(domain.pending.is_none() && domain.ticket.is_none());
            domain.ticket = Some(ticket);
            match self.advance_domain_inner(work, &mut domain, ports) {
                Ok(()) => Ok(()),
                Err(error) => Err(TaskDriverRefusal {
                    error: TaskDriverError::Request(error),
                    ticket: None,
                }),
            }
        })();
        self.resources
            .requests
            .get_mut(&work)
            .expect("same original rich R")
            .domain = Some(domain);
        result
    }
    pub(super) fn remember_domain_feedback_delivery(
        &mut self,
        work: ScopedWorkRef,
        delivery: crate::control_runtime::causal::CausalDelivery,
    ) {
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("same original Domain R delivery");
        let domain = original
            .domain
            .as_mut()
            .expect("same original Domain return inventory");
        assert!(domain.feedback_inputs.iter().all(Option::is_none));
        domain.feedback_inputs = [delivery.prefix, Some(delivery.event)];
        assert!(self.request_inputs.insert(delivery.event, work).is_none());
        if let Some(prefix) = delivery.prefix {
            assert!(self.request_inputs.insert(prefix, work).is_none());
        }
        original.endpoint.external().notify();
    }
    pub(crate) fn domain_inputs_committed(&self, inputs: &[InputId]) {
        for input in inputs {
            if let Some(work) = self.request_inputs.get(input)
                && let Some(original) = self.resources.requests.get(work)
                && original.domain.is_some()
            {
                original.endpoint.external().notify();
            }
        }
    }
    /// Source progress schedules a finite original-row epoch. New admissions
    /// cannot extend its upper fence or reset an existing cursor.
    pub(crate) fn notify_domain_source_progress(&mut self, task: TaskIdentity) {
        if let Some(epoch) = self.task_driver.domain_wakes.get_mut(&task) {
            epoch.2 = true;
            return;
        }
        let Some(upper) = self
            .task_driver
            .domain_requests
            .get(&task)
            .and_then(|rows| rows.last())
            .copied()
        else {
            return;
        };
        self.task_driver
            .domain_wakes
            .insert(task, (None, upper, false));
        self.task_driver.domain_wake_order.push_back(task);
        self.ready_notice().notify();
    }
    pub(crate) fn advance_domain_wakes(&mut self, budget: usize) -> bool {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        for _ in 0..budget {
            let Some(task) = self.task_driver.domain_wake_order.pop_front() else {
                break;
            };
            let Some((after, upper, again)) = self.task_driver.domain_wakes.remove(&task) else {
                continue;
            };
            let next = self
                .task_driver
                .domain_requests
                .get(&task)
                .and_then(|rows| {
                    rows.range((after.map_or(Unbounded, Excluded), Included(upper)))
                        .next()
                })
                .copied();
            if let Some(work) = next {
                if let Some(original) = self.resources.requests.get(&work) {
                    original.endpoint.external().notify();
                }
                self.task_driver
                    .domain_wakes
                    .insert(task, (Some(work), upper, again));
                self.task_driver.domain_wake_order.push_back(task);
            } else if again
                && let Some(upper) = self
                    .task_driver
                    .domain_requests
                    .get(&task)
                    .and_then(|rows| rows.last())
                    .copied()
            {
                self.task_driver
                    .domain_wakes
                    .insert(task, (None, upper, false));
                self.task_driver.domain_wake_order.push_back(task);
            }
        }
        !self.task_driver.domain_wake_order.is_empty()
    }
    pub(super) fn remove_domain_request_index(&mut self, task: TaskIdentity, work: ScopedWorkRef) {
        if let Some(rows) = self.task_driver.domain_requests.get_mut(&task) {
            rows.remove(&work);
            if rows.is_empty() {
                self.task_driver.domain_requests.remove(&task);
            }
        }
    }
}

impl ContextRequestCustody {
    fn abandon_failed_domain_message(
        &mut self,
        custody: &DomainCustody,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<(), ContextRequestError> {
        let Some(refused) = &self.failed_message else {
            return Ok(());
        };
        if self.task_route.is_some()
            || self.submitted
            || !self.started
            || !matches!(
                refused.error,
                DriverError::ActorFailed
                    | DriverError::Closed
                    | DriverError::FailurePublicationRequired
            )
        {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        let Some(request) = self.request else {
            return Err(ContextRequestError::WrongRequest);
        };
        let matches = matches!(&refused.message, WorkerMessage::Context(ContextMessage::Command {
            work, request: Some(metadata), command: ContextInput::Request(command),
        }) if *work==self.work && request.author==self.work
            && metadata.operation==request.operation && metadata.deadline==request.deadline
            && custody.matches_projection(command));
        if !matches {
            return Err(ContextRequestError::WrongRequest);
        }
        stage
            .validate(&self.runtime, self.actor)
            .map_err(ContextRequestError::Driver)?;
        if !self.reply_completed || !self.endpoint.reply().is_committed() {
            return Ok(());
        }
        let actual = self
            .runtime
            .input_custody(self.actor, request.token)
            .map_err(ContextRequestError::Driver)?
            .ok_or(ContextRequestError::MissingCoreReceipt)?;
        if actual.class != InputClass::Ordinary
            || actual.role.is_some()
            || actual.runtime_metadata
            || !matches!(
                actual.phase,
                InputPhase::Bound | InputPhase::AbandonedByOwnerFailure
            )
        {
            return Err(ContextRequestError::WrongRequest);
        }
        self.runtime
            .abandon_failed_input(self.actor, request.token)
            .map_err(ContextRequestError::Driver)?;
        if self
            .runtime
            .input_custody(self.actor, request.token)
            .map_err(ContextRequestError::Driver)?
            .is_none_or(|actual| actual.phase != InputPhase::AbandonedByOwnerFailure)
        {
            return Err(ContextRequestError::MissingCoreReceipt);
        }
        self.core_receipt = CoreReceipt::Abandoned;
        self.failed_message.take();
        Ok(())
    }
}
