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

//! Task-causal leg of the original Context RPC. Reply and physical main-R
//! completion stay in ContextRequestCustody; this leg retains the exact cut,
//! delayed input receipt and R outer storage. Source-slot payments follow the
//! bank's actual holders, including future open sources after this R retires.

use super::*;
use crate::control_runtime::{
    causal::{CausalDelivery, CausalSourceRetirement, RpcAuthorReturned, RpcCausalSender},
    source_positions::{ReapedPosition, SourcePositionError},
};
use crate::task_proposal_ingress::RetiredCut;
use novarocks_worker_protocol::proposal_summary::CutId;

pub(super) struct TaskRequestRoute {
    task: TaskIdentity,
    cut: CutId,
    sender: Option<RpcCausalSender>,
    returned: Option<RpcAuthorReturned>,
    failed_input: Option<ContextInput>,
    outer: CapacityBundle,
    delivery: Option<CausalDelivery>,
    // Positive saved receipt from the original queued RPC's failure takeover;
    // no runtime event input was ever filled for this specific cut.
    queued_abandoned: bool,
    prefix_retired: bool,
    cut_consumed: bool,
    retired: Option<RetiredCut>,
    sources_exited: bool,
    reaped: [Option<ReapedPosition>; 2],
    sources_released: bool,
    detached: Option<[crate::control_runtime::source_positions::DetachedSourceSlotStorage; 2]>,
}
impl TaskRequestRoute {
    pub(super) fn new(
        task: TaskIdentity,
        cut: CutId,
        sender: RpcCausalSender,
        outer: CapacityBundle,
    ) -> Self {
        Self {
            task,
            cut,
            sender: Some(sender),
            returned: None,
            failed_input: None,
            outer,
            delivery: None,
            queued_abandoned: false,
            prefix_retired: false,
            cut_consumed: false,
            retired: None,
            sources_exited: false,
            reaped: [None, None],
            sources_released: false,
            detached: None,
        }
    }
    pub(super) fn task(&self) -> TaskIdentity {
        self.task
    }
    pub(super) fn cut(&self) -> CutId {
        self.cut
    }
    pub(super) fn abandoned_before_fill(&self) -> bool {
        self.queued_abandoned
    }
    fn observe_return(&mut self, task: &mut TaskHostRecord) -> Result<(), ContextRequestError> {
        if let Some(returned) = self.returned.take()
            && let Err((error, returned)) = task.causal().observe_rpc_author_return(returned)
        {
            self.returned = Some(returned);
            return Err(ContextRequestError::Causal(error));
        }
        Ok(())
    }
    pub(super) fn submit(
        &mut self,
        original: &mut ContextRequestCustody,
        task: &mut TaskHostRecord,
        command: ContextCommand,
    ) -> Result<(), ContextRequestSubmitRefusal> {
        let reject = |error, command| ContextRequestSubmitRefusal {
            error,
            command: Some(command),
        };
        if original.submitted || self.sender.is_none() || self.failed_input.is_some() {
            return Err(reject(ContextRequestError::WrongPhase, command));
        }
        if command.task() != Some(self.task)
            || !matches!(
                command,
                ContextCommand::Cancel { .. }
                    | ContextCommand::UpdateTask { .. }
                    | ContextCommand::GetFinalInfo { .. }
                    | ContextCommand::GetDynamicFilters { .. }
            )
        {
            return Err(reject(ContextRequestError::WrongRequest, command));
        }
        if !original.started {
            if let Err(error) = original
                .authority
                .as_mut()
                .expect("original Task R authority")
                .start()
            {
                return Err(reject(ContextRequestError::Exit(error), command));
            }
            original.started = true;
        }
        let result = self
            .sender
            .take()
            .expect("one original Task RPC sender")
            .send_input(ContextInput::Request(command));
        match result {
            Ok(returned) => {
                self.returned = Some(returned);
                original.submitted = true;
            }
            Err(refusal) => {
                self.returned = Some(refusal.returned);
                self.failed_input = Some(refusal.input);
                // The exact refused input remains here; neither external
                // cancellation nor a failed handoff is a physical R exit.
                self.observe_return(task)
                    .map_err(|error| ContextRequestSubmitRefusal {
                        error,
                        command: None,
                    })?;
                return Err(ContextRequestSubmitRefusal {
                    error: ContextRequestError::SubmissionUnresolved,
                    command: None,
                });
            }
        }
        self.observe_return(task)
            .map_err(|error| ContextRequestSubmitRefusal {
                error,
                command: None,
            })
    }
    /// The same already paid R owns this channel fact even when its external
    /// observer has ended. It must retain its original Task causal prefix.
    pub(super) fn submit_root_end_acknowledged(
        &mut self,
        original: &mut ContextRequestCustody,
        task: &mut TaskHostRecord,
        shared: &RootReadShared,
    ) -> Result<(), ContextRequestError> {
        if !shared.end_acknowledged()
            || task.task_id() != self.task
            || original.expected_task != Some(self.task)
        {
            return Err(ContextRequestError::WrongRequest);
        }
        if original.submitted || self.sender.is_none() || self.failed_input.is_some() {
            return Err(ContextRequestError::WrongPhase);
        }
        if !original.started {
            original
                .authority
                .as_mut()
                .expect("original Root R authority")
                .start()
                .map_err(ContextRequestError::Exit)?;
            original.started = true;
        }
        let input = ContextInput::RootEndAcknowledged { task: self.task };
        match self
            .sender
            .take()
            .expect("same original Root R sender")
            .send_input(input)
        {
            Ok(returned) => {
                self.returned = Some(returned);
                original.submitted = true;
            }
            Err(refusal) => {
                self.returned = Some(refusal.returned);
                self.failed_input = Some(refusal.input);
                self.observe_return(task)?;
                return Err(ContextRequestError::SubmissionUnresolved);
            }
        }
        self.observe_return(task)
    }
    pub(super) fn capture_delivery(
        &mut self,
        original: &mut ContextRequestCustody,
        task: &mut TaskHostRecord,
    ) -> Result<(), ContextRequestError> {
        if task.task_id() != self.task {
            return Err(ContextRequestError::WrongRequest);
        }
        if self.delivery.is_none() {
            let delivery = task
                .causal()
                .rpc_delivery(self.cut)
                .map_err(ContextRequestError::Causal)?;
            if let Some(delivery) = delivery {
                let metadata = original.endpoint.metadata();
                original.request = Some(RequestContext {
                    token: delivery.event,
                    operation: metadata.operation,
                    deadline: metadata.deadline,
                });
                if let Some(domain) = original
                    .domain
                    .as_ref()
                    .and_then(|domain| domain.custody.as_ref())
                {
                    domain
                        .bind_request(delivery.event)
                        .map_err(ContextRequestError::Domain)?;
                }
                self.delivery = Some(delivery);
            } else if task
                .causal()
                .rpc_abandoned_before_fill(self.cut)
                .map_err(ContextRequestError::Causal)?
            {
                self.queued_abandoned = true;
                original.core_receipt = CoreReceipt::Abandoned;
            }
        }
        Ok(())
    }
    pub(super) fn prepare(
        &mut self,
        original: &mut ContextRequestCustody,
        task: &mut TaskHostRecord,
    ) -> Result<(), ContextRequestError> {
        if task.task_id() != self.task {
            return Err(ContextRequestError::WrongRequest);
        }
        if !original.submitted
            && !original
                .deferred_root
                .as_ref()
                .is_some_and(|root| root.shared.end_acknowledged())
            && original.endpoint.external_ended()
            && let Some(sender) = self.sender.take()
        {
            self.returned = Some(sender.withdraw());
        }
        self.observe_return(task)?;
        if self.failed_input.is_some() {
            return Err(ContextRequestError::SubmissionUnresolved);
        }
        if self.retired.is_some() {
            return Ok(());
        }
        if original.submitted {
            self.capture_delivery(original, task)?;
            if self.delivery.is_some() {
                // Preserve both prefix and event receipts before either real
                // runtime header can disappear. This grants no physical exit.
                task.causal()
                    .observe_core(self.cut)
                    .map_err(ContextRequestError::Causal)?;
            }
        }
        Ok(())
    }
    pub(super) fn finish(
        &mut self,
        original: &mut ContextRequestCustody,
        task: &mut TaskHostRecord,
    ) -> Result<bool, ContextRequestError> {
        if self.retired.is_none() {
            if let Some(prefix) = self.delivery.and_then(|delivery| delivery.prefix)
                && !self.prefix_retired
            {
                match original.runtime.retire_input(original.actor, prefix) {
                    Ok(()) => self.prefix_retired = true,
                    Err(
                        DriverError::InputStillQueued
                        | DriverError::EffectsRemain
                        | DriverError::ReservationsRemain,
                    ) => return Ok(false),
                    Err(error) => return Err(ContextRequestError::Driver(error)),
                }
            }
            if !task
                .causal()
                .observe_effects_exited(self.cut)
                .map_err(ContextRequestError::Causal)?
            {
                return Ok(false);
            }
            if original.submitted && !self.cut_consumed {
                task.port()
                    .ingress()
                    .consume_cut(self.cut, 0)
                    .map_err(|error| ContextRequestError::Causal(error.into()))?;
                self.cut_consumed = true;
            }
            let result = task
                .causal()
                .retire_after_exit(self.cut, CausalSourceRetirement::Unclaimed);
            match result {
                Ok(retired) => self.retired = Some(retired),
                Err(refusal) => match refusal.error {
                    CausalError::Cut(
                        novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved,
                    )
                    | CausalError::Source(
                        SourcePositionError::NotAvailable | SourcePositionError::SourceStillHeld,
                    ) => {
                        let progress = task
                            .causal()
                            .advance_source_pair_retirement(
                                self.cut,
                                &original.ledger,
                                &original.scope,
                            )
                            .map_err(ContextRequestError::Causal)?;
                        let Some(reaped) = progress.reaped else {
                            if progress.progressed {
                                original.endpoint.external().notify();
                            }
                            return Ok(false);
                        };
                        match task
                            .causal()
                            .retire_after_exit(self.cut, CausalSourceRetirement::Reaped(reaped))
                        {
                            Ok(retired) => self.retired = Some(retired),
                            Err(refusal) => match refusal.error {
                                CausalError::Driver(DriverError::RoleBusy)
                                | CausalError::Cut(
                                    novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved
                                    | novarocks_worker_protocol::proposal_summary::CutError::NotReady
                                    | novarocks_worker_protocol::proposal_summary::CutError::Consuming,
                                ) => {
                                    // These witnesses observe the same resident
                                    // originals and can be read again next turn.
                                    if progress.progressed {
                                        original.endpoint.external().notify();
                                    }
                                    return Ok(false);
                                }
                                error => return Err(ContextRequestError::Causal(error)),
                            },
                        }
                    }
                    CausalError::Cut(
                        novarocks_worker_protocol::proposal_summary::CutError::NotReady
                        | novarocks_worker_protocol::proposal_summary::CutError::Consuming,
                    )
                    | CausalError::Driver(DriverError::RoleBusy) => return Ok(false),
                    error => return Err(ContextRequestError::Causal(error)),
                },
            }
        }
        let ingress = task.port().ingress().clone();
        let retired = self.retired.as_ref().expect("same original paid pair");
        if !self.sources_exited {
            let unused = ingress
                .take_unused_owned_pair(&retired.pair)
                .map_err(|error| ContextRequestError::Causal(error.into()))?;
            for holder in unused.into_iter().flatten() {
                holder.exit();
            }
            self.sources_exited = true;
        }
        if !self.sources_released {
            for source in ingress
                .owned_pair_inventory(&retired.pair)
                .map_err(|error| ContextRequestError::Causal(error.into()))?
            {
                if !source
                    .custody
                    .try_retire()
                    .map_err(ContextRequestError::Exit)?
                {
                    return Ok(false);
                }
            }
            original
                .ledger
                .reap_scope_ready(original.scope.id(), 1)
                .map_err(ContextRequestError::Runtime)?;
            let probes = ingress
                .prepare_owned_pair_reap(&retired.pair)
                .map_err(|error| ContextRequestError::Causal(error.into()))?;
            for (saved, probe) in self.reaped.iter_mut().zip(probes) {
                if saved.is_none() {
                    *saved = probe.observe_reaped().ok();
                }
            }
            if self.reaped.iter().any(Option::is_none) {
                // A bounded same-scope reaper can retire a different sibling
                // first. Retry only this finite metadata work, not live I/O.
                original.endpoint.external().notify();
                return Ok(false);
            }
            let reaped = self
                .reaped
                .each_mut()
                .map(|saved| saved.take().expect("both actual reap witnesses"));
            let detached = match ingress.release_owned_pair(&retired.pair, reaped) {
                Ok(storage) => storage,
                Err((error, reaped)) => {
                    self.reaped = reaped.map(Some);
                    return Err(ContextRequestError::Causal(error.into()));
                }
            };
            self.detached = Some(detached);
            self.sources_released = true;
        }
        if let Some(detached) = &mut self.detached {
            for storage in detached {
                storage.release().map_err(ContextRequestError::Runtime)?;
            }
        }
        self.detached = None;
        if !self.outer.is_released() {
            let resources = self
                .outer
                .resources()
                .map_err(ContextRequestError::Runtime)?;
            self.outer
                .release(&resources)
                .map_err(ContextRequestError::Runtime)?;
        }
        Ok(true)
    }
}

impl ContextRequestPartial {
    pub(super) fn from_task_funding(
        funding: super::task_funding::TaskRequestFunding,
        ledger: RuntimeLedger,
    ) -> Self {
        let work_retirement = funding.author.work.retirement_observer();
        let scope = funding.author.work.scope().clone();
        Self {
            ledger,
            work: funding.author.work,
            capacity: funding.author.capacity,
            report: Some(funding.author.report),
            permit: None,
            work_retirement,
            report_revoked: false,
            exited: false,
            unresolved: false,
            outer: Some(funding.outer),
            sources: Some(SourceSlotIssuePartial::from_admissions(
                &scope,
                funding.sources,
            )),
        }
    }
}

impl ContextHostRecord {
    pub(crate) fn admit_task_request(
        &mut self,
        ready: &ControlRequestReady,
        task: TaskIdentity,
        class: InputClass,
        operation: TaskOperationId,
        deadline: MonotonicInstant,
    ) -> Result<(WorkId, TaskContextRequestHandle), ContextRequestAdmissionRefusal> {
        use crate::control_runtime::causal::{RpcAuthorAdmissionError, RpcAuthorAdmissionRefusal};
        use crate::task_proposal_ingress::CutSourceAdmission;
        let no_partial = |error| ContextRequestAdmissionRefusal {
            error,
            partial: None,
        };
        if !ready.same_runtime(&self.control_runtime) {
            return Err(no_partial(ContextRequestError::WrongRuntime));
        }
        if !matches!(class, InputClass::Ordinary | InputClass::Control) {
            return Err(no_partial(ContextRequestError::InvalidClass));
        }
        if self.resources.failure.is_some() {
            return Err(no_partial(ContextRequestError::OwnerFailed));
        }
        let snapshot = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(|error| no_partial(ContextRequestError::Driver(error)))?;
        if snapshot.sealed
            || self.resources.control_admission_closed
            || (class == InputClass::Ordinary && !snapshot.external_open)
        {
            return Err(no_partial(ContextRequestError::AdmissionClosed));
        }
        match self.tasks.get(&task) {
            Some(super::super::TaskHostOwner::Ready(host)) if host.request_admission_open() => {}
            Some(super::super::TaskHostOwner::Ready(_)) => {
                return Err(no_partial(ContextRequestError::Causal(
                    CausalError::RequestAdmissionClosed,
                )));
            }
            Some(super::super::TaskHostOwner::Initializing(_)) => {
                return Err(no_partial(ContextRequestError::TaskNotReady));
            }
            Some(super::super::TaskHostOwner::Unpublished(_)) => {
                return Err(no_partial(ContextRequestError::TaskUnpublished));
            }
            None => return Err(no_partial(ContextRequestError::UnknownTask)),
        }
        let funding = self.reserve_task_request_funding()?;
        let work = funding.author.work.id();
        let work_retirement = funding.author.work.retirement_observer();
        let report = funding.author.report.clone();
        let slot = match ready.register_context(self.scope(), work, self.registration()) {
            Ok(slot) => slot,
            Err(error) => {
                let partial =
                    ContextRequestPartial::from_task_funding(funding, self.ledger.clone());
                return Err(
                    self.retain_request_partial(ContextRequestError::Ready(error), Some(partial))
                );
            }
        };
        let metadata = RequestMetadata {
            operation,
            deadline,
        };
        let (notification, observer) =
            novarocks_control_runtime::reply_notification::ReplyNotification::for_admitted_work(
                &self.control_runtime,
                self.registration(),
                &funding.author.work,
            )
            .unwrap_or_else(|_| std::process::abort());
        let admitted = match self.tasks.get_mut(&task) {
            Some(super::super::TaskHostOwner::Ready(host)) => host.bind_request_author(
                funding.author,
                metadata,
                class,
                1,
                CutSourceAdmission::Fresh(funding.sources),
            ),
            _ => unreachable!("same serial Context still owns the Task"),
        };
        let admitted = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => {
                ready
                    .unregister(&slot)
                    .expect("same never-exposed original Task R slot");
                let error = match &refusal {
                    RpcAuthorAdmissionRefusal::BeforeCut {
                        error: RpcAuthorAdmissionError::Binding(error),
                        ..
                    } => ContextRequestError::Binding(*error),
                    RpcAuthorAdmissionRefusal::BeforeCut {
                        error: RpcAuthorAdmissionError::Causal(error),
                        ..
                    }
                    | RpcAuthorAdmissionRefusal::BoundCut { error, .. } => {
                        ContextRequestError::Causal(*error)
                    }
                };
                match refusal {
                    RpcAuthorAdmissionRefusal::BeforeCut {
                        admitted,
                        admission: CutSourceAdmission::Fresh(sources),
                        ..
                    } => {
                        let partial = ContextRequestPartial::from_task_funding(
                            super::task_funding::TaskRequestFunding {
                                author: admitted,
                                sources,
                                outer: funding.outer,
                            },
                            self.ledger.clone(),
                        );
                        return Err(self.retain_request_partial(error, Some(partial)));
                    }
                    refusal => {
                        assert!(
                            self.resources
                                .request_partials
                                .insert(
                                    work,
                                    ContextRequestAdmissionPartial::BoundTask(Box::new(
                                        TaskRequestBindPartial {
                                            refusal,
                                            outer: funding.outer,
                                        }
                                    ))
                                )
                                .is_none()
                        );
                        if let Some(notice) = &self.resources.notice {
                            notice.notify();
                        }
                        return Err(ContextRequestAdmissionRefusal {
                            error,
                            partial: Some(work),
                        });
                    }
                }
            }
        };
        let endpoint = Arc::new(RequestEndpoint {
            request: metadata,
            ready: ready.clone(),
            reply: PublishedReply::new(self.scope()),
            external: RequestExternalLeg::new(slot),
        });
        let handle = RequestHandle {
            endpoint: endpoint.clone(),
            active: true,
            observer: Some(observer),
        };
        let cut = admitted.cut;
        let custody = ContextRequestCustody {
            runtime: self.control_runtime.clone(),
            ledger: self.ledger.clone(),
            scope: self.scope().clone(),
            actor: self.actor,
            generation: self.initial_generation,
            work,
            endpoint: ContextRequestEndpoint::Task(endpoint),
            request: None,
            domain: None,
            task_route: Some(TaskRequestRoute::new(
                task,
                cut,
                admitted.sender,
                funding.outer,
            )),
            permit: None,
            role: Some(admitted.role),
            authority: Some(admitted.authority),
            report,
            work_retirement,
            expected_task: Some(task),
            submitted: false,
            started: false,
            failed_message: None,
            #[cfg(novarocks_dev_controls)]
            owner_panic_on_renew: false,
            published: None,
            reply_completed: false,
            input_retired: false,
            core_receipt: CoreReceipt::Pending,
            exit_observed: false,
            core_acknowledged: false,
            external_acknowledged: false,
            unresolved_revocation: false,
            create: None,
            retired_task_read: None,
            read_payload: None,
            deferred_root: None,
            observation: None,
            notification,
            pending_notification: None,
            protocol_observation: Default::default(),
            pending_protocol_observation: None,
            protocol_observation_kind: Default::default(),
            release_observation: None,
        };
        assert!(self.resources.requests.insert(work, custody).is_none());
        assert!(self.task_request_cuts.insert((task, cut), work).is_none());
        Ok((work, handle))
    }
    /// Real Task events are filled through this owner. Only the exact RPC cut
    /// gains a token correlation, and only from the inbox's actual receipt.
    pub(crate) fn deliver_task_event(
        &mut self,
        task: TaskIdentity,
    ) -> Result<Option<CausalDelivery>, ContextRequestError> {
        let host = match self.tasks.get_mut(&task) {
            Some(
                super::super::TaskHostOwner::Ready(host)
                | super::super::TaskHostOwner::Initializing(host),
            ) => host.as_mut(),
            Some(super::super::TaskHostOwner::Unpublished(_)) => {
                return Err(ContextRequestError::TaskUnpublished);
            }
            None => return Err(ContextRequestError::UnknownTask),
        };
        let delivery = host
            .causal()
            .deliver_next()
            .map_err(ContextRequestError::Causal)?;
        if let Some(delivery) = delivery
            && let Some(work) = self.task_request_cuts.get(&(task, delivery.cut))
        {
            let original = self
                .resources
                .requests
                .get_mut(work)
                .expect("same original Task R cut index");
            let mut route = original.task_route.take().expect("same actual Task route");
            let result = route.capture_delivery(original, host);
            original.task_route = Some(route);
            result?;
            let input = original.request().token;
            assert_eq!(input, delivery.event);
            assert!(self.request_inputs.insert(input, *work).is_none());
        }
        if let Some(delivery) = delivery {
            if let Some(&work) = self
                .task_driver
                .domain_feedback_cuts
                .get(&(task, delivery.cut))
            {
                self.remember_domain_feedback_delivery(work, delivery);
            }
        }
        Ok(delivery)
    }
    pub(crate) fn close_task_request_admission(
        &mut self,
        task: TaskIdentity,
    ) -> Result<(), ContextRequestError> {
        match self.tasks.get_mut(&task) {
            Some(
                super::super::TaskHostOwner::Ready(host)
                | super::super::TaskHostOwner::Initializing(host),
            ) => {
                host.close_request_admission();
                Ok(())
            }
            Some(super::super::TaskHostOwner::Unpublished(_)) => {
                Err(ContextRequestError::TaskUnpublished)
            }
            None => Err(ContextRequestError::UnknownTask),
        }
    }
}
