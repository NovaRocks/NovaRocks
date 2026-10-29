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

//! Failed Core cleanup drives the same Task, request and M/R/H originals.
//! The live supervisor owns one serial physical-disposal capture position.

use super::*;
use crate::control_runtime::context_host::{
    ContextDriverError,
    task_driver::{TaskDriverHostJob, TaskDriverPorts},
};

impl ContextHostRecord {
    pub(in crate::control_runtime::context_host) fn failed_owner_completed(&self) -> bool {
        self.resources.teardown.failed_owner_completed
    }

    pub(in crate::control_runtime::context_host) fn failure_tail_started(&self) -> bool {
        self.resources.failure.as_ref().is_some_and(|original| {
            original.completed_tail_input.is_some()
                || original
                    .physical
                    .as_ref()
                    .is_some_and(|(purpose, _)| *purpose == FailurePhysicalPurpose::FinalCarrier)
        })
    }

    /// One original rich/neutral capture is physically disposed at a time.
    /// Queue refusal preserves the original job in the existing physical lane.
    fn queue_failed_capture(
        &mut self,
        original: &mut ScopeFailureOwner,
        purpose: FailurePhysicalPurpose,
        capture: super::super::failure_tail::FailureTail,
        job: super::super::failure_tail::FailureTailJob,
        ports: &TaskDriverPorts,
    ) -> Result<(), ContextDriverError> {
        assert!(original.physical.is_none(), "one prepaid failure capture");
        original.physical = Some((purpose, capture));
        self.queue_task_cleanup(TaskDriverHostJob::FailureTail(job), ports)
            .map_err(ContextDriverError::Task)
    }

    fn observe_failed_capture(
        &mut self,
        original: &mut ScopeFailureOwner,
    ) -> Result<bool, ContextDriverError> {
        let Some((purpose, capture)) = &original.physical else {
            return Ok(true);
        };
        if !capture.actual_exit_observed() || *purpose == FailurePhysicalPurpose::FinalCarrier {
            return Ok(false);
        }
        let task = match purpose {
            FailurePhysicalPurpose::Projection(task) | FailurePhysicalPurpose::Neutral(task) => {
                *task
            }
            FailurePhysicalPurpose::RetirementProjection(task) => {
                let task = *task;
                self.complete_failed_retirement_projection(task)
                    .map_err(ContextDriverError::Task)?;
                task
            }
            FailurePhysicalPurpose::FinalCarrier => unreachable!("checked final carrier"),
        };
        original.physical = None;
        if let Some(TaskHostOwner::Ready(owner) | TaskHostOwner::Initializing(owner)) =
            self.tasks.get(&task)
        {
            owner.pending_notice().pending();
        }
        Ok(true)
    }

    pub(in crate::control_runtime::context_host) fn advance_failed_task_cleanup(
        &mut self,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<bool, ContextDriverError> {
        let mut original = self
            .resources
            .failure
            .take()
            .ok_or(ContextDriverError::Failure(
                FailurePublicationError::NotStarted,
            ))?;
        let result = self.advance_failed_tasks_owned(&mut original, ports, budget);
        self.resources.failure = Some(original);
        result
    }

    fn advance_failed_tasks_owned(
        &mut self,
        original: &mut ScopeFailureOwner,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<bool, ContextDriverError> {
        original
            .drained()
            .map_err(ContextDriverError::Failure)?
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextDriverError::Driver)?;
        // Attach original cleanup tickets before a Source header can leave
        // its original bank. This finite pass invokes no foreign body.
        if !self.bind_failed_cleanup(original, budget)? {
            return Ok(true);
        }
        self.settle_failed_driver_effect(original)?;
        self.flush_task_cleanup_pending(ports, budget)
            .map_err(ContextDriverError::Task)?;
        if !self.observe_failed_capture(original)? {
            return Ok(false);
        }
        let health = ports
            .protocol_events
            .as_ref()
            .ok_or(ContextDriverError::WrongOwner)?
            .execution_health();
        let author = original
            .ticket
            .as_ref()
            .ok_or(ContextDriverError::WrongOwner)?
            .descriptor
            .work;
        let key = (self.scope().id(), author);
        if let Some((task, candidate)) = self
            .failed_retirement_candidate()
            .map_err(ContextDriverError::Task)?
        {
            let port = self
                .task(task)
                .ok_or(ContextDriverError::WrongOwner)?
                .port();
            let old = original
                .session
                .commit_snapshot(port.publication(), candidate)
                .map_err(|(error, candidate)| {
                    drop(candidate);
                    ContextDriverError::Driver(error)
                })?;
            self.failed_retirement_projection_committed(task);
            let (capture, job) = super::super::failure_tail::FailureTail::projection(
                key,
                old,
                self.ready_notice().clone(),
                health.clone(),
            );
            self.queue_failed_capture(
                original,
                FailurePhysicalPurpose::RetirementProjection(task),
                capture,
                job,
                ports,
            )?;
            return Ok(false);
        }
        let mut runnable = false;
        for _ in 0..budget {
            let next = if let Some(task) = original.pending_projection_task {
                let next = original
                    .pending_projection
                    .take()
                    .expect("original refused detached projection");
                Some((
                    task,
                    crate::control_runtime::task::FailedTaskStep {
                        detached_projection: Some(next),
                        ..Default::default()
                    },
                ))
            } else {
                let stage = original.drained().map_err(ContextDriverError::Failure)?;
                self.next_failed_task_step(&stage, ports, budget)
                    .map_err(ContextDriverError::Task)?
            };
            let Some((task, mut step)) = next else {
                break;
            };
            runnable |= step.runnable;
            if let Some(action) = step.action.take() {
                self.queue_task_cleanup(TaskDriverHostJob::FailedTask(action), ports)
                    .map_err(ContextDriverError::Task)?;
            }
            if let Some(root) = step.root.take() {
                let scope = self.scope().clone();
                if let Err((error, root)) =
                    self.resources
                        .root_reads
                        .adopt_installed_root(self.identity, &scope, root)
                {
                    let restored = match self.tasks.get_mut(&task) {
                        Some(TaskHostOwner::Ready(owner) | TaskHostOwner::Initializing(owner)) => {
                            owner.restore_root_context_retention(root)
                        }
                        _ => Err(root),
                    };
                    if let Err(root) = restored {
                        std::mem::forget(root);
                        std::process::abort();
                    }
                    return Err(ContextDriverError::Metadata(ContextHostError::RootRead(
                        error,
                    )));
                }
                self.notify_host_progress();
                runnable = true;
            }
            if let Some(neutral) = step.neutral.take() {
                let (capture, job) = super::super::failure_tail::FailureTail::neutral(
                    key,
                    neutral,
                    self.ready_notice().clone(),
                    health.clone(),
                );
                self.queue_failed_capture(
                    original,
                    FailurePhysicalPurpose::Neutral(task),
                    capture,
                    job,
                    ports,
                )?;
                break;
            }
            if let Some(next) = step.detached_projection.take() {
                let port = match self.tasks.get(&task) {
                    Some(TaskHostOwner::Ready(owner) | TaskHostOwner::Initializing(owner)) => {
                        owner.port()
                    }
                    _ => return Err(ContextDriverError::WrongOwner),
                };
                let old = match original.session.commit_snapshot(port.publication(), next) {
                    Ok(old) => old,
                    Err((error, next)) => {
                        original.pending_projection_task = Some(task);
                        original.pending_projection = Some(next);
                        return Err(ContextDriverError::Driver(error));
                    }
                };
                original.pending_projection_task = None;
                let (capture, job) = super::super::failure_tail::FailureTail::projection(
                    key,
                    old,
                    self.ready_notice().clone(),
                    health.clone(),
                );
                self.queue_failed_capture(
                    original,
                    FailurePhysicalPurpose::Projection(task),
                    capture,
                    job,
                    ports,
                )?;
                break;
            }
            if step.ready_for_disposal {
                let stage = original.drained().map_err(ContextDriverError::Failure)?;
                runnable |= self
                    .retire_failed_task_owner(&stage, task, ports)
                    .map_err(ContextDriverError::Task)?;
            }
        }
        let stage = original.drained().map_err(ContextDriverError::Failure)?;
        runnable |= self
            .advance_failed_task_disposals(&stage, ports, budget)
            .map_err(ContextDriverError::Task)?;
        Ok(runnable || self.failed_task_ready())
    }

    pub(in crate::control_runtime::context_host) fn advance_failed_cleanup_tail(
        &mut self,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<bool, ContextDriverError> {
        if budget == 0 || self.failed_owner_completed() {
            return Ok(false);
        }
        let Some(mut original) = self.resources.failure.take() else {
            return Ok(false);
        };
        let result = self.advance_failed_tail_owned(&mut original, ports, budget);
        if result.as_ref().is_ok_and(|(_, completed)| *completed) {
            self.resources.teardown.failed_owner_completed = true;
        } else {
            self.resources.failure = Some(original);
        }
        result.map(|(runnable, _)| runnable)
    }

    fn advance_failed_tail_owned(
        &mut self,
        original: &mut ScopeFailureOwner,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<(bool, bool), ContextDriverError> {
        self.flush_task_cleanup_pending(ports, budget)
            .map_err(ContextDriverError::Task)?;
        if let Some(input) = original.completed_tail_input {
            self.control_runtime
                .retire_input(self.actor, input)
                .map_err(ContextDriverError::Driver)?;
            original.completed_tail_input = None;
            self.notify_host_progress();
            return Ok((true, true));
        }
        if let Some((FailurePhysicalPurpose::FinalCarrier, capture)) = &original.physical {
            if !capture.actual_exit_observed() {
                return Ok((false, false));
            }
            let ticket = original
                .ticket
                .take()
                .ok_or(ContextDriverError::WrongOwner)?;
            let input = ticket.input;
            if let Err(refusal) = self.complete_host_effect(ticket) {
                original.ticket = Some(refusal.ticket);
                return Err(ContextDriverError::Driver(refusal.error));
            }
            original.completed_tail_input = Some(input);
            self.control_runtime
                .retire_input(self.actor, input)
                .map_err(ContextDriverError::Driver)?;
            original.completed_tail_input = None;
            original.physical = None;
            self.notify_host_progress();
            return Ok((true, true));
        }
        self.start_failed_tail(original, ports, budget)
    }

    fn start_failed_tail(
        &mut self,
        original: &mut ScopeFailureOwner,
        ports: &TaskDriverPorts,
        budget: usize,
    ) -> Result<(bool, bool), ContextDriverError> {
        original
            .drained()
            .map_err(ContextDriverError::Failure)?
            .validate(&self.control_runtime, self.actor)
            .map_err(ContextDriverError::Driver)?;
        if !self.tasks.is_empty()
            || !self.task_driver.is_idle()
            || !self.resources.requests.is_empty()
            || !self.resources.request_partials.is_empty()
            || !self.resources.routes.is_empty()
            || original.physical.is_some()
            || original.pending_projection.is_some()
            || self.resources.pending_host_jobs.driver_effect.is_some()
            || self.resources.pending_host_jobs.driver_close.is_some()
            || self.resources.pending_host_jobs.driver_seal.is_some()
            || self.resources.pending_host_jobs.driver_release.is_some()
        {
            return Ok((false, false));
        }
        let jobs = self
            .resources
            .host_jobs
            .as_ref()
            .ok_or(ContextDriverError::WrongOwner)?;
        if !jobs
            .holder_disposed(
                &self.control_runtime,
                self.actor,
                self.identity,
                self.scope(),
            )
            .map_err(ContextDriverError::Host)?
        {
            return Ok((false, false));
        }
        if !self
            .retire_owner_control()
            .map_err(ContextDriverError::Metadata)?
        {
            return Ok((false, false));
        }
        let ticket = original
            .ticket
            .as_ref()
            .ok_or(ContextDriverError::WrongOwner)?;
        if !self
            .resources
            .internal
            .ready_for_failure_tail(ticket.input)
            .map_err(ContextDriverError::Driver)?
        {
            return Ok((false, false));
        }
        if !self
            .dispatch
            .drain_failed_index_turn(&self.control_runtime, budget)
            .map_err(ContextDriverError::Dispatch)?
        {
            return Ok((true, false));
        }
        let key = (self.scope().id(), ticket.descriptor.work);
        let health = ports
            .protocol_events
            .as_ref()
            .ok_or(ContextDriverError::WrongOwner)?
            .execution_health();
        let payload = self
            .control_runtime
            .take_failure_payload(self.actor)
            .map_err(ContextDriverError::Driver)?
            .ok_or(ContextDriverError::WrongOwner)?;
        let (capture, job) = super::super::failure_tail::FailureTail::original(
            key,
            payload,
            self.ready_notice().clone(),
            health,
        );
        self.queue_failed_capture(
            original,
            FailurePhysicalPurpose::FinalCarrier,
            capture,
            job,
            ports,
        )?;
        Ok((false, false))
    }

    fn settle_failed_driver_effect(
        &mut self,
        original: &ScopeFailureOwner,
    ) -> Result<(), ContextDriverError> {
        let Some(ticket) = self.resources.pending_host_jobs.driver_effect.take() else {
            return Ok(());
        };
        let stage = original.drained().map_err(ContextDriverError::Failure)?;
        if matches!(
            &ticket.effect,
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::RetireTask { .. }
                    | novarocks_worker_protocol::context::ContextEffect::FinalizeTaskExecution { .. }
            )
        ) {
            return match self.retain_failed_retirement_ticket(ticket) {
                Ok(()) => Ok(()),
                Err((error, ticket)) => {
                    self.resources.pending_host_jobs.driver_effect = Some(ticket);
                    if matches!(
                        error,
                        task_driver::TaskDriverError::EffectRequiresOriginalOwner
                    ) {
                        Ok(())
                    } else {
                        Err(ContextDriverError::Task(error))
                    }
                }
            };
        }
        let result = match &ticket.effect {
            WorkerAction::Reply(_) => self.finish_failed_reply_effect(&stage, ticket),
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::Subscribe { .. },
            ) => self.settle_failed_subscribe(&stage, ticket),
            // A refused mechanical read preparation never ran a getter.
            // Its reply was superseded by this real failure session; any
            // already-installed Ready pin remains with the original R cell.
            // The Started ticket could not have released its dependent reply.
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::PrepareReadPayload { .. },
            ) => self.complete_host_effect(ticket).map_err(|refusal| {
                (
                    crate::control_runtime::context_host::requests::ContextRequestError::Driver(
                        refusal.error,
                    ),
                    refusal.ticket,
                )
            }),
            // Every Task publication was positively superseded by the same
            // failure session before this stage. These never-dispatched
            // metadata handlers own no foreign body; original Source custody
            // remains in the Task and is independently drained above.
            WorkerAction::Context(
                novarocks_worker_protocol::context::ContextEffect::PublishTask { .. }
                | novarocks_worker_protocol::context::ContextEffect::PublishMetrics { .. }
                | novarocks_worker_protocol::context::ContextEffect::PublishTaskConvergence {
                    ..
                }
                | novarocks_worker_protocol::context::ContextEffect::PublishConclusion { .. }
                | novarocks_worker_protocol::context::ContextEffect::PublishContextConvergence {
                    ..
                },
            ) => self.complete_host_effect(ticket).map_err(|refusal| {
                (
                    crate::control_runtime::context_host::requests::ContextRequestError::Driver(
                        refusal.error,
                    ),
                    refusal.ticket,
                )
            }),
            _ => {
                self.resources.pending_host_jobs.driver_effect = Some(ticket);
                return Ok(());
            }
        };
        if let Err((error, ticket)) = result {
            self.resources.pending_host_jobs.driver_effect = Some(ticket);
            return Err(ContextDriverError::Request(error));
        }
        Ok(())
    }
}
