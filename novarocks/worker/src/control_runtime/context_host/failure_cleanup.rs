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

//! Transfer the original failed batch cleanup into its actual resource owner.

use super::*;
use crate::control_runtime::adopted_cleanup::AdoptedCleanupEffect;
use crate::control_runtime::context_host::ContextDriverError;
use novarocks_worker_protocol::context::ContextEffect;

impl ContextHostRecord {
    /// Finish this finite binding pass before Task source/header retirement.
    /// No new inputs can be committed by this already-failed Core.
    pub(super) fn bind_failed_cleanup(
        &mut self,
        original: &mut ScopeFailureOwner,
        budget: usize,
    ) -> Result<bool, ContextDriverError> {
        if let Some(ticket) = self.resources.pending_host_jobs.driver_effect.take() {
            if ticket.descriptor.kind.allowed_after_failure() {
                if let Err((error, ticket)) = self.adopt_failed_cleanup(original, ticket) {
                    self.resources.pending_host_jobs.driver_effect = Some(ticket);
                    return Err(error);
                }
            } else {
                self.resources.pending_host_jobs.driver_effect = Some(ticket);
            }
        }
        for _ in 0..budget {
            match original.cleanup_bindings {
                CleanupBindingCursor::Bound => return Ok(true),
                CleanupBindingCursor::Batches(after) => {
                    let inputs = self
                        .control_runtime
                        .pending_batches(self.actor, after, 1)
                        .map_err(ContextDriverError::Driver)?;
                    match inputs.first().copied() {
                        Some(input)
                            if original
                                .ticket
                                .as_ref()
                                .is_some_and(|ticket| ticket.input == input) =>
                        {
                            original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                        }
                        Some(input) => {
                            original.cleanup_bindings = CleanupBindingCursor::Effects(input, None)
                        }
                        None => original.cleanup_bindings = CleanupBindingCursor::Bound,
                    }
                }
                CleanupBindingCursor::Effects(input, after) => {
                    let effects = match self
                        .control_runtime
                        .effect_inventory(self.actor, input, after, 1)
                    {
                        Ok(effects) => effects,
                        Err(DriverError::UnknownInput) => {
                            original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                            continue;
                        }
                        Err(error) => return Err(ContextDriverError::Driver(error)),
                    };
                    let Some((descriptor, state)) = effects.first() else {
                        original.cleanup_bindings = CleanupBindingCursor::Batches(Some(input));
                        continue;
                    };
                    if *state == EffectState::Pending && descriptor.kind.allowed_after_failure() {
                        let ticket = original
                            .session
                            .take_cleanup(input, descriptor.id)
                            .map_err(ContextDriverError::Driver)?;
                        if let Err((error, ticket)) = self.adopt_failed_cleanup(original, ticket) {
                            // This is the same exclusive ticket, retained at
                            // the existing dispatch-refusal position.
                            assert!(self.resources.pending_host_jobs.driver_effect.is_none());
                            self.resources.pending_host_jobs.driver_effect = Some(ticket);
                            return Err(error);
                        }
                    }
                    original.cleanup_bindings =
                        CleanupBindingCursor::Effects(input, Some(descriptor.id));
                }
            }
        }
        Ok(matches!(
            original.cleanup_bindings,
            CleanupBindingCursor::Bound
        ))
    }

    fn adopt_failed_cleanup(
        &mut self,
        original: &ScopeFailureOwner,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), (ContextDriverError, EffectTicket<WorkerAction>)> {
        let stage = match original.drained() {
            Ok(stage) => stage,
            Err(error) => return Err((ContextDriverError::Failure(error), ticket)),
        };
        if let Err(error) = stage.validate(&self.control_runtime, self.actor) {
            return Err((ContextDriverError::Driver(error), ticket));
        }
        match &ticket.effect {
            WorkerAction::Context(ContextEffect::SealRoot { context, .. })
                if *context == self.identity =>
            {
                if self.resources.pending_host_jobs.driver_seal.is_some() {
                    return Err((ContextDriverError::Driver(DriverError::RoleBusy), ticket));
                }
                self.resources.pending_host_jobs.driver_seal = Some(ticket);
                Ok(())
            }
            WorkerAction::Context(ContextEffect::ReleaseHost { .. }) => {
                if self.resources.pending_host_jobs.driver_release.is_some() {
                    return Err((ContextDriverError::Driver(DriverError::RoleBusy), ticket));
                }
                self.resources.pending_host_jobs.driver_release = Some(ticket);
                Ok(())
            }
            WorkerAction::Context(ContextEffect::CancelLoader { task, .. }) => {
                let Some(owner) = self.task(*task) else {
                    return Err((ContextDriverError::WrongOwner, ticket));
                };
                // This field is the original loader custodian; it is never
                // cleared while the Task remains owned. None means LoadInput
                // was never handed to a loader, rather than inferred exit.
                if let Some(loader) = owner.loader() {
                    loader.cancel();
                }
                self.complete_host_effect(ticket)
                    .map_err(|refusal| (ContextDriverError::Driver(refusal.error), refusal.ticket))
            }
            WorkerAction::Context(
                ContextEffect::StopTask { task, .. }
                | ContextEffect::DisposeSource { task, .. }
                | ContextEffect::DisposeLoadedInput { task, .. },
            ) => {
                let notice = self.request_notice_for_input(ticket.input);
                let runtime = self.control_runtime.clone();
                let Some(owner) = self.task(*task) else {
                    return Err((ContextDriverError::WrongOwner, ticket));
                };
                if matches!(&ticket.effect, WorkerAction::Context(ContextEffect::DisposeLoadedInput { payload, .. }) if *payload != owner.original_payload_id())
                {
                    return Err((ContextDriverError::WrongOwner, ticket));
                }
                let cleanup = AdoptedCleanupEffect::from_original(&runtime, &stage, ticket, notice)
                    .map_err(|(error, ticket)| (ContextDriverError::Driver(error), ticket))?;
                match cleanup.effect() {
                    WorkerAction::Context(ContextEffect::StopTask { .. }) => owner
                        .host_call(crate::control_runtime::task_host_call::TaskHostCallKind::Stop)
                        .adopt_failed_stop_effect(&stage, cleanup)
                        .map_err(|(error, cleanup)| {
                            (
                                ContextDriverError::Task(task_driver::TaskDriverError::HostCall(
                                    error,
                                )),
                                cleanup.into_ticket(),
                            )
                        }),
                    WorkerAction::Context(ContextEffect::DisposeSource { .. }) => owner
                        .port()
                        .ingress()
                        .adopt_source_cleanup(&stage, cleanup)
                        .map_err(|(error, cleanup)| {
                            (ContextDriverError::Driver(error), cleanup.into_ticket())
                        }),
                    WorkerAction::Context(ContextEffect::DisposeLoadedInput { .. }) => {
                        let Some(loader) = owner.loader() else {
                            return Err((ContextDriverError::WrongOwner, cleanup.into_ticket()));
                        };
                        loader
                            .adopt_input_cleanup(&stage, cleanup)
                            .map_err(|(error, cleanup)| {
                                (ContextDriverError::Driver(error), cleanup.into_ticket())
                            })
                    }
                    _ => unreachable!("selected original resource cleanup"),
                }
            }
            _ => Err((ContextDriverError::WrongOwner, ticket)),
        }
    }
}
