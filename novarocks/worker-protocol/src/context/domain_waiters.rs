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

//! Original admitted RPC domain responsibility, independent of its external
//! reply deadline. Each queued entry moves its neutral values; no host retry
//! receives a second command or a replacement causal cut.

use super::*;

#[derive(Debug)]
pub(super) enum TaskDomainWaiter {
    Request {
        request: InputId,
        payload: PayloadId,
        updates: Vec<TaskDomainInput>,
    },
    // A separate bounded continuation emits the source's completion notice.
    // It cannot add an output to the journal's existing maximum-sized step.
    Completed {
        request: InputId,
        payload: PayloadId,
    },
}
#[derive(Debug)]
pub(super) struct ContextDomainWaiter {
    pub(super) request: InputId,
    pub(super) payload: PayloadId,
    pub(super) update: ContextDomainInput,
}
#[derive(Debug)]
pub(super) struct ContextDomainComparison {
    original: ContextDomainWaiter,
    expected: CredentialState,
}

impl LiveContextCore {
    pub(super) fn context_domain_runnable(&self) -> bool {
        self.pending_context_update.is_none()
            && self.context_domain_comparison.is_none()
            && !self.context_domain_waiters.is_empty()
    }
    pub(super) fn refresh_task_domain_waiter(&mut self, task: TaskIdentity) {
        if !self.pending_task_updates.contains_key(&task)
            && self
                .tasks
                .get(&task)
                .is_some_and(|record| record.initial_creation.is_none())
            && self.task_domain_waiters.contains_key(&task)
        {
            self.runnable_task_domains.insert(task);
        }
    }
    pub(super) fn complete_task_domain_source(
        &mut self,
        task: TaskIdentity,
        request: InputId,
        payload: PayloadId,
    ) {
        self.task_domain_waiters
            .entry(task)
            .or_default()
            .push_front(TaskDomainWaiter::Completed { request, payload });
        self.runnable_task_domains.insert(task);
    }
    pub(super) fn advance_task_domain_waiter(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.runnable_task_domains.remove(&task);
        let queue = self
            .task_domain_waiters
            .get_mut(&task)
            .ok_or(CoreError::IllegalFact)?;
        let original = queue.pop_front().ok_or(CoreError::IllegalFact)?;
        if queue.is_empty() {
            self.task_domain_waiters.remove(&task);
        }
        match original {
            TaskDomainWaiter::Completed { request, payload } => {
                step.effects.push(ContextEffect::DomainRequestCompleted {
                    request,
                    payload,
                    task: Some(task),
                });
                self.refresh_task_domain_waiter(task);
                self.retire_if_ready(task, step)?;
            }
            TaskDomainWaiter::Request {
                request,
                payload,
                updates,
            } => {
                // This request already consumed its original cut. Starting
                // the journal never repeats task_request_cut/handle_request.
                self.start_task_domain(request, task, payload, updates, step)?;
            }
        }
        Ok(())
    }
    pub(super) fn advance_context_domain_waiter(
        &mut self,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if !self.context_domain_runnable() {
            return Err(CoreError::IllegalFact);
        }
        let ContextDomainWaiter {
            request,
            payload,
            update,
        } = self
            .context_domain_waiters
            .pop_front()
            .expect("original Context domain waiter");
        if !matches!(
            self.state,
            QueryContextState::Active | QueryContextState::Quiescing
        ) {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(self.closed_outcome())),
            );
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: None,
            });
            return Ok(());
        }
        self.start_context_domain(request, payload, update, step)
    }
    pub(super) fn recompare_context_credential(
        &mut self,
        request: InputId,
        payload: PayloadId,
        update: ContextDomainInput,
        step: &mut ContextStep,
    ) -> Result<bool, CoreError> {
        let ContextDomainInput::Credential(credential) = update else {
            return Ok(false);
        };
        let expected = self
            .context_domains
            .credential()
            .ok_or(CoreError::IllegalFact)?;
        if credential.current_resource == expected.resource {
            return Ok(false);
        }
        if self.context_domain_comparison.is_some() || self.pending_context_update.is_some() {
            return Err(CoreError::IllegalFact);
        }
        self.context_domain_comparison = Some(ContextDomainComparison {
            original: ContextDomainWaiter {
                request,
                payload,
                update,
            },
            expected,
        });
        step.effects
            .push(ContextEffect::RecompareContextCredential {
                request,
                payload,
                expected,
            });
        Ok(true)
    }
    pub(super) fn context_credential_compared(
        &mut self,
        request: InputId,
        expected: CredentialState,
        result: Result<bool, DomainRefusal>,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let original = self
            .context_domain_comparison
            .take()
            .ok_or(CoreError::IllegalFact)?;
        if original.original.request != request
            || original.expected != expected
            || self.context_domains.credential() != Some(expected)
        {
            self.context_domain_comparison = Some(original);
            return Err(CoreError::WrongIdentity);
        }
        let ContextDomainWaiter {
            payload,
            mut update,
            ..
        } = original.original;
        if !matches!(
            self.state,
            QueryContextState::Active | QueryContextState::Quiescing
        ) {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(self.closed_outcome())),
            );
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: None,
            });
            return Ok(());
        }
        match result {
            Ok(same_material) => {
                let ContextDomainInput::Credential(credential) = &mut update else {
                    return Err(CoreError::IllegalFact);
                };
                credential.current_resource = expected.resource;
                credential.same_material = same_material;
                self.start_context_domain(request, payload, update, step)?;
            }
            Err(failure) => {
                step.reply(request, ContextReplyBody::Domain(failure));
                step.effects.push(ContextEffect::DomainRequestCompleted {
                    request,
                    payload,
                    task: None,
                });
            }
        }
        Ok(())
    }
}
