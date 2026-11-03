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

//! The terminal action maps its unique real exit to one admitted causal input.
//! Work, report and refused-input custody are shared with lifecycle authors.

use super::{
    causal::{TaskCausalInbox, TaskCausalSender},
    causal_input::{CausalInputBinding, CausalInputCustody, InputAuthor},
};
use crate::{
    task_completion_set::AdmittedCompletionTransfer,
    task_lifecycle::{CompletionExitCapability, CompletionExitNotice},
    task_port::TaskPendingNotice,
};
use novarocks_execution::runtime::execution_health::ExecutionHealth;
use novarocks_execution_contract::TaskIdentity;
use novarocks_worker_protocol::{participation::ParticipationInput, proposal_summary::CutId};
use std::{ops::Deref, sync::Arc};

#[derive(Clone)]
#[must_use = "The original terminal action retains its causal input custody"]
pub(crate) struct CompletionActionCustody(pub(super) CausalInputCustody);
impl Deref for CompletionActionCustody {
    type Target = CausalInputCustody;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl CompletionActionCustody {
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        admitted: AdmittedCompletionTransfer,
        inbox: &mut TaskCausalInbox,
        cut: CutId,
        sender: TaskCausalSender,
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> (Self, CompletionExitNotice) {
        let (original, author) = CausalInputCustody::from_admitted(
            admitted,
            inbox,
            cut,
            sender,
            CausalInputBinding {
                task,
                pending,
                health,
                unresolved: novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
                ),
            },
        );
        let notice =
            CompletionExitNotice::from_admitted_capability(task, Box::new(Exit { task, author }));
        (Self(original), notice)
    }
}
struct Exit {
    task: TaskIdentity,
    author: InputAuthor,
}
impl CompletionExitCapability for Exit {
    fn exited(self: Box<Self>) {
        let Self { task, author } = *self;
        let _reported = author.publish(ParticipationInput::CompletionExited { task });
    }
    fn withdrawn(self: Box<Self>) {
        let _reported = self.author.withdraw();
    }
    fn revoke_unused(self: Box<Self>) {
        let _reported = self.author.withdraw();
    }
    fn unresolved(self: Box<Self>) {
        self.author.unresolved();
    }
}
