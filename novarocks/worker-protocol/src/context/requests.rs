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

//! Original RPC state survives execution compaction. A ready body retains
//! its original decision time; new requests never borrow an older R's result.

use super::*;

#[derive(Debug, Default)]
pub(super) struct RequestState {
    // Keep the complete original request, including its neutral scoped author;
    // a later timer/continuation never substitutes its own work for this R.
    pub(super) requests: BTreeMap<InputId, RequestContext>,
    pub(super) request_tasks: BTreeMap<InputId, TaskIdentity>,
    pub(super) request_deadlines: BTreeSet<(MonotonicInstant, InputId)>,
    pub(super) pending_replies: BTreeMap<InputId, PendingReply>,
    pub(super) task_read_waiters: BTreeMap<InputId, TaskReadCommand>,
}
impl RequestState {
    pub(super) fn await_task_read(
        &mut self,
        token: InputId,
        command: TaskReadCommand,
    ) -> Result<(), CoreError> {
        if !self.requests.contains_key(&token)
            || !self.request_tasks.contains_key(&token)
            || self.pending_replies.contains_key(&token)
            || self.task_read_waiters.contains_key(&token)
        {
            return Err(CoreError::WrongIdentity);
        }
        self.task_read_waiters.insert(token, command);
        Ok(())
    }
    /// Absence, a different Task, or an already selected decision never
    /// grants a late continuation permission to read or revive the old R.
    pub(super) fn take_task_read(
        &mut self,
        token: InputId,
        task: TaskIdentity,
    ) -> Option<(RequestContext, TaskReadCommand)> {
        if !self.task_read_waiters.contains_key(&token)
            || self.request_tasks.get(&token) != Some(&task)
            || self.pending_replies.contains_key(&token)
        {
            return None;
        }
        let request = *self.requests.get(&token)?;
        let command = self.task_read_waiters.remove(&token)?;
        Some((request, command))
    }
    pub(super) fn remove(&mut self, token: InputId) -> Option<RequestContext> {
        self.task_read_waiters.remove(&token);
        let request = self.requests.remove(&token)?;
        self.request_deadlines.remove(&(request.deadline, token));
        self.pending_replies.remove(&token);
        self.request_tasks.remove(&token);
        Some(request)
    }
    pub(super) fn flush(&mut self, token: InputId, step: &mut ContextStep) {
        if !step.requests.is_empty() {
            return;
        }
        let Some(reply) = self.pending_replies.remove(&token) else {
            return;
        };
        let task = self.request_tasks.get(&token).copied();
        let request = self.remove(token).expect("original ready request");
        step.requests
            .push(if reply.settled_at.has_reached(request.deadline) {
                ContextRequestSettlement::verdict(
                    request,
                    task,
                    RequestResult::TimeoutWhileAccepted,
                )
            } else {
                ContextRequestSettlement::new(request, task, reply.body)
            });
    }
    pub(super) fn complete(
        &mut self,
        token: InputId,
        body: Arc<ContextReplyBody>,
        settled_at: MonotonicInstant,
        step: &mut ContextStep,
    ) {
        if !self.requests.contains_key(&token) {
            return;
        }
        self.task_read_waiters.remove(&token);
        self.pending_replies
            .entry(token)
            .or_insert(PendingReply { body, settled_at });
        self.flush(token, step);
    }
    pub(super) fn settle(&mut self, now: MonotonicInstant, step: &mut ContextStep) -> usize {
        let decisions = std::mem::take(&mut step.decisions);
        let visited = decisions.len();
        for (token, body) in decisions {
            self.complete(token, body, now, step);
        }
        if let Some((&operation, _)) = self.pending_replies.first_key_value() {
            step.effects
                .push(ContextEffect::ContinueRequestReplies { operation });
        }
        visited
    }
    pub(super) fn expire(&mut self, now: MonotonicInstant, step: &mut ContextStep) {
        let Some((at, token)) = self.request_deadlines.first().copied() else {
            return;
        };
        if now < at {
            return;
        }
        if self
            .pending_replies
            .get(&token)
            .is_some_and(|reply| reply.settled_at < at)
        {
            self.flush(token, step);
            return;
        }
        let task = self.request_tasks.get(&token).copied();
        let request = self.remove(token).expect("indexed original request");
        step.requests.push(ContextRequestSettlement::verdict(
            request,
            task,
            RequestResult::TimeoutWhileAccepted,
        ));
    }
}
