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

//! The exact physical rejection receipt starts bounded neutral inventory
//! disposal. Its old identity remains occupied until every original member and
//! ready/deadline index is gone; no later same-identity attempt sees stale domains.

use super::*;

impl ContextCore {
    pub(super) fn begin_rejected_creation_retirement(
        &mut self,
        task: TaskIdentity,
        payload: PayloadId,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        if record.payload != payload
            || record.phase != CreationPhase::Retained
            || record.accepted()
            || record.pending_reply_outcome.is_none()
            || !record.waiting_replies.is_empty()
            || self.creation_reply_tasks.contains(&task)
            || record.initial_creation.is_some()
            || self.pending_task_updates.contains_key(&task)
            || self.task_domain_waiters.contains_key(&task)
            || record.scheduled_close_generation.is_some()
            || record.scheduled_quiesce_generation.is_some()
            || !record.proposals.is_empty()
            // Only installed Accepted creation commits this immutable vector.
            // A never-Accepted record must not own an arbitrary shared journal.
            || !record.initial_domain_receipts.is_empty()
        {
            return Err(CoreError::IllegalFact);
        }
        self.tasks
            .get_mut(&task)
            .expect("original rejected Task")
            .rejected_retirement_observed = true;
        self.continue_rejected_creation_retirement(task, step);
        Ok(())
    }

    fn continue_rejected_creation_retirement(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) {
        self.runnable_task_domains.insert(task);
        step.effects
            .push(ContextEffect::ContinueTaskDomain { task });
    }

    pub(super) fn advance_rejected_creation_retirement(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        if !record.rejected_retirement_observed
            || record.accepted()
            || record.phase != CreationPhase::Retained
            || record.initial_creation.is_some()
            || !record.waiting_replies.is_empty()
            || self.creation_reply_tasks.contains(&task)
            || self.pending_task_updates.contains_key(&task)
            || self.task_domain_waiters.contains_key(&task)
        {
            return Err(CoreError::IllegalFact);
        }
        if let Some(domains) = self.task_domains.get_mut(&task) {
            if domains.retire_one() {
                // All nested containers are empty before removing the row.
                self.task_domains.remove(&task);
            }
            step.records_visited += 1;
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        if self.dynamic_filters.remove(&task).is_some() {
            step.records_visited += 1;
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        if let Some(due) = self.metric_due.remove(&task) {
            step.records_visited += 1 + usize::from(self.metric_deadlines.remove(&(due, task)));
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        if self.metrics.remove(&task).is_some() {
            step.records_visited += 1;
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        if self
            .tasks
            .get_mut(&task)
            .expect("original rejected Task")
            .operator_statistics
            .pop()
            .is_some()
        {
            // One operator owns only its separately frozen fixed counter bound.
            step.records_visited += 1;
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        if let Some(info) = self.final_info.get(&task) {
            // A rejected loader never issued an execution statistics producer.
            // Closing may have published an empty final status; no shared
            // arbitrary statistics vector may be destroyed in this slice.
            if !info.operator_statistics().is_empty() {
                return Err(CoreError::IllegalFact);
            }
            self.final_info.remove(&task);
            step.records_visited += 1;
            self.continue_rejected_creation_retirement(task, step);
            return Ok(());
        }
        // Cursors are rotating metadata keys, not retained ownership. Old RPC
        // request_tasks/pending_replies retain their frozen bodies independently;
        // they neither authorize a new domain nor need a scan/rebinding here.
        self.runnable_task_domains.remove(&task);
        self.tasks.remove(&task);
        self.spent = self.spent.checked_sub(1).ok_or(CoreError::IllegalFact)?;
        Ok(())
    }
}
