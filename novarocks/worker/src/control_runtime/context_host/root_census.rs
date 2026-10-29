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

//! Read-only census of the original Context's registered Root owners.

use super::{ContextHostRecord, TaskHostOwner};
use crate::control_runtime::task::RootHandoffCensus;
use crate::root_result_channel::RootOwnershipSnapshot;
use novarocks_execution_contract::TaskIdentity;

impl ContextHostRecord {
    fn root_terminal_record(&self, task: TaskIdentity) -> Option<bool> {
        match self.tasks.get(&task) {
            Some(TaskHostOwner::Ready(owner) | TaskHostOwner::Initializing(owner)) => Some(
                owner
                    .resources_for_retirement()
                    .port
                    .publication()
                    .try_borrow()?
                    .status()
                    .is_terminal(),
            ),
            Some(TaskHostOwner::Unpublished(_)) => None,
            None => self
                .retired_task_reads
                .get(&task)
                .map(|read| read.status.is_terminal()),
        }
    }
    pub(in crate::control_runtime) fn try_root_ownership_snapshot(
        &self,
    ) -> Option<RootOwnershipSnapshot> {
        if self
            .resources
            .host_jobs
            .as_ref()
            .is_some_and(|jobs| !jobs.try_root_census_ready())
        {
            return None;
        }
        let mut total = self.resources.root_reads.try_ownership_snapshot()?;
        for task in self.resources.root_reads.installed_root_tasks() {
            total.terminal_task_records = total
                .terminal_task_records
                .checked_add(usize::from(self.root_terminal_record(task)?))?;
        }
        // Installation can publish its actual token before the next Context
        // host turn moves that same token into its original Root slot.
        for (task, owner) in &self.tasks {
            let owner = match owner {
                TaskHostOwner::Ready(owner) | TaskHostOwner::Initializing(owner) => owner,
                TaskHostOwner::Unpublished(_) => continue,
            };
            match owner.try_root_handoff_census()? {
                RootHandoffCensus::NoRoot => {}
                RootHandoffCensus::ContextOwned => {
                    if !self.resources.root_reads.has_root(*task) {
                        return None;
                    }
                }
                RootHandoffCensus::Pending(mut root) => {
                    if self.resources.root_reads.has_root(*task) {
                        return None;
                    }
                    root.terminal_task_records = usize::from(self.root_terminal_record(*task)?);
                    total.try_add(root)?;
                }
            }
        }
        Some(total)
    }
}
