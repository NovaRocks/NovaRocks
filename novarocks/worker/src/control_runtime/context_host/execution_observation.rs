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

//! Known-neutral observations of actual Context execution custody.

use super::ContextHostRecord;
use crate::root_result_channel::RootOwnershipSnapshot;

impl ContextHostRecord {
    /// Permanent metadata, retained reads and H authorization are not an
    /// execution exit proof. M/R and every actual active custodian must retire.
    pub(in crate::control_runtime) fn try_execution_observation(
        &self,
    ) -> Option<(bool, RootOwnershipSnapshot)> {
        let callbacks = self
            .resources
            .host_jobs
            .as_ref()?
            .try_execution_callbacks_quiescent()?;
        let driver = self.task_driver.try_is_idle()?;
        let roots = self.try_root_ownership_snapshot()?;
        let idle = callbacks
            && driver
            && self.tasks.is_empty()
            && self.resources.requests.is_empty()
            && self.resources.request_partials.is_empty()
            && self.resources.routes.is_empty()
            && self.resources.failure.is_none();
        Some((idle, roots))
    }
}

#[cfg(test)]
impl ContextHostRecord {
    pub(crate) fn print_original_execution_inventory(&self) {
        eprintln!(
            "Context original execution scope={:?} callbacks={:?} root_census_ready={:?} raw_root_census={:?} observed_roots={:?} driver_idle={:?} tasks={} requests={} partials={} routes={} failure={} parent_authorized={} internal={:?}",
            self.resources.scope.as_ref().map(|scope| scope.snapshot()),
            self.resources
                .host_jobs
                .as_ref()
                .map(|jobs| jobs.try_execution_callbacks_quiescent()),
            self.resources
                .host_jobs
                .as_ref()
                .map(|jobs| jobs.try_root_census_ready()),
            self.resources.root_reads.try_ownership_snapshot(),
            self.try_root_ownership_snapshot(),
            self.task_driver.try_is_idle(),
            self.tasks.len(),
            self.resources.requests.len(),
            self.resources.request_partials.len(),
            self.resources.routes.len(),
            self.resources.failure.is_some(),
            self.resources.teardown.parent_retire_authorized,
            self.control_runtime.role_inventory(self.actor, None, 8)
        );
        eprintln!(
            "Context original requests={:?}",
            self.original_request_inventory_for_test()
        );
    }
}
