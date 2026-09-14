// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Product-owned process lifecycle for materialized-view activity and workers.

use std::time::Instant;

use crate::activity::{
    MvActivityAdmissionError, MvActivityGate, MvActivityLease, MvActivityOwner, MvActivityTicket,
};
use crate::process_runtime::{
    MvBackgroundRuntime, MvBackgroundRuntimeLifecycleError, MvBackgroundRuntimeOwner,
    MvBackgroundRuntimeStart,
};
use crate::product::MvTarget;

/// The one process-local MV owner for activity admission and background-worker
/// lifecycle. Hosts may inject effect callbacks, but cannot own a parallel
/// gate, shutdown state, or worker supervisor.
#[derive(Default)]
pub struct MvProductService {
    activity_gate: MvActivityGate,
    background: MvBackgroundRuntimeOwner,
}

impl MvProductService {
    pub fn acquire_foreground(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground(target, owner, cancelled)
    }

    /// Register background work with the shared product FIFO gate. The host
    /// retains only its effect callback and must release the returned ticket or
    /// lease; the product retains the state machine itself.
    pub fn request_activity(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, crate::activity::MvActivityGateError> {
        self.activity_gate.request(target, owner)
    }

    /// A host callback may use this capability only to register or complete
    /// product work; the gate state remains owned by this service.
    pub fn activity_gate(&self) -> MvActivityGate {
        self.activity_gate.clone()
    }

    pub fn begin_background_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        self.background.begin_start()
    }

    pub fn begin_stopping(&self) {
        self.activity_gate.begin_stopping();
    }

    pub async fn shutdown_background_workers_until(&self, deadline: Instant) -> Result<(), String> {
        self.background.shutdown_until(deadline).await
    }

    pub fn request_background_stop_for_process_exit(&self) {
        self.background.request_stop_for_process_exit();
    }
}

#[cfg(test)]
mod tests {
    use super::MvProductService;
    use crate::activity::{CanonicalMvTarget, MvActivityGateError, MvActivityOwner};

    #[test]
    fn stopping_product_service_rejects_new_background_activity() {
        let service = MvProductService::default();
        service.begin_stopping();

        assert!(matches!(
            service.request_activity(
                CanonicalMvTarget::from_parts(Some("ice"), "sales", "mv_orders"),
                MvActivityOwner::ScheduledRefresh,
            ),
            Err(MvActivityGateError::Stopping)
        ));
    }
}
