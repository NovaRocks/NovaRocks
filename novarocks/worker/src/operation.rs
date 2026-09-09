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

use std::time::Duration;

use novarocks_execution_contract::{MaxWait, OperationKind};

/// The process-local upper bounds a worker applies to requested waits.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperationWaitCaps {
    create: Duration,
    update: Duration,
}

impl OperationWaitCaps {
    pub const DEFAULT: Self = Self {
        create: MaxWait::DEFAULT_CREATE,
        update: MaxWait::DEFAULT_UPDATE,
    };

    pub fn new(create: Duration, update: Duration) -> Option<Self> {
        if create.is_zero() || update.is_zero() {
            return None;
        }
        Some(Self { create, update })
    }

    /// Returns the effective wait for one operation.
    pub fn clamp(self, kind: OperationKind, requested: MaxWait) -> Duration {
        let cap = match kind {
            OperationKind::AcquireQueryContextAdmissionTicket
            | OperationKind::CreateTask
            | OperationKind::UpdateQueryContext => self.create,
            _ => self.update,
        };
        requested.get().min(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::OperationWaitCaps;
    use novarocks_execution_contract::{MaxWait, OperationKind};
    use std::time::Duration;

    #[test]
    fn worker_clamps_each_dispatch_lane_to_its_local_limit() {
        let caps = OperationWaitCaps::new(Duration::from_secs(2), Duration::from_secs(1))
            .expect("positive caps");
        let requested = MaxWait::new(Duration::from_secs(5)).expect("representable wait");

        assert_eq!(
            caps.clamp(OperationKind::CreateTask, requested),
            Duration::from_secs(2)
        );
        assert_eq!(
            caps.clamp(OperationKind::UpdateTask, requested),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn zero_is_not_a_serviceable_wait_cap() {
        assert!(OperationWaitCaps::new(Duration::ZERO, Duration::from_secs(1)).is_none());
        assert!(OperationWaitCaps::new(Duration::from_secs(1), Duration::ZERO).is_none());
    }
}
