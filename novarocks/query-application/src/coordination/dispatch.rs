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

use novarocks_execution_contract::OperationKind;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DispatchBudget {
    create_permits: usize,
    update_permits: usize,
    lifecycle_permits: usize,
}

impl DispatchBudget {
    pub const DEFAULT: Self = Self {
        create_permits: 16,
        update_permits: 12,
        lifecycle_permits: 4,
    };

    pub const fn new(create: usize, update: usize, lifecycle: usize) -> Option<Self> {
        if create == 0 || update == 0 || lifecycle == 0 {
            None
        } else {
            Some(Self {
                create_permits: create,
                update_permits: update,
                lifecycle_permits: lifecycle,
            })
        }
    }

    pub const fn create_permits(self) -> usize {
        self.create_permits
    }
    pub const fn update_permits(self) -> usize {
        self.update_permits
    }
    pub const fn lifecycle_permits(self) -> usize {
        self.lifecycle_permits
    }
    pub const fn total_permits(self) -> usize {
        self.create_permits + self.update_permits + self.lifecycle_permits
    }

    pub const fn lane_of(kind: OperationKind) -> DispatchLane {
        if matches!(
            kind,
            OperationKind::AcquireQueryContextAdmissionTicket
                | OperationKind::UpdateQueryContext
                | OperationKind::AbortQueryContext
                | OperationKind::ReleaseQueryContext
        ) {
            DispatchLane::Lifecycle
        } else if matches!(kind, OperationKind::CreateTask) {
            DispatchLane::Create
        } else {
            DispatchLane::Update
        }
    }

    pub const fn permits_for(self, lane: DispatchLane) -> usize {
        match lane {
            DispatchLane::Create => self.create_permits,
            DispatchLane::Update => self.update_permits,
            DispatchLane::Lifecycle => self.lifecycle_permits,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DispatchLane {
    Create,
    Update,
    Lifecycle,
}

/// Budgets whose authority belongs to query coordination.
///
/// Worker wait and lease caps remain Worker policy. Native queue and payload
/// bounds remain transport policy and are deliberately absent here.
#[derive(Clone, Copy, Debug)]
pub struct CoordinationBudgets {
    pub dispatch: DispatchBudget,
    pub status_subscription_error_budget: u32,
}

impl CoordinationBudgets {
    pub const DEFAULT: Self = Self {
        dispatch: DispatchBudget::DEFAULT,
        status_subscription_error_budget: DEFAULT_STATUS_SUBSCRIPTION_ERROR_BUDGET,
    };
}

pub const DEFAULT_STATUS_SUBSCRIPTION_ERROR_BUDGET: u32 = 8;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_operations_have_reserved_dispatch() {
        assert_eq!(
            DispatchBudget::lane_of(OperationKind::ReleaseQueryContext),
            DispatchLane::Lifecycle
        );
        assert_ne!(
            DispatchBudget::lane_of(OperationKind::CreateTask),
            DispatchLane::Lifecycle
        );
    }
}
