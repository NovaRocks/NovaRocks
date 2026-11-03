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

use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::task::error::LocalTaskExecutionError;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskCancelDetail {
    detail: novarocks_failure_contract::BoundedDetail,
}

impl TaskCancelDetail {
    pub fn new(detail: impl std::fmt::Display) -> Self {
        Self {
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        }
    }

    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalTaskOutcome {
    Succeeded,
    Failed(LocalTaskExecutionError),
    Cancelled { reason: TaskCancelDetail },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTaskTerminalFact {
    query_id: QueryId,
    task_key: UniqueId,
    outcome: LocalTaskOutcome,
    profile: Option<RuntimeProfileTree>,
}

impl LocalTaskTerminalFact {
    pub fn new(
        query_id: QueryId,
        task_key: UniqueId,
        outcome: LocalTaskOutcome,
        profile: Option<RuntimeProfileTree>,
    ) -> Self {
        Self {
            query_id,
            task_key,
            outcome,
            profile,
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn task_key(&self) -> UniqueId {
        self.task_key
    }

    pub const fn outcome(&self) -> &LocalTaskOutcome {
        &self.outcome
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.as_ref()
    }
}

impl std::fmt::Display for TaskCancelDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for TaskCancelDetail {}
impl novarocks_failure_contract::FailureProjection for TaskCancelDetail {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::stop_observation(
            crate::runtime::task::ExecutionReason::FragmentStopRequested.reason(),
        )
    }
}
impl From<TaskCancelDetail> for crate::runtime::task::ExecutionFailure {
    fn from(source: TaskCancelDetail) -> Self {
        Self::from_domain(source)
    }
}
