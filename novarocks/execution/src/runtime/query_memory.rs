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
//! Task-hosted account capability transport; this module performs no grant.
use novarocks_execution_contract::TaskIdentity;
use novarocks_memory::{Account, CapacityError, MemoryAuthority};
use novarocks_types::identity::QueryExecutionId;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryMemoryBindingError {
    OwnerAttemptMismatch,
    TaskAttemptMismatch,
    MissingTaskIdentity,
    QueryIdentityMismatch,
    MissingExecutionRuntime,
    Authority(CapacityError),
}
impl std::fmt::Display for QueryMemoryBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OwnerAttemptMismatch => f.write_str("query memory owner attempt mismatch"),
            Self::TaskAttemptMismatch => f.write_str("query memory task attempt mismatch"),
            Self::MissingTaskIdentity => f.write_str("query memory binding requires task identity"),
            Self::QueryIdentityMismatch => {
                f.write_str("query memory invocation query identity mismatch")
            }
            Self::MissingExecutionRuntime => {
                f.write_str("query memory binding requires execution runtime")
            }
            Self::Authority(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for QueryMemoryBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Authority(cause) => Some(cause),
            _ => None,
        }
    }
}

/// Supplied by the exact context owner. Clones reference one account; they
/// neither duplicate redemption rights nor establish an invocation scope.
#[derive(Clone)]
pub struct QueryMemoryBinding {
    execution: QueryExecutionId,
    authority: Arc<MemoryAuthority>,
    account: Account,
}
impl QueryMemoryBinding {
    pub fn try_new(
        execution: QueryExecutionId,
        authority: Arc<MemoryAuthority>,
        account: Account,
    ) -> Result<Self, QueryMemoryBindingError> {
        authority
            .validate_account(&account)
            .map_err(QueryMemoryBindingError::Authority)?;
        Ok(Self {
            execution,
            authority,
            account,
        })
    }
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    pub fn authority(&self) -> &Arc<MemoryAuthority> {
        &self.authority
    }
    pub fn account(&self) -> &Account {
        &self.account
    }
    pub fn validate_task(
        &self,
        identity: TaskIdentity,
        runtime_authority: &Arc<MemoryAuthority>,
    ) -> Result<(), QueryMemoryBindingError> {
        if identity.query_execution_id() != self.execution {
            return Err(QueryMemoryBindingError::TaskAttemptMismatch);
        }
        if !Arc::ptr_eq(runtime_authority, &self.authority) {
            return Err(QueryMemoryBindingError::Authority(CapacityError::Invalid {
                detail: "account belongs to another authority",
            }));
        }
        runtime_authority
            .validate_account(&self.account)
            .map_err(QueryMemoryBindingError::Authority)
    }
}
#[cfg(test)]
#[path = "query_memory_tests.rs"]
mod tests;

impl novarocks_failure_contract::FailureProjection for QueryMemoryBindingError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::OwnerAttemptMismatch => ExecutionReason::QueryMemoryOwnerAttempt,
            Self::TaskAttemptMismatch => ExecutionReason::QueryMemoryTaskAttempt,
            Self::MissingTaskIdentity => ExecutionReason::QueryMemoryMissingTask,
            Self::QueryIdentityMismatch => ExecutionReason::QueryMemoryQueryIdentity,
            Self::MissingExecutionRuntime => ExecutionReason::QueryMemoryMissingRuntime,
            Self::Authority(cause) => {
                return novarocks_memory_failure::CapacityFailureProjection::new(cause)
                    .failure_fact();
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<QueryMemoryBindingError> for crate::runtime::task::ExecutionFailure {
    fn from(cause: QueryMemoryBindingError) -> Self {
        Self::from_domain(cause)
    }
}
