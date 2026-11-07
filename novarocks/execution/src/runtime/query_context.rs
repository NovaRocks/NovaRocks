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
//! Resources of one admitted backend Participation. There is no process table,
//! expiry thread, task-key routing, or count-driven context removal here.

use super::{
    mem_tracker::{MemTracker, MemTrackerLimitInstallError},
    query_memory::{QueryMemoryBinding, QueryMemoryBindingError},
};
use novarocks_memory::{Account, CapacityError};
use novarocks_types::QueryExecutionId;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Exact account ownership carried by the Participation's original resource owner.
#[derive(Clone, Debug)]
pub struct QueryMemoryAccount {
    execution: QueryExecutionId,
    account: Account,
}
impl QueryMemoryAccount {
    pub fn new(execution: QueryExecutionId, account: Account) -> Self {
        Self { execution, account }
    }
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    pub fn account(&self) -> &Account {
        &self.account
    }
    pub fn into_account(self) -> Account {
        self.account
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryMemoryAccountError {
    Capacity(CapacityError),
}
impl std::fmt::Display for QueryMemoryAccountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity(error) => write!(f, "create query memory account: {error}"),
        }
    }
}
impl std::error::Error for QueryMemoryAccountError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Capacity(error) => Some(error),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for QueryMemoryAccountError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Capacity(error) => {
                novarocks_memory_failure::CapacityFailureProjection::new(error).failure_fact()
            }
        }
    }
}

pub struct QueryContext {
    execution: QueryExecutionId,
    tracker: Arc<MemTracker>,
    memory: QueryMemoryBinding,
    active_tasks: Arc<AtomicUsize>,
}
impl QueryContext {
    pub fn try_new(
        execution: QueryExecutionId,
        tracker: Arc<MemTracker>,
        memory: QueryMemoryBinding,
    ) -> Result<Self, QueryMemoryBindingError> {
        if memory.execution() != execution {
            return Err(QueryMemoryBindingError::OwnerAttemptMismatch);
        }
        Ok(Self {
            execution,
            tracker,
            memory,
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }
    pub fn tracker(&self) -> Arc<MemTracker> {
        self.tracker.clone()
    }
    pub fn memory_binding(&self) -> &QueryMemoryBinding {
        &self.memory
    }
    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Acquire)
    }
    pub fn validate_task_limit(
        &self,
        limit: Option<i64>,
    ) -> Result<(), MemTrackerLimitInstallError> {
        if let Some(limit) = limit {
            self.tracker.install_limit_once(limit)?;
        }
        Ok(())
    }
    pub fn task_guard(self: &Arc<Self>) -> QueryTaskGuard {
        self.active_tasks
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .expect("actual query Task count fits usize");
        QueryTaskGuard {
            context: Some(self.clone()),
            active_tasks: self.active_tasks.clone(),
        }
    }
}

/// One Task's actual resource reference. Completion owns this guard, rather
/// than terminal publication, a task-key directory, or a runtime alias.
pub struct QueryTaskGuard {
    context: Option<Arc<QueryContext>>,
    active_tasks: Arc<AtomicUsize>,
}
impl QueryTaskGuard {
    pub fn tracker(&self) -> Arc<MemTracker> {
        self.context
            .as_ref()
            .expect("live query Task guard")
            .tracker()
    }
    pub fn memory_binding(&self) -> &QueryMemoryBinding {
        self.context
            .as_ref()
            .expect("live query Task guard")
            .memory_binding()
    }
}
impl Drop for QueryTaskGuard {
    fn drop(&mut self) {
        // Drop this Task's original context reference before publishing the
        // diagnostic decrement. The counter survives independently for that order.
        drop(self.context.take());
        self.active_tasks
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_sub(1)
            })
            .expect("one actual query Task exit per guard");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_memory::{AccountKind, ExternalRef, MemoryAuthority};
    use novarocks_types::{AttemptId, QueryId};

    fn context() -> Arc<QueryContext> {
        let authority = Arc::new(
            MemoryAuthority::new(novarocks_memory::AuthorityConfig::new(
                1 << 24,
                1 << 23,
                1 << 23,
            ))
            .unwrap(),
        );
        let execution =
            QueryExecutionId::new(QueryId::new(78301, 78302), AttemptId::new(1).unwrap()).unwrap();
        let account = authority
            .create_account(AccountKind::Work, ExternalRef::new(78301, 78302))
            .unwrap();
        let binding = QueryMemoryBinding::try_new(execution, authority, account).unwrap();
        Arc::new(
            QueryContext::try_new(
                execution,
                MemTracker::new_root("original Participation query resources"),
                binding,
            )
            .unwrap(),
        )
    }

    #[test]
    fn task_guards_reference_one_original_context_and_exit_independently() {
        let context = context();
        let first = context.task_guard();
        let second = context.task_guard();
        assert_eq!(context.active_tasks(), 2);
        assert!(Arc::ptr_eq(&first.tracker(), &context.tracker()));
        assert_eq!(
            first.memory_binding().account().id(),
            second.memory_binding().account().id()
        );
        drop(first);
        assert_eq!(context.active_tasks(), 1);
        drop(second);
        assert_eq!(context.active_tasks(), 0);
        assert!(
            !context.memory_binding().account().is_retired(),
            "Task exit cannot release the Participation's original resources"
        );
    }

    #[test]
    fn owner_release_keeps_the_context_until_the_last_actual_task_guard_drop() {
        let context = context();
        let first = context.task_guard();
        let second = context.task_guard();
        let weak = Arc::downgrade(&context);
        let diagnostic = context.active_tasks.clone();
        drop(context);
        assert!(weak.upgrade().is_some());
        drop(first);
        assert_eq!(diagnostic.load(Ordering::Acquire), 1);
        assert!(weak.upgrade().is_some());
        drop(second);
        assert!(
            weak.upgrade().is_none(),
            "the last Task released its actual original context reference"
        );
        assert_eq!(diagnostic.load(Ordering::Acquire), 0);
    }

    #[test]
    fn task_limit_drift_and_other_attempt_binding_cannot_replace_the_owner() {
        let context = context();
        context.validate_task_limit(Some(1024)).unwrap();
        context.validate_task_limit(Some(1024)).unwrap();
        context.validate_task_limit(None).unwrap();
        assert!(context.validate_task_limit(Some(2048)).is_err());
        assert_eq!(context.tracker().limit(), 1024);
        assert_eq!(context.active_tasks(), 0);
        let other =
            QueryExecutionId::new(context.execution().query_id(), AttemptId::new(2).unwrap())
                .unwrap();
        assert!(matches!(
            QueryContext::try_new(other, context.tracker(), context.memory_binding().clone()),
            Err(QueryMemoryBindingError::OwnerAttemptMismatch)
        ));
        assert_ne!(context.execution(), other);
    }
}
