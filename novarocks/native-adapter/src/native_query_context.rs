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

//! Process-supplied resource construction and exact Participation references.
//! This object owns no query directory, independent query lifetime, expiry
//! service, registration counter, or task-key route.

use novarocks_execution::runtime::{
    mem_tracker::{self, MemTracker},
    profile::Profiler,
    query_context::{QueryContext, QueryMemoryAccount, QueryMemoryAccountError, QueryTaskGuard},
    query_memory::{QueryMemoryBinding, QueryMemoryBindingError},
    task::{
        LocalTaskPrepareContext,
        io::{ExchangeFrameTransmitter, TaskEventSink, TaskResultWriter},
    },
};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_memory::{AccountKind, ExternalRef, LimitDimension, MemoryAuthority};
use novarocks_types::{QueryExecutionId, UniqueId};
use novarocks_worker::sink_commit::WorkerSinkCommitPort;
use std::sync::Arc;

#[derive(Clone)]
pub struct NativeQueryContextRuntime {
    memory_authority: Arc<MemoryAuthority>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observation_controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}
impl NativeQueryContextRuntime {
    pub fn new(memory_authority: Arc<MemoryAuthority>) -> Self {
        Self {
            memory_authority,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
        }
    }
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    ) -> Self {
        self.test_controls = controls;
        self
    }
    #[cfg(novarocks_dev_controls)]
    pub fn test_controls(&self) -> Option<Arc<crate::test_controls::NativeTestControls>> {
        self.test_controls.clone()
    }
    #[cfg(novarocks_dev_controls)]
    pub fn with_observation_controls(
        mut self,
        controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    ) -> Self {
        self.observation_controls = controls;
        self
    }
    #[cfg(novarocks_dev_controls)]
    pub fn observation_controls(
        &self,
    ) -> Option<Arc<crate::test_observation_controls::NativeObservationControls>> {
        self.observation_controls.clone()
    }

    /// Called by the original Participation instance during materialization.
    /// The returned owner remains in that instance until its explicit Release.
    pub(crate) fn establish_query_resources(
        &self,
        execution: QueryExecutionId,
        limit: Option<i64>,
    ) -> Result<Arc<QueryContext>, NativeTaskAdmissionError> {
        let query = execution.query_id();
        let tracker = MemTracker::new_child(
            mem_tracker::query_tracker_label(query.high(), query.low()),
            &mem_tracker::process_mem_tracker(),
        );
        if let Some(limit) = limit {
            tracker
                .install_limit_once(limit)
                .map_err(NativeTaskAdmissionError::Limit)?;
        }
        let account = self
            .memory_authority
            .create_account(
                AccountKind::Work,
                ExternalRef::new(query.high() as u64, query.low() as u64),
            )
            .map_err(|error| {
                NativeTaskAdmissionError::MemoryAccount(QueryMemoryAccountError::Capacity(error))
            })?;
        if let Some(limit) = limit {
            let bytes =
                u64::try_from(limit).map_err(|_| NativeTaskAdmissionError::InvalidLimit(limit))?;
            account.install_policy(bytes, LimitDimension::Work);
        }
        let memory = bind_query_memory(
            execution,
            self.memory_authority.clone(),
            QueryMemoryAccount::new(execution, account),
        )
        .map_err(NativeTaskAdmissionError::Binding)?;
        QueryContext::try_new(execution, tracker, memory)
            .map(Arc::new)
            .map_err(NativeTaskAdmissionError::Binding)
    }
    fn validate_limit(
        &self,
        context: &QueryContext,
        limit: Option<i64>,
    ) -> Result<(), NativeTaskAdmissionError> {
        context
            .validate_task_limit(limit)
            .map_err(NativeTaskAdmissionError::Limit)?;
        if let Some(limit) = limit {
            let bytes =
                u64::try_from(limit).map_err(|_| NativeTaskAdmissionError::InvalidLimit(limit))?;
            context
                .memory_binding()
                .account()
                .install_policy(bytes, LimitDimension::Work);
        }
        Ok(())
    }
    /// A reference supplied by the original instance, never a key-only lookup
    /// and never a task-side context creation or replacement.
    pub(crate) fn prepare_query_memory_typed(
        &self,
        context: &Arc<QueryContext>,
        limit: Option<i64>,
    ) -> Result<NativeQueryPreparationMemory, NativeTaskAdmissionError> {
        self.validate_limit(context, limit)?;
        Ok(NativeQueryPreparationMemory {
            query_memory: context.memory_binding().clone(),
            query_mem_tracker: context.tracker(),
            query_task: context.task_guard(),
        })
    }
    pub(crate) fn type_construction_tracker(
        &self,
        context: &QueryContext,
        limit: Option<i64>,
    ) -> Result<Arc<MemTracker>, NativeTaskAdmissionError> {
        self.validate_limit(context, limit)?;
        Ok(context.tracker())
    }
    pub(crate) fn prepare_admission_with_memory(
        &self,
        task_key: UniqueId,
        memory: NativeQueryPreparationMemory,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> NativeTaskAdmissionResources {
        let NativeQueryPreparationMemory {
            query_memory,
            query_mem_tracker,
            query_task,
        } = memory;
        let task_mem_tracker = MemTracker::new_child(
            format!("task_{:x}_{:x}", task_key.high(), task_key.low()),
            &query_mem_tracker,
        );
        NativeTaskAdmissionResources {
            query_memory: Some(query_memory),
            query_task: Some(query_task),
            query_mem_tracker,
            task_mem_tracker,
            runtime_filter,
        }
    }
}

#[derive(Debug)]
pub(crate) enum NativeTaskAdmissionError {
    Limit(novarocks_execution::runtime::mem_tracker::MemTrackerLimitInstallError),
    InvalidLimit(i64),
    MemoryAccount(QueryMemoryAccountError),
    Binding(QueryMemoryBindingError),
}
impl std::fmt::Display for NativeTaskAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit(error) => error.fmt(f),
            Self::InvalidLimit(limit) => {
                write!(f, "query memory limit must not be negative: {limit}")
            }
            Self::MemoryAccount(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for NativeTaskAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limit(error) => Some(error),
            Self::MemoryAccount(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::InvalidLimit(_) => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativeTaskAdmissionError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        match self {
            Self::Limit(error) => error.failure_fact(),
            Self::MemoryAccount(error) => error.failure_fact(),
            Self::Binding(error) => error.failure_fact(),
            // 830 and 831 are retired and must not be reused (MissingTracker, MissingBinding).
            Self::InvalidLimit(_) => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(832).unwrap()),
            ),
        }
    }
}
pub(crate) struct NativeQueryPreparationMemory {
    query_memory: QueryMemoryBinding,
    query_mem_tracker: Arc<MemTracker>,
    query_task: QueryTaskGuard,
}
impl NativeQueryPreparationMemory {
    pub(crate) fn binding(&self) -> &QueryMemoryBinding {
        &self.query_memory
    }
}

pub struct NativeTaskAdmissionResources {
    query_memory: Option<QueryMemoryBinding>,
    query_task: Option<QueryTaskGuard>,
    query_mem_tracker: Arc<MemTracker>,
    task_mem_tracker: Arc<MemTracker>,
    runtime_filter: Option<RuntimeFilterSessionRef>,
}

impl NativeTaskAdmissionResources {
    pub fn query_memory(&self) -> Option<&QueryMemoryBinding> {
        self.query_memory.as_ref()
    }
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn task_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.task_mem_tracker)
    }

    pub fn into_prepare_context(
        self,
        profiler: Option<Profiler>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn TaskResultWriter>,
        event_sink: Arc<dyn TaskEventSink>,
        identity: novarocks_execution_contract::TaskIdentity,
        control: Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>,
        completion: novarocks_execution_contract::AdmittedWorkParts<
            novarocks_execution::runtime::task::ExecutionFailure,
        >,
    ) -> LocalTaskPrepareContext {
        #[cfg(novarocks_dev_controls)]
        let query_tracker_witness = Arc::downgrade(&self.query_mem_tracker);
        let mut context = LocalTaskPrepareContext::new(
            profiler,
            Some(self.task_mem_tracker),
            self.runtime_filter,
            exchange_transmitter,
            result_writer,
            event_sink,
            identity,
            control,
            completion,
        )
        .with_query_memory(self.query_memory)
        .with_task_commit_port(Arc::new(WorkerSinkCommitPort));
        if let Some(guard) = self.query_task {
            context = context.with_query_task_guard(guard);
        }
        #[cfg(novarocks_dev_controls)]
        let context = context.with_query_tracker_witness(query_tracker_witness);
        context
    }
}

// Account identity is evidence from the actual resource owner, not a new grant.
fn bind_query_memory(
    execution: QueryExecutionId,
    authority: Arc<MemoryAuthority>,
    owner: QueryMemoryAccount,
) -> Result<QueryMemoryBinding, QueryMemoryBindingError> {
    if owner.execution() != execution {
        return Err(QueryMemoryBindingError::OwnerAttemptMismatch);
    }
    QueryMemoryBinding::try_new(execution, authority, owner.into_account())
}
#[cfg(test)]
#[path = "native_query_memory_transport_tests.rs"]
mod memory_transport_tests;
