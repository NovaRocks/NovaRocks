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

//! Narrow query-runtime services consumed by the native backend adapter.
//!
//! This facade deliberately exposes only the operations needed to compose a
//! native fragment around the protocol-neutral fragment kernel. The underlying
//! query manager remains private to core.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::profile::Profiler;
use novarocks_execution::runtime::query_memory::{QueryMemoryBinding, QueryMemoryBindingError};
use novarocks_execution::runtime::task::LocalTaskPrepareContext;
use novarocks_execution::runtime::task::io::{
    ExchangeFrameTransmitter, TaskEventSink, TaskResultWriter,
};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_memory::{LimitDimension, MemoryAuthority};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use novarocks_worker::query_context::{
    QueryContextManager, QueryExecutionKey, QueryMemoryAccount, QueryMemoryAccountError,
    query_context_manager,
};
use novarocks_worker::sink_commit::WorkerSinkCommitPort;

#[derive(Clone)]
pub struct NativeQueryContextRuntime {
    manager: Arc<QueryContextManager>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observation_controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    /// Clones must publish one current manager snapshot in a single order.
    resource_metrics_publish: Arc<Mutex<()>>,
    #[cfg(test)]
    resource_snapshot_observer: Option<
        Arc<
            dyn Fn(novarocks_worker::query_context::NativeQueryExecutionResourceSnapshot)
                + Send
                + Sync,
        >,
    >,
    /// The one memory capacity authority this OS process was given.
    ///
    /// The registry behind `manager` is a process-global singleton, so the
    /// authority is carried here instead: this type is constructed by the
    /// backend composition root, which is the only place entitled to hand it
    /// out.
    memory_authority: Arc<MemoryAuthority>,
}

impl NativeQueryContextRuntime {
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

    pub fn publish_resource_snapshot(&self) {
        let _publication = self
            .resource_metrics_publish
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _global_publication = crate::backend_metrics::NATIVE_QUERY_RESOURCE_SCRAPE_LOCK
            .lock()
            .expect("native query resource scrape lock");
        let snapshot = self.manager.native_execution_resource_snapshot();
        crate::backend_metrics::publish_backend_query_execution_resource(
            "native_query_contexts_active",
            snapshot.active_contexts,
        );
        crate::backend_metrics::publish_backend_query_execution_resource(
            "native_query_contexts_second_chance",
            snapshot.second_chance_contexts,
        );
        crate::backend_metrics::publish_backend_query_execution_resource(
            "native_query_active_tasks",
            snapshot.active_fragments,
        );
        #[cfg(test)]
        if let Some(observer) = &self.resource_snapshot_observer {
            observer(snapshot);
        }
    }
    pub fn global(memory_authority: Arc<MemoryAuthority>) -> Self {
        Self {
            manager: query_context_manager(),
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
            resource_metrics_publish: Arc::new(Mutex::new(())),
            #[cfg(test)]
            resource_snapshot_observer: None,
            memory_authority,
        }
    }

    #[cfg(any(test, novarocks_dev_controls))]
    pub fn new_for_test(
        manager: Arc<QueryContextManager>,
        memory_authority: Arc<MemoryAuthority>,
    ) -> Self {
        Self {
            manager,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
            resource_metrics_publish: Arc::new(Mutex::new(())),
            #[cfg(test)]
            resource_snapshot_observer: None,
            memory_authority,
        }
    }

    #[cfg(test)]
    pub(crate) fn observe_resource_publication_for_test(
        mut self,
        observer: Arc<
            dyn Fn(novarocks_worker::query_context::NativeQueryExecutionResourceSnapshot)
                + Send
                + Sync,
        >,
    ) -> Self {
        self.resource_snapshot_observer = Some(observer);
        self
    }

    /// Compatibility boundary retaining the original full admission diagnostic.
    pub fn prepare_admission_execution(
        &self,
        execution_id: QueryExecutionId,
        task_key: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Result<NativeTaskAdmissionResources, String> {
        self.prepare_admission_execution_typed(
            execution_id,
            task_key,
            delivery_expire,
            query_expire,
            exec_mem_limit,
            runtime_filter,
        )
        .map_err(|error| error.to_string())
    }

    pub(crate) fn prepare_admission_execution_typed(
        &self,
        execution_id: QueryExecutionId,
        task_key: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Result<NativeTaskAdmissionResources, NativeTaskAdmissionError> {
        let (query_mem_tracker, query_memory) = self.establish_query_resources(
            execution_id,
            delivery_expire,
            query_expire,
            exec_mem_limit,
        )?;
        let fragment_label = format!("task_{:x}_{:x}", task_key.high(), task_key.low());
        let task_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        let resources = NativeTaskAdmissionResources {
            query_memory,
            query_mem_tracker,
            task_mem_tracker,
            runtime_filter,
        };
        Ok(resources)
    }

    /// Establish resources for an admitted native query context. Its original
    /// options own this policy, before RF facts or a first task decode types.
    /// The task protocol remains the lifecycle authority; this is no lookup
    /// fallback and adds no preparation quota.
    pub(crate) fn establish_query_resources(
        &self,
        execution_id: QueryExecutionId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
    ) -> Result<(Arc<MemTracker>, Option<QueryMemoryBinding>), NativeTaskAdmissionError> {
        let execution = execution_key(execution_id);
        self.manager
            .ensure_native_context_execution(execution, false, delivery_expire, query_expire)
            .map_err(NativeTaskAdmissionError::Context)?;
        let query_mem_tracker = self
            .manager
            .query_mem_tracker_execution(execution)
            .ok_or(NativeTaskAdmissionError::MissingTracker)?;
        let query_memory_account = if let Some(limit) = exec_mem_limit {
            query_mem_tracker
                .install_limit_once(limit)
                .map_err(NativeTaskAdmissionError::Limit)?;
            // The same number, stated on both mechanisms. Charges still live
            // on the tracker, so the account's own `L` is near zero and this
            // policy refuses nothing yet; that is deliberate. As later slices
            // move charges onto accounts, the tracker side shrinks and the
            // account side grows, and their sum stays this one limit.
            let account = self
                .manager
                .ensure_query_memory_account(execution, &self.memory_authority)
                .map_err(NativeTaskAdmissionError::MemoryAccount)?;
            let limit_bytes =
                u64::try_from(limit).map_err(|_| NativeTaskAdmissionError::InvalidLimit(limit))?;
            account
                .account()
                .install_policy(limit_bytes, LimitDimension::Work);
            account
        } else {
            // Query-owned work always has its real account. None installs no
            // local policy and never clears an existing context's policy.
            self.manager
                .ensure_query_memory_account(execution, &self.memory_authority)
                .map_err(NativeTaskAdmissionError::MemoryAccount)?
        };
        let query_memory = Some(
            bind_query_memory(
                execution_id,
                Arc::clone(&self.memory_authority),
                query_memory_account,
            )
            .map_err(NativeTaskAdmissionError::Binding)?,
        );
        Ok((query_mem_tracker, query_memory))
    }

    /// Type decoding precedes runtime installation, but belongs to the already
    /// established BE query. Observe that same tracker and its frozen policy;
    /// never invent a default context or a second preparation byte quota.
    pub(crate) fn type_construction_tracker(
        &self,
        execution_id: QueryExecutionId,
        exec_mem_limit: Option<i64>,
    ) -> Result<Arc<MemTracker>, NativeTaskAdmissionError> {
        let tracker = self
            .manager
            .query_mem_tracker_execution(execution_key(execution_id))
            .ok_or(NativeTaskAdmissionError::MissingTracker)?;
        if let Some(limit) = exec_mem_limit {
            tracker
                .install_limit_once(limit)
                .map_err(NativeTaskAdmissionError::Limit)?;
        }
        Ok(tracker)
    }

    /// The SAME original query owner, obtained before compiled type births.
    /// The caller must hold this fragment's original registration lease.
    pub(crate) fn prepare_query_memory_typed(
        &self,
        execution_id: QueryExecutionId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
    ) -> Result<NativeQueryPreparationMemory, NativeTaskAdmissionError> {
        let (query_mem_tracker, query_memory) = self.establish_query_resources(
            execution_id,
            delivery_expire,
            query_expire,
            exec_mem_limit,
        )?;
        let query_memory = query_memory.ok_or(NativeTaskAdmissionError::MissingBinding)?;
        Ok(NativeQueryPreparationMemory {
            query_memory,
            query_mem_tracker,
        })
    }

    /// Consume the early binding rather than minting a second query account.
    pub(crate) fn prepare_admission_with_memory(
        &self,
        task_key: UniqueId,
        memory: NativeQueryPreparationMemory,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> NativeTaskAdmissionResources {
        let NativeQueryPreparationMemory {
            query_memory,
            query_mem_tracker,
        } = memory;
        let fragment_label = format!("task_{:x}_{:x}", task_key.high(), task_key.low());
        let task_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);

        NativeTaskAdmissionResources {
            query_memory: Some(query_memory),
            query_mem_tracker,
            task_mem_tracker,
            runtime_filter,
        }
    }

    pub(crate) fn register_task_execution(
        &self,
        execution_id: QueryExecutionId,
        task_key: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<NativeTaskRegistration, NativeTaskAdmissionError> {
        let execution = execution_key(execution_id);
        self.manager
            .get_or_register_native_execution(execution, false, delivery_expire, query_expire)
            .map_err(NativeTaskAdmissionError::Context)?;
        self.manager
            .register_native_finst_execution(task_key, execution)
            .map_err(NativeTaskAdmissionError::Context)?;
        let lease = NativeTaskRegistration {
            runtime: self.clone(),
            execution,
            task_key,
            active: true,
        };
        Ok(lease)
    }

    pub fn finish_task(&self, execution_id: QueryExecutionId) {
        self.manager
            .finish_fragment_execution(execution_key(execution_id));
    }

    pub fn retire_idle_execution(&self, execution_id: QueryExecutionId) -> bool {
        self.manager
            .retire_idle_native_execution(execution_key(execution_id))
    }

    pub fn unregister_task_execution(&self, execution_id: QueryExecutionId, task_key: UniqueId) {
        self.manager
            .unregister_finst_execution(task_key, execution_key(execution_id));
    }
}

pub struct NativeTaskRegistration {
    runtime: NativeQueryContextRuntime,
    execution: QueryExecutionKey,
    task_key: UniqueId,
    active: bool,
}

impl NativeTaskRegistration {
    pub fn into_running(mut self) {
        self.active = false;
    }
}

impl Drop for NativeTaskRegistration {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .runtime
                .manager
                .rollback_pre_ready_native_fragment_execution(self.execution, self.task_key);
            self.runtime.publish_resource_snapshot();
            self.active = false;
        }
    }
}

fn execution_key(execution_id: QueryExecutionId) -> QueryExecutionKey {
    QueryExecutionKey::native_attempt(
        QueryId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        NonZeroU64::new(execution_id.attempt_id().get())
            .expect("QueryExecutionId always has a nonzero attempt"),
    )
}

/// Exact host admission causes; memory refusals stay typed until task projection.
#[derive(Debug)]
pub(crate) enum NativeTaskAdmissionError {
    Context(novarocks_worker::query_context::QueryContextRegistrationError),
    Limit(novarocks_execution::runtime::mem_tracker::MemTrackerLimitInstallError),
    MissingTracker,
    MissingBinding,
    InvalidLimit(i64),
    MemoryAccount(QueryMemoryAccountError),
    Binding(QueryMemoryBindingError),
}
impl std::fmt::Display for NativeTaskAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(error) => error.fmt(f),
            Self::Limit(error) => error.fmt(f),
            Self::MissingTracker => {
                f.write_str("native admission requires the established query tracker")
            }
            Self::MissingBinding => {
                f.write_str("native preparation requires the established query memory binding")
            }
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
            Self::Context(source) => Some(source),
            Self::Limit(source) => Some(source),
            Self::MemoryAccount(source) => Some(source),
            Self::Binding(source) => Some(source),
            Self::MissingTracker | Self::MissingBinding | Self::InvalidLimit(_) => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativeTaskAdmissionError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let code = match self {
            Self::Context(source) => return source.failure_fact(),
            Self::Limit(source) => return source.failure_fact(),
            Self::MemoryAccount(source) => return source.failure_fact(),
            Self::Binding(source) => return source.failure_fact(),
            Self::MissingTracker => 830,
            Self::MissingBinding => 831,
            Self::InvalidLimit(_) => 832,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// Borrowed by metadata preparation, then consumed by the original admission.
pub(crate) struct NativeQueryPreparationMemory {
    query_memory: QueryMemoryBinding,
    query_mem_tracker: Arc<MemTracker>,
}
impl NativeQueryPreparationMemory {
    pub(crate) fn binding(&self) -> &QueryMemoryBinding {
        &self.query_memory
    }
}

pub struct NativeTaskAdmissionResources {
    query_memory: Option<QueryMemoryBinding>,
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
        let context = LocalTaskPrepareContext::new(
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
        #[cfg(novarocks_dev_controls)]
        let context = context.with_query_tracker_witness(query_tracker_witness);
        context
    }
}

// The witness is issued while the Worker holds the exact context lock. This
// check precedes capability construction; ExternalRef/CV/type do not author attempt.
fn bind_query_memory(
    execution: QueryExecutionId,
    authority: Arc<MemoryAuthority>,
    owner: QueryMemoryAccount,
) -> Result<QueryMemoryBinding, QueryMemoryBindingError> {
    if owner.execution() != execution_key(execution) {
        return Err(QueryMemoryBindingError::OwnerAttemptMismatch);
    }
    QueryMemoryBinding::try_new(execution, authority, owner.into_account())
}
#[cfg(test)]
#[path = "native_query_memory_transport_tests.rs"]
mod memory_transport_tests;
