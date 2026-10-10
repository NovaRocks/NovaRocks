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

use novarocks_execution::runtime::fragment::FragmentPrepareContext;
use novarocks_execution::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentResultWriter,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::profile::Profiler;
use novarocks_execution::runtime::query_memory::{QueryMemoryBinding, QueryMemoryBindingError};
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
pub struct NativeFragmentQueryRuntime {
    manager: Arc<QueryContextManager>,
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

impl NativeFragmentQueryRuntime {
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
            "native_query_active_fragments",
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
            resource_metrics_publish: Arc::new(Mutex::new(())),
            #[cfg(test)]
            resource_snapshot_observer: None,
            memory_authority,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn new_for_test(
        manager: Arc<QueryContextManager>,
        memory_authority: Arc<MemoryAuthority>,
    ) -> Self {
        Self {
            manager,
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
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Result<NativeFragmentAdmissionResources, String> {
        self.prepare_admission_execution_typed(
            execution_id,
            fragment_instance_id,
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
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Result<NativeFragmentAdmissionResources, NativeFragmentAdmissionError> {
        let memory = self.prepare_query_memory_typed(
            execution_id,
            delivery_expire,
            query_expire,
            exec_mem_limit,
        )?;
        Ok(self.prepare_admission_with_memory(fragment_instance_id, memory, runtime_filter))
    }

    /// The SAME original query owner, obtained before compiled type births.
    /// The caller must hold this fragment's original registration lease.
    pub(crate) fn prepare_query_memory_typed(
        &self,
        execution_id: QueryExecutionId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
    ) -> Result<NativeQueryPreparationMemory, NativeFragmentAdmissionError> {
        let execution = execution_key(execution_id);
        self.manager
            .ensure_native_context_execution(execution, false, delivery_expire, query_expire)
            .map_err(NativeFragmentAdmissionError::Existing)?;
        let query_mem_tracker = self
            .manager
            .query_mem_tracker_execution(execution)
            .ok_or_else(|| {
                NativeFragmentAdmissionError::Existing(
                    "QueryContext missing mem_tracker".to_string(),
                )
            })?;
        let query_memory_account = if let Some(limit) = exec_mem_limit {
            query_mem_tracker
                .install_limit_once(limit)
                .map_err(NativeFragmentAdmissionError::Existing)?;
            // Preserve both original policies during this partial migration.
            // Tagged type births use the account; other execution allocations
            // still use the tracker. Full combined hard-limit coverage belongs
            // to the remaining query funding migration.
            let account = self
                .manager
                .ensure_query_memory_account(execution, &self.memory_authority)
                .map_err(NativeFragmentAdmissionError::MemoryAccount)?;
            let limit_bytes = u64::try_from(limit).map_err(|_| {
                NativeFragmentAdmissionError::Existing(format!(
                    "query memory limit must not be negative: {limit}"
                ))
            })?;
            account
                .account()
                .install_policy(limit_bytes, LimitDimension::Work);
            account
        } else {
            // Query-owned work always has its real account. None installs no
            // local policy and never clears an existing context's policy.
            self.manager
                .ensure_query_memory_account(execution, &self.memory_authority)
                .map_err(NativeFragmentAdmissionError::MemoryAccount)?
        };
        let query_memory = bind_query_memory(
            execution_id,
            Arc::clone(&self.memory_authority),
            query_memory_account,
        )
        .map_err(NativeFragmentAdmissionError::Binding)?;
        Ok(NativeQueryPreparationMemory {
            query_memory,
            query_mem_tracker,
        })
    }

    /// Consume the early binding rather than minting a second query account.
    pub(crate) fn prepare_admission_with_memory(
        &self,
        fragment_instance_id: UniqueId,
        memory: NativeQueryPreparationMemory,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> NativeFragmentAdmissionResources {
        let NativeQueryPreparationMemory {
            query_memory,
            query_mem_tracker,
        } = memory;
        let fragment_label = format!(
            "fragment_{:x}_{:x}",
            fragment_instance_id.high(),
            fragment_instance_id.low()
        );
        let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        let resources = NativeFragmentAdmissionResources {
            query_memory: Some(query_memory),
            query_mem_tracker,
            fragment_mem_tracker,
            runtime_filter,
        };
        resources
    }

    pub fn register_fragment_execution(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
    ) -> Result<NativeFragmentRegistrationLease, String> {
        let execution = execution_key(execution_id);
        self.manager.get_or_register_native_execution(
            execution,
            false,
            delivery_expire,
            query_expire,
        )?;
        self.manager
            .register_native_finst_execution(fragment_instance_id, execution)?;
        let lease = NativeFragmentRegistrationLease {
            runtime: self.clone(),
            execution,
            fragment_instance_id,
            active: true,
        };
        Ok(lease)
    }

    pub fn finish_fragment(&self, execution_id: QueryExecutionId) {
        self.manager
            .finish_fragment_execution(execution_key(execution_id));
    }

    pub fn retire_idle_execution(&self, execution_id: QueryExecutionId) -> bool {
        self.manager
            .retire_idle_native_execution(execution_key(execution_id))
    }

    pub fn unregister_fragment_execution(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) {
        self.manager
            .unregister_finst_execution(fragment_instance_id, execution_key(execution_id));
    }
}

pub struct NativeFragmentRegistrationLease {
    runtime: NativeFragmentQueryRuntime,
    execution: QueryExecutionKey,
    fragment_instance_id: UniqueId,
    active: bool,
}

impl NativeFragmentRegistrationLease {
    pub fn into_running(mut self) {
        self.active = false;
    }
}

impl Drop for NativeFragmentRegistrationLease {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .runtime
                .manager
                .rollback_pre_ready_native_fragment_execution(
                    self.execution,
                    self.fragment_instance_id,
                );
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
pub(crate) enum NativeFragmentAdmissionError {
    Existing(String),
    MemoryAccount(QueryMemoryAccountError),
    Binding(QueryMemoryBindingError),
}
impl std::fmt::Display for NativeFragmentAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Existing(error) => f.write_str(error),
            Self::MemoryAccount(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for NativeFragmentAdmissionError {}

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

pub struct NativeFragmentAdmissionResources {
    query_memory: Option<QueryMemoryBinding>,
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    runtime_filter: Option<RuntimeFilterSessionRef>,
}

impl NativeFragmentAdmissionResources {
    pub fn query_memory(&self) -> Option<&QueryMemoryBinding> {
        self.query_memory.as_ref()
    }
    pub fn query_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.query_mem_tracker)
    }

    pub fn fragment_mem_tracker(&self) -> Arc<MemTracker> {
        Arc::clone(&self.fragment_mem_tracker)
    }

    pub fn into_prepare_context(
        self,
        profiler: Option<Profiler>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        event_sink: Arc<dyn FragmentEventSink>,
    ) -> FragmentPrepareContext {
        FragmentPrepareContext::new(
            profiler,
            Some(self.fragment_mem_tracker),
            self.runtime_filter,
            exchange_transmitter,
            result_writer,
            event_sink,
        )
        .with_query_memory(self.query_memory)
        .with_fragment_commit_port(Arc::new(WorkerSinkCommitPort))
        .with_debug_exec_node_output(crate::debug_environment::debug_exec_node_output())
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
