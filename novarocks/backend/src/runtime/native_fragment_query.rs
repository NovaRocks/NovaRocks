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
use std::sync::Arc;
use std::time::Duration;

use novarocks_spi::connector::ConnectorCancellation;

use crate::runtime::sink_commit::BackendSinkCommitPort;
use novarocks_execution::exec::node::scan::ScanOp;
use novarocks_execution::exec::operators::scan::ScanDispatchState;
use novarocks_execution::runtime::fragment::FragmentPrepareContext;
use novarocks_execution::runtime::fragment::io::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentResultWriter, ScanRegistrationPort,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime::profile::Profiler;
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_memory::{LimitDimension, MemoryAuthority};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use novarocks_worker::query_context::{
    QueryContextManager, QueryExecutionKey, query_context_manager,
};

#[derive(Clone)]
pub struct NativeFragmentQueryRuntime {
    manager: Arc<QueryContextManager>,
    /// The one memory capacity authority this OS process was given.
    ///
    /// The registry behind `manager` is a process-global singleton, so the
    /// authority is carried here instead: this type is constructed by the
    /// backend composition root, which is the only place entitled to hand it
    /// out.
    memory_authority: Arc<MemoryAuthority>,
}

struct QueryContextScanRegistrationPort {
    manager: Arc<QueryContextManager>,
}

impl ScanRegistrationPort for QueryContextScanRegistrationPort {
    fn register_incremental_scan(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
        op: Arc<dyn ScanOp>,
        dispatch: Arc<ScanDispatchState>,
    ) -> Result<(), String> {
        self.manager
            .register_incremental_scan_node(fragment_instance_id, node_id, op, dispatch)
    }
}

struct NativeExecutionConnectorCancellation {
    manager: Arc<QueryContextManager>,
    query_id: QueryId,
}

impl ConnectorCancellation for NativeExecutionConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.manager.is_query_canceled(self.query_id)
    }
}

impl NativeFragmentQueryRuntime {
    pub fn scan_registration_port(&self) -> Arc<dyn ScanRegistrationPort> {
        Arc::new(QueryContextScanRegistrationPort {
            manager: Arc::clone(&self.manager),
        })
    }

    pub fn publish_resource_snapshot(&self) {
        let snapshot = self.manager.native_execution_resource_snapshot();
        novarocks_native_adapter::backend_metrics::publish_backend_query_execution_resource(
            "native_query_contexts_active",
            snapshot.active_contexts,
        );
        novarocks_native_adapter::backend_metrics::publish_backend_query_execution_resource(
            "native_query_contexts_second_chance",
            snapshot.second_chance_contexts,
        );
        novarocks_native_adapter::backend_metrics::publish_backend_query_execution_resource(
            "native_query_active_fragments",
            snapshot.active_fragments,
        );
    }
    pub fn global(memory_authority: Arc<MemoryAuthority>) -> Self {
        Self {
            manager: query_context_manager(),
            memory_authority,
        }
    }

    pub fn prepare_admission_execution(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        delivery_expire: Duration,
        query_expire: Duration,
        exec_mem_limit: Option<i64>,
        runtime_filter: Option<RuntimeFilterSessionRef>,
    ) -> Result<NativeFragmentAdmissionResources, String> {
        let execution = execution_key(execution_id);
        self.manager.ensure_native_context_execution(
            execution,
            false,
            delivery_expire,
            query_expire,
        )?;
        let query_mem_tracker = self
            .manager
            .query_mem_tracker_execution(execution)
            .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
        if let Some(limit) = exec_mem_limit {
            query_mem_tracker.install_limit_once(limit)?;
            // The same number, stated on both mechanisms. Charges still live
            // on the tracker, so the account's own `L` is near zero and this
            // policy refuses nothing yet; that is deliberate. As later slices
            // move charges onto accounts, the tracker side shrinks and the
            // account side grows, and their sum stays this one limit.
            let account = self
                .manager
                .ensure_query_account(execution.query_id(), &self.memory_authority)?;
            let limit_bytes = u64::try_from(limit)
                .map_err(|_| format!("query memory limit must not be negative: {limit}"))?;
            account.install_policy(limit_bytes, LimitDimension::Work);
        }
        let fragment_label = format!(
            "fragment_{:x}_{:x}",
            fragment_instance_id.high(),
            fragment_instance_id.low()
        );
        let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
        let resources = NativeFragmentAdmissionResources {
            query_mem_tracker,
            fragment_mem_tracker,
            runtime_filter,
            scan_registration: self.scan_registration_port(),
        };
        self.publish_resource_snapshot();
        Ok(resources)
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
        self.publish_resource_snapshot();
        Ok(lease)
    }

    /// Returns the read-only cancellation capability that must be passed into
    /// backend-owned connector-read decode. The decoder never receives the
    /// query manager itself.
    pub fn connector_cancellation_for_execution(
        &self,
        execution_id: QueryExecutionId,
    ) -> Arc<dyn ConnectorCancellation> {
        Arc::new(NativeExecutionConnectorCancellation {
            manager: Arc::clone(&self.manager),
            query_id: QueryId::new(
                execution_id.query_id().high(),
                execution_id.query_id().low(),
            ),
        })
    }

    pub fn finish_fragment(&self, execution_id: QueryExecutionId) {
        self.manager
            .finish_fragment_execution(execution_key(execution_id));
        self.publish_resource_snapshot();
    }

    pub fn unregister_fragment_execution(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) {
        self.manager
            .unregister_finst_execution(fragment_instance_id, execution_key(execution_id));
        self.publish_resource_snapshot();
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

pub struct NativeFragmentAdmissionResources {
    query_mem_tracker: Arc<MemTracker>,
    fragment_mem_tracker: Arc<MemTracker>,
    runtime_filter: Option<RuntimeFilterSessionRef>,
    scan_registration: Arc<dyn ScanRegistrationPort>,
}

impl NativeFragmentAdmissionResources {
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
        .with_scan_registration_port(self.scan_registration)
        .with_fragment_commit_port(Arc::new(BackendSinkCommitPort))
        .with_debug_exec_node_output(
            novarocks_native_adapter::debug_environment::debug_exec_node_output(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::NativeFragmentQueryRuntime;
    use novarocks_execution::exec::expr::agg::{
        ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
    };
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_execution::runtime::mem_tracker::{self, MemTracker};
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_types::{QueryId, UniqueId};
    use novarocks_worker::query_context::QueryContextManager;

    fn execution_runtime() -> Arc<ExecutionRuntime> {
        let config = ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        };
        let mut builder = ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
            .expect("builtin function metadata");
        novarocks_execution::exec::expr::agg::contribute_builtin_aggregate_implementations(
            &mut builder,
        )
        .expect("builtin aggregate implementations");
        let function_set: Arc<SealedExecutionFunctionSet> =
            Arc::new(builder.seal().expect("builtin execution function set"));
        Arc::new(
            ExecutionRuntime::new(
                config,
                function_set,
                crate::application::test_memory_authority(),
            )
            .expect("execution runtime"),
        )
    }

    fn process_root_children_labelled(label: &str) -> Vec<Arc<MemTracker>> {
        mem_tracker::process_mem_tracker()
            .children()
            .into_iter()
            .filter(|child| child.label() == label)
            .collect()
    }

    #[test]
    fn native_admission_installs_one_query_memory_limit() {
        let manager = QueryContextManager::new_for_test();
        let runtime = NativeFragmentQueryRuntime {
            manager: manager.clone(),
            memory_authority: crate::application::test_memory_authority(),
        };
        let query_id = QueryId::new(91_101, 91_102);
        let execution_id =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id");

        let first = runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_103, 1),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(1024),
                None,
            )
            .expect("first fragment installs limit");
        assert_eq!(first.query_mem_tracker().limit(), 1024);

        runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_103, 2),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(1024),
                None,
            )
            .expect("same query contract is idempotent");
        let error = runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_103, 3),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(2048),
                None,
            )
            .err()
            .expect("query memory limit drift must fail");
        assert!(error.contains("already has limit 1024"), "{error}");
    }

    /// The limit is stated on both mechanisms at the same number, and the
    /// account is one per query rather than one per fragment.
    #[test]
    fn the_query_limit_lands_on_the_account_as_well_as_the_tracker() {
        let manager = QueryContextManager::new_for_test();
        let authority = crate::application::test_memory_authority();
        let runtime = NativeFragmentQueryRuntime {
            manager: manager.clone(),
            memory_authority: Arc::clone(&authority),
        };
        let query_id = QueryId::new(91_201, 91_202);
        let execution_id =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id");

        let admitted = runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_203, 1),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(4096),
                None,
            )
            .expect("the first fragment installs the limit");

        let account = manager
            .ensure_query_account(query_id, &authority)
            .expect("the account must exist after admission");
        assert_eq!(
            admitted.query_mem_tracker().limit(),
            4096,
            "the tracker keeps the limit it always had"
        );
        assert_eq!(
            account.snapshot().policy_limit_bytes,
            Some(4096),
            "the same number must also be the account's policy"
        );

        // A second fragment of the same query reuses one account: capacity is
        // a property of the query, not of each fragment that turns up.
        runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_203, 2),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(4096),
                None,
            )
            .expect("the same query contract is idempotent");
        let again = manager
            .ensure_query_account(query_id, &authority)
            .expect("the account must still exist");
        assert_eq!(
            again.id(),
            account.id(),
            "one query must have exactly one memory account"
        );
    }

    /// Installing the policy must not change any query's outcome yet: charges
    /// still live on the tracker, so the account's own `L` is zero and the
    /// policy refuses nothing. This is the property that makes W2A safe to
    /// ship ahead of the charge migration.
    #[test]
    fn the_account_policy_refuses_nothing_while_charges_still_live_on_the_tracker() {
        let manager = QueryContextManager::new_for_test();
        let authority = crate::application::test_memory_authority();
        let runtime = NativeFragmentQueryRuntime {
            manager: manager.clone(),
            memory_authority: Arc::clone(&authority),
        };
        let query_id = QueryId::new(91_301, 91_302);
        let execution_id =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id");

        let admitted = runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(91_303, 1),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Some(4096),
                None,
            )
            .expect("the first fragment installs the limit");

        // Charge the tracker well past the limit, exactly as a real query
        // would today. The tracker is the mechanism that decides, so this is
        // where the old behaviour still lives.
        admitted.query_mem_tracker().consume(8192);
        let account = manager
            .ensure_query_account(query_id, &authority)
            .expect("the account must exist");
        assert_eq!(
            account.snapshot().live_bytes,
            0,
            "no charge has moved onto the account in this slice"
        );
        account
            .request_grant(4096)
            .expect("the account policy must still have its whole limit available");
        admitted.query_mem_tracker().release(8192);
    }

    #[test]
    fn worker_query_tracker_and_execution_fallback_share_parent_and_label() {
        let query_id = QueryId::new(0x6d65_6d31, 0x6d65_6d32);
        let label = mem_tracker::query_tracker_label(query_id.high(), query_id.low());
        assert!(
            process_root_children_labelled(&label).is_empty(),
            "the test query id must be unique to this test"
        );

        let manager = QueryContextManager::new_for_test();
        let runtime = NativeFragmentQueryRuntime {
            manager,
            memory_authority: crate::application::test_memory_authority(),
        };
        let execution_id =
            QueryExecutionId::new(query_id, AttemptId::new(1).expect("nonzero attempt"))
                .expect("valid execution id");
        let admitted = runtime
            .prepare_admission_execution(
                execution_id,
                UniqueId::new(0x6d65_6d33, 1),
                Duration::from_secs(5),
                Duration::from_secs(5),
                None,
                None,
            )
            .expect("worker query context admission");
        let from_context = admitted.query_mem_tracker();
        assert_eq!(from_context.label(), label);
        let after_context = process_root_children_labelled(&label);
        assert_eq!(after_context.len(), 1);
        assert!(Arc::ptr_eq(&after_context[0], &from_context));

        let _state = RuntimeState::new(
            None,
            None,
            Some(query_id),
            Some(UniqueId::new(0x6d65_6d33, 0x6d65_6d34)),
            None,
            None,
            None,
            None,
            Some(execution_runtime()),
            None,
        );
        let after_state = process_root_children_labelled(&label);
        assert_eq!(after_state.len(), 2);
        assert!(
            after_state
                .iter()
                .any(|child| Arc::ptr_eq(child, &from_context))
        );
    }
}
