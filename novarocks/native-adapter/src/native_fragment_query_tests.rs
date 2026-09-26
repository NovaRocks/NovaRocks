#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::native_fragment_query::NativeFragmentQueryRuntime;
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
    use novarocks_worker::query_context::{QueryContextManager, QueryExecutionKey};

    /// The execution key of a query's first attempt, the one these tests admit.
    fn first_attempt(query_id: QueryId) -> QueryExecutionKey {
        QueryExecutionKey::native_attempt(
            query_id,
            std::num::NonZeroU64::new(1).expect("nonzero attempt"),
        )
    }

    fn execution_runtime() -> Arc<ExecutionRuntime> {
        let config = ExecutionRuntimeConfig {
            driver_threads: 1,
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
                crate::backend_test_support::test_memory_authority(),
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
        let runtime = NativeFragmentQueryRuntime::new_for_test(
            manager.clone(),
            crate::backend_test_support::test_memory_authority(),
        );
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

    #[test]
    fn terminal_attempt_retires_a_prepared_context_without_a_registered_fragment() {
        let manager = QueryContextManager::new_for_test();
        let runtime = NativeFragmentQueryRuntime::new_for_test(
            manager.clone(),
            crate::backend_test_support::test_memory_authority(),
        );
        let query_id = QueryId::new(91_111, 91_112);
        let first = QueryExecutionId::new(query_id, AttemptId::new(1).unwrap()).unwrap();
        let second = QueryExecutionId::new(query_id, AttemptId::new(2).unwrap()).unwrap();
        runtime
            .prepare_admission_execution(
                first,
                UniqueId::new(91_113, 1),
                Duration::from_secs(1),
                Duration::from_secs(300),
                Some(4096),
                None,
            )
            .unwrap();
        assert_eq!(
            manager.native_execution_resource_snapshot().active_contexts,
            1
        );
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            0
        );

        assert!(!runtime.retire_idle_execution(second));
        assert!(runtime.retire_idle_execution(first));
        assert_eq!(
            manager.native_execution_resource_snapshot().active_contexts,
            0
        );
        runtime
            .prepare_admission_execution(
                second,
                UniqueId::new(91_113, 2),
                Duration::from_secs(1),
                Duration::from_secs(300),
                Some(8192),
                None,
            )
            .expect("a successor attempt receives a fresh query context");
    }

    #[test]
    fn terminal_attempt_keeps_a_registered_fragment_until_its_owner_releases_it() {
        let manager = QueryContextManager::new_for_test();
        let runtime = NativeFragmentQueryRuntime::new_for_test(
            manager.clone(),
            crate::backend_test_support::test_memory_authority(),
        );
        let execution =
            QueryExecutionId::new(QueryId::new(91_121, 91_122), AttemptId::new(1).unwrap())
                .unwrap();
        let finst = UniqueId::new(91_123, 1);
        runtime
            .prepare_admission_execution(
                execution,
                finst,
                Duration::from_secs(1),
                Duration::from_secs(300),
                None,
                None,
            )
            .unwrap();
        let registration = runtime
            .register_fragment_execution(
                execution,
                finst,
                Duration::from_secs(1),
                Duration::from_secs(300),
            )
            .unwrap();

        assert!(!runtime.retire_idle_execution(execution));
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            1
        );
        drop(registration);
        assert_eq!(
            manager.native_execution_resource_snapshot().active_contexts,
            0
        );
    }

    /// The limit is stated on both mechanisms at the same number, and the
    /// account is one per query rather than one per fragment.
    #[test]
    fn the_query_limit_lands_on_the_account_as_well_as_the_tracker() {
        let manager = QueryContextManager::new_for_test();
        let authority = crate::backend_test_support::test_memory_authority();
        let runtime =
            NativeFragmentQueryRuntime::new_for_test(manager.clone(), Arc::clone(&authority));
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
            .ensure_query_account(first_attempt(query_id), &authority)
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
            .ensure_query_account(first_attempt(query_id), &authority)
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
        let authority = crate::backend_test_support::test_memory_authority();
        let runtime =
            NativeFragmentQueryRuntime::new_for_test(manager.clone(), Arc::clone(&authority));
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
            .ensure_query_account(first_attempt(query_id), &authority)
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
        let runtime = NativeFragmentQueryRuntime::new_for_test(
            manager,
            crate::backend_test_support::test_memory_authority(),
        );
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
