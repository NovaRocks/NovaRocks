#[cfg(test)]
mod tests {
    use crate::native_query_context::{NativeQueryContextRuntime, NativeTaskAdmissionResources};
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig,
    };
    use novarocks_execution::runtime::function_set::{
        ExecutionFunctionSetBuilder, SealedExecutionFunctionSet,
    };
    use novarocks_execution::runtime::mem_tracker::{self, MemTracker};
    use novarocks_execution::runtime::query_context::QueryContext;
    use novarocks_execution::runtime::runtime_state::RuntimeState;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_types::{QueryId, UniqueId};
    use std::sync::Arc;

    fn execution(query: QueryId, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(query, AttemptId::new(attempt).unwrap()).unwrap()
    }
    fn admit(
        runtime: &NativeQueryContextRuntime,
        context: &Arc<QueryContext>,
        task: UniqueId,
        limit: Option<i64>,
    ) -> Result<NativeTaskAdmissionResources, String> {
        let memory = runtime
            .prepare_query_memory_typed(context, limit)
            .map_err(|error| error.to_string())?;
        Ok(runtime.prepare_admission_with_memory(task, memory, None))
    }
    fn execution_runtime() -> Arc<ExecutionRuntime> {
        let config = ExecutionRuntimeConfig {
            driver_threads: 1,
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
    fn preparation_rollback_releases_only_its_task_reference_and_keeps_the_participation_owner() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let context = runtime
            .establish_query_resources(execution(QueryId::new(91411, 91412), 1), None)
            .unwrap();
        let running = context.task_guard();
        let first_preparation = runtime.prepare_query_memory_typed(&context, None).unwrap();
        let last_preparation = runtime.prepare_query_memory_typed(&context, None).unwrap();
        assert_eq!(context.active_tasks(), 3);
        drop(first_preparation);
        assert_eq!(context.active_tasks(), 2);
        drop(running);
        assert_eq!(context.active_tasks(), 1);
        drop(last_preparation);
        assert_eq!(context.active_tasks(), 0);
        let weak = Arc::downgrade(&context);
        assert!(
            weak.upgrade().is_some(),
            "the original Participation still owns its resources"
        );
        drop(context);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn releasing_the_participation_keeps_resources_until_its_last_actual_task_guard_exits() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let query = QueryId::new(91111, 91112);
        let context = runtime
            .establish_query_resources(execution(query, 1), Some(4096))
            .unwrap();
        let weak = Arc::downgrade(&context);
        let guard = context.task_guard();
        let account = guard.memory_binding().account().id();
        drop(context);
        assert_eq!(weak.upgrade().unwrap().active_tasks(), 1);
        let successor = runtime
            .establish_query_resources(execution(query, 2), Some(8192))
            .unwrap();
        assert_ne!(successor.memory_binding().account().id(), account);
        assert_eq!(successor.tracker().limit(), 8192);
        assert_eq!(guard.tracker().limit(), 4096);
        drop(guard);
        assert!(weak.upgrade().is_none());
        assert_eq!(successor.active_tasks(), 0);
    }

    #[test]
    fn equal_execution_keys_do_not_replace_the_original_participation_resource_reference() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let execution = execution(QueryId::new(91421, 91422), 1);
        let first = runtime
            .establish_query_resources(execution, Some(1024))
            .unwrap();
        let second = runtime
            .establish_query_resources(execution, Some(2048))
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_ne!(
            first.memory_binding().account().id(),
            second.memory_binding().account().id()
        );
        let original = admit(&runtime, &first, UniqueId::new(91423, 1), None).unwrap();
        assert_eq!(original.query_mem_tracker().limit(), 1024);
        assert_eq!(
            original.query_memory().unwrap().account().id(),
            first.memory_binding().account().id()
        );
        assert_eq!(second.active_tasks(), 0);
    }

    #[test]
    fn native_admission_installs_one_query_memory_limit() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let context = runtime
            .establish_query_resources(execution(QueryId::new(91101, 91102), 1), None)
            .unwrap();
        let first = admit(&runtime, &context, UniqueId::new(91103, 1), Some(1024)).unwrap();
        assert_eq!(first.query_mem_tracker().limit(), 1024);
        let second = admit(&runtime, &context, UniqueId::new(91103, 2), Some(1024)).unwrap();
        assert!(Arc::ptr_eq(
            &first.query_mem_tracker(),
            &second.query_mem_tracker()
        ));
        let error = admit(&runtime, &context, UniqueId::new(91103, 3), Some(2048))
            .err()
            .unwrap();
        assert!(error.contains("already has limit 1024"), "{error}");
        assert_eq!(context.active_tasks(), 2, "a refused task creates no guard");
    }

    #[test]
    fn the_query_limit_lands_on_the_account_as_well_as_the_tracker() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let context = runtime
            .establish_query_resources(execution(QueryId::new(91201, 91202), 1), Some(4096))
            .unwrap();
        let admitted = admit(&runtime, &context, UniqueId::new(91203, 1), Some(4096)).unwrap();
        let account = context.memory_binding().account();
        assert_eq!(admitted.query_mem_tracker().limit(), 4096);
        assert_eq!(account.snapshot().policy_limit_bytes, Some(4096));
        let again = admit(&runtime, &context, UniqueId::new(91203, 2), Some(4096)).unwrap();
        assert_eq!(
            again.query_memory().unwrap().account().id(),
            account.id(),
            "one Participation supplies the same account to its tasks"
        );
    }

    #[test]
    fn the_account_policy_refuses_nothing_while_charges_still_live_on_the_tracker() {
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let context = runtime
            .establish_query_resources(execution(QueryId::new(91301, 91302), 1), Some(4096))
            .unwrap();
        let admitted = admit(&runtime, &context, UniqueId::new(91303, 1), Some(4096)).unwrap();
        admitted.query_mem_tracker().consume(8192);
        let account = context.memory_binding().account();
        assert_eq!(
            account.snapshot().live_bytes,
            0,
            "tracker charges do not grant or charge the account"
        );
        account
            .prefund(4096)
            .expect("the account retains its whole policy limit");
        admitted.query_mem_tracker().release(8192);
    }

    #[test]
    fn worker_query_tracker_and_execution_fallback_share_parent_and_label() {
        let query_id = QueryId::new(0x6d65_6d31, 0x6d65_6d32);
        let label = mem_tracker::query_tracker_label(query_id.high(), query_id.low());
        assert!(process_root_children_labelled(&label).is_empty());
        let runtime =
            NativeQueryContextRuntime::new(crate::backend_test_support::test_memory_authority());
        let context = runtime
            .establish_query_resources(execution(query_id, 1), None)
            .unwrap();
        let admitted = admit(&runtime, &context, UniqueId::new(0x6d65_6d33, 1), None).unwrap();
        let from_context = admitted.query_mem_tracker();
        assert_eq!(from_context.label(), label);
        let after_context = process_root_children_labelled(&label);
        assert_eq!(after_context.len(), 1);
        assert!(Arc::ptr_eq(&after_context[0], &from_context));
        let _state = RuntimeState::for_local_execution(
            None,
            None,
            Some(query_id),
            Some(UniqueId::new(0x6d65_6d33, 0x6d65_6d34)),
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
