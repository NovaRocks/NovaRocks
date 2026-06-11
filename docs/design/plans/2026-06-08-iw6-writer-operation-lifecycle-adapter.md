# IW-6 Writer Operation Lifecycle Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first IW-6 writer-operation lifecycle slice without coupling runtime execution to standalone metadata state: normalize writer commit/abort outputs into Iceberg operation requests and expose engine-owned helpers that persist those requests when a caller supplies the missing operation context.

**Architecture:** Runtime owns only lifecycle normalization from concrete writer outputs. Engine owns metadata persistence because it has `StandaloneState`, the Iceberg operation repository, and future INSERT/commit routing context. `WriteCommitInput` is intentionally not treated as sufficient to infer target table, commit semantics, or base snapshot.

**Tech Stack:** Rust, NovaRocks standalone engine, managed-lake metadata repository, Iceberg operation repository, Cargo unit tests.

---

## Task 1: Add Runtime Writer Lifecycle Normalization

- [x] Add failing unit tests for commit and abort lifecycle normalization in `src/runtime/write_operation_lifecycle.rs`.

  ```rust
  #[test]
  fn write_commit_builds_operation_request_with_staged_artifacts() {
      let ctx = WriteOperationContext {
          operation_kind: IcebergOperationKind::InsertAppend,
          target: IcebergOperationTarget {
              catalog: "ice".to_string(),
              namespace: "db".to_string(),
              table: "orders".to_string(),
              ref_name: None,
          },
          attempt_id: "attempt-1".to_string(),
          commit_op_kind: CommitOpKind::FastAppend,
          base_snapshot_id: Some(7),
          base_snapshot_map: BTreeMap::new(),
          created_at_ms: 1234,
      };
      let input = write_commit_input_with_data_file();

      let request = operation_request_from_write_commit(ctx, &input).unwrap();

      assert_eq!(request.operation_kind, IcebergOperationKind::InsertAppend);
      assert_eq!(request.base_snapshot_id, Some(7));
      assert_eq!(request.staged_artifacts.len(), 1);
  }

  #[test]
  fn write_abort_records_failed_known_uncommitted_fact() {
      let input = write_abort_input_with_data_file();

      let fact = operation_fact_update_from_write_abort(42, &input, 5678).unwrap();

      assert_eq!(fact.operation_id, 42);
      assert_eq!(fact.state, IcebergOperationState::FailedKnownUncommitted);
      let failure = fact.failure.unwrap();
      assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
      assert_eq!(failure.next_action, IcebergOperationNextAction::RetryAbort);
  }
  ```

- [x] Implement `WriteOperationContext`, `operation_request_from_write_commit`, `operation_fact_update_from_write_abort`, and staged artifact extraction.

  ```rust
  pub(crate) struct WriteOperationContext {
      pub(crate) operation_kind: IcebergOperationKind,
      pub(crate) target: IcebergOperationTarget,
      pub(crate) attempt_id: String,
      pub(crate) commit_op_kind: CommitOpKind,
      pub(crate) base_snapshot_id: Option<i64>,
      pub(crate) base_snapshot_map: BTreeMap<String, i64>,
      pub(crate) created_at_ms: i64,
  }

  pub(crate) fn operation_request_from_write_commit(
      ctx: WriteOperationContext,
      input: &WriteCommitInput,
  ) -> Result<CreateIcebergOperationRequest, String> {
      let expected = operation_kind_for_commit_op_kind(ctx.commit_op_kind);
      if ctx.operation_kind != expected {
          return Err(format!(
              "writer operation kind {:?} does not match commit op kind {:?}",
              ctx.operation_kind, ctx.commit_op_kind
          ));
      }

      Ok(CreateIcebergOperationRequest {
          operation_kind: ctx.operation_kind,
          target: ctx.target,
          attempt_id: ctx.attempt_id,
          base_snapshot_id: ctx.base_snapshot_id,
          base_snapshot_map: ctx.base_snapshot_map,
          staged_artifacts: staged_artifacts_from_writer_outputs(&input.writers)?,
          created_at_ms: ctx.created_at_ms,
      })
  }
  ```

- [x] Register the runtime module in `src/runtime/mod.rs`.

  ```rust
  pub(crate) mod write_operation_lifecycle;
  ```

- [x] Verify the runtime adapter.

  ```bash
  cargo test -q write_commit
  cargo test -q writer_abort
  cargo test -q staged_artifacts
  ```

  Expected result: all runtime writer lifecycle tests pass.

## Task 2: Persist Writer Operations from the Engine Boundary

- [x] Add failing unit tests in `src/engine/write_operation_lifecycle.rs` for creating operation records and recording abort facts.

  ```rust
  #[test]
  fn writer_operation_commit_persists_operation_record() {
      let env = test_state_with_metadata();
      let operation_id = create_writer_operation_from_commit(
          &env.state,
          append_operation_context(),
          &write_commit_input_with_data_file(),
      )
      .unwrap();

      let txn = env.provider.begin_read().unwrap();
      let stored = env
          .state
          .iceberg_operation_repo
          .load_operation(txn.as_ref(), operation_id)
          .unwrap()
          .unwrap();

      assert_eq!(stored.operation_kind, IcebergOperationKind::InsertAppend);
      assert_eq!(stored.staged_artifacts.len(), 1);
  }

  #[test]
  fn writer_operation_abort_records_known_uncommitted_fact() {
      let env = test_state_with_metadata();
      let operation_id = create_writer_operation_from_commit(
          &env.state,
          append_operation_context(),
          &write_commit_input_with_data_file(),
      )
      .unwrap();

      record_writer_abort_fact(
          &env.state,
          operation_id,
          &write_abort_input_with_data_file(),
          2345,
      )
      .unwrap();

      let txn = env.provider.begin_read().unwrap();
      let stored = env
          .state
          .iceberg_operation_repo
          .load_operation(txn.as_ref(), operation_id)
          .unwrap()
          .unwrap();

      assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
      let failure = stored.failure.unwrap();
      assert_eq!(failure.kind, IcebergOperationFailureKind::KnownUncommitted);
      assert_eq!(failure.next_action, IcebergOperationNextAction::RetryAbort);
  }
  ```

- [x] Implement engine-owned persistence helpers that require an explicit `WriteOperationContext`.

  ```rust
  pub(crate) fn create_writer_operation_from_commit(
      state: &Arc<StandaloneState>,
      ctx: WriteOperationContext,
      commit: &WriteCommitInput,
  ) -> Result<i64, String> {
      let provider = state
          .metadata_provider
          .as_ref()
          .ok_or_else(|| "metadata provider is required for iceberg writer operations".to_string())?;
      let request = operation_request_from_write_commit(ctx, commit)?;
      let mut txn = provider.begin_write("create iceberg writer operation")?;
      let operation = state
          .iceberg_operation_repo
          .create_operation(txn.as_mut(), request)?;
      txn.commit()?;
      Ok(operation.operation_id)
  }
  ```

- [x] Register the engine helper module in `src/engine/mod.rs`.

  ```rust
  mod write_operation_lifecycle;
  ```

- [x] Verify the engine persistence helper.

  ```bash
  cargo test -q writer_operation
  ```

  Expected result: engine persistence tests and runtime adapter tests pass.

## Task 3: Keep Writer Coordinator Tests Isolated

- [x] Reproduce the parallel test pollution by running writer coordinator filtered tests.

  ```bash
  cargo test -q write_coordinator
  ```

  Before the fix, the expected failure is a report-status test clearing the global write registry while coordinator tests are active, producing `write coordinator not found for query ...`.

- [x] Update report-status tests in `src/service/grpc_server.rs` to use `write_registry_test_guard()`.

  ```rust
  let _registry_guard = write_registry_test_guard();
  let coordinator = register_query(query_id, vec![node_id]);
  ```

- [x] Verify the registry isolation.

  ```bash
  cargo test -q write_coordinator
  cargo test -q report_exec_status
  ```

  Expected result: both filtered suites pass without global registry interference.

## Task 4: Final Verification

- [x] Format the Rust changes.

  ```bash
  cargo fmt
  ```

- [x] Run focused verification.

  ```bash
  cargo test -q write_commit
  cargo test -q writer_abort
  cargo test -q staged_artifacts
  cargo test -q writer_operation
  cargo test -q write_coordinator
  cargo test -q report_exec_status
  git diff --check
  ```

- [x] Inspect the final diff to ensure this slice does not add fake target inference or couple runtime to `StandaloneState`.

  ```bash
  git diff -- src/runtime/write_operation_lifecycle.rs src/engine/write_operation_lifecycle.rs src/runtime/mod.rs src/engine/mod.rs src/service/grpc_server.rs
  ```

## Task 5: Commit the Completed Slice

- [x] Stage the plan and code changes.

  ```bash
  git add docs/design/plans/2026-06-08-iw6-writer-operation-lifecycle-adapter.md \
      src/runtime/mod.rs \
      src/runtime/write_operation_lifecycle.rs \
      src/engine/mod.rs \
      src/engine/write_operation_lifecycle.rs \
      src/service/grpc_server.rs
  ```

- [x] Commit with an English message.

  ```bash
  git commit -m "Add writer operation lifecycle adapter"
  ```
