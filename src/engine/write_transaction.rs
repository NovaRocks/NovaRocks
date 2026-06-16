// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Engine-owned Iceberg write transaction runner.
//!
//! The runner is the default boundary for user-level Iceberg SQL writes that
//! need coordinated file output, metadata commit, lifecycle persistence, and
//! post-commit finalization. It drives the Iceberg operation state machine and
//! persists facts via the operation repository, delegating the side-effecting
//! steps (running the coordinated write, calling the typed commit service,
//! finalization) to an [`IcebergWriteTransactionExecutor`]. PR-1 ships the
//! runner + fake-backed tests; the real executor and SQL routing land in PR-2.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opendal::Operator;

use crate::common::engine_error::EngineError;
use crate::connector::iceberg::catalog::registry::block_on_iceberg;
use crate::connector::iceberg::commit::{
    AbortLog, CleanupAttempt, CleanupPathMapper, CommitOpKind, CommitOutcome, CommitServiceError,
    CowUpdateRewriteSet, IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit_typed,
};
use crate::connector::iceberg::operation_lifecycle::{
    IcebergOperationFact, operation_fact_from_commit_result, operation_fact_from_finalize_failure,
};
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergOperationFactUpdate, IcebergOperationKind,
    IcebergOperationState, IcebergOperationTarget,
};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::query_result::QueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;

/// How the runner should commit the collected writer output.
pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) commit_op_kind: CommitOpKind,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

/// SQL-specific validation captured at spec-build time. Consumed by the
/// executor's write step (the runner itself does not validate). Grown in PR-2.
pub(crate) struct IcebergWriteValidationPolicy {
    /// Branch writes require Iceberg format v3.
    pub(crate) require_v3_for_branch: bool,
}

/// What the write produces. The runner does not execute the source; the
/// executor does. Variants are filled out as flows are cut over in PR-2+.
pub(crate) enum IcebergWriteSource {
    /// Rows produced by a coordinated query/mutation plan.
    CoordinatedPlan,
}

/// A complete description of one Iceberg write transaction. SQL flows build
/// this; the runner owns the lifecycle.
pub(crate) struct IcebergWriteTransactionSpec {
    pub(crate) target: IcebergOperationTarget,
    pub(crate) operation_kind: IcebergOperationKind,
    pub(crate) attempt_id: String,
    pub(crate) commit: IcebergWriteCommitPolicy,
    pub(crate) validation: IcebergWriteValidationPolicy,
    pub(crate) source: IcebergWriteSource,
}

/// Outcome of a successful (or empty/no-op) transaction.
#[derive(Debug)]
pub(crate) struct IcebergWriteTransactionOutcome {
    pub(crate) query_result: QueryResult,
    /// `Some` for committed writes; `None` for empty/no-op writes.
    pub(crate) operation_id: Option<i64>,
    /// `Some` for committed writes.
    pub(crate) committed_snapshot_id: Option<i64>,
}

/// The side-effecting dependencies of a write transaction. Real implementation
/// (PR-2) wraps the execution coordinator + typed commit service + cache/dict
/// finalization; tests inject a fake.
pub(crate) trait IcebergWriteTransactionExecutor {
    /// Run the coordinated writer plan, returning the writer outcome.
    fn run_coordinated_write(
        &self,
        spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String>;

    /// Commit the collected writer output through the typed commit service.
    fn commit(
        &self,
        spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError>;

    /// Post-commit finalization (cache invalidation, dictionary stale marking).
    fn finalize(&self, spec: &IcebergWriteTransactionSpec) -> Result<(), String>;

    fn has_preloaded_commit_output(&self) -> bool {
        false
    }
}

/// Reusable Iceberg commit/finalize context for coordinated writer output.
///
/// SQL routing is intentionally kept outside this type; callers supply a
/// collected [`WriteCommitInput`] after the coordinated write has completed.
pub(crate) struct IcebergWriteCommitExecutor {
    pub(crate) state: Arc<StandaloneState>,
    pub(crate) target: TargetBackend,
    pub(crate) catalog: Arc<dyn iceberg::Catalog>,
    pub(crate) table: iceberg::table::Table,
    pub(crate) collector: Arc<IcebergCommitCollector>,
    pub(crate) fs: Operator,
    pub(crate) cleanup_path_mapper: Option<CleanupPathMapper>,
    pub(crate) cow_update_rewrite: Option<CowUpdateRewriteSet>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

impl IcebergWriteCommitExecutor {
    pub(crate) fn commit_write_input(
        &self,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut writer_files = Vec::new();
        for writer in &write_commit.writers {
            for info in &writer.sink_commit_infos {
                match self.collector.convert_sink_commit_info(info.clone()) {
                    Ok(file) => writer_files.push(file),
                    Err(message) => {
                        let cleanup = self.cleanup_converted_writer_files(&writer_files);
                        return Err(CommitServiceError::known_uncommitted(message, cleanup));
                    }
                }
            }
        }
        self.collector.inject_written_files(writer_files);

        let file_io = self.table.file_io().clone();
        let input = RunInput {
            collector: Arc::clone(&self.collector),
            catalog: Arc::clone(&self.catalog),
            table: self.table.clone(),
            fs: self.fs.clone(),
            file_io,
            cleanup_path_mapper: self.cleanup_path_mapper.clone(),
            cow_update_rewrite: self.cow_update_rewrite.clone(),
            target_ref: self.target_ref.clone(),
            snapshot_properties: self.snapshot_properties.clone(),
        };

        match block_on_iceberg(async { run_iceberg_commit_typed(input).await }) {
            Ok(result) => result,
            Err(message) => Err(CommitServiceError::known_uncommitted(
                message,
                CleanupAttempt::not_attempted(),
            )),
        }
    }

    fn cleanup_converted_writer_files(&self, files: &[WrittenFile]) -> CleanupAttempt {
        let abort_log = AbortLog::new();
        for file in files {
            abort_log.record_data_file(file.path.clone());
        }
        let fs = self.fs.clone();
        let cleanup_path_mapper = self.cleanup_path_mapper.clone();
        match block_on_iceberg(async move {
            if let Some(mapper) = cleanup_path_mapper {
                abort_log
                    .cleanup_with_path_mapper(&fs, |path| mapper(path))
                    .await
            } else {
                abort_log.cleanup(&fs).await
            }
        }) {
            Ok(cleanup_errors) => CleanupAttempt::from_cleanup_errors(&cleanup_errors),
            Err(message) => {
                CleanupAttempt::completed(vec![format!("abort cleanup runtime failed: {message}")])
            }
        }
    }

    pub(crate) fn finalize(&self) -> Result<(), String> {
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)?;
        crate::engine::dictionary::maintenance::mark_target_stale(&self.state, &self.target)
    }
}

/// Current time in unix milliseconds for operation-record timestamps.
pub(crate) fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn write_commit_has_files(write_commit: &WriteCommitInput) -> bool {
    write_commit
        .writers
        .iter()
        .any(|writer| !writer.sink_commit_infos.is_empty())
}

/// Drives one Iceberg write transaction through the operation state machine.
pub(crate) struct IcebergWriteTransactionRunner<'a, E: IcebergWriteTransactionExecutor> {
    state: Arc<StandaloneState>,
    executor: &'a E,
}

impl<'a, E: IcebergWriteTransactionExecutor> IcebergWriteTransactionRunner<'a, E> {
    pub(crate) fn new(state: Arc<StandaloneState>, executor: &'a E) -> Self {
        Self { state, executor }
    }

    pub(crate) fn run(
        &self,
        spec: IcebergWriteTransactionSpec,
    ) -> Result<IcebergWriteTransactionOutcome, String> {
        let operation_id = self.create_preparing(&spec)?;

        let written = match self.executor.run_coordinated_write(&spec) {
            Ok(written) => written,
            Err(message) => {
                let err = CommitServiceError::known_uncommitted(
                    message.clone(),
                    CleanupAttempt::not_attempted(),
                );
                self.record_fact(operation_id, operation_fact_from_commit_result(Err(&err)))?;
                return Err(message);
            }
        };

        if let Some(abort) = &written.write_abort {
            crate::engine::write_operation_lifecycle::record_writer_abort_fact(
                &self.state,
                operation_id,
                abort,
                current_unix_millis(),
            )?;
            return Err(format!(
                "iceberg write operation {operation_id} aborted before commit: {}",
                abort.reason
            ));
        }

        let Some(write_commit) = written.write_commit.as_ref().filter(|c| {
            write_commit_has_files(c)
                || self.executor.has_preloaded_commit_output()
                || !matches!(spec.commit.commit_op_kind, CommitOpKind::FastAppend)
        }) else {
            self.transition(operation_id, IcebergOperationState::Aborting)?;
            self.transition(operation_id, IcebergOperationState::Aborted)?;
            return Ok(IcebergWriteTransactionOutcome {
                query_result: written.query_result,
                operation_id: None,
                committed_snapshot_id: None,
            });
        };

        self.transition(operation_id, IcebergOperationState::Committing)?;
        match self.executor.commit(&spec, write_commit) {
            Ok(commit_outcome) => {
                let snapshot_id = commit_outcome.new_snapshot_id;
                self.record_fact(
                    operation_id,
                    operation_fact_from_commit_result(Ok(&commit_outcome)),
                )?;
                self.transition(operation_id, IcebergOperationState::Finalizing)?;
                match self.executor.finalize(&spec) {
                    Ok(()) => {
                        self.transition(operation_id, IcebergOperationState::Finalized)?;
                        Ok(IcebergWriteTransactionOutcome {
                            query_result: written.query_result,
                            operation_id: Some(operation_id),
                            committed_snapshot_id: Some(snapshot_id),
                        })
                    }
                    Err(message) => {
                        self.record_fact(
                            operation_id,
                            operation_fact_from_finalize_failure(message),
                        )?;
                        Err(format!(
                            "iceberg write operation {operation_id}: metadata commit succeeded \
                             (snapshot {snapshot_id}, known committed) but finalization failed; \
                             do not retry the write"
                        ))
                    }
                }
            }
            Err(commit_err) => {
                self.record_fact(
                    operation_id,
                    operation_fact_from_commit_result(Err(&commit_err)),
                )?;
                let engine_error = EngineError::from(commit_err);
                Err(format!(
                    "[{}] iceberg write operation {operation_id} commit failed: {}",
                    engine_error.code().as_str(),
                    engine_error.to_user_message()
                ))
            }
        }
    }

    fn create_preparing(&self, spec: &IcebergWriteTransactionSpec) -> Result<i64, String> {
        let provider = self.metadata_provider()?;
        let request = CreateIcebergOperationRequest {
            operation_kind: spec.operation_kind,
            operation_subkind: None,
            target: spec.target.clone(),
            attempt_id: spec.attempt_id.clone(),
            base_snapshot_id: spec.commit.base_snapshot_id,
            base_snapshot_map: spec.commit.base_snapshot_map.clone(),
            staged_artifacts: Vec::new(),
            created_at_ms: current_unix_millis(),
        };
        let mut txn = provider
            .begin_write("create iceberg write operation")
            .map_err(|e| e.to_string())?;
        let stored = self
            .state
            .iceberg_operation_repo
            .create_operation(txn.as_mut(), request)
            .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        Ok(stored.operation_id)
    }

    fn transition(&self, operation_id: i64, to: IcebergOperationState) -> Result<(), String> {
        let provider = self.metadata_provider()?;
        let mut txn = provider
            .begin_write("advance iceberg write operation")
            .map_err(|e| e.to_string())?;
        self.state
            .iceberg_operation_repo
            .transition_operation(txn.as_mut(), operation_id, to, current_unix_millis())
            .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn record_fact(&self, operation_id: i64, fact: IcebergOperationFact) -> Result<(), String> {
        let provider = self.metadata_provider()?;
        let update = IcebergOperationFactUpdate {
            operation_id,
            state: fact.state,
            commit_outcome: fact.commit_outcome,
            cleanup_outcome: fact.cleanup_outcome,
            recovery_evidence: fact.recovery_evidence,
            failure: fact.failure,
            now_ms: current_unix_millis(),
        };
        let mut txn = provider
            .begin_write("record iceberg write operation fact")
            .map_err(|e| e.to_string())?;
        self.state
            .iceberg_operation_repo
            .record_operation_fact(txn.as_mut(), update)
            .map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn metadata_provider(&self) -> Result<&Arc<dyn crate::meta::MetaStoreProvider>, String> {
        self.state
            .metadata_provider
            .as_ref()
            .ok_or_else(|| "metadata provider is required for iceberg write operations".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::commit::{CommitOutcome, CommitServiceError};
    use crate::meta::repository::iceberg_operation::IcebergOperationState;
    use crate::runtime::query_result::QueryResult;
    use crate::runtime::write_coordinator::{WriteCommitInput, WriterCommitInput, WriterKey};
    use std::cell::RefCell;

    struct TestEnv {
        state: Arc<StandaloneState>,
        provider: Arc<dyn crate::meta::MetaStoreProvider>,
        _dir: tempfile::TempDir,
    }

    fn test_env() -> TestEnv {
        let dir = tempfile::tempdir().expect("metadata tempdir");
        let provider: Arc<dyn crate::meta::MetaStoreProvider> = Arc::new(
            crate::meta::SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))
                .expect("provider"),
        );
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(Arc::clone(&provider)),
            ..StandaloneState::default()
        });
        TestEnv {
            state,
            provider,
            _dir: dir,
        }
    }

    fn sample_spec() -> IcebergWriteTransactionSpec {
        IcebergWriteTransactionSpec {
            target: crate::meta::repository::iceberg_operation::IcebergOperationTarget {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                ref_name: None,
            },
            operation_kind: IcebergOperationKind::InsertAppend,
            attempt_id: "attempt-1".to_string(),
            commit: IcebergWriteCommitPolicy {
                commit_op_kind: CommitOpKind::FastAppend,
                base_snapshot_id: Some(7),
                base_snapshot_map: BTreeMap::new(),
                target_ref: "main".to_string(),
                snapshot_properties: BTreeMap::new(),
            },
            validation: IcebergWriteValidationPolicy {
                require_v3_for_branch: false,
            },
            source: IcebergWriteSource::CoordinatedPlan,
        }
    }

    fn empty_query_result() -> QueryResult {
        QueryResult {
            columns: Vec::new(),
            chunks: Vec::new(),
        }
    }

    fn write_commit_with_one_writer() -> WriteCommitInput {
        crate::engine::write_operation_lifecycle::test_support::write_commit_with_data_file()
    }

    fn write_commit_with_writer_without_files() -> WriteCommitInput {
        let write_id = crate::types::TUniqueId::new(10, 20);
        let writer_key = WriterKey {
            query_id: write_id.clone(),
            fragment_instance_id: crate::types::TUniqueId::new(101, 201),
            backend_num: 0,
        };
        WriteCommitInput {
            write_id,
            writers: vec![WriterCommitInput {
                writer_id: 0,
                writer_key,
                sink_commit_infos: Vec::new(),
                tablet_commit_infos: Vec::new(),
                tablet_fail_infos: Vec::new(),
                load_counters: BTreeMap::new(),
                loaded_rows: 0,
                loaded_bytes: 0,
                filtered_rows: 0,
            }],
        }
    }

    struct FakeExecutor {
        write: RefCell<Option<Result<CoordinatedQueryResult, String>>>,
        commit: RefCell<Option<Result<CommitOutcome, CommitServiceError>>>,
        finalize: Result<(), String>,
        preloaded_commit_output: bool,
    }

    impl IcebergWriteTransactionExecutor for FakeExecutor {
        fn run_coordinated_write(
            &self,
            _spec: &IcebergWriteTransactionSpec,
        ) -> Result<CoordinatedQueryResult, String> {
            self.write
                .borrow_mut()
                .take()
                .expect("write outcome set once")
        }

        fn commit(
            &self,
            _spec: &IcebergWriteTransactionSpec,
            _write_commit: &WriteCommitInput,
        ) -> Result<CommitOutcome, CommitServiceError> {
            self.commit
                .borrow_mut()
                .take()
                .expect("commit outcome set once")
        }

        fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
            self.finalize.clone()
        }

        fn has_preloaded_commit_output(&self) -> bool {
            self.preloaded_commit_output
        }
    }

    fn one_writer_abort() -> crate::runtime::write_coordinator::WriteAbortInput {
        crate::engine::write_operation_lifecycle::test_support::write_abort_with_data_file()
    }

    #[test]
    fn successful_append_drives_operation_to_finalized() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 1234,
                written_manifest_paths: vec!["s3://bucket/m.avro".to_string()],
            }))),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let outcome = runner.run(sample_spec()).expect("run");
        assert_eq!(outcome.committed_snapshot_id, Some(1234));
        let op_id = outcome.operation_id.expect("operation id");

        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), op_id)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Finalized);
        assert_eq!(
            stored.commit_outcome.as_ref().map(|c| c.snapshot_id),
            Some(1234)
        );
    }

    #[test]
    fn writer_abort_records_failed_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: Some(one_writer_abort()),
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner.run(sample_spec()).expect_err("abort surfaces error");
        assert!(err.contains("aborted before commit"), "got: {err}");
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
    }

    #[test]
    fn coordinated_write_failure_records_failed_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Err("coordinated write failed".to_string()))),
            commit: RefCell::new(None),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner
            .run(sample_spec())
            .expect_err("write failure surfaces");
        assert!(
            err.contains("coordinated write failed"),
            "original message should be preserved, got: {err}"
        );
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(
            stored.failure.as_ref().map(|f| f.message.as_str()),
            Some("coordinated write failed")
        );
    }

    #[test]
    fn commit_known_uncommitted_records_failed_known_uncommitted() {
        use crate::connector::iceberg::commit::CleanupAttempt;
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(Some(Err(CommitServiceError::KnownUncommitted {
                message: "conflict".to_string(),
                cleanup: CleanupAttempt {
                    attempted: true,
                    error_count: 0,
                    error_paths: Vec::new(),
                },
            }))),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner
            .run(sample_spec())
            .expect_err("commit failure surfaces");
        assert!(
            err.starts_with("[CommitKnownUncommitted] iceberg write operation 1 commit failed:"),
            "got prefix-extractable engine error: {err}"
        );
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
    }

    #[test]
    fn commit_unknown_records_commit_unknown_and_skips_finalize() {
        use crate::connector::iceberg::commit::RecoveryEvidence;
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(Some(Err(CommitServiceError::Unknown {
                message: "rpc timeout".to_string(),
                evidence: RecoveryEvidence {
                    table_ident: "db.orders".to_string(),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: Some(7),
                    base_sequence_number: 3,
                    staging_dir: "s3://bucket/_staging/x".to_string(),
                },
            }))),
            finalize: Err("finalize must not be called".to_string()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner
            .run(sample_spec())
            .expect_err("commit unknown surfaces");
        assert!(
            err.starts_with("[CommitUnknown] iceberg write operation 1 commit failed:"),
            "got prefix-extractable engine error: {err}"
        );
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::CommitUnknown);
        assert!(stored.recovery_evidence.is_some());
    }

    #[test]
    fn finalize_failure_records_finalize_failed_known_committed() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 9,
                written_manifest_paths: Vec::new(),
            }))),
            finalize: Err("cache invalidation failed".to_string()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let err = runner
            .run(sample_spec())
            .expect_err("finalize failure surfaces");
        assert!(err.contains("known committed"), "got: {err}");
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(
            stored.state,
            IcebergOperationState::FinalizeFailedKnownCommitted
        );
    }

    #[test]
    fn empty_write_transitions_to_aborted_with_no_committed_outcome() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let outcome = runner.run(sample_spec()).expect("empty is OK");
        assert_eq!(outcome.operation_id, None);
        assert_eq!(outcome.committed_snapshot_id, None);
        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Aborted);
    }

    #[test]
    fn runner_treats_writers_without_files_as_empty_write() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_writer_without_files()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
            preloaded_commit_output: false,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let outcome = runner
            .run(sample_spec())
            .expect("writer with no data files is an empty write");
        assert_eq!(outcome.operation_id, None);
        assert_eq!(outcome.committed_snapshot_id, None);

        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Aborted);
    }

    #[test]
    fn runner_commits_fast_append_with_preloaded_local_output() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(CoordinatedQueryResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_writer_without_files()),
                write_abort: None,
                profilers: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 4321,
                written_manifest_paths: vec!["s3://bucket/preloaded.avro".to_string()],
            }))),
            finalize: Ok(()),
            preloaded_commit_output: true,
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let outcome = runner
            .run(sample_spec())
            .expect("preloaded local output must be committed");
        assert_eq!(outcome.committed_snapshot_id, Some(4321));

        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), outcome.operation_id.expect("operation id"))
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::Finalized);
    }
}
