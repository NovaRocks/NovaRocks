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
//! steps (running the coordinated write, resolving the connector control
//! outcome, and finalization) to an [`IcebergWriteTransactionExecutor`].

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opendal::Operator;

use crate::common::engine_error::EngineError;
use crate::connector::iceberg::catalog::registry::block_on_iceberg;
use crate::connector::iceberg::change_stream_routing::{
    ChangeStreamWriterCommitPlan, ChangeStreamWriterReports, route_change_stream_staged_reports,
};
use crate::connector::iceberg::commit::{
    AbortLog, CleanupAttempt, CleanupPathMapper, CommitOpKind, CommitOutcome, CommitServiceError,
    CowUpdateRewriteSet, IcebergCommitCollector, RunInput, WrittenFile, run_iceberg_commit,
};
use crate::connector::iceberg::operation_lifecycle::{
    IcebergOperationFact, operation_fact_from_commit_result, operation_fact_from_finalize_failure,
};
use crate::connector::iceberg::report::IcebergWriterReport;
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergOperationFactUpdate, IcebergOperationKind,
    IcebergOperationState, IcebergOperationTarget,
};
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::write::WriteCommitInput;
use crate::runtime::query_result::QueryResult;

/// How the runner should commit the collected writer output.
pub(crate) struct IcebergWriteCommitPolicy {
    pub(crate) commit_op_kind: CommitOpKind,
    pub(crate) base_snapshot_id: Option<i64>,
    pub(crate) base_snapshot_map: BTreeMap<String, i64>,
    pub(crate) target_ref: String,
    pub(crate) snapshot_properties: BTreeMap<String, String>,
}

/// SQL-specific validation captured at spec-build time and consumed by the
/// executor's write step (the runner itself does not validate).
pub(crate) struct IcebergWriteValidationPolicy {
    /// Branch writes require Iceberg format v3.
    pub(crate) require_v3_for_branch: bool,
}

/// What the write produces. The runner does not execute the source; the
/// executor does.
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

/// The side-effecting dependencies of a write transaction. Production
/// implementations wrap the execution coordinator, connector control binding,
/// and cache finalization; tests inject a fake.
pub(crate) trait IcebergWriteTransactionExecutor {
    /// Run the coordinated writer plan, returning the writer outcome.
    fn run_coordinated_write(
        &self,
        spec: &IcebergWriteTransactionSpec,
    ) -> Result<QueryExecutionResult, String>;

    /// Commit a provider-neutral staged writer completion.  The default is
    /// deliberately absent: callers that have not been migrated must not
    /// accidentally reinterpret generic reports through the legacy carrier.
    fn commit_connector_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        _completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Option<Result<CommitOutcome, CommitServiceError>> {
        None
    }

    /// Post-commit finalization (cache invalidation).
    fn finalize(&self, spec: &IcebergWriteTransactionSpec) -> Result<(), String>;
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
    /// Commit provider-private reports that were reconstructed by a connector
    /// control binding.  This is the narrow bridge from the generic writer
    /// contract into Iceberg's existing collector and commit service: generic
    /// callers never need to construct the legacy native commit carrier.
    pub(crate) fn commit_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut writer_files = Vec::new();
        self.convert_iceberg_writer_reports(reports, &mut writer_files)?;
        self.collector.inject_written_files(writer_files);
        self.run_commit_after_collector_injection()
    }

    /// Best-effort cleanup for reports that are known not to have reached the
    /// catalog commit boundary.  It deliberately does not invoke a commit,
    /// reconcile, or generation takeover path.
    pub(crate) fn abort_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
    ) -> Result<CleanupAttempt, String> {
        let mut writer_files = Vec::new();
        for report in reports {
            let file = self.collector.convert_writer_report(report).map_err(|message| {
                let cleanup = self.cleanup_converted_writer_files(&writer_files);
                format!(
                    "convert Iceberg staged report during abort failed: {message}; cleanup attempted={}, errors={}",
                    cleanup.attempted, cleanup.error_count
                )
            })?;
            writer_files.push(file);
        }
        Ok(self.cleanup_converted_writer_files(&writer_files))
    }

    /// Commit generic staged reports for a multi-sink change stream.  The
    /// provider-control boundary retains the writer identity until routing,
    /// so no legacy native commit carrier participates in this path.
    pub(crate) fn commit_change_stream_staged_reports(
        &self,
        staged_reports: Vec<novarocks_spi::connector::ConnectorStagedReport>,
        plan: &ChangeStreamWriterCommitPlan,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let mut by_writer = Vec::with_capacity(staged_reports.len());
        for staged in staged_reports {
            staged.validate().map_err(|error| {
                CommitServiceError::invalid_input(format!(
                    "validate change-stream connector staged report: {error}"
                ))
            })?;
            let reports = crate::connector::iceberg::write_contract::decode_writer_reports(
                staged.payload(),
                self.table.metadata(),
            )
            .map_err(CommitServiceError::invalid_input)?;
            by_writer.push(ChangeStreamWriterReports {
                fragment_id: staged.writer().fragment_id(),
                reports,
            });
        }
        let routed = route_change_stream_staged_reports(&self.collector, by_writer, plan).map_err(
            |error| {
                let (message, converted_files) = error.into_parts();
                CommitServiceError::known_uncommitted(
                    message,
                    self.cleanup_converted_writer_files(&converted_files),
                )
            },
        )?;
        routed.inject(&self.collector);
        self.run_commit_after_collector_injection()
    }

    fn run_commit_after_collector_injection(&self) -> Result<CommitOutcome, CommitServiceError> {
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

        match block_on_iceberg(async { run_iceberg_commit(input).await }) {
            Ok(result) => result,
            Err(message) => Err(CommitServiceError::known_uncommitted(
                message,
                CleanupAttempt::not_attempted(),
            )),
        }
    }

    fn convert_iceberg_writer_reports(
        &self,
        reports: impl IntoIterator<Item = IcebergWriterReport>,
        writer_files: &mut Vec<WrittenFile>,
    ) -> Result<(), CommitServiceError> {
        for report in reports {
            match self.collector.convert_writer_report(report) {
                Ok(file) => writer_files.push(file),
                Err(message) => {
                    let cleanup = self.cleanup_converted_writer_files(writer_files);
                    return Err(CommitServiceError::known_uncommitted(message, cleanup));
                }
            }
        }
        Ok(())
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
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.state, &self.target)
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
        .any(|writer| !writer.connector_staged_report_frames.is_empty())
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

        if let Some(completion) = written.connector_completion.as_ref() {
            self.transition(operation_id, IcebergOperationState::Committing)?;
            let commit = self.executor.commit_connector_write(&spec, completion).ok_or_else(|| {
                format!(
                    "iceberg write operation {operation_id} completed through the connector writer carrier, \
                     but its transaction executor has no connector commit implementation"
                )
            })?;
            return self.finish_commit(operation_id, written.query_result, spec, commit);
        }

        let Some(write_commit) = written.write_commit.as_ref() else {
            self.transition(operation_id, IcebergOperationState::Aborting)?;
            self.transition(operation_id, IcebergOperationState::Aborted)?;
            return Ok(IcebergWriteTransactionOutcome {
                query_result: written.query_result,
                operation_id: None,
                committed_snapshot_id: None,
            });
        };
        if write_commit_has_files(write_commit) {
            let message = format!(
                "iceberg write operation {operation_id} completed with staged output but no connector write completion"
            );
            let error = CommitServiceError::known_uncommitted(
                message.clone(),
                CleanupAttempt::not_attempted(),
            );
            self.record_fact(operation_id, operation_fact_from_commit_result(Err(&error)))?;
            return Err(message);
        }
        self.transition(operation_id, IcebergOperationState::Aborting)?;
        self.transition(operation_id, IcebergOperationState::Aborted)?;
        Ok(IcebergWriteTransactionOutcome {
            query_result: written.query_result,
            operation_id: None,
            committed_snapshot_id: None,
        })
    }

    fn finish_commit(
        &self,
        operation_id: i64,
        query_result: QueryResult,
        spec: IcebergWriteTransactionSpec,
        commit: Result<CommitOutcome, CommitServiceError>,
    ) -> Result<IcebergWriteTransactionOutcome, String> {
        match commit {
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
                            query_result,
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
    use crate::common::types::UniqueId;
    use crate::connector::iceberg::commit::{CommitOutcome, CommitServiceError};
    use crate::meta::repository::iceberg_operation::IcebergOperationState;
    use crate::query_execution::write::{WriteCommitInput, WriterCommitInput, WriterKey};
    use crate::runtime::query_result::QueryResult;
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
        let write_id = UniqueId::new(10, 20);
        let writer_key = WriterKey {
            query_id: write_id,
            fragment_instance_id: UniqueId::new(101, 201),
            backend_num: 0,
        };
        WriteCommitInput {
            write_id,
            writers: vec![WriterCommitInput {
                writer_id: 0,
                fragment_id: 0,
                writer_key,
                connector_staged_report_frames: Vec::new(),
                load_counters: BTreeMap::new(),
                loaded_rows: 0,
                loaded_bytes: 0,
                filtered_rows: 0,
            }],
        }
    }

    fn write_commit_with_unattached_staged_output() -> WriteCommitInput {
        let mut commit = write_commit_with_writer_without_files();
        commit.writers[0]
            .connector_staged_report_frames
            .push(crate::proto::novarocks::ConnectorStagedReportFrame::default());
        commit
    }

    struct FakeExecutor {
        write: RefCell<Option<Result<QueryExecutionResult, String>>>,
        // Retained only so test fixtures can describe outcomes from the
        // retired carrier while the runner verifies they are never consumed.
        commit: RefCell<Option<Result<CommitOutcome, CommitServiceError>>>,
        finalize: Result<(), String>,
    }

    impl IcebergWriteTransactionExecutor for FakeExecutor {
        fn run_coordinated_write(
            &self,
            _spec: &IcebergWriteTransactionSpec,
        ) -> Result<QueryExecutionResult, String> {
            self.write
                .borrow_mut()
                .take()
                .expect("write outcome set once")
        }

        fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
            self.finalize.clone()
        }
    }

    fn one_writer_abort() -> crate::query_execution::write::WriteAbortInput {
        crate::engine::write_operation_lifecycle::test_support::write_abort_with_data_file()
    }

    #[test]
    fn staged_output_without_connector_completion_fails_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_unattached_staged_output()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 1234,
                written_manifest_paths: vec!["s3://bucket/m.avro".to_string()],
            }))),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let error = runner.run(sample_spec()).expect_err("must fail closed");
        assert!(error.contains("no connector write completion"), "{error}");

        let read = env.provider.begin_read().expect("read txn");
        let stored = env
            .state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), 1)
            .expect("load")
            .expect("present");
        assert_eq!(stored.state, IcebergOperationState::FailedKnownUncommitted);
        assert_eq!(stored.commit_outcome, None);
    }

    #[test]
    fn writer_abort_records_failed_known_uncommitted() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: Some(one_writer_abort()),
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
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
    #[ignore = "legacy commit carrier retired; connector control outcome tests own this case"]
    fn commit_known_uncommitted_records_failed_known_uncommitted() {
        use crate::connector::iceberg::commit::CleanupAttempt;
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
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
    #[ignore = "legacy commit carrier retired; connector control outcome tests own this case"]
    fn commit_unknown_records_commit_unknown_and_skips_finalize() {
        use crate::connector::iceberg::commit::RecoveryEvidence;
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
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
    #[ignore = "legacy commit carrier retired; connector control outcome tests own this case"]
    fn finalize_failure_records_finalize_failed_known_committed() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_one_writer()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 9,
                written_manifest_paths: Vec::new(),
            }))),
            finalize: Err("cache invalidation failed".to_string()),
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
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: None,
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
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
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_writer_without_files()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(None),
            finalize: Ok(()),
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
    #[ignore = "legacy commit carrier retired; connector control outcome tests own this case"]
    fn runner_commits_fileless_non_fast_append_write_through_op_kind_gate() {
        let env = test_env();
        let exec = FakeExecutor {
            write: RefCell::new(Some(Ok(QueryExecutionResult {
                query_result: empty_query_result(),
                write_commit: Some(write_commit_with_writer_without_files()),
                write_abort: None,
                connector_completion: None,
                fragment_profiles: Vec::new(),
            }))),
            commit: RefCell::new(Some(Ok(CommitOutcome {
                new_snapshot_id: 4321,
                written_manifest_paths: Vec::new(),
            }))),
            finalize: Ok(()),
        };
        let runner = IcebergWriteTransactionRunner::new(Arc::clone(&env.state), &exec);
        let mut spec = sample_spec();
        spec.commit.commit_op_kind = CommitOpKind::RowDelta;
        let outcome = runner
            .run(spec)
            .expect("non-FastAppend file-less writes must enter commit");
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
