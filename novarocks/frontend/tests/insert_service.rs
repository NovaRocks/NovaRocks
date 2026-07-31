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

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks::common::app_config::ClusterRole;
use novarocks::engine::insert_engine::{
    AppendBatchRequest, AppendRowsRequest, IcebergInsertCommit, IcebergInsertOperation,
    IcebergPreparedInsert, IcebergWriteReport, InsertEngine, InsertOverwriteMode,
    InsertQueryRequest, InsertTargetBackend, InsertValue, PrepareIcebergInsert,
    PreparedIcebergInsert, QueryInsertBatch, QueryInsertColumn, ResolveInsertTarget,
    ResolvedInsertTarget,
};
use novarocks::engine::statistics::{
    CatalogTableStatistics, CollectedColumnStatistics, StatisticsColumn, StatisticsEngine,
    StatisticsInsertObservation, StatisticsRequestContext, StatisticsService,
    StatisticsStatementResult, StatisticsTableTarget,
};
use novarocks::query_execution::backend::BackendTopologySnapshot;
use novarocks::query_execution::cancellation::QueryCancellationSource;
use novarocks::query_execution::request_context::{RequestAdmission, RequestContext};
use novarocks::runtime::query_result::QueryResult;
use novarocks_catalog::schema::ColumnDef;
use novarocks_frontend::dml::model::DML_OPERATION_SCHEMA_VERSION;
use novarocks_frontend::dml::{
    CommitOpKind, CommitOutcome, CommitServiceError, CreatePreparingRequest, DmlError,
    DmlErrorKind, DmlOperationId, DmlService, OperationFact, OperationJournal, OperationState,
    RecoveryEvidence, StoredOperation,
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
enum Call {
    Resolve {
        target: Vec<String>,
        topology_revision: u64,
        deadline: Option<Instant>,
    },
    AppendRows(Vec<Vec<InsertValue>>),
    ExecuteQuery {
        topology_revision: u64,
        deadline: Option<Instant>,
    },
    AppendBatch {
        rows: usize,
        topology_revision: u64,
    },
    Prepare {
        target_ref: String,
        overwrite_mode: InsertOverwriteMode,
        topology_revision: u64,
        insert_columns: Vec<String>,
    },
    Run,
    Commit,
    Finalize,
}

#[derive(Clone, Copy)]
enum WriteBehavior {
    NoOp,
    Committable,
    Aborted,
}

#[derive(Clone, Copy)]
enum CommitBehavior {
    Success,
    Unknown,
}

struct FakePrepared;

impl IcebergPreparedInsert for FakePrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct FakeCommit;

impl IcebergInsertCommit for FakeCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct FakeInsertEngine {
    target: Mutex<ResolvedInsertTarget>,
    calls: Mutex<Vec<Call>>,
    query_result: Mutex<QueryInsertBatch>,
    write_behavior: Mutex<WriteBehavior>,
    commit_behavior: Mutex<CommitBehavior>,
    prepare_error: Mutex<Option<String>>,
}

impl FakeInsertEngine {
    fn new(target: ResolvedInsertTarget) -> Self {
        Self {
            target: Mutex::new(target),
            calls: Mutex::new(Vec::new()),
            query_result: Mutex::new(QueryInsertBatch {
                columns: vec![QueryInsertColumn {
                    name: "a".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                }],
                batches: vec![
                    RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
                        vec![Arc::new(Int64Array::from(vec![5_i64]))],
                    )
                    .unwrap(),
                ],
            }),
            write_behavior: Mutex::new(WriteBehavior::Committable),
            commit_behavior: Mutex::new(CommitBehavior::Success),
            prepare_error: Mutex::new(None),
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn set_write_behavior(&self, behavior: WriteBehavior) {
        *self.write_behavior.lock().unwrap() = behavior;
    }

    fn set_commit_behavior(&self, behavior: CommitBehavior) {
        *self.commit_behavior.lock().unwrap() = behavior;
    }

    fn set_prepare_error(&self, message: &str) {
        *self.prepare_error.lock().unwrap() = Some(message.to_string());
    }
}

impl StatisticsEngine for FakeInsertEngine {
    fn resolve_table_columns(
        &self,
        _target: &StatisticsTableTarget,
    ) -> Result<Vec<StatisticsColumn>, String> {
        Ok(Vec::new())
    }

    fn resolve_local_table_columns(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, String> {
        Ok(None)
    }

    fn collect_table_statistics(
        &self,
        _target: &StatisticsTableTarget,
        _columns: &[String],
    ) -> Result<Vec<CollectedColumnStatistics>, String> {
        Ok(Vec::new())
    }
}

impl InsertEngine for FakeInsertEngine {
    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String> {
        self.calls.lock().unwrap().push(Call::Resolve {
            target: request.target.parts,
            topology_revision: request.execution.topology().revision(),
            deadline: request.execution.deadline(),
        });
        Ok(self.target.lock().unwrap().clone())
    }

    fn append_rows(&self, request: AppendRowsRequest) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::AppendRows(request.rows));
        Ok(())
    }

    fn execute_insert_query(
        &self,
        request: InsertQueryRequest,
    ) -> Result<QueryInsertBatch, String> {
        self.calls.lock().unwrap().push(Call::ExecuteQuery {
            topology_revision: request.execution.topology().revision(),
            deadline: request.execution.deadline(),
        });
        Ok(self.query_result.lock().unwrap().clone())
    }

    fn append_batch(&self, request: AppendBatchRequest) -> Result<(), String> {
        self.calls.lock().unwrap().push(Call::AppendBatch {
            rows: request.batch.num_rows(),
            topology_revision: request.execution.topology().revision(),
        });
        Ok(())
    }

    fn prepare_iceberg_write(
        &self,
        request: PrepareIcebergInsert,
    ) -> Result<PreparedIcebergInsert, String> {
        self.calls.lock().unwrap().push(Call::Prepare {
            target_ref: request.target_ref.clone(),
            overwrite_mode: request.overwrite_mode,
            topology_revision: request.execution.topology().revision(),
            insert_columns: request.insert_columns,
        });
        if let Some(message) = self.prepare_error.lock().unwrap().clone() {
            return Err(message);
        }
        let commit_op_kind = match request.overwrite_mode {
            InsertOverwriteMode::Append => CommitOpKind::FastAppend,
            InsertOverwriteMode::FullTable => CommitOpKind::Overwrite,
            InsertOverwriteMode::DynamicPartitions => CommitOpKind::OverwritePartitions,
        };
        Ok(PreparedIcebergInsert {
            operation: IcebergInsertOperation {
                catalog: request.target.catalog,
                namespace: request.target.namespace,
                table: request.target.table,
                target_ref: request.target_ref,
                attempt_id: "fake-attempt".to_string(),
                commit_op_kind,
                base_snapshot_id: Some(10),
            },
            handle: Arc::new(FakePrepared),
        })
    }

    fn run_iceberg_write(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String> {
        self.calls.lock().unwrap().push(Call::Run);
        Ok(match *self.write_behavior.lock().unwrap() {
            WriteBehavior::NoOp => IcebergWriteReport::NoOp(Arc::new(FakeCommit)),
            WriteBehavior::Committable => IcebergWriteReport::Committable(Arc::new(FakeCommit)),
            WriteBehavior::Aborted => IcebergWriteReport::Aborted {
                reason: "writer aborted".to_string(),
                has_staged_files: true,
            },
        })
    }

    fn commit_iceberg_write(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _commit: &dyn IcebergInsertCommit,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.calls.lock().unwrap().push(Call::Commit);
        match *self.commit_behavior.lock().unwrap() {
            CommitBehavior::Success => Ok(CommitOutcome {
                new_snapshot_id: 11,
                written_manifest_paths: Vec::new(),
            }),
            CommitBehavior::Unknown => Err(CommitServiceError::unknown(
                "connection reset by peer".to_string(),
                RecoveryEvidence {
                    table_ident: "ice.db.t".to_string(),
                    op_kind: CommitOpKind::FastAppend,
                    base_snapshot_id: Some(10),
                    base_sequence_number: 10,
                    staging_dir: "s3://warehouse/_staging/fake".to_string(),
                },
            )),
        }
    }

    fn finalize_iceberg_write(&self, _prepared: &dyn IcebergPreparedInsert) -> Result<(), String> {
        self.calls.lock().unwrap().push(Call::Finalize);
        Ok(())
    }
}

struct RecordingStatistics {
    insert_count: AtomicUsize,
    fail_insert: AtomicBool,
}

impl RecordingStatistics {
    fn new() -> Self {
        Self {
            insert_count: AtomicUsize::new(0),
            fail_insert: AtomicBool::new(false),
        }
    }

    fn fail_insert(&self) {
        self.fail_insert.store(true, Ordering::SeqCst);
    }
}

impl StatisticsService for RecordingStatistics {
    fn try_handle_statement(
        &self,
        _engine: &dyn StatisticsEngine,
        _sql: &str,
        _context: StatisticsRequestContext<'_>,
    ) -> Result<Option<StatisticsStatementResult>, String> {
        Ok(None)
    }

    fn try_query(
        &self,
        _sql: &str,
        _query: &sqlparser::ast::Query,
        _context: StatisticsRequestContext<'_>,
    ) -> Result<Option<QueryResult>, String> {
        Ok(None)
    }

    fn observe_query(
        &self,
        _query: &sqlparser::ast::Query,
        _current_database: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    fn observe_insert(
        &self,
        _engine: &dyn StatisticsEngine,
        _observation: StatisticsInsertObservation<'_>,
    ) -> Result<(), String> {
        self.insert_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_insert.load(Ordering::SeqCst) {
            Err("statistics observation failed".to_string())
        } else {
            Ok(())
        }
    }

    fn observe_update(&self, _sql: &str, _current_database: &str) -> Result<(), String> {
        Ok(())
    }

    fn drop_table(&self, _database: &str, _table: &str) {}

    fn drop_database(&self, _database: &str) {}

    fn catalog_table_statistics(
        &self,
        _database: &str,
        _table: &str,
    ) -> Result<Option<CatalogTableStatistics>, String> {
        Ok(None)
    }
}

#[derive(Default)]
struct FakeJournal {
    operations: Mutex<BTreeMap<Uuid, StoredOperation>>,
}

impl FakeJournal {
    fn states(&self) -> Vec<OperationState> {
        self.operations
            .lock()
            .unwrap()
            .values()
            .map(|operation| operation.state)
            .collect()
    }
}

impl OperationJournal for FakeJournal {
    fn create_preparing(
        &self,
        request: CreatePreparingRequest,
    ) -> Result<DmlOperationId, DmlError> {
        let operation_id = DmlOperationId::new_v7();
        self.operations.lock().unwrap().insert(
            *operation_id.as_uuid(),
            StoredOperation {
                schema_version: DML_OPERATION_SCHEMA_VERSION,
                operation_id,
                revision: 1,
                last_mutation_id: Uuid::now_v7(),
                operation_kind: request.operation_kind,
                operation_subkind: request.operation_subkind,
                target: request.target,
                state: OperationState::Preparing,
                attempt_id: request.attempt_id,
                base_snapshot_id: request.base_snapshot_id,
                base_snapshot_map: request.base_snapshot_map,
                staged_artifacts: request.staged_artifacts,
                commit_outcome: None,
                cleanup_outcome: None,
                recovery_evidence: None,
                failure: None,
                created_at_ms: request.created_at_ms,
                updated_at_ms: request.created_at_ms,
                finished_at_ms: None,
            },
        );
        Ok(operation_id)
    }

    fn transition(&self, operation_id: DmlOperationId, to: OperationState) -> Result<(), DmlError> {
        let mut operations = self.operations.lock().unwrap();
        let operation = operations
            .get_mut(operation_id.as_uuid())
            .expect("fake operation");
        operation.state = to;
        operation.revision += 1;
        Ok(())
    }

    fn record_fact(
        &self,
        operation_id: DmlOperationId,
        fact: OperationFact,
    ) -> Result<(), DmlError> {
        let mut operations = self.operations.lock().unwrap();
        let operation = operations
            .get_mut(operation_id.as_uuid())
            .expect("fake operation");
        operation.state = fact.state;
        operation.commit_outcome = fact.commit_outcome;
        operation.cleanup_outcome = fact.cleanup_outcome;
        operation.recovery_evidence = fact.recovery_evidence;
        operation.failure = fact.failure;
        operation.revision += 1;
        Ok(())
    }

    fn load(&self, operation_id: DmlOperationId) -> Result<Option<StoredOperation>, DmlError> {
        Ok(self
            .operations
            .lock()
            .unwrap()
            .get(operation_id.as_uuid())
            .cloned())
    }

    fn list_operations(&self) -> Result<Vec<StoredOperation>, DmlError> {
        Ok(self.operations.lock().unwrap().values().cloned().collect())
    }

    fn list_unfinished(&self) -> Result<Vec<StoredOperation>, DmlError> {
        Ok(self
            .operations
            .lock()
            .unwrap()
            .values()
            .filter(|operation| !operation.state.is_finished())
            .cloned()
            .collect())
    }
}

fn column(name: &str, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type: DataType::Int64,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn target(backend: InsertTargetBackend, supports_pipeline_insert: bool) -> ResolvedInsertTarget {
    ResolvedInsertTarget {
        backend,
        catalog: if backend == InsertTargetBackend::Iceberg {
            "ice".to_string()
        } else {
            "default_catalog".to_string()
        },
        namespace: "db".to_string(),
        table: "t".to_string(),
        columns: vec![column("a", false), column("b", true)],
        supports_pipeline_insert,
    }
}

fn context() -> (RequestContext, QueryCancellationSource, Instant) {
    let cancellation = QueryCancellationSource::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    (
        RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(91),
            Some(deadline),
            cancellation.view(),
            Default::default(),
        )),
        cancellation,
        deadline,
    )
}

fn service(journal: Option<Arc<FakeJournal>>, statistics: Arc<RecordingStatistics>) -> DmlService {
    DmlService::compose(
        journal.map(|journal| journal as Arc<dyn OperationJournal>),
        statistics,
    )
}

#[test]
fn non_insert_returns_none_without_engine_calls() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Local, false));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    assert_eq!(
        service(None, statistics)
            .try_execute_insert(&engine, "DELETE FROM t WHERE a = 1", &context, None)
            .unwrap(),
        None
    );
    assert!(engine.calls().is_empty());
}

#[test]
fn local_insert_without_state_store_is_reordered_and_appended() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Local, false));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    service(None, Arc::clone(&statistics))
        .try_execute_insert(&engine, "INSERT INTO t (a) VALUES (7)", &context, None)
        .unwrap();
    assert_eq!(
        engine.calls(),
        vec![
            Call::Resolve {
                target: vec!["t".to_string()],
                topology_revision: 91,
                deadline: context.execution().deadline(),
            },
            Call::AppendRows(vec![vec![InsertValue::Int(7), InsertValue::Null]]),
        ]
    );
    assert_eq!(statistics.insert_count.load(Ordering::SeqCst), 1);
}

#[test]
fn starrocks_insert_without_state_store_executes_aligns_then_appends_batch() {
    let mut resolved = target(InsertTargetBackend::StarRocks, true);
    resolved.columns = vec![column("a", false)];
    let engine = FakeInsertEngine::new(resolved);
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, deadline) = context();
    service(None, statistics)
        .try_execute_insert(
            &engine,
            "INSERT INTO t (a) SELECT a FROM src",
            &context,
            None,
        )
        .unwrap();
    assert_eq!(
        engine.calls(),
        vec![
            Call::Resolve {
                target: vec!["t".to_string()],
                topology_revision: 91,
                deadline: Some(deadline),
            },
            Call::ExecuteQuery {
                topology_revision: 91,
                deadline: Some(deadline),
            },
            Call::AppendBatch {
                rows: 1,
                topology_revision: 91,
            },
        ]
    );
}

#[test]
fn unsupported_pipeline_insert_fails_before_query_execution() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Local, false));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(None, statistics)
        .try_execute_insert(
            &engine,
            "INSERT INTO t SELECT a, b FROM src",
            &context,
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("does not support INSERT SELECT"));
    assert_eq!(engine.calls().len(), 1);
}

#[test]
fn union_all_dispatches_parts_in_source_order() {
    let mut resolved = target(InsertTargetBackend::Local, false);
    resolved.columns = vec![column("a", false)];
    let engine = FakeInsertEngine::new(resolved);
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    service(None, statistics)
        .try_execute_insert(
            &engine,
            "INSERT INTO t SELECT 1 UNION ALL SELECT 2",
            &context,
            None,
        )
        .unwrap();
    let calls = engine.calls();
    assert_eq!(
        &calls[1..],
        &[
            Call::AppendRows(vec![vec![InsertValue::Int(1)]]),
            Call::AppendRows(vec![vec![InsertValue::Int(2)]]),
        ]
    );
}

#[test]
fn non_iceberg_overwrite_is_rejected() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::StarRocks, true));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(None, statistics)
        .try_execute_insert(&engine, "INSERT OVERWRITE t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert!(error.to_string().contains("only supported for iceberg"));
    assert_eq!(engine.calls().len(), 1);
}

#[test]
fn dynamic_overwrite_requires_iceberg() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Local, false));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(None, statistics)
        .try_execute_insert(
            &engine,
            "INSERT OVERWRITE PARTITIONS TABLE t SELECT 1, 2",
            &context,
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("OVERWRITE PARTITIONS"));
    assert_eq!(engine.calls().len(), 1);
}

#[test]
fn tag_target_is_read_only() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(Some(Arc::new(FakeJournal::default())), statistics)
        .try_execute_insert(
            &engine,
            "INSERT INTO t.tag_v1 VALUES (1, 2)",
            &context,
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("tag 'v1' is read-only"));
    assert!(matches!(
        engine.calls().as_slice(),
        [Call::Resolve { target, .. }] if target == &vec!["t".to_string()]
    ));
}

#[test]
fn branch_insert_requires_iceberg_v3() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    engine.set_prepare_error("iceberg ref: branch writes require Iceberg v3 tables");
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(Some(Arc::new(FakeJournal::default())), statistics)
        .try_execute_insert(
            &engine,
            "INSERT INTO t.branch_dev VALUES (1, 2)",
            &context,
            None,
        )
        .unwrap_err();
    assert!(error.to_string().contains("require Iceberg v3"));
    assert!(engine.calls().iter().any(|call| matches!(
        call,
        Call::Prepare { target_ref, .. } if target_ref == "dev"
    )));
}

#[test]
fn iceberg_without_journal_fails_before_prepare() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(None, Arc::clone(&statistics))
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.to_string().contains("state store is required"));
    assert!(matches!(
        engine.calls().as_slice(),
        [Call::Resolve { target, .. }] if target == &vec!["t".to_string()]
    ));
    assert_eq!(statistics.insert_count.load(Ordering::SeqCst), 0);
}

#[test]
fn iceberg_append_empty_records_aborted_noop() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    engine.set_write_behavior(WriteBehavior::NoOp);
    let journal = Arc::new(FakeJournal::default());
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    service(Some(Arc::clone(&journal)), statistics)
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap();
    assert_eq!(journal.states(), vec![OperationState::Aborted]);
    assert!(!engine.calls().contains(&Call::Commit));
    assert!(!engine.calls().contains(&Call::Finalize));
}

#[test]
fn iceberg_overwrite_empty_commits_and_finalizes() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    engine.set_write_behavior(WriteBehavior::NoOp);
    let journal = Arc::new(FakeJournal::default());
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    service(Some(Arc::clone(&journal)), statistics)
        .try_execute_insert(&engine, "INSERT OVERWRITE t VALUES (1, 2)", &context, None)
        .unwrap();
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert!(engine.calls().contains(&Call::Commit));
    assert!(engine.calls().contains(&Call::Finalize));
}

#[test]
fn iceberg_commit_unknown_is_persisted_without_retry() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    engine.set_commit_behavior(CommitBehavior::Unknown);
    let journal = Arc::new(FakeJournal::default());
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(Some(Arc::clone(&journal)), statistics)
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::Commit);
    assert_eq!(journal.states(), vec![OperationState::CommitUnknown]);
    assert_eq!(
        engine
            .calls()
            .iter()
            .filter(|call| **call == Call::Commit)
            .count(),
        1
    );
    assert!(!engine.calls().contains(&Call::Finalize));
}

#[test]
fn admitted_context_reaches_insert_select_and_iceberg_write() {
    let mut starrocks_target = target(InsertTargetBackend::StarRocks, true);
    starrocks_target.columns = vec![column("a", false)];
    let starrocks = FakeInsertEngine::new(starrocks_target);
    let statistics = Arc::new(RecordingStatistics::new());
    let (query_context, _, deadline) = context();
    service(None, Arc::clone(&statistics))
        .try_execute_insert(
            &starrocks,
            "INSERT INTO t SELECT a FROM src",
            &query_context,
            None,
        )
        .unwrap();
    assert!(starrocks.calls().iter().any(|call| matches!(
        call,
        Call::ExecuteQuery { topology_revision: 91, deadline: Some(value) } if *value == deadline
    )));

    let iceberg = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    let (write_context, _, write_deadline) = context();
    service(
        Some(Arc::new(FakeJournal::default())),
        Arc::clone(&statistics),
    )
    .try_execute_insert(
        &iceberg,
        "INSERT INTO t VALUES (1, 2)",
        &write_context,
        None,
    )
    .unwrap();
    assert!(iceberg.calls().iter().any(|call| matches!(
        call,
        Call::Prepare {
            topology_revision: 91,
            ..
        }
    )));
    assert!(iceberg.calls().iter().any(|call| matches!(
        call,
        Call::Resolve { deadline: Some(value), .. } if *value == write_deadline
    )));
}

#[test]
fn statistics_runs_once_after_success() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Local, false));
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    service(None, Arc::clone(&statistics))
        .try_execute_insert(
            &engine,
            "INSERT INTO t VALUES (1, 2), (3, 4)",
            &context,
            None,
        )
        .unwrap();
    assert_eq!(statistics.insert_count.load(Ordering::SeqCst), 1);
}

#[test]
fn statistics_error_does_not_change_finalized_operation() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    let journal = Arc::new(FakeJournal::default());
    let statistics = Arc::new(RecordingStatistics::new());
    statistics.fail_insert();
    let (context, _, _) = context();
    let error = service(Some(Arc::clone(&journal)), Arc::clone(&statistics))
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert!(error.to_string().contains("statistics observation failed"));
    assert_eq!(statistics.insert_count.load(Ordering::SeqCst), 1);
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert_eq!(
        engine
            .calls()
            .iter()
            .filter(|call| **call == Call::Commit)
            .count(),
        1
    );
}

#[test]
fn writer_abort_is_recorded_without_commit() {
    let engine = FakeInsertEngine::new(target(InsertTargetBackend::Iceberg, true));
    engine.set_write_behavior(WriteBehavior::Aborted);
    let journal = Arc::new(FakeJournal::default());
    let statistics = Arc::new(RecordingStatistics::new());
    let (context, _, _) = context();
    let error = service(Some(Arc::clone(&journal)), statistics)
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert!(error.to_string().contains("aborted before commit"));
    assert!(!engine.calls().contains(&Call::Commit));
    assert_eq!(
        journal.states(),
        vec![OperationState::FailedKnownUncommitted]
    );
}
