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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;
use bytes::Bytes;
use common::coordination_fixture::JournalInspect;
use novarocks::common::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks::common::backend_topology::BackendTopologySnapshot;
use novarocks::common::query_cancellation::QueryCancellationSource;
use novarocks_catalog::schema::ColumnDef;
use novarocks_frontend::FrontendStatisticsService;
use novarocks_frontend::dml::model::DML_OPERATION_SCHEMA_VERSION;
use novarocks_frontend::dml::{
    CreatePreparingRequest, DmlError, DmlErrorKind, DmlOperationId, DmlService, OperationFact,
    OperationJournal, OperationState, StoredOperation,
};
use novarocks_frontend::query_execution::dml::insert::{
    IcebergInsertCommit, IcebergInsertOperation, IcebergInsertSource, IcebergPreparedInsert,
    IcebergWriteReport, InsertEngine, InsertOverwriteMode, InsertValue, PrepareIcebergInsert,
    PreparedIcebergInsert, ResolveInsertTarget, ResolvedInsertTarget,
};
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorControlBinding, ConnectorControlPlanningLease,
    ConnectorError, ConnectorErrorKind, ConnectorExecutionDeclaration,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMetadata,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorMutationOperationId,
    ConnectorNamespaceRequest, ConnectorProviderId, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorSplitPlanningRequest, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest, ConnectorWriteReceipt, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
};
use novarocks_types::ClusterRole;
use uuid::Uuid;

mod common;

#[derive(Clone, Debug, PartialEq)]
enum Call {
    Resolve {
        target: Vec<String>,
        topology_revision: u64,
        deadline: Option<Instant>,
    },
    Prepare {
        target_ref: String,
        overwrite_mode: InsertOverwriteMode,
        topology_revision: u64,
        insert_columns: Vec<String>,
        source: IcebergInsertSource,
    },
    Run,
    Commit,
    Finalize,
}

#[derive(Clone, Copy)]
enum WriteBehavior {
    FilelessOutput,
    Committable,
    Aborted,
}

#[derive(Clone, Copy)]
enum CommitBehavior {
    Success,
    Unknown,
}

struct FakePrepared {
    is_overwrite: bool,
}

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
    write_behavior: Mutex<WriteBehavior>,
    commit_behavior: Mutex<CommitBehavior>,
    prepare_error: Mutex<Option<String>>,
}

impl FakeInsertEngine {
    fn new(target: ResolvedInsertTarget) -> Self {
        Self {
            target: Mutex::new(target),
            calls: Mutex::new(Vec::new()),
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

impl InsertEngine for FakeInsertEngine {
    /// Distributed write fails closed until a fence is established, so the fake
    /// engine must expose a real write authority to fence against.
    fn establish_iceberg_write_external_fence(
        &self,
        _prepared: &dyn novarocks_frontend::query_execution::dml::insert::IcebergPreparedInsert,
        proposal: &dyn novarocks_frontend::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        common::fence_fixture::establish_from_proposal(|operation_id, table, target_ref| {
            proposal.seal(operation_id, table, target_ref)
        })
    }

    fn resolve_target(&self, request: ResolveInsertTarget) -> Result<ResolvedInsertTarget, String> {
        self.calls.lock().unwrap().push(Call::Resolve {
            target: request.target.parts,
            topology_revision: request.execution.topology().revision(),
            deadline: request.execution.deadline(),
        });
        Ok(self.target.lock().unwrap().clone())
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
            source: request.source,
        });
        if let Some(message) = self.prepare_error.lock().unwrap().clone() {
            return Err(message);
        }
        let is_overwrite = !matches!(request.overwrite_mode, InsertOverwriteMode::Append);
        Ok(PreparedIcebergInsert {
            operation: IcebergInsertOperation {
                catalog: request.target.catalog,
                namespace: request.target.namespace,
                table: request.target.table,
                target_ref: request.target_ref,
                attempt_id: "fake-attempt".to_string(),
                is_overwrite,
                base_snapshot_id: Some(10),
            },
            handle: Arc::new(FakePrepared { is_overwrite }),
        })
    }

    fn run_iceberg_write(
        &self,
        prepared: &dyn IcebergPreparedInsert,
    ) -> Result<IcebergWriteReport, String> {
        self.calls.lock().unwrap().push(Call::Run);
        Ok(match *self.write_behavior.lock().unwrap() {
            WriteBehavior::FilelessOutput => {
                let prepared = prepared
                    .as_any()
                    .downcast_ref::<FakePrepared>()
                    .ok_or_else(|| "foreign fake prepared handle".to_string())?;
                if !prepared.is_overwrite {
                    IcebergWriteReport::NoOp
                } else {
                    IcebergWriteReport::CommitRequired(Arc::new(FakeCommit))
                }
            }
            WriteBehavior::Committable => IcebergWriteReport::CommitRequired(Arc::new(FakeCommit)),
            WriteBehavior::Aborted => IcebergWriteReport::Aborted {
                reason: "writer aborted".to_string(),
                has_staged_files: true,
            },
        })
    }

    fn iceberg_write_native_encoding<'a>(
        &self,
        _prepared: &'a dyn IcebergPreparedInsert,
    ) -> Result<
        novarocks_frontend::query_execution::dml::insert::PreparedIcebergWriteNativeEncoding<'a>,
        String,
    > {
        novarocks_frontend::query_execution::dml::insert::PreparedIcebergWriteNativeEncoding::test_fixture()
    }

    fn run_iceberg_write_with_native_bundle(
        &self,
        prepared: &dyn IcebergPreparedInsert,
        _native_bundle: novarocks_frontend::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<IcebergWriteReport, String> {
        self.run_iceberg_write(prepared)
    }

    fn commit_iceberg_write_terminal(
        &self,
        _prepared: &dyn IcebergPreparedInsert,
        _commit: &dyn IcebergInsertCommit,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        self.calls.lock().unwrap().push(Call::Commit);
        match *self.commit_behavior.lock().unwrap() {
            CommitBehavior::Success => Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: test_receipt(b"commit-success"),
                finalization: ExternalMutationFinalization::Complete,
            }),
            CommitBehavior::Unknown => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "connection reset by peer",
                ),
                evidence: test_evidence(),
            }),
        }
    }

    fn finalize_iceberg_write(&self, _prepared: &dyn IcebergPreparedInsert) -> Result<(), String> {
        self.calls.lock().unwrap().push(Call::Finalize);
        Ok(())
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

    fn only_operation(&self) -> StoredOperation {
        let operations = self.operations.lock().unwrap();
        assert_eq!(operations.len(), 1, "expected exactly one DML operation");
        operations.values().next().unwrap().clone()
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
                payload: novarocks_frontend::dml::model::OperationPayload::ConnectorWriteLifecycle(
                    novarocks_frontend::dml::model::ConnectorWriteLifecycleRecord::Pending,
                ),
                coordination_provenance: None,
                recovery_due_at_ms: None,
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
        operation.payload =
            novarocks_frontend::dml::model::OperationPayload::ConnectorWriteLifecycle(
                fact.lifecycle,
            );
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

fn test_receipt(bytes: &'static [u8]) -> ConnectorWriteReceipt {
    ConnectorWriteReceipt::try_new(Bytes::from_static(bytes)).expect("test receipt")
}

fn test_evidence() -> ExternalMutationEvidence {
    ExternalMutationEvidence::try_new(
        1,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
        },
        ConnectorInstanceIncarnation::from_bytes([1; 16]),
        ConnectorMutationOperationId::from_bytes([2; 16]),
        "test-insert",
        Bytes::from_static(b"opaque-evidence"),
    )
    .expect("test evidence")
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

struct InsertTestControl {
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorMetadata for InsertTestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        Err(unused_test_capability())
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        Err(unused_test_capability())
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        Err(unused_test_capability())
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        Err(unused_test_capability())
    }
}

impl ConnectorScanPlanning for InsertTestControl {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _table: &ConnectorTableHandle,
        _request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        Err(unused_test_capability())
    }

    fn plan_splits(
        &self,
        _scan: &ConnectorScanHandle,
        _request: ConnectorSplitPlanningRequest,
    ) -> Result<novarocks_spi::connector::ConnectorSplitPlanningResult, ConnectorError> {
        Err(unused_test_capability())
    }
}

impl ConnectorExecutionDistribution for InsertTestControl {
    fn declaration(
        &self,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::try_new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
                instance_id: self.instance_id.clone(),
            },
            self.incarnation,
            Bytes::from_static(b"insert-service-test"),
        )
    }
}

fn unused_test_capability() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "insert-service test connector capability is unused",
    )
}

fn target_planning_lease() -> ConnectorControlPlanningLease {
    let control = Arc::new(InsertTestControl {
        instance_id: ConnectorInstanceId::parse("ice").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    });
    let binding = ConnectorControlBinding::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: control.instance_id.clone(),
        },
        control.incarnation,
        control.clone(),
        control.clone(),
        control,
        None,
    )
    .expect("test control binding");
    ConnectorControlPlanningLease::new(Arc::new(binding), || {})
}

fn target() -> ResolvedInsertTarget {
    ResolvedInsertTarget {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "t".to_string(),
        columns: vec![column("a", false), column("b", true)],
        planning_lease: target_planning_lease(),
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

/// A service wired to real coordination over a real journal.
///
/// Distributed write is fenced now, and a fence can only be minted from a live
/// coordination lease, so a service composed without coordination cannot
/// dispatch at all. Derefs to `DmlService` so the call sites stay unchanged.
struct TestService {
    dml: DmlService,
    _coordination: common::coordination_fixture::BlockingCoordination,
}

impl std::ops::Deref for TestService {
    type Target = DmlService;

    fn deref(&self) -> &Self::Target {
        &self.dml
    }
}

fn service_over(
    coordination: common::coordination_fixture::BlockingCoordination,
    journal: Option<Arc<dyn OperationJournal>>,
) -> TestService {
    let dml = DmlService::compose_with_coordination(
        journal,
        Arc::new(FrontendStatisticsService::new()),
        Arc::clone(&coordination.coordination),
        coordination.handle(),
    );
    TestService {
        dml,
        _coordination: coordination,
    }
}

fn service(journal: Option<Arc<FakeJournal>>) -> TestService {
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = journal.map(|_| Arc::clone(&coordination.journal) as Arc<dyn OperationJournal>);
    service_over(coordination, journal)
}

#[test]
fn non_insert_returns_none_without_engine_calls() {
    let engine = FakeInsertEngine::new(target());
    let (context, _, _) = context();
    assert_eq!(
        service(None)
            .try_execute_insert(&engine, "DELETE FROM t WHERE a = 1", &context, None)
            .unwrap(),
        None
    );
    assert!(engine.calls().is_empty());
}

#[test]
fn union_all_commits_once_in_source_order() {
    let mut resolved = target();
    resolved.columns = vec![column("a", false)];
    let engine = FakeInsertEngine::new(resolved);
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();
    service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
    .try_execute_insert(
        &engine,
        "INSERT INTO t SELECT 1 UNION ALL SELECT 2",
        &context,
        None,
    )
    .unwrap();
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert_eq!(
        engine
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::Prepare { .. }))
            .count(),
        1
    );
    let Call::Prepare { source, .. } = engine
        .calls()
        .into_iter()
        .find(|call| matches!(call, Call::Prepare { .. }))
        .expect("prepare call")
    else {
        unreachable!();
    };
    assert_eq!(
        source,
        IcebergInsertSource::Rows(vec![vec![InsertValue::Int(1)], vec![InsertValue::Int(2)]])
    );
}

#[test]
fn tag_target_is_read_only() {
    let engine = FakeInsertEngine::new(target());
    let (context, _, _) = context();
    let error = service(Some(Arc::new(FakeJournal::default())))
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
    let engine = FakeInsertEngine::new(target());
    engine.set_prepare_error("iceberg ref: branch writes require Iceberg v3 tables");
    let (context, _, _) = context();
    let error = service(Some(Arc::new(FakeJournal::default())))
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
fn branch_insert_journals_the_prepared_branch_base_snapshot() {
    let engine = FakeInsertEngine::new(target());
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();

    service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
    .try_execute_insert(
        &engine,
        "INSERT INTO t.branch_dev VALUES (1, 2)",
        &context,
        None,
    )
    .expect("branch INSERT");

    let operation = journal.only_operation();
    assert_eq!(operation.target.ref_name.as_deref(), Some("dev"));
    assert_eq!(operation.base_snapshot_id, Some(10));
    assert_eq!(operation.state, OperationState::Finalized);
    let novarocks_frontend::dml::model::OperationPayload::ConnectorWriteLifecycle(
        novarocks_frontend::dml::ConnectorWriteLifecycleRecord::KnownCommitted {
            receipt_wire, ..
        },
    ) = operation.payload
    else {
        panic!("expected a provider-neutral commit receipt");
    };
    assert_eq!(
        receipt_wire.try_decode().expect("decode receipt"),
        test_receipt(b"commit-success")
    );
}

#[test]
fn iceberg_without_journal_fails_before_prepare() {
    let engine = FakeInsertEngine::new(target());
    let (context, _, _) = context();
    let error = service(None)
        .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.to_string().contains("state store is required"));
    assert!(matches!(
        engine.calls().as_slice(),
        [Call::Resolve { target, .. }] if target == &vec!["t".to_string()]
    ));
}

#[test]
fn iceberg_append_empty_records_known_empty_terminal_fact() {
    let engine = FakeInsertEngine::new(target());
    engine.set_write_behavior(WriteBehavior::FilelessOutput);
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();
    service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
    .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
    .unwrap();
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert!(!engine.calls().contains(&Call::Commit));
    assert!(!engine.calls().contains(&Call::Finalize));
}

#[test]
fn iceberg_overwrite_empty_commits_and_finalizes() {
    let engine = FakeInsertEngine::new(target());
    engine.set_write_behavior(WriteBehavior::FilelessOutput);
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();
    service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
    .try_execute_insert(&engine, "INSERT OVERWRITE t VALUES (1, 2)", &context, None)
    .unwrap();
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert!(engine.calls().contains(&Call::Commit));
    assert!(engine.calls().contains(&Call::Finalize));
}

#[test]
fn iceberg_commit_unknown_is_persisted_without_retry() {
    let engine = FakeInsertEngine::new(target());
    engine.set_commit_behavior(CommitBehavior::Unknown);
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();
    let error = service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
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
    let mut resolved = target();
    resolved.columns = vec![column("a", false)];
    let iceberg = FakeInsertEngine::new(resolved);
    let (write_context, _, write_deadline) = context();
    service(Some(Arc::new(FakeJournal::default())))
        .try_execute_insert(
            &iceberg,
            "INSERT INTO t SELECT a FROM src",
            &write_context,
            None,
        )
        .unwrap();
    assert!(iceberg.calls().iter().any(|call| matches!(
        call,
        Call::Prepare {
            topology_revision: 91,
            insert_columns,
            ..
        } if insert_columns.is_empty()
    )));
    assert!(iceberg.calls().iter().any(|call| matches!(
        call,
        Call::Resolve { deadline: Some(value), .. } if *value == write_deadline
    )));
}

#[test]
fn writer_abort_is_recorded_without_commit() {
    let engine = FakeInsertEngine::new(target());
    engine.set_write_behavior(WriteBehavior::Aborted);
    let coordination = common::coordination_fixture::open_blocking("insert-service-test");
    let journal = Arc::clone(&coordination.journal);
    let (context, _, _) = context();
    let error = service_over(
        coordination,
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
    )
    .try_execute_insert(&engine, "INSERT INTO t VALUES (1, 2)", &context, None)
    .unwrap_err();
    assert!(error.to_string().contains("writer aborted"));
    assert!(!engine.calls().contains(&Call::Commit));
    assert_eq!(
        journal.states(),
        vec![OperationState::FailedKnownUncommitted]
    );
}
