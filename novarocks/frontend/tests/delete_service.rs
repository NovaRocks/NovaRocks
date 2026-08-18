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

use bytes::Bytes;
use common::coordination_fixture::JournalInspect;
use novarocks::common::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks::common::backend_topology::BackendTopologySnapshot;
use novarocks::common::query_cancellation::QueryCancellationSource;
use novarocks_frontend::FrontendStatisticsService;
use novarocks_frontend::dml::model::DML_OPERATION_SCHEMA_VERSION;
use novarocks_frontend::dml::{
    CreatePreparingRequest, DmlError, DmlErrorKind, DmlOperationId, DmlService, OperationFact,
    OperationJournal, OperationKind, OperationState, StoredOperation,
};
use novarocks_frontend::query_execution::dml::delete::{
    DeleteCommit, DeleteEngine, DeleteOperation, DeletePrepared, DeleteStatementKind,
    DeleteWriteReport, PrepareDeleteRequest, PreparedDelete,
};
use novarocks_spi::connector::{
    ConnectorWriteReceipt, ExternalMutationEffect, ExternalMutationFinalization,
    ExternalMutationOutcome,
};
use novarocks_types::ClusterRole;
use uuid::Uuid;

mod common;

#[derive(Clone, Copy)]
enum WriteBehavior {
    NoOp,
    CommitRequired,
    Aborted,
}

#[derive(Default)]
struct FakePrepared;

impl DeletePrepared for FakePrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
struct FakeCommit;

impl DeleteCommit for FakeCommit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct FakeDeleteEngine {
    behavior: Mutex<WriteBehavior>,
    prepare_calls: Mutex<Vec<(DeleteStatementKind, u64, Option<Instant>)>>,
    run_calls: Mutex<usize>,
    commit_calls: Mutex<usize>,
    finalize_calls: Mutex<usize>,
}

impl FakeDeleteEngine {
    fn new(behavior: WriteBehavior) -> Self {
        Self {
            behavior: Mutex::new(behavior),
            prepare_calls: Mutex::new(Vec::new()),
            run_calls: Mutex::new(0),
            commit_calls: Mutex::new(0),
            finalize_calls: Mutex::new(0),
        }
    }
}

impl DeleteEngine for FakeDeleteEngine {
    /// Distributed write fails closed until a fence is established, so the fake
    /// engine must expose a real write authority to fence against.
    fn establish_delete_external_fence(
        &self,
        _prepared: &dyn novarocks_frontend::query_execution::dml::delete::DeletePrepared,
        proposal: &dyn novarocks_frontend::query_execution::dml::external_write_fence::ExternalWriteFenceProposal,
    ) -> Result<
        novarocks_spi::connector::ConnectorEstablishedWriteFence,
        novarocks_spi::connector::ConnectorError,
    > {
        common::fence_fixture::establish_from_proposal(|operation_id, table, target_ref| {
            proposal.seal(operation_id, table, target_ref)
        })
    }

    fn prepare_delete(&self, request: PrepareDeleteRequest<'_>) -> Result<PreparedDelete, String> {
        self.prepare_calls.lock().unwrap().push((
            request.kind,
            request.execution.topology().revision(),
            request.execution.deadline(),
        ));
        Ok(PreparedDelete {
            operation: DeleteOperation {
                catalog: request.current_catalog.unwrap_or_else(|| "ice".to_string()),
                namespace: request.current_database,
                table: "orders".to_string(),
                target_ref: "main".to_string(),
                attempt_id: "delete-test-attempt".to_string(),
                base_snapshot_id: Some(7),
            },
            handle: Arc::new(FakePrepared),
        })
    }

    fn run_delete(&self, _prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String> {
        *self.run_calls.lock().unwrap() += 1;
        Ok(match *self.behavior.lock().unwrap() {
            WriteBehavior::NoOp => DeleteWriteReport::NoOp,
            WriteBehavior::CommitRequired => {
                DeleteWriteReport::CommitRequired(Arc::new(FakeCommit))
            }
            WriteBehavior::Aborted => DeleteWriteReport::Aborted {
                reason: "writer aborted".to_string(),
                has_staged_files: true,
            },
        })
    }

    fn delete_native_encoding<'a>(
        &self,
        _prepared: &'a dyn DeletePrepared,
    ) -> Result<novarocks_frontend::query_execution::dml::delete::DeleteNativeEncoding<'a>, String>
    {
        novarocks_frontend::query_execution::dml::delete::DeleteNativeEncoding::test_fixture()
    }

    fn run_delete_with_native_bundle(
        &self,
        prepared: &dyn DeletePrepared,
        _native_bundle: novarocks_frontend::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<DeleteWriteReport, String> {
        self.run_delete(prepared)
    }

    fn commit_delete_terminal(
        &self,
        _prepared: &dyn DeletePrepared,
        _commit: &dyn DeleteCommit,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, String> {
        *self.commit_calls.lock().unwrap() += 1;
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: ConnectorWriteReceipt::try_new(Bytes::from_static(b"delete-commit"))
                .expect("test receipt"),
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn finalize_delete(&self, _prepared: &dyn DeletePrepared) -> Result<(), String> {
        *self.finalize_calls.lock().unwrap() += 1;
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

fn context() -> (RequestContext, QueryCancellationSource, Instant) {
    let cancellation = QueryCancellationSource::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    (
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
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

#[test]
fn non_delete_skips_engine_and_journal() {
    let engine = FakeDeleteEngine::new(WriteBehavior::NoOp);
    let (context, _, _) = context();
    assert_eq!(
        DmlService::compose(None, Arc::new(FrontendStatisticsService::new()))
            .try_execute_delete(&engine, "SELECT 1", &context, None)
            .unwrap(),
        None,
    );
    assert!(engine.prepare_calls.lock().unwrap().is_empty());
}

#[test]
fn delete_requires_journal_before_prepare() {
    let engine = FakeDeleteEngine::new(WriteBehavior::NoOp);
    let (context, _, _) = context();
    let error = DmlService::compose(None, Arc::new(FrontendStatisticsService::new()))
        .try_execute_delete(&engine, "DELETE FROM orders WHERE id = 1", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(engine.prepare_calls.lock().unwrap().is_empty());
}

#[test]
fn delete_uses_admitted_context_and_records_noop_as_known_empty() {
    let engine = FakeDeleteEngine::new(WriteBehavior::NoOp);
    let coordination = common::coordination_fixture::open_blocking("delete-service-test");
    let journal = Arc::clone(&coordination.journal);
    let service = DmlService::compose_with_coordination(
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
        Arc::new(FrontendStatisticsService::new()),
        Arc::clone(&coordination.coordination),
        coordination.handle(),
    );
    let (context, _, deadline) = context();
    assert_eq!(
        service
            .try_execute_delete(&engine, "DELETE FROM orders WHERE id = 1", &context, None)
            .unwrap(),
        Some(()),
    );
    assert_eq!(
        engine.prepare_calls.lock().unwrap().as_slice(),
        &[(DeleteStatementKind::Predicate, 91, Some(deadline))],
    );
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert_eq!(*engine.commit_calls.lock().unwrap(), 0);
    assert_eq!(*engine.finalize_calls.lock().unwrap(), 0);
}

#[test]
fn equality_delete_commits_and_finalizes_row_delta() {
    let engine = FakeDeleteEngine::new(WriteBehavior::CommitRequired);
    let coordination = common::coordination_fixture::open_blocking("delete-service-test");
    let journal = Arc::clone(&coordination.journal);
    let service = DmlService::compose_with_coordination(
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
        Arc::new(FrontendStatisticsService::new()),
        Arc::clone(&coordination.coordination),
        coordination.handle(),
    );
    let (context, _, _) = context();
    service
        .try_execute_delete(
            &engine,
            "ALTER TABLE orders ADD EQUALITY DELETE (id) VALUES (2)",
            &context,
            None,
        )
        .unwrap();
    assert_eq!(
        engine.prepare_calls.lock().unwrap()[0].0,
        DeleteStatementKind::Equality,
    );
    assert_eq!(journal.states(), vec![OperationState::Finalized]);
    assert_eq!(*engine.commit_calls.lock().unwrap(), 1);
    assert_eq!(*engine.finalize_calls.lock().unwrap(), 1);
    assert_eq!(
        service.list_operations().unwrap()[0].operation_kind,
        OperationKind::RowDelta,
    );
}

#[test]
fn aborted_delete_does_not_commit() {
    let engine = FakeDeleteEngine::new(WriteBehavior::Aborted);
    let coordination = common::coordination_fixture::open_blocking("delete-service-test");
    let journal = Arc::clone(&coordination.journal);
    let service = DmlService::compose_with_coordination(
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
        Arc::new(FrontendStatisticsService::new()),
        Arc::clone(&coordination.coordination),
        coordination.handle(),
    );
    let (context, _, _) = context();
    let error = service
        .try_execute_delete(&engine, "DELETE FROM orders WHERE id = 1", &context, None)
        .unwrap_err();
    assert!(error.to_string().contains("writer aborted"));
    assert_eq!(
        journal.states(),
        vec![OperationState::FailedKnownUncommitted]
    );
    assert_eq!(*engine.commit_calls.lock().unwrap(), 0);
}
