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

use bytes::Bytes;
use novarocks::common::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks::common::backend_topology::BackendTopologySnapshot;
use novarocks::common::query_cancellation::QueryCancellationSource;
use novarocks_frontend::FrontendStatisticsService;
use novarocks_frontend::dml::model::{
    DML_EXTERNAL_FACT_ENCODED_LIMIT, DML_OPERATION_SCHEMA_VERSION,
    DmlDirectMutationFenceMutationRequest, DmlDirectMutationFenceReceiptRecord,
    DmlDirectMutationKind, validate_direct_mutation_fence_receipt, validate_operation_transition,
};
use novarocks_frontend::dml::truncate::{
    decode_truncate_evidence_hex, encode_truncate_evidence_hex,
};
use novarocks_frontend::dml::{
    CreatePreparingRequest, CreateStatementOperationRequest, DmlError, DmlErrorKind,
    DmlOperationId, DmlService, OperationFact, OperationJournal, OperationMutationRequest,
    OperationPayload, OperationState, StatementNextAction, StoredOperation, TruncateLifecyclePhase,
};
use novarocks_frontend::query_execution::dml::truncate::{
    PlanTruncateRequest, PreparedTruncate, TruncateCommand, TruncateDispatchState, TruncateEffect,
    TruncateEngine, TruncateEvidence, TruncateFailure, TruncateFailureKind, TruncateFinalization,
    TruncateOutcome, TruncatePlanError, TruncatePlanFacts, TruncatePlanSummary, TruncatePrepared,
    TruncateReceipt, parse_truncate_command,
};
use novarocks_spi::connector::{
    ConnectorDataMutationPlanSummary, ConnectorDataMutationReceipt, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMutationOperationId,
    ConnectorProviderId, ExternalMutationEvidence,
};
use novarocks_types::ClusterRole;
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod common;

type PlanContextCapture = (u64, Option<Instant>, [u8; 16]);

#[derive(Clone)]
enum Behavior {
    Committed {
        finalization_failure: Option<TruncateFailure>,
    },
    KnownUncommitted(TruncateFailure),
    CommitUnknown(TruncateFailure),
    CommitUnknownWithPayload(TruncateFailure, Vec<u8>),
    PossiblyDispatched(TruncateFailure),
    CommittedWithCorruptReceipt(ReceiptCorruption),
}

#[derive(Clone, Copy)]
enum ReceiptCorruption {
    Identity,
    OperationKind,
    PayloadDigest,
}

struct FakePrepared {
    facts: TruncatePlanFacts,
}

impl TruncatePrepared for FakePrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct FakeTruncateEngine {
    plan_error: Mutex<Option<TruncatePlanError>>,
    execute_behavior: Behavior,
    reconcile_behavior: Behavior,
    classify_calls: AtomicUsize,
    plan_calls: AtomicUsize,
    execute_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
    plan_context: Mutex<Vec<PlanContextCapture>>,
    reconcile_evidence: Mutex<Vec<TruncateEvidence>>,
    unknown_is_durable: Arc<AtomicBool>,
}

impl FakeTruncateEngine {
    fn new(execute_behavior: Behavior, reconcile_behavior: Behavior) -> Self {
        Self {
            plan_error: Mutex::new(None),
            execute_behavior,
            reconcile_behavior,
            classify_calls: AtomicUsize::new(0),
            plan_calls: AtomicUsize::new(0),
            execute_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            plan_context: Mutex::new(Vec::new()),
            reconcile_evidence: Mutex::new(Vec::new()),
            unknown_is_durable: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_plan_error(error: TruncatePlanError) -> Self {
        let engine = Self::new(
            Behavior::KnownUncommitted(failure(TruncateFailureKind::Internal, "unused")),
            Behavior::KnownUncommitted(failure(TruncateFailureKind::Internal, "unused")),
        );
        *engine.plan_error.lock().unwrap() = Some(error);
        engine
    }

    fn counts(&self) -> (usize, usize, usize, usize) {
        (
            self.classify_calls.load(Ordering::SeqCst),
            self.plan_calls.load(Ordering::SeqCst),
            self.execute_calls.load(Ordering::SeqCst),
            self.reconcile_calls.load(Ordering::SeqCst),
        )
    }
}

impl TruncateEngine for FakeTruncateEngine {
    /// Acknowledge the sealed fence the way a provider does: with a receipt
    /// that names exactly this fence. The establish-before-dispatch ordering
    /// and the journalled receipt are what these tests exercise, not the
    /// provider's marker publication itself.
    fn establish_truncate_external_fence(
        &self,
        _prepared: &dyn novarocks_frontend::query_execution::dml::truncate::TruncatePrepared,
        fence: novarocks_spi::connector::ConnectorExternalOperationFence,
    ) -> Result<
        novarocks_spi::connector::ConnectorExternalFenceReceipt,
        novarocks_spi::connector::ConnectorError,
    > {
        novarocks_spi::connector::ConnectorExternalFenceReceipt::try_new(
            &fence,
            Bytes::from_static(b"truncate-fence-marker"),
        )
    }

    fn classify_truncate(&self, sql: &str) -> Result<Option<TruncateCommand>, String> {
        self.classify_calls.fetch_add(1, Ordering::SeqCst);
        parse_truncate_command(sql)
    }

    fn plan_truncate(
        &self,
        request: PlanTruncateRequest,
    ) -> Result<PreparedTruncate, TruncatePlanError> {
        self.plan_calls.fetch_add(1, Ordering::SeqCst);
        self.plan_context.lock().unwrap().push((
            request.execution.topology().revision(),
            request.execution.deadline(),
            request.mutation_operation_id,
        ));
        if let Some(error) = self.plan_error.lock().unwrap().take() {
            return Err(error);
        }
        let parts = &request.command.target_parts;
        let (catalog, namespace, table) = match parts.as_slice() {
            [catalog, namespace, table] => (catalog.clone(), namespace.clone(), table.clone()),
            [namespace, table] => (
                request.current_catalog.unwrap_or_default(),
                namespace.clone(),
                table.clone(),
            ),
            [table] => (
                request.current_catalog.unwrap_or_default(),
                request.current_database,
                table.clone(),
            ),
            _ => panic!("test command target"),
        };
        let facts = TruncatePlanFacts {
            catalog,
            namespace,
            table,
            target_ref: request.command.target_ref,
            provider_id: "iceberg".to_string(),
            instance_id: "ice".to_string(),
            incarnation: [0x11; 16],
            mutation_operation_id: request.mutation_operation_id,
            request_digest: [0x22; 32],
            plan_digest: [0x33; 32],
            state_digest: [0x44; 32],
            summary: TruncatePlanSummary {
                file_count: 3,
                row_count: 7,
                total_bytes: 101,
            },
        };
        Ok(PreparedTruncate {
            handle: Arc::new(FakePrepared {
                facts: facts.clone(),
            }),
            facts,
        })
    }

    fn execute_truncate(&self, prepared: &dyn TruncatePrepared) -> TruncateOutcome {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        let prepared = prepared
            .as_any()
            .downcast_ref::<FakePrepared>()
            .expect("fake prepared");
        project_behavior(&self.execute_behavior, &prepared.facts)
    }

    fn reconcile_truncate(
        &self,
        prepared: &dyn TruncatePrepared,
        evidence: &TruncateEvidence,
    ) -> TruncateOutcome {
        assert!(
            self.unknown_is_durable.load(Ordering::SeqCst),
            "reconcile must not start before unknown evidence is durable"
        );
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        self.reconcile_evidence
            .lock()
            .unwrap()
            .push(evidence.clone());
        let prepared = prepared
            .as_any()
            .downcast_ref::<FakePrepared>()
            .expect("fake prepared");
        project_behavior(&self.reconcile_behavior, &prepared.facts)
    }
}

fn project_behavior(behavior: &Behavior, facts: &TruncatePlanFacts) -> TruncateOutcome {
    match behavior {
        Behavior::Committed {
            finalization_failure,
        } => TruncateOutcome::KnownCommitted {
            effect: TruncateEffect::Applied,
            receipt: matching_receipt(facts),
            finalization: finalization_failure
                .clone()
                .map(TruncateFinalization::Failed)
                .unwrap_or(TruncateFinalization::Complete),
        },
        Behavior::KnownUncommitted(failure) => TruncateOutcome::KnownUncommitted {
            failure: failure.clone(),
        },
        Behavior::CommitUnknown(failure) => TruncateOutcome::CommitUnknown {
            failure: failure.clone(),
            evidence: evidence_for_facts(facts),
        },
        Behavior::CommitUnknownWithPayload(failure, payload) => TruncateOutcome::CommitUnknown {
            failure: failure.clone(),
            evidence: external_evidence(
                &facts.provider_id,
                &facts.instance_id,
                facts.incarnation,
                facts.mutation_operation_id,
                payload.clone(),
            ),
        },
        Behavior::PossiblyDispatched(failure) => TruncateOutcome::ContractFailure {
            failure: failure.clone(),
            dispatch: TruncateDispatchState::PossiblyDispatched,
        },
        Behavior::CommittedWithCorruptReceipt(corruption) => {
            let mut receipt = matching_receipt(facts);
            match corruption {
                ReceiptCorruption::Identity => receipt.instance_id = "other".to_string(),
                ReceiptCorruption::OperationKind => {
                    receipt.operation_kind = "register-existing-files".to_string()
                }
                ReceiptCorruption::PayloadDigest => receipt.opaque_payload_digest = [0xff; 32],
            }
            TruncateOutcome::KnownCommitted {
                effect: TruncateEffect::Applied,
                receipt,
                finalization: TruncateFinalization::Complete,
            }
        }
    }
}

fn matching_receipt(facts: &TruncatePlanFacts) -> TruncateReceipt {
    let opaque_payload = br#"{"snapshot_id":42}"#.to_vec();
    let spi_receipt = ConnectorDataMutationReceipt::try_new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(&facts.provider_id).unwrap(),
            instance_id: ConnectorInstanceId::parse(&facts.instance_id).unwrap(),
        },
        ConnectorInstanceIncarnation::from_bytes(facts.incarnation),
        ConnectorMutationOperationId::from_bytes(facts.mutation_operation_id),
        "truncate",
        facts.request_digest,
        facts.plan_digest,
        facts.state_digest,
        ConnectorDataMutationPlanSummary::try_new(
            facts.summary.file_count,
            facts.summary.row_count,
            facts.summary.total_bytes,
        )
        .unwrap(),
        Bytes::from(opaque_payload.clone()),
    )
    .unwrap();
    TruncateReceipt {
        provider_id: facts.provider_id.clone(),
        instance_id: facts.instance_id.clone(),
        incarnation: facts.incarnation,
        mutation_operation_id: facts.mutation_operation_id,
        operation_kind: "truncate".to_string(),
        request_digest: facts.request_digest,
        plan_digest: facts.plan_digest,
        state_digest: facts.state_digest,
        summary: facts.summary,
        opaque_payload,
        opaque_payload_digest: spi_receipt.provider_payload_digest(),
    }
}

fn evidence_for_facts(facts: &TruncatePlanFacts) -> TruncateEvidence {
    external_evidence(
        &facts.provider_id,
        &facts.instance_id,
        facts.incarnation,
        facts.mutation_operation_id,
        b"opaque-evidence".to_vec(),
    )
}

fn evidence_with_payload(payload: Vec<u8>) -> TruncateEvidence {
    external_evidence("iceberg", "ice", [0x11; 16], [0x77; 16], payload)
}

fn external_evidence(
    provider_id: &str,
    instance_id: &str,
    incarnation: [u8; 16],
    operation_id: [u8; 16],
    payload: Vec<u8>,
) -> TruncateEvidence {
    let evidence = ExternalMutationEvidence::try_new(
        1,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(provider_id).unwrap(),
            instance_id: ConnectorInstanceId::parse(instance_id).unwrap(),
        },
        ConnectorInstanceIncarnation::from_bytes(incarnation),
        ConnectorMutationOperationId::from_bytes(operation_id),
        "truncate",
        Bytes::from(payload),
    )
    .unwrap();
    TruncateEvidence {
        schema_version: evidence.schema_version(),
        digest: evidence.digest(),
        wire_bytes: evidence.try_to_wire_v1().unwrap().to_vec(),
    }
}

fn evidence_with_wire_len(target_len: usize) -> TruncateEvidence {
    let base_len = evidence_with_payload(Vec::new()).wire_bytes.len();
    assert!(target_len >= base_len);
    let evidence = evidence_with_payload(vec![0xab; target_len - base_len]);
    assert_eq!(evidence.wire_bytes.len(), target_len);
    evidence
}

fn failure(kind: TruncateFailureKind, message: &str) -> TruncateFailure {
    TruncateFailure {
        kind,
        message: message.to_string(),
    }
}

struct FakeJournal {
    operations: Mutex<BTreeMap<Uuid, StoredOperation>>,
    history: Mutex<Vec<StoredOperation>>,
    create_calls: AtomicUsize,
    mutation_calls: AtomicUsize,
    unknown_is_durable: Arc<AtomicBool>,
    fail_mutation_at: Mutex<Option<usize>>,
    injected_error: Mutex<Option<DmlError>>,
    max_statement_bytes: AtomicUsize,
    direct_mutation_fences: Mutex<Vec<DmlDirectMutationFenceReceiptRecord>>,
}

impl Default for FakeJournal {
    fn default() -> Self {
        Self {
            operations: Mutex::new(BTreeMap::new()),
            history: Mutex::new(Vec::new()),
            create_calls: AtomicUsize::new(0),
            mutation_calls: AtomicUsize::new(0),
            unknown_is_durable: Arc::new(AtomicBool::new(false)),
            fail_mutation_at: Mutex::new(None),
            injected_error: Mutex::new(None),
            max_statement_bytes: AtomicUsize::new(usize::MAX),
            direct_mutation_fences: Mutex::new(Vec::new()),
        }
    }
}

impl FakeJournal {
    fn only_operation(&self) -> StoredOperation {
        let operations = self.operations.lock().unwrap();
        assert_eq!(operations.len(), 1);
        operations.values().next().unwrap().clone()
    }

    fn direct_mutation_fences(&self) -> Vec<DmlDirectMutationFenceReceiptRecord> {
        self.direct_mutation_fences.lock().unwrap().clone()
    }

    fn history(&self) -> Vec<StoredOperation> {
        self.history.lock().unwrap().clone()
    }

    fn fail_mutation_at(&self, call: usize) {
        let error = DmlService::compose(None, Arc::new(FrontendStatisticsService::new()))
            .list_operations()
            .unwrap_err();
        *self.fail_mutation_at.lock().unwrap() = Some(call);
        *self.injected_error.lock().unwrap() = Some(error);
    }

    fn set_max_statement_bytes(&self, max_statement_bytes: usize) {
        self.max_statement_bytes
            .store(max_statement_bytes, Ordering::SeqCst);
    }

    fn journal_limit_error() -> DmlError {
        DmlService::compose(None, Arc::new(FrontendStatisticsService::new()))
            .list_operations()
            .unwrap_err()
    }
}

impl OperationJournal for FakeJournal {
    fn create_preparing(
        &self,
        _request: CreatePreparingRequest,
    ) -> Result<DmlOperationId, DmlError> {
        panic!("TRUNCATE must not use the write journal API")
    }

    fn transition(
        &self,
        _operation_id: DmlOperationId,
        _to: OperationState,
    ) -> Result<(), DmlError> {
        panic!("TRUNCATE must not use the write journal API")
    }

    fn record_fact(
        &self,
        _operation_id: DmlOperationId,
        _fact: OperationFact,
    ) -> Result<(), DmlError> {
        panic!("TRUNCATE must not use the write journal API")
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

    /// The coordinated path admits the intent inside the journal transaction.
    /// This fake has no transaction to admit inside, so it cannot run the
    /// validator; admission itself is covered by the StateStore journal tests.
    /// Here it only needs to let a coordinated operation be created, so these
    /// tests exercise TRUNCATE routing under a real fence.
    /// Claiming is an ownership transition this fake does not model; the real
    /// claim semantics are covered by the StateStore journal tests. Returning
    /// the stored operation lets the coordinated route proceed so these tests
    /// can exercise TRUNCATE under a real fence.
    fn claim_operation_admitted(
        &self,
        request: novarocks_frontend::dml::model::DmlCoordinationClaimRequest,
        _admission: Arc<dyn novarocks_frontend::dml::journal::DmlIntentAdmissionValidator>,
        _authority: novarocks_frontend::dml::DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        Ok(self
            .load(request.operation_id)?
            .expect("claimed DML operation must exist in this fake journal"))
    }

    fn create_statement_operation_admitted(
        &self,
        request: CreateStatementOperationRequest,
        _admission: Arc<dyn novarocks_frontend::dml::journal::DmlIntentAdmissionValidator>,
    ) -> Result<StoredOperation, DmlError> {
        self.create_statement_operation(request)
    }

    fn create_statement_operation(
        &self,
        request: CreateStatementOperationRequest,
    ) -> Result<StoredOperation, DmlError> {
        self.create_calls.fetch_add(1, Ordering::SeqCst);
        let stored = StoredOperation {
            schema_version: DML_OPERATION_SCHEMA_VERSION,
            operation_id: request.operation_id,
            revision: 1,
            last_mutation_id: request.mutation_id,
            operation_kind: request.operation_kind,
            operation_subkind: None,
            target: request.target,
            state: OperationState::Preparing,
            attempt_id: request.attempt_id,
            base_snapshot_id: None,
            base_snapshot_map: BTreeMap::new(),
            staged_artifacts: Vec::new(),
            payload: request.payload,
            coordination_provenance: None,
            recovery_due_at_ms: None,
            created_at_ms: request.created_at_ms,
            updated_at_ms: request.created_at_ms,
            finished_at_ms: None,
        };
        self.operations
            .lock()
            .unwrap()
            .insert(*stored.operation_id.as_uuid(), stored.clone());
        self.history.lock().unwrap().push(stored.clone());
        Ok(stored)
    }

    /// The coordinated path validates the fence inside the journal
    /// transaction. This fake has no transaction, so it delegates to the plain
    /// mutation; fence validation is covered by the StateStore journal tests.
    fn mutate_statement_operation_authorized(
        &self,
        request: OperationMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: novarocks_frontend::dml::DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.mutate_statement_operation(request)
    }

    fn mutate_statement_operation(
        &self,
        request: OperationMutationRequest,
    ) -> Result<StoredOperation, DmlError> {
        let call = self.mutation_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if *self.fail_mutation_at.lock().unwrap() == Some(call) {
            return Err(self.injected_error.lock().unwrap().take().unwrap());
        }
        let mut operations = self.operations.lock().unwrap();
        let operation = operations
            .get_mut(request.operation_id.as_uuid())
            .expect("fake operation");
        assert_eq!(operation.revision, request.expected_revision);
        validate_operation_transition(operation.state, request.state).unwrap();
        operation.revision += 1;
        operation.last_mutation_id = request.mutation_id;
        operation.state = request.state;
        operation.payload = request.payload;
        operation.updated_at_ms += 1;
        if operation.state.is_finished() {
            operation.finished_at_ms = Some(operation.updated_at_ms);
        }
        let stored = operation.clone();
        if matches!(stored.state, OperationState::CommitUnknown)
            && matches!(
                &stored.payload,
                OperationPayload::TruncateLifecycle(record)
                    if record.outcome.as_ref().and_then(|fact| fact.evidence.as_ref()).is_some()
            )
        {
            self.unknown_is_durable.store(true, Ordering::SeqCst);
        }
        drop(operations);
        self.history.lock().unwrap().push(stored.clone());
        Ok(stored)
    }

    fn preflight_statement_operation(&self, operation: &StoredOperation) -> Result<(), DmlError> {
        let encoded = serde_json::to_vec(operation).unwrap();
        if encoded.len() > self.max_statement_bytes.load(Ordering::SeqCst) {
            Err(Self::journal_limit_error())
        } else {
            Ok(())
        }
    }

    fn preflight_direct_mutation_fence(
        &self,
        request: &DmlDirectMutationFenceMutationRequest,
    ) -> Result<(), DmlError> {
        validate_direct_mutation_fence_receipt(&request.fence)
            .map_err(|_| Self::journal_limit_error())
    }

    /// The coordinated path validates the live lease fence inside the same
    /// transaction that writes the receipt. This fake has no transaction, so it
    /// records the receipt and advances the revision the way the real journal
    /// does; the transactional guarantees are covered by the StateStore journal
    /// tests.
    fn record_direct_mutation_fence_authorized(
        &self,
        request: DmlDirectMutationFenceMutationRequest,
        _recovery_due_at_ms: Option<i64>,
        _authority: novarocks_frontend::dml::DmlMutationAuthority,
    ) -> Result<StoredOperation, DmlError> {
        self.preflight_direct_mutation_fence(&request)?;
        let mut operations = self.operations.lock().unwrap();
        let operation = operations
            .get_mut(request.operation_id.as_uuid())
            .expect("fenced fake operation");
        assert_eq!(operation.revision, request.expected_revision);
        operation.revision += 1;
        operation.last_mutation_id = request.mutation_id;
        let stored = operation.clone();
        drop(operations);
        self.direct_mutation_fences
            .lock()
            .unwrap()
            .push(request.fence);
        Ok(stored)
    }
}

/// A service wired to real coordination, holding its runtime alive.
///
/// Dispatch is fenced now, and a fence can only be minted from a live
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

fn harness(engine: &mut FakeTruncateEngine) -> (TestService, Arc<FakeJournal>) {
    let journal = Arc::new(FakeJournal::default());
    engine.unknown_is_durable = Arc::clone(&journal.unknown_is_durable);
    let coordination = common::coordination_fixture::open_blocking("truncate-service-test");
    let dml = DmlService::compose_with_coordination(
        Some(Arc::clone(&journal) as Arc<dyn OperationJournal>),
        Arc::new(FrontendStatisticsService::new()),
        Arc::clone(&coordination.coordination),
        coordination.handle(),
    );
    (
        TestService {
            dml,
            _coordination: coordination,
        },
        journal,
    )
}

fn admitted_context() -> (RequestContext, QueryCancellationSource, Instant) {
    let cancellation = QueryCancellationSource::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    (
        RequestContext::admit(RequestAdmission::new(
            Some("ice".to_string()),
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(83),
            Some(deadline),
            cancellation.view(),
            Default::default(),
        )),
        cancellation,
        deadline,
    )
}

fn truncate_record(
    operation: &StoredOperation,
) -> &novarocks_frontend::dml::TruncateLifecycleRecord {
    match &operation.payload {
        OperationPayload::TruncateLifecycle(record) => record,
        other => panic!("expected TRUNCATE payload, got {other:?}"),
    }
}

fn assert_bounded_failure_projection(encoded: &str, original: &str) {
    let projection: serde_json::Value =
        serde_json::from_str(encoded).expect("bounded failure must be valid JSON");
    let prefix = projection["message_prefix"]
        .as_str()
        .expect("bounded failure must preserve a message prefix");
    let expected_digest = hex::encode(Sha256::digest(original.as_bytes()));

    assert_eq!(projection["version"], 1);
    assert_eq!(projection["kind"], "UNAVAILABLE");
    assert_eq!(projection["message_truncated"], true);
    assert_eq!(
        projection["original_message_bytes"].as_u64(),
        Some(original.len() as u64)
    );
    assert_eq!(
        projection["original_message_sha256"].as_str(),
        Some(expected_digest.as_str())
    );
    assert!(prefix.len() <= 2 * 1024);
    assert!(original.is_char_boundary(prefix.len()));
    assert!(original.starts_with(prefix));
}

#[test]
fn non_truncate_has_no_plan_execute_reconcile_or_journal_calls() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: None,
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    assert_eq!(
        service
            .try_execute_truncate(&engine, "SELECT 1", &context, None)
            .unwrap(),
        None
    );
    assert_eq!(engine.counts(), (1, 0, 0, 0));
    assert_eq!(journal.create_calls.load(Ordering::SeqCst), 0);
    assert_eq!(journal.mutation_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn truncate_requires_journal_before_plan_and_reports_stable_operation_id() {
    let engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: None,
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let service = DmlService::compose(None, Arc::new(FrontendStatisticsService::new()));
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.operation_id().is_some());
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 0, 0, 0));
}

#[test]
fn plan_failure_is_terminal_known_uncommitted_without_execute() {
    let mut engine = FakeTruncateEngine::with_plan_error(TruncatePlanError::KnownUncommitted(
        failure(TruncateFailureKind::NotFound, "missing branch"),
    ));
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(
            &engine,
            "TRUNCATE TABLE ice.db.orders.branch_missing",
            &context,
            None,
        )
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::Executor);
    assert!(error.operation_id().is_some());
    assert_eq!(error.next_action(), Some(StatementNextAction::None));
    assert_eq!(engine.counts(), (1, 1, 0, 0));
    let operation = journal.only_operation();
    assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
    assert_eq!(
        truncate_record(&operation).phase,
        TruncateLifecyclePhase::Failed
    );
}

#[test]
fn execute_known_uncommitted_is_terminal_and_never_reconciles() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::KnownUncommitted(failure(
            TruncateFailureKind::Conflict,
            "target ref advanced concurrently",
        )),
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::Executor);
    assert_eq!(error.next_action(), Some(StatementNextAction::None));
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    let operation = journal.only_operation();
    assert_eq!(operation.state, OperationState::FailedKnownUncommitted);
    assert_eq!(
        truncate_record(&operation)
            .outcome
            .as_ref()
            .unwrap()
            .outcome,
        novarocks_frontend::dml::ExternalFactOutcome::Conflict
    );
}

#[test]
fn committed_truncate_persists_exact_plan_and_versioned_receipt_then_finishes() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: None,
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, deadline) = admitted_context();
    assert_eq!(
        service
            .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
            .unwrap(),
        Some(())
    );
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    let plan_context = engine.plan_context.lock().unwrap();
    assert_eq!(plan_context[0].0, 83);
    assert_eq!(plan_context[0].1, Some(deadline));
    let operation = journal.only_operation();
    assert_eq!(operation.state, OperationState::Finalized);
    let record = truncate_record(&operation);
    assert_eq!(record.provider_id.as_deref(), Some("iceberg"));
    assert_eq!(record.connector_instance_id.as_deref(), Some("ice"));
    assert_eq!(record.connector_incarnation, Some("11".repeat(16)));
    assert_eq!(record.request_digest, Some("22".repeat(32)));
    assert_eq!(record.plan_digest, Some("33".repeat(32)));
    assert_eq!(record.state_digest, Some("44".repeat(32)));
    assert_eq!(
        record.connector_operation_id.into_bytes(),
        plan_context[0].2
    );
    let fact = record.outcome.as_ref().unwrap();
    let receipt: serde_json::Value =
        serde_json::from_str(fact.receipt.as_deref().unwrap()).unwrap();
    assert_eq!(receipt["version"], 1);
    assert_eq!(receipt["effect"], "APPLIED");
    assert_eq!(
        receipt["opaque_payload"],
        hex::encode(br#"{"snapshot_id":42}"#)
    );
    assert!(fact.receipt.as_ref().unwrap().len() <= DML_EXTERNAL_FACT_ENCODED_LIMIT);

    // The fence the provider acknowledged must be durable before the
    // destructive execute, and it must bind this exact statement: a later owner
    // recovers the historical fence from this record alone. TRUNCATE owns no
    // source set, so it must not bind a source scope.
    let fences = journal.direct_mutation_fences();
    assert_eq!(fences.len(), 1, "one fence receipt per TRUNCATE attempt");
    assert_eq!(fences[0].operation_kind, DmlDirectMutationKind::Truncate);
    assert_eq!(fences[0].source_scope_digest, None);
    assert_eq!(
        fences[0].mutation_operation_id().into_bytes(),
        plan_context[0].2
    );
}

#[test]
fn committed_finalization_failure_stays_known_committed_and_retry_finalize() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: Some(failure(
                TruncateFailureKind::Unavailable,
                "catalog cache refresh failed",
            )),
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::RetryFinalize)
    );
    let operation = journal.only_operation();
    assert_eq!(
        operation.state,
        OperationState::FinalizeFailedKnownCommitted
    );
    let fact = truncate_record(&operation).outcome.as_ref().unwrap();
    assert!(fact.receipt.is_some());
    assert!(fact.finalization_failure.is_some());
    assert_eq!(engine.counts(), (1, 1, 1, 0));
}

#[test]
fn unknown_evidence_is_durable_before_one_reconcile_and_matching_marker_converges() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "response lost")),
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap();
    assert_eq!(engine.counts(), (1, 1, 1, 1));
    let reconciled_evidence = engine.reconcile_evidence.lock().unwrap();
    assert_eq!(reconciled_evidence.len(), 1);
    let history = journal.history();
    let unknown = history
        .iter()
        .find(|operation| {
            operation.state == OperationState::CommitUnknown
                && truncate_record(operation)
                    .outcome
                    .as_ref()
                    .and_then(|fact| fact.evidence.as_ref())
                    .is_some()
        })
        .expect("durable unknown evidence");
    let encoded = truncate_record(unknown)
        .outcome
        .as_ref()
        .unwrap()
        .evidence
        .as_ref()
        .unwrap();
    assert_eq!(
        decode_truncate_evidence_hex(encoded).unwrap(),
        reconciled_evidence[0].wire_bytes
    );
    let operation = journal.only_operation();
    assert_eq!(operation.state, OperationState::Finalized);
    assert_eq!(
        truncate_record(&operation)
            .outcome
            .as_ref()
            .and_then(|fact| fact.evidence.as_ref()),
        Some(encoded)
    );
}

#[test]
fn missing_or_conflicting_marker_remains_unresolved_without_reexecute() {
    for kind in [TruncateFailureKind::NotFound, TruncateFailureKind::Conflict] {
        let mut engine = FakeTruncateEngine::new(
            Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "response lost")),
            Behavior::CommitUnknown(failure(kind, "marker did not establish outcome")),
        );
        let (service, journal) = harness(&mut engine);
        let (context, _, _) = admitted_context();
        let error = service
            .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
            .unwrap_err();
        assert_eq!(error.kind(), DmlErrorKind::Commit);
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 1));
        let operation = journal.only_operation();
        assert_eq!(operation.state, OperationState::CommitUnknown);
        assert_eq!(
            truncate_record(&operation).next_action,
            StatementNextAction::ManualInspect
        );
    }
}

#[test]
fn possibly_dispatched_without_evidence_stays_unresolved_and_never_reconciles() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::PossiblyDispatched(failure(
            TruncateFailureKind::Internal,
            "provider violated outcome contract",
        )),
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    assert_eq!(
        journal.only_operation().state,
        OperationState::CommitUnknown
    );
}

#[test]
fn reconcile_possibly_dispatched_preserves_first_durable_evidence() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "response lost")),
        Behavior::PossiblyDispatched(failure(
            TruncateFailureKind::Internal,
            "reconcile transport contract failed",
        )),
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 1, 1, 1));
    let evidence = truncate_record(&journal.only_operation())
        .outcome
        .as_ref()
        .and_then(|fact| fact.evidence.clone());
    assert!(
        evidence.is_some(),
        "first durable evidence must not be erased"
    );
}

#[test]
fn reconcile_different_evidence_is_corrupt_and_cannot_replace_first_evidence() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "response lost")),
        Behavior::CommitUnknownWithPayload(
            failure(TruncateFailureKind::Conflict, "different marker"),
            b"different-evidence".to_vec(),
        ),
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::Commit);
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 1, 1, 1));
    let history = journal.history();
    let first = history
        .iter()
        .find_map(|operation| {
            truncate_record(operation)
                .outcome
                .as_ref()
                .and_then(|fact| fact.evidence.clone())
        })
        .unwrap();
    let final_evidence = truncate_record(&journal.only_operation())
        .outcome
        .as_ref()
        .and_then(|fact| fact.evidence.clone())
        .unwrap();
    assert_eq!(final_evidence, first);
}

#[test]
fn oversized_unknown_failure_cannot_block_evidence_durability_or_trigger_reconcile() {
    let huge_message = "未知失败".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT);
    let mut engine = FakeTruncateEngine::new(
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, &huge_message)),
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "unused")),
    );
    let (service, journal) = harness(&mut engine);
    journal.fail_mutation_at(4);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    let operation = journal.only_operation();
    assert_eq!(operation.state, OperationState::CommitUnknown);
    let fact = truncate_record(&operation).outcome.as_ref().unwrap();
    assert!(fact.evidence.is_some());
    let failure = fact.failure.as_deref().unwrap();
    assert!(failure.len() <= DML_EXTERNAL_FACT_ENCODED_LIMIT);
    assert_bounded_failure_projection(failure, &huge_message);
}

#[test]
fn oversized_finalization_failure_keeps_committed_truth_durable() {
    let huge_message = "finalization failed ".repeat(DML_EXTERNAL_FACT_ENCODED_LIMIT);
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: Some(failure(TruncateFailureKind::Unavailable, &huge_message)),
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    let operation = journal.only_operation();
    assert_eq!(
        operation.state,
        OperationState::FinalizeFailedKnownCommitted
    );
    let fact = truncate_record(&operation).outcome.as_ref().unwrap();
    assert!(fact.receipt.is_some());
    let finalization_failure = fact.finalization_failure.as_deref().unwrap();
    assert!(finalization_failure.len() <= DML_EXTERNAL_FACT_ENCODED_LIMIT);
    assert_bounded_failure_projection(finalization_failure, &huge_message);
}

#[test]
fn worst_case_journal_envelope_is_preflighted_before_execute() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: None,
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    journal.set_max_statement_bytes(4 * 1024);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::Executor);
    assert_eq!(error.next_action(), Some(StatementNextAction::None));
    assert_eq!(engine.counts(), (1, 1, 0, 0));
    assert_eq!(
        journal.only_operation().state,
        OperationState::FailedKnownUncommitted
    );
}

#[test]
fn invalid_committed_receipt_keeps_committed_truth_without_trusting_receipt() {
    for corruption in [
        ReceiptCorruption::Identity,
        ReceiptCorruption::OperationKind,
        ReceiptCorruption::PayloadDigest,
    ] {
        let mut engine = FakeTruncateEngine::new(
            Behavior::CommittedWithCorruptReceipt(corruption),
            Behavior::Committed {
                finalization_failure: None,
            },
        );
        let (service, journal) = harness(&mut engine);
        let (context, _, _) = admitted_context();
        let error = service
            .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
            .unwrap_err();
        assert_eq!(error.kind(), DmlErrorKind::CommittedButUnfinalized);
        assert_eq!(
            error.next_action(),
            Some(StatementNextAction::ManualInspect)
        );
        assert_eq!(engine.counts(), (1, 1, 1, 0));

        let operation = journal.only_operation();
        assert_eq!(
            operation.state,
            OperationState::FinalizeFailedKnownCommitted
        );
        let record = truncate_record(&operation);
        assert_eq!(record.next_action, StatementNextAction::ManualInspect);
        let fact = record.outcome.as_ref().unwrap();
        assert_eq!(
            fact.outcome,
            novarocks_frontend::dml::ExternalFactOutcome::KnownCommitted
        );
        assert!(fact.receipt.is_none());
        assert!(fact.failure.is_some());
    }
}

#[test]
fn journal_uncertainty_blocks_execute_and_reconcile_at_each_external_barrier() {
    let mut engine = FakeTruncateEngine::new(
        Behavior::Committed {
            finalization_failure: None,
        },
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    journal.fail_mutation_at(1);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.operation_id().is_some());
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 1, 0, 0));

    let mut engine = FakeTruncateEngine::new(
        Behavior::CommitUnknown(failure(TruncateFailureKind::Unavailable, "response lost")),
        Behavior::Committed {
            finalization_failure: None,
        },
    );
    let (service, journal) = harness(&mut engine);
    journal.fail_mutation_at(3);
    let (context, _, _) = admitted_context();
    let error = service
        .try_execute_truncate(&engine, "TRUNCATE TABLE ice.db.orders", &context, None)
        .unwrap_err();
    assert_eq!(error.kind(), DmlErrorKind::JournalUnavailable);
    assert!(error.operation_id().is_some());
    assert_eq!(
        error.next_action(),
        Some(StatementNextAction::ManualInspect)
    );
    assert_eq!(engine.counts(), (1, 1, 1, 0));
    assert!(!journal.unknown_is_durable.load(Ordering::SeqCst));
}

#[test]
fn canonical_evidence_codec_is_lossless_bounded_and_rejects_uppercase() {
    let evidence = evidence_with_wire_len(DML_EXTERNAL_FACT_ENCODED_LIMIT / 2);
    let encoded = encode_truncate_evidence_hex(&evidence).unwrap();
    assert_eq!(encoded.len(), DML_EXTERNAL_FACT_ENCODED_LIMIT);
    assert_eq!(
        decode_truncate_evidence_hex(&encoded).unwrap(),
        evidence.wire_bytes
    );
    assert!(decode_truncate_evidence_hex("AB").is_err());
    assert!(
        encode_truncate_evidence_hex(&evidence_with_wire_len(
            DML_EXTERNAL_FACT_ENCODED_LIMIT / 2 + 1
        ))
        .is_err()
    );
}
