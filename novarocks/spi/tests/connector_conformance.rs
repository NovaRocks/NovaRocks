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

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::conformance::assert_batch_reader_contract;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorControlBinding,
    ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationPlan,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
    ConnectorExecutionDistribution, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMetadata,
    ConnectorNamespaceRequest, ConnectorOpenReaderRequest, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit,
    ConnectorPreparedScanUnitDescriptor, ConnectorPreparedScanUnitSet, ConnectorProviderId,
    ConnectorReadExecution, ConnectorReaderMetricsSnapshot, ConnectorScalarType,
    ConnectorScalarValue, ConnectorScan, ConnectorScanHandle, ConnectorScanPlanning,
    ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsMissingReason, ConnectorSplit,
    ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStaticComparisonOp, ConnectorStaticPredicate, ConnectorStaticPredicateColumn,
    ConnectorStaticPredicateId, ConnectorStaticPredicateKind, ConnectorStatistics,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest,
    ExternalMutationOutcome, StatisticsEvidence, StatisticsReadRequest,
    normalize_predicate_dispositions, validate_static_predicates,
};

struct OwnerExecution {
    key: ConnectorExecutionBindingKey,
}

impl OwnerExecution {
    fn new(instance_id: &str) -> Self {
        Self {
            key: ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
                incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
            },
        }
    }
}

impl ConnectorReadExecution for OwnerExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        ConnectorPreparedScanUnitSet::try_new(
            self.key.clone(),
            split,
            bytes::Bytes::new(),
            vec![ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"owner-test-unit"),
                split.estimated_bytes(),
                missing_facts(),
            )?],
            &request,
        )
    }

    fn open_unit_reader(
        &self,
        _unit: &ConnectorPreparedScanUnit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        unreachable!("instance construction must not open a reader")
    }
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

struct AlwaysCancelled;

impl novarocks_spi::connector::ConnectorCancellation for AlwaysCancelled {
    fn is_cancelled(&self) -> bool {
        true
    }
}

fn preparation_request() -> ConnectorPrepareSplitRequest {
    preparation_request_with(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NeverCancelled),
        1024,
        4096,
    )
}

fn preparation_request_with(
    deadline: Instant,
    cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
) -> ConnectorPrepareSplitRequest {
    ConnectorPrepareSplitRequest {
        context: novarocks_spi::connector::ConnectorRequestContext::try_new(
            deadline,
            cancellation,
            max_handle_payload_bytes,
            max_total_payload_bytes,
        )
        .expect("preparation context"),
    }
}

fn prepared_unit(
    payload: &'static [u8],
    estimated_bytes: Option<u64>,
) -> ConnectorPreparedScanUnitDescriptor {
    ConnectorPreparedScanUnitDescriptor::try_new(
        bytes::Bytes::from_static(payload),
        estimated_bytes,
        missing_facts(),
    )
    .expect("non-empty prepared unit")
}

fn missing_facts() -> ConnectorScanUnitDomainFacts {
    ConnectorScanUnitDomainFacts::missing(ConnectorScanUnitFactsMissingReason::ProviderUnsupported)
}

fn prepared_split(
    execution: &OwnerExecution,
    split_id: &str,
    estimated_bytes: Option<u64>,
) -> ConnectorSplit {
    ConnectorSplit::try_new(
        execution.key.instance_id.clone(),
        split_id,
        bytes::Bytes::from_static(b"opaque-split"),
        estimated_bytes,
    )
    .expect("split")
}

#[test]
fn prepared_unit_set_is_sealed_bounded_and_cost_exact() {
    let execution = OwnerExecution::new("file");
    let split = ConnectorSplit::try_new(
        execution.key.instance_id.clone(),
        "split-a",
        bytes::Bytes::from_static(b"opaque-split"),
        Some(11),
    )
    .expect("split");
    let set = ConnectorPreparedScanUnitSet::try_new(
        execution.key.clone(),
        &split,
        bytes::Bytes::from_static(b"shared"),
        vec![
            ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"first"),
                Some(4),
                missing_facts(),
            )
            .expect("first unit"),
            ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"second"),
                Some(7),
                missing_facts(),
            )
            .expect("second unit"),
        ],
        &preparation_request(),
    )
    .expect("sealed unit set");

    assert_eq!(set.len(), 2);
    assert!(!set.is_empty());
    assert_eq!(
        set.units().map(|unit| unit.ordinal()).collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        set.units()
            .map(|unit| unit.estimated_bytes())
            .collect::<Vec<_>>(),
        [Some(4), Some(7)]
    );
    assert_eq!(set.membership_digest().len(), 32);
    assert_eq!(
        set.units()
            .next()
            .expect("first unit")
            .domain_facts()
            .missing_reason(),
        Some(ConnectorScanUnitFactsMissingReason::ProviderUnsupported)
    );
}

#[test]
fn prepared_unit_facts_are_sealed_but_do_not_redefine_membership_identity() {
    let execution = OwnerExecution::new("file");
    let split = prepared_split(&execution, "split-a", Some(1));
    let exact = ConnectorScanUnitDomainFacts::available(
        1,
        novarocks_spi::connector::ConnectorScanUnitFactsEvidence::Exact,
        vec![
            novarocks_spi::connector::ConnectorScanUnitColumnDomain::try_range(
                novarocks_spi::connector::ConnectorScanUnitColumn::new(
                    0,
                    ConnectorScalarType::Int32,
                    false,
                ),
                ConnectorScalarValue::Int32(7),
                ConnectorScalarValue::Int32(7),
                0,
                1,
            )
            .expect("range"),
        ],
    )
    .expect("available facts");
    let missing = missing_facts();
    let with_exact = ConnectorPreparedScanUnitSet::try_new(
        execution.key.clone(),
        &split,
        bytes::Bytes::new(),
        vec![
            ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"unit"),
                Some(1),
                exact.clone(),
            )
            .expect("exact descriptor"),
        ],
        &preparation_request(),
    )
    .expect("exact set");
    let with_missing = ConnectorPreparedScanUnitSet::try_new(
        execution.key.clone(),
        &split,
        bytes::Bytes::new(),
        vec![
            ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"unit"),
                Some(1),
                missing,
            )
            .expect("missing descriptor"),
        ],
        &preparation_request(),
    )
    .expect("missing set");

    assert_eq!(
        with_exact.membership_digest(),
        with_missing.membership_digest()
    );
    assert_eq!(
        with_exact
            .units()
            .next()
            .expect("exact unit")
            .domain_facts(),
        &exact
    );
}

#[test]
fn prepared_unit_set_rejects_unknown_unit_cost_for_known_split_cost() {
    let execution = OwnerExecution::new("file");
    let split = ConnectorSplit::try_new(
        execution.key.instance_id.clone(),
        "split-a",
        bytes::Bytes::from_static(b"opaque-split"),
        Some(1),
    )
    .expect("split");
    let error = ConnectorPreparedScanUnitSet::try_new(
        execution.key,
        &split,
        bytes::Bytes::new(),
        vec![
            ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"unit"),
                None,
                missing_facts(),
            )
            .expect("unit"),
        ],
        &preparation_request(),
    )
    .expect_err("known split cost cannot contain an unknown unit cost");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn prepared_unit_set_rejects_empty_and_over_limit_membership() {
    let execution = OwnerExecution::new("file");
    let split = prepared_split(&execution, "split-a", None);

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::new(),
            Vec::new(),
            &preparation_request(),
        )
        .expect_err("a sealed set cannot be empty")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );

    let descriptors = vec![prepared_unit(b"unit", None); 4097];
    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::new(),
            descriptors,
            &preparation_request(),
        )
        .expect_err("a split cannot contain more than 4096 prepared units")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn prepared_unit_set_rejects_handle_and_aggregate_payload_budget_excess() {
    let execution = OwnerExecution::new("file");
    let split = prepared_split(&execution, "split-a", None);

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::from_static(b"shared"),
            vec![prepared_unit(b"unit", None)],
            &preparation_request_with(
                Instant::now() + Duration::from_secs(30),
                Arc::new(NeverCancelled),
                4,
                16,
            ),
        )
        .expect_err("the shared payload must honor the handle budget")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::from_static(b"unit"),
            vec![prepared_unit(b"large", None)],
            &preparation_request_with(
                Instant::now() + Duration::from_secs(30),
                Arc::new(NeverCancelled),
                4,
                16,
            ),
        )
        .expect_err("each unit payload must honor the handle budget")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::from_static(b"four"),
            vec![prepared_unit(b"four", None), prepared_unit(b"four", None)],
            &preparation_request_with(
                Instant::now() + Duration::from_secs(30),
                Arc::new(NeverCancelled),
                4,
                10,
            ),
        )
        .expect_err("shared and unit payloads must honor the aggregate budget")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

#[test]
fn prepared_unit_set_rejects_known_cost_mismatch_and_overflow() {
    let execution = OwnerExecution::new("file");
    let mismatch = prepared_split(&execution, "split-mismatch", Some(10));
    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &mismatch,
            bytes::Bytes::new(),
            vec![prepared_unit(b"unit", Some(9))],
            &preparation_request(),
        )
        .expect_err("known unit costs must equal the known split cost")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );

    let overflow = prepared_split(&execution, "split-overflow", None);
    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &overflow,
            bytes::Bytes::new(),
            vec![
                prepared_unit(b"first", Some(u64::MAX)),
                prepared_unit(b"second", Some(1))
            ],
            &preparation_request(),
        )
        .expect_err("unit cost summation must be checked")
        .kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

#[test]
fn prepared_unit_set_rejects_cancelled_and_expired_preparation() {
    let execution = OwnerExecution::new("file");
    let split = prepared_split(&execution, "split-a", None);

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::new(),
            vec![prepared_unit(b"unit", None)],
            &preparation_request_with(
                Instant::now() + Duration::from_secs(30),
                Arc::new(AlwaysCancelled),
                1024,
                4096,
            ),
        )
        .expect_err("preparation must observe cancellation before publication")
        .kind(),
        ConnectorErrorKind::Cancelled
    );

    assert_eq!(
        ConnectorPreparedScanUnitSet::try_new(
            execution.key.clone(),
            &split,
            bytes::Bytes::new(),
            vec![prepared_unit(b"unit", None)],
            &preparation_request_with(
                Instant::now() - Duration::from_secs(1),
                Arc::new(NeverCancelled),
                1024,
                4096,
            ),
        )
        .expect_err("preparation must observe an elapsed deadline before publication")
        .kind(),
        ConnectorErrorKind::DeadlineExceeded
    );
}

#[test]
fn prepared_unit_set_digest_is_deterministic_and_binding_sensitive() {
    let execution = OwnerExecution::new("file");
    let split = prepared_split(&execution, "split-a", Some(7));
    let descriptors = vec![
        prepared_unit(b"first", Some(3)),
        prepared_unit(b"second", Some(4)),
    ];
    let first = ConnectorPreparedScanUnitSet::try_new(
        execution.key.clone(),
        &split,
        bytes::Bytes::from_static(b"shared"),
        descriptors.clone(),
        &preparation_request(),
    )
    .expect("first sealed set");
    let second = ConnectorPreparedScanUnitSet::try_new(
        execution.key.clone(),
        &split,
        bytes::Bytes::from_static(b"shared"),
        descriptors,
        &preparation_request(),
    )
    .expect("identical sealed set");
    assert_eq!(first.membership_digest(), second.membership_digest());

    let foreign_binding = ConnectorExecutionBindingKey {
        instance_id: execution.key.instance_id.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([2; 16]),
    };
    let foreign = ConnectorPreparedScanUnitSet::try_new(
        foreign_binding,
        &split,
        bytes::Bytes::from_static(b"shared"),
        vec![
            prepared_unit(b"first", Some(3)),
            prepared_unit(b"second", Some(4)),
        ],
        &preparation_request(),
    )
    .expect("same split under another incarnation is separately sealed");
    assert_ne!(first.membership_digest(), foreign.membership_digest());
}

struct OwnerPlanning {
    instance_id: ConnectorInstanceId,
}

impl ConnectorScanPlanning for OwnerPlanning {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        _: &ConnectorTableHandle,
        _: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        unreachable!("control binding construction must not begin a scan")
    }

    fn plan_splits(
        &self,
        _: &ConnectorScanHandle,
        _: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        unreachable!("control binding construction must not plan splits")
    }
}

struct OwnerDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionDistribution for OwnerDistribution {
    fn declaration(
        &self,
        _: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            bytes::Bytes::new(),
        )
    }
}

struct OwnerMetadata {
    instance_id: ConnectorInstanceId,
}

impl OwnerMetadata {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
        }
    }
}

impl ConnectorMetadata for OwnerMetadata {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn namespace_exists(
        &self,
        _request: ConnectorNamespaceRequest,
    ) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn list_tables(
        &self,
        _request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }

    fn load_table(
        &self,
        _request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        unreachable!("instance construction must not resolve metadata")
    }
}

fn descriptor(instance_id: &str) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("file").expect("provider ID"),
        instance_id: ConnectorInstanceId::parse(instance_id).expect("instance ID"),
    }
}

#[test]
fn execution_bindings_are_valid_without_control_capabilities() {
    let key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("file").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    let binding = ConnectorExecutionBinding::try_new(
        ConnectorProviderId::parse("file").expect("provider ID"),
        key.clone(),
        Arc::new(OwnerExecution::new("file")),
    )
    .expect("read-only execution binding");

    assert_eq!(binding.key(), &key);
}

#[test]
fn execution_binding_rejects_a_read_capability_owned_by_another_generation() {
    let key = ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("file").expect("instance ID"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    assert_eq!(
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("file").expect("provider ID"),
            key,
            Arc::new(OwnerExecution::new("foreign")),
        )
        .err()
        .expect("a host must not attach a foreign read capability")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn control_binding_rejects_metadata_owned_by_another_instance() {
    let descriptor = descriptor("file");
    assert_eq!(
        ConnectorControlBinding::try_new(
            descriptor.clone(),
            ConnectorInstanceIncarnation::from_bytes([1; 16]),
            Arc::new(OwnerMetadata::new("foreign")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
            }),
            None,
        )
        .err()
        .expect("a host must not attach foreign metadata")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

struct OwnerStatistics {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl novarocks_spi::connector::StatisticsReader for OwnerStatistics {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn read_statistics(
        &self,
        _: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        unreachable!("control binding construction must not read statistics")
    }
}

impl ConnectorStatistics for OwnerStatistics {}

#[test]
fn control_binding_rejects_statistics_owned_by_another_generation() {
    let descriptor = descriptor("file");
    let incarnation = ConnectorInstanceIncarnation::from_bytes([1; 16]);
    let foreign = Arc::new(OwnerStatistics {
        descriptor: descriptor.clone(),
        incarnation: ConnectorInstanceIncarnation::from_bytes([2; 16]),
    });
    assert_eq!(
        ConnectorControlBinding::try_new_with_statistics(
            descriptor.clone(),
            incarnation,
            Arc::new(OwnerMetadata::new("file")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation,
            }),
            None,
            Some(foreign),
        )
        .err()
        .expect("a host must not attach foreign statistics")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

struct OwnerDataMutation {
    descriptor: ConnectorInstanceDescriptor,
    key: ConnectorExecutionBindingKey,
}

impl ConnectorDataMutation for OwnerDataMutation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn plan_mutation(
        &self,
        _: ConnectorDataMutationPlanningRequest,
    ) -> Result<ConnectorDataMutationPlan, ConnectorError> {
        unreachable!("binding construction must not plan a mutation")
    }

    fn execute(
        &self,
        _: ConnectorDataMutationExecuteRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        unreachable!("binding construction must not execute a mutation")
    }

    fn reconcile(
        &self,
        _: ConnectorDataMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorDataMutationReceipt>, ConnectorError> {
        unreachable!("binding construction must not reconcile a mutation")
    }
}

#[test]
fn control_binding_rejects_data_mutation_owned_by_another_generation() {
    let descriptor = descriptor("file");
    let incarnation = ConnectorInstanceIncarnation::from_bytes([1; 16]);
    let foreign = Arc::new(OwnerDataMutation {
        descriptor: descriptor.clone(),
        key: ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([2; 16]),
        },
    });
    assert_eq!(
        ConnectorControlBinding::try_new_with_data_mutation(
            descriptor.clone(),
            incarnation,
            Arc::new(OwnerMetadata::new("file")),
            Arc::new(OwnerPlanning {
                instance_id: descriptor.instance_id.clone(),
            }),
            Arc::new(OwnerDistribution {
                descriptor,
                incarnation,
            }),
            None,
            Some(foreign),
        )
        .err()
        .expect("a host must not attach foreign data mutation")
        .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

struct FixtureReader {
    batches: VecDeque<RecordBatch>,
    close_calls: usize,
}

struct ScriptedReader {
    responses: VecDeque<Option<RecordBatch>>,
}

impl ScriptedReader {
    fn new(responses: impl IntoIterator<Item = Option<RecordBatch>>) -> Self {
        Self {
            responses: responses.into_iter().collect(),
        }
    }
}

impl ConnectorBatchReader for ScriptedReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(self.responses.pop_front().flatten())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

impl FixtureReader {
    fn new(batches: impl IntoIterator<Item = RecordBatch>) -> Self {
        Self {
            batches: batches.into_iter().collect(),
            close_calls: 0,
        }
    }
}

impl ConnectorBatchReader for FixtureReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        Ok(self.batches.pop_front())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_calls += 1;
        Ok(())
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn batch(schema: SchemaRef, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).expect("fixture batch")
}

fn budget() -> ConnectorBatchBudget {
    ConnectorBatchBudget {
        max_rows: NonZeroUsize::new(2).expect("nonzero rows"),
        max_bytes: NonZeroUsize::new(1024).expect("nonzero bytes"),
    }
}

#[test]
fn batch_reader_conformance_accepts_schema_matched_stable_eos() {
    let expected_schema = schema();
    let mut reader = FixtureReader::new([
        batch(expected_schema.clone(), vec![1, 2]),
        batch(expected_schema.clone(), vec![3]),
    ]);

    let batches = assert_batch_reader_contract(&mut reader, &expected_schema, budget())
        .expect("reader with matching schema and stable EOS");

    assert_eq!(batches.len(), 2);
    assert_eq!(reader.close_calls, 2);
}

#[test]
fn batch_reader_conformance_rejects_a_schema_drift() {
    let expected_schema = schema();
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "other_value",
        DataType::Int64,
        false,
    )]));
    let mut reader = FixtureReader::new([batch(wrong_schema, vec![1])]);

    assert_eq!(
        assert_batch_reader_contract(&mut reader, &expected_schema, budget())
            .expect_err("a reader must not drift from its declared output schema")
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn batch_reader_conformance_rejects_a_batch_after_eos() {
    let expected_schema = schema();
    let mut reader = ScriptedReader::new([
        Some(batch(expected_schema.clone(), vec![1])),
        None,
        Some(batch(expected_schema.clone(), vec![2])),
    ]);

    assert_eq!(
        assert_batch_reader_contract(&mut reader, &expected_schema, budget())
            .expect_err("a provider must not resume after reporting EOS")
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}

#[test]
fn reader_metrics_snapshot_add_and_delta_are_saturating() {
    let first = ConnectorReaderMetricsSnapshot {
        bytes_read: 10,
        rows_decoded: 2,
        ..ConnectorReaderMetricsSnapshot::default()
    };
    let second = ConnectorReaderMetricsSnapshot {
        bytes_read: 7,
        rows_decoded: 3,
        ..ConnectorReaderMetricsSnapshot::default()
    };
    let total = first.saturating_add(second);
    assert_eq!(total.bytes_read, 17);
    assert_eq!(total.rows_decoded, 5);
    assert_eq!(
        total
            .saturating_delta_since(ConnectorReaderMetricsSnapshot {
                bytes_read: 20,
                rows_decoded: 1,
                ..ConnectorReaderMetricsSnapshot::default()
            })
            .bytes_read,
        0
    );
}

fn static_int_predicate(id: u32) -> ConnectorStaticPredicate {
    ConnectorStaticPredicate {
        id: ConnectorStaticPredicateId(id),
        column: ConnectorStaticPredicateColumn {
            field_ordinal: 2,
            data_type: ConnectorScalarType::Int32,
            nullable: false,
        },
        kind: ConnectorStaticPredicateKind::Comparison {
            op: ConnectorStaticComparisonOp::Ge,
            literal: ConnectorScalarValue::Int32(42),
        },
    }
}

#[test]
fn static_predicate_conformance_normalizes_a_total_out_of_order_response() {
    let predicates = vec![static_int_predicate(4), static_int_predicate(8)];
    let normalized = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(8),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Exact,
            },
        ],
    )
    .expect("total response is valid");

    assert_eq!(normalized[0].predicate_id, ConnectorStaticPredicateId(4));
    assert_eq!(normalized[0].kind, ConnectorPredicateDispositionKind::Exact);
    assert_eq!(normalized[1].predicate_id, ConnectorStaticPredicateId(8));
}

#[test]
fn static_predicate_conformance_rejects_unknown_or_duplicate_response_ids() {
    let predicates = vec![static_int_predicate(4), static_int_predicate(8)];
    let unknown = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(9),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
        ],
    )
    .expect_err("unknown ID is malformed provider output");
    assert_eq!(unknown.kind(), ConnectorErrorKind::CorruptData);

    let duplicate = normalize_predicate_dispositions(
        &predicates,
        &[
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::Unsupported,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(4),
                kind: ConnectorPredicateDispositionKind::PruningOnly,
            },
        ],
    )
    .expect_err("duplicate ID is malformed provider output");
    assert_eq!(duplicate.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn static_predicate_conformance_rejects_type_mismatch_and_invalid_planning_metrics() {
    let mut predicate = static_int_predicate(4);
    predicate.kind = ConnectorStaticPredicateKind::Comparison {
        op: ConnectorStaticComparisonOp::Eq,
        literal: ConnectorScalarValue::Int64(42),
    };
    assert_eq!(
        validate_static_predicates(&[predicate])
            .expect_err("literal and column types must match")
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );

    assert_eq!(
        novarocks_spi::connector::ConnectorSplitPlanningResult::try_new(
            Vec::new(),
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 1,
                candidate_units_pruned: 2,
                ..ConnectorSplitPlanningMetrics::default()
            },
        )
        .expect_err("pruned candidates cannot exceed considered candidates")
        .kind(),
        ConnectorErrorKind::CorruptData
    );
}
