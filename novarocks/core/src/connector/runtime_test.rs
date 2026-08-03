// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorOpenReaderRequest, ConnectorPrepareSplitRequest,
    ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor, ConnectorPreparedScanUnitSet,
    ConnectorProviderId, ConnectorReadExecution, ConnectorReaderMetricsSnapshot,
    ConnectorRequestContext, ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsMissingReason,
    ConnectorSplit,
};

use super::runtime::{
    ConnectorBatchReaderIter, ConnectorReadScanSource, ConnectorSplitAppend,
    IncrementalConnectorSplitAdapter,
};
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::node::scan::{BoundScanRanges, IncrementalScanRange, ScanMorsel, ScanSource};

struct FakeReader {
    batches: Vec<Result<Option<RecordBatch>, ConnectorError>>,
    close_result: Result<(), ConnectorError>,
    close_calls: Arc<Mutex<usize>>,
}

impl ConnectorBatchReader for FakeReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.batches.remove(0)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        *self.close_calls.lock().expect("close calls") += 1;
        self.close_result.clone()
    }
}

fn chunk_schema() -> Arc<ChunkSchema> {
    Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("id", DataType::Int32, false),
            None,
            None,
        )])
        .expect("chunk schema"),
    )
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![7]))],
    )
    .expect("record batch")
}

struct FakeRead {
    key: ConnectorExecutionBindingKey,
    unit_count: usize,
}

struct FailOnSplitRead {
    key: ConnectorExecutionBindingKey,
    failing_split_id: &'static str,
    prepare_calls: Arc<AtomicUsize>,
    open_calls: Arc<AtomicUsize>,
}

impl ConnectorReadExecution for FailOnSplitRead {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        if split.split_id() == self.failing_split_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "scripted prepare failure",
            ));
        }
        ConnectorPreparedScanUnitSet::try_new(
            self.key.clone(),
            split,
            bytes::Bytes::new(),
            vec![ConnectorPreparedScanUnitDescriptor::try_new(
                bytes::Bytes::from_static(b"scripted-unit"),
                split.estimated_bytes(),
                ConnectorScanUnitDomainFacts::missing(
                    ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
                ),
            )?],
            &request,
        )
    }

    fn open_unit_reader(
        &self,
        _unit: &ConnectorPreparedScanUnit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeReader {
            batches: vec![Ok(None)],
            close_result: Ok(()),
            close_calls: Arc::new(Mutex::new(0)),
        }))
    }
}

impl ConnectorReadExecution for FakeRead {
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
            bytes::Bytes::from_static(b"fake-shared"),
            (0..self.unit_count)
                .map(|ordinal| {
                    ConnectorPreparedScanUnitDescriptor::try_new(
                        bytes::Bytes::from(format!("fake-unit-{ordinal}")),
                        split
                            .estimated_bytes()
                            .map(|bytes| if ordinal == 0 { bytes } else { 0 }),
                        ConnectorScanUnitDomainFacts::missing(
                            ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
                        ),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            &request,
        )
    }

    fn open_unit_reader(
        &self,
        _unit: &ConnectorPreparedScanUnit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        Ok(Box::new(FakeReader {
            batches: vec![Ok(Some(batch())), Ok(None)],
            close_result: Ok(()),
            close_calls: Arc::new(Mutex::new(0)),
        }))
    }
}

fn fake_execution_binding(instance_id: ConnectorInstanceId) -> Arc<ConnectorExecutionBinding> {
    fake_execution_binding_with_unit_count(instance_id, 1)
}

fn fake_execution_binding_with_unit_count(
    instance_id: ConnectorInstanceId,
    unit_count: usize,
) -> Arc<ConnectorExecutionBinding> {
    let key = ConnectorExecutionBindingKey {
        instance_id,
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    Arc::new(
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("test").expect("provider ID"),
            key.clone(),
            Arc::new(FakeRead { key, unit_count }),
        )
        .expect("execution binding"),
    )
}

fn fail_on_split_execution_binding(
    instance_id: ConnectorInstanceId,
    failing_split_id: &'static str,
    prepare_calls: Arc<AtomicUsize>,
    open_calls: Arc<AtomicUsize>,
) -> Arc<ConnectorExecutionBinding> {
    let key = ConnectorExecutionBindingKey {
        instance_id,
        incarnation: ConnectorInstanceIncarnation::from_bytes([1; 16]),
    };
    Arc::new(
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("test").expect("provider ID"),
            key.clone(),
            Arc::new(FailOnSplitRead {
                key,
                failing_split_id,
                prepare_calls,
                open_calls,
            }),
        )
        .expect("execution binding"),
    )
}

fn request_context() -> ConnectorRequestContext {
    struct NotCancelled;
    impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        Arc::new(NotCancelled),
        1024,
        4096,
    )
    .expect("request context")
}

fn cancelled_request_context() -> ConnectorRequestContext {
    struct Cancelled;
    impl novarocks_spi::connector::ConnectorCancellation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        Arc::new(Cancelled),
        1024,
        4096,
    )
    .expect("cancelled request context")
}

#[test]
fn reader_iterator_converts_batches_and_closes_once_at_eos() {
    let close_calls = Arc::new(Mutex::new(0));
    let reader = FakeReader {
        batches: vec![Ok(Some(batch())), Ok(None)],
        close_result: Ok(()),
        close_calls: Arc::clone(&close_calls),
    };
    let chunks = ConnectorBatchReaderIter::new(Box::new(reader), chunk_schema())
        .collect::<Result<Vec<_>, _>>()
        .expect("reader chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 1);
    assert_eq!(*close_calls.lock().expect("close calls"), 1);
}

#[test]
fn reader_iterator_preserves_primary_read_failure_and_cleanup_context() {
    let close_calls = Arc::new(Mutex::new(0));
    let reader = FakeReader {
        batches: vec![Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "primary read failure",
        ))],
        close_result: Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            "cleanup failure",
        )),
        close_calls: Arc::clone(&close_calls),
    };
    let err = ConnectorBatchReaderIter::new(Box::new(reader), chunk_schema())
        .next()
        .expect("reader result")
        .expect_err("reader must fail");
    assert!(err.contains("primary read failure"), "err={err}");
    assert!(err.contains("cleanup failure"), "err={err}");
    assert_eq!(*close_calls.lock().expect("close calls"), 1);
}

struct MetricsReader {
    step: usize,
    metrics: ConnectorReaderMetricsSnapshot,
}

impl ConnectorBatchReader for MetricsReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        self.step += 1;
        match self.step {
            1 => {
                self.metrics.bytes_read = 10;
                self.metrics.read_requests = 1;
                self.metrics.rows_decoded = 1;
                self.metrics.batches_delivered = 1;
                Ok(Some(batch()))
            }
            2 => {
                self.metrics.bytes_read = 25;
                self.metrics.read_requests = 2;
                Ok(None)
            }
            _ => unreachable!("metrics reader reached terminal state"),
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

#[test]
fn file_read_profile_receives_metrics_deltas_once() {
    let profile = crate::runtime::profile::RuntimeProfile::new("file-read");
    let reader = MetricsReader {
        step: 0,
        metrics: ConnectorReaderMetricsSnapshot::default(),
    };
    ConnectorBatchReaderIter::with_profile(Box::new(reader), chunk_schema(), Some(profile.clone()))
        .collect::<Result<Vec<_>, _>>()
        .expect("consume metric reader");
    assert_eq!(profile.counter_value("ConnectorFileBytesRead"), Some(25));
    assert_eq!(profile.counter_value("ConnectorFileReadRequests"), Some(2));
    assert_eq!(profile.counter_value("ConnectorFileRowsDecoded"), Some(1));
    assert_eq!(
        profile.counter_value("ConnectorFileBatchesDelivered"),
        Some(1)
    );
}

#[test]
fn read_scan_source_opens_a_typed_split_and_adapts_its_batches() {
    let instance_id = ConnectorInstanceId::parse("test").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let split =
        ConnectorSplit::try_new(instance_id.clone(), "split-0", bytes::Bytes::new(), Some(1))
            .expect("split");
    let split_1 = ConnectorSplit::try_new(instance_id, "split-1", bytes::Bytes::new(), Some(1))
        .expect("split");
    let source = ConnectorReadScanSource::new(
        binding,
        vec![split, split_1],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let morsels = op.build_morsels().expect("build connector morsels");
    assert!(matches!(
        morsels.morsels.as_slice(),
        [
            ScanMorsel::ConnectorScanUnit { index: 0, .. },
            ScanMorsel::ConnectorScanUnit { index: 1, .. }
        ]
    ));
    let chunks = op
        .execute_iter(
            ScanMorsel::ConnectorScanUnit {
                index: 0,
                row_position: None,
            },
            None,
            None,
        )
        .expect("execute reader")
        .collect::<Result<Vec<_>, _>>()
        .expect("reader chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 1);
}

#[test]
fn read_scan_source_emits_one_morsel_per_prepared_unit() {
    let instance_id = ConnectorInstanceId::parse("test.multi-unit").expect("instance ID");
    let binding = fake_execution_binding_with_unit_count(instance_id.clone(), 3);
    let split =
        ConnectorSplit::try_new(instance_id, "split", bytes::Bytes::new(), Some(3)).expect("split");
    let source = ConnectorReadScanSource::new(
        binding,
        vec![split],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    assert!(matches!(
        op.build_morsels()
            .expect("prepared unit morsels")
            .morsels
            .as_slice(),
        [
            ScanMorsel::ConnectorScanUnit { index: 0, .. },
            ScanMorsel::ConnectorScanUnit { index: 1, .. },
            ScanMorsel::ConnectorScanUnit { index: 2, .. },
        ]
    ));
}

#[test]
fn read_scan_profile_counts_prepared_units_before_reader_open() {
    let instance_id = ConnectorInstanceId::parse("test.profile-units").expect("instance ID");
    let binding = fake_execution_binding_with_unit_count(instance_id.clone(), 3);
    let split =
        ConnectorSplit::try_new(instance_id, "split", bytes::Bytes::new(), Some(3)).expect("split");
    let source = ConnectorReadScanSource::new(
        binding,
        vec![split],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let profile = crate::runtime::profile::RuntimeProfile::new("connector");
    op.execute_iter(
        ScanMorsel::ConnectorScanUnit {
            index: 0,
            row_position: None,
        },
        Some(profile.clone()),
        None,
    )
    .expect("open first unit")
    .collect::<Result<Vec<_>, _>>()
    .expect("consume first unit");
    assert_eq!(profile.counter_value("ConnectorScanUnitsPrepared"), Some(3));
    assert_eq!(
        profile.counter_value("ConnectorScanUnitFactsExactUnits"),
        Some(0)
    );
    assert_eq!(
        profile.counter_value("ConnectorScanUnitFactsConservativeUnits"),
        Some(0)
    );
    assert_eq!(
        profile.counter_value("ConnectorScanUnitFactsMissingUnits"),
        Some(3)
    );
    assert_eq!(
        profile.counter_value("ConnectorScanUnitFactsAvailableColumns"),
        Some(0)
    );
    assert_eq!(
        profile.counter_value("ConnectorScanUnitFactsMissingColumns"),
        Some(0)
    );
    assert_eq!(profile.counter_value("ConnectorUnitReadersOpened"), Some(1));
}

#[test]
fn read_scan_source_rejects_cancellation_before_any_unit_is_published() {
    let instance_id = ConnectorInstanceId::parse("test.cancelled").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let split =
        ConnectorSplit::try_new(instance_id, "split", bytes::Bytes::new(), Some(1)).expect("split");
    let result = ConnectorReadScanSource::new(
        binding,
        vec![split],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: cancelled_request_context(),
        },
        chunk_schema(),
    );
    let error = match result {
        Ok(_) => panic!("cancelled preparation must not publish scan units"),
        Err(error) => error,
    };
    assert!(error.contains("cancelled"), "error={error}");
}

#[test]
fn read_scan_source_rejects_later_prepare_failure_without_publishing_units() {
    let instance_id = ConnectorInstanceId::parse("test.prepare-failure").expect("instance ID");
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let open_calls = Arc::new(AtomicUsize::new(0));
    let binding = fail_on_split_execution_binding(
        instance_id.clone(),
        "fail",
        Arc::clone(&prepare_calls),
        Arc::clone(&open_calls),
    );
    let result = ConnectorReadScanSource::new(
        binding,
        vec![
            ConnectorSplit::try_new(instance_id.clone(), "good", bytes::Bytes::new(), Some(1))
                .expect("good split"),
            ConnectorSplit::try_new(instance_id, "fail", bytes::Bytes::new(), Some(1))
                .expect("failing split"),
        ],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
    );
    let error = match result {
        Ok(_) => panic!("later prepare failure must reject the complete source"),
        Err(error) => error,
    };
    assert!(error.contains("scripted prepare failure"), "error={error}");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 2);
    assert_eq!(open_calls.load(Ordering::SeqCst), 0);
}

struct FakeIncrementalSplitAdapter {
    append: Mutex<Option<ConnectorSplitAppend>>,
}

impl IncrementalConnectorSplitAdapter for FakeIncrementalSplitAdapter {
    fn prepare_incremental_ranges(
        &self,
        _ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String> {
        self.append
            .lock()
            .expect("incremental split lock")
            .take()
            .ok_or_else(|| "no incremental split was configured".to_string())
    }
}

struct CountingIncrementalSplitAdapter {
    calls: Arc<Mutex<usize>>,
}

impl IncrementalConnectorSplitAdapter for CountingIncrementalSplitAdapter {
    fn prepare_incremental_ranges(
        &self,
        _ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String> {
        *self.calls.lock().expect("incremental calls") += 1;
        Ok(ConnectorSplitAppend::Plain {
            splits: Vec::new(),
            has_more: false,
        })
    }
}

struct CommitTrackingIncrementalSplitAdapter {
    append: ConnectorSplitAppend,
    commit_calls: Arc<Mutex<usize>>,
}

impl IncrementalConnectorSplitAdapter for CommitTrackingIncrementalSplitAdapter {
    fn prepare_incremental_ranges(
        &self,
        _ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String> {
        Ok(self.append.clone())
    }

    fn commit_incremental_ranges(&self, _append: &ConnectorSplitAppend) -> Result<(), String> {
        *self.commit_calls.lock().expect("commit calls") += 1;
        Ok(())
    }
}

#[test]
fn incremental_connector_source_appends_only_new_connector_morsels() {
    let instance_id = ConnectorInstanceId::parse("test.incremental").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let initial =
        ConnectorSplit::try_new(instance_id.clone(), "initial", bytes::Bytes::new(), Some(1))
            .expect("initial split");
    let next = ConnectorSplit::try_new(instance_id, "next", bytes::Bytes::new(), Some(1))
        .expect("next split");
    let adapter = Arc::new(FakeIncrementalSplitAdapter {
        append: Mutex::new(Some(ConnectorSplitAppend::Plain {
            splits: vec![next],
            has_more: false,
        })),
    });
    let source = ConnectorReadScanSource::new_with_incremental(
        binding,
        vec![initial],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
        adapter,
        true,
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    assert!(matches!(
        op.build_morsels()
            .expect("initial morsels")
            .morsels
            .as_slice(),
        [ScanMorsel::ConnectorScanUnit { index: 0, .. }]
    ));
    let appended = op
        .build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
        .expect("append connector split");
    assert!(matches!(
        appended.morsels.as_slice(),
        [ScanMorsel::ConnectorScanUnit { index: 1, .. }]
    ));
    assert!(!appended.has_more);
}

#[test]
fn incremental_connector_source_rejects_append_after_eos_without_calling_provider() {
    let instance_id = ConnectorInstanceId::parse("test.closed").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let calls = Arc::new(Mutex::new(0));
    let source = ConnectorReadScanSource::new_with_incremental(
        binding,
        vec![
            ConnectorSplit::try_new(instance_id, "initial", bytes::Bytes::new(), Some(1))
                .expect("initial split"),
        ],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
        Arc::new(CountingIncrementalSplitAdapter {
            calls: Arc::clone(&calls),
        }),
        false,
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let err = op
        .build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
        .expect_err("closed source must not accept more ranges");
    assert!(err.contains("closed"), "err={err}");
    assert_eq!(*calls.lock().expect("incremental calls"), 0);
    assert!(matches!(
        op.build_morsels()
            .expect("morsels after rejected append")
            .morsels
            .as_slice(),
        [ScanMorsel::ConnectorScanUnit { index: 0, .. }]
    ));
}

#[test]
fn incremental_connector_source_rejects_duplicate_appended_split_ids_atomically() {
    let instance_id = ConnectorInstanceId::parse("test.duplicate").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let next = ConnectorSplit::try_new(
        instance_id.clone(),
        "duplicate",
        bytes::Bytes::new(),
        Some(1),
    )
    .expect("duplicate split");
    let source = ConnectorReadScanSource::new_with_incremental(
        binding,
        vec![
            ConnectorSplit::try_new(instance_id, "initial", bytes::Bytes::new(), Some(1))
                .expect("initial split"),
        ],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
        Arc::new(FakeIncrementalSplitAdapter {
            append: Mutex::new(Some(ConnectorSplitAppend::Plain {
                splits: vec![next.clone(), next],
                has_more: false,
            })),
        }),
        true,
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let err = op
        .build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
        .expect_err("duplicate split IDs must fail");
    assert!(err.contains("duplicate"), "err={err}");
    assert!(matches!(
        op.build_morsels()
            .expect("morsels after rejected append")
            .morsels
            .as_slice(),
        [ScanMorsel::ConnectorScanUnit { index: 0, .. }]
    ));
}

#[test]
fn incremental_connector_source_does_not_commit_a_rejected_append() {
    let instance_id = ConnectorInstanceId::parse("test.commit.rejected").expect("instance ID");
    let binding = fake_execution_binding(instance_id.clone());
    let duplicate = ConnectorSplit::try_new(
        instance_id.clone(),
        "duplicate",
        bytes::Bytes::new(),
        Some(1),
    )
    .expect("duplicate split");
    let commit_calls = Arc::new(Mutex::new(0));
    let source = ConnectorReadScanSource::new_with_incremental(
        binding,
        vec![
            ConnectorSplit::try_new(instance_id, "initial", bytes::Bytes::new(), Some(1))
                .expect("initial split"),
        ],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
        Arc::new(CommitTrackingIncrementalSplitAdapter {
            append: ConnectorSplitAppend::Plain {
                splits: vec![duplicate.clone(), duplicate],
                has_more: false,
            },
            commit_calls: Arc::clone(&commit_calls),
        }),
        true,
    )
    .expect("prepare source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    op.build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
        .expect_err("duplicate split IDs must fail");
    assert_eq!(*commit_calls.lock().expect("commit calls"), 0);
}

#[test]
fn incremental_connector_source_keeps_original_morsels_when_prepare_fails() {
    let instance_id =
        ConnectorInstanceId::parse("test.incremental-prepare-failure").expect("instance ID");
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let open_calls = Arc::new(AtomicUsize::new(0));
    let binding = fail_on_split_execution_binding(
        instance_id.clone(),
        "fail",
        Arc::clone(&prepare_calls),
        Arc::clone(&open_calls),
    );
    let commit_calls = Arc::new(Mutex::new(0));
    let source = ConnectorReadScanSource::new_with_incremental(
        binding,
        vec![
            ConnectorSplit::try_new(instance_id.clone(), "initial", bytes::Bytes::new(), Some(1))
                .expect("initial split"),
        ],
        ConnectorOpenReaderRequest {
            expected_schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            batch: ConnectorBatchBudget {
                max_rows: std::num::NonZeroUsize::new(128).expect("rows"),
                max_bytes: std::num::NonZeroUsize::new(1024).expect("bytes"),
            },
            context: request_context(),
        },
        chunk_schema(),
        Arc::new(CommitTrackingIncrementalSplitAdapter {
            append: ConnectorSplitAppend::Plain {
                splits: vec![
                    ConnectorSplit::try_new(
                        instance_id.clone(),
                        "good",
                        bytes::Bytes::new(),
                        Some(1),
                    )
                    .expect("good append"),
                    ConnectorSplit::try_new(instance_id, "fail", bytes::Bytes::new(), Some(1))
                        .expect("failing append"),
                ],
                has_more: false,
            },
            commit_calls: Arc::clone(&commit_calls),
        }),
        true,
    )
    .expect("prepare initial source");
    let op = source.bind(BoundScanRanges::None).expect("bind source");
    let error = op
        .build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
        .expect_err("prepare failure must reject the whole append");
    assert!(error.contains("scripted prepare failure"), "error={error}");
    assert_eq!(*commit_calls.lock().expect("commit calls"), 0);
    assert_eq!(
        op.build_morsels()
            .expect("original morsels remain")
            .morsels
            .len(),
        1
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 3);
    assert_eq!(open_calls.load(Ordering::SeqCst), 0);
}
