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
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use novarocks_connector_paimon::domain::{
    PaimonBucketMode, PaimonColumn, PaimonMergeEngine, PaimonReadView, PaimonSplit, PaimonTable,
};
use novarocks_connector_paimon::page_source::PaimonPageSource;
use novarocks_connector_paimon::reader::{PaimonBatchReader, PaimonReadBatch, PaimonReader};
use novarocks_connector_paimon::resources::PaimonRequestResources;
use novarocks_connector_paimon::schema::PaimonDataType;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, SchemaTableName, SplitWeight};
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorRequestResources,
    ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLease,
    ConnectorResourceLedger,
};
use paimon::catalog::Identifier;
use paimon::io::{
    FileIO, FileStatus, FileStatusStream, ReadControl, ReadOnlyFileIO, ReadReservation,
};
use paimon::spec::{BinaryRow, DataType as SdkDataType, IntType, Schema as SdkSchema, TableSchema};
use paimon::table::Table;

enum Step {
    Batch(RecordBatch),
    Transferred(PaimonReadBatch),
    TransferThenCancel(PaimonReadBatch, Arc<Ledger>),
    Error(ConnectorError),
    Eof,
}

struct ScriptedReader {
    steps: VecDeque<Step>,
    closes: Arc<AtomicUsize>,
}

impl PaimonBatchReader for ScriptedReader {
    fn next_batch(&mut self) -> Result<Option<PaimonReadBatch>, ConnectorError> {
        match self.steps.pop_front().unwrap_or(Step::Eof) {
            Step::Batch(batch) => Ok(Some(PaimonReadBatch::unreserved(batch))),
            Step::Transferred(batch) => Ok(Some(batch)),
            Step::TransferThenCancel(batch, ledger) => {
                ledger.cancelled.store(true, Ordering::Release);
                Ok(Some(batch))
            }
            Step::Error(error) => Err(error),
            Step::Eof => Ok(None),
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct Ledger {
    retained: Arc<AtomicU64>,
    peak: AtomicU64,
    reservations: AtomicUsize,
    checkpoints: AtomicUsize,
    cancelled: AtomicBool,
    limit: u64,
}

impl ConnectorResourceLedger for Ledger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        self.checkpoints.fetch_add(1, Ordering::AcqRel);
        if self.cancelled.load(Ordering::Acquire) {
            Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "test cancellation",
            ))
        } else {
            Ok(ConnectorResourceCheckpoint::new(1))
        }
    }

    fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
        assert_eq!(class, ConnectorResourceClass::ReaderOutput);
        self.reservations.fetch_add(1, Ordering::AcqRel);
        let old = self.retained.fetch_add(bytes, Ordering::AcqRel);
        if old.saturating_add(bytes) > self.limit {
            self.retained.fetch_sub(bytes, Ordering::AcqRel);
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "test output budget exhausted",
            ));
        }
        self.peak.fetch_max(old + bytes, Ordering::AcqRel);
        Ok(Box::new(Lease {
            bytes,
            retained: Arc::clone(&self.retained),
        }))
    }
}

fn ledger(budget: u64) -> Arc<Ledger> {
    Arc::new(Ledger {
        retained: Arc::new(AtomicU64::new(0)),
        peak: AtomicU64::new(0),
        reservations: AtomicUsize::new(0),
        checkpoints: AtomicUsize::new(0),
        cancelled: AtomicBool::new(false),
        limit: budget,
    })
}

fn request_resources(ledger: &Arc<Ledger>) -> (ConnectorRequestResources, PaimonRequestResources) {
    let resources = ConnectorRequestResources::new(ledger.clone());
    let paimon_resources = PaimonRequestResources::new(
        resources.clone(),
        ledger.clone(),
        Instant::now() + Duration::from_secs(60),
    );
    (resources, paimon_resources)
}

impl ConnectorCancellation for Ledger {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct Lease {
    bytes: u64,
    retained: Arc<AtomicU64>,
}

impl ConnectorResourceLease for Lease {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        self.retained.fetch_add(additional, Ordering::AcqRel);
        self.bytes += additional;
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        self.retained
            .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn fixture(
    steps: Vec<Step>,
    row_limit: Option<u64>,
    budget: u64,
) -> (PaimonPageSource, Arc<Ledger>, Arc<AtomicUsize>) {
    let closes = Arc::new(AtomicUsize::new(0));
    let ledger = ledger(budget);
    let reader = ScriptedReader {
        steps: steps.into(),
        closes: Arc::clone(&closes),
    };
    let (_, resources) = request_resources(&ledger);
    (
        PaimonPageSource::new(Box::new(reader), resources, row_limit),
        ledger,
        closes,
    )
}

fn int_batch(values: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        ArrowDataType::Int32,
        false,
    )]));
    let column: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
    RecordBatch::try_new(schema, vec![column]).expect("batch")
}

fn count_batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .expect("count batch")
}

#[test]
fn zero_projection_preserves_visible_row_count_for_count() {
    let (mut source, ledger, closes) =
        fixture(vec![Step::Batch(count_batch(3)), Step::Eof], None, 1024);
    let page = source.next_source_page().unwrap().expect("count page");
    assert_eq!(page.position_count(), 3);
    assert_eq!(page.channel_count(), 0);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert!(source.next_source_page().unwrap().is_none());
    assert!(source.is_finished());
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn zero_limit_closes_without_polling_the_reader() {
    let (mut source, ledger, closes) = fixture(vec![Step::Batch(int_batch(&[1]))], Some(0), 1024);
    assert!(source.is_finished());
    assert!(source.next_source_page().unwrap().is_none());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert_eq!(ledger.checkpoints.load(Ordering::Acquire), 0);
}

#[test]
fn post_merge_limit_slices_output_and_closes_reader_once() {
    let (mut source, _ledger, closes) = fixture(
        vec![Step::Batch(int_batch(&[20, 21, 22])), Step::Eof],
        Some(2),
        1024,
    );
    let mut page = source.next_source_page().unwrap().expect("limited page");
    assert_eq!(page.position_count(), 2);
    let values = page
        .block(0)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &[20, 21]);
    assert!(source.is_finished());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    source.close().unwrap();
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn output_charge_moves_with_page_and_releases_on_drop() {
    let (mut source, ledger, _closes) = fixture(
        vec![Step::Batch(int_batch(&[1, 2, 3])), Step::Eof],
        None,
        1024,
    );
    let page = source.next_source_page().unwrap().expect("page");
    let charged = page.output_memory_bytes().expect("accounted output");
    assert!(charged > 0);
    assert_eq!(ledger.retained.load(Ordering::Acquire), charged);
    drop(page);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
}

#[test]
fn transferred_output_uses_one_charge_with_only_one_batch_of_budget() {
    let batch = int_batch(&[1, 2, 3]);
    let retained_bytes = batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum();
    let ledger = ledger(retained_bytes);
    let (resources, paimon_resources) = request_resources(&ledger);
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, retained_bytes)
        .expect("SDK output reservation");
    let closes = Arc::new(AtomicUsize::new(0));
    let reader = ScriptedReader {
        steps: vec![Step::Transferred(PaimonReadBatch::with_output_reservation(
            batch,
            reservation,
        ))]
        .into(),
        closes: Arc::clone(&closes),
    };
    let mut source = PaimonPageSource::new(Box::new(reader), paimon_resources, None);

    let page = source.next_source_page().unwrap().expect("page");
    assert_eq!(page.output_memory_bytes(), Some(retained_bytes));
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.peak.load(Ordering::Acquire), retained_bytes);
    assert_eq!(ledger.retained.load(Ordering::Acquire), retained_bytes);

    drop(page);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
}

#[test]
fn transferred_output_is_released_when_post_poll_checkpoint_fails() {
    let batch = int_batch(&[1, 2, 3]);
    let retained_bytes = batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum();
    let ledger = ledger(retained_bytes);
    let (resources, paimon_resources) = request_resources(&ledger);
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, retained_bytes)
        .expect("SDK output reservation");
    let closes = Arc::new(AtomicUsize::new(0));
    let reader = ScriptedReader {
        steps: vec![Step::TransferThenCancel(
            PaimonReadBatch::with_output_reservation(batch, reservation),
            Arc::clone(&ledger),
        )]
        .into(),
        closes: Arc::clone(&closes),
    };
    let mut source = PaimonPageSource::new(Box::new(reader), paimon_resources, None);

    let error = source.next_source_page().unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn close_releases_transferred_output_still_owned_by_reader() {
    let batch = int_batch(&[1, 2, 3]);
    let retained_bytes = batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum();
    let ledger = ledger(retained_bytes);
    let (resources, paimon_resources) = request_resources(&ledger);
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, retained_bytes)
        .expect("SDK output reservation");
    let closes = Arc::new(AtomicUsize::new(0));
    let reader = ScriptedReader {
        steps: vec![Step::Transferred(PaimonReadBatch::with_output_reservation(
            batch,
            reservation,
        ))]
        .into(),
        closes: Arc::clone(&closes),
    };
    let mut source = PaimonPageSource::new(Box::new(reader), paimon_resources, None);

    source.close().unwrap();
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn cancellation_before_poll_closes_without_producing_a_page() {
    let (mut source, ledger, closes) = fixture(vec![Step::Batch(int_batch(&[1]))], None, 1024);
    ledger.cancelled.store(true, Ordering::Release);
    let error = source.next_source_page().unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert!(source.is_finished());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
}

#[test]
fn output_budget_failure_closes_and_balances_resources() {
    let (mut source, ledger, closes) = fixture(vec![Step::Batch(int_batch(&[1, 2, 3]))], None, 1);
    let error = source.next_source_page().unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert!(source.is_finished());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
}

#[test]
fn corrupt_reader_error_closes_exactly_once() {
    let (mut source, _ledger, closes) = fixture(
        vec![Step::Error(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "missing _VALUE_KIND",
        ))],
        None,
        1024,
    );
    let error = source.next_source_page().unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(source.is_finished());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    source.close().unwrap();
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn empty_merged_batches_are_not_exposed_as_end_or_pages() {
    let (mut source, ledger, closes) = fixture(
        vec![
            Step::Batch(count_batch(0)),
            Step::Batch(count_batch(2)),
            Step::Eof,
        ],
        None,
        1024,
    );
    let page = source.next_source_page().unwrap().expect("non-empty page");
    assert_eq!(page.position_count(), 2);
    assert!(ledger.checkpoints.load(Ordering::Acquire) >= 2);
    assert!(source.next_source_page().unwrap().is_none());
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[test]
fn concurrent_close_calls_remain_idempotent() {
    let (source, _ledger, closes) = fixture(vec![Step::Eof], None, 1024);
    let source = Arc::new(Mutex::new(source));
    let workers = (0..4)
        .map(|_| {
            let source = Arc::clone(&source);
            std::thread::spawn(move || source.lock().unwrap().close().unwrap())
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(closes.load(Ordering::Acquire), 1);
}

#[derive(Debug)]
struct NoIo;

#[async_trait]
impl ReadOnlyFileIO for NoIo {
    async fn stat(&self, _path: &str) -> paimon::Result<FileStatus> {
        unreachable!("frozen validation performs no I/O")
    }

    async fn exists(&self, _path: &str) -> paimon::Result<bool> {
        unreachable!("frozen validation performs no I/O")
    }

    async fn read(&self, _path: &str, _range: Range<u64>) -> paimon::Result<Bytes> {
        unreachable!("frozen validation performs no I/O")
    }

    async fn list(&self, _path: &str, _recursive: bool) -> paimon::Result<FileStatusStream> {
        Ok(Box::pin(stream::empty()))
    }
}

#[derive(Debug)]
struct NoControl;

impl ReadControl for NoControl {
    fn check_active(&self) -> paimon::Result<()> {
        Ok(())
    }

    fn checkpoint(&self) -> paimon::Result<()> {
        Ok(())
    }

    fn try_reserve(&self, bytes: u64) -> paimon::Result<Box<dyn ReadReservation>> {
        Ok(Box::new(NoReservation(bytes)))
    }
}

#[derive(Debug)]
struct NoReservation(u64);

impl ReadReservation for NoReservation {
    fn bytes(&self) -> u64 {
        self.0
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

fn frozen_reader_fixture() -> (
    Arc<Table>,
    PaimonTable,
    PaimonReadView,
    PaimonSplit,
    paimon::DataSplit,
    Vec<PaimonColumn>,
) {
    let location = "s3://bucket/warehouse/db/table";
    let schema = SdkSchema::builder()
        .column("id", SdkDataType::Int(IntType::with_nullable(false)))
        .column("value", SdkDataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    let sdk_table = Arc::new(Table::new(
        FileIO::from_read_only(Arc::new(NoIo), Arc::new(NoControl)),
        Identifier::new("db", "table"),
        location.to_string(),
        TableSchema::new(7, &schema),
        None,
    ));
    let table = PaimonTable::try_new(
        SchemaTableName::try_new("db", "table").unwrap(),
        location,
        PaimonMergeEngine::Deduplicate,
        PaimonBucketMode::Fixed,
        vec![0],
        vec![],
    )
    .unwrap();
    let view = PaimonReadView::try_new(location, Some(11), 7, [1; 32], [2; 32], None).unwrap();
    let serialized_partition = BinaryRow::new(0).to_serialized_bytes();
    let partition = serialized_partition[4..].to_vec();
    let split = PaimonSplit::try_new(
        11,
        7,
        0,
        partition.clone(),
        0,
        format!("{location}/bucket-0"),
        1,
        vec![],
        None,
        None,
        false,
        false,
        SplitWeight::STANDARD,
    )
    .unwrap();
    let sdk_split = paimon::DataSplit::builder()
        .with_snapshot(11)
        .with_partition(BinaryRow::from_bytes(0, partition))
        .with_bucket(0)
        .with_bucket_path(format!("{location}/bucket-0"))
        .with_total_buckets(1)
        .with_data_files(vec![])
        .with_raw_convertible(false)
        .build()
        .unwrap();
    let columns = vec![
        PaimonColumn::try_new(0, "id", PaimonDataType::Int32, false, 0).unwrap(),
        PaimonColumn::try_new(1, "value", PaimonDataType::Int32, true, 1).unwrap(),
    ];
    (sdk_table, table, view, split, sdk_split, columns)
}

#[test]
fn reader_construction_is_zero_io_and_accepts_zero_projection() {
    let (sdk_table, table, view, split, sdk_split, _) = frozen_reader_fixture();
    let reader = PaimonReader::try_new(sdk_table, &table, &view, &split, sdk_split, &[])
        .expect("zero projection reader");
    assert!(reader.output_schema().fields().is_empty());
}

#[test]
fn sdk_reader_exposes_only_the_requested_public_columns() {
    let (sdk_table, table, view, split, sdk_split, columns) = frozen_reader_fixture();
    let mut reader = PaimonReader::try_new(sdk_table, &table, &view, &split, sdk_split, &columns)
        .expect("projected reader");
    let names = reader
        .output_schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["id", "value"]);
    assert!(reader.next_batch().unwrap().is_none());
    reader.close().unwrap();
}

#[test]
fn reader_rejects_snapshot_drift_before_sdk_io() {
    let (sdk_table, table, _view, split, sdk_split, columns) = frozen_reader_fixture();
    let drifted =
        PaimonReadView::try_new(table.location(), Some(12), 7, [1; 32], [2; 32], None).unwrap();
    let error =
        match PaimonReader::try_new(sdk_table, &table, &drifted, &split, sdk_split, &columns) {
            Ok(_) => panic!("snapshot drift must fail"),
            Err(error) => error,
        };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn reader_rejects_projection_type_or_order_drift() {
    let (sdk_table, table, view, split, sdk_split, _) = frozen_reader_fixture();
    let wrong = vec![PaimonColumn::try_new(0, "id", PaimonDataType::Int64, false, 0).unwrap()];
    let error = match PaimonReader::try_new(sdk_table, &table, &view, &split, sdk_split, &wrong) {
        Ok(_) => panic!("projection drift must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}
