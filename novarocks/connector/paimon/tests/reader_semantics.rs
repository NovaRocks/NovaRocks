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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::StreamExt;
use novarocks_connector_paimon::page_source::PaimonPageStream;
use novarocks_connector_paimon::reader::{PaimonBatchStream, PaimonReadBatch};
use novarocks_connector_paimon::resources::{PaimonExecutionResources, PaimonRequestControl};
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, ConnectorSourceOperations, OwnedConnectorPageStream,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionResources, ConnectorResourceCheckpoint,
    ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger, ConnectorStopOwner,
};

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

impl ScriptedReader {
    fn next_batch(&mut self) -> Result<Option<PaimonReadBatch>, ConnectorError> {
        match self.steps.pop_front().unwrap_or(Step::Eof) {
            Step::Batch(batch) => Ok(Some(PaimonReadBatch::unreserved(batch))),
            Step::Transferred(batch) => Ok(Some(batch)),
            Step::TransferThenCancel(batch, ledger) => {
                ledger.stop.request_stop();
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

/// The same script, polled by a page stream: every step is ready at once.
struct ScriptedStream {
    reader: ScriptedReader,
}

impl PaimonBatchStream for ScriptedStream {
    fn poll_next_batch(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<Option<PaimonReadBatch>, ConnectorError>> {
        std::task::Poll::Ready(self.reader.next_batch())
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.reader.close()
    }
}

/// A page stream over `steps`, already open; returns the stream and how many
/// times its reader was closed.
fn stream_fixture(
    steps: Vec<Step>,
    resources: PaimonExecutionResources,
    budget: &ConnectorPollBudget,
    operations: Option<ConnectorSourceOperations>,
) -> (OwnedConnectorPageStream, Arc<AtomicUsize>) {
    let closes = Arc::new(AtomicUsize::new(0));
    let reader = ScriptedStream {
        reader: ScriptedReader {
            steps: steps.into(),
            closes: Arc::clone(&closes),
        },
    };
    let opening = Box::pin(async move { Ok(Box::new(reader) as Box<dyn PaimonBatchStream>) });
    (
        Box::pin(PaimonPageStream::new(
            opening, resources, budget, operations,
        )),
        closes,
    )
}

struct Ledger {
    retained: Arc<AtomicU64>,
    peak: AtomicU64,
    reservations: AtomicUsize,
    checkpoints: AtomicUsize,
    stop: ConnectorStopOwner,
    limit: u64,
}

impl ConnectorResourceLedger for Ledger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        self.checkpoints.fetch_add(1, Ordering::AcqRel);
        if self.stop.is_stopped() {
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
        stop: ConnectorStopOwner::new(),
        limit: budget,
    })
}

fn request_resources(
    ledger: &Arc<Ledger>,
) -> (ConnectorExecutionResources, PaimonExecutionResources) {
    let resources = ConnectorExecutionResources::from_admitted_ledger(ledger.clone());
    let paimon_resources = PaimonExecutionResources::new(
        PaimonRequestControl::new(ledger.stop.view(), Instant::now() + Duration::from_secs(60)),
        resources.clone(),
    );
    (resources, paimon_resources)
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

/// The retained bytes of `batch`'s columns.
fn retained_bytes_of(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size() as u64)
        .sum()
}

/// A budget no stream in these tests runs out of.
fn unbounded_budget() -> ConnectorPollBudget {
    let budget = ConnectorPollBudget::new();
    budget.refill(u64::MAX);
    budget
}

#[tokio::test]
async fn a_zero_projection_stream_reports_the_visible_row_count_for_count() {
    let ledger = ledger(1024);
    let (_, resources) = request_resources(&ledger);
    let budget = unbounded_budget();
    let (mut stream, closes) = stream_fixture(
        vec![Step::Batch(count_batch(3)), Step::Eof],
        resources,
        &budget,
        None,
    );
    let page = stream.next().await.expect("a page").expect("count page");
    assert_eq!(page.position_count(), 3);
    assert_eq!(page.channel_count(), 0);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert!(stream.next().await.is_none());
    assert_eq!(closes.load(Ordering::Acquire), 1);
    stream.close().await.expect("close");
}

#[tokio::test]
async fn a_stream_page_carries_its_output_charge_and_releases_it_on_drop() {
    let ledger = ledger(1024);
    let (_, resources) = request_resources(&ledger);
    let budget = unbounded_budget();
    let (mut stream, _) = stream_fixture(
        vec![Step::Batch(int_batch(&[1, 2, 3])), Step::Eof],
        resources,
        &budget,
        None,
    );
    let page = stream.next().await.expect("a page").expect("page");
    let charged = page.output_memory_bytes().expect("accounted output");
    assert!(charged > 0);
    assert_eq!(ledger.retained.load(Ordering::Acquire), charged);
    drop(page);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert!(stream.next().await.is_none());
    stream.close().await.expect("close");
}

#[tokio::test]
async fn a_failed_checkpoint_after_a_batch_releases_its_transferred_output() {
    let batch = int_batch(&[1, 2, 3]);
    let retained_bytes = retained_bytes_of(&batch);
    let ledger = ledger(retained_bytes);
    let (resources, paimon_resources) = request_resources(&ledger);
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, retained_bytes)
        .expect("SDK output reservation");
    let budget = unbounded_budget();
    let (mut stream, closes) = stream_fixture(
        vec![Step::TransferThenCancel(
            PaimonReadBatch::with_output_reservation(batch, reservation),
            Arc::clone(&ledger),
        )],
        paimon_resources,
        &budget,
        None,
    );
    let error = stream
        .next()
        .await
        .expect("an item")
        .expect_err("cancelled");
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert!(stream.next().await.is_none());
    stream.close().await.expect("close a failed stream");
}

#[tokio::test]
async fn closing_an_unopened_stream_releases_the_output_its_reader_still_owns() {
    let batch = int_batch(&[1, 2, 3]);
    let retained_bytes = retained_bytes_of(&batch);
    let ledger = ledger(retained_bytes);
    let (resources, paimon_resources) = request_resources(&ledger);
    let reservation = resources
        .try_reserve(ConnectorResourceClass::ReaderOutput, retained_bytes)
        .expect("SDK output reservation");
    let budget = unbounded_budget();
    let (stream, closes) = stream_fixture(
        vec![Step::Transferred(PaimonReadBatch::with_output_reservation(
            batch,
            reservation,
        ))],
        paimon_resources,
        &budget,
        None,
    );
    assert_eq!(ledger.retained.load(Ordering::Acquire), retained_bytes);
    stream.close().await.expect("close");
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert_eq!(closes.load(Ordering::Acquire), 0, "the reader never opened");
}

#[tokio::test]
async fn cancellation_and_an_exhausted_output_budget_close_the_reader_and_balance() {
    // Cancelled before its first poll: no page, the reader closed once.
    let ledger_cancelled = ledger(1024);
    let (_, resources) = request_resources(&ledger_cancelled);
    ledger_cancelled.stop.request_stop();
    let budget = unbounded_budget();
    let (mut stream, closes) =
        stream_fixture(vec![Step::Batch(int_batch(&[1]))], resources, &budget, None);
    let error = stream
        .next()
        .await
        .expect("an item")
        .expect_err("cancelled");
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert_eq!(ledger_cancelled.retained.load(Ordering::Acquire), 0);
    stream.close().await.expect("close a failed stream");

    // An output budget too small for the page: refused, nothing retained.
    let ledger_small = ledger(1);
    let (_, resources) = request_resources(&ledger_small);
    let (mut stream, closes) = stream_fixture(
        vec![Step::Batch(int_batch(&[1, 2, 3]))],
        resources,
        &budget,
        None,
    );
    let error = stream
        .next()
        .await
        .expect("an item")
        .expect_err("over budget");
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert_eq!(ledger_small.retained.load(Ordering::Acquire), 0);
    stream.close().await.expect("close a failed stream");
}

#[tokio::test]
async fn a_stream_spends_a_turn_on_every_sdk_batch_and_skips_the_empty_ones() {
    let ledger = ledger(u64::MAX);
    let (_, resources) = request_resources(&ledger);
    let mut steps = (0..5)
        .map(|_| Step::Batch(int_batch(&[])))
        .collect::<Vec<_>>();
    steps.push(Step::Batch(int_batch(&[1, 2, 3])));
    steps.push(Step::Eof);
    let budget = ConnectorPollBudget::new();
    let (mut stream, closes) = stream_fixture(steps, resources, &budget, None);

    // One unit a turn; every step of the script is ready, so each Pending is
    // a spent turn rather than a wait for I/O.
    budget.refill(1);
    let mut rows = 0;
    let mut yields = 0;
    loop {
        match futures::poll!(stream.next()) {
            std::task::Poll::Ready(Some(page)) => rows += page.expect("page").position_count(),
            std::task::Poll::Ready(None) => break,
            std::task::Poll::Pending => {
                yields += 1;
                assert!(yields < 100, "the stream never ended");
                budget.refill(1);
            }
        }
    }
    assert_eq!(rows, 3, "empty batches are skipped, not delivered");
    assert_eq!(budget.exhaustions(), yields);
    assert!(
        yields >= 5,
        "six SDK batches on one unit a turn must yield, saw {yields}"
    );
    assert_eq!(
        closes.load(Ordering::Acquire),
        1,
        "the reader closes at its end"
    );
}

#[tokio::test]
async fn a_stream_page_keeps_the_output_reservation_the_sdk_handed_over() {
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
    let budget = ConnectorPollBudget::new();
    budget.refill(64);
    let (mut stream, _) = stream_fixture(
        vec![Step::Transferred(PaimonReadBatch::with_output_reservation(
            batch,
            reservation,
        ))],
        paimon_resources,
        &budget,
        None,
    );
    let page = stream.next().await.expect("a page").expect("page");
    assert_eq!(page.output_memory_bytes(), Some(retained_bytes));
    // One charge for the batch: the page reuses the SDK's reservation.
    assert_eq!(ledger.reservations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.peak.load(Ordering::Acquire), retained_bytes);
    assert_eq!(ledger.retained.load(Ordering::Acquire), retained_bytes);
    drop(page);
    assert_eq!(ledger.retained.load(Ordering::Acquire), 0);
    assert!(stream.next().await.is_none());
    stream.close().await.expect("close");
}

#[tokio::test]
async fn a_failing_stream_closes_its_reader_once_and_then_ends() {
    let ledger = ledger(u64::MAX);
    let (_, resources) = request_resources(&ledger);
    let budget = ConnectorPollBudget::new();
    budget.refill(64);
    let (mut stream, closes) = stream_fixture(
        vec![Step::Error(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "injected corrupt batch",
        ))],
        resources,
        &budget,
        None,
    );
    let error = stream.next().await.expect("an item").expect_err("error");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert_eq!(closes.load(Ordering::Acquire), 1);
    assert!(stream.next().await.is_none());
    stream.close().await.expect("closing a failed stream");
    assert_eq!(
        closes.load(Ordering::Acquire),
        1,
        "its reader is closed once"
    );
}

#[tokio::test]
async fn closing_a_stream_observes_the_exit_of_its_own_operations() {
    let ledger = ledger(u64::MAX);
    let (_, resources) = request_resources(&ledger);
    let budget = ConnectorPollBudget::new();
    let task = ConnectorSourceOperations::new();
    let split = task.child().expect("the split's own operations");
    // A read the split admitted and that is still in flight.
    let read = split.admit(Arc::new(|| {})).expect("admitted read");
    let (stream, closes) = stream_fixture(vec![Step::Eof], resources, &budget, Some(split.clone()));

    let mut closed = stream.close();
    assert!(
        futures::poll!(&mut closed).is_pending(),
        "its read has not exited"
    );
    assert!(split.is_sealed());
    assert!(!task.is_sealed(), "the task source stays open");
    assert_eq!(closes.load(Ordering::Acquire), 0, "the stream never opened");
    read.end(Ok(()));
    closed.await.expect("the split exited");
    assert_eq!(task.live_operations(), 0);

    // A close future that is dropped only stops observing.
    let (_, resources) = request_resources(&ledger);
    let split = task.child().expect("the split's own operations");
    let read = split.admit(Arc::new(|| {})).expect("admitted read");
    let (stream, _) = stream_fixture(vec![Step::Eof], resources, &budget, Some(split.clone()));
    drop(stream.close());
    assert!(split.is_sealed());
    read.end(Ok(()));
    split.exited().await.expect("the split still exits");
}
