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

//! The awaited Parquet reader against the blocking one: the same rows,
//! positions and storage reads, with a decoder request's independent ranges
//! fetched concurrently through the source's range service.

mod common;

use std::collections::HashMap;
use std::fs::File;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::{Fixture, collect};
use novarocks_fs::{
    AsyncFileBatchReader, BoundFile, FileBatch, FileCancellation, FileErrorKind, FileFormat,
    FileIdentity, FileIoRuntime, FileProjection, FileRangeScope, FileRangeService, FileReadBudget,
    FileReadContext, FileReadRange, FileReadRequest, FileReaderOptions, FileResult, FileTask,
    FileTaskFuture, FileTaskSpawner, FsAccessResolver, MinMaxPredicateOp, MinMaxPredicateValue,
    PhysicalPageSelection, PhysicalPruning, ScanPredicate, ScanPredicateDomain,
    ScanPredicateSource, TokioFileIoRuntime, inspect_parquet_metadata,
    inspect_parquet_metadata_async, open_file_reader, open_file_reader_async,
};
use novarocks_spi::connector::StorageAccessDomainId;
use novarocks_spi::connector::read_stack::ConnectorSourceOperations;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::sync::Notify;

/// Spawns each file task on the runtime but holds it until the test
/// releases its spawn index, or drops it unrun when told to fail it.
struct OrderedGateSpawner {
    handle: tokio::runtime::Handle,
    started: AtomicUsize,
    gates: Mutex<HashMap<usize, Arc<Notify>>>,
    released: Mutex<Vec<usize>>,
    fail: Mutex<Vec<usize>>,
    hold: bool,
}

impl OrderedGateSpawner {
    fn new(handle: tokio::runtime::Handle, hold: bool) -> Arc<Self> {
        Arc::new(Self {
            handle,
            started: AtomicUsize::new(0),
            gates: Mutex::new(HashMap::new()),
            released: Mutex::new(Vec::new()),
            fail: Mutex::new(Vec::new()),
            hold,
        })
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn gate(&self, index: usize) -> Arc<Notify> {
        Arc::clone(
            self.gates
                .lock()
                .unwrap()
                .entry(index)
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    fn release(&self, index: usize) {
        self.released.lock().unwrap().push(index);
        self.gate(index).notify_one();
    }

    fn fail_unrun(&self, index: usize) {
        self.fail.lock().unwrap().push(index);
    }
}

impl FileTaskSpawner for OrderedGateSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        let index = self.started.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail.lock().unwrap().contains(&index);
        let hold = self.hold && !self.released.lock().unwrap().contains(&index);
        let gate = self.gate(index);
        Ok(FileTask::new(self.handle.spawn(async move {
            if fail {
                drop(task);
                return;
            }
            if hold {
                gate.notified().await;
            }
            task.await;
        })))
    }

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.handle.spawn_blocking(job);
    }
}

/// Counts file tasks, one per storage read segment or stat.
struct CountingSpawner {
    handle: tokio::runtime::Handle,
    spawned: AtomicUsize,
}

impl FileTaskSpawner for CountingSpawner {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        self.spawned.fetch_add(1, Ordering::SeqCst);
        Ok(FileTask::new(self.handle.spawn(task)))
    }

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.handle.spawn_blocking(job);
    }
}

struct LargeFile {
    _directory: tempfile::TempDir,
    file: BoundFile,
    runtime: tokio::runtime::Runtime,
}

/// Three row groups of 20 000 rows: well past the small-file probe, so
/// every read reaches storage by exact range.
fn large_parquet() -> LargeFile {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("large.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(20_000))
        .set_data_page_row_count_limit(5_000)
        .set_write_batch_size(5_000)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).expect("create Parquet"),
        Arc::clone(&schema),
        Some(properties),
    )
    .expect("Parquet writer");
    for group in 0..3 {
        let start = group * 20_000;
        let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(start..start + 20_000));
        let names: ArrayRef = Arc::new(StringArray::from(
            (start..start + 20_000)
                .map(|value| format!("name-{value:08}"))
                .collect::<Vec<_>>(),
        ));
        writer
            .write(&RecordBatch::try_new(Arc::clone(&schema), vec![ids, names]).expect("batch"))
            .expect("write row group");
    }
    writer.close().expect("close Parquet");
    let file_size = std::fs::metadata(&path).expect("metadata").len();
    assert!(file_size > novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES);
    let access = FsAccessResolver::new()
        .resolve_location(
            StorageAccessDomainId::from_bytes([2; 32]),
            path.to_string_lossy(),
            None,
        )
        .expect("resolve");
    let file = access
        .bind(
            0,
            FileIdentity::new(path.to_string_lossy(), file_size, None),
        )
        .expect("bind");
    LargeFile {
        _directory: directory,
        file,
        runtime: tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime"),
    }
}

fn request_through(
    file: &BoundFile,
    handle: &tokio::runtime::Handle,
    spawner: Arc<dyn FileTaskSpawner>,
    service: Option<(Arc<FileRangeService>, ConnectorSourceOperations)>,
) -> FileReadRequest {
    FileReadRequest {
        file: file.clone(),
        format: FileFormat::Parquet,
        range: FileReadRange::WholeFile,
        projection: FileProjection::All,
        budget: FileReadBudget {
            max_rows: NonZeroUsize::new(4096).unwrap(),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
        },
        predicates: Vec::new(),
        pruning: PhysicalPruning::default(),
        options: FileReaderOptions::default(),
        cache: None,
        prepared_input: None,
        context: FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(handle.clone())) as Arc<dyn FileIoRuntime>,
            task_spawner: spawner,
            range: service.map(|(service, operations)| {
                service.bind(
                    FileRangeScope::try_new(1, 0, 1, 1, 0, 1).unwrap(),
                    operations,
                )
            }),
        },
    }
}

fn service(
    spawner: Arc<dyn FileTaskSpawner>,
    handle: &tokio::runtime::Handle,
    window: usize,
) -> Arc<FileRangeService> {
    FileRangeService::new(
        NonZeroUsize::new(window).unwrap(),
        NonZeroUsize::new(window).unwrap(),
        NonZeroUsize::new(16).unwrap(),
        spawner,
        handle.clone(),
    )
}

async fn collect_awaited(reader: &mut AsyncFileBatchReader) -> FileResult<Vec<FileBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().await? {
        batches.push(batch);
    }
    Ok(batches)
}

fn rows(batches: &[FileBatch]) -> Vec<(i32, String, u64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        let names = batch
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let positions = batch.physical_row_positions.as_ref().expect("positions");
        for row in 0..batch.batch.num_rows() {
            rows.push((
                ids.value(row),
                names.value(row).to_string(),
                positions.value(row),
            ));
        }
    }
    rows
}

#[test]
fn awaited_reader_matches_the_blocking_reader_row_for_row() {
    // Page selection and a predicate over the small fixture.
    let fixture = Fixture::parquet();
    let selective = |request: &mut FileReadRequest| {
        request.predicates.push(ScanPredicate::new(
            "id",
            ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(2),
            },
            ScanPredicateSource::Static,
        ));
        request.pruning.row_groups = Some(vec![0]);
        request.pruning.pages.push(PhysicalPageSelection {
            row_group: 0,
            page_indices: vec![1],
        });
    };
    let mut blocking = fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1 << 20);
    selective(&mut blocking);
    let expected = collect(open_file_reader(blocking).expect("open").as_mut()).expect("read");
    let mut awaited = fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1 << 20);
    selective(&mut awaited);
    let actual = fixture.io.handle().block_on(async {
        let mut reader = open_file_reader_async(awaited, None).await.expect("open");
        collect_awaited(&mut reader).await.expect("read")
    });
    assert!(!expected.is_empty());
    assert_eq!(rows(&actual), rows(&expected));

    // Every row group of the large file, through the range service.
    let large = large_parquet();
    let handle = large.runtime.handle().clone();
    let spawner: Arc<dyn FileTaskSpawner> = Arc::new(CountingSpawner {
        handle: handle.clone(),
        spawned: AtomicUsize::new(0),
    });
    let blocking = request_through(
        &large.file,
        &handle,
        Arc::clone(&spawner),
        Some((
            service(Arc::clone(&spawner), &handle, 2),
            ConnectorSourceOperations::new(),
        )),
    );
    let expected = collect(open_file_reader(blocking).expect("open").as_mut()).expect("read");
    let awaited = request_through(
        &large.file,
        &handle,
        Arc::clone(&spawner),
        Some((
            service(Arc::clone(&spawner), &handle, 2),
            ConnectorSourceOperations::new(),
        )),
    );
    let actual = handle.block_on(async {
        let mut reader = open_file_reader_async(awaited, None).await.expect("open");
        collect_awaited(&mut reader).await.expect("read")
    });
    assert_eq!(rows(&expected).len(), 60_000);
    assert_eq!(rows(&actual), rows(&expected));
}

#[test]
fn awaited_footer_and_page_indexes_read_what_the_blocking_path_reads() {
    let large = large_parquet();
    let handle = large.runtime.handle().clone();
    let count_reads = |awaited: bool, page_indexes: bool| {
        let spawner = Arc::new(CountingSpawner {
            handle: handle.clone(),
            spawned: AtomicUsize::new(0),
        });
        let as_spawner: Arc<dyn FileTaskSpawner> = spawner.clone();
        let request = request_through(
            &large.file,
            &handle,
            Arc::clone(&as_spawner),
            Some((
                service(as_spawner, &handle, 2),
                ConnectorSourceOperations::new(),
            )),
        );
        let context = request.context.clone();
        let file = large.file.clone();
        let mut request = request;
        if page_indexes {
            request.pruning.row_groups = Some(vec![1]);
            request.pruning.pages.push(PhysicalPageSelection {
                row_group: 1,
                page_indices: vec![0],
            });
        }
        if awaited {
            handle.block_on(async {
                let inspection = inspect_parquet_metadata_async(file, None, context)
                    .await
                    .expect("inspect");
                let footer_reads = spawner.spawned.load(Ordering::SeqCst);
                if page_indexes {
                    let mut reader = open_file_reader_async(request, Some(&inspection))
                        .await
                        .expect("open");
                    assert!(reader.next_batch().await.expect("read").is_some());
                }
                (footer_reads, spawner.spawned.load(Ordering::SeqCst))
            })
        } else {
            let inspection = inspect_parquet_metadata(file, None, context).expect("inspect");
            let footer_reads = spawner.spawned.load(Ordering::SeqCst);
            if page_indexes {
                let mut reader = novarocks_fs::open_file_reader_with_parquet_inspection(
                    request,
                    Some(&inspection),
                )
                .expect("open");
                assert!(reader.next_batch().expect("read").is_some());
            }
            (footer_reads, spawner.spawned.load(Ordering::SeqCst))
        }
    };
    let (blocking_footer, _) = count_reads(false, false);
    let (awaited_footer, _) = count_reads(true, false);
    assert_eq!(blocking_footer, 2, "the 8-byte tail, then the metadata");
    assert_eq!(awaited_footer, blocking_footer);
    let (_, blocking_indexed) = count_reads(false, true);
    let (_, awaited_indexed) = count_reads(true, true);
    assert_eq!(awaited_indexed, blocking_indexed);
}

#[test]
fn a_decoder_request_s_ranges_run_concurrently_and_land_in_order() {
    let large = large_parquet();
    let handle = large.runtime.handle().clone();
    // The footer comes through an ungated path, so the gated spawner sees
    // only the decoder's two column chunks.
    let plain: Arc<dyn FileTaskSpawner> = Arc::new(CountingSpawner {
        handle: handle.clone(),
        spawned: AtomicUsize::new(0),
    });
    let inspection = inspect_parquet_metadata(
        large.file.clone(),
        None,
        request_through(&large.file, &handle, plain, None).context,
    )
    .expect("inspect");
    let gated = OrderedGateSpawner::new(handle.clone(), true);
    let as_spawner: Arc<dyn FileTaskSpawner> = gated.clone();
    let operations = ConnectorSourceOperations::new();
    let mut request = request_through(
        &large.file,
        &handle,
        Arc::clone(&as_spawner),
        Some((service(as_spawner, &handle, 2), operations.clone())),
    );
    request.pruning.row_groups = Some(vec![2]);
    request.options.coalesce_reads = false;

    let reading = handle.spawn(async move {
        let mut reader = open_file_reader_async(request, Some(&inspection))
            .await
            .expect("open");
        collect_awaited(&mut reader).await
    });
    handle.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while gated.started() < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("both column chunks are in flight together");
    });
    // The later chunk completes first; the decoder still gets each chunk
    // at its own range.
    gated.release(1);
    gated.release(0);
    let batches = handle
        .block_on(reading)
        .expect("reading task")
        .expect("read");
    let rows = rows(&batches);
    assert_eq!(rows.len(), 20_000);
    assert_eq!(rows[0], (40_000, "name-00040000".to_string(), 40_000));
    assert_eq!(rows[19_999], (59_999, "name-00059999".to_string(), 59_999));
    assert_eq!(operations.live_operations(), 0);
}

#[test]
fn a_failed_range_stops_its_sibling_and_the_error_wins() {
    let large = large_parquet();
    let handle = large.runtime.handle().clone();
    let plain: Arc<dyn FileTaskSpawner> = Arc::new(CountingSpawner {
        handle: handle.clone(),
        spawned: AtomicUsize::new(0),
    });
    let inspection = inspect_parquet_metadata(
        large.file.clone(),
        None,
        request_through(&large.file, &handle, plain, None).context,
    )
    .expect("inspect");
    let gated = OrderedGateSpawner::new(handle.clone(), true);
    gated.fail_unrun(0);
    let as_spawner: Arc<dyn FileTaskSpawner> = gated.clone();
    let operations = ConnectorSourceOperations::new();
    let mut request = request_through(
        &large.file,
        &handle,
        Arc::clone(&as_spawner),
        Some((service(as_spawner, &handle, 2), operations.clone())),
    );
    request.pruning.row_groups = Some(vec![0]);
    request.options.coalesce_reads = false;

    let error = handle.block_on(async {
        let mut reader = open_file_reader_async(request, Some(&inspection))
            .await
            .expect("open");
        collect_awaited(&mut reader)
            .await
            .expect_err("a chunk failed")
    });
    assert_eq!(error.kind(), FileErrorKind::Internal);
    assert_eq!(gated.started(), 2, "the sibling had been dispatched");
    // The held sibling was stopped; once its task runs it exits, and the
    // source has nothing left in flight.
    gated.release(1);
    handle.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while operations.live_operations() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the stopped sibling exits");
    });
}

#[test]
fn concurrent_ranges_of_a_small_file_share_one_whole_file_read() {
    let fixture = Fixture::parquet();
    let handle = fixture.io.handle();
    let spawner = Arc::new(CountingSpawner {
        handle: handle.clone(),
        spawned: AtomicUsize::new(0),
    });
    let as_spawner: Arc<dyn FileTaskSpawner> = spawner.clone();
    let mut request = request_through(
        &fixture.file,
        &handle,
        Arc::clone(&as_spawner),
        Some((
            service(as_spawner, &handle, 4),
            ConnectorSourceOperations::new(),
        )),
    );
    request.options.coalesce_reads = false;
    let context = request.context.clone();
    let rows = handle.block_on(async {
        let inspection = inspect_parquet_metadata_async(fixture.file.clone(), None, context)
            .await
            .expect("inspect");
        let mut reader = open_file_reader_async(request, Some(&inspection))
            .await
            .expect("open");
        collect_awaited(&mut reader).await.expect("read")
    });
    assert_eq!(
        rows.iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
    assert_eq!(
        spawner.spawned.load(Ordering::SeqCst),
        1,
        "footer and every column chunk come from one whole-file read"
    );
}
