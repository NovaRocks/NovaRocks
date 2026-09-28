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

#![allow(dead_code)]

use std::fs::File;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    BoundFile, FileBatch, FileBatchReader, FileBytesFuture, FileCancellation, FileFormat,
    FileIdentity, FileIoRuntime, FileProjection, FileReadBudget, FileReadContext, FileReadRange,
    FileReadRequest, FileResult, FileTask, FileTaskFuture, FileTaskSpawner, FileU64Future,
    FsAccessResolver, PhysicalPruning,
};
use novarocks_spi::connector::StorageAccessDomainId;
use orc_rust::ArrowWriterBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

pub struct TestIo {
    runtime: tokio::runtime::Runtime,
    block_on_bytes_calls: AtomicUsize,
}

impl TestIo {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("test runtime"),
            block_on_bytes_calls: AtomicUsize::new(0),
        })
    }

    pub fn block_on_bytes_calls(&self) -> usize {
        self.block_on_bytes_calls.load(Ordering::Relaxed)
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }
}

impl FileIoRuntime for TestIo {
    fn block_on_bytes(&self, future: FileBytesFuture) -> FileResult<bytes::Bytes> {
        self.block_on_bytes_calls.fetch_add(1, Ordering::Relaxed);
        self.runtime.block_on(future)
    }

    fn block_on_u64(&self, future: FileU64Future) -> FileResult<u64> {
        self.runtime.block_on(future)
    }
}

impl FileTaskSpawner for TestIo {
    fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
        Ok(FileTask::new(self.runtime.spawn(task)))
    }

    fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.runtime.spawn_blocking(move || job());
    }
}

pub struct Fixture {
    pub directory: TempDir,
    pub file: BoundFile,
    pub io: Arc<TestIo>,
}

impl Fixture {
    pub fn parquet() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("sample.parquet");
        write_parquet(&path);
        Self::from_path(directory, path)
    }

    pub fn parquet_three_groups() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("three-groups.parquet");
        write_parquet_groups(&path, 3);
        Self::from_path(directory, path)
    }

    pub fn orc() -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("sample.orc");
        write_orc(&path);
        Self::from_path(directory, path)
    }

    fn from_path(directory: TempDir, path: std::path::PathBuf) -> Self {
        let file_size = std::fs::metadata(&path).expect("file metadata").len();
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([1; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("resolve fixture");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), file_size, Some(7)),
            )
            .expect("bind fixture");
        Self {
            directory,
            file,
            io: TestIo::new(),
        }
    }

    pub fn request(
        &self,
        format: FileFormat,
        projection: FileProjection,
        max_rows: usize,
        max_bytes: usize,
    ) -> FileReadRequest {
        let runtime: Arc<dyn FileIoRuntime> = self.io.clone();
        let task_spawner: Arc<dyn FileTaskSpawner> = self.io.clone();
        FileReadRequest {
            file: self.file.clone(),
            format,
            range: FileReadRange::WholeFile,
            projection,
            budget: FileReadBudget {
                max_rows: NonZeroUsize::new(max_rows).expect("positive rows"),
                max_bytes: NonZeroUsize::new(max_bytes).expect("positive bytes"),
            },
            predicates: Vec::new(),
            pruning: PhysicalPruning::default(),
            options: Default::default(),
            cache: None,
            prepared_input: None,
            context: FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime,
                task_spawner,
                range: None,
            },
        }
    }
}

pub fn collect(reader: &mut dyn FileBatchReader) -> FileResult<Vec<FileBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        batches.push(batch);
    }
    Ok(batches)
}

fn write_parquet(path: &std::path::Path) {
    write_parquet_groups(path, 2);
}

fn write_parquet_groups(path: &std::path::Path, groups: usize) {
    let schema = fixture_schema();
    let file = File::create(path).expect("create Parquet");
    let properties = WriterProperties::builder()
        .set_data_page_row_count_limit(2)
        .set_write_batch_size(2)
        .build();
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties)).expect("Parquet writer");
    for group in 0..groups {
        writer
            .write(&fixture_batch(&schema, (group * 4) as i32))
            .expect("write row group");
        if group + 1 < groups {
            writer.flush().expect("flush row group");
        }
    }
    writer.close().expect("close Parquet");
}

fn write_orc(path: &std::path::Path) {
    let schema = fixture_schema();
    let file = File::create(path).expect("create ORC");
    let mut writer = ArrowWriterBuilder::new(file, Arc::clone(&schema))
        .try_build()
        .expect("ORC writer");
    writer.write(&fixture_batch(&schema, 0)).expect("write ORC");
    writer.write(&fixture_batch(&schema, 4)).expect("write ORC");
    writer.close().expect("close ORC");
}

fn fixture_schema() -> Arc<Schema> {
    let id = Field::new("id", DataType::Int32, false).with_metadata(
        [(PARQUET_FIELD_ID_META_KEY.to_string(), "10".to_string())]
            .into_iter()
            .collect(),
    );
    let name = Field::new("name", DataType::Utf8, false).with_metadata(
        [(PARQUET_FIELD_ID_META_KEY.to_string(), "20".to_string())]
            .into_iter()
            .collect(),
    );
    Arc::new(Schema::new(vec![id, name]))
}

fn fixture_batch(schema: &Arc<Schema>, start: i32) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(start..start + 4));
    let names: ArrayRef = Arc::new(StringArray::from(
        (start..start + 4)
            .map(|value| format!("name-{value}"))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(Arc::clone(schema), vec![ids, names]).expect("fixture batch")
}
