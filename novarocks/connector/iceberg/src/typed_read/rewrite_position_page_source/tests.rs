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

use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Array;
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FsAccessResolver, TokioFileIoRuntime,
    TokioFileIoSpawner,
};
use novarocks_spi::connector::read_stack::{ConnectorSourceOperations, SplitWeight};

use crate::commit::DeletionVector;
use crate::iceberg::spec::{NestedField, Type};
use crate::typed_read::split::{
    IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
};
use crate::typed_read::table_execute::IcebergRewritePositionDeleteFilesSplitParams;
use crate::typed_read::test_spawners::{GatedFileIoSpawner, route_reads_through};
use crate::typed_read::test_streams::DrivenStream;

use super::*;

struct RewriteFixture {
    runtime: tokio::runtime::Runtime,
    _directory: tempfile::TempDir,
    data_file_path: String,
    split: IcebergRewritePositionDeleteFilesSplit,
    columns: Vec<IcebergColumnHandle>,
    binding: IcebergReadBinding,
    context: FileReadContext,
}

impl RewriteFixture {
    fn request(&self) -> IcebergRewritePositionDeleteFilesPageSourceRequest<'_> {
        IcebergRewritePositionDeleteFilesPageSourceRequest {
            split: &self.split,
            columns: &self.columns,
            access_binding: self.binding.clone(),
            context: self.context.clone(),
        }
    }

    /// Routes the fixture's reads through a range service whose file
    /// tasks wait for [`GatedFileIoSpawner::release`]; returns the task
    /// source's operations.
    fn gate_reads(&mut self) -> (Arc<GatedFileIoSpawner>, ConnectorSourceOperations) {
        let spawner = GatedFileIoSpawner::closed(self.runtime.handle().clone());
        let operations = route_reads_through(
            &mut self.context,
            spawner.clone(),
            self.runtime.handle().clone(),
        );
        (spawner, operations)
    }
}

/// Two selected deletion vectors of one data file, in one Puffin file:
/// positions {1, 4} and {4, 9}.
fn rewrite_fixture() -> RewriteFixture {
    let directory = tempfile::tempdir().expect("temporary directory");
    let delete_path = directory.path().join("deletes.puffin");
    let data_file_path = directory
        .path()
        .join("data.parquet")
        .to_string_lossy()
        .to_string();

    let mut first = DeletionVector::new();
    first.insert(1).expect("position");
    first.insert(4).expect("position");
    let mut second = DeletionVector::new();
    second.insert(4).expect("position");
    second.insert(9).expect("position");
    let first_payload = first.to_iceberg_payload().expect("first payload");
    let second_payload = second.to_iceberg_payload().expect("second payload");
    let mut payloads = first_payload.clone();
    payloads.extend_from_slice(&second_payload);
    fs::write(&delete_path, &payloads).expect("write deletion vectors");

    let delete_path = delete_path.to_string_lossy().to_string();
    let delete = |offset: usize, payload: &[u8]| {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            partition_spec_id: 0,
            partition_data_json: r#"{"version":1,"values":[]}"#.to_string(),
            content: IcebergDeleteFileContent::PositionDeletes,
            path: delete_path.clone(),
            format: IcebergFileFormat::Puffin,
            record_count: 2,
            file_size_in_bytes: payloads.len() as i64,
            equality_field_ids: Vec::new(),
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number: 1,
            content_offset: Some(offset as i64),
            content_size_in_bytes: Some(payload.len() as i64),
            referenced_data_file: Some(data_file_path.clone()),
            decryption_data: None,
        })
        .expect("selected deletion vector")
    };
    let split = IcebergRewritePositionDeleteFilesSplit::try_new(
        IcebergRewritePositionDeleteFilesSplitParams {
            data_file_path: data_file_path.clone(),
            data_file_size: 0,
            partition_spec_id: 0,
            partition_data_json: r#"{"version":1,"values":[]}"#.to_string(),
            selected_position_deletes: vec![
                delete(0, &first_payload),
                delete(first_payload.len(), &second_payload),
            ],
            split_weight: SplitWeight::STANDARD,
        },
    )
    .expect("rewrite split");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let io_spawner: Arc<dyn FileIoSpawner> =
        Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::clone(&file_runtime),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
        Arc::clone(&io_spawner),
    );
    let context = FileReadContext {
        turn: None,
        cancellation: FileCancellation::new(),
        deadline: Some(Instant::now() + Duration::from_secs(10)),
        runtime: file_runtime,
        io_spawner,
        range: None,
    };
    let columns = REWRITE_POSITION_DELETE_OUTPUT_COLUMNS
        .map(|(name, metadata)| {
            IcebergColumnHandle::base_column(&NestedField::required(
                metadata.field_id(),
                name,
                Type::Primitive(metadata.declared_type()),
            ))
            .expect("canonical output handle")
        })
        .to_vec();
    RewriteFixture {
        runtime,
        _directory: directory,
        data_file_path,
        split,
        columns,
        binding,
        context,
    }
}

/// A page's `(file_path, pos)` rows.
fn rows_of(page: SourcePage) -> Vec<(String, i64)> {
    let (rows, columns) = page.into_columns().expect("materialize page");
    let paths = columns[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("file_path string column");
    let positions = columns[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("pos bigint column");
    (0..rows)
        .map(|row| (paths.value(row).to_string(), positions.value(row)))
        .collect()
}

#[test]
fn a_rewrite_stream_reads_selected_vectors_and_emits_canonical_output_columns() {
    let fixture = rewrite_fixture();
    let budget = ConnectorPollBudget::new();
    let stream =
        create_iceberg_rewrite_position_delete_files_page_stream(fixture.request(), &budget)
            .expect("page stream");
    let mut source = DrivenStream::new(&fixture.runtime, stream, budget);
    let page = source
        .next_page()
        .expect("read page")
        .expect("one output page");
    let path = fixture.data_file_path.clone();
    assert_eq!(
        rows_of(page),
        vec![(path.clone(), 1), (path.clone(), 4), (path, 9)]
    );
    assert!(source.next_page().expect("finish the stream").is_none());
    assert!(source.is_finished());
    source.close().expect("close the drained stream");
}

#[test]
fn a_rewrite_stream_reads_nothing_until_polled_and_then_emits_the_merged_positions() {
    use futures::StreamExt;

    let mut fixture = rewrite_fixture();
    let (spawner, _task_operations) = fixture.gate_reads();
    let budget = ConnectorPollBudget::new();
    let mut stream =
        create_iceberg_rewrite_position_delete_files_page_stream(fixture.request(), &budget)
            .expect("page stream");
    assert_eq!(spawner.started(), 0, "creating the stream reads nothing");

    spawner.release(64);
    // Three positions and their paths fit one complete engine output prefix.
    budget.refill(1);
    let rows = fixture.runtime.block_on(async {
        let page = stream
            .next()
            .await
            .expect("one output prefix")
            .expect("page");
        assert_eq!(page.position_count(), 3);
        assert!(stream.next().await.is_none());
        rows_of(page)
    });
    assert_eq!(spawner.started(), 2, "one read per selected vector");
    let path = fixture.data_file_path.clone();
    assert_eq!(rows, vec![(path.clone(), 1), (path.clone(), 4), (path, 9)]);
    assert_eq!(budget.remaining(), 0);
    assert_eq!(budget.exhaustions(), 0);
    fixture
        .runtime
        .block_on(stream.close())
        .expect("closing a drained stream");
}

#[test]
fn closing_a_loading_rewrite_stream_stops_its_read_and_waits_for_its_exit() {
    use futures::StreamExt;

    let mut fixture = rewrite_fixture();
    let (spawner, task_operations) = fixture.gate_reads();
    let budget = ConnectorPollBudget::new();
    budget.refill(1024);
    let mut stream =
        create_iceberg_rewrite_position_delete_files_page_stream(fixture.request(), &budget)
            .expect("page stream");
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while spawner.started() == 0 {
                assert!(futures::poll!(stream.next()).is_pending());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the first vector read is in flight");
    });
    assert!(task_operations.live_operations() > 0);

    // Closed the way a driver closes it: inside the runtime's context.
    let mut closed = {
        let _context = fixture.runtime.enter();
        stream.close()
    };
    fixture.runtime.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut closed)
                .await
                .is_err(),
            "the stream has not exited while its read is held"
        );
    });
    assert!(
        !task_operations.is_sealed(),
        "closing one stream leaves its task source open"
    );
    spawner.release(64);
    fixture
        .runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), closed).await })
        .expect("the stream exits once its read does")
        .expect("a stopped read exits cleanly");
    assert_eq!(task_operations.live_operations(), 0);
    assert_eq!(spawner.started(), 1, "the stopped load reads nothing more");
}
