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

//! Actual provider writers, real local files, and the local seal cut.

use super::*;
use crate::commit::write_stack::domain::{IcebergWriteTableFacts, IcebergWriterOutput};
use crate::commit::write_stack::test_support::{
    data_branch_plan, equality_delete_recipe, merge_target,
};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogVersion, ConnectorInstanceId, ConnectorProviderId, ConnectorStopOwner,
};
use std::path::{Path, PathBuf};
use std::time::Duration;

struct Fixture {
    directory: tempfile::TempDir,
    execution: IcebergWriteStackExecution,
    adapter: IcebergWriteAdapter,
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("local-seal").unwrap(),
        };
        let catalog = CatalogHandle::new(
            descriptor.instance_id.clone(),
            CatalogVersion::from_bytes([7; 32]),
        );
        let adapter =
            crate::commit::write_stack::runtime::build_write_adapter(descriptor, catalog.clone());
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
        );
        let execution = IcebergWriteStackExecution::new(catalog, adapter.clone(), binding);
        Self {
            directory,
            execution,
            adapter,
        }
    }
    async fn writer(
        &self,
        branch: IcebergWriteBranch,
    ) -> (
        Box<dyn ConnectorBatchWriter>,
        RecordBatch,
        ConnectorStopOwner,
    ) {
        self.writer_with_late_failure(branch, false).await
    }
    async fn writer_with_late_failure(
        &self,
        branch: IcebergWriteBranch,
        unreadable_second: bool,
    ) -> (
        Box<dyn ConnectorBatchWriter>,
        RecordBatch,
        ConnectorStopOwner,
    ) {
        let root = format!("file://{}", self.directory.path().display());
        let table = IcebergWriteTableFacts::try_new(
            "6f7c3a0e-0000-4000-8000-000000000001".into(),
            "db".into(),
            "t".into(),
            root.clone(),
            format!("{root}/data"),
            "main".into(),
            Some(77),
            4,
            1,
            0,
            if branch == IcebergWriteBranch::DeletionVector {
                3
            } else {
                2
            },
        )
        .unwrap();
        let format = if branch == IcebergWriteBranch::DeletionVector {
            IcebergFileFormat::Puffin
        } else {
            IcebergFileFormat::Parquet
        };
        let output =
            IcebergWriterOutput::try_new(format, parquet::basic::Compression::SNAPPY, None)
                .unwrap();
        let data = data_branch_plan();
        let (handle, batch) = match branch {
            IcebergWriteBranch::Data | IcebergWriteBranch::EqualityDelete => {
                let handle = if branch == IcebergWriteBranch::Data {
                    IcebergWriterHandle::try_new_data(table, output, data.recipe).unwrap()
                } else {
                    IcebergWriterHandle::try_new_equality_delete(
                        table,
                        output,
                        equality_delete_recipe(),
                    )
                    .unwrap()
                };
                let schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Int64, true)]));
                (
                    handle,
                    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))])
                        .unwrap(),
                )
            }
            IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => {
                let source = format!("{root}/a-source.parquet");
                let mut sources = vec![source.clone()];
                let mut targets = vec![merge_target(&source, 2, Vec::new())];
                if unreadable_second {
                    let second = format!("{root}/z-source.parquet");
                    let missing = crate::commit::write_stack::test_support::parquet_ref(
                        &format!("{root}/missing-old-delete.parquet"),
                        Some(&second),
                        1,
                        1,
                    )
                    .unwrap();
                    targets.push(merge_target(&second, 2, vec![missing]));
                    sources.push(second);
                }
                let positions = vec![0; sources.len()];
                let handle =
                    IcebergWriterHandle::try_new_delete(branch, table, output, targets).unwrap();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("_file", DataType::Utf8, false),
                    Field::new("_pos", DataType::Int64, false),
                ]));
                (
                    handle,
                    RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(StringArray::from(sources)),
                            Arc::new(Int64Array::from(positions)),
                        ],
                    )
                    .unwrap(),
                )
            }
        };
        let stop = ConnectorStopOwner::new();
        let writer = self
            .execution
            .open_writer(ConnectorOpenWriterRequest {
                handle: self.adapter.wrap_writer_handle(handle),
                target: WriteTargetOrdinal::try_new(0).unwrap(),
                expected_schema: batch.schema(),
                physical: ConnectorWriterPhysicalContext::new([1; 16], 1, [2; 16], 0, 0),
                context: ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(60),
                    stop.view(),
                    64 * 1024,
                    1024 * 1024,
                )
                .unwrap(),
            })
            .await
            .unwrap();
        (writer, batch, stop)
    }
    fn files(&self) -> Vec<PathBuf> {
        fn visit(root: &Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(root).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, out);
                } else {
                    out.push(path);
                }
            }
        }
        let mut result = Vec::new();
        visit(self.directory.path(), &mut result);
        result.sort();
        result
    }
}

async fn check_sealed(branch: IcebergWriteBranch) {
    let fixture = Fixture::new();
    let (mut writer, batch, stop) = fixture.writer(branch).await;
    writer.append(batch).await.unwrap();
    let fragments = writer.finish().await.unwrap();
    assert_eq!(fragments.len(), 1);
    let path = PathBuf::from(
        fixture
            .adapter
            .commit_fragment(&fragments[0])
            .unwrap()
            .path()
            .trim_start_matches("file://"),
    );
    let bytes = std::fs::read(&path).unwrap();
    assert!(!bytes.is_empty());
    stop.request_stop();
    writer.abort().await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "sealed output is outside the old writer's delete authority"
    );
    assert!(
        writer.finish().await.is_err(),
        "a sealed writer cannot finish twice"
    );
    drop(writer);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "writer retirement cannot revoke the output"
    );
    drop(fragments);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "lost transport/FE receipt cannot grant local deletion"
    );
}
#[tokio::test]
async fn sealed_data_survives_old_abort_drop_and_lost_receipt() {
    check_sealed(IcebergWriteBranch::Data).await;
}
#[tokio::test]
async fn sealed_equality_delete_survives_old_abort_drop_and_lost_receipt() {
    check_sealed(IcebergWriteBranch::EqualityDelete).await;
}
#[tokio::test]
async fn sealed_position_delete_survives_old_abort_drop_and_lost_receipt() {
    check_sealed(IcebergWriteBranch::PositionDelete).await;
}
#[tokio::test]
async fn sealed_deletion_vector_survives_old_abort_drop_and_lost_receipt() {
    check_sealed(IcebergWriteBranch::DeletionVector).await;
}
#[tokio::test]
async fn unsealed_data_and_equality_delete_still_clean_their_own_objects() {
    for branch in [IcebergWriteBranch::Data, IcebergWriteBranch::EqualityDelete] {
        let fixture = Fixture::new();
        let (mut writer, batch, stop) = fixture.writer(branch).await;
        writer.append(batch).await.unwrap();
        assert!(!fixture.files().is_empty());
        stop.request_stop();
        writer.abort().await.unwrap();
        assert!(fixture.files().is_empty());
        assert!(writer.finish().await.is_err());
        writer.abort().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_unsealed_finish_keeps_cleanup_of_its_already_written_prefix() {
    for branch in [
        IcebergWriteBranch::PositionDelete,
        IcebergWriteBranch::DeletionVector,
    ] {
        let fixture = Fixture::new();
        let (mut writer, batch, stop) = fixture.writer_with_late_failure(branch, true).await;
        writer.append(batch).await.unwrap();
        let failure = writer
            .finish()
            .await
            .expect_err("the second target's actual missing old delete must fail");
        assert_eq!(
            failure.kind(),
            ConnectorErrorKind::NotFound,
            "actual missing storage object"
        );
        assert_eq!(
            fixture.files().len(),
            1,
            "the first target already staged a real output before failure"
        );
        stop.request_stop();
        writer.abort().await.unwrap();
        assert!(
            fixture.files().is_empty(),
            "failed finish never revokes the original cleanup authority"
        );
        writer.abort().await.unwrap();
    }
}
