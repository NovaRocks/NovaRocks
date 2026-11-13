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
use crate::commit::write_stack::domain::{
    IcebergCreationScope, IcebergWriteSessionId, IcebergWriteTableFacts, IcebergWriterOutput,
};
use crate::commit::write_stack::test_support::{
    data_branch_plan, equality_delete_recipe, position_rewrite_target, writer_resources,
};
use arrow::array::{Int64Array, StringArray};
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
            novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
            Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime)),
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
                    IcebergWriterHandle::try_new_data(table, output, data.recipe)
                        .unwrap()
                        .with_creation_scope(
                            IcebergCreationScope::try_new(
                                IcebergWriteSessionId::new(),
                                [1; 16],
                                [2; 32],
                            )
                            .unwrap(),
                        )
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
            IcebergWriteBranch::DeletionVector => {
                let source = format!("{root}/a-source.parquet");
                let handle = IcebergWriterHandle::try_new_delete(branch, table, output)
                    .unwrap()
                    .try_with_position_rewrite(position_rewrite_target(&source, 2))
                    .unwrap()
                    .with_creation_scope(
                        IcebergCreationScope::try_new(
                            IcebergWriteSessionId::new(),
                            [1; 16],
                            [2; 32],
                        )
                        .unwrap(),
                    );
                let schema = Arc::new(Schema::new(vec![
                    Field::new(
                        crate::position_delete::FILE_PATH_COLUMN,
                        DataType::Utf8,
                        false,
                    ),
                    Field::new(crate::position_delete::POS_COLUMN, DataType::Int64, false),
                ]));
                (
                    handle,
                    RecordBatch::try_new(
                        schema,
                        vec![
                            Arc::new(StringArray::from(vec![
                                source.clone(),
                                source.clone(),
                                source,
                            ])),
                            Arc::new(Int64Array::from(vec![0, 0, 1])),
                        ],
                    )
                    .unwrap(),
                )
            }
            IcebergWriteBranch::PositionDelete => unreachable!("maintenance requires v3"),
        };
        let stop = ConnectorStopOwner::new();
        let writer = self
            .execution
            .open_writer(ConnectorOpenWriterRequest {
                resources: (branch == IcebergWriteBranch::DeletionVector).then(writer_resources),
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
async fn sealed_maintenance_vector_survives_old_abort_drop_and_lost_receipt() {
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

#[tokio::test]
async fn rejected_append_keeps_the_open_data_stream_abortable() {
    let fixture = Fixture::new();
    let (mut writer, batch, _) = fixture.writer(IcebergWriteBranch::Data).await;
    writer.append(batch).await.unwrap();
    let original = fixture.files();
    assert_eq!(
        original.len(),
        1,
        "real conditional writer owns one private object before close"
    );
    let invalid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k1", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec!["invalid"]))],
    )
    .unwrap();
    assert!(writer.append(invalid).await.is_err());
    assert_eq!(
        fixture.files(),
        original,
        "rejected input does not replace the original open stream"
    );
    writer.abort().await.unwrap();
    assert!(fixture.files().is_empty());
    writer.abort().await.unwrap();
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn failed_original_stream_abort_remains_a_failure_on_reentry() {
    let fixture = Fixture::new();
    let (mut writer, batch, stop) = fixture.writer(IcebergWriteBranch::Data).await;
    writer.append(batch).await.unwrap();
    let [private]: [PathBuf; 1] = fixture.files().try_into().unwrap();
    // Fail the actual conditional local writer's unlink. The original abort
    // must be awaited, and neither another caller nor a terminal flag can
    // replace its retained raw exit failure with successful cleanup.
    std::fs::remove_file(&private).unwrap();
    std::fs::create_dir(&private).unwrap();
    stop.request_stop();
    assert!(writer.abort().await.is_err());
    assert!(private.is_dir());
    std::fs::remove_dir(&private).unwrap();
    assert!(
        writer.abort().await.is_err(),
        "repairing the path cannot invent the lost original abort receipt"
    );
    assert!(writer.finish().await.is_err());
}

#[tokio::test]
async fn maintenance_vector_rejects_foreign_and_out_of_domain_rows_before_staging() {
    let fixture = Fixture::new();
    for (foreign, position) in [(true, 0), (false, 2), (false, -1)] {
        let (mut writer, original, _) = fixture.writer(IcebergWriteBranch::DeletionVector).await;
        let names = original
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let path = if foreign {
            "file:///foreign/data.parquet"
        } else {
            names.value(0)
        };
        let batch = RecordBatch::try_new(
            original.schema(),
            vec![
                Arc::new(StringArray::from(vec![path])),
                Arc::new(Int64Array::from(vec![position])),
            ],
        )
        .unwrap();
        assert_eq!(
            writer.append(batch).await.unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        writer.abort().await.unwrap();
        assert!(fixture.files().is_empty());
    }
}

#[tokio::test]
async fn maintenance_vector_repacks_full_coverage_with_an_owned_puffin() {
    let fixture = Fixture::new();
    let (mut writer, batch, _) = fixture.writer(IcebergWriteBranch::DeletionVector).await;
    writer.append(batch).await.unwrap();
    let fragments = writer.finish().await.unwrap();
    let [fragment] = fragments.as_slice() else {
        panic!("one selected file must produce one vector");
    };
    let fragment = fixture.adapter.commit_fragment(fragment).unwrap();
    let crate::commit::write_stack::IcebergCommitArtifact::DeletionVector(vector) =
        fragment.artifact()
    else {
        panic!("maintenance must stage a replacement vector even at full coverage");
    };
    assert_eq!(vector.cardinality(), 2);
    assert!(vector.merged_old_references().is_empty());
    assert!(fragment.creation().is_some());
    let [path]: [_; 1] = fixture.files().try_into().unwrap();
    assert!(std::fs::read(path).unwrap().starts_with(b"PFA1"));
}
