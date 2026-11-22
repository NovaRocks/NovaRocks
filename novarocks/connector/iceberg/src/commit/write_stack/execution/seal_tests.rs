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
    IcebergCreationScope, IcebergDataBranchRecipe, IcebergEqualityDeleteColumnFacts,
    IcebergEqualityDeleteRecipe, IcebergWriteSessionId, IcebergWriteTableFacts,
    IcebergWriterOutput,
};
use crate::commit::write_stack::test_support::{
    data_branch_plan, equality_delete_recipe, writer_resources,
};
use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogVersion, ConnectorInstanceId, ConnectorProviderId, ConnectorReservationBackend,
    ConnectorResourceCheckpoint, ConnectorResourceLedger, ConnectorStopOwner,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
struct WriterOptions {
    partitioned: bool,
    output: Option<IcebergWriterOutput>,
    resources: Option<ConnectorExecutionResources>,
}

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
        self.writer_with_data_input(branch, None).await
    }
    async fn writer_with_data_input(
        &self,
        branch: IcebergWriteBranch,
        data_input: Option<(String, RecordBatch)>,
    ) -> (
        Box<dyn ConnectorBatchWriter>,
        RecordBatch,
        ConnectorStopOwner,
    ) {
        self.writer_with_options(branch, data_input, WriterOptions::default())
            .await
    }
    async fn writer_with_options(
        &self,
        branch: IcebergWriteBranch,
        data_input: Option<(String, RecordBatch)>,
        options: WriterOptions,
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
            2,
        )
        .unwrap();
        let format = IcebergFileFormat::Parquet;
        let output = options.output.unwrap_or_else(|| {
            IcebergWriterOutput::for_test(format, parquet::basic::Compression::SNAPPY)
        });
        let mut data = data_branch_plan();
        let mut equality = equality_delete_recipe();
        let data_batch = if let Some((schema, batch)) = data_input {
            data.recipe = IcebergDataBranchRecipe::try_new(
                Some(schema.clone()),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                false,
            )
            .unwrap();
            equality = IcebergEqualityDeleteRecipe::try_new(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        IcebergEqualityDeleteColumnFacts::try_new(
                            field.name().clone(),
                            index as i32 + 1,
                        )
                        .unwrap()
                    })
                    .collect(),
                schema,
            )
            .unwrap();
            Some(batch)
        } else {
            None
        };
        if options.partitioned {
            data.recipe = IcebergDataBranchRecipe::try_new(
                data.recipe.target_schema_json().map(str::to_owned),
                vec!["k1".into()],
                vec!["k1".into()],
                vec!["identity".into()],
                false,
            )
            .unwrap();
        }
        let (handle, batch) = match branch {
            IcebergWriteBranch::Data | IcebergWriteBranch::EqualityDelete => {
                let handle = if branch == IcebergWriteBranch::Data {
                    if let Some(batch) = data_batch.as_ref() {
                        data.input = novarocks_spi::connector::ConnectorWriteInputShape::Data {
                            fields: batch.schema().fields().iter().enumerate().map(|(ordinal, field)| {
                                let mut token = [0; 32];
                                token[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
                                novarocks_spi::connector::ConnectorWriteFieldBinding::try_new_typed(
                                    novarocks_spi::connector::ConnectorWriteFieldToken::from_bytes(token),
                                    field.as_ref().clone(),
                                    novarocks_type_contract::FunctionValueType::new(field.data_type().clone(), field.is_nullable()),
                                ).unwrap()
                            }).collect(),
                        };
                    }
                    let identity =
                        crate::commit::write_stack::domain::data_write_input_identity(&data.input)
                            .unwrap();
                    IcebergWriterHandle::try_new_data(
                        table,
                        output,
                        data.recipe.with_write_input_identity(identity),
                    )
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
                    IcebergWriterHandle::try_new_equality_delete(table, output, equality).unwrap()
                };
                let batch = data_batch.unwrap_or_else(|| {
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("k1", DataType::Int64, true)]));
                    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10, 20]))])
                        .unwrap()
                });
                (handle, batch)
            }
            _ => unreachable!("fixture uses data or equality-delete writers"),
        };
        let stop = ConnectorStopOwner::new();
        let writer = self
            .execution
            .open_writer(ConnectorOpenWriterRequest {
                resources: options
                    .resources
                    .or_else(|| (branch == IcebergWriteBranch::Data).then(writer_resources)),
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
async fn open_partition_limit_keeps_typed_counts_and_abort_cleans_unsealed_files() {
    use novarocks_failure_contract::connector_reason::ICEBERG_TOO_MANY_OPEN_PARTITIONS;

    let fixture = Fixture::new();
    let output = IcebergWriterOutput::try_new(
        IcebergFileFormat::Parquet,
        parquet::basic::Compression::SNAPPY,
        128 << 20,
        1 << 30,
        1,
    )
    .unwrap();
    let (mut writer, batch, _) = fixture
        .writer_with_options(
            IcebergWriteBranch::Data,
            None,
            WriterOptions {
                partitioned: true,
                output: Some(output),
                resources: None,
            },
        )
        .await;
    writer.append(batch.slice(0, 1)).await.unwrap();
    assert_eq!(fixture.files().len(), 1);
    let error = writer.append(batch.slice(1, 1)).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(
        error.message(),
        "Exceeded limit of 1 open writers for partitions: 2"
    );
    let fact = error.failure_fact();
    assert_eq!(fact.reason(), ICEBERG_TOO_MANY_OPEN_PARTITIONS);
    let resource = fact.resource().unwrap();
    assert_eq!(
        (resource.limit, resource.requested, resource.live),
        (Some(1), Some(2), Some(1))
    );
    assert_eq!(fixture.files().len(), 1);

    writer.abort().await.unwrap();
    assert!(fixture.files().is_empty());
}

#[derive(Clone)]
struct BufferLedger {
    live: Arc<AtomicU64>,
    limit: Arc<AtomicU64>,
}

impl BufferLedger {
    fn check_limit(&self, size: u64) -> Result<(), ConnectorError> {
        if size > self.limit.load(Ordering::Relaxed) {
            return Err(error(
                ConnectorErrorKind::ResourceExhausted,
                "writer memory denied",
            ));
        }
        Ok(())
    }
}

impl ConnectorResourceLedger for BufferLedger {
    fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
        Ok(ConnectorResourceCheckpoint::new(0))
    }

    fn try_reserve(
        &self,
        class: ConnectorResourceClass,
        bytes: u64,
    ) -> Result<Box<dyn ConnectorReservationBackend>, ConnectorError> {
        assert_eq!(class, ConnectorResourceClass::WriterState);
        self.check_limit(bytes)?;
        assert_eq!(self.live.swap(bytes, Ordering::Relaxed), 0);
        Ok(Box::new(BufferReservation {
            ledger: self.clone(),
            bytes,
        }))
    }
}

struct BufferReservation {
    ledger: BufferLedger,
    bytes: u64,
}

impl ConnectorReservationBackend for BufferReservation {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
        let next = self.bytes + additional;
        self.ledger.check_limit(next)?;
        self.bytes = next;
        self.ledger.live.store(next, Ordering::Relaxed);
        Ok(())
    }

    fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
        assert!(bytes <= self.bytes);
        self.bytes = bytes;
        self.ledger.live.store(bytes, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for BufferReservation {
    fn drop(&mut self) {
        assert_eq!(self.ledger.live.swap(0, Ordering::Relaxed), self.bytes);
    }
}

#[tokio::test]
async fn data_writer_holds_only_live_parquet_buffer_charge_and_preserves_refusal() {
    let fixture = Fixture::new();
    let ledger = BufferLedger {
        live: Arc::new(AtomicU64::new(0)),
        limit: Arc::new(AtomicU64::new(u64::MAX)),
    };
    let options = || WriterOptions {
        resources: Some(ConnectorExecutionResources::from_admitted_ledger(Arc::new(
            ledger.clone(),
        ))),
        ..WriterOptions::default()
    };
    let (mut writer, batch, _) = fixture
        .writer_with_options(IcebergWriteBranch::Data, None, options())
        .await;
    assert_eq!(ledger.live.load(Ordering::Relaxed), 0);
    writer.append(batch.clone()).await.unwrap();
    let held = ledger.live.load(Ordering::Relaxed);
    assert!(held > 0);
    ledger.limit.store(held, Ordering::Relaxed);
    let growth = RecordBatch::try_new(
        batch.schema(),
        vec![Arc::new(Int64Array::from_iter_values(100..300))],
    )
    .unwrap();
    let refusal = writer.append(growth).await.unwrap_err();
    assert_eq!(refusal.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(refusal.message(), "writer memory denied");
    assert_eq!(ledger.live.load(Ordering::Relaxed), held);
    writer.abort().await.unwrap();
    assert!(fixture.files().is_empty());
    assert_eq!(ledger.live.load(Ordering::Relaxed), 0);

    let (mut writer, batch, _) = fixture
        .writer_with_options(IcebergWriteBranch::Data, None, options())
        .await;
    writer.append(batch).await.unwrap();
    assert_eq!(ledger.live.load(Ordering::Relaxed), held);
    assert_eq!(writer.finish().await.unwrap().len(), 1);
    assert_eq!(ledger.live.load(Ordering::Relaxed), 0);
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
async fn writer_append_preserves_width_data_failures_and_internal_contract_failures_before_staging()
{
    for branch in [IcebergWriteBranch::Data, IcebergWriteBranch::EqualityDelete] {
        for (case, required, expected_kind, expected_class) in [
            (
                "width",
                false,
                ConnectorErrorKind::InvalidRequest,
                FailureClass::Data,
            ),
            (
                "required",
                true,
                ConnectorErrorKind::Internal,
                FailureClass::Unexpected,
            ),
            (
                "field-name",
                false,
                ConnectorErrorKind::Internal,
                FailureClass::Unexpected,
            ),
            (
                "column-count",
                false,
                ConnectorErrorKind::Internal,
                FailureClass::Unexpected,
            ),
        ] {
            let fixture = Fixture::new();
            let target = serde_json::json!({"type":"struct","schema-id":1,"fields":[{"id":1,"name":"raw","required":required,"type":"fixed[4]"}]}).to_string();
            let source_schema =
                Arc::new(Schema::new(vec![Field::new("raw", DataType::Binary, true)]));
            let valid = RecordBatch::try_new(
                source_schema.clone(),
                vec![Arc::new(BinaryArray::from(vec![Some(&b"abcd"[..])]))],
            )
            .unwrap();
            let (mut writer, _, _stop) = fixture
                .writer_with_data_input(branch, Some((target, valid)))
                .await;
            let batch = match case {
                "width" => RecordBatch::try_new(
                    source_schema.clone(),
                    vec![Arc::new(BinaryArray::from(vec![Some(&b"x"[..])]))],
                )
                .unwrap(),
                "required" => RecordBatch::try_new(
                    source_schema.clone(),
                    vec![Arc::new(BinaryArray::from(vec![None::<&[u8]>]))],
                )
                .unwrap(),
                "field-name" => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "other",
                        DataType::Binary,
                        true,
                    )])),
                    vec![Arc::new(BinaryArray::from(vec![Some(&b"abcd"[..])]))],
                )
                .unwrap(),
                "column-count" => RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
                )
                .unwrap(),
                _ => unreachable!(),
            };
            let failure = writer.append(batch).await.unwrap_err();
            assert_eq!(failure.kind(), expected_kind, "{case}: {failure}");
            assert_eq!(
                failure.failure_fact().class(),
                expected_class,
                "{case}: {failure}"
            );
            assert!(
                fixture.files().is_empty(),
                "{case} staged bytes before rejecting the value"
            );
            writer.abort().await.unwrap();
            assert!(fixture.files().is_empty());
        }
    }
}
