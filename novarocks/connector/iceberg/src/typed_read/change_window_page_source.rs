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

//! The reader behind one change-window split.
//!
//! A change window is the **set difference of the visible rows at its two
//! endpoints**, never a replay of what happened between them. The enumeration
//! in `split_source` already guarantees that: a row written and deleted inside
//! the window is invisible at both endpoints, so no split names it. This module
//! must not reintroduce it, which is why nothing here ever reads a data file's
//! rows without also subtracting what was already invisible at the endpoint the
//! rows are being compared against.
//!
//! Two things separate this reader from the data reader it delegates to:
//!
//! * `__change_op` is derived from the split variant and produced here as an
//!   Arrow `Int8`. It is never read from a file, and it is never widened -- the
//!   engine contract in `novarocks-execution` is eight-bit, while the Iceberg
//!   column handle has to declare `int` because the table format has no
//!   eight-bit integer;
//! * the reverse side *selects* the rows a delete removed instead of hiding
//!   them by comparing both complete endpoint closures. Added and removed
//!   files use ordinary exclusion at their independently pinned endpoint.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int8Array};
use futures::Stream;
use futures::future::BoxFuture;
use novarocks_fs::{FileReadBudget, FileReadContext, FileReaderOptions};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPageStream, ConnectorPollBudget, OwnedConnectorPageStream, PageSourceMetrics,
    SourcePage,
};

use crate::access_binding::IcebergReadBinding;

use super::change_window::{
    ICEBERG_CHANGE_OP_FIELD_ID, IcebergChangeSplit, IcebergChangeWindowHandle,
    change_op_column_handle,
};
use super::column_handle::{IcebergColumnHandle, invalid, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, EndpointDeleteClosure};
use super::page_source::{
    IcebergDynamicFilter, IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache,
    create_iceberg_page_stream,
};

/// Everything the change-window reader needs, all of it frozen or
/// process-local.
pub struct IcebergChangeWindowPageSourceRequest<'a> {
    pub handle: &'a IcebergChangeWindowHandle,
    pub split: &'a IcebergChangeSplit,
    /// The scan's ordered output columns; `__change_op` may appear among them.
    pub columns: &'a [IcebergColumnHandle],
    pub delete_manager: Arc<DeleteManager>,
    pub footers: Arc<ParquetFooterCache>,
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
    pub cache: Option<novarocks_fs::DataCacheContext>,
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    pub scheduled_split_sequence_id: u64,
    pub dynamic_filter: Arc<IcebergDynamicFilter>,
}

/// Build the page stream for one change-window split, polled with `budget`:
/// the stream of its data split, with the derived sign put back in order.
pub fn create_iceberg_change_window_page_stream(
    request: IcebergChangeWindowPageSourceRequest<'_>,
    budget: &ConnectorPollBudget,
) -> Result<OwnedConnectorPageStream, ConnectorError> {
    let ChangeWindowRead {
        relation,
        base_columns,
        delete_mode,
        output,
    } = ChangeWindowRead::of(&request)?;
    let inner = create_iceberg_page_stream(
        data_request(request, &relation, &base_columns, delete_mode),
        budget,
    )?;
    Ok(Box::pin(IcebergChangeWindowPageStream { inner, output }))
}

/// What a change-window split reads through the data reader, and how its
/// pages are put back into the scan's order.
struct ChangeWindowRead {
    relation: IcebergReadRelation,
    base_columns: Vec<IcebergColumnHandle>,
    delete_mode: DeleteEvaluationMode,
    output: ChangeOpOutput,
}

impl ChangeWindowRead {
    fn of(request: &IcebergChangeWindowPageSourceRequest<'_>) -> Result<Self, ConnectorError> {
        let split = request.split;
        let ChangeOpProjection {
            slots,
            base_columns,
        } = ChangeOpProjection::of(request.columns)?;
        Ok(Self {
            relation: match split {
                IcebergChangeSplit::VisibilityDifference(_) => {
                    IcebergReadRelation::of_change_window(
                        request.handle,
                        split.data().partition_spec_id(),
                    )?
                }
                _ => IcebergReadRelation::of_change_window_endpoint(
                    request.handle,
                    split.data().partition_spec_id(),
                    matches!(split, IcebergChangeSplit::DeletedDataFileRows(_)),
                )?,
            },
            delete_mode: delete_mode_of(split, request.handle)?,
            output: ChangeOpOutput {
                slots,
                base_channel_count: base_columns.len(),
                change_op: split.change_op(),
            },
            base_columns,
        })
    }
}

/// The request of a change-window split's data split.
fn data_request<'a>(
    request: IcebergChangeWindowPageSourceRequest<'a>,
    relation: &'a IcebergReadRelation,
    base_columns: &'a [IcebergColumnHandle],
    delete_mode: DeleteEvaluationMode,
) -> IcebergPageSourceRequest<'a> {
    IcebergPageSourceRequest {
        relation,
        split: request.split.data(),
        columns: base_columns,
        delete_manager: request.delete_manager,
        delete_mode,
        footers: request.footers,
        access_binding: request.access_binding,
        context: request.context,
        cache: request.cache,
        budget: request.budget,
        reader_options: request.reader_options,
        scheduled_split_sequence_id: request.scheduled_split_sequence_id,
        dynamic_filter: request.dynamic_filter,
        prepared_input: None,
        pending_preparation_control: None,
    }
}

/// How one split's delete state is spent, chosen by its variant.
///
/// Every reverse-side variant subtracts what the lower endpoint had already
/// removed. Leaving that out would emit rows that were invisible at both
/// endpoints, which is exactly the double counting the set-difference contract
/// exists to prevent.
fn delete_mode_of(
    split: &IcebergChangeSplit,
    handle: &IcebergChangeWindowHandle,
) -> Result<DeleteEvaluationMode, ConnectorError> {
    match split {
        IcebergChangeSplit::AddedRows(rows) => {
            if !rows.restricted_row_ids().is_empty() {
                // The wire field names row *ids*, not row positions, and
                // nothing in this stack produces one. Reading it as either
                // would be a guess about which rows a split owns.
                return Err(unsupported(format!(
                    "iceberg change-window added rows of {} are narrowed to {} row ids, which this page source does not implement",
                    rows.data().path(),
                    rows.restricted_row_ids().len()
                )));
            }
            // The split carries the upper endpoint's own closure, so excluding
            // what it deletes leaves exactly the rows that survive at `to`.
            Ok(DeleteEvaluationMode::ExcludeDeleted)
        }
        IcebergChangeSplit::VisibilityDifference(rows) => {
            Ok(DeleteEvaluationMode::VisibleDifference {
                from: EndpointDeleteClosure {
                    read_domain: handle.from_read_domain().clone(),
                    deletes: rows.from_deletes().to_vec(),
                },
                to: EndpointDeleteClosure {
                    read_domain: handle.to_read_domain().clone(),
                    deletes: rows.to_deletes().to_vec(),
                },
            })
        }
        IcebergChangeSplit::DeletedDataFileRows(_) => Ok(DeleteEvaluationMode::ExcludeDeleted),
    }
}

/// Which output channel each scan assignment comes from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputSlot {
    /// The next channel the data reader produces.
    Base,
    /// The split's sign, derived here.
    ChangeOp,
}

/// The scan's output columns, split into what a file supplies and what the
/// variant does.
struct ChangeOpProjection {
    slots: Vec<OutputSlot>,
    base_columns: Vec<IcebergColumnHandle>,
}

impl ChangeOpProjection {
    fn of(columns: &[IcebergColumnHandle]) -> Result<Self, ConnectorError> {
        let change_op = change_op_column_handle()?;
        let mut slots = Vec::with_capacity(columns.len());
        let mut base_columns = Vec::with_capacity(columns.len());
        for column in columns {
            if column.base_field_id() == ICEBERG_CHANGE_OP_FIELD_ID {
                // The reserved field ID belongs to the sign and to nothing
                // else, so a handle that claims it while describing another
                // column would silently be answered with a sign.
                if *column != change_op {
                    return Err(invalid(
                        "an iceberg scan assignment claims the change-window sign field id without being the sign column",
                    ));
                }
                slots.push(OutputSlot::ChangeOp);
            } else {
                slots.push(OutputSlot::Base);
                base_columns.push(column.clone());
            }
        }
        Ok(Self {
            slots,
            base_columns,
        })
    }
}

/// How a change-window page is put back into the scan's output order.
struct ChangeOpOutput {
    slots: Vec<OutputSlot>,
    base_channel_count: usize,
    change_op: i8,
}

impl ChangeOpOutput {
    fn project(&self, page: SourcePage) -> Result<SourcePage, ConnectorError> {
        let (rows, base_columns) = page.into_columns()?;
        if base_columns.len() != self.base_channel_count {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                format!(
                    "iceberg change-window page produced {} channels for {} base columns",
                    base_columns.len(),
                    self.base_channel_count
                ),
            ));
        }
        let mut columns = Vec::with_capacity(self.slots.len());
        let mut base = base_columns.into_iter();
        for slot in &self.slots {
            match slot {
                OutputSlot::Base => {
                    let Some(column) = base.next() else {
                        return Err(ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::Internal,
                            "iceberg change-window page ran out of base channels",
                        ));
                    };
                    columns.push(column);
                }
                OutputSlot::ChangeOp => columns.push(change_op_column(self.change_op, rows)),
            }
        }
        SourcePage::try_new(rows, columns)
    }
}

/// The sign column: eight-bit, one value, never widened.
fn change_op_column(change_op: i8, rows: usize) -> ArrayRef {
    Arc::new(Int8Array::from(vec![change_op; rows]))
}

/// One change-window split read as a page stream: its data split's stream,
/// with each page put back into the scan's order. It owns no cursor of its
/// own: the data reader underneath decides what rows exist. It prepares no
/// successor.
pub struct IcebergChangeWindowPageStream {
    inner: OwnedConnectorPageStream,
    output: ChangeOpOutput,
}

impl Stream for IcebergChangeWindowPageStream {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner
            .as_mut()
            .poll_next(cx)
            .map(|page| page.map(|page| page.and_then(|page| this.output.project(page))))
    }
}

impl ConnectorPageStream for IcebergChangeWindowPageStream {
    fn metrics(&self) -> PageSourceMetrics {
        self.inner.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.inner.memory_usage_bytes()
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        Pin::into_inner(self).inner.close()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::ConnectorErrorKind;
    use novarocks_spi::connector::read_stack::{
        CompleteAllDynamicFilter, SchemaTableName, SplitWeight, TupleDomain,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};
    use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};
    use crate::typed_read::change_window::{
        IcebergAddedRows, IcebergChangeWindowHandleParams, IcebergDeletedDataFileRows,
        IcebergVisibilityDifferenceRows,
    };
    use crate::typed_read::split::{
        IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
        IcebergSplit, IcebergSplitParams,
    };
    use crate::typed_read::test_streams::DrivenStream;

    use super::*;

    /// The data file's sequence number. Every delete below outranks it, which
    /// is what makes it applicable at all.
    const DATA_SEQUENCE_NUMBER: i64 = 3;

    fn iceberg_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("frozen table schema")
    }

    fn identified(name: &str, data_type: DataType, field_id: i32, nullable: bool) -> Field {
        Field::new(name, data_type, nullable).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())]
                .into_iter()
                .collect(),
        )
    }

    fn arrow_file_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            identified("id", DataType::Int64, 1, false),
            identified("region", DataType::Utf8, 2, true),
        ]))
    }

    /// One Parquet data file whose rows are `ids`, at positions `0..ids.len()`.
    fn write_data_file(path: &Path, ids: &[i64]) -> u64 {
        let schema = arrow_file_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|id| format!("r{id}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build data batch");
        write_parquet(path, schema, batch);
        fs::metadata(path).expect("stat data file").len()
    }

    fn write_position_delete(path: &Path, data_file: &str, positions: &[i64]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POS_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![data_file; positions.len()])),
                Arc::new(Int64Array::from(positions.to_vec())),
            ],
        )
        .expect("build position-delete batch");
        write_parquet(path, schema, batch);
    }

    fn write_equality_delete(path: &Path, ids: &[i64]) {
        let schema = Arc::new(ArrowSchema::new(vec![identified(
            "id",
            DataType::Int64,
            1,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .expect("build equality-delete batch");
        write_parquet(path, schema, batch);
    }

    fn write_parquet(path: &Path, schema: Arc<ArrowSchema>, batch: RecordBatch) {
        let file = fs::File::create(path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet writer");
    }

    fn file_size_of(path: &Path) -> i64 {
        i64::try_from(fs::metadata(path).expect("stat file").len()).expect("size fits in i64")
    }

    fn position_delete_descriptor(path: &Path, data_sequence_number: i64) -> IcebergDeleteFile {
        delete_descriptor(
            IcebergDeleteFileContent::PositionDeletes,
            path,
            data_sequence_number,
            Vec::new(),
        )
    }

    fn equality_delete_descriptor(path: &Path, data_sequence_number: i64) -> IcebergDeleteFile {
        delete_descriptor(
            IcebergDeleteFileContent::EqualityDeletes,
            path,
            data_sequence_number,
            vec![1],
        )
    }

    fn delete_descriptor(
        content: IcebergDeleteFileContent,
        path: &Path,
        data_sequence_number: i64,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFile {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            partition_spec_id: 0,
            partition_data_json: r#"{"version":1,"values":[]}"#.to_string(),
            content,
            path: path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(path).unwrap(),
            )
            .unwrap()
            .metadata()
            .file_metadata()
            .num_rows(),
            file_size_in_bytes: file_size_of(path),
            equality_field_ids,
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: None,
            decryption_data: None,
        })
        .expect("valid delete descriptor")
    }

    fn change_window_handle(schema: &Schema) -> IcebergChangeWindowHandle {
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("unpartitioned spec");
        IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: SchemaTableName::try_new("sales", "orders").expect("name"),
            table_schema_json: serde_json::to_string(schema).expect("schema json"),
            columns: vec![
                IcebergColumnHandle::base_column_of(schema, 1).expect("id"),
                IcebergColumnHandle::base_column_of(schema, 2).expect("region"),
            ],
            name_mapping_json: None,
            from_snapshot_id_exclusive: 11,
            to_snapshot_id_inclusive: 12,
            from_read_domain: crate::delete_semantics::test_read_domain(
                schema,
                &[spec.clone()],
                11,
            ),
            to_read_domain: crate::delete_semantics::test_read_domain(schema, &[spec.clone()], 12),
            partition_spec_jsons: BTreeMap::from([(
                0,
                serde_json::to_string(&spec).expect("spec json"),
            )]),
        })
        .expect("change window handle")
    }

    struct Fixture {
        _runtime: tokio::runtime::Runtime,
        directory: tempfile::TempDir,
        binding: IcebergReadBinding,
        context: FileReadContext,
        footers: Arc<ParquetFooterCache>,
        delete_manager: Arc<DeleteManager>,
        data_path: PathBuf,
        data_file_size: u64,
        record_count: i64,
    }

    impl Fixture {
        /// A fixture whose one data file holds `ids` at positions `0..n`.
        fn new(ids: &[i64]) -> Self {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            let file_runtime: Arc<dyn FileIoRuntime> =
                Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
            let task_spawner: Arc<dyn FileTaskSpawner> =
                Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
            let binding = IcebergReadBinding::new(
                None,
                FsAccessResolver::new(),
                Arc::clone(&file_runtime),
                Arc::clone(&task_spawner),
            );
            let context = FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: Some(Instant::now() + Duration::from_secs(60)),
                runtime: file_runtime,
                task_spawner,
                range: None,
            };
            let directory = tempfile::tempdir().expect("temporary directory");
            let data_path = directory.path().join("data.parquet");
            let data_file_size = write_data_file(&data_path, ids);
            Self {
                _runtime: runtime,
                directory,
                delete_manager: Arc::new(DeleteManager::new(binding.clone(), context.clone())),
                binding,
                context,
                footers: Arc::new(ParquetFooterCache::new()),
                data_path,
                data_file_size,
                record_count: ids.len() as i64,
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            self.directory.path().join(name)
        }

        fn data_file(&self) -> String {
            self.data_path.to_string_lossy().to_string()
        }

        fn data_split(&self, deletes: Vec<IcebergDeleteFile>) -> IcebergSplit {
            self.data_split_in_format(deletes, IcebergFileFormat::Parquet)
        }

        fn data_split_in_format(
            &self,
            deletes: Vec<IcebergDeleteFile>,
            file_format: IcebergFileFormat,
        ) -> IcebergSplit {
            self.data_split_at(deletes, file_format, 12)
        }

        fn data_split_at(
            &self,
            deletes: Vec<IcebergDeleteFile>,
            file_format: IcebergFileFormat,
            snapshot: i64,
        ) -> IcebergSplit {
            let schema = iceberg_schema();
            IcebergSplit::try_new(IcebergSplitParams {
                read_domain: crate::delete_semantics::test_read_domain(
                    &schema,
                    &[PartitionSpec::unpartition_spec()],
                    snapshot,
                ),
                path: self.data_file(),
                start: 0,
                length: self.data_file_size as i64,
                file_size: self.data_file_size as i64,
                file_record_count: self.record_count,
                file_format,
                partition_spec_id: 0,
                partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
                deletes: deletes.into(),
                file_statistics_domain: TupleDomain::all(),
                data_sequence_number: Some(DATA_SEQUENCE_NUMBER),
                file_first_row_id: None,
                decryption_data: None,
                split_weight: SplitWeight::STANDARD,
                affinity_key: None,
            })
            .expect("valid data split")
        }

        /// The split's stream, driven on the fixture runtime.
        fn page_source(
            &self,
            handle: &IcebergChangeWindowHandle,
            split: &IcebergChangeSplit,
            columns: &[IcebergColumnHandle],
        ) -> Result<DrivenStream<'_>, ConnectorError> {
            let budget = ConnectorPollBudget::new();
            let stream = self.page_stream(handle, split, columns, &budget)?;
            Ok(DrivenStream::new(&self._runtime, stream, budget))
        }

        fn page_stream(
            &self,
            handle: &IcebergChangeWindowHandle,
            split: &IcebergChangeSplit,
            columns: &[IcebergColumnHandle],
            budget: &ConnectorPollBudget,
        ) -> Result<OwnedConnectorPageStream, ConnectorError> {
            create_iceberg_change_window_page_stream(self.request(handle, split, columns), budget)
        }

        fn request<'a>(
            &self,
            handle: &'a IcebergChangeWindowHandle,
            split: &'a IcebergChangeSplit,
            columns: &'a [IcebergColumnHandle],
        ) -> IcebergChangeWindowPageSourceRequest<'a> {
            IcebergChangeWindowPageSourceRequest {
                handle,
                split,
                columns,
                delete_manager: Arc::clone(&self.delete_manager),
                footers: Arc::clone(&self.footers),
                access_binding: self.binding.clone(),
                context: self.context.clone(),
                cache: None,
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                scheduled_split_sequence_id: 0,
                dynamic_filter: Arc::new(CompleteAllDynamicFilter::new(
                    std::collections::BTreeSet::new(),
                )) as Arc<IcebergDynamicFilter>,
            }
        }
    }

    /// The scan's output columns: the base relation's `id`, then the sign.
    fn id_and_change_op(schema: &Schema) -> Vec<IcebergColumnHandle> {
        vec![
            IcebergColumnHandle::base_column_of(schema, 1).expect("id"),
            change_op_column_handle().expect("change op"),
        ]
    }

    /// Drain the source into `(id, __change_op)` pairs, proving the sign column
    /// is eight-bit on the way through.
    fn drain(source: &mut DrivenStream<'_>) -> Vec<(i64, i8)> {
        let mut rows = Vec::new();
        for _ in 0..64 {
            let Some(page) = source.next_page().expect("page") else {
                break;
            };
            rows.extend(signed_rows(page));
        }
        assert!(source.is_finished(), "the split drains in bounded steps");
        rows
    }

    fn drain_stream(
        fixture: &Fixture,
        stream: &mut OwnedConnectorPageStream,
        budget: &ConnectorPollBudget,
    ) -> Vec<(i64, i8)> {
        use futures::StreamExt;
        fixture._runtime.block_on(async {
            let mut rows = Vec::new();
            loop {
                budget.refill(1024);
                match stream.next().await {
                    Some(page) => rows.extend(signed_rows(page.expect("page"))),
                    None => return rows,
                }
            }
        })
    }

    /// One page's `(id, __change_op)` pairs.
    fn signed_rows(page: SourcePage) -> Vec<(i64, i8)> {
        let (count, columns) = page.into_columns().expect("materialize");
        assert_eq!(columns.len(), 2, "id and the derived sign");
        assert_eq!(
            columns[1].data_type(),
            &DataType::Int8,
            "the change sign is eight-bit"
        );
        let ids = columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ids");
        let signs = columns[1]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 signs");
        (0..count)
            .map(|row| (ids.value(row), signs.value(row)))
            .collect()
    }

    /// Reads the split as a stream and closes it.
    fn read_streamed(
        fixture: &Fixture,
        handle: &IcebergChangeWindowHandle,
        split: &IcebergChangeSplit,
        schema: &Schema,
    ) -> Vec<(i64, i8)> {
        let columns = id_and_change_op(schema);
        let budget = ConnectorPollBudget::new();
        let mut stream = fixture
            .page_stream(handle, split, &columns, &budget)
            .expect("page stream");
        let streamed = drain_stream(fixture, &mut stream, &budget);
        fixture
            ._runtime
            .block_on(stream.close())
            .expect("close a drained stream");
        streamed
    }

    #[test]
    fn added_rows_emit_every_row_that_survives_at_the_upper_endpoint_with_a_plus_one_sign() {
        let fixture = Fixture::new(&[10, 11, 12]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(fixture.data_split(Vec::new()), Vec::new())
                .expect("added rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        assert_eq!(drain(&mut source), vec![(10, 1), (11, 1), (12, 1)]);
    }

    #[test]
    fn a_row_written_and_deleted_inside_the_window_produces_no_output_row() {
        // The file is new at the upper endpoint, so it travels with the upper
        // endpoint's own closure. Position 1 was written and deleted inside the
        // window: it is invisible at both endpoints and the difference does not
        // own it in either direction.
        let fixture = Fixture::new(&[10, 11, 12]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let deletes = fixture.path("inside-window.parquet");
        write_position_delete(&deletes, &fixture.data_file(), &[1]);
        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(
                fixture.data_split(vec![position_delete_descriptor(
                    &deletes,
                    DATA_SEQUENCE_NUMBER + 1,
                )]),
                Vec::new(),
            )
            .expect("added rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        let rows = drain(&mut source);
        assert_eq!(rows, vec![(10, 1), (12, 1)]);
        assert!(
            rows.iter().all(|(id, _)| *id != 11),
            "a row written and deleted inside the window has no sign at all"
        );
    }

    fn difference(
        fixture: &Fixture,
        from: Vec<IcebergDeleteFile>,
        to: Vec<IcebergDeleteFile>,
    ) -> IcebergChangeSplit {
        IcebergChangeSplit::VisibilityDifference(
            IcebergVisibilityDifferenceRows::try_new(fixture.data_split(to.clone()), from, to)
                .unwrap(),
        )
    }
    fn removed(fixture: &Fixture, from: Vec<IcebergDeleteFile>) -> IcebergChangeSplit {
        IcebergChangeSplit::DeletedDataFileRows(
            IcebergDeletedDataFileRows::try_new(fixture.data_split_at(
                from,
                IcebergFileFormat::Parquet,
                11,
            ))
            .unwrap(),
        )
    }
    #[test]
    fn removed_files_exclude_the_complete_from_closure() {
        let fixture = Fixture::new(&[20, 21, 22]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let old = fixture.path("old.parquet");
        write_position_delete(&old, &fixture.data_file(), &[0]);
        let split = removed(
            &fixture,
            vec![position_delete_descriptor(&old, DATA_SEQUENCE_NUMBER + 1)],
        );
        assert_eq!(
            read_streamed(&fixture, &handle, &split, &schema),
            vec![(21, -1), (22, -1)]
        );
        assert_eq!(
            read_streamed(&fixture, &handle, &removed(&fixture, vec![]), &schema),
            vec![(20, -1), (21, -1), (22, -1)]
        );
    }
    #[test]
    fn replacement_closure_emits_only_the_endpoint_row_bag_difference() {
        let fixture = Fixture::new(&[30, 31, 32, 33]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let old = fixture.path("old.parquet");
        let new = fixture.path("replacement.parquet");
        write_position_delete(&old, &fixture.data_file(), &[0]);
        write_position_delete(&new, &fixture.data_file(), &[0, 2]);
        let split = difference(
            &fixture,
            vec![position_delete_descriptor(&old, 4)],
            vec![position_delete_descriptor(&new, 5)],
        );
        assert_eq!(
            read_streamed(&fixture, &handle, &split, &schema),
            vec![(32, -1)]
        );
    }
    #[test]
    fn overlapping_kinds_and_duplicate_row_values_preserve_signed_bag_multiplicity() {
        let fixture = Fixture::new(&[40, 41, 42, 42, 43]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let pos = fixture.path("pos.parquet");
        let eq = fixture.path("eq.parquet");
        write_position_delete(&pos, &fixture.data_file(), &[1, 2]);
        write_equality_delete(&eq, &[41, 42]);
        let split = difference(
            &fixture,
            vec![],
            vec![
                position_delete_descriptor(&pos, 4),
                equality_delete_descriptor(&eq, 4),
            ],
        );
        assert_eq!(
            read_streamed(&fixture, &handle, &split, &schema),
            vec![(41, -1), (42, -1), (42, -1)]
        );
    }
    #[test]
    fn from_equality_keys_remain_in_the_hidden_projection() {
        let fixture = Fixture::new(&[40, 41, 42]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let old = fixture.path("old-eq.parquet");
        let new = fixture.path("new-pos.parquet");
        write_equality_delete(&old, &[41]);
        write_position_delete(&new, &fixture.data_file(), &[1, 2]);
        let split = difference(
            &fixture,
            vec![equality_delete_descriptor(&old, 4)],
            vec![position_delete_descriptor(&new, 5)],
        );
        assert_eq!(
            read_streamed(&fixture, &handle, &split, &schema),
            vec![(42, -1)]
        );
    }
    #[test]
    fn repeated_output_field_ids_do_not_duplicate_the_equality_binding() {
        let fixture = Fixture::new(&[10, 11]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let eq = fixture.path("eq.parquet");
        write_equality_delete(&eq, &[11]);
        let split = difference(&fixture, vec![], vec![equality_delete_descriptor(&eq, 4)]);
        let id = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
        let columns = vec![id.clone(), id, change_op_column_handle().unwrap()];
        let mut source = fixture.page_source(&handle, &split, &columns).unwrap();
        let mut rows = Vec::new();
        while let Some(page) = source.next_page().unwrap() {
            let (count, columns) = page.into_columns().unwrap();
            assert_eq!(columns.len(), 3);
            let a = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
            let b = columns[1].as_any().downcast_ref::<Int64Array>().unwrap();
            let sign = columns[2].as_any().downcast_ref::<Int8Array>().unwrap();
            rows.extend((0..count).map(|i| (a.value(i), b.value(i), sign.value(i))));
        }
        assert_eq!(rows, vec![(11, 11, -1)]);
    }

    #[test]
    fn actual_row_reappearance_remains_explicitly_unsupported() {
        let fixture = Fixture::new(&[40, 41]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let old = fixture.path("old.parquet");
        write_position_delete(&old, &fixture.data_file(), &[0]);
        let split = difference(&fixture, vec![position_delete_descriptor(&old, 4)], vec![]);
        let columns = id_and_change_op(&schema);
        let mut source = fixture.page_source(&handle, &split, &columns).unwrap();
        assert_eq!(
            source.next_page().unwrap_err().kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn the_change_sign_is_an_int8_array_rather_than_a_widened_integer() {
        // The Iceberg column handle has to declare `int` because the table
        // format has no eight-bit integer, but the engine contract is Int8 and
        // the page must not widen it.
        let fixture = Fixture::new(&[50]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let column = change_op_column_handle().expect("change op");
        assert_eq!(
            column.type_json(),
            "\"int\"",
            "the declared iceberg type stays int"
        );

        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(fixture.data_split(Vec::new()), Vec::new())
                .expect("added rows"),
        );
        let mut source = fixture
            .page_source(&handle, &split, &[column])
            .expect("page source");
        let page = source
            .next_page()
            .expect("page")
            .expect("one page of one row");
        let (rows, columns) = page.into_columns().expect("materialize");
        assert_eq!(rows, 1);
        assert_eq!(columns.len(), 1, "the sign alone is a legal projection");
        assert_eq!(columns[0].data_type(), &DataType::Int8);
        let signs = columns[0]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 signs");
        assert_eq!(signs.value(0), 1);
    }

    #[test]
    fn a_non_parquet_change_window_split_is_a_stable_unsupported_rather_than_a_wrong_read() {
        let fixture = Fixture::new(&[60]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
            let split = IcebergChangeSplit::AddedRows(
                IcebergAddedRows::try_new(
                    fixture.data_split_in_format(Vec::new(), format),
                    Vec::new(),
                )
                .expect("added rows"),
            );
            let error = fixture
                .page_source(&handle, &split, &id_and_change_op(&schema))
                .err()
                .expect("only parquet is implemented");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
            assert!(
                error
                    .to_string()
                    .contains("not readable by this page source")
            );
        }
    }

    #[test]
    fn an_added_rows_split_narrowed_to_row_ids_is_rejected_rather_than_guessed() {
        // The wire field names row *ids*, which are not row positions. Nothing
        // in this stack produces one, and reading it as either would be a guess
        // about which rows the split owns.
        let fixture = Fixture::new(&[70, 71]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(fixture.data_split(Vec::new()), vec![0, 1])
                .expect("added rows"),
        );

        let error = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .err()
            .expect("a narrowed added-rows split is not implemented");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(error.to_string().contains("row ids"));
    }

    #[test]
    fn the_sign_column_the_frontend_binds_is_the_one_this_reader_recognizes() {
        // The frontend appends `change_op_column_handle()` to a change
        // window's bindings, and the provider decodes each scan assignment from
        // its private wire value. A lossy round trip would make the sign look
        // like an impostor claiming its reserved field id.
        let column = change_op_column_handle().expect("change op");
        let decoded =
            IcebergColumnHandle::from_proto(&column.to_proto()).expect("decode the sign column");

        assert_eq!(decoded, column);
        let projection = ChangeOpProjection::of(&[decoded]).expect("the sign is recognized");
        assert_eq!(projection.slots, vec![OutputSlot::ChangeOp]);
        assert!(projection.base_columns.is_empty());
    }

    #[test]
    fn a_scan_assignment_that_steals_the_sign_field_id_is_rejected() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                ICEBERG_CHANGE_OP_FIELD_ID,
                "borrowed",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let impostor = IcebergColumnHandle::base_column_of(&schema, ICEBERG_CHANGE_OP_FIELD_ID)
            .expect("handle");

        let error = ChangeOpProjection::of(&[impostor])
            .err()
            .expect("the reserved field id belongs to the sign alone");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn an_equivalent_rewrite_of_delete_artifacts_emits_no_rows() {
        let fixture = Fixture::new(&[10, 11, 12]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let before = fixture.path("before.parquet");
        let after = fixture.path("after.parquet");
        write_position_delete(&before, &fixture.data_file(), &[0, 1]);
        write_equality_delete(&after, &[10, 11]);
        let split = difference(
            &fixture,
            vec![position_delete_descriptor(&before, 4)],
            vec![equality_delete_descriptor(&after, 5)],
        );
        assert!(read_streamed(&fixture, &handle, &split, &schema).is_empty());
    }
}
