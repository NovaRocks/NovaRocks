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
//!   them, which is [`DeleteEvaluationMode::SelectRemovedRows`]. The forward
//!   side is an ordinary exclusion read of the upper endpoint's own closure.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array};
use novarocks_fs::{FileReadBudget, FileReadContext, FileReaderOptions};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, PageSourceMetrics, SourcePage};

use crate::access_binding::IcebergReadBinding;

use super::change_window::{
    ICEBERG_CHANGE_OP_FIELD_ID, IcebergChangeSplit, IcebergChangeWindowHandle,
    change_op_column_handle,
};
use super::column_handle::{IcebergColumnHandle, invalid, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, RemovedRowSelection};
use super::page_source::{
    IcebergDynamicFilter, IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache,
    create_iceberg_page_source,
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

/// Build the page source for one change-window split.
pub fn create_iceberg_change_window_page_source(
    request: IcebergChangeWindowPageSourceRequest<'_>,
) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
    let split = request.split;
    let data = split.data();
    let projection = ChangeOpProjection::of(request.columns)?;
    let relation = IcebergReadRelation::of_change_window(request.handle, data.partition_spec_id())?;
    let delete_mode = delete_mode_of(split)?;

    let inner = create_iceberg_page_source(IcebergPageSourceRequest {
        relation: &relation,
        split: data,
        columns: &projection.base_columns,
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
    })?;

    Ok(Box::new(IcebergChangeWindowPageSource {
        inner,
        slots: projection.slots,
        base_channel_count: projection.base_columns.len(),
        change_op: split.change_op(),
    }))
}

/// How one split's delete state is spent, chosen by its variant.
///
/// Every reverse-side variant subtracts what the lower endpoint had already
/// removed. Leaving that out would emit rows that were invisible at both
/// endpoints, which is exactly the double counting the set-difference contract
/// exists to prevent.
fn delete_mode_of(split: &IcebergChangeSplit) -> Result<DeleteEvaluationMode, ConnectorError> {
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
        IcebergChangeSplit::PositionDeletedRows(rows) => {
            Ok(DeleteEvaluationMode::SelectRemovedRows {
                selected: RemovedRowSelection::NamedBy(rows.newly_applied_deletes().to_vec()),
                previously_applied: rows.previously_applied_deletes().to_vec(),
            })
        }
        IcebergChangeSplit::EqualityDeletedRows(rows) => {
            Ok(DeleteEvaluationMode::SelectRemovedRows {
                selected: RemovedRowSelection::NamedBy(
                    rows.newly_applied_equality_deletes().to_vec(),
                ),
                // Every position delete that newly applies to this data file
                // is here too, so a row both a position delete and an equality
                // delete named is emitted once, by the position variant.
                previously_applied: rows.previously_applied_deletes().to_vec(),
            })
        }
        IcebergChangeSplit::DeletedDataFileRows(rows) => {
            Ok(DeleteEvaluationMode::SelectRemovedRows {
                selected: RemovedRowSelection::WholeFile,
                previously_applied: rows.previously_applied_deletes().to_vec(),
            })
        }
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

/// One change-window split's reader.
///
/// It owns no cursor of its own: the data reader underneath decides what rows
/// exist, and this source only restores the scan's output order by putting the
/// derived sign back where the assignments asked for it.
pub struct IcebergChangeWindowPageSource {
    inner: Box<dyn ConnectorPageSource>,
    slots: Vec<OutputSlot>,
    base_channel_count: usize,
    change_op: i8,
}

impl IcebergChangeWindowPageSource {
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

impl ConnectorPageSource for IcebergChangeWindowPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        let Some(page) = self.inner.next_source_page()? else {
            return Ok(None);
        };
        Ok(Some(self.project(page)?))
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    fn is_blocked(&self) -> bool {
        self.inner.is_blocked()
    }

    fn metrics(&self) -> PageSourceMetrics {
        self.inner.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.inner.memory_usage_bytes()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.inner.close()
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
        IcebergEqualityDeletedRows, IcebergPositionDeletedRows,
    };
    use crate::typed_read::split::{
        IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
        IcebergSplit, IcebergSplitParams,
    };

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
            content,
            path: path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: 1,
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
            IcebergSplit::try_new(IcebergSplitParams {
                path: self.data_file(),
                start: 0,
                length: self.data_file_size as i64,
                file_size: self.data_file_size as i64,
                file_record_count: self.record_count,
                file_format,
                partition_spec_id: 0,
                partition_data_json: "{}".to_owned(),
                deletes,
                file_statistics_domain: TupleDomain::all(),
                data_sequence_number: Some(DATA_SEQUENCE_NUMBER),
                file_first_row_id: None,
                decryption_data: None,
                split_weight: SplitWeight::STANDARD,
                affinity_key: None,
            })
            .expect("valid data split")
        }

        fn page_source(
            &self,
            handle: &IcebergChangeWindowHandle,
            split: &IcebergChangeSplit,
            columns: &[IcebergColumnHandle],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            create_iceberg_change_window_page_source(IcebergChangeWindowPageSourceRequest {
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
            })
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
    fn drain(source: &mut Box<dyn ConnectorPageSource>) -> Vec<(i64, i8)> {
        let mut rows = Vec::new();
        for _ in 0..64 {
            if source.is_finished() {
                break;
            }
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
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
            for row in 0..count {
                rows.push((ids.value(row), signs.value(row)));
            }
        }
        assert!(source.is_finished(), "the split drains in bounded steps");
        rows
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

    #[test]
    fn deleted_data_file_rows_emit_what_was_visible_at_the_lower_endpoint_with_a_minus_one_sign() {
        // The whole file is gone at the upper endpoint, so every row it still
        // had at the lower one left the relation -- but position 0 was already
        // invisible there and was never part of `Visible(from)`.
        let fixture = Fixture::new(&[20, 21, 22]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let previously = fixture.path("already-applied.parquet");
        write_position_delete(&previously, &fixture.data_file(), &[0]);
        let split = IcebergChangeSplit::DeletedDataFileRows(
            IcebergDeletedDataFileRows::try_new(
                fixture.data_split(Vec::new()),
                vec![position_delete_descriptor(
                    &previously,
                    DATA_SEQUENCE_NUMBER + 1,
                )],
            )
            .expect("deleted data file rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        assert_eq!(drain(&mut source), vec![(21, -1), (22, -1)]);
    }

    #[test]
    fn deleted_data_file_rows_without_a_prior_closure_emit_the_whole_file() {
        let fixture = Fixture::new(&[20, 21]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let split = IcebergChangeSplit::DeletedDataFileRows(
            IcebergDeletedDataFileRows::try_new(fixture.data_split(Vec::new()), Vec::new())
                .expect("deleted data file rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        assert_eq!(drain(&mut source), vec![(20, -1), (21, -1)]);
    }

    #[test]
    fn position_deleted_rows_select_exactly_the_newly_deleted_rows_and_not_the_surviving_ones() {
        // The reverse side is the inverse of exclusion: the newly applied
        // artifact names the rows to emit. Position 0 was already gone at the
        // lower endpoint, so naming it again removes nothing new.
        let fixture = Fixture::new(&[30, 31, 32, 33]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let previously = fixture.path("previously.parquet");
        let newly = fixture.path("newly.parquet");
        write_position_delete(&previously, &fixture.data_file(), &[0]);
        write_position_delete(&newly, &fixture.data_file(), &[0, 2]);
        let split = IcebergChangeSplit::PositionDeletedRows(
            IcebergPositionDeletedRows::try_new(
                fixture.data_split(Vec::new()),
                vec![position_delete_descriptor(&newly, DATA_SEQUENCE_NUMBER + 2)],
                vec![position_delete_descriptor(
                    &previously,
                    DATA_SEQUENCE_NUMBER + 1,
                )],
            )
            .expect("position deleted rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        // 32 alone: 30 was already invisible at the lower endpoint, and 31/33
        // survive at the upper one.
        assert_eq!(drain(&mut source), vec![(32, -1)]);
    }

    #[test]
    fn equality_deleted_rows_do_not_re_emit_a_row_a_position_delete_of_the_same_window_named() {
        // The window applies both a position delete naming position 1 and an
        // equality delete naming ids 41 and 42. The position variant owns
        // position 1, so the equality variant is handed it as already applied
        // and emits only what the first one did not.
        let fixture = Fixture::new(&[40, 41, 42, 43]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let positions = fixture.path("newly-positions.parquet");
        let equality = fixture.path("newly-equality.parquet");
        write_position_delete(&positions, &fixture.data_file(), &[1]);
        write_equality_delete(&equality, &[41, 42]);

        let newly_position = position_delete_descriptor(&positions, DATA_SEQUENCE_NUMBER + 1);
        let newly_equality = equality_delete_descriptor(&equality, DATA_SEQUENCE_NUMBER + 1);

        let position_split = IcebergChangeSplit::PositionDeletedRows(
            IcebergPositionDeletedRows::try_new(
                fixture.data_split(Vec::new()),
                vec![newly_position.clone()],
                Vec::new(),
            )
            .expect("position deleted rows"),
        );
        let equality_split = IcebergChangeSplit::EqualityDeletedRows(
            IcebergEqualityDeletedRows::try_new(
                fixture.data_split(Vec::new()),
                vec![newly_equality],
                vec![newly_position],
            )
            .expect("equality deleted rows"),
        );

        let mut position_source = fixture
            .page_source(&handle, &position_split, &id_and_change_op(&schema))
            .expect("position page source");
        let mut equality_source = fixture
            .page_source(&handle, &equality_split, &id_and_change_op(&schema))
            .expect("equality page source");

        assert_eq!(drain(&mut position_source), vec![(41, -1)]);
        // 41 is not emitted a second time, and 42 is emitted exactly once.
        assert_eq!(drain(&mut equality_source), vec![(42, -1)]);
    }

    #[test]
    fn a_previously_applied_equality_delete_is_read_through_the_pages_hidden_suffix() {
        // The lower endpoint had already lost id 41 to an equality delete, so
        // the newly applied position delete naming its row removes nothing new.
        // Proving it needs the equality key column on the page, which only the
        // previously applied side asked for.
        let fixture = Fixture::new(&[40, 41, 42]);
        let schema = iceberg_schema();
        let handle = change_window_handle(&schema);
        let previously = fixture.path("previously-equality.parquet");
        let newly = fixture.path("newly-positions.parquet");
        write_equality_delete(&previously, &[41]);
        write_position_delete(&newly, &fixture.data_file(), &[1, 2]);

        let split = IcebergChangeSplit::PositionDeletedRows(
            IcebergPositionDeletedRows::try_new(
                fixture.data_split(Vec::new()),
                vec![position_delete_descriptor(&newly, DATA_SEQUENCE_NUMBER + 2)],
                vec![equality_delete_descriptor(
                    &previously,
                    DATA_SEQUENCE_NUMBER + 1,
                )],
            )
            .expect("position deleted rows"),
        );

        let mut source = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .expect("page source");
        assert_eq!(drain(&mut source), vec![(42, -1)]);
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
            .next_source_page()
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
}
