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
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FileRangeScope, FileRangeService,
    FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
};
use novarocks_spi::connector::ConnectorErrorKind;
use novarocks_spi::connector::read_stack::{
    DynamicFilter, OwnedConnectorPageStream, SplitWeight, TupleDomain,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;

use super::*;

mod dictionary_output;
mod output_policy;
use crate::iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type};
use crate::typed_read::schema_binding::IcebergMetadataColumn;
use crate::typed_read::split::{
    IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergSplitParams,
};
use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};
use crate::typed_read::test_spawners::{CountingFileIoSpawner, GatedFileIoSpawner};
use crate::typed_read::test_streams::DrivenStream;

const ROWS_PER_GROUP: usize = 4;

fn tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("build Tokio runtime")
}

fn read_context(runtime: &tokio::runtime::Runtime) -> FileReadContext {
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let io_spawner: Arc<dyn FileIoSpawner> =
        Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
    FileReadContext {
        turn: None,
        cancellation: FileCancellation::new(),
        deadline: Some(Instant::now() + Duration::from_secs(60)),
        runtime: file_runtime,
        io_spawner,
        range: None,
    }
}

fn read_binding(runtime: &tokio::runtime::Runtime) -> IcebergReadBinding {
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let io_spawner: Arc<dyn FileIoSpawner> =
        Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
    IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        file_runtime,
        novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
        io_spawner,
    )
}

fn iceberg_schema() -> IcebergSchema {
    IcebergSchema::builder()
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

#[test]
fn typed_bigint_lower_bound_becomes_a_physical_file_predicate() {
    let schema = iceberg_schema();
    let column = IcebergColumnHandle::base_column_of(&schema, 1).expect("id handle");
    let range = novarocks_spi::connector::read_stack::Range::try_new(
        ConnectorValueType::BigInt,
        Bound::Inclusive(ConnectorValue::BigInt(199_000)),
        Bound::Unbounded,
    )
    .expect("lower bound");
    let domain = Domain::new(
        novarocks_spi::connector::read_stack::ValueSet::of_ranges(
            ConnectorValueType::BigInt,
            vec![range],
        )
        .expect("value set"),
        false,
    );
    let predicate = TupleDomain::with_column_domains([(column, domain)].into_iter().collect())
        .expect("tuple domain");

    let predicates = static_file_predicates(&predicate, None);

    assert_eq!(predicates.len(), 1);
    assert_eq!(predicates[0].column(), "id");
    assert_eq!(predicates[0].physical_field_id(), Some(1));
    assert_eq!(predicates[0].source(), ScanPredicateSource::Static);
    assert_eq!(
        predicates[0].domain(),
        &ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int64(199_000),
        }
    );
}

#[test]
fn nullable_typed_domain_stays_residual() {
    let schema = iceberg_schema();
    let column = IcebergColumnHandle::base_column_of(&schema, 1).expect("id handle");
    let domain = Domain::new(
        novarocks_spi::connector::read_stack::ValueSet::of_values(
            ConnectorValueType::BigInt,
            vec![ConnectorValue::BigInt(7)],
        )
        .expect("value set"),
        true,
    );
    let predicate = TupleDomain::with_column_domains([(column, domain)].into_iter().collect())
        .expect("tuple domain");

    assert!(static_file_predicates(&predicate, None).is_empty());
}

fn arrow_file_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        ),
        Field::new("region", DataType::Utf8, true).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    ]))
}

/// Write `groups * ROWS_PER_GROUP` rows into that many Parquet row groups.
fn write_data_file(path: &Path, groups: usize) -> u64 {
    let schema = arrow_file_schema();
    let file = fs::File::create(path).expect("create data file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
        .expect("create parquet writer");
    for group in 0..groups {
        let base = (group * ROWS_PER_GROUP) as i64;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    (0..ROWS_PER_GROUP as i64)
                        .map(|row| base + row)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..ROWS_PER_GROUP)
                        .map(|row| format!("r{}", base as usize + row))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build data batch");
        writer.write(&batch).expect("write data batch");
        writer.flush().expect("close row group");
    }
    writer.close().expect("close parquet writer");
    fs::metadata(path).expect("stat data file").len()
}

fn write_large_data_file(path: &Path) -> u64 {
    let schema = arrow_file_schema();
    let file = fs::File::create(path).expect("create data file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
        .expect("create parquet writer");
    for group in 0..3 {
        let base = (group * ROWS_PER_GROUP) as i64;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    (0..ROWS_PER_GROUP as i64)
                        .map(|row| base + row)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    (0..ROWS_PER_GROUP)
                        .map(|row| format!("{}{}", base + row as i64, "x".repeat(24 * 1024)))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("build data batch");
        writer.write(&batch).expect("write data batch");
        writer.flush().expect("close row group");
    }
    writer.close().expect("close parquet writer");
    fs::metadata(path).expect("stat data file").len()
}

/// The Arrow schema of a data file that materializes its row lineage.
///
/// This is the shape a rewrite produces: the two reserved field IDs are
/// real Parquet columns alongside the table's own. Both are nullable here
/// because Iceberg declares them optional and a null row means "inherit",
/// which is the half of the rule a required column could never exercise.
fn arrow_row_lineage_file_schema() -> Arc<ArrowSchema> {
    fn with_field_id(field: Field, field_id: i32) -> Field {
        field.with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        )
    }
    Arc::new(ArrowSchema::new(vec![
        with_field_id(Field::new("id", DataType::Int64, false), 1),
        with_field_id(Field::new("region", DataType::Utf8, true), 2),
        with_field_id(
            Field::new(
                crate::row_lineage_synth::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                true,
            ),
            crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        ),
        with_field_id(
            Field::new(
                crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL,
                DataType::Int64,
                true,
            ),
            crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        ),
    ]))
}

/// Write one row group whose rows carry the lineage a rewrite preserved.
///
/// The four rows deliberately cover every corner of the rule: both stored,
/// only `_row_id` stored, only `_last_updated_sequence_number` stored, and
/// neither.
fn write_row_lineage_data_file(path: &Path) -> u64 {
    let schema = arrow_row_lineage_file_schema();
    let file = fs::File::create(path).expect("create data file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
        .expect("create parquet writer");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3])),
            Arc::new(StringArray::from(vec!["r0", "r1", "r2", "r3"])),
            Arc::new(Int64Array::from(vec![
                Some(70_i64),
                Some(71),
                Some(72),
                None,
            ])),
            Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(2), None])),
        ],
    )
    .expect("build data batch");
    writer.write(&batch).expect("write data batch");
    writer.close().expect("close parquet writer");
    fs::metadata(path).expect("stat data file").len()
}

/// The byte offset at which each row group's data starts.
fn row_group_offsets(path: &Path) -> Vec<u64> {
    let file = fs::File::open(path).expect("open data file");
    let reader =
        parquet::file::reader::SerializedFileReader::new(file).expect("open parquet footer");
    parquet::file::reader::FileReader::metadata(&reader)
        .row_groups()
        .iter()
        .map(|group| {
            let column = group.column(0);
            let offset = column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset())
                .min(column.data_page_offset());
            u64::try_from(offset).expect("nonnegative row group offset")
        })
        .collect()
}

fn table_handle(schema: &IcebergSchema, partitioned: bool) -> IcebergTableHandle {
    let mut partition_spec_jsons = std::collections::BTreeMap::new();
    let spec = if partitioned {
        crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("identity partition field")
            .build()
            .expect("partition spec")
    } else {
        crate::iceberg::spec::PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("unpartitioned spec")
    };
    partition_spec_jsons.insert(
        0,
        serde_json::to_string(&spec).expect("serialize partition spec"),
    );
    IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: novarocks_spi::connector::read_stack::SchemaTableName::try_new(
            "sales", "orders",
        )
        .expect("schema table name"),
        snapshot_id: Some(11),
        read_domain: Some(crate::delete_semantics::test_read_domain(
            schema,
            std::slice::from_ref(&spec),
            11,
        )),
        table_schema_json: serde_json::to_string(schema).expect("serialize schema"),
        spec_id: Some(0),
        partition_spec_jsons,
        format_version: 2,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: Default::default(),
        name_mapping_json: None,
        table_location: "/tmp/iceberg/orders".to_owned(),
        storage_properties: Default::default(),
        pinned_data_files: None,
    })
    .expect("table handle")
}

struct SplitOptions {
    start: i64,
    length: i64,
    file_size: i64,
    file_record_count: i64,
    format: IcebergFileFormat,
    partition_data_json: String,
    deletes: Vec<IcebergDeleteFile>,
    first_row_id: Option<i64>,
    decryption: Option<ParquetFileDecryptionData>,
}

impl SplitOptions {
    fn whole_file(file_size: u64, records: i64) -> Self {
        Self {
            start: 0,
            length: file_size as i64,
            file_size: file_size as i64,
            file_record_count: records,
            format: IcebergFileFormat::Parquet,
            partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
            deletes: Vec::new(),
            first_row_id: None,
            decryption: None,
        }
    }
}

fn build_split(name: &str, options: SplitOptions) -> IcebergSplit {
    let schema = iceberg_schema();
    let partitioned = options.partition_data_json != r#"{"version":1,"values":[]}"#;
    let handle = table_handle(&schema, partitioned);
    IcebergSplit::try_new(IcebergSplitParams {
        read_domain: Arc::clone(handle.read_domain().expect("fixture domain")),
        path: name.to_owned(),
        start: options.start,
        length: options.length,
        file_size: options.file_size,
        file_record_count: options.file_record_count,
        file_format: options.format,
        partition_spec_id: 0,
        partition_data_json: options.partition_data_json,
        deletes: options.deletes.into(),
        file_statistics_domain: TupleDomain::all(),
        data_sequence_number: Some(3),
        file_first_row_id: options.first_row_id,
        decryption_data: options.decryption,
        split_weight: SplitWeight::STANDARD,
        affinity_key: None,
    })
    .expect("split")
}

struct Harness {
    _runtime: tokio::runtime::Runtime,
    _directory: tempfile::TempDir,
    binding: IcebergReadBinding,
    context: FileReadContext,
    footers: Arc<ParquetFooterCache>,
    delete_manager: Arc<DeleteManager>,
    file_size: u64,
    offsets: Vec<u64>,
    file_name: String,
}

fn harness(groups: usize) -> Harness {
    harness_of(|path| write_data_file(path, groups))
}

/// A harness over a data file this test wrote itself.
fn harness_of(write: impl FnOnce(&Path) -> u64) -> Harness {
    let runtime = tokio_runtime();
    let directory = tempfile::tempdir().expect("temporary directory");
    let data_path = directory.path().join("data.parquet");
    let file_size = write(&data_path);
    let offsets = row_group_offsets(&data_path);
    let context = read_context(&runtime);
    let access_binding = read_binding(&runtime);
    let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
    Harness {
        _runtime: runtime,
        _directory: directory,
        binding: access_binding,
        context,
        footers: Arc::new(ParquetFooterCache::new()),
        delete_manager,
        file_size,
        offsets,
        file_name: data_path.to_string_lossy().to_string(),
    }
}

impl Harness {
    /// The split's page stream, driven on the harness runtime.
    fn page_source(
        &self,
        split: &IcebergSplit,
        handle: &IcebergTableHandle,
        columns: &[IcebergColumnHandle],
    ) -> Result<DrivenStream<'_>, ConnectorError> {
        self.page_source_with_mode(split, handle, columns, DeleteEvaluationMode::ExcludeDeleted)
    }

    fn page_source_with_mode(
        &self,
        split: &IcebergSplit,
        handle: &IcebergTableHandle,
        columns: &[IcebergColumnHandle],
        delete_mode: DeleteEvaluationMode,
    ) -> Result<DrivenStream<'_>, ConnectorError> {
        let relation = IcebergReadRelation::of_table(handle, split.partition_spec_id())?;
        let budget = ConnectorPollBudget::new();
        let request = IcebergPageSourceRequest {
            relation: &relation,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode,
            footers: Arc::clone(&self.footers),
            access_binding: self.binding.clone(),
            context: self.context.clone(),
            cache: None,
            budget: ReaderSizing {
                max_read_rows: NonZeroUsize::new(1024).expect("nonzero"),
                max_read_bytes: 8 * 1024 * 1024,
            },
            reader_options: FileReaderOptions {
                parquet_interpretation_policy:
                    novarocks_fs::ParquetInterpretationPolicy::DerivedByteRepresentation {
                        preserve_top_level_dictionary: false,
                    },
                ..Default::default()
            },
            scheduled_split_sequence_id: 0,
            dynamic_filter: Arc::new(
                novarocks_spi::connector::read_stack::CompleteAllDynamicFilter::new(
                    std::collections::BTreeSet::new(),
                ),
            ) as Arc<IcebergDynamicFilter>,
            prepared_input: None,
            pending_preparation_control: None,
            output_projection: None,
        };
        let stream = create_iceberg_page_stream(request, &budget)?;
        Ok(DrivenStream::new(&self._runtime, stream, budget))
    }
}

fn drain_ids(source: &mut DrivenStream<'_>) -> Vec<i64> {
    let mut ids = Vec::new();
    while !source.is_finished() {
        let Some(page) = source.next_page().expect("page") else {
            continue;
        };
        let (rows, columns) = page.into_columns().expect("materialize");
        assert_eq!(columns[0].len(), rows);
        let values = columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ids");
        ids.extend(values.values().iter().copied());
    }
    ids
}

/// Drain every page and return the first `count` output columns as i64.
fn drain_i64_columns(source: &mut DrivenStream<'_>, count: usize) -> Vec<Vec<i64>> {
    let mut out = vec![Vec::new(); count];
    while !source.is_finished() {
        let Some(page) = source.next_page().expect("page") else {
            continue;
        };
        let (rows, columns) = page.into_columns().expect("materialize");
        for (ordinal, values) in out.iter_mut().enumerate() {
            let column = columns[ordinal]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 column");
            assert_eq!(column.len(), rows);
            assert_eq!(column.null_count(), 0, "column {ordinal} reports no nulls");
            values.extend(column.values().iter().copied());
        }
    }
    out
}

/// The handle a scan names a hidden metadata column with. It mirrors what
/// the boundary publishes: typed and NULL-contracted by the column itself.
fn metadata_handle(metadata: IcebergMetadataColumn) -> IcebergColumnHandle {
    IcebergColumnHandle::base_column(&NestedField::new(
        metadata.field_id(),
        metadata.column_name(),
        Type::Primitive(metadata.declared_type()),
        !metadata.nullable(),
    ))
    .expect("metadata column handle")
}

fn id_column(schema: &IcebergSchema) -> Vec<IcebergColumnHandle> {
    vec![IcebergColumnHandle::base_column_of(schema, 1).expect("id handle")]
}

// ------------------------------------------------------ dynamic filtering

/// A dynamic filter a test drives directly.
///
/// It answers exactly what the test tells it to and records every question
/// it was asked, so a test can tell "never consulted" apart from "consulted
/// and kept".
struct TestDynamicFilter {
    covered: std::collections::BTreeSet<IcebergColumnHandle>,
    state: Mutex<TestFilterState>,
    asked: Mutex<Vec<ColumnValueBounds>>,
}

struct TestFilterState {
    /// Row groups whose minimum id is inside this set are impossible.
    rejected_minimums: Vec<i64>,
    /// When set, every row group is impossible.
    reject_everything: bool,
    answer_unknown: bool,
    complete: bool,
}

impl TestDynamicFilter {
    fn new(covered_field_id: i32, schema: &IcebergSchema) -> Arc<Self> {
        let handle = IcebergColumnHandle::base_column_of(schema, covered_field_id).expect("handle");
        Arc::new(Self {
            covered: std::collections::BTreeSet::from([handle]),
            state: Mutex::new(TestFilterState {
                rejected_minimums: Vec::new(),
                reject_everything: false,
                answer_unknown: false,
                complete: false,
            }),
            asked: Mutex::new(Vec::new()),
        })
    }

    fn reject_row_groups_starting_at(&self, minimums: &[i64]) {
        self.state.lock().expect("filter state").rejected_minimums = minimums.to_vec();
    }

    fn reject_everything(&self) {
        self.state.lock().expect("filter state").reject_everything = true;
    }

    fn answer_unknown(&self) {
        self.state.lock().expect("filter state").answer_unknown = true;
    }

    fn questions(&self) -> Vec<ColumnValueBounds> {
        self.asked.lock().expect("asked").clone()
    }
}

impl DynamicFilter<IcebergColumnHandle> for TestDynamicFilter {
    fn columns_covered(&self) -> &std::collections::BTreeSet<IcebergColumnHandle> {
        &self.covered
    }

    fn current_predicate(&self) -> TupleDomain<IcebergColumnHandle> {
        TupleDomain::all()
    }

    fn is_complete(&self) -> bool {
        self.state.lock().expect("filter state").complete
    }

    fn is_awaitable(&self) -> bool {
        false
    }

    fn bounds_may_match(
        &self,
        column: &IcebergColumnHandle,
        bounds: &ColumnValueBounds,
    ) -> BoundsMatch {
        self.asked.lock().expect("asked").push(bounds.clone());
        if !self.covered.contains(column) {
            return BoundsMatch::Unknown;
        }
        let state = self.state.lock().expect("filter state");
        if state.answer_unknown {
            return BoundsMatch::Unknown;
        }
        if state.reject_everything {
            return BoundsMatch::Impossible;
        }
        let Some(ConnectorValue::BigInt(min)) = bounds.min else {
            return BoundsMatch::Unknown;
        };
        if state.rejected_minimums.contains(&min) {
            BoundsMatch::Impossible
        } else {
            BoundsMatch::Possible
        }
    }
}

impl Harness {
    fn page_source_with_filter(
        &self,
        split: &IcebergSplit,
        handle: &IcebergTableHandle,
        columns: &[IcebergColumnHandle],
        dynamic_filter: Arc<IcebergDynamicFilter>,
        scheduled_split_sequence_id: u64,
    ) -> Result<DrivenStream<'_>, ConnectorError> {
        let relation = IcebergReadRelation::of_table(handle, split.partition_spec_id())?;
        let budget = ConnectorPollBudget::new();
        let request = IcebergPageSourceRequest {
            relation: &relation,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::clone(&self.footers),
            access_binding: self.binding.clone(),
            context: self.context.clone(),
            cache: None,
            budget: ReaderSizing {
                max_read_rows: NonZeroUsize::new(1024).expect("nonzero"),
                max_read_bytes: 8 * 1024 * 1024,
            },
            reader_options: FileReaderOptions {
                parquet_interpretation_policy:
                    novarocks_fs::ParquetInterpretationPolicy::DerivedByteRepresentation {
                        preserve_top_level_dictionary: false,
                    },
                ..Default::default()
            },
            scheduled_split_sequence_id,
            dynamic_filter,
            prepared_input: None,
            pending_preparation_control: None,
            output_projection: None,
        };
        let stream = create_iceberg_page_stream(request, &budget)?;
        Ok(DrivenStream::new(&self._runtime, stream, budget))
    }
}

fn whole_file_split(harness: &Harness, groups: usize) -> IcebergSplit {
    build_split(
        &harness.file_name,
        SplitOptions::whole_file(harness.file_size, (groups * ROWS_PER_GROUP) as i64),
    )
}

#[test]
fn a_pruned_row_group_is_named_by_its_split_sequence_and_ordinal() {
    let live = LiveDynamicFilter::new(
        Arc::new(
            novarocks_spi::connector::read_stack::CompleteAllDynamicFilter::new(
                std::collections::BTreeSet::new(),
            ),
        ) as Arc<IcebergDynamicFilter>,
        41,
    );
    assert_eq!(
        live.row_group(2),
        DynamicFilterRowGroupId {
            scheduled_split_sequence_id: 41,
            row_group_ordinal: 2,
        }
    );
    assert_eq!(live.scheduled_split_sequence_id(), 41);
}

#[test]
fn a_filter_present_before_the_first_row_group_prunes_a_disjoint_one() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    // The first row group holds ids 0..3, so rejecting minimum 0 rejects it.
    filter.reject_row_groups_starting_at(&[0]);
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            7,
        )
        .expect("page source");
    let ids = drain_ids(&mut source);
    // Rows that were never read cannot appear. This page source never drops
    // a row for the dynamic filter, so a missing row group proves a skip.
    assert_eq!(
        ids,
        (ROWS_PER_GROUP as i64..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>()
    );
    assert!(
        filter
            .questions()
            .iter()
            .any(|bounds| bounds.min == Some(ConnectorValue::BigInt(0))),
        "the first row group's own bounds must be the ones judged"
    );
}

#[test]
fn a_filter_arriving_mid_split_prunes_only_the_not_yet_read_row_groups() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");

    // The first row group is read while nothing is constrained.
    let first = source
        .next_page()
        .expect("page")
        .expect("the first row group is readable");
    assert_eq!(first.position_count(), ROWS_PER_GROUP);

    // The filter tightens after that row group was already decoded.
    filter.reject_everything();
    let rest = drain_ids(&mut source);
    assert!(
        rest.is_empty(),
        "a filter that arrives mid-split must prune every row group that has not been read"
    );
    assert!(source.is_finished());
}

#[test]
fn a_prepared_successor_is_rechecked_against_a_late_filter() {
    let mut harness = harness_of(write_large_data_file);
    assert!(harness.file_size > novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES);
    harness.context.range = Some(
        FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::clone(&harness.context.io_spawner),
            harness._runtime.handle().clone(),
        )
        .bind(
            FileRangeScope::for_test(1, 0, 1, 2, 0, 3).unwrap(),
            novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test(),
        ),
    );
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    assert_eq!(
        source
            .next_page()
            .expect("page")
            .expect("first run")
            .position_count(),
        ROWS_PER_GROUP
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let progress = source
            .advance_successor_preparation(2 * 1024 * 1024, 1)
            .expect("prepare next run");
        if progress == ConnectorPreparationProgress::Ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "successor preparation did not finish"
        );
        std::thread::yield_now();
    }
    assert!(source.successor_preparation_input_bytes() > 0);
    filter.reject_everything();
    assert!(drain_ids(&mut source).is_empty());
    source.close().expect("drain source and successor");
    assert_eq!(source.successor_preparation_input_bytes(), 0);
}

#[test]
fn an_answer_of_unknown_keeps_every_row_group() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    filter.answer_unknown();
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert_eq!(ids, (0..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>());
    assert!(
        !filter.questions().is_empty(),
        "an undecided answer is still an answer that was asked for"
    );
}

#[test]
fn a_covered_column_carries_this_row_groups_own_statistics() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    filter.answer_unknown();
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    let _ = drain_ids(&mut source);
    let questions = filter.questions();
    let first = questions
        .iter()
        .find(|bounds| bounds.min == Some(ConnectorValue::BigInt(0)))
        .expect("the first row group is judged on its own bounds");
    assert_eq!(first.max, Some(ConnectorValue::BigInt(3)));
    assert_eq!(first.value_count, Some(ROWS_PER_GROUP as u64));
    assert_eq!(first.null_count, Some(0));
    assert!(first.bounds_are_exact);
}

#[test]
fn a_covered_column_absent_from_the_file_is_never_asked_about() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    // Field id 99 exists in no Parquet leaf of this file, so it can never
    // contribute a prune and must never be asked about.
    let absent = IcebergSchema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            99,
            "absent",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("a schema naming a column this file does not have");
    let filter = TestDynamicFilter::new(99, &absent);
    filter.reject_everything();
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert_eq!(ids.len(), 3 * ROWS_PER_GROUP);
    assert!(
        filter.questions().is_empty(),
        "a covered column with no leaf in this file is never a question"
    );
}

#[test]
fn a_covered_column_the_scan_does_not_project_still_prunes() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    // `region` is a real column of the file that this scan does not output.
    // Its footer statistics can still prove a row group impossible.
    let filter = TestDynamicFilter::new(2, &schema);
    filter.reject_everything();
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    assert!(drain_ids(&mut source).is_empty());
}

#[test]
fn an_entirely_pruned_split_reads_no_data() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    filter.reject_everything();
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            3,
        )
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert!(ids.is_empty());
    assert!(source.is_finished());
    assert_eq!(
        source.metrics().completed_bytes,
        0,
        "a split whose every row group is proven impossible opens no reader"
    );
}

#[test]
fn a_closed_scan_reads_no_footer_and_asks_the_filter_nothing() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    let mut source = harness
        .page_source_with_filter(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            0,
        )
        .expect("page source");
    source.close().expect("close");
    assert!(source.is_finished());
    assert!(source.next_page().expect("no page").is_none());
    assert!(
        filter.questions().is_empty(),
        "a closed scan must not consult the filter"
    );
    assert!(
        harness.footers.is_empty().expect("footer cache"),
        "a closed scan must not read a footer"
    );
}

#[test]
fn footer_cache_keeps_file_identity_and_checks_stop_on_hit() {
    let harness = harness(1);
    let access = harness
        .binding
        .resolve_access(&harness.file_name)
        .expect("access");
    harness
        .footers
        .footer(
            &access,
            &harness.context,
            &harness.file_name,
            harness.file_size,
            novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint,
        )
        .expect("first footer");
    let replacement_size = write_data_file(Path::new(&harness.file_name), 2);
    assert_ne!(replacement_size, harness.file_size);
    harness
        .footers
        .footer(
            &access,
            &harness.context,
            &harness.file_name,
            replacement_size,
            novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint,
        )
        .expect("different identity footer");
    assert_eq!(harness.footers.len().expect("footer count"), 2);

    harness.context.cancellation.cancel();
    let error = harness
        .footers
        .footer(
            &access,
            &harness.context,
            &harness.file_name,
            harness.file_size,
            novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint,
        )
        .expect_err("stopped request cannot use cached footer");
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
}

#[test]
fn a_whole_file_split_reads_every_row_with_absolute_positions() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = build_split(
        &harness.file_name,
        SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64),
    );
    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert_eq!(ids, (0..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>());
    assert!(source.is_finished());
}

#[test]
fn byte_range_selection_takes_a_row_group_at_the_lower_bound_and_not_at_the_upper() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let records = (3 * ROWS_PER_GROUP) as i64;

    // A range that starts exactly on the second row group's first byte and
    // ends exactly on the third's takes only the second: the range is
    // half-open.
    let start = harness.offsets[1];
    let end = harness.offsets[2];
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            start: start as i64,
            length: (end - start) as i64,
            file_size: harness.file_size as i64,
            file_record_count: records,
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert_eq!(
        ids,
        (ROWS_PER_GROUP as i64..(2 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>(),
        "the row group starting at the exclusive upper bound must not be read"
    );
}

/// `_row_id`: `first_row_id` plus the row's absolute position in its file.
fn row_id_column() -> IcebergColumnHandle {
    IcebergColumnHandle::try_new(
        crate::typed_read::column_handle::IcebergColumnHandleParams {
            base_column_identity: crate::typed_read::column_handle::ColumnIdentity::try_new(
                crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                crate::row_lineage_synth::ICEBERG_ROW_ID_COL,
                crate::typed_read::column_handle::ColumnIdentityCategory::Primitive,
                Vec::new(),
            )
            .expect("identity"),
            base_type_json: "\"long\"".to_owned(),
            field_id_path: Vec::new(),
            type_json: "\"long\"".to_owned(),
            nullable: false,
            comment: None,
        },
    )
    .expect("row id handle")
}

#[test]
fn absolute_row_positions_survive_row_group_pruning() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let records = (3 * ROWS_PER_GROUP) as i64;
    let start = harness.offsets[2];
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            start: start as i64,
            length: (harness.file_size - start) as i64,
            file_size: harness.file_size as i64,
            file_record_count: records,
            first_row_id: Some(1_000),
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    // `_row_id` is `first_row_id + absolute position`, so it proves the
    // positions were not renumbered from the start of the split.
    let mut source = harness
        .page_source(&split, &handle, &[row_id_column()])
        .expect("page source");
    let ids = drain_ids(&mut source);
    assert_eq!(
        ids,
        (1_000 + 2 * ROWS_PER_GROUP as i64..1_000 + 3 * ROWS_PER_GROUP as i64).collect::<Vec<_>>()
    );
}

/// A rewrite preserves row history only if the reader reads it back.
///
/// The data file materializes both reserved row-lineage columns, which is
/// what a rewritten v3 file looks like. Synthesizing over them would report
/// `first_row_id + position` and the rewriting snapshot's own sequence
/// number for every row -- silently claiming that rows the rewrite merely
/// copied had changed. Each column falls back on its own, so a row may
/// inherit one and keep the other.
#[test]
fn a_stored_row_lineage_column_is_read_rather_than_synthesized() {
    let harness = harness_of(write_row_lineage_data_file);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            first_row_id: Some(1_000),
            ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
        },
    );
    let mut source = harness
        .page_source(
            &split,
            &handle,
            &[
                metadata_handle(IcebergMetadataColumn::RowId),
                metadata_handle(IcebergMetadataColumn::LastUpdatedSequenceNumber),
            ],
        )
        .expect("page source");
    let columns = drain_i64_columns(&mut source, 2);

    // Rows 0..3 store a `_row_id`; row 3 stores none and inherits
    // `first_row_id + position` = 1000 + 3.
    assert_eq!(columns[0], vec![70, 71, 72, 1_003], "_row_id");
    // Rows 0 and 2 store a sequence number; rows 1 and 3 inherit the data
    // file's own, which `build_split` freezes at 3.
    assert_eq!(
        columns[1],
        vec![1, 3, 2, 3],
        "_last_updated_sequence_number"
    );
}

#[test]
fn none_is_not_end_of_stream_and_close_is_idempotent() {
    let harness = harness(1);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = build_split(
        &harness.file_name,
        SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64),
    );
    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");

    assert!(!source.is_finished(), "a fresh page source is not finished");
    let first = source.next_page().expect("first page");
    assert!(first.is_some());
    assert!(
        !source.is_finished(),
        "a produced page never terminates the source on its own"
    );
    let second = source.next_page().expect("second call");
    assert!(second.is_none(), "the reader is drained");
    assert!(source.is_finished(), "only is_finished is terminal");

    source.close().expect("close");
    source.close().expect("close is idempotent");
    assert!(source.next_page().expect("after close").is_none());
}

#[test]
fn a_zero_column_scan_still_counts_its_rows() {
    let harness = harness(2);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = build_split(
        &harness.file_name,
        SplitOptions::whole_file(harness.file_size, (2 * ROWS_PER_GROUP) as i64),
    );
    let mut source = harness
        .page_source(&split, &handle, &[])
        .expect("page source");
    let mut positions = 0usize;
    while !source.is_finished() {
        let Some(page) = source.next_page().expect("page") else {
            continue;
        };
        assert_eq!(page.channel_count(), 0, "a zero-column page is legal");
        positions += page.position_count();
    }
    assert_eq!(positions, 2 * ROWS_PER_GROUP);
}

#[test]
fn the_partition_only_fast_path_never_opens_the_data_file() {
    let harness = harness(2);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, true);
    let records = (2 * ROWS_PER_GROUP) as i64;
    let region_value = "x".repeat(400 * 1024);
    let split = build_split(
        "s3://bucket/this/file/does/not/exist.parquet",
        SplitOptions {
            partition_data_json: serde_json::json!({
                "version": 1,
                "values": [{ "string": region_value }]
            })
            .to_string(),
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region handle");
    let mut source = harness
        .page_source(&split, &handle, &[region])
        .expect("page source");

    let mut positions = 0usize;
    while !source.is_finished() {
        let Some(page) = source.next_page().expect("page") else {
            continue;
        };
        let column = page.block(0).expect("partition constant").clone();
        let regions = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 constant");
        // Two full constants fit the construction target, but three do not.
        assert_eq!(page.position_count(), 2);
        for row in 0..regions.len() {
            assert_eq!(regions.value(row), region_value);
        }
        positions += page.position_count();
    }
    assert_eq!(
        positions as i64, records,
        "the fast path must account for every record of the file"
    );
    assert!(
        harness.footers.is_empty().expect("footer cache"),
        "the fast path reads no footer at all"
    );
}

#[test]
fn the_partition_only_fast_path_may_emit_zero_column_pages() {
    let harness = harness(1);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, true);
    let split = build_split(
        "s3://bucket/absent.parquet",
        SplitOptions {
            partition_data_json: r#"{"version":1,"values":[{"string":"emea"}]}"#.to_owned(),
            ..SplitOptions::whole_file(harness.file_size, 7)
        },
    );
    let mut source = harness
        .page_source(&split, &handle, &[])
        .expect("page source");
    let page = source
        .next_page()
        .expect("page")
        .expect("a zero-column page is still a page");
    assert_eq!(page.channel_count(), 0);
    assert_eq!(page.position_count(), 7);
    // A stream reports its end when it is polled again.
    assert!(source.next_page().expect("end of stream").is_none());
    assert!(source.is_finished());
}

#[test]
fn orc_and_avro_are_rejected_at_page_source_admission() {
    let harness = harness(1);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                format,
                ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
            },
        );
        let error = harness
            .page_source(&split, &handle, &id_column(&schema))
            .err()
            .expect("only parquet is implemented");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{format:?}");
    }
}

#[test]
fn decryption_material_is_rejected_without_leaking_it() {
    let harness = harness(1);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let secret = b"super-secret-key-metadata".to_vec();
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            decryption: Some(
                ParquetFileDecryptionData::try_new(secret.clone(), Vec::new())
                    .expect("decryption material"),
            ),
            ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
        },
    );
    let error = harness
        .page_source(&split, &handle, &id_column(&schema))
        .err()
        .expect("modular encryption is not implemented");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    let rendered = format!("{} {:?}", error.message(), error);
    assert!(
        !rendered.contains("super-secret"),
        "key material must never reach a message: {rendered}"
    );
    // The redacted `Debug` of the material itself is the same guarantee.
    let material =
        ParquetFileDecryptionData::try_new(secret, Vec::new()).expect("decryption material");
    assert!(!format!("{material:?}").contains("super-secret"));
}

#[test]
fn a_delete_closure_adds_a_hidden_suffix_that_is_truncated_after_evaluation() {
    let harness = harness(2);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let records = (2 * ROWS_PER_GROUP) as i64;

    // An equality delete on `region` needs `region` in the page even though
    // the scan only asked for `id`.
    let delete_path = std::path::Path::new(&harness.file_name)
        .parent()
        .expect("data directory")
        .join("eq-delete.parquet");
    write_equality_delete(&delete_path, &["r2", "r5"]);
    let delete = IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        partition_spec_id: 0,
        partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
        content: IcebergDeleteFileContent::EqualityDeletes,
        path: delete_path.to_string_lossy().to_string(),
        format: IcebergFileFormat::Parquet,
        record_count: 2,
        file_size_in_bytes: fs::metadata(&delete_path).expect("stat").len() as i64,
        equality_field_ids: vec![2],
        row_position_lower_bound: None,
        row_position_upper_bound: None,
        data_sequence_number: 9,
        content_offset: None,
        content_size_in_bytes: None,
        referenced_data_file: None,
        decryption_data: None,
    })
    .expect("delete descriptor");

    let split = build_split(
        &harness.file_name,
        SplitOptions {
            deletes: vec![delete],
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");

    let mut ids = Vec::new();
    while !source.is_finished() {
        let Some(page) = source.next_page().expect("page") else {
            continue;
        };
        assert_eq!(
            page.channel_count(),
            1,
            "the hidden delete suffix must be dropped before the page leaves"
        );
        let (_, columns) = page.into_columns().expect("materialize");
        let values = columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 ids");
        ids.extend(values.values().iter().copied());
    }
    assert_eq!(ids, vec![0, 1, 3, 4, 6, 7]);
}

fn write_equality_delete(path: &Path, regions: &[&str]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("region", DataType::Utf8, true).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(regions.to_vec()))],
    )
    .expect("build equality delete batch");
    let file = fs::File::create(path).expect("create delete file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write delete batch");
    writer.close().expect("close parquet writer");
}

#[test]
fn a_bounded_split_records_its_half_open_row_position_window() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let records = (3 * ROWS_PER_GROUP) as i64;
    let start = harness.offsets[1];
    let end = harness.offsets[2];
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            start: start as i64,
            length: (end - start) as i64,
            file_size: harness.file_size as i64,
            file_record_count: records,
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    let mut window = ReaderPageSourceWithRowPositions::default();
    window
        .observe(
            &UInt64Array::from((4_u64..8).collect::<Vec<_>>()),
            "data.parquet",
        )
        .expect("observe");
    assert_eq!(window.start_row_position, Some(4));
    assert_eq!(window.end_row_position, Some(8));
    assert!(
        window
            .observe(&UInt64Array::from(vec![6_u64]), "data.parquet")
            .is_err(),
        "a revisited row position is corrupt"
    );

    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");
    let _ = drain_ids(&mut source);
}

#[test]
fn row_predicate_values_preserve_int16_and_timestamp_milliseconds() {
    let small: ArrayRef = Arc::new(Int16Array::from(vec![-32_768, 32_767]));
    assert_eq!(
        connector_value_at(&small, ConnectorValueType::SmallInt, 0, "small")
            .expect("smallint value"),
        ConnectorValue::SmallInt(i16::MIN)
    );

    let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        -1_704_067_200_123,
        1_704_067_200_123,
    ]));
    assert_eq!(
        connector_value_at(
            &millis,
            ConnectorValueType::TimestampMillis,
            1,
            "created_at",
        )
        .expect("millisecond value"),
        ConnectorValue::TimestampMillis(1_704_067_200_123)
    );
    assert!(
        connector_value_at(
            &millis,
            ConnectorValueType::TimestampMicros,
            0,
            "created_at",
        )
        .is_err()
    );
}

// --- page streams -------------------------------------------------------

impl Harness {
    fn page_stream_with(
        &self,
        split: &IcebergSplit,
        handle: &IcebergTableHandle,
        columns: &[IcebergColumnHandle],
        dynamic_filter: Arc<IcebergDynamicFilter>,
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        let relation = IcebergReadRelation::of_table(handle, split.partition_spec_id())?;
        create_iceberg_page_stream(
            IcebergPageSourceRequest {
                relation: &relation,
                split,
                columns,
                delete_manager: Arc::clone(&self.delete_manager),
                delete_mode: DeleteEvaluationMode::ExcludeDeleted,
                footers: Arc::clone(&self.footers),
                access_binding: self.binding.clone(),
                context: self.context.clone(),
                cache: None,
                budget: ReaderSizing {
                    max_read_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_read_bytes: 8 * 1024 * 1024,
                },
                reader_options: FileReaderOptions {
                    parquet_interpretation_policy:
                        novarocks_fs::ParquetInterpretationPolicy::DerivedByteRepresentation {
                            preserve_top_level_dictionary: false,
                        },
                    ..Default::default()
                },
                scheduled_split_sequence_id: 0,
                dynamic_filter,
                prepared_input: None,
                pending_preparation_control: None,
                output_projection: None,
            },
            budget,
        )
    }
}

fn no_dynamic_filter() -> Arc<IcebergDynamicFilter> {
    Arc::new(
        novarocks_spi::connector::read_stack::CompleteAllDynamicFilter::new(
            std::collections::BTreeSet::new(),
        ),
    ) as Arc<IcebergDynamicFilter>
}

fn page_ids(page: SourcePage) -> Vec<i64> {
    let (rows, columns) = page.into_columns().expect("materialize");
    assert_eq!(columns[0].len(), rows);
    columns[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 ids")
        .values()
        .to_vec()
}

/// Drains a stream on the harness runtime, one host turn per page.
fn drain_stream(
    harness: &Harness,
    stream: &mut OwnedConnectorPageStream,
    budget: &ConnectorPollBudget,
) -> Vec<i64> {
    use futures::StreamExt;
    harness._runtime.block_on(async {
        let mut ids = Vec::new();
        loop {
            budget.refill(1024);
            match stream.next().await {
                Some(page) => ids.extend(page_ids(page.expect("page"))),
                None => return ids,
            }
        }
    })
}

fn equality_delete_of(harness: &Harness, regions: &[&str]) -> IcebergDeleteFile {
    let delete_path = std::path::Path::new(&harness.file_name)
        .parent()
        .expect("data directory")
        .join("eq-delete.parquet");
    write_equality_delete(&delete_path, regions);
    IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        partition_spec_id: 0,
        partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
        content: IcebergDeleteFileContent::EqualityDeletes,
        path: delete_path.to_string_lossy().to_string(),
        format: IcebergFileFormat::Parquet,
        record_count: regions.len() as i64,
        file_size_in_bytes: fs::metadata(&delete_path).expect("stat").len() as i64,
        equality_field_ids: vec![2],
        row_position_lower_bound: None,
        row_position_upper_bound: None,
        data_sequence_number: 9,
        content_offset: None,
        content_size_in_bytes: None,
        referenced_data_file: None,
        decryption_data: None,
    })
    .expect("delete descriptor")
}

#[test]
fn a_page_stream_reads_the_rows_its_page_source_reads() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let mut source = harness
        .page_source(&split, &handle, &id_column(&schema))
        .expect("page source");
    let expected = drain_ids(&mut source);
    let budget = ConnectorPollBudget::new();
    let mut stream = harness
        .page_stream_with(
            &split,
            &handle,
            &id_column(&schema),
            no_dynamic_filter(),
            &budget,
        )
        .expect("page stream");
    assert_eq!(drain_stream(&harness, &mut stream, &budget), expected);
    assert_eq!(expected.len(), 3 * ROWS_PER_GROUP);
    harness
        ._runtime
        .block_on(stream.close())
        .expect("closing a drained stream");

    // A delete closure is judged over the hidden suffix and dropped, in
    // the stream exactly as in the page source.
    let deleted = build_split(
        &harness.file_name,
        SplitOptions {
            deletes: vec![equality_delete_of(&harness, &["r2", "r5"])],
            ..SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64)
        },
    );
    let mut stream = harness
        .page_stream_with(
            &deleted,
            &handle,
            &id_column(&schema),
            no_dynamic_filter(),
            &budget,
        )
        .expect("page stream");
    let ids = drain_stream(&harness, &mut stream, &budget);
    assert_eq!(
        ids,
        (0..(3 * ROWS_PER_GROUP) as i64)
            .filter(|id| *id != 2 && *id != 5)
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_filter_arriving_mid_split_prunes_the_stream_s_later_row_groups() {
    use futures::StreamExt;

    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let filter = TestDynamicFilter::new(1, &schema);
    let budget = ConnectorPollBudget::new();
    budget.refill(1024);
    let mut stream = harness
        .page_stream_with(
            &split,
            &handle,
            &id_column(&schema),
            Arc::clone(&filter) as Arc<IcebergDynamicFilter>,
            &budget,
        )
        .expect("page stream");
    let first = harness
        ._runtime
        .block_on(stream.next())
        .expect("a page")
        .expect("the first row group is readable");
    assert_eq!(first.position_count(), ROWS_PER_GROUP);
    filter.reject_everything();
    assert!(
        drain_stream(&harness, &mut stream, &budget).is_empty(),
        "a filter that arrives mid-split prunes every row group not yet read"
    );
}

#[test]
fn a_split_of_filtered_batches_yields_when_its_turn_runs_out() {
    use futures::StreamExt;

    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let regions = (0..3 * ROWS_PER_GROUP)
        .map(|id| format!("r{id}"))
        .collect::<Vec<_>>();
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            deletes: vec![equality_delete_of(
                &harness,
                &regions.iter().map(String::as_str).collect::<Vec<_>>(),
            )],
            ..SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64)
        },
    );
    let budget = ConnectorPollBudget::new();
    let mut stream = harness
        .page_stream_with(
            &split,
            &handle,
            &id_column(&schema),
            no_dynamic_filter(),
            &budget,
        )
        .expect("page stream");
    // Driven the way a driver runs it: every poll is one turn with room
    // for one decoded batch, and a Pending turn ends until the stream's
    // own wake -- a spent turn or a finished read -- starts the next.
    harness._runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(30),
            std::future::poll_fn(|cx| {
                budget.refill(1);
                stream.poll_next_unpin(cx).map(|page| {
                    if let Some(page) = page {
                        panic!(
                            "every row is deleted, got {:?}",
                            page.map(|page| page.position_count())
                        );
                    }
                })
            }),
        )
        .await
        .expect("the stream ends");
    });
    assert!(
        budget.exhaustions() >= 2,
        "three all-deleted batches with one unit per turn must yield, saw {}",
        budget.exhaustions()
    );
}

#[test]
fn closing_a_stream_seals_its_split_alone_and_waits_for_its_held_read() {
    use futures::StreamExt;

    let mut harness = harness_of(write_large_data_file);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let spawner = Arc::new(GatedFileIoSpawner {
        handle: harness._runtime.handle().clone(),
        gate: Arc::clone(&gate),
        started: std::sync::atomic::AtomicUsize::new(0),
    });
    let task_operations =
        novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test();
    harness.context.range = Some(
        FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            spawner.clone() as Arc<dyn FileIoSpawner>,
            harness._runtime.handle().clone(),
        )
        .bind(
            FileRangeScope::for_test(1, 0, 1, 2, 0, 3).unwrap(),
            task_operations.clone(),
        ),
    );
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let budget = ConnectorPollBudget::new();
    budget.refill(1024);
    let mut stream = harness
        .page_stream_with(
            &split,
            &handle,
            &id_column(&schema),
            no_dynamic_filter(),
            &budget,
        )
        .expect("page stream");
    // Let the footer through (its tail, then its metadata) and hold the
    // first data read.
    gate.add_permits(2);
    harness._runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while spawner.started.load(std::sync::atomic::Ordering::SeqCst) < 3 {
                assert!(futures::poll!(stream.next()).is_pending());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the first data read is in flight");
    });
    assert!(task_operations.live_operations() > 0);

    // Closed the way a driver closes it: inside the runtime's context.
    let mut closed = {
        let _context = harness._runtime.enter();
        stream.close()
    };
    harness._runtime.block_on(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut closed)
                .await
                .is_err(),
            "the split has not exited while its read is held"
        );
    });
    assert!(
        !task_operations.is_sealed(),
        "closing one split leaves its task source open"
    );
    gate.add_permits(64);
    harness
        ._runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), closed).await })
        .expect("the split exits once its read does")
        .expect("a stopped read exits cleanly");
    assert_eq!(task_operations.live_operations(), 0);
}

/// Routes a harness's data and delete reads through a range service whose
/// every file task is spawned by `spawner`.
fn route_through(harness: &mut Harness, spawner: Arc<dyn FileIoSpawner>) {
    harness.context.io_spawner = Arc::clone(&spawner);
    harness.context.range = Some(
        FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            spawner,
            harness._runtime.handle().clone(),
        )
        .bind(
            FileRangeScope::for_test(1, 0, 1, 2, 0, 3).unwrap(),
            novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test(),
        ),
    );
    harness.delete_manager = Arc::new(DeleteManager::new(
        harness.binding.clone(),
        harness.context.clone(),
    ));
}

#[test]
fn each_open_phase_of_a_stream_waits_on_its_own_read() {
    use futures::StreamExt;

    let mut harness = harness_of(write_large_data_file);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let spawner = Arc::new(GatedFileIoSpawner {
        handle: harness._runtime.handle().clone(),
        gate: Arc::clone(&gate),
        started: std::sync::atomic::AtomicUsize::new(0),
    });
    route_through(&mut harness, spawner.clone());
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    // The large file's region of row 1, so the delete phase has a row to
    // remove.
    let region = format!("1{}", "x".repeat(24 * 1024));
    let split = build_split(
        &harness.file_name,
        SplitOptions {
            deletes: vec![equality_delete_of(&harness, &[&region])],
            ..SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64)
        },
    );
    let budget = ConnectorPollBudget::new();
    budget.refill(1024);
    let mut stream = harness
        .page_stream_with(
            &split,
            &handle,
            &id_column(&schema),
            no_dynamic_filter(),
            &budget,
        )
        .expect("page stream");
    let started = || spawner.started.load(std::sync::atomic::Ordering::SeqCst);
    let ids = harness._runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(60), async {
            let mut ids = Vec::new();
            let mut released = 0;
            loop {
                match futures::poll!(stream.next()) {
                    std::task::Poll::Ready(Some(page)) => ids.extend(page_ids(page.expect("page"))),
                    std::task::Poll::Ready(None) => return (ids, released),
                    std::task::Poll::Pending => {
                        // A pending stream waits on the one read it
                        // holds, on a read already let through, or on
                        // its split's exit -- never on two held reads.
                        if started() > released {
                            assert_eq!(started(), released + 1, "one phase reads at a time");
                            gate.add_permits(1);
                            released += 1;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        })
        .await
        .expect("the stream ends")
    });
    let (ids, released) = ids;
    assert_eq!(
        ids,
        (0..(3 * ROWS_PER_GROUP) as i64)
            .filter(|id| *id != 1)
            .collect::<Vec<_>>()
    );
    // The delete file, the footer's tail and metadata, then one data read
    // per row group.
    assert!(released >= 6, "saw {released} managed reads");
}

#[test]
fn a_page_stream_issues_the_reads_its_page_source_issues() {
    let count_reads = |stream: bool| {
        let mut harness = harness_of(write_large_data_file);
        let spawner = Arc::new(CountingFileIoSpawner {
            handle: harness._runtime.handle().clone(),
            spawned: std::sync::atomic::AtomicUsize::new(0),
        });
        route_through(&mut harness, spawner.clone());
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = whole_file_split(&harness, 3);
        let ids = if stream {
            let budget = ConnectorPollBudget::new();
            let mut stream = harness
                .page_stream_with(
                    &split,
                    &handle,
                    &id_column(&schema),
                    no_dynamic_filter(),
                    &budget,
                )
                .expect("page stream");
            drain_stream(&harness, &mut stream, &budget)
        } else {
            let mut source = harness
                .page_source(&split, &handle, &id_column(&schema))
                .expect("page source");
            drain_ids(&mut source)
        };
        (
            ids,
            spawner.spawned.load(std::sync::atomic::Ordering::SeqCst),
        )
    };
    let (source_ids, source_reads) = count_reads(false);
    let (stream_ids, stream_reads) = count_reads(true);
    assert_eq!(stream_ids, source_ids);
    assert!(source_reads > 0);
    assert_eq!(stream_reads, source_reads, "no GET is repeated or added");
}

/// A Puffin deletion vector naming `positions` of the harness's data file.
fn deletion_vector_of(harness: &Harness, positions: &[u64]) -> IcebergDeleteFile {
    let path = std::path::Path::new(&harness.file_name)
        .parent()
        .expect("data directory")
        .join("dv.puffin");
    let mut vector = crate::commit::DeletionVector::new();
    for position in positions {
        vector.insert(*position).expect("position");
    }
    let payload = vector.to_iceberg_payload().expect("payload");
    fs::write(&path, &payload).expect("write deletion vector");
    IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        partition_spec_id: 0,
        partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
        content: IcebergDeleteFileContent::PositionDeletes,
        path: path.to_string_lossy().to_string(),
        format: IcebergFileFormat::Puffin,
        record_count: positions.len() as i64,
        file_size_in_bytes: payload.len() as i64,
        equality_field_ids: Vec::new(),
        row_position_lower_bound: None,
        row_position_upper_bound: None,
        data_sequence_number: 9,
        content_offset: Some(0),
        content_size_in_bytes: Some(payload.len() as i64),
        referenced_data_file: Some(harness.file_name.clone()),
        decryption_data: None,
    })
    .expect("deletion vector descriptor")
}

#[test]
fn a_page_stream_applies_a_deletion_vector_and_keeps_absolute_positions() {
    let harness = harness(3);
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let records = (3 * ROWS_PER_GROUP) as i64;
    let budget = ConnectorPollBudget::new();
    // Each read as a stream first, so that the stream loads the split's
    // deletes, and then as a page source.
    let read_both_ways = |split: &IcebergSplit, columns: &[IcebergColumnHandle]| {
        let mut stream = harness
            .page_stream_with(split, &handle, columns, no_dynamic_filter(), &budget)
            .expect("page stream");
        let streamed = drain_stream(&harness, &mut stream, &budget);
        let mut source = harness
            .page_source(split, &handle, columns)
            .expect("page source");
        assert_eq!(streamed, drain_ids(&mut source));
        streamed
    };

    let deleted = build_split(
        &harness.file_name,
        SplitOptions {
            deletes: vec![deletion_vector_of(&harness, &[1, 5])],
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    assert_eq!(
        read_both_ways(&deleted, &id_column(&schema)),
        (0..records)
            .filter(|id| *id != 1 && *id != 5)
            .collect::<Vec<_>>()
    );

    // A split that starts at the last row group reads it at its absolute
    // positions, not renumbered from the split's start.
    let start = harness.offsets[2];
    let bounded = build_split(
        &harness.file_name,
        SplitOptions {
            start: start as i64,
            length: (harness.file_size - start) as i64,
            file_size: harness.file_size as i64,
            file_record_count: records,
            first_row_id: Some(1_000),
            ..SplitOptions::whole_file(harness.file_size, records)
        },
    );
    assert_eq!(
        read_both_ways(&bounded, &[row_id_column()]),
        (1_000 + 2 * ROWS_PER_GROUP as i64..1_000 + records).collect::<Vec<_>>()
    );
}

#[test]
fn a_second_stream_over_a_file_reuses_its_footer() {
    let mut harness = harness_of(write_large_data_file);
    let spawner = Arc::new(CountingFileIoSpawner {
        handle: harness._runtime.handle().clone(),
        spawned: std::sync::atomic::AtomicUsize::new(0),
    });
    route_through(&mut harness, spawner.clone());
    let schema = iceberg_schema();
    let handle = table_handle(&schema, false);
    let split = whole_file_split(&harness, 3);
    let budget = ConnectorPollBudget::new();
    let spawned = || spawner.spawned.load(std::sync::atomic::Ordering::SeqCst);
    let mut reads = Vec::new();
    for _ in 0..2 {
        let before = spawned();
        let mut stream = harness
            .page_stream_with(
                &split,
                &handle,
                &id_column(&schema),
                no_dynamic_filter(),
                &budget,
            )
            .expect("page stream");
        assert_eq!(
            drain_stream(&harness, &mut stream, &budget).len(),
            3 * ROWS_PER_GROUP
        );
        harness
            ._runtime
            .block_on(stream.close())
            .expect("closing a drained stream");
        reads.push(spawned() - before);
    }
    assert_eq!(harness.footers.len().expect("footer count"), 1);
    assert_eq!(
        reads[1], 3,
        "the second stream reads its three row groups and no footer: {reads:?}"
    );
    assert!(
        reads[0] > reads[1],
        "only the first read the footer: {reads:?}"
    );
}
