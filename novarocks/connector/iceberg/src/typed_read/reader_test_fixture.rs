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

//! Provider-owned real file fixture for downstream scan-consumer tests.

use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use novarocks_fs::{
    FileCancellation, FileReadContext, FileReaderOptions, FsAccessResolver,
    ParquetInterpretationPolicy, ReaderSizing, TokioFileIoRuntime, TokioFileIoSpawner,
    TokioRefreshExecutor,
};
use novarocks_spi::connector::read_stack::{
    CompleteAllDynamicFilter, ConnectorPollBudget, OwnedConnectorPageStream, SchemaTableName,
    SplitWeight, TupleDomain,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;

use crate::access_binding::IcebergReadBinding;
use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};

use super::page_source::create_iceberg_page_stream;
use super::runtime::IcebergReadLayout;
use super::split::{IcebergFileFormat, IcebergSplitParams};
use super::{
    DeleteEvaluationMode, DeleteManager, IcebergColumnHandle, IcebergPageSourceRequest,
    IcebergReadRelation, IcebergSplit, IcebergTableHandle, IcebergTableHandleParams,
    ParquetFooterCache,
};

pub const FIRST_VALUE: &str = "an actual dictionary value in the first column chunk";
pub const SECOND_VALUE: &str = "another dictionary value in the second column chunk";

/// Two column chunks, each yielding two source batches. Deliberately use a
/// view slot and the same compatible Utf8 dictionary-value backing as offsets.
pub fn open_view_scan(
    path: &Path,
    runtime: &tokio::runtime::Handle,
    budget: &ConnectorPollBudget,
    preserve_dictionary: bool,
) -> OwnedConnectorPageStream {
    let field = Field::new("s", DataType::Utf8, true)
        .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]);
    let physical_schema = Arc::new(ArrowSchema::new(vec![field]));
    let props = WriterProperties::builder()
        .set_dictionary_enabled(preserve_dictionary)
        .set_max_row_group_row_count(Some(8))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        physical_schema.clone(),
        Some(props),
    )
    .unwrap();
    for value in [FIRST_VALUE, SECOND_VALUE] {
        let column: ArrayRef = Arc::new(StringArray::from(vec![
            Some(value),
            None,
            Some(value),
            Some(value),
            Some(value),
            None,
            Some(value),
            Some(value),
        ]));
        writer
            .write(&RecordBatch::try_new(physical_schema.clone(), vec![column]).unwrap())
            .unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();
    let file_size = i64::try_from(std::fs::metadata(path).unwrap().len()).unwrap();

    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "s",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
        .unwrap();
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .unwrap();
    let domain =
        crate::delete_semantics::test_read_domain(&schema, std::slice::from_ref(&spec), 11);
    let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: SchemaTableName::try_new("fixture", "dictionary_scan").unwrap(),
        snapshot_id: Some(11),
        read_domain: Some(domain.clone()),
        table_schema_json: serde_json::to_string(&schema).unwrap(),
        spec_id: Some(0),
        partition_spec_jsons: [(0, serde_json::to_string(&spec).unwrap())].into(),
        format_version: 2,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: Default::default(),
        name_mapping_json: None,
        table_location: path.parent().unwrap().to_str().unwrap().to_owned(),
        storage_properties: Default::default(),
        pinned_data_files: None,
    })
    .unwrap();
    let split = IcebergSplit::try_new(IcebergSplitParams {
        read_domain: domain,
        path: path.to_str().unwrap().to_owned(),
        start: 0,
        length: file_size,
        file_size,
        file_record_count: 16,
        file_format: IcebergFileFormat::Parquet,
        partition_spec_id: 0,
        partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
        deletes: Vec::new().into(),
        file_statistics_domain: TupleDomain::all(),
        data_sequence_number: Some(3),
        file_first_row_id: None,
        decryption_data: None,
        split_weight: SplitWeight::STANDARD,
        affinity_key: None,
    })
    .unwrap();
    let relation = IcebergReadRelation::of_table(&table, 0)
        .unwrap()
        .with_output_layout(IcebergReadLayout::View);
    let context = FileReadContext {
        cancellation: FileCancellation::new(),
        deadline: Some(Instant::now() + Duration::from_secs(60)),
        runtime: Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        io_spawner: Arc::new(TokioFileIoSpawner::new(runtime.clone())),
        range: None,
        turn: None,
    };
    let access = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        context.runtime.clone(),
        TokioRefreshExecutor::shared(runtime.clone()),
        context.io_spawner.clone(),
    );
    let columns = [IcebergColumnHandle::base_column_of(&schema, 1).unwrap()];
    create_iceberg_page_stream(
        IcebergPageSourceRequest {
            relation: &relation,
            split: &split,
            columns: &columns,
            delete_manager: Arc::new(DeleteManager::new(access.clone(), context.clone())),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::new(ParquetFooterCache::new()),
            access_binding: access,
            context,
            cache: None,
            budget: ReaderSizing {
                max_read_rows: NonZeroUsize::new(4).unwrap(),
                max_read_bytes: 1 << 20,
            },
            parquet_policy: ParquetInterpretationPolicy::DerivedByteRepresentation {
                preserve_top_level_dictionary: true,
            },
            reader_options: FileReaderOptions::default(),
            scheduled_split_sequence_id: 1,
            dynamic_filter: Arc::new(CompleteAllDynamicFilter::new(Default::default())),
            prepared_input: None,
            pending_preparation_control: None,
            output_projection: None,
        },
        budget,
    )
    .unwrap()
}
