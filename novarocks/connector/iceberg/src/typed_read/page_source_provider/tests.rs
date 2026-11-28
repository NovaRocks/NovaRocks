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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FileRangeScope, FileRangeService,
    FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
};
use novarocks_spi::connector::read_stack::{
    CompleteAllDynamicFilter, ConnectorSplit, SchemaTableName, SourcePage, SplitWeight, TupleDomain,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use crate::typed_read::runtime::IcebergExecutionReadRuntime;
use crate::typed_read::split::{IcebergFileFormat, IcebergSplit, IcebergSplitParams};
use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};

use super::*;
use crate::typed_read::test_streams::DrivenStream;

#[test]
fn the_provider_shares_one_footer_cache_and_one_delete_manager() {
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
        deadline: Some(Instant::now() + Duration::from_secs(60)),
        runtime: file_runtime,
        io_spawner,
        range: None,
    };
    let provider = IcebergPageSourceProvider::new(
        binding,
        context,
        IcebergPageSourceProviderOptions {
            reader_options: FileReaderOptions::default(),
            cache_options: disabled_external_cache_options(),
        },
    );

    assert!(provider.footers().is_empty().expect("footer cache"));
    assert_eq!(
        provider.delete_manager().loaded_artifacts().expect("state"),
        0
    );
    assert!(Arc::ptr_eq(provider.footers(), provider.footers()));
    assert!(Arc::ptr_eq(
        provider.delete_manager(),
        provider.delete_manager()
    ));
}

/// A one-row-group Iceberg file with a bad final Arrow hint. Preparation
/// and the promoted page-index reader must retain the real field identity;
/// preparation holds the whole file as the split's input.
fn staged_small_file() -> (
    tokio::runtime::Runtime,
    tempfile::TempDir,
    IcebergPreparedPageSource,
    u64,
) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("data.parquet");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(
            [(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    ]));
    let file = std::fs::File::create(&path).expect("create data file");
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&arrow_schema), None).expect("parquet writer");
    writer
        .write(
            &RecordBatch::try_new(
                arrow_schema,
                vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
            )
            .expect("batch"),
        )
        .expect("write data");
    writer.append_key_value_metadata(parquet::file::metadata::KeyValue::new(
        parquet::arrow::ARROW_SCHEMA_META_KEY.to_owned(),
        "not-an-arrow-schema".to_owned(),
    ));
    writer.close().expect("close writer");
    let file_size = std::fs::metadata(&path).expect("stat data file").len();
    assert!(file_size < novarocks_fs::SMALL_FILE_PROBE_MAX_BYTES);
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("table schema");
    let partition_spec = crate::iceberg::spec::PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .expect("partition spec");
    let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: SchemaTableName::try_new("sales", "orders").unwrap(),
        snapshot_id: Some(11),
        read_domain: Some(crate::delete_semantics::test_read_domain(
            &schema,
            std::slice::from_ref(&partition_spec),
            11,
        )),
        table_schema_json: serde_json::to_string(&schema).unwrap(),
        spec_id: Some(0),
        partition_spec_jsons: [(0, serde_json::to_string(&partition_spec).unwrap())].into(),
        format_version: 2,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: Default::default(),
        name_mapping_json: None,
        table_location: directory.path().to_string_lossy().to_string(),
        storage_properties: Default::default(),
        pinned_data_files: None,
    })
    .expect("table handle");
    let path = path.to_string_lossy().to_string();
    let split = IcebergSplit::try_new(IcebergSplitParams {
        read_domain: Arc::clone(table.read_domain().unwrap()),
        path,
        start: 0,
        length: file_size as i64,
        file_size: file_size as i64,
        file_record_count: 3,
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
    .expect("split");
    let file_runtime: Arc<dyn FileIoRuntime> =
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
    let io_spawner: Arc<dyn FileIoSpawner> =
        Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
    let service = FileRangeService::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        Arc::clone(&io_spawner),
        runtime.handle().clone(),
    );
    let context = FileReadContext {
        turn: None,
        cancellation: FileCancellation::new(),
        deadline: Some(Instant::now() + Duration::from_secs(10)),
        runtime: Arc::clone(&file_runtime),
        io_spawner: Arc::clone(&io_spawner),
        range: Some(service.bind(
            FileRangeScope::for_test(1, 0, 1, 2, 0, 3).unwrap(),
            novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test(),
        )),
    };
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        file_runtime,
        novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
        io_spawner,
    );
    let provider = IcebergPageSourceProvider::new(
        binding,
        context,
        IcebergPageSourceProviderOptions {
            reader_options: FileReaderOptions {
                enable_parquet_reader_page_index: true,
                ..Default::default()
            },
            ..IcebergPageSourceProviderOptions::default()
        },
    );
    let mut prepared = IcebergPreparedPageSource {
        sizing: crate::file_reader::default_parquet_reader_sizing(),
        output_layout: IcebergReadLayout::Offsets,
        table,
        split,
        columns: vec![IcebergColumnHandle::base_column_of(&schema, 1).unwrap()],
        sequence_id: 0,
        access_binding: provider.access_binding.clone(),
        context: provider.context.clone(),
        options: provider.options.clone(),
        parquet_policy: ParquetInterpretationPolicy::DerivedByteRepresentation {
            preserve_top_level_dictionary: true,
        },
        footers: Arc::clone(&provider.footers),
        delete_manager: Arc::clone(&provider.delete_manager),
        phase: FuturePreparationPhase::Unstarted,
        footer: None,
        failure: None,
        control: Arc::new(SuccessorPreparationGroup::new()),
        observed_reclaim_epoch: 0,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while prepared.advance_stages(file_size) != ConnectorPreparationProgress::Ready {
        assert!(
            Instant::now() < deadline,
            "staged preparation did not finish"
        );
        std::thread::yield_now();
    }
    assert!(prepared.failure.is_none());
    let footer = prepared.footer.as_ref().expect("prepared footer");
    assert_eq!(
        footer.interpretation_policy(),
        novarocks_fs::ParquetInterpretationPolicy::DerivedByteRepresentation {
            preserve_top_level_dictionary: true
        },
    );
    assert_eq!(footer.physical_columns()[0].field_id(), Some(1));
    assert_eq!(
        footer
            .schema()
            .field(0)
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY),
        Some(&"1".to_owned()),
    );
    let last_hint = footer
        .raw_metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("footer metadata")
        .iter()
        .rfind(|entry| entry.key == parquet::arrow::ARROW_SCHEMA_META_KEY)
        .expect("Arrow hint");
    assert_eq!(last_hint.value.as_deref(), Some("not-an-arrow-schema"));
    assert_eq!(prepared.control.retained_input_bytes(), file_size);
    (runtime, directory, prepared, file_size)
}

fn ids_of(page: SourcePage) -> Vec<i64> {
    let (_, columns) = page.into_columns().expect("columns");
    columns[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids")
        .values()
        .to_vec()
}

#[test]
fn staged_small_file_preparation_parses_on_advance_and_promotes() {
    let (runtime, _directory, prepared, _) = staged_small_file();
    let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
        Arc::new(CompleteAllDynamicFilter::new(Default::default()));
    let budget = ConnectorPollBudget::new();
    let stream = <IcebergPreparedPageSource as ProviderPreparedPageSource<
        IcebergExecutionReadRuntime,
    >>::promote(Box::new(prepared), &filter, &budget)
    .expect("promote prepared split");
    let mut source = DrivenStream::new(&runtime, stream, budget);
    let mut values = Vec::new();
    while let Some(page) = source.next_page().expect("page") {
        values.extend(ids_of(page));
    }
    assert_eq!(values, vec![1, 2, 3]);
    source.close().expect("close stream");
}

#[test]
fn a_stream_promoted_from_a_prepared_split_counts_its_input_from_creation() {
    use futures::StreamExt;

    let (runtime, _directory, prepared, file_size) = staged_small_file();
    let split_bytes = prepared.split.retained_size_in_bytes();
    let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
        Arc::new(CompleteAllDynamicFilter::new(Default::default()));
    let budget = ConnectorPollBudget::new();
    let mut stream = <IcebergPreparedPageSource as ProviderPreparedPageSource<
        IcebergExecutionReadRuntime,
    >>::promote(Box::new(prepared), &filter, &budget)
    .expect("promote prepared stream");
    // The input left the preparation with the promotion; the stream holds
    // it, and reports it, before its first poll.
    assert!(
        stream.memory_usage_bytes() >= split_bytes + file_size,
        "a {file_size}-byte input is unaccounted: the stream reports {}",
        stream.memory_usage_bytes()
    );
    let values = runtime.block_on(async {
        let mut values = Vec::new();
        loop {
            budget.refill(1024);
            match stream.next().await {
                Some(page) => values.extend(ids_of(page.expect("page"))),
                None => break values,
            }
        }
    });
    assert_eq!(values, vec![1, 2, 3]);
    runtime.block_on(stream.close()).expect("close stream");
}

#[test]
fn a_frozen_byte_target_sizes_cold_and_prepared_reads_identically() {
    use novarocks_spi::connector::read_stack::adapter::ProviderReadPageSourceProvider;
    use novarocks_spi::connector::read_stack::{Assignment, ConnectorValueType};

    let (runtime, _directory, fixture, file_size) = staged_small_file();
    let provider = IcebergPageSourceProvider::new(
        fixture.access_binding.clone(),
        fixture.context.clone(),
        fixture.options.clone(),
    );
    let provider: &dyn ProviderReadPageSourceProvider<IcebergExecutionReadRuntime> = &provider;
    let session = ConnectorSession::try_new(
        "q-1",
        "test",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("session");
    let mut execution = IcebergReadExecution::default();
    execution.sizing.max_read_bytes = 0;
    let table = IcebergRuntimeRelation::Table(fixture.table.clone());
    let split = IcebergReadSplit::Data(fixture.split.clone());
    let columns = [Assignment::try_new(
        "v_id",
        fixture.columns[0].clone(),
        ConnectorValueType::BigInt,
    )
    .expect("assignment")];
    drop(fixture);
    let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
        Arc::new(CompleteAllDynamicFilter::new(Default::default()));
    let assert_output = |stream, budget| {
        let mut source = DrivenStream::new(&runtime, stream, budget);
        for expected_id in 1..=3 {
            let page = source.next_page().expect("read page").expect("one row");
            assert_eq!(ids_of(page), vec![expected_id]);
        }
        assert!(source.next_page().expect("end of stream").is_none());
        source.close().expect("close stream");
    };

    let budget = ConnectorPollBudget::new();
    let stream = provider
        .create_page_stream(
            &session,
            &table,
            Some(&execution),
            &split,
            0,
            &columns,
            &filter,
            &budget,
        )
        .expect("cold stream");
    assert_output(stream, budget);

    let ProviderPreparationStart::Prepared(mut prepared) = provider
        .prepare_page_source(
            &session,
            &table,
            Some(&execution),
            &split,
            0,
            &columns,
            &filter,
        )
        .expect("prepare the same split")
    else {
        panic!("the Parquet split must support preparation");
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while prepared.advance(file_size).expect("advance preparation")
        != ConnectorPreparationProgress::Ready
    {
        assert!(Instant::now() < deadline, "preparation did not finish");
        std::thread::yield_now();
    }
    assert_eq!(prepared.retained_input_bytes(), file_size);
    let budget = ConnectorPollBudget::new();
    let stream = prepared.promote(&filter, &budget).expect("prepared stream");
    assert_output(stream, budget);
}

#[test]
fn the_provider_streams_a_data_split_and_refuses_a_pair_it_cannot_read() {
    use futures::StreamExt;
    use novarocks_spi::connector::read_stack::adapter::ProviderReadPageSourceProvider;
    use novarocks_spi::connector::read_stack::{Assignment, ConnectorValueType};

    let (runtime, _directory, prepared, _) = staged_small_file();
    let provider = IcebergPageSourceProvider::new(
        prepared.access_binding.clone(),
        prepared.context.clone(),
        prepared.options.clone(),
    );
    let session = ConnectorSession::try_new(
        "q-1",
        "test",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("session");
    let columns = vec![
        Assignment::try_new(
            "v_id",
            prepared.columns[0].clone(),
            ConnectorValueType::BigInt,
        )
        .expect("assignment"),
    ];
    let filter: Arc<dyn DynamicFilter<IcebergColumnHandle>> =
        Arc::new(CompleteAllDynamicFilter::new(Default::default()));
    let budget = ConnectorPollBudget::new();
    let open = |table: &IcebergRuntimeRelation, split: &IcebergReadSplit| {
        <IcebergPageSourceProvider as ProviderReadPageSourceProvider<
            IcebergExecutionReadRuntime,
        >>::create_page_stream(
            &provider,
            &session,
            table,
            Some(&IcebergReadExecution::default()),
            split,
            0,
            &columns,
            &filter,
            &budget,
        )
    };

    let data = IcebergReadSplit::Data(prepared.split.clone());
    let mut stream = open(
        &IcebergRuntimeRelation::Table(prepared.table.clone()),
        &data,
    )
    .expect("a data split opens a stream");
    let values = runtime.block_on(async {
        let mut values = Vec::new();
        loop {
            budget.refill(1024);
            match stream.next().await {
                Some(page) => values.extend(ids_of(page.expect("page"))),
                None => break values,
            }
        }
    });
    assert_eq!(values, vec![1, 2, 3]);
    runtime.block_on(stream.close()).expect("close stream");

    // A data split named against a system relation is refused, as the
    // page source refuses it, rather than read as something else.
    let reference = crate::typed_read::system_table::IcebergSystemTableReference::try_new(
        crate::typed_read::system_table::IcebergSystemTableReferenceParams {
            schema_table_name: SchemaTableName::try_new("sales", "orders").unwrap(),
            system_table_type: crate::typed_read::system_table::IcebergSystemTableType::Snapshots,
            metadata_file_location: "file:///metadata/00001.metadata.json".to_owned(),
            table_uuid: "7b7e3b8a-6a55-4c8e-9a5c-2b3f0c1d2e3f".to_owned(),
            snapshot_id: None,
        },
    )
    .expect("reference");
    let mismatched = match open(
        &IcebergRuntimeRelation::SystemTable(crate::typed_read::SystemTableScan::empty_for_test(
            reference,
        )),
        &data,
    ) {
        Ok(_) => panic!("a mismatched relation and split open no stream"),
        Err(error) => error,
    };
    assert_eq!(
        mismatched.kind(),
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
    );
}
