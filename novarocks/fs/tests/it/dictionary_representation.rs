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

// These inputs are produced through Arrow's Parquet writer. The row oracle
// comes from the strings passed to the writer, and footer assertions check
// that each fixture actually exercises the intended on-disk encoding.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, ListArray,
    RecordBatch, StringArray, StringViewArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use novarocks_fs::{
    CacheOptions, DataCacheManager, DataCachePageCacheOptions, FileBatch, FileErrorKind,
    FileFormat, FileProjection, ParquetInterpretationPolicy, PreparedFileInput,
    inspect_parquet_metadata, inspect_parquet_metadata_from_prepared, open_file_reader,
    open_file_reader_async, open_file_reader_with_parquet_inspection, plan_parquet_input_ranges,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Encoding;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::properties::WriterProperties;

use crate::common::{Fixture, collect};

const VIEWS: ParquetInterpretationPolicy = ParquetInterpretationPolicy::DerivedByteRepresentation {
    preserve_top_level_dictionary: false,
};
const DICTIONARY: ParquetInterpretationPolicy =
    ParquetInterpretationPolicy::DerivedByteRepresentation {
        preserve_top_level_dictionary: true,
    };

fn write_fixture(column: ArrayRef, props: WriterProperties, strip_stats: bool) -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("strings.parquet");
    let field = Field::new("names", column.data_type().clone(), false).with_metadata(
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), "42".to_owned())]),
    );
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("fixture batch");
    let mut writer = ArrowWriter::try_new(File::create(&path).expect("file"), schema, Some(props))
        .expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("finish");
    if strip_stats {
        // Preserve all data and index pages. Drop only optional footer facts
        // that are absent in files produced by some writers.
        let metadata = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("file"))
            .expect("read metadata")
            .metadata()
            .as_ref()
            .clone();
        let mut metadata = metadata.into_builder();
        let mut groups = metadata.take_row_groups();
        for group in &mut groups {
            for column in group.columns_mut() {
                *column = column
                    .clone()
                    .into_builder()
                    .clear_page_encoding_stats()
                    .set_unencoded_byte_array_data_bytes(None)
                    .build()
                    .expect("valid column without optional stats");
            }
        }
        let mut bytes = std::fs::read(&path).expect("original file");
        let footer = bytes.len() - 8;
        let length = u32::from_le_bytes(bytes[footer..footer + 4].try_into().unwrap()) as usize;
        bytes.truncate(footer - length);
        ParquetMetaDataWriter::new(&mut bytes, &metadata.set_row_groups(groups).build())
            .finish()
            .expect("write footer without optional stats");
        std::fs::write(&path, bytes).expect("replace footer");
    }
    Fixture::from_path(directory, path)
}

fn dictionary(batch: &FileBatch) -> &DictionaryArray<Int32Type> {
    batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref()
        .expect("Int32 dictionary")
}

fn decode_strings(batches: &[FileBatch]) -> Vec<String> {
    let mut output = Vec::new();
    for batch in batches {
        let column = batch.batch.column(0);
        if let Some(dictionary) = column.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            output.extend(
                dictionary
                    .keys()
                    .values()
                    .iter()
                    .map(|&key| values.value(key as usize).to_owned()),
            );
        } else if let Some(views) = column.as_any().downcast_ref::<StringViewArray>() {
            output.extend(views.iter().map(|value| value.unwrap().to_owned()));
        } else {
            let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
            output.extend(strings.iter().map(|value| value.unwrap().to_owned()));
        }
    }
    output
}

fn read_fixture(
    fixture: &Fixture,
    policy: ParquetInterpretationPolicy,
    rows: usize,
) -> Vec<FileBatch> {
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, rows, u64::MAX);
    request.options.parquet_interpretation_policy = policy;
    let mut reader = open_file_reader(request).expect("open Parquet reader");
    collect(reader.as_mut()).expect("read batches")
}

#[test]
fn full_dictionary_keeps_exact_values_object_within_each_row_group() {
    let prefix = "first value held in dictionary backing";
    let suffix = "second value held in dictionary backing";
    let expected = [prefix; 8]
        .into_iter()
        .chain([suffix; 8])
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let fixture = write_fixture(
        Arc::new(StringArray::from(expected.clone())),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(8))
            .build(),
        false,
    );
    let dictionaries = read_fixture(&fixture, DICTIONARY, 4);
    assert_eq!(dictionaries.len(), 4);
    assert!(Arc::ptr_eq(
        dictionary(&dictionaries[0]).values(),
        dictionary(&dictionaries[1]).values()
    ));
    assert!(Arc::ptr_eq(
        dictionary(&dictionaries[2]).values(),
        dictionary(&dictionaries[3]).values()
    ));
    assert!(!Arc::ptr_eq(
        dictionary(&dictionaries[1]).values(),
        dictionary(&dictionaries[2]).values()
    ));
    assert_eq!(decode_strings(&dictionaries), expected);
    assert_eq!(decode_strings(&read_fixture(&fixture, VIEWS, 4)), expected);
    assert_eq!(
        decode_strings(&read_fixture(
            &fixture,
            ParquetInterpretationPolicy::SkipArrowHint,
            4
        )),
        expected
    );
    let fields = dictionaries[0].batch.schema();
    let field = fields.field(0);
    assert_eq!(field.name(), "names");
    assert!(!field.is_nullable());
    assert_eq!(
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .map(String::as_str),
        Some("42")
    );
}

#[test]
fn nested_ordinary_leaves_use_views_and_retain_physical_field_ids() {
    let field = |name, data_type, id: i32| {
        Arc::new(
            Field::new(name, data_type, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                id.to_string(),
            )])),
        )
    };
    let names = Arc::new(StringArray::from(vec!["nested name"; 4]));
    let bytes = Arc::new(BinaryArray::from_vec(vec![b"nested bytes".as_slice(); 4]));
    let fields = vec![
        field("name", DataType::Utf8, 43),
        field("bytes", DataType::Binary, 44),
    ]
    .into();
    let entries = Arc::new(StructArray::new(fields, vec![names, bytes], None));
    let list = ListArray::new(
        field("item", entries.data_type().clone(), 45),
        OffsetBuffer::new(vec![0, 2, 4].into()),
        entries,
        None,
    );
    let fixture = write_fixture(Arc::new(list), WriterProperties::default(), false);
    let batches = read_fixture(&fixture, DICTIONARY, 2);
    assert_eq!(batches.len(), 1);
    let list = batches[0]
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 4]);
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let names = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    let bytes = entries
        .column(1)
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .unwrap();
    assert!(names.iter().all(|value| value == Some("nested name")));
    assert!(
        bytes
            .iter()
            .all(|value| value == Some(b"nested bytes".as_slice()))
    );
    for (field, id) in entries.fields().iter().zip(["43", "44"]) {
        assert_eq!(
            field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some(id)
        );
    }
}

#[test]
fn a_plain_fallback_page_keeps_the_whole_column_in_views() {
    let expected = (0..64)
        .map(|index| format!("value-{index:02} long enough to overflow the small dictionary"))
        .collect::<Vec<_>>();
    let props = WriterProperties::builder()
        .set_dictionary_page_size_limit(96)
        .set_data_page_row_count_limit(2)
        .set_write_batch_size(1)
        .build();
    let fixture = write_fixture(Arc::new(StringArray::from(expected.clone())), props, false);
    let file = File::open(fixture.file.location().path()).expect("fixture file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("fixture metadata");
    let mask = reader
        .metadata()
        .row_group(0)
        .column(0)
        .page_encoding_stats_mask()
        .unwrap();
    assert!(mask.is_set(Encoding::RLE_DICTIONARY));
    assert!(mask.is_set(Encoding::PLAIN));
    let batches = read_fixture(&fixture, DICTIONARY, 4);
    assert!(
        batches
            .iter()
            .all(|batch| batch.batch.column(0).data_type() == &DataType::Utf8View)
    );
    assert_eq!(decode_strings(&batches), expected);
}

#[test]
fn absent_page_and_size_statistics_allow_reading_with_view_carriers() {
    let expected = vec!["same value read without optional footer statistics".to_owned(); 32];
    let fixture = write_fixture(
        Arc::new(StringArray::from(expected.clone())),
        WriterProperties::default(),
        true,
    );
    let file = File::open(fixture.file.location().path()).expect("fixture file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("fixture metadata");
    let column = reader.metadata().row_group(0).column(0);
    assert!(column.dictionary_page_offset().is_some());
    assert!(column.encodings_mask().is_set(Encoding::RLE_DICTIONARY));
    assert!(!column.encodings_mask().is_set(Encoding::PLAIN_DICTIONARY));
    assert!(column.page_encoding_stats_mask().is_none());
    assert!(column.unencoded_byte_array_data_bytes().is_none());

    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 8, 4096);
    request.options.parquet_interpretation_policy = DICTIONARY;
    let mut reader = open_file_reader(request).expect("open with missing optional statistics");
    assert!((1..=8).contains(&reader.reader_batch_rows().expect("nonempty fixture")));
    let batches = collect(reader.as_mut()).expect("all batches");
    assert!(
        batches
            .iter()
            .all(|batch| batch.batch.column(0).data_type() == &DataType::Utf8View)
    );
    assert_eq!(decode_strings(&batches), expected);
}

#[test]
fn decoded_size_statistics_supply_plain_view_backing_estimate() {
    let expected = (0..16)
        .map(|index| format!("{index:032}"))
        .collect::<Vec<_>>();
    let fixture = write_fixture(
        Arc::new(StringArray::from(expected.clone())),
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .build(),
        false,
    );
    let file = File::open(fixture.file.location().path()).expect("fixture file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("fixture metadata");
    assert_eq!(
        reader
            .metadata()
            .row_group(0)
            .column(0)
            .unencoded_byte_array_data_bytes(),
        Some(16 * 32)
    );
    // 16 bytes per view descriptor, plus 32 bytes of backing per value.
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 16, 2 * (16 + 32));
    request.options.parquet_interpretation_policy = VIEWS;
    let mut reader = open_file_reader(request).expect("open with size statistics");
    assert_eq!(reader.reader_batch_rows(), Some(2));
    let batches = collect(reader.as_mut()).expect("all batches");
    assert_eq!(batches.len(), 8);
    assert_eq!(decode_strings(&batches), expected);
}

#[test]
fn repeated_long_value_uses_the_dictionary_backing_at_view_decode() {
    const ROWS: usize = 8192;
    let value = "x".repeat(300 * 1024);
    let values = Arc::new(StringArray::from(vec![value.as_str()]));
    let keys = Int32Array::from(vec![0; ROWS]);
    let array = DictionaryArray::try_new(keys, values).expect("one valid dictionary value");
    let fixture = write_fixture(Arc::new(array), WriterProperties::default(), false);
    let mut request = fixture.request(
        FileFormat::Parquet,
        FileProjection::All,
        ROWS,
        16 * 1024 * 1024,
    );
    request.options.parquet_interpretation_policy = VIEWS;
    let mut reader = open_file_reader(request).expect("open compact fixture");
    assert_eq!(reader.reader_batch_rows(), Some(ROWS));
    let batch = reader
        .next_batch()
        .expect("decode into view descriptors")
        .expect("nonempty");
    let views = batch
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert_eq!(views.len(), ROWS);
    assert_eq!(views.value(0), value);
    let shared = views.value(0).as_ptr();
    assert!(views.iter().all(|value| value.unwrap().as_ptr() == shared));
    assert_eq!(
        batch
            .physical_row_positions
            .as_ref()
            .unwrap()
            .values()
            .as_ref(),
        (0..ROWS as u64).collect::<Vec<_>>().as_slice()
    );
    assert!(
        views
            .data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>()
            < 1024 * 1024
    );
    // Only materialize known safe prefixes of the large logical extent.
    for offset in [0, ROWS - 3] {
        let prefix = views.slice(offset, 3);
        let flat = arrow::compute::cast(&prefix, &DataType::Utf8).expect("small flat prefix");
        let flat = flat.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(flat.value_offsets(), &[0, 307_200, 614_400, 921_600]);
        assert!(flat.iter().all(|entry| entry == Some(value.as_str())));
    }
    assert!(reader.next_batch().expect("finished").is_none());
}

#[test]
fn derived_policy_has_separate_cache_identity_and_reuses_exact_prepared_footer() {
    let fixture = write_fixture(
        Arc::new(StringArray::from(vec!["dictionary-backed value"; 8])),
        WriterProperties::default(),
        false,
    );
    let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
        capacity: 1024 * 1024,
        evict_probability: 100,
    });
    let cache = DataCacheManager::instance().external_context(CacheOptions {
        enable_scan_datacache: true,
        enable_populate_datacache: true,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    });
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, u64::MAX);
    let inspect = |policy| {
        inspect_parquet_metadata(
            fixture.file.clone(),
            Some(cache.clone()),
            request.context.clone(),
            policy,
        )
        .expect("inspect standard schema")
    };
    let views = inspect(VIEWS);
    let dictionary = inspect(DICTIONARY);
    assert!(Arc::ptr_eq(views.raw_metadata(), dictionary.raw_metadata()));
    assert!(!Arc::ptr_eq(views.schema(), dictionary.schema()));
    assert!(Arc::ptr_eq(
        dictionary.schema(),
        inspect(DICTIONARY).schema()
    ));
    assert_eq!(dictionary.schema().field(0).data_type(), &DataType::Utf8);
    request.options.parquet_interpretation_policy = DICTIONARY;
    assert_eq!(
        plan_parquet_input_ranges(&request, &views)
            .unwrap_err()
            .kind(),
        FileErrorKind::Invalid
    );
    assert_eq!(
        open_file_reader_with_parquet_inspection(request.clone(), Some(&views))
            .err()
            .expect("reject wrong policy")
            .kind(),
        FileErrorKind::Invalid
    );
    let bytes = std::fs::read(fixture.file.location().path()).expect("file bytes");
    let prepared =
        PreparedFileInput::new(&fixture.file, 0, bytes::BytesMut::from(bytes.as_slice()))
            .expect("prepared bytes");
    let inspection = inspect_parquet_metadata_from_prepared(
        fixture.file.clone(),
        prepared.clone(),
        request.context.clone(),
        DICTIONARY,
    )
    .expect("same policy for prepared footer");
    assert!(
        !plan_parquet_input_ranges(&request, &inspection)
            .expect("plan same policy")
            .is_empty()
    );
    request.prepared_input = Some(prepared);
    let expected = read_fixture(&fixture, DICTIONARY, 4);
    fixture.io.handle().block_on(async {
        let mut reader = open_file_reader_async(request, Some(&inspection))
            .await
            .expect("open prepared");
        assert_eq!(reader.reader_batch_rows(), Some(4));
        let mut actual = Vec::new();
        while let Some(batch) = reader.next_batch().await.expect("read") {
            assert!(
                batch
                    .batch
                    .column(0)
                    .as_any()
                    .is::<DictionaryArray<Int32Type>>()
            );
            actual.push(batch);
        }
        assert_eq!(decode_strings(&actual), decode_strings(&expected));
        reader.close().expect("close prepared reader");
    });
}
