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

//! The FS owner protects policy isolation and raw physical fact custody here.
//! Expected values come from the writer's original schema and ordered footer
//! entries; no Connector schema or table default participates in interpretation.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_fs::{
    CacheOptions, DataCacheManager, DataCachePageCacheOptions, FileErrorKind, FileFormat,
    FileProjection, ParquetInterpretationPolicy, inspect_parquet_metadata,
    open_file_reader_with_parquet_inspection, plan_parquet_input_ranges,
};
use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::metadata::KeyValue;

use common::Fixture;

fn policy_fixture(bad_last_hint: bool) -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("policy.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false).with_metadata(HashMap::from([
            (PARQUET_FIELD_ID_META_KEY.to_string(), "77".to_string()),
            ("hint-only".to_string(), "writer-field-metadata".to_string()),
        ])),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .expect("batch");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path).expect("file"), schema, None)
            .expect("writer");
    writer.write(&batch).expect("write");
    writer.append_key_value_metadata(KeyValue::new("duplicate".to_string(), "first".to_string()));
    writer.append_key_value_metadata(KeyValue::new("duplicate".to_string(), "last".to_string()));
    if bad_last_hint {
        writer.append_key_value_metadata(KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_string(),
            "not-base64".to_string(),
        ));
    }
    writer.close().expect("close");
    Fixture::from_path(directory, path)
}

#[test]
fn same_file_interpretations_share_raw_footer_but_never_arrow_schema() {
    let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
        capacity: 1024 * 1024,
        evict_probability: 100,
    });
    let fixture = policy_fixture(false);
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
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let inspect = |policy| {
        inspect_parquet_metadata(
            fixture.file.clone(),
            Some(cache.clone()),
            request.context.clone(),
            policy,
        )
        .expect("inspect under selected policy")
    };
    let skipped = inspect(ParquetInterpretationPolicy::SkipArrowHint);
    let used = inspect(ParquetInterpretationPolicy::UseArrowHint);
    let skipped_again = inspect(ParquetInterpretationPolicy::SkipArrowHint);
    assert!(Arc::ptr_eq(skipped.raw_metadata(), used.raw_metadata()));
    assert!(Arc::ptr_eq(skipped.schema(), skipped_again.schema()));
    assert!(
        !skipped
            .schema()
            .field(0)
            .metadata()
            .contains_key("hint-only")
    );
    assert_eq!(
        used.schema()
            .field(0)
            .metadata()
            .get("hint-only")
            .map(String::as_str),
        Some("writer-field-metadata")
    );
    for inspection in [&used, &skipped] {
        assert_eq!(inspection.physical_columns()[0].field_id(), Some(77));
        let raw = inspection.raw_metadata().file_metadata();
        assert_eq!(raw.schema().get_fields()[0].get_basic_info().id(), 77);
        let duplicates = raw
            .key_value_metadata()
            .expect("footer KV")
            .iter()
            .filter(|kv| kv.key == "duplicate")
            .map(|kv| kv.value.as_deref())
            .collect::<Vec<_>>();
        assert_eq!(duplicates, vec![Some("first"), Some("last")]);
    }
}

#[test]
fn bad_last_arrow_hint_is_not_a_fallback_and_skip_policy_ignores_all_hints() {
    let fixture = policy_fixture(true);
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let error = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect_err("last hint is invalid; do not fall back to earlier valid hint");
    assert_eq!(error.kind(), FileErrorKind::Corrupt);
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        ParquetInterpretationPolicy::SkipArrowHint,
    )
    .expect("all hint bytes are irrelevant to skip policy");
    assert_eq!(inspection.physical_columns()[0].field_id(), Some(77));
    let mut request = request;
    request.options.parquet_interpretation_policy = ParquetInterpretationPolicy::SkipArrowHint;
    request.options.enable_parquet_reader_page_index = true;
    request.predicates.push(novarocks_fs::ScanPredicate::new(
        "value",
        novarocks_fs::ScanPredicateDomain::Range {
            op: novarocks_fs::MinMaxPredicateOp::Ge,
            value: novarocks_fs::MinMaxPredicateValue::Int32(1),
        },
        novarocks_fs::ScanPredicateSource::Static,
    ));
    let mut reader = open_file_reader_with_parquet_inspection(request, Some(&inspection))
        .expect("same skip policy for decoder");
    let batches = common::collect(reader.as_mut()).expect("read physical data");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        3
    );
}

#[test]
fn inspection_cannot_be_used_under_a_different_interpretation_policy() {
    let fixture = policy_fixture(false);
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        ParquetInterpretationPolicy::SkipArrowHint,
    )
    .expect("skip inspection");
    assert_eq!(
        plan_parquet_input_ranges(&request, &inspection)
            .unwrap_err()
            .kind(),
        FileErrorKind::Invalid
    );
    let error = open_file_reader_with_parquet_inspection(request, Some(&inspection))
        .err()
        .expect("reader cannot inherit an incompatible schema");
    assert_eq!(error.kind(), FileErrorKind::Invalid);
}

#[test]
fn skip_policy_survives_async_and_prepared_footer_open() {
    let fixture = policy_fixture(true);
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    request.options.parquet_interpretation_policy = ParquetInterpretationPolicy::SkipArrowHint;
    let prepared = novarocks_fs::PreparedFileInput::new(
        &fixture.file,
        0,
        bytes::BytesMut::from(
            std::fs::read(fixture.file.location().path())
                .expect("file bytes")
                .as_slice(),
        ),
    )
    .expect("prepared file");
    let inspection = novarocks_fs::inspect_parquet_metadata_from_prepared(
        fixture.file.clone(),
        prepared,
        request.context.clone(),
        ParquetInterpretationPolicy::SkipArrowHint,
    )
    .expect("prepared footer ignores hint");
    assert_eq!(inspection.physical_columns()[0].field_id(), Some(77));
    fixture.io.handle().block_on(async {
        let inspection = novarocks_fs::inspect_parquet_metadata_async(
            fixture.file.clone(),
            None,
            request.context.clone(),
            ParquetInterpretationPolicy::SkipArrowHint,
        )
        .await
        .expect("async footer ignores hint");
        let mut reader = novarocks_fs::open_file_reader_async(request, Some(&inspection))
            .await
            .expect("async decoder retains policy");
        let mut rows = 0;
        while let Some(batch) = reader.next_batch().await.expect("async batch") {
            rows += batch.batch.num_rows();
        }
        assert_eq!(rows, 3);
        reader.close().expect("close async reader");
    });
}
