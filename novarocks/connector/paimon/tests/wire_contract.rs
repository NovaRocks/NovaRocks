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

use novarocks_connector_paimon::domain::{
    MAX_PAIMON_FILES_PER_SPLIT, PaimonBinaryTableStats, PaimonBucketMode, PaimonColumn,
    PaimonDataCompression, PaimonDataFile, PaimonDataFileFacts, PaimonDeletionFile,
    PaimonMergeEngine, PaimonReadView, PaimonRowRange, PaimonSplit, PaimonTable,
};
use novarocks_connector_paimon::schema::PaimonDataType;
use novarocks_connector_paimon::wire::read::PaimonReadWireCodec;
use novarocks_spi::connector::read_stack::{
    ConnectorReadSplitFacts, ConnectorSplit, ConnectorTableHandle, SchemaTableName, SplitWeight,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecErrorKind,
    ConnectorCodecRevision, ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits,
    ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorPrivateDecoder, ConnectorPrivateEncoder,
    ConnectorProviderId,
};

fn header(category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
    ConnectorEnvelopeHeader::new(
        ConnectorProviderId::parse("paimon").unwrap(),
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake").unwrap(),
            CatalogVersion::from_bytes([7; 32]),
        ),
        category,
        ConnectorCodecRevision::try_new(1).unwrap(),
    )
}

fn decode_context(
    category: ConnectorCodecCategory,
) -> (ConnectorEnvelopeHeader, ConnectorDecodeLedger) {
    (
        header(category),
        ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(1 << 20, 1 << 20, 1 << 20, 10_000, 16).unwrap(),
        ),
    )
}

fn empty_stats() -> PaimonBinaryTableStats {
    PaimonBinaryTableStats::try_new(vec![0, 0, 0, 0], vec![0, 0, 0, 0], Vec::new()).unwrap()
}

fn minimal_file(extra_files: Vec<String>) -> PaimonDataFile {
    PaimonDataFile::try_new(PaimonDataFileFacts {
        file_name: "data.parquet".to_string(),
        file_size: 1,
        row_count: 1,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: empty_stats(),
        value_stats: empty_stats(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 0,
        level: 0,
        extra_files,
        creation_time_millis: None,
        delete_row_count: None,
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        compression: PaimonDataCompression::Zstd,
    })
    .unwrap()
}

#[test]
fn table_column_and_view_round_trip_as_distinct_private_types() {
    let codec = PaimonReadWireCodec;
    let table = PaimonTable::try_new(
        SchemaTableName::try_new("db", "events").unwrap(),
        "s3://warehouse/db.db/events",
        PaimonMergeEngine::Deduplicate,
        PaimonBucketMode::Dynamic,
        vec![1],
        vec![],
    )
    .unwrap();
    let payload = codec.encode_private(&table).unwrap();
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadTable);
    let decoded: PaimonTable = codec
        .decode_private(
            &payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap();
    assert_eq!(decoded.schema_table_name().table_name(), "events");
    assert_eq!(decoded.primary_key_field_ids(), [1]);

    let column = PaimonColumn::try_new(1, "id", PaimonDataType::Int64, false, 0).unwrap();
    let payload = codec.encode_private(&column).unwrap();
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadColumn);
    let decoded: PaimonColumn = codec
        .decode_private(
            &payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap();
    assert_eq!(decoded, column);

    let view = PaimonReadView::try_new(
        "s3://warehouse/db.db/events",
        Some(3),
        2,
        [4; 32],
        [5; 32],
        Some(1),
    )
    .unwrap();
    let payload = codec.encode_private(&view).unwrap();
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadView);
    let decoded: PaimonReadView = codec
        .decode_private(
            &payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap();
    assert_eq!(decoded, view);
}

#[test]
fn split_round_trip_requires_exact_public_scheduling_facts() {
    let file = PaimonDataFile::try_new(PaimonDataFileFacts {
        file_name: "data.parquet".to_string(),
        file_size: 1024,
        row_count: 10,
        min_key: vec![1, 2],
        max_key: vec![8, 9],
        key_stats: PaimonBinaryTableStats::try_new(
            vec![0, 0, 0, 0],
            vec![0, 0, 0, 0],
            vec![Some(0)],
        )
        .unwrap(),
        value_stats: PaimonBinaryTableStats::try_new(
            vec![0, 0, 0, 0],
            vec![0, 0, 0, 0],
            vec![Some(2), None],
        )
        .unwrap(),
        min_sequence_number: 1,
        max_sequence_number: 9,
        schema_id: 2,
        level: 1,
        extra_files: vec!["data.parquet.idx".to_string()],
        creation_time_millis: Some(1_700_000_000_000),
        delete_row_count: Some(2),
        embedded_index: Some(vec![7, 8, 9]),
        file_source: Some(2),
        value_stats_cols: Some(vec!["value".to_string()]),
        external_path: Some("s3://warehouse/external/data.parquet".to_string()),
        first_row_id: Some(100),
        write_cols: Some(Vec::new()),
        compression: PaimonDataCompression::Zstd,
    })
    .unwrap();
    let split = PaimonSplit::try_new(
        3,
        2,
        0,
        Vec::new(),
        0,
        "s3://warehouse/db.db/events/bucket-0",
        8,
        vec![file],
        None,
        None,
        false,
        true,
        SplitWeight::STANDARD,
    )
    .unwrap();
    let codec = PaimonReadWireCodec;
    let payload = codec.encode_private(&split).unwrap();
    let facts = ConnectorReadSplitFacts::new(
        true,
        vec![],
        None::<&str>,
        SplitWeight::STANDARD,
        split.retained_size_in_bytes(),
    );
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadSplit);
    let decoded = codec
        .decode_split_private(
            &payload,
            &facts,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap();
    assert_eq!(decoded.snapshot_id(), 3);
    assert_eq!(decoded, split);
    assert!(decoded.contains_delete_rows());
    let facts = decoded.files()[0].facts();
    assert_eq!(facts.extra_files, vec!["data.parquet.idx".to_string()]);
    assert_eq!(facts.value_stats.null_counts(), [Some(2), None]);
    assert_eq!(facts.write_cols, Some(Vec::<String>::new()));
}

#[test]
fn split_rejects_unsupported_pruning_carriers_and_invalid_nested_facts() {
    assert!(
        PaimonSplit::try_new(
            1,
            0,
            0,
            Vec::new(),
            0,
            "s3://warehouse/table/bucket-0",
            1,
            vec![minimal_file(Vec::new())],
            Some(Vec::new()),
            None,
            true,
            false,
            SplitWeight::STANDARD,
        )
        .is_err()
    );
    assert!(
        PaimonSplit::try_new(
            1,
            0,
            0,
            Vec::new(),
            0,
            "s3://warehouse/table/bucket-0",
            1,
            vec![minimal_file(Vec::new())],
            None,
            Some(vec![PaimonRowRange::try_new(0, 0).unwrap()]),
            true,
            false,
            SplitWeight::STANDARD,
        )
        .is_err()
    );
    assert!(
        PaimonBinaryTableStats::try_new(vec![0, 0, 0, 0], vec![0, 0, 0, 0], vec![Some(-1)],)
            .is_err()
    );
    assert!(
        PaimonDataFile::try_new(PaimonDataFileFacts {
            extra_files: vec!["x".to_string(); MAX_PAIMON_FILES_PER_SPLIT + 1],
            ..minimal_file(Vec::new()).facts().clone()
        })
        .is_err()
    );
    assert!(
        PaimonSplit::try_new(
            1,
            0,
            0,
            Vec::new(),
            0,
            "s3://warehouse/table/bucket-0",
            1,
            vec![minimal_file(Vec::new())],
            Some(vec![Some(
                PaimonDeletionFile::try_new("s3://warehouse/table/delete.dv", 0, 1, Some(2))
                    .unwrap(),
            )]),
            None,
            true,
            false,
            SplitWeight::STANDARD,
        )
        .is_err()
    );
    for (bucket, total_buckets) in [(-2, 1), (-1, 1), (0, 0), (0, -2), (1, 1)] {
        assert!(
            PaimonSplit::try_new(
                1,
                0,
                0,
                Vec::new(),
                bucket,
                "s3://warehouse/table/bucket-0",
                total_buckets,
                vec![minimal_file(Vec::new())],
                None,
                None,
                true,
                false,
                SplitWeight::STANDARD,
            )
            .is_err()
        );
    }
}

#[test]
fn split_decoder_rejects_unknown_nested_file_field_before_materialization() {
    let split = PaimonSplit::try_new(
        1,
        0,
        0,
        Vec::new(),
        0,
        "s3://warehouse/table/bucket-0",
        1,
        vec![minimal_file(Vec::new())],
        None,
        None,
        true,
        false,
        SplitWeight::STANDARD,
    )
    .unwrap();
    let codec = PaimonReadWireCodec;
    let payload = inject_into_first_file(
        codec.encode_private(&split).unwrap().to_vec(),
        &[0xf8, 0x07, 0x01],
    );
    let facts = ConnectorReadSplitFacts::new(
        true,
        vec![],
        None::<&str>,
        SplitWeight::STANDARD,
        split.retained_size_in_bytes(),
    );
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadSplit);
    let error = codec
        .decode_split_private(
            &payload,
            &facts,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorCodecErrorKind::UnknownField);
}

#[test]
fn split_decoder_rejects_duplicate_nested_file_field_before_materialization() {
    let split = PaimonSplit::try_new(
        1,
        0,
        0,
        Vec::new(),
        0,
        "s3://warehouse/table/bucket-0",
        1,
        vec![minimal_file(Vec::new())],
        None,
        None,
        true,
        false,
        SplitWeight::STANDARD,
    )
    .unwrap();
    let codec = PaimonReadWireCodec;
    // field 1 is the singular file name; append a second occurrence.
    let payload = inject_into_first_file(
        codec.encode_private(&split).unwrap().to_vec(),
        &[0x0a, 0x01, b'x'],
    );
    let facts = ConnectorReadSplitFacts::new(
        true,
        vec![],
        None::<&str>,
        SplitWeight::STANDARD,
        split.retained_size_in_bytes(),
    );
    let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadSplit);
    let error = codec
        .decode_split_private(
            &payload,
            &facts,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorCodecErrorKind::DuplicateField);
}

fn inject_into_first_file(payload: Vec<u8>, suffix: &[u8]) -> Vec<u8> {
    let mut cursor = 0usize;
    while cursor < payload.len() {
        let (key, key_end) = decode_varint(&payload, cursor);
        cursor = key_end;
        let field = key >> 3;
        let wire = key & 7;
        if wire == 2 {
            let length_start = cursor;
            let (length, content_start) = decode_varint(&payload, cursor);
            let content_end = content_start + length as usize;
            if field == 5 {
                let mut nested = payload[content_start..content_end].to_vec();
                nested.extend_from_slice(suffix);
                let mut output = payload[..length_start].to_vec();
                encode_varint(nested.len() as u64, &mut output);
                output.extend_from_slice(&nested);
                output.extend_from_slice(&payload[content_end..]);
                return output;
            }
            cursor = content_end;
        } else if wire == 0 {
            cursor = decode_varint(&payload, cursor).1;
        } else {
            panic!("unexpected root wire type {wire}");
        }
    }
    panic!("split payload did not contain a data file")
}

fn inject_into_column_type(payload: Vec<u8>, suffix: &[u8]) -> Vec<u8> {
    let mut cursor = 0usize;
    while cursor < payload.len() {
        let (key, key_end) = decode_varint(&payload, cursor);
        cursor = key_end;
        let field = key >> 3;
        let wire = key & 7;
        if wire == 2 {
            let length_start = cursor;
            let (length, content_start) = decode_varint(&payload, cursor);
            let content_end = content_start + length as usize;
            if field == 3 {
                let mut nested = payload[content_start..content_end].to_vec();
                nested.extend_from_slice(suffix);
                let mut output = payload[..length_start].to_vec();
                encode_varint(nested.len() as u64, &mut output);
                output.extend_from_slice(&nested);
                output.extend_from_slice(&payload[content_end..]);
                return output;
            }
            cursor = content_end;
        } else if wire == 0 {
            cursor = decode_varint(&payload, cursor).1;
        } else {
            panic!("unexpected column root wire type {wire}");
        }
    }
    panic!("column payload did not contain a data type")
}

fn decode_varint(bytes: &[u8], mut offset: usize) -> (u64, usize) {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[offset];
        offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (value, offset);
        }
        shift += 7;
    }
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

#[test]
fn decoder_rejects_unknown_and_duplicate_singular_root_fields_before_materialization() {
    let codec = PaimonReadWireCodec;
    for payload in [vec![0x48, 0x01], vec![0x08, 0x01, 0x08, 0x02]] {
        let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadColumn);
        let error = <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonColumn>>::decode_private(
            &codec,
            &payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            ConnectorCodecErrorKind::UnknownField | ConnectorCodecErrorKind::DuplicateField
        ));
    }
}

#[test]
fn column_decoder_rejects_unknown_and_duplicate_nested_type_fields() {
    let codec = PaimonReadWireCodec;
    for (column, suffix, expected) in [
        (
            PaimonColumn::try_new(1, "id", PaimonDataType::Int64, false, 0).unwrap(),
            vec![0x28, 0x01],
            ConnectorCodecErrorKind::UnknownField,
        ),
        (
            PaimonColumn::try_new(1, "id", PaimonDataType::Int64, false, 0).unwrap(),
            vec![0x08, 0x04],
            ConnectorCodecErrorKind::DuplicateField,
        ),
        (
            PaimonColumn::try_new(
                1,
                "amount",
                PaimonDataType::decimal(18, 2).unwrap(),
                false,
                0,
            )
            .unwrap(),
            vec![0x10, 0x26],
            ConnectorCodecErrorKind::DuplicateField,
        ),
    ] {
        let payload =
            inject_into_column_type(codec.encode_private(&column).unwrap().to_vec(), &suffix);
        let (header, mut ledger) = decode_context(ConnectorCodecCategory::ReadColumn);
        let error = <PaimonReadWireCodec as ConnectorPrivateDecoder<PaimonColumn>>::decode_private(
            &codec,
            &payload,
            &mut ConnectorDecodeContext::new(&header, &mut ledger),
        )
        .unwrap_err();
        assert_eq!(error.kind(), expected);
    }
}

#[test]
fn unsupported_schema_types_fail_closed() {
    assert!(PaimonDataType::decimal(39, 0).is_err());
    assert!(PaimonDataType::timestamp(7).is_err());
}
