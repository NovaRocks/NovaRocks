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

use parquet::basic::{Encoding, EncodingMask};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::SchemaDescriptor;

use super::{ReaderSizing, decoder_batch_rows, whole_chunk_dictionary};

#[test]
fn unknown_width_uses_only_the_row_limit() {
    let sizing = ReaderSizing {
        max_read_rows: NonZeroUsize::new(512).unwrap(),
        max_read_bytes: 0,
    };
    assert_eq!(decoder_batch_rows(sizing, None), 512);
    assert_eq!(decoder_batch_rows(sizing, Some(0)), 512);
    assert_eq!(decoder_batch_rows(sizing, Some(8)), 1);
}

#[test]
fn a_byte_target_keeps_eight_bit_packed_boolean_rows_per_byte() {
    let sizing = ReaderSizing {
        max_read_rows: NonZeroUsize::new(512).unwrap(),
        max_read_bytes: 1,
    };
    assert_eq!(decoder_batch_rows(sizing, Some(1)), 8);
}

#[test]
#[expect(deprecated, reason = "exercise the Parquet v1 footer rule")]
fn legacy_dictionary_proof_requires_only_dictionary_and_level_encodings() {
    let schema = parse_message_type("message table { required BINARY words (UTF8); }").unwrap();
    let schema = SchemaDescriptor::new(Arc::new(schema));
    let legacy = ColumnChunkMetaData::builder(schema.column(0))
        .set_dictionary_page_offset(Some(4))
        .set_encodings(vec![
            Encoding::PLAIN_DICTIONARY,
            Encoding::RLE,
            Encoding::BIT_PACKED,
        ])
        .build()
        .unwrap();
    assert!(whole_chunk_dictionary(&legacy));

    let with_plain = legacy
        .clone()
        .into_builder()
        .set_encodings(vec![
            Encoding::PLAIN_DICTIONARY,
            Encoding::RLE,
            Encoding::PLAIN,
        ])
        .build()
        .unwrap();
    assert!(!whole_chunk_dictionary(&with_plain));

    let empty_data_page_mask = legacy
        .into_builder()
        .set_page_encoding_stats_mask(EncodingMask::default())
        .build()
        .unwrap();
    assert!(!whole_chunk_dictionary(&empty_data_page_mask));
}
