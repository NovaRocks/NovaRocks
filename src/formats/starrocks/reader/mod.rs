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
//! StarRocks native reader entrypoint.
//!
//! This module exposes the public API and wires internal submodules.

mod column_decode;
mod column_state;
mod complex;
mod encoding;
mod indexed_column;
mod io;
mod model;
mod page;
mod record_batch;
mod schema_map;

pub use record_batch::build_native_record_batch;

mod types {
    pub(crate) mod decimal;
    pub(crate) mod temporal;
}

mod constants {
    //! Shared constants for the StarRocks native reader.
    //!
    //! The values in this module follow StarRocks BE on-disk definitions.
    //! Keeping them centralized avoids copy/paste drift between decoders.
    //!
    //! Current limitations reflected by these constants:
    //! - Nullable data page decoding is restricted to format v2.
    //! - Page compression is restricted to LZ4 frame.

    pub(super) const PAGE_TYPE_DATA: i32 = 1;
    pub(super) const PAGE_TYPE_INDEX: i32 = 2;
    pub(super) const PAGE_TYPE_DICTIONARY: i32 = 3;

    pub(super) const ENCODING_PLAIN: i32 = 2;
    pub(super) const ENCODING_RLE: i32 = 4;
    pub(super) const ENCODING_DICT: i32 = 5;
    pub(super) const ENCODING_BIT_SHUFFLE: i32 = 6;

    pub(super) const LOGICAL_TYPE_TINYINT: i32 = 1;
    pub(super) const LOGICAL_TYPE_SMALLINT: i32 = 3;
    pub(super) const LOGICAL_TYPE_INT: i32 = 5;
    pub(super) const LOGICAL_TYPE_BIGINT: i32 = 7;
    pub(super) const LOGICAL_TYPE_LARGEINT: i32 = 9;
    pub(super) const LOGICAL_TYPE_FLOAT: i32 = 10;
    pub(super) const LOGICAL_TYPE_DOUBLE: i32 = 11;
    pub(super) const LOGICAL_TYPE_CHAR: i32 = 13;
    pub(super) const LOGICAL_TYPE_VARCHAR: i32 = 17;
    pub(super) const LOGICAL_TYPE_HLL: i32 = 23;
    pub(super) const LOGICAL_TYPE_OBJECT: i32 = 25;
    pub(super) const LOGICAL_TYPE_DECIMAL256: i32 = 26;
    pub(super) const LOGICAL_TYPE_BOOLEAN: i32 = 24;
    pub(super) const LOGICAL_TYPE_BINARY: i32 = 45;
    pub(super) const LOGICAL_TYPE_VARBINARY: i32 = 46;
    pub(super) const LOGICAL_TYPE_DECIMAL32: i32 = 47;
    pub(super) const LOGICAL_TYPE_DECIMAL64: i32 = 48;
    pub(super) const LOGICAL_TYPE_DECIMAL128: i32 = 49;
    pub(super) const LOGICAL_TYPE_DATE: i32 = 50;
    pub(super) const LOGICAL_TYPE_DATETIME: i32 = 51;
    pub(super) const LOGICAL_TYPE_JSON: i32 = 54;

    pub(super) const COMPRESSION_NO_COMPRESSION: i32 = 0;
    pub(super) const COMPRESSION_LZ4_FRAME: i32 = 5;

    pub(super) const NULL_ENCODING_BITSHUFFLE: i32 = 0;
    pub(super) const NULL_ENCODING_LZ4: i32 = 1;
    pub(super) const DATA_PAGE_FORMAT_V2: u32 = 2;

    pub(super) const BITSHUFFLE_PAGE_HEADER_SIZE: usize = 16;
    pub(super) const BITSHUFFLE_TARGET_BLOCK_SIZE_BYTES: usize = 8192;
    pub(super) const BITSHUFFLE_MIN_BLOCK_SIZE: usize = 128;

    pub(super) const RLE_PAGE_HEADER_SIZE: usize = 4;
    pub(super) const MAX_VLQ_U32_BYTES: usize = 5;

    pub(super) const DATE_UNIX_EPOCH_JULIAN: i32 = 2_440_588;
    pub(super) const TIMESTAMP_BITS: u32 = 40;
    pub(super) const TIMESTAMP_TIME_MASK: u64 = (1_u64 << TIMESTAMP_BITS) - 1;
    pub(super) const USECS_PER_DAY_I64: i64 = 86_400_000_000;
}
