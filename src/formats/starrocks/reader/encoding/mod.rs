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
//! Low-level page encoding decoders.
//!
//! Module split:
//! - `plain_fixed`: fixed-width plain values with page-local value count header.
//! - `plain_binary`: variable-length plain values with offset trailer.
//! - `dict_binary`: dictionary-encoded variable-length values.
//! - `rle`: fixed-width scalar RLE payloads.
//! - `bitshuffle`: bitshuffle+LZ4 payloads for fixed-width values.
//! - `nullmap`: nullable data-page null flag decoders.
//!
//! Current limitations:
//! - Decoders target scalar page payloads only.
//! - Nullmap decoding is format-v2 only.

mod bitshuffle;
mod dict_binary;
mod nullmap;
mod plain_binary;
mod plain_fixed;
mod rle;

pub(super) use bitshuffle::{align_up_8, bitshuffle_lz4_decompress, decode_bitshuffle_page_body};
pub(super) use dict_binary::decode_binary_dict_values;
pub(super) use nullmap::decode_v2_null_flags;
pub(super) use plain_binary::decode_binary_plain_values;
pub(super) use plain_fixed::decode_fixed_plain_values;
pub(super) use rle::decode_rle_page_body;
