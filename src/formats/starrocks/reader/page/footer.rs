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
//! Lightweight page footer protobuf definitions.
//!
//! This file intentionally keeps only the protobuf fields required by the
//! current scalar native reader path.
//!
//! Current limitations:
//! - Footer structs are partial views, not full StarRocks protobuf schemas.
//! - Fields unrelated to current decode path are omitted by design.

use prost::Message;

/// Minimal `DataPageFooterPB` fields required by native reader.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct DataPageFooterPbLite {
    /// Logical ordinal of the first value in this page.
    #[prost(uint64, optional, tag = "1")]
    pub(crate) first_ordinal: Option<u64>,
    /// Total logical values encoded in this page.
    #[prost(uint64, optional, tag = "2")]
    pub(crate) num_values: Option<u64>,
    /// Byte length of page nullmap section appended to page body.
    #[prost(uint32, optional, tag = "3")]
    pub(crate) nullmap_size: Option<u32>,
    /// Data-page format version (`2` is required for nullable pages in this reader).
    #[prost(uint32, optional, tag = "20")]
    pub(crate) format_version: Option<u32>,
    /// Nullmap encoding identifier used by format-version-2 nullable pages.
    #[prost(int32, optional, tag = "21")]
    pub(crate) null_encoding: Option<i32>,
}

/// Minimal `IndexPageFooterPB` fields required by ordinal/page index decoder.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct IndexPageFooterPbLite {
    /// Number of index entries in this page.
    #[prost(uint32, optional, tag = "1")]
    pub(crate) num_entries: Option<u32>,
    /// Index page node type (`LEAF` or `INTERNAL`).
    #[prost(int32, optional, tag = "2")]
    pub(crate) r#type: Option<i32>,
}

/// Minimal `PageFooterPB` fields required by native reader.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct PageFooterPbLite {
    /// Page type id (DATA/DICT/etc).
    #[prost(int32, optional, tag = "1")]
    pub(crate) r#type: Option<i32>,
    /// Expected uncompressed body size for the page payload.
    #[prost(uint32, optional, tag = "2")]
    pub(crate) uncompressed_size: Option<u32>,
    /// Data-page specific footer; required when `type == DATA`.
    #[prost(message, optional, tag = "7")]
    pub(crate) data_page_footer: Option<DataPageFooterPbLite>,
    /// Index-page specific footer; required when `type == INDEX`.
    #[prost(message, optional, tag = "8")]
    pub(crate) index_page_footer: Option<IndexPageFooterPbLite>,
}
