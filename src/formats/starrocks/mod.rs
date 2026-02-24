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
//! StarRocks native format support.
//!
//! Module map:
//! - `metadata`: snapshot/rowset metadata loading.
//! - `segment`: segment footer decoding.
//! - `plan`: native read planning and schema compatibility checks.
//! - `reader`: native data page decoding and Arrow RecordBatch assembly.
//! - `data`: compatibility facade for legacy import paths.
//!
//! Current native loader scope:
//! - Scalar columns including DATE/DATETIME, string/binary, and DECIMALV3.
//! - Complex columns ARRAY/MAP/STRUCT.
//! - Bundle metadata + bundle segment layout.
//! - S3-compatible object storage options from FE (`fs.s3a.*`).
//!
//! Explicitly unsupported:
//! - JSON/VARIANT columns.
//! - DECIMALV2.
//! - Standalone metadata/segment layout outside bundle path.
//! - Nullable data pages with format version `1`.

pub mod cache;
pub mod data;
pub mod metadata;
pub mod plan;
pub mod reader;
pub mod segment;
pub mod writer;
