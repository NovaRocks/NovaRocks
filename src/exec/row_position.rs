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
use arrow::datatypes::Field;

use crate::common::ids::SlotId;
use crate::descriptors;

// Iceberg virtual column names (no trailing underscore)
pub const ROW_SOURCE_ID_COL: &str = "_row_source_id";
pub const SCAN_RANGE_ID_COL: &str = "_scan_range_id";
pub const ROW_ID_COL: &str = "_row_id";

// Lake (PRIMARY KEY cloud-native) virtual column names (with trailing underscore)
pub const LAKE_SOURCE_ID_COL: &str = "_source_id_";
pub const LAKE_TABLET_ID_COL: &str = "_tablet_id_";
pub const LAKE_RSS_ID_COL: &str = "_rss_id_";
pub const LAKE_ROW_ID_COL: &str = "_row_id_";

pub fn is_row_source_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_SOURCE_ID_COL)
}

pub fn is_scan_range_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(SCAN_RANGE_ID_COL)
}

pub fn is_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_ID_COL)
}

pub fn is_lake_source_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(LAKE_SOURCE_ID_COL)
}

pub fn is_lake_tablet_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(LAKE_TABLET_ID_COL)
}

pub fn is_lake_rss_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(LAKE_RSS_ID_COL)
}

pub fn is_lake_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(LAKE_ROW_ID_COL)
}

#[derive(Clone, Debug)]
pub struct RowPositionDescriptor {
    pub row_position_type: descriptors::TRowPositionType,
    pub row_source_slot: SlotId,
    pub fetch_ref_slots: Vec<SlotId>,
    pub lookup_ref_slots: Vec<SlotId>,
}

/// Row position spec for Iceberg V3 tables (scan_range_id + row_id).
#[derive(Clone, Debug)]
pub struct RowPositionSpec {
    pub row_source_slot: SlotId,
    pub scan_range_slot: SlotId,
    pub row_id_slot: SlotId,
    pub row_source_field: Field,
    pub scan_range_field: Field,
    pub row_id_field: Field,
}

/// Row position spec for lake (PRIMARY KEY cloud-native) tables.
/// Encodes position as (source_id, tablet_id, rss_id, row_id) where
/// source_id = backend_id, tablet_id = actual tablet, rss_id = synthetic
/// range index (assigned during scan), row_id = sequential row offset.
#[derive(Clone, Debug)]
pub struct LakeRowPositionSpec {
    pub source_id_slot: SlotId,
    pub tablet_id_slot: SlotId,
    pub rss_id_slot: SlotId,
    pub row_id_slot: SlotId,
    pub source_id_field: Field,
    pub tablet_id_field: Field,
    pub rss_id_field: Field,
    pub row_id_field: Field,
}
