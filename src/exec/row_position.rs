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

// Iceberg v2 row-level DELETE virtual column names used by
// `DeleteAnalyzer`'s `INSERT INTO iceberg_delete_sink SELECT _file, _pos, ...`
// rewrite. `_file` is a per-scan-range constant delivered via
// `THdfsScanRange.extended_columns`; `_pos` is per-row and the BE parquet
// reader synthesizes it from the row's absolute position within the file.
pub const ICEBERG_FILE_PATH_COL: &str = "_file";
pub const ICEBERG_ROW_POS_COL: &str = "_pos";

pub fn is_iceberg_file_path(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_FILE_PATH_COL)
}

pub fn is_iceberg_row_pos(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_ROW_POS_COL)
}

// Iceberg V3 row-lineage virtual column names.
pub const ICEBERG_ROW_ID_COL: &str = "_row_id";
pub const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number";
pub const CHANGE_OP_COL: &str = crate::exec::change_op::CHANGE_OP_COLUMN;

// Reserved Iceberg field IDs for V3 row-lineage metadata columns.
pub const ICEBERG_RESERVED_FIELD_ID_ROW_ID: i32 = i32::MAX - 107;
pub const ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER: i32 = i32::MAX - 108;

pub fn is_iceberg_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
}

pub fn is_iceberg_last_updated_sequence_number(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
}

pub fn is_change_op(name: &str) -> bool {
    name.eq_ignore_ascii_case(CHANGE_OP_COL)
}

#[derive(Clone, Debug)]
pub struct RowPositionDescriptor {
    pub row_position_type: descriptors::TRowPositionType,
    pub row_source_slot: SlotId,
    pub fetch_ref_slots: Vec<SlotId>,
    pub lookup_ref_slots: Vec<SlotId>,
}

/// Iceberg virtual columns used by row-level DELETE (`_file`, `_pos`) and
/// V3 row-lineage reads (`_row_id`, `_last_updated_sequence_number`).
/// All fields are optional: only the slots present in the SELECT list (and
/// therefore in the scan-node output layout) are populated.
#[derive(Clone, Debug, Default)]
pub struct IcebergVirtualSpec {
    pub file_path_slot: Option<SlotId>,
    pub row_pos_slot: Option<SlotId>,
    pub row_id_slot: Option<SlotId>,
    pub last_updated_seq_slot: Option<SlotId>,
    pub change_op_slot: Option<SlotId>,
    pub file_path_field: Option<Field>,
    pub row_pos_field: Option<Field>,
    pub row_id_field: Option<Field>,
    pub last_updated_seq_field: Option<Field>,
    pub change_op_field: Option<Field>,
}

impl IcebergVirtualSpec {
    pub fn is_empty(&self) -> bool {
        self.file_path_slot.is_none()
            && self.row_pos_slot.is_none()
            && self.row_id_slot.is_none()
            && self.last_updated_seq_slot.is_none()
            && self.change_op_slot.is_none()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_iceberg_row_id_recognizes_name_case_insensitive() {
        assert!(is_iceberg_row_id("_row_id"));
        assert!(is_iceberg_row_id("_ROW_ID"));
        assert!(!is_iceberg_row_id("row_id"));
        assert!(!is_iceberg_row_id("_rowid"));
    }

    #[test]
    fn is_iceberg_last_updated_sequence_number_recognizes_name_case_insensitive() {
        assert!(is_iceberg_last_updated_sequence_number(
            "_last_updated_sequence_number"
        ));
        assert!(is_iceberg_last_updated_sequence_number(
            "_Last_Updated_Sequence_Number"
        ));
        assert!(!is_iceberg_last_updated_sequence_number(
            "last_updated_sequence_number"
        ));
    }

    #[test]
    fn is_change_op_recognizes_name_case_insensitive() {
        assert!(is_change_op("__change_op"));
        assert!(is_change_op("__CHANGE_OP"));
        assert!(!is_change_op("change_op"));
        assert!(!is_change_op("_change_op"));
    }

    #[test]
    fn iceberg_virtual_spec_default_is_empty() {
        let spec = IcebergVirtualSpec::default();
        assert!(spec.is_empty());
    }
}
