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

use novarocks_types::SlotId;

// Iceberg virtual column names (no trailing underscore)
pub const ROW_SOURCE_ID_COL: &str = "_row_source_id";
pub const SCAN_RANGE_ID_COL: &str = "_scan_range_id";
pub const ROW_ID_COL: &str = "_row_id";

pub fn is_row_source_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_SOURCE_ID_COL)
}

pub fn is_scan_range_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(SCAN_RANGE_ID_COL)
}

pub fn is_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_ID_COL)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowPositionType {
    Iceberg,
}

#[derive(Clone, Debug)]
pub struct RowPositionDescriptor {
    pub row_position_type: RowPositionType,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_position_type_is_thrift_free_domain_enum() {
        assert_eq!(RowPositionType::Iceberg, RowPositionType::Iceberg);
    }

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
}
