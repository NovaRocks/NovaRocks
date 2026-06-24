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

use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

use crate::fs::scan_context::FileScanRange;
use crate::novarocks_logging::debug;

use super::{
    BoundVariantPathPruningPredicate, MinMaxPredicate, MinMaxPredicateValue,
    variant_residual_value_all_null_for_row_group,
};

pub(crate) fn select_row_groups_for_range(
    metadata: &ParquetMetaData,
    range: &FileScanRange,
    mut remaining_rows: Option<usize>,
    min_max_predicates: &[MinMaxPredicate],
    variant_predicates: &[BoundVariantPathPruningPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> Option<Vec<usize>> {
    if range.length == 0
        && remaining_rows.is_none()
        && min_max_predicates.is_empty()
        && variant_predicates.is_empty()
    {
        return None;
    }

    let split_start = range.offset;
    let mut split_end = split_start.saturating_add(range.length);
    if range.file_len > 0 && split_end > range.file_len {
        split_end = range.file_len;
    }
    if range.length == 0 && range.file_len == 0 {
        split_end = u64::MAX;
    }

    let mut row_groups = Vec::new();
    let mut filtered_count = 0;

    for (idx, row_group) in metadata.row_groups().iter().enumerate() {
        let rg_start = row_group_start_offset(row_group)?;
        if rg_start >= split_start && rg_start < split_end {
            if !min_max_predicates.is_empty() || !variant_predicates.is_empty() {
                match should_read_row_group(
                    row_group,
                    min_max_predicates,
                    variant_predicates,
                    columns,
                    case_sensitive,
                ) {
                    Ok(true) => {
                        // Passed pruning.
                    }
                    Ok(false) => {
                        // Pruned out.
                        filtered_count += 1;
                        continue;
                    }
                    Err(e) => {
                        // On error, conservatively keep the row group.
                        debug!("error checking row group predicates: {}", e);
                    }
                }
            }

            row_groups.push(idx);
            if let Some(rows_left) = remaining_rows.as_mut() {
                let rg_rows = row_group.num_rows().max(0) as usize;
                if rg_rows >= *rows_left {
                    break;
                }
                *rows_left = rows_left.saturating_sub(rg_rows);
                if *rows_left == 0 {
                    break;
                }
            }
        }
    }

    if filtered_count > 0 {
        debug!(
            "row group pruning: filtered {} row groups, kept {}",
            filtered_count,
            row_groups.len()
        );
    }

    Some(row_groups)
}

fn should_read_row_group(
    row_group: &RowGroupMetaData,
    predicates: &[MinMaxPredicate],
    variant_predicates: &[BoundVariantPathPruningPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> Result<bool, String> {
    for pred in predicates {
        let col_idx_str = match pred {
            MinMaxPredicate::Le { column, .. }
            | MinMaxPredicate::Ge { column, .. }
            | MinMaxPredicate::Lt { column, .. }
            | MinMaxPredicate::Gt { column, .. }
            | MinMaxPredicate::Eq { column, .. } => column.as_str(),
        };
        let Ok(col_idx) = col_idx_str.parse::<usize>() else {
            continue;
        };

        if col_idx >= columns.len() {
            continue;
        }
        let col_name = &columns[col_idx];

        let chunk = row_group.columns().iter().find(|c| {
            let path_str = c.column_path().string();
            if case_sensitive {
                path_str == *col_name
            } else {
                path_str.eq_ignore_ascii_case(col_name)
            }
        });

        if let Some(chunk) = chunk
            && !column_stats_satisfy_predicate(chunk, pred)?
        {
            return Ok(false);
        }
    }

    for pred in variant_predicates {
        if !variant_residual_value_all_null_for_row_group(row_group, pred) {
            continue;
        }
        let Some(column) = row_group.columns().get(pred.leaf_column_index) else {
            continue;
        };
        if !column_stats_satisfy_predicate(column, &pred.predicate)? {
            return Ok(false);
        }
    }

    Ok(true)
}

fn column_stats_satisfy_predicate(
    column: &ColumnChunkMetaData,
    pred: &MinMaxPredicate,
) -> Result<bool, String> {
    if let Some(stats) = column.statistics() {
        let satisfies = match pred {
            MinMaxPredicate::Le { value, .. } => check_min_satisfies_le(stats, value)?,
            MinMaxPredicate::Ge { value, .. } => check_max_satisfies_ge(stats, value)?,
            MinMaxPredicate::Lt { value, .. } => check_min_satisfies_lt(stats, value)?,
            MinMaxPredicate::Gt { value, .. } => check_max_satisfies_gt(stats, value)?,
            MinMaxPredicate::Eq { value, .. } => {
                check_max_satisfies_ge(stats, value)? && check_min_satisfies_le(stats, value)?
            }
        };

        if !satisfies {
            return Ok(false);
        }
    }
    Ok(true)
}

fn check_max_satisfies_ge(
    stats: &Statistics,
    value: &MinMaxPredicateValue,
) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = value.as_i64() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = value.as_i32() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Float(s) => {
            let Some(v) = value.as_f32() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = value.as_f64() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Boolean(s) => {
            let Some(v) = value.as_bool() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::ByteArray(s) => {
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(max.data() >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(v) = fixed_len_predicate_value_as_i128(value)
                && let Some(max) = s
                    .max_opt()
                    .and_then(|max| decode_signed_be_i128(max.data()))
            {
                return Ok(max >= v);
            }
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(max.data() >= v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

fn check_min_satisfies_le(
    stats: &Statistics,
    value: &MinMaxPredicateValue,
) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = value.as_i64() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = value.as_i32() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Float(s) => {
            let Some(v) = value.as_f32() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = value.as_f64() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Boolean(s) => {
            let Some(v) = value.as_bool() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::ByteArray(s) => {
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(min.data() <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(v) = fixed_len_predicate_value_as_i128(value)
                && let Some(min) = s
                    .min_opt()
                    .and_then(|min| decode_signed_be_i128(min.data()))
            {
                return Ok(min <= v);
            }
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(min.data() <= v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

fn check_max_satisfies_gt(
    stats: &Statistics,
    value: &MinMaxPredicateValue,
) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = value.as_i64() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = value.as_i32() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Float(s) => {
            let Some(v) = value.as_f32() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = value.as_f64() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Boolean(s) => {
            let Some(v) = value.as_bool() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max & !v)
            } else {
                Ok(true)
            }
        }
        Statistics::ByteArray(s) => {
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(max.data() > v)
            } else {
                Ok(true)
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(v) = fixed_len_predicate_value_as_i128(value)
                && let Some(max) = s
                    .max_opt()
                    .and_then(|max| decode_signed_be_i128(max.data()))
            {
                return Ok(max > v);
            }
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(max.data() > v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

fn check_min_satisfies_lt(
    stats: &Statistics,
    value: &MinMaxPredicateValue,
) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = value.as_i64() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = value.as_i32() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Float(s) => {
            let Some(v) = value.as_f32() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = value.as_f64() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Boolean(s) => {
            let Some(v) = value.as_bool() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(!*min & v)
            } else {
                Ok(true)
            }
        }
        Statistics::ByteArray(s) => {
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(min.data() < v)
            } else {
                Ok(true)
            }
        }
        Statistics::FixedLenByteArray(s) => {
            if let Some(v) = fixed_len_predicate_value_as_i128(value)
                && let Some(min) = s
                    .min_opt()
                    .and_then(|min| decode_signed_be_i128(min.data()))
            {
                return Ok(min < v);
            }
            let Some(v) = value.as_bytes() else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(min.data() < v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

fn fixed_len_predicate_value_as_i128(value: &MinMaxPredicateValue) -> Option<i128> {
    match value {
        MinMaxPredicateValue::Decimal128 { value, .. } => Some(*value),
        MinMaxPredicateValue::FixedLenByteArray(bytes) => decode_signed_be_i128(bytes),
        _ => None,
    }
}

fn decode_signed_be_i128(bytes: &[u8]) -> Option<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let fill = if bytes[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut buf = [fill; 16];
    let start = buf.len().saturating_sub(bytes.len());
    buf[start..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

fn row_group_start_offset(row_group: &RowGroupMetaData) -> Option<u64> {
    let mut start: Option<u64> = None;
    for column in row_group.columns() {
        let col_start = column.data_page_offset();
        if col_start < 0 {
            continue;
        }
        let col_start = col_start as u64;
        start = Some(match start {
            Some(v) => v.min(col_start),
            None => col_start,
        });
    }
    start
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::file::statistics::ValueStatistics;

    use crate::formats::parquet::BoundVariantPathPruningPredicate;

    fn test_scan_range() -> FileScanRange {
        FileScanRange {
            path: "memory.parquet".to_string(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: 0,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
        }
    }

    fn int64_parquet_metadata(values: Vec<i64>, stats: EnabledStatistics) -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(stats)
            .build();

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer =
                ArrowWriter::try_new(cursor, schema, Some(props)).expect("parquet writer");
            writer.write(&batch).expect("write parquet batch");
            writer.close().expect("close parquet writer");
        }

        let reader =
            SerializedFileReader::new(bytes::Bytes::from(buffer)).expect("metadata reader");
        reader.metadata().clone()
    }

    fn typed_and_residual_parquet_metadata() -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload.typed_value.a.typed_value", DataType::Int64, true),
            Field::new("payload.typed_value.a.value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(10),
                    Some(11),
                    Some(12),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("residual"),
                    None,
                    None,
                    None,
                    None,
                    None,
                ])) as ArrayRef,
            ],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer =
                ArrowWriter::try_new(cursor, schema, Some(props)).expect("parquet writer");
            writer.write(&batch).expect("write parquet batch");
            writer.close().expect("close parquet writer");
        }

        let reader =
            SerializedFileReader::new(bytes::Bytes::from(buffer)).expect("metadata reader");
        reader.metadata().clone()
    }

    fn variant_gt_i64_predicate(
        leaf_column_index: usize,
        value: i64,
    ) -> BoundVariantPathPruningPredicate {
        BoundVariantPathPruningPredicate {
            leaf_column_index,
            leaf_column_path: "a".to_string(),
            residual_value_column_index: None,
            residual_value_column_path: None,
            requested_type: DataType::Int64,
            predicate: MinMaxPredicate::Gt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int64(value),
            },
        }
    }

    #[test]
    fn variant_row_group_pruning_uses_bound_typed_leaf_stats() {
        let metadata = int64_parquet_metadata(vec![1, 2, 3, 10, 11, 12], EnabledStatistics::Chunk);
        let predicate = variant_gt_i64_predicate(0, 5);

        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &[predicate],
            &[],
            true,
        )
        .expect("row groups selected");

        assert_eq!(selected, vec![1]);
    }

    #[test]
    fn variant_row_group_pruning_keeps_group_when_residual_value_has_non_nulls() {
        let metadata = typed_and_residual_parquet_metadata();
        let mut predicate = variant_gt_i64_predicate(0, 5);
        predicate.residual_value_column_index = Some(1);
        predicate.residual_value_column_path = Some("payload.typed_value.a.value".to_string());

        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &[predicate],
            &[],
            true,
        )
        .expect("row groups selected");

        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn variant_row_group_pruning_reads_all_when_binding_or_stats_missing() {
        let metadata = int64_parquet_metadata(vec![1, 2, 3, 10, 11, 12], EnabledStatistics::Chunk);
        let out_of_range = variant_gt_i64_predicate(9, 5);
        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &[out_of_range],
            &[],
            true,
        )
        .expect("row groups selected");
        assert_eq!(selected, vec![0, 1]);

        let metadata_without_stats =
            int64_parquet_metadata(vec![1, 2, 3, 10, 11, 12], EnabledStatistics::None);
        let missing_stats = variant_gt_i64_predicate(0, 5);
        let selected = select_row_groups_for_range(
            &metadata_without_stats,
            &test_scan_range(),
            None,
            &[],
            &[missing_stats],
            &[],
            true,
        )
        .expect("row groups selected");
        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn variant_row_group_pruning_continues_after_unusable_variant_predicate() {
        let metadata = int64_parquet_metadata(vec![1, 2, 3, 10, 11, 12], EnabledStatistics::Chunk);
        let out_of_range = variant_gt_i64_predicate(9, 5);
        let usable = variant_gt_i64_predicate(0, 5);

        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &[out_of_range, usable],
            &[],
            true,
        )
        .expect("row groups selected");

        assert_eq!(selected, vec![1]);
    }

    #[test]
    fn fixed_len_decimal_stats_compare_numerically_with_binary_literal_width_mismatch() {
        let stats = Statistics::FixedLenByteArray(ValueStatistics::new(
            Some(FixedLenByteArray::from(ByteArray::from(vec![
                0x00, 0x00, 0x00, 0x00,
            ]))),
            Some(FixedLenByteArray::from(ByteArray::from(vec![
                0x00, 0x00, 0x4d, 0xf4,
            ]))),
            None,
            None,
            false,
        ));
        let predicate = MinMaxPredicateValue::FixedLenByteArray(vec![0; 16]);

        assert!(check_max_satisfies_gt(&stats, &predicate).expect("compare fixed-len stats"));
        assert!(check_min_satisfies_le(&stats, &predicate).expect("compare fixed-len stats"));
    }

    #[test]
    fn fixed_len_decimal_stats_compare_numerically_with_decimal_literal() {
        let stats = Statistics::FixedLenByteArray(ValueStatistics::new(
            Some(FixedLenByteArray::from(ByteArray::from(vec![
                0x00, 0x00, 0x00, 0x00,
            ]))),
            Some(FixedLenByteArray::from(ByteArray::from(vec![
                0x00, 0x00, 0x4d, 0xf4,
            ]))),
            None,
            None,
            false,
        ));
        let predicate = MinMaxPredicateValue::Decimal128 {
            value: 0,
            precision: 7,
            scale: 2,
        };

        assert!(check_max_satisfies_gt(&stats, &predicate).expect("compare decimal stats"));
        assert!(!check_min_satisfies_lt(&stats, &predicate).expect("compare decimal stats"));
    }
}
