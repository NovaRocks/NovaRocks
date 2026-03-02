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

use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

use crate::fs::scan_context::FileScanRange;
use crate::novarocks_logging::debug;

use super::{MinMaxPredicate, MinMaxPredicateValue};

pub(crate) fn select_row_groups_for_range(
    metadata: &ParquetMetaData,
    range: &FileScanRange,
    mut remaining_rows: Option<usize>,
    min_max_predicates: &[MinMaxPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> Option<Vec<usize>> {
    if range.length == 0 && remaining_rows.is_none() && min_max_predicates.is_empty() {
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
            if !min_max_predicates.is_empty() {
                match should_read_row_group(row_group, min_max_predicates, columns, case_sensitive)
                {
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
            "min_max filter: filtered {} row groups, kept {}",
            filtered_count,
            row_groups.len()
        );
    }

    Some(row_groups)
}

fn should_read_row_group(
    row_group: &RowGroupMetaData,
    predicates: &[MinMaxPredicate],
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

        if let Some(chunk) = chunk {
            if let Some(stats) = chunk.statistics() {
                let satisfies = match pred {
                    MinMaxPredicate::Le { value, .. } => check_min_satisfies_le(stats, value)?,
                    MinMaxPredicate::Ge { value, .. } => check_max_satisfies_ge(stats, value)?,
                    MinMaxPredicate::Lt { value, .. } => check_min_satisfies_lt(stats, value)?,
                    MinMaxPredicate::Gt { value, .. } => check_max_satisfies_gt(stats, value)?,
                    MinMaxPredicate::Eq { value, .. } => {
                        check_max_satisfies_ge(stats, value)?
                            && check_min_satisfies_le(stats, value)?
                    }
                };

                if !satisfies {
                    return Ok(false);
                }
            }
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
                Ok(*max > v)
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
                Ok(*min < v)
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
