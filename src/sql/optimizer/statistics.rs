//! Statistics types for the cost-based optimizer.

use std::collections::HashMap;

use arrow::datatypes::DataType;

/// Per-column statistics derived from Iceberg file metadata.
#[derive(Clone, Debug)]
pub struct ColumnStatistic {
    pub min_value: f64,
    pub max_value: f64,
    pub nulls_fraction: f64,
    pub average_row_size: f64,
    pub distinct_values_count: f64,
}

impl ColumnStatistic {
    pub fn unknown() -> Self {
        Self {
            min_value: f64::NEG_INFINITY,
            max_value: f64::INFINITY,
            nulls_fraction: 0.0,
            average_row_size: 8.0,
            distinct_values_count: 1.0,
        }
    }
}

/// Operator-level statistics propagated through the plan tree.
#[derive(Clone, Debug)]
pub struct Statistics {
    pub output_row_count: f64,
    pub column_statistics: HashMap<String, ColumnStatistic>,
}

impl Statistics {
    pub fn avg_row_size(&self) -> f64 {
        if self.column_statistics.is_empty() {
            8.0
        } else {
            self.column_statistics
                .values()
                .map(|c| c.average_row_size)
                .sum()
        }
    }

    pub fn compute_size(&self) -> f64 {
        self.output_row_count * self.avg_row_size()
    }
}

/// Three-dimensional cost estimate (aligned with StarRocks CostEstimate).
#[derive(Clone, Debug, Default)]
pub struct CostEstimate {
    pub cpu_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
}

impl CostEstimate {
    pub fn total_cost(&self) -> f64 {
        self.cpu_cost * 0.5 + self.memory_cost * 2.0 + self.network_cost * 1.5
    }

    #[allow(dead_code)] // used by cost model tests
    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            cpu_cost: self.cpu_cost + other.cpu_cost,
            memory_cost: self.memory_cost + other.memory_cost,
            network_cost: self.network_cost + other.network_cost,
        }
    }
}

/// Table-level statistics aggregated from file metadata.
#[derive(Clone, Debug)]
pub struct TableStatistics {
    pub row_count: u64,
    pub column_stats: HashMap<String, ColumnStatistic>,
}

/// Build table-level statistics from `S3FileInfo` entries.
///
/// Aggregates row counts and per-column Iceberg statistics across all files.
/// Returns `None` if no file has a row count (e.g., non-Iceberg sources).
///
/// `columns`, when provided, supplies the per-column Arrow data type used to
/// decode manifest `lower_bound`/`upper_bound` bytes into numeric `min_value`
/// / `max_value` ranges. Without it, bounds stay at +/-infinity (the legacy
/// behavior).
#[allow(dead_code)] // kept for tests and external callers that do not have column schema handy
pub fn build_table_statistics(
    files: &[crate::sql::catalog::S3FileInfo],
) -> Option<TableStatistics> {
    build_table_statistics_with_columns(files, &[])
}

/// Like `build_table_statistics`, but also decodes manifest min/max bounds
/// using the supplied column schema. The `columns` slice should match
/// `TableDef::columns` so that `column.name` maps to the correct Arrow
/// `DataType` for decoding.
pub fn build_table_statistics_with_columns(
    files: &[crate::sql::catalog::S3FileInfo],
    columns: &[crate::sql::catalog::ColumnDef],
) -> Option<TableStatistics> {
    build_table_statistics_with_ndv(files, columns, &HashMap::new(), &HashMap::new())
}

/// Like `build_table_statistics_with_columns`, but additionally accepts an
/// Iceberg Puffin NDV map keyed by column name (lowercased) so that the
/// optimizer can use precise Theta-sketch cardinality where available.
///
/// `name_to_field_id` is unused by this function (NDV is keyed by name to
/// match the column lookup), but is retained on the signature so callers can
/// pre-compute it from `IcebergSchemaDef` once per query.
///
/// Priority for `distinct_values_count`:
///   1. Puffin NDV when present for the column.
///   2. Manifest `value_counts` (as an upper bound).
///   3. `sqrt(non_null) * 10` heuristic.
pub fn build_table_statistics_with_ndv(
    files: &[crate::sql::catalog::S3FileInfo],
    columns: &[crate::sql::catalog::ColumnDef],
    ndv_by_name: &HashMap<String, f64>,
    _name_to_field_id: &HashMap<String, i32>,
) -> Option<TableStatistics> {
    // Need at least one file with a row count to produce meaningful stats.
    let all_have_row_count = !files.is_empty() && files.iter().all(|f| f.row_count.is_some());
    if !all_have_row_count {
        return None;
    }

    let total_rows: u64 = files
        .iter()
        .map(|f| f.row_count.unwrap().max(0) as u64)
        .sum();

    // Build a column name → Arrow type lookup for bound decoding.
    let type_by_name: HashMap<&str, &DataType> = columns
        .iter()
        .map(|c| (c.name.as_str(), &c.data_type))
        .collect();

    // Aggregate per-column stats across files.
    let mut col_null_total: HashMap<String, i64> = HashMap::new();
    let mut col_value_total: HashMap<String, i64> = HashMap::new();
    let mut col_size_total: HashMap<String, i64> = HashMap::new();
    let mut col_count: HashMap<String, u64> = HashMap::new();
    let mut col_min: HashMap<String, f64> = HashMap::new();
    let mut col_max: HashMap<String, f64> = HashMap::new();

    for file in files {
        if let Some(ref cs) = file.column_stats {
            for (col_name, stats) in cs {
                *col_count.entry(col_name.clone()).or_default() += 1;
                if let Some(nc) = stats.null_count {
                    *col_null_total.entry(col_name.clone()).or_default() += nc;
                }
                if let Some(vc) = stats.value_count {
                    *col_value_total.entry(col_name.clone()).or_default() += vc;
                }
                if let Some(sz) = stats.column_size {
                    *col_size_total.entry(col_name.clone()).or_default() += sz;
                }
                if let Some(dtype) = type_by_name.get(col_name.as_str()) {
                    if let Some(bytes) = stats.lower_bound.as_deref()
                        && let Some(lo) = decode_bound_to_f64(bytes, dtype)
                    {
                        let entry = col_min.entry(col_name.clone()).or_insert(lo);
                        if lo < *entry {
                            *entry = lo;
                        }
                    }
                    if let Some(bytes) = stats.upper_bound.as_deref()
                        && let Some(hi) = decode_bound_to_f64(bytes, dtype)
                    {
                        let entry = col_max.entry(col_name.clone()).or_insert(hi);
                        if hi > *entry {
                            *entry = hi;
                        }
                    }
                }
            }
        }
    }

    let num_files = files.len() as u64;
    let mut column_stats = HashMap::new();
    for (col_name, count) in &col_count {
        // Only include columns that appear in all files for consistency.
        if *count < num_files {
            continue;
        }
        let nulls = col_null_total.get(col_name).copied().unwrap_or(0);
        let nulls_fraction = if total_rows > 0 {
            nulls as f64 / total_rows as f64
        } else {
            0.0
        };
        let avg_row_size = if total_rows > 0 {
            let total_size = col_size_total.get(col_name).copied().unwrap_or(0);
            total_size as f64 / total_rows as f64
        } else {
            8.0
        };
        let min_value = col_min.get(col_name).copied().unwrap_or(f64::NEG_INFINITY);
        let max_value = col_max.get(col_name).copied().unwrap_or(f64::INFINITY);
        let value_count = col_value_total.get(col_name).copied();
        column_stats.insert(
            col_name.clone(),
            ColumnStatistic {
                min_value,
                max_value,
                nulls_fraction,
                average_row_size: if avg_row_size > 0.0 {
                    avg_row_size
                } else {
                    8.0
                },
                // NDV priority:
                //   1. Iceberg Puffin theta sketch when present.
                //   2. Manifest value_counts as an upper bound.
                //   3. sqrt(non_null) * 10 heuristic.
                // value_count is total rows including nulls, so cap by
                // non_null rows.
                distinct_values_count: {
                    let non_null = (total_rows as f64 * (1.0 - nulls_fraction)).max(1.0);
                    let key = col_name.to_lowercase();
                    if let Some(&ndv) = ndv_by_name.get(&key) {
                        ndv.min(non_null).max(1.0)
                    } else {
                        match value_count {
                            Some(vc) if vc > 0 => (vc as f64).min(non_null).max(1.0),
                            _ => (non_null.sqrt() * 10.0).min(non_null).max(1.0),
                        }
                    }
                },
            },
        );
    }

    Some(TableStatistics {
        row_count: total_rows,
        column_stats,
    })
}

/// Decode an Iceberg manifest lower/upper bound byte payload into a numeric
/// `f64` based on the column's Arrow data type. Returns `None` for types that
/// do not have a meaningful numeric ordering (strings, binary, nested).
///
/// Encoding follows the Iceberg spec (see `Datum::to_bytes`):
/// - BOOLEAN: 1 byte, 0 or 1
/// - INT: 4-byte little-endian i32
/// - LONG / DATE+epoch days are encoded as INT; TIMESTAMP/TIMESTAMPTZ as LONG
/// - FLOAT: 4-byte little-endian f32
/// - DOUBLE: 8-byte little-endian f64
/// - DECIMAL: big-endian two's-complement unscaled, truncated to min bytes
fn decode_bound_to_f64(bytes: &[u8], dtype: &DataType) -> Option<f64> {
    match dtype {
        DataType::Boolean => match bytes {
            [0] => Some(0.0),
            [1] => Some(1.0),
            _ => None,
        },
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Date32 => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(i32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                Some(i64::from_le_bytes(arr) as f64)
            } else {
                None
            }
        }
        DataType::Time32(_) => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(i32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Float32 => {
            if bytes.len() == 4 {
                let arr: [u8; 4] = bytes.try_into().ok()?;
                Some(f64::from(f32::from_le_bytes(arr)))
            } else {
                None
            }
        }
        DataType::Float64 => {
            if bytes.len() == 8 {
                let arr: [u8; 8] = bytes.try_into().ok()?;
                Some(f64::from_le_bytes(arr))
            } else {
                None
            }
        }
        DataType::Decimal128(_, scale) => decode_decimal_be_bytes(bytes, *scale as i32),
        DataType::Decimal256(_, scale) => decode_decimal_be_bytes(bytes, *scale as i32),
        // Strings, binary, nested and other types have no meaningful numeric
        // ordering for optimizer cost; leave bounds unset.
        _ => None,
    }
}

/// Decode a big-endian two's-complement unscaled decimal byte payload into an
/// approximate `f64` using the given scale. Lossy for large precision but
/// sufficient as a cost-model bound.
fn decode_decimal_be_bytes(bytes: &[u8], scale: i32) -> Option<f64> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    // Sign-extend to 16 bytes.
    let sign_byte = bytes[0];
    let is_negative = sign_byte & 0x80 != 0;
    let mut buf = [if is_negative { 0xFF } else { 0x00 }; 16];
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    let raw = i128::from_be_bytes(buf);
    let pow = 10f64.powi(scale);
    if pow == 0.0 {
        None
    } else {
        Some(raw as f64 / pow)
    }
}

/// Selectivity constants aligned with StarRocks StatisticsEstimateCoefficient.
pub const PREDICATE_UNKNOWN_FILTER: f64 = 0.25;
pub const IS_NULL_FILTER: f64 = 0.1;
pub const IN_PREDICATE_DEFAULT_FILTER: f64 = 0.5;
pub const UNKNOWN_GROUP_BY_CORRELATION: f64 = 0.75;
pub const ANTI_JOIN_SELECTIVITY: f64 = 0.4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_estimate_total() {
        let cost = CostEstimate {
            cpu_cost: 100.0,
            memory_cost: 50.0,
            network_cost: 0.0,
        };
        assert!((cost.total_cost() - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn cost_estimate_add() {
        let a = CostEstimate {
            cpu_cost: 10.0,
            memory_cost: 20.0,
            network_cost: 5.0,
        };
        let b = CostEstimate {
            cpu_cost: 30.0,
            memory_cost: 10.0,
            network_cost: 15.0,
        };
        let c = a.add(&b);
        assert!((c.cpu_cost - 40.0).abs() < f64::EPSILON);
        assert!((c.memory_cost - 30.0).abs() < f64::EPSILON);
        assert!((c.network_cost - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn statistics_compute_size() {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            "a".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 100.0,
                nulls_fraction: 0.0,
                average_row_size: 4.0,
                distinct_values_count: 50.0,
            },
        );
        col_stats.insert(
            "b".to_string(),
            ColumnStatistic {
                min_value: 0.0,
                max_value: 1000.0,
                nulls_fraction: 0.1,
                average_row_size: 8.0,
                distinct_values_count: 200.0,
            },
        );
        let stats = Statistics {
            output_row_count: 1000.0,
            column_statistics: col_stats,
        };
        assert!((stats.compute_size() - 12000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn statistics_empty_columns_default_size() {
        let stats = Statistics {
            output_row_count: 100.0,
            column_statistics: HashMap::new(),
        };
        assert!((stats.avg_row_size() - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn column_statistic_unknown() {
        let cs = ColumnStatistic::unknown();
        assert!(cs.min_value.is_infinite());
        assert_eq!(cs.distinct_values_count, 1.0);
    }

    #[test]
    fn decode_int_bound_le_bytes() {
        let bytes = (-12345_i32).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Int32).expect("decode int");
        assert!((v - -12345.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_long_bound_le_bytes() {
        let bytes = (9_876_543_210_i64).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Int64).expect("decode long");
        assert!((v - 9_876_543_210.0).abs() < 1.0);
    }

    #[test]
    fn decode_double_bound_le_bytes() {
        let bytes = (3.14159_f64).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Float64).expect("decode double");
        assert!((v - 3.14159).abs() < 1e-9);
    }

    #[test]
    fn decode_float_bound_le_bytes() {
        let bytes = (2.5_f32).to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Float32).expect("decode float");
        assert!((v - 2.5).abs() < 1e-6);
    }

    #[test]
    fn decode_boolean_bound() {
        let lo = decode_bound_to_f64(&[0u8], &DataType::Boolean).expect("decode false");
        let hi = decode_bound_to_f64(&[1u8], &DataType::Boolean).expect("decode true");
        assert_eq!(lo, 0.0);
        assert_eq!(hi, 1.0);
    }

    #[test]
    fn decode_timestamp_bound_le_bytes() {
        // 2026-01-01T00:00:00Z in microseconds-since-epoch
        let micros: i64 = 1_767_225_600_000_000;
        let bytes = micros.to_le_bytes();
        use arrow::datatypes::TimeUnit;
        let v = decode_bound_to_f64(&bytes, &DataType::Timestamp(TimeUnit::Microsecond, None))
            .expect("decode ts");
        assert!((v - micros as f64).abs() < 1.0);
    }

    #[test]
    fn decode_date_bound_le_bytes() {
        let days: i32 = 20_454; // ~2026-01-01
        let bytes = days.to_le_bytes();
        let v = decode_bound_to_f64(&bytes, &DataType::Date32).expect("decode date");
        assert!((v - 20_454.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decode_string_bound_returns_none() {
        let bytes = b"hello";
        assert!(decode_bound_to_f64(bytes, &DataType::Utf8).is_none());
    }

    #[test]
    fn decode_truncated_int_bytes_returns_none() {
        let bytes = [0u8, 1u8]; // too short for i32
        assert!(decode_bound_to_f64(&bytes, &DataType::Int32).is_none());
    }

    #[test]
    fn decode_decimal_be_bytes_basic() {
        // Decimal(10, 2) value = 12345 → 123.45
        let raw: i128 = 12345;
        // Big-endian, minimum bytes (truncated).
        let be = raw.to_be_bytes();
        // Strip leading zero bytes per Iceberg spec.
        let start = be.iter().position(|&b| b != 0).unwrap_or(15);
        let bytes = &be[start..];
        let v = decode_decimal_be_bytes(bytes, 2).expect("decode decimal");
        assert!((v - 123.45).abs() < 1e-9);
    }

    #[test]
    fn decode_decimal_be_bytes_negative() {
        let raw: i128 = -250;
        let be = raw.to_be_bytes();
        // Negative values: pick a minimal sign-extension slice. Use last 2 bytes
        // since -250 fits in i16 range (0xFF06).
        let bytes = &be[14..];
        let v = decode_decimal_be_bytes(bytes, 1).expect("decode neg decimal");
        assert!((v - -25.0).abs() < 1e-9);
    }

    #[test]
    fn build_table_statistics_decodes_int_min_max() {
        use crate::sql::catalog::{ColumnDef, IcebergColumnStats, S3FileInfo};

        let file = S3FileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(100),
            column_stats: Some(HashMap::from([(
                "a".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(60),
                    column_size: Some(400),
                    lower_bound: Some(10_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(100_i32.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "a".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let ts = build_table_statistics_with_columns(&[file], &cols).expect("table stats present");
        let col = ts.column_stats.get("a").expect("col stats present");
        assert!((col.min_value - 10.0).abs() < f64::EPSILON);
        assert!((col.max_value - 100.0).abs() < f64::EPSILON);
        // value_count=60 should drive NDV
        assert!((col.distinct_values_count - 60.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_table_statistics_skips_string_bounds() {
        use crate::sql::catalog::{ColumnDef, IcebergColumnStats, S3FileInfo};

        let file = S3FileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(50),
            column_stats: Some(HashMap::from([(
                "name".to_string(),
                IcebergColumnStats {
                    null_count: Some(5),
                    value_count: None,
                    column_size: Some(200),
                    lower_bound: Some(b"alice".to_vec()),
                    upper_bound: Some(b"zoe".to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "name".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let ts = build_table_statistics_with_columns(&[file], &cols).expect("table stats present");
        let col = ts.column_stats.get("name").expect("col stats present");
        // String bounds are not decoded, so min/max stay at +/-infinity.
        assert!(col.min_value.is_infinite() && col.min_value.is_sign_negative());
        assert!(col.max_value.is_infinite() && col.max_value.is_sign_positive());
    }

    #[test]
    fn build_table_statistics_without_columns_uses_heuristic_ndv() {
        use crate::sql::catalog::{IcebergColumnStats, S3FileInfo};

        let file = S3FileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(10_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: None,
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let ts = build_table_statistics(&[file]).expect("table stats present");
        let col = ts.column_stats.get("x").expect("col stats present");
        // No value_count → fallback heuristic = sqrt(10000)*10 = 1000.0
        assert!((col.distinct_values_count - 1000.0).abs() < 1.0);
    }

    #[test]
    fn build_table_statistics_with_ndv_overrides_value_count_heuristic() {
        use crate::sql::catalog::{ColumnDef, IcebergColumnStats, S3FileInfo};

        let file = S3FileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(10_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    // Manifest value_count would give NDV=8000; the Puffin
                    // NDV must override.
                    value_count: Some(8000),
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("x".to_string(), 1234.0);
        let ts = build_table_statistics_with_ndv(&[file], &cols, &ndv_by_name, &HashMap::new())
            .expect("table stats");
        let col = ts.column_stats.get("x").expect("col stats present");
        // Puffin NDV (1234) wins over manifest value_count (8000) and the
        // heuristic (sqrt(10000)*10 = 1000).
        assert!((col.distinct_values_count - 1234.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_table_statistics_with_ndv_clamps_to_non_null_count() {
        use crate::sql::catalog::{ColumnDef, IcebergColumnStats, S3FileInfo};

        let file = S3FileInfo {
            path: "f1.parquet".to_string(),
            size: 100,
            row_count: Some(1_000),
            column_stats: Some(HashMap::from([(
                "x".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: Some(1000),
                    column_size: None,
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        };
        let cols = vec![ColumnDef {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        // NDV overshoots row count — clamp.
        let mut ndv_by_name = HashMap::new();
        ndv_by_name.insert("x".to_string(), 1e7);
        let ts = build_table_statistics_with_ndv(&[file], &cols, &ndv_by_name, &HashMap::new())
            .expect("table stats");
        let col = ts.column_stats.get("x").expect("col stats present");
        // Clamped to non_null = 1000.
        assert!((col.distinct_values_count - 1000.0).abs() < f64::EPSILON);
    }
}
