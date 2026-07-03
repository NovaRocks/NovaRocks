use std::collections::HashMap;

use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue};
use crate::common::scan_predicate::{ScanPredicate, ScanPredicateDomain, ScanPredicateSource};
use crate::fs::scan_context::FileScanRange;
use crate::sql::catalog::{IcebergColumnStats, IcebergDataFileInfo, IcebergPartitionValue};

#[derive(Clone, Debug)]
pub struct IcebergFilePruningMetadata {
    pub(crate) columns: HashMap<String, IcebergColumnStats>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct IcebergFilePruningCounters {
    pub(crate) files_total: u128,
    pub(crate) files_selected: u128,
    pub(crate) files_pruned: u128,
    pub(crate) predicates: u128,
    pub(crate) partition_evaluated: u128,
    pub(crate) stats_evaluated: u128,
    pub(crate) unsupported: u128,
    pub(crate) unavailable: u128,
}

pub(crate) fn min_max_predicates_to_scan_predicates(
    predicates: &[MinMaxPredicate],
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .cloned()
        .map(|predicate| {
            ScanPredicate::from_min_max_predicate(predicate, ScanPredicateSource::Static)
        })
        .collect()
}

#[allow(dead_code)]
pub(crate) fn file_may_satisfy_min_max(
    file: &IcebergDataFileInfo,
    predicates: &[MinMaxPredicate],
) -> bool {
    let scan_predicates = min_max_predicates_to_scan_predicates(predicates);
    let mut counters = IcebergFilePruningCounters::default();
    file_may_satisfy_scan_predicates(file, &scan_predicates, &mut counters)
}

pub(crate) fn file_may_satisfy_scan_predicates(
    file: &IcebergDataFileInfo,
    predicates: &[ScanPredicate],
    counters: &mut IcebergFilePruningCounters,
) -> bool {
    counters.files_total += 1;
    counters.predicates += predicates.len() as u128;

    if predicates.is_empty() {
        counters.files_selected += 1;
        return true;
    }

    for predicate in predicates {
        if let Some(decision) = partition_may_satisfy_predicate(file, predicate) {
            match decision {
                PredicateDecision::Evaluated(may_satisfy) => {
                    counters.partition_evaluated += 1;
                    if !may_satisfy {
                        counters.files_pruned += 1;
                        return false;
                    }
                    continue;
                }
                PredicateDecision::Unsupported => {}
            }
        }

        match stats_may_satisfy_predicate(file.column_stats.as_ref(), predicate) {
            PredicateDecision::Evaluated(may_satisfy) => {
                counters.stats_evaluated += 1;
                if !may_satisfy {
                    counters.files_pruned += 1;
                    return false;
                }
            }
            PredicateDecision::Unsupported => {
                counters.unsupported += 1;
            }
        }
    }

    counters.files_selected += 1;
    true
}

pub(crate) fn iceberg_range_may_satisfy_scan_predicates(
    range: &FileScanRange,
    predicates: &[ScanPredicate],
    counters: &mut IcebergFilePruningCounters,
) -> bool {
    counters.files_total += 1;
    counters.predicates += predicates.len() as u128;

    if predicates.is_empty() {
        counters.files_selected += 1;
        return true;
    }

    let Some(metadata) = range.iceberg_file_pruning.as_ref() else {
        counters.unsupported += 1;
        counters.files_selected += 1;
        return true;
    };

    for predicate in predicates {
        match stats_may_satisfy_predicate(Some(&metadata.columns), predicate) {
            PredicateDecision::Evaluated(may_satisfy) => {
                counters.stats_evaluated += 1;
                if !may_satisfy {
                    counters.files_pruned += 1;
                    return false;
                }
            }
            PredicateDecision::Unsupported => {
                counters.unsupported += 1;
            }
        }
    }

    counters.files_selected += 1;
    true
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PredicateDecision {
    Evaluated(bool),
    Unsupported,
}

fn partition_may_satisfy_predicate(
    file: &IcebergDataFileInfo,
    predicate: &ScanPredicate,
) -> Option<PredicateDecision> {
    let partition = file.partition_values.iter().find(|value| {
        value.transform.eq_ignore_ascii_case("identity")
            && value.source_column.eq_ignore_ascii_case(predicate.column())
    })?;
    let Some(value) = partition.value.as_ref() else {
        return Some(PredicateDecision::Evaluated(false));
    };
    Some(partition_value_may_satisfy_predicate(value, predicate))
}

fn partition_value_may_satisfy_predicate(
    partition_value: &IcebergPartitionValue,
    predicate: &ScanPredicate,
) -> PredicateDecision {
    match predicate.domain() {
        ScanPredicateDomain::Range { op, value } => {
            partition_value_may_satisfy_range(partition_value, *op, value)
        }
        ScanPredicateDomain::DiscreteSet { values, .. } => {
            partition_value_may_satisfy_discrete_set(partition_value, values)
        }
    }
}

fn partition_value_may_satisfy_range(
    partition_value: &IcebergPartitionValue,
    op: MinMaxPredicateOp,
    value: &MinMaxPredicateValue,
) -> PredicateDecision {
    match partition_value {
        IcebergPartitionValue::Boolean(v) => match value.as_bool() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(
                i64::from(*v),
                i64::from(*v),
                op,
                i64::from(value),
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Int32(v) => match value.as_i64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(
                i64::from(*v),
                i64::from(*v),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Int64(v) => match value.as_i64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_i64(*v, *v, op, value)),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Float(v) => match value.as_f64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_f64(
                f64::from(*v),
                f64::from(*v),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Double(v) => match value.as_f64() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_f64(*v, *v, op, value)),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::String(v) => match value.as_bytes() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_bytes(
                v.as_bytes(),
                v.as_bytes(),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
        IcebergPartitionValue::Binary(v) => match value.as_bytes() {
            Some(value) => PredicateDecision::Evaluated(range_may_satisfy_bytes(
                v.as_slice(),
                v.as_slice(),
                op,
                value,
            )),
            None => PredicateDecision::Unsupported,
        },
    }
}

fn partition_value_may_satisfy_discrete_set(
    partition_value: &IcebergPartitionValue,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let any_match = match partition_value {
        IcebergPartitionValue::Boolean(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bool)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == *v)),
        IcebergPartitionValue::Int32(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_i64)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == i64::from(*v))),
        IcebergPartitionValue::Int64(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_i64)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == *v)),
        IcebergPartitionValue::Float(v) => {
            if v.is_nan() {
                None
            } else {
                values
                    .iter()
                    .map(MinMaxPredicateValue::as_f64)
                    .collect::<Option<Vec<_>>>()
                    .and_then(|values| {
                        if values.iter().any(|value| value.is_nan()) {
                            None
                        } else {
                            Some(values.into_iter().any(|value| value == f64::from(*v)))
                        }
                    })
            }
        }
        IcebergPartitionValue::Double(v) => {
            if v.is_nan() {
                None
            } else {
                values
                    .iter()
                    .map(MinMaxPredicateValue::as_f64)
                    .collect::<Option<Vec<_>>>()
                    .and_then(|values| {
                        if values.iter().any(|value| value.is_nan()) {
                            None
                        } else {
                            Some(values.into_iter().any(|value| value == *v))
                        }
                    })
            }
        }
        IcebergPartitionValue::String(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bytes)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == v.as_bytes())),
        IcebergPartitionValue::Binary(v) => values
            .iter()
            .map(MinMaxPredicateValue::as_bytes)
            .collect::<Option<Vec<_>>>()
            .map(|values| values.into_iter().any(|value| value == v.as_slice())),
    };

    match any_match {
        Some(any_match) => PredicateDecision::Evaluated(any_match),
        None => PredicateDecision::Unsupported,
    }
}

fn stats_may_satisfy_predicate(
    column_stats: Option<&HashMap<String, IcebergColumnStats>>,
    predicate: &ScanPredicate,
) -> PredicateDecision {
    let Some(column_stats) = column_stats else {
        return PredicateDecision::Unsupported;
    };
    let Some(stats) = find_column_stats(column_stats, predicate.column()) else {
        return PredicateDecision::Unsupported;
    };

    match predicate.domain() {
        ScanPredicateDomain::Range { op, value } => stats_may_satisfy_range(stats, *op, value),
        ScanPredicateDomain::DiscreteSet { values, .. } => {
            stats_may_satisfy_discrete_set(stats, values)
        }
    }
}

fn find_column_stats<'a>(
    column_stats: &'a HashMap<String, IcebergColumnStats>,
    column: &str,
) -> Option<&'a IcebergColumnStats> {
    column_stats.get(column).or_else(|| {
        column_stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn stats_may_satisfy_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: &MinMaxPredicateValue,
) -> PredicateDecision {
    if let Some(value) = value.as_bool() {
        return stats_may_satisfy_bool_range(stats, op, value);
    }
    if let Some(value) = value.as_i64() {
        return stats_may_satisfy_i64_range(stats, op, value);
    }
    if let Some(value) = value.as_f64() {
        return stats_may_satisfy_f64_range(stats, op, value);
    }
    if let Some(value) = value.as_bytes() {
        return stats_may_satisfy_bytes_range(stats, op, value);
    }
    PredicateDecision::Unsupported
}

fn stats_may_satisfy_bool_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: bool,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_i64(
        i64::from(lower),
        i64::from(upper),
        op,
        i64::from(value),
    ))
}

fn stats_may_satisfy_i64_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: i64,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_i64(lower, upper, op, value))
}

fn stats_may_satisfy_f64_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: f64,
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    if lower.is_nan() || upper.is_nan() || value.is_nan() {
        return PredicateDecision::Unsupported;
    }
    PredicateDecision::Evaluated(range_may_satisfy_f64(lower, upper, op, value))
}

fn stats_may_satisfy_bytes_range(
    stats: &IcebergColumnStats,
    op: MinMaxPredicateOp,
    value: &[u8],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(range_may_satisfy_bytes(lower, upper, op, value))
}

fn stats_may_satisfy_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(first) = values.first() else {
        return PredicateDecision::Unsupported;
    };

    if first.as_bool().is_some() {
        return stats_may_satisfy_bool_discrete_set(stats, values);
    }
    if first.as_i64().is_some() {
        return stats_may_satisfy_i64_discrete_set(stats, values);
    }
    if first.as_f64().is_some() {
        return stats_may_satisfy_f64_discrete_set(stats, values);
    }
    if first.as_bytes().is_some() {
        return stats_may_satisfy_bytes_discrete_set(stats, values);
    }

    PredicateDecision::Unsupported
}

fn stats_may_satisfy_bool_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_bool_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_bool)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    let lower = i64::from(lower);
    let upper = i64::from(upper);
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .map(i64::from)
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_i64_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_i64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_i64)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_f64_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_f64_bound) else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_f64)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    if lower.is_nan() || upper.is_nan() || values.iter().any(|value| value.is_nan()) {
        return PredicateDecision::Unsupported;
    }
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn stats_may_satisfy_bytes_discrete_set(
    stats: &IcebergColumnStats,
    values: &[MinMaxPredicateValue],
) -> PredicateDecision {
    let Some(lower) = stats.lower_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(upper) = stats.upper_bound.as_deref() else {
        return PredicateDecision::Unsupported;
    };
    let Some(values) = values
        .iter()
        .map(MinMaxPredicateValue::as_bytes)
        .collect::<Option<Vec<_>>>()
    else {
        return PredicateDecision::Unsupported;
    };
    PredicateDecision::Evaluated(
        values
            .into_iter()
            .any(|value| lower <= value && value <= upper),
    )
}

fn range_may_satisfy_i64(lower: i64, upper: i64, op: MinMaxPredicateOp, value: i64) -> bool {
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_f64(lower: f64, upper: f64, op: MinMaxPredicateOp, value: f64) -> bool {
    if lower.is_nan() || upper.is_nan() || value.is_nan() {
        return true;
    }
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_bytes(
    lower: &[u8],
    upper: &[u8],
    op: MinMaxPredicateOp,
    value: &[u8],
) -> bool {
    match op {
        MinMaxPredicateOp::Le => lower <= value,
        MinMaxPredicateOp::Ge => upper >= value,
        MinMaxPredicateOp::Lt => lower < value,
        MinMaxPredicateOp::Gt => upper > value,
        MinMaxPredicateOp::Eq => lower <= value && value <= upper,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_i64_bound(bytes: &[u8]) -> Option<i64> {
    match bytes.len() {
        1 => bytes.first().copied().map(i64::from),
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_f64_bound(bytes: &[u8]) -> Option<f64> {
    match bytes.len() {
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use arrow::datatypes::DataType;
    use thrift::OrderedFloat;

    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::common::scan_predicate::{ScanPredicate, ScanPredicateSource};
    use crate::connector::iceberg::file_pruning_wire::{
        iceberg_file_pruning_metadata_from_thrift, iceberg_file_pruning_metadata_to_thrift,
    };
    use crate::fs::scan_context::FileScanRange;
    use crate::sql::catalog::{ColumnDef, IcebergColumnStats, IcebergDataFileInfo};
    use crate::thrift::{descriptors, exprs, plan_nodes, types};

    use super::{
        IcebergFilePruningCounters, IcebergFilePruningMetadata, file_may_satisfy_scan_predicates,
        iceberg_range_may_satisfy_scan_predicates,
    };

    #[test]
    fn from_thrift_skips_malformed_min_max_entries_and_keeps_valid_entry() {
        let hdfs_range = hdfs_range_with_min_max_values(Some(BTreeMap::from([
            (
                -1,
                min_max_int_value(exprs::TExprNodeType::INT_LITERAL, Some(1), Some(2)),
            ),
            (
                0,
                min_max_int_value(exprs::TExprNodeType::INT_LITERAL, Some(10), Some(20)),
            ),
            (
                1,
                min_max_int_value(exprs::TExprNodeType::BOOL_LITERAL, Some(0), Some(2)),
            ),
            (
                2,
                min_max_int_value(exprs::TExprNodeType::INT_LITERAL, Some(7), None),
            ),
            (
                3,
                min_max_int_value(exprs::TExprNodeType::STRING_LITERAL, Some(1), Some(2)),
            ),
            (
                99,
                min_max_int_value(exprs::TExprNodeType::INT_LITERAL, Some(30), Some(40)),
            ),
        ])));
        let column_names = vec![
            "valid".to_string(),
            "bad_bool".to_string(),
            "missing_bound".to_string(),
            "unsupported".to_string(),
        ];

        let metadata = iceberg_file_pruning_metadata_from_thrift(&hdfs_range, &column_names)
            .expect("valid entry should remain");

        assert_eq!(metadata.columns.len(), 1);
        let valid = metadata.columns.get("valid").expect("valid stats");
        assert_eq!(valid.lower_bound, Some(10_i64.to_le_bytes().to_vec()));
        assert_eq!(valid.upper_bound, Some(20_i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn to_thrift_bridges_width_specific_numeric_stats_and_skips_nan_float() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.column_stats = Some(HashMap::from([
            (
                "tiny".to_string(),
                stats((-3_i8).to_le_bytes().to_vec(), 4_i8.to_le_bytes().to_vec()),
            ),
            (
                "regular".to_string(),
                stats(
                    (-100_i32).to_le_bytes().to_vec(),
                    200_i32.to_le_bytes().to_vec(),
                ),
            ),
            (
                "ratio".to_string(),
                stats(
                    1.25_f32.to_le_bytes().to_vec(),
                    9.5_f32.to_le_bytes().to_vec(),
                ),
            ),
            (
                "bad_float".to_string(),
                stats(
                    f32::NAN.to_le_bytes().to_vec(),
                    1.0_f32.to_le_bytes().to_vec(),
                ),
            ),
        ]));
        let columns = vec![
            column("tiny", DataType::Int8),
            column("regular", DataType::Int32),
            column("ratio", DataType::Float32),
            column("bad_float", DataType::Float32),
        ];

        let values =
            iceberg_file_pruning_metadata_to_thrift(&file, &columns).expect("min/max values");

        assert_eq!(values.len(), 3);
        let tiny = values.get(&0).expect("int8 stats");
        assert_eq!(tiny.type_, exprs::TExprNodeType::INT_LITERAL);
        assert_eq!(tiny.min_int_value, Some(-3));
        assert_eq!(tiny.max_int_value, Some(4));

        let regular = values.get(&1).expect("int32 stats");
        assert_eq!(regular.type_, exprs::TExprNodeType::INT_LITERAL);
        assert_eq!(regular.min_int_value, Some(-100));
        assert_eq!(regular.max_int_value, Some(200));

        let ratio = values.get(&2).expect("float32 stats");
        assert_eq!(ratio.type_, exprs::TExprNodeType::FLOAT_LITERAL);
        assert_eq!(
            ratio.min_float_value.map(|v| v.0),
            Some(f64::from(1.25_f32))
        );
        assert_eq!(ratio.max_float_value.map(|v| v.0), Some(f64::from(9.5_f32)));

        assert!(!values.contains_key(&3), "NaN stats must not be bridged");
    }

    #[test]
    fn range_predicate_skips_file_when_stats_do_not_overlap() {
        let file = data_file_with_i64_stats("k1", 10, 20);
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Gt {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(30),
            },
            ScanPredicateSource::Static,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_pruned, 1);
    }

    #[test]
    fn discrete_set_skips_file_when_values_are_outside_file_bounds() {
        let file = data_file_with_i64_stats("k1", 100, 200);
        let predicate = ScanPredicate::discrete_set(
            "k1".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete set");
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_pruned, 1);
    }

    #[test]
    fn discrete_set_identity_partition_skips_non_matching_point() {
        let file = data_file_with_identity_i64_partition("k1", 7);
        let predicate = ScanPredicate::discrete_set(
            "k1".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete set");
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_pruned, 1);
        assert_eq!(counters.partition_evaluated, 1);
        assert_eq!(counters.unsupported, 0);
    }

    #[test]
    fn discrete_set_identity_partition_keeps_matching_point() {
        let file = data_file_with_identity_i64_partition("k1", 7);
        let predicate = ScanPredicate::discrete_set(
            "k1".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(7),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete set");
        let mut counters = IcebergFilePruningCounters::default();

        assert!(file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_selected, 1);
        assert_eq!(counters.partition_evaluated, 1);
        assert_eq!(counters.unsupported, 0);
    }

    #[test]
    fn missing_stats_keeps_file() {
        let file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Le {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(0),
            },
            ScanPredicateSource::RuntimeMinMax,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.unsupported, 1);
    }

    #[test]
    fn range_predicate_uses_attached_file_stats() {
        let range = range_with_i64_stats("k1", 100, 200);
        let predicate = ScanPredicate::discrete_set(
            "k1".to_string(),
            vec![
                MinMaxPredicateValue::Int64(1),
                MinMaxPredicateValue::Int64(2),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete set");
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!iceberg_range_may_satisfy_scan_predicates(
            &range,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_total, 1);
        assert_eq!(counters.files_pruned, 1);
        assert_eq!(counters.stats_evaluated, 1);
    }

    #[test]
    fn range_predicate_missing_metadata_keeps_range() {
        let range = range_without_metadata();
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Eq {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(9),
            },
            ScanPredicateSource::RuntimeMinMax,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(iceberg_range_may_satisfy_scan_predicates(
            &range,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.files_selected, 1);
        assert_eq!(counters.unsupported, 1);
    }

    #[test]
    fn identity_partition_point_can_skip_file() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.partition_values.push(
            crate::sql::catalog::IcebergPartitionFieldValue::identity_int64_for_test("k1", 7),
        );
        let predicate = ScanPredicate::from_min_max_predicate(
            MinMaxPredicate::Eq {
                column: "k1".to_string(),
                value: MinMaxPredicateValue::Int64(9),
            },
            ScanPredicateSource::Static,
        );
        let mut counters = IcebergFilePruningCounters::default();

        assert!(!file_may_satisfy_scan_predicates(
            &file,
            &[predicate],
            &mut counters
        ));
        assert_eq!(counters.partition_evaluated, 1);
    }

    fn data_file_with_i64_stats(column: &str, lower: i64, upper: i64) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.column_stats = Some(HashMap::from([(
            column.to_string(),
            IcebergColumnStats {
                null_count: None,
                value_count: None,
                column_size: None,
                lower_bound: Some(lower.to_le_bytes().to_vec()),
                upper_bound: Some(upper.to_le_bytes().to_vec()),
            },
        )]));
        file
    }

    fn range_with_i64_stats(column: &str, lower: i64, upper: i64) -> FileScanRange {
        FileScanRange {
            path: "s3://bucket/data.parquet".to_string(),
            file_len: 10,
            offset: 0,
            length: 10,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: Some(IcebergFilePruningMetadata {
                columns: HashMap::from([(
                    column.to_string(),
                    IcebergColumnStats {
                        null_count: None,
                        value_count: None,
                        column_size: None,
                        lower_bound: Some(lower.to_le_bytes().to_vec()),
                        upper_bound: Some(upper.to_le_bytes().to_vec()),
                    },
                )]),
            }),
        }
    }

    fn range_without_metadata() -> FileScanRange {
        FileScanRange {
            path: "s3://bucket/data.parquet".to_string(),
            file_len: 10,
            offset: 0,
            length: 10,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }
    }

    fn stats(lower: Vec<u8>, upper: Vec<u8>) -> IcebergColumnStats {
        IcebergColumnStats {
            null_count: None,
            value_count: None,
            column_size: None,
            lower_bound: Some(lower),
            upper_bound: Some(upper),
        }
    }

    fn column(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn min_max_int_value(
        type_: exprs::TExprNodeType,
        min_int_value: Option<i64>,
        max_int_value: Option<i64>,
    ) -> exprs::TExprMinMaxValue {
        exprs::TExprMinMaxValue::new(
            type_,
            false,
            false,
            min_int_value,
            max_int_value,
            None::<OrderedFloat<f64>>,
            None::<OrderedFloat<f64>>,
        )
    }

    fn hdfs_range_with_min_max_values(
        min_max_values: Option<BTreeMap<i32, exprs::TExprMinMaxValue>>,
    ) -> plan_nodes::THdfsScanRange {
        plan_nodes::THdfsScanRange::new(
            None::<String>,
            Some(0_i64),
            Some(100_i64),
            None::<i64>,
            Some(256_i64),
            Some(descriptors::THdfsFileFormat::PARQUET),
            None::<descriptors::TTextFileDesc>,
            Some("s3://bucket/path/file.parquet".to_string()),
            None::<Vec<String>>,
            None::<bool>,
            None::<Vec<plan_nodes::TIcebergDeleteFile>>,
            None::<i64>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<i64>,
            None::<crate::thrift::data_cache::TDataCacheOptions>,
            None::<Vec<types::TSlotId>>,
            None::<bool>,
            None::<BTreeMap<String, String>>,
            None::<Vec<types::TSlotId>>,
            None::<bool>,
            None::<String>,
            None::<bool>,
            None::<String>,
            None::<String>,
            None::<plan_nodes::TPaimonDeletionFile>,
            None::<BTreeMap<types::TSlotId, exprs::TExpr>>,
            None::<descriptors::THdfsPartition>,
            None::<types::TTableId>,
            None::<plan_nodes::TDeletionVectorDescriptor>,
            None::<String>,
            None::<i64>,
            None::<bool>,
            min_max_values,
            None::<i32>,
            None::<i64>,
            None::<i64>,
            None::<Vec<i64>>,
        )
    }

    fn data_file_with_identity_i64_partition(column: &str, value: i64) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/data.parquet", 10, 1);
        file.partition_values.push(
            crate::sql::catalog::IcebergPartitionFieldValue::identity_int64_for_test(column, value),
        );
        file
    }
}
