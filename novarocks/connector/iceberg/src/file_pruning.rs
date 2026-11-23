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

//! File-level pruning against Iceberg manifest facts.
//!
//! ADR-0018 settled that one physical field-id predicate serves both file-level
//! and Parquet row-group pruning. This module is the file-level half: it judges
//! a data file using only the manifest -- identity partition values and column
//! min/max bounds -- so a file that cannot produce a TRUE row is never opened at
//! all. The row-group half lives in `novarocks-fs` and runs after the file is
//! already open, which is why it cannot substitute for this one.
//!
//! Both halves share the same judgement,
//! [`ScanPredicateDomain::may_match_bounds`], and the same binding rule: field
//! id first, column name as fallback.
//!
//! Pruning must be *sound*: a file may only be skipped when it provably cannot
//! produce a TRUE row. Skipping too much loses rows; keeping too much only costs
//! I/O. Every "cannot judge" therefore keeps the file.

use std::collections::HashMap;

use novarocks_fs::{MinMaxPredicateValue, ScanPredicate, ScanPredicateDomain};

use crate::scan_model::{
    IcebergColumnStats, IcebergDataFileInfo, IcebergPartitionValue, IcebergPhysicalPredicate,
    IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateValue,
};

/// Judge one data file against the frozen physical predicates.
///
/// Returns `true` when the file must be scanned.
pub fn file_may_satisfy_physical_predicates(
    file: &IcebergDataFileInfo,
    predicates: &[IcebergPhysicalPredicate],
) -> bool {
    for predicate in predicates {
        let temporal = matches!(
            match &predicate.domain {
                IcebergPhysicalPredicateDomain::Range { value, .. } => Some(value),
                IcebergPhysicalPredicateDomain::DiscreteSet { values } => values.first(),
            },
            Some(IcebergPhysicalPredicateValue::Temporal(_))
        );
        let physical = crate::file_reader::physical_predicates_to_file_predicates(
            std::slice::from_ref(predicate),
        );
        let Some(predicate) = physical.first() else {
            continue;
        };
        // An identity partition value is exact, so it outranks the column
        // bounds when it can decide.
        if let Some(may_satisfy) = partition_decision(file, predicate, temporal) {
            if !may_satisfy {
                return false;
            }
            continue;
        }
        if let Some(may_satisfy) = stats_decision(file, predicate, temporal)
            && !may_satisfy
        {
            return false;
        }
    }
    true
}

/// Decide from an identity partition value, or `None` when this predicate has
/// no identity partition to judge against.
///
/// Only `identity` is considered. A bucket/truncate/year/month transform maps
/// many source values onto one partition value, so a comparison on the source
/// column cannot be evaluated from the partition value alone.
///
/// Binding is by name here, unlike the statistics path: a partition field's
/// `source_column` is resolved from the same schema snapshot that resolved the
/// predicate, and partition pruning has no row-group-level counterpart that
/// could disagree with it.
fn partition_decision(
    file: &IcebergDataFileInfo,
    predicate: &ScanPredicate,
    temporal: bool,
) -> Option<bool> {
    let partition = file.partition_values.iter().find(|value| {
        value.transform.eq_ignore_ascii_case("identity")
            && if temporal {
                value.source_field_id.is_some()
                    && value.source_field_id == predicate.physical_field_id()
            } else {
                value.source_column.eq_ignore_ascii_case(predicate.column())
            }
    })?;
    if temporal && !matches!(partition.value, Some(IcebergPartitionValue::Int64(_))) {
        // A historic DATE partition may still carry an i32 count of days.
        // A missing value also cannot prove the original temporal unit.
        return None;
    }
    let Some(value) = partition.value.as_ref() else {
        // The partition value is NULL. None of the comparison or IN predicates
        // the provider can emit is satisfied by NULL, so the file is provably
        // empty for this predicate.
        return Some(false);
    };
    let point = partition_value_as_i64(value)?;
    let point = MinMaxPredicateValue::Int64(point);
    let domain = domain_as_i64(predicate.domain())?;
    Some(domain.may_match_bounds(&point, &point))
}

/// Decide from the manifest column bounds, or `None` when they cannot be
/// judged (no statistics, no bound pair, or an undecodable literal).
fn stats_decision(
    file: &IcebergDataFileInfo,
    predicate: &ScanPredicate,
    temporal: bool,
) -> Option<bool> {
    let columns = file.column_stats.as_ref()?;
    let stats = if temporal {
        let field_id = predicate.physical_field_id()?;
        columns
            .values()
            .find(|stats| stats.field_id == Some(field_id))?
    } else {
        find_column_stats(columns, predicate)?
    };
    let decode = |bytes: &[u8]| {
        if temporal {
            // The only legal temporal promotion that changes the physical
            // unit is DATE -> TIMESTAMP; DATE bounds are four bytes.
            Some(i64::from_le_bytes(bytes.try_into().ok()?))
        } else {
            decode_i64_bound(bytes)
        }
    };
    let lower = decode(stats.lower_bound.as_deref()?)?;
    let upper = decode(stats.upper_bound.as_deref()?)?;
    let domain = domain_as_i64(predicate.domain())?;
    Some(domain.may_match_bounds(
        &MinMaxPredicateValue::Int64(lower),
        &MinMaxPredicateValue::Int64(upper),
    ))
}

/// Bind statistics the same way the Parquet reader binds row-group statistics:
/// by Iceberg field id when the predicate carries one, falling back to the
/// column name. The fallback matters because a manifest entry predating the
/// field-id carrier decodes to `field_id: None`.
fn find_column_stats<'a>(
    column_stats: &'a HashMap<String, IcebergColumnStats>,
    predicate: &ScanPredicate,
) -> Option<&'a IcebergColumnStats> {
    if let Some(field_id) = predicate.physical_field_id()
        && let Some(stats) = column_stats
            .values()
            .find(|stats| stats.field_id == Some(field_id))
    {
        return Some(stats);
    }
    column_stats.get(predicate.column()).or_else(|| {
        column_stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(predicate.column()))
            .map(|(_, stats)| stats)
    })
}

/// Project a predicate domain onto the i64 domain used for manifest bounds.
///
/// ADR-0018 limits the provider's literals to boolean, int32, int64 and date32,
/// and every one of those maps onto i64 without loss. Normalising both sides
/// onto a single domain is what keeps the comparison well-typed across an
/// Iceberg `int -> long` promotion, where the bounds were written as four bytes
/// but the literal now arrives as an `Int64`.
///
/// `None` means "cannot judge", which keeps the file.
fn domain_as_i64(domain: &ScanPredicateDomain) -> Option<ScanPredicateDomain> {
    Some(match domain {
        ScanPredicateDomain::Range { op, value } => ScanPredicateDomain::Range {
            op: *op,
            value: MinMaxPredicateValue::Int64(literal_as_i64(value)?),
        },
        ScanPredicateDomain::DiscreteSet { values, .. } => {
            let values = values
                .iter()
                .map(literal_as_i64)
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .map(MinMaxPredicateValue::Int64)
                .collect::<Vec<_>>();
            // An empty set never reaches here in production: `IN ()` is
            // rejected at negotiation and signed as `Unsupported`
            // (ADR-0018 admits non-empty IN only). Treat it as unjudgeable
            // rather than inventing a verdict.
            let min = values.first()?.clone();
            let max = values.last()?.clone();
            ScanPredicateDomain::DiscreteSet { values, min, max }
        }
        // Membership only originates from runtime filters, which never reach
        // static split planning.
        ScanPredicateDomain::Membership { .. } => return None,
    })
}

fn literal_as_i64(value: &MinMaxPredicateValue) -> Option<i64> {
    match value {
        MinMaxPredicateValue::Boolean(value) => Some(i64::from(*value)),
        MinMaxPredicateValue::Int32(value) => Some(i64::from(*value)),
        MinMaxPredicateValue::Int64(value) => Some(*value),
        MinMaxPredicateValue::Date32(value) => Some(i64::from(*value)),
        _ => None,
    }
}

fn partition_value_as_i64(value: &IcebergPartitionValue) -> Option<i64> {
    match value {
        IcebergPartitionValue::Boolean(value) => Some(i64::from(*value)),
        IcebergPartitionValue::Int32(value) => Some(i64::from(*value)),
        IcebergPartitionValue::Int64(value) => Some(*value),
        // Float, Double, String and Binary partitions cannot be compared
        // against the provider's integral literals, so they stay unjudged.
        _ => None,
    }
}

/// Decode an untyped manifest bound as a little-endian integer.
///
/// Iceberg publishes bounds as the primitive's physical bytes with no type tag,
/// so the width is what identifies it: one byte for boolean, four for int and
/// date, eight for long. Widening here is exactly what makes a pre-promotion
/// four-byte bound comparable to an `Int64` literal.
fn decode_i64_bound(bytes: &[u8]) -> Option<i64> {
    match bytes.len() {
        1 => bytes.first().copied().map(i64::from),
        4 => Some(i64::from(i32::from_le_bytes(bytes.try_into().ok()?))),
        8 => Some(i64::from_le_bytes(bytes.try_into().ok()?)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan_model::{
        IcebergPartitionFieldValue, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
        IcebergPhysicalPredicateValue,
    };

    fn file_with_i32_stats(
        column: &str,
        field_id: Option<i32>,
        min: i32,
        max: i32,
    ) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/f.parquet", 128, 10);
        file.column_stats = Some(HashMap::from([(
            column.to_string(),
            IcebergColumnStats {
                field_id,
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: Some(min.to_le_bytes().to_vec()),
                upper_bound: Some(max.to_le_bytes().to_vec()),
            },
        )]));
        file
    }

    fn file_with_identity_partition(column: &str, value: i64) -> IcebergDataFileInfo {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/p.parquet", 128, 10);
        file.partition_values = vec![IcebergPartitionFieldValue::identity_int64_for_test(
            column, value,
        )];
        file
    }

    fn eq(
        column: &str,
        field_id: i32,
        value: IcebergPhysicalPredicateValue,
    ) -> IcebergPhysicalPredicate {
        IcebergPhysicalPredicate {
            column: column.to_string(),
            field_id,
            domain: IcebergPhysicalPredicateDomain::Range {
                op: IcebergPhysicalPredicateOp::Eq,
                value,
            },
        }
    }

    fn in_set(
        column: &str,
        field_id: i32,
        values: Vec<IcebergPhysicalPredicateValue>,
    ) -> IcebergPhysicalPredicate {
        IcebergPhysicalPredicate {
            column: column.to_string(),
            field_id,
            domain: IcebergPhysicalPredicateDomain::DiscreteSet { values },
        }
    }

    #[test]
    fn stats_prune_a_file_whose_bounds_exclude_the_literal() {
        let file = file_with_i32_stats("id", Some(7), 1, 5);
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    #[test]
    fn stats_keep_a_file_whose_bounds_contain_the_literal() {
        let file = file_with_i32_stats("id", Some(7), 10, 20);
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    /// An Iceberg `int -> long` promotion leaves four-byte bounds behind while
    /// the literal arrives as `Int64`. Both sides normalise onto i64, so the
    /// file is still judged rather than blindly kept.
    #[test]
    fn int_to_long_promotion_still_prunes_by_widening_the_bound() {
        let file = file_with_i32_stats("id", Some(7), 1, 5);
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int64(12));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));

        let file = file_with_i32_stats("id", Some(7), 10, 20);
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int64(12));
        assert!(file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    /// The column was renamed after the manifest was written, so the map key no
    /// longer matches the predicate. Binding by field id still finds it.
    #[test]
    fn statistics_bind_by_field_id_before_the_column_name() {
        let file = file_with_i32_stats("old_name", Some(7), 1, 5);
        let predicate = eq("new_name", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    /// Manifest entries predating the field-id carrier decode to `None`, so the
    /// name fallback has to keep working.
    #[test]
    fn statistics_fall_back_to_the_column_name_without_a_field_id() {
        let file = file_with_i32_stats("id", None, 1, 5);
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    #[test]
    fn identity_partition_decides_exactly() {
        let file = file_with_identity_partition("id", 12);
        let hit = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&hit)
        ));

        let miss = eq("id", 7, IcebergPhysicalPredicateValue::Int32(1));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&miss)
        ));
    }

    #[test]
    fn non_identity_transforms_are_not_judged() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/b.parquet", 128, 10);
        file.partition_values = vec![IcebergPartitionFieldValue {
            source_field_id: None,
            source_column: "id".to_string(),
            field_name: "id_bucket".to_string(),
            transform: "bucket[16]".to_string(),
            value: Some(IcebergPartitionValue::Int32(3)),
        }];
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    #[test]
    fn discrete_set_prunes_only_when_disjoint_from_the_bounds() {
        let file = file_with_i32_stats("id", Some(7), 10, 20);
        let disjoint = in_set(
            "id",
            7,
            vec![
                IcebergPhysicalPredicateValue::Int32(1),
                IcebergPhysicalPredicateValue::Int32(30),
            ],
        );
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&disjoint)
        ));

        let overlapping = in_set(
            "id",
            7,
            vec![
                IcebergPhysicalPredicateValue::Int32(1),
                IcebergPhysicalPredicateValue::Int32(12),
            ],
        );
        assert!(file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&overlapping)
        ));
    }

    #[test]
    fn boolean_and_date_literals_are_judged() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/d.parquet", 128, 10);
        file.column_stats = Some(HashMap::from([(
            "flag".to_string(),
            IcebergColumnStats {
                field_id: Some(3),
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: Some(vec![0]),
                upper_bound: Some(vec![0]),
            },
        )]));
        let truthy = eq("flag", 3, IcebergPhysicalPredicateValue::Boolean(true));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&truthy)
        ));

        let date_file = file_with_i32_stats("d", Some(4), 19_000, 19_010);
        let date = eq("d", 4, IcebergPhysicalPredicateValue::Date32(20_000));
        assert!(!file_may_satisfy_physical_predicates(
            &date_file,
            std::slice::from_ref(&date)
        ));
    }

    /// Degenerate inputs must never prune: no statistics, no bound pair, an
    /// unbindable column, and a partition value the literal cannot be compared
    /// against.
    #[test]
    fn unjudgeable_inputs_keep_the_file() {
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));

        let no_stats = IcebergDataFileInfo::for_test("s3://bucket/n.parquet", 128, 10);
        assert!(file_may_satisfy_physical_predicates(
            &no_stats,
            std::slice::from_ref(&predicate)
        ));

        let mut no_bounds = IcebergDataFileInfo::for_test("s3://bucket/n.parquet", 128, 10);
        no_bounds.column_stats = Some(HashMap::from([(
            "id".to_string(),
            IcebergColumnStats {
                field_id: Some(7),
                null_count: Some(0),
                value_count: Some(10),
                column_size: None,
                lower_bound: None,
                upper_bound: None,
            },
        )]));
        assert!(file_may_satisfy_physical_predicates(
            &no_bounds,
            std::slice::from_ref(&predicate)
        ));

        let other_column = file_with_i32_stats("unrelated", Some(99), 1, 5);
        assert!(file_may_satisfy_physical_predicates(
            &other_column,
            std::slice::from_ref(&predicate)
        ));

        let mut string_partition = IcebergDataFileInfo::for_test("s3://bucket/s.parquet", 128, 10);
        string_partition.partition_values = vec![IcebergPartitionFieldValue {
            source_field_id: None,
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some(IcebergPartitionValue::String("x".to_string())),
        }];
        assert!(file_may_satisfy_physical_predicates(
            &string_partition,
            std::slice::from_ref(&predicate)
        ));
    }

    #[test]
    fn null_identity_partition_prunes() {
        let mut file = IcebergDataFileInfo::for_test("s3://bucket/z.parquet", 128, 10);
        file.partition_values = vec![IcebergPartitionFieldValue {
            source_field_id: None,
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: None,
        }];
        let predicate = eq("id", 7, IcebergPhysicalPredicateValue::Int32(12));
        assert!(!file_may_satisfy_physical_predicates(
            &file,
            std::slice::from_ref(&predicate)
        ));
    }

    #[test]
    fn no_predicates_keeps_every_file() {
        let file = file_with_i32_stats("id", Some(7), 1, 5);
        assert!(file_may_satisfy_physical_predicates(&file, &[]));
    }

    /// Every predicate must hold; one disjoint predicate prunes the file even
    /// when another matches.
    #[test]
    fn predicates_are_conjunctive() {
        let mut file = file_with_i32_stats("id", Some(7), 10, 20);
        if let Some(stats) = file.column_stats.as_mut() {
            stats.insert(
                "other".to_string(),
                IcebergColumnStats {
                    field_id: Some(8),
                    null_count: Some(0),
                    value_count: Some(10),
                    column_size: None,
                    lower_bound: Some(1_i32.to_le_bytes().to_vec()),
                    upper_bound: Some(5_i32.to_le_bytes().to_vec()),
                },
            );
        }
        let predicates = vec![
            eq("id", 7, IcebergPhysicalPredicateValue::Int32(12)),
            eq("other", 8, IcebergPhysicalPredicateValue::Int32(99)),
        ];
        assert!(!file_may_satisfy_physical_predicates(&file, &predicates));
    }
}
