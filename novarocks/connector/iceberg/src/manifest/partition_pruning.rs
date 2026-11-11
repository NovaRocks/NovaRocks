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

use std::cmp::Ordering;

use crate::delete_semantics::{CanonicalScalar, TypedPartition};
use crate::iceberg::spec::{
    Datum, FieldSummary, ManifestFile, PartitionSpec, PrimitiveType, Transform,
};

/// Only a definite spec or inclusive summary disjointness excludes a source.
/// A missing/unknown field contributes no evidence about another field.
pub(super) fn may_contain(
    manifest: &ManifestFile,
    spec: &PartitionSpec,
    target: &TypedPartition,
) -> bool {
    if manifest.partition_spec_id != target.spec_id() {
        return false;
    }
    let Some(summaries) = &manifest.partitions else {
        return true;
    };
    if summaries.len() != spec.fields().len() {
        return true;
    }
    !spec
        .fields()
        .iter()
        .zip(summaries)
        .zip(target.fields().iter().zip(target.values()))
        .any(|((field, summary), ((field_id, ty), value))| {
            field.field_id == *field_id
                && !matches!(field.transform, Transform::Unknown)
                && outside_summary(summary, ty, value.as_ref())
        })
}

fn outside_summary(
    summary: &FieldSummary,
    ty: &PrimitiveType,
    value: Option<&CanonicalScalar>,
) -> bool {
    let Some(value) = value else {
        return !summary.contains_null;
    };
    if is_nan(value) {
        return summary.contains_nan == Some(false);
    }
    let (Some(lower), Some(upper)) = (&summary.lower_bound, &summary.upper_bound) else {
        return false;
    };
    let (Some(lower), Some(upper)) = (decode_bound(ty, lower), decode_bound(ty, upper)) else {
        return false;
    };
    if compare(&lower, &upper).is_none_or(|ordering| ordering.is_gt()) {
        return false;
    }
    compare(value, &lower).is_some_and(|ordering| ordering.is_lt())
        || compare(value, &upper).is_some_and(|ordering| ordering.is_gt())
}

fn decode_bound(ty: &PrimitiveType, bytes: &[u8]) -> Option<CanonicalScalar> {
    // The SDK accepts any nonzero Boolean encoding. Such a bound cannot
    // prove exclusion. Fixed-length binary similarly requires its exact size.
    match ty {
        PrimitiveType::Boolean if !matches!(bytes, [0] | [1]) => return None,
        PrimitiveType::Fixed(size) if u64::try_from(bytes.len()).ok()? != *size => return None,
        _ => {}
    }
    let datum = Datum::try_from_bytes(bytes, ty.clone()).ok()?;
    let value = CanonicalScalar::from_literal(datum.literal(), ty).ok()?;
    (!is_nan(&value)).then_some(value)
}

fn is_nan(value: &CanonicalScalar) -> bool {
    match value {
        CanonicalScalar::Float(bits) => f32::from_bits(*bits).is_nan(),
        CanonicalScalar::Double(bits) => f64::from_bits(*bits).is_nan(),
        _ => false,
    }
}

fn compare(left: &CanonicalScalar, right: &CanonicalScalar) -> Option<Ordering> {
    use CanonicalScalar::*;
    Some(match (left, right) {
        (Boolean(a), Boolean(b)) => a.cmp(b),
        (Int(a), Int(b)) => a.cmp(b),
        (Long(a), Long(b)) => a.cmp(b),
        (Float(a), Float(b)) => f32::from_bits(*a).total_cmp(&f32::from_bits(*b)),
        (Double(a), Double(b)) => f64::from_bits(*a).total_cmp(&f64::from_bits(*b)),
        (Decimal(a), Decimal(b)) => a.cmp(b),
        (String(a), String(b)) => a.cmp(b),
        (Binary(a), Binary(b)) => a.cmp(b),
        // UUID comparison varies with the external writer's comparator.
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_inclusive_bounds_exclude_a_partition_value() {
        let summary = FieldSummary {
            contains_null: false,
            contains_nan: None,
            lower_bound: Some(Datum::int(5).to_bytes().unwrap()),
            upper_bound: Some(Datum::int(10).to_bytes().unwrap()),
        };
        // Old Int bounds are compared with the pinned, legally promoted Long type.
        for (value, excludes) in [(4, true), (5, false), (10, false), (11, true)] {
            assert_eq!(
                outside_summary(
                    &summary,
                    &PrimitiveType::Long,
                    Some(&CanonicalScalar::Long(value))
                ),
                excludes,
            );
        }
        assert!(outside_summary(&summary, &PrimitiveType::Long, None));
        let missing = FieldSummary {
            upper_bound: None,
            ..summary
        };
        assert!(!outside_summary(
            &missing,
            &PrimitiveType::Long,
            Some(&CanonicalScalar::Long(11))
        ));
        let nan = CanonicalScalar::Double(f64::NAN.to_bits());
        assert!(!outside_summary(
            &missing,
            &PrimitiveType::Double,
            Some(&nan)
        ));
        let known_no_nan = FieldSummary {
            contains_nan: Some(false),
            ..missing
        };
        assert!(outside_summary(
            &known_no_nan,
            &PrimitiveType::Double,
            Some(&nan)
        ));
    }
}
