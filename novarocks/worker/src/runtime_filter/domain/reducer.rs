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

use std::collections::BTreeSet;
use std::fmt;

use arrow::datatypes::DataType;

use novarocks_execution::runtime_filter::{
    RuntimeFilterNullSemantics,
    contribution::{MembershipValues, ValueDomainDelta},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReducerError {
    TypeMismatch,
    UnsupportedType,
    SizeOverflow,
}

impl fmt::Display for ReducerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch => write!(formatter, "runtime filter membership type mismatch"),
            Self::UnsupportedType => {
                write!(formatter, "unsupported runtime filter membership type")
            }
            Self::SizeOverflow => write!(formatter, "runtime filter reducer size overflow"),
        }
    }
}

impl std::error::Error for ReducerError {}

#[derive(Clone, Debug)]
pub struct MembershipReducer {
    data_type: DataType,
    null_semantics: RuntimeFilterNullSemantics,
    /// Backend-owned mutable union state. Execution supplies canonical deltas;
    /// it never owns this participant-local reduction or its retained bytes.
    domain: ValueDomainDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReducerProjection {
    retained_growth: usize,
}

impl ReducerProjection {
    pub const fn retained_growth(self) -> usize {
        self.retained_growth
    }
}

impl MembershipReducer {
    pub fn try_new(
        data_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
    ) -> Result<Self, ReducerError> {
        let values = empty_membership_values(&data_type).ok_or(ReducerError::UnsupportedType)?;
        Ok(Self {
            data_type,
            null_semantics,
            domain: ValueDomainDelta::new(values, false),
        })
    }

    pub fn preflight(&self, delta: &ValueDomainDelta) -> Result<ReducerProjection, ReducerError> {
        if !delta.matches_data_type(&self.data_type) {
            return Err(ReducerError::TypeMismatch);
        }
        let value_growth = projected_value_growth(self.domain.values(), delta.values())?;
        let null_growth = usize::from(
            delta.contains_null()
                && self.null_semantics == RuntimeFilterNullSemantics::NullSafeEqual
                && !self.domain.contains_null(),
        );
        Ok(ReducerProjection {
            retained_growth: value_growth
                .checked_add(null_growth)
                .ok_or(ReducerError::SizeOverflow)?,
        })
    }

    pub fn commit_preflighted(&mut self, delta: &ValueDomainDelta) -> Result<(), ReducerError> {
        let mut values = self.domain.values().clone();
        union_membership_values(&mut values, delta.values())?;
        let contains_null = self.domain.contains_null()
            || (delta.contains_null()
                && self.null_semantics == RuntimeFilterNullSemantics::NullSafeEqual);
        self.domain = ValueDomainDelta::new(values, contains_null);
        Ok(())
    }

    pub const fn domain(&self) -> &ValueDomainDelta {
        &self.domain
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub fn into_domain(self) -> ValueDomainDelta {
        self.domain
    }
}

fn empty_membership_values(data_type: &DataType) -> Option<MembershipValues> {
    Some(match data_type {
        DataType::Boolean => MembershipValues::boolean([]),
        DataType::Int8 => MembershipValues::int8([]),
        DataType::Int16 => MembershipValues::int16([]),
        DataType::Int32 => MembershipValues::int32([]),
        DataType::Int64 => MembershipValues::int64([]),
        DataType::FixedSizeBinary(16) => MembershipValues::large_int([]),
        DataType::FixedSizeBinary(width @ (10 | 12 | 14)) => MembershipValues::Temporal {
            carrier_width: *width,
            values: Default::default(),
        },
        DataType::Float32 => MembershipValues::float32([]),
        DataType::Float64 => MembershipValues::float64([]),
        DataType::Utf8 => MembershipValues::utf8(std::iter::empty::<String>()),
        DataType::Date32 => MembershipValues::date32([]),
        DataType::Timestamp(unit, timezone) => {
            MembershipValues::timestamp(*unit, timezone.as_ref().map(|value| value.to_string()), [])
        }
        DataType::Decimal128(precision, scale) => {
            MembershipValues::decimal128(*precision, *scale, []).ok()?
        }
        _ => return None,
    })
}

fn union_membership_values(
    current: &mut MembershipValues,
    incoming: &MembershipValues,
) -> Result<(), ReducerError> {
    macro_rules! union {
        ($left:expr, $right:expr) => {{
            $left.extend($right.iter().copied());
            Ok(())
        }};
    }
    match (current, incoming) {
        (MembershipValues::Boolean(left), MembershipValues::Boolean(right)) => union!(left, right),
        (MembershipValues::Int8(left), MembershipValues::Int8(right)) => union!(left, right),
        (MembershipValues::Int16(left), MembershipValues::Int16(right)) => union!(left, right),
        (MembershipValues::Int32(left), MembershipValues::Int32(right)) => union!(left, right),
        (MembershipValues::Int64(left), MembershipValues::Int64(right)) => union!(left, right),
        (MembershipValues::LargeInt(left), MembershipValues::LargeInt(right)) => {
            union!(left, right)
        }
        (
            MembershipValues::Temporal {
                carrier_width: left_width,
                values: left,
            },
            MembershipValues::Temporal {
                carrier_width: right_width,
                values: right,
            },
        ) if left_width == right_width => union!(left, right),
        (MembershipValues::Float32(left), MembershipValues::Float32(right)) => union!(left, right),
        (MembershipValues::Float64(left), MembershipValues::Float64(right)) => union!(left, right),
        (MembershipValues::Utf8(left), MembershipValues::Utf8(right)) => {
            left.extend(right.iter().cloned());
            Ok(())
        }
        (MembershipValues::Date32(left), MembershipValues::Date32(right)) => union!(left, right),
        (
            MembershipValues::Timestamp {
                unit: left_unit,
                timezone: left_timezone,
                values: left,
            },
            MembershipValues::Timestamp {
                unit: right_unit,
                timezone: right_timezone,
                values: right,
            },
        ) if left_unit == right_unit && left_timezone == right_timezone => union!(left, right),
        (
            MembershipValues::Decimal128 {
                precision: left_precision,
                scale: left_scale,
                values: left,
            },
            MembershipValues::Decimal128 {
                precision: right_precision,
                scale: right_scale,
                values: right,
            },
        ) if left_precision == right_precision && left_scale == right_scale => union!(left, right),
        _ => Err(ReducerError::TypeMismatch),
    }
}

fn missing_fixed_bytes<T: Ord>(
    current: &BTreeSet<T>,
    incoming: &BTreeSet<T>,
    width: usize,
) -> Result<usize, ReducerError> {
    incoming
        .iter()
        .filter(|value| !current.contains(*value))
        .count()
        .checked_mul(width)
        .ok_or(ReducerError::SizeOverflow)
}

fn projected_value_growth(
    current: &MembershipValues,
    incoming: &MembershipValues,
) -> Result<usize, ReducerError> {
    match (current, incoming) {
        (MembershipValues::Boolean(left), MembershipValues::Boolean(right)) => {
            missing_fixed_bytes(left, right, size_of::<bool>())
        }
        (MembershipValues::Int8(left), MembershipValues::Int8(right)) => {
            missing_fixed_bytes(left, right, size_of::<i8>())
        }
        (MembershipValues::Int16(left), MembershipValues::Int16(right)) => {
            missing_fixed_bytes(left, right, size_of::<i16>())
        }
        (MembershipValues::Int32(left), MembershipValues::Int32(right))
        | (MembershipValues::Date32(left), MembershipValues::Date32(right)) => {
            missing_fixed_bytes(left, right, size_of::<i32>())
        }
        (MembershipValues::Int64(left), MembershipValues::Int64(right)) => {
            missing_fixed_bytes(left, right, size_of::<i64>())
        }
        (MembershipValues::LargeInt(left), MembershipValues::LargeInt(right)) => {
            missing_fixed_bytes(left, right, size_of::<i128>())
        }
        (
            MembershipValues::Temporal {
                carrier_width: left_width,
                values: left,
            },
            MembershipValues::Temporal {
                carrier_width: right_width,
                values: right,
            },
        ) if left_width == right_width => missing_fixed_bytes(left, right, 12),
        (MembershipValues::Float32(left), MembershipValues::Float32(right)) => {
            missing_fixed_bytes(left, right, size_of::<u32>())
        }
        (MembershipValues::Float64(left), MembershipValues::Float64(right)) => {
            missing_fixed_bytes(left, right, size_of::<u64>())
        }
        (MembershipValues::Utf8(left), MembershipValues::Utf8(right)) => right
            .iter()
            .filter(|value| !left.contains(*value))
            .try_fold(0usize, |total, value| {
                total
                    .checked_add(value.len())
                    .ok_or(ReducerError::SizeOverflow)
            }),
        (
            MembershipValues::Timestamp {
                unit: left_unit,
                timezone: left_timezone,
                values: left,
            },
            MembershipValues::Timestamp {
                unit: right_unit,
                timezone: right_timezone,
                values: right,
            },
        ) if left_unit == right_unit && left_timezone == right_timezone => {
            missing_fixed_bytes(left, right, size_of::<i64>())
        }
        (
            MembershipValues::Decimal128 {
                precision: left_precision,
                scale: left_scale,
                values: left,
            },
            MembershipValues::Decimal128 {
                precision: right_precision,
                scale: right_scale,
                values: right,
            },
        ) if left_precision == right_precision && left_scale == right_scale => {
            missing_fixed_bytes(left, right, size_of::<i128>())
        }
        _ => Err(ReducerError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use novarocks_execution::runtime_filter::{
        RuntimeFilterNullSemantics,
        contribution::{MembershipValues, ValueDomainDelta},
    };

    use super::MembershipReducer;

    #[test]
    fn value_domain_union_accepts_unseen_out_of_order_deltas() {
        let mut reducer =
            MembershipReducer::try_new(DataType::Int64, RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();
        let first = ValueDomainDelta::new(MembershipValues::int64([30]), false);
        assert_eq!(reducer.preflight(&first).unwrap().retained_growth(), 8);
        reducer.commit_preflighted(&first).unwrap();
        let second = ValueDomainDelta::new(MembershipValues::int64([10, 20]), false);
        assert_eq!(reducer.preflight(&second).unwrap().retained_growth(), 16);
        reducer.commit_preflighted(&second).unwrap();

        assert_eq!(
            reducer.domain().values(),
            &MembershipValues::int64([10, 20, 30])
        );
    }

    #[test]
    fn reducer_deduplicates_values_and_retains_null_only_for_null_safe_equal() {
        let mut reducer =
            MembershipReducer::try_new(DataType::Int64, RuntimeFilterNullSemantics::NullSafeEqual)
                .unwrap();
        let first = ValueDomainDelta::new(MembershipValues::int64([1, 1]), true);
        assert_eq!(reducer.preflight(&first).unwrap().retained_growth(), 9);
        reducer.commit_preflighted(&first).unwrap();
        let second = ValueDomainDelta::new(MembershipValues::int64([1, 2]), true);
        assert_eq!(reducer.preflight(&second).unwrap().retained_growth(), 8);
        reducer.commit_preflighted(&second).unwrap();

        assert_eq!(reducer.domain().values(), &MembershipValues::int64([1, 2]));
        assert!(reducer.domain().contains_null());
    }

    #[test]
    fn reducer_rejects_type_mismatch_before_mutation() {
        let reducer =
            MembershipReducer::try_new(DataType::Int64, RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();
        assert!(
            reducer
                .preflight(&ValueDomainDelta::new(MembershipValues::int32([1]), false))
                .is_err()
        );
        assert!(reducer.domain().values().is_empty());
    }

    #[test]
    fn duplicate_value_projection_has_zero_growth() {
        let mut reducer =
            MembershipReducer::try_new(DataType::Int64, RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        reducer.commit_preflighted(&delta).unwrap();
        assert_eq!(reducer.preflight(&delta).unwrap().retained_growth(), 0);
    }

    #[test]
    fn reducer_uses_port_owned_empty_largeint_construction() {
        let data_type = MembershipValues::large_int([]).data_type();
        let reducer =
            MembershipReducer::try_new(data_type.clone(), RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();

        assert_eq!(reducer.domain().data_type(), data_type);
        assert!(reducer.domain().values().is_empty());
    }

    #[test]
    fn backend_union_preserves_timestamp_and_decimal_contracts() {
        let timestamp_type = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
        let mut timestamps =
            MembershipReducer::try_new(timestamp_type, RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();
        timestamps
            .commit_preflighted(&ValueDomainDelta::new(
                MembershipValues::timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None::<String>,
                    [7],
                ),
                false,
            ))
            .unwrap();
        assert_eq!(
            timestamps.domain().values(),
            &MembershipValues::timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None::<String>,
                [7],
            )
        );

        let decimal_type = DataType::Decimal128(12, 3);
        let mut decimals =
            MembershipReducer::try_new(decimal_type, RuntimeFilterNullSemantics::NeverMatches)
                .unwrap();
        decimals
            .commit_preflighted(&ValueDomainDelta::new(
                MembershipValues::decimal128(12, 3, [42]).unwrap(),
                false,
            ))
            .unwrap();
        assert_eq!(
            decimals.domain().values(),
            &MembershipValues::decimal128(12, 3, [42]).unwrap()
        );
    }
}
