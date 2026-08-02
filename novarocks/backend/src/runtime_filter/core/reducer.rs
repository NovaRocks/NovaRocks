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

use novarocks::runtime_filter_transition::model::contract::NullSemantics;
use novarocks::runtime_filter_transition::port::value_domain::{
    ContributionSizeError, MembershipValues, ReducedMembershipDomain, ValueDomainDelta,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReducerError {
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

impl From<ContributionSizeError> for ReducerError {
    fn from(_: ContributionSizeError) -> Self {
        Self::SizeOverflow
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MembershipReducer {
    data_type: DataType,
    null_semantics: NullSemantics,
    domain: ReducedMembershipDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReducerProjection {
    retained_growth: usize,
}

impl ReducerProjection {
    pub(crate) const fn retained_growth(self) -> usize {
        self.retained_growth
    }
}

impl MembershipReducer {
    pub(crate) fn try_new(
        data_type: DataType,
        null_semantics: NullSemantics,
    ) -> Result<Self, ReducerError> {
        let values = MembershipValues::empty_for_data_type(&data_type)
            .ok_or(ReducerError::UnsupportedType)?;
        Ok(Self {
            data_type,
            null_semantics,
            domain: ReducedMembershipDomain::new(values, false),
        })
    }

    pub(crate) fn preflight(
        &self,
        delta: &ValueDomainDelta,
    ) -> Result<ReducerProjection, ReducerError> {
        if !delta.matches_data_type(&self.data_type) {
            return Err(ReducerError::TypeMismatch);
        }
        let value_growth = projected_value_growth(self.domain.values(), delta.values())?;
        let null_growth =
            usize::from(delta.retains_null(self.null_semantics) && !self.domain.contains_null());
        Ok(ReducerProjection {
            retained_growth: value_growth
                .checked_add(null_growth)
                .ok_or(ReducerError::SizeOverflow)?,
        })
    }

    pub(crate) fn commit_preflighted(
        &mut self,
        delta: &ValueDomainDelta,
    ) -> Result<(), ReducerError> {
        self.domain
            .union_prevalidated(delta.values(), delta.retains_null(self.null_semantics))
            .map_err(|_| ReducerError::TypeMismatch)
    }

    pub(crate) const fn domain(&self) -> &ReducedMembershipDomain {
        &self.domain
    }

    pub(crate) fn into_domain(self) -> ReducedMembershipDomain {
        self.domain
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
        (MembershipValues::Decimal128(left), MembershipValues::Decimal128(right))
            if left.precision() == right.precision() && left.scale() == right.scale() =>
        {
            missing_fixed_bytes(left.values(), right.values(), size_of::<i128>())
        }
        _ => Err(ReducerError::TypeMismatch),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use novarocks::runtime_filter_transition::model::contract::NullSemantics;
    use novarocks::runtime_filter_transition::port::value_domain::{
        MembershipValues, ValueDomainDelta,
    };

    use super::MembershipReducer;

    #[test]
    fn value_domain_union_accepts_unseen_out_of_order_deltas() {
        let mut reducer =
            MembershipReducer::try_new(DataType::Int64, NullSemantics::NeverMatches).unwrap();
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
            MembershipReducer::try_new(DataType::Int64, NullSemantics::NullSafeEqual).unwrap();
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
            MembershipReducer::try_new(DataType::Int64, NullSemantics::NeverMatches).unwrap();
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
            MembershipReducer::try_new(DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let delta = ValueDomainDelta::new(MembershipValues::int64([1]), false);
        reducer.commit_preflighted(&delta).unwrap();
        assert_eq!(reducer.preflight(&delta).unwrap().retained_growth(), 0);
    }

    #[test]
    fn reducer_uses_port_owned_empty_largeint_construction() {
        let data_type = MembershipValues::large_int([]).data_type();
        let reducer =
            MembershipReducer::try_new(data_type.clone(), NullSemantics::NeverMatches).unwrap();

        assert_eq!(reducer.domain().data_type(), data_type);
        assert!(reducer.domain().values().is_empty());
    }
}
