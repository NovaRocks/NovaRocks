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

use arrow::datatypes::DataType;

use crate::runtime_filter::port::value_domain::{IntegralProjectionError, MembershipValues};

pub const BITSET_METADATA_BYTES: usize = 1 + 8 + 8 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitsetPlan {
    type_tag: u8,
    min: i64,
    max: i64,
    bit_count: u64,
    byte_count: usize,
}

impl BitsetPlan {
    pub fn new(values: &MembershipValues) -> Result<Self, BitsetError> {
        if values.is_empty() {
            return Err(BitsetError::EmptyDomain);
        }
        let type_tag = match values.data_type() {
            DataType::Boolean => 1,
            DataType::Int8 => 2,
            DataType::Int16 => 3,
            DataType::Int32 => 4,
            DataType::Int64 => 5,
            DataType::Date32 => 10,
            DataType::Decimal128(1..=18, _) => 12,
            _ => return Err(BitsetError::UnsupportedType),
        };
        let mut min = None::<i64>;
        let mut max = None::<i64>;
        values
            .visit_lossless_i64(|value| {
                min = Some(min.map_or(value, |old| old.min(value)));
                max = Some(max.map_or(value, |old| old.max(value)));
            })
            .map_err(BitsetError::from)?;
        let min = min.ok_or(BitsetError::EmptyDomain)?;
        let max = max.ok_or(BitsetError::EmptyDomain)?;
        let span = i128::from(max)
            .checked_sub(i128::from(min))
            .and_then(|value| value.checked_add(1))
            .ok_or(BitsetError::SpanOverflow)?;
        let bit_count = u64::try_from(span).map_err(|_| BitsetError::SpanOverflow)?;
        let byte_count =
            usize::try_from(bit_count.checked_add(7).ok_or(BitsetError::SizeOverflow)? / 8)
                .map_err(|_| BitsetError::SizeOverflow)?;
        Ok(Self {
            type_tag,
            min,
            max,
            bit_count,
            byte_count,
        })
    }

    pub const fn type_tag(self) -> u8 {
        self.type_tag
    }
    pub const fn min(self) -> i64 {
        self.min
    }
    pub const fn max(self) -> i64 {
        self.max
    }
    pub const fn bit_count(self) -> u64 {
        self.bit_count
    }
    pub const fn byte_count(self) -> usize {
        self.byte_count
    }
    pub fn payload_len(self) -> Result<usize, BitsetError> {
        BITSET_METADATA_BYTES
            .checked_add(self.byte_count)
            .ok_or(BitsetError::SizeOverflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitsetError {
    UnsupportedType,
    ValueOutOfRange,
    EmptyDomain,
    SpanOverflow,
    SizeOverflow,
}

impl From<IntegralProjectionError> for BitsetError {
    fn from(error: IntegralProjectionError) -> Self {
        match error {
            IntegralProjectionError::UnsupportedType => Self::UnsupportedType,
            IntegralProjectionError::ValueOutOfRange => Self::ValueOutOfRange,
        }
    }
}

pub fn build_bits(values: &MembershipValues, plan: BitsetPlan) -> Result<Vec<u8>, BitsetError> {
    let mut bits = vec![0u8; plan.byte_count()];
    let mut error = None;
    values
        .visit_lossless_i64(|value| {
            let offset = i128::from(value) - i128::from(plan.min());
            let Ok(offset) = u64::try_from(offset) else {
                error = Some(BitsetError::ValueOutOfRange);
                return;
            };
            if offset >= plan.bit_count() {
                error = Some(BitsetError::ValueOutOfRange);
                return;
            }
            let byte = usize::try_from(offset / 8).expect("bitset byte index fits allocation");
            bits[byte] |= 1 << (offset % 8);
        })
        .map_err(BitsetError::from)?;
    if let Some(error) = error {
        return Err(error);
    }
    Ok(bits)
}

pub fn contains(plan: BitsetPlan, bits: &[u8], value: i64) -> bool {
    let offset = i128::from(value) - i128::from(plan.min());
    let Ok(offset) = u64::try_from(offset) else {
        return false;
    };
    if offset >= plan.bit_count() {
        return false;
    }
    let byte = usize::try_from(offset / 8).expect("bitset byte index fits platform");
    bits.get(byte)
        .is_some_and(|value| value & (1 << (offset % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use crate::runtime_filter::port::value_domain::MembershipValues;

    use super::{BitsetError, BitsetPlan, build_bits, contains};

    fn assert_exact(values: MembershipValues, members: &[i64], absent: &[i64]) {
        let plan = BitsetPlan::new(&values).unwrap();
        let bits = build_bits(&values, plan).unwrap();
        for value in members {
            assert!(contains(plan, &bits, *value), "missing member {value}");
        }
        for value in absent {
            assert!(!contains(plan, &bits, *value), "unexpected member {value}");
        }
    }

    #[test]
    fn exact_bitset_preserves_every_whitelisted_integral_domain() {
        assert_exact(MembershipValues::boolean([false, true]), &[0, 1], &[-1, 2]);
        assert_exact(MembershipValues::int8([-5, 0, 7]), &[-5, 0, 7], &[-6, 1, 8]);
        assert_exact(
            MembershipValues::int16([-500, -3, 9]),
            &[-500, -3, 9],
            &[-4, 10],
        );
        assert_exact(
            MembershipValues::int32([-70_000, 4, 8]),
            &[-70_000, 4, 8],
            &[3, 9],
        );
        assert_exact(
            MembershipValues::int64([-1_000_000, 2, 19]),
            &[-1_000_000, 2, 19],
            &[1, 20],
        );
        assert_exact(
            MembershipValues::date32([1, 2, 31]),
            &[1, 2, 31],
            &[0, 3, 32],
        );
        assert_exact(
            MembershipValues::decimal128(18, 3, [-101, 0, 205]).unwrap(),
            &[-101, 0, 205],
            &[-102, 1, 206],
        );
    }

    #[test]
    fn bitset_rejects_non_whitelisted_types_and_unrepresentable_span() {
        for values in [
            MembershipValues::large_int([1]),
            MembershipValues::float64([1.0]),
            MembershipValues::utf8(["x"]),
            MembershipValues::timestamp(arrow::datatypes::TimeUnit::Second, None, [1]),
            MembershipValues::decimal128(19, 0, [1]).unwrap(),
        ] {
            assert_eq!(BitsetPlan::new(&values), Err(BitsetError::UnsupportedType));
        }
        assert_eq!(
            BitsetPlan::new(&MembershipValues::int64([i64::MIN, i64::MAX])),
            Err(BitsetError::SpanOverflow)
        );
    }

    #[test]
    fn bitset_bytes_are_order_and_duplicate_independent() {
        let left = MembershipValues::int32([9, 2, 9, 4]);
        let right = MembershipValues::int32([4, 2, 9]);
        let left_plan = BitsetPlan::new(&left).unwrap();
        let right_plan = BitsetPlan::new(&right).unwrap();
        assert_eq!(left_plan, right_plan);
        assert_eq!(build_bits(&left, left_plan), build_bits(&right, right_plan));
    }
}
