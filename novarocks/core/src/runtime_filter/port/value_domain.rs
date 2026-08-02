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
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, TimeUnit};
use sha2::{Digest, Sha256};

use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
use novarocks_types::largeint::LARGEINT_BYTE_WIDTH;

use super::identity::LogicalVersion;
use super::ordered_bound::{OrderedTuple, RuntimeOrderContract};
use super::support::RetainedMemoryReservation;

pub const FINGERPRINT_VERSION_TAG: &[u8] = b"novarocks.runtime-filter.value-domain-delta.v1";
const CANONICAL_F32_NAN: u32 = 0x7fc0_0000;
const CANONICAL_F64_NAN: u64 = 0x7ff8_0000_0000_0000;

/// Immutable ordered-domain value shared by the execution predicate and the
/// backend participant reducer.  It deliberately contains no stream or
/// query-lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedBoundDomain {
    contract: Arc<RuntimeOrderContract>,
    bound: OrderedTuple,
}

impl OrderedBoundDomain {
    pub fn new(contract: Arc<RuntimeOrderContract>, bound: OrderedTuple) -> Self {
        Self { contract, bound }
    }

    pub const fn contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.contract
    }

    pub const fn bound(&self) -> &OrderedTuple {
        &self.bound
    }

    pub fn estimated_retained_bytes(&self) -> Option<usize> {
        self.bound.estimated_retained_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContributionSizeError {
    LengthExceedsCanonicalRange,
    SizeOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegralProjectionError {
    UnsupportedType,
    ValueOutOfRange,
}

impl fmt::Display for ContributionSizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthExceedsCanonicalRange => {
                write!(formatter, "canonical contribution length exceeds u64")
            }
            Self::SizeOverflow => write!(formatter, "canonical contribution size overflows usize"),
        }
    }
}

impl Error for ContributionSizeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decimal128ValidationError {
    InvalidPrecision { precision: u8 },
    InvalidScale { precision: u8, scale: i8 },
    ValueOutOfRange { precision: u8, value: i128 },
}

impl fmt::Display for Decimal128ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrecision { precision } => {
                write!(formatter, "invalid Decimal128 precision {precision}")
            }
            Self::InvalidScale { precision, scale } => write!(
                formatter,
                "invalid Decimal128 scale {scale} for precision {precision}"
            ),
            Self::ValueOutOfRange { precision, value } => write!(
                formatter,
                "Decimal128 value {value} exceeds precision {precision}"
            ),
        }
    }
}

impl Error for Decimal128ValidationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decimal128UnionError {
    TypeMismatch {
        expected_precision: u8,
        expected_scale: i8,
        actual_precision: u8,
        actual_scale: i8,
    },
}

impl fmt::Display for Decimal128UnionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeMismatch {
                expected_precision,
                expected_scale,
                actual_precision,
                actual_scale,
            } => write!(
                formatter,
                "Decimal128 union type mismatch: expected ({expected_precision}, {expected_scale}), got ({actual_precision}, {actual_scale})"
            ),
        }
    }
}

impl Error for Decimal128UnionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Decimal128Values {
    precision: u8,
    scale: i8,
    values: BTreeSet<i128>,
}

impl Decimal128Values {
    fn new_validated(precision: u8, scale: i8, values: BTreeSet<i128>) -> Self {
        Self {
            precision,
            scale,
            values,
        }
    }

    pub const fn precision(&self) -> u8 {
        self.precision
    }

    pub const fn scale(&self) -> i8 {
        self.scale
    }

    pub const fn values(&self) -> &BTreeSet<i128> {
        &self.values
    }

    pub fn union(&mut self, incoming: &Self) -> Result<usize, Decimal128UnionError> {
        if self.precision != incoming.precision || self.scale != incoming.scale {
            return Err(Decimal128UnionError::TypeMismatch {
                expected_precision: self.precision,
                expected_scale: self.scale,
                actual_precision: incoming.precision,
                actual_scale: incoming.scale,
            });
        }
        let previous_len = self.values.len();
        self.values.extend(incoming.values.iter().copied());
        Ok(self.values.len() - previous_len)
    }
}

pub(super) trait CanonicalOutput {
    fn write(&mut self, bytes: &[u8]) -> Result<(), ContributionSizeError>;
}

struct DigestOutput(Sha256);

impl CanonicalOutput for DigestOutput {
    fn write(&mut self, bytes: &[u8]) -> Result<(), ContributionSizeError> {
        self.0.update(bytes);
        Ok(())
    }
}

#[derive(Default)]
struct SizeOutput(usize);

impl CanonicalOutput for SizeOutput {
    fn write(&mut self, bytes: &[u8]) -> Result<(), ContributionSizeError> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(ContributionSizeError::SizeOverflow)?;
        Ok(())
    }
}

struct VecOutput<'a>(&'a mut Vec<u8>);

impl CanonicalOutput for VecOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), ContributionSizeError> {
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| ContributionSizeError::SizeOverflow)?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF32(u32);

impl CanonicalF32 {
    pub fn new(value: f32) -> Self {
        let bits = if value == 0.0 {
            0
        } else if value.is_nan() {
            CANONICAL_F32_NAN
        } else {
            value.to_bits()
        };
        Self(bits)
    }
}

impl Ord for CanonicalF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        f32::from_bits(self.0).total_cmp(&f32::from_bits(other.0))
    }
}

impl PartialOrd for CanonicalF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalF64(u64);

impl CanonicalF64 {
    pub fn new(value: f64) -> Self {
        let bits = if value == 0.0 {
            0
        } else if value.is_nan() {
            CANONICAL_F64_NAN
        } else {
            value.to_bits()
        };
        Self(bits)
    }
}

impl Ord for CanonicalF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        f64::from_bits(self.0).total_cmp(&f64::from_bits(other.0))
    }
}

impl PartialOrd for CanonicalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipValues {
    Boolean(BTreeSet<bool>),
    Int8(BTreeSet<i8>),
    Int16(BTreeSet<i16>),
    Int32(BTreeSet<i32>),
    Int64(BTreeSet<i64>),
    LargeInt(BTreeSet<i128>),
    Float32(BTreeSet<CanonicalF32>),
    Float64(BTreeSet<CanonicalF64>),
    Utf8(BTreeSet<String>),
    Date32(BTreeSet<i32>),
    Timestamp {
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
        values: BTreeSet<i64>,
    },
    Decimal128(Decimal128Values),
}

macro_rules! membership_constructor {
    ($name:ident, $variant:ident, $value:ty) => {
        pub fn $name(values: impl IntoIterator<Item = $value>) -> Self {
            Self::$variant(values.into_iter().collect())
        }
    };
}

impl MembershipValues {
    membership_constructor!(boolean, Boolean, bool);
    membership_constructor!(int8, Int8, i8);
    membership_constructor!(int16, Int16, i16);
    membership_constructor!(int32, Int32, i32);
    membership_constructor!(int64, Int64, i64);
    membership_constructor!(large_int, LargeInt, i128);
    membership_constructor!(date32, Date32, i32);

    pub fn boolean_set(values: BTreeSet<bool>) -> Self {
        Self::Boolean(values)
    }
    pub fn int8_set(values: BTreeSet<i8>) -> Self {
        Self::Int8(values)
    }
    pub fn int16_set(values: BTreeSet<i16>) -> Self {
        Self::Int16(values)
    }
    pub fn int32_set(values: BTreeSet<i32>) -> Self {
        Self::Int32(values)
    }
    pub fn int64_set(values: BTreeSet<i64>) -> Self {
        Self::Int64(values)
    }
    pub fn large_int_set(values: BTreeSet<i128>) -> Self {
        Self::LargeInt(values)
    }
    pub fn date32_set(values: BTreeSet<i32>) -> Self {
        Self::Date32(values)
    }
    pub fn float32_set(values: BTreeSet<CanonicalF32>) -> Self {
        Self::Float32(values)
    }
    pub fn float64_set(values: BTreeSet<CanonicalF64>) -> Self {
        Self::Float64(values)
    }
    pub fn utf8_set(values: BTreeSet<String>) -> Self {
        Self::Utf8(values)
    }

    pub fn timestamp_set(
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
        values: BTreeSet<i64>,
    ) -> Self {
        Self::Timestamp {
            unit,
            timezone,
            values,
        }
    }

    pub fn decimal128_set(
        precision: u8,
        scale: i8,
        values: BTreeSet<i128>,
    ) -> Result<Self, Decimal128ValidationError> {
        Self::validate_decimal128_values(precision, scale, &values)?;
        Ok(Self::Decimal128(Decimal128Values::new_validated(
            precision, scale, values,
        )))
    }

    pub fn validate_decimal128_values(
        precision: u8,
        scale: i8,
        values: &BTreeSet<i128>,
    ) -> Result<(), Decimal128ValidationError> {
        if precision == 0 || precision > DECIMAL128_MAX_PRECISION {
            return Err(Decimal128ValidationError::InvalidPrecision { precision });
        }
        if !decimal_scale_is_valid(precision, scale) {
            return Err(Decimal128ValidationError::InvalidScale { precision, scale });
        }
        let exclusive_bound = 10_i128
            .checked_pow(u32::from(precision))
            .expect("Decimal128 maximum precision fits i128");
        if let Some(value) = values
            .iter()
            .copied()
            .find(|value| *value <= -exclusive_bound || *value >= exclusive_bound)
        {
            return Err(Decimal128ValidationError::ValueOutOfRange { precision, value });
        }
        Ok(())
    }

    pub fn validate_decimal128_scalar(
        precision: u8,
        scale: i8,
        value: i128,
    ) -> Result<(), Decimal128ValidationError> {
        if precision == 0 || precision > DECIMAL128_MAX_PRECISION {
            return Err(Decimal128ValidationError::InvalidPrecision { precision });
        }
        if !decimal_scale_is_valid(precision, scale) {
            return Err(Decimal128ValidationError::InvalidScale { precision, scale });
        }
        let exclusive_bound = 10_i128
            .checked_pow(u32::from(precision))
            .expect("Decimal128 maximum precision fits i128");
        if value <= -exclusive_bound || value >= exclusive_bound {
            return Err(Decimal128ValidationError::ValueOutOfRange { precision, value });
        }
        Ok(())
    }

    pub fn float32(values: impl IntoIterator<Item = f32>) -> Self {
        Self::Float32(values.into_iter().map(CanonicalF32::new).collect())
    }

    pub fn float64(values: impl IntoIterator<Item = f64>) -> Self {
        Self::Float64(values.into_iter().map(CanonicalF64::new).collect())
    }

    pub fn utf8<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Utf8(values.into_iter().map(Into::into).collect())
    }

    pub fn timestamp(
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
        values: impl IntoIterator<Item = i64>,
    ) -> Self {
        Self::Timestamp {
            unit,
            timezone,
            values: values.into_iter().collect(),
        }
    }

    pub fn decimal128(
        precision: u8,
        scale: i8,
        values: impl IntoIterator<Item = i128>,
    ) -> Result<Self, Decimal128ValidationError> {
        if precision == 0 || precision > DECIMAL128_MAX_PRECISION {
            return Err(Decimal128ValidationError::InvalidPrecision { precision });
        }
        if !decimal_scale_is_valid(precision, scale) {
            return Err(Decimal128ValidationError::InvalidScale { precision, scale });
        }
        let values = values.into_iter().collect::<BTreeSet<_>>();
        let exclusive_bound = 10_i128
            .checked_pow(u32::from(precision))
            .expect("Decimal128 maximum precision fits i128");
        if let Some(value) = values
            .iter()
            .copied()
            .find(|value| *value <= -exclusive_bound || *value >= exclusive_bound)
        {
            return Err(Decimal128ValidationError::ValueOutOfRange { precision, value });
        }
        Ok(Self::Decimal128(Decimal128Values::new_validated(
            precision, scale, values,
        )))
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Boolean(_) => DataType::Boolean,
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::LargeInt(_) => DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH),
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
            Self::Date32(_) => DataType::Date32,
            Self::Timestamp { unit, timezone, .. } => {
                DataType::Timestamp(unit.clone(), timezone.clone())
            }
            Self::Decimal128(values) => DataType::Decimal128(values.precision(), values.scale()),
        }
    }

    pub fn empty_for_data_type(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Boolean => Self::boolean([]),
            DataType::Int8 => Self::int8([]),
            DataType::Int16 => Self::int16([]),
            DataType::Int32 => Self::int32([]),
            DataType::Int64 => Self::int64([]),
            DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => {
                Self::large_int([])
            }
            DataType::Float32 => Self::float32([]),
            DataType::Float64 => Self::float64([]),
            DataType::Utf8 => Self::utf8::<[String; 0], String>([]),
            DataType::Date32 => Self::date32([]),
            DataType::Timestamp(unit, timezone) => {
                Self::timestamp(unit.clone(), timezone.clone(), [])
            }
            DataType::Decimal128(precision, scale) => {
                Self::decimal128(*precision, *scale, []).ok()?
            }
            _ => return None,
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Boolean(values) => values.len(),
            Self::Int8(values) => values.len(),
            Self::Int16(values) => values.len(),
            Self::Int32(values) | Self::Date32(values) => values.len(),
            Self::Int64(values) => values.len(),
            Self::LargeInt(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Utf8(values) => values.len(),
            Self::Timestamp { values, .. } => values.len(),
            Self::Decimal128(values) => values.values().len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn estimated_value_bytes(&self) -> Result<usize, ContributionSizeError> {
        let fixed = |len: usize, width: usize| {
            len.checked_mul(width)
                .ok_or(ContributionSizeError::SizeOverflow)
        };
        match self {
            Self::Boolean(values) => fixed(values.len(), size_of::<bool>()),
            Self::Int8(values) => fixed(values.len(), size_of::<i8>()),
            Self::Int16(values) => fixed(values.len(), size_of::<i16>()),
            Self::Int32(values) | Self::Date32(values) => fixed(values.len(), size_of::<i32>()),
            Self::Int64(values) => fixed(values.len(), size_of::<i64>()),
            Self::LargeInt(values) => fixed(values.len(), size_of::<i128>()),
            Self::Float32(values) => fixed(values.len(), size_of::<u32>()),
            Self::Float64(values) => fixed(values.len(), size_of::<u64>()),
            Self::Utf8(values) => values.iter().try_fold(0usize, |total, value| {
                total
                    .checked_add(value.len())
                    .ok_or(ContributionSizeError::SizeOverflow)
            }),
            Self::Timestamp { values, .. } => fixed(values.len(), size_of::<i64>()),
            Self::Decimal128(values) => fixed(values.values().len(), size_of::<i128>()),
        }
    }

    pub fn float32_bits(&self) -> Option<Vec<u32>> {
        match self {
            Self::Float32(values) => Some(values.iter().map(|value| value.0).collect()),
            _ => None,
        }
    }

    pub fn canonical_encoded_len(&self) -> Result<usize, ContributionSizeError> {
        let mut output = SizeOutput::default();
        self.encode_canonical(&mut output)?;
        Ok(output.0)
    }

    pub fn encode_canonical_into(&self, output: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        self.encode_canonical(&mut VecOutput(output))
    }

    pub fn visit_lossless_i64(
        &self,
        mut visit: impl FnMut(i64),
    ) -> Result<(), IntegralProjectionError> {
        macro_rules! visit_values {
            ($values:expr) => {{
                for value in $values {
                    visit(i64::from(*value));
                }
                Ok(())
            }};
        }
        match self {
            Self::Boolean(values) => {
                for value in values {
                    visit(i64::from(*value));
                }
                Ok(())
            }
            Self::Int8(values) => visit_values!(values),
            Self::Int16(values) => visit_values!(values),
            Self::Int32(values) | Self::Date32(values) => visit_values!(values),
            Self::Int64(values) => {
                for value in values {
                    visit(*value);
                }
                Ok(())
            }
            Self::Decimal128(values) if values.precision() <= 18 => {
                for value in values.values() {
                    visit(
                        i64::try_from(*value)
                            .map_err(|_| IntegralProjectionError::ValueOutOfRange)?,
                    );
                }
                Ok(())
            }
            Self::LargeInt(_)
            | Self::Float32(_)
            | Self::Float64(_)
            | Self::Utf8(_)
            | Self::Timestamp { .. }
            | Self::Decimal128(_) => Err(IntegralProjectionError::UnsupportedType),
        }
    }

    pub fn canonical_scalar_max_frame_len(&self) -> Result<usize, ContributionSizeError> {
        let payload = match self {
            Self::Boolean(_) | Self::Int8(_) => 1,
            Self::Int16(_) => 2,
            Self::Int32(_) | Self::Float32(_) | Self::Date32(_) => 4,
            Self::Int64(_) | Self::Float64(_) | Self::Timestamp { .. } => 8,
            Self::LargeInt(_) | Self::Decimal128(_) => 16,
            Self::Utf8(values) => values.iter().map(String::len).max().unwrap_or(0),
        };
        1usize
            .checked_add(8)
            .and_then(|size| size.checked_add(payload))
            .ok_or(ContributionSizeError::SizeOverflow)
    }

    pub fn visit_canonical_scalar_frames(
        &self,
        frame: &mut Vec<u8>,
        mut visit: impl FnMut(&[u8]),
    ) -> Result<(), ContributionSizeError> {
        let max_len = self.canonical_scalar_max_frame_len()?;
        if frame.capacity() < max_len {
            frame
                .try_reserve(
                    max_len
                        .checked_sub(frame.len())
                        .ok_or(ContributionSizeError::SizeOverflow)?,
                )
                .map_err(|_| ContributionSizeError::SizeOverflow)?;
        }
        macro_rules! visit_fixed {
            ($tag:expr, $values:expr, $encode:expr) => {{
                for value in $values {
                    let bytes = $encode(value);
                    write_scalar_frame(frame, $tag, &bytes)?;
                    visit(frame);
                }
            }};
        }
        match self {
            Self::Boolean(values) => visit_fixed!(1, values, |value: &bool| [u8::from(*value)]),
            Self::Int8(values) => visit_fixed!(2, values, |value: &i8| value.to_be_bytes()),
            Self::Int16(values) => visit_fixed!(3, values, |value: &i16| value.to_be_bytes()),
            Self::Int32(values) => visit_fixed!(4, values, |value: &i32| value.to_be_bytes()),
            Self::Int64(values) => visit_fixed!(5, values, |value: &i64| value.to_be_bytes()),
            Self::LargeInt(values) => {
                visit_fixed!(6, values, |value: &i128| value.to_be_bytes())
            }
            Self::Float32(values) => {
                visit_fixed!(7, values, |value: &CanonicalF32| value.0.to_be_bytes())
            }
            Self::Float64(values) => {
                visit_fixed!(8, values, |value: &CanonicalF64| value.0.to_be_bytes())
            }
            Self::Utf8(values) => {
                for value in values {
                    write_scalar_frame(frame, 9, value.as_bytes())?;
                    visit(frame);
                }
            }
            Self::Date32(values) => {
                visit_fixed!(10, values, |value: &i32| value.to_be_bytes())
            }
            Self::Timestamp { values, .. } => {
                visit_fixed!(11, values, |value: &i64| value.to_be_bytes())
            }
            Self::Decimal128(values) => {
                visit_fixed!(12, values.values(), |value: &i128| value.to_be_bytes())
            }
        }
        Ok(())
    }

    fn encode_canonical(
        &self,
        output: &mut impl CanonicalOutput,
    ) -> Result<(), ContributionSizeError> {
        match self {
            Self::Boolean(values) => {
                encode_fixed_values(output, 1, values, |value| [u8::from(*value)])?
            }
            Self::Int8(values) => {
                encode_fixed_values(output, 2, values, |value| value.to_be_bytes())?
            }
            Self::Int16(values) => {
                encode_fixed_values(output, 3, values, |value| value.to_be_bytes())?
            }
            Self::Int32(values) => {
                encode_fixed_values(output, 4, values, |value| value.to_be_bytes())?
            }
            Self::Int64(values) => {
                encode_fixed_values(output, 5, values, |value| value.to_be_bytes())?
            }
            Self::LargeInt(values) => {
                encode_fixed_values(output, 6, values, |value| value.to_be_bytes())?
            }
            Self::Float32(values) => {
                encode_fixed_values(output, 7, values, |value| value.0.to_be_bytes())?
            }
            Self::Float64(values) => {
                encode_fixed_values(output, 8, values, |value| value.0.to_be_bytes())?
            }
            Self::Utf8(values) => {
                output.write(&[9])?;
                encode_cardinality(output, values.len())?;
                for value in values {
                    encode_length_delimited(output, value.as_bytes())?;
                }
            }
            Self::Date32(values) => {
                encode_fixed_values(output, 10, values, |value| value.to_be_bytes())?
            }
            Self::Timestamp {
                unit,
                timezone,
                values,
            } => {
                output.write(&[11, time_unit_tag(unit)])?;
                match timezone {
                    Some(timezone) => {
                        output.write(&[1])?;
                        encode_length_delimited(output, timezone.as_bytes())?;
                    }
                    None => output.write(&[0])?,
                }
                encode_cardinality(output, values.len())?;
                for value in values {
                    output.write(&value.to_be_bytes())?;
                }
            }
            Self::Decimal128(values) => {
                output.write(&[12, values.precision(), values.scale() as u8])?;
                encode_cardinality(output, values.values().len())?;
                for value in values.values() {
                    output.write(&value.to_be_bytes())?;
                }
            }
        }
        Ok(())
    }
}

fn write_scalar_frame(
    frame: &mut Vec<u8>,
    schema_tag: u8,
    scalar: &[u8],
) -> Result<(), ContributionSizeError> {
    let len = u64::try_from(scalar.len())
        .map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
    frame.clear();
    frame.push(schema_tag);
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(scalar);
    Ok(())
}

fn encode_fixed_values<T, const N: usize>(
    output: &mut impl CanonicalOutput,
    type_tag: u8,
    values: &BTreeSet<T>,
    encode: impl Fn(&T) -> [u8; N],
) -> Result<(), ContributionSizeError>
where
    T: Ord,
{
    output.write(&[type_tag])?;
    encode_cardinality(output, values.len())?;
    for value in values {
        output.write(&encode(value))?;
    }
    Ok(())
}

fn encode_cardinality(
    output: &mut impl CanonicalOutput,
    len: usize,
) -> Result<(), ContributionSizeError> {
    let len = u64::try_from(len).map_err(|_| ContributionSizeError::LengthExceedsCanonicalRange)?;
    output.write(&len.to_be_bytes())
}

fn encode_length_delimited(
    output: &mut impl CanonicalOutput,
    bytes: &[u8],
) -> Result<(), ContributionSizeError> {
    encode_cardinality(output, bytes.len())?;
    output.write(bytes)
}

fn time_unit_tag(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}

fn decimal_scale_is_valid(precision: u8, scale: i8) -> bool {
    scale <= DECIMAL128_MAX_SCALE && (scale <= 0 || scale as u8 <= precision)
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ContributionFingerprint([u8; 32]);

impl ContributionFingerprint {
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValueDomainDelta {
    values: MembershipValues,
    contains_null: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReducedMembershipDomain {
    values: MembershipValues,
    contains_null: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipUnionError {
    TypeMismatch,
}

impl fmt::Display for MembershipUnionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "membership union type mismatch")
    }
}

impl Error for MembershipUnionError {}

impl ReducedMembershipDomain {
    pub fn new(values: MembershipValues, contains_null: bool) -> Self {
        Self {
            values,
            contains_null,
        }
    }

    pub const fn values(&self) -> &MembershipValues {
        &self.values
    }

    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }

    pub fn data_type(&self) -> DataType {
        self.values.data_type()
    }

    pub fn estimated_retained_bytes(&self) -> Result<usize, ContributionSizeError> {
        self.values.estimated_value_bytes().and_then(|bytes| {
            bytes
                .checked_add(usize::from(self.contains_null))
                .ok_or(ContributionSizeError::SizeOverflow)
        })
    }

    pub fn union_prevalidated(
        &mut self,
        incoming: &MembershipValues,
        retain_null: bool,
    ) -> Result<(), MembershipUnionError> {
        if self.data_type() != incoming.data_type() {
            return Err(MembershipUnionError::TypeMismatch);
        }

        macro_rules! extend_same {
            ($left:expr, $right:expr) => {{
                $left.extend($right.iter().cloned());
            }};
        }
        match (&mut self.values, incoming) {
            (MembershipValues::Boolean(left), MembershipValues::Boolean(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Int8(left), MembershipValues::Int8(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Int16(left), MembershipValues::Int16(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Int32(left), MembershipValues::Int32(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Int64(left), MembershipValues::Int64(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::LargeInt(left), MembershipValues::LargeInt(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Float32(left), MembershipValues::Float32(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Float64(left), MembershipValues::Float64(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Utf8(left), MembershipValues::Utf8(right)) => {
                extend_same!(left, right)
            }
            (MembershipValues::Date32(left), MembershipValues::Date32(right)) => {
                extend_same!(left, right)
            }
            (
                MembershipValues::Timestamp { values: left, .. },
                MembershipValues::Timestamp { values: right, .. },
            ) => extend_same!(left, right),
            (MembershipValues::Decimal128(left), MembershipValues::Decimal128(right)) => {
                left.union(right)
                    .expect("matching Decimal128 data type must preserve precision and scale");
            }
            _ => unreachable!("equal membership data types must use the same variant"),
        }
        self.contains_null |= retain_null;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalSnapshotDomain {
    Membership(Arc<ReducedMembershipDomain>),
    OrderedBound(Arc<OrderedBoundDomain>),
}

pub struct LogicalSnapshot {
    channel_id: ChannelId,
    version: LogicalVersion,
    domain: LogicalSnapshotDomain,
    retained_memory_reservation: RetainedMemoryReservation,
}

impl LogicalSnapshot {
    pub fn first(
        channel_id: ChannelId,
        domain: ReducedMembershipDomain,
        retained_memory_reservation: RetainedMemoryReservation,
    ) -> Self {
        Self {
            channel_id,
            version: LogicalVersion::FIRST,
            domain: LogicalSnapshotDomain::Membership(Arc::new(domain)),
            retained_memory_reservation,
        }
    }

    pub fn ordered(
        channel_id: ChannelId,
        version: LogicalVersion,
        domain: Arc<OrderedBoundDomain>,
        retained_memory_reservation: RetainedMemoryReservation,
    ) -> Self {
        Self {
            channel_id,
            version,
            domain: LogicalSnapshotDomain::OrderedBound(domain),
            retained_memory_reservation,
        }
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub const fn version(&self) -> LogicalVersion {
        self.version
    }

    pub fn domain(&self) -> &ReducedMembershipDomain {
        match &self.domain {
            LogicalSnapshotDomain::Membership(domain) => domain,
            LogicalSnapshotDomain::OrderedBound(_) => {
                panic!("ordered logical snapshot is not a membership domain")
            }
        }
    }

    pub const fn logical_domain(&self) -> &LogicalSnapshotDomain {
        &self.domain
    }

    pub const fn ordered_bound(&self) -> Option<&Arc<OrderedBoundDomain>> {
        match &self.domain {
            LogicalSnapshotDomain::Membership(_) => None,
            LogicalSnapshotDomain::OrderedBound(domain) => Some(domain),
        }
    }

    pub const fn retained_memory_bytes(&self) -> usize {
        self.retained_memory_reservation.bytes()
    }
}

impl std::fmt::Debug for LogicalSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogicalSnapshot")
            .field("channel_id", &self.channel_id)
            .field("version", &self.version)
            .field("domain", &self.domain)
            .field(
                "retained_memory_bytes",
                &self.retained_memory_reservation.bytes(),
            )
            .finish()
    }
}

impl ValueDomainDelta {
    pub fn new(values: MembershipValues, contains_null: bool) -> Self {
        Self {
            values,
            contains_null,
        }
    }

    pub fn values(&self) -> &MembershipValues {
        &self.values
    }

    pub fn data_type(&self) -> DataType {
        self.values.data_type()
    }

    pub fn matches_data_type(&self, expected: &DataType) -> bool {
        self.data_type() == *expected
    }

    pub const fn contains_null(&self) -> bool {
        self.contains_null
    }

    pub const fn retains_null(&self, null_semantics: NullSemantics) -> bool {
        self.contains_null && matches!(null_semantics, NullSemantics::NullSafeEqual)
    }

    pub fn estimated_retained_bytes(
        &self,
        null_semantics: NullSemantics,
    ) -> Result<usize, ContributionSizeError> {
        self.values
            .estimated_value_bytes()?
            .checked_add(usize::from(self.retains_null(null_semantics)))
            .ok_or(ContributionSizeError::SizeOverflow)
    }

    pub fn estimated_contribution_bytes(&self) -> Result<usize, ContributionSizeError> {
        self.canonical_encoded_len()
    }

    pub fn canonical_encoded_len(&self) -> Result<usize, ContributionSizeError> {
        let mut output = SizeOutput::default();
        self.encode_canonical(&mut output)?;
        Ok(output.0)
    }

    pub fn encode_canonical_into(&self, output: &mut Vec<u8>) -> Result<(), ContributionSizeError> {
        self.encode_canonical(&mut VecOutput(output))
    }

    pub fn fingerprint(&self) -> ContributionFingerprint {
        let mut output = DigestOutput(Sha256::new());
        self.encode_canonical(&mut output)
            .expect("addressable contribution lengths fit the canonical u64 format");
        ContributionFingerprint(output.0.finalize().into())
    }

    fn encode_canonical(
        &self,
        output: &mut impl CanonicalOutput,
    ) -> Result<(), ContributionSizeError> {
        encode_length_delimited(output, FINGERPRINT_VERSION_TAG)?;
        self.values.encode_canonical(output)?;
        output.write(&[u8::from(self.contains_null)])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};

    use super::{
        Decimal128UnionError, Decimal128ValidationError, IntegralProjectionError,
        MembershipUnionError, MembershipValues, ReducedMembershipDomain, ValueDomainDelta,
    };
    use crate::runtime_filter::model::contract::NullSemantics;

    #[test]
    fn membership_values_preserve_supported_type_and_null_contract() {
        let delta = ValueDomainDelta::new(MembershipValues::int64([2, 1, 2]), true);

        assert_eq!(delta.data_type(), DataType::Int64);
        assert!(delta.matches_data_type(&DataType::Int64));
        assert!(!delta.matches_data_type(&DataType::Int32));
        assert!(delta.contains_null());
        assert!(!delta.retains_null(NullSemantics::NeverMatches));
        assert!(delta.retains_null(NullSemantics::NullSafeEqual));
    }

    #[test]
    fn float_membership_values_canonicalize_zero_and_nan_without_losing_infinity() {
        let values = MembershipValues::float32([
            -0.0,
            0.0,
            f32::from_bits(0x7fc0_0001),
            f32::from_bits(0xffc0_0002),
            f32::INFINITY,
            f32::NEG_INFINITY,
        ]);

        assert_eq!(
            values.float32_bits().unwrap(),
            vec![
                f32::NEG_INFINITY.to_bits(),
                0,
                f32::INFINITY.to_bits(),
                0x7fc0_0000,
            ]
        );
    }

    #[test]
    fn value_domain_fingerprint_is_deterministic_and_payload_sensitive() {
        let left = ValueDomainDelta::new(MembershipValues::utf8(["b", "a", "a"]), true);
        let reordered = ValueDomainDelta::new(MembershipValues::utf8(["a", "b"]), true);
        let different_value = ValueDomainDelta::new(MembershipValues::utf8(["a", "c"]), true);
        let different_null = ValueDomainDelta::new(MembershipValues::utf8(["a", "b"]), false);

        assert_eq!(left.fingerprint(), reordered.fingerprint());
        assert_ne!(left.fingerprint(), different_value.fingerprint());
        assert_ne!(left.fingerprint(), different_null.fingerprint());
    }

    #[test]
    fn value_domain_codec_helpers_report_exact_appended_length() {
        let delta = ValueDomainDelta::new(MembershipValues::utf8(["", "é", "東京"]), true);
        let exact = delta.canonical_encoded_len().unwrap();
        let mut canonical = Vec::new();
        delta.encode_canonical_into(&mut canonical).unwrap();
        let mut encoded = vec![0xaa, 0xbb];

        delta.encode_canonical_into(&mut encoded).unwrap();

        assert_eq!(canonical.len(), exact);
        assert_eq!(encoded.len(), 2 + exact);
        assert_eq!(&encoded[2..], canonical);
        assert_eq!(delta.estimated_contribution_bytes(), Ok(exact));
    }

    #[test]
    fn reduced_domain_union_rejects_type_mismatch_without_mutation() {
        let mut domain = ReducedMembershipDomain::new(MembershipValues::int64([1]), false);

        assert_eq!(
            domain.union_prevalidated(&MembershipValues::int32([2]), true),
            Err(MembershipUnionError::TypeMismatch)
        );
        assert_eq!(domain.values(), &MembershipValues::int64([1]));
        assert!(!domain.contains_null());
    }

    #[test]
    fn supported_membership_types_round_trip_with_distinct_canonical_encodings() {
        let cases = vec![
            (MembershipValues::boolean([true]), DataType::Boolean),
            (MembershipValues::int8([1]), DataType::Int8),
            (MembershipValues::int16([1]), DataType::Int16),
            (MembershipValues::int32([1]), DataType::Int32),
            (MembershipValues::int64([1]), DataType::Int64),
            (
                MembershipValues::large_int([1]),
                DataType::FixedSizeBinary(16),
            ),
            (MembershipValues::float32([1.0]), DataType::Float32),
            (MembershipValues::float64([1.0]), DataType::Float64),
            (MembershipValues::utf8(["1"]), DataType::Utf8),
            (MembershipValues::date32([1]), DataType::Date32),
            (
                MembershipValues::timestamp(TimeUnit::Second, Some(Arc::from("UTC")), [1]),
                DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")), [1]),
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")), [1]),
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")), [1]),
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            ),
            (
                MembershipValues::decimal128(19, 4, [1]).unwrap(),
                DataType::Decimal128(19, 4),
            ),
        ];
        let mut fingerprints = BTreeSet::new();

        for (values, expected_type) in cases {
            let delta = ValueDomainDelta::new(values, true);
            assert_eq!(delta.data_type(), expected_type);
            assert!(MembershipValues::empty_for_data_type(&expected_type).is_some());
            assert!(delta.estimated_contribution_bytes().unwrap() > 0);
            assert!(fingerprints.insert(delta.fingerprint().bytes()));
        }
    }

    #[test]
    fn lossless_integral_projection_is_explicitly_whitelisted() {
        let mut projected = Vec::new();
        MembershipValues::date32([-2, 3])
            .visit_lossless_i64(|value| projected.push(value))
            .unwrap();
        assert_eq!(projected, [-2, 3]);

        let mut decimal = Vec::new();
        MembershipValues::decimal128(18, 2, [-123, 456])
            .unwrap()
            .visit_lossless_i64(|value| decimal.push(value))
            .unwrap();
        assert_eq!(decimal, [-123, 456]);
        assert_eq!(
            MembershipValues::utf8(["7"]).visit_lossless_i64(|_| {}),
            Err(IntegralProjectionError::UnsupportedType)
        );
        assert_eq!(
            MembershipValues::decimal128(19, 0, [1])
                .unwrap()
                .visit_lossless_i64(|_| {}),
            Err(IntegralProjectionError::UnsupportedType)
        );
    }

    #[test]
    fn port_constructs_typed_empty_largeint_without_exposing_width_to_core() {
        let data_type = DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH);
        let values = MembershipValues::empty_for_data_type(&data_type).unwrap();

        assert_eq!(values.data_type(), data_type);
        assert!(values.is_empty());
    }

    #[test]
    fn canonical_size_includes_type_parameters_framing_and_null_marker() {
        let utc = ValueDomainDelta::new(
            MembershipValues::timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")), [7]),
            false,
        );
        let shanghai = ValueDomainDelta::new(
            MembershipValues::timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("Asia/Shanghai")),
                [7],
            ),
            false,
        );
        let decimal_scale_two =
            ValueDomainDelta::new(MembershipValues::decimal128(20, 2, [7]).unwrap(), false);
        let decimal_scale_three =
            ValueDomainDelta::new(MembershipValues::decimal128(20, 3, [7]).unwrap(), false);

        assert!(
            shanghai.estimated_contribution_bytes().unwrap()
                > utc.estimated_contribution_bytes().unwrap()
        );
        assert_ne!(utc.fingerprint(), shanghai.fingerprint());
        assert_eq!(
            decimal_scale_two.estimated_contribution_bytes().unwrap(),
            decimal_scale_three.estimated_contribution_bytes().unwrap()
        );
        assert_ne!(
            decimal_scale_two.fingerprint(),
            decimal_scale_three.fingerprint()
        );
    }

    #[test]
    fn canonical_scalar_visitor_fully_reserves_a_partially_preallocated_frame() {
        let empty = MembershipValues::utf8(std::iter::empty::<String>());
        let max_len = empty.canonical_scalar_max_frame_len().unwrap();
        let mut frame = Vec::with_capacity(max_len - 4);
        frame.push(0xff);

        empty
            .visit_canonical_scalar_frames(&mut frame, |_| unreachable!())
            .unwrap();
        assert!(frame.capacity() >= max_len);
        let reserved_capacity = frame.capacity();

        MembershipValues::utf8([""])
            .visit_canonical_scalar_frames(&mut frame, |scalar| {
                assert_eq!(scalar.len(), max_len);
            })
            .unwrap();
        assert_eq!(frame.capacity(), reserved_capacity);
    }

    #[test]
    fn null_only_never_matches_still_has_nonzero_temporary_encoded_bytes() {
        let delta = ValueDomainDelta::new(MembershipValues::int64([]), true);

        assert!(delta.estimated_contribution_bytes().unwrap() > 0);
        assert_eq!(
            delta
                .estimated_retained_bytes(NullSemantics::NeverMatches)
                .unwrap(),
            0
        );
    }

    #[test]
    fn decimal128_rejects_invalid_precision_scale_and_values() {
        assert_eq!(
            MembershipValues::decimal128(0, 0, [0]),
            Err(Decimal128ValidationError::InvalidPrecision { precision: 0 })
        );
        assert_eq!(
            MembershipValues::decimal128(39, 0, [0]),
            Err(Decimal128ValidationError::InvalidPrecision { precision: 39 })
        );
        assert_eq!(
            MembershipValues::decimal128(4, 5, [0]),
            Err(Decimal128ValidationError::InvalidScale {
                precision: 4,
                scale: 5,
            })
        );
        assert_eq!(
            MembershipValues::decimal128(38, 39, [0]),
            Err(Decimal128ValidationError::InvalidScale {
                precision: 38,
                scale: 39,
            })
        );
        assert_eq!(
            MembershipValues::decimal128(3, 0, [1_000]),
            Err(Decimal128ValidationError::ValueOutOfRange {
                precision: 3,
                value: 1_000,
            })
        );
        assert!(MembershipValues::decimal128(3, -2, [-999, 999]).is_ok());
        assert!(MembershipValues::empty_for_data_type(&DataType::Decimal128(0, 0)).is_none());
        assert!(MembershipValues::empty_for_data_type(&DataType::Decimal128(4, 5)).is_none());
    }

    #[test]
    fn decimal128_value_object_exposes_read_only_validated_union_boundary() {
        let mut left = MembershipValues::decimal128(5, 2, [123, 456]).unwrap();
        let right = MembershipValues::decimal128(5, 2, [456, 789]).unwrap();
        let mismatched = MembershipValues::decimal128(6, 2, [999]).unwrap();

        let MembershipValues::Decimal128(left_values) = &mut left else {
            panic!("expected Decimal128 values");
        };
        let MembershipValues::Decimal128(right_values) = &right else {
            panic!("expected Decimal128 values");
        };
        let MembershipValues::Decimal128(mismatched_values) = &mismatched else {
            panic!("expected Decimal128 values");
        };

        assert_eq!(left_values.precision(), 5);
        assert_eq!(left_values.scale(), 2);
        assert_eq!(
            left_values.values().iter().copied().collect::<Vec<_>>(),
            vec![123, 456]
        );
        assert_eq!(left_values.union(right_values), Ok(1));
        assert_eq!(
            left_values.values().iter().copied().collect::<Vec<_>>(),
            vec![123, 456, 789]
        );
        assert_eq!(
            left_values.union(mismatched_values),
            Err(Decimal128UnionError::TypeMismatch {
                expected_precision: 5,
                expected_scale: 2,
                actual_precision: 6,
                actual_scale: 2,
            })
        );
    }
}
