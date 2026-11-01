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

//! Numeric distinct results retain the original arithmetic and decimal rules.
use crate::KernelFailure;
use crate::kernel_input::EvaluationCheckpoints;
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::sync::Arc;
/// The numeric aggregate owns arithmetic and canonical-state failures. It
/// preserves control and allocation causes supplied by their actual owners.
#[derive(Debug)]
pub enum DistinctComputationError {
    Kernel(KernelFailure),
    Contract(novarocks_failure_contract::BoundedDetail),
    NumericOverflow(Option<std::num::TryFromIntError>),
    FixedWidth(std::array::TryFromSliceError),
    Arrow(arrow_schema::ArrowError),
    OutputConcat(arrow_schema::ArrowError),
    NumericPrecision(arrow_schema::ArrowError),
    Extent {
        required: usize,
        limit: u64,
        layout: u16,
        source: Option<std::num::TryFromIntError>,
    },
    BatchExtent {
        required: u128,
    },
    Arithmetic,
}
impl From<KernelFailure> for DistinctComputationError {
    fn from(source: KernelFailure) -> Self {
        Self::Kernel(source)
    }
}
impl std::fmt::Display for DistinctComputationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kernel(e) => e.fmt(f),
            Self::Contract(detail) => f.write_str(detail.as_str()),
            Self::NumericOverflow(_) => {
                f.write_str("numeric distinct result exceeds the numeric domain")
            }
            Self::FixedWidth(source) => write!(
                f,
                "numeric distinct canonical key width is invalid: {source}"
            ),
            Self::Arrow(source) | Self::OutputConcat(source) => {
                write!(f, "numeric distinct array construction failed: {source}")
            }
            Self::NumericPrecision(source) => {
                write!(f, "numeric distinct result precision exceeded: {source}")
            }
            Self::Extent {
                required,
                limit,
                layout,
                ..
            } => write!(
                f,
                "numeric distinct state extent {required} exceeds layout {layout} limit {limit}"
            ),
            Self::BatchExtent { required } => write!(
                f,
                "numeric distinct state batch extent {required} exceeds Binary offsets"
            ),
            Self::Arithmetic => f.write_str("numeric distinct storage geometry overflow"),
        }
    }
}
impl std::error::Error for DistinctComputationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(e) => Some(e),
            Self::NumericOverflow(Some(e)) => Some(e),
            Self::FixedWidth(e) => Some(e),
            Self::Arrow(e) | Self::OutputConcat(e) | Self::NumericPrecision(e) => Some(e),
            Self::Extent {
                source: Some(e), ..
            } => Some(e),
            _ => None,
        }
    }
}
impl FailureProjection for DistinctComputationError {
    fn failure_fact(&self) -> FailureFact {
        use novarocks_failure_contract::{
            FailureReason, RepresentationBoundary, RepresentationFacts, RepresentationLayout,
            domain,
        };
        use std::num::NonZeroU16;
        let (class, code) = match self {
            Self::Kernel(source) => return source.failure_fact(),
            Self::NumericOverflow(_) | Self::NumericPrecision(_) => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Self::Contract(_) => (FailureClass::Invariant, 170),
            Self::FixedWidth(_) => (FailureClass::Invariant, 171),
            Self::Arrow(
                arrow_schema::ArrowError::MemoryError(_)
                | arrow_schema::ArrowError::ArithmeticOverflow(_),
            )
            | Self::OutputConcat(
                arrow_schema::ArrowError::MemoryError(_)
                | arrow_schema::ArrowError::ArithmeticOverflow(_),
            ) => (FailureClass::Resource, 172),
            Self::Arrow(
                arrow_schema::ArrowError::IoError(..) | arrow_schema::ArrowError::ExternalError(_),
            )
            | Self::OutputConcat(
                arrow_schema::ArrowError::IoError(..) | arrow_schema::ArrowError::ExternalError(_),
            ) => (FailureClass::Unexpected, 173),
            // Decimal fixed-width builders have no variable offset carrier.
            Self::OutputConcat(arrow_schema::ArrowError::OffsetOverflowError(_)) => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(175).unwrap()),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(29).unwrap(),
                    },
                    required: None,
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
            Self::Arrow(_) | Self::OutputConcat(_) => (FailureClass::Invariant, 174),
            Self::Extent {
                required,
                limit,
                layout,
                ..
            } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(175).unwrap()),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::MinimumUnit,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(*layout).unwrap(),
                    },
                    required: u64::try_from(*required).ok(),
                    extent_limit: Some(*limit),
                    representable_prefix: None,
                });
            }
            Self::BatchExtent { required } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(175).unwrap()),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: NonZeroU16::new(29).unwrap(),
                    },
                    required: u64::try_from(*required).ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                });
            }
            Self::Arithmetic => (FailureClass::Resource, 176),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for DistinctComputationError {}
impl DistinctComputationError {
    pub fn into_kernel_failure(self) -> KernelFailure {
        match self {
            Self::Kernel(source) => source,
            other => KernelFailure::from_domain(other),
        }
    }
}
fn distinct_contract(detail: impl std::fmt::Display) -> DistinctComputationError {
    DistinctComputationError::Contract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}
fn numeric_overflow() -> DistinctComputationError {
    DistinctComputationError::NumericOverflow(None)
}
fn read_fixed_numeric<const N: usize>(bytes: &[u8]) -> Result<[u8; N], DistinctComputationError> {
    bytes
        .try_into()
        .map_err(DistinctComputationError::FixedWidth)
}
fn state_count_u32(required: usize, layout: u16) -> Result<u32, DistinctComputationError> {
    u32::try_from(required).map_err(|source| DistinctComputationError::Extent {
        required,
        limit: u32::MAX as u64,
        layout,
        source: Some(source),
    })
}

/// Each individual state was admitted by serialize_set_into. This boundary
/// describes their one selected Binary output batch, before Arrow append.
pub fn checked_state_batch_bytes(
    current: usize,
    additional: usize,
) -> Result<usize, DistinctComputationError> {
    let required = current as u128 + additional as u128;
    if required > i32::MAX as u128 {
        return Err(DistinctComputationError::BatchExtent { required });
    }
    Ok(required as usize)
}

/// Storage supplies its existing iteration order. This is part of legacy
/// floating accumulation and must not be replaced by insertion order or sorting.
pub trait NumericDistinctSet {
    fn len(&self) -> usize;
    fn keys(&self) -> impl Iterator<Item = &[u8]>;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// The caller supplies the original typed allocation refusal. No text or
/// parallel typed/untyped callback exists in this storage contract.
pub trait NumericDistinctBuffer {
    fn reserve_exact(&mut self, size: usize) -> Result<(), KernelFailure>;
    fn append(&mut self, bytes: &[u8]);
}

pub fn sum_from_set(
    set: &impl NumericDistinctSet,
    input_type: &DataType,
    output_type: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, DistinctComputationError> {
    if set.is_empty() {
        // Return null
        return match output_type {
            DataType::Int64 => Ok(std::sync::Arc::new(Int64Array::from(vec![None]))),
            DataType::Float64 => Ok(std::sync::Arc::new(Float64Array::from(vec![None]))),
            DataType::Decimal128(precision, scale) => {
                let array = Decimal128Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(DistinctComputationError::Arrow)?;
                Ok(std::sync::Arc::new(array))
            }
            DataType::Decimal256(precision, scale) => {
                let array = Decimal256Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(DistinctComputationError::Arrow)?;
                Ok(std::sync::Arc::new(array))
            }
            other => Err(distinct_contract(format_args!(
                "multi_distinct_sum output type unsupported: {:?}",
                other
            ))),
        };
    }

    match output_type {
        DataType::Int64 => {
            let mut sum: i128 = 0;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Int8 => i8::from_le_bytes(read_fixed_numeric::<1>(v)?) as i128,
                    DataType::Int16 => i16::from_le_bytes(read_fixed_numeric::<2>(v)?) as i128,
                    DataType::Int32 => i32::from_le_bytes(read_fixed_numeric::<4>(v)?) as i128,
                    DataType::Int64 => i64::from_le_bytes(read_fixed_numeric::<8>(v)?) as i128,
                    DataType::Boolean => i8::from_le_bytes(read_fixed_numeric::<1>(v)?) as i128,
                    other => {
                        return Err(distinct_contract(format_args!(
                            "multi_distinct_sum unsupported input type for int output: {:?}",
                            other
                        )));
                    }
                };
                sum += value;
            }
            let sum_i64 = i64::try_from(sum)
                .map_err(|source| DistinctComputationError::NumericOverflow(Some(source)))?;
            Ok(std::sync::Arc::new(Int64Array::from(vec![Some(sum_i64)])))
        }
        DataType::Float64 => {
            let mut sum = 0.0f64;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Float32 => f32::from_le_bytes(read_fixed_numeric::<4>(v)?) as f64,
                    DataType::Float64 => f64::from_le_bytes(read_fixed_numeric::<8>(v)?),
                    other => {
                        return Err(distinct_contract(format_args!(
                            "multi_distinct_sum unsupported input type for float output: {:?}",
                            other
                        )));
                    }
                };
                sum += value;
            }
            Ok(std::sync::Arc::new(Float64Array::from(vec![Some(sum)])))
        }
        DataType::Decimal128(precision, scale) => {
            let mut sum: i128 = 0;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Decimal128(_, _) => i128::from_le_bytes(read_fixed_numeric::<16>(v)?),
                    other => {
                        return Err(distinct_contract(format_args!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        )));
                    }
                };
                sum = sum.checked_add(value).ok_or_else(numeric_overflow)?;
            }
            let array = Decimal128Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(DistinctComputationError::Arrow)?;
            Ok(std::sync::Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let mut sum = i256::ZERO;
            for v in set.keys() {
                work.step()?;
                let value = match input_type {
                    DataType::Decimal256(_, _) => i256::from_le_bytes(read_fixed_numeric::<32>(v)?),
                    other => {
                        return Err(distinct_contract(format_args!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        )));
                    }
                };
                sum = sum.checked_add(value).ok_or_else(numeric_overflow)?;
            }
            let array = Decimal256Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(DistinctComputationError::Arrow)?;
            Ok(std::sync::Arc::new(array))
        }
        other => Err(distinct_contract(format_args!(
            "multi_distinct_sum output type unsupported: {:?}",
            other
        ))),
    }
}

pub fn avg_from_set(
    set: &impl NumericDistinctSet,
    input_type: &DataType,
    output_type: &DataType,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, DistinctComputationError> {
    if set.is_empty() {
        return Ok(arrow_array::new_null_array(output_type, 1));
    }
    match output_type {
        DataType::Float64 => {
            let mut sum = 0.0f64;
            for value in set.keys() {
                work.step()?;
                sum += match input_type {
                    DataType::Int8 => i8::from_le_bytes(read_fixed_numeric::<1>(value)?) as f64,
                    DataType::Int16 => i16::from_le_bytes(read_fixed_numeric::<2>(value)?) as f64,
                    DataType::Int32 => i32::from_le_bytes(read_fixed_numeric::<4>(value)?) as f64,
                    DataType::Int64 => i64::from_le_bytes(read_fixed_numeric::<8>(value)?) as f64,
                    DataType::Float32 => f32::from_le_bytes(read_fixed_numeric::<4>(value)?) as f64,
                    DataType::Float64 => f64::from_le_bytes(read_fixed_numeric::<8>(value)?),
                    other => {
                        return Err(distinct_contract(format_args!(
                            "distinct avg numeric input unsupported: {other:?}"
                        )));
                    }
                };
            }
            Ok(Arc::new(Float64Array::from(vec![Some(
                sum / set.len() as f64,
            )])))
        }
        DataType::Decimal128(precision, scale) => {
            let DataType::Decimal128(_, input_scale) = input_type else {
                return Err(distinct_contract(
                    "distinct avg decimal input signature mismatch",
                ));
            };
            let mut sum = i256::ZERO;
            for value in set.keys() {
                work.step()?;
                let coefficient = i128::from_le_bytes(read_fixed_numeric::<16>(value)?);
                sum = sum
                    .checked_add(i256::from_i128(coefficient))
                    .ok_or_else(numeric_overflow)?;
            }
            let difference = i32::from(*scale) - i32::from(*input_scale);
            let factor = pow10(difference.unsigned_abs() as usize, work)?;
            if difference >= 0 {
                sum = sum.checked_mul(factor).ok_or_else(numeric_overflow)?;
            } else {
                sum /= factor;
            }
            let coefficient = round(sum, i256::from_i128(set.len() as i128))?
                .to_i128()
                .ok_or_else(numeric_overflow)?;
            let result = Decimal128Array::from(vec![Some(coefficient)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(DistinctComputationError::Arrow)?;
            result
                .validate_decimal_precision(*precision)
                .map_err(DistinctComputationError::NumericPrecision)?;
            Ok(Arc::new(result))
        }
        DataType::Decimal256(precision, scale) => {
            if input_type != output_type {
                return Err(distinct_contract(
                    "distinct avg decimal256 bound precision/scale mismatch",
                ));
            }
            // Divide each coefficient before accumulation. A valid DECIMAL256
            // mean can fit even when the sum of its inputs exceeds i256.
            let count = i256::from_i128(set.len() as i128);
            let mut quotient = i256::ZERO;
            let mut remainder = i256::ZERO;
            for value in set.keys() {
                work.step()?;
                let coefficient = i256::from_le_bytes(read_fixed_numeric::<32>(value)?);
                quotient = quotient
                    .checked_add(coefficient / count)
                    .ok_or_else(numeric_overflow)?;
                remainder = remainder
                    .checked_add(coefficient % count)
                    .ok_or_else(numeric_overflow)?;
            }
            quotient = quotient
                .checked_add(remainder / count)
                .ok_or_else(numeric_overflow)?;
            remainder %= count;
            // Normalize to the truncated quotient of the complete sum so that
            // half-up rounding also handles opposite-sign coefficients.
            if quotient > i256::ZERO && remainder < i256::ZERO {
                quotient -= i256::ONE;
                remainder += count;
            } else if quotient < i256::ZERO && remainder > i256::ZERO {
                quotient += i256::ONE;
                remainder -= count;
            }
            let rounded = quotient
                .checked_add(round(remainder, count)?)
                .ok_or_else(numeric_overflow)?;
            let result = Decimal256Array::from(vec![Some(rounded)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(DistinctComputationError::Arrow)?;
            result
                .validate_decimal_precision(*precision)
                .map_err(DistinctComputationError::NumericPrecision)?;
            Ok(Arc::new(result))
        }
        other => Err(distinct_contract(format_args!(
            "distinct avg output unsupported: {other:?}"
        ))),
    }
}

fn pow10(n: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<i256, DistinctComputationError> {
    let mut v = i256::ONE;
    for _ in 0..n {
        work.step()?;
        v = v
            .checked_mul(i256::from_i128(10))
            .ok_or_else(numeric_overflow)?;
    }
    Ok(v)
}
fn round(dividend: i256, divisor: i256) -> Result<i256, DistinctComputationError> {
    let mut q = dividend.checked_div(divisor).ok_or_else(numeric_overflow)?;
    let r = dividend.checked_rem(divisor).ok_or_else(numeric_overflow)?;
    if r == i256::ZERO {
        return Ok(q);
    }
    let abs_b = if divisor.is_negative() {
        divisor.checked_neg().ok_or_else(numeric_overflow)?
    } else {
        divisor
    };
    let abs_r = if r.is_negative() {
        r.checked_neg().ok_or_else(numeric_overflow)?
    } else {
        r
    };
    let threshold = (abs_b >> 1_u32)
        .checked_add(abs_b & i256::ONE)
        .ok_or_else(numeric_overflow)?;
    if abs_r >= threshold {
        let carry = if dividend.is_negative() ^ divisor.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        q = q.checked_add(carry).ok_or_else(numeric_overflow)?;
    }
    Ok(q)
}

pub struct NumericKey {
    bytes: [u8; 32],
    len: usize,
}
impl NumericKey {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
fn encode_le<const N: usize>(value: [u8; N]) -> NumericKey {
    let mut bytes = [0u8; 32];
    bytes[..N].copy_from_slice(&value);
    NumericKey { bytes, len: N }
}
fn canonical_f32_bits(v: f32) -> u32 {
    if v.is_nan() {
        f32::NAN.to_bits()
    } else if v == 0.0 {
        0.0f32.to_bits()
    } else {
        v.to_bits()
    }
}
fn canonical_f64_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0.0f64.to_bits()
    } else {
        v.to_bits()
    }
}
pub fn encode_numeric_row(
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<NumericKey>, DistinctComputationError> {
    work.step()?;
    if array.is_null(row) {
        return Ok(None);
    }
    let encoded = match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Int8Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Int16Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Int32Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Int64Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| distinct_contract("failed to downcast to BooleanArray"))?;
            encode_le([u8::from(arr.value(row))])
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Float32Array"))?;
            encode_le(canonical_f32_bits(arr.value(row)).to_le_bytes())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Float64Array"))?;
            encode_le(canonical_f64_bits(arr.value(row)).to_le_bytes())
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Decimal128Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| distinct_contract("failed to downcast to Decimal256Array"))?;
            encode_le(arr.value(row).to_le_bytes())
        }
        other => {
            return Err(distinct_contract(format_args!(
                "multi_distinct_sum unsupported input type: {:?}",
                other
            )));
        }
    };
    Ok(Some(encoded))
}

pub fn serialize_set_into(
    set: &impl NumericDistinctSet,
    out: &mut impl NumericDistinctBuffer,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), DistinctComputationError> {
    let count = state_count_u32(set.len(), 26)?;
    let size = set.keys().try_fold(
        4usize,
        |size, value| -> Result<usize, DistinctComputationError> {
            work.step()?;
            state_count_u32(value.len(), 27)?;
            size.checked_add(4)
                .and_then(|size| size.checked_add(value.len()))
                .ok_or(DistinctComputationError::Arithmetic)
        },
    )?;
    if size > i32::MAX as usize {
        return Err(DistinctComputationError::Extent {
            required: size,
            limit: i32::MAX as u64,
            layout: 28,
            source: None,
        });
    }
    work.flush()?;
    out.reserve_exact(size)?;
    work.flush()?;
    out.append(&count.to_le_bytes());
    for value in set.keys() {
        work.step()?;
        out.append(&(value.len() as u32).to_le_bytes());
        out.append(value);
    }
    Ok(())
}

pub fn numeric_key_width(data_type: &DataType) -> Result<usize, DistinctComputationError> {
    match data_type {
        DataType::Boolean | DataType::Int8 => Ok(1),
        DataType::Int16 => Ok(2),
        DataType::Int32 | DataType::Float32 => Ok(4),
        DataType::Int64 | DataType::Float64 => Ok(8),
        DataType::Decimal128(..) => Ok(16),
        DataType::Decimal256(..) => Ok(32),
        other => Err(distinct_contract(format_args!(
            "distinct numeric key type unsupported: {other:?}"
        ))),
    }
}

// Validate the complete payload before mutation, then visit borrowed keys.
// Keep the existing SUM state-v1 bytes: count:u32, then (length:u32, bytes)*.
pub fn visit_serialized_keys(
    bytes: &[u8],
    width: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(&[u8]) -> Result<(), DistinctComputationError>,
) -> Result<(), DistinctComputationError> {
    visit_serialized_keys_with_work(bytes, width, work, |bytes, _| visit(bytes))
}

pub fn visit_serialized_keys_with_work(
    bytes: &[u8],
    width: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut visit: impl FnMut(&[u8], &mut EvaluationCheckpoints<'_>) -> Result<(), DistinctComputationError>,
) -> Result<(), DistinctComputationError> {
    let read = |at: usize| -> Result<u32, DistinctComputationError> {
        let end = at
            .checked_add(4)
            .ok_or_else(|| DistinctComputationError::Arithmetic)?;
        let word = bytes
            .get(at..end)
            .ok_or_else(|| distinct_contract("invalid distinct set encoding"))?;
        Ok(u32::from_le_bytes(word.try_into().unwrap()))
    };
    let count = read(0)? as usize;
    let stride = 4usize
        .checked_add(width)
        .ok_or_else(|| DistinctComputationError::Arithmetic)?;
    let expected = count
        .checked_mul(stride)
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| DistinctComputationError::Arithmetic)?;
    if bytes.len() != expected {
        return Err(distinct_contract("invalid distinct set payload length"));
    }
    for index in 0..count {
        work.step()?;
        if read(4 + index * stride)? as usize != width {
            return Err(distinct_contract(
                "distinct set key width differs from selected input type",
            ));
        }
    }
    for index in 0..count {
        work.step()?;
        let start = 8 + index * stride;
        visit(&bytes[start..start + width], work)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_distinct_numeric_tests.rs"]
mod tests;
