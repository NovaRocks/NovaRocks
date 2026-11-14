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

//! Original Types integral -> Decimal128 conversion and Execution policy projection.
//! Observation grants no allocation. Valid negative scales do not require an oversized factor.
use crate::decimal128_rescale::{DecimalCastFailure, decimal_contract};
use crate::decimal128_rescale::{DecimalRescaleError, DecimalRescaleObservation};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::convert::Infallible;
#[cfg(test)]
use std::sync::Arc;
#[derive(Clone, Copy)]
struct Factors {
    up: Option<i128>,
    down: Option<i128>,
    zero: bool,
}
fn observe<E>(
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
    event: DecimalRescaleObservation,
) -> Result<(), DecimalRescaleError<E>> {
    observer(event).map_err(DecimalRescaleError::Host)
}
fn factors(scale: i8) -> Result<Factors, DecimalCastFailure> {
    let up = if scale > 0 {
        Some(
            crate::legacy_decimal::checked_pow10_i128(scale as usize)
                .ok_or_else(|| decimal_contract("invalid frozen Decimal128 integral cast scale"))?,
        )
    } else {
        None
    };
    let down = if (-38..0).contains(&scale) {
        Some(
            crate::legacy_decimal::checked_pow10_i128(scale.unsigned_abs() as usize)
                .expect("negative scale magnitude at most 38 has an i128 power of ten"),
        )
    } else {
        None
    };
    Ok(Factors {
        up,
        down,
        zero: scale < -38,
    })
}
/// One original per-value conversion, also the author of static endpoint facts.
fn converted(mut value: i128, precision: u8, factors: Factors) -> Option<i128> {
    if factors.zero {
        return Some(0);
    }
    if let Some(factor) = factors.up {
        value = value.checked_mul(factor)?;
    } else if let Some(factor) = factors.down {
        value /= factor;
    }
    // Keep original std to_string/19-digit window, including its allocation.
    if precision <= 18 && value.unsigned_abs().to_string().len() > 19 {
        return None;
    }
    Some(value)
}
fn raw<T>(
    f: impl FnOnce(
        &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), Infallible>,
    ) -> Result<T, DecimalRescaleError<Infallible>>,
) -> Result<T, DecimalCastFailure> {
    match f(&mut |_| Ok(())) {
        Ok(out) => Ok(out),
        Err(DecimalRescaleError::Domain(message)) => Err(message),
        Err(DecimalRescaleError::Host(never)) => match never {},
    }
}
pub fn relaxed_legacy(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, DecimalCastFailure> {
    raw(|observe| relaxed_observed(source, precision, scale, observe))
}
pub fn relaxed_observed<E>(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    let factors = factors(scale)?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let mut values = Vec::with_capacity(source.len());
    for row in 0..source.len() {
        observe(observer, DecimalRescaleObservation::Step)?;
        if source.is_null(row) {
            values.push(None);
            continue;
        }
        let value = match source.data_type() {
            DataType::Int8 => source
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| decimal_contract("failed to downcast to Int8Array"))?
                .value(row) as i128,
            DataType::Int16 => source
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| decimal_contract("failed to downcast to Int16Array"))?
                .value(row) as i128,
            DataType::Int32 => source
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| decimal_contract("failed to downcast to Int32Array"))?
                .value(row) as i128,
            DataType::Int64 => source
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| decimal_contract("failed to downcast to Int64Array"))?
                .value(row) as i128,
            other => {
                return Err(decimal_contract(format_args!(
                    "integral to DECIMAL cast unsupported source type: {other:?}"
                ))
                .into());
            }
        };
        observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
        let value = converted(value, precision, factors);
        observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
        values.push(value);
    }
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, scale)
        .map_err(DecimalCastFailure::Arrow)?;
    observe(observer, DecimalRescaleObservation::OpaqueBoundary)?;
    crate::decimal128_rescale::retag_observed(&wide, precision, scale, observer)
}
pub fn evaluate_legacy(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> Result<ArrayRef, DecimalCastFailure> {
    raw(|observe| evaluate_observed(source, precision, scale, policy, allow, observe))
}
pub fn evaluate_observed<E>(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
    policy: DecimalOverflowPolicy,
    _allow: bool,
    observer: &mut dyn FnMut(DecimalRescaleObservation) -> Result<(), E>,
) -> Result<ArrayRef, DecimalRescaleError<E>> {
    let casted = match relaxed_observed(source, precision, scale, observer) {
        Ok(casted) => casted,
        Err(DecimalRescaleError::Domain(cause)) => return Err(DecimalRescaleError::Domain(cause)),
        Err(DecimalRescaleError::Host(cause)) => return Err(DecimalRescaleError::Host(cause)),
    };
    let casted = crate::decimal128_rescale::enforce_precision_observed(casted, observer)?;
    let overflow = crate::decimal128_rescale::checked_numeric_cast_has_overflow_observed(
        source, &casted, observer,
    )?;
    // Original ALLOW rule applies only when the original source is Decimal;
    // integral sources retain the separately frozen policy, regardless of ALLOW.
    if overflow && policy == DecimalOverflowPolicy::ReportError {
        return Err(DecimalCastFailure::NumericOverflow.into());
    }
    // Original non-finite sanitizer returns Decimal128 unchanged.
    Ok(casted)
}
fn endpoints(source: &DataType) -> Option<(i128, i128)> {
    match source {
        DataType::Int8 => Some((i8::MIN as i128, i8::MAX as i128)),
        DataType::Int16 => Some((i16::MIN as i128, i16::MAX as i128)),
        DataType::Int32 => Some((i32::MIN as i128, i32::MAX as i128)),
        DataType::Int64 => Some((i64::MIN as i128, i64::MAX as i128)),
        _ => None,
    }
}
/// Actual successful-value shapes only. Other raw shapes always fail before
/// their row loop (or panic); they need explicit invocation-data/host handling.
pub fn selected_shape_supported(source: &DataType, target: &DataType) -> bool {
    endpoints(source).is_some()
        && matches!(target,DataType::Decimal128(p,s) if (1..=38).contains(p) && *s<=*p as i8)
}
pub fn can_overflow(source: &DataType, target: &DataType) -> bool {
    if !selected_shape_supported(source, target) {
        return false;
    }
    let DataType::Decimal128(p, s) = *target else {
        return false;
    };
    let (min, max) = endpoints(source).expect("checked signed shape");
    let factor = factors(s).expect("admitted factor is representable");
    let limit = 10_u128.pow(u32::from(p));
    [min, max]
        .into_iter()
        .any(|value| converted(value, p, factor).is_none_or(|value| value.unsigned_abs() >= limit))
}
pub fn can_produce_null(
    source: &DataType,
    target: &DataType,
    policy: DecimalOverflowPolicy,
) -> bool {
    policy == DecimalOverflowPolicy::OutputNull && can_overflow(source, target)
}
pub fn may_raise(source: &DataType, target: &DataType, policy: DecimalOverflowPolicy) -> bool {
    policy == DecimalOverflowPolicy::ReportError && can_overflow(source, target)
}

#[cfg(test)]
mod tests;
