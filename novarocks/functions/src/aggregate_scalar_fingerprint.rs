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
//! Original ARRAY aggregate scalar fingerprint and owned comparator bodies.
//! These original fingerprints differ from COUNT DISTINCT and are not interchangeable.
use crate::aggregate_scalar::{
    AggScalarValue, ScalarOrdering, ScalarStateAllocator, ScalarWork, TrackedAggScalarValue,
    aggregate_vec_with_capacity,
};
use allocator_api2::vec::Vec as ScalarVec;
use std::cmp::Ordering;
#[path = "aggregate_scalar_fingerprint_failure.rs"]
mod extent_failure;
use extent_failure::{LengthKind, checked_encoded_len_add, checked_u32_len};

pub(crate) fn temporal_key(
    declaration: novarocks_type_contract::TemporalType,
    value: novarocks_type_contract::TemporalValue,
) -> Result<novarocks_type_contract::TemporalRelationKey, crate::KernelFailure> {
    value
        .relation_key(declaration)
        .map_err(|_| crate::aggregate_scalar::temporal_scalar_contract())
}

fn canonical_nan_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

#[cfg(test)]
pub fn tracked_key_fingerprint<A: ScalarStateAllocator>(
    key: &TrackedAggScalarValue<A>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, crate::KernelFailure> {
    tracked_key_fingerprint_with_float_identity(key, allocator, canonical_nan_bits, work)
}

pub fn tracked_key_fingerprint_with_float_identity<A: ScalarStateAllocator>(
    key: &TrackedAggScalarValue<A>,
    allocator: &A,
    float_identity: fn(f64) -> u64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, crate::KernelFailure> {
    work.step()?;
    let encoded_len = tracked_scalar_encoded_len(key, work)?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve aggregate scalar fingerprint",
        work,
    )?;
    encode_tracked_scalar(&mut output, key, float_identity, work)?;
    debug_assert_eq!(output.len(), encoded_len);
    Ok(output)
}

pub fn tracked_optional_key_fingerprint<A: ScalarStateAllocator>(
    value: &Option<TrackedAggScalarValue<A>>,
    allocator: &A,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarVec<u8, A>, crate::KernelFailure> {
    work.step()?;
    let value_len = value
        .as_ref()
        .map(|value| tracked_scalar_encoded_len(value, work))
        .transpose()?
        .unwrap_or(0);
    let mut encoded_len = 1usize;
    checked_encoded_len_add(&mut encoded_len, value_len)?;
    let mut output = aggregate_vec_with_capacity(
        allocator,
        encoded_len,
        "reserve optional aggregate scalar fingerprint",
        work,
    )?;
    encode_tracked_optional_value(&mut output, value, canonical_nan_bits, work)?;
    debug_assert_eq!(output.len(), encoded_len);
    Ok(output)
}

fn tracked_scalar_encoded_len<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, crate::KernelFailure> {
    work.step()?;
    let mut len = 1usize;
    match value {
        TrackedAggScalarValue::Bool(_) => checked_encoded_len_add(&mut len, 1)?,
        TrackedAggScalarValue::Int64(_)
        | TrackedAggScalarValue::Float64(_)
        | TrackedAggScalarValue::Timestamp(_) => checked_encoded_len_add(&mut len, 8)?,
        TrackedAggScalarValue::Date32(_) => checked_encoded_len_add(&mut len, 4)?,
        TrackedAggScalarValue::Temporal(..) => checked_encoded_len_add(&mut len, 12)?,
        TrackedAggScalarValue::Decimal128(_) => checked_encoded_len_add(&mut len, 16)?,
        TrackedAggScalarValue::Decimal256(_) => checked_encoded_len_add(&mut len, 32)?,
        TrackedAggScalarValue::Utf8(bytes) | TrackedAggScalarValue::Binary(bytes) => {
            let _ = checked_u32_len(LengthKind::Bytes, bytes.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            checked_encoded_len_add(&mut len, bytes.len())?;
        }
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => {
            let _ = checked_u32_len(LengthKind::Children, values.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            for value in values {
                work.step()?;
                checked_encoded_len_add(&mut len, 1)?;
                if let Some(value) = value {
                    checked_encoded_len_add(&mut len, tracked_scalar_encoded_len(value, work)?)?;
                }
            }
        }
        TrackedAggScalarValue::Map(entries) => {
            let _ = checked_u32_len(LengthKind::Entries, entries.len())?;
            checked_encoded_len_add(&mut len, 4)?;
            for (key, value) in entries {
                work.step()?;
                for value in [key, value] {
                    work.step()?;
                    checked_encoded_len_add(&mut len, 1)?;
                    if let Some(value) = value {
                        checked_encoded_len_add(
                            &mut len,
                            tracked_scalar_encoded_len(value, work)?,
                        )?;
                    }
                }
            }
        }
    }
    Ok(len)
}

fn encode_tracked_scalar<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    value: &TrackedAggScalarValue<A>,
    float_identity: fn(f64) -> u64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    work.step()?;
    match value {
        TrackedAggScalarValue::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        TrackedAggScalarValue::Int64(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Float64(value) => {
            output.push(3);
            let bits = float_identity(*value);
            output.extend_from_slice(&bits.to_le_bytes());
        }
        TrackedAggScalarValue::Utf8(value) => {
            output.push(4);
            output
                .extend_from_slice(&checked_u32_len(LengthKind::Bytes, value.len())?.to_le_bytes());
            for _ in value {
                work.step()?;
            }
            work.flush()?;
            output.extend_from_slice(value);
            work.flush()?;
        }
        TrackedAggScalarValue::Date32(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Timestamp(value) => {
            output.push(6);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Temporal(ty, value) => {
            output.push(13);
            output.extend_from_slice(&temporal_key(*ty, *value)?.order_bytes());
        }
        TrackedAggScalarValue::Decimal128(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Struct(values) => {
            output.push(8);
            encode_tracked_optional_values(output, values, float_identity, work)?;
        }
        TrackedAggScalarValue::Map(entries) => {
            output.push(9);
            output.extend_from_slice(
                &checked_u32_len(LengthKind::Entries, entries.len())?.to_le_bytes(),
            );
            for (key, value) in entries {
                work.step()?;
                encode_tracked_optional_value(output, key, float_identity, work)?;
                encode_tracked_optional_value(output, value, float_identity, work)?;
            }
        }
        TrackedAggScalarValue::List(values) => {
            output.push(10);
            encode_tracked_optional_values(output, values, float_identity, work)?;
        }
        TrackedAggScalarValue::Decimal256(value) => {
            output.push(11);
            output.extend_from_slice(&value.to_le_bytes());
        }
        TrackedAggScalarValue::Binary(value) => {
            output.push(12);
            output
                .extend_from_slice(&checked_u32_len(LengthKind::Bytes, value.len())?.to_le_bytes());
            for _ in value {
                work.step()?;
            }
            work.flush()?;
            output.extend_from_slice(value);
            work.flush()?;
        }
    }
    Ok(())
}

fn encode_tracked_optional_values<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    values: &[Option<TrackedAggScalarValue<A>>],
    float_identity: fn(f64) -> u64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    work.step()?;
    output.extend_from_slice(&checked_u32_len(LengthKind::Children, values.len())?.to_le_bytes());
    for value in values {
        work.step()?;
        encode_tracked_optional_value(output, value, float_identity, work)?;
    }
    Ok(())
}

fn encode_tracked_optional_value<A: ScalarStateAllocator>(
    output: &mut ScalarVec<u8, A>,
    value: &Option<TrackedAggScalarValue<A>>,
    float_identity: fn(f64) -> u64,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    work.step()?;
    if let Some(value) = value {
        output.push(1);
        encode_tracked_scalar(output, value, float_identity, work)?;
    } else {
        output.push(0);
    }
    Ok(())
}

/// Strict whole-value comparison. Unordered floating values end this
/// computation; callers with a different ordering policy use the partial API.
pub fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Ordering, crate::KernelFailure> {
    crate::aggregate_scalar::strict_scalar_ordering(
        compare_scalar_values_partial(left, right, work)?,
        "scalar comparison type mismatch",
    )
}
/// Value ordering policy is distinct from callback failure. A consumer may
/// choose a policy for NaN or heterogeneous kinds without swallowing errors.
pub fn compare_scalar_values_partial(
    left: &AggScalarValue,
    right: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOrdering, crate::KernelFailure> {
    use ScalarOrdering::Ordered;
    work.step()?;
    match (left, right) {
        (AggScalarValue::Bool(l), AggScalarValue::Bool(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Int64(l), AggScalarValue::Int64(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Float64(l), AggScalarValue::Float64(r)) => Ok(l
            .partial_cmp(r)
            .map(Ordered)
            .unwrap_or(ScalarOrdering::UnorderedFloat)),
        (AggScalarValue::Utf8(l), AggScalarValue::Utf8(r)) => {
            work.flush()?;
            let ordering = l.cmp(r);
            work.flush()?;
            Ok(Ordered(ordering))
        }
        (AggScalarValue::Binary(l), AggScalarValue::Binary(r)) => {
            work.flush()?;
            let ordering = l.cmp(r);
            work.flush()?;
            Ok(Ordered(ordering))
        }
        (AggScalarValue::Date32(l), AggScalarValue::Date32(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Timestamp(l), AggScalarValue::Timestamp(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Temporal(lt, l), AggScalarValue::Temporal(rt, r)) if lt == rt => {
            Ok(Ordered(temporal_key(*lt, *l)?.cmp(&temporal_key(*rt, *r)?)))
        }
        (AggScalarValue::Decimal128(l), AggScalarValue::Decimal128(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Decimal256(l), AggScalarValue::Decimal256(r)) => Ok(Ordered(l.cmp(r))),
        (AggScalarValue::Struct(l), AggScalarValue::Struct(r))
        | (AggScalarValue::List(l), AggScalarValue::List(r)) => {
            for (left, right) in l.iter().zip(r) {
                work.step()?;
                let order = compare_optional_scalar_values(left, right, work)?;
                if !matches!(order, Ordered(Ordering::Equal)) {
                    return Ok(order);
                }
            }
            Ok(Ordered(l.len().cmp(&r.len())))
        }
        (AggScalarValue::Map(l), AggScalarValue::Map(r)) => {
            for ((lk, lv), (rk, rv)) in l.iter().zip(r) {
                work.step()?;
                let order = compare_optional_scalar_values(lk, rk, work)?;
                if !matches!(order, Ordered(Ordering::Equal)) {
                    return Ok(order);
                }
                let order = compare_optional_scalar_values(lv, rv, work)?;
                if !matches!(order, Ordered(Ordering::Equal)) {
                    return Ok(order);
                }
            }
            Ok(Ordered(l.len().cmp(&r.len())))
        }
        _ => Ok(ScalarOrdering::MismatchedKinds),
    }
}
fn compare_optional_scalar_values(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<ScalarOrdering, crate::KernelFailure> {
    use ScalarOrdering::Ordered;
    work.step()?;
    match (left, right) {
        (None, None) => Ok(Ordered(Ordering::Equal)),
        (None, Some(_)) => Ok(Ordered(Ordering::Less)),
        (Some(_), None) => Ok(Ordered(Ordering::Greater)),
        (Some(left), Some(right)) => compare_scalar_values_partial(left, right, work),
    }
}

/// Stable byte fingerprint of an `AggScalarValue` suitable for use as a
/// `HashMap`/`HashSet` key when ordinary `PartialEq + Hash` is not available
/// (e.g. floats, decimals). Notable properties:
///
/// - `Utf8` is length-prefixed so `"ab"+"c"` and `"a"+"bc"` cannot collide.
/// - `Float32`/`Float64` NaN values are normalized to a single canonical bit
///   pattern so multiple NaN inputs collapse into one bucket.
/// - Composite types (`Struct`/`Map`/`List`) are encoded recursively for
///   safety, even though aggregate callers reject them upstream when needed.
///
/// Shared by aggregate functions that need stable scalar-key identity.
pub fn key_fingerprint(
    key: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<Vec<u8>, crate::KernelFailure> {
    work.step()?;
    let mut out = Vec::new();
    encode_scalar(&mut out, key, work)?;
    Ok(out)
}

fn encode_scalar(
    out: &mut Vec<u8>,
    key: &AggScalarValue,
    work: &mut ScalarWork<'_, '_>,
) -> Result<(), crate::KernelFailure> {
    work.step()?;
    match key {
        AggScalarValue::Bool(v) => {
            out.push(1);
            out.push(if *v { 1 } else { 0 });
        }
        AggScalarValue::Int64(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            out.push(3);
            // Normalize NaN: any NaN maps to the same fingerprint so
            // multiple NaN inputs collapse into one bucket.
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            };
            out.extend_from_slice(&bits.to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            out.push(4);
            let len = checked_u32_len(LengthKind::Bytes, v.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for _ in v.as_bytes() {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(v.as_bytes());
            work.flush()?;
        }
        AggScalarValue::Date32(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            out.push(6);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Temporal(ty, value) => {
            out.push(13);
            out.extend_from_slice(&temporal_key(*ty, *value)?.order_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            out.push(7);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal256(v) => {
            out.push(11);
            let text = v.to_string();
            let len = checked_u32_len(LengthKind::Bytes, text.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for _ in text.as_bytes() {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(text.as_bytes());
            work.flush()?;
        }
        AggScalarValue::Binary(v) => {
            out.push(12);
            let len = checked_u32_len(LengthKind::Bytes, v.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for _ in v {
                work.step()?;
            }
            work.flush()?;
            out.extend_from_slice(v);
            work.flush()?;
        }
        AggScalarValue::Struct(items) => {
            out.push(8);
            let len = checked_u32_len(LengthKind::Children, items.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for item in items {
                work.step()?;
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::Map(items) => {
            out.push(9);
            let len = checked_u32_len(LengthKind::Entries, items.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for (k, v) in items {
                work.step()?;
                match k {
                    Some(k) => {
                        out.push(1);
                        encode_scalar(out, k, work)?;
                    }
                    None => out.push(0),
                }
                match v {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::List(items) => {
            out.push(10);
            let len = checked_u32_len(LengthKind::Children, items.len())?;
            out.extend_from_slice(&len.to_le_bytes());
            for item in items {
                work.step()?;
                match item {
                    Some(v) => {
                        out.push(1);
                        encode_scalar(out, v, work)?;
                    }
                    None => out.push(0),
                }
            }
        }
    }
    Ok(())
}
