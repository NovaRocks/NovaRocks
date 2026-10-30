// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact collision comparisons. Coordinates are selected logical rows, not
//! occurrence ordinals or dictionary codes. The caller discards every output
//! slot if an operation fails; no comparison runs inside a table bool closure.

use super::{NullPolicy, PreparedKeyBatch, checkpoint_at, invariant, original, vector};
use crate::{
    exec::{
        hash_table::{
            hash::{canonical_f32_bits, canonical_f64_bits},
            key_builder::canonical_nullable_equals_observed,
        },
        pipeline::operator::InputConstructionOwner,
    },
    runtime::fragment::ExecutionResult,
};
use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
};
use novarocks_value_arrays::{
    ValueViolation, batch::compare::compare_pairs_into, hash_eq::NullEquality,
};

pub(super) fn compare_prepared_pairs(
    left: &PreparedKeyBatch<'_>,
    left_rows: &[usize],
    right: &PreparedKeyBatch<'_>,
    right_rows: &[usize],
    output: &mut [u8],
    owner: &dyn InputConstructionOwner,
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    checkpoint()?;
    if !left.spec.same_instance(&right.spec)
        || left_rows.len() != right_rows.len()
        || output.len() != left_rows.len()
    {
        return Err(invariant(ValueViolation::Layout));
    }
    for (i, ((&a, &b), result)) in left_rows
        .iter()
        .zip(right_rows)
        .zip(output.iter_mut())
        .enumerate()
    {
        checkpoint_at(i, checkpoint)?;
        if !left.input.selected().contains(a) || !right.input.selected().contains(b) {
            return Err(invariant(ValueViolation::UndefinedRow));
        }
        *result = 1;
    }
    let mut bytes_output = vector(owner, output.len())?;
    for i in 0..output.len() {
        checkpoint_at(i, checkpoint)?;
        bytes_output.push(None);
    }
    for column in 0..left.spec.types().len() {
        checkpoint()?;
        let nulls = left.spec.nulls()[column];
        match (left.bytes(column)?, right.bytes(column)?) {
            (Some(a), Some(b)) => {
                compare_pairs_into(
                    &a,
                    left_rows,
                    &b,
                    right_rows,
                    match nulls {
                        NullPolicy::Equal => NullEquality::Equal,
                        NullPolicy::Unmatchable => NullEquality::Unequal,
                    },
                    &mut bytes_output,
                    &mut |_| checkpoint(),
                )
                .map_err(original)?;
                for (i, (result, equal)) in output.iter_mut().zip(&bytes_output).enumerate() {
                    checkpoint_at(i, checkpoint)?;
                    *result &= u8::from(*equal == Some(true));
                }
            }
            (None, None) => {
                let (a, am) = left.tuples().physical_component(column)?;
                let (b, bm) = right.tuples().physical_component(column)?;
                if super::prepared::is_fixed(left.spec.types()[column].data_type()) {
                    compare_fixed_pairs(
                        a, am, left_rows, b, bm, right_rows, nulls, output, checkpoint,
                    )?;
                } else {
                    for (i, ((&ar, &br), result)) in left_rows
                        .iter()
                        .zip(right_rows)
                        .zip(output.iter_mut())
                        .enumerate()
                    {
                        checkpoint_at(i, checkpoint)?;
                        if *result == 0 {
                            continue;
                        }
                        let ap = physical_row(am, ar)?;
                        let bp = physical_row(bm, br)?;
                        if let Some(equal) = null_equal(a.is_null(ap), b.is_null(bp), nulls) {
                            *result = u8::from(equal);
                        } else {
                            let expected = left.tuples().encode_component_row(
                                column,
                                ar,
                                left.allocator.clone(),
                                checkpoint,
                            )?;
                            *result = u8::from(canonical_nullable_equals_observed(
                                b, bp, &expected, checkpoint,
                            )?);
                        }
                    }
                }
            }
            _ => return Err(invariant(ValueViolation::Carrier)),
        }
    }
    checkpoint()
}

pub(super) fn physical_row(mapping: Option<&[usize]>, row: usize) -> ExecutionResult<usize> {
    match mapping {
        Some(mapping) => mapping
            .get(row)
            .copied()
            .filter(|&row| row != usize::MAX)
            .ok_or_else(|| invariant(ValueViolation::UndefinedRow)),
        None => Ok(row),
    }
}
fn null_equal(a: bool, b: bool, policy: NullPolicy) -> Option<bool> {
    (a || b).then_some(a && b && policy == NullPolicy::Equal)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compare_fixed_pairs(
    a: &ArrayRef,
    am: Option<&[usize]>,
    left_rows: &[usize],
    b: &ArrayRef,
    bm: Option<&[usize]>,
    right_rows: &[usize],
    nulls: NullPolicy,
    output: &mut [u8],
    checkpoint: &dyn Fn() -> ExecutionResult<()>,
) -> ExecutionResult<()> {
    if a.data_type() != b.data_type() {
        return Err(invariant(ValueViolation::Layout));
    }
    macro_rules! primitive {
        ($ty:ty, $canonical:expr) => {{
            let a = a
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            let b = b
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (i, ((&ar, &br), result)) in left_rows
                .iter()
                .zip(right_rows)
                .zip(output.iter_mut())
                .enumerate()
            {
                checkpoint_at(i, checkpoint)?;
                if *result == 0 {
                    continue;
                }
                let ar = physical_row(am, ar)?;
                let br = physical_row(bm, br)?;
                *result = u8::from(
                    null_equal(a.is_null(ar), b.is_null(br), nulls)
                        .unwrap_or_else(|| ($canonical)(a.value(ar)) == ($canonical)(b.value(br))),
                );
            }
            Ok(())
        }};
    }
    match a.data_type() {
        DataType::Null => {
            for (i, result) in output.iter_mut().enumerate() {
                checkpoint_at(i, checkpoint)?;
                *result &= u8::from(nulls == NullPolicy::Equal);
            }
            Ok(())
        }
        DataType::Boolean => primitive!(BooleanArray, |v: bool| v),
        DataType::Int8 => primitive!(Int8Array, |v: i8| v),
        DataType::Int16 => primitive!(Int16Array, |v: i16| v),
        DataType::Int32 => primitive!(Int32Array, |v: i32| v),
        DataType::Int64 => primitive!(Int64Array, |v: i64| v),
        DataType::UInt8 => primitive!(UInt8Array, |v: u8| v),
        DataType::UInt16 => primitive!(UInt16Array, |v: u16| v),
        DataType::UInt32 => primitive!(UInt32Array, |v: u32| v),
        DataType::UInt64 => primitive!(UInt64Array, |v: u64| v),
        DataType::Float32 => primitive!(Float32Array, canonical_f32_bits),
        DataType::Float64 => primitive!(Float64Array, canonical_f64_bits),
        DataType::Decimal128(..) => primitive!(Decimal128Array, |v: i128| v),
        DataType::Decimal256(..) => primitive!(Decimal256Array, |v: arrow_buffer::i256| v),
        DataType::Date32 => primitive!(Date32Array, |v: i32| v),
        DataType::Date64 => primitive!(Date64Array, |v: i64| v),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondArray, |v: i32| v),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray, |v: i32| v),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray, |v: i64| v),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray, |v: i64| v),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray, |v: i64| v),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondArray, |v: i64| v)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondArray, |v: i64| v)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondArray, |v: i64| v),
        DataType::Duration(TimeUnit::Millisecond) => {
            primitive!(DurationMillisecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            primitive!(DurationMicrosecondArray, |v: i64| v)
        }
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondArray, |v: i64| v),
        DataType::FixedSizeBinary(_) => {
            let a = a
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            let b = b
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| invariant(ValueViolation::Carrier))?;
            for (i, ((&ar, &br), result)) in left_rows
                .iter()
                .zip(right_rows)
                .zip(output.iter_mut())
                .enumerate()
            {
                checkpoint_at(i, checkpoint)?;
                if *result == 0 {
                    continue;
                }
                let ar = physical_row(am, ar)?;
                let br = physical_row(bm, br)?;
                if let Some(equal) = null_equal(a.is_null(ar), b.is_null(br), nulls) {
                    *result = u8::from(equal);
                } else {
                    // FixedSizeBinary is an exact-width scalar, outside V's
                    // ordinary variable-length STRING/BINARY vocabulary.
                    for (a, b) in a
                        .value(ar)
                        .chunks(64 * 1024)
                        .zip(b.value(br).chunks(64 * 1024))
                    {
                        checkpoint()?;
                        if a != b {
                            *result = 0;
                            break;
                        }
                    }
                }
            }
            Ok(())
        }
        _ => Err(invariant(ValueViolation::Carrier)),
    }
}
