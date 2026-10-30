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

//! Frozen scalar Arrow cast paths used by ROUND. Encodings are followed only
//! at selected rows; no dictionary, run, list or Union is expanded into a batch.

use super::rounding_binding::{CastCapability, CastTarget, select_union_cast_field_typed};
use crate::{
    FunctionBindingError, FunctionValueType, KernelDiagnostic, KernelFailure,
    kernel_control::internal, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{types::*, *};
use arrow_cast::cast::DecimalCast;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
#[cfg(test)]
use novarocks_type_contract::ValueLogicalType;
use novarocks_type_contract::{CompileCheckpoints, MAX_VALUE_TYPE_DEPTH};
use num_traits::ToPrimitive;

type Wide = <Decimal256Type as ArrowPrimitiveType>::Native;

#[derive(Clone, Copy, Debug)]
enum IndexKind {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
}
#[derive(Clone, Copy, Debug)]
enum Leaf {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Utf8,
    LargeUtf8,
    Utf8View,
    Decimal32(i8),
    Decimal64(i8),
    Decimal128(i8),
    Decimal256(i8),
    Timestamp(TimeUnit),
    Duration(TimeUnit),
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    // Arrow's capability table advertises these casts, but the actual runtime
    // has no corresponding arm. Preserve the deferred outer cast failure.
    Interval(IntervalUnit),
}
#[derive(Clone, Copy, Debug)]
enum Factor {
    None,
    Float(f64),
    D32(Option<i32>),
    D64(Option<i64>),
    D128(Option<i128>),
    D256(Option<Wide>),
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Dictionary(IndexKind),
    RunEnd(IndexKind),
    FixedSizeList,
    Union(i8),
    Done,
}

/// Complete immutable selected path with no owned heap or shared Arc backing.
/// Copying into an instance does not re-resolve or compile the source type.
#[derive(Clone, Copy, Debug)]
pub(super) struct CastRecipe {
    steps: [Step; MAX_VALUE_TYPE_DEPTH],
    count: usize,
    leaf: Leaf,
    target: CastTarget,
    factor: Factor,
}
pub(super) enum CastValue {
    Null,
    Float(f64),
    Integer(i64),
    CheckedDecimalOverflow,
}

fn unsupported() -> FunctionBindingError {
    FunctionBindingError::NoMatchingOverload
}
fn key_kind(ty: &DataType) -> Result<IndexKind, FunctionBindingError> {
    Ok(match ty {
        DataType::Int8 => IndexKind::I8,
        DataType::Int16 => IndexKind::I16,
        DataType::Int32 => IndexKind::I32,
        DataType::Int64 => IndexKind::I64,
        DataType::UInt8 => IndexKind::U8,
        DataType::UInt16 => IndexKind::U16,
        DataType::UInt32 => IndexKind::U32,
        DataType::UInt64 => IndexKind::U64,
        _ => return Err(unsupported()),
    })
}
impl CastRecipe {
    /// The caller already performed shared observed full-type preflight and
    /// exact binding validation. Selected field choice has one shared author.
    pub(super) fn prepare(
        source: &FunctionValueType,
        target: CastTarget,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FunctionBindingError> {
        Self::prepare_typed(source.data_type(), source.value_type(), target, work)
    }

    /// Borrow an already admitted source type without cloning its field tree.
    /// The caller supplies the exact authored root identity and the same
    /// observed full-type preflight used by `prepare`.
    #[cfg(test)]
    pub(super) fn prepare_type(
        source: &DataType,
        logical_type: ValueLogicalType,
        target: CastTarget,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FunctionBindingError> {
        let value = FunctionValueType::try_from_legacy_declared_projection(
            source.clone(),
            true,
            logical_type,
        )
        .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        Self::prepare_typed(source, value.value_type(), target, work)
    }
    fn prepare_typed(
        source: &DataType,
        semantic: &novarocks_type_contract::ValueTypeRef,
        target: CastTarget,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FunctionBindingError> {
        if semantic.domain().is_some() {
            return Err(unsupported());
        }
        let mut semantic = semantic;
        let mut steps = [Step::Done; MAX_VALUE_TYPE_DEPTH];
        let mut count = 0;
        let mut ty = source;
        loop {
            work.step()?;
            let depth = count + 1;
            if depth > MAX_VALUE_TYPE_DEPTH {
                return Err(unsupported());
            }
            let (step, next) = match ty {
                DataType::Dictionary(key, value) => {
                    (Step::Dictionary(key_kind(key)?), value.as_ref())
                }
                DataType::RunEndEncoded(key, value) => {
                    (Step::RunEnd(key_kind(key.data_type())?), value.data_type())
                }
                DataType::FixedSizeList(value, 1) => {
                    semantic = semantic.child_type_at(0).ok_or_else(unsupported)?;
                    if semantic.domain().is_some() {
                        return Err(unsupported());
                    }
                    (Step::FixedSizeList, value.data_type())
                }
                DataType::Union(fields, _) => {
                    let (tag, field, selected_semantic, capability) =
                        select_union_cast_field_typed(fields, semantic, target, depth + 1, work)?
                            .ok_or_else(unsupported)?;
                    if capability != CastCapability::Physical {
                        return Err(unsupported());
                    }
                    semantic = selected_semantic;
                    (Step::Union(tag), field.data_type())
                }
                _ => {
                    let (leaf, factor) = freeze_leaf(ty, target, work)?;
                    return Ok(Self {
                        steps,
                        count,
                        leaf,
                        target,
                        factor,
                    });
                }
            };
            steps[count] = step;
            count += 1;
            ty = next;
        }
    }
    pub(super) fn checked_decimal_overflow(&self) -> bool {
        matches!(
            (self.target, self.factor),
            (
                CastTarget::Int64,
                Factor::D128(Some(_)) | Factor::D256(Some(_))
            )
        )
    }
    /// Static cast failure is retained inertly during prepare. Unreachable
    /// calls must not expose it; nonempty calls check before reading row NULLs.
    pub(super) fn check_available(&self) -> Result<(), KernelFailure> {
        match (self.leaf, self.factor) {
            (
                _,
                Factor::D32(None) | Factor::D64(None) | Factor::D128(None) | Factor::D256(None),
            ) => Err(KernelFailure::Operational(KernelDiagnostic::new(
                "round decimal digits scale causes native factor overflow",
            ))),
            (Leaf::Interval(_), _) => Err(KernelFailure::Operational(KernelDiagnostic::new(
                "round cannot cast interval digits to Int64",
            ))),
            _ => Ok(()),
        }
    }
    pub(super) fn read(
        &self,
        mut array: &dyn Array,
        mut row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<CastValue, KernelFailure> {
        for step in &self.steps[..self.count] {
            work.step()?;
            if row >= array.len() {
                return Err(internal("round encoded row is outside its exact carrier"));
            }
            // Accepted Strict respects parent SQL NULL, including FSL parents
            // that legacy Arrow cast incorrectly omitted from its bitmap.
            if array.is_null(row) {
                return Ok(CastValue::Null);
            }
            (array, row) = match step {
                Step::Dictionary(kind) => {
                    macro_rules! dict {
                        ($t:ty) => {{
                            let a = array
                                .as_any()
                                .downcast_ref::<DictionaryArray<$t>>()
                                .ok_or_else(|| {
                                    internal("round dictionary carrier cannot be downcast")
                                })?;
                            let Some(index) = a.key(row) else {
                                return Ok(CastValue::Null);
                            };
                            (a.values().as_ref(), index)
                        }};
                    }
                    match kind {
                        IndexKind::I8 => dict!(Int8Type),
                        IndexKind::I16 => dict!(Int16Type),
                        IndexKind::I32 => dict!(Int32Type),
                        IndexKind::I64 => dict!(Int64Type),
                        IndexKind::U8 => dict!(UInt8Type),
                        IndexKind::U16 => dict!(UInt16Type),
                        IndexKind::U32 => dict!(UInt32Type),
                        IndexKind::U64 => dict!(UInt64Type),
                    }
                }
                Step::RunEnd(kind) => {
                    macro_rules! run {
                        ($t:ty) => {{
                            let a = array
                                .as_any()
                                .downcast_ref::<RunArray<$t>>()
                                .ok_or_else(|| internal("round run carrier cannot be downcast"))?;
                            let index = run_index(a, row, work)?;
                            (a.values().as_ref(), index)
                        }};
                    }
                    match kind {
                        IndexKind::I16 => run!(Int16Type),
                        IndexKind::I32 => run!(Int32Type),
                        IndexKind::I64 => run!(Int64Type),
                        _ => return Err(internal("round frozen run index is invalid")),
                    }
                }
                Step::FixedSizeList => {
                    let a = array
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            internal("round singleton list carrier cannot be downcast")
                        })?;
                    (a.values().as_ref(), row)
                }
                Step::Union(tag) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<UnionArray>()
                        .ok_or_else(|| internal("round Union carrier cannot be downcast"))?;
                    if a.type_id(row) != *tag {
                        return Ok(CastValue::Null);
                    }
                    (a.child(*tag).as_ref(), a.value_offset(row))
                }
                Step::Done => return Err(internal("round frozen path has an unused step")),
            };
        }
        work.step()?;
        if row >= array.len() {
            return Err(internal("round scalar row is outside its exact carrier"));
        }
        if array.is_null(row) {
            return Ok(CastValue::Null);
        }
        read_leaf(self.leaf, self.target, self.factor, array, row, work)
    }
}
pub(super) fn checked_decimal_digits(
    source: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    Ok(CastRecipe::prepare(source, CastTarget::Int64, work)?.checked_decimal_overflow())
}
fn run_index<T: RunEndIndexType>(
    array: &RunArray<T>,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure>
where
    T::Native: ToPrimitive,
{
    let ends = array.run_ends();
    let logical = ends
        .offset()
        .checked_add(row)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let mut low = 0;
    let mut high = ends.values().len();
    while low < high {
        work.step()?;
        let mid = low + (high - low) / 2;
        let end = ends.values()[mid]
            .to_usize()
            .ok_or_else(|| internal("round run end is not representable"))?;
        if end <= logical {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low >= ends.values().len() {
        return Err(internal("round selected run is outside its values"));
    }
    Ok(low)
}
fn freeze_leaf(
    ty: &DataType,
    target: CastTarget,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Leaf, Factor), FunctionBindingError> {
    let leaf = match ty {
        DataType::Null => Leaf::Null,
        DataType::Boolean => Leaf::Bool,
        DataType::Int8 => Leaf::I8,
        DataType::Int16 => Leaf::I16,
        DataType::Int32 => Leaf::I32,
        DataType::Int64 => Leaf::I64,
        DataType::UInt8 => Leaf::U8,
        DataType::UInt16 => Leaf::U16,
        DataType::UInt32 => Leaf::U32,
        DataType::UInt64 => Leaf::U64,
        DataType::Float16 => Leaf::F16,
        DataType::Float32 => Leaf::F32,
        DataType::Float64 => Leaf::F64,
        DataType::Utf8 => Leaf::Utf8,
        DataType::LargeUtf8 => Leaf::LargeUtf8,
        DataType::Utf8View => Leaf::Utf8View,
        DataType::Decimal32(_, s) => Leaf::Decimal32(*s),
        DataType::Decimal64(_, s) => Leaf::Decimal64(*s),
        DataType::Decimal128(_, s) => Leaf::Decimal128(*s),
        DataType::Decimal256(_, s) => Leaf::Decimal256(*s),
        DataType::Timestamp(u, _) => Leaf::Timestamp(*u),
        DataType::Duration(u) => Leaf::Duration(*u),
        DataType::Date32 => Leaf::Date32,
        DataType::Date64 => Leaf::Date64,
        DataType::Time32(u) => Leaf::Time32(*u),
        DataType::Time64(u) => Leaf::Time64(*u),
        DataType::Interval(u) => Leaf::Interval(*u),
        _ => return Err(unsupported()),
    };
    // A non-recursive probe against primitive targets shares Arrow's
    // actual leaf capability vocabulary with the binding author.
    let target_ty = match target {
        CastTarget::Float64 => DataType::Float64,
        CastTarget::Int64 => DataType::Int64,
    };
    work.step()?;
    if !super::rounding_binding::primitive_cast_capability(ty, &target_ty) {
        return Err(unsupported());
    }
    let factor = match (leaf, target) {
        (
            Leaf::Decimal32(s) | Leaf::Decimal64(s) | Leaf::Decimal128(s) | Leaf::Decimal256(s),
            CastTarget::Float64,
        ) => Factor::Float(10_f64.powi(i32::from(s))),
        (Leaf::Decimal32(s), CastTarget::Int64) => {
            Factor::D32(10_i32.checked_pow(u32::from(s.unsigned_abs())))
        }
        (Leaf::Decimal64(s), CastTarget::Int64) => {
            Factor::D64(10_i64.checked_pow(u32::from(s.unsigned_abs())))
        }
        (Leaf::Decimal128(s), CastTarget::Int64) => {
            Factor::D128(10_i128.checked_pow(u32::from(s.unsigned_abs())))
        }
        (Leaf::Decimal256(s), CastTarget::Int64) => {
            Factor::D256(Wide::from_i128(10).checked_pow(u32::from(s.unsigned_abs())))
        }
        _ => Factor::None,
    };
    Ok((leaf, factor))
}
fn read_leaf(
    leaf: Leaf,
    target: CastTarget,
    factor: Factor,
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastValue, KernelFailure> {
    macro_rules! value {
        ($t:ty) => {
            array
                .as_any()
                .downcast_ref::<$t>()
                .ok_or_else(|| internal("round scalar carrier cannot be downcast"))?
                .value(row)
        };
    }
    macro_rules! integer {
        ($v:expr) => {{
            let value = $v;
            Ok(match target {
                CastTarget::Float64 => CastValue::Float(value as f64),
                CastTarget::Int64 => match i64::try_from(value) {
                    Ok(v) => CastValue::Integer(v),
                    Err(_) => CastValue::Null,
                },
            })
        }};
    }
    macro_rules! float {
        ($v:expr) => {{
            let value = $v;
            Ok(match target {
                CastTarget::Float64 => CastValue::Float(value),
                CastTarget::Int64 => match <i64 as DecimalCast>::from_f64(value) {
                    Some(v) => CastValue::Integer(v),
                    None => CastValue::Null,
                },
            })
        }};
    }
    macro_rules! decimal {
        ($t:ty,$scale:expr,$factor:ident,$wide:expr,$to_f64:expr) => {{
            let raw = value!($t);
            match (target, factor) {
                (CastTarget::Float64, Factor::Float(divisor)) => Ok(CastValue::Float(
                    ($to_f64)(raw)
                        .ok_or_else(|| internal("round decimal cannot convert to f64"))?
                        / divisor,
                )),
                (CastTarget::Int64, Factor::$factor(Some(factor))) => {
                    let converted = if $scale < 0 {
                        raw.checked_mul(factor)
                    } else {
                        raw.checked_div(factor)
                    }
                    .and_then(|v| v.to_i64());
                    Ok(match converted {
                        Some(v) => CastValue::Integer(v),
                        None if $wide => CastValue::CheckedDecimalOverflow,
                        None => CastValue::Null,
                    })
                }
                _ => Err(internal("round decimal frozen factor is unavailable")),
            }
        }};
    }
    match leaf {
        Leaf::Null => Ok(CastValue::Null),
        Leaf::Bool => integer!(u8::from(value!(BooleanArray))),
        Leaf::I8 => integer!(value!(Int8Array)),
        Leaf::I16 => integer!(value!(Int16Array)),
        Leaf::I32 => integer!(value!(Int32Array)),
        Leaf::I64 => integer!(value!(Int64Array)),
        Leaf::U8 => integer!(value!(UInt8Array)),
        Leaf::U16 => integer!(value!(UInt16Array)),
        Leaf::U32 => integer!(value!(UInt32Array)),
        Leaf::U64 => integer!(value!(UInt64Array)),
        Leaf::F16 => float!(value!(Float16Array).to_f64()),
        Leaf::F32 => float!(f64::from(value!(Float32Array))),
        Leaf::F64 => float!(value!(Float64Array)),
        Leaf::Utf8 | Leaf::LargeUtf8 | Leaf::Utf8View => {
            let text = match leaf {
                Leaf::Utf8 => value!(StringArray),
                Leaf::LargeUtf8 => value!(LargeStringArray),
                Leaf::Utf8View => value!(StringViewArray),
                _ => unreachable!(),
            };
            Ok(match target {
                CastTarget::Float64 => match super::round_cast_float_text::parse_f64(text, work)? {
                    Some(v) => CastValue::Float(v),
                    None => CastValue::Null,
                },
                CastTarget::Int64 => match super::round_cast_text::parse_i64(text, work)? {
                    Some(v) => CastValue::Integer(v),
                    None => CastValue::Null,
                },
            })
        }
        Leaf::Decimal32(s) => decimal!(Decimal32Array, s, D32, false, |v: i32| v.to_f64()),
        Leaf::Decimal64(s) => decimal!(Decimal64Array, s, D64, false, |v: i64| v.to_f64()),
        Leaf::Decimal128(s) => decimal!(Decimal128Array, s, D128, true, |v: i128| v.to_f64()),
        Leaf::Decimal256(s) => decimal!(Decimal256Array, s, D256, true, |v| Some(
            crate::decimal_float_cast::decimal256_unscaled_to_f64(v)
        )),
        Leaf::Timestamp(unit) => match unit {
            TimeUnit::Second => integer!(value!(TimestampSecondArray)),
            TimeUnit::Millisecond => integer!(value!(TimestampMillisecondArray)),
            TimeUnit::Microsecond => integer!(value!(TimestampMicrosecondArray)),
            TimeUnit::Nanosecond => integer!(value!(TimestampNanosecondArray)),
        },
        Leaf::Duration(unit) => match unit {
            TimeUnit::Second => integer!(value!(DurationSecondArray)),
            TimeUnit::Millisecond => integer!(value!(DurationMillisecondArray)),
            TimeUnit::Microsecond => integer!(value!(DurationMicrosecondArray)),
            TimeUnit::Nanosecond => integer!(value!(DurationNanosecondArray)),
        },
        Leaf::Date32 => integer!(value!(Date32Array)),
        Leaf::Date64 => integer!(value!(Date64Array)),
        Leaf::Time32(unit) => match unit {
            TimeUnit::Second => integer!(value!(Time32SecondArray)),
            TimeUnit::Millisecond => integer!(value!(Time32MillisecondArray)),
            _ => Err(internal("round invalid Time32 unit")),
        },
        Leaf::Time64(unit) => match unit {
            TimeUnit::Microsecond => integer!(value!(Time64MicrosecondArray)),
            TimeUnit::Nanosecond => integer!(value!(Time64NanosecondArray)),
            _ => Err(internal("round invalid Time64 unit")),
        },
        Leaf::Interval(unit) => {
            let _ = unit;
            Err(internal("round interval cast failure was not checked"))
        }
    }
}

#[cfg(test)]
#[path = "round_cast_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "round_cast_arrow_tests.rs"]
mod arrow_tests;
