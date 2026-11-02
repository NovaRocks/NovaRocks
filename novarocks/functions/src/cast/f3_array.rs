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

//! Exact selected-array CAST bodies for bytes, wide decimals and metadata retags.
//! Nominal identity is checked during preparation; no carrier grants a domain change.
use super::*;
use crate::{SelectedValues, Selection, selected_copy};
use arrow_array::{
    ArrayRef, LargeListArray, ListArray, StructArray, UInt64Array, new_empty_array, new_null_array,
};
use arrow_buffer::i256;
use novarocks_type_contract::ValueTypeRef;
use std::{alloc::Layout, cell::RefCell, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Body {
    ArrowScalar,
    Structure,
}

fn bytes(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}
fn numeric(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}
pub(super) fn arrow_profile(source: &DataType, result: &DataType) -> bool {
    let admitted = (bytes(source) && bytes(result))
        || (matches!(
            source,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) && matches!(result, DataType::Binary | DataType::LargeBinary))
        || (matches!(result, DataType::Decimal256(..))
            && (numeric(source)
                || matches!(
                    source,
                    DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8
                )))
        || (matches!(
            source,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        ) && matches!(result, DataType::Decimal128(..)))
        || (matches!(source, DataType::Decimal128(..) | DataType::Decimal256(..))
            && matches!(
                result,
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            ))
        || (matches!(source, DataType::Timestamp(_, None)) && numeric(result))
        || (matches!(source, DataType::Time64(_) | DataType::Time32(_))
            && matches!(
                result,
                DataType::Int32
                    | DataType::Int64
                    | DataType::Time64(_)
                    | DataType::Time32(_)
                    | DataType::Utf8
                    | DataType::LargeUtf8
            ))
        || (source == &DataType::Int64 && result == &DataType::Time64(TimeUnit::Microsecond))
        || (source == &DataType::LargeUtf8
            && (numeric(result)
                || matches!(
                    result,
                    DataType::Boolean | DataType::Date32 | DataType::Time64(TimeUnit::Microsecond)
                )));
    // The original physical fixed-16 profile also carries numeric bytes.
    // This explicit carrier recipe never establishes a nominal LargeInt domain.
    let fixed_numeric = (result == &DataType::FixedSizeBinary(16)
        && (numeric(source) || matches!(source, DataType::Boolean | DataType::Utf8)))
        || (source == &DataType::FixedSizeBinary(16)
            && ((numeric(result)
                && !matches!(
                    result,
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                ))
                || result == &DataType::Boolean));
    fixed_numeric
        || (admitted
            && (arrow_cast::can_cast_types(source, result)
                || (source == &DataType::Boolean && matches!(result, DataType::Decimal256(..)))))
}

/// Offset widening preserves the source validity bitmap exactly. Other
/// admitted Arrow bodies retain their existing conservative NULL contract.
pub(super) fn can_produce_null(source: &DataType, result: &DataType) -> bool {
    !matches!((source, result), (DataType::Utf8, DataType::LargeUtf8))
}

pub(super) fn prepare(
    source: &FunctionValueType,
    result: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Body>, CastPrepareError> {
    if source.logical_type() != ValueLogicalType::Physical
        || result.logical_type() != ValueLogicalType::Physical
    {
        return Ok(None);
    }
    if arrow_profile(source.data_type(), result.data_type()) {
        if (source.nullable() || can_produce_null(source.data_type(), result.data_type()))
            && !result.nullable()
        {
            return Err(CastPrepareError::TypeMismatch);
        }
        return Ok(Some(Body::ArrowScalar));
    }
    if matches!(
        source.data_type(),
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_)
    ) && structural(
        source.value_type(),
        source.data_type(),
        result.value_type(),
        result.data_type(),
        work,
    )? {
        if source.nullable() && !result.nullable() {
            return Err(CastPrepareError::TypeMismatch);
        }
        return Ok(Some(Body::Structure));
    }
    Ok(None)
}
fn structural(
    source_value: &ValueTypeRef,
    source: &DataType,
    result_value: &ValueTypeRef,
    result: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CastPrepareError> {
    work.step()?;
    if source_value.nullable() && !result_value.nullable() {
        return Ok(false);
    }
    match (source, result) {
        (DataType::Struct(from), DataType::Struct(to)) if from.len() == to.len() => {
            let mut by_name = true;
            for field in to {
                let mut found = false;
                for candidate in from {
                    work.step()?;
                    found |= candidate.name() == field.name();
                }
                by_name &= found;
            }
            for (ordinal, field) in to.iter().enumerate() {
                let mut selected = ordinal;
                if by_name {
                    for (i, candidate) in from.iter().enumerate() {
                        work.step()?;
                        if candidate.name() == field.name() {
                            selected = i;
                        }
                    }
                }
                if !structural(
                    source_value
                        .child_type_at(selected)
                        .expect("validated source field"),
                    from[selected].data_type(),
                    result_value
                        .child_type_at(ordinal)
                        .expect("validated target field"),
                    field.data_type(),
                    work,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (DataType::List(from), DataType::List(to))
        | (DataType::LargeList(from), DataType::LargeList(to)) => structural(
            source_value.child_type_at(0).expect("validated list child"),
            from.data_type(),
            result_value.child_type_at(0).expect("validated list child"),
            to.data_type(),
            work,
        ),
        (DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_), _)
        | (_, DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_)) => Ok(false),
        _ => {
            if source != result {
                return Ok(false);
            }
            FunctionValueType::borrowed_value_domains_equal_observed::<KernelFailure>(
                source_value,
                result_value,
                || work.step().map_err(crate::kernel_control::compile_failure),
            )
            .map_err(CastPrepareError::Kernel)
        }
    }
}
fn copy_error(error: selected_copy::CopyError) -> KernelFailure {
    error.into_kernel()
}
fn reserve<T>(
    len: usize,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len)
        .map_err(|source| selected_copy::CopyError::layout::<T>(len, source).into_kernel())?;
    work.borrow_mut().flush()?;
    let mut out = Vec::new();
    out.try_reserve_exact(len).map_err(|source| {
        KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
            "selected CAST scratch allocation",
            source,
        ))
    })?;
    work.borrow_mut().flush()?;
    Ok(out)
}
pub(super) fn evaluate<'a>(
    body: Body,
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'a>,
    inherited: &[RowDataError],
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let work = RefCell::new(EvaluationCheckpoints::new(control));
    let outcome = (|| {
        match argument {
            EvaluatedArgument::SelectedColumn(values) => {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.borrow_mut().step())?
                    || !std::ptr::eq(inherited, values.errors())
                {
                    return Err(invalid(
                        "selected CAST has a foreign compact domain or journal",
                    ));
                }
            }
            _ => {
                if !inherited.is_empty() {
                    return Err(invalid("selected CAST has a foreign inherited journal"));
                }
                argument.validate_shape_observed::<KernelFailure>(selection, || {
                    work.borrow_mut().step()
                })?;
            }
        }
        if let Some(value) = argument.constant_value()
            && !value
                .value_type()
                .fits_value_type_observed(&recipe.source, || work.borrow_mut().step())?
        {
            return Err(invalid(
                "selected CAST constant differs from its frozen declaration",
            ));
        }
        if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            argument.array().data_type(),
            recipe.source.data_type(),
            || work.borrow_mut().step(),
        )? {
            return Err(invalid(
                "selected CAST source differs from its frozen carrier",
            ));
        }
        if !concrete_source(argument.array().as_ref()) {
            return Err(invalid(
                "selected CAST source has a foreign concrete carrier",
            ));
        }
        if body == Body::Structure {
            let mut indices = reserve::<Option<u64>>(selection.len(), &work)?;
            let mut journal = inherited.iter().peekable();
            for (ordinal, logical_row) in selection.iter().enumerate() {
                work.borrow_mut().step()?;
                if journal
                    .peek()
                    .is_some_and(|e| e.selected_ordinal() == ordinal)
                {
                    journal.next();
                    indices.push(None);
                    continue;
                }
                let row = argument.value_row(ordinal, logical_row);
                if row >= argument.array().len() {
                    return Err(invalid("selected CAST address exceeds its input"));
                }
                if !recipe.source.nullable() && argument.array().is_null(row) {
                    return Err(invalid("non-null selected CAST source contains NULL"));
                }
                indices.push(Some(
                    u64::try_from(row).map_err(|_| KernelFailure::ResourceExhausted)?,
                ));
            }
            if journal.next().is_some() {
                return Err(invalid(
                    "selected CAST inherited journal exceeds its domain",
                ));
            }
            selected_copy::preflight_take(argument.array().as_ref(), &indices, |boundary| {
                if boundary {
                    work.borrow_mut().flush()
                } else {
                    work.borrow_mut().step()
                }
            })
            .map_err(copy_error)?;
            work.borrow_mut().flush()?;
            let indices = UInt64Array::from(indices);
            let compact = arrow_select::take::take(argument.array().as_ref(), &indices, None)
                .map_err(|e| {
                    crate::control_values::AssemblyFailure::arrow("CAST projection", e)
                        .into_kernel()
                })?;
            work.borrow_mut().flush()?;
            let out = retag(&compact, recipe.result.data_type(), &work)?;
            let mut errors = reserve::<RowDataError>(inherited.len(), &work)?;
            for error in inherited {
                work.borrow_mut().flush()?;
                errors.push(error.clone());
            }
            work.borrow_mut().flush()?;
            return SelectedValues::try_new_observed(
                selection,
                recipe.result.data_type(),
                out,
                errors.into_boxed_slice(),
                || work.borrow_mut().step(),
            );
        }
        selected_copy::guarded_interleave_extent(recipe.result.data_type(), selection.len())
            .map_err(copy_error)?;
        let mut arrays = reserve::<ArrayRef>(selection.len(), &work)?;
        let mut errors = reserve::<RowDataError>(selection.len(), &work)?;
        let mut choices = reserve::<(usize, usize)>(selection.len(), &work)?;
        let mut journal = inherited.iter().peekable();
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.borrow_mut().step()?;
            let out = if journal
                .peek()
                .is_some_and(|e| e.selected_ordinal() == ordinal)
            {
                work.borrow_mut().flush()?;
                errors.push(journal.next().expect("checked journal").clone());
                new_null_array(recipe.result.data_type(), 1)
            } else {
                let row = argument.value_row(ordinal, logical_row);
                if row >= argument.array().len() {
                    return Err(invalid("selected CAST address exceeds its input"));
                }
                let source_null = argument.array().is_null(row);
                if source_null && !recipe.source.nullable() {
                    return Err(invalid("non-null selected CAST source contains NULL"));
                }
                work.borrow_mut().flush()?;
                let selected = argument.array().slice(row, 1);
                work.borrow_mut().flush()?;
                let converted = cast_one(
                    &selected,
                    recipe.result.data_type(),
                    recipe.allow_throw_exception,
                );
                match converted {
                    Ok(out) => {
                        work.borrow_mut().flush()?;
                        let nonfinite = match selected.data_type() {
                            DataType::Float32 => selected
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .is_some_and(|v| !v.value(0).is_finite()),
                            DataType::Float64 => selected
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .is_some_and(|v| !v.value(0).is_finite()),
                            _ => false,
                        };
                        let checked = novarocks_type_contract::is_checked_decimal_numeric_cast(
                            recipe.source.data_type(),
                            recipe.result.data_type(),
                        );
                        let both_decimal = matches!(
                            recipe.source.data_type(),
                            DataType::Decimal128(..) | DataType::Decimal256(..)
                        ) && matches!(
                            recipe.result.data_type(),
                            DataType::Decimal128(..) | DataType::Decimal256(..)
                        );
                        if checked
                            && !source_null
                            && !nonfinite
                            && out.is_null(0)
                            && (recipe.decimal_overflow_policy
                                == DecimalOverflowPolicy::ReportError
                                || (both_decimal && recipe.allow_throw_exception))
                        {
                            errors.push(RowDataError::with_reason(ordinal, crate::DataFailureReason::NumericOutOfRange, "Expr evaluate meet error: The numeric type cast involving decimal overflows"));
                        }
                        out
                    }
                    Err(cause) => {
                        let row = cause.into_row(ordinal)?;
                        work.borrow_mut().flush()?;
                        errors.push(row);
                        new_null_array(recipe.result.data_type(), 1)
                    }
                }
            };
            work.borrow_mut().flush()?;
            arrays.push(out);
            choices.push((ordinal, 0));
        }
        if journal.next().is_some() {
            return Err(invalid(
                "selected CAST inherited journal exceeds its domain",
            ));
        }
        work.borrow_mut().flush()?;
        let out = if arrays.is_empty() {
            new_empty_array(recipe.result.data_type())
        } else {
            selected_copy::preflight_guarded_interleave(
                recipe.result.data_type(),
                &arrays,
                &choices,
                |boundary| {
                    if boundary {
                        work.borrow_mut().flush()
                    } else {
                        work.borrow_mut().step()
                    }
                },
            )
            .map_err(copy_error)?;
            let mut sources = reserve::<&dyn Array>(arrays.len(), &work)?;
            for array in &arrays {
                work.borrow_mut().step()?;
                sources.push(array.as_ref());
            }
            work.borrow_mut().flush()?;
            arrow_select::interleave::interleave(&sources, &choices).map_err(|e| {
                crate::control_values::AssemblyFailure::arrow("selected CAST assembly", e)
                    .into_kernel()
            })?
        };
        work.borrow_mut().flush()?;
        SelectedValues::try_new_observed(
            selection,
            recipe.result.data_type(),
            out,
            errors.into_boxed_slice(),
            || work.borrow_mut().step(),
        )
    })();
    work.into_inner().finish_result(outcome)
}
fn concrete_source(array: &dyn Array) -> bool {
    macro_rules! is {
        ($ty:ty) => {
            array.as_any().is::<$ty>()
        };
    }
    match array.data_type() {
        DataType::Boolean => is!(BooleanArray),
        DataType::Int8 => is!(Int8Array),
        DataType::Int16 => is!(Int16Array),
        DataType::Int32 => is!(Int32Array),
        DataType::Int64 => is!(Int64Array),
        DataType::UInt8 => is!(UInt8Array),
        DataType::UInt16 => is!(UInt16Array),
        DataType::UInt32 => is!(UInt32Array),
        DataType::UInt64 => is!(UInt64Array),
        DataType::Float32 => is!(Float32Array),
        DataType::Float64 => is!(Float64Array),
        DataType::Decimal128(..) => is!(Decimal128Array),
        DataType::Decimal256(..) => is!(Decimal256Array),
        DataType::Utf8 => is!(StringArray),
        DataType::LargeUtf8 => is!(arrow_array::LargeStringArray),
        DataType::Binary => is!(BinaryArray),
        DataType::LargeBinary => is!(arrow_array::LargeBinaryArray),
        DataType::FixedSizeBinary(_) => is!(FixedSizeBinaryArray),
        DataType::Date32 => is!(Date32Array),
        DataType::Time32(TimeUnit::Second) => is!(arrow_array::Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => is!(arrow_array::Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => is!(arrow_array::Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => is!(arrow_array::Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => is!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => is!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => is!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => is!(TimestampNanosecondArray),
        DataType::Struct(_) => is!(StructArray),
        DataType::List(_) => is!(ListArray),
        DataType::LargeList(_) => is!(LargeListArray),
        _ => false,
    }
}

#[derive(Debug)]
pub(super) enum ArrayCastFailure {
    Outer(KernelFailure),
    Decimal(crate::legacy_decimal::DecimalMathFailure),
    Arrow(arrow_schema::ArrowError),
    RawNumeric(crate::math_numeric::RawNumericCastFailure),
    Temporal(crate::temporal_carrier::TemporalCastFailure),
}
impl From<KernelFailure> for ArrayCastFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Outer(cause)
    }
}
impl From<crate::legacy_decimal::DecimalMathFailure> for ArrayCastFailure {
    fn from(cause: crate::legacy_decimal::DecimalMathFailure) -> Self {
        Self::Decimal(cause)
    }
}
impl ArrayCastFailure {
    pub(super) fn into_row(self, ordinal: usize) -> Result<RowDataError, KernelFailure> {
        match self {
            Self::RawNumeric(cause) => RowDataError::from_domain(ordinal, cause),
            Self::Temporal(cause) => RowDataError::from_domain(ordinal, cause),
            Self::Outer(cause) => Err(cause),
            Self::Decimal(cause) => RowDataError::from_domain(ordinal, cause),
            Self::Arrow(cause) => RowDataError::from_domain(ordinal, ArrayCastArrowFailure(cause)),
        }
    }
}
#[derive(Debug)]
struct ArrayCastArrowFailure(arrow_schema::ArrowError);
impl std::fmt::Display for ArrayCastArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ArrayCastArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ArrayCastArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        match &self.0 {
            ArrowError::CastError(_) | ArrowError::ParseError(_) => {
                return crate::DataFailureReason::InvalidValue.failure_fact();
            }
            ArrowError::ArithmeticOverflow(_) => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            _ => {}
        }
        let (class, code) = match &self.0 {
            ArrowError::MemoryError(_) => (FailureClass::Resource, 6101),
            ArrowError::OffsetOverflowError(_) => (FailureClass::Representation, 6102),
            ArrowError::IoError(..) | ArrowError::ExternalError(_) => {
                (FailureClass::Unexpected, 6103)
            }
            _ => (FailureClass::Invariant, 6104),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if matches!(self.0, ArrowError::OffsetOverflowError(_)) {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(6101).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}

// Resolve only the legal decimal shapes whose Arrow i8 intermediates are
// unsafe. Outer None delegates to the unchanged Arrow rescale/rounding owner;
// inner None is a source NULL or a mathematically proven precision overflow.
pub(super) fn arrow_decimal_wide_delta(
    coefficient: Option<i256>,
    source_precision: u8,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
    target_carrier_precision: u8,
) -> Option<Option<i256>> {
    let delta = i16::from(target_scale) - i16::from(source_scale);
    let unsafe_upscale = delta >= 0
        && (delta > i16::from(i8::MAX)
            || (delta <= i16::from(target_carrier_precision)
                && i16::from(source_precision) + delta > i16::from(i8::MAX)));
    // Arrow looks up the target carrier's power table before evaluating the
    // precision sum. A representable delta past that table is an existing
    // static CastError, including on NULL; delegate that refusal unchanged.
    let unsafe_downscale = -delta > i16::from(i8::MAX);
    if !unsafe_upscale && !unsafe_downscale {
        return None;
    }
    let Some(coefficient) = coefficient else {
        return Some(None);
    };
    // A downscale here divides by at least10^128. Even the full i256 native
    // range has magnitude<10^77, hence rounds0 independently of the sign.
    if unsafe_downscale || coefficient == i256::ZERO {
        return Some(Some(i256::ZERO));
    }
    // A nonzero coefficient multiplied by10^77 cannot fit any supported
    // decimal precision. A smaller delta may fit Decimal256 even though
    // source_precision+delta overflows i8, so perform checked value arithmetic.
    if delta > 76 {
        return Some(None);
    }
    let value = crate::legacy_decimal::pow10_i256(delta as usize)
        .ok()
        .and_then(|factor| coefficient.checked_mul(factor));
    let limit = crate::legacy_decimal::pow10_i256(usize::from(target_precision)).ok();
    Some(
        value
            .zip(limit)
            .and_then(|(value, limit)| (value < limit && value > -limit).then_some(value)),
    )
}
fn cast_one(
    source: &ArrayRef,
    result: &DataType,
    allow_throw_exception: bool,
) -> Result<ArrayRef, ArrayCastFailure> {
    if source.data_type() == &DataType::LargeUtf8 && result == &DataType::Date32 {
        let source = source
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .ok_or_else(|| invalid("DATE text CAST has a foreign carrier"))?;
        return Ok(crate::temporal_carrier::cast_large_date_text(source));
    }
    if let DataType::Decimal256(target_precision, target_scale) = result {
        let input = match source.data_type() {
            DataType::Decimal128(precision, scale) => {
                let array = source
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| invalid("decimal widening CAST has a foreign carrier"))?;
                Some((
                    *precision,
                    *scale,
                    (!array.is_null(0)).then(|| i256::from_i128(array.value(0))),
                ))
            }
            DataType::Decimal256(precision, scale) => {
                let array = source
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| invalid("wide decimal CAST has a foreign carrier"))?;
                Some((
                    *precision,
                    *scale,
                    (!array.is_null(0)).then(|| array.value(0)),
                ))
            }
            _ => None,
        };
        if let Some((precision, scale, coefficient)) = input
            && let Some(value) = arrow_decimal_wide_delta(
                coefficient,
                precision,
                scale,
                *target_precision,
                *target_scale,
                arrow_schema::DECIMAL256_MAX_PRECISION,
            )
        {
            let array = Decimal256Array::from(vec![value])
                .with_precision_and_scale(*target_precision, *target_scale)
                .map_err(|cause| {
                    crate::control_values::AssemblyFailure::arrow("CAST decimal metadata", cause)
                        .into_kernel()
                })?;
            return Ok(Arc::new(array));
        }
    }

    if result == &DataType::FixedSizeBinary(16)
        && (numeric(source.data_type())
            || matches!(source.data_type(), DataType::Boolean | DataType::Utf8))
    {
        return crate::largeint::array_from_i128(&[numeric_i128(source)?])
            .map_err(|cause| ArrayCastFailure::Outer(cause.into()));
    }
    if source.data_type() == &DataType::FixedSizeBinary(16)
        && (numeric(result) || result == &DataType::Boolean)
    {
        let source = source
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("fixed numeric CAST has a foreign carrier"))?;
        let value = if source.is_null(0) {
            None
        } else {
            Some(crate::largeint::value_at(source, 0).map_err(KernelFailure::from)?)
        };
        macro_rules! primitive {
            ($array:ty,$map:expr) => {
                Ok(Arc::new(<$array>::from(vec![value.and_then($map)])) as ArrayRef)
            };
        }
        return match result {
            DataType::Int8 => primitive!(Int8Array, |v| i8::try_from(v).ok()),
            DataType::Int16 => primitive!(Int16Array, |v| i16::try_from(v).ok()),
            DataType::Int32 => primitive!(Int32Array, |v| i32::try_from(v).ok()),
            DataType::Int64 => primitive!(Int64Array, |v| i64::try_from(v).ok()),
            DataType::UInt8 => primitive!(UInt8Array, |v| u8::try_from(v).ok()),
            DataType::UInt16 => primitive!(UInt16Array, |v| u16::try_from(v).ok()),
            DataType::UInt32 => primitive!(UInt32Array, |v| u32::try_from(v).ok()),
            DataType::UInt64 => primitive!(UInt64Array, |v| u64::try_from(v).ok()),
            DataType::Float32 => primitive!(Float32Array, |v| Some(v as f32)),
            DataType::Float64 => primitive!(Float64Array, |v| Some(v as f64)),
            DataType::Boolean => primitive!(BooleanArray, |v| Some(v != 0)),
            DataType::Decimal128(..) | DataType::Decimal256(..) => fixed_decimal(value, result),
            _ => Err(invalid("fixed numeric CAST lost its selected target").into()),
        };
    }
    if source.data_type() == &DataType::Boolean
        && let DataType::Decimal256(precision, scale) = result
    {
        let mut factor = Some(arrow_buffer::i256::ONE);
        for _ in 0..scale.unsigned_abs() {
            factor = factor.and_then(|v| v.checked_mul(arrow_buffer::i256::from_i128(10)));
        }
        let factor = factor.ok_or(crate::legacy_decimal::DecimalMathFailure::Overflow)?;
        let source = source
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("boolean CAST has a foreign carrier"))?;
        let value = if source.is_null(0) {
            None
        } else {
            let value = arrow_buffer::i256::from_i128(i128::from(source.value(0)));
            if *scale < 0 {
                value.checked_div(factor)
            } else {
                value.checked_mul(factor)
            }
        };
        // Arrow's precision mask uses unary_opt, which creates a fresh
        // primitive array with default decimal metadata. Apply the exact
        // frozen precision/scale after masking, before output assembly.
        let array = Decimal256Array::from(vec![value])
            .null_if_overflow_precision(*precision)
            .with_precision_and_scale(*precision, *scale)
            .map_err(|cause| {
                crate::control_values::AssemblyFailure::arrow("CAST decimal metadata", cause)
                    .into_kernel()
            })?;
        return Ok(Arc::new(array));
    }
    if matches!(
        source.data_type(),
        DataType::Time32(_) | DataType::Time64(_)
    ) && matches!(result, DataType::Time32(_) | DataType::Time64(_))
    {
        return match crate::temporal_carrier::cast_time_units(source.as_ref(), result) {
            Ok(out) => Ok(out),
            Err(cause) if cause.is_data() && !allow_throw_exception => {
                Ok(new_null_array(result, source.len()))
            }
            Err(cause) => Err(ArrayCastFailure::Temporal(cause)),
        };
    }
    let out = if numeric(result) {
        if let Some(text) = source
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
        {
            crate::math_numeric::cast_raw_numeric(text, result)
                .map_err(ArrayCastFailure::RawNumeric)?
        } else if let Some(text) = source.as_any().downcast_ref::<StringArray>() {
            crate::math_numeric::cast_raw_numeric(text, result)
                .map_err(ArrayCastFailure::RawNumeric)?
        } else {
            arrow_cast::cast(source.as_ref(), result).map_err(ArrayCastFailure::Arrow)?
        }
    } else {
        arrow_cast::cast(source.as_ref(), result).map_err(ArrayCastFailure::Arrow)?
    };
    // The old scalar evaluator sanitized parser-produced non-finite floats.
    let invalid = match result {
        DataType::Float32 => out
            .as_any()
            .downcast_ref::<Float32Array>()
            .is_some_and(|v| !v.is_null(0) && !v.value(0).is_finite()),
        DataType::Float64 => out
            .as_any()
            .downcast_ref::<Float64Array>()
            .is_some_and(|v| !v.is_null(0) && !v.value(0).is_finite()),
        _ => false,
    };
    Ok(if invalid {
        new_null_array(result, 1)
    } else {
        out
    })
}
pub(super) fn numeric_i128(source: &ArrayRef) -> Result<Option<i128>, KernelFailure> {
    if source.is_null(0) {
        return Ok(None);
    }
    macro_rules! primitive {
        ($array:ty) => {
            source
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| invalid("numeric fixed CAST has a foreign carrier"))?
                .value(0)
        };
    }
    let value = match source.data_type() {
        DataType::Boolean => Some(i128::from(primitive!(BooleanArray))),
        DataType::FixedSizeBinary(16) => Some(
            crate::largeint::value_at(
                source
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| invalid("numeric fixed CAST has a foreign carrier"))?,
                0,
            )
            .map_err(KernelFailure::from)?,
        ),
        DataType::Int8 => Some(i128::from(primitive!(Int8Array))),
        DataType::Int16 => Some(i128::from(primitive!(Int16Array))),
        DataType::Int32 => Some(i128::from(primitive!(Int32Array))),
        DataType::Int64 => Some(i128::from(primitive!(Int64Array))),
        DataType::UInt8 => Some(i128::from(primitive!(UInt8Array))),
        DataType::UInt16 => Some(i128::from(primitive!(UInt16Array))),
        DataType::UInt32 => Some(i128::from(primitive!(UInt32Array))),
        DataType::UInt64 => Some(i128::from(primitive!(UInt64Array))),
        DataType::Float32 => {
            let value = f64::from(primitive!(Float32Array));
            (value.is_finite() && value >= i128::MIN as f64 && value <= i128::MAX as f64)
                .then_some(value.trunc() as i128)
        }
        DataType::Float64 => {
            let value = primitive!(Float64Array);
            (value.is_finite() && value >= i128::MIN as f64 && value <= i128::MAX as f64)
                .then_some(value.trunc() as i128)
        }
        DataType::Utf8 => crate::largeint_text::parse_utf8(primitive!(StringArray)),
        DataType::Decimal128(_, scale) => {
            let value = primitive!(Decimal128Array);
            crate::legacy_decimal::checked_pow10_i128(usize::from(scale.unsigned_abs())).and_then(
                |factor| {
                    if *scale < 0 {
                        value.checked_mul(factor)
                    } else {
                        value.checked_div(factor)
                    }
                },
            )
        }
        DataType::Decimal256(_, scale) => {
            let value = primitive!(Decimal256Array);
            crate::legacy_decimal::pow10_i256(usize::from(scale.unsigned_abs()))
                .ok()
                .and_then(|factor| {
                    if *scale < 0 {
                        value.checked_mul(factor)
                    } else {
                        value.checked_div(factor)
                    }
                })
                .and_then(|value| value.to_i128())
        }
        _ => return Err(invalid("numeric fixed CAST lost its selected source")),
    };
    Ok(value)
}
fn fixed_decimal(value: Option<i128>, result: &DataType) -> Result<ArrayRef, ArrayCastFailure> {
    match result {
        DataType::Decimal128(precision, scale) => {
            let factor =
                crate::legacy_decimal::checked_pow10_i128(usize::from(scale.unsigned_abs()))
                    .ok_or(crate::legacy_decimal::DecimalMathFailure::Overflow)?;
            let value = value.and_then(|value| {
                if *scale < 0 {
                    value.checked_div(factor)
                } else {
                    value.checked_mul(factor)
                }
            });
            let array = Decimal128Array::from(vec![value])
                .null_if_overflow_precision(*precision)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|cause| {
                    crate::control_values::AssemblyFailure::arrow("CAST decimal metadata", cause)
                        .into_kernel()
                })?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let factor = crate::legacy_decimal::pow10_i256(usize::from(scale.unsigned_abs()))?;
            let value = value.map(arrow_buffer::i256::from_i128).and_then(|value| {
                if *scale < 0 {
                    value.checked_div(factor)
                } else {
                    value.checked_mul(factor)
                }
            });
            let array = Decimal256Array::from(vec![value])
                .null_if_overflow_precision(*precision)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|cause| {
                    crate::control_values::AssemblyFailure::arrow("CAST decimal metadata", cause)
                        .into_kernel()
                })?;
            Ok(Arc::new(array))
        }
        _ => Err(invalid("numeric fixed decimal CAST lost its selected target").into()),
    }
}

fn retag(
    source: &ArrayRef,
    result: &DataType,
    work: &RefCell<EvaluationCheckpoints<'_>>,
) -> Result<ArrayRef, KernelFailure> {
    work.borrow_mut().step()?;
    if source.data_type() == result {
        return Ok(Arc::clone(source));
    }
    match (source.data_type(), result) {
        (DataType::Struct(from), DataType::Struct(to)) => {
            let source = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("retag source is not a StructArray"))?;
            let mut by_name = true;
            for field in to {
                let mut found = false;
                for candidate in from {
                    work.borrow_mut().step()?;
                    found |= candidate.name() == field.name();
                }
                by_name &= found;
            }
            let mut columns = reserve::<ArrayRef>(to.len(), work)?;
            for (ordinal, field) in to.iter().enumerate() {
                let mut selected = ordinal;
                if by_name {
                    for (i, candidate) in from.iter().enumerate() {
                        work.borrow_mut().step()?;
                        if candidate.name() == field.name() {
                            selected = i;
                        }
                    }
                }
                columns.push(retag(source.column(selected), field.data_type(), work)?);
            }
            work.borrow_mut().flush()?;
            let array = StructArray::try_new(to.clone(), columns, source.nulls().cloned())
                .map_err(|e| {
                    crate::control_values::AssemblyFailure::arrow("signed Struct CAST", e)
                        .into_kernel()
                })?;
            work.borrow_mut().flush()?;
            Ok(Arc::new(array))
        }
        (DataType::List(_), DataType::List(field)) => {
            let source = source
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| invalid("retag source is not a ListArray"))?;
            let child = retag(source.values(), field.data_type(), work)?;
            work.borrow_mut().flush()?;
            let array = ListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                child,
                source.nulls().cloned(),
            )
            .map_err(|e| {
                crate::control_values::AssemblyFailure::arrow("signed List CAST", e).into_kernel()
            })?;
            work.borrow_mut().flush()?;
            Ok(Arc::new(array))
        }
        (DataType::LargeList(_), DataType::LargeList(field)) => {
            let source = source
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| invalid("retag source is not a LargeListArray"))?;
            let child = retag(source.values(), field.data_type(), work)?;
            work.borrow_mut().flush()?;
            let array = LargeListArray::try_new(
                Arc::clone(field),
                source.offsets().clone(),
                child,
                source.nulls().cloned(),
            )
            .map_err(|e| {
                crate::control_values::AssemblyFailure::arrow("signed LargeList CAST", e)
                    .into_kernel()
            })?;
            work.borrow_mut().flush()?;
            Ok(Arc::new(array))
        }
        _ => Err(invalid(
            "prepared structural CAST contains an unselected value conversion",
        )),
    }
}

#[cfg(test)]
#[path = "f3_array/tests.rs"]
mod tests;
