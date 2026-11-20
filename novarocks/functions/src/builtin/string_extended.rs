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

//! Selected string encodings, fixed-width numeric rendering and chained Murmur3.
//! Every variable-size traversal observes the original evaluation control.
pub use super::murmur::{
    format_decimal_with_scale, format_decimal256_with_scale, format_float32_for_varchar,
    format_float64_for_varchar, format_timestamp_for_varchar, murmur_hash3_32,
};
pub use super::string_extended_owner::Operation as StringOperation;
pub use super::string_failure::{RegexSyntax, StringDataError};
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use StringOperation as Operation;
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, StringArray};
use arrow_buffer::{
    BooleanBuffer, BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;

pub(super) fn validate_profile(
    op: Operation,
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    let arity = match op {
        Operation::Unhex => types.len() == 1,
        Operation::ToBinary => types.len() == 1 || types.len() == 2,
        Operation::Murmur => !types.is_empty(),
        Operation::RegexpReplace | Operation::RegexpExtract | Operation::RegexpExtractAll => {
            types.len() == 3
        }
    };
    if !arity {
        return Err(invalid(
            "extended string call differs from its exact selected arity",
        ));
    }
    for (ordinal, ty) in types.iter().enumerate() {
        let FunctionArgumentType::Value(ty) = ty else {
            return Err(invalid("extended string requires value arguments"));
        };
        step()?;
        let physical = ty.logical_type() == ValueLogicalType::Physical;
        let admitted = match op {
            Operation::Unhex | Operation::ToBinary | Operation::RegexpReplace => {
                physical && ty.data_type() == &DataType::Utf8
            }
            Operation::RegexpExtract | Operation::RegexpExtractAll => {
                physical
                    && if ordinal < 2 {
                        ty.data_type() == &DataType::Utf8
                    } else {
                        ty.data_type() == &DataType::Int32
                    }
            }
            Operation::Murmur => {
                (physical
                    && matches!(
                        ty.data_type(),
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64
                            | DataType::Boolean
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Decimal128(..)
                            | DataType::Decimal256(..)
                            | DataType::Timestamp(_, None)
                    ))
                    || (ty.logical_type() == ValueLogicalType::LargeInt
                        && ty.data_type() == &DataType::FixedSizeBinary(16))
            }
        };
        if !admitted {
            return Err(invalid(
                "extended string call has no exact installed selected profile",
            ));
        }
    }
    let expected = match op {
        Operation::Unhex | Operation::ToBinary => DataType::Binary,
        Operation::RegexpReplace | Operation::RegexpExtract | Operation::RegexpExtractAll => {
            DataType::Utf8
        }
        Operation::Murmur => DataType::Int32,
    };
    step()?;
    if contract.result_type().logical_type() != ValueLogicalType::Physical
        || *contract.result_type().data_type() != expected
    {
        return Err(invalid(
            "extended string call differs from its exact selected result profile",
        ));
    }
    Ok(())
}

fn reserve<T>(n: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    super::string_failure::array_layout::<T>(n)?;
    work.flush()?;
    let mut v = Vec::new();
    v.try_reserve_exact(n)
        .map_err(|source| super::string_failure::reservation::<T>(n, source))?;
    work.flush()?;
    Ok(v)
}
#[derive(Clone, Copy)]
pub(super) enum Row {
    Null,
    Value,
}
/// Carrier-neutral shared arguments. The shell owns binding and projection.
/// Optional adapter for promoting a producer-declared string row failure.
pub type StringErrorBoundary<'call> =
    Option<&'call dyn for<'error> Fn(usize, StringDataError<'error>) -> Result<(), KernelFailure>>;

#[derive(Clone, Copy)]
pub struct StringCoreInput<'call, 'a> {
    pub arguments: &'a [EvaluatedArgument<'a>],
    pub selection: Selection<'a>,
    /// Required-call adapters promote only a producer-declared row data failure.
    pub error_boundary: StringErrorBoundary<'call>,
}
impl<'a> StringCoreInput<'_, 'a> {
    fn arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.arguments
    }
    fn selection(self) -> Selection<'a> {
        self.selection
    }
}
fn argument_row(
    input: &StringCoreInput<'_, '_>,
    i: usize,
    ordinal: usize,
    batch: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    let argument = &input.arguments()[i];
    let row = argument.value_row(ordinal, batch);
    work.step()?;
    if row >= argument.array().len() {
        return Err(internal(
            "extended string selected argument row out of bounds",
        ));
    }
    Ok(row)
}
fn visit_row(
    op: Operation,
    input: &StringCoreInput<'_, '_>,
    ordinal: usize,
    batch: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<Row, KernelFailure> {
    if matches!(op, Operation::Murmur) && input.arguments().is_empty() {
        for byte in 104_729u32.to_le_bytes() {
            emit(byte)?;
            work.step()?;
        }
        return Ok(Row::Value);
    }
    let row = argument_row(input, 0, ordinal, batch, work)?;
    let array = input.arguments()[0].array();
    if array.is_null(row) {
        return Ok(Row::Null);
    }
    match op {
        Operation::Unhex | Operation::ToBinary => {
            let text = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal("binary input is not Utf8"))?
                .value(row);
            let format = if input.arguments().len() == 2 {
                let at = argument_row(input, 1, ordinal, batch, work)?;
                let strings = input.arguments()[1]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("binary format is not Utf8"))?;
                if strings.is_null(at) {
                    None
                } else {
                    Some(strings.value(at))
                }
            } else {
                None
            };
            super::string_binary::visit(text, format, matches!(op, Operation::Unhex), work, emit)
        }
        Operation::RegexpReplace => {
            Err(internal("regexp_replace must use its shared selected core"))
        }
        Operation::RegexpExtract | Operation::RegexpExtractAll => Err(internal(
            "regexp extraction must use its shared selected core",
        )),
        Operation::Murmur => {
            let mut seed = 104_729u32;
            for i in 0..input.arguments().len() {
                let row = argument_row(input, i, ordinal, batch, work)?;
                let array = input.arguments()[i].array();
                if array.is_null(row) {
                    return Ok(Row::Null);
                }
                let Some(next) = super::murmur::hash_array_selected(array, row, seed, work)? else {
                    return Ok(Row::Null);
                };
                seed = next;
            }
            for byte in seed.to_le_bytes() {
                emit(byte)?;
                work.step()?;
            }
            Ok(Row::Value)
        }
    }
}

pub(super) fn evaluate<'a>(
    op: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    validate_profile(op, input.contract(), || work.step())?;
    if input.arguments().len() != input.contract().selected().argument_types.len() {
        return Err(invalid("extended string checked argument count differs"));
    }
    for (argument, ty) in input
        .arguments()
        .iter()
        .zip(&input.contract().selected().argument_types)
    {
        let FunctionArgumentType::Value(ty) = ty else {
            return Err(invalid("extended string requires value arguments"));
        };
        if argument.array().data_type() != ty.data_type() {
            return Err(invalid(
                "extended string actual argument carrier differs from selected contract",
            ));
        }
        for (ordinal, batch) in input.selection().iter().enumerate() {
            let row = argument.value_row(ordinal, batch);
            work.step()?;
            if row >= argument.array().len() {
                return Err(internal(
                    "extended string selected argument row out of bounds",
                ));
            }
            if argument.array().is_null(row) && !ty.nullable() {
                return Err(internal(
                    "extended string non-null argument contains selected SQL NULL",
                ));
            }
        }
        work.step()?;
    }
    work.finish()?;
    evaluate_selected(
        op,
        StringCoreInput {
            arguments: input.arguments(),
            selection: input.selection(),
            error_boundary: None,
        },
        control,
    )
}
/// Fixed-width hashes have no variable-width offsets or sizing pass. The
/// original value author runs once per selected row and emits into a stack cell.
fn evaluate_murmur_selected<'a>(
    input: StringCoreInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let selection = input.selection();
        let mut values = reserve::<i32>(selection.len(), &mut work)?;
        let bitmap_bytes = selection.len().div_ceil(8);
        let mut validity = reserve::<u8>(bitmap_bytes, &mut work)?;
        for _ in 0..bitmap_bytes {
            work.step()?;
            validity.push(0);
        }
        let mut has_null = false;
        for (ordinal, batch) in selection.iter().enumerate() {
            let mut bytes = [0_u8; 4];
            let mut emitted = 0;
            let row = visit_row(
                Operation::Murmur,
                &input,
                ordinal,
                batch,
                &mut work,
                |byte| {
                    let target = bytes
                        .get_mut(emitted)
                        .ok_or_else(|| internal("Murmur3 emitted more than one i32"))?;
                    *target = byte;
                    emitted += 1;
                    Ok(())
                },
            )?;
            match row {
                Row::Value => {
                    if emitted != 4 {
                        return Err(internal("Murmur3 emitted an incomplete i32"));
                    }
                    values.push(i32::from_le_bytes(bytes));
                    validity[ordinal / 8] |= 1 << (ordinal % 8);
                }
                Row::Null => {
                    values.push(0);
                    has_null = true;
                }
            }
            work.step()?;
        }
        work.flush()?;
        let nulls = has_null.then(|| {
            NullBuffer::new(BooleanBuffer::new(
                Buffer::from(validity),
                0,
                selection.len(),
            ))
        });
        let array: ArrayRef = Arc::new(Int32Array::new(ScalarBuffer::from(values), nulls));
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Int32,
            array,
            Box::new([]),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

/// Both v1 full selections and prepared selected owners enter this same computation.
pub fn evaluate_selected<'a>(
    op: StringOperation,
    input: StringCoreInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if matches!(op, Operation::Murmur) {
        return evaluate_murmur_selected(input, control);
    }
    if matches!(op, Operation::RegexpReplace) {
        return super::string_regexp_replace::evaluate_selected(input, control);
    }
    if matches!(op, Operation::RegexpExtract | Operation::RegexpExtractAll) {
        return super::string_regexp_extract::evaluate_selected(
            matches!(op, Operation::RegexpExtractAll),
            input,
            control,
        );
    }
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let target = match op {
        Operation::Unhex | Operation::ToBinary => DataType::Binary,
        Operation::RegexpReplace | Operation::RegexpExtract | Operation::RegexpExtractAll => {
            DataType::Utf8
        }
        Operation::Murmur => DataType::Int32,
    };
    let result = (|| {
        let selection = input.selection();
        let mut total = 0usize;
        for (ordinal, batch) in selection.iter().enumerate() {
            let mut length = 0usize;
            let row = visit_row(op, &input, ordinal, batch, &mut work, |_| {
                length = length
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                Ok(())
            })?;
            match row {
                Row::Value => {
                    total = total
                        .checked_add(length)
                        .ok_or(KernelFailure::ResourceExhausted)?
                }
                Row::Null => {}
            }
            work.step()?;
        }
        i32::try_from(total).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut bytes = reserve::<u8>(total, &mut work)?;
        let mut offsets = reserve::<i32>(
            selection
                .len()
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            &mut work,
        )?;
        let row_errors = reserve::<RowDataError>(0, &mut work)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0);
        let mut has_null = false;
        for (ordinal, batch) in selection.iter().enumerate() {
            let start = bytes.len();
            let row = visit_row(op, &input, ordinal, batch, &mut work, |byte| {
                if bytes.len() >= total {
                    return Err(internal("extended string exceeded measured output extent"));
                }
                bytes.push(byte);
                Ok(())
            })?;
            let valid = matches!(row, Row::Value);
            if !valid {
                bytes.truncate(start);
            }
            validity.append(valid);
            has_null |= !valid;
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal("extended string measured output extent differs"));
        }
        work.flush()?;
        let array: ArrayRef = match op {
            Operation::Murmur => {
                return Err(internal(
                    "fixed-width Murmur3 reached variable-width output",
                ));
            }
            Operation::RegexpReplace | Operation::RegexpExtract | Operation::RegexpExtractAll => {
                Arc::new(StringArray::new(
                    OffsetBuffer::new(offsets.into()),
                    Buffer::from(bytes),
                    has_null.then(|| NullBuffer::new(validity.finish())),
                ))
            }
            _ => Arc::new(BinaryArray::new(
                OffsetBuffer::new(offsets.into()),
                Buffer::from(bytes),
                has_null.then(|| NullBuffer::new(validity.finish())),
            )),
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target,
            array,
            row_errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    match result {
        Ok(values) => {
            work.finish()?;
            Ok(values)
        }
        Err(cause) => Err(cause),
    }
}

/// V1 host adapter: arguments have already been evaluated and expanded.
/// Data errors remain batch failures with the original first-row message.
pub fn evaluate_required(
    op: StringOperation,
    arrays: &[ArrayRef],
    rows: usize,
) -> Result<ArrayRef, KernelFailure> {
    let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let error_boundary = |row: usize, cause: StringDataError<'_>| {
        Err(super::string_failure::required_row(row, cause))
    };
    let values = evaluate_selected(
        op,
        StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(rows),
            error_boundary: Some(&error_boundary),
        },
        &DirectStringControl,
    )?;
    if let Some(error) = values.errors().first() {
        return Err(super::string_failure::required_journal(error));
    }
    Ok(Arc::clone(values.values()))
}
pub(super) struct DirectStringControl;
impl KernelEvaluationControl for DirectStringControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("legacy string computation must not wait"))
    }
}

/// V1 knows the actual Arrow carrier before its residual carrier projection.
pub fn murmur_supported_carrier(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Timestamp(_, None)
            | DataType::FixedSizeBinary(16)
    )
}

#[cfg(test)]
mod shared_legacy_tests {
    use super::*;
    use arrow_array::{Int32Array, StructArray};
    use arrow_schema::Field;

    #[test]
    fn shared_string_legacy_preserves_long_arrow_projection_error() {
        let field = Arc::new(Field::new("long-field-".repeat(128), DataType::Int32, true));
        let values: ArrayRef = Arc::new(StructArray::from(vec![(
            field,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]));
        let options = arrow_cast::CastOptions {
            safe: false,
            format_options: arrow_cast::display::FormatOptions::default(),
        };
        let raw =
            arrow_cast::cast_with_options(values.as_ref(), &DataType::Utf8, &options).unwrap_err();
        let expected = format!("cast to Utf8 failed for murmur_hash3_32: {raw}");
        assert!(
            expected.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES,
            "{expected}"
        );
        let failure = evaluate_required(StringOperation::Murmur, &[values], 1).unwrap_err();
        let source =
            super::super::string_failure::original_source::<arrow_schema::ArrowError>(&failure);
        assert_eq!(
            format!("cast to Utf8 failed for murmur_hash3_32: {source}"),
            expected
        );
        assert!(failure.to_string().len() <= crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    }

    #[test]
    fn shared_string_legacy_masks_unsupported_projection_after_preceding_null() {
        let field = Arc::new(Field::new("invalid-projection", DataType::Int32, true));
        let values: ArrayRef = Arc::new(StructArray::from(vec![(
            field,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]));
        let null: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let output = evaluate_required(StringOperation::Murmur, &[null, values], 1).unwrap();
        assert_eq!(output.null_count(), 1);
    }

    #[test]
    fn shared_string_legacy_unknown_long_binary_format_keeps_hex_default() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["00FF", "xx"]));
        let format = "unsupported-format-".repeat(128);
        let formats: ArrayRef = Arc::new(StringArray::from(vec![format.as_str(), format.as_str()]));
        let output = evaluate_required(StringOperation::ToBinary, &[input, formats], 2).unwrap();
        let binary = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0), [0, 255]);
        assert!(binary.is_null(1));
    }
}

#[cfg(test)]
mod fixed_murmur_output_properties {
    use super::*;
    #[test]
    fn selected_sliced_and_scalar_hashes_keep_exact_i32_values_and_null_coordinates() {
        let base: ArrayRef = Arc::new(StringArray::from(vec![
            Some("outside"),
            Some("é"),
            None,
            Some("skipped"),
            Some("abc"),
            Some(""),
            Some("outside"),
        ]));
        let column = base.slice(1, 5);
        drop(base);
        let scalar: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let weak_column = Arc::downgrade(&column);
        let weak_scalar = Arc::downgrade(&scalar);
        let rows = [0, 1, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let arguments = [
            EvaluatedArgument::Scalar(&scalar),
            EvaluatedArgument::Column(&column),
        ];
        let output = evaluate_selected(
            StringOperation::Murmur,
            StringCoreInput {
                arguments: &arguments,
                selection,
                error_boundary: None,
            },
            &DirectStringControl,
        )
        .unwrap();
        let (_, output, errors) = output.into_parts();
        assert!(errors.is_empty());
        let values = output.as_any().downcast_ref::<Int32Array>().unwrap();
        let seed = murmur_hash3_32(b"x", 104_729);
        let expected: Vec<Option<i32>> = [Some("é"), None, Some("abc"), Some("")]
            .into_iter()
            .map(|value| value.map(|value| murmur_hash3_32(value.as_bytes(), seed) as i32))
            .collect();
        assert_eq!(values.to_data(), Int32Array::from(expected).to_data());
        assert_eq!(values.len(), 4);
        assert_eq!(values.null_count(), 1);
        drop(column);
        drop(scalar);
        assert!(weak_column.upgrade().is_none());
        assert!(weak_scalar.upgrade().is_none());
        assert_eq!(output.len(), 4);
    }
    #[test]
    fn zero_argument_and_empty_hash_outputs_keep_seed_and_declared_fixed_carrier() {
        let output = evaluate_required(StringOperation::Murmur, &[], 3).unwrap();
        assert_eq!(
            output.to_data(),
            Int32Array::from(vec![104_729; 3]).to_data()
        );
        let output = evaluate_required(StringOperation::Murmur, &[], 0).unwrap();
        assert_eq!(output.data_type(), &DataType::Int32);
        assert!(output.is_empty());
    }
}
