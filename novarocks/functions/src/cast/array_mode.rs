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

//! Explicit array conversion within the original prepared CAST recipe.
//! Only conversion-authored row Data enters this boundary's TRY policy.
use super::*;
use crate::{SelectedValues, Selection, selected_copy};
use arrow_array::{ArrayRef, ListArray, new_empty_array, new_null_array};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::ArrayCastFailureMode;
use std::{alloc::Layout, sync::Arc};

fn recipe(
    source: &FunctionValueType,
    result: &FunctionValueType,
    body: CastBody,
    child: Option<Box<PreparedCastRecipe>>,
    policy: DecimalOverflowPolicy,
    allow: bool,
    mode: Option<ArrayCastFailureMode>,
) -> PreparedCastRecipe {
    PreparedCastRecipe {
        operation: CastOperation::Carrier,
        source: source.clone(),
        result: result.clone(),
        body,
        array_child: child,
        decimal_overflow_policy: policy,
        allow_throw_exception: allow,
        array_failure_mode: mode,
    }
}

pub(super) fn prepare(
    source: &FunctionValueType,
    result: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
    mode: ArrayCastFailureMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedCastRecipe, CastPrepareError> {
    work.step()?;
    if !novarocks_type_contract::array_cast_mode_types_valid_observed(source, result, mode, || {
        work.step()
    })? {
        return Err(CastPrepareError::TypeMismatch);
    }
    if source.data_type() == &DataType::Null {
        return Ok(recipe(
            source,
            result,
            CastBody::ArrayNull,
            None,
            policy,
            allow,
            Some(mode),
        ));
    }
    let from = source.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
    let to = result.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
    let child = prepare_element(&from, &to, policy, allow, work)?;
    work.flush()?;
    Ok(recipe(
        source,
        result,
        CastBody::ExplicitArray,
        Some(Box::new(child)),
        policy,
        allow,
        Some(mode),
    ))
}

fn prepare_element(
    source: &FunctionValueType,
    result: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedCastRecipe, CastPrepareError> {
    work.step()?;
    if source.nullable() && !result.nullable() {
        return Err(CastPrepareError::TypeMismatch);
    }
    if source.logical_type() == ValueLogicalType::Physical && source.data_type() == &DataType::Null
    {
        if !result.nullable() {
            return Err(CastPrepareError::TypeMismatch);
        }
        return Ok(recipe(
            source,
            result,
            CastBody::ArrayNull,
            None,
            policy,
            allow,
            None,
        ));
    }
    if matches!(
        (source.data_type(), result.data_type()),
        (DataType::List(_), DataType::List(_))
    ) {
        return prepare(
            source,
            result,
            policy,
            allow,
            ArrayCastFailureMode::ReportError,
            work,
        );
    }
    if source.data_type() == result.data_type()
        && source
            .same_value_domain_observed::<KernelFailure>(result, || {
                work.step().map_err(crate::kernel_control::compile_failure)
            })
            .map_err(CastPrepareError::Kernel)?
    {
        return Ok(recipe(
            source,
            result,
            CastBody::Identity,
            None,
            policy,
            allow,
            None,
        ));
    }
    if let Some(operation) = super::array_json::profile(source, result) {
        return Ok(recipe(
            source,
            result,
            CastBody::ArrayJson(operation),
            None,
            policy,
            allow,
            None,
        ));
    }
    if source.logical_type() != ValueLogicalType::Physical
        || result.logical_type() != ValueLogicalType::Physical
    {
        return Err(CastPrepareError::Unsupported);
    }
    if source.data_type() == &DataType::Utf8 && result.data_type() == &DataType::Int32 {
        return Ok(recipe(
            source,
            result,
            CastBody::ArrayText,
            None,
            policy,
            allow,
            None,
        ));
    }
    let leaf = trino_array_leaf::profile(source.data_type(), result.data_type())
        .ok_or(CastPrepareError::Unsupported)?;
    Ok(recipe(
        source,
        result,
        CastBody::ArrayLeaf(leaf),
        None,
        policy,
        allow,
        None,
    ))
}

fn reserve<T>(len: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

pub(super) fn evaluate<'a>(
    recipe: &PreparedCastRecipe,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'a>,
    inherited: &[RowDataError],
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let outcome = (|| {
        match argument {
            EvaluatedArgument::SelectedColumn(values) => {
                if !values
                    .selection()
                    .same_rows_observed(selection, || work.step())?
                    || values.values().len() != selection.len()
                    || !std::ptr::eq(inherited, values.errors())
                {
                    return Err(invalid(
                        "array CAST requires its exact selected address and inherited journal",
                    ));
                }
            }
            _ => {
                if !inherited.is_empty() {
                    return Err(invalid("array CAST has a foreign inherited journal"));
                }
                argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
            }
        }
        if let Some(value) = argument.constant_value()
            && !value
                .value_type()
                .fits_value_type_observed(&recipe.source, || work.step())?
        {
            return Err(invalid(
                "array CAST constant differs from its frozen complete type",
            ));
        }
        if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
            argument.array().data_type(),
            recipe.source.data_type(),
            || work.step(),
        )? {
            return Err(invalid(
                "array CAST input differs from its frozen Field metadata",
            ));
        }
        match recipe.source.data_type() {
            DataType::List(_) if !argument.array().as_any().is::<ListArray>() => {
                return Err(invalid("array CAST has a foreign concrete List carrier"));
            }
            DataType::Null if !argument.array().as_any().is::<arrow_array::NullArray>() => {
                return Err(invalid("array CAST has a foreign concrete unknown carrier"));
            }
            _ => {}
        }
        selected_copy::guarded_result_extent(recipe.result.data_type(), selection.len())
            .map_err(|e| e.into_kernel())?;
        let mut arrays = reserve::<ArrayRef>(selection.len(), &mut work)?;
        let mut errors = reserve::<RowDataError>(selection.len(), &mut work)?;
        let mut choices = reserve::<(usize, usize)>(selection.len(), &mut work)?;
        let mut journal = inherited.iter().peekable();
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.step()?;
            let output = if journal
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                work.flush()?;
                errors.push(journal.next().expect("checked original journal").clone());
                new_null_array(recipe.result.data_type(), 1)
            } else {
                let row = argument.value_row(ordinal, logical_row);
                if row >= argument.array().len() {
                    return Err(invalid("array CAST address exceeds its carrier"));
                }
                if logical_is_null(argument.array().as_ref(), row, 1, &mut work)? {
                    if !recipe.source.nullable() {
                        return Err(invalid(
                            "non-null array CAST input contains a selected NULL",
                        ));
                    }
                    work.flush()?;
                    new_null_array(recipe.result.data_type(), 1)
                } else if recipe.body == CastBody::Identity {
                    work.flush()?;
                    argument.array().slice(row, 1)
                } else if recipe.body == CastBody::ArrayNull {
                    return Err(invalid(
                        "unknown array CAST input contains a non-NULL value",
                    ));
                } else {
                    let list = argument
                        .array()
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| invalid("array CAST has a foreign concrete List carrier"))?;
                    let offsets = list.value_offsets();
                    let start = usize::try_from(offsets[row])
                        .map_err(|_| invalid("array CAST has a negative offset"))?;
                    let end = usize::try_from(offsets[row + 1])
                        .map_err(|_| invalid("array CAST has a negative offset"))?;
                    if start > end || end > list.values().len() {
                        return Err(invalid("array CAST offsets exceed its child"));
                    }
                    let child = recipe
                        .array_child
                        .as_ref()
                        .ok_or_else(|| internal("array CAST lost its prepared child"))?;
                    work.flush()?;
                    let values = list.values().slice(start, end - start);
                    let converted = child.evaluate_selected(
                        EvaluatedArgument::Column(&values),
                        Selection::all(end - start),
                        &[],
                        control,
                    )?;
                    work.flush()?;
                    if let Some(error) = converted.errors().first() {
                        if recipe.array_failure_mode != Some(ArrayCastFailureMode::OutputNull) {
                            errors.push(error.with_selected_ordinal(ordinal));
                        }
                        new_null_array(recipe.result.data_type(), 1)
                    } else {
                        let DataType::List(field) = recipe.result.data_type() else {
                            return Err(internal("array CAST lost its frozen List target"));
                        };
                        let length = i32::try_from(converted.values().len())
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        work.flush()?;
                        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, length]));
                        Arc::new(
                            ListArray::try_new(
                                Arc::clone(field),
                                offsets,
                                Arc::clone(converted.values()),
                                None,
                            )
                            .map_err(|cause| {
                                crate::control_values::AssemblyFailure::arrow(
                                    "array CAST child assembly",
                                    cause,
                                )
                                .into_kernel()
                            })?,
                        ) as ArrayRef
                    }
                }
            };
            work.flush()?;
            arrays.push(output);
            choices.push((ordinal, 0));
        }
        if journal.next().is_some() {
            return Err(invalid("array CAST journal exceeds its selected domain"));
        }
        let out = if arrays.is_empty() {
            work.flush()?;
            new_empty_array(recipe.result.data_type())
        } else {
            selected_copy::preflight_guarded_interleave(
                recipe.result.data_type(),
                &arrays,
                &choices,
                |boundary| if boundary { work.flush() } else { work.step() },
            )
            .map_err(|e| e.into_kernel())?;
            let mut sources = reserve::<&dyn Array>(arrays.len(), &mut work)?;
            for array in &arrays {
                work.step()?;
                sources.push(array.as_ref());
            }
            work.flush()?;
            arrow_select::interleave::interleave(&sources, &choices).map_err(|cause| {
                crate::control_values::AssemblyFailure::arrow("array CAST assembly", cause)
                    .into_kernel()
            })?
        };
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            recipe.result.data_type(),
            out,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(outcome)
}

#[derive(Debug)]
struct ConversionFailure(trino_array_leaf::LeafError);
impl fmt::Display for ConversionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            trino_array_leaf::LeafError::InvalidCastArgument => {
                "Invalid array element cast argument"
            }
            trino_array_leaf::LeafError::NumericOutOfRange => {
                "Array element cast is outside the numeric range"
            }
            trino_array_leaf::LeafError::TypeMismatch => {
                "Array element differs from its frozen type"
            }
        })
    }
}
impl Error for ConversionFailure {}
impl novarocks_failure_contract::FailureProjection for ConversionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_function_contract::kernel_failure::{
            INVALID_CAST_ARGUMENT, NUMERIC_OUT_OF_RANGE,
        };
        FailureFact::new(
            FailureClass::Data,
            match self.0 {
                trino_array_leaf::LeafError::InvalidCastArgument => INVALID_CAST_ARGUMENT,
                trino_array_leaf::LeafError::NumericOutOfRange => NUMERIC_OUT_OF_RANGE,
                trino_array_leaf::LeafError::TypeMismatch => {
                    unreachable!("contract failure stays on the outer channel")
                }
            },
        )
    }
}

pub(super) fn evaluate_leaf(
    recipe: &PreparedCastRecipe,
    leaf: trino_array_leaf::Leaf,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    use trino_array_leaf::{Input, LeafError, Value};
    let row =
        recipe.checked_row_with_shape(
            argument,
            ordinal,
            logical_row,
            work,
            |array| match recipe.source.data_type() {
                DataType::Int8 => array.as_any().is::<Int8Array>(),
                DataType::Int16 => array.as_any().is::<Int16Array>(),
                DataType::Int32 => array.as_any().is::<Int32Array>(),
                DataType::Int64 => array.as_any().is::<Int64Array>(),
                DataType::Float64 => array.as_any().is::<Float64Array>(),
                DataType::Decimal128(..) => array.as_any().is::<Decimal128Array>(),
                _ => false,
            },
        )?;
    if logical_is_null(argument.array().as_ref(), row, 1, work)? {
        return if recipe.source.nullable() {
            Ok(CastRowResult::Null)
        } else {
            Err(invalid("non-null array CAST leaf contains a selected NULL"))
        };
    }
    let value = match recipe.source.data_type() {
        DataType::Int8 => Input::Signed {
            width: trino_array_leaf::SignedWidth::I8,
            value: i64::from(
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("checked carrier")
                    .value(row),
            ),
        },
        DataType::Int16 => Input::Signed {
            width: trino_array_leaf::SignedWidth::I16,
            value: i64::from(
                argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("checked carrier")
                    .value(row),
            ),
        },
        DataType::Int32 => {
            let value = argument
                .array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("checked carrier")
                .value(row);
            if matches!(leaf, trino_array_leaf::Leaf::SignedToDecimal { .. }) {
                Input::Signed {
                    width: trino_array_leaf::SignedWidth::I32,
                    value: i64::from(value),
                }
            } else {
                Input::Int32(value)
            }
        }
        DataType::Int64 => Input::Signed {
            width: trino_array_leaf::SignedWidth::I64,
            value: argument
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("checked carrier")
                .value(row),
        },
        DataType::Float64 => Input::Float64(
            argument
                .array()
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("checked carrier")
                .value(row),
        ),
        DataType::Decimal128(..) => Input::Decimal128(
            argument
                .array()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("checked carrier")
                .value(row),
        ),
        _ => return Err(invalid("array CAST leaf has a foreign frozen carrier")),
    };
    work.step()?;
    Ok(match leaf.convert(value) {
        Ok(Value::Int64(value)) => CastRowResult::Signed(value),
        Ok(Value::Decimal128(value)) => CastRowResult::Decimal128(value),
        Ok(Value::Float64(value)) => CastRowResult::Float64(value),
        Ok(Value::Float32(value)) => CastRowResult::Float32(value),
        Err(LeafError::TypeMismatch) => {
            return Err(invalid("array CAST leaf differs from its frozen input"));
        }
        Err(cause) => {
            work.flush()?;
            CastRowResult::RowError(RowDataError::from_domain(
                ordinal,
                ConversionFailure(cause),
            )?)
        }
    })
}

/// The same original selected scalar result author owns text and NULL output.
pub(super) fn evaluate_json(
    recipe: &PreparedCastRecipe,
    operation: super::array_json::Operation,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let row = recipe.checked_row_with_shape(argument, ordinal, logical_row, work, |array| {
        array.as_any().is::<StringArray>()
    })?;
    if logical_is_null(argument.array().as_ref(), row, 1, work)? {
        return if recipe.source.nullable() {
            Ok(CastRowResult::Null)
        } else {
            Err(invalid(
                "non-null JSON array CAST input contains a selected SQL NULL",
            ))
        };
    }
    let raw = argument
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("checked JSON text carrier")
        .value(row);
    Ok(
        match super::array_json::convert_non_null_observed(operation, raw, work) {
            Ok(Some(text)) => CastRowResult::Text(text),
            Ok(None) => CastRowResult::Null,
            Err(super::array_json::LeafError::Outer(cause)) => return Err(cause),
            Err(super::array_json::LeafError::Conversion(cause)) => {
                work.flush()?;
                CastRowResult::RowError(RowDataError::from_domain(ordinal, cause)?)
            }
            Err(super::array_json::LeafError::Parse(cause)) => {
                work.flush()?;
                CastRowResult::RowError(RowDataError::from_domain(ordinal, cause)?)
            }
        },
    )
}
