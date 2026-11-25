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

use super::trino::{self, ConversionFailure, Numeric};
use super::*;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::builtin::json_sql_selected::{RowFailure, RowWork, assemble_bytes};
use crate::json_value::{self, JsonError, JsonWork, Kind, Node, Scalar};
use crate::{AggregateStateAllocator, SelectedValues, Selection};
use arrow_array::LargeBinaryArray;
use novarocks_type_contract::{
    CharacterKind, TemporalPrecision, TemporalType, TemporalValue, ValueKind,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Operation {
    To,
    From(Target),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Target {
    Varchar(Option<u32>),
    Numeric(Numeric),
    Boolean,
    Date,
    Time(TemporalPrecision),
}

pub(super) fn bind(
    source: &FunctionValueType,
    result: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Operation>, CastPrepareError> {
    if result.logical_type() == ValueLogicalType::Json
        && super::json_input::supports_cast(source, work)?
    {
        return Ok(Some(Operation::To));
    }
    if source.logical_type() != ValueLogicalType::Json {
        return Ok(None);
    }
    let kind = result.value_type().kind();
    let target = match kind {
        ValueKind::String => Target::Varchar(None),
        ValueKind::BoundedCharacter(ty) if ty.kind() == CharacterKind::Varchar => {
            Target::Varchar(Some(ty.length()))
        }
        ValueKind::Scalar(DataType::Boolean) => Target::Boolean,
        ValueKind::Scalar(DataType::Date32) => Target::Date,
        ValueKind::Temporal(TemporalType::Time(precision)) => Target::Time(*precision),
        _ => {
            let Some(ty) = Numeric::from_kind(kind) else {
                return Ok(None);
            };
            Target::Numeric(ty)
        }
    };
    Ok(Some(Operation::From(target)))
}

impl Operation {
    pub(super) fn may_raise(self, source: &FunctionValueType) -> bool {
        matches!(self, Self::From(_))
            || matches!(
                source.value_type().kind(),
                ValueKind::List(_) | ValueKind::Map { .. } | ValueKind::Struct(_)
            )
    }
}

fn checked_json<'a>(
    array: &'a dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Kind<'a>, KernelFailure> {
    let array = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid("JSON CAST source differs from the frozen LargeBinary carrier"))?;
    let node = Node::from_root(array.value(row)).map_err(|cause| internal(&cause.to_string()))?;
    node.kind_observed(|| work.step())
        .map_err(|cause| match cause {
            json_value::ObservedError::Value(cause) => internal(&cause.to_string()),
            json_value::ObservedError::Control(cause) => cause,
        })
}

fn failed_cast() -> RowFailure {
    RowFailure::Data {
        reason: novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
        message: "Cannot cast JSON to the requested type",
    }
}

fn encode_failure(cause: JsonError) -> RowFailure {
    match cause {
        JsonError::Host(cause) => RowFailure::Outer(cause),
        JsonError::Depth => failed_cast(),
        cause => RowFailure::Outer(internal(&cause.to_string())),
    }
}

pub(super) fn evaluate_variable<'a>(
    recipe: &PreparedCastRecipe,
    operation: Operation,
    argument: EvaluatedArgument<'_>,
    selection: Selection<'a>,
    inherited: &[RowDataError],
    control: &dyn KernelEvaluationControl,
    host: Option<&Arc<dyn AggregateStateAllocator>>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if let EvaluatedArgument::SelectedColumn(values) = argument {
            if !values
                .selection()
                .same_rows_observed(selection, || work.step())?
                || values.values().len() != selection.len()
                || !std::ptr::eq(inherited, values.errors())
            {
                return Err(invalid(
                    "JSON CAST requires its exact selected address and inherited journal",
                ));
            }
        } else {
            if !inherited.is_empty() {
                return Err(invalid("JSON CAST has a foreign inherited journal"));
            }
            argument.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
        }
        let host = host.ok_or_else(|| {
            invalid("JSON CAST requires its original expression allocation scope")
        })?;
        work.flush()?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(host))?;
        let ty = recipe.result_type().data_type();
        let suppress = recipe.cast_mode() == Some(TrinoCastMode::Try);
        let evaluate = |ordinal, logical_row, work: &mut RowWork<'_, '_>| {
            variable_row(recipe, operation, argument, ordinal, logical_row, work)
        };
        match operation {
            Operation::To => assemble_bytes::<i64>(
                ty, selection, inherited, suppress, &allocator, host, &mut work, evaluate,
            ),
            Operation::From(Target::Varchar(_)) => assemble_bytes::<i32>(
                ty, selection, inherited, suppress, &allocator, host, &mut work, evaluate,
            ),
            _ => Err(internal("JSON CAST has an unexpected variable output type")),
        }
    })();
    work.finish_result(result)
}

fn variable_row(
    recipe: &PreparedCastRecipe,
    operation: Operation,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut RowWork<'_, '_>,
) -> Result<Option<Vec<u8>>, RowFailure> {
    let row = recipe.checked_row_with_shape(argument, ordinal, logical_row, work.work, |_| true)?;
    let input = argument.array().as_ref();
    if logical_is_null(input, row, 1, work.work)? {
        return if recipe.source.nullable() {
            Ok(None)
        } else {
            Err(invalid("non-null JSON CAST input contains an SQL NULL").into())
        };
    }
    match operation {
        Operation::To => super::json_input::encode_selected(input, &recipe.source, row, work)
            .map(Some)
            .map_err(encode_failure),
        Operation::From(Target::Varchar(max)) => {
            let value = match checked_json(input, row, work.work)? {
                Kind::Null => return Ok(None),
                Kind::Scalar(value) => value,
                _ => return Err(failed_cast()),
            };
            let mut scratch = json_value::FixedText::<256>::new();
            let text = if let Scalar::String(text) = value {
                text
            } else {
                // The installed SQL floating CAST author uses a bounded rendering workspace.
                let _scratch_reservation = if matches!(value, Scalar::Real(_) | Scalar::Double(_)) {
                    Some(work.reserve(super::varchar_row_temporary_extent(argument.array(), row)?)?)
                } else {
                    None
                };
                json_value::write_scalar_cast(&mut scratch, value, work.work)
                    .map_err(encode_failure)?;
                scratch.as_str()
            };
            if let Some(limit) = max {
                for (ordinal, _) in text.chars().enumerate() {
                    work.step()?;
                    if ordinal >= limit as usize {
                        return Err(failed_cast());
                    }
                }
            }
            work.reserve_output(text.len())?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(text.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for chunk in text.as_bytes().chunks(256) {
                work.step()?;
                bytes.extend_from_slice(chunk);
            }
            Ok(Some(bytes))
        }
        _ => Err(internal("JSON CAST lost its variable result recipe").into()),
    }
}

pub(super) fn evaluate_fixed(
    recipe: &PreparedCastRecipe,
    target: Target,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let row = recipe.checked_row_with_shape(argument, ordinal, logical_row, work, |array| {
        array.as_any().is::<LargeBinaryArray>()
    })?;
    if logical_is_null(argument.array().as_ref(), row, 1, work)? {
        return if recipe.source.nullable() {
            Ok(CastRowResult::Null)
        } else {
            Err(invalid("non-null JSON CAST input contains an SQL NULL"))
        };
    }
    let result = match checked_json(argument.array().as_ref(), row, work)? {
        Kind::Null => Ok(CastRowResult::Null),
        Kind::Scalar(value) => fixed_value(value, target, work),
        _ => Err(ConversionFailure::Invalid),
    };
    match result {
        Ok(value) => Ok(value),
        Err(ConversionFailure::Outer(error)) => Err(error),
        Err(error) => Ok(CastRowResult::RowError(RowDataError::from_domain(
            ordinal, error,
        )?)),
    }
}

fn fixed_value(
    value: Scalar<'_>,
    target: Target,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, ConversionFailure> {
    use crate::temporal_value::{parse_zoned_time_cast, round_time};
    use CastRowResult as R;
    match target {
        Target::Numeric(numeric) => trino::json_number_cast(value, numeric, work),
        Target::Boolean => trino::json_boolean_cast(value, work).map(R::Boolean),
        Target::Date => match value {
            Scalar::Date(days) => Ok(R::Signed(i64::from(days))),
            Scalar::String(text) => {
                crate::datetime_value::parse_trino_date32_observed(text, |_| work.step())?
                    .map(|days| R::Signed(i64::from(days)))
                    .ok_or(ConversionFailure::Invalid)
            }
            _ => Err(ConversionFailure::Invalid),
        },
        Target::Time(precision) => {
            let value = match value {
                Scalar::Temporal {
                    value: TemporalValue::Time(picos),
                    ..
                } => round_time(picos, precision).map_err(|_| ConversionFailure::Invalid)?,
                Scalar::String(text) => {
                    for _ in text.as_bytes().chunks(256) {
                        work.step()?;
                    }
                    let (picos, zone) = parse_zoned_time_cast(text, precision)
                        .map_err(|_| ConversionFailure::Invalid)?;
                    if zone.is_some() {
                        return Err(ConversionFailure::Invalid);
                    }
                    picos
                }
                _ => return Err(ConversionFailure::Invalid),
            };
            Ok(R::Signed(value))
        }
        Target::Varchar(_) => Err(internal("JSON CAST has a fixed VARCHAR recipe").into()),
    }
}

#[cfg(test)]
mod tests;
