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

use arrow_array::{Array, ArrayRef, Date32Array, Int32Array, Int64Array, StringArray};
use novarocks_type_contract::{FunctionValueType, SemanticParameterValue, TemporalType};
use novarocks_value_arrays::decode::PreparedTemporalArray;

use crate::builtin::temporal_output::TemporalOutputBuilder;
use crate::builtin::value_conversion;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::temporal_cast::{Facts, Input, Output, Pair, Type};
use crate::temporal_value::{LiteralError, TemporalTextFormat, format};
use crate::{EvaluatedArgument, KernelFailure, RowDataError, ScalarCallContract, Selection};

#[derive(Clone, Copy, Debug)]
pub(super) struct PreparedTemporalCast {
    pair: Pair,
    facts: Facts,
    safe: bool,
    lossless: Option<LosslessTemporalWiden>,
}

/// A same-family precision increase changes no temporal value, even when
/// the exact result declaration uses a different Arrow carrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LosslessTemporalWiden {
    source: TemporalType,
    target: TemporalType,
}

impl LosslessTemporalWiden {
    pub(crate) fn bind(source: &FunctionValueType, target: &FunctionValueType) -> Option<Self> {
        let source = source.value_type().temporal()?;
        let target = target.value_type().temporal()?;
        source
            .is_lossless_widening_to(target)
            .then_some(Self { source, target })
    }

    pub(crate) fn evaluate(
        self,
        argument: EvaluatedArgument<'_>,
        selection: Selection<'_>,
        nullable: bool,
        inherited: &[RowDataError],
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        let source = PreparedTemporalArray::bind(self.source, argument.array().as_ref(), nullable)
            .map_err(|_| invalid("temporal cast input differs from its frozen source"))?;
        work.flush()?;
        let mut output = TemporalOutputBuilder::new(Type::Temporal(self.target), selection.len())?;
        work.flush()?;
        let mut inherited = inherited.iter().peekable();
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.step()?;
            if inherited
                .next_if(|error| error.selected_ordinal() == ordinal)
                .is_some()
            {
                output.null();
                continue;
            }
            let row = argument.value_row(ordinal, logical_row);
            let value = source
                .value(row)
                .map_err(|_| invalid("temporal cast encountered an invalid frozen carrier"))?;
            match value {
                Some(value) => output.temporal(self.target, value)?,
                None => output.null(),
            }
        }
        if inherited.next().is_some() {
            return Err(invalid(
                "temporal widening journal exceeds its selected domain",
            ));
        }
        work.flush()?;
        let output = output.finish();
        work.flush()?;
        Ok(output)
    }
}

impl PreparedTemporalCast {
    pub(super) fn new(
        contract: &ScalarCallContract,
        source: &FunctionValueType,
    ) -> Result<Self, KernelFailure> {
        let overload = contract.selected().overload.as_str();
        let (dependencies, safe) = value_conversion::temporal_overload(overload)
            .ok_or_else(|| invalid("temporal conversion has a foreign overload"))?;
        if value_conversion::temporal_cast_overload(source, contract.result_type(), safe)
            != Some(overload)
        {
            return Err(invalid(
                "temporal conversion differs from its selected overload",
            ));
        }
        let pair = Pair::bind(source, contract.result_type())
            .filter(|pair| pair.dependencies == dependencies)
            .ok_or_else(|| invalid("temporal conversion differs from its selected type pair"))?;
        let mut facts = Facts::default();
        for reference in contract.effects().environment.iter().copied() {
            match contract.parameters().require(reference) {
                Ok(SemanticParameterValue::TimeZone(zone)) => facts.zone = Some(*zone),
                Ok(SemanticParameterValue::StatementStartUtc(start)) => facts.start = Some(*start),
                _ => {
                    return Err(invalid(
                        "temporal conversion has a foreign semantic parameter",
                    ));
                }
            }
        }
        let lossless = if overload == value_conversion::TEMPORAL_WIDEN {
            Some(
                LosslessTemporalWiden::bind(source, contract.result_type()).ok_or_else(|| {
                    invalid("temporal widening differs from its selected type pair")
                })?,
            )
        } else {
            None
        };
        Ok(Self {
            pair,
            facts,
            safe,
            lossless,
        })
    }

    pub(super) fn evaluate(
        self,
        argument: EvaluatedArgument<'_>,
        selection: Selection<'_>,
        nullable: bool,
        errors: &mut Vec<RowDataError>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        if let Some(lossless) = self.lossless {
            return lossless.evaluate(argument, selection, nullable, &[], work);
        }
        let source = Source::bind(self.pair.source, argument.array().as_ref(), nullable)?;
        work.flush()?;
        let mut output = TemporalOutputBuilder::new(self.pair.target, selection.len())?;
        work.flush()?;
        for (ordinal, logical_row) in selection.iter().enumerate() {
            work.step()?;
            let row = argument.value_row(ordinal, logical_row);
            let Some(value) = source.value(row)? else {
                output.null();
                continue;
            };
            if let Input::Text(text) = value {
                for _ in text.as_bytes().chunks(32) {
                    work.step()?;
                }
                work.flush()?;
            }
            if self.pair.target == Type::Text {
                let Input::Temporal(value) = value else {
                    return Err(invalid("text conversion has a foreign selected source"));
                };
                let ty = self
                    .pair
                    .source
                    .temporal()
                    .ok_or_else(|| invalid("text source has no temporal declaration"))?;
                let text = format_cast_text(ty, value, work)?;
                output.text(&text)?;
                work.flush()?;
                continue;
            }
            let converted = self.pair.convert(value, self.facts);
            work.step()?;
            match converted {
                Ok(Output::Date(value)) => output.date(value)?,
                Ok(Output::Bigint(value)) => output.bigint(value)?,
                Ok(Output::Temporal(value)) => {
                    let ty = self
                        .pair
                        .target
                        .temporal()
                        .ok_or_else(|| invalid("temporal result has no declaration"))?;
                    output.temporal(ty, value)?;
                }
                Err(error) => {
                    output.null();
                    if !self.safe {
                        work.flush()?;
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(RowDataError::from_domain(ordinal, CastFailure(error))?);
                        work.flush()?;
                    }
                }
            }
        }
        work.flush()?;
        let output = output.finish();
        work.flush()?;
        Ok(output)
    }
}

/// The original dependency-free temporal-to-VARCHAR conversion, available
/// to collection calls which consume individual elements.
#[derive(Clone, Copy, Debug)]
pub(super) struct TemporalTextCast(Pair);

impl TemporalTextCast {
    pub(super) fn bind(source: &FunctionValueType, target: &FunctionValueType) -> Option<Self> {
        let pair = Pair::bind(source, target)?;
        (pair.target == Type::Text && pair.source.temporal().is_some()).then_some(Self(pair))
    }

    pub(super) fn evaluate(
        self,
        array: &dyn Array,
        row: usize,
        nullable: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<String>, KernelFailure> {
        let source = Source::bind(self.0.source, array, nullable)?;
        let Some(Input::Temporal(value)) = source.value(row)? else {
            return Ok(None);
        };
        let ty = self
            .0
            .source
            .temporal()
            .ok_or_else(|| invalid("text source has no temporal declaration"))?;
        format_cast_text(ty, value, work).map(Some)
    }
}

// A single canonical temporal rendering includes fixed-width calendar/time,
// fraction, offset and an installed IANA zone name.
pub(super) const TEMPORAL_CAST_TEXT_SCRATCH: usize = 512;

fn format_cast_text(
    ty: novarocks_type_contract::TemporalType,
    value: novarocks_type_contract::TemporalValue,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, KernelFailure> {
    let mut text = String::new();
    work.flush()?;
    format(&mut text, ty, value, TemporalTextFormat::Cast)
        .map_err(|_| internal("validated temporal value failed text projection"))?;
    Ok(text)
}

enum Source<'a> {
    Date(&'a Date32Array),
    Integer(&'a Int32Array),
    Bigint(&'a Int64Array),
    Text(&'a StringArray),
    Temporal(PreparedTemporalArray<'a>),
}

impl<'a> Source<'a> {
    fn bind(ty: Type, array: &'a dyn Array, nullable: bool) -> Result<Self, KernelFailure> {
        let wrong_type = || invalid("temporal cast input differs from its frozen source");
        Ok(match ty {
            Type::Date => Self::Date(array.as_any().downcast_ref().ok_or_else(wrong_type)?),
            Type::Integer => Self::Integer(array.as_any().downcast_ref().ok_or_else(wrong_type)?),
            Type::Bigint => Self::Bigint(array.as_any().downcast_ref().ok_or_else(wrong_type)?),
            Type::Text => Self::Text(array.as_any().downcast_ref().ok_or_else(wrong_type)?),
            Type::Temporal(_) | Type::LegacyTimestamp => Self::Temporal(
                PreparedTemporalArray::bind(ty.temporal().ok_or_else(wrong_type)?, array, nullable)
                    .map_err(|_| wrong_type())?,
            ),
        })
    }

    fn value(&self, row: usize) -> Result<Option<Input<'a>>, KernelFailure> {
        Ok(match self {
            Self::Date(array) => (!array.is_null(row)).then(|| Input::Date(array.value(row))),
            Self::Integer(array) => {
                (!array.is_null(row)).then(|| Input::Integral(i64::from(array.value(row))))
            }
            Self::Bigint(array) => (!array.is_null(row)).then(|| Input::Integral(array.value(row))),
            Self::Text(array) => (!array.is_null(row)).then(|| Input::Text(array.value(row))),
            Self::Temporal(array) => array
                .value(row)
                .map_err(|_| invalid("temporal cast encountered an invalid frozen carrier"))?
                .map(Input::Temporal),
        })
    }
}

#[derive(Debug)]
pub(super) struct CastFailure(pub(super) LiteralError);

impl std::fmt::Display for CastFailure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "Cannot cast temporal value: {}", self.0)
    }
}

impl std::error::Error for CastFailure {}

impl novarocks_failure_contract::FailureProjection for CastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_function_contract::kernel_failure::{
            INVALID_CAST_ARGUMENT, NUMERIC_OUT_OF_RANGE,
        };
        let reason = match self.0 {
            LiteralError::IntervalRange => NUMERIC_OUT_OF_RANGE,
            _ => INVALID_CAST_ARGUMENT,
        };
        FailureFact::new(FailureClass::Data, reason)
    }
}
