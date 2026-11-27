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
//! Unicode LIKE matching shared by native predicates and the explicit ESCAPE function.
use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use std::{borrow::Cow, convert::Infallible, fmt, str::Chars, sync::Arc};

pub const ESCAPE_FUNCTION_NAME: &str = "__like_with_escape";
pub const ESCAPE_FUNCTION_ID: &str = "builtin.scalar/__like_with_escape/v1";

#[derive(Clone, Copy, Debug)]
pub enum LikeObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum LikeError<E> {
    Operation(crate::KernelFailure),
    Pattern(LikePatternError),
    Host(E),
}

#[derive(Debug)]
pub enum LikePatternError {
    EscapeLength,
    InvalidEscape,
}

impl fmt::Display for LikePatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EscapeLength => "Escape string must be a single character",
            Self::InvalidEscape => {
                "Escape character must be followed by '%', '_' or the escape character itself"
            }
        })
    }
}
impl std::error::Error for LikePatternError {}
impl novarocks_failure_contract::FailureProjection for LikePatternError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Data,
            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LikePatternError {}

fn event<E>(
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
    e: LikeObservation,
) -> Result<(), LikeError<E>> {
    observe(e).map_err(LikeError::Host)
}
fn required<T>(
    run: impl FnOnce(
        &mut dyn FnMut(LikeObservation) -> Result<(), Infallible>,
    ) -> Result<T, LikeError<Infallible>>,
) -> Result<T, crate::KernelFailure> {
    match run(&mut |_| Ok(())) {
        Ok(v) => Ok(v),
        Err(LikeError::Operation(cause)) => Err(cause),
        Err(LikeError::Pattern(cause)) => Err(crate::KernelFailure::from_domain(cause)),
        Err(LikeError::Host(never)) => match never {},
    }
}
pub fn like_match(text: &str, pattern: &str) -> bool {
    required(|observe| like_match_observed(text, pattern, observe)).unwrap()
}
pub fn like_match_observed<E>(
    text: &str,
    pattern: &str,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    match_observed(text, pattern, None, observe)
}

pub(crate) fn like_with_escape_observed<E>(
    text: &str,
    pattern: &str,
    escape: &str,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    match_observed(
        text,
        pattern,
        Some(escape_character(escape).map_err(LikeError::Pattern)?),
        observe,
    )
}

fn escape_character(escape: &str) -> Result<char, LikePatternError> {
    let mut chars = escape.chars();
    chars
        .next()
        .filter(|c| c.len_utf16() == 1 && chars.next().is_none())
        .ok_or(LikePatternError::EscapeLength)
}

#[derive(Clone, Copy)]
enum PatternToken {
    Literal(char),
    AnyOne,
    AnySequence,
}

fn next_pattern_token<E>(
    chars: &mut Chars<'_>,
    escape: Option<char>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<PatternToken>, LikeError<E>> {
    let Some(character) = chars.next() else {
        return Ok(None);
    };
    event(observe, LikeObservation::Step)?;
    let token = if Some(character) == escape {
        let escaped = chars
            .next()
            .filter(|escaped| matches!(*escaped, '%' | '_') || *escaped == character)
            .ok_or(LikeError::Pattern(LikePatternError::InvalidEscape))?;
        event(observe, LikeObservation::Step)?;
        PatternToken::Literal(escaped)
    } else {
        match character {
            '%' => PatternToken::AnySequence,
            '_' => PatternToken::AnyOne,
            _ => PatternToken::Literal(character),
        }
    };
    Ok(Some(token))
}

/// Prove that a LIKE pattern matches exactly one string. A wildcard removes
/// the proof. The executor remains responsible for evaluating the predicate
/// and reporting malformed patterns.
pub fn exact_literal_observed<'a, E>(
    pattern: &'a str,
    escape: Option<&str>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<Cow<'a, str>>, LikeError<E>> {
    let escape = escape
        .map(escape_character)
        .transpose()
        .map_err(LikeError::Pattern)?;
    let mut chars = pattern.chars();
    let mut literal = String::new();
    while let Some(token) = next_pattern_token(&mut chars, escape, observe)? {
        let PatternToken::Literal(value) = token else {
            return Ok(None);
        };
        if escape.is_some() {
            if literal.capacity() - literal.len() < value.len_utf8() {
                event(observe, LikeObservation::OpaqueBoundary)?;
                literal
                    .try_reserve(value.len_utf8())
                    .map_err(|_| LikeError::Operation(crate::KernelFailure::ResourceExhausted))?;
                event(observe, LikeObservation::OpaqueBoundary)?;
            }
            literal.push(value);
        }
    }
    Ok(Some(if escape.is_some() {
        Cow::Owned(literal)
    } else {
        Cow::Borrowed(pattern)
    }))
}

fn parse_pattern<E>(
    pattern: &str,
    escape: Option<char>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Vec<PatternToken>, LikeError<E>> {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(token) = next_pattern_token(&mut chars, escape, observe)? {
        if tokens.len() == tokens.capacity() {
            event(observe, LikeObservation::OpaqueBoundary)?;
            tokens
                .try_reserve(1)
                .map_err(|_| LikeError::Operation(crate::KernelFailure::ResourceExhausted))?;
            event(observe, LikeObservation::OpaqueBoundary)?;
        }
        tokens.push(token);
    }
    Ok(tokens)
}

fn match_observed<E>(
    text: &str,
    pattern: &str,
    escape: Option<char>,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<bool, LikeError<E>> {
    // Validate the whole pattern even when a mismatch at the start of the
    // input would be sufficient to decide the match.
    let tokens = parse_pattern(pattern, escape, observe)?;
    let mut text = text.chars();
    let mut pattern_index = 0;
    let mut wildcard = None;
    loop {
        event(observe, LikeObservation::Step)?;
        match tokens.get(pattern_index).copied() {
            Some(PatternToken::AnySequence) => {
                pattern_index += 1;
                wildcard = Some((pattern_index, text.clone()));
            }
            Some(PatternToken::AnyOne) if text.next().is_some() => pattern_index += 1,
            Some(PatternToken::Literal(literal)) if text.next() == Some(literal) => {
                pattern_index += 1;
            }
            None if text.as_str().is_empty() => return Ok(true),
            _ => {
                // Retry the suffix after the most recent '%' with one more
                // input code point consumed by that wildcard.
                let Some((resume_pattern, resume_text)) = &mut wildcard else {
                    return Ok(false);
                };
                if resume_text.next().is_none() {
                    return Ok(false);
                }
                pattern_index = *resume_pattern;
                text = resume_text.clone();
            }
        }
    }
}
fn row_observed<E>(
    text: &StringArray,
    text_row: usize,
    pattern: &StringArray,
    pattern_row: usize,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<Option<bool>, LikeError<E>> {
    event(observe, LikeObservation::Step)?;
    if text.is_null(text_row) || pattern.is_null(pattern_row) {
        return Ok(None);
    }
    Ok(Some(like_match_observed(
        text.value(text_row),
        pattern.value(pattern_row),
        observe,
    )?))
}
pub fn evaluate_required(
    text: &ArrayRef,
    pattern: &ArrayRef,
) -> Result<ArrayRef, crate::KernelFailure> {
    required(|observe| evaluate_observed(text, pattern, observe))
}
pub fn evaluate_observed<E>(
    text: &ArrayRef,
    pattern: &ArrayRef,
    observe: &mut dyn FnMut(LikeObservation) -> Result<(), E>,
) -> Result<ArrayRef, LikeError<E>> {
    let len = text.len();
    if pattern.len() != len {
        return Err(LikeError::Operation(contract(
            "like: pattern length differs from its input",
        )));
    }
    let text = text
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(LikeError::Operation(contract(
            "like: first argument must be a string array",
        )))?;
    let pattern = pattern
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(LikeError::Operation(contract(
            "like: second argument must be a string array",
        )))?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    let result_values: Vec<Option<bool>> = (0..len)
        .map(|i| row_observed(text, i, pattern, i, observe))
        .collect::<Result<_, _>>()?;
    event(observe, LikeObservation::OpaqueBoundary)?;
    let result = BooleanArray::from_iter(result_values);
    event(observe, LikeObservation::OpaqueBoundary)?;
    Ok(Arc::new(result))
}

/// Exact already-bound Utf8-only original native operator domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNativeLikeRecipe {
    negated: bool,
    text: novarocks_type_contract::FunctionValueType,
    pattern: novarocks_type_contract::FunctionValueType,
    result: novarocks_type_contract::FunctionValueType,
}
impl PreparedNativeLikeRecipe {
    pub fn try_new(
        negated: bool,
        text: &novarocks_type_contract::FunctionValueType,
        pattern: &novarocks_type_contract::FunctionValueType,
        result: &novarocks_type_contract::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, crate::ComparisonPrepareError> {
        use arrow_schema::DataType;
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueLogicalType};
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            for ty in [text, pattern, result] {
                crate::kernel_input::validate_type_observed(ty, &mut work)
                    .map_err(crate::ComparisonPrepareError::Kernel)?;
                work.step()?;
            }
            if [text, pattern].iter().any(|ty| {
                ty.logical_type() != ValueLogicalType::Physical || ty.data_type() != &DataType::Utf8
            }) {
                return Err(crate::ComparisonPrepareError::Unsupported);
            }
            if result.logical_type() != ValueLogicalType::Physical
                || result.data_type() != &DataType::Boolean
                || ((text.nullable() || pattern.nullable()) && !result.nullable())
            {
                return Err(crate::ComparisonPrepareError::TypeMismatch);
            }
            work.flush()?;
            Ok(Self {
                negated,
                text: text.clone(),
                pattern: pattern.clone(),
                result: result.clone(),
            })
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|e: &crate::ComparisonPrepareError| e.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn negated(&self) -> bool {
        self.negated
    }
    pub fn text_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.text
    }
    pub fn pattern_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.pattern
    }
    pub fn result_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.result
    }
    pub fn own_effects(
        &self,
        context: novarocks_type_contract::ExpressionEffectContext,
    ) -> crate::ScopedExpressionEffects {
        crate::ScopedExpressionEffects::primitive(context, self.static_own_effects())
    }

    pub fn static_own_effects(&self) -> novarocks_type_contract::ExpressionEffects {
        novarocks_type_contract::ExpressionEffects::PURE_VALUE
    }
    pub fn evaluate_selected<'a>(
        &self,
        text: crate::EvaluatedArgument<'a>,
        pattern: crate::EvaluatedArgument<'a>,
        selection: crate::Selection<'a>,
        truth_only: bool,
        control: &dyn crate::KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, crate::KernelFailure> {
        use crate::kernel_control::invalid;
        use crate::{EvaluatedArgument, KernelEvaluationControl, KernelFailure};
        let observed = crate::KernelControlObservation::new(control);
        observed.checkpoint(0)?;
        let mut work = crate::EvaluationCheckpoints::new(&observed);
        let outcome = (|| {
            for (arg, expected) in [(text, &self.text), (pattern, &self.pattern)] {
                // Header identity belongs to the checked invocation even when
                // every demanded row carries an inherited error placeholder.
                if arg.array().data_type() != expected.data_type()
                    || !arg.array().as_any().is::<StringArray>()
                {
                    return Err(invalid("LIKE exact Utf8 carrier is absent"));
                }
                work.step()?;
                if let Some(value) = arg.constant_value() {
                    if value.value_type().logical_type() != expected.logical_type()
                        || (value.value_type().nullable() && !expected.nullable())
                    {
                        return Err(invalid("LIKE constant differs from its exact logical type"));
                    }
                    work.step()?;
                }
                if let EvaluatedArgument::SelectedColumn(value) = arg {
                    if !value
                        .selection()
                        .same_rows_observed(selection, || work.step())?
                    {
                        return Err(invalid(
                            "LIKE compact argument has a foreign selected domain",
                        ));
                    }
                } else {
                    arg.validate_shape_observed::<KernelFailure>(selection, || work.step())?;
                }
            }
            if selection.is_empty() {
                crate::kernel_input::validate_argument_observed(
                    text, selection, &self.text, &observed,
                )?;
                crate::kernel_input::validate_argument_observed(
                    pattern,
                    selection,
                    &self.pattern,
                    &observed,
                )?;
            }
            let mut left_errors = match text {
                EvaluatedArgument::SelectedColumn(v) => v.errors(),
                _ => &[],
            }
            .iter()
            .peekable();
            let mut right_errors = match pattern {
                EvaluatedArgument::SelectedColumn(v) => v.errors(),
                _ => &[],
            }
            .iter()
            .peekable();
            let mut errors = Vec::new();
            let mut values = Vec::with_capacity(selection.len());
            work.flush()?;
            for (ordinal, row) in selection.iter().enumerate() {
                work.step()?;
                let left = if left_errors
                    .peek()
                    .is_some_and(|e| e.selected_ordinal() == ordinal)
                {
                    left_errors.next()
                } else {
                    None
                };
                let right = if right_errors
                    .peek()
                    .is_some_and(|e| e.selected_ordinal() == ordinal)
                {
                    right_errors.next()
                } else {
                    None
                };
                if let Some(error) = left.or(right) {
                    errors.push(error.clone());
                    values.push(None);
                    work.step()?;
                    continue;
                }
                for (arg, ty) in [(text, &self.text), (pattern, &self.pattern)] {
                    crate::arithmetic::checked_row(
                        arg,
                        ordinal,
                        row,
                        ty,
                        |array| {
                            if array.as_any().is::<StringArray>() {
                                Ok(())
                            } else {
                                Err(invalid("LIKE exact Utf8 carrier is absent"))
                            }
                        },
                        &mut work,
                    )?;
                }
                let left = text
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("LIKE source carrier differs"))?;
                let right = pattern
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("LIKE pattern carrier differs"))?;
                let value = row_observed(
                    left,
                    text.value_row(ordinal, row),
                    right,
                    pattern.value_row(ordinal, row),
                    &mut |event| match event {
                        LikeObservation::Step => work.step(),
                        LikeObservation::OpaqueBoundary => work.flush(),
                    },
                )
                .map_err(|error| match error {
                    LikeError::Host(cause) => cause,
                    LikeError::Operation(cause) => cause,
                    LikeError::Pattern(cause) => KernelFailure::from_domain(cause),
                })?;
                values.push(value);
                work.step()?;
            }
            work.flush()?;
            let output = BooleanArray::from_iter(values);
            work.flush()?;
            // The original native negated expansion invokes this same Arrow
            // Boolean NOT author after the complete positive LIKE result.
            let output = if self.negated {
                let out = arrow_arith::boolean::not(&output).map_err(arrow_failure)?;
                work.flush()?;
                out
            } else {
                output
            };
            let output = if truth_only {
                let mut failed = errors.iter().peekable();
                let mut rows = Vec::with_capacity(output.len());
                for (ordinal, value) in output.iter().enumerate() {
                    work.step()?;
                    rows.push(
                        if failed
                            .peek()
                            .is_some_and(|error| error.selected_ordinal() == ordinal)
                        {
                            failed.next();
                            None
                        } else {
                            Some(value.unwrap_or(false))
                        },
                    );
                }
                work.flush()?;
                let output = BooleanArray::from_iter(rows);
                work.flush()?;
                output
            } else {
                output
            };
            crate::SelectedValues::try_new_observed(
                selection,
                self.result.data_type(),
                Arc::new(output),
                errors.into_boxed_slice(),
                || work.step(),
            )
        })();
        observed.finish(work.finish_result(outcome))
    }
}
#[cfg(test)]
#[path = "native_like_tests.rs"]
mod tests;

#[derive(Debug)]
enum LikeOperationFailure {
    Contract(&'static str),
    Arrow(arrow_schema::ArrowError),
}
impl std::fmt::Display for LikeOperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(message) => f.write_str(message),
            Self::Arrow(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for LikeOperationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for LikeOperationFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::Contract(_) => (FailureClass::Invariant, 384),
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => (FailureClass::Resource, 386),
            Self::Arrow(_) => (FailureClass::Invariant, 385),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for LikeOperationFailure {}
fn contract(message: &'static str) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(LikeOperationFailure::Contract(message))
}
fn arrow_failure(source: arrow_schema::ArrowError) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(LikeOperationFailure::Arrow(source))
}
