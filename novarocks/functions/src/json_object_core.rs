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

//! Original JSONObject computation over evaluated, independently addressed
//! arguments. Callers own exact type validation and any allocation authority.

use crate::Selection;
use crate::aggregate_scalar::{
    AggScalarValue, ScalarReadFailure, ScalarReadFailureSink, ScalarReadObservation,
    scalar_from_array_with_failure,
};
use crate::aggregate_scalar_debug::{ScalarDebugFailure, ScalarDebugObserver, ScalarDebugRoot};
use crate::decimal256_digits::fixed_digits;
use crate::json_object_resources::JsonObjectArguments;
use crate::parse_json_core::{JsonRenderMessages, ParseJsonFailure, ParseJsonObserver};
use arrow_array::{ArrayRef, builder::StringBuilder};
use std::{convert::Infallible, sync::Arc};

const MESSAGES: JsonRenderMessages = JsonRenderMessages {
    stringify: "json_object stringify failed: ",
    stringify_key: "json_object stringify key failed: ",
    missing_key: "json_object missing object key",
};

#[derive(Debug)]
pub enum JsonObjectFailure<E> {
    OriginalData(JsonObjectValueFailure),
    Outer(crate::KernelFailure),
    Control(E),
}
use JsonObjectFailure::{Control, OriginalData, Outer};
#[derive(Debug)]
pub enum JsonObjectValueFailure {
    Date32 { days: i32 },
    ComplexKey,
}
impl JsonObjectValueFailure {
    pub const fn reason(&self) -> crate::DataFailureReason {
        match self {
            Self::Date32 { .. } => crate::DataFailureReason::NumericOutOfRange,
            Self::ComplexKey => crate::DataFailureReason::InvalidValue,
        }
    }
}
impl std::fmt::Display for JsonObjectValueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Date32 { days } => write!(f, "json_object invalid date32 day value: {days}"),
            Self::ComplexKey => f.write_str("json_object key does not support complex type"),
        }
    }
}
impl std::error::Error for JsonObjectValueFailure {}
impl novarocks_failure_contract::FailureProjection for JsonObjectValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.reason())
    }
}
#[derive(Debug)]
enum JsonObjectContractFailure {
    Arity,
    Quote(serde_json::Error),
}
impl std::fmt::Display for JsonObjectContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arity => f.write_str("json_object expects at least 1 argument"),
            Self::Quote(source) => write!(f, "json_object string serialization failed: {source}"),
        }
    }
}
impl std::error::Error for JsonObjectContractFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Quote(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for JsonObjectContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::Arity => 302,
            Self::Quote(_) => 304,
        };
        // The destination is a String and the serialized input is an &str.
        // Its serializer cannot report a user-value or external IO failure.
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for JsonObjectContractFailure {}
impl<E: std::fmt::Display> std::fmt::Display for JsonObjectFailure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OriginalData(source) => source.fmt(f),
            Self::Outer(source) => source.fmt(f),
            Self::Control(source) => source.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for JsonObjectFailure<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OriginalData(source) => Some(source),
            Self::Outer(source) => Some(source),
            Self::Control(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for JsonObjectFailure<Infallible> {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::OriginalData(source) => source.failure_fact(),
            Self::Outer(source) => source.failure_fact(),
            Self::Control(never) => match *never {},
        }
    }
}

pub trait JsonObjectPort {
    type Control;
    fn step(&mut self) -> Result<(), Self::Control>;
    fn boundary(&mut self) -> Result<(), Self::Control>;
    fn read_scalar(
        &mut self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<Option<AggScalarValue>, JsonObjectFailure<Self::Control>>;
    fn debug_formatting_failure(&mut self) -> Self::Control;
}
struct Observer<'a, P>(&'a mut P);
impl<P: JsonObjectPort> ParseJsonObserver for Observer<'_, P> {
    type Error = P::Control;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step()
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        self.0.boundary()
    }
}
impl<P: JsonObjectPort> ScalarDebugObserver for Observer<'_, P> {
    type Error = P::Control;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step()
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        self.0.boundary()
    }
}

fn format_date32<E>(days: i32) -> Result<String, JsonObjectFailure<E>> {
    let naive = crate::datetime_value::date32_to_naive(days)
        .ok_or(OriginalData(JsonObjectValueFailure::Date32 { days }))?;
    Ok(naive.format("%Y-%m-%d").to_string())
}
fn decimal256<P: JsonObjectPort>(
    value: &arrow_buffer::i256,
    port: &mut P,
) -> Result<String, JsonObjectFailure<P::Control>> {
    let digits = fixed_digits(value.to_le_bytes(), |_| port.step()).map_err(Control)?;
    port.boundary().map_err(Control)?;
    let mut out = String::new();
    if digits.negative() {
        out.push('-');
    }
    // One bulk digits append is the fixed original author's destination bound.
    out.push_str(digits.as_str());
    port.boundary().map_err(Control)?;
    Ok(out)
}
fn key_from_scalar<P: JsonObjectPort>(
    value: &AggScalarValue,
    port: &mut P,
) -> Result<String, JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    use AggScalarValue::*;
    let text = match value {
        Bool(v) => if *v { "true" } else { "false" }.to_string(),
        Int64(v) => v.to_string(),
        Float64(v) => v.to_string(),
        Utf8(v) => v.clone(),
        Date32(v) => format_date32(*v)?,
        Timestamp(v) => v.to_string(),
        Temporal(..) => return Err(Outer(crate::aggregate_scalar::temporal_scalar_contract())),
        Decimal128(v) => v.to_string(),
        Decimal256(v) => decimal256(v, port)?,
        Binary(v) => hex::encode(v),
        Struct(_) | Map(_) | List(_) => {
            // The generic invocation admits these scalar values. This owner
            // rejects them as JSON object keys, independently of Arrow shape.
            return Err(OriginalData(JsonObjectValueFailure::ComplexKey));
        }
    };
    port.boundary().map_err(Control)?;
    Ok(text)
}
fn quote<P: JsonObjectPort>(
    text: &str,
    port: &mut P,
) -> Result<String, JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    let out = serde_json::to_string(text).map_err(|source| {
        Outer(crate::KernelFailure::from_domain(
            JsonObjectContractFailure::Quote(source),
        ))
    })?;
    port.boundary().map_err(Control)?;
    Ok(out)
}
fn normalize<P: JsonObjectPort>(
    raw: &str,
    port: &mut P,
) -> Result<Option<String>, JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    // JSONObject's original Err branch drops the serde error directly. The
    // PARSE_JSON invalid-input String projection is not part of this operation.
    let value = match crate::parse_json_core::parse_value(raw) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    port.boundary().map_err(Control)?;
    let mut out = String::new();
    match crate::parse_json_core::format_json_value(
        &value,
        &mut out,
        &mut Observer(port),
        &MESSAGES,
    ) {
        Ok(()) => Ok(Some(out)),
        Err(ParseJsonFailure::Data(_)) => Ok(None),
        Err(ParseJsonFailure::Control(cause)) => Err(Control(cause)),
    }
}
fn debug<P: JsonObjectPort>(
    root: ScalarDebugRoot<'_>,
    port: &mut P,
) -> Result<String, JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    let mut out = String::new();
    match crate::aggregate_scalar_debug::write_observed(root, &mut out, &mut Observer(port)) {
        Ok(()) => {}
        Err(ScalarDebugFailure::Control(cause)) => return Err(Control(cause)),
        Err(ScalarDebugFailure::Formatting) => {
            return Err(Control(port.debug_formatting_failure()));
        }
    }
    port.boundary().map_err(Control)?;
    Ok(out)
}
fn value_from_scalar<P: JsonObjectPort>(
    value: &AggScalarValue,
    port: &mut P,
) -> Result<String, JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    use AggScalarValue::*;
    let text = match value {
        Bool(v) => if *v { "true" } else { "false" }.to_string(),
        Int64(v) => v.to_string(),
        Float64(v) => {
            if v.is_finite() {
                v.to_string()
            } else {
                "null".to_string()
            }
        }
        Utf8(v) => match normalize(v, port)? {
            Some(v) => v,
            None => quote(v, port)?,
        },
        Date32(v) => quote(&format_date32(*v)?, port)?,
        Timestamp(v) => quote(&v.to_string(), port)?,
        Temporal(..) => return Err(Outer(crate::aggregate_scalar::temporal_scalar_contract())),
        Decimal128(v) => v.to_string(),
        Decimal256(v) => decimal256(v, port)?,
        Binary(v) => quote(&hex::encode(v), port)?,
        Struct(v) | List(v) => quote(&debug(ScalarDebugRoot::Sequence(v), port)?, port)?,
        Map(v) => quote(&debug(ScalarDebugRoot::Pairs(v), port)?, port)?,
    };
    port.boundary().map_err(Control)?;
    Ok(text)
}
fn append<P: JsonObjectPort>(
    out: &mut String,
    text: &str,
    port: &mut P,
) -> Result<(), JsonObjectFailure<P::Control>> {
    port.boundary().map_err(Control)?;
    out.push_str(text);
    port.boundary().map_err(Control)
}

pub fn evaluate_selected<A: JsonObjectArguments + ?Sized, P: JsonObjectPort>(
    arguments: &A,
    selection: Selection<'_>,
    port: &mut P,
) -> Result<ArrayRef, JsonObjectFailure<P::Control>> {
    if arguments.is_empty() {
        return Err(Outer(crate::KernelFailure::from_domain(
            JsonObjectContractFailure::Arity,
        )));
    }
    port.boundary().map_err(Control)?;
    let mut builder = StringBuilder::new();
    port.boundary().map_err(Control)?;
    for (ordinal, batch_row) in selection.iter().enumerate() {
        port.step().map_err(Control)?;
        port.boundary().map_err(Control)?;
        let mut object = String::from("{");
        let mut first_pair = true;
        let mut key_idx = 0usize;
        let mut row_is_null = false;
        while key_idx < arguments.len() {
            port.step().map_err(Control)?;
            let key_value = port.read_scalar(
                arguments.array(key_idx),
                arguments.value_row(key_idx, ordinal, batch_row),
            )?;
            let Some(key_scalar) = key_value.as_ref() else {
                row_is_null = true;
                break;
            };
            let key = key_from_scalar(key_scalar, port)?;
            if !first_pair {
                append(&mut object, ",", port)?;
            }
            first_pair = false;
            append(&mut object, &quote(&key, port)?, port)?;
            append(&mut object, ": ", port)?;
            let value_idx = key_idx + 1;
            if value_idx >= arguments.len() {
                append(&mut object, "null", port)?;
            } else {
                let value = port.read_scalar(
                    arguments.array(value_idx),
                    arguments.value_row(value_idx, ordinal, batch_row),
                )?;
                if let Some(value_scalar) = value.as_ref() {
                    append(&mut object, &value_from_scalar(value_scalar, port)?, port)?;
                } else {
                    append(&mut object, "null", port)?;
                }
            }
            key_idx += 2;
        }
        port.boundary().map_err(Control)?;
        if row_is_null {
            builder.append_null();
        } else {
            append(&mut object, "}", port)?;
            builder.append_value(object);
        }
        port.boundary().map_err(Control)?;
    }
    port.boundary().map_err(Control)?;
    let out = Arc::new(builder.finish()) as ArrayRef;
    // Drop the builder's reset-offset allocation before any caller custody.
    drop(builder);
    port.boundary().map_err(Control)?;
    Ok(out)
}

struct OriginalPort;
impl ScalarReadFailureSink for OriginalPort {
    type Error = JsonObjectFailure<Infallible>;
    fn read_contract(&mut self, failure: ScalarReadFailure<'_>) -> Self::Error {
        Outer(failure.into_kernel())
    }
    fn observe(&mut self, _: ScalarReadObservation) -> Result<(), Self::Error> {
        Ok(())
    }
    fn reserve_scalar_copy(&mut self, _: usize, _: usize, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl JsonObjectPort for OriginalPort {
    type Control = Infallible;
    fn step(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn boundary(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn read_scalar(
        &mut self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<Option<AggScalarValue>, JsonObjectFailure<Infallible>> {
        scalar_from_array_with_failure(array, row, self)
    }
    fn debug_formatting_failure(&mut self) -> Infallible {
        panic!("json_object original Debug destination failed")
    }
}

pub fn evaluate_original(
    arguments: &[ArrayRef],
    selection: Selection<'_>,
) -> Result<ArrayRef, JsonObjectFailure<Infallible>> {
    evaluate_selected(arguments, selection, &mut OriginalPort)
}

#[cfg(test)]
#[path = "json_object_core_tests.rs"]
mod tests;
