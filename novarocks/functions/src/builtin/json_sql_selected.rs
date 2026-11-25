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

//! Selected SQL JSON operations with original host custody.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, GenericBinaryArray, GenericStringArray, LargeBinaryArray,
    OffsetSizeTrait, StringArray,
};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_failure_contract::FailureReason;
use novarocks_function_contract::kernel_failure::{
    INVALID_FUNCTION_ARGUMENT, JSON_INPUT_CONVERSION_ERROR,
};
use novarocks_type_contract::{CharacterKind, ValueKind};

use super::json_sql::{JsonEncoding, JsonFunction, read_scalar};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::arrow_result_custody::retain_result_backing_direct;
use crate::json_value::{self, Input, JsonError, JsonWork, Kind, Node, Scalar};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{
    AggregateStateAllocator, DataFailureReason, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallInput, SelectedValues,
};

pub(crate) struct RowWork<'a, 'c> {
    pub(crate) work: &'a mut EvaluationCheckpoints<'c>,
    charge: &'a OpaqueRetainedCharge,
    output: Option<OpaqueReservation>,
}

impl JsonWork for RowWork<'_, '_> {
    type Reservation = OpaqueReservation;

    fn step(&mut self) -> Result<(), KernelFailure> {
        self.work.step()
    }

    fn reserve(&mut self, bytes: usize) -> Result<Self::Reservation, KernelFailure> {
        self.work.flush()?;
        self.charge.reserve_operation(bytes)
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        if self.output.is_some() {
            return Err(invalid(
                "SQL JSON row still owns a previous output reservation",
            ));
        }
        let reservation = self.reserve(bytes)?;
        self.output = Some(reservation);
        Ok(())
    }
}

fn reserve_vec<T>(len: usize) -> Result<Vec<T>, KernelFailure> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(values)
}

fn size<T>(len: usize) -> Result<usize, KernelFailure> {
    len.checked_mul(size_of::<T>())
        .ok_or(KernelFailure::ResourceExhausted)
}

fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}

pub(super) fn evaluate<'a>(
    function: JsonFunction,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let ty = input.contract().result_type().data_type();
    let result = match ty {
        DataType::LargeBinary => evaluate_bytes::<i64>(function, input, allocator, host, &mut work),
        DataType::Utf8 | DataType::Binary => {
            evaluate_bytes::<i32>(function, input, allocator, host, &mut work)
        }
        _ => Err(invalid(
            "SQL JSON function has an unexpected result carrier",
        )),
    };
    work.finish_result(result)
}

fn evaluate_bytes<'a, O: OffsetSizeTrait>(
    function: JsonFunction,
    input: ScalarCallInput<'_, 'a>,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    assemble_bytes::<O>(
        input.contract().result_type().data_type(),
        input.selection(),
        &[],
        function.null_on_error(),
        allocator,
        host,
        work,
        |ordinal, batch_row, row| evaluate_row(function, input, ordinal, batch_row, row),
    )
}

pub(crate) fn assemble_bytes<'a, O: OffsetSizeTrait>(
    ty: &DataType,
    selection: crate::Selection<'a>,
    inherited: &[RowDataError],
    suppress_own_data: bool,
    allocator: &HostAggregateAllocator,
    host: &Arc<dyn AggregateStateAllocator>,
    work: &mut EvaluationCheckpoints<'_>,
    mut evaluate: impl FnMut(usize, usize, &mut RowWork<'_, '_>) -> Result<Option<Vec<u8>>, RowFailure>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let offset_count = add(selection.len(), 1)?;
    let mut charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    let mut journal_charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
    work.flush()?;
    let mut base =
        charge.reserve_operation(crate::scalar_output_resources::flat_arrow_output_peak(
            size::<O>(offset_count)?,
            selection.len(),
        )?)?;
    let mut offsets = reserve_vec::<O>(offset_count)?;
    let mut validity = BooleanBufferBuilder::new(selection.len());
    let mut payload = Vec::new();
    let mut errors = Vec::new();
    let mut diagnostic_bytes = 0usize;
    let mut null_count = 0;
    offsets.push(O::usize_as(0));

    let mut inherited = inherited.iter().peekable();
    for (ordinal, batch_row) in selection.iter().enumerate() {
        let mut row = RowWork {
            work,
            charge: &charge,
            output: None,
        };
        row.step()?;
        let inherited_error = if inherited
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            inherited.next()
        } else {
            None
        };
        let mut own_error = None;
        let result = if inherited_error.is_some() {
            None
        } else {
            match evaluate(ordinal, batch_row, &mut row) {
                Ok(value) => value,
                Err(RowFailure::Outer(cause)) => return Err(cause),
                Err(RowFailure::Data { .. }) if suppress_own_data => None,
                Err(RowFailure::Data { reason, message }) => {
                    own_error = Some((reason, message));
                    None
                }
            }
        };
        let message_bytes = inherited_error
            .map(|error| error.message().len())
            .or_else(|| own_error.map(|(_, message)| message.len()));
        if let Some(message_bytes) = message_bytes {
            let message_bytes = message_bytes.min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
            let next_capacity = if errors.len() == errors.capacity() {
                errors
                    .capacity()
                    .checked_mul(2)
                    .ok_or(KernelFailure::ResourceExhausted)?
                    .max(1)
            } else {
                errors.capacity()
            };
            let growing = next_capacity > errors.capacity();
            // The previous stock stays charged while the new journal or
            // diagnostic is being allocated.
            let request = if growing {
                add(size::<RowDataError>(next_capacity)?, message_bytes)?
            } else {
                message_bytes
            };
            row.work.flush()?;
            let mut grant = journal_charge.reserve_operation(request)?;
            if growing {
                errors
                    .try_reserve_exact(next_capacity - errors.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
            }
            if let Some(error) = inherited_error {
                errors.push(error.clone());
            }
            if let Some((reason, message)) = own_error {
                errors.push(RowDataError::with_reason(
                    ordinal,
                    DataFailureReason::Domain(reason),
                    message,
                ));
            }
            diagnostic_bytes = add(diagnostic_bytes, message_bytes)?;
            let stock = add(size::<RowDataError>(errors.capacity())?, diagnostic_bytes)?;
            journal_charge.reconcile_under_reservation(stock, &mut grant)?;
        }
        let output_charge = row.output.take();
        if let Some(bytes) = result {
            if ty == &DataType::Utf8 {
                match json_value::validate_utf8(&bytes, &mut || work.step()) {
                    Ok(()) => {}
                    Err(json_value::ObservedError::Control(cause)) => return Err(cause),
                    Err(json_value::ObservedError::Value(_)) => {
                        return Err(internal("SQL JSON renderer returned invalid UTF-8"));
                    }
                }
            }
            let next = add(payload.len(), bytes.len())?;
            O::from_usize(next).ok_or(KernelFailure::ResourceExhausted)?;
            if next > payload.capacity() {
                let target = payload.capacity().checked_mul(2).unwrap_or(next).max(next);
                work.flush()?;
                let mut growth = charge.reserve_operation(target)?;
                payload
                    .try_reserve_exact(target - payload.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                charge.reconcile_under_reservation(payload.capacity(), &mut growth)?;
            }
            for chunk in bytes.chunks(1024) {
                work.step()?;
                payload.extend_from_slice(chunk);
            }
            validity.append(true);
        } else {
            null_count += 1;
            validity.append(false);
        }
        drop(output_charge);
        offsets.push(O::from_usize(payload.len()).ok_or(KernelFailure::ResourceExhausted)?);
    }

    if inherited.next().is_some() {
        return Err(invalid("SQL JSON row journal exceeds the selected input"));
    }
    work.flush()?;
    // SAFETY: offsets begin at zero and each selected row appends a
    // representable payload length. Lengths never decrease.
    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
    let payload = Buffer::from(payload);
    // SAFETY: the assembly increments null_count exactly when appending false.
    let nulls = (null_count != 0)
        .then(|| unsafe { NullBuffer::new_unchecked(validity.finish(), null_count) });
    let output: ArrayRef = if ty == &DataType::Utf8 {
        // SAFETY: each row starts at the previous checked payload extent,
        // contains validated UTF-8, and appended one matching validity bit.
        Arc::new(unsafe { GenericStringArray::<O>::new_unchecked(offsets, payload, nulls) })
    } else {
        // SAFETY: each row appends one checked extent and validity bit.
        Arc::new(unsafe { GenericBinaryArray::<O>::new_unchecked(offsets, payload, nulls) })
    };
    let output = retain_result_backing_direct(output, charge, &mut base, allocator.clone(), work)?;
    let _compact_grant = if errors.len() != errors.capacity() {
        Some(journal_charge.reserve_operation(size::<RowDataError>(errors.len())?)?)
    } else {
        None
    };
    SelectedValues::try_new_observed(
        selection,
        ty,
        output.values,
        errors.into_boxed_slice(),
        || work.step(),
    )
}

pub(crate) enum RowFailure {
    Data {
        reason: FailureReason,
        message: &'static str,
    },
    Outer(KernelFailure),
}

impl From<KernelFailure> for RowFailure {
    fn from(value: KernelFailure) -> Self {
        Self::Outer(value)
    }
}

impl From<json_value::JsonValueError> for RowFailure {
    fn from(value: json_value::JsonValueError) -> Self {
        Self::Outer(KernelFailure::from_domain(value))
    }
}

fn item_failure(error: JsonError, reason: FailureReason) -> RowFailure {
    match error {
        JsonError::Host(cause) => RowFailure::Outer(cause),
        JsonError::InvalidText(_) => RowFailure::Data {
            reason,
            message: "Invalid JSON input",
        },
        JsonError::Depth => RowFailure::Data {
            reason,
            message: "JSON nesting depth exceeds 1024",
        },
        JsonError::Value(cause) => cause.into(),
    }
}

fn input_failure(function: JsonFunction, error: JsonError) -> RowFailure {
    let reason = if function == JsonFunction::Parse {
        INVALID_FUNCTION_ARGUMENT
    } else {
        JSON_INPUT_CONVERSION_ERROR
    };
    item_failure(error, reason)
}

#[derive(Clone, Copy)]
struct RowArgument<'a> {
    argument: EvaluatedArgument<'a>,
    row: usize,
}

impl<'a> RowArgument<'a> {
    fn array(self) -> &'a dyn Array {
        self.argument.array().as_ref()
    }

    fn is_null(self) -> Result<bool, RowFailure> {
        let array = self.array();
        if self.row >= array.len() {
            return Err(internal("SQL JSON argument address exceeds its Arrow array").into());
        }
        Ok(array.is_null(self.row) || array.data_type() == &DataType::Null)
    }

    fn text(self) -> Result<&'a str, RowFailure> {
        self.array()
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(self.row))
            .ok_or_else(|| internal("SQL JSON text source lacks its declared Utf8 carrier").into())
    }

    fn binary(self) -> Result<&'a [u8], RowFailure> {
        self.array()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| array.value(self.row))
            .ok_or_else(|| {
                internal("SQL JSON binary source lacks its declared VARBINARY carrier").into()
            })
    }

    fn json(self) -> Result<&'a [u8], RowFailure> {
        self.array()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|array| array.value(self.row))
            .ok_or_else(|| {
                internal("SQL JSON source lacks its declared LargeBinary carrier").into()
            })
    }
}

fn evaluate_row(
    function: JsonFunction,
    input: ScalarCallInput<'_, '_>,
    ordinal: usize,
    batch_row: usize,
    work: &mut RowWork<'_, '_>,
) -> Result<Option<Vec<u8>>, RowFailure> {
    let argument = |index| {
        let argument: EvaluatedArgument<'_> = *input
            .arguments()
            .get(index)
            .ok_or_else(|| invalid("SQL JSON function is missing a bound argument"))?;
        Ok::<_, KernelFailure>(RowArgument {
            row: argument.value_row(ordinal, batch_row),
            argument,
        })
    };
    let values = match function {
        JsonFunction::Array { null_on_null } => {
            let count = input.arguments().len();
            let _grant = work.reserve(size::<Input<'_>>(count)?)?;
            let mut items = reserve_vec(count)?;
            for index in 0..count {
                work.step()?;
                let value = argument(index)?;
                if !value.is_null()? {
                    items.push(Input::Encoded(Node::from_root(value.json()?)?));
                } else if null_on_null {
                    items.push(Input::Null);
                }
            }
            json_value::encode(Input::Array(&items), work)
                .map_err(|error| item_failure(error, INVALID_FUNCTION_ARGUMENT))?
        }
        JsonFunction::Object {
            null_on_null,
            unique,
        } => {
            if !input.arguments().len().is_multiple_of(2) {
                return Err(invalid("JSON_OBJECT has incomplete key/value arguments").into());
            }
            let count = input.arguments().len() / 2;
            let _grant = work.reserve(size::<(&str, Input<'_>)>(count)?)?;
            let mut members = reserve_vec(count)?;
            for index in 0..count {
                work.step()?;
                let key = argument(index)?;
                if key.is_null()? {
                    return Err(RowFailure::Data {
                        reason: INVALID_FUNCTION_ARGUMENT,
                        message: "Null key is not valid in JSON_OBJECT",
                    });
                }
                let value = argument(count + index)?;
                let item = if !value.is_null()? {
                    let node = Node::from_root(value.json()?)?;
                    Input::Encoded(node)
                } else if null_on_null {
                    Input::Null
                } else {
                    continue;
                };
                members.push((key.text()?, item));
            }
            let bytes = json_value::encode(Input::Object(&members), work)
                .map_err(|error| item_failure(error, INVALID_FUNCTION_ARGUMENT))?;
            if unique {
                validate_unique(Node::from_root(&bytes)?, work)?;
            }
            bytes
        }
        other => {
            let value = argument(0)?;
            if value.is_null()? {
                return Ok(None);
            }
            match other {
                JsonFunction::Parse | JsonFunction::ReadText { .. } => {
                    json_value::parse(value.text()?, work)
                        .map_err(|error| input_failure(function, error))?
                }
                JsonFunction::ReadBinary { encoding, .. } => {
                    let bytes = value.binary()?;
                    if encoding == JsonEncoding::Utf8 {
                        match json_value::validate_utf8(bytes, &mut || work.step()) {
                            Ok(()) => {
                                // SAFETY: the entire same immutable slice was just validated.
                                let text = unsafe { std::str::from_utf8_unchecked(bytes) };
                                return json_value::parse(text, work)
                                    .map(Some)
                                    .map_err(|error| input_failure(function, error));
                            }
                            Err(json_value::ObservedError::Control(cause)) => {
                                return Err(cause.into());
                            }
                            Err(json_value::ObservedError::Value(_)) => {}
                        }
                    }
                    let text = decode_text(bytes, encoding, work)
                        .map_err(|error| input_failure(function, error))?;
                    json_value::parse(&text.text, work)
                        .map_err(|error| input_failure(function, error))?
                }
                JsonFunction::Scalar => {
                    let ty = input
                        .contract()
                        .value_argument_types()
                        .next()
                        .ok_or_else(|| invalid("json_scalar has no selected argument type"))?;
                    let scalar = read_scalar(value.array(), ty, value.row)?
                        .ok_or_else(|| internal("json_scalar lost a selected non-NULL value"))?;
                    encode_scalar(scalar, ty.value_type().kind(), work)?
                }
                JsonFunction::Format | JsonFunction::SerializeBinary(JsonEncoding::Utf8) => {
                    json_value::format(value.json()?, work)
                        .map_err(format_failure)?
                        .into_bytes()
                }
                JsonFunction::SerializeBinary(encoding) => {
                    let text = json_value::format(value.json()?, work).map_err(format_failure)?;
                    let text = ReservedText {
                        text,
                        _charge: work.output.take().ok_or_else(|| {
                            internal("SQL JSON serializer did not reserve its text")
                        })?,
                    };
                    encode_binary(&text.text, encoding, work)?
                }
                JsonFunction::Array { .. } | JsonFunction::Object { .. } => unreachable!(),
            }
        }
    };
    Ok(Some(values))
}

fn format_failure(error: JsonError) -> RowFailure {
    match error {
        JsonError::Host(cause) => cause.into(),
        JsonError::Value(cause) => cause.into(),
        JsonError::Depth => json_value::JsonValueError::Depth.into(),
        JsonError::InvalidText(_) => {
            internal("Native JSON formatter unexpectedly parsed text").into()
        }
    }
}

fn encode_scalar(
    scalar: Scalar<'_>,
    kind: &ValueKind,
    work: &mut RowWork<'_, '_>,
) -> Result<Vec<u8>, RowFailure> {
    if let (Scalar::String(text), ValueKind::BoundedCharacter(character)) = (scalar, kind)
        && character.kind() == CharacterKind::Char
    {
        let mut chars = 0;
        for _ in text.chars() {
            work.step()?;
            chars += 1;
        }
        let padding = (character.length() as usize)
            .checked_sub(chars)
            .ok_or_else(|| internal("SQL CHAR storage exceeds its frozen width"))?;
        if padding != 0 {
            let bytes = add(text.len(), padding)?;
            let _grant = work.reserve(bytes)?;
            let mut padded = String::new();
            padded
                .try_reserve_exact(bytes)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for character in text.chars() {
                work.step()?;
                padded.push(character);
            }
            for _ in 0..padding {
                work.step()?;
                padded.push(' ');
            }
            return json_value::encode(Input::Scalar(Scalar::String(&padded)), work)
                .map_err(format_failure);
        }
    }
    json_value::encode(Input::Scalar(scalar), work).map_err(format_failure)
}

struct ReservedText {
    text: String,
    _charge: OpaqueReservation,
}

fn decode_text(
    bytes: &[u8],
    encoding: JsonEncoding,
    work: &mut RowWork<'_, '_>,
) -> Result<ReservedText, JsonError> {
    // SQL/JSON byte input in Trino uses JDK InputStreamReader. Malformed
    // byte sequences become replacement characters before the JSON grammar.
    let mut capacity = 0usize;
    visit_decoded_text(bytes, encoding, work, |character| {
        capacity = add(capacity, character.len_utf8())?;
        Ok(())
    })?;
    let grant = work.reserve(capacity)?;
    let mut text = String::new();
    text.try_reserve_exact(capacity)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    visit_decoded_text(bytes, encoding, work, |character| {
        text.push(character);
        Ok(())
    })?;
    Ok(ReservedText {
        text,
        _charge: grant,
    })
}

fn visit_decoded_text(
    bytes: &[u8],
    encoding: JsonEncoding,
    work: &mut RowWork<'_, '_>,
    emit: impl FnMut(char) -> Result<(), JsonError>,
) -> Result<(), JsonError> {
    match encoding {
        JsonEncoding::Utf8 => visit_jdk_utf8(bytes, work, emit),
        JsonEncoding::Utf16 => visit_jdk_utf16(bytes, work, emit),
        JsonEncoding::Utf32 => visit_jdk_utf32(bytes, work, emit),
    }
}

fn visit_jdk_utf8(
    mut bytes: &[u8],
    work: &mut RowWork<'_, '_>,
    mut emit: impl FnMut(char) -> Result<(), JsonError>,
) -> Result<(), JsonError> {
    while let Some((&lead, next)) = bytes.split_first() {
        work.step()?;
        let width = match lead {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 1,
        };
        let allowed_continuation = |index: usize, value: u8| {
            matches!(value, 0x80..=0xbf)
                && (index != 0
                    || (lead != 0xe0 || value >= 0xa0)
                        && (lead != 0xf0 || value >= 0x90)
                        && (lead != 0xf4 || value < 0x90))
        };
        let length = 1 + next
            .iter()
            .take(width - 1)
            .enumerate()
            .take_while(|&(index, value)| allowed_continuation(index, *value))
            .count();
        let (character, rest) = bytes.split_at(length);
        let character = std::str::from_utf8(character)
            .ok()
            .and_then(|character| character.chars().next())
            .unwrap_or(char::REPLACEMENT_CHARACTER);
        emit(character)?;
        bytes = rest;
    }
    Ok(())
}

fn visit_jdk_utf16(
    bytes: &[u8],
    work: &mut RowWork<'_, '_>,
    mut emit: impl FnMut(char) -> Result<(), JsonError>,
) -> Result<(), JsonError> {
    let units = bytes.chunks_exact(2);
    let trailing = !units.remainder().is_empty();
    let mut units = units.map(|unit| u16::from_le_bytes([unit[0], unit[1]]));
    while let Some(unit) = units.next() {
        work.step()?;
        let value = match unit {
            0xd800..=0xdbff => match units.next() {
                Some(next @ 0xdc00..=0xdfff) => {
                    let high = u32::from(unit - 0xd800) << 10;
                    let low = u32::from(next - 0xdc00);
                    char::from_u32(0x10000 + high + low).expect("valid UTF-16 surrogate pair")
                }
                Some(_) => char::REPLACEMENT_CHARACTER,
                None => {
                    // At end-of-input the decoder replaces the entire
                    // incomplete surrogate sequence, including a trailing byte.
                    emit(char::REPLACEMENT_CHARACTER)?;
                    return Ok(());
                }
            },
            0xdc00..=0xdfff => char::REPLACEMENT_CHARACTER,
            _ => char::from_u32(u32::from(unit)).expect("non-surrogate UTF-16 code unit"),
        };
        emit(value)?;
    }
    if trailing {
        work.step()?;
        emit(char::REPLACEMENT_CHARACTER)?;
    }
    Ok(())
}

fn visit_jdk_utf32(
    bytes: &[u8],
    work: &mut RowWork<'_, '_>,
    mut emit: impl FnMut(char) -> Result<(), JsonError>,
) -> Result<(), JsonError> {
    let bytes = bytes.strip_prefix(&[0xff, 0xfe, 0, 0]).unwrap_or(bytes);
    let chunks = bytes.chunks_exact(4);
    let trailing = !chunks.remainder().is_empty();
    let mut units =
        chunks.map(|unit| u32::from_le_bytes(unit.try_into().expect("known UTF-32 width")));
    let mut offset = 0;
    while let Some(unit) = units.next() {
        work.step()?;
        let value = match unit {
            0xd800..=0xdbff => match units.next() {
                Some(low @ 0xdc00..=0xdfff) => {
                    offset += 4;
                    char::from_u32(0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00))
                        .expect("valid surrogate pair decoded by JDK to UTF-16")
                }
                _ => return Err(JsonError::InvalidText(offset)),
            },
            0xdc00..=0xdfff => return Err(JsonError::InvalidText(offset)),
            _ => char::from_u32(unit).unwrap_or(char::REPLACEMENT_CHARACTER),
        };
        emit(value)?;
        offset += 4;
    }
    if trailing {
        work.step()?;
        emit(char::REPLACEMENT_CHARACTER)?;
    }
    Ok(())
}

fn encode_binary(
    text: &str,
    encoding: JsonEncoding,
    work: &mut RowWork<'_, '_>,
) -> Result<Vec<u8>, KernelFailure> {
    match encoding {
        JsonEncoding::Utf8 => Err(invalid(
            "UTF-8 serialization must preserve its original text",
        )),
        JsonEncoding::Utf16 => {
            let mut units = 0usize;
            for character in text.chars() {
                work.step()?;
                units = add(units, character.len_utf16())?;
            }
            let bytes = size::<u16>(units)?;
            work.reserve_output(bytes)?;
            let mut result = reserve_vec(bytes)?;
            for unit in text.encode_utf16() {
                work.step()?;
                result.extend_from_slice(&unit.to_le_bytes());
            }
            Ok(result)
        }
        JsonEncoding::Utf32 => {
            let mut units = 0usize;
            for _ in text.chars() {
                work.step()?;
                units = add(units, 1)?;
            }
            let bytes = size::<u32>(units)?;
            work.reserve_output(bytes)?;
            let mut result = reserve_vec(bytes)?;
            for character in text.chars() {
                work.step()?;
                result.extend_from_slice(&u32::from(character).to_le_bytes());
            }
            Ok(result)
        }
    }
}

fn duplicate_key() -> RowFailure {
    RowFailure::Data {
        reason: INVALID_FUNCTION_ARGUMENT,
        message: "Duplicate key is not valid in JSON_OBJECT WITH UNIQUE KEYS",
    }
}

fn validate_unique(root: Node<'_>, work: &mut RowWork<'_, '_>) -> Result<(), RowFailure> {
    let mut grant = None;
    let mut stack = Vec::<json_value::Entries<'_>>::new();
    let mut node = Some(root);
    loop {
        if let Some(value) = node.take() {
            work.step()?;
            let container = match value.kind()? {
                Kind::Object(container) => {
                    let unique = container
                        .has_unique_keys_observed(&mut || work.step())
                        .map_err(|error| match error {
                            json_value::ObservedError::Value(cause) => RowFailure::from(cause),
                            json_value::ObservedError::Control(cause) => RowFailure::from(cause),
                        })?;
                    if !unique {
                        return Err(duplicate_key());
                    }
                    Some(container)
                }
                Kind::Array(container) => Some(container),
                _ => None,
            };
            if let Some(container) = container {
                if stack.len() == stack.capacity() {
                    let capacity = stack
                        .capacity()
                        .checked_mul(2)
                        .ok_or(KernelFailure::ResourceExhausted)?
                        .max(1);
                    let next_grant = work.reserve(size::<json_value::Entries<'_>>(capacity)?)?;
                    stack
                        .try_reserve_exact(capacity - stack.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?;
                    grant = Some(next_grant);
                }
                stack.push(container.entries());
            }
        }
        while let Some(entries) = stack.last_mut() {
            work.step()?;
            if let Some(entry) = entries.next() {
                node = Some(entry?.value);
                break;
            }
            stack.pop();
        }
        if node.is_none() {
            break;
        }
    }
    drop(stack);
    drop(grant);
    Ok(())
}
