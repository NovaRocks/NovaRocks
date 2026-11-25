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

//! Conversion of an original typed JSON value to a frozen SQL structure.

use super::{
    LeafError, Reader, Scalar, consume_value, decode_string, number_text, numeric, validate,
};
use crate::cast::{
    CastPrepareError, CastRowResult, PreparedCastRecipe, SignedWidth, map_mode, trino,
};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{DataFailureReason, KernelEvaluationControl, KernelFailure, RowDataError};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
    StructBuilder, make_builder,
};
use arrow_array::{Array, ArrayRef, ListArray, MapArray, StringArray, StructArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CharacterKind, CompileCheckpoints, FunctionValueType, ValueKind, ValueLogicalType, ValueTypeRef,
};
use std::{collections::HashMap, ops::Range};

#[derive(Clone, Copy)]
enum Target {
    Array,
    Map,
    Row,
    Json,
    Boolean,
    Numeric(trino::Numeric),
    Varchar(Option<u32>),
}

fn target(kind: &ValueKind) -> Option<Target> {
    Some(match kind {
        ValueKind::List(_) => Target::Array,
        ValueKind::Map { .. } => Target::Map,
        ValueKind::Struct(_) => Target::Row,
        ValueKind::Nominal(ValueLogicalType::Json) => Target::Json,
        ValueKind::Scalar(DataType::Boolean) => Target::Boolean,
        ValueKind::String => Target::Varchar(None),
        ValueKind::BoundedCharacter(character) if character.kind() == CharacterKind::Varchar => {
            Target::Varchar(Some(character.length()))
        }
        _ => Target::Numeric(trino::Numeric::from_kind(kind)?),
    })
}

pub(in crate::cast) fn validate_target(
    value: &ValueTypeRef,
    carrier: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CastPrepareError> {
    work.step()?;
    let Some(target) = target(value.kind()) else {
        return Err(CastPrepareError::Illegal);
    };
    match (target, value.kind(), carrier) {
        (Target::Array, ValueKind::List(item), DataType::List(field)) => {
            validate_target(item, field.data_type(), work)?;
        }
        (Target::Map, ValueKind::Map { key, value }, DataType::Map(entries, sorted)) => {
            if !matches!(
                self::target(key.kind()),
                Some(Target::Boolean | Target::Numeric(_) | Target::Varchar(_))
            ) {
                return Err(CastPrepareError::Illegal);
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CastPrepareError::TypeMismatch);
            };
            if fields.len() != 2 || fields[0].is_nullable() || entries.is_nullable() {
                return Err(CastPrepareError::TypeMismatch);
            }
            // The Arrow MAP builder owns this canonical entries layout.
            if *sorted || !entries.metadata().is_empty() {
                return Err(CastPrepareError::Unsupported);
            }
            validate_target(key, fields[0].data_type(), work)?;
            validate_target(value, fields[1].data_type(), work)?;
        }
        (Target::Row, ValueKind::Struct(members), DataType::Struct(fields)) => {
            if members.len() != fields.len() {
                return Err(CastPrepareError::TypeMismatch);
            }
            let first_is_named = members
                .first()
                .is_some_and(|field| !field.name().is_empty());
            for (member, field) in members.iter().zip(fields) {
                work.step()?;
                // Anonymous and completely named ROWs have stable matching
                // contracts. A mixed declaration has no installed JSON profile.
                if member.name().is_empty() == first_is_named {
                    return Err(CastPrepareError::Unsupported);
                }
                validate_target(member.value_type(), field.data_type(), work)?;
            }
        }
        (Target::Json | Target::Varchar(_), _, DataType::Utf8)
        | (Target::Boolean, _, DataType::Boolean)
        | (
            Target::Numeric(_),
            _,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..),
        ) => {}
        _ => return Err(CastPrepareError::Unsupported),
    }
    Ok(())
}

enum CastFault {
    Data(&'static str),
    Outer(KernelFailure),
}

impl From<KernelFailure> for CastFault {
    fn from(error: KernelFailure) -> Self {
        Self::Outer(error)
    }
}
impl From<LeafError> for CastFault {
    fn from(error: LeafError) -> Self {
        match error {
            LeafError::Outer(error) => Self::Outer(error),
            _ => Self::Data("Invalid JSON value for the cast target"),
        }
    }
}

pub(in crate::cast) fn evaluate_row(
    recipe: &PreparedCastRecipe,
    input: &ArrayRef,
    row: usize,
    ordinal: usize,
    _control: &dyn KernelEvaluationControl,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, KernelFailure> {
    let input = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("JSON structure CAST lost its UTF-8 source"))?;
    match convert(input.value(row), &recipe.result, ordinal, work) {
        Ok(value) => Ok(value),
        Err(CastFault::Outer(error)) => Err(error),
        Err(CastFault::Data(detail)) => Ok(Err(RowDataError::with_reason(
            ordinal,
            DataFailureReason::Domain(
                novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT,
            ),
            detail,
        ))),
    }
}

fn convert(
    raw: &str,
    target: &FunctionValueType,
    ordinal: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, CastFault> {
    // Preserve the original JSON validation, including opaque child values
    // and the existing raw-input depth rule. No object map collapses members.
    validate(raw, work)?;
    let mut reader = Reader { raw, at: 0, work };
    reader.work.flush()?;
    let mut builder = make_builder(target.data_type(), 1);
    append_value(&mut reader, target.value_type(), builder.as_mut())?;
    reader.space()?;
    if reader.at != raw.len() {
        return Err(CastFault::Data(
            "Unexpected trailing token after JSON value",
        ));
    }
    reader.work.flush()?;
    let value = builder.finish();
    reader.work.flush()?;
    if has_duplicate_map_key(target, &value, reader.work)? {
        return Ok(Err(map_mode::duplicate_map_key_error(ordinal)));
    }
    Ok(Ok(value))
}

fn builder_as<T: 'static>(builder: &mut dyn ArrayBuilder) -> Result<&mut T, KernelFailure> {
    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| internal("JSON CAST builder differs from its frozen target"))
}

fn append_value(
    reader: &mut Reader<'_, '_, '_>,
    value: &ValueTypeRef,
    builder: &mut dyn ArrayBuilder,
) -> Result<(), CastFault> {
    reader.space()?;
    reader.work.flush()?;
    let target =
        target(value.kind()).ok_or_else(|| internal("JSON CAST lost its frozen value target"))?;
    if matches!(target, Target::Json) {
        let start = reader.at;
        consume_value(reader)?;
        return append_text(builder, Some(&reader.raw[start..reader.at]), reader.work);
    }
    if reader.byte() == Some(b'n') {
        reader.literal(b"null")?;
        if !value.nullable() {
            return Err(CastFault::Data(
                "JSON null cannot populate a required field",
            ));
        }
        return append_null(value, builder, reader.work);
    }
    match (target, value.kind()) {
        (Target::Array, ValueKind::List(item)) => {
            if reader.byte() != Some(b'[') {
                return Err(CastFault::Data("Expected a JSON array"));
            }
            reader.require(b'[')?;
            reader.space()?;
            let out = builder_as::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
            if reader.byte() != Some(b']') {
                loop {
                    append_value(reader, item, out.values().as_mut())?;
                    if !next_member(reader, b']')? {
                        break;
                    }
                }
            }
            reader.require(b']')?;
            check_child_offset(out.values().len())?;
            reader.work.flush()?;
            out.append(true);
            Ok(())
        }
        (Target::Map, ValueKind::Map { key, value }) => {
            if reader.byte() != Some(b'{') {
                return Err(CastFault::Data("Expected a JSON object for MAP"));
            }
            reader.require(b'{')?;
            reader.space()?;
            let out =
                builder_as::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(builder)?;
            if reader.byte() != Some(b'}') {
                loop {
                    let (token, decoded_bytes) = member_name(reader)?;
                    let target = self::target(key.kind())
                        .ok_or_else(|| internal("JSON CAST lost its frozen MAP key type"))?;
                    let name = Scalar::String {
                        token,
                        decoded_bytes,
                    };
                    append_scalar(
                        target,
                        read_scalar(reader, name, target)?,
                        out.keys().as_mut(),
                        reader.work,
                    )?;
                    append_value(reader, value, out.values().as_mut())?;
                    if !next_member(reader, b'}')? {
                        break;
                    }
                }
            }
            reader.require(b'}')?;
            check_child_offset(out.keys().len())?;
            reader.work.flush()?;
            out.append(true).map_err(assembly_failure)?;
            Ok(())
        }
        (Target::Row, ValueKind::Struct(fields)) => append_row(reader, fields, builder),
        _ => {
            if matches!(reader.byte(), Some(b'[' | b'{')) {
                return Err(CastFault::Data(
                    "Expected a JSON scalar for the cast target",
                ));
            }
            let scalar = reader.scalar()?;
            let value = read_scalar(reader, scalar, target)?;
            append_scalar(target, value, builder, reader.work)
        }
    }
}

fn member_name(reader: &mut Reader<'_, '_, '_>) -> Result<(Range<usize>, usize), LeafError> {
    reader.space()?;
    let name = reader.string_token()?;
    reader.space()?;
    reader.require(b':')?;
    reader.space()?;
    Ok(name)
}

fn next_member(reader: &mut Reader<'_, '_, '_>, close: u8) -> Result<bool, LeafError> {
    reader.space()?;
    if reader.byte() == Some(close) {
        return Ok(false);
    }
    reader.require(b',')?;
    reader.space()?;
    Ok(true)
}

fn append_row(
    reader: &mut Reader<'_, '_, '_>,
    fields: &[novarocks_type_contract::ValueField],
    builder: &mut dyn ArrayBuilder,
) -> Result<(), CastFault> {
    let row = builder_as::<StructBuilder>(builder)?;
    match reader.byte() {
        Some(b'[') => {
            reader.require(b'[')?;
            reader.space()?;
            for (index, (field, builder)) in fields.iter().zip(row.field_builders_mut()).enumerate()
            {
                reader.work.step()?;
                if reader.byte() == Some(b']') {
                    return Err(CastFault::Data(
                        "JSON array length differs from ROW field count",
                    ));
                }
                if index != 0 {
                    reader.require(b',')?;
                }
                append_value(reader, field.value_type(), builder.as_mut())?;
                reader.space()?;
            }
            if reader.byte() != Some(b']') {
                return Err(CastFault::Data(
                    "JSON array length differs from ROW field count",
                ));
            }
            reader.require(b']')?;
        }
        Some(b'{') if fields.first().is_some_and(|field| !field.name().is_empty()) => {
            reader.require(b'{')?;
            reader.space()?;
            reader.work.flush()?;
            let mut names = HashMap::new();
            names
                .try_reserve(fields.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut seen = crate::cast::array_mode::reserve(fields.len(), reader.work)?;
            seen.resize(fields.len(), false);
            for (index, field) in fields.iter().enumerate() {
                for _ in field.name().as_bytes().chunks(32) {
                    reader.work.step()?;
                }
                reader.work.flush()?;
                names.insert(field.name(), index);
            }
            if reader.byte() != Some(b'}') {
                loop {
                    let (token, bytes) = member_name(reader)?;
                    let name = decode_string(&reader.raw[token], bytes, reader.work)?;
                    reader.work.flush()?;
                    // Like Trino, fold the incoming JSON member. The target
                    // keeps the exact SQL field name that analysis declared.
                    let folded = name.to_lowercase();
                    reader.work.flush()?;
                    if let Some(&index) = names.get(folded.as_str()) {
                        if seen[index] {
                            return Err(CastFault::Data("Duplicate JSON field for ROW cast"));
                        }
                        seen[index] = true;
                        append_value(
                            reader,
                            fields[index].value_type(),
                            row.field_builders_mut()[index].as_mut(),
                        )?;
                    } else {
                        consume_value(reader)?;
                    }
                    if !next_member(reader, b'}')? {
                        break;
                    }
                }
            }
            reader.require(b'}')?;
            for (index, present) in seen.into_iter().enumerate() {
                reader.work.step()?;
                if !present {
                    let value = fields[index].value_type();
                    if !value.nullable() {
                        return Err(CastFault::Data(
                            "Missing JSON member for a required ROW field",
                        ));
                    }
                    append_null(value, row.field_builders_mut()[index].as_mut(), reader.work)?;
                }
            }
        }
        _ => {
            return Err(CastFault::Data(
                "Expected a JSON array or an object for a named ROW",
            ));
        }
    }
    reader.work.flush()?;
    row.append(true);
    Ok(())
}

fn append_null(
    value: &ValueTypeRef,
    builder: &mut dyn ArrayBuilder,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    work.flush()?;
    match value.kind() {
        ValueKind::List(_) => {
            builder_as::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?.append_null()
        }
        ValueKind::Map { .. } => {
            builder_as::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(builder)?
                .append(false)
                .map_err(assembly_failure)?;
        }
        ValueKind::Struct(fields) => {
            let row = builder_as::<StructBuilder>(builder)?;
            for (field, child) in fields.iter().zip(row.field_builders_mut()) {
                work.step()?;
                append_null(field.value_type(), child.as_mut(), work)?;
            }
            work.flush()?;
            row.append_null();
        }
        _ => {
            let target = target(value.kind())
                .ok_or_else(|| internal("JSON CAST lost the nullable leaf target"))?;
            append_scalar(target, CastRowResult::Null, builder, work)?;
        }
    }
    Ok(())
}

fn read_scalar(
    reader: &mut Reader<'_, '_, '_>,
    value: Scalar,
    target: Target,
) -> Result<CastRowResult, CastFault> {
    use CastRowResult as R;

    let converted = match (value, target) {
        (
            Scalar::String {
                token,
                decoded_bytes,
            },
            target,
        ) => {
            let text = decode_string(&reader.raw[token], decoded_bytes, reader.work)?;
            match target {
                Target::Varchar(_) => Some(R::Text(text)),
                Target::Boolean => numeric::converted(trino::parse_boolean(&text).map(R::Boolean))?,
                Target::Numeric(target) => {
                    numeric::converted(trino::parse_number(&text, target, reader.work))?
                }
                _ => None,
            }
        }
        (Scalar::Boolean(value), Target::Varchar(_)) => Some(R::Text(super::copy_text(
            if value { "true" } else { "false" },
            reader.work,
        )?)),
        (Scalar::Boolean(value), Target::Boolean) => Some(R::Boolean(value)),
        (Scalar::Boolean(value), Target::Numeric(target)) => numeric::converted(
            trino::parse_number(if value { "1" } else { "0" }, target, reader.work),
        )?,
        (Scalar::Number(token), target) => {
            let raw = &reader.raw[token];
            match target {
                Target::Varchar(_) => Some(R::Text(number_text(raw, reader.work)?)),
                Target::Boolean => Some(R::Boolean(numeric::nonzero(raw, reader.work)?)),
                Target::Numeric(target) => numeric::convert(raw, target, reader.work)?,
                _ => None,
            }
        }
        _ => None,
    };
    converted.ok_or(CastFault::Data(
        "Cannot convert JSON scalar to the requested type",
    ))
}

fn append_scalar(
    target: Target,
    value: CastRowResult,
    builder: &mut dyn ArrayBuilder,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    use CastRowResult as R;
    use trino::Numeric::{Decimal, Double, Integer, Real};

    work.flush()?;
    macro_rules! insert {
        ($builder:ty, $variant:path, $convert:expr) => {{
            let out = builder_as::<$builder>(builder)?;
            match value {
                $variant(value) => out.append_value(($convert)(value)),
                R::Null => out.append_null(),
                _ => return Err(internal("JSON CAST scalar differs from its frozen result").into()),
            }
        }};
    }
    match target {
        Target::Boolean => insert!(BooleanBuilder, R::Boolean, |value| value),
        Target::Numeric(Integer(SignedWidth::I8)) => {
            insert!(Int8Builder, R::Signed, |value| value as i8)
        }
        Target::Numeric(Integer(SignedWidth::I16)) => {
            insert!(Int16Builder, R::Signed, |value| value as i16)
        }
        Target::Numeric(Integer(SignedWidth::I32)) => {
            insert!(Int32Builder, R::Signed, |value| value as i32)
        }
        Target::Numeric(Integer(SignedWidth::I64)) => {
            insert!(Int64Builder, R::Signed, |value| value)
        }
        Target::Numeric(Real) => insert!(Float32Builder, R::Float32, |value| value),
        Target::Numeric(Double) => insert!(Float64Builder, R::Float64, |value| value),
        Target::Numeric(Decimal(..)) => insert!(Decimal128Builder, R::Decimal128, |value| value),
        Target::Varchar(max_length) => {
            let text = match &value {
                R::Text(text) => Some(text.as_str()),
                R::Null => None,
                _ => return Err(internal("JSON CAST scalar has a foreign VARCHAR result").into()),
            };
            if let (Some(limit), Some(text)) = (max_length, text) {
                let mut width = 0;
                for _ in text.chars() {
                    work.step()?;
                    if width == limit {
                        return Err(CastFault::Data(
                            "JSON string exceeds the frozen VARCHAR length",
                        ));
                    }
                    width += 1;
                }
            }
            append_text(builder, text, work)?;
        }
        Target::Json if matches!(value, R::Null) => append_text(builder, None, work)?,
        _ => return Err(internal("JSON CAST scalar has a foreign target").into()),
    }
    Ok(())
}

fn append_text(
    builder: &mut dyn ArrayBuilder,
    value: Option<&str>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    let out = builder_as::<StringBuilder>(builder)?;
    if let Some(text) = value {
        let extent = out
            .values_slice()
            .len()
            .checked_add(text.len())
            .ok_or(KernelFailure::ResourceExhausted)?;
        crate::selected_copy::checked_utf8_output_offset(extent)?;
        work.flush()?;
        out.append_value(text);
    } else {
        work.flush()?;
        out.append_null();
    }
    work.flush()?;
    Ok(())
}

fn check_child_offset(length: usize) -> Result<(), KernelFailure> {
    i32::try_from(length).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(())
}

fn assembly_failure(cause: arrow_schema::ArrowError) -> KernelFailure {
    crate::control_values::AssemblyFailure::arrow("JSON CAST child assembly", cause).into_kernel()
}

fn has_duplicate_map_key(
    value: &FunctionValueType,
    array: &ArrayRef,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step()?;
    match value.value_type().kind() {
        ValueKind::Map { .. } => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| internal("JSON CAST did not assemble the frozen MAP"))?;
            let key = value
                .child_at(0)
                .ok_or_else(|| internal("JSON CAST lost the MAP key type"))?;
            for offsets in map.value_offsets().windows(2) {
                work.step()?;
                if map_mode::converted_keys_have_duplicate(
                    &key,
                    map.keys(),
                    offsets[0] as usize..offsets[1] as usize,
                    work,
                )? {
                    return Ok(true);
                }
            }
            let item = value
                .child_at(1)
                .ok_or_else(|| internal("JSON CAST lost the MAP value type"))?;
            has_duplicate_map_key(&item, map.values(), work)
        }
        ValueKind::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| internal("JSON CAST did not assemble the frozen ARRAY"))?;
            let item = value
                .child_at(0)
                .ok_or_else(|| internal("JSON CAST lost the ARRAY type"))?;
            has_duplicate_map_key(&item, list.values(), work)
        }
        ValueKind::Struct(_) => {
            let row = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| internal("JSON CAST did not assemble the frozen ROW"))?;
            for (index, column) in row.columns().iter().enumerate() {
                let item = value
                    .child_at(index)
                    .ok_or_else(|| internal("JSON CAST lost a ROW field"))?;
                if has_duplicate_map_key(&item, column, work)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}
