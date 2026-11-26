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

use crate::cast::{
    CastPrepareError, CastRowResult, PreparedCastRecipe, SignedWidth, map_mode, trino,
};
use crate::json_value::{self, Container, Kind, Node, ObservedError, Scalar};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{DataFailureReason, KernelFailure, RowDataError};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder,
    StringBuilder, StructBuilder, make_builder,
};
use arrow_array::{Array, ArrayRef, LargeBinaryArray, ListArray, MapArray, StructArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CharacterKind, CompileCheckpoints, FunctionValueType, ValueKind, ValueLogicalType, ValueTypeRef,
};
use std::{collections::HashMap, io::Write as _};

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
        (Target::Json, _, DataType::LargeBinary)
        | (Target::Varchar(_), _, DataType::Utf8)
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
impl From<json_value::JsonValueError> for CastFault {
    fn from(error: json_value::JsonValueError) -> Self {
        Self::Outer(internal(&error.to_string()))
    }
}

impl From<ObservedError<KernelFailure>> for CastFault {
    fn from(error: ObservedError<KernelFailure>) -> Self {
        match error {
            ObservedError::Value(error) => error.into(),
            ObservedError::Control(error) => error.into(),
        }
    }
}

impl From<trino::ConversionFailure> for CastFault {
    fn from(error: trino::ConversionFailure) -> Self {
        match error {
            trino::ConversionFailure::Outer(error) => Self::Outer(error),
            _ => Self::Data("Cannot convert JSON scalar to the requested type"),
        }
    }
}

pub(in crate::cast) fn evaluate_row(
    recipe: &PreparedCastRecipe,
    input: &ArrayRef,
    row: usize,
    ordinal: usize,
    staging: &mut OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, KernelFailure> {
    let input = input
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid("JSON structure CAST lost its LargeBinary source"))?;
    let previous_bytes = staging.bytes();
    let converted = convert(input.value(row), &recipe.result, ordinal, staging, work);
    if matches!(&converted, Ok(Err(_)) | Err(CastFault::Data(_)))
        && staging.bytes() != previous_bytes
    {
        // The failed row's builders have already been destroyed.
        let mut no_growth = staging.reserve_operation(0)?;
        staging.reconcile_under_reservation(previous_bytes, &mut no_growth)?;
    }
    match converted {
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
    raw: &[u8],
    target: &FunctionValueType,
    ordinal: usize,
    staging: &mut OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, CastFault> {
    let root = Node::from_root(raw)?;
    root.validate(|| work.step())?;
    work.flush()?;
    let mut builder = make_builder(target.data_type(), 1);
    append_value(root, target.value_type(), builder.as_mut(), staging, work)?;
    work.flush()?;
    let value = builder.finish();
    work.flush()?;
    if has_duplicate_map_key(target, &value, work)? {
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
    node: Node<'_>,
    value: &ValueTypeRef,
    builder: &mut dyn ArrayBuilder,
    staging: &mut OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    let target =
        target(value.kind()).ok_or_else(|| internal("JSON CAST lost its frozen value target"))?;
    if matches!(target, Target::Json) {
        let out = builder_as::<LargeBinaryBuilder>(builder)?;
        let payload_bytes = node
            .item_bytes()
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let staged_bytes = staging
            .bytes()
            .checked_add(payload_bytes)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let extent = out
            .values_slice()
            .len()
            .checked_add(payload_bytes)
            .ok_or(KernelFailure::ResourceExhausted)?;
        i64::try_from(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut reservation = staging.reserve_operation(payload_bytes)?;
        staging.reconcile_under_reservation(staged_bytes, &mut reservation)?;
        out.write_all(&[json_value::VERSION])
            .map_err(|_| internal("JSON CAST could not write its version byte"))?;
        for chunk in node.item_bytes().chunks(4096) {
            work.step()?;
            out.write_all(chunk)
                .map_err(|_| internal("JSON CAST could not write its selected item"))?;
        }
        out.append_value([]);
        return Ok(());
    }
    let kind = node.kind_observed(|| work.step())?;
    if matches!(kind, Kind::Null) {
        if !value.nullable() {
            return Err(CastFault::Data(
                "JSON null cannot populate a required field",
            ));
        }
        return append_null(value, builder, work);
    }
    match (target, value.kind(), kind) {
        (Target::Array, ValueKind::List(item), Kind::Array(elements)) => {
            let out = builder_as::<ListBuilder<Box<dyn ArrayBuilder>>>(builder)?;
            for entry in elements.entries() {
                work.step()?;
                append_value(entry?.value, item, out.values().as_mut(), staging, work)?;
            }
            check_child_offset(out.values().len())?;
            work.flush()?;
            out.append(true);
            Ok(())
        }
        (Target::Map, ValueKind::Map { key, value }, Kind::Object(members)) => {
            let out =
                builder_as::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(builder)?;
            let key_type = self::target(key.kind())
                .ok_or_else(|| internal("JSON CAST lost its frozen MAP key type"))?;
            for entry in members.entries() {
                work.step()?;
                let entry = entry?;
                let name = entry
                    .key
                    .ok_or_else(|| internal("JSON object entry lost its key"))?;
                append_json_scalar(key_type, Scalar::String(name), out.keys().as_mut(), work)?;
                append_value(entry.value, value, out.values().as_mut(), staging, work)?;
            }
            check_child_offset(out.keys().len())?;
            work.flush()?;
            out.append(true).map_err(assembly_failure)?;
            Ok(())
        }
        (Target::Row, ValueKind::Struct(fields), kind) => {
            append_row(kind, fields, builder, staging, work)
        }
        (_, _, Kind::Scalar(scalar)) => append_json_scalar(target, scalar, builder, work),
        _ => Err(CastFault::Data("Unexpected JSON shape for the cast target")),
    }
}

fn append_row(
    kind: Kind<'_>,
    fields: &[novarocks_type_contract::ValueField],
    builder: &mut dyn ArrayBuilder,
    staging: &mut OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    let row = builder_as::<StructBuilder>(builder)?;
    match kind {
        Kind::Array(elements) => {
            if fields.len() != elements.len() {
                return Err(CastFault::Data(
                    "JSON array length differs from ROW field count",
                ));
            }
            for ((field, builder), entry) in fields
                .iter()
                .zip(row.field_builders_mut())
                .zip(elements.entries())
            {
                work.step()?;
                append_value(
                    entry?.value,
                    field.value_type(),
                    builder.as_mut(),
                    staging,
                    work,
                )?;
            }
        }
        Kind::Object(members) if fields.first().is_some_and(|field| !field.name().is_empty()) => {
            append_named_row(members, fields, row, staging, work)?;
        }
        _ => {
            return Err(CastFault::Data(
                "Expected a JSON array or an object for a named ROW",
            ));
        }
    }
    work.flush()?;
    row.append(true);
    Ok(())
}

fn append_named_row(
    members: Container<'_>,
    fields: &[novarocks_type_contract::ValueField],
    row: &mut StructBuilder,
    staging: &mut OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    let mut names = HashMap::new();
    work.flush()?;
    names
        .try_reserve(fields.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut seen = crate::cast::array_mode::reserve(fields.len(), work)?;
    seen.resize(fields.len(), false);
    for (index, field) in fields.iter().enumerate() {
        for _ in field.name().as_bytes().chunks(32) {
            work.step()?;
        }
        work.flush()?;
        names.insert(field.name(), index);
    }
    for entry in members.entries() {
        let entry = entry?;
        let name = entry
            .key
            .ok_or_else(|| internal("JSON object entry lost its key"))?;
        for _ in name.as_bytes().chunks(32) {
            work.step()?;
        }
        work.flush()?;
        // Trino folds the incoming member, retaining the exact SQL field name.
        let folded = name.to_lowercase();
        if let Some(&index) = names.get(folded.as_str()) {
            if seen[index] {
                return Err(CastFault::Data("Duplicate JSON field for ROW cast"));
            }
            seen[index] = true;
            append_value(
                entry.value,
                fields[index].value_type(),
                row.field_builders_mut()[index].as_mut(),
                staging,
                work,
            )?;
        }
    }
    for (index, present) in seen.into_iter().enumerate() {
        work.step()?;
        if !present {
            let value = fields[index].value_type();
            if !value.nullable() {
                return Err(CastFault::Data(
                    "Missing JSON member for a required ROW field",
                ));
            }
            append_null(value, row.field_builders_mut()[index].as_mut(), work)?;
        }
    }
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

fn append_json_scalar(
    target: Target,
    value: Scalar<'_>,
    builder: &mut dyn ArrayBuilder,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), CastFault> {
    let converted = match target {
        Target::Boolean => CastRowResult::Boolean(trino::json_boolean_cast(value, work)?),
        Target::Numeric(target) => trino::json_number_cast(value, target, work)?,
        Target::Varchar(max) => {
            let mut scratch = json_value::FixedText::<256>::new();
            let text = match value {
                Scalar::String(text) => text,
                _ => {
                    json_value::write_scalar_cast(&mut scratch, value, work).map_err(|error| {
                        match error {
                            json_value::JsonError::Host(error) => CastFault::Outer(error),
                            _ => CastFault::Data("Cannot convert JSON scalar to VARCHAR"),
                        }
                    })?;
                    scratch.as_str()
                }
            };
            if let Some(max) = max {
                for (width, _) in text.chars().enumerate() {
                    work.step()?;
                    if width == max as usize {
                        return Err(CastFault::Data(
                            "JSON string exceeds the frozen VARCHAR length",
                        ));
                    }
                }
            }
            return append_text(builder, Some(text), work);
        }
        _ => {
            return Err(CastFault::Data(
                "Expected a JSON scalar for the cast target",
            ));
        }
    };
    append_scalar(target, converted, builder, work)
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
        Target::Varchar(_) if matches!(value, R::Null) => append_text(builder, None, work)?,
        Target::Json if matches!(value, R::Null) => {
            builder_as::<LargeBinaryBuilder>(builder)?.append_null()
        }
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
