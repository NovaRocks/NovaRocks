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

//! Borrowed comparison of explicit source declarations and complete values.

use super::*;
use crate::iceberg::spec::{PrimitiveType, Type};
use novarocks_type_contract::{ValueKind, ValueTypeRef};

pub(super) fn value(
    source: &Type,
    actual: &ValueTypeRef,
    layout: &DataType,
    nominal: Option<ValueLogicalType>,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    nominals: &crate::schema_mapping::nominal_declaration::NominalSourceDeclarations,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    work.step()?;
    match source {
        Type::Primitive(_) | Type::Variant(_) => {
            let (expected, domain) = match source {
                Type::Primitive(primitive) => (
                    crate::schema_mapping::typed_value::primitive_layout(primitive, integer)
                        .map_err(|cause| match cause {
                            PureProviderCompileError::Provider(cause) => source_error(cause),
                            PureProviderCompileError::Control(cause) => Failure::Control(cause),
                        })?,
                    nominal.unwrap_or(match primitive {
                        PrimitiveType::Uuid => ValueLogicalType::Uuid,
                        _ => ValueLogicalType::Physical,
                    }),
                ),
                Type::Variant(_) => {
                    if integer.is_some() {
                        return Err(invalid(
                            "Iceberg narrow integer domain targets non-INT storage",
                        ));
                    }
                    (
                        DataType::LargeBinary,
                        nominal.unwrap_or(ValueLogicalType::Variant),
                    )
                }
                _ => unreachable!("scalar source arm"),
            };
            let same_kind = match (domain, actual.kind()) {
                (ValueLogicalType::Physical, ValueKind::String) => expected == DataType::Utf8,
                (ValueLogicalType::Physical, ValueKind::Binary) => expected == DataType::Binary,
                (ValueLogicalType::Physical, ValueKind::Scalar(ty)) => ty == &expected,
                (domain, ValueKind::Nominal(actual)) => domain == *actual,
                _ => false,
            };
            work.step()?;
            if !same_kind || layout != &expected {
                return Err(invalid(
                    "public execution value differs from its explicit Iceberg source domain/layout",
                ));
            }
        }
        Type::Struct(source) => {
            let (ValueKind::Struct(values), DataType::Struct(fields)) = (actual.kind(), layout)
            else {
                return Err(invalid(
                    "public execution struct differs from its complete source declaration",
                ));
            };
            if nominal.is_some()
                || integer.is_some()
                || source.fields().len() != values.len()
                || fields.len() != values.len()
            {
                return Err(invalid(
                    "public execution struct source child counts differ",
                ));
            }
            for ((source, value), field) in source.fields().iter().zip(values).zip(fields) {
                if !equal_text(&source.name, value.name(), work)?
                    || !equal_text(&source.name, field.name(), work)?
                    || value.value_type().nullable() == source.required
                    || field.is_nullable() == source.required
                {
                    return Err(invalid(
                        "public execution struct child differs from its exact source name/NULL contract",
                    ));
                }
                self::value(
                    &source.field_type,
                    value.value_type(),
                    field.data_type(),
                    nominals.get(&source.id).map(|domain| domain.logical_type()),
                    None,
                    nominals,
                    work,
                )?;
            }
        }
        Type::List(list) => {
            let (ValueKind::List(value), DataType::List(field)) = (actual.kind(), layout) else {
                return Err(invalid(
                    "public execution list differs from its exact source offsets/shape",
                ));
            };
            if nominal.is_some()
                || integer.is_some()
                || field.name() != "item"
                || value.nullable() == list.element_field.required
                || field.is_nullable() == list.element_field.required
            {
                return Err(invalid(
                    "public execution list element differs from its source NULL contract",
                ));
            }
            self::value(
                &list.element_field.field_type,
                value,
                field.data_type(),
                nominals
                    .get(&list.element_field.id)
                    .map(|domain| domain.logical_type()),
                None,
                nominals,
                work,
            )?;
        }
        Type::Map(map) => {
            let (ValueKind::Map { key, value }, DataType::Map(entries, false)) =
                (actual.kind(), layout)
            else {
                return Err(invalid(
                    "public execution map differs from its exact source layout",
                ));
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("public map has no physical entries"));
            };
            if nominal.is_some()
                || integer.is_some()
                || entries.name() != "entries"
                || entries.is_nullable()
                || fields.len() != 2
                || fields[0].name() != "key"
                || fields[1].name() != "value"
                || key.nullable() == map.key_field.required
                || fields[0].is_nullable() == map.key_field.required
                || value.nullable() == map.value_field.required
                || fields[1].is_nullable() == map.value_field.required
            {
                return Err(invalid(
                    "public map key/value differs from its complete source NULL contract",
                ));
            }
            self::value(
                &map.key_field.field_type,
                key,
                fields[0].data_type(),
                nominals
                    .get(&map.key_field.id)
                    .map(|domain| domain.logical_type()),
                None,
                nominals,
                work,
            )?;
            self::value(
                &map.value_field.field_type,
                value,
                fields[1].data_type(),
                nominals
                    .get(&map.value_field.id)
                    .map(|domain| domain.logical_type()),
                None,
                nominals,
                work,
            )?;
        }
    }
    Ok(())
}

pub(super) fn nominal_declarations(
    relation: &IcebergRuntimeRelation,
) -> Option<&crate::schema_mapping::nominal_declaration::NominalSourceDeclarations> {
    match relation {
        IcebergRuntimeRelation::Table(table) => Some(table.nominal_declarations()),
        IcebergRuntimeRelation::ChangeWindow(window) => Some(window.nominal_declarations()),
        IcebergRuntimeRelation::TableExecute(execute) => match execute.procedure_handle() {
            Some(IcebergTableExecuteProcedureHandle::Optimize(optimize)) => {
                Some(optimize.table_handle().nominal_declarations())
            }
            _ => None,
        },
        _ => None,
    }
}

/// Verify source occurrences directly against their frozen format declaration.
/// SQL's execution projection uses its own anonymous child names and is not
/// the authority for this separate namespace.
pub(super) fn declared_shape(
    declaration: &Type,
    actual: &DataType,
    integer: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    work.step()?;
    let mut field = |source: &NestedField, actual: &Field| -> Result<(), Failure> {
        if !equal_text(&source.name, actual.name(), work)?
            || actual.is_nullable() == source.required
        {
            return Err(invalid(
                "public source occurrence differs from its formal name/NULL declaration",
            ));
        }
        declared_shape(&source.field_type, actual.data_type(), None, work)
    };
    match (declaration, actual) {
        (Type::Primitive(primitive), actual) => {
            let expected = crate::schema_mapping::typed_value::primitive_layout(primitive, integer)
                .map_err(|cause| match cause {
                    PureProviderCompileError::Provider(cause) => source_error(cause),
                    PureProviderCompileError::Control(cause) => Failure::Control(cause),
                })?;
            if actual != &expected {
                return Err(invalid(
                    "public source primitive layout differs from its formal declaration",
                ));
            }
        }
        // This is the existing public source declaration carrier only. It
        // does not enable the SDK's physical Variant reader/writer path.
        (Type::Variant(_), DataType::LargeBinary) if integer.is_none() => {}
        (Type::Struct(source), DataType::Struct(actual))
            if source.fields().len() == actual.len() =>
        {
            for (source, actual) in source.fields().iter().zip(actual) {
                field(source, actual)?;
            }
        }
        (Type::List(source), DataType::List(actual)) => field(&source.element_field, actual)?,
        (Type::Map(source), DataType::Map(entries, false)) => {
            if entries.name() != "entries" || entries.is_nullable() {
                return Err(invalid("public source map entries occurrence differs"));
            }
            let DataType::Struct(actual) = entries.data_type() else {
                return Err(invalid("public source map lacks entries struct"));
            };
            if actual.len() != 2 {
                return Err(invalid("public source map child count differs"));
            }
            field(&source.key_field, &actual[0])?;
            field(&source.value_field, &actual[1])?;
        }
        _ => {
            return Err(invalid(
                "public source shape differs from its formal layout declaration",
            ));
        }
    }
    Ok(())
}

/// Source shape names are format facts. No field annotation contributes a
/// value domain, and source layout offsets remain exact.
pub(super) fn shape(
    a: &DataType,
    b: &DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    work.step()?;
    let mut field = |a: &Field, b: &Field| -> Result<(), Failure> {
        if !equal_text(a.name(), b.name(), work)? || a.is_nullable() != b.is_nullable() {
            return Err(invalid(
                "public source shape field name or NULL contract differs",
            ));
        }
        shape(a.data_type(), b.data_type(), work)
    };
    match (a, b) {
        (DataType::Struct(a), DataType::Struct(b)) if a.len() == b.len() => {
            for (a, b) in a.iter().zip(b) {
                field(a, b)?;
            }
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => field(a, b)?,
        (DataType::FixedSizeList(a, n), DataType::FixedSizeList(b, m)) if n == m => field(a, b)?,
        (DataType::Map(a, n), DataType::Map(b, m)) if n == m => field(a, b)?,
        _ if a == b => {}
        _ => {
            return Err(invalid(
                "public source shape differs from its precise provider layout",
            ));
        }
    }
    Ok(())
}

fn units(work: &mut CompileCheckpoints<'_>, mut count: usize) -> Result<(), CompileControlError> {
    work.flush()?;
    while count != 0 {
        let quantum = count.min(256) as u32;
        work.control()
            .checkpoint(CompilePhase::ProviderValidation, quantum)?;
        count -= quantum as usize;
    }
    Ok(())
}
fn creation_error(error: PureProviderCompileError<ConnectorError>) -> Failure {
    match error {
        PureProviderCompileError::Control(cause) => Failure::Control(cause),
        PureProviderCompileError::Provider(cause) => source_error(cause),
    }
}
fn fact_at<'a>(
    source: &'a novarocks_connector_contract::ConnectorSourceSchemaFacts,
    path: &[u32],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a novarocks_connector_contract::ConnectorSourceFieldFacts>, Failure> {
    source
        .field_fact_observed(path, |count| units(work, count))
        .map_err(Failure::Control)
}
fn attribute<'a>(
    fact: &'a novarocks_connector_contract::ConnectorSourceFieldFacts,
    key: &str,
    expected: Option<&str>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let actual = fact
        .attribute_observed(key, |count| {
            units(work, count).map_err(PureProviderCompileError::Control)
        })
        .map_err(creation_error)?;
    let same = match (actual, expected) {
        (Some(actual), Some(expected)) => equal_text(actual, expected, work)?,
        (None, None) => true,
        _ => false,
    };
    if !same {
        return Err(invalid(
            "public source attribute differs from its formal Iceberg author",
        ));
    }
    Ok(())
}

/// Compare typed source facts against the private formal source declaration.
/// Source attributes never establish value identity. Unknown attributes remain
/// retained source facts, while the provider's necessary attributes are exact.
pub(super) fn source_facts(
    column: &IcebergColumnHandle,
    declaration: Option<&NestedField>,
    input_work: usize,
    source: &novarocks_connector_contract::ConnectorSourceSchemaFacts,
    root_path: &[u32],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    let mut path = [0u32; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 1];
    path[..root_path.len()].copy_from_slice(root_path);
    if let Some(declaration) = declaration {
        return formal_field(
            declaration,
            source,
            &mut path,
            root_path.len(),
            input_work,
            work,
        );
    }
    let fact =
        fact_at(source, root_path, work)?.ok_or_else(|| invalid("missing public source fact"))?;
    if fact.field_id() != Some(column.base_field_id()) || fact.initial_default().is_some() {
        return Err(invalid("synthetic source field ID/default differs"));
    }
    for key in ["doc", "ARROW:extension:name", "ARROW:extension:metadata"] {
        attribute(fact, key, None, work)?;
    }
    Ok(())
}

fn formal_field(
    field: &NestedField,
    source: &novarocks_connector_contract::ConnectorSourceSchemaFacts,
    path: &mut [u32; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 1],
    depth: usize,
    input_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Failure> {
    if depth > path.len() {
        return Err(invalid(
            "source occurrence depth exceeds its finite grammar",
        ));
    }
    let fact = fact_at(source, &path[..depth], work)?
        .ok_or_else(|| invalid("missing complete public source field fact"))?;
    if fact.field_id() != Some(field.id) {
        return Err(invalid(
            "public source field ID differs from its formal declaration",
        ));
    }
    match (&field.initial_default, fact.initial_default()) {
        (None, None) => {}
        (Some(default), Some(actual)) => {
            units(work, input_work)?;
            let expected = opaque(work, || default.clone().try_into_json(&field.field_type))?
                .map_err(|_| invalid("formal source initial default is invalid"))?;
            units(work, actual.as_json().len())?;
            let actual = opaque(work, || {
                serde_json::from_str::<serde_json::Value>(actual.as_json())
            })?
            .map_err(|_| invalid("public source initial default is invalid"))?;
            if actual != expected {
                return Err(invalid(
                    "public source initial default differs from formal declaration",
                ));
            }
        }
        _ => return Err(invalid("public source initial default presence differs")),
    }
    attribute(fact, "doc", field.doc.as_deref(), work)?;
    let variant = matches!(field.field_type.as_ref(), Type::Variant(_));
    attribute(
        fact,
        "ARROW:extension:name",
        variant.then_some("arrow.parquet.variant"),
        work,
    )?;
    // VariantExtensionType::serialize_metadata returns None in the pinned SDK;
    // Arrow's actual field author removes this key rather than inserting "".
    attribute(fact, "ARROW:extension:metadata", None, work)?;
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => {}
        Type::Struct(fields) => {
            for (ordinal, field) in fields.fields().iter().enumerate() {
                if depth >= path.len() {
                    return Err(invalid("source occurrence exceeds finite depth"));
                }
                path[depth] = u32::try_from(ordinal)
                    .map_err(|_| invalid("source child ordinal exceeds address space"))?;
                formal_field(field, source, path, depth + 1, input_work, work)?;
            }
        }
        Type::List(list) => {
            if depth >= path.len() {
                return Err(invalid("source occurrence exceeds finite depth"));
            }
            path[depth] = 0;
            formal_field(
                &list.element_field,
                source,
                path,
                depth + 1,
                input_work,
                work,
            )?;
        }
        Type::Map(map) => {
            if depth + 1 >= path.len() {
                return Err(invalid("source map occurrence exceeds finite depth"));
            }
            path[depth] = 0;
            if fact_at(source, &path[..depth + 1], work)?.is_some() {
                return Err(invalid(
                    "Iceberg map entries is a physical occurrence without a source field ID",
                ));
            }
            for (ordinal, field) in [&map.key_field, &map.value_field].into_iter().enumerate() {
                path[depth + 1] = ordinal as u32;
                formal_field(field, source, path, depth + 2, input_work, work)?;
            }
        }
    }
    Ok(())
}
