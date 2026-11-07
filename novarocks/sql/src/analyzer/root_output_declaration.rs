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

//! Exact internal root-output targets. Conversion remains with value_conversion.
use crate::{
    analysis::{QueryBody, ResolvedQuery},
    column_id::ColumnRefFactory,
    compiler::{SqlCompileControl, SqlCompileError, SqlFunctionCatalog},
};
use arrow::datatypes::{DataType, Field, FieldRef, UnionFields};
use novarocks_type_contract::{
    CompilePhase, FunctionValueType, PureCompileControl, ValueField, ValueKind, ValueLayout,
    ValueTypeRef, ValueTypeStorageLifetime, ValueUnionField,
};
use std::sync::Arc;

fn step(control: &SqlCompileControl) -> Result<(), SqlCompileError> {
    control
        .checkpoint(CompilePhase::Validate, 1)
        .map_err(Into::into)
}
fn invalid(error: impl std::fmt::Display) -> SqlCompileError {
    SqlCompileError::InvalidRequest(error.to_string())
}
fn compilation_source<E>(error: E) -> SqlCompileError
where
    E: std::error::Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
{
    SqlCompileError::CompilationSource(novarocks_failure_contract::SharedFailureCause::new(error))
}
fn allocation_source(error: std::collections::TryReserveError) -> SqlCompileError {
    compilation_source(
        novarocks_functions::allocation_failure::AllocationRefusal::new(
            "root output declaration",
            error,
        ),
    )
}
fn check_type(ty: &FunctionValueType, control: &SqlCompileControl) -> Result<(), SqlCompileError> {
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = novarocks_functions::validate_function_value_type_observed(ty, &mut work).map_err(
        |error| match error {
            novarocks_functions::KernelFailure::CompileControl(cause) => cause.into(),
            error => compilation_source(error),
        },
    );
    work.finish_result(result, SqlCompileError::from)
}
/// Compare an actual COW root against its exact declaration. Only NULL promises
/// may differ; nominal domains, names, physical widths and parameters stay exact.
pub fn root_output_type_matches(
    actual: &FunctionValueType,
    declared: &FunctionValueType,
    control: &SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    check_type(actual, control)?;
    check_type(declared, control)?;
    Ok(
        semantic_equal(actual.value_type(), declared.value_type(), control)?
            && layout_equal(actual.data_type(), declared.data_type(), control)?,
    )
}
fn semantic_equal(
    a: &ValueTypeRef,
    b: &ValueTypeRef,
    c: &SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    step(c)?;
    Ok(match (a.kind(), b.kind()) {
        (ValueKind::List(a), ValueKind::List(b)) => semantic_equal(a, b, c)?,
        (
            ValueKind::FixedSizeList {
                item: a,
                length: al,
            },
            ValueKind::FixedSizeList {
                item: b,
                length: bl,
            },
        ) => al == bl && semantic_equal(a, b, c)?,
        (ValueKind::Struct(a), ValueKind::Struct(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                step(c)?;
                if a.name() != b.name() || !semantic_equal(a.value_type(), b.value_type(), c)? {
                    return Ok(false);
                }
            }
            true
        }
        (ValueKind::Map { key: ak, value: av }, ValueKind::Map { key: bk, value: bv }) => {
            semantic_equal(ak, bk, c)? && semantic_equal(av, bv, c)?
        }
        (ValueKind::Union(a), ValueKind::Union(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                step(c)?;
                if a.type_id != b.type_id
                    || a.field.name() != b.field.name()
                    || !semantic_equal(a.field.value_type(), b.field.value_type(), c)?
                {
                    return Ok(false);
                }
            }
            true
        }
        (a, b) => a == b,
    })
}
fn fields_equal(
    a: &FieldRef,
    b: &FieldRef,
    c: &SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    step(c)?;
    Ok(a.name() == b.name()
        && a.metadata() == b.metadata()
        && layout_equal(a.data_type(), b.data_type(), c)?)
}
fn layout_equal(
    a: &DataType,
    b: &DataType,
    c: &SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    step(c)?;
    Ok(match (a, b) {
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => fields_equal(a, b, c)?,
        (DataType::FixedSizeList(a, al), DataType::FixedSizeList(b, bl)) => {
            al == bl && fields_equal(a, b, c)?
        }
        (DataType::Struct(a), DataType::Struct(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !fields_equal(a, b, c)? {
                    return Ok(false);
                }
            }
            true
        }
        (DataType::Map(a, al), DataType::Map(b, bl)) => al == bl && fields_equal(a, b, c)?,
        (DataType::Union(a, al), DataType::Union(b, bl)) => {
            if al != bl || a.len() != b.len() {
                return Ok(false);
            }
            for ((ai, a), (bi, b)) in a.iter().zip(b.iter()) {
                if ai != bi || !fields_equal(a, b, c)? {
                    return Ok(false);
                }
            }
            true
        }
        (a, b) => a == b,
    })
}

pub(crate) fn apply_root_output_declaration(
    query: &mut ResolvedQuery,
    factory: &mut ColumnRefFactory,
    declaration: Option<&[FunctionValueType]>,
    functions: &dyn SqlFunctionCatalog,
    semantics: &crate::sql_mode::SqlSemanticSettings,
    policy: novarocks_functions::ConstantPolicy,
    control: &SqlCompileControl,
) -> Result<(), SqlCompileError> {
    let Some(declaration) = declaration else {
        return Ok(());
    };
    step(control)?;
    if declaration.len() != query.output_columns.len() {
        return Err(invalid(
            "root output declaration width differs from the analyzed projection",
        ));
    }
    let QueryBody::Select(select) = &mut query.body else {
        return Err(invalid(
            "root output declarations require an internal SELECT projection",
        ));
    };
    if select.projection.len() != declaration.len() {
        return Err(invalid(
            "root output projection differs from its output declaration",
        ));
    }
    for declared in declaration {
        check_type(declared, control)?;
    }
    let original = std::mem::take(&mut select.projection);
    let mut projection = Vec::new();
    projection
        .try_reserve_exact(original.len())
        .map_err(allocation_source)?;
    for ((mut item, output), declared) in original
        .into_iter()
        .zip(&mut query.output_columns)
        .zip(declaration)
    {
        step(control)?;
        if !root_output_type_matches(&item.expr.value_type, declared, control)? {
            let target = widen_declaration(declared, control)?;
            item.expr = super::value_conversion::cast_to_value_type(
                item.expr,
                target,
                novarocks_parser::Span::new(0, 0),
                functions,
                semantics,
                policy,
                control,
            )
            .map_err(SqlCompileError::from)?;
            // A direct reference may still carry its source ColumnId. Mint a
            // new occurrence instead of mutating the scan's complete binding.
            let id = factory.create(None, item.output_name.clone(), item.expr.value_type.clone());
            let logical = novarocks_types::sql_type_from_value_type(&item.expr.value_type)
                .map_err(invalid)?;
            factory.set_logical_type(id, Some(logical));
            item.output_column_id = id;
            output.column_id = id;
            output.value_type = item.expr.value_type.clone();
        }
        projection.push(item);
    }
    select.projection = projection;
    Ok(())
}

pub(super) fn widen_declaration(
    value: &FunctionValueType,
    control: &SqlCompileControl,
) -> Result<FunctionValueType, SqlCompileError> {
    let (semantic, layout) = widen(value.value_type(), value.data_type(), true, control)?;
    let layout = ValueLayout::try_new(&layout, ValueTypeStorageLifetime::pure())
        .map_err(compilation_source)?;
    FunctionValueType::try_from_value(semantic, layout).map_err(compilation_source)
}
fn relabel(field: &FieldRef, ty: DataType, nullable: bool) -> FieldRef {
    Arc::new(Field::new(field.name(), ty, nullable).with_metadata(field.metadata().clone()))
}
fn widen(
    value: &ValueTypeRef,
    layout: &DataType,
    nullable: bool,
    c: &SqlCompileControl,
) -> Result<(ValueTypeRef, DataType), SqlCompileError> {
    step(c)?;
    let storage = ValueTypeStorageLifetime::pure();
    let (kind, layout) = match (value.kind(), layout) {
        (ValueKind::List(child), DataType::List(field)) => {
            let (v, t) = widen(child, field.data_type(), true, c)?;
            (ValueKind::List(v), DataType::List(relabel(field, t, true)))
        }
        (ValueKind::List(child), DataType::LargeList(field)) => {
            let (v, t) = widen(child, field.data_type(), true, c)?;
            (
                ValueKind::List(v),
                DataType::LargeList(relabel(field, t, true)),
            )
        }
        (ValueKind::List(child), DataType::ListView(field)) => {
            let (v, t) = widen(child, field.data_type(), true, c)?;
            (
                ValueKind::List(v),
                DataType::ListView(relabel(field, t, true)),
            )
        }
        (ValueKind::List(child), DataType::LargeListView(field)) => {
            let (v, t) = widen(child, field.data_type(), true, c)?;
            (
                ValueKind::List(v),
                DataType::LargeListView(relabel(field, t, true)),
            )
        }
        (ValueKind::FixedSizeList { item, length }, DataType::FixedSizeList(field, width))
            if length == width =>
        {
            let (v, t) = widen(item, field.data_type(), true, c)?;
            (
                ValueKind::FixedSizeList {
                    item: v,
                    length: *length,
                },
                DataType::FixedSizeList(relabel(field, t, true), *width),
            )
        }
        (ValueKind::Struct(fields), DataType::Struct(layouts)) => {
            if fields.len() != layouts.len() {
                return Err(invalid("root declaration Struct shape is inconsistent"));
            }
            let mut values = Vec::new();
            let mut types = Vec::new();
            values
                .try_reserve_exact(fields.len())
                .map_err(allocation_source)?;
            types
                .try_reserve_exact(fields.len())
                .map_err(allocation_source)?;
            for (field, layout) in fields.iter().zip(layouts) {
                let (v, t) = widen(field.value_type(), layout.data_type(), true, c)?;
                step(c)?;
                values.push(ValueField::new(field.name(), v, storage.clone()));
                types.push(relabel(layout, t, true));
            }
            (
                ValueKind::Struct(values.into_boxed_slice()),
                DataType::Struct(types.into()),
            )
        }
        (ValueKind::Map { key, value }, DataType::Map(entries, ordered)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("root declaration MAP entries are not a Struct"));
            };
            if fields.len() != 2 {
                return Err(invalid("root declaration MAP has invalid entries width"));
            }
            // MAP keys and the entries wrapper are required structural roles.
            // Key descendants may widen, but its root must remain nonnullable.
            let (k, kt) = widen(key, fields[0].data_type(), false, c)?;
            let (v, vt) = widen(value, fields[1].data_type(), true, c)?;
            let entries_ty = DataType::Struct(
                vec![
                    relabel(&fields[0], kt, false),
                    relabel(&fields[1], vt, true),
                ]
                .into(),
            );
            (
                ValueKind::Map { key: k, value: v },
                DataType::Map(relabel(entries, entries_ty, false), *ordered),
            )
        }
        (ValueKind::Union(fields), DataType::Union(layouts, mode)) => {
            if fields.len() != layouts.len() {
                return Err(invalid("root declaration Union shape is inconsistent"));
            }
            let mut values = Vec::new();
            let mut types = Vec::new();
            let mut ids = Vec::new();
            values
                .try_reserve_exact(fields.len())
                .map_err(allocation_source)?;
            types
                .try_reserve_exact(fields.len())
                .map_err(allocation_source)?;
            ids.try_reserve_exact(fields.len())
                .map_err(allocation_source)?;
            for (field, (id, layout)) in fields.iter().zip(layouts.iter()) {
                if field.type_id != id {
                    return Err(invalid(
                        "root declaration Union identity differs from its layout",
                    ));
                }
                let (v, t) = widen(field.field.value_type(), layout.data_type(), true, c)?;
                values.push(ValueUnionField {
                    type_id: id,
                    field: ValueField::new(field.field.name(), v, storage.clone()),
                });
                types.push(relabel(layout, t, true));
                ids.push(id);
            }
            let fields = UnionFields::try_new(ids, types).map_err(invalid)?;
            (
                ValueKind::Union(values.into_boxed_slice()),
                DataType::Union(fields, *mode),
            )
        }
        (kind, layout) => (kind.clone(), layout.clone()),
    };
    let semantic = ValueTypeRef::try_new(kind, nullable, storage).map_err(compilation_source)?;
    Ok((semantic, layout))
}

#[cfg(test)]
mod tests;
