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

//! Widen view declarations while retaining their complete value domains and layouts.
use crate::compiler::{SqlCompileControl, SqlCompileError};
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
