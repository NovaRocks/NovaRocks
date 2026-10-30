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

// Checked Nova output projection, preserving the existing Iceberg serializer.
// Original borrowed Iceberg Serialize is deliberately retained (D17).
use crate::iceberg::spec::{NestedField, Schema, TableMetadata, Type};
use crate::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use novarocks_spi::connector::{ConnectorError, ConnectorPayloadRetentionGuard};
use serde::Serialize;
use std::io;
#[path = "raw_defaults.rs"]
mod raw_defaults;

pub(crate) enum Error<E = ConnectorError> {
    Control(E),
    Json,
    Overflow,
    ShapeChanged,
}
impl<E> std::fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Control(_) => "Control",
            Self::Json => "Json",
            Self::Overflow => "Overflow",
            Self::ShapeChanged => "ShapeChanged",
        })
    }
}
impl<E> std::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
pub(crate) trait OriginalScope<E = ConnectorError> {
    fn check_active(&self) -> Result<(), E>;
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard;
}

// Library errors are retained as objects; no arbitrary Display into a String.
// io::Error::from(kind) is allocation-free. Original control error stays here.
struct Writer<'a, E> {
    scope: &'a dyn OriginalScope<E>,
    original: Option<E>,
    length: u64,
    output: Option<&'a mut Vec<u8>>,
    ceiling: u64,
    changed: bool,
}
impl<E> io::Write for Writer<'_, E> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.original.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.scope.check_active() {
            self.original = Some(error);
            // write_all retries Interrupted. Keep the first cause and exit.
            return Err(io::ErrorKind::Other.into());
        }
        let Some(next) = self.length.checked_add(bytes.len() as u64) else {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        };
        if next > self.ceiling {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let Some(output) = &mut self.output {
            output.extend_from_slice(bytes);
        }
        self.length = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn serialize<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
    output: Option<&mut Vec<u8>>,
    ceiling: u64,
) -> Result<u64, Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    let mut writer = Writer {
        scope,
        original: None,
        length: 0,
        output,
        ceiling,
        changed: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.original {
        return Err(Error::Control(error));
    }
    if writer.changed {
        return Err(Error::ShapeChanged);
    }
    result.map_err(|_| Error::Json)?;
    scope.check_active().map_err(Error::Control)?;
    Ok(writer.length)
}
pub(crate) fn count<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
) -> Result<u64, Error<E>> {
    serialize(value, scope, None, u64::MAX)
}
pub(crate) fn exact_json<T: Serialize + ?Sized, E>(
    value: &T,
    length: u64,
    scope: &dyn OriginalScope<E>,
) -> Result<String, Error<E>> {
    let length = usize::try_from(length).map_err(|_| Error::Overflow)?;
    let mut bytes = Vec::with_capacity(length);
    if bytes.capacity() != length {
        return Err(Error::ShapeChanged);
    }
    let actual = serialize(value, scope, Some(&mut bytes), length as u64)?;
    if actual != length as u64 {
        return Err(Error::ShapeChanged);
    }
    // Serializer produces UTF-8; conversion transfers this SAME allocation.
    String::from_utf8(bytes).map_err(|_| Error::ShapeChanged)
}
fn exact_string<E>(text: &str) -> Result<String, Error<E>> {
    let mut output = String::with_capacity(text.len());
    if output.capacity() != text.len() {
        return Err(Error::ShapeChanged);
    }
    output.push_str(text);
    Ok(output)
}
fn children(ty: &Type) -> &[std::sync::Arc<NestedField>] {
    // Struct has a native slice. List/Map are handled separately below.
    match ty {
        Type::Struct(ty) => ty.fields(),
        _ => &[],
    }
}
fn child_count(ty: &Type) -> usize {
    match ty {
        Type::Struct(ty) => ty.fields().len(),
        Type::List(_) => 1,
        Type::Map(_) => 2,
        Type::Primitive(_) | Type::Variant(_) => 0,
    }
}
fn for_children<E>(
    ty: &Type,
    mut visit: impl FnMut(&NestedField) -> Result<(), Error<E>>,
) -> Result<(), Error<E>> {
    match ty {
        Type::Struct(_) => {
            for field in children(ty) {
                visit(field)?;
            }
        }
        Type::List(ty) => visit(&ty.element_field)?,
        Type::Map(ty) => {
            visit(&ty.key_field)?;
            visit(&ty.value_field)?;
        }
        Type::Primitive(_) | Type::Variant(_) => {}
    }
    Ok(())
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Shape {
    pub metadata_json: u64,
}

pub(crate) struct Plan<'a> {
    metadata: &'a TableMetadata,
    schema: &'a Schema,
    shape: Shape,
}
impl<'a> Plan<'a> {
    pub fn inspect<E>(
        metadata: &'a TableMetadata,
        snapshot_schema: &'a Schema,
        scope: &dyn OriginalScope<E>,
    ) -> Result<Self, Error<E>> {
        let metadata_json = count(metadata, scope)?;
        Ok(Self {
            metadata,
            schema: snapshot_schema,
            shape: Shape { metadata_json },
        })
    }
    pub fn for_copies<E>(
        self,
        copies: usize,
        scope: &'a dyn OriginalScope<E>,
    ) -> Result<Checked<'a, E>, Error<E>> {
        scope.check_active().map_err(Error::Control)?;
        Ok(Checked {
            plan: self,
            scope,
            remaining: copies,
        })
    }
}
pub(crate) struct OwnedProjection {
    metadata_json: String,
    schema: IcebergSchemaDef,
    _guard: ConnectorPayloadRetentionGuard,
}
impl OwnedProjection {
    #[cfg(test)]
    pub fn metadata_json(&self) -> &str {
        &self.metadata_json
    }
    #[cfg(test)]
    pub fn schema(&self) -> &IcebergSchemaDef {
        &self.schema
    }
    pub(crate) fn into_guarded_parts(
        self,
    ) -> (String, IcebergSchemaDef, ConnectorPayloadRetentionGuard) {
        (self.metadata_json, self.schema, self._guard)
    }
}
pub(crate) struct Checked<'a, E> {
    plan: Plan<'a>,
    scope: &'a dyn OriginalScope<E>,
    remaining: usize,
}
impl<E> Checked<'_, E> {
    pub fn build_one(&mut self) -> Result<OwnedProjection, Error<E>> {
        self.scope.check_active().map_err(Error::Control)?;
        if self.remaining == 0 {
            return Err(Error::ShapeChanged);
        }
        let guard = self.scope.original_guard(); // Before any own output growth.
        let metadata_json = exact_json(
            self.plan.metadata,
            self.plan.shape.metadata_json,
            self.scope,
        )?;
        let fields = self.plan.schema.as_struct().fields();
        let mut output = Vec::with_capacity(fields.len());
        if output.capacity() != fields.len() {
            return Err(Error::ShapeChanged);
        }
        for field in fields {
            output.push(build_field(field, self.scope)?);
        }
        self.scope.check_active().map_err(Error::Control)?;
        self.remaining -= 1;
        Ok(OwnedProjection {
            metadata_json,
            schema: IcebergSchemaDef { fields: output },
            _guard: guard,
        })
    }
}
fn build_field<E>(
    field: &NestedField,
    scope: &dyn OriginalScope<E>,
) -> Result<IcebergSchemaFieldDef, Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    // Recount without any output graph; there is no per-field recipe Vec.
    let length = count(field, scope)?;
    let (initial_default_json, write_default_json) = {
        let text = exact_json(field, length, scope)?;
        let raw: &serde_json::value::RawValue =
            serde_json::from_str(&text).map_err(|_| Error::Json)?;
        let defaults = raw_defaults::defaults(raw).map_err(|_| Error::Json)?;
        let initial = defaults
            .initial
            .map(|value| exact_string(value.get()))
            .transpose()?;
        let write = defaults
            .write
            .map(|value| exact_string(value.get()))
            .transpose()?;
        (initial, write)
        // text and parser scratch exit BEFORE child recursion.
    };
    let mut output = Vec::with_capacity(child_count(&field.field_type));
    if output.capacity() != child_count(&field.field_type) {
        return Err(Error::ShapeChanged);
    }
    for_children(&field.field_type, |child| {
        output.push(build_field(child, scope)?);
        Ok(())
    })?;
    Ok(IcebergSchemaFieldDef {
        is_variant: matches!(field.field_type.as_ref(), Type::Variant(_)),
        field_id: field.id,
        name: exact_string(&field.name)?,
        // These two are serde-skipped and no production COW default reader
        // consumes them. Canonical typed JSON defaults are retained verbatim.
        initial_default: None,
        write_default: None,
        initial_default_json,
        write_default_json,
        children: output,
    })
}
#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;
