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

//! Bind one observed declaration, including when a catalog reuses version IDs
//! after history expiry. This digest grants neither a lease nor lookup authority.

use crate::iceberg::spec::{
    NestedField, PrimitiveType, Schema, Type, ViewMetadata, ViewRepresentation, ViewVersion,
};
use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::{ConnectorError, read_stack::ConnectorReadSchemaCreation};
use sha2::{Digest, Sha256};
use std::sync::Arc;

type Failure = PureProviderCompileError<ConnectorError>;

pub(super) fn observed(
    metadata: &ViewMetadata,
    version: &ViewVersion,
    schema: &Schema,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<Arc<str>, Failure> {
    // SDK Schema serialization consumes a clone and emits identifier IDs in
    // HashSet iteration order. Observe the borrowed declaration directly and
    // fund the only sorting storage through the original creator.
    let mut identifiers = creation.reserve::<i32>(schema.identifier_field_ids().len())?;
    identifiers.extend(schema.identifier_field_ids());
    creation.work(identifiers.len())?;
    identifiers.sort_unstable();
    creation.work(identifiers.len())?;
    let mut summary = creation.reserve::<(&String, &String)>(version.summary().len())?;
    summary.extend(version.summary().iter());
    creation.work(summary.len())?;
    summary.sort_by(|left, right| left.0.cmp(right.0));
    creation.work(summary.len())?;
    let mut digest = Sha256::new();
    let mut write = |bytes: &[u8]| -> Result<(), Failure> {
        creation.work(1)?;
        digest.update(bytes);
        Ok(())
    };
    write(b"novarocks-iceberg-view-declaration-v1")?;
    write(metadata.uuid().as_bytes())?;
    write(&metadata.current_version_id().to_le_bytes())?;
    write(&version.schema_id().to_le_bytes())?;
    write(&version.timestamp_ms().to_le_bytes())?;
    match version.default_catalog() {
        Some(catalog) => {
            write(&[1])?;
            text(catalog, &mut write)?;
        }
        None => write(&[0])?,
    }
    write(&(version.default_namespace().iter().count() as u64).to_le_bytes())?;
    for component in version.default_namespace().iter() {
        text(component, &mut write)?;
    }
    write(&(version.representations().iter().count() as u64).to_le_bytes())?;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql) = representation;
        text(&sql.dialect, &mut write)?;
        text(&sql.sql, &mut write)?;
    }
    write(&(summary.len() as u64).to_le_bytes())?;
    for (key, value) in summary {
        text(key, &mut write)?;
        text(value, &mut write)?;
    }
    // These are the original schema's declaration facts, including required
    // bits and field IDs, before SQL view nullability canonicalization. Field
    // docs/default values do not declare a Trino ViewColumn type or SQL promise.
    write(&(identifiers.len() as u64).to_le_bytes())?;
    for id in identifiers {
        write(&id.to_le_bytes())?;
    }
    write(&(schema.as_struct().fields().len() as u64).to_le_bytes())?;
    for field in schema.as_struct().fields() {
        declaration_field(field, &mut write)?;
    }
    Ok(format!("iceberg-view:{:x}", digest.finalize()).into())
}
fn text(value: &str, write: &mut impl FnMut(&[u8]) -> Result<(), Failure>) -> Result<(), Failure> {
    write(&(value.len() as u64).to_le_bytes())?;
    for chunk in value.as_bytes().chunks(1024) {
        write(chunk)?;
    }
    Ok(())
}

fn declaration_field(
    field: &NestedField,
    write: &mut impl FnMut(&[u8]) -> Result<(), Failure>,
) -> Result<(), Failure> {
    write(&field.id.to_le_bytes())?;
    text(&field.name, write)?;
    write(&[u8::from(field.required)])?;
    declaration_type(&field.field_type, write)
}

fn declaration_type(
    ty: &Type,
    write: &mut impl FnMut(&[u8]) -> Result<(), Failure>,
) -> Result<(), Failure> {
    match ty {
        Type::Primitive(primitive) => {
            write(&[0])?;
            // Stable tags identify the actual Iceberg declaration vocabulary;
            // no Arrow or Nova nominal interpretation participates here.
            let tag = match primitive {
                PrimitiveType::Boolean => 0,
                PrimitiveType::Int => 1,
                PrimitiveType::Long => 2,
                PrimitiveType::Float => 3,
                PrimitiveType::Double => 4,
                PrimitiveType::Decimal { .. } => 5,
                PrimitiveType::Date => 6,
                PrimitiveType::Time => 7,
                PrimitiveType::Timestamp => 8,
                PrimitiveType::Timestamptz => 9,
                PrimitiveType::TimestampNs => 10,
                PrimitiveType::TimestamptzNs => 11,
                PrimitiveType::String => 12,
                PrimitiveType::Uuid => 13,
                PrimitiveType::Fixed(_) => 14,
                PrimitiveType::Binary => 15,
            };
            write(&[tag])?;
            match primitive {
                PrimitiveType::Decimal { precision, scale } => {
                    write(&precision.to_le_bytes())?;
                    write(&scale.to_le_bytes())?;
                }
                PrimitiveType::Fixed(width) => write(&width.to_le_bytes())?,
                _ => {}
            }
        }
        Type::Struct(fields) => {
            write(&[1])?;
            write(&(fields.fields().len() as u64).to_le_bytes())?;
            for field in fields.fields() {
                declaration_field(field, write)?;
            }
        }
        Type::List(list) => {
            write(&[2])?;
            declaration_field(&list.element_field, write)?;
        }
        Type::Map(map) => {
            write(&[3])?;
            declaration_field(&map.key_field, write)?;
            declaration_field(&map.value_field, write)?;
        }
        Type::Variant(_) => write(&[4])?,
    }
    Ok(())
}
