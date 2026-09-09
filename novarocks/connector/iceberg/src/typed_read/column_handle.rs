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

//! Iceberg column identity and column handles.
//!
//! A column handle is the only physical binding this stack uses: a base
//! column's Iceberg field ID, the dereference path of field IDs below it, and
//! the exact Iceberg types of both ends. No ordinal, no name fallback, and no
//! provider-private payload participates in identity, so a rename or a
//! reordering of the table schema cannot silently rebind a read.
//!
//! Column handles are also the key type of every [`TupleDomain`] the Iceberg
//! stack produces, which is why the predicate codec for that key lives here
//! rather than in the handle or split modules that consume it.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::wire::dto;
use novarocks_proto_codec::connector_read::{MAX_JSON_BYTES, MAX_NAME_BYTES};
use novarocks_spi::connector::read_stack::{
    Bound, ColumnHandle, ConnectorValue, ConnectorValueType, Domain, Range, TupleDomain, ValueSet,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::spec::{NestedField, Schema, Type};

/// Maximum nesting depth of a column identity tree, mirroring the wire bound.
pub const MAX_COLUMN_IDENTITY_DEPTH: usize = 64;

/// Maximum number of field IDs in one dereference path, mirroring the wire
/// bound on `IcebergColumnHandle.field_id_path`.
pub const MAX_FIELD_ID_PATH_DEPTH: usize = 64;

/// Maximum number of children one column identity may declare.
pub const MAX_COLUMN_IDENTITY_CHILDREN: usize = 4096;

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

pub(crate) fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

pub(crate) fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

/// The kind of Iceberg type a column identity names.
///
/// Only these four exist: Iceberg has no other type constructor, so the set is
/// closed and every match over it is exhaustive.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnIdentityCategory {
    Primitive,
    Struct,
    Array,
    Map,
}

/// One Iceberg field's identity: its ID, its name, its type category, and its
/// children in schema order.
///
/// Children are ordered because a struct's field order is part of the table
/// schema; an array's single child is its element, and a map's two children
/// are its key and its value, in that order.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnIdentity {
    field_id: i32,
    name: Arc<str>,
    category: ColumnIdentityCategory,
    children: Vec<ColumnIdentity>,
}

impl ColumnIdentity {
    pub fn try_new(
        field_id: i32,
        name: impl AsRef<str>,
        category: ColumnIdentityCategory,
        children: Vec<ColumnIdentity>,
    ) -> Result<Self, ConnectorError> {
        if field_id <= 0 {
            return Err(invalid("iceberg column identity field id must be positive"));
        }
        let name = name.as_ref();
        if name.is_empty() || name.len() > MAX_NAME_BYTES {
            return Err(invalid(
                "iceberg column identity name must be non-empty and bounded",
            ));
        }
        if children.len() > MAX_COLUMN_IDENTITY_CHILDREN {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg column identity child count exceeds the hard limit",
            ));
        }
        // The arity of each category is a table-format fact, not a convention:
        // an array always has exactly one element field and a map always has a
        // key and a value field. Accepting any other shape would let a
        // malformed identity claim a dereference path that cannot exist.
        match category {
            ColumnIdentityCategory::Primitive => {
                if !children.is_empty() {
                    return Err(invalid(
                        "a primitive iceberg column identity must have no children",
                    ));
                }
            }
            ColumnIdentityCategory::Struct => {}
            ColumnIdentityCategory::Array => {
                if children.len() != 1 {
                    return Err(invalid(
                        "an array iceberg column identity must have exactly one element child",
                    ));
                }
            }
            ColumnIdentityCategory::Map => {
                if children.len() != 2 {
                    return Err(invalid(
                        "a map iceberg column identity must have exactly a key and a value child",
                    ));
                }
            }
        }
        Ok(Self {
            field_id,
            name: Arc::from(name),
            category,
            children,
        })
    }

    /// Build the identity of one Iceberg schema field, recursively.
    pub fn from_nested_field(field: &NestedField) -> Result<Self, ConnectorError> {
        Self::from_nested_field_at(field, 1)
    }

    /// Build the identity of one top-level field of a frozen table schema.
    ///
    /// Only top-level fields are looked up: a nested field is reached through
    /// [`IcebergColumnHandle::dereference`], never by pretending it is a base
    /// column of its own.
    pub fn from_schema_field(schema: &Schema, field_id: i32) -> Result<Self, ConnectorError> {
        Self::from_nested_field(top_level_field(schema, field_id)?)
    }

    fn from_nested_field_at(field: &NestedField, depth: usize) -> Result<Self, ConnectorError> {
        if depth > MAX_COLUMN_IDENTITY_DEPTH {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg column identity nesting exceeds the hard limit",
            ));
        }
        let (category, children) = match field.field_type.as_ref() {
            Type::Primitive(_) => (ColumnIdentityCategory::Primitive, Vec::new()),
            Type::Struct(struct_type) => {
                let mut children = Vec::with_capacity(struct_type.fields().len());
                for child in struct_type.fields() {
                    children.push(Self::from_nested_field_at(child.as_ref(), depth + 1)?);
                }
                (ColumnIdentityCategory::Struct, children)
            }
            Type::List(list_type) => (
                ColumnIdentityCategory::Array,
                vec![Self::from_nested_field_at(
                    list_type.element_field.as_ref(),
                    depth + 1,
                )?],
            ),
            Type::Map(map_type) => (
                ColumnIdentityCategory::Map,
                vec![
                    Self::from_nested_field_at(map_type.key_field.as_ref(), depth + 1)?,
                    Self::from_nested_field_at(map_type.value_field.as_ref(), depth + 1)?,
                ],
            ),
        };
        Self::try_new(field.id, &field.name, category, children)
    }

    pub const fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn category(&self) -> ColumnIdentityCategory {
        self.category
    }

    pub fn children(&self) -> &[ColumnIdentity] {
        &self.children
    }

    pub fn to_proto(&self) -> dto::ColumnIdentity {
        dto::ColumnIdentity {
            field_id: self.field_id,
            name: self.name.to_string(),
            category: match self.category {
                ColumnIdentityCategory::Primitive => dto::ColumnIdentityCategory::Primitive,
                ColumnIdentityCategory::Struct => dto::ColumnIdentityCategory::Struct,
                ColumnIdentityCategory::Array => dto::ColumnIdentityCategory::Array,
                ColumnIdentityCategory::Map => dto::ColumnIdentityCategory::Map,
            } as i32,
            children: self.children.iter().map(Self::to_proto).collect(),
        }
    }

    pub fn from_proto(raw: &dto::ColumnIdentity) -> Result<Self, ConnectorError> {
        Self::from_proto_at(raw, 1)
    }

    fn from_proto_at(raw: &dto::ColumnIdentity, depth: usize) -> Result<Self, ConnectorError> {
        if depth > MAX_COLUMN_IDENTITY_DEPTH {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg column identity nesting exceeds the hard limit",
            ));
        }
        let category = dto::ColumnIdentityCategory::try_from(raw.category)
            .map_err(|_| invalid("unknown iceberg column identity category"))?;
        let category = match category {
            dto::ColumnIdentityCategory::Unspecified => {
                return Err(invalid(
                    "iceberg column identity category must be specified",
                ));
            }
            dto::ColumnIdentityCategory::Primitive => ColumnIdentityCategory::Primitive,
            dto::ColumnIdentityCategory::Struct => ColumnIdentityCategory::Struct,
            dto::ColumnIdentityCategory::Array => ColumnIdentityCategory::Array,
            dto::ColumnIdentityCategory::Map => ColumnIdentityCategory::Map,
        };
        let mut children = Vec::with_capacity(raw.children.len());
        for child in &raw.children {
            children.push(Self::from_proto_at(child, depth + 1)?);
        }
        Self::try_new(raw.field_id, &raw.name, category, children)
    }
}

/// The exact facts one Iceberg column handle is built from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergColumnHandleParams {
    pub base_column_identity: ColumnIdentity,
    /// Iceberg type JSON of the base column.
    pub base_type_json: String,
    /// Field IDs dereferenced below the base column, outermost first.
    pub field_id_path: Vec<i32>,
    /// Iceberg type JSON of the projected column the path resolves to.
    pub type_json: String,
    pub nullable: bool,
    pub comment: Option<String>,
}

/// One Iceberg column, identified by field ID rather than by name or ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergColumnHandle {
    base_column_identity: ColumnIdentity,
    base_type_json: Arc<str>,
    field_id_path: Vec<i32>,
    type_json: Arc<str>,
    nullable: bool,
    comment: Option<Arc<str>>,
}

impl IcebergColumnHandle {
    /// Build a handle from already-frozen facts, checking that the dereference
    /// path really resolves to the declared projected type.
    pub fn try_new(params: IcebergColumnHandleParams) -> Result<Self, ConnectorError> {
        let IcebergColumnHandleParams {
            base_column_identity,
            base_type_json,
            field_id_path,
            type_json,
            nullable,
            comment,
        } = params;

        let base_type = parse_type(&base_type_json, "base_type_json")?;
        let projected_type = parse_type(&type_json, "type_json")?;
        if field_id_path.len() > MAX_FIELD_ID_PATH_DEPTH {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg column dereference path exceeds the hard limit",
            ));
        }
        if field_id_path.iter().any(|field_id| *field_id <= 0) {
            return Err(invalid(
                "iceberg column dereference path field ids must be positive",
            ));
        }
        if let Some(comment) = comment.as_deref()
            && comment.len() > MAX_JSON_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "iceberg column comment exceeds the hard limit",
            ));
        }

        // The declared projected type must be exactly what the path resolves
        // to. Without this the handle could claim a type the file can never
        // produce, and the mismatch would only surface as a reader failure.
        let (resolved_type, _) = resolve_field_id_path(&base_type, &field_id_path)?;
        if resolved_type != projected_type {
            return Err(invalid(
                "iceberg column projected type does not match its dereference path",
            ));
        }

        Ok(Self {
            base_column_identity,
            base_type_json: Arc::from(base_type_json.as_str()),
            field_id_path,
            type_json: Arc::from(type_json.as_str()),
            nullable,
            comment: comment.map(|comment| Arc::from(comment.as_str())),
        })
    }

    /// The handle of a whole top-level column of the frozen table schema.
    pub fn base_column(field: &NestedField) -> Result<Self, ConnectorError> {
        let type_json = type_to_json(field.field_type.as_ref())?;
        Self::try_new(IcebergColumnHandleParams {
            base_column_identity: ColumnIdentity::from_nested_field(field)?,
            base_type_json: type_json.clone(),
            field_id_path: Vec::new(),
            type_json,
            nullable: !field.required,
            comment: field.doc.clone(),
        })
    }

    /// The handle of one top-level column of a frozen table schema, by field ID.
    pub fn base_column_of(schema: &Schema, field_id: i32) -> Result<Self, ConnectorError> {
        Self::base_column(top_level_field(schema, field_id)?)
    }

    /// The handle of a nested field reached from this base column.
    ///
    /// A dereference is always relative to the base column, so chaining two
    /// dereferences appends to the same path rather than nesting handles.
    pub fn dereference(&self, field_ids: &[i32]) -> Result<Self, ConnectorError> {
        let base_type = parse_type(&self.base_type_json, "base_type_json")?;
        let mut path = self.field_id_path.clone();
        path.extend_from_slice(field_ids);
        let (resolved_type, optional_on_path) = resolve_field_id_path(&base_type, &path)?;
        Self::try_new(IcebergColumnHandleParams {
            base_column_identity: self.base_column_identity.clone(),
            base_type_json: self.base_type_json.to_string(),
            field_id_path: path,
            type_json: type_to_json(&resolved_type)?,
            // A nested field is nullable when it is optional itself or when any
            // ancestor on the path is, because an absent ancestor materializes
            // the whole subtree as null.
            nullable: self.nullable || optional_on_path,
            comment: None,
        })
    }

    pub const fn base_column_identity(&self) -> &ColumnIdentity {
        &self.base_column_identity
    }

    /// The base column's Iceberg field ID; the identity root of this handle.
    pub const fn base_field_id(&self) -> i32 {
        self.base_column_identity.field_id
    }

    pub fn base_type_json(&self) -> &str {
        &self.base_type_json
    }

    pub fn field_id_path(&self) -> &[i32] {
        &self.field_id_path
    }

    /// Whether this handle names a whole base column rather than a nested field.
    pub fn is_base_column(&self) -> bool {
        self.field_id_path.is_empty()
    }

    pub fn type_json(&self) -> &str {
        &self.type_json
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn to_proto(&self) -> dto::IcebergColumnHandle {
        dto::IcebergColumnHandle {
            base_column_identity: Some(self.base_column_identity.to_proto()),
            base_type_json: self.base_type_json.to_string(),
            field_id_path: self.field_id_path.clone(),
            type_json: self.type_json.to_string(),
            nullable: self.nullable,
            comment: self.comment.as_ref().map(|comment| comment.to_string()),
        }
    }

    pub fn from_proto(raw: &dto::IcebergColumnHandle) -> Result<Self, ConnectorError> {
        let identity = raw
            .base_column_identity
            .as_ref()
            .ok_or_else(|| invalid("iceberg column handle requires a base column identity"))?;
        Self::try_new(IcebergColumnHandleParams {
            base_column_identity: ColumnIdentity::from_proto(identity)?,
            base_type_json: raw.base_type_json.clone(),
            field_id_path: raw.field_id_path.clone(),
            type_json: raw.type_json.clone(),
            nullable: raw.nullable,
            comment: raw.comment.clone(),
        })
    }
}

impl ColumnHandle for IcebergColumnHandle {}

impl Ord for IcebergColumnHandle {
    /// Canonical column order: base field ID, then the dereference path.
    ///
    /// Field IDs are the only stable identity Iceberg gives a column, so
    /// ordering by them keeps every `TupleDomain` and projected-column set
    /// deterministic across renames and schema reorderings. The remaining
    /// fields only break ties so the order stays total and consistent with
    /// equality.
    fn cmp(&self, other: &Self) -> Ordering {
        self.base_column_identity
            .field_id
            .cmp(&other.base_column_identity.field_id)
            .then_with(|| self.field_id_path.cmp(&other.field_id_path))
            .then_with(|| self.base_column_identity.cmp(&other.base_column_identity))
            .then_with(|| self.base_type_json.cmp(&other.base_type_json))
            .then_with(|| self.type_json.cmp(&other.type_json))
            .then_with(|| self.nullable.cmp(&other.nullable))
            .then_with(|| self.comment.cmp(&other.comment))
    }
}

impl PartialOrd for IcebergColumnHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Resolve one top-level field of a frozen table schema.
fn top_level_field(schema: &Schema, field_id: i32) -> Result<&NestedField, ConnectorError> {
    schema
        .as_struct()
        .field_by_id(field_id)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            corrupt(format!(
                "iceberg field id {field_id} is not a top-level field of the frozen table schema"
            ))
        })
}

pub(crate) fn parse_type(json: &str, what: &'static str) -> Result<Type, ConnectorError> {
    if json.is_empty() {
        return Err(invalid(format!("iceberg column {what} must not be empty")));
    }
    if json.len() > MAX_JSON_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("iceberg column {what} exceeds the hard limit"),
        ));
    }
    serde_json::from_str::<Type>(json).map_err(|error| {
        invalid(format!(
            "iceberg column {what} is not a valid type: {error}"
        ))
    })
}

pub(crate) fn type_to_json(value: &Type) -> Result<String, ConnectorError> {
    serde_json::to_string(value)
        .map_err(|error| invalid(format!("iceberg type cannot be serialized: {error}")))
}

/// Walk a dereference path and return the resolved type together with whether
/// any field on the path is optional.
fn resolve_field_id_path(
    base_type: &Type,
    field_id_path: &[i32],
) -> Result<(Type, bool), ConnectorError> {
    let mut current = base_type.clone();
    let mut optional_on_path = false;
    for field_id in field_id_path {
        let child: &NestedField = match &current {
            Type::Primitive(_) => {
                return Err(invalid(
                    "iceberg column dereference path descends into a primitive type",
                ));
            }
            Type::Struct(struct_type) => struct_type
                .field_by_id(*field_id)
                .ok_or_else(|| {
                    invalid(format!(
                        "iceberg column dereference path field id {field_id} is not a struct field"
                    ))
                })?
                .as_ref(),
            Type::List(list_type) => {
                if list_type.element_field.id != *field_id {
                    return Err(invalid(format!(
                        "iceberg column dereference path field id {field_id} is not the list element"
                    )));
                }
                list_type.element_field.as_ref()
            }
            Type::Map(map_type) => {
                if map_type.key_field.id == *field_id {
                    map_type.key_field.as_ref()
                } else if map_type.value_field.id == *field_id {
                    map_type.value_field.as_ref()
                } else {
                    return Err(invalid(format!(
                        "iceberg column dereference path field id {field_id} is not a map key or value"
                    )));
                }
            }
        };
        optional_on_path = optional_on_path || !child.required;
        current = child.field_type.as_ref().clone();
    }
    Ok((current, optional_on_path))
}

// ---------------------------------------------------------------------------
// Predicates keyed by Iceberg column handles
// ---------------------------------------------------------------------------

/// Encode a tuple domain keyed by Iceberg column handles.
///
/// Encoding cannot fail: every handle and every value in the domain was
/// validated when it was built.
pub fn encode_tuple_domain(domain: &TupleDomain<IcebergColumnHandle>) -> dto::TupleDomain {
    match domain.domains() {
        None => dto::TupleDomain {
            none: true,
            column_domains: Vec::new(),
        },
        Some(domains) => dto::TupleDomain {
            none: false,
            column_domains: domains
                .iter()
                .map(|(column, domain)| dto::ColumnDomain {
                    column: Some(column.to_proto()),
                    domain: Some(encode_domain(domain)),
                })
                .collect(),
        },
    }
}

/// Decode a tuple domain keyed by Iceberg column handles.
pub fn decode_tuple_domain(
    raw: &dto::TupleDomain,
) -> Result<TupleDomain<IcebergColumnHandle>, ConnectorError> {
    if raw.none {
        if !raw.column_domains.is_empty() {
            return Err(invalid(
                "an unsatisfiable tuple domain must carry no column domains",
            ));
        }
        return Ok(TupleDomain::none());
    }
    let mut domains = BTreeMap::new();
    for entry in &raw.column_domains {
        let column = entry
            .column
            .as_ref()
            .ok_or_else(|| invalid("column domain requires a column handle"))?;
        let column = IcebergColumnHandle::from_proto(column)?;
        let domain = entry
            .domain
            .as_ref()
            .ok_or_else(|| invalid("column domain requires a domain"))?;
        let domain = decode_domain(domain)?;
        if domains.insert(column, domain).is_some() {
            return Err(invalid("tuple domain contains a duplicate column"));
        }
    }
    TupleDomain::with_column_domains(domains)
}

fn encode_domain(domain: &Domain) -> dto::Domain {
    dto::Domain {
        values: Some(encode_value_set(domain.values())),
        null_allowed: domain.null_allowed(),
    }
}

fn decode_domain(raw: &dto::Domain) -> Result<Domain, ConnectorError> {
    let values = raw
        .values
        .as_ref()
        .ok_or_else(|| invalid("domain requires a value set"))?;
    Ok(Domain::new(decode_value_set(values)?, raw.null_allowed))
}

fn encode_value_type(value_type: ConnectorValueType) -> dto::ValueType {
    let (kind, decimal_precision, decimal_scale, fixed_length) = match value_type {
        ConnectorValueType::Boolean => (dto::ValueTypeKind::Boolean, None, None, None),
        ConnectorValueType::TinyInt => (dto::ValueTypeKind::TinyInt, None, None, None),
        ConnectorValueType::SmallInt => (dto::ValueTypeKind::SmallInt, None, None, None),
        ConnectorValueType::Integer => (dto::ValueTypeKind::Integer, None, None, None),
        ConnectorValueType::BigInt => (dto::ValueTypeKind::BigInt, None, None, None),
        ConnectorValueType::Real => (dto::ValueTypeKind::Real, None, None, None),
        ConnectorValueType::Double => (dto::ValueTypeKind::Double, None, None, None),
        ConnectorValueType::Decimal { precision, scale } => (
            dto::ValueTypeKind::Decimal,
            Some(u32::from(precision)),
            Some(i32::from(scale)),
            None,
        ),
        ConnectorValueType::Date => (dto::ValueTypeKind::Date, None, None, None),
        ConnectorValueType::TimeMicros => (dto::ValueTypeKind::TimeMicros, None, None, None),
        ConnectorValueType::TimestampMicros => {
            (dto::ValueTypeKind::TimestampMicros, None, None, None)
        }
        ConnectorValueType::TimestampMillis => {
            (dto::ValueTypeKind::TimestampMillis, None, None, None)
        }
        ConnectorValueType::TimestampTzMicros => {
            (dto::ValueTypeKind::TimestampTzMicros, None, None, None)
        }
        ConnectorValueType::TimestampNanos => {
            (dto::ValueTypeKind::TimestampNanos, None, None, None)
        }
        ConnectorValueType::TimestampTzNanos => {
            (dto::ValueTypeKind::TimestampTzNanos, None, None, None)
        }
        ConnectorValueType::Varchar => (dto::ValueTypeKind::Varchar, None, None, None),
        ConnectorValueType::Varbinary => (dto::ValueTypeKind::Varbinary, None, None, None),
        ConnectorValueType::Uuid => (dto::ValueTypeKind::Uuid, None, None, None),
        ConnectorValueType::Fixed { length } => {
            (dto::ValueTypeKind::Fixed, None, None, Some(length))
        }
        ConnectorValueType::NonComparable => (dto::ValueTypeKind::NonComparable, None, None, None),
    };
    dto::ValueType {
        kind: kind as i32,
        decimal_precision,
        decimal_scale,
        fixed_length,
    }
}

fn decode_value_type(raw: &dto::ValueType) -> Result<ConnectorValueType, ConnectorError> {
    let kind = dto::ValueTypeKind::try_from(raw.kind)
        .map_err(|_| invalid("unknown iceberg predicate value type"))?;
    match kind {
        dto::ValueTypeKind::Unspecified => {
            Err(invalid("iceberg predicate value type must be specified"))
        }
        dto::ValueTypeKind::Decimal => {
            let precision = raw
                .decimal_precision
                .ok_or_else(|| invalid("decimal precision is required"))?;
            let scale = raw
                .decimal_scale
                .ok_or_else(|| invalid("decimal scale is required"))?;
            if precision == 0 || precision > 38 || scale < 0 || scale > precision as i32 {
                return Err(invalid("decimal precision or scale is out of range"));
            }
            if raw.fixed_length.is_some() {
                return Err(invalid("decimal type must not carry fixed length"));
            }
            Ok(ConnectorValueType::Decimal {
                precision: precision as u8,
                scale: scale as i8,
            })
        }
        dto::ValueTypeKind::Fixed => {
            let length = raw
                .fixed_length
                .ok_or_else(|| invalid("fixed type length is required"))?;
            if length == 0 || length > 64 * 1024 {
                return Err(invalid("fixed type length is out of range"));
            }
            if raw.decimal_precision.is_some() || raw.decimal_scale.is_some() {
                return Err(invalid("fixed type must not carry decimal parameters"));
            }
            Ok(ConnectorValueType::Fixed { length })
        }
        simple => {
            if raw.decimal_precision.is_some()
                || raw.decimal_scale.is_some()
                || raw.fixed_length.is_some()
            {
                return Err(invalid("simple value type carries unrelated parameters"));
            }
            Ok(match simple {
                dto::ValueTypeKind::Boolean => ConnectorValueType::Boolean,
                dto::ValueTypeKind::TinyInt => ConnectorValueType::TinyInt,
                dto::ValueTypeKind::SmallInt => ConnectorValueType::SmallInt,
                dto::ValueTypeKind::Integer => ConnectorValueType::Integer,
                dto::ValueTypeKind::BigInt => ConnectorValueType::BigInt,
                dto::ValueTypeKind::Real => ConnectorValueType::Real,
                dto::ValueTypeKind::Double => ConnectorValueType::Double,
                dto::ValueTypeKind::Date => ConnectorValueType::Date,
                dto::ValueTypeKind::TimeMicros => ConnectorValueType::TimeMicros,
                dto::ValueTypeKind::TimestampMicros => ConnectorValueType::TimestampMicros,
                dto::ValueTypeKind::TimestampMillis => ConnectorValueType::TimestampMillis,
                dto::ValueTypeKind::TimestampTzMicros => ConnectorValueType::TimestampTzMicros,
                dto::ValueTypeKind::TimestampNanos => ConnectorValueType::TimestampNanos,
                dto::ValueTypeKind::TimestampTzNanos => ConnectorValueType::TimestampTzNanos,
                dto::ValueTypeKind::Varchar => ConnectorValueType::Varchar,
                dto::ValueTypeKind::Varbinary => ConnectorValueType::Varbinary,
                dto::ValueTypeKind::Uuid => ConnectorValueType::Uuid,
                dto::ValueTypeKind::NonComparable => ConnectorValueType::NonComparable,
                dto::ValueTypeKind::Unspecified
                | dto::ValueTypeKind::Decimal
                | dto::ValueTypeKind::Fixed => unreachable!("handled above"),
            })
        }
    }
}

fn encode_value(value: &ConnectorValue) -> dto::Value {
    let value = match value {
        ConnectorValue::Boolean(value) => dto::value::Value::Boolean(*value),
        ConnectorValue::TinyInt(value) => dto::value::Value::TinyInt(i32::from(*value)),
        ConnectorValue::SmallInt(value) => dto::value::Value::SmallInt(i32::from(*value)),
        ConnectorValue::Integer(value) => dto::value::Value::Integer(*value),
        ConnectorValue::BigInt(value) => dto::value::Value::BigInt(*value),
        ConnectorValue::Real(value) => dto::value::Value::Real(*value),
        ConnectorValue::Double(value) => dto::value::Value::DoubleValue(*value),
        ConnectorValue::Decimal {
            unscaled,
            precision,
            scale,
        } => dto::value::Value::Decimal(dto::DecimalValue {
            unscaled: unscaled.to_be_bytes().to_vec(),
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        ConnectorValue::Date(value) => dto::value::Value::Date(*value),
        ConnectorValue::TimeMicros(value) => dto::value::Value::TimeMicros(*value),
        ConnectorValue::TimestampMicros(value) => dto::value::Value::TimestampMicros(*value),
        ConnectorValue::TimestampMillis(value) => dto::value::Value::TimestampMillis(*value),
        ConnectorValue::TimestampTzMicros(value) => dto::value::Value::TimestampTzMicros(*value),
        ConnectorValue::TimestampNanos(value) => dto::value::Value::TimestampNanos(*value),
        ConnectorValue::TimestampTzNanos(value) => dto::value::Value::TimestampTzNanos(*value),
        ConnectorValue::Varchar(value) => dto::value::Value::Varchar(value.to_string()),
        ConnectorValue::Varbinary(value) => dto::value::Value::Varbinary(value.to_vec()),
        ConnectorValue::Uuid(value) => dto::value::Value::Uuid(value.to_vec()),
        ConnectorValue::Fixed(value) => dto::value::Value::Fixed(value.to_vec()),
    };
    dto::Value { value: Some(value) }
}

fn decode_value(
    raw: &dto::Value,
    expected: ConnectorValueType,
) -> Result<ConnectorValue, ConnectorError> {
    let raw = raw
        .value
        .as_ref()
        .ok_or_else(|| invalid("predicate value is required"))?;
    let value = match raw {
        dto::value::Value::Boolean(value) => ConnectorValue::Boolean(*value),
        dto::value::Value::TinyInt(value) => ConnectorValue::TinyInt(
            i8::try_from(*value).map_err(|_| invalid("tiny integer is out of range"))?,
        ),
        dto::value::Value::SmallInt(value) => ConnectorValue::SmallInt(
            i16::try_from(*value).map_err(|_| invalid("small integer is out of range"))?,
        ),
        dto::value::Value::Integer(value) => ConnectorValue::Integer(*value),
        dto::value::Value::BigInt(value) => ConnectorValue::BigInt(*value),
        dto::value::Value::Real(value) => ConnectorValue::Real(*value),
        dto::value::Value::DoubleValue(value) => ConnectorValue::Double(*value),
        dto::value::Value::Decimal(value) => {
            let unscaled: [u8; 16] = value
                .unscaled
                .as_slice()
                .try_into()
                .map_err(|_| invalid("decimal unscaled value must contain 16 bytes"))?;
            ConnectorValue::try_decimal(
                i128::from_be_bytes(unscaled),
                u8::try_from(value.precision)
                    .map_err(|_| invalid("decimal precision is out of range"))?,
                i8::try_from(value.scale).map_err(|_| invalid("decimal scale is out of range"))?,
            )?
        }
        dto::value::Value::Date(value) => ConnectorValue::Date(*value),
        dto::value::Value::TimeMicros(value) => ConnectorValue::TimeMicros(*value),
        dto::value::Value::TimestampMicros(value) => ConnectorValue::TimestampMicros(*value),
        dto::value::Value::TimestampMillis(value) => ConnectorValue::TimestampMillis(*value),
        dto::value::Value::TimestampTzMicros(value) => ConnectorValue::TimestampTzMicros(*value),
        dto::value::Value::TimestampNanos(value) => ConnectorValue::TimestampNanos(*value),
        dto::value::Value::TimestampTzNanos(value) => ConnectorValue::TimestampTzNanos(*value),
        dto::value::Value::Varchar(value) => ConnectorValue::Varchar(Arc::from(value.as_str())),
        dto::value::Value::Varbinary(value) => {
            ConnectorValue::Varbinary(Arc::from(value.as_slice()))
        }
        dto::value::Value::Uuid(value) => {
            let uuid: [u8; 16] = value
                .as_slice()
                .try_into()
                .map_err(|_| invalid("UUID value must contain 16 bytes"))?;
            ConnectorValue::Uuid(uuid)
        }
        dto::value::Value::Fixed(value) => ConnectorValue::Fixed(Arc::from(value.as_slice())),
    };
    if value.value_type() != expected || value.payload_bytes() > 64 * 1024 {
        return Err(invalid(
            "predicate value does not match its exact declared type",
        ));
    }
    Ok(value)
}

fn encode_value_set(values: &ValueSet) -> dto::ValueSet {
    dto::ValueSet {
        value_type: Some(encode_value_type(values.value_type())),
        ranges: values.ranges().iter().map(encode_range).collect(),
    }
}

fn decode_value_set(raw: &dto::ValueSet) -> Result<ValueSet, ConnectorError> {
    let value_type = raw
        .value_type
        .as_ref()
        .ok_or_else(|| invalid("value set requires its exact type"))?;
    let value_type = decode_value_type(value_type)?;
    let mut ranges = Vec::with_capacity(raw.ranges.len());
    for range in &raw.ranges {
        ranges.push(decode_range(range, value_type)?);
    }
    ValueSet::of_ranges(value_type, ranges)
}

fn encode_range(range: &Range) -> dto::Range {
    dto::Range {
        low: Some(encode_bound(range.low())),
        high: Some(encode_bound(range.high())),
    }
}

fn decode_range(raw: &dto::Range, value_type: ConnectorValueType) -> Result<Range, ConnectorError> {
    let low = raw
        .low
        .as_ref()
        .ok_or_else(|| invalid("range requires a low bound"))?;
    let high = raw
        .high
        .as_ref()
        .ok_or_else(|| invalid("range requires a high bound"))?;
    Range::try_new(
        value_type,
        decode_bound(low, value_type)?,
        decode_bound(high, value_type)?,
    )
}

fn encode_bound(bound: &Bound) -> dto::Bound {
    match bound {
        Bound::Unbounded => dto::Bound {
            kind: dto::BoundKind::Unbounded as i32,
            value: None,
        },
        Bound::Inclusive(value) => dto::Bound {
            kind: dto::BoundKind::Inclusive as i32,
            value: Some(encode_value(value)),
        },
        Bound::Exclusive(value) => dto::Bound {
            kind: dto::BoundKind::Exclusive as i32,
            value: Some(encode_value(value)),
        },
    }
}

fn decode_bound(raw: &dto::Bound, value_type: ConnectorValueType) -> Result<Bound, ConnectorError> {
    let kind =
        dto::BoundKind::try_from(raw.kind).map_err(|_| invalid("unknown range bound kind"))?;
    match kind {
        dto::BoundKind::Unspecified => Err(invalid("range bound kind must be specified")),
        dto::BoundKind::Unbounded => {
            if raw.value.is_some() {
                return Err(invalid("an unbounded range bound must not carry a value"));
            }
            Ok(Bound::Unbounded)
        }
        dto::BoundKind::Inclusive => Ok(Bound::Inclusive(decode_bound_value(raw, value_type)?)),
        dto::BoundKind::Exclusive => Ok(Bound::Exclusive(decode_bound_value(raw, value_type)?)),
    }
}

fn decode_bound_value(
    raw: &dto::Bound,
    value_type: ConnectorValueType,
) -> Result<novarocks_spi::connector::read_stack::ConnectorValue, ConnectorError> {
    let value = raw
        .value
        .as_ref()
        .ok_or_else(|| invalid("a bounded range bound requires a value"))?;
    decode_value(value, value_type)
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc as StdArc;

    use novarocks_spi::connector::read_stack::ConnectorValue;

    use crate::iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType};

    use super::*;

    pub(in crate::typed_read) fn nested_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                StdArc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                StdArc::new(NestedField::optional(
                    2,
                    "info",
                    Type::Struct(StructType::new(vec![
                        StdArc::new(NestedField::required(
                            3,
                            "city",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        StdArc::new(NestedField::optional(
                            4,
                            "zip",
                            Type::Primitive(PrimitiveType::Int),
                        )),
                    ])),
                )),
                StdArc::new(NestedField::optional(
                    5,
                    "tags",
                    Type::List(ListType::new(StdArc::new(NestedField::required(
                        6,
                        "element",
                        Type::Primitive(PrimitiveType::String),
                    )))),
                )),
                StdArc::new(NestedField::optional(
                    7,
                    "props",
                    Type::Map(MapType::new(
                        StdArc::new(NestedField::required(
                            8,
                            "key",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        StdArc::new(NestedField::optional(
                            9,
                            "value",
                            Type::Primitive(PrimitiveType::Long),
                        )),
                    )),
                )),
            ])
            .build()
            .expect("valid iceberg schema")
    }

    pub(in crate::typed_read) fn long_column(
        schema: &Schema,
        field_id: i32,
    ) -> IcebergColumnHandle {
        IcebergColumnHandle::base_column_of(schema, field_id).expect("base column handle")
    }

    #[test]
    fn column_identity_mirrors_the_iceberg_type_category_and_child_order() {
        let schema = nested_schema();
        let info = ColumnIdentity::from_schema_field(&schema, 2).expect("identity");
        assert_eq!(info.field_id(), 2);
        assert_eq!(info.name(), "info");
        assert_eq!(info.category(), ColumnIdentityCategory::Struct);
        assert_eq!(
            info.children()
                .iter()
                .map(ColumnIdentity::field_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        let tags = ColumnIdentity::from_schema_field(&schema, 5).expect("identity");
        assert_eq!(tags.category(), ColumnIdentityCategory::Array);
        assert_eq!(tags.children().len(), 1);

        let props = ColumnIdentity::from_schema_field(&schema, 7).expect("identity");
        assert_eq!(props.category(), ColumnIdentityCategory::Map);
        assert_eq!(
            props
                .children()
                .iter()
                .map(ColumnIdentity::name)
                .collect::<Vec<_>>(),
            vec!["key", "value"]
        );

        let id = ColumnIdentity::from_schema_field(&schema, 1).expect("identity");
        assert_eq!(id.category(), ColumnIdentityCategory::Primitive);
        assert!(id.children().is_empty());
    }

    #[test]
    fn column_identity_rejects_impossible_category_arity() {
        assert!(
            ColumnIdentity::try_new(
                1,
                "c",
                ColumnIdentityCategory::Primitive,
                vec![
                    ColumnIdentity::try_new(2, "x", ColumnIdentityCategory::Primitive, vec![])
                        .expect("child")
                ],
            )
            .is_err()
        );
        assert!(ColumnIdentity::try_new(1, "c", ColumnIdentityCategory::Array, vec![]).is_err());
        assert!(ColumnIdentity::try_new(1, "c", ColumnIdentityCategory::Map, vec![]).is_err());
        assert!(
            ColumnIdentity::try_new(0, "c", ColumnIdentityCategory::Primitive, vec![]).is_err()
        );
        assert!(ColumnIdentity::try_new(1, "", ColumnIdentityCategory::Primitive, vec![]).is_err());
    }

    #[test]
    fn nested_field_id_paths_resolve_to_their_exact_projected_type() {
        let schema = nested_schema();
        let info = IcebergColumnHandle::base_column_of(&schema, 2).expect("base handle");
        assert!(info.is_base_column());

        let city = info.dereference(&[3]).expect("struct dereference");
        assert_eq!(city.field_id_path(), &[3]);
        assert_eq!(city.base_field_id(), 2);
        assert_eq!(
            city.type_json(),
            serde_json::to_string(&Type::Primitive(PrimitiveType::String)).expect("json")
        );
        // `city` is required, but its `info` parent is optional, so the
        // projected column can still materialize as null.
        assert!(city.nullable());

        let tags = IcebergColumnHandle::base_column_of(&schema, 5).expect("base handle");
        let element = tags.dereference(&[6]).expect("list element dereference");
        assert_eq!(element.field_id_path(), &[6]);

        let props = IcebergColumnHandle::base_column_of(&schema, 7).expect("base handle");
        assert!(props.dereference(&[8]).is_ok());
        assert!(props.dereference(&[9]).is_ok());
        assert!(props.dereference(&[99]).is_err());

        let id = IcebergColumnHandle::base_column_of(&schema, 1).expect("base handle");
        assert!(id.dereference(&[3]).is_err());
    }

    #[test]
    fn a_handle_rejects_a_projected_type_that_the_path_cannot_produce() {
        let schema = nested_schema();
        let info = IcebergColumnHandle::base_column_of(&schema, 2).expect("base handle");
        let error = IcebergColumnHandle::try_new(IcebergColumnHandleParams {
            base_column_identity: info.base_column_identity().clone(),
            base_type_json: info.base_type_json().to_string(),
            field_id_path: vec![3],
            type_json: serde_json::to_string(&Type::Primitive(PrimitiveType::Long)).expect("json"),
            nullable: true,
            comment: None,
        })
        .expect_err("mismatched projected type must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn column_handles_order_by_field_id_then_dereference_path() {
        let schema = nested_schema();
        let id = IcebergColumnHandle::base_column_of(&schema, 1).expect("base handle");
        let info = IcebergColumnHandle::base_column_of(&schema, 2).expect("base handle");
        let city = info.dereference(&[3]).expect("dereference");
        let zip = info.dereference(&[4]).expect("dereference");

        let mut ordered = vec![zip.clone(), city.clone(), info.clone(), id.clone()];
        ordered.sort();
        assert_eq!(ordered, vec![id, info, city, zip]);
    }

    #[test]
    fn column_handles_round_trip_through_the_private_wire_value() {
        let schema = nested_schema();
        let info = IcebergColumnHandle::base_column_of(&schema, 2).expect("base handle");
        let city = info.dereference(&[3]).expect("dereference");
        let encoded = city.to_proto();
        let decoded = IcebergColumnHandle::from_proto(&encoded).expect("decoded handle");
        assert_eq!(decoded, city);
    }

    #[test]
    fn tuple_domains_round_trip_and_reject_malformed_wire_shapes() {
        let schema = nested_schema();
        let id = long_column(&schema, 1);
        let mut domains = BTreeMap::new();
        domains.insert(
            id.clone(),
            Domain::new(
                ValueSet::of_ranges(
                    ConnectorValueType::BigInt,
                    vec![
                        Range::try_new(
                            ConnectorValueType::BigInt,
                            Bound::Inclusive(ConnectorValue::BigInt(1)),
                            Bound::Exclusive(ConnectorValue::BigInt(9)),
                        )
                        .expect("range"),
                    ],
                )
                .expect("value set"),
                false,
            ),
        );
        let domain = TupleDomain::with_column_domains(domains).expect("tuple domain");

        let encoded = encode_tuple_domain(&domain);
        assert_eq!(decode_tuple_domain(&encoded).expect("decoded"), domain);

        assert!(decode_tuple_domain(&encode_tuple_domain(&TupleDomain::all())).is_ok());
        let none = encode_tuple_domain(&TupleDomain::<IcebergColumnHandle>::none());
        assert!(none.none);
        assert!(decode_tuple_domain(&none).expect("decoded").is_none());

        let contradictory = dto::TupleDomain {
            none: true,
            column_domains: encoded.column_domains.clone(),
        };
        assert!(decode_tuple_domain(&contradictory).is_err());

        let missing_column = dto::TupleDomain {
            none: false,
            column_domains: vec![dto::ColumnDomain {
                column: None,
                domain: None,
            }],
        };
        assert!(decode_tuple_domain(&missing_column).is_err());
    }

    #[test]
    fn private_value_codec_preserves_smallint_and_timestamp_millis_exactly() {
        for value in [
            ConnectorValue::SmallInt(i16::MIN),
            ConnectorValue::SmallInt(i16::MAX),
            ConnectorValue::TimestampMillis(-1_704_067_200_123),
            ConnectorValue::TimestampMillis(1_704_067_200_123),
        ] {
            let encoded = encode_value(&value);
            let decoded = decode_value(&encoded, value.value_type()).expect("decoded value");
            assert_eq!(decoded, value);
        }

        let out_of_range = dto::Value {
            value: Some(dto::value::Value::SmallInt(i32::from(i16::MAX) + 1)),
        };
        assert!(decode_value(&out_of_range, ConnectorValueType::SmallInt).is_err());
    }

    #[test]
    fn an_unspecified_wire_enum_is_never_accepted() {
        let schema = nested_schema();
        let id = long_column(&schema, 1);
        let mut raw = id.to_proto();
        if let Some(identity) = raw.base_column_identity.as_mut() {
            identity.category = dto::ColumnIdentityCategory::Unspecified as i32;
        }
        assert!(IcebergColumnHandle::from_proto(&raw).is_err());

        let bad_bound = dto::Bound {
            kind: dto::BoundKind::Unspecified as i32,
            value: None,
        };
        assert!(decode_bound(&bad_bound, ConnectorValueType::BigInt).is_err());
    }
}
