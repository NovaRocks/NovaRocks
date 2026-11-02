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

//! Explicit provider property declarations, frozen once into source field IDs.
//! This owner never reads Arrow annotations or derives a domain from bytes.

use crate::iceberg::spec::{PrimitiveType, Schema, TableMetadata, Type};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::ValueLogicalType;
use std::collections::{BTreeMap, HashMap};

pub(crate) const PROPERTY: &str = "novarocks.nominal-types.v1";
const VERSIONED_PREFIX: &str = "novarocks.nominal-types.";
const MAX_FIELDS: usize = 16384;
const MAX_DECLARATION_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum NominalSourceDomain {
    Hll,
    Bitmap,
    LargeInt,
}
pub(crate) type NominalSourceDeclarations = BTreeMap<i32, NominalSourceDomain>;

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
impl NominalSourceDomain {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Hll => "hll",
            Self::Bitmap => "bitmap",
            Self::LargeInt => "largeint",
        }
    }
    pub(crate) fn parse(name: &str) -> Result<Self, ConnectorError> {
        match name {
            "hll" => Ok(Self::Hll),
            "bitmap" => Ok(Self::Bitmap),
            "largeint" => Ok(Self::LargeInt),
            _ => Err(corrupt("Iceberg frozen nominal declaration is invalid")),
        }
    }
    pub(crate) fn logical_type(self) -> ValueLogicalType {
        match self {
            Self::Hll => ValueLogicalType::Hll,
            Self::Bitmap => ValueLogicalType::Bitmap,
            Self::LargeInt => ValueLogicalType::LargeInt,
        }
    }
    pub(crate) fn validate_storage(self, ty: &Type) -> Result<(), ConnectorError> {
        let valid = match self {
            Self::Hll | Self::Bitmap => matches!(ty, Type::Primitive(PrimitiveType::Binary)),
            Self::LargeInt => matches!(ty, Type::Primitive(PrimitiveType::Fixed(16))),
        };
        if valid {
            Ok(())
        } else {
            Err(corrupt(
                "Iceberg nominal declaration conflicts with its authoritative storage type",
            ))
        }
    }
}

// Design: ADR-0182 (docs/adr/ADR-0182-iceberg-recursive-nominal-field-identities.md)
/// The sole durable declaration is decoded once, then projected into the two
/// existing frozen views. Names and Arrow carriers never author a domain.
#[derive(Clone, Debug, Default)]
pub(crate) struct TableNominalDocument {
    pub(crate) nominals: NominalSourceDeclarations,
    pub(crate) integers: crate::scalar_integer_domain::ScalarIntegerDomains,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum DeclaredDomain {
    Hll,
    Bitmap,
    LargeInt,
    TinyInt,
    SmallInt,
}
impl DeclaredDomain {
    fn nominal(self) -> Option<NominalSourceDomain> {
        match self {
            Self::Hll => Some(NominalSourceDomain::Hll),
            Self::Bitmap => Some(NominalSourceDomain::Bitmap),
            Self::LargeInt => Some(NominalSourceDomain::LargeInt),
            _ => None,
        }
    }
    fn integer(self) -> Option<crate::scalar_integer_domain::ScalarIntegerDomain> {
        use crate::scalar_integer_domain::ScalarIntegerDomain as D;
        match self {
            Self::TinyInt => Some(D::Int8),
            Self::SmallInt => Some(D::Int16),
            _ => None,
        }
    }
}
impl TableNominalDocument {
    pub(crate) fn from_metadata(metadata: &TableMetadata) -> Result<Self, ConnectorError> {
        Self::from_metadata_checked(metadata, &|_| Ok(()))
    }
    pub(crate) fn from_metadata_checked<E: From<ConnectorError>>(
        metadata: &TableMetadata,
        check: &dyn Fn(crate::scalar_integer_domain::CheckPoint) -> Result<(), E>,
    ) -> Result<Self, E> {
        use crate::scalar_integer_domain::CheckPoint as C;
        let entries = decode_properties(metadata.properties(), check)?;
        let mut active = BTreeMap::new();
        for (id, domain) in entries {
            check(C::HistoryDomain)?;
            if id > metadata.last_column_id() {
                return Err(corrupt(format!(
                    "Iceberg nominal declaration field ID {id} exceeds last-column-id"
                ))
                .into());
            }
            let mut found = false;
            for schema in metadata.schemas_iter() {
                check(C::HistorySchema)?;
                // Check the real ID lookup, not a second name/ordinal walk.
                check(C::HistoryField)?;
                if let Some(field) = schema.field_by_id(id) {
                    found = true;
                    let valid = match domain {
                        DeclaredDomain::Hll | DeclaredDomain::Bitmap => matches!(
                            field.field_type.as_ref(),
                            Type::Primitive(PrimitiveType::Binary)
                        ),
                        DeclaredDomain::LargeInt => matches!(
                            field.field_type.as_ref(),
                            Type::Primitive(PrimitiveType::Fixed(16))
                        ),
                        DeclaredDomain::TinyInt | DeclaredDomain::SmallInt => matches!(
                            field.field_type.as_ref(),
                            Type::Primitive(PrimitiveType::Int)
                        ),
                    };
                    if !valid {
                        return Err(corrupt(format!("Iceberg nominal declaration field ID {id} conflicts with retained storage type")).into());
                    }
                }
            }
            // A removed historical field below the assigned-ID high water is
            // lawful. It has no active view and is pruned by the next encode.
            if found {
                active.insert(id, domain);
            }
        }
        let document = Self::from_entries(active);
        for schema in metadata.schemas_iter() {
            validate_schema(schema, &document.nominals)?;
            crate::scalar_integer_domain::validate_schema_checked(
                schema,
                &document.integers,
                check,
            )?;
        }
        Ok(document)
    }
    #[cfg(test)]
    pub(crate) fn from_properties_checked<E: From<ConnectorError>>(
        schema: &Schema,
        properties: &HashMap<String, String>,
        check: &dyn Fn(crate::scalar_integer_domain::CheckPoint) -> Result<(), E>,
    ) -> Result<Self, E> {
        let document = Self::from_entries(decode_properties(properties, check)?);
        validate_schema(schema, &document.nominals)?;
        crate::scalar_integer_domain::validate_schema_checked(schema, &document.integers, check)?;
        Ok(document)
    }
    fn from_entries(entries: BTreeMap<i32, DeclaredDomain>) -> Self {
        let mut document = Self::default();
        for (id, domain) in entries {
            if let Some(value) = domain.nominal() {
                document.nominals.insert(id, value);
            }
            if let Some(value) = domain.integer() {
                document.integers.insert(id, value);
            }
        }
        document
    }
    pub(crate) fn from_columns(
        schema: &Schema,
        columns: &[novarocks_spi::connector::ConnectorColumnDefinition],
    ) -> Result<Self, ConnectorError> {
        let nominals = from_columns(schema, columns)?;
        let integers = schema
            .as_struct()
            .fields()
            .iter()
            .zip(columns)
            .filter_map(|(field, column)| {
                use crate::scalar_integer_domain::ScalarIntegerDomain as I;
                use novarocks_spi::connector::ConnectorDataType as D;
                match column.data_type {
                    D::TinyInt => Some((field.id, I::Int8)),
                    D::SmallInt => Some((field.id, I::Int16)),
                    _ => None,
                }
            })
            .collect();
        crate::scalar_integer_domain::validate_schema(schema, &integers)?;
        Ok(Self { nominals, integers })
    }
    pub(crate) fn encode(&self) -> Result<String, ConnectorError> {
        encode_views(&self.nominals, &self.integers)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.nominals.is_empty() && self.integers.is_empty()
    }
}
#[cfg(test)]
pub(crate) fn from_metadata(
    metadata: &TableMetadata,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    Ok(TableNominalDocument::from_metadata(metadata)?.nominals)
}
#[cfg(test)]
pub(crate) fn from_properties(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    Ok(
        TableNominalDocument::from_properties_checked::<ConnectorError>(
            schema,
            properties,
            &|_| Ok(()),
        )?
        .nominals,
    )
}

/// A historical schema may omit a currently declared field. Every occurrence
/// with that stable ID must still satisfy the exact declared storage contract.
pub(crate) fn validate_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<(), ConnectorError> {
    if declarations.len() > MAX_FIELDS || declarations.keys().any(|id| *id <= 0) {
        return Err(corrupt(
            "Iceberg nominal source declarations have invalid field IDs or count",
        ));
    }
    for (id, domain) in declarations {
        if let Some(field) = schema.field_by_id(*id) {
            domain.validate_storage(field.field_type.as_ref()).map_err(|_| corrupt(format!("Iceberg nominal declaration field ID {id} conflicts with its authoritative storage type")))?;
        }
    }
    Ok(())
}

pub(crate) fn restrict_to_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    validate_schema(schema, declarations)?;
    Ok(declarations
        .iter()
        .filter(|(id, _)| schema.field_by_id(**id).is_some())
        .map(|(id, domain)| (*id, *domain))
        .collect())
}

pub(crate) fn validate_frozen_schema(
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<(), ConnectorError> {
    validate_schema(schema, declarations)?;
    for id in declarations.keys() {
        if schema.field_by_id(*id).is_none() {
            return Err(corrupt(
                "Iceberg frozen nominal declaration names no source field",
            ));
        }
    }
    Ok(())
}

/// This versioned provider namespace cannot be set by property mutations.
pub(crate) fn reserved_property(key: &str) -> bool {
    key.starts_with(VERSIONED_PREFIX)
}

#[derive(serde::Serialize)]
struct Document {
    version: u32,
    fields: Vec<Entry>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: i32,
    domain: DeclaredDomain,
}
fn decode_properties<E: From<ConnectorError>>(
    properties: &HashMap<String, String>,
    check: &dyn Fn(crate::scalar_integer_domain::CheckPoint) -> Result<(), E>,
) -> Result<BTreeMap<i32, DeclaredDomain>, E> {
    if properties
        .keys()
        .any(|key| reserved_property(key) && key != PROPERTY)
    {
        return Err(corrupt("Iceberg nominal declaration version is unsupported").into());
    }
    match properties.get(PROPERTY) {
        Some(raw) => decode_checked(raw, check),
        None => Ok(BTreeMap::new()),
    }
}
fn decode_checked<E: From<ConnectorError>>(
    raw: &str,
    check: &dyn Fn(crate::scalar_integer_domain::CheckPoint) -> Result<(), E>,
) -> Result<BTreeMap<i32, DeclaredDomain>, E> {
    use crate::scalar_integer_domain::CheckPoint as C;
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    if raw.len() > MAX_DECLARATION_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg nominal declarations exceed the hard limit",
        )
        .into());
    }
    struct Fields<'a, E> {
        check: &'a dyn Fn(C) -> Result<(), E>,
        failure: &'a std::cell::RefCell<Option<E>>,
    }
    impl<'de, E: From<ConnectorError>> DeserializeSeed<'de> for Fields<'_, E> {
        type Value = BTreeMap<i32, DeclaredDomain>;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            d.deserialize_seq(self)
        }
    }
    impl<'de, E: From<ConnectorError>> Visitor<'de> for Fields<'_, E> {
        type Value = BTreeMap<i32, DeclaredDomain>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a bounded unique field-ID declaration array")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
            let mut fields = BTreeMap::new();
            loop {
                if let Err(original) = (self.check)(C::DecodeEntry) {
                    *self.failure.borrow_mut() = Some(original);
                    return Err(serde::de::Error::custom("declaration traversal stopped"));
                }
                let Some(entry) = input.next_element::<Entry>()? else {
                    break;
                };
                if fields.len() >= MAX_FIELDS {
                    *self.failure.borrow_mut() = Some(
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "Iceberg nominal declarations exceed the hard limit",
                        )
                        .into(),
                    );
                    return Err(serde::de::Error::custom("excessive field IDs"));
                }
                if let Err(original) = (self.check)(C::DeclarationId) {
                    *self.failure.borrow_mut() = Some(original);
                    return Err(serde::de::Error::custom("declaration traversal stopped"));
                }
                if entry.id <= 0 || fields.insert(entry.id, entry.domain).is_some() {
                    return Err(serde::de::Error::custom("duplicate or invalid field IDs"));
                }
            }
            Ok(fields)
        }
    }
    struct Root<'a, E> {
        check: &'a dyn Fn(C) -> Result<(), E>,
        failure: &'a std::cell::RefCell<Option<E>>,
    }
    impl<'de, E: From<ConnectorError>> Visitor<'de> for Root<'_, E> {
        type Value = BTreeMap<i32, DeclaredDomain>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a versioned nominal declaration document")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
            let mut version = None;
            let mut fields = None;
            while let Some(key) = input.next_key::<String>()? {
                match key.as_str() {
                    "version" if version.is_none() => version = Some(input.next_value::<u32>()?),
                    "fields" if fields.is_none() => {
                        fields = Some(input.next_value_seed(Fields {
                            check: self.check,
                            failure: self.failure,
                        })?)
                    }
                    _ => {
                        return Err(serde::de::Error::custom(
                            "unknown or duplicate document field",
                        ));
                    }
                }
            }
            if version != Some(1) {
                return Err(serde::de::Error::custom("unsupported declaration version"));
            }
            fields.ok_or_else(|| serde::de::Error::custom("missing declaration fields"))
        }
    }
    // Preserve the caller's exact cancellation cause across serde's error type.
    let failure = std::cell::RefCell::new(None);
    let mut decoder = serde_json::Deserializer::from_str(raw);
    let decoded = serde::de::Deserializer::deserialize_map(
        &mut decoder,
        Root {
            check,
            failure: &failure,
        },
    );
    if let Some(original) = failure.into_inner() {
        return Err(original);
    }
    let fields =
        decoded.map_err(|_| corrupt("Iceberg nominal declarations have an invalid document"))?;
    decoder
        .end()
        .map_err(|_| corrupt("Iceberg nominal declarations have an invalid document"))?;
    Ok(fields)
}
#[cfg(test)]
fn decode(raw: &str) -> Result<NominalSourceDeclarations, ConnectorError> {
    Ok(
        TableNominalDocument::from_entries(decode_checked::<ConnectorError>(raw, &|_| Ok(()))?)
            .nominals,
    )
}
#[cfg(test)]
pub(crate) fn encode(declarations: &NominalSourceDeclarations) -> Result<String, ConnectorError> {
    encode_views(declarations, &BTreeMap::new())
}
pub(crate) fn encode_views(
    nominals: &NominalSourceDeclarations,
    integers: &crate::scalar_integer_domain::ScalarIntegerDomains,
) -> Result<String, ConnectorError> {
    let mut fields = BTreeMap::new();
    for (id, domain) in nominals {
        fields.insert(
            *id,
            match domain {
                NominalSourceDomain::Hll => DeclaredDomain::Hll,
                NominalSourceDomain::Bitmap => DeclaredDomain::Bitmap,
                NominalSourceDomain::LargeInt => DeclaredDomain::LargeInt,
            },
        );
    }
    for (id, domain) in integers {
        if fields
            .insert(
                *id,
                match domain {
                    crate::scalar_integer_domain::ScalarIntegerDomain::Int8 => {
                        DeclaredDomain::TinyInt
                    }
                    crate::scalar_integer_domain::ScalarIntegerDomain::Int16 => {
                        DeclaredDomain::SmallInt
                    }
                },
            )
            .is_some()
        {
            return Err(corrupt("Iceberg field ID has conflicting nominal domains"));
        }
    }
    if fields.len() > MAX_FIELDS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg nominal declarations exceed the hard limit",
        ));
    }
    if fields.keys().any(|id| *id <= 0) {
        return Err(corrupt(
            "Iceberg nominal source declarations have invalid field IDs",
        ));
    }
    let raw = serde_json::to_string(&Document {
        version: 1,
        fields: fields
            .into_iter()
            .map(|(id, domain)| Entry { id, domain })
            .collect(),
    })
    .map_err(|_| corrupt("Iceberg nominal declarations cannot be encoded"))?;
    if raw.len() > MAX_DECLARATION_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg nominal declarations exceed the hard limit",
        ));
    }
    Ok(raw)
}

/// Only the admitted CREATE/ADD definition authors these domains. Field IDs
/// come from the exact schema being published, including every anonymous role.
pub(crate) fn from_columns(
    schema: &Schema,
    columns: &[novarocks_spi::connector::ConnectorColumnDefinition],
) -> Result<NominalSourceDeclarations, ConnectorError> {
    if schema.as_struct().fields().len() != columns.len() {
        return Err(corrupt("Iceberg nominal definition field count differs"));
    }
    let mut declarations = BTreeMap::new();
    for (field, column) in schema.as_struct().fields().iter().zip(columns) {
        if !field.name.eq_ignore_ascii_case(&column.name) {
            return Err(corrupt("Iceberg nominal definition field name differs"));
        }
        append_definition(field, &column.data_type, &mut declarations, 1)?;
    }
    Ok(declarations)
}
pub(crate) fn append_definition(
    field: &crate::iceberg::spec::NestedField,
    definition: &novarocks_spi::connector::ConnectorDataType,
    declarations: &mut NominalSourceDeclarations,
    depth: usize,
) -> Result<(), ConnectorError> {
    use novarocks_spi::connector::ConnectorDataType as D;
    if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
        return Err(corrupt(
            "Iceberg nominal definition nesting exceeds the hard limit",
        ));
    }
    let domain = match definition {
        D::Hll => Some(NominalSourceDomain::Hll),
        D::Bitmap => Some(NominalSourceDomain::Bitmap),
        D::LargeInt => Some(NominalSourceDomain::LargeInt),
        _ => None,
    };
    if let Some(domain) = domain {
        domain.validate_storage(&field.field_type)?;
        if let Some(previous) = declarations.insert(field.id, domain)
            && previous != domain
        {
            return Err(corrupt(
                "Iceberg nominal definition conflicts with retained field ID",
            ));
        }
        if declarations.len() > MAX_FIELDS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg nominal declarations exceed the hard limit",
            ));
        }
    }
    match (definition, field.field_type.as_ref()) {
        (D::Array(item), Type::List(list)) => {
            append_definition(&list.element_field, item, declarations, depth + 1)?
        }
        (D::Map(key, value), Type::Map(map)) => {
            append_definition(&map.key_field, key, declarations, depth + 1)?;
            append_definition(&map.value_field, value, declarations, depth + 1)?;
        }
        (D::Struct(definitions), Type::Struct(fields)) => {
            if definitions.len() != fields.fields().len() {
                return Err(corrupt("Iceberg nominal definition child count differs"));
            }
            for (definition, field) in definitions.iter().zip(fields.fields()) {
                if !field.name.eq_ignore_ascii_case(&definition.name) {
                    return Err(corrupt("Iceberg nominal definition child name differs"));
                }
                append_definition(field, &definition.data_type, declarations, depth + 1)?;
            }
        }
        (D::Array(_) | D::Map(_, _) | D::Struct(_), _) => {
            return Err(corrupt("Iceberg nominal definition shape differs"));
        }
        _ => {}
    }
    Ok(())
}

/// Outward Arrow annotations reflect the same explicit field-ID declaration.
/// They never feed back into source-domain authority.
pub(crate) fn apply_to_arrow(
    arrow: arrow::datatypes::SchemaRef,
    schema: &Schema,
    declarations: &NominalSourceDeclarations,
) -> Result<arrow::datatypes::SchemaRef, ConnectorError> {
    use std::sync::Arc;
    validate_schema(schema, declarations)?;
    if arrow.fields().len() != schema.as_struct().fields().len() {
        return Err(corrupt("Iceberg nominal source schema arity differs"));
    }
    let fields = arrow
        .fields()
        .iter()
        .zip(schema.as_struct().fields())
        .map(|(arrow, source)| project_field(arrow, source, declarations).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}
fn project_field(
    arrow: &arrow::datatypes::Field,
    source: &crate::iceberg::spec::NestedField,
    declarations: &NominalSourceDeclarations,
) -> Result<arrow::datatypes::Field, ConnectorError> {
    use arrow::datatypes::DataType;
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, field_logical_type};
    use std::sync::Arc;
    if arrow.name() != &source.name || arrow.is_nullable() == source.required {
        return Err(corrupt("Iceberg nominal source field identity differs"));
    }
    let layout = match (source.field_type.as_ref(), arrow.data_type()) {
        (Type::Struct(source), DataType::Struct(fields))
            if source.fields().len() == fields.len() =>
        {
            DataType::Struct(
                fields
                    .iter()
                    .zip(source.fields())
                    .map(|(field, source)| project_field(field, source, declarations).map(Arc::new))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            )
        }
        (Type::List(source), DataType::List(field)) => DataType::List(Arc::new(project_field(
            field,
            &source.element_field,
            declarations,
        )?)),
        (Type::List(source), DataType::LargeList(field)) => DataType::LargeList(Arc::new(
            project_field(field, &source.element_field, declarations)?,
        )),
        (Type::Map(source), DataType::Map(entries, sorted)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(corrupt("Iceberg nominal map source has no entries"));
            };
            if fields.len() != 2 {
                return Err(corrupt("Iceberg nominal map source arity differs"));
            }
            let fields = vec![
                Arc::new(project_field(&fields[0], &source.key_field, declarations)?),
                Arc::new(project_field(
                    &fields[1],
                    &source.value_field,
                    declarations,
                )?),
            ];
            DataType::Map(
                Arc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(fields.into())),
                ),
                *sorted,
            )
        }
        (Type::Primitive(_) | Type::Variant(_), layout) => layout.clone(),
        _ => return Err(corrupt("Iceberg nominal source layout shape differs")),
    };
    let mut projected = arrow.clone().with_data_type(layout);
    let explicit = declarations.get(&source.id).copied();
    let logical = explicit
        .map(NominalSourceDomain::logical_type)
        .unwrap_or_else(|| match source.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Uuid) => ValueLogicalType::Uuid,
            Type::Variant(_) => ValueLogicalType::Variant,
            _ => ValueLogicalType::Physical,
        });
    if let Some(domain) = explicit {
        // The logical domain may support several carriers elsewhere. This
        // provider's SQL read declaration owns one exact storage projection.
        let expected = match domain {
            NominalSourceDomain::Hll | NominalSourceDomain::Bitmap => DataType::Binary,
            NominalSourceDomain::LargeInt => DataType::FixedSizeBinary(16),
        };
        if projected.data_type() != &expected {
            return Err(corrupt("Iceberg nominal source SQL carrier differs"));
        }
    }
    logical
        .validate_carrier(projected.data_type())
        .map_err(|_| corrupt("Iceberg nominal source SQL carrier differs"))?;
    if arrow.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
        && field_logical_type(arrow)
            .map_err(|_| corrupt("Iceberg nominal source annotation is invalid"))?
            != logical
    {
        return Err(corrupt(
            "Iceberg nominal source annotation conflicts with its declaration",
        ));
    }
    if explicit.is_some() {
        projected.metadata_mut().insert(
            NR_LOGICAL_TYPE_KEY,
            logical.metadata_value().expect("explicit nominal domain"),
        );
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::NestedField;
    use std::sync::Arc;
    #[test]
    fn declarations_freeze_exact_ids_and_do_not_promote_equal_width_storage() {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    7,
                    "h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    8,
                    "n",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
            ])
            .build()
            .unwrap();
        assert!(
            from_properties(&schema, &HashMap::new())
                .unwrap()
                .is_empty()
        );
        let properties = HashMap::from([(
            PROPERTY.into(),
            encode(&BTreeMap::from([
                (7, NominalSourceDomain::Hll),
                (8, NominalSourceDomain::LargeInt),
            ]))
            .unwrap(),
        )]);
        assert_eq!(
            from_properties(&schema, &properties).unwrap(),
            BTreeMap::from([
                (7, NominalSourceDomain::Hll),
                (8, NominalSourceDomain::LargeInt)
            ])
        );
        let invalid = HashMap::from([(
            PROPERTY.into(),
            encode(&BTreeMap::from([(7, NominalSourceDomain::LargeInt)])).unwrap(),
        )]);
        let error = from_properties(&schema, &invalid).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.message().contains("7"));
    }
}

#[cfg(test)]
#[path = "nominal_declaration/recursive_tests.rs"]
pub(crate) mod recursive_tests;

#[cfg(test)]
#[path = "nominal_declaration/document_tests.rs"]
mod document_tests;
