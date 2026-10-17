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
use std::collections::{BTreeMap, HashMap, HashSet};

const PREFIX: &str = "novarocks.logical_type.";
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

fn corrupt(message: &'static str) -> ConnectorError {
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
/// The exact LoadedTable metadata properties author the declaration. Names
/// resolve against that same current schema, and only stable IDs are retained.
pub(crate) fn from_metadata(
    metadata: &TableMetadata,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    let declarations = from_properties(metadata.current_schema(), metadata.properties())?;
    for schema in metadata.schemas_iter() {
        validate_schema(schema, &declarations)?;
    }
    for id in declarations.keys() {
        if !metadata
            .schemas_iter()
            .any(|schema| schema.field_by_id(*id).is_some())
        {
            return Err(corrupt(
                "Iceberg nominal declaration names no retained source field",
            ));
        }
    }
    Ok(declarations)
}

pub(crate) fn from_properties(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<NominalSourceDeclarations, ConnectorError> {
    let mut declarations = match properties.get(PROPERTY) {
        Some(raw) => decode(raw)?,
        None => BTreeMap::new(),
    };
    let mut names = HashSet::new();
    let mut bytes = 0usize;
    for (key, value) in properties {
        if reserved_property(key) && key != PROPERTY {
            return Err(corrupt(
                "Iceberg nominal declaration version is unsupported",
            ));
        }
        let Some(name) = key.strip_prefix(PREFIX) else {
            continue;
        };
        bytes = bytes
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()))
            .ok_or_else(|| corrupt("Iceberg nominal declaration extent exceeds address space"))?;
        if bytes > MAX_DECLARATION_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg nominal declarations exceed the hard limit",
            ));
        }
        let value = value.to_ascii_lowercase();
        if !matches!(value.as_str(), "hll" | "bitmap" | "largeint") {
            // Other existing property authors (integer/date/decimal) retain
            // their own exact contracts; this leaf grants no such domain.
            continue;
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(corrupt(
                "Iceberg nominal source declaration has ambiguous names",
            ));
        }
        let mut candidates = schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.name.eq_ignore_ascii_case(name));
        let field = candidates
            .next()
            .ok_or_else(|| corrupt("Iceberg nominal declaration names no current source field"))?;
        if candidates.next().is_some() {
            return Err(corrupt(
                "Iceberg nominal source declaration has ambiguous names",
            ));
        }
        let domain = NominalSourceDomain::parse(&value)?;
        domain.validate_storage(field.field_type.as_ref())?;
        if let Some(previous) = declarations.insert(field.id, domain)
            && previous != domain
        {
            return Err(corrupt(
                "Iceberg field-ID nominal declaration conflicts with its legacy declaration",
            ));
        }
        if declarations.len() > MAX_FIELDS {
            return Err(corrupt(
                "Iceberg nominal source declarations exceed the hard limit",
            ));
        }
    }
    validate_schema(schema, &declarations)?;
    Ok(declarations)
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
            domain.validate_storage(field.field_type.as_ref())?;
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    fields: Vec<Entry>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: i32,
    domain: NominalSourceDomain,
}
fn decode(raw: &str) -> Result<NominalSourceDeclarations, ConnectorError> {
    if raw.len() > MAX_DECLARATION_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg nominal declarations exceed the hard limit",
        ));
    }
    let document: Document = serde_json::from_str(raw)
        .map_err(|_| corrupt("Iceberg nominal declarations have an invalid document"))?;
    if document.version != 1 || document.fields.len() > MAX_FIELDS {
        return Err(corrupt(
            "Iceberg nominal declaration version or count is invalid",
        ));
    }
    let mut declarations = BTreeMap::new();
    for entry in document.fields {
        if entry.id <= 0 || declarations.insert(entry.id, entry.domain).is_some() {
            return Err(corrupt(
                "Iceberg nominal declarations have duplicate or invalid field IDs",
            ));
        }
    }
    Ok(declarations)
}
pub(crate) fn encode(declarations: &NominalSourceDeclarations) -> Result<String, ConnectorError> {
    if declarations.len() > MAX_FIELDS || declarations.keys().any(|id| *id <= 0) {
        return Err(corrupt(
            "Iceberg nominal source declarations have invalid field IDs or count",
        ));
    }
    let document = Document {
        version: 1,
        fields: declarations
            .iter()
            .map(|(id, domain)| Entry {
                id: *id,
                domain: *domain,
            })
            .collect(),
    };
    let raw = serde_json::to_string(&document)
        .map_err(|_| corrupt("Iceberg nominal declarations cannot be encoded"))?;
    if raw.len() > MAX_DECLARATION_BYTES {
        return Err(corrupt(
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
            return Err(corrupt(
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
            NR_LOGICAL_TYPE_KEY.into(),
            logical
                .metadata_value()
                .expect("explicit nominal domain")
                .into(),
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
        let properties = HashMap::from([
            ("novarocks.logical_type.h".into(), "hll".into()),
            ("novarocks.logical_type.n".into(), "largeint".into()),
        ]);
        assert_eq!(
            from_properties(&schema, &properties).unwrap(),
            BTreeMap::from([
                (7, NominalSourceDomain::Hll),
                (8, NominalSourceDomain::LargeInt)
            ])
        );
        for invalid in [
            HashMap::from([("novarocks.logical_type.h".into(), "largeint".into())]),
            HashMap::from([("novarocks.logical_type.absent".into(), "bitmap".into())]),
            HashMap::from([
                ("novarocks.logical_type.h".into(), "hll".into()),
                ("novarocks.logical_type.H".into(), "bitmap".into()),
            ]),
        ] {
            assert!(from_properties(&schema, &invalid).is_err());
        }
    }
}

#[cfg(test)]
#[path = "nominal_declaration/recursive_tests.rs"]
pub(crate) mod recursive_tests;
