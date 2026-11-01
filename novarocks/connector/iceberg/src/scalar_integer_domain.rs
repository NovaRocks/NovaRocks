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

//! Provider-private declarations over Iceberg's scalar INT storage carrier.
//! The field-ID property survives rename/drop; it is not an Arrow type override.

use crate::iceberg::spec::{PrimitiveType, Schema, Type};
use arrow::array::{Array, ArrayRef, Int8Builder, Int16Builder, Int32Array};
use arrow::datatypes::{DataType, SchemaRef};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Domain, Range, ValueSet,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;

const MAX_FIELDS: usize = 16384;

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub(crate) enum ScalarIntegerDomain {
    #[serde(rename = "tinyint")]
    Int8,
    #[serde(rename = "smallint")]
    Int16,
}

pub(crate) type ScalarIntegerDomains = BTreeMap<i32, ScalarIntegerDomain>;

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

impl ScalarIntegerDomain {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Int8 => "tinyint",
            Self::Int16 => "smallint",
        }
    }
    pub(crate) fn parse(value: &str) -> Result<Self, ConnectorError> {
        match value {
            "tinyint" => Ok(Self::Int8),
            "smallint" => Ok(Self::Int16),
            _ => Err(corrupt("invalid Iceberg scalar integer domain")),
        }
    }
    pub(crate) fn data_type(self) -> DataType {
        match self {
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
        }
    }
    pub(crate) fn value_type(self) -> ConnectorValueType {
        match self {
            Self::Int8 => ConnectorValueType::TinyInt,
            Self::Int16 => ConnectorValueType::SmallInt,
        }
    }
    pub(crate) fn value(self, value: i32) -> Result<ConnectorValue, ConnectorError> {
        match self {
            Self::Int8 => i8::try_from(value)
                .map(ConnectorValue::TinyInt)
                .map_err(|_| corrupt("Iceberg INT value exceeds declared TINYINT domain")),
            Self::Int16 => i16::try_from(value)
                .map(ConnectorValue::SmallInt)
                .map_err(|_| corrupt("Iceberg INT value exceeds declared SMALLINT domain")),
        }
    }
    /// Convert only a column with an independently frozen declaration. A safe
    /// Arrow cast would silently turn corrupt stored values into NULL.
    pub(crate) fn array(self, source: &ArrayRef) -> Result<ArrayRef, ConnectorError> {
        let input = source
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| corrupt("declared Iceberg scalar integer requires INT32 storage"))?;
        match self {
            Self::Int8 => {
                let mut output = Int8Builder::with_capacity(input.len());
                for value in input.iter() {
                    output.append_option(
                        value
                            .map(|value| {
                                i8::try_from(value).map_err(|_| {
                                    corrupt("Iceberg stored value exceeds declared TINYINT domain")
                                })
                            })
                            .transpose()?,
                    );
                }
                Ok(Arc::new(output.finish()))
            }
            Self::Int16 => {
                let mut output = Int16Builder::with_capacity(input.len());
                for value in input.iter() {
                    output.append_option(
                        value
                            .map(|value| {
                                i16::try_from(value).map_err(|_| {
                                    corrupt("Iceberg stored value exceeds declared SMALLINT domain")
                                })
                            })
                            .transpose()?,
                    );
                }
                Ok(Arc::new(output.finish()))
            }
        }
    }

    /// Metrics use the four-byte storage domain. Rebuild the exact logical
    /// domain before any typed intersection; never clamp supplied bounds.
    pub(crate) fn domain(self, source: &Domain) -> Result<Domain, ConnectorError> {
        if source.value_type() != ConnectorValueType::Integer {
            return Err(corrupt("Iceberg integer metric has a non-INT32 domain"));
        }
        let bound = |bound: &Bound| -> Result<Bound, ConnectorError> {
            let value = |value: &ConnectorValue| match value {
                ConnectorValue::Integer(value) => self.value(*value),
                _ => Err(corrupt("Iceberg integer metric has a non-INT32 bound")),
            };
            match bound {
                Bound::Unbounded => Ok(Bound::Unbounded),
                Bound::Inclusive(v) => Ok(Bound::Inclusive(value(v)?)),
                Bound::Exclusive(v) => Ok(Bound::Exclusive(value(v)?)),
            }
        };
        let ranges = source
            .values()
            .ranges()
            .iter()
            .map(|range| {
                Range::try_new(self.value_type(), bound(range.low())?, bound(range.high())?)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Domain::new(
            ValueSet::of_ranges(self.value_type(), ranges)?,
            source.null_allowed(),
        ))
    }
}

/// Borrowed progress checkpoints. No capacity, deadline, state or guard is minted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckPoint {
    DecodeEntry,
    DeclarationId,
    ValidationId,
    ValidationField,
    ValidationDefault,
    ValidationTopLevel,
    HistoryDomain,
    HistorySchema,
    HistoryField,
}

/// Project the top-level integer view from the sole durable document.
#[cfg(test)]
pub(crate) fn declarations(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<ScalarIntegerDomains, ConnectorError> {
    declarations_checked(schema, properties, &|_| Ok(()))
}
#[cfg(test)]
pub(crate) fn declarations_checked<E: From<ConnectorError>>(
    schema: &Schema,
    properties: &HashMap<String, String>,
    check: &dyn Fn(CheckPoint) -> Result<(), E>,
) -> Result<ScalarIntegerDomains, E> {
    Ok(
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_properties_checked(
            schema, properties, check,
        )?
        .integers,
    )
}
pub(crate) fn metadata_declarations(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<ScalarIntegerDomains, ConnectorError> {
    metadata_declarations_checked(metadata, &|_| Ok(()))
}
pub(crate) fn metadata_declarations_checked<E: From<ConnectorError>>(
    metadata: &crate::iceberg::spec::TableMetadata,
    check: &dyn Fn(CheckPoint) -> Result<(), E>,
) -> Result<ScalarIntegerDomains, E> {
    Ok(
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata_checked(
            metadata, check,
        )?
        .integers,
    )
}

pub(crate) fn validate_schema(
    schema: &Schema,
    domains: &ScalarIntegerDomains,
) -> Result<(), ConnectorError> {
    validate_schema_checked(schema, domains, &|_| Ok(()))
}

pub(crate) fn validate_schema_checked<E: From<ConnectorError>>(
    schema: &Schema,
    domains: &ScalarIntegerDomains,
    check: &dyn Fn(CheckPoint) -> Result<(), E>,
) -> Result<(), E> {
    if domains.len() > MAX_FIELDS {
        return Err(corrupt("invalid Iceberg scalar integer field-ID declaration").into());
    }
    for id in domains.keys() {
        check(CheckPoint::ValidationId)?;
        if *id <= 0 {
            return Err(corrupt("invalid Iceberg scalar integer field-ID declaration").into());
        }
    }
    for field in schema.as_struct().fields() {
        check(CheckPoint::ValidationField)?;
        if let Some(domain) = domains.get(&field.id)
            && field.field_type.as_ref() == &Type::Primitive(PrimitiveType::Int)
        {
            for default in [field.initial_default.as_ref(), field.write_default.as_ref()]
                .into_iter()
                .flatten()
            {
                check(CheckPoint::ValidationDefault)?;
                match default {
                    crate::iceberg::spec::Literal::Primitive(
                        crate::iceberg::spec::PrimitiveLiteral::Int(value),
                    ) => {
                        domain.value(*value)?;
                    }
                    _ => {
                        return Err(corrupt(
                            "Iceberg declared scalar integer default is not an INT32 value",
                        )
                        .into());
                    }
                }
            }
        }
        if domains.contains_key(&field.id)
            && !matches!(
                field.field_type.as_ref(),
                Type::Primitive(PrimitiveType::Int)
            )
        {
            return Err(corrupt(format!(
                "Iceberg scalar integer declaration field ID {} requires top-level INT storage",
                field.id
            ))
            .into());
        }
    }
    // Preserve field_by_id first and original any() short-circuit order.
    for id in domains.keys() {
        check(CheckPoint::ValidationId)?;
        if schema.field_by_id(*id).is_some() {
            let mut top_level = false;
            for field in schema.as_struct().fields() {
                check(CheckPoint::ValidationTopLevel)?;
                if field.id == *id {
                    top_level = true;
                    break;
                }
            }
            if !top_level {
                return Err(corrupt(format!(
                    "Iceberg scalar integer declaration field ID {id} is not top-level"
                ))
                .into());
            }
        }
    }
    Ok(())
}

pub(crate) fn of_schema(
    schema: &Schema,
    domains: &ScalarIntegerDomains,
) -> Result<ScalarIntegerDomains, ConnectorError> {
    validate_schema(schema, domains)?;
    Ok(domains
        .iter()
        .filter(|(id, _)| {
            schema.as_struct().fields().iter().any(|field| {
                field.id == **id
                    && field.field_type.as_ref() == &Type::Primitive(PrimitiveType::Int)
            })
        })
        .map(|(id, d)| (*id, *d))
        .collect())
}

#[cfg(test)]
fn decode(raw: &str) -> Result<ScalarIntegerDomains, ConnectorError> {
    let schema = Schema::builder()
        .build()
        .map_err(|error| corrupt(error.to_string()))?;
    declarations(
        &schema,
        &HashMap::from([(
            crate::schema_mapping::nominal_declaration::PROPERTY.into(),
            raw.into(),
        )]),
    )
}
#[cfg(test)]
fn decode_checked<E: From<ConnectorError>>(
    raw: &str,
    check: &dyn Fn(CheckPoint) -> Result<(), E>,
) -> Result<ScalarIntegerDomains, E> {
    let schema = Schema::builder()
        .build()
        .map_err(|error| corrupt(error.to_string()))?;
    declarations_checked(
        &schema,
        &HashMap::from([(
            crate::schema_mapping::nominal_declaration::PROPERTY.into(),
            raw.into(),
        )]),
        check,
    )
}
#[cfg(test)]
fn encode(domains: &ScalarIntegerDomains) -> Result<String, ConnectorError> {
    crate::schema_mapping::nominal_declaration::encode_views(&BTreeMap::new(), domains)
}

pub(crate) fn metadata_sql_schema(
    metadata: &crate::iceberg::spec::TableMetadata,
    schema: &Schema,
) -> Result<SchemaRef, ConnectorError> {
    sql_schema_from_document(
        schema,
        &crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?,
    )
}
pub(crate) fn sql_schema_from_document(
    schema: &Schema,
    document: &crate::schema_mapping::nominal_declaration::TableNominalDocument,
) -> Result<SchemaRef, ConnectorError> {
    let arrow = apply_schema(
        crate::schema_mapping::sql_read_schema_from_iceberg(schema).map_err(corrupt)?,
        schema,
        &document.integers,
    )?;
    crate::schema_mapping::nominal_declaration::apply_to_arrow(arrow, schema, &document.nominals)
}
#[cfg(test)]
pub(crate) fn sql_schema(
    schema: &Schema,
    properties: &HashMap<String, String>,
) -> Result<SchemaRef, ConnectorError> {
    sql_schema_from_document(
        schema,
        &crate::schema_mapping::nominal_declaration::TableNominalDocument::from_properties_checked::<
            ConnectorError,
        >(schema, properties, &|_| Ok(()))?,
    )
}

pub(crate) fn apply_schema(
    arrow: SchemaRef,
    schema: &Schema,
    domains: &ScalarIntegerDomains,
) -> Result<SchemaRef, ConnectorError> {
    validate_schema(schema, domains)?;
    let fields = arrow
        .fields()
        .iter()
        .zip(schema.as_struct().fields())
        .map(|(field, storage)| {
            Arc::new(match domains.get(&storage.id) {
                Some(domain)
                    if storage.field_type.as_ref() == &Type::Primitive(PrimitiveType::Int) =>
                {
                    field.as_ref().clone().with_data_type(domain.data_type())
                }
                Some(_) => field.as_ref().clone(),
                None => field.as_ref().clone(),
            })
        })
        .collect::<Vec<_>>();
    if fields.len() != arrow.fields().len() || fields.len() != schema.as_struct().fields().len() {
        return Err(corrupt("Iceberg scalar integer schema arity differs"));
    }
    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        arrow.metadata().clone(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::NestedField;
    use crate::schema_mapping::nominal_declaration::PROPERTY;
    use arrow::array::{Int8Array, Int16Array};

    fn schema(fields: &[(i32, &str)]) -> Schema {
        Schema::builder()
            .with_fields(
                fields
                    .iter()
                    .map(|(id, name)| {
                        Arc::new(NestedField::optional(
                            *id,
                            *name,
                            Type::Primitive(PrimitiveType::Int),
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap()
    }

    #[test]
    fn scalar_integer_unified_declarations_freeze_exact_sql_domains() {
        let storage = schema(&[(7, "t"), (8, "s"), (9, "plain")]);
        let properties = HashMap::from([(
            PROPERTY.to_string(),
            r#"{"version":1,"fields":[{"id":7,"domain":"tinyint"},{"id":8,"domain":"smallint"}]}"#
                .to_string(),
        )]);
        let read = sql_schema(&storage, &properties).unwrap();
        assert_eq!(
            read.fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
            vec![DataType::Int8, DataType::Int16, DataType::Int32]
        );
        assert!(read.fields().iter().all(|field| field.is_nullable()));
        let external = sql_schema(&storage, &HashMap::new()).unwrap();
        assert!(
            external
                .fields()
                .iter()
                .all(|field| field.data_type() == &DataType::Int32)
        );
        let domains = declarations(&storage, &properties).unwrap();
        assert_eq!(
            encode(&domains).unwrap(),
            r#"{"version":1,"fields":[{"id":7,"domain":"tinyint"},{"id":8,"domain":"smallint"}]}"#
        );
    }

    #[test]
    fn scalar_integer_stable_ids_preserve_rename_and_dropped_historical_field() {
        let historical = schema(&[(7, "old"), (8, "retained")]);
        let current = schema(&[(7, "renamed"), (9, "old")]);
        let properties = HashMap::from([(
            PROPERTY.to_string(),
            r#"{"version":1,"fields":[{"id":7,"domain":"tinyint"},{"id":8,"domain":"smallint"}]}"#
                .to_string(),
        )]);
        let domains = declarations(&current, &properties).unwrap();
        let current_read = apply_schema(
            crate::schema_mapping::sql_read_schema_from_iceberg(&current).unwrap(),
            &current,
            &domains,
        )
        .unwrap();
        assert_eq!(current_read.field(0).data_type(), &DataType::Int8);
        assert_eq!(
            current_read.field(1).data_type(),
            &DataType::Int32,
            "name reuse does not inherit dropped field identity"
        );
        let old_read = apply_schema(
            crate::schema_mapping::sql_read_schema_from_iceberg(&historical).unwrap(),
            &historical,
            &domains,
        )
        .unwrap();
        assert_eq!(old_read.field(0).data_type(), &DataType::Int8);
        assert_eq!(old_read.field(1).data_type(), &DataType::Int16);
    }

    #[test]
    fn scalar_integer_corrupt_declarations_are_not_type_overrides() {
        let storage = schema(&[(1, "t")]);
        for raw in [
            r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"},{"id":1,"domain":"smallint"}]}"#,
            r#"{"version":1,"fields":[{"id":-1,"domain":"tinyint"}]}"#,
            r#"{"version":1,"fields":[{"id":1,"domain":"int32"}]}"#,
            r#"{"1":"tinyint"} {}"#,
        ] {
            assert_eq!(
                declarations(
                    &storage,
                    &HashMap::from([(PROPERTY.to_string(), raw.to_string())])
                )
                .unwrap_err()
                .kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        let wrong = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "t",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            wrong,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://unproven-long".to_string(),
            crate::iceberg::spec::FormatVersion::V3,
            HashMap::from([(
                PROPERTY.to_string(),
                r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"}]}"#.to_string(),
            )]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        assert!(
            metadata_declarations(&metadata).is_err(),
            "a LONG-only field has no narrow INT history"
        );
    }

    #[test]
    fn scalar_integer_checked_arrays_preserve_slices_and_reject_stored_overflow() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(999),
            Some(-128),
            None,
            Some(127),
            Some(999),
        ]));
        let slice = input.slice(1, 3);
        let result = ScalarIntegerDomain::Int8.array(&slice).unwrap();
        assert_eq!(result.data_type(), &DataType::Int8);
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-128), None, Some(127)]
        );
        for (domain, invalid) in [
            (ScalarIntegerDomain::Int8, 128),
            (ScalarIntegerDomain::Int8, -129),
            (ScalarIntegerDomain::Int16, 32768),
            (ScalarIntegerDomain::Int16, -32769),
        ] {
            let input: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(invalid)]));
            assert_eq!(
                domain.array(&input).unwrap_err().kind(),
                ConnectorErrorKind::CorruptData,
                "stored overflow must not become NULL"
            );
        }
        let small: ArrayRef = Arc::new(Int32Array::from(vec![Some(-32768), None, Some(32767)]));
        assert_eq!(
            ScalarIntegerDomain::Int16
                .array(&small)
                .unwrap()
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-32768), None, Some(32767)]
        );
    }

    #[test]
    fn scalar_integer_metric_domains_have_exact_tags_and_never_clamp() {
        let storage = Domain::new(
            ValueSet::of_ranges(
                ConnectorValueType::Integer,
                vec![
                    Range::try_new(
                        ConnectorValueType::Integer,
                        Bound::Inclusive(ConnectorValue::Integer(-128)),
                        Bound::Inclusive(ConnectorValue::Integer(127)),
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
            true,
        );
        let logical = ScalarIntegerDomain::Int8.domain(&storage).unwrap();
        assert_eq!(logical.value_type(), ConnectorValueType::TinyInt);
        assert!(logical.null_allowed());
        assert!(
            logical
                .overlaps(&Domain::single_value(ConnectorValue::TinyInt(127)).unwrap())
                .unwrap()
        );
        assert!(
            ScalarIntegerDomain::Int8
                .domain(&Domain::single_value(ConnectorValue::Integer(128)).unwrap())
                .is_err()
        );
        assert_eq!(
            ScalarIntegerDomain::Int16
                .domain(&Domain::only_null(ConnectorValueType::Integer))
                .unwrap(),
            Domain::only_null(ConnectorValueType::SmallInt)
        );
    }
    #[test]
    fn scalar_integer_persisted_defaults_are_validated_before_any_row_is_read() {
        use crate::iceberg::spec::{Literal, PrimitiveLiteral};
        let field = NestedField::optional(1, "tiny", Type::Primitive(PrimitiveType::Int))
            .with_initial_default(Literal::Primitive(PrimitiveLiteral::Int(128)));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let error = declarations(
            &schema,
            &HashMap::from([(
                PROPERTY.to_string(),
                r#"{"version":1,"fields":[{"id":1,"domain":"tinyint"}]}"#.to_string(),
            )]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    fn opaque_source_schema() -> Schema {
        use crate::iceberg::spec::{Literal, PrimitiveLiteral, StructType};
        Schema::builder()
            .with_fields(vec![
                Arc::new(
                    NestedField::required(10, "h", Type::Primitive(PrimitiveType::Binary))
                        .with_initial_default(Literal::Primitive(PrimitiveLiteral::Binary(vec![
                            1, 2,
                        ]))),
                ),
                Arc::new(NestedField::optional(
                    11,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    12,
                    "l",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::optional(
                    13,
                    "plain",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    14,
                    "fixed",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::optional(
                    15,
                    "json",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    16,
                    "record",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        17,
                        "u",
                        Type::Primitive(PrimitiveType::Uuid),
                    ))])),
                )),
            ])
            .build()
            .unwrap()
    }

    fn opaque_source_properties(schema: &Schema) -> HashMap<String, String> {
        let nominals = [
            (
                "h",
                crate::schema_mapping::nominal_declaration::NominalSourceDomain::Hll,
            ),
            (
                "b",
                crate::schema_mapping::nominal_declaration::NominalSourceDomain::Bitmap,
            ),
            (
                "l",
                crate::schema_mapping::nominal_declaration::NominalSourceDomain::LargeInt,
            ),
        ]
        .into_iter()
        .map(|(name, domain)| (schema.field_by_name(name).unwrap().id, domain))
        .collect();
        HashMap::from([(
            PROPERTY.into(),
            crate::schema_mapping::nominal_declaration::encode(&nominals).unwrap(),
        )])
    }

    #[test]
    fn table_metadata_authors_only_declared_opaque_source_domains() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType, field_logical_type};
        let storage = opaque_source_schema();
        let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
            storage,
            crate::iceberg::spec::PartitionSpec::unpartition_spec(),
            crate::iceberg::spec::SortOrder::unsorted_order(),
            "memory://opaque-source".into(),
            crate::iceberg::spec::FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let properties = opaque_source_properties(metadata.current_schema());
        let metadata = metadata
            .into_builder(None)
            .set_properties(properties)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        // Table creation assigns the persisted field IDs. Compare annotations
        // against that actual owner schema rather than the pre-creation IDs.
        let base =
            crate::schema_mapping::sql_read_schema_from_iceberg(metadata.current_schema()).unwrap();
        let projected = metadata_sql_schema(&metadata, metadata.current_schema()).unwrap();
        for (ordinal, logical) in [
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Physical,
            ValueLogicalType::Physical,
            ValueLogicalType::Physical,
            ValueLogicalType::Physical,
        ]
        .into_iter()
        .enumerate()
        {
            let field = projected.field(ordinal);
            assert_eq!(field_logical_type(field).unwrap(), logical);
            assert_eq!(field.data_type(), base.field(ordinal).data_type());
            assert_eq!(field.is_nullable(), base.field(ordinal).is_nullable());
            for (key, value) in base.field(ordinal).metadata() {
                assert_eq!(field.metadata().get(key), Some(value));
            }
            FunctionValueType::try_from_field(field).unwrap();
        }
        assert_eq!(projected.field(0).data_type(), &DataType::Binary);
        assert!(!projected.field(0).is_nullable());
        assert_eq!(
            projected.field(2).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(projected.field(6), base.field(6));
        assert_eq!(projected.metadata(), base.metadata());
        let plain = sql_schema(metadata.current_schema(), &HashMap::new()).unwrap();
        assert_eq!(
            field_logical_type(plain.field(0)).unwrap(),
            ValueLogicalType::Physical
        );
        assert_eq!(
            field_logical_type(plain.field(2)).unwrap(),
            ValueLogicalType::Physical
        );
    }

    #[test]
    #[allow(deprecated)]
    fn source_domain_authoring_keeps_actual_field_annotations_and_dictionary_identity() {
        use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType, field_logical_type};
        let storage = opaque_source_schema();
        let declarations = crate::schema_mapping::nominal_declaration::from_properties(
            &storage,
            &opaque_source_properties(&storage),
        )
        .unwrap();
        let base = crate::schema_mapping::sql_read_schema_from_iceberg(&storage).unwrap();
        let mut fields = base.fields().iter().cloned().collect::<Vec<_>>();
        let mut annotations = fields[0].metadata().clone();
        annotations.insert("provider.annotation".to_string(), "retained".to_string());
        annotations.insert("default.annotation".to_string(), "0102".to_string());
        fields[0] = Arc::new(
            fields[0]
                .as_ref()
                .clone()
                .with_metadata(annotations.clone()),
        );
        fields[5] = Arc::new(
            arrow::datatypes::Field::new_dict(
                "json",
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                true,
                93,
                true,
            )
            .with_metadata(HashMap::from([(
                "provider.dictionary".into(),
                "retained".into(),
            )])),
        );
        let annotated = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            HashMap::from([("schema.annotation".into(), "retained".into())]),
        ));
        let projected = crate::schema_mapping::nominal_declaration::apply_to_arrow(
            annotated.clone(),
            &storage,
            &declarations,
        )
        .unwrap();
        annotations.insert(NR_LOGICAL_TYPE_KEY.to_string(), "hll".to_string());
        assert_eq!(projected.field(0).metadata(), &annotations);
        assert_eq!(projected.field(5).dict_id(), Some(93));
        assert_eq!(projected.field(5).dict_is_ordered(), Some(true));
        assert_eq!(projected.field(5), annotated.field(5));
        assert_eq!(
            field_logical_type(projected.field(0)).unwrap(),
            ValueLogicalType::Hll
        );
        assert_eq!(projected.field(6), annotated.field(6));
        assert_eq!(projected.metadata(), annotated.metadata());
        // An identical explicit declaration is checked, not overwritten.
        assert!(
            crate::schema_mapping::nominal_declaration::apply_to_arrow(
                projected,
                &storage,
                &declarations
            )
            .is_ok()
        );
    }

    #[test]
    fn source_domain_declarations_reject_wrong_storage_carrier_and_explicit_metadata() {
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        let storage = opaque_source_schema();
        let declarations = crate::schema_mapping::nominal_declaration::from_properties(
            &storage,
            &opaque_source_properties(&storage),
        )
        .unwrap();
        for (name, value) in [("json", "hll"), ("fixed", "bitmap"), ("plain", "largeint")] {
            let error = sql_schema(
                &storage,
                &HashMap::from([(
                    PROPERTY.into(),
                    format!(
                        r#"{{"version":1,"fields":[{{"id":{},"domain":"{value}"}}]}}"#,
                        storage.field_by_name(name).unwrap().id
                    ),
                )]),
            )
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
        let base = crate::schema_mapping::sql_read_schema_from_iceberg(&storage).unwrap();
        for (ordinal, label) in [(0, "bitmap"), (0, "unknown"), (3, "hll"), (5, "json")] {
            let mut fields = base.fields().iter().cloned().collect::<Vec<_>>();
            let mut metadata = fields[ordinal].metadata().clone();
            metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), label.to_string());
            fields[ordinal] = Arc::new(fields[ordinal].as_ref().clone().with_metadata(metadata));
            let error = crate::schema_mapping::nominal_declaration::apply_to_arrow(
                Arc::new(arrow::datatypes::Schema::new(fields)),
                &storage,
                &declarations,
            )
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
        for carrier in [DataType::LargeBinary, DataType::Utf8] {
            let mut fields = base.fields().iter().cloned().collect::<Vec<_>>();
            fields[0] = Arc::new(fields[0].as_ref().clone().with_data_type(carrier));
            assert!(
                crate::schema_mapping::nominal_declaration::apply_to_arrow(
                    Arc::new(arrow::datatypes::Schema::new(fields)),
                    &storage,
                    &declarations
                )
                .is_err()
            );
        }
    }

    #[test]
    fn current_field_id_declarations_do_not_invent_other_historical_or_json_domains() {
        use novarocks_type_contract::{ValueLogicalType, field_logical_type};
        let historical = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    99,
                    "old_h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    15,
                    "json",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let properties = opaque_source_properties(&opaque_source_schema());
        let declarations = crate::schema_mapping::nominal_declaration::from_properties(
            &opaque_source_schema(),
            &properties,
        )
        .unwrap();
        let projected = crate::schema_mapping::nominal_declaration::apply_to_arrow(
            crate::schema_mapping::sql_read_schema_from_iceberg(&historical).unwrap(),
            &historical,
            &declarations,
        )
        .unwrap();
        for field in projected.fields() {
            assert_eq!(
                field_logical_type(field).unwrap(),
                ValueLogicalType::Physical
            );
        }
        // A renamed historical occurrence with the same stable ID retains its
        // independently authored domain; names do not erase field identity.
        let renamed = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                10,
                "old_h",
                Type::Primitive(PrimitiveType::Binary),
            ))])
            .build()
            .unwrap();
        let projected = crate::schema_mapping::nominal_declaration::apply_to_arrow(
            crate::schema_mapping::sql_read_schema_from_iceberg(&renamed).unwrap(),
            &renamed,
            &declarations,
        )
        .unwrap();
        assert_eq!(
            field_logical_type(projected.field(0)).unwrap(),
            ValueLogicalType::Hll
        );
    }
}

#[cfg(test)]
#[path = "scalar_integer_domain/checked_tests.rs"]
mod checked_tests;
