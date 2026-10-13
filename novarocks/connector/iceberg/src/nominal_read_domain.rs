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

//! Frozen nominal identities from the provider's original SQL schema author.
//! No property interpretation or array conversion lives in this module.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType, field_logical_type};

use crate::iceberg::spec::{PrimitiveType, Schema, Type};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum NominalReadDomain {
    Hll,
    Bitmap,
    LargeInt,
}

pub(crate) type NominalReadDomains = BTreeMap<i32, NominalReadDomain>;

fn invalid(detail: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, detail)
}

impl NominalReadDomain {
    pub(crate) fn from_logical(logical: ValueLogicalType) -> Option<Self> {
        match logical {
            ValueLogicalType::Hll => Some(Self::Hll),
            ValueLogicalType::Bitmap => Some(Self::Bitmap),
            ValueLogicalType::LargeInt => Some(Self::LargeInt),
            _ => None,
        }
    }

    pub(crate) fn logical(self) -> ValueLogicalType {
        match self {
            Self::Hll => ValueLogicalType::Hll,
            Self::Bitmap => ValueLogicalType::Bitmap,
            Self::LargeInt => ValueLogicalType::LargeInt,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        self.logical()
            .metadata_value()
            .expect("nominal read domains have canonical labels")
    }

    pub(crate) fn parse(value: &str) -> Result<Self, ConnectorError> {
        let logical = ValueLogicalType::from_metadata_value(value)
            .map_err(|_| invalid("invalid Iceberg nominal read declaration"))?;
        Self::from_logical(logical)
            .ok_or_else(|| invalid("unsupported Iceberg nominal read declaration"))
    }

    pub(crate) fn validate_storage(self, storage: &Type) -> Result<(), ConnectorError> {
        let expected = match self {
            Self::Hll | Self::Bitmap => PrimitiveType::Binary,
            Self::LargeInt => PrimitiveType::Fixed(16),
        };
        if storage != &Type::Primitive(expected) {
            return Err(invalid(
                "Iceberg nominal declaration differs from its exact storage carrier",
            ));
        }
        Ok(())
    }

    /// Preserve the original field and all its annotations. The caller has an
    /// independent frozen declaration; equal-width arrays cannot author one.
    pub(crate) fn annotate(self, field: &Field) -> Result<Field, ConnectorError> {
        let expected = match self {
            Self::Hll | Self::Bitmap => DataType::Binary,
            Self::LargeInt => DataType::FixedSizeBinary(16),
        };
        if field.data_type() != &expected {
            return Err(invalid(
                "Iceberg nominal read field has a different carrier",
            ));
        }
        if field.metadata().contains_key(NR_LOGICAL_TYPE_KEY)
            && field_logical_type(field)
                .map_err(|_| invalid("Iceberg nominal read field has invalid logical metadata"))?
                != self.logical()
        {
            return Err(invalid(
                "Iceberg nominal read field identity conflicts with its declaration",
            ));
        }
        let mut annotated = field.clone();
        annotated
            .metadata_mut()
            .insert(NR_LOGICAL_TYPE_KEY.into(), self.name().into());
        Ok(annotated)
    }
}

/// Extract only identities the existing selected-schema author has proven.
/// Its original property/history interpretation is deliberately not repeated.
pub(crate) fn from_sql_schema(
    source: &Schema,
    authored: &ArrowSchema,
) -> Result<NominalReadDomains, ConnectorError> {
    if source.as_struct().fields().len() != authored.fields().len() {
        return Err(invalid("Iceberg nominal source schema arity differs"));
    }
    let mut domains = BTreeMap::new();
    for (storage, field) in source.as_struct().fields().iter().zip(authored.fields()) {
        if field.name() != &storage.name
            || field.is_nullable() == storage.required
            || crate::schema_mapping::field_id_for_arrow_field(field).map_err(|_| {
                invalid("Iceberg nominal source field has invalid field ID metadata")
            })? != Some(storage.id)
        {
            return Err(invalid("Iceberg nominal source field identity differs"));
        }
        let logical = field_logical_type(field)
            .map_err(|_| invalid("Iceberg nominal source field has invalid logical metadata"))?;
        if let Some(domain) = NominalReadDomain::from_logical(logical) {
            domain.validate_storage(storage.field_type.as_ref())?;
            domains.insert(storage.id, domain);
        }
    }
    Ok(domains)
}

/// Check roots in source order without building another schema or an ID index.
/// Absent and nested IDs cannot gain a top-level declaration.
pub(crate) fn validate_schema(
    source: &Schema,
    domains: &NominalReadDomains,
) -> Result<(), ConnectorError> {
    let mut found = 0;
    for field in source.as_struct().fields() {
        if let Some(domain) = domains.get(&field.id) {
            domain.validate_storage(field.field_type.as_ref())?;
            found += 1;
        }
    }
    if found != domains.len() {
        return Err(invalid(
            "Iceberg nominal declaration names an absent root field",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, SortOrder, TableMetadataBuilder,
    };

    #[test]
    fn original_source_author_freezes_only_declared_nominal_roots() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "n",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "h",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
                Arc::new(NestedField::optional(
                    4,
                    "raw",
                    Type::Primitive(PrimitiveType::Fixed(16)),
                )),
                Arc::new(NestedField::optional(
                    5,
                    "u",
                    Type::Primitive(PrimitiveType::Uuid),
                )),
                Arc::new(NestedField::optional(
                    6,
                    "v",
                    Type::Primitive(PrimitiveType::Variant),
                )),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/nominal_read_domains".into(),
            FormatVersion::V3,
            HashMap::from([
                ("novarocks.logical_type.n".into(), "largeint".into()),
                ("novarocks.logical_type.h".into(), "hll".into()),
                ("novarocks.logical_type.b".into(), "bitmap".into()),
            ]),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let source = metadata.current_schema();
        let authored =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, source).unwrap();
        let declarations = from_sql_schema(source, &authored).unwrap();
        assert_eq!(
            declarations,
            BTreeMap::from([
                (1, NominalReadDomain::LargeInt),
                (2, NominalReadDomain::Hll),
                (3, NominalReadDomain::Bitmap),
            ])
        );
        validate_schema(source, &declarations).unwrap();
        assert_eq!(
            field_logical_type(authored.field(3)).unwrap(),
            ValueLogicalType::Physical
        );
        assert_eq!(
            field_logical_type(authored.field(4)).unwrap(),
            ValueLogicalType::Uuid
        );
        assert_eq!(
            field_logical_type(authored.field(5)).unwrap(),
            ValueLogicalType::Variant
        );
        assert!(!authored.field(0).is_nullable());
        assert!(authored.field(1).is_nullable());
    }

    #[test]
    fn declared_field_annotation_preserves_original_metadata_and_nullability() {
        for (domain, carrier, label) in [
            (NominalReadDomain::Hll, DataType::Binary, "hll"),
            (NominalReadDomain::Bitmap, DataType::Binary, "bitmap"),
            (
                NominalReadDomain::LargeInt,
                DataType::FixedSizeBinary(16),
                "largeint",
            ),
        ] {
            let metadata = HashMap::from([
                ("PARQUET:field_id".into(), "17".into()),
                ("provider.annotation".into(), "retained".into()),
                (
                    crate::default_value::ICEBERG_INITIAL_DEFAULT_META_KEY.into(),
                    "null".into(),
                ),
            ]);
            let original =
                Field::new("source", carrier.clone(), true).with_metadata(metadata.clone());
            let projected = domain.annotate(&original).unwrap();
            let mut expected = metadata;
            expected.insert(NR_LOGICAL_TYPE_KEY.into(), label.into());
            assert_eq!(
                projected,
                Field::new("source", carrier, true).with_metadata(expected)
            );
            assert!(!original.metadata().contains_key(NR_LOGICAL_TYPE_KEY));
            assert_eq!(domain.annotate(&projected).unwrap(), projected);
            assert_eq!(NominalReadDomain::parse(label).unwrap(), domain);
        }
    }

    #[test]
    fn source_declarations_do_not_reinterpret_uuid_or_invent_absent_roots() {
        let uuid = Field::new("u", DataType::FixedSizeBinary(16), true)
            .with_metadata(HashMap::from([(NR_LOGICAL_TYPE_KEY.into(), "uuid".into())]));
        assert!(NominalReadDomain::LargeInt.annotate(&uuid).is_err());
        assert!(NominalReadDomain::parse("uuid").is_err());
        let source = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "raw",
                Type::Primitive(PrimitiveType::Fixed(16)),
            ))])
            .build()
            .unwrap();
        assert!(
            validate_schema(&source, &BTreeMap::from([(1, NominalReadDomain::LargeInt)])).is_ok()
        );
        assert!(
            validate_schema(&source, &BTreeMap::from([(2, NominalReadDomain::LargeInt)])).is_err()
        );
        assert!(validate_schema(&source, &BTreeMap::from([(1, NominalReadDomain::Hll)])).is_err());
    }
}
