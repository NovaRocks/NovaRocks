// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MV storage evidence comes from the ordinary Iceberg field authors.

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind,
    ConnectorPreparedCreateDocumentTarget, ConnectorRequestContext, ConnectorTableMetadata,
    MvCurrentStorageMapping, MvObservedStorageMapping, MvStorageColumnDeclaration,
    MvStorageColumnRequest, MvStoragePushdownClass, ProviderSourceType,
};
use novarocks_type_contract::{
    CanonicalFactWriter, CharacterKind, ValueKind, ValueLogicalType, ValueTypeRef,
};

use crate::iceberg::spec::{FormatVersion, NestedField, TableMetadata};
use crate::schema_mapping::{nominal_declaration::TableNominalDocument, typed_value};
use crate::typed_read::runtime::{IcebergReadLayout, IcebergReadProjection};

use super::{
    IcebergStorageInspector, corrupt, decoded_table_generation, exact_schema_version,
    iceberg_object_id_from_uuid, invalid, project_source_type, reserve_bytes,
};

impl IcebergStorageInspector {
    pub fn map_prepared_storage(
        &self,
        target: &ConnectorPreparedCreateDocumentTarget,
        columns: &[MvStorageColumnRequest],
        context: ConnectorRequestContext,
    ) -> Result<Vec<MvObservedStorageMapping>, ConnectorError> {
        columns
            .iter()
            .map(|column| {
                super::validate_context(&context)?;
                let field = target
                    .fields()
                    .iter()
                    .find(|field| field.provider_field_id() == &column.field_id)
                    .ok_or_else(|| invalid("MV output has no exact prepared Iceberg field"))?;
                let storage = field
                    .source_type()
                    .ok_or_else(|| corrupt("prepared Iceberg field has no storage type"))?;
                let declaration = field
                    .storage_declaration()
                    .ok_or_else(|| corrupt("prepared Iceberg field has no mapping declaration"))?;
                map_column(column, storage, field.nullable(), declaration)
            })
            .collect()
    }

    pub fn observe_current_storage_mapping(
        &self,
        exact_lease: &ConnectorControlPlanningPin,
        metadata: &ConnectorTableMetadata,
        columns: &[MvStorageColumnRequest],
        context: ConnectorRequestContext,
    ) -> Result<MvCurrentStorageMapping, ConnectorError> {
        let (table, location) = decoded_table_generation(exact_lease, metadata, &context)?;
        let location = location
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                corrupt("current MV storage mapping requires committed Iceberg metadata")
            })?;
        let schema = table.current_schema();
        if metadata.version.as_ref() != Some(&exact_schema_version(schema.schema_id())) {
            return Err(corrupt(
                "MV mapping differs from the originally observed Iceberg schema",
            ));
        }
        crate::commit::validation::deny_unwritable_data_files(
            schema,
            &format!(
                "{}.{}.{}",
                metadata.identity.instance_id.as_str(),
                metadata.identity.namespace,
                metadata.identity.table
            ),
        )?;
        let declarations = TableNominalDocument::from_metadata_checked(&table, &|_| {
            super::validate_context(&context)
        })?;
        let mut bytes = 0;
        let columns = columns
            .iter()
            .map(|column| {
                super::validate_context(&context)?;
                let id = i32::from_be_bytes(
                    column
                        .field_id
                        .as_ref()
                        .try_into()
                        .map_err(|_| invalid("invalid Iceberg storage field identity"))?,
                );
                let field = schema
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|field| field.id == id)
                    .ok_or_else(|| invalid("MV output has no current Iceberg storage field"))?;
                let storage = project_source_type(&field.field_type, &context, |n| {
                    reserve_bytes(&context, &mut bytes, n)
                })?;
                let declaration = declare_column(&table, field, &storage, &declarations, &context)?;
                map_column(column, &storage, !field.required, &declaration)
            })
            .collect::<Result<_, _>>()?;
        Ok(MvCurrentStorageMapping {
            target_object: iceberg_object_id_from_uuid(table.uuid().to_string())?,
            target_version: crate::document_storage::observation::committed_version_from_metadata(
                &table,
                Some(location),
            )?,
            columns,
        })
    }
}

pub(crate) fn declare_column(
    table: &TableMetadata,
    field: &NestedField,
    storage: &ProviderSourceType,
    declarations: &TableNominalDocument,
    context: &ConnectorRequestContext,
) -> Result<MvStorageColumnDeclaration, ConnectorError> {
    let (read_type, write_type) =
        crate::metadata::with_pure_schema_creation(context, |creation| {
            let integer = declarations.integers.get(&field.id).copied();
            let read = typed_value::from_field(
                field,
                !field.required,
                &declarations.nominals,
                integer,
                IcebergReadProjection::Sql(IcebergReadLayout::Offsets),
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)?;
            let write = typed_value::from_write_field(
                field,
                !field.required,
                &declarations.nominals,
                integer,
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)?;
            Ok((
                read.binding.value_type().clone(),
                write.binding.value_type().clone(),
            ))
        })?;
    let storage_format_version = match table.format_version() {
        FormatVersion::V1 => 1,
        FormatVersion::V2 => 2,
        FormatVersion::V3 => 3,
    };
    let mut provider_declaration =
        CanonicalFactWriter::new("novarocks.iceberg.mv.storage-column", 1);
    provider_declaration.bytes(&storage.canonical_bytes());
    provider_declaration.boolean(field.required);
    provider_declaration.value_type(&read_type);
    provider_declaration.value_type(&write_type);
    let provider_declaration = Bytes::from(provider_declaration.finish());
    Ok(MvStorageColumnDeclaration {
        read_type,
        write_type,
        provider_declaration,
        storage_format_version,
        encode_identity: Bytes::from_static(b"novarocks.iceberg.ordinary-value-write/v1"),
        decode_identity: Bytes::from_static(b"novarocks.iceberg.ordinary-value-read/v1"),
        content_contract: Bytes::from_static(b"novarocks.iceberg.mv.observable-value-content/v1"),
    })
}

fn map_column(
    column: &MvStorageColumnRequest,
    storage: &ProviderSourceType,
    storage_nullable: bool,
    declaration: &MvStorageColumnDeclaration,
) -> Result<MvObservedStorageMapping, ConnectorError> {
    let logical = &column.logical_type;
    let read = &declaration.read_type;
    let write = &declaration.write_type;
    if !supports_values(logical, read) || !supports_values(logical, write) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "Iceberg field cannot preserve the exact logical content of MV output {}",
                column.output_ordinal
            ),
        ));
    }
    let pushdown = match logical.kind() {
        ValueKind::List(_)
        | ValueKind::Struct(_)
        | ValueKind::Map { .. }
        | ValueKind::Nominal(_) => MvStoragePushdownClass::Opaque,
        ValueKind::BoundedCharacter(declaration)
            if declaration.kind() == CharacterKind::Varchar =>
        {
            MvStoragePushdownClass::OrderPreservingInjective
        }
        _ if logical.kind() == read.kind() && logical.kind() == write.kind() => {
            MvStoragePushdownClass::Identity
        }
        _ => MvStoragePushdownClass::Opaque,
    };
    Ok(MvObservedStorageMapping {
        output_ordinal: column.output_ordinal,
        field_id: column.field_id.clone(),
        logical_type: logical.clone(),
        storage_type: storage.clone(),
        storage_nullable,
        declaration: declaration.clone(),
        pushdown,
    })
}

fn supports_values(logical: &ValueTypeRef, physical: &ValueTypeRef) -> bool {
    if logical.nullable() && !physical.nullable() {
        return false;
    }
    match (logical.kind(), physical.kind()) {
        (ValueKind::List(logical), ValueKind::List(storage)) => supports_values(logical, storage),
        (ValueKind::Struct(logical), ValueKind::Struct(storage)) => {
            logical.len() == storage.len()
                && logical.iter().zip(storage).all(|(logical, storage)| {
                    logical.name() == storage.name()
                        && supports_values(logical.value_type(), storage.value_type())
                })
        }
        (
            ValueKind::Map { key, value },
            ValueKind::Map {
                key: storage_key,
                value: storage_value,
            },
        ) => supports_values(key, storage_key) && supports_values(value, storage_value),
        (
            ValueKind::BoundedCharacter(_) | ValueKind::Nominal(ValueLogicalType::Json),
            ValueKind::String,
        ) => true,
        (logical, storage) => logical == storage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{PartitionSpec, Schema, SortOrder, TableMetadataBuilder};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::{
        ConnectorColumnDefinition, ConnectorDataType, ConnectorStructField,
    };
    use novarocks_type_contract::{CharacterType, FunctionValueType, ValueTypeStorageLifetime};
    use std::sync::Arc;

    fn table(version: FormatVersion) -> TableMetadata {
        use ConnectorDataType as Type;
        let columns: Vec<_> = [
            ("amount", Type::BigInt),
            ("text_value", Type::String),
            (
                "payload",
                Type::Map(
                    Box::new(Type::BigInt),
                    Box::new(Type::Struct(vec![ConnectorStructField {
                        name: "bits".into(),
                        data_type: Type::LargeInt,
                        nullable: true,
                    }])),
                ),
            ),
        ]
        .into_iter()
        .map(|(name, data_type)| ConnectorColumnDefinition {
            name: name.into(),
            data_type,
            nullable: false,
            default: None,
        })
        .collect();
        let schema = Schema::builder()
            .with_fields(
                crate::catalog_control::type_mapping::schema_fields(&columns, version).unwrap(),
            )
            .build()
            .unwrap();
        let nominal = TableNominalDocument::from_columns(&schema, &columns).unwrap();
        let meta = crate::schema_mapping::nominal_declaration::PROPERTY;
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///mv-storage".into(),
            version,
            [(meta.to_string(), nominal.encode().unwrap())].into(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    fn mappings(
        table: &TableMetadata,
    ) -> Vec<(
        Arc<NestedField>,
        ProviderSourceType,
        MvStorageColumnDeclaration,
    )> {
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(10),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            4096,
            65_536,
        )
        .unwrap();
        let nominal = TableNominalDocument::from_metadata(table).unwrap();
        table
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| {
                let storage = project_source_type(&field.field_type, &context, |_| Ok(())).unwrap();
                let declaration =
                    declare_column(table, field, &storage, &nominal, &context).unwrap();
                (Arc::clone(field), storage, declaration)
            })
            .collect()
    }

    #[test]
    fn ordinary_iceberg_declarations_distinguish_opaque_shape_and_order_preserving_text() {
        let table = table(FormatVersion::V3);
        let fields = mappings(&table);
        let types = [
            fields[0].2.read_type.clone(),
            ValueTypeRef::try_new(
                ValueKind::BoundedCharacter(
                    CharacterType::new(CharacterKind::Varchar, 12).unwrap(),
                ),
                false,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
            fields[2].2.read_type.clone(),
        ];
        assert!(matches!(
            types[2].kind(),
            ValueKind::Map { value, .. } if matches!(
                value.kind(), ValueKind::Struct(fields) if matches!(
                    fields[0].value_type().kind(), ValueKind::Nominal(ValueLogicalType::LargeInt)
                )
            )
        ));
        for (i, ((field, storage, declaration), logical_type)) in
            fields.iter().zip(types).enumerate()
        {
            let request = MvStorageColumnRequest {
                output_ordinal: i as u32,
                field_id: Bytes::copy_from_slice(&field.id.to_be_bytes()),
                logical_type,
            };
            let mapped = map_column(&request, storage, !field.required, declaration).unwrap();
            assert_eq!(
                mapped.pushdown,
                [
                    MvStoragePushdownClass::Identity,
                    MvStoragePushdownClass::OrderPreservingInjective,
                    MvStoragePushdownClass::Opaque,
                ][i]
            );
            assert_eq!(mapped.declaration.storage_format_version, 3);
        }
        let (field, storage, declaration) = &fields[0];
        let request = MvStorageColumnRequest {
            output_ordinal: 0,
            field_id: Bytes::copy_from_slice(&field.id.to_be_bytes()),
            logical_type: FunctionValueType::new(DataType::Int64, true)
                .value_type()
                .clone(),
        };
        assert_eq!(
            map_column(&request, storage, false, declaration)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn storage_format_is_part_of_the_observed_mapping() {
        let earlier = mappings(&table(FormatVersion::V2));
        let later = mappings(&table(FormatVersion::V3));
        assert_eq!(earlier[0].1, later[0].1);
        assert_eq!(earlier[0].2.read_type, later[0].2.read_type);
        assert_ne!(earlier[0].2, later[0].2);
    }
}
