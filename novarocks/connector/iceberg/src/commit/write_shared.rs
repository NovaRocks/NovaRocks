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

//! Helpers shared by the write-control preparation paths.
//!
//! `prepare_write` and `prepare_row_mutation` resolve the same ref-scoped
//! facts before they diverge into their own field-signing rules. Keeping the
//! shared resolution here stops the two paths from drifting apart.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorWriteFieldRequest};

use crate::iceberg::spec::{Schema, TableMetadata};

/// Resolve the snapshot a write against `target_ref` will be based on.
///
/// `main` resolves to the table's current snapshot; any other ref resolves to
/// that branch's head. `Ok(None)` means the ref exists but has no snapshot yet.
pub(crate) fn write_target_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot_id());
    }
    crate::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

/// Render a resolved base snapshot for the `base_version` / preparation
/// payload strings. Kept next to the resolver so both preparation paths spell
/// a missing snapshot the same way.
pub(crate) fn snapshot_token(target_snapshot_id: Option<i64>) -> String {
    target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string())
}

/// Resolve each requested write column against the frozen target schema and
/// restate it with the Arrow type the Iceberg writers actually consume.
///
/// The shared metadata schema owner authors Variant/Binary/Timestamptz
/// carriers and every declared logical domain. Both write paths consume those
/// complete fields instead of reconstructing an untagged carrier.
pub(crate) fn exact_requested_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    exact_requested_write_fields_at_schema(metadata, metadata.current_schema(), requested, creation)
}

/// Resolve write fields against an already-frozen Iceberg schema.
pub(crate) fn exact_requested_write_fields_at_schema(
    metadata: &TableMetadata,
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    creation
        .work(0)
        .map_err(crate::metadata::metadata_creation_error)?;
    let document =
        crate::schema_mapping::nominal_declaration::TableNominalDocument::from_metadata(metadata)?;
    let execution = crate::schema_mapping::catalog_value::table_with_document(
        &document,
        iceberg_schema,
        &[],
        None,
        creation,
    )
    .map_err(crate::metadata::metadata_creation_error)?;
    exact_requested_write_fields_from_declaration(
        &document,
        iceberg_schema,
        requested,
        &execution,
        creation,
    )
}

/// Borrow an already-authored provider declaration at the same frozen schema.
/// Source fields still come from the original format projection.
pub(crate) fn exact_requested_write_fields_from_declaration(
    document: &crate::schema_mapping::nominal_declaration::TableNominalDocument,
    iceberg_schema: &Schema,
    requested: &[ConnectorWriteFieldRequest],
    execution: &novarocks_connector_contract::ConnectorReadExecutionSchema,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let arrow_schema =
        crate::scalar_integer_domain::sql_schema_from_document(iceberg_schema, document)?;
    requested
        .iter()
        .map(|request| {
            creation.work(1).map_err(crate::metadata::metadata_creation_error)?;
            let requested_name = request.field().name();
            let (ordinal, _iceberg_field) = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name.eq_ignore_ascii_case(requested_name))
                .ok_or_else(|| {
                    invalid_write_activation(format!(
                        "Iceberg write input column `{requested_name}` is absent from the frozen target schema"
                    ))
                })?;
            let arrow_field = arrow_schema.field(ordinal);
            // The original source Field and the complete declaration have
            // independent owners. Only this catalog author establishes domains.
            let value = execution.bindings().get(ordinal).ok_or_else(|| {
                invalid_write_activation("frozen writer execution declaration is absent")
            })?;
            creation.work(0).map_err(crate::metadata::metadata_creation_error)?;
            ConnectorWriteFieldRequest::try_new_typed(arrow_field.clone(), value.clone())
        })
        .collect()
}

enum WriteValueFailure {
    Control(novarocks_type_contract::CompileControlError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for WriteValueFailure
{
    fn from(value: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(value)
    }
}
impl WriteValueFailure {
    fn into_error(self) -> ConnectorError {
        match self {
            Self::Control(cause) => crate::metadata::metadata_creation_error(
                novarocks_connector_contract::PureProviderCompileError::Control(cause),
            ),
            Self::Resource(cause) => ConnectorError::new(
                if matches!(cause, novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel) {
                    novarocks_spi::connector::ConnectorErrorKind::Internal
                } else { novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted },
                format!("writer value root construction failed: {cause:?}"),
            ),
        }
    }
}

pub(crate) fn writer_value_with_nullable(
    value: &novarocks_type_contract::FunctionValueType,
    nullable: bool,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<novarocks_type_contract::FunctionValueType, ConnectorError> {
    if value.nullable() == nullable {
        return Ok(value.clone());
    }
    let storage = creation.storage();
    let (request, work) = creation.observers();
    value
        .with_nullable_observed(
            nullable,
            storage,
            |layout| request(layout).map_err(WriteValueFailure::Control),
            |units| work(units).map_err(WriteValueFailure::Control),
        )
        .map_err(WriteValueFailure::into_error)
}

/// Metadata roles are authored by the existing provider declaration, not the
/// SQL proposal's carrier or attributes. Original proposal fields stay intact.
pub(crate) fn exact_metadata_write_fields(
    _metadata: &TableMetadata,
    fields: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let mut authored = creation
        .reserve(fields.len())
        .map_err(crate::metadata::metadata_creation_error)?;
    for field in fields {
        creation
            .work(1)
            .map_err(crate::metadata::metadata_creation_error)?;
        let role = crate::typed_read::ALWAYS_BOUND_METADATA_COLUMNS
            .into_iter()
            .chain(crate::typed_read::ROW_LINEAGE_METADATA_COLUMNS)
            .find(|role| {
                role.column_name()
                    .eq_ignore_ascii_case(field.field().name())
            })
            .ok_or_else(|| {
                invalid_write_activation("writer identity field has no provider metadata role")
            })?;
        let value = crate::schema_mapping::catalog_value::metadata_column(role, creation)
            .map_err(crate::metadata::metadata_creation_error)?;
        // Keep the original source Field and its requested root NULL obligation;
        // the static role does not validate or borrow unrelated table data.
        let value = writer_value_with_nullable(&value, field.field().is_nullable(), creation)?;
        authored.push(ConnectorWriteFieldRequest::try_new_typed(
            field.field().clone(),
            value,
        )?);
    }
    Ok(authored)
}

/// Resolve the exact schema owned by a previously resolved write base.
///
/// A missing snapshot denotes an empty table/ref and deliberately retains the
/// admitted metadata's current schema. A concrete snapshot always owns the
/// schema, including when that snapshot is an older branch head.
pub(crate) fn write_target_schema(
    metadata: &TableMetadata,
    target_snapshot_id: Option<i64>,
) -> Result<std::sync::Arc<Schema>, ConnectorError> {
    match target_snapshot_id {
        Some(snapshot_id) => metadata
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "resolved Iceberg write base snapshot is absent from admitted metadata",
                )
            })?
            .schema(metadata)
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("resolve admitted Iceberg write base schema: {error}"),
                )
            }),
        None => Ok(metadata.current_schema().clone()),
    }
}

pub(crate) fn position_mutation_base(
    metadata: &TableMetadata,
    location: Option<&str>,
    target_ref: &str,
    snapshot_id: Option<i64>,
) -> Result<novarocks_spi::connector::ConnectorWriteBaseVersion, ConnectorError> {
    use sha2::{Digest, Sha256};
    let metadata_identity = crate::read_snapshot::metadata_identity(metadata)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
    let metadata_object = location.map_or_else(
        || "none".to_owned(),
        |path| format!("{:x}", Sha256::digest(path.as_bytes())),
    );
    let snapshot = snapshot_token(snapshot_id);
    novarocks_spi::connector::ConnectorWriteBaseVersion::try_new(bytes::Bytes::from(format!(
        "iceberg/position-mutation-base/v2/{}/{target_ref}/{snapshot}/{metadata_object}/{metadata_identity}",
        metadata.uuid(),
    )))
}

pub(crate) fn position_mutation_read_schema(
    metadata: &TableMetadata,
    target_ref: &str,
    snapshot_id: Option<i64>,
) -> Result<std::sync::Arc<Schema>, ConnectorError> {
    match snapshot_id {
        Some(snapshot) if target_ref != "main" => {
            crate::typed_boundary::projection_schema_for_pinned_snapshot(metadata, snapshot)
        }
        _ => Ok(metadata.current_schema().clone()),
    }
}

pub(crate) fn invalid_write_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, SortOrder, TableMetadataBuilder,
        Type,
    };
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

    fn context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("original fixture request context")
    }

    fn metadata(properties: HashMap<String, String>) -> TableMetadata {
        let fields = [
            (1, "uuid", Type::Primitive(PrimitiveType::Uuid)),
            (
                2,
                "variant",
                Type::Variant(crate::iceberg::spec::VariantType),
            ),
            (3, "hll", Type::Primitive(PrimitiveType::Binary)),
            (4, "bitmap", Type::Primitive(PrimitiveType::Binary)),
            (5, "largeint", Type::Primitive(PrimitiveType::Fixed(16))),
            (6, "plain_fixed", Type::Primitive(PrimitiveType::Fixed(16))),
            (7, "plain_binary", Type::Primitive(PrimitiveType::Binary)),
        ]
        .into_iter()
        .map(|(id, name, field_type)| Arc::new(NestedField::optional(id, name, field_type)))
        .collect::<Vec<_>>();
        TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(fields)
                .build()
                .expect("schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/write_domain_test".to_string(),
            FormatVersion::V3,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn frozen_write_fields_keep_provider_uuid_variant_and_declared_opaque_domains() {
        let metadata = metadata(HashMap::from([(crate::schema_mapping::nominal_declaration::PROPERTY.into(),r#"{"version":1,"fields":[{"id":3,"domain":"hll"},{"id":4,"domain":"bitmap"},{"id":5,"domain":"largeint"}]}"#.into())]));
        let expected = [
            (
                "uuid",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            ("variant", DataType::LargeBinary, ValueLogicalType::Variant),
            ("hll", DataType::Binary, ValueLogicalType::Hll),
            ("bitmap", DataType::Binary, ValueLogicalType::Bitmap),
            (
                "largeint",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            ),
            (
                "plain_fixed",
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
            ),
            ("plain_binary", DataType::Binary, ValueLogicalType::Physical),
        ];
        // Names select the frozen provider fields. A caller's placeholder
        // carrier cannot author their target type or logical domain.
        let requests = expected
            .iter()
            .map(|(name, _, _)| {
                ConnectorWriteFieldRequest::new(Field::new(
                    name.to_uppercase(),
                    DataType::Null,
                    true,
                ))
            })
            .collect::<Vec<_>>();
        let fields = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            exact_requested_write_fields(&metadata, &requests, creation)
        })
        .expect("exact write fields");
        let authored =
            crate::scalar_integer_domain::metadata_sql_schema(&metadata, metadata.current_schema())
                .expect("provider schema");
        for (ordinal, (request, (name, carrier, domain))) in fields.iter().zip(expected).enumerate()
        {
            assert_eq!(request.field(), authored.field(ordinal));
            assert_eq!(request.field().name(), name);
            assert_eq!(
                request
                    .execution_type()
                    .expect("provider complete declaration")
                    .clone(),
                FunctionValueType::try_with_logical_type(carrier, true, domain)
                    .expect("expected provider type")
            );
        }
    }

    #[test]
    fn frozen_write_fields_reject_conflicting_provider_domain_declarations() {
        for (name, domain) in [
            ("plain_fixed", "hll"),
            ("plain_binary", "largeint"),
            ("uuid", "largeint"),
        ] {
            let metadata = metadata(HashMap::from([(
                crate::schema_mapping::nominal_declaration::PROPERTY.into(),
                format!(
                    r#"{{"version":1,"fields":[{{"id":{},"domain":"{domain}"}}]}}"#,
                    match name {
                        "plain_fixed" => 6,
                        "plain_binary" => 7,
                        "uuid" => 1,
                        _ => unreachable!(),
                    }
                ),
            )]));
            let request = ConnectorWriteFieldRequest::new(Field::new(name, DataType::Null, true));
            let error = crate::metadata::with_pure_schema_creation(&context(), |creation| {
                exact_requested_write_fields(&metadata, &[request], creation)
            })
            .expect_err("a conflicting provider declaration cannot be silently erased");
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        }
    }
    #[test]
    fn cancelled_writer_type_author_refuses_before_the_operation() {
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        stop.request_stop();
        let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            stop.view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .unwrap();
        let error = crate::metadata::with_pure_schema_creation::<()>(&context, |_| {
            panic!("no constructor tail after cancellation")
        })
        .expect_err("original cancellation is observed first");
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    }
}
