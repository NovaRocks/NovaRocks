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

//! Provider-owned `ConnectorWriteControl::prepare_write`.
//!
//! Signs the SQL-proposed Arrow input while the Iceberg provider still owns
//! the exact admitted table, so no application layer can decode the handle,
//! substitute a catalog field ID, or recreate a preparation for another
//! connector incarnation.

use arrow::datatypes::{DataType, Field};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorTableHandle,
    ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion, ConnectorWriteFieldBinding,
    ConnectorWriteFieldRequest, ConnectorWriteFieldToken, ConnectorWriteInputRequest,
    ConnectorWriteInputShape, ConnectorWriteIntent, ConnectorWritePreparation,
    ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
};
use sha2::{Digest, Sha256};

use crate::commit::validation::{
    ensure_equality_delete_single_partition_spec_from_metadata,
    ensure_iceberg_write_supported_from_metadata, ensure_no_equality_deletes_from_metadata,
    ensure_overwrite_single_partition_spec_from_metadata,
};
use crate::commit::write_shared::{
    exact_metadata_write_fields, exact_requested_write_fields, invalid_write_activation,
    snapshot_token, write_target_snapshot_id,
};
use crate::file_reader::execution_payload::decode_payload;
use crate::iceberg::spec::{FormatVersion, TableMetadata};
use crate::metadata::IcebergTablePayload;
use crate::row_lineage_synth::{is_iceberg_last_updated_sequence_number, is_iceberg_row_id};
use crate::storage_inspector::MV_DESCRIPTOR_PACKAGE_ID_PROP;

/// Sign the SQL-proposed Arrow input while the Iceberg provider still owns the
/// exact admitted table.  This is intentionally close to `IcebergTablePayload`:
/// no application layer can decode the handle, substitute a catalog field ID,
/// or recreate a preparation for another connector incarnation.
pub(crate) fn prepare_write(
    request: ConnectorWritePreparationRequest,
    owner: &ConnectorProviderBindingKey,
) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
    request.validate(owner)?;
    let payload: IcebergTablePayload =
        decode_payload(request.table.payload(), "admitted Iceberg write table")?;
    if payload.metadata_table_type.is_some() {
        return Ok(ConnectorWritePreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be write targets",
            ),
        ));
    }
    let table = payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write table is missing its frozen table descriptor",
        )
    })?;
    let serialized_metadata = table.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized_metadata).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg write metadata: {error}"),
        )
    })?;
    let managed = match crate::document_storage::keyed::view::management_marker(&metadata) {
        Ok(marker) => marker.is_some(),
        Err(error) => {
            return Ok(ConnectorWritePreparationOutcome::Denied(
                crate::document_storage::observation::keyed_read_error(error),
            ));
        }
    };
    if matches!(request.purpose, ConnectorWriteAdmissionPurpose::OrdinaryDml) {
        if managed
            || metadata
                .properties()
                .contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP)
        {
            return Ok(ConnectorWritePreparationOutcome::Denied(
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "table {}.{}.{} is a materialized view; use REFRESH MATERIALIZED VIEW to update it",
                        table.catalog, table.namespace, table.table
                    ),
                ),
            ));
        }
    }

    let target_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if let Some(denied) = write_support_denial(&metadata, &request, &target_fqn) {
        return Ok(ConnectorWritePreparationOutcome::Denied(denied));
    }

    let input = bind_write_input(&request, owner, &metadata)?;
    if let ConnectorWriteInputShape::EqualityDelete { equality_fields } = &input {
        // Reuse session admission's exact logical-key projection. The same
        // FixedSizeBinary carrier may represent uuid or ordinary fixed data;
        // neither the application nor the carrier can decide this policy.
        if let Err(error) = crate::commit::write_stack::control::equality_delete_recipe(
            &metadata,
            equality_fields,
            &target_fqn,
        ) {
            return Ok(ConnectorWritePreparationOutcome::Denied(error));
        }
    }
    let target_snapshot_id = write_target_snapshot_id(&metadata, request.target_ref.as_str())?;
    let table_uuid = table.table_uuid.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg write table is missing its table UUID",
        )
    })?;
    let snapshot = snapshot_token(target_snapshot_id);
    let base_version = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/write-base/v1/{table_uuid}/{}/{snapshot}",
        request.target_ref.as_str()
    )))?;
    let preparation_payload = Bytes::from(format!(
        "iceberg/write-preparation/v1/{}/{}/{}/{snapshot}",
        owner.instance_id.as_str(),
        table_uuid,
        request.target_ref.as_str()
    ));
    Ok(ConnectorWritePreparationOutcome::Prepared(
        ConnectorWritePreparation::try_new(
            owner.clone(),
            request.table,
            request.target_ref,
            request.intent,
            base_version,
            input,
            preparation_payload,
        )?,
    ))
}

/// Write-support guards for the INSERT-shaped intents and for the declared
/// equality-delete input shape.
///
/// These reject table shapes this writer cannot encode. They used to run in the
/// SQL application layer, which had to load a concrete Iceberg table to do it;
/// the rules are Iceberg facts, so they belong here, next to the frozen
/// metadata the provider already decoded.
///
/// Two independent scopings, because the two guard families answer to two
/// different callers:
///
/// - The INSERT-shaped guards run for `Append` / `Overwrite` /
///   `PartitionOverwrite` only. `RowDelta` is deliberately excluded:
///   row-mutation admission runs its own guards through
///   `row_mutation_strategy_from_metadata`, and widening this family *by intent*
///   would newly reject tables that position-delete and deletion-vector writes
///   accept today.
/// - The `ADD EQUALITY DELETE` guards run for the
///   `ConnectorWriteInputRequest::EqualityDelete` *input shape*. That shape is
///   declared by exactly one statement -- `ALTER TABLE ... ADD EQUALITY DELETE`
///   -- and by no other `RowDelta` write, so keying on it reaches the statement
///   these three rules were written for without touching the rest of `RowDelta`.
fn write_support_denial(
    metadata: &TableMetadata,
    request: &ConnectorWritePreparationRequest,
    target_fqn: &str,
) -> Option<ConnectorError> {
    let invalid =
        |message: String| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);

    if matches!(
        request.input,
        ConnectorWriteInputRequest::EqualityDelete { .. }
    ) && let Some(denied) = equality_delete_support_denial(metadata)
    {
        return Some(denied);
    }

    let insert_shaped = matches!(
        request.intent,
        ConnectorWriteIntent::Append
            | ConnectorWriteIntent::Overwrite
            | ConnectorWriteIntent::PartitionOverwrite
    );
    if !insert_shaped {
        return None;
    }

    if let Err(error) = ensure_iceberg_write_supported_from_metadata(metadata) {
        return Some(invalid(error));
    }

    insert_intent_support_denial(
        metadata,
        request.intent,
        request.target_ref.as_str(),
        target_fqn,
    )
}

/// Existing ADD EQUALITY DELETE table-shape policy. Both preparation and the
/// ordinary session call this before projecting an equality-key recipe, so the
/// caller cannot bypass a table-level refusal by skipping preparation.
pub(crate) fn equality_delete_support_denial(metadata: &TableMetadata) -> Option<ConnectorError> {
    let invalid = |message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);
    if metadata.format_version() == FormatVersion::V1 {
        return Some(invalid(
            "ADD EQUALITY DELETE requires an Iceberg v2 or v3 table".to_string(),
        ));
    }
    if let Err(error) = ensure_equality_delete_single_partition_spec_from_metadata(metadata) {
        return Some(invalid(error));
    }
    if !metadata.default_partition_spec().fields().is_empty() {
        return Some(invalid(
            "ADD EQUALITY DELETE currently supports only unpartitioned iceberg tables".to_string(),
        ));
    }
    None
}

/// INSERT-only metadata/ref policy shared by preparation and ordinary data
/// session admission. Generic writer support and equality-delete input policy
/// stay at their existing call sites, with their own error kinds and ordering.
pub(crate) fn insert_intent_support_denial(
    metadata: &TableMetadata,
    intent: ConnectorWriteIntent,
    target_ref: &str,
    target_fqn: &str,
) -> Option<ConnectorError> {
    if !matches!(
        intent,
        ConnectorWriteIntent::Append
            | ConnectorWriteIntent::Overwrite
            | ConnectorWriteIntent::PartitionOverwrite
    ) {
        return None;
    }
    if let Err(error) =
        crate::commit::validation::deny_unwritable_data_files(metadata.current_schema(), target_fqn)
    {
        return Some(error);
    }
    let invalid =
        |message: String| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);

    if matches!(intent, ConnectorWriteIntent::Overwrite) {
        if let Err(error) = ensure_overwrite_single_partition_spec_from_metadata(metadata) {
            return Some(invalid(error));
        }
        if let Err(error) = ensure_no_equality_deletes_from_metadata(metadata) {
            return Some(invalid(error));
        }
    }

    if matches!(intent, ConnectorWriteIntent::PartitionOverwrite)
        && metadata.default_partition_spec().is_unpartitioned()
    {
        return Some(invalid(format!(
            "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
             table {} is unpartitioned (use OVERWRITE without PARTITIONS)",
            target_fqn,
        )));
    }

    if matches!(intent, ConnectorWriteIntent::PartitionOverwrite)
        && metadata.format_version() != FormatVersion::V3
    {
        return Some(invalid(format!(
            "INSERT OVERWRITE PARTITIONS requires an Iceberg v3 row-lineage table; \
             table {} is v{}",
            target_fqn,
            metadata.format_version() as u8,
        )));
    }

    // Branch writes carry row-lineage semantics, which are Iceberg v3 only.
    if target_ref != "main" {
        let format_version = metadata.format_version();
        if format_version != FormatVersion::V3 {
            return Some(invalid(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                target_fqn, format_version as u8,
            )));
        }
    }

    None
}

fn bind_write_input(
    request: &ConnectorWritePreparationRequest,
    owner: &ConnectorProviderBindingKey,
    metadata: &TableMetadata,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let authored = freeze_write_input_request(metadata, &request.input, &request.context)?;
    Ok(match &authored {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: bind_write_fields(fields, owner, &request.table, request.intent, 1)?,
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: bind_write_fields(data_fields, owner, &request.table, request.intent, 2)?,
            row_identity_fields: bind_write_fields(
                row_identity_fields,
                owner,
                &request.table,
                request.intent,
                3,
            )?,
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::PositionDelete {
            identity_fields: bind_write_fields(
                identity_fields,
                owner,
                &request.table,
                request.intent,
                4,
            )?,
            partition_source_fields: bind_write_fields(
                partition_source_fields,
                owner,
                &request.table,
                request.intent,
                5,
            )?,
        },
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => ConnectorWriteInputShape::DeletionVector {
            identity_fields: bind_write_fields(
                identity_fields,
                owner,
                &request.table,
                request.intent,
                6,
            )?,
            partition_source_fields: bind_write_fields(
                partition_source_fields,
                owner,
                &request.table,
                request.intent,
                7,
            )?,
        },
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: bind_write_fields(
                    equality_fields,
                    owner,
                    &request.table,
                    request.intent,
                    8,
                )?,
            }
        }
    })
}

/// Re-author the canonical artifact relation only after begin_write validated
/// the retained position-delete rewrite's target, UUID, base and operation.
/// Ordinary row identities keep the strict scan-metadata declaration path.
pub(crate) fn freeze_position_delete_rewrite_input(
    metadata: &TableMetadata,
    input: &ConnectorWriteInputRequest,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorWriteInputRequest, ConnectorError> {
    let ConnectorWriteInputRequest::DeletionVector {
        identity_fields,
        partition_source_fields,
    } = input
    else {
        return Err(invalid_write_activation(
            "position-delete rewrite requires a deletion-vector artifact input",
        ));
    };
    crate::metadata::with_pure_schema_creation(context, |creation| {
        let declaration = crate::distributed_rewrite::delete_artifact_execution_schema(creation)
            .map_err(crate::metadata::metadata_creation_error)?;
        let fields = declaration.schema().fields();
        if identity_fields.len() != fields.len()
            || identity_fields
                .iter()
                .zip(fields)
                .any(|(source, expected)| source.field().name() != expected.name())
        {
            return Err(invalid_write_activation(
                "position-delete rewrite identity differs from its canonical artifact schema",
            ));
        }
        let mut authored = creation
            .reserve(fields.len())
            .map_err(crate::metadata::metadata_creation_error)?;
        for (ordinal, field) in fields.iter().enumerate() {
            creation
                .work(1)
                .map_err(crate::metadata::metadata_creation_error)?;
            authored.push(ConnectorWriteFieldRequest::try_new_typed(
                field.as_ref().clone(),
                declaration
                    .binding(ordinal)
                    .expect("provider artifact schema covers every identity field")
                    .clone(),
            )?);
        }
        Ok(ConnectorWriteInputRequest::DeletionVector {
            identity_fields: authored,
            partition_source_fields: exact_partition_source_write_fields(
                metadata,
                partition_source_fields,
                creation,
            )?,
        })
    })
}

/// Freeze complete provider declarations before either session-signing entry.
/// The loaded schema and exact metadata roles author execution semantics;
/// caller Field attributes remain source facts and supply no type authority.
pub(crate) fn freeze_write_input_request(
    metadata: &TableMetadata,
    input: &ConnectorWriteInputRequest,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorWriteInputRequest, ConnectorError> {
    crate::metadata::with_pure_schema_creation(context, |creation| {
        Ok(match input {
            ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputRequest::Data {
                fields: exact_data_write_fields(metadata, fields, creation)?,
            },
            ConnectorWriteInputRequest::RowLineage {
                data_fields,
                row_identity_fields,
            } => ConnectorWriteInputRequest::RowLineage {
                data_fields: exact_row_lineage_data_write_fields(metadata, data_fields, creation)?,
                row_identity_fields: exact_metadata_write_fields(
                    metadata,
                    row_identity_fields,
                    creation,
                )?,
            },
            ConnectorWriteInputRequest::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => ConnectorWriteInputRequest::PositionDelete {
                identity_fields: exact_metadata_write_fields(metadata, identity_fields, creation)?,
                partition_source_fields: exact_partition_source_write_fields(
                    metadata,
                    partition_source_fields,
                    creation,
                )?,
            },
            ConnectorWriteInputRequest::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => ConnectorWriteInputRequest::DeletionVector {
                identity_fields: exact_metadata_write_fields(metadata, identity_fields, creation)?,
                partition_source_fields: exact_partition_source_write_fields(
                    metadata,
                    partition_source_fields,
                    creation,
                )?,
            },
            ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
                ConnectorWriteInputRequest::EqualityDelete {
                    equality_fields: exact_requested_write_fields(
                        metadata,
                        equality_fields,
                        creation,
                    )?,
                }
            }
        })
    })
}

/// Check the caller's complete declaration against the independently frozen
/// provider input before the session signs it. Preparation may rebuild target
/// facts, but that must never silently repair a caller's execution contract.
pub(crate) fn validate_session_write_input_request(
    proposed: &ConnectorWriteInputRequest,
    authored: &ConnectorWriteInputRequest,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    crate::metadata::with_pure_schema_creation(context, |creation| {
        let mut fields = |proposed: &[ConnectorWriteFieldRequest],
                          authored: &[ConnectorWriteFieldRequest]|
         -> Result<(), ConnectorError> {
            if proposed.is_empty() {
                return Ok(());
            }
            let resource = || {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg session role name index extent is unrepresentable",
                )
            };
            let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
                String,
                &ConnectorWriteFieldRequest,
            >(authored.len())
            .map_err(|_| resource())?;
            let mut max_key_bytes = 0;
            for field in authored {
                creation
                    .work(field.field().name().len())
                    .map_err(crate::metadata::metadata_creation_error)?;
                max_key_bytes = max_key_bytes.max(field.field().name().len());
            }
            // Only this role's borrowed declarations are indexed. ASCII folding
            // retains eq_ignore_ascii_case semantics, including non-ASCII names.
            creation
                .work(1)
                .map_err(crate::metadata::metadata_creation_error)?;
            let mut by_name = std::collections::HashMap::new();
            if let Some(layout) = table.layout {
                creation
                    .request(layout)
                    .map_err(crate::metadata::metadata_creation_error)?;
                creation
                    .work(layout.size())
                    .map_err(crate::metadata::metadata_creation_error)?;
            }
            by_name
                .try_reserve(authored.len())
                .map_err(|_| resource())?;
            for field in authored {
                let mut name = creation
                    .text(field.field().name())
                    .map_err(crate::metadata::metadata_creation_error)?;
                creation
                    .work(name.len())
                    .map_err(crate::metadata::metadata_creation_error)?;
                name.make_ascii_lowercase();
                creation
                    .work(
                        novarocks_type_contract::owned_resources::hashmap::string_operations_work_upper_bound(
                            table.buckets, 1, name.len(), max_key_bytes,
                        )
                        .map_err(|_| resource())?,
                    )
                    .map_err(crate::metadata::metadata_creation_error)?;
                // Preserve the original scan's first match if an authoritative
                // role contains duplicate names; this validator adds no gate.
                by_name.entry(name).or_insert(field);
            }
            for source in proposed {
                creation
                    .work(1)
                    .map_err(crate::metadata::metadata_creation_error)?;
                let mut name = creation
                    .text(source.field().name())
                    .map_err(crate::metadata::metadata_creation_error)?;
                creation
                    .work(name.len())
                    .map_err(crate::metadata::metadata_creation_error)?;
                name.make_ascii_lowercase();
                creation
                    .work(
                        novarocks_type_contract::owned_resources::hashmap::string_operations_work_upper_bound(
                            table.buckets, 1, name.len(), max_key_bytes.max(name.len()),
                        )
                        .map_err(|_| resource())?,
                    )
                    .map_err(crate::metadata::metadata_creation_error)?;
                let target = by_name.get(name.as_str()).copied().ok_or_else(|| {
                    invalid_write_activation(
                        "Iceberg session source field is absent from its authoritative input role",
                    )
                })?;
                let actual = source.execution_type().ok_or_else(|| {
                    invalid_write_activation(
                        "Iceberg session source field has no complete execution declaration",
                    )
                })?;
                let expected = target.execution_type().ok_or_else(|| {
                    invalid_write_activation(
                        "Iceberg session target field has no authoritative complete declaration",
                    )
                })?;
                if actual.nullable() && !expected.nullable() {
                    return Err(invalid_write_activation(
                        "Iceberg session nullable source cannot satisfy a required target field",
                    ));
                }
                if !actual.same_physical_binding_ignoring_root_nullability_observed(
                    expected,
                    || {
                        creation
                            .work(1)
                            .map_err(crate::metadata::metadata_creation_error)
                    },
                )? {
                    // Retain the established narrow-integer refusal detail while
                    // comparing its real complete provider declaration.
                    let narrow = matches!(
                        expected.value_type().kind(),
                        novarocks_type_contract::ValueKind::Scalar(
                            DataType::Int8 | DataType::Int16
                        )
                    );
                    return Err(invalid_write_activation(
                        if narrow && actual.data_type() != expected.data_type() {
                            "Iceberg write input differs from the authoritative scalar integer declaration"
                        } else {
                            "Iceberg session source differs from its authoritative complete execution declaration"
                        },
                    ));
                }
            }
            Ok(())
        };
        match (proposed, authored) {
            (
                ConnectorWriteInputRequest::Data { fields: source },
                ConnectorWriteInputRequest::Data { fields: target },
            ) => fields(source, target),
            (
                ConnectorWriteInputRequest::RowLineage {
                    data_fields: source,
                    row_identity_fields: source_identity,
                },
                ConnectorWriteInputRequest::RowLineage {
                    data_fields: target,
                    row_identity_fields: target_identity,
                },
            ) => {
                fields(source, target)?;
                fields(source_identity, target_identity)
            }
            (
                ConnectorWriteInputRequest::PositionDelete {
                    identity_fields: source,
                    partition_source_fields: source_partition,
                },
                ConnectorWriteInputRequest::PositionDelete {
                    identity_fields: target,
                    partition_source_fields: target_partition,
                },
            )
            | (
                ConnectorWriteInputRequest::DeletionVector {
                    identity_fields: source,
                    partition_source_fields: source_partition,
                },
                ConnectorWriteInputRequest::DeletionVector {
                    identity_fields: target,
                    partition_source_fields: target_partition,
                },
            ) => {
                fields(source, target)?;
                fields(source_partition, target_partition)
            }
            (
                ConnectorWriteInputRequest::EqualityDelete {
                    equality_fields: source,
                },
                ConnectorWriteInputRequest::EqualityDelete {
                    equality_fields: target,
                },
            ) => fields(source, target),
            _ => Err(invalid_write_activation(
                "Iceberg session source and authoritative input roles differ",
            )),
        }
    })
}

/// Rebuild SQL-proposed data fields from the Provider-owned frozen Iceberg
/// schema before signing them. Arrow offset width is part of the execution
/// contract: Iceberg `binary` is `Binary`, while Iceberg `variant` is the
/// engine's encoded `LargeBinary` representation.
fn exact_data_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    for request in requested {
        if metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .all(|field| !field.name.eq_ignore_ascii_case(request.field().name()))
        {
            return Err(invalid_write_activation(format!(
                "Iceberg write input column `{}` is absent from the frozen target schema",
                request.field().name()
            )));
        }
    }
    let requested_all = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            ConnectorWriteFieldRequest::new(Field::new(
                &field.name,
                DataType::Null,
                !field.required,
            ))
        })
        .collect::<Vec<_>>();
    exact_requested_write_fields(metadata, &requested_all, creation)
}

/// Rebuild a row-lineage after-image's data fields from the frozen schema.
///
/// A row-lineage write carries Iceberg's reserved v3 lineage columns with the
/// after-image so a replaced row keeps the identity it already had. Those
/// columns are reserved table-format metadata, not schema fields, so the
/// frozen schema has nothing to rebuild them from and their absence from it is
/// the specification, not a defect in the request. They pass through exactly
/// as proposed -- which is what the write stack signs, and what the
/// copy-on-write rewrite branch carries through unchanged.
fn exact_row_lineage_data_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let is_lineage = |request: &ConnectorWriteFieldRequest| {
        is_iceberg_row_id(request.field().name())
            || is_iceberg_last_updated_sequence_number(request.field().name())
    };
    let target = requested
        .iter()
        .filter(|request| !is_lineage(request))
        .cloned()
        .collect::<Vec<_>>();
    let mut fields = exact_data_write_fields(metadata, &target, creation)?;
    let lineage = requested
        .iter()
        .filter(|request| is_lineage(request))
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(exact_metadata_write_fields(metadata, &lineage, creation)?);
    Ok(fields)
}

fn exact_partition_source_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let authored = position_delete_partition_field_requests(metadata, creation)?;
    if requested.is_empty() {
        return Ok(authored);
    }
    requested
        .iter()
        .map(|requested| {
            let original = authored
                .iter()
                .find(|field| {
                    field
                        .field()
                        .name()
                        .eq_ignore_ascii_case(requested.field().name())
                })
                .ok_or_else(|| {
                    invalid_write_activation(
                        "partition source field is absent from the frozen spec",
                    )
                })?;
            ConnectorWriteFieldRequest::try_new_typed(
                requested.field().clone(),
                original
                    .execution_type()
                    .expect("provider partition declaration")
                    .clone(),
            )
        })
        .collect()
}

/// Position-delete and deletion-vector SQL only name the fixed row identity.
/// The exact partition-source projection is provider-owned and is added while
/// the frozen Iceberg metadata is still available.
fn position_delete_partition_field_requests(
    metadata: &TableMetadata,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let declarations = crate::scalar_integer_domain::of_schema(
        metadata.current_schema(),
        &crate::scalar_integer_domain::metadata_declarations(metadata)?,
    )?;
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|partition| {
            let source = metadata
                .current_schema()
                .field_by_id(partition.source_id)
                .ok_or_else(|| {
                    invalid_write_activation(format!(
                        "Iceberg position-delete partition source field id {} is absent from the frozen schema",
                        partition.source_id
                    ))
                })?;
            if matches!(source.field_type.as_ref(), crate::iceberg::spec::Type::Variant(_)) {
                return Err(ConnectorError::new(ConnectorErrorKind::Unsupported,
                    "partition descriptor does not support variant payload"));
            }
            let data_type = crate::metadata_batch_reader::iceberg_type_to_arrow_type(source.field_type.as_ref())
                .map_err(invalid_write_activation)?;
            let data_type = match declarations.get(&source.id) {
                Some(domain) => domain.data_type(), None => data_type,
            };
            let binding = crate::metadata_batch_reader::metadata_alias_value_type(
                &data_type, !source.required, creation,
            ).map_err(crate::metadata::metadata_creation_error)?;
            ConnectorWriteFieldRequest::try_new_typed(
                Field::new(&source.name, data_type, !source.required), binding,
            )
        })
        .collect()
}

fn bind_write_fields(
    fields: &[ConnectorWriteFieldRequest],
    owner: &ConnectorProviderBindingKey,
    table: &ConnectorTableHandle,
    intent: ConnectorWriteIntent,
    domain: u8,
) -> Result<Vec<ConnectorWriteFieldBinding>, ConnectorError> {
    fields
        .iter()
        .enumerate()
        .map(|(ordinal, request)| {
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg.write-field-token.v1\0");
            hasher.update(owner.instance_id.as_str().as_bytes());
            hasher.update(owner.incarnation.to_bytes());
            hasher.update(table.payload());
            hasher.update(format!("{intent:?}").as_bytes());
            hasher.update([domain]);
            hasher.update((ordinal as u64).to_be_bytes());
            hasher.update(format!("{:?}", request.field()).as_bytes());
            let value = request.execution_type().ok_or_else(|| {
                invalid_write_activation(
                    "Iceberg writer field has no complete provider declaration",
                )
            })?;
            ConnectorWriteFieldBinding::try_new_typed(
                ConnectorWriteFieldToken::from_bytes(hasher.finalize().into()),
                request.field().clone(),
                value.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorRequestContext, ConnectorWriteTargetRef, ProviderBindingEpoch,
    };

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };
    use crate::metadata_batch_reader::MetadataTableType;
    use crate::scan_model::IcebergTableInfo;

    use super::*;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context")
    }

    fn owner() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        }
    }

    fn metadata_with_properties(properties: HashMap<String, String>) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            FormatVersion::V2,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    fn metadata() -> TableMetadata {
        metadata_with_properties(HashMap::new())
    }

    fn table_info(metadata: &TableMetadata) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some(metadata.uuid().to_string()),
            current_snapshot_id: metadata.current_snapshot_id(),
            schema_id: metadata.current_schema_id(),
            location: metadata.location().to_string(),
            schema: crate::schema_facts::iceberg_schema_def(metadata.current_schema()),
            serialized_metadata: Some(serde_json::to_string(metadata).expect("metadata JSON")),
            serialized_metadata_rows: None,
        }
    }

    fn table_payload(table_info: Option<IcebergTableInfo>) -> IcebergTablePayload {
        IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            metadata_location: None,
            table_info,
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        }
    }

    fn table_handle(
        owner: &ConnectorProviderBindingKey,
        payload: &IcebergTablePayload,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            Bytes::from(serde_json::to_vec(payload).expect("table payload")),
        )
        .expect("table handle")
    }

    fn data_request(
        owner: &ConnectorProviderBindingKey,
        payload: &IcebergTablePayload,
        purpose: ConnectorWriteAdmissionPurpose,
    ) -> ConnectorWritePreparationRequest {
        ConnectorWritePreparationRequest {
            table: table_handle(owner, payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::Append,
            purpose,
            input: ConnectorWriteInputRequest::Data {
                fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("id", DataType::Int64, false)),
                    ConnectorWriteFieldRequest::new(Field::new("name", DataType::Utf8, true)),
                ],
            },
            context: context(),
        }
    }

    /// `ALTER TABLE ... ADD EQUALITY DELETE` as it reaches the provider: the
    /// `RowDelta` intent it declares plus the equality-delete input shape that
    /// only this statement produces.
    fn equality_delete_request(
        owner: &ConnectorProviderBindingKey,
        payload: &IcebergTablePayload,
    ) -> ConnectorWritePreparationRequest {
        ConnectorWritePreparationRequest {
            table: table_handle(owner, payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                    "id",
                    DataType::Int64,
                    false,
                ))],
            },
            context: context(),
        }
    }

    /// A target column exactly as preparation signs it. Signed fields are the
    /// provider's frozen fields, so they carry the Iceberg field-id annotation
    /// the frozen schema authors; a request field only selects the column.
    fn provider_field(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    /// The two-column `id`/`name` schema every metadata fixture here shares.
    fn fixture_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn metadata_with_format_version(format_version: FormatVersion) -> TableMetadata {
        TableMetadataBuilder::new(
            fixture_schema(),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            format_version,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    /// An identity partition spec over `name`, optionally with a second
    /// (default) spec over `id` so the table reads as partition-evolved.
    fn partitioned_metadata(evolved: bool) -> TableMetadata {
        let schema = fixture_schema();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("name", "name", crate::iceberg::spec::Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let builder = TableMetadataBuilder::new(
            schema.clone(),
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/partitioned".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        let builder = if evolved {
            let second = PartitionSpec::builder(schema)
                .with_spec_id(1)
                .add_partition_field("id", "id", crate::iceberg::spec::Transform::Identity)
                .expect("partition field")
                .build()
                .expect("second partition spec");
            builder
                .add_default_partition_spec(second.into_unbound())
                .expect("evolve partition spec")
        } else {
            builder
        };
        builder.build().expect("metadata").metadata
    }

    /// `ConnectorWritePreparationOutcome` is deliberately not `Debug`, so the
    /// fault branches cannot use `expect_err`.
    fn expect_error(
        result: Result<ConnectorWritePreparationOutcome, ConnectorError>,
    ) -> ConnectorError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("expected a fault, not an admission outcome"),
        }
    }

    fn expect_denied(outcome: ConnectorWritePreparationOutcome) -> ConnectorError {
        match outcome {
            ConnectorWritePreparationOutcome::Denied(error) => error,
            ConnectorWritePreparationOutcome::Prepared(_) => {
                panic!("expected a policy denial, not a prepared admission")
            }
        }
    }

    fn expect_prepared(outcome: ConnectorWritePreparationOutcome) -> ConnectorWritePreparation {
        match outcome {
            ConnectorWritePreparationOutcome::Prepared(preparation) => preparation,
            ConnectorWritePreparationOutcome::Denied(error) => {
                panic!("expected a prepared admission, got denial: {error}")
            }
        }
    }

    #[test]
    fn variant_and_uuid_data_admission_denies_before_input_binding() {
        for data_type in [
            Type::Variant(crate::iceberg::spec::VariantType),
            Type::Primitive(PrimitiveType::Uuid),
        ] {
            let metadata = TableMetadataBuilder::new(
                Schema::builder()
                    .with_fields(vec![NestedField::optional(1, "payload", data_type).into()])
                    .build()
                    .unwrap(),
                PartitionSpec::unpartition_spec().into_unbound(),
                SortOrder::unsorted_order(),
                "file:///warehouse/db/t".to_owned(),
                FormatVersion::V3,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
            for intent in [
                ConnectorWriteIntent::Append,
                ConnectorWriteIntent::Overwrite,
                ConnectorWriteIntent::PartitionOverwrite,
            ] {
                let denied = insert_intent_support_denial(&metadata, intent, "main", "ice.db.t")
                    .expect("data-file writes are denied");
                assert_eq!(denied.kind(), ConnectorErrorKind::Unsupported);
                assert!(denied.message().contains("column payload"), "{denied}");
            }
            let owner = owner();
            let payload = table_payload(Some(table_info(&metadata)));
            // This request asks for absent id/name columns: the type denial
            // must win before any input declaration or execution allocation.
            for purpose in [
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            ] {
                let request = data_request(&owner, &payload, purpose);
                let denied = expect_denied(prepare_write(request, &owner).unwrap());
                assert_eq!(denied.kind(), ConnectorErrorKind::Unsupported);
                assert!(denied.message().contains("ice.db.t"), "{denied}");
            }
        }
    }

    /// SPI-5M relocated the write-support guards out of the SQL layer, which
    /// had to load a concrete Iceberg table to run them, into write
    /// preparation, which already holds the frozen admitted metadata. These
    /// four cases pin the relocated INSERT-shaped behaviour: the two
    /// conditional denials, the absence of a false denial on the ordinary path,
    /// and the deliberate exclusion of `RowDelta`. The `spi5m_add_equality_delete_*`
    /// cases below pin the second, input-shape-keyed family.
    ///
    /// The `INSERT OVERWRITE` evolved-partition-spec and
    /// pre-existing-equality-delete denials are covered end to end by
    /// `tests/sql/correctness/iceberg-ddl/sql/partition_evolution_unsupported.sql`, which
    /// asserts their messages through `@expect_error`.
    #[test]
    fn spi5m_branch_write_on_a_pre_v3_table_is_denied_by_write_preparation() {
        let owner = owner();
        let metadata = metadata(); // FormatVersion::V2
        let payload = table_payload(Some(table_info(&metadata)));
        let mut request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        request.target_ref = ConnectorWriteTargetRef::parse("nightly").expect("branch ref");

        let error = expect_denied(prepare_write(request, &owner).expect("prepare branch write"));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: iceberg ref: branch writes require Iceberg v3 tables \
             (table ice.db.t is v2)",
            "the relocated guard must reproduce the message the SQL layer used to emit"
        );
    }

    #[test]
    fn spi5m_partition_overwrite_on_an_unpartitioned_table_is_denied_by_write_preparation() {
        let owner = owner();
        let metadata = metadata(); // unpartition_spec
        let payload = table_payload(Some(table_info(&metadata)));
        let mut request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        request.intent = ConnectorWriteIntent::PartitionOverwrite;

        let error =
            expect_denied(prepare_write(request, &owner).expect("prepare partition overwrite"));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: INSERT OVERWRITE PARTITIONS requires a partitioned table; \
             table ice.db.t is unpartitioned (use OVERWRITE without PARTITIONS)",
        );
    }

    #[test]
    fn partition_overwrite_on_a_pre_v3_partitioned_table_is_denied_before_commit() {
        let owner = owner();
        let metadata = partitioned_metadata(false); // FormatVersion::V2
        let payload = table_payload(Some(table_info(&metadata)));
        let mut request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        request.intent = ConnectorWriteIntent::PartitionOverwrite;

        let error =
            expect_denied(prepare_write(request, &owner).expect("prepare v2 partition overwrite"));
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: INSERT OVERWRITE PARTITIONS requires an Iceberg v3 row-lineage table; \
             table ice.db.t is v2",
        );
    }

    #[test]
    fn spi5m_append_on_main_is_not_denied_by_the_relocated_write_guards() {
        let owner = owner();
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );

        // A plain v2 append must stay admissible: relocating the guards must
        // not widen the rejection set.
        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare ordinary append"));
        assert_eq!(preparation.intent(), ConnectorWriteIntent::Append);
    }

    #[test]
    fn spi5m_row_delta_branch_write_is_left_to_row_mutation_admission() {
        let owner = owner();
        let metadata = metadata(); // FormatVersion::V2
        let payload = table_payload(Some(table_info(&metadata)));
        let mut request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        request.intent = ConnectorWriteIntent::RowDelta;
        request.target_ref = ConnectorWriteTargetRef::parse("nightly").expect("branch ref");

        // The INSERT-shaped guards deliberately skip RowDelta: row-mutation
        // admission enforces its own v3 branch rule through
        // `row_mutation_strategy_from_metadata`. Running them here would newly
        // reject tables position-delete and deletion-vector writes accept
        // today. Note this request declares the `Data` input shape, so the
        // equality-delete guards do not apply either.
        //
        // Proof that the guard was skipped: this same request under an
        // INSERT-shaped intent is denied with the v3 message (see the branch
        // test above), whereas RowDelta gets past it and only then hits the
        // pre-existing downstream ref-resolution check.
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error.to_string(),
            "InvalidRequest: iceberg ref: branch 'nightly' not found in table metadata",
            "RowDelta must reach ref resolution, not the INSERT-shaped v3 guard"
        );
    }

    /// SPI-5M also relocated the three `ALTER TABLE ... ADD EQUALITY DELETE`
    /// table-shape gates here from `engine::delete_engine::equality`, which had
    /// to load a concrete Iceberg table to answer them. They are keyed on the
    /// equality-delete *input shape* rather than on `RowDelta`, so no other
    /// row-delta write picks them up. Each message is reproduced byte for byte;
    /// the `Iceberg write admission denied:` prefix is added by the Core
    /// boundary when it unwraps `Denied`.
    #[test]
    fn spi5m_add_equality_delete_on_a_v1_table_is_denied_by_write_preparation() {
        let owner = owner();
        let metadata = metadata_with_format_version(FormatVersion::V1);
        let payload = table_payload(Some(table_info(&metadata)));

        let error = expect_denied(
            prepare_write(equality_delete_request(&owner, &payload), &owner)
                .expect("prepare equality delete"),
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: ADD EQUALITY DELETE requires an Iceberg v2 or v3 table",
        );
    }

    #[test]
    fn spi5m_add_equality_delete_on_an_evolved_partition_spec_is_denied_by_write_preparation() {
        let owner = owner();
        let metadata = partitioned_metadata(true);
        let payload = table_payload(Some(table_info(&metadata)));

        // The evolved-spec gate runs before the unpartitioned gate, which this
        // fixture would also trip: `partition_evolution_unsupported.sql`
        // asserts exactly this message for exactly this table shape.
        let error = expect_denied(
            prepare_write(equality_delete_request(&owner, &payload), &owner)
                .expect("prepare equality delete"),
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error.to_string().starts_with(
                "InvalidRequest: ADD EQUALITY DELETE on an evolved Iceberg table is not \
                 supported yet: "
            ),
            "{error}"
        );
    }

    #[test]
    fn spi5m_add_equality_delete_on_a_partitioned_table_is_denied_by_write_preparation() {
        let owner = owner();
        let metadata = partitioned_metadata(false);
        let payload = table_payload(Some(table_info(&metadata)));

        let error = expect_denied(
            prepare_write(equality_delete_request(&owner, &payload), &owner)
                .expect("prepare equality delete"),
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "InvalidRequest: ADD EQUALITY DELETE currently supports only unpartitioned \
             iceberg tables",
        );
    }

    #[test]
    fn spi5m_add_equality_delete_on_an_unpartitioned_v2_table_is_admitted() {
        let owner = owner();
        let metadata = metadata(); // unpartitioned, FormatVersion::V2, single spec
        let payload = table_payload(Some(table_info(&metadata)));

        // Relocating the three gates must not widen the rejection set: the
        // shape ADD EQUALITY DELETE actually supports stays admissible.
        let preparation = expect_prepared(
            prepare_write(equality_delete_request(&owner, &payload), &owner)
                .expect("prepare equality delete"),
        );
        assert_eq!(preparation.intent(), ConnectorWriteIntent::RowDelta);
        let ConnectorWriteInputShape::EqualityDelete { equality_fields } = preparation.input()
        else {
            panic!("equality-delete input must sign an equality-delete shape");
        };
        assert_eq!(
            equality_fields
                .iter()
                .map(|binding| binding.field().clone())
                .collect::<Vec<_>>(),
            vec![provider_field("id", DataType::Int64, false, 1)]
        );
    }

    #[test]
    fn equality_preparation_checks_selected_logical_keys_not_shared_fixed_carriers_or_whole_table()
    {
        let owner = owner();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "uuid_key", Type::Primitive(PrimitiveType::Uuid)).into(),
                NestedField::optional(3, "raw_key", Type::Primitive(PrimitiveType::Fixed(16)))
                    .into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///tmp/equality-logical-types".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let payload = table_payload(Some(table_info(&metadata)));
        for (name, carrier, nullable, denied) in [
            ("uuid_key", DataType::FixedSizeBinary(16), true, true),
            ("raw_key", DataType::FixedSizeBinary(16), true, false),
            ("id", DataType::Int64, false, false),
        ] {
            let mut request = equality_delete_request(&owner, &payload);
            request.input = ConnectorWriteInputRequest::EqualityDelete {
                equality_fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                    name, carrier, nullable,
                ))],
            };
            let outcome = prepare_write(request, &owner).unwrap();
            if denied {
                let error = expect_denied(outcome);
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert!(error.message().contains("uuid"));
                assert!(error.message().contains("table `ice.db.t`"));
                assert!(error.message().contains("column `uuid_key`"));
                assert!(!error.retryable_before_progress());
            } else {
                let preparation = expect_prepared(outcome);
                let ConnectorWriteInputShape::EqualityDelete { equality_fields } =
                    preparation.input()
                else {
                    panic!("equality input")
                };
                assert_eq!(equality_fields.len(), 1);
                assert_eq!(equality_fields[0].field().name(), name);
            }
        }
    }

    /// The relocated gates must stay invisible to every other row-delta write.
    /// A partitioned table is the sharpest probe: position-delete writes accept
    /// it today, and the unpartitioned gate would reject it if the scoping had
    /// been widened to `RowDelta`.
    #[test]
    fn spi5m_position_delete_on_a_partitioned_table_skips_the_equality_delete_gates() {
        let owner = owner();
        let metadata = partitioned_metadata(false);
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::PositionDelete {
                identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
                partition_source_fields: Vec::new(),
            },
            context: context(),
        };

        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare position delete"));
        assert_eq!(preparation.intent(), ConnectorWriteIntent::RowDelta);
    }

    #[test]
    fn data_write_preparation_signs_the_exact_frozen_schema() {
        let owner = owner();
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let table = request.table.clone();
        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare data write"));

        assert_eq!(preparation.target_ref().as_str(), "main");
        assert_eq!(preparation.intent(), ConnectorWriteIntent::Append);

        // The frozen schema decides the signed Arrow layout, not the request.
        let ConnectorWriteInputShape::Data { fields } = preparation.input() else {
            panic!("data input must sign a data shape");
        };
        assert_eq!(
            fields
                .iter()
                .map(|binding| binding.field().clone())
                .collect::<Vec<_>>(),
            vec![
                provider_field("id", DataType::Int64, false, 1),
                provider_field("name", DataType::Utf8, true, 2),
            ]
        );

        let uuid = metadata.uuid().to_string();
        let expected_base = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
            "iceberg/write-base/v1/{uuid}/main/none"
        )))
        .expect("expected base version");
        assert_eq!(preparation.base_version(), &expected_base);

        // Rebuilding with the expected payload must reproduce the digest, which
        // pins the preparation payload string byte for byte.
        let expected = ConnectorWritePreparation::try_new(
            owner.clone(),
            table,
            ConnectorWriteTargetRef::main(),
            ConnectorWriteIntent::Append,
            expected_base,
            preparation.input().clone(),
            Bytes::from(format!("iceberg/write-preparation/v1/ice/{uuid}/main/none")),
        )
        .expect("expected preparation");
        assert_eq!(preparation.digest(), expected.digest());
    }

    #[test]
    fn deletion_vector_preparation_adds_the_provider_owned_partition_projection() {
        let owner = owner();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field(
                "region",
                "region",
                crate::iceberg::spec::Transform::Identity,
            )
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = TableMetadataBuilder::new(
            schema,
            partition_spec.into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/partitioned".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::DeletionVector {
                identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
                partition_source_fields: Vec::new(),
            },
            context: context(),
        };
        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare deletion vector"));
        let ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } = preparation.input()
        else {
            panic!("deletion-vector input must sign a deletion-vector shape");
        };
        assert_eq!(identity_fields.len(), 2);
        assert_eq!(
            partition_source_fields
                .iter()
                .map(|binding| binding.field().clone())
                .collect::<Vec<_>>(),
            vec![Field::new("region", DataType::Utf8, false)]
        );
    }

    /// A row-lineage after-image carries Iceberg's reserved v3 lineage columns
    /// so a replaced row keeps the identity it already had. Those columns are
    /// reserved table-format metadata: the frozen schema deliberately does not
    /// list them, so demanding that it does would refuse every merge-on-read
    /// mutation and every incremental MV publication.
    #[test]
    fn a_row_lineage_after_image_may_carry_the_reserved_lineage_columns() {
        let owner = owner();
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
            input: ConnectorWriteInputRequest::RowLineage {
                data_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("id", DataType::Int64, false)),
                    ConnectorWriteFieldRequest::new(Field::new("name", DataType::Utf8, true)),
                    ConnectorWriteFieldRequest::new(Field::new("_row_id", DataType::Int64, true)),
                    ConnectorWriteFieldRequest::new(Field::new(
                        "_last_updated_sequence_number",
                        DataType::Int64,
                        true,
                    )),
                ],
                row_identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
            },
            context: context(),
        };
        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare row lineage"));
        let ConnectorWriteInputShape::RowLineage { data_fields, .. } = preparation.input() else {
            panic!("a row-lineage input must sign a row-lineage shape");
        };
        // The target's own columns are rebuilt from the frozen schema; the
        // reserved lineage columns pass through as proposed, because the schema
        // holds nothing to rebuild them from.
        assert_eq!(
            data_fields
                .iter()
                .map(|binding| binding.field().clone())
                .collect::<Vec<_>>(),
            vec![
                provider_field("id", DataType::Int64, false, 1),
                provider_field("name", DataType::Utf8, true, 2),
                Field::new("_row_id", DataType::Int64, true),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ]
        );
    }

    /// The tolerance above belongs to the row-lineage after-image alone. A
    /// plain data write that names a reserved lineage column is naming a
    /// column the table does not have, and the commit would have nothing to
    /// write into it.
    #[test]
    fn a_plain_data_write_still_refuses_a_reserved_lineage_column() {
        let owner = owner();
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::Append,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::Data {
                fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("id", DataType::Int64, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_row_id", DataType::Int64, true)),
                ],
            },
            context: context(),
        };
        let Err(error) = prepare_write(request, &owner) else {
            panic!("a reserved column is not a field the frozen schema can supply");
        };
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg write input column `_row_id` is absent from the frozen target schema",
            )
        );
    }

    #[test]
    fn metadata_table_target_is_denied() {
        let owner = owner();
        let metadata = metadata();
        let mut payload = table_payload(Some(table_info(&metadata)));
        payload.metadata_table_type = Some(MetadataTableType::Snapshots);
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_denied(prepare_write(request, &owner).expect("metadata-table outcome"));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be write targets",
            )
        );
    }

    #[test]
    fn managed_materialized_view_denies_ordinary_dml_only() {
        let owner = owner();
        let metadata = metadata_with_properties(HashMap::from([(
            MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
            "package-1".to_string(),
        )]));
        let payload = table_payload(Some(table_info(&metadata)));

        let denied = expect_denied(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::OrdinaryDml,
                ),
                &owner,
            )
            .expect("managed MV outcome"),
        );
        assert_eq!(
            denied,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "table ice.db.t is a materialized view; use REFRESH MATERIALIZED VIEW to update it",
            )
        );

        // Refresh is the sanctioned writer for the same managed target.
        expect_prepared(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
                ),
                &owner,
            )
            .expect("managed MV refresh outcome"),
        );
    }

    #[test]
    fn keyed_management_slot_denies_ordinary_dml_but_allows_refresh() {
        let owner = owner();
        let slot =
            novarocks_spi::connector::ConnectorSlotName::try_new("novarocks.storage", "management")
                .unwrap();
        let mut content = Vec::new();
        for value in ["materialized-view", "deployment", "writer"] {
            content.extend_from_slice(&(value.len() as u32).to_be_bytes());
            content.extend_from_slice(value.as_bytes());
        }
        let documents = novarocks_spi::connector::KeyedCreate {
            slots: std::collections::BTreeMap::from([(
                slot,
                novarocks_spi::connector::ConnectorSlotValue {
                    format: novarocks_spi::connector::ConnectorDocumentFormat::try_new(
                        "novarocks.mv",
                        "management",
                        1,
                    )
                    .unwrap(),
                    content: bytes::Bytes::from(content),
                },
            )]),
            ..Default::default()
        };
        let metadata = metadata_with_properties(
            crate::document_storage::keyed::write::create_properties(&documents).unwrap(),
        );
        let payload = table_payload(Some(table_info(&metadata)));

        let denied = expect_denied(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::OrdinaryDml,
                ),
                &owner,
            )
            .expect("application-managed MV outcome"),
        );
        assert_eq!(denied.kind(), ConnectorErrorKind::InvalidRequest);

        expect_prepared(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
                ),
                &owner,
            )
            .expect("application-managed MV refresh outcome"),
        );
    }

    #[test]
    fn legacy_managed_marker_refuses_all_writes() {
        let owner = owner();
        let metadata = metadata_with_properties(HashMap::from([(
            "novarocks.managed.owner".to_string(),
            "deployment".to_string(),
        )]));
        let payload = table_payload(Some(table_info(&metadata)));

        let denied = expect_denied(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::OrdinaryDml,
                ),
                &owner,
            )
            .expect("partial managed marker outcome"),
        );
        assert_eq!(denied.kind(), ConnectorErrorKind::Unsupported);
        assert!(denied.message().contains("only DROP TABLE is supported"));
        let denied = expect_denied(
            prepare_write(
                data_request(
                    &owner,
                    &payload,
                    ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
                ),
                &owner,
            )
            .unwrap(),
        );
        assert_eq!(denied.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn missing_frozen_table_descriptor_is_a_fault() {
        let owner = owner();
        let payload = table_payload(None);
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg write table is missing its frozen table descriptor",
            )
        );
    }

    #[test]
    fn missing_frozen_metadata_is_a_fault() {
        let owner = owner();
        let metadata = metadata();
        let mut info = table_info(&metadata);
        info.serialized_metadata = None;
        let payload = table_payload(Some(info));
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg write table is missing frozen metadata",
            )
        );
    }

    #[test]
    fn undecodable_frozen_metadata_is_corrupt_data() {
        let owner = owner();
        let metadata = metadata();
        let mut info = table_info(&metadata);
        info.serialized_metadata = Some("{\"not\":\"table metadata\"}".to_string());
        let payload = table_payload(Some(info));
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_error(prepare_write(request, &owner));
        let decode_error = serde_json::from_str::<TableMetadata>("{\"not\":\"table metadata\"}")
            .expect_err("fixture must not decode");
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode admitted Iceberg write metadata: {decode_error}"),
            )
        );
    }

    #[test]
    fn missing_table_uuid_is_corrupt_data() {
        let owner = owner();
        let metadata = metadata();
        let mut info = table_info(&metadata);
        info.table_uuid = None;
        let payload = table_payload(Some(info));
        let request = data_request(
            &owner,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "admitted Iceberg write table is missing its table UUID",
            )
        );
    }

    #[test]
    fn write_input_column_outside_the_frozen_schema_is_rejected() {
        let owner = owner();
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::Append,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::Data {
                fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                    "absent",
                    DataType::Int64,
                    true,
                ))],
            },
            context: context(),
        };
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg write input column `absent` is absent from the frozen target schema",
            )
        );
    }

    #[test]
    fn a_table_owned_by_another_instance_is_rejected_before_decoding() {
        let owner = owner();
        let other = ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("other").expect("instance"),
            incarnation: owner.incarnation,
        };
        let metadata = metadata();
        let payload = table_payload(Some(table_info(&metadata)));
        let request = data_request(
            &other,
            &payload,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        );
        let error = expect_error(prepare_write(request, &owner));
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write preparation table does not match the exact control owner",
            )
        );
    }
    #[test]
    fn deletion_vector_preparation_preserves_declared_i16_partition_source() {
        let owner = owner();
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "region", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field(
                "region",
                "region",
                crate::iceberg::spec::Transform::Identity,
            )
            .expect("partition field")
            .build()
            .expect("partition spec");
        let metadata = TableMetadataBuilder::new(
            schema,
            partition_spec.into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/partitioned".to_string(),
            FormatVersion::V2,
            HashMap::from([(
                crate::scalar_integer_domain::PROPERTY.to_string(),
                r#"{"2":"smallint"}"#.to_string(),
            )]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let payload = table_payload(Some(table_info(&metadata)));
        let request = ConnectorWritePreparationRequest {
            table: table_handle(&owner, &payload),
            target_ref: ConnectorWriteTargetRef::main(),
            intent: ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::DeletionVector {
                identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
                partition_source_fields: Vec::new(),
            },
            context: context(),
        };
        let preparation =
            expect_prepared(prepare_write(request, &owner).expect("prepare deletion vector"));
        let ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } = preparation.input()
        else {
            panic!("deletion-vector input must sign a deletion-vector shape");
        };
        assert_eq!(identity_fields.len(), 2);
        assert_eq!(
            partition_source_fields
                .iter()
                .map(|binding| binding.field().clone())
                .collect::<Vec<_>>(),
            vec![Field::new("region", DataType::Int16, false)]
        );
    }
    fn partition_domain_metadata(
        field_type: crate::iceberg::spec::Type,
        transform: crate::iceberg::spec::Transform,
        partition_name: &str,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::optional(7, "u", field_type).into()])
            .build()
            .unwrap();
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .add_partition_field("u", partition_name, transform)
            .unwrap()
            .build()
            .unwrap();
        TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "memory://routing_alias".into(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata
    }

    #[test]
    fn table_uuid_and_routing_alias_keep_separate_complete_declarations() {
        let metadata = partition_domain_metadata(
            crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Uuid),
            crate::iceberg::spec::Transform::Identity,
            "u",
        );
        let requested = [ConnectorWriteFieldRequest::new(Field::new(
            "u",
            DataType::Null,
            true,
        ))];
        let data = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            exact_requested_write_fields(&metadata, &requested, creation)
        })
        .unwrap();
        let aliases = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            position_delete_partition_field_requests(&metadata, creation)
        })
        .unwrap();
        let data_type = data[0].execution_type().unwrap();
        let alias = aliases[0].execution_type().unwrap();
        assert_eq!(
            data_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Uuid
        );
        assert_eq!(data_type.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            alias.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical
        );
        assert_eq!(alias.data_type(), &DataType::Utf8);
        assert!(data_type.nullable());
        assert!(alias.nullable());
        assert_eq!(aliases[0].field(), &Field::new("u", DataType::Utf8, true));
        assert!(aliases[0].field().metadata().is_empty());
    }

    #[test]
    fn variant_partition_keeps_the_original_descriptor_refusal() {
        let metadata = partition_domain_metadata(
            crate::iceberg::spec::Type::Variant(crate::iceberg::spec::VariantType),
            // SDK0.11 rejects Variant Identity before our descriptor boundary.
            // Void is a legal frozen source and must still hit the same refusal.
            crate::iceberg::spec::Transform::Void,
            "u_void",
        );
        let error = crate::metadata::with_pure_schema_creation(&context(), |creation| {
            position_delete_partition_field_requests(&metadata, creation)
        })
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            error.message(),
            "partition descriptor does not support variant payload"
        );
    }
    fn declared_session_field(
        value: novarocks_type_contract::FunctionValueType,
    ) -> ConnectorWriteFieldRequest {
        ConnectorWriteFieldRequest::try_new_typed(
            Field::new("v", value.data_type().clone(), value.nullable()),
            value,
        )
        .unwrap()
    }

    #[test]
    fn session_complete_input_allows_only_root_null_widening_and_exact_role() {
        use novarocks_type_contract::FunctionValueType;
        let source = ConnectorWriteInputRequest::Data {
            fields: vec![declared_session_field(FunctionValueType::new(
                DataType::Int64,
                false,
            ))],
        };
        let target = ConnectorWriteInputRequest::Data {
            fields: vec![declared_session_field(FunctionValueType::new(
                DataType::Int64,
                true,
            ))],
        };
        validate_session_write_input_request(&source, &target, &context()).unwrap();
        assert_eq!(
            validate_session_write_input_request(&target, &source, &context())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let wrong_role = ConnectorWriteInputRequest::EqualityDelete {
            equality_fields: match &target {
                ConnectorWriteInputRequest::Data { fields } => fields.clone(),
                _ => unreachable!(),
            },
        };
        assert_eq!(
            validate_session_write_input_request(&source, &wrong_role, &context())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let absent = ConnectorWriteInputRequest::Data {
            fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                "v",
                DataType::Int64,
                false,
            ))],
        };
        assert_eq!(
            validate_session_write_input_request(&absent, &target, &context())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn session_role_index_preserves_reordering_subsets_ascii_first_match_and_empty_partition() {
        use novarocks_type_contract::FunctionValueType;
        let field = |name: &str, data_type: DataType| {
            ConnectorWriteFieldRequest::try_new_typed(
                Field::new(name, data_type.clone(), false),
                FunctionValueType::new(data_type, false),
            )
            .unwrap()
        };
        let authored = ConnectorWriteInputRequest::Data {
            fields: vec![
                field("a", DataType::Int64),
                field("b", DataType::Int64),
                field("A", DataType::Utf8),
                field("Ä", DataType::Int64),
            ],
        };
        let proposed = ConnectorWriteInputRequest::Data {
            // Independent declarations: reordered, a subset, and ASCII aliases.
            fields: vec![field("B", DataType::Int64), field("A", DataType::Int64)],
        };
        validate_session_write_input_request(&proposed, &authored, &context()).unwrap();
        let exact_non_ascii = ConnectorWriteInputRequest::Data {
            fields: vec![field("Ä", DataType::Int64)],
        };
        validate_session_write_input_request(&exact_non_ascii, &authored, &context()).unwrap();
        let unicode_case_alias = ConnectorWriteInputRequest::Data {
            fields: vec![field("ä", DataType::Int64)],
        };
        let error =
            validate_session_write_input_request(&unicode_case_alias, &authored, &context())
                .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "Iceberg session source field is absent from its authoritative input role"
        );
        let proposed = ConnectorWriteInputRequest::PositionDelete {
            identity_fields: vec![field("_pos", DataType::Int64)],
            partition_source_fields: vec![],
        };
        let authored = ConnectorWriteInputRequest::PositionDelete {
            identity_fields: vec![field("_pos", DataType::Int64)],
            partition_source_fields: vec![field("provider_partition", DataType::Int64)],
        };
        validate_session_write_input_request(&proposed, &authored, &context()).unwrap();
    }

    #[test]
    fn session_complete_input_rejects_same_carrier_nominal_and_child_null_drift() {
        use novarocks_type_contract::{
            FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
            ValueTypeStorageLifetime,
        };
        let list = |domain, child_nullable| {
            let storage = ValueTypeStorageLifetime::pure();
            let child =
                FunctionValueType::try_with_logical_type(DataType::Utf8, child_nullable, domain)
                    .unwrap();
            let semantic = ValueTypeRef::try_new(
                ValueKind::List(child.value_type().clone()),
                false,
                storage.clone(),
            )
            .unwrap();
            let layout = ValueLayout::try_new(
                &DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    child_nullable,
                ))),
                storage,
            )
            .unwrap();
            FunctionValueType::try_from_value(semantic, layout).unwrap()
        };
        let target_value = list(ValueLogicalType::Json, false);
        let target = ConnectorWriteInputRequest::Data {
            fields: vec![declared_session_field(target_value.clone())],
        };
        validate_session_write_input_request(&target, &target, &context()).unwrap();
        for wrong in [
            list(ValueLogicalType::Physical, false),
            list(ValueLogicalType::Json, true),
        ] {
            let source = ConnectorWriteInputRequest::Data {
                fields: vec![declared_session_field(wrong)],
            };
            assert_eq!(
                validate_session_write_input_request(&source, &target, &context())
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::InvalidRequest
            );
        }
    }
}
