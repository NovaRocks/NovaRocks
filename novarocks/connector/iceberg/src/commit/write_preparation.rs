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
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorTableHandle,
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
    exact_requested_write_fields, invalid_write_activation, snapshot_token,
    write_target_snapshot_id,
};
use crate::control_provider::IcebergTablePayload;
use crate::file_reader::execution_payload::decode_payload;
use crate::iceberg::spec::{FormatVersion, TableMetadata};
use crate::storage_inspector::MV_DESCRIPTOR_PACKAGE_ID_PROP;

/// Sign the SQL-proposed Arrow input while the Iceberg provider still owns the
/// exact admitted table.  This is intentionally close to `IcebergTablePayload`:
/// no application layer can decode the handle, substitute a catalog field ID,
/// or recreate a preparation for another connector incarnation.
pub(crate) fn prepare_write(
    request: ConnectorWritePreparationRequest,
    owner: &ConnectorExecutionBindingKey,
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
    if matches!(request.purpose, ConnectorWriteAdmissionPurpose::OrdinaryDml)
        && metadata
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

    let target_fqn = format!("{}.{}.{}", table.catalog, table.namespace, table.table);
    if let Some(denied) = write_support_denial(&metadata, &request, &target_fqn) {
        return Ok(ConnectorWritePreparationOutcome::Denied(denied));
    }

    let input = bind_write_input(&request, owner, &metadata)?;
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
    ) {
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
                "ADD EQUALITY DELETE currently supports only unpartitioned iceberg tables"
                    .to_string(),
            ));
        }
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

    if matches!(request.intent, ConnectorWriteIntent::Overwrite) {
        if let Err(error) = ensure_overwrite_single_partition_spec_from_metadata(metadata) {
            return Some(invalid(error));
        }
        if let Err(error) = ensure_no_equality_deletes_from_metadata(metadata) {
            return Some(invalid(error));
        }
    }

    if matches!(request.intent, ConnectorWriteIntent::PartitionOverwrite)
        && metadata.default_partition_spec().is_unpartitioned()
    {
        return Some(invalid(format!(
            "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
             table {} is unpartitioned (use OVERWRITE without PARTITIONS)",
            target_fqn,
        )));
    }

    // Branch writes carry row-lineage semantics, which are Iceberg v3 only.
    if request.target_ref.as_str() != "main" {
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
    owner: &ConnectorExecutionBindingKey,
    metadata: &TableMetadata,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    Ok(match &request.input {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: bind_write_fields(
                &exact_data_write_fields(metadata, fields)?,
                owner,
                &request.table,
                request.intent,
                1,
            )?,
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: bind_write_fields(
                &exact_data_write_fields(metadata, data_fields)?,
                owner,
                &request.table,
                request.intent,
                2,
            )?,
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
        } => {
            let partition_source_fields = if partition_source_fields.is_empty() {
                position_delete_partition_field_requests(metadata)?
            } else {
                partition_source_fields.clone()
            };
            ConnectorWriteInputShape::PositionDelete {
                identity_fields: bind_write_fields(
                    identity_fields,
                    owner,
                    &request.table,
                    request.intent,
                    4,
                )?,
                partition_source_fields: bind_write_fields(
                    &partition_source_fields,
                    owner,
                    &request.table,
                    request.intent,
                    5,
                )?,
            }
        }
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => {
            let partition_source_fields = if partition_source_fields.is_empty() {
                position_delete_partition_field_requests(metadata)?
            } else {
                partition_source_fields.clone()
            };
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: bind_write_fields(
                    identity_fields,
                    owner,
                    &request.table,
                    request.intent,
                    6,
                )?,
                partition_source_fields: bind_write_fields(
                    &partition_source_fields,
                    owner,
                    &request.table,
                    request.intent,
                    7,
                )?,
            }
        }
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: bind_write_fields(
                    &exact_requested_write_fields(metadata, equality_fields)?,
                    owner,
                    &request.table,
                    request.intent,
                    8,
                )?,
            }
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
    exact_requested_write_fields(metadata, &requested_all)
}

/// Position-delete and deletion-vector SQL only name the fixed row identity.
/// The exact partition-source projection is provider-owned and is added while
/// the frozen Iceberg metadata is still available.
fn position_delete_partition_field_requests(
    metadata: &TableMetadata,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
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
            let data_type =
                crate::metadata_batch_reader::iceberg_type_to_arrow_type(source.field_type.as_ref())
                .map_err(invalid_write_activation)?;
            Ok(ConnectorWriteFieldRequest::new(Field::new(
                &source.name,
                data_type,
                !source.required,
            )))
        })
        .collect()
}

fn bind_write_fields(
    fields: &[ConnectorWriteFieldRequest],
    owner: &ConnectorExecutionBindingKey,
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
            Ok(ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes(hasher.finalize().into()),
                request.field().clone(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorWriteTargetRef,
    };

    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };
    use crate::metadata_batch_reader::MetadataTableType;
    use crate::scan_model::IcebergTableInfo;

    use super::*;

    #[derive(Default)]
    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("context")
    }

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
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
        owner: &ConnectorExecutionBindingKey,
        payload: &IcebergTablePayload,
    ) -> ConnectorTableHandle {
        ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            Bytes::from(serde_json::to_vec(payload).expect("table payload")),
        )
        .expect("table handle")
    }

    fn data_request(
        owner: &ConnectorExecutionBindingKey,
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
        owner: &ConnectorExecutionBindingKey,
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
    /// `sql-tests/iceberg-ddl/sql/partition_evolution_unsupported.sql`, which
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
            vec![Field::new("id", DataType::Int64, false)]
        );
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
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
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
        let other = ConnectorExecutionBindingKey {
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
}
