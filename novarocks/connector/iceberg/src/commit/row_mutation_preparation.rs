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

//! Provider-owned `ConnectorWriteControl::prepare_row_mutation`.
//!
//! Plans a logical row mutation under one exact write-control generation. The
//! provider owns the physical strategy, the signed match layout, and the
//! base version; the application only persists what it is handed.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorMutationEffectField,
    ConnectorMutationMatchContract, ConnectorMutationSourceField, ConnectorMutationTargetField,
    ConnectorRowMutationEffect, ConnectorRowMutationPreparation,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorTableHandle, ConnectorWriteBaseVersion, ConnectorWriteFieldRequest,
    ConnectorWriteFieldToken,
};
use sha2::{Digest, Sha256};

use crate::commit::validation::row_mutation_strategy_from_metadata;
use crate::commit::write_shared::{
    exact_requested_write_fields_at_schema, snapshot_token, write_target_schema,
    write_target_snapshot_id,
};
use crate::control_provider::{IcebergTablePayload, metadata_arrow_fields};
use crate::file_reader::execution_payload::{decode_payload, encode_payload};
use crate::iceberg::spec::{FormatVersion, TableMetadata};

/// Provider-side row-mutation admission. This is intentionally independent of
/// `prepare_write`: it chooses the table-format strategy and signs identity
/// facts before any staging service is registered.
pub(crate) fn prepare_row_mutation(
    request: ConnectorRowMutationPreparationRequest,
    owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
    request.validate(owner)?;
    let payload: IcebergTablePayload = decode_payload(
        request.table.payload(),
        "admitted Iceberg row-mutation table",
    )?;
    if payload.metadata_table_type.is_some() {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be row-mutation targets",
            ),
        ));
    }
    let table = payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg row-mutation table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata =
        serde_json::from_str(table.serialized_metadata.as_deref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg row-mutation table has no serialized metadata",
            )
        })?)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode admitted Iceberg row-mutation metadata: {error}"),
            )
        })?;
    // The managed-materialized-view rejection deliberately does NOT live here.
    // Incremental MV refresh drives its own change-stream writes through this
    // same admission, so a check at this level cannot tell a user DML statement
    // apart from the MV machinery maintaining its own target. That rejection
    // stays at the SQL entry points, where `reject_if_iceberg_mv_table` already
    // makes it from neutral metadata under the same exact lease.

    // Writing to a non-main branch needs the v3 row-lineage semantics the
    // branch writer relies on.
    if request.target_ref.as_str() != "main" && metadata.format_version() != FormatVersion::V3 {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                    table.table,
                    metadata.format_version() as u8,
                ),
            ),
        ));
    }
    // The strategy rule, the fail-fast write guards it runs first, and the
    // merge-on-read override for a MERGE that can delete all live in the
    // provider. A policy rejection here is a denial, not an internal fault.
    let strategy = match row_mutation_strategy_from_metadata(&metadata, &request.intent) {
        Ok(strategy) => strategy,
        Err(error) => {
            return Ok(ConnectorRowMutationPreparationOutcome::Denied(
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error),
            ));
        }
    };
    // A MERGE that can append has no target identity or before-image for its
    // Insert rows.  The signed match layout therefore declares precisely those
    // fields nullable; Delete/Replace validation still rejects null keys.
    let insert_eligible = request.intent.accepts(ConnectorRowMutationEffect::Insert);
    let identity_fields = metadata_arrow_fields(&payload.metadata_columns)?
        .into_iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorMutationSourceField::new(
                row_mutation_identity_token(owner, &request.table, ordinal),
                field
                    .as_ref()
                    .clone()
                    .with_nullable(field.is_nullable() || insert_eligible),
                ordinal as u32,
            )
        })
        .collect::<Vec<_>>();
    if identity_fields.is_empty() {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg row-mutation target has no admitted identity fields",
            ),
        ));
    }
    let table_uuid = table.table_uuid.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg row-mutation table is missing table UUID",
        )
    })?;
    let target_snapshot_id = write_target_snapshot_id(&metadata, request.target_ref.as_str())?;
    let target_iceberg_schema = write_target_schema(&metadata, target_snapshot_id)?;
    let snapshot = snapshot_token(target_snapshot_id);
    let base_version = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/row-mutation-base/v1/{table_uuid}/{}/{snapshot}",
        request.target_ref.as_str()
    )))?;
    // The match layout is provider-signed rather than name-derived by SQL:
    // source identities precede target before/after values and the logical
    // effect field is last.  The source/target ordinals are the sole cross
    // layer binding; these familiar Iceberg names never become a Core rule.
    let requested_target_fields = target_iceberg_schema
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
    let target_schema = Schema::new(
        exact_requested_write_fields_at_schema(&target_iceberg_schema, &requested_target_fields)?
            .into_iter()
            .map(|field| Arc::new(field.field().clone()))
            .collect::<Vec<_>>(),
    );
    // The match query must scan the exact target ref/base chosen above. The
    // ordinary admitted table handle names the provider's default/current ref,
    // so reusing it would silently scan main for a branch mutation. Freeze a
    // second provider-owned handle whose `Current` selector means this exact
    // admitted snapshot, and sign the schema that handle will produce.
    let mut match_source_payload = payload.clone();
    match_source_payload.prepared_files.clear();
    match_source_payload.explicit_files = None;
    match_source_payload.row_mutation_frozen_source = false;
    let match_table_info = match_source_payload.table_info.as_mut().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg row-mutation table lost its frozen descriptor",
        )
    })?;
    match_table_info.current_snapshot_id = target_snapshot_id;
    match_table_info.schema_id = target_iceberg_schema.schema_id();
    match_table_info.schema = crate::schema_facts::iceberg_schema_def(&target_iceberg_schema);
    let mut match_source_fields = target_schema.fields().to_vec();
    match_source_fields.extend(metadata_arrow_fields(&payload.metadata_columns)?);
    let match_source_schema = Arc::new(Schema::new(match_source_fields));
    let match_source = ConnectorTableHandle::try_new(
        owner.instance_id.clone(),
        encode_payload(
            &match_source_payload,
            "row-mutation match source",
            request.context.max_handle_payload_bytes(),
        )?,
    )?;
    let target_start = u32::try_from(identity_fields.len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg row-mutation identity layout exceeds u32 ordinals",
        )
    })?;
    let target_width = u32::try_from(target_schema.fields().len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg row-mutation target layout exceeds u32 ordinals",
        )
    })?;
    let before_fields = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorMutationTargetField::new(
                row_mutation_field_token(owner, &request.table, b"before", ordinal),
                field
                    .as_ref()
                    .clone()
                    .with_nullable(field.is_nullable() || insert_eligible),
                target_start + u32::try_from(ordinal).expect("target ordinal fits u32"),
            )
        })
        .collect::<Vec<_>>();
    let after_fields = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorMutationTargetField::new(
                row_mutation_field_token(owner, &request.table, b"after", ordinal),
                field.as_ref().clone(),
                target_start
                    + target_width
                    + u32::try_from(ordinal).expect("target ordinal fits u32"),
            )
        })
        .collect::<Vec<_>>();
    let effect_field = ConnectorMutationEffectField::try_new(
        row_mutation_effect_token(owner, &request.table),
        Field::new("__row_mutation_effect", DataType::Int8, false),
        target_start
            .checked_add(target_width.checked_mul(2).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-mutation target layout overflowed",
                )
            })?)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-mutation effect ordinal overflowed",
                )
            })?,
    )?;
    // `_last_updated_sequence_number` is a signed source role, but it is not
    // part of row uniqueness.  COW rewrites bind the same token separately as
    // the writer's forward-looking version field. Keeping it in the match
    // tuple would make one token claim both roles and the SPI correctly
    // rejects that ambiguous recipe.
    let uniqueness_tokens = identity_fields
        .iter()
        .filter(|field| {
            !field
                .field()
                .name()
                .eq_ignore_ascii_case(crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL)
        })
        .map(ConnectorMutationSourceField::token)
        .collect::<Vec<_>>();
    if uniqueness_tokens.is_empty() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg row-mutation identity has no non-version uniqueness field",
        ));
    }
    let contract = ConnectorMutationMatchContract::try_new(
        owner.clone(),
        request.table.clone(),
        base_version.clone(),
        identity_fields,
        before_fields,
        after_fields,
        uniqueness_tokens,
        effect_field,
    )?;
    let preparation_payload = Bytes::from(format!(
        "iceberg/row-mutation-preparation/v1/{}/{table_uuid}/{}/{snapshot}/{strategy:?}",
        owner.instance_id.as_str(),
        request.target_ref.as_str()
    ));
    Ok(ConnectorRowMutationPreparationOutcome::Prepared(
        ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            request.operation_id,
            request.table,
            match_source,
            match_source_schema,
            request.target_ref,
            request.intent,
            base_version,
            contract,
            strategy,
            // The application persists this in its durable DML journal. It is
            // the same ref-scoped resolution the SQL entry points used to run
            // against a concrete table handle: the current snapshot for main,
            // the branch head otherwise.
            target_snapshot_id,
            // The sequence number this mutation's rows will belong to. A
            // merge-on-read writer stamps it on every rewritten row, so it has
            // to be known before the commit exists.
            Some(metadata.last_sequence_number() + 1),
            preparation_payload,
        )?,
    ))
}

/// Sign one admitted source-identity column.
///
/// The hashed byte sequence is a frozen cross-layer domain: any change here
/// silently invalidates every preparation already issued against it.
fn row_mutation_identity_token(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    ordinal: usize,
) -> ConnectorWriteFieldToken {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-identity.v1\0");
    hasher.update(owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update(table.payload());
    hasher.update((ordinal as u64).to_be_bytes());
    ConnectorWriteFieldToken::from_bytes(hasher.finalize().into())
}

/// Sign one before/after target column. `role` is length-prefixed so that no
/// two role/ordinal pairs can collide by concatenation.
fn row_mutation_field_token(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    role: &[u8],
    ordinal: usize,
) -> ConnectorWriteFieldToken {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-field.v1\0");
    hasher.update(owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update(table.payload());
    hasher.update((role.len() as u64).to_be_bytes());
    hasher.update(role);
    hasher.update((ordinal as u64).to_be_bytes());
    ConnectorWriteFieldToken::from_bytes(hasher.finalize().into())
}

/// Sign the single logical-effect column. Unlike the identity and target
/// domains this one deliberately does not mix in the incarnation.
fn row_mutation_effect_token(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
) -> ConnectorWriteFieldToken {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-effect.v1\0");
    hasher.update(owner.instance_id.as_str().as_bytes());
    hasher.update(table.payload());
    ConnectorWriteFieldToken::from_bytes(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorRequestContext, ConnectorRowMutationIntent, ConnectorRowMutationStrategy,
        ConnectorWriteOperationId, ConnectorWriteTargetRef,
    };

    use super::*;
    use crate::commit::types::{
        NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_COW, NOVAROCKS_UPDATE_MODE_MOR,
    };
    use crate::iceberg::spec::{
        NestedField, Operation, PartitionSpec, PrimitiveType, Schema as IcebergSchema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use crate::metadata_batch_reader::MetadataTableType;
    use crate::scan_model::IcebergTableInfo;
    use crate::schema_facts::iceberg_schema_def;

    /// The fixed identity columns SQL admits for an Iceberg row mutation.
    const IDENTITY_COLUMNS: [&str; 2] = ["_file", "_pos"];
    const ROW_LINEAGE_ON: (&str, &str) = ("write.row-lineage", "true");

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn instance_id() -> ConnectorInstanceId {
        ConnectorInstanceId::parse("ice").expect("instance id")
    }

    /// A binding key with a pinned incarnation, so every signed token in these
    /// tests is reproducible byte for byte.
    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: instance_id(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            16 * 1024,
            64 * 1024,
        )
        .expect("request context")
    }

    fn iceberg_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn metadata(format_version: FormatVersion, props: &[(&str, &str)]) -> TableMetadata {
        let schema = Arc::new(iceberg_schema());
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("spec");
        let sort_order = SortOrder::builder().build_unbound().expect("sort order");
        TableMetadataBuilder::new(
            schema.as_ref().clone(),
            partition_spec,
            sort_order,
            "file:///tmp/row-mutation".to_string(),
            format_version,
            props
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata build")
        .metadata
    }

    fn metadata_with_older_base_schema() -> TableMetadata {
        let base_schema = Arc::new(iceberg_schema());
        let snapshot = Snapshot::builder()
            .with_snapshot_id(41)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:///tmp/row-mutation/snap-41.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: BTreeMap::new().into_iter().collect(),
            })
            .with_schema_id(0)
            .with_row_range(0, 0)
            .build();
        let evolved = IcebergSchema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "later", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("evolved schema");
        let evolved_snapshot = Snapshot::builder()
            .with_snapshot_id(42)
            .with_parent_snapshot_id(Some(41))
            .with_sequence_number(2)
            .with_timestamp_ms(2)
            .with_manifest_list("file:///tmp/row-mutation/snap-42.avro".to_string())
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: BTreeMap::new().into_iter().collect(),
            })
            .with_schema_id(2)
            .with_row_range(0, 0)
            .build();
        TableMetadataBuilder::new(
            base_schema.as_ref().clone(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///tmp/row-mutation".to_string(),
            FormatVersion::V3,
            [(ROW_LINEAGE_ON.0.to_string(), ROW_LINEAGE_ON.1.to_string())]
                .into_iter()
                .collect(),
        )
        .expect("metadata builder")
        .add_snapshot(snapshot)
        .expect("base snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                41,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("main ref")
        .add_schema(evolved)
        .expect("evolved schema")
        .set_current_schema(-1)
        .expect("current schema")
        .add_snapshot(evolved_snapshot)
        .expect("evolved snapshot")
        .set_ref(
            "main",
            SnapshotReference::new(
                42,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("main ref")
        .set_ref(
            "dev",
            SnapshotReference::new(
                41,
                SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            ),
        )
        .expect("dev ref")
        .build()
        .expect("metadata")
        .metadata
    }

    struct PayloadSpec {
        serialized_metadata: Option<String>,
        table_uuid: Option<String>,
        metadata_columns: Vec<String>,
        metadata_table_type: Option<MetadataTableType>,
        drop_table_info: bool,
    }

    impl PayloadSpec {
        fn new(metadata: &TableMetadata) -> Self {
            Self {
                serialized_metadata: Some(
                    serde_json::to_string(metadata).expect("serialize metadata"),
                ),
                table_uuid: Some("11111111-2222-3333-4444-555555555555".to_string()),
                metadata_columns: IDENTITY_COLUMNS.iter().map(|s| (*s).to_string()).collect(),
                metadata_table_type: None,
                drop_table_info: false,
            }
        }

        fn handle(self) -> ConnectorTableHandle {
            let table_info = (!self.drop_table_info).then(|| IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                table_uuid: self.table_uuid,
                current_snapshot_id: None,
                schema_id: 1,
                location: "file:///tmp/row-mutation".to_string(),
                schema: iceberg_schema_def(&iceberg_schema()),
                serialized_metadata: self.serialized_metadata,
                serialized_metadata_rows: None,
            });
            let payload = IcebergTablePayload {
                namespace: "db".to_string(),
                table: "t".to_string(),
                table_info,
                metadata_columns: self.metadata_columns,
                metadata_table_type: self.metadata_table_type,
                prepared_files: Vec::new(),
                explicit_files: None,
                row_mutation_frozen_source: false,
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
            };
            ConnectorTableHandle::try_new(
                instance_id(),
                Bytes::from(serde_json::to_vec(&payload).expect("encode table payload")),
            )
            .expect("table handle")
        }
    }

    fn request(
        table: ConnectorTableHandle,
        target_ref: &str,
        intent: ConnectorRowMutationIntent,
    ) -> ConnectorRowMutationPreparationRequest {
        ConnectorRowMutationPreparationRequest {
            operation_id: ConnectorWriteOperationId::from_bytes([9; 16]),
            table,
            target_ref: ConnectorWriteTargetRef::parse(target_ref.to_string()).expect("target ref"),
            intent,
            context: context(),
        }
    }

    fn prepared(
        outcome: ConnectorRowMutationPreparationOutcome,
    ) -> ConnectorRowMutationPreparation {
        match outcome {
            ConnectorRowMutationPreparationOutcome::Prepared(preparation) => preparation,
            ConnectorRowMutationPreparationOutcome::Denied(error) => {
                panic!("expected a prepared row mutation, got denial: {error}")
            }
        }
    }

    /// `ConnectorRowMutationPreparationOutcome` is not `Debug`, so `expect_err`
    /// is unavailable on the outcome result.
    fn failure(
        result: Result<ConnectorRowMutationPreparationOutcome, ConnectorError>,
        what: &str,
    ) -> ConnectorError {
        match result {
            Err(error) => error,
            Ok(_) => panic!("{what} must fail"),
        }
    }

    fn denial(outcome: ConnectorRowMutationPreparationOutcome) -> ConnectorError {
        match outcome {
            ConnectorRowMutationPreparationOutcome::Denied(error) => error,
            ConnectorRowMutationPreparationOutcome::Prepared(_) => {
                panic!("expected a denial, got a prepared row mutation")
            }
        }
    }

    fn hex(bytes: [u8; 32]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn merge(effects: &[ConnectorRowMutationEffect]) -> ConnectorRowMutationIntent {
        ConnectorRowMutationIntent::Merge {
            effects: effects.to_vec(),
        }
    }

    // ---------------------------------------------------------------------
    // Success path / intent matrix
    // ---------------------------------------------------------------------

    #[test]
    fn delete_on_v3_prepares_a_deletion_vector_with_the_signed_match_layout() {
        let owner = owner();
        let table_metadata = metadata(FormatVersion::V3, &[]);
        let handle = PayloadSpec::new(&table_metadata).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Delete),
                &owner,
            )
            .expect("prepare"),
        );

        assert_eq!(
            preparation.strategy(),
            ConnectorRowMutationStrategy::DeletionVector
        );
        assert_eq!(preparation.base_version_ordinal(), None);
        assert_eq!(
            preparation.written_version_ordinal(),
            Some(table_metadata.last_sequence_number() + 1)
        );
        assert_eq!(
            preparation.payload().as_ref(),
            b"iceberg/row-mutation-preparation/v1/ice/11111111-2222-3333-4444-555555555555/main/none/DeletionVector"
        );

        let contract = preparation.match_contract();
        // identity | before | after | effect
        assert_eq!(
            contract
                .identity_fields()
                .iter()
                .map(|field| (field.field().name().clone(), field.source_ordinal()))
                .collect::<Vec<_>>(),
            vec![("_file".to_string(), 0), ("_pos".to_string(), 1)]
        );
        assert_eq!(
            contract
                .before_fields()
                .iter()
                .map(|field| (field.field().name().clone(), field.target_ordinal()))
                .collect::<Vec<_>>(),
            vec![("id".to_string(), 2), ("name".to_string(), 3)]
        );
        assert_eq!(
            contract
                .after_fields()
                .iter()
                .map(|field| (field.field().name().clone(), field.target_ordinal()))
                .collect::<Vec<_>>(),
            vec![("id".to_string(), 4), ("name".to_string(), 5)]
        );
        assert_eq!(
            contract.effect_field().field(),
            &Field::new("__row_mutation_effect", DataType::Int8, false)
        );
        assert_eq!(contract.effect_field().target_ordinal(), 6);
        assert_eq!(
            contract.uniqueness_tokens(),
            contract
                .identity_fields()
                .iter()
                .map(ConnectorMutationSourceField::token)
                .collect::<Vec<_>>()
        );
        // Delete never accepts Insert, so nothing is widened.
        assert!(!contract.identity_fields()[0].field().is_nullable());
        assert!(!contract.before_fields()[0].field().is_nullable());
        assert!(!contract.after_fields()[0].field().is_nullable());
    }

    #[test]
    fn delete_on_v2_prepares_position_deletes() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V2, &[])).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Delete),
                &owner,
            )
            .expect("prepare"),
        );
        assert_eq!(
            preparation.strategy(),
            ConnectorRowMutationStrategy::PositionDelete
        );
        assert!(preparation.payload().ends_with(b"/none/PositionDelete"));
    }

    #[test]
    fn preparation_and_frozen_source_share_the_resolved_base_snapshot_schema() {
        let owner = owner();
        let table_metadata = metadata_with_older_base_schema();
        assert_eq!(
            table_metadata.current_schema().as_struct().fields().len(),
            3
        );
        let preparation = prepared(
            prepare_row_mutation(
                request(
                    PayloadSpec::new(&table_metadata).handle(),
                    "dev",
                    ConnectorRowMutationIntent::Delete,
                ),
                &owner,
            )
            .expect("prepare"),
        );
        assert_eq!(preparation.base_version_ordinal(), Some(41));
        let match_source: IcebergTablePayload = decode_payload(
            preparation.match_source().payload(),
            "row-mutation match source",
        )
        .expect("decode match source");
        let match_info = match_source.table_info.expect("match table info");
        assert_eq!(match_info.current_snapshot_id, Some(41));
        assert_eq!(match_info.schema_id, 0);
        let prepared_names = preparation
            .match_contract()
            .after_fields()
            .iter()
            .map(|field| field.field().name().as_str())
            .collect::<Vec<_>>();
        let frozen_schema =
            write_target_schema(&table_metadata, Some(41)).expect("frozen source schema");
        let frozen_names = frozen_schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            prepared_names,
            frozen_names.iter().map(String::as_str).collect::<Vec<_>>()
        );
        assert_eq!(prepared_names, vec!["id", "name"]);
        assert_eq!(preparation.match_source_schema().field(0).name(), "id");
        assert_eq!(preparation.match_source_schema().field(1).name(), "name");
        assert_eq!(preparation.match_source_schema().field(2).name(), "_file");
    }

    #[test]
    fn written_version_identity_is_not_a_uniqueness_token() {
        let owner = owner();
        let table_metadata = metadata(FormatVersion::V3, &[ROW_LINEAGE_ON]);
        let mut spec = PayloadSpec::new(&table_metadata);
        spec.metadata_columns = vec![
            "_file".to_string(),
            "_pos".to_string(),
            "_row_id".to_string(),
            crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
        ];
        let preparation = prepared(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Update),
                &owner,
            )
            .expect("prepare"),
        );
        let contract = preparation.match_contract();
        let written = contract
            .identity_fields()
            .iter()
            .find(|field| {
                field
                    .field()
                    .name()
                    .eq_ignore_ascii_case(crate::row_lineage_synth::ICEBERG_LAST_UPDATED_SEQ_COL)
            })
            .expect("written version identity")
            .token();
        assert!(!contract.uniqueness_tokens().contains(&written));
        assert_eq!(contract.identity_fields().len(), 4);
        assert_eq!(contract.uniqueness_tokens().len(), 3);
    }

    #[test]
    fn update_follows_the_table_write_mode_and_never_widens_nullability() {
        let owner = owner();
        for (props, expected) in [
            (
                vec![ROW_LINEAGE_ON],
                ConnectorRowMutationStrategy::CopyOnWrite,
            ),
            (
                vec![
                    ROW_LINEAGE_ON,
                    (NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_MOR),
                ],
                ConnectorRowMutationStrategy::MergeOnRead,
            ),
        ] {
            let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &props)).handle();
            let preparation = prepared(
                prepare_row_mutation(
                    request(handle, "main", ConnectorRowMutationIntent::Update),
                    &owner,
                )
                .expect("prepare"),
            );
            assert_eq!(preparation.strategy(), expected);
            let contract = preparation.match_contract();
            // Update accepts Replace, never Insert: no field is widened.
            assert!(!contract.identity_fields()[0].field().is_nullable());
            assert!(!contract.before_fields()[0].field().is_nullable());
            assert!(!contract.after_fields()[0].field().is_nullable());
        }
    }

    #[test]
    fn merge_that_can_delete_is_forced_merge_on_read_even_under_copy_on_write() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(
            FormatVersion::V3,
            &[
                ROW_LINEAGE_ON,
                (NOVAROCKS_UPDATE_MODE, NOVAROCKS_UPDATE_MODE_COW),
            ],
        ))
        .handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(
                    handle,
                    "main",
                    merge(&[
                        ConnectorRowMutationEffect::Delete,
                        ConnectorRowMutationEffect::Replace,
                    ]),
                ),
                &owner,
            )
            .expect("prepare"),
        );
        assert_eq!(
            preparation.strategy(),
            ConnectorRowMutationStrategy::MergeOnRead
        );
        let contract = preparation.match_contract();
        assert!(!contract.identity_fields()[0].field().is_nullable());
        assert!(!contract.before_fields()[0].field().is_nullable());
    }

    #[test]
    fn merge_that_can_insert_widens_identity_and_before_but_not_after() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &[ROW_LINEAGE_ON])).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(
                    handle,
                    "main",
                    merge(&[
                        ConnectorRowMutationEffect::Replace,
                        ConnectorRowMutationEffect::Insert,
                    ]),
                ),
                &owner,
            )
            .expect("prepare"),
        );
        let contract = preparation.match_contract();
        // `_file` and `_pos` are non-null Iceberg metadata columns; an Insert
        // row has neither, so exactly the identity and before images widen.
        assert!(
            contract
                .identity_fields()
                .iter()
                .all(|field| field.field().is_nullable())
        );
        assert!(
            contract
                .before_fields()
                .iter()
                .all(|field| field.field().is_nullable())
        );
        // `id` is a required Iceberg column and must stay non-null after the
        // mutation; the after image is deliberately not widened.
        assert!(!contract.after_fields()[0].field().is_nullable());
        assert!(contract.after_fields()[1].field().is_nullable());
    }

    #[test]
    fn merge_that_only_inserts_still_widens_identity_and_before() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &[ROW_LINEAGE_ON])).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(handle, "main", merge(&[ConnectorRowMutationEffect::Insert])),
                &owner,
            )
            .expect("prepare"),
        );
        assert_eq!(
            preparation.strategy(),
            ConnectorRowMutationStrategy::CopyOnWrite
        );
        assert!(
            preparation.match_contract().identity_fields()[0]
                .field()
                .is_nullable()
        );
    }

    #[test]
    fn merge_that_can_delete_and_insert_is_merge_on_read_and_widened() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &[ROW_LINEAGE_ON])).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(
                    handle,
                    "main",
                    merge(&[
                        ConnectorRowMutationEffect::Delete,
                        ConnectorRowMutationEffect::Replace,
                        ConnectorRowMutationEffect::Insert,
                    ]),
                ),
                &owner,
            )
            .expect("prepare"),
        );
        assert_eq!(
            preparation.strategy(),
            ConnectorRowMutationStrategy::MergeOnRead
        );
        let contract = preparation.match_contract();
        assert!(contract.identity_fields()[1].field().is_nullable());
        assert!(contract.before_fields()[0].field().is_nullable());
        assert!(!contract.after_fields()[0].field().is_nullable());
    }

    /// A v3 table clears the branch gate, after which the shared ref resolver
    /// owns the outcome: an absent branch is its `InvalidRequest`, not a
    /// silent fall back to the current snapshot.
    #[test]
    fn a_branch_write_on_v3_defers_to_the_shared_ref_resolver() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &[])).handle();
        let error = failure(
            prepare_row_mutation(
                request(handle, "dev", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "an absent branch",
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "iceberg ref: branch 'dev' not found in table metadata",
            )
        );
    }

    // ---------------------------------------------------------------------
    // Denials and errors
    // ---------------------------------------------------------------------

    #[test]
    fn a_metadata_table_target_is_denied() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.metadata_table_type = Some(MetadataTableType::Files);
        let error = denial(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            )
            .expect("outcome"),
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be row-mutation targets",
            )
        );
    }

    #[test]
    fn a_table_without_frozen_metadata_is_an_invalid_request() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.drop_table_info = true;
        let error = failure(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "missing frozen metadata",
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg row-mutation table is missing frozen metadata",
            )
        );
    }

    #[test]
    fn a_table_without_serialized_metadata_is_an_invalid_request() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.serialized_metadata = None;
        let error = failure(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "missing serialized metadata",
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg row-mutation table has no serialized metadata",
            )
        );
    }

    #[test]
    fn undecodable_metadata_is_corrupt_data() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.serialized_metadata = Some("{not-json".to_string());
        let error = failure(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "undecodable metadata",
        );
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            error
                .to_string()
                .starts_with("CorruptData: decode admitted Iceberg row-mutation metadata: "),
            "{error}"
        );
    }

    #[test]
    fn an_undecodable_table_handle_is_corrupt_data() {
        let owner = owner();
        let handle = ConnectorTableHandle::try_new(instance_id(), Bytes::from_static(b"{nope"))
            .expect("handle");
        let error = failure(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "undecodable handle",
        );
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            error
                .to_string()
                .starts_with("CorruptData: decode Iceberg admitted Iceberg row-mutation table: "),
            "{error}"
        );
    }

    #[test]
    fn a_branch_write_on_a_non_v3_table_is_denied_with_the_actual_format_version() {
        let owner = owner();
        for (format_version, digit) in [(FormatVersion::V1, 1), (FormatVersion::V2, 2)] {
            let handle = PayloadSpec::new(&metadata(format_version, &[])).handle();
            let error = denial(
                prepare_row_mutation(
                    request(handle, "dev", ConnectorRowMutationIntent::Delete),
                    &owner,
                )
                .expect("outcome"),
            );
            assert_eq!(
                error,
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "iceberg ref: branch writes require Iceberg v3 tables (table t is v{digit})"
                    ),
                )
            );
        }
    }

    #[test]
    fn a_strategy_rule_failure_is_a_denial_not_an_internal_fault() {
        let owner = owner();
        // UPDATE needs v3 + row lineage; a v2 table fails the rule.
        let handle = PayloadSpec::new(&metadata(FormatVersion::V2, &[])).handle();
        let error = denial(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Update),
                &owner,
            )
            .expect("outcome"),
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "UPDATE requires an Iceberg v3 table with write.row-lineage=true",
            )
        );
    }

    #[test]
    fn an_empty_identity_layout_is_denied() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.metadata_columns = Vec::new();
        let error = denial(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            )
            .expect("outcome"),
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg row-mutation target has no admitted identity fields",
            )
        );
    }

    #[test]
    fn an_unknown_identity_column_is_corrupt_data() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.metadata_columns = vec!["_nope".to_string()];
        let error = failure(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "unknown metadata column",
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "unknown Iceberg metadata column `_nope`",
            )
        );
    }

    #[test]
    fn a_missing_table_uuid_is_corrupt_data() {
        let owner = owner();
        let mut spec = PayloadSpec::new(&metadata(FormatVersion::V3, &[]));
        spec.table_uuid = None;
        let error = failure(
            prepare_row_mutation(
                request(spec.handle(), "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "missing table uuid",
        );
        assert_eq!(
            error,
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "admitted Iceberg row-mutation table is missing table UUID",
            )
        );
    }

    #[test]
    fn a_foreign_table_handle_is_rejected_before_any_provider_work() {
        let owner = owner();
        let handle = ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("other").expect("instance id"),
            Bytes::from_static(b"{}"),
        )
        .expect("handle");
        let error = failure(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Delete),
                &owner,
            ),
            "foreign handle",
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    // The two `ResourceExhausted` ordinal-overflow branches
    // (`u32::try_from(..)` on the identity/target widths, and the
    // `checked_mul`/`checked_add` on the effect ordinal) are unreachable from
    // any constructible fixture: they need more than `u32::MAX` Arrow fields.
    // They stay as fail-closed guards; there is no test that can build one.

    // ---------------------------------------------------------------------
    // Frozen hash domains
    // ---------------------------------------------------------------------

    /// The signed field tokens are the only cross-layer binding between the
    /// provider's match layout and everything downstream. These goldens pin
    /// the exact hashed byte sequence — domain prefix, instance id,
    /// incarnation, table payload, length-prefixed role, ordinal — so that any
    /// change to a domain fails here instead of silently invalidating every
    /// preparation that was already issued.
    #[test]
    fn field_token_hash_domains_are_frozen() {
        let owner = owner();
        let table = ConnectorTableHandle::try_new(
            instance_id(),
            Bytes::from_static(b"golden-row-mutation-table"),
        )
        .expect("table handle");

        assert_eq!(
            hex(row_mutation_identity_token(&owner, &table, 0).to_bytes()),
            "20b5be22ceba2b83696e39001179fa90c183f70749cd0e6bbb0583bba00da214"
        );
        assert_eq!(
            hex(row_mutation_identity_token(&owner, &table, 1).to_bytes()),
            "50feb06c9b9bff74a396cfdc6c83a66a6a2c71b67b49ef8ea27754ef2f0e6dd8"
        );
        assert_eq!(
            hex(row_mutation_field_token(&owner, &table, b"before", 0).to_bytes()),
            "c79a7064b161d907a408b6f84328b5d37d2582d1515b80ff5a46e92ae4f350a1"
        );
        assert_eq!(
            hex(row_mutation_field_token(&owner, &table, b"before", 3).to_bytes()),
            "2ca58382e74802cbc6a201c1650be08a1ebf7a9eba34280ad1c0a131fd935204"
        );
        assert_eq!(
            hex(row_mutation_field_token(&owner, &table, b"after", 0).to_bytes()),
            "bcb06c5d3e0eeb68c8768a74666ad1d16cba67bf3b94b7963b565f6ec4ea1420"
        );
        assert_eq!(
            hex(row_mutation_effect_token(&owner, &table).to_bytes()),
            "3e59070d1634b0fbd9105c94b059c68885b3287016897119daee56e5273292a8"
        );
    }

    /// The identity and target domains mix in the incarnation, so a new
    /// generation of the same instance can never reuse a token. The effect
    /// domain deliberately does not.
    #[test]
    fn token_domains_bind_the_incarnation_except_for_the_effect_field() {
        let first = owner();
        let second = ConnectorExecutionBindingKey {
            instance_id: instance_id(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([8; 16]),
        };
        let table = ConnectorTableHandle::try_new(
            instance_id(),
            Bytes::from_static(b"golden-row-mutation-table"),
        )
        .expect("table handle");

        assert_ne!(
            row_mutation_identity_token(&first, &table, 0).to_bytes(),
            row_mutation_identity_token(&second, &table, 0).to_bytes()
        );
        assert_ne!(
            row_mutation_field_token(&first, &table, b"before", 0).to_bytes(),
            row_mutation_field_token(&second, &table, b"before", 0).to_bytes()
        );
        assert_eq!(
            row_mutation_effect_token(&first, &table).to_bytes(),
            row_mutation_effect_token(&second, &table).to_bytes()
        );
        // The length prefix keeps role/ordinal pairs from colliding.
        assert_ne!(
            row_mutation_field_token(&first, &table, b"before", 0).to_bytes(),
            row_mutation_field_token(&first, &table, b"after", 0).to_bytes()
        );
    }

    /// The strings the application persists are part of the contract, so they
    /// are pinned separately from the tokens.
    #[test]
    fn base_version_and_preparation_payload_strings_are_frozen() {
        let owner = owner();
        let handle = PayloadSpec::new(&metadata(FormatVersion::V3, &[ROW_LINEAGE_ON])).handle();
        let preparation = prepared(
            prepare_row_mutation(
                request(handle, "main", ConnectorRowMutationIntent::Update),
                &owner,
            )
            .expect("prepare"),
        );
        let expected_base =
            "iceberg/row-mutation-base/v1/11111111-2222-3333-4444-555555555555/main/none";
        assert_eq!(
            preparation.base_version(),
            &ConnectorWriteBaseVersion::try_new(Bytes::from(expected_base)).expect("base version")
        );
        assert_eq!(
            preparation.payload().as_ref(),
            b"iceberg/row-mutation-preparation/v1/ice/11111111-2222-3333-4444-555555555555/main/none/CopyOnWrite"
        );
    }
}
