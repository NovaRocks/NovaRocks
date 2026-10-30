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

//! Actual COW preparation retaining the caller's original operation.

use super::cow_begin_own as own;
use super::*;
use crate::metadata::existing_serde_projection::{self as projection, OriginalScope};
pub(crate) use novarocks_spi::connector::ConnectorCowBeginCause as Cause;
use novarocks_spi::connector::{
    ConnectorCowBeginFailure, ConnectorCowBeginPlan, ConnectorOriginalResultScope,
    ConnectorPayloadRetentionGuard, OriginalResultCheckError,
};

pub(crate) fn overflow() -> Cause {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW construction arithmetic overflowed",
    )
    .into()
}
pub(crate) fn projection_error(e: projection::Error<OriginalResultCheckError>) -> Cause {
    match e {
        projection::Error::Control(e) => e.into(),
        projection::Error::Json => ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW canonical projection failed",
        )
        .into(),
        projection::Error::Overflow => overflow(),
        projection::Error::ShapeChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg COW canonical projection changed during serialization",
        )
        .into(),
    }
}
pub(super) fn own_error(e: own::Failure<OriginalResultCheckError>) -> Cause {
    match e {
        own::Failure::Original(e) => e.into(),
        own::Failure::Provider(e) => e.into(),
        own::Failure::Projection(e) => projection_error(e),
        own::Failure::Overflow => overflow(),
        own::Failure::ShapeChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg COW constructor changed its exact output shape",
        )
        .into(),
    }
}
/// Borrows the original operation's cancellation, deadline and retention.
pub(crate) struct CowBeginScope<'a> {
    original: &'a ConnectorOriginalResultScope,
}
impl<'a> CowBeginScope<'a> {
    pub(crate) fn new(original: &'a ConnectorOriginalResultScope) -> Result<Self, Cause> {
        original.check_active()?;
        Ok(Self { original })
    }
    pub(crate) fn original(&self) -> &ConnectorOriginalResultScope {
        self.original
    }
    pub(crate) fn active(&self) -> Result<(), Cause> {
        self.original.check_active().map_err(Into::into)
    }
}
impl OriginalScope<OriginalResultCheckError> for CowBeginScope<'_> {
    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        self.original.check_active()
    }
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.original.retention_guard()
    }
}
impl IcebergWriteSessionControl {
    // Transitional old-family bridge; the asynchronous checked owner below
    // remains the only COW admission algorithm.
    pub(super) fn begin_cow_original(
        &self,
        request: ConnectorWriteBeginRequest,
        original: ConnectorOriginalResultScope,
    ) -> Result<ConnectorCowBeginPlan, ConnectorCowBeginFailure> {
        let retained = original.clone();
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(self.begin_cow_original_async(request, original))
            .map_err(|cause| ConnectorCowBeginFailure::new(unavailable(cause).into(), retained))?
    }

    pub(super) async fn begin_cow_original_async(
        &self,
        request: ConnectorWriteBeginRequest,
        original: ConnectorOriginalResultScope,
    ) -> Result<ConnectorCowBeginPlan, ConnectorCowBeginFailure> {
        let outcome: Result<ConnectorWriteSessionPlan, Cause> = async {
            validate_context(&request.context)?;
            let ConnectorWriteSessionFlavor::CopyOnWrite {
                selection,
                match_contract,
            } = &request.flavor
            else {
                return Err(invalid("checked COW begin requires a copy-on-write request").into());
            };
            if original.original_deadline() != request.context.deadline() {
                return Err(
                    invalid("checked COW begin changed the original absolute deadline").into(),
                );
            }
            let scope = CowBeginScope::new(&original)?;
            let (namespace, table_name) = request.table.rsplit_once('.').ok_or_else(|| {
                invalid("Iceberg write target must be a namespace-qualified table name")
            })?;
            use crate::catalog::admission::{
                CatalogAdmissionRequest, CatalogOperation, connector_unsupported,
            };
            self.runtime
                .novarocks_catalog()
                .admit(&CatalogAdmissionRequest::new(
                    CatalogOperation::CopyOnWrite,
                    crate::catalog::CatalogTableName::new(namespace, table_name),
                    request.context.initiation(),
                ))
                .map_err(connector_unsupported)?;
            scope.active()?;
            let physical = self
                .runtime
                .load_table_for_request_async(namespace, table_name, &request.context)
                .await
                .map_err(|e| unavailable(e.to_string()))?;
            let cow_read_access =
                crate::loaded_table::IcebergAttemptTableAccess::freeze_with_original_scope(
                    &physical, &scope,
                )?;
            let table = physical.into_table();
            let read_metadata = table.metadata_ref();
            let metadata = IcebergAdmissionStatisticsMetadata::from_loaded_table(&table, true);
            scope.active()?;
            crate::commit::validation::ensure_iceberg_write_supported_from_metadata(&metadata)
                .map_err(|e| ConnectorError::new(ConnectorErrorKind::Unsupported, e))?;
            let snapshot_id = crate::ref_snapshot::resolve_branch_head_snapshot_id(
                &metadata,
                request.target_ref.as_str(),
            )
            .map_err(|e| invalid(e.to_string()))?;
            if let Some(base) = &request.base {
                base.validate()?;
            }
            let loaded = own::loaded_facts(own::LoadedFactsInput {
                metadata: &metadata,
                namespace,
                table: table_name,
                target_ref: request.target_ref.as_str(),
                base_snapshot_id: snapshot_id,
                base_sequence_number: source_snapshot_sequence(&metadata, snapshot_id)?,
                schema_id: metadata.current_schema_id(),
                spec_id: metadata.default_partition_spec_id(),
                format_version: format_version_number(&metadata),
                scope: &scope,
            })
            .map_err(own_error)?;
            let facts = loaded.into_facts();
            let signed = own::signed_input(&facts, &request.input, &scope)
                .map_err(own_error)?;
            let input = signed.into_shape();
            let output = IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                crate::commit::data_writer::parquet_row_group_size_bytes(metadata.properties())
                    .map_err(invalid)?
                    .map(|n| n as u64),
            )?;
            let recipe = own::data_recipe(
                &metadata,
                matches!(input, ConnectorWriteInputShape::RowLineage { .. }),
                &scope,
            )
            .map_err(own_error)?;
            let material = IcebergSessionMaterial {
                table: facts,
                input,
                data_output: output,
                data_recipe: recipe,
                merge_targets: Vec::new(),
                equality: None,
            };
            let snapshot_id = snapshot_id.ok_or_else(|| {
                invalid("Iceberg copy-on-write mutation requires a frozen base snapshot")
            })?;
            let base_digest = request
                .base
                .as_ref()
                .map(ConnectorWriteBaseVersion::digest)
                .ok_or_else(|| {
                    invalid("Iceberg copy-on-write mutation requires its signed base version")
                })?;
            let original_read = scope.original().clone();
            // Move the loaded Table into the existing runtime bridge: no
            // explicit Table/metadata clone or refreshed activity/deadline.
            let files = crate::manifest::extract_cow_data_files_with_stats_at_with_original_scope(
                &table, snapshot_id, &original_read,
            ).await?;
            drop(table);
            // Same original ReadSnapshot facts and loaded-table authority, moved
            // into typed base holders without another observation or SDK clone.
            let base_files = files
                .files
                .into_iter()
                .map(|file| {
                    crate::commit::write_stack::copy_on_write::IcebergCowFrozenBaseFile::new(
                        file,
                        read_metadata.clone(),
                        cow_read_access.clone(),
                    )
                })
                .collect();
            let recipes=crate::commit::write_stack::copy_on_write::freeze_copy_on_write_branches_with_original_scope(
                selection,match_contract,crate::commit::write_stack::copy_on_write::IcebergCowFreezeInput{
                    owner:&self.key,catalog:&self.key.instance_id,namespace,table_name,metadata:&metadata,
                    snapshot_id,base_files,input:&material.input,base_version_digest:base_digest,
                    max_handle_payload_bytes:request.context.max_handle_payload_bytes()},&scope)?;
            let plan = plan_copy_on_write_branches(&material, &recipes, match_contract)?;
            let IcebergSessionFlavorPlan {
                flavor,
                document_publication,
                rewrite_inputs,
                copy_on_write,
                branches,
            } = plan;
            let (handle, targets) = plan_branch_session(
                IcebergWriteSessionId::new(),
                IcebergBranchSessionPlanInput {
                    flavor,
                    purpose: request.purpose,
                    table: material.table,
                    base_version_digest: Some(base_digest),
                    document_publication,
                    staged_metadata: None,
                    rewrite_inputs,
                    copy_on_write,
                    repartition: None,
                    writer_table: None,
                    branches,
                },
            )?;
            scope.active()?;
            // Admission has no frozen attempt or owned-object ledger to encode.
            // IRU-5 constructs and size-checks the complete recovery envelope
            // immediately before dispatch, once those actual facts exist.
            let handle = handle.with_source_metadata(Arc::clone(&read_metadata))?;
            let plan = session_plan_from_targets(
                &self.adapter,
                handle,
                targets,
                Some(&metadata),
                Some((selection.clone(), match_contract.clone())),
            )?;
            scope.active()?;
            Ok(plan)
        }.await;
        match outcome {
            Ok(plan) => Ok(ConnectorCowBeginPlan::new(plan, original)),
            Err(cause) => Err(ConnectorCowBeginFailure::new(cause, original)),
        }
    }
}
