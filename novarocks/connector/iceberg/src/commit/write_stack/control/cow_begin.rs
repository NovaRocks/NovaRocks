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

//! Actual COW admission under the caller's original result authority.

use super::cow_begin_own as own;
use super::*;
use crate::metadata::existing_serde_projection::{self as projection, OriginalScope};
pub(crate) use novarocks_spi::connector::ConnectorCowBeginCause as Cause;
use novarocks_spi::connector::{
    ConnectorCowBeginFailure, ConnectorCowBeginPlan, ConnectorOriginalResultScope,
    ConnectorPayloadRetentionGuard, ConnectorRowConversionFootprint, OriginalResultCheckError,
};
use std::{cell::Cell, mem::size_of};

pub(crate) fn overflow() -> Cause {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW construction coexistence arithmetic overflowed",
    )
    .into()
}
pub(crate) fn add(a: u64, b: u64) -> Result<u64, Cause> {
    a.checked_add(b).ok_or_else(overflow)
}
pub(crate) fn mul(a: u64, b: u64) -> Result<u64, Cause> {
    a.checked_mul(b).ok_or_else(overflow)
}
pub(crate) fn projection_error(e: projection::Error<OriginalResultCheckError>) -> Cause {
    match e {
        projection::Error::Control(e) => e.into(),
        projection::Error::Json(_) => ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg COW canonical projection failed",
        )
        .into(),
        projection::Error::Overflow => overflow(),
        projection::Error::ShapeChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg COW canonical projection changed after authorization",
        )
        .into(),
    }
}
fn own_error(e: own::Failure<OriginalResultCheckError>) -> Cause {
    match e {
        own::Failure::Original(e) => e.into(),
        own::Failure::Provider(e) => e.into(),
        own::Failure::Projection(e) => projection_error(e),
        own::Failure::Overflow => overflow(),
        own::Failure::CapacityChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg COW constructor changed capacity after authorization",
        )
        .into(),
    }
}
/// A monotonically conservative construction envelope, never another wallet.
/// Retired scratch may remain in this bound; no authority is released early.
pub(crate) struct CowBeginScope<'a> {
    original: &'a ConnectorOriginalResultScope,
    upper: Cell<u64>,
}
impl<'a> CowBeginScope<'a> {
    pub(crate) fn new(
        original: &'a ConnectorOriginalResultScope,
        upper: u64,
    ) -> Result<Self, Cause> {
        original.check_before_growth(upper)?;
        Ok(Self {
            original,
            upper: Cell::new(upper),
        })
    }
    pub(crate) fn upper(&self) -> u64 {
        self.upper.get()
    }
    pub(crate) fn original(&self) -> &ConnectorOriginalResultScope {
        self.original
    }
    pub(crate) fn active(&self) -> Result<(), Cause> {
        self.original.check_active().map_err(Into::into)
    }
    pub(crate) fn reserve(&self, prospective: u64) -> Result<(), Cause> {
        let total = add(self.upper(), prospective)?;
        self.original.check_before_growth(total)?;
        self.upper.set(total);
        Ok(())
    }
    pub(crate) fn cover_total(&self, total: u64) -> Result<(), Cause> {
        self.original.check_before_growth(total)?;
        self.upper.set(self.upper().max(total));
        Ok(())
    }
}
impl OriginalScope<OriginalResultCheckError> for CowBeginScope<'_> {
    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        self.original.check_active()
    }
    fn check_total(&self, total: u64) -> Result<(), OriginalResultCheckError> {
        self.original.check_before_growth(total)
    }
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.original.retention_guard()
    }
}
fn table_heap(facts: &IcebergWriteTableFacts) -> Result<u64, Cause> {
    [
        facts.table_uuid(),
        facts.namespace(),
        facts.table_name(),
        facts.table_location(),
        facts.data_location(),
        facts.target_ref(),
    ]
    .into_iter()
    .try_fold(size_of::<IcebergWriteTableFacts>() as u64, |n, s| {
        add(n, s.len() as u64)
    })
}
fn input_slices(
    input: &ConnectorWriteInputShape,
) -> (&[ConnectorWriteFieldBinding], &[ConnectorWriteFieldBinding]) {
    match input {
        ConnectorWriteInputShape::Data { fields } => (fields, &[]),
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => (data_fields, row_identity_fields),
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => (identity_fields, partition_source_fields),
        ConnectorWriteInputShape::EqualityDelete { equality_fields } => (equality_fields, &[]),
    }
}
fn input_heap(input: &ConnectorWriteInputShape) -> Result<u64, Cause> {
    let (a, b) = input_slices(input);
    add(
        mul(
            (a.len() + b.len()) as u64,
            size_of::<ConnectorWriteFieldBinding>() as u64,
        )?,
        add(
            ConnectorRowConversionFootprint::for_fields(a.iter().map(|f| f.field()))?.schema_bytes
                as u64,
            ConnectorRowConversionFootprint::for_fields(b.iter().map(|f| f.field()))?.schema_bytes
                as u64,
        )?,
    )
}
pub(crate) fn tree_upper<K, V>(n: u64) -> Result<u64, Cause> {
    if n == 0 {
        return Ok(0);
    }
    let align = std::mem::align_of::<K>()
        .max(std::mem::align_of::<V>())
        .max(std::mem::align_of::<usize>());
    let node = (size_of::<usize>()
        + 4
        + 11 * (size_of::<K>() + size_of::<V>())
        + 12 * size_of::<usize>()
        + 9 * (align - 1)) as u64;
    mul(add(mul(n, 2)?, 1)?, node)
}
pub(crate) fn geometry_error(
    e: crate::read_snapshot::cow_capture::geometry::Failure<Cause>,
) -> Cause {
    match e {
        crate::read_snapshot::cow_capture::geometry::Failure::Original(e) => e,
        crate::read_snapshot::cow_capture::geometry::Failure::Overflow => overflow(),
        crate::read_snapshot::cow_capture::geometry::Failure::ReceiptExceeded => {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "COW constructor layout changed",
            )
            .into()
        }
    }
}
fn validation_peak(input: &ConnectorWriteInputShape) -> Result<u64, Cause> {
    let (a, b) = input_slices(input);
    let n = (a.len() + b.len()) as u64;
    let names = a
        .iter()
        .chain(b)
        .try_fold(0u64, |n, f| add(n, f.field().name().len() as u64))?;
    use crate::read_snapshot::cow_capture::geometry;
    let token_table = geometry::hash_max::<Cause, ConnectorWriteFieldToken>(n as usize)
        .map_err(geometry_error)?;
    let name_table = geometry::hash_max::<Cause, String>(n as usize).map_err(geometry_error)?;
    add(
        names,
        add(
            mul(n, size_of::<usize>() as u64)?,
            mul(add(token_table, name_table)?, 2)?,
        )?,
    )
}
fn contract_clone_upper(
    c: &novarocks_spi::connector::ConnectorMutationMatchContract,
) -> Result<u64, Cause> {
    use novarocks_spi::connector::{ConnectorMutationSourceField, ConnectorMutationTargetField};
    let entries = add(
        mul(
            c.identity_fields().len() as u64,
            size_of::<ConnectorMutationSourceField>() as u64,
        )?,
        mul(
            (c.before_fields().len() + c.after_fields().len()) as u64,
            size_of::<ConnectorMutationTargetField>() as u64,
        )?,
    )?;
    let field_bytes = [
        ConnectorRowConversionFootprint::for_fields(c.identity_fields().iter().map(|f| f.field()))?
            .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(c.before_fields().iter().map(|f| f.field()))?
            .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(c.after_fields().iter().map(|f| f.field()))?
            .schema_bytes,
        ConnectorRowConversionFootprint::for_fields(std::iter::once(c.effect_field().field()))?
            .schema_bytes,
    ]
    .into_iter()
    .try_fold(0u64, |n, bytes| add(n, bytes as u64))?;
    add(
        entries,
        add(
            field_bytes,
            mul(
                c.uniqueness_tokens().len() as u64,
                size_of::<ConnectorWriteFieldToken>() as u64,
            )?,
        )?,
    )
}

fn recipe_heap(recipe: &IcebergDataBranchRecipe) -> Result<u64, Cause> {
    // Shared schema backing is already retained by the one checked constructor.
    [
        recipe.partition_source_column_names(),
        recipe.partition_column_names(),
        recipe.transform_exprs(),
    ]
    .into_iter()
    .try_fold(size_of::<IcebergDataBranchRecipe>() as u64, |n, fields| {
        fields.iter().try_fold(
            add(n, mul(fields.len() as u64, size_of::<String>() as u64)?)?,
            |n, s| add(n, s.len() as u64),
        )
    })
}

impl IcebergWriteSessionControl {
    pub(super) fn begin_cow_original(
        &self,
        request: ConnectorWriteBeginRequest,
        original: ConnectorOriginalResultScope,
        existing_caller_upper: u64,
    ) -> Result<ConnectorCowBeginPlan, ConnectorCowBeginFailure> {
        let outcome = (|| -> Result<(ConnectorWriteSessionPlan, u64), Cause> {
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
            let scope = CowBeginScope::new(&original, existing_caller_upper)?;
            let (namespace, table_name) = request.table.rsplit_once('.').ok_or_else(|| {
                invalid("Iceberg write target must be a namespace-qualified table name")
            })?;
            scope.reserve(
                own::entry_upper::<OriginalResultCheckError>(namespace, table_name)
                    .map_err(own_error)?,
            )?;
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
                .load_table_for_request(namespace, table_name, &request.context)
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
            // URL parsing/query decoding is third-party internals; Nova's unchanged
            // validator diagnostics have three fixed subjects and suffixes.
            let validator = own::LocationValidationUpper {
                maximum_whole_validator_temporary: [
                    "table location",
                    "data location",
                    "partition transform",
                ]
                .into_iter()
                // Pinned format! can start with twice its literal size;
                // twice the complete text also covers old+new output growth.
                .map(|s| 2 * ("Iceberg ".len() + s.len() + " must not embed credentials".len()))
                .max()
                .unwrap() as u64,
            };
            let loaded = own::loaded_facts(
                &metadata,
                namespace,
                table_name,
                request.target_ref.as_str(),
                snapshot_id,
                source_snapshot_sequence(&metadata, snapshot_id)?,
                metadata.current_schema_id(),
                metadata.default_partition_spec_id(),
                format_version_number(&metadata),
                scope.upper(),
                &validator,
                &scope,
            )
            .map_err(own_error)?;
            scope.reserve(loaded.retained_upper())?;
            let facts = loaded.into_facts();
            let signed = own::signed_input(&facts, &request.input, scope.upper(), &scope)
                .map_err(own_error)?;
            scope.reserve(signed.retained_upper())?;
            let input = signed.into_shape();
            let output = IcebergWriterOutput::try_new(
                crate::delete_file::IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                crate::commit::data_writer::parquet_row_group_size_bytes(metadata.properties())
                    .map_err(invalid)?
                    .map(|n| n as u64),
            )?;
            let schema_plan =
                projection::SchemaOnlyPlan::inspect(metadata.current_schema(), &scope)
                    .map_err(projection_error)?;
            let schema_upper = schema_plan.retained_upper().map_err(projection_error)?;
            let recipe = own::data_recipe(
                &metadata,
                matches!(input, ConnectorWriteInputShape::RowLineage { .. }),
                scope.upper(),
                &validator,
                &scope,
            )
            .map_err(own_error)?;
            scope.reserve(add(schema_upper, recipe_heap(&recipe)?)?)?;
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
            let read_upper = scope.upper();
            // Move the loaded Table into the existing runtime bridge: no
            // explicit Table/metadata clone or refreshed activity/deadline.
            let files = self
                .runtime
                .resources()
                .catalog_runtime()
                .block_on(async move {
                    crate::manifest::extract_cow_data_files_with_stats_at_with_original_scope(
                        &table,
                        snapshot_id,
                        &original_read,
                        read_upper,
                    )
                    .await
                })
                .map_err(unavailable)??;
            scope.cover_total(files.simultaneous_upper)?;
            // Same original ReadSnapshot facts and loaded-table authority, moved
            // into typed base holders without another observation or SDK clone.
            scope.reserve(mul(
                files.files.len() as u64,
                size_of::<crate::commit::write_stack::copy_on_write::IcebergCowFrozenBaseFile>()
                    as u64,
            )?)?;
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
            // All simultaneous flavor/target clones are authorized before either
            // planner starts. Clone capacities use initialized lengths, shared
            // Arrow/schema pointees keep the original holder.
            let branch_bytes = add(
                add(
                    input_heap(&material.input)?,
                    recipe_heap(&material.data_recipe)?,
                )?,
                table_heap(&material.table)?,
            )?;
            let per_branch = add(
                branch_bytes,
                add(
                    size_of::<crate::commit::write_stack::planning::IcebergWriteTargetPlan>()
                        as u64,
                    size_of::<crate::commit::write_stack::planning::IcebergWriteBranchPlan>()
                        as u64,
                )?,
            )?;
            scope.reserve(mul(recipes.len() as u64, mul(per_branch, 3)?)?)?;
            let width = {
                let (a, b) = input_slices(&material.input);
                (a.len() + b.len()) as u64
            };
            let route_owned=add(mul(width,(size_of::<novarocks_spi::connector::ConnectorMutationRouteInput>()+
                size_of::<novarocks_spi::connector::write_stack::session::ConnectorWriteSelectionBinding>()+
                size_of::<ConnectorWriteFieldToken>()+2*size_of::<usize>()) as u64)?,
                (2*size_of::<novarocks_spi::connector::ConnectorRowMutationEffect>()) as u64)?;
            let route_maps = mul(
                tree_upper::<
                    ConnectorWriteFieldToken,
                    novarocks_spi::connector::write_stack::session::ConnectorWriteSelectionBinding,
                >(width)?,
                2,
            )?;
            let route_copies = add(mul(recipes.len() as u64, 3)?, 2)?;
            let route_peak = add(
                route_maps,
                add(
                    mul(route_owned, route_copies)?,
                    validation_peak(&material.input)?,
                )?,
            )?;
            scope.reserve(route_peak)?;
            let plan = plan_copy_on_write_branches(&material, &recipes, match_contract)?;
            let IcebergSessionFlavorPlan {
                flavor,
                publication,
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
                    publication,
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
            let target_headers = mul(
                targets.len() as u64,
                (size_of::<ConnectorWriteTargetPlan>()
                    + size_of::<novarocks_spi::connector::write_stack::WriteTargetOrdinal>())
                    as u64,
            )?;
            let route_seen = tree_upper::<novarocks_spi::connector::ConnectorWriteRouteId, ()>(
                targets.len() as u64,
            )?;
            let selection_clone = add(
                selection.cloned_container_bytes()? as u64,
                add(
                    contract_clone_upper(match_contract)?,
                    add(selection.row_count(), add(route_seen, target_headers)?)?,
                )?,
            )?;
            scope.reserve(selection_clone)?;
            // COW statistics perform declaration/schema validation even though
            // CowUpdate does not collect artifacts. Cover those own temporary
            // Arrow/domain passes before entering the unchanged validator.
            scope.reserve(crate::metadata::cow_projected_schema_upper(
                &metadata,
                snapshot_id,
                &scope,
            )?)?;
            let plan = session_plan_from_targets(
                &self.adapter,
                handle,
                targets,
                Some(&metadata),
                Some((selection.clone(), match_contract.clone())),
            )?;
            scope.active()?;
            Ok((plan, scope.upper()))
        })();
        match outcome {
            Ok((plan, upper)) => Ok(ConnectorCowBeginPlan::new(plan, original, upper)),
            Err(cause) => Err(ConnectorCowBeginFailure::new(cause, original)),
        }
    }
}
