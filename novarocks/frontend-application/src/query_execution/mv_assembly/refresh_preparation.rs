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

//! Frontend-owned MV refresh-preparation coordinator.

//! Projection/filter materialized views backed by Iceberg target tables in the
//! current Iceberg catalog. Aggregate shapes are accepted at CREATE time for
//! target schema and contract persistence; refresh execution is gated later.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog_application::query_catalog::CatalogServiceSource;
use crate::mv::domain::analysis::refresh_property::derive_fragment_property;
use crate::mv::domain::analysis::{
    canonicalize_iceberg_mv_select_query, validate_mv_partition_columns,
};
use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::iceberg_refresh::{
    IcebergMvCorePorts, join_base_refs_for_definition,
    plan_iceberg_mv_refresh_with_connector_context,
};
use crate::mv::domain::lifecycle::RefreshError;
use crate::mv::domain::refresh::capabilities::RefreshCapabilities;
use crate::mv::domain::refresh::definition::{
    load_iceberg_mv_definition_by_target, mv_definition_fingerprint, parse_mv_select_query,
};
use crate::mv::domain::refresh::execution_policy::{
    non_join_incremental_write_mode, select_join_incremental_execution_mode,
};
use crate::mv::domain::refresh::non_join_incremental::{
    NonJoinBaseChange, NonJoinIncrementalChangePlan, plan_non_join_incremental_changes,
};
use crate::mv::domain::refresh::observation::{
    observe_schema_validation_for_table, rebind_mv_definition_before_refresh_derivation,
};
use crate::mv::domain::refresh::pin::{RefreshSnapshotPin, RefreshSnapshotPinOccurrence};
use crate::mv::domain::refresh::planning::{
    RefreshPlanContract, RefreshStateBaseline, RefreshStateBaselineSource,
};
use crate::mv::domain::refresh::repartition::select_repartition_shape;
use crate::mv::domain::refresh::rewrite_context::{
    RefreshRewriteInputs, admitted_change_facts, freeze_refresh_rewrite_context,
    observe_and_admit_change_window_for_table,
};
use crate::mv::domain::refresh::schema_contract::{
    validate_projection_target, validate_repartition_schema_contract,
};
use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
use crate::mv::domain::refresh::target::{IcebergMvTarget, load_iceberg_mv_target_binding};
use crate::mv::domain::staged_create::AdmittedMvDataPublication;
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvFirstRefreshWritePreparer, MvFirstRefreshWriteRequest, MvIncrementalExecutionArtifact,
    MvIncrementalWritePreparer, MvIncrementalWriteRequest, PreparedMvFirstRefreshWrite,
    PreparedMvIncrementalWrite, PreparedMvMetadataOnlyWrite,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshPreparationRequest, MvRefreshPreparationService, PreparedMvRefresh,
    PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use novarocks_mv_application::persistence::exact_revision::persist_exact_connector_revision;
use novarocks_mv_application::persistence::projection::StoredMvProjection;
use novarocks_mv_application::product::MvRefreshAttemptIdentity;
use novarocks_mv_application::product::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};
use novarocks_mv_application::publication::{
    MvRefreshPublicationBase, MvRefreshPublicationIntent, MvRefreshPublicationTechnique,
};
use novarocks_spi::connector::{
    ConnectorCommittedPartitioning, ConnectorInstanceId,
    ConnectorManagedPartitionSpecPreviewRequest, ConnectorProviderBindingKey,
    ConnectorTableIdentity, ConnectorTableObjectId,
};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
use novarocks_sql::planning::mv::MvRefreshFinalizeFacts;
use novarocks_sql::planning::mv::{SqlMvAggregateLayoutScope, extract_aggregate_sql_calls};
use novarocks_sql::semantic::IcebergPartitionFieldExpr;
/// FE-role adapter that prepares one MV refresh with the caller's already
/// admitted native query capability.
pub struct FrontendMvRefreshPreparationService<'a> {
    source: &'a IcebergMvCorePorts,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    statement: &'a MvRefreshRequest,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    repartition_fields: Option<&'a [IcebergPartitionFieldExpr]>,
}

impl<'a> FrontendMvRefreshPreparationService<'a> {
    pub fn new_with_ports(
        ports: &'a IcebergMvCorePorts,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a MvRefreshRequest,
        connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            source: ports,
            current_catalog,
            current_database,
            statement,
            connector_context,
            repartition_fields: None,
        }
    }

    pub fn new_repartition_with_ports(
        ports: &'a IcebergMvCorePorts,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a MvRefreshRequest,
        repartition_fields: &'a [IcebergPartitionFieldExpr],
        connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            source: ports,
            current_catalog,
            current_database,
            statement,
            connector_context,
            repartition_fields: Some(repartition_fields),
        }
    }
}

/// Freeze the one rewrite context an EXPLAIN or foreground attempt reasons
/// over, from canonical D/L and one exact target observation.
pub(crate) fn freeze_statement_refresh_rewrite_context(
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    name_parts: &[String],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    (
        Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>,
        novarocks_spi::connector::ConnectorControlPlanningLease,
    ),
    String,
> {
    let target = crate::mv::domain::refresh::target::resolve_refresh_target(
        current_catalog,
        current_database,
        name_parts,
    )?;
    let projection = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let target_binding = load_iceberg_mv_target_binding(
        source.connector_control(),
        source.storage_observation(),
        &target,
        connector_context,
    )?;
    let target_schema_validation =
        crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            target_binding.lease(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe exact MV target schema for {}.{}.{}: {error}",
                target.catalog, target.namespace, target.table
            )
        })?;
    let runtime_bindings = validate_projection_target(&projection, &target_schema_validation)?;
    let pin = crate::mv::domain::refresh_pin_adapter::capture_refresh_snapshot_pin_with_ports(
        source.connector_control(),
        source.storage_observation(),
        &projection,
        connector_context,
    )?;
    let state_baseline = crate::mv::domain::iceberg_refresh::build_refresh_state_baseline(
        &projection,
        &target_binding,
    )?;
    let query = canonical_mv_select_query(&projection)?;
    let aggregate =
        frozen_refresh_aggregate_analysis(source, &projection, &query, connector_context)?;
    let lease = target_binding.lease().clone();
    let rewrite = freeze_refresh_rewrite_context(RefreshRewriteInputs {
        connector_control: source.connector_control(),
        connector_context: &connector_context,
        projection: Arc::new(projection),
        pin: &pin,
        state_baseline: &state_baseline,
        target_binding: &target_binding,
        target_observation: &target_schema_validation,
        runtime_bindings: &runtime_bindings,
        join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(&query)?,
        aggregate,
    })?;
    Ok((rewrite, lease))
}

/// SQL aggregate calls and physical layout for an aggregate definition.
/// A branch UNION's representative layout is its first branch.
pub(crate) fn frozen_refresh_aggregate_analysis(
    source: &IcebergMvCorePorts,
    projection: &StoredMvProjection,
    query: &novarocks_parser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    Option<(
        novarocks_sql::planning::mv::SqlMvAggregateCalls,
        novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
    )>,
    String,
> {
    if projection.facts.interpretation().aggregates.is_empty() {
        return Ok(None);
    }
    let resolution = &projection.facts.definition().query.resolution;
    let representative = if projection.facts.interpretation().branches.is_empty() {
        query.clone()
    } else {
        crate::mv::domain::rewrite::context::first_union_branch_query(query)?
    };
    let calls = extract_aggregate_sql_calls(&representative)?;
    let layout = build_aggregate_layout_for_refresh_select_sql(
        source,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
        &novarocks_parser::printer::print_query(&representative),
        connector_context,
    )?;
    Ok(Some((calls, layout)))
}

fn build_aggregate_layout_for_refresh_select_sql(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    select_sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout, String> {
    let visible_query = parse_mv_select_query(select_sql)?;
    let provider = crate::catalog_application::query_materializer::build_catalog_service_provider(
        current_catalog,
        ports.catalog_service().as_ref(),
        ports.connector_control(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        ports.catalog_application(),
    );
    let visible_analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        &provider,
        current_database,
        &visible_query,
        ports.function_catalog().as_ref(),
    )?;
    let facts = visible_analysis
        .refresh_input
        .aggregate_layout_facts(&visible_query, SqlMvAggregateLayoutScope::WholeQuery)?;
    novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
}

impl MvRefreshPreparationService for FrontendMvRefreshPreparationService<'_> {
    fn prepare_step(
        &self,
        request: MvRefreshPreparationRequest,
    ) -> Result<PreparedMvRefresh, RefreshError> {
        request.validate().map_err(RefreshError::user)?;
        if request.statement != self.statement.sql_refresh_statement() {
            return Err(RefreshError::user(
                "MV refresh preparation statement does not match the admitted SQL request",
            ));
        }
        let mut plan = plan_iceberg_mv_refresh_with_connector_context(
            self.source,
            self.current_catalog,
            self.current_database,
            self.statement,
            request.target.clone(),
            self.connector_context,
        )?;
        let retained_repartition_target = self
            .repartition_fields
            .map(|_| {
                retain_exact_repartition_target(self.source, &plan.contract, self.connector_context)
            })
            .transpose()?;
        let repartition_transition = match self.repartition_fields {
            Some(fields) => {
                plan.contract.decision = ExecutableRefreshDecision::FirstRefresh;
                Some(prepare_managed_repartition_transition(
                    self.source,
                    self.current_catalog,
                    self.current_database,
                    fields,
                    &plan.contract,
                    request.attempt.write_operation_id(),
                    retained_repartition_target.as_ref().ok_or_else(|| {
                        "MV repartition preparation lost its retained target binding".to_string()
                    })?,
                    self.connector_context,
                )?)
            }
            None => None,
        };
        let catalog = plan
            .contract
            .target
            .catalog
            .as_deref()
            .ok_or_else(|| "Iceberg MV refresh target has no connector catalog".to_string())?;
        let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
        let observed_binding = match retained_repartition_target.as_ref() {
            Some(retained) => execution_binding_key_for_target(retained.binding.lease()),
            None => self
                .source
                .connector_control()
                .observe_current_binding(&instance_id)
                .map_err(|error| error.to_string())?,
        };
        let base_table_object_ids = plan
            .contract
            .base_refs
            .iter()
            .map(|base| {
                observe_schema_validation_for_table(
                    self.source.connector_control(),
                    self.source.storage_observation(),
                    &base.table,
                    self.connector_context,
                )
                .map(|observed| (base.occurrence_id, observed.table_object_id().clone()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let expected_target_snapshot_id = match &plan.contract.state_baseline {
            RefreshStateBaseline::SnapshotBacked {
                target_snapshot_id, ..
            } => *target_snapshot_id,
            RefreshStateBaseline::Pinless => None,
        };
        let target_table_uuid = match &plan.contract.state_baseline {
            RefreshStateBaseline::SnapshotBacked {
                target_table_uuid, ..
            } => target_table_uuid.clone(),
            RefreshStateBaseline::Pinless => String::new(),
        };
        let full_overwrite = self.statement.full
            && matches!(
                &plan.contract.state_baseline,
                RefreshStateBaseline::SnapshotBacked { .. }
            );
        let work = match plan.contract.decision {
            ExecutableRefreshDecision::SkipEmpty => PreparedMvRefreshWork::NoOp,
            ExecutableRefreshDecision::MetadataOnly => {
                let (write, admitted) = prepare_metadata_only_publication(
                    self.source,
                    &plan.contract,
                    &request.attempt,
                    &observed_binding,
                    self.connector_context,
                    &base_table_object_ids,
                )?;
                PreparedMvRefreshWork::MetadataOnly { write, admitted }
            }
            ExecutableRefreshDecision::FirstRefresh => {
                let (write, admitted) = prepare_frontend_first_refresh_write(
                    self.source,
                    self.current_catalog,
                    self.current_database,
                    &plan.contract,
                    &request.attempt,
                    &base_table_object_ids,
                    observed_binding.clone(),
                    repartition_transition.as_ref(),
                    retained_repartition_target.as_ref(),
                    self.connector_context.clone(),
                )?;
                PreparedMvRefreshWork::DataProducing {
                    write: PreparedMvRefreshWrite::first_refresh(if full_overwrite {
                        write.into_full_overwrite()
                    } else {
                        write
                    }),
                    admitted,
                }
            }
            ExecutableRefreshDecision::Incremental => match prepare_frontend_incremental_write(
                self.source,
                self.current_catalog,
                self.current_database,
                &plan.contract,
                &request.attempt,
                observed_binding.clone(),
                self.connector_context.clone(),
            )? {
                PreparedIncrementalRefreshWork::ChangeStream(incremental, admitted) => {
                    PreparedMvRefreshWork::DataProducing {
                        write: PreparedMvRefreshWrite::incremental(incremental),
                        admitted,
                    }
                }
                PreparedIncrementalRefreshWork::FullRebuild(rebuild, admitted) => {
                    PreparedMvRefreshWork::DataProducing {
                        write: PreparedMvRefreshWrite::first_refresh(rebuild),
                        admitted,
                    }
                }
                PreparedIncrementalRefreshWork::MetadataOnly => {
                    let (write, admitted) = prepare_metadata_only_publication(
                        self.source,
                        &plan.contract,
                        &request.attempt,
                        &observed_binding,
                        self.connector_context,
                        &base_table_object_ids,
                    )?;
                    PreparedMvRefreshWork::MetadataOnly { write, admitted }
                }
            },
        };
        Ok(PreparedMvRefresh {
            statement: request.statement,
            attempt: request.attempt,
            observed_binding,
            finalize: MvRefreshFinalizeFacts {
                mv_id: plan.contract.mv_id.ok_or_else(|| {
                    "Iceberg MV refresh plan has no persisted materialized-view ID".to_string()
                })?,
                target: plan.contract.target,
                base_snapshots: plan.contract.snapshot_pins,
                base_table_object_ids,
                expected_target_snapshot_id,
                target_table_uuid,
            },
            work,
        })
    }
}

/// One repartition target observation retained across transition and write
/// preparation. Both payloads were produced from the same exact lease and
/// metadata value; no downstream repartition step may resolve `latest` again.
struct RetainedRepartitionTarget {
    binding: crate::mv::domain::refresh::target_binding::MvTargetBinding,
    schema_validation: MvSchemaValidationObservation,
}

struct PreparedManagedRepartitionTransition {
    replacement: novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement,
    preview: novarocks_spi::connector::ConnectorManagedPartitionSpecPreview,
}

fn retain_exact_repartition_target(
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RetainedRepartitionTarget, String> {
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV repartition target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let binding = load_iceberg_mv_target_binding(
        source.connector_control(),
        source.storage_observation(),
        &target,
        connector_context,
    )?;
    validate_retained_target_identity(&target, binding.identity())?;
    let schema_validation = crate::mv::domain::storage_observation::observe_schema_validation(
        source.storage_observation(),
        binding.lease(),
        binding.metadata(),
        connector_context.clone(),
    )
    .map_err(|error| {
        format!(
            "observe exact MV repartition target schema for {}.{}.{}: {error}",
            target.catalog, target.namespace, target.table
        )
    })?;
    Ok(RetainedRepartitionTarget {
        binding,
        schema_validation,
    })
}

pub(crate) fn validate_retained_target_identity(
    target: &IcebergMvTarget,
    identity: &ConnectorTableIdentity,
) -> Result<(), String> {
    let expected_instance =
        ConnectorInstanceId::parse(&target.catalog).map_err(|error| error.to_string())?;
    if identity.instance_id != expected_instance
        || identity.namespace.as_ref() != target.namespace
        || identity.table.as_ref() != target.table
    {
        return Err(format!(
            "retained MV repartition target identity does not match {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(())
}

fn execution_binding_key_for_target(
    lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
) -> ConnectorProviderBindingKey {
    ConnectorProviderBindingKey {
        instance_id: lease.binding().descriptor().instance_id.clone(),
        incarnation: lease.binding().incarnation(),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Managed repartition preparation keeps the frozen lease, partition contract, and operation identity explicit."
)]
fn prepare_managed_repartition_transition(
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    fields: &[IcebergPartitionFieldExpr],
    contract: &RefreshPlanContract,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    retained_target: &RetainedRepartitionTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedManagedRepartitionTransition, String> {
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV repartition target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    validate_retained_target_identity(&target, retained_target.binding.identity())?;
    let projection = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let interpretation = projection.facts.interpretation();
    if !interpretation.branches.is_empty() && !interpretation.aggregates.is_empty() {
        return Err(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support branch UNION ALL aggregates"
                .to_string(),
        );
    }
    let query = canonical_mv_select_query(&projection)?;
    let bindings = validate_projection_target(&projection, &retained_target.schema_validation)?;
    select_repartition_shape(&RefreshCapabilities::from_canonical_facts(
        interpretation,
        &bindings,
        projection.facts.definition().relation_occurrences.len(),
        definition_has_join(&query),
    )?)?;
    validate_repartition_schema_contract(
        source.connector_control(),
        source.storage_observation(),
        &projection,
        // Positional, in the contract's own occurrence order: this consumer
        // matches each entry against D's occurrence at the same index.
        &contract_base_tables(contract),
        &retained_target.schema_validation,
        connector_context,
    )?;
    let provider = crate::catalog_application::query_materializer::build_catalog_service_provider(
        current_catalog,
        source.catalog_service().as_ref(),
        source.connector_control(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        source.catalog_application(),
    );
    let analysis = crate::mv::domain::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        &provider,
        current_database,
        &query,
        source.function_catalog().as_ref(),
    )?;
    validate_mv_partition_columns(Some(fields), &analysis.output_columns)?;
    if derive_fragment_property(&analysis)?.is_composed_aggregate_schema_contract_fallback() {
        return Err("partitioned composed aggregate Iceberg MV is not supported".to_string());
    }

    // Both the prior specification and the physical field IDs come from the
    // one retained target observation. Canonical L keeps provider-opaque
    // partition identities and typed transforms; none are decoded here.
    let prior_partition = retained_target.binding.partition();
    let prior_fields = prior_partition
        .fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                field.source_target_field_id,
                u32::try_from(position)
                    .map_err(|_| "MV repartition prior field count exceeds u32".to_string())?,
                managed_partition_transform(&field.transform),
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_prior =
        novarocks_spi::connector::ConnectorManagedPartitionSpecObservation::try_from_fields(
            prior_partition.target_spec_id,
            &prior_fields,
        )
        .map_err(|error| error.to_string())?;
    let target_field_id_by_name = retained_target_field_id_by_name(retained_target)?;
    let replacement_fields = fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let (column, transform) = managed_repartition_field(field);
            let source_field_id = target_field_id_by_name
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(column))
                .map(|(_, field_id)| *field_id)
                .ok_or_else(|| {
                    format!(
                        "MV repartition source column `{column}` is missing from the exact target observation"
                    )
                })?;
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                source_field_id,
                u32::try_from(position)
                    .map_err(|_| "MV repartition field count exceeds u32".to_string())?,
                transform,
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let replacement = novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement::try_new(
        operation_id,
        expected_prior,
        replacement_fields,
    )
    .map_err(|error| error.to_string())?;
    let write_lease = retained_target
        .binding
        .lease()
        .derive_write_lease()
        .map_err(|error| format!("derive MV repartition preview lease: {error}"))?;
    let preview = write_lease
        .preview_managed_partition_spec(ConnectorManagedPartitionSpecPreviewRequest::new(
            operation_id,
            retained_target.binding.handle().clone(),
            replacement.clone(),
            connector_context.clone(),
        ))
        .map_err(|error| format!("preview managed MV repartition: {error}"))?;
    Ok(PreparedManagedRepartitionTransition {
        replacement,
        preview,
    })
}

fn managed_repartition_field(
    field: &IcebergPartitionFieldExpr,
) -> (
    &str,
    novarocks_spi::connector::ConnectorManagedPartitionTransform,
) {
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Transform;
    match field {
        IcebergPartitionFieldExpr::Identity { column } => (column, Transform::Identity),
        IcebergPartitionFieldExpr::Year { column } => (column, Transform::Year),
        IcebergPartitionFieldExpr::Month { column } => (column, Transform::Month),
        IcebergPartitionFieldExpr::Day { column } => (column, Transform::Day),
        IcebergPartitionFieldExpr::Hour { column } => (column, Transform::Hour),
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => (
            column,
            Transform::Bucket {
                buckets: *num_buckets,
            },
        ),
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            (column, Transform::Truncate { width: *width })
        }
        IcebergPartitionFieldExpr::Void { column } => (column, Transform::Void),
    }
}

fn managed_partition_transform(
    transform: &novarocks_mv_application::persistence::schema::MvPartitionTransformContract,
) -> novarocks_spi::connector::ConnectorManagedPartitionTransform {
    use novarocks_mv_application::persistence::schema::MvPartitionTransformContract as Observed;
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Managed;
    match transform {
        Observed::Identity => Managed::Identity,
        Observed::Year => Managed::Year,
        Observed::Month => Managed::Month,
        Observed::Day => Managed::Day,
        Observed::Hour => Managed::Hour,
        Observed::Bucket { num_buckets } => Managed::Bucket {
            buckets: *num_buckets,
        },
        Observed::Truncate { width } => Managed::Truncate { width: *width },
        Observed::Void => Managed::Void,
    }
}

/// The exact target's physical field IDs are positionally aligned with the
/// same retained observation's Arrow schema.
fn retained_target_field_id_by_name(
    retained_target: &RetainedRepartitionTarget,
) -> Result<Vec<(String, i32)>, String> {
    let schema = retained_target.binding.physical_write_schema()?;
    let field_ids = retained_target.binding.observation().field_ids();
    if schema.fields().len() != field_ids.len() {
        return Err(
            "MV repartition target observation does not align field IDs with its schema"
                .to_string(),
        );
    }
    Ok(schema
        .fields()
        .iter()
        .zip(field_ids)
        .map(|(field, field_id)| (field.name().clone(), *field_id))
        .collect())
}

/// Reparse D's immutable effective SQL under its own frozen resolution context.
/// The session that happens to issue the statement never resolves these names.
fn canonical_mv_select_query(
    projection: &StoredMvProjection,
) -> Result<novarocks_parser::ast::Query, String> {
    let effective_sql = projection.facts.definition().query.effective_sql.clone();
    canonical_mv_select_query_from_source(projection, &effective_sql)
}

/// Same canonical resolution, applied to the SQL an occurrence-aware rebind
/// produced for this attempt. The rebind never changes name resolution.
fn canonical_mv_select_query_from_source(
    projection: &StoredMvProjection,
    effective_sql: &str,
) -> Result<novarocks_parser::ast::Query, String> {
    let resolution = &projection.facts.definition().query.resolution;
    Ok(canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(effective_sql)?,
        Some(resolution.default_catalog.as_str()),
        &resolution.default_namespace,
    ))
}

/// D's relation shape decides whether one empty source may still refresh.
/// A two-relation join pair may; a fan-in of independent relations may not.
fn definition_has_join(query: &novarocks_parser::ast::Query) -> bool {
    novarocks_sql::planning::mv::extract_join_aliases(query).is_ok()
}

/// Prepare the SQL-shaped first-refresh artifact from persisted MV facts.
/// Ordinary refreshes deliberately re-read metadata only while SQL preparation
/// is active. Repartition reuses its retained exact target binding so schema,
/// partition, execution-generation, and write-lease facts cannot split.
/// This allocates no provider ref, write service, execution, or durable intent.
/// Join first refresh remains behind its typed logical binder and therefore
/// fails closed here rather than using the old frontend row-materialization
/// implementation.
#[allow(clippy::too_many_arguments)]
fn prepare_frontend_first_refresh_write(
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    observed_binding: ConnectorProviderBindingKey,
    repartition_transition: Option<&PreparedManagedRepartitionTransition>,
    retained_repartition_target: Option<&RetainedRepartitionTarget>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(PreparedMvFirstRefreshWrite, AdmittedMvDataPublication), String> {
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV first-refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    if let Some(retained) = retained_repartition_target {
        validate_retained_target_identity(&target, retained.binding.identity())?;
    }
    let planning_lease = match retained_repartition_target {
        Some(retained) => retained.binding.lease().clone(),
        None => crate::connector::acquire_metadata_planning_lease(
            source.connector_control(),
            &target.catalog,
        )?,
    };
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive MV first-refresh write lease: {error}"))?;
    if !write_lease.matches_provider_binding_key(&observed_binding) {
        return Err(
            "MV first-refresh target connector generation changed during admission".to_string(),
        );
    }
    let projection = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let (projection, refresh_query_source) = rebind_mv_definition_before_refresh_derivation(
        source.connector_control(),
        source.storage_observation(),
        &projection,
        &contract_base_tables(contract),
        &target,
        retained_repartition_target.map(|retained| &retained.schema_validation),
        &connector_context,
    )?;
    let mut admitted_publication = crate::mv::domain::staged_create::admit_mv_publication(
        source
            .management_entrance()
            .map_err(|error| error)?
            .as_ref(),
        &planning_lease,
        &projection,
        attempt.publication_id,
        &connector_context,
    )?;
    if let Some(transition) = repartition_transition {
        admitted_publication.set_repartition_partitioning(&transition.preview)?;
    }
    let mut publication_intent = frontend_refresh_publication_intent(
        source.connector_control(),
        &connector_context,
        contract,
        attempt,
        &projection,
        &admitted_publication,
        &refresh_query_source,
        base_table_object_ids,
    )?;
    if let Some(transition) = repartition_transition {
        // The retained binding already proved this is the exact installed
        // generation: `validate_projection_target` compared L's own
        // partition-spec version against it. There is no descriptor left to
        // compare, and the replacement carries the provider's expected prior
        // specification itself.
        retained_repartition_target.ok_or_else(|| {
            "MV repartition preparation lost its retained target binding".to_string()
        })?;
        publication_intent = publication_intent.with_partition_spec_replacement(
            transition.replacement.clone(),
            transition.preview.committed_partitioning().clone(),
        );
    }
    let loaded_target_binding;
    let target_binding = match retained_repartition_target {
        Some(retained) => &retained.binding,
        None => {
            loaded_target_binding = load_iceberg_mv_target_binding(
                source.connector_control(),
                source.storage_observation(),
                &target,
                &connector_context,
            )?;
            &loaded_target_binding
        }
    };
    let loaded_target_schema_validation;
    let target_schema_validation = match retained_repartition_target {
        Some(retained) => &retained.schema_validation,
        None => {
            loaded_target_schema_validation =
                crate::mv::domain::storage_observation::observe_schema_validation(
                    source.storage_observation(),
                    target_binding.lease(),
                    target_binding.metadata(),
                    connector_context.clone(),
                )
                .map_err(|error| {
                    format!(
                        "observe exact MV first-refresh target schema for {}.{}.{}: {error}",
                        target.catalog, target.namespace, target.table
                    )
                })?;
            &loaded_target_schema_validation
        }
    };
    let query = canonical_mv_select_query_from_source(&projection, &refresh_query_source)?;
    let runtime_bindings = validate_projection_target(&projection, target_schema_validation)?;
    let capabilities = RefreshCapabilities::from_canonical_facts(
        projection.facts.interpretation(),
        &runtime_bindings,
        projection.facts.definition().relation_occurrences.len(),
        definition_has_join(&query),
    )?;
    let target_arrow_schema = target_binding.physical_write_schema()?.as_ref().clone();
    let target_write_fields = Arc::<[arrow::datatypes::Field]>::from(
        target_arrow_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let target_field_ids = target_binding.observation().field_ids().to_vec();
    // `validate_projection_target` already proved this observation carries L's
    // exact partition-spec version; the numeric spec ID below is the provider's
    // own fact from that same observation, never a decode of the opaque bytes.
    let partition_spec_id = target_binding.partition().target_spec_id;
    let target_contract =
        novarocks_sql::planning::mv::first_refresh::MvFirstRefreshTargetContract::try_new(
            Arc::new(target_arrow_schema),
            target_field_ids,
            partition_spec_id,
            capabilities.apply_key_column.clone(),
        )?;
    let pin = pin_contract_bases(
        source,
        contract,
        &projection,
        base_table_object_ids,
        &connector_context,
        "MV first-refresh",
    )?;
    // P's input watermark is exactly what this pin says, so it is frozen here,
    // beside the pin, rather than reconstructed at the commit from facts that
    // would by then be a second reading of the same sources.
    let admitted_publication = AdmittedMvDataPublication::try_new(
        admitted_publication,
        &pin.exact_revisions_by_occurrence(),
    )?;
    // The canonical definition owns name resolution. The execution artifact is
    // transient and may be canonicalized, but it must never fall back to the
    // session which happens to issue REFRESH.
    let resolution = &projection.facts.definition().query.resolution;
    let source_catalog = Some(resolution.default_catalog.clone());
    let source_database = resolution.default_namespace.clone();
    let expected_target_snapshot_id = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
        RefreshStateBaseline::Pinless => None,
    };
    if definition_has_join(&query) && !capabilities.has_agg_state {
        if !matches!(
            contract.state_baseline,
            RefreshStateBaseline::SnapshotBacked { .. }
        ) {
            return Err("MV first-refresh join requires a snapshot-backed baseline".to_string());
        }
        let rewrite = freeze_refresh_rewrite_context(RefreshRewriteInputs {
            connector_control: source.connector_control(),
            connector_context: &connector_context,
            projection: Arc::new(projection.clone()),
            pin: &pin,
            state_baseline: &contract.state_baseline,
            target_binding,
            target_observation: target_schema_validation,
            runtime_bindings: &runtime_bindings,
            join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(&query)?,
            aggregate: None,
        })?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source.connector_control(),
            &connector_context,
            &rewrite,
        )?;
        let table = first_refresh_target_handle(
            retained_repartition_target.map(|retained| retained.binding.handle()),
            &write_lease,
            &target,
            connector_context.clone(),
        )?;
        let request = MvFirstRefreshWriteRequest::try_new(
            target.catalog,
            target.namespace,
            target.table,
            attempt.staging_branch(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            expected_target_snapshot_id,
            table,
            Arc::clone(&target_write_fields),
            observed_binding,
            attempt.write_operation_id(),
        )?;
        let prepared = MvFirstRefreshWritePreparer::prepare_join_logical(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            publication_intent,
        )?;
        return Ok((
            if repartition_transition.is_some() {
                prepared.into_full_overwrite()
            } else {
                prepared
            },
            admitted_publication,
        ));
    }
    let sql_pin =
        novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPin::try_from_occurrences(
            pin.occurrences()
                .iter()
                .map(|occurrence| {
                    novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPinOccurrence::try_new(
                        occurrence.occurrence_id(),
                        occurrence.table().clone(),
                        occurrence.snapshot_id(),
                        occurrence.table_object_id().clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )?;
    // L owns the branch and relation shape; the count of UNION branches is the
    // number of branch interpretations, not a separately persisted number.
    let branch_count = projection.facts.interpretation().branches.len();
    let multi_relation = projection.facts.definition().relation_occurrences.len() > 1;
    let shape = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if branch_count > 0 {
            crate::mv::domain::rewrite::context::first_union_branch_query(&query)?
        } else {
            query.clone()
        };
        let calls = extract_aggregate_sql_calls(&aggregate_query)?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout_sql = if branch_count > 0 {
            novarocks_parser::printer::print_query(&aggregate_query)
        } else {
            novarocks_parser::printer::print_query(&query)
        };
        let aggregate_layout = build_aggregate_layout_for_refresh_select_sql(
            source,
            source_catalog.as_deref(),
            &source_database,
            &aggregate_layout_sql,
            &connector_context,
        )?;
        if branch_count > 0 {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::BranchUnionAggregate {
                branch_count,
                calls,
            }
        } else if multi_relation {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::FanInAggregate {
                calls,
                aggregate_input_types: aggregate_layout
                    .runtime_layout()
                    .aggregate_input_types()
                    .to_vec(),
            }
        } else {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::Aggregate {
                calls,
                aggregate_input_types: aggregate_layout
                    .runtime_layout()
                    .aggregate_input_types()
                    .to_vec(),
            }
        }
    } else if branch_count > 0 {
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::UnionProjection {
            branch_count,
        }
    } else {
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::Projection
    };
    let physical_sql =
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactBuilder::try_new(
            query.clone(),
            sql_pin,
            source_catalog,
            source_database,
            target_contract,
            shape,
        )?
        .build()?;
    let table = first_refresh_target_handle(
        retained_repartition_target.map(|retained| retained.binding.handle()),
        &write_lease,
        &target,
        connector_context.clone(),
    )?;
    let request = MvFirstRefreshWriteRequest::try_new(
        target.catalog,
        target.namespace,
        target.table,
        attempt.staging_branch(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        expected_target_snapshot_id,
        table,
        target_write_fields,
        observed_binding,
        attempt.write_operation_id(),
    )?;
    let prepared = MvFirstRefreshWritePreparer::prepare(request, physical_sql, publication_intent)?;
    Ok((
        if repartition_transition.is_some() {
            prepared.into_full_overwrite()
        } else {
            prepared
        },
        admitted_publication,
    ))
}

fn first_refresh_target_handle(
    retained: Option<&novarocks_spi::connector::ConnectorTableHandle>,
    write_lease: &novarocks_spi::connector::ConnectorWriteLease,
    target: &IcebergMvTarget,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorTableHandle, String> {
    select_retained_target_handle(retained, || {
        crate::catalog_application::resolver::iceberg_connector_table_handle(
            write_lease,
            &crate::catalog_application::resolver::TargetBackend {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .expect("static Iceberg provider ID"),
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            connector_context,
        )
    })
}

pub(crate) fn select_retained_target_handle(
    retained: Option<&novarocks_spi::connector::ConnectorTableHandle>,
    load_current: impl FnOnce() -> Result<novarocks_spi::connector::ConnectorTableHandle, String>,
) -> Result<novarocks_spi::connector::ConnectorTableHandle, String> {
    match retained {
        Some(handle) => Ok(handle.clone()),
        None => load_current(),
    }
}

fn frontend_refresh_publication_intent(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    projection: &StoredMvProjection,
    admitted: &crate::mv::domain::staged_create::AdmittedMvPublication,
    select_sql: &str,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
) -> Result<MvRefreshPublicationIntent, String> {
    let snapshots = pinned_snapshots_by_occurrence(contract, "MV staging provenance")?;
    let previous_sources = baseline_previous_sources(&contract.state_baseline);
    mv_refresh_publication_intent(
        connector_control,
        connector_context,
        attempt.publication_id,
        projection.facts.source_revision().target_object_id.clone(),
        expected_target_snapshot(contract),
        admitted.admission().clone(),
        MvRefreshPublicationTechnique::Full,
        &snapshots,
        base_table_object_ids,
        previous_sources,
        mv_definition_fingerprint(select_sql),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV refresh publication target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
    )
}

/// Pin every contract base to the exact revision D's matching occurrence names.
///
/// A publication's input watermark is exactly what this pin says, so the check
/// that D, the contract and the live source still name the same object happens
/// here -- once, for every technique that freezes one -- rather than being
/// reconstructed at the commit from a second reading of the same sources.
fn pin_contract_bases(
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    projection: &novarocks_mv_application::persistence::projection::StoredMvProjection,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    what: &str,
) -> Result<RefreshSnapshotPin, String> {
    let definition_occurrences = &projection.facts.definition().relation_occurrences;
    if definition_occurrences.len() != contract.base_refs.len() {
        return Err(format!(
            "{what} base facts do not retain every D occurrence"
        ));
    }
    RefreshSnapshotPin::try_from_occurrences(
        definition_occurrences
            .iter()
            .zip(&contract.base_refs)
            .map(|(occurrence, base)| {
                if occurrence.occurrence_id != base.occurrence_id.get()
                    || occurrence.catalog_at_binding != base.table.catalog
                    || occurrence.namespace_at_binding != base.table.namespace
                    || occurrence.relation_at_binding != base.table.table
                {
                    return Err(format!(
                        "{what} base does not match D occurrence {}",
                        occurrence.occurrence_id,
                    ));
                }
                let named = base.display();
                let snapshot_id = contract
                    .snapshot_pins
                    .get(&base.occurrence_id)
                    .and_then(|snapshot| *snapshot)
                    .ok_or_else(|| format!("{what} has no pinned snapshot for {named}"))?;
                let (observed, exact_revision) =
                    crate::mv::domain::refresh_io::observe_current_refresh_revision_with_ports(
                        source.connector_control(),
                        source.storage_observation(),
                        &base.table,
                        connector_context,
                    )?;
                if observed.current_snapshot_id() != Some(snapshot_id) {
                    return Err(format!(
                        "{what} source snapshot changed after planning for {named}"
                    ));
                }
                let object_id = observed.object_id().clone();
                let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
                    .map_err(|error| format!("persist {what} source identity: {error}"))?;
                if persisted_object != occurrence.object_id {
                    return Err(format!(
                        "{what} source object changed for D occurrence {}",
                        occurrence.occurrence_id,
                    ));
                }
                let expected_object_id = base_table_object_ids
                    .get(&base.occurrence_id)
                    .ok_or_else(|| format!("{what} has no object-ID fact for {named}"))?;
                if &object_id != expected_object_id {
                    return Err(format!(
                        "{what} base table identity changed after planning for {named}"
                    ));
                }
                RefreshSnapshotPinOccurrence::try_new(
                    base.occurrence_id,
                    base.table.clone(),
                    snapshot_id,
                    object_id,
                    exact_revision,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
}

/// Prepare the publication a refresh whose inputs did not move commits.
///
/// It freezes exactly what a data publication freezes -- the same admission,
/// the same input watermark, the same target -- because it commits through the
/// same write session. The one thing it does not carry is a plan: there is no
/// window to read, so the session it opens writes nothing, and the output
/// version that commit mints is the whole published effect.
fn prepare_metadata_only_publication(
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: &novarocks_spi::connector::ConnectorProviderBindingKey,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
) -> Result<(PreparedMvMetadataOnlyWrite, AdmittedMvDataPublication), String> {
    let snapshots = pinned_snapshots_by_occurrence(contract, "MV metadata-only provenance")?;
    let RefreshStateBaseline::SnapshotBacked {
        previous_sources,
        definition_fingerprint,
        ..
    } = &contract.state_baseline
    else {
        return Err(
            "MV metadata-only refresh requires a snapshot-backed target baseline".to_string(),
        );
    };
    let target = IcebergMvTarget {
        catalog: contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV metadata-only target has no connector catalog".to_string())?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let projection = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        source.connector_control(),
        &target.catalog,
    )?;
    let admitted = crate::mv::domain::staged_create::admit_mv_publication(
        source.management_entrance()?.as_ref(),
        &planning_lease,
        &projection,
        attempt.publication_id,
        connector_context,
    )?;
    let intent = mv_refresh_publication_intent(
        source.connector_control(),
        &connector_context,
        attempt.publication_id,
        projection.facts.source_revision().target_object_id.clone(),
        expected_target_snapshot(contract),
        admitted.admission().clone(),
        MvRefreshPublicationTechnique::MetadataOnly,
        &snapshots,
        base_table_object_ids,
        previous_sources,
        definition_fingerprint.clone(),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV metadata-only target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
    )?;
    let pin = pin_contract_bases(
        source,
        contract,
        &projection,
        base_table_object_ids,
        connector_context,
        "MV metadata-only refresh",
    )?;
    let admitted =
        AdmittedMvDataPublication::try_new(admitted, &pin.exact_revisions_by_occurrence())?;
    // The session opens against the target itself, so it declares the same
    // handle and input shape a data publication does. Both are read from the
    // exact installed binding, so a metadata-only publication cannot declare a
    // shape the target no longer has.
    let target_binding = load_iceberg_mv_target_binding(
        source.connector_control(),
        source.storage_observation(),
        &target,
        connector_context,
    )?;
    let write_input_fields = Arc::<[arrow::datatypes::Field]>::from(
        target_binding
            .physical_write_schema()?
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let write = PreparedMvMetadataOnlyWrite::try_new(
        intent,
        target_binding.handle().clone(),
        write_input_fields,
        observed_binding.clone(),
    )?;
    Ok((write, admitted))
}

#[allow(clippy::too_many_arguments)]
fn mv_refresh_publication_intent(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    publication_id: novarocks_spi::connector::LakePublicationId,
    target_object_id: ConnectorTableObjectId,
    expected_target_snapshot_id: Option<i64>,
    admission: novarocks_spi::connector::document_storage::ConnectorDocumentManagementAdmission,
    technique: MvRefreshPublicationTechnique,
    snapshots: &BTreeMap<SqlMvRelationOccurrenceId, (novarocks_types::naming::TableIdentity, i64)>,
    base_table_object_ids: &BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    previous_sources: &[RefreshStateBaselineSource],
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
) -> Result<MvRefreshPublicationIntent, String> {
    // The publication records the window it consumed, so each base carries the
    // point the published baseline pinned it at as well as the point this
    // refresh read. A source this MV has not published before has no
    // predecessor, which is a fact about a first publication rather than a
    // missing one.
    let previous = crate::mv::domain::refresh::planning::baseline_predecessors(
        previous_sources,
        connector_control,
        connector_context,
    )?;
    let bases = snapshots
        .iter()
        .map(|(occurrence_id, (table, to_snapshot))| {
            MvRefreshPublicationBase::try_new(
                occurrence_id.get(),
                table.fqn(),
                base_table_object_ids
                    .get(occurrence_id)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "MV refresh publication has no object-ID fact for {}",
                            table.fqn()
                        )
                    })?,
                previous.snapshots.get(occurrence_id).copied(),
                *to_snapshot,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    MvRefreshPublicationIntent::try_new(
        publication_id,
        target_object_id,
        expected_target_snapshot_id,
        admission,
        technique,
        bases,
        definition_fingerprint,
        target_catalog,
        target_namespace,
        target_name,
    )
}

fn expected_target_snapshot(contract: &RefreshPlanContract) -> Option<i64> {
    match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
        RefreshStateBaseline::Pinless => None,
    }
}

/// Ordered exact source revisions the published baseline pinned, if any.
/// What each occurrence was pinned at, for the provenance a publication
/// records. An unpinned base is a planning mistake, not an absent fact.
fn pinned_snapshots_by_occurrence(
    contract: &RefreshPlanContract,
    context: &str,
) -> Result<
    BTreeMap<SqlMvRelationOccurrenceId, (novarocks_types::naming::TableIdentity, i64)>,
    String,
> {
    contract
        .base_refs
        .iter()
        .map(|base| {
            contract
                .snapshot_pins
                .get(&base.occurrence_id)
                .copied()
                .flatten()
                .map(|snapshot| (base.occurrence_id, (base.table.clone(), snapshot)))
                .ok_or_else(|| format!("{context} has no pinned snapshot for {}", base.display()))
        })
        .collect()
}

/// The contract's base relations as a positional table list.
///
/// Some consumers still match each base against D's occurrence at the same
/// index rather than by occurrence id. The order is the contract's own, so
/// projecting away the ids here loses nothing those consumers use.
fn contract_base_tables(
    contract: &RefreshPlanContract,
) -> Vec<novarocks_types::naming::TableIdentity> {
    contract
        .base_refs
        .iter()
        .map(|base| base.table.clone())
        .collect()
}

fn baseline_previous_sources(baseline: &RefreshStateBaseline) -> &[RefreshStateBaselineSource] {
    match baseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_sources, ..
        } => previous_sources,
        RefreshStateBaseline::Pinless => &[],
    }
}

/// Prepare the value-only non-join incremental handoff.  This is deliberately
/// limited to change-stream shapes that already have one generic native
/// writer contract.  Join branches and policy-driven full rebuilds retain
/// their explicit preparation boundary until their distinct physical artifacts
/// are extracted; treating either as an append would be incorrect.
#[allow(clippy::too_many_arguments)]
enum PreparedIncrementalRefreshWork {
    MetadataOnly,
    ChangeStream(PreparedMvIncrementalWrite, AdmittedMvDataPublication),
    FullRebuild(PreparedMvFirstRefreshWrite, AdmittedMvDataPublication),
}

fn prepare_frontend_incremental_write(
    source: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    observed_binding: ConnectorProviderBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedIncrementalRefreshWork, String> {
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV incremental refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let projection = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let canonical_query = canonical_mv_select_query(&projection)?;
    let interpretation = projection.facts.interpretation();
    let is_join = definition_has_join(&canonical_query);
    let is_aggregate = !interpretation.aggregates.is_empty();
    let is_branch_union = !interpretation.branches.is_empty();
    let join_bases = if is_join {
        let (left, right) =
            join_base_refs_for_definition(&projection, &canonical_query, &contract.base_refs)?;
        Some((left.clone(), right.clone()))
    } else {
        None
    };
    let RefreshStateBaseline::SnapshotBacked {
        target_snapshot_id,
        definition_fingerprint,
        ..
    } = &contract.state_baseline
    else {
        return Err(
            "MV incremental refresh requires a snapshot-backed target baseline".to_string(),
        );
    };
    let definition_occurrences = &projection.facts.definition().relation_occurrences;
    if definition_occurrences.len() != contract.base_refs.len() {
        return Err(
            "MV incremental refresh base facts do not retain every D occurrence".to_string(),
        );
    }
    let pin = RefreshSnapshotPin::try_from_occurrences(
        definition_occurrences
            .iter()
            .zip(&contract.base_refs)
            .map(|(occurrence, base)| {
                if occurrence.occurrence_id != base.occurrence_id.get()
                    || occurrence.catalog_at_binding != base.table.catalog
                    || occurrence.namespace_at_binding != base.table.namespace
                    || occurrence.relation_at_binding != base.table.table
                {
                    return Err(format!(
                        "MV incremental refresh base does not match D occurrence {}",
                        occurrence.occurrence_id,
                    ));
                }
                let snapshot_id = contract
                    .snapshot_pins
                    .get(&base.occurrence_id)
                    .and_then(|snapshot| *snapshot)
                    .ok_or_else(|| {
                        format!(
                            "MV incremental refresh has no pinned snapshot for {}",
                            base.display()
                        )
                    })?;
                let (observed, exact_revision) =
                    crate::mv::domain::refresh_io::observe_current_refresh_revision_with_ports(
                        source.connector_control(),
                        source.storage_observation(),
                        &base.table,
                        &connector_context,
                    )?;
                if observed.current_snapshot_id() != Some(snapshot_id) {
                    return Err(format!(
                        "MV incremental refresh source snapshot changed after planning for {}",
                        base.display()
                    ));
                }
                let (persisted_object, _) = persist_exact_connector_revision(&exact_revision)
                    .map_err(|error| {
                        format!("persist incremental-refresh source identity: {error}")
                    })?;
                if persisted_object != occurrence.object_id {
                    return Err(format!(
                        "MV incremental refresh source object changed for D occurrence {}",
                        occurrence.occurrence_id,
                    ));
                }
                RefreshSnapshotPinOccurrence::try_new(
                    base.occurrence_id,
                    base.table.clone(),
                    snapshot_id,
                    observed.object_id().clone(),
                    exact_revision,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?;
    let (projection, _refresh_query_source) = rebind_mv_definition_before_refresh_derivation(
        source.connector_control(),
        source.storage_observation(),
        &projection,
        &contract_base_tables(contract),
        &target,
        None,
        &connector_context,
    )?;
    let target_binding = load_iceberg_mv_target_binding(
        source.connector_control(),
        source.storage_observation(),
        &target,
        &connector_context,
    )?;
    let target_schema_validation =
        crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            target_binding.lease(),
            target_binding.metadata(),
            connector_context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe exact MV incremental target schema for {}.{}.{}: {error}",
                target.catalog, target.namespace, target.table
            )
        })?;
    let runtime_bindings = validate_projection_target(&projection, &target_schema_validation)?;
    // An incremental refresh of an aggregate view needs the same analyzed
    // aggregate facts a first refresh does: L says the view aggregates, and a
    // rewrite that cannot say which SQL call each aggregate identity belongs
    // to has nothing to maintain the state columns with.
    let aggregate = frozen_refresh_aggregate_analysis(
        source,
        &projection,
        &canonical_query,
        &connector_context,
    )?;
    let rewrite = freeze_refresh_rewrite_context(RefreshRewriteInputs {
        connector_control: source.connector_control(),
        connector_context: &connector_context,
        projection: Arc::new(projection),
        pin: &pin,
        state_baseline: &contract.state_baseline,
        target_binding: &target_binding,
        target_observation: &target_schema_validation,
        runtime_bindings: &runtime_bindings,
        join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(
            &canonical_query,
        )?,
        aggregate,
    })?;

    if let Some((left_ref, right_ref)) = join_bases.as_ref() {
        let left_from = rewrite.previous_snapshot_id(left_ref)?;
        let right_from = rewrite.previous_snapshot_id(right_ref)?;
        let left_to = rewrite.pinned_snapshot_id(left_ref)?;
        let right_to = rewrite.pinned_snapshot_id(right_ref)?;
        let (left_admission, _) = observe_and_admit_change_window_for_table(
            source.connector_control(),
            source.storage_observation(),
            &left_ref.table,
            rewrite.previous_revision(left_ref)?,
            left_from,
            left_to,
            &connector_context,
        )?;
        let (right_admission, _) = observe_and_admit_change_window_for_table(
            source.connector_control(),
            source.storage_observation(),
            &right_ref.table,
            rewrite.previous_revision(right_ref)?,
            right_from,
            right_to,
            &connector_context,
        )?;
        let left_facts = admitted_change_facts(&left_admission);
        let right_facts = admitted_change_facts(&right_admission);
        let mut full_rebuild_reasons = Vec::new();
        if let Err(reason) = &left_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", left_ref.display()));
        }
        if let Err(reason) = &right_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", right_ref.display()));
        }
        if !full_rebuild_reasons.is_empty() {
            tracing::info!(
                target = %rewrite.target.fqn(),
                reasons = %full_rebuild_reasons.join("; "),
                "MV join refresh admission selected a distributed full-rebuild staging overwrite"
            );
            let (rebuild, admitted) = prepare_frontend_first_refresh_write(
                source,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pinned_objects_by_occurrence()?,
                observed_binding,
                None,
                None,
                connector_context,
            )?;
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(
                rebuild.into_full_overwrite(),
                admitted,
            ));
        }
        let left_facts = left_facts.expect("full-rebuild admission returned above");
        let right_facts = right_facts.expect("full-rebuild admission returned above");
        let branches = crate::mv::domain::iceberg_join_branch::plan_join_delta_branches(
            &left_ref.table,
            &right_ref.table,
            crate::mv::domain::iceberg_join_branch::SnapshotWindow {
                from: left_from,
                to: left_to,
            },
            crate::mv::domain::iceberg_join_branch::SnapshotWindow {
                from: right_from,
                to: right_to,
            },
            left_facts.has_inserts || left_facts.has_deletes,
            right_facts.has_inserts || right_facts.has_deletes,
        );
        if branches.is_empty() {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        let join_mode = if is_aggregate {
            MvIncrementalJoinMode::Coalesce
        } else {
            select_join_incremental_execution_mode(left_facts.has_deletes, right_facts.has_deletes)
        };
        let request = MvIncrementalWriteRequest::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
            attempt.staging_branch(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            *target_snapshot_id,
            observed_binding,
            attempt.write_operation_id(),
            target_binding.handle().clone(),
        )?;
        let admitted_publication = crate::mv::domain::staged_create::admit_mv_publication(
            source.management_entrance()?.as_ref(),
            target_binding.lease(),
            rewrite.mv_definition.as_ref(),
            attempt.publication_id,
            &connector_context,
        )?;
        let publication_intent = mv_refresh_publication_intent(
            source.connector_control(),
            &connector_context,
            attempt.publication_id,
            rewrite
                .mv_definition
                .facts
                .source_revision()
                .target_object_id
                .clone(),
            *target_snapshot_id,
            admitted_publication.admission().clone(),
            MvRefreshPublicationTechnique::Incremental,
            &rewrite.pinned_snapshots_by_occurrence()?,
            &rewrite.pinned_objects_by_occurrence()?,
            baseline_previous_sources(&contract.state_baseline),
            definition_fingerprint.clone(),
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source.connector_control(),
            &connector_context,
            &rewrite,
        )?;
        let admitted_publication = AdmittedMvDataPublication::try_new(
            admitted_publication,
            &rewrite.exact_revisions_by_occurrence(),
        )?;
        return MvIncrementalWritePreparer::prepare(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            match join_mode {
                MvIncrementalJoinMode::AppendOnly => {
                    MvIncrementalWriteMode::FastAppend
                }
                MvIncrementalJoinMode::Coalesce => {
                    MvIncrementalWriteMode::RowDelta
                }
            },
            if is_aggregate {
                MvIncrementalRewriteEvidence::JoinAggregate
            } else {
                MvIncrementalRewriteEvidence::None
            },
            MvIncrementalExecutionArtifact::JoinLogical {
                mode: match join_mode {
                    MvIncrementalJoinMode::AppendOnly => {
                        MvIncrementalJoinMode::AppendOnly
                    }
                    MvIncrementalJoinMode::Coalesce => {
                        MvIncrementalJoinMode::Coalesce
                    }
                },
            },
            publication_intent,
        )
        .map(|write| PreparedIncrementalRefreshWork::ChangeStream(write, admitted_publication));
    }

    let loaded_bases = rewrite
        .base_refs
        .iter()
        .map(|base| {
            let previous_snapshot_id = rewrite.previous_snapshot_id(base)?;
            let current_snapshot_id = rewrite.pinned_snapshot_id(base)?;
            let current_table_object_id = rewrite.pinned_table_object_id(base)?;
            let observed = observe_schema_validation_for_table(
                source.connector_control(),
                source.storage_observation(),
                &base.table,
                &connector_context,
            )?;
            if observed.table_object_id() != &current_table_object_id {
                return Err(format!(
                    "MV incremental refresh base table identity changed after planning for {}",
                    base.display()
                ));
            }
            let (admission, _) = observe_and_admit_change_window_for_table(
                source.connector_control(),
                source.storage_observation(),
                &base.table,
                rewrite.previous_revision(base)?,
                previous_snapshot_id,
                current_snapshot_id,
                &connector_context,
            )?;
            Ok::<_, String>((
                base,
                previous_snapshot_id,
                current_snapshot_id,
                current_table_object_id.clone(),
                admission,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let changes = loaded_bases
        .iter()
        .map(
            |(
                base_ref,
                previous_snapshot_id,
                current_snapshot_id,
                current_table_object_id,
                admission,
            )| {
                NonJoinBaseChange {
                    occurrence_id: base_ref.occurrence_id,
                    base_ref: &base_ref.table,
                    previous_snapshot_id: *previous_snapshot_id,
                    current_snapshot_id: *current_snapshot_id,
                    current_table_object_id,
                    admission: admission.clone(),
                }
            },
        )
        .collect::<Vec<_>>();
    let (mode, evidence) = match plan_non_join_incremental_changes(&changes)? {
        NonJoinIncrementalChangePlan::MetadataOnly(_) => {
            return Ok(PreparedIncrementalRefreshWork::MetadataOnly);
        }
        NonJoinIncrementalChangePlan::FullRebuild { reason, .. } => {
            tracing::info!(
                target = %rewrite.target.fqn(),
                "MV refresh SQL preparation selected a distributed full-rebuild staging overwrite: {reason}"
            );
            let (rebuild, admitted) = prepare_frontend_first_refresh_write(
                source,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pinned_objects_by_occurrence()?,
                observed_binding,
                None,
                None,
                connector_context,
            )?;
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(
                rebuild.into_full_overwrite(),
                admitted,
            ));
        }
        NonJoinIncrementalChangePlan::ChangeStream {
            has_delete_changes, ..
        } => {
            let mode = non_join_incremental_write_mode(is_aggregate, has_delete_changes);
            let evidence = if is_aggregate {
                if is_branch_union {
                    MvIncrementalRewriteEvidence::BranchUnionAggregate
                } else {
                    MvIncrementalRewriteEvidence::Aggregate
                }
            } else {
                MvIncrementalRewriteEvidence::None
            };
            (mode, evidence)
        }
    };
    let request = MvIncrementalWriteRequest::try_new(
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        attempt.staging_branch(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        *target_snapshot_id,
        observed_binding,
        attempt.write_operation_id(),
        target_binding.handle().clone(),
    )?;
    let admitted_publication = crate::mv::domain::staged_create::admit_mv_publication(
        source.management_entrance()?.as_ref(),
        target_binding.lease(),
        rewrite.mv_definition.as_ref(),
        attempt.publication_id,
        &connector_context,
    )?;
    let publication_intent = mv_refresh_publication_intent(
        source.connector_control(),
        &connector_context,
        attempt.publication_id,
        rewrite
            .mv_definition
            .facts
            .source_revision()
            .target_object_id
            .clone(),
        *target_snapshot_id,
        admitted_publication.admission().clone(),
        MvRefreshPublicationTechnique::Incremental,
        &rewrite.pinned_snapshots_by_occurrence()?,
        &rewrite.pinned_objects_by_occurrence()?,
        baseline_previous_sources(&contract.state_baseline),
        definition_fingerprint.clone(),
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
    )?;
    let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        source.connector_control(),
        &connector_context,
        &rewrite,
    )?;
    let admitted_publication = AdmittedMvDataPublication::try_new(
        admitted_publication,
        &rewrite.exact_revisions_by_occurrence(),
    )?;
    MvIncrementalWritePreparer::prepare(
        request,
        crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
            &rewrite,
            contract.affected_partitions.clone(),
            Some(frozen_base_overlays),
        )?,
        mode,
        evidence,
        MvIncrementalExecutionArtifact::CanonicalQuery,
        publication_intent,
    )
    .map(|write| PreparedIncrementalRefreshWork::ChangeStream(write, admitted_publication))
}
