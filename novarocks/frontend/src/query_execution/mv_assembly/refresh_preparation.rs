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
use crate::mv::domain::application::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};
use crate::mv::domain::iceberg_refresh::{
    IcebergMvCorePorts, join_base_refs_for_schema_contract,
    plan_iceberg_mv_refresh_with_connector_context,
};
use crate::mv::domain::lifecycle::RefreshError;
use crate::mv::domain::persistence::schema::{
    MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
};
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
    observe_current_refresh_base, observe_schema_validation_for_table,
    rebind_mv_definition_before_refresh_derivation,
};
use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::planning::{RefreshPlanContract, RefreshStateBaseline};
use crate::mv::domain::refresh::repartition::select_repartition_shape;
use crate::mv::domain::refresh::rewrite_context::{
    admitted_change_facts, build_neutral_refresh_rewrite_context,
    observe_and_admit_change_window_for_table,
};
use crate::mv::domain::refresh::schema_contract::validate_repartition_schema_contract;
use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
use crate::mv::domain::refresh::target::{IcebergMvTarget, load_iceberg_mv_target_binding};
use crate::mv::domain::storage_observation::MvSchemaValidationObservation;
use crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvFirstRefreshWritePreparer, MvFirstRefreshWriteRequest, MvIncrementalExecutionArtifact,
    MvIncrementalWritePreparer, MvIncrementalWriteRequest, MvRefreshPublicationBase,
    MvRefreshPublicationIntent, MvRefreshPublicationTechnique, PreparedMvFirstRefreshWrite,
    PreparedMvIncrementalWrite,
};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, MvRefreshPreparationRequest, MvRefreshPreparationService,
    PreparedMvRefresh, PreparedMvRefreshWork, PreparedMvRefreshWrite,
};
use novarocks_spi::connector::{
    ConnectorCommittedPartitioning, ConnectorInstanceId, ConnectorManagedDescriptorProperties,
    ConnectorManagedPartitionSpecPreviewRequest, ConnectorProviderBindingKey,
    ConnectorTableIdentity, ConnectorTableObjectId,
};
use novarocks_sql::planning::mv::MvRefreshFinalizeFacts;
use novarocks_sql::planning::mv::{SqlMvAggregateLayoutScope, extract_aggregate_sql_calls};
use novarocks_sql::semantic::IcebergPartitionFieldExpr;
pub struct StandaloneMvRefreshPreparationService<'a> {
    source: &'a IcebergMvCorePorts,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    statement: &'a MvRefreshRequest,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    repartition_fields: Option<&'a [IcebergPartitionFieldExpr]>,
}

impl<'a> StandaloneMvRefreshPreparationService<'a> {
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
    )?;
    let facts = visible_analysis
        .refresh_input
        .aggregate_layout_facts(&visible_query, SqlMvAggregateLayoutScope::WholeQuery)?;
    novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(&facts)
}

impl MvRefreshPreparationService for StandaloneMvRefreshPreparationService<'_> {
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
                    base,
                    self.connector_context,
                )
                .and_then(|observed| {
                    observed
                        .table_object_id()
                        .cloned()
                        .map(|object_id| (base.fqn(), object_id))
                        .ok_or_else(|| {
                            format!(
                                "MV refresh base observation has no captured table object ID for {}",
                                base.fqn()
                            )
                        })
                })
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
        let work = match plan.contract.decision {
            ExecutableRefreshDecision::SkipEmpty => PreparedMvRefreshWork::NoOp,
            ExecutableRefreshDecision::MetadataOnly => PreparedMvRefreshWork::MetadataOnly {
                intent: metadata_only_publication_intent(
                    self.source,
                    &plan.contract,
                    &request.attempt,
                    &base_table_object_ids,
                )?,
            },
            ExecutableRefreshDecision::FirstRefresh => PreparedMvRefreshWork::DataProducing {
                write: PreparedMvRefreshWrite::first_refresh(prepare_frontend_first_refresh_write(
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
                )?),
            },
            ExecutableRefreshDecision::Incremental => match prepare_frontend_incremental_write(
                self.source,
                self.current_catalog,
                self.current_database,
                &plan.contract,
                &request.attempt,
                observed_binding.clone(),
                self.connector_context.clone(),
            )? {
                PreparedIncrementalRefreshWork::ChangeStream(incremental) => {
                    PreparedMvRefreshWork::DataProducing {
                        write: PreparedMvRefreshWrite::incremental(incremental),
                    }
                }
                PreparedIncrementalRefreshWork::FullRebuild(rebuild) => {
                    PreparedMvRefreshWork::DataProducing {
                        write: PreparedMvRefreshWrite::first_refresh(rebuild),
                    }
                }
                PreparedIncrementalRefreshWork::MetadataOnly => {
                    PreparedMvRefreshWork::MetadataOnly {
                        intent: metadata_only_publication_intent(
                            self.source,
                            &plan.contract,
                            &request.attempt,
                            &base_table_object_ids,
                        )?,
                    }
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
    preview: ConnectorCommittedPartitioning,
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
    let definition = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let schema_contract = definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing its schema contract; recreate the MV before repartitioning",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.branch.is_some() && schema_contract.aggregate.is_some() {
        return Err(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support branch UNION ALL aggregates"
                .to_string(),
        );
    }
    select_repartition_shape(&RefreshCapabilities::from_schema_contract(schema_contract)?)?;
    validate_repartition_schema_contract(
        source.connector_control(),
        source.storage_observation(),
        schema_contract,
        &contract.base_refs,
        &retained_target.schema_validation,
        connector_context,
    )?;
    let query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.query_definition.raw_query_source)?,
        Some(
            definition
                .query_definition
                .resolution
                .default_catalog
                .as_str(),
        ),
        &definition.query_definition.resolution.default_database,
    );
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
    )?;
    validate_mv_partition_columns(Some(fields), &analysis.output_columns)?;
    if derive_fragment_property(&analysis)?.is_composed_aggregate_schema_contract_fallback() {
        return Err("partitioned composed aggregate Iceberg MV is not supported".to_string());
    }

    let observation = &retained_target.schema_validation;
    let prior_fields = observation
        .partition()
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                field.source_target_field_id(),
                u32::try_from(position)
                    .map_err(|_| "MV repartition prior field count exceeds u32".to_string())?,
                managed_partition_transform(field.transform())?,
            )
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_prior =
        novarocks_spi::connector::ConnectorManagedPartitionSpecObservation::try_from_fields(
            observation.partition().spec_id(),
            &prior_fields,
        )
        .map_err(|error| error.to_string())?;
    let replacement_fields = fields
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let (column, transform) = managed_repartition_field(field);
            let source = observation
                .fields()
                .iter()
                .find(|candidate| candidate.name().eq_ignore_ascii_case(column))
                .ok_or_else(|| {
                    format!(
                        "MV repartition source column `{column}` is missing from the exact target observation"
                    )
                })?;
            novarocks_spi::connector::ConnectorManagedPartitionField::try_new(
                source.field_id(),
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
        preview: preview.committed_partitioning().clone(),
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
    transform: &crate::mv::domain::storage_observation::MvSchemaValidationPartitionTransform,
) -> Result<novarocks_spi::connector::ConnectorManagedPartitionTransform, String> {
    use crate::mv::domain::storage_observation::MvSchemaValidationPartitionTransform as Observed;
    use novarocks_spi::connector::ConnectorManagedPartitionTransform as Managed;
    match transform {
        Observed::Identity => Ok(Managed::Identity),
        Observed::Year => Ok(Managed::Year),
        Observed::Month => Ok(Managed::Month),
        Observed::Day => Ok(Managed::Day),
        Observed::Hour => Ok(Managed::Hour),
        Observed::Bucket { num_buckets } => Ok(Managed::Bucket {
            buckets: *num_buckets,
        }),
        Observed::Truncate { width } => Ok(Managed::Truncate { width: *width }),
        Observed::Void => Ok(Managed::Void),
        Observed::Unsupported(name) => Err(format!(
            "MV repartition cannot preserve unsupported prior partition transform `{name}`"
        )),
    }
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
    base_table_object_ids: &BTreeMap<String, ConnectorTableObjectId>,
    observed_binding: ConnectorProviderBindingKey,
    repartition_transition: Option<&PreparedManagedRepartitionTransition>,
    retained_repartition_target: Option<&RetainedRepartitionTarget>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedMvFirstRefreshWrite, String> {
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
    let definition = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let (definition, refresh_query_source) = rebind_mv_definition_before_refresh_derivation(
        source.connector_control(),
        source.storage_observation(),
        &definition,
        &contract.base_refs,
        &target,
        retained_repartition_target.map(|retained| &retained.schema_validation),
        &connector_context,
    )?;
    let mut publication_intent = frontend_refresh_publication_intent(
        contract,
        attempt,
        &definition,
        &refresh_query_source,
        base_table_object_ids,
    )?;
    if let Some(transition) = repartition_transition {
        let retained = retained_repartition_target.ok_or_else(|| {
            "MV repartition preparation lost its retained target binding".to_string()
        })?;
        let package = crate::mv::domain::storage_observation::observe_lake_package(
            source.storage_observation(),
            retained.binding.lease(),
            retained.binding.metadata(),
            connector_context.clone(),
        )
        .map_err(|error| format!("observe exact MV descriptor for repartition: {error}"))?
        .ok_or_else(|| "MV repartition target is missing its lake descriptor".to_string())?;
        if package
            .source_revision()
            .map_err(|error| error.to_string())?
            .descriptor_content_hash
            != definition.source_revision.descriptor_content_hash
        {
            return Err(
                "MV repartition descriptor drifted from its ready accelerator projection"
                    .to_string(),
            );
        }
        let mut descriptor = package.descriptor;
        descriptor.schema_contract.target.partition = Some(
            mv_partition_contract_from_committed_partitioning(&transition.preview)?,
        );
        publication_intent = publication_intent.with_partition_spec_replacement(
            transition.replacement.clone(),
            transition.preview.clone(),
            managed_descriptor_properties_from_descriptor(&descriptor)?,
        );
    }
    let schema_contract = definition.schema_contract.as_ref().ok_or_else(|| {
        "Iceberg MV first-refresh requires a persisted schema contract".to_string()
    })?;
    let capabilities = RefreshCapabilities::from_schema_contract(schema_contract)?;
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
    let target_arrow_schema = target_binding.physical_write_schema()?.as_ref().clone();
    let target_write_fields = Arc::<[arrow::datatypes::Field]>::from(
        target_arrow_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    let target_field_ids = target_binding.observation().field_ids().to_vec();
    let observed_spec_id = target_binding.partition().target_spec_id;
    let partition_spec_id = schema_contract
        .target
        .partition
        .as_ref()
        .map(|partition| partition.target_spec_id)
        .unwrap_or(observed_spec_id);
    if observed_spec_id != partition_spec_id {
        return Err(
            "MV first-refresh target partition spec drifted from its persisted contract"
                .to_string(),
        );
    }
    let target_contract =
        novarocks_sql::planning::mv::first_refresh::MvFirstRefreshTargetContract::try_new(
            Arc::new(target_arrow_schema),
            target_field_ids,
            partition_spec_id,
            schema_contract.target.hidden_apply_key.column_name.clone(),
        )?;
    let pin = RefreshSnapshotPin::from_captured_entries(
        contract
            .base_refs
            .iter()
            .map(|base| {
                let snapshot_id = contract
                    .snapshot_pins
                    .get(&base.fqn())
                    .and_then(|snapshot| *snapshot)
                    .ok_or_else(|| {
                        format!("MV first-refresh has no pinned snapshot for {}", base.fqn())
                    })?;
                let observed = observe_current_refresh_base(
                    source.connector_control(),
                    source.storage_observation(),
                    base,
                    &connector_context,
                )?;
                let object_id = observed.object_id().clone();
                let expected_object_id =
                    base_table_object_ids.get(&base.fqn()).ok_or_else(|| {
                        format!("MV first-refresh has no object-ID fact for {}", base.fqn())
                    })?;
                if &object_id != expected_object_id {
                    return Err(format!(
                        "MV first-refresh base table identity changed after planning for {}",
                        base.fqn()
                    ));
                }
                Ok::<_, String>((base.clone(), snapshot_id, object_id))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&refresh_query_source)?,
        Some(
            definition
                .query_definition
                .resolution
                .default_catalog
                .as_str(),
        ),
        &definition.query_definition.resolution.default_database,
    );
    // The persisted source definition owns name resolution. The execution
    // artifact is transient and may be canonicalized, but it must never fall
    // back to the session which happens to issue REFRESH.
    let source_catalog = Some(
        definition
            .query_definition
            .resolution
            .default_catalog
            .clone(),
    );
    let source_database = definition
        .query_definition
        .resolution
        .default_database
        .clone();
    let expected_target_snapshot_id = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
        RefreshStateBaseline::Pinless => None,
    };
    if schema_contract.join.is_some() && !capabilities.has_agg_state {
        let RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids,
            previous_table_object_ids,
            target_table_uuid,
            ..
        } = &contract.state_baseline
        else {
            return Err("MV first-refresh join requires a snapshot-backed baseline".to_string());
        };
        let rewrite = build_neutral_refresh_rewrite_context(
            source.connector_control(),
            source.storage_observation(),
            &target,
            definition.mv_id,
            current_catalog,
            current_database,
            Arc::new(definition.clone()),
            Arc::new(query.clone()),
            Arc::from(contract.base_refs.clone()),
            Arc::new(pin.clone()),
            previous_snapshot_ids.clone(),
            previous_table_object_ids.clone(),
            expected_target_snapshot_id,
            target_table_uuid.clone(),
            retained_repartition_target.map(|retained| &retained.binding),
            &connector_context,
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source.connector_control(),
            &connector_context,
            &rewrite.base_refs,
            &rewrite.pin,
            &rewrite.previous_snapshot_ids,
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
        return Ok(if repartition_transition.is_some() {
            prepared.into_full_overwrite()
        } else {
            prepared
        });
    }
    let sql_pin = novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPin::try_from_maps(
        pin.to_snapshot_map(),
        pin.to_table_object_id_map(),
    )?;
    let shape = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if schema_contract.branch.is_some() {
            crate::mv::domain::rewrite::context::first_union_branch_query(&query)?
        } else {
            query.clone()
        };
        let calls = extract_aggregate_sql_calls(&aggregate_query)?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout_sql = if schema_contract.branch.is_some() {
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
        if let Some(branch) = &schema_contract.branch {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::BranchUnionAggregate {
                branch_count: branch.branch_count as usize,
                calls,
            }
        } else if !schema_contract.bases.is_empty() {
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
    } else if let Some(branch) = &schema_contract.branch {
        novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::UnionProjection {
            branch_count: branch.branch_count as usize,
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
    Ok(if repartition_transition.is_some() {
        prepared.into_full_overwrite()
    } else {
        prepared
    })
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
                backend_name: "iceberg",
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
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    definition: &crate::mv::domain::persistence::definition::StoredMvDefinition,
    select_sql: &str,
    base_table_object_ids: &BTreeMap<String, ConnectorTableObjectId>,
) -> Result<MvRefreshPublicationIntent, String> {
    let snapshots = contract
        .snapshot_pins
        .iter()
        .map(|(base, snapshot)| {
            snapshot
                .map(|snapshot| (base.clone(), snapshot))
                .ok_or_else(|| format!("MV staging provenance has no pinned snapshot for {base}"))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let previous_snapshots = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids,
            ..
        } => previous_snapshot_ids.clone(),
        RefreshStateBaseline::Pinless => BTreeMap::new(),
    };
    mv_refresh_publication_intent(
        attempt.publication_id,
        definition.source_revision.target_object_id.clone(),
        expected_target_snapshot(contract),
        managed_descriptor_properties(&definition)?,
        MvRefreshPublicationTechnique::Full,
        &snapshots,
        base_table_object_ids,
        &previous_snapshots,
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

fn metadata_only_publication_intent(
    source: &IcebergMvCorePorts,
    contract: &RefreshPlanContract,
    attempt: &MvRefreshAttemptIdentity,
    base_table_object_ids: &BTreeMap<String, ConnectorTableObjectId>,
) -> Result<MvRefreshPublicationIntent, String> {
    let snapshots = contract
        .snapshot_pins
        .iter()
        .map(|(base, snapshot)| {
            snapshot
                .map(|snapshot| (base.clone(), snapshot))
                .ok_or_else(|| {
                    format!("MV metadata-only provenance has no pinned snapshot for {base}")
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let RefreshStateBaseline::SnapshotBacked {
        previous_snapshot_ids,
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
    let definition = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    mv_refresh_publication_intent(
        attempt.publication_id,
        definition.source_revision.target_object_id.clone(),
        expected_target_snapshot(contract),
        managed_descriptor_properties(&definition)?,
        MvRefreshPublicationTechnique::MetadataOnly,
        &snapshots,
        base_table_object_ids,
        previous_snapshot_ids,
        definition_fingerprint.clone(),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV metadata-only target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn mv_refresh_publication_intent(
    publication_id: novarocks_spi::connector::LakePublicationId,
    target_object_id: ConnectorTableObjectId,
    expected_target_snapshot_id: Option<i64>,
    descriptor_properties: ConnectorManagedDescriptorProperties,
    technique: MvRefreshPublicationTechnique,
    snapshots: &BTreeMap<String, i64>,
    base_table_object_ids: &BTreeMap<String, ConnectorTableObjectId>,
    previous_snapshots: &BTreeMap<String, i64>,
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
) -> Result<MvRefreshPublicationIntent, String> {
    let bases = snapshots
        .iter()
        .map(|(table_fqn, to_snapshot)| {
            MvRefreshPublicationBase::try_new(
                table_fqn.clone(),
                base_table_object_ids
                    .get(table_fqn)
                    .cloned()
                    .ok_or_else(|| {
                        format!("MV refresh publication has no object-ID fact for {table_fqn}")
                    })?,
                previous_snapshots.get(table_fqn).copied(),
                *to_snapshot,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    MvRefreshPublicationIntent::try_new(
        publication_id,
        target_object_id,
        expected_target_snapshot_id,
        descriptor_properties,
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

fn managed_descriptor_properties(
    definition: &crate::mv::domain::persistence::definition::StoredMvDefinition,
) -> Result<ConnectorManagedDescriptorProperties, String> {
    use crate::mv::domain::persistence::descriptor::MV_DESCRIPTOR_HASH_PROP;

    ConnectorManagedDescriptorProperties::try_new(vec![(
        Arc::from(MV_DESCRIPTOR_HASH_PROP),
        Arc::from(definition.source_revision.descriptor_content_hash.as_str()),
    )])
    .map_err(|error| format!("build managed MV descriptor properties: {error}"))
}

fn managed_descriptor_properties_from_descriptor(
    descriptor: &crate::mv::domain::persistence::descriptor::MvDescriptorV3,
) -> Result<ConnectorManagedDescriptorProperties, String> {
    let mut entries = descriptor.to_storage_properties()?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    ConnectorManagedDescriptorProperties::try_new(
        entries
            .into_iter()
            .map(|(key, value)| (Arc::from(key), Arc::from(value)))
            .collect(),
    )
    .map_err(|error| format!("build complete managed MV descriptor properties: {error}"))
}

fn mv_partition_contract_from_committed_partitioning(
    partitioning: &ConnectorCommittedPartitioning,
) -> Result<MvPartitionContract, String> {
    partitioning.validate().map_err(|error| error.to_string())?;
    let fields = partitioning
        .fields()
        .iter()
        .map(|field| {
            Ok(MvPartitionFieldContract {
                partition_field_id: field.partition_field_id(),
                partition_field_name: field.partition_field_name().to_string(),
                source_target_field_id: field.source_field_id(),
                source_column_name: field.source_column_name().to_string(),
                transform: match field.transform() {
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Identity => {
                        MvPartitionTransformContract::Identity
                    }
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Year => {
                        MvPartitionTransformContract::Year
                    }
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Month => {
                        MvPartitionTransformContract::Month
                    }
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Day => {
                        MvPartitionTransformContract::Day
                    }
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Hour => {
                        MvPartitionTransformContract::Hour
                    }
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Bucket {
                        buckets,
                    } => MvPartitionTransformContract::Bucket {
                        num_buckets: buckets,
                    },
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Truncate {
                        width,
                    } => MvPartitionTransformContract::Truncate { width },
                    novarocks_spi::connector::ConnectorManagedPartitionTransform::Void => {
                        MvPartitionTransformContract::Void
                    }
                },
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(MvPartitionContract {
        target_spec_id: partitioning.spec_id(),
        fields,
    })
}

/// Prepare the value-only non-join incremental handoff.  This is deliberately
/// limited to change-stream shapes that already have one generic native
/// writer contract.  Join branches and policy-driven full rebuilds retain
/// their explicit preparation boundary until their distinct physical artifacts
/// are extracted; treating either as an append would be incorrect.
#[allow(clippy::too_many_arguments)]
enum PreparedIncrementalRefreshWork {
    MetadataOnly,
    ChangeStream(PreparedMvIncrementalWrite),
    FullRebuild(PreparedMvFirstRefreshWrite),
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
    let definition = load_iceberg_mv_definition_by_target(source.readiness().as_ref(), &target)?;
    let schema_contract = definition.schema_contract.as_ref().ok_or_else(|| {
        "Iceberg MV incremental refresh requires a persisted schema contract".to_string()
    })?;
    let is_join = schema_contract.join.is_some();
    let is_aggregate = schema_contract.aggregate.is_some();
    let is_branch_union = schema_contract.branch.is_some();
    let join_bases = if is_join {
        let (left, right) =
            join_base_refs_for_schema_contract(schema_contract, &contract.base_refs)?;
        Some((left.clone(), right.clone()))
    } else {
        None
    };
    let RefreshStateBaseline::SnapshotBacked {
        previous_snapshot_ids,
        previous_table_object_ids,
        target_snapshot_id,
        target_table_uuid,
        definition_fingerprint,
    } = &contract.state_baseline
    else {
        return Err(
            "MV incremental refresh requires a snapshot-backed target baseline".to_string(),
        );
    };
    let pin = RefreshSnapshotPin::from_captured_entries(
        contract
            .base_refs
            .iter()
            .map(|base| {
                let snapshot_id = contract
                    .snapshot_pins
                    .get(&base.fqn())
                    .and_then(|snapshot| *snapshot)
                    .ok_or_else(|| {
                        format!(
                            "MV incremental refresh has no pinned snapshot for {}",
                            base.fqn()
                        )
                    })?;
                let observed = observe_schema_validation_for_table(
                    source.connector_control(),
                    source.storage_observation(),
                    base,
                    &connector_context,
                )?;
                let object_id = observed.table_object_id().cloned().ok_or_else(|| {
                    format!(
                        "MV incremental refresh observation has no captured table object ID for {}",
                        base.fqn()
                    )
                })?;
                Ok::<_, String>((base.clone(), snapshot_id, object_id))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let (definition, refresh_query_source) = rebind_mv_definition_before_refresh_derivation(
        source.connector_control(),
        source.storage_observation(),
        &definition,
        &contract.base_refs,
        &target,
        None,
        &connector_context,
    )?;
    let canonical_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&refresh_query_source)?,
        Some(
            definition
                .query_definition
                .resolution
                .default_catalog
                .as_str(),
        ),
        &definition.query_definition.resolution.default_database,
    );
    let rewrite = build_neutral_refresh_rewrite_context(
        source.connector_control(),
        source.storage_observation(),
        &target,
        definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(definition),
        Arc::new(canonical_query),
        Arc::from(contract.base_refs.clone()),
        Arc::new(pin),
        previous_snapshot_ids.clone(),
        previous_table_object_ids.clone(),
        *target_snapshot_id,
        target_table_uuid.clone(),
        None,
        &connector_context,
    )?;

    if let Some((left_ref, right_ref)) = join_bases {
        let left_from = rewrite
            .previous_snapshot_ids
            .get(&left_ref.fqn())
            .copied()
            .ok_or_else(|| {
                format!(
                    "MV join incremental refresh is missing previous snapshot for {}",
                    left_ref.fqn()
                )
            })?;
        let right_from = rewrite
            .previous_snapshot_ids
            .get(&right_ref.fqn())
            .copied()
            .ok_or_else(|| {
                format!(
                    "MV join incremental refresh is missing previous snapshot for {}",
                    right_ref.fqn()
                )
            })?;
        let left_to = rewrite.pin.get(&left_ref).ok_or_else(|| {
            format!(
                "MV join incremental refresh is missing pinned snapshot for {}",
                left_ref.fqn()
            )
        })?;
        let right_to = rewrite.pin.get(&right_ref).ok_or_else(|| {
            format!(
                "MV join incremental refresh is missing pinned snapshot for {}",
                right_ref.fqn()
            )
        })?;
        let (left_admission, _) = observe_and_admit_change_window_for_table(
            source.connector_control(),
            source.storage_observation(),
            &left_ref,
            left_from,
            left_to,
            &connector_context,
        )?;
        let (right_admission, _) = observe_and_admit_change_window_for_table(
            source.connector_control(),
            source.storage_observation(),
            &right_ref,
            right_from,
            right_to,
            &connector_context,
        )?;
        let left_facts = admitted_change_facts(&left_admission);
        let right_facts = admitted_change_facts(&right_admission);
        let mut full_rebuild_reasons = Vec::new();
        if let Err(reason) = &left_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", left_ref.fqn()));
        }
        if let Err(reason) = &right_facts {
            full_rebuild_reasons.push(format!("{}: {reason}", right_ref.fqn()));
        }
        if !full_rebuild_reasons.is_empty() {
            tracing::info!(
                target = %rewrite.target.fqn(),
                reasons = %full_rebuild_reasons.join("; "),
                "MV join refresh admission selected a distributed full-rebuild staging overwrite"
            );
            let rebuild = prepare_frontend_first_refresh_write(
                source,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pin.to_table_object_id_map(),
                observed_binding,
                None,
                None,
                connector_context,
            )?
            .into_full_overwrite();
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(rebuild));
        }
        let left_facts = left_facts.expect("full-rebuild admission returned above");
        let right_facts = right_facts.expect("full-rebuild admission returned above");
        let branches = crate::mv::domain::iceberg_join_branch::plan_join_delta_branches(
            &left_ref,
            &right_ref,
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
        )?;
        let publication_intent = mv_refresh_publication_intent(
            attempt.publication_id,
            rewrite
                .mv_definition
                .source_revision
                .target_object_id
                .clone(),
            *target_snapshot_id,
            managed_descriptor_properties(rewrite.mv_definition.as_ref())?,
            MvRefreshPublicationTechnique::Incremental,
            &rewrite.pin.to_snapshot_map(),
            &rewrite.pin.to_table_object_id_map(),
            &rewrite.previous_snapshot_ids,
            definition_fingerprint.clone(),
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source.connector_control(),
            &connector_context,
            &rewrite.base_refs,
            &rewrite.pin,
            &rewrite.previous_snapshot_ids,
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
        .map(PreparedIncrementalRefreshWork::ChangeStream);
    }

    let loaded_bases = rewrite
        .base_refs
        .iter()
        .map(|base| {
            let previous_snapshot_id = rewrite
                .previous_snapshot_ids
                .get(&base.fqn())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "MV incremental refresh is missing previous snapshot for {}",
                        base.fqn()
                    )
                })?;
            let current_snapshot_id = rewrite.pin.get(base).ok_or_else(|| {
                format!(
                    "MV incremental refresh is missing pinned snapshot for {}",
                    base.fqn()
                )
            })?;
            let current_table_object_id = rewrite.pin.object_id(base).ok_or_else(|| {
                format!(
                    "MV incremental refresh is missing pinned object ID for {}",
                    base.fqn()
                )
            })?;
            let observed = observe_schema_validation_for_table(
                source.connector_control(),
                source.storage_observation(),
                base,
                &connector_context,
            )?;
            if observed.table_object_id() != Some(current_table_object_id) {
                return Err(format!(
                    "MV incremental refresh base table identity changed after planning for {}",
                    base.fqn()
                ));
            }
            let (admission, _) = observe_and_admit_change_window_for_table(
                source.connector_control(),
                source.storage_observation(),
                base,
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
                    base_ref,
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
            let rebuild = prepare_frontend_first_refresh_write(
                source,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pin.to_table_object_id_map(),
                observed_binding,
                None,
                None,
                connector_context,
            )?
            .into_full_overwrite();
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(rebuild));
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
    )?;
    let publication_intent = mv_refresh_publication_intent(
        attempt.publication_id,
        rewrite
            .mv_definition
            .source_revision
            .target_object_id
            .clone(),
        *target_snapshot_id,
        managed_descriptor_properties(rewrite.mv_definition.as_ref())?,
        MvRefreshPublicationTechnique::Incremental,
        &rewrite.pin.to_snapshot_map(),
        &rewrite.pin.to_table_object_id_map(),
        &rewrite.previous_snapshot_ids,
        definition_fingerprint.clone(),
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
    )?;
    let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        source.connector_control(),
        &connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
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
    .map(PreparedIncrementalRefreshWork::ChangeStream)
}
