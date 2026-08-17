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

//! Projection/filter materialized views backed by Iceberg target tables in the
//! current Iceberg catalog. Aggregate shapes are accepted at CREATE time for
//! target schema and contract persistence; refresh execution is gated later.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::catalog_application::CatalogApplicationPort;
use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::common::engine_error::EngineError;
use crate::mv::aggregate_state::physical_column::validate_unique_aggregate_physical_column_names;
use crate::mv::analysis::rebind::rewrite_select_sql_for_rebind;
use crate::mv::analysis::refresh_property::{
    RefreshFragmentProperty, TargetIdentity, derive_fragment_property, derive_imv_refresh_contract,
};
use crate::mv::analysis::{
    MvAnalysis, canonicalize_iceberg_mv_select_query, output_column_to_table_column,
    resolve_mv_name, validate_mv_partition_columns,
};
use crate::mv::analysis_adapter::{
    BaseColumnDescriptor, BaseTableDescriptor, now_ms, validate_ivm_primary_key,
};
use crate::mv::application::{
    CreatedMvTarget, MvCreateStatement, MvEngine, MvEngineError, MvEngineErrorKind,
    MvRefreshPreparationRequest, MvRefreshPreparationService, MvRefreshPublicationBase,
    MvRefreshPublicationIntent, MvRefreshPublicationTechnique, PrepareMvCreateRequest,
    PreparedMvCreate, PreparedMvDefinition, PreparedMvRefresh, PreparedMvRefreshWork,
    PreparedMvRefreshWrite,
};
use crate::mv::dependency::model::{MvDependencyObjectType, MvDependencyStorageEngine};
use crate::mv::lifecycle::{BackendRefreshPlan, IcebergRefreshPlan, RefreshError, RefreshPlan};
use crate::mv::model::{MvStorageEngine, MvTarget, RefreshMode};
use crate::mv::persistence::definition::CreateMvDefinitionRequest;
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::descriptor::{
    DescriptorDependency, MV_DESCRIPTOR_VERSION, MvDescriptorV1,
};
use crate::mv::persistence::schema as mv_schema;
use crate::mv::persistence::schema::{
    APPLY_KEY_COLUMN_PROPERTY, APPLY_KEY_FIELD_ID_PROPERTY, APPLY_KEY_SOURCE_PROPERTY,
    HIDDEN_COLUMNS_PROPERTY,
};
use crate::mv::refresh::apply_key::ApplyKeyContract;
use crate::mv::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::refresh::contract::ImvRefreshContract;
#[cfg(test)]
use crate::mv::refresh::execution_policy::should_use_join_delta_append_only_fast_path;
use crate::mv::refresh::execution_policy::{
    explain_refresh_full_guard, non_join_incremental_write_mode,
    select_join_incremental_execution_mode,
};
use crate::mv::refresh::non_join_incremental::{
    NonJoinBaseChange, NonJoinIncrementalChangePlan, full_rebuild_reason_message,
    plan_non_join_incremental_changes,
};
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::mv::refresh::planning::{
    RefreshPlanContract, RefreshPlanningInput, RefreshStateBaseline, decide_refresh_plan,
};
#[cfg(test)]
use crate::mv::refresh::repartition::RepartitionShape;
use crate::mv::refresh::repartition::select_repartition_shape;
use crate::mv::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
};
use crate::mv::refresh::target::{
    IcebergMvTarget, resolve_refresh_target, validate_target_snapshot,
};
use crate::mv::refresh::target_apply::{
    apply_key_table_column, branch_id_table_column, ensure_base_row_lineage_contract,
    join_apply_key_table_column,
};
use crate::mv::refresh_io::{acquire_mv_refresh_lock, parse_iceberg_table_refs};
use crate::mv::refresh_pin_adapter::capture_refresh_snapshot_pin_with_ports;
use crate::mv::repository::CreateMvRepositoryRequest;
use crate::mv::repository::MvRepository;
use crate::mv::schema_validation::{
    BranchFieldValidationError, ContractDecision, JoinContractDecision, validate_branch_id_field,
};
use crate::mv::schema_validation::{validate_join_schema_contract, validate_schema_contract};
use crate::mv::storage_observation::MvSchemaValidationObservation;
use crate::mv::storage_observation::{MvStorageObservationPort, MvTargetCreationObservation};
use crate::query_execution::StatementResult;
use crate::query_execution::planning::bindings::{
    MvTargetReadAdmission, QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey,
    QueryTableBindingStore,
};
use mv_schema::MvPartitionContract;
use novarocks_catalog::identifier::{TableIdentity, normalize_identifier};
use novarocks_spi::connector::{
    ConnectorChangeWindowAdmission, ConnectorControlRegistry, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorTableIdentity,
};
use novarocks_sql::planning::mv::UnionBranchKind;
use novarocks_sql::planning::mv::{FULL_REFRESH_DISABLED_MESSAGE, MvRefreshFinalizeFacts};
use novarocks_sql::planning::mv::{
    MV_BRANCH_ID_COLUMN_NAME as BRANCH_ID_COLUMN_NAME,
    MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
    MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME,
    MV_JOIN_APPLY_KEY_COLUMN_NAME as JOIN_APPLY_KEY_COLUMN_NAME, SqlMvAggregateCalls,
    SqlMvAggregateLayoutScope, SqlMvApplyKeySourceFacts, SqlMvExpressionLineageKind,
    SqlMvJoinAliases, SqlMvJoinContractKindFacts, SqlMvLineageScope, SqlMvObservedFieldFacts,
    SqlMvObservedSchemaFacts, SqlMvOutputColumnFacts, SqlMvPersistedApplyKeySourceFacts,
    extract_aggregate_sql_calls, extract_join_aliases, extract_single_scan_table_fqn,
    mv_apply_key_source_from_column_name,
};
use novarocks_sql::syntax::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, IcebergPartitionFieldExpr,
    MaterializedViewRefreshPolicy, ObjectName, RefreshMaterializedViewStmt, TableColumnDef,
};

/// The explicit Core ports a refresh preparation may read while deriving its
/// immutable write artifact.  This deliberately names the individual
/// dependencies instead of admitting the aggregate application state into a
/// frontend-owned preparation path.
trait IcebergMvRefreshSource:
    crate::catalog_application::query_catalog::CatalogServiceSource + Send + Sync
{
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort>;
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
    fn repository(&self) -> &dyn MvRepository;
    fn storage_observation(&self) -> &dyn MvStorageObservationPort;
}

/// SQL-owned bridge for refresh planning.  It owns only analysis and immutable
/// facts; all durable intent, ref mutations, and writer execution are handed
/// to the frontend as `PreparedMvRefresh`.
pub(crate) struct StandaloneMvRefreshPreparationService<'a> {
    source: &'a dyn IcebergMvRefreshSource,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    statement: &'a RefreshMaterializedViewStmt,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    repartition_fields: Option<&'a [IcebergPartitionFieldExpr]>,
}

impl<'a> StandaloneMvRefreshPreparationService<'a> {
    pub(crate) fn new_with_ports(
        ports: &'a IcebergMvCorePorts,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a RefreshMaterializedViewStmt,
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

    pub(crate) fn new_repartition_with_ports(
        ports: &'a IcebergMvCorePorts,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a RefreshMaterializedViewStmt,
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

impl MvRefreshPreparationService for StandaloneMvRefreshPreparationService<'_> {
    fn prepare_step(
        &self,
        request: MvRefreshPreparationRequest,
    ) -> Result<PreparedMvRefresh, String> {
        request.validate()?;
        if request.statement
            != novarocks_sql::planning::mv::MvRefreshStatement::from(self.statement)
        {
            return Err(
                "MV refresh preparation statement does not match the admitted SQL request"
                    .to_string(),
            );
        }
        let mut plan = plan_iceberg_mv_refresh_with_connector_context(
            self.source,
            self.current_catalog,
            self.current_database,
            self.statement,
            request.target.clone(),
            self.connector_context,
        )
        .map_err(|error| error.to_string())?;
        let retained_repartition_target = self
            .repartition_fields
            .map(|_| {
                retain_exact_repartition_target(self.source, &plan.contract, self.connector_context)
            })
            .transpose()?;
        let partition_spec_replacement = match self.repartition_fields {
            Some(fields) => {
                plan.contract.decision = ExecutableRefreshDecision::FirstRefresh;
                Some(prepare_managed_repartition_transition(
                    self.source,
                    self.current_catalog,
                    self.current_database,
                    fields,
                    &plan.contract,
                    request.attempt.write_operation_id,
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
        let base_table_uuids = plan
            .contract
            .base_refs
            .iter()
            .map(|base| {
                observe_schema_validation_for_table(self.source, base, self.connector_context)
                    .map(|observed| (base.fqn(), observed.table_uuid().to_string()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let expected_target_snapshot_id = match &plan.contract.state_baseline {
            RefreshStateBaseline::SnapshotBacked {
                target_snapshot_id, ..
            } => *target_snapshot_id,
            RefreshStateBaseline::Pinless => None,
        };
        let work = match plan.contract.decision {
            ExecutableRefreshDecision::SkipEmpty => PreparedMvRefreshWork::NoOp,
            ExecutableRefreshDecision::MetadataOnly => PreparedMvRefreshWork::MetadataOnly,
            ExecutableRefreshDecision::FirstRefresh => PreparedMvRefreshWork::DataProducing {
                write: PreparedMvRefreshWrite::FirstRefresh(prepare_frontend_first_refresh_write(
                    self.source,
                    self.current_catalog,
                    self.current_database,
                    &plan.contract,
                    &request.attempt,
                    &base_table_uuids,
                    observed_binding.clone(),
                    partition_spec_replacement.clone(),
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
                        write: PreparedMvRefreshWrite::Incremental(incremental),
                    }
                }
                PreparedIncrementalRefreshWork::FullRebuild(rebuild) => {
                    PreparedMvRefreshWork::DataProducing {
                        write: PreparedMvRefreshWrite::FirstRefresh(rebuild),
                    }
                }
                PreparedIncrementalRefreshWork::MetadataOnly => PreparedMvRefreshWork::MetadataOnly,
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
                base_table_uuids,
                expected_target_snapshot_id,
            },
            work,
        })
    }
}

/// One repartition target observation retained across transition and write
/// preparation. Both payloads were produced from the same exact lease and
/// metadata value; no downstream repartition step may resolve `latest` again.
struct RetainedRepartitionTarget {
    binding: crate::mv::refresh::target_binding::MvTargetBinding,
    schema_validation: MvSchemaValidationObservation,
}

fn retain_exact_repartition_target(
    source: &dyn IcebergMvRefreshSource,
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
    let binding = target_binding_for(source, &target, connector_context)?;
    validate_retained_target_identity(&target, binding.identity())?;
    let schema_validation = source
        .storage_observation()
        .observe_schema_validation(
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

fn validate_retained_target_identity(
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
) -> ConnectorExecutionBindingKey {
    ConnectorExecutionBindingKey {
        instance_id: lease.binding().descriptor().instance_id.clone(),
        incarnation: lease.binding().incarnation(),
    }
}

fn prepare_managed_repartition_transition(
    source: &dyn IcebergMvRefreshSource,
    current_catalog: Option<&str>,
    current_database: &str,
    fields: &[IcebergPartitionFieldExpr],
    contract: &RefreshPlanContract,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
    retained_target: &RetainedRepartitionTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement, String> {
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
    let definition = load_iceberg_mv_definition_by_target(source, &target)?;
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
        source,
        schema_contract,
        &contract.base_refs,
        &retained_target.schema_validation,
        connector_context,
    )?;
    let query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let provider = crate::query_execution::compiler::build_catalog_service_provider(
        current_catalog,
        source.catalog_service().as_ref(),
        source.connector_control(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        source.catalog_application(),
    );
    let analysis = crate::mv::analysis_adapter::analyze_mv_select_with_provider(
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
    novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement::try_new(
        operation_id,
        expected_prior,
        replacement_fields,
    )
    .map_err(|error| error.to_string())
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
    transform: &crate::mv::storage_observation::MvSchemaValidationPartitionTransform,
) -> Result<novarocks_spi::connector::ConnectorManagedPartitionTransform, String> {
    use crate::mv::storage_observation::MvSchemaValidationPartitionTransform as Observed;
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
    source: &dyn IcebergMvRefreshSource,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &crate::mv::application::MvRefreshAttemptIdentity,
    base_table_uuids: &BTreeMap<String, String>,
    observed_binding: ConnectorExecutionBindingKey,
    partition_spec_replacement: Option<
        novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement,
    >,
    retained_repartition_target: Option<&RetainedRepartitionTarget>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::application::PreparedMvFirstRefreshWrite, String> {
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
    if write_lease.binding_key() != &observed_binding {
        return Err(
            "MV first-refresh target connector generation changed during admission".to_string(),
        );
    }
    let definition = load_iceberg_mv_definition_by_target(source, &target)?;
    let definition = rebind_mv_definition_before_refresh_derivation(
        source,
        &definition,
        &contract.base_refs,
        &target,
        retained_repartition_target.map(|retained| &retained.schema_validation),
        &connector_context,
    )
    .map_err(IcebergMvRefreshExecutionError::into_message)?;
    let mut publication_intent = frontend_refresh_publication_intent(
        contract,
        attempt,
        definition.mv_id,
        &definition.select_sql,
        base_table_uuids,
    )?;
    if let Some(replacement) = partition_spec_replacement.clone() {
        publication_intent = publication_intent.with_partition_spec_replacement(replacement);
    }
    let schema_contract = definition.schema_contract.as_ref().ok_or_else(|| {
        "Iceberg MV first-refresh requires a persisted schema contract".to_string()
    })?;
    let capabilities = RefreshCapabilities::from_schema_contract(schema_contract)?;
    let loaded_target_binding;
    let target_binding = match retained_repartition_target {
        Some(retained) => &retained.binding,
        None => {
            loaded_target_binding = target_binding_for(source, &target, &connector_context)?;
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
                let observed =
                    observe_current_refresh_base_with_source(source, base, &connector_context)?;
                let uuid = observed.table_uuid().to_string();
                let expected_uuid = base_table_uuids.get(&base.fqn()).ok_or_else(|| {
                    format!("MV first-refresh has no UUID fact for {}", base.fqn())
                })?;
                if &uuid != expected_uuid {
                    return Err(format!(
                        "MV first-refresh base table identity changed after planning for {}",
                        base.fqn()
                    ));
                }
                Ok::<_, String>((base.clone(), snapshot_id, uuid))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let expected_target_snapshot_id = match &contract.state_baseline {
        RefreshStateBaseline::SnapshotBacked {
            target_snapshot_id, ..
        } => *target_snapshot_id,
        RefreshStateBaseline::Pinless => None,
    };
    if schema_contract.join.is_some() && !capabilities.has_agg_state {
        let RefreshStateBaseline::SnapshotBacked {
            previous_snapshot_ids,
            previous_table_uuids,
            target_table_uuid,
            ..
        } = &contract.state_baseline
        else {
            return Err("MV first-refresh join requires a snapshot-backed baseline".to_string());
        };
        let rewrite = build_neutral_refresh_rewrite_context(
            source,
            &target,
            definition.mv_id,
            current_catalog,
            current_database,
            Arc::new(definition.clone()),
            Arc::new(query.clone()),
            Arc::from(contract.base_refs.clone()),
            Arc::new(pin.clone()),
            previous_snapshot_ids.clone(),
            previous_table_uuids.clone(),
            expected_target_snapshot_id,
            target_table_uuid.clone(),
            retained_repartition_target.map(|retained| &retained.binding),
            &connector_context,
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source,
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
        let request = crate::mv::application::MvFirstRefreshWriteRequest::try_new(
            target.catalog,
            target.namespace,
            target.table,
            attempt.staging_branch.clone(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            expected_target_snapshot_id,
            table,
            Arc::clone(&target_write_fields),
            observed_binding,
            attempt.write_operation_id,
        )?;
        let prepared = crate::mv::application::MvFirstRefreshWritePreparer::prepare_join_logical(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            publication_intent,
        )?;
        return Ok(if partition_spec_replacement.is_some() {
            prepared.into_full_overwrite()
        } else {
            prepared
        });
    }
    let sql_pin = novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPin::try_from_maps(
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )?;
    let shape = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if schema_contract.branch.is_some() {
            crate::mv::rewrite::context::first_union_branch_query(&query)?
        } else {
            query.clone()
        };
        let calls = extract_aggregate_sql_calls(&aggregate_query)?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout_sql = if schema_contract.branch.is_some() {
            aggregate_query.to_string()
        } else {
            definition.select_sql.clone()
        };
        let aggregate_layout = build_aggregate_layout_for_refresh_select_sql(
            source,
            current_catalog,
            current_database,
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
                aggregate_input_types: aggregate_layout.aggregate_input_types,
            }
        } else {
            novarocks_sql::planning::mv::first_refresh::SqlMvFirstRefreshArtifactShape::Aggregate {
                calls,
                aggregate_input_types: aggregate_layout.aggregate_input_types,
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
            definition.select_sql.clone(),
            sql_pin,
            current_catalog.map(str::to_string),
            current_database.to_string(),
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
    let request = crate::mv::application::MvFirstRefreshWriteRequest::try_new(
        target.catalog,
        target.namespace,
        target.table,
        attempt.staging_branch.clone(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        expected_target_snapshot_id,
        table,
        target_write_fields,
        observed_binding,
        attempt.write_operation_id,
    )?;
    let prepared = crate::mv::application::MvFirstRefreshWritePreparer::prepare(
        request,
        physical_sql,
        publication_intent,
    )?;
    Ok(if partition_spec_replacement.is_some() {
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

fn select_retained_target_handle(
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
    attempt: &crate::mv::application::MvRefreshAttemptIdentity,
    mv_id: i64,
    select_sql: &str,
    base_table_uuids: &BTreeMap<String, String>,
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
        attempt.refresh_id,
        mv_id,
        attempt.marker_token.clone(),
        MvRefreshPublicationTechnique::Full,
        &snapshots,
        base_table_uuids,
        &previous_snapshots,
        mv_definition_fingerprint(select_sql),
        contract
            .target
            .catalog
            .clone()
            .ok_or_else(|| "MV refresh publication target has no connector catalog".to_string())?,
        contract.target.database.clone(),
        contract.target.name.clone(),
        attempt.staging_branch.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn mv_refresh_publication_intent(
    refresh_id: i64,
    mv_id: i64,
    marker_token: String,
    technique: MvRefreshPublicationTechnique,
    snapshots: &BTreeMap<String, i64>,
    base_table_uuids: &BTreeMap<String, String>,
    previous_snapshots: &BTreeMap<String, i64>,
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    staging_branch: String,
) -> Result<MvRefreshPublicationIntent, String> {
    let bases = snapshots
        .iter()
        .map(|(table_fqn, to_snapshot)| {
            MvRefreshPublicationBase::try_new(
                table_fqn.clone(),
                base_table_uuids.get(table_fqn).cloned().ok_or_else(|| {
                    format!("MV refresh publication has no UUID fact for {table_fqn}")
                })?,
                previous_snapshots.get(table_fqn).copied(),
                *to_snapshot,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    MvRefreshPublicationIntent::try_new(
        refresh_id,
        mv_id,
        marker_token,
        technique,
        bases,
        definition_fingerprint,
        target_catalog,
        target_namespace,
        target_name,
        staging_branch,
    )
}

/// Prepare the value-only non-join incremental handoff.  This is deliberately
/// limited to change-stream shapes that already have one generic native
/// writer contract.  Join branches and policy-driven full rebuilds retain
/// their explicit preparation boundary until their distinct physical artifacts
/// are extracted; treating either as an append would be incorrect.
#[allow(clippy::too_many_arguments)]
enum PreparedIncrementalRefreshWork {
    MetadataOnly,
    ChangeStream(crate::mv::application::PreparedMvIncrementalWrite),
    FullRebuild(crate::mv::application::PreparedMvFirstRefreshWrite),
}

fn prepare_frontend_incremental_write(
    source: &dyn IcebergMvRefreshSource,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &crate::mv::application::MvRefreshAttemptIdentity,
    observed_binding: ConnectorExecutionBindingKey,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedIncrementalRefreshWork, String> {
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV incremental refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let definition = load_iceberg_mv_definition_by_target(source, &target)?;
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
        previous_table_uuids,
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
                let observed =
                    observe_schema_validation_for_table(source, base, &connector_context)?;
                Ok::<_, String>((base.clone(), snapshot_id, observed.table_uuid().to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let definition = rebind_mv_definition_before_refresh_derivation(
        source,
        &definition,
        &contract.base_refs,
        &target,
        None,
        &connector_context,
    )
    .map_err(IcebergMvRefreshExecutionError::into_message)?;
    let canonical_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let rewrite = build_neutral_refresh_rewrite_context(
        source,
        &target,
        definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(definition),
        Arc::new(canonical_query),
        Arc::from(contract.base_refs.clone()),
        Arc::new(pin),
        previous_snapshot_ids.clone(),
        previous_table_uuids.clone(),
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
            source,
            &left_ref,
            left_from,
            left_to,
            &connector_context,
        )?;
        let (right_admission, _) = observe_and_admit_change_window_for_table(
            source,
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
                &rewrite.pin.to_table_uuid_map(),
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
        let branches = crate::mv::iceberg_join_branch::plan_join_delta_branches(
            &left_ref,
            &right_ref,
            crate::mv::iceberg_join_branch::SnapshotWindow {
                from: left_from,
                to: left_to,
            },
            crate::mv::iceberg_join_branch::SnapshotWindow {
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
            crate::mv::application::MvIncrementalJoinMode::Coalesce
        } else {
            select_join_incremental_execution_mode(left_facts.has_deletes, right_facts.has_deletes)
        };
        let request = crate::mv::application::MvIncrementalWriteRequest::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
            attempt.staging_branch.clone(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            *target_snapshot_id,
            observed_binding,
            attempt.write_operation_id,
        )?;
        let publication_intent = mv_refresh_publication_intent(
            attempt.refresh_id,
            rewrite.mv_definition.mv_id,
            attempt.marker_token.clone(),
            MvRefreshPublicationTechnique::Incremental,
            &rewrite.pin.to_snapshot_map(),
            &rewrite.pin.to_table_uuid_map(),
            &rewrite.previous_snapshot_ids,
            definition_fingerprint.clone(),
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
            attempt.staging_branch.clone(),
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            source,
            &connector_context,
            &rewrite.base_refs,
            &rewrite.pin,
            &rewrite.previous_snapshot_ids,
        )?;
        return crate::mv::application::MvIncrementalWritePreparer::prepare(
            request,
            crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            match join_mode {
                crate::mv::application::MvIncrementalJoinMode::AppendOnly => {
                    crate::mv::application::MvIncrementalWriteMode::FastAppend
                }
                crate::mv::application::MvIncrementalJoinMode::Coalesce => {
                    crate::mv::application::MvIncrementalWriteMode::RowDelta
                }
            },
            if is_aggregate {
                crate::mv::application::MvIncrementalRewriteEvidence::JoinAggregate
            } else {
                crate::mv::application::MvIncrementalRewriteEvidence::None
            },
            crate::mv::application::MvIncrementalExecutionArtifact::JoinLogical {
                mode: match join_mode {
                    crate::mv::application::MvIncrementalJoinMode::AppendOnly => {
                        crate::mv::application::MvIncrementalJoinMode::AppendOnly
                    }
                    crate::mv::application::MvIncrementalJoinMode::Coalesce => {
                        crate::mv::application::MvIncrementalJoinMode::Coalesce
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
            let current_table_uuid = rewrite.pin.uuid(base).ok_or_else(|| {
                format!(
                    "MV incremental refresh is missing pinned UUID for {}",
                    base.fqn()
                )
            })?;
            let observed = observe_schema_validation_for_table(source, base, &connector_context)?;
            if observed.table_uuid() != current_table_uuid {
                return Err(format!(
                    "MV incremental refresh base table identity changed after planning for {}",
                    base.fqn()
                ));
            }
            let (admission, _) = observe_and_admit_change_window_for_table(
                source,
                base,
                previous_snapshot_id,
                current_snapshot_id,
                &connector_context,
            )?;
            Ok::<_, String>((
                base,
                previous_snapshot_id,
                current_snapshot_id,
                current_table_uuid.to_string(),
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
                current_table_uuid,
                admission,
            )| {
                NonJoinBaseChange {
                    base_ref,
                    previous_snapshot_id: *previous_snapshot_id,
                    current_snapshot_id: *current_snapshot_id,
                    current_table_uuid,
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
                &rewrite.pin.to_table_uuid_map(),
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
                    crate::mv::application::MvIncrementalRewriteEvidence::BranchUnionAggregate
                } else {
                    crate::mv::application::MvIncrementalRewriteEvidence::Aggregate
                }
            } else {
                crate::mv::application::MvIncrementalRewriteEvidence::None
            };
            (mode, evidence)
        }
    };
    let request = crate::mv::application::MvIncrementalWriteRequest::try_new(
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        attempt.staging_branch.clone(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        *target_snapshot_id,
        observed_binding,
        attempt.write_operation_id,
    )?;
    let publication_intent = mv_refresh_publication_intent(
        attempt.refresh_id,
        rewrite.mv_definition.mv_id,
        attempt.marker_token.clone(),
        MvRefreshPublicationTechnique::Incremental,
        &rewrite.pin.to_snapshot_map(),
        &rewrite.pin.to_table_uuid_map(),
        &rewrite.previous_snapshot_ids,
        definition_fingerprint.clone(),
        target.catalog.clone(),
        target.namespace.clone(),
        target.table.clone(),
        attempt.staging_branch.clone(),
    )?;
    let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        source,
        &connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
    )?;
    crate::mv::application::MvIncrementalWritePreparer::prepare(
        request,
        crate::query_execution::mv_assembly::first_refresh_staging::frozen_logical_context_from_rewrite(
            &rewrite,
            contract.affected_partitions.clone(),
            Some(frozen_base_overlays),
        )?,
        mode,
        evidence,
        crate::mv::application::MvIncrementalExecutionArtifact::CanonicalQuery,
        publication_intent,
    )
    .map(PreparedIncrementalRefreshWork::ChangeStream)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IcebergMvRefreshExecutionError {
    PreCommit(String),
    Commit(RefreshError),
}

impl IcebergMvRefreshExecutionError {
    fn pre_commit(message: impl Into<String>) -> Self {
        Self::PreCommit(message.into())
    }

    fn commit(error: RefreshError) -> Self {
        Self::Commit(error)
    }

    fn into_message(self) -> String {
        match self {
            Self::PreCommit(message) => message,
            Self::Commit(error) => error.message,
        }
    }

    fn into_refresh_error(self) -> RefreshError {
        match self {
            Self::PreCommit(message) => RefreshError::pre_commit(message),
            Self::Commit(error) => error,
        }
    }
}

impl From<String> for IcebergMvRefreshExecutionError {
    fn from(message: String) -> Self {
        Self::pre_commit(message)
    }
}

/// The explicit Core inputs required by an Iceberg MV CREATE operation.
///
/// This is intentionally a narrow composition value: it owns the frozen
/// catalog source plus the provider and durable MV ports that CREATE needs.
/// It does not retain aggregate application state, and it has no state-based
/// constructor so a frontend composition must name every dependency.
#[derive(Clone)]
pub struct IcebergMvCorePorts {
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    repository: Arc<dyn MvRepository>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl IcebergMvCorePorts {
    /// Construct the exact provider and durable-MV ports required by the
    /// Iceberg MV backend. Frontend composition must provide every leaf; this
    /// value deliberately has no application-facade constructor.
    pub fn new(
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        repository: Arc<dyn MvRepository>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            catalog_service,
            catalog_application,
            connector_control,
            repository,
            storage_observation,
        }
    }

    pub(crate) fn repository(&self) -> &Arc<dyn MvRepository> {
        &self.repository
    }

    pub(crate) fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    pub(crate) fn storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.storage_observation.as_ref()
    }
}

impl crate::catalog_application::query_catalog::CatalogServiceSource for IcebergMvCorePorts {
    fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }
}

impl IcebergMvRefreshSource for IcebergMvCorePorts {
    fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }

    fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    fn repository(&self) -> &dyn MvRepository {
        self.repository.as_ref()
    }

    fn storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.storage_observation.as_ref()
    }
}

/// Core adapter used by the frontend-owned MV application service. It keeps
/// explicit connector/analyzer ports in core while exposing CREATE as
/// auditable, side-effect-sized primitives.
pub(crate) struct StandaloneMvEngine {
    ports: IcebergMvCorePorts,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    preparations: Mutex<HashMap<String, Arc<IcebergMvCreatePreparation>>>,
}

struct IcebergMvCreatePreparation {
    target: IcebergMvTarget,
    canonical_select_query: sqlparser::ast::Query,
    analysis: MvAnalysis,
    refresh_contract: ImvRefreshContract,
    property: RefreshFragmentProperty,
    base_refs: Vec<TableIdentity>,
    dependencies: Vec<CreateMvDependencyRequest>,
    base_field_observations: std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    expected_apply_key_field_id: i32,
    created_at_ms: i64,
    columns: Vec<TableColumnDef>,
    partition_fields: Vec<IcebergPartitionFieldExpr>,
    target_properties: Vec<(String, String)>,
    created_target_observation: Mutex<Option<MvTargetCreationObservation>>,
}

impl StandaloneMvEngine {
    pub(crate) fn new_with_ports(
        ports: IcebergMvCorePorts,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            ports,
            connector_context,
            preparations: Mutex::new(HashMap::new()),
        }
    }

    fn preparation_key(target: &MvTarget) -> String {
        format!(
            "{}.{}.{}",
            target.catalog.as_deref().unwrap_or_default(),
            target.database,
            target.name
        )
    }

    fn preparation(
        &self,
        plan: &PreparedMvCreate,
    ) -> Result<Arc<IcebergMvCreatePreparation>, MvEngineError> {
        self.preparation_for_target(&plan.target)
    }

    fn preparation_for_target(
        &self,
        target: &MvTarget,
    ) -> Result<Arc<IcebergMvCreatePreparation>, MvEngineError> {
        self.preparations
            .lock()
            .map_err(|error| {
                MvEngineError::new(
                    MvEngineErrorKind::TargetOperation,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .get(&Self::preparation_key(target))
            .cloned()
            .ok_or_else(|| {
                MvEngineError::new(
                    MvEngineErrorKind::InvalidRequest,
                    "MV CREATE plan was not prepared by this engine",
                )
            })
    }
}

impl MvEngine for StandaloneMvEngine {
    fn prepare_create(
        &self,
        request: PrepareMvCreateRequest<'_>,
        repository: &dyn crate::mv::repository::MvRepository,
    ) -> Result<PreparedMvCreate, MvEngineError> {
        let prepared = prepare_iceberg_mv_create_with_ports(
            &self.ports,
            request.context.current_catalog,
            request.context.current_database,
            request.statement,
            repository,
            &self.connector_context,
        )
        .map_err(engine_prepare_error)?;
        let target = MvTarget {
            catalog: Some(prepared.target.catalog.clone()),
            database: prepared.target.namespace.clone(),
            name: prepared.target.table.clone(),
        };
        let repository_request = CreateMvRepositoryRequest {
            definition: CreateMvDefinitionRequest {
                select_sql: prepared.canonical_select_query.to_string(),
                base_table_refs: prepared.base_refs.iter().map(TableIdentity::fqn).collect(),
                primary_key_columns: request.statement.primary_key.clone().unwrap_or_default(),
                storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
                target_catalog: Some(prepared.target.catalog.clone()),
                target_namespace: Some(prepared.target.namespace.clone()),
                target_table: Some(prepared.target.table.clone()),
                schema_contract: None,
                partition_spec: None,
                created_at_ms: prepared.created_at_ms,
            },
            refresh: initial_refresh_configuration_for_create(&request.statement.refresh_policy),
            dependencies: prepared.dependencies.clone(),
        };
        self.preparations
            .lock()
            .map_err(|error| {
                MvEngineError::new(
                    MvEngineErrorKind::TargetOperation,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .insert(Self::preparation_key(&target), Arc::new(prepared));
        Ok(PreparedMvCreate::new(target, repository_request))
    }

    fn create_target(
        &self,
        plan: &PreparedMvCreate,
        operation_id: uuid::Uuid,
    ) -> Result<CreatedMvTarget, MvEngineError> {
        let prepared = self.preparation(plan)?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&prepared.target.catalog)
                .map_err(|error| engine_target_error(error.to_string()))?;
        let planning_lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.ports.connector_control.as_ref(),
            &instance_id,
        )
        .map_err(|error| engine_target_error(error.to_string()))?;
        let mutation_lease = planning_lease
            .derive_mutation_lease()
            .map_err(|error| engine_target_error(error.to_string()))?;
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from(prepared.target.namespace.as_str()),
            table: Arc::from(prepared.target.table.as_str()),
        };
        let created = require_known_committed_target_mutation(
            crate::connector::mutation::resolve_catalog_mutation_with_lease(
                &mutation_lease,
                novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(
                    *operation_id.as_bytes(),
                ),
                novarocks_spi::connector::ConnectorCatalogMutationOperation::CreateTable {
                    table: table.clone(),
                    columns: prepared
                        .columns
                        .iter()
                        .map(crate::catalog_application::statement::connector_column)
                        .collect::<Result<_, _>>()
                        .map_err(engine_target_error)?,
                    key: None,
                    partitioning: prepared
                        .partition_fields
                        .iter()
                        .map(crate::catalog_application::statement::connector_partition_transform)
                        .collect(),
                    properties: prepared
                        .target_properties
                        .iter()
                        .map(|(key, value)| (Arc::from(key.as_str()), Arc::from(value.as_str())))
                        .collect(),
                    policy: novarocks_spi::connector::CreatePolicy::FailIfExists,
                },
                self.connector_context.clone(),
            ),
            "materialized view target create",
        )?;
        if created.effect != novarocks_spi::connector::ExternalMutationEffect::Applied {
            return Err(engine_target_error(
                "materialized view target create unexpectedly returned NoOp".to_string(),
            ));
        }
        let bootstrap = crate::connector::mutation::resolve_catalog_mutation_with_lease(
            &mutation_lease,
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            novarocks_spi::connector::ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot {
                table: table.clone(),
                expected_current_snapshot: None,
                properties: vec![(
                    Arc::from("novarocks.mv.bootstrap"),
                    Arc::from("true"),
                )],
            },
            self.connector_context.clone(),
        );
        match bootstrap {
            crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { failure } => {
                let cleanup = require_known_committed_target_mutation(
                    crate::connector::mutation::resolve_catalog_mutation_with_lease(
                        &mutation_lease,
                        novarocks_spi::connector::ConnectorMutationOperationId::new(),
                        novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
                            table,
                            policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                            data_disposition:
                                novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
                        },
                        self.connector_context.clone(),
                    ),
                    "materialized view target bootstrap cleanup",
                );
                return Err(engine_target_error(format!(
                    "{}; target cleanup={cleanup:?}",
                    EngineError::commit_known_uncommitted(format!(
                        "materialized view target empty-snapshot bootstrap: {failure}"
                    ))
                )));
            }
            bootstrap => require_known_committed_target_mutation(
                bootstrap,
                "materialized view target empty-snapshot bootstrap",
            )?,
        };
        #[cfg(test)]
        run_after_create_target_hook();
        let loaded_target =
            match crate::connector::metadata_load_connector_table_with_planning_lease(
                &planning_lease,
                self.connector_context.clone(),
                &table.namespace,
                &table.table,
                novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            ) {
                Ok(loaded_target) => loaded_target,
                Err(error) => {
                    let cleanup = require_known_committed_target_mutation(
                    crate::connector::mutation::resolve_catalog_mutation_with_lease(
                        &mutation_lease,
                        novarocks_spi::connector::ConnectorMutationOperationId::new(),
                        novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
                            table,
                            policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                            data_disposition:
                                novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
                        },
                        self.connector_context.clone(),
                    ),
                    "materialized view target metadata-load cleanup",
                )
                .map(|_| ());
                    return Err(engine_target_error(format!(
                        "{error}; target cleanup={cleanup:?}"
                    )));
                }
            };
        let observation = match self.ports.storage_observation.observe_created_target(
            &planning_lease,
            &loaded_target,
            self.connector_context.clone(),
        ) {
            Ok(observation) => observation,
            Err(error) => {
                let cleanup = require_known_committed_target_mutation(
                    crate::connector::mutation::resolve_catalog_mutation_with_lease(
                        &mutation_lease,
                        novarocks_spi::connector::ConnectorMutationOperationId::new(),
                        novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
                            table,
                            policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                            data_disposition:
                                novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
                        },
                        self.connector_context.clone(),
                    ),
                    "materialized view target observation cleanup",
                )
                .map(|_| ());
                return Err(engine_target_error(format!(
                    "{error}; target cleanup={cleanup:?}"
                )));
            }
        };
        *prepared
            .created_target_observation
            .lock()
            .map_err(|error| {
                engine_target_error(format!("MV CREATE observation lock poisoned: {error}"))
            })? = Some(observation.clone());
        Ok(CreatedMvTarget {
            target: plan.target.clone(),
            table_uuid: observation.table_uuid,
        })
    }

    fn inspect_created_target(
        &self,
        plan: &PreparedMvCreate,
        _target: &CreatedMvTarget,
    ) -> Result<PreparedMvDefinition, MvEngineError> {
        let prepared = self.preparation(plan)?;
        let target_observation = prepared
            .created_target_observation
            .lock()
            .map_err(|error| {
                engine_target_error(format!("MV CREATE observation lock poisoned: {error}"))
            })?
            .clone()
            .ok_or_else(|| {
                engine_target_error("MV CREATE target was not observed after bootstrap".to_string())
            })?;
        if target_observation.table_uuid != _target.table_uuid {
            return Err(engine_target_error(
                "MV CREATE target UUID differs from the retained creation observation".to_string(),
            ));
        }
        let actual_apply_key_field_id = target_field_id_by_column(
            &target_observation,
            prepared.refresh_contract.apply_key.column_name,
        )
        .map_err(engine_target_error)?;
        if actual_apply_key_field_id != prepared.expected_apply_key_field_id {
            return Err(MvEngineError::new(
                MvEngineErrorKind::TargetOperation,
                format!(
                    "Iceberg MV target apply-key field id mismatch: expected {}, got {actual_apply_key_field_id}",
                    prepared.expected_apply_key_field_id
                ),
            ));
        }
        let schema_contract = build_iceberg_mv_schema_contract(
            &prepared.refresh_contract,
            &prepared.property,
            &prepared.canonical_select_query,
            &prepared.analysis,
            &prepared.base_refs,
            &prepared.base_field_observations,
            &prepared.target,
            &target_observation,
            actual_apply_key_field_id,
        )
        .map_err(engine_target_error)?;
        let mut repository_request = plan.repository_request.clone();
        repository_request.definition.schema_contract = Some(schema_contract.clone());
        repository_request.definition.partition_spec = schema_contract.target.partition.clone();
        Ok(PreparedMvDefinition { repository_request })
    }

    fn sync_target_descriptor(
        &self,
        _target: &CreatedMvTarget,
        definition: &StoredMvDefinition,
    ) -> Result<(), MvEngineError> {
        // Reached from the MV engine trait, which carries no request context.
        // Use the same bounded, non-cancellable context other context-free
        // connector paths use.
        let connector_context = crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .map_err(|error| MvEngineError::new(MvEngineErrorKind::DescriptorSync, error))?;
        sync_iceberg_mv_descriptor_with_ports(
            &self.ports,
            definition,
            &definition.refresh_policy,
            definition.refresh_paused,
            definition.refresh_interval_ms,
            None,
            &connector_context,
        )
        .map_err(|error| MvEngineError::new(MvEngineErrorKind::DescriptorSync, error))
    }

    fn register_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        let preparation_key = Self::preparation_key(&target.target);
        let target = IcebergMvTarget {
            catalog: target.target.catalog.clone().ok_or_else(|| {
                MvEngineError::new(
                    MvEngineErrorKind::CatalogRegistration,
                    "Iceberg MV target has no catalog",
                )
            })?,
            namespace: target.target.database.clone(),
            table: target.target.name.clone(),
        };
        #[cfg(test)]
        if let Some(error) = take_catalog_registration_failure_for_test() {
            return Err(MvEngineError::new(
                MvEngineErrorKind::CatalogRegistration,
                error,
            ));
        }
        register_iceberg_mv_target_in_catalog(
            &MvTargetRestoreContext {
                connector_control: self.ports.connector_control.as_ref(),
                mv_repository: self.ports.repository.as_ref(),
            },
            &target,
        )
        .map_err(|error| MvEngineError::new(MvEngineErrorKind::CatalogRegistration, error))?;
        self.preparations
            .lock()
            .map_err(|error| {
                MvEngineError::new(
                    MvEngineErrorKind::CatalogRegistration,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .remove(&preparation_key);
        Ok(())
    }

    fn drop_created_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        let prepared = self.preparation_for_target(&target.target)?;
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse(&prepared.target.catalog)
                .map_err(|error| engine_target_error(error.to_string()))?;
        crate::connector::mutation::execute_catalog_mutation(
            self.ports.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(prepared.target.namespace.as_str()),
                    table: Arc::from(prepared.target.table.as_str()),
                },
                policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                data_disposition:
                    novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
            },
            self.connector_context.clone(),
        )
        .map_err(engine_target_error)?;
        self.preparations
            .lock()
            .map_err(|error| {
                MvEngineError::new(
                    MvEngineErrorKind::TargetOperation,
                    format!("MV CREATE preparation lock poisoned: {error}"),
                )
            })?
            .remove(&Self::preparation_key(&target.target));
        Ok(())
    }
}

fn engine_prepare_error(error: String) -> MvEngineError {
    MvEngineError::new(MvEngineErrorKind::Analysis, error)
}

fn engine_target_error(error: String) -> MvEngineError {
    MvEngineError::new(MvEngineErrorKind::TargetOperation, error)
}

/// Converts a typed external-mutation outcome into the CREATE target boundary.
///
/// A bootstrap that is known committed but cannot finalize remains an error:
/// callers must not persist an MV definition unless every required target fact
/// has been re-read successfully. Commit-unknown is likewise propagated as
/// such, so no cleanup can erase a target whose external truth is unresolved.
fn require_known_committed_target_mutation(
    resolution: crate::connector::mutation::ResolvedCatalogMutation,
    operation: &str,
) -> Result<crate::connector::mutation::CompletedCatalogMutation, MvEngineError> {
    match resolution {
        crate::connector::mutation::ResolvedCatalogMutation::KnownCommitted(completed) => {
            if let novarocks_spi::connector::ExternalMutationFinalization::Failed(failure) =
                &completed.finalization
            {
                return Err(engine_target_error(
                    EngineError::commit_known_committed_finalize_failed(format!(
                        "{operation}: {failure}"
                    ))
                    .to_string(),
                ));
            }
            Ok(completed)
        }
        crate::connector::mutation::ResolvedCatalogMutation::KnownUncommitted { failure } => {
            Err(engine_target_error(
                EngineError::commit_known_uncommitted(format!("{operation}: {failure}"))
                    .to_string(),
            ))
        }
        crate::connector::mutation::ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            Err(engine_target_error(
                EngineError::commit_unknown(format!("{operation}: {failure}")).to_string(),
            ))
        }
        crate::connector::mutation::ResolvedCatalogMutation::ContractFailure { error, .. } => {
            Err(engine_target_error(format!("{operation}: {error}")))
        }
    }
}

fn initial_refresh_configuration_for_create(
    policy: &crate::mv::application::MvCreateRefreshPolicy,
) -> crate::mv::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = match policy {
        crate::mv::application::MvCreateRefreshPolicy::Manual => {
            (StoredMvRefreshPolicy::Manual, None)
        }
        crate::mv::application::MvCreateRefreshPolicy::AsyncOnChange => {
            (StoredMvRefreshPolicy::AsyncOnChange, None)
        }
        crate::mv::application::MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            (StoredMvRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    };
    crate::mv::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
        next_refresh_after_ms: None,
    }
}

fn refresh_policy_descriptor_json_for_create(
    policy: &crate::mv::application::MvCreateRefreshPolicy,
) -> serde_json::Value {
    match policy {
        crate::mv::application::MvCreateRefreshPolicy::Manual => serde_json::json!({
            "policy": "DEFERRED_MANUAL", "interval_ms": null, "paused": false,
        }),
        crate::mv::application::MvCreateRefreshPolicy::AsyncOnChange => serde_json::json!({
            "policy": "ASYNC_ON_CHANGE", "interval_ms": null, "paused": false,
        }),
        crate::mv::application::MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            serde_json::json!({
                "policy": "ASYNC_INTERVAL", "interval_ms": interval_ms, "paused": false,
            })
        }
    }
}

fn partition_fields_for_create(
    fields: Option<&Vec<crate::mv::application::MvCreatePartitionField>>,
) -> Vec<IcebergPartitionFieldExpr> {
    fields
        .into_iter()
        .flatten()
        .map(|field| match field {
            crate::mv::application::MvCreatePartitionField::Identity { column } => {
                IcebergPartitionFieldExpr::Identity {
                    column: column.clone(),
                }
            }
            crate::mv::application::MvCreatePartitionField::Year { column } => {
                IcebergPartitionFieldExpr::Year {
                    column: column.clone(),
                }
            }
            crate::mv::application::MvCreatePartitionField::Month { column } => {
                IcebergPartitionFieldExpr::Month {
                    column: column.clone(),
                }
            }
            crate::mv::application::MvCreatePartitionField::Day { column } => {
                IcebergPartitionFieldExpr::Day {
                    column: column.clone(),
                }
            }
            crate::mv::application::MvCreatePartitionField::Hour { column } => {
                IcebergPartitionFieldExpr::Hour {
                    column: column.clone(),
                }
            }
            crate::mv::application::MvCreatePartitionField::Bucket {
                column,
                num_buckets,
            } => IcebergPartitionFieldExpr::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            crate::mv::application::MvCreatePartitionField::Truncate { column, width } => {
                IcebergPartitionFieldExpr::Truncate {
                    column: column.clone(),
                    width: *width,
                }
            }
            crate::mv::application::MvCreatePartitionField::Void { column } => {
                IcebergPartitionFieldExpr::Void {
                    column: column.clone(),
                }
            }
        })
        .collect()
}

fn prepare_iceberg_mv_create_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvCreateStatement,
    repository: &dyn crate::mv::repository::MvRepository,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<IcebergMvCreatePreparation, String> {
    crate::connector::validate_request_context(connector_context)?;
    let storage_engine = stmt
        .properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, value)| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "iceberg".to_string());
    match storage_engine.as_str() {
        "iceberg" => {}
        "starrocks" => return Err(
            "storage_engine='starrocks' is no longer supported for standalone materialized views; use storage_engine='iceberg'".to_string(),
        ),
        _ => return Err(format!("unknown materialized view storage_engine `{storage_engine}`")),
    }
    let current_catalog = current_catalog.ok_or_else(|| {
        "storage_engine='iceberg' requires current catalog to be an Iceberg catalog".to_string()
    })?;
    let (namespace, table) = match stmt.name_parts.as_slice() {
        [table] => (
            normalize_identifier(current_database)?,
            normalize_identifier(table)?,
        ),
        [namespace, table] => (
            normalize_identifier(namespace)?,
            normalize_identifier(table)?,
        ),
        [catalog, namespace, table] if normalize_identifier(catalog)? == "default_catalog" => (
            normalize_identifier(namespace)?,
            normalize_identifier(table)?,
        ),
        [catalog, ..] => {
            return Err(format!(
                "materialized view name catalog must be `default_catalog`, got {}",
                normalize_identifier(catalog)?
            ));
        }
        _ => return Err("materialized view name must have one, two, or three parts".to_string()),
    };
    let target = IcebergMvTarget {
        catalog: normalize_identifier(current_catalog)?,
        namespace,
        table,
    };
    ensure_mv_create_target_absent_with_ports(ports, &target, connector_context)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &stmt.select_query,
        Some(current_catalog),
        current_database,
    );
    let catalog_service =
        crate::catalog_application::query_catalog::catalog_service_snapshot(ports);
    let provider = crate::query_execution::compiler::build_catalog_service_provider(
        Some(current_catalog),
        &catalog_service,
        ports.connector_control.as_ref(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        ports.catalog_application.as_deref(),
    );
    let analysis = crate::mv::analysis_adapter::analyze_mv_select_with_provider(
        Some(current_catalog),
        &provider,
        current_database,
        &canonical_select_query,
    )?;
    let refresh_contract = derive_imv_refresh_contract(&analysis)?;
    let partition_fields = partition_fields_for_create(stmt.partition_by.as_ref());
    validate_mv_partition_columns(Some(&partition_fields), &analysis.output_columns)?;
    let created_at_ms = now_ms();
    let resolved_dependencies =
        crate::mv::dependency_resolver::resolve_create_mv_dependencies_with_repository(
            repository,
            &analysis.resolved_refs,
            created_at_ms,
        )?;
    let dependency_target = crate::mv::dependency::model::iceberg_mv_dependency_ref(
        &target.catalog,
        &target.namespace,
        &target.table,
    );
    crate::mv::dependency_resolver::validate_no_create_cycle_with_repository(
        repository,
        &dependency_target,
        &resolved_dependencies.dependencies,
    )
    .map_err(|e| {
        format!(
            "cannot create materialized view {}.{}.{}: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;
    let property = derive_fragment_property(&analysis)?;
    let base_field_observations = observe_base_fields_for_refs_with_ports(
        ports,
        &resolved_dependencies.base_refs,
        connector_context,
    )?;
    for base_ref in &resolved_dependencies.base_refs {
        ensure_base_row_lineage_contract(
            observed_base(&base_field_observations, base_ref)?,
            &base_ref.fqn(),
        )?;
    }
    if let Some(pk_cols) = stmt.primary_key.as_deref() {
        match &property.identity {
            TargetIdentity::BaseRowId => {
                let base_ref = resolved_dependencies.base_refs.first().ok_or_else(|| {
                    "iceberg-backed materialized view has no resolved base table".to_string()
                })?;
                validate_ivm_primary_key(
                    pk_cols,
                    &base_table_descriptor_from_observation(observed_base(
                        &base_field_observations,
                        base_ref,
                    )?),
                )?;
            }
            TargetIdentity::JoinRowKey(_, _) => return Err("iceberg-backed join materialized views do not support PRIMARY KEY in this phase".to_string()),
            TargetIdentity::BranchScoped(_) => return Err("iceberg-backed UNION ALL materialized views do not support PRIMARY KEY in this phase".to_string()),
            TargetIdentity::GroupRowId(_) => return Err("iceberg-backed aggregate materialized views do not support PRIMARY KEY".to_string()),
        }
    }
    if !partition_fields.is_empty() && property.is_composed_aggregate_schema_contract_fallback() {
        return Err("partitioned composed aggregate Iceberg MV is not supported".to_string());
    }
    let apply_key_column_name = refresh_contract.apply_key.column_name;
    if analysis
        .output_columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
    {
        return Err(format!(
            "Iceberg MV output column name {apply_key_column_name} is reserved for internal apply key"
        ));
    }
    if identity_needs_branch_id_column(&property.identity)
        && analysis
            .output_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME))
    {
        return Err(format!(
            "Iceberg MV output column name {BRANCH_ID_COLUMN_NAME} is reserved for internal branch id"
        ));
    }
    let mut columns =
        create_target_columns_from_property(&property, &canonical_select_query, &analysis)?;
    if identity_needs_physical_apply_key_column(&property.identity) {
        columns.push(create_apply_key_table_column(&refresh_contract.apply_key)?);
    }
    if identity_needs_branch_id_column(&property.identity) {
        columns.push(branch_id_table_column());
    }
    let expected_apply_key_field_id = columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
        .and_then(|idx| i32::try_from(idx + 1).ok())
        .ok_or_else(|| {
            format!(
                "Iceberg MV target columns are missing apply-key column {apply_key_column_name}"
            )
        })?;
    let aggregate_state_hidden_columns = aggregate_state_hidden_columns_from_property(
        &property,
        &canonical_select_query,
        &analysis,
    )?;
    let mut descriptor_hidden_columns = Vec::new();
    if identity_needs_physical_apply_key_column(&property.identity) {
        descriptor_hidden_columns.push(apply_key_column_name.to_string());
    }
    if identity_needs_branch_id_column(&property.identity) {
        descriptor_hidden_columns.push(BRANCH_ID_COLUMN_NAME.to_string());
    }
    descriptor_hidden_columns.extend(aggregate_state_hidden_columns.iter().cloned());
    let descriptor = MvDescriptorV1 {
        descriptor_version: MV_DESCRIPTOR_VERSION,
        package_id: format!("{}.{}", target.namespace, target.table),
        logical_sql: canonical_select_query.to_string(),
        dialect: "starrocks".to_string(),
        visible_columns: analysis
            .output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        hidden_columns: descriptor_hidden_columns,
        base_dependencies: resolved_dependencies
            .dependencies
            .iter()
            .map(descriptor_dependency_from_request)
            .collect(),
        schema_contract: None,
        refresh_contract: Some(refresh_policy_descriptor_json_for_create(
            &stmt.refresh_policy,
        )),
        created_at_ms,
    };
    let mut target_properties = vec![
        ("format-version".to_string(), "3".to_string()),
        ("write.row-lineage".to_string(), "true".to_string()),
        (
            APPLY_KEY_COLUMN_PROPERTY.to_string(),
            apply_key_column_name.to_string(),
        ),
        (
            APPLY_KEY_SOURCE_PROPERTY.to_string(),
            create_apply_key_source_property(&refresh_contract.apply_key).to_string(),
        ),
        (
            APPLY_KEY_FIELD_ID_PROPERTY.to_string(),
            expected_apply_key_field_id.to_string(),
        ),
    ];
    if !aggregate_state_hidden_columns.is_empty() {
        target_properties.push((
            HIDDEN_COLUMNS_PROPERTY.to_string(),
            aggregate_state_hidden_columns.join(","),
        ));
    }
    target_properties.extend(descriptor.to_storage_properties()?);
    Ok(IcebergMvCreatePreparation {
        target,
        canonical_select_query,
        analysis,
        refresh_contract,
        property,
        base_refs: resolved_dependencies.base_refs,
        dependencies: resolved_dependencies.dependencies,
        base_field_observations,
        expected_apply_key_field_id,
        columns,
        partition_fields,
        target_properties,
        created_at_ms,
        created_target_observation: Mutex::new(None),
    })
}

pub(crate) fn create_iceberg_mv_with_ports(
    ports: IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let statement = MvCreateStatement::from(stmt);
    let engine = StandaloneMvEngine::new_with_ports(ports.clone(), connector_context.clone());
    let plan = engine
        .prepare_create(
            PrepareMvCreateRequest {
                statement: &statement,
                context: crate::mv::application::MvRequestContext {
                    current_catalog,
                    current_database,
                },
            },
            ports.repository.as_ref(),
        )
        .map_err(|error| error.to_string())?;
    let operation_id = uuid::Uuid::now_v7();
    let target = engine
        .create_target(&plan, operation_id)
        .map_err(|error| error.to_string())?;
    let definition = match engine.inspect_created_target(&plan, &target) {
        Ok(definition) => definition,
        Err(error) => {
            return Err(legacy_cleanup_created_target(
                &engine,
                &target,
                error.to_string(),
            ));
        }
    };
    let definition = match ports
        .repository
        .create(operation_id, definition.repository_request)
    {
        Ok(definition) => definition,
        Err(error)
            if error.kind() == crate::mv::repository::MvRepositoryErrorKind::CommitUnknown =>
        {
            return Err(error.to_string());
        }
        Err(error) => {
            return Err(legacy_cleanup_created_target(
                &engine,
                &target,
                format!("create iceberg MV repository metadata failed: {error}"),
            ));
        }
    };
    if let Err(error) = engine.sync_target_descriptor(&target, &definition) {
        return Err(known_committed_create_finalize_error(
            "descriptor sync",
            error,
        ));
    }
    if let Err(error) = engine.register_target(&target) {
        return Err(known_committed_create_finalize_error(
            "catalog registration",
            error,
        ));
    }
    Ok(StatementResult::Ok)
}

fn known_committed_create_finalize_error(phase: &str, error: impl std::fmt::Display) -> String {
    EngineError::commit_known_committed_finalize_failed(format!(
        "Iceberg MV repository create committed but {phase} failed: {error}"
    ))
    .to_bracketed_user_message()
}

fn legacy_cleanup_created_target(
    engine: &dyn MvEngine,
    target: &CreatedMvTarget,
    primary: String,
) -> String {
    let cleanup = engine.drop_created_target(target);
    format!("{primary}; target cleanup={cleanup:?}")
}

fn ensure_mv_create_target_absent_with_ports(
    ports: &IcebergMvCorePorts,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    ensure_mv_create_target_absent_with_connector_control(
        ports.connector_control.as_ref(),
        target,
        connector_context,
    )
}

fn ensure_mv_create_target_absent_with_connector_control(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &target.catalog)?;
    if lease.binding().descriptor().provider_id.as_str() != "iceberg" {
        return Err(
            "storage_engine='iceberg' requires current catalog to be an Iceberg catalog"
                .to_string(),
        );
    }
    let instance_id = lease.binding().descriptor().instance_id.clone();
    match lease
        .binding()
        .metadata()
        .load_table(novarocks_spi::connector::ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
            context: connector_context.clone(),
        }) {
        Ok(_) => Err(format!(
            "Iceberg MV target table {}.{}.{} already exists",
            target.catalog, target.namespace, target.table
        )),
        Err(error) if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound => {
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

fn base_table_descriptor_from_observation(
    observation: &MvSchemaValidationObservation,
) -> BaseTableDescriptor {
    BaseTableDescriptor {
        format_version: if observation.is_format_v3() { 3 } else { 2 },
        columns: observation
            .fields()
            .iter()
            .map(|field| BaseColumnDescriptor {
                name: field.name().to_string(),
                data_type: DataType::Null,
                sql_type: observed_iceberg_type_sql_head(field.type_signature()),
                nullable: field.nullable(),
            })
            .collect(),
    }
}

fn observed_iceberg_type_sql_head(type_signature: &str) -> String {
    let lower = type_signature.trim().to_ascii_lowercase();
    let head = lower.split(['(', '<']).next().unwrap_or("").trim();
    match head {
        "long" => "BIGINT".to_string(),
        "int" => "INT".to_string(),
        "string" => "STRING".to_string(),
        "decimal" => "DECIMAL".to_string(),
        "date" => "DATE".to_string(),
        "timestamp" | "timestamptz" => "DATETIME".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

/// Validate the resolved base-ref set for an aggregate-over-UNION-ALL fan-in MV.
///
/// The legacy invariant compared the classifier's `fan_in_bases` against the
/// analyzer-resolved `base_refs` and required exact equality. With the shape
/// retired, the resolved `base_refs` ARE the fan-in base set (the analyzer
/// resolved the union branches), so the "fan_in == resolved" comparison is
/// trivially satisfied by construction. The only remaining invariant to enforce
/// is the one the legacy check also enforced independently: the resolved base
/// refs must be distinct (a duplicate fan-in base is not supported in this
/// build). Each resolved base is further checked against the persisted schema
/// contract by `validate_aggregate_schema_contract_for_base`.
fn validate_aggregate_fan_in_base_refs(base_refs: &[TableIdentity]) -> Result<(), String> {
    let mut resolved_refs = BTreeSet::new();
    for base in base_refs {
        let fqn = base.fqn().to_ascii_lowercase();
        if !resolved_refs.insert(fqn.clone()) {
            return Err(format!(
                "aggregate-over-UNION-ALL MV duplicate resolved base ref {fqn} is not supported in this build"
            ));
        }
    }
    Ok(())
}

/// Validate the resolved base-ref set for a branch UNION ALL aggregate MV.
///
/// The legacy invariant (one distinct base per branch, branch_count ==
/// base_ref count, no fan-in branches) is incompatible with composed branch
/// unions: under the CREATE-time homogeneity gate every branch references the
/// SAME (possibly multi-table) base set, so the resolved base refs are exactly
/// that shared distinct set — not one-per-branch. The branch homogeneity itself
/// (same distinct base set / join structure / fan-in arity / group-key layout
/// across branches) is enforced at CREATE in `derive_from_set_operation`, and
/// every resolved base is independently checked against the persisted schema
/// contract by `validate_aggregate_schema_contract_for_base`. The only remaining
/// invariant to enforce here is that the resolved base refs are distinct: the
/// branch base set is a set, so a duplicate resolved ref would mean the resolved
/// refs and the branch base set cannot be in 1:1 correspondence.
fn validate_branch_union_aggregate_base_refs(base_refs: &[TableIdentity]) -> Result<(), String> {
    let mut resolved_refs = BTreeSet::new();
    for base_ref in base_refs {
        let fqn = base_ref.fqn().to_ascii_lowercase();
        if !resolved_refs.insert(fqn.clone()) {
            return Err(format!(
                "branch UNION ALL aggregate MV duplicate resolved base ref {fqn} is not supported in this build"
            ));
        }
    }
    Ok(())
}

fn validate_aggregate_schema_contract_for_base(
    schema_contract: &mv_schema::MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let mut base_contract = schema_contract.clone();
    if !schema_contract.bases.is_empty() {
        base_contract.base = schema_contract
            .bases
            .iter()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
            .cloned()
            .ok_or_else(|| {
                format!(
                    "iceberg aggregate-over-UNION-ALL MV schema contract missing base {}; recreate the MV",
                    base_ref.fqn()
                )
            })?;
    } else if !schema_contract
        .base
        .table_fqn
        .eq_ignore_ascii_case(&base_ref.fqn())
    {
        return Err(format!(
            "iceberg aggregate-over-UNION-ALL MV schema contract missing base {}; recreate the MV",
            base_ref.fqn()
        ));
    }
    match validate_schema_contract(&base_contract, base_observation, target_observation) {
        ContractDecision::Incompatible(err) => Err(format!("{err}")),
        ContractDecision::CompatibleSafe => Ok(()),
        ContractDecision::CompatibleSafeWithRebind { .. } => Err(format!(
            "iceberg aggregate-over-UNION-ALL MV requires schema rebind for base {}, which is not supported for fan-in aggregate refresh; rebuild or recreate the MV",
            base_ref.fqn()
        )),
    }
}

fn validate_branch_union_contract(
    target: &IcebergMvTarget,
    schema_contract: &mv_schema::MvSchemaContract,
    query_branch_count: usize,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    if schema_contract.contract_version != 3 {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} requires schema contract version 3, got {}",
            target.catalog, target.namespace, target.table, schema_contract.contract_version
        ));
    }
    schema_contract.ensure_self_consistent().map_err(|e| {
        format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} schema contract is invalid: {e}",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.aggregate.is_none() {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} is missing aggregate contract; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    let branch_contract = schema_contract.branch.as_ref().ok_or_else(|| {
        format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} is missing branch contract; recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if branch_contract.branch_count != query_branch_count as u32 {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch contract expected {} branches, query has {}",
            target.catalog,
            target.namespace,
            target.table,
            branch_contract.branch_count,
            query_branch_count
        ));
    }
    if branch_contract
        .inner_apply_key_source
        .sql_mv_apply_key_source_facts()
        != SqlMvApplyKeySourceFacts::GroupRowId
    {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch contract must use GroupRowId inner apply keys",
            target.catalog, target.namespace, target.table
        ));
    }
    match validate_branch_id_field(&branch_contract.branch_id_column, target_observation) {
        Ok(()) => Ok(()),
        Err(BranchFieldValidationError::Missing { field_id }) => Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch id field id {field_id} is missing from target schema",
            target.catalog, target.namespace, target.table
        )),
        Err(BranchFieldValidationError::Renamed { actual, .. }) => Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch id column renamed externally to {actual}; recreate the MV",
            target.catalog, target.namespace, target.table
        )),
        Err(BranchFieldValidationError::NotRequired) => Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch id column must be required",
            target.catalog, target.namespace, target.table
        )),
        Err(BranchFieldValidationError::WrongType { actual, .. }) => Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch id column must be Int, got {actual}",
            target.catalog, target.namespace, target.table
        )),
    }
}

/// Validate that the resolved `base_refs` exactly match the persisted base set
/// in the schema contract and that the contract has one base per expected branch.
///
/// Replaces the legacy `UnionAllMvShape`-based check: instead of collecting
/// per-branch `base_table` FQNs from the classifier, we compare the resolved
/// `base_refs` (already the authority for base identity) against the FQNs
/// recorded in `schema_contract.bases[]`. The accept/reject contract is
/// identical: a mismatch between the resolved refs and the contract base set is
/// an error; a match is accepted.
fn validate_union_projection_base_refs(
    base_refs: &[TableIdentity],
    schema_contract: &mv_schema::MvSchemaContract,
) -> Result<(), String> {
    let contract_fqns = schema_contract
        .bases
        .iter()
        .map(|base| base.table_fqn.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let resolved_fqns = base_refs
        .iter()
        .map(|base_ref| base_ref.fqn().to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if contract_fqns != resolved_fqns {
        return Err(format!(
            "UNION ALL projection/filter MV branch bases must exactly match resolved base refs: contract_bases={contract_fqns:?}, resolved={resolved_fqns:?}"
        ));
    }
    Ok(())
}

/// Validate the persisted schema contract for a single base table of a UNION ALL
/// projection/filter MV.
///
/// The `branch_count` parameter replaces the legacy `union_shape.branches.len()`
/// access: callers source it from `contract.branch.branch_count` (the persisted
/// contract) so the check remains byte-identical — a mismatch between the
/// contract-recorded count and the caller-supplied count is still an error.
fn validate_union_projection_schema_contract_for_base(
    iceberg_target: &IcebergMvTarget,
    schema_contract: &mv_schema::MvSchemaContract,
    branch_count: usize,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    if schema_contract.contract_version != 1 {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} requires schema contract version 1, got {}",
            iceberg_target.catalog,
            iceberg_target.namespace,
            iceberg_target.table,
            schema_contract.contract_version
        ));
    }
    schema_contract.ensure_self_consistent().map_err(|e| {
        format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} schema contract is invalid: {e}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )
    })?;
    let branch_contract = schema_contract.branch.as_ref().ok_or_else(|| {
        format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} is missing branch contract; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )
    })?;
    if branch_contract.branch_count != branch_count as u32 {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} branch contract expected {} branches, query has {}",
            iceberg_target.catalog,
            iceberg_target.namespace,
            iceberg_target.table,
            branch_contract.branch_count,
            branch_count
        ));
    }
    if branch_contract
        .inner_apply_key_source
        .sql_mv_apply_key_source_facts()
        != SqlMvApplyKeySourceFacts::BaseRowId
    {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} branch contract must use BaseRowId inner apply keys",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ));
    }
    if let Err(error) =
        validate_branch_id_field(&branch_contract.branch_id_column, target_observation)
    {
        return Err(match error {
            BranchFieldValidationError::Missing { field_id } => format!(
                "iceberg UNION ALL projection/filter MV {}.{}.{} branch id field id {field_id} is missing from target schema",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ),
            BranchFieldValidationError::Renamed { actual, .. } => format!(
                "iceberg UNION ALL projection/filter MV {}.{}.{} branch id column renamed externally to {actual}; recreate the MV",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ),
            BranchFieldValidationError::NotRequired => format!(
                "iceberg UNION ALL projection/filter MV {}.{}.{} branch id column must be required",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ),
            BranchFieldValidationError::WrongType { actual, .. } => format!(
                "iceberg UNION ALL projection/filter MV {}.{}.{} branch id column must be Int, got {actual}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ),
        });
    }

    let mut base_contract = schema_contract.clone();
    base_contract.base = schema_contract
        .bases
        .iter()
        .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
        .cloned()
        .ok_or_else(|| {
            format!(
                "iceberg UNION ALL projection/filter MV {}.{}.{} schema contract missing base {}; recreate the MV",
                iceberg_target.catalog,
                iceberg_target.namespace,
                iceberg_target.table,
                base_ref.fqn()
            )
        })?;
    match validate_schema_contract(&base_contract, base_observation, target_observation) {
        ContractDecision::Incompatible(err) => Err(format!("{err}")),
        ContractDecision::CompatibleSafe => Ok(()),
        ContractDecision::CompatibleSafeWithRebind { .. } => Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} requires schema rebind, which is not supported for UNION ALL refresh; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )),
    }
}

pub(crate) fn union_branch_inner_apply_key(
    branch_kind: UnionBranchKind,
) -> SqlMvApplyKeySourceFacts {
    match branch_kind {
        UnionBranchKind::Aggregate => SqlMvApplyKeySourceFacts::GroupRowId,
        UnionBranchKind::ProjectionFilter => SqlMvApplyKeySourceFacts::BaseRowId,
    }
}

/// Visible target columns for a new Iceberg MV, driven by the synthesized
/// identity. Stateless identities (projection/filter, join, and their UNION
/// ALL) keep the analyzed visible output columns verbatim; aggregate identities
/// derive their physical (state-shaped) layout from the representative
/// aggregate sub-query.
fn create_target_columns_from_property(
    property: &RefreshFragmentProperty,
    canonical_query: &sqlparser::ast::Query,
    analysis: &MvAnalysis,
) -> Result<Vec<TableColumnDef>, String> {
    match representative_aggregate_layout(property, canonical_query, analysis)? {
        None => analysis
            .output_columns
            .iter()
            .map(output_column_to_table_column)
            .collect::<Result<Vec<_>, _>>(),
        Some(layout) => iceberg_aggregate_target_columns_from_layout(&layout),
    }
}

/// Hidden aggregate-state column names for a new Iceberg MV (empty for
/// non-aggregate identities), driven by the synthesized identity.
fn aggregate_state_hidden_columns_from_property(
    property: &RefreshFragmentProperty,
    canonical_query: &sqlparser::ast::Query,
    analysis: &MvAnalysis,
) -> Result<Vec<String>, String> {
    let Some(layout) = representative_aggregate_layout(property, canonical_query, analysis)? else {
        return Ok(Vec::new());
    };
    Ok(layout
        .state_columns
        .iter()
        .map(|column| column.name.clone())
        .collect())
}

/// The aggregate physical layout used for target-schema generation, or `None`
/// when the property's identity carries no aggregate state (projection/filter,
/// join, or their UNION ALL).
///
/// For a non-branch aggregate (`GroupRowId`) the layout is built from the whole
/// query. For a branch-union aggregate (`BranchScoped(GroupRowId)`) it is built
/// from the *first* branch — matching the legacy single-representative target
/// layout. The first branch may itself be a simple aggregate, an aggregate over
/// a join, or a fan-in aggregate; the FROM-agnostic `extract_aggregate_sql_calls`
/// extractor yields the right aggregate-call surface in every case.
fn representative_aggregate_layout(
    property: &RefreshFragmentProperty,
    canonical_query: &sqlparser::ast::Query,
    analysis: &MvAnalysis,
) -> Result<Option<crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout>, String> {
    match inner_row_identity(&property.identity) {
        TargetIdentity::BaseRowId | TargetIdentity::JoinRowKey(_, _) => Ok(None),
        TargetIdentity::GroupRowId(_) => {
            let scope = if matches!(property.identity, TargetIdentity::BranchScoped(_)) {
                SqlMvAggregateLayoutScope::FirstUnionBranch
            } else {
                SqlMvAggregateLayoutScope::WholeQuery
            };
            let facts = analysis
                .refresh_input
                .aggregate_layout_facts(canonical_query, scope)?;
            crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_from_sql_facts(
                &facts,
            )
            .map(Some)
        }
        // `inner_row_identity` already peeled the branch wrapper; a nested
        // `BranchScoped` cannot occur (construction flattens it).
        TargetIdentity::BranchScoped(_) => Err(
            "Iceberg MV target layout internal error: unflattened branch-scoped identity"
                .to_string(),
        ),
    }
}

/// Extract the first UNION ALL branch as a standalone AST query.
fn first_union_branch_ast_query(
    query: &sqlparser::ast::Query,
) -> Result<sqlparser::ast::Query, String> {
    fn first_branch_body(
        body: &sqlparser::ast::SetExpr,
    ) -> Result<&sqlparser::ast::SetExpr, String> {
        match body {
            sqlparser::ast::SetExpr::SetOperation { left, .. } => first_branch_body(left),
            sqlparser::ast::SetExpr::Query(inner) => first_branch_body(inner.body.as_ref()),
            other => Ok(other),
        }
    }
    let mut branch = query.clone();
    branch.body = Box::new(first_branch_body(query.body.as_ref())?.clone());
    Ok(branch)
}

/// Observe neutral schema fields for every base, keyed by table FQN.
fn observe_base_fields_for_refs_with_ports(
    ports: &IcebergMvCorePorts,
    base_refs: &[TableIdentity],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    String,
> {
    let mut observed = std::collections::BTreeMap::new();
    for base_ref in base_refs {
        let observation =
            observe_schema_validation_for_table_with_ports(ports, base_ref, connector_context)?;
        observed.insert(base_ref.fqn(), observation);
    }
    Ok(observed)
}

fn create_apply_key_source_property(apply_key: &ApplyKeyContract) -> &'static str {
    mv_apply_key_source_from_column_name(apply_key.column_name)
        .expect("known Iceberg MV apply-key column")
        .table_property_value()
}

fn create_apply_key_contract_source(apply_key: &ApplyKeyContract) -> SqlMvApplyKeySourceFacts {
    mv_apply_key_source_from_column_name(apply_key.column_name)
        .expect("known Iceberg MV apply-key column")
}

fn descriptor_dependency_from_request(request: &CreateMvDependencyRequest) -> DescriptorDependency {
    let upstream = &request.upstream;
    DescriptorDependency {
        catalog: upstream.catalog.clone().unwrap_or_default(),
        namespace: upstream.database_or_namespace.clone(),
        name: upstream.name.clone(),
        object_type: match upstream.object_type {
            MvDependencyObjectType::Table => "table",
            MvDependencyObjectType::MaterializedView => "materialized_view",
        }
        .to_string(),
        storage_engine: match upstream.storage_engine {
            MvDependencyStorageEngine::StarRocks => "starrocks",
            MvDependencyStorageEngine::Iceberg => "iceberg",
            MvDependencyStorageEngine::ExternalTable => "external_table",
        }
        .to_string(),
    }
}

fn refresh_policy_descriptor_json(
    policy: &MaterializedViewRefreshPolicy,
    paused: bool,
) -> serde_json::Value {
    match policy {
        MaterializedViewRefreshPolicy::Manual => serde_json::json!({
            "policy": "DEFERRED_MANUAL",
            "interval_ms": null,
            "paused": paused,
        }),
        MaterializedViewRefreshPolicy::AsyncOnChange => {
            serde_json::json!({
                "policy": "ASYNC_ON_CHANGE",
                "interval_ms": null,
                "paused": paused,
            })
        }
        MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => {
            serde_json::json!({
                "policy": "ASYNC_INTERVAL",
                "interval_ms": interval_ms,
                "paused": paused,
            })
        }
    }
}

fn stored_refresh_policy_descriptor_json(
    policy: &StoredMvRefreshPolicy,
    paused: bool,
    interval_ms: Option<i64>,
) -> serde_json::Value {
    serde_json::json!({
        "policy": policy.as_sql_str(),
        "interval_ms": interval_ms,
        "paused": paused,
    })
}

/// Read-modify-write the Iceberg MV target table's descriptor properties:
/// observe the descriptor from one exact connector generation, overwrite `refresh_contract` from the given
/// refresh-policy inputs, carry `definition.schema_contract` (if present)
/// into the descriptor's `schema_contract` field, and write the descriptor
/// back through a mutation lease derived from the same generation. Repartition
/// projection supplies the raw provider-committed partitioning as an atomic
/// property-mutation guard; ordinary CREATE/ALTER policy sync passes `None`.
pub(crate) fn sync_iceberg_mv_descriptor_with_ports(
    ports: &IcebergMvCorePorts,
    definition: &StoredMvDefinition,
    refresh_policy: &StoredMvRefreshPolicy,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    expected_committed_partitioning: Option<
        novarocks_spi::connector::ConnectorCommittedPartitioning,
    >,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    if definition.storage_engine != MvStorageEngine::Iceberg.as_sql_str() {
        return Ok(());
    }
    let catalog_name = definition
        .target_catalog
        .as_deref()
        .ok_or_else(|| "Iceberg MV descriptor sync missing target catalog".to_string())?;
    let namespace = definition
        .target_namespace
        .as_deref()
        .ok_or_else(|| "Iceberg MV descriptor sync missing target namespace".to_string())?;
    let target_table_name = definition
        .target_table
        .as_deref()
        .ok_or_else(|| "Iceberg MV descriptor sync missing target table".to_string())?;
    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        ports.connector_control.as_ref(),
        catalog_name,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        namespace,
        target_table_name,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let package = ports
        .storage_observation
        .observe_lake_package(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| format!("observe MV descriptor storage facts failed: {error}"))?
        .ok_or_else(|| {
            format!(
                "Iceberg MV descriptor is absent for target {catalog_name}.{namespace}.{target_table_name}"
            )
        })?;
    let mut descriptor = package.descriptor;
    descriptor.refresh_contract = Some(stored_refresh_policy_descriptor_json(
        refresh_policy,
        refresh_paused,
        refresh_interval_ms,
    ));
    // W2: the descriptor is the authoritative home for the schema contract.
    // Carry the definition's contract into the descriptor whenever we sync.
    if let Some(contract) = &definition.schema_contract {
        descriptor.set_schema_contract(contract)?;
    }
    let descriptor_properties = descriptor.to_storage_properties()?;
    // The MV descriptor lives in the target's engine-owned property namespace.
    // Writing it is a catalog property mutation, not a reason for the MV path to
    // open an Iceberg transaction of its own (SPI-5I F6).
    let mutation_lease = exact_lease
        .derive_mutation_lease()
        .map_err(|error| error.to_string())?;
    require_known_committed_target_mutation(
        crate::connector::mutation::resolve_catalog_mutation_with_lease(
            &mutation_lease,
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
            novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterProperties {
                table: package.table,
                changes: descriptor_properties
                    .into_iter()
                    .map(
                        |(key, value)| novarocks_spi::connector::ConnectorPropertyChange::Set {
                            key: Arc::from(key.as_str()),
                            value: Arc::from(value.as_str()),
                        },
                    )
                    .collect(),
                authority: novarocks_spi::connector::ConnectorPropertyAuthority::EngineOwned,
                expected_committed_partitioning,
            },
            connector_context.clone(),
        ),
        "sync MV descriptor properties",
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// Peel any top-level `BranchScoped` wrapper, returning the per-row inner
/// identity. `BranchScoped` construction already flattens nesting, so a single
/// peel is sufficient.
fn inner_row_identity(identity: &TargetIdentity) -> &TargetIdentity {
    match identity {
        TargetIdentity::BranchScoped(inner) => inner.as_ref(),
        other => other,
    }
}

/// A physical apply-key column is materialized iff each output row is
/// identified by a base or join row id (`BaseRowId` / `JoinRowKey`), whose
/// apply key is stored as a real target column. Group-row identities
/// (`GroupRowId`) derive their apply key from the group keys, so no physical
/// column is added. The `BranchScoped` wrapper is transparent here — what
/// matters is the per-row inner identity. This reproduces the legacy
/// strategy-based gating (ProjectionFilter / JoinProjectionFilter /
/// UnionProjectionFilter required the column; the aggregate strategies did
/// not).
fn identity_needs_physical_apply_key_column(identity: &TargetIdentity) -> bool {
    matches!(
        inner_row_identity(identity),
        TargetIdentity::BaseRowId | TargetIdentity::JoinRowKey(_, _)
    )
}

/// A `__branch_id__` discriminant column is materialized iff the output is a
/// UNION ALL (the identity top is `BranchScoped`). Reproduces the legacy gating
/// (UnionProjectionFilter / BranchUnionAggregate required it).
fn identity_needs_branch_id_column(identity: &TargetIdentity) -> bool {
    matches!(identity, TargetIdentity::BranchScoped(_))
}

fn create_apply_key_table_column(apply_key: &ApplyKeyContract) -> Result<TableColumnDef, String> {
    match apply_key.column_name {
        HIDDEN_APPLY_KEY_COLUMN_NAME => Ok(apply_key_table_column()),
        JOIN_APPLY_KEY_COLUMN_NAME => Ok(join_apply_key_table_column()),
        other => Err(format!(
            "Iceberg MV refresh contract apply-key column {other} is not a physical target apply-key column"
        )),
    }
}

fn base_snapshot_status_for_refresh(
    base_ref: &TableIdentity,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id_before_pin: Option<i64>,
) -> BaseSnapshotStatus {
    BaseSnapshotStatus::new(
        base_ref.fqn(),
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )
}

fn iceberg_aggregate_target_columns_from_layout(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<TableColumnDef>, String> {
    validate_unique_aggregate_physical_column_names(&layout.physical_columns)?;
    Ok(layout
        .physical_columns
        .iter()
        .map(|column| column.column.clone())
        .collect())
}

/// Build the persisted [`MvSchemaContract`] for a new Iceberg MV, dispatching
/// on the synthesized [`TargetIdentity`].
///
/// The dispatch is identity-RECURSIVE: a `BranchScoped(inner)` top builds each
/// branch's inner contract by recursing on the inner structure (so a branch
/// that is `Agg(a JOIN b)` builds its join lineage WITHIN the branch), then
/// attaches a [`BranchUnionContract`]. The leaf builders
/// (`target_contract` / `aggregate_contract` / `base_contract` /
/// `build_*_lineage`) are reused unchanged; only the dispatch changed.
///
/// Shape data the leaf builders need (aggregate calls, join table
/// identity/aliases) is sourced from the focused FROM-agnostic extractors
/// (`extract_aggregate_sql_calls` / `extract_join_aliases` /
/// `extract_single_scan_table_fqn`) over `canonical_query` (or its first branch)
/// plus the resolved `MvAnalysis`.
#[allow(clippy::too_many_arguments)]
fn build_iceberg_mv_schema_contract(
    refresh_contract: &ImvRefreshContract,
    property: &RefreshFragmentProperty,
    canonical_query: &sqlparser::ast::Query,
    analysis: &crate::mv::analysis::MvAnalysis,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target: &IcebergMvTarget,
    target_observation: &MvTargetCreationObservation,
    actual_apply_key_field_id: i32,
) -> Result<mv_schema::MvSchemaContract, String> {
    let target_apply_key_column = refresh_contract.apply_key.column_name;
    let target_apply_key_source = create_apply_key_contract_source(&refresh_contract.apply_key);
    let target_contract = target_contract(
        analysis,
        target,
        target_observation,
        actual_apply_key_field_id,
        target_apply_key_column,
        target_apply_key_source,
    )?;

    let contract = match &property.identity {
        // UNION ALL top: build the (first) branch's inner contract, widen the
        // base set to all branches, and attach the branch contract.
        TargetIdentity::BranchScoped(inner) => build_branch_union_schema_contract(
            inner,
            canonical_query,
            analysis,
            base_refs,
            base_field_observations,
            target_observation,
            target_contract,
        )?,
        // Non-branch: build the core contract directly over the whole query.
        _ => build_non_branch_schema_contract(
            &property.identity,
            canonical_query,
            analysis,
            base_refs,
            base_field_observations,
            target_observation,
            target_contract,
        )?,
    };

    contract
        .ensure_self_consistent()
        .map_err(|e| format!("Iceberg MV schema contract is self-inconsistent: {e}"))?;
    Ok(contract)
}

/// The base/output/join/aggregate "core" of a non-branch MV schema contract,
/// plus the contract version. Carried so the UNION ALL builder can reuse a
/// branch's core while substituting the full (cross-branch) base set.
struct NonBranchContractCore {
    contract_version: u16,
    /// The branch-local base contracts, with referenced-field narrowing already
    /// applied (e.g. a projection/filter base narrowed to its lineage fields,
    /// or both join bases narrowed to their join lineage fields). For a fan-in
    /// aggregate these are the full fan-in base schemas (Mixed output).
    bases: Vec<mv_schema::BaseContract>,
    output: mv_schema::OutputContract,
    join: Option<mv_schema::JoinContract>,
    aggregate: Option<mv_schema::AggregateStateContract>,
}

/// Build a full (branch-free) schema contract for a non-branch identity over
/// opaque analyzed SQL input. Used for top-level non-branch MVs.
fn build_non_branch_schema_contract(
    identity: &TargetIdentity,
    query: &sqlparser::ast::Query,
    analysis: &crate::mv::analysis::MvAnalysis,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target_observation: &MvTargetCreationObservation,
    target: mv_schema::TargetContract,
) -> Result<mv_schema::MvSchemaContract, String> {
    let core = build_non_branch_contract_core(
        identity,
        query,
        analysis,
        base_refs,
        base_field_observations,
        target_observation,
    )?;
    let base = core.bases.first().cloned().ok_or_else(|| {
        "iceberg MV schema contract requires at least one loaded base".to_string()
    })?;
    // Single-base shapes historically persist an empty `bases` vec (the single
    // base lives in `base`); multi-base shapes persist the full list.
    let bases = if core.bases.len() == 1 {
        Vec::new()
    } else {
        core.bases
    };
    Ok(mv_schema::MvSchemaContract {
        contract_version: core.contract_version,
        base,
        bases,
        output: core.output,
        join: core.join,
        aggregate: core.aggregate,
        branch: None,
        target,
    })
}

/// Build the core of a non-branch contract for `identity` over
/// query and opaque analyzed SQL input, classifying any shape data locally. This is the
/// per-identity dispatch that the legacy per-`RefreshStrategy` match performed,
/// reproduced verbatim but keyed on the identity.
fn build_non_branch_contract_core(
    identity: &TargetIdentity,
    query: &sqlparser::ast::Query,
    analysis: &crate::mv::analysis::MvAnalysis,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target_observation: &MvTargetCreationObservation,
) -> Result<NonBranchContractCore, String> {
    match identity {
        // Projection / filter over a single scan (legacy ProjectionFilter).
        TargetIdentity::BaseRowId => {
            let [base_ref] = base_refs else {
                return Err(
                    "projection/filter iceberg MV schema contract requires one loaded base"
                        .to_string(),
                );
            };
            let lineage_schema = sql_mv_lineage_schema(observed_base_fields(
                base_field_observations,
                base_ref,
            )?);
            let lineage = analysis
                .refresh_input
                .projection_schema_lineage_facts(SqlMvLineageScope::WholeQuery, &lineage_schema)?;
            let (base_fields, output) = persist_sql_mv_projection_lineage(lineage);
            Ok(NonBranchContractCore {
                contract_version: 1,
                bases: vec![base_contract(
                    base_ref,
                    observed_base(base_field_observations, base_ref)?,
                    None,
                    base_fields,
                )],
                output,
                join: None,
                aggregate: None,
            })
        }
        // Two-table inner equi-join projection / filter (legacy
        // JoinProjectionFilter), or — when an aggregate sits over it — the
        // join half of a JoinAggregate. The aggregate is layered on by the
        // GroupRowId arm below.
        TargetIdentity::JoinRowKey(_, _) => {
            let join_aliases = extract_join_aliases(query)?;
            let (left_contract, right_contract, output, join) =
                build_join_base_contracts_and_lineage(
                    &join_aliases,
                    analysis,
                    SqlMvLineageScope::WholeQuery,
                    base_refs,
                    base_field_observations,
                )?;
            Ok(NonBranchContractCore {
                contract_version: 2,
                bases: vec![left_contract, right_contract],
                output,
                join: Some(join),
                aggregate: None,
            })
        }
        // Aggregate group row, dispatched by what it sits over (legacy
        // SingleAggregate / JoinAggregate / FanInAggregate).
        TargetIdentity::GroupRowId(_) => {
            build_aggregate_contract_core(
                query,
                analysis,
                SqlMvAggregateLayoutScope::WholeQuery,
                base_refs,
                base_field_observations,
                target_observation,
            )
        }
        // `build_non_branch_contract_core` is only called for non-branch
        // identities (the branch top is handled separately).
        TargetIdentity::BranchScoped(_) => Err(
            "iceberg MV schema contract internal error: branch-scoped identity in non-branch builder"
                .to_string(),
        ),
    }
}

/// Whether an aggregate query's FROM clause is a fan-in UNION ALL subquery.
///
/// FROM-side complement to [`extract_aggregate_sql_calls`] for distinguishing a
/// fan-in aggregate from a single-scan aggregate. A fan-in FROM is exactly one
/// relation, no joins, a non-lateral derived subquery whose body is a `UNION ALL`
/// set operation.
fn from_clause_is_fan_in_union(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    if !from.joins.is_empty() {
        return false;
    }
    let sqlparser::ast::TableFactor::Derived {
        lateral, subquery, ..
    } = &from.relation
    else {
        return false;
    };
    if *lateral {
        return false;
    }
    matches!(
        subquery.body.as_ref(),
        sqlparser::ast::SetExpr::SetOperation { .. }
    )
}

fn from_clause_is_direct_inner_on_join(query: &sqlparser::ast::Query) -> bool {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    let [join] = from.joins.as_slice() else {
        return false;
    };
    matches!(
        join.join_operator,
        sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(_))
            | sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(_))
    )
}

fn validate_composed_aggregate_fallback_query(query: &sqlparser::ast::Query) -> Result<(), String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("composed aggregate fallback requires a plain SELECT body".to_string());
    };
    if select.from.len() != 1 {
        return Err(
            "composed aggregate fallback requires a single direct FROM join tree".to_string(),
        );
    }
    let from = &select.from[0];
    if from.joins.is_empty() {
        return Err(
            "composed aggregate fallback requires the aggregate input to be a direct join tree"
                .to_string(),
        );
    }
    validate_composed_aggregate_table_factor(&from.relation)?;
    for join in &from.joins {
        validate_composed_aggregate_join_operator(&join.join_operator)?;
        validate_composed_aggregate_table_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_composed_aggregate_table_factor(
    factor: &sqlparser::ast::TableFactor,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table { .. } => Ok(()),
        sqlparser::ast::TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_composed_aggregate_table_factor(&table_with_joins.relation)?;
            if table_with_joins.joins.is_empty() {
                return Err(
                    "composed aggregate fallback nested join must contain at least one join"
                        .to_string(),
                );
            }
            for join in &table_with_joins.joins {
                validate_composed_aggregate_join_operator(&join.join_operator)?;
                validate_composed_aggregate_table_factor(&join.relation)?;
            }
            Ok(())
        }
        _ => Err(
            "composed aggregate fallback supports only direct base-table joins, not subqueries or table functions"
                .to_string(),
        ),
    }
}

fn validate_composed_aggregate_join_operator(
    operator: &sqlparser::ast::JoinOperator,
) -> Result<(), String> {
    use sqlparser::ast::{JoinConstraint, JoinOperator};

    match operator {
        JoinOperator::Join(JoinConstraint::On(_)) | JoinOperator::Inner(JoinConstraint::On(_)) => {
            Ok(())
        }
        JoinOperator::CrossJoin(JoinConstraint::None) => Ok(()),
        _ => Err(
            "composed aggregate fallback supports only direct INNER JOIN ... ON predicates or CROSS JOIN"
                .to_string(),
        ),
    }
}

fn mixed_output_contract(output_columns: &[SqlMvOutputColumnFacts]) -> mv_schema::OutputContract {
    mv_schema::OutputContract {
        columns: output_columns
            .iter()
            .map(|_| mv_schema::OutputColumnLineage {
                expression: mv_schema::ExpressionLineage {
                    kind: mv_schema::ExpressionKind::Mixed,
                    referenced_base_field_ids: Vec::new(),
                    referenced_base_fields: Vec::new(),
                },
            })
            .collect(),
        filter: None,
    }
}

/// Build the aggregate-group-row contract core, dispatching on whether the
/// aggregate sits over a single scan, a join, or a fan-in union. Reproduces the
/// legacy SingleAggregate / JoinAggregate / FanInAggregate arms.
fn build_aggregate_contract_core(
    query: &sqlparser::ast::Query,
    analysis: &crate::mv::analysis::MvAnalysis,
    aggregate_layout_scope: SqlMvAggregateLayoutScope,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target_observation: &MvTargetCreationObservation,
) -> Result<NonBranchContractCore, String> {
    // SQL derives aggregate calls, visible output facts, and argument types
    // from one matching admitted/analyzed query. Core receives only that
    // immutable layout input.
    let aggregate_layout_facts = analysis
        .refresh_input
        .aggregate_layout_facts(query, aggregate_layout_scope)?;
    let layout =
        crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_from_sql_facts(
            &aggregate_layout_facts,
        )?;

    // Dispatch on the FROM structure rather than the legacy classifier:
    //   * a two-table inner equi-join FROM    -> JoinAggregate core
    //   * a fan-in UNION ALL subquery in FROM -> FanInAggregate core
    //   * a single scan                       -> SingleAggregate core
    // SQL derives join lineage (predicate field-ids, output/filter lineage, and
    // per-base narrowing) from the opaque analyzed input; the join-alias
    // extractor supplies only immutable syntax facts.
    if from_clause_is_direct_inner_on_join(query) {
        let join_aliases = extract_join_aliases(query)?;
        // Aggregate over a two-table inner equi-join (legacy JoinAggregate).
        let (left_contract, right_contract, output, join) = build_join_base_contracts_and_lineage(
            &join_aliases,
            analysis,
            match aggregate_layout_scope {
                SqlMvAggregateLayoutScope::WholeQuery => SqlMvLineageScope::WholeQuery,
                SqlMvAggregateLayoutScope::FirstUnionBranch => SqlMvLineageScope::FirstUnionBranch,
            },
            base_refs,
            base_field_observations,
        )?;
        return Ok(NonBranchContractCore {
            contract_version: 3,
            bases: vec![left_contract, right_contract],
            output,
            join: Some(join),
            aggregate: Some(aggregate_contract(&layout, target_observation)?),
        });
    }

    if from_clause_is_fan_in_union(query) {
        // Aggregate over a fan-in UNION ALL (legacy FanInAggregate).
        //
        // The degenerate fan-in over the SAME physical table more than once
        // (e.g. `FROM (SELECT .. FROM ice.s.t UNION ALL SELECT .. FROM
        // ice.s.t)`) dedups to a single resolved base, but is already rejected
        // upstream of this builder by
        // `RefreshFragmentProperty::into_refresh_contract` →
        // `validate_distinct_base_ref_arity` (which requires the distinct base
        // count to equal the fan-in branch count). So by the time the schema
        // contract is built the fan-in base set is guaranteed distinct; the
        // `validate_aggregate_fan_in_base_refs` check below is the
        // schema-contract-side restatement of that invariant.
        validate_aggregate_fan_in_base_refs(base_refs)?;
        let bases = base_refs
            .iter()
            .map(|base_ref| {
                Ok(base_contract(
                    base_ref,
                    observed_base(base_field_observations, base_ref)?,
                    None,
                    base_fields_from_observation(observed_base_fields(
                        base_field_observations,
                        base_ref,
                    )?),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(NonBranchContractCore {
            contract_version: 3,
            bases,
            output: mv_schema::OutputContract {
                // Precise branch-aware output lineage for aggregate fan-in is not
                // available yet. Keep full base schemas and mark outputs as mixed so
                // refresh validates base schema compatibility conservatively.
                columns: analysis
                    .output_columns
                    .iter()
                    .map(|_| mv_schema::OutputColumnLineage {
                        expression: mv_schema::ExpressionLineage {
                            kind: mv_schema::ExpressionKind::Mixed,
                            referenced_base_field_ids: Vec::new(),
                            referenced_base_fields: Vec::new(),
                        },
                    })
                    .collect(),
                filter: None,
            },
            join: None,
            aggregate: Some(aggregate_contract(&layout, target_observation)?),
        })
    } else if base_refs.len() > 1 {
        validate_composed_aggregate_fallback_query(query)?;
        let bases = base_refs
            .iter()
            .map(|base_ref| {
                Ok(base_contract(
                    base_ref,
                    observed_base(base_field_observations, base_ref)?,
                    None,
                    base_fields_from_observation(observed_base_fields(
                        base_field_observations,
                        base_ref,
                    )?),
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(NonBranchContractCore {
            contract_version: 3,
            bases,
            output: mixed_output_contract(&analysis.output_columns),
            join: None,
            aggregate: Some(aggregate_contract(&layout, target_observation)?),
        })
    } else {
        // Aggregate directly over a single scan (legacy SingleAggregate).
        let [base_ref] = base_refs else {
            return Err(
                "aggregate iceberg MV schema contract requires one loaded base".to_string(),
            );
        };
        let lineage_schema =
            sql_mv_lineage_schema(observed_base_fields(base_field_observations, base_ref)?);
        let lineage_scope = match aggregate_layout_scope {
            SqlMvAggregateLayoutScope::WholeQuery => SqlMvLineageScope::WholeQuery,
            SqlMvAggregateLayoutScope::FirstUnionBranch => SqlMvLineageScope::FirstUnionBranch,
        };
        let lineage = analysis
            .refresh_input
            .projection_schema_lineage_facts(lineage_scope, &lineage_schema)?;
        let (base_fields, output) = persist_sql_mv_projection_lineage(lineage);
        Ok(NonBranchContractCore {
            contract_version: 3,
            bases: vec![base_contract(
                base_ref,
                observed_base(base_field_observations, base_ref)?,
                None,
                base_fields,
            )],
            output,
            join: None,
            aggregate: Some(aggregate_contract(&layout, target_observation)?),
        })
    }
}

/// Build the left/right base contracts (with join lineage field narrowing) and
/// the join contract for a two-table inner equi-join over opaque SQL analysis.
/// Shared by the JoinProjectionFilter and JoinAggregate cores and by a composed
/// join-aggregate branch.
fn build_join_base_contracts_and_lineage(
    join_aliases: &SqlMvJoinAliases,
    analysis: &crate::mv::analysis::MvAnalysis,
    lineage_scope: SqlMvLineageScope,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
) -> Result<
    (
        mv_schema::BaseContract,
        mv_schema::BaseContract,
        mv_schema::OutputContract,
        mv_schema::JoinContract,
    ),
    String,
> {
    let left_ref = base_ref_for_table_fqn(base_refs, &join_aliases.left_table)?;
    let right_ref = base_ref_for_table_fqn(base_refs, &join_aliases.right_table)?;
    let left_schema =
        sql_mv_lineage_schema(observed_base_fields(base_field_observations, left_ref)?);
    let right_schema =
        sql_mv_lineage_schema(observed_base_fields(base_field_observations, right_ref)?);
    let lineage_aliases = SqlMvJoinAliases {
        left_table: left_ref.fqn(),
        left_alias: join_aliases.left_alias.clone(),
        right_table: right_ref.fqn(),
        right_alias: join_aliases.right_alias.clone(),
    };
    // SQL owns alias interpretation, predicate normalization, and the
    // analyzed tree. Core retains provider observations and maps only the
    // immutable lineage values into its persisted schema contract.
    let join_lineage = analysis.refresh_input.join_schema_lineage_facts(
        lineage_scope,
        &lineage_aliases,
        &left_schema,
        &right_schema,
    )?;
    let left_contract = base_contract(
        left_ref,
        observed_base(base_field_observations, left_ref)?,
        Some(join_aliases.left_alias.clone()),
        persist_sql_mv_base_fields(join_lineage.left_base_fields()),
    );
    let right_contract = base_contract(
        right_ref,
        observed_base(base_field_observations, right_ref)?,
        Some(join_aliases.right_alias.clone()),
        persist_sql_mv_base_fields(join_lineage.right_base_fields()),
    );
    let output = persist_sql_mv_output_contract(join_lineage.output());
    let join = persist_sql_mv_join_contract(&join_lineage);
    Ok((left_contract, right_contract, output, join))
}

/// Build a UNION ALL schema contract: build the first branch's inner core,
/// widen the base set to every branch's bases (full schema, overlaying the
/// first branch's narrowed bases), and attach the [`BranchUnionContract`].
///
/// `inner` is the per-branch identity (already peeled from the `BranchScoped`
/// wrapper). The first branch may be a projection/filter, a simple aggregate,
/// an aggregate over a join, or a fan-in aggregate; the inner core is built by
/// recursing through the non-branch dispatch over the first branch's own query.
fn build_branch_union_schema_contract(
    inner: &TargetIdentity,
    canonical_query: &sqlparser::ast::Query,
    analysis: &crate::mv::analysis::MvAnalysis,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target_observation: &MvTargetCreationObservation,
    target: mv_schema::TargetContract,
) -> Result<mv_schema::MvSchemaContract, String> {
    let branch_id_field_id = target_field_id_by_column(target_observation, BRANCH_ID_COLUMN_NAME)?;
    let branch_count = union_branch_count(canonical_query);

    // Full cross-branch base set (every branch's bases, full schema).
    let all_bases = base_refs
        .iter()
        .map(|base_ref| {
            Ok(base_contract(
                base_ref,
                observed_base(base_field_observations, base_ref)?,
                None,
                base_fields_from_observation(observed_base_fields(
                    base_field_observations,
                    base_ref,
                )?),
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if all_bases.is_empty() {
        return Err("UNION ALL iceberg MV schema contract requires loaded bases".to_string());
    }

    let mut contract = match inner {
        // UNION ALL of projection/filter branches (legacy UnionProjectionFilter).
        // Output lineage is taken from the whole query (falling back to the
        // first branch), and every base keeps its full schema — byte-identical
        // to the legacy build.
        TargetIdentity::BaseRowId => {
            // Resolve the FIRST branch's base table by name (mirroring the
            // legacy `loaded_base_for_shape_table(.., first_branch.base_table)`)
            // so the lineage schema is the first branch's, even if the loaded
            // bases are not in branch order. The single-scan base FQN is sourced
            // from the focused FROM extractor (the projection/filter branch has
            // neither aggregate nor join), byte-identical to the legacy
            // `ProjectionFilterMvShape.base_table`.
            let first_branch_ast = first_union_branch_ast_query(canonical_query)?;
            let first_branch_base_table = extract_single_scan_table_fqn(&first_branch_ast)?;
            let first_base_ref = base_ref_for_table_fqn(base_refs, &first_branch_base_table)?;
            let first_schema = sql_mv_lineage_schema(observed_base_fields(
                base_field_observations,
                first_base_ref,
            )?);
            let lineage = analysis.refresh_input.projection_schema_lineage_facts(
                SqlMvLineageScope::WholeQueryOrFirstUnionBranch,
                &first_schema,
            )?;
            let (_, output) = persist_sql_mv_projection_lineage(lineage);
            let base = all_bases.first().cloned().expect("non-empty checked above");
            mv_schema::MvSchemaContract {
                contract_version: 1,
                base,
                bases: all_bases,
                output,
                join: None,
                aggregate: None,
                branch: None,
                target,
            }
        }
        // UNION ALL of aggregate branches. Both the simple BranchUnionAggregate
        // shape (a UNION ALL of GROUP BY aggregates over scans) and a HOMOGENEOUS
        // composed branch union (a UNION ALL of `Agg(a JOIN b)` / `Agg(fan-in)`)
        // reach here: `into_refresh_contract` accepts both. Build the first
        // branch's aggregate core, then overlay its narrowed bases onto the full
        // cross-branch base set.
        TargetIdentity::GroupRowId(_) => {
            // First-branch lineage is representative under the RETAINED homogeneity
            // gate in `derive_from_set_operation`: every branch shares the same
            // distinct base set, top-level join key count, fan-in branch count, and
            // group-key layout, so the aggregate `bases`/`join`/group-key lineage
            // built from the FIRST branch describes every branch. (`build_aggregate_contract_core`
            // already handles a composed first branch — a `JoinAggregate` builds a
            // two-base join core, a fan-in `Aggregate` builds a multi-base core —
            // and `first_branch_loaded_bases` returns the branch's full base set.)
            // Per-branch lineage (e.g. a `Vec<BranchContract>`) is only needed if
            // the homogeneity gate is ever lifted to admit heterogeneous-base
            // composed unions; refresh does not consume branch lineage, so none is
            // persisted here.
            let first_branch_ast = first_union_branch_ast_query(canonical_query)?;
            // Only the first branch's bases are loaded for the inner core; pick
            // them out of the full set so the core builder's single/two-base
            // expectations hold.
            let first_branch_refs = first_branch_base_refs(&first_branch_ast, base_refs)?;
            let core = build_aggregate_contract_core(
                &first_branch_ast,
                analysis,
                SqlMvAggregateLayoutScope::FirstUnionBranch,
                &first_branch_refs,
                base_field_observations,
                target_observation,
            )?;
            let bases = overlay_narrowed_bases(all_bases, core.bases);
            let base = bases.first().cloned().ok_or_else(|| {
                "UNION ALL aggregate iceberg MV schema contract requires loaded bases".to_string()
            })?;
            mv_schema::MvSchemaContract {
                contract_version: core.contract_version,
                base,
                bases,
                output: core.output,
                join: core.join,
                aggregate: core.aggregate,
                branch: None,
                target,
            }
        }
        // Homogeneity (set-op synthesis) pins every branch to the same inner
        // identity kind, so a `BranchScoped` whose inner is itself a join or a
        // nested branch never reaches CREATE — the property's narrowing rejects
        // it. Treat anything else as an internal inconsistency.
        other => {
            return Err(format!(
                "iceberg MV UNION ALL schema contract does not support per-branch identity {other:?}"
            ));
        }
    };

    let inner_apply_key_source = match inner {
        TargetIdentity::BaseRowId => SqlMvApplyKeySourceFacts::BaseRowId,
        TargetIdentity::GroupRowId(_) => SqlMvApplyKeySourceFacts::GroupRowId,
        other => {
            return Err(format!(
                "iceberg MV UNION ALL branch inner apply key undefined for identity {other:?}"
            ));
        }
    };
    contract.branch = Some(mv_schema::BranchUnionContract {
        branch_id_column: mv_schema::BranchIdColumnContract {
            column_name: BRANCH_ID_COLUMN_NAME.into(),
            target_field_id: branch_id_field_id,
        },
        branch_count,
        inner_apply_key_source: inner_apply_key_source.into(),
    });
    Ok(contract)
}

/// Number of UNION ALL branches in `query`, counted off the AST so the build
/// does not depend on a top-level classified shape.
fn union_branch_count(query: &sqlparser::ast::Query) -> u32 {
    fn count(body: &sqlparser::ast::SetExpr) -> u32 {
        match body {
            sqlparser::ast::SetExpr::SetOperation { left, right, .. } => count(left) + count(right),
            sqlparser::ast::SetExpr::Query(inner) => count(inner.body.as_ref()),
            _ => 1,
        }
    }
    count(query.body.as_ref())
}

/// The (lower-cased) base-table FQNs referenced by a single UNION ALL branch
/// query, sourced from the focused FROM extractors. A single branch is one of:
/// single-scan projection/filter or aggregate, a two-table join, or a fan-in
/// aggregate over a UNION ALL of single scans:
///   * a two-table inner equi-join FROM    -> [left_table, right_table]
///   * a fan-in UNION ALL subquery in FROM -> one FQN per union branch
///   * a single scan                       -> [the single FROM table]
fn branch_base_table_fqns(branch_query: &sqlparser::ast::Query) -> Result<Vec<String>, String> {
    if let Ok(join_aliases) = extract_join_aliases(branch_query) {
        return Ok(vec![join_aliases.left_table, join_aliases.right_table]);
    }

    // Fan-in aggregate: collect the single-scan base table of each UNION ALL
    // branch inside the FROM subquery. `from_clause_is_fan_in_union` confirms
    // the fan-in FROM shape; the branch base tables come from flattening that
    // subquery's branches.
    if from_clause_is_fan_in_union(branch_query) {
        let sqlparser::ast::SetExpr::Select(select) = branch_query.body.as_ref() else {
            return Err("UNION ALL branch fan-in requires a SELECT body".to_string());
        };
        let [from] = select.from.as_slice() else {
            return Err("UNION ALL branch fan-in requires a single FROM relation".to_string());
        };
        let sqlparser::ast::TableFactor::Derived { subquery, .. } = &from.relation else {
            return Err("UNION ALL branch fan-in requires a derived FROM subquery".to_string());
        };
        let mut branch_bodies = Vec::new();
        flatten_union_all_branches(subquery.body.as_ref(), &mut branch_bodies);
        return branch_bodies
            .into_iter()
            .map(|body| {
                let branch = wrap_set_expr_as_query(branch_query, body);
                extract_single_scan_table_fqn(&branch)
            })
            .collect();
    }

    // Single scan (projection/filter or aggregate over one table).
    Ok(vec![extract_single_scan_table_fqn(branch_query)?])
}

/// Flatten a (possibly nested) UNION ALL set-operation body into its leaf
/// branch bodies without re-validating the UNION ALL operator (the fan-in shape
/// is already confirmed by the caller).
fn flatten_union_all_branches<'a>(
    body: &'a sqlparser::ast::SetExpr,
    out: &mut Vec<&'a sqlparser::ast::SetExpr>,
) {
    match body {
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            flatten_union_all_branches(left, out);
            flatten_union_all_branches(right, out);
        }
        sqlparser::ast::SetExpr::Query(inner) => {
            flatten_union_all_branches(inner.body.as_ref(), out)
        }
        other => out.push(other),
    }
}

/// Wrap a `SetExpr` branch body as a standalone `Query`, inheriting the outer
/// query's non-body fields.
fn wrap_set_expr_as_query(
    outer: &sqlparser::ast::Query,
    body: &sqlparser::ast::SetExpr,
) -> sqlparser::ast::Query {
    let mut query = outer.clone();
    query.body = Box::new(body.clone());
    query
}

/// The subset of `base_refs` referenced by `branch_query`, preserving the
/// resolved order. Used to feed only a branch's own bases into the non-branch
/// core builder.
fn first_branch_base_refs(
    branch_query: &sqlparser::ast::Query,
    base_refs: &[TableIdentity],
) -> Result<Vec<TableIdentity>, String> {
    let branch_table_fqns = branch_base_table_fqns(branch_query)?
        .into_iter()
        .map(|fqn| fqn.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    Ok(base_refs
        .iter()
        .filter(|base_ref| branch_table_fqns.contains(&base_ref.fqn().to_ascii_lowercase()))
        .cloned()
        .collect())
}

/// Overlay branch-local narrowed base contracts onto the full cross-branch base
/// set, replacing each full base whose fqn matches a narrowed base. Bases not
/// touched by the branch keep their full schema. This reproduces the legacy
/// branch-aggregate base narrowing (only the first branch's base(s) narrowed)
/// while generalizing to composed branches (a join branch narrows two bases).
fn overlay_narrowed_bases(
    mut all_bases: Vec<mv_schema::BaseContract>,
    narrowed: Vec<mv_schema::BaseContract>,
) -> Vec<mv_schema::BaseContract> {
    for narrow in narrowed {
        if let Some(slot) = all_bases
            .iter_mut()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(&narrow.table_fqn))
        {
            slot.schema_at_create.fields = narrow.schema_at_create.fields;
            slot.alias_at_create = narrow.alias_at_create;
        }
    }
    all_bases
}

fn base_ref_for_table_fqn<'a>(
    base_refs: &'a [TableIdentity],
    table_fqn: &str,
) -> Result<&'a TableIdentity, String> {
    base_refs
        .iter()
        .find(|base_ref| base_ref.fqn().eq_ignore_ascii_case(table_fqn))
        .ok_or_else(|| format!("join MV shape base {table_fqn} was not resolved"))
}

fn base_contract(
    base_ref: &TableIdentity,
    observation: &crate::mv::storage_observation::MvSchemaValidationObservation,
    alias_at_create: Option<String>,
    fields: Vec<mv_schema::BaseFieldRecord>,
) -> mv_schema::BaseContract {
    mv_schema::BaseContract {
        table_fqn: base_ref.fqn(),
        table_uuid: observation.table_uuid().to_string(),
        alias_at_create,
        schema_id_at_create: observation.schema_id(),
        schema_at_create: mv_schema::BaseSchemaSnapshot { fields },
    }
}

fn base_fields_from_observation(
    fields: &[crate::mv::storage_observation::MvObservedTargetField],
) -> Vec<mv_schema::BaseFieldRecord> {
    fields
        .iter()
        .map(|field| mv_schema::BaseFieldRecord {
            field_id: field.field_id,
            name_at_create: field.name.clone(),
            type_signature: field.type_signature.clone(),
            required: !field.nullable,
        })
        .collect()
}

/// Neutral schema observation for `base_ref`.
///
/// Fails closed: a base the caller did not observe is a programming error, not
/// a reason to fall back to reading provider metadata.
fn observed_base<'a>(
    base_field_observations: &'a std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    base_ref: &TableIdentity,
) -> Result<&'a crate::mv::storage_observation::MvSchemaValidationObservation, String> {
    base_field_observations.get(&base_ref.fqn()).ok_or_else(|| {
        format!(
            "MV base {} was not observed before contract build",
            base_ref.fqn()
        )
    })
}

fn observed_base_fields<'a>(
    base_field_observations: &'a std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    base_ref: &TableIdentity,
) -> Result<&'a [crate::mv::storage_observation::MvObservedTargetField], String> {
    Ok(observed_base(base_field_observations, base_ref)?.fields())
}

/// Project provider schema metadata into immutable SQL facade input. The SQL
/// package receives no provider handle and Core retains the observation used
/// to build the persisted base contract.
fn sql_mv_lineage_schema(
    fields: &[crate::mv::storage_observation::MvObservedTargetField],
) -> SqlMvObservedSchemaFacts {
    SqlMvObservedSchemaFacts::new(
        fields
            .iter()
            .map(|field| {
                SqlMvObservedFieldFacts::new(
                    field.field_id,
                    field.name.clone(),
                    field.type_signature.clone(),
                    !field.nullable,
                )
            })
            .collect(),
    )
}

fn persist_sql_mv_base_fields(
    fields: &[SqlMvObservedFieldFacts],
) -> Vec<mv_schema::BaseFieldRecord> {
    fields
        .iter()
        .map(|field| mv_schema::BaseFieldRecord {
            field_id: field.field_id(),
            name_at_create: field.name_at_create().to_string(),
            type_signature: field.type_signature().to_string(),
            required: field.required(),
        })
        .collect()
}

fn persist_sql_mv_qualified_field(
    field: &novarocks_sql::planning::mv::SqlMvQualifiedFieldLineageFacts,
) -> mv_schema::QualifiedFieldLineage {
    mv_schema::QualifiedFieldLineage {
        table_fqn: field.table_fqn().to_string(),
        qualifier_at_create: field.qualifier_at_create().to_string(),
        field_id: field.field_id(),
    }
}

fn persist_sql_mv_expression_kind(kind: SqlMvExpressionLineageKind) -> mv_schema::ExpressionKind {
    match kind {
        SqlMvExpressionLineageKind::Column => mv_schema::ExpressionKind::Column,
        SqlMvExpressionLineageKind::Cast => mv_schema::ExpressionKind::Cast,
        SqlMvExpressionLineageKind::Func => mv_schema::ExpressionKind::Func,
        SqlMvExpressionLineageKind::Literal => mv_schema::ExpressionKind::Literal,
        SqlMvExpressionLineageKind::Mixed => mv_schema::ExpressionKind::Mixed,
    }
}

fn persist_sql_mv_output_contract(
    output: &novarocks_sql::planning::mv::SqlMvOutputLineageFacts,
) -> mv_schema::OutputContract {
    mv_schema::OutputContract {
        columns: output
            .columns()
            .iter()
            .map(|column| mv_schema::OutputColumnLineage {
                expression: mv_schema::ExpressionLineage {
                    kind: persist_sql_mv_expression_kind(column.kind()),
                    referenced_base_field_ids: column.referenced_base_field_ids().to_vec(),
                    referenced_base_fields: column
                        .referenced_base_fields()
                        .iter()
                        .map(persist_sql_mv_qualified_field)
                        .collect(),
                },
            })
            .collect(),
        filter: output.filter().map(|filter| mv_schema::FilterLineage {
            referenced_base_field_ids: filter.referenced_base_field_ids().to_vec(),
            referenced_base_fields: filter
                .referenced_base_fields()
                .iter()
                .map(persist_sql_mv_qualified_field)
                .collect(),
        }),
    }
}

fn persist_sql_mv_projection_lineage(
    lineage: novarocks_sql::planning::mv::SqlMvProjectionLineageFacts,
) -> (Vec<mv_schema::BaseFieldRecord>, mv_schema::OutputContract) {
    (
        persist_sql_mv_base_fields(lineage.base_fields()),
        persist_sql_mv_output_contract(lineage.output()),
    )
}

fn persist_sql_mv_join_contract(
    join: &novarocks_sql::planning::mv::SqlMvJoinLineageFacts,
) -> mv_schema::JoinContract {
    let kind = match join.kind() {
        SqlMvJoinContractKindFacts::InnerEquiJoin => mv_schema::JoinContractKind::InnerEquiJoin,
    };
    mv_schema::JoinContract {
        kind,
        predicates: join
            .predicates()
            .iter()
            .map(|predicate| mv_schema::JoinPredicateLineage {
                left: persist_sql_mv_qualified_field(predicate.left()),
                right: persist_sql_mv_qualified_field(predicate.right()),
            })
            .collect(),
    }
}

fn target_field_id_by_column(
    target_observation: &MvTargetCreationObservation,
    column_name: &str,
) -> Result<i32, String> {
    target_observation
        .fields
        .iter()
        .find(|field| field.name.eq_ignore_ascii_case(column_name))
        .map(|field| field.field_id)
        .ok_or_else(|| format!("iceberg MV target schema is missing column {column_name}"))
}

fn target_contract(
    analysis: &crate::mv::analysis::MvAnalysis,
    target: &IcebergMvTarget,
    target_observation: &MvTargetCreationObservation,
    actual_apply_key_field_id: i32,
    hidden_apply_key_column_name: &str,
    hidden_apply_key_source: SqlMvApplyKeySourceFacts,
) -> Result<mv_schema::TargetContract, String> {
    Ok(mv_schema::TargetContract {
        table_fqn: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        table_uuid: target_observation.table_uuid.clone(),
        schema_id_at_create: target_observation.schema_id,
        visible_columns: analysis
            .output_columns
            .iter()
            .map(|col| {
                let field = target_observation
                    .fields
                    .iter()
                    .find(|f| f.name.eq_ignore_ascii_case(&col.name))
                    .ok_or_else(|| {
                        format!(
                            "iceberg MV target schema is missing visible output column `{}`",
                            col.name
                        )
                    })?;
                Ok(mv_schema::TargetVisibleColumn {
                    output_name: col.name.clone(),
                    target_field_id: field.field_id,
                    type_signature: field.type_signature.clone(),
                    nullable: field.nullable,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        hidden_apply_key: mv_schema::HiddenApplyKeyContract {
            column_name: hidden_apply_key_column_name.to_string(),
            target_field_id: actual_apply_key_field_id,
            source: hidden_apply_key_source.into(),
        },
        partition: Some(target_observation.partition.clone()),
    })
}

fn aggregate_contract(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
    target_observation: &MvTargetCreationObservation,
) -> Result<mv_schema::AggregateStateContract, String> {
    let state_columns = layout
        .state_columns
        .iter()
        .map(|column| {
            let target_field = target_observation
                .fields
                .iter()
                .find(|field| field.name.eq_ignore_ascii_case(&column.name))
                .ok_or_else(|| {
                    format!(
                        "Iceberg MV target aggregate state column {} is missing from target schema",
                        column.name
                    )
                })?;
            Ok(mv_schema::AggregateStateColumnContract {
                column_name: column.name.clone(),
                target_field_id: target_field.field_id,
                type_signature: target_field.type_signature.clone(),
                nullable: target_field.nullable,
                role: aggregate_state_role_contract(column.state_role),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(mv_schema::AggregateStateContract {
        state_layout_version: 1,
        row_id_column_name: layout.row_id_column.column.name.clone(),
        state_columns,
    })
}

fn aggregate_state_role_contract(
    role: crate::mv::model::AggregateStateRole,
) -> mv_schema::AggregateStateRoleContract {
    match role {
        crate::mv::model::AggregateStateRole::Single => {
            mv_schema::AggregateStateRoleContract::Single
        }
        crate::mv::model::AggregateStateRole::RetractionCount => {
            mv_schema::AggregateStateRoleContract::RetractionCount
        }
    }
}

/// The state an Iceberg MV target restore reads, named explicitly rather than
/// reached through aggregate engine state.
///
/// Same shape and motive as the lake rebuild's context: two inputs, both already
/// reachable from a frontend composition, so restoring targets stops requiring
/// the engine.
pub struct MvTargetRestoreContext<'a> {
    pub connector_control: &'a dyn novarocks_spi::connector::ConnectorControlRegistry,
    pub mv_repository: &'a dyn crate::mv::repository::MvRepository,
}

pub(crate) fn register_iceberg_mv_target_in_catalog(
    ctx: &MvTargetRestoreContext<'_>,
    target: &IcebergMvTarget,
) -> Result<(), String> {
    // SQLX-2 keeps provider tables out of the process-wide planner catalog.
    // Every subsequent query resolves this target through its own admitted
    // binding store. Confirm the catalog's exact generation is published;
    // provider commits own their cache invalidation. Registering or mutating a
    // concrete Core catalog entry here would create a second runtime owner.
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| format!("parse MV target connector identity: {error}"))?;
    novarocks_spi::connector::ConnectorControlResolver::acquire_current(
        ctx.connector_control,
        &instance_id,
    )
    .map_err(|error| format!("acquire MV target connector generation: {error}"))?;
    Ok(())
}

pub fn restore_iceberg_mv_targets(ctx: &MvTargetRestoreContext<'_>) -> Result<(), String> {
    if !ctx.mv_repository.availability().is_available() {
        return Ok(());
    }
    for mv in ctx
        .mv_repository
        .list_definitions()
        .map_err(|e| format!("load MV definitions for iceberg restore failed: {e}"))?
        .into_iter()
        .filter(|mv| {
            mv.storage_engine
                .eq_ignore_ascii_case(MvStorageEngine::Iceberg.as_sql_str())
        })
    {
        let target = IcebergMvTarget {
            catalog: mv
                .target_catalog
                .ok_or_else(|| format!("iceberg MV {} missing target_catalog", mv.mv_id))?,
            namespace: mv
                .target_namespace
                .ok_or_else(|| format!("iceberg MV {} missing target_namespace", mv.mv_id))?,
            table: mv
                .target_table
                .ok_or_else(|| format!("iceberg MV {} missing target_table", mv.mv_id))?,
        };
        register_iceberg_mv_target_in_catalog(ctx, &target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::refresh::apply_key::ApplyKeyValueType;
    use crate::mv::refresh::capabilities::PartitionPruningPolicy;
    use arrow::datatypes::DataType;
    use std::cell::Cell;

    #[test]
    fn retained_repartition_target_identity_matches_exact_target() {
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_sales".to_string(),
        };
        let identity = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            namespace: Arc::from("analytics"),
            table: Arc::from("mv_sales"),
        };

        validate_retained_target_identity(&target, &identity).expect("matching identity");
    }

    #[test]
    fn retained_repartition_target_identity_rejects_drift() {
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_sales".to_string(),
        };
        let identity = ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
            namespace: Arc::from("analytics"),
            table: Arc::from("mv_sales_recreated"),
        };

        let error = validate_retained_target_identity(&target, &identity)
            .expect_err("drifted identity must fail closed");
        assert!(error.contains("does not match ice.analytics.mv_sales"));
    }

    #[test]
    fn retained_repartition_target_handle_skips_latest_reload() {
        let handle = novarocks_spi::connector::ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("ice").expect("instance"),
            bytes::Bytes::from_static(b"retained-table-metadata"),
        )
        .expect("retained table handle");
        let reload_called = Cell::new(false);

        let selected = select_retained_target_handle(Some(&handle), || {
            reload_called.set(true);
            Err("latest metadata reload must not run".to_string())
        })
        .expect("select retained target handle");

        assert_eq!(selected, handle);
        assert!(!reload_called.get());
    }

    #[test]
    fn aggregate_incremental_inserts_use_row_delta() {
        assert!(matches!(
            non_join_incremental_write_mode(true, false),
            crate::mv::application::MvIncrementalWriteMode::RowDelta
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, false),
            crate::mv::application::MvIncrementalWriteMode::FastAppend
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, true),
            crate::mv::application::MvIncrementalWriteMode::RowDelta
        ));
    }
    #[test]
    fn explain_refresh_full_guard_rejects_full_with_disabled_message() {
        let err = super::explain_refresh_full_guard(true).unwrap_err();
        assert!(
            err.contains(concat!("currently disabled", " pending redesign")),
            "EXPLAIN REFRESH FULL must align with the exec-side disabled message, got: {err}"
        );
        assert!(
            !err.contains("not supported"),
            "stale 'not supported' wording must be gone: {err}"
        );
        assert!(super::explain_refresh_full_guard(false).is_ok());
    }

    #[test]
    fn imv_change_stream_effect_set_can_include_zero_row_route() {
        let effects = [
            novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
        ];
        assert!(
            effects
                .iter()
                .any(|effect| *effect
                    == novarocks_spi::connector::ConnectorRowMutationEffect::Delete)
        );
        assert!(
            effects
                .iter()
                .any(|effect| *effect
                    == novarocks_spi::connector::ConnectorRowMutationEffect::Replace)
        );
    }

    #[test]
    fn join_base_refs_for_schema_contract_uses_join_lineage_order() {
        let left = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
        };
        let right = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "dim".to_string(),
        };
        let base_refs = vec![right.clone(), left.clone()];
        let contract = test_join_projection_schema_contract(&left.fqn(), &right.fqn());

        let (actual_left, actual_right) = join_base_refs_for_schema_contract(&contract, &base_refs)
            .expect("schema-contract join base refs");

        assert_eq!(actual_left.fqn(), left.fqn());
        assert_eq!(actual_right.fqn(), right.fqn());
    }

    fn test_join_projection_schema_contract(
        left_fqn: &str,
        right_fqn: &str,
    ) -> mv_schema::MvSchemaContract {
        use mv_schema::{
            HiddenApplyKeyContract, JoinContract, JoinContractKind, JoinPredicateLineage,
            MvSchemaContract, OutputContract, QualifiedFieldLineage, TargetContract,
        };

        MvSchemaContract {
            contract_version: 2,
            base: test_base_contract(left_fqn),
            bases: vec![test_base_contract(left_fqn), test_base_contract(right_fqn)],
            output: OutputContract {
                columns: Vec::new(),
                filter: None,
            },
            join: Some(JoinContract {
                kind: JoinContractKind::InnerEquiJoin,
                predicates: vec![JoinPredicateLineage {
                    left: QualifiedFieldLineage {
                        table_fqn: left_fqn.to_string(),
                        qualifier_at_create: "l".to_string(),
                        field_id: 1,
                    },
                    right: QualifiedFieldLineage {
                        table_fqn: right_fqn.to_string(),
                        qualifier_at_create: "r".to_string(),
                        field_id: 1,
                    },
                }],
            }),
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.sales.mv".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 1,
                visible_columns: Vec::new(),
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    target_field_id: 1,
                    source: SqlMvApplyKeySourceFacts::JoinRowKey.into(),
                },
                partition: None,
            },
        }
    }

    fn test_base_contract(table_fqn: &str) -> mv_schema::BaseContract {
        mv_schema::BaseContract {
            table_fqn: table_fqn.to_string(),
            table_uuid: format!("{table_fqn}-uuid"),
            alias_at_create: None,
            schema_id_at_create: 1,
            schema_at_create: mv_schema::BaseSchemaSnapshot { fields: Vec::new() },
        }
    }

    fn parse_select_query(sql: &str) -> sqlparser::ast::Query {
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!("expected SELECT");
        };
        *q
    }

    #[test]
    fn iceberg_join_mv_uses_join_apply_key_column() {
        let column = crate::mv::refresh::target_apply::join_apply_key_table_column();
        assert_eq!(column.name, JOIN_APPLY_KEY_COLUMN_NAME);
    }

    #[test]
    fn create_apply_key_metadata_comes_from_refresh_contract() {
        use crate::mv::refresh::apply_key::ApplyKeyContract;

        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::projection_filter()),
            SqlMvApplyKeySourceFacts::BaseRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_projection_filter()),
            SqlMvApplyKeySourceFacts::JoinRowKey.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::aggregate_group_row()),
            SqlMvApplyKeySourceFacts::GroupRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_aggregate_group_row()),
            SqlMvApplyKeySourceFacts::GroupRowId.table_property_value()
        );
    }

    #[test]
    fn repartition_support_accepts_projection_filter_and_aggregate() {
        let projection = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            has_agg_state: false,
            identity: RefreshIdentity::BaseRowId,
            apply_key_column: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Int64,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&projection).expect("projection/filter support"),
            RepartitionShape::ProjectionFilterSingleBase
        );

        let aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::SingleBase,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&aggregate).expect("aggregate support"),
            RepartitionShape::AggregateSingleBase
        );
    }

    #[test]
    fn repartition_support_accepts_join_projection_filter() {
        let join = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            has_agg_state: false,
            identity: RefreshIdentity::JoinRowKey,
            apply_key_column: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&join).expect("join projection/filter support"),
            RepartitionShape::JoinProjectionFilter
        );
    }

    #[test]
    fn repartition_support_accepts_multi_base_shapes() {
        let join_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&join_aggregate).expect("join aggregate support"),
            RepartitionShape::JoinAggregate
        );

        let fan_in_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: true,
            identity: RefreshIdentity::GroupRowId,
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&fan_in_aggregate).expect("fan-in aggregate support"),
            RepartitionShape::FanInAggregate
        );

        let union_projection = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: false,
            identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::BaseRowId)),
            apply_key_column: HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::BranchInt64,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        assert_eq!(
            select_repartition_shape(&union_projection).expect("union projection support"),
            RepartitionShape::UnionProjectionFilter
        );
    }

    #[test]
    fn repartition_support_rejects_specific_unsupported_shape() {
        let invalid = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: false,
            identity: RefreshIdentity::JoinRowKey,
            apply_key_column: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };

        let err = select_repartition_shape(&invalid).expect_err("shape must be rejected");
        assert!(err.contains("UnsupportedRepartitionShape"));
        assert!(err.contains("JoinRowKey"));
        assert!(err.contains("AllBasesRequired"));
        assert!(err.contains("aggregate_state=false"));

        let branch_union_aggregate = RefreshCapabilities {
            snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
            has_agg_state: true,
            identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId)),
            apply_key_column: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            apply_key_value_type: ApplyKeyValueType::BranchUtf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        };
        let err = select_repartition_shape(&branch_union_aggregate)
            .expect_err("branch UNION ALL aggregate repartition is unsupported");
        assert!(err.contains("UnsupportedRepartitionShape"));
        assert!(err.contains("BranchScoped"));
        assert!(err.contains("aggregate_state=true"));
    }

    #[test]
    fn identity_gating_matches_legacy_strategy_gating() {
        use crate::mv::analysis::refresh_property::TargetIdentity;

        let base_row = TargetIdentity::BaseRowId;
        let join_row = TargetIdentity::JoinRowKey(
            Box::new(TargetIdentity::BaseRowId),
            Box::new(TargetIdentity::BaseRowId),
        );
        let group_row = TargetIdentity::GroupRowId(vec!["region".to_string()]);
        let union_proj = TargetIdentity::BranchScoped(Box::new(TargetIdentity::BaseRowId));
        let union_agg = TargetIdentity::BranchScoped(Box::new(group_row.clone()));

        // Physical apply-key column: required for base/join row identities
        // (ProjectionFilter / JoinProjectionFilter / UnionProjectionFilter),
        // not for group-row identities (the aggregate strategies).
        assert!(identity_needs_physical_apply_key_column(&base_row));
        assert!(identity_needs_physical_apply_key_column(&join_row));
        assert!(identity_needs_physical_apply_key_column(&union_proj));
        assert!(!identity_needs_physical_apply_key_column(&group_row));
        assert!(!identity_needs_physical_apply_key_column(&union_agg));

        // Branch id column: required iff the identity top is BranchScoped.
        assert!(!identity_needs_branch_id_column(&base_row));
        assert!(!identity_needs_branch_id_column(&join_row));
        assert!(!identity_needs_branch_id_column(&group_row));
        assert!(identity_needs_branch_id_column(&union_proj));
        assert!(identity_needs_branch_id_column(&union_agg));
    }

    #[test]
    fn refresh_status_uses_base_ref_fqn() {
        let base_ref = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "orders".to_string(),
        };

        let status = base_snapshot_status_for_refresh(&base_ref, Some(10), Some(11));

        assert_eq!(status.fqn, "ice.sales.orders");
        assert_eq!(status.previous_snapshot_id, Some(10));
        assert_eq!(status.current_snapshot_id_before_pin, Some(11));
    }
}

/// Resolve the MV target's neutral binding for a validation-only read.
///
/// Callers that already hold a binding should pass it through instead; this is
/// for sites that still load the target the legacy way and only need facts.
fn target_binding_for(
    source: &dyn IcebergMvRefreshSource,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::refresh::target_binding::MvTargetBinding, String> {
    crate::mv::refresh::target_binding::load_mv_target_binding_with_ports(
        source.connector_control(),
        source.storage_observation(),
        &novarocks_catalog::identifier::TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_neutral_refresh_rewrite_context(
    source: &dyn IcebergMvRefreshSource,
    target: &IcebergMvTarget,
    mv_id: i64,
    current_catalog: Option<&str>,
    current_database: &str,
    definition: Arc<StoredMvDefinition>,
    canonical_query: Arc<sqlparser::ast::Query>,
    base_refs: Arc<[TableIdentity]>,
    pin: Arc<RefreshSnapshotPin>,
    previous_snapshot_ids: BTreeMap<String, i64>,
    previous_table_uuids: BTreeMap<String, String>,
    target_snapshot_id: Option<i64>,
    target_table_uuid: String,
    retained_target_binding: Option<&crate::mv::refresh::target_binding::MvTargetBinding>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<crate::mv::rewrite::context::IcebergMvRewriteContext>, String> {
    let loaded_target_binding;
    let binding = match retained_target_binding {
        Some(binding) => binding,
        None => {
            loaded_target_binding = target_binding_for(source, target, connector_context)?;
            &loaded_target_binding
        }
    };
    if binding.table_uuid() != target_table_uuid {
        return Err(format!(
            "MV refresh target UUID drifted after planning for {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    if binding.current_snapshot_id() != target_snapshot_id {
        return Err(format!(
            "MV refresh target snapshot drifted after planning for {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    }
    let schema_contract = definition.schema_contract.clone().map(Arc::new);
    crate::mv::rewrite::context::IcebergMvRewriteContext::from_parts(
        TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        mv_id,
        current_catalog.map(str::to_string),
        current_database.to_string(),
        definition,
        canonical_query,
        base_refs,
        pin,
        previous_snapshot_ids,
        previous_table_uuids,
        target_snapshot_id,
        target_table_uuid,
        binding.physical_write_schema()?,
        Arc::from(binding.observation().field_ids().to_vec()),
        schema_contract,
    )
    .map(Arc::new)
}

fn observe_schema_validation_for_table(
    source: &dyn IcebergMvRefreshSource,
    table: &TableIdentity,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    observe_schema_validation_for_table_with_parts(
        source.connector_control(),
        source.storage_observation(),
        table,
        connector_context,
    )
}

fn observe_current_refresh_base_with_source(
    source: &dyn IcebergMvRefreshSource,
    table: &TableIdentity,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::storage_observation::MvRefreshBaseObservation, String> {
    crate::mv::refresh_io::observe_current_refresh_base_with_ports(
        source.connector_control(),
        source.storage_observation(),
        table,
        connector_context,
    )
}

fn observe_schema_validation_for_table_with_ports(
    ports: &IcebergMvCorePorts,
    table: &TableIdentity,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    observe_schema_validation_for_table_with_parts(
        ports.connector_control.as_ref(),
        ports.storage_observation.as_ref(),
        table,
        connector_context,
    )
}

fn observe_schema_validation_for_table_with_parts(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    storage_observation
        .observe_schema_validation(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV schema validation facts for {}: {error}",
                table.fqn()
            )
        })
}

fn observe_and_admit_change_window_for_table(
    source: &dyn IcebergMvRefreshSource,
    table: &TableIdentity,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    (
        novarocks_spi::connector::ConnectorChangeWindowAdmission,
        MvSchemaValidationObservation,
    ),
    String,
> {
    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        source.connector_control(),
        &table.catalog,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let window =
        novarocks_spi::connector::ConnectorChangeWindow::new(from_snapshot_id, to_snapshot_id);
    let scan =
        crate::query_execution::planning::catalog_materializer::admit_connector_change_window(
            &metadata.table,
            &metadata.schema,
            &exact_lease,
            connector_context.clone(),
            window,
        )?;
    let novarocks_spi::connector::ConnectorScanAdmission::ChangeWindow(admission) =
        scan.admission()
    else {
        return Err("connector returned a snapshot admission for a change-window scan".to_string());
    };
    let observation = source
        .storage_observation()
        .observe_schema_validation(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV schema validation facts for {}: {error}",
                table.fqn()
            )
        })?;
    Ok((admission.clone(), observation))
}

fn rebind_mv_definition_before_refresh_derivation(
    source: &dyn IcebergMvRefreshSource,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
    retained_target_observation: Option<&MvSchemaValidationObservation>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StoredMvDefinition, IcebergMvRefreshExecutionError> {
    let Some(contract) = mv_definition.schema_contract.as_ref() else {
        return Ok(mv_definition.clone());
    };
    let caps = RefreshCapabilities::from_schema_contract(contract)?;
    let target_ref = TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    match caps.snapshot_policy {
        BaseSnapshotPolicy::SingleBase => {
            let [base_ref] = base_refs else {
                return Err("single-base MV refresh has an invalid base reference set"
                    .to_string()
                    .into());
            };
            let base_observation =
                observe_schema_validation_for_table(source, base_ref, connector_context)?;
            let loaded_target_observation;
            let target_observation = match retained_target_observation {
                Some(observation) => observation,
                None => {
                    loaded_target_observation = observe_schema_validation_for_table(
                        source,
                        &target_ref,
                        connector_context,
                    )?;
                    &loaded_target_observation
                }
            };
            match validate_schema_contract(contract, &base_observation, target_observation) {
                ContractDecision::Incompatible(error) => Err(format!("{error}").into()),
                ContractDecision::CompatibleSafe => Ok(mv_definition.clone()),
                ContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
                    let mut definition = mv_definition.clone();
                    definition.select_sql =
                        rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
                    Ok(definition)
                }
            }
        }
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            let [left_ref, right_ref] = base_refs else {
                return Err("join MV refresh has an invalid base reference set"
                    .to_string()
                    .into());
            };
            let left_observation =
                observe_schema_validation_for_table(source, left_ref, connector_context)?;
            let right_observation =
                observe_schema_validation_for_table(source, right_ref, connector_context)?;
            let loaded_target_observation;
            let target_observation = match retained_target_observation {
                Some(observation) => observation,
                None => {
                    loaded_target_observation = observe_schema_validation_for_table(
                        source,
                        &target_ref,
                        connector_context,
                    )?;
                    &loaded_target_observation
                }
            };
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            let decision = validate_join_schema_contract(
                contract,
                &[
                    (left_fqn.as_str(), left_observation),
                    (right_fqn.as_str(), right_observation),
                ],
                target_observation,
            )
            .map_err(|error| error.to_string())?;
            apply_join_schema_contract_decision(decision, mv_definition).map_err(Into::into)
        }
        BaseSnapshotPolicy::AllBasesRequired => Ok(mv_definition.clone()),
    }
}

fn validate_aggregate_schema_contract_metadata<'a>(
    target: &IcebergMvTarget,
    mv_definition: &'a StoredMvDefinition,
) -> Result<&'a mv_schema::MvSchemaContract, String> {
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if schema_contract.contract_version != 3 {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} requires schema contract version 3, got {}",
            target.catalog, target.namespace, target.table, schema_contract.contract_version
        ));
    }
    if schema_contract.aggregate.is_none() {
        return Err(format!(
            "iceberg aggregate MV {}.{}.{} is missing aggregate schema contract; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(schema_contract)
}

/// A-family `Aggregate(UNION ALL(b1..bn))` refresh execution (fan-in over
/// multiple bases).
///
/// UNION ALL sits BELOW the aggregate, so the same group key folds across
/// branches and the ordinary group-row-id apply key applies — there is no
/// `__branch_id__` (that is the B-family / branch-union concern). The rewrite (`RewriteUnionAggregateDelta` + the aggregate-state
/// stage) and IMV scan binding already fan a per-branch delta window off the
/// multi-base pin, exactly like `refresh_join_aggregate_iceberg_mv` does for
/// its two bases. This orchestration just pins/loads every fan-in base, builds
/// one refresh context over all of them, and drives the shared aggregate merge
/// with one canonical `NonJoinBaseChange` per base.
///
/// Structurally this mirrors `refresh_iceberg_union_projection_mv` (multi-base
/// first/metadata/incremental dispatch) but uses the aggregate contract
/// validators, the aggregate first-refresh path, and the aggregate merge
/// options. Field-id rebind is not supported on the fan-in path yet; a base
/// whose columns were rebound is accepted by the contract check but its SELECT
/// is not rewritten, matching the pre-existing single-base behavior closely
/// enough for the unchanged-schema case this build targets.

/// The per-shape payload distinguishing the two `AllBasesRequired` aggregate
/// refresh variants that share the wrapper [`refresh_fan_in_aggregate_iceberg_mv`].
///
/// This enum is the *identity gate* the folded wrapper dispatches on: the
/// `BranchUnion` variant corresponds to a `BranchScoped` row identity (UNION
/// ALL of aggregate branches), while `FanIn` corresponds to a plain
/// `GroupRowId` aggregate fanning in over a UNION ALL of scans. Both produce an
/// `AllBasesRequired` snapshot policy and an aggregate state contract; only the
/// branch-contract validation and the first-refresh strategy differ.
enum AllBasesAggregateRefresh<'a> {
    /// Aggregate-over-UNION-ALL fan-in: one aggregate above a union of scans.
    /// The aggregate-call surface is sourced from the focused extractor
    /// (`extract_aggregate_sql_calls`), not the legacy classifier.
    FanIn {
        schema_contract: &'a mv_schema::MvSchemaContract,
        aggregate_calls: &'a SqlMvAggregateCalls,
    },
    /// Aggregate over a composed multi-base relation, such as a nested join or
    /// a zero-key CROSS JOIN. The change stream still uses the aggregate
    /// rewrite-merge path; the apply-key evidence decides whether join-delta
    /// proof is required.
    ComposedAggregate {
        schema_contract: &'a mv_schema::MvSchemaContract,
        aggregate_calls: &'a SqlMvAggregateCalls,
    },
    /// UNION ALL of aggregate branches (`BranchScoped` identity): the union sits
    /// above per-branch aggregates and the first refresh injects `__branch_id__`.
    /// The per-branch aggregate-call model is sourced from the focused extractor
    /// (not the legacy classifier), so a composed branch (`Agg(a JOIN b)` /
    /// `Agg(fan-in)`) is supported. `branch_count` is the persisted branch count;
    /// `first_branch_calls` is the first branch's aggregate-call surface, which is
    /// representative of every branch under the CREATE-time homogeneity gate.
    BranchUnion {
        branch_count: usize,
        first_branch_calls: &'a SqlMvAggregateCalls,
    },
}

fn target_fqn_string(target: &IcebergMvTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.namespace, target.table)
}

// Previous implementation of REFRESH FULL — `refresh_full_iceberg_mv` —
// was removed. It dropped the target table + deleted the MV definition +
// re-ran create_iceberg_mv (which leaves the new target empty), and the
// drop and the create were in separate transactions. The user-visible
// outcome was misleading ("MV is now empty" rather than "MV is fully
// repopulated") and the operation could leave behind an inconsistent
// state on partial failure. It also silently dropped partition_by.
//
// Re-introduce only after a redesign that clarifies:
//   - the keyword name (probably REBUILD rather than REFRESH FULL),
//   - atomic drop+create+populate semantics,
//   - a deterministic data-repopulation step,
//   - faithful preservation of the original DDL (partition_by,
//     distribution, properties).
// See the rejection in refresh_iceberg_mv for the user-facing error.

fn unknown_join_affected_partitions() -> crate::mv::model::AffectedTargetPartitions {
    crate::mv::model::AffectedTargetPartitions::not_derived(
        "join MV affected partition planning is not implemented",
    )
}

fn base_snapshot_statuses_for_plan(
    base_refs: &[TableIdentity],
    previous_snapshots: &BTreeMap<String, i64>,
    current_snapshots: &BTreeMap<String, Option<i64>>,
) -> Vec<BaseSnapshotStatus> {
    base_refs
        .iter()
        .map(|base_ref| {
            base_snapshot_status_for_refresh(
                base_ref,
                previous_snapshots.get(&base_ref.fqn()).copied(),
                current_snapshots.get(&base_ref.fqn()).copied().flatten(),
            )
        })
        .collect()
}

fn noop_affected_partitions(
    schema_contract: &mv_schema::MvSchemaContract,
) -> crate::mv::model::AffectedTargetPartitions {
    if is_unpartitioned_mv_contract(schema_contract) {
        crate::mv::model::AffectedTargetPartitions::Unpartitioned
    } else {
        crate::mv::model::AffectedTargetPartitions::known(std::iter::empty::<
            crate::mv::model::MvPartitionKey,
        >())
    }
}

fn merge_affected_partition_results(
    context: &str,
    results: impl IntoIterator<Item = (String, crate::mv::model::AffectedTargetPartitions)>,
) -> crate::mv::model::AffectedTargetPartitions {
    let mut merged = BTreeSet::new();
    let mut saw_unpartitioned = false;

    for (base, result) in results {
        match result {
            crate::mv::model::AffectedTargetPartitions::Known { partitions } => {
                merged.extend(partitions);
            }
            crate::mv::model::AffectedTargetPartitions::Unpartitioned => {
                saw_unpartitioned = true;
            }
            crate::mv::model::AffectedTargetPartitions::NotDerived { reason } => {
                return crate::mv::model::AffectedTargetPartitions::not_derived(format!(
                    "{context}: {base}: {reason}"
                ));
            }
        }
    }

    if saw_unpartitioned {
        if merged.is_empty() {
            crate::mv::model::AffectedTargetPartitions::Unpartitioned
        } else {
            crate::mv::model::AffectedTargetPartitions::not_derived(format!(
                "{context}: mixed unpartitioned and partitioned branch results"
            ))
        }
    } else {
        crate::mv::model::AffectedTargetPartitions::known(merged)
    }
}

fn plan_multi_base_affected_partitions(
    schema_contract: &mv_schema::MvSchemaContract,
    mode: RefreshMode,
    base_refs: &[TableIdentity],
    previous_snapshots: &BTreeMap<String, i64>,
    current_snapshots: &BTreeMap<String, Option<i64>>,
    mut admit_for_base: impl FnMut(
        &TableIdentity,
        i64,
        i64,
    ) -> Result<
        (
            novarocks_spi::connector::ConnectorChangeWindowAdmission,
            MvSchemaValidationObservation,
        ),
        String,
    >,
    context: &str,
) -> crate::mv::model::AffectedTargetPartitions {
    match mode {
        RefreshMode::Noop => noop_affected_partitions(schema_contract),
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_contract(schema_contract) {
                crate::mv::model::AffectedTargetPartitions::Unpartitioned
            } else {
                crate::mv::model::AffectedTargetPartitions::not_derived(format!(
                    "{context}: full refresh affected partition planning is not implemented"
                ))
            }
        }
        RefreshMode::Incremental => {
            if is_unpartitioned_mv_contract(schema_contract) {
                return crate::mv::model::AffectedTargetPartitions::Unpartitioned;
            }

            let results = base_refs.iter().map(|base_ref| {
                let fqn = base_ref.fqn();
                let result = match (
                    previous_snapshots.get(&fqn).copied(),
                    current_snapshots.get(&fqn).copied().flatten(),
                ) {
                    (Some(previous), Some(current)) if previous == current => {
                        crate::mv::model::AffectedTargetPartitions::known(
                            std::iter::empty::<crate::mv::model::MvPartitionKey>(),
                        )
                    }
                    (Some(previous), Some(current)) => {
                        match admit_for_base(base_ref, previous, current) {
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                                _,
                            )) => crate::mv::model::AffectedTargetPartitions::known(
                                std::iter::empty::<crate::mv::model::MvPartitionKey>(),
                            ),
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental {
                                    partition_impact,
                                    ..
                                },
                                observation,
                            )) => crate::mv::partition::planner::plan_affected_partitions(
                                &crate::mv::partition::planner::AffectedPartitionPlanInput {
                                    schema_contract,
                                    partition_impact: Some(&partition_impact),
                                    schema_observation: Some(&observation),
                                },
                            ),
                            Ok((
                                novarocks_spi::connector::ConnectorChangeWindowAdmission::FullRebuild(_),
                                _,
                            )) => crate::mv::model::AffectedTargetPartitions::not_derived(
                                "connector change-window admission requires a full rebuild",
                            ),
                            Err(err) => crate::mv::model::AffectedTargetPartitions::not_derived(
                                format!("failed to admit connector changes for affected partitions: {err}"),
                            ),
                        }
                    }
                    (None, _) => crate::mv::model::AffectedTargetPartitions::not_derived(
                        "incremental affected partition planning missing previous snapshot",
                    ),
                    (_, None) => crate::mv::model::AffectedTargetPartitions::not_derived(
                        "incremental affected partition planning missing current snapshot",
                    ),
                };
                (fqn, result)
            });

            merge_affected_partition_results(context, results)
        }
    }
}

fn plan_aggregate_mv_affected_partitions(
    source: &dyn IcebergMvRefreshSource,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_ref: &TableIdentity,
    schema_contract: &mv_schema::MvSchemaContract,
    mode: RefreshMode,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id: Option<i64>,
) -> crate::mv::model::AffectedTargetPartitions {
    match mode {
        RefreshMode::Noop => noop_affected_partitions(schema_contract),
        RefreshMode::Incremental => {
            if is_unpartitioned_mv_contract(schema_contract) {
                crate::mv::model::AffectedTargetPartitions::Unpartitioned
            } else {
                let Some(previous) = previous_snapshot_id else {
                    return crate::mv::model::AffectedTargetPartitions::not_derived(
                        "incremental aggregate MV affected partition planning missing previous snapshot",
                    );
                };
                let Some(current) = current_snapshot_id else {
                    return crate::mv::model::AffectedTargetPartitions::not_derived(
                        "incremental aggregate MV affected partition planning missing current snapshot",
                    );
                };
                match observe_and_admit_change_window_for_table(
                    source,
                    base_ref,
                    previous,
                    current,
                    connector_context,
                ) {
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                        _,
                    )) => crate::mv::model::AffectedTargetPartitions::known(std::iter::empty::<
                        crate::mv::model::MvPartitionKey,
                    >(
                    )),
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::Incremental {
                            partition_impact,
                            ..
                        },
                        observation,
                    )) => crate::mv::partition::planner::plan_affected_partitions(
                        &crate::mv::partition::planner::AffectedPartitionPlanInput {
                            schema_contract,
                            partition_impact: Some(&partition_impact),
                            schema_observation: Some(&observation),
                        },
                    ),
                    Ok((
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::FullRebuild(_),
                        _,
                    )) => crate::mv::model::AffectedTargetPartitions::not_derived(
                        "connector change-window admission requires a full rebuild",
                    ),
                    Err(err) => crate::mv::model::AffectedTargetPartitions::not_derived(format!(
                        "failed to admit connector changes for affected partitions: {err}"
                    )),
                }
            }
        }
        RefreshMode::Full | RefreshMode::Rebuild => {
            if is_unpartitioned_mv_contract(schema_contract) {
                crate::mv::model::AffectedTargetPartitions::Unpartitioned
            } else {
                crate::mv::partition::planner::plan_affected_partitions(
                    &crate::mv::partition::planner::AffectedPartitionPlanInput {
                        schema_contract,
                        partition_impact: None,
                        schema_observation: None,
                    },
                )
            }
        }
    }
}

fn is_unpartitioned_mv_contract(schema_contract: &mv_schema::MvSchemaContract) -> bool {
    schema_contract
        .target
        .partition
        .as_ref()
        .is_none_or(|partition| partition.fields.is_empty())
}

fn log_planned_iceberg_mv_affected_partitions(
    iceberg_target: &IcebergMvTarget,
    affected_partitions: &crate::mv::model::AffectedTargetPartitions,
) {
    tracing::info!(
        target = %format!(
            "{}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ),
        affected_partitions = ?affected_partitions,
        "planned iceberg MV affected partitions"
    );
}

#[derive(Serialize)]
struct RefreshDefinitionFingerprint<'a> {
    mv_id: i64,
    canonical_select_sql: String,
    canonical_base_refs: BTreeSet<String>,
    storage_engine: &'a str,
    target_catalog: &'a Option<String>,
    target_namespace: &'a Option<String>,
    target_table: &'a Option<String>,
    schema_contract: &'a Option<mv_schema::MvSchemaContract>,
    partition_contract: &'a Option<MvPartitionContract>,
}

fn refresh_execution_definition_fingerprint(
    mv_definition: &StoredMvDefinition,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let canonical_select_sql = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql)?,
        current_catalog,
        current_database,
    )
    .to_string();
    let canonical_base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?
        .into_iter()
        .map(|base_ref| base_ref.fqn())
        .collect::<BTreeSet<_>>();
    let input = RefreshDefinitionFingerprint {
        mv_id: mv_definition.mv_id,
        canonical_select_sql,
        canonical_base_refs,
        storage_engine: &mv_definition.storage_engine,
        target_catalog: &mv_definition.target_catalog,
        target_namespace: &mv_definition.target_namespace,
        target_table: &mv_definition.target_table,
        schema_contract: &mv_definition.schema_contract,
        partition_contract: &mv_definition.partition_spec,
    };
    let canonical = serde_json::to_vec(&input)
        .map_err(|error| format!("encode MV refresh definition fingerprint failed: {error}"))?;
    String::from_utf8(canonical).map_err(|error| {
        format!("encode MV refresh definition fingerprint as UTF-8 failed: {error}")
    })
}

fn build_refresh_state_baseline(
    mv_definition: &StoredMvDefinition,
    target: &crate::mv::refresh::target_binding::MvTargetBinding,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<RefreshStateBaseline, String> {
    Ok(RefreshStateBaseline::SnapshotBacked {
        previous_snapshot_ids: mv_definition.last_refresh_snapshots.clone(),
        previous_table_uuids: mv_definition.last_refresh_table_uuids.clone(),
        target_snapshot_id: target.current_snapshot_id(),
        target_table_uuid: target.table_uuid().to_string(),
        definition_fingerprint: refresh_execution_definition_fingerprint(
            mv_definition,
            current_catalog,
            current_database,
        )?,
    })
}

pub(crate) fn plan_iceberg_mv_refresh_with_connector_context(
    source: &dyn IcebergMvRefreshSource,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    target: MvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let iceberg_target = resolve_refresh_target(current_catalog, current_database, &stmt.name)
        .map_err(RefreshError::user)?;
    if stmt.full {
        return Err(RefreshError::user(FULL_REFRESH_DISABLED_MESSAGE));
    }

    crate::connector::validate_request_context(connector_context)
        .map_err(RefreshError::pre_commit)?;
    // Preparation only observes the currently admitted catalog and MV facts.
    // Historical v1/v2 recovery stays in the legacy execution adapter; a
    // current frontend-owned attempt must never perform recovery before its
    // durable v3 intent exists.
    let mv_definition = load_iceberg_mv_definition_by_target(source, &iceberg_target)
        .map_err(RefreshError::user)?;
    let target_ref = TableIdentity {
        catalog: iceberg_target.catalog.clone(),
        namespace: iceberg_target.namespace.clone(),
        table: iceberg_target.table.clone(),
    };
    validate_target_snapshot(
        &iceberg_target,
        &mv_definition,
        &target_binding_for(source, &iceberg_target, connector_context)
            .map_err(RefreshError::user)?,
    )
    .map_err(RefreshError::user)?;

    let base_refs =
        parse_iceberg_table_refs(&mv_definition.base_table_refs).map_err(RefreshError::user)?;
    let mv_definition = rebind_mv_definition_before_refresh_derivation(
        source,
        &mv_definition,
        &base_refs,
        &iceberg_target,
        None,
        connector_context,
    )
    .map_err(IcebergMvRefreshExecutionError::into_message)
    .map_err(RefreshError::user)?;
    let refresh_state_baseline = build_refresh_state_baseline(
        &mv_definition,
        &target_binding_for(source, &iceberg_target, connector_context)
            .map_err(RefreshError::user)?,
        current_catalog,
        current_database,
    )
    .map_err(RefreshError::user)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql).map_err(RefreshError::user)?,
        current_catalog,
        current_database,
    );
    // Driver dispatch (Phase 3 / B2): plan-side dispatch is capability-driven,
    // matching the execute path.
    let dispatch_schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        RefreshError::user(format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ))
    })?;
    let caps = RefreshCapabilities::from_schema_contract(dispatch_schema_contract)
        .map_err(RefreshError::user)?;
    let is_join = matches!(
        caps.snapshot_policy,
        BaseSnapshotPolicy::JoinPairPartialInitialSkip
    );
    match (caps.has_agg_state, &caps.snapshot_policy, &caps.identity) {
        // UNION ALL of projection/filter branches.
        (false, BaseSnapshotPolicy::AllBasesRequired, _) => {
            // Source branch count from the persisted contract; fall back to
            // counting the AST branches if the branch contract is absent,
            // matching the refresh-dispatch fallback pattern.
            let branch_count = dispatch_schema_contract
                .branch
                .as_ref()
                .map(|b| b.branch_count as usize)
                .unwrap_or_else(|| union_branch_count(&canonical_select_query) as usize);
            return plan_iceberg_union_projection_mv_refresh(
                source,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                branch_count,
                dispatch_schema_contract,
                &connector_context,
            );
        }
        // Aggregate shapes: single-base, fan-in, branch-union, and join
        // aggregate all route through the aggregate planner, which selects the
        // per-shape plan by capability. The branch-union sub-path sources its
        // branch count + first-branch aggregate calls from the focused extractor
        // (not the union classifier), so composed branches are supported.
        (true, _, _) => {
            return plan_iceberg_aggregate_mv_refresh(
                source,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                &caps,
                &canonical_select_query,
                &connector_context,
            );
        }
        // Join / single-base projection-filter: fall through to the inline
        // paths below.
        (false, BaseSnapshotPolicy::JoinPairPartialInitialSkip, _)
        | (false, BaseSnapshotPolicy::SingleBase, _) => {}
    }
    if is_join {
        // The join projection/filter plan path sources the left/right table
        // aliases from the focused join-alias extractor (not the legacy
        // classifier); base-ref matching uses the `JoinAliases`-sourced
        // validators, mirroring the execute path.
        let join_aliases =
            extract_join_aliases(&canonical_select_query).map_err(RefreshError::user)?;
        if base_refs.len() != 2 {
            return Err(RefreshError::user(
                "iceberg join materialized view refresh requires exactly two base table references",
            ));
        }
        validate_join_aliases_base_refs(&join_aliases, &base_refs).map_err(RefreshError::user)?;
        let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
            RefreshError::user(format!(
                "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            ))
        })?;
        if schema_contract.contract_version != 2 {
            return Err(RefreshError::user(format!(
                "iceberg join MV {}.{}.{} requires schema contract version 2, got {}",
                iceberg_target.catalog,
                iceberg_target.namespace,
                iceberg_target.table,
                schema_contract.contract_version
            )));
        }
        let (left_ref, right_ref) =
            join_base_refs_for_aliases(&join_aliases, &base_refs).map_err(RefreshError::user)?;
        let left_refresh =
            observe_current_refresh_base_with_source(source, left_ref, connector_context)
                .map_err(RefreshError::user)?;
        let right_refresh =
            observe_current_refresh_base_with_source(source, right_ref, connector_context)
                .map_err(RefreshError::user)?;
        let left_observation =
            observe_schema_validation_for_table(source, left_ref, connector_context)
                .map_err(RefreshError::user)?;
        let right_observation =
            observe_schema_validation_for_table(source, right_ref, connector_context)
                .map_err(RefreshError::user)?;
        let target_observation =
            observe_schema_validation_for_table(source, &target_ref, connector_context)
                .map_err(RefreshError::user)?;
        let left_fqn = left_ref.fqn();
        let right_fqn = right_ref.fqn();
        match validate_join_schema_contract(
            schema_contract,
            &[
                (left_fqn.as_str(), left_observation),
                (right_fqn.as_str(), right_observation),
            ],
            &target_observation,
        )
        .map_err(|error| RefreshError::user(error.to_string()))?
        {
            JoinContractDecision::CompatibleSafe
            | JoinContractDecision::CompatibleSafeWithRebind { .. } => {}
        }
        let left_current = left_refresh.current_snapshot_id();
        let right_current = right_refresh.current_snapshot_id();
        let mut snapshot_pins = BTreeMap::new();
        snapshot_pins.insert(left_ref.fqn(), left_current);
        snapshot_pins.insert(right_ref.fqn(), right_current);
        let mut current_snapshots = BTreeMap::new();
        current_snapshots.insert(left_ref.fqn(), left_current);
        current_snapshots.insert(right_ref.fqn(), right_current);
        let previous_snapshots = &mv_definition.last_refresh_snapshots;
        let refresh_label = format!(
            "iceberg join MV {}.{}.{}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        );
        let refresh_statuses =
            base_snapshot_statuses_for_plan(&base_refs, previous_snapshots, &current_snapshots);
        let decision = decide_refresh_plan(&RefreshPlanningInput {
            snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
            base_snapshots: &refresh_statuses,
            label: &refresh_label,
        })
        .map_err(RefreshError::user)?;
        let has_previous = base_refs
            .iter()
            .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
        if has_previous {
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                if previous_snapshots.contains_key(&fqn)
                    && current_snapshots.get(&fqn).copied().flatten().is_none()
                {
                    return Err(RefreshError::user(format!(
                        "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                    )));
                }
            }
            for base_ref in &base_refs {
                let fqn = base_ref.fqn();
                previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                    RefreshError::user(format!(
                        "iceberg join MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                    ))
                })?;
                current_snapshots
                    .get(&fqn)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        RefreshError::user(format!(
                            "cannot refresh iceberg join materialized view {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                        ))
                    })?;
            }
        }
        let affected_partitions = unknown_join_affected_partitions();
        log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
        return Ok(RefreshPlan {
            contract: RefreshPlanContract {
                mv_id: Some(mv_definition.mv_id),
                target,
                storage_engine: MvStorageEngine::Iceberg,
                decision: decision.refresh,
                state_baseline: refresh_state_baseline.clone(),
                base_refs: base_refs
                    .iter()
                    .map(|base_ref| TableIdentity {
                        catalog: base_ref.catalog.clone(),
                        namespace: base_ref.namespace.clone(),
                        table: base_ref.table.clone(),
                    })
                    .collect(),
                snapshot_pins,
                affected_partitions,
            },
            backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                stmt: stmt.clone(),
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
            }),
        });
    }
    // The capability dispatch above (has_agg=false, SingleBase, non-join) routes only
    // single-base projection/filter MVs to this point. No classify guard needed.
    let [base_ref] = base_refs.as_slice() else {
        return Err(RefreshError::user(
            "iceberg materialized view refresh requires exactly one base table reference",
        ));
    };
    let schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        RefreshError::user(format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ))
    })?;
    let current_snapshot_id_before_pin =
        observe_current_refresh_base_with_source(source, base_ref, connector_context)
            .map_err(RefreshError::user)?
            .current_snapshot_id();
    let previous_snapshot_id = mv_definition
        .last_refresh_snapshots
        .get(&base_ref.fqn())
        .copied();
    let refresh_label = format!(
        "iceberg materialized view {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let pre_pin_statuses = [base_snapshot_status_for_refresh(
        base_ref,
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )];
    let pre_pin_decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        base_snapshots: &pre_pin_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let base_observation = observe_schema_validation_for_table(source, base_ref, connector_context)
        .map_err(RefreshError::user)?;
    let target_observation =
        observe_schema_validation_for_table(source, &target_ref, connector_context)
            .map_err(RefreshError::user)?;
    match validate_schema_contract(schema_contract, &base_observation, &target_observation) {
        ContractDecision::Incompatible(err) => {
            return Err(RefreshError::user(format!("{err}")));
        }
        ContractDecision::CompatibleSafeWithRebind { .. } | ContractDecision::CompatibleSafe => {}
    }
    match pre_pin_decision.refresh {
        ExecutableRefreshDecision::SkipEmpty => {
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_ref.fqn(), None);
            let affected_partitions = noop_affected_partitions(schema_contract);
            log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
            return Ok(RefreshPlan {
                contract: RefreshPlanContract {
                    mv_id: Some(mv_definition.mv_id),
                    target,
                    storage_engine: MvStorageEngine::Iceberg,
                    decision: pre_pin_decision.refresh,
                    state_baseline: refresh_state_baseline.clone(),
                    base_refs: vec![TableIdentity {
                        catalog: base_ref.catalog.clone(),
                        namespace: base_ref.namespace.clone(),
                        table: base_ref.table.clone(),
                    }],
                    snapshot_pins,
                    affected_partitions,
                },
                backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
                    stmt: stmt.clone(),
                    current_catalog: current_catalog.map(str::to_string),
                    current_database: current_database.to_string(),
                }),
            });
        }
        ExecutableRefreshDecision::FirstRefresh
        | ExecutableRefreshDecision::MetadataOnly
        | ExecutableRefreshDecision::Incremental => {}
    }

    let current_snapshot_id = current_snapshot_id_before_pin;

    let refresh_statuses = [base_snapshot_status_for_refresh(
        base_ref,
        previous_snapshot_id,
        current_snapshot_id,
    )];
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    let mut snapshot_pins = BTreeMap::new();
    snapshot_pins.insert(base_ref.fqn(), current_snapshot_id);
    let affected_partitions = plan_aggregate_mv_affected_partitions(
        source,
        connector_context,
        base_ref,
        schema_contract,
        mode,
        previous_snapshot_id,
        current_snapshot_id,
    );
    log_planned_iceberg_mv_affected_partitions(&iceberg_target, &affected_partitions);
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline: refresh_state_baseline,
            base_refs: vec![TableIdentity {
                catalog: base_ref.catalog.clone(),
                namespace: base_ref.namespace.clone(),
                table: base_ref.table.clone(),
            }],
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_iceberg_union_projection_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    branch_count: usize,
    schema_contract: &mv_schema::MvSchemaContract,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    validate_union_projection_base_refs(base_refs, schema_contract).map_err(RefreshError::user)?;
    let target_ref = TableIdentity {
        catalog: iceberg_target.catalog.clone(),
        namespace: iceberg_target.namespace.clone(),
        table: iceberg_target.table.clone(),
    };
    let target_observation =
        observe_schema_validation_for_table(source, &target_ref, connector_context)
            .map_err(RefreshError::user)?;

    let mut current_snapshots = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for base_ref in base_refs {
        let refresh = observe_current_refresh_base_with_source(source, base_ref, connector_context)
            .map_err(RefreshError::user)?;
        let base_observation =
            observe_schema_validation_for_table(source, base_ref, connector_context)
                .map_err(RefreshError::user)?;
        validate_union_projection_schema_contract_for_base(
            iceberg_target,
            schema_contract,
            branch_count,
            base_ref,
            &base_observation,
            &target_observation,
        )
        .map_err(RefreshError::user)?;
        let current = refresh.current_snapshot_id();
        let fqn = base_ref.fqn();
        snapshot_pins.insert(fqn.clone(), current);
        current_snapshots.insert(fqn.clone(), current);
    }

    let previous_snapshots = &mv_definition.last_refresh_snapshots;
    let previous_table_uuids = &mv_definition.last_refresh_table_uuids;
    let has_previous_snapshots = base_refs
        .iter()
        .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    let has_previous_table_uuids = base_refs
        .iter()
        .any(|base_ref| previous_table_uuids.contains_key(&base_ref.fqn()));
    let has_previous = has_previous_snapshots || has_previous_table_uuids;
    let all_previous_snapshots = base_refs
        .iter()
        .all(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    let all_previous_table_uuids = base_refs
        .iter()
        .all(|base_ref| previous_table_uuids.contains_key(&base_ref.fqn()));

    if has_previous && (!all_previous_snapshots || !all_previous_table_uuids) {
        return Err(RefreshError::user(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )));
    }
    let refresh_label = format!(
        "iceberg UNION ALL projection/filter MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    if has_previous {
        for base_ref in base_refs {
            let fqn = base_ref.fqn();
            if let Some(previous_uuid) = previous_table_uuids.get(&fqn) {
                let current_uuid =
                    observe_schema_validation_for_table(source, base_ref, connector_context)
                        .map_err(RefreshError::user)?
                        .table_uuid()
                        .to_string();
                if previous_uuid != &current_uuid {
                    return Err(RefreshError::user(format!(
                        "iceberg MV base table identity changed for {fqn}; incremental refresh is unsafe, rebuild or recreate the MV"
                    )));
                }
            }
            previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                RefreshError::user(format!(
                    "iceberg UNION ALL projection/filter MV {}.{}.{} has partial previous refresh metadata; recreate the MV",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                ))
            })?;
            current_snapshots.get(&fqn).copied().flatten().ok_or_else(|| {
                RefreshError::user(format!(
                    "cannot refresh iceberg UNION ALL projection/filter MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                ))
            })?;
        }
    }

    let affected_partitions = plan_multi_base_affected_partitions(
        schema_contract,
        mode,
        base_refs,
        previous_snapshots,
        &current_snapshots,
        |base_ref, previous, current| {
            observe_and_admit_change_window_for_table(
                source,
                base_ref,
                previous,
                current,
                connector_context,
            )
        },
        "UNION ALL MV affected partition planning",
    );
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    let state_baseline = build_refresh_state_baseline(
        mv_definition,
        &target_binding_for(source, iceberg_target, connector_context)
            .map_err(RefreshError::user)?,
        current_catalog,
        current_database,
    )
    .map_err(RefreshError::user)?;
    Ok(RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision: decision.refresh,
            state_baseline,
            base_refs: base_refs
                .iter()
                .map(|base_ref| TableIdentity {
                    catalog: base_ref.catalog.clone(),
                    namespace: base_ref.namespace.clone(),
                    table: base_ref.table.clone(),
                })
                .collect(),
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    })
}

#[allow(clippy::too_many_arguments)]
/// Plan the `AllBasesRequired` aggregate refresh variants (fan-in
/// `GroupRowId` and branch-union `BranchScoped`). Extracted from
/// `plan_iceberg_aggregate_mv_refresh` (I2) to mirror the execute-side
/// [`refresh_fan_in_aggregate_iceberg_mv`] fold: both `AllBasesRequired`
/// aggregate identities pin/validate every base, decide the refresh from the
/// combined base-snapshot statuses, and build one multi-base refresh plan; only
/// the up-front branch-contract vs fan-in base-ref validation and the log label
/// differ. Behavior is byte-for-byte identical to the inline block it replaced.
#[allow(clippy::too_many_arguments)]
fn plan_iceberg_all_bases_aggregate_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    caps: &RefreshCapabilities,
    canonical_select_query: &sqlparser::ast::Query,
    schema_contract: &mv_schema::MvSchemaContract,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    // The branch-union variant validates the branch count (off the AST, so a
    // composed branch union is supported) + the resolved base-ref set; the
    // fan-in variant validates the resolved base-ref set directly (the resolved
    // bases ARE the fan-in base set now that the classifier is retired).
    let is_branch_union = matches!(caps.identity, RefreshIdentity::BranchScoped(_));
    let is_composed_join_aggregate =
        !is_branch_union && !from_clause_is_fan_in_union(canonical_select_query);
    let target_ref = TableIdentity {
        catalog: iceberg_target.catalog.clone(),
        namespace: iceberg_target.namespace.clone(),
        table: iceberg_target.table.clone(),
    };
    let target_observation =
        observe_schema_validation_for_table(source, &target_ref, connector_context)
            .map_err(RefreshError::user)?;
    if is_branch_union {
        let branch_count = union_branch_count(canonical_select_query) as usize;
        validate_branch_union_contract(
            iceberg_target,
            schema_contract,
            branch_count,
            &target_observation,
        )
        .map_err(RefreshError::user)?;
        validate_branch_union_aggregate_base_refs(base_refs).map_err(RefreshError::user)?;
    } else if is_composed_join_aggregate {
        validate_composed_aggregate_fallback_query(canonical_select_query)
            .map_err(RefreshError::user)?;
    } else {
        validate_aggregate_fan_in_base_refs(base_refs).map_err(RefreshError::user)?;
    }
    let mut current_snapshots = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for base_ref in base_refs {
        let refresh = observe_current_refresh_base_with_source(source, base_ref, connector_context)
            .map_err(RefreshError::user)?;
        let base_observation =
            observe_schema_validation_for_table(source, base_ref, connector_context)
                .map_err(RefreshError::user)?;
        validate_aggregate_schema_contract_for_base(
            schema_contract,
            base_ref,
            &base_observation,
            &target_observation,
        )
        .map_err(RefreshError::user)?;
        let current = refresh.current_snapshot_id();
        let fqn = base_ref.fqn();
        current_snapshots.insert(fqn.clone(), current);
        snapshot_pins.insert(fqn.clone(), current);
    }
    let previous_snapshots = &mv_definition.last_refresh_snapshots;
    let refresh_kind_label = if is_branch_union {
        "branch UNION ALL aggregate"
    } else if is_composed_join_aggregate {
        "composed join aggregate"
    } else {
        "aggregate-over-UNION-ALL"
    };
    let refresh_label = format!(
        "iceberg {refresh_kind_label} MV {}.{}.{}",
        iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
    );
    let refresh_statuses =
        base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
    let decision = decide_refresh_plan(&RefreshPlanningInput {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        base_snapshots: &refresh_statuses,
        label: &refresh_label,
    })
    .map_err(RefreshError::user)?;
    let mode = decision.mode();
    let has_previous = base_refs
        .iter()
        .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
    if has_previous {
        for base_ref in base_refs {
            let fqn = base_ref.fqn();
            previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                RefreshError::user(format!(
                    "iceberg {refresh_kind_label} MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                ))
            })?;
            current_snapshots.get(&fqn).copied().flatten().ok_or_else(|| {
                RefreshError::user(format!(
                    "cannot refresh iceberg {refresh_kind_label} MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                    iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table, fqn
                ))
            })?;
        }
    }
    let affected_partition_context =
        format!("iceberg {refresh_kind_label} MV affected partition planning");
    let affected_partitions = plan_multi_base_affected_partitions(
        schema_contract,
        mode,
        base_refs,
        previous_snapshots,
        &current_snapshots,
        |base_ref, previous, current| {
            observe_and_admit_change_window_for_table(
                source,
                base_ref,
                previous,
                current,
                connector_context,
            )
        },
        &affected_partition_context,
    );
    log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
    Ok(build_iceberg_refresh_plan(
        mv_definition,
        target,
        stmt,
        current_catalog,
        current_database,
        base_refs,
        snapshot_pins,
        decision.refresh,
        build_refresh_state_baseline(
            mv_definition,
            &target_binding_for(source, iceberg_target, connector_context)
                .map_err(RefreshError::user)?,
            current_catalog,
            current_database,
        )
        .map_err(RefreshError::user)?,
        affected_partitions,
    ))
}

fn plan_iceberg_aggregate_mv_refresh(
    source: &dyn IcebergMvRefreshSource,
    iceberg_target: &IcebergMvTarget,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    caps: &RefreshCapabilities,
    canonical_select_query: &sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<RefreshPlan, RefreshError> {
    let schema_contract =
        validate_aggregate_schema_contract_metadata(iceberg_target, mv_definition)
            .map_err(RefreshError::user)?;
    // Aggregate plan dispatch (Phase 3 / B2): selected by capability.
    //   SingleBase                 -> single-base aggregate
    //   AllBasesRequired           -> fan-in (GroupRowId) or branch-union
    //                                 (BranchScoped), gated on the row identity
    //   JoinPairPartialInitialSkip -> join aggregate
    match &caps.snapshot_policy {
        BaseSnapshotPolicy::SingleBase | BaseSnapshotPolicy::AllBasesRequired => {
            // AllBasesRequired (fan-in `GroupRowId` / branch-union
            // `BranchScoped`) is planned by a dedicated helper (I2) mirroring
            // the execute-side `refresh_fan_in_aggregate_iceberg_mv` fold.
            if matches!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired) {
                return plan_iceberg_all_bases_aggregate_mv_refresh(
                    source,
                    iceberg_target,
                    target,
                    stmt,
                    current_catalog,
                    current_database,
                    mv_definition,
                    base_refs,
                    caps,
                    canonical_select_query,
                    schema_contract,
                    connector_context,
                );
            }
            let [base_ref] = base_refs else {
                return Err(RefreshError::user(
                    "iceberg aggregate materialized view refresh requires exactly one base table reference",
                ));
            };
            let refresh =
                observe_current_refresh_base_with_source(source, base_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let base_observation =
                observe_schema_validation_for_table(source, base_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let target_ref = TableIdentity {
                catalog: iceberg_target.catalog.clone(),
                namespace: iceberg_target.namespace.clone(),
                table: iceberg_target.table.clone(),
            };
            let target_observation =
                observe_schema_validation_for_table(source, &target_ref, connector_context)
                    .map_err(RefreshError::user)?;
            match validate_schema_contract(schema_contract, &base_observation, &target_observation)
            {
                ContractDecision::Incompatible(err) => {
                    return Err(RefreshError::user(format!("{err}")));
                }
                ContractDecision::CompatibleSafe
                | ContractDecision::CompatibleSafeWithRebind { .. } => {}
            }
            let current = refresh.current_snapshot_id();
            let previous = mv_definition
                .last_refresh_snapshots
                .get(&base_ref.fqn())
                .copied();
            let refresh_label = format!(
                "iceberg aggregate materialized view {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses = [base_snapshot_status_for_refresh(
                base_ref, previous, current,
            )];
            let decision = decide_refresh_plan(&RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::SingleBase,
                base_snapshots: &refresh_statuses,
                label: &refresh_label,
            })
            .map_err(RefreshError::user)?;
            let mode = decision.mode();
            let mut snapshot_pins = BTreeMap::new();
            snapshot_pins.insert(base_ref.fqn(), current);
            let affected_partitions = plan_aggregate_mv_affected_partitions(
                source,
                connector_context,
                base_ref,
                schema_contract,
                mode,
                previous,
                current,
            );
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                decision.refresh,
                build_refresh_state_baseline(
                    mv_definition,
                    &target_binding_for(source, iceberg_target, connector_context)
                        .map_err(RefreshError::user)?,
                    current_catalog,
                    current_database,
                )
                .map_err(RefreshError::user)?,
                affected_partitions,
            ))
        }
        BaseSnapshotPolicy::JoinPairPartialInitialSkip => {
            // The join-aggregate plan sources the left/right table aliases from
            // the focused join-alias extractor (FROM-side only); the join ON
            // keys are never read by the plan path. Base-ref matching uses those
            // table FQNs against the analyzer-resolved base refs.
            let join_aliases =
                extract_join_aliases(canonical_select_query).map_err(RefreshError::user)?;
            if base_refs.len() != 2 {
                return Err(RefreshError::user(
                    "iceberg join aggregate MV refresh requires exactly two base table references",
                ));
            }
            validate_join_aliases_base_refs(&join_aliases, base_refs)
                .map_err(RefreshError::user)?;
            let (left_ref, right_ref) =
                join_base_refs_for_aliases(&join_aliases, base_refs).map_err(RefreshError::user)?;
            let left_refresh =
                observe_current_refresh_base_with_source(source, left_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let right_refresh =
                observe_current_refresh_base_with_source(source, right_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let left_observation =
                observe_schema_validation_for_table(source, left_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let right_observation =
                observe_schema_validation_for_table(source, right_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let target_ref = TableIdentity {
                catalog: iceberg_target.catalog.clone(),
                namespace: iceberg_target.namespace.clone(),
                table: iceberg_target.table.clone(),
            };
            let target_observation =
                observe_schema_validation_for_table(source, &target_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            match validate_join_schema_contract(
                schema_contract,
                &[
                    (left_fqn.as_str(), left_observation),
                    (right_fqn.as_str(), right_observation),
                ],
                &target_observation,
            )
            .map_err(|error| RefreshError::user(error.to_string()))?
            {
                JoinContractDecision::CompatibleSafe
                | JoinContractDecision::CompatibleSafeWithRebind { .. } => {}
            }

            let mut snapshot_pins = BTreeMap::new();
            let mut current_snapshots = BTreeMap::new();
            current_snapshots.insert(left_ref.fqn(), left_refresh.current_snapshot_id());
            current_snapshots.insert(right_ref.fqn(), right_refresh.current_snapshot_id());
            for base_ref in base_refs {
                snapshot_pins.insert(
                    base_ref.fqn(),
                    current_snapshots.get(&base_ref.fqn()).copied().flatten(),
                );
            }
            let previous_snapshots = &mv_definition.last_refresh_snapshots;
            let refresh_label = format!(
                "iceberg join aggregate MV {}.{}.{}",
                iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
            );
            let refresh_statuses =
                base_snapshot_statuses_for_plan(base_refs, previous_snapshots, &current_snapshots);
            let decision = decide_refresh_plan(&RefreshPlanningInput {
                snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                base_snapshots: &refresh_statuses,
                label: &refresh_label,
            })
            .map_err(RefreshError::user)?;
            let has_previous = base_refs
                .iter()
                .any(|base_ref| previous_snapshots.contains_key(&base_ref.fqn()));
            if has_previous {
                for base_ref in base_refs {
                    let fqn = base_ref.fqn();
                    previous_snapshots.get(&fqn).copied().ok_or_else(|| {
                        RefreshError::user(format!(
                            "iceberg join aggregate MV {}.{}.{} has partial previous refresh snapshots; recreate the MV",
                            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
                        ))
                    })?;
                    current_snapshots.get(&fqn).copied().flatten().ok_or_else(
                        || {
                            RefreshError::user(format!(
                                "cannot refresh iceberg join aggregate MV {}.{}.{}: previously-refreshed base snapshot for {} is no longer reachable",
                                iceberg_target.catalog,
                                iceberg_target.namespace,
                                iceberg_target.table,
                                fqn
                            ))
                        },
                    )?;
                }
            }
            let affected_partitions = unknown_join_affected_partitions();
            log_planned_iceberg_mv_affected_partitions(iceberg_target, &affected_partitions);
            Ok(build_iceberg_refresh_plan(
                mv_definition,
                target,
                stmt,
                current_catalog,
                current_database,
                base_refs,
                snapshot_pins,
                decision.refresh,
                build_refresh_state_baseline(
                    mv_definition,
                    &target_binding_for(source, iceberg_target, connector_context)
                        .map_err(RefreshError::user)?,
                    current_catalog,
                    current_database,
                )
                .map_err(RefreshError::user)?,
                affected_partitions,
            ))
        }
    }
}

fn build_iceberg_refresh_plan(
    mv_definition: &StoredMvDefinition,
    target: MvTarget,
    stmt: &RefreshMaterializedViewStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    base_refs: &[TableIdentity],
    snapshot_pins: BTreeMap<String, Option<i64>>,
    decision: ExecutableRefreshDecision,
    state_baseline: RefreshStateBaseline,
    affected_partitions: crate::mv::model::AffectedTargetPartitions,
) -> RefreshPlan {
    RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision,
            state_baseline,
            base_refs: base_refs
                .iter()
                .map(|base_ref| TableIdentity {
                    catalog: base_ref.catalog.clone(),
                    namespace: base_ref.namespace.clone(),
                    table: base_ref.table.clone(),
                })
                .collect(),
            snapshot_pins,
            affected_partitions,
        },
        backend_plan: BackendRefreshPlan::Iceberg(IcebergRefreshPlan {
            stmt: stmt.clone(),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
        }),
    }
}

#[cfg(test)]
thread_local! {
    static DEFINITION_LOAD_COUNTER: std::cell::RefCell<Option<Arc<std::sync::atomic::AtomicUsize>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static CATALOG_REGISTRATION_FAILURE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct CatalogRegistrationFailureGuard;

#[cfg(test)]
impl CatalogRegistrationFailureGuard {
    fn install(message: impl Into<String>) -> Self {
        CATALOG_REGISTRATION_FAILURE.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "catalog registration failure already installed"
            );
            *slot.borrow_mut() = Some(message.into());
        });
        Self
    }
}

#[cfg(test)]
impl Drop for CatalogRegistrationFailureGuard {
    fn drop(&mut self) {
        CATALOG_REGISTRATION_FAILURE.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
fn take_catalog_registration_failure_for_test() -> Option<String> {
    CATALOG_REGISTRATION_FAILURE.with(|slot| slot.borrow_mut().take())
}

#[cfg(test)]
struct DefinitionLoadCounterGuard;

#[cfg(test)]
impl DefinitionLoadCounterGuard {
    fn install(counter: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        DEFINITION_LOAD_COUNTER.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "definition load counter already installed"
            );
            *slot.borrow_mut() = Some(counter);
        });
        Self
    }
}

#[cfg(test)]
impl Drop for DefinitionLoadCounterGuard {
    fn drop(&mut self) {
        DEFINITION_LOAD_COUNTER.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
fn record_definition_load() {
    DEFINITION_LOAD_COUNTER.with(|slot| {
        if let Some(counter) = slot.borrow().as_ref() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
}

#[cfg(test)]
thread_local! {
    static AFTER_CREATE_TARGET_HOOK: std::cell::RefCell<Option<Arc<dyn Fn() + Send + Sync>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct AfterCreateTargetHookGuard;

#[cfg(test)]
impl AfterCreateTargetHookGuard {
    fn install(hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        AFTER_CREATE_TARGET_HOOK.with(|slot| {
            assert!(
                slot.borrow().is_none(),
                "after-create target hook already installed"
            );
            *slot.borrow_mut() = Some(hook);
        });
        Self
    }
}

#[cfg(test)]
impl Drop for AfterCreateTargetHookGuard {
    fn drop(&mut self) {
        AFTER_CREATE_TARGET_HOOK.with(|slot| *slot.borrow_mut() = None);
    }
}

#[cfg(test)]
fn run_after_create_target_hook() {
    AFTER_CREATE_TARGET_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow().as_ref() {
            hook();
        }
    });
}

#[cfg(test)]
type AfterObserveBeforeCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
struct AfterObserveBeforeCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterObserveBeforeCaptureHook,
}

#[cfg(test)]
fn after_observe_before_capture_hook_slot()
-> &'static std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_observe_before_capture_hook() {
    let current_thread = std::thread::current().id();
    let hook = after_observe_before_capture_hook_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .and_then(|registration| {
            (registration.owner == current_thread).then(|| Arc::clone(&registration.hook))
        });
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
struct AfterObserveBeforeCaptureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl AfterObserveBeforeCaptureHookGuard {
    fn install(hook: AfterObserveBeforeCaptureHook) -> Self {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        let lock = LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *after_observe_before_capture_hook_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(AfterObserveBeforeCaptureHookRegistration {
                owner: std::thread::current().id(),
                hook,
            });
        Self { _lock: lock }
    }
}

#[cfg(test)]
impl Drop for AfterObserveBeforeCaptureHookGuard {
    fn drop(&mut self) {
        *after_observe_before_capture_hook_slot()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

fn optional_snapshot_map(pin: &RefreshSnapshotPin) -> BTreeMap<String, Option<i64>> {
    pin.to_snapshot_map()
        .into_iter()
        .map(|(fqn, snapshot_id)| (fqn, Some(snapshot_id)))
        .collect()
}

fn validate_refresh_pin_table_uuids_against_baseline(
    baseline: &RefreshStateBaseline,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    let RefreshStateBaseline::SnapshotBacked {
        previous_table_uuids,
        ..
    } = baseline
    else {
        return Err(
            "iceberg refresh execution requires a snapshot-backed state baseline".to_string(),
        );
    };
    for base_ref in base_refs {
        let Some(previous_uuid) = previous_table_uuids.get(&base_ref.fqn()) else {
            continue;
        };
        let current_uuid = pin.uuid(base_ref).ok_or_else(|| {
            format!(
                "refresh pin missing uuid for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?;
        if previous_uuid != current_uuid {
            return Err(format!(
                "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
                base_ref.fqn(),
            ));
        }
    }
    Ok(())
}

fn load_iceberg_mv_definition_by_target(
    source: &dyn IcebergMvRefreshSource,
    target: &IcebergMvTarget,
) -> Result<StoredMvDefinition, String> {
    #[cfg(test)]
    record_definition_load();
    source
        .repository()
        .find_by_target(&crate::mv::model::MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load iceberg mv definition failed: {e}"))?
        .ok_or_else(|| {
            format!(
                "iceberg materialized view {}.{}.{} has no MV definition",
                target.catalog, target.namespace, target.table
            )
        })
}

fn mv_definition_fingerprint(select_sql: &str) -> String {
    hex::encode(Sha256::digest(select_sql.as_bytes()))
}

fn build_aggregate_layout_for_refresh_select_sql(
    source: &dyn IcebergMvRefreshSource,
    current_catalog: Option<&str>,
    current_database: &str,
    select_sql: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout, String> {
    let visible_query = parse_mv_select_query(select_sql)?;
    let provider = crate::query_execution::compiler::build_catalog_service_provider(
        current_catalog,
        source.catalog_service().as_ref(),
        source.connector_control(),
        connector_context.clone(),
        novarocks_sql::planning::catalog::TableLookupMode::SchemaOnly,
        source.catalog_application(),
    );
    let visible_analysis = crate::mv::analysis_adapter::analyze_mv_select_with_provider(
        current_catalog,
        &provider,
        current_database,
        &visible_query,
    )?;
    let facts = visible_analysis
        .refresh_input
        .aggregate_layout_facts(&visible_query, SqlMvAggregateLayoutScope::WholeQuery)?;
    crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_from_sql_facts(&facts)
}
pub(crate) fn parse_mv_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)
        .map_err(|e| format!("stored MV SELECT normalize error: {e}"))?;
    let statement = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized)
        .map_err(|err| format!("sql parser error: {err}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn validate_repartition_schema_contract(
    source: &dyn IcebergMvRefreshSource,
    schema_contract: &mv_schema::MvSchemaContract,
    base_refs: &[TableIdentity],
    target_observation: &MvSchemaValidationObservation,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    if schema_contract.join.is_some() {
        let [left_ref, right_ref] = base_refs else {
            return Err(format!(
                "Iceberg join MV repartition schema contract requires exactly two base tables, got {}",
                base_refs.len()
            ));
        };
        let left_observation =
            observe_schema_validation_for_table(source, left_ref, connector_context)?;
        let right_observation =
            observe_schema_validation_for_table(source, right_ref, connector_context)?;
        let left_fqn = left_ref.fqn();
        let right_fqn = right_ref.fqn();
        match validate_join_schema_contract(
            schema_contract,
            &[
                (left_fqn.as_str(), left_observation),
                (right_fqn.as_str(), right_observation),
            ],
            target_observation,
        )
        .map_err(|error| error.to_string())?
        {
            JoinContractDecision::CompatibleSafe => {}
            JoinContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
                if schema_contract.aggregate.is_some() {
                    return Err(format!(
                        "iceberg join aggregate MV repartition requires schema rebind for {rebound_columns:?}, which is not supported during repartition; recreate the MV"
                    ));
                }
            }
        }
        return Ok(());
    }

    if !schema_contract.bases.is_empty() {
        for base_ref in base_refs {
            let base_observation =
                observe_schema_validation_for_table(source, base_ref, connector_context)?;
            if schema_contract.aggregate.is_some() {
                validate_aggregate_repartition_schema_contract_for_base(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    target_observation,
                )?;
            } else {
                validate_repartition_base_schema_contract(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    target_observation,
                )?;
            }
        }
        return Ok(());
    }

    let [base_ref] = base_refs else {
        return Err(format!(
            "ALTER MATERIALIZED VIEW ... REPARTITION single-base schema contract requires exactly one base table, got {}",
            base_refs.len()
        ));
    };
    let base_observation =
        observe_schema_validation_for_table(source, base_ref, connector_context)?;
    if schema_contract.aggregate.is_some() {
        validate_aggregate_repartition_schema_contract_for_base(
            schema_contract,
            base_ref,
            &base_observation,
            &target_observation,
        )
    } else {
        match validate_schema_contract(schema_contract, &base_observation, &target_observation) {
            ContractDecision::Incompatible(error) => Err(error.to_string()),
            ContractDecision::CompatibleSafe
            | ContractDecision::CompatibleSafeWithRebind { .. } => Ok(()),
        }
    }
}

fn validate_aggregate_repartition_schema_contract_for_base(
    schema_contract: &mv_schema::MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    validate_aggregate_schema_contract_for_base(
        schema_contract,
        base_ref,
        base_observation,
        target_observation,
    )
    .map_err(|err| {
        if err.contains("requires schema rebind") {
            format!(
                "iceberg aggregate MV repartition requires schema rebind for base {}, which is not supported during repartition; recreate the MV",
                base_ref.fqn()
            )
        } else {
            err
        }
    })
}

fn validate_repartition_base_schema_contract(
    schema_contract: &mv_schema::MvSchemaContract,
    base_ref: &TableIdentity,
    base_observation: &MvSchemaValidationObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let mut base_contract = schema_contract.clone();
    base_contract.base = schema_contract
        .bases
        .iter()
        .find(|base| base.table_fqn.eq_ignore_ascii_case(&base_ref.fqn()))
        .cloned()
        .ok_or_else(|| {
            format!(
                "Iceberg MV repartition schema contract missing base {}; recreate the MV",
                base_ref.fqn()
            )
        })?;
    match validate_schema_contract(&base_contract, base_observation, target_observation) {
        ContractDecision::Incompatible(err) => Err(format!("{err}")),
        ContractDecision::CompatibleSafe | ContractDecision::CompatibleSafeWithRebind { .. } => {
            Ok(())
        }
    }
}

fn validate_join_aliases_base_refs(
    aliases: &SqlMvJoinAliases,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    for name in [
        aliases.left_table.to_ascii_lowercase(),
        aliases.right_table.to_ascii_lowercase(),
    ] {
        if !base_refs
            .iter()
            .any(|base| base.fqn().eq_ignore_ascii_case(&name))
        {
            return Err(format!(
                "join MV references base {name} but analyzer resolved {base_refs:?}"
            ));
        }
    }
    Ok(())
}

/// Resolves the left/right `base_refs` for a join MV by matching
/// `JoinAliases.{left_table,right_table}` (the `ObjectName.to_string()` FQN form)
/// against `base.fqn()`.
fn join_base_refs_for_aliases<'a>(
    aliases: &SqlMvJoinAliases,
    base_refs: &'a [TableIdentity],
) -> Result<(&'a TableIdentity, &'a TableIdentity), String> {
    let left_name = aliases.left_table.as_str();
    let right_name = aliases.right_table.as_str();
    let left = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(left_name))
        .ok_or_else(|| format!("join MV left base {left_name} was not resolved"))?;
    let right = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(right_name))
        .ok_or_else(|| format!("join MV right base {right_name} was not resolved"))?;
    Ok((left, right))
}

pub(crate) fn join_base_refs_for_schema_contract<'a>(
    schema_contract: &mv_schema::MvSchemaContract,
    base_refs: &'a [TableIdentity],
) -> Result<(&'a TableIdentity, &'a TableIdentity), String> {
    let join = schema_contract
        .join
        .as_ref()
        .ok_or_else(|| "join MV schema contract missing join lineage".to_string())?;
    let predicate = join
        .predicates
        .first()
        .ok_or_else(|| "join MV schema contract has no join predicates".to_string())?;
    let left_name = predicate.left.table_fqn.as_str();
    let right_name = predicate.right.table_fqn.as_str();
    if left_name.eq_ignore_ascii_case(right_name) {
        return Err(format!(
            "join MV schema contract has identical left/right bases: {left_name}"
        ));
    }
    let left = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(left_name))
        .ok_or_else(|| format!("join MV left base {left_name} was not resolved"))?;
    let right = base_refs
        .iter()
        .find(|base| base.fqn().eq_ignore_ascii_case(right_name))
        .ok_or_else(|| format!("join MV right base {right_name} was not resolved"))?;
    Ok((left, right))
}

fn validate_refresh_pin_table_uuids(
    mv_definition: &StoredMvDefinition,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    validate_refresh_pin_table_uuids_for_operation(
        mv_definition,
        pin,
        base_refs,
        "incremental refresh is unsafe, rebuild or recreate the MV",
    )
}

fn validate_repartition_refresh_pin_table_uuids(
    mv_definition: &StoredMvDefinition,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    validate_refresh_pin_table_uuids_for_operation(
        mv_definition,
        pin,
        base_refs,
        "repartition is unsafe, recreate the MV",
    )
}

fn validate_refresh_pin_table_uuids_for_operation(
    mv_definition: &StoredMvDefinition,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
    unsafe_message: &str,
) -> Result<(), String> {
    for base_ref in base_refs {
        let Some(previous_uuid) = mv_definition.last_refresh_table_uuids.get(&base_ref.fqn())
        else {
            continue;
        };
        let current_uuid = pin.uuid(base_ref).ok_or_else(|| {
            format!(
                "refresh pin missing uuid for base {} (this should not happen)",
                base_ref.fqn()
            )
        })?;
        if previous_uuid != current_uuid {
            return Err(format!(
                "iceberg MV base table identity changed for {}; {unsafe_message}",
                base_ref.fqn(),
            ));
        }
    }
    Ok(())
}

fn apply_join_schema_contract_decision(
    decision: JoinContractDecision,
    mv_definition: &StoredMvDefinition,
) -> Result<StoredMvDefinition, String> {
    match decision {
        JoinContractDecision::CompatibleSafe => Ok(mv_definition.clone()),
        JoinContractDecision::CompatibleSafeWithRebind { rebound_columns } => {
            let rewritten_sql =
                rewrite_select_sql_for_rebind(&mv_definition.select_sql, &rebound_columns)?;
            let mut definition = mv_definition.clone();
            definition.select_sql = rewritten_sql;
            Ok(definition)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AdmittedChangeFacts {
    has_inserts: bool,
    has_deletes: bool,
}

fn admitted_change_facts(
    admission: &ConnectorChangeWindowAdmission,
) -> Result<AdmittedChangeFacts, String> {
    match admission {
        ConnectorChangeWindowAdmission::MetadataOnly => Ok(AdmittedChangeFacts::default()),
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            ..
        } => Ok(AdmittedChangeFacts {
            has_inserts: *has_inserts,
            has_deletes: *has_deletes,
        }),
        ConnectorChangeWindowAdmission::FullRebuild(reason) => {
            Err(full_rebuild_reason_message(*reason))
        }
    }
}

fn rewrite_snapshot_table_factor(
    factor: &mut sqlparser::ast::TableFactor,
    base: &TableIdentity,
    snapshot_id: i64,
    default_alias: Option<&str>,
) -> Result<(), String> {
    let sqlparser::ast::TableFactor::Table {
        name,
        version,
        alias,
        args,
        ..
    } = factor
    else {
        return Err("join snapshot side must be a table".to_string());
    };
    if args.is_some() {
        return Err("join snapshot side must be a base table".to_string());
    }
    if !object_name_matches_base(name, base)? {
        return Err(format!(
            "join snapshot rewrite expected base {}, got {}",
            base.fqn(),
            name
        ));
    }
    if let Some(version) = version {
        let rendered = version.to_string();
        if !rendered.contains(&snapshot_id.to_string()) {
            return Err(format!(
                "join snapshot side {} has conflicting version {rendered}",
                base.fqn()
            ));
        }
    }
    *name = synthetic_snapshot_object_name(base, snapshot_id);
    *version = None;
    if alias.is_none()
        && let Some(default_alias) = default_alias
    {
        *alias = Some(sqlparser::ast::TableAlias {
            explicit: true,
            name: sqlparser::ast::Ident::new(default_alias),
            columns: Vec::new(),
        });
    }
    Ok(())
}

fn object_name_matches_base(
    name: &sqlparser::ast::ObjectName,
    base: &TableIdentity,
) -> Result<bool, String> {
    let parts = object_name_identifier_parts(name);
    Ok(match parts.as_slice() {
        [table] => table.eq_ignore_ascii_case(&base.table),
        [namespace, table] => {
            namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        [catalog, namespace, table] => {
            catalog.eq_ignore_ascii_case(&base.catalog)
                && namespace.eq_ignore_ascii_case(&base.namespace)
                && table.eq_ignore_ascii_case(&base.table)
        }
        _ => false,
    })
}

fn object_name_identifier_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect()
}

fn synthetic_snapshot_table_name(base: &TableIdentity, snapshot_id: i64) -> String {
    format!("{}__at_{}", base.table, snapshot_id)
}

fn synthetic_snapshot_object_name(
    base: &TableIdentity,
    snapshot_id: i64,
) -> sqlparser::ast::ObjectName {
    sqlparser::ast::ObjectName(vec![
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(&base.namespace)),
        sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
            synthetic_snapshot_table_name(base, snapshot_id),
        )),
    ])
}

fn refresh_explain_rewrite_disabled_rules(
    is_aggregate_refresh: bool,
    optimizer_settings: &novarocks_sql::compiler::SessionOptimizerSettings,
) -> Vec<String> {
    let mut disabled_rules = optimizer_settings.disabled_rules.clone();
    if is_aggregate_refresh
        && !disabled_rules
            .iter()
            .any(|rule| rule == "RecordJoinRefreshDescriptor")
    {
        disabled_rules.push("RecordJoinRefreshDescriptor".to_string());
    }
    disabled_rules
}
fn sql_imv_planning_input_from_rewrite(
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    target_binding: novarocks_sql::binding::SqlTableBindingId,
    evidence: RewriteMergeRefreshEvidence,
) -> Result<novarocks_sql::compiler::SqlImvPlanningInput, String> {
    use novarocks_sql::compiler::SqlImvRewriteValidation;

    let validation = match evidence {
        RewriteMergeRefreshEvidence::None => SqlImvRewriteValidation::None,
        RewriteMergeRefreshEvidence::Aggregate => SqlImvRewriteValidation::Aggregate,
        RewriteMergeRefreshEvidence::JoinAggregate => SqlImvRewriteValidation::JoinAggregate,
        RewriteMergeRefreshEvidence::BranchUnionAggregate => {
            SqlImvRewriteValidation::BranchUnionAggregate
        }
    };
    Ok(novarocks_sql::compiler::SqlImvPlanningInput::new(
        rewrite.to_sql_rewrite_snapshot(target_binding)?,
        validation,
    ))
}

/// Freeze the IMV target exactly once for one compilation request.  The SQL
/// planner receives only the returned scoped token; the provider table/files
/// and retained control generation stay in the application binding store.
pub(crate) fn bind_imv_target_query_table_in_store_from_rewrite(
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    store: &Arc<QueryTableBindingStore>,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<novarocks_sql::binding::SqlTableBindingId, String> {
    let target = &rewrite.target;
    let target_table_uuid = rewrite.target_table_uuid.clone();
    let frozen_snapshot_id = rewrite.target_snapshot_id;
    let planning_lease = planning_lease.clone();
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &planning_lease,
        connector_context.clone(),
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let selector = frozen_snapshot_id
        .map(novarocks_spi::connector::ConnectorReadSelector::SnapshotId)
        .unwrap_or(novarocks_spi::connector::ConnectorReadSelector::Current);
    let target_read = QueryScanMaterialization {
        table: metadata.table.clone(),
        schema: metadata.schema.clone(),
        selector,
        statistics_pin: None,
        planning_lease: planning_lease.clone(),
    };
    let mv_target_read = MvTargetReadAdmission {
        full: target_read.clone(),
        affected_partitions: target_read,
        target_table_uuid: target_table_uuid.clone(),
        frozen_snapshot_id,
    };
    let key = QueryTableBindingKey::mv_target(
        &target.catalog,
        &target.namespace,
        &target.table,
        &target_table_uuid,
        frozen_snapshot_id,
    );
    let token = store.resolve_or_insert_with_id(key, |binding| {
        let resolved = novarocks_sql::planning::catalog::materialize_mv_target_locator_table(
            novarocks_sql::planning::catalog::SqlMvTargetLocatorTableFacts::try_new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
                target_table_uuid.clone(),
                frozen_snapshot_id,
                rewrite
                    .schema_contract
                    .target
                    .hidden_apply_key
                    .column_name
                    .clone(),
                rewrite
                    .schema_contract
                    .branch
                    .as_ref()
                    .map(|branch| branch.branch_id_column.column_name.clone()),
                binding,
            )?,
        )
        .into_resolved_table();
        Ok(QueryTableBinding {
            resolved,
            // IMV target scans use their frozen file materialization; no
            // optimizer statistics are resolved for this target as a side
            // channel during refresh preparation.
            statistics_pin: None,
            admission:
                crate::query_execution::planning::bindings::QueryTableBindingAdmission::Exact(
                    planning_lease,
                ),
            scan_materialization: Some(mv_target_read.full.clone()),
            mv_target_read: Some(mv_target_read),
            write_target_admission: None,
            frozen_snapshot_materializations: BTreeMap::new(),
            admitted_change_scans: BTreeMap::new(),
        })
    })?;
    Ok(token)
}

/// Materialize every pinned IMV base immediately after capture.  The returned
/// overlays retain the exact connector lease, table handle, selected files and
/// delta facts; callers must carry them through later compilation instead of
/// asking a provider for its current generation again.
pub(crate) fn freeze_imv_base_query_local_overlays_from_captured_inputs(
    source: &dyn IcebergMvRefreshSource,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_refs: &[TableIdentity],
    pin: &RefreshSnapshotPin,
    previous_snapshot_ids: &BTreeMap<String, i64>,
) -> Result<
    Vec<crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay>,
    String,
> {
    let mut seen = BTreeSet::new();
    let mut overlays = Vec::with_capacity(base_refs.len());
    for base in base_refs {
        let snapshot_id = pin.get(base).ok_or_else(|| {
            format!(
                "IMV query binding is missing snapshot pin for {}",
                base.fqn()
            )
        })?;
        let identity = format!(
            "{}.{}.{}@{}",
            base.catalog.to_ascii_lowercase(),
            base.namespace.to_ascii_lowercase(),
            base.table.to_ascii_lowercase(),
            snapshot_id
        );
        if !seen.insert(identity) {
            continue;
        }
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&base.catalog)
            .map_err(|error| error.to_string())?;
        let planning_lease = novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            source.connector_control(),
            &instance_id,
        )
        .map_err(|error| error.to_string())?;
        let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
            &planning_lease,
            connector_context.clone(),
            &base.namespace,
            &base.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?;
        let mut materialization = crate::catalog_application::query_catalog::connector_table_materialization_from_metadata(
            metadata,
            planning_lease,
        )?;
        materialization.read_selector =
            novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        let mut frozen_snapshot_ids = std::collections::BTreeSet::from([snapshot_id]);
        let mut admitted_change_scans = BTreeMap::new();
        if let Some(previous_snapshot_id) = previous_snapshot_ids.get(&base.fqn()) {
            frozen_snapshot_ids.insert(*previous_snapshot_id);
            let window = novarocks_spi::connector::ConnectorChangeWindow::new(
                *previous_snapshot_id,
                snapshot_id,
            );
            let admitted_scan =
                crate::query_execution::planning::catalog_materializer::admit_connector_change_window(
                    &materialization.read_table,
                    &materialization.read_schema,
                    &materialization.planning_lease,
                    connector_context.clone(),
                    window,
                )?;
            admitted_change_scans.insert((*previous_snapshot_id, snapshot_id), admitted_scan);
        }

        let catalog = base.catalog.clone();
        let namespace = base.namespace.clone();
        let table = base.table.clone();
        let key = QueryTableBindingKey::snapshot(&catalog, &namespace, &table, snapshot_id);
        overlays.push(
            crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay::new(
                namespace.clone(),
                table.clone(),
                key,
                move |binding| {
                    let mut result = crate::query_execution::planning::catalog_materializer::connector_query_binding_from_materialization(
                        materialization.clone(),
                        &catalog,
                        &namespace,
                        &table,
                        binding,
                    )?;
                    result.admitted_change_scans = admitted_change_scans.clone();
                    for frozen_snapshot_id in frozen_snapshot_ids.iter().copied() {
                        result.frozen_snapshot_materializations.insert(
                            frozen_snapshot_id,
                            QueryScanMaterialization {
                                table: materialization.read_table.clone(),
                                schema: materialization.read_schema.clone(),
                                selector: novarocks_spi::connector::ConnectorReadSelector::SnapshotId(frozen_snapshot_id),
                                statistics_pin: materialization.statistics_pin.clone(),
                                planning_lease: materialization.planning_lease.clone(),
                            },
                        );
                    }
                    Ok(result)
                },
            ),
        );
    }
    Ok(overlays)
}

#[cfg(test)]
mod partition_planning_tests {
    use super::*;
    use mv_schema::{
        BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
        HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
        MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
        TargetContract, TargetVisibleColumn,
    };

    fn key(value: &str) -> crate::mv::model::MvPartitionKey {
        crate::mv::model::MvPartitionKey::new(
            7,
            vec![crate::mv::model::MvPartitionKeyField::new(
                "region".to_string(),
                crate::mv::model::MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    fn base_ref(table: &str) -> TableIdentity {
        TableIdentity {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        }
    }

    fn contract_with_identity_partition() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 1,
            base: BaseContract {
                table_fqn: "ice.db.left".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![BaseFieldRecord {
                        field_id: 1,
                        name_at_create: "id".to_string(),
                        type_signature: "int".to_string(),
                        required: true,
                    }],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![OutputColumnLineage {
                    expression: ExpressionLineage {
                        kind: ExpressionKind::Column,
                        referenced_base_field_ids: vec![1],
                        referenced_base_fields: Vec::new(),
                    },
                }],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 0,
                visible_columns: vec![TargetVisibleColumn {
                    output_name: "id".to_string(),
                    target_field_id: 10,
                    type_signature: "int".to_string(),
                    nullable: false,
                }],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "__nova_base_row_id".to_string(),
                    target_field_id: 11,
                    source: SqlMvApplyKeySourceFacts::BaseRowId.into(),
                },
                partition: Some(MvPartitionContract {
                    target_spec_id: 7,
                    fields: vec![MvPartitionFieldContract {
                        partition_field_id: 100,
                        partition_field_name: "id".to_string(),
                        source_target_field_id: 10,
                        source_column_name: "id".to_string(),
                        transform: MvPartitionTransformContract::Identity,
                    }],
                }),
            },
        }
    }

    #[test]
    fn merge_affected_partition_results_unions_known_sets() {
        let merged = merge_affected_partition_results(
            "UNION ALL MV affected partition planning",
            vec![
                (
                    "ice.db.left".to_string(),
                    crate::mv::model::AffectedTargetPartitions::known([key("west"), key("east")]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::model::AffectedTargetPartitions::known([key("east"), key("north")]),
                ),
            ],
        );

        assert_eq!(
            merged,
            crate::mv::model::AffectedTargetPartitions::known([
                key("east"),
                key("north"),
                key("west"),
            ])
        );
    }

    #[test]
    fn merge_affected_partition_results_preserves_first_not_derived_reason() {
        let merged = merge_affected_partition_results(
            "UNION ALL MV affected partition planning",
            vec![
                (
                    "ice.db.left".to_string(),
                    crate::mv::model::AffectedTargetPartitions::known([key("west")]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::model::AffectedTargetPartitions::not_derived(
                        "missing file partition metadata",
                    ),
                ),
            ],
        );

        assert_eq!(
            merged.not_derived_reason(),
            Some(
                "UNION ALL MV affected partition planning: ice.db.right: missing file partition metadata"
            )
        );
    }

    #[test]
    fn plan_multi_base_affected_partitions_unchanged_bases_return_empty_known_set() {
        let contract = contract_with_identity_partition();
        let base_refs = vec![base_ref("left"), base_ref("right")];
        let previous_snapshots = BTreeMap::from([
            ("ice.db.left".to_string(), 11_i64),
            ("ice.db.right".to_string(), 22_i64),
        ]);
        let current_snapshots = BTreeMap::from([
            ("ice.db.left".to_string(), Some(11_i64)),
            ("ice.db.right".to_string(), Some(22_i64)),
        ]);

        let planned = plan_multi_base_affected_partitions(
            &contract,
            RefreshMode::Incremental,
            &base_refs,
            &previous_snapshots,
            &current_snapshots,
            |_base_ref, _previous, _current| {
                panic!("unchanged bases should not require change-window admission")
            },
            "UNION ALL MV affected partition planning",
        );

        assert_eq!(
            planned,
            crate::mv::model::AffectedTargetPartitions::known(std::iter::empty::<
                crate::mv::model::MvPartitionKey,
            >(),)
        );
    }
}

#[cfg(test)]
mod join_delta_append_only_fast_path_tests {
    use super::*;

    #[test]
    fn join_incremental_refresh_plan_kind_uses_logical_cutover() {
        let mode = select_join_incremental_execution_mode(false, false);
        assert_eq!(
            mode,
            crate::mv::application::MvIncrementalJoinMode::AppendOnly
        );
        let mode = select_join_incremental_execution_mode(true, false);
        assert_eq!(
            mode,
            crate::mv::application::MvIncrementalJoinMode::Coalesce
        );
        let mode = select_join_incremental_execution_mode(false, true);
        assert_eq!(
            mode,
            crate::mv::application::MvIncrementalJoinMode::Coalesce
        );
    }

    #[test]
    fn aggregate_refresh_explain_disables_join_refresh_descriptor_recording() {
        let disabled_rules = refresh_explain_rewrite_disabled_rules(
            true,
            &novarocks_sql::compiler::SessionOptimizerSettings::default(),
        );

        assert!(
            disabled_rules
                .iter()
                .any(|rule| rule == "RecordJoinRefreshDescriptor"),
            "aggregate refresh explain must not record a pure join refresh descriptor"
        );
    }

    #[test]
    fn join_delta_append_only_fast_path_requires_append_only_inner_or_cross_join() {
        assert!(should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            false,
            false,
        ));
        assert!(should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l cross join ice.ns.right r"),
            false,
            false,
        ));

        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            true,
            false,
        ));
        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"),
            false,
            true,
        ));
        assert!(!should_use_join_delta_append_only_fast_path(
            &parse_query("select l.id from ice.ns.left l left join ice.ns.right r on l.id = r.id"),
            false,
            false,
        ));
    }

    #[test]
    fn join_delta_coalesce_uses_normalized_snapshot_ctes() {
        let base_query = parse_query(
            "select l.id, r.label from ice.ns.left l join ice.ns.right r on l.id = r.id",
        );
        let left = base("left");
        let right = base("right");
        let branches = crate::mv::iceberg_join_branch::plan_join_delta_branches(
            &left,
            &right,
            crate::mv::iceberg_join_branch::SnapshotWindow { from: 10, to: 11 },
            crate::mv::iceberg_join_branch::SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        let mut branch_queries = Vec::new();
        for branch in &branches {
            let mut branch_query = crate::mv::iceberg_join_branch::rewrite_join_branch_query(
                &base_query,
                branch,
                "l",
                "r",
            )
            .expect("branch rewrite");
            normalize_join_branch_snapshot_tables(&mut branch_query, branch)
                .expect("snapshot normalization");
            branch_queries.push(branch_query);
        }

        let coalesced =
            crate::mv::iceberg_join_branch::rewrite_join_delta_coalesce_query_with_branch_queries(
                &base_query,
                branch_queries,
                "left-uuid",
                "right-uuid",
            )
            .expect("coalesce rewrite");
        let rendered = coalesced.to_string();

        assert!(rendered.contains("right__at_20"), "sql={rendered}");
        assert!(rendered.contains("left__at_11"), "sql={rendered}");
        assert!(!rendered.contains("VERSION AS OF"), "sql={rendered}");
        assert!(
            rendered.contains("__nr_join_delta_branch_0"),
            "sql={rendered}"
        );
        assert!(
            rendered.contains("__nr_join_delta_branch_1"),
            "sql={rendered}"
        );
    }

    fn base(name: &str) -> TableIdentity {
        TableIdentity {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }
}

/// Compile an `EXPLAIN REFRESH MATERIALIZED VIEW` plan using only the
/// frontend-composed MV and statistics capabilities.  The query-local table
/// overlays retain the same snapshot pins and connector leases used by the
/// refresh path; this diagnostic must not consult an application facade or a
/// second latest-generation source.
pub(crate) fn explain_iceberg_mv_refresh_rewrite_plan_with_ports(
    source: &IcebergMvCorePorts,
    _statistics_resolver: &impl crate::query_execution::planning::statistics::QueryStatisticsResolver,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, String> {
    explain_refresh_full_guard(stmt.full)?;

    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name)?;
    let mv_definition = load_iceberg_mv_definition_by_target(source, &target)?;
    let target_binding = target_binding_for(source, &target, connector_context)?;
    validate_target_snapshot(&target, &mv_definition, &target_binding)?;

    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql)?,
        current_catalog,
        current_database,
    );
    // Capability-driven (not classifier-driven): the aggregate metadata
    // validation runs when the persisted contract carries an aggregate state.
    // Deriving this from the schema contract instead of re-classifying the
    // SELECT keeps EXPLAIN REFRESH working for composed branch-union shapes
    // whose branches contain joins (the legacy classifier rejects those).
    let dispatch_schema_contract = mv_definition.schema_contract.as_ref().ok_or_else(|| {
        format!(
            "iceberg MV target {}.{}.{} is missing A11 schema contract; rebuild or recreate the MV",
            target.catalog, target.namespace, target.table
        )
    })?;
    if RefreshCapabilities::from_schema_contract(dispatch_schema_contract)?.has_agg_state {
        validate_aggregate_schema_contract_metadata(&target, &mv_definition)?;
    }

    let pin = capture_refresh_snapshot_pin_with_ports(
        source.connector_control(),
        source.storage_observation(),
        &base_refs,
        connector_context,
    )?;
    validate_refresh_pin_table_uuids(&mv_definition, &pin, &base_refs)?;
    let rewrite = build_neutral_refresh_rewrite_context(
        source,
        &target,
        mv_definition.mv_id,
        current_catalog,
        current_database,
        Arc::new(mv_definition.clone()),
        Arc::new(canonical_select_query),
        Arc::from(base_refs.clone()),
        Arc::new(pin.clone()),
        mv_definition.last_refresh_snapshots.clone(),
        mv_definition.last_refresh_table_uuids.clone(),
        target_binding.current_snapshot_id(),
        target_binding.table_uuid().to_string(),
        None,
        connector_context,
    )?;
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = bind_imv_target_query_table_in_store_from_rewrite(
        &rewrite,
        &bindings,
        target_binding.lease(),
        connector_context,
    )?;
    let catalog_service_snapshot =
        crate::catalog_application::query_catalog::catalog_service_snapshot(source);
    let overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        source,
        connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
    )?;
    let materializer = crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        None,
        &catalog_service_snapshot,
        Arc::clone(&bindings),
        crate::query_execution::planning::statistics::iceberg_table_binding_loader(
            source.connector_control(),
            connector_context.clone(),
        ),
        overlays,
    );
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
    novarocks_sql::compiler::compile_imv_refresh_explain_lines(
        novarocks_sql::compiler::SqlImvRefreshExplainContext {
            canonical_query: Box::new((*rewrite.canonical_select_query).clone()),
            imv_rewrite: novarocks_sql::compiler::SqlImvPlanningInput::new(
                rewrite.to_sql_rewrite_snapshot(target_binding)?,
                novarocks_sql::compiler::SqlImvRewriteValidation::None,
            ),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings::default(),
            environment: novarocks_sql::compiler::SqlPlanningEnvironment::NotApplicable,
            catalog: &catalog,
            functions: novarocks_sql::compiler::builtin_sql_function_catalog(),
            control: novarocks_sql::compiler::SqlCompileControl::new(
                Some(connector_context.deadline()),
                Arc::new(MvRefreshConnectorCancellationObservation {
                    cancellation: connector_context.cancellation().clone(),
                }),
            ),
            level,
        },
    )
    .map_err(|error| error.to_string())
}

struct MvRefreshConnectorCancellationObservation {
    cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
}

impl novarocks_sql::compiler::SqlCancellationObservation
    for MvRefreshConnectorCancellationObservation
{
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn normalize_join_branch_snapshot_tables(
    query: &mut sqlparser::ast::Query,
    branch: &crate::mv::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<(), String> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch snapshot normalization requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one JOIN".to_string());
    };
    if let crate::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.left {
        rewrite_snapshot_table_factor(&mut from.relation, &branch.left_base, snapshot_id, None)?;
    }
    if let crate::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.right {
        rewrite_snapshot_table_factor(&mut join.relation, &branch.right_base, snapshot_id, None)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RewriteMergeRefreshOptions {
    apply_key: ApplyKeyContract,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RewriteMergeRefreshEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// Activate a value-only incremental refresh artifact after frontend intent
/// persistence and exact-lease admission. Core rebuilds only provider-private
/// scan and writer facts here; it returns a sealed native-assembly carrier and
/// never advances MV metadata or executes an external commit.
pub(crate) fn bind_prepared_mv_incremental_staging(
    query_kernel: &crate::query_execution::kernels::QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    prepared: crate::mv::application::PreparedMvIncrementalWrite,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::mv_native_write::PreparedMvNativeWriteAssembly, String> {
    let (request, facts, mode, evidence, execution_artifact, publication_intent) =
        prepared.into_parts();
    if request.observed_binding != *exact_lease.binding_key() {
        return Err("MV incremental write lease drifted from prepared binding".to_string());
    }
    let connector_context =
        crate::connector::connector_request_context_for_execution(None, execution)?;
    let refresh_rewrite = crate::query_execution::mv_assembly::first_refresh_staging::rebuild_frozen_mv_rewrite_context(
        ports,
        request.current_catalog.as_deref(),
        &request.current_database,
        request.expected_target_snapshot_id,
        &request.target_catalog,
        &request.target_namespace,
        &request.target_name,
        &facts,
        planning_lease,
        &connector_context,
    )?;
    let target = crate::catalog_application::resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: request.target_catalog,
        namespace: request.target_namespace,
        table: request.target_name,
    };
    let target_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = bind_imv_target_query_table_in_store_from_rewrite(
        &refresh_rewrite,
        &target_bindings,
        planning_lease,
        &connector_context,
    )?;
    let target_metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        planning_lease,
        connector_context.clone(),
        &target.namespace,
        &target.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let mutation_intent = novarocks_spi::connector::ConnectorRowMutationIntent::Merge {
        effects: match mode {
            crate::mv::application::MvIncrementalWriteMode::FastAppend => {
                vec![novarocks_spi::connector::ConnectorRowMutationEffect::Insert]
            }
            crate::mv::application::MvIncrementalWriteMode::RowDelta => vec![
                novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
                novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
                novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            ],
        },
    };
    let mutation_preparation = match exact_lease
        .prepare_row_mutation(
            novarocks_spi::connector::ConnectorRowMutationPreparationRequest {
                operation_id: request.operation_id,
                table: target_metadata.table,
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(
                    request.staging_branch.clone(),
                )
                .map_err(|error| error.to_string())?,
                intent: mutation_intent,
                context: connector_context.clone(),
            },
        )
        .map_err(|error| error.to_string())?
    {
        novarocks_spi::connector::ConnectorRowMutationPreparationOutcome::Prepared(preparation) => {
            preparation
        }
        novarocks_spi::connector::ConnectorRowMutationPreparationOutcome::Denied(error) => {
            return Err(error.to_string());
        }
    };
    let provider_plan = exact_lease
        .activate_row_mutation(
            novarocks_spi::connector::ConnectorRowMutationActivationRequest::Direct {
                preparation: mutation_preparation,
                context: connector_context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    let mutation_lease = exact_lease.clone();
    let mut provider_routes = provider_plan.routes().to_vec();
    provider_routes.sort_by_key(|route| route.cohort_id());
    let selected_cohort = provider_routes
        .first()
        .map(|route| route.cohort_id())
        .ok_or_else(|| "MV incremental provider plan has no writer routes".to_string())?;
    let sealed_change_stream_routes = provider_routes
        .iter()
        .map(|route| {
            let target_binding = crate::query_execution::planning::write_sink::admit_prepared_connector_write_target(
                target_bindings.as_ref(),
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target.catalog.clone(),
                    target.namespace.clone(),
                    target.table.clone(),
                )?,
                route.preparation().clone(),
                planning_lease.clone(),
            )?;
            let mode = match route.input() {
                novarocks_spi::connector::ConnectorWriteInputShape::Data { .. } => {
                    novarocks_sql::planning::dml::DmlWriteSinkMode::Data
                }
                novarocks_spi::connector::ConnectorWriteInputShape::RowLineage { .. } => {
                    novarocks_sql::planning::dml::DmlWriteSinkMode::RowLineageData
                }
                novarocks_spi::connector::ConnectorWriteInputShape::PositionDelete { .. } => {
                    novarocks_sql::planning::dml::DmlWriteSinkMode::PositionDeletes
                }
                novarocks_spi::connector::ConnectorWriteInputShape::DeletionVector { .. } => {
                    novarocks_sql::planning::dml::DmlWriteSinkMode::DeletionVectors
                }
                novarocks_spi::connector::ConnectorWriteInputShape::EqualityDelete { .. } => {
                    novarocks_sql::planning::dml::DmlWriteSinkMode::EqualityDeletes
                }
            };
            let sink = crate::query_execution::planning::write_sink::dml_write_plan_input_for_admitted_target(
                target_bindings.as_ref(),
                target_binding,
                mode,
                novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )?;
            Ok(novarocks_sql::planning::dml::DmlChangeStreamRoute {
                route_id: route.route_id(),
                cohort_id: route.cohort_id(),
                accepted_effects: route.accepted_effects().to_vec(),
                input_fields: route
                    .input()
                    .fields()
                    .into_iter()
                    .map(|field| novarocks_sql::planning::dml::DmlChangeStreamRouteField {
                        token: field.token(),
                        output_name: field.field().name().to_string(),
                    })
                    .collect(),
                partition_input_tokens: route.partition_fields().to_vec(),
                sink,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let managed_publication =
        crate::query_execution::mv_assembly::iceberg_activation::managed_publication_activation_intent(
            &publication_intent,
            novarocks_spi::connector::ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit,
        )?;
    let activation = mutation_lease
        .activate_write(novarocks_spi::connector::ConnectorWriteActivationRequest {
            operation_id: request.operation_id,
            source: novarocks_spi::connector::ConnectorWriteActivationSource::RowMutation(
                provider_plan,
            ),
            intent: novarocks_spi::connector::ConnectorWriteActivationIntent::ManagedPublication(
                managed_publication,
            ),
            context: connector_context.clone(),
        })
        .map_err(|error| format!("activate exact Iceberg MV incremental write: {error}"))?;
    let activated_cohort = activation.cohort(selected_cohort).ok_or_else(|| {
        "MV incremental activation omitted its selected Provider cohort".to_string()
    })?;
    let connector_write =
        crate::query_execution::contract::ConnectorWritePlanningTemplate::from_activated_cohort(
            activated_cohort,
            connector_context.clone(),
            mutation_lease,
        )
        .map_err(|error| format!("build activated MV incremental write template: {error}"))?;
    let rewrite_evidence = match evidence {
        crate::mv::application::MvIncrementalRewriteEvidence::None => {
            RewriteMergeRefreshEvidence::None
        }
        crate::mv::application::MvIncrementalRewriteEvidence::Aggregate => {
            RewriteMergeRefreshEvidence::Aggregate
        }
        crate::mv::application::MvIncrementalRewriteEvidence::JoinAggregate => {
            RewriteMergeRefreshEvidence::JoinAggregate
        }
        crate::mv::application::MvIncrementalRewriteEvidence::BranchUnionAggregate => {
            RewriteMergeRefreshEvidence::BranchUnionAggregate
        }
    };
    match execution_artifact {
        crate::mv::application::MvIncrementalExecutionArtifact::CanonicalQuery => {
            let imv_rewrite_input = sql_imv_planning_input_from_rewrite(
                &refresh_rewrite,
                target_binding,
                rewrite_evidence,
            )?;
            let catalog_service_snapshot =
                crate::catalog_application::query_catalog::catalog_service_snapshot(query_kernel);
            let base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
                ports,
                &connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let analyzer_catalog = crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&target_bindings),
                crate::query_execution::planning::statistics::iceberg_table_binding_loader(
                query_kernel.connector_control().as_ref(),
                    connector_context.clone(),
                ),
                base_overlays,
            );
            let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
                .ok_or_else(|| {
                "IMV incremental refresh requires a non-empty admitted backend topology".to_string()
            })?;
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_catalog);
            let write_mode = match mode {
                crate::mv::application::MvIncrementalWriteMode::FastAppend => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::FastAppend
                }
                crate::mv::application::MvIncrementalWriteMode::RowDelta => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::RowDelta
                }
            };
            let analyzed = novarocks_sql::planning::mv::first_refresh::analyze_mv_incremental_refresh_change_stream(
                novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalRefreshAnalyzeContext {
                    canonical_query: Box::new((*refresh_rewrite.canonical_select_query).clone()),
                    imv_rewrite: imv_rewrite_input,
                    write_mode,
                    routes: sealed_change_stream_routes,
                    current_catalog: None,
                    current_database: refresh_rewrite.current_database.clone(),
                    environment: novarocks_sql::compiler::SqlPlanningEnvironment::Distributed {
                        backend_count,
                    },
                    catalog: &catalog,
                    functions: novarocks_sql::compiler::builtin_sql_function_catalog(),
                    control: novarocks_sql::compiler::SqlCompileControl::new(
                        execution.deadline(),
                        crate::query_execution::planning::sql_cancellation_observation(
                            execution.cancellation().clone(),
                        ),
                    ),
                },
            )?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                analyzer_catalog.query_table_bindings(),
                &connector_context,
            )?;
            let sealed = novarocks_sql::planning::mv::first_refresh::compile_mv_incremental_refresh_change_stream(
                analyzed,
                &statistics,
            )?;
            let planned =
                crate::query_execution::compiler::prepare_dml_change_stream_write_with_execution(
                    query_kernel.connector_control().as_ref(),
                    execution,
                    sealed,
                    target_bindings.as_ref(),
                    &connector_context,
                )?;
            let distributed =
                crate::query_execution::compiler::prepare_planned_iceberg_change_stream_write(
                    planned.encoding,
                    None,
                    Some(
                        crate::query_execution::compiler::DistributedConnectorWrite::Begin(
                            connector_write,
                        ),
                    ),
                )?;
            if distributed.write_operation_id() != request.operation_id
                || distributed.write_cohort_id() != selected_cohort
            {
                return Err("MV incremental distributed artifact identity mismatch".to_string());
            }
            return Ok(distributed);
        }
        crate::mv::application::MvIncrementalExecutionArtifact::JoinLogical {
            mode: join_execution_mode,
        } => {
            let join_mode = match join_execution_mode {
                crate::mv::application::MvIncrementalJoinMode::AppendOnly => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::AppendOnly
                }
                crate::mv::application::MvIncrementalJoinMode::Coalesce => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::Coalesce
                }
            };
            let write_mode = match mode {
                crate::mv::application::MvIncrementalWriteMode::FastAppend => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::FastAppend
                }
                crate::mv::application::MvIncrementalWriteMode::RowDelta => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::RowDelta
                }
            };
            let base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
                ports,
                &connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let catalog_service_snapshot =
                crate::catalog_application::query_catalog::catalog_service_snapshot(query_kernel);
            let analyzer_catalog = crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&target_bindings),
                crate::query_execution::planning::statistics::iceberg_table_binding_loader(
                    query_kernel.connector_control().as_ref(),
                    connector_context.clone(),
                ),
                base_overlays,
            );
            let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
                .ok_or_else(|| {
                "IMV join incremental refresh requires a non-empty admitted backend topology"
                    .to_string()
            })?;
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_catalog);
            let analyzed = novarocks_sql::planning::mv::first_refresh::analyze_join_incremental_refresh_change_stream(
                novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshAnalyzeContext {
                    canonical_query: Box::new((*refresh_rewrite.canonical_select_query).clone()),
                    rewrite_snapshot: refresh_rewrite.to_sql_rewrite_snapshot(target_binding)?,
                    join_mode,
                    write_mode,
                    routes: sealed_change_stream_routes,
                    current_catalog: None,
                    current_database: refresh_rewrite.current_database.clone(),
                    optimizer_settings: execution.optimizer_settings().clone(),
                    environment: novarocks_sql::compiler::SqlPlanningEnvironment::Distributed {
                        backend_count,
                    },
                    catalog: &catalog,
                    functions: novarocks_sql::compiler::builtin_sql_function_catalog(),
                    control: novarocks_sql::compiler::SqlCompileControl::new(
                        execution.deadline(),
                        crate::query_execution::planning::sql_cancellation_observation(
                            execution.cancellation().clone(),
                        ),
                    ),
                },
            )?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                analyzer_catalog.query_table_bindings(),
                &connector_context,
            )?;
            let sealed = novarocks_sql::planning::mv::first_refresh::compile_join_incremental_refresh_change_stream(
                analyzed,
                &statistics,
            )?;
            let planned =
                crate::query_execution::compiler::prepare_dml_change_stream_write_with_execution(
                    query_kernel.connector_control().as_ref(),
                    execution,
                    sealed,
                    target_bindings.as_ref(),
                    &connector_context,
                )?;
            let distributed =
                crate::query_execution::compiler::prepare_planned_iceberg_change_stream_write(
                    planned.encoding,
                    None,
                    Some(
                        crate::query_execution::compiler::DistributedConnectorWrite::Begin(
                            connector_write,
                        ),
                    ),
                )?;
            if distributed.write_operation_id() != request.operation_id
                || distributed.write_cohort_id() != selected_cohort
            {
                return Err("MV incremental distributed artifact identity mismatch".to_string());
            }
            return Ok(distributed);
        }
    }
}

pub(crate) fn drop_iceberg_mv_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let _refresh_guard = acquire_mv_refresh_lock()?;
    let target = resolve_drop_target(current_catalog, current_database, &stmt.name)?;
    if !preflight_iceberg_mv_drop_with_repository(
        ports.repository.as_ref(),
        &target,
        stmt.if_exists,
    )? {
        return Ok(StatementResult::Ok);
    }

    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    crate::connector::mutation::execute_catalog_mutation(
        ports.connector_control.as_ref(),
        &instance_id,
        novarocks_spi::connector::ConnectorCatalogMutationOperation::DropTable {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
            data_disposition: novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
        },
        connector_context.clone(),
    )?;
    drop_iceberg_mv_metadata_with_repository(ports.repository.as_ref(), &target)?;
    crate::catalog_application::query_catalog::drop_local_table_registration_if_exists(
        ports,
        &target.namespace,
        &target.table,
    )?;

    tracing::info!(
        "iceberg mv {}.{}.{}: dropped successfully",
        target.catalog,
        target.namespace,
        target.table
    );
    Ok(StatementResult::Ok)
}

fn drop_iceberg_mv_metadata_with_repository(
    repository: &dyn MvRepository,
    target: &IcebergMvTarget,
) -> Result<(), String> {
    let dropped = repository
        .drop_by_target(&crate::mv::model::MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("drop iceberg mv metadata failed: {e}"))?;
    if !dropped {
        return Err(format!(
            "materialized view {}.{}.{} metadata disappeared during drop",
            target.catalog, target.namespace, target.table
        ));
    }
    Ok(())
}

fn preflight_iceberg_mv_drop_with_repository(
    repository: &dyn MvRepository,
    target: &IcebergMvTarget,
    if_exists: bool,
) -> Result<bool, String> {
    let Some(definition) = repository
        .find_by_target(&crate::mv::model::MvTarget {
            catalog: Some(target.catalog.clone()),
            database: target.namespace.clone(),
            name: target.table.clone(),
        })
        .map_err(|e| format!("load iceberg mv definition for drop failed: {e}"))?
    else {
        if if_exists {
            return Ok(false);
        }
        return Err(format!(
            "materialized view does not exist: {}.{}.{}",
            target.catalog, target.namespace, target.table
        ));
    };
    if definition.refresh_in_progress || definition.active_refresh_id.is_some() {
        return Err(format!(
            "cannot drop materialized view {}.{}.{}: refresh in progress",
            target.catalog, target.namespace, target.table
        ));
    }
    crate::mv::dependency_resolver::ensure_no_downstream_dependencies_with_repository(
        repository,
        &crate::mv::dependency::model::iceberg_mv_dependency_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        ),
    )?;
    Ok(true)
}

fn resolve_drop_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "DROP MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: novarocks_catalog::identifier::normalize_identifier(catalog)?,
        namespace,
        table,
    })
}
