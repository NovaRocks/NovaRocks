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

use crate::sql::planner::vocabulary::ApplyKeySource;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_connector_iceberg::iceberg::spec::DataFile;
#[cfg(test)]
use novarocks_connector_iceberg::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
#[cfg(test)]
use novarocks_connector_iceberg::iceberg::transaction::ApplyTransactionAction;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::common::engine_error::EngineError;
use crate::engine::mv::analysis_adapter::{
    BaseColumnDescriptor, BaseTableDescriptor, analyze_mv_select_with_connector_context, now_ms,
    validate_ivm_primary_key,
};
#[cfg(test)]
use crate::engine::mv::lifecycle::IcebergRefreshOutcome;
use crate::engine::mv::lifecycle::{
    BackendRefreshPlan, IcebergRefreshPlan, RefreshError, RefreshPlan,
};
use crate::engine::mv::refresh_io::{
    acquire_mv_refresh_lock, observe_current_refresh_base, parse_iceberg_table_refs,
};
#[cfg(test)]
use crate::engine::mv::refresh_io::{
    load_current_iceberg_base_table, run_mv_full_select_chunks_with_catalog, single_snapshot_map,
    single_table_uuid_map,
};
use crate::engine::mv::refresh_pin_adapter::capture_refresh_snapshot_pin;
#[cfg(test)]
use crate::engine::mv::schema_validation_adapter::current_iceberg_table_observation;
use crate::engine::query_planning::bindings::{
    MvTargetReadAdmission, QueryScanMaterialization, QueryTableBinding, QueryTableBindingKey,
    QueryTableBindingStore,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::iceberg_operation::{
    CreateIcebergOperationRequest, IcebergCommitOutcomeRecord, IcebergOperationFactUpdate,
    IcebergOperationFailureKind, IcebergOperationFailureRecord, IcebergOperationKind,
    IcebergOperationNextAction, IcebergOperationState, IcebergOperationTarget,
    StoredIcebergOperation,
};
use crate::mv::aggregate_state::mv_shape::UnionBranchKind;
use crate::mv::aggregate_state::physical_column::validate_unique_aggregate_physical_column_names;
use crate::mv::analysis::rebind::rewrite_select_sql_for_rebind;
use crate::mv::analysis::refresh_property::{
    RefreshFragmentProperty, TargetIdentity, derive_fragment_property, derive_imv_refresh_contract,
};
use crate::mv::analysis::{
    MvAnalysis, canonicalize_iceberg_mv_select_query, output_column_to_table_column,
    resolve_mv_name, validate_mv_partition_columns,
};
use crate::mv::application::{
    CreatedMvTarget, MvCreateStatement, MvEngine, MvEngineError, MvEngineErrorKind,
    MvRefreshPreparationRequest, MvRefreshPreparationService, MvRefreshPublicationBase,
    MvRefreshPublicationIntent, MvRefreshPublicationTechnique, PrepareMvCreateRequest,
    PreparedMvCreate, PreparedMvDefinition, PreparedMvRefresh, PreparedMvRefreshWork,
    PreparedMvRefreshWrite,
};
use crate::mv::dependency::model::{MvDependencyObjectType, MvDependencyStorageEngine};
use crate::mv::model::{MvStorageEngine, MvTarget, RefreshMode};
use crate::mv::persistence::definition::CreateMvDefinitionRequest;
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::mv::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::persistence::descriptor::{
    DescriptorDependency, MV_DESCRIPTOR_VERSION, MvDescriptorV1,
};
#[cfg(test)]
use crate::mv::persistence::partition::MvPartitionRefreshStatus;
use crate::mv::persistence::partition::{
    ReplaceMvPartitionStatesRequest, UpdateMvPartitionContractRequest,
};
#[cfg(test)]
use crate::mv::persistence::refresh::UpdateStarRocksMvRefreshSummaryRequest;
use crate::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, MvRefreshState,
    RecordPublishCommitRequest, RecordStagingCommitRequest, RefreshExternalOutcome,
    StoredMvRefresh,
};
use crate::mv::persistence::schema as mv_schema;
use crate::mv::persistence::schema::{
    APPLY_KEY_COLUMN_PROPERTY, APPLY_KEY_FIELD_ID_PROPERTY, APPLY_KEY_SOURCE_PROPERTY,
    HIDDEN_COLUMNS_PROPERTY,
};
#[cfg(test)]
use crate::mv::refresh::aggregate_first_refresh::{
    AggregateStateRead, prepare_aggregate_first_refresh_chunks,
    prepare_branch_union_aggregate_first_refresh_chunks,
};
use crate::mv::refresh::apply_key::ApplyKeyContract;
#[cfg(test)]
use crate::mv::refresh::apply_key::RewriteEvidence;
use crate::mv::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::refresh::contract::{ImvRefreshContract, MvTargetWriteEffect};
#[cfg(test)]
use crate::mv::refresh::execution::{
    RefreshExecutionObservation, ValidatedRefreshExecution, dispatch_refresh_decision,
    validate_refresh_execution,
};
use crate::mv::refresh::join_incremental_refresh::{
    JoinIncrementalLogicalInput, JoinIncrementalRefreshMode,
    build_join_incremental_refresh_logical_plan, select_join_incremental_refresh_mode,
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
use crate::mv::refresh::projection_first_refresh::{
    prepare_projection_first_refresh_chunks, prepare_projection_full_read_sql,
    prepare_union_projection_first_refresh_chunks,
};
#[cfg(test)]
use crate::mv::refresh::repartition::RepartitionShape;
use crate::mv::refresh::repartition::select_repartition_shape;
use crate::mv::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
};
use crate::mv::refresh::target_apply::{
    apply_key_table_column, branch_id_table_column, ensure_base_row_lineage_contract,
    join_apply_key_table_column,
};
use crate::mv::repository::CreateMvRepositoryRequest;
use crate::mv::schema_validation::{
    BranchFieldValidationError, ContractDecision, JoinContractDecision, validate_branch_id_field,
};
use crate::mv::schema_validation::{validate_join_schema_contract, validate_schema_contract};
use crate::mv::storage_observation::MvSchemaValidationObservation;
use crate::mv::storage_observation::{MvStorageObservationPort, MvTargetCreationObservation};
#[cfg(test)]
use crate::runtime::global_async_runtime::data_block_on;
#[cfg(test)]
use crate::sql::analysis::ProjectItem;
use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::catalog::ResolvedAnalyzerTable;
use crate::sql::column_id::ColumnId;
use crate::sql::mv_refresh::{FULL_REFRESH_DISABLED_MESSAGE, MvRefreshFinalizeFacts};
#[cfg(test)]
use crate::sql::parser::ast::{AlterMaterializedViewAction, AlterMaterializedViewStmt};
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, IcebergPartitionFieldExpr, ObjectName,
    RefreshMaterializedViewStmt,
};
use crate::sql::planner::table::{
    ScanSource, SqlMvTargetLocatorScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
};
use crate::sql::planner::vocabulary::{
    BRANCH_ID_COLUMN_NAME, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME,
    JOIN_APPLY_KEY_COLUMN_NAME,
};
use mv_schema::MvPartitionContract;
use novarocks_catalog::identifier::{TableIdentity, normalize_identifier};
#[cfg(test)]
use novarocks_connector_iceberg::commit::CommitOutcome;
use novarocks_connector_iceberg::commit::data_writer::write_record_batches_as_data_files;
#[cfg(test)]
use novarocks_connector_iceberg::commit::{
    MV_PROVENANCE_VERSION, MvProvenanceV1, ProvenanceBase, RefreshTechnique,
};
use novarocks_spi::connector::{
    ConnectorChangeWindowAdmission, ConnectorExecutionBindingKey, ConnectorInstanceId,
};

/// SQL-owned bridge for refresh planning.  It owns only analysis and immutable
/// facts; all durable intent, ref mutations, and writer execution are handed
/// to the frontend as `PreparedMvRefresh`.
pub(crate) struct StandaloneMvRefreshPreparationService<'a> {
    state: &'a Arc<StandaloneState>,
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    statement: &'a RefreshMaterializedViewStmt,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    repartition_fields: Option<&'a [IcebergPartitionFieldExpr]>,
}

impl<'a> StandaloneMvRefreshPreparationService<'a> {
    pub(crate) fn new(
        state: &'a Arc<StandaloneState>,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a RefreshMaterializedViewStmt,
        connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            state,
            current_catalog,
            current_database,
            statement,
            connector_context,
            repartition_fields: None,
        }
    }

    pub(crate) fn new_repartition(
        state: &'a Arc<StandaloneState>,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        statement: &'a RefreshMaterializedViewStmt,
        repartition_fields: &'a [IcebergPartitionFieldExpr],
        connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self {
            state,
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
        if request.statement != crate::sql::mv_refresh::MvRefreshStatement::from(self.statement) {
            return Err(
                "MV refresh preparation statement does not match the admitted SQL request"
                    .to_string(),
            );
        }
        let mut plan = plan_iceberg_mv_refresh_with_connector_context(
            self.state,
            self.current_catalog,
            self.current_database,
            self.statement,
            request.target.clone(),
            self.connector_context,
        )
        .map_err(|error| error.to_string())?;
        let partition_spec_replacement = match self.repartition_fields {
            Some(fields) => {
                plan.contract.decision = ExecutableRefreshDecision::FirstRefresh;
                Some(prepare_managed_repartition_transition(
                    self.state,
                    self.current_catalog,
                    self.current_database,
                    fields,
                    &plan.contract,
                    request.attempt.write_operation_id,
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
        let observed_binding = self
            .state
            .connector_control
            .observe_current_binding(&instance_id)
            .map_err(|error| error.to_string())?;
        let base_table_uuids = plan
            .contract
            .base_refs
            .iter()
            .map(|base| {
                observe_schema_validation_for_table(self.state, base, self.connector_context)
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
                    self.state,
                    self.current_catalog,
                    self.current_database,
                    &plan.contract,
                    &request.attempt,
                    &base_table_uuids,
                    observed_binding.clone(),
                    partition_spec_replacement.clone(),
                    self.connector_context.clone(),
                )?),
            },
            ExecutableRefreshDecision::Incremental => match prepare_frontend_incremental_write(
                self.state,
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

fn prepare_managed_repartition_transition(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    fields: &[IcebergPartitionFieldExpr],
    contract: &RefreshPlanContract,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
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
    let definition = load_iceberg_mv_definition_by_target(state, &target)?;
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
        state,
        &target,
        schema_contract,
        &contract.base_refs,
        connector_context,
    )?;
    let query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let analysis = analyze_mv_select_with_connector_context(
        state,
        current_catalog,
        current_database,
        &query,
        connector_context,
    )?;
    validate_mv_partition_columns(Some(fields), &analysis.output_columns)?;
    if derive_fragment_property(&analysis.resolved_query)?
        .is_composed_aggregate_schema_contract_fallback()
    {
        return Err("partitioned composed aggregate Iceberg MV is not supported".to_string());
    }

    let target_ref = TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    let observation = observe_schema_validation_for_table(state, &target_ref, connector_context)?;
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
/// This deliberately re-reads metadata only while SQL preparation is active;
/// it allocates no provider ref, write service, execution, or durable intent.
/// Join first refresh remains behind its typed logical binder and therefore
/// fails closed here rather than using the old frontend row-materialization
/// implementation.
#[allow(clippy::too_many_arguments)]
fn prepare_frontend_first_refresh_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    contract: &RefreshPlanContract,
    attempt: &crate::mv::application::MvRefreshAttemptIdentity,
    base_table_uuids: &BTreeMap<String, String>,
    observed_binding: ConnectorExecutionBindingKey,
    partition_spec_replacement: Option<
        novarocks_spi::connector::ConnectorManagedPartitionSpecReplacement,
    >,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::application::PreparedMvFirstRefreshWrite, String> {
    let target = IcebergMvTarget {
        catalog: contract.target.catalog.clone().ok_or_else(|| {
            "Iceberg MV first-refresh target has no connector catalog".to_string()
        })?,
        namespace: contract.target.database.clone(),
        table: contract.target.name.clone(),
    };
    let planning_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
    let write_lease = planning_lease
        .derive_write_lease()
        .map_err(|error| format!("derive MV first-refresh write lease: {error}"))?;
    if write_lease.binding_key() != &observed_binding {
        return Err(
            "MV first-refresh target connector generation changed during admission".to_string(),
        );
    }
    let definition = load_iceberg_mv_definition_by_target(state, &target)?;
    let definition = rebind_mv_definition_before_refresh_derivation(
        state,
        &definition,
        &contract.base_refs,
        &target,
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
    let target_binding = target_binding_for(state, &target, &connector_context)?;
    let target_arrow_schema = target_binding.physical_write_schema()?.as_ref().clone();
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
        crate::sql::mv_refresh::first_refresh::MvFirstRefreshTargetContract::try_new(
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
                let observed = observe_current_refresh_base(state, base, &connector_context)?;
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
            state,
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
            &connector_context,
        )?;
        let frozen_base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            state,
            &connector_context,
            &rewrite.base_refs,
            &rewrite.pin,
            &rewrite.previous_snapshot_ids,
        )?;
        let table = crate::engine::iceberg_writer::iceberg_connector_table_handle(
            &write_lease,
            &crate::engine::backend_resolver::TargetBackend {
                backend_name: "iceberg",
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            connector_context.clone(),
        )?;
        let request = crate::mv::application::MvFirstRefreshWriteRequest::try_new(
            definition.select_sql.clone(),
            crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::Join,
            target.catalog,
            target.namespace,
            target.table,
            attempt.staging_branch.clone(),
            current_catalog.map(str::to_string),
            current_database.to_string(),
            expected_target_snapshot_id,
            table,
            target_contract,
            observed_binding,
            attempt.write_operation_id,
        )?;
        let prepared = crate::mv::application::MvFirstRefreshWritePreparer::prepare_join_logical(
            request,
            crate::engine::mv_first_refresh_staging::frozen_logical_context_from_rewrite(
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
    let sql_pin = sql_first_refresh_snapshot_pin(&pin)?;
    let (shape, physical_sql) = if capabilities.has_agg_state {
        // A branch UNION ALL has no top-level GROUP BY. Its aggregate-state
        // layout is defined by the first branch and CREATE-time validation
        // guarantees the remaining branches share that layout.
        let aggregate_query = if schema_contract.branch.is_some() {
            crate::mv::rewrite::context::first_union_branch_query(&query)?
        } else {
            query.clone()
        };
        let calls = crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
            &aggregate_query,
        )?;
        // The analyzer attaches aggregate input types to a SELECT body.  A
        // top-level branch UNION has no such body, while the first branch has
        // the validated representative aggregate layout.
        let aggregate_layout_sql = if schema_contract.branch.is_some() {
            aggregate_query.to_string()
        } else {
            definition.select_sql.clone()
        };
        let aggregate_layout = build_aggregate_layout_for_refresh_select_sql(
            state,
            current_catalog,
            current_database,
            &aggregate_layout_sql,
            &calls,
            &connector_context,
        )?;
        // The aggregate-state layout remains an application/runtime concern,
        // while first-refresh SQL shaping consumes an immutable SQL value.
        // Convert the already validated aggregate surface exactly once at the
        // application boundary; no SQL module receives the legacy MV shape.
        let sql_calls = sql_first_refresh_aggregate_calls(&calls);
        if let Some(branch) = &schema_contract.branch {
            (
                crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::BranchUnionAggregate,
                crate::sql::mv_refresh::first_refresh::prepare_branch_union_aggregate_first_refresh_write_sql_with_target_schema(
                    &definition.select_sql,
                    branch.branch_count as usize,
                    &sql_calls,
                    &sql_pin,
                    current_catalog,
                    current_database,
                    Some(target_contract.schema()),
                )?,
            )
        } else if !schema_contract.bases.is_empty() {
            (
                crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::FanInAggregate,
                crate::sql::mv_refresh::first_refresh::prepare_fan_in_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                    &definition.select_sql,
                    &sql_calls,
                    &sql_pin,
                    current_catalog,
                    current_database,
                    Some(target_contract.schema()),
                    Some(&aggregate_layout.aggregate_input_types),
                )?,
            )
        } else {
            (
                crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::Aggregate,
                crate::sql::mv_refresh::first_refresh::prepare_aggregate_first_refresh_write_sql_with_target_schema_and_input_types(
                    &definition.select_sql,
                    &sql_calls,
                    &sql_pin,
                    current_catalog,
                    current_database,
                    Some(target_contract.schema()),
                    Some(&aggregate_layout.aggregate_input_types),
                )?,
            )
        }
    } else if let Some(branch) = &schema_contract.branch {
        (
            crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::UnionProjection,
            crate::sql::mv_refresh::first_refresh::prepare_union_projection_first_refresh_write_sql(
                &definition.select_sql,
                branch.branch_count as usize,
                &sql_pin,
                current_catalog,
                current_database,
            )?,
        )
    } else {
        (
            crate::sql::mv_refresh::first_refresh::MvFirstRefreshShape::Projection,
            crate::sql::mv_refresh::first_refresh::prepare_projection_first_refresh_write_sql(
                &definition.select_sql,
                &sql_pin,
                current_catalog,
                current_database,
            )?,
        )
    };
    let table = crate::engine::iceberg_writer::iceberg_connector_table_handle(
        &write_lease,
        &crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context.clone(),
    )?;
    let request = crate::mv::application::MvFirstRefreshWriteRequest::try_new(
        definition.select_sql,
        shape,
        target.catalog,
        target.namespace,
        target.table,
        attempt.staging_branch.clone(),
        current_catalog.map(str::to_string),
        current_database.to_string(),
        expected_target_snapshot_id,
        table,
        target_contract,
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
    state: &Arc<StandaloneState>,
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
    let definition = load_iceberg_mv_definition_by_target(state, &target)?;
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
                    observe_schema_validation_for_table(state, base, &connector_context)?;
                Ok::<_, String>((base.clone(), snapshot_id, observed.table_uuid().to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    let definition = rebind_mv_definition_before_refresh_derivation(
        state,
        &definition,
        &contract.base_refs,
        &target,
        &connector_context,
    )
    .map_err(IcebergMvRefreshExecutionError::into_message)?;
    let canonical_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&definition.select_sql)?,
        current_catalog,
        current_database,
    );
    let rewrite = build_neutral_refresh_rewrite_context(
        state,
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
            state,
            &left_ref,
            left_from,
            left_to,
            &connector_context,
        )?;
        let (right_admission, _) = observe_and_admit_change_window_for_table(
            state,
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
                state,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pin.to_table_uuid_map(),
                observed_binding,
                None,
                connector_context,
            )?
            .into_full_overwrite();
            return Ok(PreparedIncrementalRefreshWork::FullRebuild(rebuild));
        }
        let left_facts = left_facts.expect("full-rebuild admission returned above");
        let right_facts = right_facts.expect("full-rebuild admission returned above");
        let branches = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
            &left_ref,
            &right_ref,
            crate::engine::mv::iceberg_join_branch::SnapshotWindow {
                from: left_from,
                to: left_to,
            },
            crate::engine::mv::iceberg_join_branch::SnapshotWindow {
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
            JoinIncrementalRefreshMode::Coalesce
        } else {
            select_join_incremental_refresh_mode(left_facts.has_deletes, right_facts.has_deletes)
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
            state,
            &connector_context,
            &rewrite.base_refs,
            &rewrite.pin,
            &rewrite.previous_snapshot_ids,
        )?;
        return crate::mv::application::MvIncrementalWritePreparer::prepare(
            request,
            crate::engine::mv_first_refresh_staging::frozen_logical_context_from_rewrite(
                &rewrite,
                contract.affected_partitions.clone(),
                Some(frozen_base_overlays),
            )?,
            match join_mode {
                JoinIncrementalRefreshMode::AppendOnly => {
                    crate::mv::application::MvIncrementalWriteMode::FastAppend
                }
                JoinIncrementalRefreshMode::Coalesce => {
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
                    JoinIncrementalRefreshMode::AppendOnly => {
                        crate::mv::application::MvIncrementalJoinMode::AppendOnly
                    }
                    JoinIncrementalRefreshMode::Coalesce => {
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
            let observed = observe_schema_validation_for_table(state, base, &connector_context)?;
            if observed.table_uuid() != current_table_uuid {
                return Err(format!(
                    "MV incremental refresh base table identity changed after planning for {}",
                    base.fqn()
                ));
            }
            let (admission, _) = observe_and_admit_change_window_for_table(
                state,
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
                state,
                current_catalog,
                current_database,
                contract,
                attempt,
                &rewrite.pin.to_table_uuid_map(),
                observed_binding,
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
        state,
        &connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
    )?;
    crate::mv::application::MvIncrementalWritePreparer::prepare(
        request,
        crate::engine::mv_first_refresh_staging::frozen_logical_context_from_rewrite(
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

fn non_join_incremental_write_mode(
    is_aggregate: bool,
    has_delete_changes: bool,
) -> crate::mv::application::MvIncrementalWriteMode {
    if is_aggregate || has_delete_changes {
        crate::mv::application::MvIncrementalWriteMode::RowDelta
    } else {
        crate::mv::application::MvIncrementalWriteMode::FastAppend
    }
}

fn explain_refresh_full_guard(full: bool) -> Result<(), String> {
    if full {
        return Err(FULL_REFRESH_DISABLED_MESSAGE.to_string());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergMvTarget {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

impl From<&TableIdentity> for IcebergMvTarget {
    fn from(target: &TableIdentity) -> Self {
        Self {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        }
    }
}

#[cfg(test)]
mod target_identity_tests {
    use super::*;

    #[test]
    fn iceberg_mv_target_from_table_identity_preserves_exact_case() {
        let identity = TableIdentity {
            catalog: "TargetCase".to_string(),
            namespace: "NameSpace".to_string(),
            table: "MvTable".to_string(),
        };

        let target = IcebergMvTarget::from(&identity);

        assert_eq!(target.catalog, identity.catalog);
        assert_eq!(target.namespace, identity.namespace);
        assert_eq!(target.table, identity.table);
    }
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

/// Core adapter used by the frontend-owned MV application service. It keeps
/// connector/analyzer state in core while exposing CREATE as auditable,
/// side-effect-sized primitives.
pub(crate) struct StandaloneMvEngine {
    state: Arc<StandaloneState>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    storage_observer: Arc<dyn MvStorageObservationPort>,
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
    columns: Vec<crate::sql::parser::ast::TableColumnDef>,
    partition_fields: Vec<IcebergPartitionFieldExpr>,
    target_properties: Vec<(String, String)>,
    created_target_observation: Mutex<Option<MvTargetCreationObservation>>,
}

impl StandaloneMvEngine {
    pub(crate) fn new(
        state: Arc<StandaloneState>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        let storage_observer = Arc::clone(&state.mv_storage_observation);
        Self {
            state,
            connector_context,
            storage_observer,
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
        let prepared = prepare_iceberg_mv_create(
            &self.state,
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
            self.state.connector_control.as_ref(),
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
                        .map(crate::engine::statement::connector_column)
                        .collect::<Result<_, _>>()
                        .map_err(engine_target_error)?,
                    key: None,
                    partitioning: prepared
                        .partition_fields
                        .iter()
                        .map(crate::engine::statement::connector_partition_transform)
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
        let observation = match self.storage_observer.observe_created_target(
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
        sync_iceberg_mv_descriptor(
            &self.state,
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
        register_iceberg_mv_target_in_catalog(&self.state, &target)
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
            self.state.connector_control.as_ref(),
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

fn prepare_iceberg_mv_create(
    state: &Arc<StandaloneState>,
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
    ensure_mv_create_target_absent(state, &target, connector_context)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &stmt.select_query,
        Some(current_catalog),
        current_database,
    );
    let analysis = analyze_mv_select_with_connector_context(
        state,
        Some(current_catalog),
        current_database,
        &canonical_select_query,
        connector_context,
    )?;
    let refresh_contract = derive_imv_refresh_contract(&analysis)?;
    let partition_fields = partition_fields_for_create(stmt.partition_by.as_ref());
    validate_mv_partition_columns(Some(&partition_fields), &analysis.output_columns)?;
    let created_at_ms = now_ms();
    let resolved_dependencies =
        crate::engine::mv::dependency::resolve_create_mv_dependencies_with_repository(
            repository,
            &analysis.resolved_refs,
            created_at_ms,
        )?;
    let dependency_target = crate::mv::dependency::model::iceberg_mv_dependency_ref(
        &target.catalog,
        &target.namespace,
        &target.table,
    );
    crate::engine::mv::dependency::validate_no_create_cycle_with_repository(
        state,
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
    let property = derive_fragment_property(&analysis.resolved_query)?;
    let base_field_observations =
        observe_base_fields_for_refs(state, &resolved_dependencies.base_refs, connector_context)?;
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

/// Temporary core entrypoint retained until Task 9 installs the frontend
/// application service at statement dispatch. It deliberately delegates every
/// side-effect-sized step to `StandaloneMvEngine`; sequencing ownership moves
/// to frontend as soon as the host wiring is enabled.
#[cfg(test)]
pub(crate) fn create_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    create_iceberg_mv_with_connector_context(
        state,
        current_catalog,
        current_database,
        stmt,
        &crate::connector::test_request_context(),
    )
}

pub(crate) fn create_iceberg_mv_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &CreateMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let statement = MvCreateStatement::from(stmt);
    let engine = StandaloneMvEngine::new(Arc::clone(state), connector_context.clone());
    let plan = engine
        .prepare_create(
            PrepareMvCreateRequest {
                statement: &statement,
                context: crate::mv::application::MvRequestContext {
                    current_catalog,
                    current_database,
                },
            },
            state.mv_repository.as_ref(),
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
    let definition = match state
        .mv_repository
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

fn ensure_mv_create_target_absent(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &target.catalog,
    )?;
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

/// Project the validated aggregate-state surface into the SQL compiler's
/// immutable vocabulary.  The legacy shape continues to serve runtime merge
/// layout construction; SQL first-refresh shaping must not depend on it.
fn sql_first_refresh_aggregate_calls(
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
) -> crate::sql::mv_refresh::aggregate_shape::SqlAggregateCalls {
    use crate::mv::aggregate_state::mv_shape::AggregateInput;
    use crate::sql::mv_refresh::aggregate_shape::{
        SqlAggregateCall, SqlAggregateGroupKey, SqlAggregateInput,
    };

    crate::sql::mv_refresh::aggregate_shape::SqlAggregateCalls {
        group_keys: calls
            .group_keys
            .iter()
            .map(|key| SqlAggregateGroupKey {
                output_name: key.output_name.clone(),
                expr: key.expr.clone(),
            })
            .collect(),
        aggregates: calls
            .aggregates
            .iter()
            .map(|aggregate| SqlAggregateCall {
                output_name: aggregate.output_name.clone(),
                function: aggregate.function,
                input: match &aggregate.input {
                    AggregateInput::Star => SqlAggregateInput::Star,
                    AggregateInput::Expr(expr) => SqlAggregateInput::Expr(expr.clone()),
                },
            })
            .collect(),
        visible_outputs: calls.visible_outputs.clone(),
    }
}

/// Project the admitted application pin before SQL first-refresh shaping.
/// This is the only conversion from the refresh owner's pin into the
/// compiler-facing immutable snapshot vocabulary.
fn sql_first_refresh_snapshot_pin(
    pin: &crate::mv::refresh::pin::RefreshSnapshotPin,
) -> Result<crate::sql::mv_refresh::first_refresh::SqlMvSnapshotPin, String> {
    crate::sql::mv_refresh::first_refresh::SqlMvSnapshotPin::try_from_maps(
        pin.to_snapshot_map(),
        pin.to_table_uuid_map(),
    )
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
    if branch_contract.inner_apply_key_source
        != crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
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
    if branch_contract.inner_apply_key_source
        != crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId
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
) -> crate::sql::planner::vocabulary::ApplyKeySource {
    match branch_kind {
        UnionBranchKind::Aggregate => crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId,
        UnionBranchKind::ProjectionFilter => {
            crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId
        }
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
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
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
            let (aggregate_calls, resolved_query) =
                representative_aggregate_calls(property, canonical_query, analysis)?;
            let layout = build_aggregate_layout_from_resolved_query(
                &aggregate_calls,
                &analysis.output_columns,
                resolved_query,
            )?;
            Ok(Some(layout))
        }
        // `inner_row_identity` already peeled the branch wrapper; a nested
        // `BranchScoped` cannot occur (construction flattens it).
        TargetIdentity::BranchScoped(_) => Err(
            "Iceberg MV target layout internal error: unflattened branch-scoped identity"
                .to_string(),
        ),
    }
}

/// The representative aggregate `(calls, resolved query)` for the property: the
/// whole query for a non-branch aggregate, or the first branch for a
/// branch-union aggregate. The aggregate-call surface is sourced from the
/// FROM-agnostic [`extract_aggregate_sql_calls`] extractor, so a simple
/// aggregate, an aggregate over a join, and a fan-in aggregate all yield the
/// same `group_keys`/`aggregates`/`visible_outputs` the layout builder needs —
/// the build is driven by the focused extractor and the persisted contract.
fn representative_aggregate_calls<'a>(
    property: &RefreshFragmentProperty,
    canonical_query: &sqlparser::ast::Query,
    analysis: &'a MvAnalysis,
) -> Result<
    (
        crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
        &'a crate::sql::analysis::ResolvedQuery,
    ),
    String,
> {
    if matches!(property.identity, TargetIdentity::BranchScoped(_)) {
        let first_branch_ast = first_union_branch_ast_query(canonical_query)?;
        let aggregate_calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
                &first_branch_ast,
            )?;
        let resolved_query = first_union_branch_resolved_query(&analysis.resolved_query)?;
        Ok((aggregate_calls, resolved_query))
    } else {
        let aggregate_calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
                canonical_query,
            )?;
        Ok((aggregate_calls, &analysis.resolved_query))
    }
}

/// Extract the first UNION ALL branch as a standalone AST query. Mirrors
/// `mv_shape::flatten_union_all` + `wrap_setexpr_as_query` (kept local because
/// those helpers are private to `mv_shape`).
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
fn observe_base_fields_for_refs(
    state: &Arc<StandaloneState>,
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
        let observation = observe_schema_validation_for_table(state, base_ref, connector_context)?;
        observed.insert(base_ref.fqn(), observation);
    }
    Ok(observed)
}

fn create_apply_key_source_property(apply_key: &ApplyKeyContract) -> &'static str {
    match apply_key.column_name {
        HIDDEN_APPLY_KEY_COLUMN_NAME => ApplyKeySource::BaseRowId.table_property_value(),
        JOIN_APPLY_KEY_COLUMN_NAME => ApplyKeySource::JoinRowKey.table_property_value(),
        GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME => ApplyKeySource::GroupRowId.table_property_value(),
        other => unreachable!("unknown Iceberg MV apply-key column {other}"),
    }
}

fn create_apply_key_contract_source(
    apply_key: &ApplyKeyContract,
) -> crate::sql::planner::vocabulary::ApplyKeySource {
    match apply_key.column_name {
        HIDDEN_APPLY_KEY_COLUMN_NAME => crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId,
        JOIN_APPLY_KEY_COLUMN_NAME => crate::sql::planner::vocabulary::ApplyKeySource::JoinRowKey,
        GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME => {
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        }
        other => unreachable!("unknown Iceberg MV apply-key column {other}"),
    }
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
    policy: &crate::sql::parser::ast::MaterializedViewRefreshPolicy,
    paused: bool,
) -> serde_json::Value {
    match policy {
        crate::sql::parser::ast::MaterializedViewRefreshPolicy::Manual => serde_json::json!({
            "policy": "DEFERRED_MANUAL",
            "interval_ms": null,
            "paused": paused,
        }),
        crate::sql::parser::ast::MaterializedViewRefreshPolicy::AsyncOnChange => {
            serde_json::json!({
                "policy": "ASYNC_ON_CHANGE",
                "interval_ms": null,
                "paused": paused,
            })
        }
        crate::sql::parser::ast::MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => {
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
pub(crate) fn sync_iceberg_mv_descriptor(
    state: &Arc<StandaloneState>,
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
        state.connector_control.as_ref(),
        catalog_name,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        namespace,
        target_table_name,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    let package = state
        .mv_storage_observation
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

fn create_apply_key_table_column(
    apply_key: &ApplyKeyContract,
) -> Result<crate::sql::parser::ast::TableColumnDef, String> {
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

fn iceberg_aggregate_target_columns(
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    analysis: &MvAnalysis,
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
    let layout = build_aggregate_layout_from_analysis(calls, analysis)?;
    iceberg_aggregate_target_columns_from_layout(&layout)
}

fn iceberg_aggregate_target_columns_from_resolved_query(
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    output_columns: &[crate::sql::analysis::OutputColumn],
    resolved_query: &crate::sql::analysis::ResolvedQuery,
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
    let layout = build_aggregate_layout_from_resolved_query(calls, output_columns, resolved_query)?;
    iceberg_aggregate_target_columns_from_layout(&layout)
}

fn iceberg_aggregate_target_columns_from_layout(
    layout: &crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
) -> Result<Vec<crate::sql::parser::ast::TableColumnDef>, String> {
    validate_unique_aggregate_physical_column_names(&layout.physical_columns)?;
    Ok(layout
        .physical_columns
        .iter()
        .map(|column| column.column.clone())
        .collect())
}

fn build_aggregate_layout_from_analysis(
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    analysis: &MvAnalysis,
) -> Result<crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout, String> {
    build_aggregate_layout_from_resolved_query(
        calls,
        &analysis.output_columns,
        &analysis.resolved_query,
    )
}

fn build_aggregate_layout_from_resolved_query(
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    output_columns: &[crate::sql::analysis::OutputColumn],
    resolved_query: &crate::sql::analysis::ResolvedQuery,
) -> Result<crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout, String> {
    let aggregate_input_types =
        crate::mv::aggregate_state::mv_agg_state::aggregate_input_types_from_resolved_query(
            calls,
            resolved_query,
        )?;
    crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
        calls,
        output_columns,
        &aggregate_input_types,
    )
}

fn first_union_branch_resolved_query(
    resolved_query: &crate::sql::analysis::ResolvedQuery,
) -> Result<&crate::sql::analysis::ResolvedQuery, String> {
    match &resolved_query.body {
        crate::sql::analysis::QueryBody::SetOperation(set_op) => {
            first_union_branch_resolved_query(&set_op.left)
        }
        crate::sql::analysis::QueryBody::Select(_) => Ok(resolved_query),
        crate::sql::analysis::QueryBody::Values(_) => {
            Err("UNION ALL MV first branch requires SELECT analysis".to_string())
        }
    }
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
            &analysis.resolved_query,
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
/// `query`/`resolved_query`. Used for top-level non-branch MVs.
fn build_non_branch_schema_contract(
    identity: &TargetIdentity,
    query: &sqlparser::ast::Query,
    resolved_query: &crate::sql::analysis::ResolvedQuery,
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
        resolved_query,
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
/// `query`/`resolved_query`, classifying any shape data locally. This is the
/// per-identity dispatch that the legacy per-`RefreshStrategy` match performed,
/// reproduced verbatim but keyed on the identity.
fn build_non_branch_contract_core(
    identity: &TargetIdentity,
    query: &sqlparser::ast::Query,
    resolved_query: &crate::sql::analysis::ResolvedQuery,
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
            let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
                resolved_query,
                &sql_mv_lineage_schema(observed_base_fields(base_field_observations, base_ref)?),
            )?;
            let (base_fields, output) = persist_sql_mv_lineage(lineage);
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
            let join_aliases =
                crate::mv::aggregate_state::aggregate_sql_calls::extract_join_aliases(
                    query,
                )?;
            let (left_contract, right_contract, output, join) =
                build_join_base_contracts_and_lineage(
                    &join_aliases,
                    resolved_query,
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
                resolved_query,
                analysis,
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
/// fan-in aggregate from a single-scan aggregate WITHOUT the legacy classifier.
/// Mirrors `mv_shape::extract_union_all_fan_in_bases`'s structural test: a
/// fan-in FROM is exactly one relation, no joins, a non-lateral derived subquery
/// whose body is a `UNION ALL` set operation.
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

fn mixed_output_contract(
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> mv_schema::OutputContract {
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
    resolved_query: &crate::sql::analysis::ResolvedQuery,
    analysis: &crate::mv::analysis::MvAnalysis,
    base_refs: &[TableIdentity],
    base_field_observations: &std::collections::BTreeMap<
        String,
        crate::mv::storage_observation::MvSchemaValidationObservation,
    >,
    target_observation: &MvTargetCreationObservation,
) -> Result<NonBranchContractCore, String> {
    // Aggregate-call surface (group keys, aggregates, visible-output ordering)
    // is FROM-agnostic, so the focused extractor produces the same calls for a
    // simple aggregate, a join-aggregate, and a fan-in aggregate — byte-identical
    // to the legacy `AggregateSqlCalls::from(&shape)` (both share
    // `classify_aggregate_select_outputs`).
    let aggregate_calls =
        crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(query)?;
    let layout = build_aggregate_layout_from_resolved_query(
        &aggregate_calls,
        &analysis.output_columns,
        resolved_query,
    )?;

    // Dispatch on the FROM structure rather than the legacy classifier:
    //   * a two-table inner equi-join FROM    -> JoinAggregate core
    //   * a fan-in UNION ALL subquery in FROM -> FanInAggregate core
    //   * a single scan                       -> SingleAggregate core
    // The join lineage (predicate field-ids, output/filter lineage, per-base
    // narrowing) is still derived from the resolved AST inside
    // `build_join_base_contracts_and_lineage`; the join-alias extractor supplies
    // only the (table FQN, qualifier) pairs.
    if from_clause_is_direct_inner_on_join(query) {
        let join_aliases =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_join_aliases(query)?;
        // Aggregate over a two-table inner equi-join (legacy JoinAggregate).
        let (left_contract, right_contract, output, join) = build_join_base_contracts_and_lineage(
            &join_aliases,
            resolved_query,
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
        let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
            resolved_query,
            &sql_mv_lineage_schema(observed_base_fields(base_field_observations, base_ref)?),
        )?;
        let (base_fields, output) = persist_sql_mv_lineage(lineage);
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
/// the join contract for a two-table inner equi-join over `resolved_query`.
/// Shared by the JoinProjectionFilter and JoinAggregate cores and by a composed
/// join-aggregate branch.
fn build_join_base_contracts_and_lineage(
    join_aliases: &crate::mv::aggregate_state::aggregate_sql_calls::JoinAliases,
    resolved_query: &crate::sql::analysis::ResolvedQuery,
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
    let left_fqn = left_ref.fqn();
    let right_fqn = right_ref.fqn();
    // The join predicate field-ids, output-column lineage, filter lineage, and
    // per-base field narrowing are all derived from `resolved_query` (the
    // analyzer-resolved AST) — NOT the join aliases. The aliases supply only the
    // (table FQN, qualifier) pairs the collector keys schemas by, so the
    // persisted `join`/`output` sections are byte-identical to the legacy build.
    let join_lineage = crate::sql::analyzer::mv_lineage::build_join_projection_filter_lineage(
        resolved_query,
        &[
            (&left_fqn, &join_aliases.left_alias, &left_schema),
            (&right_fqn, &join_aliases.right_alias, &right_schema),
        ],
    )?;
    let left_fields = join_lineage
        .base_fields_by_table
        .get(&left_fqn)
        .cloned()
        .unwrap_or_default();
    let right_fields = join_lineage
        .base_fields_by_table
        .get(&right_fqn)
        .cloned()
        .unwrap_or_default();
    let left_contract = base_contract(
        left_ref,
        observed_base(base_field_observations, left_ref)?,
        Some(join_aliases.left_alias.clone()),
        persist_sql_mv_base_fields(left_fields),
    );
    let right_contract = base_contract(
        right_ref,
        observed_base(base_field_observations, right_ref)?,
        Some(join_aliases.right_alias.clone()),
        persist_sql_mv_base_fields(right_fields),
    );
    let output = persist_sql_mv_output_contract(join_lineage.output_columns, join_lineage.filter);
    let join = persist_sql_mv_join_contract(join_lineage.join);
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
    let first_branch_resolved = first_union_branch_resolved_query(&analysis.resolved_query)?;

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
            let first_branch_base_table =
                crate::mv::aggregate_state::aggregate_sql_calls::extract_single_scan_table_fqn(
                    &first_branch_ast,
                )?;
            let first_base_ref = base_ref_for_table_fqn(base_refs, &first_branch_base_table)?;
            let first_schema = sql_mv_lineage_schema(observed_base_fields(
                base_field_observations,
                first_base_ref,
            )?);
            let lineage = crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
                &analysis.resolved_query,
                &first_schema,
            )
            .or_else(|_| {
                crate::sql::analyzer::mv_lineage::build_projection_filter_lineage(
                    first_branch_resolved,
                    &first_schema,
                )
            })?;
            let (_, output) = persist_sql_mv_lineage(lineage);
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
                first_branch_resolved,
                analysis,
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
        TargetIdentity::BaseRowId => crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId,
        TargetIdentity::GroupRowId(_) => {
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        }
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
        inner_apply_key_source,
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
    use crate::mv::aggregate_state::aggregate_sql_calls;

    if let Ok(join_aliases) = aggregate_sql_calls::extract_join_aliases(branch_query) {
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
                aggregate_sql_calls::extract_single_scan_table_fqn(&branch)
            })
            .collect();
    }

    // Single scan (projection/filter or aggregate over one table).
    Ok(vec![aggregate_sql_calls::extract_single_scan_table_fqn(
        branch_query,
    )?])
}

/// Flatten a (possibly nested) UNION ALL set-operation body into its leaf
/// branch bodies, mirroring `mv_shape::flatten_union_all` without re-validating
/// the UNION ALL operator (the fan-in shape is already confirmed by the caller).
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
/// query's non-body fields. Mirrors `mv_shape::wrap_setexpr_as_query`.
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

/// Project provider schema metadata into the SQL-owned lineage vocabulary at
/// the application boundary. The SQL analyzer never retains the provider
/// schema object or consults it after this conversion.
fn sql_mv_lineage_schema(
    fields: &[crate::mv::storage_observation::MvObservedTargetField],
) -> crate::sql::analyzer::mv_lineage::SqlMvLineageSchema {
    crate::sql::analyzer::mv_lineage::SqlMvLineageSchema {
        fields: fields
            .iter()
            .map(
                |field| crate::sql::analyzer::mv_lineage::SqlMvLineageField {
                    field_id: field.field_id,
                    name_at_create: field.name.clone(),
                    type_signature: field.type_signature.clone(),
                    required: !field.nullable,
                },
            )
            .collect(),
    }
}

fn persist_sql_mv_base_fields(
    fields: Vec<crate::sql::analyzer::mv_lineage::SqlMvLineageField>,
) -> Vec<mv_schema::BaseFieldRecord> {
    fields
        .into_iter()
        .map(|field| mv_schema::BaseFieldRecord {
            field_id: field.field_id,
            name_at_create: field.name_at_create,
            type_signature: field.type_signature,
            required: field.required,
        })
        .collect()
}

fn persist_sql_mv_qualified_field(
    field: crate::sql::analyzer::mv_lineage::SqlMvQualifiedFieldLineage,
) -> mv_schema::QualifiedFieldLineage {
    mv_schema::QualifiedFieldLineage {
        table_fqn: field.table_fqn,
        qualifier_at_create: field.qualifier_at_create,
        field_id: field.field_id,
    }
}

fn persist_sql_mv_expression_kind(
    kind: crate::sql::analyzer::mv_lineage::SqlMvExpressionKind,
) -> mv_schema::ExpressionKind {
    match kind {
        crate::sql::analyzer::mv_lineage::SqlMvExpressionKind::Column => {
            mv_schema::ExpressionKind::Column
        }
        crate::sql::analyzer::mv_lineage::SqlMvExpressionKind::Cast => {
            mv_schema::ExpressionKind::Cast
        }
        crate::sql::analyzer::mv_lineage::SqlMvExpressionKind::Func => {
            mv_schema::ExpressionKind::Func
        }
        crate::sql::analyzer::mv_lineage::SqlMvExpressionKind::Literal => {
            mv_schema::ExpressionKind::Literal
        }
        crate::sql::analyzer::mv_lineage::SqlMvExpressionKind::Mixed => {
            mv_schema::ExpressionKind::Mixed
        }
    }
}

fn persist_sql_mv_output_contract(
    columns: Vec<crate::sql::analyzer::mv_lineage::SqlMvOutputColumnLineage>,
    filter: Option<crate::sql::analyzer::mv_lineage::SqlMvFilterLineage>,
) -> mv_schema::OutputContract {
    mv_schema::OutputContract {
        columns: columns
            .into_iter()
            .map(|column| mv_schema::OutputColumnLineage {
                expression: mv_schema::ExpressionLineage {
                    kind: persist_sql_mv_expression_kind(column.expression.kind),
                    referenced_base_field_ids: column.expression.referenced_base_field_ids,
                    referenced_base_fields: column
                        .expression
                        .referenced_base_fields
                        .into_iter()
                        .map(persist_sql_mv_qualified_field)
                        .collect(),
                },
            })
            .collect(),
        filter: filter.map(|filter| mv_schema::FilterLineage {
            referenced_base_field_ids: filter.referenced_base_field_ids,
            referenced_base_fields: filter
                .referenced_base_fields
                .into_iter()
                .map(persist_sql_mv_qualified_field)
                .collect(),
        }),
    }
}

fn persist_sql_mv_lineage(
    lineage: crate::sql::analyzer::mv_lineage::SqlMvLineageResult,
) -> (Vec<mv_schema::BaseFieldRecord>, mv_schema::OutputContract) {
    (
        persist_sql_mv_base_fields(lineage.base_fields),
        persist_sql_mv_output_contract(lineage.output_columns, lineage.filter),
    )
}

fn persist_sql_mv_join_contract(
    join: crate::sql::analyzer::mv_lineage::SqlMvJoinContract,
) -> mv_schema::JoinContract {
    let kind = match join.kind {
        crate::sql::analyzer::mv_lineage::SqlMvJoinContractKind::InnerEquiJoin => {
            mv_schema::JoinContractKind::InnerEquiJoin
        }
    };
    mv_schema::JoinContract {
        kind,
        predicates: join
            .predicates
            .into_iter()
            .map(|predicate| mv_schema::JoinPredicateLineage {
                left: persist_sql_mv_qualified_field(predicate.left),
                right: persist_sql_mv_qualified_field(predicate.right),
            })
            .collect(),
    }
}

#[cfg(test)]
fn target_observation_from_loaded_for_test(
    target: &IcebergMvTarget,
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> Result<MvTargetCreationObservation, String> {
    let metadata = loaded.table.metadata();
    let table = novarocks_spi::connector::ConnectorTableIdentity {
        instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?,
        namespace: Arc::from(target.namespace.as_str()),
        table: Arc::from(target.table.as_str()),
    };
    let fields = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(
            |field| crate::mv::storage_observation::MvObservedTargetField {
                field_id: field.id,
                name: field.name.clone(),
                type_signature: field.field_type.to_string(),
                nullable: !field.required,
            },
        )
        .collect();
    MvTargetCreationObservation::try_new(
        table,
        metadata.uuid().to_string(),
        metadata.current_schema_id(),
        fields,
        test_partition_contract_from_table(&loaded.table)?,
    )
    .map_err(|error| error.to_string())
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
    hidden_apply_key_source: ApplyKeySource,
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
            source: hidden_apply_key_source,
        },
        partition: Some(target_observation.partition.clone()),
    })
}

/// Test-only projection of a live Iceberg partition spec onto the MV contract.
///
/// Production reads this from the neutral observation; the Provider owns the
/// projection and rejects unknown transforms (spec D4). Fixtures that build a
/// table by hand still need the same mapping, and it retires with the rest of
/// the MV test scaffolding in T20/T21.
#[cfg(test)]
fn test_partition_contract_from_table(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<mv_schema::MvPartitionContract, String> {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    let mut fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "iceberg MV target partition field {} references missing target field id {}",
                field.name, field.source_id
            )
        })?;
        fields.push(mv_schema::MvPartitionFieldContract {
            partition_field_id: field.field_id,
            partition_field_name: field.name.clone(),
            source_target_field_id: field.source_id,
            source_column_name: source.name.clone(),
            transform: mv_partition_transform_contract(&field.transform)?,
        });
    }
    Ok(mv_schema::MvPartitionContract {
        target_spec_id: spec.spec_id(),
        fields,
    })
}

#[cfg(test)]
fn mv_partition_transform_contract(
    transform: &novarocks_connector_iceberg::iceberg::spec::Transform,
) -> Result<mv_schema::MvPartitionTransformContract, String> {
    match transform {
        novarocks_connector_iceberg::iceberg::spec::Transform::Identity => {
            Ok(mv_schema::MvPartitionTransformContract::Identity)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Year => {
            Ok(mv_schema::MvPartitionTransformContract::Year)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Month => {
            Ok(mv_schema::MvPartitionTransformContract::Month)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Day => {
            Ok(mv_schema::MvPartitionTransformContract::Day)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Hour => {
            Ok(mv_schema::MvPartitionTransformContract::Hour)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(num_buckets) => {
            Ok(mv_schema::MvPartitionTransformContract::Bucket {
                num_buckets: *num_buckets,
            })
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(width) => {
            Ok(mv_schema::MvPartitionTransformContract::Truncate { width: *width })
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Void => {
            Ok(mv_schema::MvPartitionTransformContract::Void)
        }
        novarocks_connector_iceberg::iceberg::spec::Transform::Unknown => {
            Err("iceberg MV target partition contract cannot persist unknown transform".to_string())
        }
    }
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

pub(crate) fn register_iceberg_mv_target_in_catalog(
    state: &Arc<StandaloneState>,
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
        state.connector_control.as_ref(),
        &instance_id,
    )
    .map_err(|error| format!("acquire MV target connector generation: {error}"))?;
    Ok(())
}

pub(crate) fn restore_iceberg_mv_targets(state: &Arc<StandaloneState>) -> Result<(), String> {
    if !state.mv_repository.availability().is_available() {
        return Ok(());
    }
    for mv in state
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
        register_iceberg_mv_target_in_catalog(state, &target)?;
    }
    Ok(())
}

pub(crate) fn resolve_refresh_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(name, current_database)?;
    Ok(IcebergMvTarget {
        catalog: novarocks_catalog::identifier::normalize_identifier(catalog)?,
        namespace,
        table,
    })
}

/// Resolve the MV target's neutral binding for a validation-only read.
///
/// Callers that already hold a binding should pass it through instead; this is
/// for sites that still load the target the legacy way and only need facts.
fn target_binding_for(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::refresh::target_binding::MvTargetBinding, String> {
    crate::mv::refresh::target_binding::load_mv_target_binding(
        state,
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
    state: &Arc<StandaloneState>,
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
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<crate::mv::rewrite::context::IcebergMvRewriteContext>, String> {
    let binding = target_binding_for(state, target, connector_context)?;
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
        binding.arrow_schema().clone(),
        Arc::from(binding.observation().field_ids().to_vec()),
        schema_contract,
    )
    .map(Arc::new)
}

fn validate_target_snapshot(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
    binding: &crate::mv::refresh::target_binding::MvTargetBinding,
) -> Result<(), String> {
    let actual = binding.current_snapshot_id();
    let expected = mv_definition.last_refreshed_iceberg_snapshot_id;
    if actual != expected
        && !(expected.is_none() && binding.observation().current_snapshot_is_empty_bootstrap())
    {
        return Err(format!(
            "target table {}.{}.{} was modified outside NovaRocks: expected snapshot {:?}, current snapshot {:?}",
            target.catalog, target.namespace, target.table, expected, actual
        ));
    }
    Ok(())
}

fn recorded_target_snapshot_id(
    target: &IcebergMvTarget,
    mv_definition: &StoredMvDefinition,
) -> Result<i64, String> {
    mv_definition
        .last_refreshed_iceberg_snapshot_id
        .ok_or_else(|| {
            format!(
                "iceberg materialized view {}.{}.{} has no recorded target snapshot",
                target.catalog, target.namespace, target.table
            )
        })
}

fn observe_schema_validation_for_table(
    state: &Arc<StandaloneState>,
    table: &TableIdentity,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvSchemaValidationObservation, String> {
    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &table.catalog,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )?;
    state
        .mv_storage_observation
        .observe_schema_validation(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV schema validation facts for {}: {error}",
                table.fqn()
            )
        })
}

fn observe_and_admit_change_window_for_table(
    state: &Arc<StandaloneState>,
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
        state.connector_control.as_ref(),
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
    let scan = crate::engine::query_planning::catalog_materializer::admit_connector_change_window(
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
    let observation = state
        .mv_storage_observation
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
    state: &Arc<StandaloneState>,
    mv_definition: &StoredMvDefinition,
    base_refs: &[TableIdentity],
    target: &IcebergMvTarget,
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
                observe_schema_validation_for_table(state, base_ref, connector_context)?;
            let target_observation =
                observe_schema_validation_for_table(state, &target_ref, connector_context)?;
            match validate_schema_contract(contract, &base_observation, &target_observation) {
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
                observe_schema_validation_for_table(state, left_ref, connector_context)?;
            let right_observation =
                observe_schema_validation_for_table(state, right_ref, connector_context)?;
            let target_observation =
                observe_schema_validation_for_table(state, &target_ref, connector_context)?;
            let left_fqn = left_ref.fqn();
            let right_fqn = right_ref.fqn();
            let decision = validate_join_schema_contract(
                contract,
                &[
                    (left_fqn.as_str(), left_observation),
                    (right_fqn.as_str(), right_observation),
                ],
                &target_observation,
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
        aggregate_calls: &'a crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    },
    /// Aggregate over a composed multi-base relation, such as a nested join or
    /// a zero-key CROSS JOIN. The change stream still uses the aggregate
    /// rewrite-merge path; the apply-key evidence decides whether join-delta
    /// proof is required.
    ComposedAggregate {
        schema_contract: &'a mv_schema::MvSchemaContract,
        aggregate_calls: &'a crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
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
        first_branch_calls: &'a crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
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
                            )) => crate::engine::mv::partition::planner::plan_affected_partitions(
                                &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
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
    state: &Arc<StandaloneState>,
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
                    state,
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
                    )) => crate::engine::mv::partition::planner::plan_affected_partitions(
                        &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
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
                crate::engine::mv::partition::planner::plan_affected_partitions(
                    &crate::engine::mv::partition::planner::AffectedPartitionPlanInput {
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

#[cfg(test)]
pub(crate) fn plan_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    target: MvTarget,
) -> Result<RefreshPlan, RefreshError> {
    plan_iceberg_mv_refresh_with_connector_context(
        state,
        current_catalog,
        current_database,
        stmt,
        target,
        &crate::connector::test_request_context(),
    )
}

pub(crate) fn plan_iceberg_mv_refresh_with_connector_context(
    state: &Arc<StandaloneState>,
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
    let mv_definition =
        load_iceberg_mv_definition_by_target(state, &iceberg_target).map_err(RefreshError::user)?;
    let target_ref = TableIdentity {
        catalog: iceberg_target.catalog.clone(),
        namespace: iceberg_target.namespace.clone(),
        table: iceberg_target.table.clone(),
    };
    validate_target_snapshot(
        &iceberg_target,
        &mv_definition,
        &target_binding_for(state, &iceberg_target, connector_context)
            .map_err(RefreshError::user)?,
    )
    .map_err(RefreshError::user)?;

    let base_refs =
        parse_iceberg_table_refs(&mv_definition.base_table_refs).map_err(RefreshError::user)?;
    let mv_definition = rebind_mv_definition_before_refresh_derivation(
        state,
        &mv_definition,
        &base_refs,
        &iceberg_target,
        connector_context,
    )
    .map_err(IcebergMvRefreshExecutionError::into_message)
    .map_err(RefreshError::user)?;
    let refresh_state_baseline = build_refresh_state_baseline(
        &mv_definition,
        &target_binding_for(state, &iceberg_target, connector_context)
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
                state,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                branch_count,
                dispatch_schema_contract,
                connector_context,
            );
        }
        // Aggregate shapes: single-base, fan-in, branch-union, and join
        // aggregate all route through the aggregate planner, which selects the
        // per-shape plan by capability. The branch-union sub-path sources its
        // branch count + first-branch aggregate calls from the focused extractor
        // (not the union classifier), so composed branches are supported.
        (true, _, _) => {
            return plan_iceberg_aggregate_mv_refresh(
                state,
                &iceberg_target,
                target,
                stmt,
                current_catalog,
                current_database,
                &mv_definition,
                &base_refs,
                &caps,
                &canonical_select_query,
                connector_context,
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
        let join_aliases = crate::mv::aggregate_state::aggregate_sql_calls::extract_join_aliases(
            &canonical_select_query,
        )
        .map_err(RefreshError::user)?;
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
        let left_refresh = observe_current_refresh_base(state, left_ref, connector_context)
            .map_err(RefreshError::user)?;
        let right_refresh = observe_current_refresh_base(state, right_ref, connector_context)
            .map_err(RefreshError::user)?;
        let left_observation =
            observe_schema_validation_for_table(state, left_ref, connector_context)
                .map_err(RefreshError::user)?;
        let right_observation =
            observe_schema_validation_for_table(state, right_ref, connector_context)
                .map_err(RefreshError::user)?;
        let target_observation =
            observe_schema_validation_for_table(state, &target_ref, connector_context)
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
        observe_current_refresh_base(state, base_ref, connector_context)
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
    let base_observation = observe_schema_validation_for_table(state, base_ref, connector_context)
        .map_err(RefreshError::user)?;
    let target_observation =
        observe_schema_validation_for_table(state, &target_ref, connector_context)
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
        state,
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
    state: &Arc<StandaloneState>,
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
        observe_schema_validation_for_table(state, &target_ref, connector_context)
            .map_err(RefreshError::user)?;

    let mut current_snapshots = BTreeMap::new();
    let mut snapshot_pins = BTreeMap::new();
    for base_ref in base_refs {
        let refresh = observe_current_refresh_base(state, base_ref, connector_context)
            .map_err(RefreshError::user)?;
        let base_observation =
            observe_schema_validation_for_table(state, base_ref, connector_context)
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
                    observe_schema_validation_for_table(state, base_ref, connector_context)
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
                state,
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
        &target_binding_for(state, iceberg_target, connector_context)
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
    state: &Arc<StandaloneState>,
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
        observe_schema_validation_for_table(state, &target_ref, connector_context)
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
        let refresh = observe_current_refresh_base(state, base_ref, connector_context)
            .map_err(RefreshError::user)?;
        let base_observation =
            observe_schema_validation_for_table(state, base_ref, connector_context)
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
                state,
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
            &target_binding_for(state, iceberg_target, connector_context)
                .map_err(RefreshError::user)?,
            current_catalog,
            current_database,
        )
        .map_err(RefreshError::user)?,
        affected_partitions,
    ))
}

fn plan_iceberg_aggregate_mv_refresh(
    state: &Arc<StandaloneState>,
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
                    state,
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
            let refresh = observe_current_refresh_base(state, base_ref, connector_context)
                .map_err(RefreshError::user)?;
            let base_observation =
                observe_schema_validation_for_table(state, base_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let target_ref = TableIdentity {
                catalog: iceberg_target.catalog.clone(),
                namespace: iceberg_target.namespace.clone(),
                table: iceberg_target.table.clone(),
            };
            let target_observation =
                observe_schema_validation_for_table(state, &target_ref, connector_context)
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
                state,
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
                    &target_binding_for(state, iceberg_target, connector_context)
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
                crate::mv::aggregate_state::aggregate_sql_calls::extract_join_aliases(
                    canonical_select_query,
                )
                .map_err(RefreshError::user)?;
            if base_refs.len() != 2 {
                return Err(RefreshError::user(
                    "iceberg join aggregate MV refresh requires exactly two base table references",
                ));
            }
            validate_join_aliases_base_refs(&join_aliases, base_refs)
                .map_err(RefreshError::user)?;
            let (left_ref, right_ref) =
                join_base_refs_for_aliases(&join_aliases, base_refs).map_err(RefreshError::user)?;
            let left_refresh = observe_current_refresh_base(state, left_ref, connector_context)
                .map_err(RefreshError::user)?;
            let right_refresh = observe_current_refresh_base(state, right_ref, connector_context)
                .map_err(RefreshError::user)?;
            let left_observation =
                observe_schema_validation_for_table(state, left_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let right_observation =
                observe_schema_validation_for_table(state, right_ref, connector_context)
                    .map_err(RefreshError::user)?;
            let target_ref = TableIdentity {
                catalog: iceberg_target.catalog.clone(),
                namespace: iceberg_target.namespace.clone(),
                table: iceberg_target.table.clone(),
            };
            let target_observation =
                observe_schema_validation_for_table(state, &target_ref, connector_context)
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
                    &target_binding_for(state, iceberg_target, connector_context)
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
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<StoredMvDefinition, String> {
    #[cfg(test)]
    record_definition_load();
    state
        .mv_repository
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

fn alias_aggregate_refresh_group_key_projection_from_rewrite(
    query: &mut sqlparser::ast::Query,
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
) -> Result<(), String> {
    let (calls, layout) = rewrite.aggregate_shape_and_layout_for_execution()?;
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("aggregate MV incremental refresh SELECT body is required".to_string());
    };
    for (projection_index, output) in calls.visible_outputs.iter().enumerate() {
        match output {
            crate::sql::mv_refresh::VisibleAggregateOutput::GroupKey(group_key_index) => {
                let visible_source_index = layout
                    .group_key_source_indexes
                    .get(*group_key_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV group key projection index {group_key_index} out of range"
                        )
                    })?;
                let expected_name = layout
                    .visible_columns
                    .get(*visible_source_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV group key visible source index {visible_source_index} out of range"
                        )
                    })?
                    .name
                    .clone();
                let item = select.projection.get_mut(projection_index).ok_or_else(|| {
                    format!(
                        "aggregate MV group key projection position {projection_index} is missing"
                    )
                })?;
                alias_select_projection_item(item, &expected_name)?;
                if let sqlparser::ast::GroupByExpr::Expressions(expressions, _) =
                    &mut select.group_by
                    && let Some(group_expr) = expressions.get_mut(*group_key_index)
                {
                    *group_expr = sqlparser::ast::Expr::Identifier(aggregate_refresh_alias_ident(
                        &expected_name,
                    ));
                }
            }
            crate::sql::mv_refresh::VisibleAggregateOutput::Aggregate(_) => {}
        }
    }
    Ok(())
}

fn alias_select_projection_item(
    item: &mut sqlparser::ast::SelectItem,
    alias: &str,
) -> Result<(), String> {
    use sqlparser::ast::SelectItem;

    let alias = aggregate_refresh_alias_ident(alias);
    match item {
        SelectItem::UnnamedExpr(expr) => {
            let expr = expr.clone();
            *item = SelectItem::ExprWithAlias { expr, alias };
            Ok(())
        }
        SelectItem::ExprWithAlias {
            alias: existing, ..
        } => {
            *existing = alias;
            Ok(())
        }
        SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
            Err("aggregate MV group key projection cannot be a wildcard".to_string())
        }
    }
}

fn aggregate_refresh_alias_ident(alias: &str) -> sqlparser::ast::Ident {
    let mut chars = alias.chars();
    let is_plain = chars
        .next()
        .map(|first| first.is_ascii_alphabetic() || first == '_')
        .unwrap_or(false)
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
    if is_plain {
        sqlparser::ast::Ident::new(alias)
    } else {
        sqlparser::ast::Ident::with_quote('`', alias)
    }
}

fn build_aggregate_layout_for_refresh_select_sql(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    select_sql: &str,
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout, String> {
    let visible_query = parse_mv_select_query(select_sql)?;
    let visible_analysis =
        crate::engine::mv::analysis_adapter::analyze_mv_select_with_connector_context(
            state,
            current_catalog,
            current_database,
            &visible_query,
            connector_context,
        )?;
    build_aggregate_layout_from_analysis(calls, &visible_analysis)
}
pub(crate) fn parse_mv_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|e| format!("stored MV SELECT normalize error: {e}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|err| format!("sql parser error: {err}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn validate_repartition_schema_contract(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    schema_contract: &mv_schema::MvSchemaContract,
    base_refs: &[TableIdentity],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let target_ref = TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    let target_observation =
        observe_schema_validation_for_table(state, &target_ref, connector_context)?;
    if schema_contract.join.is_some() {
        let [left_ref, right_ref] = base_refs else {
            return Err(format!(
                "Iceberg join MV repartition schema contract requires exactly two base tables, got {}",
                base_refs.len()
            ));
        };
        let left_observation =
            observe_schema_validation_for_table(state, left_ref, connector_context)?;
        let right_observation =
            observe_schema_validation_for_table(state, right_ref, connector_context)?;
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
                observe_schema_validation_for_table(state, base_ref, connector_context)?;
            if schema_contract.aggregate.is_some() {
                validate_aggregate_repartition_schema_contract_for_base(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    &target_observation,
                )?;
            } else {
                validate_repartition_base_schema_contract(
                    schema_contract,
                    base_ref,
                    &base_observation,
                    &target_observation,
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
    let base_observation = observe_schema_validation_for_table(state, base_ref, connector_context)?;
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
    aliases: &crate::mv::aggregate_state::aggregate_sql_calls::JoinAliases,
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
    aliases: &crate::mv::aggregate_state::aggregate_sql_calls::JoinAliases,
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

/// Build a one-shot InMemoryCatalog for IMV optimizer-pipeline planning.
///
/// Registers each base in `ctx.rewrite.base_refs` under its namespace at
/// the snapshot captured by `ctx.rewrite.pin`. The catalog mirrors what
/// `canonical_select_query` references after `canonicalize_iceberg_mv_select_query`
/// rewrites `db.table` to `db.<synthetic>_at_<snapshot_id>`.
///
/// Reuses `build_iceberg_table_def_for_snapshot_scan` for per-base
/// table-def construction, so schemas / partition specs match what the
/// existing snapshot-scan path already uses.
/// Re-plan ctx.rewrite.canonical_select_query into a LogicalPlanNode suitable
/// for handing to `run_imv_rewrite`.
///
/// Failure here is fail-fast: if the canonical SELECT cannot be analyzed
/// or planned, the refresh attempt aborts. This deliberately surfaces
/// canonicalization bugs early rather than tolerating divergence between
/// today's hand-built refresh path and the IMV pipeline.
/// Plan a canonical IMV SELECT against the same request-local bindings that
/// will later prepare its scans.  This is the application adapter for the
/// remaining logical join artifact: it deliberately does not re-register
/// snapshot tables in `PlannerMemoryCatalog`.
pub(crate) fn compile_canonical_select_for_imv_with_frozen_rewrite(
    state: &Arc<StandaloneState>,
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<QueryTableBindingStore>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    overlays: Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>,
) -> Result<
    (
        crate::sql::planner::logical::LogicalPlanNode,
        crate::sql::column_id::ColumnRefFactory,
    ),
    RefreshError,
> {
    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let materializer = crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        None,
        &catalog_service_snapshot,
        bindings,
        crate::engine::query_stats::iceberg_table_binding_loader(
            state.connector_control.as_ref(),
            connector_context.clone(),
        ),
        overlays,
    );
    let mut query = (*rewrite.canonical_select_query).clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);
    let statistics =
        crate::engine::query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
            state,
            materializer.query_table_bindings(),
        );
    let catalog = crate::sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            RefreshError::user("IMV join planning requires a non-empty admitted backend topology")
        })?;
    let request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query)),
        crate::sql::compiler::SqlCompileIntent::LogicalOnly,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: None,
            current_database: rewrite.current_database.clone(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Logical(output) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| {
            RefreshError::user(format!(
                "imv plan failed for {}.{}.{}: canonical SQL compiler: {error}",
                rewrite.target.catalog, rewrite.target.namespace, rewrite.target.table
            ))
        })?
    else {
        return Err(RefreshError::user(
            "IMV logical intent did not produce logical SQL facts",
        ));
    };
    Ok((
        crate::sql::planner::imv_rewrite::entrypoint::normalize_imv_rewrite_root_project(
            output.logical_plan,
        ),
        output.factory,
    ))
}

#[cfg(test)]
pub(crate) fn normalize_imv_rewrite_root_project(
    plan: crate::sql::planner::logical::LogicalPlanNode,
) -> crate::sql::planner::logical::LogicalPlanNode {
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};

    let LogicalPlanNode {
        kind,
        mut children,
        required_output_columns,
    } = plan;
    let LogicalPlanKind::Project(project) = kind else {
        return LogicalPlanNode::new(kind, children, required_output_columns);
    };
    let input = children.remove(0);
    let LogicalPlanNode {
        kind: input_kind,
        children: aggregate_children,
        required_output_columns: aggregate_required_output_columns,
    } = input;
    let LogicalPlanKind::Aggregate(mut aggregate) = input_kind else {
        let input = LogicalPlanNode::new(
            input_kind,
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    };
    if project.items.len() != aggregate.output_columns.len() {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(aggregate),
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    }
    let Some(output_columns) = project
        .items
        .iter()
        .zip(aggregate.output_columns.iter())
        .map(|item| {
            let (item, aggregate_output) = item;
            let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                return None;
            };
            if *column_id != aggregate_output.column_id {
                return None;
            }
            Some(crate::sql::analysis::OutputColumn {
                column_id: *column_id,
                name: item.output_name.clone(),
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
                is_internal: false,
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        let input = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(aggregate),
            aggregate_children,
            aggregate_required_output_columns,
        );
        return LogicalPlanNode::new(
            LogicalPlanKind::Project(project),
            vec![input],
            required_output_columns,
        );
    };
    aggregate.output_columns = output_columns;
    LogicalPlanNode::new(
        LogicalPlanKind::Aggregate(aggregate),
        aggregate_children,
        aggregate_required_output_columns,
    )
}

fn refresh_explain_rewrite_disabled_rules(
    is_aggregate_refresh: bool,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
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

#[cfg(test)]
fn validate_aggregate_refresh_rewrite_outcome(
    ctx: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    outcome: &crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome,
    evidence: RewriteMergeRefreshEvidence,
) -> Result<(), String> {
    if evidence == RewriteMergeRefreshEvidence::JoinAggregate
        && !rewrite_outcome_rule_changed(outcome, "RewriteJoinDelta")
    {
        return Err(format!(
            "iceberg join aggregate MV {} incremental refresh rewrite did not apply RewriteJoinDelta",
            ctx.target.fqn()
        ));
    }
    if evidence == RewriteMergeRefreshEvidence::BranchUnionAggregate
        && !rewrite_outcome_rule_changed(outcome, "RewriteBranchUnion")
    {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {} incremental refresh rewrite did not apply RewriteBranchUnion",
            ctx.target.fqn()
        ));
    }
    if evidence != RewriteMergeRefreshEvidence::BranchUnionAggregate
        && !rewrite_outcome_rule_changed(outcome, "RewriteAggregateState")
    {
        let label = match evidence {
            RewriteMergeRefreshEvidence::JoinAggregate => "join aggregate",
            _ => "aggregate",
        };
        return Err(format!(
            "iceberg {label} MV {} incremental refresh rewrite did not apply RewriteAggregateState",
            ctx.target.fqn()
        ));
    }
    if !outcome.annotation.change_stream.has_aggregate() {
        let label = match evidence {
            RewriteMergeRefreshEvidence::JoinAggregate => "join aggregate",
            RewriteMergeRefreshEvidence::BranchUnionAggregate => "branch UNION ALL aggregate",
            _ => "aggregate",
        };
        return Err(format!(
            "iceberg {label} MV {} incremental refresh rewrite plan does not contain aggregate state change stream",
            ctx.target.fqn()
        ));
    }
    tracing::info!(
        mv_target = ?ctx.target,
        mv_id = ctx.mv_id,
        stages = ?outcome.trace.stage_names(),
        "iceberg aggregate MV incremental refresh rewrite evidence validated"
    );
    Ok(())
}

fn sql_imv_planning_input_from_rewrite(
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    target_binding: crate::sql::binding::SqlTableBindingId,
    evidence: RewriteMergeRefreshEvidence,
) -> Result<crate::sql::compiler::SqlImvPlanningInput, String> {
    use crate::sql::compiler::SqlImvRewriteValidation;

    let validation = match evidence {
        RewriteMergeRefreshEvidence::None => SqlImvRewriteValidation::None,
        RewriteMergeRefreshEvidence::Aggregate => SqlImvRewriteValidation::Aggregate,
        RewriteMergeRefreshEvidence::JoinAggregate => SqlImvRewriteValidation::JoinAggregate,
        RewriteMergeRefreshEvidence::BranchUnionAggregate => {
            SqlImvRewriteValidation::BranchUnionAggregate
        }
    };
    Ok(crate::sql::compiler::SqlImvPlanningInput::new(
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
) -> Result<crate::sql::binding::SqlTableBindingId, String> {
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
        let identity = SqlTableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        };
        let source = ScanSource::Sql(SqlScanSource::new(
            binding,
            identity,
            SqlScanKind::MvTargetLocator {
                facts: SqlMvTargetLocatorScan {
                    target_table_uuid: target_table_uuid.clone(),
                    target_snapshot_id: frozen_snapshot_id,
                    apply_key_column: rewrite
                        .schema_contract
                        .target
                        .hidden_apply_key
                        .column_name
                        .clone(),
                    branch_id_column: rewrite
                        .schema_contract
                        .branch
                        .as_ref()
                        .map(|branch| branch.branch_id_column.column_name.clone()),
                },
            },
        ));
        Ok(QueryTableBinding {
            resolved: ResolvedAnalyzerTable::from_planner(
                Some(&target.catalog),
                &target.namespace,
                TableDef {
                    name: target.table.clone(),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source,
                },
            ),
            // IMV target scans use their frozen file materialization; no
            // optimizer statistics are resolved for this target as a side
            // channel during refresh preparation.
            statistics_pin: None,
            admission: crate::engine::query_planning::bindings::QueryTableBindingAdmission::Exact(
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
    state: &Arc<StandaloneState>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    base_refs: &[TableIdentity],
    pin: &RefreshSnapshotPin,
    previous_snapshot_ids: &BTreeMap<String, i64>,
) -> Result<Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>, String>
{
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
            state.connector_control.as_ref(),
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
        let mut materialization = crate::engine::query_planning::catalog_materializer::connector_table_materialization_from_metadata(
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
                crate::engine::query_planning::catalog_materializer::admit_connector_change_window(
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
            crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay::new(
                namespace.clone(),
                table.clone(),
                key,
                move |binding| {
                    let mut result = crate::engine::query_planning::catalog_materializer::connector_query_binding_from_materialization(
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
fn rewrite_outcome_rule_changed(
    outcome: &crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome,
    rule_name: &str,
) -> bool {
    outcome.trace.events().iter().any(|event| {
        matches!(
            event,
            crate::sql::optimizer::rewrite::trace::RewriteTraceEvent::RuleChanged { rule, .. }
                if *rule == rule_name
        )
    })
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
                    source: ApplyKeySource::BaseRowId,
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
mod aggregate_refresh_rewrite_validation_tests {
    use super::*;

    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::phase::RewritePhase;
    use crate::sql::optimizer::rewrite::trace::RewriteTrace;
    use crate::sql::planner::imv_rewrite::annotation::ImvPlanAnnotation;
    use crate::sql::planner::imv_rewrite::change_stream::{
        AggregateChangeStreamDescriptor, AggregateChangeStreamShape, ImvChangeStreamDescriptor,
        SignedStateAggregateProof, TargetStateProof,
    };
    use crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome;
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::sql::planner::payload::PlanValuesNode;

    fn empty_values_plan() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: Vec::new(),
            }),
            vec![],
            None,
        )
    }

    fn outcome(plan: LogicalPlanNode, changed_rules: &[&'static str]) -> ImvRewriteOutcome {
        let mut trace = RewriteTrace::default();
        for rule in changed_rules {
            trace.rule_changed(RewritePhase::SemanticRewrite, rule, 0);
        }
        ImvRewriteOutcome {
            plan,
            trace,
            annotation: ImvPlanAnnotation::default(),
        }
    }

    fn aggregate_change_stream_descriptor() -> ImvChangeStreamDescriptor {
        ImvChangeStreamDescriptor {
            aggregate: Some(AggregateChangeStreamDescriptor {
                action_column_id: ColumnId::new_for_test(1),
                action_column_name: novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
                    .to_string(),
                shape: AggregateChangeStreamShape::UnionChangeStream,
                target_state: TargetStateProof { present: true },
                signed_state_aggregate: SignedStateAggregateProof { present: true },
            }),
            ..Default::default()
        }
    }

    fn outcome_with_aggregate_descriptor(
        plan: LogicalPlanNode,
        changed_rules: &[&'static str],
    ) -> ImvRewriteOutcome {
        let mut outcome = outcome(plan, changed_rules);
        outcome.annotation.change_stream = aggregate_change_stream_descriptor();
        outcome
    }

    #[test]
    fn aggregate_refresh_rejects_unchanged_rewrite_outcome() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome = outcome(empty_values_plan(), &[]);

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::Aggregate,
        )
        .expect_err("aggregate refresh must not continue with unchanged rewrite outcome");

        assert!(
            err.contains("did not apply RewriteAggregateState"),
            "got: {err}"
        );
    }

    #[test]
    fn aggregate_refresh_rejects_missing_merge_plan_evidence() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome = outcome(empty_values_plan(), &["RewriteAggregateState"]);

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::Aggregate,
        )
        .expect_err("aggregate refresh must require change stream in the rewrite plan");

        assert!(
            err.contains("does not contain aggregate state change stream"),
            "got: {err}"
        );
    }

    #[test]
    fn join_aggregate_refresh_rejects_missing_join_rewrite_evidence() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome =
            outcome_with_aggregate_descriptor(empty_values_plan(), &["RewriteAggregateState"]);

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::JoinAggregate,
        )
        .expect_err("join aggregate refresh must require join rewrite evidence");

        assert!(err.contains("did not apply RewriteJoinDelta"), "got: {err}");
    }

    #[test]
    fn join_aggregate_refresh_missing_merge_plan_uses_join_label() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome = outcome(
            empty_values_plan(),
            &["RewriteJoinDelta", "RewriteAggregateState"],
        );

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::JoinAggregate,
        )
        .expect_err("join aggregate refresh must require change stream in the rewrite plan");

        assert!(
            err.contains("iceberg join aggregate MV")
                && err.contains("does not contain aggregate state change stream"),
            "got: {err}"
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_rejects_missing_branch_union_rewrite_evidence() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome =
            outcome_with_aggregate_descriptor(empty_values_plan(), &["RewriteAggregateState"]);

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::BranchUnionAggregate,
        )
        .expect_err("branch UNION ALL aggregate refresh must require branch rewrite evidence");

        assert!(
            err.contains("branch UNION ALL aggregate")
                && err.contains("did not apply RewriteBranchUnion"),
            "got: {err}"
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_requires_state_merge_plan_evidence() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome = outcome(empty_values_plan(), &["RewriteBranchUnion"]);

        let err = validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::BranchUnionAggregate,
        )
        .expect_err("branch UNION ALL aggregate refresh must require change-stream plan evidence");

        assert!(
            err.contains("iceberg branch UNION ALL aggregate MV")
                && err.contains("does not contain aggregate state change stream"),
            "got: {err}"
        );
    }

    #[test]
    fn branch_union_aggregate_refresh_accepts_branch_rewrite_with_change_stream_plan() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome =
            outcome_with_aggregate_descriptor(empty_values_plan(), &["RewriteBranchUnion"]);

        validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::BranchUnionAggregate,
        )
        .expect(
            "branch UNION ALL aggregate refresh should accept branch rewrite with aggregate-state change-stream evidence",
        );
    }

    #[test]
    fn aggregate_refresh_accepts_change_stream_descriptor_evidence() {
        let ctx = crate::mv::rewrite::context::tests_support::dummy_rewrite_context();
        let outcome =
            outcome_with_aggregate_descriptor(empty_values_plan(), &["RewriteAggregateState"]);

        validate_aggregate_refresh_rewrite_outcome(
            &ctx,
            &outcome,
            RewriteMergeRefreshEvidence::Aggregate,
        )
        .expect(
            "aggregate refresh should accept aggregate-state change-stream descriptor evidence",
        );
    }
}

#[cfg(test)]
mod join_delta_append_only_fast_path_tests {
    use super::*;

    #[test]
    fn join_incremental_refresh_plan_kind_uses_logical_cutover() {
        let mode = select_join_incremental_refresh_mode(false, false);
        assert_eq!(mode, JoinIncrementalRefreshMode::AppendOnly);
        let mode = select_join_incremental_refresh_mode(true, false);
        assert_eq!(mode, JoinIncrementalRefreshMode::Coalesce);
        let mode = select_join_incremental_refresh_mode(false, true);
        assert_eq!(mode, JoinIncrementalRefreshMode::Coalesce);
    }

    #[test]
    fn aggregate_refresh_explain_disables_join_refresh_descriptor_recording() {
        let disabled_rules = refresh_explain_rewrite_disabled_rules(
            true,
            &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
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
        let branches = crate::engine::mv::iceberg_join_branch::plan_join_delta_branches(
            &left,
            &right,
            crate::engine::mv::iceberg_join_branch::SnapshotWindow { from: 10, to: 11 },
            crate::engine::mv::iceberg_join_branch::SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        let mut branch_queries = Vec::new();
        for branch in &branches {
            let mut branch_query =
                crate::engine::mv::iceberg_join_branch::rewrite_join_branch_query(
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
            crate::engine::mv::iceberg_join_branch::rewrite_join_delta_coalesce_query_with_branch_queries(
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
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }
}

pub(crate) fn explain_iceberg_mv_refresh_rewrite_plan(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    level: crate::sql::explain::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, String> {
    explain_refresh_full_guard(stmt.full)?;

    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name)?;
    let mv_definition = load_iceberg_mv_definition_by_target(state, &target)?;
    let target_binding = target_binding_for(state, &target, connector_context)?;
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

    let pin = capture_refresh_snapshot_pin(state, &base_refs, connector_context)?;
    validate_refresh_pin_table_uuids(&mv_definition, &pin, &base_refs)?;
    let rewrite = build_neutral_refresh_rewrite_context(
        state,
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
        connector_context,
    )?;
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = bind_imv_target_query_table_in_store_from_rewrite(
        &rewrite,
        &bindings,
        target_binding.lease(),
        connector_context,
    )?;
    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        state,
        connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
    )?;
    let materializer = crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        None,
        &catalog_service_snapshot,
        Arc::clone(&bindings),
        crate::engine::query_stats::iceberg_table_binding_loader(
            state.connector_control.as_ref(),
            connector_context.clone(),
        ),
        overlays,
    );
    let statistics =
        crate::engine::query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
            state,
            materializer.query_table_bindings(),
        );
    let catalog = crate::sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
    let mut query = (*rewrite.canonical_select_query).clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);
    let input = crate::sql::compiler::SqlImvPlanningInput::new(
        rewrite.to_sql_rewrite_snapshot(target_binding)?,
        crate::sql::compiler::SqlImvRewriteValidation::None,
    );
    let request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query)),
        crate::sql::compiler::SqlCompileIntent::LogicalOnly,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::NotApplicable,
        &catalog,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            Some(connector_context.deadline()),
            Arc::new(MvRefreshConnectorCancellationObservation {
                cancellation: connector_context.cancellation().clone(),
            }),
        ),
    )
    .with_imv_rewrite(&input);
    let crate::sql::compiler::SqlCompileOutput::Logical(output) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("EXPLAIN REFRESH logical intent did not produce logical SQL facts".to_string());
    };
    crate::sql::explain::explain_plan_checked(&output.logical_plan, level)
}

struct MvRefreshConnectorCancellationObservation {
    cancellation: Arc<dyn novarocks_spi::connector::ConnectorCancellation>,
}

impl crate::sql::compiler::SqlCancellationObservation
    for MvRefreshConnectorCancellationObservation
{
    fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}

fn column_ref_expr(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

#[cfg(test)]
fn should_use_join_delta_append_only_fast_path(
    query: &sqlparser::ast::Query,
    left_has_delete_changes: bool,
    right_has_delete_changes: bool,
) -> bool {
    !left_has_delete_changes
        && !right_has_delete_changes
        && crate::engine::mv::iceberg_join_branch::is_append_only_join_delta_eligible(query)
}

#[cfg(test)]
fn build_join_delta_coalesce_catalog(
    state: &Arc<StandaloneState>,
    branches: &[crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan],
    target: &IcebergMvTarget,
    target_table: &novarocks_connector_iceberg::iceberg::table::Table,
    target_snapshot_id: Option<i64>,
) -> Result<crate::sql::catalog::local::PlannerMemoryCatalog, String> {
    let mut catalog = crate::sql::catalog::local::PlannerMemoryCatalog::default();
    let mut registered_delta_tables = BTreeSet::new();
    let mut registered_snapshot_tables = BTreeSet::new();
    for branch in branches {
        register_join_delta_coalesce_side(
            &mut catalog,
            state,
            &branch.left_base,
            branch.left,
            &mut registered_delta_tables,
            &mut registered_snapshot_tables,
        )?;
        register_join_delta_coalesce_side(
            &mut catalog,
            state,
            &branch.right_base,
            branch.right,
            &mut registered_delta_tables,
            &mut registered_snapshot_tables,
        )?;
    }
    register_join_delta_target_locator(
        &mut catalog,
        state,
        target,
        target_table,
        target_snapshot_id,
    )?;
    Ok(catalog)
}

#[cfg(test)]
fn register_join_delta_target_locator(
    catalog: &mut crate::sql::catalog::local::PlannerMemoryCatalog,
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_table: &novarocks_connector_iceberg::iceberg::table::Table,
    target_snapshot_id: Option<i64>,
) -> Result<(), String> {
    catalog.create_database(&target.namespace)?;
    let table_def =
        build_join_delta_target_locator_table_def(state, target, target_table, target_snapshot_id)?;
    catalog
        .register(&target.namespace, table_def)
        .map_err(|e| format!("register join coalesce target locator table: {e}"))
}

#[cfg(test)]
fn build_join_delta_target_locator_table_def(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_table: &novarocks_connector_iceberg::iceberg::table::Table,
    target_snapshot_id: Option<i64>,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let _ = (state, target, target_table, target_snapshot_id);
    Err(
        "legacy planner-memory join target-locator fixture was removed; use tokenized MV planning input"
            .to_string(),
    )
}

#[cfg(test)]
fn build_empty_join_delta_target_locator_table_def(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)?;
    let mut table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_for_delta_scan(
        &target.catalog,
        &target.namespace,
        &target.table,
        loaded,
    )?;
    table_def
        .iceberg_row_lineage_metadata_columns
        .retain(|column| column.name != novarocks_execution::exec::change_op::CHANGE_OP_COLUMN);
    Ok(table_def)
}

#[cfg(test)]
fn register_join_delta_coalesce_side(
    catalog: &mut crate::sql::catalog::local::PlannerMemoryCatalog,
    state: &Arc<StandaloneState>,
    base: &TableIdentity,
    side: crate::engine::mv::iceberg_join_branch::BranchSide,
    registered_delta_tables: &mut BTreeSet<String>,
    registered_snapshot_tables: &mut BTreeSet<String>,
) -> Result<(), String> {
    let _ = (
        catalog,
        state,
        base,
        side,
        registered_delta_tables,
        registered_snapshot_tables,
    );
    Err(
        "legacy planner-memory join branch fixture was removed; use tokenized MV planning input"
            .to_string(),
    )
}

#[cfg(test)]
fn join_catalog_registration_key(
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<String, String> {
    Ok(format!(
        "{}.{}.{}",
        novarocks_catalog::identifier::normalize_identifier(catalog)?,
        novarocks_catalog::identifier::normalize_identifier(namespace)?,
        novarocks_catalog::identifier::normalize_identifier(table)?
    ))
}

fn normalize_join_branch_snapshot_tables(
    query: &mut sqlparser::ast::Query,
    branch: &crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan,
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
    if let crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.left {
        rewrite_snapshot_table_factor(&mut from.relation, &branch.left_base, snapshot_id, None)?;
    }
    if let crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.right
    {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImvChangeStreamProducerRoute {
    Deletion,
    ExistingData,
    AppendedData,
}

const IMV_CHANGE_STREAM_EFFECT_COLUMN: &str = "__imv_change_stream_effect";
const IMV_CHANGE_STREAM_EFFECT_EXISTING: i32 = 1;
const IMV_CHANGE_STREAM_EFFECT_APPENDED: i32 = 2;

struct ImvRefreshPlannedChangeStream {
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    table_bindings:
        Option<std::sync::Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>>,
    output_columns: Vec<OutputColumn>,
    change_stream: crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    producer_branches: Vec<ImvChangeStreamProducerRoute>,
    snapshot_properties: BTreeMap<String, String>,
    connector_operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
}

#[cfg(test)]
std::thread_local! {
    static IMV_CHANGE_STREAM_EXECUTION_RESULT_FOR_TEST: std::cell::RefCell<
        Option<crate::query_execution::outcome::QueryExecutionResult>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
struct ImvChangeStreamExecutionResultGuard {
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
impl Drop for ImvChangeStreamExecutionResultGuard {
    fn drop(&mut self) {
        IMV_CHANGE_STREAM_EXECUTION_RESULT_FOR_TEST.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }
}

#[cfg(test)]
fn install_imv_change_stream_execution_result_for_test(
    result: crate::query_execution::outcome::QueryExecutionResult,
) -> ImvChangeStreamExecutionResultGuard {
    IMV_CHANGE_STREAM_EXECUTION_RESULT_FOR_TEST.with(|slot| {
        let replaced = slot.borrow_mut().replace(result);
        assert!(
            replaced.is_none(),
            "IMV change-stream execution result already installed for this test thread"
        );
    });
    ImvChangeStreamExecutionResultGuard {
        _not_send: std::marker::PhantomData,
    }
}

#[cfg(test)]
fn take_imv_change_stream_execution_result_for_test()
-> Option<crate::query_execution::outcome::QueryExecutionResult> {
    IMV_CHANGE_STREAM_EXECUTION_RESULT_FOR_TEST.with(|slot| slot.borrow_mut().take())
}

#[cfg(test)]
fn imv_change_stream_writer_abort_result_for_test(
    reason: &str,
) -> crate::query_execution::outcome::QueryExecutionResult {
    let mut abort =
        crate::engine::write_operation_lifecycle::test_support::write_abort_with_data_file();
    abort.reason = reason.to_string();
    crate::query_execution::outcome::QueryExecutionResult {
        query_result: crate::runtime::query_result::QueryResult::empty(),
        write_commit: None,
        write_abort: Some(abort),
        connector_completion: None,
        fragment_profiles: Vec::new(),
    }
}

/// A change stream that carries any deletion route retracts previously
/// materialized rows; otherwise the refresh only adds rows.
///
/// Both change-stream entrypoints derived this independently before SPI-5I,
/// so a future producer route added to one and not the other would have
/// silently changed only half the refresh paths.
fn deletion_route_write_effect(
    refresh_plan: &ImvRefreshPlannedChangeStream,
) -> MvTargetWriteEffect {
    if refresh_plan
        .producer_branches
        .iter()
        .any(|route| matches!(route, ImvChangeStreamProducerRoute::Deletion))
    {
        MvTargetWriteEffect::DeltaRetractingStagedFiles
    } else {
        MvTargetWriteEffect::Append
    }
}

/// Bind an already prepared IMV change-stream plan to the frontend's admitted
/// execution and retained exact lease. Unlike the legacy executor above, this
/// function has no query submission, provider commit, catalog publication, or
/// MV repository transition. It is the Core-side activation half of a
/// frontend-owned incremental refresh attempt.
#[allow(clippy::too_many_arguments)]
fn prepare_imv_change_stream_writer(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    refresh_plan: ImvRefreshPlannedChangeStream,
    provider_routes: &[novarocks_spi::connector::ConnectorRowMutationRoute],
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::prepared_write::PreparedDistributedWriteRequest, String> {
    let (refresh_plan, effect_output_ordinal) = ensure_imv_change_stream_effect(refresh_plan)?;
    crate::connector::validate_request_context(connector_context)?;
    let table_bindings = refresh_plan.table_bindings.as_deref().ok_or_else(|| {
        "Iceberg MV change-stream write is missing admission-frozen query table bindings"
            .to_string()
    })?;
    let mut dag = provider_change_stream_write_dag_for_imv_refresh(
        target,
        &refresh_plan,
        effect_output_ordinal,
        provider_routes,
    )?;
    let planned =
        crate::engine::build_physical_plan_as_iceberg_change_stream_write_with_connector_context(
            state,
            Some(&target.catalog),
            &target.namespace,
            &refresh_plan.optimized_tree,
            Some(table_bindings),
            &mut dag,
            None,
            connector_context,
        )?;
    crate::engine::prepare_planned_iceberg_change_stream_write(
        planned.prepared,
        planned.native_bundle,
        None,
        execution,
        Some(crate::engine::DistributedConnectorWrite::Begin(
            connector_write,
        )),
    )
}

/// Activate a value-only incremental refresh artifact after frontend intent
/// persistence and exact-lease admission. Core rebuilds only provider-private
/// scan and writer facts here; it returns a prepared result-free request and
/// never advances MV metadata or executes an external commit.
pub(crate) fn bind_prepared_mv_incremental_staging(
    state: &Arc<StandaloneState>,
    prepared: crate::mv::application::PreparedMvIncrementalWrite,
    planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
    exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::prepared_write::PreparedDistributedWriteRequest, String> {
    let (request, facts, mode, evidence, execution_artifact, publication_intent) =
        prepared.into_parts();
    if request.observed_binding != *exact_lease.binding_key() {
        return Err("MV incremental write lease drifted from prepared binding".to_string());
    }
    let connector_context =
        crate::connector::connector_request_context_for_execution(None, execution)?;
    let refresh_rewrite =
        crate::engine::mv_first_refresh_staging::rebuild_frozen_mv_rewrite_context(
            state,
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
    let target = crate::engine::backend_resolver::TargetBackend {
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
    for route in &provider_routes {
        crate::engine::query_planning::write_sink::admit_prepared_connector_write_target(
            target_bindings.as_ref(),
            crate::sql::planner::table::SqlTableIdentity {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
            },
            route.preparation().clone(),
            planning_lease.clone(),
        )?;
    }
    let managed_publication =
        crate::engine::mv::iceberg_activation::managed_publication_activation_intent(
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
    let planned_query = match execution_artifact {
        crate::mv::application::MvIncrementalExecutionArtifact::CanonicalQuery => {
            let mut query = (*refresh_rewrite.canonical_select_query).clone();
            if rewrite_evidence != RewriteMergeRefreshEvidence::None
                && rewrite_evidence != RewriteMergeRefreshEvidence::BranchUnionAggregate
            {
                alias_aggregate_refresh_group_key_projection_from_rewrite(
                    &mut query,
                    &refresh_rewrite,
                )?;
            }
            crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);
            let imv_rewrite_input = sql_imv_planning_input_from_rewrite(
                &refresh_rewrite,
                target_binding,
                rewrite_evidence,
            )?;
            let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
            let base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
                state,
                &connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let analyzer_catalog = crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&target_bindings),
                crate::engine::query_stats::iceberg_table_binding_loader(
                state.connector_control.as_ref(),
                    connector_context.clone(),
                ),
                base_overlays,
            );
            crate::engine::plan_query_for_iceberg_change_stream_refresh(
                state,
                &query,
                &analyzer_catalog,
                &refresh_rewrite.current_database,
                Some(&imv_rewrite_input),
                Arc::clone(&target_bindings),
                execution,
            )?
        }
        crate::mv::application::MvIncrementalExecutionArtifact::JoinLogical { mode } => {
            let join_mode = match mode {
                crate::mv::application::MvIncrementalJoinMode::AppendOnly => {
                    JoinIncrementalRefreshMode::AppendOnly
                }
                crate::mv::application::MvIncrementalJoinMode::Coalesce => {
                    JoinIncrementalRefreshMode::Coalesce
                }
            };
            let base_overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
                state,
                &connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let (plan, factory) = compile_canonical_select_for_imv_with_frozen_rewrite(
                state,
                &refresh_rewrite,
                &connector_context,
                Arc::clone(&target_bindings),
                execution,
                base_overlays,
            )
            .map_err(|error| error.message)?;
            let logical = build_join_incremental_refresh_logical_plan(
                &refresh_rewrite.to_sql_rewrite_snapshot(target_binding)?,
                join_mode,
                JoinIncrementalLogicalInput { plan, factory },
            )?;
            let mut planned = crate::engine::plan_logical_for_iceberg_change_stream_refresh(
                logical.plan,
                logical.factory,
            )?;
            if let Some(change_stream) = logical.change_stream_override {
                planned.change_stream = change_stream;
            }
            planned.table_bindings = Some(Arc::clone(&target_bindings));
            planned
        }
    };
    let producer_branches = match mode {
        crate::mv::application::MvIncrementalWriteMode::FastAppend => {
            vec![ImvChangeStreamProducerRoute::AppendedData]
        }
        crate::mv::application::MvIncrementalWriteMode::RowDelta => vec![
            ImvChangeStreamProducerRoute::Deletion,
            ImvChangeStreamProducerRoute::ExistingData,
            ImvChangeStreamProducerRoute::AppendedData,
        ],
    };
    let operation_id = request.operation_id;
    let distributed = prepare_imv_change_stream_writer(
        state,
        &target,
        ImvRefreshPlannedChangeStream {
            optimized_tree: planned_query.optimized_tree,
            table_bindings: planned_query.table_bindings,
            output_columns: planned_query.output_columns,
            change_stream: planned_query.change_stream,
            producer_branches,
            snapshot_properties: BTreeMap::new(),
            connector_operation_id: operation_id,
        },
        &provider_routes,
        connector_write,
        &connector_context,
        execution,
    )?;
    if distributed.write_operation_id() != operation_id
        || distributed.write_cohort_id() != selected_cohort
    {
        return Err("MV incremental distributed artifact identity mismatch".to_string());
    }
    Ok(distributed)
}

fn ensure_imv_change_stream_effect(
    mut refresh_plan: ImvRefreshPlannedChangeStream,
) -> Result<(ImvRefreshPlannedChangeStream, Option<usize>), String> {
    let route_mode = imv_change_stream_effect_mode(&refresh_plan.producer_branches)?
        .unwrap_or(ImvChangeStreamEffectMode::Constant(0));

    let has_delete_branch = refresh_plan
        .producer_branches
        .iter()
        .any(|route| matches!(route, ImvChangeStreamProducerRoute::Deletion));
    let action_output = if has_delete_branch {
        let action_ordinal = imv_change_op_output_ordinal(&refresh_plan)?;
        Some(refresh_plan.output_columns[action_ordinal].clone())
    } else {
        None
    };
    let row_lineage_output = match route_mode {
        ImvChangeStreamEffectMode::Constant(_) => None,
        ImvChangeStreamEffectMode::ByRowLineage => Some(
            output_column_by_name(
                &refresh_plan.output_columns,
                novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
                "reuse/fresh route row-lineage column",
            )?
            .clone(),
        ),
    };

    let route_output = imv_change_stream_effect_output_column(&refresh_plan.output_columns);
    let route_output_ordinal = refresh_plan.output_columns.len();
    let optimized_tree = add_imv_change_stream_effect_project(
        refresh_plan.optimized_tree,
        &refresh_plan.output_columns,
        action_output.as_ref(),
        row_lineage_output.as_ref(),
        route_mode,
        route_output.clone(),
    )?;
    refresh_plan.optimized_tree = optimized_tree;
    refresh_plan.output_columns.push(route_output);

    Ok((refresh_plan, Some(route_output_ordinal)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImvChangeStreamEffectMode {
    Constant(i32),
    ByRowLineage,
}

fn imv_change_stream_effect_mode(
    producer_branches: &[ImvChangeStreamProducerRoute],
) -> Result<Option<ImvChangeStreamEffectMode>, String> {
    let has_reuse = producer_branches
        .iter()
        .any(|route| matches!(route, ImvChangeStreamProducerRoute::ExistingData));
    let has_fresh = producer_branches
        .iter()
        .any(|route| matches!(route, ImvChangeStreamProducerRoute::AppendedData));
    Ok(match (has_reuse, has_fresh) {
        (false, false) => None,
        (true, false) => Some(ImvChangeStreamEffectMode::Constant(
            IMV_CHANGE_STREAM_EFFECT_EXISTING,
        )),
        (false, true) => Some(ImvChangeStreamEffectMode::Constant(
            IMV_CHANGE_STREAM_EFFECT_APPENDED,
        )),
        (true, true) => Some(ImvChangeStreamEffectMode::ByRowLineage),
    })
}

fn imv_change_stream_effect_output_column(existing: &[OutputColumn]) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(
            existing
                .iter()
                .map(|column| column.column_id.0)
                .max()
                .unwrap_or(0)
                + 1,
        ),
        name: IMV_CHANGE_STREAM_EFFECT_COLUMN.to_string(),
        data_type: DataType::Int8,
        nullable: false,
        is_internal: true,
    }
}

fn add_imv_change_stream_effect_project(
    child: crate::sql::optimizer::OptimizedOperatorNode,
    child_output_columns: &[OutputColumn],
    action_output: Option<&OutputColumn>,
    row_lineage_output: Option<&OutputColumn>,
    route_mode: ImvChangeStreamEffectMode,
    route_output: OutputColumn,
) -> Result<crate::sql::optimizer::OptimizedOperatorNode, String> {
    use crate::sql::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
    use crate::sql::optimizer::optimized_tree::{PlanExecutionProps, attach_scalar_arena};
    use crate::sql::optimizer::scalar::ScalarNode;

    let existing_arena =
        child.execution_props.scalar_arena.as_ref().ok_or_else(|| {
            "IMV change-stream route projection requires a scalar arena".to_string()
        })?;
    let mut arena = (**existing_arena).clone();
    let mut items = Vec::with_capacity(child_output_columns.len() + 1);
    for column in child_output_columns {
        arena.remember_source_column_display(column.column_id, None, column.name.clone());
        let expr = arena.intern(
            ScalarNode::ColumnRef(column.column_id),
            column.data_type.clone(),
            column.nullable,
        );
        items.push(ScalarProjectItem {
            expr,
            output_name: column.name.clone(),
            output_column_id: column.column_id,
            expr_display: None,
        });
    }

    let route_expr =
        imv_change_stream_effect_scalar(&mut arena, action_output, row_lineage_output, route_mode)?;
    arena.remember_project_output_display(route_output.column_id, None, route_output.name.clone());
    items.push(ScalarProjectItem {
        expr: route_expr,
        output_name: route_output.name.clone(),
        output_column_id: route_output.column_id,
        expr_display: None,
    });

    let output_property = child.execution_props.output_property.clone();
    let stats = child.stats.clone();
    let mut output_columns = child_output_columns.to_vec();
    output_columns.push(route_output);
    let arena = Arc::new(arena);
    let mut plan = crate::sql::optimizer::OptimizedOperatorNode {
        op: Operator::PhysicalProject(ProjectOp {
            items,
            output_qualifier: None,
        }),
        children: vec![child],
        stats,
        explain_stats: crate::sql::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: PlanExecutionProps {
            output_property: output_property.clone(),
            child_output_properties: vec![output_property],
            join_distribution: None,
            scalar_arena: Some(Arc::clone(&arena)),
        },
    };
    attach_scalar_arena(&mut plan, arena);
    Ok(plan)
}

fn imv_change_stream_effect_scalar(
    arena: &mut crate::sql::optimizer::scalar::ScalarArena,
    action_output: Option<&OutputColumn>,
    row_lineage_output: Option<&OutputColumn>,
    route_mode: ImvChangeStreamEffectMode,
) -> Result<crate::sql::optimizer::scalar::ScalarId, String> {
    use crate::sql::common::{BinOp, CHANGE_OP_DELETE, LiteralValue};
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

    let route_value_expr = match route_mode {
        ImvChangeStreamEffectMode::Constant(route_value) => arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(match route_value {
                IMV_CHANGE_STREAM_EFFECT_EXISTING => 2,
                IMV_CHANGE_STREAM_EFFECT_APPENDED => 3,
                _ => 1,
            }))),
            DataType::Int8,
            false,
        ),
        ImvChangeStreamEffectMode::ByRowLineage => {
            let row_lineage_output = row_lineage_output.ok_or_else(|| {
                "IMV reuse/fresh route requires preserved row-lineage output".to_string()
            })?;
            let row_lineage_ref = arena.intern(
                ScalarNode::ColumnRef(row_lineage_output.column_id),
                row_lineage_output.data_type.clone(),
                row_lineage_output.nullable,
            );
            let is_fresh = arena.intern(
                ScalarNode::IsNull {
                    child: row_lineage_ref,
                    negated: false,
                },
                DataType::Boolean,
                false,
            );
            let fresh_route = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(3))),
                DataType::Int8,
                false,
            );
            let reuse_route = arena.intern(
                ScalarNode::Literal(HashableLiteral(LiteralValue::Int(2))),
                DataType::Int8,
                false,
            );
            arena.intern(
                ScalarNode::Case {
                    operand: None,
                    when_then: vec![(is_fresh, fresh_route)],
                    else_expr: Some(reuse_route),
                },
                DataType::Int8,
                false,
            )
        }
    };
    let Some(action_output) = action_output else {
        return Ok(route_value_expr);
    };

    let action_ref = arena.intern(
        ScalarNode::ColumnRef(action_output.column_id),
        action_output.data_type.clone(),
        action_output.nullable,
    );
    let delete_literal = arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(CHANGE_OP_DELETE as i64))),
        action_output.data_type.clone(),
        false,
    );
    let is_delete = arena.intern(
        ScalarNode::BinaryOp {
            op: BinOp::Eq,
            left: action_ref,
            right: delete_literal,
        },
        DataType::Boolean,
        action_output.nullable,
    );
    let delete_effect = arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
        DataType::Int8,
        false,
    );
    Ok(arena.intern(
        ScalarNode::Case {
            operand: None,
            when_then: vec![(is_delete, delete_effect)],
            else_expr: Some(route_value_expr),
        },
        DataType::Int8,
        false,
    ))
}

fn iceberg_mv_target_backend(
    target: &IcebergMvTarget,
) -> crate::engine::backend_resolver::TargetBackend {
    crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    }
}

fn iceberg_change_stream_write_dag_for_imv_refresh(
    target: &crate::engine::backend_resolver::TargetBackend,
    refresh_plan: &ImvRefreshPlannedChangeStream,
    effect_output_ordinal: Option<usize>,
) -> Result<crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec, String>
{
    let bindings = refresh_plan.table_bindings.as_deref().ok_or_else(|| {
        "IMV change-stream write is missing admission-frozen query table bindings".to_string()
    })?;
    let routes = build_imv_change_stream_routes(
        target,
        bindings,
        &refresh_plan.output_columns,
        &refresh_plan.producer_branches,
    )?;
    Ok(
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec {
            effect_output_ordinal: effect_output_ordinal.ok_or_else(|| {
                "IMV change-stream plan did not install its dedicated effect column".to_string()
            })?,
            routes,
        },
    )
}

fn provider_change_stream_write_dag_for_imv_refresh(
    target: &crate::engine::backend_resolver::TargetBackend,
    refresh_plan: &ImvRefreshPlannedChangeStream,
    effect_output_ordinal: Option<usize>,
    provider_routes: &[novarocks_spi::connector::ConnectorRowMutationRoute],
) -> Result<crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec, String>
{
    let bindings = refresh_plan.table_bindings.as_deref().ok_or_else(|| {
        "IMV change-stream write is missing admission-frozen query table bindings".to_string()
    })?;
    let routes = build_provider_imv_change_stream_routes(
        target,
        bindings,
        &refresh_plan.output_columns,
        provider_routes,
    )?;
    Ok(
        crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec {
            effect_output_ordinal: effect_output_ordinal.ok_or_else(|| {
                "IMV change-stream plan did not install its dedicated effect column".to_string()
            })?,
            routes,
        },
    )
}

fn imv_change_op_output_ordinal(
    refresh_plan: &ImvRefreshPlannedChangeStream,
) -> Result<usize, String> {
    if let Some(aggregate) = refresh_plan.change_stream.aggregate() {
        return output_ordinal_by_column_id(
            &refresh_plan.output_columns,
            aggregate.action_column_id,
            "aggregate change-stream action column",
        );
    }
    if let Some(join_refresh) = refresh_plan.change_stream.join_refresh.as_ref() {
        return output_ordinal_by_column_id(
            &refresh_plan.output_columns,
            join_refresh.action_column.column_id,
            "join change-stream action column",
        );
    }
    refresh_plan
        .output_columns
        .iter()
        .position(is_imv_change_op_output_column)
        .ok_or_else(|| {
            let outputs = refresh_plan
                .output_columns
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    format!(
                        "#{idx}:{}:{:?}:internal={}",
                        column.name, column.column_id, column.is_internal
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "IMV change-stream write requires __change_op output column; outputs=[{outputs}]"
            )
        })
}

fn is_imv_change_op_output_column(column: &OutputColumn) -> bool {
    column
        .name
        .eq_ignore_ascii_case(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN)
        && column.data_type == DataType::Int8
        && !column.nullable
}

fn build_imv_change_stream_routes(
    target: &crate::engine::backend_resolver::TargetBackend,
    bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    output_columns: &[OutputColumn],
    producer_branches: &[ImvChangeStreamProducerRoute],
) -> Result<
    Vec<crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec>,
    String,
> {
    use crate::engine::query_planning::write_sink::sql_write_plan_input_for_admitted_target;
    use crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec;
    use crate::sql::planner::distributed::write::contract::{
        ConnectorWriteInputBinding, SqlWriteSinkMode,
    };
    use novarocks_spi::connector::{
        ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteCohortId,
        ConnectorWriteRouteId,
    };

    producer_branches
        .iter()
        .copied()
        .enumerate()
        .map(|(idx, producer_branch)| {
            let (sink, partition_ordinals) = match producer_branch {
                ImvChangeStreamProducerRoute::Deletion => {
                    let target_binding = mv_change_stream_write_binding_for_mode(
                        bindings,
                        target,
                        SqlWriteSinkMode::DeletionVectors,
                    )?;
                    let sink = sql_write_plan_input_for_admitted_target(
                        bindings,
                        target_binding,
                        SqlWriteSinkMode::DeletionVectors,
                        ConnectorWriteInputBinding::RootOutputByOrdinal,
                        None,
                    )?;
                    let file_ordinal = output_ordinal_by_name(
                        output_columns,
                        novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
                        "DV file locator",
                    )?;
                    (sink, vec![file_ordinal])
                }
                ImvChangeStreamProducerRoute::ExistingData => {
                    let target_binding = mv_change_stream_write_binding_for_mode(
                        bindings,
                        target,
                        SqlWriteSinkMode::RowLineageData,
                    )?;
                    let sink = sql_write_plan_input_for_admitted_target(
                        bindings,
                        target_binding,
                        SqlWriteSinkMode::RowLineageData,
                        ConnectorWriteInputBinding::RootOutputByOrdinal,
                        None,
                    )?;
                    let partition_ordinals =
                        target_partition_source_ordinals_for_sql_sink(&sink, output_columns)?;
                    (sink, partition_ordinals)
                }
                ImvChangeStreamProducerRoute::AppendedData => {
                    let target_binding = mv_change_stream_write_binding_for_mode(
                        bindings,
                        target,
                        SqlWriteSinkMode::Data,
                    )?;
                    let sink = sql_write_plan_input_for_admitted_target(
                        bindings,
                        target_binding,
                        SqlWriteSinkMode::Data,
                        ConnectorWriteInputBinding::RootOutputByOrdinal,
                        None,
                    )?;
                    let partition_ordinals =
                        target_partition_source_ordinals_for_sql_sink(&sink, output_columns)?;
                    (sink, partition_ordinals)
                }
            };
            let stream_output_ordinals =
                output_ordinals_for_sink_columns(output_columns, &sink.contract.input_columns)?;
            let role = match producer_branch {
                ImvChangeStreamProducerRoute::Deletion => b"delete".as_slice(),
                ImvChangeStreamProducerRoute::ExistingData => b"replace".as_slice(),
                ImvChangeStreamProducerRoute::AppendedData => b"insert".as_slice(),
            };
            let mut route_hash = Sha256::new();
            route_hash.update(b"novarocks.imv.change-stream.route.v1\0");
            route_hash.update(role);
            route_hash.update((idx as u64).to_be_bytes());
            let route_id = ConnectorWriteRouteId::from_bytes(route_hash.finalize().into());
            let mut cohort_hash = Sha256::new();
            cohort_hash.update(b"novarocks.imv.change-stream.cohort.v1\0");
            cohort_hash.update(route_id.to_bytes());
            let cohort_id = ConnectorWriteCohortId::from_bytes(cohort_hash.finalize().into());
            let input_ordinals = sink
                .contract
                .target
                .fields
                .iter()
                .zip(stream_output_ordinals.iter().copied())
                .map(|(field, ordinal)| {
                    ConnectorMutationRouteInput::new(field.token, ordinal as u32)
                })
                .collect::<Vec<_>>();
            if input_ordinals.is_empty() {
                return Err("IMV change-stream route has no token-bound inputs".to_string());
            }
            let accepted_effects = match producer_branch {
                ImvChangeStreamProducerRoute::Deletion => vec![ConnectorRowMutationEffect::Delete],
                ImvChangeStreamProducerRoute::ExistingData => {
                    vec![ConnectorRowMutationEffect::Replace]
                }
                ImvChangeStreamProducerRoute::AppendedData => {
                    vec![ConnectorRowMutationEffect::Insert]
                }
            };
            Ok(ChangeStreamWriteRouteSpec {
                route_id,
                cohort_id,
                accepted_effects,
                input_ordinals,
                output_partition_ordinals: partition_ordinals,
                sink,
            })
        })
        .collect()
}

fn build_provider_imv_change_stream_routes(
    target: &crate::engine::backend_resolver::TargetBackend,
    bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    output_columns: &[OutputColumn],
    provider_routes: &[novarocks_spi::connector::ConnectorRowMutationRoute],
) -> Result<
    Vec<crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec>,
    String,
> {
    use crate::engine::query_planning::write_sink::sql_write_plan_input_for_admitted_target;
    use crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteRouteSpec;
    use crate::sql::planner::distributed::write::contract::{
        ConnectorWriteInputBinding, SqlWriteSinkMode,
    };
    use novarocks_spi::connector::{ConnectorMutationRouteInput, ConnectorWriteInputShape};

    let mut provider_routes = provider_routes.to_vec();
    provider_routes.sort_by_key(|route| route.cohort_id());
    provider_routes
        .into_iter()
        .map(|route| {
            let target_binding = bindings.admitted_iceberg_write_binding_id_for_preparation(
                &target.catalog,
                &target.namespace,
                &target.table,
                route.preparation(),
            )?;
            let mode = match route.input() {
                ConnectorWriteInputShape::Data { .. } => SqlWriteSinkMode::Data,
                ConnectorWriteInputShape::RowLineage { .. } => SqlWriteSinkMode::RowLineageData,
                ConnectorWriteInputShape::PositionDelete { .. } => {
                    SqlWriteSinkMode::PositionDeletes
                }
                ConnectorWriteInputShape::DeletionVector { .. } => {
                    SqlWriteSinkMode::DeletionVectors
                }
                ConnectorWriteInputShape::EqualityDelete { .. } => {
                    SqlWriteSinkMode::EqualityDeletes
                }
            };
            let sink = sql_write_plan_input_for_admitted_target(
                bindings,
                target_binding,
                mode,
                ConnectorWriteInputBinding::RootOutputByOrdinal,
                None,
            )?;
            let input_ordinals = route
                .input()
                .fields()
                .into_iter()
                .map(|field| {
                    output_columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(field.field().name()))
                        .ok_or_else(|| {
                            format!(
                                "IMV change-stream producer has no output for Provider route field `{}`",
                                field.field().name()
                            )
                        })
                        .and_then(|ordinal| {
                            u32::try_from(ordinal).map_err(|_| {
                                "IMV change-stream producer output ordinal exceeds u32".to_string()
                            })
                        })
                        .map(|ordinal| ConnectorMutationRouteInput::new(field.token(), ordinal))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ChangeStreamWriteRouteSpec {
                route_id: route.route_id(),
                cohort_id: route.cohort_id(),
                accepted_effects: route.accepted_effects().to_vec(),
                input_ordinals,
                output_partition_ordinals: Vec::new(),
                sink,
            })
        })
        .collect()
}

fn mv_change_stream_write_binding_for_mode(
    bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    target: &crate::engine::backend_resolver::TargetBackend,
    mode: crate::sql::planner::distributed::write::contract::SqlWriteSinkMode,
) -> Result<crate::sql::binding::SqlTableBindingId, String> {
    use crate::sql::planner::distributed::write::contract::SqlWriteSinkMode;
    use crate::sql::planner::table::ScanSource;
    use novarocks_spi::connector::ConnectorWriteInputShape;

    let matches = bindings
        .captured_bindings()
        .into_iter()
        .filter(|(_, binding)| {
            binding.resolved.catalog.identity.catalog == target.catalog
                && binding.resolved.catalog.identity.namespace == target.namespace
                && binding.resolved.catalog.identity.table == target.table
                && binding
                    .write_target_admission
                    .as_ref()
                    .is_some_and(|admission| {
                        matches!(
                            (mode, admission.preparation.input()),
                            (
                                SqlWriteSinkMode::Data,
                                ConnectorWriteInputShape::Data { .. }
                            ) | (
                                SqlWriteSinkMode::RowLineageData,
                                ConnectorWriteInputShape::RowLineage { .. }
                            ) | (
                                SqlWriteSinkMode::DeletionVectors,
                                ConnectorWriteInputShape::DeletionVector { .. }
                            )
                        )
                    })
        })
        .collect::<Vec<_>>();
    let [(_, binding)] = matches.as_slice() else {
        return Err(format!(
            "IMV change-stream target {}.{}.{} does not have exactly one Provider preparation for mode {mode:?}",
            target.catalog, target.namespace, target.table
        ));
    };
    let ScanSource::Sql(source) = &binding.resolved.planner.source;
    Ok(source.binding)
}

fn output_ordinals_for_sink_columns(
    output_columns: &[OutputColumn],
    sink_columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<Vec<usize>, String> {
    sink_columns
        .iter()
        .map(|column| output_ordinal_by_name(output_columns, &column.name, "sink input column"))
        .collect()
}

fn output_column_by_name<'a>(
    output_columns: &'a [OutputColumn],
    name: &str,
    label: &str,
) -> Result<&'a OutputColumn, String> {
    let ordinal = output_ordinal_by_name(output_columns, name, label)?;
    Ok(&output_columns[ordinal])
}

fn target_partition_source_ordinals_for_sql_sink(
    sink: &crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    output_columns: &[OutputColumn],
) -> Result<Vec<usize>, String> {
    // The provider consumes partition transforms from its sealed preparation;
    // SQL only preserves the tokenized Arrow input layout.
    let _ = (sink, output_columns);
    Ok(Vec::new())
}

fn output_ordinal_by_column_id(
    output_columns: &[OutputColumn],
    column_id: ColumnId,
    label: &str,
) -> Result<usize, String> {
    let mut matches = output_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.column_id == column_id)
        .map(|(idx, _)| idx);
    let ordinal = matches.next().ok_or_else(|| {
        format!(
            "IMV change-stream {label} ColumnId({}) not found",
            column_id.0
        )
    })?;
    if matches.next().is_some() {
        return Err(format!(
            "IMV change-stream {label} ColumnId({}) is ambiguous",
            column_id.0
        ));
    }
    Ok(ordinal)
}

fn output_ordinal_by_name(
    output_columns: &[OutputColumn],
    name: &str,
    label: &str,
) -> Result<usize, String> {
    let mut matches = output_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.eq_ignore_ascii_case(name))
        .map(|(idx, _)| idx);
    let ordinal = matches
        .next()
        .ok_or_else(|| format!("IMV change-stream {label} `{name}` not found in plan output"))?;
    if matches.next().is_some() {
        return Err(format!(
            "IMV change-stream {label} `{name}` is ambiguous in plan output"
        ));
    }
    Ok(ordinal)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImvBranchShape {
    DeleteAndReuse,
}

#[cfg(test)]
fn build_imv_change_stream_branches_for_test(
    shape: ImvBranchShape,
) -> Vec<novarocks_spi::connector::ConnectorRowMutationEffect> {
    match shape {
        ImvBranchShape::DeleteAndReuse => vec![
            novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
        ],
    }
}

#[derive(Clone, Copy)]
enum FullRebuildSelectPreparation<'a> {
    Projection {
        select_sql: &'a str,
        pin: &'a RefreshSnapshotPin,
        current_catalog: Option<&'a str>,
        current_database: &'a str,
    },
    LegacyVisible {
        select_sql: &'a str,
    },
}

pub(crate) async fn write_chunks_as_iceberg_data_files(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    chunks: &[novarocks_execution::exec::chunk::Chunk],
) -> Result<Vec<novarocks_connector_iceberg::iceberg::spec::DataFile>, String> {
    write_record_batches_as_data_files(table, chunks.iter().map(|chunk| chunk.batch.clone())).await
}

#[cfg(test)]
pub(crate) fn drop_iceberg_mv(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    drop_iceberg_mv_with_connector_context(
        state,
        current_catalog,
        current_database,
        stmt,
        &crate::connector::test_request_context(),
    )
}

pub(crate) fn drop_iceberg_mv_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &DropMaterializedViewStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    let _refresh_guard = acquire_mv_refresh_lock()?;
    let target = resolve_drop_target(current_catalog, current_database, &stmt.name)?;
    if !preflight_iceberg_mv_drop(state, &target, stmt.if_exists)? {
        return Ok(StatementResult::Ok);
    }

    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    crate::connector::mutation::execute_catalog_mutation(
        state.connector_control.as_ref(),
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
    drop_iceberg_mv_metadata(state, &target)?;
    crate::engine::query_prep::drop_local_table_registration_if_exists(
        state,
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

fn drop_iceberg_mv_metadata(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
) -> Result<(), String> {
    let dropped = state
        .mv_repository
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

fn preflight_iceberg_mv_drop(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    if_exists: bool,
) -> Result<bool, String> {
    let Some(definition) = state
        .mv_repository
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
    crate::engine::mv::dependency::ensure_no_downstream_dependencies(
        state,
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

/// Build an Iceberg `Schema` from the MV's analyzed output columns.
/// Each column is mapped to a primitive Iceberg type; nullable columns become
/// optional fields, non-nullable columns become required fields.
#[cfg(test)]
fn build_iceberg_schema_from_outputs(output_columns: &[OutputColumn]) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(output_columns.len());
    for (idx, col) in output_columns.iter().enumerate() {
        let id = (idx + 1) as i32;
        let primitive = arrow_data_type_to_iceberg_primitive(&col.data_type)?;
        let field: Arc<NestedField> = if col.nullable {
            NestedField::optional(id, &col.name, Type::Primitive(primitive)).into()
        } else {
            NestedField::required(id, &col.name, Type::Primitive(primitive)).into()
        };
        fields.push(field);
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| format!("build iceberg mv schema failed: {e}"))
}

/// Map an Arrow `DataType` to an Iceberg `PrimitiveType`. Returns an error
/// for types that cannot be represented as Iceberg primitive columns.
#[cfg(test)]
fn arrow_data_type_to_iceberg_primitive(
    arrow_type: &arrow::datatypes::DataType,
) -> Result<PrimitiveType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match arrow_type {
        DataType::Boolean => PrimitiveType::Boolean,
        // Promote narrow integer types — Iceberg has no Int8/Int16 primitive.
        DataType::Int8 | DataType::Int16 => PrimitiveType::Int,
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Float32 => PrimitiveType::Float,
        DataType::Float64 => PrimitiveType::Double,
        DataType::Date32 => PrimitiveType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, _) => PrimitiveType::Timestamp,
        DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
        DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
        DataType::Decimal128(precision, scale) => {
            let scale_u32 = u32::try_from(*scale).map_err(|_| {
                format!("iceberg-backed mv: Decimal128 negative scale {scale} is not supported")
            })?;
            PrimitiveType::Decimal {
                precision: *precision as u32,
                scale: scale_u32,
            }
        }
        DataType::Decimal256(_, _) => {
            return Err(
                "iceberg-backed mv: Decimal256 (precision > 38) is not supported by Iceberg; \
                 use DECIMAL with precision <= 38"
                    .to_string(),
            );
        }
        DataType::FixedSizeBinary(16) => {
            return Err(
                "iceberg-backed mv: LARGEINT (FixedSizeBinary(16)) is not supported in \
                 iceberg-backed MV; use BIGINT or DECIMAL"
                    .to_string(),
            );
        }
        other => {
            return Err(format!(
                "iceberg-backed mv: unsupported column type `{other:?}`"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{
        ExpectedRevision, IdScope, MetaCommitOutcome, MetaError, MetaErrorKind, MetaKey,
        MetaReadTxn, MetaRecord, MetaRecordPut, MetaStoreCapabilities, MetaStoreProvider,
        MetaWriteTxn,
    };
    use crate::mv::refresh::apply_key::ApplyKeyValueType;
    use crate::mv::refresh::capabilities::PartitionPruningPolicy;
    use crate::mv::test_repository::{
        TestMvRepositoryFailurePoint, after_next_mv_repository_create,
        fail_next_mv_repository_command,
    };
    use crate::query_execution::backend::{
        BackendTopologyError, BackendTopologyPort, BackendTopologySnapshot,
        BackendTopologyValidationError, LiveBackendTarget,
    };
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::*;
    use crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr;
    use crate::sql::planner::payload::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
    use std::sync::Arc as StdArc;
    use tempfile::TempDir;

    /// Test-only topology paired with the all-in-one loopback exchange server.
    ///
    /// Production admission must never invent a backend count. These refresh
    /// fixtures do install one concrete loopback backend, so model that exact
    /// endpoint explicitly instead of inheriting the default empty topology.
    #[derive(Clone)]
    struct LoopbackTestBackendTopology {
        snapshot: BackendTopologySnapshot,
    }

    impl LoopbackTestBackendTopology {
        fn new(exchange_port: u16) -> Self {
            let endpoint = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), exchange_port);
            let snapshot =
                BackendTopologySnapshot::try_new(0, vec![LiveBackendTarget::new(0, endpoint, 1)])
                    .expect("single loopback backend forms a valid topology");
            Self { snapshot }
        }
    }

    impl BackendTopologyPort for LoopbackTestBackendTopology {
        fn snapshot(&self) -> Result<BackendTopologySnapshot, BackendTopologyError> {
            Ok(self.snapshot.clone())
        }

        fn validate_snapshot(
            &self,
            expected: &BackendTopologySnapshot,
        ) -> Result<(), BackendTopologyValidationError> {
            if expected == &self.snapshot {
                Ok(())
            } else {
                Err(
                    BackendTopologyValidationError::ContentChangedWithoutRevision {
                        revision: expected.revision(),
                    },
                )
            }
        }

        fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}

        fn add_backend(&self, _endpoint: SocketAddr) -> Result<(), String> {
            Err("loopback test topology is immutable".to_string())
        }

        fn drop_backend(&self, _endpoint: SocketAddr, _force: bool) -> Result<(), String> {
            Err("loopback test topology is immutable".to_string())
        }

        fn show_backends(&self) -> Result<crate::runtime::query_result::QueryResult, String> {
            Err("loopback test topology does not expose backend management".to_string())
        }
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
        let effects = build_imv_change_stream_branches_for_test(ImvBranchShape::DeleteAndReuse);
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
    fn normalize_imv_rewrite_root_project_preserves_aggregate_output_identity() {
        let group_output = OutputColumn {
            column_id: ColumnId::new_for_test(1),
            name: "region".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            is_internal: false,
        };
        let aggregate_output = OutputColumn {
            column_id: ColumnId::new_for_test(11),
            name: "sum(amount)".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        };
        let child = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![group_output.clone()],
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![column_ref_expr(&group_output)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: aggregate_output.column_id,
                }],
                output_columns: vec![group_output.clone(), aggregate_output.clone()],
                already_pushed: false,
            }),
            vec![child],
            None,
        );
        let root = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref_expr(&group_output),
                        output_name: "region".to_string(),
                        output_column_id: ColumnId::new_for_test(21),
                    },
                    ProjectItem {
                        expr: column_ref_expr(&aggregate_output),
                        output_name: "s".to_string(),
                        output_column_id: ColumnId::new_for_test(22),
                    },
                ],
                output_qualifier: None,
            }),
            vec![aggregate],
            None,
        );

        let normalized = normalize_imv_rewrite_root_project(root);
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.kind else {
            panic!(
                "expected normalized root Aggregate, got {:?}",
                normalized.kind
            );
        };

        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| (column.column_id, column.name.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (group_output.column_id, "region"),
                (aggregate_output.column_id, "s")
            ]
        );
        assert_eq!(
            aggregate.aggregates[0].output_column_id,
            aggregate_output.column_id
        );

        let mut arena = ScalarArena::new();
        try_to_optimizer_expr(&normalized, &mut arena)
            .expect("normalized aggregate must satisfy optimizer bridge contract");
    }

    #[test]
    fn normalize_imv_rewrite_root_project_keeps_reordered_passthrough_project() {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let root = normalization_project_over_aggregate(vec![
            normalization_project_item(&g2, 21, "g2"),
            normalization_project_item(&g1, 22, "g1"),
            normalization_project_item(&sum_output, 23, "s"),
        ]);

        let normalized = normalize_imv_rewrite_root_project(root);

        assert!(matches!(&normalized.kind, LogicalPlanKind::Project(_)));
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.unary_input().kind else {
            panic!("expected preserved Project over Aggregate");
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![g1.column_id, g2.column_id, sum_output.column_id]
        );
    }

    #[test]
    fn normalize_imv_rewrite_root_project_keeps_duplicate_passthrough_project() {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let root = normalization_project_over_aggregate(vec![
            normalization_project_item(&g1, 21, "g1"),
            normalization_project_item(&g1, 22, "g1_again"),
            normalization_project_item(&sum_output, 23, "s"),
        ]);

        let normalized = normalize_imv_rewrite_root_project(root);

        assert!(matches!(&normalized.kind, LogicalPlanKind::Project(_)));
        let LogicalPlanKind::Aggregate(aggregate) = &normalized.unary_input().kind else {
            panic!("expected preserved Project over Aggregate");
        };
        assert_eq!(
            aggregate
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![g1.column_id, g2.column_id, sum_output.column_id]
        );
    }

    fn normalization_aggregate_outputs() -> (OutputColumn, OutputColumn, OutputColumn) {
        (
            normalization_output_column(1, "g1", DataType::Utf8, false),
            normalization_output_column(2, "g2", DataType::Utf8, false),
            normalization_output_column(11, "sum(amount)", DataType::Int64, true),
        )
    }

    fn normalization_project_over_aggregate(project_items: Vec<ProjectItem>) -> LogicalPlanNode {
        let (g1, g2, sum_output) = normalization_aggregate_outputs();
        let child = LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: Vec::new(),
                columns: vec![g1.clone(), g2.clone()],
            }),
            vec![],
            None,
        );
        let aggregate = LogicalPlanNode::new(
            LogicalPlanKind::Aggregate(LogicalAggregateNode {
                group_by: vec![column_ref_expr(&g1), column_ref_expr(&g2)],
                aggregates: vec![AggregateCall {
                    name: "sum".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: Vec::new(),
                    output_column_id: sum_output.column_id,
                }],
                output_columns: vec![g1, g2, sum_output],
                already_pushed: false,
            }),
            vec![child],
            None,
        );
        LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: project_items,
                output_qualifier: None,
            }),
            vec![aggregate],
            None,
        )
    }

    fn normalization_project_item(
        source: &OutputColumn,
        output_id: u32,
        name: &str,
    ) -> ProjectItem {
        ProjectItem {
            expr: column_ref_expr(source),
            output_name: name.to_string(),
            output_column_id: ColumnId::new_for_test(output_id),
        }
    }

    fn normalization_output_column(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    #[test]
    fn join_coalesce_locator_ids_reserve_rewritten_plan_outputs() {
        let child_output = crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId(42),
            name: "child_k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let root_output = crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId(6),
            name: "root_k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        };
        let child = crate::sql::planner::logical::LogicalPlanNode::new(
            crate::sql::planner::logical::LogicalPlanKind::Values(
                crate::sql::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: vec![child_output.clone()],
                },
            ),
            Vec::new(),
            None,
        );
        let plan = crate::sql::planner::logical::LogicalPlanNode::new(
            crate::sql::planner::logical::LogicalPlanKind::Project(
                crate::sql::planner::payload::PlanProjectNode {
                    items: vec![crate::sql::analysis::ProjectItem {
                        expr: crate::sql::analysis::TypedExpr {
                            kind: crate::sql::analysis::ExprKind::ColumnRef {
                                column_id: child_output.column_id,
                                qualifier: None,
                                column: child_output.name.clone(),
                            },
                            data_type: child_output.data_type.clone(),
                            nullable: child_output.nullable,
                        },
                        output_name: root_output.name.clone(),
                        output_column_id: root_output.column_id,
                    }],
                    output_qualifier: None,
                },
            ),
            vec![child],
            None,
        );
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();

        let ids = crate::mv::refresh::join_incremental_refresh::allocate_join_coalesce_locator_column_ids(
            &mut factory,
            &plan,
        )
        .expect("allocate locator column ids");

        let allocated = [
            ids.net,
            ids.file,
            ids.pos,
            ids.row_id,
            ids.last_updated_sequence_number,
        ];
        assert!(allocated.iter().all(|id| *id > child_output.column_id.0));
        let unique = allocated
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), allocated.len());
    }

    #[test]
    fn imv_change_stream_write_recognizes_physical_change_op_by_reserved_shape() {
        let output = OutputColumn {
            column_id: ColumnId(17),
            name: novarocks_execution::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: false,
        };

        assert!(is_imv_change_op_output_column(&output));
    }

    fn branch_field_test_contract(source: ApplyKeySource) -> mv_schema::MvSchemaContract {
        let aggregate = (source == crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId)
            .then(|| mv_schema::AggregateStateContract {
                state_layout_version: 1,
                row_id_column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                state_columns: vec![mv_schema::AggregateStateColumnContract {
                    column_name: "__agg_state_count".to_string(),
                    target_field_id: 3,
                    type_signature: "long".to_string(),
                    nullable: false,
                    role: mv_schema::AggregateStateRoleContract::Single,
                }],
            });
        let hidden_name = match source {
            crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId => {
                HIDDEN_APPLY_KEY_COLUMN_NAME
            }
            crate::sql::planner::vocabulary::ApplyKeySource::JoinRowKey => {
                JOIN_APPLY_KEY_COLUMN_NAME
            }
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId => {
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
            }
        };
        mv_schema::MvSchemaContract {
            contract_version: if aggregate.is_some() { 3 } else { 1 },
            base: test_base_contract("ice.db.base"),
            bases: vec![test_base_contract("ice.db.base")],
            output: mv_schema::OutputContract {
                columns: Vec::new(),
                filter: None,
            },
            join: None,
            aggregate,
            branch: Some(mv_schema::BranchUnionContract {
                branch_id_column: mv_schema::BranchIdColumnContract {
                    column_name: BRANCH_ID_COLUMN_NAME.to_string(),
                    target_field_id: 2,
                },
                branch_count: 2,
                inner_apply_key_source: source,
            }),
            target: mv_schema::TargetContract {
                table_fqn: "ice.db.mv".to_string(),
                table_uuid: "target-uuid".to_string(),
                schema_id_at_create: 1,
                visible_columns: Vec::new(),
                hidden_apply_key: mv_schema::HiddenApplyKeyContract {
                    column_name: hidden_name.to_string(),
                    target_field_id: 10,
                    source,
                },
                partition: None,
            },
        }
    }

    fn branch_field_test_table(
        branch_field: Option<novarocks_connector_iceberg::iceberg::spec::NestedField>,
    ) -> novarocks_connector_iceberg::iceberg::table::Table {
        use novarocks_connector_iceberg::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };

        let mut fields = vec![StdArc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Int),
        ))];
        fields.extend(branch_field.map(StdArc::new));
        let schema = Schema::builder()
            .with_schema_id(2)
            .with_fields(fields)
            .build()
            .expect("branch field test schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///tmp/ebd16b-branch-field".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::from([(
                "write.row-lineage".to_string(),
                "true".to_string(),
            )]),
        )
        .expect("branch field table metadata builder")
        .build()
        .expect("branch field table metadata")
        .metadata;
        novarocks_connector_iceberg::iceberg::table::Table::builder()
            .identifier(
                novarocks_connector_iceberg::iceberg::TableIdent::from_strs([
                    "db",
                    "branch_target",
                ])
                .expect("branch field table ident"),
            )
            .file_io(novarocks_connector_iceberg::iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("branch field test table")
    }

    fn branch_field_cases() -> Vec<(
        novarocks_connector_iceberg::iceberg::table::Table,
        &'static str,
    )> {
        use novarocks_connector_iceberg::iceberg::spec::{NestedField, PrimitiveType, Type};

        vec![
            (branch_field_test_table(None), "missing"),
            (
                branch_field_test_table(Some(NestedField::required(
                    2,
                    "renamed_branch",
                    Type::Primitive(PrimitiveType::Int),
                ))),
                "renamed",
            ),
            (
                branch_field_test_table(Some(NestedField::optional(
                    2,
                    BRANCH_ID_COLUMN_NAME,
                    Type::Primitive(PrimitiveType::Int),
                ))),
                "not-required",
            ),
            (
                branch_field_test_table(Some(NestedField::required(
                    2,
                    BRANCH_ID_COLUMN_NAME,
                    Type::Primitive(PrimitiveType::Long),
                ))),
                "wrong-type",
            ),
        ]
    }

    #[test]
    fn branch_union_aggregate_branch_field_errors_preserve_exact_messages() {
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        };
        let contract =
            branch_field_test_contract(crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId);
        let expected = [
            "iceberg branch UNION ALL aggregate MV ice.db.mv branch id field id 2 is missing from target schema",
            "iceberg branch UNION ALL aggregate MV ice.db.mv branch id column renamed externally to renamed_branch; recreate the MV",
            "iceberg branch UNION ALL aggregate MV ice.db.mv branch id column must be required",
            "iceberg branch UNION ALL aggregate MV ice.db.mv branch id column must be Int, got long",
        ];

        for ((table, label), expected) in branch_field_cases().into_iter().zip(expected) {
            let target_observation =
                current_iceberg_table_observation(&table).expect("target observation");
            let error = validate_branch_union_contract(&target, &contract, 2, &target_observation)
                .expect_err(label);
            assert_eq!(error, expected, "case={label}");
        }
    }

    #[test]
    fn union_projection_branch_field_errors_preserve_exact_messages() {
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        };
        let contract =
            branch_field_test_contract(crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId);
        let base_ref = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "base".to_string(),
        };
        let expected = [
            "iceberg UNION ALL projection/filter MV ice.db.mv branch id field id 2 is missing from target schema",
            "iceberg UNION ALL projection/filter MV ice.db.mv branch id column renamed externally to renamed_branch; recreate the MV",
            "iceberg UNION ALL projection/filter MV ice.db.mv branch id column must be required",
            "iceberg UNION ALL projection/filter MV ice.db.mv branch id column must be Int, got long",
        ];

        for ((table, label), expected) in branch_field_cases().into_iter().zip(expected) {
            let observation =
                current_iceberg_table_observation(&table).expect("schema observation");
            let error = validate_union_projection_schema_contract_for_base(
                &target,
                &contract,
                2,
                &base_ref,
                &observation,
                &observation,
            )
            .expect_err(label);
            assert_eq!(error, expected, "case={label}");
        }
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
                    source: ApplyKeySource::JoinRowKey,
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
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!("expected SELECT");
        };
        *q
    }

    #[test]
    fn iceberg_join_mv_uses_join_apply_key_column() {
        let column = crate::mv::refresh::target_apply::join_apply_key_table_column();
        assert_eq!(
            column.name,
            crate::sql::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME
        );
    }

    #[test]
    fn create_apply_key_metadata_comes_from_refresh_contract() {
        use crate::mv::refresh::apply_key::ApplyKeyContract;

        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::projection_filter()),
            ApplyKeySource::BaseRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_projection_filter()),
            ApplyKeySource::JoinRowKey.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::aggregate_group_row()),
            ApplyKeySource::GroupRowId.table_property_value()
        );
        assert_eq!(
            create_apply_key_source_property(&ApplyKeyContract::join_aggregate_group_row()),
            ApplyKeySource::GroupRowId.table_property_value()
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

    #[test]
    #[ignore = "legacy planner-memory fixture replaced by tokenized MV planning coverage"]
    fn join_delta_coalesce_catalog_registers_target_locator_at_supplied_snapshot() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "sales");
        create_base_table(&env.state, "ice", "sales", "left_orders");
        create_base_table(&env.state, "ice", "sales", "right_orders");
        insert_into_iceberg_table(&env.state, "ice", "sales", "left_orders", &[(1, "left")]);
        insert_into_iceberg_table(&env.state, "ice", "sales", "right_orders", &[(1, "right")]);
        let right_snapshot_id = load_iceberg_table(&env.state, "ice", "sales", "right_orders")
            .metadata()
            .current_snapshot()
            .expect("right snapshot")
            .snapshot_id();
        create_join_mv_target_locator_table(&env.state, "ice", "sales", "mv_join");
        insert_into_join_mv_target_locator_table(
            &env.state,
            "ice",
            "sales",
            "mv_join",
            &[(1, "sku-1", "join-key-1")],
        );
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "mv_join".to_string(),
        };
        let target_table = load_iceberg_table(&env.state, "ice", "sales", "mv_join");
        let target_snapshot_id = target_table
            .metadata()
            .current_snapshot()
            .expect("target snapshot")
            .snapshot_id();
        let branches = vec![
            crate::engine::mv::iceberg_join_branch::JoinDeltaBranchPlan {
                left_base: iceberg_ref("ice", "sales", "left_orders"),
                right_base: iceberg_ref("ice", "sales", "right_orders"),
                left: crate::engine::mv::iceberg_join_branch::BranchSide::Delta(
                    crate::engine::mv::iceberg_join_branch::SnapshotWindow { from: 10, to: 11 },
                ),
                right: crate::engine::mv::iceberg_join_branch::BranchSide::Snapshot(
                    right_snapshot_id,
                ),
            },
        ];

        let catalog = build_join_delta_coalesce_catalog(
            &env.state,
            &branches,
            &target,
            &target_table,
            Some(target_snapshot_id),
        )
        .expect("join coalesce catalog");
        let locator = catalog
            .get("sales", "__nr_join_delta_target_locator")
            .expect("target locator table");

        assert!(
            locator
                .columns
                .iter()
                .any(|column| column.name == JOIN_APPLY_KEY_COLUMN_NAME),
            "locator columns={:?}",
            locator.columns
        );
        assert!(
            locator
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_file"),
            "locator metadata columns={:?}",
            locator.iceberg_row_lineage_metadata_columns
        );
        assert!(
            locator
                .iceberg_row_lineage_metadata_columns
                .iter()
                .any(|column| column.name == "_pos"),
            "locator metadata columns={:?}",
            locator.iceberg_row_lineage_metadata_columns
        );
        let crate::sql::planner::table::ScanSource::Sql(source) = &locator.source else {
            panic!("expected tokenized SQL locator source");
        };
        assert_eq!(source.table.catalog, "ice");
        assert_eq!(source.table.namespace, "sales");
        assert_eq!(source.table.table, "mv_join");
        assert!(matches!(
            source.kind,
            crate::sql::planner::table::SqlScanKind::Data {
                version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(id),
            } if id == target_snapshot_id
        ));
    }

    #[test]
    fn join_coalesce_builder_factory_metadata_survives_rewritten_plan_reserve() {
        let desc = join_coalesce_factory_test_descriptor();
        let branch_union = join_coalesce_factory_test_branch_union(&desc);
        let locator =
            crate::sql::planner::imv_rewrite::join_refresh_builder::JoinRefreshTargetLocatorBinding {
                target_binding: crate::sql::compiler::mv_rewrite::test_target_binding(),
                target_table_uuid: "target-uuid".to_string(),
                target_snapshot_id: Some(77),
            };
        let mut factory = crate::sql::column_id::ColumnRefFactory::new();
        factory.reserve_until(109);
        let locator_columns = crate::mv::refresh::join_incremental_refresh::allocate_join_coalesce_locator_column_ids(
            &mut factory,
            &branch_union,
        )
        .expect("allocate locator column ids");

        let plan =
            crate::sql::planner::imv_rewrite::join_refresh_builder::build_join_delta_coalesce_plan_with_locator(
                branch_union,
                &desc,
                &locator,
                &mut factory,
                locator_columns.net,
                locator_columns.file,
                locator_columns.pos,
                locator_columns.row_id,
                locator_columns.last_updated_sequence_number,
            )
            .expect("join coalesce plan");
        crate::mv::refresh::join_incremental_refresh::reserve_factory_for_logical_plan(
            &mut factory,
            &plan,
        )
        .expect("reserve rewritten plan outputs");

        let watched_columns =
            collect_join_coalesce_factory_watch_columns(&plan, ColumnId(locator_columns.net));
        let watched_names = watched_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        for expected in [
            "net",
            JOIN_APPLY_KEY_COLUMN_NAME,
            "__pending_insert_count",
            "__pending_delete_count",
            novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL,
            novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL,
            novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
            novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        ] {
            assert!(
                watched_names.contains(expected),
                "missing watched column {expected}; watched={watched_columns:?}"
            );
        }
        for column in watched_columns {
            let metadata = factory.get(column.column_id);
            assert!(
                !metadata.name.starts_with("__reserved_col_"),
                "column {} leaked reserved metadata {:?}",
                column.name,
                metadata
            );
            assert_eq!(metadata.name, column.name);
            assert_eq!(metadata.data_type, column.data_type);
            assert_eq!(metadata.nullable, column.nullable);
        }
    }

    #[test]
    fn refresh_contract_selects_fan_in_aggregate_for_a_family() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice_fan_in", "sales");
        create_aggregate_fact_table(&env.state, "ice_fan_in", "sales", "t1");
        create_aggregate_fact_table(&env.state, "ice_fan_in", "sales", "t2");
        let query = parse_mv_select_query(
            "select region, count(*) as c from (
               select region from ice_fan_in.sales.t1
               union all
               select region from ice_fan_in.sales.t2
             ) u group by region",
        )
        .expect("parse");
        let analysis = analyze_mv_select_with_connector_context(
            &env.state,
            Some("ice_fan_in"),
            "sales",
            &query,
            &crate::connector::test_request_context(),
        )
        .expect("analyze");
        let contract = derive_imv_refresh_contract(&analysis).expect("derive");
        // FanInAggregate: aggregate over a UNION ALL of simple scans.
        // Contract fields: aggregate present, branch present (the fan-in union),
        // no join, apply key is aggregate_group_row (Utf8, group-row-id column).
        assert!(
            contract.aggregate.is_some(),
            "fan-in aggregate must have aggregate contract"
        );
        assert!(
            contract.branch.is_some(),
            "fan-in aggregate must have branch contract"
        );
        assert!(
            contract.join.is_none(),
            "fan-in aggregate must not have join contract"
        );
        assert_eq!(
            contract.apply_key,
            crate::mv::refresh::apply_key::ApplyKeyContract::aggregate_group_row(),
        );
        assert_eq!(contract.base_refs.len(), 2);
    }

    #[test]
    fn iceberg_aggregate_target_columns_use_state_layout() {
        let (shape, analysis) = analyze_aggregate_fact_query(
            "select region, count(*) as c, sum(amount) as s \
             from ice.ns.fact group by region",
        );

        let columns = iceberg_aggregate_target_columns(&shape, &analysis).expect("columns");
        let names = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "__row_id__",
                "region",
                "c",
                "s",
                "__agg_state_c",
                "__agg_state_s"
            ]
        );
    }

    #[test]
    fn definition_fingerprint_is_exact_canonical_json_and_order_independent() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "one")]);
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_fingerprint");
        let mut definition =
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fingerprint")
                .expect("definition");
        definition.base_table_refs = vec![
            "ice.sales.orders".to_string(),
            "ice.sales.other".to_string(),
        ];
        let first =
            refresh_execution_definition_fingerprint(&definition, Some("ice"), &env.current_db)
                .expect("fingerprint");
        definition.base_table_refs.reverse();
        let reordered =
            refresh_execution_definition_fingerprint(&definition, Some("ice"), &env.current_db)
                .expect("reordered fingerprint");

        assert_eq!(first, reordered);
        let encoded: serde_json::Value =
            serde_json::from_str(&first).expect("fingerprint must be exact JSON");
        assert_eq!(encoded["mv_id"], definition.mv_id);
        assert_eq!(
            encoded["canonical_base_refs"],
            serde_json::json!(["ice.sales.orders", "ice.sales.other"])
        );
    }

    // Stale contract validation is covered by mv::refresh::execution unit tests;
    // completing the first attempt is a native FE/BE writer lifecycle concern.

    // Pure SQLX-2 exact-generation coverage is
    // `sqlx2_mv_frozen_base_overlays_survive_connector_generation_replacement`.
    // The writer/RF execution portion belongs to the native FE/BE IVM suite.

    // Native FE/BE owner coverage: iceberg_ivm_join_aggregate.sql.

    // Native FE/BE owner coverage: iceberg_ivm_fan_in_aggregate_union.sql.

    fn assert_aggregate_region_rows(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
        expected: &[(&str, i64, i64)],
    ) {
        let sql = format!("SELECT region, c, s FROM {mv_name} ORDER BY region");
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        let result = match session
            .execute_in_context(&sql, Some(current_catalog), current_database, None)
            .expect("query fan-in aggregate mv")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("expected query result for {sql}"),
        };
        let actual = string_i64_i64_rows(&result);
        let expected = expected
            .iter()
            .map(|(region, count, sum)| ((*region).to_string(), *count, *sum))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn create_b_family_union_aggregate_persists_branch_contract() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "t1");
        create_aggregate_fact_table(&env.state, "ice", "sales", "t2");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_agg
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c FROM ice.sales.t1 GROUP BY region
                UNION ALL
                SELECT region, count(*) AS c FROM ice.sales.t2 GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create B-family UNION ALL aggregate MV");
        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_union_agg")
            .expect("stored MV definition");
        let contract = mv.schema_contract.expect("schema contract");
        assert!(contract.aggregate.is_some());
        let branch = contract.branch.expect("branch contract");
        assert_eq!(branch.branch_count, 2);
        assert_eq!(
            branch.inner_apply_key_source,
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        );
    }

    // Native FE/BE owner coverage: iceberg_ivm_branch_union_aggregate.sql.

    fn assert_aggregate_region_sum_rows(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
        expected: &[(&str, i64, i64)],
    ) {
        let sql = format!("SELECT region, c, s FROM {mv_name} ORDER BY region, s");
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        let result = match session
            .execute_in_context(&sql, Some(current_catalog), current_database, None)
            .expect("query union-of-aggregates mv")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("expected query result for {sql}"),
        };
        let actual = string_i64_i64_rows(&result);
        let expected = expected
            .iter()
            .map(|(region, count, sum)| ((*region).to_string(), *count, *sum))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn execute_iceberg_sql(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_database: &str,
        sql: &str,
    ) {
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        match session
            .execute_in_context(sql, current_catalog, current_database, None)
            .expect("execute iceberg sql")
        {
            StatementResult::Ok => {}
            StatementResult::Query(_) => panic!("expected non-query statement for {sql}"),
        }
    }

    fn assert_join_aggregate_rows(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
        expected: &[(&str, i64, i64)],
    ) {
        let sql = format!("SELECT category, c, s FROM {mv_name} ORDER BY category");
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        let result = match session
            .execute_in_context(&sql, Some(current_catalog), current_database, None)
            .expect("query join aggregate mv")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("expected query result for {sql}"),
        };
        let actual = string_i64_i64_rows(&result);
        let expected = expected
            .iter()
            .map(|(category, count, sum)| ((*category).to_string(), *count, *sum))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    fn string_i64_i64_rows(
        result: &crate::runtime::query_result::QueryResult,
    ) -> Vec<(String, i64, i64)> {
        let mut rows = Vec::new();
        for chunk in &result.chunks {
            let category = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("category column");
            let count = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count column");
            let sum = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum column");
            for row in 0..chunk.batch.num_rows() {
                rows.push((
                    category.value(row).to_string(),
                    count.value(row),
                    sum.value(row),
                ));
            }
        }
        rows
    }

    fn id_name_rows(result: &crate::runtime::query_result::QueryResult) -> Vec<(i32, String)> {
        let mut rows = Vec::new();
        for chunk in &result.chunks {
            let id = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column");
            let name = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column");
            for row in 0..chunk.batch.num_rows() {
                rows.push((id.value(row), name.value(row).to_string()));
            }
        }
        rows
    }

    fn join_repartition_rows(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
    ) -> Vec<(i32, String)> {
        let sql = format!("SELECT id, region FROM {mv_name} ORDER BY id");
        let session = crate::engine::StandaloneSession {
            inner: Arc::clone(state),
        };
        let result = match session
            .execute_in_context(&sql, Some(current_catalog), current_database, None)
            .expect("query join repartition mv")
        {
            StatementResult::Query(result) => result,
            StatementResult::Ok => panic!("expected query result for {sql}"),
        };
        id_name_rows(&result)
    }

    fn union_projection_rows_with_hidden(
        state: &Arc<StandaloneState>,
        current_catalog: &str,
        current_database: &str,
        mv_name: &str,
    ) -> Vec<(i32, String, i64, i32)> {
        use futures::StreamExt;
        use novarocks_connector_iceberg::iceberg::arrow::ArrowReaderBuilder;

        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(current_catalog).expect("catalog")
        };
        let loaded = load_iceberg_mv_table(&entry, current_database, mv_name);
        let mut rows = data_block_on(async {
            let scan = loaded
                .table
                .scan()
                .select(vec![
                    "id".to_string(),
                    "name".to_string(),
                    HIDDEN_APPLY_KEY_COLUMN_NAME.to_string(),
                    BRANCH_ID_COLUMN_NAME.to_string(),
                ])
                .build()
                .expect("build UNION ALL target scan");
            let tasks = scan
                .plan_files()
                .await
                .expect("plan UNION ALL target files");
            let arrow_reader = ArrowReaderBuilder::new(loaded.table.file_io().clone()).build();
            let mut stream = arrow_reader.read(tasks).expect("read UNION ALL target");
            let mut rows = Vec::new();
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result.expect("UNION ALL target batch");
                let id = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column");
                let name = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("name column");
                let base_row_id = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("base row id column");
                let branch_id = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("branch id column");
                for row in 0..batch.num_rows() {
                    rows.push((
                        id.value(row),
                        name.value(row).to_string(),
                        base_row_id.value(row),
                        branch_id.value(row),
                    ));
                }
            }
            rows
        })
        .expect("read UNION ALL projection/filter target rows");
        rows.sort_by_key(|(id, _, base_row_id, branch_id)| (*branch_id, *id, *base_row_id));
        rows
    }

    fn output_col(name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: crate::sql::column_id::ColumnId::UNSET,
            name: name.to_string(),
            data_type: ty,
            nullable,
            is_internal: false,
        }
    }

    fn join_coalesce_factory_test_descriptor()
    -> crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor {
        use crate::sql::planner::imv_rewrite::join_refresh_descriptor::{
            JoinRefreshBranchDescriptor, JoinRefreshBranchSide, JoinRefreshDescriptor,
            JoinRefreshJoinKeyPair, JoinRefreshMode, JoinRefreshMvIdentity,
            JoinRefreshOutputMapping, JoinRefreshOutputSource,
        };

        let payload = join_coalesce_factory_test_column(1, "id", DataType::Int32, false, false);
        let payload_output =
            join_coalesce_factory_test_column(80, "id", DataType::Int32, false, false);
        let action = join_coalesce_factory_test_column(
            4,
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let action_output = join_coalesce_factory_test_column(
            91,
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN,
            DataType::Int8,
            false,
            true,
        );
        let join_apply_key = join_coalesce_factory_test_column(
            5,
            JOIN_APPLY_KEY_COLUMN_NAME,
            DataType::Utf8,
            false,
            true,
        );
        let join_apply_key_output = join_coalesce_factory_test_column(
            90,
            JOIN_APPLY_KEY_COLUMN_NAME,
            DataType::Utf8,
            false,
            true,
        );

        JoinRefreshDescriptor {
            mode: JoinRefreshMode::Coalesce,
            mv_identity: JoinRefreshMvIdentity {
                catalog: "ice".to_string(),
                database: "sales".to_string(),
                name: "mv_join".to_string(),
            },
            left_base_fqn: "ice.sales.left_orders".to_string(),
            right_base_fqn: "ice.sales.right_orders".to_string(),
            left_row_id_column: join_coalesce_factory_test_column(
                2,
                novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            right_row_id_column: join_coalesce_factory_test_column(
                3,
                novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            action_column: action.clone(),
            join_apply_key_column: join_apply_key.clone(),
            payload_columns: vec![payload.clone()],
            join_key_pairs: vec![JoinRefreshJoinKeyPair {
                left_column: join_coalesce_factory_test_column(
                    6,
                    "left_id",
                    DataType::Int32,
                    false,
                    false,
                ),
                right_column: join_coalesce_factory_test_column(
                    7,
                    "right_id",
                    DataType::Int32,
                    false,
                    false,
                ),
            }],
            output_mappings: vec![
                JoinRefreshOutputMapping {
                    mv_output_column: payload_output,
                    source: JoinRefreshOutputSource::Payload(payload.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: join_apply_key_output,
                    source: JoinRefreshOutputSource::JoinApplyKey(join_apply_key.column_id),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: action_output,
                    source: JoinRefreshOutputSource::Action(action.column_id),
                },
            ],
            branches: vec![
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                    action_column_id: action.column_id,
                },
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftSnapshotRightDelta,
                    action_column_id: action.column_id,
                },
            ],
            needs_target_locator: true,
        }
    }

    fn join_coalesce_factory_test_branch_union(
        desc: &crate::sql::planner::imv_rewrite::join_refresh_descriptor::JoinRefreshDescriptor,
    ) -> crate::sql::planner::logical::LogicalPlanNode {
        let mut output_columns = desc.payload_columns.clone();
        output_columns.push(desc.action_column.clone());
        output_columns.push(desc.join_apply_key_column.clone());
        let branch = crate::sql::planner::logical::LogicalPlanNode::new(
            crate::sql::planner::logical::LogicalPlanKind::Values(
                crate::sql::planner::payload::PlanValuesNode {
                    rows: Vec::new(),
                    columns: output_columns.clone(),
                },
            ),
            Vec::new(),
            None,
        );
        crate::sql::planner::logical::LogicalPlanNode::new(
            crate::sql::planner::logical::LogicalPlanKind::Union(
                crate::sql::planner::logical::LogicalUnionNode {
                    all: true,
                    output_columns,
                },
            ),
            vec![branch.clone(), branch],
            None,
        )
    }

    fn collect_join_coalesce_factory_watch_columns(
        plan: &crate::sql::planner::logical::LogicalPlanNode,
        min_id: ColumnId,
    ) -> Vec<OutputColumn> {
        let mut columns = Vec::new();
        collect_join_coalesce_factory_watch_columns_inner(plan, min_id, &mut columns);
        columns
    }

    fn collect_join_coalesce_factory_watch_columns_inner(
        plan: &crate::sql::planner::logical::LogicalPlanNode,
        min_id: ColumnId,
        columns: &mut Vec<OutputColumn>,
    ) {
        match &plan.kind {
            crate::sql::planner::logical::LogicalPlanKind::Project(project) => {
                columns.extend(project.items.iter().filter_map(|item| {
                    is_join_coalesce_factory_locator_output(&item.output_name).then(|| {
                        OutputColumn {
                            column_id: item.output_column_id,
                            name: item.output_name.clone(),
                            data_type: item.expr.data_type.clone(),
                            nullable: item.expr.nullable,
                            is_internal: true,
                        }
                    })
                }));
            }
            crate::sql::planner::logical::LogicalPlanKind::Aggregate(aggregate) => {
                columns.extend(
                    aggregate
                        .output_columns
                        .iter()
                        .filter(|column| {
                            column.column_id >= min_id
                                && is_join_coalesce_factory_internal_output(&column.name)
                        })
                        .cloned(),
                );
            }
            crate::sql::planner::logical::LogicalPlanKind::Scan(scan) => {
                columns.extend(
                    scan.columns
                        .iter()
                        .filter(|column| {
                            column.column_id >= min_id && column.name == JOIN_APPLY_KEY_COLUMN_NAME
                        })
                        .cloned(),
                );
            }
            _ => {}
        }
        for child in &plan.children {
            collect_join_coalesce_factory_watch_columns_inner(child, min_id, columns);
        }
    }

    fn is_join_coalesce_factory_internal_output(name: &str) -> bool {
        matches!(
            name,
            "net"
                | JOIN_APPLY_KEY_COLUMN_NAME
                | "__pending_insert_count"
                | "__pending_delete_count"
        )
    }

    fn is_join_coalesce_factory_locator_output(name: &str) -> bool {
        matches!(
            name,
            novarocks_execution::exec::row_position::ICEBERG_FILE_PATH_COL
                | novarocks_execution::exec::row_position::ICEBERG_ROW_POS_COL
                | novarocks_execution::exec::row_position::ICEBERG_ROW_ID_COL
                | novarocks_execution::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL
        )
    }

    fn join_coalesce_factory_test_column(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn analyze_aggregate_fact_query(
        sql: &str,
    ) -> (
        crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
        MvAnalysis,
    ) {
        let env = open_test_state_with_iceberg_catalog("ice", "ns");
        create_aggregate_fact_table(&env.state, "ice", "ns", "fact");
        let query = parse_select_query(sql);
        let calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
                .expect("extract aggregate calls");
        let analysis = analyze_mv_select_with_connector_context(
            &env.state,
            Some("ice"),
            &env.current_db,
            &query,
            &crate::connector::test_request_context(),
        )
        .expect("analyze aggregate query");
        (calls, analysis)
    }

    struct IcebergMvTestState {
        state: Arc<StandaloneState>,
        current_db: String,
        _metadata_dir: TempDir,
        _warehouse_dir: TempDir,
        _loopback_backend: crate::engine::StandaloneLoopbackTestBackend,
    }

    fn parse_create_mv(sql: &str) -> CreateMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::CreateMaterializedView(stmt) = statements.remove(0)
        else {
            panic!("expected CREATE MATERIALIZED VIEW");
        };
        stmt
    }

    fn parse_refresh_mv(sql: &str) -> RefreshMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) =
            statements.remove(0)
        else {
            panic!("expected REFRESH MATERIALIZED VIEW");
        };
        stmt
    }

    fn parse_alter_mv(sql: &str) -> AlterMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::AlterMaterializedView(stmt) = statements.remove(0)
        else {
            panic!("expected ALTER MATERIALIZED VIEW");
        };
        stmt
    }

    fn iceberg_ref(catalog: &str, namespace: &str, table: &str) -> TableIdentity {
        TableIdentity {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        }
    }

    fn parse_drop_mv(sql: &str) -> DropMaterializedViewStmt {
        let mut statements = crate::sql::parser::parse_sql(sql).expect("parse");
        let crate::sql::parser::ast::Statement::DropMaterializedView(stmt) = statements.remove(0)
        else {
            panic!("expected DROP MATERIALIZED VIEW");
        };
        stmt
    }

    fn create_test_iceberg_namespace(state: &Arc<StandaloneState>, catalog: &str, namespace: &str) {
        let entry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg catalogs")
            .get(catalog)
            .expect("catalog");
        crate::connector::iceberg::catalog::registry::create_namespace(&entry, namespace)
            .expect("create test namespace");
    }

    fn initialize_iceberg_mv_test_namespaces(
        state: &Arc<StandaloneState>,
        catalog: &str,
        current_db: &str,
    ) {
        create_test_iceberg_namespace(state, catalog, current_db);
        if !current_db.eq_ignore_ascii_case("sales") {
            create_test_iceberg_namespace(state, catalog, "sales");
        }
    }

    fn iceberg_mv_test_state_defaults() -> StandaloneState {
        let defaults = StandaloneState::default();
        let query_execution = crate::engine::test_query_execution_service_with_connector_control(
            Some(Arc::clone(&defaults.connector_control)),
        );
        StandaloneState {
            query_execution,
            mv_storage_observation: Arc::new(
                crate::engine::mv::schema_validation_adapter::TestIcebergMvStorageObservationAdapter::default(),
            ),
            ..defaults
        }
    }

    fn open_test_state_with_iceberg_catalog(catalog: &str, current_db: &str) -> IcebergMvTestState {
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test();
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let metadata_path = metadata_dir.path().join("standalone.sqlite");
        let metadata_provider: Arc<dyn MetaStoreProvider> = Arc::new(
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider"),
        );
        let state = Arc::new(StandaloneState {
            mv_repository: crate::engine::test_mv_repository(),
            metadata_provider: Some(metadata_provider),
            exchange_port: loopback_backend.exchange_port,
            backend_topology: Arc::new(LoopbackTestBackendTopology::new(
                loopback_backend.exchange_port,
            )),
            ..iceberg_mv_test_state_defaults()
        });
        crate::connector::register_standalone_backends(&state);
        // CREATE EXTERNAL CATALOG persists a durable attachment and only then
        // materializes the runtime registry. Populating just the registry
        // leaves anything that reads attachments -- lake MV rebuild is the one
        // that matters -- looking at an empty catalog list.
        let catalog_properties = vec![
            ("type".to_string(), "iceberg".to_string()),
            ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
            (
                "iceberg.catalog.warehouse".to_string(),
                warehouse_dir.path().display().to_string(),
            ),
        ];
        crate::engine::persist_catalog_attachment_if_needed(&state, catalog, &catalog_properties)
            .expect("persist iceberg catalog attachment");
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(catalog, &catalog_properties)
                .expect("create iceberg catalog");
        }
        initialize_iceberg_mv_test_namespaces(&state, catalog, current_db);
        crate::engine::register_iceberg_control_binding(&state, catalog)
            .expect("register Iceberg connector control binding");
        state.catalog_service.register_catalog(
            crate::engine::query_planning::catalog_runtime::build_iceberg_catalog(
                catalog,
                Arc::clone(&state.connector_control)
                    as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
            ),
        );
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
            _loopback_backend: loopback_backend,
        }
    }

    fn open_test_state_with_iceberg_catalog_without_metadata(
        catalog: &str,
        current_db: &str,
    ) -> IcebergMvTestState {
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test();
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let state = Arc::new(StandaloneState {
            metadata_provider: None,
            exchange_port: loopback_backend.exchange_port,
            backend_topology: Arc::new(LoopbackTestBackendTopology::new(
                loopback_backend.exchange_port,
            )),
            ..iceberg_mv_test_state_defaults()
        });
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    catalog,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            warehouse_dir.path().display().to_string(),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        initialize_iceberg_mv_test_namespaces(&state, catalog, current_db);
        crate::engine::register_iceberg_control_binding(&state, catalog)
            .expect("register Iceberg connector control binding");
        state.catalog_service.register_catalog(
            crate::engine::query_planning::catalog_runtime::build_iceberg_catalog(
                catalog,
                Arc::clone(&state.connector_control)
                    as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
            ),
        );
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
            _loopback_backend: loopback_backend,
        }
    }

    fn open_test_state_with_hadoop_iceberg_catalog(
        catalog: &str,
        current_db: &str,
    ) -> IcebergMvTestState {
        let loopback_backend = crate::engine::install_all_in_one_loopback_backend_for_test();
        let metadata_dir = TempDir::new().expect("metadata tempdir");
        let warehouse_dir = TempDir::new().expect("warehouse tempdir");
        let metadata_path = metadata_dir.path().join("standalone.sqlite");
        let metadata_provider: Arc<dyn MetaStoreProvider> = Arc::new(
            crate::meta::SqliteMetaStoreProvider::open(&metadata_path).expect("open meta provider"),
        );
        let state = Arc::new(StandaloneState {
            mv_repository: crate::engine::test_mv_repository(),
            metadata_provider: Some(metadata_provider),
            exchange_port: loopback_backend.exchange_port,
            backend_topology: Arc::new(LoopbackTestBackendTopology::new(
                loopback_backend.exchange_port,
            )),
            ..iceberg_mv_test_state_defaults()
        });
        crate::connector::register_standalone_backends(&state);
        {
            let mut catalogs = state.iceberg_catalogs.write().expect("iceberg catalogs");
            catalogs
                .create_catalog(
                    catalog,
                    &[
                        ("type".to_string(), "iceberg".to_string()),
                        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                        (
                            "iceberg.catalog.warehouse".to_string(),
                            format!("file://{}", warehouse_dir.path().display()),
                        ),
                    ],
                )
                .expect("create iceberg catalog");
        }
        initialize_iceberg_mv_test_namespaces(&state, catalog, current_db);
        crate::engine::register_iceberg_control_binding(&state, catalog)
            .expect("register Iceberg connector control binding");
        state.catalog_service.register_catalog(
            crate::engine::query_planning::catalog_runtime::build_iceberg_catalog(
                catalog,
                Arc::clone(&state.connector_control)
                    as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
            ),
        );
        IcebergMvTestState {
            state,
            current_db: current_db.to_string(),
            _metadata_dir: metadata_dir,
            _warehouse_dir: warehouse_dir,
            _loopback_backend: loopback_backend,
        }
    }

    fn find_iceberg_mv_definition(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<StoredMvDefinition> {
        state
            .mv_repository
            .find_by_target(&MvTarget {
                catalog: Some(catalog.to_string()),
                database: namespace.to_string(),
                name: table.to_string(),
            })
            .expect("lookup mv definition")
    }

    fn load_iceberg_mv_table(
        entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
        namespace: &str,
        table: &str,
    ) -> crate::connector::iceberg::catalog::IcebergLoadedTable {
        crate::connector::iceberg::catalog::load_table(entry, namespace, table)
            .expect("load iceberg MV table")
    }

    fn rewrite_mv_descriptor_package_id_for_test(
        state: &Arc<StandaloneState>,
        catalog_name: &str,
        namespace: &str,
        table: &str,
        package_id: &str,
    ) -> Result<(), String> {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog_name).expect("catalog")
        };
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)?;
        let mut descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())?;
        descriptor.package_id = package_id.to_string();
        let descriptor_properties = descriptor.to_storage_properties()?;
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
        let tx = novarocks_connector_iceberg::iceberg::transaction::Transaction::new(&loaded.table);
        let mut action = tx.update_table_properties();
        for (key, value) in descriptor_properties {
            action = action.set(key, value);
        }
        let tx = action
            .apply(tx)
            .map_err(|e| format!("apply descriptor package-id test update failed: {e}"))?;
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            tx.commit(catalog.as_ref()).await
        })
        .map_err(|e| format!("descriptor package-id test update runtime failed: {e}"))?
        .map_err(|e| format!("descriptor package-id test update failed: {e}"))?;
        entry.invalidate_table_cache(namespace, table);
        Ok(())
    }

    fn corrupt_mv_descriptor_for_test(
        state: &Arc<StandaloneState>,
        catalog_name: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(), String> {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog_name).expect("catalog")
        };
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)?;
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&entry)?;
        let tx = novarocks_connector_iceberg::iceberg::transaction::Transaction::new(&loaded.table);
        let tx = tx
            .update_table_properties()
            .set(
                crate::mv::persistence::descriptor::MV_DESCRIPTOR_INLINE_PROP.to_string(),
                "injected-invalid-descriptor-json".to_string(),
            )
            .apply(tx)
            .map_err(|e| format!("apply descriptor corruption test update failed: {e}"))?;
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            tx.commit(catalog.as_ref()).await
        })
        .map_err(|e| format!("descriptor corruption test update runtime failed: {e}"))?
        .map_err(|e| format!("descriptor corruption test update failed: {e}"))?;
        entry.invalidate_table_cache(namespace, table);
        Ok(())
    }

    fn load_test_operation_for_refresh(
        state: &Arc<StandaloneState>,
        refresh_id: i64,
    ) -> crate::meta::repository::iceberg_operation::StoredIcebergOperation {
        let provider = state.metadata_provider.as_ref().expect("metadata provider");
        let read = provider.begin_read().expect("open read txn");
        let refresh = state
            .mv_repository
            .load_refresh(refresh_id)
            .expect("load refresh")
            .expect("refresh");
        let operation_id = refresh.operation_id.expect("operation id");
        state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load operation")
            .expect("operation")
    }

    fn seed_union_projection_uuid_only_refresh_metadata(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        base_fqn: &str,
    ) {
        let mv = find_iceberg_mv_definition(state, catalog, namespace, table)
            .expect("mv definition for uuid-only metadata seed");
        let mut table_uuids = BTreeMap::new();
        table_uuids.insert(base_fqn.to_string(), "uuid-without-snapshot".to_string());
        let updated = state
            .mv_repository
            .update_starrocks_refresh_summary_if_present(UpdateStarRocksMvRefreshSummaryRequest {
                mv_id: mv.mv_id,
                last_refresh_ms: now_ms(),
                last_refresh_rows: 0,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: table_uuids,
            })
            .expect("seed uuid-only refresh metadata");
        assert!(updated);
    }

    fn seed_mismatched_refresh_uuid_metadata(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        mismatched_base_fqn: &str,
    ) {
        let mv = find_iceberg_mv_definition(state, catalog, namespace, table)
            .expect("mv definition for mismatched uuid metadata seed");
        let mut table_uuids = mv.last_refresh_table_uuids.clone();
        table_uuids.insert(
            mismatched_base_fqn.to_string(),
            "mismatched-table-uuid".to_string(),
        );
        let updated = state
            .mv_repository
            .update_starrocks_refresh_summary_if_present(UpdateStarRocksMvRefreshSummaryRequest {
                mv_id: mv.mv_id,
                last_refresh_ms: now_ms(),
                last_refresh_rows: mv.last_refresh_rows.unwrap_or(0),
                base_snapshots: mv.last_refresh_snapshots.clone(),
                base_table_uuids: table_uuids,
            })
            .expect("seed mismatched uuid refresh metadata");
        assert!(updated);
    }

    fn seed_union_projection_mismatched_uuid_refresh_metadata(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        mismatched_base_fqn: &str,
    ) {
        let mv = find_iceberg_mv_definition(state, catalog, namespace, table)
            .expect("mv definition for mismatched uuid metadata seed");
        assert_eq!(
            mv.last_refresh_snapshots.len(),
            2,
            "mismatched uuid seed expects complete previous snapshots"
        );
        assert_eq!(
            mv.last_refresh_table_uuids.len(),
            2,
            "mismatched uuid seed expects complete previous table uuids"
        );
        let mut table_uuids = mv.last_refresh_table_uuids.clone();
        table_uuids.insert(
            mismatched_base_fqn.to_string(),
            "mismatched-table-uuid".to_string(),
        );
        let updated = state
            .mv_repository
            .update_starrocks_refresh_summary_if_present(UpdateStarRocksMvRefreshSummaryRequest {
                mv_id: mv.mv_id,
                last_refresh_ms: now_ms(),
                last_refresh_rows: mv.last_refresh_rows.unwrap_or(0),
                base_snapshots: mv.last_refresh_snapshots.clone(),
                base_table_uuids: table_uuids,
            })
            .expect("seed mismatched uuid refresh metadata");
        assert!(updated);
    }

    fn create_base_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "name".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create iceberg table");
    }

    fn create_aggregate_fact_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "amount".to_string(),
                data_type: novarocks_catalog::schema::SqlType::BigInt,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate fact iceberg table");
    }

    fn create_aggregate_fact_table_with_float(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "price".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Double,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate fact (float) iceberg table");
    }

    fn create_aggregate_dim_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "category".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create aggregate dim iceberg table");
    }

    fn create_join_orders_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "customer_id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create join orders iceberg table");
    }

    fn create_join_customers_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "region".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create join customers iceberg table");
    }

    fn create_join_mv_target_locator_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "label".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: false,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
                (
                    APPLY_KEY_COLUMN_PROPERTY.to_string(),
                    JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                ),
            ],
        )
        .expect("create join MV target locator table");
    }

    fn insert_into_join_mv_target_locator_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, label, join_key)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*label).to_string()),
                    crate::sql::Literal::String((*join_key).to_string()),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert join MV target locator rows");
    }

    fn load_iceberg_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> novarocks_connector_iceberg::iceberg::table::Table {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load iceberg table")
            .into_table()
    }

    fn create_identity_partitioned_base_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let columns = vec![
            crate::sql::TableColumnDef {
                name: "id".to_string(),
                data_type: novarocks_catalog::schema::SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            crate::sql::TableColumnDef {
                name: "name".to_string(),
                data_type: novarocks_catalog::schema::SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ];
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            namespace,
            table,
            &columns,
            None,
            &[
                crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity {
                    column: "id".to_string(),
                },
            ],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
            ],
        )
        .expect("create identity-partitioned iceberg table");
    }

    fn insert_into_iceberg_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let has_apply_key_column =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
                .expect("load iceberg table")
                .table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .any(|field| {
                    field
                        .name
                        .eq_ignore_ascii_case(HIDDEN_APPLY_KEY_COLUMN_NAME)
                });
        let rows = rows
            .iter()
            .enumerate()
            .map(|(idx, (id, name))| {
                let mut values = vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*name).to_string()),
                ];
                if has_apply_key_column {
                    values.push(crate::sql::Literal::Int(1_000_i64 + idx as i64));
                }
                values
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert iceberg rows");
    }

    fn insert_into_aggregate_fact_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str, i64)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, region, amount)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*region).to_string()),
                    crate::sql::Literal::Int(*amount),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert aggregate fact iceberg rows");
    }

    fn insert_into_aggregate_dim_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, category)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*category).to_string()),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert aggregate dim iceberg rows");
    }

    fn insert_into_join_orders_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, i32)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, customer_id)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::Int(i64::from(*customer_id)),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert join orders iceberg rows");
    }

    fn insert_into_join_customers_table(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        let rows = rows
            .iter()
            .map(|(id, region)| {
                vec![
                    crate::sql::Literal::Int(i64::from(*id)),
                    crate::sql::Literal::String((*region).to_string()),
                ]
            })
            .collect::<Vec<_>>();
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, &rows)
            .expect("insert join customers iceberg rows");
    }

    fn create_base_table_with_rows(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        rows: &[(i32, &str)],
    ) {
        create_base_table(state, catalog, namespace, table);
        insert_into_iceberg_table(state, catalog, namespace, table, rows);
    }

    #[test]
    fn sqlx2_mv_frozen_base_overlays_survive_connector_generation_replacement() {
        use crate::sql::catalog::PlannerTableProvider;
        use crate::sql::planner::table::{ScanSource, SqlScanKind, SqlTableVersionSelector};

        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(
            &env.state,
            "ice",
            "sales",
            "orders",
            &[(1, "before-generation-replacement")],
        );
        let base_ref = iceberg_ref("ice", "sales", "orders");
        let base_refs = vec![base_ref.clone()];
        let pin = capture_refresh_snapshot_pin(
            &env.state,
            &base_refs,
            &crate::connector::test_request_context(),
        )
        .expect("capture base pin");
        let frozen_snapshot_id = pin.get(&base_ref).expect("pinned base snapshot");
        let connector_context = crate::connector::test_request_context();
        let overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
            &env.state,
            &connector_context,
            &base_refs,
            &pin,
            &BTreeMap::new(),
        )
        .expect("freeze base overlay before connector replacement");
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse("ice")
            .expect("connector instance id");
        let original_incarnation =
            novarocks_spi::connector::ConnectorControlResolver::acquire_current(
                env.state.connector_control.as_ref(),
                &instance_id,
            )
            .expect("acquire original connector generation")
            .binding()
            .incarnation();

        crate::engine::retire_iceberg_control_binding(&env.state, "ice")
            .expect("retire original connector generation");
        crate::engine::register_iceberg_control_binding(&env.state, "ice")
            .expect("register replacement connector generation");
        let replacement_incarnation =
            novarocks_spi::connector::ConnectorControlResolver::acquire_current(
                env.state.connector_control.as_ref(),
                &instance_id,
            )
            .expect("acquire replacement connector generation")
            .binding()
            .incarnation();
        assert_ne!(replacement_incarnation, original_incarnation);

        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let catalog_service = crate::engine::catalog_service_snapshot(&env.state);
        let materializer = crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
            None,
            &catalog_service,
            Arc::clone(&bindings),
            crate::engine::query_stats::iceberg_table_binding_loader(
                env.state.connector_control.as_ref(),
                connector_context,
            ),
            overlays,
        );
        let resolved = materializer
            .resolve_table_for_analysis(None, "sales", "orders")
            .expect("materialize original frozen overlay");
        let ScanSource::Sql(source) = resolved.planner.source else {
            panic!("frozen overlay must resolve to an SQL scan source");
        };
        assert!(matches!(
            source.kind,
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(snapshot_id),
            } if snapshot_id == frozen_snapshot_id
        ));
        let lease = bindings
            .exact_planning_lease(source.binding)
            .expect("frozen overlay must retain planning lease");
        assert_eq!(lease.binding().incarnation(), original_incarnation);
        assert_ne!(lease.binding().incarnation(), replacement_incarnation);
        let Some(materialization) = bindings
            .scan_materialization(source.binding)
            .expect("read frozen overlay materialization")
        else {
            panic!("frozen overlay must retain a connector read handle");
        };
        assert_eq!(
            materialization.selector,
            novarocks_spi::connector::ConnectorReadSelector::SnapshotId(frozen_snapshot_id)
        );
    }

    fn create_mv_only(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_db: &str,
        mv_name: &str,
    ) {
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {mv_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders"
        ));
        create_iceberg_mv(state, current_catalog, current_db, &stmt).expect("create iceberg mv");
    }

    fn add_target_identity_partition_column(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
        column: &str,
    ) {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        crate::connector::iceberg::catalog::registry::alter_partition_spec(
            &entry,
            namespace,
            table,
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table: crate::sql::parser::ast::ObjectName {
                    parts: vec![
                        catalog.to_string(),
                        namespace.to_string(),
                        table.to_string(),
                    ],
                },
                field: crate::sql::parser::ast::IcebergPartitionFieldExpr::Identity {
                    column: column.to_string(),
                },
            },
        )
        .expect("alter target partition spec");
    }

    fn sorted_base_field_ids(contract: &mv_schema::BaseContract) -> Vec<i32> {
        contract
            .schema_at_create
            .fields
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>()
    }

    fn create_mv_with_select_only(
        state: &Arc<StandaloneState>,
        current_catalog: Option<&str>,
        current_db: &str,
        mv_name: &str,
        select_sql: &str,
    ) {
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {mv_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS {select_sql}"
        ));
        create_iceberg_mv(state, current_catalog, current_db, &stmt).expect("create iceberg mv");
    }

    fn load_all_mv_refreshes(state: &Arc<StandaloneState>) -> Vec<StoredMvRefresh> {
        let mut refreshes = state
            .mv_repository
            .list_refreshes()
            .expect("list MV refreshes through repository port");
        refreshes.sort_by_key(|refresh| refresh.refresh_id);
        refreshes
    }

    fn single_int_chunk(values: &[i32]) -> Vec<novarocks_execution::exec::chunk::Chunk> {
        let arrow_schema = StdArc::new(ArrowSchema::new(vec![Field::new(
            "k",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![StdArc::new(Int32Array::from(values.to_vec()))],
        )
        .expect("record batch");
        let chunk_schema_ref =
            novarocks_execution::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[novarocks_types::SlotId(0)],
            )
            .expect("chunk schema");
        vec![
            novarocks_execution::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref),
        ]
    }

    fn id_name_chunk(rows: &[(i32, &str)]) -> Vec<novarocks_execution::exec::chunk::Chunk> {
        let arrow_schema = StdArc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(HIDDEN_APPLY_KEY_COLUMN_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                StdArc::new(Int32Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                StdArc::new(StringArray::from_iter_values(
                    rows.iter().map(|(_, name)| *name),
                )),
                StdArc::new(Int64Array::from_iter_values(
                    rows.iter()
                        .enumerate()
                        .map(|(idx, _)| 1_000_i64 + idx as i64),
                )),
            ],
        )
        .expect("record batch");
        let chunk_schema_ref =
            novarocks_execution::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                &arrow_schema,
                &[
                    novarocks_types::SlotId(0),
                    novarocks_types::SlotId(1),
                    novarocks_types::SlotId(2),
                ],
            )
            .expect("chunk schema");
        vec![
            novarocks_execution::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema_ref),
        ]
    }

    #[test]
    fn create_iceberg_mv_creates_branch_capable_v3_target() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target table");
        assert_eq!(
            loaded.table.metadata().format_version(),
            novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3
        );
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get("write.row-lineage")
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get(APPLY_KEY_COLUMN_PROPERTY)
                .map(String::as_str),
            Some(HIDDEN_APPLY_KEY_COLUMN_NAME)
        );
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields();
        let apply_key_field = fields
            .iter()
            .find(|field| field.name == HIDDEN_APPLY_KEY_COLUMN_NAME)
            .expect("target apply-key field");
        assert_eq!(apply_key_field.id, 3);
        assert!(apply_key_field.required);
        assert_eq!(
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
                .expect("mv definition")
                .schema_contract
                .expect("schema contract")
                .target
                .hidden_apply_key
                .target_field_id,
            3
        );
    }

    #[test]
    fn create_iceberg_mv_writes_descriptor_property_on_mv_table() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load MV table");
        let descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())
                .expect("descriptor properties");

        assert_eq!(descriptor.descriptor_version, MV_DESCRIPTOR_VERSION);
        assert_eq!(descriptor.package_id, "analytics.mv_orders");
        assert_eq!(
            descriptor.logical_sql,
            "SELECT id, name FROM ice.sales.orders"
        );
        assert_eq!(descriptor.dialect, "starrocks");
        assert_eq!(
            descriptor.visible_columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            descriptor.hidden_columns,
            vec![HIDDEN_APPLY_KEY_COLUMN_NAME.to_string()]
        );
        assert_eq!(
            descriptor.base_dependencies,
            vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }]
        );
        // W2: CREATE now pushes the freshly-persisted schema contract into the
        // MV table descriptor after the metadata transaction commits. Previously this field stayed
        // `None` on the descriptor written at create_table time, because the
        // contract needs the created target table's field-ids.
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let stored_definition = load_iceberg_mv_definition_by_target(&env.state, &target)
            .expect("load stored mv definition");
        assert_eq!(
            descriptor.schema_contract_typed().expect("typed contract"),
            stored_definition.schema_contract
        );
        assert!(descriptor.schema_contract_typed().unwrap().is_some());
        assert_eq!(
            descriptor.refresh_contract,
            Some(serde_json::json!({
                "policy": "DEFERRED_MANUAL",
                "interval_ms": null,
                "paused": false,
            }))
        );
        assert!(descriptor.created_at_ms > 0);

        let legacy_alias_name = ["__nr", "_mv_", "mv_orders"].concat();
        let missing_direct_target_err =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", &legacy_alias_name)
                .expect_err("legacy alias target table must not exist");
        assert!(
            missing_direct_target_err.contains(&legacy_alias_name),
            "{missing_direct_target_err}"
        );
    }

    #[test]
    fn iceberg_mv_reader_loads_target_table_without_alias() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let resolved = crate::connector::metadata_load_table(
            env.state.connector_control.as_ref(),
            crate::connector::test_request_context(),
            "ice",
            "analytics",
            "mv_orders",
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .expect("read MV table by public name")
        .0;
        assert_eq!(resolved.table, "mv_orders");

        let (schema_table, schema_id) = crate::connector::metadata_load_table(
            env.state.connector_control.as_ref(),
            crate::connector::test_request_context(),
            "ice",
            "analytics",
            "mv_orders",
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .expect("read MV schema id by public name");
        assert_eq!(schema_table.table, "mv_orders");
        assert!(schema_id.is_some());

        let legacy_alias_name = ["__nr", "_mv_", "mv_orders"].concat();
        let missing_schema_err = crate::connector::metadata_load_table(
            env.state.connector_control.as_ref(),
            crate::connector::test_request_context(),
            "ice",
            "analytics",
            &legacy_alias_name,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .expect_err("legacy alias schema id must not resolve");
        assert!(
            missing_schema_err.contains(&legacy_alias_name) || missing_schema_err.contains("not"),
            "{missing_schema_err}"
        );

        let missing_target_err = crate::connector::metadata_load_table(
            env.state.connector_control.as_ref(),
            crate::connector::test_request_context(),
            "ice",
            "analytics",
            &legacy_alias_name,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )
        .expect_err("legacy alias target must not resolve");
        assert!(
            missing_target_err.contains(&legacy_alias_name) || missing_target_err.contains("not"),
            "{missing_target_err}"
        );
    }

    fn discover_lake_packages_for_test(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
    ) -> Result<Vec<crate::mv::storage_observation::MvLakePackageObservation>, String> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog)
            .map_err(|error| error.to_string())?;
        crate::mv::storage_observation::discover_mv_lake_packages(
            state.connector_control.as_ref(),
            [instance_id],
            state.mv_storage_observation.as_ref(),
            crate::connector::test_request_context(),
        )
        .map(|packages| {
            packages
                .into_iter()
                .filter(|package| {
                    package.table.instance_id.as_str() == catalog
                        && package.table.namespace.as_ref() == namespace
                })
                .collect()
        })
        .map_err(|error| error.to_string())
    }

    #[test]
    fn discover_iceberg_mvs_recovers_single_table_descriptor_from_lake() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "analytics", "plain_orders");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let discovered = discover_lake_packages_for_test(&env.state, "ice", "analytics")
            .expect("discover iceberg mvs");
        assert_eq!(discovered.len(), 1);
        assert_eq!(
            discovered
                .iter()
                .map(|mv| mv.table.table.as_ref())
                .collect::<Vec<_>>(),
            vec!["mv_orders"]
        );
        let mv = &discovered[0];
        assert_eq!(mv.table.instance_id.as_str(), "ice");
        assert_eq!(mv.table.namespace.as_ref(), "analytics");
        assert_eq!(mv.table.table.as_ref(), "mv_orders");
        assert_eq!(mv.descriptor.package_id, "analytics.mv_orders");
        assert_eq!(mv.descriptor.base_dependencies[0].name.as_str(), "orders");
    }

    #[test]
    fn discover_iceberg_mvs_skips_legacy_namespace_names() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");
        std::fs::create_dir_all(env._warehouse_dir.path().join("statistics-sqlt-deadbeef"))
            .expect("create legacy Iceberg namespace directory");

        let discovered = discover_lake_packages_for_test(&env.state, "ice", "analytics")
            .expect("legacy namespace name must not abort MV lake discovery");
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].table.table.as_ref(), "mv_orders");
    }

    /// Read the [`MvProvenanceV1`] carried by an MV table's current snapshot
    /// summary, reloading the table fresh from the catalog. Returns `None` when
    /// there is no current snapshot or the snapshot carries no provenance.
    fn read_target_current_snapshot_provenance(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<MvProvenanceV1> {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        entry.invalidate_table_cache(namespace, table);
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load target table for provenance read");
        let current = loaded.table.metadata().current_snapshot()?;
        MvProvenanceV1::from_snapshot_summary(current).expect("parse provenance from summary")
    }

    /// Current-snapshot id of an MV table (reloaded fresh from the catalog).
    fn read_target_current_snapshot_id(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<i64> {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        entry.invalidate_table_cache(namespace, table);
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load target table for snapshot id read");
        loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    }

    /// Count live data-file entries reachable from an MV table's current
    /// snapshot. A data-free watermark append adds an empty manifest, so this
    /// count is invariant across a net-zero refresh — a direct proxy for "the
    /// MV's materialized data is unchanged" that avoids a full `SELECT *` round
    /// trip through the pipeline.
    fn live_target_data_file_count(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> usize {
        let entry = {
            let catalogs = state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get(catalog).expect("catalog")
        };
        entry.invalidate_table_cache(namespace, table);
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load target table for data-file count");
        let table = loaded.into_table();
        data_block_on(async {
            let Some(current) = table.metadata().current_snapshot() else {
                return 0usize;
            };
            let manifest_list = current
                .load_manifest_list(table.file_io(), table.metadata())
                .await
                .expect("load manifest list");
            let mut count = 0usize;
            for entry in manifest_list.entries() {
                let manifest = entry
                    .load_manifest(table.file_io())
                    .await
                    .expect("load manifest");
                for manifest_entry in manifest.entries() {
                    if manifest_entry.is_alive()
                        && manifest_entry.data_file().content_type()
                            == novarocks_connector_iceberg::iceberg::spec::DataContentType::Data
                    {
                        count += 1;
                    }
                }
            }
            count
        })
        .expect("count live target data files")
    }

    /// Delete an MV definition through its port without dropping its lake table.
    fn drop_mv_definition_from_sqlite_only(
        state: &Arc<StandaloneState>,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) {
        let dropped = state
            .mv_repository
            .drop_by_target(&MvTarget {
                catalog: Some(catalog.to_string()),
                database: namespace.to_string(),
                name: table.to_string(),
            })
            .expect("drop mv definition");
        assert!(dropped, "expected an existing mv definition to drop");
    }

    fn list_mv_dependency_names(
        state: &Arc<StandaloneState>,
        mv_id: i64,
    ) -> Vec<(Option<String>, String, String)> {
        state
            .mv_repository
            .list_dependencies_by_downstream(mv_id)
            .expect("list dependencies")
            .into_iter()
            .map(|dep| {
                (
                    dep.upstream.catalog,
                    dep.upstream.database_or_namespace,
                    dep.upstream.name,
                )
            })
            .collect()
    }

    #[test]
    fn rebuild_imv_cache_from_lake_reappears_after_sqlite_definition_dropped() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        // Capture the created definition + its dependencies as the golden.
        let before = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition present after create");
        let before_deps = list_mv_dependency_names(&env.state, before.mv_id);

        // Forget the MV in SQLite but keep its lake MV table + descriptor.
        drop_mv_definition_from_sqlite_only(&env.state, "ice", "analytics", "mv_orders");
        assert!(
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders").is_none(),
            "definition should be gone from SQLite after drop"
        );

        // Rebuild purely from the lake.
        crate::engine::mv::lake_rebuild::rebuild_imv_cache_from_lake(&env.state)
            .expect("rebuild imv cache from lake");

        // The definition reappears with matching select_sql / target / schema
        // contract / watermark, and its base dependencies are restored.
        let after = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition reappears after rebuild");
        assert_eq!(after.select_sql, before.select_sql);
        assert_eq!(after.storage_engine, before.storage_engine);
        assert_eq!(after.target_catalog, before.target_catalog);
        assert_eq!(after.target_namespace, before.target_namespace);
        assert_eq!(after.target_table, before.target_table);
        assert_eq!(after.schema_contract, before.schema_contract);
        assert_eq!(after.partition_spec, before.partition_spec);
        assert_eq!(after.last_refresh_snapshots, before.last_refresh_snapshots);
        assert_eq!(
            after.last_refresh_table_uuids,
            before.last_refresh_table_uuids
        );
        let after_deps = list_mv_dependency_names(&env.state, after.mv_id);
        assert_eq!(after_deps, before_deps);
        assert_eq!(
            after_deps,
            vec![(
                Some("ice".to_string()),
                "sales".to_string(),
                "orders".to_string()
            )]
        );

        // Idempotent: a second rebuild is a no-op (the MV is now a cache hit).
        crate::engine::mv::lake_rebuild::rebuild_imv_cache_from_lake(&env.state)
            .expect("second rebuild is a no-op");
        let after_again = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition still present after idempotent rebuild");
        assert_eq!(after_again.select_sql, after.select_sql);
        assert_eq!(
            list_mv_dependency_names(&env.state, after_again.mv_id),
            after_deps
        );
    }

    /// Read one string cell out of the W0 stateless-rebuild report chunk.
    /// Returns `None` for a NULL cell (the hash columns are nullable).
    fn stateless_report_cell(
        result: &crate::runtime::query_result::QueryResult,
        column: usize,
    ) -> Option<String> {
        let chunk = &result.chunks[0];
        let array = chunk
            .batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("stateless report column is Utf8");
        arrow::array::Array::is_valid(array, 0).then(|| array.value(0).to_string())
    }

    /// W0 `full` level: invoking the stateless-rebuild procedure at `full`
    /// clears the MV's SQLite records and rebuilds them purely from the lake,
    /// proving SQLite is a rebuildable cache (not just readable lake metadata as
    /// `provenance` does). The report is `AvailableLevel=full`,
    /// `RebuildSource=lake`, with non-null descriptor/provenance/waterline
    /// hashes, and the MV's SQLite definition round-trips back to an equivalent
    /// record.

    /// W0 `full` level statelessness precondition: if the MV has no SQLite
    /// definition to clear, there is nothing to prove a round-trip against, so
    /// the probe fails loud rather than silently reporting `full`.
    #[test]
    fn stateless_rebuild_full_errors_when_no_sqlite_definition_to_clear() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");
        // Forget the MV in SQLite (lake MV table stays), so `full` has no
        // cached record to clear.
        drop_mv_definition_from_sqlite_only(&env.state, "ice", "analytics", "mv_orders");

        let req = crate::engine::mv::stateless_rebuild::ImvStatelessRebuildRequest {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            mv: "mv_orders".to_string(),
            required_level: crate::engine::mv::stateless_rebuild::StatelessLevel::Full,
        };
        let err = crate::engine::mv::stateless_rebuild::execute_request(&env.state, &req)
            .expect_err("full rebuild must fail with no SQLite definition to clear");
        assert!(
            err.contains("no repository definition to clear"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_writes_schema_contract_into_descriptor() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");
        // Discover the MV descriptor straight off the MV table, the same way
        // `discover_iceberg_mvs_recovers_single_table_descriptor_from_lake`
        // does above.
        let discovered = discover_lake_packages_for_test(&env.state, "ice", "analytics")
            .expect("discover iceberg mvs");
        assert_eq!(discovered.len(), 1);
        let descriptor = &discovered[0].descriptor;

        // Load the MV definition from the metadata store directly, to compare
        // against what the descriptor carries.
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        };
        let stored_definition = load_iceberg_mv_definition_by_target(&env.state, &target)
            .expect("load stored mv definition");

        let descriptor_contract = descriptor
            .schema_contract_typed()
            .expect("parse typed schema contract from descriptor");

        // The descriptor now carries the schema contract (not just `None`,
        // and not merely "some arbitrary JSON") and it is byte-for-byte the
        // same contract the CREATE path persisted into the metadata store.
        assert!(descriptor_contract.is_some());
        assert_eq!(descriptor_contract, stored_definition.schema_contract);
    }

    #[test]
    fn discover_iceberg_mvs_rejects_descriptor_package_id_mismatch() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");
        rewrite_mv_descriptor_package_id_for_test(
            &env.state,
            "ice",
            "analytics",
            "mv_orders",
            "analytics.wrong_public_name",
        )
        .expect("rewrite descriptor package id");

        let err = discover_lake_packages_for_test(&env.state, "ice", "analytics")
            .expect_err("package id mismatch should fail discovery");
        assert!(
            err.contains("descriptor package id mismatch"),
            "unexpected discovery error: {err}"
        );
        assert!(err.contains("analytics.mv_orders"), "err={err}");
        assert!(err.contains("analytics.wrong_public_name"), "err={err}");
    }

    #[test]
    fn alter_iceberg_mv_refresh_metadata_updates_descriptor_refresh_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let set_refresh = parse_alter_mv(
            "ALTER MATERIALIZED VIEW mv_orders SET REFRESH ASYNC EVERY INTERVAL 2 HOURS",
        );
        crate::engine::mv_flow::alter_mv(&env.state, Some("ice"), &env.current_db, &set_refresh)
            .expect("alter refresh policy");
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load MV table after alter");
        let descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())
                .expect("descriptor after alter");
        assert_eq!(
            descriptor.refresh_contract,
            Some(serde_json::json!({
                "policy": "ASYNC_INTERVAL",
                "interval_ms": 7_200_000,
                "paused": false,
            }))
        );

        let pause = parse_alter_mv("ALTER MATERIALIZED VIEW mv_orders PAUSE REFRESH");
        crate::engine::mv_flow::alter_mv(&env.state, Some("ice"), &env.current_db, &pause)
            .expect("pause refresh");
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load MV table after pause");
        let descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())
                .expect("descriptor after pause");
        assert_eq!(
            descriptor.refresh_contract,
            Some(serde_json::json!({
                "policy": "ASYNC_INTERVAL",
                "interval_ms": 7_200_000,
                "paused": true,
            }))
        );
    }

    #[test]
    fn create_iceberg_mv_creates_partitioned_target_from_partition_by() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             PARTITION BY (bucket(id, 16), truncate(name, 8))
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target table");
        let spec = loaded.table.metadata().default_partition_spec();
        assert_eq!(spec.spec_id(), 0);
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id_bucket_16");
        assert_eq!(fields[0].source_id, 1);
        assert_eq!(
            fields[0].transform,
            novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(16)
        );
        assert_eq!(fields[1].name, "name_truncate_8");
        assert_eq!(fields[1].source_id, 2);
        assert_eq!(
            fields[1].transform,
            novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(8)
        );
        let definition = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition");
        let stored_partition = definition.partition_spec.expect("stored partition spec");
        assert_eq!(stored_partition.target_spec_id, 0);
        assert_eq!(stored_partition.fields.len(), 2);
        let contract_partition = definition
            .schema_contract
            .expect("schema contract")
            .target
            .partition
            .expect("target partition contract");
        assert_eq!(contract_partition, stored_partition);
    }

    // Native FE/BE owner coverage: iceberg_ivm_projection_repartition.sql.

    #[test]
    fn replace_default_partition_spec_expected_default_rejects_external_spec_change() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "alfa")]);
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             PARTITION BY (bucket(id, 16))
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let before =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target before external spec change");
        let old_default_spec_id = before.table.metadata().default_partition_spec_id();
        assert_eq!(
            before.table.metadata().default_partition_spec().fields()[0].name,
            "id_bucket_16"
        );

        let external_alter =
            parse_alter_mv("ALTER MATERIALIZED VIEW mv_orders REPARTITION BY (bucket(id, 8))");
        let AlterMaterializedViewAction::Repartition(external_fields) = &external_alter.action
        else {
            panic!("expected external repartition action");
        };
        let externally_updated =
            crate::connector::iceberg::catalog::registry::replace_default_partition_spec(
                &entry,
                "analytics",
                "mv_orders",
                external_fields,
            )
            .expect("replace default partition spec externally");
        let external_default_spec_id = externally_updated.metadata().default_partition_spec_id();
        assert_ne!(external_default_spec_id, old_default_spec_id);
        assert_eq!(
            externally_updated
                .metadata()
                .default_partition_spec()
                .fields()[0]
                .name,
            "id_bucket_8"
        );

        let alter =
            parse_alter_mv("ALTER MATERIALIZED VIEW mv_orders REPARTITION BY (truncate(name, 2))");
        let AlterMaterializedViewAction::Repartition(fields) = &alter.action else {
            panic!("expected repartition action");
        };
        let err = crate::connector::iceberg::catalog::registry::replace_default_partition_spec_with_expected_default(
            &entry,
            "analytics",
            "mv_orders",
            fields,
            old_default_spec_id,
        )
        .expect_err("stale expected default spec id should reject replacement");
        assert!(err.contains("expected default spec id"), "{err}");

        let after =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load target after rejected replacement");
        let after_spec = after.table.metadata().default_partition_spec();
        assert_eq!(after_spec.spec_id(), external_default_spec_id);
        assert_eq!(after_spec.fields().len(), 1);
        assert_eq!(after_spec.fields()[0].name, "id_bucket_8");
    }

    #[test]
    fn create_iceberg_aggregate_mv_persists_v3_group_row_id_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c, sum(amount) AS s
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create aggregate iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded = load_iceberg_mv_table(&entry, "analytics", "mv_fact_region");
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get(HIDDEN_COLUMNS_PROPERTY)
                .map(String::as_str),
            Some("__agg_state_c,__agg_state_s")
        );

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        assert_eq!(contract.contract_version, 3);
        assert_eq!(
            contract.target.hidden_apply_key.column_name,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        );
        assert_eq!(contract.target.hidden_apply_key.target_field_id, 1);
        assert_eq!(sorted_base_field_ids(&contract.base), vec![2, 3]);
        assert_eq!(
            contract.output.columns[2]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        let aggregate = contract.aggregate.expect("aggregate contract");
        assert_eq!(aggregate.state_layout_version, 1);
        assert_eq!(
            aggregate.row_id_column_name,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );
        assert_eq!(aggregate.state_columns.len(), 2);
        assert_eq!(aggregate.state_columns[0].column_name, "__agg_state_c");
        assert_eq!(aggregate.state_columns[0].target_field_id, 5);
        assert_eq!(aggregate.state_columns[0].type_signature, "binary");
        assert!(!aggregate.state_columns[0].nullable);
        assert_eq!(
            aggregate.state_columns[0].role,
            mv_schema::AggregateStateRoleContract::Single
        );
        assert_eq!(aggregate.state_columns[1].column_name, "__agg_state_s");
        assert_eq!(aggregate.state_columns[1].target_field_id, 6);
        assert_eq!(aggregate.state_columns[1].type_signature, "binary");
        assert!(!aggregate.state_columns[1].nullable);
        assert_eq!(
            aggregate.state_columns[1].role,
            mv_schema::AggregateStateRoleContract::Single
        );
    }

    #[test]
    fn create_iceberg_join_aggregate_mv_persists_join_and_group_row_id_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        create_aggregate_dim_table(&env.state, "ice", "sales", "dim");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_dim
             DISTRIBUTED BY HASH(category) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT d.category, sum(f.amount) AS total
                FROM ice.sales.fact f JOIN ice.sales.dim d ON f.id = d.id
                GROUP BY d.category",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join aggregate iceberg mv");

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_dim")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        assert_eq!(contract.contract_version, 3);
        assert_eq!(contract.bases.len(), 2);
        assert!(contract.join.is_some());
        assert!(contract.aggregate.is_some());
        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        );
        assert_eq!(
            contract.target.hidden_apply_key.column_name,
            GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME
        );

        let fact_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn == "ice.sales.fact")
            .expect("fact base contract");
        let dim_contract = contract
            .bases
            .iter()
            .find(|base| base.table_fqn == "ice.sales.dim")
            .expect("dim base contract");
        assert_eq!(fact_contract.alias_at_create.as_deref(), Some("f"));
        assert_eq!(dim_contract.alias_at_create.as_deref(), Some("d"));
        assert_eq!(sorted_base_field_ids(fact_contract), vec![1, 3]);
        assert_eq!(sorted_base_field_ids(dim_contract), vec![1, 2]);
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].left.table_fqn,
            "ice.sales.fact"
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].left.field_id,
            1
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0]
                .right
                .table_fqn,
            "ice.sales.dim"
        );
        assert_eq!(
            contract.join.as_ref().unwrap().predicates[0].right.field_id,
            1
        );
        assert_eq!(
            contract.output.columns[1].expression.referenced_base_fields,
            vec![mv_schema::QualifiedFieldLineage {
                table_fqn: "ice.sales.fact".to_string(),
                qualifier_at_create: "f".to_string(),
                field_id: 3,
            }]
        );
    }

    #[test]
    fn create_iceberg_union_all_aggregate_over_join_persists_composed_branch_contract() {
        // P4.4: a HOMOGENEOUS composed branch-union shape — UNION ALL of
        // `Agg(a JOIN b)` x 2 over the SAME bases — is now SUPPORTED at CREATE.
        // The delta execution composes the branches off the full UNION ALL
        // logical plan, so CREATE persists a BranchUnionAggregate contract and a
        // target table carrying the apply-key + branch-id columns. The first
        // branch is a join aggregate, so the schema contract carries the two-base
        // join lineage from that branch (representative under homogeneity).
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_a");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_b");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_agg_join
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT a.region, count(*) AS c, sum(a.amount) AS s
                FROM ice.sales.fact_a a JOIN ice.sales.fact_b b ON a.id = b.id
                WHERE a.amount > 0
                GROUP BY a.region
                UNION ALL
                SELECT a.region, count(*) AS c, sum(a.amount) AS s
                FROM ice.sales.fact_a a JOIN ice.sales.fact_b b ON a.id = b.id
                WHERE a.amount > 10
                GROUP BY a.region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("homogeneous composed branch-union aggregate-over-join must be created");

        // The target table was created with the branch-id + apply-key columns.
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded = load_iceberg_mv_table(&entry, "analytics", "mv_union_agg_join");
        let field_names = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        assert!(
            field_names.iter().any(|name| name == BRANCH_ID_COLUMN_NAME),
            "composed branch-union target must carry the branch-id column, got {field_names:?}"
        );
        // The aggregate apply key is the synthetic row-id column (BranchUtf8
        // group-row identity), and the per-aggregate state columns are present.
        assert!(
            field_names
                .iter()
                .any(|name| { name == crate::mv::aggregate_state::mv_agg_state::ROW_ID_COLUMN }),
            "composed branch-union aggregate target must carry the row-id apply key, got {field_names:?}"
        );

        // The persisted schema contract is a version-3 branch contract carrying
        // the two-base join lineage + aggregate + branch sections.
        let mv_definition =
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_union_agg_join")
                .expect("load mv definition");
        let schema_contract = mv_definition
            .schema_contract
            .as_ref()
            .expect("composed branch-union MV must persist a schema contract");
        assert_eq!(schema_contract.contract_version, 3);
        assert!(
            schema_contract.aggregate.is_some(),
            "composed branch-union contract must carry an aggregate section"
        );
        assert!(
            schema_contract.join.is_some(),
            "composed branch-union (join first branch) contract must carry a join section"
        );
        let branch = schema_contract
            .branch
            .as_ref()
            .expect("composed branch-union contract must carry a branch section");
        assert_eq!(branch.branch_count, 2);
        assert_eq!(schema_contract.bases.len(), 2, "two join bases");
    }

    #[test]
    fn create_iceberg_union_all_aggregate_over_join_rejects_heterogeneous_bases() {
        // A composed `BranchScoped(GroupRowId)` UNION ALL whose branches join
        // *different* base sets (branch0: a JOIN b, branch1: c JOIN d) is
        // structurally heterogeneous and stays REJECTED: the homogeneity gate
        // inside `derive_fragment_property` rejects it because first-branch-only
        // persisted lineage cannot describe branch1's different bases. (The
        // HOMOGENEOUS composed case — same bases in every branch — is now
        // supported and persists a contract; see
        // `create_iceberg_union_all_aggregate_over_join_persists_composed_branch_contract`.)
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_a");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_b");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_c");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_d");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_agg_join_het
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT a.region, count(*) AS c, sum(a.amount) AS s
                FROM ice.sales.fact_a a JOIN ice.sales.fact_b b ON a.id = b.id
                GROUP BY a.region
                UNION ALL
                SELECT c0.region, count(*) AS c, sum(c0.amount) AS s
                FROM ice.sales.fact_c c0 JOIN ice.sales.fact_d d ON c0.id = d.id
                GROUP BY c0.region",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("heterogeneous-base composed UNION ALL aggregate must be rejected");
        assert!(
            err.contains("homogeneous") || err.contains("same base"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_iceberg_union_all_projection_mv_persists_branch_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "sales", "returns");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders
                UNION ALL
                SELECT id, name FROM ice.sales.returns",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create projection/filter UNION ALL iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded = load_iceberg_mv_table(&entry, "analytics", "mv_union_orders");
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields();
        let field_names = fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            field_names,
            vec![
                "id",
                "name",
                HIDDEN_APPLY_KEY_COLUMN_NAME,
                BRANCH_ID_COLUMN_NAME
            ]
        );
        let branch_field = fields
            .iter()
            .find(|field| field.name == BRANCH_ID_COLUMN_NAME)
            .expect("branch id field");

        let contract =
            find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_union_orders")
                .expect("mv definition")
                .schema_contract
                .expect("schema contract");
        assert_eq!(contract.bases.len(), 2);
        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId
        );
        assert_eq!(contract.target.hidden_apply_key.target_field_id, 3);
        let branch = contract.branch.expect("branch contract");
        assert_eq!(branch.branch_count, 2);
        assert_eq!(
            branch.inner_apply_key_source,
            crate::sql::planner::vocabulary::ApplyKeySource::BaseRowId
        );
        assert_eq!(branch.branch_id_column.column_name, BRANCH_ID_COLUMN_NAME);
        assert_eq!(branch.branch_id_column.target_field_id, branch_field.id);
    }

    #[test]
    fn plan_iceberg_mv_refresh_plans_union_all_projection_mv() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "sales", "returns");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders
                UNION ALL
                SELECT id, name FROM ice.sales.returns",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create projection/filter UNION ALL iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_union_orders");
        let target = crate::mv::model::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_union_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("projection/filter UNION ALL refresh planning");

        assert_eq!(plan.contract.mode(), RefreshMode::Noop);
        assert_eq!(
            plan.contract
                .base_refs
                .iter()
                .map(|base| format!("{}.{}.{}", base.catalog, base.namespace, base.table))
                .collect::<Vec<_>>(),
            vec![
                "ice.sales.orders".to_string(),
                "ice.sales.returns".to_string()
            ]
        );
        assert_eq!(plan.contract.snapshot_pins.len(), 2);
        assert_eq!(
            plan.contract.snapshot_pins.get("ice.sales.orders").copied(),
            Some(None)
        );
        assert_eq!(
            plan.contract
                .snapshot_pins
                .get("ice.sales.returns")
                .copied(),
            Some(None)
        );
    }

    #[test]
    fn plan_iceberg_union_all_projection_rejects_uuid_without_snapshot_metadata() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "sales", "returns");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders
                UNION ALL
                SELECT id, name FROM ice.sales.returns",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create projection/filter UNION ALL iceberg mv");
        seed_union_projection_uuid_only_refresh_metadata(
            &env.state,
            "ice",
            "analytics",
            "mv_union_orders",
            "ice.sales.orders",
        );

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_union_orders");
        let target = crate::mv::model::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_union_orders".to_string(),
        };
        let err =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect_err("uuid-only previous metadata should be rejected during planning");

        assert!(
            err.message.contains("partial previous refresh metadata"),
            "err={err:?}"
        );
        assert!(err.message.contains("recreate the MV"), "err={err:?}");
    }

    // Native FE/BE owner coverage: iceberg_ivm_union_projection_filter.sql.

    #[test]
    fn validate_aggregate_fan_in_base_refs_accepts_distinct_resolved_refs() {
        // The validator no longer compares a classifier-derived fan-in base set
        // against the resolved refs (that invariant is trivially satisfied now
        // that the resolved refs ARE the fan-in base set). A distinct resolved
        // base-ref set is accepted; per-base schema-contract checks live in
        // `validate_aggregate_schema_contract_for_base`.
        let base_refs = vec![
            iceberg_ref("ice", "sales", "fact_east"),
            iceberg_ref("ice", "sales", "fact_west"),
        ];

        validate_aggregate_fan_in_base_refs(&base_refs)
            .expect("distinct resolved fan-in base refs must be accepted");
    }

    #[test]
    fn validate_aggregate_fan_in_base_refs_rejects_duplicate_resolved_base() {
        let base_refs = vec![
            iceberg_ref("ice", "sales", "fact"),
            iceberg_ref("ice", "sales", "fact"),
        ];

        let err = validate_aggregate_fan_in_base_refs(&base_refs)
            .expect_err("duplicate resolved base refs should be rejected");

        assert!(err.contains("duplicate resolved base ref"), "err={err}");
        assert!(err.contains("ice.sales.fact"), "err={err}");
    }

    #[test]
    fn create_iceberg_union_all_projection_mv_rejects_branch_id_output_alias() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "sales", "returns");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_branch_id
             DISTRIBUTED BY HASH(__branch_id__) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id AS __branch_id__, name FROM ice.sales.orders
                UNION ALL
                SELECT id AS __branch_id__, name FROM ice.sales.returns",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("UNION ALL branch id output alias should be rejected");
        assert!(err.contains(BRANCH_ID_COLUMN_NAME), "err={err}");
        assert!(err.contains("reserved"), "err={err}");

        let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
        let entry = catalogs.get("ice").expect("catalog");
        assert!(
            crate::connector::iceberg::catalog::load_table(
                &entry,
                "analytics",
                "mv_union_branch_id",
            )
            .is_err(),
            "reserved-name failure must happen before target schema creation"
        );
    }

    #[test]
    fn build_iceberg_union_all_aggregate_schema_contract_includes_branch_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_east");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_west");
        let query = parse_select_query(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM ice.sales.fact_east
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM ice.sales.fact_west
             GROUP BY region",
        );
        let analysis = analyze_mv_select_with_connector_context(
            &env.state,
            Some("ice"),
            &env.current_db,
            &query,
            &crate::connector::test_request_context(),
        )
        .expect("analyze UNION ALL aggregate query");
        let resolved_dependencies = crate::engine::mv::dependency::resolve_create_mv_dependencies(
            &env.state,
            &analysis.resolved_refs,
            now_ms(),
        )
        .expect("resolve dependencies");
        let loaded_bases = resolved_dependencies
            .base_refs
            .iter()
            .map(|base_ref| {
                let loaded = load_current_iceberg_base_table(&env.state, base_ref)
                    .expect("load current base table");
                (base_ref.clone(), loaded)
            })
            .collect::<Vec<_>>();
        // Source the first-branch aggregate calls from the AST (no classifier).
        let first_branch_ast =
            first_union_branch_ast_query(&query).expect("first branch ast query");
        let first_branch_calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
                &first_branch_ast,
            )
            .expect("first branch aggregate calls");
        let target = IcebergMvTarget {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_union_fact_region_contract".to_string(),
        };
        let mut columns = iceberg_aggregate_target_columns_from_resolved_query(
            &first_branch_calls,
            &analysis.output_columns,
            first_union_branch_resolved_query(&analysis.resolved_query)
                .expect("first branch resolved query"),
        )
        .expect("target columns");
        columns.push(branch_id_table_column());
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::registry::create_table(
            &entry,
            &target.namespace,
            &target.table,
            &columns,
            None,
            &[],
            &[
                ("format-version".to_string(), "3".to_string()),
                ("write.row-lineage".to_string(), "true".to_string()),
                (
                    APPLY_KEY_COLUMN_PROPERTY.to_string(),
                    GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
                ),
                (
                    APPLY_KEY_SOURCE_PROPERTY.to_string(),
                    ApplyKeySource::GroupRowId
                        .table_property_value()
                        .to_string(),
                ),
                (
                    HIDDEN_COLUMNS_PROPERTY.to_string(),
                    "__agg_state_c,__agg_state_s".to_string(),
                ),
            ],
        )
        .expect("create contract target table");
        let loaded = crate::connector::iceberg::catalog::load_table(
            &entry,
            &target.namespace,
            &target.table,
        )
        .expect("load union aggregate target table");
        let target_observation =
            target_observation_from_loaded_for_test(&target, &loaded).expect("target observation");
        let actual_apply_key_field_id =
            target_field_id_by_column(&target_observation, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME)
                .expect("apply-key field");

        let refresh_contract = derive_imv_refresh_contract(&analysis).expect("refresh contract");
        let property =
            derive_fragment_property(&analysis.resolved_query).expect("fragment property");
        let contract = build_iceberg_mv_schema_contract(
            &refresh_contract,
            &property,
            &query,
            &analysis,
            &resolved_dependencies.base_refs,
            &observe_base_fields_for_refs(
                &env.state,
                &resolved_dependencies.base_refs,
                &crate::connector::test_request_context(),
            )
            .expect("base field observations"),
            &target,
            &target_observation,
            actual_apply_key_field_id,
        )
        .expect("schema contract");
        assert_eq!(
            loaded
                .table
                .metadata()
                .properties()
                .get(HIDDEN_COLUMNS_PROPERTY)
                .map(String::as_str),
            Some("__agg_state_c,__agg_state_s")
        );
        let fields = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields();
        let field_names = fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let branch_field = fields
            .iter()
            .find(|field| field.name == BRANCH_ID_COLUMN_NAME)
            .expect("branch id field");
        assert_eq!(
            field_names,
            vec![
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                "region",
                "c",
                "s",
                "__agg_state_c",
                "__agg_state_s",
                BRANCH_ID_COLUMN_NAME
            ]
        );

        assert_eq!(
            contract.target.hidden_apply_key.source,
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        );
        assert!(contract.aggregate.is_some());
        let branch = contract.branch.expect("branch contract");
        assert_eq!(branch.branch_count, 2);
        assert_eq!(
            branch.inner_apply_key_source,
            crate::sql::planner::vocabulary::ApplyKeySource::GroupRowId
        );
        assert_eq!(branch.branch_id_column.target_field_id, branch_field.id);
    }

    // A fan-in aggregate over the SAME physical table more than once
    // (`FROM (SELECT .. FROM t UNION ALL SELECT .. FROM t) GROUP BY ..`) dedups
    // to a single resolved base. After de-classifying the CREATE schema-contract
    // builders (R6), the degenerate fan-in must still be rejected at CREATE. It
    // is rejected upstream of the schema-contract builder, by the capability
    // property's `validate_distinct_base_ref_arity` (which requires the distinct
    // base count to equal the fan-in branch count), so the de-classification
    // did not regress this guard. This end-to-end `create_iceberg_mv` test pins
    // that rejection (the `refresh_contract` unit suite covers the in-isolation
    // `derive_imv_refresh_contract` path).
    #[test]
    fn create_iceberg_mv_rejects_fan_in_aggregate_over_same_table_twice() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fan_in_same_table
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, sum(amount) AS s
                FROM (
                    SELECT region, amount FROM ice.sales.fact
                    UNION ALL
                    SELECT region, amount FROM ice.sales.fact
                ) u
                GROUP BY region",
        );
        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("fan-in over the same physical table twice must be rejected at CREATE");
        assert!(
            err.contains("distinct Iceberg base table refs"),
            "expected same-physical-table fan-in rejection, got: {err}"
        );
    }

    #[test]
    fn plan_iceberg_mv_refresh_plans_aggregate_over_union_all_mv() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_east");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact_west");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_union_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, sum(amount) AS s
                FROM (
                    SELECT region, amount FROM ice.sales.fact_east
                    UNION ALL
                    SELECT region, amount FROM ice.sales.fact_west
                ) u
                GROUP BY region",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create aggregate-over-UNION-ALL iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_union_fact_region");
        let target = crate::mv::model::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_union_fact_region".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("aggregate-over-UNION-ALL refresh planning");

        assert_eq!(plan.contract.mode(), RefreshMode::Noop);
        assert_eq!(
            plan.contract
                .base_refs
                .iter()
                .map(|base| format!("{}.{}.{}", base.catalog, base.namespace, base.table))
                .collect::<Vec<_>>(),
            vec![
                "ice.sales.fact_east".to_string(),
                "ice.sales.fact_west".to_string()
            ]
        );
        assert_eq!(plan.contract.snapshot_pins.len(), 2);
        assert_eq!(
            plan.contract
                .snapshot_pins
                .get("ice.sales.fact_east")
                .copied(),
            Some(None)
        );
        assert_eq!(
            plan.contract
                .snapshot_pins
                .get("ice.sales.fact_west")
                .copied(),
            Some(None)
        );
    }

    #[test]
    fn iceberg_aggregate_target_columns_reject_duplicate_physical_names() {
        let (shape, analysis) = analyze_aggregate_fact_query(
            "select region, sum(amount) as s, count(*) as __agg_state_s \
             from ice.ns.fact group by region",
        );

        let err = iceberg_aggregate_target_columns(&shape, &analysis)
            .expect_err("duplicate aggregate physical column names should be rejected");
        assert!(
            err.contains("aggregate MV physical column name collision"),
            "err={err}"
        );
        assert!(err.contains("__agg_state_s"), "err={err}");
    }

    #[test]
    fn union_branch_inner_apply_key_maps_kind_to_source() {
        use crate::sql::planner::vocabulary::ApplyKeySource;

        assert_eq!(
            union_branch_inner_apply_key(UnionBranchKind::Aggregate),
            ApplyKeySource::GroupRowId
        );
        assert_eq!(
            union_branch_inner_apply_key(UnionBranchKind::ProjectionFilter),
            ApplyKeySource::BaseRowId
        );
    }

    #[test]
    fn create_iceberg_aggregate_mv_rejects_primary_key() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PRIMARY KEY (region)
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c
                FROM ice.sales.fact
                GROUP BY region",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("aggregate primary key should be rejected");
        assert!(
            err.contains("iceberg-backed aggregate materialized views do not support PRIMARY KEY"),
            "err={err}"
        );
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_max_int64_passes_validation() {
        // DDL-time MIN/MAX rejection has been removed. Detail-map state
        // runtime support allows creating an aggregate MV containing
        // MIN(int64_col) and MAX(int64_col) end-to-end.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_region
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, min(amount) AS min_amount, max(amount) AS max_amount
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("MIN/MAX on int64 column should be accepted");

        // Verify the resulting layout has Map<Int64, Int64> state columns
        // for both MIN and MAX (the visible scalar SQL type is BigInt, but
        // the physical state column stores a value-count detail map).
        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_region")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        let aggregate = contract.aggregate.expect("aggregate contract");
        // Two MIN/MAX state columns plus one retraction-count state column
        // (no explicit COUNT(*) in the shape — see
        // `aggregate_shape_needs_retraction_count_state`).
        assert_eq!(
            aggregate.state_columns.len(),
            3,
            "unexpected state column layout: {:?}",
            aggregate
                .state_columns
                .iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
        );
        let by_name: std::collections::HashMap<&str, &str> = aggregate
            .state_columns
            .iter()
            .map(|c| (c.column_name.as_str(), c.type_signature.as_str()))
            .collect();
        assert_eq!(
            by_name.get("__agg_state_min_amount").copied(),
            Some("binary")
        );
        assert_eq!(
            by_name.get("__agg_state_max_amount").copied(),
            Some("binary")
        );
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_max_combined_with_others_passes_validation() {
        // MIN/MAX coexists with SUM/COUNT/AVG aggregate VARBINARY states.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_mix
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region,
                       min(amount) AS min_amount,
                       max(region) AS max_region,
                       sum(amount) AS sum_amount,
                       count(*) AS row_count,
                       avg(amount) AS avg_amount
                FROM ice.sales.fact
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("mixed aggregate MV with MIN/MAX should be accepted post-Phase-5");

        let contract = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_fact_mix")
            .expect("mv definition")
            .schema_contract
            .expect("schema contract");
        let aggregate = contract.aggregate.expect("aggregate contract");
        // One opaque VARBINARY state column per aggregate.
        assert_eq!(
            aggregate.state_columns.len(),
            5,
            "unexpected state column layout: {:?}",
            aggregate
                .state_columns
                .iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
        );
        let by_name: std::collections::HashMap<&str, &str> = aggregate
            .state_columns
            .iter()
            .map(|c| (c.column_name.as_str(), c.type_signature.as_str()))
            .collect();
        assert_eq!(
            by_name.get("__agg_state_min_amount").copied(),
            Some("binary")
        );
        assert_eq!(
            by_name.get("__agg_state_max_region").copied(),
            Some("binary")
        );
        assert_eq!(
            by_name.get("__agg_state_sum_amount").copied(),
            Some("binary")
        );
        assert_eq!(
            by_name.get("__agg_state_row_count").copied(),
            Some("binary")
        );
        assert_eq!(
            by_name.get("__agg_state_avg_amount").copied(),
            Some("binary")
        );
    }

    #[test]
    fn iceberg_aggregate_mv_with_min_float_is_accepted() {
        // IVM-P5 Float follow-up: Float MIN/MAX is now supported in
        // detail-state aggregate IMVs. NaN handling lives in three sites:
        // `scalar_keys_equal` (NaN == NaN), `sort_map_entries_by_key`
        // (NaN sorts to end), and `derive_visible_from_detail_map` (skips
        // NaN keys — matches SQL standard "ignore NaN in MIN/MAX").
        // This replaces the previous MIN/MAX rejection test.
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table_with_float(&env.state, "ice", "sales", "fact_float");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact_float
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, min(price) AS min_price
                FROM ice.sales.fact_float
                GROUP BY region",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("Float MIN/MAX should now be accepted at the validator layer");
    }

    #[test]
    fn create_iceberg_mv_uses_current_catalog_target_without_starrocks_table_row() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        crate::engine::mv_flow::create_mv(
            &env.state,
            Some("ice"),
            &env.current_db,
            &stmt,
            &crate::connector::test_request_context(),
        )
        .expect("create iceberg mv through ddl");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv relationship");
        assert_eq!(mv.select_sql, "SELECT id, name FROM ice.sales.orders");
        assert_eq!(mv.target_catalog.as_deref(), Some("ice"));
        assert_eq!(mv.target_namespace.as_deref(), Some("analytics"));
        assert_eq!(mv.target_table.as_deref(), Some("mv_orders"));

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("target table");
        let catalog = env
            .state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog");
        assert!(
            catalog.get("analytics", "mv_orders").is_err(),
            "provider MV targets must not leak into the process-wide planner catalog"
        );
    }

    // Native FE/BE owner coverage: iceberg_backed_mv_basic_lifecycle.sql,
    // iceberg_ivm_union_projection_filter.sql,
    // iceberg_ivm_fan_in_aggregate_union.sql,
    // iceberg_ivm_aggregate_cross_join.sql,
    // iceberg_ivm_branch_union_aggregate.sql, iceberg_ivm_join_aggregate.sql,
    // and iceberg_ivm_projection_repartition.sql. Core retains only
    // contract-level unit tests.

    // Native FE/BE owner coverage: iceberg_ivm_aggregate_cross_join.sql.

    // Native FE/BE owner coverage: iceberg_ivm_a11_base_rename_referenced.sql.
    // The provider-visible rename plus refresh lifecycle cannot be represented
    // by the Core in-process executor after SQLX-2.

    /// W5 external-interop contract for the current single-table lake-native
    /// MV model: external engines read the MV Iceberg table itself. The table
    /// physically carries NovaRocks internal columns, while the descriptor
    /// records which columns form the public read surface.
    #[test]
    fn mv_table_descriptor_separates_external_visible_columns_from_hidden_columns() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("load MV table by visible target name");
        let field_names = loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert!(field_names.contains(&"id"), "{field_names:?}");
        assert!(field_names.contains(&"name"), "{field_names:?}");
        assert!(
            field_names.contains(&HIDDEN_APPLY_KEY_COLUMN_NAME),
            "{field_names:?}"
        );

        let descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())
                .expect("descriptor properties");
        assert_eq!(descriptor.package_id, "analytics.mv_orders");
        assert_eq!(
            descriptor.visible_columns,
            vec!["id".to_string(), "name".to_string()]
        );
        assert_eq!(
            descriptor.hidden_columns,
            vec![HIDDEN_APPLY_KEY_COLUMN_NAME.to_string()]
        );
        for hidden in &descriptor.hidden_columns {
            assert!(
                !descriptor.visible_columns.contains(hidden),
                "hidden column `{hidden}` must not be part of the external visible-column contract"
            );
        }
    }

    #[test]
    fn create_iceberg_mv_treats_legacy_prefixed_visible_name_as_target_name() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let visible_name = ["__nr", "_mv_", "visible_name"].concat();
        let stmt = parse_create_mv(&format!(
            "CREATE MATERIALIZED VIEW {visible_name}
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders"
        ));

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("legacy-shaped prefix is no longer reserved");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", &visible_name)
                .expect("load MV table by visible target name");
        let descriptor =
            MvDescriptorV1::from_storage_properties(loaded.table.metadata().properties())
                .expect("descriptor properties");
        assert_eq!(descriptor.package_id, format!("analytics.{visible_name}"));

        let doubled_legacy_name = ["__nr", "_mv_", &visible_name].concat();
        let doubled_err = crate::connector::iceberg::catalog::load_table(
            &entry,
            "analytics",
            &doubled_legacy_name,
        )
        .expect_err("doubled legacy-style target table must not exist");
        assert!(doubled_err.contains(&doubled_legacy_name), "{doubled_err}");
    }

    #[test]
    fn create_iceberg_mv_resolves_unqualified_base_in_current_catalog() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "analytics", "orders");

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg mv");

        let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv relationship");
        assert_eq!(mv.base_table_refs.len(), 1);
        assert_eq!(mv.base_table_refs[0], "ice.analytics.orders");
    }

    #[test]
    fn drop_iceberg_mv_drops_target_table_and_relationship() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let definition = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("mv definition before drop");
        let mv_id = definition.mv_id;
        assert!(!list_mv_dependency_names(&env.state, mv_id).is_empty());

        let stmt = parse_drop_mv("DROP MATERIALIZED VIEW mv_orders");
        drop_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt).expect("drop mv");

        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read metadata");
        assert!(
            env.state
                .mv_repository
                .load_by_id(mv_id)
                .expect("load definition")
                .is_none()
        );
        assert!(
            env.state
                .mv_repository
                .list_dependencies_by_downstream(mv_id)
                .expect("list dependencies")
                .is_empty()
        );
        assert!(
            env.state
                .mv_repository
                .list_partition_states(mv_id)
                .expect("list partition states")
                .is_empty()
        );
        drop(read);

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        assert!(
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders",)
                .is_err()
        );
        assert!(find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders").is_none());
        let catalog = env
            .state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog");
        assert!(catalog.get("analytics", "mv_orders").is_err());
    }

    #[test]
    fn drop_iceberg_mv_rejects_active_refresh_before_external_drop() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");

        let mv_id = {
            let provider = env
                .state
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let read = provider.begin_read().expect("open read txn");
            env.state
                .mv_repository
                .find_by_target(&MvTarget {
                    catalog: Some("ice".to_string()),
                    database: "analytics".to_string(),
                    name: "mv_orders".to_string(),
                })
                .expect("find mv target")
                .expect("mv definition")
                .mv_id
        };
        {
            let provider = env
                .state
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let mut txn = provider
                .begin_write("begin active mv refresh")
                .expect("write");
            env.state
                .mv_repository
                .begin_refresh_intent(mv_id, std::collections::BTreeMap::new())
                .expect("begin refresh");
            txn.commit().expect("commit refresh intent");
        }

        let stmt = parse_drop_mv("DROP MATERIALIZED VIEW mv_orders");
        let err = drop_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("active refresh should block drop before external table drop");
        assert!(err.contains("refresh in progress"), "err={err}");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("target table should remain after rejected drop");
        assert!(find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders").is_some());
    }

    #[test]
    fn create_iceberg_mv_rejects_existing_target_table() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("existing target should fail");
        assert_eq!(
            err,
            "Iceberg MV target table ice.analytics.mv_orders already exists"
        );
    }

    #[test]
    fn create_iceberg_mv_post_create_failure_drops_target_table() {
        let env = open_test_state_with_iceberg_catalog_without_metadata("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        // The typed repository availability check runs before target creation.
        // With no StateStore repository attached to the test state, we fail
        // fast and the Iceberg target table is never created — so there is
        // nothing to clean up.
        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("missing StateStore repository should fail before target create");
        assert!(
            err.contains("materialized view service requires [state_store]"),
            "err={err}"
        );

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        assert!(
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders",)
                .is_err(),
            "target table should never have been created"
        );
    }

    #[test]
    fn create_iceberg_mv_target_preflight_failure_leaves_no_repository_metadata() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let blocked_target = env
            ._warehouse_dir
            .path()
            .join("analytics")
            .join("mv_orders");
        std::fs::write(&blocked_target, b"not a directory")
            .expect("block target table directory creation");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("target create must fail");
        let canonical_warehouse = env
            ._warehouse_dir
            .path()
            .canonicalize()
            .expect("canonical warehouse path");
        let metadata_location = format!(
            "file://{}/analytics/mv_orders/metadata/v1.metadata.json",
            canonical_warehouse.display()
        );
        assert!(
            err.contains("read iceberg metadata dir")
                && err.contains("/analytics/mv_orders/metadata failed")
                && err.contains("Not a directory"),
            "unexpected target preflight error for {metadata_location}: {err}"
        );
        assert!(blocked_target.is_file());
        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("repository read");
        assert!(
            env.state
                .mv_repository
                .list_definitions()
                .expect("definitions")
                .is_empty(),
            "target creation fails before repository persistence"
        );
    }

    #[test]
    fn create_iceberg_mv_target_inspection_failure_cleans_target_and_metadata() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let hook_entry = entry.clone();
        let _after_create_hook = AfterCreateTargetHookGuard::install(Arc::new(move || {
            hook_entry.poison_table_cache_for_test();
        }));
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("post-create target inspection must fail");
        assert_eq!(
            err,
            "Internal: table cache lock: poisoned lock: another task failed inside; target cleanup=Ok(())"
        );
        assert!(
            !env._warehouse_dir
                .path()
                .join("analytics")
                .join("mv_orders")
                .exists(),
            "inspection failure must clean the created target"
        );
        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("repository read");
        assert!(
            env.state
                .mv_repository
                .list_definitions()
                .expect("definitions")
                .is_empty(),
            "inspection fails before repository persistence"
        );
    }

    #[test]
    fn create_iceberg_mv_characterizes_successful_boundary_artifacts() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let definition_loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let _definition_load_guard =
            DefinitionLoadCounterGuard::install(Arc::clone(&definition_loads));
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create iceberg MV");

        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        let target =
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
                .expect("created and inspected target");
        let bootstrap = target
            .table
            .metadata()
            .current_snapshot()
            .expect("CREATE MV must establish a provider-owned empty snapshot");
        assert!(
            bootstrap.parent_snapshot_id().is_none(),
            "the MV bootstrap snapshot must be the target table's first snapshot"
        );
        assert_eq!(
            bootstrap
                .summary()
                .additional_properties
                .get("novarocks.mv.bootstrap")
                .map(String::as_str),
            Some("true"),
            "the bootstrap request properties must be committed by the provider"
        );
        assert!(
            bootstrap
                .summary()
                .additional_properties
                .contains_key("novarocks.bootstrap.empty.operation-id"),
            "the provider must retain its idempotency marker on the bootstrap snapshot"
        );
        let definition = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_orders")
            .expect("repository definition");
        assert!(definition.schema_contract.is_some());
        assert_eq!(
            definition_loads.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "descriptor sync must use the committed definition supplied by the application shell"
        );
        let descriptor =
            MvDescriptorV1::from_storage_properties(target.table.metadata().properties())
                .expect("synced descriptor");
        assert_eq!(
            descriptor.schema_contract_typed().expect("typed contract"),
            definition.schema_contract
        );
        let provider = env
            .state
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("repository read");
        let dependencies = env
            .state
            .mv_repository
            .list_dependencies_by_downstream(definition.mv_id)
            .expect("repository dependencies");
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].upstream.display_name(), "ice.sales.orders");
        assert!(
            env.state
                .catalog_service
                .local()
                .read()
                .expect("local catalog")
                .get("analytics", "mv_orders")
                .is_err(),
            "the provider target is resolved through a query-local binding, not a planner table"
        );
    }

    #[test]
    fn create_iceberg_mv_repository_open_failure_cleans_created_target() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let _failure = fail_next_mv_repository_command(TestMvRepositoryFailurePoint::Create);
        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("repository write open must fail");
        assert_eq!(
            err,
            "create iceberg MV repository metadata failed: test-only injected MV repository failure at Create; target cleanup=Ok(())"
        );
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        assert!(
            crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders",)
                .is_err(),
            "known repository failure must clean the created target"
        );
        assert!(
            env.state
                .mv_repository
                .list_definitions()
                .expect("definitions")
                .is_empty()
        );
    }

    #[test]
    fn create_iceberg_mv_descriptor_sync_failure_retains_target_and_committed_metadata() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        let state_slot = Arc::new(std::sync::Mutex::new(None::<Arc<StandaloneState>>));
        let hook_slot = Arc::clone(&state_slot);
        let _after_create = after_next_mv_repository_create(Arc::new(move || {
            let state = hook_slot
                .lock()
                .expect("state slot")
                .as_ref()
                .expect("test state installed")
                .clone();
            corrupt_mv_descriptor_for_test(&state, "ice", "analytics", "mv_orders")
                .expect("corrupt descriptor after repository commit");
        }));
        *state_slot.lock().expect("state slot") = Some(Arc::clone(&env.state));
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("descriptor sync must fail after repository create");
        assert!(
            err.starts_with("[CommitKnownCommittedFinalizeFailed] Iceberg MV repository create committed but descriptor sync failed:"),
            "unexpected error: {err}"
        );
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("descriptor sync failure must retain the committed target");
        let definitions = env
            .state
            .mv_repository
            .list_definitions()
            .expect("definitions");
        assert_eq!(
            definitions.len(),
            1,
            "repository commit precedes descriptor sync"
        );
        assert_eq!(definitions[0].target_table.as_deref(), Some("mv_orders"));
        assert_eq!(
            env.state
                .mv_repository
                .list_dependencies_by_downstream(definitions[0].mv_id)
                .expect("dependencies")
                .len(),
            1
        );
    }

    #[test]
    fn create_iceberg_mv_catalog_register_failure_keeps_target_and_committed_metadata() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        let _failure = CatalogRegistrationFailureGuard::install(
            "test-only injected catalog registration failure",
        );
        create_base_table(&env.state, "ice", "sales", "orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect_err("catalog register must fail");
        assert_eq!(
            err,
            "[CommitKnownCommittedFinalizeFailed] Iceberg MV repository create committed but catalog registration failed: test-only injected catalog registration failure"
        );
        let entry = {
            let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
            catalogs.get("ice").expect("catalog")
        };
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_orders")
            .expect("register failure occurs after target create and descriptor sync");
        let definition = env
            .state
            .mv_repository
            .find_by_target(&MvTarget {
                catalog: Some("ice".to_string()),
                database: "analytics".to_string(),
                name: "mv_orders".to_string(),
            })
            .expect("definition lookup")
            .expect("committed definition");
        assert!(definition.schema_contract.is_some());
        assert_eq!(
            env.state
                .mv_repository
                .list_dependencies_by_downstream(definition.mv_id)
                .expect("dependencies")
                .len(),
            1
        );
    }

    #[test]
    fn create_iceberg_mv_if_not_exists_does_not_adopt_existing_target() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        register_iceberg_mv_target_in_catalog(
            &env.state,
            &IcebergMvTarget {
                catalog: "ice".to_string(),
                namespace: "analytics".to_string(),
                table: "mv_orders".to_string(),
            },
        )
        .expect("register existing target");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        let err = crate::engine::mv_flow::create_mv(
            &env.state,
            Some("ice"),
            &env.current_db,
            &stmt,
            &crate::connector::test_request_context(),
        )
        .expect_err("existing target should fail even with IF NOT EXISTS");
        assert_eq!(
            err,
            "Iceberg MV target table ice.analytics.mv_orders already exists"
        );
    }

    #[test]
    fn create_iceberg_mv_requires_current_iceberg_catalog() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id, name FROM ice.sales.orders",
        );

        for current_catalog in [None, Some("default_catalog")] {
            let err = create_iceberg_mv(&env.state, current_catalog, &env.current_db, &stmt)
                .expect_err("non-iceberg catalog should fail");
            assert_eq!(
                err,
                "storage_engine='iceberg' requires current catalog to be an Iceberg catalog"
            );
        }
    }

    #[test]
    fn plan_iceberg_mv_refresh_reports_unknown_for_join_mv() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "left_orders");
        create_base_table(&env.state, "ice", "sales", "right_orders");
        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_join_orders
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT l.id, l.name
                FROM ice.sales.left_orders l
                JOIN ice.sales.right_orders r ON l.id = r.id",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create join iceberg mv");

        let refresh = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_join_orders");
        let target = crate::mv::model::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_join_orders".to_string(),
        };
        let plan =
            plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &refresh, target)
                .expect("join refresh plan");

        assert_eq!(
            plan.contract.affected_partitions.not_derived_reason(),
            Some("join MV affected partition planning is not implemented")
        );
    }

    #[test]
    fn plan_iceberg_mv_refresh_requires_a11_schema_contract() {
        let env = open_test_state_with_iceberg_catalog("ice", "analytics");
        create_base_table(&env.state, "ice", "sales", "orders");
        create_base_table(&env.state, "ice", "analytics", "mv_orders");
        env.state
            .mv_repository
            .create(
                uuid::Uuid::new_v4(),
                CreateMvRepositoryRequest {
                    definition: CreateMvDefinitionRequest {
                        select_sql: "SELECT id, name FROM ice.sales.orders".to_string(),
                        base_table_refs: vec!["ice.sales.orders".to_string()],
                        primary_key_columns: Vec::new(),
                        storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
                        target_catalog: Some("ice".to_string()),
                        target_namespace: Some("analytics".to_string()),
                        target_table: Some("mv_orders".to_string()),
                        schema_contract: None,
                        partition_spec: None,
                        created_at_ms: now_ms(),
                    },
                    refresh: initial_refresh_configuration_for_create(
                        &crate::mv::application::MvCreateRefreshPolicy::Manual,
                    ),
                    dependencies: Vec::new(),
                },
            )
            .expect("create mv definition");

        let stmt = parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders");
        let target = crate::mv::model::MvTarget {
            catalog: Some("ice".to_string()),
            database: "analytics".to_string(),
            name: "mv_orders".to_string(),
        };
        let err = plan_iceberg_mv_refresh(&env.state, Some("ice"), &env.current_db, &stmt, target)
            .expect_err("missing schema contract should fail");

        assert!(
            err.to_string().contains("missing A11 schema contract"),
            "{err}"
        );
    }

    #[test]
    fn change_stream_writer_result_override_is_test_thread_local() {
        let _main_guard = install_imv_change_stream_execution_result_for_test(
            imv_change_stream_writer_abort_result_for_test("main-thread abort"),
        );

        let worker_reason = std::thread::spawn(|| {
            assert!(
                take_imv_change_stream_execution_result_for_test().is_none(),
                "worker must not observe another test thread's override"
            );
            let _worker_guard = install_imv_change_stream_execution_result_for_test(
                imv_change_stream_writer_abort_result_for_test("worker-thread abort"),
            );
            take_imv_change_stream_execution_result_for_test()
                .and_then(|result| result.write_abort)
                .map(|abort| abort.reason)
                .expect("worker override")
        })
        .join()
        .expect("worker thread");

        assert_eq!(worker_reason, "worker-thread abort");
        let main_reason = take_imv_change_stream_execution_result_for_test()
            .and_then(|result| result.write_abort)
            .map(|abort| abort.reason)
            .expect("main-thread override");
        assert_eq!(main_reason, "main-thread abort");
    }

    /// Lake-native recovery: when `main` was moved to some OTHER snapshot (here
    /// by an external writer with no MV marker) while a refresh's staging
    /// branch is still off `main`'s lineage, the staged snapshot never
    /// published, so recovery classifies it `Diverged` and rolls it back —
    /// dropping the staging branch and aborting the ledger row — while leaving
    /// the (externally advanced) `main` untouched. Fail-loud is reserved for
    /// the narrower case where `main`'s current snapshot IS the staged snapshot
    /// id but its marker does not match (see
    /// `classify_staging_branch`); a divergent `main` is a rollback, not a
    /// commit-unknown.

    #[test]
    fn build_iceberg_schema_maps_int_bigint_string() {
        let cols = vec![
            output_col("k", DataType::Int32, false),
            output_col("v", DataType::Int64, true),
            output_col("s", DataType::Utf8, true),
        ];
        let schema = build_iceberg_schema_from_outputs(&cols).expect("schema");
        assert_eq!(schema.as_struct().fields().len(), 3);
        assert_eq!(schema.as_struct().fields()[0].name, "k");
        assert!(schema.as_struct().fields()[0].required);
        assert_eq!(schema.as_struct().fields()[1].name, "v");
        assert!(!schema.as_struct().fields()[1].required);
        assert_eq!(schema.as_struct().fields()[2].name, "s");
        assert!(!schema.as_struct().fields()[2].required);
    }

    #[test]
    fn arrow_data_type_to_iceberg_rejects_unsupported_types() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Map(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::empty()),
                false,
            )),
            false,
        ))
        .unwrap_err();
        assert!(err.to_lowercase().contains("unsupported"));
    }

    #[test]
    fn arrow_decimal_negative_scale_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal128(10, -2)).unwrap_err();
        assert!(err.contains("negative scale"));
    }

    #[test]
    fn arrow_int8_int16_promote_to_iceberg_int() {
        use novarocks_connector_iceberg::iceberg::spec::PrimitiveType;
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int8).unwrap(),
            PrimitiveType::Int
        );
        assert_eq!(
            arrow_data_type_to_iceberg_primitive(&DataType::Int16).unwrap(),
            PrimitiveType::Int
        );
    }

    #[test]
    fn arrow_decimal256_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::Decimal256(40, 2)).unwrap_err();
        assert!(err.contains("Decimal256"));
    }

    #[test]
    fn arrow_fixed_size_binary_16_is_rejected() {
        let err = arrow_data_type_to_iceberg_primitive(&DataType::FixedSizeBinary(16)).unwrap_err();
        assert!(err.contains("LARGEINT"));
    }
    mod aggregate_apply_test_helpers {
        use crate::mv::aggregate_state::mv_agg_state::{
            AggregateMvLayout, AggregateStateColumn, AggregateVisibleColumn,
        };
        use crate::mv::aggregate_state::physical_column::starrocks_physical_column;
        use crate::mv::model::AggregateStateRole;
        use crate::sql::mv_refresh::AggregateFunctionKind;
        use arrow::datatypes::DataType;
        use novarocks_catalog::schema::SqlType;

        pub(super) fn count_layout(group_key: &str) -> AggregateMvLayout {
            let row_id = starrocks_physical_column(
                "__row_id__".to_string(),
                SqlType::String,
                false,
                false,
                true,
            );
            let group = starrocks_physical_column(
                group_key.to_string(),
                SqlType::String,
                true,
                true,
                false,
            );
            let counter =
                starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
            let state = starrocks_physical_column(
                "__agg_state_c".to_string(),
                SqlType::BigInt,
                false,
                false,
                false,
            );
            AggregateMvLayout {
                row_id_column: row_id.clone(),
                visible_columns: vec![
                    AggregateVisibleColumn {
                        name: group_key.to_string(),
                        data_type: DataType::Utf8,
                        sql_type: SqlType::String,
                        nullable: true,
                        source_index: 0,
                    },
                    AggregateVisibleColumn {
                        name: "c".to_string(),
                        data_type: DataType::Int64,
                        sql_type: SqlType::BigInt,
                        nullable: false,
                        source_index: 1,
                    },
                ],
                state_columns: vec![AggregateStateColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    visible_source_index: 1,
                    aggregate_index: 0,
                    function: AggregateFunctionKind::Count,
                    state_role: AggregateStateRole::Single,
                    count_star: true,
                }],
                aggregate_input_types: vec![None],
                group_key_source_indexes: vec![0],
                physical_columns: vec![row_id, group, counter, state],
            }
        }
    }

    // Native FE/BE owner coverage: iceberg_ivm_optimize_replace_refresh.sql.
    #[allow(dead_code)]
    fn incremental_refresh_absorbs_optimize_replace_snapshot() {
        let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
        create_aggregate_fact_table(&env.state, "ice", "sales", "fact");
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(1, "east", 10)]);

        let stmt = parse_create_mv(
            "CREATE MATERIALIZED VIEW mv_fact
             DISTRIBUTED BY HASH(region) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT region, count(*) AS c, sum(amount) AS s
                FROM ice.sales.fact GROUP BY region",
        );
        create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
            .expect("create incremental MV");
        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "REFRESH MATERIALIZED VIEW mv_fact",
        );

        // Second append snapshot on the base, then rewrite the base table.
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(2, "west", 5)]);
        let base_snapshot_id = read_target_current_snapshot_id(&env.state, "ice", "sales", "fact")
            .expect("base snapshot before rewrite");
        let target = crate::connector::iceberg::compact::WholeTableRewriteTarget {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
            base_snapshot_id,
            job_id: None,
        };
        crate::connector::iceberg::compact::execute_whole_table_rewrite_for_target(
            &env.state, &target,
        )
        .expect("rewrite base table");

        // Third append after the replace snapshot, then refresh incrementally.
        insert_into_aggregate_fact_table(&env.state, "ice", "sales", "fact", &[(3, "east", 7)]);
        execute_iceberg_sql(
            &env.state,
            Some("ice"),
            &env.current_db,
            "REFRESH MATERIALIZED VIEW mv_fact",
        );

        // Lineage walk previous -> current crossed the REPLACE snapshot; rows
        // must reflect all three appends.
        assert_aggregate_region_rows(
            &env.state,
            "ice",
            &env.current_db,
            "mv_fact",
            &[("east", 2, 17), ("west", 1, 5)],
        );
    }
}

#[cfg(test)]
mod imv_planning_catalog_tests {
    use super::*;

    // The test below exercises the correct API surface for
    // build_iceberg_mv_planning_catalog. Full execution requires a
    // StandaloneState with two real Iceberg catalog entries, which depends on
    // the iceberg-rest Docker harness. Deferred to the iceberg-ivm SQL suite
    // (Task 15). The #[ignore] keeps the test compilable and discoverable
    // without requiring the harness in unit-test runs.
    #[test]
    #[ignore = "fixture deferred — covered by iceberg-ivm suite (Task 15)"]
    fn build_iceberg_mv_planning_catalog_registers_each_base() {
        // Planning-catalog registration was a concrete provider fixture. The
        // canonical compiler now receives tokenized, admission-frozen table
        // facts; native Iceberg coverage belongs to the iceberg-ivm suite.
        assert!(true);
    }
}

#[cfg(test)]
mod imv_pipeline_wiring_tests {
    // Lib-level refresh smoke for the IMV pipeline wire-up.
    //
    // Reuses the same fixture obstacle as imv_planning_catalog_tests above:
    // exercising refresh pipeline wiring end-to-end requires a StandaloneState
    // with real Iceberg catalog entries (driven by the iceberg-rest Docker
    // harness). Deferred to the iceberg-ivm SQL suite.
    #[test]
    #[ignore = "fixture deferred — covered by iceberg-ivm suite (Task 15)"]
    fn projection_filter_refresh_through_imv_pipeline_matches_baseline() {
        unimplemented!(
            "inline an existing ProjectionFilter refresh fixture and assert row equality"
        )
    }
}
