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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::DataType;

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::mv::domain::analysis::refresh_property::{
    RefreshFragmentProperty, TargetIdentity, derive_fragment_property, derive_imv_refresh_contract,
};
use crate::mv::domain::analysis::{
    MvAnalysis, canonicalize_iceberg_mv_select_query, output_column_to_table_column,
    validate_mv_partition_columns,
};
use crate::mv::domain::analysis_adapter::{
    BaseColumnDescriptor, BaseTableDescriptor, validate_ivm_primary_key,
};
use crate::mv::domain::application::MvCreateProjectionSeed;
use crate::mv::domain::application::{
    MvApplicationError, MvApplicationErrorKind, MvCreateRefreshPolicy, MvCreateStatement,
    MvRefreshRequest,
};
use crate::mv::domain::lifecycle::RefreshPlan;
use crate::mv::domain::model::{MvStorageEngine, RefreshMode};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::refresh::apply_key::ApplyKeyContract;
use crate::mv::domain::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::domain::refresh::contract::ImvRefreshContract;
use crate::mv::domain::refresh::definition::{mv_definition_fingerprint, parse_mv_select_query};
#[cfg(test)]
use crate::mv::domain::refresh::execution_policy::{
    non_join_incremental_write_mode, select_join_incremental_execution_mode,
    should_use_join_delta_append_only_fast_path,
};
use crate::mv::domain::refresh::pin::RefreshSnapshotPin;
use crate::mv::domain::refresh::planning::{
    RefreshBaseRelationOccurrence, RefreshPlanContract, RefreshPlanningInput, RefreshStateBaseline,
    RefreshStateBaselineSource, decide_requested_refresh_plan,
};
#[cfg(test)]
use crate::mv::domain::refresh::repartition::{RepartitionShape, select_repartition_shape};
use crate::mv::domain::refresh::schema_contract::{
    MvOccurrenceFieldRebind, validate_aggregate_schema_contract_for_base,
    validate_aggregate_schema_contract_metadata, validate_projection_target,
};
use crate::mv::domain::refresh::snapshot::{
    BaseSnapshotPolicy, BaseSnapshotStatus, ExecutableRefreshDecision,
};
use crate::mv::domain::refresh::target::{IcebergMvTarget, resolve_refresh_target};
use crate::mv::domain::refresh::target_apply::{
    apply_key_table_column, branch_id_table_column, ensure_base_row_lineage_contract,
    join_apply_key_table_column,
};
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::schema_validation::{
    validate_branch_id_field, validate_join_schema_contract, validate_schema_contract,
};
use crate::mv::domain::storage_observation::{
    MvSchemaValidationObservation, MvSourceSchemaObservation,
};
use novarocks_catalog_application::CatalogApplicationPort;
use novarocks_mv_application::persistence::codec::{ApplyKeyKind, RelationOccurrence};
use novarocks_mv_application::persistence::definition::CreateMvDefinitionRequest;
use novarocks_mv_application::persistence::definition::MvDesiredRefreshPolicy;
use novarocks_mv_application::persistence::dependency::CreateMvDependencyRequest;
use novarocks_mv_application::persistence::exact_revision::restore_exact_query_revision;
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::persistence::schema as mv_schema;
use novarocks_mv_application::persistence::schema::{
    APPLY_KEY_FIELD_ID_PROPERTY, APPLY_KEY_SOURCE_PROPERTY,
};
#[cfg(test)]
use novarocks_mv_application::product::{MvIncrementalJoinMode, MvIncrementalWriteMode};
use novarocks_parser::{Span, ast};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{
    CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY as APPLY_KEY_COLUMN_PROPERTY,
    CONNECTOR_MV_HIDDEN_COLUMNS_PROPERTY as HIDDEN_COLUMNS_PROPERTY, ConnectorControlRegistry,
    ConnectorInstanceId, ConnectorTableObjectId,
};
#[cfg(test)]
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_sql::compiler::SqlMvRelationOccurrenceId;
#[cfg(test)]
use novarocks_sql::planning::mv::MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_sql::planning::mv::UnionBranchKind;
use novarocks_sql::planning::mv::{
    MV_BRANCH_ID_COLUMN_NAME as BRANCH_ID_COLUMN_NAME,
    MV_HIDDEN_APPLY_KEY_COLUMN_NAME as HIDDEN_APPLY_KEY_COLUMN_NAME,
    MV_JOIN_APPLY_KEY_COLUMN_NAME as JOIN_APPLY_KEY_COLUMN_NAME, SqlMvAggregateCalls,
    SqlMvAggregateLayoutScope, SqlMvApplyKeySourceFacts, extract_join_aliases,
    mv_apply_key_source_from_column_name,
};
use novarocks_sql::semantic::{IcebergPartitionFieldExpr, TableColumnDef};
use novarocks_types::naming::{TableIdentity, normalize_identifier};

/// SQL-owned bridge for refresh planning.  It owns only analysis and immutable
/// facts; all durable intent, ref mutations, and writer execution are handed
/// to the frontend as `PreparedMvRefresh`.
/// The explicit Core inputs required by an Iceberg MV CREATE operation.
///
/// This is intentionally a narrow composition value: it owns the frozen
/// catalog source plus the provider and durable MV ports that CREATE needs.
/// It does not retain aggregate application state, and it has no state-based
/// constructor so a frontend composition must name every dependency.
#[derive(Clone)]
pub struct IcebergMvCorePorts {
    functions: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    catalog_service: Arc<QueryCatalogService>,
    catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    readiness: Arc<MvReadinessPort>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    /// The one FE-process management authority every document-managed effect
    /// commits under. The composed constructor installs this original authority;
    /// its existing operation-facing getter retains the Result contract.
    management_entrance: Option<Arc<novarocks_mv_application::management::ManagementEntrance>>,
}

impl IcebergMvCorePorts {
    /// Construct a complete MV capability graph from the original role leaves.
    /// Source preparation and staged publication share the exact typed host;
    /// a background refresh cannot be composed with only the erased registry.
    pub(crate) fn new_with_management_entrance(
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        catalog_service: Arc<QueryCatalogService>,
        catalog_application: Option<Arc<dyn CatalogApplicationPort>>,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        readiness: Arc<MvReadinessPort>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        management_entrance: Arc<novarocks_mv_application::management::ManagementEntrance>,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self {
            functions,
            constant_policy,
            catalog_service,
            catalog_application,
            connector_control,
            typed_connector_control,
            readiness,
            storage_observation,
            management_entrance: Some(management_entrance),
        }
    }

    pub(crate) fn management_entrance(
        &self,
    ) -> Result<&Arc<novarocks_mv_application::management::ManagementEntrance>, String> {
        self.management_entrance.as_ref().ok_or_else(|| {
            "document-managed MV effects require the composed FE management entrance".to_string()
        })
    }

    pub(crate) fn typed_connector_control(
        &self,
    ) -> &Arc<novarocks_catalog_application::ConnectorControlHost> {
        &self.typed_connector_control
    }

    pub(crate) const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    pub(crate) fn function_catalog(&self) -> &Arc<novarocks_functions::EngineFunctionCatalog> {
        &self.functions
    }

    pub(crate) fn readiness(&self) -> &Arc<MvReadinessPort> {
        &self.readiness
    }

    pub(crate) fn connector_control(&self) -> &dyn ConnectorControlRegistry {
        self.connector_control.as_ref()
    }

    pub(crate) fn storage_observation(&self) -> &dyn MvStorageObservationPort {
        self.storage_observation.as_ref()
    }

    pub(crate) fn catalog_application(&self) -> Option<&dyn CatalogApplicationPort> {
        self.catalog_application.as_deref()
    }
}

impl crate::catalog_application::query_catalog::CatalogServiceSource for IcebergMvCorePorts {
    fn catalog_service(&self) -> &Arc<QueryCatalogService> {
        &self.catalog_service
    }
}

pub(crate) struct IcebergMvCreatePlan {
    pub(crate) target: IcebergMvTarget,
    pub(crate) refresh_contract: ImvRefreshContract,
    pub(crate) property: RefreshFragmentProperty,
    pub(crate) base_refs: Vec<TableIdentity>,
    pub(crate) dependencies: Vec<CreateMvDependencyRequest>,
    /// The occurrence-qualified SQL facts D/L are bound against. Computed once
    /// with the analysis so the document build cannot re-derive a different
    /// projection of the same query.
    pub(crate) create_persistence_facts: novarocks_sql::planning::mv::SqlMvCreatePersistenceFacts,
    /// Present exactly when the target carries a UNION branch discriminator.
    pub(crate) branch_id_column_name: Option<String>,
    pub(crate) source_field_observations:
        Vec<novarocks_mv_application::persistence::aggregate_bindings::MvCreateRelationObservation>,
    pub(crate) binding_document:
        novarocks_mv_application::persistence::codec::keyed::BindingDocument,
    pub(crate) created_at_ms: i64,
    /// Runtime state layout frozen during the same CREATE analysis as the
    /// physical request. Stateless shapes use the validated empty layout so
    /// D/L construction never invents an alternate aggregate carrier.
    pub(crate) aggregate_runtime_layout:
        novarocks_types::mv_aggregate_layout::MvAggregateRuntimeLayout,
    pub(crate) columns: Vec<TableColumnDef>,
    pub(crate) partition_fields: Vec<IcebergPartitionFieldExpr>,
    pub(crate) target_properties: Vec<(String, String)>,
}
impl IcebergMvCreatePlan {
    pub(crate) fn staged_write_fields(
        &self,
        handle: &novarocks_spi::connector::ConnectorStagedTableHandle,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorWriteFieldRequest>, MvApplicationError> {
        staged_target_write_fields_from_handle(handle, &self.columns)
    }

    /// Same pure document seed; no stage or Provider permission is created.
    pub(crate) fn projection_seed(
        &self,
        statement: &MvCreateStatement,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<MvCreateProjectionSeed, String> {
        Ok(MvCreateProjectionSeed {
            definition: CreateMvDefinitionRequest {
                query_definition:
                    novarocks_query_application::persisted_query_definition::PersistedQueryDefinition::new(
                        statement.select_sql.clone(),
                        novarocks_query_application::persisted_query_definition::PersistedQueryDialect::StarRocks,
                        current_catalog.unwrap_or("default_catalog"),
                        current_database,
                    )
                    .map_err(|error| error.to_string())?,
                base_table_refs: self.base_refs.iter().map(TableIdentity::fqn).collect(),
                primary_key_columns: statement.primary_key.clone().unwrap_or_default(),
                storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
                target_catalog: Some(self.target.catalog.clone()),
                target_namespace: Some(self.target.namespace.clone()),
                target_table: Some(self.target.table.clone()),
                created_at_ms: self.created_at_ms,
            },
            refresh: initial_refresh_configuration_for_create(&statement.refresh_policy),
            dependencies: self.dependencies.clone(),
        })
    }
}

fn initial_refresh_configuration_for_create(
    policy: &crate::mv::domain::application::MvCreateRefreshPolicy,
) -> novarocks_mv_application::repository::InitialMvRefreshConfiguration {
    let (policy, interval_ms) = match policy {
        crate::mv::domain::application::MvCreateRefreshPolicy::Manual => {
            (MvDesiredRefreshPolicy::Manual, None)
        }
        crate::mv::domain::application::MvCreateRefreshPolicy::AsyncOnChange => {
            (MvDesiredRefreshPolicy::AsyncOnChange, None)
        }
        crate::mv::domain::application::MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            (MvDesiredRefreshPolicy::AsyncInterval, Some(*interval_ms))
        }
    };
    novarocks_mv_application::repository::InitialMvRefreshConfiguration {
        policy,
        paused: false,
        interval_ms,
        max_staleness_ms: None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn partition_fields_for_create(
    fields: Option<&Vec<crate::mv::domain::application::MvCreatePartitionField>>,
) -> Vec<IcebergPartitionFieldExpr> {
    fields
        .into_iter()
        .flatten()
        .map(|field| match field {
            crate::mv::domain::application::MvCreatePartitionField::Identity { column } => {
                IcebergPartitionFieldExpr::Identity {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Year { column } => {
                IcebergPartitionFieldExpr::Year {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Month { column } => {
                IcebergPartitionFieldExpr::Month {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Day { column } => {
                IcebergPartitionFieldExpr::Day {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Hour { column } => {
                IcebergPartitionFieldExpr::Hour {
                    column: column.clone(),
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Bucket {
                column,
                num_buckets,
            } => IcebergPartitionFieldExpr::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            crate::mv::domain::application::MvCreatePartitionField::Truncate { column, width } => {
                IcebergPartitionFieldExpr::Truncate {
                    column: column.clone(),
                    width: *width,
                }
            }
            crate::mv::domain::application::MvCreatePartitionField::Void { column } => {
                IcebergPartitionFieldExpr::Void {
                    column: column.clone(),
                }
            }
        })
        .collect()
}

pub(crate) struct MvCreateShape {
    pub(crate) target: IcebergMvTarget,
    pub(crate) canonical_select_query: ast::Query,
    pub(crate) analysis: MvAnalysis,
    pub(crate) refresh_contract: ImvRefreshContract,
    pub(crate) partition_fields: Vec<IcebergPartitionFieldExpr>,
    pub(crate) created_at_ms: i64,
    pub(crate) resolved_dependencies:
        crate::mv::domain::dependency_resolver::ResolvedCreateMvDependencies,
}

pub(crate) fn prepare_mv_create_target(
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &MvCreateStatement,
) -> Result<IcebergMvTarget, novarocks_sql::compiler::SqlCompileError> {
    let storage_engine = stmt
        .properties
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("storage_engine"))
        .map(|(_, value)| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "iceberg".to_string());
    match storage_engine.as_str() {
        "iceberg" => {}
        "starrocks" => return Err(("storage_engine='starrocks' is no longer supported for standalone materialized views; use storage_engine='iceberg'".to_string()).into()),
        _ => return Err((format!("unknown materialized view storage_engine `{storage_engine}`")).into()),
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
            return Err((format!(
                "materialized view name catalog must be `default_catalog`, got {}",
                normalize_identifier(catalog)?
            ))
            .into());
        }
        _ => {
            return Err(
                ("materialized view name must have one, two, or three parts".to_string()).into(),
            );
        }
    };
    let target = IcebergMvTarget {
        catalog: normalize_identifier(current_catalog)?,
        namespace,
        table,
    };
    Ok(target)
}

pub(crate) fn prepare_mv_create_shape(
    target: IcebergMvTarget,
    stmt: &MvCreateStatement,
    canonical_select_query: ast::Query,
    analysis: MvAnalysis,
    completion_control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<MvCreateShape, novarocks_sql::compiler::SqlCompileError> {
    let refresh_contract = derive_imv_refresh_contract(&analysis, completion_control)?;
    let partition_fields = partition_fields_for_create(stmt.partition_by.as_ref());
    validate_mv_partition_columns(Some(&partition_fields), &analysis.output_columns)?;
    let created_at_ms = now_ms();
    let resolved_dependencies =
        crate::mv::domain::dependency_resolver::resolve_create_mv_dependencies(
            &analysis.resolved_refs,
            created_at_ms,
        )?;
    Ok(MvCreateShape {
        target,
        canonical_select_query,
        analysis,
        refresh_contract,
        partition_fields,
        created_at_ms,
        resolved_dependencies,
    })
}

pub(crate) fn prepare_mv_create_properties(
    shape: &MvCreateShape,
    control: &novarocks_sql::compiler::SqlCompileControl,
) -> Result<
    (
        RefreshFragmentProperty,
        novarocks_sql::planning::mv::SqlMvCreatePersistenceFacts,
    ),
    novarocks_sql::compiler::SqlCompileError,
> {
    Ok((
        derive_fragment_property(&shape.analysis, control)?,
        shape.analysis.refresh_input.create_persistence_facts()?,
    ))
}

pub(crate) fn finish_mv_create_preparation(
    shape: MvCreateShape,
    stmt: &MvCreateStatement,
    property: RefreshFragmentProperty,
    create_persistence_facts: novarocks_sql::planning::mv::SqlMvCreatePersistenceFacts,
    source_field_observations: Vec<
        novarocks_mv_application::persistence::aggregate_bindings::MvCreateRelationObservation,
    >,
    binding_document: novarocks_mv_application::persistence::codec::keyed::BindingDocument,
    base_field_observations: BTreeMap<String, MvSourceSchemaObservation>,
) -> Result<Arc<IcebergMvCreatePlan>, novarocks_sql::compiler::SqlCompileError> {
    let MvCreateShape {
        target,
        canonical_select_query,
        analysis,
        refresh_contract,
        partition_fields,
        created_at_ms,
        resolved_dependencies,
    } = shape;
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
            TargetIdentity::JoinRowKey(_, _) => return Err(("iceberg-backed join materialized views do not support PRIMARY KEY in this phase".to_string()).into()),
            TargetIdentity::BranchScoped(_) => return Err(("iceberg-backed UNION ALL materialized views do not support PRIMARY KEY in this phase".to_string()).into()),
            TargetIdentity::GroupRowId(_) => return Err(("iceberg-backed aggregate materialized views do not support PRIMARY KEY".to_string()).into()),
        }
    }
    if !partition_fields.is_empty() && property.is_composed_aggregate_schema_contract_fallback() {
        return Err(
            ("partitioned composed aggregate Iceberg MV is not supported".to_string()).into(),
        );
    }
    let apply_key_column_name = refresh_contract.apply_key.column_name;
    if analysis
        .output_columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(apply_key_column_name))
    {
        return Err((format!(
            "Iceberg MV output column name {apply_key_column_name} is reserved for internal apply key"
        )).into());
    }
    if identity_needs_branch_id_column(&property.identity)
        && analysis
            .output_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(BRANCH_ID_COLUMN_NAME))
    {
        return Err((format!(
            "Iceberg MV output column name {BRANCH_ID_COLUMN_NAME} is reserved for internal branch id"
        )).into());
    }
    let mut columns =
        create_target_columns_from_property(&property, &canonical_select_query, &analysis)?;
    if identity_needs_physical_apply_key_column(&property.identity) {
        columns.push(create_apply_key_table_column(&refresh_contract.apply_key)?);
    }
    let branch_id_column_name = identity_needs_branch_id_column(&property.identity).then(|| {
        columns.push(branch_id_table_column());
        BRANCH_ID_COLUMN_NAME.to_string()
    });
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
    let aggregate_runtime_layout =
        representative_aggregate_layout(&property, &canonical_select_query, &analysis)?
            .map(|layout| layout.runtime_layout().clone())
            .unwrap_or_else(|| {
                novarocks_types::mv_aggregate_layout::MvAggregateRuntimeLayout::try_new(
                    "__row_id__".to_string(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
                .expect("empty aggregate layout is a valid stateless CREATE carrier")
            });
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
    Ok(Arc::new(IcebergMvCreatePlan {
        target,
        refresh_contract,
        property,
        base_refs: resolved_dependencies.base_refs,
        dependencies: resolved_dependencies.dependencies,
        create_persistence_facts,
        branch_id_column_name,
        source_field_observations,
        binding_document,
        columns,
        aggregate_runtime_layout,
        partition_fields,
        target_properties,
        created_at_ms,
    }))
}

fn create_declaration_error(message: String) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}

fn staged_target_write_fields_from_handle(
    handle: &novarocks_spi::connector::ConnectorStagedTableHandle,
    columns: &[TableColumnDef],
) -> Result<Vec<novarocks_spi::connector::ConnectorWriteFieldRequest>, MvApplicationError> {
    let prepared = handle.document_target().ok_or_else(|| {
        create_declaration_error(
            "staged MV CREATE target carries no prepared document binding".to_string(),
        )
    })?;
    if prepared.fields().len() != columns.len() {
        return Err(create_declaration_error(
            "MV staged target declaration arity changed".to_string(),
        ));
    }
    prepared
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let declaration = columns
                .get(field.request_ordinal() as usize)
                .ok_or_else(|| {
                    create_declaration_error(
                        "MV staged target request ordinal is absent".to_string(),
                    )
                })?;
            if field.request_ordinal() as usize != ordinal
                || field.name() != declaration.name
                || field.nullable() != declaration.nullable
            {
                return Err(create_declaration_error(
                    "MV staged target declaration changed".to_string(),
                ));
            }
            let data_type = novarocks_sql::literal::sql_type_to_arrow_type(&declaration.data_type)
                .map_err(create_declaration_error)?;
            let binding = novarocks_types::execution_value_type_from_declaration(
                &declaration.data_type,
                &data_type,
                declaration.nullable,
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
            .map_err(|error| create_declaration_error(error.to_string()))?;
            let value_type =
                novarocks_type_contract::FunctionValueType::try_from_typed_binding(binding)
                    .map_err(|error| create_declaration_error(error.to_string()))?;
            novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                arrow::datatypes::Field::new(
                    field.name(),
                    value_type.data_type().clone(),
                    field.nullable(),
                ),
                value_type,
            )
            .map_err(|error| create_declaration_error(error.to_string()))
        })
        .collect()
}

/// Project the CREATE-time refresh configuration into its canonical document.
///
/// C is an independent document: it carries only the desired refresh policy
/// and never a publication or schema fact.
fn create_configuration_document(
    refresh: &novarocks_mv_application::repository::InitialMvRefreshConfiguration,
) -> Result<novarocks_mv_application::persistence::codec::ConfigurationDocument, String> {
    use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};

    fn non_negative(value: Option<i64>, what: &str) -> Result<Option<u64>, String> {
        value
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| format!("MV CREATE {what} must not be negative, got {value}"))
            })
            .transpose()
    }

    Ok(ConfigurationDocument {
        refresh_policy: match refresh.policy {
            MvDesiredRefreshPolicy::Manual => RefreshPolicy::Manual,
            MvDesiredRefreshPolicy::AsyncOnChange => RefreshPolicy::AsyncOnChange,
            MvDesiredRefreshPolicy::AsyncInterval => RefreshPolicy::AsyncInterval,
        },
        paused: refresh.paused,
        refresh_interval_ms: non_negative(refresh.interval_ms, "refresh interval")?,
        max_staleness_ms: non_negative(refresh.max_staleness_ms, "max staleness")?,
    })
}

/// Build this CREATE's canonical D/L/C from the facts frozen during
/// preparation and the provider's own prepared staged target.
///
/// The target facts come only from the staged-create handle. A post-create
/// reload is deliberately not accepted here: it would describe a generation
/// this statement did not stage.
pub(crate) fn build_create_documents_for_prepared_target(
    prepared: &IcebergMvCreatePlan,
    seed: &crate::mv::domain::application::MvCreateProjectionSeed,
    prepared_target: &novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget,
) -> Result<novarocks_mv_application::persistence::create_documents::MvCreateDocuments, String> {
    let created_at_ms = u64::try_from(prepared.created_at_ms).map_err(|_| {
        format!(
            "MV CREATE time must be a non-negative epoch millisecond, got {}",
            prepared.created_at_ms
        )
    })?;
    novarocks_mv_application::persistence::create_documents::build_mv_create_documents(
        novarocks_mv_application::persistence::create_documents::MvCreateDocumentFacts {
            created_at_ms,
            query_definition: seed.definition.query_definition.clone(),
            sql_facts: &prepared.create_persistence_facts,
            runtime_layout: &prepared.aggregate_runtime_layout,
            source_observations: &prepared.source_field_observations,
            prepared_target,
            target_identity: &prepared.property.identity,
            apply_key_column_name: prepared.refresh_contract.apply_key.column_name,
            branch_column_name: prepared.branch_id_column_name.as_deref(),
            configuration: create_configuration_document(&seed.refresh)?,
        },
    )
}

fn base_table_descriptor_from_observation(
    observation: &MvSourceSchemaObservation,
) -> BaseTableDescriptor {
    BaseTableDescriptor {
        format_version: if observation.is_format_v3() { 3 } else { 2 },
        columns: observation
            .fields()
            .iter()
            .map(|field| BaseColumnDescriptor {
                name: field.name.clone(),
                data_type: DataType::Null,
                sql_type: observed_iceberg_type_sql_head(&field.type_signature),
                nullable: field.nullable,
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

/// Validate a branch UNION ALL aggregate definition against one exact target
/// observation. Branch count, inner apply-key kind and the branch
/// discriminator all come from L; there is no separate persisted contract
/// version to compare.
fn validate_branch_union_contract(
    target: &IcebergMvTarget,
    projection: &StoredKeyedMvProjection,
    query_branch_count: usize,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let interpretation = projection.facts.interpretation();
    if interpretation.aggregates.is_empty() {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} has no aggregate interpretation; recreate the MV",
            target.catalog, target.namespace, target.table
        ));
    }
    if interpretation.branches.len() != query_branch_count {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} interpretation has {} branches, query has {}",
            target.catalog,
            target.namespace,
            target.table,
            interpretation.branches.len(),
            query_branch_count
        ));
    }
    if interpretation.apply_key.kind != ApplyKeyKind::GroupRowId {
        return Err(format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} must use GroupRowId inner apply keys",
            target.catalog, target.namespace, target.table
        ));
    }
    validate_branch_id_field(projection, target_observation).map_err(|error| {
        format!(
            "iceberg branch UNION ALL aggregate MV {}.{}.{} branch binding is invalid: {error}",
            target.catalog, target.namespace, target.table
        )
    })
}

/// Validate one D relation occurrence of a UNION ALL projection/filter MV
/// against its exact source observation and the exact target generation.
///
/// `branch_count` is the query's own branch count; L's branch interpretations
/// are the persisted counterpart, so a mismatch is still an error.
fn validate_union_projection_schema_contract_for_base(
    iceberg_target: &IcebergMvTarget,
    projection: &StoredKeyedMvProjection,
    branch_count: usize,
    occurrence: &RelationOccurrence,
    base_observation: &MvSourceSchemaObservation,
    target_observation: &MvSchemaValidationObservation,
) -> Result<(), String> {
    let interpretation = projection.facts.interpretation();
    if interpretation.branches.len() != branch_count {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} interpretation has {} branches, query has {}",
            iceberg_target.catalog,
            iceberg_target.namespace,
            iceberg_target.table,
            interpretation.branches.len(),
            branch_count
        ));
    }
    if interpretation.apply_key.kind != ApplyKeyKind::BaseRowId {
        return Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} must use BaseRowId inner apply keys",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ));
    }
    validate_branch_id_field(projection, target_observation).map_err(|error| {
        format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} branch binding is invalid: {error}",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        )
    })?;
    let renames =
        validate_schema_contract(projection, occurrence, base_observation, target_observation)?;
    if renames.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "iceberg UNION ALL projection/filter MV {}.{}.{} requires schema rebind, which is not supported for UNION ALL refresh; rebuild or recreate the MV",
            iceberg_target.catalog, iceberg_target.namespace, iceberg_target.table
        ))
    }
}
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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
    canonical_query: &ast::Query,
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
    canonical_query: &ast::Query,
    analysis: &MvAnalysis,
) -> Result<Vec<String>, String> {
    let Some(layout) = representative_aggregate_layout(property, canonical_query, analysis)? else {
        return Ok(Vec::new());
    };
    Ok(layout
        .runtime_layout()
        .state_columns()
        .iter()
        .map(|column| column.name().to_string())
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
    canonical_query: &ast::Query,
    analysis: &MvAnalysis,
) -> Result<
    Option<novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout>,
    String,
> {
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
            novarocks_sql::planning::mv_aggregate_layout::build_sql_mv_aggregate_physical_layout(
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

fn create_apply_key_source_property(apply_key: &ApplyKeyContract) -> &'static str {
    mv_apply_key_source_from_column_name(apply_key.column_name)
        .expect("known Iceberg MV apply-key column")
        .table_property_value()
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn refresh_policy_descriptor_json(
    policy: &MvCreateRefreshPolicy,
    paused: bool,
) -> serde_json::Value {
    match policy {
        MvCreateRefreshPolicy::Manual => serde_json::json!({
            "policy": "DEFERRED_MANUAL",
            "interval_ms": null,
            "paused": paused,
        }),
        MvCreateRefreshPolicy::AsyncOnChange => {
            serde_json::json!({
                "policy": "ASYNC_ON_CHANGE",
                "interval_ms": null,
                "paused": paused,
            })
        }
        MvCreateRefreshPolicy::AsyncInterval { interval_ms } => {
            serde_json::json!({
                "policy": "ASYNC_INTERVAL",
                "interval_ms": interval_ms,
                "paused": paused,
            })
        }
    }
}

pub(crate) fn configuration_document(
    desired: novarocks_mv_application::persistence::semantic::MvRefreshDesiredConfiguration,
) -> Result<novarocks_mv_application::persistence::codec::ConfigurationDocument, String> {
    use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};
    let positive = |value: Option<i64>, field: &str| {
        value
            .map(|v| u64::try_from(v).map_err(|_| format!("MV {field} must not be negative")))
            .transpose()
    };
    Ok(ConfigurationDocument {
        refresh_policy: match desired.policy {
            MvDesiredRefreshPolicy::Manual => RefreshPolicy::Manual,
            MvDesiredRefreshPolicy::AsyncOnChange => RefreshPolicy::AsyncOnChange,
            MvDesiredRefreshPolicy::AsyncInterval => RefreshPolicy::AsyncInterval,
        },
        paused: desired.paused,
        refresh_interval_ms: positive(desired.interval_ms, "refresh interval")?,
        max_staleness_ms: positive(desired.max_staleness_ms, "maximum staleness")?,
    })
}

pub(crate) fn configuration_effect_disposition(
    resolved: &crate::connector::mutation::ResolvedCatalogMutation,
) -> novarocks_mv_application::management::EffectDisposition {
    use crate::connector::mutation::{MutationDispatchState, ResolvedCatalogMutation};
    use novarocks_mv_application::management::EffectDisposition;

    match resolved {
        ResolvedCatalogMutation::KnownCommitted(_) => EffectDisposition::KnownCommitted,
        ResolvedCatalogMutation::KnownUncommitted { .. }
        | ResolvedCatalogMutation::ContractFailure {
            dispatch: MutationDispatchState::ConfirmedNotDispatched,
            ..
        } => EffectDisposition::KnownUncommitted,
        ResolvedCatalogMutation::CommitUnknown { .. }
        | ResolvedCatalogMutation::ContractFailure {
            dispatch: MutationDispatchState::PossiblyDispatched,
            ..
        } => EffectDisposition::CommitUnknown,
    }
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
    base: &RefreshBaseRelationOccurrence,
    previous_snapshot_id: Option<i64>,
    current_snapshot_id_before_pin: Option<i64>,
) -> BaseSnapshotStatus {
    BaseSnapshotStatus::new(
        // Named by occurrence as well as by table: in a self-join the table
        // name alone would report both mentions under one name, and the whole
        // point of the status is to say which one moved.
        base.display(),
        previous_snapshot_id,
        current_snapshot_id_before_pin,
    )
}

fn iceberg_aggregate_target_columns_from_layout(
    layout: &novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout,
) -> Result<Vec<TableColumnDef>, String> {
    novarocks_sql::planning::mv_aggregate_layout::validate_unique_aggregate_physical_column_names(
        layout.physical_columns(),
    )?;
    Ok(layout
        .physical_columns()
        .iter()
        .map(|column| column.column().clone())
        .collect())
}

/// Whether an aggregate query's FROM clause is a fan-in UNION ALL subquery.
///
/// FROM-side complement to [`extract_aggregate_sql_calls`] for distinguishing a
/// fan-in aggregate from a single-scan aggregate. A fan-in FROM is exactly one
/// relation, no joins, a non-lateral derived subquery whose body is a `UNION ALL`
/// set operation.
fn from_clause_is_fan_in_union(query: &ast::Query) -> bool {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    if !from.joins.is_empty() {
        return false;
    }
    let ast::TableFactor::Derived {
        lateral, subquery, ..
    } = &from.relation
    else {
        return false;
    };
    if *lateral {
        return false;
    }
    matches!(subquery.body.as_ref(), ast::SetExpr::SetOperation(_))
}

fn validate_composed_aggregate_fallback_query(query: &ast::Query) -> Result<(), String> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
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
        validate_composed_aggregate_join(&join.operator, &join.constraint)?;
        validate_composed_aggregate_table_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_composed_aggregate_table_factor(factor: &ast::TableFactor) -> Result<(), String> {
    match factor {
        ast::TableFactor::Table { .. } => Ok(()),
        ast::TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_composed_aggregate_table_factor(&table_with_joins.relation)?;
            if table_with_joins.joins.is_empty() {
                return Err(
                    "composed aggregate fallback nested join must contain at least one join"
                        .to_string(),
                );
            }
            for join in &table_with_joins.joins {
                validate_composed_aggregate_join(&join.operator, &join.constraint)?;
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

fn validate_composed_aggregate_join(
    operator: &ast::JoinOperator,
    constraint: &ast::JoinConstraint,
) -> Result<(), String> {
    match (operator, constraint) {
        (ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit, ast::JoinConstraint::On(_)) => Ok(()),
        (ast::JoinOperator::Cross, ast::JoinConstraint::None) => Ok(()),
        _ => Err(
            "composed aggregate fallback supports only direct INNER JOIN ... ON predicates or CROSS JOIN"
                .to_string(),
        ),
    }
}

/// Number of UNION ALL branches in `query`, counted off the AST so the build
/// does not depend on a top-level classified shape.
fn union_branch_count(query: &ast::Query) -> u32 {
    fn count(body: &ast::SetExpr) -> u32 {
        match body {
            ast::SetExpr::SetOperation(operation) => {
                count(&operation.left) + count(&operation.right)
            }
            ast::SetExpr::Query(inner) => count(inner.body.as_ref()),
            _ => 1,
        }
    }
    count(query.body.as_ref())
}

/// Neutral schema observation for `base_ref`.
///
/// Fails closed: a base the caller did not observe is a programming error, not
/// a reason to fall back to reading provider metadata.
fn observed_base<'a>(
    base_field_observations: &'a std::collections::BTreeMap<
        String,
        crate::mv::domain::storage_observation::MvSourceSchemaObservation,
    >,
    base_ref: &TableIdentity,
) -> Result<&'a crate::mv::domain::storage_observation::MvSourceSchemaObservation, String> {
    base_field_observations.get(&base_ref.fqn()).ok_or_else(|| {
        format!(
            "MV base {} was not observed before contract build",
            base_ref.fqn()
        )
    })
}

/// The state an Iceberg MV target restore reads, named explicitly rather than
/// reached through aggregate engine state.
///
/// Same shape and motive as the lake rebuild's context: two inputs, both already
/// reachable from a frontend composition, so restoring targets stops requiring
/// the engine.
pub struct MvTargetRestoreContext<'a> {
    pub connector_control: &'a dyn novarocks_spi::connector::ConnectorControlRegistry,
    pub readiness: &'a MvReadinessPort,
}

pub(crate) fn register_iceberg_mv_target_in_catalog(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
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
        connector_control,
        &instance_id,
    )
    .map_err(|error| format!("acquire MV target connector generation: {error}"))?;
    Ok(())
}

pub fn restore_iceberg_mv_targets(ctx: &MvTargetRestoreContext<'_>) -> Result<(), String> {
    // Restore every MV this process holds a projection for, not only the ones
    // it may manage. A projection rebuilt from the lake is a query candidate
    // immediately and stays closed to management until its readmission
    // completes; consuming the management-ready inventory here would leave a
    // restarted process unable to confirm the generation of its own MVs until
    // an operator intervened.
    let projections = match ctx
        .readiness
        .candidate_reader()
        .list_candidate_definitions()
    {
        Ok(projections) => projections,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "skipping Iceberg MV target restore because the Accelerator is unavailable"
            );
            return Ok(());
        }
    };
    for projection in projections {
        let source = projection.facts.source_revision();
        let target = IcebergMvTarget {
            catalog: source.target.instance_id.as_str().to_string(),
            namespace: source.target.namespace.to_string(),
            table: source.target.table.to_string(),
        };
        if let Err(error) = register_iceberg_mv_target_in_catalog(ctx.connector_control, &target) {
            tracing::warn!(
                mv_id = projection.mv_id,
                target = %format!("{}.{}.{}", target.catalog, target.namespace, target.table),
                error = %error,
                "skipping failed Iceberg MV target registration during startup restore"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::refresh::apply_key::ApplyKeyValueType;
    use crate::mv::domain::refresh::capabilities::PartitionPruningPolicy;

    #[test]
    fn configuration_dispatch_contract_failure_keeps_unknown_barrier() {
        let failure = crate::connector::mutation::ResolvedCatalogMutation::ContractFailure {
            error: ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "transport outcome unavailable",
            ),
            dispatch: crate::connector::mutation::MutationDispatchState::PossiblyDispatched,
        };
        assert_eq!(
            configuration_effect_disposition(&failure),
            novarocks_mv_application::management::EffectDisposition::CommitUnknown
        );
    }

    #[test]
    fn aggregate_incremental_inserts_use_row_delta() {
        assert!(matches!(
            non_join_incremental_write_mode(true, false),
            MvIncrementalWriteMode::RowDelta
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, false),
            MvIncrementalWriteMode::FastAppend
        ));
        assert!(matches!(
            non_join_incremental_write_mode(false, true),
            MvIncrementalWriteMode::RowDelta
        ));
    }
    #[test]
    fn imv_change_stream_effect_set_can_include_zero_row_route() {
        let effects = [
            novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
        ];
        assert!(effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Delete));
        assert!(effects.contains(&novarocks_spi::connector::ConnectorRowMutationEffect::Replace));
    }

    /// D's own effective SQL, not the order the caller happens to pass base
    /// refs in, decides which relation is the join's left side. The retired
    /// persisted join lineage is gone; the two-relation gate now comes from D's
    /// occurrence inventory.
    #[test]
    fn join_base_refs_for_definition_uses_the_definition_join_order() {
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
        // D records `fact` at occurrence 0 under `l` and `dim` at occurrence 1
        // under `r`; the base list is in that same order.
        let base_refs = vec![
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(0),
                table: left.clone(),
            },
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(1),
                table: right.clone(),
            },
        ];
        let projection = test_two_relation_projection(&left, &right);
        let query = parse_select_query(
            "SELECT l.order_id FROM ice.sales.dim r JOIN ice.sales.fact l ON l.order_id = r.order_id",
        );

        let (actual_left, actual_right) =
            join_base_refs_for_definition(&projection, &query, &base_refs)
                .expect("definition join base refs");

        // The definition puts `r` on the left, so the resolved left side is
        // `dim` even though the base list starts with `fact`.
        assert_eq!(actual_left.table.fqn(), right.fqn());
        assert_eq!(actual_right.table.fqn(), left.fqn());
    }

    /// A self-join has two D occurrences of one relation, and the qualifier is
    /// the only thing that tells them apart. Resolving by table would have to
    /// pick one; resolving by qualifier does not have to choose.
    #[test]
    fn join_base_refs_for_definition_resolves_a_self_join_by_qualifier() {
        let table = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
        };
        let base_refs = vec![
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(0),
                table: table.clone(),
            },
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(1),
                table: table.clone(),
            },
        ];
        let projection = test_two_relation_projection(&table, &table);
        let query = parse_select_query(
            "SELECT l.order_id FROM ice.sales.fact l JOIN ice.sales.fact r ON l.order_id = r.order_id",
        );

        let (left, right) = join_base_refs_for_definition(&projection, &query, &base_refs)
            .expect("a self join's sides are two occurrences of one table");

        assert_eq!(left.occurrence_id, SqlMvRelationOccurrenceId::new(0));
        assert_eq!(right.occurrence_id, SqlMvRelationOccurrenceId::new(1));
        assert_eq!(left.table.fqn(), right.table.fqn());
    }

    /// Two sides bound under one qualifier is the definition being ambiguous
    /// about itself, which no amount of resolving can fix.
    #[test]
    fn join_base_refs_for_definition_refuses_one_qualifier_for_both_sides() {
        let table = TableIdentity {
            catalog: "ice".to_string(),
            namespace: "sales".to_string(),
            table: "fact".to_string(),
        };
        let base_refs = vec![
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(0),
                table: table.clone(),
            },
            RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(1),
                table: table.clone(),
            },
        ];
        let projection =
            test_two_relation_projection_with_qualifiers(&table, &table, "fact", "fact");
        let query = parse_select_query(
            "SELECT fact.order_id FROM ice.sales.fact JOIN ice.sales.fact ON true",
        );

        let error = join_base_refs_for_definition(&projection, &query, &base_refs)
            .expect_err("one qualifier cannot name two sides");

        assert!(error.contains("qualifier"), "{error}");
    }

    /// Two canonical D occurrences bound to the given locators. Every other
    /// fact (outputs, interpretation, target binding) stays at the shared
    /// fixture so only the relation shape under test varies.
    fn test_two_relation_projection(
        left: &TableIdentity,
        right: &TableIdentity,
    ) -> StoredKeyedMvProjection {
        test_two_relation_projection_with_qualifiers(left, right, "l", "r")
    }

    fn test_two_relation_projection_with_qualifiers(
        left: &TableIdentity,
        right: &TableIdentity,
        left_qualifier: &str,
        right_qualifier: &str,
    ) -> StoredKeyedMvProjection {
        let mut fixture = crate::mv::domain::test_admission::KeyedProjectionFixture::new(
            novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        for (occurrence, (table, qualifier)) in fixture
            .definition
            .relation_occurrences
            .iter_mut()
            .zip([(left, left_qualifier), (right, right_qualifier)])
        {
            occurrence.catalog_at_binding.clone_from(&table.catalog);
            occurrence.namespace_at_binding.clone_from(&table.namespace);
            occurrence.relation_at_binding.clone_from(&table.table);
            occurrence.qualifier_at_binding = qualifier.to_string();
        }
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("two-relation fixture projection"),
        }
    }

    fn parse_select_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(q)] = statements.as_slice() else {
            panic!("expected SELECT");
        };
        q.clone()
    }

    #[test]
    fn iceberg_join_mv_uses_join_apply_key_column() {
        let column = crate::mv::domain::refresh::target_apply::join_apply_key_table_column();
        assert_eq!(column.name, JOIN_APPLY_KEY_COLUMN_NAME);
    }

    #[test]
    fn create_apply_key_metadata_comes_from_refresh_contract() {
        use crate::mv::domain::refresh::apply_key::ApplyKeyContract;

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
        use crate::mv::domain::analysis::refresh_property::TargetIdentity;

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
    fn refresh_status_names_the_table_and_which_mention_of_it() {
        let base = RefreshBaseRelationOccurrence {
            occurrence_id: SqlMvRelationOccurrenceId::new(1),
            table: TableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
        };

        let status = base_snapshot_status_for_refresh(&base, Some(10), Some(11));

        // Both, because a self-join would otherwise report two mentions of one
        // table under one name.
        assert_eq!(status.fqn, "ice.sales.orders (occurrence 1)");
        assert_eq!(status.previous_snapshot_id, Some(10));
        assert_eq!(status.current_snapshot_id_before_pin, Some(11));
    }
}

// A-family aggregate-union execution is orchestrated by the shared fan-in
// refresh path below. It pins every base, builds one refresh context, and
// drives the aggregate merge with one canonical change per base.

/// The per-shape payload distinguishing the two `AllBasesRequired` aggregate
/// refresh variants that share the wrapper [`refresh_fan_in_aggregate_iceberg_mv`].
///
/// This enum is the *identity gate* the folded wrapper dispatches on: the
/// `BranchUnion` variant corresponds to a `BranchScoped` row identity (UNION
/// ALL of aggregate branches), while `FanIn` corresponds to a plain
/// `GroupRowId` aggregate fanning in over a UNION ALL of scans. Both produce an
/// `AllBasesRequired` snapshot policy and an aggregate state contract; only the
/// branch-contract validation and the first-refresh strategy differ.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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

fn unknown_join_affected_partitions() -> crate::mv::domain::model::AffectedTargetPartitions {
    crate::mv::domain::model::AffectedTargetPartitions::not_derived(
        "join MV affected partition planning is not implemented",
    )
}

fn base_snapshot_statuses_for_plan(
    bases: &[RefreshBaseRelationOccurrence],
    previous_snapshots: &BTreeMap<SqlMvRelationOccurrenceId, i64>,
    current_snapshots: &BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>,
) -> Vec<BaseSnapshotStatus> {
    bases
        .iter()
        .map(|base| {
            base_snapshot_status_for_refresh(
                base,
                previous_snapshots.get(&base.occurrence_id).copied(),
                current_snapshots
                    .get(&base.occurrence_id)
                    .copied()
                    .flatten(),
            )
        })
        .collect()
}

fn noop_affected_partitions(
    target_partition: &mv_schema::MvPartitionContract,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    if is_unpartitioned_mv_target(target_partition) {
        crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
    } else {
        crate::mv::domain::model::AffectedTargetPartitions::known(std::iter::empty::<
            crate::mv::domain::model::MvPartitionKey,
        >())
    }
}

fn merge_affected_partition_results(
    context: &str,
    results: impl IntoIterator<Item = (String, crate::mv::domain::model::AffectedTargetPartitions)>,
) -> crate::mv::domain::model::AffectedTargetPartitions {
    let mut merged = BTreeSet::new();
    let mut saw_unpartitioned = false;

    for (base, result) in results {
        match result {
            crate::mv::domain::model::AffectedTargetPartitions::Known { partitions } => {
                merged.extend(partitions);
            }
            crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned => {
                saw_unpartitioned = true;
            }
            crate::mv::domain::model::AffectedTargetPartitions::NotDerived { reason } => {
                return crate::mv::domain::model::AffectedTargetPartitions::not_derived(format!(
                    "{context}: {base}: {reason}"
                ));
            }
        }
    }

    if saw_unpartitioned {
        if merged.is_empty() {
            crate::mv::domain::model::AffectedTargetPartitions::Unpartitioned
        } else {
            crate::mv::domain::model::AffectedTargetPartitions::not_derived(format!(
                "{context}: mixed unpartitioned and partitioned branch results"
            ))
        }
    } else {
        crate::mv::domain::model::AffectedTargetPartitions::known(merged)
    }
}

/// Whether the MV target is unpartitioned.
///
/// The retired schema contract carried its own copy of the target spec. The
/// canonical documents keep only the provider-opaque partition-spec version,
/// which must never be decoded, so this fact now comes from the typed
/// partition observation the exact target binding already carries.
fn is_unpartitioned_mv_target(target_partition: &mv_schema::MvPartitionContract) -> bool {
    target_partition.fields.is_empty()
}

fn log_planned_iceberg_mv_affected_partitions(
    iceberg_target: &IcebergMvTarget,
    affected_partitions: &crate::mv::domain::model::AffectedTargetPartitions,
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

pub(crate) fn build_refresh_state_baseline(
    mv_definition: &StoredKeyedMvProjection,
    target: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
) -> Result<RefreshStateBaseline, String> {
    let definition = mv_definition.facts.definition();
    let previous_sources = match mv_definition.facts.publication() {
        None => Vec::new(),
        Some(publication) => {
            if publication.inputs.len() != definition.relation_occurrences.len() {
                return Err(
                    "MV publication inputs do not cover every D relation occurrence".into(),
                );
            }
            definition
                .relation_occurrences
                .iter()
                .zip(&publication.inputs)
                .map(|(occurrence, input)| {
                    if occurrence.occurrence_id != input.relation_occurrence_id {
                        return Err(format!(
                            "MV publication input order differs at D occurrence {}",
                            occurrence.occurrence_id,
                        ));
                    }
                    if occurrence.object_id != input.object_id {
                        return Err(format!(
                            "MV publication input object differs from D occurrence {}",
                            occurrence.occurrence_id,
                        ));
                    }
                    let semantic_revision =
                        restore_exact_query_revision(&input.object_id, &input.native_data_version)
                            .map_err(|error| {
                                format!(
                                    "restore exact MV publication input occurrence {}: {error}",
                                    occurrence.occurrence_id,
                                )
                            })?;
                    Ok(RefreshStateBaselineSource {
                        occurrence_id: novarocks_sql::compiler::SqlMvRelationOccurrenceId::new(
                            occurrence.occurrence_id,
                        ),
                        table: TableIdentity {
                            catalog: occurrence.catalog_at_binding.clone(),
                            namespace: occurrence.namespace_at_binding.clone(),
                            table: occurrence.relation_at_binding.clone(),
                        },
                        semantic_revision,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?
        }
    };
    Ok(RefreshStateBaseline::SnapshotBacked {
        previous_sources,
        target_snapshot_id: target.current_snapshot_id(),
        target_table_uuid: target.table_uuid().to_string(),
        definition_fingerprint: mv_definition_fingerprint(&definition.query.effective_sql),
    })
}

/// D's binding-time locator for one relation occurrence.
fn occurrence_table(occurrence: &RelationOccurrence) -> TableIdentity {
    TableIdentity {
        catalog: occurrence.catalog_at_binding.clone(),
        namespace: occurrence.namespace_at_binding.clone(),
        table: occurrence.relation_at_binding.clone(),
    }
}

/// D's ordered relation occurrences, as the base locators refresh planning
/// speaks. The order is D occurrence order, which every canonical validator
/// requires, and which is what pairs each entry back with the occurrence it
/// came from. A relation named twice is two entries: two mentions of one table
/// are two sources, and everything downstream keys them by occurrence.
fn definition_base_refs(
    projection: &StoredKeyedMvProjection,
) -> Result<Vec<TableIdentity>, String> {
    Ok(projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .map(occurrence_table)
        .collect())
}

/// D's relation shape decides whether one empty source may still refresh: a
/// two-relation join pair may, a fan-in of independent relations may not.
fn definition_has_join(query: &ast::Query) -> bool {
    extract_join_aliases(query).is_ok()
}

/// D's occurrences in their own order, proven to describe exactly the base
/// locators this attempt resolved.
fn definition_occurrences_for_base_refs<'a>(
    projection: &'a StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
) -> Result<Vec<&'a RelationOccurrence>, String> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != base_refs.len() {
        return Err("MV refresh base references do not retain every D occurrence".to_string());
    }
    for (occurrence, table) in occurrences.iter().zip(base_refs) {
        if occurrence.catalog_at_binding != table.catalog
            || occurrence.namespace_at_binding != table.namespace
            || occurrence.relation_at_binding != table.table
        {
            return Err(format!(
                "MV refresh base locator does not match D occurrence {}",
                occurrence.occurrence_id
            ));
        }
    }
    Ok(occurrences.iter().collect())
}

/// The same resolution, projected to what the planning contract carries: each
/// base relation paired with the occurrence the documents say it is.
fn base_relation_occurrences(
    projection: &StoredKeyedMvProjection,
    base_refs: &[TableIdentity],
) -> Result<Vec<RefreshBaseRelationOccurrence>, String> {
    Ok(definition_occurrences_for_base_refs(projection, base_refs)?
        .into_iter()
        .zip(base_refs)
        .map(|(occurrence, table)| RefreshBaseRelationOccurrence {
            occurrence_id: SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
            table: table.clone(),
        })
        .collect())
}

/// Applying a renamed source column needs the SQL owner's occurrence-aware
/// rewriter, which is not connected yet. A rename therefore fails closed here
/// exactly as it does in `refresh::observation`.
fn require_no_occurrence_rebind(renames: &[MvOccurrenceFieldRebind]) -> Result<(), String> {
    if renames.is_empty() {
        Ok(())
    } else {
        Err("MV refresh requires occurrence-aware SQL field rebinding".to_string())
    }
}

/// The predecessor facts this snapshot-oriented planner compares against,
/// keyed by the occurrence each was pinned for.
///
/// Occurrences rather than table names, so a definition that reads one table
/// twice keeps one predecessor per mention instead of having the second
/// overwrite the first.
///
/// A baseline with no published predecessor yields empty maps, which is a
/// fact, not a fallback.
struct PreviousRefreshLocators {
    snapshots: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
}

fn build_iceberg_refresh_plan(
    mv_definition: &StoredKeyedMvProjection,
    target: MvTarget,
    _stmt: &MvRefreshRequest,
    _current_catalog: Option<&str>,
    _current_database: &str,
    base_refs: &[RefreshBaseRelationOccurrence],
    snapshot_pins: BTreeMap<SqlMvRelationOccurrenceId, Option<i64>>,
    decision: ExecutableRefreshDecision,
    state_baseline: RefreshStateBaseline,
    affected_partitions: crate::mv::domain::model::AffectedTargetPartitions,
) -> RefreshPlan {
    RefreshPlan {
        contract: RefreshPlanContract {
            mv_id: Some(mv_definition.mv_id),
            target,
            storage_engine: MvStorageEngine::Iceberg,
            decision,
            state_baseline,
            base_refs: base_refs.to_vec(),
            snapshot_pins,
            affected_partitions,
        },
    }
}

#[cfg(test)]
thread_local! {
    static AFTER_CREATE_TARGET_HOOK: std::cell::RefCell<Option<Arc<dyn Fn() + Send + Sync>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterCreateTargetHookGuard;

#[cfg(test)]
impl AfterCreateTargetHookGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
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
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
type AfterObserveBeforeCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterObserveBeforeCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterObserveBeforeCaptureHook,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn after_observe_before_capture_hook_slot()
-> &'static std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<
        std::sync::Mutex<Option<AfterObserveBeforeCaptureHookRegistration>>,
    > = std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct AfterObserveBeforeCaptureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl AfterObserveBeforeCaptureHookGuard {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
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

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn validate_refresh_pin_table_object_ids_against_baseline(
    baseline: &RefreshStateBaseline,
    pin: &RefreshSnapshotPin,
    base_refs: &[TableIdentity],
) -> Result<(), String> {
    let RefreshStateBaseline::SnapshotBacked {
        previous_sources, ..
    } = baseline;
    for previous in previous_sources {
        if !base_refs.iter().any(|base_ref| base_ref == &previous.table) {
            return Err(format!(
                "refresh baseline occurrence {} refers to an unknown D locator {}",
                previous.occurrence_id.get(),
                previous.table.fqn(),
            ));
        }
        let current = pin.get(previous.occurrence_id).ok_or_else(|| {
            format!(
                "refresh pin missing D occurrence {} for base {}",
                previous.occurrence_id.get(),
                previous.table.fqn(),
            )
        })?;
        if current.table() != &previous.table {
            return Err(format!(
                "refresh pin locator differs at D occurrence {}",
                previous.occurrence_id.get(),
            ));
        }
        if previous
            .semantic_revision
            .object_identity()
            .value()
            .as_ref()
            != current.table_object_id().as_bytes().as_ref()
        {
            return Err(format!(
                "iceberg MV base table identity changed for {}; incremental refresh is unsafe, rebuild or recreate the MV",
                previous.table.fqn(),
            ));
        }
    }
    Ok(())
}

/// Resolve a join MV's left and right sides from D's own effective SQL.
///
/// The join order is a property of the definition, so it is reparsed from D
/// rather than read back from a persisted lineage copy. Each side is resolved
/// by the qualifier it was bound under, not by the table it reads: `moves out
/// JOIN moves inb` names one table twice, and only the qualifier says which
/// mention is which. That is also why the result is the occurrence rather than
/// the table -- two sides of a self-join have the same table and differ in
/// nothing else.
pub fn join_base_refs_for_definition<'a>(
    projection: &StoredKeyedMvProjection,
    canonical_query: &ast::Query,
    base_refs: &'a [RefreshBaseRelationOccurrence],
) -> Result<
    (
        &'a RefreshBaseRelationOccurrence,
        &'a RefreshBaseRelationOccurrence,
    ),
    String,
> {
    let occurrences = &projection.facts.definition().relation_occurrences;
    if occurrences.len() != 2 || base_refs.len() != 2 {
        return Err("join MV refresh requires exactly two D relation occurrences".to_string());
    }
    let aliases = extract_join_aliases(canonical_query)?;
    if aliases
        .left_alias
        .eq_ignore_ascii_case(aliases.right_alias.as_str())
    {
        return Err(format!(
            "join MV definition binds both sides under the qualifier {}; each side needs its own",
            aliases.left_alias
        ));
    }
    let side = |alias: &str, label: &str| {
        let matches = occurrences
            .iter()
            .enumerate()
            .filter(|(_, occurrence)| occurrence.qualifier_at_binding.eq_ignore_ascii_case(alias))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => Ok(&base_refs[*index]),
            [] => Err(format!("join MV {label} side {alias} was not resolved")),
            _ => Err(format!(
                "join MV definition binds {} relation occurrences under the qualifier {alias}",
                matches.len()
            )),
        }
    };
    Ok((
        side(aliases.left_alias.as_str(), "left")?,
        side(aliases.right_alias.as_str(), "right")?,
    ))
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn rewrite_snapshot_table_factor(
    factor: &mut ast::TableFactor,
    base: &TableIdentity,
    snapshot_id: i64,
    default_alias: Option<&str>,
) -> Result<(), String> {
    let ast::TableFactor::Table {
        name,
        version,
        alias,
        ..
    } = factor
    else {
        return Err("join snapshot side must be a table".to_string());
    };
    if !object_name_matches_base(name, base)? {
        return Err(format!(
            "join snapshot rewrite expected base {}, got {}",
            base.fqn(),
            novarocks_parser::printer::print_object_name(name)
        ));
    }
    if let Some(version) = version {
        let rendered = novarocks_parser::printer::print_expr(&version.value);
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
        *alias = Some(ast::TableAlias {
            name: generated_ident(default_alias),
            columns: Vec::new(),
            explicit_as: true,
            span: Span::new(0, 0),
        });
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn object_name_matches_base(name: &ast::ObjectName, base: &TableIdentity) -> Result<bool, String> {
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

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn object_name_identifier_parts(name: &ast::ObjectName) -> Vec<String> {
    name.parts.iter().map(|ident| ident.value.clone()).collect()
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn synthetic_snapshot_table_name(base: &TableIdentity, snapshot_id: i64) -> String {
    format!("{}__at_{}", base.table, snapshot_id)
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn synthetic_snapshot_object_name(base: &TableIdentity, snapshot_id: i64) -> ast::ObjectName {
    ast::ObjectName {
        parts: vec![
            generated_ident(&base.namespace),
            generated_ident(&synthetic_snapshot_table_name(base, snapshot_id)),
        ],
        span: Span::new(0, 0),
    }
}

fn generated_ident(value: &str) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted: false,
        quote_style: None,
        span: Span::new(0, 0),
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
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

#[cfg(test)]
mod partition_planning_tests {
    use super::*;

    fn key(value: &str) -> crate::mv::domain::model::MvPartitionKey {
        crate::mv::domain::model::MvPartitionKey::new(
            novarocks_mv_application::persistence::identity::PartitionSpecVersion::try_new(vec![7])
                .unwrap(),
            vec![crate::mv::domain::model::MvPartitionKeyField::new(
                novarocks_mv_application::persistence::identity::FieldIdentity::try_new(vec![9])
                    .unwrap(),
                crate::mv::domain::model::MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn merge_affected_partition_results_unions_known_sets() {
        let merged = merge_affected_partition_results(
            "UNION ALL MV affected partition planning",
            vec![
                (
                    "ice.db.left".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::known([
                        key("west"),
                        key("east"),
                    ]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::known([
                        key("east"),
                        key("north"),
                    ]),
                ),
            ],
        );

        assert_eq!(
            merged,
            crate::mv::domain::model::AffectedTargetPartitions::known([
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
                    crate::mv::domain::model::AffectedTargetPartitions::known([key("west")]),
                ),
                (
                    "ice.db.right".to_string(),
                    crate::mv::domain::model::AffectedTargetPartitions::not_derived(
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
}

#[cfg(test)]
mod join_delta_append_only_fast_path_tests {
    use super::*;

    #[test]
    fn join_incremental_refresh_plan_kind_uses_logical_cutover() {
        let mode = select_join_incremental_execution_mode(false, false);
        assert_eq!(mode, MvIncrementalJoinMode::AppendOnly);
        let mode = select_join_incremental_execution_mode(true, false);
        assert_eq!(mode, MvIncrementalJoinMode::Coalesce);
        let mode = select_join_incremental_execution_mode(false, true);
        assert_eq!(mode, MvIncrementalJoinMode::Coalesce);
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
        let branches = crate::mv::domain::iceberg_join_branch::plan_join_delta_branches(
            &left,
            &right,
            crate::mv::domain::iceberg_join_branch::SnapshotWindow { from: 10, to: 11 },
            crate::mv::domain::iceberg_join_branch::SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        let mut branch_queries = Vec::new();
        for branch in &branches {
            let mut branch_query =
                crate::mv::domain::iceberg_join_branch::rewrite_join_branch_query(
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
            crate::mv::domain::iceberg_join_branch::rewrite_join_delta_coalesce_query_with_branch_queries(
                &base_query,
                branch_queries,
                "left-uuid",
                "right-uuid",
            )
            .expect("coalesce rewrite");
        let rendered = novarocks_parser::printer::print_query(&coalesced);

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

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
fn normalize_join_branch_snapshot_tables(
    query: &mut ast::Query,
    branch: &crate::mv::domain::iceberg_join_branch::JoinDeltaBranchPlan,
) -> Result<(), String> {
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err("join branch snapshot normalization requires SELECT body".to_string());
    };
    let [from] = select.from.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one FROM item".to_string());
    };
    let [join] = from.joins.as_mut_slice() else {
        return Err("join branch snapshot normalization requires one JOIN".to_string());
    };
    if let crate::mv::domain::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.left {
        rewrite_snapshot_table_factor(&mut from.relation, &branch.left_base, snapshot_id, None)?;
    }
    if let crate::mv::domain::iceberg_join_branch::BranchSide::Snapshot(snapshot_id) = branch.right
    {
        rewrite_snapshot_table_factor(&mut join.relation, &branch.right_base, snapshot_id, None)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
struct RewriteMergeRefreshOptions {
    apply_key: ApplyKeyContract,
}

pub(crate) fn validate_drop_readiness(
    readiness: novarocks_mv_application::readiness::MvKeyedDropReadiness,
    expected_object_id: Option<&ConnectorTableObjectId>,
) -> Result<
    novarocks_mv_application::readiness::MvKeyedDropReadiness,
    novarocks_mv_application::ports::MvProviderFailure,
> {
    match (&readiness, expected_object_id) {
        (
            novarocks_mv_application::readiness::MvKeyedDropReadiness::ReadyToDrop(guard),
            Some(expected),
        ) if guard.expected_target_object_id() == Some(expected) => Ok(readiness),
        (novarocks_mv_application::readiness::MvKeyedDropReadiness::AlreadyAbsent, None) => {
            Ok(readiness)
        }
        _ => Err(novarocks_mv_application::ports::MvProviderFailure::new(
            novarocks_mv_application::ports::MvProviderFailureKind::TargetReplaced,
            "MV DROP target changed after management admission",
        )),
    }
}

pub(crate) fn validate_drop_installation(
    outcome: novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome,
) -> Result<(), novarocks_mv_application::ports::MvProviderFailure> {
    match outcome {
            novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::Removed
            | novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::AlreadyAbsent => {
                Ok(())
            }
            novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::Superseded => {
                Err(novarocks_mv_application::ports::MvProviderFailure::new(
                    novarocks_mv_application::ports::MvProviderFailureKind::TargetReplaced,
                    "MV projection changed after the provider DROP committed",
                ))
            }
            novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::Installed(_)
            | novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::Unchanged(_)
            | novarocks_mv_application::readiness::MvKeyedProjectionInstallOutcome::AlreadyProjectedElsewhere(_) => {
                Err(novarocks_mv_application::ports::MvProviderFailure::new(
                    novarocks_mv_application::ports::MvProviderFailureKind::Corruption,
                    "MV DROP finalization returned an installation outcome",
                ))
            }
    }
}

pub(crate) mod async_preparation;
