// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Production preparation and binding for frontend-owned MV refresh writes.

use std::sync::Arc;

use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorWriteLease};

use crate::catalog_application::query_catalog::catalog_service_snapshot;
use crate::mv::application::{
    MvFirstRefreshExecutionArtifact, MvFirstRefreshLogicalContext, PreparedMvFirstRefreshWrite,
    PreparedMvNativeWriteAssembly,
};
use crate::mv::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::compiler::prepare_sealed_iceberg_write_native_assembly;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::planning::bindings::QueryTableBindingStore;
use crate::query_execution::planning::write_sink::{
    admit_prepared_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::request_context::QueryExecutionContext;
use novarocks_sql::planning::mv::first_refresh::{
    SqlMvFirstRefreshAnalyzeContext, SqlMvJoinFirstRefreshAnalyzeContext,
    analyze_join_first_refresh_connector_write, analyze_mv_first_refresh_connector_write,
    compile_join_first_refresh_connector_write, compile_mv_first_refresh_connector_write,
};

pub(crate) fn frozen_logical_context_from_rewrite(
    rewrite: &crate::mv::rewrite::context::IcebergMvRewriteContext,
    affected_partitions: crate::mv::model::AffectedTargetPartitions,
    frozen_base_overlays: Option<
        Vec<crate::query_execution::planning::catalog_materializer::QueryLocalTableOverlay>,
    >,
) -> Result<MvFirstRefreshLogicalContext, String> {
    Ok(MvFirstRefreshLogicalContext {
        mv_definition: (*rewrite.mv_definition).clone(),
        canonical_select_query: (*rewrite.canonical_select_query).clone(),
        base_refs: rewrite.base_refs.to_vec(),
        pin: novarocks_sql::planning::mv::first_refresh::SqlMvSnapshotPin::try_from_maps(
            rewrite.pin.to_snapshot_map(),
            rewrite.pin.to_table_uuid_map(),
        )?,
        previous_snapshot_ids: rewrite.previous_snapshot_ids.clone(),
        previous_table_uuids: rewrite.previous_table_uuids.clone(),
        target_table_uuid: rewrite.target_table_uuid.clone(),
        affected_partitions,
        frozen_base_overlays,
    })
}

/// Reserve the one primary first-refresh write cohort from facts frozen in an
/// MV refresh context. The staging branch must already exist and `exact_lease`
/// must have been derived from the retained target control binding. This is
/// the first point that mutates the provider write-service registry; SQL
/// artifact preparation remains side-effect free.
/// Bind an SQL-shaped first-refresh artifact only after the frontend has
/// retained its exact write lease and admitted an immutable query execution.
/// The result retains the exact native-assembly input for the Frontend; it
/// deliberately does not encode or submit a query, commit a provider mutation,
/// or expose row payloads.
pub(crate) fn bind_prepared_mv_first_refresh_staging(
    query_kernel: &QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    prepared: PreparedMvFirstRefreshWrite,
    planning_lease: &ConnectorControlPlanningLease,
    exact_lease: &ConnectorWriteLease,
    execution: &QueryExecutionContext,
) -> Result<PreparedMvNativeWriteAssembly, String> {
    let operation_id = prepared.operation_id();
    let cohort_id = prepared.primary_cohort();
    let expected_target_snapshot_id = prepared.expected_target_snapshot_id();
    let target_catalog = prepared.target_catalog().to_string();
    let target_namespace = prepared.target_namespace().to_string();
    let target_name = prepared.target_name().to_string();
    let current_catalog = prepared.current_catalog().map(str::to_string);
    let current_database = prepared.current_database().to_string();
    let connector_context =
        crate::connector::connector_request_context_for_execution(None, execution)?;
    let root_hash_column = prepared.root_hash_column().to_string();
    let template = crate::mv::iceberg_activation::activate_first_refresh_connector_write(
        &prepared,
        connector_context.clone(),
        exact_lease,
    )?;
    let distributed = match prepared.into_execution_artifact() {
        MvFirstRefreshExecutionArtifact::Sql(physical_sql) => {
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding = admit_prepared_connector_write_target(
                bindings.as_ref(),
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target_catalog.clone(),
                    target_namespace.clone(),
                    target_name.clone(),
                )?,
                template.preparation().clone(),
                planning_lease.clone(),
            )?;
            let sink = dml_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                target_binding,
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data,
                novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )?;
            let catalog_service_snapshot = catalog_service_snapshot(query_kernel);
            let materializer = crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new(
                None,
                &catalog_service_snapshot,
                Arc::clone(&bindings),
                crate::query_execution::planning::statistics::iceberg_table_binding_loader(
                    query_kernel.connector_control().as_ref(),
                    connector_context.clone(),
                ),
            );
            let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
                .ok_or_else(|| {
                "MV first-refresh write requires a non-empty admitted backend topology".to_string()
            })?;
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
            let analyzed = analyze_mv_first_refresh_connector_write(
                physical_sql,
                SqlMvFirstRefreshAnalyzeContext {
                    current_catalog: current_catalog.clone(),
                    current_database: current_database.clone(),
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
                    sink,
                },
            )?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                Arc::clone(&bindings),
                &connector_context,
            )?;
            let distributed_plan = compile_mv_first_refresh_connector_write(analyzed, &statistics)?;
            prepare_sealed_iceberg_write_native_assembly(
                query_kernel.connector_control().as_ref(),
                execution,
                distributed_plan,
                bindings.as_ref(),
                &connector_context,
                template,
            )?
        }
        MvFirstRefreshExecutionArtifact::Logical(logical) => {
            let facts = logical.into_context();
            let frozen_base_overlays = facts.frozen_base_overlays.clone().ok_or_else(|| {
                "MV first-refresh logical artifact is missing its admitted base bindings"
                    .to_string()
            })?;
            let refresh_rewrite = rebuild_frozen_mv_rewrite_context(
                ports,
                current_catalog.as_deref(),
                &current_database,
                expected_target_snapshot_id,
                &target_catalog,
                &target_namespace,
                &target_name,
                &facts,
                planning_lease,
                &connector_context,
            )?;
            let bindings = Arc::new(QueryTableBindingStore::try_new()?);
            let target_binding =
                crate::mv::iceberg_refresh::bind_imv_target_query_table_in_store_from_rewrite(
                    &refresh_rewrite,
                    &bindings,
                    planning_lease,
                    &connector_context,
                )?;
            let write_target_binding = admit_prepared_connector_write_target(
                bindings.as_ref(),
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target_catalog.clone(),
                    target_namespace.clone(),
                    target_name.clone(),
                )?,
                template.preparation().clone(),
                planning_lease.clone(),
            )?;
            let sink = dml_write_plan_input_for_admitted_target(
                bindings.as_ref(),
                write_target_binding,
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data,
                novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )?;
            let catalog_service_snapshot = catalog_service_snapshot(query_kernel);
            let materializer = crate::query_execution::planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&bindings),
                crate::query_execution::planning::statistics::iceberg_table_binding_loader(
                    query_kernel.connector_control().as_ref(),
                    connector_context.clone(),
                ),
                frozen_base_overlays,
            );
            let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
                .ok_or_else(|| {
                "MV first-refresh write requires a non-empty admitted backend topology".to_string()
            })?;
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&materializer);
            let analyzed =
                analyze_join_first_refresh_connector_write(SqlMvJoinFirstRefreshAnalyzeContext {
                    canonical_query: Box::new((*refresh_rewrite.canonical_select_query).clone()),
                    rewrite_snapshot: refresh_rewrite.to_sql_rewrite_snapshot(target_binding)?,
                    expected_root_hash_column: root_hash_column,
                    current_catalog: current_catalog.clone(),
                    current_database: current_database.clone(),
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
                    sink,
                })?;
            let statistics = crate::query_execution::planning::statistics::QueryStatisticsContext::from_statistics_resolver_with_bindings(
                query_kernel,
                materializer.query_table_bindings(),
                &connector_context,
            )?;
            let distributed_plan =
                compile_join_first_refresh_connector_write(analyzed, &statistics)?;
            prepare_sealed_iceberg_write_native_assembly(
                query_kernel.connector_control().as_ref(),
                execution,
                distributed_plan,
                bindings.as_ref(),
                &connector_context,
                template,
            )?
        }
    };
    if distributed.write_operation_id() != operation_id
        || distributed.write_cohort_id() != cohort_id
    {
        return Err("MV first-refresh distributed artifact identity mismatch".to_string());
    }
    Ok(distributed)
}

pub(crate) fn rebuild_frozen_mv_rewrite_context(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    expected_target_snapshot_id: Option<i64>,
    target_catalog: &str,
    target_namespace: &str,
    target_name: &str,
    facts: &MvFirstRefreshLogicalContext,
    planning_lease: &ConnectorControlPlanningLease,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Arc<crate::mv::rewrite::context::IcebergMvRewriteContext>, String> {
    let target_identity =
        novarocks_catalog::identifier::TableIdentity {
            catalog: facts.mv_definition.target_catalog.clone().ok_or_else(|| {
                "MV first-refresh logical artifact target has no connector catalog".to_string()
            })?,
            namespace: facts
                .mv_definition
                .target_namespace
                .clone()
                .ok_or_else(|| {
                    "MV first-refresh logical artifact target has no namespace".to_string()
                })?,
            table: facts.mv_definition.target_table.clone().ok_or_else(|| {
                "MV first-refresh logical artifact target has no table".to_string()
            })?,
        };
    if target_identity.catalog != target_catalog
        || target_identity.namespace != target_namespace
        || target_identity.table != target_name
    {
        return Err(
            "MV refresh logical artifact target does not match its frozen write request"
                .to_string(),
        );
    }
    validate_frozen_join_base_facts(facts)?;
    let target_binding =
        crate::mv::refresh::target_binding::load_mv_target_binding_with_lease_and_ports(
            ports.storage_observation(),
            &target_identity,
            planning_lease.clone(),
            connector_context,
        )?;
    if target_binding.table_uuid() != facts.target_table_uuid {
        return Err(
            "MV refresh logical artifact target UUID drifted after preparation".to_string(),
        );
    }
    if target_binding.current_snapshot_id() != expected_target_snapshot_id {
        return Err(
            "MV refresh logical artifact target snapshot drifted after preparation".to_string(),
        );
    }
    let application_pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_captured_entries(
        facts
            .base_refs
            .iter()
            .map(|base| {
                let snapshot_id = facts.pin.get(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no snapshot pin for {}",
                        base.fqn()
                    )
                })?;
                let table_uuid = facts.pin.uuid(base).ok_or_else(|| {
                    format!(
                        "MV first-refresh logical artifact has no UUID pin for {}",
                        base.fqn()
                    )
                })?;
                Ok((base.clone(), snapshot_id, table_uuid.to_string()))
            })
            .collect::<Result<Vec<_>, String>>()?,
    );
    let schema_contract = facts.mv_definition.schema_contract.clone().map(Arc::new);
    crate::mv::rewrite::context::IcebergMvRewriteContext::from_parts(
        target_identity,
        facts.mv_definition.mv_id,
        current_catalog.map(str::to_string),
        current_database.to_string(),
        Arc::new(facts.mv_definition.clone()),
        Arc::new(facts.canonical_select_query.clone()),
        Arc::from(facts.base_refs.clone()),
        Arc::new(application_pin),
        facts.previous_snapshot_ids.clone(),
        facts.previous_table_uuids.clone(),
        expected_target_snapshot_id,
        facts.target_table_uuid.clone(),
        target_binding.physical_write_schema()?,
        Arc::from(target_binding.observation().field_ids().to_vec()),
        schema_contract,
    )
    .map(Arc::new)
}

fn validate_frozen_join_base_facts(facts: &MvFirstRefreshLogicalContext) -> Result<(), String> {
    if facts.base_refs.is_empty() || facts.pin.len() != facts.base_refs.len() {
        return Err(
            "MV first-refresh logical artifact has incomplete base snapshot pins".to_string(),
        );
    }
    // Production logical first-refresh artifacts retain the materializations
    // admitted during preparation.  Those overlays carry the exact lease,
    // table identity, and pinned input set that activation must use; asking
    // the catalog for the current base here would silently reintroduce a
    // latest-generation acquire.
    facts
        .frozen_base_overlays
        .as_ref()
        .map(|_| ())
        .ok_or_else(|| {
            "MV logical artifact is missing exact-generation frozen base overlays".to_string()
        })
}

fn parse_query_from_sql(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
    let statement = novarocks_sql::syntax::parse_normalized_sql_raw(&normalized)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("MV first-refresh physical artifact is not a SELECT query".to_string());
    };
    Ok(*query)
}
