//! Frontend-owned assembly for `EXPLAIN REFRESH MATERIALIZED VIEW`.

use std::sync::Arc;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::mv::analysis::canonicalize_iceberg_mv_select_query;
use crate::mv::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::refresh::capabilities::RefreshCapabilities;
use crate::mv::refresh::definition::parse_iceberg_table_refs;
use crate::mv::refresh::definition::{load_iceberg_mv_definition_by_target, parse_mv_select_query};
use crate::mv::refresh::execution_policy::explain_refresh_full_guard;
use crate::mv::refresh::pin::validate_refresh_pin_table_uuids;
use crate::mv::refresh::rewrite_context::build_neutral_refresh_rewrite_context;
use crate::mv::refresh::schema_contract::validate_aggregate_schema_contract_metadata;
use crate::mv::refresh::target::{
    load_iceberg_mv_target_binding, resolve_refresh_target, validate_target_snapshot,
};
use crate::mv::refresh_pin_adapter::capture_refresh_snapshot_pin_with_ports;
use crate::query_execution::mv_assembly::query_local_bindings::{
    bind_imv_target_query_table_in_store_from_rewrite,
    freeze_imv_base_query_local_overlays_from_captured_inputs,
};
use novarocks_sql::syntax::RefreshMaterializedViewStmt;

/// Compiles an EXPLAIN refresh plan from the exact frozen MV ports and
/// query-local bindings used by refresh preparation.
pub fn explain_iceberg_mv_refresh_rewrite_plan_with_ports(
    ports: &IcebergMvCorePorts,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &RefreshMaterializedViewStmt,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, String> {
    explain_refresh_full_guard(stmt.full)?;

    let target = resolve_refresh_target(current_catalog, current_database, &stmt.name)?;
    let mv_definition = load_iceberg_mv_definition_by_target(ports.repository().as_ref(), &target)?;
    let target_binding = load_iceberg_mv_target_binding(
        ports.connector_control(),
        ports.storage_observation(),
        &target,
        connector_context,
    )?;
    validate_target_snapshot(&target, &mv_definition, &target_binding)?;

    let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
    let canonical_select_query = canonicalize_iceberg_mv_select_query(
        &parse_mv_select_query(&mv_definition.select_sql)?,
        current_catalog,
        current_database,
    );
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
        ports.connector_control(),
        ports.storage_observation(),
        &base_refs,
        connector_context,
    )?;
    validate_refresh_pin_table_uuids(&mv_definition, &pin, &base_refs)?;
    let rewrite = build_neutral_refresh_rewrite_context(
        ports.connector_control(),
        ports.storage_observation(),
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
        crate::catalog_application::query_catalog::catalog_service_snapshot(ports);
    let overlays = freeze_imv_base_query_local_overlays_from_captured_inputs(
        ports.connector_control(),
        connector_context,
        &rewrite.base_refs,
        &rewrite.pin,
        &rewrite.previous_snapshot_ids,
    )?;
    let materializer = crate::catalog_application::query_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        None,
        &catalog_service_snapshot,
        Arc::clone(&bindings),
        crate::catalog_application::query_materializer::iceberg_table_binding_loader(
            ports.connector_control(),
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
