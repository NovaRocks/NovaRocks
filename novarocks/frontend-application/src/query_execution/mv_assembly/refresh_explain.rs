//! Frontend-owned assembly for `EXPLAIN REFRESH MATERIALIZED VIEW`.

use std::sync::Arc;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::target::resolve_refresh_target;
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;

/// The unique pure SQL explain author; its catalog cannot perform live I/O.
fn compile_captured_refresh_explain(
    ports: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    target_binding_id: novarocks_sql::binding::SqlTableBindingId,
    catalog: &dyn novarocks_sql::compiler::SqlCatalogSnapshot,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, novarocks_sql::compiler::SqlCompileError> {
    novarocks_sql::compiler::compile_imv_refresh_explain_lines(
        novarocks_sql::compiler::SqlImvRefreshExplainContext {
            canonical_query: Box::new((*rewrite.canonical_select_query).clone()),
            imv_rewrite: novarocks_sql::compiler::SqlImvPlanningInput::new(
                rewrite.to_sql_rewrite_snapshot(target_binding_id)?,
                novarocks_sql::compiler::SqlImvRewriteValidation::None,
            ),
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings::default(),
            environment: novarocks_sql::compiler::SqlPlanningEnvironment::NotApplicable,
            catalog,
            functions: ports.function_catalog().as_ref(),
            constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
            constant_policy: ports.constant_policy(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                connector_context,
            ),
            level,
        },
    )
}

/// Display the original no-read decision without inventing a numeric snapshot.
fn empty_refresh_explain_lines(
    decision: crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision,
) -> Option<Vec<String>> {
    use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
    matches!(decision, ExecutableRefreshDecision::SkipEmpty).then(|| {
        vec![
            "MV refresh: SkipEmpty".to_string(),
            "Data read: none (original source snapshot policy selected SkipEmpty)".to_string(),
        ]
    })
}

/// Read-only inputs; no refresh identity, publication admission or writer.
pub(crate) struct MvExplainPreparationInput {
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) statement: MvRefreshRequest,
    pub(crate) level: novarocks_sql::compiler::ExplainLevel,
    pub(crate) context: novarocks_spi::connector::ConnectorRequestContext,
}

/// Suspends only at the existing named I/O gate. The original reader executes
/// the pure turns and retains all exact observations across every suspension.
pub(crate) async fn explain_original(
    input: MvExplainPreparationInput,
    io: &super::refresh_preparation::io_gate::MvRefreshPreparationIo,
) -> Result<Vec<String>, crate::mv::domain::application::MvApplicationError> {
    explain_original_inner(input, io)
        .await
        .map_err(|error| error.0)
}
async fn explain_original_inner(
    input: MvExplainPreparationInput,
    io: &super::refresh_preparation::io_gate::MvRefreshPreparationIo,
) -> Result<Vec<String>, super::refresh_preparation::async_facts::PreparationError> {
    use super::refresh_preparation::async_original::{
        MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog,
    };
    use crate::mv::domain::refresh::rewrite_context::RefreshRewriteInputs;
    let ports = &input.ports;
    let context = &input.context;
    let target = resolve_refresh_target(
        input.current_catalog.as_deref(),
        &input.current_database,
        &input.statement.name_parts,
    )?;
    let projection = io.definition(ports, &target).await?;
    let binding = io.target_binding(ports, &target, context).await?;
    let schema = crate::mv::domain::storage_observation::observe_schema_validation(
        ports.storage_observation(),
        binding.lease(),
        binding.metadata(),
        context.clone(),
    )?;
    let runtime_bindings = crate::mv::domain::refresh::schema_contract::validate_projection_target(
        &projection,
        &schema,
    )?;
    let baseline =
        crate::mv::domain::iceberg_refresh::build_refresh_state_baseline(&projection, &binding)?;
    io.capture_refresh_sources(ports, &projection, &baseline, input.statement.full, context)
        .await?;
    let base_refs = projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .map(|base| novarocks_types::naming::TableIdentity {
            catalog: base.catalog_at_binding.clone(),
            namespace: base.namespace_at_binding.clone(),
            table: base.relation_at_binding.clone(),
        })
        .collect::<Vec<_>>();
    let (projection, _) = io
        .rebind_definition(
            ports,
            &projection,
            &base_refs,
            &target,
            Some(&schema),
            context,
        )
        .await?;
    let query = super::refresh_preparation::canonical_mv_select_query(&projection)?;
    let decision =
        crate::mv::domain::iceberg_refresh::async_preparation::captured_refresh_decision_original(
            io,
            ports,
            &projection,
            &runtime_bindings,
            &baseline,
            &query,
            input.statement.full,
            context,
        )
        .await?;
    if let Some(lines) = empty_refresh_explain_lines(decision) {
        return Ok(lines);
    }
    let pin = io.capture_pin(ports, &projection, context).await?;
    let aggregate = super::refresh_preparation::pure_reader::frozen_refresh_aggregate_analysis(
        io,
        ports,
        &projection,
        &query,
        context,
    )
    .await?;
    let lease = binding.lease().clone();
    let rewrite =
        crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
            io,
            RefreshRewriteInputs {
                connector_control: ports.connector_control(),
                connector_context: context,
                projection: Arc::new(projection),
                pin: &pin,
                state_baseline: &baseline,
                target_binding: &binding,
                target_observation: &schema,
                runtime_bindings: &runtime_bindings,
                join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(&query)?,
                aggregate,
            },
        )
        .await?;
    let table = novarocks_types::naming::TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    };
    let current_binding = io
        .target_binding_with_lease(ports.storage_observation(), &table, lease.clone(), context)
        .await?;
    crate::mv::domain::iceberg_refresh::async_preparation::validate_target_snapshot_original(
        io,
        ports,
        &target,
        &rewrite.mv_definition,
        &current_binding,
        context,
    )
    .await?;
    if current_binding.table_uuid() != rewrite.target_table_uuid {
        return Err("EXPLAIN REFRESH target UUID differs from its rewrite context".into());
    }
    let bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding_id =
        super::query_local_bindings::bind_imv_target_query_table_in_store_from_rewrite_original(
            io, &rewrite, &bindings, &lease, context, None,
        )
        .await?;
    let overlays = super::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs_original(
        io, ports.connector_control(), context, &rewrite,
    ).await?;
    let captured = capture_catalog(
        MvCatalogCaptureInput {
            query: MvCatalogCaptureQuery::Query(Box::new(
                (*rewrite.canonical_select_query).clone(),
            )),
            ports: ports.clone(),
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            context: context.clone(),
            control: crate::query_execution::planning::sql_compile_control_from_connector_request(
                context,
            ),
            bindings,
            overlays,
        },
        io,
    )
    .await?;
    compile_captured_refresh_explain(
        ports,
        &rewrite,
        target_binding_id,
        &captured.catalog,
        input.level,
        context,
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::empty_refresh_explain_lines;
    use crate::mv::domain::refresh::planning::{
        RefreshPlanningInput, decide_requested_refresh_plan,
    };
    use crate::mv::domain::refresh::snapshot::{
        BaseSnapshotPolicy as Policy, BaseSnapshotStatus as Status,
    };

    #[test]
    fn empty_explain_preserves_the_original_single_all_and_partial_join_policy() {
        let single = [Status::new("ice.ns.left", None, None)];
        let mixed = [
            Status::new("ice.ns.left", None, Some(17)),
            Status::new("ice.ns.right", None, None),
        ];
        for (policy, statuses, skips) in [
            (Policy::SingleBase, single.as_slice(), true),
            (Policy::AllBasesRequired, mixed.as_slice(), false),
            (Policy::JoinPairPartialInitialSkip, mixed.as_slice(), true),
        ] {
            let original = decide_requested_refresh_plan(
                &RefreshPlanningInput {
                    snapshot_policy: policy,
                    base_snapshots: statuses,
                    label: "EXPLAIN REFRESH",
                },
                false,
            );
            if skips {
                let lines = empty_refresh_explain_lines(original.unwrap().refresh).unwrap();
                assert!(lines.iter().any(|line| line.contains("SkipEmpty")));
                assert!(lines.iter().any(|line| line.contains("Data read: none")));
            } else {
                assert!(original.is_err());
            }
        }
    }

    #[test]
    fn empty_explain_never_hides_a_lost_predecessor_or_changes_full_policy() {
        for (previous, full) in [(Some(17), false), (None, true)] {
            let statuses = [Status::new("ice.ns.source", previous, None)];
            let refused = decide_requested_refresh_plan(
                &RefreshPlanningInput {
                    snapshot_policy: Policy::SingleBase,
                    base_snapshots: &statuses,
                    label: "EXPLAIN REFRESH",
                },
                full,
            );
            assert!(refused.is_err());
        }
    }
}
