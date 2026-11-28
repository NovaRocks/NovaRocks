//! Frontend-owned assembly for `EXPLAIN REFRESH MATERIALIZED VIEW`.

use std::sync::Arc;

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::strategy::RefreshStrategy;
use crate::mv::domain::refresh::target::resolve_refresh_target;
use crate::mv::domain::rewrite::context::IcebergMvRewriteContext;

/// The unique pure SQL explain author; its catalog cannot perform live I/O.
fn compile_captured_refresh_explain(
    ports: &IcebergMvCorePorts,
    rewrite: &IcebergMvRewriteContext,
    imv_rewrite: novarocks_sql::compiler::SqlImvPlanningInput,
    catalog: &dyn novarocks_sql::compiler::SqlCatalogSnapshot,
    level: novarocks_sql::compiler::ExplainLevel,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, novarocks_sql::compiler::SqlCompileError> {
    novarocks_sql::compiler::compile_imv_refresh_explain_lines(
        novarocks_sql::compiler::SqlImvRefreshExplainContext {
            canonical_query: Box::new((*rewrite.canonical_select_query).clone()),
            imv_rewrite,
            current_catalog: rewrite.current_catalog.clone(),
            current_database: rewrite.current_database.clone(),
            optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings::default(),
            environment: novarocks_sql::compiler::SqlPlanningEnvironment::NotApplicable,
            catalog,
            functions: ports.function_catalog().as_ref(),
            sealed_functions: Arc::clone(ports.sealed_functions()),
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
    pub(crate) query_memory_budget_bytes: u64,
    pub(crate) optimizer_settings: novarocks_sql::compiler::SessionOptimizerSettings,
    pub(crate) session_properties:
        Option<Arc<novarocks_query_application::sql::session::registry::SessionPropertyInputs>>,
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
    use crate::mv::domain::iceberg_refresh::async_preparation::{
        RefreshBindingObservation, definition_analysis_failure, prepare_refresh_binding_candidate,
    };
    use crate::mv::domain::refresh::rewrite_context::RefreshRewriteInputs;
    use novarocks_mv_application::persistence::{
        codec::{Marker, PublicationEvidence},
        keyed_documents::{MvKeyedDocumentProjection, StoredKeyedMvProjection},
        rebuild_classification::{
            RebuildEvidence, RefreshBindingClassification, classify_refresh_observation,
        },
    };
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
        binding.planning_pin(),
        binding.metadata(),
        context.clone(),
    )?;
    let documents = binding.planning_pin().derive_document_storage_lease()?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        binding.identity().clone(),
        schema.table_object_id().clone(),
        context.clone(),
    );
    let current = io.observe_current(ports, &documents, request).await?;
    if current.view.metadata_version != schema.exact_schema().metadata_version
        || current.view.object_id != projection.facts.source_revision().target_object_id
        || current.view.current_snapshot != binding.current_snapshot_id()
        || &current.versions != projection.facts.versions()
    {
        return Err("EXPLAIN REFRESH observed a different MV generation".into());
    }
    let provider = novarocks_spi::connector::ConnectorProviderBindingKey {
        instance_id: binding
            .planning_pin()
            .binding()
            .descriptor()
            .instance_id
            .clone(),
        incarnation: binding.planning_pin().binding().incarnation(),
    };
    let unattributed_full = current.requires_unattributed_full_refresh();
    let full = match &input.session_properties {
        Some(properties) => novarocks_mv_application::session_properties::manual_refresh_is_full(
            properties,
            &provider,
            input.statement.is_full(),
        )?,
        None => input.statement.is_full(),
    } || unattributed_full;
    let eligibility = current.eligibility();
    if eligibility.explicit_rebuild_required || (full && eligibility.strict_recovery_required) {
        return Ok(vec!["MV refresh: ExplicitRebuildRequired".into()]);
    }
    if eligibility.strict_recovery_required {
        use crate::mv::domain::refresh::recovery::prove_strict_recovery;
        let lines = match prove_strict_recovery(io, ports, &current, &binding, context).await {
            Ok(recovery) => {
                let requirement = recovery.fence.requirement();
                vec![
                    "MV refresh: StrictRecovery".into(),
                    format!(
                        "Frozen ContentLocate estimate: {} bytes; budget: {} bytes",
                        requirement.estimate_bytes(),
                        requirement.budget()
                    ),
                    format!("Original source windows: {}", recovery.targets.len()),
                ]
            }
            Err(error) => vec![
                "MV refresh: StrictRecoveryRequired".into(),
                error.0.to_string(),
            ],
        };
        return Ok(lines);
    }
    // Strict replay owns its historical P check. An unattributed full refresh
    // uses the exact observed target with no previous publication. EXPLAIN
    // keeps this projection local to the statement.
    let projection = if unattributed_full {
        StoredKeyedMvProjection {
            mv_id: projection.mv_id,
            facts: MvKeyedDocumentProjection::try_from_current(current.clone())?,
        }
    } else {
        if current.publication_revision != projection.facts.source_revision().publication_revision {
            return Err("EXPLAIN REFRESH observed a different MV generation".into());
        }
        projection
    };
    let can_incrementally_refresh =
        eligibility.automatic_refresh && !eligibility.automatic_reuse_blocked;
    let analyzed =
        prepare_refresh_binding_candidate(io, ports, &projection, &binding, &schema, context).await;
    let markers = current
        .decoded_markers()
        .map_err(|error| error.to_string())?;
    let root = current.root_binding_revision();
    let scope = current.publication_evidence(&root);
    if matches!(scope, PublicationEvidence::Unattributed) && !unattributed_full {
        return Err("EXPLAIN REFRESH has no reusable MV generation".into());
    }
    match classify_refresh_observation(
        RebuildEvidence::from(&current.versions),
        analyzed
            .as_ref()
            .map(RefreshBindingObservation::evidence)
            .map_err(definition_analysis_failure),
        scope,
        &markers,
    )? {
        RefreshBindingClassification::Unchanged => {}
        RefreshBindingClassification::NeedsRebuild(_) => {
            return Ok(vec![
                "MV refresh: NeedsRebuild".into(),
                "Data write: full overwrite with the newly analyzed sources".into(),
            ]);
        }
        RefreshBindingClassification::NeedsRebuildWithoutMarker => {
            return Ok(vec![
                "MV refresh: FullReplace".into(),
                "Data write: full overwrite with the newly analyzed sources".into(),
            ]);
        }
        RefreshBindingClassification::InvalidDefinition {
            marker: Marker::InvalidDefinition { diagnostic, .. },
            ..
        } => return Ok(vec!["MV refresh: InvalidDefinition".into(), diagnostic]),
        RefreshBindingClassification::IncompatibleStorage { cause, .. } => {
            return Ok(vec!["MV refresh: NeedsRebuild".into(), cause.to_string()]);
        }
        RefreshBindingClassification::Blocked
        | RefreshBindingClassification::InvalidDefinition { .. } => {
            return Err("EXPLAIN REFRESH has no reusable MV generation".into());
        }
    }
    let RefreshBindingObservation::Complete(candidate) = analyzed? else {
        return Err("EXPLAIN REFRESH has no admitted current binding".into());
    };
    let runtime_bindings = crate::mv::domain::refresh::schema_contract::validate_projection_target(
        &projection,
        &schema,
    )?;
    let baseline =
        crate::mv::domain::iceberg_refresh::build_refresh_state_baseline(&projection, &binding)?;
    io.capture_refresh_sources(
        ports,
        &candidate.observed.document,
        &baseline,
        full,
        context,
    )
    .await?;
    let query = candidate.canonical_query();
    let decision =
        crate::mv::domain::iceberg_refresh::async_preparation::captured_refresh_decision_original(
            io,
            ports,
            &projection,
            &runtime_bindings,
            &baseline,
            query,
            full,
            context,
        )
        .await?;
    if let Some(lines) = empty_refresh_explain_lines(decision) {
        return Ok(lines);
    }
    use crate::mv::domain::refresh::snapshot::ExecutableRefreshDecision;
    let nonaggregate = !projection.facts.interpretation().is_aggregate();
    if nonaggregate {
        match decision {
            ExecutableRefreshDecision::FirstRefresh => {
                return Ok(vec![if full {
                    "MV refresh: FullReplace".into()
                } else {
                    "MV refresh: FirstRefresh".into()
                }]);
            }
            ExecutableRefreshDecision::MetadataOnly => {
                return Ok(vec!["MV refresh: MetadataOnly".into()]);
            }
            ExecutableRefreshDecision::SkipEmpty | ExecutableRefreshDecision::Incremental => {}
        }
    }
    let pin = io.capture_pin(ports, &projection, context).await?;
    let aggregate = super::refresh_preparation::pure_reader::frozen_refresh_aggregate_analysis(
        io,
        ports,
        &projection,
        query,
        context,
    )
    .await?;
    let lease = binding.planning_pin().clone();
    let rewrite =
        crate::mv::domain::refresh::rewrite_context::freeze_refresh_rewrite_context_original(
            io,
            RefreshRewriteInputs {
                rebuild_layout: None,
                recovery_publication: None,
                connector_control: ports.connector_control(),
                storage_observation: ports.storage_observation(),
                connector_context: context,
                projection: Arc::new(projection),
                pin: &pin,
                state_baseline: &baseline,
                target_binding: &binding,
                target_observation: &schema,
                runtime_bindings: &runtime_bindings,
                join_analysis: novarocks_sql::planning::mv::extract_refresh_join_analysis(query)?,
                aggregate,
            },
        )
        .await?;
    let mut lines = Vec::new();
    let content_strategy = if nonaggregate {
        let (decision, replacement) =
            super::refresh_preparation::pure_reader::choose_content_strategy(
                io,
                ports,
                &rewrite,
                &binding,
                context,
                can_incrementally_refresh,
                input.query_memory_budget_bytes,
                &input.optimizer_settings,
            )
            .await?;
        for (candidate, cost) in &decision.costs {
            let (name, basis) = match candidate {
                RefreshStrategy::AppendOnly => (
                    "AppendOnly",
                    "exact change-window volume and source manifests",
                ),
                RefreshStrategy::ContentLocate(_) => (
                    "ContentLocate",
                    "exact change-window volume, source and target manifests",
                ),
                RefreshStrategy::PartitionReplace => {
                    ("PartitionReplace", "exact sliced source manifests")
                }
                RefreshStrategy::FullReplace => ("FullReplace", "complete source manifests"),
                RefreshStrategy::MetadataOnly => continue,
            };
            lines.push(format!("MV optimizer cost {name}: {cost:.6} ({basis})"));
        }
        if let Some(rejection) = decision.content_rejection {
            lines.push(format!("ContentLocate unavailable: {rejection}"));
        }
        match &decision.strategy {
            RefreshStrategy::ContentLocate(requirement) => {
                lines.insert(0, "MV refresh: ContentLocate".into());
                lines.push(format!(
                    "ContentLocate estimate: {} bytes, deployment query memory budget: {} bytes",
                    requirement.estimate_bytes(),
                    requirement.budget(),
                ));
            }
            RefreshStrategy::AppendOnly => lines.insert(0, "MV refresh: AppendOnly".into()),
            RefreshStrategy::MetadataOnly => {
                lines.insert(0, "MV refresh: MetadataOnly".into());
                return Ok(lines);
            }
            RefreshStrategy::FullReplace => {
                lines.insert(0, "MV refresh: FullReplace".into());
                return Ok(lines);
            }
            RefreshStrategy::PartitionReplace => {
                let replacement =
                    replacement.ok_or("MV EXPLAIN lost its admitted replacement scope")?;
                lines.insert(0, "MV refresh: PartitionReplace".into());
                if let crate::mv::domain::model::AffectedTargetPartitions::Known { partitions } =
                    replacement.affected
                {
                    lines.push(format!("Affected target partitions: {}", partitions.len()));
                }
                return Ok(lines);
            }
        }
        Some(decision.strategy)
    } else {
        None
    };
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
    use novarocks_sql::compiler::{SqlImvPlanningInput, SqlImvRewriteValidation};
    let sql_snapshot = match &content_strategy {
        Some(RefreshStrategy::ContentLocate(_)) => {
            rewrite.to_sql_content_rewrite_snapshot(target_binding_id, binding.metadata())?
        }
        _ => rewrite.to_sql_rewrite_snapshot(target_binding_id)?,
    };
    let mut imv = SqlImvPlanningInput::new(sql_snapshot, SqlImvRewriteValidation::None);
    imv = match content_strategy {
        Some(RefreshStrategy::AppendOnly) => imv.for_append_only(),
        Some(RefreshStrategy::ContentLocate(_)) => imv.for_content_location(),
        _ => imv,
    };
    lines.extend(compile_captured_refresh_explain(
        ports,
        &rewrite,
        imv,
        &captured.catalog,
        input.level,
        context,
    )?);
    Ok(lines)
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
