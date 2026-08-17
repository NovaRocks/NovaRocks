// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Production binding for frontend-owned incremental MV refresh writes.

use std::sync::Arc;

use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorWriteLease};

use crate::mv::application::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};
use crate::mv::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::refresh_artifact::PreparedMvIncrementalWrite;
use crate::query_execution::mv_native_write::PreparedMvNativeWriteAssembly;
use crate::query_execution::planning::bindings::QueryTableBindingStore;
use crate::query_execution::request_context::QueryExecutionContext;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RewriteMergeRefreshEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
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

/// Activate a value-only incremental refresh artifact after frontend intent
/// persistence and exact-lease admission. Core rebuilds only provider-private
/// scan and writer facts here; it returns a sealed native-assembly carrier and
/// never advances MV metadata or executes an external commit.
pub(crate) fn bind_prepared_mv_incremental_staging(
    query_kernel: &QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    prepared: PreparedMvIncrementalWrite,
    planning_lease: &ConnectorControlPlanningLease,
    exact_lease: &ConnectorWriteLease,
    execution: &QueryExecutionContext,
) -> Result<PreparedMvNativeWriteAssembly, String> {
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
    let target_binding = crate::query_execution::mv_assembly::query_local_bindings::bind_imv_target_query_table_in_store_from_rewrite(
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
            MvIncrementalWriteMode::FastAppend => {
                vec![novarocks_spi::connector::ConnectorRowMutationEffect::Insert]
            }
            MvIncrementalWriteMode::RowDelta => vec![
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
        MvIncrementalRewriteEvidence::None => RewriteMergeRefreshEvidence::None,
        MvIncrementalRewriteEvidence::Aggregate => RewriteMergeRefreshEvidence::Aggregate,
        MvIncrementalRewriteEvidence::JoinAggregate => RewriteMergeRefreshEvidence::JoinAggregate,
        MvIncrementalRewriteEvidence::BranchUnionAggregate => {
            RewriteMergeRefreshEvidence::BranchUnionAggregate
        }
    };
    match execution_artifact {
        crate::query_execution::mv_assembly::refresh_artifact::MvIncrementalExecutionArtifact::CanonicalQuery => {
            let imv_rewrite_input = sql_imv_planning_input_from_rewrite(
                &refresh_rewrite,
                target_binding,
                rewrite_evidence,
            )?;
            let catalog_service_snapshot =
                crate::catalog_application::query_catalog::catalog_service_snapshot(query_kernel);
            let base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs(
                ports.connector_control(),
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
                    "IMV incremental refresh requires a non-empty admitted backend topology"
                        .to_string()
                })?;
            let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_catalog);
            let write_mode = match mode {
                MvIncrementalWriteMode::FastAppend => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::FastAppend
                }
                MvIncrementalWriteMode::RowDelta => {
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
            Ok(distributed)
        }
        crate::query_execution::mv_assembly::refresh_artifact::MvIncrementalExecutionArtifact::JoinLogical {
            mode: join_execution_mode,
        } => {
            let join_mode = match join_execution_mode {
                MvIncrementalJoinMode::AppendOnly => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::AppendOnly
                }
                MvIncrementalJoinMode::Coalesce => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::Coalesce
                }
            };
            let write_mode = match mode {
                MvIncrementalWriteMode::FastAppend => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::FastAppend
                }
                MvIncrementalWriteMode::RowDelta => {
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::RowDelta
                }
            };
            let base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs(
                ports.connector_control(),
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
            Ok(distributed)
        }
    }
}
