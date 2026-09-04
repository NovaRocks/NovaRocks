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

//! Production binding for frontend-owned incremental MV refresh writes.

use std::sync::Arc;

use novarocks_plan_codec::SealedWriteTargets;
use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorWriteLease};

use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::common::admitted_query_context::QueryExecutionContext;
use crate::mv::domain::application::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::query_execution::kernels::QueryPreparationKernel;
use crate::query_execution::mv_assembly::iceberg_activation::{
    begin_incremental_connector_write_session, release_mv_write_session_without_commit,
};
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvIncrementalExecutionArtifact, MvIncrementalWriteRequest, PreparedMvIncrementalWrite,
};
use crate::query_execution::mv_native_write::PreparedMvNativeWriteAssembly;
use crate::query_execution::planning::write_sink::{
    admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
};
use crate::query_execution::write_session::ConnectorWriteSession;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RewriteMergeRefreshEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

fn sql_imv_planning_input_from_rewrite(
    rewrite: &crate::mv::domain::rewrite::context::IcebergMvRewriteContext,
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

/// The publication's branches, in the order their sealed write target ordinals
/// put them.
///
/// Two facts are established here rather than assumed. A change-stream
/// publication routes every branch, so a branch that arrived without routing
/// facts is a contract violation and fails closed instead of being defaulted
/// into one. And the branch order *is* the write target order, so the branches
/// are sorted by their sealed ordinal rather than taken in whatever order the
/// provider happened to hand them over -- the router gives branch `i` the writer
/// holding ordinal `i`, so a permuted list would silently feed one branch
/// another branch's rows.
fn change_stream_routed_targets(
    sealed_targets: &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan],
) -> Result<
    Vec<(
        &novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan,
        &novarocks_spi::connector::write_stack::ConnectorWriteRouteFacts,
    )>,
    String,
> {
    let mut routed = sealed_targets
        .iter()
        .map(|write_target| {
            write_target
                .route()
                .map(|route| (write_target, route))
                .ok_or_else(|| {
                    format!(
                        "MV incremental write target {} carries no provider routing facts",
                        write_target.ordinal().get()
                    )
                })
        })
        .collect::<Result<Vec<_>, String>>()?;
    routed.sort_by_key(|(write_target, _)| write_target.ordinal());
    Ok(routed)
}

/// Project the session's sealed branches into the routes SQL compiles against.
fn incremental_change_stream_routes(
    write_session: &ConnectorWriteSession,
    target: &crate::catalog_application::resolver::TargetBackend,
    target_bindings: &QueryTableBindingStore,
    planning_lease: &ConnectorControlPlanningLease,
) -> Result<Vec<novarocks_sql::planning::dml::DmlChangeStreamRoute>, String> {
    use novarocks_spi::connector::ConnectorWriteInputShape;

    let routed_targets = change_stream_routed_targets(write_session.targets())?;
    let mut routes = Vec::with_capacity(routed_targets.len());
    for (write_target, route) in routed_targets {
        let target_binding = admit_session_connector_write_target(
            target_bindings,
            novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                target.catalog.clone(),
                target.namespace.clone(),
                target.table.clone(),
            )?,
            write_target,
            planning_lease.clone(),
        )?;
        let mode = match write_target.input() {
            ConnectorWriteInputShape::Data { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data
            }
            ConnectorWriteInputShape::RowLineage { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::RowLineageData
            }
            ConnectorWriteInputShape::PositionDelete { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::PositionDeletes
            }
            ConnectorWriteInputShape::DeletionVector { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::DeletionVectors
            }
            ConnectorWriteInputShape::EqualityDelete { .. } => {
                novarocks_sql::planning::dml::DmlWriteSinkMode::EqualityDeletes
            }
        };
        let sink = dml_write_plan_input_for_admitted_target(
            target_bindings,
            target_binding,
            mode,
            novarocks_sql::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )?;
        routes.push(novarocks_sql::planning::dml::DmlChangeStreamRoute {
            route_id: route.route_id(),
            // The branch's identity is its sealed ordinal, never its position
            // in this loop.
            write_target_ordinal: write_target.ordinal(),
            accepted_effects: route.accepted_effects().to_vec(),
            input_fields: write_target
                .input()
                .fields()
                .into_iter()
                .map(
                    |field| novarocks_sql::planning::dml::DmlChangeStreamRouteField {
                        token: field.token(),
                        output_name: field.field().name().to_string(),
                    },
                )
                .collect(),
            partition_input_tokens: route.partition_fields().to_vec(),
            sink,
        });
    }
    Ok(routes)
}

fn incremental_change_stream_statistics_targets(
    write_session: &ConnectorWriteSession,
) -> Result<Vec<novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget>, String> {
    write_session
        .targets()
        .iter()
        .map(|target| {
            Ok(
                novarocks_sql::planning::dml::DmlChangeStreamStatisticsTarget {
                    write_target_ordinal: target.ordinal(),
                    requirements: write_session
                        .statistics_requirements(target.ordinal())
                        .map_err(|error| error.to_string())?
                        .to_vec(),
                },
            )
        })
        .collect()
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
    if !exact_lease.matches_provider_binding_key(&request.observed_binding) {
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
    // The physical write schema the target's own binding produced, which is the
    // same fact a first refresh signs its data input from.
    let target_write_fields = refresh_rewrite
        .target_arrow_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    // The session is opened before the plan is compiled because the plan's
    // writer nodes carry the recipes it seals: a plan and the session that
    // sealed it must not be separable.
    let write_session = begin_incremental_connector_write_session(
        &request,
        &publication_intent,
        mode,
        &target_write_fields,
        connector_context.clone(),
        exact_lease,
        planning_lease,
        query_kernel.typed_connector_control(),
    )?;
    match bind_incremental_write_dataflow(
        query_kernel,
        ports,
        &request,
        &refresh_rewrite,
        mode,
        evidence,
        execution_artifact,
        planning_lease,
        execution,
        &connector_context,
        &write_session,
    ) {
        Ok(assembly) => Ok(assembly),
        Err(error) => {
            release_mv_write_session_without_commit(&write_session, &connector_context);
            Err(error)
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Binding an incremental dataflow needs each independently frozen rewrite, target, and session fact."
)]
fn bind_incremental_write_dataflow(
    query_kernel: &QueryPreparationKernel,
    ports: &IcebergMvCorePorts,
    request: &MvIncrementalWriteRequest,
    refresh_rewrite: &Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>,
    mode: MvIncrementalWriteMode,
    evidence: MvIncrementalRewriteEvidence,
    execution_artifact: MvIncrementalExecutionArtifact,
    planning_lease: &ConnectorControlPlanningLease,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &Arc<ConnectorWriteSession>,
) -> Result<PreparedMvNativeWriteAssembly, String> {
    let target = crate::catalog_application::resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: request.target_catalog.clone(),
        namespace: request.target_namespace.clone(),
        table: request.target_name.clone(),
    };
    let target_bindings = Arc::new(QueryTableBindingStore::try_new()?);
    let target_binding = crate::query_execution::mv_assembly::query_local_bindings::bind_imv_target_query_table_in_store_from_rewrite(
        refresh_rewrite,
        &target_bindings,
        planning_lease,
        connector_context,
    )?;
    // The recipes are sealed once, here, and travel with the plan they were
    // sealed for, so an encode can never pair one round's plan with another's
    // session.
    let sealed_write_targets = write_session
        .seal_write_targets()
        .map_err(|error| format!("seal MV incremental write targets: {error}"))?;
    let sealed_change_stream_routes = incremental_change_stream_routes(
        write_session,
        &target,
        target_bindings.as_ref(),
        planning_lease,
    )?;
    let sealed_statistics_targets = incremental_change_stream_statistics_targets(write_session)?;
    let rewrite_evidence = match evidence {
        MvIncrementalRewriteEvidence::None => RewriteMergeRefreshEvidence::None,
        MvIncrementalRewriteEvidence::Aggregate => RewriteMergeRefreshEvidence::Aggregate,
        MvIncrementalRewriteEvidence::JoinAggregate => RewriteMergeRefreshEvidence::JoinAggregate,
        MvIncrementalRewriteEvidence::BranchUnionAggregate => {
            RewriteMergeRefreshEvidence::BranchUnionAggregate
        }
    };
    match execution_artifact {
        MvIncrementalExecutionArtifact::CanonicalQuery => {
            let imv_rewrite_input = sql_imv_planning_input_from_rewrite(
                refresh_rewrite,
                target_binding,
                rewrite_evidence,
            )?;
            let catalog_service_snapshot =
                crate::catalog_application::query_catalog::catalog_service_snapshot(query_kernel);
            let base_overlays = crate::query_execution::mv_assembly::query_local_bindings::freeze_imv_base_query_local_overlays_from_captured_inputs(
                ports.connector_control(),
                connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let analyzer_catalog = crate::catalog_application::query_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&target_bindings),
                crate::catalog_application::query_materializer::iceberg_table_binding_loader(
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
                    functions: query_kernel.function_catalog().as_ref(),
                    constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
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
                connector_context,
            )?;
            let sealed = novarocks_sql::planning::mv::first_refresh::compile_mv_incremental_refresh_change_stream(
                analyzed,
                &statistics,
                sealed_statistics_targets,
                // Every writer is an ordinary dataflow node whose rows gather
                // into one Root finish fragment; the session, not a terminal
                // sink, owns the commit.
                novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
            )?;
            session_native_assembly(
                query_kernel,
                execution,
                sealed,
                target_bindings.as_ref(),
                connector_context,
                write_session,
                sealed_write_targets,
            )
        }
        MvIncrementalExecutionArtifact::JoinLogical {
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
                connector_context,
                &refresh_rewrite.base_refs,
                &refresh_rewrite.pin,
                &refresh_rewrite.previous_snapshot_ids,
            )?;
            let catalog_service_snapshot =
                crate::catalog_application::query_catalog::catalog_service_snapshot(query_kernel);
            let analyzer_catalog = crate::catalog_application::query_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
                None,
                &catalog_service_snapshot,
                Arc::clone(&target_bindings),
                crate::catalog_application::query_materializer::iceberg_table_binding_loader(
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
                    functions: query_kernel.function_catalog().as_ref(),
                    constant_evaluator: crate::query_execution::constant_eval::constant_evaluator(),
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
                connector_context,
            )?;
            let sealed = novarocks_sql::planning::mv::first_refresh::compile_join_incremental_refresh_change_stream(
                analyzed,
                &statistics,
                sealed_statistics_targets,
                // Every writer is an ordinary dataflow node whose rows gather
                // into one Root finish fragment; the session, not a terminal
                // sink, owns the commit.
                novarocks_sql::planning::dml::DmlWritePlanShape::Dataflow,
            )?;
            session_native_assembly(
                query_kernel,
                execution,
                sealed,
                target_bindings.as_ref(),
                connector_context,
                write_session,
                sealed_write_targets,
            )
        }
    }
}

/// Pair one sealed change-stream plan with the session that admitted it.
///
/// The sealed recipes travel with the plan they were sealed for, and the session
/// rides along as the write's single commit authority -- so no operation,
/// cohort, or attempt identity reaches the writer data plane, and there is
/// nothing to re-check afterwards.
fn session_native_assembly(
    query_kernel: &QueryPreparationKernel,
    execution: &QueryExecutionContext,
    sealed: novarocks_sql::planning::dml::DmlChangeStreamPlan,
    target_bindings: &QueryTableBindingStore,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    write_session: &Arc<ConnectorWriteSession>,
    sealed_write_targets: SealedWriteTargets,
) -> Result<PreparedMvNativeWriteAssembly, String> {
    let planned = crate::query_execution::compiler::prepare_dml_change_stream_write_with_execution(
        query_kernel.connector_control().as_ref(),
        query_kernel.typed_connector_control(),
        execution,
        sealed,
        target_bindings,
        connector_context,
    )?;
    Ok(PreparedMvNativeWriteAssembly::session(
        planned
            .encoding
            .with_sealed_write_targets(sealed_write_targets),
        None,
        Arc::clone(write_session),
    ))
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::{
        ConnectorWriteRouteFacts, ConnectorWriteTargetPlan, ProviderWriteRuntime,
        WriteRuntimeAdapter, WriteTargetOrdinal,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRowMutationEffect, ConnectorWriteFieldBinding,
        ConnectorWriteFieldToken, ConnectorWriteInputShape, ConnectorWriteRouteId,
    };

    use super::change_stream_routed_targets;

    /// The provider-opaque writer payload. These tests assert branch ordinals,
    /// routes, and effects, none of which the payload takes part in.
    #[derive(Clone, Debug)]
    struct Value;

    struct FakeProvider {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl ProviderWriteRuntime for FakeProvider {
        type CommitHandle = Value;
        type WriterHandle = Value;
        type CommitFragment = Value;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<FakeProvider> {
        let instance_id = ConnectorInstanceId::parse("imv_routes_unit").expect("instance id");
        WriteRuntimeAdapter::new(std::sync::Arc::new(FakeProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fake").expect("provider id"),
                instance_id: instance_id.clone(),
            },
            catalog_handle: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([5; 32])),
        }))
    }

    fn binding(name: &str, tag: u8) -> ConnectorWriteFieldBinding {
        ConnectorWriteFieldBinding::new(
            ConnectorWriteFieldToken::from_bytes([tag; 32]),
            arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Int64, true),
        )
    }

    fn data_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::Data {
            fields: vec![binding("k1", 1)],
        }
    }

    fn deletion_vector_input() -> ConnectorWriteInputShape {
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![binding("_file", 2), binding("_pos", 3)],
            partition_source_fields: Vec::new(),
        }
    }

    fn route(key: u8, effects: Vec<ConnectorRowMutationEffect>) -> ConnectorWriteRouteFacts {
        ConnectorWriteRouteFacts::try_new(
            ConnectorWriteRouteId::from_bytes([key; 32]),
            effects,
            Vec::new(),
            Vec::new(),
        )
        .expect("route facts")
    }

    fn target(
        adapter: &WriteRuntimeAdapter<FakeProvider>,
        ordinal: u32,
        input: ConnectorWriteInputShape,
    ) -> ConnectorWriteTargetPlan {
        ConnectorWriteTargetPlan::new(
            WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"),
            adapter.wrap_writer_handle(Value),
            input,
        )
    }

    /// A fast-append refresh supersedes nothing, so its publication seals a
    /// single data branch -- but a *routed* one, because its rows arrive as
    /// change events. The branch accepts only `Insert`, and that is the one
    /// effect the router may deliver to it.
    #[test]
    fn a_fast_append_refresh_routes_one_insert_only_branch() {
        let adapter = adapter();
        let sealed = vec![
            target(&adapter, 0, data_input())
                .with_route(route(1, vec![ConnectorRowMutationEffect::Insert])),
        ];

        let routed = change_stream_routed_targets(&sealed).expect("one routed branch");

        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].0.ordinal().get(), 0);
        assert_eq!(
            routed[0].1.accepted_effects(),
            &[ConnectorRowMutationEffect::Insert]
        );
    }

    /// A row-delta refresh seals a delete branch beside its data branch, and
    /// each branch keeps its own sealed ordinal.
    ///
    /// The order matters, not just the membership: the router gives branch `i`
    /// the writer holding ordinal `i`, so branches taken in whatever order the
    /// provider handed them over would feed the deletion-vector writer the data
    /// branch's after-images. They are sorted by ordinal here, and the ordinal
    /// each route carries is its own rather than its position in the list.
    #[test]
    fn a_row_delta_refresh_keeps_each_branch_on_its_own_sealed_ordinal() {
        let adapter = adapter();
        // Handed over deliberately out of ordinal order.
        let sealed = vec![
            target(&adapter, 1, deletion_vector_input()).with_route(route(
                2,
                vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
            )),
            target(&adapter, 0, data_input()).with_route(route(
                1,
                vec![
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            )),
        ];

        let routed = change_stream_routed_targets(&sealed).expect("two routed branches");

        assert_eq!(
            routed
                .iter()
                .map(|(write_target, _)| write_target.ordinal().get())
                .collect::<Vec<_>>(),
            vec![0, 1],
        );
        // The data branch takes the after-images; the delete branch retires the
        // row versions they supersede.
        assert!(matches!(
            routed[0].0.input(),
            ConnectorWriteInputShape::Data { .. }
        ));
        assert!(matches!(
            routed[1].0.input(),
            ConnectorWriteInputShape::DeletionVector { .. }
        ));
        assert!(
            routed[1]
                .1
                .accepted_effects()
                .contains(&ConnectorRowMutationEffect::Delete),
            "a row-delta refresh must have somewhere to send a delete"
        );
    }

    /// A change-stream publication routes every branch. One that arrived without
    /// routing facts is a contract violation, and defaulting it into a route
    /// would silently hand it another branch's rows.
    #[test]
    fn a_branch_without_routing_facts_fails_closed() {
        let adapter = adapter();
        let sealed = vec![
            target(&adapter, 0, data_input())
                .with_route(route(1, vec![ConnectorRowMutationEffect::Insert])),
            target(&adapter, 1, deletion_vector_input()),
        ];

        let error = change_stream_routed_targets(&sealed)
            .expect_err("an unrouted branch cannot be routed to");

        assert!(
            error.contains("carries no provider routing facts"),
            "unexpected failure: {error}"
        );
    }
}
