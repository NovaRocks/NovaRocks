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

//! Closed CPU reader for the original MV write analysis inputs.
//! Catalog misses suspend only through the existing named metadata I/O gate.

use super::{
    activation_cpu::{MvWriteAnalysisContext, MvWriteAnalysisInput},
    first_refresh_staging::{
        finish_frozen_mv_rewrite_context_original, validate_frozen_mv_rewrite_identity,
    },
    incremental_staging::{
        RewriteMergeRefreshEvidence, incremental_change_stream_routes,
        incremental_change_stream_statistics_targets, sql_imv_planning_input_from_rewrite,
    },
    refresh_artifact::{MvFirstRefreshExecutionArtifact, MvIncrementalExecutionArtifact},
    refresh_handoff::{PreparedMvRefreshWrite, PreparedMvRefreshWriteArtifact},
    refresh_preparation::{
        async_original::{MvCatalogCaptureInput, MvCatalogCaptureQuery, capture_catalog},
        io_gate::{MvRefreshPreparationIo, contract_error, provider_error},
    },
};
use crate::{
    catalog_application::query_bindings::QueryTableBindingStore,
    mv::domain::{
        application::MvApplicationError, iceberg_refresh::IcebergMvCorePorts,
        refresh::target_binding::MvTargetBinding,
    },
    query_execution::{
        kernels::QueryPreparationKernel,
        planning::write_sink::{
            admit_session_connector_write_target, dml_write_plan_input_for_admitted_target,
        },
        write_session::ConnectorWriteSession,
    },
};
use novarocks_query_application::admitted_query_context::QueryExecutionContext;
use novarocks_spi::connector::{ConnectorControlPlanningLease, ConnectorRequestContext};
use std::sync::Arc;

/// The prepared refresh is move-only and belongs to this original reader.
/// The caller must retain its actual session, target and required domain facts in
/// stable receiving fields; this reader grants no new Root or execution.
pub(crate) struct MvWriteAnalysisPreparationInput {
    pub(crate) kernel: QueryPreparationKernel,
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) prepared: PreparedMvRefreshWrite,
    pub(crate) target: MvTargetBinding,
    pub(crate) planning: ConnectorControlPlanningLease,
    pub(crate) execution: QueryExecutionContext,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) session: Arc<ConnectorWriteSession>,
}

pub(crate) struct MvWriteAnalysisPrepared {
    pub(crate) analysis: MvWriteAnalysisInput,
    pub(crate) bindings: Arc<QueryTableBindingStore>,
}

/// Polled only by MvRefreshPreparationOriginal on the original CPU factory.
/// This is a concrete closed reader, not an arbitrary future execution port.
pub(crate) async fn prepare_analysis(
    input: MvWriteAnalysisPreparationInput,
    io: &MvRefreshPreparationIo,
) -> Result<MvWriteAnalysisPrepared, MvApplicationError> {
    let bindings = Arc::new(QueryTableBindingStore::try_new().map_err(contract_error)?);
    let control =
        crate::query_execution::planning::sql_compile_control_from_execution(&input.execution);
    let settings = input.execution.optimizer_settings().clone();
    let (artifact, sources) = input.prepared.into_assembly_artifact();
    match artifact {
        PreparedMvRefreshWriteArtifact::FirstRefresh(prepared) => {
            let current_catalog = prepared.current_catalog().map(str::to_owned);
            let current_database = prepared.current_database().to_owned();
            let root_hash_column = prepared.root_hash_column().to_owned();
            let expected_snapshot = prepared.expected_target_snapshot_id();
            let target_catalog = prepared.target_catalog().to_owned();
            let target_namespace = prepared.target_namespace().to_owned();
            let target_name = prepared.target_name().to_owned();
            let write_target =
                super::first_refresh_staging::sole_publication_write_target(&input.session)
                    .map_err(contract_error)?;
            let ordinal = write_target.ordinal();
            seal_analysis_write_targets(&input.session)?;
            let write_binding = admit_session_connector_write_target(
                &bindings,
                novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
                    target_catalog.clone(),
                    target_namespace.clone(),
                    target_name.clone(),
                )
                .map_err(contract_error)?,
                write_target,
                input.planning.clone(),
            )
            .map_err(contract_error)?;
            let sink = dml_write_plan_input_for_admitted_target(
                &bindings,
                write_binding,
                novarocks_sql::planning::dml::DmlWriteSinkMode::Data,
                novarocks_sql::planning::dml::ConnectorWriteInputBinding::RootOutputByOrdinal,
            )
            .map_err(contract_error)?;
            let required = input
                .session
                .statistics_requirements(ordinal)
                .map_err(provider_error)?
                .to_vec();
            let (capture, rewrite) = match prepared.into_execution_artifact() {
                MvFirstRefreshExecutionArtifact::Sql(source) => {
                    (MvCatalogCaptureQuery::FirstSql(source), None)
                }
                MvFirstRefreshExecutionArtifact::Logical(logical) => {
                    let facts = logical.into_context();
                    let identity = validate_frozen_mv_rewrite_identity(
                        &target_catalog,
                        &target_namespace,
                        &target_name,
                        &facts,
                    )
                    .map_err(contract_error)?;
                    validate_target(&input.target, &input.planning, &identity)?;
                    let rewrite = finish_frozen_mv_rewrite_context_original(
                        &input.ports,
                        expected_snapshot,
                        &facts,
                        &input.target,
                        &input.context,
                    )?;
                    let target_binding =
                        super::query_local_bindings::bind_imv_target_query_table_from_metadata(
                            &rewrite,
                            &bindings,
                            input.planning.clone(),
                            input.target.metadata().clone(),
                            Some(&facts.affected_partitions),
                        )
                        .map_err(contract_error)?;
                    let overlay = facts.frozen_base_overlays.ok_or_else(|| {
                        contract_error(
                            "MV logical activation is missing its original base overlays",
                        )
                    })?;
                    let capture = capture_catalog(
                        MvCatalogCaptureInput {
                            query: MvCatalogCaptureQuery::Query(Box::new(
                                (*rewrite.canonical_select_query).clone(),
                            )),
                            ports: input.ports.clone(),
                            current_catalog: current_catalog.clone(),
                            current_database: current_database.clone(),
                            context: input.context.clone(),
                            control: control.clone(),
                            bindings: bindings.clone(),
                            overlays: overlay,
                        },
                        io,
                    )
                    .await?;
                    (
                        capture.query,
                        Some((
                            capture.catalog,
                            rewrite
                                .to_sql_rewrite_snapshot(target_binding)
                                .map_err(contract_error)?,
                        )),
                    )
                }
            };
            let (query, catalog, rewrite) = match rewrite {
                Some((catalog, rewrite)) => (capture, catalog, Some(rewrite)),
                None => {
                    let captured = capture_catalog(
                        MvCatalogCaptureInput {
                            query: capture,
                            ports: input.ports.clone(),
                            current_catalog: current_catalog.clone(),
                            current_database: current_database.clone(),
                            context: input.context.clone(),
                            control: control.clone(),
                            bindings: bindings.clone(),
                            overlays: super::query_local_bindings::freeze_captured_source_overlays(
                                &sources,
                            )
                            .map_err(|error| error.0)?,
                        },
                        io,
                    )
                    .await?;
                    (captured.query, captured.catalog, None)
                }
            };
            let context = MvWriteAnalysisContext {
                catalog,
                functions: input.kernel.function_catalog().clone(),
                constant_policy: input.kernel.constant_policy(),
                control,
                current_catalog,
                current_database,
                settings,
            };
            let analysis = match (query, rewrite) {
                (MvCatalogCaptureQuery::FirstSql(source), None) => MvWriteAnalysisInput::FirstSql {
                    source,
                    context,
                    sink,
                    required,
                    ordinal,
                },
                (MvCatalogCaptureQuery::Query(query), Some(rewrite)) => {
                    MvWriteAnalysisInput::FirstJoin {
                        query,
                        rewrite,
                        root_hash_column,
                        context,
                        sink,
                        required,
                        ordinal,
                    }
                }
                _ => {
                    return Err(contract_error(
                        "MV first-refresh catalog returned a foreign original query",
                    ));
                }
            };
            Ok(MvWriteAnalysisPrepared { analysis, bindings })
        }
        PreparedMvRefreshWriteArtifact::Incremental(prepared) => {
            let (request, facts, mode, evidence, artifact, _) = prepared.into_parts();
            let identity = validate_frozen_mv_rewrite_identity(
                &request.target_catalog,
                &request.target_namespace,
                &request.target_name,
                &facts,
            )
            .map_err(contract_error)?;
            validate_target(&input.target, &input.planning, &identity)?;
            if input.target.lease().control_runtime_id() != input.planning.control_runtime_id()
                || request.observed_binding.instance_id
                    != input.planning.binding().descriptor().instance_id
                || request.observed_binding.incarnation != input.planning.binding().incarnation()
            {
                return Err(contract_error(
                    "MV incremental activation retained a foreign target generation",
                ));
            }
            let rewrite = finish_frozen_mv_rewrite_context_original(
                &input.ports,
                request.expected_target_snapshot_id,
                &facts,
                &input.target,
                &input.context,
            )?;
            let target_binding =
                super::query_local_bindings::bind_imv_target_query_table_from_metadata(
                    &rewrite,
                    &bindings,
                    input.planning.clone(),
                    input.target.metadata().clone(),
                    Some(&facts.affected_partitions),
                )
                .map_err(contract_error)?;
            seal_analysis_write_targets(&input.session)?;
            let target = crate::catalog_application::resolver::TargetBackend {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                catalog: request.target_catalog,
                namespace: request.target_namespace,
                table: request.target_name,
            };
            let routes = incremental_change_stream_routes(
                &input.session,
                &target,
                &bindings,
                &input.planning,
            )
            .map_err(contract_error)?;
            let statistics_targets = incremental_change_stream_statistics_targets(&input.session)
                .map_err(contract_error)?;
            let overlays = facts.frozen_base_overlays.ok_or_else(|| {
                contract_error("MV incremental activation is missing its original base overlays")
            })?;
            let capture = capture_catalog(
                MvCatalogCaptureInput {
                    query: MvCatalogCaptureQuery::Query(Box::new(
                        (*rewrite.canonical_select_query).clone(),
                    )),
                    ports: input.ports.clone(),
                    current_catalog: rewrite.current_catalog.clone(),
                    current_database: rewrite.current_database.clone(),
                    context: input.context.clone(),
                    control: control.clone(),
                    bindings: bindings.clone(),
                    overlays,
                },
                io,
            )
            .await?;
            let MvCatalogCaptureQuery::Query(query) = capture.query else {
                return Err(contract_error(
                    "MV incremental catalog returned a foreign original query",
                ));
            };
            let context = MvWriteAnalysisContext {
                catalog: capture.catalog,
                functions: input.kernel.function_catalog().clone(),
                constant_policy: input.kernel.constant_policy(),
                control,
                current_catalog: rewrite.current_catalog.clone(),
                current_database: rewrite.current_database.clone(),
                settings,
            };
            let mode = match mode {
                novarocks_mv_application::product::MvIncrementalWriteMode::FastAppend =>
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::FastAppend,
                novarocks_mv_application::product::MvIncrementalWriteMode::RowDelta =>
                    novarocks_sql::planning::mv::first_refresh::SqlMvIncrementalWriteMode::RowDelta,
            };
            let analysis = match artifact {
                MvIncrementalExecutionArtifact::CanonicalQuery => {
                    use novarocks_mv_application::product::MvIncrementalRewriteEvidence as E;
                    let evidence = match evidence {
                        E::None => RewriteMergeRefreshEvidence::None,
                        E::Aggregate => RewriteMergeRefreshEvidence::Aggregate,
                        E::JoinAggregate => RewriteMergeRefreshEvidence::JoinAggregate,
                        E::BranchUnionAggregate => {
                            RewriteMergeRefreshEvidence::BranchUnionAggregate
                        }
                    };
                    MvWriteAnalysisInput::IncrementalCanonical {
                        query,
                        rewrite: sql_imv_planning_input_from_rewrite(
                            &rewrite,
                            target_binding,
                            evidence,
                        )
                        .map_err(contract_error)?,
                        mode,
                        context,
                        routes,
                        statistics_targets,
                    }
                }
                MvIncrementalExecutionArtifact::JoinLogical { mode: join_mode } => {
                    let join_mode = match join_mode {
                        novarocks_mv_application::product::MvIncrementalJoinMode::AppendOnly =>
                            novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::AppendOnly,
                        novarocks_mv_application::product::MvIncrementalJoinMode::Coalesce =>
                            novarocks_sql::planning::mv::first_refresh::SqlMvJoinIncrementalRefreshMode::Coalesce,
                    };
                    MvWriteAnalysisInput::IncrementalJoin {
                        query,
                        rewrite: rewrite
                            .to_sql_rewrite_snapshot(target_binding)
                            .map_err(contract_error)?,
                        join_mode,
                        mode,
                        context,
                        routes,
                        statistics_targets,
                    }
                }
            };
            Ok(MvWriteAnalysisPrepared { analysis, bindings })
        }
    }
}

fn validate_target(
    target: &MvTargetBinding,
    planning: &ConnectorControlPlanningLease,
    identity: &novarocks_types::naming::TableIdentity,
) -> Result<(), MvApplicationError> {
    if target.identity().instance_id.as_str() != identity.catalog
        || target.identity().namespace.as_ref() != identity.namespace
        || target.identity().table.as_ref() != identity.table
        || target.lease().control_runtime_id() != planning.control_runtime_id()
    {
        return Err(contract_error(
            "MV activation retained a foreign exact target binding",
        ));
    }
    Ok(())
}

/// Validate the same session's actual sealed recipes before analysis consumes
/// their ordinals. The original Connector refusal remains a Provider failure.
fn seal_analysis_write_targets(session: &ConnectorWriteSession) -> Result<(), MvApplicationError> {
    session.seal_write_targets().map_err(provider_error)?;
    Ok(())
}

#[cfg(test)]
mod original_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    #[test]
    fn actual_session_seal_refusal_keeps_its_connector_fact_and_one_received_cause() {
        use crate::query_execution::write_session::tests::{fixture_with_outcome, known_committed};
        // The real session encoder and single-handle author refuse this value;
        // no simulated SQL/control error stands in for the Connector request.
        let fixture = fixture_with_outcome(
            1,
            novarocks_spi::connector::write_stack::MAX_CONNECTOR_WRITER_HANDLE_BYTES + 1,
            known_committed(),
        );
        let error = seal_analysis_write_targets(&fixture.session)
            .expect_err("the original writer-handle author must refuse the oversized recipe");
        let cause = error
            .original_failure()
            .expect("actual Connector refusal is retained")
            .clone();
        let connector = cause
            .downcast_ref::<ConnectorError>()
            .expect("the first capture owns the actual ConnectorError");
        assert_eq!(connector.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(cause.failure_fact(), connector.failure_fact());
        assert_eq!(cause.failure_fact().class(), FailureClass::Resource);
        assert_eq!(
            error.kind(),
            crate::mv::domain::application::MvApplicationErrorKind::Engine
        );
        assert_eq!(error.compile_control_error(), None);
        let received = error.clone();
        assert_eq!(received.original_failure(), Some(&cause));
        assert_eq!(
            std::error::Error::source(&received)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
            Some(&cause)
        );
        assert_eq!(
            received.original_failure().unwrap().failure_fact(),
            connector.failure_fact()
        );
        let recorded = fixture.recorded.lock().unwrap();
        assert_eq!(recorded.begin, 1);
        assert_eq!(recorded.finish, 0);
        assert_eq!(recorded.abort, 0);
        drop(recorded);
        fixture
            .session
            .abort_for_test(crate::query_execution::write_session::tests::request_context())
            .expect("retire the same original uncommitted session");
        assert_eq!(fixture.recorded.lock().unwrap().abort, 1);
    }
}
