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

//! Connector-facing table-maintenance execution.
//!
//! SQL parsing, application dispatch, and result encoding belong to
//! the Frontend role application. Catalog, snapshot, file and commit truth belongs to
//! the Connector; this module only routes maintenance intents to it and shapes
//! the neutral outcome the frontend reports.

use crate::connector::metadata_maintenance::{MetadataMaintenanceCall, MetadataMaintenanceIntent};
use novarocks_table_maintenance::runtime::TerminalError;
use novarocks_table_maintenance::{
    AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceActionRequest,
    MaintenanceEffectId, MaintenanceTarget,
};

/// Dispatch one automatic metadata effect with the identity already frozen by
/// MV management. Keep the provider's terminal classification intact.
pub(crate) async fn execute_automatic_metadata_action_with_ports(
    pending: &mut Option<MetadataMaintenanceCall>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,

    request: MaintenanceActionRequest,
    effect_id: MaintenanceEffectId,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
    use crate::connector::metadata_maintenance::{
        MetadataMaintenanceDispatchState, MetadataMaintenanceIntent, ResolvedMetadataMaintenance,
    };
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorMutationOperationId, ConnectorTableIdentity,
        ExternalMutationFinalization,
    };

    let target = match &request {
        MaintenanceActionRequest::RewriteManifests { target, .. }
        | MaintenanceActionRequest::ExpireSnapshots { target, .. } => target,
        _ => {
            return Err(TerminalError::pre_dispatch_failed(
                "unsupported automatic metadata action",
            ));
        }
    };
    let instance_id = ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| TerminalError::pre_dispatch_failed(error.to_string()))?;
    let identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: target.namespace.clone().into(),
        table: target.table.clone().into(),
    };
    let intent = match &request {
        MaintenanceActionRequest::RewriteManifests {
            use_caching: None,
            spec_id: None,
            ..
        } => MetadataMaintenanceIntent::rewrite_metadata_layout(),
        MaintenanceActionRequest::ExpireSnapshots {
            older_than_ms,
            retain_last,
            ..
        } => MetadataMaintenanceIntent::expire_table_versions(*older_than_ms, *retain_last),
        _ => {
            return Err(TerminalError::pre_dispatch_failed(
                "unsupported automatic metadata action",
            ));
        }
    };
    let operation_id = ConnectorMutationOperationId::from_bytes(effect_id.to_bytes());
    let call = match original_call(
        pending,
        connector_control,
        &instance_id,
        operation_id,
        identity,
        intent,
        connector_context,
    ) {
        Ok(call) => call,
        Err(failure) => {
            return Err(match failure {
                ResolvedMetadataMaintenance::ContractFailure { error, .. } => {
                    TerminalError::pre_dispatch_failed(error.to_string())
                }
                _ => unreachable!("pure call construction only returns contract failure"),
            });
        }
    };
    let completed =
        match crate::connector::metadata_maintenance::resolve_metadata_maintenance(call).await {
            ResolvedMetadataMaintenance::KnownCommitted(completed) => *completed,
            ResolvedMetadataMaintenance::KnownUncommitted { failure, cleanup } => {
                return Err(TerminalError::known_uncommitted(
                    crate::connector::mutation::known_uncommitted_message(failure, &cleanup),
                ));
            }
            ResolvedMetadataMaintenance::CommitUnknown { failure, .. } => {
                return Err(TerminalError::commit_unknown(failure.to_string()));
            }
            ResolvedMetadataMaintenance::ContractFailure { error, dispatch } => {
                return Err(match dispatch {
                    MetadataMaintenanceDispatchState::ConfirmedNotDispatched => {
                        TerminalError::known_uncommitted(error.to_string())
                    }
                    MetadataMaintenanceDispatchState::PossiblyDispatched => {
                        TerminalError::commit_unknown(error.to_string())
                    }
                });
            }
        };
    if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
        return Err(TerminalError::known_committed_finalization_failed(
            failure.to_string(),
        ));
    }
    match request {
        MaintenanceActionRequest::RewriteManifests { .. } => {
            let summary = completed.receipt.summary();
            Ok(AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::RewriteManifests {
                    rewritten_manifests_count: i32::try_from(summary.rewritten_items).map_err(
                        |_| {
                            TerminalError::known_committed_finalization_failed(
                                "rewrite manifest count exceeds Spark result range",
                            )
                        },
                    )?,
                    added_manifests_count: i32::try_from(summary.added_items).map_err(|_| {
                        TerminalError::known_committed_finalization_failed(
                            "added manifest count exceeds Spark result range",
                        )
                    })?,
                },
            ))
        }
        MaintenanceActionRequest::ExpireSnapshots { .. } => {
            Ok(AutomaticMaintenanceOutcome::KnownCommitted(
                MaintenanceActionOutcome::ExpireSnapshots {
                    deleted_data_files_count: None,
                    deleted_position_delete_files_count: None,
                    deleted_equality_delete_files_count: None,
                    deleted_manifest_files_count: None,
                    deleted_manifest_lists_count: None,
                    deleted_statistics_files_count: None,
                },
            ))
        }
        _ => unreachable!("validated metadata action"),
    }
}

/// Execute a non-rewrite maintenance action through explicit frontend-owned
/// connector ports. The caller owns the request context;
/// this helper never captures a process-local cancellation or topology view.
pub(crate) async fn execute_action_with_ports(
    pending: &mut Option<MetadataMaintenanceCall>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,

    request: MaintenanceActionRequest,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    if let Some(call) = pending.as_mut()
        && let Some(intent) = call.trino_execute()
    {
        let matches = match (&request, intent) {
            (
                MaintenanceActionRequest::RewriteManifests {
                    use_caching: None,
                    spec_id: None,
                    target,
                },
                MetadataMaintenanceIntent::TrinoOptimizeManifests,
            )
            | (
                MaintenanceActionRequest::ExpireSnapshots { target, .. },
                MetadataMaintenanceIntent::TrinoExpireSnapshots { .. },
            ) => {
                let instance =
                    novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
                        .map_err(TerminalError::from_original_cause)?;
                let table = novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance.clone(),
                    namespace: target.namespace.clone().into(),
                    table: target.table.clone().into(),
                };
                match (&request, intent) {
                    (
                        MaintenanceActionRequest::ExpireSnapshots {
                            older_than_ms,
                            retain_last,
                            ..
                        },
                        MetadataMaintenanceIntent::TrinoExpireSnapshots {
                            older_than_ms: frozen_cutoff,
                            retain_last: frozen_retain_last,
                            ..
                        },
                    ) if *older_than_ms != Some(*frozen_cutoff)
                        || retain_last != frozen_retain_last =>
                    {
                        false
                    }
                    _ => call.matches(&instance, call.operation_id(), &table, intent),
                }
            }
            _ => false,
        };
        if !matches {
            return Err(TerminalError::pre_dispatch_failed(
                "EXECUTE differs from its retained metadata call",
            ));
        }
        return run_trino_metadata_procedure(call).await;
    }
    match request {
        MaintenanceActionRequest::RewriteManifests { .. } => {
            Err("rewrite manifests requires its retained table procedure".to_string())
        }
        MaintenanceActionRequest::ExpireSnapshots {
            target,
            older_than_ms,
            retain_last,
        } => {
            run_expire_snapshots_action_with_ports(
                pending,
                connector_control,
                target,
                older_than_ms,
                retain_last,
                connector_context,
            )
            .await
        }
        MaintenanceActionRequest::RemoveOrphanFiles { .. } => Err(
            "remove orphan files must be dispatched by the frontend durable cleanup owner"
                .to_string(),
        ),
    }
    .map_err(TerminalError::from)
}

async fn run_trino_metadata_procedure(
    call: &mut MetadataMaintenanceCall,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    use crate::connector::metadata_maintenance::{
        KnownUncommittedMetadataMaintenance, MetadataMaintenanceDispatchState,
        ResolvedMetadataMaintenance,
    };
    use novarocks_spi::connector::ExternalMutationFinalization;
    use novarocks_table_maintenance::ProcedureMetrics;

    let completion =
        crate::connector::metadata_maintenance::resolve_metadata_maintenance(call).await;
    let completed = match completion {
        ResolvedMetadataMaintenance::KnownCommitted(completed) => completed,
        ResolvedMetadataMaintenance::KnownUncommitted {
            failure: KnownUncommittedMetadataMaintenance::Planning(error),
            ..
        } => {
            return Err(procedure_connector_terminal(
                error,
                TerminalError::pre_dispatch_failed,
            ));
        }
        ResolvedMetadataMaintenance::KnownUncommitted {
            failure: KnownUncommittedMetadataMaintenance::Provider(failure),
            cleanup,
        } => {
            let detail = crate::connector::mutation::known_uncommitted_message(&failure, &cleanup);
            return Err(procedure_mutation_terminal(
                failure,
                detail,
                TerminalError::known_uncommitted,
            ));
        }
        ResolvedMetadataMaintenance::CommitUnknown { failure } => {
            let detail = failure.to_string();
            return Err(procedure_mutation_terminal(
                failure,
                detail,
                TerminalError::commit_unknown,
            ));
        }
        ResolvedMetadataMaintenance::ContractFailure {
            error,
            dispatch: MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
        } => {
            return Err(procedure_connector_terminal(
                error,
                TerminalError::pre_dispatch_failed,
            ));
        }
        ResolvedMetadataMaintenance::ContractFailure {
            error,
            dispatch: MetadataMaintenanceDispatchState::PossiblyDispatched,
        } => {
            return Err(procedure_connector_terminal(
                error,
                TerminalError::commit_unknown,
            ));
        }
    };
    if let ExternalMutationFinalization::Failed(failure) = completed.finalization {
        let detail = failure.to_string();
        return Err(procedure_mutation_terminal(
            failure,
            detail,
            TerminalError::known_committed_finalization_failed,
        ));
    }
    let metrics = completed
        .receipt
        .summary()
        .manifest_rewrite_metrics
        .map(|metrics| {
            ProcedureMetrics::from_counts([
                ("rewritten_manifests_count", metrics.rewritten),
                ("added_manifests_count", metrics.added),
                ("kept_manifests_count", metrics.kept),
                (
                    "processed_manifest_entries_count",
                    metrics.processed_entries,
                ),
            ])
        })
        .transpose()
        .map_err(TerminalError::known_committed_finalization_failed)?;
    Ok(MaintenanceActionOutcome::Procedure { metrics })
}

pub(crate) fn retention_format_error(
    terminal: TerminalError,
    procedure: &'static str,
    target: &MaintenanceTarget,
) -> TerminalError {
    use novarocks_failure_contract::{BoundedDetail, SharedFailureCause};
    use novarocks_spi::connector::{ConnectorError, ConnectorTableFormatVersionUnsupported};
    use std::error::Error as _;

    if terminal.state
        != novarocks_table_maintenance::runtime::MaintenanceJobState::PreDispatchFailed
    {
        return terminal;
    }
    let Some(cause) = terminal.failure_cause.as_ref() else {
        return terminal;
    };
    let typed = cause
        .downcast_ref::<ConnectorTableFormatVersionUnsupported>()
        .or_else(|| {
            cause
                .downcast_ref::<ConnectorError>()?
                .source()?
                .downcast_ref::<SharedFailureCause>()?
                .downcast_ref::<ConnectorTableFormatVersionUnsupported>()
        });
    let Some(&ConnectorTableFormatVersionUnsupported(version)) = typed else {
        return terminal;
    };
    let detail = format!(
        "{procedure} is not supported for Iceberg table format version > 3. Table {}.{} format version is {version}.",
        target.namespace, target.table,
    );
    TerminalError::pre_dispatch_failed(detail.clone()).with_failure_cause(Some(
        SharedFailureCause::from_bounded_detail(
            ConnectorTableFormatVersionUnsupported(version),
            BoundedDetail::truncating(&detail),
        ),
    ))
}

pub(crate) fn invalid_procedure_error(
    terminal: &TerminalError,
) -> Option<novarocks_query_application::session_error::QueryServiceError> {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use novarocks_table_maintenance::runtime::MaintenanceJobState;
    if terminal.state != MaintenanceJobState::PreDispatchFailed {
        return None;
    }
    let cause = terminal.failure_cause.as_ref()?;
    let error = cause.downcast_ref::<ConnectorError>()?;
    if error.kind() != ConnectorErrorKind::InvalidRequest {
        return None;
    }
    let message = error.public_failure_detail()?;
    Some(
        novarocks_query_application::sql::table_procedure::invalid_procedure_request(
            cause.clone(),
            message,
        ),
    )
}

fn procedure_mutation_terminal(
    failure: novarocks_spi::connector::ConnectorMutationFailure,
    detail: String,
    terminal: impl FnOnce(String) -> TerminalError,
) -> TerminalError {
    match failure.connector_source().cloned() {
        Some(error) => procedure_connector_terminal(error, terminal),
        None => terminal(detail).with_failure_cause(Some(
            novarocks_failure_contract::SharedFailureCause::from_static_detail(
                failure,
                "Iceberg table procedure failed",
            ),
        )),
    }
}

pub(super) fn procedure_connector_terminal(
    error: novarocks_spi::connector::ConnectorError,
    terminal: impl FnOnce(String) -> TerminalError,
) -> TerminalError {
    use novarocks_failure_contract::{BoundedDetail, SharedFailureCause};
    let detail = error
        .public_failure_detail()
        .unwrap_or("Iceberg table procedure failed")
        .to_owned();
    terminal(detail.clone()).with_failure_cause(Some(SharedFailureCause::from_bounded_detail(
        error,
        BoundedDetail::truncating(&detail),
    )))
}

/// Read the current snapshot on a caller-supplied exact request context.
async fn run_expire_snapshots_action_with_ports(
    pending: &mut Option<MetadataMaintenanceCall>,
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,

    target: MaintenanceTarget,
    older_than_ms: Option<i64>,
    retain_last: Option<u32>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MaintenanceActionOutcome, String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
        .map_err(|error| error.to_string())?;
    let operation_id = pending.as_ref().map_or_else(
        novarocks_spi::connector::ConnectorMutationOperationId::new,
        MetadataMaintenanceCall::operation_id,
    );
    let call = original_call(
        pending,
        connector_control,
        &instance_id,
        operation_id,
        novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: instance_id.clone(),
            namespace: target.namespace.clone().into(),
            table: target.table.clone().into(),
        },
        MetadataMaintenanceIntent::expire_table_versions(older_than_ms, retain_last),
        connector_context,
    )
    .map_err(|error| format!("{error:?}"))?;
    crate::connector::metadata_maintenance::execute_metadata_maintenance(call).await?;

    tracing::info!(
        catalog = %target.catalog,
        namespace = %target.namespace,
        table = %target.table,
        "expire_snapshots: completed"
    );

    Ok(MaintenanceActionOutcome::ExpireSnapshots {
        deleted_data_files_count: None,
        deleted_position_delete_files_count: None,
        deleted_equality_delete_files_count: None,
        deleted_manifest_files_count: None,
        deleted_manifest_lists_count: None,
        deleted_statistics_files_count: None,
    })
}

/// The caller's original durable action row owns `pending`. Never clear this
/// field just because a wait was cancelled or an outcome is still Unknown.
fn original_call<'a>(
    pending: &'a mut Option<MetadataMaintenanceCall>,
    resolver: &dyn novarocks_spi::connector::ConnectorMetadataMaintenanceResolver,
    instance: &novarocks_spi::connector::ConnectorInstanceId,
    operation: novarocks_spi::connector::ConnectorMutationOperationId,
    table: novarocks_spi::connector::ConnectorTableIdentity,
    intent: MetadataMaintenanceIntent,
    context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<
    &'a mut MetadataMaintenanceCall,
    crate::connector::metadata_maintenance::ResolvedMetadataMaintenance,
> {
    if let Some(call) = pending.as_ref() {
        if !call.matches(instance, operation, &table, &intent) {
            return Err(crate::connector::metadata_maintenance::ResolvedMetadataMaintenance::ContractFailure {
                error:novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "maintenance wait changes the original action identity"),
                dispatch:crate::connector::metadata_maintenance::MetadataMaintenanceDispatchState::ConfirmedNotDispatched,
            });
        }
    } else {
        *pending = Some(MetadataMaintenanceCall::try_new(
            resolver, instance, operation, table, intent, context,
        )?);
    }
    Ok(pending
        .as_mut()
        .expect("original maintenance call is installed"))
}
