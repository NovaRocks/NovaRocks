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

//! Frontend adapter bound to the MV background runtime.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::mv::domain::dependency::model::iceberg_mv_dependency_ref;
use crate::mv::domain::dependency::refresh::build_upstream_refresh_steps_with_repository;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::{
    definition::parse_iceberg_table_refs, observation::observe_current_refresh_base,
};
use crate::mv::domain::repository::MvTarget;
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, MvRefreshPreparationRequest, MvRefreshPreparationService,
    PreparedMvRefresh,
};
use novarocks::maintenance::MaintenanceTarget;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorControlRegistry, ConnectorRequestContext,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_sql::planning::mv::MvRefreshStatement;

use super::background::{
    MvBackgroundEngine, MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
    MvRefreshStep,
};

struct BackgroundConnectorCancellation {
    signal: Arc<AtomicBool>,
}

impl ConnectorCancellation for BackgroundConnectorCancellation {
    fn is_cancelled(&self) -> bool {
        self.signal.load(Ordering::SeqCst)
    }
}

fn background_connector_request_context() -> Result<ConnectorRequestContext, String> {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(300),
        Arc::new(BackgroundConnectorCancellation {
            signal: Arc::new(AtomicBool::new(false)),
        }),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())
}

#[derive(Clone)]
pub(crate) struct StandaloneMvBackgroundEngine {
    ports: IcebergMvCorePorts,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    repository: Arc<dyn crate::mv::domain::repository::MvRepository>,
    storage_observation: Arc<dyn novarocks_spi::connector::MvStorageObservationPort>,
}

impl StandaloneMvBackgroundEngine {
    pub(crate) fn new_with_ports(
        ports: IcebergMvCorePorts,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        repository: Arc<dyn crate::mv::domain::repository::MvRepository>,
        storage_observation: Arc<dyn novarocks_spi::connector::MvStorageObservationPort>,
    ) -> Self {
        Self {
            ports,
            connector_control,
            repository,
            storage_observation,
        }
    }

    fn definition_for_target(
        &self,
        target: &MvTarget,
    ) -> Result<
        crate::mv::domain::persistence::definition::StoredMvDefinition,
        MvBackgroundEngineError,
    > {
        let definition = self
            .repository
            .find_by_target(target)
            .map_err(repository_error)?
            .ok_or_else(|| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TargetGone,
                    format!("MV target {} no longer exists", target.display_name()),
                )
            })?;
        if definition.storage_engine != "iceberg" {
            return Err(MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvalidDefinition,
                format!("MV {} is not Iceberg-backed", definition.mv_id),
            ));
        }
        Ok(definition)
    }
}

impl MvBackgroundEngine for StandaloneMvBackgroundEngine {
    fn resolve_refresh_steps(
        &self,
        target: &MvTarget,
    ) -> Result<Vec<MvRefreshStep>, MvBackgroundEngineError> {
        let requested = iceberg_mv_dependency_ref(
            target.catalog.as_deref().unwrap_or("default_catalog"),
            &target.database,
            &target.name,
        );
        let steps =
            build_upstream_refresh_steps_with_repository(self.repository.as_ref(), &requested)
                .map_err(|error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        error,
                    )
                })?;
        steps
            .into_iter()
            .map(|step| {
                if !step.is_iceberg() {
                    return Err(MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        format!(
                            "MV refresh step {} is not Iceberg-backed",
                            step.display_name()
                        ),
                    ));
                }
                let mv_id = self.definition_for_target(step.target())?.mv_id;
                Ok(MvRefreshStep {
                    mv_id,
                    target: step.into_target(),
                })
            })
            .collect()
    }

    fn prepare_refresh_step(
        &self,
        step: &MvRefreshStep,
        attempt: MvRefreshAttemptIdentity,
        connector_context: &ConnectorRequestContext,
    ) -> Result<PreparedMvRefresh, MvBackgroundEngineError> {
        let statement = MvRefreshStatement {
            name_parts: vec![step.target.name.clone()],
            full: false,
        };
        let ast_statement = novarocks_sql::syntax::RefreshMaterializedViewStmt {
            name: novarocks_sql::syntax::ObjectName {
                parts: vec![step.target.name.clone()],
            },
            full: false,
        };
        let service = crate::query_execution::mv_assembly::refresh_preparation::StandaloneMvRefreshPreparationService::new_with_ports(
            &self.ports,
            step.target.catalog.as_deref(),
            &step.target.database,
            &ast_statement,
            connector_context,
        );
        service
            .prepare_step(MvRefreshPreparationRequest {
                statement,
                target: step.target.clone(),
                attempt,
            })
            .map_err(|error| {
                MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::InvalidDefinition, error)
            })
    }

    fn current_base_snapshots(
        &self,
        target: &MvTarget,
    ) -> Result<BTreeMap<String, Option<i64>>, MvBackgroundEngineError> {
        let definition = self.definition_for_target(target)?;
        let refs = parse_iceberg_table_refs(&definition.base_table_refs).map_err(|error| {
            MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::InvalidDefinition, error)
        })?;
        let connector_context = background_connector_request_context().map_err(|error| {
            MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
        })?;
        refs.into_iter()
            .map(|table_ref| {
                let snapshot = observe_current_refresh_base(
                    self.connector_control.as_ref(),
                    self.storage_observation.as_ref(),
                    &table_ref,
                    &connector_context,
                )
                .map_err(|error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::TransientUnavailable,
                        error,
                    )
                })?
                .current_snapshot_id();
                Ok((table_ref.fqn(), snapshot))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    }

    fn maintenance_facts(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<MvMaintenanceFacts, MvBackgroundEngineError> {
        let definitions = self
            .repository
            .list_definitions()
            .map_err(repository_error)?;
        let stats = crate::mv::domain::maintenance::stats::collect_table_stats_with_ports(
            self.connector_control.as_ref(),
            self.storage_observation.as_ref(),
            &target.catalog,
            &target.namespace,
            &target.table,
            &definitions,
        )
        .map_err(|error| {
            MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
        })?;
        Ok(MvMaintenanceFacts {
            current_snapshot_id: stats.current_snapshot_id,
            total_data_files: stats.total_data_files.map(|value| value as i64),
            max_compactable_data_files: stats.max_compactable_data_files.map(|value| value as i64),
            total_delete_files: stats.total_delete_files.map(|value| value as i64),
            total_files_size_bytes: stats.total_files_size_bytes.map(|value| value as i64),
            oldest_snapshot_timestamp_ms: stats
                .snapshots
                .iter()
                .map(|snapshot| snapshot.timestamp_ms)
                .min(),
            snapshot_count: stats.snapshots.len(),
            non_default_reference_count: stats.non_default_reference_count,
            downstream_floor_ts_ms: stats.downstream_floor_ts_ms,
            downstream_floor_unknown: stats.downstream_floor_unknown,
            maintenance_enabled: stats.maintenance_enabled,
            expire_max_snapshot_age_ms: stats.expire_max_snapshot_age_ms,
            expire_min_snapshots_to_keep: stats.expire_min_snapshots_to_keep,
            target_file_size_bytes: stats.target_file_size_bytes,
        })
    }
}

fn repository_error(
    error: crate::mv::domain::repository::MvRepositoryError,
) -> MvBackgroundEngineError {
    use crate::mv::domain::repository::MvRepositoryErrorKind;

    let kind = match error.kind() {
        MvRepositoryErrorKind::NotFound => MvBackgroundEngineErrorKind::TargetGone,
        MvRepositoryErrorKind::Unavailable => MvBackgroundEngineErrorKind::TransientUnavailable,
        MvRepositoryErrorKind::Corruption => MvBackgroundEngineErrorKind::Corruption,
        MvRepositoryErrorKind::CommitUnknown
        | MvRepositoryErrorKind::KnownCommittedFinalizeFailed => {
            MvBackgroundEngineErrorKind::RecoveryRequired
        }
        MvRepositoryErrorKind::InvalidRequest | MvRepositoryErrorKind::Conflict => {
            MvBackgroundEngineErrorKind::InvariantViolation
        }
    };
    MvBackgroundEngineError::new(kind, error.to_string())
}
