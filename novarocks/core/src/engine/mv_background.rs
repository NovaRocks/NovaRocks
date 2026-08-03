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

//! Core adapter bound to the frontend-owned MV background runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::mv::refresh_io::{load_current_iceberg_base_table, parse_iceberg_table_refs};
use crate::engine::table_maintenance::MaintenanceTarget;
use crate::mv::background::{
    MvBackgroundEngine, MvBackgroundEngineError, MvBackgroundEngineErrorKind, MvMaintenanceFacts,
    MvRefreshStep,
};
use crate::mv::dependency::model::iceberg_mv_dependency_ref;
use crate::mv::model::{MvStorageEngine, MvTarget};
use crate::sql::mv_refresh::{
    MvRefreshAttemptIdentity, MvRefreshPreparationRequest, MvRefreshPreparationService,
    MvRefreshStatement, PreparedMvRefresh,
};
use novarocks_spi::connector::ConnectorRequestContext;

#[derive(Clone)]
pub(crate) struct StandaloneMvBackgroundEngine {
    state: Arc<StandaloneState>,
}

impl StandaloneMvBackgroundEngine {
    pub(crate) fn new(state: Arc<StandaloneState>) -> Self {
        Self { state }
    }

    fn definition_for_target(
        &self,
        target: &MvTarget,
    ) -> Result<crate::mv::persistence::definition::StoredMvDefinition, MvBackgroundEngineError>
    {
        let definition = self
            .state
            .mv_repository
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
            crate::engine::mv::dependency::build_upstream_refresh_steps(&self.state, &requested)
                .map_err(|error| {
                    MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        error,
                    )
                })?;
        steps
            .into_iter()
            .map(|step| {
                if step.storage_engine != MvStorageEngine::Iceberg {
                    return Err(MvBackgroundEngineError::new(
                        MvBackgroundEngineErrorKind::InvalidDefinition,
                        format!(
                            "MV refresh step {} is not Iceberg-backed",
                            step.object.display_name()
                        ),
                    ));
                }
                let mv_id = self.definition_for_target(&step.target)?.mv_id;
                Ok(MvRefreshStep {
                    mv_id,
                    target: step.target,
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
        let ast_statement = crate::sql::parser::ast::RefreshMaterializedViewStmt {
            name: crate::sql::parser::ast::ObjectName {
                parts: vec![step.target.name.clone()],
            },
            full: false,
        };
        let service =
            crate::engine::mv::iceberg_refresh::StandaloneMvRefreshPreparationService::new(
                &self.state,
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
        refs.into_iter()
            .map(|table_ref| {
                let snapshot = load_current_iceberg_base_table(&self.state, &table_ref)
                    .map_err(|error| {
                        MvBackgroundEngineError::new(
                            MvBackgroundEngineErrorKind::TransientUnavailable,
                            error,
                        )
                    })?
                    .table
                    .metadata()
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id());
                Ok((table_ref.fqn(), snapshot))
            })
            .collect()
    }

    fn maintenance_facts(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<MvMaintenanceFacts, MvBackgroundEngineError> {
        let definitions = self
            .state
            .mv_repository
            .list_definitions()
            .map_err(repository_error)?;
        let stats = crate::engine::mv_maintenance::stats::collect_table_stats(
            &self.state,
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
            non_main_ref_count: stats.non_main_ref_count,
            downstream_floor_ts_ms: stats.downstream_floor_ts_ms,
            downstream_floor_unknown: stats.downstream_floor_unknown,
            properties: stats.properties.into_iter().collect(),
        })
    }
}

fn repository_error(error: crate::mv::repository::MvRepositoryError) -> MvBackgroundEngineError {
    use crate::mv::repository::MvRepositoryErrorKind;

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
