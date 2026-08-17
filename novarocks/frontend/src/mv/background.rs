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

//! Frontend-owned contracts and bindings for MV background workers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use novarocks::maintenance::{MaintenanceTarget, TableMaintenanceEngine};
use novarocks::mv::repository::MvTarget;
use novarocks::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, PreparedMvRefresh,
};
use novarocks_spi::connector::ConnectorRequestContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvBackgroundEngineErrorKind {
    TargetGone,
    TransientUnavailable,
    InvalidDefinition,
    RecoveryRequired,
    Corruption,
    InvariantViolation,
    ShutdownCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvBackgroundEngineError {
    kind: MvBackgroundEngineErrorKind,
    message: String,
}

impl MvBackgroundEngineError {
    pub(crate) fn new(kind: MvBackgroundEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) const fn kind(&self) -> MvBackgroundEngineErrorKind {
        self.kind
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvBackgroundEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvBackgroundEngineError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRefreshStep {
    pub(crate) mv_id: i64,
    pub(crate) target: MvTarget,
}

/// The provider facts required to apply maintenance policy without leaking an
/// Iceberg metadata object into the frontend.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MvMaintenanceFacts {
    pub(crate) current_snapshot_id: Option<i64>,
    pub(crate) total_data_files: Option<i64>,
    pub(crate) max_compactable_data_files: Option<i64>,
    pub(crate) total_delete_files: Option<i64>,
    pub(crate) total_files_size_bytes: Option<i64>,
    pub(crate) oldest_snapshot_timestamp_ms: Option<i64>,
    pub(crate) snapshot_count: usize,
    pub(crate) non_default_reference_count: usize,
    pub(crate) downstream_floor_ts_ms: Option<i64>,
    pub(crate) downstream_floor_unknown: bool,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value; the frontend owns every default and
    /// every clamp. These four fields are the only way maintenance policy
    /// crosses this boundary — there is deliberately no property map fallback.
    pub(crate) maintenance_enabled: Option<bool>,
    pub(crate) expire_max_snapshot_age_ms: Option<i64>,
    pub(crate) expire_min_snapshots_to_keep: Option<u32>,
    pub(crate) target_file_size_bytes: Option<i64>,
}

/// Side-effect-free discovery and preparation capability consumed by
/// frontend-owned MV background workers.
pub(crate) trait MvBackgroundEngine: Send + Sync {
    fn resolve_refresh_steps(
        &self,
        target: &MvTarget,
    ) -> Result<Vec<MvRefreshStep>, MvBackgroundEngineError>;

    fn prepare_refresh_step(
        &self,
        step: &MvRefreshStep,
        attempt: MvRefreshAttemptIdentity,
        connector_context: &ConnectorRequestContext,
    ) -> Result<PreparedMvRefresh, MvBackgroundEngineError>;

    fn current_base_snapshots(
        &self,
        target: &MvTarget,
    ) -> Result<BTreeMap<String, Option<i64>>, MvBackgroundEngineError>;

    fn maintenance_facts(
        &self,
        target: &MaintenanceTarget,
    ) -> Result<MvMaintenanceFacts, MvBackgroundEngineError>;
}

/// Bound only after Core has restored catalogs, performed MV recovery, bound
/// providers and started table-maintenance recovery. The frontend starts and
/// owns worker threads after it receives this value.
#[derive(Clone)]
pub(crate) struct MvBackgroundBindings {
    pub(crate) engine: Arc<dyn MvBackgroundEngine>,
    pub(crate) table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
}

pub(crate) trait MvBackgroundEngineSink: Send + Sync {
    fn bind_mv_background_engine(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError>;
}

#[cfg(test)]
mod tests {
    use super::{MvBackgroundEngineError, MvBackgroundEngineErrorKind};

    #[test]
    fn typed_error_keeps_retry_policy_out_of_display_text() {
        let error = MvBackgroundEngineError::new(
            MvBackgroundEngineErrorKind::TransientUnavailable,
            "connector lease is temporarily unavailable",
        );
        assert_eq!(
            error.kind(),
            MvBackgroundEngineErrorKind::TransientUnavailable
        );
        assert_eq!(
            error.to_string(),
            "connector lease is temporarily unavailable"
        );
    }
}
