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

//! Provider-neutral facts and preparation ports for frontend MV workers.
//!
//! The frontend owns worker admission, retry policy, cancellation and join.
//! Core only binds this narrow adapter after restore and provider recovery.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use novarocks_spi::connector::ConnectorRequestContext;

use crate::engine::table_maintenance::{MaintenanceTarget, TableMaintenanceEngine};
use crate::mv::model::MvTarget;
use crate::sql::mv_refresh::{MvRefreshAttemptIdentity, PreparedMvRefresh};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvBackgroundEngineErrorKind {
    TargetGone,
    TransientUnavailable,
    InvalidDefinition,
    RecoveryRequired,
    Corruption,
    InvariantViolation,
    ShutdownCancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvBackgroundEngineError {
    kind: MvBackgroundEngineErrorKind,
    message: String,
}

impl MvBackgroundEngineError {
    pub fn new(kind: MvBackgroundEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> MvBackgroundEngineErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
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
pub struct MvRefreshStep {
    pub mv_id: i64,
    pub target: MvTarget,
}

/// The provider facts required to apply maintenance policy without leaking an
/// Iceberg metadata object into the frontend.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvMaintenanceFacts {
    pub current_snapshot_id: Option<i64>,
    pub total_data_files: Option<i64>,
    pub max_compactable_data_files: Option<i64>,
    pub total_delete_files: Option<i64>,
    pub total_files_size_bytes: Option<i64>,
    pub oldest_snapshot_timestamp_ms: Option<i64>,
    pub snapshot_count: usize,
    pub non_main_ref_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
    pub properties: BTreeMap<String, String>,
}

/// Side-effect-free discovery and preparation capability consumed by
/// frontend-owned MV background workers.
pub trait MvBackgroundEngine: Send + Sync {
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
/// providers and started table-maintenance recovery.  The frontend starts and
/// owns worker threads after it receives this value.
#[derive(Clone)]
pub struct MvBackgroundBindings {
    pub engine: Arc<dyn MvBackgroundEngine>,
    pub table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
}

pub trait MvBackgroundEngineSink: Send + Sync {
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
