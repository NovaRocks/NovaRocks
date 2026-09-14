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
use std::sync::Arc;

use crate::query_execution::maintenance::TableMaintenanceEngine;
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, PreparedMvRefresh,
};
use novarocks_mv_application::maintenance::{MvBackgroundEngineError, MvMaintenanceFacts};
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_table_maintenance::MaintenanceTarget;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRefreshStep {
    pub(crate) mv_id: i64,
    pub(crate) target: MvTarget,
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
