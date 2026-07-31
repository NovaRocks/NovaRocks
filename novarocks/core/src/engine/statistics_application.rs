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

//! Typed Core-to-Frontend application port for unified statistics commands.
//!
//! This module deliberately contains no parser AST or SQL string. Core turns
//! its parser variants into these commands exactly once; the frontend owns
//! durable job state and implements this port without raw-SQL interception.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorControlRegistry, ConnectorRequestContext,
    ConnectorTableResolution, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Portable immutable table/version pin that the frontend may persist in a
/// durable ANALYZE job. It contains opaque provider bytes only—never a reader,
/// scan artifact, sketch, or executable runtime object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTablePin {
    pub connector_instance_id: String,
    pub table_handle: Vec<u8>,
    pub data_version: Vec<u8>,
}

/// Core resolves a logical ANALYZE target exactly once. The frontend invokes
/// this before creating a job and persists the returned pin; background work
/// has no latest-name resolution capability.
pub trait StatisticsTargetResolver: Send + Sync {
    fn resolve_table_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<StatisticsTablePin, StatisticsApplicationError>;
}

/// Frontend composition sink installed before engine open. Core calls it once
/// after connector control is ready, so ANALYZE submission can resolve and
/// persist a pin without giving the durable worker a resolver.
pub trait StatisticsTargetResolverSink: Send + Sync {
    fn bind_statistics_target_resolver(
        &self,
        resolver: Arc<dyn StatisticsTargetResolver>,
    ) -> Result<(), String>;
}

pub struct ConnectorStatisticsTargetResolver {
    controls: Arc<dyn ConnectorControlRegistry>,
}

impl ConnectorStatisticsTargetResolver {
    pub fn new(controls: Arc<dyn ConnectorControlRegistry>) -> Self {
        Self { controls }
    }
}

impl StatisticsTargetResolver for ConnectorStatisticsTargetResolver {
    fn resolve_table_pin(
        &self,
        target: &StatisticsTableTarget,
    ) -> Result<StatisticsTablePin, StatisticsApplicationError> {
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .map_err(|error| StatisticsApplicationError::new(error.to_string()))?;
        let (resolved, _) = crate::connector::metadata_load_table(
            self.controls.as_ref(),
            context,
            &target.catalog,
            &target.namespace,
            &target.table,
            ConnectorTableResolution::StrictBaseTable,
        )
        .map_err(StatisticsApplicationError::new)?;
        let pin = resolved.statistics_pin.ok_or_else(|| {
            StatisticsApplicationError::new(
                "connector metadata did not provide a statistics data-version pin",
            )
        })?;
        Ok(StatisticsTablePin {
            connector_instance_id: pin.table.owner().as_str().to_string(),
            table_handle: pin.table.payload().to_vec(),
            data_version: pin.data_version.as_bytes().to_vec(),
        })
    }
}

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationCommand {
    AnalyzeTable {
        target: StatisticsTableTarget,
        columns: Vec<String>,
    },
    ShowAnalyzeJobs,
    CancelAnalyze {
        job_id: Uuid,
    },
    ShowTableStats {
        target: StatisticsTableTarget,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobView {
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub state: String,
    pub attempt: u32,
    pub target: StatisticsTableTarget,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTableStatView {
    pub metric: String,
    pub value: Option<String>,
    pub status: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsApplicationResult {
    JobSubmitted(StatisticsJobView),
    JobCancellationRequested(StatisticsJobView),
    AnalyzeJobs(Vec<StatisticsJobView>),
    TableStats(Vec<StatisticsTableStatView>),
}

pub trait StatisticsApplicationPort: Send + Sync {
    fn execute(
        &self,
        command: StatisticsApplicationCommand,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError>;
}

/// Non-frontend composition must not gain an in-memory statistics authority.
/// It fails closed until a frontend explicitly installs the durable port.
pub struct UnavailableStatisticsApplicationPort;

impl StatisticsApplicationPort for UnavailableStatisticsApplicationPort {
    fn execute(
        &self,
        _command: StatisticsApplicationCommand,
    ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
        Err(StatisticsApplicationError::new(
            "unified statistics application service is not installed",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsApplicationError {
    message: String,
}

impl StatisticsApplicationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for StatisticsApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsApplicationError {}
