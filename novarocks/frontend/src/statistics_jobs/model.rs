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

use novarocks_spi::connector::{
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_STATISTICS_PINNED_COLUMNS: usize = 128;
const MAX_STATISTICS_PINNED_COLUMN_BYTES: usize = 256;

/// The durable schema carried by a statistics job record.
pub const STATISTICS_JOB_SCHEMA_VERSION: u8 = 2;

/// A stable table reference for a submitted ANALYZE request.
///
/// It deliberately contains no scan artifact, sketch, or runtime handle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatisticsJobTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// Immutable connector table/data-version pin resolved when ANALYZE is
/// submitted. The worker consumes it directly and must never resolve the
/// logical name to latest after it owns the job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatisticsJobTablePin {
    pub connector_instance_id: String,
    pub table_handle: Vec<u8>,
    pub data_version: Vec<u8>,
    /// Columns resolved alongside the pinned table handle. This is metadata
    /// only (not a statistics artifact) and lets an empty ANALYZE column list
    /// expand without rereading the latest table schema in a worker.
    #[serde(default)]
    pub columns: Vec<String>,
}

impl StatisticsJobTablePin {
    pub fn validate(&self) -> Result<(), String> {
        novarocks_spi::connector::ConnectorInstanceId::parse(&self.connector_instance_id)
            .map_err(|error| format!("invalid statistics connector instance ID: {error}"))?;
        if self.table_handle.is_empty()
            || self.table_handle.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
        {
            return Err("statistics table handle is empty or exceeds the SPI bound".to_string());
        }
        if self.data_version.is_empty()
            || self.data_version.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
        {
            return Err("statistics data version is empty or exceeds the SPI bound".to_string());
        }
        if self.columns.len() > MAX_STATISTICS_PINNED_COLUMNS
            || self.columns.iter().any(|column| {
                column.is_empty() || column.len() > MAX_STATISTICS_PINNED_COLUMN_BYTES
            })
            || self.columns.iter().enumerate().any(|(index, column)| {
                self.columns[..index]
                    .iter()
                    .any(|seen| seen.eq_ignore_ascii_case(column))
            })
        {
            return Err("statistics pinned columns are invalid or exceed the bound".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobCreate {
    pub target: StatisticsJobTarget,
    pub table_pin: StatisticsJobTablePin,
    pub metric_names: Vec<String>,
    pub submitted_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatisticsJobState {
    Submitted,
    Preparing,
    Running,
    Publishing,
    Succeeded,
    Failed,
    Cancelled,
}

impl StatisticsJobState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Submitted, Self::Preparing)
                | (Self::Preparing, Self::Running)
                | (Self::Running, Self::Publishing)
                // A new fenced owner may replay only work that has not
                // crossed the external publish boundary. Re-claiming the
                // returned SUBMITTED job increments the same durable
                // operation's attempt counter.
                | (Self::Preparing, Self::Submitted)
                | (Self::Running, Self::Submitted)
                | (Self::Publishing, Self::Succeeded)
                | (Self::Preparing, Self::Failed)
                | (Self::Running, Self::Failed)
                | (Self::Publishing, Self::Failed)
                | (Self::Submitted, Self::Cancelled)
                | (Self::Preparing, Self::Cancelled)
                | (Self::Running, Self::Cancelled)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StatisticsJobErrorKind {
    Configuration,
    Connector,
    Collection,
    Publish,
    Cancelled,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatisticsJobError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJob {
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub target: StatisticsJobTarget,
    pub table_pin: StatisticsJobTablePin,
    pub metric_names: Vec<String>,
    pub state: StatisticsJobState,
    pub attempt: u32,
    pub retry_not_before_ms: Option<i64>,
    /// Bounded opaque operation evidence used only to reconcile a publish
    /// whose external commit outcome became unknown. It is not a statistics
    /// artifact, sketch, or execution handle.
    pub publication_evidence: Option<Vec<u8>>,
    /// Client intent only. The fenced worker performs the state transition to
    /// CANCELLED, so an unfenced session cannot race publication.
    pub cancel_requested: bool,
    pub error: Option<StatisticsJobError>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredStatisticsJobV2 {
    pub schema_version: u8,
    pub job_id: Uuid,
    pub operation_id: Uuid,
    pub target: StatisticsJobTarget,
    pub table_pin: StatisticsJobTablePin,
    pub metric_names: Vec<String>,
    pub state: StatisticsJobState,
    pub attempt: u32,
    #[serde(default)]
    pub retry_not_before_ms: Option<i64>,
    #[serde(default)]
    pub publication_evidence: Option<Vec<u8>>,
    #[serde(default)]
    pub cancel_requested: bool,
    pub error: Option<StatisticsJobError>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

impl From<&StoredStatisticsJobV2> for StatisticsJob {
    fn from(value: &StoredStatisticsJobV2) -> Self {
        Self {
            job_id: value.job_id,
            operation_id: value.operation_id,
            target: value.target.clone(),
            table_pin: value.table_pin.clone(),
            metric_names: value.metric_names.clone(),
            state: value.state,
            attempt: value.attempt,
            retry_not_before_ms: value.retry_not_before_ms,
            publication_evidence: value.publication_evidence.clone(),
            cancel_requested: value.cancel_requested,
            error: value.error.clone(),
            submitted_at_ms: value.submitted_at_ms,
            updated_at_ms: value.updated_at_ms,
            completed_at_ms: value.completed_at_ms,
        }
    }
}
