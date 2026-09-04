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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Current-process observations for ANALYZE.
//!
//! These values deliberately have no persistence codec, schema version, or
//! recovery representation. A frontend restart drops the whole runtime.

use uuid::Uuid;

use novarocks_spi::connector::LakePublicationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl From<super::application::StatisticsTableTarget> for StatisticsJobTarget {
    fn from(value: super::application::StatisticsTableTarget) -> Self {
        Self {
            catalog: value.catalog,
            namespace: value.namespace,
            table: value.table,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobCreate {
    pub target: StatisticsJobTarget,
    pub connector_instance_id: String,
    pub object_id: Vec<u8>,
    pub columns: super::application::StatisticsColumnIntent,
    pub submitted_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobState {
    Submitted,
    Preparing,
    Running,
    Succeeded,
    Failed,
    Stale,
    Cancelled,
    CommitUnknown,
}

impl StatisticsJobState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Stale | Self::Cancelled | Self::CommitUnknown
        )
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Submitted, Self::Preparing)
                | (Self::Submitted, Self::Cancelled)
                | (Self::Preparing, Self::Running)
                | (Self::Preparing, Self::Failed)
                | (Self::Preparing, Self::Stale)
                | (Self::Preparing, Self::Cancelled)
                | (Self::Running, Self::Succeeded)
                | (Self::Running, Self::CommitUnknown)
                | (Self::Running, Self::Failed)
                | (Self::Running, Self::Stale)
                | (Self::Running, Self::Cancelled)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobErrorKind {
    Configuration,
    Connector,
    Collection,
    Publish,
    TargetReplaced,
    TargetMissing,
    Cancelled,
    DeadlineExceeded,
    KnownCommittedFinalization,
    CommitUnknown,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
}

/// A bounded, current-process job observation. `operation_id` is still a v7
/// publication identity, but it is never used to recover or reconcile an old
/// attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJob {
    pub job_id: Uuid,
    pub operation_id: LakePublicationId,
    pub target: StatisticsJobTarget,
    pub connector_instance_id: String,
    pub object_id: Vec<u8>,
    pub columns: super::application::StatisticsColumnIntent,
    pub state: StatisticsJobState,
    pub attempt: u32,
    pub cancel_requested: bool,
    pub error: Option<StatisticsJobError>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

impl StatisticsJob {
    pub(crate) fn new(
        job_id: Uuid,
        operation_id: LakePublicationId,
        request: StatisticsJobCreate,
    ) -> Self {
        Self {
            job_id,
            operation_id,
            target: request.target,
            connector_instance_id: request.connector_instance_id,
            object_id: request.object_id,
            columns: request.columns,
            state: StatisticsJobState::Submitted,
            attempt: 1,
            cancel_requested: false,
            error: None,
            submitted_at_ms: request.submitted_at_ms,
            updated_at_ms: request.submitted_at_ms,
            completed_at_ms: None,
        }
    }
}
