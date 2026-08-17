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

//! Frontend refresh-lifecycle handoff assembled from Core SQL facts.

use novarocks_spi::connector::{ConnectorExecutionBindingKey, ConnectorWriteOperationId};

use super::refresh_artifact::{
    MvRefreshPublicationIntent, PreparedMvFirstRefreshWrite, PreparedMvIncrementalWrite,
};
use novarocks_sql::planning::mv::{MvRefreshFinalizeFacts, MvRefreshStatement, SqlMvTarget};

/// Frontend-preallocated identities for a single MV refresh lifecycle. These
/// are application lifecycle values: SQL may validate them but cannot create
/// a connector operation or persist their durable intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshAttemptIdentity {
    pub refresh_id: i64,
    pub request_id: [u8; 16],
    pub staging_branch: String,
    pub marker_token: String,
    pub staging_create_operation_id: [u8; 16],
    pub write_operation_id: ConnectorWriteOperationId,
    pub publication_operation_id: [u8; 16],
    pub staging_drop_operation_id: [u8; 16],
}

impl MvRefreshAttemptIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.refresh_id <= 0 || self.staging_branch.is_empty() || self.marker_token.is_empty() {
            return Err(
                "MV refresh preparation requires a positive identity and non-empty staging marker"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Application request supplied to the side-effect-free SQL preparation port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPreparationRequest {
    pub statement: MvRefreshStatement,
    pub target: SqlMvTarget,
    pub attempt: MvRefreshAttemptIdentity,
}

impl MvRefreshPreparationRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.statement.validate_supported()?;
        self.attempt.validate()
    }
}

/// Application-owned work handoff after SQL planning. SQL plans determine the
/// semantic shape; this lifecycle envelope owns operation/cohort-bearing
/// artifacts and is the only value admitted by frontend staging.
pub enum PreparedMvRefreshWork {
    NoOp,
    MetadataOnly,
    DataProducing { write: PreparedMvRefreshWrite },
}

/// Exactly one SQL-prepared staged write for a data-producing refresh.
///
/// The public application handoff intentionally hides the assembly-specific
/// first-refresh and incremental artifact shapes. Frontend lifecycle code can
/// validate its operation, cohort, and publication facts, but only the
/// query-assembly activation adapter may recover the exact artifact needed to
/// construct a native write.
pub struct PreparedMvRefreshWrite {
    artifact: PreparedMvRefreshWriteArtifact,
}

/// The assembly-facing shape remains crate-private so it cannot become part
/// of the public refresh-lifecycle contract.
pub(crate) enum PreparedMvRefreshWriteArtifact {
    FirstRefresh(PreparedMvFirstRefreshWrite),
    Incremental(PreparedMvIncrementalWrite),
}

impl PreparedMvRefreshWrite {
    pub(crate) fn first_refresh(write: PreparedMvFirstRefreshWrite) -> Self {
        Self {
            artifact: PreparedMvRefreshWriteArtifact::FirstRefresh(write),
        }
    }

    pub(crate) fn incremental(write: PreparedMvIncrementalWrite) -> Self {
        Self {
            artifact: PreparedMvRefreshWriteArtifact::Incremental(write),
        }
    }

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => write.operation_id(),
            PreparedMvRefreshWriteArtifact::Incremental(write) => write.operation_id(),
        }
    }

    pub fn primary_cohort(&self) -> novarocks_spi::connector::ConnectorWriteCohortId {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => write.primary_cohort(),
            PreparedMvRefreshWriteArtifact::Incremental(write) => write.primary_cohort(),
        }
    }

    pub fn publication_intent(&self) -> &MvRefreshPublicationIntent {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => write.publication_intent(),
            PreparedMvRefreshWriteArtifact::Incremental(write) => write.publication_intent(),
        }
    }

    pub(crate) fn into_assembly_artifact(self) -> PreparedMvRefreshWriteArtifact {
        self.artifact
    }
}

/// Frontend lifecycle artifact assembled from SQL facts and a reserved attempt.
pub struct PreparedMvRefresh {
    pub statement: MvRefreshStatement,
    pub attempt: MvRefreshAttemptIdentity,
    pub observed_binding: ConnectorExecutionBindingKey,
    pub finalize: MvRefreshFinalizeFacts,
    pub work: PreparedMvRefreshWork,
}

/// SQL preparation port consumed by the frontend lifecycle owner. Its request
/// and output are application envelopes around immutable SQL values, never a
/// way for SQL to acquire lifecycle or connector authority itself.
pub trait MvRefreshPreparationService: Send + Sync {
    fn prepare_step(
        &self,
        request: MvRefreshPreparationRequest,
    ) -> Result<PreparedMvRefresh, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt() -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity {
            refresh_id: 7,
            request_id: [1; 16],
            staging_branch: "__nova_mv_7".to_string(),
            marker_token: "marker".to_string(),
            staging_create_operation_id: [2; 16],
            write_operation_id: ConnectorWriteOperationId::from_bytes([3; 16]),
            publication_operation_id: [4; 16],
            staging_drop_operation_id: [5; 16],
        }
    }

    #[test]
    fn refresh_attempt_is_lifecycle_owned() {
        attempt().validate().expect("complete attempt identity");
        assert!(
            MvRefreshAttemptIdentity {
                marker_token: String::new(),
                ..attempt()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn refresh_request_keeps_sql_rejection_and_attempt_together() {
        let request = MvRefreshPreparationRequest {
            statement: MvRefreshStatement {
                name_parts: vec!["mv".to_string()],
                full: false,
            },
            target: SqlMvTarget {
                catalog: Some("iceberg".to_string()),
                database: "db".to_string(),
                name: "mv".to_string(),
            },
            attempt: attempt(),
        };
        request.validate().expect("complete request");
        assert!(
            MvRefreshPreparationRequest {
                statement: MvRefreshStatement {
                    full: true,
                    ..request.statement
                },
                ..request
            }
            .validate()
            .is_err()
        );
    }
}
