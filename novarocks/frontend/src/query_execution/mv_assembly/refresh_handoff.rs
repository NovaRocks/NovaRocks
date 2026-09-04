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

use novarocks_spi::connector::{
    ConnectorProviderBindingKey, ConnectorWriteOperationId, LakePublicationId,
};

use super::refresh_artifact::{
    MvRefreshPublicationIntent, PreparedMvFirstRefreshWrite, PreparedMvIncrementalWrite,
};
use novarocks_sql::planning::mv::{MvRefreshFinalizeFacts, MvRefreshStatement, SqlMvTarget};

use crate::mv::domain::lifecycle::RefreshError;

/// Frontend-preallocated identity for one MV refresh publication.
///
/// A publication ID is the sole cross-boundary identity. Provider-specific
/// write IDs and ref names are derived from it at the assembly boundary, so a
/// refresh cannot accidentally split one publication across unrelated
/// operation IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshAttemptIdentity {
    pub publication_id: LakePublicationId,
}

impl MvRefreshAttemptIdentity {
    pub fn validate(&self) -> Result<(), String> {
        Ok(())
    }

    pub fn write_operation_id(&self) -> ConnectorWriteOperationId {
        self.publication_id.into()
    }

    pub fn staging_branch(&self) -> String {
        format!("__novarocks_mv_publication_{}", self.publication_id)
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
#[expect(
    clippy::large_enum_variant,
    reason = "The refresh handoff keeps the exact prepared-work payload at the frontend boundary."
)]
pub enum PreparedMvRefreshWork {
    NoOp,
    MetadataOnly { intent: MvRefreshPublicationIntent },
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
    pub observed_binding: ConnectorProviderBindingKey,
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
    ) -> Result<PreparedMvRefresh, RefreshError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt() -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity {
            publication_id: LakePublicationId::new_v7(),
        }
    }

    #[test]
    fn refresh_attempt_is_lifecycle_owned() {
        attempt().validate().expect("complete attempt identity");
        let attempt = attempt();
        assert_eq!(
            attempt.write_operation_id().to_bytes(),
            attempt.publication_id.to_bytes()
        );
        assert_eq!(
            attempt.staging_branch(),
            format!("__novarocks_mv_publication_{}", attempt.publication_id)
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
