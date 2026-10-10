// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Publication evidence and bounded cleanup are separate results.

use super::{ArtifactRecord, ObjectIdentity, OperationToken};
use novarocks_spi::connector::ConnectorMutationFailure;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupRemainingReason {
    BudgetExhausted,
    DeleteFailed(String),
    BridgeInterrupted(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemainingArtifact {
    pub object: ObjectIdentity,
    pub reason: CleanupRemainingReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergCleanupReport {
    Complete {
        deleted: usize,
    },
    Partial {
        deleted: usize,
        remaining: Vec<RemainingArtifact>,
    },
    /// A possibly dispatched publication keeps every object until adjudication.
    NotAttempted,
}

impl IcebergCleanupReport {
    pub(crate) fn finalization(&self) -> novarocks_spi::connector::ExternalMutationFinalization {
        use novarocks_spi::connector::{
            ConnectorMutationFailureKind, ExternalMutationFinalization,
        };
        match self {
            Self::Complete { .. } => ExternalMutationFinalization::Complete,
            Self::Partial { deleted, remaining } => {
                ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!(
                        "Iceberg owned artifact cleanup incomplete: deleted={deleted}, remaining={remaining:?}"
                    ),
                ))
            }
            Self::NotAttempted => {
                ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "Iceberg owned artifact cleanup completion was not attempted",
                ))
            }
        }
    }
}

#[derive(Debug)]
pub enum PublicationOutcome<Proof, Evidence> {
    Committed(Proof),
    KnownUncommitted(ConnectorMutationFailure),
    Unknown {
        failure: ConnectorMutationFailure,
        evidence: Evidence,
        operation: OperationToken,
        artifacts: Vec<ArtifactRecord>,
    },
}

#[derive(Debug)]
pub struct PublicationReport<Proof, Evidence> {
    pub publication: PublicationOutcome<Proof, Evidence>,
    pub cleanup: IcebergCleanupReport,
}
