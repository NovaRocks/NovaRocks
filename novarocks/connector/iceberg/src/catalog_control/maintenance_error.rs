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

//! Original failure facts from explicit Iceberg metadata maintenance.

use std::{error::Error, fmt};

use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection,
    connector_reason::{ICEBERG_COMMIT_ERROR, ICEBERG_FILESYSTEM_ERROR, ICEBERG_INVALID_METADATA},
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
};

#[derive(Debug)]
struct InvalidMetadata(String);

impl fmt::Display for InvalidMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for InvalidMetadata {}

impl FailureProjection for InvalidMetadata {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Data, ICEBERG_INVALID_METADATA)
    }
}

#[derive(Debug)]
struct FilesystemFailure(String);

impl fmt::Display for FilesystemFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for FilesystemFailure {}

impl FailureProjection for FilesystemFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Dependency, ICEBERG_FILESYSTEM_ERROR)
    }
}

/// `message` is the maintenance author's public diagnostic.
pub(crate) fn invalid_metadata(message: impl Into<String>) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::CorruptData,
        InvalidMetadata(message.into()),
    )
    .with_public_failure_detail()
}

/// `message` is the maintenance author's public diagnostic.
pub(crate) fn filesystem_failure(message: impl Into<String>) -> ConnectorError {
    ConnectorError::from_domain(
        ConnectorErrorKind::Unavailable,
        FilesystemFailure(message.into()),
    )
    .with_public_failure_detail()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MaintenanceCommitProcedure {
    OptimizeManifests,
    ExpireSnapshots,
}

impl MaintenanceCommitProcedure {
    fn name(self) -> &'static str {
        match self {
            Self::OptimizeManifests => "optimize_manifests",
            Self::ExpireSnapshots => "expire_snapshots",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitFailureDisposition {
    KnownUncommitted,
    Unknown,
}

#[derive(Debug)]
struct MaintenanceCommitFailure {
    procedure: MaintenanceCommitProcedure,
    disposition: CommitFailureDisposition,
    original: ConnectorMutationFailure,
}

impl fmt::Display for MaintenanceCommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to commit during {}: ", self.procedure.name())?;
        if self.disposition == CommitFailureDisposition::Unknown {
            f.write_str("[CommitUnknown] ")?;
        }
        let detail = self
            .original
            .connector_source()
            .and_then(ConnectorError::public_failure_detail)
            .unwrap_or(match (self.disposition, self.original.kind()) {
                (CommitFailureDisposition::Unknown, _) => "the catalog did not confirm the commit",
                (_, ConnectorMutationFailureKind::Conflict) => "catalog commit conflict",
                _ => "the catalog rejected the commit",
            });
        f.write_str(detail)
    }
}

impl Error for MaintenanceCommitFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.original)
    }
}

impl FailureProjection for MaintenanceCommitFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Dependency, ICEBERG_COMMIT_ERROR)
    }
}

/// Project an original commit failure after the owner has finished retry and
/// selected the terminal. The owner keeps the terminal, evidence and cleanup.
pub(crate) fn commit_failure(
    procedure: MaintenanceCommitProcedure,
    disposition: CommitFailureDisposition,
    original: ConnectorMutationFailure,
) -> ConnectorMutationFailure {
    let fact = original.failure_fact();
    if fact.reason() == ICEBERG_COMMIT_ERROR {
        return original;
    }
    if disposition == CommitFailureDisposition::KnownUncommitted
        && (matches!(
            fact.class(),
            FailureClass::Control
                | FailureClass::Resource
                | FailureClass::Unsupported
                | FailureClass::Representation
                | FailureClass::Invariant
        ) || matches!(
            fact.reason(),
            ICEBERG_INVALID_METADATA | ICEBERG_FILESYSTEM_ERROR
        ) || matches!(
            original.kind(),
            ConnectorMutationFailureKind::NotFound
                | ConnectorMutationFailureKind::Unauthenticated
                | ConnectorMutationFailureKind::PermissionDenied
        ) || original
            .connector_source()
            .is_some_and(|error| error.table_object_binding_failure().is_some()))
    {
        return original;
    }

    ConnectorMutationFailure::from_connector(
        ConnectorError::from_domain(
            ConnectorErrorKind::Unavailable,
            MaintenanceCommitFailure {
                procedure,
                disposition,
                original,
            },
        )
        .with_public_failure_detail(),
    )
}

#[cfg(test)]
#[path = "maintenance_error/tests.rs"]
mod tests;
