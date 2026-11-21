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

//! Iceberg's sole author of operation-specific public commit failure detail.

use std::fmt;

use novarocks_failure_contract::connector_reason::{
    COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_ERROR, ICEBERG_COMMIT_STATE_UNKNOWN,
    ICEBERG_COMMIT_VALIDATION_CONFLICT,
};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, FailureReason};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
};

use super::model::{CommitOperation, CommitStyle};

const UNKNOWN_MARKER: &str = "[CommitUnknown] ";

#[derive(Clone, Copy, Debug)]
pub(crate) enum CommitFailureKind {
    Validation,
    Contention { attempts: u32 },
    Rejected,
    Unknown,
}

#[derive(Debug)]
struct PublicCommitFailure {
    reason: FailureReason,
    message: String,
    original: ConnectorMutationFailure,
}

impl fmt::Display for PublicCommitFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PublicCommitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}

impl FailureProjection for PublicCommitFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Dependency, self.reason)
    }
}

/// The caller supplies public detail selected by the original Provider owner.
pub(crate) fn commit_failure(
    operation: CommitOperation,
    kind: CommitFailureKind,
    original: ConnectorMutationFailure,
    detail: &str,
) -> ConnectorMutationFailure {
    let prefix = match operation {
        CommitOperation::Legacy => return original,
        CommitOperation::Write => "Failed to commit the transaction during write: ",
        CommitOperation::Optimize => "Failed to commit the transaction during optimize: ",
        CommitOperation::Truncate => "Failed to commit during truncate: ",
        CommitOperation::Delete => "Failed to commit during delete: ",
        CommitOperation::OptimizeManifests => "Failed to commit during optimize_manifests: ",
        CommitOperation::ExpireSnapshots => "Failed to commit during expire_snapshots: ",
    };
    let (reason, disposition, message) = match kind {
        CommitFailureKind::Validation => (
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
            ConnectorMutationFailureKind::Conflict,
            format!("{prefix}{detail}"),
        ),
        CommitFailureKind::Contention { attempts } => (
            match operation.style() {
                CommitStyle::Transaction => COMMIT_TRANSACTION_CONFLICT,
                CommitStyle::Update => ICEBERG_COMMIT_ERROR,
                CommitStyle::Legacy => unreachable!("legacy failure kept its original projection"),
            },
            ConnectorMutationFailureKind::Conflict,
            format!("{prefix}{detail} (after {attempts} commit attempts)"),
        ),
        CommitFailureKind::Rejected => (
            ICEBERG_COMMIT_ERROR,
            original.kind(),
            format!("{prefix}{detail}"),
        ),
        CommitFailureKind::Unknown => (
            ICEBERG_COMMIT_STATE_UNKNOWN,
            original.kind(),
            format!(
                "{prefix}{UNKNOWN_MARKER}{}",
                detail.trim_start_matches(UNKNOWN_MARKER)
            ),
        ),
    };
    let source = ConnectorError::from_domain(
        ConnectorErrorKind::Unavailable,
        PublicCommitFailure {
            reason,
            message: message.clone(),
            original,
        },
    )
    .with_public_failure_detail();
    ConnectorMutationFailure::new(disposition, message).with_connector_source(source)
}

#[cfg(test)]
mod tests;
