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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_failure_contract::connector_reason::{
    COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN, ICEBERG_COMMIT_VALIDATION_CONFLICT,
};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, FailureReason};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
};

use super::model::EntryIdentity;
use crate::iceberg::spec::DataFile;

/// Paths and identities needed by every prepare attempt, including after the
/// operation has converted its compact additions into reusable manifests.
#[derive(Debug, Default)]
pub(crate) struct RowDeltaValidationFacts {
    pub(crate) referenced: BTreeSet<Arc<str>>,
    pub(crate) removed: BTreeSet<Arc<str>>,
    pub(crate) dv_targets: BTreeSet<Arc<str>>,
    pub(crate) replaced: Vec<EntryIdentity>,
}

/// One immutable manifest entry's partition, metrics and content, borrowed
/// directly from the manifest data file.
pub(crate) struct CandidateFile<'a> {
    pub(crate) spec_id: i32,
    pub(crate) file: &'a DataFile,
}

/// Inclusive predicate evaluation for conflict validation.
pub(crate) trait MayMatch: fmt::Display + Send + Sync {
    fn may_match(&self, file: &CandidateFile<'_>) -> Result<bool, ConnectorError>;
}

#[derive(Debug)]
struct CommitFailureCause {
    reason: FailureReason,
    message: Arc<str>,
    original: Option<ConnectorMutationFailure>,
}

impl fmt::Display for CommitFailureCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CommitFailureCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
    }
}

impl FailureProjection for CommitFailureCause {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(FailureClass::Dependency, self.reason)
    }
}

fn commit_failure(
    kind: ConnectorMutationFailureKind,
    reason: FailureReason,
    message: Arc<str>,
    original: Option<ConnectorMutationFailure>,
) -> ConnectorMutationFailure {
    let source = ConnectorError::from_domain(
        ConnectorErrorKind::Unavailable,
        CommitFailureCause {
            reason,
            message: message.clone(),
            original,
        },
    );
    ConnectorMutationFailure::new(kind, message).with_connector_source(source)
}

pub(crate) fn validation_conflict(message: impl Into<Arc<str>>) -> ConnectorMutationFailure {
    commit_failure(
        ConnectorMutationFailureKind::Conflict,
        ICEBERG_COMMIT_VALIDATION_CONFLICT,
        message.into(),
        None,
    )
}

pub(crate) fn transaction_conflict(
    original: ConnectorMutationFailure,
    attempts: u32,
) -> ConnectorMutationFailure {
    let message = format!("{} (after {attempts} commit attempts)", original.message()).into();
    commit_failure(
        ConnectorMutationFailureKind::Conflict,
        COMMIT_TRANSACTION_CONFLICT,
        message,
        Some(original),
    )
}

pub(crate) fn commit_state_unknown(original: ConnectorMutationFailure) -> ConnectorMutationFailure {
    commit_failure(
        original.kind(),
        ICEBERG_COMMIT_STATE_UNKNOWN,
        original.message().into(),
        Some(original),
    )
}

#[cfg(test)]
mod tests;
