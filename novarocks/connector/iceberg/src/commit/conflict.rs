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

use novarocks_spi::connector::{ConnectorError, ConnectorMutationFailure};

use super::commit_failure::{CommitFailureKind, original_commit_failure, validation_failure};
use super::model::{CommitOperation, EntryIdentity};
use crate::iceberg::spec::{DataFile, TableMetadata};

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
    /// Metadata of the same observation, including historical partition specs.
    pub(crate) metadata: &'a TableMetadata,
    pub(crate) spec_id: i32,
    pub(crate) file: &'a DataFile,
}

/// Inclusive predicate evaluation for conflict validation.
pub(crate) trait MayMatch: fmt::Display + Send + Sync {
    fn may_match(&self, file: &CandidateFile<'_>) -> Result<bool, ConnectorError>;
}

pub(crate) fn validation_conflict(message: impl Into<Arc<str>>) -> ConnectorMutationFailure {
    validation_conflict_for(CommitOperation::Write, message)
}

pub(super) fn validation_conflict_for(
    operation: CommitOperation,
    message: impl Into<Arc<str>>,
) -> ConnectorMutationFailure {
    validation_failure(operation, message)
}

pub(crate) fn commit_state_unknown(original: ConnectorMutationFailure) -> ConnectorMutationFailure {
    original_commit_failure(CommitOperation::Write, CommitFailureKind::Unknown, original)
}

#[cfg(test)]
mod tests;
