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

use serde::{Deserialize, Serialize};

use crate::meta::repository::{RepositoryError, RepositoryResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationKind {
    InsertAppend,
    InsertOverwrite,
    RowDelta,
    MvRefresh,
    Maintenance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergOperationState {
    Preparing,
    Writing,
    Collecting,
    Committing,
    Committed,
    CommitUnknown,
    Finalizing,
    Finalized,
    Aborting,
    Aborted,
    FailedKnownUncommitted,
    FinalizeFailedKnownCommitted,
}

impl IcebergOperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "PREPARING",
            Self::Writing => "WRITING",
            Self::Collecting => "COLLECTING",
            Self::Committing => "COMMITTING",
            Self::Committed => "COMMITTED",
            Self::CommitUnknown => "COMMIT_UNKNOWN",
            Self::Finalizing => "FINALIZING",
            Self::Finalized => "FINALIZED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::FailedKnownUncommitted => "FAILED_KNOWN_UNCOMMITTED",
            Self::FinalizeFailedKnownCommitted => "FINALIZE_FAILED_KNOWN_COMMITTED",
        }
    }

    pub fn is_finished(self) -> bool {
        matches!(
            self,
            Self::Finalized | Self::Aborted | Self::FailedKnownUncommitted
        )
    }
}

pub fn validate_operation_transition(
    from: IcebergOperationState,
    to: IcebergOperationState,
) -> RepositoryResult<()> {
    if from == to {
        return Ok(());
    }
    let allowed = matches!(
        (from, to),
        (
            IcebergOperationState::Preparing,
            IcebergOperationState::Writing
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Preparing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Collecting
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Writing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::Committing
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::Aborting
        ) | (
            IcebergOperationState::Collecting,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::Committed
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::CommitUnknown
        ) | (
            IcebergOperationState::Committing,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::Committed
        ) | (
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::FailedKnownUncommitted
        ) | (
            IcebergOperationState::Committed,
            IcebergOperationState::Finalizing
        ) | (
            IcebergOperationState::Committed,
            IcebergOperationState::Finalized
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::Finalized
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::FinalizeFailedKnownCommitted
        ) | (
            IcebergOperationState::Finalizing,
            IcebergOperationState::CommitUnknown
        ) | (
            IcebergOperationState::FinalizeFailedKnownCommitted,
            IcebergOperationState::Finalizing
        ) | (
            IcebergOperationState::Aborting,
            IcebergOperationState::Aborted
        ) | (
            IcebergOperationState::Aborting,
            IcebergOperationState::FailedKnownUncommitted
        )
    );
    if allowed {
        Ok(())
    } else {
        Err(RepositoryError::conflict(format!(
            "invalid Iceberg operation state transition from {} to {}",
            from.as_str(),
            to.as_str()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::meta::repository::RepositoryErrorKind;

    #[test]
    fn operation_state_as_str_is_stable_for_diagnostics() {
        assert_eq!(IcebergOperationState::Preparing.as_str(), "PREPARING");
        assert_eq!(
            IcebergOperationState::CommitUnknown.as_str(),
            "COMMIT_UNKNOWN"
        );
        assert_eq!(
            IcebergOperationState::FinalizeFailedKnownCommitted.as_str(),
            "FINALIZE_FAILED_KNOWN_COMMITTED"
        );
    }

    #[test]
    fn transition_helper_allows_main_commit_path_and_idempotent_replay() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::Preparing,
                IcebergOperationState::Writing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Writing,
                IcebergOperationState::Collecting
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Collecting,
                IcebergOperationState::Committing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Committing,
                IcebergOperationState::Committed
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Committed,
                IcebergOperationState::Finalizing
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::Finalizing,
                IcebergOperationState::Finalized
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::CommitUnknown
            )
            .is_ok()
        );
    }

    #[test]
    fn transition_helper_rejects_commit_unknown_to_aborted() {
        let err = validate_operation_transition(
            IcebergOperationState::CommitUnknown,
            IcebergOperationState::Aborted,
        )
        .expect_err("commit unknown must not be treated as aborted");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(err.to_string().contains("COMMIT_UNKNOWN"));
        assert!(err.to_string().contains("ABORTED"));
    }

    #[test]
    fn transition_helper_allows_commit_unknown_recovery_outcomes() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::Committed
            )
            .is_ok()
        );
        assert!(
            validate_operation_transition(
                IcebergOperationState::CommitUnknown,
                IcebergOperationState::FailedKnownUncommitted
            )
            .is_ok()
        );
    }

    #[test]
    fn transition_helper_routes_finalize_failure_to_known_committed_failure() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::Finalizing,
                IcebergOperationState::FinalizeFailedKnownCommitted
            )
            .is_ok()
        );
        assert!(!IcebergOperationState::FinalizeFailedKnownCommitted.is_finished());
        assert!(IcebergOperationState::Finalized.is_finished());
        assert!(IcebergOperationState::Aborted.is_finished());
        assert!(IcebergOperationState::FailedKnownUncommitted.is_finished());
    }

    #[test]
    fn transition_helper_retries_known_committed_finalize_failure_through_finalizing() {
        assert!(
            validate_operation_transition(
                IcebergOperationState::FinalizeFailedKnownCommitted,
                IcebergOperationState::Finalizing
            )
            .is_ok()
        );
        let err = validate_operation_transition(
            IcebergOperationState::FinalizeFailedKnownCommitted,
            IcebergOperationState::Finalized,
        )
        .expect_err("finalize retry must pass through FINALIZING");
        assert_eq!(err.kind(), RepositoryErrorKind::Conflict);
        assert!(err.to_string().contains("FINALIZE_FAILED_KNOWN_COMMITTED"));
        assert!(err.to_string().contains("FINALIZED"));
    }
}
