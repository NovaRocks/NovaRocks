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

//! Core application mapping for provider-owned Iceberg commit outcomes.

pub use novarocks_connector_iceberg::commit::service::{
    CleanupAttempt, CommitFailureKind, CommitServiceError, CommitServiceOutcome, RecoveryEvidence,
    classify_commit_error,
};

use super::collector::IcebergCommitCollector;
use novarocks_connector_iceberg::commit::service::CommitRecoverySource;

impl CommitRecoverySource for IcebergCommitCollector {
    fn recovery_table_ident(&self) -> String {
        self.table_ident.to_string()
    }

    fn recovery_op_kind(&self) -> novarocks_connector_iceberg::commit::CommitOpKind {
        self.op_kind
    }

    fn recovery_base_snapshot_id(&self) -> Option<i64> {
        self.base_snapshot_id
    }

    fn recovery_base_sequence_number(&self) -> i64 {
        self.base_sequence_number
    }

    fn recovery_staging_dir(&self) -> String {
        self.staging_dir.clone()
    }

    fn recovery_manifest_cleanup_token(&self) -> Option<String> {
        None
    }
}

impl From<CommitServiceError> for crate::common::engine_error::EngineError {
    fn from(value: CommitServiceError) -> Self {
        let kind = value.failure_kind();
        let message = value.into_legacy_string();
        match kind {
            CommitFailureKind::KnownUncommitted => Self::commit_known_uncommitted(message),
            CommitFailureKind::Unknown => Self::commit_unknown(message),
            CommitFailureKind::FinalizeFailedKnownCommitted => {
                Self::commit_known_committed_finalize_failed(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_error_maps_to_core_application_error() {
        let error = CommitServiceError::invalid_input("invalid commit".to_string());
        let engine_error = crate::common::engine_error::EngineError::from(error);
        assert_eq!(
            engine_error.code(),
            crate::common::engine_error::EngineErrorCode::CommitKnownUncommitted
        );
    }
}
