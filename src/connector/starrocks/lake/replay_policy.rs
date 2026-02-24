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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissingTxnLogPolicy {
    ReturnPublished,
    SkipTxn,
    AdvanceToNextBaseVersion,
    ErrorSinglePublishMissingLog,
    ErrorBatchMissingLogAndMetadata { expected_meta_version: i64 },
    ErrorMissingLogAtTxnIndex,
}

pub(crate) fn decide_missing_txn_log_policy(
    txn_count: usize,
    txn_idx: usize,
    force_publish: bool,
    current_base_version: i64,
    has_new_version_metadata: bool,
    has_next_base_metadata: bool,
) -> MissingTxnLogPolicy {
    if txn_idx == 0 && txn_count == 1 {
        if has_new_version_metadata {
            return MissingTxnLogPolicy::ReturnPublished;
        }
        if force_publish || current_base_version == 1 {
            return MissingTxnLogPolicy::SkipTxn;
        }
        return MissingTxnLogPolicy::ErrorSinglePublishMissingLog;
    }

    if txn_idx == 0 && txn_count > 1 {
        if has_next_base_metadata {
            return MissingTxnLogPolicy::AdvanceToNextBaseVersion;
        }
        if force_publish {
            return MissingTxnLogPolicy::SkipTxn;
        }
        return MissingTxnLogPolicy::ErrorBatchMissingLogAndMetadata {
            expected_meta_version: current_base_version.saturating_add(1),
        };
    }

    if force_publish {
        return MissingTxnLogPolicy::SkipTxn;
    }
    if has_new_version_metadata {
        return MissingTxnLogPolicy::ReturnPublished;
    }
    MissingTxnLogPolicy::ErrorMissingLogAtTxnIndex
}

#[cfg(test)]
mod tests {
    use super::{MissingTxnLogPolicy, decide_missing_txn_log_policy};

    #[test]
    fn single_publish_returns_published_when_target_meta_exists() {
        let got = decide_missing_txn_log_policy(1, 0, false, 1, true, false);
        assert_eq!(got, MissingTxnLogPolicy::ReturnPublished);
    }

    #[test]
    fn first_batch_txn_advances_base_when_next_meta_exists() {
        let got = decide_missing_txn_log_policy(2, 0, false, 1, false, true);
        assert_eq!(got, MissingTxnLogPolicy::AdvanceToNextBaseVersion);
    }

    #[test]
    fn missing_generic_txn_without_force_returns_error() {
        let got = decide_missing_txn_log_policy(3, 1, false, 1, false, false);
        assert_eq!(got, MissingTxnLogPolicy::ErrorMissingLogAtTxnIndex);
    }
}
