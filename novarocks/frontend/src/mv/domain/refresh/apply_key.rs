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

use novarocks_sql::planning::mv::{
    MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
    MV_JOIN_APPLY_KEY_COLUMN_NAME,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKeyValueType {
    Int64,
    Utf8,
    BranchInt64,
    BranchUtf8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApplyKeyContract {
    pub(crate) column_name: &'static str,
    pub(crate) value_type: ApplyKeyValueType,
    pub(crate) rewrite_evidence: RewriteEvidence,
    pub(crate) allow_full_rebuild_on_policy_full_refresh: bool,
    pub(crate) preload_locator_for_change_stream_deletes: bool,
}

impl ApplyKeyContract {
    pub(crate) fn projection_filter() -> Self {
        Self {
            column_name: MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::Int64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: true,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn union_projection_filter() -> Self {
        Self {
            column_name: MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::BranchInt64,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn join_projection_filter() -> Self {
        Self {
            column_name: MV_JOIN_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::None,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: false,
        }
    }

    pub(crate) fn aggregate_group_row() -> Self {
        Self {
            column_name: MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::Aggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }

    pub(crate) fn join_aggregate_group_row() -> Self {
        Self {
            column_name: MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::Utf8,
            rewrite_evidence: RewriteEvidence::JoinAggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }

    pub(crate) fn branch_union_aggregate_group_row() -> Self {
        Self {
            column_name: MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
            value_type: ApplyKeyValueType::BranchUtf8,
            rewrite_evidence: RewriteEvidence::Aggregate,
            allow_full_rebuild_on_policy_full_refresh: false,
            preload_locator_for_change_stream_deletes: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_matrix_is_stable() {
        let cases = [
            (
                ApplyKeyContract::projection_filter(),
                MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::Int64,
                RewriteEvidence::None,
                true,
                false,
            ),
            (
                ApplyKeyContract::union_projection_filter(),
                MV_HIDDEN_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::BranchInt64,
                RewriteEvidence::None,
                false,
                false,
            ),
            (
                ApplyKeyContract::join_projection_filter(),
                MV_JOIN_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::Utf8,
                RewriteEvidence::None,
                false,
                false,
            ),
            (
                ApplyKeyContract::aggregate_group_row(),
                MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::Utf8,
                RewriteEvidence::Aggregate,
                false,
                true,
            ),
            (
                ApplyKeyContract::join_aggregate_group_row(),
                MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::Utf8,
                RewriteEvidence::JoinAggregate,
                false,
                true,
            ),
            (
                ApplyKeyContract::branch_union_aggregate_group_row(),
                MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeyValueType::BranchUtf8,
                RewriteEvidence::Aggregate,
                false,
                true,
            ),
        ];

        for (actual, column_name, value_type, rewrite_evidence, rebuild, preload) in cases {
            assert_eq!(actual.column_name, column_name);
            assert_eq!(actual.value_type, value_type);
            assert_eq!(actual.rewrite_evidence, rewrite_evidence);
            assert_eq!(actual.allow_full_rebuild_on_policy_full_refresh, rebuild);
            assert_eq!(actual.preload_locator_for_change_stream_deletes, preload);
        }
    }
}
