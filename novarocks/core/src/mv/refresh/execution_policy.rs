// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Domain policy for choosing an incremental MV refresh execution mode.

use crate::mv::application::{MvIncrementalJoinMode, MvIncrementalWriteMode};

pub(crate) fn non_join_incremental_write_mode(
    is_aggregate: bool,
    has_delete_changes: bool,
) -> MvIncrementalWriteMode {
    if is_aggregate || has_delete_changes {
        MvIncrementalWriteMode::RowDelta
    } else {
        MvIncrementalWriteMode::FastAppend
    }
}

pub(crate) fn select_join_incremental_execution_mode(
    left_has_delete_changes: bool,
    right_has_delete_changes: bool,
) -> MvIncrementalJoinMode {
    if left_has_delete_changes || right_has_delete_changes {
        MvIncrementalJoinMode::Coalesce
    } else {
        MvIncrementalJoinMode::AppendOnly
    }
}

#[cfg(test)]
pub(crate) fn should_use_join_delta_append_only_fast_path(
    query: &sqlparser::ast::Query,
    left_has_delete_changes: bool,
    right_has_delete_changes: bool,
) -> bool {
    !left_has_delete_changes
        && !right_has_delete_changes
        && crate::mv::iceberg_join_branch::is_append_only_join_delta_eligible(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_or_deletes_require_row_delta() {
        assert_eq!(
            non_join_incremental_write_mode(true, false),
            MvIncrementalWriteMode::RowDelta
        );
        assert_eq!(
            non_join_incremental_write_mode(false, true),
            MvIncrementalWriteMode::RowDelta
        );
        assert_eq!(
            non_join_incremental_write_mode(false, false),
            MvIncrementalWriteMode::FastAppend
        );
    }

    #[test]
    fn deletes_select_join_coalesce_mode() {
        assert_eq!(
            select_join_incremental_execution_mode(false, false),
            MvIncrementalJoinMode::AppendOnly
        );
        assert_eq!(
            select_join_incremental_execution_mode(true, false),
            MvIncrementalJoinMode::Coalesce
        );
        assert_eq!(
            select_join_incremental_execution_mode(false, true),
            MvIncrementalJoinMode::Coalesce
        );
    }
}
