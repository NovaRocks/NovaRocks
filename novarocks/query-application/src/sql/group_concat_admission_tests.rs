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

use super::*;

#[test]
fn group_concat_admission_preserves_raw_limit_and_owned_snapshot() {
    for raw in [-1, 0, 3, 4, 1024, i64::MAX] {
        let mut connection = SessionSqlState::default();
        connection.execution_settings.set_group_concat_max_len(raw);
        let (.., frozen, _) = connection.clone().into_query_attempt_inputs();
        assert_eq!(frozen.group_concat_max_len(), Some(raw));
        connection.execution_settings.set_group_concat_max_len(4096);
        assert_eq!(frozen.group_concat_max_len(), Some(raw));
        assert_eq!(
            connection
                .into_query_attempt_inputs()
                .4
                .group_concat_max_len(),
            Some(4096)
        );
    }
}
#[test]
fn group_concat_actual_default_is_projected_once_without_owner_fallback() {
    let state = SessionSqlState::default();
    let raw = state.execution_settings.group_concat_max_len();
    assert_eq!(raw, 1024);
    assert_eq!(
        state.into_query_attempt_inputs().4.group_concat_max_len(),
        Some(raw)
    );
    assert_eq!(
        novarocks_sql::sql_mode::SqlSemanticSettings::default().group_concat_max_len(),
        None
    );
}
