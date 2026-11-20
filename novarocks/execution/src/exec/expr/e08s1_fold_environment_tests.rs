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
//! Permanent same original fold-parent frontier, independent of Task submission.
use super::fold_environment_sql_test_source::optimize;
use super::sql_fold_observation_test_source::{SERIAL, fold_count, reset};
use novarocks_sql::{analyze_error::AnalyzeErrorKind, compiler::SqlCompileError};
#[test]
fn e08s1_fold_environment_exact_missing_authors_refuse_parent_before_children() {
    let _serial = SERIAL.lock().unwrap();
    for (sql, name) in [
        (
            "SELECT GROUP_CONCAT(REVERSE('abc'))",
            "builtin.aggregate/string_agg/",
        ),
        (
            "SELECT FROM_UNIXTIME(CAST(ABS(CAST(1 AS BIGINT)) AS BIGINT))",
            "builtin.scalar/from_unixtime/",
        ),
    ] {
        reset();
        let error = optimize(sql, None).unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("expected typed Unavailable: {error:?}")
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert!(
            error.to_string().contains(name),
            "actual original overload: {error:?}"
        );
        assert_eq!(
            fold_count(),
            0,
            "no child data evaluation before unavailable parent"
        );
    }
}
#[test]
fn e08s1_fold_environment_exact_original_concat_raw_limits_keep_child_fold() {
    let _serial = SERIAL.lock().unwrap();
    for raw in [-1, 4096] {
        reset();
        optimize("SELECT GROUP_CONCAT(REVERSE('abc'))", Some(raw)).unwrap();
        assert!(
            fold_count() > 0,
            "the original raw limit has a genuine author"
        );
    }
}
