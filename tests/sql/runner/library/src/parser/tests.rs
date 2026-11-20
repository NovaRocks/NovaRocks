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

use super::opt5_directive_tests::meta_re;
use super::{
    QueryMeta, TrinoErrorExpectation, merge_meta, parse_meta, validate_trino_error_expectation,
};

/// Observes the private merge and validation of Trino error metadata and statement counts.
#[test]
fn trino_error_expectation_checks_protocol_fields_and_optional_ordinal() {
    let re = meta_re();
    let meta = parse_meta(
        &["-- @expect_trino_error= MISSING_COLUMN_NAME , USER_ERROR ".into()],
        &re,
    )
    .unwrap();
    assert_eq!(
        meta.expect_trino_error,
        Some(TrinoErrorExpectation {
            name: "MISSING_COLUMN_NAME".into(),
            error_type: "USER_ERROR".into(),
            statement_ordinal: None,
        })
    );
    for value in [
        "",
        "MISSING_COLUMN_NAME",
        ",USER_ERROR",
        "MISSING_COLUMN_NAME,",
        "A,B,C",
        "A,B,0",
        "A,B,2,3",
    ] {
        assert!(
            parse_meta(&[format!("-- @expect_trino_error={value}")], &re).is_err(),
            "{value}"
        );
    }
    assert!(parse_meta(&["-- @expect_trino_error".into()], &re).is_err());
    let legacy = QueryMeta {
        expect_error: Some("marker".into()),
        ..Default::default()
    };
    validate_trino_error_expectation(&merge_meta(&legacy, &meta), Some("SELECT x")).unwrap();
    let competing_code = QueryMeta {
        expect_error_code: Some("OTHER".into()),
        ..Default::default()
    };
    assert!(
        validate_trino_error_expectation(&merge_meta(&competing_code, &meta), Some("SELECT x"))
            .is_err()
    );
    let valid = [
        "SELECT ';'",
        "-- leading comment\nSELECT absent; -- trailing",
        "INVALID SQL",
    ];
    for sql in valid {
        validate_trino_error_expectation(&meta, Some(sql)).unwrap();
    }
    for sql in [
        "",
        "-- comment only",
        "SELECT 1; SELECT 2",
        "shell: echo marker",
    ] {
        assert!(
            validate_trino_error_expectation(&meta, Some(sql)).is_err(),
            "{sql}"
        );
    }
    assert!(meta.has_error_expectation());

    let indexed = parse_meta(
        &["-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR,2".into()],
        &re,
    )
    .unwrap();
    validate_trino_error_expectation(&indexed, Some("USE fixture; SELECT idSELECT id")).unwrap();
    for sql in ["SELECT 1", "USE fixture; SELECT idSELECT id; SELECT 3"] {
        assert!(validate_trino_error_expectation(&indexed, Some(sql)).is_err());
    }
    assert!(validate_trino_error_expectation(&merge_meta(&legacy, &indexed), None).is_err());
    assert!(
        validate_trino_error_expectation(&merge_meta(&competing_code, &indexed), None).is_err()
    );
}
