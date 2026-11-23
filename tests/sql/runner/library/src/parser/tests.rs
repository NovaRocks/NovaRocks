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

#[test]
fn view_fixture_directives_parse_bound_targets_and_statements_with_multiple_fixtures() {
    use super::{
        IcebergViewFixtureAssertion, IcebergViewFixtureDirective, IcebergViewFixtureEndpoint,
        IcebergViewFixtureSample, IcebergViewFixtureTarget,
    };

    let re = meta_re();
    let target = IcebergViewFixtureTarget {
        catalog: Some("lake".into()),
        namespace: "case_ns".into(),
        view: "v".into(),
    };
    let meta = parse_meta(
        &[
            "-- @iceberg_view_fixture=rest-mv:lake.case_ns.v:spark-mv:storage".into(),
            "-- @iceberg_view_fixture_unchanged=lake.case_ns.v".into(),
        ],
        &re,
    )
    .unwrap();
    assert_eq!(
        meta.iceberg_view_fixture,
        Some(IcebergViewFixtureDirective {
            endpoint: IcebergViewFixtureEndpoint::RestMv,
            target: target.clone(),
            sample: IcebergViewFixtureSample::SparkMv,
            storage_table: Some("storage".into()),
        })
    );
    assert_eq!(
        meta.iceberg_view_fixture_unchanged,
        Some(IcebergViewFixtureAssertion::Named(target))
    );
    let meta = parse_meta(
        &[
            "-- @iceberg_view_fixture=rest:case_ns.v:trino-mv".into(),
            "-- @iceberg_view_fixture_unchanged".into(),
        ],
        &re,
    )
    .unwrap();
    let fixture = meta.iceberg_view_fixture.unwrap();
    assert_eq!(fixture.endpoint, IcebergViewFixtureEndpoint::Rest);
    assert!(fixture.target.catalog.is_none());
    assert!(fixture.storage_table.is_none());
    assert_eq!(
        meta.iceberg_view_fixture_unchanged,
        Some(IcebergViewFixtureAssertion::InstalledOnStepOrOnlyFixture)
    );
}

#[test]
fn view_fixture_rejects_unrecognized_parameters_and_duplicate_directives() {
    let re = meta_re();
    for invalid in [
        "-- @iceberg_view_fixture",
        "-- @iceberg_view_fixture=",
        "-- @iceberg_view_fixture=other:n.v:trino-mv",
        "-- @iceberg_view_fixture=rest:other.namespace/schema.v:trino-mv",
        "-- @iceberg_view_fixture=rest:n.v:../trino-mv",
        "-- @iceberg_view_fixture=rest:n.v:trino-mv:s.extra",
        "-- @iceberg_view_fixture=rest:n.v:trino-mv:",
        "-- @iceberg_view_fixture=rest:n.v:trino-mv:s:extra",
        "-- @iceberg_view_fixture_unchanged=",
        "-- @iceberg_view_fixture_unchanged=lake.schema.*",
    ] {
        assert!(parse_meta(&[invalid.into()], &re).is_err(), "{invalid}");
    }
    let assertion = "-- @iceberg_view_fixture_unchanged".to_string();
    assert!(parse_meta(&[assertion.clone(), assertion], &re).is_err());
}

#[test]
fn view_fixture_definition_preserves_placeholder_names_without_authorizing_them() {
    use super::{MetadataNameBinding, parse_meta_with_name_binding};

    let line =
        "-- @iceberg_view_fixture=rest-mv:lake_${suite_uuid0}.ns_${uuid0}.v:trino-mv:${table.name}";
    let re = meta_re();
    assert!(parse_meta(&[line.into()], &re).is_err());
    let meta = parse_meta_with_name_binding(
        &[line.into()],
        &re,
        &[],
        MetadataNameBinding::LiteralDefinition,
    )
    .unwrap();
    let fixture = meta.iceberg_view_fixture.unwrap();
    assert_eq!(
        fixture.target.catalog.as_deref(),
        Some("lake_${suite_uuid0}")
    );
    assert_eq!(fixture.target.namespace, "ns_${uuid0}");
    assert_eq!(fixture.storage_table.as_deref(), Some("${table.name}"));
}

#[test]
fn update_count_expectation_parses_and_validates_one_statement() {
    use super::{UpdateCountExpectation, validate_update_count_expectation};

    let re = meta_re();
    for (text, expected) in [
        ("42", UpdateCountExpectation::Counted(42)),
        ("0", UpdateCountExpectation::Counted(0)),
        ("NULL", UpdateCountExpectation::NoCount),
    ] {
        let meta = parse_meta(&[format!("-- @expect_update_count={text}")], &re).unwrap();
        assert_eq!(meta.expect_update_count, Some(expected));
        assert_eq!(
            merge_meta(&meta, &QueryMeta::default()).expect_update_count,
            Some(expected)
        );
        validate_update_count_expectation(
            &meta,
            Some("DELETE FROM t WHERE k = ';'; -- trailing comment"),
        )
        .unwrap();
        for sql in [
            "",
            "-- comment only",
            "shell: exit 0",
            "DELETE FROM t; DELETE FROM t",
        ] {
            assert!(
                validate_update_count_expectation(&meta, Some(sql)).is_err(),
                "{sql}"
            );
        }
    }
    for text in ["", "-1", "+2", "1.0", "18446744073709551616", "none"] {
        assert!(
            parse_meta(&[format!("-- @expect_update_count={text}")], &re).is_err(),
            "{text}"
        );
    }
    assert!(parse_meta(&["-- @expect_update_count".into()], &re).is_err());
    assert!(
        parse_meta(
            &[
                "-- @expect_update_count=0".into(),
                "-- @expect_error=denied".into()
            ],
            &re
        )
        .is_err()
    );
    assert!(
        parse_meta(
            &[
                "-- @expect_update_count=0".into(),
                "-- @expect_update_count=NULL".into()
            ],
            &re
        )
        .is_err()
    );
}

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
