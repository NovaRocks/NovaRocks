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
use arrow::datatypes::{DataType, Field};
#[test]
fn actual_map_key_keeps_original_exact_binding_and_candidate_named_refusal() {
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;
    {
        let result = try_sql_source_with_declared_fields(
            "SELECT map{CAST(1 AS TINYINT):10,CAST(2 AS TINYINT):20}[k] AS picked FROM t",
            &[Field::new("k", DataType::Int64, false)],
        );

        let failure = result.unwrap_err();
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("actual FE named missing implementation refusal: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert_eq!(
            error.message(),
            "selected overload `builtin.scalar/map/dynamic-v1` has no installed implementation for the selected carrier"
        );
    }
}

mod fold_frontiers {
    use crate::fixtures::{
        analyze_table_free, optimize_table_free, session, table_free_request, table_parent,
    };
    use crate::fold_observation::FoldProbe;
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileIntent, builtin_sql_function_catalog},
        sql_mode::SqlSemanticSettings,
    };
    use std::sync::{Arc, atomic::Ordering};

    fn unavailable(error: SqlCompileError, expected: &str) {
        let SqlCompileError::Analyze(error) = error else {
            panic!("expected typed SQL analyze refusal: {error}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert_eq!(
            error.code(),
            AnalyzeErrorKind::UnavailableImplementation
                .descriptor()
                .code
        );
        assert!(
            error.span().is_some(),
            "the parser-owned function span remains"
        );
        assert!(error.message().contains(expected), "{}", error.message());
        assert!(error.control_error().is_none());
    }

    fn analyze(sql: &str, probe: &Arc<FoldProbe>) -> Result<(), SqlCompileError> {
        analyze_table_free(
            sql,
            SqlCompileIntent::Query,
            session(Default::default(), true),
            builtin_sql_function_catalog(),
            &probe.control(),
        )?
        .into_pending()?;
        Ok(())
    }

    #[test]
    fn e08s1_missing_aggregate_window_and_over_keep_unavailable_span_before_any_fold() {
        for (sql, name) in [
            ("SELECT dict_merge('x', 1)", "builtin.aggregate/dict_merge/"),
            (
                "SELECT session_number(1, 2) OVER (ORDER BY 1)",
                "builtin.window/session_number/",
            ),
        ] {
            let probe = Arc::new(FoldProbe::default());
            unavailable(analyze(sql, &probe).unwrap_err(), name);
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn e08s1_supported_over_and_invalid_regexp_data_are_not_evaluated_by_static_gate() {
        for sql in [
            "SELECT count(1) OVER ()",
            "SELECT max_by(1, 2) OVER ()",
            "SELECT min_by(1, 2) OVER ()",
            "SELECT row_number() OVER ()",
            "SELECT regexp_count('x', '[')",
        ] {
            let probe = Arc::new(FoldProbe::default());
            analyze(sql, &probe).unwrap();
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }

    const UNAUTHORED_ENVIRONMENT: &[(&str, &str)] = &[
        (
            "SELECT GROUP_CONCAT(REVERSE('abc'))",
            "builtin.aggregate/group_concat/",
        ),
        (
            "SELECT FROM_UNIXTIME(CAST(ABS(CAST(1 AS BIGINT)) AS BIGINT))",
            "builtin.scalar/from_unixtime/",
        ),
    ];

    #[test]
    fn e08s1_fold_environment_exact_missing_authors_refuse_parent_before_children() {
        for &(sql, name) in UNAUTHORED_ENVIRONMENT {
            let probe = Arc::new(FoldProbe::default());
            let error = optimize_table_free(
                sql,
                session(Default::default(), true),
                builtin_sql_function_catalog(),
                &probe.control(),
            )
            .unwrap_err();
            let SqlCompileError::Analyze(error) = error else {
                panic!("expected typed Unavailable: {error:?}");
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
            assert!(
                error.message().contains(name),
                "actual original overload: {error:?}"
            );
            assert!(
                probe.receipts.lock().unwrap().is_empty(),
                "parent admission precedes folding"
            );
        }
    }

    #[test]
    fn e08s1_logical_environment_exact_absent_source_is_named_with_original_span() {
        for &(sql, name) in UNAUTHORED_ENVIRONMENT {
            let probe = Arc::new(FoldProbe::default());
            let error = analyze_table_free(
                sql,
                SqlCompileIntent::LogicalOnly,
                session(Default::default(), true),
                builtin_sql_function_catalog(),
                &probe.control(),
            )
            .err()
            .unwrap();
            unavailable(error, name);
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn e08s1_logical_environment_exact_authored_raw_limit_keeps_original_logical_success() {
        for raw in [-1, 4096] {
            let probe = Arc::new(FoldProbe::default());
            let semantics = SqlSemanticSettings::default().with_group_concat_max_len(raw);
            analyze_table_free(
                "SELECT GROUP_CONCAT(REVERSE('abc'))",
                SqlCompileIntent::LogicalOnly,
                session(semantics, true),
                builtin_sql_function_catalog(),
                &probe.control(),
            )
            .unwrap()
            .into_complete()
            .unwrap();
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn e08s1_table_fold_parent_unavailable_precedes_real_argument_evaluation() {
        let probe = Arc::new(FoldProbe::default());
        let catalog = table_parent::source(table_parent::Outcome::Unavailable);
        let error = table_parent::optimize(&catalog, &probe).unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("typed original admission: {error:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        let sources = catalog.sources.lock().unwrap();
        assert_eq!(sources.len(), 1, "one actual failing parent, no tail loan");
        assert!(error.message().contains(sources[0].overload.as_str()));
        assert_eq!(catalog.calls.load(Ordering::Relaxed), 1);
        assert!(
            probe.receipts.lock().unwrap().is_empty(),
            "unavailable Table parent precedes its real child"
        );
    }

    const HOUR_LITERAL: &str = "SELECT hour_from_unixtime(CAST(1700000000 AS BIGINT)) AS h";
    const HOUR_OWNER: &str = "builtin.scalar/hour_from_unixtime/(i64)->i32;strict;legacy";

    #[test]
    fn sql_presence_candidate_hour_literal_and_projected_column_refuse_before_fold() {
        for sql in [
            HOUR_LITERAL,
            "SELECT hour_from_unixtime(t.k) AS h FROM (SELECT CAST(1700000000 AS BIGINT) AS k) t",
        ] {
            let probe = Arc::new(FoldProbe::default());
            unavailable(analyze(sql, &probe).unwrap_err(), HOUR_OWNER);
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn sql_presence_candidate_invalid_pattern_is_not_prepared_or_evaluated_by_admission() {
        let probe = Arc::new(FoldProbe::default());
        analyze("SELECT regexp_count('x', '[') AS n", &probe).unwrap();
        analyze(
            "SELECT regexp_count('x', t.pattern) AS n FROM (SELECT '[' AS pattern) t",
            &probe,
        )
        .unwrap();
        assert!(probe.receipts.lock().unwrap().is_empty());
    }

    #[test]
    fn sql_presence_candidate_completion_uses_actual_request_mode_before_fold_or_publication() {
        let probe = Arc::new(FoldProbe::default());
        let request = table_free_request(
            HOUR_LITERAL,
            session(Default::default(), true),
            &probe.control(),
        );
        unavailable(
            request
                .try_into_completion()
                .err()
                .expect("unavailable before completed source"),
            HOUR_OWNER,
        );
        assert!(probe.receipts.lock().unwrap().is_empty());
    }
}

mod percentile_hash {
    use crate::fixtures::{session, table_free_request};
    use crate::fold_observation::FoldProbe;
    use arrow::datatypes::DataType;
    use novarocks_functions::{
        FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionValueType,
    };
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileControl, SqlCompileError, builtin_sql_function_catalog},
    };
    use std::sync::Arc;

    #[test]
    fn hash_native_n1_exact_admission_preserves_original_variadic_binding_and_refuses_before_fold()
    {
        let original = builtin_sql_function_catalog().snapshot();
        let exact = original.snapshot_for_scalar_presence();
        let control = SqlCompileControl::unbounded();
        for n in [1, 2, 5] {
            let args = (0..n)
                .map(|_| FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Int32, true),
                    constant: None,
                })
                .collect::<Vec<_>>();
            let bound = original
                .resolve_scalar_binding("percentile_hash", &args, &control)
                .unwrap();
            assert_eq!(bound.admitted_execution_abi, None);
            assert_eq!(bound.logical_argument_count, n);
            assert_eq!(bound.selected.argument_types.len(), n);
            let request = FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: n,
                expected_result_type: None,
            };
            original
                .select_exact_overload_observed(
                    &bound.function_id,
                    bound.kind,
                    &bound.selected.overload,
                    request,
                    &control,
                )
                .unwrap();
            if n == 1 {
                assert_eq!(bound.admitted_execution_abi, None);
                let actual_abi = original
                    .pure_overload_declaration_observed(
                        &bound.function_id,
                        bound.kind,
                        &bound.selected.overload,
                        &control,
                    )
                    .unwrap()
                    .implementation()
                    .abi;
                let mut admitted = exact
                    .resolve_scalar_binding("percentile_hash", &args, &control)
                    .unwrap();
                assert_eq!(admitted.admitted_execution_abi, Some(actual_abi));
                admitted.admitted_execution_abi = None;
                assert_eq!(
                    admitted, bound,
                    "complete original selected source is unchanged"
                );
                exact
                    .select_exact_overload_observed(
                        &bound.function_id,
                        bound.kind,
                        &bound.selected.overload,
                        request,
                        &control,
                    )
                    .unwrap();
            } else {
                assert!(
                    matches!(exact.resolve_scalar_binding("percentile_hash", &args, &control),
                Err(FunctionBindingError::UnavailableImplementation(ref o)) if o == &bound.selected.overload)
                );
                assert!(
                    matches!(exact.select_exact_overload_observed(&bound.function_id, bound.kind,
                &bound.selected.overload, request, &control),
                Err(FunctionBindingError::UnavailableImplementation(ref o)) if o == &bound.selected.overload)
                );
            }
        }
        for sql in [
            "SELECT percentile_hash(CAST(17 AS INT),CAST('bad' AS INT)) AS encoded",
            "SELECT percentile_hash(CAST(17 AS INT),CAST(1 AS INT),CAST(2 AS INT),CAST(3 AS INT),CAST(4 AS INT))",
        ] {
            let probe = Arc::new(FoldProbe::default());
            let request =
                table_free_request(sql, session(Default::default(), true), &probe.control());
            let SqlCompileError::Analyze(error) = request
                .try_into_completion()
                .err()
                .expect("deferred production arity must fail before completing the source")
            else {
                panic!("expected original typed analysis category");
            };
            assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
            assert!(error.span().is_some());
            assert!(error.message().contains("builtin.scalar/percentile_hash/"));
            assert!(probe.receipts.lock().unwrap().is_empty());
        }
    }
}
