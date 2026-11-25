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
fn approximate_aggregate_availability_uses_the_exact_selected_profile() {
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;

    let fields = [Field::new("k", DataType::Int64, false)];
    for (sql, overload) in [
        (
            "SELECT approx_distinct(k, 0.01) FROM t",
            "explicit-error-hll-v1",
        ),
        (
            "SELECT approx_percentile(k, 1, 0.5, 0.01) FROM t",
            "bigint-accuracy-v1",
        ),
        ("SELECT approx_distinct(ARRAY[k]) FROM t", "default-hll-v1"),
    ] {
        let failure = try_sql_source_with_declared_fields(sql, &fields).unwrap_err();
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("expected typed aggregate profile refusal: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert!(error.message().contains(overload), "{error}");
    }
}

#[test]
fn actual_map_key_keeps_original_exact_binding_and_names_uninstalled_key_domain() {
    use novarocks_sql::{
        analyze_error::AnalyzeErrorKind,
        compiler::{SqlCompileError, SqlCompileProgressError},
    };
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;

    let fields = [Field::new("k", DataType::Int64, false)];
    let source = try_sql_source_with_declared_fields(
        "SELECT map(ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT)], ARRAY[10,20])[k] AS picked FROM t",
        &fields,
    )
    .unwrap();
    let output = source.original_public_result_declaration().unwrap();
    assert_eq!(output.fields()[0].ty.data_type(), &DataType::Int32);

    let array_keys = try_sql_source_with_declared_fields(
        "SELECT map(ARRAY[ARRAY[k]], ARRAY[10]) AS candidate FROM t",
        &fields,
    )
    .unwrap();
    let schema = array_keys.original_public_result_declaration().unwrap();
    let key = schema.fields()[0]
        .ty
        .child_at(0)
        .expect("MAP key declaration");
    assert_eq!(key.child_at(0).unwrap().data_type(), &DataType::Int64);

    let failure = try_sql_source_with_declared_fields(
        "SELECT map(ARRAY[ROW(k, JSON '{}')], ARRAY[10]) AS candidate FROM t",
        &fields,
    )
    .unwrap_err();
    let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
        panic!("expected an exact MAP key domain refusal: {failure:?}");
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
    assert!(
        error
            .message()
            .contains("builtin.scalar/map/array-keys-array-values-v1")
    );
}

#[test]
fn concat_array_widening_preserves_the_original_common_conversion_in_the_package() {
    use novarocks_expression_contract::ExprKind;
    use novarocks_functions::builtin::value_conversion::ARRAY_COMMON_COERCION;
    use novarocks_sql_testkit::try_sql_source_with_declared_fields;

    let fields = [
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int64, true),
    ];
    let authored = try_sql_source_with_declared_fields(
        "SELECT concat(ARRAY[k, NULL], ARRAY[v]) AS items FROM t",
        &fields,
    )
    .unwrap();
    let schema = authored.original_public_result_declaration().unwrap();
    let result = &schema.fields()[0].ty;
    let item = result.child_at(0).expect("concat ARRAY result");
    assert_eq!(item.data_type(), &DataType::Int64);
    assert!(item.nullable());

    let mut found = false;
    for fragment in authored.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let Some(unit) = &node.expressions else {
                continue;
            };
            for expression in unit.definitions() {
                let ExprKind::FunctionCall { function, args } = &expression.kind else {
                    continue;
                };
                if function.overload.as_str() != ARRAY_COMMON_COERCION {
                    continue;
                }
                let source = unit.definition(args[0]).expect("original ARRAY operand");
                let Some(source_item) = source.ty.child_at(0) else {
                    continue;
                };
                if source_item.data_type() != &DataType::Int32 {
                    continue;
                }
                let converted_item = expression.ty.child_at(0).expect("converted ARRAY item");
                assert_eq!(converted_item.data_type(), &DataType::Int64);
                assert_eq!(expression.ty.nullable(), source.ty.nullable());
                found = true;
            }
        }
    }
    assert!(
        found,
        "the source ARRAY(INTEGER) uses the installed exact common coercion"
    );
}

mod fold_frontiers {
    use crate::fixtures::{analyze_table_free, optimize_table_free, session, table_parent};
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
    fn collection_lambda_signatures_keep_their_own_unavailable_overloads() {
        let probe = Arc::new(FoldProbe::default());
        for (sql, overload) in [
            (
                "SELECT array_sort(ARRAY[3,1,2], (x,y) -> 0)",
                "builtin.scalar/array_sort/lambda-v1",
            ),
            (
                "SELECT transform(ARRAY[1,2], x -> x + 1)",
                "builtin.scalar/transform/lambda-v1",
            ),
            (
                "SELECT map_filter(map(), (k,v) -> true)",
                "builtin.scalar/map_filter/lambda-v1",
            ),
        ] {
            unavailable(analyze(sql, &probe).unwrap_err(), overload);
        }
    }
    #[test]
    fn reverse_keeps_installed_string_and_array_overloads_distinct() {
        let probe = Arc::new(FoldProbe::default());
        analyze("SELECT reverse('abc'), reverse(ARRAY[1,2])", &probe).unwrap();
    }

    #[test]
    fn distinct_aggregates_are_selected_only_from_supported_sql_calls() {
        let probe = Arc::new(FoldProbe::default());
        analyze(
            "SELECT count(DISTINCT 1, 2), sum(DISTINCT 1), avg(DISTINCT 1)",
            &probe,
        )
        .unwrap();

        for (sql, expected) in [
            (
                "SELECT multi_distinct_count(1)",
                AnalyzeErrorKind::UnknownFunction,
            ),
            (
                "SELECT multi_distinct_sum(1) OVER ()",
                AnalyzeErrorKind::UnknownFunction,
            ),
            (
                "SELECT multi_distinct_avg(1)",
                AnalyzeErrorKind::UnknownFunction,
            ),
            (
                "SELECT count(DISTINCT 1) OVER ()",
                AnalyzeErrorKind::UnsupportedExpression,
            ),
        ] {
            let error = analyze(sql, &probe).unwrap_err();
            let SqlCompileError::Analyze(error) = error else {
                panic!("expected analysis error for {sql}: {error:?}");
            };
            assert_eq!(error.kind(), expected, "{sql}: {error}");
        }
    }

    #[test]
    fn retired_trc_6d_names_are_unknown_to_sql_analysis() {
        use novarocks_functions::{
            FunctionKind, FunctionVisibility, builtin::catalogue::builtin_engine_function_catalog,
        };

        let catalog = builtin_engine_function_catalog();
        let probe = Arc::new(FoldProbe::default());
        for name in [
            "__iceberg_transform_bucket",
            "__iceberg_transform_day",
            "__iceberg_transform_hour",
            "__iceberg_transform_identity",
            "__iceberg_transform_month",
            "__iceberg_transform_truncate",
            "__iceberg_transform_void",
            "__iceberg_transform_year",
            "aes_decrypt",
            "aes_encrypt",
            "append_trailing_char_if_absent",
            "approx_cosine_similarity",
            "approx_count_distinct_hll_sketch",
            "approx_l2_distance",
            "array_avg",
            "array_contains_all",
            "array_cum_sum",
            "array_difference",
            "array_sortby",
            "array_sum",
            "array_top_n",
            "array_unique_agg",
            "bit_length",
            "bitmap_agg",
            "bitmap_and",
            "bitmap_andnot",
            "bitmap_contains",
            "bitmap_count",
            "bitmap_empty",
            "bitmap_from_binary",
            "bitmap_from_string",
            "bitmap_has_any",
            "bitmap_intersect",
            "bitmap_max",
            "bitmap_min",
            "bitmap_or",
            "bitmap_subset_in_range",
            "bitmap_subset_limit",
            "bitmap_to_array",
            "bitmap_to_base64",
            "bitmap_to_binary",
            "bitmap_to_string",
            "bitmap_union",
            "bitmap_union_count",
            "bitmap_union_int",
            "bitmap_xor",
            "cosine_similarity_norm",
            "cot",
            "date_slice",
            "dict_merge",
            "distinct_map_keys",
            "ds_hll_accumulate",
            "ds_hll_combine",
            "ds_hll_count_distinct",
            "ds_hll_count_distinct_merge",
            "ds_hll_count_distinct_state",
            "ds_hll_count_distinct_union",
            "ds_hll_estimate",
            "ds_theta_count_distinct",
            "elt",
            "encode_fingerprint_sha256",
            "encode_row_id",
            "encode_sort_key",
            "field",
            "find_in_set",
            "from_days",
            "grouping_id",
            "hll_cardinality",
            "hll_hash",
            "hll_raw_agg",
            "hll_union",
            "hll_union_agg",
            "hour_from_unixtime",
            "makedate",
            "mann_whitney_u_test",
            "map_apply",
            "md5sum",
            "md5sum_numeric",
            "microseconds_add",
            "microseconds_sub",
            "money_format",
            "multi_distinct_avg",
            "multi_distinct_count",
            "multi_distinct_sum",
            "next_day",
            "null_or_empty",
            "octet_length",
            "ord",
            "percentile_approx_raw",
            "percentile_cont",
            "percentile_disc",
            "percentile_disc_lc",
            "percentile_empty",
            "percentile_hash",
            "percentile_union",
            "pmod",
            "previous_day",
            "sec_to_time",
            "session_number",
            "sm3",
            "space",
            "square",
            "strcmp",
            "sub_bitmap",
            "substring_index",
            "sum_map",
            "time_format",
            "time_slice",
            "time_to_sec",
            "to_bitmap",
            "to_date",
            "to_datetime_ntz",
            "to_days",
            "utc_time",
            "utc_timestamp",
            "xx_hash3_128",
            "xx_hash3_64",
            "yearweek",
        ] {
            for kind in [
                FunctionKind::Scalar,
                FunctionKind::Aggregate,
                FunctionKind::Window,
                FunctionKind::Table,
            ] {
                let expected = if kind == FunctionKind::Aggregate
                    && matches!(
                        name,
                        "multi_distinct_count" | "multi_distinct_sum" | "multi_distinct_avg"
                    ) {
                    Some(FunctionVisibility::Hidden)
                } else {
                    None
                };
                assert_eq!(
                    catalog
                        .definition(name, kind)
                        .map(|entry| entry.visibility()),
                    expected,
                    "catalogue entry for {kind:?} {name}"
                );
            }
            let sql = format!("SELECT {name}(1)");
            let error = analyze(&sql, &probe).unwrap_err();
            let SqlCompileError::Analyze(error) = error else {
                panic!("expected analysis error for {sql}: {error:?}");
            };
            assert_eq!(
                error.kind(),
                AnalyzeErrorKind::UnknownFunction,
                "{sql}: {error}"
            );
        }

        analyze("SELECT sleep(0)", &probe).unwrap();
        let error = analyze("SELECT murmur_hash3_32('x')", &probe).unwrap_err();
        let SqlCompileError::Analyze(error) = error else {
            panic!("retired Murmur call must fail analysis: {error:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::UnknownFunction);
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

    const UNAUTHORED_ENVIRONMENT: &[(&str, &str)] = &[(
        "SELECT FROM_UNIXTIME(-(BIGINT '-1'))",
        "builtin.scalar/from_unixtime/",
    )];

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
        {
            let probe = Arc::new(FoldProbe::default());
            let semantics = SqlSemanticSettings::default();
            analyze_table_free(
                "SELECT LISTAGG(REVERSE('abc')) WITHIN GROUP (ORDER BY 1)",
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
}
