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

//! Record conversion retains the original oracle and its placeholder author.
// Design: ADR-0210.
use crate::golden::execution::{blocks_from_results, compare_blocks};
use crate::golden::legacy::{OldComparePolicy, OldRenderContext};
use crate::golden::migration::convert_legacy_section_pair;
use crate::types::{GoldenSection, QueryExecution, ResultSet, SqlStep};
use anyhow::{Context, Result, ensure};
use novarocks_test_trino_client::{StatementResult, TrinoValue};

/// Convert only against the paired original source and validated expansion.
/// A matching typed/shell expectation remains the original expectation, even
/// when the comparison policy would also accept a different actual spelling.
pub(crate) fn record_result(
    step: &SqlStep,
    execution: &QueryExecution,
    expected: &ResultSet,
    context: &OldRenderContext,
    policy: &OldComparePolicy,
) -> Result<ResultSet> {
    let golden = expected
        .golden
        .as_ref()
        .context("legacy conversion requires the paired source and expanded expectation")?;
    ensure!(
        golden.source.step == step.query_number && golden.expanded.step == step.query_number,
        "conversion expectation does not belong to the selected step"
    );
    // This checks the complete original/expansion pair, including source
    // placeholders whose canonical quoting depends on their actual expansion.
    crate::golden::GoldenFile {
        sections: vec![golden.source.clone()],
    }
    .render_template(&crate::golden::GoldenFile {
        sections: vec![golden.expanded.clone()],
    })?;
    let converted = if crate::shell::is_shell_step(&step.sql) {
        let actual = crate::results::execution_golden(step, execution)?;
        let (equal, detail) = compare_blocks(
            &golden.expanded.blocks,
            &actual.expanded.blocks,
            policy.order_sensitive,
            policy.float_epsilon,
        );
        ensure!(equal, "existing shell expectation mismatch: {detail}");
        golden.clone()
    } else {
        let actual = normalized_results(step, &execution.typed_results)?;
        let converted = convert_legacy_section_pair(
            context,
            policy,
            &golden.source,
            &golden.expanded,
            &actual,
        )?;
        GoldenSection {
            source: converted.source,
            expanded: converted.expanded,
        }
    };
    let mut result = crate::results::project_golden_blocks(&converted.expanded.blocks)?;
    result.golden = Some(converted);
    Ok(result)
}

fn normalized_results(step: &SqlStep, actual: &[StatementResult]) -> Result<Vec<StatementResult>> {
    // Validate the original column metadata and values before a text
    // transformation. Normalization never supplies a type or changes a shape.
    blocks_from_results(actual)?;
    let mut normalized = actual.to_vec();
    if !step.meta.normalize_explain_timing {
        return Ok(normalized);
    }
    for result in &mut normalized {
        let text_rows = result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| match value {
                        TrinoValue::Text(text) => text.clone(),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let text_rows = crate::results::normalize_explain_timing_rows(&text_rows);
        for (row, text_row) in result.rows.iter_mut().zip(text_rows) {
            for (value, normalized) in row.iter_mut().zip(text_row) {
                if let TrinoValue::Text(text) = value {
                    *text = normalized;
                }
            }
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::golden::{Block, GoldenFile, Section, parse_type};
    use novarocks_test_trino_client::{Column, TrinoValue as V};

    fn step(sql: &str) -> SqlStep {
        SqlStep {
            query_number: 7,
            sql: sql.into(),
            meta: Default::default(),
        }
    }
    fn result(ty: &str, name: &str, values: Vec<V>) -> StatementResult {
        StatementResult {
            columns: vec![Column {
                name: name.into(),
                type_name: ty.into(),
                type_signature: parse_type(ty).unwrap(),
            }],
            rows: values.into_iter().map(|value| vec![value]).collect(),
            query_id: "conversion-query".into(),
            pages: 1,
            update_type: None,
            update_count: None,
        }
    }
    fn execution(results: Vec<StatementResult>) -> QueryExecution {
        QueryExecution {
            result_statements: vec!["SELECT v".into(); results.len()],
            shell_result: None,
            typed_results: results,
            // These legacy projections deliberately carry no type authority.
            header: Vec::new(),
            rows: Vec::new(),
            text_output: String::new(),
            elapsed: std::time::Duration::ZERO,
        }
    }
    fn expected(source: Block, expanded: Block) -> ResultSet {
        let mut view =
            crate::results::project_golden_blocks(std::slice::from_ref(&expanded)).unwrap();
        view.golden = Some(GoldenSection {
            source: Section {
                step: 7,
                blocks: vec![source],
            },
            expanded: Section {
                step: 7,
                blocks: vec![expanded],
            },
        });
        view
    }
    fn legacy(name: &str, values: &[&str]) -> Block {
        Block::Legacy {
            names: vec![name.into()],
            rows: values.iter().map(|value| vec![(*value).into()]).collect(),
        }
    }
    fn context() -> OldRenderContext {
        OldRenderContext {
            variant_offset_seconds: 0,
        }
    }

    #[test]
    fn matching_legacy_uses_the_old_row_mapping_without_mutating_the_oracle() {
        let oracle = expected(legacy("v", &["2", "1", "1"]), legacy("v", &["2", "1", "1"]));
        let original = oracle.clone();
        let actual = execution(vec![result(
            "double",
            "v",
            vec![V::Double(1.0), V::Double(1.0), V::Double(2.0)],
        )]);
        let converted = record_result(
            &step("SELECT v"),
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy::default(),
        )
        .unwrap();
        assert_eq!(converted.rows, vec![vec!["2.0"], vec!["1.0"], vec!["1.0"]]);
        assert!(
            matches!(&converted.golden.as_ref().unwrap().source.blocks[0], Block::Typed { types, .. } if types == &["double"])
        );
        assert_eq!(oracle, original);
        assert!(
            record_result(
                &step("SELECT v"),
                &actual,
                &oracle,
                &context(),
                &OldComparePolicy {
                    order_sensitive: true,
                    float_epsilon: None
                }
            )
            .is_err()
        );
    }
    #[test]
    fn mismatching_legacy_refuses_to_replace_old_expected_values() {
        let oracle = expected(legacy("v", &["3"]), legacy("v", &["3"]));
        let original = oracle.clone();
        let actual = execution(vec![result("bigint", "v", vec![V::Integer(9)])]);
        let error = record_result(
            &step("SELECT v"),
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("legacy rendering mismatch"));
        assert_eq!(oracle, original);
    }
    #[test]
    fn matching_typed_expectations_keep_original_values_even_with_epsilon() {
        let typed = Block::Typed {
            types: vec!["double".into()],
            names: vec!["v".into()],
            rows: vec![vec!["2.0".into()]],
        };
        let oracle = expected(typed.clone(), typed);
        let actual = execution(vec![result("double", "v", vec![V::Double(2.01)])]);
        let converted = record_result(
            &step("SELECT v"),
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy {
                order_sensitive: false,
                float_epsilon: Some(0.1),
            },
        )
        .unwrap();
        assert_eq!(converted, oracle);
        let wrong = execution(vec![result("double", "v", vec![V::Double(9.0)])]);
        assert!(
            record_result(
                &step("SELECT v"),
                &wrong,
                &oracle,
                &context(),
                &OldComparePolicy::default()
            )
            .is_err()
        );
        assert_eq!(oracle.rows, vec![vec!["2.0"]]);
    }
    #[test]
    fn paired_templates_keep_the_original_placeholder_spelling_and_quote_modes() {
        let oracle = expected(
            legacy("${column}", &["${value}", "literal"]),
            legacy("NULL", &["NULL", "literal"]),
        );
        let actual = execution(vec![result(
            "varchar",
            "NULL",
            vec![V::Text("literal".into()), V::Text("NULL".into())],
        )]);
        let converted = record_result(
            &step("SELECT v"),
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy::default(),
        )
        .unwrap();
        let pair = converted.golden.unwrap();
        assert_eq!(
            GoldenFile {
                sections: vec![pair.source]
            }
            .render_template(&GoldenFile {
                sections: vec![pair.expanded]
            })
            .unwrap(),
            "-- query 7\n-- types\tvarchar\n\"${column}\"\n\"${value}\"\nliteral\n"
        );
    }
    #[test]
    fn shell_conversion_uses_the_owned_block_and_preserves_trailing_blank_lines() {
        let source = Block::Shell {
            exit: "0".into(),
            lines: vec!["${text}".into(), "".into()],
        };
        let expanded = Block::Shell {
            exit: "0".into(),
            lines: vec!["NULL".into(), "".into()],
        };
        let oracle = expected(source, expanded.clone());
        let mut actual = execution(Vec::new());
        actual.shell_result = Some(expanded);
        actual.text_output = "a stale flat view does not authorize shell output".into();
        let shell = step("shell: printf 'NULL\\n\\n'");
        let converted = record_result(
            &shell,
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy::default(),
        )
        .unwrap();
        assert_eq!(converted, oracle);
        actual.shell_result = Some(Block::Shell {
            exit: "0".into(),
            lines: vec!["NULL".into()],
        });
        assert!(
            record_result(
                &shell,
                &actual,
                &oracle,
                &context(),
                &OldComparePolicy::default()
            )
            .unwrap_err()
            .to_string()
            .contains("shell result mismatch")
        );
    }
    #[test]
    fn explain_timing_is_normalized_before_both_legacy_equivalence_and_typed_recording() {
        let mut explain = step("EXPLAIN ANALYZE SELECT 7");
        explain.meta.normalize_explain_timing = true;
        let normalized = "Planning: <MS> ms / Execution: <MS> ms / Rows: 7";
        let untouched = "Scan Planning: 99 ms";
        let oracle = expected(
            legacy("Explain", &[normalized, untouched]),
            legacy("Explain", &[normalized, untouched]),
        );
        let actual = execution(vec![result(
            "varchar",
            "Explain",
            vec![
                V::Text("Planning: 12 ms / Execution: 345 ms / Rows: 7".into()),
                V::Text(untouched.into()),
            ],
        )]);
        let converted = record_result(
            &explain,
            &actual,
            &oracle,
            &context(),
            &OldComparePolicy {
                order_sensitive: true,
                float_epsilon: None,
            },
        )
        .unwrap();
        assert_eq!(converted.rows, vec![vec![normalized], vec![untouched]]);
        assert!(
            matches!(&actual.typed_results[0].rows[0][0], V::Text(text) if text.contains("Planning: 12 ms")),
            "raw actual facts are not mutated"
        );
        let again = record_result(
            &explain,
            &actual,
            &converted,
            &context(),
            &OldComparePolicy {
                order_sensitive: true,
                float_epsilon: None,
            },
        )
        .unwrap();
        assert_eq!(again, converted);
        let wrong_rows = execution(vec![result(
            "varchar",
            "Explain",
            vec![
                V::Text("Planning: 12 ms / Execution: 345 ms / Rows: 8".into()),
                V::Text(untouched.into()),
            ],
        )]);
        assert!(
            record_result(
                &explain,
                &wrong_rows,
                &oracle,
                &context(),
                &OldComparePolicy::default()
            )
            .is_err()
        );
    }
    #[test]
    fn conversion_requires_the_paired_expected_section_for_this_step() {
        let actual = execution(Vec::new());
        assert!(
            record_result(
                &step("SELECT 1"),
                &actual,
                &ResultSet::default(),
                &context(),
                &OldComparePolicy::default()
            )
            .is_err()
        );
        let mut oracle = expected(legacy("v", &["1"]), legacy("v", &["1"]));
        oracle.golden.as_mut().unwrap().expanded.step = 8;
        assert!(
            record_result(
                &step("SELECT v"),
                &actual,
                &oracle,
                &context(),
                &OldComparePolicy::default()
            )
            .is_err()
        );
    }
    #[test]
    fn conversion_output_retains_unreferenced_legacy_sections() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("case.result");
        std::fs::write(
            &path,
            "-- query 7\n-- legacy\nv\n1\n\n-- query 31\n-- legacy\noriginal\nretained\\tcell\n",
        )
        .unwrap();
        let variables = std::collections::HashMap::new();
        let mut expectations =
            crate::results::load_expected_results_with_variables(&path, &variables).unwrap();
        let unreferenced = expectations[&31].clone();
        let active = step("SELECT v");
        crate::fixture_context::render_expected_result(
            &active.sql,
            expectations.get_mut(&active.query_number).unwrap(),
            &variables,
            &std::collections::BTreeMap::new(),
        )
        .unwrap();
        let actual = execution(vec![result("bigint", "v", vec![V::Integer(1)])]);
        let converted = record_result(
            &active,
            &actual,
            &expectations[&active.query_number],
            &OldRenderContext {
                variant_offset_seconds: 28800,
            },
            &OldComparePolicy {
                order_sensitive: false,
                float_epsilon: None,
            },
        )
        .unwrap();
        expectations.insert(active.query_number, converted);
        crate::results::write_result_file(&path, &expectations, true).unwrap();
        let restored =
            crate::results::load_expected_results_with_variables(&path, &variables).unwrap();
        assert_eq!(restored.keys().copied().collect::<Vec<_>>(), [7, 31]);
        assert_eq!(restored[&31], unreferenced);
        assert!(matches!(
            restored[&7]
                .golden
                .as_ref()
                .unwrap()
                .source
                .blocks
                .as_slice(),
            [Block::Typed { .. }]
        ));
        assert_eq!(
            GoldenFile::legacy_locations(&std::fs::read_to_string(path).unwrap()).unwrap(),
            [(31, 1)]
        );
    }
    #[test]
    fn retained_async_on_change_actual_rows_use_full_conversion_and_reject_historical_two_rows() {
        // These canonical bytes come from retained native artifacts. The test
        // reconstructs only a fixture; its query id/pages are not HTTP facts.
        const OLD: &str = r#"-- query 3
-- legacy
k1	v2
1	10
2	20
3	30
"#;
        const ACTUAL_THREE: &str = r#"-- query 3
-- types	integer	bigint
k1	v2
1	10
2	20
3	30
"#;
        const HISTORICAL_TWO: &str = r#"-- query 3
-- types	integer	bigint
k1	v2
1	10
2	20
"#;

        fn retained_execution(text: &str) -> QueryExecution {
            let file = GoldenFile::parse(text).unwrap();
            assert_eq!(file.sections.len(), 1);
            assert_eq!(file.sections[0].step, 3);
            let [Block::Typed { types, names, rows }] = file.sections[0].blocks.as_slice() else {
                panic!("retained actual must have exactly one typed block");
            };
            let columns = types
                .iter()
                .zip(names)
                .map(|(ty, name)| Column {
                    name: name.clone(),
                    type_name: ty.clone(),
                    type_signature: parse_type(ty).unwrap(),
                })
                .collect::<Vec<_>>();
            let values = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .zip(&columns)
                        .map(|(cell, column)| {
                            crate::golden::parse_cell(&column.type_signature, cell).unwrap()
                        })
                        .collect()
                })
                .collect();
            execution(vec![StatementResult {
                columns,
                rows: values,
                query_id: "retained-artifact-fixture-not-original-query-id".into(),
                pages: 1,
                update_type: None,
                update_count: None,
            }])
        }

        let old_file = GoldenFile::parse(OLD).unwrap();
        let old_section = old_file.sections[0].clone();
        let mut oracle = crate::results::project_golden_blocks(&old_section.blocks).unwrap();
        oracle.golden = Some(GoldenSection {
            source: old_section.clone(),
            expanded: old_section,
        });
        let original = oracle.clone();
        let mut query = step("SELECT k1, v2 FROM orders_change_mv_${uuid0} ORDER BY k1;");
        query.query_number = 3;
        query.meta.order_sensitive = Some(true);
        query.meta.retry_count = Some(30);
        query.meta.retry_interval_ms = Some(500);
        let context = OldRenderContext {
            variant_offset_seconds: 28800,
        };
        let policy = OldComparePolicy {
            order_sensitive: true,
            float_epsilon: None,
        };
        let actual = retained_execution(ACTUAL_THREE);
        let converted = record_result(&query, &actual, &oracle, &context, &policy).unwrap();
        assert_eq!(oracle, original, "the old independent oracle is immutable");
        let pair = converted.golden.as_ref().unwrap();
        assert_eq!(
            GoldenFile {
                sections: vec![pair.source.clone()]
            }
            .render()
            .unwrap(),
            ACTUAL_THREE
        );
        assert_eq!(pair.source, pair.expanded);

        // Exercise the permanent file writer in an isolated temporary directory.
        let output = tempfile::tempdir().unwrap();
        let path = output.path().join("proof.result");
        crate::results::write_result_file(
            &path,
            &std::collections::BTreeMap::from([(3, converted)]),
            true,
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), ACTUAL_THREE);

        let historical = retained_execution(HISTORICAL_TWO);
        let error = record_result(&query, &historical, &oracle, &context, &policy).unwrap_err();
        assert!(error.to_string().contains("legacy rendering mismatch"));
        assert_eq!(
            oracle, original,
            "a wrong two-row result must not replace the old three rows"
        );
        let pair = oracle.golden.as_ref().unwrap();
        assert_eq!(
            GoldenFile {
                sections: vec![pair.source.clone()]
            }
            .render()
            .unwrap(),
            OLD
        );
    }
}
