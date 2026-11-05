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

//! Typed result construction and the runner's unchanged comparison policy.
use anyhow::{Result, ensure};
use novarocks_test_trino_client::{StatementResult, TrinoValue};

use super::{Block, parse_cell, parse_text, parse_type, render_cell};

pub fn blocks_from_results(results: &[StatementResult]) -> Result<Vec<Block>> {
    results
        .iter()
        .filter(|result| result.is_result_set())
        .map(|result| {
            for column in &result.columns {
                ensure!(
                    parse_type(&column.type_name)? == column.type_signature,
                    "Trino column type text differs from its signature: {}",
                    column.type_name
                );
            }
            let rows = result
                .rows
                .iter()
                .map(|row| {
                    ensure!(
                        row.len() == result.columns.len(),
                        "Trino result width differs from its columns"
                    );
                    row.iter()
                        .zip(&result.columns)
                        .map(|(value, column)| render_cell(&column.type_signature, value))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Block::Typed {
                types: result
                    .columns
                    .iter()
                    .map(|column| column.type_name.clone())
                    .collect(),
                names: result
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                rows,
            })
        })
        .collect()
}

/// Input is the exact combined exit-code/newline/stdout text from the shell owner.
pub fn shell_block(text: &str) -> Block {
    let (exit, output) = text.split_once('\n').unwrap_or((text, ""));
    Block::Shell {
        exit: exit.to_string(),
        lines: output.split_terminator('\n').map(str::to_string).collect(),
    }
}

/// Text assertions see string contents and otherwise the canonical typed text.
pub fn blocks_text(blocks: &[Block]) -> Result<String> {
    let mut lines = Vec::new();
    for block in blocks {
        match block {
            Block::Typed { types, names, rows } => {
                ensure!(
                    types.len() == names.len(),
                    "golden type and name widths differ"
                );
                let signatures = types
                    .iter()
                    .map(|ty| parse_type(ty))
                    .collect::<Result<Vec<_>>>()?;
                lines.push(names.join("\t"));
                for row in rows {
                    ensure!(
                        row.len() == types.len(),
                        "golden data width differs from its types"
                    );
                    let cells = row
                        .iter()
                        .zip(&signatures)
                        .map(|(text, signature)| {
                            Ok(match parse_cell(signature, text)? {
                                TrinoValue::Text(value) => value,
                                TrinoValue::Binary(_) => parse_text(text)?,
                                _ => text.clone(),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    lines.push(cells.join("\t"));
                }
            }
            Block::Shell {
                exit,
                lines: output,
            } => {
                lines.push(exit.clone());
                lines.extend(output.iter().cloned());
            }
            Block::Legacy { names, rows } => {
                lines.push(
                    names
                        .iter()
                        .map(|text| crate::results::unescape_cell(text))
                        .collect::<Vec<_>>()
                        .join("\t"),
                );
                lines.extend(rows.iter().map(|row| {
                    row.iter()
                        .map(|text| crate::results::unescape_cell(text))
                        .collect::<Vec<_>>()
                        .join("\t")
                }));
            }
        }
    }
    Ok(lines.join("\n"))
}

pub fn compare_blocks(
    expected: &[Block],
    actual: &[Block],
    order_sensitive: bool,
    epsilon: Option<f64>,
) -> (bool, String) {
    if expected
        .iter()
        .any(|block| matches!(block, Block::Legacy { .. }))
    {
        return (false, "legacy expectation pending conversion".to_string());
    }
    if expected.len() != actual.len() {
        return (
            false,
            format!(
                "block count mismatch (actual={}, expected={})",
                actual.len(),
                expected.len()
            ),
        );
    }
    for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        let (ok, detail) = match (expected, actual) {
            (
                Block::Typed {
                    types: expected_types,
                    names: expected_names,
                    rows: expected_rows,
                },
                Block::Typed {
                    types: actual_types,
                    names: actual_names,
                    rows: actual_rows,
                },
            ) => {
                if expected_types != actual_types {
                    (
                        false,
                        format!(
                            "type mismatch (actual={actual_types:?}, expected={expected_types:?})"
                        ),
                    )
                } else {
                    crate::results::compare_result_sets(
                        expected_names,
                        expected_rows,
                        actual_names,
                        actual_rows,
                        order_sensitive,
                        epsilon,
                    )
                }
            }
            (
                Block::Shell {
                    exit: expected_exit,
                    lines: expected_lines,
                },
                Block::Shell {
                    exit: actual_exit,
                    lines: actual_lines,
                },
            ) => {
                if expected_exit == actual_exit && expected_lines == actual_lines {
                    (true, String::new())
                } else {
                    (
                        false,
                        format!(
                            "shell result mismatch (actual={actual_exit:?}/{actual_lines:?}, expected={expected_exit:?}/{expected_lines:?})"
                        ),
                    )
                }
            }
            _ => (false, "result block kind mismatch".to_string()),
        };
        if !ok {
            return (false, format!("block {}: {detail}", index + 1));
        }
    }
    (true, String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_test_trino_client::{Column, StatementResult, TrinoValue as V};

    fn result(ty: &str, rows: Vec<Vec<V>>) -> StatementResult {
        StatementResult {
            columns: vec![Column {
                name: "v".into(),
                type_name: ty.into(),
                type_signature: parse_type(ty).unwrap(),
            }],
            rows,
            query_id: "query".into(),
            pages: 1,
            update_type: None,
            update_count: None,
        }
    }

    #[test]
    fn results_preserve_statement_blocks_and_reject_conflicting_metadata() {
        let mut inputs = vec![
            result("boolean", vec![vec![V::Boolean(true)]]),
            result("varchar", vec![vec![V::Text("NULL".into())]]),
        ];
        let blocks = blocks_from_results(&inputs).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks_text(&blocks).unwrap(), "v\ntrue\nv\nNULL");
        inputs[0].columns[0].type_signature = parse_type("bigint").unwrap();
        assert!(blocks_from_results(&inputs).is_err());
    }

    #[test]
    fn shell_empty_lines_and_carriage_returns_are_not_filtered() {
        assert_eq!(
            shell_block("0\n\nx\n\n"),
            Block::Shell {
                exit: "0".into(),
                lines: vec!["".into(), "x".into(), "".into()],
            }
        );
        assert_eq!(
            blocks_text(&[shell_block("0\n\nx\n\n")]).unwrap(),
            "0\n\nx\n"
        );
        assert_eq!(
            shell_block("0"),
            Block::Shell {
                exit: "0".into(),
                lines: vec![]
            }
        );
        assert_eq!(
            shell_block("0\nx\r\n"),
            Block::Shell {
                exit: "0".into(),
                lines: vec!["x\r".into()]
            }
        );
    }

    #[test]
    fn comparison_keeps_type_names_order_and_epsilon_responsibilities() {
        let expected = blocks_from_results(&[result(
            "double",
            vec![vec![V::Double(2.0)], vec![V::Double(1.0)]],
        )])
        .unwrap();
        let actual = blocks_from_results(&[result(
            "double",
            vec![vec![V::Double(1.01)], vec![V::Double(2.0)]],
        )])
        .unwrap();
        assert!(compare_blocks(&expected, &actual, false, Some(0.1)).0);
        assert!(!compare_blocks(&expected, &actual, true, Some(0.1)).0);
        assert!(!compare_blocks(&expected, &actual, false, None).0);
        let wrong_type =
            blocks_from_results(&[result("real", vec![vec![V::Real(2.0)], vec![V::Real(1.0)]])])
                .unwrap();
        assert!(
            compare_blocks(&expected, &wrong_type, false, Some(1.0))
                .1
                .contains("type mismatch")
        );
        let mut wrong_name = expected.clone();
        let Block::Typed { names, .. } = &mut wrong_name[0] else {
            unreachable!()
        };
        names[0] = "different".into();
        assert!(
            compare_blocks(&expected, &wrong_name, false, None)
                .1
                .contains("header mismatch")
        );
    }

    #[test]
    fn legacy_failure_is_distinct_and_shell_comparison_is_strict() {
        let legacy = Block::Legacy {
            names: vec!["v".into()],
            rows: vec![vec!["1".into()]],
        };
        assert_eq!(
            compare_blocks(&[legacy], &[], false, None),
            (false, "legacy expectation pending conversion".into())
        );
        assert!(
            !compare_blocks(
                &[shell_block("0\nx\ny")],
                &[shell_block("0\ny\nx")],
                false,
                Some(100.0)
            )
            .0
        );
    }

    #[test]
    fn text_assertions_decode_strings_but_keep_variant_json_and_numeric_spelling() {
        let blocks = blocks_from_results(&[
            result(
                "varchar",
                vec![vec![V::Text("a\tb".into())], vec![V::Text("".into())]],
            ),
            result("variant", vec![vec![V::Variant("\"x\"".into())]]),
            result("double", vec![vec![V::Double(2.0)]]),
        ])
        .unwrap();
        assert_eq!(blocks_text(&blocks).unwrap(), "v\na\tb\n\nv\n\"x\"\nv\n2.0");
    }
    #[test]
    fn command_count_rows_never_create_query_golden_blocks() {
        let before = result("bigint", vec![vec![V::Integer(1)]]);
        let after = result("bigint", vec![vec![V::Integer(2)]]);
        let mut counted = result("bigint", vec![vec![V::Integer(0)]]);
        counted.columns[0].name = "rows".into();
        counted.update_type = Some("INSERT".into());
        counted.update_count = Some(0);
        let mut unknown = result("bigint", vec![vec![V::Null]]);
        unknown.update_type = Some("ANALYZE".into());
        let expected = blocks_from_results(&[before.clone(), after.clone()]).unwrap();
        let actual = blocks_from_results(&[before, counted, unknown, after]).unwrap();
        assert_eq!(actual, expected);
    }
}
