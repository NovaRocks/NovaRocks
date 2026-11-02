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

//! Old-oracle-preserving conversion. A block changes only after full equivalence.
use anyhow::{Context, Result, bail, ensure};
use novarocks_test_trino_client::{StatementResult, TrinoValue};

use super::execution::{blocks_from_results, compare_blocks};
use super::grammar::{render_template_text, validate_section_pair};
use super::legacy::{OldComparePolicy, OldRenderContext, old_equivalent, read_legacy};
use super::{Block, GoldenFile, Section};

/// Both sides are required when source spellings contain expanded placeholders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConvertedSection {
    pub source: Section,
    pub expanded: Section,
}

pub fn convert_legacy_section(
    context: &OldRenderContext,
    policy: &OldComparePolicy,
    section: &Section,
    actual: &[StatementResult],
) -> Result<Section> {
    Ok(convert_legacy_section_pair(context, policy, section, section, actual)?.source)
}

pub fn convert_legacy_section_expanded(
    context: &OldRenderContext,
    policy: &OldComparePolicy,
    original: &Section,
    expanded: &Section,
    actual: &[StatementResult],
) -> Result<Section> {
    Ok(convert_legacy_section_pair(context, policy, original, expanded, actual)?.source)
}

/// The caller supplies source and expansion from the same placeholder author.
/// Original legacy blocks are atomic: missing resultsets never authorize a split.
pub fn convert_legacy_section_pair(
    context: &OldRenderContext,
    policy: &OldComparePolicy,
    original: &Section,
    expanded: &Section,
    actual: &[StatementResult],
) -> Result<ConvertedSection> {
    GoldenFile {
        sections: vec![expanded.clone()],
    }
    .render()?;
    validate_section_pair(original, expanded)?;
    if matches!(original.blocks.as_slice(), [Block::Legacy { .. }]) {
        let (source, expanded_blocks) = convert_group(
            context,
            policy,
            &original.blocks[0],
            &expanded.blocks[0],
            actual,
        )?;
        return Ok(ConvertedSection {
            source: Section {
                step: original.step,
                blocks: source,
            },
            expanded: Section {
                step: original.step,
                blocks: expanded_blocks,
            },
        });
    }
    let actual_blocks = blocks_from_results(actual)?;
    if !expanded
        .blocks
        .iter()
        .any(|block| matches!(block, Block::Legacy { .. }))
    {
        let (equal, detail) = compare_blocks(
            &expanded.blocks,
            &actual_blocks,
            policy.order_sensitive,
            policy.float_epsilon,
        );
        ensure!(equal, "existing typed expectation mismatch: {detail}");
        return Ok(ConvertedSection {
            source: original.clone(),
            expanded: expanded.clone(),
        });
    }
    let actual_results = actual
        .iter()
        .filter(|result| result.has_columns())
        .collect::<Vec<_>>();
    ensure!(
        original.blocks.len() == actual_results.len(),
        "mixed legacy section requires one explicit result per block"
    );
    let mut source_blocks = Vec::new();
    let mut expanded_blocks = Vec::new();
    for ((source, expanded), actual) in original
        .blocks
        .iter()
        .zip(&expanded.blocks)
        .zip(actual_results)
    {
        match source {
            Block::Legacy { .. } => {
                let (source, expanded) = convert_group(
                    context,
                    policy,
                    source,
                    expanded,
                    std::slice::from_ref(actual),
                )?;
                source_blocks.extend(source);
                expanded_blocks.extend(expanded);
            }
            Block::Typed { .. } => {
                let observed = blocks_from_results(std::slice::from_ref(actual))?;
                let (equal, detail) = compare_blocks(
                    std::slice::from_ref(expanded),
                    &observed,
                    policy.order_sensitive,
                    policy.float_epsilon,
                );
                ensure!(equal, "existing typed expectation mismatch: {detail}");
                source_blocks.push(source.clone());
                expanded_blocks.push(expanded.clone());
            }
            Block::Shell { .. } => {
                bail!("shell blocks require their shell result, not SQL migration")
            }
        }
    }
    Ok(ConvertedSection {
        source: Section {
            step: original.step,
            blocks: source_blocks,
        },
        expanded: Section {
            step: original.step,
            blocks: expanded_blocks,
        },
    })
}

#[derive(Clone, Copy)]
enum Origin {
    Header(usize),
    Row(usize, usize),
}

fn convert_group(
    context: &OldRenderContext,
    policy: &OldComparePolicy,
    original: &Block,
    expanded: &Block,
    actual: &[StatementResult],
) -> Result<(Vec<Block>, Vec<Block>)> {
    let old_source = read_legacy(original)?;
    let old_expanded = read_legacy(expanded)?;
    let mapping = old_equivalent(context, policy, &old_expanded, actual)?;
    let observed = actual
        .iter()
        .filter(|result| result.has_columns())
        .collect::<Vec<_>>();
    ensure!(
        !observed.is_empty(),
        "legacy conversion has no typed result"
    );
    let rendered = blocks_from_results(actual)?;
    let mut expanded_blocks = rendered.clone();
    let mut source_blocks = rendered.clone();
    for block in source_blocks.iter_mut().chain(&mut expanded_blocks) {
        let Block::Typed { rows, .. } = block else {
            unreachable!("rendered SQL result")
        };
        rows.clear();
    }
    restore_names(
        &old_source.header,
        &old_expanded.header,
        &mut source_blocks[0],
    )?;
    let mut origins = Vec::new();
    for (result_index, result) in observed.iter().enumerate() {
        if result_index != 0 {
            origins.push(Origin::Header(result_index));
        }
        origins.extend((0..result.rows.len()).map(|row| Origin::Row(result_index, row)));
    }
    ensure!(
        mapping.actual_rows.len() == old_source.rows.len()
            && old_source.rows.len() == old_expanded.rows.len()
            && origins.len() == mapping.actual_rows.len(),
        "legacy row mapping shape differs from its original block"
    );
    let mut seen = vec![false; origins.len()];
    for (expected_row, actual_row) in mapping.actual_rows.into_iter().enumerate() {
        let origin = *origins
            .get(actual_row)
            .context("legacy row mapping is outside actual result")?;
        ensure!(!seen[actual_row], "legacy row mapping reuses an actual row");
        seen[actual_row] = true;
        match origin {
            Origin::Header(result) => {
                restore_names(
                    &old_source.rows[expected_row],
                    &old_expanded.rows[expected_row],
                    &mut source_blocks[result],
                )?;
            }
            Origin::Row(result, row) => {
                let Block::Typed {
                    rows: canonical_rows,
                    ..
                } = &rendered[result]
                else {
                    unreachable!()
                };
                let canonical = &canonical_rows[row];
                let original = &old_source.rows[expected_row];
                let expanded = &old_expanded.rows[expected_row];
                ensure!(
                    original.len() == canonical.len() && expanded.len() == canonical.len(),
                    "legacy mapped row width differs from typed result"
                );
                let restored = original
                    .iter()
                    .zip(expanded)
                    .zip(&observed[result].rows[row])
                    .zip(canonical)
                    .map(|(((source, expanded), value), canonical)| {
                        restore_cell(source, expanded, value, canonical)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let Block::Typed {
                    rows: source_rows, ..
                } = &mut source_blocks[result]
                else {
                    unreachable!()
                };
                source_rows.push(restored);
                let Block::Typed {
                    rows: expanded_rows,
                    ..
                } = &mut expanded_blocks[result]
                else {
                    unreachable!()
                };
                expanded_rows.push(canonical.clone());
            }
        }
    }
    ensure!(
        seen.into_iter().all(|seen| seen),
        "legacy row mapping omits actual output"
    );
    Ok((source_blocks, expanded_blocks))
}

fn restore_names(source: &[String], expanded: &[String], block: &mut Block) -> Result<()> {
    let Block::Typed { names, .. } = block else {
        unreachable!("typed output")
    };
    ensure!(
        source.len() == names.len() && expanded.len() == names.len(),
        "legacy header width differs from typed result"
    );
    for ((source, expanded), actual) in source.iter().zip(expanded).zip(names) {
        ensure!(
            expanded.as_str() == actual.as_str(),
            "legacy header mapping differs from actual result"
        );
        *actual = source.clone();
    }
    Ok(())
}

fn restore_cell(
    source: &str,
    expanded: &str,
    value: &TrinoValue,
    canonical: &str,
) -> Result<String> {
    if source == expanded {
        return Ok(canonical.to_string());
    }
    // Only a paired, whole text value has an unambiguous source projection.
    // Nested/format-changing templates require an explicit typed source author;
    // searching the canonical actual text for coincident values is forbidden.
    let TrinoValue::Text(text) = value else {
        bail!("legacy placeholder requires an explicit typed projection for this value");
    };
    ensure!(
        expanded == text,
        "legacy placeholder changes representation; explicit typed projection required"
    );
    Ok(render_template_text(source, text))
}

#[cfg(test)]
mod tests {
    use super::super::{GoldenFile, parse_type};
    use super::*;
    use novarocks_test_trino_client::{Column, TrinoValue as V};

    fn result(name: &str, ty: &str, rows: Vec<V>) -> StatementResult {
        StatementResult {
            columns: vec![Column {
                name: name.into(),
                type_name: ty.into(),
                type_signature: parse_type(ty).unwrap(),
            }],
            rows: rows.into_iter().map(|v| vec![v]).collect(),
            query_id: "query".into(),
            pages: 1,
            update_type: None,
        }
    }
    fn context() -> OldRenderContext {
        OldRenderContext {
            variant_offset_seconds: 0,
        }
    }
    fn legacy(names: &[&str], rows: &[&str]) -> Section {
        Section {
            step: 7,
            blocks: vec![Block::Legacy {
                names: names.iter().map(|s| s.to_string()).collect(),
                rows: rows.iter().map(|s| vec![s.to_string()]).collect(),
            }],
        }
    }

    #[test]
    fn unordered_conversion_keeps_old_row_order_and_refuses_non_equivalence() {
        let original = legacy(&["v"], &["2", "1"]);
        let actual = vec![result("v", "double", vec![V::Double(1.0), V::Double(2.0)])];
        let converted =
            convert_legacy_section(&context(), &OldComparePolicy::default(), &original, &actual)
                .unwrap();
        let Block::Typed { rows, .. } = &converted.blocks[0] else {
            unreachable!()
        };
        assert_eq!(
            rows,
            &vec![vec!["2.0".to_string()], vec!["1.0".to_string()]]
        );
        assert!(
            convert_legacy_section(
                &context(),
                &OldComparePolicy {
                    order_sensitive: true,
                    float_epsilon: None
                },
                &original,
                &actual
            )
            .is_err()
        );
        assert!(
            convert_legacy_section(
                &context(),
                &OldComparePolicy::default(),
                &legacy(&["v"], &["3", "1"]),
                &actual
            )
            .is_err()
        );
        assert!(
            matches!(original.blocks[0], Block::Legacy { .. }),
            "original never mutated"
        );
    }

    #[test]
    fn flattened_later_headers_are_not_typed_data_rows() {
        let original = legacy(&["first"], &["2", "1", "second", "x"]);
        let actual = vec![
            result("first", "bigint", vec![V::Integer(1), V::Integer(2)]),
            result("second", "varchar", vec![V::Text("x".into())]),
        ];
        let converted =
            convert_legacy_section(&context(), &OldComparePolicy::default(), &original, &actual)
                .unwrap();
        assert_eq!(converted.blocks.len(), 2);
        let Block::Typed { names, rows, .. } = &converted.blocks[1] else {
            unreachable!()
        };
        assert_eq!(names, &["second".to_string()]);
        assert_eq!(rows, &[vec!["x".to_string()]]);
        assert!(
            convert_legacy_section(
                &context(),
                &OldComparePolicy::default(),
                &original,
                &actual[..1]
            )
            .is_err()
        );
    }

    #[test]
    fn paired_placeholders_preserve_source_spelling_and_quoted_expansion() {
        let source = legacy(&["${name}"], &["${x}", "literal"]);
        let expanded = legacy(&["NULL"], &["NULL", "literal"]);
        let actual = vec![result(
            "NULL",
            "varchar",
            vec![V::Text("NULL".into()), V::Text("literal".into())],
        )];
        let converted = convert_legacy_section_pair(
            &context(),
            &OldComparePolicy::default(),
            &source,
            &expanded,
            &actual,
        )
        .unwrap();
        let source_file = GoldenFile {
            sections: vec![converted.source],
        };
        let expanded_file = GoldenFile {
            sections: vec![converted.expanded],
        };
        let text = source_file.render_template(&expanded_file).unwrap();
        assert_eq!(
            text,
            "-- query 7\n-- types\tvarchar\n\"${name}\"\n\"${x}\"\nliteral\n"
        );
        assert!(
            text.contains("\nliteral\n"),
            "coincident literals are not rewritten"
        );
    }

    #[test]
    fn placeholder_projection_never_guesses_nested_or_reformatted_values() {
        let source = legacy(&["v"], &["${flag}"]);
        let expanded = legacy(&["v"], &["1"]);
        assert!(
            convert_legacy_section_pair(
                &context(),
                &OldComparePolicy::default(),
                &source,
                &expanded,
                &[result("v", "boolean", vec![V::Boolean(true)])]
            )
            .is_err()
        );
        let source = legacy(&["v"], &["${stamp}"]);
        let expanded = legacy(&["v"], &["2024-01-01 00:00:00"]);
        assert!(
            convert_legacy_section_pair(
                &context(),
                &OldComparePolicy::default(),
                &source,
                &expanded,
                &[result(
                    "v",
                    "timestamp(6)",
                    vec![V::Text("2024-01-01 00:00:00.000000".into())]
                )]
            )
            .is_err()
        );
    }

    #[test]
    fn existing_typed_blocks_must_still_match_and_are_preserved() {
        let actual = vec![result("v", "double", vec![V::Double(2.0)])];
        let typed = Section {
            step: 7,
            blocks: blocks_from_results(&actual).unwrap(),
        };
        assert_eq!(
            convert_legacy_section(&context(), &OldComparePolicy::default(), &typed, &actual)
                .unwrap(),
            typed
        );
        assert!(
            convert_legacy_section(
                &context(),
                &OldComparePolicy::default(),
                &typed,
                &[result("v", "double", vec![V::Double(3.0)])]
            )
            .is_err()
        );
    }
}
