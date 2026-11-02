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

use anyhow::{Context, Result, bail, ensure};
use std::fs;
use std::path::Path;

use super::{parse_cell, parse_text, parse_type, render_text};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoldenFile {
    pub sections: Vec<Section>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    pub step: usize,
    pub blocks: Vec<Block>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    /// Names are decoded contents; rows contain canonical typed cell text.
    Typed {
        types: Vec<String>,
        names: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Exit and output are decoded contents, quoted only by the file writer.
    Shell { exit: String, lines: Vec<String> },
    /// Raw old TSV cells, including their original escaping and row widths.
    Legacy {
        names: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

/// A template is usable only together with its strictly validated expansion.
#[derive(Clone, Debug)]
pub struct GoldenTemplate {
    source: GoldenFile,
    expanded: GoldenFile,
}
impl GoldenTemplate {
    pub fn source(&self) -> &GoldenFile {
        &self.source
    }
    pub fn expanded(&self) -> &GoldenFile {
        &self.expanded
    }
    pub fn render(&self) -> Result<String> {
        self.source.render_template(&self.expanded)
    }
}

impl GoldenFile {
    pub fn read(path: &Path) -> Result<Self> {
        Self::parse(
            &fs::read_to_string(path).with_context(|| format!("read golden {}", path.display()))?,
        )
    }
    pub fn write(&self, path: &Path) -> Result<()> {
        fs::write(path, self.render()?).with_context(|| format!("write golden {}", path.display()))
    }
    /// Callers expand fixture/catalog placeholders before invoking this parser.
    pub fn parse(text: &str) -> Result<Self> {
        Self::parse_inner(text, true)
    }
    /// Inventory framing without interpreting unexpanded template data. This
    /// returns locations only; the relaxed material cannot enter verification.
    pub fn legacy_locations(source: &str) -> Result<Vec<(usize, usize)>> {
        Ok(Self::parse_inner(source, false)?
            .sections
            .iter()
            .flat_map(|section| {
                section
                    .blocks
                    .iter()
                    .enumerate()
                    .filter_map(|(index, block)| {
                        matches!(block, Block::Legacy { .. }).then_some((section.step, index + 1))
                    })
            })
            .collect())
    }
    /// The caller supplies the exact expansion made by its placeholder owner.
    /// There is no public lax reader which can be used as verify material alone.
    pub fn parse_template(source: &str, expanded: &str) -> Result<GoldenTemplate> {
        let expanded = Self::parse(expanded)?;
        let source_file = Self::parse_inner(source, false)?;
        ensure!(
            source_file.render_template(&expanded)? == source,
            "golden template framing is not canonical"
        );
        Ok(GoldenTemplate {
            source: source_file,
            expanded,
        })
    }
    fn parse_inner(text: &str, canonical: bool) -> Result<Self> {
        if text.is_empty() {
            return Ok(Self::default());
        }
        let lines = text.split_terminator('\n').collect::<Vec<_>>();
        let mut index = 0;
        let mut sections = Vec::new();
        while index < lines.len() {
            if lines[index].is_empty() {
                index += 1;
                continue;
            }
            let Some(number) = lines[index].strip_prefix("-- query ") else {
                bail!("expected golden query section at line {}", index + 1);
            };
            let step: usize = number.parse().context("invalid golden query number")?;
            if step == 0
                || sections
                    .last()
                    .is_some_and(|section: &Section| section.step >= step)
            {
                bail!("golden query sections must increase without duplicates");
            }
            index += 1;
            let mut blocks = Vec::new();
            while index < lines.len()
                && !lines[index].is_empty()
                && !lines[index].starts_with("-- query ")
            {
                let marker = lines[index];
                index += 1;
                if let Some(type_line) = marker.strip_prefix("-- types\t") {
                    let types = type_line
                        .split('\t')
                        .map(str::to_string)
                        .collect::<Vec<_>>();
                    let signatures = types
                        .iter()
                        .map(|ty| parse_type(ty))
                        .collect::<Result<Vec<_>>>()?;
                    let names = take_line(&lines, &mut index, "typed names")?
                        .split('\t')
                        .map(|text| decode_text(text, canonical))
                        .collect::<Result<Vec<_>>>()?;
                    if names.len() != types.len() {
                        bail!("golden type and name widths differ");
                    }
                    let mut rows = Vec::new();
                    while is_data(&lines, index) {
                        let row = lines[index]
                            .split('\t')
                            .map(str::to_string)
                            .collect::<Vec<_>>();
                        if row.len() != types.len() {
                            bail!("golden data width differs from its types");
                        }
                        if canonical {
                            for (signature, text) in signatures.iter().zip(&row) {
                                parse_cell(signature, text)?;
                            }
                        }
                        rows.push(row);
                        index += 1;
                    }
                    blocks.push(Block::Typed { types, names, rows });
                } else if marker == "-- shell" {
                    let exit =
                        decode_text(take_line(&lines, &mut index, "shell exit code")?, canonical)?;
                    let mut output = Vec::new();
                    while is_data(&lines, index) {
                        output.push(decode_text(lines[index], canonical)?);
                        index += 1;
                    }
                    blocks.push(Block::Shell {
                        exit,
                        lines: output,
                    });
                } else if marker == "-- legacy" {
                    let names = take_line(&lines, &mut index, "legacy names")?
                        .split('\t')
                        .map(str::to_string)
                        .collect();
                    let mut rows = Vec::new();
                    while is_data(&lines, index) {
                        rows.push(lines[index].split('\t').map(str::to_string).collect());
                        index += 1;
                    }
                    blocks.push(Block::Legacy { names, rows });
                } else {
                    bail!("unknown golden metadata line: {marker}");
                }
            }
            sections.push(Section { step, blocks });
        }
        let file = Self { sections };
        if canonical && file.render()? != text {
            bail!("golden file is not canonical");
        }
        Ok(file)
    }
    /// Writes original placeholder spellings using the validated expansion's
    /// name/shell quote modes. Typed source cells already retain their syntax.
    pub fn render_template(&self, expanded: &Self) -> Result<String> {
        expanded.render()?;
        ensure!(
            self.sections.len() == expanded.sections.len(),
            "golden template section count differs from expansion"
        );
        let mut out = String::new();
        for (index, (source, expanded)) in self.sections.iter().zip(&expanded.sections).enumerate()
        {
            validate_section_pair(source, expanded)?;
            if index != 0 {
                out.push('\n');
            }
            push_line(&mut out, &format!("-- query {}", source.step));
            for (source, expanded) in source.blocks.iter().zip(&expanded.blocks) {
                match (source, expanded) {
                    (
                        Block::Typed { types, names, rows },
                        Block::Typed {
                            names: expanded_names,
                            ..
                        },
                    ) => {
                        push_line(&mut out, &format!("-- types\t{}", types.join("\t")));
                        push_line(
                            &mut out,
                            &names
                                .iter()
                                .zip(expanded_names)
                                .map(|(source, expanded)| render_template_text(source, expanded))
                                .collect::<Vec<_>>()
                                .join("\t"),
                        );
                        for row in rows {
                            ensure!(
                                row.iter().all(|text| !text.contains(['\n', '\r', '\t'])),
                                "golden template cell breaks framing"
                            );
                            let line = row.join("\t");
                            ensure!(
                                !line.is_empty() && !line.starts_with("-- "),
                                "golden template row breaks metadata framing"
                            );
                            push_line(&mut out, &line);
                        }
                    }
                    (
                        Block::Shell { exit, lines },
                        Block::Shell {
                            exit: expanded_exit,
                            lines: expanded_lines,
                        },
                    ) => {
                        out.push_str("-- shell\n");
                        push_line(&mut out, &render_template_text(exit, expanded_exit));
                        for (source, expanded) in lines.iter().zip(expanded_lines) {
                            push_line(&mut out, &render_template_text(source, expanded));
                        }
                    }
                    (Block::Legacy { names, rows }, Block::Legacy { .. }) => {
                        out.push_str("-- legacy\n");
                        for row in std::iter::once(names).chain(rows) {
                            let line = row.join("\t");
                            ensure!(
                                !line.is_empty()
                                    && !line.starts_with("-- ")
                                    && !line.contains(['\n', '\r']),
                                "legacy template row breaks framing"
                            );
                            push_line(&mut out, &line);
                        }
                    }
                    _ => unreachable!("validated template block kinds"),
                }
            }
        }
        Ok(out)
    }
    pub fn render(&self) -> Result<String> {
        let mut out = String::new();
        let mut previous = 0;
        for (position, section) in self.sections.iter().enumerate() {
            if section.step <= previous {
                bail!("golden query sections must increase without duplicates");
            }
            previous = section.step;
            if position != 0 {
                out.push('\n');
            }
            out.push_str(&format!("-- query {}\n", section.step));
            for block in &section.blocks {
                match block {
                    Block::Typed { types, names, rows } => {
                        if types.is_empty() || types.len() != names.len() {
                            bail!("golden type block must have matching nonempty types and names");
                        }
                        let signatures = types
                            .iter()
                            .map(|ty| {
                                if ty.contains(['\n', '\r', '\t']) {
                                    bail!("Trino type text breaks golden framing");
                                }
                                parse_type(ty)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        out.push_str("-- types\t");
                        out.push_str(&types.join("\t"));
                        out.push('\n');
                        push_line(
                            &mut out,
                            &names
                                .iter()
                                .map(|name| render_text(name))
                                .collect::<Vec<_>>()
                                .join("\t"),
                        );
                        for row in rows {
                            if row.len() != types.len() {
                                bail!("golden data width differs from its types");
                            }
                            for (signature, text) in signatures.iter().zip(row) {
                                parse_cell(signature, text)?;
                            }
                            push_line(&mut out, &row.join("\t"));
                        }
                    }
                    Block::Shell { exit, lines } => {
                        out.push_str("-- shell\n");
                        push_line(&mut out, &render_text(exit));
                        for line in lines {
                            push_line(&mut out, &render_text(line));
                        }
                    }
                    Block::Legacy { names, rows } => {
                        if names.is_empty() {
                            bail!("legacy block requires a name row");
                        }
                        out.push_str("-- legacy\n");
                        for cells in std::iter::once(names).chain(rows) {
                            let line = cells.join("\t");
                            if line.is_empty()
                                || line.starts_with("-- ")
                                || line.contains(['\n', '\r'])
                            {
                                bail!("legacy TSV row breaks golden framing");
                            }
                            push_line(&mut out, &line);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

fn decode_text(text: &str, canonical: bool) -> Result<String> {
    if canonical {
        return parse_text(text);
    }
    Ok(if text.starts_with('"') {
        serde_json::from_str(text)?
    } else {
        text.to_string()
    })
}

/// Quote mode belongs to the expanded value. The source may contain a placeholder
/// whose expansion changes whether quotes are necessary.
pub(super) fn render_template_text(source: &str, expanded: &str) -> String {
    if render_text(expanded).starts_with('"') {
        let quoted = render_text(source);
        if quoted.starts_with('"') {
            quoted
        } else {
            serde_json::to_string(source).expect("string JSON encoding")
        }
    } else {
        source.to_string()
    }
}

pub(super) fn validate_section_pair(source: &Section, expanded: &Section) -> Result<()> {
    ensure!(
        source.step == expanded.step,
        "golden template step differs from expansion"
    );
    ensure!(
        source.blocks.len() == expanded.blocks.len(),
        "golden template block count differs from expansion"
    );
    for (source, expanded) in source.blocks.iter().zip(&expanded.blocks) {
        match (source, expanded) {
            (
                Block::Typed { types, names, rows },
                Block::Typed {
                    types: expanded_types,
                    names: expanded_names,
                    rows: expanded_rows,
                },
            ) => {
                ensure!(
                    types == expanded_types && !types.is_empty(),
                    "golden template type declarations differ from expansion"
                );
                for ty in types {
                    parse_type(ty)?;
                }
                ensure!(
                    names.len() == types.len() && expanded_names.len() == types.len(),
                    "golden template name width differs from expansion"
                );
                ensure!(
                    rows.len() == expanded_rows.len()
                        && rows.iter().all(|row| row.len() == types.len()),
                    "golden template data shape differs from expansion"
                );
            }
            (
                Block::Shell { lines, .. },
                Block::Shell {
                    lines: expanded_lines,
                    ..
                },
            ) => {
                ensure!(
                    lines.len() == expanded_lines.len(),
                    "golden shell template line count differs from expansion"
                );
            }
            (
                Block::Legacy { names, rows },
                Block::Legacy {
                    names: expanded_names,
                    rows: expanded_rows,
                },
            ) => {
                ensure!(
                    names.len() == expanded_names.len()
                        && rows.len() == expanded_rows.len()
                        && rows
                            .iter()
                            .zip(expanded_rows)
                            .all(|(left, right)| left.len() == right.len()),
                    "legacy template shape differs from expansion"
                );
            }
            _ => bail!("golden template block kind differs from expansion"),
        }
    }
    Ok(())
}

fn is_data(lines: &[&str], index: usize) -> bool {
    index < lines.len() && !lines[index].is_empty() && !lines[index].starts_with("-- ")
}
fn take_line<'a>(lines: &[&'a str], index: &mut usize, description: &str) -> Result<&'a str> {
    if !is_data(lines, *index) {
        bail!("missing golden {description}");
    }
    let line = lines[*index];
    *index += 1;
    Ok(line)
}
fn push_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

#[cfg(test)]
mod template_tests {
    use super::*;
    #[test]
    fn expanded_values_determine_quote_mode_without_reverse_substitution() {
        for (source_value, expanded_value) in [
            ("\"${x}\"", "\"NULL\""),
            ("\"${x}\"", "\"\""),
            ("\"${x}\"", "\"tail \""),
            ("\"${x}\"", "\"a\\tb\""),
            ("${x}", "literal"),
        ] {
            let source =
                format!("-- query 1\n-- types\tvarchar\n\"${{name}}\"\n{source_value}\nliteral\n");
            let expanded =
                format!("-- query 1\n-- types\tvarchar\n\"NULL\"\n{expanded_value}\nliteral\n");
            let pair = GoldenFile::parse_template(&source, &expanded).unwrap();
            assert_eq!(pair.render().unwrap(), source);
            let Block::Typed { names, rows, .. } = &pair.source().sections[0].blocks[0] else {
                unreachable!()
            };
            assert_eq!(names, &["${name}".to_string()]);
            assert_eq!(
                rows[1][0], "literal",
                "coincident literal must not become a placeholder"
            );
        }
    }
    #[test]
    fn template_requires_strict_expansion_and_identical_framing() {
        let source = "-- query 1\n-- types\tbigint\nv\n${x}\n";
        let expanded = "-- query 1\n-- types\tbigint\nv\n1\n";
        assert!(GoldenFile::parse_template(source, expanded).is_ok());
        assert!(GoldenFile::parse_template(source, &expanded.replace("\n1\n", "\n01\n")).is_err());
        assert!(
            GoldenFile::parse_template(&source.replace("-- types", "-- unknown"), expanded)
                .is_err()
        );
        assert!(
            GoldenFile::parse_template(source, &expanded.replace("\n1\n", "\n1\n2\n")).is_err()
        );
        assert!(GoldenFile::parse(source).is_err());
    }
}
