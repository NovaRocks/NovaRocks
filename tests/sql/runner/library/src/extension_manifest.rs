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

use crate::config::list_sql_files;
use crate::parser::load_sql_case_from_file_preserving_placeholders;
use crate::types::{SqlCase, SuiteConfig};
use anyhow::{Context, Result};
use regex::Regex;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionManifestEntry {
    pub suite: String,
    pub case_id: String,
    pub statement: usize,
    pub extension: String,
    pub statement_summary: String,
}

/// Derive the declaration manifest from SQL source without launching a server.
/// Placeholder values intentionally remain literal `${...}` tokens so the
/// resulting text is stable across invocations.
pub fn derive_extension_manifest(
    suite_configs: &BTreeMap<String, SuiteConfig>,
    meta_re: &Regex,
    marker_re: &Regex,
) -> Result<Vec<ExtensionManifestEntry>> {
    let mut entries = Vec::new();
    for (suite_name, suite) in suite_configs {
        let files = list_sql_files(&suite.sql_dir, &suite.sql_glob)
            .with_context(|| format!("list extension cases for suite {suite_name}"))?;
        for file in files {
            let source = std::fs::read_to_string(&file)
                .with_context(|| format!("read extension case {}", file.display()))?;
            let variables = literal_placeholder_variables(&source);
            let Some(case) = load_sql_case_from_file_preserving_placeholders(
                &file, meta_re, marker_re, &variables,
            )
            .with_context(|| format!("parse extension case {}", file.display()))?
            else {
                continue;
            };
            entries.extend(entries_for_case(suite_name, case));
        }
    }
    entries.sort_by(|left, right| {
        (
            &left.suite,
            &left.case_id,
            left.statement,
            &left.extension,
            &left.statement_summary,
        )
            .cmp(&(
                &right.suite,
                &right.case_id,
                right.statement,
                &right.extension,
                &right.statement_summary,
            ))
    });
    Ok(entries)
}

pub fn render_extension_manifest(entries: &[ExtensionManifestEntry]) -> String {
    let mut output = String::from(
        "# NovaRocks SQL extension manifest\n# suite\tcase\tstatement\textension\tstatement_summary\n",
    );
    for entry in entries {
        output.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\n",
            entry.suite, entry.case_id, entry.statement, entry.extension, entry.statement_summary,
        ));
    }
    output
}

fn entries_for_case(suite: &str, case: SqlCase) -> Vec<ExtensionManifestEntry> {
    case.steps
        .into_iter()
        .filter_map(|step| {
            step.meta
                .nova_extension
                .map(|extension| ExtensionManifestEntry {
                    suite: suite.to_string(),
                    case_id: case.case_id.clone(),
                    statement: step.query_number,
                    extension,
                    statement_summary: summarize_statement(&step.sql),
                })
        })
        .collect()
}

pub(crate) fn literal_placeholder_variables(source: &str) -> HashMap<String, String> {
    let placeholder_re =
        Regex::new(r"\$\{([A-Za-z0-9_.-]+)\}").expect("literal placeholder regex must compile");
    placeholder_re
        .captures_iter(source)
        .filter_map(|captures| {
            let key = captures.get(1)?.as_str();
            Some((key.to_string(), format!("${{{key}}}")))
        })
        .collect()
}

fn summarize_statement(sql: &str) -> String {
    const MAX_SUMMARY_CHARS: usize = 160;

    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_SUMMARY_CHARS {
        return normalized;
    }
    let prefix: String = normalized.chars().take(MAX_SUMMARY_CHARS - 1).collect();
    format!("{prefix}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::suite_manifest::SuiteManifest;
    use crate::types::SuiteConfig;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn meta_re() -> Regex {
        Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").expect("meta regex")
    }

    fn marker_re() -> Regex {
        Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").expect("marker regex")
    }

    fn suite_config(name: &str, sql_dir: PathBuf) -> SuiteConfig {
        SuiteConfig {
            name: name.to_string(),
            sql_dir,
            result_dir: None,
            sql_glob: "*.sql".to_string(),
            default_catalog: "default_catalog".to_string(),
            default_db: String::new(),
            auto_case_db: false,
            verify_default: true,
            init_sql: None,
            cleanup_sql: None,
            manifest: SuiteManifest::default(),
        }
    }

    #[test]
    fn manifest_derivation_is_sorted_and_keeps_placeholders_literal() {
        let temp = tempdir().expect("temporary suite root");
        let sql_dir = temp.path().join("sql");
        std::fs::create_dir(&sql_dir).expect("create SQL dir");
        std::fs::write(
            sql_dir.join("z_case.sql"),
            "-- query 1\n-- @nova_extension=branch DDL\nALTER TABLE ${case_db}.t CREATE BRANCH dev;\n",
        )
        .expect("write z case");
        std::fs::write(
            sql_dir.join("a_case.sql"),
            "-- @nova_extension=materialized view refresh\nREFRESH MATERIALIZED VIEW mv WITH SYNC MODE;\n",
        )
        .expect("write a case");

        let suites = BTreeMap::from([(
            "extensions".to_string(),
            suite_config("extensions", sql_dir),
        )]);
        let entries =
            derive_extension_manifest(&suites, &meta_re(), &marker_re()).expect("derive manifest");

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].case_id, "a_case");
        assert_eq!(entries[1].case_id, "z_case");
        assert_eq!(entries[1].statement, 1);
        assert_eq!(
            entries[1].statement_summary,
            "ALTER TABLE ${case_db}.t CREATE BRANCH dev;"
        );
        assert_eq!(
            render_extension_manifest(&entries),
            "# NovaRocks SQL extension manifest\n# suite\tcase\tstatement\textension\tstatement_summary\nextensions\ta_case\t1\tmaterialized view refresh\tREFRESH MATERIALIZED VIEW mv WITH SYNC MODE;\nextensions\tz_case\t1\tbranch DDL\tALTER TABLE ${case_db}.t CREATE BRANCH dev;\n"
        );
    }
    #[test]
    fn manifest_derivation_accepts_unbound_fault_definitions_without_dropping_entries() {
        let temp = tempdir().unwrap();
        let actual = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../correctness/lnp-3d-mv-accelerator/sql/mv_catalog_isolation.sql");
        std::fs::copy(&actual, temp.path().join("mv_catalog_isolation.sql")).unwrap();
        std::fs::write(
            temp.path().join("extension.sql"),
            "-- @nova_extension=branch DDL\nALTER TABLE ${case_db}.t CREATE BRANCH dev;\n",
        )
        .unwrap();
        let suites = BTreeMap::from([(
            "definitions".to_string(),
            suite_config("definitions", temp.path().to_path_buf()),
        )]);
        let entries = derive_extension_manifest(&suites, &meta_re(), &marker_re()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].case_id, "extension");
        assert_eq!(
            entries[0].statement_summary,
            "ALTER TABLE ${case_db}.t CREATE BRANCH dev;"
        );
        assert!(render_extension_manifest(&entries).contains("${case_db}"));
    }
}
