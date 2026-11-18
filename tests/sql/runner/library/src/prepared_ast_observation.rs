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

//! Observe the original dry-run preparation in process without publishing SQL text.

use crate::{
    parser::CaseSourceFrames,
    results_record::{ResultsWriter, digest, optional_hash},
    types::QueryMeta,
};
use anyhow::{Context, Result, ensure};
use novarocks_parser::{
    ast::{Query, SyntaxEq, Visit},
    parse, trino_parser,
};
use serde_json::{Value, json};
use std::{
    fs,
    io::ErrorKind,
    ops::Range,
    path::{Path, PathBuf},
};

pub(crate) struct PeerRoot {
    workspace: PathBuf,
    root: PathBuf,
}

impl PeerRoot {
    pub(crate) fn bind(workspace: &Path, root: &Path) -> Result<Self> {
        ensure!(
            root.is_absolute(),
            "prepared AST peer root must be absolute"
        );
        let root = root.canonicalize().context("open prepared AST peer root")?;
        ensure!(root.is_dir(), "prepared AST peer root must be a directory");
        ensure!(
            root != workspace.canonicalize()?,
            "prepared AST peer root is the original workspace"
        );
        Ok(Self {
            workspace: workspace.to_path_buf(),
            root,
        })
    }

    pub(crate) fn record_scope(&self, writer: &ResultsWriter) {
        writer.observe(json!({
            "kind": "prepared_ast", "schema": 1, "stage": "peer_scope",
            "facts": {"root": self.root, "execution": "NOT_RUN", "actual_cleanup": "native_run_records"},
        }));
    }

    fn resolve(&self, original: &Path) -> Result<Option<PathBuf>> {
        let relative = original
            .strip_prefix(&self.workspace)
            .context("original prepared SQL is outside the workspace")?;
        ensure!(
            relative.starts_with("tests/sql/correctness"),
            "peer requires a declared correctness SQL path"
        );
        let path = self.root.join(relative);
        match path.canonicalize() {
            Ok(canonical) => {
                ensure!(
                    canonical == path && canonical.is_file(),
                    "peer is not the exact mirrored SQL file"
                );
                Ok(Some(canonical))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

pub(crate) struct Observer<'a> {
    writer: &'a ResultsWriter,
    suite: &'a str,
    side: &'static str,
}

impl<'a> Observer<'a> {
    pub(crate) fn new(writer: &'a ResultsWriter, suite: &'a str) -> Self {
        Self {
            writer,
            suite,
            side: "original",
        }
    }

    fn record(&self, stage: &'static str, facts: Value) {
        self.writer.observe(json!({
            "kind": "prepared_ast", "schema": 1, "suite": self.suite, "side": self.side,
            "stage": stage, "facts": facts,
        }));
    }

    pub(crate) fn peer_not_prepared(&self, original: &Path, role: &str, status: &str) {
        self.record(
            "peer_binding",
            json!({
                "original_path": original, "role": role, "status": status, "preparation": "NOT_RUN",
            }),
        );
    }

    pub(crate) fn observe_peer<T>(
        &self,
        root: &PeerRoot,
        original: &Path,
        role: &str,
        load: impl FnOnce(&Path, &str, &Observer<'_>) -> Result<Option<T>>,
    ) {
        let path = match root.resolve(original) {
            Ok(Some(path)) => path,
            Ok(None) => return self.peer_not_prepared(original, role, "missing_peer"),
            Err(_) => return self.peer_not_prepared(original, role, "invalid_peer_binding"),
        };
        let Ok(source) = fs::read_to_string(&path) else {
            return self.peer_not_prepared(original, role, "unreadable_peer");
        };
        let sha = digest(source.as_bytes());
        let peer = Self {
            writer: self.writer,
            suite: self.suite,
            side: "peer",
        };
        let result = load(&path, &source, &peer);
        let stable = root.resolve(original).ok().flatten().as_deref() == Some(path.as_path())
            && optional_hash(Some(&path)).ok().flatten().as_deref() == Some(sha.as_str());
        let status = if !stable {
            "peer_source_changed_or_unreadable"
        } else {
            match result {
                Ok(Some(_)) => "loaded",
                Ok(None) => "empty",
                Err(_) => "load_rejected",
            }
        };
        self.record(
            "peer_binding",
            json!({
                "original_path": original, "peer_path": path, "role": role,
                "same_basename": true, "resource_binding": "original_variables_borrow",
                "peer_source_sha256": sha, "peer_source_stable": stable,
                "status": status, "execution": "NOT_RUN", "oracle_execution": "NOT_RUN",
            }),
        );
    }

    pub(crate) fn raw_case(
        &self,
        path: &Path,
        source: &str,
        frames: Option<&CaseSourceFrames>,
    ) -> String {
        let source_sha256 = digest(source.as_bytes());
        self.record("raw_case", json!({
            "path": path,
            "source_sha256": source_sha256,
            "source_len": source.len(),
            "raw_framing": if frames.is_some() { "accepted" } else { "rejected" },
            "preface_range": frames.map(|frames| [frames.file_preface.start, frames.file_preface.end]),
            "frames": frames.map(|frames| frames.sections.iter().map(|(step, range)| json!({
                "step": step, "source_range": [range.start, range.end],
                "section_sha256": digest(source[range.clone()].as_bytes()),
            })).collect::<Vec<_>>()),
        }));
        source_sha256
    }

    pub(crate) fn step(
        &self,
        path: &Path,
        source_sha256: &str,
        prepared_document_sha256: &str,
        step: usize,
        prepared_section_range: Range<usize>,
        prepared_section: &str,
        sql: &str,
        meta: &QueryMeta,
        required: &[&str],
    ) {
        self.record("prepared_case_step", json!({
            "path": path, "source_sha256": source_sha256, "step": step,
            "prepared_document_sha256": prepared_document_sha256,
            "prepared_section_range": [prepared_section_range.start, prepared_section_range.end],
            "prepared_section_sha256": digest(prepared_section.as_bytes()),
            "prepared_sql_sha256": digest(sql.as_bytes()), "prepared_sql_len": sql.len(),
            "oracle": {"required": required, "expected_error": meta.has_error_expectation(),
                       "expect_trino_error": meta.expect_trino_error.as_ref().map(|error| {
                           json!({"name": error.name, "type": error.error_type})
                       }),
                       "skip_result_check": meta.skip_result_check,
                       "order_sensitive": meta.order_sensitive, "float_epsilon": meta.float_epsilon,
                       "execution": "NOT_RUN"},
            "cleanup_step": meta.cleanup,
            "old_parser": inspect(sql),
        }));
    }

    pub(crate) fn raw_hook(&self, path: &Path, role: &str, span: [usize; 2], sha: &str) {
        self.record(
            "raw_suite_hook",
            json!({
                "path": path, "role": role, "source_range": span, "source_sha256": sha,
            }),
        );
    }

    pub(crate) fn hook(
        &self,
        path: &Path,
        role: &str,
        source_range: [usize; 2],
        source_sha256: &str,
        substituted_document: &str,
        sql: &str,
    ) {
        self.record("prepared_suite_hook", json!({
            "path": path, "role": role, "source_range": source_range, "source_sha256": source_sha256,
            "substituted_document_sha256": digest(substituted_document.as_bytes()),
            "substituted_document_range": [0, substituted_document.len()],
            "prepared_sql_sha256": digest(sql.as_bytes()), "prepared_sql_len": sql.len(),
            "hook_execution": "NOT_RUN",
            "old_parser": inspect(sql),
        }));
    }

    pub(crate) fn case_outcome(&self, path: &Path, status: &str) {
        self.record("case_load", json!({"path": path, "status": status}));
    }

    pub(crate) fn suite_checked(&self, inputs_sha256: &Value) {
        self.record(
            "suite_inputs_checked",
            json!({"inputs_sha256": inputs_sha256}),
        );
    }
}

fn inspect(sql: &str) -> Value {
    if sql.trim().is_empty() {
        return json!({"status": "empty"});
    }
    if crate::shell::is_shell_step(sql) {
        return json!({"status": "external_shell"});
    }
    let statements = match parse(sql) {
        Ok(statements) => statements,
        Err(error) => {
            let diagnostic = error.to_user_error(sql);
            return json!({
                "status": "rejected", "code": diagnostic.code().as_str(),
                "prepared_sql_location": diagnostic.location().map(|location| json!({
                    "line": location.line(), "column": location.column(),
                    "end_line": location.end_line(), "end_column": location.end_column(),
                })),
            });
        }
    };
    let mut queries = Vec::new();
    for (index, statement) in statements.iter().enumerate() {
        let mut visitor = PreparedQueries {
            sql,
            statement: index + 1,
            ordinal: 0,
            queries: &mut queries,
        };
        visitor.visit_statement(statement);
    }
    json!({"status": "accepted", "statements": statements.len(), "queries": queries})
}

struct PreparedQueries<'a> {
    sql: &'a str,
    statement: usize,
    ordinal: usize,
    queries: &'a mut Vec<Value>,
}

impl Visit for PreparedQueries<'_> {
    fn visit_query(&mut self, query: &Query) {
        self.ordinal += 1;
        let start = query.span.start();
        let end = query.span.end();
        let source = self.sql.get(start..end);
        let trino = match source {
            None => json!({"status": "source_range_unavailable"}),
            Some(source) => match trino_parser::parse_query(source) {
                Ok(parsed) => json!({
                    "status": if query.syntax_eq(&parsed) { "strict_equal" } else { "different" },
                }),
                Err(error) => json!({
                    "status": "rejected", "code": error.to_user_error(source).code().as_str(),
                }),
            },
        };
        self.queries.push(json!({
            "statement": self.statement, "query": self.ordinal, "prepared_sql_range": [start, end],
            "query_sha256": source.map(|source| digest(source.as_bytes())),
            "trino_same_source": trino,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::placeholder_variables, fixture_context::RunNames, parser, types::RunnerConfig,
    };
    use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
    use regex::Regex;
    use std::{collections::BTreeMap, sync::Arc};

    #[test]
    fn prepared_query_compares_actual_bytes_without_disclosing_them() {
        for (sql, expected) in [
            ("SELECT COUNT()", "strict_equal"),
            ("SELECT COUNT(*)", "different"),
            ("SELECT `private_name`", "rejected"),
        ] {
            let report = inspect(sql);
            assert_eq!(report["status"], "accepted");
            let query = &report["queries"][0];
            assert_eq!(query["query_sha256"], digest(sql.as_bytes()));
            assert_eq!(query["trino_same_source"]["status"], expected);
            assert!(!report.to_string().contains(sql));
        }
    }

    #[test]
    fn paired_case_keeps_original_naming_and_independent_source_and_oracle_facts() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let alternate = temp.path().join("alternate");
        let relative = Path::new("tests/sql/correctness/join/sql/paired.sql");
        let original = workspace.join(relative);
        let peer_path = alternate.join(relative);
        fs::create_dir_all(original.parent().unwrap()).unwrap();
        fs::create_dir_all(peer_path.parent().unwrap()).unwrap();
        let before = "SELECT '${case_db}' AS db_name;\n";
        let after =
            "-- @cleanup=true\n-- @skip_result_check=true\nSELECT '${case_db}' AS db_name;\n";
        fs::write(&original, before).unwrap();
        fs::write(&peer_path, after).unwrap();
        let budget = RunBudget::admit(
            FixturePolicy::default(),
            ScratchIdentity::create("s3://novarocks/paired-observation").unwrap(),
            600,
            None,
        )
        .unwrap();
        let names = RunNames::prepare(
            &budget,
            &BTreeMap::from([("join".into(), vec![original.clone()])]),
        )
        .unwrap();
        let config = RunnerConfig {
            fixture_names: Some(Arc::new(names)),
            ..RunnerConfig::default()
        };
        let variables = placeholder_variables(&config, "join").unwrap();
        let bound = PeerRoot::bind(&workspace, &alternate).unwrap();
        let output = temp.path().join("results.jsonl");
        let writer = ResultsWriter::create(temp.path(), Some(output.to_str().unwrap())).unwrap();
        let observer = Observer::new(&writer, "join");
        let meta = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        let case = parser::load_sql_case_from_file_observed(
            &original,
            &meta,
            &marker,
            &variables,
            Some(&observer),
            Some(&bound),
        )
        .unwrap()
        .unwrap();
        assert_eq!(case.case_id, "paired");
        assert!(!case.steps[0].meta.cleanup);
        assert!(!case.steps[0].meta.skip_result_check);
        let expected = format!("SELECT '{}' AS db_name;", case.auto_db);
        assert_eq!(case.steps[0].sql, expected);
        writer.check().unwrap();
        let text = fs::read_to_string(&output).unwrap();
        assert!(!text.contains(&case.auto_db));
        let records: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for (side, raw, cleanup, required) in [
            ("original", before, false, json!(["result_set"])),
            ("peer", after, true, json!([])),
        ] {
            let row = records
                .iter()
                .find(|row| row["side"] == side && row["stage"] == "prepared_case_step")
                .unwrap();
            let facts = &row["facts"];
            assert_eq!(facts["source_sha256"], digest(raw.as_bytes()));
            assert_eq!(facts["prepared_sql_sha256"], digest(expected.as_bytes()));
            assert_eq!(facts["cleanup_step"], cleanup);
            assert_eq!(facts["oracle"]["required"], required);
            assert_eq!(facts["oracle"]["execution"], "NOT_RUN");
        }
        let pair = records
            .iter()
            .find(|row| row["stage"] == "peer_binding")
            .unwrap();
        assert_eq!(pair["facts"]["peer_source_stable"], true);
        assert_eq!(pair["facts"]["same_basename"], true);
        assert_eq!(pair["facts"]["status"], "loaded");
    }

    #[cfg(unix)]
    #[test]
    fn peer_paths_require_the_declared_mirror_and_never_follow_file_aliases() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let alternate = temp.path().join("alternate");
        let relative = Path::new("tests/sql/correctness/join/sql/case.sql");
        let original = workspace.join(relative);
        let peer = alternate.join(relative);
        fs::create_dir_all(original.parent().unwrap()).unwrap();
        fs::write(&original, "SELECT 1;").unwrap();
        fs::create_dir_all(peer.parent().unwrap()).unwrap();
        let bound = PeerRoot::bind(&workspace, &alternate).unwrap();
        assert!(bound.resolve(&original).unwrap().is_none());
        std::os::unix::fs::symlink(&original, &peer).unwrap();
        assert!(bound.resolve(&original).is_err());
        fs::remove_file(&peer).unwrap();
        fs::write(&peer, "SELECT 2;").unwrap();
        assert_eq!(
            bound.resolve(&original).unwrap().unwrap(),
            peer.canonicalize().unwrap()
        );
        assert!(bound.resolve(&temp.path().join("case.sql")).is_err());
    }
}
