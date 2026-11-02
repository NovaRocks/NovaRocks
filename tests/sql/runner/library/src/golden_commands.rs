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

//! Resource-free inventory of remaining legacy golden blocks.
// Design: ADR-0210.
use crate::golden::GoldenFile;
use crate::types::SuiteConfig;
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

pub(crate) fn inventory(cli: &crate::Cli, suites: &BTreeMap<String, SuiteConfig>) -> Result<i32> {
    let selected = selected_suites(cli, suites)?;
    let mut locations = Vec::new();
    for name in selected {
        let suite = &suites[&name];
        let sql_dir = cli
            .sql_dir
            .as_deref()
            .map(Path::new)
            .unwrap_or(&suite.sql_dir);
        let result_dir = cli
            .result_dir
            .as_deref()
            .map(Path::new)
            .or(suite.result_dir.as_deref());
        let Some(result_dir) = result_dir else {
            continue;
        };
        if !result_dir.exists() {
            continue;
        }
        let pattern = cli.sql_glob.as_deref().unwrap_or(&suite.sql_glob);
        let sql_files = crate::config::list_sql_files(sql_dir, pattern)?;
        let mut case_ids = BTreeSet::new();
        for sql in sql_files {
            let case_id = sql
                .file_stem()
                .and_then(|s| s.to_str())
                .context("invalid SQL case name")?;
            case_ids.insert(case_id.to_string());
            let path = crate::results::case_result_path(result_dir, case_id);
            if !path.exists() {
                continue;
            }
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read golden {}", path.display()))?;
            let found = GoldenFile::legacy_locations(&raw)
                .with_context(|| format!("inventory golden {}", path.display()))?;
            for (step, block) in found {
                locations.push(
                    serde_json::json!({"suite":name,"case":case_id,"step":step,"block":block}),
                );
            }
        }
        // An orphan cannot be assigned a statement number or a shell owner.
        // Respect an explicit case glob; otherwise inventory every result file.
        if cli.sql_glob.is_none() {
            for path in crate::config::list_sql_files(result_dir, "*.result")? {
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("invalid result case name")?;
                ensure!(
                    case_ids.contains(id),
                    "golden has no matching SQL source: {}",
                    path.display()
                );
            }
        }
    }
    for location in locations {
        println!("{location}");
    }
    Ok(0)
}

fn selected_suites(
    cli: &crate::Cli,
    suites: &BTreeMap<String, SuiteConfig>,
) -> Result<Vec<String>> {
    let mut selected = BTreeSet::new();
    match cli.suite.as_deref() {
        None | Some("all") => selected.extend(suites.keys().cloned()),
        Some(names) => {
            for name in names.split(',').map(str::trim) {
                ensure!(suites.contains_key(name), "unknown suite: {name}");
                selected.insert(name.to_string());
            }
        }
    }
    ensure!(
        selected.len() == 1
            || (cli.sql_dir.is_none() && cli.result_dir.is_none() && cli.sql_glob.is_none()),
        "golden directory/glob overrides require exactly one suite"
    );
    Ok(selected.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::suite_manifest::SuiteManifest;
    use clap::Parser;

    fn cli(args: &[&str]) -> crate::Cli {
        crate::Cli::try_parse_from(std::iter::once("runner").chain(args.iter().copied())).unwrap()
    }
    fn suite(name: &str, root: &Path, disabled: bool) -> SuiteConfig {
        SuiteConfig {
            name: name.into(),
            sql_dir: root.join(name).join("sql"),
            result_dir: Some(root.join(name).join("result")),
            sql_glob: "*.sql".into(),
            default_catalog: "default_catalog".into(),
            default_db: String::new(),
            auto_case_db: false,
            verify_default: true,
            init_sql: None,
            cleanup_sql: None,
            manifest: SuiteManifest {
                explicit_only: disabled,
                disabled_reason: disabled.then(|| "disabled for execution".into()),
                ..Default::default()
            },
        }
    }
    #[test]
    fn inventory_selection_includes_disabled_and_explicit_suites() {
        let root = tempfile::tempdir().unwrap();
        let suites = BTreeMap::from([
            ("a".into(), suite("a", root.path(), false)),
            ("b".into(), suite("b", root.path(), true)),
        ]);
        assert_eq!(
            selected_suites(&cli(&["--list-legacy-goldens"]), &suites).unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            selected_suites(&cli(&["--list-legacy-goldens", "--suite", "all"]), &suites).unwrap(),
            ["a", "b"]
        );
        assert!(
            selected_suites(
                &cli(&["--list-legacy-goldens", "--sql-dir", "other"]),
                &suites
            )
            .is_err()
        );
    }
    #[test]
    fn framing_inventory_preserves_templates_and_rejects_unknown_metadata() {
        let raw = "-- query 2\n-- types\tvarchar\nv\n\"${fixture}\"\n-- legacy\nv\nold\n-- shell\n0\n${fixture}\n\n-- query 4\n-- legacy\nv\nx\n";
        assert_eq!(GoldenFile::legacy_locations(raw).unwrap(), [(2, 2), (4, 1)]);
        assert!(GoldenFile::legacy_locations("-- query 1\n-- unknown\nv\n").is_err());
        assert!(GoldenFile::legacy_locations("-- query 1\n-- types\talien_type\nv\nx\n").is_err());
    }
    #[test]
    fn inventory_keeps_orphan_checks_and_respects_an_explicit_glob_without_writes() {
        let root = tempfile::tempdir().unwrap();
        let config = suite("a", root.path(), false);
        fs::create_dir_all(&config.sql_dir).unwrap();
        let result_dir = config.result_dir.as_ref().unwrap();
        fs::create_dir_all(result_dir).unwrap();
        fs::write(config.sql_dir.join("case.sql"), "SELECT 1;\n").unwrap();
        let golden = "-- query 1\n-- types\tvarchar\nv\n\"${fixture}\"\n-- legacy\nv\nold\n";
        let case = result_dir.join("case.result");
        let orphan = result_dir.join("orphan.result");
        fs::write(&case, golden).unwrap();
        fs::write(&orphan, "-- query 1\n-- legacy\nv\nold\n").unwrap();
        let suites = BTreeMap::from([("a".into(), config)]);
        let error =
            inventory(&cli(&["--list-legacy-goldens", "--suite", "a"]), &suites).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("golden has no matching SQL source")
        );
        assert_eq!(
            inventory(
                &cli(&[
                    "--list-legacy-goldens",
                    "--suite",
                    "a",
                    "--sql-glob",
                    "case*.sql"
                ]),
                &suites,
            )
            .unwrap(),
            0
        );
        assert_eq!(fs::read_to_string(case).unwrap(), golden);
        assert_eq!(
            fs::read_to_string(orphan).unwrap(),
            "-- query 1\n-- legacy\nv\nold\n"
        );
    }

    #[test]
    fn inventory_rejects_unmarked_input_without_rewriting_it() {
        let root = tempfile::tempdir().unwrap();
        let config = suite("a", root.path(), false);
        fs::create_dir_all(&config.sql_dir).unwrap();
        let result_dir = config.result_dir.as_ref().unwrap();
        fs::create_dir_all(result_dir).unwrap();
        fs::write(config.sql_dir.join("case.sql"), "SELECT 1;\n").unwrap();
        let path = result_dir.join("case.result");
        let original = "v\n1\n";
        fs::write(&path, original).unwrap();
        let suites = BTreeMap::from([("a".into(), config)]);
        assert!(inventory(&cli(&["--list-legacy-goldens", "--suite", "a"]), &suites).is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), original);
    }
}
