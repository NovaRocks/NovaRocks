// SPDX-License-Identifier: Apache-2.0

use super::{extract_case_source_definition, extract_suite_hook, load_sql_case_from_file};
use crate::config::{build_suite_configs, list_sql_files};
use crate::fixture_context::RunNames;
use crate::session::split_trino_step_sql;
use crate::shell::is_shell_step;
use crate::types::RunnerConfig;
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
use novarocks_parser::{ParserError, ast::TrinoStatement, parser::parse_statement};
use regex::Regex;
use std::path::Path;
use std::{collections::BTreeMap, fs, sync::Arc};

#[test]
fn repository_corpus_uses_the_runner_step_and_statement_boundaries() -> Result<()> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    let meta = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
    let markers = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
    let mut cases = 0;
    let (mut shell_steps, mut empty_steps, mut templated_sql) = (0, 0, 0);
    let (mut complete_sql, mut statements, mut asserted_error_steps) = (0, 0, 0);
    let (mut complete_hooks, mut templated_hooks) = (0, 0);
    for suite in build_suite_configs(&workspace)?.into_values() {
        for file in list_sql_files(&suite.sql_dir, &suite.sql_glob)? {
            cases += 1;
            let source = fs::read_to_string(&file)?;
            let case = extract_case_source_definition(&source, &meta, &markers)
                .with_context(|| format!("frame authored case {}", file.display()))?;
            let mut previous = 0;
            for (index, step) in case.steps.into_iter().enumerate() {
                ensure!(
                    step.query_number == index + 1
                        && previous <= step.source_range.start
                        && source.get(step.source_range.clone()).is_some(),
                    "invalid step {} source coordinates in {}",
                    step.query_number,
                    file.display()
                );
                previous = step.source_range.end;
                if is_shell_step(&step.native_sql) {
                    shell_steps += 1;
                    ensure!(
                        step.meta.expect_trino_error.is_none(),
                        "shell step {} in {} has a SQL error assertion",
                        step.query_number,
                        file.display()
                    );
                    continue;
                }
                // Such SQL is framed again by execution after the original
                // fixture/RunVars owner supplies real parameter values.
                if step.native_sql.is_empty() {
                    empty_steps += 1;
                    continue;
                }
                if step.native_sql.contains("${") {
                    templated_sql += 1;
                    continue;
                }
                complete_sql += 1;
                asserted_error_steps += usize::from(step.meta.expect_trino_error.is_some());
                let frames =
                    split_trino_step_sql(&step.native_sql, step.meta.expect_trino_error.as_ref())
                        .with_context(|| {
                        format!("frame step {} in {}", step.query_number, file.display())
                    })?;
                statements += frames.len();
            }
        }
        for hook in [suite.init_sql, suite.cleanup_sql].into_iter().flatten() {
            if !hook.exists() {
                continue;
            }
            let source = fs::read_to_string(&hook)?;
            let lines: Vec<_> = source.lines().map(ToString::to_string).collect();
            let (_, _, sql) = extract_suite_hook(&lines, &meta)
                .with_context(|| format!("frame authored hook {}", hook.display()))?;
            if sql.contains("${") {
                templated_hooks += 1;
            } else if !sql.is_empty() {
                complete_hooks += 1;
                split_trino_step_sql(&sql, None)
                    .with_context(|| format!("frame submitted hook {}", hook.display()))?;
            }
        }
    }
    assert!(cases > 0);
    assert!(shell_steps > 0 && templated_sql > 0 && asserted_error_steps > 0);
    assert!(complete_sql > 0 && statements >= complete_sql);
    eprintln!(
        "Authored SQL corpus: {cases} cases, {shell_steps} shell steps, {empty_steps} empty steps, \
         {templated_sql} SQL templates, {complete_sql} framed SQL steps with {statements} statements \
         and {asserted_error_steps} typed error steps, {complete_hooks} framed hooks, \
         {templated_hooks} templated hooks"
    );
    Ok(())
}

#[test]
fn bound_complex_type_corpus_preserves_original_step_and_error_ordinals() -> Result<()> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    let meta = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
    let markers = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
    let suites = build_suite_configs(&workspace)?;
    let suite_name = "complex-type";
    let suite = &suites[suite_name];
    let sources = BTreeMap::from([(
        suite_name.to_owned(),
        list_sql_files(&suite.sql_dir, &suite.sql_glob)?,
    )]);
    let budget = RunBudget::admit(
        FixturePolicy::default(),
        ScratchIdentity::create("s3://novarocks/runner-corpus-unit")?,
        600,
        None,
    )?;
    let mut names = RunNames::prepare(&budget, &sources)?;
    let mut config = RunnerConfig::default();
    names.seal_catalogs(&config, &sources, &suites)?;
    config.fixture_names = Some(Arc::new(names));
    let vars = crate::config::placeholder_variables(&config, suite_name)?;
    for path in &sources[suite_name] {
        let source = fs::read_to_string(path)?;
        let raw = extract_case_source_definition(&source, &meta, &markers)?;
        let bound = load_sql_case_from_file(path, &meta, &markers, &vars)?
            .with_context(|| format!("missing bound case {}", path.display()))?;
        let authored = raw.steps.iter().filter(|step| !step.native_sql.is_empty());
        assert!(
            bound
                .steps
                .iter()
                .map(|step| step.query_number)
                .eq(authored.map(|step| step.query_number))
        );
        for step in bound.steps {
            if is_shell_step(&step.sql) {
                continue;
            }
            let frames = split_trino_step_sql(&step.sql, step.meta.expect_trino_error.as_ref())?;
            if bound.case_id == "complex_test_array_contains"
                && (71..=72).contains(&step.query_number)
            {
                let [prefix, refused] = frames.as_slice() else {
                    panic!(
                        "array_contains step {} lost an original statement",
                        step.query_number
                    );
                };
                assert!(matches!(
                    parse_statement(prefix),
                    Ok(TrinoStatement::Use(_))
                ));
                assert!(matches!(
                    parse_statement(refused),
                    Err(ParserError::Lex(_) | ParserError::Parse(_))
                ));
                assert_eq!(
                    step.meta
                        .expect_trino_error
                        .as_ref()
                        .map(|value| value.statement_count()),
                    Some(2)
                );
            }
        }
    }
    Ok(())
}
