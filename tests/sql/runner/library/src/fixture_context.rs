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

//! One admitted invocation clock and collision-free bounded SQL resource names.
mod trino_namespace;
use trino_namespace::TrinoNamespaceAuthor;
pub(crate) use trino_namespace::TrinoNamespaceBoundary;

use crate::source_map::{SourceEdit, SourceMap};
use crate::types::{RunnerConfig, SuiteConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{
    FixturePolicy, FixtureRunContext, RunBudget, RunStorageScope, ScratchIdentity, rejected,
};
use novarocks_parser::ast::{Literal, LiteralKind};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

const CAPACITY: usize = 36 * 36 * 36;
#[derive(Debug, Clone)]
pub(crate) struct RunNames {
    pub run_id: String,
    pub scope: String,
    pub suites: BTreeMap<String, SuiteNames>,
    pub catalogs: crate::catalog_symbol::CatalogSymbols,
    hms_warehouses: BTreeMap<String, String>,
}
#[derive(Debug, Clone)]
pub(crate) struct SuiteNames {
    pub ordinal: usize,
    pub cases: BTreeMap<String, usize>,
}
pub(crate) fn ordinal(value: usize) -> Result<String> {
    ensure!(
        value < CAPACITY,
        "FixtureBudgetRejected: SQL resource ordinal exceeds capacity"
    );
    let digit = |n: usize| char::from(b"0123456789abcdefghijklmnopqrstuvwxyz"[n]);
    Ok(format!(
        "{}{}{}",
        digit(value / 1296),
        digit(value / 36 % 36),
        digit(value % 36)
    ))
}
pub(crate) fn resource(run: &str, kind: char, index: usize, slot: usize) -> Result<String> {
    ensure!(
        slot < 10,
        "FixtureBudgetRejected: SQL resource slot exceeds capacity"
    );
    let result = format!("{run}_{kind}{}{slot}", ordinal(index)?);
    ensure!(
        result.len() <= 63,
        "FixtureBudgetRejected: SQL resource name exceeds 63 bytes"
    );
    Ok(result)
}
fn sql_namespace_declarations(source: &str) -> Result<BTreeSet<String>> {
    // Only explicit, line-start SQL DDL declares SQL namespace write ownership.
    let re = Regex::new(
        r"(?im)^\s*CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[A-Za-z0-9_$\{\}]+`?\s*\.\s*)?`?([A-Za-z0-9_$\{\}]+)`?\s*;",
    )?;
    Ok(re.captures_iter(source).map(|c| c[1].to_string()).collect())
}
// Recognize only the existing versioned Variant producer command, not generic
// shell --namespace arguments, command substitutions or lookalike executables.
fn variant_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/sql/fixtures/iceberg-variant/run\.sh"\s+--mode\s+(unshredded|shredded|bounds|nested|uuid|write-target|empty-int|empty-bigint|empty-region|observe|unchanged)\s+--namespace\s+([A-Za-z0-9_${}]+)\s+--table\s+[A-Za-z0-9_]+\s+--output\s+"\$receipt_dir"(?:\s+>"\$receipt_dir/run\.log")?\s*$"#,
    )?)
}
// These match the complete, versioned producer commands. Read/cleanup modes
// refer to a declaration from the same source; they never mint another scope.
fn hms_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*python3\s+"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/sql/fixtures/iceberg-hms/fixture\.py"\s+(prepare|verify-prepared|before|after|cleanup)\s+([A-Za-z0-9_${}]+)(?:\s+--receipt\s+.+)?\s*$"#,
    )?)
}
fn trino_producer_command() -> Result<Regex> {
    Ok(Regex::new(
        r#"^\s*"\$\{NOVAROCKS_WORKSPACE_ROOT:-\.\}/tests/datasketches-tck/interop/trino/verify_rest_catalog\.sh"\s+(create-parent|verify-current)\s+([A-Za-z0-9_${}]+)\s+[A-Za-z0-9_${}]+(?:\s+id\s+4\s+3\s+1\s+3)?\s*$"#,
    )?)
}
fn shell_line<'a>(line: &'a str, shell: &mut bool) -> Option<&'a str> {
    let trimmed = line.trim_start();
    if trimmed.to_ascii_lowercase().starts_with("-- query ") {
        *shell = false;
    }
    if let Some(command) = trimmed.strip_prefix("shell:") {
        *shell = true;
        Some(command)
    } else if *shell {
        Some(line)
    } else {
        None
    }
}
pub(crate) fn namespace_declarations(source: &str) -> Result<BTreeSet<String>> {
    namespace_declarations_with_sql(source, sql_namespace_declarations(source)?)
}

fn namespace_declarations_with_sql(
    source: &str,
    mut declared: BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let producer = variant_producer_command()?;
    let hms = hms_producer_command()?;
    let trino = trino_producer_command()?;
    let mut shell = false;
    for line in source.lines() {
        if let Some(command) = shell_line(line, &mut shell) {
            if let Some(capture) = producer.captures(command)
                && !matches!(&capture[1], "observe" | "unchanged")
            {
                declared.insert(capture[2].to_string());
            }
            if let Some(capture) = hms.captures(command)
                && &capture[1] == "prepare"
            {
                declared.insert(capture[2].to_string());
                declared.insert(format!("{}_new", &capture[2]));
            }
            if let Some(capture) = trino.captures(command)
                && &capture[1] == "create-parent"
                && !command.trim_end().ends_with(" id 4 3 1 3")
            {
                declared.insert(capture[2].to_string());
            }
        }
    }
    Ok(declared)
}
impl RunNames {
    #[cfg(test)]
    pub fn prepare(
        run: &RunBudget,
        suites: &BTreeMap<String, Vec<std::path::PathBuf>>,
    ) -> Result<Self> {
        Self::prepare_with_case_inputs(run, suites, &BTreeSet::new())
    }
    pub fn prepare_with_case_inputs(
        run: &RunBudget,
        suites: &BTreeMap<String, Vec<std::path::PathBuf>>,
        case_inputs: &BTreeSet<(String, String)>,
    ) -> Result<Self> {
        ensure!(
            suites.len() <= CAPACITY,
            "FixtureBudgetRejected: too many SQL suites"
        );
        let literal_alias =
            Regex::new(r#"(?i)^\s*['"]([^'"]+)['"]\s+(?:AS\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?\s*$"#)?;
        let mut total = 0;
        let mut names = BTreeMap::new();
        for (suite_index, (suite, files)) in suites.iter().enumerate() {
            let mut cases = BTreeMap::new();
            for file in files {
                let case = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .context("invalid SQL case filename")?;
                ensure!(
                    cases.insert(case.to_string(), 0).is_none(),
                    "ambiguous SQL case identity {suite}/{case}"
                );
                let source = std::fs::read_to_string(file)?;
                let mut declared = namespace_declarations(&source)?;
                if case_inputs.contains(&(suite.clone(), case.to_string())) {
                    crate::hms_case_input::add_recipe_names(&source, &mut declared)?;
                }
                if let Some(projection) = metadata_projection(&source) {
                    let columns = metadata_columns(&source);
                    let duplicate_data_column = projection.split(',').any(|part| {
                        literal_alias.captures(part).is_some_and(|c| {
                            declared.contains(&c[1]) && columns.contains(&c[2].to_ascii_lowercase())
                        })
                    });
                    let mixed_union = !columns.is_empty()
                        && source.to_ascii_uppercase().contains("UNION")
                        && declared.iter().any(|name| {
                            source.contains(&format!("'{name}'"))
                                || source.contains(&format!("\"{name}\""))
                        });
                    ensure!(
                        !duplicate_data_column && !mixed_union,
                        "ambiguous SQL namespace metadata/data output in {suite}/{case}"
                    );
                }
                for namespace in &declared {
                    let alias = Regex::new(&format!(
                        r"(?im)\bAS\s+{}(?:\s|[,;)])",
                        regex::escape(namespace)
                    ))?;
                    let implicit_alias = Regex::new(&format!(
                        r"(?im)(?:\b(?:FROM|JOIN)\s+[A-Za-z0-9_`.${{}}]+\s+|\)\s+)(?:AS\s+)?{}(?:\s|[,;)])",
                        regex::escape(namespace)
                    ))?;
                    ensure!(
                        !alias.is_match(&source) && !implicit_alias.is_match(&source),
                        "ambiguous namespace/SQL alias ownership in {suite}/{case}"
                    );
                }
                let extra = declared
                    .iter()
                    .filter(|name| {
                        !name.starts_with("${case_db")
                            && !(name.starts_with("${uuid") && name.ends_with('}'))
                            && !(name.starts_with("${suite_uuid") && name.ends_with('}'))
                    })
                    .count();
                let reserved = 1
                    + (1..9)
                        .filter(|slot| source.contains(&format!("${{case_db_{}}}", slot + 1)))
                        .count();
                ensure!(
                    extra + reserved <= 10,
                    "FixtureBudgetRejected: SQL case {suite}/{case} exceeds namespace slots"
                );
            }
            for index in cases.values_mut() {
                ordinal(total)?;
                *index = total;
                total += 1;
            }
            names.insert(
                suite.clone(),
                SuiteNames {
                    ordinal: suite_index,
                    cases,
                },
            );
        }
        Ok(Self {
            run_id: run.identity().run_id.clone(),
            scope: run.identity().scope_root.clone(),
            suites: names,
            catalogs: Default::default(),
            hms_warehouses: Default::default(),
        })
    }
    /// Second pass: hook roles never participate in case ordinal allocation.
    pub fn seal_catalogs(
        &mut self,
        config: &RunnerConfig,
        sources: &BTreeMap<String, Vec<std::path::PathBuf>>,
        suites: &BTreeMap<String, SuiteConfig>,
    ) -> Result<()> {
        let mut rendered = Vec::new();
        let mut deferred_hms_catalog_uses = Vec::new();
        let machine = config
            .values
            .get("fixture_env_file")
            .map(|env| -> Result<bool> {
                let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
                    Path::new(env).with_file_name("manifest.json"),
                )?)?;
                Ok(manifest["runtime"]["machine"].is_object())
            })
            .transpose()?
            .unwrap_or(false);
        let hms = hms_producer_command()?;
        let input_meta = Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$")?;
        let input_marker = Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$")?;
        for (suite, files) in sources {
            if machine && suites[suite].manifest.case_input.is_some() {
                self.hms_warehouses
                    .insert(suite.clone(), format!("{}/hms", self.scope));
            }
            if machine {
                for file in files {
                    let source = std::fs::read_to_string(file)?;
                    let mut shell = false;
                    if source.lines().any(|line| {
                        shell_line(line, &mut shell)
                            .and_then(|command| hms.captures(command))
                            .is_some_and(|capture| &capture[1] == "prepare")
                    }) {
                        self.hms_warehouses
                            .insert(suite.clone(), format!("{}/hms", self.scope));
                    }
                }
            }

            let mut variables = config.values.clone();
            crate::config::apply_suite_placeholder_defaults(&mut variables, suite);
            self.extend_suite_variables(suite, &mut variables)?;
            variables.insert("suite".into(), suite.clone());
            for path in [&suites[suite].init_sql, &suites[suite].cleanup_sql]
                .into_iter()
                .flatten()
            {
                if path.exists() {
                    rendered.push(render_expected(&std::fs::read_to_string(path)?, &variables));
                }
            }
            for file in files {
                let case = file
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .context("invalid case identity")?;
                let variables = crate::config::case_placeholder_variables(&variables, case)?;
                if suites[suite].manifest.case_input.is_some() {
                    ensure!(
                        suite == crate::hms_case_input::SUITE
                            && case == crate::hms_case_input::CASE,
                        "HMS declaration projection escaped its closed unit"
                    );
                    let definition =
                        crate::hms_case_input::Definition::load(file, &input_meta, &input_marker)?;
                    let projection = definition.catalog_definition_projection()?;
                    deferred_hms_catalog_uses.push(projection.referenced_catalog(&variables)?);
                    rendered.push(render_expected(projection.source(), &variables));
                } else {
                    rendered.push(render_expected(&std::fs::read_to_string(file)?, &variables));
                }
            }
        }
        self.catalogs = crate::catalog_symbol::CatalogSymbols::seal(&self.run_id, &rendered)?;
        for catalog in deferred_hms_catalog_uses {
            ensure!(
                self.catalogs.lookup(&catalog).is_some(),
                "HMS deferred q12 catalog must be owned by its actual q2 declaration"
            );
        }
        Ok(())
    }
    pub fn extend_suite_variables(
        &self,
        suite: &str,
        variables: &mut HashMap<String, String>,
    ) -> Result<()> {
        let names = self
            .suites
            .get(suite)
            .context("SQL suite omitted its frozen naming map")?;
        variables.insert("run_id".into(), self.run_id.clone());
        variables.insert("suite_run_id".into(), self.run_id.clone());
        variables.insert("__scratch_scope".into(), self.scope.clone());
        if let Some(warehouse) = self.hms_warehouses.get(suite) {
            variables.insert("iceberg_hms_warehouse".into(), warehouse.clone());
        }
        variables.insert("__suite_ordinal".into(), names.ordinal.to_string());
        for (case, ordinal) in &names.cases {
            variables.insert(format!("__case_ordinal.{case}"), ordinal.to_string());
        }
        self.catalogs.extend_variables(variables)?;
        for slot in 0..10 {
            let name = resource(&self.run_id, 's', names.ordinal, slot)?;
            variables.insert(format!("suite_uuid{slot}"), name.clone());
            variables.insert(format!("uuid{slot}"), name);
        }
        Ok(())
    }
}

/// Admission precedes private Docker, prepared-dataset commands and SQL writes.
/// A READY publication contributes policy only; its benchmark warehouse stays read-only.
pub(crate) fn admit(config: &RunnerConfig, root: &Path, seconds: u64) -> Result<RunBudget> {
    (|| -> Result<_> {
        let machine = config
            .values
            .get("fixture_env_file")
            .map(|env| {
                let manifest = Path::new(env)
                    .parent()
                    .context("fixture publication directory")?
                    .join("manifest.json");
                let value: serde_json::Value = serde_json::from_slice(&std::fs::read(manifest)?)?;
                let machine = value["runtime"]["machine"].clone();
                if machine.is_object() {
                    ensure!(
                        value["ready"] == true && machine["phase"] == "ready",
                        "shared fixture publication is not ready"
                    );
                    Ok(Some(machine))
                } else {
                    Ok(None)
                }
            })
            .transpose()?
            .flatten();
        let policy = match &machine {
            Some(value) => serde_json::from_value(value["policy"].clone())?,
            None => FixturePolicy::from_environment()?,
        };
        let inherited = std::env::var(novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV)
            .ok()
            .map(|v| serde_json::from_str::<FixtureRunContext>(&v))
            .transpose()?;
        let identity = match &inherited {
            Some(value) => value.identity.clone(),
            None => {
                let fallback = format!(
                    "s3://novarocks/sql-{:x}",
                    Sha256::digest(root.to_string_lossy().as_bytes())
                );
                let warehouse = std::env::var("CATALOG_WAREHOUSE_URI")
                    .ok()
                    .filter(|v| v.starts_with("s3://") && !v.contains("${"))
                    .or_else(|| {
                        config
                            .values
                            .get("iceberg_catalog_warehouse")
                            .filter(|v| v.starts_with("s3://") && !v.contains("${"))
                            .cloned()
                    })
                    .unwrap_or(fallback);
                ScratchIdentity::create(&warehouse)?
            }
        };
        let budget = RunBudget::admit(policy, identity, seconds, inherited)?;
        if let Some(machine) = machine {
            budget.validate_machine(&machine)?;
        }
        Ok(budget)
    })()
    .map_err(rejected)
}

/// Capture the original shared publication before private endpoints can replace
/// runner configuration. This is a storage projection, never another run clock.
pub(crate) fn storage_scope(
    config: &RunnerConfig,
    budget: &RunBudget,
) -> Result<Option<novarocks_cluster_harness::fixture_run::RunStorageScope>> {
    let Some(env_file) = config.values.get("fixture_env_file") else {
        return Ok(None);
    };
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
        Path::new(env_file).with_file_name("manifest.json"),
    )?)?;
    if manifest["shared_docker"] != true {
        return Ok(None);
    }
    novarocks_cluster_harness::fixture_run::RunStorageScope::from_publication(
        Path::new(env_file),
        budget,
    )
    .map(Some)
}

/// Explicit writer-start maintenance, after the complete naming preflight.
/// Read-only prepared-dataset admission never calls this helper.
pub(crate) fn startup_sweep(config: &RunnerConfig, repo: &Path, budget: &RunBudget) -> Result<()> {
    startup_sweep_in_scope(config, repo, budget, None)
}

fn startup_sweep_in_scope(
    config: &RunnerConfig,
    repo: &Path,
    budget: &RunBudget,
    storage: Option<&RunStorageScope>,
) -> Result<()> {
    let Some(env_file) = config.values.get("fixture_env_file") else {
        return Ok(());
    };
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(
        Path::new(env_file).with_file_name("manifest.json"),
    )?)?;
    if !manifest["runtime"]["machine"].is_object() {
        return Ok(());
    }
    budget.check_work()?;
    if let Some(storage) = storage {
        novarocks_cluster_harness::fixture_run::startup_sweep_with_storage(
            repo,
            Path::new(env_file),
            budget,
            storage,
        )?;
    } else {
        novarocks_cluster_harness::fixture_run::startup_sweep(repo, Path::new(env_file), budget)?;
    }
    budget.check_work()
}

/// The invocation owner calls this only after every writer has actually exited.
pub(crate) fn cleanup_run(
    repo: &Path,
    env_file: &Path,
    budget: &RunBudget,
    storage: &RunStorageScope,
) -> Result<serde_json::Value> {
    ensure!(
        std::time::Instant::now() < budget.deadline(),
        "original fixture cleanup deadline expired"
    );
    ensure!(
        storage.identity() == budget.identity(),
        "cleanup storage differs from original run"
    );
    let mut command = novarocks_test_support::ManagedCommand::new("python3");
    command
        .env_clear()
        .envs(std::env::vars_os().filter(|(key, _)| {
            matches!(
                key.to_str(),
                Some(
                    "PATH"
                        | "HOME"
                        | "TMPDIR"
                        | "LANG"
                        | "LC_ALL"
                        | "SSL_CERT_FILE"
                        | "DOCKER_CONTEXT"
                        | "DOCKER_HOST"
                        | "DOCKER_CONFIG"
                        | "DOCKER_TLS_VERIFY"
                        | "DOCKER_CERT_PATH"
                        | "NOVA_FIXTURE_STORE"
                        | "XDG_STATE_HOME"
                        | "XDG_CONFIG_HOME"
                )
            )
        }))
        .current_dir(repo)
        .arg(repo.join("docker/iceberg-rest/fixture_run.py"))
        .arg("cleanup-run")
        .arg("--env-file")
        .arg(env_file)
        .env(
            novarocks_cluster_harness::fixture_run::RUN_CONTEXT_ENV,
            budget.to_json()?,
        )
        .env(
            novarocks_cluster_harness::fixture_run::RUN_STORAGE_ENV,
            storage.to_json()?,
        );
    let outcome = novarocks_test_support::capture_command_until(
        command,
        tempfile::tempfile()?,
        tempfile::tempfile()?,
        budget.deadline(),
    )?;
    ensure!(
        !outcome.timed_out && outcome.failures.is_empty() && outcome.output.status.success(),
        "fixture run cleanup failed with {}; stdout={}; stderr={}; diagnostics retained at {}",
        outcome.output.status,
        String::from_utf8_lossy(&outcome.output.stdout),
        String::from_utf8_lossy(&outcome.output.stderr),
        outcome.diagnostic_root().display()
    );
    let report: serde_json::Value =
        serde_json::from_slice(&outcome.output.stdout).with_context(|| {
            format!(
                "invalid fixture cleanup report; diagnostics retained at {}",
                outcome.diagnostic_root().display()
            )
        })?;
    ensure!(
        report["complete"] == true,
        "fixture cleanup incomplete; report={report}; diagnostics retained at {}",
        outcome.diagnostic_root().display()
    );
    outcome.release_diagnostics()?;
    Ok(report)
}

pub(crate) fn selected_sources(
    cli: &crate::Cli,
    root: &Path,
    selected: &[String],
    suites: &BTreeMap<String, SuiteConfig>,
) -> Result<BTreeMap<String, Vec<std::path::PathBuf>>> {
    let mut result = BTreeMap::new();
    for name in selected {
        let suite = &suites[name];
        let directory = if selected.len() == 1 {
            crate::config::resolve_path(cli.sql_dir.as_deref(), root)
                .unwrap_or_else(|| suite.sql_dir.clone())
        } else {
            suite.sql_dir.clone()
        };
        let glob = if selected.len() == 1 {
            cli.sql_glob.as_deref().unwrap_or(&suite.sql_glob)
        } else {
            &suite.sql_glob
        };
        result.insert(
            name.clone(),
            crate::config::list_sql_files(&directory, glob)?,
        );
    }
    Ok(result)
}

/// Replace owned namespace identifiers, control metadata and shell references.
/// Ordinary SQL string values, column names and catalog prefixes are preserved.
#[cfg(test)]
pub(crate) fn rewrite_namespaces(
    source: &str,
    variables: &HashMap<String, String>,
) -> Result<String> {
    prepare_trino_namespaces(source, variables).map(|(sql, _)| sql)
}

#[derive(Default)]
pub(crate) struct NamespaceTrace {
    pub(crate) identifiers: Option<SourceMap>,
    pub(crate) delta: Option<SourceMap>,
    pub(crate) hms: Option<SourceMap>,
}

pub(crate) fn prepare_trino_namespaces(
    source: &str,
    variables: &HashMap<String, String>,
) -> Result<(String, BTreeMap<String, String>)> {
    prepare_namespaces(source, variables, None)
}

#[cfg(test)]
pub(crate) fn prepare_trino_namespaces_observed(
    source: &str,
    variables: &HashMap<String, String>,
    trace: &mut NamespaceTrace,
) -> Result<(String, BTreeMap<String, String>)> {
    prepare_namespaces(source, variables, Some(trace))
}

fn prepare_namespaces(
    source: &str,
    variables: &HashMap<String, String>,
    trace: Option<&mut NamespaceTrace>,
) -> Result<(String, BTreeMap<String, String>)> {
    let hms_ready = variables.contains_key("__hms_case_input_kind");
    if hms_ready {
        ensure!(
            variables
                .get("__hms_case_input_kind")
                .is_some_and(|kind| kind == crate::hms_case_input::KIND)
                && variables
                    .get("case_id")
                    .is_some_and(|case| case == crate::hms_case_input::CASE),
            TrinoNamespaceBoundary::HmsUri
        );
    }
    let author = TrinoNamespaceAuthor::inspect(source)?;
    let mapping = namespace_mapping_from(source, variables, || {
        namespace_declarations_with_sql(source, author.declared.clone())
    })?;
    ensure!(
        !hms_ready || !mapping.is_empty(),
        TrinoNamespaceBoundary::HmsUri
    );
    author.require_supported(&mapping)?;
    let rewritten = rewrite_namespaces_with_owner(source, variables, &mapping, &author, trace)?;
    Ok((rewritten, mapping))
}

fn rewrite_namespaces_with_owner(
    source: &str,
    variables: &HashMap<String, String>,
    mapping: &BTreeMap<String, String>,
    author: &TrinoNamespaceAuthor,
    mut trace: Option<&mut NamespaceTrace>,
) -> Result<String> {
    if mapping.is_empty() {
        if let Some(trace) = trace {
            trace.identifiers = SourceMap::verify(source, source, Vec::new());
            trace.delta = SourceMap::verify(source, source, Vec::new());
            trace.hms = SourceMap::verify(source, source, Vec::new());
        }
        return Ok(source.to_string());
    }
    let sql_declared = &author.declared;
    let sql_mapping = mapping
        .iter()
        .filter(|(name, _)| sql_declared.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    let producer = variant_producer_command()?;
    let hms = hms_producer_command()?;
    let trino_producer = trino_producer_command()?;
    let mut result = String::new();
    let has_mapped_delta = author.has_mapped_delta(mapping);
    let mut identifier_edits = (trace.is_some() || has_mapped_delta).then(Vec::new);
    let mut sql = String::new();
    let mut sql_start = 0;
    let mut source_offset = 0;
    let mut shell = false;
    for line in source.split_inclusive('\n') {
        let line_start = source_offset;
        source_offset += line.len();
        let trimmed = line.trim_start();
        if trimmed.to_ascii_lowercase().starts_with("-- query ") {
            shell = false;
        }
        if trimmed.starts_with("shell:") {
            shell = true;
        }
        if shell || trimmed.starts_with("-- @") {
            result.push_str(&author.rewrite_fragment(
                &sql,
                mapping,
                sql_start,
                identifier_edits.as_mut(),
            )?);
            sql.clear();
            sql_start = source_offset;
            let rewritten = if trimmed.starts_with("-- @") {
                replace_references(line, mapping)
            } else {
                // Producer-only ownership changes its namespace argument, never
                // unrelated shell values or arbitrary commands in the block.
                let mut rewritten = replace_references(line, &sql_mapping);
                let original_command = line.trim_start().strip_prefix("shell:").unwrap_or(line);
                if let Some(capture) = hms.captures(original_command) {
                    if let (Some(base), Some(sibling)) = (
                        mapping.get(&capture[2]),
                        mapping.get(&format!("{}_new", &capture[2])),
                    ) {
                        let argument = capture.get(2).unwrap();
                        let offset = line.len() - original_command.len();
                        let mut exact = line.to_string();
                        exact.replace_range(
                            offset + argument.start()..offset + argument.end(),
                            base,
                        );
                        let newline = if exact.ends_with('\n') { "\n" } else { "" };
                        rewritten =
                            format!("{} --new-namespace {sibling}{newline}", exact.trim_end());
                    }
                } else if let Some(capture) = trino_producer.captures(original_command) {
                    let read = &capture[1] == "verify-current";
                    ensure!(
                        read == original_command.trim_end().ends_with(" id 4 3 1 3"),
                        "invalid versioned Trino producer argument shape"
                    );
                    if let Some(base) = mapping.get(&capture[2]) {
                        let argument = capture.get(2).unwrap();
                        let offset = line.len() - original_command.len();
                        rewritten = line.to_string();
                        rewritten.replace_range(
                            offset + argument.start()..offset + argument.end(),
                            base,
                        );
                    }
                }
                let command = rewritten
                    .trim_start()
                    .strip_prefix("shell:")
                    .unwrap_or(&rewritten);
                if let Some(capture) = producer.captures(command)
                    && let Some(name) = mapping.get(&capture[2])
                {
                    let argument = capture.get(2).unwrap();
                    let offset = rewritten.len() - command.len();
                    let range = offset + argument.start()..offset + argument.end();
                    let name = name.clone();
                    rewritten.replace_range(range, &name);
                }
                rewritten
            };
            // These are complete metadata or shell lines. A SQL Query that
            // touches a replaced line cannot take its endpoints from it.
            SourceEdit::record(
                identifier_edits.as_mut(),
                line_start..source_offset,
                line,
                &rewritten,
            );
            result.push_str(&rewritten);
        } else {
            sql.push_str(line);
        }
    }
    result.push_str(&author.rewrite_fragment(
        &sql,
        mapping,
        sql_start,
        identifier_edits.as_mut(),
    )?);
    let identifier_map =
        identifier_edits.and_then(|edits| SourceMap::verify(source, &result, edits));
    let mut delta_edits = trace.as_ref().map(|_| Vec::new());
    let delta = if has_mapped_delta {
        Some(
            author.rewrite_delta(
                &result,
                mapping,
                identifier_map
                    .as_ref()
                    .ok_or(TrinoNamespaceBoundary::Delta)?,
                delta_edits.as_mut(),
            )?,
        )
    } else {
        None
    };
    if let Some(trace) = trace.as_deref_mut() {
        trace.identifiers = identifier_map;
        trace.delta = SourceMap::verify(
            &result,
            delta.as_deref().unwrap_or(&result),
            delta_edits.unwrap_or_default(),
        );
    }
    if let Some(delta) = delta {
        result = delta;
    }
    let mut hms_input = None;
    let mut hms_edits = Vec::new();

    let mut shell = false;
    let mut families: Vec<_> = source
        .lines()
        .filter_map(|line| {
            shell_line(line, &mut shell)
                .and_then(|command| hms.captures(command))
                .filter(|capture| &capture[1] == "prepare")
                .map(|capture| capture[2].to_string())
        })
        .collect();
    // The fixed manifest recipe declares this family. A verifier command by
    // itself is still read-only and never grants producer ownership.
    if variables
        .get("__hms_case_input_kind")
        .is_some_and(|kind| kind == crate::hms_case_input::KIND)
    {
        ensure!(
            variables
                .get("case_id")
                .is_some_and(|case| case == crate::hms_case_input::CASE),
            "HMS URI family recipe escaped its closed case"
        );
        let original_family = format!(
            "iru7_{}",
            variables
                .get("uuid0")
                .context("HMS URI family omitted original UUID")?
        );
        ensure!(
            mapping.contains_key(&original_family),
            "HMS URI family has no original namespace mapping"
        );
        if !families.contains(&original_family) {
            families.push(original_family);
        }
    }
    if !families.is_empty() {
        ensure!(
            variables
                .get("__hms_case_input_kind")
                .is_some_and(|kind| kind == crate::hms_case_input::KIND)
                && variables
                    .get("case_id")
                    .is_some_and(|name| name == crate::hms_case_input::CASE)
                && families.len() == 1,
            TrinoNamespaceBoundary::HmsUri
        );
        let warehouse = variables
            .get("iceberg_hms_warehouse")
            .ok_or(TrinoNamespaceBoundary::HmsUri)?;
        let original = &families[0];
        let mapped = mapping
            .get(original)
            .ok_or(TrinoNamespaceBoundary::HmsUri)?;
        let uuid = variables
            .get("uuid0")
            .ok_or(TrinoNamespaceBoundary::HmsUri)?;
        let edits = [crate::hms_case_input::bind_trino_q40_uri(
            &result,
            warehouse,
            &format!("ice_hms_{uuid}"),
            original,
            mapped,
        )
        .context(TrinoNamespaceBoundary::HmsUri)?];
        if trace.is_some() && !edits.is_empty() {
            hms_input = Some(result.clone());
            hms_edits.extend(edits.iter().map(|(start, end, value)| SourceEdit {
                input: *start..*end,
                output_len: value.len(),
            }));
        }
        for (start, end, value) in edits.into_iter().rev() {
            result.replace_range(start..end, &value);
        }
    }
    if let Some(trace) = trace {
        trace.hms = SourceMap::verify(hms_input.as_deref().unwrap_or(&result), &result, hms_edits);
    }
    Ok(result)
}
// The internal delta TVF's first argument is a table identity. It is not an
// ordinary SQL string; only its namespace component consumes this mapping.
fn delta_identity_parts(literal: &Literal) -> Option<[&str; 3]> {
    let LiteralKind::String(value) = &literal.kind else {
        return None;
    };
    let mut parts = value.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(catalog), Some(namespace), Some(table), None) => Some([catalog, namespace, table]),
        _ => None,
    }
}

fn render_delta_identity(catalog: &str, namespace: &str, table: &str) -> String {
    format!("'{catalog}.{namespace}.{table}'")
}

#[cfg(test)]
fn rewrite_delta_namespace_refs(
    source: &str,
    mapping: &BTreeMap<String, String>,
) -> Result<String> {
    let owner = TrinoNamespaceAuthor::inspect(source)?;
    owner.require_supported(mapping)?;
    let map = SourceMap::verify(source, source, Vec::new()).expect("unchanged source");
    owner.rewrite_delta(source, mapping, &map, None)
}
pub(crate) fn namespace_mapping(
    source: &str,
    variables: &HashMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let author = TrinoNamespaceAuthor::inspect(source)?;
    namespace_mapping_from(source, variables, || {
        namespace_declarations_with_sql(source, author.declared)
    })
}

fn namespace_mapping_from(
    source: &str,
    variables: &HashMap<String, String>,
    declare: impl FnOnce() -> Result<BTreeSet<String>>,
) -> Result<BTreeMap<String, String>> {
    let Some(run) = variables
        .get("suite_run_id")
        .filter(|_| variables.contains_key("__scratch_scope"))
    else {
        return Ok(BTreeMap::new());
    };
    let Some(case) = variables.get("case_id") else {
        return Ok(BTreeMap::new());
    };
    let index = variables
        .get(&format!("__case_ordinal.{case}"))
        .context("missing SQL ordinal")?
        .parse::<usize>()?;
    let mut used = BTreeSet::from([0usize]);
    for slot in 0..10 {
        let key = if slot == 0 {
            "case_db".into()
        } else {
            format!("case_db_{}", slot + 1)
        };
        if variables
            .get(&key)
            .is_some_and(|name| source.contains(name))
        {
            used.insert(slot);
        }
    }
    let mut mapping = BTreeMap::new();
    let mut declared = declare()?;
    if variables
        .get("__hms_case_input_kind")
        .is_some_and(|v| v == crate::hms_case_input::KIND)
    {
        ensure!(
            case == crate::hms_case_input::CASE,
            "HMS input namespace recipe escaped its case"
        );
        crate::hms_case_input::add_recipe_names(source, &mut declared)?;
    }
    if let Some(bound) = crate::hms_case_input::bound_namespace_mapping(variables)? {
        ensure!(
            declared == bound.keys().cloned().collect::<BTreeSet<_>>(),
            "HMS bound namespace declarations differ from original registration"
        );
        return Ok(bound);
    }
    for name in declared {
        if name.starts_with(&format!("{run}_")) {
            ensure!(name.len() <= 63, "owned SQL namespace exceeds 63 bytes");
            continue;
        }
        let slot = (0..10)
            .find(|slot| !used.contains(slot))
            .context("SQL namespace slots exhausted")?;
        used.insert(slot);
        mapping.insert(name, resource(run, 'd', index, slot)?);
    }
    Ok(mapping)
}
pub(crate) fn render_expected(value: &str, variables: &HashMap<String, String>) -> String {
    let mut rendered = value.to_string();
    for (name, value) in variables {
        rendered = rendered.replace(&format!("${{{name}}}"), value);
    }
    rendered
}
fn metadata_projection(sql: &str) -> Option<String> {
    let re = Regex::new(r"(?is)\bSELECT\s+(.*?)\s+FROM\s+(?:(?:[A-Za-z0-9_`.$\{\}]+\.)?information_schema\.(?:schemata|tables|columns|task_runs)|system\.metadata\.materialized_views|system\.runtime\.materialized_view_states)\b").ok()?;
    re.captures(sql).map(|c| c[1].to_string())
}
fn metadata_columns(sql: &str) -> BTreeSet<String> {
    let Some(projection) = metadata_projection(sql) else {
        return BTreeSet::new();
    };
    let re = Regex::new(r"(?i)^\s*(?:[A-Za-z_][A-Za-z0-9_]*\.)?`?(schema_name|table_schema|storage_schema|database|db_name)`?(?:\s+(?:AS\s+)?`?([A-Za-z_][A-Za-z0-9_]*)`?)?\s*$").unwrap();
    projection
        .split(',')
        .filter_map(|part| {
            re.captures(part).map(|c| {
                c.get(2).map_or_else(
                    || c[1].to_ascii_lowercase(),
                    |a| a.as_str().to_ascii_lowercase(),
                )
            })
        })
        .collect()
}
pub(crate) fn rewrite_metadata_filters(sql: &str, mapping: &BTreeMap<String, String>) -> String {
    rewrite_metadata_filters_observed(sql, mapping, None)
}

pub(crate) fn rewrite_metadata_filters_observed(
    sql: &str,
    mapping: &BTreeMap<String, String>,
    mut edits: Option<&mut Vec<SourceEdit>>,
) -> String {
    if metadata_projection(sql).is_none() {
        return sql.to_string();
    }
    let re = Regex::new(
        r#"(?i)(\b(?:table_schema|schema_name|storage_schema|database|db_name)`?\s*=\s*)(['"])([^'"]+)(['"])"#,
    )
    .unwrap();
    re.replace_all(sql, |c: &regex::Captures<'_>| {
        let value = mapping.get(&c[3]).map_or(&c[3], String::as_str);
        if let Some(edits) = edits.as_deref_mut() {
            let captured = c
                .get(3)
                .expect("metadata filter regex always captures a value");
            SourceEdit::record(Some(edits), captured.range(), captured.as_str(), value);
        }
        format!("{}{}{}{}", &c[1], &c[2], value, &c[4])
    })
    .into_owned()
}
/// Typed files have already expanded placeholders before canonical validation.
/// Legacy binding keeps its old flattened owner. Typed metadata waits for the
/// actual statement/result pairing; the whole step cannot identify its author.
pub(crate) fn render_expected_result(
    sql: &str,
    result: &mut crate::types::ResultSet,
    variables: &HashMap<String, String>,
    mapping: &BTreeMap<String, String>,
) -> Result<()> {
    use crate::golden::Block;
    let Some(mut golden) = result.golden.clone() else {
        return render_expected_flat(sql, result, variables, mapping, true);
    };
    for block in &mut golden.expanded.blocks {
        match block {
            Block::Legacy { names, rows } => {
                let mut flat = crate::types::ResultSet {
                    golden: None,
                    header: names
                        .iter()
                        .map(|cell| crate::results::unescape_cell(cell))
                        .collect(),
                    rows: rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|cell| crate::results::unescape_cell(cell))
                                .collect()
                        })
                        .collect(),
                };
                render_expected_flat(sql, &mut flat, variables, mapping, false)?;
                *names = flat
                    .header
                    .iter()
                    .map(|cell| crate::results::escape_cell(cell))
                    .collect();
                *rows = flat
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|cell| crate::results::escape_cell(cell))
                            .collect()
                    })
                    .collect();
            }
            Block::Typed { .. } | Block::Shell { .. } => {}
        }
    }
    commit_expanded_golden(result, golden)
}

/// Bind typed metadata using the exact submitted statement paired with each
/// row-bearing result. Legacy blocks have already followed their old owner.
pub(crate) fn render_expected_for_execution(
    _sql: &str,
    result: &mut crate::types::ResultSet,
    variables: &HashMap<String, String>,
    mapping: &BTreeMap<String, String>,
    statements: &[String],
) -> Result<()> {
    use crate::golden::{Block, parse_cell, parse_type, render_cell};
    use novarocks_test_trino_client::TrinoValue;
    let Some(mut golden) = result.golden.clone() else {
        return Ok(());
    };
    if !golden
        .expanded
        .blocks
        .iter()
        .any(|block| matches!(block, Block::Typed { .. }))
    {
        return Ok(());
    }
    anyhow::ensure!(
        golden.expanded.blocks.len() == statements.len(),
        "ambiguous typed metadata binding: {} expected blocks differ from {} actual result statements",
        golden.expanded.blocks.len(),
        statements.len()
    );
    for (block, statement) in golden.expanded.blocks.iter_mut().zip(statements) {
        let Block::Typed { types, names, rows } = block else {
            continue;
        };
        let signatures = types
            .iter()
            .map(|ty| parse_type(ty))
            .collect::<Result<Vec<_>>>()?;
        let mut contents = rows.clone();
        for row in &mut contents {
            for (cell, sig) in row.iter_mut().zip(&signatures) {
                if let TrinoValue::Text(value) = parse_cell(sig, cell)? {
                    *cell = value;
                }
            }
        }
        let mut flat = crate::types::ResultSet {
            golden: None,
            header: names.clone(),
            rows: contents,
        };
        render_expected_flat(statement, &mut flat, variables, mapping, false)?;
        for (original, content) in rows.iter_mut().zip(flat.rows) {
            for ((cell, text), sig) in original.iter_mut().zip(content).zip(&signatures) {
                // The flat sentinel "NULL" cannot establish string ownership.
                // Only an original text value may accept a metadata rewrite.
                match parse_cell(sig, cell)? {
                    TrinoValue::Text(_) => {
                        *cell = render_cell(sig, &TrinoValue::Text(text))?;
                    }
                    TrinoValue::Array(_) if *cell != text => {
                        // Only the statement-owned metadata array positions were
                        // changed by the catalog adapter; validate the original type.
                        *cell = render_cell(sig, &parse_cell(sig, &text)?)?;
                    }
                    _ => {}
                }
            }
        }
        *names = flat.header;
    }
    commit_expanded_golden(result, golden)
}

fn commit_expanded_golden(
    result: &mut crate::types::ResultSet,
    golden: crate::types::GoldenSection,
) -> Result<()> {
    crate::golden::GoldenFile {
        sections: vec![golden.expanded.clone()],
    }
    .render()?;
    let projected = crate::results::project_golden_blocks(&golden.expanded.blocks)?;
    result.header = projected.header;
    result.rows = projected.rows;
    result.golden = Some(golden);
    Ok(())
}

fn render_expected_flat(
    sql: &str,
    result: &mut crate::types::ResultSet,
    variables: &HashMap<String, String>,
    mapping: &BTreeMap<String, String>,
    expand: bool,
) -> Result<()> {
    if expand {
        for cell in result
            .header
            .iter_mut()
            .chain(result.rows.iter_mut().flatten())
        {
            *cell = render_expected(cell, variables);
        }
    }
    let columns = metadata_columns(sql);
    let upper = sql.trim().to_ascii_uppercase();
    let show_databases = upper.starts_with("SHOW SCHEMAS");
    let current_database = Regex::new(r"(?i)^\s*SELECT\s+CURRENT_SCHEMA\s*;?\s*$")
        .unwrap()
        .is_match(sql);
    let show_create =
        upper.starts_with("SHOW CREATE TABLE") || upper.starts_with("SHOW CREATE VIEW");
    for (index, header) in result.header.iter().enumerate() {
        let namespace = columns.contains(&header.to_ascii_lowercase())
            || (show_databases && header.eq_ignore_ascii_case("schema"))
            || (current_database && result.header.len() == 1);
        let ddl = show_create
            && (header.eq_ignore_ascii_case("create table")
                || header.eq_ignore_ascii_case("create view"));
        for row in &mut result.rows {
            if let Some(cell) = row.get_mut(index) {
                if namespace {
                    if let Some(name) = mapping.get(cell) {
                        *cell = name.clone();
                    }
                } else if ddl {
                    *cell = rewrite_displayed_ddl(cell, mapping)?;
                }
            }
        }
    }
    crate::catalog_symbol::CatalogSymbols::from_variables(variables)?
        .render_expected(sql, result)?;
    Ok(())
}
fn replace_references(text: &str, mapping: &BTreeMap<String, String>) -> String {
    let mut text = text.to_string();
    for (old, new) in mapping {
        let mut next = String::new();
        let mut offset = 0;
        for (start, _) in text.match_indices(old) {
            let end = start + old.len();
            let boundary = |c: u8| !c.is_ascii_alphanumeric() && c != b'_';
            if (start == 0 || boundary(text.as_bytes()[start - 1]))
                && (end == text.len() || boundary(text.as_bytes()[end]))
            {
                next.push_str(&text[offset..start]);
                next.push_str(new);
                offset = end;
            }
        }
        next.push_str(&text[offset..]);
        text = next;
    }
    text
}
fn rewrite_displayed_ddl(sql: &str, mapping: &BTreeMap<String, String>) -> Result<String> {
    if mapping.is_empty() {
        return Ok(sql.into());
    }
    let author = TrinoNamespaceAuthor::inspect(sql)?;
    author.require_supported(mapping)?;
    author.rewrite_fragment(sql, mapping, 0, None)
}

#[cfg(test)]
fn rewrite_sql_identifiers(sql: &str, mapping: &BTreeMap<String, String>) -> String {
    rewrite_displayed_ddl(sql, mapping).unwrap()
}

#[cfg(test)]
pub(crate) fn test_names(suite: &str, cases: &[&str]) -> RunNames {
    RunNames {
        catalogs: Default::default(),
        hms_warehouses: Default::default(),
        run_id: "nr_scratch_1792540800000_0123456789abcdef0123456789abcdef".into(),
        scope:
            "s3://novarocks/test/scratch/nr_scratch_1792540800000_0123456789abcdef0123456789abcdef"
                .into(),
        suites: BTreeMap::from([(
            suite.into(),
            SuiteNames {
                ordinal: 0,
                cases: cases
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.to_string(), i))
                    .collect(),
            },
        )]),
    }
}
#[cfg(test)]
pub(crate) fn test_variables(cases: &[&str]) -> HashMap<String, String> {
    let mut variables = HashMap::new();
    test_names("test", cases)
        .extend_suite_variables("test", &mut variables)
        .unwrap();
    variables
}

#[cfg(test)]
mod tests;
