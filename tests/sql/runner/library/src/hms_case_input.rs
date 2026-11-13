// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One explicit fixture-owned input. Facts are cloneable; execution custody is not.
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{
    RUN_CONTEXT_ENV, RUN_STORAGE_ENV, RunBudget, RunStorageScope,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub(crate) const SUITE: &str = "iceberg-hms";
pub(crate) const CASE: &str = "iceberg_hms_read_only_compatibility";
pub(crate) const KIND: &str = "hms-read-only-compatibility-v1";
const PRODUCER: &str = "tests/sql/fixtures/iceberg-hms/fixture.py";
const REMOTE: &str = "docker/iceberg-rest/fixture_run.py";
const ID: &str = "hms_first_snapshot_id";
const RECEIPT: &str = "hms_prepared_input_receipt";
const BOUND_NAMESPACE: &str = "__hms_input_namespace";
const BOUND_SIBLING: &str = "__hms_input_new_namespace";

/// The original registration owns this pair. Receipt data cannot allocate names.
pub(crate) fn bound_namespace_mapping(
    variables: &HashMap<String, String>,
) -> Result<Option<BTreeMap<String, String>>> {
    let primary = variables.get(BOUND_NAMESPACE);
    let sibling = variables.get(BOUND_SIBLING);
    if primary.is_none() && sibling.is_none() {
        return Ok(None);
    }
    ensure!(
        variables
            .get("__hms_case_input_kind")
            .is_some_and(|v| v == KIND)
            && variables.get("case_id").is_some_and(|v| v == CASE),
        "HMS bound namespace pair escaped its closed recipe"
    );
    let primary = primary.context("HMS original primary namespace absent")?;
    let sibling = sibling.context("HMS original sibling namespace absent")?;
    let run = variables
        .get("suite_run_id")
        .context("HMS original run absent")?;
    ensure!(
        primary != sibling
            && [primary, sibling].iter().all(|name| {
                name.starts_with(&format!("{run}_d"))
                    && name.len() <= 63
                    && name
                        .bytes()
                        .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_')
            }),
        "HMS bound namespace pair differs from its original run"
    );
    let family = format!(
        "iru7_{}",
        variables.get("uuid0").context("HMS original UUID absent")?
    );
    Ok(Some(BTreeMap::from([
        (family.clone(), primary.clone()),
        (format!("{family}_new"), sibling.clone()),
    ])))
}

fn empty_object_inventory(value: &Value) -> bool {
    value.as_object().is_some_and(|objects| objects.is_empty())
}

/// A syntax/identity binding only, never a successful SQL execution or Ready fact.
struct CaseBinding {
    case: crate::types::SqlCase,
    snapshot: i64,
    q40_uri: String,
    warehouse: String,
    v1: String,
    v2: String,
}

/// This is intentionally not SqlCase and cannot be passed to control_plan/run_case.
pub(crate) struct Definition {
    path: PathBuf,
    source_sha256: String,
}
/// A non-executable view for FE catalog ownership only. It cannot become a SqlCase.
pub(crate) struct CatalogDefinitionProjection {
    source: String,
    catalog_template: &'static str,
}
impl CatalogDefinitionProjection {
    pub(crate) fn source(&self) -> &str {
        &self.source
    }
    pub(crate) fn referenced_catalog(&self, variables: &HashMap<String, String>) -> Result<String> {
        crate::config::substitute_placeholders(
            self.catalog_template,
            variables,
            "HMS deferred numeric VERSION catalog reference",
        )
    }
}

impl Definition {
    pub(crate) fn case_id(&self) -> &str {
        CASE
    }
    pub(crate) fn catalog_definition_projection(&self) -> Result<CatalogDefinitionProjection> {
        ensure!(
            crate::results_record::hash_file(&self.path)? == self.source_sha256,
            "HMS definition changed before catalog projection"
        );
        let source = fs::read_to_string(&self.path)?;
        // Definition::load has already admitted this exact closed q12 shape.
        // Only its runtime selector is deferred. Catalog/table/column/order uses
        // remain in the symbol-only view; the template is never changed.
        let numeric = "SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 FOR VERSION AS OF ${hms_first_snapshot_id} ORDER BY id;";
        ensure!(
            source.matches(numeric).count() == 1,
            "HMS deferred VERSION use is not exact"
        );
        Ok(CatalogDefinitionProjection {
            source: source.replacen(
                numeric,
                "SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 ORDER BY id;",
                1,
            ),
            catalog_template: "ice_hms_${uuid0}",
        })
    }
    fn bind_ready_case(
        &self,
        variables: &HashMap<String, String>,
        namespaces: (&str, &str),
        ready: &Value,
        directory: &Path,
        meta: &regex::Regex,
        marker: &regex::Regex,
    ) -> Result<CaseBinding> {
        let snapshot = ready["facts"]["first_append"]["snapshot_id"]
            .as_i64()
            .filter(|v| *v > 0)
            .context("HMS snapshot is not a positive Int64")?;
        let mut vars = variables.clone();
        vars.insert("__hms_case_input_kind".into(), KIND.into());
        vars.insert(BOUND_NAMESPACE.into(), namespaces.0.into());
        vars.insert(BOUND_SIBLING.into(), namespaces.1.into());
        vars.insert(ID.into(), snapshot.to_string());
        vars.insert(
            RECEIPT.into(),
            quote(
                directory
                    .join("input.ready.json")
                    .to_str()
                    .context("non UTF8 HMS receipt")?,
            ),
        );
        let case = crate::parser::load_sql_case_from_file(&self.path, meta, marker, &vars)?
            .context("bound HMS input has no executable case")?;
        ensure!(case.steps.len() == 59, "bound HMS case changed step count");
        // The existing loader above rewrites/parses the complete real SQL with
        // the sealed owned catalog. The declaration projection never reaches here.
        ensure!(
            case.steps[11]
                .sql
                .contains(&format!(" FOR VERSION AS OF {snapshot} ORDER BY id;"))
                && !case
                    .steps
                    .iter()
                    .any(|step| step.sql.contains("${hms_first_snapshot_id}")),
            "HMS numeric VERSION was not fully bound before execution"
        );
        // This negative statement must still name the actual prepared v1
        // data. An earlier read-only refusal must not hide a stale URI.
        let v1 = ready["facts"]["actual_locations"]["v1"]
            .as_str()
            .context("HMS actual v1 location absent")?;
        let frozen_warehouse = vars
            .get("iceberg_hms_warehouse")
            .context("HMS frozen warehouse absent")?;
        let frozen_root = format!(
            "{}/iru7/{}",
            frozen_warehouse.trim_end_matches('/'),
            namespaces.0
        );
        let v2 = ready["facts"]["actual_locations"]["v2"]
            .as_str()
            .context("HMS actual v2 location absent")?;
        ensure!(
            v1 == format!("{frozen_root}/v1") && v2 == format!("{frozen_root}/v2"),
            "HMS actual SDK locations differ from frozen namespace/warehouse"
        );
        let expected_add_files_uri = format!("{}/data", v1.trim_end_matches('/'));
        let actual_uri = q40_location(&case.steps[39].sql);
        write_once(
            &directory.join("input.binding-attempt.json"),
            &json!({
                "schema":"hms-case-input-binding-attempt-v1",
                "qualification":"diagnostic-only-not-Bound-or-Ready",
                "source_sha256":self.source_sha256,
                "ready_sha256":crate::results_record::hash_file(&directory.join("input.ready.json"))?,
                "namespace":namespaces.0,"new_namespace":namespaces.1,
                "q40_sql_sha256":format!("{:x}",Sha256::digest(case.steps[39].sql.as_bytes())),
                "actual_uri":actual_uri.as_ref().ok(),
                "parse_error":actual_uri.as_ref().err().map(|error| format!("{error:#}")),
                "expected_uri":expected_add_files_uri,
                "actual_sdk_v1":v1,"actual_sdk_v2":v2,
                "frozen_hms_warehouse":frozen_warehouse
            }),
        )?;
        ensure!(
            actual_uri? == expected_add_files_uri,
            "bound HMS q40 ADD FILES location differs from actual prepared v1 data"
        );
        Ok(CaseBinding {
            case,
            snapshot,
            q40_uri: expected_add_files_uri,
            warehouse: frozen_warehouse.clone(),
            v1: v1.into(),
            v2: v2.into(),
        })
    }
    pub(crate) fn load(path: &Path, meta: &regex::Regex, marker: &regex::Regex) -> Result<Self> {
        ensure!(
            path.file_stem().and_then(|v| v.to_str()) == Some(CASE),
            "closed HMS input case mismatch"
        );
        let source = fs::read_to_string(path)?;
        let variables = crate::extension_manifest::literal_placeholder_variables(&source);
        let case = crate::parser::load_sql_case_from_file_preserving_placeholders(
            path, meta, marker, &variables,
        )?
        .context("HMS input definition has no steps")?;
        ensure!(
            case.steps.len() == 59 && case.sequential,
            "HMS recipe requires its sequential 59 steps"
        );
        // Queries 1-52 retain their original ordinals. Retired SHOW OPTIMIZE
        // and SHOW ANALYZE observations are removed; the original inventory,
        // rows, MV listing and table/view listings now occupy queries 53-57.
        // The same two owned cleanup steps remain last, at queries 58-59.
        ensure!(
            case.steps
                .iter()
                .filter(|s| s.meta.expect_error.is_some())
                .count()
                == 38,
            "HMS recipe requires its original negative guards and supported empty view listing"
        );
        ensure!(
            case.steps
                .iter()
                .filter(|s| s.sql.trim_start().starts_with("shell:"))
                .count()
                == 4,
            "HMS recipe requires its original shell observations"
        );
        ensure!(
            source.matches("${hms_first_snapshot_id}").count() == 2
                && source.matches("${hms_prepared_input_receipt}").count() == 1,
            "HMS input slots escaped their closed recipe positions"
        );
        ensure!(
            case.steps[10].meta.result_rows_where
                == vec![("snapshot_id".into(), "${hms_first_snapshot_id}".into())]
                && case.steps[10].meta.result_rows_count == Some(1),
            "HMS snapshot identity oracle absent"
        );
        for step in &case.steps {
            ensure!(
                !step.sql.contains("${hms_first_snapshot_id}") || step.query_number == 12,
                "HMS numeric input outside the numeric VERSION step"
            );
            ensure!(
                !step.sql.contains("${hms_prepared_input_receipt}") || step.query_number == 1,
                "HMS receipt input outside the fixed verifier"
            );
        }
        ensure!(
            case.steps[0].sql
                == "shell: python3 \"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-hms/fixture.py\" verify-prepared iru7_${uuid0} --receipt ${hms_prepared_input_receipt}",
            "HMS verifier must be the fixed recipe action"
        );
        ensure!(
            case.steps[10].sql
                == "SELECT snapshot_id FROM ice_hms_${uuid0}.iru7_${uuid0}.v2$snapshots ORDER BY committed_at LIMIT 1;",
            "HMS metadata observation changed"
        );
        ensure!(
            case.steps[11].sql
                == "SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 FOR VERSION AS OF ${hms_first_snapshot_id} ORDER BY id;",
            "HMS numeric VERSION observation changed"
        );
        Ok(Self {
            path: path.into(),
            source_sha256: crate::results_record::hash_file(path)?,
        })
    }
}

pub(crate) fn add_recipe_names(source: &str, declared: &mut BTreeSet<String>) -> Result<()> {
    let re = regex::Regex::new(r"verify-prepared\s+(iru7_[A-Za-z0-9_${}]+)\s+--receipt")?;
    let captures = re
        .captures(source)
        .context("closed HMS recipe has no fixed verifier namespace")?;
    declared.insert(captures[1].into());
    declared.insert(format!("{}_new", &captures[1]));
    Ok(())
}
fn q40_location(sql: &str) -> Result<String> {
    use novarocks_parser::ast::{IcebergStatement, IcebergTableAction, LiteralKind, Statement};
    let statements = novarocks_parser::parse(sql)
        .map_err(|error| anyhow::anyhow!("parse bound HMS q40 ADD FILES: {error}"))?;
    let [Statement::Iceberg(IcebergStatement::AlterTable(table))] = statements.as_slice() else {
        anyhow::bail!("bound HMS q40 lost its exact ADD FILES statement");
    };
    let IcebergTableAction::AddFiles(files) = &table.action else {
        anyhow::bail!("bound HMS q40 changed action");
    };
    let LiteralKind::String(uri) = &files.location.kind else {
        anyhow::bail!("bound HMS q40 location is not a string literal");
    };
    Ok(uri.clone())
}

fn canonical(v: &Value) -> Value {
    match v {
        Value::Object(items) => Value::Object(
            items
                .iter()
                .map(|(k, v)| (k.clone(), canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        _ => v.clone(),
    }
}
fn digest(v: &Value) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&canonical(v))?)
    ))
}
fn read(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn write_once(path: &Path, v: &Value) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(&serde_json::to_vec_pretty(v)?)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    Ok(())
}
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

pub(crate) struct Registration<'a> {
    pub manifest: PathBuf,
    pub expected: PathBuf,
    pub budget: &'a RunBudget,
    pub storage: &'a RunStorageScope,
    pub invocation: Value,
}

/// Preserve the original inventory across unwinding; an unconfirmed actor is retained.
pub(crate) fn capture_unwind(
    body: impl FnOnce() -> Result<i32>,
) -> std::thread::Result<Result<i32>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(body))
}

/// Registered outside fixture_primary, before any producer can start. No Clone.
pub(crate) struct Custody<'a> {
    repo: PathBuf,
    definition: Definition,
    directory: PathBuf,
    namespace: String,
    sibling: String,
    budget: &'a RunBudget,
    storage: &'a RunStorageScope,
    attempted: bool,
    local_exit: bool,
    finished: bool,
    finalizer_attempted: bool,
}
impl<'a> Custody<'a> {
    pub(crate) fn register(
        repo: &Path,
        definition: Definition,
        variables: &HashMap<String, String>,
        registration: Registration<'a>,
    ) -> Result<Self> {
        let Registration {
            manifest,
            expected,
            budget,
            storage,
            invocation,
        } = registration;
        ensure!(
            storage.identity() == budget.identity(),
            "HMS input storage differs from original run"
        );
        let mut vars = crate::config::case_placeholder_variables(variables, CASE)?;
        vars.insert("__hms_case_input_kind".into(), KIND.into());
        let raw = fs::read_to_string(&definition.path)?;
        let raw = crate::config::substitute_placeholders(
            &raw,
            &{
                let mut v = vars.clone();
                v.insert(ID.into(), format!("${{{ID}}}"));
                v.insert(RECEIPT.into(), format!("${{{RECEIPT}}}"));
                v
            },
            "HMS namespace binding",
        )?;
        let mapping = crate::fixture_context::namespace_mapping(&raw, &vars)?;
        let alias = format!("iru7_{}", vars.get("uuid0").context("HMS UUID absent")?);
        let namespace = mapping
            .get(&alias)
            .context("HMS original namespace mapping absent")?
            .clone();
        let sibling = mapping
            .get(&format!("{alias}_new"))
            .context("HMS sibling mapping absent")?
            .clone();
        let directory = repo
            .join("tests/sql/.runtime/hms-evidence")
            .join(&namespace);
        fs::create_dir_all(directory.parent().context("HMS evidence parent absent")?)?;
        fs::create_dir(&directory)?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        let mut sources = BTreeMap::new();
        for path in [
            definition.path.as_path(),
            manifest.as_path(),
            expected.as_path(),
            &repo.join(PRODUCER),
            &repo.join(REMOTE),
        ] {
            let relative = path
                .strip_prefix(repo)
                .context("HMS source must belong to this workspace")?;
            sources.insert(
                relative.to_string_lossy().to_string(),
                crate::results_record::hash_file(path)?,
            );
        }
        let mut publication_paths = BTreeMap::new();
        for key in [
            "NOVA_ENV_REST_ENV_FILE",
            "NOVAROCKS_SPARK_DEFAULTS",
            "NOVAROCKS_SPARK_HMS_DEFAULTS",
        ] {
            publication_paths.insert(
                key.to_string(),
                PathBuf::from(
                    std::env::var_os(key)
                        .with_context(|| format!("HMS original publication path absent: {key}"))?,
                ),
            );
        }
        publication_paths.insert(
            "REST_MANIFEST".into(),
            publication_paths["NOVA_ENV_REST_ENV_FILE"].with_file_name("manifest.json"),
        );
        let mut publications = BTreeMap::new();
        for (key, path) in publication_paths {
            publications.insert(
                key,
                json!({"path":path,"sha256":crate::results_record::hash_file(&path)?}),
            );
        }
        let context: Value = serde_json::from_str(&budget.to_json()?)?;
        let store: Value = serde_json::from_str(&storage.to_json()?)?;
        write_once(
            &directory.join("input.intent.json"),
            &json!({"schema":"hms-case-input-intent-v1",
            "kind":KIND,"unit":format!("sql-case:{SUITE}/{CASE}"),"context":context,
            "storage_sha256":digest(&store)?,"namespace":namespace,"new_namespace":sibling,
            "sources":sources,"publications":publications,"invocation":invocation}),
        )?;
        Ok(Self {
            repo: repo.into(),
            definition,
            directory,
            namespace,
            sibling,
            budget,
            storage,
            attempted: false,
            local_exit: false,
            finished: false,
            finalizer_attempted: false,
        })
    }
    fn command(&mut self, action: &str) -> Result<()> {
        let cleanup = action == "cleanup";
        let mut command = novarocks_test_support::ManagedCommand::new("python3");
        command
            .current_dir(&self.repo)
            .env("NOVAROCKS_WORKSPACE_ROOT", &self.repo)
            .arg(self.repo.join(PRODUCER))
            .arg(action)
            .arg(&self.namespace)
            .arg("--new-namespace")
            .arg(&self.sibling)
            .env(RUN_CONTEXT_ENV, self.budget.to_json()?)
            .env(RUN_STORAGE_ENV, self.storage.to_json()?);
        if cleanup {
            command.observe_group_exit_until(self.budget.deadline());
        } else {
            command.terminate_at(self.budget.work_deadline(), self.budget.deadline());
        }
        command.env_remove("NOVA_FIXTURE_CLEANUP_MODE");
        if cleanup {
            command.env("NOVA_FIXTURE_CLEANUP_MODE", "true");
        }
        let observed = novarocks_test_support::capture_command_until(
            command,
            tempfile::tempfile()?,
            tempfile::tempfile()?,
            self.budget.deadline(),
        )?;
        self.local_exit = observed.group_exit_observed();
        for (name, bytes) in [
            ("stdout", &observed.output.stdout),
            ("stderr", &observed.output.stderr),
        ] {
            use std::os::unix::fs::OpenOptionsExt;
            let mut raw = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(self.directory.join(format!("{action}.{name}.private")))?;
            raw.write_all(bytes)?;
        }
        write_once(
            &self.directory.join(format!("{action}.local.json")),
            &json!({
            "schema":"hms-case-input-local-exit-v1","actual_exit":observed.output.status.code(),
            "group_exit_observed":observed.group_exit_observed(),"timed_out":observed.timed_out,
            "failures":observed.failures,"diagnostic_root":observed.diagnostic_root()}),
        )?;
        ensure!(
            !observed.timed_out
                && observed.failures.is_empty()
                && self.local_exit
                && observed.output.status.success(),
            "HMS {action} failed; original diagnostics retained"
        );
        Ok(())
    }
    pub(crate) fn prepare_and_bind(
        &mut self,
        meta: &regex::Regex,
        marker: &regex::Regex,
        variables: &HashMap<String, String>,
        writer: &crate::results_record::ResultsWriter,
    ) -> Result<crate::types::SqlCase> {
        ensure!(
            crate::results_record::hash_file(&self.definition.path)?
                == self.definition.source_sha256,
            "HMS template changed after definition"
        );
        let intent_path = self.directory.join("input.intent.json");
        writer.write(json!({"kind":"prepared_input","phase":"registered","unit":format!("sql-case:{SUITE}/{CASE}"),
            "intent":intent_path,"sha256":writer.track_file(&intent_path)?}))?;
        ensure!(
            std::time::Instant::now() < self.budget.work_deadline(),
            "original HMS preparation work deadline expired"
        );
        self.attempted = true;
        self.command("prepare")?;
        let ready = read(&self.directory.join("input.ready.json"))?;
        self.check_ready(&ready)?;
        let intent = read(&self.directory.join("input.intent.json"))?;
        for (name, sha) in intent["sources"]
            .as_object()
            .context("HMS original source identities absent")?
        {
            ensure!(
                crate::results_record::hash_file(&self.repo.join(name))?
                    == sha.as_str().context("HMS source hash absent")?,
                "HMS input source changed during preparation"
            );
        }
        let binding = self.definition.bind_ready_case(
            variables,
            (&self.namespace, &self.sibling),
            &ready,
            &self.directory,
            meta,
            marker,
        );
        let attempt = self.directory.join("input.binding-attempt.json");
        if attempt.exists() {
            writer.track_file(&attempt)?;
        }
        let CaseBinding {
            case,
            snapshot,
            q40_uri: expected_add_files_uri,
            warehouse: frozen_warehouse,
            v1,
            v2,
        } = binding?;
        let steps = case
            .steps
            .iter()
            .map(|step| {
                json!({
                    "step":step.query_number,
                    "sql_sha256":format!("{:x}",Sha256::digest(step.sql.as_bytes()))
                })
            })
            .collect::<Vec<_>>();
        write_once(
            &self.directory.join("input.bound.json"),
            &json!({"schema":"hms-case-input-bound-v1",
            "ready_sha256":crate::results_record::hash_file(&self.directory.join("input.ready.json"))?,
            "namespace":self.namespace,"new_namespace":self.sibling,"snapshot_id":snapshot,"steps":steps,
            "q11_result_rows_where":case.steps[10].meta.result_rows_where,
            "q11_result_rows_count":case.steps[10].meta.result_rows_count,
            "q40_add_files_uri":expected_add_files_uri,
            "frozen_hms_warehouse":frozen_warehouse,
            "actual_v1_location":v1,"actual_v2_location":v2}),
        )?;
        for publication in ready["publications"]
            .as_object()
            .context("HMS publication identities absent")?
            .values()
        {
            let path = Path::new(
                publication["path"]
                    .as_str()
                    .context("HMS publication path absent")?,
            );
            ensure!(
                writer.track_file(path)?
                    == publication["sha256"]
                        .as_str()
                        .context("HMS publication hash absent")?,
                "HMS publication changed after preparation"
            );
        }
        for name in [
            "input.intent.json",
            "input.ready.json",
            "prepare.remote.json",
            "prepare.local.json",
            "input.bound.json",
        ] {
            let path = self.directory.join(name);
            let sha = writer.track_file(&path)?;
            writer.write(
                json!({"kind":"prepared_input","unit":format!("sql-case:{SUITE}/{CASE}"),
                "phase":name,"path":path,"sha256":sha}),
            )?;
        }
        Ok(case)
    }
    fn remote_exited(&self, action: &str) -> Result<()> {
        let receipt = read(&self.directory.join(format!("{action}.remote.json")))?;
        let context: Value = serde_json::from_str(&self.budget.to_json()?)?;
        let code = receipt["returncode"]
            .as_i64()
            .context("HMS remote actual exit absent")?;
        let cli = if code < 0 { 128 - code } else { code };
        ensure!(
            receipt["container_id"]
                .as_str()
                .is_some_and(|v| !v.is_empty())
                && receipt["owner_folder"]
                    .as_str()
                    .is_some_and(|v| !v.is_empty())
                && receipt["payload_sha256"]
                    .as_str()
                    .is_some_and(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())),
            "HMS original remote operation identity absent"
        );
        ensure!(
            receipt["schema"] == "fixture-compose-settlement-v1"
                && receipt["context"] == context
                && receipt["child_waited"] == true
                && receipt["cli_returncode"].as_i64() == Some(cli)
                && receipt["cleanup_failed"] == false
                && receipt["temporary_owner_removed"] == true,
            "HMS original remote actor exit unconfirmed"
        );
        Ok(())
    }
    fn check_ready(&self, ready: &Value) -> Result<()> {
        ensure!(
            ready["publications"]
                == read(&self.directory.join("input.intent.json"))?["publications"],
            "HMS ready publication differs from the registered original input"
        );
        self.remote_exited("prepare")?;
        ensure!(
            ready["schema"] == "hms-case-input-ready-v1"
                && ready["intent_sha256"]
                    == crate::results_record::hash_file(&self.directory.join("input.intent.json"))?
                && ready["remote_sha256"]
                    == crate::results_record::hash_file(
                        &self.directory.join("prepare.remote.json")
                    )?,
            "HMS prepared input identity changed"
        );
        ensure!(
            read(&self.directory.join("prepare.remote.json"))?["returncode"] == 0,
            "HMS ready fact requires the successful original setup actor"
        );
        let facts = &ready["facts"];
        ensure!(
            facts["prepared"] == self.namespace
                && facts["first_append"]["table_uuid"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
                && facts["v2_metadata_location"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()),
            "HMS metadata identity absent"
        );
        let first = &facts["first_append"];
        ensure!(
            first["table_uuid"] == facts["v2_table_uuid"],
            "HMS table identity changed after first append"
        );
        let snapshots = facts["final_snapshots"]
            .as_array()
            .context("HMS final snapshots absent")?;
        let at = first["timestamp_ms"]
            .as_i64()
            .context("HMS first timestamp absent")?;
        ensure!(
            snapshots
                .iter()
                .filter(|v| v["timestamp_ms"].as_i64() == Some(at))
                .count()
                == 1
                && snapshots
                    .iter()
                    .all(|v| v["timestamp_ms"].as_i64().is_some_and(|v| v >= at))
                && snapshots
                    .iter()
                    .any(|v| v["snapshot_id"] == first["snapshot_id"]
                        && v["timestamp_ms"] == first["timestamp_ms"]),
            "HMS prepared snapshot is not uniquely earliest and retained"
        );
        Ok(())
    }
    /// Once only. A failed case cleanup is never silently dispatched a second time.
    pub(crate) fn finish(&mut self, consumers_exited: bool) -> Result<Value> {
        ensure!(
            !self.finalizer_attempted,
            "HMS input finalizer already attempted; original state retained"
        );
        self.finalizer_attempted = true;
        if !self.attempted {
            self.finished = true;
            return Ok(json!({"status":"not_started"}));
        }
        ensure!(
            consumers_exited,
            "HMS consumers have no actual exit proof; input retained"
        );
        ensure!(
            self.local_exit,
            "HMS local setup actor has no actual group exit proof"
        );
        self.remote_exited("prepare")?;
        if !self.directory.join("input.cleanup.json").exists() {
            ensure!(
                !self.directory.join("input.cleanup-attempt.json").exists(),
                "HMS original cleanup failed or is unknown; no automatic retry"
            );
            ensure!(
                std::time::Instant::now() < self.budget.deadline(),
                "original HMS cleanup deadline expired"
            );
            write_once(
                &self.directory.join("input.tail-cleanup-attempt.json"),
                &json!({
                    "schema":"hms-tail-cleanup-attempt-v1", "phase":"before-managed-launch",
                    "intent_sha256":crate::results_record::hash_file(&self.directory.join("input.intent.json"))?,
                    "context":serde_json::from_str::<Value>(&self.budget.to_json()?)?
                }),
            )?;
            self.command("cleanup")?;
        }
        let receipt = read(&self.directory.join("input.cleanup.json"))?;
        self.remote_exited("cleanup")?;
        ensure!(
            read(&self.directory.join("cleanup.remote.json"))?["returncode"] == 0,
            "HMS cleanup did not complete successfully"
        );
        ensure!(
            receipt["schema"] == "hms-case-input-cleanup-v1"
                && receipt["namespace"] == self.namespace
                && receipt["new_namespace"] == self.sibling
                && empty_object_inventory(&receipt["remaining_objects"])
                && receipt["intent_sha256"]
                    == crate::results_record::hash_file(&self.directory.join("input.intent.json"))?
                && receipt["remote_sha256"]
                    == crate::results_record::hash_file(
                        &self.directory.join("cleanup.remote.json")
                    )?,
            "HMS original cleanup receipt does not close its exact input scope"
        );
        self.finished = true;
        Ok(
            json!({"status":"closed","receipt":self.directory.join("input.cleanup.json"),
            "sha256":crate::results_record::hash_file(&self.directory.join("input.cleanup.json"))?}),
        )
    }
}
impl Drop for Custody<'_> {
    fn drop(&mut self) {
        if !self.finished {
            eprintln!(
                "HMS input custody is unconfirmed; original intent retained at {}",
                self.directory.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const TEMPLATE: &str = include_str!(
        "../../../correctness/iceberg-hms/sql/iceberg_hms_read_only_compatibility.sql"
    );
    fn definition(source: &str) -> (tempfile::TempDir, Result<Definition>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{CASE}.sql"));
        fs::write(&path, source).unwrap();
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        let result = Definition::load(&path, &meta, &marker);
        (dir, result)
    }
    #[test]
    fn hms_definition_keeps_59_steps_without_starting_a_producer() {
        let (dir, result) = definition(TEMPLATE);
        assert_eq!(result.unwrap().case_id(), CASE);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        assert!(!dir.path().join("tests/sql/.runtime").exists());
    }
    #[test]
    fn hms_catalog_projection_preserves_ownership_uses_without_binding_a_snapshot() {
        let (dir, definition) = definition(TEMPLATE);
        let definition = definition.unwrap();
        let view = definition.catalog_definition_projection().unwrap();
        assert_eq!(
            fs::read_to_string(dir.path().join(format!("{CASE}.sql"))).unwrap(),
            TEMPLATE
        );
        assert!(TEMPLATE.contains("FOR VERSION AS OF ${hms_first_snapshot_id}"));
        assert!(
            view.source()
                .contains("SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 ORDER BY id;")
        );
        assert!(
            !view
                .source()
                .contains("FOR VERSION AS OF ${hms_first_snapshot_id}")
        );
        assert!(
            view.source()
                .contains("CREATE EXTERNAL CATALOG ice_hms_${uuid0}")
        );
        assert_eq!(
            view.referenced_catalog(&HashMap::from([("uuid0".into(), "definition_only".into())]))
                .unwrap(),
            "ice_hms_definition_only"
        );
    }
    #[test]
    fn hms_definition_rejects_receipt_or_numeric_slot_escape_before_production() {
        let bad = TEMPLATE.replace(
            "SELECT COUNT(*) AS snapshots",
            "SELECT '${hms_first_snapshot_id}', COUNT(*) AS snapshots",
        );
        let (dir, result) = definition(&bad);
        assert!(result.is_err());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        let bad = TEMPLATE.replace("-- @result_rows_count=1\n", "");
        assert!(definition(&bad).1.is_err());
        let numeric_version = "v2 FOR VERSION AS OF ${hms_first_snapshot_id} ORDER BY id;";
        let escaped = [
            (
                "numeric slot used as an identifier",
                TEMPLATE.replace(numeric_version, "v2_${hms_first_snapshot_id} ORDER BY id;"),
            ),
            (
                "numeric slot moved from its exact row selector into text control",
                TEMPLATE.replace(
                    "-- @result_rows_where=snapshot_id=${hms_first_snapshot_id}",
                    "-- @result_contains=${hms_first_snapshot_id}",
                ),
            ),
            (
                "receipt slot moved from the fixed verifier into text control",
                TEMPLATE
                    .replace(
                        "--receipt ${hms_prepared_input_receipt}",
                        "--receipt 'invalid-test-only-receipt'",
                    )
                    .replace(
                        "-- @result_contains=HMS_PREPARE_OK",
                        "-- @result_contains=${hms_prepared_input_receipt}",
                    ),
            ),
            (
                "malformed static control metadata",
                TEMPLATE.replace(
                    "-- @result_rows_count=1",
                    "-- @result_rows_count=not-an-integer",
                ),
            ),
        ];
        for (label, source) in escaped {
            // Keep slot counts exact: these failures must not merely exercise
            // the duplicate-slot-count check, and no custody is registered.
            assert_eq!(
                source.matches("${hms_first_snapshot_id}").count(),
                2,
                "{label}"
            );
            assert_eq!(
                source.matches("${hms_prepared_input_receipt}").count(),
                1,
                "{label}"
            );
            let (directory, rejected) = definition(&source);
            assert!(rejected.is_err(), "accepted {label}");
            assert_eq!(
                fs::read_dir(directory.path()).unwrap().count(),
                1,
                "{label}"
            );
            assert!(
                !directory
                    .path()
                    .join("tests/sql/.runtime/hms-evidence")
                    .exists()
            );
        }
    }
    #[test]
    fn hms_case_input_manifest_is_closed_and_resource_free() {
        let m = crate::suite_manifest::SuiteManifest::parse(
            "explicit_only=true\ncase_input='hms-read-only-compatibility-v1'\nheavy={reason='fixture',detail='fixed HMS fixture input'}",
        )
        .unwrap();
        assert_eq!(
            m.case_input,
            Some(crate::suite_manifest::CaseInputRecipe::HmsReadOnlyCompatibilityV1)
        );
        assert!(
            crate::suite_manifest::SuiteManifest::parse(
                "explicit_only=true\ncase_input='capture-shell'\nheavy={reason='fixture',detail='fixed HMS fixture input'}"
            )
            .is_err()
        );
        assert!(
            crate::suite_manifest::SuiteManifest::parse(
                "case_input='hms-read-only-compatibility-v1'"
            )
            .is_err()
        );
    }

    #[test]
    fn hms_cleanup_inventory_requires_the_python_mapping_shape() {
        assert!(empty_object_inventory(&json!({})));
        for rejected in [
            Value::Null,
            json!([]),
            json!(false),
            json!({
                "s3://fixture/owned/object": {"etag":"original","size":1}
            }),
        ] {
            assert!(!empty_object_inventory(&rejected));
        }
    }

    #[test]
    fn actual_python_inventory_crosses_as_a_map_not_a_list() {
        let producer =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/iceberg-hms/fixture.py");
        let script = r#"
import importlib.util, json, os, sys, xml.etree.ElementTree as ET
spec = importlib.util.spec_from_file_location('inventory_contract_fixture', sys.argv[1])
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
ns = 'http://s3.amazonaws.com/doc/2006-03-01/'
nonempty = False
def observed_page(bucket, parameters, method='GET', key=''):
    root = ET.Element('{'+ns+'}ListBucketResult')
    ET.SubElement(root, '{'+ns+'}IsTruncated').text = 'false'
    if nonempty:
        item = ET.SubElement(root, '{'+ns+'}Contents')
        ET.SubElement(item, '{'+ns+'}Key').text = parameters['prefix']+'part.parquet'
        ET.SubElement(item, '{'+ns+'}ETag').text = 'fixture-etag'
        ET.SubElement(item, '{'+ns+'}Size').text = '7'
    return root
fixture.s3_request = observed_page
os.environ['NOVAROCKS_ICEBERG_HMS_WAREHOUSE'] = 's3://fixture/original/hms'
empty, empty_pages = fixture.inventory('owned', new_namespace='owned_new')
nonempty = True
objects, pages = fixture.inventory('owned', new_namespace='owned_new')
print(json.dumps({'empty': {'remaining_objects':empty},
                  'nonempty': {'remaining_objects':objects},
                  'pages':[empty_pages,pages]}, sort_keys=True))
"#;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut command = novarocks_test_support::ManagedCommand::new("python3");
        command
            .env_clear()
            .envs(std::env::vars_os().filter(|(key, _)| {
                matches!(
                    key.to_str(),
                    Some("PATH" | "HOME" | "TMPDIR" | "LANG" | "LC_ALL")
                )
            }));
        command
            .arg("-c")
            .arg(script)
            .arg(producer)
            .observe_group_exit_until(deadline);
        let actual = novarocks_test_support::capture_command_until(
            command,
            tempfile::tempfile().unwrap(),
            tempfile::tempfile().unwrap(),
            deadline,
        )
        .unwrap();
        assert!(
            actual.output.status.success(),
            "{}",
            String::from_utf8_lossy(&actual.output.stderr)
        );
        assert!(actual.group_exit_observed());
        assert!(!actual.timed_out && actual.failures.is_empty());
        let value: Value = serde_json::from_slice(&actual.output.stdout).unwrap();
        assert_eq!(value["empty"]["remaining_objects"], json!({}));
        assert!(empty_object_inventory(&value["empty"]["remaining_objects"]));
        assert!(!empty_object_inventory(
            &value["nonempty"]["remaining_objects"]
        ));
        assert!(
            value["nonempty"]["remaining_objects"]
                .as_object()
                .unwrap()
                .len()
                > 1
        );
    }

    fn original_native02_variables(
        definition: &Definition,
    ) -> (HashMap<String, String>, String, String) {
        // Actual original coordinates seed this hermetic binding regression.
        // No old budget is admitted, no SDK fact or remote Ready is fabricated.
        let run = "nr_scratch_1793352066131_5547f5050f68e39fc7e1f891835f3da1";
        let scope = format!("s3://novarocks/novarocks-2be402e0/iceberg-catalog/scratch/{run}");
        let mut variables = HashMap::new();
        let source = fs::read_to_string(&definition.path).unwrap();
        let placeholder = regex::Regex::new(r"\$\{([A-Za-z0-9_.]+)\}").unwrap();
        for captures in placeholder.captures_iter(&source) {
            variables.insert(captures[1].into(), "syntax_only".into());
        }
        variables.insert("suite_run_id".into(), run.into());
        variables.insert("__scratch_scope".into(), scope.clone());
        variables.insert(format!("__case_ordinal.{CASE}"), "0".into());
        variables.insert("iceberg_hms_warehouse".into(), format!("{scope}/hms"));
        variables.insert("__hms_case_input_kind".into(), KIND.into());
        variables.insert(ID.into(), format!("${{{ID}}}"));
        variables.insert(RECEIPT.into(), format!("${{{RECEIPT}}}"));
        let case_variables = crate::config::case_placeholder_variables(&variables, CASE).unwrap();
        let raw = crate::config::substitute_placeholders(
            &source,
            &case_variables,
            "original registration",
        )
        .unwrap();
        let mapping = crate::fixture_context::namespace_mapping(&raw, &case_variables).unwrap();
        let family = format!("iru7_{}", case_variables["uuid0"]);
        let namespace = mapping[&family].clone();
        let sibling = mapping[&format!("{family}_new")].clone();
        assert_eq!(namespace, format!("{run}_d0001"));
        assert_eq!(sibling, format!("{run}_d0002"));
        let view = definition.catalog_definition_projection().unwrap();
        let projected = crate::config::substitute_placeholders(
            view.source(),
            &case_variables,
            "original catalog ownership",
        )
        .unwrap();
        let projected =
            crate::fixture_context::rewrite_namespaces(&projected, &case_variables).unwrap();
        let symbols = crate::catalog_symbol::CatalogSymbols::seal(run, &[projected]).unwrap();
        symbols.extend_variables(&mut variables).unwrap();
        (variables, namespace, sibling)
    }

    #[test]
    fn real_receipt_path_keeps_original_names_through_complete_catalog_and_uri_binding() {
        let (directory, definition) = definition(TEMPLATE);
        let definition = definition.unwrap();
        let (variables, namespace, sibling) = original_native02_variables(&definition);
        let evidence = directory.path().join(&namespace);
        fs::create_dir(&evidence).unwrap();
        let root = format!("{}/iru7/{namespace}", variables["iceberg_hms_warehouse"]);
        let ready = json!({"schema":"syntax-regression-not-remote-Ready", "facts":{
            "first_append":{"snapshot_id":4018151980747658674i64},
            "actual_locations":{"v1":format!("{root}/v1"),"v2":format!("{root}/v2")}
        }});
        write_once(&evidence.join("input.ready.json"), &ready).unwrap();
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        let binding = definition
            .bind_ready_case(
                &variables,
                (&namespace, &sibling),
                &ready,
                &evidence,
                &meta,
                &marker,
            )
            .unwrap();
        let case = binding.case;
        let family = format!("iru7_{}", case.expected_variables["uuid0"]);
        assert_eq!(case.namespace_rewrites[&family], namespace);
        assert_eq!(case.namespace_rewrites[&format!("{family}_new")], sibling);
        assert_eq!(case.steps.len(), 59);
        assert_eq!(
            case.steps
                .iter()
                .filter(|step| step.meta.expect_error.is_some())
                .count(),
            38
        );
        assert_eq!(
            case.steps
                .iter()
                .filter(|step| step.sql.starts_with("shell:"))
                .count(),
            4
        );
        assert!(
            case.steps[0]
                .sql
                .contains(evidence.join("input.ready.json").to_str().unwrap())
        );
        assert!(
            case.steps[0]
                .sql
                .contains(&format!("--new-namespace {sibling}"))
        );
        assert_eq!(
            case.steps[10].meta.result_rows_where,
            vec![("snapshot_id".into(), "4018151980747658674".into())]
        );
        assert_eq!(case.steps[10].meta.result_rows_count, Some(1));
        assert!(
            case.steps[11]
                .sql
                .contains(" FOR VERSION AS OF 4018151980747658674 ORDER BY id;")
        );
        let expected_uri = format!("{root}/v1/data");
        assert_eq!(q40_location(&case.steps[39].sql).unwrap(), expected_uri);
        let sealed =
            crate::catalog_symbol::CatalogSymbols::from_variables(&case.expected_variables)
                .unwrap();
        let old_catalog = format!("ice_hms_{}", case.expected_variables["uuid0"]);
        let catalog = sealed.lookup(&old_catalog).unwrap();
        assert!(
            case.steps[1]
                .sql
                .contains(&format!("CREATE EXTERNAL CATALOG {catalog}"))
        );
        assert!(
            case.steps[39]
                .sql
                .contains(&format!("ALTER TABLE {catalog}.{namespace}.v2"))
        );
        assert_eq!(
            case.steps
                .iter()
                .map(|step| step.query_number)
                .collect::<Vec<_>>(),
            (1..=59).collect::<Vec<_>>()
        );
        assert!(
            case.steps[55]
                .sql
                .contains(&format!("FROM {catalog}.information_schema.tables"))
        );
        assert!(!case.steps[55].meta.cleanup);
        assert!(
            case.steps[54]
                .sql
                .contains("FROM system.metadata.materialized_views")
        );
        assert!(
            case.steps[54]
                .sql
                .contains(&format!("catalog_name = '{catalog}'"))
        );
        assert!(
            case.steps[54]
                .sql
                .contains(&format!("schema_name = '{namespace}'"))
        );
        assert!(
            case.steps[56]
                .sql
                .contains(&format!("FROM {catalog}.information_schema.tables"))
        );
        assert!(case.steps[56].sql.contains("table_type = 'VIEW'"));
        assert!(
            case.steps[56]
                .sql
                .contains(&format!("table_schema = '{namespace}'"))
        );
        assert!(case.steps[56].meta.expect_sql_code.is_none());
        assert!(case.steps[56].meta.expect_sql_phase.is_none());
        assert!(case.steps[56].meta.expect_error.is_none());
        assert!(!case.steps[56].meta.cleanup);
        assert!(case.steps[57].meta.cleanup);
        assert!(
            case.steps[57]
                .sql
                .contains(&format!(" cleanup {namespace}"))
        );
        assert!(case.steps[58].meta.cleanup);
        assert_eq!(case.steps[58].sql, format!("DROP CATALOG {catalog};"));
        let attempt = read(&evidence.join("input.binding-attempt.json")).unwrap();
        assert_eq!(attempt["actual_uri"], expected_uri);
        assert_eq!(attempt["expected_uri"], expected_uri);
        assert_eq!(
            attempt["qualification"],
            "diagnostic-only-not-Bound-or-Ready"
        );
        assert!(!evidence.join("input.bound.json").exists());
        let mut escaped = case.expected_variables.clone();
        escaped.remove(BOUND_SIBLING);
        assert!(bound_namespace_mapping(&escaped).is_err());
        escaped.insert(BOUND_SIBLING.into(), "foreign_run_d0002".into());
        assert!(bound_namespace_mapping(&escaped).is_err());
        assert_eq!(fs::read_to_string(&definition.path).unwrap(), TEMPLATE);
    }

    #[test]
    fn unmatched_add_files_location_is_retained_as_attempt_not_bound() {
        let changed = TEMPLATE.replace("/v1/data';", "/v2/data';");
        let (directory, definition) = definition(&changed);
        let definition = definition.unwrap();
        let (variables, namespace, sibling) = original_native02_variables(&definition);
        let evidence = directory.path().join(&namespace);
        fs::create_dir(&evidence).unwrap();
        let root = format!("{}/iru7/{namespace}", variables["iceberg_hms_warehouse"]);
        let ready = json!({"facts":{"first_append":{"snapshot_id":4018151980747658674i64},
            "actual_locations":{"v1":format!("{root}/v1"),"v2":format!("{root}/v2")}}});
        write_once(&evidence.join("input.ready.json"), &ready).unwrap();
        let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
        let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
        assert!(
            definition
                .bind_ready_case(
                    &variables,
                    (&namespace, &sibling),
                    &ready,
                    &evidence,
                    &meta,
                    &marker
                )
                .is_err()
        );
        let attempt = read(&evidence.join("input.binding-attempt.json")).unwrap();
        assert_ne!(attempt["actual_uri"], attempt["expected_uri"]);
        assert!(!evidence.join("input.bound.json").exists());
    }

    #[cfg(unix)]
    mod local_lifecycle {
        use super::*;
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, ScratchIdentity};
        use std::os::unix::fs::{PermissionsExt, symlink};
        use std::sync::Arc;

        // This fixed-path test producer creates only local processes/files.
        // Its SDK/container-shaped facts are explicitly modeled, never proof
        // of Spark, Docker, a real snapshot, or successful native SQL.
        const MODELED_PRODUCER: &str = r#"
import hashlib, json, os, pathlib, shutil, subprocess, sys
root = pathlib.Path(os.environ['NOVAROCKS_WORKSPACE_ROOT'])
action, namespace = sys.argv[1:3]
sibling = sys.argv[sys.argv.index('--new-namespace') + 1]
evidence = root / 'tests/sql/.runtime/hms-evidence' / namespace
intent_path = evidence / 'input.intent.json'
intent = json.loads(intent_path.read_text())
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
assert intent['namespace'] == namespace and intent['new_namespace'] == sibling
assert intent['context'] == json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT'])
storage = json.dumps(json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE']), sort_keys=True, separators=(',', ':'))
assert hashlib.sha256(storage.encode()).hexdigest() == intent['storage_sha256']
for publication in intent['publications'].values():
    assert sha(pathlib.Path(publication['path'])) == publication['sha256']
for path, digest in intent['sources'].items():
    if action == 'cleanup' and path not in ['tests/sql/fixtures/iceberg-hms/fixture.py', 'docker/iceberg-rest/fixture_run.py']:
        continue
    assert sha(root / path) == digest

def write_once(name, value):
    with (evidence / name).open('x') as output:
        os.chmod(output.name, 0o600)
        json.dump(value, output, sort_keys=True)
        output.write('\n')

with (root / 'model-actions.jsonl').open('a') as output:
    output.write(json.dumps({'action': action, 'local_model_only': True, 'pid': os.getpid()}) + '\n')
if action == 'cleanup':
    write_once('input.cleanup-attempt.json', {'schema': 'hms-cleanup-attempt-v1', 'intent_sha256': sha(intent_path)})
mode = os.environ['NOVAROCKS_HMS_LOCAL_TEST_MODE']
code = 'import time; time.sleep(30)' if action == 'prepare' and mode in ['hold', 'hard-death'] else 'raise SystemExit(9)' if action == 'cleanup' and mode == 'cleanup9' else 'pass'
folder = root / ('local-model-owner-' + action)
folder.mkdir()
child = subprocess.Popen([sys.executable, '-c', code], cwd=folder)
write_once(action + '.local-model-child.json', {'local_model_only': True, 'pid': child.pid, 'group': os.getpgrp()})
status = child.wait()
shutil.rmtree(folder)
write_once(action + '.remote.json', {'schema': 'fixture-compose-settlement-v1',
    'local_model_only': True, 'context': intent['context'], 'container_id': 'LOCAL_MODEL_NOT_A_CONTAINER',
    'owner_folder': str(folder), 'payload_sha256': hashlib.sha256(code.encode()).hexdigest(),
    'child_waited': child.returncode is not None, 'returncode': status,
    'cli_returncode': status, 'cleanup_failed': False, 'never_started': False,
    'temporary_owner_removed': not folder.exists()})
if status:
    raise SystemExit(status)
if action == 'prepare':
    for name in [namespace, sibling]:
        owned = root / 'model-owned-data' / name
        owned.mkdir(parents=True)
        (owned / 'local-model-object').write_text('LOCAL_MODEL_ONLY')
    warehouse = intent['context']['identity']['scope_root'] + '/hms'
    table = '00000000-0000-0000-0000-000000000731'
    write_once('input.ready.json', {'schema': 'hms-case-input-ready-v1', 'local_model_only': True,
        'publications': intent['publications'], 'intent_sha256': sha(intent_path),
        'remote_sha256': sha(evidence / 'prepare.remote.json'),
        'facts': {'prepared': namespace, 'first_append': {'snapshot_id': 731, 'timestamp_ms': 1000, 'table_uuid': table},
          'v2_table_uuid': table, 'v2_metadata_location': warehouse + '/local-model-metadata.json',
          'final_snapshots': [{'snapshot_id': 731, 'timestamp_ms': 1000}, {'snapshot_id': 732, 'timestamp_ms': 2000}],
          'actual_locations': {'v1': warehouse + '/iru7/' + namespace + '/v1', 'v2': warehouse + '/iru7/' + namespace + '/v2'}}})
    if mode in ['change-sql', 'change-expected']:
        path = next(path for path in intent['sources'] if path.endswith('.sql' if mode == 'change-sql' else '.result'))
        with (root / path).open('a') as output:
            output.write('\n-- local-model source mutation\n')
elif action == 'cleanup':
    for name in [namespace, sibling]:
        owned = root / 'model-owned-data' / name
        if owned.exists():
            shutil.rmtree(owned)
    remaining = {str(path.relative_to(root)): path.stat().st_size for name in [namespace, sibling]
                 for path in (root / 'model-owned-data' / name).glob('**/*') if path.is_file()}
    write_once('input.cleanup.json', {'schema': 'hms-case-input-cleanup-v1', 'local_model_only': True,
        'namespace': namespace, 'new_namespace': sibling, 'remaining_objects': remaining,
        'intent_sha256': sha(intent_path), 'remote_sha256': sha(evidence / 'cleanup.remote.json')})
else:
    raise AssertionError('unexpected local model action')
"#;

        fn isolated(name: &str, mode: &str, check: impl FnOnce(&LocalModel)) {
            const CHILD: &str = "NOVAROCKS_HMS_LOCAL_TEST_CHILD";
            const ROOT: &str = "NOVAROCKS_HMS_LOCAL_TEST_ROOT";
            if std::env::var(CHILD).as_deref() == Ok(name) {
                let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
                let model = LocalModel::new(root, mode);
                check(&model);
                fs::write(model.root.join("child-completed"), name).unwrap();
                return;
            }
            // Follow the existing CLI-test subprocess isolation pattern. Only
            // this new child receives test publications/PATH; no global ENV
            // mutation occurs in the parallel parent test process.
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            let sql = root.join("tests/sql/correctness/iceberg-hms/sql");
            let expected = root.join("tests/sql/correctness/iceberg-hms/result");
            fs::create_dir_all(&sql).unwrap();
            fs::create_dir_all(&expected).unwrap();
            fs::write(sql.join(format!("{CASE}.sql")), TEMPLATE).unwrap();
            fs::write(
                sql.parent().unwrap().join("suite.toml"),
                include_str!("../../../correctness/iceberg-hms/suite.toml"),
            )
            .unwrap();
            fs::write(expected.join(format!("{CASE}.result")), include_bytes!(
                "../../../correctness/iceberg-hms/result/iceberg_hms_read_only_compatibility.result"
            )).unwrap();
            let producer = root.join(PRODUCER);
            fs::create_dir_all(producer.parent().unwrap()).unwrap();
            fs::write(&producer, MODELED_PRODUCER).unwrap();
            let remote = root.join(REMOTE);
            fs::create_dir_all(remote.parent().unwrap()).unwrap();
            fs::write(
                remote,
                "# Local model control-source identity; never executed.\n",
            )
            .unwrap();
            fs::create_dir(root.join("model-owned-data")).unwrap();
            fs::write(
                root.join("model-owned-data/foreign-marker"),
                "not owned by this case",
            )
            .unwrap();
            let publication = root.join("publications");
            fs::create_dir(&publication).unwrap();
            for (file, body) in [
                ("env.sh", "# local model only\n"),
                (
                    "manifest.json",
                    r#"{"ready":true,"runtime":{"machine":{}}}"#,
                ),
                ("spark.conf", "# local model Spark publication\n"),
                ("hms.conf", "# local model HMS publication\n"),
            ] {
                fs::write(publication.join(file), body).unwrap();
            }
            let python = std::env::split_paths(&std::env::var_os("PATH").unwrap())
                .map(|directory| directory.join("python3"))
                .find(|path| {
                    path.is_file() && fs::metadata(path).unwrap().permissions().mode() & 0o111 != 0
                })
                .expect("local lifecycle tests require the existing Python3 executable");
            let bin = root.join("bin");
            fs::create_dir(&bin).unwrap();
            symlink(fs::canonicalize(python).unwrap(), bin.join("python3")).unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", name, "--nocapture"])
                .env(CHILD, name)
                .env(ROOT, root)
                .env("NOVAROCKS_HMS_LOCAL_TEST_MODE", mode)
                .env("PATH", &bin)
                .env("NOVA_ENV_REST_ENV_FILE", publication.join("env.sh"))
                .env("NOVAROCKS_SPARK_DEFAULTS", publication.join("spark.conf"))
                .env("NOVAROCKS_SPARK_HMS_DEFAULTS", publication.join("hms.conf"))
                .env_remove(RUN_CONTEXT_ENV)
                .env_remove(RUN_STORAGE_ENV)
                .env_remove("NOVA_FIXTURE_CLEANUP_MODE");
            if matches!(mode, "hard-death" | "cleanup-spawn-failure") {
                // These local fault models inspect their own failed actor facts.
                // Keep the isolated child's identity complete and its deliberate
                // startup failure outside the parent nextest execution domain.
                command.env("XDG_STATE_HOME", root.join("state"));
                for key in [
                    "NOVAROCKS_TEST_RUN_ROOT",
                    "NEXTEST",
                    "NEXTEST_RUN_ID",
                    "NEXTEST_BINARY_ID",
                    "NEXTEST_TEST_NAME",
                    "NEXTEST_VERSION",
                ] {
                    command.env_remove(key);
                }
            } else {
                novarocks_test_support::ensure_run_retention(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                )
                .unwrap();
            }
            if mode == "hard-death" {
                observe_hard_caller_death(command, root);
                return;
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                fs::read_to_string(root.join("child-completed")).unwrap(),
                name
            );
        }

        struct LocalModel {
            root: PathBuf,
            sql: PathBuf,
            expected: PathBuf,
            budget: RunBudget,
            storage: RunStorageScope,
            variables: HashMap<String, String>,
            writer: Arc<crate::results_record::ResultsWriter>,
        }
        impl LocalModel {
            fn new(root: PathBuf, mode: &str) -> Self {
                let policy = FixturePolicy {
                    scratch_ttl_seconds: 120,
                    max_run_seconds: 30,
                    cleanup_grace_seconds: 2,
                };
                let budget = RunBudget::admit(
                    policy,
                    ScratchIdentity::create("s3://novarocks/hms-local-model").unwrap(),
                    if matches!(mode, "hold" | "hard-death") {
                        6
                    } else {
                        30
                    },
                    None,
                )
                .unwrap();
                let vectors: Value = serde_json::from_str(include_str!(
                    "../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
                ))
                .unwrap();
                let storage =
                    RunStorageScope::from_record(budget.identity(), &vectors["record"]).unwrap();
                let sql = root.join(format!("tests/sql/correctness/{SUITE}/sql/{CASE}.sql"));
                let expected = root.join(format!(
                    "tests/sql/correctness/{SUITE}/result/{CASE}.result"
                ));
                let manifest = crate::suite_manifest::SuiteManifest::parse(include_str!(
                    "../../../correctness/iceberg-hms/suite.toml"
                ))
                .unwrap();
                let suite = crate::types::SuiteConfig {
                    name: SUITE.into(),
                    sql_dir: sql.parent().unwrap().into(),
                    result_dir: Some(expected.parent().unwrap().into()),
                    sql_glob: "*.sql".into(),
                    default_catalog: "default_catalog".into(),
                    default_db: String::new(),
                    auto_case_db: false,
                    verify_default: true,
                    init_sql: None,
                    cleanup_sql: None,
                    manifest,
                };
                let suites = BTreeMap::from([(SUITE.into(), suite)]);
                let sources = BTreeMap::from([(SUITE.into(), vec![sql.clone()])]);
                let units = BTreeSet::from([(SUITE.into(), CASE.into())]);
                let mut names = crate::fixture_context::RunNames::prepare_with_case_inputs(
                    &budget, &sources, &units,
                )
                .unwrap();
                let mut config = crate::types::RunnerConfig {
                    values: crate::extension_manifest::literal_placeholder_variables(TEMPLATE)
                        .into_iter()
                        .map(|(key, value)| {
                            let value = if key == ID || key == RECEIPT {
                                value
                            } else {
                                "LOCAL_MODEL_VALUE".into()
                            };
                            (key, value)
                        })
                        .collect(),
                    ..Default::default()
                };
                config.values.insert(
                    "fixture_env_file".into(),
                    root.join("publications/env.sh").to_str().unwrap().into(),
                );
                config.values.insert(
                    "iceberg_hms_uri".into(),
                    "thrift://local-model.invalid:9083".into(),
                );
                config.values.insert(
                    "iceberg_hms_warehouse".into(),
                    format!("{}/hms", budget.identity().scope_root),
                );
                names.seal_catalogs(&config, &sources, &suites).unwrap();
                let mut variables = config.values;
                names.extend_suite_variables(SUITE, &mut variables).unwrap();
                let writer_path = root.join("model-native.jsonl");
                let writer =
                    crate::results_record::ResultsWriter::create(&root, writer_path.to_str())
                        .unwrap();
                Self {
                    root,
                    sql,
                    expected,
                    budget,
                    storage,
                    variables,
                    writer,
                }
            }
            fn regexes(&self) -> (regex::Regex, regex::Regex) {
                (
                    regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap(),
                    regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap(),
                )
            }
            fn register(&self) -> Custody<'_> {
                let (meta, marker) = self.regexes();
                let definition = Definition::load(&self.sql, &meta, &marker).unwrap();
                Custody::register(
                    &self.root,
                    definition,
                    &self.variables,
                    Registration {
                        manifest: self
                            .sql
                            .parent()
                            .unwrap()
                            .parent()
                            .unwrap()
                            .join("suite.toml"),
                        expected: self.expected.clone(),
                        budget: &self.budget,
                        storage: &self.storage,
                        invocation: json!({"local_model_only":true}),
                    },
                )
                .unwrap()
            }
            fn prepare(&self, owner: &mut Custody<'_>) -> Result<crate::types::SqlCase> {
                let (meta, marker) = self.regexes();
                owner.prepare_and_bind(&meta, &marker, &self.variables, self.writer.as_ref())
            }
            fn assert_data_retained(&self, owner: &Custody<'_>) {
                assert!(
                    self.root
                        .join("model-owned-data")
                        .join(&owner.namespace)
                        .join("local-model-object")
                        .is_file()
                );
                assert!(self.root.join("model-owned-data/foreign-marker").is_file());
            }
            fn assert_owned_data_removed(&self, owner: &Custody<'_>) {
                assert!(
                    !self
                        .root
                        .join("model-owned-data")
                        .join(&owner.namespace)
                        .exists()
                );
                assert!(
                    !self
                        .root
                        .join("model-owned-data")
                        .join(&owner.sibling)
                        .exists()
                );
                assert!(self.root.join("model-owned-data/foreign-marker").is_file());
            }
            fn actions(&self, action: &str) -> usize {
                fs::read_to_string(self.root.join("model-actions.jsonl"))
                    .unwrap_or_default()
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .filter(|value| value["action"] == action)
                    .count()
            }
            fn assert_local_final(
                &self,
                owner: &Custody<'_>,
                action: &str,
                success: bool,
            ) -> Value {
                let local = read(&owner.directory.join(format!("{action}.local.json"))).unwrap();
                assert_eq!(local["group_exit_observed"], true, "{local}");
                if success {
                    assert_eq!(local["actual_exit"], 0);
                    assert_eq!(local["failures"], json!([]));
                    assert_eq!(local["timed_out"], false);
                }
                let root = Path::new(local["diagnostic_root"].as_str().unwrap());
                let events = fs::read_to_string(root.join("record.jsonl")).unwrap();
                let records = events
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>();
                assert!(
                    records
                        .iter()
                        .any(|record| record["event"] == "ExitObserved"),
                    "{events}"
                );
                let spawned = records
                    .iter()
                    .find(|record| record["event"] == "Spawned")
                    .unwrap();
                let child = read(
                    &owner
                        .directory
                        .join(format!("{action}.local-model-child.json")),
                )
                .unwrap();
                assert_eq!(child["local_model_only"], true);
                assert_eq!(child["group"], spawned["pid"]);
                assert!(child["pid"].as_u64().is_some_and(|pid| pid > 0));
                assert_ne!(
                    child["pid"], spawned["pid"],
                    "a real separate descendant must be observed"
                );
                let final_fact = records
                    .iter()
                    .find(|record| record["event"] == "Final")
                    .unwrap()
                    .clone();
                assert_eq!(final_fact["group_exit_observed"], true);
                assert_eq!(final_fact["cleanup_complete"], true);
                final_fact
            }
        }

        #[test]
        fn local_custody_prepares_binds_and_finalizes_unused_once() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_prepares_binds_and_finalizes_unused_once",
                "ready",
                |model| {
                    let mut owner = model.register();
                    assert_eq!(model.actions("prepare"), 0);
                    let case = model.prepare(&mut owner).unwrap();
                    assert_eq!(case.steps.len(), 59);
                    assert_eq!(
                        case.steps[10].meta.result_rows_where,
                        vec![("snapshot_id".into(), "731".into())]
                    );
                    assert_eq!(case.steps[10].meta.result_rows_count, Some(1));
                    let ready = read(&owner.directory.join("input.ready.json")).unwrap();
                    let bound = read(&owner.directory.join("input.bound.json")).unwrap();
                    assert_eq!(ready["local_model_only"], true);
                    assert_eq!(bound["snapshot_id"], 731);
                    model.assert_local_final(&owner, "prepare", true);
                    model.assert_data_retained(&owner);
                    assert_eq!(model.actions("prepare"), 1);
                    // No Server, session or SQL consumer is constructed anywhere
                    // in this local scope; only the actually joined model child.
                    assert_eq!(owner.finish(true).unwrap()["status"], "closed");
                    model.assert_local_final(&owner, "cleanup", true);
                    model.assert_owned_data_removed(&owner);
                    assert_eq!(
                        read(&owner.directory.join("input.cleanup.json")).unwrap()["remaining_objects"],
                        json!({})
                    );
                    assert_eq!(model.actions("cleanup"), 1);
                    assert!(
                        owner
                            .finish(true)
                            .unwrap_err()
                            .to_string()
                            .contains("already attempted")
                    );
                    assert_eq!(model.actions("cleanup"), 1);
                },
            );
        }

        fn changed_input_is_rejected(model: &LocalModel) {
            let mut owner = model.register();
            let error = model.prepare(&mut owner).unwrap_err();
            assert!(
                format!("{error:#}").contains("source changed during preparation"),
                "{error:#}"
            );
            assert!(!owner.directory.join("input.bound.json").exists());
            model.assert_local_final(&owner, "prepare", true);
            assert_eq!(owner.finish(true).unwrap()["status"], "closed");
            model.assert_local_final(&owner, "cleanup", true);
            model.assert_owned_data_removed(&owner);
            assert_eq!(model.actions("prepare"), 1);
            assert_eq!(model.actions("cleanup"), 1);
        }
        #[test]
        fn local_custody_changed_expected_is_rejected_and_owned_cleanup_runs() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_changed_expected_is_rejected_and_owned_cleanup_runs",
                "change-expected",
                changed_input_is_rejected,
            );
        }
        #[test]
        fn local_custody_changed_source_is_rejected_and_owned_cleanup_runs() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_changed_source_is_rejected_and_owned_cleanup_runs",
                "change-sql",
                changed_input_is_rejected,
            );
        }
        #[test]
        fn local_custody_work_timeout_observes_group_and_retains_unknown() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_work_timeout_observes_group_and_retains_unknown",
                "hold",
                |model| {
                    let mut owner = model.register();
                    assert!(model.prepare(&mut owner).is_err());
                    assert_eq!(model.actions("prepare"), 1);
                    assert!(
                        owner
                            .directory
                            .join("prepare.local-model-child.json")
                            .is_file(),
                        "a real descendant must have started"
                    );
                    let final_fact = model.assert_local_final(&owner, "prepare", false);
                    assert_eq!(final_fact["lifetime_expired"], true);
                    assert!(!owner.directory.join("prepare.remote.json").exists());
                    assert!(!owner.directory.join("input.ready.json").exists());
                    // Host group exit cannot substitute for missing remote proof.
                    assert!(owner.finish(true).is_err());
                    assert!(!owner.finished);
                    assert_eq!(model.actions("cleanup"), 0);
                    assert!(owner.directory.join("input.intent.json").exists());
                },
            );
        }
        #[test]
        fn local_custody_cleanup_nonzero_cannot_dispatch_twice() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_cleanup_nonzero_cannot_dispatch_twice",
                "cleanup9",
                |model| {
                    let mut owner = model.register();
                    model.prepare(&mut owner).unwrap();
                    model.assert_local_final(&owner, "prepare", true);
                    model.assert_data_retained(&owner);
                    assert!(owner.finish(true).is_err());
                    model.assert_local_final(&owner, "cleanup", false);
                    assert_eq!(
                        read(&owner.directory.join("cleanup.local.json")).unwrap()["actual_exit"],
                        9
                    );
                    assert!(
                        owner
                            .directory
                            .join("input.tail-cleanup-attempt.json")
                            .is_file()
                    );
                    assert!(!owner.directory.join("input.cleanup.json").exists());
                    assert_eq!(model.actions("cleanup"), 1);
                    assert!(owner.finish(true).is_err());
                    assert_eq!(model.actions("cleanup"), 1);
                    assert!(!owner.finished);
                    model.assert_data_retained(&owner);
                },
            );
        }
        #[test]
        fn local_custody_cleanup_spawn_failure_records_attempt_before_launch() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_cleanup_spawn_failure_records_attempt_before_launch",
                "cleanup-spawn-failure",
                |model| {
                    let mut owner = model.register();
                    model.prepare(&mut owner).unwrap();
                    model.assert_local_final(&owner, "prepare", true);
                    model.assert_data_retained(&owner);
                    fs::remove_file(model.root.join("bin/python3")).unwrap();
                    assert!(owner.finish(true).is_err());
                    let attempt =
                        read(&owner.directory.join("input.tail-cleanup-attempt.json")).unwrap();
                    assert_eq!(attempt["phase"], "before-managed-launch");
                    assert!(!owner.directory.join("input.cleanup-attempt.json").exists());
                    assert_eq!(model.actions("cleanup"), 0);
                    assert!(owner.finish(true).is_err());
                    assert_eq!(model.actions("cleanup"), 0);
                    assert!(!owner.finished);
                    model.assert_data_retained(&owner);
                    let mut records = vec![];
                    monitor_records(&model.root.join("state/novarocks/test-runs"), &mut records);
                    let failed_finals = records
                        .iter()
                        .flat_map(|path| {
                            fs::read_to_string(path)
                                .unwrap()
                                .lines()
                                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                                .collect::<Vec<_>>()
                        })
                        .filter(|event| {
                            event["event"] == "Final" && event["startup_error"].is_string()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(failed_finals.len(), 1);
                    let failed = &failed_finals[0];
                    assert!(failed["status_raw"].is_null());
                    assert_eq!(failed["owner_lost"], false);
                    assert_eq!(failed["cleanup_complete"], true);
                    assert_eq!(failed["errors"], json!([]));
                },
            );
        }

        #[test]
        fn local_custody_rejects_foreign_or_incomplete_ready_without_changing_authority() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_rejects_foreign_or_incomplete_ready_without_changing_authority",
                "ready",
                |model| {
                    let mut owner = model.register();
                    model.prepare(&mut owner).unwrap();
                    model.assert_local_final(&owner, "prepare", true);
                    let original_intent =
                        fs::read(owner.directory.join("input.intent.json")).unwrap();
                    let original_ready =
                        fs::read(owner.directory.join("input.ready.json")).unwrap();
                    let ready: Value = serde_json::from_slice(&original_ready).unwrap();
                    owner.check_ready(&ready).unwrap();
                    let publication = ready["publications"]
                        .as_object()
                        .unwrap()
                        .keys()
                        .next()
                        .unwrap();
                    let cases = [
                        ("/schema".into(), json!("foreign-schema")),
                        ("/intent_sha256".into(), json!("foreign-intent")),
                        ("/remote_sha256".into(), json!("foreign-remote")),
                        (
                            format!("/publications/{publication}/path"),
                            json!(model.root.join("foreign-receipt.json")),
                        ),
                        (
                            format!("/publications/{publication}/sha256"),
                            json!("foreign-publication"),
                        ),
                        ("/facts/prepared".into(), json!("foreign_namespace")),
                        ("/facts/first_append/table_uuid".into(), json!("")),
                        ("/facts/v2_table_uuid".into(), json!("different-table")),
                        ("/facts/v2_metadata_location".into(), json!("")),
                        ("/facts/first_append/timestamp_ms".into(), Value::Null),
                        ("/facts/final_snapshots".into(), json!([])),
                        (
                            "/facts/final_snapshots".into(),
                            json!([{"snapshot_id":731,"timestamp_ms":1000},{"snapshot_id":732,"timestamp_ms":1000}]),
                        ),
                        (
                            "/facts/final_snapshots".into(),
                            json!([{"snapshot_id":731,"timestamp_ms":1000},{"snapshot_id":732,"timestamp_ms":999}]),
                        ),
                        (
                            "/facts/final_snapshots".into(),
                            json!([{"snapshot_id":999,"timestamp_ms":1000}]),
                        ),
                    ];
                    for (pointer, value) in cases {
                        let mut rejected = ready.clone();
                        *rejected.pointer_mut(&pointer).unwrap() = value;
                        assert!(
                            owner.check_ready(&rejected).is_err(),
                            "accepted {pointer}: {rejected}"
                        );
                    }
                    let (meta, marker) = model.regexes();
                    let binding_cases = [
                        ("/facts/first_append/snapshot_id", Value::Null),
                        ("/facts/first_append/snapshot_id", json!(0)),
                        ("/facts/first_append/snapshot_id", json!(-1)),
                        ("/facts/first_append/snapshot_id", json!("731")),
                        ("/facts/first_append/snapshot_id", json!(731.5)),
                        ("/facts/first_append/snapshot_id", json!(u64::MAX)),
                        (
                            "/facts/actual_locations/v1",
                            json!("s3://foreign/namespace/v1"),
                        ),
                        (
                            "/facts/actual_locations/v2",
                            json!("s3://foreign/namespace/v2"),
                        ),
                    ];
                    for (index, (pointer, value)) in binding_cases.into_iter().enumerate() {
                        let mut rejected = ready.clone();
                        *rejected.pointer_mut(pointer).unwrap() = value;
                        let directory = model.root.join(format!("pure-rejected-binding-{index}"));
                        fs::create_dir(&directory).unwrap();
                        fs::write(
                            directory.join("input.ready.json"),
                            serde_json::to_vec(&rejected).unwrap(),
                        )
                        .unwrap();
                        assert!(
                            owner
                                .definition
                                .bind_ready_case(
                                    &model.variables,
                                    (&owner.namespace, &owner.sibling),
                                    &rejected,
                                    &directory,
                                    &meta,
                                    &marker
                                )
                                .is_err(),
                            "accepted {pointer}: {rejected}"
                        );
                        assert!(!directory.join("input.bound.json").exists());
                    }
                    assert_eq!(
                        fs::read(owner.directory.join("input.intent.json")).unwrap(),
                        original_intent
                    );
                    assert_eq!(
                        fs::read(owner.directory.join("input.ready.json")).unwrap(),
                        original_ready
                    );
                    // Rejected copied facts never become cleanup authority. The
                    // original joined producer and original in-memory pair remain.
                    assert_eq!(owner.finish(true).unwrap()["status"], "closed");
                    model.assert_local_final(&owner, "cleanup", true);
                    model.assert_owned_data_removed(&owner);
                    assert_eq!(model.actions("prepare"), 1);
                    assert_eq!(model.actions("cleanup"), 1);
                },
            );
        }

        #[test]
        fn local_custody_rejects_foreign_remote_exit_and_retains_original_unknown() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_rejects_foreign_remote_exit_and_retains_original_unknown",
                "ready",
                |model| {
                    let mut owner = model.register();
                    model.prepare(&mut owner).unwrap();
                    model.assert_local_final(&owner, "prepare", true);
                    let intent = fs::read(owner.directory.join("input.intent.json")).unwrap();
                    let remote_path = owner.directory.join("prepare.remote.json");
                    let original_remote = fs::read(&remote_path).unwrap();
                    fs::write(
                        owner
                            .directory
                            .join("prepare.remote.original-model-preimage.json"),
                        &original_remote,
                    )
                    .unwrap();
                    let remote: Value = serde_json::from_slice(&original_remote).unwrap();
                    let cases = [
                        ("/schema", json!("wrong-settlement")),
                        ("/container_id", json!("")),
                        ("/owner_folder", json!("")),
                        ("/payload_sha256", json!("short")),
                        ("/payload_sha256", json!("z".repeat(64))),
                        ("/child_waited", json!(false)),
                        ("/cli_returncode", json!(9)),
                        ("/cleanup_failed", json!(true)),
                        ("/temporary_owner_removed", json!(false)),
                        ("/context/identity/run_id", json!("foreign-run")),
                        ("/context/identity/scope_root", json!("s3://foreign/scope")),
                        ("/context/deadline_unix_ms", json!(0)),
                    ];
                    for (pointer, value) in cases {
                        let mut rejected = remote.clone();
                        *rejected.pointer_mut(pointer).unwrap() = value;
                        fs::write(&remote_path, serde_json::to_vec(&rejected).unwrap()).unwrap();
                        assert!(
                            owner.remote_exited("prepare").is_err(),
                            "accepted {pointer}: {rejected}"
                        );
                        assert_eq!(model.actions("cleanup"), 0);
                    }
                    // Keep the final invalid original-model receipt in place.
                    // A group-exit fact does not permit cleanup under foreign
                    // context, and no restoration or replacement run is attempted.
                    assert!(owner.finish(true).is_err());
                    assert!(!owner.finished);
                    assert_eq!(
                        fs::read(owner.directory.join("input.intent.json")).unwrap(),
                        intent
                    );
                    assert!(
                        !owner
                            .directory
                            .join("input.tail-cleanup-attempt.json")
                            .exists()
                    );
                    assert!(owner.finish(true).is_err());
                    assert_eq!(model.actions("prepare"), 1);
                    assert_eq!(model.actions("cleanup"), 0);
                    model.assert_data_retained(&owner);
                },
            );
        }

        struct NeverStartedServer;
        impl crate::cluster::TestClusterOwner for NeverStartedServer {
            fn target_host(&self) -> Option<&str> {
                panic!("aborted case must not access a Server")
            }
            fn target_port(&self) -> Option<u16> {
                panic!("aborted case must not access a Server")
            }
        }

        #[test]
        fn local_custody_survives_real_case_abort_and_captured_unwind() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_survives_real_case_abort_and_captured_unwind",
                "ready",
                |model| {
                    let mut owner = model.register();
                    let case = model.prepare(&mut owner).unwrap();
                    model.assert_local_final(&owner, "prepare", true);
                    let intent = fs::read(owner.directory.join("input.intent.json")).unwrap();
                    let connection = crate::types::ConnectionConfig {
                        time_zone: "UTC".into(),
                        host: "must-not-connect.invalid".into(),
                        port: "1".into(),
                        user: "test".into(),
                        catalog: None,
                        db: None,
                    };
                    let context = crate::SuiteRunContext {
                        fixture_budget: model.budget.clone(),
                        fixture_storage: Some(model.storage.clone()),
                        fixture_writers_settled: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                        namespace_catalogs: crate::namespace_guard::CatalogFacts::default(),
                        suite_name: SUITE.into(),
                        controls: None,
                        mode: crate::Mode::Verify,
                        convert_legacy: false,
                        legacy_variant_offset_seconds: None,
                        target_conn_base: connection.clone(),
                        target_admin_conn: connection,
                        result_dir: Some(model.expected.parent().unwrap().into()),
                        actual_artifact_dir: None,
                        verify_enabled: true,
                        query_timeout: 1,
                        auto_case_db: false,
                        order_sensitive_default: false,
                        float_epsilon: None,
                        preview_lines: 0,
                        update_expected: false,
                        session_defaults: crate::suite_manifest::SuiteSessionDefaults::default(),
                        case_input: Some(
                            crate::suite_manifest::CaseInputRecipe::HmsReadOnlyCompatibilityV1,
                        ),
                        target_session_sql: vec![],
                        rewrite_explain_contains_as_not_contains: vec![],
                        fail_fast: true,
                        cluster_owner: Arc::new(std::sync::Mutex::new(Box::new(
                            NeverStartedServer,
                        ))),
                        failure_artifacts: Arc::new(
                            crate::failure_artifacts::FailureArtifactRecorder::new(
                                crate::failure_artifacts::FailureArtifactContext {
                                    root: model.root.join("unused-failure-artifacts"),
                                    lane: "local-model".into(),
                                    suites: vec![SUITE.into()],
                                },
                            ),
                        ),
                        publication_catalog_control: None,
                        publication_service_control: None,
                        results: None,
                    };
                    let mut evidence = crate::results_record::CaseEvidence::default();
                    let captured = capture_unwind(|| {
                        let aborted = std::sync::atomic::AtomicBool::new(true);
                        let outcome =
                            crate::run_case_observed(&context, &case, &aborted, &mut evidence);
                        assert_eq!(outcome.status, crate::CaseStatus::Skipped);
                        assert!(outcome.log.is_empty());
                        panic!("intentional local custody unwind after actual aborted case");
                    });
                    let payload = captured.unwrap_err();
                    assert_eq!(
                        payload.downcast_ref::<&str>().copied(),
                        Some("intentional local custody unwind after actual aborted case")
                    );
                    assert!(evidence.executed.is_empty());
                    assert!(evidence.failure.is_none());
                    assert_eq!(
                        fs::read(owner.directory.join("input.intent.json")).unwrap(),
                        intent
                    );
                    // No actual Server/session/consumer was created; the original
                    // managed producer has already exited. The owner stayed
                    // outside the captured closure and now follows the tail.
                    assert_eq!(owner.finish(true).unwrap()["status"], "closed");
                    model.assert_local_final(&owner, "cleanup", true);
                    model.assert_owned_data_removed(&owner);
                    assert_eq!(model.actions("cleanup"), 1);
                },
            );
        }

        fn read_json_when_present(path: &Path, deadline: std::time::Instant) -> Value {
            loop {
                if let Ok(bytes) = fs::read(path)
                    && let Ok(value) = serde_json::from_slice(&bytes)
                {
                    return value;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "original observation deadline expired: {}",
                    path.display()
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        fn monitor_records(directory: &Path, output: &mut Vec<PathBuf>) {
            if !directory.is_dir() {
                return;
            }
            for entry in fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    monitor_records(&path, output);
                } else if path.file_name().is_some_and(|name| name == "record.jsonl") {
                    output.push(path);
                }
            }
        }

        fn observe_hard_caller_death(mut command: std::process::Command, root: &Path) {
            struct Caller(Option<std::process::Child>);
            impl Drop for Caller {
                fn drop(&mut self) {
                    if let Some(child) = &mut self.0 {
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                }
            }
            let child = command
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let mut caller = Caller(Some(child));
            let owner_path = root.join("hard-death-owner.json");
            let owner = read_json_when_present(
                &owner_path,
                std::time::Instant::now() + std::time::Duration::from_secs(5),
            );
            let deadline_ms = owner["context"]["deadline_unix_ms"].as_u64().unwrap();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(deadline_ms.saturating_sub(now_ms));
            let directory = PathBuf::from(owner["directory"].as_str().unwrap());
            let intent_path = directory.join("input.intent.json");
            let intent_bytes = fs::read(&intent_path).unwrap();
            let intent: Value = serde_json::from_slice(&intent_bytes).unwrap();
            assert_eq!(intent["context"], owner["context"]);
            let descendant =
                read_json_when_present(&directory.join("prepare.local-model-child.json"), deadline);
            assert!(descendant["pid"].as_u64().is_some_and(|pid| pid > 0));
            assert_ne!(
                descendant["pid"], descendant["group"],
                "an actual separate descendant must be observed"
            );
            let record = loop {
                let mut records = vec![];
                monitor_records(&root.join("state/novarocks/test-runs"), &mut records);
                if let Some(path) = records.into_iter().find(|path| {
                    fs::read_to_string(path)
                        .unwrap()
                        .lines()
                        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                        .any(|event| {
                            event["event"] == "Spawned" && event["pid"] == descendant["group"]
                        })
                }) {
                    break path;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "original managed actor registration not observed"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            write_once(&root.join("hard-death-observed-original-actor.json"), &json!({"record":record,"caller_pid":caller.0.as_ref().unwrap().id(),"actual_descendant":descendant,"original_context":owner["context"],"intent_sha256":crate::results_record::hash_file(&intent_path).unwrap()})).unwrap();
            caller.0.as_mut().unwrap().kill().unwrap();
            let output = caller.0.take().unwrap().wait_with_output().unwrap();
            assert!(!output.status.success());
            let final_fact = loop {
                let text = fs::read_to_string(&record).unwrap();
                if let Some(final_fact) = text
                    .lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .find(|event| event["event"] == "Final")
                {
                    break final_fact;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "original monitor did not publish actual Final: {text}"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            assert_eq!(final_fact["owner_lost"], true);
            assert_eq!(final_fact["group_exit_observed"], true);
            assert_eq!(final_fact["cleanup_complete"], true);
            assert_eq!(fs::read(&intent_path).unwrap(), intent_bytes);
            assert!(!directory.join("prepare.remote.json").exists());
            assert!(!directory.join("input.ready.json").exists());
            assert!(!directory.join("input.bound.json").exists());
            assert!(!directory.join("input.cleanup.json").exists());
            assert!(!directory.join("input.tail-cleanup-attempt.json").exists());
            assert!(!root.join("child-completed").exists());
            let actions = fs::read_to_string(root.join("model-actions.jsonl")).unwrap();
            assert_eq!(actions.lines().count(), 1);
            assert_eq!(
                serde_json::from_str::<Value>(actions.lines().next().unwrap()).unwrap()["action"],
                "prepare"
            );
            // Hard caller death never runs Custody::Drop or remote cleanup.
            // Retained context/intent and actual monitor Final stay distinct;
            // no remote exit or fixture-complete fact is fabricated.
        }

        #[test]
        fn local_custody_hard_caller_death_retains_original_intent_and_actor() {
            isolated(
                "hms_case_input::tests::local_lifecycle::local_custody_hard_caller_death_retains_original_intent_and_actor",
                "hard-death",
                |model| {
                    let mut owner = model.register();
                    write_once(&model.root.join("hard-death-owner.json"), &json!({"directory":owner.directory,"context":serde_json::from_str::<Value>(&model.budget.to_json().unwrap()).unwrap()})).unwrap();
                    let _ = model.prepare(&mut owner);
                    panic!("parent must kill the exact caller while its managed child is held");
                },
            );
        }
    }
}
