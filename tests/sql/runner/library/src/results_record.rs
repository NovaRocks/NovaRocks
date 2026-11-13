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

//! Append-only execution facts. Interpretation belongs to the verification consumer.
use crate::suite_manifest::{EnvInputKind, SuiteManifest};
use crate::types::{SqlCase, SqlStep, SuiteConfig};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);
pub(crate) fn now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("read input {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub(crate) fn optional_hash(path: Option<&Path>) -> Result<Option<String>> {
    path.filter(|p| p.exists()).map(hash_file).transpose()
}
pub(crate) fn environment(manifest: &SuiteManifest) -> Result<Value> {
    let mut env = BTreeMap::new();
    for input in manifest.reuse.env_inputs.iter().flatten() {
        let value = match std::env::var_os(&input.name) {
            None => "unset".to_owned(),
            Some(value) => match input.kind {
                EnvInputKind::Value => digest(value.as_encoded_bytes()),
                EnvInputKind::FileContent => hash_file(Path::new(&value))?,
            },
        };
        env.insert(input.name.clone(), value);
    }
    Ok(serde_json::to_value(env)?)
}

pub(crate) struct ResultsWriter {
    path: PathBuf,
    file: Mutex<File>,
    error: Mutex<Option<String>>,
    input_files: Mutex<BTreeMap<PathBuf, String>>,
}
impl ResultsWriter {
    pub fn create(root: &Path, explicit: Option<&str>) -> Result<Arc<Self>> {
        let path = explicit.map(PathBuf::from).unwrap_or_else(|| {
            root.join("logs/sql-test-results").join(format!(
                "sql-results-{:020}-{}-{:04}.jsonl",
                now().parse::<u128>().unwrap(),
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ))
        });
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(parent)?;
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("create SQL results {}", path.display()))?;
        eprintln!("sql results: {}", path.display());
        Ok(Arc::new(Self {
            path,
            file: Mutex::new(file),
            error: Mutex::new(None),
            input_files: Mutex::new(BTreeMap::new()),
        }))
    }
    pub fn capture_configuration(
        &self,
        cli: &crate::Cli,
        root: &Path,
        config: &crate::types::RunnerConfig,
    ) -> Result<PathBuf> {
        let mut files = BTreeMap::new();
        if let Some(path) = &config.path {
            files.insert("runner", path.clone());
        }
        if cli.cluster_mode == crate::ClusterMode::CrossProcess && !cli.dry_run {
            files.insert(
                "frontend",
                crate::cluster::resolve_base_frontend_config_path(root, config)?,
            );
            if let Some(path) = std::env::var_os("NOVAROCKS_BE_CONFIG") {
                files.insert("backend", PathBuf::from(path));
            }
        }
        let mut source_hashes = BTreeMap::new();
        let mut env = BTreeMap::new();
        let reference = regex::Regex::new(r"\$\{ENV:([A-Za-z_][A-Za-z0-9_]*)\}")?;
        for (role, path) in &files {
            source_hashes.insert(*role, self.track_file(path)?);
            for capture in reference.captures_iter(&fs::read_to_string(path)?) {
                let name = &capture[1];
                env.insert(
                    name.to_owned(),
                    std::env::var_os(name)
                        .map(|v| digest(v.as_encoded_bytes()))
                        .unwrap_or_else(|| "unset".into()),
                );
            }
        }
        for name in [
            "NOVAROCKS_SQL_TEST_HOST",
            "NOVAROCKS_SQL_TEST_TRINO_PORT",
            "NOVAROCKS_SQL_TEST_USER",
            "NOVAROCKS_SQL_TEST_TIMEOUT",
            "NOVAROCKS_STARTUP_TIMEOUT_SECS",
        ] {
            env.insert(
                name.to_owned(),
                std::env::var_os(name)
                    .map(|v| digest(v.as_encoded_bytes()))
                    .unwrap_or_else(|| "unset".into()),
            );
        }
        let configured_values = config
            .values
            .iter()
            .map(|(k, v)| (k, digest(v.as_bytes())))
            .collect::<BTreeMap<_, _>>();
        let cluster_values = config
            .cluster
            .iter()
            .map(|(k, v)| (k, digest(v.as_bytes())))
            .collect::<BTreeMap<_, _>>();
        let corpus_path = root.join("tests/sql/correctness/corpus.toml");
        let corpus_session = crate::config::load_corpus_session(root)?;
        source_hashes.insert("corpus_session", self.track_file(&corpus_path)?);
        let value = json!({"schema":1,"source_hashes":source_hashes,"configured_values":configured_values,"cluster_values":cluster_values,"environment":env,
            "client_capabilities":novarocks_test_trino_client::CLIENT_CAPABILITIES,
            "corpus_session":corpus_session,
            "cli":{"host":cli.host,"trino_port":cli.trino_port,"user":cli.user,
                "convert_legacy":cli.convert_legacy,"legacy_variant_offset_seconds":cli.legacy_variant_offset_seconds}});
        let path = self.path.with_extension("effective-config.json");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path)?;
        serde_json::to_writer(&mut file, &value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        self.track_file(&path)?;
        Ok(path)
    }
    pub fn track_file(&self, path: &Path) -> Result<String> {
        let digest = hash_file(path)?;
        let mut inputs = self.input_files.lock().unwrap();
        if let Some(previous) = inputs.get(path) {
            anyhow::ensure!(
                *previous == digest,
                "execution input {} changed before launch",
                path.display()
            );
        } else {
            inputs.insert(path.to_path_buf(), digest.clone());
        }
        Ok(digest)
    }
    fn inputs_stable(&self) -> bool {
        self.input_files
            .lock()
            .unwrap()
            .iter()
            .all(|(path, digest)| hash_file(path).is_ok_and(|value| value == *digest))
    }
    pub fn write(&self, record: Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let mut file = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("SQL results lock poisoned"))?;
        file.write_all(&bytes)?;
        file.flush()?;
        if matches!(
            record["kind"].as_str(),
            Some("suite" | "server" | "fixture" | "end")
        ) {
            file.sync_all()?;
        }
        Ok(())
    }
    /// Workers cannot return an I/O error through a CaseOutcome. Retain it and
    /// fail the invocation before its end record instead of claiming complete evidence.
    pub fn observe(&self, record: Value) {
        if let Err(error) = self.write(record) {
            self.error
                .lock()
                .unwrap()
                .get_or_insert_with(|| format!("{error:#}"));
        }
    }
    pub fn check(&self) -> Result<()> {
        let error = self.error.lock().unwrap().clone();
        anyhow::ensure!(error.is_none(), "SQL results write failed: {error:?}");
        Ok(())
    }
}

pub(crate) fn suite_inputs(suite: &SuiteConfig) -> Result<Value> {
    let manifest_path = suite
        .sql_dir
        .parent()
        .context("suite directory has no parent")?
        .join("suite.toml");
    let cases = crate::config::list_sql_files(&suite.sql_dir, &suite.sql_glob)?.into_iter().map(|source| {
        let id = source.file_stem().and_then(|s| s.to_str()).context("invalid SQL case name")?.to_owned();
        let expected = suite.result_dir.as_ref().map(|dir| crate::results::case_result_path(dir, &id));
        Ok(json!({"case":id,"source":source,"source_sha256":hash_file(&source).ok(),"expected":expected,
            "expected_sha256":optional_hash(expected.as_deref())?}))
    }).collect::<Result<Vec<Value>>>()?;
    let mut facts = json!({"manifest_sha256":optional_hash(Some(&manifest_path))?,
        "init_sha256":optional_hash(suite.init_sql.as_deref())?, "cleanup_sha256":optional_hash(suite.cleanup_sql.as_deref())?,
        "env":environment(&suite.manifest)?,"declaration":suite.manifest.reuse,"cases":cases});
    if !suite.manifest.session.is_empty() {
        facts["session"] = json!(suite.manifest.session);
    }
    let mut value = facts.clone();
    value["inputs_sha256"] = json!(digest(&serde_json::to_vec(&facts)?));
    value["suite"] = json!(suite.name);
    Ok(value)
}

/// Every native SQL suite consumes this runner and its shared launch/control
/// mechanics, independently of the suite's engine-specific assertions.
fn native_suite_impact(manifest: &SuiteManifest) -> crate::suite_manifest::ImpactDeclaration {
    let mut impact = manifest.impact.clone();
    for path in [
        "tests/sql/runner/**",
        "tests/trino-client/**",
        "tests/cluster-harness/**",
        "tests/test-support/**",
        "novarocks/failpoint/**",
    ] {
        if !impact.paths.iter().any(|declared| declared == path) {
            impact.paths.push(path.to_owned());
        }
    }
    impact
}

pub(crate) fn declarations(suites: &BTreeMap<String, SuiteConfig>) -> Result<Value> {
    let entries = suites
        .values()
        .map(|suite| {
            let mut facts = suite_inputs(suite)?;
            facts["name"] = json!(suite.name);
            facts["explicit_only"] = json!(suite.manifest.explicit_only);
            facts["disabled_reason"] = json!(suite.manifest.disabled_reason);
            facts["heavy"] = json!(suite.manifest.heavy);
            facts["prepared_dataset"] = json!(suite.manifest.prepared_dataset);
            if !suite.manifest.reference_inputs.is_empty() {
                facts["reference_inputs"] = json!(suite.manifest.reference_inputs);
            }
            facts["tier"] = json!(if suite.manifest.explicit_only {
                "full"
            } else {
                "admission"
            });
            facts["impact"] = json!(native_suite_impact(&suite.manifest));
            facts["reuse"] = json!(suite.manifest.reuse);
            Ok(facts)
        })
        .collect::<Result<Vec<Value>>>()?;
    Ok(json!({"schema":1,"suites":entries}))
}

/// Every requirement comes from the parsed native case, independent of CLI policy.
pub(crate) fn required(step: &SqlStep) -> Vec<&'static str> {
    let m = &step.meta;
    let mut kinds = Vec::new();
    if m.has_error_expectation() {
        kinds.push("expected_error");
    }
    if !m.has_error_expectation() && crate::results::step_requires_recorded_result(step) {
        kinds.push("result_set");
    }
    if !m.explain_contains.is_empty() {
        kinds.push("explain_contains");
    }
    if !m.explain_not_contains.is_empty() {
        kinds.push("explain_not_contains");
    }
    if m.has_be_log_directives() {
        kinds.push("be_log");
    }
    if m.query_lifecycle_structured_assertion.is_some() {
        kinds.push("runtime_lifecycle");
    }
    if !m.result_contains.is_empty()
        || !m.result_not_contains.is_empty()
        || !m.result_contains_any.is_empty()
        || m.query_stats_contract.is_some()
        || m.result_rows_count.is_some()
        || !m.result_rows_expect.is_empty()
    {
        kinds.push("text_assertions");
    }
    if m.imv_equivalence_check.is_some() {
        kinds.push("imv_equivalence");
    }
    if m.imv_stateless_rebuild.is_some() {
        kinds.push("imv_stateless_rebuild");
    }
    if m.mv_rest_document_graph.is_some() {
        kinds.push("mv_rest_document_graph");
    }
    if m.iceberg_orphan_fixture_absent {
        kinds.push("iceberg_orphan_absent");
    }
    if m.wait_alter_column.is_some()
        || m.wait_alter_rollup.is_some()
        || m.wait_alter_optimize.is_some()
    {
        kinds.push("wait_alter");
    }
    kinds
}

#[derive(Default)]
pub(crate) struct CaseEvidence {
    pub artifacts: Vec<PathBuf>,
    pub diagnostics: Vec<Value>,
    pub executed: Vec<Value>,
    pub failure: Option<Value>,
    statement: Option<usize>,
    candidate_failure: Option<Value>,
    cleanup_step: bool,
}
impl CaseEvidence {
    pub fn begin_step(&mut self, statement: usize, previous_failed: bool) {
        if previous_failed {
            self.freeze_failure();
        }
        self.statement = Some(statement);
        self.candidate_failure = None;
        self.cleanup_step = false;
    }
    pub fn begin_attempt(&mut self) {
        // Only the last unresolved retry can describe this statement's failure.
        self.candidate_failure = None;
    }
    pub fn mark_cleanup_step(&mut self) {
        self.cleanup_step = true;
    }
    pub fn record_failure(&mut self, mut failure: Value) {
        if self.cleanup_step {
            failure["class"] = json!("error");
            failure["cleanup"] = json!(true);
        }
        if failure["class"] == "error_code" && failure["error_code"].is_null() {
            failure["class"] = json!("error");
        }
        self.candidate_failure = Some(failure);
    }
    fn append_failure(&mut self, additional: Value) {
        self.failure = Some(match self.failure.take() {
            None => additional,
            Some(mut primary) if primary.get("additional_failures").is_some() => {
                primary["additional_failures"]
                    .as_array_mut()
                    .expect("failure diagnostics array")
                    .push(additional);
                primary
            }
            Some(primary) => json!({
                "class":"error", "error_code":null, "statement_index":null,
                "actual_digest":null, "primary_failure":primary,
                "additional_failures":[additional]
            }),
        });
    }
    fn freeze_failure(&mut self) {
        if let Some(candidate) = self.candidate_failure.take() {
            self.append_failure(candidate);
        } else if self.failure.is_none() {
            self.failure = Some(json!({"class":"error","error_code":null,
                "statement_index":self.statement,"actual_digest":null}));
        }
    }
    fn record_distinct_failure(&mut self, statement: Option<usize>, error: String, cleanup: bool) {
        // Preserve unresolved statement facts before recording a distinct failure.
        if self.candidate_failure.is_some() {
            self.freeze_failure();
        }
        let mut failure = json!({"class":"error", "error_code":null,
            "statement_index":statement,"actual_digest":null,"error":error});
        if cleanup {
            failure["cleanup"] = json!(true);
        }
        self.append_failure(failure);
    }
    pub fn record_independent_failure(&mut self, statement: Option<usize>, error: String) {
        self.record_distinct_failure(statement, error, false);
    }
    pub fn record_case_cleanup_failure(&mut self, statement: Option<usize>, error: String) {
        self.record_distinct_failure(statement, error, true);
    }
    pub fn finish_execution(&mut self, failed: bool) {
        if failed {
            self.freeze_failure();
        }
        self.statement = None;
        self.candidate_failure = None;
    }
    pub fn finish_case(&mut self, failed: bool) {
        // A session/database cleanup failure after successful statements is a
        // case failure, not an error from the last successful SQL statement.
        if failed {
            self.freeze_failure();
        }
    }
    pub fn record_expected_error_log(&mut self, step: &SqlStep, matched: bool, passed: bool) {
        // The expected-error helper returns before checking logs when the
        // query did not match its required error.
        if matched && step.meta.has_be_log_directives() {
            self.record(step, "be_log", passed, json!({}));
        }
    }
    pub fn record(&mut self, step: &SqlStep, kind: &str, passed: bool, policy: Value) {
        // Retry loops record the last observation for each required oracle.
        self.executed
            .retain(|v| !(v["step"] == step.query_number && v["kind"] == kind));
        self.executed.push(json!({"step":step.query_number,"kind":kind,"outcome":if passed {"pass"} else {"fail"},"policy":policy}));
    }
    pub fn oracle(&self, case: &SqlCase, expected: Option<&Path>) -> Value {
        let required = case
            .steps
            .iter()
            .flat_map(|step| {
                required(step)
                    .into_iter()
                    .map(move |kind| json!({"step":step.query_number,"kind":kind}))
            })
            .collect::<Vec<_>>();
        json!({"required":required,"executed":self.executed,"expected_source":expected})
    }
}

pub(crate) struct SuiteRecorder {
    pub writer: Arc<ResultsWriter>,
    pub config: SuiteConfig,
    pub lifecycle: String,
    pub start: Value,
    seen: Mutex<std::collections::BTreeSet<String>>,
}
impl SuiteRecorder {
    pub fn new(writer: Arc<ResultsWriter>, config: SuiteConfig, start: Value) -> Self {
        Self {
            writer,
            config,
            start,
            lifecycle: format!("{}-{}", now(), SEQUENCE.fetch_add(1, Ordering::Relaxed)),
            seen: Mutex::new(Default::default()),
        }
    }
    pub fn case(&self, case: &SqlCase, outcome: &crate::CaseOutcome, evidence: &CaseEvidence) {
        let expected = self
            .config
            .result_dir
            .as_ref()
            .map(|dir| crate::results::case_result_path(dir, &case.case_id));
        self.write_case(
            outcome,
            {
                let mut oracle = evidence.oracle(case, expected.as_deref());
                oracle["artifacts"] = json!(evidence.artifacts);
                oracle
            },
            evidence.failure.clone(),
            &evidence.diagnostics,
            None,
        );
    }
    fn write_case(
        &self,
        outcome: &crate::CaseOutcome,
        oracle: Value,
        failure: Option<Value>,
        diagnostics: &[Value],
        reason: Option<&str>,
    ) {
        if !self.seen.lock().unwrap().insert(outcome.case_id.clone()) {
            return;
        }
        let artifacts = oracle
            .get("artifacts")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let (status, reason) = match outcome.status {
            crate::CaseStatus::Pass => ("pass", reason),
            crate::CaseStatus::Fail => ("fail", reason),
            crate::CaseStatus::Skipped => ("not_run", Some("aborted")),
        };
        let failure = if status == "fail" {
            Some(failure.unwrap_or_else(|| json!({"class":"error", "error_code":null,"statement_index":null,"actual_digest":null})))
        } else {
            None
        };
        self.writer.observe(json!({"kind":"case","suite":self.config.name,"case":outcome.case_id,
            "lifecycle_id":self.lifecycle,"status":status,"reason":reason,"elapsed_ms":outcome.elapsed.as_millis(),
            "oracle":oracle,"failure":failure,"artifacts":artifacts,"diagnostics":diagnostics}));
    }
    pub fn load_failure(&self, failure: &crate::CaseLoadFailure) {
        let outcome = crate::CaseOutcome {
            case_id: failure.case_id.clone(),
            status: crate::CaseStatus::Fail,
            elapsed: Default::default(),
            log: String::new(),
        };
        self.write_case(&outcome,json!({"required":[],"executed":[],"expected_source":null}),Some(json!({"class":"load","error_code":null,"statement_index":null,"actual_digest":null,"error":failure.error})),&[],Some("load_failed"));
    }
    pub fn init_failed(&self, cases: &[SqlCase]) {
        for case in cases {
            let outcome = crate::CaseOutcome {
                case_id: case.case_id.clone(),
                status: crate::CaseStatus::Fail,
                elapsed: Default::default(),
                log: String::new(),
            };
            let expected = self
                .config
                .result_dir
                .as_ref()
                .map(|dir| crate::results::case_result_path(dir, &case.case_id));
            self.write_case(
                &outcome,
                CaseEvidence::default().oracle(case, expected.as_deref()),
                None,
                &[],
                Some("suite_init_failed"),
            );
        }
    }
    pub fn finish(
        &self,
        ps: &crate::PreparedSuite,
        outcome: &crate::SuiteOutcome,
        aborted: bool,
        init_errors: &[String],
    ) {
        for item in &outcome.outcomes {
            if let Some(load) = ps.load_failures.iter().find(|f| f.case_id == item.case_id) {
                self.write_case(item,json!({"required":[],"executed":[],"expected_source":null}),Some(json!({"class":"load","error_code":null,"statement_index":null,"actual_digest":null,"error":load.error})),&[],Some("load_failed"));
            } else if let Some(case) = ps.cases.iter().find(|c| c.case_id == item.case_id) {
                let expected = self
                    .config
                    .result_dir
                    .as_ref()
                    .map(|dir| crate::results::case_result_path(dir, &case.case_id));
                self.write_case(
                    item,
                    CaseEvidence::default().oracle(case, expected.as_deref()),
                    None,
                    &[],
                    (!init_errors.is_empty()).then_some("suite_init_failed"),
                );
            }
        }
        let end = suite_inputs(&self.config);
        let (end_hash, stable) = match end {
            Ok(ref end) => (
                end["inputs_sha256"].clone(),
                end["inputs_sha256"] == self.start["inputs_sha256"] && self.writer.inputs_stable(),
            ),
            Err(_) => (Value::Null, false),
        };
        let mut record = json!({"kind":"suite","suite":self.config.name,"lifecycle_id":self.lifecycle,
            "executed_cases":outcome.outcomes.iter().filter(|o| init_errors.is_empty() && o.status != crate::CaseStatus::Skipped).map(|o|o.case_id.clone()).collect::<Vec<_>>(),
            "inputs_end_sha256":end_hash,"inputs_stable":stable,
            "init":{"status":if init_errors.is_empty(){"ok"}else{"failed"},"error":if init_errors.is_empty(){Value::Null}else{json!(init_errors.join("; "))}},
            "cleanup":{"status":if outcome.cleanup_errors.is_empty(){"ok"}else{"failed"},"errors":outcome.cleanup_errors},"aborted":aborted});
        if let Some(reference) = self.start.get("reference") {
            record["reference"] = reference.clone();
        }
        self.writer.observe(record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn writer_is_exclusive_and_parallel_lines_are_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("results.jsonl");
        let writer = ResultsWriter::create(dir.path(), path.to_str()).unwrap();
        assert!(ResultsWriter::create(dir.path(), path.to_str()).is_err());
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let w = &writer;
                scope.spawn(move || {
                    for n in 0..40 {
                        w.write(json!({"kind":"case","worker":worker,"n":n}))
                            .unwrap();
                    }
                });
            }
        });
        writer
            .write(json!({"kind":"end","completed":true}))
            .unwrap();
        let text = fs::read_to_string(path).unwrap();
        assert!(text.ends_with('\n'));
        let rows = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 321);
        assert_eq!(rows.last().unwrap()["kind"], "end");
    }
    #[test]
    fn pure_error_and_explain_assertions_do_not_require_result_sets() {
        let mut step = SqlStep {
            query_number: 1,
            sql: "SELECT missing".into(),
            meta: Default::default(),
        };
        step.meta.expect_error_code = Some("NR-1".into());
        assert_eq!(required(&step), vec!["expected_error"]);
        step.meta.expect_error_code = None;
        step.meta.expect_error = Some("missing".into());
        assert_eq!(required(&step), vec!["expected_error"]);
        step.meta.expect_error = None;
        step.meta.skip_result_check = true;
        step.meta.explain_contains.push("Scan".into());
        assert_eq!(required(&step), vec!["explain_contains"]);
    }
    #[test]
    fn unresolved_final_retry_supplies_the_failure_feature() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(7, false);
        evidence.begin_attempt();
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-RETRY","statement_index":7}),
        );
        evidence.begin_attempt();
        evidence.record_failure(json!({"class":"mismatch","error_code":null,"statement_index":7,"actual_digest":"final-result"}));
        evidence.finish_execution(true);
        evidence.finish_case(true);
        assert_eq!(evidence.failure.as_ref().unwrap()["class"], "mismatch");
        assert_eq!(
            evidence.failure.as_ref().unwrap()["actual_digest"],
            "final-result"
        );
    }
    #[test]
    fn successful_retry_followed_by_case_cleanup_failure_has_no_statement_feature() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(3, false);
        evidence.begin_attempt();
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-RETRY","statement_index":3}),
        );
        evidence.begin_attempt();
        evidence.finish_execution(false);
        evidence.finish_case(true);
        let failure = evidence.failure.unwrap();
        assert_eq!(failure["class"], "error");
        assert_eq!(failure["error_code"], Value::Null);
        assert_eq!(failure["statement_index"], Value::Null);
    }
    #[test]
    fn an_error_without_a_code_cannot_publish_an_error_code_feature() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(2, false);
        evidence.begin_attempt();
        evidence
            .record_failure(json!({"class":"error_code","error_code":null,"statement_index":2}));
        evidence.finish_execution(true);
        assert_eq!(evidence.failure.unwrap()["class"], "error");
    }
    #[test]
    fn independent_failure_keeps_both_facts_and_cannot_match_the_first_known_code() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-KNOWN","statement_index":1}),
        );
        evidence.begin_step(2, true);
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-NEW","statement_index":2}),
        );
        evidence.finish_execution(true);
        evidence.finish_case(true);
        let failure = evidence.failure.unwrap();
        assert_eq!(failure["class"], "error");
        assert_eq!(failure["error_code"], Value::Null);
        assert_eq!(failure["primary_failure"]["error_code"], "NR-KNOWN");
        assert_eq!(failure["additional_failures"].as_array().unwrap().len(), 1);
        assert_eq!(failure["additional_failures"][0]["error_code"], "NR-NEW");
    }
    #[test]
    fn case_cleanup_records_the_actual_error_after_a_known_primary_and_pending_failure() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-KNOWN","statement_index":1}),
        );
        evidence.begin_step(2, true);
        evidence.record_failure(
            json!({"class":"mismatch","statement_index":2,"actual_digest":"changed"}),
        );
        evidence.record_case_cleanup_failure(None, "session close failed".into());
        evidence.finish_execution(true);
        evidence.finish_case(true);
        let failure = evidence.failure.unwrap();
        assert_eq!(failure["class"], "error");
        assert_eq!(failure["primary_failure"]["error_code"], "NR-KNOWN");
        let additional = failure["additional_failures"].as_array().unwrap();
        assert_eq!(additional.len(), 2);
        assert_eq!(additional[0]["actual_digest"], "changed");
        assert_eq!(additional[1]["error"], "session close failed");
        assert_eq!(additional[1]["cleanup"], true);
        assert_eq!(additional[1]["statement_index"], Value::Null);
    }
    #[test]
    fn independent_control_failure_preserves_primary_without_a_cleanup_label() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-KNOWN","statement_index":1}),
        );
        evidence.record_independent_failure(Some(1), "control installation failed".into());
        evidence.record_case_cleanup_failure(None, "session close failed".into());
        evidence.finish_execution(true);
        evidence.finish_case(true);
        let failure = evidence.failure.unwrap();
        assert_eq!(failure["class"], "error");
        assert_eq!(failure["primary_failure"]["error_code"], "NR-KNOWN");
        let additional = failure["additional_failures"].as_array().unwrap();
        assert_eq!(additional.len(), 2);
        assert_eq!(additional[0]["class"], "error");
        assert_eq!(additional[0]["error"], "control installation failed");
        assert_eq!(additional[0]["statement_index"], 1);
        assert!(additional[0].get("cleanup").is_none());
        assert_eq!(additional[1]["error"], "session close failed");
        assert_eq!(additional[1]["cleanup"], true);
    }
    #[test]
    fn cleanup_marker_is_per_step_and_cleanup_only_codes_are_diagnostic() {
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.mark_cleanup_step();
        evidence.record_failure(json!({"class":"error_code","error_code":"NR-CLEANUP"}));
        evidence.finish_execution(true);
        assert_eq!(evidence.failure.as_ref().unwrap()["class"], "error");
        assert_eq!(
            evidence.failure.as_ref().unwrap()["error_code"],
            "NR-CLEANUP"
        );
        evidence.begin_step(2, false);
        evidence.record_failure(json!({"class":"error_code","error_code":"NR-LATER"}));
        assert_eq!(
            evidence.candidate_failure.as_ref().unwrap()["class"],
            "error_code"
        );
    }
    #[test]
    fn expected_error_log_records_only_executed_checks_and_keeps_the_last_result() {
        let step = SqlStep {
            query_number: 1,
            sql: "select rejected".into(),
            meta: crate::types::QueryMeta {
                expect_error: Some("planned rejection".into()),
                be_log_contains: vec!["native boundary".into()],
                ..Default::default()
            },
        };
        let mut evidence = CaseEvidence::default();
        evidence.record_expected_error_log(&step, false, false);
        assert!(evidence.executed.is_empty());
        evidence.record_expected_error_log(&step, true, true);
        evidence.record_expected_error_log(&step, true, false);
        assert_eq!(evidence.executed.len(), 1);
        assert_eq!(evidence.executed[0]["kind"], "be_log");
        assert_eq!(evidence.executed[0]["outcome"], "fail");
    }
    #[test]
    fn session_statement_digests_distinguish_contents() {
        assert_ne!(digest(b"SET a=1"), digest(b"SET a=2"));
    }
}

fn execution_platform() -> Result<Value> {
    let uname = |flag: &str| -> Result<String> {
        let output = std::process::Command::new("uname").arg(flag).output()?;
        anyhow::ensure!(
            output.status.success(),
            "platform observation failed: uname {flag}"
        );
        let value = String::from_utf8(output.stdout)?.trim().to_owned();
        anyhow::ensure!(
            !value.is_empty(),
            "platform observation was empty: uname {flag}"
        );
        Ok(value)
    };
    Ok(json!({"system":uname("-s")?,"machine":uname("-m")?,"release":uname("-r")?}))
}

pub(crate) fn run_record(
    cli: &crate::Cli,
    root: &Path,
    config: Option<&Path>,
    suites: &[String],
    fixture_root: Option<&Path>,
) -> Result<Value> {
    let git = |args: &[&str]| -> Result<String> {
        let result = std::process::Command::new("git")
            .current_dir(root)
            .args(args)
            .output()?;
        anyhow::ensure!(result.status.success(), "git source observation failed");
        Ok(String::from_utf8(result.stdout)?.trim().to_owned())
    };
    let runner = std::env::current_exe()?;
    let binary = if cli.cluster_mode == crate::ClusterMode::AllInOne || cli.dry_run {
        None
    } else {
        Some(crate::cluster::discover_novarocks_binary(root)?)
    };
    Ok(
        json!({"kind":"run","schema":1,"attempt_id":format!("sql-{}-{}",now(),std::process::id()),"started_at":now(),
        "source":{"commit":git(&["rev-parse","HEAD"])?,"dirty":!git(&["status","--porcelain"])?.is_empty()},
        "platform":execution_platform()?,
        "runner":{"path":runner,"sha256":hash_file(&runner)?},
        "server":{"path":binary,"sha256":binary.as_deref().map(hash_file).transpose()?,"launched":if binary.is_none(){"external"}else{"novarocks-bin"}},
        "config":{"path":config,"sha256":optional_hash(config)?},"fixture_root":fixture_root,
        "topology":{"cluster_mode":match cli.cluster_mode {crate::ClusterMode::AllInOne=>"all-in-one",crate::ClusterMode::CrossProcess=>"cross-process"},"cluster_size":cli.cluster_size.unwrap_or(1),"launch_profile":match cli.cluster_launch_profile.unwrap_or(novarocks_cluster_harness::LaunchProfile::FaultScenario){novarocks_cluster_harness::LaunchProfile::FaultScenario=>"fault-scenario",novarocks_cluster_harness::LaunchProfile::Performance=>"performance"}},
        "mode":crate::mode_name(cli.mode),
        "convert_legacy":cli.convert_legacy,
        "legacy_variant_offset_seconds":cli.legacy_variant_offset_seconds,
        "client_capabilities":novarocks_test_trino_client::CLIENT_CAPABILITIES,
        "corpus_session":crate::config::load_corpus_session(root)?,
        "verification":{"verify_enabled":!cli.no_verify,"update_expected":cli.update_expected,"float_epsilon":cli.float_epsilon,
            "order_sensitive_default":cli.order_sensitive_default,"rewrite_explain_contains_as_not_contains":cli.rewrite_explain_contains_as_not_contains,
            "target_session_sql":cli.target_session_sql.iter().map(|sql|json!({"sha256":digest(sql.as_bytes())})).collect::<Vec<_>>(),"dry_run":cli.dry_run,"query_timeout_secs":cli.query_timeout},
        "selection":{"suites":suites,"only":cli.only.as_deref().unwrap_or("").split(',').filter(|s|!s.is_empty()).collect::<Vec<_>>(),"skip":cli.skip.as_deref().unwrap_or("").split(',').filter(|s|!s.is_empty()).collect::<Vec<_>>(),"sql_glob":cli.sql_glob,"limit":cli.limit,"fail_fast":cli.fail_fast,"jobs":cli.jobs}}),
    )
}

pub(crate) fn fixture_start<T>(
    writer: &ResultsWriter,
    root: Option<&Path>,
    before: &novarocks_cluster_harness::resource_registration::RegistrationSnapshot,
    start: impl FnOnce() -> Result<T>,
) -> Result<T> {
    match start() {
        Ok(fixture) => Ok(fixture),
        Err(error) => {
            let resources = match root {
                Some(root) => novarocks_cluster_harness::resource_registration::collect(
                    root,
                    before,
                    std::process::id(),
                )?,
                None => Vec::new(),
            };
            writer.write(json!({"kind":"fixture","start":"failed","error":format!("{error:#}"),"shutdown":{"status":"not_called","errors":[]},"resources":resources}))?;
            Err(error)
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::types::*;
    use crate::{CaseStatus, PreparedSuite, SuiteRunContext};
    use std::sync::atomic::AtomicBool;
    struct NoServer;
    impl crate::cluster::TestClusterOwner for NoServer {
        fn target_host(&self) -> Option<&str> {
            None
        }
        fn target_port(&self) -> Option<u16> {
            None
        }
    }
    fn prepared(root: &Path) -> (PreparedSuite, Arc<ResultsWriter>) {
        let sql_dir = root.join("sql");
        let result_dir = root.join("result");
        fs::create_dir(&sql_dir).unwrap();
        fs::create_dir(&result_dir).unwrap();
        fs::write(sql_dir.join("one.sql"), "select 1;").unwrap();
        fs::write(
            result_dir.join("one.result"),
            "-- query 1\n-- types\tbigint\nid\n1\n",
        )
        .unwrap();
        let writer = ResultsWriter::create(root, root.join("results.jsonl").to_str()).unwrap();
        let config = SuiteConfig {
            name: "unit".into(),
            sql_dir: sql_dir.clone(),
            result_dir: Some(result_dir.clone()),
            sql_glob: "*.sql".into(),
            default_catalog: "default_catalog".into(),
            default_db: String::new(),
            auto_case_db: false,
            verify_default: true,
            init_sql: None,
            cleanup_sql: None,
            manifest: Default::default(),
        };
        let fact = suite_inputs(&config).unwrap();
        let recorder = Arc::new(SuiteRecorder::new(Arc::clone(&writer), config, fact));
        let conn = ConnectionConfig {
            time_zone: "Asia/Shanghai".into(),

            host: "localhost".into(),
            port: "1".into(),
            user: "root".into(),

            catalog: None,
            db: None,
        };
        let artifacts = Arc::new(crate::failure_artifacts::FailureArtifactRecorder::new(
            crate::failure_artifacts::FailureArtifactContext {
                root: root.join("failures"),
                lane: "correctness".into(),
                suites: vec!["unit".into()],
            },
        ));
        let ctx = SuiteRunContext {
            case_input: None,
            reference: Default::default(),
            fixture_storage: None,
            fixture_writers_settled: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            namespace_catalogs: crate::namespace_guard::CatalogFacts::default(),
            fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget::from_environment(
                3600,
            )
            .unwrap(),
            suite_name: "unit".into(),
            controls: None,
            mode: crate::Mode::Verify,
            convert_legacy: false,
            legacy_variant_offset_seconds: None,
            target_conn_base: conn.clone(),
            target_admin_conn: conn.clone(),
            result_dir: Some(result_dir),
            actual_artifact_dir: None,
            verify_enabled: true,
            query_timeout: 1,
            auto_case_db: false,
            order_sensitive_default: false,
            float_epsilon: None,
            preview_lines: 0,
            update_expected: false,
            session_defaults: crate::suite_manifest::SuiteSessionDefaults::default(),
            target_session_sql: vec![],
            rewrite_explain_contains_as_not_contains: vec![],
            fail_fast: false,
            cluster_owner: Arc::new(Mutex::new(Box::new(NoServer))),
            failure_artifacts: artifacts,
            publication_catalog_control: None,
            publication_service_control: None,
            results: Some(recorder),
        };
        let case = SqlCase {
            expected_variables: Default::default(),
            namespace_rewrites: Default::default(),
            auto_db: String::new(),
            source_file: sql_dir.join("one.sql"),
            case_id: "one".into(),
            steps: vec![SqlStep {
                query_number: 1,
                sql: "select 1".into(),
                meta: Default::default(),
            }],
            case_dbs: vec![],
            sequential: false,
        };
        (
            PreparedSuite {
                ctx,
                cases: vec![case],
                load_failures: vec![],
                init_hook: None,
                cleanup_hook: None,
            },
            writer,
        )
    }
    #[test]
    fn normal_and_failed_suite_settlements_retain_the_bound_reference() {
        for failed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (mut ps, _) = prepared(dir.path());
            let reference = json!({"trino_image":{"alias":"reference","image_id":"sha256:original","version":"declared-version"}});
            Arc::get_mut(ps.ctx.results.as_mut().unwrap())
                .unwrap()
                .start["reference"] = reference.clone();
            let outcome = crate::SuiteOutcome {
                suite_name: "unit".into(),
                total: 0,
                outcomes: vec![],
                cleanup_errors: if failed {
                    vec!["original cleanup failure".into()]
                } else {
                    vec![]
                },
                wall_time: Default::default(),
            };
            ps.ctx
                .results
                .as_ref()
                .unwrap()
                .finish(&ps, &outcome, false, &[]);
            let record = &rows(dir.path())[0];
            assert_eq!(record["kind"], "suite");
            assert_eq!(record["reference"], reference);
            assert_eq!(
                record["cleanup"]["status"],
                if failed { "failed" } else { "ok" }
            );
        }
    }

    #[test]
    fn case_diagnostics_are_retained_outside_correctness_oracles() {
        let dir = tempfile::tempdir().unwrap();
        let (prepared, _) = prepared(dir.path());
        let case = &prepared.cases[0];
        let mut evidence = CaseEvidence::default();
        evidence.record(&case.steps[0], "result_set", true, json!({}));
        evidence.diagnostics.push(json!({
            "step":1,
            "kind":"publication_kill_query_diagnostic",
            "retained":true,
            "details":{"original_query_ok":false,"authorized_as_complete_R_old_input":false}
        }));
        prepared.ctx.results.as_ref().unwrap().case(
            case,
            &crate::CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Pass,
                elapsed: Default::default(),
                log: String::new(),
            },
            &evidence,
        );
        let record = &rows(dir.path())[0];
        assert_eq!(record["status"], "pass");
        assert_eq!(record["failure"], Value::Null);
        assert_eq!(record["diagnostics"], json!(evidence.diagnostics));
        assert_eq!(record["oracle"]["executed"], json!(evidence.executed));
        assert_eq!(record["oracle"]["executed"].as_array().unwrap().len(), 1);
        assert_eq!(record["oracle"]["executed"][0]["kind"], "result_set");
    }

    #[test]
    fn declared_session_defaults_are_explicit_native_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let (prepared, _) = prepared(dir.path());
        let mut suite = prepared.ctx.results.as_ref().unwrap().config.clone();
        let absent = suite_inputs(&suite).unwrap();
        assert!(absent.get("session").is_none());
        suite.manifest.session.pipeline_dop = Some(5);
        let five = suite_inputs(&suite).unwrap();
        assert_eq!(five["session"]["pipeline_dop"], 5);
        assert_ne!(absent["inputs_sha256"], five["inputs_sha256"]);
        suite.manifest.session.pipeline_dop = Some(6);
        let six = suite_inputs(&suite).unwrap();
        assert_ne!(five["inputs_sha256"], six["inputs_sha256"]);
        assert_eq!(five["manifest_sha256"], six["manifest_sha256"]);
    }

    #[test]
    fn native_declarations_keep_suite_facts_and_cover_shared_sql_launch_owners() {
        let dir = tempfile::tempdir().unwrap();
        let (prepared, _) = prepared(dir.path());
        let original = prepared.ctx.results.as_ref().unwrap().config.clone();
        let mut suites = BTreeMap::new();
        for (name, explicit) in [("regular", false), ("heavy", true)] {
            let mut suite = original.clone();
            suite.name = name.to_owned();
            suite.manifest.impact.paths = vec![
                "novarocks/sql/src/optimizer/**".to_owned(),
                "tests/sql/runner/**".to_owned(),
            ];
            suite.manifest.explicit_only = explicit;
            if explicit {
                suite.manifest.heavy = Some(crate::suite_manifest::HeavyDeclaration {
                    reason: crate::suite_manifest::HeavyReason::Dataset,
                    detail: "Existing independently selected dataset fixture".to_owned(),
                });
                suite.manifest.prepared_dataset =
                    Some(crate::suite_manifest::PreparedDatasetRequirement {
                        dataset: crate::suite_manifest::DatasetName::Ssb,
                        scale: "1".to_owned(),
                    });
            }
            suites.insert(name.to_owned(), suite);
        }
        let declared = declarations(&suites).unwrap();
        assert_eq!(declared["suites"].as_array().unwrap().len(), suites.len());
        for entry in declared["suites"].as_array().unwrap() {
            let name = entry["name"].as_str().unwrap();
            let original = &suites[name];
            let paths = entry["impact"]["paths"].as_array().unwrap();
            for shared in [
                "tests/sql/runner/**",
                "tests/trino-client/**",
                "tests/cluster-harness/**",
                "tests/test-support/**",
                "novarocks/failpoint/**",
            ] {
                assert_eq!(
                    paths
                        .iter()
                        .filter(|path| path.as_str() == Some(shared))
                        .count(),
                    1
                );
            }
            assert!(
                paths
                    .iter()
                    .any(|path| path.as_str() == Some("novarocks/sql/src/optimizer/**"))
            );
            assert_eq!(
                entry["explicit_only"],
                json!(original.manifest.explicit_only)
            );
            assert_eq!(entry["heavy"], json!(original.manifest.heavy));
            assert_eq!(
                entry["prepared_dataset"],
                json!(original.manifest.prepared_dataset)
            );
            assert_eq!(entry["reuse"], json!(original.manifest.reuse));
            assert_eq!(entry["cases"], suite_inputs(original).unwrap()["cases"]);
            assert_eq!(
                original.manifest.impact.paths.len(),
                2,
                "native projection must not mutate the suite's own declaration"
            );
        }
    }

    fn rows(root: &Path) -> Vec<Value> {
        fs::read_to_string(root.join("results.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    #[test]
    fn cleanup_statement_failure_preserves_primary_without_a_known_failure_feature() {
        let dir = tempfile::tempdir().unwrap();
        let (ps, _) = prepared(dir.path());
        let mut case = ps.cases[0].clone();
        case.steps.push(SqlStep {
            query_number: 2,
            sql: "drop table t".into(),
            meta: QueryMeta {
                cleanup: true,
                ..Default::default()
            },
        });
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.begin_attempt();
        evidence.record_failure(json!({"class":"mismatch","error_code":null,"statement_index":1,"actual_digest":"original-result"}));
        evidence.begin_step(2, true);
        evidence.mark_cleanup_step();
        evidence.begin_attempt();
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-CLEANUP","statement_index":2}),
        );
        evidence.finish_execution(true);
        evidence.finish_case(true);
        ps.ctx.results.as_ref().unwrap().case(
            &case,
            &crate::CaseOutcome {
                case_id: case.case_id.clone(),
                status: CaseStatus::Fail,
                elapsed: std::time::Duration::ZERO,
                log: String::new(),
            },
            &evidence,
        );
        let record = &rows(dir.path())[0];
        assert_eq!(record["failure"]["class"], "error");
        let primary = &record["failure"]["primary_failure"];
        assert_eq!(primary["class"], "mismatch");
        assert_eq!(primary["statement_index"], 1);
        assert_eq!(primary["actual_digest"], "original-result");
        let cleanup = &record["failure"]["additional_failures"][0];
        assert_eq!(cleanup["class"], "error");
        assert_eq!(cleanup["error_code"], "NR-CLEANUP");
        assert_eq!(cleanup["cleanup"], true);
    }
    #[test]
    fn recovered_retry_cannot_label_a_later_generic_oracle_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (ps, _) = prepared(dir.path());
        let mut evidence = CaseEvidence::default();
        evidence.begin_step(1, false);
        evidence.begin_attempt();
        evidence.record_failure(
            json!({"class":"error_code","error_code":"NR-RECOVERED","statement_index":1}),
        );
        evidence.begin_attempt();
        evidence.begin_step(2, false);
        evidence.begin_attempt();
        evidence.finish_execution(true);
        evidence.finish_case(true);
        ps.ctx.results.as_ref().unwrap().case(
            &ps.cases[0],
            &crate::CaseOutcome {
                case_id: "one".into(),
                status: CaseStatus::Fail,
                elapsed: std::time::Duration::ZERO,
                log: String::new(),
            },
            &evidence,
        );
        let record = &rows(dir.path())[0];
        assert_eq!(record["failure"]["class"], "error");
        assert_eq!(record["failure"]["error_code"], Value::Null);
        assert_eq!(record["failure"]["statement_index"], 2);
    }
    #[test]
    fn aborted_case_has_no_executed_oracle_and_is_not_run() {
        let dir = tempfile::tempdir().unwrap();
        let (ps, _) = prepared(dir.path());
        let out = crate::run_suite_with_hooks(
            &ps,
            &AtomicBool::new(true),
            &Mutex::new(()),
            |_, _, _, _| Ok(()),
        );
        assert_eq!(out.outcomes[0].status, CaseStatus::Skipped);
        let records = rows(dir.path());
        assert_eq!(records[0]["status"], "not_run");
        assert_eq!(records[0]["reason"], "aborted");
        assert_eq!(records[0]["oracle"]["executed"], json!([]));
        assert_eq!(records[1]["executed_cases"], json!([]));
    }
    #[test]
    fn init_and_cleanup_failures_are_distinct_and_cases_are_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let (mut ps, _) = prepared(dir.path());
        let hook = |name: &str| SuiteHook {
            path: dir.path().join(name),
            sql: name.into(),
            catalog: None,
            db: None,
        };
        ps.init_hook = Some(hook("init"));
        ps.cleanup_hook = Some(hook("cleanup"));
        let out = crate::run_suite_with_hooks(
            &ps,
            &AtomicBool::new(false),
            &Mutex::new(()),
            |_, _, hook, _| anyhow::bail!("{} failed", hook.sql),
        );
        assert_eq!(out.outcomes[0].status, CaseStatus::Fail);
        let records = rows(dir.path());
        assert_eq!(records[0]["reason"], "suite_init_failed");
        assert_eq!(records[1]["init"]["status"], "failed");
        assert_eq!(records[1]["cleanup"]["status"], "failed");
    }
    #[test]
    fn changed_suite_input_invalidates_confirmation_and_load_failures_are_observed() {
        let dir = tempfile::tempdir().unwrap();
        let (mut ps, _) = prepared(dir.path());
        ps.cases.clear();
        ps.load_failures.push(crate::CaseLoadFailure {
            case_id: "one".into(),
            source_file: dir.path().join("sql/one.sql"),
            error: "unreadable SQL".into(),
        });
        fs::write(dir.path().join("sql/one.sql"), "select 2;").unwrap();
        crate::run_suite_with_hooks(
            &ps,
            &AtomicBool::new(false),
            &Mutex::new(()),
            |_, _, _, _| Ok(()),
        );
        let records = rows(dir.path());
        assert_eq!(records[0]["failure"]["class"], "load");
        assert_eq!(records[1]["inputs_stable"], false);
    }
    #[test]
    fn fixture_start_failure_keeps_owner_cleanup_facts_without_an_end() {
        use novarocks_cluster_harness::resource_registration::{list, register};
        let dir = tempfile::tempdir().unwrap();
        let writer =
            ResultsWriter::create(dir.path(), dir.path().join("results.jsonl").to_str()).unwrap();
        let old = register(dir.path(), "fixture", "old", "previous-invocation").unwrap();
        let before = list(dir.path()).unwrap();
        writer.write(json!({"kind":"run","schema":1})).unwrap();
        let result: Result<()> = fixture_start(&writer, Some(dir.path()), &before, || {
            assert_eq!(rows(dir.path()).len(), 1);
            assert_eq!(rows(dir.path())[0]["kind"], "run");
            let nested = dir.path().join("partially-started-fixture");
            let mut failed = register(&nested, "fixture", "failed", "failed-project")?;
            failed.settle(&Err(anyhow::anyhow!("cleanup failed")))?;
            // Losing the creating handle makes no release claim.
            drop(register(
                &nested,
                "fixture",
                "unsettled",
                "unsettled-project",
            )?);
            let foreign = register(&nested, "fixture", "foreign", "foreign-project")?;
            let mut record: Value = serde_json::from_slice(&fs::read(foreign.path())?)?;
            let foreign_pid = std::process::id().wrapping_add(1);
            assert_ne!(foreign_pid, std::process::id());
            record["pid"] = json!(foreign_pid);
            fs::write(foreign.path(), serde_json::to_vec(&record)?)?;
            anyhow::bail!("startup failed")
        });
        assert!(format!("{:#}", result.unwrap_err()).contains("startup failed"));
        let records = rows(dir.path());
        assert_eq!(records.len(), 2);
        assert_eq!(records[1]["kind"], "fixture");
        assert_eq!(records[1]["start"], "failed");
        assert_eq!(records[1]["shutdown"]["status"], "not_called");
        assert!(
            records[1]["error"]
                .as_str()
                .unwrap()
                .contains("startup failed")
        );
        let resources = records[1]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|resource| (resource["id"].as_str().unwrap(), resource))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources["failed"]["state"], "cleanup_failed");
        assert_eq!(resources["failed"]["error"], "cleanup failed");
        assert_eq!(resources["unsettled"]["state"], "unsettled");
        assert_eq!(resources["unsettled"]["error"], Value::Null);
        assert!(!resources.contains_key("old"));
        assert!(!resources.contains_key("foreign"));
        assert!(old.path().is_file());
        assert!(
            records
                .iter()
                .all(|record| record["kind"] != "selection" && record["kind"] != "end")
        );
    }
    fn in_cli_test_process(name: &str, test: impl FnOnce(&Path)) {
        const CHILD: &str = "NOVAROCKS_RESULTS_RECORD_TEST_CHILD";
        const ROOT: &str = "NOVAROCKS_RESULTS_RECORD_TEST_ROOT";
        if std::env::var(CHILD).as_deref() == Ok(name) {
            let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
            test(&root);
            fs::write(root.join("child-completed"), name).unwrap();
            return;
        }
        // run_cli projects fixture variables before any workers exist. Keep
        // that existing mutation inside this test's own process.
        let root = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"])
            .env(CHILD, name)
            .env(ROOT, root.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(root.path().join("child-completed")).unwrap(),
            name
        );
    }

    fn local_cli_inputs(root: &Path, sql: &str) -> (PathBuf, PathBuf, PathBuf) {
        let sql_dir = root.join("sql");
        let result_dir = root.join("result");
        fs::create_dir(&sql_dir).unwrap();
        fs::create_dir(&result_dir).unwrap();
        fs::write(sql_dir.join("one.sql"), sql).unwrap();
        fs::write(
            result_dir.join("one.result"),
            "-- query 1\n-- types\tbigint\nid\n1\n",
        )
        .unwrap();
        let env_file = root.join("fixture-env.sh");
        fs::write(&env_file, "# Test-owned input; no external services.\n").unwrap();
        fs::write(
            root.join("manifest.json"),
            r#"{"ready":true,"runtime":{"catalog":{"phase":"ready"}}}"#,
        )
        .unwrap();
        let values = [
            ("iceberg_rest_uri", "http://127.0.0.1:1"),
            ("iceberg_rest_warehouse", "unit"),
            ("iceberg_rest_mv_uri", "http://127.0.0.1:1"),
            ("iceberg_rest_mv_warehouse", "unit-mv"),
            ("oss_endpoint", "http://127.0.0.1:1"),
            ("oss_ak", "unit-key"),
            ("oss_sk", "unit-secret"),
            ("fixture_env_file", env_file.to_str().unwrap()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), toml::Value::String(value.to_owned())))
        .collect();
        let config = root.join("runner.toml");
        fs::write(
            &config,
            toml::to_string(&toml::Table::from_iter([(
                "env".into(),
                toml::Value::Table(values),
            )]))
            .unwrap(),
        )
        .unwrap();
        (config, sql_dir, result_dir)
    }

    #[test]
    fn selection_precedes_a_policy_failure_without_a_launched_server() {
        in_cli_test_process(
            "results_record::lifecycle_tests::selection_precedes_a_policy_failure_without_a_launched_server",
            |root| {
                use clap::Parser;
                let (config, sql_dir, result_dir) = local_cli_inputs(
                    root,
                    "-- query 1\n-- @query_lifecycle_fault=runtime-filter-contribution-ack-drop,0\nSELECT 1;\n",
                );
                let path = root.join("results.jsonl");
                let cli = crate::Cli::try_parse_from([
                    "sql-test",
                    "--suite",
                    "session",
                    "--config",
                    config.to_str().unwrap(),
                    "--sql-dir",
                    sql_dir.to_str().unwrap(),
                    "--result-dir",
                    result_dir.to_str().unwrap(),
                    "--results-file",
                    path.to_str().unwrap(),
                    "--cluster-mode",
                    "all-in-one",
                    "--cluster-launch-profile",
                    "performance",
                    "-j",
                    "1",
                ])
                .unwrap();
                let error = crate::run_cli(cli).unwrap_err();
                assert!(
                    format!("{error:#}").contains("performance launch profile refuses"),
                    "{error:#}"
                );
                let records = rows(root);
                assert_eq!(
                    records
                        .iter()
                        .map(|row| row["kind"].as_str().unwrap())
                        .collect::<Vec<_>>(),
                    ["run", "selection", "server", "fixture", "end"]
                );
                assert_eq!(records[1]["suites"][0]["cases"][0]["case"], "one");
                assert_eq!(
                    records[2]["cleanup"],
                    json!({"status":"not_launched","error":null,"residual_pids":[]})
                );
                assert_eq!(records[4]["exit_code"], 1);
            },
        );
    }

    #[cfg(unix)]
    struct TrinoSetupPeer {
        port: u16,
        stop: Arc<AtomicBool>,
        worker: Option<std::thread::JoinHandle<Vec<String>>>,
    }
    #[cfg(unix)]
    impl TrinoSetupPeer {
        fn start() -> Self {
            use std::net::{TcpListener, TcpStream};
            use std::time::{Duration, Instant};
            fn read_request(peer: &mut TcpStream) -> Option<(String, String)> {
                let mut head = Vec::new();
                let mut byte = [0];
                while !head.ends_with(b"\r\n\r\n") {
                    peer.read_exact(&mut byte).ok()?;
                    head.push(byte[0]);
                }
                let head = String::from_utf8(head).unwrap();
                let method = head.split_whitespace().next().unwrap().to_owned();
                let length = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                let mut body = vec![0; length];
                peer.read_exact(&mut body).unwrap();
                Some((method, String::from_utf8(body).unwrap()))
            }
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = Arc::clone(&stop);
            let worker = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut queries = Vec::new();
                loop {
                    if stopping.load(Ordering::Relaxed) || Instant::now() >= deadline {
                        return queries;
                    }
                    let mut peer = match listener.accept() {
                        Ok((peer, _)) => peer,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                        Err(error) => panic!("accept test Trino setup: {error}"),
                    };
                    peer.set_nonblocking(false).unwrap();
                    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    peer.set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    // Reachability may connect without issuing HTTP.
                    let Some((method, sql)) = read_request(&mut peer) else {
                        continue;
                    };
                    let (status, body) = match method.as_str() {
                        "POST" => {
                            queries.push(sql);
                            (
                                "200 OK",
                                format!(
                                    r#"{{"id":"setup","nextUri":"http://127.0.0.1:{port}/next"}}"#
                                ),
                            )
                        }
                        "GET" => {
                            let query = queries.last().expect("GET follows a submitted statement");
                            if query.contains("trc2_http_failure") {
                                ("503 Service Unavailable", "TrinoError { name=NOT_SUPPORTED type=USER_ERROR code=13 }: body text cannot establish a query failure".into())
                            } else if query.contains("trc2_query_failure") {
                                ("200 OK", r#"{"id":"setup","error":{"message":"[sql.analyze.no_table] forced ordinary query failure","errorName":"TABLE_NOT_FOUND","errorType":"USER_ERROR","errorCode":46}}"#.into())
                            } else if query.to_ascii_uppercase().starts_with("SELECT") {
                                let (ty, value) = if query.contains("1.05") {
                                    ("double", "1.05")
                                } else {
                                    ("bigint", "1")
                                };
                                (
                                    "200 OK",
                                    format!(
                                        r#"{{"id":"setup","columns":[{{"name":"id","type":"{ty}","typeSignature":{{"rawType":"{ty}","arguments":[]}}}}],"data":[[{value}]]}}"#
                                    ),
                                )
                            } else {
                                (
                                    "200 OK",
                                    r#"{"id":"setup","updateType":"SET SESSION"}"#.into(),
                                )
                            }
                        }
                        other => panic!("unexpected Trino setup method {other}"),
                    };
                    write!(peer,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                    peer.flush().unwrap();
                }
            });
            Self {
                port,
                stop,
                worker: Some(worker),
            }
        }
        fn finish(mut self) -> Vec<String> {
            self.stop.store(true, Ordering::Relaxed);
            self.worker.take().unwrap().join().unwrap()
        }
    }
    #[cfg(unix)]
    impl Drop for TrinoSetupPeer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    #[cfg(unix)]
    fn prepared_conversion(root: &Path, peer: &TrinoSetupPeer, steps: &[&str]) -> PreparedSuite {
        let (mut ps, _) = prepared(root);
        ps.ctx.mode = crate::Mode::Record;
        ps.ctx.convert_legacy = true;
        ps.ctx.legacy_variant_offset_seconds = Some(0);
        ps.ctx.target_conn_base.host = "127.0.0.1".into();
        ps.ctx.target_conn_base.port = peer.port.to_string();
        ps.ctx.target_admin_conn = ps.ctx.target_conn_base.clone();
        ps.cases[0].steps = steps
            .iter()
            .enumerate()
            .map(|(index, sql)| SqlStep {
                query_number: index + 1,
                sql: (*sql).into(),
                meta: Default::default(),
            })
            .collect();
        ps
    }

    #[cfg(unix)]
    #[test]
    fn conversion_missing_source_fails_before_execution_without_creating_a_file() {
        let root = tempfile::tempdir().unwrap();
        let peer = TrinoSetupPeer::start();
        let ps = prepared_conversion(root.path(), &peer, &["SELECT 1 AS id"]);
        let path = root.path().join("result/one.result");
        fs::remove_file(&path).unwrap();
        let outcome = crate::run_case(&ps.ctx, &ps.cases[0], &AtomicBool::new(false));
        assert_eq!(outcome.status, CaseStatus::Fail);
        assert!(
            outcome
                .log
                .contains("conversion requires an existing paired golden file"),
            "{}",
            outcome.log
        );
        assert!(
            !path.exists(),
            "missing original oracle must not produce an empty replacement"
        );
        assert!(
            peer.finish().is_empty(),
            "a missing conversion source must fail before submitting SQL"
        );
    }

    #[cfg(unix)]
    #[test]
    fn conversion_query_failure_preserves_its_section_and_converts_a_later_success() {
        let root = tempfile::tempdir().unwrap();
        let peer = TrinoSetupPeer::start();
        let ps = prepared_conversion(
            root.path(),
            &peer,
            &["SELECT trc2_query_failure AS id", "SELECT 1 AS id"],
        );
        let path = root.path().join("result/one.result");
        let original = "-- query 1\n-- legacy\nid\n9\n\n-- query 2\n-- legacy\nid\n1\n";
        fs::write(&path, original).unwrap();
        let outcome = crate::run_case(&ps.ctx, &ps.cases[0], &AtomicBool::new(false));
        assert_eq!(
            outcome.status,
            CaseStatus::Fail,
            "conversion retains the ordinary failure verdict"
        );
        let submitted = peer.finish();
        assert_eq!(submitted.len(), 2, "{submitted:?}\n{}", outcome.log);
        assert!(submitted[0].contains("trc2_query_failure"));
        assert_eq!(submitted[1], "SELECT 1 AS id");
        let before = crate::golden::GoldenFile::parse(original).unwrap();
        let after = crate::golden::GoldenFile::read(&path).unwrap();
        assert_eq!(after.sections[0], before.sections[0]);
        assert!(
            matches!(&after.sections[1].blocks[0], crate::golden::Block::Typed { types, names, rows } if types == &["bigint"] && names == &["id"] && rows == &[vec!["1".to_string()]])
        );
        let record = &rows(root.path())[0];
        assert_eq!(record["status"], "fail");
        assert_eq!(record["failure"]["statement_index"], 1);
        assert!(record["oracle"]["executed"].as_array().unwrap().iter().any(
            |oracle| oracle["step"] == 2
                && oracle["kind"] == "legacy_conversion"
                && oracle["outcome"] == "pass"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn conversion_http_failure_stops_later_steps_even_when_its_body_mentions_trino_error() {
        let root = tempfile::tempdir().unwrap();
        let peer = TrinoSetupPeer::start();
        let ps = prepared_conversion(
            root.path(),
            &peer,
            &["SELECT trc2_http_failure AS id", "SELECT 1 AS id"],
        );
        let path = root.path().join("result/one.result");
        let original = "-- query 1\n-- legacy\nid\n9\n\n-- query 2\n-- legacy\nid\n1\n";
        fs::write(&path, original).unwrap();
        let outcome = crate::run_case(&ps.ctx, &ps.cases[0], &AtomicBool::new(false));
        assert_eq!(outcome.status, CaseStatus::Fail);
        let submitted = peer.finish();
        assert_eq!(
            submitted.len(),
            1,
            "HTTP failure must stop the case: {submitted:?}\n{}",
            outcome.log
        );
        assert!(submitted[0].contains("trc2_http_failure"));
        assert!(
            outcome.log.contains("TrinoHttp { status=503 }"),
            "{}",
            outcome.log
        );
        assert_eq!(fs::read_to_string(path).unwrap(), original);
        assert!(
            !rows(root.path())[0]["oracle"]["executed"]
                .as_array()
                .unwrap()
                .iter()
                .any(|oracle| oracle["step"] == 2)
        );
    }

    #[cfg(unix)]
    #[test]
    fn conversion_step_epsilon_overrides_the_suite_policy_in_both_directions() {
        for (suite_epsilon, step_epsilon, passes) in [(0.1, 0.001, false), (0.001, 0.1, true)] {
            let root = tempfile::tempdir().unwrap();
            let peer = TrinoSetupPeer::start();
            let mut ps =
                prepared_conversion(root.path(), &peer, &["SELECT CAST(1.05 AS DOUBLE) AS id"]);
            ps.ctx.float_epsilon = Some(suite_epsilon);
            ps.cases[0].steps[0].meta.float_epsilon = Some(step_epsilon);
            let path = root.path().join("result/one.result");
            let original = "-- query 1\n-- legacy\nid\n1.0\n";
            fs::write(&path, original).unwrap();
            let outcome = crate::run_case(&ps.ctx, &ps.cases[0], &AtomicBool::new(false));
            assert_eq!(
                outcome.status,
                if passes {
                    CaseStatus::Pass
                } else {
                    CaseStatus::Fail
                },
                "suite={suite_epsilon}, step={step_epsilon}: {}",
                outcome.log
            );
            assert_eq!(peer.finish().len(), 1);
            let after = crate::golden::GoldenFile::read(&path).unwrap();
            if passes {
                assert!(
                    matches!(&after.sections[0].blocks[0], crate::golden::Block::Typed { types, rows, .. } if types == &["double"] && rows == &[vec!["1.05".to_string()]])
                );
            } else {
                assert_eq!(fs::read_to_string(path).unwrap(), original);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn successful_legacy_verify_records_pending_conversion_separately_from_mismatch() {
        let root = tempfile::tempdir().unwrap();
        let peer = TrinoSetupPeer::start();
        let mut ps = prepared_conversion(root.path(), &peer, &["SELECT 1 AS id"]);
        ps.ctx.mode = crate::Mode::Verify;
        ps.ctx.convert_legacy = false;
        ps.ctx.legacy_variant_offset_seconds = None;
        let path = root.path().join("result/one.result");
        let original = "-- query 1\n-- legacy\nid\n1\n";
        fs::write(&path, original).unwrap();
        let outcome = crate::run_case(&ps.ctx, &ps.cases[0], &AtomicBool::new(false));
        assert_eq!(outcome.status, CaseStatus::Fail);
        assert_eq!(peer.finish().len(), 1);
        let record = &rows(root.path())[0];
        assert_eq!(record["failure"]["class"], "error");
        assert_eq!(record["failure"]["reason"], "legacy_pending_conversion");
        assert_eq!(record["failure"]["statement_index"], 1);
        assert_eq!(fs::read_to_string(path).unwrap(), original);
    }

    #[cfg(unix)]
    #[test]
    fn cli_float_epsilon_records_effective_policy_and_still_compares() {
        in_cli_test_process(
            "results_record::lifecycle_tests::cli_float_epsilon_records_effective_policy_and_still_compares",
            |root| {
                use clap::Parser;
                let (config, sql_dir, result_dir) =
                    local_cli_inputs(root, "-- query 1\nSELECT CAST(1.05 AS DOUBLE) AS id;\n");
                let original_expected = "-- query 1\n-- types\tdouble\nid\n1.0\n";
                fs::write(result_dir.join("one.result"), original_expected).unwrap();
                for (name, epsilon, expected_code, status) in [
                    ("within", "0.1", 0, "pass"),
                    ("outside", "0.001", 1, "fail"),
                ] {
                    let peer = TrinoSetupPeer::start();
                    let port = peer.port.to_string();
                    let path = root.join(format!("{name}.jsonl"));
                    let cli = crate::Cli::try_parse_from([
                        "sql-test",
                        "--suite",
                        "session",
                        "--config",
                        config.to_str().unwrap(),
                        "--sql-dir",
                        sql_dir.to_str().unwrap(),
                        "--result-dir",
                        result_dir.to_str().unwrap(),
                        "--results-file",
                        path.to_str().unwrap(),
                        "--cluster-mode",
                        "all-in-one",
                        "--host",
                        "127.0.0.1",
                        "--trino-port",
                        &port,
                        "--target-session-sql",
                        "SET pipeline_dop = 2",
                        "--float-epsilon",
                        epsilon,
                        "--failure-artifact-dir",
                        root.join("failures").to_str().unwrap(),
                        "-j",
                        "1",
                    ])
                    .unwrap();
                    assert_eq!(crate::run_cli(cli).unwrap(), expected_code);
                    assert!(
                        peer.finish()
                            .iter()
                            .any(|query| query == "SET pipeline_dop = 2")
                    );
                    let records = fs::read_to_string(path)
                        .unwrap()
                        .lines()
                        .map(|line| serde_json::from_str::<Value>(line).unwrap())
                        .collect::<Vec<_>>();
                    let policy = epsilon.parse::<f64>().unwrap();
                    assert_eq!(records[0]["verification"]["float_epsilon"], policy);
                    let case = records
                        .iter()
                        .find(|record| record["kind"] == "case")
                        .unwrap();
                    assert_eq!(case["status"], status);
                    assert_eq!(
                        case["oracle"]["required"],
                        json!([{"step":1,"kind":"result_set"}])
                    );
                    assert_eq!(
                        case["oracle"]["executed"],
                        json!([{"step":1,"kind":"result_set","outcome":status,"policy":{"float_epsilon":policy,"order_sensitive":false,"explain_rewritten":false}}])
                    );
                    if expected_code != 0 {
                        assert_eq!(case["failure"]["class"], "mismatch");
                        assert_eq!(case["failure"]["statement_index"], 1);
                    }
                    assert_eq!(records.last().unwrap()["exit_code"], expected_code);
                }
                assert_eq!(
                    fs::read_to_string(result_dir.join("one.result")).unwrap(),
                    original_expected
                );
            },
        );
    }

    #[test]
    fn selected_program_hash_is_the_actual_discovered_binary() {
        use clap::Parser;
        const CHILD: &str = "NOVAROCKS_RESULTS_RECORD_PROGRAM_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let dir = tempfile::tempdir().unwrap();
            let binary = dir.path().join("novarocks");
            fs::write(&binary, b"selected server contents").unwrap();
            let output = dir.path().join("observed-program.json");
            // Isolate environment observation from every parallel in-process test.
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "results_record::lifecycle_tests::selected_program_hash_is_the_actual_discovered_binary"])
                .env(CHILD, "1")
                .env("NOVAROCKS_BIN", &binary)
                .env("NOVAROCKS_RESULTS_RECORD_PROGRAM_OUTPUT", &output)
                .status()
                .unwrap();
            assert!(status.success());
            let observed: Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
            assert_eq!(
                observed["server"]["sha256"],
                digest(b"selected server contents")
            );
            return;
        }
        let cli = crate::Cli::try_parse_from([
            "sql-test",
            "--suite",
            "optimizer",
            "--cluster-mode",
            "cross-process",
        ])
        .unwrap();
        let root = crate::resolve_repo_root().unwrap();
        let record = run_record(&cli, &root, None, &["optimizer".into()], None).unwrap();
        let platform = &record["platform"];
        for key in ["system", "machine", "release"] {
            assert!(!platform[key].as_str().unwrap().is_empty());
        }
        if cfg!(target_os = "macos") {
            assert_eq!(platform["system"], "Darwin");
        } else if cfg!(target_os = "linux") {
            assert_eq!(platform["system"], "Linux");
        }
        assert_eq!(
            record["server"]["path"],
            std::env::var("NOVAROCKS_BIN").unwrap()
        );
        assert_eq!(
            record["server"]["sha256"],
            digest(b"selected server contents")
        );
        assert_eq!(record["server"]["launched"], "novarocks-bin");
        assert_eq!(record["convert_legacy"], false);
        assert!(record["legacy_variant_offset_seconds"].is_null());
        fs::write(
            std::env::var("NOVAROCKS_RESULTS_RECORD_PROGRAM_OUTPUT").unwrap(),
            serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let cli = crate::Cli::try_parse_from([
            "sql-test",
            "--suite",
            "optimizer",
            "--cluster-mode",
            "cross-process",
            "--dry-run",
            "--mode",
            "record",
            "--convert-legacy",
            "--legacy-variant-offset-seconds",
            "-28800",
        ])
        .unwrap();
        let record = run_record(&cli, &root, None, &["optimizer".into()], None).unwrap();
        assert_eq!(record["server"]["path"], Value::Null);
        assert_eq!(record["server"]["launched"], "external");
        assert_eq!(record["mode"], "record");
        assert_eq!(record["convert_legacy"], true);
        assert_eq!(record["legacy_variant_offset_seconds"], -28800);
    }
    #[test]
    fn tracked_input_cannot_be_rebound_after_a_concurrent_change() {
        let dir = tempfile::tempdir().unwrap();
        let writer =
            ResultsWriter::create(dir.path(), dir.path().join("results.jsonl").to_str()).unwrap();
        let file = dir.path().join("server");
        fs::write(&file, b"old").unwrap();
        let recorded = writer.track_file(&file).unwrap();
        assert_eq!(recorded, digest(b"old"));
        fs::write(&file, b"new").unwrap();
        assert!(writer.track_file(&file).is_err());
        assert!(!writer.inputs_stable());
    }
    #[test]
    fn effective_configuration_snapshot_hashes_sources_and_keeps_secrets_private() {
        use clap::Parser;
        let dir = tempfile::tempdir().unwrap();
        let writer =
            ResultsWriter::create(dir.path(), dir.path().join("results.jsonl").to_str()).unwrap();
        let corpus = dir.path().join("tests/sql/correctness");
        fs::create_dir_all(&corpus).unwrap();
        fs::write(
            corpus.join("corpus.toml"),
            "[session]\ntime_zone = \"Asia/Shanghai\"\n",
        )
        .unwrap();
        let source = dir.path().join("runner.conf");
        fs::write(&source, "password=secret").unwrap();
        let mut config = crate::types::RunnerConfig {
            path: Some(source.clone()),
            ..crate::types::RunnerConfig::default()
        };
        config.values.insert("password".into(), "secret".into());
        let cli = crate::Cli::try_parse_from([
            "sql-test",
            "--suite",
            "optimizer",
            "--mode",
            "record",
            "--convert-legacy",
            "--legacy-variant-offset-seconds",
            "-28800",
            "--trino-port",
            "8088",
        ])
        .unwrap();
        let path = writer
            .capture_configuration(&cli, dir.path(), &config)
            .unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(!text.contains("secret"));
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["source_hashes"]["runner"], digest(b"password=secret"));
        assert_eq!(
            value["client_capabilities"],
            novarocks_test_trino_client::CLIENT_CAPABILITIES
        );
        assert_eq!(value["corpus_session"]["time_zone"], "Asia/Shanghai");
        assert_eq!(value["cli"]["convert_legacy"], true);
        assert_eq!(value["cli"]["legacy_variant_offset_seconds"], -28800);
        assert_eq!(value["cli"]["trino_port"], "8088");
        assert_eq!(
            value["source_hashes"]["corpus_session"],
            hash_file(&corpus.join("corpus.toml")).unwrap()
        );
        assert!(value["cli"].get("mysql").is_none());
        assert!(value["cli"].get("password").is_none());
        assert!(value["cli"].get("ref_host").is_none());
        assert!(writer.inputs_stable());
        fs::write(source, "password=changed").unwrap();
        assert!(!writer.inputs_stable());
    }
    #[test]
    fn declared_environment_distinguishes_unset_and_empty_without_mutating_process_env() {
        let mut m = SuiteManifest::default();
        m.reuse.env_inputs = Some(vec![crate::suite_manifest::EnvInput {
            name: format!("NOVAROCKS_UNSET_{}", now()),
            kind: EnvInputKind::Value,
        }]);
        assert_eq!(
            environment(&m)
                .unwrap()
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap(),
            "unset"
        );
        assert_ne!(digest(b""), "unset");
    }
}
