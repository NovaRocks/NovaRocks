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

//! Private ownership for the existing two-process Hadoop fencing tests.
//! Domain assertions remain in hadoop_catalog_fencing.rs.

use anyhow::{Context, Result, anyhow, ensure};
use novarocks_cluster_harness::fixture_run::{
    FixturePolicy, FixtureRunContext, RUN_CONTEXT_ENV, RUN_STORAGE_ENV, RunBudget, RunStorageScope,
    ScratchIdentity, canonical_s3, startup_sweep,
};
use novarocks_test_support::{
    CapturedCommandOutcome, ManagedCommand, PayloadLifetimeExpired, capture_command_until,
};
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_WORK_END_ENV: &str = "NR_HADOOP_FENCE_WORK_DEADLINE_UNIX_MS";
const DEFAULT_MINIO_RUN_SECONDS: u64 = 900;

struct SharedRun {
    repo: PathBuf,
    publication: PathBuf,
    budget: RunBudget,
    storage: RunStorageScope,
    warehouse: String,
    owns_run: bool,
}

/// The test owns its actual children; only a standalone shared run also owns
/// storage reclamation. No destructor performs storage deletion.
pub struct RunOwner {
    shared: Option<SharedRun>,
    work_end: Instant,
    complete_end: Instant,
    environment: BTreeMap<String, String>,
    writers_settled: bool,
}

impl RunOwner {
    /// Local process races still receive a cumulative work clock and an
    /// independent monitor that settles their complete process groups.
    pub fn local() -> Result<Self> {
        let work_end = Instant::now() + Duration::from_secs(90);
        // Establish the run lease before a caller installs a short payload
        // clock and before either concurrent child can initialize it lazily.
        novarocks_test_support::ensure_run_retention(work_end)?;
        let mut environment = BTreeMap::new();
        environment.insert(
            CHILD_WORK_END_ENV.to_owned(),
            wall_deadline(work_end)?.to_string(),
        );
        Ok(Self {
            shared: None,
            work_end,
            complete_end: work_end + Duration::from_secs(10),
            environment,
            writers_settled: true,
        })
    }

    /// Bind one immutable ready publication. This never starts or upgrades the
    /// shared machine, and direct nextest needs no warehouse wrapper.
    pub fn minio(repo: &Path) -> Result<Self> {
        let repo = repo.canonicalize().context("resolve fencing repository")?;
        let publication = std::env::var_os("NOVA_ENV_REST_ENV_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| repo.join("docker/iceberg-rest/runtime/current/published/env.sh"))
            .canonicalize()
            .context("BLOCKED: fencing requires a pinned ready fixture publication")?;
        let publication_dir = publication
            .parent()
            .context("fixture publication directory")?;
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(publication_dir.join("manifest.json"))?)?;
        ensure!(
            manifest["ready"] == true
                && manifest["shared_docker"] == true
                && manifest["runtime"]["machine"]["phase"] == "ready",
            "BLOCKED: fencing requires a ready shared machine publication"
        );
        let recorded_publication = required_string(&manifest["runtime"], "publication_dir")?;
        ensure!(
            Path::new(recorded_publication).canonicalize()? == publication_dir,
            "BLOCKED: fencing publication identity differs from its immutable directory"
        );
        let machine = &manifest["runtime"]["machine"];
        let policy: FixturePolicy = serde_json::from_value(machine["policy"].clone())?;
        let inherited = match std::env::var(RUN_CONTEXT_ENV) {
            Ok(value) if value != "null" => Some(
                serde_json::from_str::<FixtureRunContext>(&value)
                    .context("BLOCKED: invalid inherited fencing context")?,
            ),
            Ok(_) | Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        };
        let owns_run = inherited.is_none();
        let identity = match &inherited {
            Some(context) => context.identity.clone(),
            None => ScratchIdentity::create(required_string(
                &manifest["novarocks"],
                "iceberg_catalog_warehouse",
            )?)?,
        };
        let seconds = match std::env::var("NOVA_FIXTURE_BUDGET_SECS") {
            Ok(value) => value.parse().context("invalid fencing run budget")?,
            Err(std::env::VarError::NotPresent) => DEFAULT_MINIO_RUN_SECONDS,
            Err(error) => return Err(error.into()),
        };
        let budget = RunBudget::admit(policy, identity, seconds, inherited)?;
        // from_publication checks the actual machine policy, protected roots,
        // and any supplied storage projection against this exact identity.
        let storage = RunStorageScope::from_publication(&publication, &budget)?;
        let warehouse = match std::env::var("NR_HADOOP_FENCE_MINIO_WAREHOUSE") {
            Ok(value) => canonical_s3(&value)?,
            Err(std::env::VarError::NotPresent) => format!(
                "{}/hadoop-fencing/{}",
                budget.identity().scope_root,
                uuid::Uuid::new_v4().simple(),
            ),
            Err(error) => return Err(error.into()),
        };
        ensure!(
            storage.belongs(&warehouse)?,
            "BLOCKED: fencing warehouse is outside the original run storage scope"
        );

        let mut environment = BTreeMap::new();
        environment.insert(RUN_CONTEXT_ENV.to_owned(), budget.to_json()?);
        environment.insert(RUN_STORAGE_ENV.to_owned(), storage.to_json()?);
        environment.insert(
            "NOVA_ENV_REST_ENV_FILE".to_owned(),
            publication.to_string_lossy().into_owned(),
        );
        environment.insert(
            CHILD_WORK_END_ENV.to_owned(),
            budget
                .context()
                .deadline_unix_ms
                .checked_sub(
                    budget
                        .context()
                        .cleanup_grace_seconds
                        .checked_mul(1000)
                        .context("fencing cleanup reserve overflow")?,
                )
                .context("invalid fencing work deadline")?
                .to_string(),
        );
        for (variable, field) in [
            ("AWS_S3_ENDPOINT", "endpoint"),
            ("AWS_S3_ACCESS_KEY_ID", "access_key_id"),
            ("AWS_S3_SECRET_ACCESS_KEY", "secret_access_key"),
        ] {
            environment.insert(
                variable.to_owned(),
                required_string(&manifest["minio"], field)?.to_owned(),
            );
        }
        environment.insert("AWS_REGION".to_owned(), "us-east-1".to_owned());
        eprintln!(
            "HADOOP_FENCE_RUN owner={} context={} storage={} warehouse={}",
            owns_run,
            budget.to_json()?,
            storage.to_json()?,
            warehouse
        );
        startup_sweep(&repo, &publication, &budget)?;
        Ok(Self {
            work_end: budget.work_deadline(),
            complete_end: budget.deadline(),
            environment,
            shared: Some(SharedRun {
                repo,
                publication,
                budget,
                storage,
                warehouse,
                owns_run,
            }),
            writers_settled: true,
        })
    }

    pub fn warehouse(&self) -> Result<&str> {
        self.shared
            .as_ref()
            .map(|run| run.warehouse.as_str())
            .context("local fencing uses its caller-owned temporary warehouse")
    }

    pub fn work_deadline(&self) -> Instant {
        self.work_end
    }

    /// Parent-side capability checks and the independent storage audit use
    /// the same immutable publication credentials as the monitored children.
    pub fn object_store_config(
        &self,
    ) -> Option<novarocks_connector_iceberg::novarocks_fs::ObjectStoreConfig> {
        use novarocks_connector_iceberg::novarocks_fs::{ObjectStoreConfig, SecretValue};
        self.shared.as_ref().map(|_| ObjectStoreConfig {
            endpoint: self.environment["AWS_S3_ENDPOINT"].clone(),
            access_key_id: SecretValue::new(self.environment["AWS_S3_ACCESS_KEY_ID"].clone()),
            access_key_secret: SecretValue::new(
                self.environment["AWS_S3_SECRET_ACCESS_KEY"].clone(),
            ),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some(self.environment["AWS_REGION"].clone()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        })
    }

    pub fn check_work(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.work_end,
            "fencing original work deadline expired"
        );
        Ok(())
    }

    /// Apply the original clocks and scope to both requests before either
    /// child starts. File-backed captures run concurrently so neither waits at
    /// the storage barrier before its peer has been launched.
    pub fn capture_pair(
        &mut self,
        mut commands: [ManagedCommand; 2],
    ) -> Result<[CapturedCommandOutcome; 2]> {
        ensure!(
            self.writers_settled,
            "previous fencing writers have no confirmed exit"
        );
        ensure!(
            Instant::now() < self.work_end,
            "fencing original work deadline expired"
        );
        for command in &mut commands {
            command
                .envs(&self.environment)
                .terminate_at(self.work_end, self.complete_end);
            if self.shared.is_some() {
                // The immutable shared publication uses its own static keys;
                // an ambient session token belongs to a different selection.
                command.env_remove("AWS_SESSION_TOKEN");
            }
        }
        // Allocate all local output handles before any child can be launched.
        let output = [
            (tempfile::tempfile()?, tempfile::tempfile()?),
            (tempfile::tempfile()?, tempfile::tempfile()?),
        ];
        self.writers_settled = false;
        let deadline = self.work_end;
        let joined = std::thread::scope(|scope| {
            let [first, second] = commands;
            let [(first_out, first_err), (second_out, second_err)] = output;
            let first =
                scope.spawn(move || capture_command_until(first, first_out, first_err, deadline));
            let second = scope
                .spawn(move || capture_command_until(second, second_out, second_err, deadline));
            // Always join both before inspecting either result, including when
            // one capture panics or its child fails before the shared barrier.
            [first.join(), second.join()]
        });
        self.writers_settled = joined.iter().all(|result| match result {
            Ok(Ok(_)) => true,
            // This typed error is only emitted after a clean monitor Final
            // and actual wait. Expired work remains failed after safe teardown.
            Ok(Err(error)) => error.is::<PayloadLifetimeExpired>(),
            Err(_) => false,
        });
        let mut captured = Vec::new();
        let mut failures = Vec::new();
        for (index, result) in joined.into_iter().enumerate() {
            match result {
                Ok(Ok(outcome)) => {
                    eprintln!(
                        "HADOOP_FENCE_CHILD child={index} status={} timed_out={} failures={:?} diagnostics={}",
                        outcome.output.status,
                        outcome.timed_out,
                        outcome.failures,
                        outcome.diagnostic_root().display()
                    );
                    if outcome.timed_out || !outcome.failures.is_empty() {
                        failures.push(format!(
                            "child {index} capture failed; diagnostics={}",
                            outcome.diagnostic_root().display()
                        ));
                    }
                    captured.push(outcome);
                }
                Ok(Err(error)) => failures.push(format!("child {index} monitor: {error:#}")),
                Err(_) => failures.push(format!(
                    "child {index} capture panicked; monitor diagnostics retained"
                )),
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("; "));
        captured
            .try_into()
            .map_err(|_| anyhow!("fencing capture did not return both child results"))
    }

    /// Keep the original assertion panic or returned failure while settling
    /// the run's separate reclamation obligation. Call once around both races.
    pub fn run(mut self, body: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        let primary = catch_unwind(AssertUnwindSafe(|| {
            self.check_work()?;
            body(&mut self)?;
            self.check_work()
        }));
        let cleanup = self.cleanup();
        match primary {
            Ok(Ok(())) => cleanup,
            Ok(Err(error)) => match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("fencing cleanup also failed: {cleanup:#}")))
                }
            },
            Err(panic) => {
                if let Err(error) = cleanup {
                    eprintln!("HADOOP_FENCE_CLEANUP failed_after_assertion={error:#}");
                }
                resume_unwind(panic)
            }
        }
    }

    fn cleanup(&self) -> Result<()> {
        ensure!(
            self.writers_settled,
            "fencing writer exit is unconfirmed; original run cleanup remains pending"
        );
        let Some(run) = &self.shared else {
            return Ok(());
        };
        if !run.owns_run {
            eprintln!(
                "HADOOP_FENCE_CLEANUP status=deferred run_id={} writers_settled=true publication={}",
                run.budget.identity().run_id,
                run.publication.display()
            );
            return Ok(());
        }
        let mut command = ManagedCommand::new("python3");
        command
            .current_dir(&run.repo)
            .arg(run.repo.join("docker/iceberg-rest/fixture_run.py"))
            .arg("cleanup-run")
            .arg("--env-file")
            .arg(&run.publication)
            .env(RUN_CONTEXT_ENV, run.budget.to_json()?)
            .env(RUN_STORAGE_ENV, run.storage.to_json()?)
            .env("NOVA_FIXTURE_CLEANUP_MODE", "true")
            .terminate_at(
                self.complete_end - Duration::from_secs(1),
                self.complete_end,
            );
        let outcome = capture_command_until(
            command,
            tempfile::tempfile()?,
            tempfile::tempfile()?,
            self.complete_end,
        )?;
        eprintln!(
            "HADOOP_FENCE_CLEANUP status={} stdout={} stderr={} diagnostics={}",
            outcome.output.status,
            String::from_utf8_lossy(&outcome.output.stdout),
            String::from_utf8_lossy(&outcome.output.stderr),
            outcome.diagnostic_root().display()
        );
        ensure!(
            !outcome.timed_out && outcome.failures.is_empty() && outcome.output.status.success(),
            "fencing original run cleanup failed; diagnostics={}",
            outcome.diagnostic_root().display()
        );
        let report: serde_json::Value = serde_json::from_slice(&outcome.output.stdout)?;
        ensure!(
            report["complete"] == true,
            "fencing original run cleanup is incomplete; diagnostics={}",
            outcome.diagnostic_root().display()
        );
        // Keep the native cleanup report independently of the primary result.
        // The outer run's retention owner controls its eventual reclamation.
        Ok(())
    }
}

/// The child uses this clock for its barrier and request contexts, rather than
/// minting a fresh complete run budget for each table operation.
pub fn child_work_deadline() -> Result<Instant> {
    let deadline: u64 = std::env::var(CHILD_WORK_END_ENV)
        .context("fencing child omitted its parent's work deadline")?
        .parse()?;
    let remaining = deadline
        .checked_sub(unix_ms()?)
        .context("fencing original work deadline expired")?;
    ensure!(remaining > 0, "fencing original work deadline expired");
    Instant::now()
        .checked_add(Duration::from_millis(remaining))
        .context("fencing child monotonic deadline overflow")
}

fn required_string<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("BLOCKED: fixture publication omitted {key}"))
}

fn unix_ms() -> Result<u64> {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .context("fencing wall clock overflow")
}

fn wall_deadline(deadline: Instant) -> Result<u64> {
    unix_ms()?
        .checked_add(u64::try_from(
            deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )?)
        .context("fencing wall deadline overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sleeping_child(pid_file: &Path) -> ManagedCommand {
        let mut command = ManagedCommand::new("/bin/sh");
        command
            .args([
                "-c",
                "printf '%s\\n' \"$$\" > \"$1\"; exec /bin/sleep 60",
                "fencing-timeout-child",
            ])
            .arg(pid_file);
        command
    }

    fn assert_process_absent(pid: u32, complete_end: Instant) -> Result<()> {
        let mut probe = ManagedCommand::new("/bin/kill");
        probe
            .arg("-0")
            .arg(pid.to_string())
            .terminate_at(complete_end - Duration::from_secs(1), complete_end);
        let outcome = capture_command_until(
            probe,
            tempfile::tempfile()?,
            tempfile::tempfile()?,
            complete_end,
        )?;
        ensure!(
            !outcome.timed_out && outcome.failures.is_empty(),
            "process-exit observation did not settle; diagnostics={}",
            outcome.diagnostic_root().display()
        );
        ensure!(
            !outcome.output.status.success(),
            "fencing child {pid} still exists after the parent observed clean monitor completion"
        );
        outcome.release_diagnostics()?;
        Ok(())
    }

    /// The private caller must retain the actual child groups until both
    /// monitors settle. Work expiry stays failed and cannot authorize a new
    /// pair, while a separate complete clock permits the local cleanup path.
    #[test]
    fn expired_pair_waits_for_actual_children_and_refuses_new_work() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let pid_files = [
            directory.path().join("first.pid"),
            directory.path().join("second.pid"),
        ];
        let mut owner = RunOwner::local()?;
        owner.work_end = Instant::now() + Duration::from_secs(2);
        owner.complete_end = owner.work_end + Duration::from_secs(10);
        owner.environment.insert(
            CHILD_WORK_END_ENV.to_owned(),
            wall_deadline(owner.work_end)?.to_string(),
        );
        let original_work = owner.work_end;
        let original_complete = owner.complete_end;
        let result =
            owner.capture_pair([sleeping_child(&pid_files[0]), sleeping_child(&pid_files[1])]);
        let capture_error = result
            .as_ref()
            .err()
            .context("sleeping children cannot finish within the original work budget")?;
        ensure!(
            Instant::now() >= original_work,
            "the original work deadline must actually expire"
        );
        ensure!(
            owner.work_end == original_work && owner.complete_end == original_complete,
            "child settlement must not renew either original clock"
        );
        // capture_pair sets this fact only for captures whose monitor has
        // actually exited with a validated clean Final, or the typed expiry
        // emitted after that same validation. Check real process absence as an
        // independent observation rather than accepting the failure alone.
        ensure!(
            owner.writers_settled,
            "both actual monitor Final records must prove clean settlement; original capture_pair error: {capture_error:#}"
        );
        let pids = pid_files.map(|file| -> Result<u32> {
            Ok(std::fs::read_to_string(file)
                .context("the timeout test requires both real sleeping children to have started")?
                .trim()
                .parse()?)
        });
        let [first, second] = pids;
        let (first, second) = (first?, second?);
        ensure!(
            first != second,
            "the timeout test requires two independent child processes"
        );
        assert_process_absent(first, original_complete)?;
        assert_process_absent(second, original_complete)?;
        owner
            .cleanup()
            .context("confirmed writer exit permits the local cleanup path")?;

        let forbidden = directory.path().join("must-not-start");
        let new_child = || {
            let mut command = ManagedCommand::new("/bin/sh");
            command
                .args(["-c", "printf started > \"$1\"", "fencing-forbidden-child"])
                .arg(&forbidden);
            command
        };
        ensure!(
            owner.capture_pair([new_child(), new_child()]).is_err(),
            "an expired run must refuse another pair before either process starts"
        );
        ensure!(!forbidden.exists(), "expired work started a new child");
        ensure!(
            owner.writers_settled,
            "refusing new work must preserve the original settlement fact"
        );
        ensure!(
            owner.run(|_| Ok(())).is_err(),
            "a clean local teardown cannot turn expired work into success"
        );
        Ok(())
    }
}
