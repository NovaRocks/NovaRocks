// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file for additional
// information. Licensed under the Apache License, Version 2.0.

//! Declared Trino reference oracle and official clients: explicit, fixture-heavy G1 assets.
//! Same-signature values have no divergence allowance. The independent producer
//! is Spark SQL plus Iceberg Java records inside the same original Spark JVM.
use crate::scenario::{
    Scenario, ScenarioContext, ScenarioDeclaration, ScenarioEnvironmentInput, ScenarioHeavyReason,
    ScenarioImpact, ScenarioLaunchConfig,
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::{RunBudget, RunStorageScope};
use novarocks_cluster_harness::reference_inputs::{ReferenceInput, ResolvedReference};
use novarocks_cluster_harness::resource_registration::{Registration, register};
use novarocks_test_support::ManagedCommand;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy)]
enum Mode {
    Encoding,
    Cli,
    Jdbc,
}
impl Mode {
    const fn selector(self) -> &'static str {
        match self {
            Self::Encoding => "encoding-conformance",
            Self::Cli => "official-cli",
            Self::Jdbc => "jdbc-probe",
        }
    }
    const fn name(self) -> &'static str {
        match self {
            Self::Encoding => "trino-interop/encoding-conformance",
            Self::Cli => "trino-interop/official-cli",
            Self::Jdbc => "trino-interop/jdbc-probe",
        }
    }
}

struct Prepared {
    budget: RunBudget,
    publication: PathBuf,
    storage: String,
    probe: Option<ProbeOwner>,
}
struct ProbeOwner {
    reference: ResolvedReference,
    registration: Registration,
    root: PathBuf,
    nova: String,
    metrics: String,
}
struct TrinoInterop {
    mode: Mode,
    prepared: Mutex<Option<Prepared>>,
}
impl TrinoInterop {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            prepared: Mutex::new(None),
        }
    }
}
pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Mode::Encoding, Mode::Cli, Mode::Jdbc]
        .into_iter()
        .map(|mode| Box::new(TrinoInterop::new(mode)) as Box<dyn Scenario>)
        .collect()
}

impl Scenario for TrinoInterop {
    fn name(&self) -> &'static str {
        self.mode.name()
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn reference_inputs(&self) -> &'static [ReferenceInput] {
        match self.mode {
            Mode::Encoding | Mode::Cli => &[ReferenceInput::TrinoImage],
            Mode::Jdbc => &[ReferenceInput::TrinoImage, ReferenceInput::TrinoJdbcDriver],
        }
    }
    fn declaration(&self) -> ScenarioDeclaration {
        ScenarioDeclaration {
            reference_inputs: self.reference_inputs().to_vec(),
            impact: ScenarioImpact {
                paths: [
                    "novarocks/trino-adapter/**",
                    "novarocks/result-render/**",
                    "novarocks/result-contract/**",
                    "novarocks/query-application/**",
                    "novarocks/frontend-application/**",
                    "novarocks/sql/**",
                    "novarocks/native-adapter/**",
                    "novarocks/connector/iceberg/**",
                    "novarocks-server/**",
                    "tests/trino-client/**",
                    "tests/cluster-harness/**",
                    "tests/test-support/**",
                    "novarocks/failpoint/**",
                    "tests/system-test-runner/src/scenarios/trino_interop.rs",
                    "tests/system-test-runner/src/scenarios/trino_interop/**",
                    "tests/system-test-runner/src/*.rs",
                    "docker/iceberg-rest/**",
                    "docker/fixture-inputs/**",
                    "tests/datasketches-tck/interop/trino/rest-catalog/**",
                    "tests/datasketches-tck/interop/trino/verify_rest_catalog.sh",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            },
            env_inputs: [
                ("NOVA_ENV_REST_ENV_FILE", "file-content"),
                ("DOCKER_HOST", "value"),
                ("DOCKER_CONTEXT", "value"),
                ("DOCKER_CONFIG", "value"),
                ("NOVA_FIXTURE_STORE", "value"),
            ]
            .into_iter()
            .map(|(name, kind)| ScenarioEnvironmentInput { name, kind })
            .collect(),
            initial_state: Some("isolated"),
            basis: Some(
                "Fresh native FE/BE processes, original fixture RunBudget namespace and explicit independent Spark data; exact Docker IDs and catalog-first cleanup are registered before side effects.",
            ),
            heavy: Some(ScenarioHeavyReason {
                reason: "external-programs",
                detail: "Requires the declared Trino reference image, Linux host networking, original Spark/Iceberg REST fixture and native 1FE+3BE. JDBC uses the declared driver mounted read-only. Reference inputs are resolved before cluster startup; unavailable Docker/host networking is BLOCKED.",
            }),
        }
    }
    fn launch_config_with_fixture_run(
        &self,
        _root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        budget.check_work()?;
        let publication = std::env::var_os("NOVA_ENV_REST_ENV_FILE")
            .map(PathBuf::from)
            .ok_or_else(|| {
                novarocks_cluster_harness::fixture_run::rejected(anyhow::anyhow!(
                    "missing generated REST fixture publication"
                ))
            })?
            .canonicalize()?;
        let storage = RunStorageScope::from_publication(&publication, budget)?.to_json()?;
        *self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("Trino interop preparation lock poisoned"))? =
            Some(Prepared {
                budget: budget.clone(),
                publication,
                storage,
                probe: None,
            });
        Ok(ScenarioLaunchConfig::default())
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        super::connector::require_three_backends(context)?;
        let epoch = Instant::now();
        super::result_delivery_support::await_idle(context, "trino", "interop-before", epoch)?;
        let root = context.scenario_root().join("released-client-probe");
        let nova = format!("http://127.0.0.1:{}", context.trino_port());
        let metrics = format!(
            "http://127.0.0.1:{}/metrics?type=json",
            context.fe_http_port()
        );
        let mut slot = self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("Trino interop owner lock poisoned"))?;
        let prepared = slot
            .as_mut()
            .context("Trino interop was not prepared with original fixture budget")?;
        prepared.budget.check_work()?;
        let registration = register(
            context.scenario_root(),
            "trino-interop",
            self.mode.selector(),
            "Released Trino image children and independent Spark REST tables; exact identities are in released-client-probe/resource-intents.json",
        )?;
        prepared.probe = Some(ProbeOwner {
            reference: context.reference().clone(),
            registration,
            root: root.clone(),
            nova,
            metrics,
        });
        let owner = prepared.probe.as_ref().expect("original external owner");
        let outcome = invoke(
            self.mode.selector(),
            prepared,
            owner,
            context.scenario_root(),
            context.deadline(),
        )?;
        let code = outcome.output.status.code();
        let clean = read_cleanup(&root).is_ok();
        let primary = if code == Some(75)
            && !outcome.timed_out
            && outcome.failures.is_empty()
            && clean
        {
            Err(novarocks_cluster_harness::fixture_run::rejected(
                anyhow::anyhow!(
                    "{} missing Docker/Linux host-network prerequisite; retained raw result at {}",
                    self.mode.name(),
                    root.display()
                ),
            ))
        } else {
            (|| -> Result<()> {
                ensure!(
                    outcome.group_exit_observed(),
                    "official-client local payload group has not actually exited"
                );
                ensure!(
                    !outcome.timed_out && outcome.failures.is_empty(),
                    "released-client process lifecycle failed: {:?}",
                    outcome.failures
                );
                ensure!(
                    outcome.output.status.success(),
                    "released-client probe exited {code:?}; retained raw responses at {}",
                    root.display()
                );
                let result: Value = serde_json::from_slice(&fs::read(root.join("result.json"))?)?;
                ensure!(
                    result["status"] == "PASS",
                    "released-client probe did not record PASS: {result}"
                );
                Ok(())
            })()
        };
        drop(slot);
        let convergence =
            super::result_delivery_support::await_idle(context, "trino", "interop-after", epoch);
        primary?;
        convergence?;
        context.action(format!("{} completed with the declared Trino reference client/oracle; Variant applicability is retained in probe reports", self.mode.name()));
        outcome.release_diagnostics()?;
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        let mut slot = self
            .prepared
            .lock()
            .map_err(|_| anyhow::anyhow!("Trino interop teardown lock poisoned"))?;
        let Some(prepared) = slot.as_mut() else {
            return Ok(());
        };
        let Some(mut owner) = prepared.probe.take() else {
            return Ok(());
        };
        let cleanup = if read_cleanup(&owner.root).is_ok() {
            Ok(())
        } else if owner.root.join("resource-intents.json").is_file() {
            // The original deadline is reused, including its cleanup grace. A retry
            // preserves the earlier cleanup/result files as prior-* materials.
            let captured = invoke(
                "cleanup",
                prepared,
                &owner,
                owner.root.parent().context("probe retained parent")?,
                prepared.budget.deadline(),
            );
            captured.and_then(|captured| {
                ensure!(
                    captured.output.status.success()
                        && !captured.timed_out
                        && captured.failures.is_empty()
                        && captured.group_exit_observed(),
                    "original interop cleanup process failed"
                );
                read_cleanup(&owner.root)?;
                captured.release_diagnostics()
            })
        } else {
            Err(anyhow::anyhow!(
                "missing original external resource ledger/cleanup receipt; external cleanup remains unknown"
            ))
        };
        let record = owner.registration.settle(&cleanup);
        cleanup?;
        record
    }
}

fn repository() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .context("resolve original workspace")
}
fn read_cleanup(root: &Path) -> Result<()> {
    let cleanup: Value = serde_json::from_slice(&fs::read(root.join("cleanup.json"))?)?;
    ensure!(
        cleanup["complete"] == true && cleanup["errors"].as_array().is_some_and(Vec::is_empty),
        "external cleanup did not complete: {cleanup}"
    );
    Ok(())
}
#[cfg(unix)]
fn bind_probe_lifecycle(
    command: &mut ManagedCommand,
    mode: &str,
    work_deadline: Instant,
    complete_deadline: Instant,
) -> Instant {
    if mode != "cleanup" {
        command.terminate_at(work_deadline, complete_deadline);
    }
    command.observe_group_exit_until(complete_deadline);
    complete_deadline
}

#[cfg(unix)]
fn invoke(
    mode: &str,
    prepared: &Prepared,
    owner: &ProbeOwner,
    artifacts: &Path,
    deadline: Instant,
) -> Result<novarocks_test_support::CapturedCommandOutcome> {
    use std::os::unix::fs::OpenOptionsExt;
    let directory = artifacts.join(if mode == "cleanup" {
        "interop-cleanup-command"
    } else {
        "interop-probe-command"
    });
    fs::create_dir(&directory)?;
    let repository = repository()?;
    let program = repository.join("docker/iceberg-rest/trino_client_protocol_probe.py");
    let mut arguments = vec![
        mode.to_owned(),
        "--env-file".into(),
        prepared.publication.display().to_string(),
        "--nova".into(),
        owner.nova.clone(),
        "--metrics".into(),
        owner.metrics.clone(),
        "--output".into(),
        owner.root.display().to_string(),
    ];
    if mode != "cleanup" {
        let image = owner
            .reference
            .trino_image
            .as_ref()
            .context("run entry omitted the declared Trino image")?;
        arguments.extend(["--trino-image".into(), image.image_id.clone()]);
        if mode == "jdbc-probe" {
            let driver = owner
                .reference
                .trino_jdbc_driver
                .as_ref()
                .context("run entry omitted the declared Trino JDBC driver")?;
            arguments.extend(["--trino-jdbc-jar".into(), driver.path.display().to_string()]);
        }
    }
    let mut recorded_arguments = vec![program.display().to_string()];
    recorded_arguments.extend(arguments.clone());
    fs::write(
        directory.join("command.json"),
        serde_json::to_vec_pretty(
            &json!({"program":"python3", "arguments":recorded_arguments, "original_run":prepared.budget.context()}),
        )?,
    )?;
    let mut command = ManagedCommand::new("python3");
    command
        .arg(program)
        .args(arguments)
        .env("NOVA_ENV_REST_ENV_FILE", &prepared.publication)
        .env("NOVAROCKS_WORKSPACE_ROOT", &repository)
        .env("NOVA_FIXTURE_RUN_CONTEXT", prepared.budget.to_json()?)
        .env("NOVA_FIXTURE_RUN_STORAGE", &prepared.storage);
    if mode == "cleanup" {
        command.env("NOVA_FIXTURE_CLEANUP_MODE", "true");
    }
    let complete_deadline = bind_probe_lifecycle(
        &mut command,
        mode,
        deadline.min(prepared.budget.work_deadline()),
        if mode == "cleanup" {
            deadline.min(prepared.budget.deadline())
        } else {
            prepared.budget.deadline()
        },
    );
    let output = novarocks_test_support::capture_command_until(
        command,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("stdout.log"))?,
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("stderr.log"))?,
        complete_deadline,
    )?;
    fs::write(
        directory.join("exit.json"),
        serde_json::to_vec_pretty(
            &json!({"exit":output.output.status.code(), "timed_out":output.timed_out, "failures":output.failures, "group_exit_observed":output.group_exit_observed()}),
        )?,
    )?;
    Ok(output)
}
#[cfg(not(unix))]
fn invoke(
    _: &str,
    _: &Prepared,
    _: &ProbeOwner,
    _: &Path,
    _: Instant,
) -> Result<novarocks_test_support::CapturedCommandOutcome> {
    Err(novarocks_cluster_harness::fixture_run::rejected(
        anyhow::anyhow!("official client scenarios require Linux host-network execution"),
    ))
}

#[cfg(all(test, unix))]
mod tests;
