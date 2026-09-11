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

use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::BackendTopologyRow;
use novarocks_cluster_harness::process_resources::{
    ProcessLaunchIdentity, recheck_process_launch_identity,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RunManifestReference {
    pub run_id: String,
    pub sha256: String,
    pub descriptor_sha256: Option<String>,
    pub effective_launch_config_sha256: String,
    pub effective_launch_config_semantics_sha256: String,
    pub fixture_realization_sha256: Option<String>,
    pub fixture_realization_semantics_sha256: Option<String>,
    pub raw_artifact_inventory_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RunManifestKind {
    Performance,
    StartupBaseline,
}

pub struct RunManifestHandle {
    path: PathBuf,
    manifest: RunManifest,
    inputs: FrozenRunInputs,
    finished: bool,
}

struct FrozenRunInputs {
    repository: PathBuf,
    binary: PathBuf,
    runner: PathBuf,
    config: PathBuf,
    workload_manifest_source: Option<PathBuf>,
    workload_manifest_artifact: PathBuf,
    fixture_spec: PathBuf,
    effective_launch_config: PathBuf,
    descriptor_source: Option<PathBuf>,
    descriptor_artifact: Option<PathBuf>,
    process_launch_identities: Vec<ProcessLaunchIdentity>,
}

#[derive(Debug, Serialize)]
struct RunManifest {
    schema_version: u32,
    kind: RunManifestKind,
    formal: bool,
    run_id: String,
    scenario: String,
    command: Vec<String>,
    started_unix_millis: u128,
    ended_unix_millis: Option<u128>,
    exit_code: Option<i32>,
    status: String,
    source_revision: String,
    native_build_identity: String,
    source_tree_sha256: String,
    source_dirty: bool,
    binary_sha256: String,
    runner_executable_path: String,
    runner_executable_sha256: String,
    process_identities: Vec<RunProcessIdentity>,
    config_sha256: String,
    workload_manifest_sha256: String,
    fixture_sha256: String,
    tool_tree_sha256: String,
    cargo_lock_sha256: String,
    third_party_build_graph_sha256: String,
    descriptor_sha256: Option<String>,
    effective_launch_config_sha256: String,
    effective_launch_config_semantics_sha256: String,
    fixture_realization_sha256: Option<String>,
    fixture_realization_semantics_sha256: Option<String>,
    raw_artifact_inventory_sha256: Option<String>,
    resources_sha256: Option<String>,
    rustc_version: String,
    cargo_version: String,
    build_profile: String,
    platform: PlatformIdentity,
}

#[derive(Debug, Serialize)]
struct RunProcessIdentity {
    role: String,
    os_pid: u32,
    process_start_token: String,
    application_process_id: Option<String>,
    build_identity: Option<String>,
    binary_sha256: String,
    executable_size_bytes: u64,
    executable_modified_unix_nanos: u64,
}

#[derive(Debug, Serialize)]
struct PlatformIdentity {
    os: String,
    os_version: String,
    architecture: String,
    cpu_model: String,
    logical_cpu_count: usize,
    physical_memory_bytes: u64,
    power_mode: String,
}

pub fn begin_run_manifest(
    context: &mut ScenarioContext,
    scenario: &str,
    workload_manifest_sha256: &str,
    workload_manifest_bytes: &[u8],
    fixture_spec: &[u8],
    formal: bool,
    kind: RunManifestKind,
) -> Result<RunManifestHandle> {
    ensure!(
        is_sha256(workload_manifest_sha256),
        "performance workload manifest SHA256 is missing or malformed"
    );
    ensure!(
        sha256_bytes(workload_manifest_bytes) == workload_manifest_sha256,
        "performance workload manifest bytes do not match their SHA256"
    );
    let repository = repository_root()?;
    let source_revision = command_text(&repository, "git", &["rev-parse", "HEAD"])?;
    let status = command_text(&repository, "git", &["status", "--porcelain=v1"])?;
    ensure!(
        !formal || status.is_empty(),
        "formal UEA-1 measurement requires a clean source checkout"
    );
    let binary = fs::canonicalize(context.primary_binary()).with_context(|| {
        format!(
            "resolve measured binary {}",
            context.primary_binary().display()
        )
    })?;
    if formal {
        ensure_checkout_release_binary(&repository, &binary)?;
    }
    let binary_sha256 = sha256_file(&binary)?;
    let process_launch_identities = {
        let (frontend, backends) = context.process_launch_identities();
        std::iter::once(frontend.clone())
            .chain(backends.iter().cloned())
            .collect::<Vec<_>>()
    };
    let (native_build_identity, process_identities) = observe_native_process_identities(
        context,
        &process_launch_identities,
        &source_revision,
        &binary_sha256,
        formal,
    )?;
    let source_tree_sha256 = source_tree_sha256(&repository, &source_revision, &status)?;
    let (runner, runner_executable_sha256) = runner_executable_identity(&repository, formal)?;
    let runner_executable_path = runner
        .to_str()
        .context("current system-test runner path is not UTF-8")?
        .to_string();
    let config = fs::canonicalize(context.base_config_path()).with_context(|| {
        format!(
            "resolve performance base config {}",
            context.base_config_path().display()
        )
    })?;
    let config_sha256 = sha256_file(&config)?;
    let workload_manifest_source = context
        .uea1_workload_manifest()
        .map(fs::canonicalize)
        .transpose()
        .context("resolve UEA-1 performance workload manifest")?;
    if let Some(path) = &workload_manifest_source {
        ensure!(
            sha256_file(path)? == workload_manifest_sha256,
            "UEA-1 workload manifest source changed before provenance collection"
        );
    }
    let fixture_sha256 = sha256_bytes(fixture_spec);
    let tool_tree_sha256 = tool_tree_sha256(&repository)?;
    let cargo_lock = repository.join("Cargo.lock");
    let cargo_lock_sha256 = sha256_file(&cargo_lock)?;
    let third_party_build_graph_sha256 = third_party_build_graph_sha256(&repository)?;
    let effective_launch_config = context.effective_launch_config_evidence().clone();
    let effective_launch_config_sha256 = effective_launch_config.artifact_sha256().to_string();
    let effective_launch_config_semantics_sha256 =
        effective_launch_config.semantics_sha256().to_string();
    ensure!(
        is_sha256(&effective_launch_config_sha256)
            && is_sha256(&effective_launch_config_semantics_sha256),
        "effective launch config identity is malformed"
    );
    let descriptor_source = match kind {
        RunManifestKind::Performance => Some(canonical_descriptor_path(&repository, scenario)?),
        RunManifestKind::StartupBaseline => None,
    };
    let descriptor_sha256 = descriptor_source.as_deref().map(sha256_file).transpose()?;
    let started_unix_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    let identity_material = format!(
        "{scenario}\0{source_tree_sha256}\0{binary_sha256}\0{started_unix_millis}\0{}",
        std::process::id()
    );
    let run_id = sha256_bytes(identity_material.as_bytes());
    let manifest = RunManifest {
        schema_version: 5,
        kind,
        formal,
        run_id: run_id.clone(),
        scenario: scenario.to_string(),
        command: std::env::args().collect(),
        started_unix_millis,
        ended_unix_millis: None,
        exit_code: None,
        status: "running".to_string(),
        source_revision,
        native_build_identity,
        source_tree_sha256,
        source_dirty: !status.is_empty(),
        binary_sha256,
        runner_executable_path,
        runner_executable_sha256,
        process_identities,
        config_sha256,
        workload_manifest_sha256: workload_manifest_sha256.to_string(),
        fixture_sha256,
        tool_tree_sha256,
        cargo_lock_sha256,
        third_party_build_graph_sha256,
        descriptor_sha256,
        effective_launch_config_sha256,
        effective_launch_config_semantics_sha256,
        fixture_realization_sha256: None,
        fixture_realization_semantics_sha256: None,
        raw_artifact_inventory_sha256: None,
        resources_sha256: None,
        rustc_version: command_text(&repository, "rustc", &["-vV"])?,
        cargo_version: command_text(&repository, "cargo", &["-vV"])?,
        build_profile: binary_build_profile(context.primary_binary()),
        platform: platform_identity()?,
    };
    let path = context.scenario_root().join("run-manifest.json");
    let workload_manifest_artifact = context.scenario_root().join("workload-manifest.json");
    let fixture_spec_path = context.scenario_root().join("fixture-spec.json");
    let effective_launch_config_path = context.scenario_root().join("effective-launch-config.json");
    let descriptor_artifact = descriptor_source
        .as_ref()
        .map(|_| context.scenario_root().join("descriptor.json"));
    fs::write(&workload_manifest_artifact, workload_manifest_bytes)
        .context("write frozen workload manifest artifact")?;
    fs::write(&fixture_spec_path, fixture_spec)
        .context("write frozen fixture specification artifact")?;
    fs::write(
        &effective_launch_config_path,
        effective_launch_config.artifact_bytes(),
    )
    .context("write frozen effective launch config artifact")?;
    if let (Some(source), Some(artifact)) = (&descriptor_source, &descriptor_artifact) {
        fs::copy(source, artifact).context("copy canonical UEA-1 descriptor artifact")?;
    }
    write_manifest(&path, &manifest)?;
    Ok(RunManifestHandle {
        path,
        manifest,
        inputs: FrozenRunInputs {
            repository,
            binary,
            runner,
            config,
            workload_manifest_source,
            workload_manifest_artifact,
            fixture_spec: fixture_spec_path,
            effective_launch_config: effective_launch_config_path,
            descriptor_source,
            descriptor_artifact,
            process_launch_identities,
        },
        finished: false,
    })
}

fn observe_native_process_identities(
    context: &mut ScenarioContext,
    process_launch_identities: &[ProcessLaunchIdentity],
    source_revision: &str,
    primary_binary_sha256: &str,
    formal: bool,
) -> Result<(String, Vec<RunProcessIdentity>)> {
    let topology = context
        .handle()
        .launched_backend_topology()
        .context("read live BE build identities through structured SHOW BACKENDS")?;
    build_native_process_identities(
        process_launch_identities,
        &topology,
        source_revision,
        primary_binary_sha256,
        formal,
    )
}

fn build_native_process_identities(
    process_launch_identities: &[ProcessLaunchIdentity],
    topology: &[BackendTopologyRow],
    source_revision: &str,
    primary_binary_sha256: &str,
    formal: bool,
) -> Result<(String, Vec<RunProcessIdentity>)> {
    ensure!(
        process_launch_identities.len() >= 2,
        "UEA-1 provenance requires one FE and at least one BE launch identity"
    );
    ensure!(
        topology.len() + 1 == process_launch_identities.len(),
        "UEA-1 provenance requires every launched BE to have one live topology identity"
    );
    if formal {
        ensure!(
            topology.len() == 3,
            "formal UEA-1 provenance requires exactly three live BEs, observed {}",
            topology.len()
        );
    }
    ensure!(
        topology.iter().all(BackendTopologyRow::is_eligible_live),
        "UEA-1 provenance requires eligible live topology identities"
    );
    ensure!(
        topology.iter().all(|row| !row.process_id.is_empty()),
        "UEA-1 provenance requires nonempty BE application process identities"
    );
    ensure!(
        topology
            .iter()
            .map(|row| row.process_id.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == topology.len(),
        "UEA-1 provenance requires distinct BE application process identities"
    );
    ensure!(
        process_launch_identities
            .iter()
            .all(|identity| identity.pid != 0),
        "UEA-1 provenance requires nonzero FE/BE operating-system process identities"
    );
    ensure!(
        process_launch_identities
            .iter()
            .map(|identity| identity.pid)
            .collect::<BTreeSet<_>>()
            .len()
            == process_launch_identities.len(),
        "UEA-1 provenance requires distinct FE/BE operating-system process identities"
    );
    ensure!(
        process_launch_identities[0].role == "fe"
            && process_launch_identities[1..]
                .iter()
                .enumerate()
                .all(|(index, identity)| identity.role == format!("be-{index}")),
        "UEA-1 provenance launch identities do not match FE/BE launch order"
    );
    let identities = topology
        .iter()
        .map(|row| row.build_identity.clone())
        .collect::<BTreeSet<_>>();
    let native_build_identity =
        validate_native_build_identities(&identities, source_revision, formal)?;

    let frontend = &process_launch_identities[0];
    let frontend_binary_sha256 = frontend.executable.sha256.clone();
    let mut backend_processes = Vec::with_capacity(topology.len());
    for (index, (row, launch_identity)) in topology
        .iter()
        .zip(process_launch_identities[1..].iter())
        .enumerate()
    {
        backend_processes.push(RunProcessIdentity {
            role: format!("be-{index}"),
            os_pid: launch_identity.pid,
            process_start_token: launch_identity.process_start_token.clone(),
            application_process_id: Some(row.process_id.clone()),
            build_identity: Some(row.build_identity.clone()),
            binary_sha256: launch_identity.executable.sha256.clone(),
            executable_size_bytes: launch_identity.executable.size_bytes,
            executable_modified_unix_nanos: launch_identity.executable.modified_unix_nanos,
        });
    }
    let frontend_build_identity = backend_processes
        .iter()
        .find(|process| process.binary_sha256 == frontend_binary_sha256)
        .and_then(|process| process.build_identity.clone());
    let mut processes = Vec::with_capacity(backend_processes.len() + 1);
    processes.push(RunProcessIdentity {
        role: "fe".to_string(),
        os_pid: frontend.pid,
        process_start_token: frontend.process_start_token.clone(),
        application_process_id: None,
        build_identity: frontend_build_identity,
        binary_sha256: frontend_binary_sha256,
        executable_size_bytes: frontend.executable.size_bytes,
        executable_modified_unix_nanos: frontend.executable.modified_unix_nanos,
    });
    processes.extend(backend_processes);
    if formal {
        ensure!(
            processes
                .iter()
                .all(|process| process.binary_sha256 == primary_binary_sha256),
            "formal UEA-1 provenance requires every FE/BE role to run the measured binary"
        );
        ensure!(
            processes
                .iter()
                .all(|process| { process.build_identity.as_deref() == Some(source_revision) }),
            "formal UEA-1 provenance requires every FE/BE role to carry the source build identity"
        );
    }
    Ok((native_build_identity, processes))
}

fn validate_native_build_identities(
    identities: &BTreeSet<String>,
    source_revision: &str,
    formal: bool,
) -> Result<String> {
    ensure!(
        identities.len() == 1,
        "UEA-1 provenance requires one native build identity across all live BEs, observed {identities:?}"
    );
    let identity = identities
        .first()
        .expect("one native build identity was checked")
        .clone();
    ensure!(
        !formal || identity == source_revision,
        "formal UEA-1 provenance requires live BE native build identity {identity:?} to equal source revision {source_revision}"
    );
    Ok(identity)
}

impl RunManifestHandle {
    pub fn run_id(&self) -> &str {
        &self.manifest.run_id
    }

    pub fn finish_success(self) -> Result<RunManifestReference> {
        ensure!(
            self.manifest.kind == RunManifestKind::StartupBaseline,
            "performance run manifests require resource and fixture realization artifacts"
        );
        self.finish_success_with_artifacts(None, None, None, None)
    }

    pub fn finish_performance(
        self,
        resources_sha256: &str,
        fixture_realization_sha256: &str,
        fixture_realization_semantics_sha256: &str,
        raw_artifact_inventory_sha256: &str,
    ) -> Result<RunManifestReference> {
        ensure!(
            self.manifest.kind == RunManifestKind::Performance,
            "startup baseline manifest cannot claim performance artifacts"
        );
        self.finish_success_with_artifacts(
            Some(resources_sha256),
            Some(fixture_realization_sha256),
            Some(fixture_realization_semantics_sha256),
            Some(raw_artifact_inventory_sha256),
        )
    }

    fn finish_success_with_artifacts(
        self,
        resources_sha256: Option<&str>,
        fixture_realization_sha256: Option<&str>,
        fixture_realization_semantics_sha256: Option<&str>,
        raw_artifact_inventory_sha256: Option<&str>,
    ) -> Result<RunManifestReference> {
        let mut this = self;
        if let Some(resources_sha256) = resources_sha256 {
            ensure!(
                is_sha256(resources_sha256),
                "process resource artifact SHA256 is missing or malformed"
            );
            this.manifest.resources_sha256 = Some(resources_sha256.to_string());
        }
        match (
            fixture_realization_sha256,
            fixture_realization_semantics_sha256,
        ) {
            (Some(artifact), Some(semantics)) => {
                ensure!(
                    is_sha256(artifact) && is_sha256(semantics),
                    "fixture realization identity is missing or malformed"
                );
                this.manifest.fixture_realization_sha256 = Some(artifact.to_string());
                this.manifest.fixture_realization_semantics_sha256 = Some(semantics.to_string());
            }
            (None, None) => {}
            _ => bail!("fixture realization requires artifact and semantic identities"),
        }
        if let Some(raw_artifact_inventory_sha256) = raw_artifact_inventory_sha256 {
            ensure!(
                is_sha256(raw_artifact_inventory_sha256),
                "raw artifact inventory SHA256 is missing or malformed"
            );
            let inventory_path = this
                .path
                .parent()
                .context("run manifest path has no artifact directory")?
                .join("raw-artifact-inventory.json");
            ensure!(
                sha256_file(&inventory_path)? == raw_artifact_inventory_sha256,
                "raw artifact inventory changed before run completion"
            );
            this.manifest.raw_artifact_inventory_sha256 =
                Some(raw_artifact_inventory_sha256.to_string());
        }
        if let Some(expected) = &this.manifest.fixture_realization_sha256 {
            let fixture_path = this
                .path
                .parent()
                .context("run manifest path has no artifact directory")?
                .join("fixture-realization.json");
            ensure!(
                sha256_file(&fixture_path)? == *expected,
                "fixture realization artifact changed before run completion"
            );
            let fixture: serde_json::Value = serde_json::from_slice(
                &fs::read(&fixture_path).context("read fixture realization at completion")?,
            )
            .context("decode fixture realization at completion")?;
            ensure!(
                fixture
                    .get("semantics_sha256")
                    .and_then(serde_json::Value::as_str)
                    == this
                        .manifest
                        .fixture_realization_semantics_sha256
                        .as_deref(),
                "fixture realization semantic identity changed before run completion"
            );
        }
        ensure!(
            this.manifest.kind != RunManifestKind::Performance
                || (this.manifest.resources_sha256.is_some()
                    && this.manifest.descriptor_sha256.is_some()
                    && this.manifest.fixture_realization_sha256.is_some()
                    && this.manifest.fixture_realization_semantics_sha256.is_some()
                    && this.manifest.raw_artifact_inventory_sha256.is_some()),
            "completed performance manifest is missing a required artifact identity"
        );
        this.validate_unchanged_inputs()?;
        this.manifest.ended_unix_millis = Some(now_unix_millis()?);
        this.manifest.exit_code = Some(0);
        this.manifest.status = "completed".to_string();
        let bytes = serde_json::to_vec_pretty(&this.manifest)
            .context("serialize completed UEA-1 run manifest")?;
        let reference = RunManifestReference {
            run_id: this.manifest.run_id.clone(),
            sha256: sha256_bytes(&bytes),
            descriptor_sha256: this.manifest.descriptor_sha256.clone(),
            effective_launch_config_sha256: this.manifest.effective_launch_config_sha256.clone(),
            effective_launch_config_semantics_sha256: this
                .manifest
                .effective_launch_config_semantics_sha256
                .clone(),
            fixture_realization_sha256: this.manifest.fixture_realization_sha256.clone(),
            fixture_realization_semantics_sha256: this
                .manifest
                .fixture_realization_semantics_sha256
                .clone(),
            raw_artifact_inventory_sha256: this.manifest.raw_artifact_inventory_sha256.clone(),
        };
        fs::write(&this.path, bytes).context("write completed UEA-1 run manifest")?;
        this.finished = true;
        Ok(reference)
    }

    fn validate_unchanged_inputs(&self) -> Result<()> {
        let revision = command_text(&self.inputs.repository, "git", &["rev-parse", "HEAD"])?;
        ensure!(
            revision == self.manifest.source_revision,
            "UEA-1 source revision changed during the measurement run"
        );
        let status = command_text(
            &self.inputs.repository,
            "git",
            &["status", "--porcelain=v1"],
        )?;
        ensure!(
            !self.manifest.formal || status.is_empty(),
            "formal UEA-1 measurement source became dirty during the run"
        );
        ensure!(
            source_tree_sha256(&self.inputs.repository, &revision, &status)?
                == self.manifest.source_tree_sha256,
            "UEA-1 source tree changed during the measurement run"
        );
        ensure!(
            sha256_file(&self.inputs.binary)? == self.manifest.binary_sha256,
            "UEA-1 measured server binary changed during the run"
        );
        ensure!(
            self.inputs.process_launch_identities.len() == self.manifest.process_identities.len(),
            "UEA-1 process binary identity cardinality changed during the run"
        );
        for (launch_identity, identity) in self
            .inputs
            .process_launch_identities
            .iter()
            .zip(&self.manifest.process_identities)
        {
            recheck_process_launch_identity(launch_identity)?;
            ensure!(
                launch_identity.role == identity.role
                    && launch_identity.pid == identity.os_pid
                    && launch_identity.process_start_token == identity.process_start_token
                    && launch_identity.executable.sha256 == identity.binary_sha256
                    && launch_identity.executable.size_bytes == identity.executable_size_bytes
                    && launch_identity.executable.modified_unix_nanos
                        == identity.executable_modified_unix_nanos,
                "UEA-1 {} live process identity changed during the run",
                identity.role
            );
        }
        let actual_runner = fs::canonicalize(
            std::env::current_exe().context("resolve current system-test runner at completion")?,
        )
        .context("canonicalize current system-test runner at completion")?;
        ensure!(
            actual_runner == self.inputs.runner
                && sha256_file(&actual_runner)? == self.manifest.runner_executable_sha256,
            "UEA-1 system-test runner changed during the run"
        );
        ensure!(
            sha256_file(&self.inputs.config)? == self.manifest.config_sha256,
            "UEA-1 base configuration changed during the run"
        );
        if let Some(path) = &self.inputs.workload_manifest_source {
            ensure!(
                sha256_file(path)? == self.manifest.workload_manifest_sha256,
                "UEA-1 workload manifest source changed during the run"
            );
        }
        ensure!(
            sha256_file(&self.inputs.workload_manifest_artifact)?
                == self.manifest.workload_manifest_sha256,
            "UEA-1 workload manifest artifact changed during the run"
        );
        ensure!(
            sha256_file(&self.inputs.fixture_spec)? == self.manifest.fixture_sha256,
            "UEA-1 fixture specification changed during the run"
        );
        ensure!(
            sha256_file(&self.inputs.effective_launch_config)?
                == self.manifest.effective_launch_config_sha256,
            "UEA-1 effective launch config artifact changed during the run"
        );
        match (
            &self.inputs.descriptor_source,
            &self.inputs.descriptor_artifact,
            &self.manifest.descriptor_sha256,
        ) {
            (Some(source), Some(artifact), Some(expected)) => ensure!(
                sha256_file(source)? == *expected && sha256_file(artifact)? == *expected,
                "UEA-1 canonical descriptor changed during the run"
            ),
            (None, None, None) => {}
            _ => bail!("UEA-1 descriptor identity is internally inconsistent"),
        }
        ensure!(
            tool_tree_sha256(&self.inputs.repository)? == self.manifest.tool_tree_sha256,
            "UEA-1 measurement tool tree changed during the run"
        );
        let cargo_lock = self.inputs.repository.join("Cargo.lock");
        ensure!(
            sha256_file(&cargo_lock)? == self.manifest.cargo_lock_sha256,
            "UEA-1 Cargo.lock changed during the run"
        );
        ensure!(
            third_party_build_graph_sha256(&self.inputs.repository)?
                == self.manifest.third_party_build_graph_sha256,
            "UEA-1 third-party build graph changed during the run"
        );
        Ok(())
    }
}

#[derive(Serialize)]
struct RunCompletionMarker<'a> {
    schema_version: u32,
    run_id: &'a str,
    scenario: &'a str,
    run_manifest_sha256: &'a str,
    performance_sha256: &'a str,
    resources_sha256: &'a str,
    descriptor_sha256: &'a str,
    effective_launch_config_sha256: &'a str,
    fixture_realization_sha256: &'a str,
    raw_artifact_inventory_sha256: &'a str,
}

pub fn write_run_completion_marker(
    root: &Path,
    scenario: &str,
    run: &RunManifestReference,
    performance_sha256: &str,
    resources_sha256: &str,
) -> Result<String> {
    let descriptor_sha256 = run
        .descriptor_sha256
        .as_deref()
        .context("performance run omitted canonical descriptor identity")?;
    let fixture_realization_sha256 = run
        .fixture_realization_sha256
        .as_deref()
        .context("performance run omitted fixture realization identity")?;
    let raw_artifact_inventory_sha256 = run
        .raw_artifact_inventory_sha256
        .as_deref()
        .context("performance run omitted raw artifact inventory identity")?;
    ensure!(
        [
            run.sha256.as_str(),
            performance_sha256,
            resources_sha256,
            descriptor_sha256,
            run.effective_launch_config_sha256.as_str(),
            fixture_realization_sha256,
            raw_artifact_inventory_sha256,
        ]
        .into_iter()
        .all(is_sha256),
        "completion marker contains a malformed artifact identity"
    );
    let marker = RunCompletionMarker {
        schema_version: 2,
        run_id: &run.run_id,
        scenario,
        run_manifest_sha256: &run.sha256,
        performance_sha256,
        resources_sha256,
        descriptor_sha256,
        effective_launch_config_sha256: &run.effective_launch_config_sha256,
        fixture_realization_sha256,
        raw_artifact_inventory_sha256,
    };
    let bytes = serde_json::to_vec_pretty(&marker)
        .context("serialize terminal UEA-1 run completion marker")?;
    fs::write(root.join("run-completion.json"), &bytes).with_context(|| {
        format!(
            "write terminal run completion marker under {}",
            root.display()
        )
    })?;
    Ok(sha256_bytes(&bytes))
}

impl Drop for RunManifestHandle {
    fn drop(&mut self) {
        if !self.finished {
            self.manifest.ended_unix_millis = now_unix_millis().ok();
            self.manifest.status = "incomplete".to_string();
            let _ = write_manifest(&self.path, &self.manifest);
        }
    }
}

fn write_manifest(path: &Path, manifest: &RunManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize UEA-1 run manifest")?;
    fs::write(path, bytes).context("write UEA-1 run manifest")
}

fn now_unix_millis() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis())
}

fn repository_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("resolve repository root for UEA-1 provenance")
}

fn canonical_descriptor_path(repository: &Path, scenario: &str) -> Result<PathBuf> {
    let file_name = match scenario {
        "performance/uea1-short-concurrent" => "short.json",
        "performance/uea1-mixed" => "mixed.json",
        "performance/uea1-slow-output" => "slow-output.json",
        _ => bail!("performance run has no canonical descriptor for scenario {scenario}"),
    };
    let path = repository
        .join("tests/benchmarks/uea1/descriptors")
        .join(file_name);
    ensure!(
        path.is_file(),
        "canonical descriptor {} is missing",
        path.display()
    );
    Ok(path)
}

fn binary_build_profile(path: &Path) -> String {
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy();
        if matches!(value.as_ref(), "release" | "dev-opt" | "debug") {
            return value.into_owned();
        }
    }
    "unknown".to_string()
}

fn ensure_checkout_release_binary(repository: &Path, binary: &Path) -> Result<()> {
    let actual = fs::canonicalize(binary)
        .with_context(|| format!("resolve measured binary {}", binary.display()))?;
    let expected_path = repository.join("target/release/novarocks");
    let expected = fs::canonicalize(&expected_path).with_context(|| {
        format!(
            "resolve checkout-local release binary {}",
            expected_path.display()
        )
    })?;
    ensure!(
        actual == expected,
        "formal UEA-1 measurement requires this checkout's target/release/novarocks"
    );
    Ok(())
}

fn runner_executable_identity(repository: &Path, formal: bool) -> Result<(PathBuf, String)> {
    let executable = std::env::current_exe().context("resolve current system-test runner")?;
    let actual = fs::canonicalize(&executable).with_context(|| {
        format!(
            "resolve current system-test runner executable {}",
            executable.display()
        )
    })?;
    let actual_sha256 = sha256_file(&actual)?;
    if formal {
        ensure_checkout_release_runner(repository, &actual, &actual_sha256)?;
    }
    Ok((actual, actual_sha256))
}

fn ensure_checkout_release_runner(
    repository: &Path,
    actual: &Path,
    actual_sha256: &str,
) -> Result<()> {
    let expected_path = repository.join("target/release/novarocks-system-tests");
    let expected = fs::canonicalize(&expected_path).with_context(|| {
        format!(
            "resolve checkout-local release runner {}",
            expected_path.display()
        )
    })?;
    ensure!(
        actual == expected,
        "formal UEA-1 measurement requires this checkout's target/release/novarocks-system-tests"
    );
    ensure!(
        actual_sha256 == sha256_file(&expected)?,
        "formal UEA-1 runner executable hash does not match the checkout-local release runner"
    );
    Ok(())
}

fn source_tree_sha256(repository: &Path, revision: &str, status: &str) -> Result<String> {
    let tracked = command_bytes(repository, "git", &["ls-files", "-s"])?;
    let diff = command_bytes(repository, "git", &["diff", "--binary", "HEAD"])?;
    let staged = command_bytes(repository, "git", &["diff", "--binary", "--cached"])?;
    let mut hasher = Sha256::new();
    hasher.update(revision.as_bytes());
    hasher.update([0]);
    hasher.update(status.as_bytes());
    hasher.update([0]);
    hasher.update(tracked);
    hasher.update([0]);
    hasher.update(diff);
    hasher.update([0]);
    hasher.update(staged);
    Ok(format!("{:x}", hasher.finalize()))
}

const BUILD_GRAPH_ROOTS: [(&str, &str); 2] = [
    ("server", "novarocks-server"),
    ("system-runner", "novarocks-system-test-runner"),
];

#[derive(Debug)]
struct BuildGraphPackage {
    workspace: bool,
    identity: String,
}

fn third_party_build_graph_sha256(repository: &Path) -> Result<String> {
    let metadata = command_bytes(
        repository,
        "cargo",
        &["metadata", "--locked", "--format-version", "1"],
    )?;
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata).context("decode cargo metadata for build graph")?;
    let mut trees = Vec::with_capacity(BUILD_GRAPH_ROOTS.len());
    for (label, package) in BUILD_GRAPH_ROOTS {
        let output = command_text(
            repository,
            "cargo",
            &[
                "tree",
                "--locked",
                "-p",
                package,
                "-e",
                "normal,build",
                "--prefix",
                "depth",
                "--format",
                "{p}|{f}",
            ],
        )?;
        trees.push((label.to_string(), output));
    }
    third_party_build_graph_sha256_from_inputs(&metadata, &trees)
}

fn third_party_build_graph_sha256_from_inputs(
    metadata: &serde_json::Value,
    trees: &[(String, String)],
) -> Result<String> {
    let workspace_members = metadata
        .get("workspace_members")
        .and_then(serde_json::Value::as_array)
        .context("cargo metadata omitted workspace_members")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .context("cargo metadata workspace member is not a string")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .context("cargo metadata omitted packages")?;
    let mut package_index: BTreeMap<String, Vec<BuildGraphPackage>> = BTreeMap::new();
    for package in packages {
        let object = package
            .as_object()
            .context("cargo metadata package is not an object")?;
        let id = object
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("cargo metadata package omitted id")?;
        let name = object
            .get("name")
            .and_then(serde_json::Value::as_str)
            .context("cargo metadata package omitted name")?;
        let version = object
            .get("version")
            .and_then(serde_json::Value::as_str)
            .context("cargo metadata package omitted version")?;
        let display_prefix = format!("{name} v{version}");
        let workspace = workspace_members.contains(id);
        let identity = if workspace {
            String::new()
        } else {
            let source = object.get("source").and_then(serde_json::Value::as_str);
            let checksum = object.get("checksum").and_then(serde_json::Value::as_str);
            let content_sha256 = if source.is_none() {
                let manifest_path = object
                    .get("manifest_path")
                    .and_then(serde_json::Value::as_str)
                    .context("non-workspace path dependency omitted manifest_path")?;
                let root = Path::new(manifest_path)
                    .parent()
                    .context("path dependency manifest has no parent")?;
                Some(package_content_sha256(root)?)
            } else {
                None
            };
            serde_json::to_string(&serde_json::json!({
                "name": name,
                "version": version,
                "source": source,
                "checksum": checksum,
                "content_sha256": content_sha256,
            }))
            .context("serialize third-party package identity")?
        };
        package_index
            .entry(display_prefix.clone())
            .or_default()
            .push(BuildGraphPackage {
                workspace,
                identity,
            });
    }

    let mut package_records = BTreeSet::new();
    let mut edges = BTreeSet::new();
    let mut root_labels = BTreeSet::new();
    for (root_label, tree) in trees {
        ensure!(
            root_labels.insert(root_label.clone()),
            "duplicate build graph root"
        );
        let root_identity = format!("root:{root_label}");
        let mut anchors: Vec<String> = Vec::new();
        for line in tree.lines().filter(|line| !line.trim().is_empty()) {
            let digit_count = line.bytes().take_while(u8::is_ascii_digit).count();
            ensure!(
                digit_count > 0,
                "cargo tree line omitted numeric depth: {line}"
            );
            let depth: usize = line[..digit_count]
                .parse()
                .context("parse cargo tree depth")?;
            let (display, features) = line[digit_count..]
                .split_once('|')
                .context("cargo tree line omitted feature delimiter")?;
            let display = display.strip_suffix(" (*)").unwrap_or(display);
            let matches = package_index
                .iter()
                .filter(|(prefix, _)| {
                    display == prefix.as_str()
                        || display.strip_prefix(prefix.as_str()).is_some_and(|suffix| {
                            suffix.starts_with(" (") || suffix == " (proc-macro)"
                        })
                })
                .flat_map(|(_, packages)| packages.iter())
                .collect::<Vec<_>>();
            ensure!(
                matches.len() == 1,
                "cargo tree package {display:?} has {} metadata matches",
                matches.len()
            );
            let package = matches[0];
            ensure!(depth <= anchors.len(), "cargo tree depth jumped at {line}");
            anchors.truncate(depth);
            let parent = anchors
                .last()
                .cloned()
                .unwrap_or_else(|| root_identity.clone());
            let anchor = if package.workspace {
                parent
            } else {
                let mut active_features = features
                    .split(',')
                    .filter(|feature| !feature.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>();
                active_features.sort();
                active_features.dedup();
                let record = serde_json::to_string(&serde_json::json!({
                    "package": package.identity,
                    "active_features": active_features,
                }))
                .context("serialize active third-party package")?;
                package_records.insert(record);
                edges.insert(format!("{parent}\0{}", package.identity));
                package.identity.clone()
            };
            anchors.push(anchor);
        }
    }
    let graph = serde_json::json!({
        "schema_version": 1,
        "roots": root_labels,
        "packages": package_records,
        "edges": edges,
    });
    let bytes =
        serde_json::to_vec(&graph).context("serialize normalized third-party build graph")?;
    Ok(sha256_bytes(&bytes))
}

fn package_content_sha256(root: &Path) -> Result<String> {
    let mut paths = Vec::new();
    collect_package_files(root, root, &mut paths)?;
    paths.sort();
    let mut hasher = Sha256::new();
    for path in paths {
        let relative = path.strip_prefix(root).unwrap_or(&path);
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("read package file metadata {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            hasher.update(b"symlink\0");
            hasher.update(
                fs::read_link(&path)
                    .with_context(|| format!("read package symlink {}", path.display()))?
                    .to_string_lossy()
                    .as_bytes(),
            );
        } else {
            hasher.update(b"file\0");
            hasher.update(
                fs::read(&path).with_context(|| format!("read package file {}", path.display()))?,
            );
        }
        hasher.update([0]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn collect_package_files(root: &Path, current: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(current)
        .with_context(|| format!("read path dependency directory {}", current.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("read path dependency entry type {}", path.display()))?;
        let relative = path.strip_prefix(root).unwrap_or(&path);
        if relative.components().next().is_some_and(|component| {
            matches!(component.as_os_str().to_str(), Some(".git" | "target"))
        }) {
            continue;
        }
        if file_type.is_dir() {
            collect_package_files(root, &path, paths)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            paths.push(path);
        }
    }
    Ok(())
}

fn tool_tree_sha256(repository: &Path) -> Result<String> {
    let paths = tool_tree_paths(repository)?;
    let mut hasher = Sha256::new();
    for path in paths {
        let relative = path.strip_prefix(repository).unwrap_or(&path);
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(fs::read(&path).with_context(|| format!("read tool {}", path.display()))?);
        hasher.update([0]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn tool_tree_paths(repository: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for root in [
        repository.join("tests/benchmarks/uea1"),
        repository.join("tests/system-test-runner/src"),
        repository.join("tests/cluster-harness/src"),
    ] {
        collect_files(&root, &mut paths)?;
    }
    for path in [
        repository.join("tests/system-test-runner/Cargo.toml"),
        repository.join("tests/cluster-harness/Cargo.toml"),
    ] {
        ensure!(
            path.is_file(),
            "UEA-1 tool input {} is missing",
            path.display()
        );
        paths.push(path);
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn collect_files(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(root).with_context(|| format!("read tool directory {}", root.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, paths)?;
        } else if path.is_file() && path.extension().is_none_or(|value| value != "pyc") {
            paths.push(path);
        }
    }
    Ok(())
}

fn platform_identity() -> Result<PlatformIdentity> {
    let logical_cpu_count = std::thread::available_parallelism()
        .context("read logical CPU count")?
        .get();
    #[cfg(target_os = "macos")]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = (
        command_text(Path::new("."), "sw_vers", &["-productVersion"])?,
        command_text(
            Path::new("."),
            "sysctl",
            &["-n", "machdep.cpu.brand_string"],
        )?,
        command_text(Path::new("."), "sysctl", &["-n", "hw.memsize"])?
            .parse()
            .context("parse macOS physical memory")?,
        command_text(Path::new("."), "pmset", &["-g", "batt"])
            .map(|output| output.lines().next().unwrap_or("unknown").to_string())
            .unwrap_or_else(|error| format!("unavailable: {error:#}")),
    );
    #[cfg(target_os = "linux")]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = {
        let os_version = fs::read_to_string("/etc/os-release")
            .unwrap_or_else(|error| format!("unavailable: {error}"));
        let cpu_info = fs::read_to_string("/proc/cpuinfo").context("read /proc/cpuinfo")?;
        let cpu_model = cpu_info
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: "))
            .unwrap_or("unknown")
            .to_string();
        let mem_info = fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
        let physical_memory_bytes = mem_info
            .lines()
            .find_map(|line| line.strip_prefix("MemTotal:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .context("parse Linux physical memory")?
            .saturating_mul(1024);
        (
            os_version,
            cpu_model,
            physical_memory_bytes,
            "not-reported".to_string(),
        )
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let (os_version, cpu_model, physical_memory_bytes, power_mode) = (
        "unknown".to_string(),
        "unknown".to_string(),
        0,
        "unknown".to_string(),
    );
    ensure!(physical_memory_bytes > 0, "physical memory is unavailable");
    Ok(PlatformIdentity {
        os: std::env::consts::OS.to_string(),
        os_version,
        architecture: std::env::consts::ARCH.to_string(),
        cpu_model,
        logical_cpu_count,
        physical_memory_bytes,
        power_mode,
    })
}

fn command_text(directory: &Path, program: &str, arguments: &[&str]) -> Result<String> {
    let bytes = command_bytes(directory, program, arguments)?;
    let text =
        String::from_utf8(bytes).with_context(|| format!("{program} returned non-UTF8 output"))?;
    Ok(text.trim().to_string())
}

fn command_bytes(directory: &Path, program: &str, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .current_dir(directory)
        .args(arguments)
        .output()
        .with_context(|| format!("run {program} for UEA-1 provenance"))?;
    if !output.status.success() {
        bail!("{program} failed while collecting UEA-1 provenance")
    }
    Ok(output.stdout)
}

pub(super) fn sha256_file(path: &Path) -> Result<String> {
    Ok(sha256_bytes(&fs::read(path).with_context(|| {
        format!("read {} for SHA256", path.display())
    })?))
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn launch_identity(role: &str, pid: u32, binary: &Path) -> ProcessLaunchIdentity {
        ProcessLaunchIdentity {
            role: role.to_string(),
            pid,
            process_start_token: format!("start-{pid}"),
            executable: novarocks_cluster_harness::process_resources::freeze_executable_identity(
                binary,
            )
            .expect("freeze test executable identity"),
        }
    }

    fn live_backend(process_id: &str, grpc_port: u16, build_identity: &str) -> BackendTopologyRow {
        BackendTopologyRow {
            process_id: process_id.to_string(),
            grpc_port,
            state: "Live".to_string(),
            alive: true,
            scheduled_fragments: 0,
            build_identity: build_identity.to_string(),
            native_compatibility_id: "compatibility".to_string(),
            status_detail: String::new(),
        }
    }

    #[test]
    fn sha256_validation_rejects_missing_provenance() {
        assert!(is_sha256(&"a".repeat(64)));
        assert!(!is_sha256("not-a-hash"));
        assert!(!is_sha256(&"a".repeat(63)));
    }

    #[test]
    fn build_profile_comes_from_the_measured_binary_path() {
        assert_eq!(
            binary_build_profile(Path::new("/checkout/target/release/novarocks")),
            "release"
        );
        assert_eq!(
            binary_build_profile(Path::new("/checkout/external/novarocks")),
            "unknown"
        );
    }

    #[test]
    fn formal_binary_must_be_the_checkout_local_release_binary() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-provenance-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        let repository = root.join("checkout");
        let expected = repository.join("target/release/novarocks");
        let external = root.join("external/release/novarocks");
        fs::create_dir_all(expected.parent().expect("expected parent")).expect("expected dir");
        fs::create_dir_all(external.parent().expect("external parent")).expect("external dir");
        fs::write(&expected, b"expected").expect("expected binary");
        fs::write(&external, b"external").expect("external binary");

        ensure_checkout_release_binary(&repository, &expected).expect("checkout binary");
        assert!(ensure_checkout_release_binary(&repository, &external).is_err());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn formal_native_build_identity_must_equal_source_revision() {
        let identities = BTreeSet::from(["old-revision".to_string()]);
        let error = validate_native_build_identities(
            &identities,
            "0123456789abcdef0123456789abcdef01234567",
            true,
        )
        .expect_err("formal run must reject a stale embedded build identity");
        assert!(
            error
                .to_string()
                .contains("to equal source revision 0123456789abcdef")
        );
    }

    #[test]
    fn nonformal_native_build_identity_is_recorded_without_source_equality() {
        let identities = BTreeSet::from(["smoke-build".to_string()]);
        assert_eq!(
            validate_native_build_identities(
                &identities,
                "0123456789abcdef0123456789abcdef01234567",
                false,
            )
            .expect("smoke run may execute a different embedded build identity"),
            "smoke-build"
        );
    }

    #[test]
    fn formal_process_identity_binds_every_role_to_pid_binary_and_build() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-process-identity-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        fs::create_dir_all(&root).expect("create process identity fixture");
        let primary = root.join("novarocks");
        let other = root.join("other-novarocks");
        fs::write(&primary, b"measured-server").expect("write measured binary");
        fs::write(&other, b"other-server").expect("write other binary");
        let source_revision = "0123456789abcdef0123456789abcdef01234567";
        let launch_identities = vec![
            launch_identity("fe", 11, &primary),
            launch_identity("be-0", 21, &primary),
            launch_identity("be-1", 22, &primary),
            launch_identity("be-2", 23, &primary),
        ];
        let topology = vec![
            live_backend("be-process-0", 19000, source_revision),
            live_backend("be-process-1", 19001, source_revision),
            live_backend("be-process-2", 19002, source_revision),
        ];
        let primary_sha256 = sha256_file(&primary).expect("hash primary binary");
        let (native_build_identity, processes) = build_native_process_identities(
            &launch_identities,
            &topology,
            source_revision,
            &primary_sha256,
            true,
        )
        .expect("bind formal process identities");
        assert_eq!(native_build_identity, source_revision);
        assert_eq!(
            processes
                .iter()
                .map(|process| process.role.as_str())
                .collect::<Vec<_>>(),
            ["fe", "be-0", "be-1", "be-2"]
        );
        assert_eq!(processes[0].application_process_id, None);
        assert_eq!(
            processes[1].application_process_id.as_deref(),
            Some("be-process-0")
        );
        assert!(processes.iter().all(|process| {
            process.build_identity.as_deref() == Some(source_revision)
                && process.binary_sha256 == primary_sha256
        }));
        for process in &processes {
            let serialized = serde_json::to_value(process).expect("serialize process identity");
            assert_eq!(
                serialized
                    .as_object()
                    .expect("process identity object")
                    .keys()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([
                    "application_process_id",
                    "binary_sha256",
                    "build_identity",
                    "executable_modified_unix_nanos",
                    "executable_size_bytes",
                    "os_pid",
                    "process_start_token",
                    "role",
                ])
            );
        }

        let mut mismatched_launch_identities = launch_identities;
        mismatched_launch_identities[0] = launch_identity("fe", 11, &other);
        let error = build_native_process_identities(
            &mismatched_launch_identities,
            &topology,
            source_revision,
            &primary_sha256,
            true,
        )
        .expect_err("formal process identity must reject a different FE binary");
        assert!(error.to_string().contains("every FE/BE role"));
        fs::remove_dir_all(root).expect("remove process identity fixture");
    }

    #[test]
    fn third_party_build_graph_binds_features_edges_and_ignores_workspace_layout() {
        fn metadata(workspace_name: &str) -> serde_json::Value {
            serde_json::json!({
                "workspace_members": [format!("path+file:///repo/{workspace_name}#0.1.0")],
                "packages": [
                    {
                        "id": format!("path+file:///repo/{workspace_name}#0.1.0"),
                        "name": workspace_name,
                        "version": "0.1.0",
                        "source": null,
                        "checksum": null,
                        "manifest_path": format!("/repo/{workspace_name}/Cargo.toml"),
                    },
                    {
                        "id": "registry+https://example.invalid/index#external-a@1.0.0",
                        "name": "external-a",
                        "version": "1.0.0",
                        "source": "registry+https://example.invalid/index",
                        "checksum": "aaa",
                        "manifest_path": "/cargo/external-a/Cargo.toml",
                    },
                    {
                        "id": "registry+https://example.invalid/index#external-b@2.0.0",
                        "name": "external-b",
                        "version": "2.0.0",
                        "source": "registry+https://example.invalid/index",
                        "checksum": "bbb",
                        "manifest_path": "/cargo/external-b/Cargo.toml",
                    }
                ]
            })
        }
        fn trees(workspace_name: &str, feature: &str, nested_edge: bool) -> Vec<(String, String)> {
            let b_depth = if nested_edge { 2 } else { 1 };
            vec![
                (
                    "server".to_string(),
                    format!(
                        "0{workspace_name} v0.1.0 (/repo/{workspace_name})|\n1external-a v1.0.0|{feature}\n{b_depth}external-b v2.0.0|default"
                    ),
                ),
                (
                    "system-runner".to_string(),
                    format!(
                        "0{workspace_name} v0.1.0 (/repo/{workspace_name})|\n1external-a v1.0.0|{feature}"
                    ),
                ),
            ]
        }

        let original = third_party_build_graph_sha256_from_inputs(
            &metadata("workspace"),
            &trees("workspace", "feature-a", false),
        )
        .expect("hash original graph");
        let renamed = third_party_build_graph_sha256_from_inputs(
            &metadata("workspace-split"),
            &trees("workspace-split", "feature-a", false),
        )
        .expect("hash renamed workspace graph");
        let changed_feature = third_party_build_graph_sha256_from_inputs(
            &metadata("workspace"),
            &trees("workspace", "feature-b", false),
        )
        .expect("hash feature change");
        let changed_edge = third_party_build_graph_sha256_from_inputs(
            &metadata("workspace"),
            &trees("workspace", "feature-a", true),
        )
        .expect("hash dependency edge change");
        assert_eq!(original, renamed);
        assert_ne!(original, changed_feature);
        assert_ne!(original, changed_edge);
    }

    #[test]
    fn third_party_build_graph_binds_nonworkspace_path_dependency_content() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-path-package-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        fs::create_dir_all(root.join("src")).expect("create path package");
        fs::write(
            root.join("Cargo.toml"),
            b"[package]\nname='path-dep'\nversion='1.0.0'\n",
        )
        .expect("write path manifest");
        fs::write(root.join("src/lib.rs"), b"pub const VALUE: u8 = 1;").expect("write path source");
        let metadata = |root: &Path| {
            serde_json::json!({
                "workspace_members": ["path+file:///repo/root#0.1.0"],
                "packages": [
                    {
                        "id": "path+file:///repo/root#0.1.0",
                        "name": "root",
                        "version": "0.1.0",
                        "source": null,
                        "checksum": null,
                        "manifest_path": "/repo/root/Cargo.toml",
                    },
                    {
                        "id": "path+file:///vendor/path-dep#1.0.0",
                        "name": "path-dep",
                        "version": "1.0.0",
                        "source": null,
                        "checksum": null,
                        "manifest_path": root.join("Cargo.toml"),
                    }
                ]
            })
        };
        let trees = vec![
            (
                "server".to_string(),
                "0root v0.1.0 (/repo/root)|\n1path-dep v1.0.0 (/vendor/path-dep)|".to_string(),
            ),
            (
                "system-runner".to_string(),
                "0root v0.1.0 (/repo/root)|".to_string(),
            ),
        ];
        let before = third_party_build_graph_sha256_from_inputs(&metadata(&root), &trees)
            .expect("hash path graph");
        let relocated = root.with_extension("relocated");
        fs::create_dir_all(relocated.join("src")).expect("create relocated path package");
        fs::copy(root.join("Cargo.toml"), relocated.join("Cargo.toml"))
            .expect("copy path manifest");
        fs::copy(root.join("src/lib.rs"), relocated.join("src/lib.rs")).expect("copy path source");
        let relocated_hash =
            third_party_build_graph_sha256_from_inputs(&metadata(&relocated), &trees)
                .expect("hash relocated path graph");
        assert_eq!(before, relocated_hash);
        fs::write(root.join("src/lib.rs"), b"pub const VALUE: u8 = 2;")
            .expect("change path source");
        let after = third_party_build_graph_sha256_from_inputs(&metadata(&root), &trees)
            .expect("rehash path graph");
        assert_ne!(before, after);
        fs::remove_dir_all(root).expect("remove path package");
        fs::remove_dir_all(relocated).expect("remove relocated path package");
    }

    #[test]
    fn current_checkout_third_party_build_graph_is_repeatable() {
        let repository = repository_root().expect("repository root");
        let first = third_party_build_graph_sha256(&repository).expect("hash current build graph");
        let second =
            third_party_build_graph_sha256(&repository).expect("rehash current build graph");
        assert!(is_sha256(&first));
        assert_eq!(first, second);
    }

    #[test]
    fn tool_tree_includes_complete_runner_and_cluster_harness_inputs() {
        let repository = repository_root().expect("repository root");
        let paths = tool_tree_paths(&repository).expect("tool tree inputs");
        for expected in [
            "tests/benchmarks/uea1/artifact_protocol.py",
            "tests/system-test-runner/Cargo.toml",
            "tests/system-test-runner/src/scenarios/uea1_performance.rs",
            "tests/cluster-harness/Cargo.toml",
            "tests/cluster-harness/src/process_resources.rs",
        ] {
            assert!(
                paths.contains(&repository.join(expected)),
                "tool tree omitted {expected}"
            );
        }
    }

    #[test]
    fn formal_runner_must_be_checkout_local_with_its_exact_hash() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-runner-provenance-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        let repository = root.join("checkout");
        let expected = repository.join("target/release/novarocks-system-tests");
        let external = root.join("external/release/novarocks-system-tests");
        fs::create_dir_all(expected.parent().expect("expected parent")).expect("expected dir");
        fs::create_dir_all(external.parent().expect("external parent")).expect("external dir");
        fs::write(&expected, b"expected runner").expect("expected runner");
        fs::write(&external, b"external runner").expect("external runner");
        let expected = fs::canonicalize(expected).expect("canonical expected runner");
        let external = fs::canonicalize(external).expect("canonical external runner");
        let expected_sha = sha256_file(&expected).expect("expected runner hash");

        ensure_checkout_release_runner(&repository, &expected, &expected_sha)
            .expect("checkout runner");
        assert!(ensure_checkout_release_runner(&repository, &external, &expected_sha).is_err());
        assert!(ensure_checkout_release_runner(&repository, &expected, &"0".repeat(64)).is_err());
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn finish_refuses_to_complete_after_measured_binary_drift() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-finish-drift-{}-{}",
            std::process::id(),
            now_unix_millis().expect("clock")
        ));
        let repository = root.join("repository");
        let artifacts = root.join("artifacts");
        for path in [
            repository.join("tests/benchmarks/uea1"),
            repository.join("tests/system-test-runner/src"),
            repository.join("tests/cluster-harness/src"),
            artifacts.clone(),
        ] {
            fs::create_dir_all(path).expect("create finish fixture directory");
        }
        fs::write(
            repository.join("Cargo.lock"),
            r#"version = 4

[[package]]
name = "external"
version = "1.0.0"
source = "registry+https://example.invalid/index"
checksum = "abc"
"#,
        )
        .expect("write lock fixture");
        for path in [
            "tests/benchmarks/uea1/tool.py",
            "tests/system-test-runner/src/main.rs",
            "tests/system-test-runner/Cargo.toml",
            "tests/cluster-harness/src/lib.rs",
            "tests/cluster-harness/Cargo.toml",
            "config.toml",
        ] {
            fs::write(repository.join(path), b"fixture").expect("write tracked fixture");
        }
        run_git(&repository, &["init", "-q"]);
        run_git(&repository, &["config", "user.name", "NovaRocks Test"]);
        run_git(
            &repository,
            &["config", "user.email", "novarocks-test@example.invalid"],
        );
        run_git(&repository, &["config", "commit.gpgsign", "false"]);
        run_git(&repository, &["add", "."]);
        run_git(&repository, &["commit", "-qm", "fixture"]);

        let binary = root.join("novarocks");
        let workload = artifacts.join("workload-manifest.json");
        let fixture = artifacts.join("fixture-spec.json");
        let effective_launch_config = artifacts.join("effective-launch-config.json");
        let descriptor = artifacts.join("descriptor.json");
        let fixture_realization = artifacts.join("fixture-realization.json");
        fs::write(&binary, b"measured-server").expect("write measured server");
        fs::write(&workload, b"workload").expect("write workload artifact");
        fs::write(&fixture, b"fixture-spec").expect("write fixture artifact");
        fs::write(&effective_launch_config, b"effective-config")
            .expect("write effective config artifact");
        fs::write(&descriptor, b"descriptor").expect("write descriptor artifact");
        fs::write(
            &fixture_realization,
            format!(r#"{{"semantics_sha256":"{}"}}"#, "e".repeat(64)),
        )
        .expect("write fixture realization artifact");
        let raw_artifact_inventory = artifacts.join("raw-artifact-inventory.json");
        fs::write(&raw_artifact_inventory, b"raw-inventory").expect("write raw artifact inventory");
        let runner = fs::canonicalize(std::env::current_exe().expect("current test executable"))
            .expect("canonical test executable");
        let source_revision = command_text(&repository, "git", &["rev-parse", "HEAD"])
            .expect("read fixture revision");
        let source_status = command_text(&repository, "git", &["status", "--porcelain=v1"])
            .expect("read fixture status");
        let binary_sha256 = sha256_file(&binary).expect("hash measured server");
        let cargo_lock = repository.join("Cargo.lock");
        let manifest = RunManifest {
            schema_version: 5,
            kind: RunManifestKind::Performance,
            formal: false,
            run_id: "run-finish-drift".to_string(),
            scenario: "performance/fixture".to_string(),
            command: Vec::new(),
            started_unix_millis: 1,
            ended_unix_millis: None,
            exit_code: None,
            status: "running".to_string(),
            source_revision: source_revision.clone(),
            native_build_identity: source_revision.clone(),
            source_tree_sha256: source_tree_sha256(&repository, &source_revision, &source_status)
                .expect("hash source tree"),
            source_dirty: false,
            binary_sha256: binary_sha256.clone(),
            runner_executable_path: runner.to_string_lossy().into_owned(),
            runner_executable_sha256: sha256_file(&runner).expect("hash test executable"),
            process_identities: vec![RunProcessIdentity {
                role: "fe".to_string(),
                os_pid: std::process::id(),
                process_start_token: "test-process-start".to_string(),
                application_process_id: None,
                build_identity: Some(source_revision),
                binary_sha256,
                executable_size_bytes: fs::metadata(&binary)
                    .expect("inspect measured server")
                    .len(),
                executable_modified_unix_nanos: 1,
            }],
            config_sha256: sha256_file(&repository.join("config.toml")).expect("hash config"),
            workload_manifest_sha256: sha256_file(&workload).expect("hash workload"),
            fixture_sha256: sha256_file(&fixture).expect("hash fixture"),
            tool_tree_sha256: tool_tree_sha256(&repository).expect("hash tool tree"),
            cargo_lock_sha256: sha256_file(&cargo_lock).expect("hash Cargo.lock"),
            third_party_build_graph_sha256: "b".repeat(64),
            descriptor_sha256: Some(sha256_file(&descriptor).expect("hash descriptor")),
            effective_launch_config_sha256: sha256_file(&effective_launch_config)
                .expect("hash effective config"),
            effective_launch_config_semantics_sha256: "c".repeat(64),
            fixture_realization_sha256: None,
            fixture_realization_semantics_sha256: None,
            raw_artifact_inventory_sha256: None,
            resources_sha256: None,
            rustc_version: "rustc fixture".to_string(),
            cargo_version: "cargo fixture".to_string(),
            build_profile: "release".to_string(),
            platform: PlatformIdentity {
                os: "fixture".to_string(),
                os_version: "fixture".to_string(),
                architecture: "fixture".to_string(),
                cpu_model: "fixture".to_string(),
                logical_cpu_count: 1,
                physical_memory_bytes: 1,
                power_mode: "fixture".to_string(),
            },
        };
        let path = artifacts.join("run-manifest.json");
        write_manifest(&path, &manifest).expect("write running manifest");
        let handle = RunManifestHandle {
            path: path.clone(),
            manifest,
            inputs: FrozenRunInputs {
                repository: repository.clone(),
                binary: binary.clone(),
                runner,
                config: repository.join("config.toml"),
                workload_manifest_source: None,
                workload_manifest_artifact: workload,
                fixture_spec: fixture,
                effective_launch_config,
                descriptor_source: Some(descriptor.clone()),
                descriptor_artifact: Some(descriptor),
                process_launch_identities: vec![launch_identity("fe", std::process::id(), &binary)],
            },
            finished: false,
        };

        fs::write(&binary, b"changed-server").expect("change measured server");
        let error = handle
            .finish_performance(
                &"a".repeat(64),
                &sha256_file(&fixture_realization).expect("hash fixture realization"),
                &"e".repeat(64),
                &sha256_file(&raw_artifact_inventory).expect("hash raw artifact inventory"),
            )
            .expect_err("changed measured binary must prevent completion");
        assert!(error.to_string().contains("server binary changed"));
        let incomplete: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read incomplete manifest"))
                .expect("decode incomplete manifest");
        assert_eq!(incomplete["status"], "incomplete");
        assert!(incomplete["exit_code"].is_null());
        assert_eq!(incomplete["resources_sha256"], "a".repeat(64));
        fs::remove_dir_all(root).expect("remove finish fixture");
    }

    fn run_git(repository: &Path, arguments: &[&str]) {
        let status = Command::new("git")
            .current_dir(repository)
            .args(arguments)
            .status()
            .expect("run git for provenance fixture");
        assert!(status.success(), "git {arguments:?} failed");
    }
}
