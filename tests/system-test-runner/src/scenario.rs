use anyhow::{Result, bail};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, CrossProcessServerHandle,
    LaunchProfile, NativeTrustFixture, ServerHandle,
};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ScenarioBinary {
    #[default]
    Primary,
    Compatible,
    OtherIsland,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScenarioBinaryLayout {
    /// `Primary` preserves the runner's primary binary for the frontend.
    pub frontend: ScenarioBinary,
    /// Empty preserves the runner's primary binary for every backend. When
    /// populated, entries map one-to-one to backend indexes.
    pub backends: Vec<ScenarioBinary>,
}

pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;

    /// Validates runner-wide inputs before any selected scenario launches a
    /// process. Scenarios with special profiles or manifests fail here.
    fn validate_runner_inputs(
        &self,
        _launch_profile: LaunchProfile,
        _uea1_workload_manifest: Option<&Path>,
    ) -> Result<()> {
        Ok(())
    }

    /// External-fixture scenarios remain discoverable and runnable by exact
    /// selector, but do not turn the normal no-Docker system baseline into a
    /// Docker requirement.
    fn is_explicit_stage(&self) -> bool {
        false
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            child_environment: self.child_environment(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()>;

    /// Releases an external fixture created while preparing this scenario.
    ///
    /// The runner calls this after both successful and failed cluster runs, as
    /// well as when cluster launch itself fails. Implementations must be
    /// idempotent because launch preparation can fail after allocating a
    /// fixture.
    fn teardown(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScenarioLaunchConfig {
    pub binary_layout: ScenarioBinaryLayout,
    /// `None` preserves the runner's full-cluster topology barrier.
    pub expected_eligible_backend_count: Option<usize>,
    pub child_environment: CrossProcessChildEnvironment,
    pub config_overlay: CrossProcessConfigOverlay,
    pub native_trust_fixture: NativeTrustFixture,
}

pub struct ScenarioContext {
    name: &'static str,
    handle: CrossProcessServerHandle,
    scenario_root: PathBuf,
    deadline: Instant,
    actions: Vec<String>,
    binary: PathBuf,
    compatible_binary: Option<PathBuf>,
    other_island_binary: Option<PathBuf>,
    base_config_path: PathBuf,
    cluster_size: usize,
    startup_timeout: Duration,
    launch_profile: LaunchProfile,
    uea1_workload_manifest: Option<PathBuf>,
    uea1_preparation_diagnostic_secret: Option<String>,
}

impl ScenarioContext {
    pub fn new(
        name: &'static str,
        handle: CrossProcessServerHandle,
        scenario_root: PathBuf,
        timeout: Duration,
        binary: PathBuf,
        compatible_binary: Option<PathBuf>,
        other_island_binary: Option<PathBuf>,
        base_config_path: PathBuf,
        cluster_size: usize,
        launch_profile: LaunchProfile,
        uea1_workload_manifest: Option<PathBuf>,
        uea1_preparation_diagnostic_secret: Option<String>,
    ) -> Self {
        Self {
            name,
            handle,
            scenario_root,
            deadline: Instant::now() + timeout,
            actions: Vec::new(),
            binary,
            compatible_binary,
            other_island_binary,
            base_config_path,
            cluster_size,
            startup_timeout: timeout,
            launch_profile,
            uea1_workload_manifest,
            uea1_preparation_diagnostic_secret,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn handle(&mut self) -> &mut CrossProcessServerHandle {
        &mut self.handle
    }

    pub fn process_ids(&self) -> novarocks_cluster_harness::process_resources::ClusterProcessIds {
        self.handle.process_ids()
    }

    pub fn process_launch_identities(
        &self,
    ) -> (
        &novarocks_cluster_harness::process_resources::ProcessLaunchIdentity,
        &[novarocks_cluster_harness::process_resources::ProcessLaunchIdentity],
    ) {
        self.handle.process_launch_identities()
    }

    pub fn process_resource_identities(
        &self,
    ) -> Result<novarocks_cluster_harness::process_resources::ClusterProcessIdentities> {
        self.handle.process_resource_identities()
    }

    pub fn recheck_live_process_launch_identities(
        &self,
    ) -> Result<Vec<novarocks_cluster_harness::process_resources::ProcessLaunchIdentity>> {
        self.handle.recheck_live_process_launch_identities()
    }

    pub fn effective_launch_config_evidence(
        &self,
    ) -> &novarocks_cluster_harness::EffectiveLaunchConfigEvidence {
        self.handle.effective_launch_config_evidence()
    }

    pub fn mysql_port(&self) -> u16 {
        self.handle.runtime().fe_mysql_port
    }

    pub fn mysql_user(&self) -> &str {
        self.handle.mysql_user()
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn remaining(&self, operation: &str) -> Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("scenario {} timed out before {operation}", self.name);
        }
        Ok(remaining)
    }

    pub fn action(&mut self, action: impl Into<String>) {
        self.actions.push(action.into());
    }

    pub fn actions(&self) -> &[String] {
        &self.actions
    }

    pub fn runtime_dir(&self) -> &Path {
        self.handle.runtime_dir()
    }

    pub fn scenario_root(&self) -> &Path {
        &self.scenario_root
    }

    pub fn primary_binary(&self) -> &Path {
        &self.binary
    }

    pub fn base_config_path(&self) -> &Path {
        &self.base_config_path
    }

    pub fn launch_profile(&self) -> LaunchProfile {
        self.launch_profile
    }

    pub fn uea1_workload_manifest(&self) -> Option<&Path> {
        self.uea1_workload_manifest.as_deref()
    }

    pub fn uea1_preparation_diagnostic_secret(&self) -> Option<&str> {
        self.uea1_preparation_diagnostic_secret.as_deref()
    }

    pub fn fe_http_port(&self) -> u16 {
        self.handle.runtime().fe_http_port
    }

    pub fn diagnostics(&self) -> String {
        self.handle.diagnostics()
    }

    pub fn compatible_binary(&self) -> Result<PathBuf> {
        self.compatible_binary
            .clone()
            .ok_or_else(|| anyhow::anyhow!("scenario requires --compatible-binary"))
    }

    pub fn retain_artifacts(&mut self) {
        self.handle.retain_runtime_artifacts();
    }

    pub fn shutdown(&mut self) -> Result<()> {
        ServerHandle::shutdown(&mut self.handle)
    }

    /// Launch a peer native cluster for a focused multi-cluster scenario.
    /// The peer shares the runner binary and base configuration, while the
    /// caller owns an isolated runtime directory and its explicit overlay.
    pub fn launch_peer_cluster(
        &self,
        name: &str,
        launch_config: ScenarioLaunchConfig,
    ) -> Result<CrossProcessServerHandle> {
        let runtime_root = self.scenario_root.join(name);
        CrossProcessServerHandle::launch(novarocks_cluster_harness::CrossProcessClusterOptions {
            binary: self.binary.clone(),
            fe_binary: resolve_binary(
                launch_config.binary_layout.frontend,
                self.compatible_binary.as_ref(),
                self.other_island_binary.as_ref(),
            )?,
            be_binaries: resolve_backend_binaries(
                &launch_config.binary_layout.backends,
                &self.binary,
                self.compatible_binary.as_ref(),
                self.other_island_binary.as_ref(),
                self.cluster_size,
            )?,
            expected_eligible_backend_count: launch_config.expected_eligible_backend_count,
            base_config_path: self.base_config_path.clone(),
            runtime_root,
            cluster_size: self.cluster_size,
            launch_profile: self.launch_profile,
            startup_timeout: self.startup_timeout,
            child_environment: launch_config.child_environment,
            config_overlay: launch_config.config_overlay,
            native_trust_fixture: launch_config.native_trust_fixture,
        })
    }
}

pub(crate) fn resolve_binary(
    selection: ScenarioBinary,
    compatible: Option<&PathBuf>,
    other_island: Option<&PathBuf>,
) -> Result<Option<PathBuf>> {
    match selection {
        ScenarioBinary::Primary => Ok(None),
        ScenarioBinary::Compatible => compatible.cloned().map(Some).ok_or_else(|| {
            anyhow::anyhow!(
                "scenario selected compatible binary, but --compatible-binary was not provided"
            )
        }),
        ScenarioBinary::OtherIsland => other_island.cloned().map(Some).ok_or_else(|| {
            anyhow::anyhow!(
                "scenario selected other-island binary, but --other-island-binary was not provided"
            )
        }),
    }
}

pub(crate) fn resolve_backend_binaries(
    selections: &[ScenarioBinary],
    primary: &Path,
    compatible: Option<&PathBuf>,
    other_island: Option<&PathBuf>,
    cluster_size: usize,
) -> Result<Vec<PathBuf>> {
    if selections.is_empty() {
        return Ok(Vec::new());
    }
    if selections.len() != cluster_size {
        bail!(
            "scenario selected {} backend binaries for cluster size {cluster_size}",
            selections.len()
        );
    }
    selections
        .iter()
        .map(|selection| {
            resolve_binary(*selection, compatible, other_island)
                .map(|binary| binary.unwrap_or_else(|| primary.to_path_buf()))
        })
        .collect()
}
