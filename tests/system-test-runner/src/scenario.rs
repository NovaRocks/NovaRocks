use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, CrossProcessNativeFaultProxyConfig,
    CrossProcessServerHandle, LaunchProfile, NativeTrustFixture, ServerHandle,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// Opt-in bounded Native TCP proxies keyed by BE index. Empty preserves
    /// direct production endpoints for ordinary scenarios.
    pub native_fault_proxies: CrossProcessNativeFaultProxyConfig,
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
    started_at: SystemTime,
}

/// A retained, secret-free record of one system scenario outcome.
///
/// The cross-process handle removes its runtime directory after a successful
/// scenario. The evidence therefore belongs to the scenario artifact root,
/// not the disposable runtime directory. Failure diagnostics come from the
/// harness's redacted log collector; arbitrary error chains are deliberately
/// not persisted because a provider error can carry credential material.
#[derive(Debug, Serialize)]
struct ScenarioEvidence<'a> {
    schema_version: u32,
    scenario: &'a str,
    outcome: ScenarioEvidenceOutcome,
    exit_code: i32,
    started_unix_millis: u128,
    ended_unix_millis: u128,
    command: Vec<String>,
    source_revision: String,
    source_dirty: bool,
    source_tree_sha256: String,
    runner_native_build_identity: String,
    runner_executable: String,
    cargo_lock_sha256: String,
    platform: ScenarioPlatformIdentity,
    actions: &'a [String],
    runtime_dir: String,
    primary_binary: String,
    base_config_path: String,
    base_config_sha256: String,
    cluster_size: usize,
    launch_profile: &'static str,
    process_launch_identities:
        Vec<novarocks_cluster_harness::process_resources::ProcessLaunchIdentity>,
    effective_launch_config_sha256: String,
    effective_launch_config_semantics_sha256: String,
    effective_launch_config: serde_json::Value,
    diagnostics: Option<String>,
}

#[derive(Debug, Serialize)]
struct ScenarioPlatformIdentity {
    os: &'static str,
    architecture: &'static str,
    logical_cpu_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ScenarioEvidenceOutcome {
    Passed,
    Failed,
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
            started_at: SystemTime::now(),
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

    /// Writes the retained evidence outside the disposable runtime directory.
    ///
    /// `diagnostics` is present only for a failed scenario and is produced by
    /// the harness's secret-redacting failure-log collector. The full error
    /// chain remains on stderr for the immediate caller rather than becoming a
    /// durable artifact with an unknown credential-redaction contract.
    pub fn write_evidence(&self, outcome: ScenarioEvidenceOutcome) -> Result<PathBuf> {
        let (frontend, backends) = self.process_launch_identities();
        let process_launch_identities = std::iter::once(frontend.clone())
            .chain(backends.iter().cloned())
            .collect::<Vec<_>>();
        let effective_launch_config = self.effective_launch_config_evidence();
        let effective_launch_config_value =
            serde_json::from_slice(effective_launch_config.artifact_bytes())
                .context("decode secret-free effective launch config for scenario evidence")?;
        let config_bytes = fs::read(self.base_config_path()).with_context(|| {
            format!(
                "read base config for scenario evidence {}",
                self.base_config_path().display()
            )
        })?;
        let source = source_checkout_identity()?;
        let repository = workspace_root()?;
        let runner_executable = runner_executable_identity()?;
        let evidence = ScenarioEvidence {
            schema_version: 4,
            scenario: self.name,
            outcome,
            exit_code: match outcome {
                ScenarioEvidenceOutcome::Passed => 0,
                ScenarioEvidenceOutcome::Failed => 1,
            },
            started_unix_millis: unix_millis(self.started_at)?,
            ended_unix_millis: unix_millis(SystemTime::now())?,
            command: std::env::args().collect(),
            source_revision: source.revision,
            source_dirty: source.dirty,
            source_tree_sha256: source.tree_sha256,
            runner_native_build_identity: novarocks_version::native_build_identity().to_string(),
            runner_executable,
            cargo_lock_sha256: sha256_file(&repository.join("Cargo.lock"))?,
            platform: scenario_platform_identity()?,
            actions: &self.actions,
            runtime_dir: self.runtime_dir().display().to_string(),
            primary_binary: self.primary_binary().display().to_string(),
            base_config_path: self.base_config_path().display().to_string(),
            base_config_sha256: format!("{:x}", Sha256::digest(config_bytes)),
            cluster_size: self.cluster_size,
            launch_profile: match self.launch_profile {
                LaunchProfile::FaultScenario => "fault-scenario",
                LaunchProfile::Performance => "performance",
            },
            process_launch_identities,
            effective_launch_config_sha256: effective_launch_config.artifact_sha256().to_string(),
            effective_launch_config_semantics_sha256: effective_launch_config
                .semantics_sha256()
                .to_string(),
            effective_launch_config: effective_launch_config_value,
            diagnostics: (outcome == ScenarioEvidenceOutcome::Failed).then(|| self.diagnostics()),
        };
        let bytes = serde_json::to_vec_pretty(&evidence)
            .context("serialize secret-free system scenario evidence")?;
        let path = self.scenario_root().join("scenario-evidence.json");
        fs::write(&path, bytes)
            .with_context(|| format!("write system scenario evidence {}", path.display()))?;
        Ok(path)
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

struct SourceCheckoutIdentity {
    revision: String,
    dirty: bool,
    tree_sha256: String,
}

fn workspace_root() -> Result<&'static Path> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .context("locate workspace root for scenario evidence")
}

fn source_checkout_identity() -> Result<SourceCheckoutIdentity> {
    let repository = workspace_root()?;
    let revision = git_output(repository, &["rev-parse", "HEAD"])?;
    let status = git_output(repository, &["status", "--porcelain=v1"])?;
    let tracked = git_output_bytes(repository, &["ls-files", "-s"])?;
    let diff = git_output_bytes(repository, &["diff", "--binary", "HEAD"])?;
    let staged = git_output_bytes(repository, &["diff", "--binary", "--cached"])?;
    Ok(SourceCheckoutIdentity {
        tree_sha256: source_tree_sha256(&revision, &status, &tracked, &diff, &staged),
        revision,
        dirty: !status.is_empty(),
    })
}

fn git_output(repository: &Path, arguments: &[&str]) -> Result<String> {
    String::from_utf8(git_output_bytes(repository, arguments)?)
        .context("decode git output for scenario evidence")
        .map(|value| value.trim().to_string())
}

fn git_output_bytes(repository: &Path, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(repository)
        .output()
        .with_context(|| format!("run git {} for scenario evidence", arguments.join(" ")))?;
    if !output.status.success() {
        bail!(
            "git {} for scenario evidence failed with status {}: {}",
            arguments.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn source_tree_sha256(
    revision: &str,
    status: &str,
    tracked: &[u8],
    diff: &[u8],
    staged: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        revision.as_bytes(),
        status.as_bytes(),
        tracked,
        diff,
        staged,
    ] {
        hasher.update(part);
        hasher.update([0]);
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {} for SHA256", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn runner_executable_identity() -> Result<String> {
    let executable = std::env::current_exe().context("resolve system-test runner executable")?;
    let canonical = fs::canonicalize(&executable).with_context(|| {
        format!(
            "resolve system-test runner executable {}",
            executable.display()
        )
    })?;
    Ok(canonical.display().to_string())
}

fn scenario_platform_identity() -> Result<ScenarioPlatformIdentity> {
    Ok(ScenarioPlatformIdentity {
        os: std::env::consts::OS,
        architecture: std::env::consts::ARCH,
        logical_cpu_count: std::thread::available_parallelism()
            .context("read logical CPU count for scenario evidence")?
            .get(),
    })
}

#[cfg(test)]
mod tests {
    use super::source_tree_sha256;

    #[test]
    fn source_tree_identity_changes_with_each_git_input() {
        let baseline = source_tree_sha256("revision", "", b"tracked", b"diff", b"staged");
        for changed in [
            source_tree_sha256("other-revision", "", b"tracked", b"diff", b"staged"),
            source_tree_sha256("revision", " M file", b"tracked", b"diff", b"staged"),
            source_tree_sha256("revision", "", b"other-tracked", b"diff", b"staged"),
            source_tree_sha256("revision", "", b"tracked", b"other-diff", b"staged"),
            source_tree_sha256("revision", "", b"tracked", b"diff", b"other-staged"),
        ] {
            assert_ne!(baseline, changed);
        }
    }
}

fn unix_millis(time: SystemTime) -> Result<u128> {
    Ok(time
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis())
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
