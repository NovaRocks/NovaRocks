#[path = "exact_mysql_fixture_clock.rs"]
mod exact_mysql_fixture_clock;
use anyhow::{Context, Result, bail};
pub(crate) use exact_mysql_fixture_clock::ExactMysqlPrelaunchClock;
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, CrossProcessNativeFaultProxyConfig,
    CrossProcessRootReplyFaultConfig, CrossProcessServerHandle, LaunchProfile, NativeTrustFixture,
    ServerHandle,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
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

    /// Owner-declared impact and initial-state facts; absent contracts disable reuse.
    fn declaration(&self) -> ScenarioDeclaration {
        ScenarioDeclaration::default()
    }

    /// Validates runner-wide inputs before any selected scenario launches a
    /// process. Scenarios with special profiles fail here.
    fn validate_runner_inputs(&self, _launch_profile: LaunchProfile) -> Result<()> {
        Ok(())
    }

    /// Explicit stages with external fixtures
    /// remain discoverable and runnable by exact selector without joining the
    /// default functional baseline.
    fn is_explicit_stage(&self) -> bool {
        false
    }

    /// Independent compatibility stages are discoverable but outside default functionality.
    fn is_compatibility_stage(&self) -> bool {
        false
    }

    /// Complete lifecycle demand, including later replacement or target-island launches.
    fn binary_requirements(&self) -> &'static [ScenarioBinary] {
        &[ScenarioBinary::Primary]
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

    /// Shared fixture writers receive the original invocation budget before setup.
    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        budget.check_work()?;
        self.launch_config(scenario_root)
    }

    /// Selected domain scenarios derive finite intent from their existing exact
    /// inputs before any role spawn. Default scenarios request no authority.
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        _cluster_size: usize,
        _binary: &Path,
        _original_deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(None)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()>;

    /// Opt-in independent neutral source; caller captures once before all role launch.
    /// This is not an exact MySQL Hub activation or a renewed scene-entry clock.
    fn root_observation_deadline(&self) -> Result<Option<Instant>> {
        Ok(None)
    }

    /// The explicit exact fixture freezes original prepared config facts before role launch.
    fn freeze_prepared_exact_config(
        &self,
        _artifact: &novarocks_cluster_harness::EffectiveLaunchConfigEvidence,
        _scenario_root: &Path,
        _original_deadline: Instant,
    ) -> Result<()> {
        Ok(())
    }

    /// Optional source-only event from the actual role log writer owner.
    fn observe_original_durable_log_owner(
        &self,
        _role: &str,
        _device: u64,
        _inode: u64,
    ) -> Result<()> {
        anyhow::bail!("scenario has no explicit held live source owner")
    }

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

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScenarioDeclaration {
    pub impact: ScenarioImpact,
    pub env_inputs: Vec<ScenarioEnvironmentInput>,
    pub initial_state: Option<&'static str>,
    pub basis: Option<&'static str>,
    pub heavy: Option<ScenarioHeavyReason>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScenarioImpact {
    pub paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioEnvironmentInput {
    pub name: &'static str,
    pub kind: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScenarioHeavyReason {
    pub reason: &'static str,
    pub detail: &'static str,
}

#[derive(Debug, Clone)]
pub struct ResolvedScenarioArtifacts {
    pub frontend: PathBuf,
    pub backends: Vec<PathBuf>,
    pub compatible: Option<PathBuf>,
    pub other_island: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
pub struct ScenarioLaunchConfig {
    /// Actual role artifacts resolved by the runner before finite intent freezing.
    pub resolved_artifacts: Option<ResolvedScenarioArtifacts>,
    pub binary_layout: ScenarioBinaryLayout,
    /// `None` preserves the runner's full-cluster topology barrier.
    pub expected_eligible_backend_count: Option<usize>,
    pub child_environment: CrossProcessChildEnvironment,
    pub config_overlay: CrossProcessConfigOverlay,
    /// Opt-in bounded Native TCP proxies keyed by BE index. Empty preserves
    /// direct production endpoints for ordinary scenarios.
    pub native_fault_proxies: CrossProcessNativeFaultProxyConfig,
    /// Opt-in complete Native RootReply message faults; None preserves TCP routing.
    pub native_root_reply_fault: Option<CrossProcessRootReplyFaultConfig>,
    pub native_trust_fixture: NativeTrustFixture,
}

pub struct ScenarioContext {
    build: crate::build_information::BuildInformation,
    name: &'static str,
    handle: CrossProcessServerHandle,
    scenario_root: PathBuf,
    deadline: Instant,
    exact_mysql_clock: Option<ExactMysqlPrelaunchClock>,
    actions: Vec<String>,
    phase_observations: Vec<ScenarioPhaseObservation>,
    binary: PathBuf,
    compatible_binary: Option<PathBuf>,
    other_island_binary: Option<PathBuf>,
    base_config_path: PathBuf,
    cluster_size: usize,
    startup_timeout: Duration,
    launch_profile: LaunchProfile,
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
    /// Fixed-name phase observations. The API accepts only static labels and
    /// numeric identities, which keeps scenario evidence secret-free by
    /// construction.
    phase_observations: &'a [ScenarioPhaseObservation],
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

#[derive(Debug, Serialize)]
pub struct ScenarioPhaseObservation {
    phase: &'static str,
    phase_sequence: u64,
    logical_read_ordinal: u64,
    attempt_ordinal: u64,
    capability_path: &'static str,
    provider_call_sequence: u64,
    outcome: &'static str,
    counters: BTreeMap<&'static str, u64>,
}

pub(crate) struct ScenarioContextParams {
    pub name: &'static str,
    pub handle: CrossProcessServerHandle,
    pub scenario_root: PathBuf,
    pub timeout: Duration,
    pub original_deadline: Instant,
    pub binary: PathBuf,
    pub compatible_binary: Option<PathBuf>,
    pub other_island_binary: Option<PathBuf>,
    pub base_config_path: PathBuf,
    pub cluster_size: usize,
    pub launch_profile: LaunchProfile,
    pub build: crate::build_information::BuildInformation,
    pub exact_mysql_clock: Option<ExactMysqlPrelaunchClock>,
}

impl ScenarioContext {
    pub(crate) fn new(params: ScenarioContextParams) -> Self {
        let ScenarioContextParams {
            name,
            handle,
            scenario_root,
            timeout,
            original_deadline,
            binary,
            compatible_binary,
            other_island_binary,
            base_config_path,
            cluster_size,
            launch_profile,
            build,
            exact_mysql_clock,
        } = params;
        Self {
            build,
            name,
            handle,
            scenario_root,
            deadline: original_deadline,
            exact_mysql_clock,
            actions: Vec::new(),
            phase_observations: Vec::new(),
            binary,
            compatible_binary,
            other_island_binary,
            base_config_path,
            cluster_size,
            startup_timeout: timeout,
            launch_profile,
            started_at: exact_mysql_clock.map_or_else(SystemTime::now, |clock| clock.started_at()),
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

    pub fn trino_port(&self) -> u16 {
        self.handle.runtime().fe_trino_port
    }

    pub fn mysql_port(&self) -> u16 {
        self.handle.runtime().fe_mysql_port
    }

    pub fn mysql_user(&self) -> &str {
        self.handle.mysql_user()
    }

    /// The launch-time exact fixture clock, never the generic run-entry deadline.
    pub(crate) fn exact_mysql_deadline(&self) -> Result<Instant> {
        let clock = self
            .exact_mysql_clock
            .context("exact MySQL scene has no prelaunch clock")?;
        clock.remaining("exact scene operation")?;
        Ok(clock.deadline())
    }
    /// Caller first saves scene wire/control evidence; cleanup must still run on expiry.
    pub(crate) fn shutdown_exact_mysql_fixture(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            let clock = self
                .exact_mysql_clock
                .context("exact MySQL scene has no prelaunch clock")?;
            clock.settle_original_roles(|| {
                self.handle.shutdown_exact_mysql_fixture(clock.deadline())
            })
        }
        #[cfg(not(unix))]
        {
            bail!("exact MySQL fixture requires Unix original role owners")
        }
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

    /// Adds a bounded, fixed-label phase observation to durable scenario
    /// evidence. Dynamic text belongs in immediate diagnostics, never here.
    pub fn record_phase_observation(
        &mut self,
        phase: &'static str,
        phase_sequence: u64,
        logical_read_ordinal: u64,
        attempt_ordinal: u64,
        capability_path: &'static str,
        provider_call_sequence: u64,
        outcome: &'static str,
        counters: BTreeMap<&'static str, u64>,
    ) -> Result<()> {
        for label in [phase, capability_path, outcome] {
            if label.is_empty() || label.len() > 96 {
                bail!("scenario phase observation labels must be 1..=96 bytes");
            }
        }
        if counters.is_empty()
            || counters.len() > 16
            || counters.keys().any(|key| key.is_empty() || key.len() > 96)
        {
            bail!(
                "scenario phase observation must contain 1..=16 fixed counter labels of at most 96 bytes"
            );
        }
        self.phase_observations.push(ScenarioPhaseObservation {
            phase,
            phase_sequence,
            logical_read_ordinal,
            attempt_ordinal,
            capability_path,
            provider_call_sequence,
            outcome,
            counters,
        });
        Ok(())
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

    pub fn fe_http_port(&self) -> u16 {
        self.handle.runtime().fe_http_port
    }

    pub fn diagnostics(&self) -> String {
        if self.exact_mysql_clock.is_some() {
            // Raw runtime diagnostics contain child tails outside the bounded
            // failure-log redactor. Keep only a finite verdict in this artifact.
            "exact MySQL fixture failed; original role cleanup remains required".to_string()
        } else {
            self.handle.diagnostics()
        }
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
        // Keep the original sealed schema5/oracle unchanged. Build facts are a
        // separate retained artifact before the disposable cluster is removed.
        fs::write(
            self.scenario_root()
                .join("scenario-role-build-information.json"),
            serde_json::to_vec_pretty(&self.handle.build_information_evidence()?)?,
        )
        .context("retain actual role binary build information")?;
        let evidence = ScenarioEvidence {
            schema_version: 5,
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
            runner_native_build_identity: self.build.native_build_identity.to_string(),
            runner_executable,
            cargo_lock_sha256: sha256_file(&repository.join("Cargo.lock"))?,
            platform: scenario_platform_identity()?,
            actions: &self.actions,
            phase_observations: &self.phase_observations,
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
        self.launch_peer_cluster_inner(name, launch_config, None)
    }

    pub fn launch_peer_cluster_with_test_session(
        &self,
        name: &str,
        launch_config: ScenarioLaunchConfig,
        plan: novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan,
    ) -> Result<CrossProcessServerHandle> {
        self.launch_peer_cluster_inner(name, launch_config, Some(plan))
    }

    fn launch_peer_cluster_inner(
        &self,
        name: &str,
        launch_config: ScenarioLaunchConfig,
        plan: Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>,
    ) -> Result<CrossProcessServerHandle> {
        anyhow::ensure!(
            Instant::now() < self.deadline,
            "original scenario deadline expired before peer launch"
        );
        let runtime_root = self.scenario_root.join(name);
        let options = novarocks_cluster_harness::CrossProcessClusterOptions {
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
            startup_timeout: self
                .startup_timeout
                .min(self.deadline.saturating_duration_since(Instant::now())),
            child_environment: launch_config.child_environment,
            config_overlay: launch_config.config_overlay,
            native_trust_fixture: launch_config.native_trust_fixture,
        };
        let mut handle = match plan {
            Some(plan) => CrossProcessServerHandle::launch_with_test_sessions(options, plan)?,
            None => CrossProcessServerHandle::launch(options)?,
        };
        let evidence = (|| -> Result<()> {
            fs::write(
                self.scenario_root
                    .join(format!("{name}-role-build-information.json")),
                serde_json::to_vec_pretty(&handle.build_information_evidence()?)?,
            )?;
            Ok(())
        })();
        if let Err(error) = evidence {
            return match handle.shutdown() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(anyhow::anyhow!(
                    "{error:#}; peer shutdown failed: {cleanup:#}"
                )),
            };
        }
        Ok(handle)
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
