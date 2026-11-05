use crate::cli::Cli;
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::LaunchProfile;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub build: crate::build_information::BuildInformation,
    pub binary: PathBuf,
    pub compatible_binary: Option<PathBuf>,
    pub other_island_binary: Option<PathBuf>,
    pub base_config_path: PathBuf,
    pub artifact_root: PathBuf,
    pub cluster_size: usize,
    pub timeout: Duration,
    pub fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget,
    pub launch_profile: LaunchProfile,
}

impl RunnerConfig {
    pub fn from_cli(cli: &Cli, build: crate::build_information::BuildInformation) -> Result<Self> {
        let binary = cli
            .binary
            .clone()
            .context("--binary is required when running scenarios")?;
        if !binary.is_file() {
            bail!("--binary path does not name a file: {}", binary.display());
        }
        let compatible_binary =
            validate_optional_binary(cli.compatible_binary.clone(), "--compatible-binary")?;
        let other_island_binary =
            validate_optional_binary(cli.other_island_binary.clone(), "--other-island-binary")?;
        let base_config_path = cli
            .config
            .clone()
            .context("--config is required when running scenarios")?;
        if !base_config_path.is_file() {
            bail!(
                "--config path does not name a file: {}",
                base_config_path.display()
            );
        }
        let artifact_root = cli
            .artifact_root
            .clone()
            .context("--artifact-root is required when running scenarios")?;
        Ok(Self {
            build,
            binary,
            compatible_binary,
            other_island_binary,
            base_config_path,
            artifact_root,
            cluster_size: cli.cluster_size,
            timeout: Duration::from_secs(cli.timeout_secs),
            fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget::from_environment(
                cli.fixture_budget_secs,
            )?,
            launch_profile: cli.launch_profile,
        })
    }
}

fn validate_optional_binary(binary: Option<PathBuf>, flag: &str) -> Result<Option<PathBuf>> {
    if let Some(binary) = binary {
        if !binary.is_file() {
            bail!("{flag} path does not name a file: {}", binary.display());
        }
        Ok(Some(binary))
    } else {
        Ok(None)
    }
}
