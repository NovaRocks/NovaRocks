use anyhow::{Result, bail};
use novarocks_cluster_harness::LaunchProfile;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub list: bool,
    pub only: Vec<String>,
    pub binary: Option<PathBuf>,
    pub compatible_binary: Option<PathBuf>,
    pub other_island_binary: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub artifact_root: Option<PathBuf>,
    pub cluster_size: usize,
    pub timeout_secs: u64,
    pub launch_profile: LaunchProfile,
    pub uea1_workload_manifest: Option<PathBuf>,
}
impl Cli {
    pub fn parse_env() -> Result<Self> {
        Self::parse(std::env::args().skip(1))
    }

    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut cli = Self {
            list: false,
            only: Vec::new(),
            binary: None,
            compatible_binary: None,
            other_island_binary: None,
            config: None,
            artifact_root: None,
            cluster_size: 3,
            timeout_secs: 300,
            launch_profile: LaunchProfile::FaultScenario,
            uea1_workload_manifest: None,
        };
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--list" => cli.list = true,
                "--only" => cli.only.push(value("--only")?),
                "--binary" => cli.binary = Some(PathBuf::from(value("--binary")?)),
                "--compatible-binary" => {
                    cli.compatible_binary = Some(PathBuf::from(value("--compatible-binary")?));
                }
                "--other-island-binary" => {
                    cli.other_island_binary = Some(PathBuf::from(value("--other-island-binary")?));
                }
                "--config" => cli.config = Some(PathBuf::from(value("--config")?)),
                "--artifact-root" => {
                    cli.artifact_root = Some(PathBuf::from(value("--artifact-root")?));
                }
                "--cluster-size" => {
                    cli.cluster_size = value("--cluster-size")?.parse().map_err(|_| {
                        anyhow::anyhow!("--cluster-size must be a positive integer")
                    })?;
                }
                "--timeout-secs" => {
                    cli.timeout_secs = value("--timeout-secs")?.parse().map_err(|_| {
                        anyhow::anyhow!("--timeout-secs must be a positive integer")
                    })?;
                }
                "--launch-profile" => {
                    cli.launch_profile = value("--launch-profile")?
                        .parse()
                        .map_err(anyhow::Error::msg)?;
                }
                "--uea1-workload-manifest" => {
                    cli.uea1_workload_manifest =
                        Some(PathBuf::from(value("--uea1-workload-manifest")?));
                }
                "--help" | "-h" => bail!(Self::usage()),
                _ => bail!("unknown option {argument}\n{}", Self::usage()),
            }
        }
        if cli.cluster_size == 0 {
            bail!("--cluster-size must be >= 1");
        }
        if cli.timeout_secs == 0 {
            bail!("--timeout-secs must be >= 1");
        }
        Ok(cli)
    }

    pub const fn usage() -> &'static str {
        concat!(
            "usage: novarocks-system-tests [--list] [--only <exact-name>]... ",
            "[--binary <path> [--compatible-binary <path>] ",
            "[--other-island-binary <path>] --config <path> ",
            "--artifact-root <path>] [--cluster-size <N>] [--timeout-secs <N>] ",
            "[--launch-profile <fault-scenario|performance>] ",
            "[--uea1-workload-manifest <path>]"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_three_backends() {
        let cli = Cli::parse(Vec::new()).expect("parse defaults");
        assert_eq!(cli.cluster_size, 3);
        assert_eq!(cli.timeout_secs, 300);
        assert_eq!(cli.launch_profile, LaunchProfile::FaultScenario);
    }

    #[test]
    fn only_is_repeatable() {
        let cli = Cli::parse(vec![
            "--only".to_string(),
            "query-lifecycle/mysql-disconnect".to_string(),
            "--only".to_string(),
            "connector/catalog-version-drain".to_string(),
        ])
        .expect("parse repeated selectors");
        assert_eq!(cli.only.len(), 2);
    }

    #[test]
    fn parses_optional_compatibility_island_binaries() {
        let cli = Cli::parse(vec![
            "--compatible-binary".to_string(),
            "/tmp/compatible".to_string(),
            "--other-island-binary".to_string(),
            "/tmp/other-island".to_string(),
        ])
        .expect("parse optional island binaries");
        assert_eq!(
            cli.compatible_binary,
            Some(PathBuf::from("/tmp/compatible"))
        );
        assert_eq!(
            cli.other_island_binary,
            Some(PathBuf::from("/tmp/other-island"))
        );
    }

    #[test]
    fn parses_performance_profile_and_manifest() {
        let cli = Cli::parse(vec![
            "--launch-profile".to_string(),
            "performance".to_string(),
            "--uea1-workload-manifest".to_string(),
            "/tmp/workloads.json".to_string(),
        ])
        .expect("parse performance inputs");
        assert_eq!(cli.launch_profile, LaunchProfile::Performance);
        assert_eq!(
            cli.uea1_workload_manifest,
            Some(PathBuf::from("/tmp/workloads.json"))
        );
    }
}
