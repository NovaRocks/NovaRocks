use anyhow::{Result, bail};
use novarocks_cluster_harness::LaunchProfile;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cli {
    pub list: bool,
    pub list_default: bool,
    pub list_compatibility: bool,
    pub list_binary_requirements: bool,
    pub list_declarations: bool,
    pub only: Vec<String>,
    pub binary: Option<PathBuf>,
    pub compatible_binary: Option<PathBuf>,
    pub other_island_binary: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub artifact_root: Option<PathBuf>,
    pub cluster_size: usize,
    pub timeout_secs: u64,
    pub fixture_budget_secs: u64,
    pub launch_profile: LaunchProfile,
}
impl Cli {
    pub fn parse_env() -> Result<Self> {
        Self::parse(std::env::args().skip(1))
    }

    pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut cli = Self {
            list: false,
            list_default: false,
            list_compatibility: false,
            list_binary_requirements: false,
            list_declarations: false,
            only: Vec::new(),
            binary: None,
            compatible_binary: None,
            other_island_binary: None,
            config: None,
            artifact_root: None,
            cluster_size: 3,
            timeout_secs: 300,
            fixture_budget_secs: 3600,
            launch_profile: LaunchProfile::FaultScenario,
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
                "--list-declarations" => cli.list_declarations = true,
                "--list-default" => cli.list_default = true,
                "--list-compatibility" => cli.list_compatibility = true,
                "--list-binary-requirements" => cli.list_binary_requirements = true,
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
                "--fixture-budget-secs" => {
                    cli.fixture_budget_secs =
                        value("--fixture-budget-secs")?.parse().map_err(|_| {
                            anyhow::anyhow!("--fixture-budget-secs must be a positive integer")
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
                "--help" | "-h" => bail!(Self::usage()),
                _ => bail!("unknown option {argument}\n{}", Self::usage()),
            }
        }
        if [
            cli.list,
            cli.list_default,
            cli.list_compatibility,
            cli.list_binary_requirements,
            cli.list_declarations,
        ]
        .into_iter()
        .filter(|value| *value)
        .count()
            > 1
        {
            bail!("list modes are mutually exclusive");
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
            "usage: novarocks-system-tests [--list | --list-default | --list-compatibility | --list-binary-requirements | --list-declarations] [--only <exact-name>]... ",
            "[--binary <path> [--compatible-binary <path>] ",
            "[--other-island-binary <path>] --config <path> ",
            "--artifact-root <path>] [--cluster-size <N>] [--timeout-secs <N>] [--fixture-budget-secs <N>] ",
            "[--launch-profile <fault-scenario|performance>]"
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
    fn parses_distinct_registry_list_modes() {
        let all = Cli::parse(vec!["--list".to_string()]).expect("list all");
        assert!(all.list && !all.list_default);
        let defaults = Cli::parse(vec!["--list-default".to_string()]).expect("list defaults");
        assert!(!defaults.list && defaults.list_default);
        assert!(Cli::parse(vec!["--list".to_string(), "--list-default".to_string()]).is_err());
    }

    #[test]
    fn only_is_repeatable() {
        let cli = Cli::parse(vec![
            "--only".to_string(),
            "query-lifecycle/client-abandonment".to_string(),
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
    fn parses_clean_launch_profile() {
        let cli = Cli::parse(vec![
            "--launch-profile".to_string(),
            "performance".to_string(),
        ])
        .expect("parse clean launch profile");
        assert_eq!(cli.launch_profile, LaunchProfile::Performance);
    }

    #[test]
    fn retired_workload_manifest_is_an_unknown_option() {
        let error = Cli::parse(vec![
            "--uea1-workload-manifest".to_string(),
            "/tmp/workloads.json".to_string(),
        ])
        .expect_err("retired manifest option must be refused");
        assert!(error.to_string().contains("unknown option"));
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::*;
    #[test]
    fn declarations_need_no_runtime_inputs_and_are_exclusive_with_existing_list_modes() {
        let cli = Cli::parse([
            "--list-declarations".into(),
            "--only".into(),
            "paused/scenario".into(),
        ])
        .unwrap();
        assert!(cli.list_declarations);
        assert_eq!(cli.only, ["paused/scenario"]);
        for flag in [
            "--list",
            "--list-default",
            "--list-compatibility",
            "--list-binary-requirements",
        ] {
            assert!(Cli::parse([flag.into(), "--list-declarations".into()]).is_err());
            assert!(!Cli::parse([flag.into()]).unwrap().list_declarations);
        }
    }
}
