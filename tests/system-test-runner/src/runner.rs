use crate::cli::Cli;
use crate::config::RunnerConfig;
use crate::scenario::{Scenario, ScenarioContext, resolve_backend_binaries, resolve_binary};
use crate::scenarios;
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::{CrossProcessClusterOptions, CrossProcessServerHandle};
use std::fs;

pub fn run(cli: Cli) -> Result<()> {
    let scenarios = scenarios::all();
    if cli.list {
        for scenario in &scenarios {
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    let config = RunnerConfig::from_cli(&cli)?;
    let selected = select(&scenarios, &cli.only)?;
    if selected.is_empty() {
        bail!("no system scenarios are registered");
    }
    for scenario in &selected {
        scenario.validate_runner_inputs(
            config.launch_profile,
            config.uea1_workload_manifest.as_deref(),
        )?;
    }
    for scenario in selected {
        run_one(scenario, &config)?;
    }
    Ok(())
}

fn select<'a>(
    scenarios: &'a [Box<dyn Scenario>],
    only: &[String],
) -> Result<Vec<&'a dyn Scenario>> {
    if only.is_empty() {
        return Ok(scenarios
            .iter()
            .filter(|scenario| !scenario.is_explicit_stage())
            .map(|scenario| scenario.as_ref())
            .collect());
    }
    let mut selected = Vec::with_capacity(only.len());
    for requested in only {
        let scenario = scenarios
            .iter()
            .find(|scenario| scenario.name() == requested)
            .map(|scenario| scenario.as_ref())
            .with_context(|| format!("unknown system scenario {requested}"))?;
        selected.push(scenario);
    }
    Ok(selected)
}

fn run_one(scenario: &dyn Scenario, config: &RunnerConfig) -> Result<()> {
    let scenario_root = config.artifact_root.join(scenario.name().replace('/', "-"));
    fs::create_dir_all(&scenario_root)
        .with_context(|| format!("create scenario artifact root {}", scenario_root.display()))?;
    let launch_config = match scenario.launch_config(&scenario_root) {
        Ok(config) => config,
        Err(error) => {
            return match scenario.teardown() {
                Ok(()) => Err(error).with_context(|| {
                    format!("prepare launch configuration for {}", scenario.name())
                }),
                Err(teardown) => Err(anyhow::anyhow!(
                    "prepare launch configuration for {} failed: {error:#}; fixture teardown failed: {teardown:#}",
                    scenario.name()
                )),
            };
        }
    };
    let uea1_preparation_diagnostic_secret = launch_config
        .child_environment
        .fe
        .get("NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET")
        .cloned();
    let handle = CrossProcessServerHandle::launch(CrossProcessClusterOptions {
        binary: config.binary.clone(),
        fe_binary: resolve_binary(
            launch_config.binary_layout.frontend,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
        )?,
        be_binaries: resolve_backend_binaries(
            &launch_config.binary_layout.backends,
            &config.binary,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
            config.cluster_size,
        )?,
        expected_eligible_backend_count: launch_config.expected_eligible_backend_count,
        base_config_path: config.base_config_path.clone(),
        runtime_root: scenario_root.clone(),
        cluster_size: config.cluster_size,
        launch_profile: config.launch_profile,
        startup_timeout: config.timeout,
        child_environment: launch_config.child_environment,
        config_overlay: launch_config.config_overlay,
        native_trust_fixture: launch_config.native_trust_fixture,
    })
    .with_context(|| format!("launch system scenario {}", scenario.name()));
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            return match scenario.teardown() {
                Ok(()) => Err(error),
                Err(teardown) => Err(anyhow::anyhow!(
                    "{error:#}; fixture teardown failed: {teardown:#}"
                )),
            };
        }
    };
    let mut context = ScenarioContext::new(
        scenario.name(),
        handle,
        scenario_root,
        config.timeout,
        config.binary.clone(),
        config.compatible_binary.clone(),
        config.other_island_binary.clone(),
        config.base_config_path.clone(),
        config.cluster_size,
        config.launch_profile,
        config.uea1_workload_manifest.clone(),
        uea1_preparation_diagnostic_secret,
    );
    context.action("cluster launched and topology barrier passed");
    let result = scenario.run(&mut context);
    if let Err(error) = &result {
        context.retain_artifacts();
        eprintln!(
            "scenario={} failed; actions={:?}; runtime_dir={}; diagnostics={}",
            context.name(),
            context.actions(),
            context.runtime_dir().display(),
            context.diagnostics()
        );
        let launch_profile = match config.launch_profile {
            novarocks_cluster_harness::LaunchProfile::FaultScenario => "fault-scenario",
            novarocks_cluster_harness::LaunchProfile::Performance => "performance",
        };
        let manifest = config
            .uea1_workload_manifest
            .as_ref()
            .map(|path| format!(" --uea1-workload-manifest {}", path.display()))
            .unwrap_or_default();
        eprintln!(
            "rerun: novarocks-system-tests --only {} --binary {} --config {} --artifact-root {} --cluster-size {} --timeout-secs {} --launch-profile {}{}",
            context.name(),
            config.binary.display(),
            config.base_config_path.display(),
            config.artifact_root.display(),
            config.cluster_size,
            config.timeout.as_secs(),
            launch_profile,
            manifest,
        );
        let cluster_cleanup = context.shutdown();
        let fixture_cleanup = scenario.teardown();
        return match (cluster_cleanup, fixture_cleanup) {
            (Ok(()), Ok(())) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}",
                context.name()
            )),
            (Err(cluster), Ok(())) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; cluster cleanup failed: {cluster:#}",
                context.name()
            )),
            (Ok(()), Err(fixture)) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; fixture teardown failed: {fixture:#}",
                context.name()
            )),
            (Err(cluster), Err(fixture)) => Err(anyhow::anyhow!(
                "scenario {} failed: {error:#}; cluster cleanup failed: {cluster:#}; fixture teardown failed: {fixture:#}",
                context.name()
            )),
        };
    }
    context.action("scenario assertions passed");
    let cluster_cleanup = context
        .shutdown()
        .with_context(|| format!("cleanup system scenario {}", context.name()));
    let fixture_cleanup = scenario.teardown();
    match (cluster_cleanup, fixture_cleanup) {
        (Ok(()), Ok(())) => {}
        (Err(cluster), Ok(())) => return Err(cluster),
        (Ok(()), Err(fixture)) => {
            return Err(fixture)
                .with_context(|| format!("teardown fixture for {}", scenario.name()));
        }
        (Err(cluster), Err(fixture)) => {
            return Err(anyhow::anyhow!(
                "{cluster:#}; fixture teardown failed: {fixture:#}"
            ));
        }
    }
    println!("scenario={} PASS", scenario.name());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_rejects_unknown_selector() {
        assert!(select(&[], &["missing".to_string()]).is_err());
    }

    #[test]
    fn default_selection_excludes_external_fixture_scenarios() {
        let scenarios = crate::scenarios::all();
        let selected = select(&scenarios, &[]).expect("select default system baseline");
        assert!(selected.iter().all(|scenario| {
            scenario.name() != "frontend-lifecycle/blue-green-session-cutover"
        }));
        assert!(
            select(
                &scenarios,
                &["frontend-lifecycle/blue-green-session-cutover".to_string()]
            )
            .expect("select explicit blue/green scenario")
            .iter()
            .any(|scenario| scenario.name() == "frontend-lifecycle/blue-green-session-cutover")
        );
    }

    #[test]
    fn performance_preflight_rejects_missing_profile_and_manifest() {
        let scenarios = crate::scenarios::all();
        let selected = select(
            &scenarios,
            &["performance/uea1-short-concurrent".to_string()],
        )
        .expect("select performance scenario");
        let scenario = selected[0];
        assert!(
            scenario
                .validate_runner_inputs(
                    novarocks_cluster_harness::LaunchProfile::FaultScenario,
                    None
                )
                .is_err()
        );
        assert!(
            scenario
                .validate_runner_inputs(novarocks_cluster_harness::LaunchProfile::Performance, None)
                .is_err()
        );
    }
}
