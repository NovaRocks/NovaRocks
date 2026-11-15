use crate::cli::Cli;
use crate::config::RunnerConfig;
use crate::scenario::{
    Scenario, ScenarioContext, ScenarioContextParams, ScenarioEvidenceOutcome,
    resolve_backend_binaries, resolve_binary,
};
use crate::scenario_settlement::ScenarioSettlement;
use crate::scenarios;
use anyhow::{Context, Result, bail};
use novarocks_cluster_harness::fixture_run::RunBudget;
use novarocks_cluster_harness::reference_inputs::{self, ReferenceInput, ResolvedReference};
use novarocks_cluster_harness::{CrossProcessCluster, CrossProcessClusterOptions};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

pub fn run(cli: Cli, build: crate::build_information::BuildInformation) -> Result<()> {
    run_dispatch(cli, build)
}

fn run_dispatch(cli: Cli, build: crate::build_information::BuildInformation) -> Result<()> {
    run_dispatch_with_reference_resolver(cli, build, reference_inputs::resolve_until)
}

fn run_dispatch_with_reference_resolver(
    cli: Cli,
    build: crate::build_information::BuildInformation,
    resolver: impl FnOnce(&Path, &BTreeSet<ReferenceInput>, &RunBudget) -> Result<ResolvedReference>,
) -> Result<()> {
    let scenarios = scenarios::all();
    if cli.list_declarations {
        println!(
            "{}",
            serde_json::to_string(&scenario_declarations(&scenarios, &cli.only)?)?
        );
        return Ok(());
    }
    if cli.list || cli.list_default {
        for scenario in list_scenarios(&scenarios, cli.list_default)? {
            if let Some(reason) = scenarios::disabled_reason(scenario.name()) {
                println!("{} [DISABLED: {reason}]", scenario.name());
            } else {
                println!("{}", scenario.name());
            }
        }
        return Ok(());
    }
    if cli.list_compatibility {
        let selected = if cli.only.is_empty() {
            scenarios
                .iter()
                .filter(|scenario| scenario.is_compatibility_stage())
                .map(|scenario| scenario.as_ref())
                .collect::<Vec<_>>()
        } else {
            select(&scenarios, &cli.only)?
        };
        anyhow::ensure!(!selected.is_empty(), "no compatibility scenarios selected");
        for scenario in selected {
            anyhow::ensure!(
                scenario.is_compatibility_stage(),
                "scenario is outside compatibility stages: {}",
                scenario.name()
            );
            println!("{}", scenario.name());
        }
        return Ok(());
    }
    if cli.list_binary_requirements {
        let selected = select(&scenarios, &cli.only)?;
        anyhow::ensure!(!selected.is_empty(), "no scenarios selected");
        let requirements = selected
            .iter()
            .flat_map(|scenario| scenario.binary_requirements().iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        for required in requirements {
            println!(
                "{}",
                match required {
                    crate::scenario::ScenarioBinary::Primary => "primary",
                    crate::scenario::ScenarioBinary::Compatible => "compatible",
                    crate::scenario::ScenarioBinary::OtherIsland => "other-island",
                }
            );
        }
        return Ok(());
    }
    let mut config = RunnerConfig::from_cli(&cli, build)?;
    let selected = select(&scenarios, &cli.only)?;
    if selected.is_empty() {
        bail!("no system scenarios are registered");
    }
    config.reference = resolve_selected_reference(
        &selected,
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
        &config.fixture_budget,
        resolver,
    )?;
    for scenario in &selected {
        scenario.validate_runner_inputs(config.launch_profile)?;
        validate_required_programs(*scenario, &config)?;
    }
    for scenario in selected {
        run_one(scenario, &config)?;
    }
    Ok(())
}

fn resolve_selected_reference(
    selected: &[&dyn Scenario],
    repository: &Path,
    budget: &RunBudget,
    resolver: impl FnOnce(&Path, &BTreeSet<ReferenceInput>, &RunBudget) -> Result<ResolvedReference>,
) -> Result<ResolvedReference> {
    let needed = selected
        .iter()
        .flat_map(|scenario| scenario.reference_inputs().iter().copied())
        .collect::<BTreeSet<_>>();
    if needed.is_empty() {
        return Ok(ResolvedReference::default());
    }
    resolver(repository, &needed, budget)
}

fn list_scenarios(
    scenarios: &[Box<dyn Scenario>],
    defaults_only: bool,
) -> Result<Vec<&dyn Scenario>> {
    if defaults_only {
        select(scenarios, &[])
    } else {
        Ok(scenarios.iter().map(|scenario| scenario.as_ref()).collect())
    }
}

fn scenario_declarations(
    scenarios: &[Box<dyn Scenario>],
    only: &[String],
) -> Result<serde_json::Value> {
    for requested in only {
        anyhow::ensure!(
            scenarios
                .iter()
                .any(|scenario| scenario.name() == requested),
            "unknown system scenario {requested}"
        );
    }
    let entries = scenarios
        .iter()
        .filter(|scenario| {
            only.is_empty() || only.iter().any(|requested| requested == scenario.name())
        })
        .map(|scenario| {
            let paused_reason = scenarios::disabled_reason(scenario.name());
            let mut declaration = scenario.declaration();
            declaration.reference_inputs = scenario.reference_inputs().to_vec();
            serde_json::json!({
                "name": scenario.name(),
                "default": !scenario.is_explicit_stage() && paused_reason.is_none(),
                "explicit": scenario.is_explicit_stage(),
                "compatibility": scenario.is_compatibility_stage(),
                "paused": paused_reason.is_some(),
                "paused_reason": paused_reason,
                "declaration": declaration,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({"schema": 1, "scenarios": entries}))
}

fn select<'a>(
    scenarios: &'a [Box<dyn Scenario>],
    only: &[String],
) -> Result<Vec<&'a dyn Scenario>> {
    if only.is_empty() {
        return Ok(scenarios
            .iter()
            .filter(|scenario| {
                !scenario.is_explicit_stage()
                    && scenarios::disabled_reason(scenario.name()).is_none()
            })
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
        if let Some(reason) = scenarios::disabled_reason(scenario.name()) {
            bail!("system scenario {requested} is disabled: {reason}");
        }
        selected.push(scenario);
    }
    Ok(selected)
}

fn validate_required_programs(scenario: &dyn Scenario, config: &RunnerConfig) -> Result<()> {
    use crate::scenario::ScenarioBinary;
    for required in scenario.binary_requirements() {
        let (binary, flag) = match required {
            ScenarioBinary::Primary => (Some(config.binary.as_path()), "--binary"),
            ScenarioBinary::Compatible => {
                (config.compatible_binary.as_deref(), "--compatible-binary")
            }
            ScenarioBinary::OtherIsland => (
                config.other_island_binary.as_deref(),
                "--other-island-binary",
            ),
        };
        anyhow::ensure!(
            binary.is_some_and(std::path::Path::is_file),
            "scenario {} requires an existing {flag} for its complete lifecycle",
            scenario.name()
        );
    }
    Ok(())
}

fn run_one(scenario: &dyn Scenario, config: &RunnerConfig) -> Result<()> {
    if let Some(reason) = scenarios::disabled_reason(scenario.name()) {
        bail!("system scenario {} is disabled: {reason}", scenario.name());
    }
    let scenario_root = config.artifact_root.join(scenario.name().replace('/', "-"));
    fs::create_dir_all(&scenario_root)
        .with_context(|| format!("create scenario artifact root {}", scenario_root.display()))?;
    let settlement = ScenarioSettlement::new(scenario.name(), &scenario_root)?;
    if !scenario.reference_inputs().is_empty() {
        settlement.record_reference(
            &config
                .reference
                .subset(&scenario.reference_inputs().iter().copied().collect()),
        )?;
    }
    let result = run_one_inner(scenario, config, scenario_root, &settlement);
    match settlement.write() {
        Ok(()) => result,
        Err(error) => {
            let bounded = result
                .as_ref()
                .err()
                .is_some_and(|error| observed_failure_presentation(error).is_some());
            finish_prelaunch_scenario(bounded, result.err(), Err(error), Ok(()))
        }
    }
}

fn run_one_inner(
    scenario: &dyn Scenario,
    config: &RunnerConfig,
    scenario_root: std::path::PathBuf,
    settlement: &ScenarioSettlement,
) -> Result<()> {
    if let Some(reason) = scenarios::disabled_reason(scenario.name()) {
        bail!("system scenario {} is disabled: {reason}", scenario.name());
    }
    let bounded_binding = false;
    let preparation_budget = (|| -> Result<_> {
        config.fixture_budget.check_work()?;
        Ok(std::time::Instant::now()
            .checked_add(config.timeout)
            .context("system original scenario deadline overflow")?
            .min(config.fixture_budget.work_deadline()))
    })();
    let scenario_deadline = match preparation_budget {
        Ok(deadline) => deadline,
        Err(error) => {
            return finish_prelaunch_scenario(
                bounded_binding,
                Some(error),
                Ok(()),
                settlement.teardown(scenario),
            );
        }
    };
    let scenario_budget = config
        .fixture_budget
        .clone()
        .with_work_deadline_limit(scenario_deadline);
    let mut launch_config =
        match scenario.launch_config_with_fixture_run(&scenario_root, &scenario_budget) {
            Ok(config) => config,
            Err(error) => {
                return finish_prelaunch_scenario(
                    bounded_binding,
                    Some(error),
                    Ok(()),
                    settlement.teardown(scenario),
                );
            }
        };
    settlement.launch_config_succeeded();
    let root_observation_clock = match scenario.root_observation_deadline() {
        Ok(clock) => clock,
        Err(error) => {
            return finish_prelaunch_scenario(
                bounded_binding,
                Some(error),
                Ok(()),
                settlement.teardown(scenario),
            );
        }
    };
    let bounded_original = root_observation_clock.is_some();
    let original_deadline = root_observation_clock.map_or(scenario_deadline, |deadline| {
        deadline.min(scenario_deadline)
    });

    let preparation = (|| -> Result<_> {
        if bounded_original {
            anyhow::ensure!(
                config.launch_profile == novarocks_cluster_harness::LaunchProfile::FaultScenario
                    && config.cluster_size == 3,
                "original root observation requires fault-scenario 1FE+3BE before role launch"
            );
        }
        let fe_binary = resolve_binary(
            launch_config.binary_layout.frontend,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
        )?;
        let be_binaries = resolve_backend_binaries(
            &launch_config.binary_layout.backends,
            &config.binary,
            config.compatible_binary.as_ref(),
            config.other_island_binary.as_ref(),
            config.cluster_size,
        )?;
        Ok((fe_binary, be_binaries))
    })();
    let (fe_binary, be_binaries) = match preparation {
        Ok(binaries) => binaries,
        Err(error) if bounded_original => {
            return finish_observed_scenario(Some(error), Ok(()), settlement.teardown(scenario));
        }
        Err(error) => {
            return finish_prelaunch_scenario(
                bounded_binding,
                Some(error),
                Ok(()),
                settlement.teardown(scenario),
            );
        }
    };
    launch_config.resolved_artifacts = Some(crate::scenario::ResolvedScenarioArtifacts {
        frontend: fe_binary.clone().unwrap_or_else(|| config.binary.clone()),
        backends: if be_binaries.is_empty() {
            vec![config.binary.clone(); config.cluster_size]
        } else {
            be_binaries.clone()
        },
        compatible: config.compatible_binary.clone(),
        other_island: config.other_island_binary.clone(),
    });
    let test_sessions = match scenario.test_session_plan(
        &launch_config,
        &scenario_root,
        config.cluster_size,
        &config.binary,
        original_deadline,
    ) {
        Ok(plan) => plan,
        Err(error) => {
            return finish_prelaunch_scenario(
                bounded_original,
                Some(error),
                Ok(()),
                settlement.teardown(scenario),
            );
        }
    };

    let cluster_options = CrossProcessClusterOptions {
        binary: config.binary.clone(),
        fe_binary,
        be_binaries,
        expected_eligible_backend_count: launch_config.expected_eligible_backend_count,
        base_config_path: config.base_config_path.clone(),
        // Reports and the retained scenario evidence live at `scenario_root`.
        // The harness may remove only this disposable child directory after a
        // successful scenario, while a failure keeps it for log inspection.
        runtime_root: scenario_root.join("runtime"),
        cluster_size: config.cluster_size,
        launch_profile: config.launch_profile,
        startup_timeout: config.timeout,
        child_environment: launch_config.child_environment,
        config_overlay: launch_config.config_overlay,
        native_trust_fixture: launch_config.native_trust_fixture,
    };
    if bounded_original
        && test_sessions.is_none()
        && (launch_config.native_root_reply_fault.is_some()
            || !launch_config
                .native_fault_proxies
                .backend_retained_byte_limits
                .is_empty())
    {
        return finish_observed_scenario(
            Some(anyhow::anyhow!(
                "original source requires direct original roles"
            )),
            Ok(()),
            settlement.teardown(scenario),
        );
    }
    settlement.cluster_launch_attempted();
    let prepared_check =
        |_artifact: &novarocks_cluster_harness::EffectiveLaunchConfigEvidence| -> Result<()> {
            anyhow::ensure!(
                std::time::Instant::now() < original_deadline,
                "original system preparation deadline expired"
            );
            anyhow::ensure!(
                std::time::Instant::now() < original_deadline,
                "original system preparation completion was late"
            );
            Ok(())
        };
    let handle = CrossProcessCluster::launch_with_fixture_budget(
        cluster_options,
        test_sessions,
        launch_config.native_fault_proxies,
        launch_config.native_root_reply_fault,
        Some(&prepared_check),
        None,
        scenario_budget
            .clone()
            .with_work_deadline_limit(original_deadline),
    )
    .with_context(|| format!("launch system scenario {}", scenario.name()));
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            settlement.cluster_launch_failed(&error);
            if bounded_original {
                return finish_observed_scenario(
                    Some(error),
                    Ok(()),
                    settlement.teardown(scenario),
                );
            }
            return match settlement.teardown(scenario) {
                Ok(()) => Err(error),
                Err(teardown) => Err(anyhow::anyhow!(
                    "{error:#}; fixture teardown failed: {teardown:#}"
                )),
            };
        }
    };
    settlement.cluster_launched();
    let mut context = ScenarioContext::new(ScenarioContextParams {
        reference: config
            .reference
            .subset(&scenario.reference_inputs().iter().copied().collect()),
        name: scenario.name(),
        handle,
        scenario_root,
        timeout: config.timeout,
        original_deadline,
        fixture_budget: scenario_budget.with_work_deadline_limit(original_deadline),
        binary: config.binary.clone(),
        compatible_binary: config.compatible_binary.clone(),
        other_island_binary: config.other_island_binary.clone(),
        base_config_path: config.base_config_path.clone(),
        cluster_size: config.cluster_size,
        launch_profile: config.launch_profile,
        build: config.build,
    });
    context.action("cluster launched and topology barrier passed");
    let result = scenario.run(&mut context).and_then(|()| {
        if std::time::Instant::now() >= original_deadline {
            return Err(novarocks_cluster_harness::fixture_run::FixtureBudgetExpired.into());
        }
        Ok(())
    });
    if let Err(error) = &result {
        context.retain_artifacts();
        let evidence_path = match context.write_evidence(ScenarioEvidenceOutcome::Failed) {
            Ok(path) => path.display().to_string(),
            Err(_) if bounded_original => {
                "unavailable (original source evidence write failed)".to_string()
            }
            Err(evidence_error) => format!("unavailable ({evidence_error:#})"),
        };
        eprintln!(
            "scenario={} failed; actions={:?}; runtime_dir={}; evidence={}; diagnostics={}",
            context.name(),
            context.actions(),
            context.runtime_dir().display(),
            evidence_path,
            context.diagnostics()
        );
        let launch_profile = match config.launch_profile {
            novarocks_cluster_harness::LaunchProfile::FaultScenario => "fault-scenario",
            novarocks_cluster_harness::LaunchProfile::Performance => "performance",
        };
        eprintln!(
            "rerun: novarocks-system-tests --only {} --binary {} --config {} --artifact-root {} --cluster-size {} --timeout-secs {} --launch-profile {}",
            context.name(),
            config.binary.display(),
            config.base_config_path.display(),
            config.artifact_root.display(),
            config.cluster_size,
            config.timeout.as_secs(),
            launch_profile,
        );
        let cluster_cleanup = context.shutdown();
        settlement.cluster_shutdown(&cluster_cleanup);
        let fixture_cleanup = settlement.teardown(scenario);
        if bounded_original {
            return finish_observed_scenario(
                Some(result.expect_err("failed scenario retains its original error")),
                cluster_cleanup,
                fixture_cleanup,
            );
        }
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
    settlement.cluster_shutdown(&cluster_cleanup);
    let fixture_cleanup = settlement.teardown(scenario);
    if bounded_original {
        finish_observed_scenario(None, cluster_cleanup, fixture_cleanup)?;
    } else {
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
    }
    context.action("cluster and fixture cleanup passed");
    let evidence_path = context
        .write_evidence(ScenarioEvidenceOutcome::Passed)
        .with_context(|| format!("write passing scenario evidence for {}", context.name()))?;
    // The original deadline includes all final durable evidence. The outer
    // wrapper writes only on paths that have not already published this record.
    settlement.write()?;
    if let Some(deadline) = root_observation_clock {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "neutral original role cleanup and final evidence exceeded prelaunch clock"
        );
    }
    println!(
        "scenario={} PASS evidence={}",
        scenario.name(),
        evidence_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_cover_native_registry_flags_and_all_explicit_reasons() {
        let scenarios = crate::scenarios::all();
        let document = scenario_declarations(&scenarios, &[]).unwrap();
        let entries = document["scenarios"].as_array().unwrap();
        assert_eq!(entries.len(), scenarios.len());
        for (entry, scenario) in entries.iter().zip(&scenarios) {
            assert_eq!(entry["name"], scenario.name());
            assert_eq!(entry["explicit"], scenario.is_explicit_stage());
            assert_eq!(entry["compatibility"], scenario.is_compatibility_stage());
            assert_eq!(
                entry["paused"],
                scenarios::disabled_reason(scenario.name()).is_some()
            );
            assert!(
                !entry["declaration"]["impact"]["paths"]
                    .as_array()
                    .unwrap()
                    .is_empty(),
                "{}",
                scenario.name()
            );
            let reference = serde_json::to_value(scenario.reference_inputs()).unwrap();
            if scenario.reference_inputs().is_empty() {
                assert!(entry["declaration"].get("reference_inputs").is_none());
            } else {
                assert_eq!(entry["declaration"]["reference_inputs"], reference);
            }
            if scenario.is_explicit_stage() {
                assert!(
                    entry["declaration"]["heavy"]["detail"]
                        .as_str()
                        .is_some_and(|detail| !detail.is_empty()),
                    "{}",
                    scenario.name()
                );
            }
            if scenarios::disabled_reason(scenario.name()).is_some() {
                assert!(
                    entry["paused_reason"]
                        .as_str()
                        .is_some_and(|reason| !reason.is_empty())
                );
                assert_eq!(entry["default"], false);
                assert_eq!(scenario_declarations(&scenarios, &[scenario.name().into()]).unwrap()["scenarios"].as_array().unwrap().len(), 1);
            }
        }
        assert!(scenario_declarations(&scenarios, &["missing".into()]).is_err());
    }

    #[test]
    fn registry_listing_preserves_default_selection_and_explicit_opt_in() {
        let scenarios = crate::scenarios::all();
        let all = list_scenarios(&scenarios, false).expect("list all scenarios");
        let defaults = list_scenarios(&scenarios, true).expect("list default scenarios");
        let names = |selected: &[&dyn Scenario]| {
            selected
                .iter()
                .map(|scenario| scenario.name())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(&defaults),
            names(&select(&scenarios, &[]).expect("default selection"))
        );
        assert_eq!(
            names(&all),
            scenarios
                .iter()
                .map(|scenario| scenario.name())
                .collect::<Vec<_>>()
        );
        assert!(!defaults.is_empty());
        assert!(
            defaults
                .iter()
                .all(|scenario| !scenario.is_explicit_stage())
        );
        let explicit = all
            .iter()
            .filter(|scenario| scenario.is_explicit_stage())
            .collect::<Vec<_>>();
        assert!(!explicit.is_empty());
        assert_eq!(all.len(), defaults.len() + explicit.len());
        for scenario in explicit {
            assert!(!names(&defaults).contains(&scenario.name()));
            if let Some(reason) = crate::scenarios::disabled_reason(scenario.name()) {
                let error = select(&scenarios, &[scenario.name().to_string()])
                    .err()
                    .expect("disabled scenario must refuse explicit selection");
                assert!(error.to_string().contains(reason));
                continue;
            }
            let selected =
                select(&scenarios, &[scenario.name().to_string()]).expect("explicit opt-in");
            assert_eq!(names(&selected), vec![scenario.name()]);
        }
    }

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
    fn compatibility_stages_are_explicit_and_declare_complete_lifecycle_programs() {
        use crate::scenario::ScenarioBinary::{Compatible, OtherIsland, Primary};
        let scenarios = crate::scenarios::all();
        let defaults = select(&scenarios, &[]).unwrap();
        assert!(
            defaults
                .iter()
                .all(|scenario| !scenario.is_compatibility_stage())
        );
        for scenario in scenarios
            .iter()
            .filter(|scenario| scenario.is_compatibility_stage())
        {
            assert!(scenario.is_explicit_stage());
            assert_eq!(
                select(&scenarios, &[scenario.name().into()]).unwrap()[0].name(),
                scenario.name()
            );
            assert!(scenario.binary_requirements().contains(&Primary));
        }
        // These stages start additional programs after initial layout resolution.
        // Their requirements must cover those later launches before any process starts.
        for name in [
            "native-compatibility/target-island-cutover",
            "native-compatibility/island-drain-and-replacement",
        ] {
            let selected = select(&scenarios, &[name.into()]).unwrap();
            assert!(selected[0].binary_requirements().contains(&Compatible));
            assert!(selected[0].binary_requirements().contains(&OtherIsland));
        }
        assert!(select(&scenarios, &[String::new()]).is_err());
    }

    #[test]
    fn selected_reference_components_are_unioned_and_resolved_once() {
        use ReferenceInput::{TrinoImage, TrinoJdbcDriver};
        let scenarios = crate::scenarios::all();
        let budget = RunBudget::from_environment(3600).unwrap();
        for (names, expected) in [
            (
                vec!["trino-interop/encoding-conformance"],
                BTreeSet::from([TrinoImage]),
            ),
            (
                vec![
                    "trino-interop/encoding-conformance",
                    "trino-interop/official-cli",
                ],
                BTreeSet::from([TrinoImage]),
            ),
            (
                vec![
                    "trino-interop/encoding-conformance",
                    "trino-interop/jdbc-probe",
                ],
                BTreeSet::from([TrinoImage, TrinoJdbcDriver]),
            ),
        ] {
            let names = names.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let selected = select(&scenarios, &names).unwrap();
            let mut calls = 0;
            resolve_selected_reference(
                &selected,
                Path::new("repository"),
                &budget,
                |repository, actual, original| {
                    calls += 1;
                    assert_eq!(repository, Path::new("repository"));
                    assert_eq!(actual, &expected);
                    assert_eq!(original.deadline(), budget.deadline());
                    Ok(ResolvedReference::default())
                },
            )
            .unwrap();
            assert_eq!(calls, 1);
        }
        let defaults = select(&scenarios, &[]).unwrap();
        resolve_selected_reference(&defaults, Path::new("repository"), &budget, |_, _, _| {
            panic!("default selection must not resolve references")
        })
        .unwrap();
    }

    #[test]
    fn reference_failure_precedes_every_scenario_launch() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("binary");
        let config = root.path().join("config");
        fs::write(&binary, "not an executable").unwrap();
        fs::write(&config, "not a server config").unwrap();
        let arguments = vec![
            "--only".to_owned(),
            "trino-interop/encoding-conformance".to_owned(),
            "--binary".to_owned(),
            binary.display().to_string(),
            "--config".to_owned(),
            config.display().to_string(),
            "--artifact-root".to_owned(),
            root.path().display().to_string(),
        ];
        let cli = Cli::parse(arguments).unwrap();
        let mut calls = 0;
        let error = run_dispatch_with_reference_resolver(
            cli,
            crate::build_information::BuildInformation::fixture(),
            |_, _, _| {
                calls += 1;
                bail!("original reference resolution failed")
            },
        )
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(error.to_string(), "original reference resolution failed");
        assert!(
            !root
                .path()
                .join("trino-interop-encoding-conformance")
                .exists()
        );
    }

    #[test]
    fn listing_options_never_resolve_reference_inputs() {
        for flag in [
            "--list",
            "--list-default",
            "--list-declarations",
            "--list-binary-requirements",
            "--list-compatibility",
        ] {
            let cli = Cli::parse([flag.to_owned()]).unwrap();
            run_dispatch_with_reference_resolver(
                cli,
                crate::build_information::BuildInformation::fixture(),
                |_, _, _| panic!("listing must not resolve reference inputs"),
            )
            .unwrap();
        }
    }

    #[test]
    fn default_selection_accepts_the_default_launch_inputs() {
        let scenarios = crate::scenarios::all();
        for scenario in select(&scenarios, &[]).expect("select default system baseline") {
            scenario
                .validate_runner_inputs(novarocks_cluster_harness::LaunchProfile::FaultScenario)
                .unwrap_or_else(|error| panic!("default scenario {}: {error:#}", scenario.name()));
        }
    }
}

// Ordinary preparation errors retain every source and show their full diagnostics.
// Exact and held fixtures use their bounded presentation below instead.
struct PrelaunchScenarioFailure {
    primary: Option<anyhow::Error>,
    settlement: Option<anyhow::Error>,
    fixture: Option<anyhow::Error>,
}
impl std::fmt::Display for PrelaunchScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("system scenario preparation failed")?;
        if let Some(error) = &self.primary {
            write!(f, "; primary: {error:#}")?;
        }
        if let Some(error) = &self.settlement {
            write!(f, "; settlement failed: {error:#}")?;
        }
        if let Some(error) = &self.fixture {
            write!(f, "; fixture teardown failed: {error:#}")?;
        }
        Ok(())
    }
}
impl std::fmt::Debug for PrelaunchScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for PrelaunchScenarioFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary
            .as_ref()
            .or(self.settlement.as_ref())
            .or(self.fixture.as_ref())
            .map(|error| error.as_ref())
    }
}
pub(crate) fn fixture_admission_rejected(error: &anyhow::Error) -> bool {
    if let Some(failure) = error.downcast_ref::<PrelaunchScenarioFailure>() {
        return failure.settlement.is_none()
            && failure.fixture.is_none()
            && failure
                .primary
                .as_ref()
                .is_some_and(fixture_admission_rejected);
    }
    if let Some(failure) = error.downcast_ref::<ObservedScenarioFailure>() {
        return failure.cluster.is_none()
            && failure.fixture.is_none()
            && failure
                .primary
                .as_ref()
                .is_some_and(fixture_admission_rejected);
    }
    error
        .chain()
        .any(|source| source.is::<novarocks_cluster_harness::fixture_run::FixtureBudgetRejected>())
}

fn finish_prelaunch_scenario(
    bounded: bool,
    primary: Option<anyhow::Error>,
    settlement: Result<()>,
    fixture: Result<()>,
) -> Result<()> {
    if bounded {
        return finish_observed_scenario(primary, settlement, fixture);
    }
    let settlement = settlement.err();
    let fixture = fixture.err();
    if settlement.is_none() && fixture.is_none() {
        return primary.map_or(Ok(()), Err);
    }
    Err(PrelaunchScenarioFailure {
        primary,
        settlement,
        fixture,
    }
    .into())
}

// Preserve original source and actual cleanup errors through bounded presentation.
struct ObservedScenarioFailure {
    primary: Option<anyhow::Error>,
    cluster: Option<anyhow::Error>,
    fixture: Option<anyhow::Error>,
}
impl std::fmt::Display for ObservedScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "observed scenario failed: primary={} role_cleanup={} fixture_cleanup={}",
            self.primary.is_some(),
            self.cluster.is_some(),
            self.fixture.is_some()
        )
    }
}
impl std::fmt::Debug for ObservedScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ObservedScenarioFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary
            .as_ref()
            .or(self.cluster.as_ref())
            .or(self.fixture.as_ref())
            .map(|error| error.as_ref())
    }
}
fn finish_observed_scenario(
    primary: Option<anyhow::Error>,
    cluster: Result<()>,
    fixture: Result<()>,
) -> Result<()> {
    let cluster = cluster.err();
    let fixture = fixture.err();
    if primary.is_none() && cluster.is_none() && fixture.is_none() {
        Ok(())
    } else {
        Err(ObservedScenarioFailure {
            primary,
            cluster,
            fixture,
        }
        .into())
    }
}
#[cfg(all(test, unix))]
mod observed_cleanup_tests {
    use super::*;
    #[test]
    fn cleanup_keeps_owned_primary_and_secondary_error_sources() {
        let primary = std::fs::File::open("/dev/null/no-original-file").unwrap_err();
        let error = finish_observed_scenario(
            Some(primary.into()),
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe).into()),
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()),
        )
        .unwrap_err();
        let owned = error.downcast_ref::<ObservedScenarioFailure>().unwrap();
        assert!(
            owned
                .primary
                .as_ref()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .is_some()
        );
        assert_eq!(
            owned
                .cluster
                .as_ref()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert!(owned.fixture.is_some());
        assert!(finish_observed_scenario(None, Ok(()), Ok(())).is_ok());
    }
}

/// Original source errors can contain raw child output. Preserve them for
/// typed inspection while terminal presentation uses only the bounded verdict.
pub(crate) fn observed_failure_presentation(error: &anyhow::Error) -> Option<String> {
    error
        .chain()
        .find_map(|source| source.downcast_ref::<ObservedScenarioFailure>())
        .map(|failure| failure.to_string())
}

#[cfg(test)]
mod observed_presentation_tests {
    use super::*;
    #[test]
    fn terminal_presentation_does_not_expand_retained_raw_source_canary() {
        let canary = "0123456789abcdef0123456789abcdef";
        let original = std::io::Error::other(format!("actual child tail: {canary}"));
        let error = finish_observed_scenario(Some(original.into()), Ok(()), Ok(())).unwrap_err();
        // The original source remains inspectable; the main terminal path uses
        // this exact bounded formatter instead of anyhow's expanded chain.
        assert!(format!("{error:#}").contains(canary));
        let shown =
            observed_failure_presentation(&error.context("outer scenario context")).unwrap();
        assert!(shown.contains("primary=true"));
        assert!(!shown.contains(canary));
        assert!(observed_failure_presentation(&anyhow::anyhow!("ordinary")).is_none());
    }
}

#[cfg(test)]
mod neutral_root_prelaunch_tests {
    use super::*;
    use crate::scenario::{ScenarioBinary, ScenarioLaunchConfig};
    use novarocks_cluster_harness::LaunchProfile;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[derive(Clone, Copy)]
    enum Mode {
        LaunchFailure,
        ClockFailure,
        Neutral,
    }
    #[derive(Debug)]
    struct OriginalSource(Arc<u8>);
    impl std::fmt::Display for OriginalSource {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original prelaunch component source")
        }
    }
    impl std::error::Error for OriginalSource {}
    struct Fixture {
        mode: Mode,
        clock: Mutex<Option<Instant>>,
        primary: Arc<u8>,
        secondary: Arc<u8>,
        teardowns: AtomicUsize,
    }
    impl Fixture {
        fn new(mode: Mode) -> Self {
            Self {
                mode,
                clock: Mutex::new(None),
                primary: Arc::new(7),
                secondary: Arc::new(9),
                teardowns: AtomicUsize::new(0),
            }
        }
        fn error(source: &Arc<u8>) -> anyhow::Error {
            std::io::Error::other(OriginalSource(source.clone())).into()
        }
    }
    impl Scenario for Fixture {
        fn name(&self) -> &'static str {
            "component/neutral-prelaunch-refusal"
        }
        fn launch_config(&self, _: &std::path::Path) -> Result<ScenarioLaunchConfig> {
            if matches!(self.mode, Mode::LaunchFailure) {
                return Err(Self::error(&self.primary).context("prepare original fixture"));
            }
            let mut clock = self.clock.lock().unwrap();
            assert!(clock.is_none());
            *clock = Some(Instant::now() + Duration::from_secs(20));
            let mut config = ScenarioLaunchConfig::default();
            // An absent compatible binary makes any missed early guard fail at
            // a different branch without ever starting a role.
            config.binary_layout.frontend = ScenarioBinary::Compatible;
            Ok(config)
        }
        fn root_observation_deadline(&self) -> Result<Option<Instant>> {
            if matches!(self.mode, Mode::ClockFailure) {
                return Err(Self::error(&self.primary));
            }
            Ok(*self.clock.lock().unwrap())
        }
        fn run(&self, _: &mut ScenarioContext) -> Result<()> {
            panic!("prelaunch refusal must never enter a role-backed scenario");
        }
        fn teardown(&self) -> Result<()> {
            self.teardowns.fetch_add(1, Ordering::SeqCst);
            if matches!(self.mode, Mode::LaunchFailure | Mode::ClockFailure) {
                Err(Self::error(&self.secondary).context("release original fixture"))
            } else {
                Ok(())
            }
        }
    }
    fn configuration(root: &std::path::Path) -> RunnerConfig {
        RunnerConfig {
            reference: Default::default(),
            build: crate::build_information::BuildInformation::fixture(),
            binary: root.join("no-binary"),
            compatible_binary: None,
            other_island_binary: None,
            base_config_path: root.join("no-config"),
            artifact_root: root.into(),
            cluster_size: 3,
            timeout: Duration::from_secs(20),
            fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget::from_environment(
                3600,
            )
            .unwrap(),
            launch_profile: LaunchProfile::FaultScenario,
        }
    }
    fn assert_source(error: &anyhow::Error, expected: &Arc<u8>) {
        let io = error.downcast_ref::<std::io::Error>().unwrap();
        let source = io
            .get_ref()
            .unwrap()
            .downcast_ref::<OriginalSource>()
            .unwrap();
        assert!(Arc::ptr_eq(&source.0, expected));
    }
    #[test]
    fn exhausted_original_budget_refuses_before_launch_config_side_effect() {
        use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
        let root = tempfile::tempdir().unwrap();
        let fixture = Fixture::new(Mode::Neutral);
        let mut config = configuration(root.path());
        let created = 1_792_540_800_000;
        let identity = ScratchIdentity::create_at(
            "s3://novarocks/tests",
            created,
            "0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        // A valid admitted run with an already elapsed monotonic work clock.
        config.fixture_budget = RunBudget::admit_at(
            FixturePolicy::default(),
            identity,
            301,
            None,
            created,
            Instant::now() - Duration::from_secs(2),
        )
        .unwrap();
        let error = run_one(&fixture, &config).unwrap_err();
        assert!(error.is::<novarocks_cluster_harness::fixture_run::FixtureBudgetExpired>());
        assert!(fixture.clock.lock().unwrap().is_none());
        assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
        assert!(!fixture_admission_rejected(&error));
    }

    #[test]
    fn admission_is_blocked_only_when_cleanup_also_succeeds() {
        let rejection =
            || novarocks_cluster_harness::fixture_run::rejected(anyhow::anyhow!("invalid input"));
        assert!(fixture_admission_rejected(&rejection()));
        let clean = finish_observed_scenario(Some(rejection()), Ok(()), Ok(())).unwrap_err();
        assert!(fixture_admission_rejected(&clean));
        let cleanup_failure = finish_observed_scenario(
            Some(rejection()),
            Ok(()),
            Err(anyhow::anyhow!("cleanup failed")),
        )
        .unwrap_err();
        assert!(!fixture_admission_rejected(&cleanup_failure));
    }

    #[test]
    fn actual_launch_and_clock_errors_retain_both_original_sources_and_teardown_once() {
        let root =
            std::env::temp_dir().join(format!("novarocks-neutral-source-{}", std::process::id()));
        for mode in [Mode::LaunchFailure, Mode::ClockFailure] {
            let fixture = Fixture::new(mode);
            let outcome = run_one(&fixture, &configuration(&root));
            let cleanup = std::fs::remove_dir_all(&root);
            assert!(cleanup.is_ok());
            let error = outcome.unwrap_err();
            let failure = error.downcast_ref::<PrelaunchScenarioFailure>().unwrap();
            assert_source(failure.primary.as_ref().unwrap(), &fixture.primary);
            assert_source(failure.fixture.as_ref().unwrap(), &fixture.secondary);
            assert!(failure.settlement.is_none());
            assert!(observed_failure_presentation(&error).is_none());
            let shown = format!("{error:#}");
            assert!(shown.contains("original prelaunch component source"));
            if matches!(mode, Mode::LaunchFailure) {
                assert!(shown.contains(
                    "primary: prepare original fixture: original prelaunch component source"
                ));
            }
            assert!(shown.contains("fixture teardown failed: release original fixture: original prelaunch component source"));
            assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn neutral_topology_and_binary_refusals_keep_one_original_teardown() {
        let root =
            std::env::temp_dir().join(format!("novarocks-neutral-topology-{}", std::process::id()));
        for (profile, count) in [
            (LaunchProfile::FaultScenario, 2),
            (LaunchProfile::Performance, 3),
            (LaunchProfile::FaultScenario, 3),
        ] {
            let fixture = Fixture::new(Mode::Neutral);
            let mut config = configuration(&root);
            config.launch_profile = profile;
            config.cluster_size = count;
            let outcome = run_one(&fixture, &config);
            let cleanup = std::fs::remove_dir_all(&root);
            assert!(cleanup.is_ok());
            let error = outcome.unwrap_err();
            let failure = error.downcast_ref::<ObservedScenarioFailure>().unwrap();
            assert!(
                failure.primary.is_some() && failure.cluster.is_none() && failure.fixture.is_none()
            );
            assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
        }
    }
}

#[cfg(test)]
mod settlement_integration_tests {
    use super::*;
    use crate::scenario::ScenarioLaunchConfig;
    use novarocks_cluster_harness::resource_registration::{self, Registration};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct Resource {
        registration: Registration,
        fail_cleanup: bool,
    }
    impl Drop for Resource {
        fn drop(&mut self) {
            self.registration
                .settle(&if self.fail_cleanup {
                    Err(anyhow::anyhow!("owner Drop cleanup failed"))
                } else {
                    Ok(())
                })
                .unwrap();
        }
    }
    struct Fixture {
        mode: &'static str,
        teardown_fails: bool,
        teardowns: AtomicUsize,
    }
    impl Scenario for Fixture {
        fn name(&self) -> &'static str {
            "component/settlement-integration"
        }
        fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
            let resource = Resource {
                registration: resource_registration::register(
                    root,
                    "fake",
                    "new-resource",
                    "fake-project",
                )?,
                fail_cleanup: self.mode == "drop-fails",
            };
            if self.mode == "forgotten" {
                std::mem::forget(resource);
            } else {
                drop(resource);
            }
            if matches!(
                self.mode,
                "drop-fails" | "forgotten" | "launch-config-fails"
            ) {
                bail!("launch preparation failed after resource creation");
            }
            Ok(ScenarioLaunchConfig::default())
        }
        fn root_observation_deadline(&self) -> Result<Option<Instant>> {
            anyhow::ensure!(
                self.mode != "clock-fails",
                "original deadline capture failed"
            );
            Ok(None)
        }
        fn run(&self, _: &mut ScenarioContext) -> Result<()> {
            panic!("test never starts a working cluster")
        }
        fn teardown(&self) -> Result<()> {
            self.teardowns.fetch_add(1, Ordering::SeqCst);
            anyhow::ensure!(!self.teardown_fails, "fixture teardown failed");
            Ok(())
        }
    }
    fn config(root: &Path) -> RunnerConfig {
        RunnerConfig {
            reference: Default::default(),
            build: crate::build_information::BuildInformation::fixture(),
            binary: root.join("deliberately-missing-binary"),
            compatible_binary: None,
            other_island_binary: None,
            base_config_path: root.join("deliberately-missing-config"),
            artifact_root: root.to_owned(),
            cluster_size: 3,
            timeout: Duration::from_millis(100),
            fixture_budget: novarocks_cluster_harness::fixture_run::RunBudget::from_environment(
                3600,
            )
            .unwrap(),
            launch_profile: novarocks_cluster_harness::LaunchProfile::FaultScenario,
        }
    }
    #[test]
    fn actual_early_returns_report_owner_cleanup_without_inferring_from_empty_slots() {
        for mode in [
            "drop-fails",
            "forgotten",
            "launch-config-fails",
            "clock-fails",
            "cluster-fails",
        ] {
            for teardown_fails in [true, false] {
                let root = tempfile::tempdir().unwrap();
                let scenario_root = root.path().join("component-settlement-integration");
                let _old = resource_registration::register(
                    &scenario_root,
                    "fake",
                    "old-resource",
                    "old-project",
                )
                .unwrap();
                let fixture = Fixture {
                    mode,
                    teardown_fails,
                    teardowns: AtomicUsize::new(0),
                };
                assert!(run_one(&fixture, &config(root.path())).is_err());
                assert_eq!(fixture.teardowns.load(Ordering::SeqCst), 1);
                let record: serde_json::Value = serde_json::from_slice(
                    &fs::read(scenario_root.join("scenario-teardown.json")).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    record["fixture"]["teardown"]["status"],
                    if teardown_fails { "failed" } else { "ok" }
                );
                let resources = record["fixture"]["resources"].as_array().unwrap();
                assert_eq!(resources.len(), 1, "old registration excluded");
                assert_eq!(resources[0]["id"], "new-resource");
                assert_eq!(
                    resources[0]["state"],
                    match mode {
                        "drop-fails" => "cleanup_failed",
                        "forgotten" => "unsettled",
                        _ => "released",
                    }
                );
                assert_eq!(
                    record["fixture"]["launch_config"],
                    if matches!(mode, "drop-fails" | "forgotten" | "launch-config-fails") {
                        "failed"
                    } else {
                        "ok"
                    }
                );
                assert_eq!(
                    record["cluster"]["launch"],
                    if mode == "cluster-fails" {
                        "failed"
                    } else {
                        "not_attempted"
                    }
                );
                assert_eq!(
                    record["cluster"]["partial_cleanup"],
                    if mode == "cluster-fails" {
                        serde_json::json!("monitor")
                    } else {
                        serde_json::Value::Null
                    }
                );
            }
        }
    }
}
