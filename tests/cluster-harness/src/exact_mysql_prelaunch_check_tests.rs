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

//! Host-only preparation: a denied actual prepared-config callback must precede every role spawn.
use super::*;
use std::os::unix::fs::PermissionsExt;

#[derive(Debug)]
struct OriginalPrelaunchRefusal;
impl std::fmt::Display for OriginalPrelaunchRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original prepared-config refusal")
    }
}
impl std::error::Error for OriginalPrelaunchRefusal {}

#[test]
fn denied_original_prepared_config_callback_preserves_cause_before_any_role_spawn() -> Result<()> {
    denied_prepared_config_precedes_every_role_spawn(false)
}

#[test]
fn denied_live_prepared_fence_precedes_every_role_and_log_owner() -> Result<()> {
    denied_prepared_config_precedes_every_role_spawn(true)
}

fn denied_prepared_config_precedes_every_role_spawn(live_fences: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let marker = directory.path().join("role-was-spawned");
    let binary = directory.path().join("must-never-run");
    // This executable records a violation if the callback is moved after spawn.
    let marker_quoted = marker.to_string_lossy().replace("'", "'\\''");
    fs::write(
        &binary,
        format!("#!/bin/sh\nprintf violated > '{marker_quoted}'\nexit 1\n"),
    )?;
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700))?;
    let calls = std::cell::Cell::new(0usize);
    let config = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tools/ci/fixtures/system-scenarios-base.toml");
    let options = CrossProcessClusterOptions {
        binary,
        fe_binary: None,
        be_binaries: Vec::new(),
        expected_eligible_backend_count: None,
        base_config_path: config,
        runtime_root: directory.path().join("runtime"),
        cluster_size: 3,
        launch_profile: LaunchProfile::FaultScenario,
        startup_timeout: Duration::from_secs(1),
        child_environment: CrossProcessChildEnvironment::default(),
        config_overlay: CrossProcessConfigOverlay::default(),
        native_trust_fixture: NativeTrustFixture::default(),
    };
    let prepared = |artifact: &EffectiveLaunchConfigEvidence| {
        calls.set(calls.get() + 1);
        ensure!(
            !artifact.artifact_bytes().is_empty(),
            "actual prepared projection absent"
        );
        ensure!(
            artifact.artifact_sha256() == artifact.semantics_sha256(),
            "actual prepared digests differ"
        );
        Err(OriginalPrelaunchRefusal.into())
    };
    let result = if live_fences {
        CrossProcessCluster::launch_with_held_live_source_checks(options, &prepared, &|_, _| {
            anyhow::bail!("log owner observed after refused preparation")
        })
    } else {
        CrossProcessCluster::launch_with_exact_mysql_prelaunch_check(options, &prepared)
    };
    let error = match result {
        Ok(mut unexpected) => {
            unexpected.shutdown()?;
            anyhow::bail!("refused preparation launched original roles")
        }
        Err(error) => error,
    };
    assert!(error.downcast_ref::<OriginalPrelaunchRefusal>().is_some());
    assert_eq!(calls.get(), 1);
    assert!(!marker.exists());
    Ok(())
}
