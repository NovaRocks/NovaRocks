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

//! SQL-runner adapter for cluster modes and runner-specific environment
//! resolution. The cross-process lifecycle itself lives in cluster-harness.

use crate::types::RunnerConfig;
use anyhow::{Result, bail};
use clap::ValueEnum;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[allow(unused_imports)]
pub(crate) use novarocks_cluster_harness::{
    BePorts, ClusterProcessRole, CrossProcessRuntime, QueryLifecyclePhase, ServerFailureLogSources,
    ServerHandle, build_novarocks_command, render_cross_process_config, startup_timeout_from_env,
};
use novarocks_cluster_harness::{
    CrossProcessClusterOptions, CrossProcessServerHandle, LaunchProfile,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ClusterMode {
    AllInOne,
    CrossProcess,
}

struct NoopServerHandle;

impl ServerHandle for NoopServerHandle {
    fn target_host(&self) -> Option<&str> {
        None
    }

    fn target_port(&self) -> Option<u16> {
        None
    }
}

pub(crate) fn launch_server(
    mode: ClusterMode,
    cluster_size: usize,
    repo_root: &Path,
    runner_config: &RunnerConfig,
    launch_profile: LaunchProfile,
) -> Result<Box<dyn ServerHandle>> {
    match mode {
        ClusterMode::AllInOne => Ok(Box::new(NoopServerHandle)),
        ClusterMode::CrossProcess => Ok(Box::new(CrossProcessServerHandle::launch(
            CrossProcessClusterOptions {
                binary: discover_novarocks_binary(repo_root)?,
                fe_binary: None,
                be_binaries: Vec::new(),
                expected_eligible_backend_count: None,
                base_config_path: resolve_base_frontend_config_path(repo_root, runner_config)?,
                runtime_root: repo_root.join("tests/sql/.runtime/cluster"),
                cluster_size,
                launch_profile,
                startup_timeout: startup_timeout(),
                child_environment: Default::default(),
                config_overlay: Default::default(),
                native_trust_fixture: Default::default(),
            },
        )?)),
    }
}

/// Validate cluster CLI arguments. Returns an error string on failure.
pub(crate) fn validate_cluster_args(mode: ClusterMode, cluster_size: usize) -> Result<()> {
    if cluster_size == 0 {
        bail!("--cluster-size must be >= 1");
    }
    if mode == ClusterMode::AllInOne && cluster_size > 1 {
        bail!(
            "all-in-one mode requires --cluster-size 1 (got {})",
            cluster_size
        );
    }
    Ok(())
}

pub(crate) fn discover_novarocks_binary(repo_root: &Path) -> Result<PathBuf> {
    discover_novarocks_binary_with_override(
        repo_root,
        std::env::var_os("NOVAROCKS_BIN").map(PathBuf::from),
    )
}

pub(crate) fn discover_novarocks_binary_with_override(
    repo_root: &Path,
    env_override: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = env_override {
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_BIN points to {}, but the file does not exist",
            path.display()
        );
    }

    for candidate in [
        repo_root.join("target/debug/novarocks"),
        repo_root.join("target/release/novarocks"),
    ] {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    bail!(
        "failed to locate novarocks binary; set NOVAROCKS_BIN or run `cargo build --quiet` from {}",
        repo_root.display()
    )
}

fn resolve_base_frontend_config_path(
    repo_root: &Path,
    runner_config: &RunnerConfig,
) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("NOVAROCKS_FE_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!(
            "NOVAROCKS_FE_CONFIG points to {}, but the file does not exist",
            path.display()
        );
    }

    if let Some(path) = runner_config.path.as_ref() {
        let sibling = path.with_extension("toml");
        if sibling.is_file() {
            return Ok(sibling);
        }
    }

    bail!(
        "failed to locate frontend config for cross-process mode under {}",
        repo_root.display()
    )
}

fn startup_timeout() -> Duration {
    startup_timeout_from_env(
        std::env::var("NOVAROCKS_STARTUP_TIMEOUT_SECS")
            .ok()
            .as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_server_handle_rejects_be_process_controls() {
        let mut handle = NoopServerHandle;
        assert!(handle.kill_be(0).is_err());
        assert!(handle.restart_be(0).is_err());
    }

    #[test]
    fn all_in_one_rejects_multiple_backends() {
        let error = validate_cluster_args(ClusterMode::AllInOne, 2).unwrap_err();
        assert!(format!("{error:#}").contains("requires --cluster-size 1"));
    }
}
