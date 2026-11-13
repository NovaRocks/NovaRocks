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

//! Resolve declared reference inputs once, through the fixture input builder.
// Design: ADR-0231 (docs/adr/ADR-0231-reference-inputs-declared-once-and-bound-per-run.md)
use crate::fixture_run::{FixturePolicy, RunBudget};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReferenceInput {
    TrinoImage,
    TrinoJdbcDriver,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrinoImageRef {
    pub alias: String,
    pub image_id: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub name: String,
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trino_image: Option<TrinoImageRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trino_jdbc_driver: Option<ArtifactRef>,
}

impl ResolvedReference {
    pub fn subset(&self, inputs: &BTreeSet<ReferenceInput>) -> Self {
        Self {
            trino_image: inputs
                .contains(&ReferenceInput::TrinoImage)
                .then(|| self.trino_image.clone())
                .flatten(),
            trino_jdbc_driver: inputs
                .contains(&ReferenceInput::TrinoJdbcDriver)
                .then(|| self.trino_jdbc_driver.clone())
                .flatten(),
        }
    }
}

/// Standalone consumers inherit the existing fixture context or policy budget.
pub fn resolve(repo_root: &Path, inputs: &BTreeSet<ReferenceInput>) -> Result<ResolvedReference> {
    if inputs.is_empty() {
        return Ok(ResolvedReference::default());
    }
    let budget = RunBudget::from_environment(FixturePolicy::from_environment()?.max_run_seconds)?;
    resolve_until(repo_root, inputs, &budget)
}

/// Runner consumers retain their original cumulative run and cleanup clocks.
pub fn resolve_until(
    repo_root: &Path,
    inputs: &BTreeSet<ReferenceInput>,
    budget: &RunBudget,
) -> Result<ResolvedReference> {
    resolve_with_builder(
        repo_root,
        inputs,
        budget,
        &repo_root.join("docker/fixture-inputs/provision.sh"),
    )
}

fn required_string<'a>(value: &'a serde_json::Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("fixture builder result omitted {name}"))
}

pub(crate) fn resolve_with_builder(
    repo_root: &Path,
    inputs: &BTreeSet<ReferenceInput>,
    budget: &RunBudget,
    builder: &Path,
) -> Result<ResolvedReference> {
    let mut resolved = ResolvedReference::default();
    for input in inputs {
        let declaration = if *input == ReferenceInput::TrinoJdbcDriver {
            Some(serde_json::from_slice::<serde_json::Value>(&fs::read(
                repo_root.join("docker/fixture-inputs/versions.json"),
            )?)?)
        } else {
            None
        };
        let artifact_name = declaration
            .as_ref()
            .map(|value| required_string(&value["images"]["trino"], "jdbc_artifact"))
            .transpose()?;
        let consumer = match input {
            ReferenceInput::TrinoImage => "image:trino".to_owned(),
            ReferenceInput::TrinoJdbcDriver => {
                format!("artifact:{}", artifact_name.expect("driver declaration"))
            }
        };
        // Keep both channels and the original invocation even if spawn or JSON decoding fails.
        let evidence_root = repo_root.join("logs/reference-inputs");
        fs::create_dir_all(&evidence_root)?;
        let directory = tempfile::Builder::new()
            .prefix("resolution-")
            .tempdir_in(&evidence_root)?
            .keep();
        let result_path = directory.join("result.json");
        let stdout_path = directory.join("stdout.log");
        let stderr_path = directory.join("stderr.log");
        let mut command = novarocks_test_support::ManagedCommand::new(builder);
        command
            .current_dir(repo_root)
            .arg("--consumer")
            .arg(&consumer)
            .arg("--result")
            .arg(&result_path)
            .terminate_at(budget.work_deadline(), budget.deadline())
            .observe_group_exit_until(budget.deadline());
        fs::write(directory.join("command.txt"), format!("{command:?}\n"))?;
        let evidence = format!(
            "command={}; stdout={}; stderr={}",
            directory.join("command.txt").display(),
            stdout_path.display(),
            stderr_path.display()
        );
        let outcome = novarocks_test_support::capture_command_until(
            command,
            File::create(&stdout_path)?,
            File::create(&stderr_path)?,
            budget.deadline(),
        )
        .with_context(|| format!("reference input {consumer} failed; {evidence}"))?;
        ensure!(
            outcome.output.status.success()
                && !outcome.timed_out
                && outcome.failures.is_empty()
                && outcome.group_exit_observed(),
            "reference input {consumer} failed: status={}, timeout={}, actual_group_exit={}, cleanup={:?}; {evidence}; monitor={}",
            outcome.output.status,
            outcome.timed_out,
            outcome.group_exit_observed(),
            outcome.failures,
            outcome.diagnostic_root().display()
        );
        let result = (|| -> Result<()> {
            // stdout is diagnostic text and must never be interpreted as JSON.
            let value: serde_json::Value = serde_json::from_slice(
                &fs::read(&result_path).context("read fixture builder result file")?,
            )
            .context("decode fixture builder result file")?;
            match input {
                ReferenceInput::TrinoImage => {
                    let image = &value["images"]["trino"];
                    resolved.trino_image = Some(TrinoImageRef {
                        alias: required_string(image, "alias")?.to_owned(),
                        image_id: required_string(image, "image_id")?.to_owned(),
                        version: required_string(image, "version")?.to_owned(),
                    });
                }
                ReferenceInput::TrinoJdbcDriver => {
                    let name = artifact_name.expect("driver declaration");
                    let path = Path::new(required_string(&value, "artifact_dir")?).join(name);
                    ensure!(
                        path.is_absolute() && path.is_file(),
                        "fixture builder driver path is not an existing absolute file: {}",
                        path.display()
                    );
                    resolved.trino_jdbc_driver = Some(ArtifactRef {
                        name: name.to_owned(),
                        path,
                        sha256: required_string(
                            &declaration.as_ref().expect("driver declaration")["artifacts"][name],
                            "sha256",
                        )?
                        .to_owned(),
                    });
                }
            }
            Ok(())
        })();
        result.with_context(|| format!("reference input {consumer} result failed; {evidence}"))?;
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests;
