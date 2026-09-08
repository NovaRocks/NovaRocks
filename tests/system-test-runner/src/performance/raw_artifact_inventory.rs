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

use super::provenance::{sha256_bytes, sha256_file};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub const RAW_ARTIFACT_INVENTORY_FILE: &str = "raw-artifact-inventory.json";

#[derive(Debug)]
pub struct RawArtifactInventoryReference {
    pub artifact_sha256: String,
}

#[derive(Debug, Serialize)]
struct RawArtifactInventory {
    schema_version: u32,
    scenario: String,
    window_count: usize,
    artifacts: Vec<RawArtifact>,
}

#[derive(Debug, Serialize)]
struct RawArtifact {
    kind: &'static str,
    window_index: Option<usize>,
    path: String,
    sha256: String,
}

pub fn write_raw_artifact_inventory(
    root: &Path,
    scenario: &str,
    window_count: usize,
) -> Result<RawArtifactInventoryReference> {
    ensure!(window_count > 0, "raw artifact inventory requires windows");
    let expected = expected_artifacts(scenario, window_count)?;
    let mut artifacts = Vec::with_capacity(expected.len());
    for (kind, window_index, name) in expected {
        let path = root.join(&name);
        ensure!(path.is_file(), "raw artifact {} is missing", path.display());
        artifacts.push(RawArtifact {
            kind,
            window_index,
            path: name,
            sha256: sha256_file(&path)?,
        });
    }
    let inventory = RawArtifactInventory {
        schema_version: 1,
        scenario: scenario.to_string(),
        window_count,
        artifacts,
    };
    let bytes =
        serde_json::to_vec_pretty(&inventory).context("serialize UEA-1 raw artifact inventory")?;
    let artifact_sha256 = sha256_bytes(&bytes);
    fs::write(root.join(RAW_ARTIFACT_INVENTORY_FILE), bytes)
        .context("write UEA-1 raw artifact inventory")?;
    Ok(RawArtifactInventoryReference { artifact_sha256 })
}

fn expected_artifacts(
    scenario: &str,
    window_count: usize,
) -> Result<Vec<(&'static str, Option<usize>, String)>> {
    if scenario != "performance/uea1-mixed" {
        ensure!(
            matches!(
                scenario,
                "performance/uea1-short-concurrent" | "performance/uea1-slow-output"
            ),
            "unsupported UEA-1 performance scenario {scenario}"
        );
        return Ok(Vec::new());
    }
    let mut artifacts = Vec::with_capacity(window_count * 2 + 1);
    for window_index in 0..window_count {
        artifacts.push((
            "business",
            Some(window_index),
            format!("mixed-business-{window_index}.json"),
        ));
        artifacts.push((
            "query",
            Some(window_index),
            format!("mixed-query-{window_index}.json"),
        ));
    }
    artifacts.push((
        "business-aggregate",
        None,
        "mixed-business.json".to_string(),
    ));
    Ok(artifacts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_inventory_is_closed_and_ordered() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-raw-inventory-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create fixture");
        for name in [
            "mixed-business-0.json",
            "mixed-query-0.json",
            "mixed-business.json",
        ] {
            fs::write(root.join(name), name).expect("write raw fixture");
        }
        let reference = write_raw_artifact_inventory(&root, "performance/uea1-mixed", 1)
            .expect("write inventory");
        let bytes = fs::read(root.join(RAW_ARTIFACT_INVENTORY_FILE)).expect("read inventory");
        assert_eq!(reference.artifact_sha256, sha256_bytes(&bytes));
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("decode inventory");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["artifacts"][0]["path"], "mixed-business-0.json");
        assert_eq!(value["artifacts"][2]["path"], "mixed-business.json");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn mixed_inventory_requires_every_raw_artifact() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-uea1-raw-inventory-missing-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create fixture");
        let error = write_raw_artifact_inventory(&root, "performance/uea1-mixed", 1)
            .expect_err("missing artifact must fail");
        assert!(error.to_string().contains("is missing"));
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
