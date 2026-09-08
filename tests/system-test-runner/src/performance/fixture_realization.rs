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

use super::fixture_identity::{PreparedWindowIdentity, PreparedWindowSemanticIdentity};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestRuntimeIdentity;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const FIXTURE_REALIZATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct FixtureRealizationReference {
    pub artifact_sha256: String,
    pub semantics_sha256: String,
}

#[derive(Serialize)]
struct SourceArtifact {
    path: String,
    sha256: String,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum RawFixtureRealization<'a> {
    None,
    Mixed {
        provider_runtime: &'a IsolatedIcebergRestRuntimeIdentity,
        diagnostic: &'a PreparedWindowIdentity,
        timed: &'a [PreparedWindowIdentity],
    },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum SemanticFixtureRealization<'a> {
    None,
    Mixed {
        provider_runtime: &'a IsolatedIcebergRestRuntimeIdentity,
        diagnostic: PreparedWindowSemanticIdentity,
        timed: Vec<PreparedWindowSemanticIdentity>,
    },
}

#[derive(Serialize)]
struct RawBundle<'a> {
    realization: &'a RawFixtureRealization<'a>,
    source_artifacts: &'a [SourceArtifact],
}

#[derive(Serialize)]
struct FixtureRealizationArtifact<'a> {
    schema_version: u32,
    scenario: &'a str,
    source_artifacts: &'a [SourceArtifact],
    raw_bundle_sha256: &'a str,
    semantics_sha256: &'a str,
    raw: &'a RawFixtureRealization<'a>,
    semantic: &'a SemanticFixtureRealization<'a>,
}

pub fn write_fixture_realization(
    root: &Path,
    scenario: &str,
    provider_runtime: Option<&IsolatedIcebergRestRuntimeIdentity>,
    diagnostic: Option<&PreparedWindowIdentity>,
    timed: &[PreparedWindowIdentity],
) -> Result<FixtureRealizationReference> {
    let mixed = scenario == "performance/uea1-mixed";
    ensure!(
        mixed == (provider_runtime.is_some() && diagnostic.is_some() && !timed.is_empty()),
        "fixture realization does not match the measured scenario"
    );
    ensure!(
        mixed || (provider_runtime.is_none() && diagnostic.is_none() && timed.is_empty()),
        "non-mixed fixture realization contains provider state"
    );

    let source_artifacts = if mixed {
        collect_mixed_source_artifacts(root, timed)?
    } else {
        Vec::new()
    };
    let raw = match (provider_runtime, diagnostic) {
        (Some(provider_runtime), Some(diagnostic)) => RawFixtureRealization::Mixed {
            provider_runtime,
            diagnostic,
            timed,
        },
        (None, None) => RawFixtureRealization::None,
        _ => unreachable!("fixture cardinality was checked"),
    };
    let semantic = match (provider_runtime, diagnostic) {
        (Some(provider_runtime), Some(diagnostic)) => SemanticFixtureRealization::Mixed {
            provider_runtime,
            diagnostic: diagnostic.semantic_identity(),
            timed: timed
                .iter()
                .map(PreparedWindowIdentity::semantic_identity)
                .collect(),
        },
        (None, None) => SemanticFixtureRealization::None,
        _ => unreachable!("fixture cardinality was checked"),
    };
    let raw_bundle_sha256 = canonical_sha256(&RawBundle {
        realization: &raw,
        source_artifacts: &source_artifacts,
    })?;
    let semantics_sha256 = canonical_sha256(&semantic)?;
    let artifact = FixtureRealizationArtifact {
        schema_version: FIXTURE_REALIZATION_SCHEMA_VERSION,
        scenario,
        source_artifacts: &source_artifacts,
        raw_bundle_sha256: &raw_bundle_sha256,
        semantics_sha256: &semantics_sha256,
        raw: &raw,
        semantic: &semantic,
    };
    let bytes = serde_json::to_vec_pretty(&artifact)
        .context("serialize UEA-1 fixture realization artifact")?;
    let artifact_sha256 = sha256_bytes(&bytes);
    fs::write(root.join("fixture-realization.json"), bytes)
        .with_context(|| format!("write fixture realization under {}", root.display()))?;
    Ok(FixtureRealizationReference {
        artifact_sha256,
        semantics_sha256,
    })
}

fn collect_mixed_source_artifacts(
    root: &Path,
    timed: &[PreparedWindowIdentity],
) -> Result<Vec<SourceArtifact>> {
    let mut artifacts = Vec::with_capacity(timed.len());
    for (window_index, expected) in timed.iter().enumerate() {
        ensure!(
            expected.window_index == window_index,
            "mixed fixture realization has a missing or reordered timed window"
        );
        let name = format!("mixed-fixture-{window_index}.json");
        let path = root.join(&name);
        let bytes = fs::read(&path)
            .with_context(|| format!("read exact mixed fixture artifact {}", path.display()))?;
        let document: Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("decode mixed fixture artifact {}", path.display()))?;
        let actual = document
            .get("fixture_identity")
            .context("mixed fixture artifact omitted fixture_identity")?;
        ensure!(
            actual == &serde_json::to_value(expected)?,
            "mixed fixture artifact identity differs from the measured window"
        );
        artifacts.push(SourceArtifact {
            path: name,
            sha256: sha256_bytes(&bytes),
        });
    }
    Ok(artifacts)
}

fn canonical_sha256(value: &impl Serialize) -> Result<String> {
    let value = serde_json::to_value(value).context("materialize canonical JSON value")?;
    Ok(sha256_bytes(canonical_json(&value).as_bytes()))
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("serialize JSON string"),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(values) => {
            let mut fields = values.iter().collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            format!(
                "{{{}}}",
                fields
                    .into_iter()
                    .map(|(name, value)| format!(
                        "{}:{}",
                        serde_json::to_string(name).expect("serialize JSON field"),
                        canonical_json(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_realization_has_a_recomputable_semantic_hash() {
        let root = std::env::temp_dir().join(format!(
            "novarocks-none-realization-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        let reference =
            write_fixture_realization(&root, "performance/uea1-short-concurrent", None, None, &[])
                .expect("write none realization");
        let bytes = fs::read(root.join("fixture-realization.json")).expect("read realization");
        let document: Value = serde_json::from_slice(&bytes).expect("decode realization");
        assert_eq!(document["raw"]["kind"], "none");
        assert_eq!(document["semantic"]["kind"], "none");
        assert_eq!(document["semantics_sha256"], reference.semantics_sha256);
        assert_eq!(sha256_bytes(&bytes), reference.artifact_sha256);
        fs::remove_dir_all(root).expect("remove fixture root");
    }
}
