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

use crate::types::SuiteConfig;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Native static suite facts. Dataset inputs are resolved only when running;
/// cluster mode and cardinality remain runner CLI concerns.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SuiteManifest {
    pub explicit_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_input: Option<CaseInputRecipe>,
    pub disabled_reason: Option<String>,
    pub prepared_dataset: Option<PreparedDatasetRequirement>,
    pub heavy: Option<HeavyDeclaration>,
    pub impact: ImpactDeclaration,
    pub reuse: ReuseDeclaration,
    #[serde(skip_serializing_if = "SuiteSessionDefaults::is_empty")]
    pub session: SuiteSessionDefaults,
}

/// Closed fixture-owned input recipes. No commands or capture expressions are accepted.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum CaseInputRecipe {
    #[serde(rename = "hms-read-only-compatibility-v1")]
    HmsReadOnlyCompatibilityV1,
}

/// Versioned initial session inputs for every case in this suite.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SuiteSessionDefaults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_dop: Option<u32>,
}

impl SuiteSessionDefaults {
    pub fn is_empty(&self) -> bool {
        self.pipeline_dop.is_none()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.pipeline_dop != Some(0),
            "suite session pipeline_dop must be positive"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeavyDeclaration {
    pub reason: HeavyReason,
    pub detail: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HeavyReason {
    Fixture,
    VariantBinary,
    Dataset,
    Toolchain,
    Cost,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ImpactDeclaration {
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum ReuseScope {
    #[default]
    Suite,
    Case,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InitialState {
    Isolated,
    Reset,
    Immutable,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ReuseDeclaration {
    pub scope: ReuseScope,
    pub initial_state: Option<InitialState>,
    pub basis: Option<String>,
    pub env_inputs: Option<Vec<EnvInput>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnvInput {
    pub name: String,
    pub kind: EnvInputKind,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EnvInputKind {
    Value,
    FileContent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreparedDatasetRequirement {
    pub dataset: DatasetName,
    pub scale: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum DatasetName {
    #[serde(rename = "ssb")]
    Ssb,
    #[serde(rename = "tpc-h")]
    TpcH,
    #[serde(rename = "tpc-ds")]
    TpcDs,
}

impl DatasetName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ssb => "ssb",
            Self::TpcH => "tpc-h",
            Self::TpcDs => "tpc-ds",
        }
    }

    fn golden_scale(self) -> &'static str {
        match self {
            Self::Ssb | Self::TpcH => "1",
            Self::TpcDs => "1GB",
        }
    }
}

impl SuiteManifest {
    pub fn parse(content: &str) -> Result<Self> {
        let manifest: Self = toml::from_str(content).context("failed to parse suite manifest")?;
        manifest.session.validate()?;
        anyhow::ensure!(
            manifest.case_input.is_none() || manifest.explicit_only,
            "case input recipes require explicit_only = true"
        );
        if let Some(requirement) = &manifest.prepared_dataset {
            anyhow::ensure!(
                manifest.explicit_only,
                "prepared dataset suites require explicit_only = true"
            );
            anyhow::ensure!(
                requirement.scale == requirement.dataset.golden_scale(),
                "dataset {} scale {} does not match checked-in golden scale {}",
                requirement.dataset.as_str(),
                requirement.scale,
                requirement.dataset.golden_scale()
            );
        }
        if let Some(heavy) = &manifest.heavy {
            anyhow::ensure!(
                !heavy.detail.trim().is_empty(),
                "heavy declaration requires detail"
            );
        }
        if manifest.reuse.initial_state.is_some() {
            anyhow::ensure!(
                manifest
                    .reuse
                    .basis
                    .as_ref()
                    .is_some_and(|v| !v.trim().is_empty()),
                "initial state requires a basis"
            );
            anyhow::ensure!(
                manifest.reuse.env_inputs.is_some(),
                "initial state requires env_inputs"
            );
        }
        anyhow::ensure!(
            !manifest.explicit_only || manifest.heavy.is_some(),
            "explicit SQL suites require a heavy declaration"
        );
        Ok(manifest)
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read suite manifest {}", path.display()))?;
        Self::parse(&content).with_context(|| format!("invalid suite manifest {}", path.display()))
    }
}

pub fn select_suite_names(
    requested: &str,
    suites: &BTreeMap<String, SuiteConfig>,
) -> Result<Vec<String>> {
    let suite_names: Vec<String> = if requested.eq_ignore_ascii_case("all") {
        suites
            .iter()
            .filter(|(_, suite)| {
                !suite.manifest.explicit_only && suite.manifest.disabled_reason.is_none()
            })
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        requested
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(ToString::to_string)
            .collect()
    };

    if suite_names.is_empty() {
        bail!("no suites selected");
    }

    let all_available: Vec<String> = suites.keys().cloned().collect();
    for name in &suite_names {
        let suite = suites.get(name).with_context(|| {
            format!(
                "unknown suite '{}'; available suites: {}",
                name,
                all_available.join(", ")
            )
        })?;
        if let Some(reason) = &suite.manifest.disabled_reason {
            bail!("SQL suite {name} is disabled: {reason}");
        }
    }

    Ok(suite_names)
}

#[cfg(test)]
mod tests {
    use super::{SuiteManifest, select_suite_names};
    use crate::types::SuiteConfig;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn fixture_suites<const N: usize>(entries: [(&str, bool); N]) -> BTreeMap<String, SuiteConfig> {
        entries
            .into_iter()
            .map(|(name, explicit_only)| {
                let name = name.to_string();
                let manifest = SuiteManifest {
                    explicit_only,
                    ..SuiteManifest::default()
                };
                (
                    name.clone(),
                    SuiteConfig {
                        name,
                        sql_dir: PathBuf::new(),
                        result_dir: None,
                        sql_glob: "*.sql".to_string(),
                        default_catalog: "default_catalog".to_string(),
                        default_db: String::new(),
                        auto_case_db: false,
                        verify_default: true,
                        init_sql: None,
                        cleanup_sql: None,
                        manifest,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn manifest_accepts_static_declarations_and_rejects_runtime_fields() {
        assert!(
            SuiteManifest::parse(
                r#"explicit_only = true
heavy = { reason = "cost", detail = "Exhaustive native matrix" }
"#
            )
            .unwrap()
            .explicit_only
        );
        assert!(SuiteManifest::parse("cluster_size = 3\n").is_err());
    }

    #[test]
    fn dataset_manifest_fixes_the_golden_scale() {
        let prefix = r#"explicit_only = true
heavy = { reason = "dataset", detail = "Requires READY SSB scale 1" }
[prepared_dataset]
dataset = "ssb"
"#;
        assert!(SuiteManifest::parse(&format!("{prefix}scale = \"1\"\n")).is_ok());
        assert!(SuiteManifest::parse(&format!("{prefix}scale = \"10\"\n")).is_err());
        assert!(
            SuiteManifest::parse("[prepared_dataset]\ndataset = \"ssb\"\nscale = \"1\"\n").is_err()
        );
    }

    #[test]
    fn all_selection_excludes_explicit_only_suites() {
        let suites = fixture_suites([("filter", false), ("explicit", true)]);
        assert_eq!(select_suite_names("all", &suites).unwrap(), vec!["filter"]);
        assert_eq!(
            select_suite_names("explicit", &suites).unwrap(),
            vec!["explicit"]
        );
    }

    #[test]
    fn disabled_suites_are_not_selected_and_explicit_selection_is_refused() {
        let mut suites = fixture_suites([("filter", false), ("suspended", false)]);
        suites.get_mut("suspended").unwrap().manifest =
            SuiteManifest::parse("disabled_reason = \"pending design\"\n").unwrap();
        assert_eq!(select_suite_names("all", &suites).unwrap(), vec!["filter"]);
        let error = select_suite_names("suspended", &suites).unwrap_err();
        assert!(error.to_string().contains("is disabled: pending design"));
    }

    #[test]
    fn explicit_suites_require_a_nonempty_heavy_basis() {
        assert!(SuiteManifest::parse("explicit_only = true\n").is_err());
        assert!(
            SuiteManifest::parse(
                r#"explicit_only = true
heavy = { reason = "cost", detail = "" }
"#
            )
            .is_err()
        );
    }

    #[test]
    fn suite_session_defaults_are_explicit_positive_and_closed() {
        let absent = SuiteManifest::parse("").unwrap();
        assert!(absent.session.is_empty());
        assert_eq!(absent.session.pipeline_dop, None);
        for dop in [1, 5, u32::MAX] {
            let manifest =
                SuiteManifest::parse(&format!("[session]\npipeline_dop = {dop}\n")).unwrap();
            assert_eq!(manifest.session.pipeline_dop, Some(dop));
        }
        for input in [
            "[session]\npipeline_dop = 0\n",
            "[session]\npipeline_dop = -1\n",
            "[session]\npipeline_dop = 4294967296\n",
            "[session]\npipeline_dop = 1.5\n",
            "[session]\npipeline_dop = '5'\n",
            "[session]\npipeline_dop = 5\nunknown = true\n",
        ] {
            assert!(SuiteManifest::parse(input).is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn corpus_manifests_declare_impact_and_heavy_requirements() {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(4)
            .expect("SQL runner library is inside the repository");
        let root = repository.join("tests/sql/correctness");
        let mut found = false;
        for entry in fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if !path.join("sql").is_dir() {
                continue;
            }
            found = true;
            let manifest_path = path.join("suite.toml");
            assert!(
                manifest_path.is_file(),
                "missing {}",
                manifest_path.display()
            );
            let manifest = SuiteManifest::load(&manifest_path).unwrap();
            assert!(
                !manifest.impact.paths.is_empty(),
                "missing impact paths in {}",
                manifest_path.display()
            );
        }
        assert!(found, "the SQL correctness corpus must be discoverable");
    }

    #[test]
    fn manifest_defaults_when_suite_toml_is_absent() {
        let temp_dir =
            std::env::temp_dir().join(format!("novarocks-suite-manifest-{}", std::process::id()));
        let manifest = SuiteManifest::load(&temp_dir.join("missing-suite.toml")).unwrap();
        assert_eq!(manifest, SuiteManifest::default());
    }
}
