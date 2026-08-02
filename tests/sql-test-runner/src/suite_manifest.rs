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
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Suite metadata deliberately controls discovery only. Cluster mode and
/// cardinality are runner CLI concerns, so a suite cannot select a runtime.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct SuiteManifest {
    pub explicit_only: bool,
}

impl Default for SuiteManifest {
    fn default() -> Self {
        Self {
            explicit_only: false,
        }
    }
}

impl SuiteManifest {
    pub fn parse(content: &str) -> Result<Self> {
        toml::from_str(content).context("failed to parse suite manifest")
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
            .filter(|(_, suite)| !suite.manifest.explicit_only)
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
        suites.get(name).with_context(|| {
            format!(
                "unknown suite '{}'; available suites: {}",
                name,
                all_available.join(", ")
            )
        })?;
    }

    Ok(suite_names)
}

#[cfg(test)]
mod tests {
    use super::{SuiteManifest, select_suite_names};
    use crate::types::SuiteConfig;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn fixture_suites<const N: usize>(entries: [(&str, bool); N]) -> BTreeMap<String, SuiteConfig> {
        entries
            .into_iter()
            .map(|(name, explicit_only)| {
                let name = name.to_string();
                let manifest = SuiteManifest { explicit_only };
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
    fn manifest_accepts_only_explicit_only() {
        assert!(SuiteManifest::parse("explicit_only = true\n").unwrap().explicit_only);
        assert!(SuiteManifest::parse("cluster_size = 3\n").is_err());
    }

    #[test]
    fn all_selection_excludes_explicit_only_suites() {
        let suites = fixture_suites([("filter", false), ("explicit", true)]);
        assert_eq!(select_suite_names("all", &suites).unwrap(), vec!["filter"]);
        assert_eq!(select_suite_names("explicit", &suites).unwrap(), vec!["explicit"]);
    }

    #[test]
    fn manifest_defaults_when_suite_toml_is_absent() {
        let temp_dir =
            std::env::temp_dir().join(format!("novarocks-suite-manifest-{}", std::process::id()));
        let manifest = SuiteManifest::load(&temp_dir.join("missing-suite.toml")).unwrap();
        assert_eq!(manifest, SuiteManifest::default());
    }
}
