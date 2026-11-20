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

//! Durable query-definition facts that survive a process restart.
//!
//! The definition deliberately contains the effective user query source rather
//! than an AST printer result. Consumers may derive normalized or qualified
//! compiler input for one request, but those derived values are not durable
//! facts.

use novarocks_types::naming::normalize_identifier;
use serde::{Deserialize, Serialize};

pub const PERSISTED_QUERY_DEFINITION_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistedQueryDefinition {
    pub format_version: u16,
    pub raw_query_source: String,
    pub dialect: PersistedQueryDialect,
    pub resolution: FrozenQueryResolutionContext,
}

impl PersistedQueryDefinition {
    pub fn new(
        raw_query_source: impl Into<String>,
        dialect: PersistedQueryDialect,
        default_catalog: &str,
        default_database: &str,
    ) -> Result<Self, String> {
        let definition = Self {
            format_version: PERSISTED_QUERY_DEFINITION_FORMAT_VERSION,
            raw_query_source: raw_query_source.into(),
            dialect,
            resolution: FrozenQueryResolutionContext::new(default_catalog, default_database)?,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != PERSISTED_QUERY_DEFINITION_FORMAT_VERSION {
            return Err(format!(
                "unsupported persisted query definition format version `{}`",
                self.format_version
            ));
        }
        if self.raw_query_source.trim().is_empty() {
            return Err("persisted query definition raw source is empty".to_string());
        }
        self.resolution.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FrozenQueryResolutionContext {
    pub default_catalog: String,
    pub default_database: String,
}

impl FrozenQueryResolutionContext {
    pub fn new(default_catalog: &str, default_database: &str) -> Result<Self, String> {
        let context = Self {
            default_catalog: normalize_identifier(default_catalog)?,
            default_database: normalize_identifier(default_database)?,
        };
        context.validate()?;
        Ok(context)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_normalized_identifier("default catalog", &self.default_catalog)?;
        validate_normalized_identifier("default database", &self.default_database)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PersistedQueryDialect {
    #[default]
    #[serde(rename = "trino")]
    Trino,
}

fn validate_normalized_identifier(label: &str, value: &str) -> Result<(), String> {
    let normalized = normalize_identifier(value)
        .map_err(|error| format!("invalid persisted query definition {label}: {error}"))?;
    if normalized != value {
        return Err(format!(
            "persisted query definition {label} must be normalized, got `{value}`"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        FrozenQueryResolutionContext, PERSISTED_QUERY_DEFINITION_FORMAT_VERSION,
        PersistedQueryDefinition, PersistedQueryDialect,
    };

    #[test]
    fn preserves_raw_source_and_freezes_normalized_resolution_context() {
        let raw = "/* hint */\nSELECT \"MiXeD\"  FROM t -- keep\n";
        let definition = PersistedQueryDefinition::new(
            raw,
            PersistedQueryDialect::Trino,
            "  `Catalog_One` ",
            "  Db_One ",
        )
        .expect("definition");

        assert_eq!(definition.raw_query_source, raw);
        assert_eq!(definition.resolution.default_catalog, "catalog_one");
        assert_eq!(definition.resolution.default_database, "db_one");
    }

    #[test]
    fn rejects_empty_source_unknown_version_and_noncanonical_context() {
        assert!(
            PersistedQueryDefinition::new(
                " \n\t ",
                PersistedQueryDialect::Trino,
                "catalog",
                "database",
            )
            .is_err()
        );

        let unknown_version = PersistedQueryDefinition {
            format_version: PERSISTED_QUERY_DEFINITION_FORMAT_VERSION + 1,
            raw_query_source: "SELECT 1".to_string(),
            dialect: PersistedQueryDialect::Trino,
            resolution: FrozenQueryResolutionContext {
                default_catalog: "catalog".to_string(),
                default_database: "database".to_string(),
            },
        };
        assert!(unknown_version.validate().is_err());

        let noncanonical_context = FrozenQueryResolutionContext {
            default_catalog: "Catalog".to_string(),
            default_database: "database".to_string(),
        };
        assert!(noncanonical_context.validate().is_err());
    }

    #[test]
    fn serde_uses_the_stable_dialect_value() {
        let definition = PersistedQueryDefinition::new(
            "SELECT 1",
            PersistedQueryDialect::Trino,
            "catalog",
            "database",
        )
        .expect("definition");

        let encoded = serde_json::to_string(&definition).expect("serialize");
        assert_eq!(
            encoded,
            r#"{"format_version":1,"raw_query_source":"SELECT 1","dialect":"trino","resolution":{"default_catalog":"catalog","default_database":"database"}}"#
        );
        let decoded: PersistedQueryDefinition =
            serde_json::from_str(&encoded).expect("deserialize");
        decoded.validate().expect("validated decoded definition");
        assert_eq!(decoded, definition);
    }

    #[test]
    fn serde_rejects_old_starrocks_definitions_without_relabeling() {
        let source = r#"{"format_version":1,"raw_query_source":"SELECT 1","dialect":"starrocks","resolution":{"default_catalog":"catalog","default_database":"database"}}"#;
        let error = serde_json::from_str::<PersistedQueryDefinition>(source).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown variant `starrocks`, expected `trino`")
        );
    }
}
