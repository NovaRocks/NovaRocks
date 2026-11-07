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

//! Provider-owned control-generation catalog state.

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;

use novarocks_types::naming::normalize_identifier;

use crate::catalog_cache::IcebergDataFilesCache;
use crate::catalog_config::IcebergCatalogConfiguration;
use crate::loaded_table::IcebergPhysicalTableCache;
use crate::manifest::DataFileWithStats;

#[path = "catalog_control/add_files.rs"]
pub(crate) mod add_files;
#[path = "catalog_control/catalog_mutation.rs"]
pub mod catalog_mutation;
#[path = "catalog_control/cleanup_candidates.rs"]
mod cleanup_candidates;
#[path = "catalog_control/cleanup_maintenance.rs"]
pub mod cleanup_maintenance;
#[path = "catalog_control/data_mutation.rs"]
pub mod data_mutation;
pub mod drop_cleanup;
#[path = "catalog_control/metadata_maintenance.rs"]
pub(crate) mod metadata_maintenance;
#[path = "catalog_control/owned_ref_cleanup.rs"]
mod owned_ref_cleanup;
#[path = "catalog_control/staged_create.rs"]
pub mod staged_create;
#[path = "catalog_control/statistics.rs"]
pub mod statistics;
#[path = "catalog_control/type_mapping.rs"]
pub(crate) mod type_mapping;
#[path = "catalog_control/unanchored_ctas_cleanup.rs"]
pub(crate) mod unanchored_ctas_cleanup;
#[path = "catalog_control/views.rs"]
pub(crate) mod views;

/// State that is private to one Iceberg control generation.
///
/// It carries provider configuration plus physical catalog caches. SQL table
/// projections and application services are intentionally excluded.
#[derive(Clone)]
pub struct IcebergCatalogControlState {
    configuration: IcebergCatalogConfiguration,
    physical_table_cache: IcebergPhysicalTableCache,
    data_files_cache: IcebergDataFilesCache,
}

/// Provider-owned registry for one control generation's catalog bindings.
///
/// The registry is deliberately limited to Iceberg configuration and physical
/// cache state.  SQL table projections, write services, and application
/// routing stay outside this value, so no Core-owned execution state is
/// captured by a provider catalog binding.
#[derive(Default)]
pub struct IcebergCatalogControlRegistry {
    entries: HashMap<String, IcebergCatalogControlState>,
}

impl IcebergCatalogControlRegistry {
    /// Register a catalog exactly once. The initializer is evaluated only for
    /// a new normalized name, preserving idempotent CREATE semantics without
    /// re-parsing credentials or allocating another provider cache.
    pub fn ensure_with<F>(&mut self, name: &str, initializer: F) -> Result<(), String>
    where
        F: FnOnce() -> Result<IcebergCatalogControlState, String>,
    {
        let key = normalize_identifier(name)?;
        if self.entries.contains_key(&key) {
            return Ok(());
        }
        self.entries.insert(key, initializer()?);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<IcebergCatalogControlState, String> {
        let key = normalize_identifier(name)?;
        self.entries
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("unknown catalog: {name}"))
    }

    pub fn contains(&self, name: &str) -> Result<bool, String> {
        Ok(self.entries.contains_key(&normalize_identifier(name)?))
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self.entries.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn remove(&mut self, name: &str) -> Result<(), String> {
        let key = normalize_identifier(name)?;
        self.entries
            .remove(&key)
            .map(|_| ())
            .ok_or_else(|| format!("unknown catalog: {name}"))
    }
}

impl IcebergCatalogControlState {
    pub fn new(configuration: IcebergCatalogConfiguration) -> Self {
        Self {
            configuration,
            physical_table_cache: IcebergPhysicalTableCache::default(),
            data_files_cache: IcebergDataFilesCache::default(),
        }
    }

    pub fn configuration(&self) -> &IcebergCatalogConfiguration {
        &self.configuration
    }

    pub fn physical_table_cache(&self) -> &IcebergPhysicalTableCache {
        &self.physical_table_cache
    }

    pub fn data_files_cache(&self) -> &IcebergDataFilesCache {
        &self.data_files_cache
    }

    pub fn invalidate_table(&self, namespace_name: &str, table_name: &str) {
        self.physical_table_cache
            .invalidate(namespace_name, table_name);
        self.data_files_cache
            .invalidate_table(namespace_name, table_name);
    }

    pub fn properties(&self) -> &[(String, String)] {
        &self.configuration.properties
    }

    pub fn is_s3(&self) -> bool {
        novarocks_fs::is_object_store_location_parse_only(&self.configuration.warehouse_uri)
            .unwrap_or(false)
    }

    pub fn cloud_properties_map(&self) -> BTreeMap<String, String> {
        self.configuration
            .properties
            .iter()
            .filter(|(key, _)| novarocks_fs::AWS_S3_CATALOG_PROPERTY_KEYS.contains(&key.as_str()))
            .cloned()
            .collect()
    }

    pub fn invalidate_table_cache(&self, namespace_name: &str, table_name: &str) {
        if let (Ok(namespace), Ok(table)) = (
            normalize_identifier(namespace_name),
            normalize_identifier(table_name),
        ) {
            self.invalidate_table(&namespace, &table);
        }
    }

    pub fn cached_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
    ) -> Result<Option<Vec<DataFileWithStats>>, String> {
        self.data_files_cache
            .get(namespace_name, table_name, snapshot_id)
    }

    pub fn cache_data_files(
        &self,
        namespace_name: &str,
        table_name: &str,
        snapshot_id: Option<i64>,
        data_files: Vec<DataFileWithStats>,
    ) -> Result<(), String> {
        self.data_files_cache
            .insert(namespace_name, table_name, snapshot_id, data_files)
    }
}

impl Deref for IcebergCatalogControlState {
    type Target = IcebergCatalogConfiguration;

    fn deref(&self) -> &Self::Target {
        self.configuration()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn provider_control_state_owns_catalog_properties_and_cache_scope() {
        let state = IcebergCatalogControlState::new(IcebergCatalogConfiguration {
            kind: crate::catalog_config::IcebergCatalogKind::Hadoop,
            warehouse_uri: "file:///tmp/warehouse".to_string(),
            rest_uri: None,
            hms_uris: None,
            properties: vec![
                ("type".to_string(), "iceberg".to_string()),
                (
                    "aws.s3.endpoint".to_string(),
                    "http://minio:9000".to_string(),
                ),
                ("unrelated".to_string(), "ignored".to_string()),
            ],
            object_store_config: None,
            warehouse_path: PathBuf::from("/tmp/warehouse"),
        });

        assert!(!state.is_s3());
        assert_eq!(state.properties().len(), 3);
        assert_eq!(
            state.cloud_properties_map().get("aws.s3.endpoint"),
            Some(&"http://minio:9000".to_string())
        );
        assert!(!state.cloud_properties_map().contains_key("unrelated"));
    }

    #[test]
    fn control_registry_normalizes_and_initializes_only_once() {
        let mut registry = IcebergCatalogControlRegistry::default();
        let configuration = || {
            IcebergCatalogControlState::new(IcebergCatalogConfiguration {
                kind: crate::catalog_config::IcebergCatalogKind::Hadoop,
                warehouse_uri: "file:///tmp/warehouse".to_string(),
                rest_uri: None,
                hms_uris: None,
                properties: Vec::new(),
                object_store_config: None,
                warehouse_path: PathBuf::from("/tmp/warehouse"),
            })
        };
        registry
            .ensure_with("SALES", || Ok(configuration()))
            .expect("initial catalog");
        registry
            .ensure_with("sales", || Err("must not initialize twice".to_string()))
            .expect("idempotent catalog");
        assert!(registry.contains("sales").expect("contains"));
        assert_eq!(registry.names(), vec!["sales"]);
        registry.remove("sales").expect("remove");
        assert!(!registry.contains("sales").expect("absent"));
    }
}
