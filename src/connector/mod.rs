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
pub(crate) mod backend;
pub mod hdfs;
pub mod iceberg;
pub mod jdbc;
pub mod schema;
pub mod starrocks;

pub(crate) use backend::{CatalogBackend, MvBackend, TableSink, TableSource};
#[cfg(test)]
pub(crate) use iceberg::catalog::load_table as load_iceberg_table;
pub(crate) use iceberg::catalog::{
    IcebergCatalogRegistry, create_namespace as create_iceberg_namespace,
    namespace_exists as iceberg_namespace_exists,
    register_existing_table as register_existing_iceberg_table,
};
#[cfg(test)]
pub(crate) use iceberg::changes::plan_changes as plan_iceberg_changes;
#[cfg(not(test))]
pub(crate) use iceberg::compact::spawn_optimize_worker as spawn_iceberg_optimize_worker;
pub(crate) use starrocks::managed::ddl::truncate_managed_table;
pub(crate) use starrocks::managed::erase::spawn_erase_worker as spawn_managed_erase_worker;
#[cfg(test)]
pub(crate) use starrocks::managed::model::IcebergTableRef;
pub(crate) use starrocks::managed::txn::{
    insert_into_managed_lake_table, publish_tablets_at_version,
};
pub(crate) use starrocks::managed::{
    ManagedLakeCatalog, ManagedLakeConfig, register_managed_tables_in_catalog, runtime_registered,
};

use std::collections::HashMap;
use std::sync::Arc;

pub use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
use crate::exec::node::scan::ScanNode;

pub use crate::formats::FileFormatConfig;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::ParquetScanConfig;
pub use crate::fs::scan_context::FileScanRange;
pub use hdfs::HdfsScanConfig;
pub use iceberg::IcebergMetadataScanConfig;
pub use jdbc::JdbcScanConfig;
pub use starrocks::{LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanOp, StarRocksScanRange};

#[cfg(test)]
mod backend_test;

#[derive(Clone, Debug)]
pub enum ScanConfig {
    Jdbc(JdbcScanConfig),
    Hdfs(Box<HdfsScanConfig>),
    IcebergMetadata(IcebergMetadataScanConfig),
    StarRocks(Box<StarRocksScanConfig>),
}

pub trait ScanConnector: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String>;
}

#[derive(Clone)]
pub struct ConnectorRegistry {
    scan_connectors: HashMap<&'static str, Arc<dyn ScanConnector>>,
    catalog_backends: HashMap<&'static str, Arc<dyn CatalogBackend>>,
    table_sources: HashMap<&'static str, Arc<dyn TableSource>>,
    table_sinks: HashMap<&'static str, Arc<dyn TableSink>>,
    mv_backends: HashMap<&'static str, Arc<dyn MvBackend>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            scan_connectors: HashMap::new(),
            catalog_backends: HashMap::new(),
            table_sources: HashMap::new(),
            table_sinks: HashMap::new(),
            mv_backends: HashMap::new(),
        }
    }

    pub fn register_scan_connector(&mut self, connector: Arc<dyn ScanConnector>) {
        self.scan_connectors.insert(connector.name(), connector);
    }

    pub(crate) fn register_catalog_backend(&mut self, backend: Arc<dyn CatalogBackend>) {
        self.catalog_backends.insert(backend.name(), backend);
    }

    pub(crate) fn catalog_backend(&self, name: &str) -> Result<Arc<dyn CatalogBackend>, String> {
        self.catalog_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown catalog backend: {name}"))
    }

    pub(crate) fn register_table_source(&mut self, source: Arc<dyn TableSource>) {
        self.table_sources.insert(source.name(), source);
    }

    pub(crate) fn table_source(&self, name: &str) -> Result<Arc<dyn TableSource>, String> {
        self.table_sources
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown table source: {name}"))
    }

    pub(crate) fn register_table_sink(&mut self, sink: Arc<dyn TableSink>) {
        self.table_sinks.insert(sink.name(), sink);
    }

    pub(crate) fn table_sink(&self, name: &str) -> Result<Arc<dyn TableSink>, String> {
        self.table_sinks
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown table sink: {name}"))
    }

    pub(crate) fn register_mv_backend(&mut self, backend: Arc<dyn MvBackend>) {
        self.mv_backends.insert(backend.name(), backend);
    }

    pub(crate) fn mv_backend(&self, name: &str) -> Result<Arc<dyn MvBackend>, String> {
        self.mv_backends
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown MV backend: {name}"))
    }

    pub(crate) fn mv_backends(&self) -> Vec<Arc<dyn MvBackend>> {
        let mut entries: Vec<_> = self.mv_backends.iter().collect();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries
            .into_iter()
            .map(|(_, backend)| Arc::clone(backend))
            .collect()
    }

    pub fn create_scan_node(
        &self,
        connector_name: &str,
        cfg: ScanConfig,
    ) -> Result<ScanNode, String> {
        let Some(connector) = self.scan_connectors.get(connector_name) else {
            return Err(format!("unknown scan connector: {connector_name}"));
        };
        connector.create_scan_node(cfg)
    }
}

pub(crate) fn register_standalone_backends(state: &Arc<crate::engine::StandaloneState>) {
    let mut connectors = state
        .connectors
        .write()
        .expect("standalone connector registry write lock");
    let iceberg_catalogs = Arc::clone(&state.iceberg_catalogs);
    connectors.register_catalog_backend(Arc::new(iceberg::catalog::IcebergCatalogBackend::new(
        Arc::clone(&iceberg_catalogs),
    )));
    connectors.register_table_source(Arc::new(iceberg::catalog::IcebergTableSource::new(
        Arc::clone(&iceberg_catalogs),
    )));
    connectors.register_table_sink(Arc::new(iceberg::catalog::IcebergTableSink::new(
        iceberg_catalogs,
    )));

    connectors
        .register_catalog_backend(Arc::new(starrocks::managed::ManagedLakeBackend::new(state)));
    connectors.register_table_source(Arc::new(starrocks::managed::ManagedLakeTableSource::new(
        state,
    )));
    connectors.register_table_sink(Arc::new(starrocks::managed::ManagedLakeTableSink::new(
        state,
    )));
    connectors.register_mv_backend(Arc::new(starrocks::managed::ManagedLakeMvBackend::new(
        state,
    )));
    connectors.register_mv_backend(Arc::new(
        crate::engine::mv::iceberg_backend::IcebergMvBackend::new(state),
    ));
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        let mut reg = ConnectorRegistry::new();
        let jdbc = Arc::new(JdbcConnector { name: "jdbc" });
        let mysql = Arc::new(JdbcConnector { name: "mysql" });
        let hdfs = Arc::new(HdfsConnector { name: "hdfs" });
        let iceberg = Arc::new(IcebergConnector { name: "iceberg" });
        let starrocks = Arc::new(StarRocksConnector { name: "starrocks" });
        reg.register_scan_connector(jdbc);
        reg.register_scan_connector(mysql);
        reg.register_scan_connector(hdfs);
        reg.register_scan_connector(iceberg);
        reg.register_scan_connector(starrocks);
        reg
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut scan_connectors: Vec<_> = self.scan_connectors.keys().copied().collect();
        scan_connectors.sort();
        let mut catalog_backends: Vec<_> = self.catalog_backends.keys().copied().collect();
        catalog_backends.sort();
        let mut table_sources: Vec<_> = self.table_sources.keys().copied().collect();
        table_sources.sort();
        let mut table_sinks: Vec<_> = self.table_sinks.keys().copied().collect();
        table_sinks.sort();
        let mut mv_backends: Vec<_> = self.mv_backends.keys().copied().collect();
        mv_backends.sort();
        f.debug_struct("ConnectorRegistry")
            .field("scan_connectors", &scan_connectors)
            .field("catalog_backends", &catalog_backends)
            .field("table_sources", &table_sources)
            .field("table_sinks", &table_sinks)
            .field("mv_backends", &mv_backends)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct JdbcConnector {
    name: &'static str,
}

impl ScanConnector for JdbcConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String> {
        match cfg {
            ScanConfig::Jdbc(cfg) => Ok(ScanNode::new(Arc::new(jdbc::JdbcScanOp::new(cfg)))),
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct HdfsConnector {
    name: &'static str,
}

impl ScanConnector for HdfsConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String> {
        match cfg {
            ScanConfig::Hdfs(cfg) => Ok(ScanNode::new(Arc::new(hdfs::HdfsScanOp::new(*cfg)))),
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct IcebergConnector {
    name: &'static str,
}

impl ScanConnector for IcebergConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String> {
        match cfg {
            ScanConfig::IcebergMetadata(cfg) => Ok(ScanNode::new(Arc::new(
                iceberg::IcebergMetadataScanOp::new(cfg)?,
            ))),
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct StarRocksConnector {
    name: &'static str,
}

impl ScanConnector for StarRocksConnector {
    fn name(&self) -> &'static str {
        self.name
    }

    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String> {
        match cfg {
            ScanConfig::StarRocks(cfg) => Ok(ScanNode::new(Arc::new(
                starrocks::StarRocksScanOp::new(*cfg),
            ))),
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}
