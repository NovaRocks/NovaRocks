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
pub mod hdfs;
pub mod jdbc;
pub mod starrocks;

use std::collections::HashMap;
use std::sync::Arc;

use crate::exec::node::scan::ScanNode;

pub use crate::formats::FileFormatConfig;
pub use crate::formats::orc::OrcScanConfig;
pub use crate::formats::parquet::{MinMaxPredicate, ParquetScanConfig};
pub use crate::fs::scan_context::FileScanRange;
pub use hdfs::HdfsScanConfig;
pub use jdbc::JdbcScanConfig;
pub use starrocks::{StarRocksScanConfig, StarRocksScanRange};

#[derive(Clone, Debug)]
pub enum ScanConfig {
    Jdbc(JdbcScanConfig),
    Hdfs(HdfsScanConfig),
    StarRocks(StarRocksScanConfig),
}

pub trait ScanConnector: Send + Sync {
    fn name(&self) -> &'static str;
    fn create_scan_node(&self, cfg: ScanConfig) -> Result<ScanNode, String>;
}

#[derive(Clone)]
pub struct ConnectorRegistry {
    scan_connectors: HashMap<&'static str, Arc<dyn ScanConnector>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            scan_connectors: HashMap::new(),
        }
    }

    pub fn register_scan_connector(&mut self, connector: Arc<dyn ScanConnector>) {
        self.scan_connectors.insert(connector.name(), connector);
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

impl Default for ConnectorRegistry {
    fn default() -> Self {
        let mut reg = ConnectorRegistry::new();
        let jdbc = Arc::new(JdbcConnector { name: "jdbc" });
        let mysql = Arc::new(JdbcConnector { name: "mysql" });
        let hdfs = Arc::new(HdfsConnector { name: "hdfs" });
        let starrocks = Arc::new(StarRocksConnector { name: "starrocks" });
        reg.register_scan_connector(jdbc);
        reg.register_scan_connector(mysql);
        reg.register_scan_connector(hdfs);
        reg.register_scan_connector(starrocks);
        reg
    }
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<_> = self.scan_connectors.keys().copied().collect();
        names.sort();
        f.debug_struct("ConnectorRegistry")
            .field("scan_connectors", &names)
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
            ScanConfig::Hdfs(cfg) => Ok(ScanNode::new(Arc::new(hdfs::HdfsScanOp::new(cfg)))),
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
                starrocks::StarRocksScanOp::new(cfg),
            ))),
            _ => Err(format!(
                "unsupported scan config for connector {}",
                self.name
            )),
        }
    }
}
