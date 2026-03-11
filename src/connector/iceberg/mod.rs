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

mod jvm;
pub mod metadata;
pub mod schema;
pub mod sink;
mod state;

use crate::novarocks_config::config as novarocks_app_config;

pub use metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanOp,
    IcebergMetadataScanRange, IcebergMetadataTableType,
};
pub use schema::{
    IcebergArrowColumn, apply_field_id_recursive, build_full_output_schema,
    build_projected_output_schema,
};
pub use sink::IcebergTableSinkFactory;
pub(crate) use state::{
    cache_iceberg_table_locations, lookup_iceberg_table_location, snapshot_iceberg_table_locations,
};

pub(crate) fn ensure_embedded_jvm_enabled(context: &str) -> Result<(), String> {
    if !cfg!(feature = "embedded-jvm") {
        return Err(format!(
            "{context} requires embedded JVM support for Iceberg metadata scans, but this NovaRocks binary was built without the `embedded-jvm` feature; rebuild with `--features embedded-jvm`"
        ));
    }
    let cfg = novarocks_app_config().map_err(|e| {
        format!("load NovaRocks config failed while checking Iceberg JVM gate: {e}")
    })?;
    if cfg.iceberg.enable_embedded_jvm {
        return Ok(());
    }
    Err(format!(
        "{context} requires embedded JVM support for Iceberg metadata scans, but [iceberg].enable_embedded_jvm is disabled"
    ))
}
