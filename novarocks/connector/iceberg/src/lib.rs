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

//! Iceberg provider dependency boundary.
//!
//! Provider-owned Iceberg facts and table-format value conversions.
//!
//! The external Iceberg, catalog, object-store, and provider-specific value
//! modules live here. Consumer crates depend on these typed facts at their
//! boundaries; the provider crate does not depend on aggregate Core, SQL, or
//! execution modules.

/// Stable provider identity used by server composition and SPI declarations.
pub const PROVIDER_ID: &str = "iceberg";

pub mod access_binding;
pub(crate) mod catalog;
pub mod catalog_cache;
pub mod catalog_config;
pub mod catalog_control;
pub mod catalog_runtime;
pub mod change_planning;
pub mod commit;
pub mod connector_factory;
pub mod default_value;
pub mod definition;
pub mod delete_file;
pub mod delta;
pub mod distributed_rewrite;
pub mod file_pruning;
pub mod file_reader;
pub mod fs_io;
pub mod hadoop_catalog;
pub mod loaded_table;
pub mod manifest;
pub mod metadata;
pub mod metadata_batch_reader;
pub mod metadata_context;
pub mod metadata_factory;
pub mod metadata_read;
pub mod planning_facts;
pub mod position_delete;
pub mod position_delete_descriptor;
pub mod provider_binding;
pub mod provider_types;
pub mod read_snapshot;
pub mod reconcile_payload;
pub mod ref_snapshot;
pub mod resources;
/// Iceberg virtual-column and row-lineage facts. These names and reserved
/// field IDs are defined by the table format, not by the execution engine.
pub mod row_lineage_synth;
pub mod scan_model;
pub mod schema_facts;
pub mod schema_mapping;
pub mod statistics_ancestry;
pub mod statistics_basis;
pub mod statistics_codec;
pub mod stats_assembler;
pub mod stats_loader;
pub mod storage_inspector;
pub mod table_definition;
pub mod typed_boundary;
pub mod typed_provider_factory;
pub mod typed_read;
pub mod wire;
pub mod write_codec;
pub mod write_descriptor;

pub use definition::iceberg_contract_definition;
pub use file_reader::execution_installer::IcebergCatalogRuntimeMaterializer;
pub use role_binding::{IcebergControlRoleBindingFactory, IcebergExecutionRoleBindingFactory};

pub mod iceberg {
    pub use ::iceberg::*;
}

pub mod iceberg_catalog_rest {
    pub use ::iceberg_catalog_rest::*;
}

pub mod iceberg_catalog_hms {
    pub use ::iceberg_catalog_hms::*;
}

pub mod opendal {
    pub use ::opendal::*;
}
pub mod read_model;
pub mod role_binding;

pub use novarocks_fs;
pub use novarocks_spi;
