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
#[cfg(all(feature = "mem-1-m07-hms-listing-observe", not(novarocks_dev_controls)))]
compile_error!("mem-1-m07-hms-listing-observe requires test-controls");

/// Capabilities compiled by this owner; activation still requires a role session.
pub const fn compiled_test_capabilities() -> (&'static str, &'static [&'static str]) {
    (
        env!("CARGO_PKG_NAME"),
        &[
            #[cfg(novarocks_dev_controls)]
            "test-controls",
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            "mem-1-m07-hms-listing-observe",
        ],
    )
}

pub const PROVIDER_ID: &str = "iceberg";

pub mod access_binding;
mod activity;
mod authority_source;
pub(crate) mod catalog;
pub mod catalog_cache;
pub mod catalog_config;
pub mod catalog_control;
mod catalog_metadata_root;
pub use catalog_control::metadata_maintenance::metadata_maintenance_io_geometry;
pub use commit::write_stack::control::write_control_io_geometry;
pub mod catalog_runtime;
pub mod change_planning;
pub mod commit;
pub mod connector_factory;
pub mod default_value;
pub mod definition;
pub mod delete_file;
pub mod delete_semantics;
pub mod distributed_rewrite;
pub mod document_storage;
mod execution_authority;
pub mod file_pruning;
pub mod file_reader;
pub mod fs_io;
pub mod hadoop_catalog;
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub mod hms_listing_probe;
pub mod loaded_table;
pub mod manifest;
pub mod metadata;
pub mod metadata_batch_reader;
pub mod metadata_context;
pub mod metadata_factory;
pub mod metadata_read;
mod partition_row;
pub mod planning_facts;
pub mod position_delete;
pub mod position_delete_descriptor;
pub mod provider_binding;
pub mod provider_types;
pub mod read_snapshot;
pub mod reconcile_payload;
pub mod ref_snapshot;
pub mod resources;
// Wired into the REST owner incrementally across IRU-13a; the allowance is
// removed once every part has a production caller.
#[allow(dead_code)]
pub(crate) mod rest_protocol;
/// Iceberg virtual-column and row-lineage facts. These names and reserved
/// field IDs are defined by the table format, not by the execution engine.
pub mod row_lineage_synth;
pub(crate) mod scalar_integer_domain;
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

#[cfg(test)]
pub(crate) mod iceberg_catalog_rest {
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

mod contract_revision;

// Compile the provider's actual projection source in workspace C0; vendor
// packages cannot run their own dev-dependency tests as workspace dependencies.
#[cfg(test)]
#[path = "../../../../vendor/iceberg-catalog-hms-0.11.0/src/table_projection.rs"]
mod hms_table_projection_contract_tests;

#[cfg(novarocks_dev_controls)]
pub mod test_observation;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");

/// Pure geometry of the original FE typed read factory.
pub use typed_boundary::read_io_geometry;
