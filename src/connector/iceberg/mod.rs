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

pub mod catalog;
pub mod changes;
pub mod commit;
#[cfg_attr(test, allow(dead_code))]
pub(crate) mod compact;
pub(crate) mod data_writer;
pub(crate) mod default_value;
pub mod equality_delete;
pub mod metadata;
pub(crate) mod metadata_read;
pub(crate) mod operation_lifecycle;
pub(crate) mod partition_spec;
pub mod position_delete;
pub(crate) mod read;
pub(crate) mod row_lineage_synth;
pub mod scan_deletes;
pub(crate) mod scan_planner;
pub mod schema;
pub mod sink;
mod state;
pub(crate) mod stats_assembler;
pub(crate) mod stats_loader;
pub(crate) mod theta_sketch;
pub(crate) mod variant_write;

pub use metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanOp,
    IcebergMetadataScanRange, IcebergMetadataTableType,
};
pub(crate) use scan_planner::{
    IcebergConnectorScanPlanner, IcebergScanHandle, IcebergSplit, IcebergTableHandle,
};
pub use schema::{
    IcebergArrowColumn, apply_field_id_recursive, build_full_output_schema,
    build_projected_output_schema,
};
pub use sink::IcebergTableSinkFactory;
pub(crate) use state::{
    cache_iceberg_table_locations, lookup_iceberg_table_location, snapshot_iceberg_table_locations,
};
