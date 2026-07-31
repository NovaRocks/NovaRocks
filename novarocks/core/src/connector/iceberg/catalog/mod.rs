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

//! Iceberg catalog registry, table loading, hadoop/S3 storage backends,
//! and `ADD FILES` support.

pub(crate) mod add_files;
pub(crate) mod backend;
pub(crate) mod hadoop_catalog;
pub(crate) mod registry;
pub(crate) mod schema_update;
pub(crate) mod views;

// Re-export the same surface the previous `standalone::iceberg::*` module
// offered, so callers only need to update the module prefix, not each
// imported symbol.
pub(crate) use backend::{
    build_iceberg_table_def_for_delta_scan, build_iceberg_table_def_with_files,
    hidden_internal_column_names_from_metadata, iceberg_table_stats_provider, row_lineage_enabled,
};
pub(crate) use registry::{
    IcebergCatalogEntry, IcebergCatalogRegistry, IcebergLoadedTable, create_namespace,
    list_namespaces, load_table, namespace_exists, register_existing_table,
};
pub(crate) use schema_update::{alter_table_properties, alter_table_schema};
