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

pub mod fe_v2_meta;
pub(crate) mod fs_access;
pub mod lake;
pub mod lake_meta;
pub mod lake_meta_storage;
mod object_store_profile;
pub mod ports;
pub mod scan;
pub mod schema;
pub mod sink;
pub(crate) mod table_schema_service;

/// StarRocks wire-plan catalog identity used by compatibility adapters.
///
/// This value does not represent a native NovaRocks internal-table catalog.
/// A future native StarRocks integration must bind an external connector
/// instance instead of treating this compatibility identity as native DDL.
pub const STARROCKS_WIRE_INTERNAL_CATALOG_NAME: &str = "default_catalog";

pub(crate) use object_store_profile::ObjectStoreProfile;
pub(crate) use scan::build_native_object_store_profile_from_properties;
pub use scan::plan_compat_starrocks_read_source;
pub(crate) use scan::plan_native_starrocks_read_source;
pub use scan::{
    LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanRange, StarRocksSchemaColumnHint,
};
