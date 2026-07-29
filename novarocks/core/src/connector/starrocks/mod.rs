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
mod lake_meta_storage;
mod object_store_profile;
pub mod scan;
pub(crate) mod schema;
pub mod sink;
pub mod starmgr;
pub mod table;
pub(crate) mod table_schema_service;

#[cfg(feature = "compat")]
pub use lake_meta_storage::resolve_lake_meta_storage;
pub(crate) use object_store_profile::ObjectStoreProfile;
pub(crate) use scan::StarRocksScanSource;
pub(crate) use scan::build_native_object_store_profile_from_properties;
pub use scan::{
    LakeScanSchemaMeta, StarRocksScanConfig, StarRocksScanOp, StarRocksScanRange,
    StarRocksSchemaColumnHint,
};
