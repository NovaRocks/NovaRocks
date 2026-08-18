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
#[cfg(test)]
mod tests;

pub mod catalog_application;
pub mod common;
pub mod connector;
pub mod maintenance;
pub mod mv;
pub mod protocol;
pub mod query_lifecycle;
pub mod runtime;
pub mod server;
pub mod service;
pub use novarocks_version as version;
// StarRocks-BE-like folder layout, with `novarocks_*` convenience aliases.
pub use common::logging as novarocks_logging;
pub use connector as novarocks_connectors;

pub use common::types::FetchResult;

/// The MV startup restore steps an application owner drives.
///
/// A deliberately narrow re-export: the frontend needs exactly these four items
/// to own startup orchestration, and widening the Core MV module wholesale would expose
/// far more than that. The lake-reading code stays here because a production SQL
/// procedure also calls it; what moves is who decides when it runs.
pub mod mv_startup {
    pub use crate::mv::iceberg_refresh::{MvTargetRestoreContext, restore_iceberg_mv_targets};
    pub use crate::mv::lake_rebuild::{LakeRebuildContext, rebuild_imv_cache_from_lake};
}
