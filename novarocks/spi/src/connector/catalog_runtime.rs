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

//! Provider-local catalog runtimes materialized by a backend.
//!
//! `CatalogProperties` is the complete, immutable frontend contribution.  A
//! materializer is selected from the backend's startup-sealed provider set and
//! may combine that contribution with local credentials, clients, and resource
//! limits.  Neither those local facts nor a process-local runtime identity
//! travel over the native query wire.

use std::sync::Arc;

use super::{CatalogHandle, CatalogProperties, CatalogProviderKind, ConnectorError};

/// One exact backend-local catalog materialization.
///
/// The trait intentionally exposes only immutable identity.  Provider-owned
/// execution capabilities remain behind the runtime until their native
/// read/write callers have resolved this exact handle through the query lease.
pub trait CatalogRuntime: Send + Sync {
    fn handle(&self) -> &CatalogHandle;

    fn provider_kind(&self) -> CatalogProviderKind;
}

/// Startup-composed provider materializer for one catalog kind.
///
/// Implementations must reject a mismatched provider kind and must not obtain
/// catalog properties from any source other than the supplied immutable value.
pub trait CatalogRuntimeMaterializer: Send + Sync {
    fn provider_kind(&self) -> CatalogProviderKind;

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError>;
}
