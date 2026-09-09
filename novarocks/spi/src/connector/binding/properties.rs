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

use std::fmt;

use crate::connector::{CatalogHandle, CatalogProperties, ConnectorProviderId};

/// Validated, deterministic, credential-free materialization input.
///
/// `CatalogProperties` owns its canonical sort and rejects secret-like
/// execution-property keys. The wrapper keeps the field private so callers
/// cannot construct an unchecked alternative alongside a role binding.
#[derive(Clone, Eq, PartialEq)]
pub struct NormalizedCatalogProperties {
    properties: CatalogProperties,
}

impl NormalizedCatalogProperties {
    pub fn try_new(properties: CatalogProperties) -> Result<Self, String> {
        if properties.execution_properties().iter().any(|property| {
            let key = property.key().to_ascii_lowercase();
            ["secret", "password", "token", "credential", "access_key"]
                .iter()
                .any(|marker| key.contains(marker))
        }) {
            return Err("normalized catalog properties contain a credential-like key".to_owned());
        }
        Ok(Self { properties })
    }

    pub const fn handle(&self) -> &CatalogHandle {
        self.properties.handle()
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        self.properties.provider_id()
    }

    pub const fn as_catalog_properties(&self) -> &CatalogProperties {
        &self.properties
    }

    pub fn into_catalog_properties(self) -> CatalogProperties {
        self.properties
    }
}

impl fmt::Debug for NormalizedCatalogProperties {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NormalizedCatalogProperties")
            .field("handle", self.properties.handle())
            .field("provider_id", &self.properties.provider_id())
            .field(
                "config_format_version",
                &self.properties.config_format_version(),
            )
            .field(
                "execution_property_count",
                &self.properties.execution_properties().len(),
            )
            .field(
                "credential_binding_count",
                &self.properties.credential_bindings().len(),
            )
            .finish()
    }
}
