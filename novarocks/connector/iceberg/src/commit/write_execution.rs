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

//! Catalog-keyed Iceberg writer construction on the backend.
//!
//! The writers themselves, and the artifact descriptions they return, are
//! [`write_stack::execution`](crate::commit::write_stack::execution). What
//! remains here is the startup-sealed factory the execution role binding still
//! has to publish: the bundle is minted per catalog generation, and the
//! `CatalogWriteExecution` inside it has no caller left, because a writer is
//! now opened through the write session's own handle.

use std::sync::Arc;

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogWriteExecution, CatalogWriteExecutionBundle,
    CatalogWriteExecutionBundleFactory, ConnectorError,
};

use crate::access_binding::IcebergReadBinding;

/// Startup-sealed provider factory for catalog-keyed Iceberg writers.
#[derive(Clone)]
pub struct IcebergCatalogWriteExecutionFactory {
    binding: IcebergReadBinding,
}

impl IcebergCatalogWriteExecutionFactory {
    pub fn new(binding: IcebergReadBinding) -> Self {
        Self { binding }
    }
}

impl CatalogWriteExecutionBundleFactory for IcebergCatalogWriteExecutionFactory {
    fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<CatalogWriteExecutionBundle, ConnectorError> {
        // Binding the catalog is what proves this generation can reach the
        // warehouse it claims, so it still runs even though the bundle it
        // carries opens no writer.
        self.binding.bind_catalog(properties)?;
        Ok(CatalogWriteExecutionBundle::new(Arc::new(
            IcebergRetiredCatalogWriteExecution {
                catalog_handle: properties.handle().clone(),
            },
        )))
    }
}

/// The catalog-keyed writer entry the write session replaced. It is still
/// required to publish a bundle; nothing opens a writer through it.
struct IcebergRetiredCatalogWriteExecution {
    catalog_handle: CatalogHandle,
}

impl CatalogWriteExecution for IcebergRetiredCatalogWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}
