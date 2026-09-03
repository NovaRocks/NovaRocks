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

//! Explicit, role-local resources for Iceberg provider generations.
//!
//! Construction belongs to the server composition root.  These values never
//! discover a runtime or credentials from process-global state.

use std::future::Future;

use crate::access_binding::IcebergReadBinding;

#[derive(Clone)]
pub struct IcebergCatalogRuntime {
    handle: tokio::runtime::Handle,
}

impl IcebergCatalogRuntime {
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }

    /// Runs one provider future on the explicitly injected runtime without
    /// probing the caller's Tokio context. A dedicated joining thread keeps a
    /// synchronous SPI factory safe when it is invoked from a runtime worker.
    pub fn block_on<F>(&self, future: F) -> Result<F::Output, String>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.handle.clone();
        std::thread::Builder::new()
            .name("iceberg-catalog-runtime".to_string())
            .spawn(move || handle.block_on(future))
            .map_err(|error| format!("spawn Iceberg catalog runtime bridge: {error}"))?
            .join()
            .map_err(|_| "Iceberg catalog runtime bridge panicked".to_string())
    }
}

impl std::fmt::Debug for IcebergCatalogRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("IcebergCatalogRuntime(<explicit tokio handle>)")
    }
}

#[derive(Clone)]
pub struct IcebergMetadataResources {
    planning_binding: IcebergReadBinding,
    catalog_runtime: IcebergCatalogRuntime,
}

impl IcebergMetadataResources {
    pub fn new(
        planning_binding: IcebergReadBinding,
        catalog_runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            planning_binding,
            catalog_runtime: IcebergCatalogRuntime::new(catalog_runtime),
        }
    }

    pub fn planning_binding(&self) -> &IcebergReadBinding {
        &self.planning_binding
    }

    /// Replaces the unbound composition template with the exact catalog
    /// binding selected during FE admission. The credential resolver remains
    /// role-local; no resolved secret is copied into catalog state.
    pub fn with_planning_binding(mut self, planning_binding: IcebergReadBinding) -> Self {
        self.planning_binding = planning_binding;
        self
    }

    pub fn catalog_runtime(&self) -> &IcebergCatalogRuntime {
        &self.catalog_runtime
    }
}

impl std::fmt::Debug for IcebergMetadataResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergMetadataResources")
            .field("planning_binding", &self.planning_binding)
            .field("catalog_runtime", &self.catalog_runtime)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct IcebergExecutionResources {
    binding: IcebergReadBinding,
}

impl IcebergExecutionResources {
    pub fn new(binding: IcebergReadBinding) -> Self {
        Self { binding }
    }

    pub fn binding(&self) -> &IcebergReadBinding {
        &self.binding
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    #[test]
    fn control_and_execution_resources_retain_the_injected_bindings() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let control = IcebergMetadataResources::new(binding.clone(), runtime.handle().clone());
        let execution = IcebergExecutionResources::new(binding.clone());

        assert!(format!("{:?}", control.planning_binding()).contains("IcebergReadBinding"));
        assert!(format!("{:?}", execution.binding()).contains("IcebergReadBinding"));
        assert_eq!(control.catalog_runtime().block_on(async { 7_u8 }), Ok(7));
    }

    #[tokio::test]
    async fn catalog_runtime_bridges_from_a_runtime_worker_without_context_probe() {
        let runtime = IcebergCatalogRuntime::new(tokio::runtime::Handle::current());
        assert_eq!(runtime.block_on(async { 11_u8 }), Ok(11));
    }
}
