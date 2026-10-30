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
use std::sync::Arc;

use crate::access_binding::IcebergReadBinding;

#[derive(Clone)]
pub struct IcebergCatalogRuntime {
    handle: tokio::runtime::Handle,
    iceberg_runtime: crate::iceberg::Runtime,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
}

impl IcebergCatalogRuntime {
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        let iceberg_runtime = {
            let _guard = handle.enter();
            crate::iceberg::Runtime::try_current()
                .expect("explicit Iceberg Tokio runtime handle must establish a runtime context")
        };
        Self {
            handle,
            iceberg_runtime,
            activity: None,
        }
    }

    pub(crate) fn activity(&self) -> Option<&Arc<crate::activity::GenerationActivity>> {
        self.activity.as_ref()
    }

    /// The SDK runtime captured once from this role-local runtime owner.
    pub fn iceberg_runtime(&self) -> &crate::iceberg::Runtime {
        &self.iceberg_runtime
    }

    /// Runs one provider future on the explicitly injected runtime without
    /// probing the caller's Tokio context. A dedicated joining thread keeps a
    /// synchronous SPI factory safe when it is invoked from a runtime worker.
    pub fn block_on<F>(&self, future: F) -> Result<F::Output, String>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let handle = self.handle.clone();
        // Retained through join, including panic and statement cancellation.
        let _bridge = self
            .activity
            .as_ref()
            .map(|owner| owner.enter(novarocks_spi::connector::CatalogActivityPhase::Bridge));
        // The named synchronous P05 consumers borrow their original session;
        // a scoped join does not require a cloneable commit authority. New
        // asynchronous consumers never enter this transitional bridge.
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("iceberg-catalog-runtime".to_string())
                .spawn_scoped(scope, move || handle.block_on(future))
                .map_err(|error| {
                    if let Some(activity) = &self.activity {
                        activity.bridge_spawn_failed();
                    }
                    format!("spawn Iceberg catalog runtime bridge: {error}")
                })?
                .join()
                .map_err(|_| "Iceberg catalog runtime bridge panicked".to_string())
        })
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
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_observation(
        mut self,
        slot: crate::test_observation::IcebergObservationSlot,
    ) -> Self {
        self.planning_binding = self.planning_binding.with_test_observation(slot);
        self
    }

    pub fn new(
        planning_binding: IcebergReadBinding,
        catalog_runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            planning_binding,
            catalog_runtime: IcebergCatalogRuntime::new(catalog_runtime),
        }
    }

    pub(crate) fn with_activity(
        mut self,
        activity: Arc<crate::activity::GenerationActivity>,
    ) -> Self {
        self.catalog_runtime.activity = Some(activity);
        self
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
    read_binding: IcebergReadBinding,
    write_binding: IcebergReadBinding,
    catalog_runtime: IcebergCatalogRuntime,
}

impl IcebergExecutionResources {
    #[cfg(novarocks_dev_controls)]
    pub fn test_storage_authority_registry(&self) -> Arc<novarocks_fs::StorageAuthorityRegistry> {
        self.read_binding.test_storage_authority_registry().clone()
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_observation(
        mut self,
        slot: crate::test_observation::IcebergObservationSlot,
    ) -> Self {
        self.read_binding = self.read_binding.with_test_observation(slot.clone());
        self.write_binding = self.write_binding.with_test_observation(slot);
        self
    }

    pub fn new(
        read_binding: IcebergReadBinding,
        write_binding: IcebergReadBinding,
        catalog_runtime: IcebergCatalogRuntime,
    ) -> Self {
        Self {
            read_binding,
            write_binding,
            catalog_runtime,
        }
    }

    pub fn read_binding(&self) -> &IcebergReadBinding {
        &self.read_binding
    }

    pub fn write_binding(&self) -> &IcebergReadBinding {
        &self.write_binding
    }

    /// The bridge an execution node drives its own catalog calls on.
    ///
    /// Until CAD-1 an execution node had none, deliberately: it never spoke to
    /// a catalog. D1 changes that — it now obtains its own data credentials —
    /// and D12 records the consequence, that executor-to-catalog reachability
    /// becomes a deployment requirement rather than an implementation detail.
    pub fn catalog_runtime(&self) -> &IcebergCatalogRuntime {
        &self.catalog_runtime
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
        let execution = IcebergExecutionResources::new(
            binding.clone(),
            binding.clone(),
            IcebergCatalogRuntime::new(runtime.handle().clone()),
        );

        assert!(format!("{:?}", control.planning_binding()).contains("IcebergReadBinding"));
        assert!(format!("{:?}", execution.read_binding()).contains("IcebergReadBinding"));
        assert!(format!("{:?}", execution.write_binding()).contains("IcebergReadBinding"));
        assert_eq!(execution.catalog_runtime().block_on(async { 9_u8 }), Ok(9));
        assert_eq!(control.catalog_runtime().block_on(async { 7_u8 }), Ok(7));
    }

    #[tokio::test]
    async fn catalog_runtime_bridges_from_a_runtime_worker_without_context_probe() {
        let runtime = IcebergCatalogRuntime::new(tokio::runtime::Handle::current());
        assert_eq!(runtime.block_on(async { 11_u8 }), Ok(11));
    }

    #[test]
    fn injected_sdk_runtime_and_table_build_without_an_ambient_context() {
        // The explicit Tokio owner outlives both the SDK handle and the table.
        // This is the lowest-level protection for role-local Runtime capture;
        // the expected result comes from the injected handle, not caller TLS.
        let owner = tokio::runtime::Runtime::new().expect("runtime owner");
        assert!(tokio::runtime::Handle::try_current().is_err());
        let runtime = IcebergCatalogRuntime::new(owner.handle().clone());
        std::thread::scope(|threads| {
            threads
                .spawn(|| {
                    use crate::iceberg::spec::{
                        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema,
                        SortOrder, TableMetadataBuilder, Type,
                    };
                    assert!(tokio::runtime::Handle::try_current().is_err());
                    let sdk = runtime.iceberg_runtime().clone();
                    let metadata = TableMetadataBuilder::new(
                        Schema::builder()
                            .with_fields(vec![Arc::new(NestedField::required(
                                1,
                                "id",
                                Type::Primitive(PrimitiveType::Long),
                            ))])
                            .build()
                            .expect("schema"),
                        PartitionSpec::unpartition_spec(),
                        SortOrder::unsorted_order(),
                        "memory://warehouse/db/t".to_string(),
                        FormatVersion::V2,
                        std::collections::HashMap::new(),
                    )
                    .expect("metadata builder")
                    .build()
                    .expect("metadata")
                    .metadata;
                    let table = crate::iceberg::table::Table::builder()
                        .runtime(sdk.clone())
                        .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
                        .metadata(metadata)
                        .file_io(crate::iceberg::io::FileIO::new_with_memory())
                        .build()
                        .expect("explicit runtime table");
                    assert_eq!(table.identifier().name(), "t");
                    let task = sdk.io().spawn(async { 11_u8 });
                    assert_eq!(runtime.block_on(task).unwrap().unwrap(), 11);
                    assert!(tokio::runtime::Handle::try_current().is_err());
                })
                .join()
                .expect("runtime-independent thread");
        });
    }
}
