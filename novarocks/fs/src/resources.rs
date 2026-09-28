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

//! Explicit process-local resources used by connector filesystem bindings.
//!
//! A composition root supplies all asynchronous and credential-bearing state.
//! This crate never discovers a Tokio runtime or creates a process-global
//! fallback on behalf of a connector.

use std::sync::Arc;

use crate::{
    FileIoRuntime, FileTaskSpawner, FsAccessResolver, ObjectStoreProviderPool, RefreshExecutor,
    RefreshPolicy, StorageAuthorityRegistry,
};

/// Bridges the registry's refresh executor onto the composed task spawner, so a
/// refresh runs wherever that spawner puts detached blocking work rather than on
/// the thread that asked for material.
struct SpawnerRefreshExecutor {
    spawner: Arc<dyn FileTaskSpawner>,
}

impl RefreshExecutor for SpawnerRefreshExecutor {
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        self.spawner.spawn_detached_blocking(job);
    }
}

/// Filesystem resources bound by a connector instance or execution binding.
///
/// Endpoint configuration and secret material are intentionally absent. Each
/// acquire operation supplies those short-lived values explicitly while this
/// resource owns only shared, bounded, process-lived state: the object-store
/// provider pool and the storage authority registry.
///
/// Those two belong together. The pool key now names an authority rather than
/// a query-scoped lease, so a resident operator signs with whatever authority
/// it captured at construction. If each resolution minted its own authority,
/// material installed by a later resolution would never reach that operator and
/// it would stop working the moment its first material expired. The registry is
/// what makes the two agree, which is why it is composed alongside the pool
/// rather than left to each caller (CAD-1 D0 and D10 together).
#[derive(Clone)]
pub struct FsAccessResources {
    object_store_provider_pool: Arc<ObjectStoreProviderPool>,
    storage_authority_registry: Arc<StorageAuthorityRegistry>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for FsAccessResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsAccessResources")
            .field(
                "object_store_provider_pool",
                &self.object_store_provider_pool,
            )
            .field(
                "storage_authority_registry",
                &self.storage_authority_registry,
            )
            .field("access_resolver", &self.access_resolver)
            .finish_non_exhaustive()
    }
}

impl FsAccessResources {
    /// Constructs a binding from composition-owned resources.
    ///
    /// All services are mandatory arguments so connectors cannot silently
    /// discover a current runtime, construct a fallback runtime, or use a
    /// process-global filesystem service.
    pub fn new(
        object_store_provider_pool: Arc<ObjectStoreProviderPool>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self::new_with_refresh_spawner(
            object_store_provider_pool,
            access_resolver,
            file_runtime,
            Arc::clone(&file_task_spawner),
            file_task_spawner,
        )
    }

    /// Keep credential acquisition on its composed owner when scan I/O uses
    /// a dedicated runtime. The scan spawner remains available to file reads;
    /// the refresh spawner is private to the storage-authority registry.
    pub fn new_with_refresh_spawner(
        object_store_provider_pool: Arc<ObjectStoreProviderPool>,
        access_resolver: FsAccessResolver,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
        refresh_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        // The registry is built here rather than passed in because it must be
        // exactly as shared as the pool beside it. Its refreshes run on the
        // explicitly composed owner, which may differ from scan I/O. Letting
        // callers supply the registry itself would allow two authorities with
        // one identity, which the pool key cannot survive.
        let storage_authority_registry = Arc::new(StorageAuthorityRegistry::with_default_options(
            Arc::new(SpawnerRefreshExecutor {
                spawner: refresh_spawner,
            }),
            RefreshPolicy::default(),
        ));
        Self {
            object_store_provider_pool,
            storage_authority_registry,
            access_resolver,
            file_runtime,
            file_task_spawner,
        }
    }

    /// Derive a file I/O view of the same process-local storage owner.
    ///
    /// Reads may use a dedicated I/O runtime while writes use the role runtime.
    /// Both views retain the same provider pool, authority registry and refresh
    /// executor, so changing I/O runtimes does not create another authority.
    pub fn with_file_io(
        &self,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self {
            object_store_provider_pool: Arc::clone(&self.object_store_provider_pool),
            storage_authority_registry: Arc::clone(&self.storage_authority_registry),
            access_resolver: self.access_resolver,
            file_runtime,
            file_task_spawner,
        }
    }

    pub fn object_store_provider_pool(&self) -> &Arc<ObjectStoreProviderPool> {
        &self.object_store_provider_pool
    }

    /// The process-lived home of storage authorities. A binding must look its
    /// authority up here rather than minting one per resolution: the operator
    /// pool keys on the authority identity, so two authorities with the same
    /// identity would leave the resident operator holding the stale one.
    pub fn storage_authority_registry(&self) -> &Arc<StorageAuthorityRegistry> {
        &self.storage_authority_registry
    }

    pub fn access_resolver(&self) -> FsAccessResolver {
        self.access_resolver
    }

    pub fn file_runtime(&self) -> &Arc<dyn FileIoRuntime> {
        &self.file_runtime
    }

    pub fn file_task_spawner(&self) -> &Arc<dyn FileTaskSpawner> {
        &self.file_task_spawner
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, StaticCredentialReference,
        StorageCredentialScopePrefix,
    };

    use super::*;
    use crate::{
        AcquisitionFailure, AuthorityCapabilityPath, AuthorityMaterial, AuthorityMaterialSource,
        FileResult, FileTask, FileTaskFuture, SecretValue, StorageAuthorityId, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };

    struct CountingSpawner {
        inner: TokioFileTaskSpawner,
        refreshes: AtomicUsize,
    }

    impl FileTaskSpawner for CountingSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            self.inner.spawn(task)
        }

        fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            self.refreshes.fetch_add(1, Ordering::Relaxed);
            self.inner.spawn_detached_blocking(job);
        }
    }

    struct CountingSource {
        acquisitions: AtomicUsize,
    }

    impl AuthorityMaterialSource for CountingSource {
        fn acquire(&self, _deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
            self.acquisitions.fetch_add(1, Ordering::Relaxed);
            Ok(AuthorityMaterial::new(
                SecretValue::new("access-key"),
                SecretValue::new("secret-key"),
                Some(SecretValue::new("session-token")),
                Instant::now() + Duration::from_secs(3600),
            ))
        }
    }

    #[test]
    fn file_io_views_share_one_authority_and_its_original_refresh_executor() {
        let owner_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("owner runtime");
        let scan_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("scan runtime");
        let owner_io: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(owner_runtime.handle().clone()));
        let scan_io: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(scan_runtime.handle().clone()));
        let owner_spawner = Arc::new(CountingSpawner {
            inner: TokioFileTaskSpawner::new(owner_runtime.handle().clone()),
            refreshes: AtomicUsize::new(0),
        });
        let scan_spawner = Arc::new(CountingSpawner {
            inner: TokioFileTaskSpawner::new(scan_runtime.handle().clone()),
            refreshes: AtomicUsize::new(0),
        });
        let owner_tasks: Arc<dyn FileTaskSpawner> = owner_spawner.clone();
        let scan_tasks: Arc<dyn FileTaskSpawner> = scan_spawner.clone();
        let write = FsAccessResources::new(
            Arc::new(
                ObjectStoreProviderPool::new(crate::ObjectStoreProviderPoolOptions::default())
                    .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::clone(&owner_io),
            Arc::clone(&owner_tasks),
        );
        let read = write.with_file_io(Arc::clone(&scan_io), Arc::clone(&scan_tasks));
        assert!(Arc::ptr_eq(write.file_runtime(), &owner_io));
        assert!(Arc::ptr_eq(read.file_runtime(), &scan_io));
        assert!(Arc::ptr_eq(write.file_task_spawner(), &owner_tasks));
        assert!(Arc::ptr_eq(read.file_task_spawner(), &scan_tasks));
        assert!(!Arc::ptr_eq(write.file_runtime(), read.file_runtime()));
        assert!(Arc::ptr_eq(
            write.object_store_provider_pool(),
            read.object_store_provider_pool()
        ));
        assert!(Arc::ptr_eq(
            write.storage_authority_registry(),
            read.storage_authority_registry()
        ));

        let id = StorageAuthorityId::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("lake").expect("catalog name"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/")
                .expect("scope prefix"),
            AuthorityCapabilityPath::CredentialsEndpoint {
                principal: StaticCredentialReference::try_new("vending", "g1")
                    .expect("vending principal"),
                endpoint: Arc::from("https://catalog.example/credentials"),
            },
        );
        let source = Arc::new(CountingSource {
            acquisitions: AtomicUsize::new(0),
        });
        let read_authority =
            read.storage_authority_registry()
                .authority(&id, Instant::now(), || source.clone());
        let write_authority =
            write
                .storage_authority_registry()
                .authority(&id, Instant::now(), || {
                    panic!("the second I/O view must reuse the resident authority")
                });
        assert!(Arc::ptr_eq(&read_authority, &write_authority));
        let deadline = Instant::now() + Duration::from_secs(5);
        owner_runtime.block_on(async {
            read_authority
                .material_for_request(Instant::now(), deadline)
                .await
                .expect("first view acquires material");
            write_authority
                .material_for_request(Instant::now(), deadline)
                .await
                .expect("second view reuses material");
        });
        assert_eq!(source.acquisitions.load(Ordering::Relaxed), 1);
        assert_eq!(owner_spawner.refreshes.load(Ordering::Relaxed), 1);
        assert_eq!(scan_spawner.refreshes.load(Ordering::Relaxed), 0);
        assert_eq!(write.storage_authority_registry().metrics().misses, 1);
        assert_eq!(write.storage_authority_registry().metrics().hits, 1);
        assert_eq!(read_authority.metrics().refreshes_applied, 1);
    }

    #[test]
    fn retains_the_explicitly_composed_runtime_services() {
        let runtime = tokio::runtime::Runtime::new().expect("build explicit Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let resources = FsAccessResources::new(
            Arc::new(
                ObjectStoreProviderPool::new(crate::ObjectStoreProviderPoolOptions::default())
                    .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );

        assert!(Arc::ptr_eq(resources.file_runtime(), &file_runtime));
        assert!(Arc::ptr_eq(resources.file_task_spawner(), &task_spawner));
        assert_eq!(
            resources.object_store_provider_pool().options(),
            crate::ObjectStoreProviderPoolOptions::default()
        );
    }
}
