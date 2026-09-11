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

//! The worker-side provider factory the backend registry installs.
//!
//! A page-source provider owns a footer cache and a delete manager and needs
//! the request's deadline and cancellation, so it is built per fragment
//! instance rather than shared process-wide. This factory is the
//! generation-scoped, stateless thing the backend can hold instead.

use std::sync::Arc;

use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadFactory, ProviderReadFactoryAdapter, ProviderReadPageSourceProvider,
    ProviderReadRuntime, ProviderReadSystemTableProvider, ReadRuntimeAdapter,
};
use novarocks_spi::connector::read_stack::{
    ConnectorDataCacheOptions, ConnectorPageSourceProviderOptions,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorProviderId, ConnectorReadWireDecoder, ConnectorRequestContext,
};

use crate::access_binding::IcebergReadBinding;
use crate::provider_types::IcebergReadCodecs;
use crate::typed_read::page_source_provider::{
    IcebergPageSourceProvider, IcebergPageSourceProviderOptions,
};
use crate::typed_read::system_page_source::IcebergSystemTableProvider;
use crate::typed_read::{
    HiveTransactionHandle, IcebergColumnHandle, IcebergConnectorReadWireAdapter,
    IcebergExecutionReadRuntime, IcebergReadSplit, IcebergRuntimeRelation,
};

/// Builds Iceberg worker readers for one immutable catalog runtime.
#[derive(Clone)]
pub struct IcebergTypedProviderFactory {
    binding: IcebergReadBinding,
    options: IcebergPageSourceProviderOptions,
}

impl IcebergTypedProviderFactory {
    /// The composition-root entry point. `binding` is the same process-local
    /// access binding the existing execution installer holds, so both paths
    /// resolve object storage through one owner.
    pub fn new(binding: IcebergReadBinding, options: IcebergPageSourceProviderOptions) -> Self {
        Self { binding, options }
    }

    /// Bind the process-local catalog runtime to the admitted fragment request
    /// before constructing a reader. In particular, a vended catalog can
    /// resolve storage only through this request's lifecycle-installed
    /// resolver; it must never fall back to the generation binding.
    fn binding_for_request(&self, request: &ConnectorRequestContext) -> IcebergReadBinding {
        self.binding.for_request(request.clone())
    }

    /// Pair this request-scoped execution factory with one exact provider
    /// runtime. This is connector-internal composition; roles only receive
    /// the erased SPI factory created by the execution bundle.
    pub fn into_read_runtime_factory<P>(
        self,
        adapter: ReadRuntimeAdapter<P>,
    ) -> ProviderReadFactoryAdapter<P, Self>
    where
        P: ProviderReadRuntime<
                Table = IcebergRuntimeRelation,
                Column = IcebergColumnHandle,
                Transaction = HiveTransactionHandle,
                Split = IcebergReadSplit,
            >,
        Self: ProviderReadFactory<P>,
    {
        ProviderReadFactoryAdapter::new(adapter, Arc::new(self))
    }

    /// Bind one immutable catalog generation to the directional BE read
    /// services. The returned decoder cannot be used by FE composition.
    pub fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<IcebergExecutionReadBinding, ConnectorError> {
        let binding = self.binding.bind_catalog(properties)?;
        let catalog_handle = properties.handle();
        let runtime = IcebergExecutionReadRuntime::new(
            iceberg_descriptor(catalog_handle),
            catalog_handle.clone(),
            HiveTransactionHandle::new(true, catalog_transaction_marker(catalog_handle)),
        );

        let adapter = ReadRuntimeAdapter::new(Arc::new(runtime));
        let decoder: Arc<dyn ConnectorReadWireDecoder> =
            Arc::new(IcebergConnectorReadWireAdapter::new(adapter.clone()));
        let provider_factory = Arc::new(ProviderReadFactoryAdapter::new(
            adapter,
            Arc::new(Self {
                binding,
                options: self.options.clone(),
            }),
        ));
        Ok(IcebergExecutionReadBinding {
            provider_factory,
            decoder,
            private_read_codecs: crate::provider_types::IcebergReadTypes::wire_codecs(),
        })
    }
}

impl std::fmt::Debug for IcebergTypedProviderFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTypedProviderFactory")
            .finish_non_exhaustive()
    }
}

impl<P> ProviderReadFactory<P> for IcebergTypedProviderFactory
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        reader_policy: ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ProviderReadPageSourceProvider<P>>, ConnectorError> {
        let binding = self.binding_for_request(request);
        let context =
            binding.file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        let options = apply_reader_policy(self.options.clone(), reader_policy);
        Ok(Arc::new(IcebergPageSourceProvider::new(
            binding, context, options,
        )))
    }

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ProviderReadSystemTableProvider<P>>, ConnectorError> {
        let binding = self.binding_for_request(request);
        let context =
            binding.file_read_context(novarocks_fs::FileCancellation::new(), request.deadline())?;
        Ok(Arc::new(IcebergSystemTableProvider::new(
            binding,
            context,
            self.options.budget.max_rows,
        )))
    }
}

fn apply_reader_policy(
    mut options: IcebergPageSourceProviderOptions,
    reader_policy: ConnectorPageSourceProviderOptions,
) -> IcebergPageSourceProviderOptions {
    options.reader_options.enable_parquet_reader_page_index =
        reader_policy.enable_parquet_reader_page_index;
    options.cache_options = external_cache_options(reader_policy.data_cache);
    options
}

fn external_cache_options(policy: ConnectorDataCacheOptions) -> novarocks_fs::CacheOptions {
    novarocks_fs::CacheOptions {
        enable_scan_datacache: policy.enable_scan_datacache,
        enable_populate_datacache: policy.enable_populate_datacache,
        enable_datacache_async_populate_mode: policy.enable_datacache_async_populate_mode,
        enable_datacache_io_adaptor: policy.enable_datacache_io_adaptor,
        enable_cache_select: policy.enable_cache_select,
        datacache_evict_probability: policy.datacache_evict_probability,
        datacache_priority: policy.datacache_priority,
        datacache_ttl_seconds: policy.datacache_ttl_seconds,
        datacache_sharing_work_period: policy.datacache_sharing_work_period,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_fs::{
        FsAccessResolver, FsAccessResources, ObjectStoreProviderPool,
        ObjectStoreProviderPoolOptions, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogProperty, CatalogVersion, ConnectorCancellation,
        ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId, ConnectorStorageResolver,
        CredentialConsumerRole, ResolvedVendedS3Access, StorageAccessRequest,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct RejectingStaticResolver;

    impl crate::access_binding::IcebergStaticCredentialResolver for RejectingStaticResolver {
        fn resolve_object_store_static(
            &self,
            _reference: &novarocks_spi::connector::StaticCredentialReference,
        ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "typed factory test static resolver must not be used",
            ))
        }
    }

    struct RecordingVendedResolver {
        calls: AtomicUsize,
    }

    impl ConnectorStorageResolver for RecordingVendedResolver {
        fn resolve_vended_s3(
            &self,
            request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            assert_eq!(request.owner().catalog_name().as_str(), "typed-vended-test");
            assert_eq!(request.location(), "s3://warehouse/table/data.parquet");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "typed factory test vended resolver was selected",
            ))
        }
    }

    fn vended_factory() -> IcebergTypedProviderFactory {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let resources = FsAccessResources::new(
            Arc::new(
                ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())
                    .expect("provider pool"),
            ),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        // The test does not schedule file I/O; the runtime is needed only to
        // construct a valid binding while validating resolver selection.
        let properties = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("typed-vended-test").expect("catalog"),
                CatalogVersion::from_bytes([0x27; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new("aws.s3.endpoint", "http://minio:9000")
                    .expect("endpoint property"),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties");
        IcebergTypedProviderFactory::new(
            IcebergReadBinding::from_catalog_properties(
                resources,
                Arc::new(RejectingStaticResolver),
                &properties,
            )
            .expect("vended binding"),
            IcebergPageSourceProviderOptions::with_default_budget(),
        )
    }

    fn request_context(resolver: Arc<RecordingVendedResolver>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            2048,
        )
        .expect("request context")
        .with_storage_resolver(resolver)
    }

    fn assert_request_binding_uses_vended_resolver(
        factory: &IcebergTypedProviderFactory,
        request: &ConnectorRequestContext,
        resolver: &RecordingVendedResolver,
    ) {
        let error = factory
            .binding_for_request(request)
            .resolve_access("s3://warehouse/table/data.parquet")
            .expect_err("request binding must select the lifecycle vended resolver");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn query_reader_policy_overrides_the_generation_default() {
        let options = apply_reader_policy(
            IcebergPageSourceProviderOptions::with_default_budget(),
            ConnectorPageSourceProviderOptions {
                enable_parquet_reader_page_index: true,
                data_cache: ConnectorDataCacheOptions {
                    enable_scan_datacache: true,
                    enable_populate_datacache: true,
                    datacache_priority: 2,
                    ..Default::default()
                },
            },
        );
        assert!(options.reader_options.enable_parquet_reader_page_index);
        assert!(options.cache_options.enable_scan_datacache);
        assert!(options.cache_options.enable_populate_datacache);
        assert_eq!(options.cache_options.datacache_priority, 2);

        let options = apply_reader_policy(
            options,
            ConnectorPageSourceProviderOptions {
                enable_parquet_reader_page_index: false,
                data_cache: ConnectorDataCacheOptions::default(),
            },
        );
        assert!(!options.reader_options.enable_parquet_reader_page_index);
        assert!(!options.cache_options.enable_scan_datacache);
    }

    #[test]
    fn transaction_marker_is_a_fixed_width_derivative_not_catalog_identity() {
        let handle = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake.analytics")
                .expect("canonical catalog name"),
            CatalogVersion::from_bytes(std::array::from_fn(|index| index as u8)),
        );

        assert_eq!(
            catalog_transaction_marker(&handle),
            std::array::from_fn(|index| index as u8),
        );
    }

    #[test]
    fn typed_data_provider_binds_the_request_vended_resolver() {
        let factory = vended_factory();
        let resolver = Arc::new(RecordingVendedResolver {
            calls: AtomicUsize::new(0),
        });
        let request = request_context(resolver.clone());

        assert_request_binding_uses_vended_resolver(&factory, &request, &resolver);
    }

    #[test]
    fn typed_system_table_provider_binds_the_request_vended_resolver() {
        let factory = vended_factory();
        let resolver = Arc::new(RecordingVendedResolver {
            calls: AtomicUsize::new(0),
        });
        let request = request_context(resolver.clone());

        assert_request_binding_uses_vended_resolver(&factory, &request, &resolver);
    }
}

pub struct IcebergExecutionReadBinding {
    provider_factory: Arc<dyn novarocks_spi::connector::read_stack::ConnectorReadProviderFactory>,
    decoder: Arc<dyn ConnectorReadWireDecoder>,
    private_read_codecs: IcebergReadCodecs,
}

impl IcebergExecutionReadBinding {
    pub fn provider_factory(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::read_stack::ConnectorReadProviderFactory> {
        Arc::clone(&self.provider_factory)
    }

    pub fn decoder(&self) -> Arc<dyn ConnectorReadWireDecoder> {
        Arc::clone(&self.decoder)
    }

    /// Provider-owned codec facet consumed by the UEA role adapters after the
    /// atomic public-envelope cutover.
    pub const fn private_read_codecs(&self) -> IcebergReadCodecs {
        self.private_read_codecs
    }
}

/// Rebuild the provider descriptor from the catalog runtime identity. The
/// provider is static because this factory is installed only in Iceberg's
/// sealed provider-kind slot; only the catalog name belongs in this diagnostic
/// descriptor. The full 32-byte content version remains in `CatalogHandle`.
pub(crate) fn iceberg_descriptor(catalog_handle: &CatalogHandle) -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse(crate::PROVIDER_ID)
            .expect("static Iceberg provider ID is valid"),
        instance_id: catalog_handle.catalog_name().clone(),
    }
}

/// Iceberg's pre-existing transaction marker is a fixed 16-byte field, while
/// catalog identity is the complete 32-byte content version. This value is
/// only the marker required by the Iceberg transaction handle; all catalog
/// lookup and relation validation use the untruncated `CatalogHandle` above.
fn catalog_transaction_marker(catalog_handle: &CatalogHandle) -> [u8; 16] {
    let mut marker = [0; 16];
    marker.copy_from_slice(&catalog_handle.version().as_bytes()[..16]);
    marker
}
