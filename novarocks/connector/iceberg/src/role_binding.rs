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

//! Iceberg's complete FE and BE role-binding factories.
//!
//! The control factory is the only provider entry point allowed to construct
//! the catalog runtime.  The execution factory accepts only a frozen catalog
//! definition and builds local, lazy reader and writer factories; it does not
//! create a catalog client or contact REST, HMS, or object storage.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
use novarocks_spi::connector::write_stack::ConnectorWriteExecutionFactory;
use novarocks_spi::connector::{
    CatalogProperties, ConnectorControlFactoryRequest, ConnectorDocumentStorageBinding,
    ConnectorError, ConnectorErrorKind, ConnectorProviderId,
};
use novarocks_spi::connector::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory,
    ConnectorControlWriteBinding, ConnectorExecutionReadBinding, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorExecutionWriteBinding,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, MaterializationContext, NormalizedCatalogProperties,
};

use crate::commit::write_stack::codec::{
    IcebergWriteFragmentDecoder, IcebergWriteFragmentEncoder, IcebergWriteHandleDecoder,
    IcebergWriteHandleEncoder,
};
use crate::commit::write_stack::control::IcebergWriteSessionControl;
use crate::commit::write_stack::execution::IcebergWriteStackExecutionFactory;
use crate::commit::write_stack::runtime::build_write_adapter;
use crate::connector_factory::IcebergConnectorFactory;
use crate::resources::{IcebergExecutionResources, IcebergMetadataResources};
use crate::typed_provider_factory::{IcebergTypedProviderFactory, iceberg_descriptor};
use crate::typed_read::page_source_provider::IcebergPageSourceProviderOptions;

/// FE-only factory for one exact Iceberg control generation.
#[derive(Clone)]
pub struct IcebergControlRoleBindingFactory {
    resources: IcebergMetadataResources,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    hms_listing_slot: Option<crate::hms_listing_probe::HmsListingObservationSlot>,
    blocking_materialization_permits: Arc<tokio::sync::Semaphore>,
}

impl IcebergControlRoleBindingFactory {
    pub fn new(
        resources: IcebergMetadataResources,
        max_blocking_materializations: NonZeroUsize,
    ) -> Self {
        Self {
            resources,
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            hms_listing_slot: None,
            blocking_materialization_permits: Arc::new(tokio::sync::Semaphore::new(
                max_blocking_materializations.get(),
            )),
        }
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub fn with_hms_listing_observation_slot(
        mut self,
        slot: crate::hms_listing_probe::HmsListingObservationSlot,
    ) -> Self {
        self.hms_listing_slot = Some(slot);
        self
    }
}

impl ConnectorControlRoleBindingFactory for IcebergControlRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("static provider ID")
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        ensure_iceberg(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn open_catalog(
        &self,
        identity: novarocks_spi::connector::CatalogScopeIdentity,
    ) -> Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory> {
        Arc::new(IcebergCatalogControlFactory {
            factory: self.clone(),
            activity: crate::activity::CatalogActivity::new(identity.clone()),
            identity,
            coordination: Arc::new(
                crate::catalog::table_coordination::IcebergTableCoordination::default(),
            ),
        })
    }
}

struct IcebergCatalogControlFactory {
    factory: IcebergControlRoleBindingFactory,
    identity: novarocks_spi::connector::CatalogScopeIdentity,
    activity: Arc<crate::activity::CatalogActivity>,
    coordination: Arc<crate::catalog::table_coordination::IcebergTableCoordination>,
}

impl novarocks_spi::connector::ProviderCatalogControlFactory for IcebergCatalogControlFactory {
    fn activity_reader(&self) -> Option<Arc<dyn novarocks_spi::connector::CatalogActivityReader>> {
        Some(self.activity.clone())
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let activity = self.activity.generation();
        let resources = self
            .factory
            .resources
            .clone()
            .with_activity(activity.clone());
        let coordination = Arc::clone(&self.coordination);
        let identity = self.identity.clone();
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        let probe = self
            .factory
            .hms_listing_slot
            .as_ref()
            .and_then(|slot| slot.active_probe());
        let permits = Arc::clone(&self.factory.blocking_materialization_permits);
        Box::pin(async move {
            let waiting = activity
                .enter(novarocks_spi::connector::CatalogActivityPhase::MaterializationWaiting);
            context.check_active()?;
            if properties.as_catalog_properties().provider_id() != identity.provider_id()
                || properties.handle().catalog_name() != identity.instance_id()
            {
                return Err(invalid_definition(
                    "Iceberg catalog materializer received another logical catalog".to_string(),
                ));
            }
            // `JoinHandle` drop cannot stop `spawn_blocking`. Move this permit
            // into the closure so a timed-out attempt continues to occupy one
            // bounded slot until its actual blocking work exits; retries can
            // wait, but they cannot accumulate detached workers or sockets.
            let permit = permits.acquire_owned().await.map_err(|_| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Internal,
                    ConnectorMaterializationRetryDisposition::Transient,
                    "Iceberg control materialization limiter is closed",
                )
            })?;
            context.check_active()?;
            let worker_context = context.clone();
            drop(waiting);
            let worker = activity
                .enter(novarocks_spi::connector::CatalogActivityPhase::MaterializationWorker);
            let binding = tokio::task::spawn_blocking(move || {
                // The activity guard drops last, after the job's permit.
                let _worker = worker;
                let _permit = permit;
                materialize_control_blocking(
                    resources,
                    coordination,
                    properties,
                    worker_context,
                    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
                    probe,
                )
            })
            .await
            .map_err(|error| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Internal,
                    ConnectorMaterializationRetryDisposition::Transient,
                    format!("Iceberg control materialization worker failed: {error}"),
                )
            })??;
            context.check_active()?;
            Ok(binding)
        })
    }
}

fn materialize_control_blocking(
    resources: IcebergMetadataResources,
    coordination: Arc<crate::catalog::table_coordination::IcebergTableCoordination>,
    properties: NormalizedCatalogProperties,
    context: MaterializationContext,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")] probe: Option<
        Arc<crate::hms_listing_probe::HmsListingProbe>,
    >,
) -> Result<ConnectorControlRoleBinding, ConnectorMaterializationError> {
    context.check_active()?;
    let catalog_properties = properties.as_catalog_properties().clone();
    ensure_iceberg(&catalog_properties)?;

    // The existing provider factory owns the catalog runtime and all
    // control capabilities. Its read installer is redirected into the
    // complete role group rather than a parallel FE registry.
    let captured_read = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured_read);
    #[cfg(novarocks_dev_controls)]
    let observation_slot = resources.planning_binding().test_observation().cloned();
    let factory = IcebergConnectorFactory::new(resources)
        .with_table_coordination(coordination)
        .with_read_control_installer(Arc::new(
            move |_handle, metadata, splits, encoder, request_factory| {
                let read = ConnectorControlReadBinding::new(
                    metadata,
                    splits,
                    Some(request_factory),
                    encoder,
                );
                let mut slot = capture.lock().map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "Iceberg role binding read capture lock was poisoned",
                    )
                })?;
                if slot.replace(read).is_some() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "Iceberg control generation installed more than one typed read group",
                    ));
                }
                Ok(Arc::new(RoleReadRegistrationLease))
            },
        ));
    let request = ConnectorControlFactoryRequest::try_new(
        factory.provider_id().clone(),
        catalog_properties.handle().catalog_name().clone(),
        catalog_properties
            .execution_properties()
            .iter()
            .map(|property| (property.key().to_owned(), property.value().to_owned()))
            .collect(),
    )
    .and_then(|request| request.with_catalog_properties(catalog_properties.clone()))
    .map_err(ConnectorMaterializationError::from)?;
    // One catalog generation is created here and nowhere else. Its runtime is
    // returned alongside the creation so the frontend write session below is
    // built from *this* generation rather than a second catalog client.
    let (creation, runtime) = factory
        .create_control_with_runtime_and_control(request, Some(&context))
        .map_err(ConnectorMaterializationError::from)?;
    context.check_active()?;
    let (control, _durable_properties) = creation.into_parts();
    let control = control
        .with_catalog_properties(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    let observed_catalog = Arc::clone(runtime.novarocks_catalog());
    let document_storage = Arc::new(crate::document_storage::IcebergDocumentStorage::new(
        control.descriptor().clone(),
        control.incarnation(),
        Arc::clone(&runtime),
    ));
    let document_storage_binding = ConnectorDocumentStorageBinding::try_new(
        control.descriptor().clone(),
        control.incarnation(),
        Some(document_storage.clone()),
        Some(document_storage),
    )
    .map_err(ConnectorMaterializationError::from)?;
    let control = control
        .try_with_document_storage(Some(document_storage_binding))
        .map_err(ConnectorMaterializationError::from)?;
    let read = captured_read
        .lock()
        .map_err(|_| {
            ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
                "Iceberg role binding read capture lock was poisoned",
            )
        })?
        .take()
        .ok_or_else(|| {
            ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
                "Iceberg control generation did not install its typed read group",
            )
        })?;
    // The typed write group is built exactly when the generic control
    // generation advertises write, which is the parity the role binding
    // enforces. Every member comes from the one generation the read group came
    // from: the same descriptor, the same incarnation, and the same exact
    // catalog handle the desired-state owner stamped.
    let write = match control.write().cloned() {
        Some(write) => {
            let descriptor = control.descriptor().clone();
            let catalog_handle = control
                .catalog_handle()
                .map_err(ConnectorMaterializationError::from)?
                .clone();
            let session = IcebergWriteSessionControl::new(
                descriptor.clone(),
                control.incarnation(),
                catalog_handle.clone(),
                runtime,
            );
            #[cfg(novarocks_dev_controls)]
            let session = session.with_test_observation(observation_slot.clone());
            let session = Arc::new(session);
            let adapter = build_write_adapter(descriptor, catalog_handle);
            Some(ConnectorControlWriteBinding::new(
                write,
                session,
                // The frontend's half of each pair: it encodes the handles it
                // sends and decodes the fragments that come back. It is given
                // no way to forge a fragment or to read a handle it received.
                Arc::new(IcebergWriteHandleEncoder::new(adapter.clone())),
                Arc::new(IcebergWriteFragmentDecoder::new(adapter)),
            ))
        }
        None => None,
    };
    context.check_active()?;
    let binding =
        ConnectorControlRoleBinding::try_new(properties, Arc::new(control), Some(read), write)
            .map_err(ConnectorMaterializationError::from)?;
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    if let Some(probe) = probe {
        probe
            .capture(
                binding
                    .control()
                    .catalog_handle()
                    .map_err(ConnectorMaterializationError::from)?,
                binding.control().incarnation(),
                observed_catalog.implementation_name(),
                &observed_catalog.listing_admission(),
            )
            .map_err(|message| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Internal,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    message,
                )
            })?;
    }
    Ok(binding)
}

/// BE-only factory for a frozen Iceberg catalog definition.
///
/// Its only inputs are the startup-composed local resources and immutable
/// properties. Reader and writer network I/O remains deferred to their
/// request-scoped operations.
#[derive(Clone)]
pub struct IcebergExecutionRoleBindingFactory {
    resources: IcebergExecutionResources,
    read_options: IcebergPageSourceProviderOptions,
}

impl IcebergExecutionRoleBindingFactory {
    pub fn new(
        resources: IcebergExecutionResources,
        read_options: IcebergPageSourceProviderOptions,
    ) -> Self {
        Self {
            resources,
            read_options,
        }
    }
}

impl ConnectorExecutionRoleBindingFactory for IcebergExecutionRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("iceberg").expect("static provider ID")
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        let catalog_properties = properties.as_catalog_properties();
        ensure_iceberg(catalog_properties)?;

        // `build` only binds startup-sealed filesystem resources to the exact
        // immutable catalog definition and constructs lazy reader factories.
        //
        // The catalog bridge is installed here and only here: it is what makes
        // a reader on this node able to acquire its own data credentials, and
        // an execution role binding is the one place that is true (CAD-1 D1).
        let read_binding = self
            .resources
            .read_binding()
            .clone()
            .with_catalog_runtime(self.resources.catalog_runtime().clone());
        let typed_read = IcebergTypedProviderFactory::new(read_binding, self.read_options.clone())
            .build(catalog_properties)
            .map_err(ConnectorMaterializationError::from)?;
        // The write-stack execution and both codec facets are minted from the
        // same immutable catalog generation the read facets above were bound
        // to: one descriptor derived from this exact catalog handle, and one
        // adapter over it. A handle another generation encoded therefore cannot
        // open a writer here.
        let catalog_handle = catalog_properties.handle().clone();
        let descriptor = iceberg_descriptor(&catalog_handle);
        let write_binding = self
            .resources
            .write_binding()
            .clone()
            .with_catalog_runtime(self.resources.catalog_runtime().clone());
        let write_execution =
            IcebergWriteStackExecutionFactory::new(descriptor.clone(), write_binding)
                .build(catalog_properties)
                .map_err(ConnectorMaterializationError::from)?;
        let adapter = build_write_adapter(descriptor, catalog_handle);
        let read = ConnectorExecutionReadBinding::new(
            typed_read.admitted_provider_factory(),
            typed_read.decoder(),
            Arc::new(crate::typed_read::codec::IcebergReadRecipeCompiler),
        );
        let write = ConnectorExecutionWriteBinding::new(
            write_execution,
            // The backend's half of each pair, the mirror image of the
            // frontend's: it decodes the handles it is given and encodes the
            // fragments it produces, and holds no commit authority at all.
            Arc::new(IcebergWriteHandleDecoder::new(adapter.clone())),
            Arc::new(IcebergWriteFragmentEncoder::new(adapter)),
        );
        ConnectorExecutionRoleBinding::try_new(properties.clone(), Some(read), Some(write))
            .map_err(ConnectorMaterializationError::from)
    }
}

/// Marker retained by the control generation while the named read group is
/// alive. The role host, rather than this provider, owns registry retirement.
struct RoleReadRegistrationLease;

impl ConnectorReadRegistrationLease for RoleReadRegistrationLease {}

fn ensure_iceberg(properties: &CatalogProperties) -> Result<(), ConnectorMaterializationError> {
    if properties.provider_id().as_str() == "iceberg" {
        return Ok(());
    }
    Err(ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        "Iceberg role binding factory received another provider kind",
    ))
}

fn invalid_definition(detail: String) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_fs::{
        FileCancellation, FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_write::{
        ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder, ConnectorWriteHandleDecoder,
        ConnectorWriteHandleEncoder, ValidatedCommitFragment, ValidatedWriterHandle,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperty, CatalogVersion, ConnectorInstanceId,
    };

    use crate::commit::write_stack::domain::{
        IcebergArtifactMetrics, IcebergCommitFragment, IcebergDataBranchRecipe,
        IcebergDataFileArtifact, IcebergWriterHandle, IcebergWriterOutput,
    };
    use crate::commit::write_stack::test_support::{sample_partition, table_facts};
    use crate::delete_file::IcebergFileFormat;

    fn writer_handle() -> IcebergWriterHandle {
        IcebergWriterHandle::try_new_data(
            table_facts(),
            IcebergWriterOutput::try_new(
                IcebergFileFormat::Parquet,
                parquet::basic::Compression::SNAPPY,
                None,
            )
            .expect("output"),
            IcebergDataBranchRecipe::try_new(None, Vec::new(), Vec::new(), Vec::new(), false)
                .expect("recipe"),
        )
        .expect("writer handle")
    }

    fn commit_fragment() -> IcebergCommitFragment {
        IcebergCommitFragment::data_file(
            IcebergDataFileArtifact::try_new(
                "s3://b/wh/db/t/data/new.parquet".to_string(),
                IcebergFileFormat::Parquet,
                sample_partition(),
                IcebergArtifactMetrics::try_new(4, 4096, Vec::new(), None).expect("metrics"),
                None,
            )
            .expect("data file"),
        )
    }

    fn properties() -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.iceberg").expect("catalog"),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    #[test]
    fn execution_binding_is_complete_and_local_for_frozen_properties() {
        let data_runtime = tokio::runtime::Runtime::new().expect("data runtime");
        let scan_runtime = tokio::runtime::Runtime::new().expect("scan runtime");
        let read_access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(scan_runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(scan_runtime.handle().clone())),
        );
        let write_access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(data_runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(data_runtime.handle().clone())),
        );
        let read_context = read_access
            .file_read_context(
                FileCancellation::new(),
                Instant::now() + Duration::from_secs(60),
            )
            .expect("read context");
        let write_context = write_access
            .file_read_context(
                FileCancellation::new(),
                Instant::now() + Duration::from_secs(60),
            )
            .expect("write context");
        assert!(!Arc::ptr_eq(&read_context.runtime, &write_context.runtime));
        assert!(!Arc::ptr_eq(
            &read_context.task_spawner,
            &write_context.task_spawner
        ));
        let factory = IcebergExecutionRoleBindingFactory::new(
            IcebergExecutionResources::new(
                read_access,
                write_access,
                crate::resources::IcebergCatalogRuntime::new(data_runtime.handle().clone()),
            ),
            IcebergPageSourceProviderOptions::with_default_budget(),
        );
        let normalized = NormalizedCatalogProperties::try_new(properties())
            .expect("normalized Iceberg properties");

        let binding = factory
            .bind(&normalized)
            .expect("local role binding without catalog I/O");

        assert_eq!(binding.properties(), &normalized);
        assert!(binding.read().is_some());

        // The three-member write group is complete and every member names the
        // one catalog generation these properties froze.
        let write = binding.write().expect("one complete typed write group");
        let catalog_name = normalized.handle().catalog_name().as_str();
        assert_eq!(write.handle_decoder().owner(), catalog_name);
        assert_eq!(write.fragment_encoder().owner(), catalog_name);
        assert_eq!(write.execution().catalog_handle(), normalized.handle());

        // The backend's decoder rebuilds a handle bound to that same
        // generation, so the writer factory beside it can open a writer for it.
        let encoder = IcebergWriteHandleEncoder::new(build_write_adapter(
            iceberg_descriptor(normalized.handle()),
            normalized.handle().clone(),
        ));
        let adapter = build_write_adapter(
            iceberg_descriptor(normalized.handle()),
            normalized.handle().clone(),
        );
        let raw = encoder
            .encode_writer_handle(&adapter.wrap_writer_handle(writer_handle()))
            .expect("encode a writer handle for this generation");
        let decoded = write
            .handle_decoder()
            .decode_writer_handle(
                &ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle"))
                    .expect("canonical carrier"),
            )
            .expect("decode a writer handle for this generation");
        assert_eq!(
            adapter
                .writer_handle(&decoded)
                .expect("the decoded handle belongs to this generation")
                .table(),
            &table_facts()
        );
    }

    struct BlockingControlIdentity {
        entered: Arc<tokio::sync::Notify>,
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }
    impl crate::access_binding::IcebergCredentialResolver for BlockingControlIdentity {
        fn resolve_object_store_static(
            &self,
            _: &novarocks_spi::connector::StaticCredentialReference,
        ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test owns no object-store identity",
            ))
        }
        fn resolve_catalog_control(
            &self,
            _: &novarocks_spi::connector::StaticCredentialReference,
        ) -> Result<crate::access_binding::IcebergRestAuthMaterial, ConnectorError> {
            self.entered.notify_one();
            let (lock, ready) = &*self.gate;
            let released = lock.lock().unwrap();
            drop(
                ready
                    .wait_timeout_while(released, Duration::from_secs(30), |released| !*released)
                    .unwrap(),
            );
            Err(ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "injected identity resolver exit before HTTP",
            ))
        }
    }
    struct ReleaseIdentityGate(Arc<(Mutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseIdentityGate {
        fn drop(&mut self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_materialization_wrapper_keeps_the_actual_worker_and_bridge_visible() {
        use novarocks_spi::connector::{
            CatalogActivityPhase as P, CatalogCredentialBinding, CatalogCredentialMode,
            CatalogCredentialPurpose, CredentialConsumerRole, StaticCredentialReference,
        };
        let entered = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release = ReleaseIdentityGate(gate.clone());
        let handle = tokio::runtime::Handle::current();
        let fs = novarocks_fs::FsAccessResources::new(
            Arc::new(novarocks_fs::ObjectStoreProviderPool::new(Default::default()).unwrap()),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(handle.clone())),
            Arc::new(TokioFileTaskSpawner::new(handle.clone())),
        );
        let access = crate::access_binding::IcebergReadBinding::with_metadata_credential_resolver(
            fs,
            Arc::new(BlockingControlIdentity {
                entered: entered.clone(),
                gate,
            }),
        );
        let factory = IcebergControlRoleBindingFactory::new(
            IcebergMetadataResources::new(access, handle),
            NonZeroUsize::new(1).unwrap(),
        );
        let instance = ConnectorInstanceId::parse("catalog.blocked").unwrap();
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let properties = CatalogProperties::new(
            CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([7; 32])),
            provider.clone(),
            1,
            vec![
                CatalogProperty::new("iceberg.catalog.type", "rest").unwrap(),
                CatalogProperty::new("uri", "http://127.0.0.1:1").unwrap(),
            ],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::CatalogControl,
                    CredentialConsumerRole::Frontend,
                    CatalogCredentialMode::Static(
                        StaticCredentialReference::try_new("coordinator", "v1").unwrap(),
                    ),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let scoped = factory.open_catalog(novarocks_spi::connector::CatalogScopeIdentity::new(
            provider,
            instance,
            uuid::Uuid::now_v7(),
        ));
        let reader = scoped.activity_reader().unwrap();
        let context = MaterializationContext::new(Instant::now() + Duration::from_secs(20));
        let work = scoped.materialize(
            factory.normalize_and_validate(properties).unwrap(),
            context.clone(),
        );
        let wrapper = tokio::spawn(work);
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("resolver entered real worker");
        context.cancel();
        wrapper.abort();
        match wrapper.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("wrapper must be cancelled"),
        }
        let snapshot = reader.snapshot();
        assert_eq!(
            snapshot.phases[P::MaterializationWorker as usize].inflight,
            1
        );
        assert_eq!(snapshot.phases[P::Bridge as usize].inflight, 1);
        assert_eq!(snapshot.generations.len(), 1);
        assert_eq!(snapshot.generations[0].generation, None);
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = reader.snapshot();
                if snapshot.phases[P::MaterializationWorker as usize].inflight == 0
                    && snapshot.phases[P::Bridge as usize].inflight == 0
                    && snapshot.generations.is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual worker and bridge exited");
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.phases[P::MaterializationWorker as usize].exited, 1);
        assert_eq!(snapshot.phases[P::Bridge as usize].exited, 1);
        assert_eq!(
            factory.blocking_materialization_permits.available_permits(),
            1
        );
    }

    #[tokio::test]
    async fn control_materialization_captures_one_request_scoped_read_group() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(tokio::runtime::Handle::current())),
            Arc::new(TokioFileTaskSpawner::new(tokio::runtime::Handle::current())),
        );
        let factory = IcebergControlRoleBindingFactory::new(
            IcebergMetadataResources::new(access, tokio::runtime::Handle::current()),
            NonZeroUsize::new(1).expect("nonzero blocking materialization limit"),
        );
        let catalog_properties = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.iceberg").expect("catalog"),
                CatalogVersion::from_bytes([8; 32]),
            ),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new(
                    "iceberg.catalog.warehouse",
                    warehouse.path().display().to_string(),
                )
                .expect("warehouse property"),
            ],
            Vec::new(),
        )
        .expect("catalog properties");
        let normalized = factory
            .normalize_and_validate(catalog_properties)
            .expect("normalize Iceberg properties");

        let binding = factory
            .open_catalog(novarocks_spi::connector::CatalogScopeIdentity::new(
                ConnectorProviderId::parse("iceberg").expect("provider"),
                normalized.handle().catalog_name().clone(),
                uuid::Uuid::now_v7(),
            ))
            .materialize(
                normalized.clone(),
                MaterializationContext::new(
                    std::time::Instant::now() + std::time::Duration::from_secs(5),
                ),
            )
            .await
            .expect("materialize Iceberg control role");

        assert_eq!(binding.properties(), &normalized);
        assert_eq!(
            binding.control().catalog_handle().expect("exact handle"),
            normalized.handle()
        );
        binding
            .require_application_document_management_prerequisites()
            .expect("complete application-document authority group");
        let read = binding.read().expect("one complete typed read group");
        assert!(read.request_factory().is_some());
        assert_eq!(
            read.encoder().owner(),
            normalized.handle().catalog_name().as_str()
        );

        // The write group is present exactly because generic control advertises
        // write, and it is built from the same generation as the read group:
        // the session's binding key is this control binding's own instance and
        // incarnation, not a second one materialized on the side.
        let write = binding.write().expect("one complete typed write group");
        let catalog_name = normalized.handle().catalog_name();
        assert_eq!(write.write().binding_key(), write.session().binding_key());
        assert_eq!(write.handle_encoder().owner(), catalog_name.as_str());
        assert_eq!(write.fragment_decoder().owner(), catalog_name.as_str());
        assert_eq!(&write.session().binding_key().instance_id, catalog_name);
        assert_eq!(
            write.session().binding_key().incarnation,
            binding.control().incarnation()
        );

        // The frontend encodes handles and decodes fragments; the fragment it
        // decodes comes back bound to this same generation.
        let adapter = build_write_adapter(
            iceberg_descriptor(normalized.handle()),
            normalized.handle().clone(),
        );
        let raw = write
            .handle_encoder()
            .encode_writer_handle(&adapter.wrap_writer_handle(writer_handle()))
            .expect("the frontend encodes its own generation's handle");
        assert!(ValidatedWriterHandle::parse(raw, FieldPath::root("writer_handle")).is_ok());

        let encoder = IcebergWriteFragmentEncoder::new(adapter.clone());
        let raw = encoder
            .encode_commit_fragment(&adapter.wrap_commit_fragment(commit_fragment()))
            .expect("encode a commit fragment for this generation");
        let decoded = write
            .fragment_decoder()
            .decode_commit_fragment(
                &ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
                    .expect("canonical carrier"),
            )
            .expect("the frontend decodes the fragments it receives");
        assert_eq!(
            adapter
                .commit_fragment(&decoded)
                .expect("the decoded fragment belongs to this generation")
                .path(),
            "s3://b/wh/db/t/data/new.parquet"
        );
    }
}
