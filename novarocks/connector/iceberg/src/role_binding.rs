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
use novarocks_connector_binding::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory,
    ConnectorControlWriteBinding, ConnectorExecutionReadBinding, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorExecutionWriteBinding,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, MaterializationContext, NormalizedCatalogProperties,
};
use novarocks_spi::connector::read_stack::ConnectorReadRegistrationLease;
use novarocks_spi::connector::write_stack::ConnectorWriteExecutionFactory;
use novarocks_spi::connector::{
    CatalogProperties, CatalogProviderKind, ConnectorControlFactoryRequest, ConnectorError,
    ConnectorErrorKind,
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
    blocking_materialization_permits: Arc<tokio::sync::Semaphore>,
}

impl IcebergControlRoleBindingFactory {
    pub fn new(
        resources: IcebergMetadataResources,
        max_blocking_materializations: NonZeroUsize,
    ) -> Self {
        Self {
            resources,
            blocking_materialization_permits: Arc::new(tokio::sync::Semaphore::new(
                max_blocking_materializations.get(),
            )),
        }
    }
}

impl ConnectorControlRoleBindingFactory for IcebergControlRoleBindingFactory {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        ensure_iceberg(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let resources = self.resources.clone();
        let permits = Arc::clone(&self.blocking_materialization_permits);
        Box::pin(async move {
            context.check_active()?;
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
            let binding = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                materialize_control_blocking(resources, properties, worker_context)
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
    properties: NormalizedCatalogProperties,
    context: MaterializationContext,
) -> Result<ConnectorControlRoleBinding, ConnectorMaterializationError> {
    context.check_active()?;
    let catalog_properties = properties.as_catalog_properties().clone();
    ensure_iceberg(&catalog_properties)?;

    // The existing provider factory owns the catalog runtime and all
    // control capabilities. Its read installer is redirected into the
    // complete role group rather than a parallel FE registry.
    let captured_read = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured_read);
    let factory = IcebergConnectorFactory::new(resources).with_read_control_installer(Arc::new(
        move |_handle, metadata, splits, encoder, request_factory| {
            let read =
                ConnectorControlReadBinding::new(metadata, splits, Some(request_factory), encoder);
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
        .create_control_with_runtime(request)
        .map_err(ConnectorMaterializationError::from)?;
    context.check_active()?;
    let (control, _durable_properties) = creation.into_parts();
    let control = control
        .with_catalog_properties(catalog_properties)
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
            let session = Arc::new(IcebergWriteSessionControl::new(
                descriptor.clone(),
                control.incarnation(),
                catalog_handle.clone(),
                runtime,
            ));
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
    ConnectorControlRoleBinding::try_new(properties, Arc::new(control), Some(read), write)
        .map_err(ConnectorMaterializationError::from)
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
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::Iceberg
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        let catalog_properties = properties.as_catalog_properties();
        ensure_iceberg(catalog_properties)?;

        // `build` only binds startup-sealed filesystem resources to the exact
        // immutable catalog definition and constructs lazy reader factories.
        let typed_read = IcebergTypedProviderFactory::new(
            self.resources.binding().clone(),
            self.read_options.clone(),
        )
        .build(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
        // The write-stack execution and both codec facets are minted from the
        // same immutable catalog generation the read facets above were bound
        // to: one descriptor derived from this exact catalog handle, and one
        // adapter over it. A handle another generation encoded therefore cannot
        // open a writer here.
        let catalog_handle = catalog_properties.handle().clone();
        let descriptor = iceberg_descriptor(&catalog_handle);
        let write_execution = IcebergWriteStackExecutionFactory::new(
            descriptor.clone(),
            self.resources.binding().clone(),
        )
        .build(catalog_properties)
        .map_err(ConnectorMaterializationError::from)?;
        let adapter = build_write_adapter(descriptor, catalog_handle);
        let read =
            ConnectorExecutionReadBinding::new(typed_read.provider_factory(), typed_read.decoder());
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
    if properties.provider_kind() == CatalogProviderKind::Iceberg {
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
    use super::*;
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_write::{
        ConnectorWriteFragmentEncoder, ConnectorWriteHandleEncoder, ValidatedCommitFragment,
        ValidatedWriterHandle,
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
            CatalogProviderKind::Iceberg,
            1,
            Vec::new(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    #[test]
    fn execution_binding_is_complete_and_local_for_frozen_properties() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let access = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
        );
        let factory = IcebergExecutionRoleBindingFactory::new(
            IcebergExecutionResources::new(access),
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
            CatalogProviderKind::Iceberg,
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
