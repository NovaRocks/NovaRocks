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

use std::sync::Arc;

use futures::future::BoxFuture;
use novarocks_proto_codec::connector_read::{ConnectorReadDecoder, ConnectorReadEncoder};
use novarocks_proto_codec::connector_write::{
    ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder, ConnectorWriteHandleDecoder,
    ConnectorWriteHandleEncoder,
};
use novarocks_spi::connector::read_stack::{
    ConnectorReadMetadata, ConnectorReadProviderFactory, ConnectorReadRequestControlFactory,
    ConnectorReadSplitManager,
};
use novarocks_spi::connector::write_stack::{
    ConnectorWriteControl as ConnectorWriteSessionControl,
    ConnectorWriteExecution as ConnectorWriteStackExecution,
};
use novarocks_spi::connector::{
    CatalogProviderKind, ConnectorControlBinding, ConnectorError, ConnectorWriteControl,
};

use crate::{ConnectorMaterializationError, MaterializationContext, NormalizedCatalogProperties};

/// The complete FE typed-read group for one exact control generation.
#[derive(Clone)]
pub struct ConnectorControlReadBinding {
    metadata: Arc<dyn ConnectorReadMetadata>,
    splits: Arc<dyn ConnectorReadSplitManager>,
    request_factory: Option<Arc<dyn ConnectorReadRequestControlFactory>>,
    encoder: Arc<dyn ConnectorReadEncoder>,
}

impl ConnectorControlReadBinding {
    pub fn new(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
        request_factory: Option<Arc<dyn ConnectorReadRequestControlFactory>>,
        encoder: Arc<dyn ConnectorReadEncoder>,
    ) -> Self {
        Self {
            metadata,
            splits,
            request_factory,
            encoder,
        }
    }

    pub fn metadata(&self) -> Arc<dyn ConnectorReadMetadata> {
        Arc::clone(&self.metadata)
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        Arc::clone(&self.splits)
    }

    pub fn request_factory(&self) -> Option<Arc<dyn ConnectorReadRequestControlFactory>> {
        self.request_factory.as_ref().map(Arc::clone)
    }

    pub fn encoder(&self) -> Arc<dyn ConnectorReadEncoder> {
        Arc::clone(&self.encoder)
    }
}

/// The complete FE write group for one exact control generation.
///
/// It exists separately from generic control so a caller cannot discover
/// optional write authority through typed-read state.
///
/// Every member is required rather than optional, so a provider structurally
/// cannot publish write behaviour with half a codec: a generation that could
/// encode a writer handle but not decode the commit fragments that come back
/// would produce writes it could never commit. Completeness is therefore a
/// property of the type, not a runtime check a caller might skip.
///
/// The directions here are the frontend's half of the pair: it encodes the
/// handles it sends and decodes the fragments it receives. It is given no way
/// to forge a fragment or to interpret a handle.
#[derive(Clone)]
pub struct ConnectorControlWriteBinding {
    write: Arc<dyn ConnectorWriteControl>,
    session: Arc<dyn ConnectorWriteSessionControl>,
    handle_encoder: Arc<dyn ConnectorWriteHandleEncoder>,
    fragment_decoder: Arc<dyn ConnectorWriteFragmentDecoder>,
}

impl ConnectorControlWriteBinding {
    pub fn new(
        write: Arc<dyn ConnectorWriteControl>,
        session: Arc<dyn ConnectorWriteSessionControl>,
        handle_encoder: Arc<dyn ConnectorWriteHandleEncoder>,
        fragment_decoder: Arc<dyn ConnectorWriteFragmentDecoder>,
    ) -> Self {
        Self {
            write,
            session,
            handle_encoder,
            fragment_decoder,
        }
    }

    pub fn write(&self) -> Arc<dyn ConnectorWriteControl> {
        Arc::clone(&self.write)
    }

    /// The begin/finish/abort/reconcile authority for this generation. It is
    /// the only path to an external commit, and it lives on the frontend only.
    pub fn session(&self) -> Arc<dyn ConnectorWriteSessionControl> {
        Arc::clone(&self.session)
    }

    pub fn handle_encoder(&self) -> Arc<dyn ConnectorWriteHandleEncoder> {
        Arc::clone(&self.handle_encoder)
    }

    pub fn fragment_decoder(&self) -> Arc<dyn ConnectorWriteFragmentDecoder> {
        Arc::clone(&self.fragment_decoder)
    }
}

/// One complete FE role binding for one exact desired catalog generation.
// Design: ADR-0130 (docs/adr/ADR-0130-connector-role-binding-generation-ownership.md)
pub struct ConnectorControlRoleBinding {
    properties: NormalizedCatalogProperties,
    control: Arc<ConnectorControlBinding>,
    read: Option<ConnectorControlReadBinding>,
    write: Option<ConnectorControlWriteBinding>,
}

impl ConnectorControlRoleBinding {
    pub fn try_new(
        properties: NormalizedCatalogProperties,
        control: Arc<ConnectorControlBinding>,
        read: Option<ConnectorControlReadBinding>,
        write: Option<ConnectorControlWriteBinding>,
    ) -> Result<Self, ConnectorError> {
        if control.catalog_properties()? != properties.as_catalog_properties() {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding properties do not match the control generation",
            ));
        }
        if control.descriptor().instance_id != *properties.handle().catalog_name()
            || control.descriptor().provider_id.as_str() != properties.provider_kind().provider_id()
        {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding owner does not match normalized catalog properties",
            ));
        }
        if read.is_some() && control.catalog_handle()? != properties.handle() {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "typed read binding does not have an exact catalog handle",
            ));
        }
        // Write parity, mirroring the read parity the execution side already
        // enforces. A generation that advertises generic write behaviour but
        // publishes no typed write group would leave a caller able to admit a
        // write it could never encode; the reverse would publish a codec for
        // behaviour that does not exist. `write: None` on both sides stays a
        // real "this provider cannot write".
        if write.is_some() != control.write().is_some() {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding write group does not match generic control capability",
            ));
        }
        Ok(Self {
            properties,
            control,
            read,
            write,
        })
    }

    pub const fn properties(&self) -> &NormalizedCatalogProperties {
        &self.properties
    }

    pub fn control(&self) -> &ConnectorControlBinding {
        &self.control
    }

    pub fn control_arc(&self) -> Arc<ConnectorControlBinding> {
        Arc::clone(&self.control)
    }

    pub const fn read(&self) -> Option<&ConnectorControlReadBinding> {
        self.read.as_ref()
    }

    pub const fn write(&self) -> Option<&ConnectorControlWriteBinding> {
        self.write.as_ref()
    }

    pub fn into_parts(
        self,
    ) -> (
        NormalizedCatalogProperties,
        Arc<ConnectorControlBinding>,
        Option<ConnectorControlReadBinding>,
        Option<ConnectorControlWriteBinding>,
    ) {
        (self.properties, self.control, self.read, self.write)
    }
}

/// The complete BE typed-read group for one exact execution binding.
#[derive(Clone)]
pub struct ConnectorExecutionReadBinding {
    provider_factory: Arc<dyn ConnectorReadProviderFactory>,
    decoder: Arc<dyn ConnectorReadDecoder>,
}

impl ConnectorExecutionReadBinding {
    pub fn new(
        provider_factory: Arc<dyn ConnectorReadProviderFactory>,
        decoder: Arc<dyn ConnectorReadDecoder>,
    ) -> Self {
        Self {
            provider_factory,
            decoder,
        }
    }

    pub fn provider_factory(&self) -> Arc<dyn ConnectorReadProviderFactory> {
        Arc::clone(&self.provider_factory)
    }

    pub fn decoder(&self) -> Arc<dyn ConnectorReadDecoder> {
        Arc::clone(&self.decoder)
    }
}

/// The complete BE write group for one exact execution generation.
///
/// Like its frontend counterpart every member is required, so a backend cannot
/// open writers it has no way to describe the results of. The directions are
/// the mirror image: the backend decodes the handles it is given and encodes
/// the fragments it produces, and it is given no commit authority at all.
#[derive(Clone)]
pub struct ConnectorExecutionWriteBinding {
    execution: Arc<dyn ConnectorWriteStackExecution>,
    handle_decoder: Arc<dyn ConnectorWriteHandleDecoder>,
    fragment_encoder: Arc<dyn ConnectorWriteFragmentEncoder>,
}

impl ConnectorExecutionWriteBinding {
    pub fn new(
        execution: Arc<dyn ConnectorWriteStackExecution>,
        handle_decoder: Arc<dyn ConnectorWriteHandleDecoder>,
        fragment_encoder: Arc<dyn ConnectorWriteFragmentEncoder>,
    ) -> Self {
        Self {
            execution,
            handle_decoder,
            fragment_encoder,
        }
    }

    /// Opens one writer per driver. It has no begin, finish, abort, or
    /// reconcile: a backend never holds a commit handle.
    pub fn execution(&self) -> Arc<dyn ConnectorWriteStackExecution> {
        Arc::clone(&self.execution)
    }

    pub fn handle_decoder(&self) -> Arc<dyn ConnectorWriteHandleDecoder> {
        Arc::clone(&self.handle_decoder)
    }

    pub fn fragment_encoder(&self) -> Arc<dyn ConnectorWriteFragmentEncoder> {
        Arc::clone(&self.fragment_encoder)
    }
}

/// One complete BE role binding. Its factory receives no remote context or
/// request; all remote control work must have completed on the FE side.
// Design: ADR-0130 (docs/adr/ADR-0130-connector-role-binding-generation-ownership.md)
pub struct ConnectorExecutionRoleBinding {
    properties: NormalizedCatalogProperties,
    read: Option<ConnectorExecutionReadBinding>,
    write: Option<ConnectorExecutionWriteBinding>,
}

impl ConnectorExecutionRoleBinding {
    pub fn try_new(
        properties: NormalizedCatalogProperties,
        read: Option<ConnectorExecutionReadBinding>,
        write: Option<ConnectorExecutionWriteBinding>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            properties,
            read,
            write,
        })
    }

    pub const fn properties(&self) -> &NormalizedCatalogProperties {
        &self.properties
    }

    pub const fn read(&self) -> Option<&ConnectorExecutionReadBinding> {
        self.read.as_ref()
    }

    pub const fn write(&self) -> Option<&ConnectorExecutionWriteBinding> {
        self.write.as_ref()
    }
}

/// FE-only factory: validation is local and I/O-free; materialization is an
/// explicitly cancellable future that the FE scheduler owns.
pub trait ConnectorControlRoleBindingFactory: Send + Sync {
    fn provider_kind(&self) -> CatalogProviderKind;

    fn normalize_and_validate(
        &self,
        properties: novarocks_spi::connector::CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError>;

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>;
}

/// BE-only factory: no request, cancellation, or remote client context enters
/// this interface, enforcing bounded process-local materialization.
pub trait ConnectorExecutionRoleBindingFactory: Send + Sync {
    fn provider_kind(&self) -> CatalogProviderKind;

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorInstanceId,
    };

    fn properties() -> NormalizedCatalogProperties {
        NormalizedCatalogProperties::try_new(
            CatalogProperties::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("catalog").unwrap(),
                    CatalogVersion::from_bytes([7; 32]),
                ),
                CatalogProviderKind::Iceberg,
                1,
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn normalized_properties_are_the_only_execution_factory_input() {
        fn accepts_only_local_input(
            factory: &dyn ConnectorExecutionRoleBindingFactory,
            properties: &NormalizedCatalogProperties,
        ) {
            let _ = factory.bind(properties);
        }

        let _ = accepts_only_local_input;
        assert_eq!(properties().provider_kind(), CatalogProviderKind::Iceberg);
    }
}
