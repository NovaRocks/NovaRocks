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

use novarocks_connector_contract::ConnectorReadRelationRecipeCompiler;

use crate::connector::read_stack::{
    ConnectorAdmittedReadProviderFactory, ConnectorReadAttemptAccessSource,
    ConnectorReadAttemptRuntime, ConnectorReadBinding, ConnectorReadMetadata,
    ConnectorReadRequestControl, ConnectorReadRequestControlFactory, ConnectorReadSplitManager,
    ConnectorReadTableHandle,
};
use crate::connector::write_stack::{
    ConnectorWriteControl as ConnectorWriteSessionControl,
    ConnectorWriteExecution as ConnectorWriteStackExecution,
};
use crate::connector::{
    ConnectorCodecError, ConnectorControlBinding, ConnectorError, ConnectorReadWireDecoder,
    ConnectorReadWireEncoder, ConnectorWriteControl, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder, ConnectorWriteHandleWireDecoder,
    ConnectorWriteHandleWireEncoder, NormalizedCatalogProperties,
};

/// The complete FE typed-read group for one exact control generation.
#[derive(Clone)]
pub struct ConnectorControlReadBinding {
    binding: ConnectorReadBinding,
    metadata: Arc<dyn ConnectorReadMetadata>,
    splits: Arc<dyn ConnectorReadSplitManager>,
    request_factory: Option<Arc<dyn ConnectorReadRequestControlFactory>>,
    encoder: Arc<dyn ConnectorReadWireEncoder>,
}

impl ConnectorControlReadBinding {
    pub fn new(
        metadata: Arc<dyn ConnectorReadMetadata>,
        splits: Arc<dyn ConnectorReadSplitManager>,
        request_factory: Option<Arc<dyn ConnectorReadRequestControlFactory>>,
        encoder: Arc<dyn ConnectorReadWireEncoder>,
    ) -> Self {
        let binding = metadata.binding().clone();
        Self {
            binding,
            metadata,
            splits,
            request_factory,
            encoder,
        }
    }

    pub fn metadata(&self) -> Arc<dyn ConnectorReadMetadata> {
        Arc::clone(&self.metadata)
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        Arc::clone(&self.splits)
    }

    pub fn request_factory(&self) -> Option<Arc<dyn ConnectorReadRequestControlFactory>> {
        self.request_factory.as_ref().map(Arc::clone)
    }

    pub fn encoder(&self) -> Arc<dyn ConnectorReadWireEncoder> {
        Arc::clone(&self.encoder)
    }

    /// Seal the final negotiated handle into the only per-attempt access path
    /// for this installed read generation.
    pub fn seal_attempt_access(
        &self,
        request_control: &ConnectorReadRequestControl,
        frozen: &ConnectorReadTableHandle,
    ) -> Result<ConnectorReadAttemptAccess, ConnectorError> {
        if self.metadata.binding() != &self.binding
            || self.splits.binding() != &self.binding
            || request_control.binding() != &self.binding
            || frozen.binding() != &self.binding
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "Connector read role binding cannot combine generations",
            ));
        }
        Ok(ConnectorReadAttemptAccess {
            source: request_control.seal_attempt_access(frozen)?,
            encoder: Arc::clone(&self.encoder),
        })
    }
}

/// Process-local ability to reacquire one immutable read for a new attempt.
/// It contains no secret and exposes no way to choose a different handle.
pub struct ConnectorReadAttemptAccess {
    source: ConnectorReadAttemptAccessSource,
    encoder: Arc<dyn ConnectorReadWireEncoder>,
}

impl ConnectorReadAttemptAccess {
    pub const fn frozen(&self) -> &ConnectorReadTableHandle {
        self.source.frozen()
    }

    pub fn for_attempt(
        &self,
        request: &crate::connector::ConnectorAttemptContext,
        generation: &crate::connector::ConnectorControlPlanningLease,
    ) -> Result<ConnectorReadAttemptCapabilities, ConnectorError> {
        let binding = generation.binding();
        if binding.descriptor() != self.source.frozen().binding().descriptor()
            || binding.catalog_handle()? != self.source.frozen().binding().catalog_handle()
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "Connector read attempt access requires its exact Catalog generation guard",
            ));
        }
        Ok(ConnectorReadAttemptCapabilities {
            frozen: self.source.frozen().clone(),
            runtime: self.source.for_attempt(request)?,
            encoder: Arc::clone(&self.encoder),
        })
    }
}

/// Complete request-bound read capabilities for one attempt and one exact
/// final handle.
pub struct ConnectorReadAttemptCapabilities {
    frozen: ConnectorReadTableHandle,
    runtime: ConnectorReadAttemptRuntime,
    encoder: Arc<dyn ConnectorReadWireEncoder>,
}

impl ConnectorReadAttemptCapabilities {
    pub const fn frozen(&self) -> &ConnectorReadTableHandle {
        &self.frozen
    }

    pub fn splits(&self) -> Arc<dyn ConnectorReadSplitManager> {
        self.runtime.splits()
    }

    pub fn encoder(&self) -> Arc<dyn ConnectorReadWireEncoder> {
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
    handle_encoder: Arc<dyn ConnectorWriteHandleWireEncoder>,
    fragment_decoder: Arc<dyn ConnectorWriteFragmentWireDecoder>,
}

impl ConnectorControlWriteBinding {
    pub fn new(
        write: Arc<dyn ConnectorWriteControl>,
        session: Arc<dyn ConnectorWriteSessionControl>,
        handle_encoder: Arc<dyn ConnectorWriteHandleWireEncoder>,
        fragment_decoder: Arc<dyn ConnectorWriteFragmentWireDecoder>,
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

    pub fn handle_encoder(&self) -> Arc<dyn ConnectorWriteHandleWireEncoder> {
        Arc::clone(&self.handle_encoder)
    }

    pub fn fragment_decoder(&self) -> Arc<dyn ConnectorWriteFragmentWireDecoder> {
        Arc::clone(&self.fragment_decoder)
    }

    fn validate_for_role(
        &self,
        expected_key: &crate::connector::ConnectorProviderBindingKey,
        expected_wire_owner: &str,
    ) -> Result<(), ConnectorError> {
        if self.write.binding_key() != expected_key
            || self.session.binding_key() != expected_key
            || self.handle_encoder.owner() != expected_wire_owner
            || self.fragment_decoder.owner() != expected_wire_owner
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "control write group does not match its exact role generation",
            ));
        }
        Ok(())
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
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding properties do not match the control generation",
            ));
        }
        if control.descriptor().instance_id != *properties.handle().catalog_name()
            || &control.descriptor().provider_id != properties.provider_id()
        {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding owner does not match normalized catalog properties",
            ));
        }
        if read.is_some() && control.catalog_handle()? != properties.handle() {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::InvalidRequest,
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
                crate::connector::ConnectorErrorKind::InvalidRequest,
                "control role binding write group does not match generic control capability",
            ));
        }
        if let Some(write) = &write {
            let expected_key = crate::connector::ConnectorProviderBindingKey {
                instance_id: control.descriptor().instance_id.clone(),
                incarnation: control.incarnation(),
            };
            write.validate_for_role(&expected_key, control.descriptor().instance_id.as_str())?;
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

    /// Verify every broad authority needed before an application-document
    /// management operation starts any provider preparation. The specific
    /// operation must still pass `admit_management`; trait presence alone does
    /// not claim support for create, update, or publication.
    pub fn require_application_document_management_prerequisites(
        &self,
    ) -> Result<(), ConnectorError> {
        let complete = self.control.document_storage().is_some_and(|documents| {
            documents.supports_observation() && documents.supports_management()
        }) && self.write.is_some()
            && self.control.mutation().is_some()
            && self.control.staged_create().is_some()
            && self.control.cleanup_maintenance().is_some();
        if !complete {
            return Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "application-document management prerequisites are not installed",
            ));
        }
        Ok(())
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
/// Its pure recipe compiler validates static private facts before task-owned
/// resources are opened. Design: ADR-0156. The factory requires task-admitted
/// resources.
#[derive(Clone)]
pub struct ConnectorExecutionReadBinding {
    binding: ConnectorReadBinding,
    provider_factory: Arc<dyn ConnectorAdmittedReadProviderFactory>,
    decoder: Arc<dyn ConnectorReadWireDecoder>,
    recipe_compiler: Arc<dyn ConnectorReadRelationRecipeCompiler<Error = ConnectorCodecError>>,
}

impl ConnectorExecutionReadBinding {
    pub fn new(
        provider_factory: Arc<dyn ConnectorAdmittedReadProviderFactory>,
        decoder: Arc<dyn ConnectorReadWireDecoder>,
        recipe_compiler: Arc<dyn ConnectorReadRelationRecipeCompiler<Error = ConnectorCodecError>>,
    ) -> Self {
        Self {
            binding: provider_factory.binding().clone(),
            provider_factory,
            decoder,
            recipe_compiler,
        }
    }

    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub fn provider_factory(&self) -> Arc<dyn ConnectorAdmittedReadProviderFactory> {
        Arc::clone(&self.provider_factory)
    }

    pub fn decoder(&self) -> Arc<dyn ConnectorReadWireDecoder> {
        Arc::clone(&self.decoder)
    }

    /// Pure private validation from this exact installed read generation.
    pub fn recipe_compiler(
        &self,
    ) -> Arc<dyn ConnectorReadRelationRecipeCompiler<Error = ConnectorCodecError>> {
        Arc::clone(&self.recipe_compiler)
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
    handle_decoder: Arc<dyn ConnectorWriteHandleWireDecoder>,
    fragment_encoder: Arc<dyn ConnectorWriteFragmentWireEncoder>,
}

impl ConnectorExecutionWriteBinding {
    pub fn new(
        execution: Arc<dyn ConnectorWriteStackExecution>,
        handle_decoder: Arc<dyn ConnectorWriteHandleWireDecoder>,
        fragment_encoder: Arc<dyn ConnectorWriteFragmentWireEncoder>,
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

    pub fn handle_decoder(&self) -> Arc<dyn ConnectorWriteHandleWireDecoder> {
        Arc::clone(&self.handle_decoder)
    }

    pub fn fragment_encoder(&self) -> Arc<dyn ConnectorWriteFragmentWireEncoder> {
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
        if let Some(read) = &read {
            let binding = read.binding();
            if binding.catalog_handle() != properties.handle()
                || &binding.descriptor().provider_id != properties.provider_id()
                || binding.descriptor().instance_id != *properties.handle().catalog_name()
            {
                return Err(ConnectorError::new(
                    crate::connector::ConnectorErrorKind::InvalidRequest,
                    "execution read binding does not match normalized catalog generation",
                ));
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    };

    fn properties() -> NormalizedCatalogProperties {
        NormalizedCatalogProperties::try_new(
            CatalogProperties::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("catalog").unwrap(),
                    CatalogVersion::from_bytes([7; 32]),
                ),
                ConnectorProviderId::parse("iceberg").expect("static provider ID"),
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
            factory: &dyn crate::connector::ProviderExecutionRoleFactory,
            properties: &NormalizedCatalogProperties,
        ) {
            let _ = factory.bind(properties);
        }

        let _ = accepts_only_local_input;
        assert_eq!(
            properties().provider_id(),
            &ConnectorProviderId::parse("iceberg").expect("static provider ID")
        );
    }
}
