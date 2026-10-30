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

use super::document::{exhausted, invalid};
use super::observation::{
    ConnectorDocumentDiscoveryPage, ConnectorDocumentDiscoveryRequest,
    ConnectorDocumentStorageObservation, validate_observation_owner, validate_owner_key,
    validate_table,
};
use crate::connector::{
    CatalogHandle, ConnectorControlRuntimeId, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorMutationOperationId, ConnectorProviderBindingKey, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableObjectId, ConnectorWriteBaseVersion, LakePublicationId,
    ProviderBindingEpoch,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
const MAX_DOCUMENT_PROVIDER_TOKEN_BYTES: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorDocumentManagementOperation {
    Create,
    SingleTargetUpdate,
    Publication,
    Drop,
}

#[derive(Clone)]
pub struct ConnectorDocumentManagementAdmissionRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentManagementAdmissionRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        operation_id: ConnectorMutationOperationId,
        target: ConnectorTableIdentity,
        expected_object_id: Option<ConnectorTableObjectId>,
        operation: ConnectorDocumentManagementOperation,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_table(&target)?;
        if target.instance_id != owner.instance_id
            || catalog_handle.catalog_name() != &owner.instance_id
            || (operation == ConnectorDocumentManagementOperation::Create)
                == expected_object_id.is_some()
        {
            return Err(invalid(
                "document admission has an inconsistent owner, target, or object precondition",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            operation_id,
            target,
            expected_object_id,
            operation,
            context,
        })
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        validate_table(&self.target)?;
        if self.target.instance_id != self.owner.instance_id
            || self.catalog_handle.catalog_name() != &self.owner.instance_id
            || (self.operation == ConnectorDocumentManagementOperation::Create)
                == self.expected_object_id.is_some()
        {
            return Err(invalid(
                "document admission has an inconsistent owner, target, or object precondition",
            ));
        }
        Ok(())
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> Option<&ConnectorTableObjectId> {
        self.expected_object_id.as_ref()
    }
    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocumentManagementAdmission {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: ConnectorTableIdentity,
    expected_object_id: Option<ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    provider_token: Bytes,
    digest: [u8; 32],
}

impl ConnectorDocumentManagementAdmission {
    fn try_new(
        request: &ConnectorDocumentManagementAdmissionRequest,
        provider_token: Bytes,
    ) -> Result<Self, ConnectorError> {
        request.validate()?;
        validate_provider_token(&provider_token)?;
        let digest = admission_digest(request, &provider_token);
        Ok(Self {
            owner: request.owner.clone(),
            catalog_handle: request.catalog_handle.clone(),
            operation_id: request.operation_id,
            target: request.target.clone(),
            expected_object_id: request.expected_object_id.clone(),
            operation: request.operation,
            provider_token,
            digest,
        })
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.digest
            != admission_digest_fields(
                &self.owner,
                &self.catalog_handle,
                self.operation_id,
                &self.target,
                self.expected_object_id.as_ref(),
                self.operation,
                &self.provider_token,
            )
        {
            return Err(invalid("document admission digest is invalid"));
        }
        Ok(())
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }
    pub const fn expected_object_id(&self) -> Option<&ConnectorTableObjectId> {
        self.expected_object_id.as_ref()
    }
    pub const fn operation(&self) -> ConnectorDocumentManagementOperation {
        self.operation
    }
    pub const fn provider_token(&self) -> &Bytes {
        &self.provider_token
    }
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

impl std::fmt::Debug for ConnectorDocumentManagementAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocumentManagementAdmission")
            .field("owner", &self.owner)
            .field("catalog_handle", &self.catalog_handle)
            .field("operation_id", &self.operation_id)
            .field("target", &self.target)
            .field("expected_object_id", &self.expected_object_id)
            .field("operation", &self.operation)
            .field("provider_token_bytes", &self.provider_token.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentPublicationDeclaration {
    publication_id: LakePublicationId,
    admission: ConnectorDocumentManagementAdmission,
    target_object_id: ConnectorTableObjectId,
    expected_base: ConnectorWriteBaseVersion,
    technique: crate::connector::ConnectorManagedPublicationTechnique,
    empty_input: crate::connector::ConnectorManagedPublicationEmptyInputDisposition,
    partition_spec_replacement: Option<crate::connector::ConnectorManagedPartitionSpecReplacement>,
    expected_committed_partitioning: Option<crate::connector::ConnectorCommittedPartitioning>,
}

impl ConnectorDocumentPublicationDeclaration {
    pub fn try_new(
        publication_id: LakePublicationId,
        admission: ConnectorDocumentManagementAdmission,
        target_object_id: ConnectorTableObjectId,
        expected_base: ConnectorWriteBaseVersion,
        technique: crate::connector::ConnectorManagedPublicationTechnique,
        empty_input: crate::connector::ConnectorManagedPublicationEmptyInputDisposition,
        partition_spec_replacement: Option<
            crate::connector::ConnectorManagedPartitionSpecReplacement,
        >,
        expected_committed_partitioning: Option<crate::connector::ConnectorCommittedPartitioning>,
    ) -> Result<Self, ConnectorError> {
        admission.validate()?;
        expected_base.validate()?;
        if admission.operation() != ConnectorDocumentManagementOperation::Publication
            || admission.operation_id().to_bytes() != publication_id.to_bytes()
            || admission.expected_object_id() != Some(&target_object_id)
        {
            return Err(invalid(
                "document publication declaration does not match its admitted operation",
            ));
        }
        if partition_spec_replacement.is_some() != expected_committed_partitioning.is_some() {
            return Err(invalid(
                "document publication partition replacement is incomplete",
            ));
        }
        if let Some(replacement) = &partition_spec_replacement {
            if technique != crate::connector::ConnectorManagedPublicationTechnique::Full
                || empty_input
                    != crate::connector::ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
            {
                return Err(invalid(
                    "document partition replacement requires a full publication that commits empty input",
                ));
            }
            replacement.validate_for_operation(publication_id.into())?;
        }
        if let Some(partitioning) = &expected_committed_partitioning {
            partitioning.validate()?;
        }
        Ok(Self {
            publication_id,
            admission,
            target_object_id,
            expected_base,
            technique,
            empty_input,
            partition_spec_replacement,
            expected_committed_partitioning,
        })
    }

    pub const fn publication_id(&self) -> LakePublicationId {
        self.publication_id
    }
    pub const fn admission(&self) -> &ConnectorDocumentManagementAdmission {
        &self.admission
    }
    pub const fn target_object_id(&self) -> &ConnectorTableObjectId {
        &self.target_object_id
    }
    pub const fn expected_base(&self) -> &ConnectorWriteBaseVersion {
        &self.expected_base
    }
    pub const fn technique(&self) -> crate::connector::ConnectorManagedPublicationTechnique {
        self.technique
    }
    pub const fn empty_input(
        &self,
    ) -> crate::connector::ConnectorManagedPublicationEmptyInputDisposition {
        self.empty_input
    }
    pub const fn partition_spec_replacement(
        &self,
    ) -> Option<&crate::connector::ConnectorManagedPartitionSpecReplacement> {
        self.partition_spec_replacement.as_ref()
    }
    pub const fn expected_committed_partitioning(
        &self,
    ) -> Option<&crate::connector::ConnectorCommittedPartitioning> {
        self.expected_committed_partitioning.as_ref()
    }

    pub(crate) fn validate_for_write(
        &self,
        base: Option<&ConnectorWriteBaseVersion>,
    ) -> Result<(), ConnectorError> {
        if base != Some(&self.expected_base) {
            return Err(invalid(
                "document publication declaration does not match its exact write base",
            ));
        }
        Ok(())
    }
}

/// Provider-owned admission. Final effects remain on the existing create,
/// catalog-mutation and write-session owners.
pub trait ConnectorDocumentStorageManagement: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError>;
}
/// Independently optional FE document facets for one exact generation.
#[derive(Clone)]
pub struct ConnectorDocumentStorageBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
    management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
}

impl ConnectorDocumentStorageBinding {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
        management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
    ) -> Result<Self, ConnectorError> {
        if observation.is_none() && management.is_none() {
            return Err(invalid(
                "document storage binding must contain at least one real capability",
            ));
        }
        if let Some(capability) = &observation {
            validate_observation_owner(&descriptor, incarnation, capability.as_ref())?;
        }
        if let Some(capability) = &management
            && (capability.descriptor() != &descriptor || capability.incarnation() != incarnation)
        {
            return Err(invalid(
                "document management capability does not match its control generation",
            ));
        }
        Ok(Self {
            descriptor,
            incarnation,
            observation,
            management,
        })
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    pub(crate) fn observation(&self) -> Option<Arc<dyn ConnectorDocumentStorageObservation>> {
        self.observation.as_ref().map(Arc::clone)
    }
    pub(crate) fn management(&self) -> Option<Arc<dyn ConnectorDocumentStorageManagement>> {
        self.management.as_ref().map(Arc::clone)
    }

    pub(crate) const fn supports_management(&self) -> bool {
        self.management.is_some()
    }

    pub(crate) const fn supports_observation(&self) -> bool {
        self.observation.is_some()
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentStorageLease {
    control_runtime_id: ConnectorControlRuntimeId,
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    binding: ConnectorDocumentStorageBinding,
    _release: Arc<DocumentStorageLeaseRelease>,
}

struct DocumentStorageLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorDocumentStorageLease {
    pub(crate) fn new(
        descriptor: ConnectorInstanceDescriptor,
        control_runtime_id: ConnectorControlRuntimeId,
        incarnation: ProviderBindingEpoch,
        catalog_handle: CatalogHandle,
        binding: ConnectorDocumentStorageBinding,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        if binding.descriptor() != &descriptor
            || binding.incarnation() != incarnation
            || catalog_handle.catalog_name() != &descriptor.instance_id
        {
            return Err(invalid(
                "document storage binding does not match its retained control generation",
            ));
        }
        Ok(Self {
            control_runtime_id,
            owner: ConnectorProviderBindingKey {
                instance_id: descriptor.instance_id,
                incarnation,
            },
            catalog_handle,
            binding,
            _release: Arc::new(DocumentStorageLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub const fn control_runtime_id(&self) -> ConnectorControlRuntimeId {
        self.control_runtime_id
    }
    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn observation(&self) -> Result<Arc<dyn ConnectorDocumentStorageObservation>, ConnectorError> {
        self.binding.observation().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "connector control generation has no document observation capability",
            )
        })
    }

    pub fn observe_keyed_documents(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::KeyedDocumentView, super::KeyedDocumentReadError> {
        self.validate_keyed_request(&request)?;
        let view = self
            .observation()?
            .observe_keyed_documents(request.clone())?;
        view.validate_for(&request)?;
        Ok(view)
    }

    pub async fn observe_keyed_documents_async(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::KeyedDocumentView, super::KeyedDocumentReadError> {
        self.validate_keyed_request(&request)?;
        let view = self
            .observation()?
            .observe_keyed_documents_async(request.clone())
            .await?;
        view.validate_for(&request)?;
        Ok(view)
    }
    pub fn observe_full_keyed_documents(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::FullKeyedDocumentView, super::KeyedDocumentReadError> {
        self.validate_keyed_request(&request)?;
        let mut view = self
            .observation()?
            .observe_full_keyed_documents(request.clone())?;
        view.current.validate_for(&request)?;
        if view.token.owner() != &request.owner
            || view.token.table() != &request.target
            || view.token.object_id() != &request.object_id
            || view.token.metadata_version() != &view.current.metadata_version
        {
            return Err(
                invalid("full keyed observation token does not match its exact load").into(),
            );
        }
        view.token.seal();
        Ok(view)
    }

    pub async fn observe_full_keyed_documents_async(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::FullKeyedDocumentView, super::KeyedDocumentReadError> {
        self.validate_keyed_request(&request)?;
        let mut view = self
            .observation()?
            .observe_full_keyed_documents_async(request.clone())
            .await?;
        view.current.validate_for(&request)?;
        if view.token.owner() != &request.owner
            || view.token.table() != &request.target
            || view.token.object_id() != &request.object_id
            || view.token.metadata_version() != &view.current.metadata_version
        {
            return Err(
                invalid("full keyed observation token does not match its exact load").into(),
            );
        }
        view.token.seal();
        Ok(view)
    }
    pub fn decode_keyed_slot_change_evidence(
        &self,
        request: super::KeyedDocumentObservationRequest,
        evidence: crate::connector::ExternalMutationEvidence,
    ) -> Result<super::UnknownCommitEvidence, ConnectorError> {
        self.validate_keyed_request(&request)?;
        if evidence.descriptor() != self.binding.descriptor()
            || evidence.incarnation() != self.owner.incarnation
        {
            return Err(invalid(
                "slot-change evidence does not match its retained control generation",
            ));
        }
        self.observation()?
            .decode_keyed_slot_change_evidence(request, evidence)
    }
    pub fn resolve_keyed_commit(
        &self,
        request: super::KeyedDocumentObservationRequest,
        evidence: super::UnknownCommitEvidence,
    ) -> Result<super::CommitConclusion, ConnectorError> {
        self.validate_keyed_request(&request)?;
        self.observation()?.resolve_keyed_commit(request, evidence)
    }

    pub async fn resolve_keyed_commit_async(
        &self,
        request: super::KeyedDocumentObservationRequest,
        evidence: super::UnknownCommitEvidence,
    ) -> Result<super::CommitConclusion, ConnectorError> {
        self.validate_keyed_request(&request)?;
        self.observation()?
            .resolve_keyed_commit_async(request, evidence)
            .await
    }
    fn validate_keyed_request(
        &self,
        request: &super::KeyedDocumentObservationRequest,
    ) -> Result<(), ConnectorError> {
        request.validate()?;
        validate_owner_key(&self.owner, &request.owner)?;
        validate_catalog_handle(&self.catalog_handle, &request.catalog_handle)
    }

    pub fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let mut page = self.observation()?.discover_documents(request)?;
        page.validate_for(&retained_request)?;
        retained_request.reserve_page(page.items().len())?;
        page.seal_for(&retained_request);
        Ok(page)
    }

    fn management(&self) -> Result<Arc<dyn ConnectorDocumentStorageManagement>, ConnectorError> {
        self.binding.management().ok_or_else(|| {
            ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "connector control generation has no document management capability",
            )
        })
    }

    pub fn admit_management(
        &self,
        request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<ConnectorDocumentManagementAdmission, ConnectorError> {
        request.validate()?;
        validate_owner_key(&self.owner, request.owner())?;
        validate_catalog_handle(&self.catalog_handle, request.catalog_handle())?;
        let retained_request = request.clone();
        let provider_token = self.management()?.admit_management(request)?;
        ConnectorDocumentManagementAdmission::try_new(&retained_request, provider_token)
    }
}

impl Drop for DocumentStorageLeaseRelease {
    fn drop(&mut self) {
        if let Some(release) = self
            .release
            .lock()
            .expect("document storage lease release lock")
            .take()
        {
            release();
        }
    }
}

fn validate_provider_token(token: &Bytes) -> Result<(), ConnectorError> {
    if token.is_empty() || token.len() > MAX_DOCUMENT_PROVIDER_TOKEN_BYTES {
        return Err(exhausted(
            "document provider token is empty or exceeds its byte limit",
        ));
    }
    Ok(())
}

fn admission_digest(
    request: &ConnectorDocumentManagementAdmissionRequest,
    token: &Bytes,
) -> [u8; 32] {
    admission_digest_fields(
        &request.owner,
        &request.catalog_handle,
        request.operation_id,
        &request.target,
        request.expected_object_id.as_ref(),
        request.operation,
        token,
    )
}

fn admission_digest_fields(
    owner: &ConnectorProviderBindingKey,
    catalog_handle: &CatalogHandle,
    operation_id: ConnectorMutationOperationId,
    target: &ConnectorTableIdentity,
    expected_object_id: Option<&ConnectorTableObjectId>,
    operation: ConnectorDocumentManagementOperation,
    token: &Bytes,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-document-admission.v1\0");
    digest_bytes(&mut hasher, owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update(catalog_handle.version().as_bytes());
    hasher.update(operation_id.to_bytes());
    digest_bytes(&mut hasher, target.namespace.as_bytes());
    digest_bytes(&mut hasher, target.table.as_bytes());
    hasher.update([operation as u8]);
    match expected_object_id {
        Some(object_id) => {
            hasher.update([1]);
            digest_bytes(&mut hasher, object_id.as_bytes());
        }
        None => hasher.update([0]),
    }
    digest_bytes(&mut hasher, token);
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_catalog_handle(
    expected: &CatalogHandle,
    actual: &CatalogHandle,
) -> Result<(), ConnectorError> {
    if expected != actual {
        return Err(invalid(
            "document storage request does not match the retained catalog handle",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    include!("operation_tests.rs");
}
