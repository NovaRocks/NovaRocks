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

use super::document::{MAX_CONNECTOR_DOCUMENT_NAME_BYTES, exhausted, invalid, validate_token};
use crate::connector::{
    CatalogHandle, ConnectorCommittedVersion, ConnectorError, ConnectorInstanceDescriptor,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableObjectId, ProviderBindingEpoch,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
pub const MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE: usize = 128;
pub const MAX_CONNECTOR_DOCUMENT_DISCOVERY_CURSOR_BYTES: usize = 16 * 1024;
pub const MAX_CONNECTOR_DOCUMENT_DISCOVERY_ITEMS: usize = 4096;

/// Shared item allowance for one discovery, including all continuation requests.
/// This bounds catalog enumeration only; inline document byte limits are separate.
#[derive(Clone)]
pub struct ConnectorDocumentDiscoveryBudget {
    remaining: Arc<Mutex<usize>>,
}
impl ConnectorDocumentDiscoveryBudget {
    pub fn try_new(max_items: usize) -> Result<Self, ConnectorError> {
        if max_items == 0 || max_items > MAX_CONNECTOR_DOCUMENT_DISCOVERY_ITEMS {
            return Err(invalid("document discovery item bound is invalid"));
        }
        Ok(Self {
            remaining: Arc::new(Mutex::new(max_items)),
        })
    }
    fn remaining_items(&self) -> Result<usize, ConnectorError> {
        Ok(*self
            .remaining
            .lock()
            .expect("document discovery budget lock"))
    }
    fn reserve_discovery(&self, items: usize) -> Result<(), ConnectorError> {
        let mut remaining = self
            .remaining
            .lock()
            .expect("document discovery budget lock");
        *remaining = remaining
            .checked_sub(items)
            .ok_or_else(|| exhausted("document discovery exceeds its cumulative item budget"))?;
        Ok(())
    }
}
impl Default for ConnectorDocumentDiscoveryBudget {
    fn default() -> Self {
        Self::try_new(MAX_CONNECTOR_DOCUMENT_DISCOVERY_ITEMS).expect("valid discovery item bound")
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorManagedObjectMarker {
    kind: Arc<str>,
    owner: Arc<str>,
    incarnation: Arc<str>,
}

impl ConnectorManagedObjectMarker {
    pub fn try_new(
        kind: impl AsRef<str>,
        owner: impl AsRef<str>,
        incarnation: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        validate_token(
            kind.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object kind",
        )?;
        validate_token(
            owner.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object owner",
        )?;
        validate_token(
            incarnation.as_ref(),
            MAX_CONNECTOR_DOCUMENT_NAME_BYTES,
            "managed object incarnation",
        )?;
        Ok(Self {
            kind: Arc::from(kind.as_ref()),
            owner: Arc::from(owner.as_ref()),
            incarnation: Arc::from(incarnation.as_ref()),
        })
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

#[derive(Clone)]
pub struct ConnectorDocumentDiscoveryRequest {
    owner: ConnectorProviderBindingKey,
    catalog_handle: CatalogHandle,
    namespace: Option<Arc<str>>,
    cursor: Option<Bytes>,
    page_size: usize,
    remaining_items: usize,
    budget: ConnectorDocumentDiscoveryBudget,
    context: ConnectorRequestContext,
}

impl ConnectorDocumentDiscoveryRequest {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        catalog_handle: CatalogHandle,
        namespace: Option<Arc<str>>,
        page_size: usize,
        budget: ConnectorDocumentDiscoveryBudget,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let max_items = budget.remaining_items()?;
        if max_items == 0 {
            return Err(exhausted(
                "document discovery has no remaining operation item budget",
            ));
        }
        if catalog_handle.catalog_name() != &owner.instance_id
            || namespace.as_ref().is_some_and(|value| value.is_empty())
            || page_size == 0
            || page_size > MAX_CONNECTOR_DOCUMENT_DISCOVERY_PAGE_SIZE
        {
            return Err(invalid(
                "document discovery scope, cursor, or item budget is invalid",
            ));
        }
        Ok(Self {
            owner,
            catalog_handle,
            namespace,
            cursor: None,
            page_size: page_size.min(max_items),
            remaining_items: max_items,
            budget,
            context,
        })
    }

    fn next_from(
        previous: &Self,
        cursor: Bytes,
        remaining_items: usize,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        if cursor.is_empty()
            || cursor.len() > MAX_CONNECTOR_DOCUMENT_DISCOVERY_CURSOR_BYTES
            || remaining_items == 0
            || remaining_items >= previous.remaining_items
        {
            return Err(invalid(
                "document discovery continuation does not reduce the original budget",
            ));
        }
        Ok(Self {
            owner: previous.owner.clone(),
            catalog_handle: previous.catalog_handle.clone(),
            namespace: previous.namespace.clone(),
            cursor: Some(cursor),
            page_size: previous.page_size.min(remaining_items),
            remaining_items,
            budget: previous.budget.clone(),
            context,
        })
    }

    pub const fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
    pub const fn namespace(&self) -> Option<&Arc<str>> {
        self.namespace.as_ref()
    }
    pub const fn cursor(&self) -> Option<&Bytes> {
        self.cursor.as_ref()
    }
    pub const fn page_size(&self) -> usize {
        self.page_size
    }
    pub const fn remaining_items(&self) -> usize {
        self.remaining_items
    }
    pub const fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub(crate) fn request_digest(&self) -> [u8; 32] {
        discovery_request_digest(self)
    }

    pub(crate) fn reserve_page(&self, items: usize) -> Result<(), ConnectorError> {
        self.budget.reserve_discovery(items)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentDiscoveryItem {
    target: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    marker: ConnectorManagedObjectMarker,
}

impl ConnectorDocumentDiscoveryItem {
    pub fn try_new(
        target: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        metadata_version: ConnectorCommittedVersion,
        marker: ConnectorManagedObjectMarker,
    ) -> Result<Self, ConnectorError> {
        validate_table(&target)?;
        metadata_version.validate()?;
        Ok(Self {
            target,
            object_id,
            metadata_version,
            marker,
        })
    }

    pub const fn target(&self) -> &ConnectorTableIdentity {
        &self.target
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }

    pub const fn marker(&self) -> &ConnectorManagedObjectMarker {
        &self.marker
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDocumentDiscoveryIncompleteReason {
    PageBoundary,
    ItemBudget,
    ProviderLimit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorDocumentDiscoveryCompleteness {
    Complete,
    Incomplete(ConnectorDocumentDiscoveryIncompleteReason),
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorDocumentDiscoveryPage {
    items: Vec<ConnectorDocumentDiscoveryItem>,
    next_cursor: Option<Bytes>,
    completeness: ConnectorDocumentDiscoveryCompleteness,
    request_digest: [u8; 32],
    lease_witness: Option<[u8; 32]>,
}

impl std::fmt::Debug for ConnectorDocumentDiscoveryPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorDocumentDiscoveryPage")
            .field("items", &self.items)
            .field(
                "next_cursor_bytes",
                &self.next_cursor.as_ref().map(Bytes::len),
            )
            .field("completeness", &self.completeness)
            .finish()
    }
}

impl ConnectorDocumentDiscoveryPage {
    pub fn try_new(
        items: Vec<ConnectorDocumentDiscoveryItem>,
        next_cursor: Option<Bytes>,
        completeness: ConnectorDocumentDiscoveryCompleteness,
        request: &ConnectorDocumentDiscoveryRequest,
    ) -> Result<Self, ConnectorError> {
        if items.len() > request.page_size() || items.len() > request.remaining_items() {
            return Err(exhausted(
                "document discovery page exceeds the caller budget",
            ));
        }
        if next_cursor.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > MAX_CONNECTOR_DOCUMENT_DISCOVERY_CURSOR_BYTES
        }) {
            return Err(invalid("document discovery continuation cursor is invalid"));
        }
        if next_cursor.is_some() && next_cursor.as_ref() == request.cursor() {
            return Err(invalid(
                "document discovery continuation cursor made no progress",
            ));
        }
        if matches!(
            completeness,
            ConnectorDocumentDiscoveryCompleteness::Complete
        ) != next_cursor.is_none()
        {
            return Err(invalid(
                "document discovery completeness and continuation disagree",
            ));
        }
        if items.is_empty()
            && matches!(
                completeness,
                ConnectorDocumentDiscoveryCompleteness::Incomplete(_)
            )
        {
            return Err(invalid(
                "incomplete document discovery page must make item progress",
            ));
        }
        let mut identities = HashSet::with_capacity(items.len());
        if items
            .iter()
            .any(|item| !identities.insert((item.target(), item.object_id())))
        {
            return Err(invalid(
                "document discovery page contains duplicate object identities",
            ));
        }
        Ok(Self {
            items,
            next_cursor,
            completeness,
            request_digest: request.request_digest(),
            lease_witness: None,
        })
    }

    pub(crate) fn validate_for(
        &self,
        request: &ConnectorDocumentDiscoveryRequest,
    ) -> Result<(), ConnectorError> {
        if self.request_digest != request.request_digest()
            || self.items.len() > request.page_size()
            || self.items.len() > request.remaining_items()
            || matches!(
                self.completeness,
                ConnectorDocumentDiscoveryCompleteness::Complete
            ) != self.next_cursor.is_none()
        {
            return Err(invalid(
                "document discovery reply does not match the original request budget",
            ));
        }
        for item in &self.items {
            if item.target().instance_id != request.owner().instance_id
                || request
                    .namespace()
                    .is_some_and(|namespace| item.target().namespace.as_ref() != namespace.as_ref())
            {
                return Err(invalid(
                    "document discovery reply escaped the original catalog or namespace scope",
                ));
            }
            validate_table(item.target())?;
            item.metadata_version().validate()?;
        }
        Ok(())
    }

    pub(crate) fn seal_for(&mut self, request: &ConnectorDocumentDiscoveryRequest) {
        self.lease_witness = Some(request.request_digest());
    }

    pub fn try_next_request(
        &self,
        previous: &ConnectorDocumentDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Option<ConnectorDocumentDiscoveryRequest>, ConnectorError> {
        if self.lease_witness != Some(previous.request_digest()) {
            return Err(invalid(
                "document discovery page was not validated by its exact storage lease",
            ));
        }
        if matches!(
            self.completeness,
            ConnectorDocumentDiscoveryCompleteness::Complete
        ) {
            return Ok(None);
        }
        let remaining_from_page = previous
            .remaining_items()
            .checked_sub(self.items.len())
            .ok_or_else(|| exhausted("document discovery item budget underflowed"))?;
        let remaining = remaining_from_page.min(previous.budget.remaining_items()?);
        if remaining == 0 {
            if matches!(
                self.completeness,
                ConnectorDocumentDiscoveryCompleteness::Incomplete(
                    ConnectorDocumentDiscoveryIncompleteReason::ItemBudget
                )
            ) {
                return Ok(None);
            }
            return Err(invalid(
                "document discovery exhausted its item budget without an item-budget result",
            ));
        }
        let cursor = self.next_cursor.clone().ok_or_else(|| {
            invalid("incomplete document discovery page omitted its continuation cursor")
        })?;
        ConnectorDocumentDiscoveryRequest::next_from(previous, cursor, remaining, context).map(Some)
    }

    pub fn items(&self) -> &[ConnectorDocumentDiscoveryItem] {
        &self.items
    }
    pub const fn next_cursor(&self) -> Option<&Bytes> {
        self.next_cursor.as_ref()
    }
    pub const fn completeness(&self) -> ConnectorDocumentDiscoveryCompleteness {
        self.completeness
    }
}

pub trait ConnectorDocumentStorageObservation: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn observe_keyed_documents(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::KeyedDocumentView, super::KeyedDocumentReadError>;
    fn observe_full_keyed_documents(
        &self,
        request: super::KeyedDocumentObservationRequest,
    ) -> Result<super::FullKeyedDocumentView, super::KeyedDocumentReadError>;
    fn observe_keyed_documents_async(
        &self,
        _request: super::KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<super::KeyedDocumentView, super::KeyedDocumentReadError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "document observation capability has no asynchronous keyed observation",
            )
            .into())
        })
    }
    fn observe_full_keyed_documents_async(
        &self,
        _request: super::KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<super::FullKeyedDocumentView, super::KeyedDocumentReadError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "document observation capability has no asynchronous full keyed observation",
            )
            .into())
        })
    }
    fn resolve_keyed_commit_async(
        &self,
        _request: super::KeyedDocumentObservationRequest,
        _evidence: super::UnknownCommitEvidence,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<super::CommitConclusion, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async {
            Err(ConnectorError::new(
                crate::connector::ConnectorErrorKind::Unsupported,
                "document observation capability has no asynchronous commit resolution",
            ))
        })
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        request: super::KeyedDocumentObservationRequest,
        evidence: crate::connector::ExternalMutationEvidence,
    ) -> Result<super::UnknownCommitEvidence, ConnectorError>;
    fn resolve_keyed_commit(
        &self,
        request: super::KeyedDocumentObservationRequest,
        evidence: super::UnknownCommitEvidence,
    ) -> Result<super::CommitConclusion, ConnectorError>;
    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError>;
}
pub(crate) fn validate_observation_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    capability: &dyn ConnectorDocumentStorageObservation,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(invalid(
            "document observation capability does not match its control generation",
        ));
    }
    Ok(())
}

pub(crate) fn validate_table(table: &ConnectorTableIdentity) -> Result<(), ConnectorError> {
    if table.namespace.is_empty() || table.table.is_empty() {
        return Err(invalid(
            "document storage target must name a namespace and table",
        ));
    }
    Ok(())
}

pub(crate) fn validate_owner_key(
    expected: &ConnectorProviderBindingKey,
    actual: &ConnectorProviderBindingKey,
) -> Result<(), ConnectorError> {
    if expected != actual {
        return Err(invalid(
            "document storage request does not match the retained control generation",
        ));
    }
    Ok(())
}

fn discovery_request_digest(request: &ConnectorDocumentDiscoveryRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.connector-document-discovery.v1\0");
    digest_bytes(&mut hasher, request.owner().instance_id.as_str().as_bytes());
    hasher.update(request.owner().incarnation.to_bytes());
    hasher.update(request.catalog_handle().version().as_bytes());
    digest_optional_bytes(
        &mut hasher,
        request.namespace().map(|value| value.as_bytes()),
    );
    digest_optional_bytes(&mut hasher, request.cursor().map(Bytes::as_ref));
    hasher.update((request.page_size() as u64).to_be_bytes());
    hasher.update((request.remaining_items() as u64).to_be_bytes());
    hasher.finalize().into()
}

fn digest_optional_bytes(hasher: &mut Sha256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            digest_bytes(hasher, value);
        }
        None => hasher.update([0]),
    }
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}
