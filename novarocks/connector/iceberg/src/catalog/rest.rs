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

//! The REST catalog implementation.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorListingBound, ConnectorListingBudget,
    ConnectorListingCollector,
};

use super::admission::{CatalogAdmissionTarget, CatalogOperation};
use super::error::CatalogOutcome;
use super::{
    CatalogCreateIntent, CatalogNamespaceName, CatalogTableName, CatalogTablePage, NovaRocksCatalog,
};
use crate::catalog_runtime::RestAccessDelegationMode;
use crate::loaded_table::{IcebergAccessDelegation, IcebergLoadedTable, parse_rest_delegation};
use crate::rest_protocol::boundary::sanitize_upstream_error;
use crate::rest_protocol::endpoints::RestEndpoints;
use crate::rest_protocol::evidence::{AttributionExpectation, ExpectedSnapshot};

/// A REST Iceberg catalog.
///
/// This is the only implementation that can satisfy a standard staged CTAS,
/// and even here that is a per-request question: staged creation needs an
/// explicit warehouse so the unanchored staging namespace is enumerable and
/// therefore collectable. A REST catalog configured without one can still
/// serve reads, DDL, and ordinary writes; it just cannot run CTAS safely, and
/// says so before the source executes.
#[derive(Debug)]
pub(super) struct NovaRocksRestCatalog {
    /// Delegates generic owner methods to the generation's one transitional
    /// adapter, whose mutation failures carry typed evidence.
    evidence_policy: super::publication::CatalogEvidencePolicy,
    listing: Arc<super::listing_admission::ListingAdmission>,
    /// NovaRocks requests sharing the upstream client's one session (L27).
    endpoints: Arc<RestEndpoints>,
    warehouse: Option<Arc<str>>,
    access_delegation: RestAccessDelegationMode,
}

impl NovaRocksRestCatalog {
    pub(super) fn with_evidence_policy(
        mut self,
        policy: super::publication::CatalogEvidencePolicy,
    ) -> Self {
        self.evidence_policy = policy;
        self
    }

    pub(super) fn new(
        endpoints: Arc<RestEndpoints>,
        warehouse: Option<Arc<str>>,
        access_delegation: RestAccessDelegationMode,
    ) -> Self {
        Self {
            evidence_policy: Default::default(),
            listing: Default::default(),
            endpoints,
            warehouse,
            access_delegation,
        }
    }

    /// Admission for the staged-create path.
    ///
    /// Returns the proven warehouse root, or the refusal to report before any
    /// side effect.
    fn admit_staged_create(&self, intent: CatalogCreateIntent) -> Result<Arc<str>, String> {
        match &self.warehouse {
            Some(warehouse) if !warehouse.is_empty() => Ok(Arc::clone(warehouse)),
            _ => Err(format!(
                "REST Iceberg catalog has no explicit warehouse, so {} cannot stage its target \
                 where the unanchored staging namespace stays enumerable and collectable",
                intent.as_str()
            )),
        }
    }
}

pub(crate) fn preflight_rest_page<'a>(
    collector: &ConnectorListingCollector<String>,
    names: impl IntoIterator<Item = &'a str>,
    next: Option<&str>,
    seen: &std::collections::HashSet<String>,
) -> Result<(), ConnectorError> {
    let bound = collector.bound();
    let exhausted = |field, limit| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("connector listing refused: it exceeds the {field} bound of {limit}"),
        )
    };
    if collector.pages() >= bound.pages || (next.is_some() && collector.pages() + 1 >= bound.pages)
    {
        return Err(exhausted("pages", bound.pages));
    }
    if let Some(token) = next {
        if token.is_empty() || seen.contains(token) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg REST catalog returned an empty or repeated continuation token",
            ));
        }
        if token.len() > bound.continuation_token_bytes {
            return Err(exhausted(
                "continuation_token_bytes",
                bound.continuation_token_bytes,
            ));
        }
    }
    let mut page_budget = ConnectorListingBudget::new(ConnectorListingBound {
        entries: bound.entries - collector.len(),
        total_name_bytes: bound.total_name_bytes - collector.total_name_bytes(),
        ..bound
    })?;
    page_budget.admit_names(names)?;
    if next.is_some() && page_budget.entries() > bound.page_entries {
        return Err(exhausted("page_entries", bound.page_entries));
    }
    Ok(())
}

/// Admit a REST response before retaining any of its names or following its
/// token. Non-paging servers may return a larger final response within the
/// whole-listing bound; continuing oversized pages remain invalid.
pub(crate) fn accept_rest_page(
    collector: &mut ConnectorListingCollector<String>,
    names: Vec<String>,
    next: Option<String>,
    seen: &mut std::collections::HashSet<String>,
) -> Result<(), ConnectorError> {
    if next.as_ref().is_some_and(|token| seen.contains(token)) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg REST catalog repeated a continuation token",
        ));
    }
    let bound = collector.bound();
    ConnectorListingBudget::new(ConnectorListingBound {
        entries: bound.entries - collector.len(),
        total_name_bytes: bound.total_name_bytes - collector.total_name_bytes(),
        ..bound
    })?
    .admit_names(names.iter().map(String::as_str))?;

    if names.len() > bound.page_entries && next.is_none() {
        collector.accept_page(Vec::new(), None)?;
        for name in names {
            collector.push(name)?;
        }
    } else {
        collector.accept_page(names, next)?;
    }
    if let Some(token) = collector.continuation_token() {
        seen.insert(token.to_string());
    }
    Ok(())
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksRestCatalog {
    fn materialize_staged_create_response(
        &self,
        table: CatalogTableName,
        document: Vec<u8>,
    ) -> Result<super::StagedCreateMaterial, ConnectorError> {
        let invalid = |message: &str| ConnectorError::new(ConnectorErrorKind::CorruptData, message);
        let response =
            serde_json::from_slice::<crate::rest_protocol::wire::LoadTableResult>(&document)
                .map_err(|_| invalid("REST stage-create response could not be decoded"))?;
        let initialization_updates =
            crate::commit::staging::staged_create_updates(&response.metadata)
                .map_err(|_| invalid("REST stage-create response has invalid initial metadata"))?;
        let ident = super::delegate::table_ident(&table)?;
        let (frozen, delegation) = crate::rest_protocol::deferred::FrozenLoadTable::from_response(
            ident,
            response,
            self.endpoints.runtime().clone(),
        );
        let (table, access_delegation) = match self.access_delegation {
            RestAccessDelegationMode::Static => {
                let file_io = self
                    .endpoints
                    .static_file_io()
                    .ok_or_else(|| invalid("REST static generation has no storage factory"))?;
                let table = frozen
                    .materialize(file_io)
                    .map_err(|_| invalid("REST staged table could not be materialized"))?;
                (
                    crate::loaded_table::IcebergLoadedTableMaterialization::Materialized(table),
                    IcebergAccessDelegation::static_binding(),
                )
            }
            RestAccessDelegationMode::Vended => (
                crate::loaded_table::IcebergLoadedTableMaterialization::DeferredRest(Box::new(
                    frozen,
                )),
                parse_rest_delegation(delegation)
                    .map_err(|_| invalid("REST staged delegation could not be decoded"))?,
            ),
        };
        Ok(super::StagedCreateMaterial {
            table,
            initialization_updates,
            access_delegation,
        })
    }

    fn freeze_create_publication(
        &self,
        request: super::catalog_request::CatalogCreatePublicationRequest,
    ) -> super::publication_dispatch::PreparationResult<
        super::catalog_request::FrozenCatalogPublication,
    > {
        let target = CatalogTableName::new(
            Arc::clone(&request.namespace.namespace),
            request.creation.name.clone(),
        );
        self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            &target.into(),
        )
        .map_err(|reason| {
            super::publication_dispatch::PreparationFailure(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    reason.message(),
                ),
            )
        })?;
        Ok(
            super::catalog_request::CatalogPublicationRequest::CreateTable {
                namespace: request.namespace,
                creation: request.creation,
                no_op_if_exists: request.no_op_if_exists,
            }
            .freeze(request.operation_id)?,
        )
    }

    async fn prepare_catalog_publication(
        &self,
        request: super::catalog_request::FrozenCatalogPublication,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
        document: super::catalog_request::CatalogResponseDocument,
        _read_binding: crate::access_binding::IcebergReadBinding,
    ) -> super::publication_dispatch::PreparationResult<
        super::publication_dispatch::PreparedPublication,
    > {
        use novarocks_spi::connector::ConnectorOperationControl;
        preparation.check_active()?;
        self.endpoints.publication_generation.require(generation)?;
        request.validate_descriptor(&descriptor)?;
        let (route, expectation) = request.rest_protocol()?;
        self.admit_operation(&request.operation, &request.target)
            .map_err(|reason| {
                super::publication_dispatch::PreparationFailure(
                    novarocks_spi::connector::ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                        reason.message(),
                    ),
                )
            })?;
        if matches!(
            request.operation,
            CatalogOperation::CreateNamespace | CatalogOperation::DropNamespace
        ) {
            let CatalogAdmissionTarget::Namespace(namespace) = &request.target else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Namespace publication requires a namespace target",
                )
                .into());
            };
            let exists = self
                .listing
                .run(&preparation, self.namespace_exists(namespace.clone()))
                .await?;
            if request.namespace_no_op(exists)? {
                return Ok(super::publication_dispatch::PreparedPublication::NoOp);
            }
        }
        if request.stage_create {
            let CatalogAdmissionTarget::Table(table) = &request.target else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Staged create requires a table target",
                )
                .into());
            };
            let namespace = CatalogNamespaceName::new(Arc::clone(&table.namespace));
            if !self
                .listing
                .run(&preparation, self.namespace_exists(namespace))
                .await?
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Staged create namespace does not exist",
                )
                .into());
            }
        }
        if request.operation == CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable)
            && self.access_delegation == RestAccessDelegationMode::Vended
        {
            return Err(super::publication_dispatch::PreparationFailure(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    "REST ordinary create cannot hand off vended storage credentials; use staged create",
                ),
            ));
        }
        let prepared = crate::rest_protocol::mutation::prepared::PreparedRestMutation::prepare(
            Arc::clone(&self.endpoints),
            route,
            &descriptor,
            generation,
            &preparation,
            expectation,
            request.stage_create,
            request
                .stage_create
                .then_some(crate::rest_protocol::endpoints::VENDED_CREDENTIALS),
        )
        .await?;
        if request.stage_create && self.endpoints.is_vended() {
            let binding = self.endpoints.established_binding().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Prepared REST stage has no established session",
                )
            })?;
            crate::rest_protocol::routing::preflight_route(
                &binding,
                &self.endpoints.route_admission(),
            )
            .map_err(|error| {
                super::publication_dispatch::PreparationFailure(
                    novarocks_spi::connector::ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                        error.message(),
                    ),
                )
            })?;
        }
        preparation.check_active()?;
        Ok(super::publication_dispatch::PreparedPublication::Mutation {
            binding: Some(prepared.binding().clone()),
            dispatch: Box::new(PreparedRestCatalogPublication { prepared, document }),
        })
    }

    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        self.endpoints.publication_generation.bind(generation)
    }

    async fn prepare_frozen_publication(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: super::admission::CatalogOperation,
        staged_expectation: Option<super::StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> super::publication_dispatch::PreparationResult<
        super::publication_dispatch::PreparedPublication,
    > {
        novarocks_spi::connector::ConnectorOperationControl::check_active(&preparation)?;
        self.endpoints.publication_generation.require(generation)?;
        descriptor.validate_frozen(&request)?;
        self.admit_operation(
            &operation,
            &CatalogTableName::from_identifier(request.identifier()).into(),
        )
        .map_err(|error| {
            super::publication_dispatch::PreparationFailure(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ),
            )
        })?;
        use crate::commit::model::BaseIdentity;
        let snapshot = request
            .ref_snapshot_after(request.target_ref())
            .map(|snapshot| ExpectedSnapshot::new(snapshot, Some(request.target_ref().to_owned())));
        let expect = match request.base() {
            BaseIdentity::Existing { uuid, .. } => {
                if staged_expectation.is_some() {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Existing table publication cannot use staged attribution",
                    )
                    .into());
                }
                if !request.has_updates() {
                    return Ok(super::publication_dispatch::PreparedPublication::NoOp);
                }
                if request.shape() == crate::commit::model::RequestShape::RefRetention {
                    AttributionExpectation::ref_retention(*uuid, request.updates())
                } else {
                    AttributionExpectation::table_commit(*uuid, snapshot)
                }
            }
            BaseIdentity::Create { staged } => {
                if operation
                    != super::admission::CatalogOperation::CreateTable(
                        super::CatalogCreateIntent::CreateTableAsSelect,
                    )
                {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Staged publication requires its actual create-table-as-select operation",
                    )
                    .into());
                }
                let expected = staged_expectation.ok_or_else(|| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Staged publication has no exact frozen attribution expectation",
                    )
                })?;
                let mut frozen_properties = std::collections::BTreeMap::new();
                for update in request.updates() {
                    match update {
                        crate::iceberg::TableUpdate::SetProperties { updates } => frozen_properties
                            .extend(
                                updates
                                    .iter()
                                    .map(|(key, value)| (key.clone(), value.clone())),
                            ),
                        crate::iceberg::TableUpdate::RemoveProperties { removals } => {
                            for key in removals {
                                frozen_properties.remove(key);
                            }
                        }
                        _ => {}
                    }
                }
                if expected
                    .properties
                    .iter()
                    .any(|(key, value)| frozen_properties.get(key) != Some(value))
                {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Staged attribution properties are absent from the exact frozen request",
                    )
                    .into());
                }
                if expected.staged_table_uuid != staged.initial_metadata().uuid()
                    || expected.snapshot_id != request.ref_snapshot_after(request.target_ref())
                    || !expected
                        .properties
                        .iter()
                        .any(|(key, _)| key == "novarocks.ctas.operation-id")
                {
                    return Err(crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Staged publication attribution differs from its frozen request",
                    )
                    .into());
                }
                AttributionExpectation::ctas_final_commit(
                    expected.staged_table_uuid,
                    snapshot,
                    expected.properties,
                )
            }
        }
        .map_err(|_| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Frozen REST publication attribution cannot be encoded",
            )
        })?;
        let prepared = crate::rest_protocol::mutation::prepared::PreparedRestMutation::prepare(
            Arc::clone(&self.endpoints),
            crate::rest_protocol::mutation::prepared::MutationRoute::CommitTable(
                request.identifier().clone(),
            ),
            &descriptor,
            generation,
            &preparation,
            expect,
            false,
            None,
        )
        .await?;
        Ok(super::publication_dispatch::PreparedPublication::Mutation {
            binding: Some(prepared.binding().clone()),
            dispatch: Box::new(PreparedRestTablePublication(prepared)),
        })
    }

    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        novarocks_spi::connector::ExternalMutationFinalization::Complete
    }

    fn evidence_policy(&self) -> super::publication::CatalogEvidencePolicy {
        self.evidence_policy
    }

    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "rest"
    }

    fn vended_credential_refresh_catalog(&self) -> Option<Arc<RestEndpoints>> {
        Some(Arc::clone(&self.endpoints))
    }

    fn admit_operation(
        &self,
        operation: &CatalogOperation,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), super::error::CatalogUnsupported> {
        operation.validate_target(target)?;
        match operation {
            CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect)
            | CatalogOperation::CreateDocuments => self
                .admit_staged_create(CatalogCreateIntent::CreateTableAsSelect)
                .map(|_| ())
                .map_err(super::error::CatalogUnsupported::new),
            _ => Ok(()),
        }
    }

    async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        let mut collector = ConnectorListingCollector::new(bound)?;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self
                .endpoints
                .list_namespaces_page(collector.continuation_token(), bound.page_entries)
                .await?;
            preflight_rest_page(
                &collector,
                page.namespaces
                    .iter()
                    .flat_map(|ident| ident.as_ref().iter().map(String::as_str)),
                page.next_page_token.as_deref(),
                &seen,
            )?;
            let names = page
                .namespaces
                .iter()
                .flat_map(|ident| ident.as_ref().iter().map(|name| name.to_owned()))
                .collect();
            accept_rest_page(&mut collector, names, page.next_page_token, &mut seen)?;
            if collector.continuation_token().is_none() {
                break;
            }
        }
        Ok(super::delegate::sorted_unique(
            collector
                .finish()?
                .into_iter()
                .filter(|name| !name.starts_with('.'))
                .collect(),
        ))
    }

    async fn list_namespaces_for_read(
        &self,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "external catalog listing requires an admitted request context",
            )
        })?;
        self.listing.run(context, self.list_namespaces(bound)).await
    }

    async fn list_tables_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "external catalog listing requires an admitted request context",
            )
        })?;
        self.listing
            .run(context, self.list_tables(namespace, bound))
            .await
    }

    async fn list_views_for_request(
        &self,
        namespace: CatalogNamespaceName,
        context: novarocks_spi::connector::ConnectorRequestContext,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.listing
            .run(&context, self.list_views(namespace, bound))
            .await
    }

    async fn list_views_with_metadata_cpu(
        &self,
        source: Arc<crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal>,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorViewIdentity>, ConnectorError> {
        let context = source.view_listing_context();
        // This is the SAME original eight-position whole-listing owner. The
        // SDK/CPU future exits before run returns its original position.
        self.listing
            .run_with_original(
                &context,
                crate::catalog_control::views::async_listing::run(
                    self.endpoints.clone(),
                    source.clone(),
                    cpu,
                ),
                source.as_ref(),
            )
            .await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        let ident = super::delegate::namespace_ident(&namespace)?;
        self.endpoints.namespace_exists(&ident).await
    }

    async fn namespace_exists_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Namespace existence requires an admitted request context",
            )
        })?;
        self.listing
            .run(context, self.namespace_exists(namespace))
            .await
    }

    /// Request bounded REST pages, checking every response before retention
    /// and continuation. An oversized final response from a server ignoring
    /// pageSize is admitted only within the remaining whole-listing bound.
    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        let ident = super::delegate::namespace_ident(&namespace)?;
        let mut collector = ConnectorListingCollector::new(bound)?;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self
                .endpoints
                .list_tables_page(&ident, collector.continuation_token(), bound.page_entries)
                .await?;
            // The SDK owns this response. Validate every borrowed name and the
            // continuation before allocating any NovaRocks-owned page vector.
            preflight_rest_page(
                &collector,
                page.identifiers.iter().map(|table| table.name.as_str()),
                page.next_page_token.as_deref(),
                &seen,
            )?;
            let mut identifiers = std::collections::HashSet::with_capacity(page.identifiers.len());
            if page.identifiers.iter().any(|table| {
                table.namespace() != &ident || !identifiers.insert(table.name.as_str())
            }) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg REST catalog returned a duplicate or out-of-scope table identifier",
                ));
            }
            let names = page
                .identifiers
                .iter()
                .map(|table| table.name.to_owned())
                .collect();
            accept_rest_page(&mut collector, names, page.next_page_token, &mut seen)?;
            if collector.continuation_token().is_none() {
                break;
            }
        }
        Ok(super::delegate::sorted_unique(collector.finish()?))
    }

    async fn list_tables_page(
        &self,
        namespace: CatalogNamespaceName,
        page_token: Option<Arc<str>>,
        page_size: usize,
    ) -> Result<CatalogTablePage, ConnectorError> {
        let ident = super::delegate::namespace_ident(&namespace)?;
        let page = self
            .endpoints
            .list_tables_page(&ident, page_token.as_deref(), page_size)
            .await?;
        // A server without pagination support may ignore pageSize and return
        // a complete listing. A continuing oversized page still violates the
        // page contract; every response must fit the frozen whole-listing bound.
        if page.identifiers.len() > page_size && page.next_page_token.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                format!(
                    "connector listing refused: Iceberg REST catalog returned more tables than \
                     the page_entries bound of {page_size}"
                ),
            ));
        }
        ConnectorListingBudget::new(ConnectorListingBound::V1)?
            .admit_names(page.identifiers.iter().map(|table| table.name.as_str()))?;
        if let Some(token) = page.next_page_token.as_deref() {
            if token.is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg REST catalog returned an empty continuation token",
                ));
            }
            if token.len() > ConnectorListingBound::V1.continuation_token_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector listing refused: continuation_token_bytes bound exceeded",
                ));
            }
        }

        let mut seen = std::collections::HashSet::with_capacity(page.identifiers.len());
        if page
            .identifiers
            .iter()
            .any(|table| table.namespace() != &ident || !seen.insert(table.name.as_str()))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg REST catalog returned a duplicate or out-of-scope table identifier",
            ));
        }
        Ok(CatalogTablePage {
            tables: page
                .identifiers
                .into_iter()
                .map(|table| CatalogTableName::new(namespace.namespace.clone(), table.name))
                .collect(),
            next_page_token: page.next_page_token.map(Arc::from),
        })
    }

    async fn list_tables_page_for_request(
        &self,
        namespace: CatalogNamespaceName,
        page_token: Option<Arc<str>>,
        page_size: usize,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<CatalogTablePage, ConnectorError> {
        self.listing
            .run(
                &context,
                self.list_tables_page(namespace, page_token, page_size),
            )
            .await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        // Existence is a catalog fact in both modes (L27): never a storage
        // capability, a FileIO, or a parse of the whole metadata.
        let ident = super::delegate::table_ident(&table)?;
        self.endpoints.table_exists(&ident).await
    }

    async fn table_exists_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Table existence requires an admitted request context",
            )
        })?;
        let exists = self.listing.run(context, self.table_exists(table)).await?;
        context.check_active()?;
        Ok(exists)
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        self.load_table_with_snapshots(table, super::SnapshotLoading::All)
            .await
    }

    async fn load_table_with_snapshots(
        &self,
        table: CatalogTableName,
        snapshots: super::SnapshotLoading,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        let ident = super::delegate::table_ident(&table)?;
        let delegated = self.access_delegation == RestAccessDelegationMode::Vended;
        let (frozen, delegation) = self
            .endpoints
            .load_table_with_snapshots(&ident, delegated, snapshots)
            .await?;
        match self.access_delegation {
            RestAccessDelegationMode::Static => {
                let file_io = self.endpoints.static_file_io().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unavailable,
                        "Iceberg REST static generation has no storage factory",
                    )
                })?;
                let table = frozen.materialize(file_io).map_err(|error| {
                    super::error::map_read_error(&sanitize_upstream_error(&error, "table load"))
                })?;
                Ok(IcebergLoadedTable::new(
                    table,
                    IcebergAccessDelegation::static_binding(),
                ))
            }
            RestAccessDelegationMode::Vended => {
                let access_delegation = parse_rest_delegation(delegation)?;
                Ok(IcebergLoadedTable::deferred_rest(frozen, access_delegation))
            }
        }
    }

    async fn load_table_for_read_with_snapshots(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
        snapshots: super::SnapshotLoading,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        let ident = super::delegate::table_ident(&table)?;
        let context =
            crate::rest_protocol::http::RequestContext::for_binding(binding.request_context());
        let delegated = self.access_delegation == RestAccessDelegationMode::Vended;
        let (frozen, delegation) = self
            .endpoints
            .load_table_with_snapshots_in(&ident, delegated, snapshots, &context)
            .await?;
        match self.access_delegation {
            RestAccessDelegationMode::Static => {
                let file_io = crate::fs_io::build_file_io_for_location(
                    frozen.metadata_location().unwrap_or(""),
                    binding,
                );
                let table = frozen.materialize(file_io).map_err(|error| {
                    super::error::map_read_error(&sanitize_upstream_error(&error, "table load"))
                })?;
                Ok(IcebergLoadedTable::new(
                    table,
                    IcebergAccessDelegation::static_binding(),
                ))
            }
            RestAccessDelegationMode::Vended => Ok(IcebergLoadedTable::deferred_rest(
                frozen,
                parse_rest_delegation(delegation)?,
            )),
        }
    }

    async fn load_complete_metadata_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
        validator: Option<&super::CatalogMetadataValidator>,
    ) -> Result<super::CatalogMetadataLoad, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Complete metadata load requires the original admitted request context",
            )
        })?;
        context.check_active()?;
        let ident = super::delegate::table_ident(&table)?;
        let result = self
            .listing
            .run(
                context,
                self.endpoints
                    .load_complete_metadata(&ident, validator, context),
            )
            .await?;
        context.check_active()?;
        Ok(result)
    }

    async fn load_commit_base(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        self.load_commit_base_with_snapshots(table, file_io, super::SnapshotLoading::All)
            .await
    }

    async fn load_commit_base_with_snapshots(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
        snapshots: super::SnapshotLoading,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        let ident = super::delegate::table_ident(&table).map_err(|error| {
            crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.to_string())
                .with_source(error)
        })?;
        let (frozen, _response_delegation) = self
            .endpoints
            .load_table_with_snapshots(
                &ident,
                self.access_delegation == RestAccessDelegationMode::Vended,
                snapshots,
            )
            .await
            .map_err(|error| {
                let kind = match error.kind() {
                    ConnectorErrorKind::NotFound => crate::iceberg::ErrorKind::TableNotFound,
                    _ => crate::iceberg::ErrorKind::Unexpected,
                };
                crate::iceberg::Error::new(kind, error.message().to_string())
            })?;
        // This reload consumes metadata only. The already-admitted operation
        // retains its exact resolver and renews credentials through its owner.
        super::CatalogCommitBase::from_table(frozen.materialize(file_io)?)
    }

    async fn view_exists_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        let result = self.listing.run(&context, self.view_exists(view)).await?;
        context.check_active()?;
        Ok(result)
    }

    async fn load_view_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        let result = self.listing.run(&context, self.load_view(view)).await?;
        context.check_active()?;
        Ok(result)
    }

    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        let ident = super::delegate::table_ident(&view)?;
        self.endpoints.view_exists(&ident).await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        let ident = super::delegate::namespace_ident(&namespace)?;
        let mut collector = ConnectorListingCollector::new(bound)?;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self
                .endpoints
                .list_views_page(&ident, collector.continuation_token(), bound.page_entries)
                .await?;
            if page
                .identifiers
                .iter()
                .any(|view| view.namespace() != &ident)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg REST catalog returned an out-of-scope view identifier",
                ));
            }
            preflight_rest_page(
                &collector,
                page.identifiers.iter().map(|view| view.name.as_str()),
                page.next_page_token.as_deref(),
                &seen,
            )?;
            accept_rest_page(
                &mut collector,
                page.identifiers
                    .iter()
                    .map(|view| view.name.to_owned())
                    .collect(),
                page.next_page_token,
                &mut seen,
            )?;
            if collector.continuation_token().is_none() {
                break;
            }
        }
        Ok(super::delegate::sorted_unique(collector.finish()?))
    }

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        let ident = super::delegate::table_ident(&view)?;
        self.endpoints.load_view(&ident).await
    }

    async fn load_table_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Table load requires an admitted request context",
            )
        })?;
        let loaded = self.listing.run(context, self.load_table(table)).await?;
        context.check_active()?;
        Ok(loaded)
    }

    async fn anchor_written_metadata(
        &self,
        table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::AnchorWrittenMetadata,
            &table.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }
        // This catalog owns its own metadata pointer, so a committed write is
        // already reachable through it.
        CatalogOutcome::committed(
            table,
            novarocks_spi::connector::ExternalMutationEffect::NoOp,
        )
    }
}

#[cfg(test)]
mod tests {
    //! REST owner listing, admission and existence correctness. Publication
    //! evidence (D13) is covered by `rest_protocol::mutation` and the owner
    //! evidence tests below. Each component test owns a loopback ephemeral
    //! listener and temporary ledger; Drop joins the server before releasing
    //! it. The catalog maintainer runs these with every connector test
    //! invocation.
    //! Existence-probe cases cover real CREATE/DROP prechecks against a catalog
    //! without declared HEAD endpoints or a StorageFactory. Expected NoOp facts
    //! come from create/drop policy; any FileIO materialization would fail.
    //! They share the listener isolation/exit owner and connector test cadence
    //! above. Existence is a status-only GET without the delegation header.
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorListingBound};

    use super::NovaRocksRestCatalog;
    use crate::catalog::{CatalogNamespaceName, NovaRocksCatalog};
    use crate::catalog_runtime::RestAccessDelegationMode;

    const TABLES_PATH: &str = "/v1/namespaces/db/tables";

    /// A REST catalog that answers list-tables with a fixed page sequence and
    /// records every list-tables request target. Page `i` announces the token
    /// `p{i+1}` when a later page exists. It ignores `pageSize`, as a server
    /// without pagination support may.
    struct PagedTablesServer {
        address: SocketAddr,
        shutdown: Arc<AtomicBool>,
        targets: Arc<Mutex<Vec<String>>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl PagedTablesServer {
        fn start(pages: Vec<Vec<&str>>) -> Self {
            let pages: Vec<Vec<String>> = pages
                .into_iter()
                .map(|page| page.into_iter().map(str::to_string).collect())
                .collect();
            Self::start_response(move |target| list_tables_body(&pages, target))
        }

        fn start_response(response_body: impl Fn(&str) -> String + Send + 'static) -> Self {
            Self::start_http_response(move |request| {
                let target = request.split_whitespace().nth(1).unwrap_or_default();
                (200, response_body(target))
            })
        }

        fn start_http_response(
            response_body: impl Fn(&str) -> (u16, String) + Send + 'static,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind paged REST catalog");
            listener
                .set_nonblocking(true)
                .expect("make paged REST catalog nonblocking");
            let address = listener.local_addr().expect("read paged REST address");
            let shutdown = Arc::new(AtomicBool::new(false));
            let targets = Arc::new(Mutex::new(Vec::new()));
            let thread_shutdown = Arc::clone(&shutdown);
            let thread_targets = Arc::clone(&targets);
            let thread = std::thread::spawn(move || {
                while !thread_shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            // An accepted socket inherits the nonblocking mode.
                            let _ = stream.set_nonblocking(false);
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                            let mut buffer = [0_u8; 4096];
                            let read = stream.read(&mut buffer).unwrap_or(0);
                            if read == 0 {
                                continue;
                            }
                            let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                            let target = request
                                .lines()
                                .next()
                                .and_then(|line| line.split_whitespace().nth(1))
                                .unwrap_or_default()
                                .to_string();
                            let (status, body) = if target.starts_with("/v1/config") {
                                (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
                            } else {
                                thread_targets
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .push(target.clone());
                                response_body(&request)
                            };
                            let response = format!(
                                "HTTP/1.1 {status} Scripted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = stream.write_all(response.as_bytes());
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("accept paged REST connection: {error}"),
                    }
                }
            });
            Self {
                address,
                shutdown,
                targets,
                thread: Some(thread),
            }
        }

        fn list_requests(&self) -> Vec<String> {
            self.targets
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        }

        async fn catalog(&self) -> NovaRocksRestCatalog {
            let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
                HashMap::from([(
                    crate::rest_protocol::wire::REST_CATALOG_PROP_URI.to_string(),
                    format!("http://{}", self.address),
                )]),
                None,
                crate::iceberg::Runtime::try_current().expect("test runtime"),
            )
            .await
            .expect("REST client");
            NovaRocksRestCatalog::new(endpoints, None, RestAccessDelegationMode::Vended)
        }
    }

    impl Drop for PagedTablesServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(thread) = self.thread.take() {
                assert!(
                    thread.join().is_ok() || std::thread::panicking(),
                    "join paged REST catalog"
                );
            }
        }
    }

    fn list_tables_body(pages: &[Vec<String>], target: &str) -> String {
        let index = target
            .split_once("pageToken=p")
            .map(|(_, token)| token.parse::<usize>().expect("scripted token"))
            .unwrap_or(0);
        let identifiers = pages[index]
            .iter()
            .map(|name| format!(r#"{{"namespace":["db"],"name":"{name}"}}"#))
            .collect::<Vec<_>>()
            .join(",");
        match index + 1 < pages.len() {
            true => format!(
                r#"{{"identifiers":[{identifiers}],"next-page-token":"p{}"}}"#,
                index + 1
            ),
            false => format!(r#"{{"identifiers":[{identifiers}]}}"#),
        }
    }

    fn page_request(page_size: usize, token: Option<&str>) -> String {
        match token {
            Some(token) => format!("{TABLES_PATH}?pageSize={page_size}&pageToken={token}"),
            None => format!("{TABLES_PATH}?pageSize={page_size}"),
        }
    }

    async fn list(
        server: &PagedTablesServer,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        server
            .catalog()
            .await
            .list_tables(CatalogNamespaceName::new("db"), bound)
            .await
    }

    #[track_caller]
    fn assert_refused(error: ConnectorError, bound: &str) {
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        assert!(
            error.message().contains(&format!("{bound} bound")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn production_table_listing_requests_v1_pages() {
        let server = PagedTablesServer::start(vec![vec!["t1"]]);
        let tables = list(&server, ConnectorListingBound::V1).await.unwrap();
        assert_eq!(tables, ["t1"]);
        assert_eq!(server.list_requests(), [page_request(256, None)]);
    }

    #[tokio::test]
    async fn table_listing_follows_each_continuation_with_the_bound_page_size() {
        let server = PagedTablesServer::start(vec![vec!["t1", "t2"], vec!["t3", "t4"], vec!["t5"]]);
        let bound = ConnectorListingBound {
            page_entries: 2,
            ..ConnectorListingBound::V1
        };
        let tables = list(&server, bound).await.unwrap();
        assert_eq!(tables, ["t1", "t2", "t3", "t4", "t5"]);
        assert_eq!(
            server.list_requests(),
            [
                page_request(2, None),
                page_request(2, Some("p1")),
                page_request(2, Some("p2")),
            ]
        );
    }

    #[tokio::test]
    async fn an_over_bound_table_listing_is_refused_before_its_next_page() {
        let server = PagedTablesServer::start(vec![vec!["t1", "t2"], vec!["t3", "t4"], vec!["t5"]]);
        let bound = ConnectorListingBound {
            entries: 3,
            page_entries: 2,
            ..ConnectorListingBound::V1
        };
        assert_refused(list(&server, bound).await.unwrap_err(), "entries");
        assert_eq!(
            server.list_requests(),
            [page_request(2, None), page_request(2, Some("p1"))],
            "the refused page's continuation must not be followed"
        );
    }

    #[tokio::test]
    async fn a_continuation_past_the_page_bound_is_never_requested() {
        let server = PagedTablesServer::start(vec![vec!["t1"], vec!["t2"], vec!["t3"]]);
        let bound = ConnectorListingBound {
            page_entries: 1,
            pages: 2,
            ..ConnectorListingBound::V1
        };
        assert_refused(list(&server, bound).await.unwrap_err(), "pages");
        assert_eq!(server.list_requests().len(), 2);
    }

    #[tokio::test]
    async fn a_continuing_page_larger_than_requested_is_refused() {
        let server = PagedTablesServer::start(vec![vec!["t1", "t2", "t3"], vec!["t4"]]);
        let bound = ConnectorListingBound {
            page_entries: 2,
            ..ConnectorListingBound::V1
        };
        assert_refused(list(&server, bound).await.unwrap_err(), "page_entries");
        assert_eq!(server.list_requests(), [page_request(2, None)]);
    }

    #[tokio::test]
    async fn an_unpaged_server_may_return_more_than_the_v1_page_size() {
        let names: Vec<String> = (0..257).map(|index| format!("t{index:03}")).collect();
        let server = PagedTablesServer::start(vec![names.iter().map(String::as_str).collect()]);
        assert_eq!(
            list(&server, ConnectorListingBound::V1).await.unwrap(),
            names
        );
        assert_eq!(server.list_requests(), [page_request(256, None)]);
    }

    #[tokio::test]
    async fn an_oversized_final_page_is_admitted_at_the_whole_listing_bounds() {
        let server = PagedTablesServer::start(vec![vec!["t1", "t2", "t3"]]);
        let bound = ConnectorListingBound {
            entries: 3,
            page_entries: 2,
            pages: 1,
            total_name_bytes: 6,
            ..ConnectorListingBound::V1
        };
        assert_eq!(list(&server, bound).await.unwrap(), ["t1", "t2", "t3"]);
        assert_eq!(server.list_requests(), [page_request(2, None)]);
    }

    #[tokio::test]
    async fn an_oversized_final_page_keeps_the_preceding_pages_charged() {
        let server = PagedTablesServer::start(vec![vec!["t1"], vec!["t2", "t3", "t4"]]);
        let bound = ConnectorListingBound {
            entries: 4,
            page_entries: 2,
            pages: 2,
            total_name_bytes: 8,
            ..ConnectorListingBound::V1
        };
        assert_eq!(
            list(&server, bound).await.unwrap(),
            ["t1", "t2", "t3", "t4"]
        );
        assert_eq!(
            server.list_requests(),
            [page_request(2, None), page_request(2, Some("p1"))]
        );
    }

    #[tokio::test]
    async fn an_oversized_final_page_is_refused_whole_when_any_listing_bound_is_exceeded() {
        for (bound, exceeded) in [
            (
                ConnectorListingBound {
                    entries: 3,
                    page_entries: 2,
                    ..ConnectorListingBound::V1
                },
                "entries",
            ),
            (
                ConnectorListingBound {
                    name_bytes: 1,
                    page_entries: 2,
                    ..ConnectorListingBound::V1
                },
                "name_bytes",
            ),
            (
                ConnectorListingBound {
                    total_name_bytes: 4,
                    page_entries: 2,
                    ..ConnectorListingBound::V1
                },
                "total_name_bytes",
            ),
        ] {
            let server = PagedTablesServer::start(vec![vec!["a"], vec!["b", "cc", "d"]]);
            assert_refused(list(&server, bound).await.unwrap_err(), exceeded);
            assert_eq!(
                server.list_requests(),
                [page_request(2, None), page_request(2, Some("p1"))]
            );
        }
    }
    #[tokio::test]
    async fn namespaces_and_views_own_the_page_loop_and_replay_tokens() {
        for views in [false, true] {
            let server = PagedTablesServer::start_response(move |target| {
                let second = target.contains("pageToken=p1");
                let name = if second { "two" } else { "one" };
                let items = if views {
                    format!(r#""identifiers":[{{"namespace":["db"],"name":"{name}"}}]"#)
                } else {
                    format!(r#""namespaces":[["{name}"]]"#)
                };
                let next = if second {
                    ""
                } else {
                    r#", "next-page-token":"p1""#
                };
                format!("{{{items}{next}}}")
            });
            let catalog = server.catalog().await;
            let bound = ConnectorListingBound {
                page_entries: 1,
                ..ConnectorListingBound::V1
            };
            let names = if views {
                catalog
                    .list_views(CatalogNamespaceName::new("db"), bound)
                    .await
            } else {
                catalog.list_namespaces(bound).await
            }
            .unwrap();
            assert_eq!(names, ["one", "two"]);
            let path = if views {
                "/v1/namespaces/db/views"
            } else {
                "/v1/namespaces"
            };
            assert_eq!(
                server.list_requests(),
                [
                    format!("{path}?pageSize=1"),
                    format!("{path}?pageSize=1&pageToken=p1")
                ]
            );
        }
    }

    #[tokio::test]
    async fn all_rest_listings_refuse_cyclic_tokens_before_a_fourth_request() {
        for kind in ["tables", "views", "namespaces"] {
            let server = PagedTablesServer::start_response(move |target| {
                let token = if target.contains("pageToken=a") {
                    "b"
                } else {
                    "a"
                };
                let items = if kind == "namespaces" {
                    r#""namespaces":[["db"]]"#
                } else {
                    r#""identifiers":[{"namespace":["db"],"name":"t"}]"#
                };
                format!(r#"{{{items},"next-page-token":"{token}"}}"#)
            });
            let catalog = server.catalog().await;
            let bound = ConnectorListingBound::V1;
            let error = match kind {
                "namespaces" => catalog.list_namespaces(bound).await,
                "views" => {
                    catalog
                        .list_views(CatalogNamespaceName::new("db"), bound)
                        .await
                }
                _ => {
                    catalog
                        .list_tables(CatalogNamespaceName::new("db"), bound)
                        .await
                }
            }
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::CorruptData, "{error}");
            assert_eq!(server.list_requests().len(), 3);
        }
    }

    #[tokio::test]
    async fn unpaged_namespace_and_view_responses_are_refused_as_a_whole() {
        for views in [false, true] {
            let server = PagedTablesServer::start_response(move |_| {
                if views {
                    r#"{"identifiers":[{"namespace":["db"],"name":"one"},{"namespace":["db"],"name":"two"}]}"#.into()
                } else {
                    r#"{"namespaces":[["one"],["two"]]}"#.into()
                }
            });
            let catalog = server.catalog().await;
            let bound = ConnectorListingBound {
                entries: 1,
                page_entries: 1,
                ..ConnectorListingBound::V1
            };
            let error = if views {
                catalog
                    .list_views(CatalogNamespaceName::new("db"), bound)
                    .await
            } else {
                catalog.list_namespaces(bound).await
            }
            .unwrap_err();
            assert_refused(error, "entries");
            assert_eq!(server.list_requests().len(), 1);
        }
    }

    #[tokio::test]
    async fn namespaces_and_views_check_token_and_page_limits_before_following() {
        for views in [false, true] {
            for (bound, expected) in [
                (
                    ConnectorListingBound {
                        continuation_token_bytes: 1,
                        ..ConnectorListingBound::V1
                    },
                    "continuation_token_bytes",
                ),
                (
                    ConnectorListingBound {
                        pages: 1,
                        ..ConnectorListingBound::V1
                    },
                    "pages",
                ),
            ] {
                let server = PagedTablesServer::start_response(move |_| {
                    if views {
                        r#"{"identifiers":[{"namespace":["db"],"name":"v"}],"next-page-token":"token"}"#.into()
                    } else {
                        r#"{"namespaces":[["db"]],"next-page-token":"token"}"#.into()
                    }
                });
                let catalog = server.catalog().await;
                let error = if views {
                    catalog
                        .list_views(CatalogNamespaceName::new("db"), bound)
                        .await
                } else {
                    catalog.list_namespaces(bound).await
                }
                .unwrap_err();
                assert_refused(error, expected);
                assert_eq!(server.list_requests().len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn request_deadline_and_stop_refuse_real_rest_calls_without_empty_success() {
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        use std::time::Instant;
        for cancelled in [false, true] {
            let server = PagedTablesServer::start_response(|_| {
                std::thread::sleep(Duration::from_millis(40));
                r#"{"identifiers":[]}"#.into()
            });
            let catalog = server.catalog().await;
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_millis(10),
                stop.view(),
                1024,
                4096,
            )
            .unwrap();
            if cancelled {
                stop.request_stop();
            }
            let error = catalog
                .list_views_for_request(
                    CatalogNamespaceName::new("db"),
                    context,
                    ConnectorListingBound::V1,
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                if cancelled {
                    ConnectorErrorKind::Cancelled
                } else {
                    ConnectorErrorKind::DeadlineExceeded
                }
            );
            assert_eq!(
                catalog.listing.available_positions(),
                super::super::listing_admission::LISTING_CONCURRENCY
            );
        }
    }

    fn commit_response(status: u16) -> String {
        if status == 200 {
            "<html>proxy replaced the response</html>".to_string()
        } else {
            serde_json::json!({"error": {"message": "scripted response", "type": "CommitFailedException", "code": status}}).to_string()
        }
    }

    fn insert_admission_metadata(
        version: crate::iceberg::spec::FormatVersion,
        partitioned: bool,
    ) -> crate::iceberg::spec::TableMetadata {
        use crate::iceberg::spec::{
            NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Transform, Type,
        };
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let spec = if partitioned {
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id", Transform::Identity)
                .expect("partition field")
                .build()
                .expect("partition spec")
        } else {
            PartitionSpec::unpartition_spec()
        };
        let builder = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "file:///tmp/novarocks-insert-admission/table".to_string(),
            version,
            HashMap::new(),
        )
        .expect("metadata builder");
        builder.build().expect("metadata").metadata
    }

    /// Exercise the actual session owner against the existing isolated REST
    /// backend, not a preparation-only guard or a mocked admission verdict.
    fn begin_insert_admission(
        metadata: crate::iceberg::spec::TableMetadata,
        intent: novarocks_spi::connector::ConnectorWriteIntent,
        target_ref: &str,
        purpose: novarocks_spi::connector::ConnectorWriteAdmissionPurpose,
    ) -> Result<
        novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
        ConnectorError,
    > {
        use novarocks_spi::connector::write_stack::session::{
            ConnectorWriteBeginRequest, ConnectorWriteControl, ConnectorWriteSessionFlavor,
        };
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
            ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner,
            ConnectorWriteFieldRequest, ConnectorWriteInputRequest, ConnectorWriteTargetRef,
            ProviderBindingEpoch,
        };
        let response = serde_json::json!({
            "metadata-location": "file:///tmp/novarocks-insert-admission/metadata.json",
            "metadata": metadata,
        })
        .to_string();
        let server = PagedTablesServer::start_http_response(move |request| {
            assert!(
                request.starts_with("GET /v1/namespaces/db/tables/t "),
                "admission must only load metadata: {request}"
            );
            (200, response.clone())
        });
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("uri".into(), format!("http://{}", server.address)),
            ],
        )
        .expect("configuration");
        let runtime = Arc::new(
            crate::metadata_context::IcebergMetadataContext::try_new(
                crate::catalog_control::IcebergCatalogControlState::new(configuration),
                crate::resources::IcebergMetadataResources::new(binding, executor.handle().clone()),
            )
            .expect("metadata owner"),
        );
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance");
        let control = crate::commit::write_stack::control::IcebergWriteSessionControl::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
                instance_id: instance_id.clone(),
            },
            ProviderBindingEpoch::new(),
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([1; 32])),
            runtime,
        );
        let result = control.begin_write(ConnectorWriteBeginRequest {
            table: Arc::from("db.t"),
            target_ref: ConnectorWriteTargetRef::parse(target_ref).expect("target ref"),
            intent,
            purpose,
            input: ConnectorWriteInputRequest::Data {
                fields: vec![
                    ConnectorWriteFieldRequest::try_new_typed(
                        arrow::datatypes::Field::new(
                            "id",
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),
                        novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),
                    )
                    .expect("INSERT caller declares required Int64 id"),
                ],
            },
            base: None,
            flavor: ConnectorWriteSessionFlavor::Ordinary,
            context: ConnectorRequestContext::try_new(
                std::time::Instant::now() + Duration::from_secs(30),
                ConnectorStopOwner::new().view(),
                64 * 1024,
                1024 * 1024,
            )
            .expect("request context"),
        });
        assert_eq!(server.list_requests(), ["/v1/namespaces/db/tables/t"]);
        result
    }

    #[test]
    fn ordinary_insert_session_denies_partition_overwrite_on_unpartitioned_and_pre_v3_tables() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
        for (version, partitioned, expected) in [
            (
                FormatVersion::V3,
                false,
                "INSERT OVERWRITE PARTITIONS requires a partitioned table; table ice.db.t is unpartitioned (use OVERWRITE without PARTITIONS)",
            ),
            (
                FormatVersion::V2,
                true,
                "INSERT OVERWRITE PARTITIONS requires an Iceberg v3 row-lineage table; table ice.db.t is v2",
            ),
        ] {
            let error = begin_insert_admission(
                insert_admission_metadata(version, partitioned),
                ConnectorWriteIntent::PartitionOverwrite,
                "main",
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
            )
            .expect_err("partition overwrite must be denied before a writer is admitted");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert_eq!(error.message(), expected);
        }
    }

    #[test]
    fn ordinary_insert_session_keeps_supported_v2_append_and_v3_partition_overwrite_admissible() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
        for (version, partitioned, intent) in [
            (FormatVersion::V2, false, ConnectorWriteIntent::Append),
            (
                FormatVersion::V3,
                true,
                ConnectorWriteIntent::PartitionOverwrite,
            ),
        ] {
            let plan = begin_insert_admission(
                insert_admission_metadata(version, partitioned),
                intent,
                "main",
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
            )
            .expect("a supported ordinary INSERT must remain admissible");
            assert_eq!(plan.expected_targets().len(), 1);
        }
    }

    #[test]
    fn ordinary_session_materialized_view_purpose_does_not_acquire_insert_only_denials() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
        // Same table, intent and input as the unpartitioned INSERT denial above.
        // The existing purpose path remains independent of the new INSERT gate.
        let plan = begin_insert_admission(
            insert_admission_metadata(FormatVersion::V3, false),
            ConnectorWriteIntent::PartitionOverwrite,
            "main",
            ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
        )
        .expect("the existing MV-purpose admission must retain its own policy");
        assert_eq!(plan.expected_targets().len(), 1);
    }
    #[test]
    fn ordinary_insert_session_keeps_generic_support_error_kind_and_precedence() {
        use crate::iceberg::spec::FormatVersion;
        use novarocks_spi::connector::{ConnectorWriteAdmissionPurpose, ConnectorWriteIntent};
        // This protocol-valid metadata names a sort source absent from its schema.
        // It also fails the unpartitioned INSERT-only gate. The existing generic
        // support error must retain its kind and precedence.
        let mut json = serde_json::to_value(insert_admission_metadata(FormatVersion::V3, false))
            .expect("metadata JSON");
        json["default-sort-order-id"] = serde_json::json!(1);
        json["sort-orders"] = serde_json::json!([{
            "order-id": 1,
            "fields": [{
                "transform": "identity", "source-id": 999,
                "direction": "asc", "null-order": "nulls-first",
            }],
        }]);
        let metadata = serde_json::from_value(json).expect("metadata with unresolved sort source");
        let error = begin_insert_admission(
            metadata,
            ConnectorWriteIntent::PartitionOverwrite,
            "main",
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
        )
        .expect_err("generic support must fail before the INSERT-only gate");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            error.message(),
            "iceberg table sort field references missing source id 999"
        );
    }

    fn existence_response() -> String {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Type, UnboundPartitionSpec,
        };
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/db/present".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        serde_json::json!({
            "metadata-location": "s3://warehouse/db/present/metadata.json",
            "metadata": metadata,
        })
        .to_string()
    }

    fn existence_provider(
        server: &PagedTablesServer,
    ) -> (tokio::runtime::Runtime, crate::metadata::IcebergMetadata) {
        use crate::access_binding::IcebergReadBinding;
        use crate::catalog_control::IcebergCatalogControlState;
        use crate::resources::IcebergMetadataResources;
        use novarocks_spi::connector::{
            ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
            ProviderBindingEpoch,
        };
        let executor = tokio::runtime::Runtime::new().unwrap();
        // This real REST owner deliberately has no StorageFactory. A SDK
        // materialization during an existence probe would therefore fail.
        let owner = Arc::new(executor.block_on(server.catalog()));
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                ("uri".into(), format!("http://{}", server.address)),
            ],
        )
        .unwrap();
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = crate::metadata_context::IcebergMetadataContext::with_catalog_for_test(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, executor.handle().clone()),
            owner,
        );
        let provider = crate::metadata::IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([9; 16]),
            Arc::new(runtime),
        );
        (executor, provider)
    }

    #[test]
    fn vended_create_and_drop_prechecks_without_head_declaration_do_not_materialize_file_io() {
        use novarocks_spi::connector::{
            ConnectorCatalogMutation, ConnectorCatalogMutationOperation,
            ConnectorCatalogMutationRequest, ConnectorColumnDefinition, ConnectorDataType,
            ConnectorDropTableDataDisposition, ConnectorMutationOperationId,
            ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorStopOwner,
            ConnectorTableIdentity, CreatePolicy, DropPolicy, ExternalMutationEffect,
            ExternalMutationOutcome,
        };
        let response = existence_response();
        let server = PagedTablesServer::start_http_response(move |request| {
            assert!(
                request.starts_with("GET "),
                "unexpected mutation: {request}"
            );
            // Existence never asks the server to vend credentials (L27).
            assert!(
                !request
                    .to_ascii_lowercase()
                    .contains("x-iceberg-access-delegation")
            );
            if request.starts_with("GET /v1/namespaces/db/tables/present ") {
                (200, response.clone())
            } else {
                assert!(request.starts_with("GET /v1/namespaces/db/tables/missing "));
                (404, commit_response(404))
            }
        });
        let (_executor, provider) = existence_provider(&server);
        let table = |name: &str| ConnectorTableIdentity {
            instance_id: provider.descriptor().instance_id.clone(),
            namespace: Arc::from("db"),
            table: Arc::from(name),
        };
        let operations = [
            ConnectorCatalogMutationOperation::CreateTable {
                table: table("present"),
                columns: vec![ConnectorColumnDefinition {
                    name: Arc::from("id"),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                key: None,
                partitioning: vec![],
                properties: vec![],
                policy: CreatePolicy::NoOpIfExists,
            },
            ConnectorCatalogMutationOperation::DropTable {
                table: table("missing"),
                policy: DropPolicy::NoOpIfMissing,
                data_disposition: ConnectorDropTableDataDisposition::Retain,
            },
        ];
        for operation in operations {
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                std::time::Instant::now() + Duration::from_secs(30),
                stop.view(),
                64 * 1024,
                256 * 1024,
            )
            .unwrap();
            let outcome = ConnectorCatalogMutation::execute(
                &provider,
                ConnectorCatalogMutationRequest {
                    operation_id: ConnectorMutationOperationId::new(),
                    target: ConnectorProviderBindingKey {
                        instance_id: provider.descriptor().instance_id.clone(),
                        incarnation: provider.incarnation(),
                    },
                    operation,
                    context,
                },
            )
            .unwrap();
            assert!(
                matches!(
                    outcome,
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        ..
                    }
                ),
                "{outcome:?}"
            );
        }
        // A missing DROP is settled by its original-context existence probe.
        // No evidence preparation repeats the lookup or materializes FileIO.
        assert_eq!(
            server.list_requests(),
            [
                "/v1/namespaces/db/tables/present",
                "/v1/namespaces/db/tables/missing"
            ]
        );
    }

    #[tokio::test]
    async fn vended_existence_probe_keeps_other_rest_errors_unavailable() {
        for status in [400, 500] {
            let server = PagedTablesServer::start_http_response(move |request| {
                assert!(request.starts_with("GET "));
                (status, commit_response(status))
            });
            let error = server
                .catalog()
                .await
                .table_exists(crate::catalog::CatalogTableName::new("db", "t"))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::Unavailable);
            assert_eq!(server.list_requests(), ["/v1/namespaces/db/tables/t"]);
        }
    }
}

#[cfg(test)]
mod evidence_tests {
    //! The REST owner on D13 evidence (L12, L18, L19, L24, L32): the typed
    //! commit dispatch (a vended success is applied without any FileIO), the
    //! delegate's DDL through the transitional adapter, view owner methods and
    //! both CTAS steps. Each test owns a loopback scripted server.
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;
    use uuid::Uuid;

    use super::NovaRocksRestCatalog;
    use crate::catalog::error::CatalogOutcome;
    use crate::catalog::{
        CatalogNamespaceName, CatalogTableName, NovaRocksCatalog, StagedPublicationExpectation,
    };
    use crate::catalog_runtime::RestAccessDelegationMode;
    use crate::commit::model::{
        AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequest, FrozenRequestParts,
        OperationToken, RequestShape,
    };
    use crate::iceberg::spec::{SnapshotReference, SnapshotRetention};
    use crate::iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};

    const TABLE_UUID: u128 = 1;

    async fn owner(server: &TestServer) -> NovaRocksRestCatalog {
        let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
            HashMap::from([("uri".to_string(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation");
        NovaRocksRestCatalog::new(
            endpoints,
            Some(Arc::from("s3://warehouse")),
            RestAccessDelegationMode::Vended,
        )
    }

    async fn publish_catalog_request(
        owner: NovaRocksRestCatalog,
        request: crate::catalog::catalog_request::CatalogPublicationRequest,
    ) -> (
        CatalogOutcome<crate::catalog::transaction::CommitProof>,
        Option<Vec<u8>>,
    ) {
        use novarocks_spi::connector::*;
        let operation = ConnectorMutationOperationId::new();
        let request = request.freeze(operation.to_bytes()).unwrap();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            64 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let handle = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
        )
        .for_request(context.clone());
        crate::catalog::admission_test_support::publish_catalog_with_document(
            Arc::new(owner),
            request,
            operation,
            ProviderBindingEpoch::from_bytes([66; 16]),
            context,
            binding,
        )
        .await
    }

    fn scripted(method: &str, path: &str, reply: Reply) -> TestServer {
        TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(method, path, reply),
        )
    }

    fn frozen_append(snapshot: i64) -> FrozenRequest {
        let uuid = Uuid::from_u128(TABLE_UUID);
        let token = OperationToken::from_write(
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([1; 16]),
        );
        FrozenRequest::new(FrozenRequestParts {
            shape: RequestShape::SnapshotProducing,
            target: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
            target_ref: "main".into(),
            base: BaseIdentity::Existing {
                uuid,
                parent: Some(10),
                metadata_location: "s3://warehouse/db/t/metadata/1.json".into(),
            },
            requirements: vec![
                TableRequirement::UuidMatch { uuid },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: Some(10),
                },
            ],
            updates: vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".into(),
                reference: SnapshotReference::new(
                    snapshot,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            }],
            artifacts: AttemptArtifacts::empty(AttemptToken::new(token, 0)),
        })
        .expect("frozen request")
    }

    fn commit_response(snapshots: &[i64]) -> serde_json::Value {
        serde_json::json!({
            "metadata-location": "s3://warehouse/db/t/metadata/2.json",
            "metadata": {
                "table-uuid": Uuid::from_u128(TABLE_UUID).to_string(),
                "snapshots": snapshots.iter().map(|id| serde_json::json!({"snapshot-id": id})).collect::<Vec<_>>(),
                "refs": {"main": {"snapshot-id": snapshots.last(), "type": "branch"}},
            },
        })
    }

    #[tokio::test]
    async fn the_owner_commit_dispatch_follows_d13_and_applies_a_vended_success() {
        let cases: Vec<(Reply, &str)> = vec![
            (Reply::json(200, &commit_response(&[10, 20])), "applied"),
            (Reply::status(401), "unauthenticated"),
            (Reply::status(403), "denied"),
            (Reply::status(409), "conflict"),
            (Reply::status(404), "unknown"),
            (Reply::status(400), "unknown"),
            (Reply::bytes(200, b"<html>proxy</html>".to_vec()), "unknown"),
            (Reply::json(200, &commit_response(&[10])), "unknown"),
        ];
        for (reply, expected) in cases {
            let server = scripted("POST", "/v1/namespaces/db/tables/t", reply);
            let owner: Arc<dyn NovaRocksCatalog> = Arc::new(owner(&server).await);
            let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                64 * 1024,
                1024 * 1024,
            )
            .unwrap();
            let result = crate::catalog::admission_test_support::publish_frozen(
                owner,
                novarocks_spi::connector::ConnectorInstanceDescriptor {
                    provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                        .unwrap(),
                    instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                        "rest-owner-fixture",
                    )
                    .unwrap(),
                },
                novarocks_spi::connector::ProviderBindingEpoch::from_bytes([66; 16]),
                frozen_append(20),
                super::super::admission::CatalogOperation::Append,
                context,
                None,
            )
            .await;
            match (expected, result) {
                ("applied", CatalogOutcome::KnownCommitted { receipt: proof, .. }) => {
                    assert_eq!(proof.snapshot_id, Some(20));
                    assert_eq!(
                        proof.table_uuid.as_deref(),
                        Some(Uuid::from_u128(TABLE_UUID).to_string().as_str())
                    );
                }
                ("unauthenticated", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::Unauthenticated)
                }
                ("denied", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::PermissionDenied)
                }
                ("conflict", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::Conflict)
                }
                ("unknown", CatalogOutcome::CommitUnknown { .. }) => {}
                (expected, result) => panic!("expected {expected}, got {result:?}"),
            }
            assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
        }
    }

    #[tokio::test]
    async fn catalog_ddl_uses_the_prepared_owner_and_typed_evidence() {
        let namespace = || CatalogNamespaceName::new("db");
        for (status, expected) in [
            (409, "already exists"),
            (401, "unauthenticated"),
            (404, "unknown"),
            (400, "unknown"),
        ] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("GET", "/v1/namespaces/db", Reply::status(404))
                    .on("POST", "/v1/namespaces", Reply::status(status)),
            );
            let (outcome, _) = publish_catalog_request(
                owner(&server).await,
                crate::catalog::catalog_request::CatalogPublicationRequest::CreateNamespace {
                    namespace: namespace(),
                    properties: HashMap::new(),
                    no_op_if_exists: false,
                },
            )
            .await;
            match (expected, &outcome) {
                ("already exists", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::AlreadyExists)
                }
                ("unauthenticated", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::Unauthenticated)
                }
                ("unknown", CatalogOutcome::CommitUnknown { .. }) => {}
                _ => panic!("HTTP {status}: expected {expected}, got {outcome:?}"),
            }
            assert_eq!(outcome.permits_cleanup(), expected != "unknown");
        }
        // A drop succeeds with 204 or an empty 200; a 200 with a body is
        // unknown (L24 as revised).
        for (label, reply, applied) in [
            ("204", Reply::status(204), true),
            ("empty 200", Reply::status(200), true),
            (
                "HTML 200",
                Reply::bytes(200, b"<html>ok</html>".to_vec()),
                false,
            ),
        ] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("DELETE", "/v1/namespaces/db/tables/t", reply),
            );
            let (outcome, _) = publish_catalog_request(
                owner(&server).await,
                crate::catalog::catalog_request::CatalogPublicationRequest::DropTable {
                    table: CatalogTableName::new("db", "t"),
                    no_op_if_missing: false,
                    purge: false,
                    capture: None,
                },
            )
            .await;
            assert_eq!(
                matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
                applied,
                "{label}: {outcome:?}"
            );
            let deletes = server.requests_to("DELETE", "/v1/namespaces/db/tables/t");
            assert_eq!(deletes.len(), 1);
            assert_eq!(deletes[0].query(), None, "purgeRequested is never sent");
        }
    }

    #[tokio::test]
    async fn view_owner_methods_report_d13_outcomes() {
        let server = scripted("DELETE", "/v1/namespaces/db/views/v", Reply::status(404));
        let (outcome, _) = publish_catalog_request(
            owner(&server).await,
            crate::catalog::catalog_request::CatalogPublicationRequest::DropView(
                CatalogTableName::new("db", "v"),
            ),
        )
        .await;
        assert!(
            matches!(outcome, CatalogOutcome::CommitUnknown { .. }),
            "{outcome:?}"
        );
    }

    fn staged_response() -> serde_json::Value {
        use crate::iceberg::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Type, UnboundPartitionSpec,
        };
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            "s3://warehouse/.staging/t".into(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let expires = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            + 3_600_000)
            .to_string();
        serde_json::json!({
            "metadata-location": null,
            "metadata": metadata,
            "storage-credentials": [{
                "prefix": "s3://warehouse/.staging/t",
                "config": {
                    "s3.access-key-id": "AKIA-staged",
                    "s3.secret-access-key": "secret",
                    "s3.session-token": "session",
                    "s3.session-token-expires-at-ms": expires,
                },
            }],
        })
    }

    fn creation() -> crate::iceberg::TableCreation {
        crate::iceberg::TableCreation::builder()
            .name("t".to_string())
            .location("s3://warehouse/.staging/t".to_string())
            .schema(
                crate::iceberg::spec::Schema::builder()
                    .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                        1,
                        "id",
                        crate::iceberg::spec::Type::Primitive(
                            crate::iceberg::spec::PrimitiveType::Long,
                        ),
                    ))])
                    .build()
                    .unwrap(),
            )
            .build()
    }

    /// Loading has one NovaRocks response decoder in both modes; Static uses
    /// the generation's FileIO and never requests storage delegation. Existing
    /// staged-response coverage does not exercise ordinary default/refs GETs.
    #[tokio::test]
    async fn ordinary_loads_share_novarocks_decoding_in_static_and_vended_modes() {
        use crate::catalog::SnapshotLoading;
        use crate::rest_protocol::endpoints::RestEndpoints;
        use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
        for mode in [
            RestAccessDelegationMode::Static,
            RestAccessDelegationMode::Vended,
        ] {
            let mut response = staged_response();
            response["metadata-location"] =
                serde_json::json!("s3://warehouse/.staging/t/metadata/1.json");
            // Response-local FileIO settings are not authoritative in Static.
            response["config"] = serde_json::json!({"s3.endpoint": "http://untrusted.invalid"});
            let expected_uuid = response["metadata"]["table-uuid"]
                .as_str()
                .unwrap()
                .to_string();
            let server = scripted(
                "GET",
                "/v1/namespaces/db/tables/t",
                Reply::json(200, &response),
            );
            let properties = HashMap::from([("uri".to_string(), server.url())]);
            let vended_endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
                properties.clone(),
                None,
                crate::iceberg::Runtime::try_current().expect("test runtime"),
            )
            .await
            .unwrap();
            let endpoints = if mode == RestAccessDelegationMode::Static {
                let handle = tokio::runtime::Handle::current();
                let binding = crate::access_binding::IcebergReadBinding::new(
                    None,
                    FsAccessResolver::new(),
                    Arc::new(TokioFileIoRuntime::new(handle.clone())),
                    Arc::new(TokioFileTaskSpawner::new(handle)),
                );
                Arc::new(RestEndpoints::new(
                    Arc::new(properties),
                    Arc::clone(vended_endpoints.owner()),
                    vended_endpoints.runtime().clone(),
                    Some(crate::fs_io::build_storage_factory_for_location(
                        "s3://warehouse",
                        binding,
                    )),
                    Default::default(),
                    false,
                ))
            } else {
                vended_endpoints
            };
            let owner = NovaRocksRestCatalog::new(endpoints, None, mode);
            for snapshots in [SnapshotLoading::All, SnapshotLoading::Refs] {
                let loaded = if snapshots == SnapshotLoading::All {
                    owner.load_table(CatalogTableName::new("db", "t")).await
                } else {
                    owner
                        .load_table_with_snapshots(CatalogTableName::new("db", "t"), snapshots)
                        .await
                }
                .expect("ordinary table load");
                let (materialization, delegation) = loaded.into_parts();
                match mode {
                    RestAccessDelegationMode::Static => {
                        assert!(delegation.into_vended_lease_seed().is_none());
                        let table = materialization.into_static_table().unwrap();
                        assert_eq!(table.metadata().uuid().to_string(), expected_uuid);
                        assert_eq!(
                            table.metadata_location(),
                            Some("s3://warehouse/.staging/t/metadata/1.json")
                        );
                        assert_eq!(table.identifier().name(), "t");
                    }
                    RestAccessDelegationMode::Vended => {
                        assert!(delegation.into_vended_lease_seed().is_some());
                        assert!(matches!(
                            materialization,
                            crate::loaded_table::IcebergLoadedTableMaterialization::DeferredRest(_)
                        ));
                    }
                }
            }
            let requests = server.requests_to("GET", "/v1/namespaces/db/tables/t");
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].query(), None);
            assert_eq!(requests[1].query(), Some("snapshots=refs"));
            for request in requests {
                assert_eq!(
                    request.header("x-iceberg-access-delegation"),
                    if mode == RestAccessDelegationMode::Static {
                        None
                    } else {
                        Some("vended-credentials")
                    }
                );
            }
        }
    }

    #[tokio::test]
    async fn the_stage_maps_onto_the_saga_branches_of_l19() {
        for (reply, expected) in [
            (Reply::status(401), "uncommitted"),
            (Reply::status(409), "conflict"),
            (Reply::status(400), "unknown"),
            (Reply::status(404), "unknown"),
            (Reply::bytes(200, b"<html/>".to_vec()), "unknown"),
            (Reply::json(200, &staged_response()), "staged"),
        ] {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on(
                        "GET",
                        "/v1/namespaces/db",
                        Reply::json(
                            200,
                            &serde_json::json!({"namespace":["db"],"properties":{}}),
                        ),
                    )
                    .on("POST", "/v1/namespaces/db/tables", reply),
            );
            let (outcome, document) = publish_catalog_request(
                owner(&server).await,
                crate::catalog::catalog_request::CatalogPublicationRequest::StageCreate {
                    namespace: CatalogNamespaceName::new("db"),
                    creation: creation(),
                },
            )
            .await;
            match (expected, outcome) {
                ("uncommitted", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::Unauthenticated)
                }
                ("conflict", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::AlreadyExists)
                }
                ("unknown", CatalogOutcome::CommitUnknown { .. }) => {}
                ("staged", CatalogOutcome::KnownCommitted { receipt, .. }) => {
                    assert!(receipt.metadata_location.is_none());
                    let response: crate::rest_protocol::wire::LoadTableResult =
                        serde_json::from_slice(&document.unwrap()).unwrap();
                    assert!(response.metadata_location.is_none());
                    assert_eq!(
                        receipt.table_uuid.as_deref(),
                        Some(response.metadata.uuid().to_string().as_str())
                    );
                    assert!(
                        !crate::commit::staging::staged_create_updates(&response.metadata)
                            .unwrap()
                            .is_empty()
                    );
                    let (_, delegation) =
                        crate::rest_protocol::deferred::FrozenLoadTable::from_response(
                            crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
                            response,
                            crate::iceberg::Runtime::try_current().unwrap(),
                        );
                    assert!(
                        crate::loaded_table::parse_rest_delegation(delegation)
                            .unwrap()
                            .into_vended_lease_seed()
                            .is_some()
                    );
                }
                (expected, outcome) => panic!("expected {expected}, got {outcome:?}"),
            }
            let stages = server.requests_to("POST", "/v1/namespaces/db/tables");
            assert_eq!(stages.len(), 1);
            assert_eq!(
                stages[0].header("x-iceberg-access-delegation"),
                Some("vended-credentials")
            );
            let body: serde_json::Value = serde_json::from_slice(&stages[0].body).unwrap();
            assert_eq!(body["stage-create"], serde_json::json!(true));
        }
    }

    #[tokio::test]
    async fn the_ctas_publication_is_attributed_before_it_is_reported() {
        let staged_uuid = Uuid::from_u128(TABLE_UUID);
        let marker = (
            "novarocks.ctas.operation-id".to_string(),
            "op-1".to_string(),
        );
        let body = |marked: bool| {
            let mut response = commit_response(&[30]);
            if marked {
                response["metadata"]["properties"] =
                    serde_json::json!({"novarocks.ctas.operation-id": "op-1"});
            }
            response
        };
        for (reply, expected) in [
            (Reply::json(200, &body(true)), "committed"),
            (Reply::json(200, &body(false)), "unknown"),
            (Reply::bytes(200, b"<html/>".to_vec()), "unknown"),
            (Reply::status(409), "conflict"),
            (Reply::status(403), "uncommitted"),
            (Reply::status(404), "unknown"),
        ] {
            let server = scripted("POST", "/v1/namespaces/db/tables/t", reply);
            let mut initial_json = staged_response()["metadata"].clone();
            initial_json["table-uuid"] = serde_json::to_value(staged_uuid).unwrap();
            let initial: crate::iceberg::spec::TableMetadata =
                serde_json::from_value(initial_json).unwrap();
            let token = OperationToken::from_write(
                novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([2; 16]),
            );
            let snapshot = crate::iceberg::spec::Snapshot::builder()
                .with_snapshot_id(30)
                .with_sequence_number(1)
                .with_timestamp_ms(initial.last_updated_ms())
                .with_manifest_list("s3://warehouse/.staging/t/metadata/30.avro")
                .with_summary(crate::iceberg::spec::Summary {
                    operation: crate::iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build();
            let mut updates = crate::commit::staging::staged_create_updates(&initial).unwrap();
            updates.extend([
                TableUpdate::SetProperties {
                    updates: HashMap::from([marker.clone()]),
                },
                TableUpdate::AddSnapshot { snapshot },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".into(),
                    reference: SnapshotReference::new(
                        30,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
            ]);
            let frozen = FrozenRequest::new(FrozenRequestParts {
                shape: RequestShape::Create,
                target: TableIdent::from_strs(["db", "t"]).unwrap(),
                target_ref: "main".into(),
                base: BaseIdentity::Create {
                    staged: crate::commit::model::StagedCreateIdentity::new(
                        token,
                        Arc::new(initial),
                    ),
                },
                requirements: vec![TableRequirement::NotExist],
                updates,
                artifacts: AttemptArtifacts::empty(AttemptToken::new(token, 0)),
            })
            .unwrap();
            let context = novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                64 * 1024,
                1024 * 1024,
            )
            .unwrap();
            let result = crate::catalog::admission_test_support::publish_frozen(
                Arc::new(owner(&server).await),
                novarocks_spi::connector::ConnectorInstanceDescriptor {
                    provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                        .unwrap(),
                    instance_id: novarocks_spi::connector::ConnectorInstanceId::parse(
                        "ctas-owner-fixture",
                    )
                    .unwrap(),
                },
                novarocks_spi::connector::ProviderBindingEpoch::from_bytes([66; 16]),
                frozen,
                super::super::admission::CatalogOperation::CreateTable(
                    super::super::CatalogCreateIntent::CreateTableAsSelect,
                ),
                context,
                Some(StagedPublicationExpectation {
                    staged_table_uuid: staged_uuid,
                    snapshot_id: Some(30),
                    properties: vec![marker.clone()],
                }),
            )
            .await;
            match (expected, result) {
                ("committed", CatalogOutcome::KnownCommitted { receipt: proof, .. }) => {
                    assert_eq!(proof.snapshot_id, Some(30))
                }
                ("conflict", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::AlreadyExists)
                }
                ("uncommitted", CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), Kind::PermissionDenied)
                }
                ("unknown", CatalogOutcome::CommitUnknown { .. }) => {}
                (expected, result) => panic!("expected {expected}, got {result:?}"),
            }
        }
    }
}

struct PreparedRestTablePublication(crate::rest_protocol::mutation::prepared::PreparedRestMutation);
struct PreparedRestCatalogPublication {
    prepared: crate::rest_protocol::mutation::prepared::PreparedRestMutation,
    document: super::catalog_request::CatalogResponseDocument,
}
#[async_trait]
impl super::publication_dispatch::PreparedCatalogDispatch for PreparedRestCatalogPublication {
    async fn dispatch_once(
        self: Box<Self>,
        evidence: super::publication::EvidenceContext,
    ) -> super::publication_dispatch::DispatchCompletion {
        let result = match self.prepared.dispatch(evidence).await {
            Ok(result) => result,
            Err(original) => {
                return super::publication_dispatch::DispatchCompletion::evidence(
                    crate::commit::attempt::observation::DispatchEvidence::NotDispatched {
                        failure: novarocks_spi::connector::ConnectorMutationFailure::from_connector(
                            original,
                        ),
                    },
                );
            }
        };
        let evidence = result.dispatch_evidence();
        // Moving already-read response data is infallible and performs no
        // materialization. Only the journal will record the conclusion.
        self.document.retain(result.document);
        super::publication_dispatch::DispatchCompletion::evidence(evidence)
    }
}
#[async_trait]
impl super::publication_dispatch::PreparedCatalogDispatch for PreparedRestTablePublication {
    async fn dispatch_once(
        self: Box<Self>,
        evidence: super::publication::EvidenceContext,
    ) -> super::publication_dispatch::DispatchCompletion {
        let result = match self.0.dispatch(evidence).await {
            Ok(result) => result.dispatch_evidence(),
            Err(original) => crate::commit::attempt::observation::DispatchEvidence::NotDispatched {
                failure: novarocks_spi::connector::ConnectorMutationFailure::from_connector(
                    original,
                ),
            },
        };
        super::publication_dispatch::DispatchCompletion::evidence(result)
    }
}
