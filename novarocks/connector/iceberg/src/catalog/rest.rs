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
use super::delegate::CatalogDelegate;
use super::error::CatalogOutcome;
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTablePage, CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateReceipt, ConditionalCreateRequest, ConditionalCreateVerdict, NovaRocksCatalog,
};
use crate::catalog_runtime::RestAccessDelegationMode;
use crate::loaded_table::{
    IcebergAccessDelegation, IcebergLoadedTable, parse_vended_access_delegation,
};

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
    delegate: CatalogDelegate,
    /// The REST client, for the staged-create protocol that has no equivalent
    /// on the generic catalog trait. It never leaves this module.
    client: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    warehouse: Option<Arc<str>>,
    access_delegation: RestAccessDelegationMode,
}

impl NovaRocksRestCatalog {
    pub(super) fn new(
        client: Arc<crate::iceberg_catalog_rest::RestCatalog>,
        warehouse: Option<Arc<str>>,
        access_delegation: RestAccessDelegationMode,
    ) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
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

fn preflight_rest_page<'a>(
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
fn accept_rest_page(
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
    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.delegate.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "rest"
    }

    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        Arc::clone(self.delegate.client())
    }

    fn vended_credential_refresh_catalog(
        &self,
    ) -> Option<Arc<crate::iceberg_catalog_rest::RestCatalog>> {
        Some(Arc::clone(&self.client))
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
                .client
                .list_namespaces_page(None, collector.continuation_token(), bound.page_entries)
                .await
                .map_err(|error| super::error::map_read_error(&error))?;
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
        self.delegate
            .listing
            .run(context, self.list_namespaces(bound))
            .await
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
        self.delegate
            .listing
            .run(context, self.list_tables(namespace, bound))
            .await
    }

    async fn list_views_for_request(
        &self,
        namespace: CatalogNamespaceName,
        context: novarocks_spi::connector::ConnectorRequestContext,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate
            .listing
            .run(&context, self.list_views(namespace, bound))
            .await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.delegate.namespace_exists(&namespace).await
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
                .client
                .list_tables_page(&ident, collector.continuation_token(), bound.page_entries)
                .await
                .map_err(|error| super::error::map_read_error(&error))?;
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
            .client
            .list_tables_page(&ident, page_token.as_deref(), page_size)
            .await
            .map_err(|error| super::error::map_read_error(&error))?;
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
        self.delegate
            .listing
            .run(
                &context,
                self.list_tables_page(namespace, page_token, page_size),
            )
            .await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.table_exists(&table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<IcebergLoadedTable, ConnectorError> {
        let ident = super::delegate::table_ident(&table)?;
        match self.access_delegation {
            RestAccessDelegationMode::Static => {
                self.delegate.load_table(&table).await.map(|table| {
                    IcebergLoadedTable::new(table, IcebergAccessDelegation::static_binding())
                })
            }
            RestAccessDelegationMode::Vended => {
                let response = self
                    .client
                    .load_table_deferred_with_access_delegation(&ident)
                    .await
                    .map_err(|error| super::error::map_read_error(&error))?;
                let (materialization, delegation) = response.into_parts();
                let access_delegation = parse_vended_access_delegation(&delegation)?;
                Ok(IcebergLoadedTable::deferred_rest(
                    materialization,
                    access_delegation,
                ))
            }
        }
    }

    async fn load_commit_base(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        let ident = super::delegate::table_ident(&table).map_err(|error| {
            crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.to_string())
                .with_source(error)
        })?;
        let response = self
            .client
            .load_table_deferred_with_access_delegation(&ident)
            .await?;
        let (materialization, _response_delegation) = response.into_parts();
        // This reload consumes metadata only. The already-admitted operation
        // retains its exact resolver and renews credentials through its owner.
        super::CatalogCommitBase::from_table(materialization.materialize_with_file_io(file_io)?)
    }

    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.view_exists(&view).await
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
                .client
                .list_views_page(&ident, collector.continuation_token(), bound.page_entries)
                .await
                .map_err(|error| super::error::map_read_error(&error))?;
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
        self.delegate.load_view(&view).await
    }

    async fn create_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateNamespace,
            &namespace.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }
        self.delegate.create_namespace(namespace).await
    }

    async fn drop_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        if let Err(reason) =
            self.admit_operation(&CatalogOperation::DropNamespace, &namespace.clone().into())
        {
            return CatalogOutcome::Unsupported(reason);
        }
        self.delegate.drop_namespace(namespace).await
    }

    async fn drop_table(&self, table: CatalogTableName) -> CatalogOutcome<CatalogDropTableReceipt> {
        if let Err(reason) =
            self.admit_operation(&CatalogOperation::DropTable, &table.clone().into())
        {
            return CatalogOutcome::Unsupported(reason);
        }
        self.delegate.drop_table(table).await
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

    async fn stage_create_table(
        &self,
        namespace: CatalogNamespaceName,
        creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        let target = CatalogTableName::new(Arc::clone(&namespace.namespace), creation.name.clone());
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect),
            &target.into(),
        ) {
            return super::StagedCreateStart::Unsupported(reason);
        }

        let creation = with_explicit_format_version(creation);
        let ident = match super::delegate::namespace_ident(&namespace) {
            Ok(ident) => ident,
            Err(error) => return super::StagedCreateStart::KnownUncommitted(error.to_string()),
        };
        let staged = match self.access_delegation {
            RestAccessDelegationMode::Static => self
                .client
                .stage_create_table_typed(&ident, creation)
                .await
                .map(|staged| {
                    let (table, initialization_updates) = staged.into_parts();
                    (
                        crate::loaded_table::IcebergLoadedTableMaterialization::Materialized(table),
                        initialization_updates,
                        IcebergAccessDelegation::static_binding(),
                    )
                }),
            RestAccessDelegationMode::Vended => self
                .client
                .stage_create_table_typed_deferred_with_access_delegation(&ident, creation)
                .await
                .and_then(|staged| {
                    let (materialization, initialization_updates, delegation) = staged.into_parts();
                    parse_vended_access_delegation(&delegation)
                        .map(|access_delegation| {
                            (
                                crate::loaded_table::IcebergLoadedTableMaterialization::DeferredRest(
                                    materialization,
                                ),
                                initialization_updates,
                                access_delegation,
                            )
                        })
                        .map_err(|error| {
                            crate::iceberg_catalog_rest::StagedCreateError::PossiblyDispatched(
                                crate::iceberg::Error::new(
                                    crate::iceberg::ErrorKind::DataInvalid,
                                    error.to_string(),
                                ),
                            )
                        })
                }),
        };
        match staged {
            Ok((table, initialization_updates, access_delegation)) => {
                super::StagedCreateStart::Staged {
                    table,
                    initialization_updates,
                    access_delegation,
                }
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::Conflict(error)) => {
                super::StagedCreateStart::Conflict(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::KnownNotDispatched(error)) => {
                super::StagedCreateStart::KnownUncommitted(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCreateError::PossiblyDispatched(error)) => {
                super::StagedCreateStart::CommitUnknown(error.to_string())
            }
        }
    }

    async fn commit_staged_table(
        &self,
        commit: crate::iceberg::TableCommit,
        request_file_io: crate::iceberg::io::FileIO,
    ) -> super::StagedCommitResult {
        let target = CatalogTableName::from_identifier(commit.identifier());
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect),
            &target.into(),
        ) {
            return super::StagedCommitResult::Unsupported(reason);
        }

        let result = match self.access_delegation {
            RestAccessDelegationMode::Static => self.client.commit_staged_table_typed(commit).await,
            // The vended generation has no catalog-global StorageFactory. The
            // caller owns an admitted request FileIO, which is the only
            // capability allowed to materialize the committed response.
            RestAccessDelegationMode::Vended => {
                self.client
                    .commit_staged_table_typed_with_file_io(commit, request_file_io)
                    .await
            }
        };
        match result {
            Ok(table) => super::StagedCommitResult::Committed(table),
            Err(crate::iceberg_catalog_rest::StagedCommitError::Conflict(error)) => {
                super::StagedCommitResult::Conflict(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::KnownNotDispatched(error)) => {
                super::StagedCommitResult::KnownUncommitted(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::PossiblyDispatched(error)) => {
                super::StagedCommitResult::CommitUnknown(error.to_string())
            }
            Err(crate::iceberg_catalog_rest::StagedCommitError::CommittedResponseInvalid(
                error,
            )) => super::StagedCommitResult::CommittedResponseInvalid(error.to_string()),
        }
    }

    async fn prepare_conditional_create(
        &self,
        _request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        CatalogOutcome::unsupported(
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        )
    }

    async fn publish_conditional_create(
        &self,
        _attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        CatalogOutcome::unsupported(
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        )
    }

    async fn adjudicate_conditional_create(
        &self,
        _evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        Err(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "REST Iceberg catalog publishes a create through the catalog, not through a conditional metadata write",
        ))
    }

    async fn new_transaction(&self, request: TransactionRequest) -> CatalogTransactionStart {
        if let Err(reason) =
            self.admit_operation(&CatalogOperation::Append, &request.target.clone().into())
        {
            return CatalogTransactionStart::Unsupported(reason);
        }
        super::start_update_table_transaction(&self.delegate, request)
    }

    async fn new_create_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateTable(request.intent),
            &request.target.clone().into(),
        ) {
            return CatalogTransactionStart::Unsupported(reason);
        }
        if self.access_delegation == RestAccessDelegationMode::Vended {
            // The generic transaction API cannot retain the response-local
            // lease seed. Do not issue its static REST request and silently
            // lose the vended authority; T23 must install an attempt consumer
            // before this direct-create surface can be enabled.
            return CatalogTransactionStart::Unsupported(super::error::CatalogUnsupported::new(
                "REST Iceberg vended credentials require a query-attempt lease consumer",
            ));
        }
        match request.intent {
            CatalogCreateIntent::EmptyTable => {
                let request = crate::catalog::transaction::CreateTableTransactionRequest {
                    creation: with_explicit_format_version(request.creation),
                    ..request
                };
                super::start_create_table_transaction(&self.delegate, request)
            }
            CatalogCreateIntent::CreateTableAsSelect => {
                // Same operation rule as admission, so a caller that asked first
                // and a caller that went straight to the constructor get the
                // same answer.
                match self.admit_operation(
                    &CatalogOperation::CreateTable(request.intent),
                    &request.target.clone().into(),
                ) {
                    Ok(()) => super::start_create_table_transaction(&self.delegate, request),
                    Err(reason) => CatalogTransactionStart::Unsupported(reason),
                }
            }
        }
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        if self.access_delegation == RestAccessDelegationMode::Vended {
            return CatalogTransactionStart::Unsupported(super::error::CatalogUnsupported::new(
                "REST Iceberg vended credentials require a query-attempt lease consumer",
            ));
        }
        match self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect),
            &request.target.clone().into(),
        ) {
            Ok(()) => super::start_create_table_transaction(&self.delegate, request),
            Err(reason) => CatalogTransactionStart::Unsupported(reason),
        }
    }
}

/// Spell the format version into the table properties.
///
/// A REST catalog needs it stated explicitly; a filesystem catalog reads it off
/// the creation itself. That difference is this implementation's to know, which
/// is why it used to be a catalog-kind comparison at the call site and is not
/// any more.
fn with_explicit_format_version(
    creation: crate::iceberg::TableCreation,
) -> crate::iceberg::TableCreation {
    if creation.properties.contains_key("format-version") {
        return creation;
    }
    let mut properties = creation.properties.clone();
    properties.insert(
        "format-version".to_string(),
        (creation.format_version as u8).to_string(),
    );
    crate::iceberg::TableCreation {
        properties,
        ..creation
    }
}

#[cfg(test)]
mod tests {
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
                            let body = if target.starts_with("/v1/config") {
                                r#"{"defaults":{},"overrides":{}}"#.to_string()
                            } else {
                                thread_targets
                                    .lock()
                                    .unwrap_or_else(|error| error.into_inner())
                                    .push(target.clone());
                                response_body(&target)
                            };
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
            let client = crate::catalog_runtime::build_rest_catalog_from_properties(
                HashMap::from([(
                    crate::iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_string(),
                    format!("http://{}", self.address),
                )]),
                RestAccessDelegationMode::Vended,
            )
            .await
            .expect("REST client");
            NovaRocksRestCatalog::new(Arc::new(client), None, RestAccessDelegationMode::Vended)
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
                catalog.delegate.listing.available_positions(),
                super::super::listing_admission::LISTING_CONCURRENCY
            );
        }
    }
}
