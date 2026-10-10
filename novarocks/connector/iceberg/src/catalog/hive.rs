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

//! The Hive Metastore catalog implementation.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorListingBound};

use super::admission::{CatalogAdmissionTarget, CatalogOperation};
use super::delegate::CatalogDelegate;
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateReceipt, ConditionalCreateRequest, ConditionalCreateVerdict, NovaRocksCatalog,
};

/// A Hive Metastore Iceberg catalog.
///
/// HMS is a permanently read-only compatibility entry. Every mutation and
/// transaction constructor uses the same local refusal rule before catalog or
/// filesystem effects. Read operations retain the vendored HMS compatibility.
#[derive(Debug)]
pub(super) struct NovaRocksHiveCatalog {
    delegate: CatalogDelegate,
}

impl NovaRocksHiveCatalog {
    fn read_only(operation: CatalogOperation) -> CatalogUnsupported {
        CatalogUnsupported::new(format!(
            "Hive Metastore catalog is a read-only compatibility entry: {} is not supported; use an Iceberg REST or Hadoop catalog to write",
            operation.name()
        ))
    }

    fn refuse_operation(
        &self,
        operation: CatalogOperation,
        target: impl Into<CatalogAdmissionTarget>,
    ) -> CatalogUnsupported {
        self.admit_operation(&operation, &target.into())
            .expect_err("HMS mutation must be refused before side effects")
    }

    /// Wrap a client the generation already built.
    pub(super) fn adopt(client: Arc<dyn crate::iceberg::Catalog>) -> Self {
        Self {
            delegate: CatalogDelegate::new(client),
        }
    }
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksHiveCatalog {
    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.delegate.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "hive"
    }

    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        Arc::clone(self.delegate.client())
    }

    fn admit_operation(
        &self,
        operation: &CatalogOperation,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        operation.validate_target(target)?;
        Err(Self::read_only(*operation))
    }

    async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_namespaces(bound).await
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
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        {
            let target = None;
            self.delegate
                .listing
                .run_hms(
                    context,
                    super::hms_listing_observer::HmsListingOperation::Namespaces,
                    target,
                    |invocation| async move {
                        self.delegate
                            .list_namespaces_observed(bound, &invocation)
                            .await
                    },
                )
                .await
        }
        #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
        {
            self.delegate
                .listing
                .run(context, self.list_namespaces(bound))
                .await
        }
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
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        {
            use sha2::Digest;
            let target = Some(sha2::Sha256::digest(namespace.namespace.as_bytes()).into());
            self.delegate
                .listing
                .run_hms(
                    context,
                    super::hms_listing_observer::HmsListingOperation::Tables,
                    target,
                    |invocation| async move {
                        self.delegate
                            .list_tables_observed(&namespace, bound, &invocation)
                            .await
                    },
                )
                .await
        }
        #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
        {
            self.delegate
                .listing
                .run(context, self.list_tables(namespace, bound))
                .await
        }
    }

    async fn list_views_for_request(
        &self,
        namespace: CatalogNamespaceName,
        context: novarocks_spi::connector::ConnectorRequestContext,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        {
            use sha2::Digest;
            let target = Some(sha2::Sha256::digest(namespace.namespace.as_bytes()).into());
            self.delegate
                .listing
                .run_hms(
                    &context,
                    super::hms_listing_observer::HmsListingOperation::Views,
                    target,
                    |invocation| async move {
                        self.delegate
                            .list_views_observed(&namespace, bound, &invocation)
                            .await
                    },
                )
                .await
        }
        #[cfg(not(feature = "mem-1-m07-hms-listing-observe"))]
        {
            self.delegate
                .listing
                .run(&context, self.list_views(namespace, bound))
                .await
        }
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.delegate.namespace_exists(&namespace).await
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_tables(&namespace, bound).await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.table_exists(&table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.delegate.load_table(&table).await.map(|table| {
            crate::loaded_table::IcebergLoadedTable::new(
                table,
                crate::loaded_table::IcebergAccessDelegation::static_binding(),
            )
        })
    }

    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.view_exists(&view).await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_views(&namespace, bound).await
    }

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.delegate.load_view(&view).await
    }

    async fn create_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        CatalogOutcome::Unsupported(
            self.refuse_operation(CatalogOperation::CreateNamespace, _namespace),
        )
    }

    async fn drop_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        CatalogOutcome::Unsupported(
            self.refuse_operation(CatalogOperation::DropNamespace, _namespace),
        )
    }

    async fn drop_table(
        &self,
        _table: CatalogTableName,
    ) -> CatalogOutcome<CatalogDropTableReceipt> {
        CatalogOutcome::Unsupported(self.refuse_operation(CatalogOperation::DropTable, _table))
    }

    async fn anchor_written_metadata(
        &self,
        _table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        CatalogOutcome::Unsupported(
            self.refuse_operation(CatalogOperation::AnchorWrittenMetadata, _table),
        )
    }

    async fn stage_create_table(
        &self,
        _namespace: CatalogNamespaceName,
        _creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        super::StagedCreateStart::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect),
            CatalogTableName::new(Arc::clone(&_namespace.namespace), _creation.name),
        ))
    }

    async fn commit_staged_table(
        &self,
        _commit: crate::iceberg::TableCommit,
        _request_file_io: crate::iceberg::io::FileIO,
    ) -> super::StagedCommitResult {
        super::StagedCommitResult::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect),
            CatalogTableName::from_identifier(_commit.identifier()),
        ))
    }

    async fn prepare_conditional_create(
        &self,
        _request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        CatalogOutcome::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            CatalogTableName::new(
                Arc::clone(&_request.namespace.namespace),
                _request.creation.name,
            ),
        ))
    }

    async fn publish_conditional_create(
        &self,
        _attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        CatalogOutcome::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            _attempt.target,
        ))
    }

    async fn adjudicate_conditional_create(
        &self,
        _evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        Err(crate::catalog::admission::connector_unsupported(
            self.refuse_operation(
                CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
                CatalogTableName::new(_evidence.namespace, _evidence.table),
            ),
        ))
    }

    async fn new_transaction(&self, _request: TransactionRequest) -> CatalogTransactionStart {
        CatalogTransactionStart::Unsupported(
            self.refuse_operation(CatalogOperation::Append, _request.target),
        )
    }

    async fn new_create_table_transaction(
        &self,
        request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        CatalogTransactionStart::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(request.intent),
            request.target,
        ))
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        CatalogTransactionStart::Unsupported(self.refuse_operation(
            CatalogOperation::CreateTable(_request.intent),
            _request.target,
        ))
    }
}
