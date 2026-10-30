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
use super::{CatalogNamespaceName, CatalogTableName, NovaRocksCatalog};

/// A Hive Metastore Iceberg catalog.
///
/// HMS is a permanently read-only compatibility entry. Every mutation and
/// transaction constructor uses the same local refusal rule before catalog or
/// filesystem effects. Read operations retain the vendored HMS compatibility.
#[derive(Debug)]
pub(super) struct NovaRocksHiveCatalog {
    delegate: CatalogDelegate,
    client: Arc<crate::iceberg_catalog_hms::HmsCatalog>,
    selected: novarocks_spi::connector::ThriftServiceOrigin,
}

impl NovaRocksHiveCatalog {
    pub(super) fn with_evidence_policy(
        mut self,
        policy: super::publication::CatalogEvidencePolicy,
    ) -> Self {
        self.delegate.evidence_policy = policy;
        self
    }

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
    pub(super) fn adopt(
        client: Arc<crate::iceberg_catalog_hms::HmsCatalog>,
        selected: novarocks_spi::connector::ThriftServiceOrigin,
    ) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
            selected,
        }
    }
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksHiveCatalog {
    fn freeze_create_publication(
        &self,
        _request: super::catalog_request::CatalogCreatePublicationRequest,
    ) -> super::publication_dispatch::PreparationResult<
        super::catalog_request::FrozenCatalogPublication,
    > {
        Err(super::publication_dispatch::PreparationFailure(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                "Hive Iceberg catalog is a read-only compatibility entry",
            ),
        ))
    }

    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        self.delegate.publication_generation.bind(generation)
    }

    async fn prepare_frozen_publication(
        &self,
        _request: crate::commit::model::FrozenRequest,
        _operation: super::admission::CatalogOperation,
        _staged_expectation: Option<super::StagedPublicationExpectation>,
        _descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        _generation: novarocks_spi::connector::ProviderBindingEpoch,
        _preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> super::publication_dispatch::PreparationResult<
        super::publication_dispatch::PreparedPublication,
    > {
        Err(super::publication_dispatch::PreparationFailure(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                "Hive Metastore catalog is a read-only compatibility entry",
            ),
        ))
    }

    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        novarocks_spi::connector::ExternalMutationFinalization::Complete
    }

    fn evidence_policy(&self) -> super::publication::CatalogEvidencePolicy {
        self.delegate.evidence_policy
    }

    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.delegate.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "hive"
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
        let _phase = context
            .admit_selected_io(novarocks_spi::connector::SharedDependency::Thrift(
                self.selected.clone(),
            ))
            .await?;
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        if let Some(observer) = self.delegate.listing.active_observer() {
            let target = None;
            return self
                .delegate
                .listing
                .run_hms(
                    context,
                    observer,
                    super::hms_listing_observer::HmsListingOperation::Namespaces,
                    target,
                    |invocation| async move {
                        self.delegate
                            .list_namespaces_observed(bound, &invocation)
                            .await
                    },
                )
                .await;
        }
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
        let _phase = context
            .admit_selected_io(novarocks_spi::connector::SharedDependency::Thrift(
                self.selected.clone(),
            ))
            .await?;
        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        if let Some(observer) = self.delegate.listing.active_observer() {
            use sha2::Digest;
            let target = Some(sha2::Sha256::digest(namespace.namespace.as_bytes()).into());
            return self
                .delegate
                .listing
                .run_hms(
                    context,
                    observer,
                    super::hms_listing_observer::HmsListingOperation::Tables,
                    target,
                    |invocation| async move {
                        self.delegate
                            .list_tables_observed(&namespace, bound, &invocation)
                            .await
                    },
                )
                .await;
        }
        {
            self.delegate
                .listing
                .run(context, self.list_tables(namespace, bound))
                .await
        }
    }

    async fn list_views_for_request(
        &self,
        _namespace: CatalogNamespaceName,
        _context: novarocks_spi::connector::ConnectorRequestContext,
        _bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        // Not admitted as a listing: there is nothing to list.
        Err(super::views_unsupported("list_views"))
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

    async fn namespace_exists_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "HMS read requires its original request context",
            )
        })?;
        let _phase = context
            .admit_selected_io(novarocks_spi::connector::SharedDependency::Thrift(
                self.selected.clone(),
            ))
            .await?;
        self.delegate.namespace_exists(&namespace).await
    }
    async fn table_exists_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        let context = binding.request_context().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "HMS read requires its original request context",
            )
        })?;
        let _phase = context
            .admit_selected_io(novarocks_spi::connector::SharedDependency::Thrift(
                self.selected.clone(),
            ))
            .await?;
        self.delegate.table_exists(&table).await
    }
    async fn load_table_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        let context = binding.request_context().cloned().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "HMS read requires its original request context",
            )
        })?;
        let _phase = context
            .admit_selected_io(novarocks_spi::connector::SharedDependency::Thrift(
                self.selected.clone(),
            ))
            .await?;
        let ident = super::delegate::table_ident(&table)?;
        // Same Thrift owner; only the metadata FileIO borrows this exact
        // request's original FS/vended gate instead of the generation's view.
        let io = crate::fs_io::build_file_io_for_location("", binding);
        let table = self
            .client
            .load_table_with_file_io(&ident, &io)
            .await
            .map_err(|e| super::error::map_read_error(&e))?;
        Ok(crate::loaded_table::IcebergLoadedTable::new(
            table,
            crate::loaded_table::IcebergAccessDelegation::static_binding(),
        ))
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

    async fn view_exists_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        self.view_exists(view).await
    }

    async fn load_view_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        self.load_view(view).await
    }

    async fn view_exists(&self, _view: CatalogTableName) -> Result<bool, ConnectorError> {
        Err(super::views_unsupported("view_exists"))
    }

    async fn list_views(
        &self,
        _namespace: CatalogNamespaceName,
        _bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        Err(super::views_unsupported("list_views"))
    }

    async fn load_view(
        &self,
        _view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        Err(super::views_unsupported("load_view"))
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
}
