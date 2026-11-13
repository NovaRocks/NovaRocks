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

//! The Hadoop filesystem catalog implementation.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorListingPageSize};

use super::admission::{
    CatalogAdmission, CatalogAdmissionRequest, CatalogAdmissionTarget, CatalogInitiation,
    CatalogOperation,
};
use super::delegate::CatalogDelegate;
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::{
    CatalogCreateIntent, CatalogNamespaceName, CatalogTableName, ConditionalCreateEvidence,
    ConditionalCreateVerdict, NovaRocksCatalog,
};

/// A Hadoop filesystem Iceberg catalog.
///
/// Its create is not a catalog call: the linearization point is a conditional
/// install of the canonical complete `v1.metadata.json` in storage. A lost
/// response needs exact positive readback (ADR-0196). That gives atomic empty-table creation, and
/// it is why this catalog accepts one create intent while refusing the other.
///
/// CTAS is refused before any side effect. There is no staged-create protocol
/// here, and the alternatives are worse than refusing: creating a visible empty
/// table and filling it afterwards exposes a half-built table to readers, and a
/// process-local lock does not fence a second writer.
///
/// This catalog format cannot store a view, so the owner answers every view
/// question with a typed `Unsupported`; saying "no views here" instead would
/// be answering a question it cannot answer.
#[derive(Debug)]
pub(super) struct NovaRocksHadoopCatalog {
    delegate: CatalogDelegate,
    client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
}

impl NovaRocksHadoopCatalog {
    pub(super) fn with_evidence_policy(
        mut self,
        policy: super::publication::CatalogEvidencePolicy,
    ) -> Self {
        self.delegate.evidence_policy = policy;
        self
    }

    pub(super) fn new(client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
        }
    }

    /// The concrete client, for the conditional-create path that has no
    /// equivalent on the generic catalog trait.
    // Reached only by tests; production reaches the client through the dispatch.
    #[allow(dead_code)]
    pub(super) fn conditional_client(
        &self,
    ) -> &Arc<crate::hadoop_catalog::HadoopFileSystemCatalog> {
        &self.client
    }
}

const MAX_BOUNDED_READ_DIAGNOSTIC_BYTES: usize = 4 * 1024;
const OVERSIZED_READ_DIAGNOSTIC: &str = "Hadoop catalog read diagnostic exceeds its bounded limit";

/// Keep control/refusal source kinds and ordinary typed read classification,
/// copying only an admitted borrowed message. The SDK's source/context display
/// can retain an arbitrary remote response and must never be rendered here.
fn map_bounded_read_error(error: &crate::iceberg::Error) -> ConnectorError {
    use std::error::Error as _;
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<ConnectorError>() {
            return error.clone().map_message(|message| {
                if message.len() <= MAX_BOUNDED_READ_DIAGNOSTIC_BYTES {
                    message
                } else {
                    OVERSIZED_READ_DIAGNOSTIC.into()
                }
            });
        }
        source = cause.source();
    }
    let message = if error.message().len() <= MAX_BOUNDED_READ_DIAGNOSTIC_BYTES {
        error.message()
    } else {
        OVERSIZED_READ_DIAGNOSTIC
    };
    ConnectorError::new(super::error::read_error_kind(error.kind()), message)
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksHadoopCatalog {
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
        let namespace = super::delegate::namespace_ident(&request.namespace)?;
        let attempt = self
            .client
            .prepare_create_attempt(
                &namespace,
                request.creation,
                uuid::Uuid::from_bytes(request.operation_id)
                    .simple()
                    .to_string(),
            )
            .map_err(|failure| {
                super::publication_dispatch::PreparationFailure(
                    novarocks_spi::connector::ConnectorMutationFailure::new(
                        match failure.kind {
                            crate::hadoop_catalog::HadoopCreateFailureKind::Invalid => novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                        },
                        failure.message,
                    ),
                )
            })?;
        Ok(
            super::catalog_request::FrozenCatalogPublication::hadoop_create(
                attempt,
                request.operation_id,
                request.no_op_if_exists,
            )?,
        )
    }

    async fn prepare_catalog_publication(
        &self,
        request: super::catalog_request::FrozenCatalogPublication,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
        _document: super::catalog_request::CatalogResponseDocument,
        read_binding: crate::access_binding::IcebergReadBinding,
    ) -> super::publication_dispatch::PreparationResult<
        super::publication_dispatch::PreparedPublication,
    > {
        use novarocks_spi::connector::ConnectorOperationControl;
        preparation.check_active()?;
        self.delegate.publication_generation.require(generation)?;
        request.validate_descriptor(&descriptor)?;
        self.admit_operation(&request.operation, &request.target)
            .map_err(|error| {
                super::publication_dispatch::PreparationFailure(
                    novarocks_spi::connector::ConnectorMutationFailure::new(
                        novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                        error.message(),
                    ),
                )
            })?;
        if request.operation
            == CatalogOperation::CreateTable(super::CatalogCreateIntent::EmptyTable)
        {
            let (attempt, no_op_if_exists) = request.into_hadoop_create()?;
            return Arc::clone(&self.client)
                .prepare_conditional_create(attempt, no_op_if_exists, &preparation)
                .await;
        }
        if request.operation == CatalogOperation::DropTable {
            let (no_op_if_missing, _) = request.table_drop_policy()?;
            let CatalogAdmissionTarget::Table(table) = &request.target else {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Table drop requires a table target",
                )
                .into());
            };
            preparation.check_active()?;
            return Arc::clone(&self.client)
                .prepare_table_drop(
                    super::delegate::table_ident(table)?,
                    no_op_if_missing,
                    preparation,
                )
                .await;
        }
        if !matches!(
            request.operation,
            CatalogOperation::CreateNamespace | CatalogOperation::DropNamespace
        ) {
            return Err(super::publication_dispatch::PreparationFailure(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    "Hadoop catalog request has no prepared mechanism for this operation",
                ),
            ));
        }
        if request.namespace_has_properties {
            return Err(super::publication_dispatch::PreparationFailure(
                novarocks_spi::connector::ConnectorMutationFailure::new(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                    "Hadoop namespace marker does not persist namespace properties",
                ),
            ));
        }
        let CatalogAdmissionTarget::Namespace(namespace) = &request.target else {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "Namespace publication requires a namespace target",
            )
            .into());
        };
        let ident = super::delegate::namespace_ident(namespace)?;
        let exists = self
            .delegate
            .listing
            .run_wait_for_exit(&preparation, async {
                self.client
                    .namespace_exists_for_read(
                        &ident,
                        read_binding.for_request(preparation.clone()),
                    )
                    .await
                    .map_err(|error| map_bounded_read_error(&error))
            })
            .await?;
        if request.namespace_no_op(exists)? {
            return Ok(super::publication_dispatch::PreparedPublication::NoOp);
        }
        preparation.check_active()?;
        Ok(Arc::clone(&self.client).prepare_namespace_publication(
            &ident,
            request.operation == CatalogOperation::CreateNamespace,
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
        self.delegate.publication_generation.require(generation)?;
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
        if staged_expectation.is_some() {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Hadoop catalog has no staged publication protocol",
            )
            .into());
        }
        if !request.has_updates()
            && matches!(
                request.base(),
                crate::commit::model::BaseIdentity::Existing { .. }
            )
        {
            return Ok(super::publication_dispatch::PreparedPublication::NoOp);
        }
        Arc::clone(&self.client)
            .prepare_frozen_update(request, preparation)
            .await
    }

    #[cfg(test)]
    async fn prepare_frozen_for_admission_test(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: CatalogOperation,
        staged_expectation: Option<super::StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> super::publication_dispatch::PreparationResult<
        super::publication_dispatch::PreparedPublication,
    > {
        novarocks_spi::connector::ConnectorOperationControl::check_active(&preparation)?;
        self.delegate.publication_generation.require(generation)?;
        descriptor.validate_frozen(&request)?;
        operation
            .validate_target(&CatalogTableName::from_identifier(request.identifier()).into())
            .map_err(|error| {
                crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, error.message())
            })?;
        if staged_expectation.is_some()
            || !matches!(
                request.base(),
                crate::commit::model::BaseIdentity::Existing { .. }
            )
        {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Hadoop test publication requires an existing frozen table",
            )
            .into());
        }
        if !request.has_updates() {
            return Ok(super::publication_dispatch::PreparedPublication::NoOp);
        }
        Arc::clone(&self.client)
            .prepare_frozen_update(request, preparation)
            .await
    }

    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        novarocks_spi::connector::ExternalMutationFinalization::Failed(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                "Positive attribution does not observe Hadoop publication cache finalization",
            ),
        )
    }

    fn evidence_policy(&self) -> super::publication::CatalogEvidencePolicy {
        self.delegate.evidence_policy
    }

    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.delegate.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "hadoop"
    }

    fn admit_operation(
        &self,
        operation: &CatalogOperation,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        operation.validate_target(target)?;
        match operation {
            CatalogOperation::CreateTable(CatalogCreateIntent::CreateTableAsSelect) => {
                Err(CatalogUnsupported::new(
                    "Hadoop Iceberg catalog has no standard staged-create protocol, so CREATE TABLE AS SELECT cannot publish its target atomically",
                ))
            }
            CatalogOperation::CreateView
            | CatalogOperation::ReplaceView
            | CatalogOperation::DropView => Err(CatalogUnsupported::new(
                "Hadoop Iceberg catalog does not support views",
            )),
            CatalogOperation::CreateDocuments
            | CatalogOperation::UpdateDocuments
            | CatalogOperation::PublishDocuments
            | CatalogOperation::DropDocuments => Err(CatalogUnsupported::new(
                "application-document management requires an Iceberg REST catalog",
            )),
            _ => Ok(()),
        }
    }

    fn admit_initiation(
        &self,
        request: &CatalogAdmissionRequest,
    ) -> Result<CatalogAdmission, CatalogUnsupported> {
        match request.initiation {
            CatalogInitiation::Background => Err(CatalogUnsupported::new(format!(
                "Hadoop Iceberg catalog requires a single writer: background {} is not supported",
                request.operation.name()
            ))),
            CatalogInitiation::Statement => Ok(CatalogAdmission::Admitted),
        }
    }

    async fn list_namespaces(
        &self,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_namespaces(bound).await
    }

    /// The warehouse directory listing is checked against the bound before any
    /// child is probed, so neither the probes nor the retained names can
    /// exceed it.
    async fn list_namespaces_for_read(
        &self,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        let context = binding.request_context().cloned().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "filesystem catalog listing requires an admitted request context",
            )
        })?;
        self.delegate
            .listing
            .run(&context, async {
                let namespaces = self
                    .client
                    .list_namespaces_for_read(binding, bound)
                    .await
                    .map_err(|error| map_bounded_read_error(&error))?;
                Ok(super::delegate::sorted_unique(
                    namespaces
                        .into_iter()
                        .flat_map(|ident| ident.inner())
                        .filter(|name| !name.starts_with('.'))
                        .collect(),
                ))
            })
            .await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.delegate.namespace_exists(&namespace).await
    }

    async fn namespace_exists_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        let ident = super::delegate::namespace_ident(&namespace)?;
        self.client
            .namespace_exists_for_read(&ident, binding)
            .await
            .map_err(|error| map_bounded_read_error(&error))
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_tables(&namespace, bound).await
    }

    /// The namespace directory listing is checked against the bound before
    /// any child is probed for a version hint.
    async fn list_tables_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        let context = binding.request_context().cloned().ok_or_else(|| {
            ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "filesystem catalog listing requires an admitted request context",
            )
        })?;
        self.delegate
            .listing
            .run(&context, async {
                let ident = super::delegate::namespace_ident(&namespace)?;
                let tables = self
                    .client
                    .list_tables_for_read(&ident, binding, bound)
                    .await
                    .map_err(|error| map_bounded_read_error(&error))?;
                Ok(super::delegate::sorted_unique(
                    tables.into_iter().map(|ident| ident.name).collect(),
                ))
            })
            .await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.delegate.table_exists(&table).await
    }

    async fn table_exists_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        let ident = super::delegate::table_ident(&table)?;
        self.client
            .table_exists_for_read(&ident, binding)
            .await
            .map_err(|error| map_bounded_read_error(&error))
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

    async fn load_table_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        let ident = super::delegate::table_ident(&table)?;
        self.client
            .load_table_for_read(&ident, binding)
            .await
            .map(|table| {
                crate::loaded_table::IcebergLoadedTable::new(
                    table,
                    crate::loaded_table::IcebergAccessDelegation::static_binding(),
                )
            })
            .map_err(|error| map_bounded_read_error(&error))
    }

    async fn load_commit_base_for_request(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
        _snapshots: super::SnapshotLoading,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        let ident = super::delegate::table_ident(&table).map_err(|error| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Invalid Hadoop commit table",
            )
            .with_source(error)
        })?;
        super::CatalogCommitBase::from_table(
            self.client
                .load_table_for_commit(&ident, file_io, binding)
                .await?,
        )
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
        _bound: ConnectorListingPageSize,
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
        table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::AnchorWrittenMetadata,
            &table.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }
        // Both the conditional create and retained update protocol publish
        // their own metadata pointer. Admission must not register it again.
        CatalogOutcome::committed(
            table,
            novarocks_spi::connector::ExternalMutationEffect::NoOp,
        )
    }

    async fn adjudicate_conditional_create_for_read(
        &self,
        evidence: ConditionalCreateEvidence,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        let result = self
            .client
            .reconcile_create_attempt_for_read(
                &evidence.namespace,
                &evidence.table,
                &evidence.expected_table_uuid,
                &evidence.metadata_location,
                &evidence.metadata_digest,
                binding,
            )
            .await?;
        Ok(match result {
            crate::hadoop_catalog::HadoopCreateReconciliation::Committed {
                finalization_failure,
            } => ConditionalCreateVerdict::Committed {
                finalization_failure: Some(Arc::from(finalization_failure.unwrap_or_else(|| {
                    "Conditional-create attribution does not observe hint or cache finalization"
                        .to_owned()
                }))),
            },
            crate::hadoop_catalog::HadoopCreateReconciliation::Absent => {
                ConditionalCreateVerdict::Absent
            }
            crate::hadoop_catalog::HadoopCreateReconciliation::Foreign => {
                ConditionalCreateVerdict::Foreign
            }
        })
    }
}

#[cfg(test)]
mod bounded_read_error_tests {
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    use super::{
        MAX_BOUNDED_READ_DIAGNOSTIC_BYTES, OVERSIZED_READ_DIAGNOSTIC, map_bounded_read_error,
    };
    use crate::iceberg::{Error, ErrorKind};

    #[derive(Debug)]
    struct PanicDisplay;

    impl fmt::Display for PanicDisplay {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("bounded projection must not render the remote source")
        }
    }

    impl std::error::Error for PanicDisplay {}

    #[derive(Debug)]
    struct LargeRemoteDisplay {
        response: String,
        displays: Arc<AtomicUsize>,
    }

    impl fmt::Display for LargeRemoteDisplay {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.displays.fetch_add(1, Ordering::SeqCst);
            formatter.write_str(&self.response)
        }
    }

    impl std::error::Error for LargeRemoteDisplay {}

    #[test]
    fn bounded_read_error_never_renders_source_or_context() {
        let error = Error::new(ErrorKind::DataInvalid, "invalid directory metadata")
            .with_context("remote-response", "r".repeat(1024 * 1024))
            .with_source(PanicDisplay);
        let projected = map_bounded_read_error(&error);
        assert_eq!(projected.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(projected.message(), "invalid directory metadata");

        let displays = Arc::new(AtomicUsize::new(0));
        let error = Error::new(ErrorKind::Unexpected, "directory request failed").with_source(
            LargeRemoteDisplay {
                response: "response".repeat(1024 * 1024),
                displays: displays.clone(),
            },
        );
        let projected = map_bounded_read_error(&error);
        assert_eq!(projected.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(projected.message(), "directory request failed");
        assert_eq!(displays.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn bounded_read_error_admits_bytes_before_message_copy() {
        let exact = "🦀".repeat(MAX_BOUNDED_READ_DIAGNOSTIC_BYTES / 4);
        let error =
            Error::new(ErrorKind::FeatureUnsupported, exact.clone()).with_source(PanicDisplay);
        let projected = map_bounded_read_error(&error);
        assert_eq!(projected.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(projected.message(), exact);
        let oversized = Error::new(ErrorKind::TableNotFound, exact + "x").with_source(PanicDisplay);
        let projected = map_bounded_read_error(&oversized);
        assert_eq!(projected.kind(), ConnectorErrorKind::NotFound);
        assert_eq!(projected.message(), OVERSIZED_READ_DIAGNOSTIC);
    }

    #[test]
    fn bounded_read_error_preserves_control_kinds_with_bounded_source_message() {
        for kind in [
            ConnectorErrorKind::ResourceExhausted,
            ConnectorErrorKind::Cancelled,
            ConnectorErrorKind::DeadlineExceeded,
        ] {
            let error = Error::new(ErrorKind::Unexpected, "wrapper")
                .with_source(ConnectorError::new(kind, "typed control reason"));
            let projected = map_bounded_read_error(&error);
            assert_eq!(projected.kind(), kind);
            assert_eq!(projected.message(), "typed control reason");

            let error = Error::new(ErrorKind::Unexpected, "wrapper").with_source(
                ConnectorError::new(kind, "x".repeat(MAX_BOUNDED_READ_DIAGNOSTIC_BYTES + 1)),
            );
            let projected = map_bounded_read_error(&error);
            assert_eq!(projected.kind(), kind);
            assert_eq!(projected.message(), OVERSIZED_READ_DIAGNOSTIC);
        }
    }
}
