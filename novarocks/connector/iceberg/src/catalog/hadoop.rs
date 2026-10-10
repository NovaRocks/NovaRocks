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
use novarocks_spi::connector::{ConnectorError, ConnectorListingBound};

use super::admission::{
    CatalogAdmission, CatalogAdmissionRequest, CatalogAdmissionTarget, CatalogInitiation,
    CatalogOperation,
};
use super::delegate::CatalogDelegate;
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::transaction::{CreateTableTransactionRequest, TransactionRequest};
use super::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateFacts, ConditionalCreateReceipt, ConditionalCreateRequest,
    ConditionalCreateVerdict, NovaRocksCatalog,
};

/// A Hadoop filesystem Iceberg catalog.
///
/// Its create is not a catalog call: the linearization point is a conditional
/// write of the canonical `v1.metadata.json` in storage, followed by an
/// authoritative reread (ADR-0077). That gives atomic empty-table creation, and
/// it is why this catalog accepts one create intent while refusing the other.
///
/// CTAS is refused before any side effect. There is no staged-create protocol
/// here, and the alternatives are worse than refusing: creating a visible empty
/// table and filling it afterwards exposes a half-built table to readers, and a
/// process-local lock does not fence a second writer.
///
/// Views are not special-cased. The Hadoop client does not implement the view
/// methods, so delegation already yields a typed `Unsupported` — this catalog
/// format cannot store a view, and saying "no views here" instead would be
/// answering a question it cannot answer.
#[derive(Debug)]
pub(super) struct NovaRocksHadoopCatalog {
    delegate: CatalogDelegate,
    client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
}

impl NovaRocksHadoopCatalog {
    pub(super) fn new(client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>) -> Self {
        Self {
            delegate: CatalogDelegate::new(client.clone()),
            client,
        }
    }

    #[cfg(test)]
    pub(super) fn new_with_vendored_client_for_test(
        client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
        vendored_client: Arc<dyn crate::iceberg::Catalog>,
    ) -> Self {
        Self {
            delegate: CatalogDelegate::new(vendored_client),
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
    use novarocks_spi::connector::ConnectorErrorKind;
    let (kind, message) = match std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<ConnectorError>())
    {
        Some(source)
            if matches!(
                source.kind(),
                ConnectorErrorKind::ResourceExhausted
                    | ConnectorErrorKind::Cancelled
                    | ConnectorErrorKind::DeadlineExceeded
            ) =>
        {
            (source.kind(), source.message())
        }
        _ => (super::error::read_error_kind(error.kind()), error.message()),
    };
    let message = if message.len() <= MAX_BOUNDED_READ_DIAGNOSTIC_BYTES {
        message
    } else {
        OVERSIZED_READ_DIAGNOSTIC
    };
    ConnectorError::new(kind, message)
}

#[async_trait]
impl NovaRocksCatalog for NovaRocksHadoopCatalog {
    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        Arc::clone(&self.delegate.listing)
    }

    fn implementation_name(&self) -> &'static str {
        "hadoop"
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
            CatalogInitiation::StatementJob => Ok(CatalogAdmission::AdmittedAwaitingCompletion),
            CatalogInitiation::Statement | CatalogInitiation::JobAttempt => {
                Ok(CatalogAdmission::Admitted)
            }
        }
    }

    async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_namespaces(bound).await
    }

    /// The warehouse directory listing is checked against the bound before any
    /// child is probed, so neither the probes nor the retained names can
    /// exceed it.
    async fn list_namespaces_for_read(
        &self,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
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
            .map_err(|error| super::error::map_read_error(&error))
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.delegate.list_tables(&namespace, bound).await
    }

    /// The namespace directory listing is checked against the bound before
    /// any child is probed for a version hint.
    async fn list_tables_for_read(
        &self,
        namespace: CatalogNamespaceName,
        binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
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
            .map_err(|error| super::error::map_read_error(&error))
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
            .map_err(|error| super::error::map_read_error(&error))
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
        super::CatalogCommitBase::from_table(
            self.client.load_table_for_commit(&ident, file_io).await?,
        )
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
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::AnchorWrittenMetadata,
            &table.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }
        // The namespace has to exist before the table can be anchored under it.
        // The previous helper created it with `let _ =`, so a namespace that
        // failed to appear surfaced later as a confusing registration failure
        // instead of the thing that actually went wrong.
        let namespace = CatalogNamespaceName::new(Arc::clone(&table.namespace));
        match self.delegate.namespace_exists(&namespace).await {
            Ok(true) => {}
            Ok(false) => {
                let created = self.delegate.create_namespace(namespace).await;
                if !matches!(created, CatalogOutcome::KnownCommitted { .. }) {
                    return match created {
                        CatalogOutcome::KnownCommitted { .. } => unreachable!(),
                        CatalogOutcome::Unsupported(reason) => CatalogOutcome::Unsupported(reason),
                        CatalogOutcome::KnownUncommitted { failure } => {
                            CatalogOutcome::KnownUncommitted { failure }
                        }
                        CatalogOutcome::CommitUnknown { failure, evidence } => {
                            CatalogOutcome::CommitUnknown { failure, evidence }
                        }
                    };
                }
            }
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                    error.to_string(),
                );
            }
        }
        self.delegate.register_table(table, metadata_location).await
    }

    /// Prepare the conditional metadata write that makes this table exist.
    ///
    /// Local only: it builds metadata and sends nothing, which is what lets the
    /// caller freeze publication evidence before the attempt is dispatched.
    async fn stage_create_table(
        &self,
        _namespace: CatalogNamespaceName,
        _creation: crate::iceberg::TableCreation,
    ) -> super::StagedCreateStart {
        super::StagedCreateStart::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog has no staged-create protocol",
        ))
    }

    async fn commit_staged_table(
        &self,
        _commit: crate::iceberg::TableCommit,
        _request_file_io: crate::iceberg::io::FileIO,
    ) -> super::StagedCommitResult {
        super::StagedCommitResult::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog has no staged-create protocol",
        ))
    }

    async fn prepare_conditional_create(
        &self,
        request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        let target = CatalogTableName::new(
            Arc::clone(&request.namespace.namespace),
            request.creation.name.clone(),
        );
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            &target.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }

        let namespace = match super::delegate::namespace_ident(&request.namespace) {
            Ok(ident) => ident,
            Err(error) => {
                return CatalogOutcome::uncommitted(
                    novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                    error.to_string(),
                );
            }
        };
        match self.client.prepare_create_attempt(
            &namespace,
            request.creation,
            request.operation_id.to_string(),
        ) {
            Ok(attempt) => {
                let facts = facts_from_hadoop(attempt.facts());
                CatalogOutcome::committed(
                    ConditionalCreateAttempt::hadoop(attempt, facts, target),
                    novarocks_spi::connector::ExternalMutationEffect::NoOp,
                )
            }
            // Preparing never dispatches, so every failure here is proven
            // uncommitted -- including an unsupported storage binding, which is
            // checked before any directory is created.
            Err(failure) => map_prepare_failure(&failure),
        }
    }

    async fn publish_conditional_create(
        &self,
        attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        if let Err(reason) = self.admit_operation(
            &CatalogOperation::CreateTable(CatalogCreateIntent::EmptyTable),
            &attempt.target.clone().into(),
        ) {
            return CatalogOutcome::Unsupported(reason);
        }

        let facts = attempt.facts.clone();
        let Some(attempt) = attempt.into_hadoop() else {
            return CatalogOutcome::uncommitted(
                novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                "conditional create attempt was not prepared by this catalog",
            );
        };
        match self.client.publish_create_attempt(attempt).await {
            Ok(result) => CatalogOutcome::committed(
                ConditionalCreateReceipt {
                    facts,
                    already_existed: matches!(
                        result.disposition,
                        crate::hadoop_catalog::HadoopCreateDisposition::Existing
                    ),
                    authoritative_table_uuid: Arc::from(result.authoritative_table_uuid),
                    authoritative_metadata_digest: Arc::from(result.authoritative_metadata_digest),
                    published_metadata_location: result.table.metadata_location().map(Arc::from),
                    finalization_failure: result.finalization_failure.map(Arc::from),
                },
                novarocks_spi::connector::ExternalMutationEffect::Applied,
            ),
            Err(failure) => map_publish_failure(&failure, &facts),
        }
    }

    async fn adjudicate_conditional_create(
        &self,
        evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        let outcome = self
            .client
            .reconcile_create_attempt(
                &evidence.namespace,
                &evidence.table,
                &evidence.expected_table_uuid,
                &evidence.metadata_location,
                &evidence.metadata_digest,
            )
            .await
            .map_err(|error| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    error,
                )
            })?;
        Ok(match outcome {
            crate::hadoop_catalog::HadoopCreateReconciliation::Committed {
                finalization_failure,
            } => ConditionalCreateVerdict::Committed {
                finalization_failure: finalization_failure.map(Arc::from),
            },
            crate::hadoop_catalog::HadoopCreateReconciliation::Absent => {
                ConditionalCreateVerdict::Absent
            }
            crate::hadoop_catalog::HadoopCreateReconciliation::Foreign => {
                ConditionalCreateVerdict::Foreign
            }
        })
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
        match request.intent {
            CatalogCreateIntent::EmptyTable => {
                // This catalog's create is a conditional metadata write, not a
                // catalog call, so the transaction is built around that
                // primitive rather than around `create_table`.
                let target = request.target.clone();
                let namespace = CatalogNamespaceName::new(Arc::clone(&target.namespace));
                let prepared = self
                    .prepare_conditional_create(super::ConditionalCreateRequest {
                        namespace,
                        creation: request.creation,
                        operation_id: Arc::from(request.identity.hex()),
                    })
                    .await;
                let Some((attempt, _effect, _witness)) = prepared.into_known_committed() else {
                    // Preparation sends nothing, so a failure here is proven
                    // uncommitted; report it without inventing a transaction.
                    return CatalogTransactionStart::KnownUncommitted {
                        failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
                            "conditional create could not be prepared",
                        ),
                    };
                };
                let evidence = super::ConditionalCreateEvidence {
                    namespace: Arc::clone(&target.namespace),
                    table: Arc::clone(&target.name),
                    expected_table_uuid: Arc::clone(&attempt.facts.table_uuid),
                    metadata_location: Arc::clone(&attempt.facts.metadata_location),
                    metadata_digest: Arc::clone(&attempt.facts.metadata_digest),
                };
                let commit_evidence =
                    super::error::CatalogCommitEvidence::for_target(target.canonical())
                        .with_target_uuid(Arc::clone(&attempt.facts.table_uuid))
                        .with_metadata_location(Arc::clone(&attempt.facts.metadata_location));
                // What admission observed, so the caller can freeze publication
                // evidence before anything is dispatched.
                let admission = super::transaction::AdmissionFacts {
                    table_uuid: Some(Arc::clone(&attempt.facts.table_uuid)),
                    metadata_location: Some(Arc::clone(&attempt.facts.metadata_location)),
                    metadata_digest: Some(Arc::clone(&attempt.facts.metadata_digest)),
                };
                CatalogTransactionStart::Ready(Box::new(
                    super::transaction::Transaction::new(
                        request.identity,
                        target,
                        super::transaction::TransactionShape::Create(request.intent),
                        commit_evidence,
                        Arc::new(super::dispatch::ConditionalCreateDispatch::new(
                            Arc::clone(&self.client),
                            attempt
                                .into_hadoop()
                                .expect("this catalog prepared the attempt"),
                            evidence,
                        )),
                    )
                    .with_admission_facts(admission),
                ))
            }
            CatalogCreateIntent::CreateTableAsSelect => match self.admit_operation(
                &CatalogOperation::CreateTable(request.intent),
                &request.target.clone().into(),
            ) {
                Ok(()) => super::start_create_table_transaction(&self.delegate, request),
                Err(reason) => CatalogTransactionStart::Unsupported(reason),
            },
        }
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        CatalogTransactionStart::Unsupported(CatalogUnsupported::new(
            "Hadoop Iceberg catalog cannot replace a table atomically",
        ))
    }
}

fn facts_from_hadoop(
    facts: &crate::hadoop_catalog::HadoopCreateAttemptFacts,
) -> ConditionalCreateFacts {
    ConditionalCreateFacts {
        operation_id: Arc::from(facts.operation_id.as_str()),
        table_uuid: Arc::from(facts.table_uuid.as_str()),
        metadata_location: Arc::from(facts.metadata_location.as_str()),
        metadata_digest: Arc::from(facts.metadata_digest.as_str()),
    }
}

/// Preparing sends nothing, so its failures are all proven uncommitted.
fn map_prepare_failure<T>(
    failure: &crate::hadoop_catalog::HadoopCreateFailure,
) -> CatalogOutcome<T> {
    use crate::hadoop_catalog::HadoopCreateFailureKind as Kind;
    use novarocks_spi::connector::ConnectorMutationFailureKind as Neutral;
    match failure.kind {
        Kind::Unsupported => CatalogOutcome::unsupported(failure.message.clone()),
        Kind::Invalid => CatalogOutcome::uncommitted(Neutral::InvalidRequest, message(failure)),
        // Preparation cannot reach these, but classifying them as unknown keeps
        // the conservative answer if it ever does.
        Kind::Uncommitted => CatalogOutcome::uncommitted(Neutral::Unavailable, message(failure)),
        Kind::Unknown => CatalogOutcome::unknown(
            message(failure),
            super::error::CatalogCommitEvidence::default(),
        ),
    }
}

// Used by `publish_conditional_create`, which only tests reach.
#[allow(dead_code)]
fn map_publish_failure<T>(
    failure: &crate::hadoop_catalog::HadoopCreateFailure,
    facts: &ConditionalCreateFacts,
) -> CatalogOutcome<T> {
    use crate::hadoop_catalog::HadoopCreateFailureKind as Kind;
    use novarocks_spi::connector::ConnectorMutationFailureKind as Neutral;
    match failure.kind {
        Kind::Unsupported => CatalogOutcome::unsupported(failure.message.clone()),
        Kind::Invalid => CatalogOutcome::uncommitted(Neutral::InvalidRequest, message(failure)),
        Kind::Uncommitted => CatalogOutcome::uncommitted(Neutral::Unavailable, message(failure)),
        // The conditional write may have landed. Carry the exact identity a
        // read-only adjudication needs; nothing here may retry or delete.
        Kind::Unknown => CatalogOutcome::unknown(
            message(failure),
            super::error::CatalogCommitEvidence::default()
                .with_target_uuid(Arc::clone(&facts.table_uuid))
                .with_metadata_location(Arc::clone(&facts.metadata_location)),
        ),
    }
}

fn message(failure: &crate::hadoop_catalog::HadoopCreateFailure) -> String {
    match failure.facts.as_ref() {
        Some(facts) => format!("{} [operation_id={}]", failure.message, facts.operation_id),
        None => failure.message.clone(),
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
