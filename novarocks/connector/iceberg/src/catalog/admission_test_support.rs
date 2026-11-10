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

//! Explicitly permissive owner for publication protocol tests only.
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorListingPageSize};

use super::admission::{CatalogAdmissionTarget, CatalogOperation};
use super::error::{CatalogOutcome, CatalogUnsupported};
use super::{
    CatalogNamespaceName, CatalogTableName, ConditionalCreateEvidence, ConditionalCreateVerdict,
    NovaRocksCatalog,
};

type FrozenRecorder =
    Arc<dyn Fn(&[crate::iceberg::TableRequirement], &[crate::iceberg::TableUpdate]) + Send + Sync>;
type CompletionTransformer = Arc<
    dyn Fn(
            super::publication_dispatch::DispatchCompletion,
        ) -> super::publication_dispatch::DispatchCompletion
        + Send
        + Sync,
>;
struct AdmittedCatalog {
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: Option<FrozenRecorder>,
    completion_transformer: Option<CompletionTransformer>,
    snapshot_trace: Option<Arc<Mutex<Vec<super::SnapshotLoading>>>>,
    dispatch_fault: Option<Arc<Mutex<Option<crate::iceberg::ErrorKind>>>>,
}

pub(crate) fn all_admitted(inner: Arc<dyn NovaRocksCatalog>) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        recorder: None,
        completion_transformer: None,
        snapshot_trace: None,
        dispatch_fault: None,
    })
}

pub(crate) fn all_admitted_recording(
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: FrozenRecorder,
) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        recorder: Some(recorder),
        completion_transformer: None,
        snapshot_trace: None,
        dispatch_fault: None,
    })
}
/// Lose the first actually-Applied reply after the real dispatch exits.
/// This test owner never publishes a substitute mutation or fabricates proof.
pub(crate) fn all_admitted_recording_with_lost_reply(
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: FrozenRecorder,
) -> Arc<dyn NovaRocksCatalog> {
    let lost = std::sync::atomic::AtomicBool::new(false);
    let transformer = Arc::new(
        move |completion: super::publication_dispatch::DispatchCompletion| {
            if matches!(
                &completion.evidence,
                crate::commit::attempt::observation::DispatchEvidence::Applied { .. }
            ) && !lost.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // Dropping the completion also drops its unrun optional finalizer.
                // The retained Unknown cannot claim that work completed.
                super::publication_dispatch::DispatchCompletion::evidence(
                    crate::commit::attempt::observation::DispatchEvidence::Unknown {
                        failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                            novarocks_spi::connector::ConnectorMutationFailureKind::Unavailable,
                            "Injected reply loss after the real catalog applied; owner finalization was not observed",
                        ),
                        observation: crate::commit::attempt::observation::ProtocolObservation {
                            adapter: "test-lost-reply",
                            stage: "after-applied",
                            ..Default::default()
                        },
                    },
                )
            } else {
                completion
            }
        },
    );
    Arc::new(AdmittedCatalog {
        inner,
        recorder: Some(recorder),
        completion_transformer: Some(transformer),
        snapshot_trace: None,
        dispatch_fault: None,
    })
}

pub(crate) fn all_admitted_with_snapshot_trace(
    inner: Arc<dyn NovaRocksCatalog>,
    trace: Arc<std::sync::Mutex<Vec<super::SnapshotLoading>>>,
) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        snapshot_trace: Some(trace),
        recorder: None,
        completion_transformer: None,
        dispatch_fault: None,
    })
}

impl AdmittedCatalog {
    fn trace_snapshot_loading(&self, snapshots: super::SnapshotLoading) {
        if let Some(trace) = &self.snapshot_trace {
            trace
                .lock()
                .expect("snapshot loading trace")
                .push(snapshots);
        }
    }
}

pub(crate) fn all_admitted_recording_with_snapshot_trace_and_dispatch_fault(
    inner: Arc<dyn NovaRocksCatalog>,
    recorder: FrozenRecorder,
    snapshot_trace: Arc<Mutex<Vec<super::SnapshotLoading>>>,
    dispatch_fault: Arc<Mutex<Option<crate::iceberg::ErrorKind>>>,
) -> Arc<dyn NovaRocksCatalog> {
    Arc::new(AdmittedCatalog {
        inner,
        recorder: Some(recorder),
        completion_transformer: None,
        snapshot_trace: Some(snapshot_trace),
        dispatch_fault: Some(dispatch_fault),
    })
}

impl std::fmt::Debug for AdmittedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmittedCatalog")
            .field("inner", &self.inner)
            .field("recording", &self.recorder.is_some())
            .finish()
    }
}
struct RecordedDispatch {
    inner: Box<dyn super::publication_dispatch::PreparedCatalogDispatch>,
    recorder: FrozenRecorder,
    completion_transformer: Option<CompletionTransformer>,
    dispatch_fault: Option<Arc<Mutex<Option<crate::iceberg::ErrorKind>>>>,
    requirements: Vec<crate::iceberg::TableRequirement>,
    updates: Vec<crate::iceberg::TableUpdate>,
}
#[async_trait]
impl super::publication_dispatch::PreparedCatalogDispatch for RecordedDispatch {
    async fn dispatch_once(
        self: Box<Self>,
        context: super::publication::EvidenceContext,
    ) -> super::publication_dispatch::DispatchCompletion {
        (self.recorder)(&self.requirements, &self.updates);
        let fault = self
            .dispatch_fault
            .as_ref()
            .and_then(|fault| fault.lock().expect("dispatch fault lock poisoned").take());
        if let Some(kind) = fault {
            use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
            use novarocks_spi::connector::{
                ConnectorMutationFailure, ConnectorMutationFailureKind,
            };
            let rejected = matches!(
                kind,
                crate::iceberg::ErrorKind::CatalogCommitConflicts
                    | crate::iceberg::ErrorKind::PreconditionFailed
            );
            let failure = ConnectorMutationFailure::new(
                if rejected {
                    ConnectorMutationFailureKind::Conflict
                } else {
                    ConnectorMutationFailureKind::Unavailable
                },
                format!("Injected prepared catalog dispatch fault: {kind:?}"),
            );
            let observation = ProtocolObservation {
                adapter: "test-prepared-dispatch",
                stage: "before-delegate",
                ..Default::default()
            };
            return super::publication_dispatch::DispatchCompletion::evidence(if rejected {
                DispatchEvidence::Rejected {
                    failure,
                    observation,
                }
            } else {
                DispatchEvidence::Unknown {
                    failure,
                    observation,
                }
            });
        }
        let completion = self.inner.dispatch_once(context).await;
        match &self.completion_transformer {
            Some(transformer) => transformer(completion),
            None => completion,
        }
    }
}

#[async_trait]
impl NovaRocksCatalog for AdmittedCatalog {
    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        self.inner.bind_publication_generation(generation)
    }
    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        self.inner.positive_attribution_finalization()
    }

    fn evidence_policy(&self) -> crate::catalog::publication::CatalogEvidencePolicy {
        self.inner.evidence_policy()
    }
    fn freeze_create_publication(
        &self,
        request: crate::catalog::catalog_request::CatalogCreatePublicationRequest,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::catalog_request::FrozenCatalogPublication,
    > {
        self.inner.freeze_create_publication(request)
    }
    async fn prepare_catalog_publication(
        &self,
        request: crate::catalog::catalog_request::FrozenCatalogPublication,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
        document: crate::catalog::catalog_request::CatalogResponseDocument,
        read_binding: crate::access_binding::IcebergReadBinding,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        self.inner
            .prepare_catalog_publication(
                request,
                descriptor,
                generation,
                preparation,
                document,
                read_binding,
            )
            .await
    }
    fn materialize_staged_create_response(
        &self,
        table: crate::catalog::CatalogTableName,
        document: Vec<u8>,
    ) -> Result<crate::catalog::StagedCreateMaterial, novarocks_spi::connector::ConnectorError>
    {
        self.inner
            .materialize_staged_create_response(table, document)
    }
    async fn prepare_frozen_publication(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: crate::catalog::admission::CatalogOperation,
        staged_expectation: Option<crate::catalog::StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> crate::catalog::publication_dispatch::PreparationResult<
        crate::catalog::publication_dispatch::PreparedPublication,
    > {
        let requirements = request.requirements().to_vec();
        let updates = request.updates().to_vec();
        let prepared = self
            .inner
            .prepare_frozen_for_admission_test(
                request,
                operation,
                staged_expectation,
                descriptor,
                generation,
                preparation,
            )
            .await?;
        Ok(match (prepared, &self.recorder) {
            (
                super::publication_dispatch::PreparedPublication::Mutation { binding, dispatch },
                Some(recorder),
            ) => super::publication_dispatch::PreparedPublication::Mutation {
                binding,
                dispatch: Box::new(RecordedDispatch {
                    inner: dispatch,
                    recorder: recorder.clone(),
                    completion_transformer: self.completion_transformer.clone(),
                    dispatch_fault: self.dispatch_fault.clone(),
                    requirements,
                    updates,
                }),
            },
            (prepared, _) => prepared,
        })
    }

    fn implementation_name(&self) -> &'static str {
        "admitted-test-owner"
    }
    async fn load_commit_base(
        &self,
        table: super::CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        self.trace_snapshot_loading(super::SnapshotLoading::All);
        self.inner.load_commit_base(table, file_io).await
    }
    fn listing_admission(&self) -> Arc<super::listing_admission::ListingAdmission> {
        self.inner.listing_admission()
    }
    fn admit_operation(
        &self,
        operation: &CatalogOperation,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        operation.validate_target(target)
    }
    async fn list_namespaces(
        &self,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_namespaces(bound).await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.inner.namespace_exists(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_tables(namespace, bound).await
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.table_exists(table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.trace_snapshot_loading(super::SnapshotLoading::All);
        self.inner.load_table(table).await
    }

    async fn load_table_with_snapshots(
        &self,
        table: CatalogTableName,
        snapshots: super::SnapshotLoading,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.trace_snapshot_loading(snapshots);
        self.inner.load_table_with_snapshots(table, snapshots).await
    }

    async fn load_table_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.trace_snapshot_loading(super::SnapshotLoading::All);
        self.inner.load_table_for_read(table, binding).await
    }

    async fn load_table_for_read_with_snapshots(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
        snapshots: super::SnapshotLoading,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.trace_snapshot_loading(snapshots);
        self.inner
            .load_table_for_read_with_snapshots(table, binding, snapshots)
            .await
    }

    async fn load_complete_metadata_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
        validator: Option<&super::CatalogMetadataValidator>,
    ) -> Result<super::CatalogMetadataLoad, ConnectorError> {
        self.trace_snapshot_loading(super::SnapshotLoading::All);
        self.inner
            .load_complete_metadata_for_read(table, binding, validator)
            .await
    }

    async fn load_commit_base_for_request(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
        snapshots: super::SnapshotLoading,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        self.trace_snapshot_loading(snapshots);
        self.inner
            .load_commit_base_for_request(table, file_io, snapshots, binding)
            .await
    }

    async fn load_commit_base_with_snapshots(
        &self,
        table: CatalogTableName,
        file_io: crate::iceberg::io::FileIO,
        snapshots: super::SnapshotLoading,
    ) -> crate::iceberg::Result<super::CatalogCommitBase> {
        self.trace_snapshot_loading(snapshots);
        self.inner
            .load_commit_base_with_snapshots(table, file_io, snapshots)
            .await
    }

    async fn view_exists_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        self.inner.view_exists_for_request(view, context).await
    }
    async fn load_view_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.inner.load_view_for_request(view, context).await
    }
    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.inner.view_exists(view).await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingPageSize,
    ) -> Result<Vec<String>, ConnectorError> {
        self.inner.list_views(namespace, bound).await
    }

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.inner.load_view(view).await
    }

    async fn anchor_written_metadata(
        &self,
        _table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.inner
            .anchor_written_metadata(_table, _metadata_location)
            .await
    }

    async fn adjudicate_conditional_create_for_read(
        &self,
        evidence: ConditionalCreateEvidence,
        binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        self.inner
            .adjudicate_conditional_create_for_read(evidence, binding)
            .await
    }
}

/// Fixture publication through the real prepared owner and sole journal. The
/// caller supplies the immutable base and pure change; no SDK mutation is used.
pub(crate) async fn publish_ref_change(
    owner: Arc<dyn NovaRocksCatalog>,
    table: &crate::iceberg::table::Table,
    change: crate::commit::staging::PreparedChange,
) -> novarocks_spi::connector::ExternalMutationOutcome<()> {
    use crate::commit::model::{AttemptToken, OperationToken, RequestShape};
    use bytes::Bytes;
    use novarocks_spi::connector::*;
    let id = ConnectorMutationOperationId::from_bytes(*uuid::Uuid::now_v7().as_bytes());
    let token = OperationToken::from_mutation(id);
    let generation = ProviderBindingEpoch::from_bytes([0x66; 16]);
    owner.bind_publication_generation(generation).unwrap();
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        64 * 1024,
        1024 * 1024,
    )
    .unwrap();
    let frozen = crate::commit::staging::freeze_existing_change(
        table.identifier().clone(),
        "main",
        table.metadata(),
        table
            .metadata_location()
            .expect("fixture authoritative metadata"),
        RequestShape::RefRetention,
        AttemptToken::new(token, 0),
        change,
        &context,
    )
    .unwrap();
    let evidence = ExternalMutationEvidence::try_new(
        2,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("frozen-fixture").unwrap(),
        },
        generation,
        id,
        "ref_action",
        Bytes::from_static(b"{}"),
    )
    .unwrap();
    let journal = crate::commit::attempt::observation::PublicationJournal::new(token);
    journal.record(evidence, (), &frozen).unwrap();
    let policy = owner.evidence_policy();
    let entry = journal
        .publish_once(
            context,
            policy,
            move |descriptor, generation, preparation| async move {
                owner
                    .prepare_frozen_for_admission_test(
                        frozen,
                        CatalogOperation::CreateBranch,
                        None,
                        descriptor,
                        generation,
                        preparation,
                    )
                    .await
            },
        )
        .await
        .unwrap();
    journal.catalog_entry_outcome(entry)
}

/// Publish a fully frozen fixture through the real owner. Tests supply the
/// original provider identity; the journal owns the only dispatch verdict.
pub(crate) async fn publish_frozen(
    owner: Arc<dyn NovaRocksCatalog>,
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    request: crate::commit::model::FrozenRequest,
    operation: CatalogOperation,
    context: novarocks_spi::connector::ConnectorRequestContext,
    staged_expectation: Option<super::StagedPublicationExpectation>,
) -> super::error::CatalogOutcome<super::transaction::CommitProof> {
    use novarocks_spi::connector::{ConnectorMutationOperationId, ExternalMutationEvidence};
    owner.bind_publication_generation(generation).unwrap();
    let token = request.artifacts().attempt().operation();
    let evidence = ExternalMutationEvidence::try_new(
        2,
        descriptor,
        generation,
        ConnectorMutationOperationId::from_bytes(token.to_bytes()),
        "fixture_publication",
        bytes::Bytes::from_static(b"{}"),
    )
    .unwrap();
    let journal = crate::commit::attempt::observation::PublicationJournal::new(token);
    journal.record(evidence, (), &request).unwrap();
    let policy = owner.evidence_policy();
    let entry = journal
        .publish_once(
            context,
            policy,
            move |descriptor, generation, preparation| async move {
                owner
                    .prepare_frozen_for_admission_test(
                        request,
                        operation,
                        staged_expectation,
                        descriptor,
                        generation,
                        preparation,
                    )
                    .await
            },
        )
        .await
        .unwrap();
    journal.commit_outcome(entry)
}

/// Drive an already-frozen catalog request through the actual owner and sole
/// publication journal. This fixture grants no SDK mutation capability.
pub(crate) async fn publish_catalog(
    owner: Arc<dyn NovaRocksCatalog>,
    request: super::catalog_request::FrozenCatalogPublication,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    context: novarocks_spi::connector::ConnectorRequestContext,
    read_binding: crate::access_binding::IcebergReadBinding,
) -> super::error::CatalogOutcome<super::transaction::CommitProof> {
    publish_catalog_with_document(
        owner,
        request,
        operation_id,
        generation,
        context,
        read_binding,
    )
    .await
    .0
}

/// Retain the actual publication response for invisible staged tables. The
/// same journal and prepared owner decide the outcome before the document is read.
pub(crate) async fn publish_catalog_with_document(
    owner: Arc<dyn NovaRocksCatalog>,
    request: super::catalog_request::FrozenCatalogPublication,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    context: novarocks_spi::connector::ConnectorRequestContext,
    read_binding: crate::access_binding::IcebergReadBinding,
) -> (
    super::error::CatalogOutcome<super::transaction::CommitProof>,
    Option<Vec<u8>>,
) {
    use novarocks_spi::connector::*;
    owner.bind_publication_generation(generation).unwrap();
    let evidence = ExternalMutationEvidence::try_new(
        2,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("catalog-fixture").unwrap(),
        },
        generation,
        operation_id,
        "catalog_fixture",
        bytes::Bytes::from_static(b"{}"),
    )
    .unwrap();
    let journal = crate::commit::attempt::observation::PublicationJournal::new(
        crate::commit::model::OperationToken::from_mutation(operation_id),
    );
    journal
        .prepare_catalog(
            evidence,
            (),
            request.targets(),
            request.request_bytes().to_vec(),
            request.attribution().clone(),
            context.clone(),
        )
        .unwrap();
    let document = super::catalog_request::CatalogResponseDocument::default();
    let response = document.clone();
    let policy = owner.evidence_policy();
    let entry = journal
        .publish_once(
            context,
            policy,
            move |descriptor, generation, preparation| async move {
                owner
                    .prepare_catalog_publication(
                        request,
                        descriptor,
                        generation,
                        preparation,
                        response,
                        read_binding,
                    )
                    .await
            },
        )
        .await
        .unwrap();
    (journal.commit_outcome(entry), document.take())
}

/// Ordinary local fixture setup uses the same frozen publication mechanisms
/// as a catalog request. The explicit epoch belongs to the fixture's provider.
pub(crate) async fn publish_catalog_fixture(
    runtime: &crate::metadata_context::IcebergMetadataContext,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    request: super::catalog_request::CatalogPublicationRequest,
) -> super::error::CatalogOutcome<super::transaction::CommitProof> {
    publish_catalog_fixture_with_owner(
        Arc::clone(runtime.novarocks_catalog()),
        runtime.resources().planning_binding().clone(),
        generation,
        request,
    )
    .await
}

/// Fixture publication retains the exact semantic owner and original binding.
pub(crate) async fn publish_catalog_fixture_with_owner(
    owner: Arc<dyn NovaRocksCatalog>,
    binding: crate::access_binding::IcebergReadBinding,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    request: super::catalog_request::CatalogPublicationRequest,
) -> super::error::CatalogOutcome<super::transaction::CommitProof> {
    use novarocks_spi::connector::{
        ConnectorMutationOperationId, ConnectorRequestContext, ConnectorStopOwner,
    };
    let operation_id = ConnectorMutationOperationId::new();
    let frozen = match request {
        super::catalog_request::CatalogPublicationRequest::CreateTable {
            namespace,
            creation,
            no_op_if_exists,
        } => owner
            .freeze_create_publication(super::catalog_request::CatalogCreatePublicationRequest {
                operation_id: operation_id.to_bytes(),
                namespace,
                creation,
                no_op_if_exists,
            })
            .expect("freeze fixture create without SDK mutation"),
        request => request
            .freeze(operation_id.to_bytes())
            .expect("freeze fixture catalog request"),
    };
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        2 * 1024 * 1024,
        4 * 1024 * 1024,
    )
    .unwrap();
    let binding = binding.for_request(context.clone());
    publish_catalog(owner, frozen, operation_id, generation, context, binding).await
}

/// A local fixture read carries its own admitted context and explicitly
/// requires static materialization; a vended result cannot silently enter it.
pub(crate) async fn load_static_fixture_table(
    runtime: &crate::metadata_context::IcebergMetadataContext,
    ident: &crate::iceberg::TableIdent,
) -> crate::iceberg::table::Table {
    load_static_fixture_table_with_owner(
        Arc::clone(runtime.novarocks_catalog()),
        runtime.resources().planning_binding().clone(),
        ident,
    )
    .await
}

/// Static materialization is explicit even for a read-only SDK fixture seam.
pub(crate) async fn load_static_fixture_table_with_owner(
    owner: Arc<dyn NovaRocksCatalog>,
    binding: crate::access_binding::IcebergReadBinding,
    ident: &crate::iceberg::TableIdent,
) -> crate::iceberg::table::Table {
    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    let context = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        2 * 1024 * 1024,
        4 * 1024 * 1024,
    )
    .unwrap();
    owner
        .load_table_for_read(
            super::CatalogTableName::from_identifier(ident),
            binding.for_request(context),
        )
        .await
        .expect("read fixture through its catalog owner")
        .into_parts()
        .0
        .into_static_table()
        .expect("fixture explicitly requires static storage")
}

/// Build an ordinary local table through frozen namespace/table publication.
/// The caller supplies the exact creation and generation; no schema is inferred.
pub(crate) async fn create_static_fixture_table(
    runtime: &crate::metadata_context::IcebergMetadataContext,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    namespace: super::CatalogNamespaceName,
    creation: crate::iceberg::TableCreation,
) -> crate::iceberg::table::Table {
    create_static_fixture_table_with_owner(
        Arc::clone(runtime.novarocks_catalog()),
        runtime.resources().planning_binding().clone(),
        generation,
        namespace,
        creation,
    )
    .await
}

/// Ordinary setup grants mutation only through an actual frozen owner request.
pub(crate) async fn create_static_fixture_table_with_owner(
    owner: Arc<dyn NovaRocksCatalog>,
    binding: crate::access_binding::IcebergReadBinding,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
    namespace: super::CatalogNamespaceName,
    creation: crate::iceberg::TableCreation,
) -> crate::iceberg::table::Table {
    use super::catalog_request::CatalogPublicationRequest;
    let ident = crate::iceberg::TableIdent::new(
        super::delegate::namespace_ident(&namespace).unwrap(),
        creation.name.clone(),
    );
    let outcome = publish_catalog_fixture_with_owner(
        Arc::clone(&owner),
        binding.clone(),
        generation,
        CatalogPublicationRequest::CreateNamespace {
            namespace: namespace.clone(),
            properties: Default::default(),
            no_op_if_exists: false,
        },
    )
    .await;
    assert!(
        matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
        "{outcome:?}"
    );
    let outcome = publish_catalog_fixture_with_owner(
        Arc::clone(&owner),
        binding.clone(),
        generation,
        CatalogPublicationRequest::CreateTable {
            namespace,
            creation,
            no_op_if_exists: false,
        },
    )
    .await;
    assert!(
        matches!(outcome, CatalogOutcome::KnownCommitted { .. }),
        "{outcome:?}"
    );
    load_static_fixture_table_with_owner(owner, binding, &ident).await
}
