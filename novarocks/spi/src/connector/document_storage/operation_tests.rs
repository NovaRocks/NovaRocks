use super::super::*;
use super::*;
use crate::connector::{
    CatalogVersion, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId,
    ConnectorStopOwner,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .unwrap()
}
fn descriptor() -> ConnectorInstanceDescriptor {
    ConnectorInstanceDescriptor {
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
    }
}
fn catalog(version: u8) -> CatalogHandle {
    CatalogHandle::new(
        descriptor().instance_id,
        CatalogVersion::from_bytes([version; 32]),
    )
}
fn request() -> KeyedDocumentObservationRequest {
    KeyedDocumentObservationRequest {
        owner: ConnectorProviderBindingKey {
            instance_id: descriptor().instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
        },
        catalog_handle: catalog(7),
        target: ConnectorTableIdentity {
            instance_id: descriptor().instance_id,
            namespace: "ns".into(),
            table: "table".into(),
        },
        object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"object")).unwrap(),
        context: context(),
    }
}
fn metadata() -> crate::connector::ConnectorCommittedVersion {
    crate::connector::ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata"), Some(7))
        .unwrap()
}
struct Management {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    calls: Arc<AtomicUsize>,
}
impl ConnectorDocumentStorageManagement for Management {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    fn admit_management(
        &self,
        _request: ConnectorDocumentManagementAdmissionRequest,
    ) -> Result<Bytes, ConnectorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::from_static(b"admission"))
    }
}
struct Observation {
    calls: Arc<AtomicUsize>,
    descriptor: ConnectorInstanceDescriptor,
    foreign_object: bool,
    foreign_token: bool,
}
impl ConnectorDocumentStorageObservation for Observation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        request().owner.incarnation
    }
    fn observe_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(KeyedDocumentView {
            owner: request.owner,
            table: request.target,
            object_id: if self.foreign_object {
                ConnectorTableObjectId::try_new(Bytes::from_static(b"foreign")).unwrap()
            } else {
                request.object_id
            },
            metadata_version: metadata(),
            current_snapshot: Some(7),
            current_snapshot_file_totals: SnapshotFileTotals {
                data_files: Some(0),
                delete_files: Some(0),
            },
            slots: Default::default(),
            markers: Default::default(),
            versions: Default::default(),
            roots: Default::default(),
            current_records: vec![],
            management: None,
        })
    }
    fn observe_full_keyed_documents(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        let current = self.observe_keyed_documents(request.clone())?;
        let version = if self.foreign_token {
            crate::connector::ConnectorCommittedVersion::try_new(
                Bytes::from_static(b"other metadata"),
                Some(8),
            )
            .unwrap()
        } else {
            current.metadata_version.clone()
        };
        Ok(FullKeyedDocumentView {
            current,
            snapshots: vec![],
            refs: Default::default(),
            diagnostics: vec![],
            token: FullObservationToken::try_new(
                request.owner,
                request.target,
                request.object_id,
                version,
                Bytes::from_static(b"provider full metadata"),
            )?,
        })
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: crate::connector::ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        unreachable!("not used by these lease tests")
    }
    fn resolve_keyed_commit(
        &self,
        _: KeyedDocumentObservationRequest,
        _: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        unreachable!("not used by these lease tests")
    }
    fn discover_documents(
        &self,
        request: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut target = super::tests::request().target;
        target.namespace = "escaped".into();
        ConnectorDocumentDiscoveryPage::try_new(
            vec![ConnectorDocumentDiscoveryItem::try_new(
                target,
                super::tests::request().object_id,
                metadata(),
                ConnectorManagedObjectMarker::try_new("materialized-view", "owner", "incarnation")?,
            )?],
            None,
            ConnectorDocumentDiscoveryCompleteness::Complete,
            &request,
        )
    }
}
fn lease_with(
    observation: Option<Arc<dyn ConnectorDocumentStorageObservation>>,
    management: Option<Arc<dyn ConnectorDocumentStorageManagement>>,
    drops: Arc<AtomicUsize>,
) -> ConnectorDocumentStorageLease {
    let descriptor = descriptor();
    let binding = ConnectorDocumentStorageBinding::try_new(
        descriptor.clone(),
        request().owner.incarnation,
        observation,
        management,
    )
    .unwrap();
    ConnectorDocumentStorageLease::new(
        descriptor,
        ConnectorControlRuntimeId::new(),
        request().owner.incarnation,
        catalog(7),
        binding,
        move || {
            drops.fetch_add(1, Ordering::SeqCst);
        },
    )
    .unwrap()
}
fn observation(
    calls: Arc<AtomicUsize>,
    foreign_object: bool,
    foreign_token: bool,
) -> Arc<Observation> {
    Arc::new(Observation {
        calls,
        descriptor: descriptor(),
        foreign_object,
        foreign_token,
    })
}
fn admission_request() -> ConnectorDocumentManagementAdmissionRequest {
    let r = request();
    ConnectorDocumentManagementAdmissionRequest::try_new(
        r.owner,
        r.catalog_handle,
        ConnectorMutationOperationId::from_bytes([8; 16]),
        r.target,
        Some(r.object_id),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        r.context,
    )
    .unwrap()
}

#[test]
fn full_token_becomes_expiry_authority_only_after_the_exact_lease_validates_it() {
    let obs = observation(Default::default(), false, false);
    let raw = obs.observe_full_keyed_documents(request()).unwrap();
    assert!(raw.token.validate_sealed().is_err());
    let management = Arc::new(Management {
        descriptor: descriptor(),
        incarnation: request().owner.incarnation,
        calls: Default::default(),
    });
    let lease = lease_with(Some(obs), Some(management), Default::default());
    let admitted = lease.admit_management(admission_request()).unwrap();
    let make = |token| KeyedMutationIntent {
        admission: admitted.clone(),
        expected_metadata: metadata(),
        expected_snapshot: Some(7),
        mutation: KeyedMutation::Expiry(KeyedExpiry {
            observation: token,
            remove_snapshots: Default::default(),
        }),
    };
    assert!(
        make(raw.token)
            .validate_for(lease.owner(), admitted.operation_id())
            .is_err()
    );
    let validated = lease.observe_full_keyed_documents(request()).unwrap();
    validated.token.validate_sealed().unwrap();
    make(validated.token)
        .validate_for(lease.owner(), admitted.operation_id())
        .unwrap();
}
#[test]
fn foreign_catalog_generation_is_rejected_before_observation_or_management_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        Some(observation(calls.clone(), false, false)),
        Some(Arc::new(Management {
            descriptor: descriptor(),
            incarnation: request().owner.incarnation,
            calls: calls.clone(),
        })),
        Default::default(),
    );
    let mut wrong = request();
    wrong.catalog_handle = catalog(9);
    assert!(lease.observe_keyed_documents(wrong).is_err());
    let r = request();
    let wrong = ConnectorDocumentManagementAdmissionRequest::try_new(
        r.owner,
        catalog(9),
        ConnectorMutationOperationId::new(),
        r.target,
        Some(r.object_id),
        ConnectorDocumentManagementOperation::SingleTargetUpdate,
        r.context,
    )
    .unwrap();
    assert_eq!(
        lease.admit_management(wrong).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[test]
fn foreign_object_or_full_metadata_token_cannot_be_sealed() {
    for (object, token) in [(true, false), (false, true)] {
        let lease = lease_with(
            Some(observation(Default::default(), object, token)),
            None,
            Default::default(),
        );
        assert!(lease.observe_full_keyed_documents(request()).is_err());
    }
}
#[test]
fn mismatched_capability_generation_is_rejected_at_binding() {
    let management = Arc::new(Management {
        descriptor: descriptor(),
        incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
        calls: Default::default(),
    });
    assert!(
        ConnectorDocumentStorageBinding::try_new(
            descriptor(),
            request().owner.incarnation,
            None,
            Some(management)
        )
        .is_err()
    );
}
#[test]
fn management_only_binding_does_not_synthesize_an_observation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        None,
        Some(Arc::new(Management {
            descriptor: descriptor(),
            incarnation: request().owner.incarnation,
            calls: calls.clone(),
        })),
        Default::default(),
    );
    lease.admit_management(admission_request()).unwrap();
    assert!(
        matches!(lease.observe_keyed_documents(request()), Err(KeyedDocumentReadError::Invalid(error)) if error.kind() == ConnectorErrorKind::Unsupported)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[test]
fn retained_lease_releases_generation_once_after_the_last_clone() {
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = lease_with(
        Some(observation(Default::default(), false, false)),
        None,
        drops.clone(),
    );
    let retained = lease.clone();
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[test]
fn discovery_cannot_escape_the_original_catalog_namespace() {
    let lease = lease_with(
        Some(observation(Default::default(), false, false)),
        None,
        Default::default(),
    );
    let r = request();
    let request = ConnectorDocumentDiscoveryRequest::try_new(
        r.owner,
        r.catalog_handle,
        Some("requested".into()),
        1,
        ConnectorDocumentDiscoveryBudget::try_new(1).unwrap(),
        context(),
    )
    .unwrap();
    assert_eq!(
        lease.discover_documents(request).unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn discovery_continuation_requires_a_sealed_page_and_a_shared_reduced_item_budget() {
    let origin = request();
    let request = ConnectorDocumentDiscoveryRequest::try_new(
        origin.owner,
        origin.catalog_handle,
        Some("ns".into()),
        2,
        ConnectorDocumentDiscoveryBudget::try_new(3).unwrap(),
        context(),
    )
    .unwrap();
    let item = ConnectorDocumentDiscoveryItem::try_new(
        origin.target,
        origin.object_id,
        metadata(),
        ConnectorManagedObjectMarker::try_new("materialized-view", "owner", "incarnation").unwrap(),
    )
    .unwrap();
    let mut page = ConnectorDocumentDiscoveryPage::try_new(
        vec![item],
        Some(Bytes::from_static(b"next")),
        ConnectorDocumentDiscoveryCompleteness::Incomplete(
            ConnectorDocumentDiscoveryIncompleteReason::PageBoundary,
        ),
        &request,
    )
    .unwrap();
    assert!(page.try_next_request(&request, context()).is_err());
    request.reserve_page(page.items().len()).unwrap();
    page.seal_for(&request);
    let next = page.try_next_request(&request, context()).unwrap().unwrap();
    assert_eq!(next.cursor().map(Bytes::as_ref), Some(b"next".as_slice()));
    assert_eq!(next.remaining_items(), 2);
    assert_eq!(next.page_size(), 2);
    let concurrent = next.clone();
    next.reserve_page(2).unwrap();
    assert_eq!(
        concurrent.reserve_page(1).unwrap_err().kind(),
        ConnectorErrorKind::ResourceExhausted
    );
}

fn publication_declaration(
    publication_id: LakePublicationId,
    technique: crate::connector::ConnectorManagedPublicationTechnique,
    empty_input: crate::connector::ConnectorManagedPublicationEmptyInputDisposition,
    replacement: Option<crate::connector::ConnectorManagedPartitionSpecReplacement>,
    preview: Option<crate::connector::ConnectorCommittedPartitioning>,
) -> Result<ConnectorDocumentPublicationDeclaration, ConnectorError> {
    let lease = lease_with(
        None,
        Some(Arc::new(Management {
            descriptor: descriptor(),
            incarnation: request().owner.incarnation,
            calls: Default::default(),
        })),
        Default::default(),
    );
    let target = request();
    let admission =
        lease.admit_management(ConnectorDocumentManagementAdmissionRequest::try_new(
            target.owner,
            target.catalog_handle,
            ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
            target.target,
            Some(target.object_id.clone()),
            ConnectorDocumentManagementOperation::Publication,
            target.context,
        )?)?;
    ConnectorDocumentPublicationDeclaration::try_new(
        publication_id,
        admission,
        target.object_id,
        ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"exact-write-base"))?,
        technique,
        empty_input,
        replacement,
        preview,
    )
}

fn publication_partition_replacement(
    publication_id: LakePublicationId,
) -> crate::connector::ConnectorManagedPartitionSpecReplacement {
    use crate::connector::{
        ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
        ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionTransform,
    };
    ConnectorManagedPartitionSpecReplacement::try_new(
        publication_id.into(),
        ConnectorManagedPartitionSpecObservation::try_from_fields(0, &[]).unwrap(),
        vec![
            ConnectorManagedPartitionField::try_new(
                1,
                0,
                ConnectorManagedPartitionTransform::Identity,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn publication_partition_preview() -> crate::connector::ConnectorCommittedPartitioning {
    use crate::connector::{
        ConnectorCommittedPartitionField, ConnectorCommittedPartitioning,
        ConnectorManagedPartitionTransform,
    };
    ConnectorCommittedPartitioning::try_new(
        1,
        vec![
            ConnectorCommittedPartitionField::try_new(
                1000,
                "id",
                1,
                "id",
                0,
                ConnectorManagedPartitionTransform::Identity,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn document_publication_preserves_the_exact_atomic_partition_replacement() {
    use crate::connector::{
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        ConnectorManagedPublicationTechnique::Full,
    };
    let publication_id = LakePublicationId::new_v7();
    let replacement = publication_partition_replacement(publication_id);
    let preview = publication_partition_preview();
    let declaration = publication_declaration(
        publication_id,
        Full,
        CommitEmptyWrite,
        Some(replacement.clone()),
        Some(preview.clone()),
    )
    .unwrap();
    assert_eq!(declaration.publication_id(), publication_id);
    assert_eq!(declaration.partition_spec_replacement(), Some(&replacement));
    assert_eq!(
        declaration.expected_committed_partitioning(),
        Some(&preview)
    );
    declaration
        .validate_for_write(Some(declaration.expected_base()))
        .unwrap();
    assert!(
        declaration
            .validate_for_write(Some(
                &ConnectorWriteBaseVersion::try_new(Bytes::from_static(b"another-base")).unwrap()
            ))
            .is_err()
    );
}

#[test]
fn document_partition_replacement_requires_full_publication_and_committed_empty_input() {
    use crate::connector::{
        ConnectorManagedPublicationEmptyInputDisposition::{
            AbortWithoutExternalCommit, CommitEmptyWrite,
        },
        ConnectorManagedPublicationTechnique::{Full, Incremental, MetadataOnly},
    };
    for technique in [Full, Incremental, MetadataOnly] {
        for empty_input in [CommitEmptyWrite, AbortWithoutExternalCommit] {
            if technique == Full && empty_input == CommitEmptyWrite {
                continue;
            }
            let publication_id = LakePublicationId::new_v7();
            let error = publication_declaration(
                publication_id,
                technique,
                empty_input,
                Some(publication_partition_replacement(publication_id)),
                Some(publication_partition_preview()),
            )
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
            assert!(error.message().contains("requires a full publication"));
        }
    }
}

#[test]
fn document_partition_replacement_and_exact_preview_must_be_paired() {
    use crate::connector::{
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        ConnectorManagedPublicationTechnique::Full,
    };
    let publication_id = LakePublicationId::new_v7();
    for (replacement, preview) in [
        (
            Some(publication_partition_replacement(publication_id)),
            None,
        ),
        (None, Some(publication_partition_preview())),
    ] {
        let error =
            publication_declaration(publication_id, Full, CommitEmptyWrite, replacement, preview)
                .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.message().contains("replacement is incomplete"));
    }
    let declaration =
        publication_declaration(publication_id, Full, CommitEmptyWrite, None, None).unwrap();
    assert!(declaration.partition_spec_replacement().is_none());
    assert!(declaration.expected_committed_partitioning().is_none());
}

#[test]
fn document_partition_replacement_is_bound_to_its_publication_operation() {
    use crate::connector::{
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        ConnectorManagedPublicationTechnique::Full,
    };
    let error = publication_declaration(
        LakePublicationId::new_v7(),
        Full,
        CommitEmptyWrite,
        Some(publication_partition_replacement(
            LakePublicationId::new_v7(),
        )),
        Some(publication_partition_preview()),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(
        error
            .message()
            .contains("does not match the write operation")
    );
}

// This gate represents only the provider's physical observation. The returned
// view still has to pass the ORIGINAL lease's exact validation after Ready.
struct AsyncObservation {
    original: Observation,
    gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
    async_calls: Arc<AtomicUsize>,
}
impl ConnectorDocumentStorageObservation for AsyncObservation {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.original.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        request().owner.incarnation
    }
    fn observe_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<KeyedDocumentView, KeyedDocumentReadError> {
        panic!("async lease must not call the sync observation")
    }
    fn observe_full_keyed_documents(
        &self,
        _: KeyedDocumentObservationRequest,
    ) -> Result<FullKeyedDocumentView, KeyedDocumentReadError> {
        panic!("async lease must not call the sync full observation")
    }
    fn decode_keyed_slot_change_evidence(
        &self,
        _: KeyedDocumentObservationRequest,
        _: crate::connector::ExternalMutationEvidence,
    ) -> Result<UnknownCommitEvidence, ConnectorError> {
        panic!("no mutation evidence is decoded")
    }
    fn resolve_keyed_commit(
        &self,
        _: KeyedDocumentObservationRequest,
        _: UnknownCommitEvidence,
    ) -> Result<CommitConclusion, ConnectorError> {
        panic!("async lease must not call sync resolution")
    }
    fn discover_documents(
        &self,
        _: ConnectorDocumentDiscoveryRequest,
    ) -> Result<ConnectorDocumentDiscoveryPage, ConnectorError> {
        panic!("no discovery in this observation")
    }
    fn observe_keyed_documents_async(
        &self,
        request: KeyedDocumentObservationRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<KeyedDocumentView, KeyedDocumentReadError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.async_calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.expect("fixture releases original observation");
            }
            self.original.observe_keyed_documents(request)
        })
    }
}
#[tokio::test]
async fn async_keyed_lease_waits_then_validates_the_same_owner_object_and_generation() {
    use futures::FutureExt;
    let (release, gate) = futures::channel::oneshot::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let original_calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let observation = Arc::new(AsyncObservation {
        original: Observation {
            calls: Arc::clone(&original_calls),
            descriptor: descriptor(),
            foreign_object: true,
            foreign_token: false,
        },
        gate: Mutex::new(Some(gate)),
        async_calls: Arc::clone(&calls),
    });
    let lease = lease_with(Some(observation), None, Arc::clone(&drops));
    let mut original = Box::pin(lease.observe_keyed_documents_async(request()));
    assert!(original.as_mut().now_or_never().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(original_calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    let error = original.await.unwrap_err();
    assert!(
        matches!(error,KeyedDocumentReadError::Invalid(ref e) if e.kind()==ConnectorErrorKind::InvalidRequest)
    );
    assert_eq!(original_calls.load(Ordering::SeqCst), 1);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn async_keyed_lease_rejects_foreign_request_before_provider_poll() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observation = Arc::new(AsyncObservation {
        original: Observation {
            calls: Default::default(),
            descriptor: descriptor(),
            foreign_object: false,
            foreign_token: false,
        },
        gate: Mutex::new(None),
        async_calls: Arc::clone(&calls),
    });
    let lease = lease_with(Some(observation), None, Default::default());
    let mut foreign = request();
    foreign.owner.incarnation = ProviderBindingEpoch::from_bytes([2; 16]);
    assert!(lease.observe_keyed_documents_async(foreign).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
