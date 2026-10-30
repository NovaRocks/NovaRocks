// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Original journal/files and control projection; no external service required.
use super::*;
use novarocks_fs::{
    FsAccessResolver, ObjectStoreConfig, SecretValue, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogVersion, ConnectorInstanceId,
    ConnectorIoAdmissionPort, ConnectorIoRequest, ConnectorPayloadRetentionGuard,
    ConnectorProviderId, ConnectorRangeScope, ConnectorRequestInitiation, ConnectorStorageResolver,
    ConnectorVendedCredentialLeaseSink, ResolvedVendedS3Access, SharedDependency,
    StorageAccessRequest, VendedS3CredentialLeaseContribution,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

fn binding(config: Option<ObjectStoreConfig>) -> IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        config,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime)),
    )
}
fn request(deadline: Instant, stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 2048).unwrap()
}
fn object_store() -> ObjectStoreConfig {
    ObjectStoreConfig {
        endpoint: "http://127.0.0.1:9".into(),
        access_key_id: SecretValue::new("test"),
        access_key_secret: SecretValue::new("test"),
        session_token: None,
        enable_path_style_access: Some(true),
        region: None,
        retry_max_times: Some(0),
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    }
}
fn staged_file() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    String,
    StagedPathJournal,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("staged.parquet");
    let location = url::Url::from_file_path(&path).unwrap().to_string();
    let journal = StagedPathJournal::default();
    journal.record(&location);
    std::fs::write(&path, b"original staged bytes").unwrap();
    (directory, path, location, journal)
}

#[tokio::test]
async fn original_cleanup_retains_its_phase_after_work_cancel_or_expiry() {
    for expired in [false, true] {
        let stop = ConnectorStopOwner::new();
        let work_deadline = if expired {
            Instant::now() - Duration::from_secs(1)
        } else {
            Instant::now() + Duration::from_secs(60)
        };
        let work = request(work_deadline, &stop);
        if !expired {
            stop.request_stop();
        }
        assert!(work.check_active().is_err());
        let (_directory, path, location, journal) = staged_file();
        let mut cleanup = UnsealedWriterCleanup::new(binding(None), work, &location);
        cleanup.run(&journal).await.unwrap();
        let phase = cleanup.phase.as_ref().unwrap();
        let deadline = phase.request.deadline();
        assert!(phase.request.check_active().is_ok());
        assert!(!path.exists());
        assert!(journal.last().is_none());
        cleanup.run(&journal).await.unwrap();
        assert_eq!(cleanup.phase.as_ref().unwrap().request.deadline(), deadline);
    }
}

#[tokio::test]
async fn cleanup_uses_the_original_s3_pool_after_work_stops_without_object_io() {
    let stop = ConnectorStopOwner::new();
    let work = request(Instant::now() + Duration::from_secs(60), &stop);
    let bound = binding(Some(object_store())).for_request(work.clone());
    let path = "s3://warehouse/owned/staged.parquet";
    bound.resolve_access_for_request_async(path).await.unwrap();
    stop.request_stop();
    assert_eq!(
        work.check_active().unwrap_err().kind(),
        ConnectorErrorKind::Cancelled
    );
    // The existing access-binding boundary projects FS errors to InvalidRequest.
    // This test keeps that boundary and proves stopped Work cannot claim even
    // the warm pool entry, while its original cleanup obligation still can.
    assert_eq!(
        bound
            .resolve_access_for_request_async(path)
            .await
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    let mut cleanup = UnsealedWriterCleanup::new(bound.clone(), work, path);
    let phase_context = cleanup.phase().request.clone();
    // Resolving the same pooled operator performs no object request. Work is
    // still stopped; only its already-existing cleanup obligation proceeds.
    bound
        .for_request(phase_context)
        .resolve_access_for_request_async(path)
        .await
        .unwrap();
}

struct Io(AtomicUsize);
impl ConnectorIoAdmissionPort for Io {
    fn request_owned(&self, _: std::num::NonZeroU64) -> Result<ConnectorIoRequest, ConnectorError> {
        Err(error(
            ConnectorErrorKind::PermissionDenied,
            "original IO denied",
        ))
    }
    fn request(&self, _: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(error(
            ConnectorErrorKind::PermissionDenied,
            "original IO denied",
        ))
    }
    fn publication_obligation_started(&self) {}
    fn retain_payload(&self) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        Err(error(
            ConnectorErrorKind::PermissionDenied,
            "original payload denied",
        ))
    }
}

#[tokio::test]
async fn real_cleanup_admission_denial_keeps_path_and_same_phase_on_reentry() {
    let stop = ConnectorStopOwner::new();
    let io = Arc::new(Io(AtomicUsize::new(0)));
    let work =
        request(Instant::now() + Duration::from_secs(60), &stop).with_io_admission(io.clone());
    let (_directory, path, location, journal) = staged_file();
    let mut cleanup = UnsealedWriterCleanup::new(binding(None), work, &location);
    let failure = cleanup.run(&journal).await.unwrap_err();
    assert_eq!(failure.kind(), ConnectorErrorKind::Internal);
    assert!(path.exists());
    assert_eq!(journal.last().as_deref(), Some(location.as_str()));
    assert_eq!(io.0.load(Ordering::SeqCst), 1);
    let phase = cleanup.phase.as_ref().unwrap();
    let deadline = phase.request.deadline();
    cleanup.run(&journal).await.unwrap_err();
    assert_eq!(cleanup.phase.as_ref().unwrap().request.deadline(), deadline);
    assert_eq!(io.0.load(Ordering::SeqCst), 2);
    assert!(path.exists());
    assert_eq!(journal.last().as_deref(), Some(location.as_str()));
    // Deterministic fixture exhaustion of this SAME phase; no sleep or
    // production clock override is introduced.
    let expired = cleanup
        .phase
        .as_ref()
        .unwrap()
        .request
        .clone()
        .with_earlier_deadline(Instant::now() - Duration::from_secs(1));
    cleanup.phase.as_mut().unwrap().request = expired;
    assert_eq!(
        cleanup.run(&journal).await.unwrap_err().kind(),
        ConnectorErrorKind::DeadlineExceeded
    );
    assert_eq!(io.0.load(Ordering::SeqCst), 2);
    assert!(path.exists());
    assert!(journal.last().is_some());
}

#[tokio::test]
async fn cleanup_round_yield_does_not_cap_the_original_journal() {
    let stop = ConnectorStopOwner::new();
    let directory = tempfile::tempdir().unwrap();
    let journal = StagedPathJournal::default();
    let total = OperationLimits::default().cleanup.objects + 1;
    for index in 0..total {
        let path = directory.path().join(format!("staged-{index}.parquet"));
        let location = url::Url::from_file_path(&path).unwrap().to_string();
        journal.record(&location);
        std::fs::write(path, b"original staged bytes").unwrap();
    }
    let location = url::Url::from_directory_path(directory.path())
        .unwrap()
        .to_string();
    let mut cleanup = UnsealedWriterCleanup::new(
        binding(None),
        request(Instant::now() + Duration::from_secs(60), &stop),
        location,
    );
    cleanup.run(&journal).await.unwrap();
    assert!(journal.last().is_none());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

struct DenyResolver;
impl ConnectorStorageResolver for DenyResolver {
    fn resolve_vended_s3(
        &self,
        _: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        Err(error(
            ConnectorErrorKind::PermissionDenied,
            "original storage denied",
        ))
    }
}
struct Sink;
impl ConnectorVendedCredentialLeaseSink for Sink {
    fn offer_vended_s3_credential_lease(
        &self,
        _: &CatalogProperties,
        _: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        Err(error(
            ConnectorErrorKind::PermissionDenied,
            "original lease sink denied",
        ))
    }
}

#[tokio::test]
async fn cleanup_projection_preserves_original_scope_capabilities_and_restrictions() {
    let stop = ConnectorStopOwner::new();
    let resolver: Arc<dyn ConnectorStorageResolver> = Arc::new(DenyResolver);
    let sink: Arc<dyn ConnectorVendedCredentialLeaseSink> = Arc::new(Sink);
    let io: Arc<dyn ConnectorIoAdmissionPort> = Arc::new(Io(AtomicUsize::new(0)));
    let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
    let range = ConnectorRangeScope::try_new(1, 2, 3, 4, 5, 6).unwrap();
    let properties = CatalogProperties::new(
        CatalogHandle::new(
            ConnectorInstanceId::parse("cleanup").unwrap(),
            CatalogVersion::from_bytes([7; 32]),
        ),
        ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![],
        vec![],
    )
    .unwrap();
    let cap = Instant::now() + Duration::from_secs(1);
    let original = request(Instant::now() + Duration::from_secs(60), &stop)
        .with_storage_resolver(resolver)
        .with_vended_credential_lease_sink(sink)
        .with_vended_credential_lease_collection(properties)
        .unwrap()
        .with_io_admission(io.clone())
        .with_execution_source(range, operations.clone())
        .with_necessary_finalization_deadline_cap(cap)
        .with_initiation(ConnectorRequestInitiation::JobAttempt);
    let marker = original.request_scope_extension_or_insert_with(|| String::from("same scope"));
    let ticket = operations.admit(Arc::new(|| {})).unwrap();
    let mut cleanup = UnsealedWriterCleanup::new(binding(None), original.clone(), "file:///owned");
    let projected = &cleanup.phase().request;
    assert!(Arc::ptr_eq(
        &marker,
        &projected.request_scope_extension_or_insert_with(|| String::from("wrong"))
    ));
    assert!(Arc::ptr_eq(
        original.storage_resolver().unwrap(),
        projected.storage_resolver().unwrap()
    ));
    assert!(Arc::ptr_eq(
        original.vended_credential_lease_sink().unwrap(),
        projected.vended_credential_lease_sink().unwrap()
    ));
    assert!(projected.vended_credential_lease_collection().is_some());
    assert!(Weak::ptr_eq(
        original.io_admission().unwrap(),
        projected.io_admission().unwrap()
    ));
    assert_eq!(projected.range_scope(), Some(range));
    assert_eq!(projected.source_operations().unwrap().live_operations(), 1);
    assert_eq!(projected.necessary_finalization_deadline_cap(), Some(cap));
    assert!(projected.deadline() > cap);
    assert_eq!(projected.initiation(), original.initiation());
    assert_eq!(
        projected.max_handle_payload_bytes(),
        original.max_handle_payload_bytes()
    );
    assert_eq!(
        projected.max_total_payload_bytes(),
        original.max_total_payload_bytes()
    );
    drop(ticket);
}
