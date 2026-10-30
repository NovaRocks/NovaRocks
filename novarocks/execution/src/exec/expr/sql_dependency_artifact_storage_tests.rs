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

//! Executes the actual FE storage leaf with real WorkloadControl accounts.
//! This proves only storage admission/rollback, not native dependency capture.
#[path = "../../../../frontend-application/src/query_execution/dependency_artifact_storage.rs"]
mod storage;
use novarocks_query_application::session_control::{SessionToken, StatementToken};
use novarocks_workload_control::{
    ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig, WorkloadControl,
};
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use storage::{
    AdmittedCaptureBytes, CaptureOutputDescriptor, CaptureRecordError, CaptureStorageError,
    SqlDependencyArtifactFactory,
};
fn authority(total: u64) -> WorkloadControl {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: total,
            control_bytes: 64,
            per_scope_bytes: total - 64,
        },
    )
    .unwrap();
    control.mark_ready().unwrap();
    control
}
struct PrivateDirectory(PathBuf);
impl PrivateDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "novarocks-capture-storage-{}",
            uuid::Uuid::new_v4()
        ));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        Self(path)
    }
    fn descriptor(&self, bytes: u64) -> CaptureOutputDescriptor {
        CaptureOutputDescriptor {
            version: 1,
            run_namespace: "real-run".into(),
            output_root: self.0.clone(),
            max_statement_bytes: bytes,
        }
    }
    fn file(&self, generation: u64) -> PathBuf {
        self.0.join(format!(
            "real-run-{}-{generation}.cap",
            token(generation).session().key().get()
        ))
    }
}
impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn token(generation: u64) -> StatementToken {
    StatementToken::new(
        SessionToken::new(
            novarocks_query_application::session_control::ClientSessionKey::new(
                (7_u64 << 32) | 11_u64,
            )
            .expect("nonzero fixture session"),
        ),
        generation,
    )
}
#[test]
fn sql_dependency_artifact_storage_actual_data_charge_and_release_match_request() {
    let owner = authority(8192);
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = work.owner.scope();
    let resources = owner.resources().expect("test workload resource authority");
    let mut buffer = AdmittedCaptureBytes::try_new(&resources, &scope, 4096).unwrap();
    let bytes = buffer.write_zeroed();
    assert_eq!(bytes.len(), 4096);
    assert_eq!(resources.snapshot().data_reserved_bytes, 0);
    assert_eq!(resources.snapshot().data_used_bytes, 4096);
    bytes[2048] = 7;
    assert_eq!(buffer.as_slice()[2048], 7);
    drop(buffer);
    assert_eq!(resources.snapshot().data_used_bytes, 0);
    assert_eq!(resources.snapshot().peak_held_bytes, 4096);
}
#[test]
fn sql_dependency_artifact_storage_refuses_real_capacity_before_allocation() {
    let owner = authority(256);
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let resources = owner.resources().expect("test workload resource authority");
    assert!(matches!(
        AdmittedCaptureBytes::try_new(&resources, &work.owner.scope(), 193),
        Err(CaptureStorageError::Work(WorkError::Capacity(_)))
    ));
    assert_eq!(resources.snapshot().held_bytes(), 0);
    assert_eq!(resources.snapshot().peak_held_bytes, 0);
}
#[test]
fn sql_dependency_artifact_storage_foreign_authority_retains_exact_nominal_error() {
    let a = authority(8192);
    let b = authority(8192);
    let work = a
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    assert!(matches!(
        AdmittedCaptureBytes::try_new(
            &b.resources().expect("test workload resource authority"),
            &work.owner.scope(),
            128
        ),
        Err(CaptureStorageError::Work(WorkError::ForeignAuthority))
    ));
    assert_eq!(
        a.resources()
            .expect("test workload resource authority")
            .snapshot()
            .held_bytes(),
        0
    );
    assert_eq!(
        b.resources()
            .expect("test workload resource authority")
            .snapshot()
            .held_bytes(),
        0
    );
}
#[test]
fn sql_dependency_artifact_storage_backing_release_is_not_terminal_owner_publication() {
    let owner = authority(8192);
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let resources = owner.resources().expect("test workload resource authority");
    let buffer = AdmittedCaptureBytes::try_new(&resources, &work.owner.scope(), 512).unwrap();
    drop(work);
    assert_eq!(resources.snapshot().data_used_bytes, 512);
    drop(buffer);
    assert_eq!(resources.snapshot().data_used_bytes, 0);
}
#[test]
fn sql_dependency_artifact_storage_distinct_tokens_and_create_new_preserve_original_file() {
    let dir = PrivateDirectory::new();
    let owner = authority(8192);
    let resources = owner.resources().expect("test workload resource authority");
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let factory =
        SqlDependencyArtifactFactory::try_new(dir.descriptor(4096), resources.clone()).unwrap();
    let mut first = factory
        .begin(token(13), &[0x71; 32], &work.owner.scope())
        .unwrap();
    assert_eq!(first.bytes_written(), 64);
    let (loan_authority, loan_scope) = first.resource_loan();
    let data_used_before_loan = loan_authority.snapshot().data_used_bytes;
    let loan_bytes = AdmittedCaptureBytes::try_new(loan_authority, loan_scope, 32).unwrap();
    assert_eq!(
        loan_authority.snapshot().data_used_bytes,
        data_used_before_loan + 32
    );
    drop(loan_bytes);
    assert_eq!(
        loan_authority.snapshot().data_used_bytes,
        data_used_before_loan
    );
    first
        .write_record(2, 9, 3, |bytes| -> Result<(), ()> {
            bytes.copy_from_slice(b"abc");
            Ok(())
        })
        .unwrap();
    assert_eq!(first.bytes_written(), 84);
    first.finish_storage().unwrap();
    let original = std::fs::read(dir.file(13)).unwrap();
    assert_eq!(&original[..8], b"NRDEPS02");
    assert_eq!(
        &original[8..16],
        &token(13).session().key().get().to_le_bytes()
    );
    assert_eq!(&original[20..28], &13u64.to_le_bytes());
    assert_eq!(&original[28..60], &[0x71; 32]);
    assert_eq!(&original[60..64], &2u32.to_le_bytes());
    assert!(
        matches!(factory.begin(token(13), &[0x72;32], &work.owner.scope()), Err(CaptureStorageError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(std::fs::read(dir.file(13)).unwrap(), original);
    let mut second = factory
        .begin(token(14), &[0x72; 32], &work.owner.scope())
        .unwrap();
    second.finish_storage().unwrap();
    assert_ne!(std::fs::read(dir.file(14)).unwrap(), original);
    assert_eq!(resources.snapshot().held_bytes(), 0);
}
#[test]
fn sql_dependency_artifact_storage_encoder_primary_is_preserved_and_prefix_cannot_replay() {
    let dir = PrivateDirectory::new();
    let owner = authority(8192);
    let resources = owner.resources().expect("test workload resource authority");
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let factory =
        SqlDependencyArtifactFactory::try_new(dir.descriptor(4096), resources.clone()).unwrap();
    let mut artifact = factory
        .begin(token(15), &[1; 32], &work.owner.scope())
        .unwrap();
    let calls = AtomicUsize::new(0);
    let full = "x".repeat(2048);
    let error = artifact.write_record(2, 0, 64, |_| -> Result<(), &str> {
        calls.fetch_add(1, Ordering::Relaxed);
        Err(&full)
    });
    assert!(matches!(error, Err(CaptureRecordError::Encoder(message)) if message == full));
    assert_eq!(resources.snapshot().held_bytes(), 0);
    assert!(matches!(
        artifact.write_record(2, 0, 64, |_| -> Result<(), ()> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }),
        Err(CaptureRecordError::Storage(CaptureStorageError::Closed))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(matches!(
        artifact.finish_storage(),
        Err(CaptureStorageError::Closed)
    ));
    drop(artifact);
    assert_eq!(std::fs::read(dir.file(15)).unwrap().len(), 64);
}
#[test]
fn sql_dependency_artifact_storage_policy_limit_is_not_a_fake_host_or_control_error() {
    let dir = PrivateDirectory::new();
    let owner = authority(8192);
    let resources = owner.resources().expect("test workload resource authority");
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let factory =
        SqlDependencyArtifactFactory::try_new(dir.descriptor(64), resources.clone()).unwrap();
    let mut artifact = factory
        .begin(token(16), &[1; 32], &work.owner.scope())
        .unwrap();
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        artifact.write_record(2, 0, 1, |_| -> Result<(), ()> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }),
        Err(CaptureRecordError::Storage(
            CaptureStorageError::StatementLimit {
                requested: 82,
                ceiling: 64
            }
        ))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(resources.snapshot().held_bytes(), 0);
    assert!(matches!(
        artifact.finish_storage(),
        Err(CaptureStorageError::Closed)
    ));
}
#[test]
fn sql_dependency_artifact_storage_zero_and_unrepresentable_requests_are_not_grants() {
    let owner = authority(8192);
    let work = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let resources = owner.resources().expect("test workload resource authority");
    for bytes in [0, usize::MAX] {
        assert!(matches!(
            AdmittedCaptureBytes::try_new(&resources, &work.owner.scope(), bytes),
            Err(CaptureStorageError::InvalidLayout)
        ));
    }
    assert_eq!(resources.snapshot().held_bytes(), 0);
}
