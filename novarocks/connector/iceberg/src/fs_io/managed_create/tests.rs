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

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_fs::{
    ConditionalCreateOutcome, FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
};
use novarocks_spi::connector::ConnectorStopOwner;

use super::*;
use crate::commit::write_stack::domain::IcebergWriteSessionId;

fn binding(request: &ConnectorRequestContext) -> IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    )
    .for_request(request.clone())
}

fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(60),
        stop.view(),
        1024,
        2048,
    )
    .unwrap()
}

fn physical() -> ConnectorWriterPhysicalContext {
    ConnectorWriterPhysicalContext::new([1; 16], 2, [3; 16], 4, 5)
}

fn scope() -> IcebergCreationScope {
    IcebergCreationScope::try_new(IcebergWriteSessionId::new(), [4; 16], [8; 32]).unwrap()
}

fn location(path: &Path) -> String {
    url::Url::from_file_path(path).unwrap().to_string()
}

fn sha256_abc() -> [u8; 32] {
    [
        0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22,
        0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00,
        0x15, 0xad,
    ]
}

#[tokio::test]
async fn managed_local_creation_records_actual_bytes_and_refuses_identical_foreign_target() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("staged.parquet");
    let path = location(&target);
    let stop = ConnectorStopOwner::new();
    let original = context(&stop);
    let binding = binding(&original);
    let scope = scope();

    let journal = StagedPathJournal::managed();
    let io = super::super::build_managed_file_io(
        &path,
        binding.clone(),
        scope,
        physical(),
        IcebergFileFormat::Parquet,
        journal.clone(),
    )
    .unwrap();
    journal.record(&path);
    let mut writer = io.new_output(&path).unwrap().writer().await.unwrap();
    writer.write(Bytes::from_static(b"a")).await.unwrap();
    assert!(!target.exists());
    writer.write(Bytes::from_static(b"bc")).await.unwrap();
    assert_eq!(writer.close().await.unwrap().size, 3);
    let receipt = journal.receipt(&path).unwrap().unwrap();
    assert_eq!(receipt.physical(), &physical());
    assert_eq!(
        receipt.identity(),
        scope
            .identity(physical(), &path, IcebergFileFormat::Parquet)
            .unwrap()
    );
    assert_eq!(receipt.content_sha256(), sha256_abc());
    assert_eq!(receipt.size(), 3);
    let first = std::fs::metadata(&target).unwrap();

    let foreign = StagedPathJournal::managed();
    let io = super::super::build_managed_file_io(
        &path,
        binding,
        scope,
        physical(),
        IcebergFileFormat::Parquet,
        foreign.clone(),
    )
    .unwrap();
    foreign.record(&path);
    assert!(
        io.new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"abc"))
            .await
            .is_err()
    );
    assert!(foreign.receipt(&path).is_err());
    assert_eq!(foreign.last_owned(), None);
    assert!(
        !foreign.has_unproven(),
        "the different inode establishes foreign ownership"
    );
    assert!(io.delete(&path).await.is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"abc");
    let actual = std::fs::metadata(&target).unwrap();
    assert_eq!((actual.dev(), actual.ino()), (first.dev(), first.ino()));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

async fn installed_local_writer(
    binding: IcebergReadBinding,
    original: ConnectorRequestContext,
    path: &str,
) -> (
    ManagedFileWrite,
    novarocks_fs::ConditionalLocalFile,
    IcebergCreationReceipt,
) {
    let access = binding
        .resolve_access_for_request_async(path)
        .await
        .unwrap();
    let cancellation = FileCancellation::from_connector_request(&original);
    let journal = StagedPathJournal::managed();
    journal.record(path);
    let claim = journal.begin(path, [8; 32]).unwrap();
    let mut writer = access
        .open_conditional_local_file(0, &cancellation)
        .await
        .unwrap();
    writer.write(b"abc", &cancellation).await.unwrap();
    assert_eq!(
        writer.close_and_install(&cancellation).await.unwrap(),
        ConditionalCreateOutcome::Created
    );
    let receipt =
        IcebergCreationReceipt::try_new(physical(), claim.id, claim.identity, 3, sha256_abc())
            .unwrap();
    (
        ManagedFileWrite {
            writer: None,
            binding,
            original,
            cancellation,
            journal,
            path: path.to_owned(),
            physical: physical(),
            claim,
            phase: None,
            stream: None,
            size: 3,
            digest: Sha256::new(),
        },
        writer,
        receipt,
    )
}

#[tokio::test]
async fn ambiguous_local_result_attributes_before_checking_exact_content() {
    let directory = tempfile::tempdir().unwrap();
    for (case, stored) in [
        ("matching", Some(b"abc")),
        ("different", Some(b"xyz")),
        ("absent", None),
    ] {
        let target = directory.path().join(case);
        let path = location(&target);
        let stop = ConnectorStopOwner::new();
        let original = context(&stop);
        let (managed, writer, receipt) =
            installed_local_writer(binding(&original), original, &path).await;
        match stored {
            Some(value) => std::fs::write(&target, value).unwrap(),
            None => std::fs::remove_file(&target).unwrap(),
        }
        stop.request_stop();
        let result = managed
            .recover_local(
                &writer,
                receipt,
                Ok(ConditionalCreateOutcome::AlreadyExists),
            )
            .await;
        assert!(managed.original.check_active().is_err());
        match case {
            "matching" => {
                result.unwrap();
                assert_eq!(managed.journal.receipt(&path).unwrap(), Some(receipt));
            }
            "different" => {
                assert!(result.is_err());
                assert!(managed.journal.receipt(&path).is_err());
                assert_eq!(managed.journal.last_owned().as_deref(), Some(path.as_str()));
                assert!(!managed.journal.has_unproven());
            }
            "absent" => {
                assert!(result.is_err());
                assert!(managed.journal.receipt(&path).is_err());
                assert!(managed.journal.last_owned().is_none());
                assert!(managed.journal.has_unproven());
            }
            _ => unreachable!(),
        }
        writer.abort().await.unwrap();
    }
}

#[tokio::test]
async fn abandoned_local_abort_is_awaited_and_its_original_failure_remains_sticky() {
    let directory = tempfile::tempdir().unwrap();
    let path = location(&directory.path().join("staged.parquet"));
    let stop = ConnectorStopOwner::new();
    let original = context(&stop);
    let journal = StagedPathJournal::managed();
    let objects = ManagedWriteObjects {
        scope: scope(),
        physical: physical(),
        format: IcebergFileFormat::Parquet,
        journal: journal.clone(),
    };
    journal.record(&path);
    let mut writer = objects.open(&binding(&original), &path).await.unwrap();
    writer.write(Bytes::from_static(b"abc")).await.unwrap();

    // The private name is exact and isolated. Replacing it with a directory
    // forces the original TempPath unlink to fail without a storage proxy.
    let [private]: [std::path::PathBuf; 1] = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    std::fs::remove_file(&private).unwrap();
    std::fs::create_dir(&private).unwrap();
    drop(writer);
    assert!(journal.first_stream_exit_failure().is_none());
    let pending = journal
        .take_stream_cleanup()
        .expect("writer retains original abort");
    assert_eq!(
        pending.await.unwrap_err().kind(),
        ConnectorErrorKind::Unavailable
    );
    let (observed, cause) = journal.first_stream_exit_failure().unwrap();
    assert_eq!(observed, path);
    let source = std::error::Error::source(cause.as_ref()).unwrap();
    assert!(source.downcast_ref::<novarocks_fs::FileError>().is_some());
    assert!(journal.last_owned().is_none());
    assert!(journal.receipt(&path).is_err());
    assert!(journal.take_stream_cleanup().is_none());
    assert!(journal.first_stream_exit_failure().is_some());
}
