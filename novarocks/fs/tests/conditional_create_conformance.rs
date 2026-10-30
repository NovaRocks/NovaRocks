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

use bytes::Bytes;
use novarocks_fs::{
    ConditionalCreateOutcome, FileCancellation, FileErrorKind, FsAccessHandle, FsAccessResolver,
    FsLocation, FsScheme, ResolvedFsPath,
};
use novarocks_spi::connector::StorageAccessDomainId;
use opendal::Operator;

fn domain() -> StorageAccessDomainId {
    StorageAccessDomainId::from_bytes([1; 32])
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

#[test]
fn local_create_is_atomic_and_preserves_original_payload() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("v1.metadata.json");
    let access = FsAccessResolver::new()
        .resolve_location(domain(), path.to_string_lossy(), None)
        .expect("resolve local file");
    let cancellation = FileCancellation::new();
    let runtime = runtime();

    assert_eq!(
        runtime
            .block_on(access.create_if_absent(
                0,
                Bytes::from_static(b"first-payload"),
                &cancellation,
            ))
            .expect("first conditional create"),
        ConditionalCreateOutcome::Created
    );
    assert_eq!(
        runtime
            .block_on(access.create_if_absent(
                0,
                Bytes::from_static(b"replacement-payload"),
                &cancellation,
            ))
            .expect("second conditional create"),
        ConditionalCreateOutcome::AlreadyExists
    );
    assert_eq!(
        std::fs::read(path).expect("read created file"),
        b"first-payload"
    );
}

#[test]
fn cancelled_create_has_no_side_effect() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("cancelled.metadata.json");
    let access = FsAccessResolver::new()
        .resolve_location(domain(), path.to_string_lossy(), None)
        .expect("resolve local file");
    let cancellation = FileCancellation::new();
    cancellation.cancel();

    let error = runtime()
        .block_on(access.create_if_absent(
            0,
            Bytes::from_static(b"must-not-be-written"),
            &cancellation,
        ))
        .expect_err("cancelled create must fail");
    assert_eq!(error.kind(), FileErrorKind::Cancelled);
    assert!(!path.exists());
}

#[test]
fn unsupported_backend_fails_before_write() {
    // Memory now supports atomic create; explicitly model an endpoint that
    // does not advertise it while retaining real storage for the no-write check.
    let operator = Operator::new(opendal::services::Memory::default())
        .expect("memory operator")
        .layer(opendal::layers::CapabilityOverrideLayer::new(
            |mut capability| {
                capability.write_with_if_not_exists = false;
                capability
            },
        ));
    let location = FsLocation::parse("unsupported.metadata.json").expect("memory path");
    let path = ResolvedFsPath::new(location, "unsupported.metadata.json").expect("resolved path");
    let access = FsAccessHandle::new(
        domain(),
        FsScheme::Local,
        operator.clone(),
        None,
        Some(".".to_string()),
        vec![path],
    );

    let runtime = runtime();
    let error = runtime
        .block_on(access.create_if_absent(
            0,
            Bytes::from_static(b"must-not-be-written"),
            &FileCancellation::new(),
        ))
        .expect_err("unsupported conditional create must fail");
    assert_eq!(error.kind(), FileErrorKind::Unsupported);
    assert!(
        !runtime
            .block_on(operator.exists("unsupported.metadata.json"))
            .expect("check memory path")
    );
}
