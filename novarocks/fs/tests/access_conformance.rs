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

use novarocks_fs::{
    FileCancellation, FileErrorKind, FileIdentity, FileReadRange, FsAccessResolver, FsLocation,
    FsScheme, ObjectStoreAccessContext, ObjectStoreConfig, ObjectStoreProviderPool,
    ObjectStoreProviderPoolOptions, SecretValue,
};
use novarocks_spi::connector::{StaticCredentialReference, StorageAccessDomainId};

fn domain(value: u8) -> StorageAccessDomainId {
    StorageAccessDomainId::from_bytes([value; 32])
}

#[test]
fn parses_local_path() {
    let location = FsLocation::parse("/tmp/a.parquet").expect("local path");
    assert_eq!(location.scheme(), FsScheme::Local);
}

#[test]
fn resolves_and_binds_local_file() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("a.parquet");
    std::fs::write(&path, b"physical-bytes").expect("write fixture");
    let access = FsAccessResolver::new()
        .resolve_location(domain(1), path.to_string_lossy(), None)
        .expect("resolve local file");
    let file = access
        .bind(0, FileIdentity::new(path.to_string_lossy(), 14, Some(123)))
        .expect("bind file");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let bytes = runtime
        .block_on(file.read(FileReadRange::WholeFile, &FileCancellation::new()))
        .expect("read bound file");
    assert_eq!(bytes.as_ref(), b"physical-bytes");
}

#[test]
fn binds_relative_local_side_file_within_authorized_root() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = directory.path().join("anchor.parquet");
    let side_file = directory.path().join("delete.parquet");
    std::fs::write(&anchor, b"anchor").expect("write anchor");
    std::fs::write(&side_file, b"side-file").expect("write side file");
    let access = FsAccessResolver::new()
        .resolve_location(domain(1), anchor.to_string_lossy(), None)
        .expect("resolve local root");
    let side_file = access
        .bind_location(
            "delete.parquet",
            FileIdentity::new("delete.parquet", 9, None),
        )
        .expect("bind relative side file");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let bytes = runtime
        .block_on(side_file.read(FileReadRange::WholeFile, &FileCancellation::new()))
        .expect("read bound side file");
    assert_eq!(bytes.as_ref(), b"side-file");
    assert_eq!(
        access
            .bind_location("../outside.parquet", FileIdentity::new("outside", 0, None))
            .expect_err("parent traversal must be rejected")
            .kind(),
        FileErrorKind::Permission
    );
}

#[test]
fn parses_object_store_schemes() {
    for location in [
        "s3://bucket/a.parquet",
        "s3a://bucket/a.parquet",
        "oss://bucket/a.parquet",
    ] {
        assert_eq!(
            FsLocation::parse(location).expect(location).scheme(),
            FsScheme::ObjectStore
        );
    }
}

#[test]
fn parses_hdfs_uri() {
    let location = FsLocation::parse("hdfs://namenode/a.parquet").expect("hdfs URI");
    assert_eq!(location.scheme(), FsScheme::Hdfs);
    assert_eq!(location.authority(), Some("namenode"));
}

#[test]
fn rejects_unsupported_scheme_with_structured_error() {
    let error = FsLocation::parse("ftp://host/a.parquet").expect_err("unsupported");
    assert_eq!(error.kind(), FileErrorKind::Unsupported);
    let mixed = FsAccessResolver::new()
        .resolve_locations(domain(1), ["/tmp/a.parquet", "s3://bucket/a.parquet"], None)
        .expect_err("mixed schemes");
    assert_eq!(mixed.kind(), FileErrorKind::Invalid);
}

#[test]
fn bounded_range_rejects_zero_and_overflow() {
    assert_eq!(
        FileReadRange::bounded(0, 0).expect_err("zero").kind(),
        FileErrorKind::Invalid
    );
    assert_eq!(
        FileReadRange::bounded(u64::MAX, 1)
            .expect_err("overflow")
            .kind(),
        FileErrorKind::Invalid
    );
}

#[test]
fn object_store_debug_redacts_secrets() {
    let config = ObjectStoreConfig {
        endpoint: "http://localhost:9000".to_string(),
        access_key_id: SecretValue::new("nwt-1-access-canary"),
        access_key_secret: SecretValue::new("nwt-1-secret-canary"),
        session_token: Some(SecretValue::new("nwt-1-token-canary")),
        enable_path_style_access: Some(true),
        region: Some("us-east-1".to_string()),
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("nwt-1-access-canary"));
    assert!(!debug.contains("nwt-1-secret-canary"));
    assert!(!debug.contains("nwt-1-token-canary"));
    assert!(debug.contains("<redacted>"));
}

#[test]
fn object_store_configuration_error_redacts_secrets() {
    let config = ObjectStoreConfig {
        endpoint: String::new(),
        access_key_id: SecretValue::new("nwt-1-access-canary"),
        access_key_secret: SecretValue::new("nwt-1-secret-canary"),
        session_token: Some(SecretValue::new("nwt-1-token-canary")),
        enable_path_style_access: Some(true),
        region: Some("us-east-1".to_string()),
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    };

    let credential_reference =
        StaticCredentialReference::try_new("warehouse-data", "blue").unwrap();
    let provider_pool =
        ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default()).unwrap();
    let error = FsAccessResolver::new()
        .resolve_location(
            domain(1),
            "s3://bucket/data.parquet",
            Some(ObjectStoreAccessContext::for_static(
                config.endpoint_config(),
                credential_reference,
                config.secret_material(),
                &provider_pool,
            )),
        )
        .expect_err("empty endpoint must fail");
    let diagnostic = format!("{error:?}: {error}");
    assert!(!diagnostic.contains("nwt-1-access-canary"));
    assert!(!diagnostic.contains("nwt-1-secret-canary"));
    assert!(!diagnostic.contains("nwt-1-token-canary"));
}

#[test]
fn file_identity_cache_tail_is_stable() {
    let identity = FileIdentity::new("a.parquet", 42, None);
    assert_eq!(identity.starrocks_cache_tail(), 42);
    assert_eq!(
        identity
            .with_modification_time_override(Some(512 << 9))
            .starrocks_cache_tail(),
        512
    );
}
