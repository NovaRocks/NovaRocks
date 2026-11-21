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

//! Exercises the provider listing seam through real local and signed S3 requests.

use std::alloc::Layout;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use novarocks_failure_contract::{FailureProjection, connector_reason};
use novarocks_fs::{
    FsAccessResolver, ObjectStoreConfig, SecretValue, TokioFileIoRuntime, TokioFileIoSpawner,
    TokioRefreshExecutor,
};
use novarocks_spi::connector::{
    ConnectorAddFilesRecursion, ConnectorError, ConnectorErrorKind, ConnectorPayloadByteBudget,
    ConnectorPayloadRetentionGuard, ConnectorRequestContext, ConnectorStopOwner,
};
use novarocks_type_contract::owned_resources::layout::arc_layout;

use crate::access_binding::IcebergReadBinding;
use crate::catalog::listing_admission::ListingAdmission;
use crate::catalog_control::add_files::listing::{CandidateFiles, resolve_and_list};
use crate::rest_protocol::test_server::{Reply, Script, TestServer};

#[derive(Default)]
struct Window(Arc<AtomicUsize>);

struct Paid(Arc<AtomicUsize>, usize);

impl Drop for Paid {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::SeqCst);
    }
}

impl ConnectorPayloadByteBudget for Window {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        self.0.fetch_add(bytes, Ordering::SeqCst);
        Ok(ConnectorPayloadRetentionGuard::new(Paid(
            self.0.clone(),
            bytes,
        )))
    }
}

fn binding(config: Option<ObjectStoreConfig>, stop: &ConnectorStopOwner) -> IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        config,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    )
    .for_request(
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(15),
            stop.view(),
            1024,
            4096,
        )
        .unwrap(),
    )
}

fn store_config(server: &TestServer) -> ObjectStoreConfig {
    ObjectStoreConfig {
        endpoint: server.url(),
        access_key_id: SecretValue::new("listing-test-access"),
        access_key_secret: SecretValue::new("listing-test-secret"),
        session_token: None,
        enable_path_style_access: Some(true),
        region: Some("us-east-1".into()),
        retry_max_times: Some(0),
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: Some(5000),
        io_timeout_ms: Some(5000),
    }
}

fn author() -> (Arc<AtomicUsize>, ConnectorPayloadRetentionGuard) {
    let window = Arc::new(Window::default());
    let paid = window.0.clone();
    (
        paid,
        ConnectorPayloadRetentionGuard::from_byte_budget(window),
    )
}

fn names(files: &CandidateFiles) -> Vec<&str> {
    files
        .iter()
        .map(|file| file.path().rsplit('/').next().unwrap())
        .collect()
}

#[tokio::test]
async fn local_listing_sorts_and_applies_hidden_and_recursion_policy() {
    let dir = tempfile::tempdir().unwrap();
    for folder in ["nested", "nested/.hidden", ".temporary"] {
        std::fs::create_dir_all(dir.path().join(folder)).unwrap();
    }
    for name in [
        "z.parquet",
        "a@ok.thirteen",
        "_SUCCESS",
        ".temporary/f.parquet",
        "nested/.hidden/z.parquet",
        "nested/_hidden",
        "nested/n.parquet",
    ] {
        std::fs::write(dir.path().join(name), b"ordinary file").unwrap();
    }
    let location = format!("file://{}/", dir.path().display());
    let stop = ConnectorStopOwner::new();
    let binding = binding(None, &stop);
    let admission = ListingAdmission::default();
    let (paid, author) = author();

    for (mode, expected) in [
        (
            ConnectorAddFilesRecursion::Include,
            &["a@ok.thirteen", "n.parquet", "z.parquet"][..],
        ),
        (
            ConnectorAddFilesRecursion::Skip,
            &["a@ok.thirteen", "z.parquet"][..],
        ),
        (
            ConnectorAddFilesRecursion::Fail,
            &["a@ok.thirteen", "n.parquet", "z.parquet"][..],
        ),
    ] {
        let (_, files) = resolve_and_list(&location, mode, &binding, &admission, &author)
            .await
            .unwrap()
            .into_parts();
        assert_eq!(names(&files), expected);
        assert_eq!(files.len(), expected.len());
        assert_eq!(files.skipped_hidden_count(), 4);
        for (index, file) in files.iter().enumerate() {
            assert_eq!(file.size(), 13);
            if mode == ConnectorAddFilesRecursion::Fail && index == 1 {
                let error = file.validate().unwrap_err();
                assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
                assert_eq!(
                    error.message(),
                    format!(
                        "Recursive directory must not exist when recursive_directory argument is 'fail': {location}nested/n.parquet"
                    )
                );
            } else {
                file.validate().unwrap();
            }
        }
        drop(files);
        assert_eq!(paid.load(Ordering::SeqCst), 0);
    }

    // A direct file may be hidden and need not use the directory's URI spelling.
    let path = dir.path().join("_SUCCESS");
    let (_, files) = resolve_and_list(
        path.to_str().unwrap(),
        ConnectorAddFilesRecursion::Fail,
        &binding,
        &admission,
        &author,
    )
    .await
    .unwrap()
    .into_parts();
    let only = files.iter().next().unwrap();
    only.validate().unwrap();
    assert_eq!(only.path(), format!("file://{}", path.display()));
    assert_eq!(only.size(), 13);

    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let (_, empty) = resolve_and_list(
        empty.to_str().unwrap(),
        ConnectorAddFilesRecursion::Fail,
        &binding,
        &admission,
        &author,
    )
    .await
    .unwrap()
    .into_parts();
    assert_eq!(empty.len(), 0);
}

fn page(objects: &[(&str, u64)], continuation: Option<&str>) -> Reply {
    let mut xml = format!(
        "<ListBucketResult><Name>source-bucket</Name><IsTruncated>{}</IsTruncated>",
        continuation.is_some()
    );
    if let Some(token) = continuation {
        xml.push_str(&format!(
            "<NextContinuationToken>{token}</NextContinuationToken>"
        ));
    }
    for (path, size) in objects {
        xml.push_str(&format!(
            "<Contents><Key>{path}</Key><LastModified>2026-10-10T00:00:00Z</LastModified><Size>{size}</Size></Contents>"
        ));
    }
    xml.push_str("</ListBucketResult>");
    Reply::bytes(200, xml.into_bytes()).with_header("content-type", "application/xml")
}

#[tokio::test]
async fn signed_s3_source_keeps_the_probe_page_and_prefers_a_directory_to_head() {
    let server = TestServer::start(
        Script::new()
            .on_fn("GET", "/source-bucket", |request, _| {
                let query = request.query().unwrap_or_default();
                let parameters = url::form_urlencoded::parse(query.as_bytes())
                    .collect::<std::collections::BTreeMap<_, _>>();
                match (
                    parameters.get("prefix").map(|part| part.as_ref()),
                    parameters
                        .get("continuation-token")
                        .map(|part| part.as_ref()),
                ) {
                    (Some("both/"), None) => page(
                        &[("both/_SUCCESS", 0), ("both/z.parquet", 91)],
                        Some("same-cursor"),
                    ),
                    (Some("both/"), Some("same-cursor")) => {
                        page(&[("both/nested/a.bin", 42)], None)
                    }
                    (Some("marker/"), None) => page(&[("marker/", 0)], None),
                    (Some("hidden/"), None) => page(&[("hidden/_temp/file", 7)], None),
                    (Some("denied/"), None) => Reply::status(403),
                    (Some("exact/"), None) | (Some("missing/"), None) => page(&[], None),
                    _ => Reply::status(400),
                }
            })
            .on("HEAD", "/source-bucket/both", Reply::status(200))
            .on("HEAD", "/source-bucket/exact", Reply::status(200)),
    );
    let stop = ConnectorStopOwner::new();
    let binding = binding(Some(store_config(&server)), &stop);
    let admission = ListingAdmission::default();
    let (_, author) = author();
    let resolve = |location| {
        resolve_and_list(
            location,
            ConnectorAddFilesRecursion::Include,
            &binding,
            &admission,
            &author,
        )
    };
    let (_, both) = resolve("s3://source-bucket/both/")
        .await
        .unwrap()
        .into_parts();
    assert_eq!(names(&both), ["a.bin", "z.parquet"]);
    assert_eq!(both.skipped_hidden_count(), 1);
    assert_eq!(
        both.iter().map(|file| file.size()).collect::<Vec<_>>(),
        [42, 91]
    );
    for file in both.iter() {
        file.validate().unwrap();
    }
    let pages = server.requests_to("GET", "/source-bucket");
    assert_eq!(pages.len(), 2);
    assert!(
        pages
            .iter()
            .all(|page| page.header("authorization").is_some())
    );
    assert!(
        pages
            .iter()
            .all(|page| page.query().unwrap().contains("max-keys=256"))
    );
    assert!(!pages[0].query().unwrap().contains("continuation-token"));
    assert!(
        pages[1]
            .query()
            .unwrap()
            .contains("continuation-token=same-cursor")
    );
    assert!(server.requests_to("HEAD", "/source-bucket/both").is_empty());

    for name in ["marker", "hidden"] {
        let location = format!("s3://source-bucket/{name}/");
        let (_, empty) = resolve_and_list(
            &location,
            ConnectorAddFilesRecursion::Include,
            &binding,
            &admission,
            &author,
        )
        .await
        .unwrap()
        .into_parts();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.skipped_hidden_count(), usize::from(name == "hidden"));
        assert!(
            server
                .requests_to("HEAD", &format!("/source-bucket/{name}"))
                .is_empty()
        );
    }

    let denied = match resolve("s3://source-bucket/denied").await {
        Ok(_) => panic!("denied location resolved"),
        Err(error) => error,
    };
    assert_eq!(denied.kind(), ConnectorErrorKind::PermissionDenied);
    assert!(
        server
            .requests_to("HEAD", "/source-bucket/denied")
            .is_empty()
    );

    let (_, exact) = resolve("s3://source-bucket/exact")
        .await
        .unwrap()
        .into_parts();
    assert_eq!(names(&exact), ["exact"]);
    assert_eq!(exact.iter().next().unwrap().size(), 0);
    assert_eq!(server.requests_to("HEAD", "/source-bucket/exact").len(), 1);
    let missing = match resolve("s3://source-bucket/missing").await {
        Ok(_) => panic!("missing location resolved"),
        Err(error) => error,
    };
    assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
    assert_eq!(
        missing.failure_fact().reason(),
        connector_reason::LOCATION_NOT_FOUND
    );
    assert_eq!(
        missing.message(),
        "Location not found: s3://source-bucket/missing"
    );
    assert_eq!(
        server.requests_to("HEAD", "/source-bucket/missing").len(),
        1
    );
}

#[tokio::test]
async fn consuming_a_candidate_moves_its_path_and_original_payment() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("path"), b"source contents").unwrap();
    let stop = ConnectorStopOwner::new();
    let binding = binding(None, &stop);
    let admission = ListingAdmission::default();
    let (paid, author) = author();
    let (_, files) = resolve_and_list(
        dir.path().to_str().unwrap(),
        ConnectorAddFilesRecursion::Include,
        &binding,
        &admission,
        &author,
    )
    .await
    .unwrap()
    .into_parts();
    let only = files.iter().next().unwrap();
    let pointer = only.identity().path().as_ptr();
    let path_bytes = arc_layout(Layout::array::<u8>(only.path().len()).unwrap())
        .unwrap()
        .size();
    let total = paid.load(Ordering::SeqCst);
    assert!(total > path_bytes);
    let mut files = files.into_iter();
    let (path, size, receipt) = files.next().unwrap().into_parts();
    assert_eq!(path.path().as_ptr(), pointer);
    assert_eq!(size, 15);
    assert_eq!(paid.load(Ordering::SeqCst), total);
    drop(files);
    assert_eq!(paid.load(Ordering::SeqCst), path_bytes);
    drop(path);
    drop(receipt);
    assert_eq!(paid.load(Ordering::SeqCst), 0);
}
