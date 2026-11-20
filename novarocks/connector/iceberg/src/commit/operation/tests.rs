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

use super::super::model::ArtifactWriteState;
use super::*;
use bytes::Bytes;
use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner};
use novarocks_spi::connector::{
    ConnectorWriteOperationId, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

fn operation(
    location: &str,
    stop: &ConnectorStopOwner,
    limits: OperationLimits,
    deadline: Instant,
) -> IcebergCommitOperation {
    let runtime = tokio::runtime::Handle::current();
    let binding = IcebergReadBinding::new(
        None,
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime.clone())),
    );
    IcebergCommitOperation::new(
        OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16])),
        location,
        binding,
        ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap(),
        IcebergCatalogRuntime::new(runtime),
        limits,
    )
    .unwrap()
}

#[tokio::test]
async fn stage_ids_are_operation_owned_and_distinct_and_the_loop_is_single_use() {
    use crate::iceberg::spec::FormatVersion;
    let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V3);
    let operation = &fixture.operation;
    let first = operation.begin_attempt().unwrap();
    operation.enter_commit_loop().unwrap();
    let id0 = first.stage_snapshot_id(0, &metadata).unwrap();
    let id1 = first.stage_snapshot_id(1, &metadata).unwrap();
    assert_ne!(id0, id1);
    let next = operation.begin_attempt().unwrap();
    assert_eq!(next.stage_snapshot_id(0, &metadata).unwrap(), id0);
    assert_eq!(next.stage_snapshot_id(1, &metadata).unwrap(), id1);
    let error = operation.enter_commit_loop().unwrap_err();
    assert_eq!(
        super::super::attempt::before_dispatch_failure(&error).kind(),
        novarocks_spi::connector::ConnectorMutationFailureKind::Internal
    );
}

#[tokio::test]
async fn cleanup_withheld_by_a_stage_id_is_reported_by_every_cleanup_entry() {
    use crate::iceberg::TableUpdate;
    use crate::iceberg::spec::{FormatVersion, Operation, Snapshot, Summary};
    let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V2);
    let operation = &fixture.operation;
    let attempt = operation.begin_attempt().unwrap();
    let id = attempt.stage_snapshot_id(0, &metadata).unwrap();
    let object = super::super::staging::write_manifest_list(
        &attempt,
        &metadata,
        id,
        None,
        std::iter::empty(),
    )
    .await
    .unwrap()
    .object;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(id)
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(metadata.last_updated_ms())
        .with_manifest_list(object.path().to_owned())
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        })
        .with_schema_id(metadata.current_schema_id())
        .build();
    let metadata = TableUpdate::AddSnapshot { snapshot }
        .apply(metadata.into_builder(None))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert!(operation.check_stage_snapshot_ids(&metadata).is_err());
    let current = operation.request_context();
    let reports = [
        operation.cleanup(CleanupScope::EntireOperation).await,
        operation
            .cleanup_in(CleanupScope::EntireOperation, current)
            .await,
        operation.cleanup_after_commit(&[]).await,
        operation.cleanup_after_commit_in(&[], current).await,
        operation
            .cleanup_after_commit(std::slice::from_ref(&object))
            .await,
    ];
    for report in reports {
        assert!(
            matches!(report, IcebergCleanupReport::Withheld { ref reason } if reason.contains(&id.to_string()))
        );
    }
    assert!(attempt.file_io().exists(object.path()).await.unwrap());
    assert_eq!(operation.artifacts().unwrap().len(), 1);
}

#[tokio::test]
async fn allocations_are_scoped_unique_and_admitted_only_once() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits::default(),
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    let second = operation.begin_attempt().unwrap();
    let a = attempt.allocate(ArtifactKind::Manifest).unwrap();
    let b = second.allocate(ArtifactKind::Manifest).unwrap();
    assert_ne!(a, b);
    assert!(a.path().contains(&attempt.attempt_token().path_component()));
    let output = attempt.file_io().new_output(a.path()).unwrap();
    output.write(Bytes::from_static(b"manifest")).await.unwrap();
    assert!(
        output
            .write(Bytes::from_static(b"replacement"))
            .await
            .is_err()
    );
    assert!(
        attempt
            .file_io()
            .new_output("not-registered")
            .unwrap()
            .write(Bytes::new())
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(a.path().trim_start_matches("file://")).unwrap(),
        b"manifest"
    );
    assert_eq!(
        operation.artifacts().unwrap()[0].write_state,
        ArtifactWriteState::Written
    );
    stop.request_stop();
    assert!(operation.begin_attempt().is_err());
    assert!(attempt.allocate(ArtifactKind::Manifest).is_err());
    assert!(output.write(Bytes::new()).await.is_err());
}

#[tokio::test]
async fn permits_bound_streams_and_cancel_waiters_without_releasing_live_writer() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits {
            io_requests: 1,
            ..OperationLimits::default()
        },
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
    let mut writer = attempt
        .file_io()
        .new_output(path.path())
        .unwrap()
        .writer()
        .await
        .unwrap();
    assert_eq!(operation.inner.permits.available_permits(), 0);
    let waiter_operation = operation.clone();
    let waiter = tokio::spawn(async move { waiter_operation.inner.io_permit().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    stop.request_stop();
    assert!(waiter.await.unwrap().is_err());
    assert_eq!(operation.inner.permits.available_permits(), 0);
    // Local FS can report Unsupported for abort without atomic_write_dir; even then
    // the actual writer call exits and the known-uncommitted file stays in the ledger.
    assert!(writer.write(Bytes::new()).await.is_err());
    assert_eq!(operation.inner.permits.available_permits(), 1);
    let IcebergCleanupReport::Partial {
        deleted: 0,
        remaining,
    } = operation.cleanup(CleanupScope::EntireOperation).await
    else {
        panic!("unconfirmed multipart abort must remain incomplete");
    };
    assert!(
        matches!(&remaining[0].reason, CleanupRemainingReason::DeleteFailed(reason) if reason.contains("abort"))
    );
}

#[tokio::test]
async fn stopped_pause_and_expired_operations_never_start_another_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits::default(),
        Instant::now() + Duration::from_secs(60),
    );
    let sleeping = operation.clone();
    let task = tokio::spawn(async move { sleeping.pause(Duration::from_secs(600)).await });
    stop.request_stop();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let expired = super::tests::operation(
        &format!("file://{}", dir.path().display()),
        &ConnectorStopOwner::new(),
        OperationLimits::default(),
        Instant::now() - Duration::from_secs(1),
    );
    assert!(expired.begin_attempt().is_err());
}

#[tokio::test]
async fn cleanup_is_independent_of_request_stop_and_retains_budget_remainder() {
    // This exact original artifact ledger remains independently eligible.
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits {
            io_requests: 1,
            cleanup: CleanupBudget {
                time: Duration::from_millis(250),
            },
        },
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    for _ in 0..3 {
        let path = attempt.allocate(ArtifactKind::Manifest).unwrap();
        attempt
            .file_io()
            .new_output(path.path())
            .unwrap()
            .write(Bytes::new())
            .await
            .unwrap();
    }
    // Hold the only I/O position. Cleanup must consume its own time budget
    // waiting for real exit, despite the original statement being stopped.
    let held = operation
        .inner
        .permits
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    stop.request_stop();
    let report = operation.cleanup(CleanupScope::EntireOperation).await;
    let IcebergCleanupReport::Partial { deleted, remaining } = report else {
        panic!("budget remainder");
    };
    assert_eq!(deleted, 0);
    assert_eq!(remaining.len(), 3);
    assert!(
        remaining
            .iter()
            .all(|r| r.reason == CleanupRemainingReason::BudgetExhausted)
    );
    assert_eq!(operation.artifacts().unwrap().len(), 3);
    drop(held);
    let final_cleanup = operation.cleanup(CleanupScope::EntireOperation).await;
    assert_eq!(final_cleanup, IcebergCleanupReport::Complete { deleted: 3 });
    assert!(operation.artifacts().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unconfirmed_drop_abort_is_reported_as_partial_even_if_object_is_absent() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits::default(),
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
    let writer = attempt
        .file_io()
        .new_output(path.path())
        .unwrap()
        .writer()
        .await
        .unwrap();
    drop(writer);
    // The local service cannot abort without an atomic staging directory. No manifest
    // absence or released permit can prove that abort succeeded.
    let IcebergCleanupReport::Partial {
        deleted: 0,
        remaining,
    } = operation.cleanup(CleanupScope::EntireOperation).await
    else {
        panic!("unconfirmed abort cannot become Complete");
    };
    assert_eq!(remaining[0].object, path);
    assert!(
        matches!(&remaining[0].reason, CleanupRemainingReason::DeleteFailed(reason)
        if reason.contains("Multipart abort failed"))
    );
    assert_eq!(operation.artifacts().unwrap().len(), 1);
}

#[tokio::test]
async fn waiting_writer_cannot_resurrect_a_path_retired_by_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits {
            io_requests: 1,
            ..OperationLimits::default()
        },
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    let path = attempt.allocate(ArtifactKind::Manifest).unwrap();
    let held = operation.inner.io_permit().await.unwrap();
    let output = attempt.file_io().new_output(path.path()).unwrap();
    let waiting = tokio::spawn(async move {
        output
            .write(Bytes::from_static(b"must-not-be-written"))
            .await
    });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    let cleaning_operation = operation.clone();
    let cleaning = tokio::spawn(async move {
        cleaning_operation
            .cleanup(CleanupScope::EntireOperation)
            .await
    });
    // Observe the explicit revocation, rather than guessing a scheduling delay.
    loop {
        if operation
            .inner
            .artifacts
            .lock()
            .unwrap()
            .ledger
            .is_retired(&path)
        {
            break;
        }
        tokio::task::yield_now().await;
    }
    drop(held);
    assert!(waiting.await.unwrap().is_err());
    assert!(matches!(
        cleaning.await.unwrap(),
        IcebergCleanupReport::Complete { deleted: 1 }
    ));
    assert!(!std::path::Path::new(path.path().trim_start_matches("file://")).exists());
    assert!(
        attempt
            .file_io()
            .new_output(path.path())
            .unwrap()
            .write(Bytes::new())
            .await
            .is_err()
    );
}

fn delete_test_binding(address: std::net::SocketAddr, timeout_ms: u64) -> IcebergReadBinding {
    let runtime = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        Some(novarocks_fs::ObjectStoreConfig {
            endpoint: format!("http://{address}"),
            access_key_id: novarocks_fs::SecretValue::new("test"),
            access_key_secret: novarocks_fs::SecretValue::new("test"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".into()),
            retry_max_times: Some(3),
            retry_min_delay_ms: Some(10),
            retry_max_delay_ms: Some(20),
            timeout_ms: Some(timeout_ms),
            io_timeout_ms: Some(timeout_ms),
        }),
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(TokioFileIoSpawner::new(runtime)),
    )
}

#[tokio::test]
async fn cleanup_removes_more_than_a_thousand_owned_objects_without_a_count_budget() {
    let directory = tempfile::tempdir().unwrap();
    let operation = operation(
        &format!("file://{}", directory.path().display()),
        &ConnectorStopOwner::new(),
        OperationLimits::default(),
        Instant::now() + Duration::from_secs(120),
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut paths = Vec::new();
    for _ in 0..1500 {
        let object = attempt.allocate(ArtifactKind::Manifest).unwrap();
        attempt
            .file_io()
            .new_output(object.path())
            .unwrap()
            .write(Bytes::new())
            .await
            .unwrap();
        paths.push(object);
    }
    let cleanup = operation.cleanup(CleanupScope::EntireOperation).await;
    assert_eq!(cleanup, IcebergCleanupReport::Complete { deleted: 1500 });
    assert!(operation.artifacts().unwrap().is_empty());
    for object in paths {
        assert!(!std::path::Path::new(object.path().strip_prefix("file://").unwrap()).exists());
    }
}

#[tokio::test]
async fn cleanup_deletes_concurrently_inside_its_original_io_window() {
    use std::io::{BufRead, Write};
    use std::sync::Condvar;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    const WINDOW: u32 = 4;
    const OBJECTS: usize = 9;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let stop_server = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let (request_seen, requests) = std::sync::mpsc::channel();
    let server_gate = gate.clone();
    let server_stop = stop_server.clone();
    let server_active = active.clone();
    let server_peak = peak.clone();
    let runtime = tokio::runtime::Handle::current();
    let stop = ConnectorStopOwner::new();
    let operation = IcebergCommitOperation::new(
        OperationToken::from_write(ConnectorWriteOperationId::new()),
        "s3://bucket/table",
        delete_test_binding(address, 15000),
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap(),
        IcebergCatalogRuntime::new(runtime),
        OperationLimits {
            io_requests: WINDOW,
            cleanup: CleanupBudget {
                time: Duration::from_secs(10),
            },
        },
    )
    .unwrap();
    for ordinal in 0..OBJECTS {
        operation
            .adopt_session_data(
                ObjectIdentity::new(format!("s3://bucket/table/data/owned-{ordinal}.parquet"))
                    .unwrap(),
            )
            .unwrap();
    }
    let server = std::thread::spawn(move || {
        let mut children = Vec::new();
        while !server_stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    let gate = server_gate.clone();
                    let active = server_active.clone();
                    let peak = server_peak.clone();
                    let request_seen = request_seen.clone();
                    children.push(std::thread::spawn(move || {
                        let _ = socket.set_read_timeout(Some(Duration::from_secs(5)));
                        let mut reader = std::io::BufReader::new(&socket);
                        let mut request = String::new();
                        if reader.read_line(&mut request).is_err() || request.is_empty() {
                            return;
                        }
                        loop {
                            let mut line = String::new();
                            if reader.read_line(&mut line).is_err() || line.is_empty() {
                                return;
                            }
                            if line == "\r\n" {
                                break;
                            }
                        }
                        drop(reader);
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        let _ = request_seen.send(request);
                        // All first-window DELETEs remain unanswered until
                        // the test observes them, so overlap is a fact.
                        let (released, changed) = &*gate;
                        let released = released.lock().unwrap();
                        let _ = changed.wait_timeout_while(
                            released, Duration::from_secs(8), |released| !*released,
                        ).unwrap();
                        // A successor cannot be received before this response.
                        // End the observed request before making its response visible.
                        active.fetch_sub(1, Ordering::SeqCst);
                        let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    // Poll only the nonblocking accept, never infer overlap from time.
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }
        drop(listener);
        for child in children {
            child.join().unwrap();
        }
    });
    let cleaning = operation.clone();
    let cleanup =
        tokio::spawn(async move { cleaning.cleanup(CleanupScope::EntireOperation).await });
    // Receiving facts runs off the Tokio thread that drives the request futures.
    let (first_window, requests) = tokio::task::spawn_blocking(move || {
        let mut seen = Vec::new();
        for _ in 0..WINDOW {
            match requests.recv_timeout(Duration::from_secs(5)) {
                Ok(request) => seen.push(request),
                Err(_) => break,
            }
        }
        (seen, requests)
    })
    .await
    .unwrap();
    let (released, changed) = &*gate;
    *released.lock().unwrap() = true;
    changed.notify_all();
    let cleanup = cleanup.await;
    stop_server.store(true, Ordering::SeqCst);
    // Always release and join the endpoint and its actual children before assertions.
    let server_exit = server.join();
    let first_count = first_window.len();
    let all_requests: Vec<_> = first_window
        .into_iter()
        .chain(requests.try_iter())
        .collect();
    server_exit.unwrap();
    assert_eq!(
        cleanup.unwrap(),
        IcebergCleanupReport::Complete { deleted: OBJECTS }
    );
    assert_eq!(first_count, WINDOW as usize);
    assert_eq!(all_requests.len(), OBJECTS);
    assert!(
        all_requests
            .iter()
            .all(|request| request.starts_with("DELETE "))
    );
    let peak = peak.load(Ordering::SeqCst);
    assert!(
        peak > 1 && peak <= WINDOW as usize,
        "observed in-flight DELETEs: {peak}"
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(operation.artifacts().unwrap().is_empty());
}

#[tokio::test]
async fn cleanup_delete_timeout_keeps_unknown_object_without_background_work() {
    use std::io::{BufRead, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let (request_seen, requests) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let runtime = tokio::runtime::Handle::current();
    let binding = delete_test_binding(address, 2000);
    let stop = ConnectorStopOwner::new();
    let operation = IcebergCommitOperation::new(
        OperationToken::from_write(ConnectorWriteOperationId::from_bytes([2; 16])),
        "s3://bucket/table",
        binding,
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            stop.view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap(),
        IcebergCatalogRuntime::new(runtime),
        OperationLimits {
            io_requests: 1,
            cleanup: CleanupBudget {
                time: Duration::from_millis(250),
            },
        },
    )
    .unwrap();
    let object = ObjectIdentity::new("s3://bucket/table/data/owned.parquet").unwrap();
    operation.adopt_session_data(object.clone()).unwrap();
    // This fault oracle needs an issued DELETE with a missing response,
    // so prepare the same binding's pooled client before its finite phase.
    // Resolution builds the client only; it sends no object-store request.
    let prepared_access = crate::fs_io::resolve_access_for_location(
        object.path(),
        &operation
            .inner
            .binding
            .for_request(operation.request_context().clone()),
    )
    .unwrap();
    drop(prepared_access);
    let server = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            match listener.accept() {
                Ok((mut socket, _)) => {
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    // TCP reads need not contain a complete request line.
                    let mut line = String::new();
                    let _ = std::io::BufReader::new(&socket).read_line(&mut line);
                    if line.is_empty() {
                        // An empty connection issued no HTTP request. Only
                        // an actual DELETE exercises response-loss behavior.
                        continue;
                    }
                    let _ = request_seen.send(line);
                    let _ = wait_release.recv_timeout(Duration::from_secs(3));
                    let _ = socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                    return;
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
                {
                    std::thread::sleep(Duration::from_millis(1))
                }
                _ => return,
            }
        }
    });
    stop.request_stop();
    let began = Instant::now();
    let report = operation.cleanup(CleanupScope::EntireOperation).await;
    let elapsed = began.elapsed();
    let request = requests.recv_timeout(Duration::from_secs(1));
    // Release and join even if an assertion fails below; this test owns its fake endpoint.
    let _ = release.send(());
    server.join().unwrap();
    let request = request.unwrap();
    assert!(
        request.starts_with("DELETE "),
        "Expected DELETE request line: {request:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "cleanup escaped its independent budget: {elapsed:?}"
    );
    let IcebergCleanupReport::Partial {
        deleted: 0,
        remaining,
    } = report
    else {
        panic!("missing DELETE completion cannot prove removal");
    };
    assert_eq!(remaining[0].object, object);
    assert_eq!(operation.artifacts().unwrap().len(), 1);
}

#[tokio::test]
async fn cleanup_waits_for_writer_exit_and_reports_it_when_budget_expires() {
    let dir = tempfile::tempdir().unwrap();
    let stop = ConnectorStopOwner::new();
    let operation = operation(
        &format!("file://{}", dir.path().display()),
        &stop,
        OperationLimits {
            io_requests: 1,
            cleanup: CleanupBudget {
                time: Duration::from_millis(250),
            },
        },
        Instant::now() + Duration::from_secs(60),
    );
    let attempt = operation.begin_attempt().unwrap();
    let path = attempt.allocate(ArtifactKind::Statistics).unwrap();
    let mut writer = attempt
        .file_io()
        .new_output(path.path())
        .unwrap()
        .writer()
        .await
        .unwrap();
    let report = tokio::time::timeout(
        Duration::from_secs(1),
        operation.cleanup(CleanupScope::EntireOperation),
    )
    .await
    .unwrap();
    let IcebergCleanupReport::Partial {
        deleted: 0,
        remaining,
    } = report
    else {
        panic!("writer remains live");
    };
    assert_eq!(remaining[0].object, path);
    assert_eq!(remaining[0].reason, CleanupRemainingReason::BudgetExhausted);
    assert_eq!(
        operation.artifacts().unwrap()[0].write_state,
        ArtifactWriteState::Writing
    );
    assert_eq!(operation.inner.permits.available_permits(), 0);
    writer.close().await.unwrap();
    assert!(operation.inner.exit_failures.lock().unwrap().is_empty());
    assert_eq!(operation.inner.permits.available_permits(), 1);
    assert_eq!(
        operation.artifacts().unwrap()[0].write_state,
        ArtifactWriteState::Written
    );
    let report = operation.cleanup(CleanupScope::EntireOperation).await;
    assert!(
        matches!(report, IcebergCleanupReport::Complete { deleted: 1 }),
        "closed writer cleanup: {report:?}; ledger: {:?}",
        operation.artifacts().unwrap()
    );
    assert!(operation.artifacts().unwrap().is_empty());
    assert!(
        !attempt
            .file_io()
            .new_input(path.path())
            .unwrap()
            .exists()
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn standard_preparation_rewrites_a_retired_manifest_without_dropping_its_record() {
    use crate::commit::model::{AddedContent, OperationIntent};
    use crate::commit::overwrite::preparer_tests::{Fixture, data};
    use crate::commit::staging::{StagingBase, StagingEngine};
    use crate::iceberg::spec::{FormatVersion, Struct};
    async fn stage(
        fixture: &Fixture,
        base: TableMetadata,
        intent: &OperationIntent,
    ) -> TableMetadata {
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "file:///test-base.metadata.json".into(),
            },
            intent,
            &attempt,
        )
        .unwrap();
        engine
            .stage(&crate::commit::FastAppendPreparer)
            .await
            .unwrap();
        let result = engine.metadata().clone();
        engine.freeze().unwrap();
        result
    }
    fn manifest_path(metadata: &TableMetadata) -> String {
        let bytes = std::fs::read(
            metadata
                .current_snapshot()
                .unwrap()
                .manifest_list()
                .trim_start_matches("file://"),
        )
        .unwrap();
        let list = crate::iceberg::spec::ManifestList::parse_with_version(
            &bytes,
            metadata.format_version(),
        )
        .unwrap();
        assert_eq!(list.entries().len(), 1);
        list.entries()[0].manifest_path.clone()
    }
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V2);
    let intent = fixture.intent(
        &base,
        "main",
        vec![
            AddedContent::new_logical_data(
                data(
                    &format!("file://{}/data.parquet", fixture.directory.path().display()),
                    3,
                    Struct::empty(),
                ),
                0,
            )
            .unwrap(),
        ],
    );
    let first = stage(&fixture, base.clone(), &intent).await;
    let first_path = manifest_path(&first);
    // Revoke eligibility at its actual owner, without a deadline race or a
    // second cleanup path. This test lives beside that owner's private state.
    fixture
        .operation
        .inner
        .artifacts
        .lock()
        .unwrap()
        .ledger
        .retire(&ObjectIdentity::new(&first_path).unwrap())
        .unwrap();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let second = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), stage(&fixture, base, &intent))
        .await;
    let second_path = manifest_path(&second);
    assert_ne!(first_path, second_path);
    let events = trace.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.operation == "write" && event.path == second_path)
    );
    assert!(
        !events
            .iter()
            .any(|event| event.operation == "write" && event.path == first_path)
    );
    assert!(!events.iter().any(|event| event.operation == "delete"));
    let records = fixture.operation.artifacts().unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.object.path() == first_path && record.retired)
    );
    assert!(
        records
            .iter()
            .any(|record| record.object.path() == second_path && !record.retired)
    );
    for path in [first_path, second_path] {
        assert!(std::path::Path::new(path.trim_start_matches("file://")).exists());
    }
}

mod written_once;
