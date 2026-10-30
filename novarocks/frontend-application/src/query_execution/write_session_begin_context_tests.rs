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

use super::*;
use novarocks_spi::connector as spi;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

struct BeginSink;
impl spi::ConnectorVendedCredentialLeaseSink for BeginSink {
    fn offer_vended_s3_credential_lease(
        &self,
        _: &spi::CatalogProperties,
        _: spi::VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        panic!("the scripted begin provider contributes no credential response")
    }
}

fn nonempty_properties() -> spi::CatalogProperties {
    spi::CatalogProperties::new(
        catalog_handle(),
        spi::ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![spi::CatalogProperty::new("uri", "http://127.0.0.1:12345").unwrap()],
        vec![
            spi::CatalogCredentialBinding::try_new(
                spi::CatalogCredentialPurpose::ObjectStoreData,
                spi::CredentialConsumerRole::Backend,
                spi::CatalogCredentialMode::Vended,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn attempt_context(
    stop: &spi::ConnectorStopOwner,
) -> (
    ConnectorRequestContext,
    Arc<dyn spi::ConnectorVendedCredentialLeaseSink>,
) {
    let sink: Arc<dyn spi::ConnectorVendedCredentialLeaseSink> = Arc::new(BeginSink);
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(60),
        stop.view(),
        4096,
        65536,
    )
    .unwrap()
    .with_vended_credential_lease_sink(Arc::clone(&sink));
    (context, sink)
}

fn unopened_begin() -> (ConnectorWriteStackLease, Arc<Mutex<Recorded>>) {
    let (lease, recorded) = unopened_fixture(1, 16, commit_unknown());
    recorded.lock().unwrap().capture_begin_context = true;
    (lease, recorded)
}

struct BoundLegacyControl(spi::ConnectorProviderBindingKey);
impl LegacyWriteControl for BoundLegacyControl {
    fn binding_key(&self) -> &spi::ConnectorProviderBindingKey {
        &self.0
    }
}

fn exact_write_lease(
    stack: &ConnectorWriteStackLease,
    properties: spi::CatalogProperties,
) -> spi::ConnectorWriteLease {
    let key = stack.session().binding_key().clone();
    spi::ConnectorWriteLease::new_with_control_runtime_id(
        stack.control_runtime_id(),
        key.clone(),
        Arc::new(BoundLegacyControl(key)),
        || {},
    )
    .unwrap()
    .with_catalog_properties(properties)
    .unwrap()
}

struct OriginalChecks {
    control: spi::ConnectorOperationControlView,
    exited: Arc<AtomicBool>,
}
impl spi::ConnectorOriginalResultScopeCheck for OriginalChecks {
    fn original_deadline(&self) -> Instant {
        self.control.deadline()
    }
    fn check_active(&self) -> Result<(), spi::OriginalResultCheckError> {
        use spi::ConnectorOperationControl;
        self.control.check_active().map_err(|error| {
            spi::OriginalResultCheckError::new(spi::OriginalResultCheckClass::Control, error)
        })
    }
}
impl Drop for OriginalChecks {
    fn drop(&mut self) {
        self.exited.store(true, Ordering::SeqCst);
    }
}

fn original_scope(
    context: &ConnectorRequestContext,
) -> (spi::ConnectorOriginalResultScope, Arc<AtomicBool>) {
    let exited = Arc::new(AtomicBool::new(false));
    (
        spi::ConnectorOriginalResultScope::from_original(Arc::new(OriginalChecks {
            control: context.original_operation_control(),
            exited: Arc::clone(&exited),
        })),
        exited,
    )
}

fn assert_attempt_at_provider(
    captured: &ConnectorRequestContext,
    original: &ConnectorRequestContext,
    sink: &Arc<dyn spi::ConnectorVendedCredentialLeaseSink>,
    properties: &spi::CatalogProperties,
) {
    assert_eq!(captured.deadline(), original.deadline());
    assert!(!captured.is_cancelled());
    assert!(Arc::ptr_eq(
        captured.vended_credential_lease_sink().unwrap(),
        sink
    ));
    assert_eq!(
        captured
            .vended_credential_lease_collection()
            .expect("provider begin needs the exact attempt collection")
            .catalog_properties(),
        properties,
    );
}

#[test]
fn ordinary_and_pending_begin_bind_exact_catalog_to_original_attempt() {
    for pending in [false, true] {
        let (lease, recorded) = unopened_begin();
        let original_session = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
        let stop = spi::ConnectorStopOwner::new();
        let (context, sink) = attempt_context(&stop);
        let original = context.clone();
        let properties = nonempty_properties();
        let write_lease = exact_write_lease(&lease, properties.clone());
        let session = if pending {
            let (declaration, _) = application_document_publication();
            let mut request = application_document_begin_request(declaration);
            request.context = context;
            futures::executor::block_on(
                begin_connector_application_document_write_session_pending_async_in(
                    lease,
                    &write_lease,
                    request,
                    &receiving,
                ),
            )
        } else {
            let mut request = begin_request();
            request.context = context;
            futures::executor::block_on(ConnectorWriteSession::begin_async_in(
                lease,
                write_lease.catalog_properties().unwrap().clone(),
                request,
                &receiving,
            ))
        }
        .unwrap();
        drop(receiving);
        drop(original_session);
        let mut recorded = recorded.lock().unwrap();
        assert_eq!(recorded.begin, 1);
        let captured = recorded.begin_context.take().unwrap();
        assert_attempt_at_provider(&captured, &original, &sink, &properties);
        assert!(original.vended_credential_lease_collection().is_none());
        stop.request_stop();
        assert!(captured.is_cancelled());
        // A terminal request consumes retained storage only. Begin decoration
        // must not make a new response collector survive this boundary.
        let terminal = session.terminal_context(captured).unwrap();
        assert!(terminal.vended_credential_lease_sink().is_none());
        assert!(terminal.vended_credential_lease_collection().is_none());
        assert_eq!(terminal.deadline(), original.deadline());
        assert!(terminal.is_cancelled());
    }
}

#[test]
fn control_only_ordinary_pending_and_cow_begin_do_not_mint_a_collection() {
    for kind in ["ordinary", "pending", "cow"] {
        let (lease, recorded) = unopened_begin();
        let original_session = Arc::new(Mutex::new(None));
        let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
        let properties = nonempty_properties();
        let context = request_context();
        let mut request = begin_request();
        request.context = context.clone();
        let write_lease = exact_write_lease(&lease, properties);
        let session = match kind {
            "ordinary" => futures::executor::block_on(ConnectorWriteSession::begin_async_in(
                lease,
                write_lease.catalog_properties().unwrap().clone(),
                request,
                &receiving,
            ))
            .unwrap(),
            "pending" => {
                let (declaration, _) = application_document_publication();
                let mut request = application_document_begin_request(declaration);
                request.context = context.clone();
                futures::executor::block_on(
                    begin_connector_application_document_write_session_pending_async_in(
                        lease,
                        &write_lease,
                        request,
                        &receiving,
                    ),
                )
                .unwrap()
            }
            _ => {
                let (original, _) = original_scope(&context);
                // The existing scripted provider owns this fixed request; this
                // probes the real checked FE entry, not COW recipe semantics.
                let session =
                    futures::executor::block_on(begin_cow_connector_write_session_async_in(
                        lease,
                        &write_lease,
                        request,
                        original,
                        &receiving,
                    ))
                    .unwrap_or_else(|error| panic!("{}", error.retire_on_original_worker(None)));
                let recorded = recorded.lock().unwrap();
                drop(recorded);
                session
            }
        };
        drop(receiving);
        drop(original_session);
        {
            let recorded = recorded.lock().unwrap();
            let captured = recorded.begin_context.as_ref().unwrap();
            assert_eq!(captured.deadline(), context.deadline());
            assert!(captured.vended_credential_lease_sink().is_none());
            assert!(captured.vended_credential_lease_collection().is_none());
        }
        // The original lease's release callback records its actual exit under
        // this same mutex, so retire the session outside the observation lock.
        drop(session);
        assert_eq!(recorded.lock().unwrap().source_drops, 1);
    }
}

fn cow_observed_context(
    context: ConnectorRequestContext,
    properties: &spi::CatalogProperties,
) -> ConnectorRequestContext {
    let (lease, recorded) = unopened_begin();
    let original_session = Arc::new(Mutex::new(None));
    let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
    let write_lease = exact_write_lease(&lease, properties.clone());
    let (original, _) = original_scope(&context);
    let mut request = begin_request();
    request.context = context;
    let _session = futures::executor::block_on(begin_cow_connector_write_session_async_in(
        lease,
        &write_lease,
        request,
        original,
        &receiving,
    ))
    .unwrap_or_else(|error| panic!("{}", error.retire_on_original_worker(None)));
    drop(receiving);
    drop(original_session);

    recorded.lock().unwrap().begin_context.take().unwrap()
}

#[test]
fn cow_begin_preserves_original_collection_and_attempt_control_without_layout_invoice() {
    let properties = nonempty_properties();
    let stop = spi::ConnectorStopOwner::new();
    let (context, sink) = attempt_context(&stop);
    let original = context.clone();
    let captured = cow_observed_context(context, &properties);
    assert_attempt_at_provider(&captured, &original, &sink, &properties);
    assert!(original.vended_credential_lease_collection().is_none());
    stop.request_stop();
    assert!(captured.is_cancelled());
}

#[test]
fn cow_begin_collection_retains_original_owner_until_the_actual_session_exit() {
    let properties = nonempty_properties();
    let stop = spi::ConnectorStopOwner::new();
    let (context, _) = attempt_context(&stop);
    let (original, exited) = original_scope(&context);
    let (lease, recorded) = unopened_begin();
    let original_session = Arc::new(Mutex::new(None));
    let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
    let write_lease = exact_write_lease(&lease, properties);
    let mut request = begin_request();
    request.context = context;
    let session = futures::executor::block_on(begin_cow_connector_write_session_async_in(
        lease,
        &write_lease,
        request,
        original,
        &receiving,
    ))
    .unwrap_or_else(|error| panic!("{}", error.retire_on_original_worker(None)));
    drop(receiving);
    drop(original_session);
    assert_eq!(recorded.lock().unwrap().begin, 1);
    assert!(!exited.load(Ordering::SeqCst));
    drop(session);
    assert!(exited.load(Ordering::SeqCst));
}

#[test]
fn cow_begin_keeps_original_stop_failure_before_any_provider_call() {
    let properties = nonempty_properties();
    let stop = spi::ConnectorStopOwner::new();
    let (context, _) = attempt_context(&stop);
    let (original, exited) = original_scope(&context);
    let (lease, recorded) = unopened_begin();
    let original_session = Arc::new(Mutex::new(None));
    let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
    let write_lease = exact_write_lease(&lease, properties);
    let mut request = begin_request();
    request.context = context;
    stop.request_stop();
    let failure = match futures::executor::block_on(begin_cow_connector_write_session_async_in(
        lease,
        &write_lease,
        request,
        original,
        &receiving,
    )) {
        Ok(_) => panic!("a stopped original scope cannot begin a provider session"),
        Err(error) => error,
    };
    drop(receiving);
    drop(original_session);
    assert_eq!(recorded.lock().unwrap().begin, 0);
    assert!(!exited.load(Ordering::SeqCst));
    assert!(
        failure
            .retire_on_original_worker(None)
            .contains("Cancelled")
    );
    assert!(exited.load(Ordering::SeqCst));
}

#[test]
fn cow_begin_keeps_expired_original_deadline_before_any_provider_call() {
    let stop = spi::ConnectorStopOwner::new();
    let sink: Arc<dyn spi::ConnectorVendedCredentialLeaseSink> = Arc::new(BeginSink);
    let context = ConnectorRequestContext::try_new(Instant::now(), stop.view(), 4096, 65536)
        .unwrap()
        .with_vended_credential_lease_sink(sink);
    let (original, exited) = original_scope(&context);
    let (lease, recorded) = unopened_begin();
    let original_session = Arc::new(Mutex::new(None));
    let receiving = WriteBeginReceiving::new(Arc::clone(&original_session));
    let write_lease = exact_write_lease(&lease, nonempty_properties());
    let mut request = begin_request();
    request.context = context;
    let failure = match futures::executor::block_on(begin_cow_connector_write_session_async_in(
        lease,
        &write_lease,
        request,
        original,
        &receiving,
    )) {
        Ok(_) => panic!("an expired original scope cannot begin a provider session"),
        Err(error) => error,
    };
    drop(receiving);
    drop(original_session);
    assert_eq!(recorded.lock().unwrap().begin, 0);
    assert!(!exited.load(Ordering::SeqCst));
    assert!(
        failure
            .retire_on_original_worker(None)
            .contains("DeadlineExceeded")
    );
    assert!(exited.load(Ordering::SeqCst));
}
