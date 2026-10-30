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
use novarocks_spi::connector::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

pub(in crate::query::mv_action::control) struct Fixture {
    descriptor: ConnectorInstanceDescriptor,
    epoch: ProviderBindingEpoch,
    pub(in crate::query::mv_action::control) applied: bool,
    pub(in crate::query::mv_action::control) release: AtomicBool,
    pub(in crate::query::mv_action::control) drops: AtomicUsize,
    calls: AtomicUsize,
    original: Arc<u8>,
    panic_drop: bool,
    refusal: Option<ConnectorError>,
    context: ConnectorRequestContext,
    waiter: futures::task::AtomicWaker,
}
impl Fixture {
    pub(in crate::query::mv_action::control) fn new(
        applied: bool,
        context: ConnectorRequestContext,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("c").unwrap(),
            },
            epoch: ProviderBindingEpoch::new(),
            applied,
            release: AtomicBool::new(false),
            drops: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            panic_drop: false,
            refusal: None,
            original: Arc::new(1),
            context,
            waiter: Default::default(),
        })
    }
    pub(in crate::query::mv_action::control) fn call(self: &Arc<Self>) -> CatalogCall {
        let call = CatalogCall::new();
        let lease = ConnectorCatalogMutationLease::new(
            self.descriptor.clone(),
            ConnectorControlRuntimeId::new(),
            self.epoch,
            self.clone(),
            || {},
        )
        .unwrap();
        call.start(
            lease,
            ConnectorMutationOperationId::new(),
            ConnectorCatalogMutationOperation::DropTable {
                table: ConnectorTableIdentity {
                    instance_id: self.descriptor.instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("mv"),
                },
                policy: DropPolicy::FailIfMissing,
                data_disposition: ConnectorDropTableDataDisposition::Purge,
            },
            self.context.clone(),
        );
        call
    }
    pub(in crate::query::mv_action::control) fn release(&self) {
        self.release.store(true, Ordering::Release);
        self.waiter.wake();
    }
}
impl ConnectorCatalogMutation for Fixture {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.epoch
    }
    fn execute(
        &self,
        _: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        panic!("no sync fallback")
    }
    fn reconcile(
        &self,
        _: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        panic!("no replay/reconcile")
    }
    fn execute_async<'a>(
        &'a self,
        request: ConnectorCatalogMutationRequest,
        receiving: &'a ConnectorCatalogMutationReceiving,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
                        ConnectorError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        assert_eq!(self.calls.fetch_add(1, Ordering::SeqCst), 0);
        receiving
            .retain_original_owner(ConnectorProcessLocalPayload::new(
                request.target.clone(),
                self.original.clone(),
            ))
            .unwrap();
        Box::pin(Child {
            fixture: self,
            request,
            receiving,
            recorded: false,
        })
    }
}
struct Child<'a> {
    fixture: &'a Fixture,
    request: ConnectorCatalogMutationRequest,
    receiving: &'a ConnectorCatalogMutationReceiving,
    recorded: bool,
}
impl Future for Child<'_> {
    type Output = Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let f = this.fixture;
        let receipt = ConnectorCatalogMutationReceipt::try_new(
            f.descriptor.clone(),
            f.epoch,
            this.request.operation_id,
            this.request.operation.kind(),
            None,
        )
        .unwrap();
        let effect = if f.applied {
            ExternalMutationEffect::Applied
        } else {
            ExternalMutationEffect::NoOp
        };
        if !this.recorded && f.refusal.is_none() {
            if f.applied {
                this.receiving.record_applied_receipt(&receipt).unwrap();
                this.receiving
                    .record_finalization_context(&receipt, Ok(f.context.clone()))
                    .unwrap();
            }
            this.receiving
                .record_needed_committed(effect, &receipt, &ExternalMutationFinalization::Complete)
                .unwrap();
            this.recorded = true;
        }
        f.waiter.register(cx.waker());
        if !f.release.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if let Some(error) = &f.refusal {
            return Poll::Ready(Err(error.clone()));
        }
        Poll::Ready(Ok(ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            cleanup: ExternalMutationFinalization::Complete,
        }))
    }
}
impl Drop for Child<'_> {
    fn drop(&mut self) {
        // This assertion observes the production SPI receiving-before-child-Drop cut.
        assert!(self.receiving.observation().raw_outcome().is_some());
        self.fixture.drops.fetch_add(1, Ordering::SeqCst);
        if self.fixture.panic_drop {
            panic!("original catalog child destructor")
        }
    }
}
fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        1024,
        4096,
    )
    .unwrap()
}
#[test]
fn original_call_needed_and_cancelled_borrow_do_not_retire_pending_child() {
    let fixture = Fixture::new(true, context());
    let call = fixture.call();
    let mut cx = Context::from_waker(Waker::noop());
    {
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| {
            call.poll_progress(cx);
            Poll::<()>::Pending
        }));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 0);
    let o = call.receiving.observation();
    assert!(o.applied_receipt().is_some());
    assert!(o.needed_committed().is_some());
    assert!(o.finalization_context().is_some());
    assert!(!o.provider_exited());
    drop(o);
    fixture.release();
    call.poll_progress(&mut cx);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
    assert!(call.state.lock().unwrap().exited);
    assert!(call.receiving.observation().provider_exited());
}
#[test]
fn actual_noop_has_no_applied_continuation_and_drop_panic_preserves_raw() {
    let fixture = Fixture::new(false, context());
    let call = fixture.call();
    let mut cx = Context::from_waker(Waker::noop());
    call.poll_progress(&mut cx);
    let o = call.receiving.observation();
    assert!(o.applied_receipt().is_none());
    assert!(o.finalization_context().is_none());
    assert_eq!(super::super::observation_effect(&o), None);
    assert_eq!(
        o.needed_committed().unwrap().effect(),
        ExternalMutationEffect::NoOp
    );
    drop(o);
    fixture.release();
    call.poll_progress(&mut cx);
    assert!(call.state.lock().unwrap().exited);
    let mut fixture = Fixture::new(true, context());
    Arc::get_mut(&mut fixture).unwrap().panic_drop = true;
    let call = fixture.call();
    fixture.release();
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.poll_progress(&mut cx)));
    assert!(panic.is_err());
    assert!(call.state.lock().unwrap().busy);
    assert!(!call.state.lock().unwrap().exited);
    assert!(call.receiving.foreign_exit_unproven());
    assert!(matches!(
        call.receiving.observation().raw_outcome(),
        Some(Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            ..
        }))
    ));
    call.poll_progress(&mut cx);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}

#[test]
fn actual_call_refusal_preserves_original_domain_cause_through_product_projection() {
    use novarocks_failure_contract::FailureProjection;
    let failure = ConnectorError::from_domain(
        ConnectorErrorKind::ResourceExhausted,
        novarocks_workload_control::WorkError::Capacity("actual control fixture refusal"),
    );
    let expected = std::error::Error::source(&failure)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap()
        .clone();
    let mut fixture = Fixture::new(false, context());
    Arc::get_mut(&mut fixture).unwrap().refusal = Some(failure);
    let call = fixture.call();
    fixture.release();
    call.poll_progress(&mut Context::from_waker(Waker::noop()));
    assert!(call.state.lock().unwrap().exited);
    let raw = call
        .receiving
        .observation()
        .validated_outcome()
        .unwrap()
        .unwrap_err()
        .clone();
    let projected = super::super::connector(raw).into_product_error();
    assert_eq!(projected.original_failure(), Some(&expected));
    assert_eq!(
        projected.original_failure().unwrap().failure_fact(),
        expected.failure_fact()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.drops.load(Ordering::SeqCst), 1);
}
