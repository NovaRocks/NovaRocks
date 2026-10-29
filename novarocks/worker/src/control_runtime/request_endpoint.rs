// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original request's endpoint and unique external delivery leg. These
//! containers grant no Work, capacity, publication or retirement authority.

use super::actors::WorkerActor;
use super::request_ready::{ControlRequestReady, RequestExternalLeg, RequestReadySlot};
use novarocks_control_runtime::{
    publication::PublishedReply,
    reply_notification::{ReplyNotificationError, ReplyNotificationJob, ReplyObserver},
};
use novarocks_worker_protocol::creation::RequestContext;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Constructed only by the original admission owner after its actual slot and
/// role exist. The PublishedReply belongs to that caller's actual Scope; this
/// shared mechanical spine does not choose a Root or Context owner for it.
/// H is the caller's frozen neutral header, not a factory for input identity.
pub(super) struct RequestEndpoint<B, H: Copy = RequestContext> {
    pub(super) request: H,
    pub(super) ready: ControlRequestReady,
    pub(super) reply: PublishedReply<B>,
    pub(super) external: RequestExternalLeg,
}
impl<B, H: Copy> RequestEndpoint<B, H> {
    fn end_external(this: &Arc<Self>) {
        this.external.end();
    }
    pub(super) fn external_ended(&self) -> bool {
        self.external.is_ended()
    }
}

/// This handle cannot be cloned. Cancellation ends only the external wait;
/// the original custodian keeps its actual work, publication and exit duties.
#[must_use = "Take the original reply or end only this external wait"]
pub(crate) struct RequestHandle<B, H: Copy = RequestContext> {
    pub(super) endpoint: Arc<RequestEndpoint<B, H>>,
    pub(super) active: bool,
    pub(super) observer: Option<ReplyObserver<WorkerActor>>,
}
impl<B, H: Copy> RequestHandle<B, H> {
    pub(crate) fn request(&self) -> H {
        self.endpoint.request
    }
    pub(crate) fn try_take(&mut self) -> Option<RequestDelivery<B, H>> {
        if !self.active {
            return None;
        }
        let body = self.endpoint.external.try_take(&self.endpoint.reply)?;
        self.active = false;
        Some(RequestDelivery {
            endpoint: self.endpoint.clone(),
            body: Some(body),
            observer: self.observer.take(),
        })
    }
}
impl<B, H: Copy> Drop for RequestHandle<B, H> {
    fn drop(&mut self) {
        if self.active {
            drop(self.observer.take());
            RequestEndpoint::end_external(&self.endpoint);
        }
    }
}

/// Taking the outbox moves the same endpoint's external exit obligation to
/// this one delivery. It supplies no Core, source or physical Work exit fact.
#[must_use = "The actual sender retains delivery custody until it really ends"]
pub(crate) struct RequestDelivery<B, H: Copy = RequestContext> {
    endpoint: Arc<RequestEndpoint<B, H>>,
    body: Option<Arc<B>>,
    observer: Option<ReplyObserver<WorkerActor>>,
}
impl<B, H: Copy> RequestDelivery<B, H> {
    pub(crate) fn request(&self) -> H {
        self.endpoint.request
    }
    pub(crate) fn take_observer(&mut self) -> Option<ReplyObserver<WorkerActor>> {
        self.observer.take()
    }
    pub(crate) fn notify_original(&self) {
        self.endpoint.external.notify();
    }
    pub(crate) fn body(&self) -> &B {
        self.body.as_deref().expect("live original reply delivery")
    }
}
impl<B, H: Copy> Drop for RequestDelivery<B, H> {
    fn drop(&mut self) {
        // A foreign body destructor runs outside all endpoint/phase locks. If
        // it panics, do not invent an Ended fact while unwinding that body.
        drop(self.body.take());
        drop(self.observer.take());
        RequestEndpoint::end_external(&self.endpoint);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestAwaitError {
    AlreadyTaken,
    Notification(ReplyNotificationError),
}
impl<B, H: Copy> Future for RequestHandle<B, H> {
    type Output = Result<RequestDelivery<B, H>, RequestAwaitError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.active {
            return Poll::Ready(Err(RequestAwaitError::AlreadyTaken));
        }
        if let Some(delivery) = this.try_take() {
            return Poll::Ready(Ok(delivery));
        }
        let observer = this.observer.as_mut().expect("same original R observer");
        match observer.arm(cx.waker().clone()) {
            Ok(armed) if !armed.failed => {}
            Ok(_) => {
                return Poll::Ready(Err(RequestAwaitError::Notification(
                    ReplyNotificationError::Failed,
                )));
            }
            Err(refusal) => {
                // This polling thread still owns a refused, never-retained Waker.
                let error = refusal.error;
                drop(refusal.waker);
                return Poll::Ready(Err(RequestAwaitError::Notification(error)));
            }
        }
        this.endpoint.external.notify();
        if let Some(delivery) = this.try_take() {
            return Poll::Ready(Ok(delivery));
        }
        Poll::Pending
    }
}

/// One physical notification capture of the same original R. Common owns
/// the callback mechanics; this wrapper only marks its exact ready position.
#[must_use = "Preserve the same original notification job on queue refusal"]
pub(crate) struct RequestNotificationJob {
    pub(super) original: ReplyNotificationJob<WorkerActor>,
    pub(super) notice: RequestNotice,
}
impl RequestNotificationJob {
    pub(crate) fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        self.original.queue_key()
    }
    pub(crate) fn belongs_to(&self, ready: &ControlRequestReady) -> bool {
        self.notice.belongs_to(ready)
    }
    pub(crate) fn run(self) {
        self.original.run();
        self.notice.notify();
    }
}

/// A concrete coalesced wake carries only the original weak ready address;
/// retaining or cloning it cannot retain the endpoint, body or scope owner.
#[derive(Clone)]
pub(super) struct RequestNotice(pub(super) Arc<RequestReadySlot>);
impl RequestNotice {
    pub(super) fn belongs_to(&self, ready: &ControlRequestReady) -> bool {
        self.0.belongs_to(ready)
    }
    pub(super) fn notify(&self) {
        self.0.notify();
    }
}

/// Physical capacity refusal is distinct from queue corruption or shutdown.
pub(crate) fn notification_submit_busy(error: &super::host_executor::HostSubmitError) -> bool {
    use novarocks_control_runtime_core::fair_queue::FairQueueError;
    matches!(
        error,
        super::host_executor::HostSubmitError::Queue(
            FairQueueError::Full
                | FairQueueError::ContextFull
                | FairQueueError::ContextCapacity
                | FairQueueError::DuplicateWork
        )
    )
}
