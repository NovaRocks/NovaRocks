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

use super::request_ready::{ControlRequestReady, RequestExternalLeg, RequestReadySlot};
use novarocks_control_runtime::publication::PublishedReply;
use novarocks_worker_protocol::creation::RequestContext;
use std::sync::Arc;

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
        })
    }
}
impl<B, H: Copy> Drop for RequestHandle<B, H> {
    fn drop(&mut self) {
        if self.active {
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
}
impl<B, H: Copy> RequestDelivery<B, H> {
    pub(crate) fn request(&self) -> H {
        self.endpoint.request
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
        RequestEndpoint::end_external(&self.endpoint);
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
