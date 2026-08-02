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

//! Outbound remote leg of the delivery Router.
//!
//! The delivery Router splits an authorized delivery scope into loopback edges
//! (delivered in-process by [`super::loopback::LoopbackRouter`]) and remote edges.
//! For each remote edge the reliable transport stamps a complete domain envelope
//! and hands it to a [`RuntimeFilterEnvelopeSink`]. The sink owns transmission only.
//!
//! Since M3 the remote leg no longer talks to the sink directly: it flows through
//! the sender-side `ReliableEnvelopeTransport`
//! ([`crate::runtime_filter::service::reliable_transport`]), which buffers each
//! frame for ack-release and bounded retry and hands it to this sink as its
//! underlying transmit primitive. The sink stays a pure transport seam: M2C/M3
//! tests inject a recording (or drivable) fake, while RFD-6 wires the live network
//! sender behind the same trait.
//!
//! The sink never re-authorizes fanout — the [`RuntimeFilterRemoteRoute`] it
//! receives was already vetted by the Router's `route_delivery`, so the sink only
//! transmits to the route's peer participant/endpoint.

use novarocks::runtime_filter_transition::port::routing::RuntimeFilterRemoteRoute;
use novarocks::runtime_filter_transition::port::transport::{
    RuntimeFilterAcceptStatus, RuntimeFilterRouteIdentity, RuntimeFilterTransportEnvelope,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SinkSubmitOutcome {
    Submitted,
    QueueFull,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SinkTransportError {
    Network(String),
    Contract(String),
}

impl SinkTransportError {
    pub(crate) fn network(error: impl Into<String>) -> Self {
        Self::Network(error.into())
    }

    pub(crate) fn contract(error: impl Into<String>) -> Self {
        Self::Contract(error.into())
    }

    pub(crate) const fn is_contract(&self) -> bool {
        matches!(self, Self::Contract(_))
    }
}

impl std::fmt::Display for SinkTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(error) => write!(formatter, "runtime filter transport failure: {error}"),
            Self::Contract(error) => {
                write!(formatter, "runtime filter contract rejection: {error}")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SinkCompletion {
    Ack(RuntimeFilterRouteIdentity, RuntimeFilterAcceptStatus),
    TransportFailure(RuntimeFilterRouteIdentity, SinkTransportError),
}

/// Transport seam for the delivery Router's remote leg.
///
/// `try_send` must never perform network I/O or block. Implementations use bounded
/// queues and publish each unary result through `try_recv_completion`; retry,
/// buffering, deadline and sequence ownership remain above this seam.
pub(crate) trait RuntimeFilterEnvelopeSink: Send + Sync {
    fn try_send(
        &self,
        route: RuntimeFilterRemoteRoute,
        envelope: RuntimeFilterTransportEnvelope,
    ) -> SinkSubmitOutcome;

    fn try_recv_completion(&self) -> Option<SinkCompletion>;

    fn shutdown(&self);
}
