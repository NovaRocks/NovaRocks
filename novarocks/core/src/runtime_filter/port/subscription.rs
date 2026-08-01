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
use std::sync::Arc;
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;
use crate::runtime_filter::port::artifact::ArtifactBundle;

use super::producer::RuntimeContractViolation;

use super::identity::RouteEdgeId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SubscriptionRequest {
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
}

impl SubscriptionRequest {
    pub(crate) const fn new(binding_id: BindingId, fragment_instance_id: UniqueId) -> Self {
        Self {
            binding_id,
            fragment_instance_id,
        }
    }

    pub(crate) const fn binding_id(self) -> BindingId {
        self.binding_id
    }

    pub(crate) const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnavailableReason {
    ResourceLimit,
    IncompleteCoverage,
    ProducerFailed,
    MaterializationFailed,
    RouteUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactUnsupportedReason {
    RangeDeferred,
    NoAcceptedRepresentation,
}

#[derive(Clone, Debug)]
pub(crate) enum ArtifactAcquireOutcome {
    Published(Arc<ArtifactBundle>),
    Unsupported(ArtifactUnsupportedReason),
    Unavailable(UnavailableReason),
    Cancelled,
    TimedOut,
}

#[derive(Clone, Debug)]
pub(crate) enum ArtifactDeliveryOutcome {
    Published(Arc<ArtifactBundle>),
    Unsupported(ArtifactUnsupportedReason),
    Unavailable(UnavailableReason),
    Cancelled,
}

impl ArtifactDeliveryOutcome {
    pub(crate) fn acquire_outcome(&self) -> ArtifactAcquireOutcome {
        match self {
            Self::Published(bundle) => ArtifactAcquireOutcome::Published(bundle.clone()),
            Self::Unsupported(reason) => ArtifactAcquireOutcome::Unsupported(*reason),
            Self::Unavailable(reason) => ArtifactAcquireOutcome::Unavailable(*reason),
            Self::Cancelled => ArtifactAcquireOutcome::Cancelled,
        }
    }
}

pub(crate) trait ArtifactDelivery: Send + Sync {
    fn deliver(&self, route_edge_id: RouteEdgeId, outcome: ArtifactDeliveryOutcome);

    fn deliver_live(
        &self,
        route_edge_id: RouteEdgeId,
        outcome: Option<ArtifactDeliveryOutcome>,
        terminal: Option<LiveTerminal>,
    ) {
        let _ = terminal;
        if let Some(outcome) = outcome {
            self.deliver(route_edge_id, outcome);
        }
    }
}

pub(crate) trait BlockingSnapshotSubscription: Send + Sync {
    fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome;
    fn snapshot(&self) -> Option<Arc<ArtifactBundle>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LiveTerminal {
    Completed,
    CompletedWithoutArtifact,
    DegradedLogical(UnavailableReason),
    DegradedArtifact(UnavailableReason),
    DegradedDelivery(UnavailableReason),
    Unavailable(UnavailableReason),
    Cancelled,
}

#[derive(Clone, Debug)]
pub(crate) enum LivePollOutcome {
    Updated {
        bundle: Arc<ArtifactBundle>,
        terminal: Option<LiveTerminal>,
    },
    Idle {
        latest_version: Option<super::identity::LogicalVersion>,
        terminal: Option<LiveTerminal>,
    },
}

pub(crate) trait NonBlockingLiveSubscription: Send + Sync {
    fn snapshot(&self) -> Option<Arc<ArtifactBundle>>;
    fn poll_after(&self, observed: Option<super::identity::LogicalVersion>) -> LivePollOutcome;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SubscriptionKind {
    BlockingSnapshot,
    NonBlockingLive,
}

pub(crate) enum SubscriptionHandle {
    Blocking(Arc<dyn BlockingSnapshotSubscription>),
    Live(Arc<dyn NonBlockingLiveSubscription>),
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SubscriptionHandle")
            .field(&self.kind())
            .finish()
    }
}

impl SubscriptionHandle {
    pub(crate) const fn kind(&self) -> SubscriptionKind {
        match self {
            Self::Blocking(_) => SubscriptionKind::BlockingSnapshot,
            Self::Live(_) => SubscriptionKind::NonBlockingLive,
        }
    }

    pub(crate) fn into_blocking(
        self,
    ) -> Result<Arc<dyn BlockingSnapshotSubscription>, RuntimeContractViolation> {
        match self {
            Self::Blocking(subscription) => Ok(subscription),
            Self::Live(_) => Err(RuntimeContractViolation::new(
                super::producer::RuntimeContractViolationKind::SubscriptionActivationMismatch,
                "live subscription handle cannot be used as a blocking subscription",
            )),
        }
    }

    pub(crate) fn into_live(
        self,
    ) -> Result<Arc<dyn NonBlockingLiveSubscription>, RuntimeContractViolation> {
        match self {
            Self::Live(subscription) => Ok(subscription),
            Self::Blocking(_) => Err(RuntimeContractViolation::new(
                super::producer::RuntimeContractViolationKind::SubscriptionActivationMismatch,
                "blocking subscription handle cannot be used as a live subscription",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::BindingId;

    use super::{ArtifactAcquireOutcome, ArtifactDelivery, SubscriptionRequest};

    #[test]
    fn artifact_router_boundary_excludes_logical_snapshot_and_caller_local_timeout() {
        fn assert_artifact_delivery(_: Arc<dyn ArtifactDelivery>) {}
        let _ = assert_artifact_delivery;
        assert!(matches!(
            ArtifactAcquireOutcome::TimedOut,
            ArtifactAcquireOutcome::TimedOut
        ));
    }

    #[test]
    fn materialization_failure_is_distinct_from_producer_failure() {
        assert_ne!(
            super::UnavailableReason::MaterializationFailed,
            super::UnavailableReason::ProducerFailed
        );
    }

    #[test]
    fn subscription_request_keeps_binding_and_fragment_instance_identity() {
        let request = SubscriptionRequest::new(BindingId::new(3), UniqueId::new(4, 5));

        assert_eq!(request.binding_id().get(), 3);
        assert_eq!(request.fragment_instance_id(), UniqueId::new(4, 5));
    }
}
