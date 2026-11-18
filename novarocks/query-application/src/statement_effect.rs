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

//! Statement-local proof that an automatic topology retry is still safe.
//!
//! This is deliberately not a publication journal and does not alter LNP-1
//! disposition/OCC ownership.  It is only a monotonic frontend admission
//! permit: an external dispatch, an unknown boundary, or ControlReady closes
//! the permit forever for this statement.

use std::sync::{Arc, Mutex};

use novarocks_spi::connector::LakePublicationId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalDispatchKind {
    Writer,
    Staging,
    CatalogMutation,
    Publication,
    ProviderMutation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyRetryClosure {
    ExternalDispatch(ExternalDispatchKind),
    UnknownExternalDispatch,
    ControlReady,
    StageOrStart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EffectState {
    NoExternalDispatch,
    Closed(TopologyRetryClosure),
}

/// A capability issued only after the tracker positively observes that this
/// statement has not crossed any external-effect boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyRetryPermit {
    ReadOnly,
    Mutating { publication_id: LakePublicationId },
}

impl TopologyRetryPermit {
    pub const fn publication_id(self) -> Option<LakePublicationId> {
        match self {
            Self::ReadOnly => None,
            Self::Mutating { publication_id } => Some(publication_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyRetryPermitError {
    Closed(TopologyRetryClosure),
}

/// Shared by all dispatch owners for one logical statement.  The mutex is
/// intentionally small and protects only the one-way state transition; no
/// connector or provider callback is made while it is held.
#[derive(Clone, Debug)]
pub struct StatementEffectTracker {
    publication_id: Option<LakePublicationId>,
    state: Arc<Mutex<EffectState>>,
}

impl StatementEffectTracker {
    pub fn read_only() -> Self {
        Self {
            publication_id: None,
            state: Arc::new(Mutex::new(EffectState::NoExternalDispatch)),
        }
    }

    pub fn mutating(publication_id: LakePublicationId) -> Self {
        Self {
            publication_id: Some(publication_id),
            state: Arc::new(Mutex::new(EffectState::NoExternalDispatch)),
        }
    }

    /// Must run immediately before a writer, staging, catalog publication, or
    /// provider mutation adapter call.  A failure after this call remains
    /// closed: it is never evidence that dispatch did not occur.
    pub fn close_before_dispatch(&self, kind: ExternalDispatchKind) {
        self.close(TopologyRetryClosure::ExternalDispatch(kind));
    }

    /// Close at an adapter boundary whose dispatch result cannot be proven.
    pub fn close_for_unknown_dispatch(&self) {
        self.close(TopologyRetryClosure::UnknownExternalDispatch);
    }

    /// `ControlReady` is the crash-only boundary of this retry mechanism.
    pub fn close_after_control_ready(&self) {
        self.close(TopologyRetryClosure::ControlReady);
    }

    /// Stage or Start permanently closes the pre-ready retry window even for
    /// a read-only statement.
    pub fn close_after_stage_or_start(&self) {
        self.close(TopologyRetryClosure::StageOrStart);
    }

    pub fn issue_topology_retry_permit(
        &self,
    ) -> Result<TopologyRetryPermit, TopologyRetryPermitError> {
        match *self
            .state
            .lock()
            .expect("statement effect tracker poisoned")
        {
            EffectState::NoExternalDispatch => Ok(match self.publication_id {
                Some(publication_id) => TopologyRetryPermit::Mutating { publication_id },
                None => TopologyRetryPermit::ReadOnly,
            }),
            EffectState::Closed(closure) => Err(TopologyRetryPermitError::Closed(closure)),
        }
    }

    pub fn closure(&self) -> Option<TopologyRetryClosure> {
        match *self
            .state
            .lock()
            .expect("statement effect tracker poisoned")
        {
            EffectState::NoExternalDispatch => None,
            EffectState::Closed(closure) => Some(closure),
        }
    }

    fn close(&self, closure: TopologyRetryClosure) {
        let mut state = self
            .state
            .lock()
            .expect("statement effect tracker poisoned");
        if matches!(*state, EffectState::NoExternalDispatch) {
            *state = EffectState::Closed(closure);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::{
        ExternalDispatchKind, StatementEffectTracker, TopologyRetryClosure, TopologyRetryPermit,
        TopologyRetryPermitError,
    };
    use novarocks_spi::connector::LakePublicationId;

    #[test]
    fn mutating_permit_carries_the_first_publication_identity() {
        let publication_id = LakePublicationId::new_v7();
        let tracker = StatementEffectTracker::mutating(publication_id);

        assert_eq!(
            tracker.issue_topology_retry_permit(),
            Ok(TopologyRetryPermit::Mutating { publication_id })
        );
        assert_eq!(
            tracker
                .issue_topology_retry_permit()
                .expect("effect-free statement retains its permit")
                .publication_id(),
            Some(publication_id)
        );
    }

    #[test]
    fn dispatch_closes_the_permit_before_the_adapter_can_run() {
        let tracker = StatementEffectTracker::read_only();
        tracker.close_before_dispatch(ExternalDispatchKind::Staging);

        assert_eq!(
            tracker.issue_topology_retry_permit(),
            Err(TopologyRetryPermitError::Closed(
                TopologyRetryClosure::ExternalDispatch(ExternalDispatchKind::Staging)
            ))
        );
    }

    #[test]
    fn first_closure_wins_under_concurrent_dispatch_owners() {
        let tracker = Arc::new(StatementEffectTracker::read_only());
        let mut workers = Vec::new();
        for kind in [
            ExternalDispatchKind::Writer,
            ExternalDispatchKind::CatalogMutation,
            ExternalDispatchKind::Publication,
            ExternalDispatchKind::ProviderMutation,
        ] {
            let tracker = Arc::clone(&tracker);
            #[expect(
                clippy::disallowed_methods,
                reason = "This test joins the competing dispatch owners before checking the original first closure."
            )]
            workers.push(thread::spawn(move || tracker.close_before_dispatch(kind)));
        }
        for worker in workers {
            worker.join().expect("dispatch owner must not panic");
        }

        assert!(matches!(
            tracker.closure(),
            Some(TopologyRetryClosure::ExternalDispatch(_))
        ));
        assert!(tracker.issue_topology_retry_permit().is_err());
    }

    #[test]
    fn control_ready_cannot_be_reopened_by_later_observations() {
        let tracker = StatementEffectTracker::read_only();
        tracker.close_after_control_ready();
        tracker.close_for_unknown_dispatch();
        tracker.close_after_stage_or_start();

        assert_eq!(tracker.closure(), Some(TopologyRetryClosure::ControlReady));
    }
}
