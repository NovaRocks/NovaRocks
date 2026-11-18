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

//! External caller/publication facts after an actual LP-accepted scalar cut.
//! No variant below is a LogicalInput or a production provider/Work capability.
//! The model's scalar metadata publication is not a FA PreparedWrite contract.
use super::{
    Seed,
    internal::{AcceptedScalar, ScalarPayload},
};
use novarocks_coordination_contract::AttemptComputationCut;
use std::sync::Weak;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitOutcome {
    Committed,
    Uncommitted,
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinalizeOutcome {
    Succeeded,
    Failed,
    NotApplicable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationRefusal {
    Cancelled,
    AlreadyAuthorized,
    AlreadyDispatched,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationEvent {
    SafeReady {
        cut: AttemptComputationCut,
        scalar: i64,
    },
    CancellationObserved,
    Authorization {
        receipt: u64,
        cut: AttemptComputationCut,
    },
    Refused(PublicationRefusal),
    MayDispatch {
        receipt: u64,
        scalar: i64,
    },
    NeverDispatched,
    Outcome(CommitOutcome),
    Finalized(FinalizeOutcome),
    CleanupScheduled {
        due: u64,
    },
    CleanupExited {
        at: u64,
    },
}
#[derive(Default)]
pub(crate) struct PublicationHistory(pub(crate) Vec<PublicationEvent>);
impl PublicationHistory {
    pub(crate) fn check(&self) {
        let mut ready = None;
        let mut cancelled = false;
        let mut authorization = None;
        let mut may_dispatch = false;
        let mut never_dispatched = false;
        let mut outcome = None;
        let mut finalized = None;
        let mut due = None;
        let mut cleanup = false;
        for event in &self.0 {
            match *event {
                PublicationEvent::SafeReady { cut, scalar } => {
                    assert!(ready.replace((cut, scalar)).is_none())
                }
                PublicationEvent::CancellationObserved => cancelled = true,
                PublicationEvent::Authorization { receipt, cut } => {
                    assert!(!cancelled && receipt != 0 && ready.unwrap().0 == cut);
                    assert!(
                        authorization.replace(receipt).is_none(),
                        "one external publication authorization"
                    );
                }
                PublicationEvent::Refused(reason) => match reason {
                    PublicationRefusal::Cancelled => assert!(cancelled),
                    PublicationRefusal::AlreadyAuthorized => assert!(authorization.is_some()),
                    PublicationRefusal::AlreadyDispatched => assert!(may_dispatch),
                },
                PublicationEvent::MayDispatch { receipt, scalar } => {
                    assert_eq!(ready.unwrap().1, scalar);
                    assert!(
                        !cancelled
                            && authorization == Some(receipt)
                            && !may_dispatch
                            && !never_dispatched
                    );
                    may_dispatch = true;
                }
                PublicationEvent::NeverDispatched => {
                    assert!(!may_dispatch && !never_dispatched);
                    never_dispatched = true;
                }
                PublicationEvent::Outcome(actual) => {
                    assert!(outcome.replace(actual).is_none());
                    assert!(
                        may_dispatch || (never_dispatched && actual == CommitOutcome::Uncommitted)
                    );
                }
                PublicationEvent::Finalized(actual) => {
                    assert!(outcome.is_some() && finalized.replace(actual).is_none());
                    assert_eq!(
                        actual == FinalizeOutcome::NotApplicable,
                        outcome != Some(CommitOutcome::Committed)
                    );
                }
                PublicationEvent::CleanupScheduled { due: actual } => {
                    assert!(finalized.is_some() && due.replace(actual).is_none());
                }
                PublicationEvent::CleanupExited { at } => {
                    assert!(at >= due.unwrap() && !cleanup);
                    cleanup = true;
                }
            }
        }
        assert!(ready.is_some() && outcome.is_some() && finalized.is_some() && cleanup);
    }
}

/// The world explicitly supplies its own authorization and external outcome.
/// Fixed caller rules only refuse new business after observed cancellation;
/// necessary finalize of an already committed outcome remains independent.
pub(crate) struct StatementPublicationPort {
    original: Option<AcceptedScalar>,
    seed: Seed,
    tick: u64,
    cancelled: bool,
    authorization: Option<u64>,
    may_dispatch: bool,
    never_dispatched: bool,
    outcome: Option<CommitOutcome>,
    finalized: Option<FinalizeOutcome>,
    cleanup_due: Option<u64>,
    history: PublicationHistory,
}
impl StatementPublicationPort {
    pub(crate) fn new(original: AcceptedScalar, seed: u64) -> Self {
        let mut history = PublicationHistory::default();
        history.0.push(PublicationEvent::SafeReady {
            cut: original.cut(),
            scalar: original.value(),
        });
        Self {
            original: Some(original),
            seed: Seed::new(seed),
            tick: 0,
            cancelled: false,
            authorization: None,
            may_dispatch: false,
            never_dispatched: false,
            outcome: None,
            finalized: None,
            cleanup_due: None,
            history,
        }
    }
    pub(crate) fn weak_payload(&self) -> Weak<ScalarPayload> {
        self.original.as_ref().unwrap().weak_payload()
    }
    pub(crate) fn cancel_new_business(&mut self) {
        self.cancelled = true;
        self.history.0.push(PublicationEvent::CancellationObserved);
    }
    fn refusal(&mut self, reason: PublicationRefusal) -> Result<(), PublicationRefusal> {
        self.history.0.push(PublicationEvent::Refused(reason));
        Err(reason)
    }
    pub(crate) fn authorize(&mut self, original_receipt: u64) -> Result<(), PublicationRefusal> {
        if self.cancelled {
            return self.refusal(PublicationRefusal::Cancelled);
        }
        if self.authorization.is_some() {
            return self.refusal(PublicationRefusal::AlreadyAuthorized);
        }
        assert_ne!(original_receipt, 0);
        self.authorization = Some(original_receipt);
        self.history.0.push(PublicationEvent::Authorization {
            receipt: original_receipt,
            cut: self.original.as_ref().unwrap().cut(),
        });
        Ok(())
    }
    pub(crate) fn may_dispatch(&mut self) -> Result<(), PublicationRefusal> {
        if self.cancelled {
            return self.refusal(PublicationRefusal::Cancelled);
        }
        if self.may_dispatch {
            return self.refusal(PublicationRefusal::AlreadyDispatched);
        }
        let receipt = self
            .authorization
            .expect("original external authorization required");
        self.may_dispatch = true;
        self.history.0.push(PublicationEvent::MayDispatch {
            receipt,
            scalar: self.original.as_ref().unwrap().value(),
        });
        Ok(())
    }
    pub(crate) fn affirm_never_dispatched(&mut self) {
        assert!(!self.may_dispatch && !self.never_dispatched);
        self.never_dispatched = true;
        self.history.0.push(PublicationEvent::NeverDispatched);
    }
    pub(crate) fn external_outcome(&mut self, original: CommitOutcome) {
        assert!(self.outcome.is_none());
        assert!(
            self.may_dispatch || (self.never_dispatched && original == CommitOutcome::Uncommitted)
        );
        self.outcome = Some(original);
        self.history.0.push(PublicationEvent::Outcome(original));
    }
    pub(crate) fn finalize(&mut self, original: FinalizeOutcome) {
        let outcome = self.outcome.expect("external outcome precedes finalize");
        assert!(self.finalized.is_none());
        assert_eq!(
            original == FinalizeOutcome::NotApplicable,
            outcome != CommitOutcome::Committed
        );
        self.finalized = Some(original);
        self.history.0.push(PublicationEvent::Finalized(original));
    }
    pub(crate) fn delay_cleanup(&mut self) -> u64 {
        assert!(self.finalized.is_some() && self.cleanup_due.is_none());
        let delay = self.seed.delay() + 1;
        let due = self.tick + delay;
        self.cleanup_due = Some(due);
        self.history
            .0
            .push(PublicationEvent::CleanupScheduled { due });
        delay
    }
    pub(crate) fn advance_cleanup(&mut self) -> bool {
        self.tick += 1;
        let due = self
            .cleanup_due
            .expect("original cleanup scheduling fact required");
        if self.tick < due {
            return false;
        }
        if self.original.take().is_some() {
            self.history
                .0
                .push(PublicationEvent::CleanupExited { at: self.tick });
        }
        true
    }
    pub(crate) fn history(&self) -> &PublicationHistory {
        &self.history
    }
}
