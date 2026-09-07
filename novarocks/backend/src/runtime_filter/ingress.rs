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

//! Composition of the backend's runtime-filter envelope ingress.
//!
//! One owner installs runtime-filter participants on this backend process:
//! the query-context host installs the participant named by an
//! `EstablishQueryContext`, and every intent runs on the task protocol. A
//! participant is reachable only through the owner that installed it, so the
//! wire ingress asks that owner.
//!
//! The question the owner answers is ownership, not permission: it returns
//! the participant it installed for the exact attempt, or nothing. Accept,
//! duplicate and reject remain the participant's own verdict.
//!
//! With one registered owner the composite's two-claimant conflict arm cannot
//! fire -- one authority produces at most one claimant -- while its
//! zero-claimant refusal stays live and is the normal answer for an envelope
//! naming an attempt this backend no longer holds. The composite is kept
//! anyway because that conflict arm is the reason it exists: whoever adds a
//! second participant owner needs the arm already in place, so that two
//! owners claiming one attempt is a refusal rather than a race the wiring
//! order settles.

use std::sync::Arc;

use tracing::{error, warn};

use crate::runtime_filter::domain::{BackendIngressResult, BackendParticipantIdentity};
use crate::runtime_filter::participant::RuntimeFilterParticipant;
use crate::runtime_filter::rpc::{
    BackendNativeRuntimeFilterEnvelope, BackendRuntimeFilterEnvelopeIngress,
};
use crate::task_execution::NativeQueryContextHost;

const NO_OWNER_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: no runtime-filter participant owner on this backend holds this query execution attempt";
const CONFLICTING_OWNERS_REJECTION: &str = "runtime filter ingress rejected [query-unavailable]: this query execution attempt is claimed by more than one runtime-filter participant owner";

/// One owner of runtime-filter participants on this backend.
///
/// An owner answers only for attempts it installed. It never speaks for
/// another owner's attempts and never falls back to another owner's
/// participant; the ingress composes the answers.
pub(crate) trait BackendRuntimeFilterParticipantAuthority: Send + Sync + 'static {
    /// A stable name used in refusal logs, so an operator reading a refused
    /// envelope can tell which owners were asked.
    fn authority_name(&self) -> &'static str;

    /// The participant this owner installed for the exact attempt.
    ///
    /// `None` is a statement of non-ownership, never a refusal: it covers both
    /// "this attempt is not mine" and "this attempt is mine but installed no
    /// filter on this backend", and neither is this owner's decision to make
    /// about another owner's attempt.
    fn claim_participant(
        &self,
        participant: BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>>;
}

/// The task protocol's query-context host as a participant owner.
///
/// Every intent runs on the task protocol. Without this owner wired, no
/// runtime-filter envelope can reach a participant at all: each producer
/// contribution and each materialized artifact is refused at the peer, so
/// every consumer waits out its whole wait cap and then scans unfiltered.
struct QueryContextParticipantAuthority(Arc<NativeQueryContextHost>);

impl BackendRuntimeFilterParticipantAuthority for QueryContextParticipantAuthority {
    fn authority_name(&self) -> &'static str {
        "the task query-context host"
    }

    fn claim_participant(
        &self,
        participant: BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        self.0.claim_runtime_filter_participant(participant)
    }
}

/// The wire ingress every runtime-filter envelope arrives through.
pub(crate) struct CompositeRuntimeFilterEnvelopeIngress {
    authorities: Vec<Arc<dyn BackendRuntimeFilterParticipantAuthority>>,
}

impl CompositeRuntimeFilterEnvelopeIngress {
    pub(crate) fn new(
        authorities: Vec<Arc<dyn BackendRuntimeFilterParticipantAuthority>>,
    ) -> Arc<Self> {
        Arc::new(Self { authorities })
    }
}

impl BackendRuntimeFilterEnvelopeIngress for CompositeRuntimeFilterEnvelopeIngress {
    fn accept(&self, envelope: BackendNativeRuntimeFilterEnvelope) -> BackendIngressResult {
        let identity = envelope.participant();
        let mut claimants: Vec<(&'static str, Arc<RuntimeFilterParticipant>)> = Vec::new();
        for authority in &self.authorities {
            if let Some(participant) = authority.claim_participant(identity) {
                claimants.push((authority.authority_name(), participant));
            }
        }
        match claimants.len() {
            // Live: one owner is registered, and it disclaims every attempt
            // it did not install.
            0 => {
                // A runtime filter is a conservative pre-filter, so this
                // refusal never wrongs a result -- it costs the consumer its
                // whole wait cap and then its pruning. That is exactly why it
                // has to be logged: nothing downstream fails, so an unlogged
                // refusal is indistinguishable from a filter that was never
                // built.
                warn!(
                    target: "novarocks::runtime_filter",
                    query_id = ?identity.query_id(),
                    deployment_epoch = identity.deployment_epoch(),
                    channel_id = envelope.channel_id().get(),
                    kind = ?envelope.kind(),
                    authorities = ?self.authority_names(),
                    "runtime filter envelope refused: no participant owner holds this attempt"
                );
                rejected(NO_OWNER_REJECTION)
            }
            1 => {
                let (_, participant) = claimants.pop().expect("one claimant");
                participant.dispatch_envelope(envelope)
            }
            // Unreachable from this process's composition: one registered
            // authority can contribute at most one claimant. It is kept
            // because a second owner must land on a refusal here rather than
            // on whichever answer the wiring order produced first.
            _ => {
                error!(
                    target: "novarocks::runtime_filter",
                    query_id = ?identity.query_id(),
                    deployment_epoch = identity.deployment_epoch(),
                    channel_id = envelope.channel_id().get(),
                    kind = ?envelope.kind(),
                    claimants = ?claimants
                        .iter()
                        .map(|(name, _)| *name)
                        .collect::<Vec<_>>(),
                    "runtime filter envelope refused: two participant owners claim one attempt"
                );
                rejected(CONFLICTING_OWNERS_REJECTION)
            }
        }
    }
}

impl CompositeRuntimeFilterEnvelopeIngress {
    fn authority_names(&self) -> Vec<&'static str> {
        self.authorities
            .iter()
            .map(|authority| authority.authority_name())
            .collect()
    }
}

fn rejected(reason: &'static str) -> BackendIngressResult {
    BackendIngressResult::rejected(reason).expect("a composed refusal reason is non-empty")
}

/// The one production composition of this backend's runtime-filter ingress.
///
/// The owner wired here answers only for the attempts it installed; an
/// envelope naming anything else is refused rather than routed somewhere as a
/// fallback.
pub(crate) fn native_runtime_filter_envelope_ingress(
    query_contexts: Arc<NativeQueryContextHost>,
) -> Arc<dyn BackendRuntimeFilterEnvelopeIngress> {
    CompositeRuntimeFilterEnvelopeIngress::new(vec![Arc::new(QueryContextParticipantAuthority(
        query_contexts,
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use crate::runtime_filter::domain::{BackendAcceptStatus, BackendEnvelopeKind};
    use crate::runtime_filter::test_support::participant_for_test;

    struct StubAuthority {
        name: &'static str,
        participant: Option<Arc<RuntimeFilterParticipant>>,
    }

    impl StubAuthority {
        fn holding(name: &'static str) -> Arc<dyn BackendRuntimeFilterParticipantAuthority> {
            Arc::new(Self {
                name,
                participant: Some(participant_for_test()),
            })
        }

        fn disclaiming(name: &'static str) -> Arc<dyn BackendRuntimeFilterParticipantAuthority> {
            Arc::new(Self {
                name,
                participant: None,
            })
        }
    }

    impl BackendRuntimeFilterParticipantAuthority for StubAuthority {
        fn authority_name(&self) -> &'static str {
            self.name
        }

        fn claim_participant(
            &self,
            _participant: BackendParticipantIdentity,
        ) -> Option<Arc<RuntimeFilterParticipant>> {
            self.participant.clone()
        }
    }

    fn envelope() -> BackendNativeRuntimeFilterEnvelope {
        crate::runtime_filter::test_support::delivery_envelope_for_test(
            BackendEnvelopeKind::CompletedWithoutArtifact,
        )
    }

    #[test]
    fn an_attempt_no_owner_holds_is_refused_rather_than_admitted() {
        let ingress = CompositeRuntimeFilterEnvelopeIngress::new(vec![
            StubAuthority::disclaiming("first"),
            StubAuthority::disclaiming("second"),
        ]);
        let result = ingress.accept(envelope());
        assert_eq!(result.status(), BackendAcceptStatus::Rejected);
        assert_eq!(result.rejection_reason(), Some(NO_OWNER_REJECTION));
    }

    #[test]
    fn an_ingress_with_no_owner_wired_refuses_instead_of_admitting() {
        let ingress = CompositeRuntimeFilterEnvelopeIngress::new(Vec::new());
        let result = ingress.accept(envelope());
        assert_eq!(result.status(), BackendAcceptStatus::Rejected);
        assert_eq!(result.rejection_reason(), Some(NO_OWNER_REJECTION));
    }

    #[test]
    fn two_owners_claiming_one_attempt_is_a_conflict_not_a_first_answer_win() {
        // Taking either answer would let one owner run envelopes the other
        // believes it owns, and the wiring order would decide which.
        let ingress = CompositeRuntimeFilterEnvelopeIngress::new(vec![
            StubAuthority::holding("first"),
            StubAuthority::holding("second"),
        ]);
        let result = ingress.accept(envelope());
        assert_eq!(result.status(), BackendAcceptStatus::Rejected);
        assert_eq!(
            result.rejection_reason(),
            Some(CONFLICTING_OWNERS_REJECTION)
        );
    }

    #[test]
    fn the_one_owner_that_holds_the_attempt_decides_the_envelope() {
        // Whichever slot holds it, and in either order: the sole claimant's
        // own verdict is the answer, so this reaches the participant's route
        // authority instead of a composition refusal.
        for authorities in [
            vec![
                StubAuthority::disclaiming("first"),
                StubAuthority::holding("second"),
            ],
            vec![
                StubAuthority::holding("first"),
                StubAuthority::disclaiming("second"),
            ],
        ] {
            let ingress = CompositeRuntimeFilterEnvelopeIngress::new(authorities);
            let result = ingress.accept(envelope());
            let reason = result
                .rejection_reason()
                .expect("a channel-less participant refuses this delivery itself");
            assert!(
                reason.contains("[artifact-delivery]"),
                "the sole owner's participant must decide, got: {reason}"
            );
        }
    }

    #[test]
    fn every_owner_is_asked_with_the_envelope_s_own_attempt_identity() {
        // A lookup that ignored the identity would resolve any envelope to
        // whatever participant the owner happens to hold, which is how a
        // late envelope of a finished attempt reaches a live one.
        let asked = Arc::new(Mutex::new(Vec::new()));
        struct RecordingAuthority(Arc<Mutex<Vec<BackendParticipantIdentity>>>);
        impl BackendRuntimeFilterParticipantAuthority for RecordingAuthority {
            fn authority_name(&self) -> &'static str {
                "recording"
            }

            fn claim_participant(
                &self,
                participant: BackendParticipantIdentity,
            ) -> Option<Arc<RuntimeFilterParticipant>> {
                self.0.lock().expect("recording lock").push(participant);
                None
            }
        }
        let ingress = CompositeRuntimeFilterEnvelopeIngress::new(vec![Arc::new(
            RecordingAuthority(Arc::clone(&asked)),
        )]);
        let envelope = envelope();
        let expected = envelope.participant();
        let _ = ingress.accept(envelope);
        assert_eq!(asked.lock().expect("recording lock").as_slice(), [expected]);
    }
}
