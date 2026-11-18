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

use super::{
    CommitOutcome, FinalizeOutcome, PublicationRefusal, StatementPublicationPort,
    audit_consumer_effects, internal_tests::accepted_scalar,
};
use crate::support::*;

#[test]
fn seeded_external_publication_outcomes_never_reauthor_the_lp_ready_cut() {
    for seed in 0..12 {
        let (mut f, internal, accepted) = accepted_scalar(seed);
        let cut = accepted.cut();
        let mut port = StatementPublicationPort::new(accepted, seed);
        let payload = port.weak_payload();
        port.authorize(seed + 1).unwrap();
        assert_eq!(
            port.authorize(seed + 101),
            Err(PublicationRefusal::AlreadyAuthorized)
        );
        port.may_dispatch().unwrap();
        assert_eq!(
            port.may_dispatch(),
            Err(PublicationRefusal::AlreadyDispatched)
        );
        let outcome = match seed % 3 {
            0 => CommitOutcome::Committed,
            1 => CommitOutcome::Uncommitted,
            _ => CommitOutcome::Unknown,
        };
        if seed % 2 == 0 {
            port.cancel_new_business();
            f.send(LogicalInput::Cancelled(novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::DeadlineExceeded));
        }
        // Original external callback authors this fact. It is never LP input.
        port.external_outcome(outcome);
        let finalized = match outcome {
            CommitOutcome::Committed if seed % 2 == 0 => FinalizeOutcome::Failed,
            CommitOutcome::Committed => FinalizeOutcome::Succeeded,
            _ => FinalizeOutcome::NotApplicable,
        };
        port.finalize(finalized);
        let delay = port.delay_cleanup();
        assert!(payload.upgrade().is_some());
        for _ in 1..delay {
            assert!(!port.advance_cleanup());
            assert!(payload.upgrade().is_some());
            assert_eq!(f.core.facts().ready_cut(), Some(cut));
        }
        assert!(port.advance_cleanup());
        assert!(
            payload.upgrade().is_none(),
            "the SAME complete scalar payload actually leaves the model"
        );
        assert_eq!(f.core.facts().ready_cut(), Some(cut));
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::ComputationReady)
        );
        internal.history().check();
        port.history().check();
        audit_consumer_effects(&f);
    }
}
#[test]
fn seeded_post_ready_cancellation_blocks_only_unissued_external_publication() {
    for after_authorization in [false, true] {
        let (mut f, internal, accepted) = accepted_scalar(u64::from(after_authorization) + 100);
        let cut = accepted.cut();
        let mut port = StatementPublicationPort::new(accepted, 70);
        if after_authorization {
            port.authorize(1).unwrap();
        }
        port.cancel_new_business();
        f.send(LogicalInput::Cancelled(novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::ClientDisconnected));
        if after_authorization {
            assert_eq!(port.may_dispatch(), Err(PublicationRefusal::Cancelled));
        } else {
            assert_eq!(port.authorize(1), Err(PublicationRefusal::Cancelled));
        }
        // This is an affirmative local never-issued boundary, not inference
        // of Uncommitted from a missing or failed remote callback.
        port.affirm_never_dispatched();
        port.external_outcome(CommitOutcome::Uncommitted);
        port.finalize(FinalizeOutcome::NotApplicable);
        let delay = port.delay_cleanup();
        for _ in 0..delay {
            port.advance_cleanup();
        }
        assert_eq!(f.core.facts().ready_cut(), Some(cut));
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::ComputationReady)
        );
        internal.history().check();
        port.history().check();
        audit_consumer_effects(&f);
    }
}
