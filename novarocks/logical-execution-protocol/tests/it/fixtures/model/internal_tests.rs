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
    AcceptedScalar, InternalPort, ScalarPayload, audit_consumer_effects, scalar_start,
    world_success_seal,
};
use crate::support::*;
use novarocks_coordination_contract::{
    AttemptRootFailure, AttemptRootProjection, QueryFailureReason,
};
use novarocks_logical_execution_protocol::failure::query_failure;

pub(super) fn scalar_world(seed: u64, payload: ScalarPayload) -> (Fixture, InternalPort) {
    let mut f = Fixture::with_start(scalar_start());
    f.budget = 3;
    f.begin();
    let status = TaskStatus::try_new(
        root(1),
        TaskStatusVersion::new(3).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::new(false),
    )
    .unwrap();
    // Passive world supplies Native/protocol facts; the domain port never does.
    f.port(AttemptToLogicalBody::RootStatus(Box::new(
        AttemptRootProjection {
            status,
            failure: AttemptRootFailure::None,
        },
    )));
    let port = InternalPort::attach(&mut f, seed, payload);
    (f, port)
}
pub(super) fn accepted_scalar(seed: u64) -> (Fixture, InternalPort, AcceptedScalar) {
    let (mut f, mut port) = scalar_world(seed, ScalarPayload::new(37));
    assert!(port.decode_complete_domain(&mut f));
    port.propose(&mut f);
    assert!(f.core.facts().ready_cut().is_none());
    f.finished_root();
    assert!(f.core.facts().ready_cut().is_none());
    world_success_seal(&mut f, |token| port.observe_success_seal(token));
    let original = port.accept_ready(&mut f);
    assert_eq!(f.core.facts().ready_cut(), Some(original.cut()));
    port.history().check();
    (f, port, original)
}
#[test]
fn seeded_scalar_codec_proof_waits_original_end_finished_and_seal_before_ready() {
    for seed in 0..8 {
        let (mut f, mut port, accepted) = accepted_scalar(seed);
        assert_eq!(accepted.value(), 37);
        assert_eq!(accepted.cut().end, end(1, 1));
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::ComputationReady)
        );
        // ComputationReady is accepted before the mechanical read-close reply.
        let close = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::SealRootRead { .. })
            )
        });
        assert!(!f.effects.iter().any(|e| matches!(&e.body,
            LogicalEffectBody::AttemptPort(p) if matches!(p.body, LogicalToAttemptBody::BeginConvergence { .. }))));
        f.ack(close.action, ActionResult::Native(NativeFact::Applied));
        let cut = accepted.cut();
        f.send(LogicalInput::Cancelled(novarocks_logical_execution_protocol::cancel::LogicalCancellationReason::DeadlineExceeded));
        port.close_source(&mut f);
        port.report_exit(
            &mut f,
            Some(Err(query_failure(
                QueryFailureReason::InternalConsumerPanicked,
                "late original modeled consumer verdict",
            ))),
        );
        assert_eq!(f.core.facts().ready_cut(), Some(cut));
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::ComputationReady)
        );
        port.history().check();
        audit_consumer_effects(&f);
    }
}
#[test]
fn seeded_scalar_domain_rejects_incomplete_original_codec_record_without_ready() {
    for seed in 10..14 {
        let mut original = ScalarPayload::new(37);
        original.corrupt();
        let (mut f, mut port) = scalar_world(seed, original);
        assert!(!port.decode_complete_domain(&mut f));
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert!(f.core.facts().ready_cut().is_none());
        port.history().check();
        audit_consumer_effects(&f);
    }
}
#[test]
fn seeded_ready_proposal_keeps_original_plan_receipt_and_end_correlation() {
    for variant in 0..3 {
        let (mut f, mut port) = scalar_world(20 + variant, ScalarPayload::new(37));
        assert!(port.decode_complete_domain(&mut f));
        let (receipt, original_end) = port.original_end();
        let proposal = match variant {
            0 => port.propose_exact(&mut f, LogicalPlanIdentity([11; 16]), receipt, original_end),
            1 => port.propose_exact(
                &mut f,
                PLAN,
                InternalEndReceiptId(std::num::NonZeroU64::new(2).unwrap()),
                original_end,
            ),
            _ => port.propose_exact(&mut f, PLAN, receipt, end(1, 2)),
        };
        port.reject(&mut f, proposal);
        assert!(f.core.facts().ready_cut().is_none());
        if variant == 0 {
            // A foreign proposal cannot consume the original complete result.
            port.propose(&mut f);
            f.finished_root();
            world_success_seal(&mut f, |token| port.observe_success_seal(token));
            let accepted = port.accept_ready(&mut f);
            assert_eq!(accepted.cut().end, original_end);
        } else {
            assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        }
        port.history().check();
        audit_consumer_effects(&f);
    }
}
#[test]
fn seeded_source_drop_waits_supervised_verdict_including_success_without_ready() {
    for seed in 30..33 {
        let (mut f, mut port) = scalar_world(seed, ScalarPayload::new(37));
        assert!(port.decode_complete_domain(&mut f));
        port.propose(&mut f);
        port.close_source(&mut f);
        f.finished_root();
        world_success_seal(&mut f, |token| port.observe_success_seal(token));
        assert!(f.core.facts().ready_cut().is_none());
        assert_eq!(f.core.facts().conclusion(), None);
        let verdict = query_failure(
            QueryFailureReason::InternalConsumerPanicked,
            "original modeled supervised consumer verdict",
        );
        let outcome = match seed % 3 {
            0 => Some(Err(verdict.clone())),
            1 => Some(Ok(())),
            _ => None,
        };
        port.report_exit(&mut f, outcome);
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert!(f.core.facts().ready_cut().is_none());
        if seed % 3 == 0 {
            assert_eq!(f.core.facts().failure(), Some(&verdict));
        }
        port.history().check();
        audit_consumer_effects(&f);
    }
}
