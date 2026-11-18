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

use super::*;
use crate::support::*;

#[test]
fn seeded_result_receipts_keep_success_end_and_original_read_close_in_order() {
    for seed in 0..12 {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.budget = 3;
        f.begin();
        f.finished_root();
        let mut port = ResultPort::attach(&mut f, seed);
        port.reserve_packet(1, 3, Disposition::Completed);
        let first = port.visibility(&mut f);
        assert!(!f.effects.iter().any(|e| matches!(&e.body,
            LogicalEffectBody::Relay(RelayEffect::Reply { token, response: RelayResponse::VisibilityGranted, .. }) if *token == first)));
        port.settle_all(&mut f);
        port.accept_visibility(&mut f, first);
        port.deliver_packet();
        port.settle_all(&mut f);
        // Later packets retain their own original slots under the one grant.
        port.reserve_packet(2, 5, Disposition::Completed);
        port.deliver_packet();
        port.settle_all(&mut f);
        let original_end = end(2, 8);
        let request = port.consume_end(&mut f, original_end);
        world_success_seal(&mut f, |token| port.observe_success_seal(token));
        let slot = port.reserve_end_slot(&mut f);
        assert_eq!(f.core.facts().conclusion(), None);
        port.end_slot_ready(&mut f, slot);
        let outcome = match seed % 3 {
            0 => Disposition::Completed,
            1 => Disposition::Failed,
            _ => Disposition::Dropped,
        };
        port.enqueue_end(&mut f, request, outcome);
        // LP has promised logical success, but the caller has not yet consumed
        // the queued End or won the independent statement-success latch.
        if seed % 2 == 0 {
            port.cancel(&mut f);
        }
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::Succeeded)
        );
        port.relay_end_handoff(&mut f, original_end);
        let read_close = f.take(|b| {
            matches!(
                b,
                LogicalEffectBody::Native(NativeEffect::SealRootRead { .. })
            )
        });
        assert!(!f.effects.iter().any(|e| matches!(&e.body,
            LogicalEffectBody::AttemptPort(p) if matches!(p.body, LogicalToAttemptBody::BeginConvergence { .. }))));
        f.ack(read_close.action, ActionResult::Native(NativeFact::Applied));
        assert!(f.effects.iter().any(|e| matches!(&e.body,
            LogicalEffectBody::AttemptPort(p) if matches!(p.body, LogicalToAttemptBody::BeginConvergence { .. }))));
        port.settle_all(&mut f);
        if seed % 2 == 1 {
            port.cancel(&mut f);
        }
        assert_eq!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::Succeeded)
        );
        let written = port
            .history()
            .0
            .iter()
            .any(|e| matches!(e, Event::ClientEndWritten));
        assert_eq!(written, outcome == Disposition::Completed && seed % 2 == 1);
        port.history().check();
        audit_consumer_effects(&f);
    }
}

#[test]
fn seeded_consumer_close_and_scope_cancel_cannot_write_an_unsealed_end() {
    for seed in 0..8 {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.budget = 2;
        f.begin();
        f.finished_root();
        let mut port = ResultPort::attach(&mut f, seed);
        port.settle_all(&mut f);
        let original_end = end(0, 0);
        port.consume_end(&mut f, original_end);
        world_success_seal(&mut f, |token| port.observe_success_seal(token));
        let slot = port.reserve_end_slot(&mut f);
        port.close_consumer(&mut f);
        assert_eq!(f.core.facts().conclusion(), None);
        port.cancel(&mut f);
        port.end_slot_ready(&mut f, slot);
        assert_ne!(
            f.core.facts().conclusion(),
            Some(LogicalConclusion::Succeeded)
        );
        assert!(!f.effects.iter().any(|e| matches!(
            e.body,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )));
        port.stopped(&mut f);
        port.history().check();
        audit_consumer_effects(&f);
        assert!(
            !port
                .history()
                .0
                .iter()
                .any(|event| matches!(event, Event::EndQueued { .. } | Event::ClientEndWritten))
        );
    }
}

#[test]
fn seeded_schema_failure_or_drop_never_grants_visibility_or_writes_end() {
    for (seed, outcome) in [(17, Disposition::Failed), (18, Disposition::Dropped)] {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.begin();
        f.finished_root();
        let mut port = ResultPort::with_schema(&mut f, seed, outcome);
        port.settle_all(&mut f);
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert!(!f.core.facts().output_visible());
        assert_eq!(
            f.core.facts().failure(),
            Some(&novarocks_logical_execution_protocol::failure::result_delivery_failed())
        );
        assert!(!f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )));
        port.stopped(&mut f);
        port.history().check();
        audit_consumer_effects(&f);
        assert!(!port.history().0.iter().any(|e| matches!(
            e,
            Event::Visible { .. } | Event::EndQueued { .. } | Event::ClientEndWritten
        )));
    }
}

#[test]
fn seeded_body_failure_or_drop_uses_original_delivery_fact_and_settles_each_receipt() {
    for seed in 20..28 {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.begin();
        f.finished_root();
        let mut port = ResultPort::attach(&mut f, seed);
        port.settle_all(&mut f);
        port.reserve_packet(1, 2, Disposition::Completed);
        let permission = port.visibility(&mut f);
        port.accept_visibility(&mut f, permission);
        port.deliver_packet();
        port.settle_all(&mut f);
        port.reserve_packet(
            2,
            3,
            if seed % 2 == 0 {
                Disposition::Failed
            } else {
                Disposition::Dropped
            },
        );
        port.deliver_packet();
        port.settle_all(&mut f);
        assert_eq!(f.core.facts().conclusion(), Some(LogicalConclusion::Failed));
        assert_eq!(
            f.core.facts().failure(),
            Some(&novarocks_logical_execution_protocol::failure::result_delivery_failed())
        );
        assert!(!f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )));
        port.stopped(&mut f);
        port.history().check();
        audit_consumer_effects(&f);
        assert!(
            !port
                .history()
                .0
                .iter()
                .any(|e| matches!(e, Event::EndQueued { .. } | Event::ClientEndWritten))
        );
    }
}

#[test]
fn seeded_close_with_pending_body_retains_the_scope_first_winner() {
    for seed in 30..34 {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.begin();
        f.finished_root();
        let mut port = ResultPort::attach(&mut f, seed);
        port.settle_all(&mut f);
        port.reserve_packet(1, 4, Disposition::Completed);
        let permission = port.visibility(&mut f);
        port.accept_visibility(&mut f, permission);
        port.deliver_packet();
        port.close_consumer(&mut f);
        assert_eq!(f.core.facts().conclusion(), None);
        port.cancel(&mut f);
        let first = f.core.facts().failure().cloned();
        let conclusion = f.core.facts().conclusion();
        port.settle_all(&mut f);
        port.stopped(&mut f);
        assert_eq!(f.core.facts().failure(), first.as_ref());
        assert_eq!(f.core.facts().conclusion(), conclusion);
        port.history().check();
        audit_consumer_effects(&f);
    }
}

#[test]
fn seeded_close_while_schema_pending_keeps_first_slot_until_the_original_stop() {
    for seed in 40..44 {
        let mut f = Fixture::new(LogicalOutputMode::ResultStream);
        f.begin();
        f.finished_root();
        let mut port = ResultPort::attach(&mut f, seed);
        port.reserve_packet(1, 7, Disposition::Completed);
        let pending = port.visibility(&mut f);
        assert!(!f.core.facts().output_visible());
        port.close_consumer(&mut f);
        port.cancel(&mut f);
        let first = f.core.facts().failure().cloned();
        port.reject_visibility(&mut f, pending);
        port.settle_all(&mut f);
        assert!(!f.effects.iter().any(|effect| matches!(
            effect.body,
            LogicalEffectBody::Consumer(ConsumerEffect::EnqueueEnd { .. })
        )));
        port.stopped(&mut f);
        assert_eq!(f.core.facts().failure(), first.as_ref());
        assert!(!f.core.facts().output_visible());
        port.history().check();
        audit_consumer_effects(&f);
    }
}
