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

//! The approved Node retention property, using the existing real Node fixture.

use super::*;

fn retire_empty_context(fixture: &Fixture, ordinal: i64) {
    let context = QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(32200, ordinal), AttemptId::new(1).unwrap()).unwrap(),
        fixture.context.frontend_process_id(),
        fixture.context.backend_process_id(),
    );
    let ticket = AdmissionTicketId::try_from_bytes(TaskOperationId::new_v7().to_bytes()).unwrap();
    let lease = LeaseValidFor::new(Duration::from_secs(10)).unwrap();
    let acquired = await_original(fixture.control.acquire(
        AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            lease,
            NativeCompatibilityId::new([0x71; 32]),
            fixture.control.admission_epoch_capability(),
        ),
        ticket,
        deadline(&fixture.control),
    ))
    .unwrap();
    assert!(matches!(
        acquired.body(),
        WorkerControlReplyBody::Admission(Ok(_))
    ));
    drop(acquired);
    let established = await_original(fixture.control.establish(
        EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            ticket,
            Arc::new(Content),
            Arc::new(Content),
            Arc::new(Content),
            CredentialUpdate::new(
                CredentialLeaseId::new(1),
                CredentialEpoch::FIRST,
                Arc::new(Secret),
            ),
            lease,
        ),
        deadline(&fixture.control),
    ))
    .unwrap();
    assert!(
        matches!(established.body(), WorkerControlReplyBody::Context(ack)
        if ack.outcome == OperationOutcome::Accepted)
    );
    drop(established);
    release_empty_context(fixture, context);
}

fn release_empty_context(fixture: &Fixture, context: QueryContextRef) {
    let quiesced = await_original(fixture.control.quiesce(
        QuiesceQueryContext::new(TaskOperationId::new_v7(), context),
        deadline(&fixture.control),
    ))
    .unwrap();
    assert!(matches!(
        quiesced.body(),
        WorkerContextReplyBody::Quiesce {
            outcome: OperationOutcome::Accepted,
            ..
        }
    ));
    drop(quiesced);
    let released = await_original(fixture.control.release(
        ReleaseQueryContext::new(TaskOperationId::new_v7(), context),
        deadline(&fixture.control),
    ))
    .unwrap();
    assert!(matches!(
        released.body(),
        WorkerContextReplyBody::Release {
            state: QueryContextState::TerminalRetained,
            ..
        }
    ));
    drop(released);
    // Serial caller admission waits for the actual preceding request retirement,
    // so this property exercises cold records rather than request saturation.
    wait_until(|| fixture.control.metrics_snapshot().requests == 0);
}

#[test]
fn three_thousand_compact_contexts_expire_and_cold_shutdown_does_not_wait_for_the_horizon() {
    // No Task/Root is installed. All context materialization, request custody,
    // release, compaction, timer delivery and forget jobs use the actual Node.
    let fixture = Fixture::uninstalled(false, None);
    let original_now = fixture.control.now();
    release_empty_context(&fixture, fixture.context);
    for ordinal in 1..=3_000 {
        retire_empty_context(&fixture, ordinal);
    }
    wait_until(|| {
        fixture
            .control
            .try_protocol_retention_snapshot()
            .is_some_and(|snapshot| snapshot.contexts.compact_contexts == 3_001)
    });
    assert_eq!(fixture.control.now(), original_now);
    assert_eq!(fixture.control.metrics_snapshot().contexts, 3_001);
    // Include the original fixture context in the same compact population;
    // none of these records may retain execution custody or task slots.
    wait_until(|| {
        fixture
            .control
            .try_execution_quiescence_snapshot()
            .is_some_and(|snapshot| {
                snapshot.contexts_with_execution_custody == 0 && snapshot.requests == 0
            })
    });
    assert_eq!(fixture.control.metrics_snapshot().active_tasks, 0);
    assert_eq!(fixture.control.metrics_snapshot().preparing_tasks, 0);
    fixture
        .clock
        .advance(RequestHorizon::DEFAULT.total() + Duration::from_millis(1));
    fixture
        .node
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .notify_clock_changed();
    wait_until(|| fixture.control.metrics_snapshot().contexts == 0);
    wait_until(|| {
        fixture
            .control
            .try_protocol_retention_snapshot()
            .is_some_and(|snapshot| {
                snapshot.contexts.compact_contexts == 0 && snapshot.admission.ticket_records == 0
            })
    });
    // A fresh cold population must drain without advancing this same clock.
    for ordinal in 3_001..=3_032 {
        retire_empty_context(&fixture, ordinal);
    }
    wait_until(|| {
        fixture
            .control
            .try_protocol_retention_snapshot()
            .is_some_and(|snapshot| snapshot.contexts.compact_contexts == 32)
    });
    let control = fixture.control.clone();
    let before_shutdown = control.now();
    drop(fixture); // The existing fixture joins the original Node and pools.
    assert_eq!(control.now(), before_shutdown);
    assert_eq!(control.metrics_snapshot().contexts, 0);
}
