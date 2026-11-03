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

//! Primitive source custody tests borrow actual paid raw fields. The symbolic
//! host reads metadata only; it claims no M admission or Native success.

use super::*;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::AtomicBool,
};

#[test]
fn one_original_source_loan_excludes_disposal_and_restores_on_return_or_unwind() {
    for panic_in_host in [false, true] {
        let mut f = fixture();
        let (raw, getters, drops) = request(&f, None, None, false);
        let (input, external) = admit(&mut f);
        let job = f
            .root
            .begin_establish_ingress(input, raw, PayloadId(1))
            .unwrap();
        host(job);
        let projected = matches!(
            f.root.take_establish_ingress(input).unwrap(),
            Some(EstablishIngressOutcome::Projected(_))
        );
        let owner = f.root.resources().requests[&input]
            .ingress_owner
            .as_ref()
            .unwrap()
            .clone();
        let loan = owner.take_materialization().unwrap();
        let gate = Gate::new();
        let same_participation = Arc::new(AtomicBool::new(false));
        let participation = f.participation;
        let worker = {
            let gate = gate.clone();
            let same_participation = same_participation.clone();
            thread::spawn(move || {
                catch_unwind(AssertUnwindSafe(move || {
                    same_participation.store(
                        loan.shared_facts().participation() == participation,
                        Ordering::SeqCst,
                    );
                    gate.entered.wait();
                    gate.resume.wait();
                    assert!(!panic_in_host, "symbolic borrowed host invocation panic");
                    drop(loan);
                }))
                .is_err()
            })
        };
        gate.entered.wait();
        let direct = owner.take_disposal();
        let direct_excluded = direct.is_none();
        let withdrawal = f.root.begin_unsubmitted_withdrawal(input).unwrap();
        let withdrawal_excluded = withdrawal.is_none();
        drop(external);
        let retired_during_loan = f.root.reap_ready_requests(32).unwrap();
        let fee_during_loan = f.ledger.used(Dimension::Requests.id());
        let drops_during_loan = drops.load(Ordering::SeqCst);
        let source_exited_during_loan = owner.try_close().unwrap();
        gate.resume.wait();
        let host_panicked = worker.join().unwrap();

        let next_loan = owner.take_materialization();
        let second_loan_refused = next_loan.is_none();
        drop(next_loan);
        let source_exited_on_return = owner.try_close().unwrap();
        let fee_after_return = f.ledger.used(Dimension::Requests.id());
        let role_after_return = f.root.resources().requests[&input].exit_custody.phase();
        let disposal = direct
            .or(withdrawal)
            .or_else(|| f.root.begin_unsubmitted_withdrawal(input).unwrap())
            .expect("the same original raw fields after the host returned its loan");
        let disposed = thread::spawn(move || disposal.run()).join().unwrap();
        let retired = f.root.reap_ready_requests(32).unwrap();
        drop(owner);
        f.finish();

        assert!(projected);
        assert!(same_participation.load(Ordering::SeqCst));
        assert!(direct_excluded);
        assert!(withdrawal_excluded);
        assert_eq!(retired_during_loan, 0);
        assert_eq!(fee_during_loan, 1);
        assert_eq!(drops_during_loan, 0);
        assert!(!source_exited_during_loan);
        assert_eq!(host_panicked, panic_in_host);
        assert!(second_loan_refused);
        assert!(!source_exited_on_return);
        assert_eq!(fee_after_return, 1);
        assert_eq!(
            role_after_return,
            novarocks_control_runtime::exit_custody::ExitCustodyPhase::Started
        );
        assert!(disposed);
        assert_eq!(retired, 1);
        assert_eq!(getters.load(Ordering::SeqCst), 3);
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }
}

#[test]
fn an_unsubmitted_request_rejects_a_real_non_materialize_ticket_without_losing_its_source() {
    let mut f = fixture();
    let (raw, getters, drops) = request(&f, None, None, false);
    let (input, external) = admit(&mut f);
    let job = f
        .root
        .begin_establish_ingress(input, raw, PayloadId(1))
        .unwrap();
    host(job);
    let projected = matches!(
        f.root.take_establish_ingress(input).unwrap(),
        Some(EstablishIngressOutcome::Projected(_))
    );
    let owner = f.root.resources().requests[&input]
        .ingress_owner
        .as_ref()
        .unwrap()
        .clone();
    let mut bytes = [12; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    let other_operation = TaskOperationId::try_from_bytes(bytes).unwrap();
    let (other, mut other_external) = f
        .root
        .admit_request(
            ActorId::new(3).unwrap(),
            other_operation,
            MonotonicInstant::from_origin(Duration::from_secs(10)),
        )
        .unwrap();
    f.root
        .submit_request(
            other,
            ControlInput::Acquire {
                request: AcquireParticipationAdmissionTicket::new(
                    other_operation,
                    f.participation,
                    LeaseValidFor::new(Duration::from_secs(6)).unwrap(),
                    NativeCompatibilityId::new([3; 32]),
                    f.capability,
                ),
                ticket: AdmissionTicketId::try_from_bytes([4; 16]).unwrap(),
            },
        )
        .unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.root.index_control_effects(64).unwrap();
    let body = f.next_control_body();
    let refused = matches!(
        f.root.resources().requests[&input].take_materialization_source(&body),
        Err(ControlRequestError::WrongEffect)
    );
    let loan = owner.take_materialization().unwrap();
    let source_unchanged = loan.shared_facts().participation() == f.participation;
    drop(loan);
    let reference = crate::control_runtime::effect_dispatch::EffectRef {
        input: body.input,
        action: body.descriptor.id,
    };
    f.complete_body(body);
    f.root.control_completion_observed(reference).unwrap();
    let reply = f.next_control_body();
    f.root.publish_request_reply(reply).unwrap();
    let delivery = other_external.try_take().unwrap();
    drop(delivery);
    drop(other_external);
    let other_retired = f.root.reap_ready_requests(32).unwrap();
    drop(external);
    let disposal = f.root.begin_unsubmitted_withdrawal(input).unwrap().unwrap();
    let disposed = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.root.reap_ready_requests(32).unwrap();
    drop(owner);
    f.finish();

    assert!(projected);
    assert!(refused);
    assert!(source_unchanged);
    assert_eq!(other_retired, 1);
    assert!(disposed);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}
