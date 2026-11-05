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

//! Deferred target registration retains the original paid R and never runs M.

use super::*;

// Two actual actors can be ready after the first backchannel fill. Observe
// their real queues instead of assuming the next scheduler claim is Participation.
fn probe_in_handoff_order(f: &mut RouteFixture, input: InputId) -> EstablishReplayResult {
    let runtime = f.f.runtime.clone();
    for claimed in 0..=2 {
        let host_address = f.f.root.service().participation(f.f.participation).unwrap();
        let mut host = host_address.lock().unwrap();
        host.dispatch.index_committed(&runtime, 64).unwrap();
        if let Some(probe) = host.dispatch.take_ready(&runtime, 64).unwrap() {
            let result = match &probe.effect {
                WorkerAction::Participation(ParticipationEffect::EstablishProbed {
                    request,
                    result,
                    ..
                }) if request.token == input => result.clone(),
                _ => panic!("the next actual Participation probe follows route handoff order"),
            };
            drop(host);
            f.assert_original_probe_author(&probe, input);
            host_address
                .lock()
                .unwrap()
                .handoff_establish_probe(probe)
                .unwrap();
            return result;
        }
        if claimed < 2 {
            runtime.poll_one(Tick(3)).unwrap();
        }
    }
    panic!("the two registered actors must expose the original queued probe");
}

fn finish_conflict_return(
    f: &mut RouteFixture,
    input: InputId,
    mut handle: ControlRequestHandle,
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
) {
    f.f.runtime.poll_one(Tick(4)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(
        &reply.effect,
        WorkerAction::ControlReply(reply)
            if reply.request.token == input
                && reply.result == RequestResult::Outcome(OperationOutcome::ParticipationConflict)
    ));
    let disposal =
        f.f.root
            .begin_rejected_ingress_disposal(&reply)
            .unwrap()
            .unwrap();
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(thread::spawn(move || disposal.run()).join().unwrap());
    f.finish_conflict_observation(input);
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_deferred_route_releases_its_root_input_and_binds_only_after_real_participation_registration() {
    let mut f = RouteFixture::with_geometry(2, StorageNeed::default(), true);
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 100, None);
    assert!(f.f.runtime.snapshot(participation_actor()).is_err());
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(f.f.runtime.input_custody(actor(), input).unwrap().is_none());
    assert!(f.f.root.resources().requests.contains_key(&input));
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 2);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(handle.try_take().is_none());
    let pending_work = f.f.root.resources().scope.snapshot().unwrap().work;
    assert_eq!(advance_route_bindings(&mut f.f.root, 32).unwrap(), 0);
    assert_eq!(
        f.f.root.resources().scope.snapshot().unwrap().work,
        pending_work
    );
    assert!(f.f.root.take_control_effect(64).unwrap().is_none());

    f.adopt_pending_participation();
    assert_eq!(advance_route_bindings(&mut f.f.root, 0).unwrap(), 0);
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .resources_for_test()
            .routes
            .is_empty()
    );
    let address = f.f.root.service().participation(f.f.participation).unwrap();
    let execution_scope = address
        .lock()
        .unwrap()
        .resources_for_test()
        .scope
        .as_ref()
        .unwrap()
        .clone();
    let execution_before = execution_scope.snapshot().unwrap();
    let financial_before = f.f.root.resources().scope.snapshot().unwrap();
    let original_r = f.f.root.resources().requests[&input].request();
    assert!(f.f.root.resources().requests[&input].route.is_none());
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 1);
    assert_eq!(execution_scope.snapshot().unwrap(), execution_before);
    let financial_after = f.f.root.resources().scope.snapshot().unwrap();
    assert_eq!(financial_after.work, financial_before.work + 1);
    assert_eq!(financial_after.storage, financial_before.storage + 1);
    assert_eq!(f.f.root.resources().requests[&input].request(), original_r);
    let route = f.f.root.resources().requests[&input]
        .route
        .as_ref()
        .unwrap();
    let return_input_id = route
        .return_input_id()
        .expect("the actual admitted original Root backchannel");
    assert!(
        f.f.runtime
            .standing_input_inventory(actor(), None, 16)
            .unwrap()
            .iter()
            .any(|record| record.standing_input_id == return_input_id
                && record.in_flight.is_none()
                && record.source == participation_actor()
                && record.class == InputClass::Control)
    );
    {
        let original_address = f.f.root.service().participation(f.f.participation).unwrap();
        let mut original_participation = original_address.lock().unwrap();
        let routes = &original_participation.resources_for_test().routes;
        assert_eq!(routes.len(), 1);
        assert!(routes.contains_key(&input));
    }
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 0);
    assert!(matches!(
        f.probe(input),
        EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict
    ));
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(input)
            .unwrap()
    );
    finish_conflict_return(&mut f, input, handle, getters, drops);
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 1);
    f.finish_model_refusal();
}

#[test]
fn cancelling_a_never_registered_target_clears_only_that_original_route_address() {
    let mut f = RouteFixture::with_geometry(2, StorageNeed::default(), true);
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 10, None);
    assert!(f.f.runtime.snapshot(participation_actor()).is_err());
    // Keep any erroneously returned job until ordinary cancellation permits
    // actual cleanup; a negative assertion must not abandon opaque ownership.
    let early_disposal = f.f.root.take_retired_route_disposal(input);
    let refused_before_cancel = early_disposal.is_none();
    assert_eq!(f.f.runtime.advance_time(Tick(10), 2).unwrap(), 1);
    f.f.runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    assert!(matches!(
        &cancel.effect,
        WorkerAction::Control { effect: ControlEffect::CancelEstablishRoute { request, .. }, .. }
            if request.token == input
    ));
    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let disposal = early_disposal
        .or_else(|| f.f.root.take_retired_route_disposal(input))
        .expect("the cancelled original seed never issued a Participation Work");
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(
        &reply.effect,
        WorkerAction::ControlReply(reply)
            if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted
    ));
    f.f.root.publish_request_reply(reply).unwrap();
    let delivery = handle.try_take().unwrap();
    drop(delivery);
    drop(handle);
    f.f.runtime.retire_input(actor(), timer_input).unwrap();
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(f.f.root.resources().requests.contains_key(&input));
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(f.f.runtime.snapshot(participation_actor()).is_err());
    assert!(thread::spawn(move || disposal.run()).join().unwrap());
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 1);
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert!(!f.f.root.resources().requests.contains_key(&input));
    assert!(f.f.root.take_retired_route_disposal(input).is_none());

    f.adopt_pending_participation();
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 0);
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .resources_for_test()
            .routes
            .is_empty()
    );
    // There is no Participation probe to consume, and no cancelled address is revived.
    let runtime = f.f.runtime.clone();
    f.f.root
        .service()
        .participation(f.f.participation)
        .unwrap()
        .lock()
        .unwrap()
        .dispatch
        .index_committed(&runtime, 64)
        .unwrap();
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .dispatch
            .take_ready(&runtime, 64)
            .unwrap()
            .is_none()
    );
    f.finish_model_refusal();
    assert!(refused_before_cancel);
}

#[test]
fn deferred_routes_bind_in_actual_handoff_order_instead_of_root_token_order() {
    let mut f = RouteFixture::with_geometry(3, StorageNeed::default(), true);
    let (earlier_token, earlier_handle) =
        f.f.root
            .admit_establish_request(ActorId::new(2).unwrap(), op(12), at(100))
            .unwrap();
    let earlier_getters = Arc::new(AtomicUsize::new(0));
    let earlier_drops = Arc::new(AtomicUsize::new(0));
    let content = || {
        Arc::new(Content {
            getters: earlier_getters.clone(),
            drops: earlier_drops.clone(),
            gate: None,
        }) as Arc<dyn CodecOwnedContent>
    };
    let raw = EstablishParticipation::new(
        op(12),
        f.f.participation,
        f.ticket,
        content(),
        content(),
        content(),
        CredentialUpdate::new(
            CredentialDomainId::new(1),
            CredentialEpoch::FIRST,
            Arc::new(Credential(earlier_drops.clone())),
        ),
        LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
    );
    let earlier_job =
        f.f.root
            .begin_establish_ingress(earlier_token, raw, PayloadId(201))
            .unwrap();
    // The earlier token has captured its actual raw request but has not run.
    let (later_token, later_handle, later_getters, later_drops) = f.begin_route(op(13), 100, None);
    assert!(later_token > earlier_token);
    assert_eq!(earlier_getters.load(Ordering::SeqCst), 0);
    assert!(f.f.runtime.snapshot(participation_actor()).is_err());

    thread::spawn(move || earlier_job.run()).join().unwrap();
    let projection = match f
        .f
        .root
        .take_establish_ingress(earlier_token)
        .unwrap()
        .unwrap()
    {
        EstablishIngressOutcome::Projected(projection) => projection,
        _ => panic!("the earlier actual getter projection"),
    };
    f.f.root
        .submit_request(
            earlier_token,
            ControlInput::Establish {
                request: projection.operation,
                participation: projection.participation,
                ticket: projection.ticket,
                payload: projection.payload,
                semantics: projection.semantics,
                generation: generation(),
            },
        )
        .unwrap();
    f.f.runtime.poll_one(Tick(2)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let route = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(
        &route.effect,
        WorkerAction::Control { effect: ControlEffect::RouteEstablish { request, .. }, .. }
            if request.request.token == earlier_token
    ));
    f.f.root.handoff_establish_route(route).unwrap();
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    assert!(
        f.f.runtime
            .input_custody(actor(), later_token)
            .unwrap()
            .is_none()
    );
    assert!(
        f.f.runtime
            .input_custody(actor(), earlier_token)
            .unwrap()
            .is_none()
    );
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 3);

    f.adopt_pending_participation();
    assert_eq!(advance_route_bindings(&mut f.f.root, 0).unwrap(), 0);
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .resources_for_test()
            .routes
            .is_empty()
    );
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 1);
    {
        let original_address = f.f.root.service().participation(f.f.participation).unwrap();
        let mut original_participation = original_address.lock().unwrap();
        let routes = &original_participation.resources_for_test().routes;
        assert_eq!(routes.len(), 1);
        assert!(routes.contains_key(&later_token));
        assert!(!routes.contains_key(&earlier_token));
    }
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 1);
    {
        let original_address = f.f.root.service().participation(f.f.participation).unwrap();
        let mut original_participation = original_address.lock().unwrap();
        let routes = &original_participation.resources_for_test().routes;
        assert_eq!(routes.len(), 2);
        assert!(routes.contains_key(&later_token));
        assert!(routes.contains_key(&earlier_token));
    }
    assert_eq!(advance_route_bindings(&mut f.f.root, 1).unwrap(), 0);
    assert!(
        matches!(probe_in_handoff_order(&mut f, later_token), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
    );
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(later_token)
            .unwrap()
    );
    assert!(
        matches!(probe_in_handoff_order(&mut f, earlier_token), EstablishReplayResult::Decided(ack) if ack.outcome == OperationOutcome::ParticipationConflict)
    );
    assert!(
        f.f.root
            .service()
            .participation(f.f.participation)
            .unwrap()
            .lock()
            .unwrap()
            .advance_establish_route(earlier_token)
            .unwrap()
    );
    finish_conflict_return(
        &mut f,
        later_token,
        later_handle,
        later_getters,
        later_drops,
    );
    finish_conflict_return(
        &mut f,
        earlier_token,
        earlier_handle,
        earlier_getters,
        earlier_drops,
    );
    assert_eq!(f.f.ledger.used(Dimension::Requests.id()), 1);
    f.finish_model_refusal();
}

#[test]
fn an_original_timeout_retires_an_unbound_transfer_and_late_bind_cannot_admit_participation_work() {
    use crate::control_runtime::establish_route::EstablishRouteError;
    let mut f = RouteFixture::with_geometry(2, StorageNeed::default(), true);
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 10, None);
    f.adopt_pending_participation();
    let prepared = f.f.root.take_ready_route_binding().unwrap().unwrap();
    let address = f.f.root.service().participation(f.f.participation).unwrap();
    let scope = address
        .lock()
        .unwrap()
        .resources_for_test()
        .scope
        .as_ref()
        .unwrap()
        .clone();
    let before = scope.snapshot().unwrap();
    assert_eq!(f.f.runtime.advance_time(Tick(10), 2).unwrap(), 1);
    f.f.runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    assert!(matches!(&cancel.effect,
        WorkerAction::Control { effect: ControlEffect::CancelEstablishRoute { request, .. }, .. }
            if request.token == input));
    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let disposal =
        f.f.root
            .take_retired_route_disposal(input)
            .expect("the original never-bound Participation leg has genuinely retired");
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(&reply.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted));
    f.f.root.publish_request_reply(reply).unwrap();
    drop(handle.try_take().unwrap());
    drop(handle);
    f.f.runtime.retire_input(actor(), timer_input).unwrap();
    let still_original = f.f.root.resources().requests.contains_key(&input);
    let late = prepared.bind();
    let no_participation_work = scope.snapshot().unwrap() == before;
    // register_establish_route stores the original alias before attempting
    // its issuer. Even this refusal is owned and really removed by Participation.
    let removed_original_alias = address
        .lock()
        .unwrap()
        .advance_establish_route(input)
        .unwrap();
    assert_eq!(f.f.root.reap_ready_requests(32).unwrap(), 0);
    let disposed = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.f.root.reap_ready_requests(32).unwrap();
    f.finish_model_refusal();
    assert!(matches!(
        late,
        Err(ControlRequestError::Route(EstablishRouteError::WrongPhase))
    ));
    assert!(still_original && no_participation_work && removed_original_alias && disposed);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn a_prepared_transfer_to_an_actually_removed_participation_parks_without_work_until_its_original_timeout()
 {
    use crate::control_runtime::establish_route::EstablishRouteError;
    let mut f = RouteFixture::with_geometry(2, StorageNeed::default(), true);
    let (input, mut handle, getters, drops) = f.begin_route(op(12), 10, None);
    f.adopt_pending_participation();
    let prepared = f.f.root.take_ready_route_binding().unwrap().unwrap();
    let original = f.f.root.resources().requests.get(&input).unwrap();
    let return_input_id = original.route_return_input_id().unwrap();
    let original_work = original.work;
    let address = f.f.root.service().participation(f.f.participation).unwrap();
    // This actually closes the sources/actor, exits the metadata Work and
    // removes its Scope. The captured address is only a metadata alias.
    f.dispose_participation_metadata();
    let actor_removed = matches!(
        f.f.runtime.snapshot(participation_actor()),
        Err(ActorTurnError::UnknownActor)
    );
    let dimensions = [Dimension::Requests];
    let fees_after_removal = dimensions.map(|dimension| f.f.ledger.used(dimension.id()));
    let owner_after_removal = f.f.root.resources().scope.snapshot().unwrap();
    let late = prepared.bind();
    let no_new_payment =
        dimensions.map(|dimension| f.f.ledger.used(dimension.id())) == fees_after_removal && {
            let actual = f.f.root.resources().scope.snapshot().unwrap();
            actual.work == owner_after_removal.work && actual.storage == owner_after_removal.storage
        };
    let no_alias = address
        .lock()
        .unwrap()
        .resources_for_test()
        .routes
        .is_empty();
    let original = f.f.root.resources().requests.get(&input).unwrap();
    let same_return_and_original_r =
        original.work == original_work && original.route_return_input_id() == Some(return_input_id);
    let early_disposal = f.f.root.take_retired_route_disposal(input);
    let no_premature_disposal = early_disposal.is_none();
    let still_pending = handle.try_take().is_none();
    let no_premature_refund = f.f.root.reap_ready_requests(32).unwrap() == 0;

    // Target loss is not a reply or an exit fact. Only this same request's
    // real Core deadline can authorize its unbound-participation cancellation.
    assert_eq!(f.f.runtime.advance_time(Tick(10), 2).unwrap(), 1);
    f.f.runtime.poll_one(Tick(10)).unwrap();
    f.f.root.index_control_effects(64).unwrap();
    let cancel = f.f.root.take_control_effect(64).unwrap().unwrap();
    let timer_input = cancel.input;
    assert!(matches!(&cancel.effect,
        WorkerAction::Control { effect: ControlEffect::CancelEstablishRoute { request, .. }, .. }
            if request.token == input));
    f.f.root.handoff_establish_route_cancel(cancel).unwrap();
    let disposal = early_disposal
        .or_else(|| f.f.root.take_retired_route_disposal(input))
        .expect("real cancellation positively retires only the original unbound Participation leg");
    let reply = f.f.root.take_control_effect(64).unwrap().unwrap();
    assert!(matches!(&reply.effect, WorkerAction::ControlReply(reply)
        if reply.request.token == input && reply.result == RequestResult::TimeoutWhileAccepted));
    f.f.root.publish_request_reply(reply).unwrap();
    drop(handle.try_take().unwrap());
    drop(handle);
    f.f.runtime.retire_input(actor(), timer_input).unwrap();
    let raw_still_owned = f.f.root.reap_ready_requests(32).unwrap() == 0
        && f.f.root.resources().requests.contains_key(&input)
        && drops.load(Ordering::SeqCst) == 0;
    let disposed = thread::spawn(move || disposal.run()).join().unwrap();
    let retired = f.f.root.reap_ready_requests(32).unwrap();
    let exact_r_removed = !f.f.root.resources().requests.contains_key(&input);
    f.finish_model_refusal(); // Reuses the already-retired Context metadata.

    assert!(matches!(
        late,
        Err(ControlRequestError::Route(
            EstablishRouteError::TargetRemoved
        ))
    ));
    assert!(actor_removed && no_alias && no_new_payment);
    assert!(
        same_return_and_original_r && no_premature_disposal && still_pending && no_premature_refund
    );
    assert!(raw_still_owned && disposed && exact_r_removed);
    assert_eq!(retired, 1);
    assert_eq!(getters.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}
