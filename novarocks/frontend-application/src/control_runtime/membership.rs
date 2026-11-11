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

//! One process service observes the original topology port. Its original Join
//! belongs to FrontendControlRuntime; it is not a per-attempt query Work.
//! The delivery gate closes before attempt drain. Already-filled standing
//! inputs still require their actual retirement and close_after_exit.

use super::actor::{FrontendActor, FrontendInput};
use crate::native::data_runtime::FrontendDataRuntime;
use novarocks_attempt_protocol::input::{MembershipObservation, MembershipView};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorTurnError},
    coalescing_input::{CoalescingInput, Merge},
};
use novarocks_control_runtime_core::identity::ScopeId;
use novarocks_execution_contract::{ParticipationRef, RuntimeEndpoint};
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};
use tokio::sync::{Notify, oneshot};

#[derive(Clone, Debug)]
pub(crate) struct MembershipUpdate(pub(crate) MembershipView);
impl Merge for MembershipUpdate {
    fn merge(&mut self, newer: Self) {
        if newer.0.revision >= self.0.revision {
            *self = newer;
        }
    }
    fn unit() -> Self {
        Self(MembershipView {
            revision: 0,
            processes: BTreeMap::new(),
        })
    }
}
pub(crate) fn membership_input(update: &MembershipUpdate) -> FrontendInput {
    FrontendInput::Membership(update.clone())
}
#[derive(Clone)]
pub(crate) struct FrozenMember {
    pub(crate) participation: ParticipationRef,
    pub(crate) endpoint: RuntimeEndpoint,
}
pub(crate) struct MembershipTarget {
    pub(crate) registration: ActorRegistration,
    pub(crate) members: Arc<[FrozenMember]>,
    pub(crate) input: CoalescingInput<FrontendActor, MembershipUpdate>,
    // Full original failures remain host-owned. They are never converted into
    // positive Current/Replacement evidence or reconstructed from error text.
    errors: Mutex<Vec<(ParticipationRef, BackendTopologyError)>>,
}
impl MembershipTarget {
    pub(crate) fn new(
        registration: ActorRegistration,
        members: Arc<[FrozenMember]>,
        input: CoalescingInput<FrontendActor, MembershipUpdate>,
    ) -> Self {
        Self {
            registration,
            members,
            input,
            errors: Mutex::new(Vec::new()),
        }
    }
    pub(crate) fn take_observation_errors(&self) -> Vec<(ParticipationRef, BackendTopologyError)> {
        std::mem::take(&mut *self.errors.lock().expect("frontend membership errors lock"))
    }
    pub(crate) fn after_retire(&self) -> Result<(), ActorTurnError> {
        self.input.after_retire().map(|_| ())
    }
    pub(crate) fn close_after_exit(&self) -> Result<(), ActorTurnError> {
        self.input.close_after_exit()
    }
}
pub(crate) struct MembershipRegistry {
    delivery_open: RwLock<bool>,
    targets: Mutex<BTreeMap<ScopeId, Arc<MembershipTarget>>>,
    changed: Notify,
}
impl MembershipRegistry {
    pub(crate) fn new() -> Self {
        Self {
            delivery_open: RwLock::new(true),
            targets: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
        }
    }
    pub(crate) fn insert(
        &self,
        target: Arc<MembershipTarget>,
    ) -> Result<(), Arc<MembershipTarget>> {
        let open = self
            .delivery_open
            .read()
            .expect("frontend membership delivery gate");
        if !*open {
            return Err(target);
        }
        let mut targets = self
            .targets
            .lock()
            .expect("frontend membership targets lock");
        if targets.contains_key(&target.registration.scope_id()) {
            return Err(target);
        }
        targets.insert(target.registration.scope_id(), target);
        drop(targets);
        drop(open);
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) fn remove(&self, registration: &ActorRegistration) -> Option<Arc<MembershipTarget>> {
        let _delivery = self
            .delivery_open
            .write()
            .expect("frontend membership delivery gate");
        let mut targets = self
            .targets
            .lock()
            .expect("frontend membership targets lock");
        if targets
            .get(&registration.scope_id())
            .is_some_and(|target| target.registration.same_registration(registration))
        {
            targets.remove(&registration.scope_id())
        } else {
            None
        }
    }
    pub(crate) fn close_delivery(&self) {
        let mut open = self
            .delivery_open
            .write()
            .expect("frontend membership delivery gate");
        *open = false;
        let targets = self
            .targets
            .lock()
            .expect("frontend membership targets lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for target in targets {
            target.input.close();
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.targets
            .lock()
            .expect("frontend membership targets lock")
            .is_empty()
    }
    fn publish(&self, revision: u64, port: &dyn BackendProcessObservationPort) {
        // Clone only frozen neutral facts and the exact registration witness,
        // never an input/Source holder across an external observer call. A slow
        // service observation may outlive query Scope drain without pinning a
        // standing input role that has already stopped accepting deliveries.
        let observations = self
            .targets
            .lock()
            .expect("frontend membership targets lock")
            .values()
            .map(|target| (target.registration.clone(), target.members.clone()))
            .collect::<Vec<_>>();
        for (registration, members) in observations {
            let mut processes = BTreeMap::new();
            let mut errors = Vec::new();
            for frozen in members.iter() {
                let observation = match port.observe_process_at_endpoint(
                    frozen.participation.backend_process_id(),
                    &frozen.endpoint,
                ) {
                    Ok(BackendProcessObservation::Current) => MembershipObservation::ExactOriginal,
                    Ok(BackendProcessObservation::Replaced { current_process }) => {
                        MembershipObservation::ExactReplacement(current_process)
                    }
                    Ok(BackendProcessObservation::Unobservable) => {
                        MembershipObservation::Unobserved
                    }
                    Err(error) => {
                        errors.push((frozen.participation, error));
                        MembershipObservation::Unobserved
                    }
                };
                processes.insert(frozen.participation, observation);
            }
            let open = self
                .delivery_open
                .read()
                .expect("frontend membership delivery gate");
            if !*open {
                return;
            }
            let target = self
                .targets
                .lock()
                .expect("frontend membership targets lock")
                .get(&registration.scope_id())
                .filter(|target| target.registration.same_registration(&registration))
                .cloned();
            let Some(target) = target else {
                continue;
            };
            *target
                .errors
                .lock()
                .expect("frontend membership errors lock") = errors;
            // Both this original input holder and its fill are inside the read
            // gate. The holder drops before close obtains the write barrier.
            match target.input.notify(MembershipUpdate(MembershipView {
                revision,
                processes,
            })) {
                Ok(_) | Err(ActorTurnError::Closed | ActorTurnError::ActorFailed) => {}
                Err(error) => panic!("frontend prepaid membership input failed: {error:?}"),
            }
            drop(target);
            drop(open);
        }
    }
}
#[derive(Debug)]
pub(crate) enum MembershipShutdownError {
    Pending,
    SourceClosed,
    Join(tokio::task::JoinError),
}
impl std::fmt::Display for MembershipShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => formatter.write_str("frontend membership observer exit is pending"),
            Self::SourceClosed => {
                formatter.write_str("frontend membership observation source closed")
            }
            Self::Join(error) => write!(
                formatter,
                "frontend membership observer join failed: {error}"
            ),
        }
    }
}
impl std::error::Error for MembershipShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Join(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) struct MembershipObserver {
    stop: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), MembershipShutdownError>>>,
}
impl MembershipObserver {
    pub(crate) fn start(
        data: &FrontendDataRuntime,
        port: Arc<dyn BackendProcessObservationPort>,
        registry: Arc<MembershipRegistry>,
    ) -> Self {
        start_membership_service(data, port, registry)
    }
    pub(crate) fn has_exited(&self) -> bool {
        self.join
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }
    pub(crate) fn request_stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
    pub(crate) async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), MembershipShutdownError> {
        self.request_stop();
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), join)
            .await
            .map_err(|_| MembershipShutdownError::Pending)?;
        self.join.take();
        result.map_err(MembershipShutdownError::Join)?
    }
}
/// The only member-observer service entry. It accepts no arbitrary query
/// future or identity. The returned original Join remains with the FE owner
/// through timeout; this service never invents a ledger Work or reports exit
/// merely because cancellation/stop was requested (ADR-0218 service boundary).
fn start_membership_service(
    data: &FrontendDataRuntime,
    port: Arc<dyn BackendProcessObservationPort>,
    registry: Arc<MembershipRegistry>,
) -> MembershipObserver {
    let mut changes = port.subscribe_process_changes();
    let (stop, mut stopped) = oneshot::channel();
    let join=data.spawn(async move {
        let initial=*changes.borrow_and_update();registry.publish(initial,port.as_ref());
        loop {
            tokio::select! {
                _=&mut stopped=>return Ok(()),
                result=changes.changed()=>{if result.is_err(){return Err(MembershipShutdownError::SourceClosed);}let revision=*changes.borrow_and_update();registry.publish(revision,port.as_ref());},
                _=registry.changed.notified()=>{let revision=*changes.borrow_and_update();registry.publish(revision,port.as_ref());},
            }
        }
    });
    MembershipObserver {
        stop: Some(stop),
        join: Some(join),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::TestActor;
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        actor_turn::ActorRuntime,
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        identity::{ActorId, Generation, Tick},
        mailbox::{InputClass, InputEnvelope},
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    struct Recorder(Arc<Mutex<Vec<MembershipView>>>);
    impl TestActor for Recorder {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            value: FrontendInput,
            _: &mut ActorStep<'_, super::super::actor::FrontendEffect>,
        ) {
            if let FrontendInput::Membership(update) = value {
                self.0.lock().unwrap().push(update.0);
            }
        }
    }
    struct ObservationPort {
        expected: BackendProcessId,
        endpoint: RuntimeEndpoint,
        value: Mutex<BackendProcessObservation>,
    }
    impl BackendProcessObservationPort for ObservationPort {
        fn observe_process_at_endpoint(
            &self,
            process: BackendProcessId,
            endpoint: &RuntimeEndpoint,
        ) -> Result<BackendProcessObservation, BackendTopologyError> {
            assert_eq!(process, self.expected);
            assert_eq!(endpoint, &self.endpoint);
            Ok(*self.value.lock().unwrap())
        }
    }
    struct Fixture {
        ledger: RuntimeLedger,
        scope: novarocks_control_runtime::transaction::ScopeHandle,
        runtime: ActorRuntime<FrontendActor>,
        registry: MembershipRegistry,
        target: Arc<MembershipTarget>,
        seen: Arc<Mutex<Vec<MembershipView>>>,
        port: ObservationPort,
        participation: ParticipationRef,
    }
    fn fixture() -> Fixture {
        let generation = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let actor = ActorId::new(1).unwrap();
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Recorder(seen.clone()))),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 8,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("original test actor admission"));
        let standing = runtime
            .reserve_input(actor, actor, InputClass::Ordinary)
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("one original ordinary standing input"));
        let backend = BackendProcessId::new_v7();
        let endpoint = RuntimeEndpoint::new("127.0.0.1", 19091).unwrap();
        let participation = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            backend,
        );
        let target = Arc::new(MembershipTarget::new(
            registration,
            Arc::from([FrozenMember {
                participation,
                endpoint: endpoint.clone(),
            }]),
            CoalescingInput::new(standing, membership_input),
        ));
        let registry = MembershipRegistry::new();
        assert!(registry.insert(target.clone()).is_ok());
        Fixture {
            ledger,
            scope,
            runtime,
            registry,
            target,
            seen,
            port: ObservationPort {
                expected: backend,
                endpoint,
                value: Mutex::new(BackendProcessObservation::Current),
            },
            participation,
        }
    }
    fn consume(f: &Fixture) {
        let input = f.target.input.in_flight().unwrap().unwrap();
        assert!(f.runtime.poll_one(Tick(1)).unwrap());
        f.runtime
            .retire_input(f.target.registration.actor(), input)
            .unwrap();
        f.target.after_retire().unwrap();
    }
    fn finish(f: Fixture) {
        f.registry.close_delivery();
        f.target.close_after_exit().unwrap();
        assert!(f.registry.remove(&f.target.registration).is_some());
        while let Some(turn) = f.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        f.scope.close_external().unwrap();
        assert!(
            f.runtime
                .remove_actor(f.target.registration.actor())
                .is_ok()
        );
        f.ledger.remove_scope(f.scope.id()).unwrap();
        assert!(f.ledger.is_empty());
    }
    #[test]
    fn membership_revisions_coalesce_to_the_latest_real_exact_process_view() {
        let f = fixture();
        f.registry.publish(1, &f.port);
        f.registry.publish(2, &f.port);
        let replacement = BackendProcessId::new_v7();
        *f.port.value.lock().unwrap() = BackendProcessObservation::Replaced {
            current_process: replacement,
        };
        f.registry.publish(3, &f.port);
        consume(&f);
        consume(&f);
        let seen = f.seen.lock().unwrap();
        assert_eq!(
            seen.iter().map(|view| view.revision).collect::<Vec<_>>(),
            [1, 3]
        );
        assert_eq!(
            seen[0].processes[&f.participation],
            MembershipObservation::ExactOriginal
        );
        assert_eq!(
            seen[1].processes[&f.participation],
            MembershipObservation::ExactReplacement(replacement)
        );
        drop(seen);
        finish(f);
    }
    #[test]
    fn closing_member_delivery_preserves_the_in_flight_input_until_real_retirement() {
        let f = fixture();
        f.registry.publish(1, &f.port);
        f.registry.close_delivery();
        f.registry.publish(2, &f.port);
        assert!(matches!(
            f.target.close_after_exit(),
            Err(ActorTurnError::StandingInputBusy)
        ));
        assert!(!f.registry.is_empty());
        assert!(!f.runtime.is_empty());
        consume(&f);
        assert!(f.target.input.in_flight().unwrap().is_none());
        assert_eq!(f.seen.lock().unwrap().len(), 1);
        finish(f);
    }
}
