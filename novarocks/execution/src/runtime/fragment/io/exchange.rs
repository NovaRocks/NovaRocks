//! Host-owned asynchronous transport boundary for exact Exchange units.

use crate::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution_contract::ExchangeStreamIdentity;

/// All implementations explicitly install the Unit/End, demand and observation
/// contract. Data call activity remains with its actual request owner through
/// exit, independently of receipt, timeout or cancellation observations.
pub trait ExchangeFrameTransmitter: Send + Sync + 'static {
    fn deliver_unit(
        &self,
        frame: super::exchange_unit::ExchangeUnitFrame,
        stop: tokio_util::sync::CancellationToken,
        activity: super::exchange_unit::ExchangeCallActivity,
        done: super::exchange_unit::ExchangeDeliveryCallback,
    );
    fn register_unit_demands(
        &self,
        destination: RuntimeEndpoint,
        entries: Vec<super::exchange_unit::ExchangeDemandEntry>,
        stop: tokio_util::sync::CancellationToken,
        done: super::exchange_unit::ExchangeDemandCallback,
    );
    fn watch_unit_facts(
        &self,
        destination: RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: super::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_failure_contract::FailureFact>;
    fn forget_unit_facts(&self, stream: ExchangeStreamIdentity);
    fn schedule_unit_retry(
        &self,
        delay: std::time::Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_failure_contract::FailureFact>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::runtime::execution_runtime::ExecutionRuntime;
    use crate::runtime::fragment::io::exchange_unit::*;
    use crate::runtime::fragment::io::{
        ExchangeReceiverKey, ExchangeReceiverPort, ExecutionRuntimeExchangeReceiverPort,
    };
    use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
    use novarocks_execution_contract::{ExchangeSource, FragmentNodeId, TaskIdentity};
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{StageId, TaskId};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    pub(crate) struct Fixture {
        pub(crate) runtime: Arc<ExecutionRuntime>,
        pub(crate) port: Arc<dyn ExchangeReceiverPort>,
        pub(crate) transport: Arc<UnitLoopback>,
        base: TaskIdentity,
        states: Mutex<HashMap<UniqueId, Arc<RuntimeState>>>,
    }
    impl Fixture {
        pub(crate) fn new() -> Arc<Self> {
            Self::with_runtime(crate::runtime::execution_runtime::test_execution_runtime())
        }
        pub(crate) fn with_runtime(runtime: Arc<ExecutionRuntime>) -> Arc<Self> {
            let port: Arc<dyn ExchangeReceiverPort> =
                Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime.clone()));
            let transport = UnitLoopback::new(port.clone());
            let base = crate::runtime::task_control::test_support::Owner::new()
                .view(novarocks_execution_contract::TaskFailurePhase::Execution)
                .identity();
            Arc::new(Self {
                runtime,
                port,
                transport,
                base,
                states: Mutex::new(HashMap::new()),
            })
        }
        pub(crate) fn state(&self, instance: UniqueId) -> Arc<RuntimeState> {
            let mut states = self.states.lock().unwrap();
            if let Some(state) = states.get(&instance) {
                return state.clone();
            }
            let identity = TaskIdentity::new(
                self.base.query_execution_id(),
                StageId::new(1).unwrap(),
                TaskId::new(u32::try_from(states.len() + 1).unwrap()).unwrap(),
                self.base.backend_process_id(),
            );
            let owner = crate::runtime::task_control::test_support::owner_for(identity);
            let control = Arc::new(RuntimeErrorState::for_task(Arc::new(
                owner.view(novarocks_execution_contract::TaskFailurePhase::Execution),
            )));
            let tracker =
                crate::runtime::mem_tracker::MemTracker::new_root("compiled Unit fixture Task");
            tracker.install_limit_once(64 * 1024 * 1024).unwrap();
            let state = Arc::new(RuntimeState::new(
                None,
                None,
                None,
                Some(instance),
                Some(1),
                Some(tracker),
                Some(self.runtime.clone()),
                control,
            ));
            states.insert(instance, state.clone());
            state
        }
        pub(crate) fn task(&self, instance: UniqueId) -> TaskIdentity {
            self.state(instance).error_state().task_identity().unwrap()
        }
        pub(crate) fn creation(
            &self,
            instance: UniqueId,
        ) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
            let state = self.state(instance);
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                state.mem_tracker().unwrap().clone(), state.error_state().clone(),
            ).unwrap()
        }
        pub(crate) fn stream(
            &self,
            source: UniqueId,
            destination: UniqueId,
            node: i32,
            ordinal: u32,
        ) -> ExchangeStreamIdentity {
            ExchangeStreamIdentity::try_new(
                ExchangeSource::new(self.task(source), source, ordinal),
                self.task(destination),
                destination,
                FragmentNodeId::new(node),
            )
            .unwrap()
        }
        pub(crate) fn bind(&self, key: ExchangeReceiverKey, sources: &[UniqueId]) {
            let streams = sources
                .iter()
                .enumerate()
                .map(|(ordinal, source)| {
                    self.stream(
                        *source,
                        key.fragment_instance_id,
                        key.node_id,
                        u32::try_from(ordinal).unwrap(),
                    )
                })
                .collect();
            self.port.bind_unit_receiver(key, streams, self.state(key.fragment_instance_id).error_state().clone(), novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64).unwrap();
        }
        pub(crate) fn destination(
            &self,
            destination: UniqueId,
            source: UniqueId,
            ordinal: u32,
            count: u32,
        ) -> crate::runtime::endpoint::FragmentDestination {
            crate::runtime::endpoint::FragmentDestination::new(
                destination,
                RuntimeEndpoint::new("127.0.0.1", 9030).unwrap(),
                source,
                ordinal,
                count,
            )
            .unwrap()
            .with_task_identity(self.task(destination))
        }
        pub(crate) fn assignment(
            &self,
            assignment: &crate::runtime::fragment::instance::FragmentSinkAssignment,
        ) -> crate::runtime::fragment::instance::FragmentSinkAssignment {
            use crate::runtime::fragment::instance::FragmentSinkAssignment;
            let bind = |dest: crate::runtime::endpoint::FragmentDestination| {
                let task = self.task(*dest.finst_id());
                dest.with_task_identity(task)
            };
            match assignment {
                FragmentSinkAssignment::StreamDestinations {
                    destinations,
                    sender_id,
                } => FragmentSinkAssignment::StreamDestinations {
                    destinations: destinations.iter().cloned().map(bind).collect(),
                    sender_id: *sender_id,
                },
                FragmentSinkAssignment::DestinationGroups { groups, sender_id } => {
                    FragmentSinkAssignment::DestinationGroups {
                        groups: groups
                            .iter()
                            .map(|group| group.iter().cloned().map(bind).collect())
                            .collect(),
                        sender_id: *sender_id,
                    }
                }
                other => other.clone(),
            }
        }
        pub(crate) fn gates(
            &self,
            program: &novarocks_local_program::LocalProgram,
            assignment: &crate::runtime::fragment::instance::FragmentSinkAssignment,
        ) -> Option<Arc<super::super::exchange_edge::ExchangeEdgeGates>> {
            use super::super::exchange_edge::{ExchangeDestinationKey, ExchangeEdgeGates};
            use crate::runtime::fragment::instance::FragmentSinkAssignment;
            use crate::task_execution::domain::{EdgeOpenVersion, ExchangeEdgeId};
            let branches: Vec<_> = match (assignment, program.graph().sink()) {
                (
                    FragmentSinkAssignment::StreamDestinations { destinations, .. },
                    Some(novarocks_local_program::StaticSinkProgram::DataStream { branch, .. }),
                ) => vec![(branch.dest_node_id(), destinations)],
                (
                    FragmentSinkAssignment::DestinationGroups { groups, .. },
                    Some(novarocks_local_program::StaticSinkProgram::ChangeStreamRouter {
                        branches,
                        ..
                    }),
                ) => branches
                    .iter()
                    .zip(groups)
                    .map(|(branch, group)| (branch.dest_node_id(), group))
                    .collect(),
                _ => return None,
            };
            let edges: Vec<_> = branches
                .into_iter()
                .enumerate()
                .map(|(index, (node, destinations))| {
                    (
                        ExchangeEdgeId::new(u32::try_from(index + 1).unwrap()).unwrap(),
                        destinations
                            .iter()
                            .map(|dest| ExchangeDestinationKey::new(*dest.finst_id(), node))
                            .collect(),
                    )
                })
                .collect();
            let ids = edges.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let gates = ExchangeEdgeGates::try_new(edges).unwrap();
            gates.open(EdgeOpenVersion::new(1).unwrap(), &ids).unwrap();
            Some(gates)
        }
        pub(crate) fn sink(
            &self,
            program: &Arc<novarocks_local_program::LocalProgram>,
            assignment: &crate::runtime::fragment::instance::FragmentSinkAssignment,
            instance: UniqueId,
            transmitter: Arc<dyn ExchangeFrameTransmitter>,
            result: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
            gates: Option<Arc<super::super::exchange_edge::ExchangeEdgeGates>>,
        ) -> Result<
            Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>,
            crate::runtime::fragment::error::FragmentLaunchError,
        > {
            let assignment = self.assignment(assignment);
            let gates = gates.or_else(|| self.gates(program, &assignment));
            crate::runtime::fragment::sink::materialize_compiled_sink(
                program,
                &assignment,
                instance,
                transmitter,
                result,
                gates,
            )
        }
    }

    pub(crate) struct UnitLoopback {
        port: Arc<dyn ExchangeReceiverPort>,
        watches: Arc<Mutex<HashMap<ExchangeStreamIdentity, ExchangeGrantWake>>>,
    }
    struct Listener(Arc<Mutex<HashMap<ExchangeStreamIdentity, ExchangeGrantWake>>>);
    impl ExchangeUnitStateListener for Listener {
        fn changed(&self, observation: ExchangeStreamObservation) {
            let wake = self.0.lock().unwrap().get(&observation.stream).cloned();
            if let Some(wake) = wake {
                wake(ExchangeGrantNotice::Facts(observation.facts));
            }
        }
    }
    impl UnitLoopback {
        pub(crate) fn new(port: Arc<dyn ExchangeReceiverPort>) -> Arc<Self> {
            let watches = Arc::new(Mutex::new(HashMap::new()));
            port.set_unit_state_listener(Arc::new(Listener(watches.clone())))
                .unwrap();
            Arc::new(Self { port, watches })
        }
        fn key(stream: ExchangeStreamIdentity) -> ExchangeReceiverKey {
            ExchangeReceiverKey {
                fragment_instance_id: stream.destination_fragment_instance_id(),
                node_id: stream.destination_node().get(),
            }
        }
    }
    impl ExchangeFrameTransmitter for UnitLoopback {
        fn deliver_unit(
            &self,
            frame: ExchangeUnitFrame,
            _stop: tokio_util::sync::CancellationToken,
            activity: ExchangeCallActivity,
            done: ExchangeDeliveryCallback,
        ) {
            let result = self
                .port
                .receive_unit(Self::key(frame.receiver.stream), frame.receiver);
            done(match result {
                Ok(receipt) => ExchangeCallOutcome::Observed(receipt),
                Err(failure) => ExchangeCallOutcome::Refused(failure),
            });
            drop(activity);
        }
        fn register_unit_demands(
            &self,
            _destination: RuntimeEndpoint,
            entries: Vec<ExchangeDemandEntry>,
            _stop: tokio_util::sync::CancellationToken,
            done: ExchangeDemandCallback,
        ) {
            let results = entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.stream,
                        self.port
                            .register_unit_demand(Self::key(entry.stream), entry)
                            .map(|observation| observation.facts),
                    )
                })
                .collect();
            done(ExchangeDemandCallOutcome::Observed(results));
        }
        fn watch_unit_facts(
            &self,
            _destination: RuntimeEndpoint,
            stream: ExchangeStreamIdentity,
            wake: ExchangeGrantWake,
        ) -> Result<(), novarocks_failure_contract::FailureFact> {
            self.watches.lock().unwrap().insert(stream, wake.clone());
            let observations = self.port.unit_sender_states(
                stream.source().task().backend_process_id(),
                None,
                usize::MAX,
            )?;
            if let Some(observation) = observations
                .into_iter()
                .find(|observation| observation.stream == stream)
            {
                wake(ExchangeGrantNotice::Facts(observation.facts));
            }
            Ok(())
        }
        fn forget_unit_facts(&self, stream: ExchangeStreamIdentity) {
            self.watches.lock().unwrap().remove(&stream);
        }
        fn schedule_unit_retry(
            &self,
            delay: std::time::Duration,
            task: Box<dyn FnOnce() + Send + 'static>,
        ) -> Result<(), novarocks_failure_contract::FailureFact> {
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                task();
            });
            Ok(())
        }
    }

    // Result-only programs have no outbound route; an accidental use fails the
    // fixture immediately instead of manufacturing receiver custody.
    pub(crate) struct UnusedTransport;
    impl ExchangeFrameTransmitter for UnusedTransport {
        fn deliver_unit(
            &self,
            _: ExchangeUnitFrame,
            _: tokio_util::sync::CancellationToken,
            _: ExchangeCallActivity,
            _: ExchangeDeliveryCallback,
        ) {
            panic!("result-only fixture attempted Unit delivery")
        }
        fn register_unit_demands(
            &self,
            _: RuntimeEndpoint,
            _: Vec<ExchangeDemandEntry>,
            _: tokio_util::sync::CancellationToken,
            _: ExchangeDemandCallback,
        ) {
            panic!("result-only fixture attempted demand registration")
        }
        fn watch_unit_facts(
            &self,
            _: RuntimeEndpoint,
            _: ExchangeStreamIdentity,
            _: ExchangeGrantWake,
        ) -> Result<(), novarocks_failure_contract::FailureFact> {
            panic!("result-only fixture attempted subscription")
        }
        fn forget_unit_facts(&self, _: ExchangeStreamIdentity) {}
        fn schedule_unit_retry(
            &self,
            _: std::time::Duration,
            _: Box<dyn FnOnce() + Send + 'static>,
        ) -> Result<(), novarocks_failure_contract::FailureFact> {
            panic!("result-only fixture attempted retry")
        }
    }
}

#[cfg(test)]
pub(crate) fn discard_exchange_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
    std::sync::Arc::new(test_support::UnusedTransport)
}

#[cfg(test)]
pub(crate) fn in_process_test_exchange_receiver_port()
-> std::sync::Arc<dyn super::ExchangeReceiverPort> {
    std::sync::Arc::new(super::ExecutionRuntimeExchangeReceiverPort::new(
        crate::runtime::execution_runtime::test_execution_runtime(),
    ))
}

#[cfg(test)]
mod tests {
    use super::test_support::Fixture;
    use crate::runtime::fragment::io::exchange_unit::{
        ExchangeUnitItem, ExchangeUnitReceiverFrame, ExchangeUnitSequence,
    };
    use crate::runtime::fragment::io::{ExchangeReceiverKey, ExchangeReceiverRegistration};
    use novarocks_types::UniqueId;

    #[test]
    fn native_eos_uses_exact_source_identity_when_legacy_sender_fields_collide() {
        let fixture = Fixture::new();
        let port = &fixture.port;
        let key = ExchangeReceiverKey {
            fragment_instance_id: UniqueId::new(50, 60),
            node_id: 70,
        };
        let sources = [UniqueId::new(1, 1), UniqueId::new(2, 2)];
        let factory = fixture.creation(key.fragment_instance_id);
        let layout = crate::exec::chunk::typed_fixture::layout(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            &[],
            &[],
        );
        port.register(ExchangeReceiverRegistration {
            key,
            expected_senders: 2,
            expected_chunk_schema: factory.create_compiled_chunk_schema(&layout).unwrap(),
            column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        })
        .unwrap();
        fixture.bind(key, &sources);
        for (ordinal, source) in sources.into_iter().enumerate() {
            port.receive_unit(
                key,
                ExchangeUnitReceiverFrame {
                    stream: fixture.stream(
                        source,
                        key.fragment_instance_id,
                        key.node_id,
                        u32::try_from(ordinal).unwrap(),
                    ),
                    sender_count: 2,
                    sender_id: 7,
                    backend_number: 11,
                    item: ExchangeUnitItem::End(ExchangeUnitSequence::FIRST),
                    payload: bytes::Bytes::new(),
                    next_demand: None,
                },
            )
            .unwrap();
        }
        let snapshot = port.snapshot(key).unwrap();
        assert_eq!(snapshot.finished_senders, 2);
        port.remove(key);
        assert!(port.snapshot(key).is_none());
        drop(factory);
        // The same Task owns schema construction and runtime execution.
        assert_eq!(
            fixture
                .state(key.fragment_instance_id)
                .mem_tracker()
                .unwrap()
                .current(),
            0
        );
    }
}
