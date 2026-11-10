//! Deterministic identities for original protocol assertions.
#[cfg(test)]
pub(crate) fn backend(value: u64) -> novarocks_types::BackendProcessId {
    let raw = (7u128 << 76) | (2u128 << 62) | u128::from(value);
    novarocks_types::BackendProcessId::try_from_bytes(raw.to_be_bytes()).expect("test UUIDv7")
}

#[cfg(test)]
pub(crate) fn remote(value: u32) -> crate::remote_task::RemoteTask {
    use crate::start::{FrozenCreation, FrozenTask, FrozenTaskGraph, PayloadRef};
    use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
    use novarocks_types::{
        AttemptId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };
    use std::{collections::BTreeMap, num::NonZeroU64};
    let execution =
        QueryExecutionId::new(QueryId::new(71, 72), AttemptId::new(1).unwrap()).unwrap();
    let stage = StageId::new(1).unwrap();
    let id = TaskId::new(value).unwrap();
    let identity = TaskIdentity::new(execution, stage, id, backend(u64::from(value)));
    let participation = ParticipationRef::new(
        execution,
        FrontendProcessId::try_from_bytes(backend(700).to_bytes()).unwrap(),
        identity.backend_process_id(),
    );
    let creation = FrozenCreation {
        resource: PayloadRef(NonZeroU64::new(u64::from(value)).unwrap()),
        queued_bytes: 256,
        plan_carrier_bytes: 0,
        descriptor_encoded_bytes: 0,
    };
    let graph = crate::graph::TaskGraph::from_frozen(FrozenTaskGraph {
        root: identity,
        tasks: BTreeMap::from([(
            id,
            FrozenTask {
                identity,
                participation,
                task_key: UniqueId::new(i64::from(value), 1),
                fragment_id: 1,
                split_plan_nodes: Box::new([]),
                creation,
            },
        )]),
        stages: BTreeMap::from([(stage, vec![identity].into_boxed_slice())]),
        edges: BTreeMap::new(),
    })
    .unwrap();
    crate::remote_task::RemoteTask::new(
        graph.task(id).unwrap(),
        creation,
        vec![],
        &mut crate::ids::OperationIds::new(crate::ids::IdSeed::new(1, 100)),
        vec![],
    )
    .unwrap()
}

use crate::graph::TaskGraph;
use crate::start::{FrozenCreation, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph, PayloadRef};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::descriptor::FragmentNodeId;
use novarocks_execution_contract::{
    ExchangeDeliveryRequirement, ExchangeEdgeId, ParticipationRef, TaskIdentity,
};
use novarocks_types::{
    AttemptId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
};
use std::{collections::BTreeMap, num::NonZeroU64};
pub(crate) fn graph_with_strict_input(
    extra_leaf_on_producer: bool,
) -> (TaskGraph, AttemptActivationIdentity) {
    let (graph, activation) = frozen_graph_with_strict_input(extra_leaf_on_producer);
    (TaskGraph::from_frozen(graph).unwrap(), activation)
}

pub(crate) fn frozen_graph_with_strict_input(
    extra_leaf_on_producer: bool,
) -> (FrozenTaskGraph, AttemptActivationIdentity) {
    let execution =
        QueryExecutionId::new(QueryId::new(71, 72), AttemptId::new(1).unwrap()).unwrap();
    let frontend =
        FrontendProcessId::try_from_bytes(crate::test_support::backend(700).to_bytes()).unwrap();
    let mut tasks = BTreeMap::new();
    let mut stages = BTreeMap::new();
    let mut identities = Vec::new();
    for index in 1..=if extra_leaf_on_producer { 4 } else { 3 } {
        let id = TaskId::new(index).unwrap();
        let stage = StageId::new(index).unwrap();
        let backend = crate::test_support::backend(if index == 4 { 2 } else { u64::from(index) });
        let identity = TaskIdentity::new(execution, stage, id, backend);
        identities.push(identity);
        tasks.insert(
            id,
            FrozenTask {
                identity,
                participation: ParticipationRef::new(execution, frontend, backend),
                task_key: UniqueId::new(i64::from(index), 1),
                fragment_id: index,
                split_plan_nodes: Box::new([]),
                creation: FrozenCreation {
                    resource: PayloadRef(NonZeroU64::new(u64::from(index)).unwrap()),
                    queued_bytes: 256,
                    plan_carrier_bytes: 0,
                    descriptor_encoded_bytes: 0,
                },
            },
        );
        stages.insert(stage, Box::from([identity]));
    }
    let mut edges = BTreeMap::new();
    for (n, producer, destination, delivery) in [
        (
            1,
            identities[1],
            identities[0],
            ExchangeDeliveryRequirement::DemandDriven,
        ),
        (
            2,
            identities[2],
            identities[1],
            ExchangeDeliveryRequirement::MustDrain,
        ),
    ] {
        let edge = ExchangeEdgeId::new(n).unwrap();
        edges.insert(
            edge,
            FrozenExchangeEdge {
                edge,
                producer_stage: producer.stage_id(),
                consumer_stage: destination.stage_id(),
                destination_node: FragmentNodeId::new(n as i32),
                delivery,
                producers: Box::from([producer]),
                destinations: Box::from([destination]),
            },
        );
    }
    let graph = FrozenTaskGraph {
        root: identities[0],
        tasks,
        stages,
        edges,
    };
    (
        graph,
        AttemptActivationIdentity::from_parts(1, execution, 1),
    )
}

pub(crate) fn execution_with_strict_input() -> crate::execution::QueryTaskExecution {
    let (graph, _) = crate::test_support::graph_with_strict_input(false);
    execution_with_graph(graph)
}
pub(crate) fn execution_with_graph(graph: TaskGraph) -> crate::execution::QueryTaskExecution {
    let epochs = graph
        .participations()
        .map(|p| {
            (
                p.backend_process_id(),
                novarocks_execution_contract::AdmissionEpochCapability::try_from_bytes([0x61; 16])
                    .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let positions = epochs.keys().map(|&p| (p, 16)).collect();
    let transport = crate::config::FrozenTransportLimits {
        max_batch_items: 4,
        max_batch_encoded_bytes: 4096,
        max_descriptor_encoded_bytes: 4096,
        max_query_backend_queued_operations: 1024,
        max_query_backend_queued_bytes: 1_048_576,
        max_backend_queued_operations: 4096,
        max_backend_queued_bytes: 4_194_304,
        max_tasks_per_participation: 1024,
        max_active_tasks_per_backend: 1024,
        frontend_queue_residence: std::time::Duration::from_secs(5),
    };
    crate::execution::QueryTaskExecution::new(
        graph,
        crate::config::DispatchBudget::DEFAULT,
        transport,
        novarocks_types::NativeCompatibilityId::new([0x41; 32]),
        &epochs,
        &positions,
        novarocks_coordination_contract::MonotonicInstant::ORIGIN,
        crate::ids::OperationIds::new(crate::ids::IdSeed::new(100, 1)),
        crate::lease::RequestedLeaseDurations::DEFAULT,
    )
    .unwrap()
}
