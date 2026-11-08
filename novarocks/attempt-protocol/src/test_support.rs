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
use novarocks_execution_contract::{
    ExchangeDeliveryRequirement, ExchangeEdgeId, ParticipationRef, PlanNodeId, TaskIdentity,
};
use novarocks_types::{
    AttemptId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
};
use std::{collections::BTreeMap, num::NonZeroU64};
pub(crate) fn graph_with_strict_input(
    extra_leaf_on_producer: bool,
) -> (TaskGraph, AttemptActivationIdentity) {
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
                fragment_id: index as i32,
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
                destination_node: PlanNodeId::new(n as i32).unwrap(),
                delivery,
                producers: Box::from([producer]),
                destinations: Box::from([destination]),
            },
        );
    }
    let graph = TaskGraph::from_frozen(FrozenTaskGraph {
        root: identities[0],
        tasks,
        stages,
        edges,
    })
    .unwrap();
    (
        graph,
        AttemptActivationIdentity::from_parts(1, execution, 1),
    )
}
