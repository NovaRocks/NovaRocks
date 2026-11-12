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

//! Current consumer proof for one bounded stage-demand traversal. Every
//! checked consumer is updated by its original Stage count mutation. Unrelated
//! changes neither restart the proof nor append an unbounded change journal.

use crate::{capacity::StepContext, error::TaskExecutionError, graph::TaskGraph};
use novarocks_execution_contract::StageRef;
use novarocks_types::{QueryExecutionId, StageId};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
struct Consumer {
    stage: StageId,
    ordinal: usize,
}
#[derive(Debug)]
pub(crate) struct FrozenStageDemand {
    consumers: BTreeMap<StageId, Arc<[Consumer]>>,
    stages: BTreeSet<StageId>,
    execution: QueryExecutionId,
}
impl FrozenStageDemand {
    pub(crate) fn new(graph: &TaskGraph) -> Self {
        let ordinals = graph
            .stages()
            .enumerate()
            .map(|(ordinal, stage)| (stage.stage_id(), ordinal))
            .collect::<BTreeMap<_, _>>();
        let mut consumers = BTreeMap::<StageId, BTreeSet<StageId>>::new();
        for edge in graph.edges() {
            consumers
                .entry(edge.producer_stage())
                .or_default()
                .insert(edge.consumer_stage());
        }
        Self {
            stages: ordinals.keys().copied().collect(),
            execution: graph.execution_id(),
            consumers: consumers
                .into_iter()
                .map(|(producer, consumers)| {
                    (
                        producer,
                        consumers
                            .into_iter()
                            .map(|stage| Consumer {
                                stage,
                                ordinal: ordinals[&stage],
                            })
                            .collect::<Vec<_>>()
                            .into(),
                    )
                })
                .collect(),
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
struct Checked {
    epoch: u128,
    released: bool,
}
#[derive(Debug)]
struct Proof {
    producer: StageId,
    consumers: Arc<[Consumer]>,
    position: usize,
    unreleased: usize,
    epoch: u128,
}
#[derive(Debug)]
struct State {
    next_epoch: u128,
    checked: Box<[Checked]>,
    proof: Option<Proof>,
    pending: BTreeSet<StageId>,
}
/// All mutation borrows are private and end before returning a Copy value.
/// No Ref/RefMut escapes to a Task guard, budget port or authorizing operation.
/// This is passive derived state, never a Source, grant, callback, or queue of
/// executable work. At most one coalesced stage entry exists per frozen Stage.
#[derive(Debug)]
pub(crate) struct StageDemandObservations {
    frozen: FrozenStageDemand,
    state: RefCell<State>,
}
impl StageDemandObservations {
    pub(crate) fn new(graph: &TaskGraph) -> Self {
        let frozen = FrozenStageDemand::new(graph);
        let checked = vec![Checked::default(); frozen.stages.len()].into_boxed_slice();
        Self {
            frozen,
            state: RefCell::new(State {
                next_epoch: 0,
                checked,
                proof: None,
                pending: BTreeSet::new(),
            }),
        }
    }
    pub(crate) fn binds_stage(&self, stage: StageRef) -> bool {
        stage.query_execution_id() == self.frozen.execution
            && self.frozen.stages.contains(&stage.stage_id())
    }
    pub(crate) fn begin(&self, producer: StageId) -> Result<(), TaskExecutionError> {
        let consumers = self
            .frozen
            .consumers
            .get(&producer)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let mut state = self.state.borrow_mut();
        if state.proof.is_some() {
            return Err(TaskExecutionError::Schedule(
                "an active stage consumer proof must be finished before replacement".into(),
            ));
        }
        state.next_epoch = state.next_epoch.checked_add(1).ok_or_else(|| {
            TaskExecutionError::Schedule("stage proof generation exhausted".into())
        })?;
        state.proof = Some(Proof {
            producer,
            consumers: Arc::clone(consumers),
            position: 0,
            unreleased: 0,
            epoch: state.next_epoch,
        });
        Ok(())
    }
    pub(crate) fn finish(&self) {
        self.state.borrow_mut().proof = None;
    }
    pub(crate) fn producer(&self) -> Option<StageId> {
        self.state
            .borrow()
            .proof
            .as_ref()
            .map(|proof| proof.producer)
    }
    pub(crate) fn next_consumer(&self) -> Option<StageId> {
        let state = self.state.borrow();
        let proof = state.proof.as_ref()?;
        proof
            .consumers
            .get(proof.position)
            .map(|consumer| consumer.stage)
    }
    /// Caller reads this original Stage's current predicate on the same pure
    /// serial owner. Charging happens before borrowing any observation state.
    pub(crate) fn check_next(
        &self,
        stage: StageId,
        released: bool,
        context: &mut dyn StepContext,
    ) -> Result<bool, TaskExecutionError> {
        if self.next_consumer() != Some(stage) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if !context.charge(1) {
            return Ok(false);
        }
        let mut state = self.state.borrow_mut();
        let State { checked, proof, .. } = &mut *state;
        let proof = proof.as_mut().expect("same original proof");
        let consumer = proof.consumers[proof.position];
        debug_assert_eq!(consumer.stage, stage);
        checked[consumer.ordinal] = Checked {
            epoch: proof.epoch,
            released,
        };
        proof.unreleased += usize::from(!released);
        proof.position += 1;
        Ok(true)
    }
    pub(crate) fn all_consumers_released(&self) -> Option<bool> {
        let state = self.state.borrow();
        let proof = state.proof.as_ref()?;
        (proof.position == proof.consumers.len()).then_some(proof.unreleased == 0)
    }
    /// Only a real current Stage released predicate change arrives here.
    /// Each primary Stage mutation updates at most one already checked row.
    pub(crate) fn stage_changed(&self, stage: StageId, released: bool) {
        let mut state = self.state.borrow_mut();
        if released {
            state.pending.insert(stage);
        } else {
            state.pending.remove(&stage);
        }
        let State { checked, proof, .. } = &mut *state;
        let Some(proof) = proof else {
            return;
        };
        let Ok(position) = proof
            .consumers
            .binary_search_by_key(&stage, |consumer| consumer.stage)
        else {
            return;
        };
        let row = &mut checked[proof.consumers[position].ordinal];
        if row.epoch != proof.epoch || row.released == released {
            return;
        }
        if released {
            proof.unreleased -= 1;
        } else {
            proof.unreleased += 1;
        }
        row.released = released;
    }
    /// Retain a real coalesced release which arrived while its original
    /// parent was waiting for admission. This schedules derived work only;
    /// it does not publish or change any consumer truth in the active proof.
    pub(crate) fn revisit_parent(&self, parent: StageId) {
        assert!(
            self.frozen.stages.contains(&parent),
            "original frozen parent"
        );
        self.state.borrow_mut().pending.insert(parent);
    }
    pub(crate) fn next_parent(&self) -> Option<StageId> {
        self.state.borrow_mut().pending.pop_first()
    }
    pub(crate) fn has_pending_parents(&self) -> bool {
        !self.state.borrow().pending.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        capacity::QueueStage,
        config::DispatchLane,
        start::{FrozenCreation, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph, PayloadRef},
    };
    use novarocks_execution_contract::descriptor::FragmentNodeId;
    use novarocks_execution_contract::{ExchangeDeliveryRequirement, ExchangeEdgeId, TaskIdentity};
    use novarocks_types::{BackendProcessId, TaskId, UniqueId};
    use std::num::NonZeroU64;
    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, n: usize) -> bool {
            if n > self.0 {
                false
            } else {
                self.0 -= n;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("consumer proof grants no transport capacity")
        }
    }
    fn stage(n: u32) -> StageId {
        StageId::new(n).unwrap()
    }
    pub(crate) fn graph() -> TaskGraph {
        graph_variant(false, false)
    }
    pub(crate) fn graph_variant(strict_producer: bool, root_producer: bool) -> TaskGraph {
        build_graph(strict_producer, root_producer, false, false)
    }
    pub(crate) fn graph_two_task_consumer() -> TaskGraph {
        build_graph(false, false, true, false)
    }
    pub(crate) fn graph_chain() -> TaskGraph {
        build_graph(false, false, false, true)
    }
    fn build_graph(
        strict_producer: bool,
        root_producer: bool,
        multi_consumer: bool,
        chain: bool,
    ) -> TaskGraph {
        let sample = crate::test_support::remote(1);
        let mut tasks = BTreeMap::new();
        let mut stages = BTreeMap::<StageId, Vec<TaskIdentity>>::new();
        let mut identities = Vec::new();
        for n in 1..=if multi_consumer { 5 } else { 4 } {
            let id = TaskId::new(n).unwrap();
            let stage = stage(if n == 5 { 1 } else { n });
            let identity = TaskIdentity::new(
                sample.identity().query_execution_id(),
                stage,
                id,
                sample.identity().backend_process_id(),
            );
            identities.push(identity);
            tasks.insert(
                id,
                FrozenTask {
                    identity,
                    participation: sample.participation(),
                    task_key: UniqueId::new(i64::from(n), 1),
                    fragment_id: n,
                    split_plan_nodes: Box::new([]),
                    creation: FrozenCreation {
                        resource: PayloadRef(NonZeroU64::new(u64::from(n)).unwrap()),
                        queued_bytes: 256,
                    },
                },
            );
            stages.entry(stage).or_default().push(identity);
        }
        let mut edges = BTreeMap::new();
        let edge_specs = if chain {
            vec![(1, 2, 1), (2, 3, 2)]
        } else {
            vec![
                (1, 2, 1),
                (2, 2, 3),
                (3, 2, 1),
                (4, 4, if strict_producer { 2 } else { 1 }),
            ]
        };
        for (n, producer, consumer) in edge_specs {
            let edge = ExchangeEdgeId::new(n).unwrap();
            edges.insert(
                edge,
                FrozenExchangeEdge {
                    edge,
                    producer_stage: stage(producer),
                    consumer_stage: stage(consumer),
                    destination_node: FragmentNodeId::new(n as i32),
                    delivery: if n == 4 && strict_producer {
                        ExchangeDeliveryRequirement::MustDrain
                    } else {
                        ExchangeDeliveryRequirement::DemandDriven
                    },
                    producers: Box::from([identities[producer as usize - 1]]),
                    destinations: Box::from([identities[consumer as usize - 1]]),
                },
            );
        }
        TaskGraph::from_frozen(FrozenTaskGraph {
            root: identities[usize::from(root_producer)],
            tasks,
            stages: stages
                .into_iter()
                .map(|(id, tasks)| (id, tasks.into_boxed_slice()))
                .collect(),
            edges,
        })
        .unwrap()
    }
    #[test]
    fn checked_rows_track_current_predicate_while_unchecked_rows_and_unrelated_changes_do_not_restart()
     {
        let observations = StageDemandObservations::new(&graph());
        observations.begin(stage(2)).unwrap();
        assert_eq!(observations.next_consumer(), Some(stage(1)));
        assert!(
            !observations
                .check_next(stage(1), true, &mut Turn(0))
                .unwrap()
        );
        assert_eq!(observations.next_consumer(), Some(stage(1)));
        assert!(
            observations
                .check_next(stage(1), true, &mut Turn(1))
                .unwrap()
        );
        assert_eq!(observations.all_consumers_released(), None);
        observations.stage_changed(stage(3), true); // Not checked yet.
        observations.stage_changed(stage(1), false); // Correct the already checked row.
        for _ in 0..1000 {
            observations.stage_changed(stage(4), true);
            observations.stage_changed(stage(4), false);
        }
        assert_eq!(observations.next_consumer(), Some(stage(3)));
        assert!(
            observations
                .check_next(stage(3), true, &mut Turn(1))
                .unwrap()
        );
        assert_eq!(observations.next_consumer(), None); // Repeated edge to C1 did not add a second row.
        assert_eq!(observations.all_consumers_released(), Some(false));
        observations.stage_changed(stage(1), true);
        observations.stage_changed(stage(1), true);
        assert_eq!(observations.all_consumers_released(), Some(true));
        observations.stage_changed(stage(3), false);
        assert_eq!(observations.all_consumers_released(), Some(false));
        observations.stage_changed(stage(3), true);
        assert_eq!(observations.all_consumers_released(), Some(true));
    }
    #[test]
    fn finishing_false_proof_allows_independent_producer_and_reuses_fixed_rows_without_stale_truth()
    {
        let observations = StageDemandObservations::new(&graph());
        let allocation = observations.state.borrow().checked.as_ptr();
        observations.begin(stage(2)).unwrap();
        observations
            .check_next(stage(1), false, &mut Turn(1))
            .unwrap();
        observations
            .check_next(stage(3), true, &mut Turn(1))
            .unwrap();
        assert_eq!(observations.all_consumers_released(), Some(false));
        assert!(observations.begin(stage(4)).is_err());
        assert_eq!(observations.producer(), Some(stage(2)));
        observations.finish();
        observations.begin(stage(4)).unwrap();
        assert_eq!(observations.all_consumers_released(), None);
        observations
            .check_next(stage(1), true, &mut Turn(1))
            .unwrap();
        assert_eq!(observations.all_consumers_released(), Some(true));
        observations.finish();
        observations.begin(stage(2)).unwrap();
        observations.stage_changed(stage(1), false); // Old epoch's checked row is not adopted.
        observations
            .check_next(stage(1), true, &mut Turn(1))
            .unwrap();
        observations
            .check_next(stage(3), true, &mut Turn(1))
            .unwrap();
        assert_eq!(observations.all_consumers_released(), Some(true));
        assert_eq!(observations.state.borrow().checked.as_ptr(), allocation);
        assert_eq!(observations.state.borrow().checked.len(), 4);
    }
    #[test]
    fn stage_changes_coalesce_and_unknown_producer_or_wrong_row_cannot_authorize_empty_truth() {
        let observations = StageDemandObservations::new(&graph());
        for _ in 0..1000 {
            observations.stage_changed(stage(1), true);
        }
        assert!(observations.has_pending_parents());
        assert_eq!(observations.next_parent(), Some(stage(1)));
        assert_eq!(observations.next_parent(), None);
        observations.stage_changed(stage(3), true);
        observations.stage_changed(stage(3), false);
        assert!(!observations.has_pending_parents());
        assert!(observations.begin(stage(1)).is_err()); // Real stage, no frozen consumers.
        assert!(observations.begin(stage(99)).is_err());
        assert_eq!(observations.producer(), None);
        assert_eq!(observations.all_consumers_released(), None);
        observations.begin(stage(2)).unwrap();
        assert!(
            observations
                .check_next(stage(3), true, &mut Turn(1))
                .is_err()
        );
        assert_eq!(observations.next_consumer(), Some(stage(1)));
    }
    #[test]
    fn exhausted_local_proof_tag_refuses_without_wrapping_or_replacing_original_state() {
        let observations = StageDemandObservations::new(&graph());
        observations.state.borrow_mut().next_epoch = u128::MAX;
        assert!(observations.begin(stage(2)).is_err());
        assert_eq!(observations.producer(), None);
        assert_eq!(observations.state.borrow().next_epoch, u128::MAX);
    }
}
