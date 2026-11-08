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

//! Graph-bounded, charged expansion of accepted destination facts. No queued
//! transport position or I/O capability is owned by these pure cursors.
use super::EdgeOpenTracker;
use crate::{capacity::StepContext, error::TaskExecutionError};
use novarocks_execution_contract::{ExchangeEdgeId, TaskIdentity};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DerivedEdgeEffect {
    Close {
        producer: TaskIdentity,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
    },
    Open {
        producer: TaskIdentity,
        edge: ExchangeEdgeId,
    },
}
impl DerivedEdgeEffect {
    pub(crate) const fn producer(self) -> TaskIdentity {
        match self {
            Self::Close { producer, .. } | Self::Open { producer, .. } => producer,
        }
    }
}
#[derive(Clone, Copy, Debug, Default)]
struct DestinationFacts {
    created: bool,
    normally_closed: bool,
}
struct DestinationCursor {
    destination: TaskIdentity,
    facts: DestinationFacts,
    normal_pass: bool,
    edge_index: usize,
}
struct CloseCursor {
    edge: ExchangeEdgeId,
    destination: TaskIdentity,
    producer: usize,
}
struct OpenCursor {
    edge: ExchangeEdgeId,
    producer: usize,
}
#[derive(Default)]
pub(crate) struct EdgeDerivations {
    seen: BTreeMap<TaskIdentity, DestinationFacts>,
    pending: BTreeMap<TaskIdentity, DestinationFacts>,
    destination: Option<DestinationCursor>,
    closes: VecDeque<CloseCursor>,
    opens: VecDeque<OpenCursor>,
}
impl std::fmt::Debug for EdgeDerivations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeDerivations")
            .field("pending_destinations", &self.pending.len())
            .field("current_destination", &self.destination.is_some())
            .field("close_edges", &self.closes.len())
            .field("open_edges", &self.opens.len())
            .finish()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EdgeDerivationStep {
    Idle,
    BudgetExhausted,
    Progress,
    Effect(DerivedEdgeEffect),
}
impl EdgeDerivations {
    pub(crate) fn created(&mut self, destination: TaskIdentity) {
        self.remember(destination, true, false);
    }
    pub(crate) fn normally_closed(&mut self, destination: TaskIdentity) {
        self.remember(destination, false, true);
    }
    fn remember(&mut self, destination: TaskIdentity, created: bool, normally_closed: bool) {
        let seen = self.seen.entry(destination).or_default();
        let delta = DestinationFacts {
            created: created && !seen.created,
            normally_closed: normally_closed && !seen.normally_closed,
        };
        seen.created |= created;
        seen.normally_closed |= normally_closed;
        if delta.created || delta.normally_closed {
            let pending = self.pending.entry(destination).or_default();
            pending.created |= delta.created;
            pending.normally_closed |= delta.normally_closed;
        }
    }
    pub(crate) fn has_pending(&self) -> bool {
        self.destination.is_some()
            || !self.pending.is_empty()
            || !self.closes.is_empty()
            || !self.opens.is_empty()
    }
    /// One call pays at most one cursor member/fanout unit. Known destination
    /// changes and close fanout always precede an unemitted open; an edge that
    /// was opened earlier still accepts later exact destination withdrawal.
    pub(crate) fn advance(
        &mut self,
        tracker: &mut EdgeOpenTracker,
        allow_open: bool,
        context: &mut dyn StepContext,
    ) -> Result<EdgeDerivationStep, TaskExecutionError> {
        if !self.has_pending() {
            return Ok(EdgeDerivationStep::Idle);
        }
        if !context.charge(1) {
            return Ok(EdgeDerivationStep::BudgetExhausted);
        }
        if let Some(mut cursor) = self.destination.take() {
            let count = tracker.destination_edge_count(cursor.destination);
            if cursor.edge_index == count {
                if cursor.normal_pass && cursor.facts.created {
                    cursor.normal_pass = false;
                    cursor.edge_index = 0;
                    self.destination = Some(cursor);
                }
                return Ok(EdgeDerivationStep::Progress);
            }
            let edge = tracker
                .destination_edge(cursor.destination, cursor.edge_index)
                .expect("frozen edge incidence");
            let delta =
                tracker.resolve_edge_member(cursor.destination, edge, cursor.normal_pass)?;
            if delta.closed {
                self.closes.push_back(CloseCursor {
                    edge,
                    destination: cursor.destination,
                    producer: 0,
                });
            }
            if delta.newly_opened {
                self.opens.push_back(OpenCursor { edge, producer: 0 });
            }
            cursor.edge_index += 1;
            self.destination = Some(cursor);
            return Ok(EdgeDerivationStep::Progress);
        }
        if let Some((destination, facts)) = self.pending.pop_first() {
            self.destination = Some(DestinationCursor {
                destination,
                normal_pass: facts.normally_closed,
                facts,
                edge_index: 0,
            });
            return Ok(EdgeDerivationStep::Progress);
        }
        if let Some(cursor) = self.closes.front_mut() {
            let Some(producer) = tracker.producer_at(cursor.edge, cursor.producer) else {
                self.closes.pop_front();
                return Ok(EdgeDerivationStep::Progress);
            };
            cursor.producer += 1;
            return Ok(EdgeDerivationStep::Effect(DerivedEdgeEffect::Close {
                producer,
                edge: cursor.edge,
                destination: cursor.destination,
            }));
        }
        if !allow_open {
            self.opens.pop_front();
            return Ok(EdgeDerivationStep::Progress);
        }
        let cursor = self.opens.front_mut().expect("remaining opening cursor");
        let Some(producer) = tracker.producer_at(cursor.edge, cursor.producer) else {
            self.opens.pop_front();
            return Ok(EdgeDerivationStep::Progress);
        };
        cursor.producer += 1;
        Ok(EdgeDerivationStep::Effect(DerivedEdgeEffect::Open {
            producer,
            edge: cursor.edge,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        capacity::QueueStage,
        config::DispatchLane,
        graph::TaskGraph,
        start::{FrozenCreation, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph, PayloadRef},
    };
    use novarocks_execution_contract::{ExchangeDeliveryRequirement, ParticipationRef, PlanNodeId};
    use novarocks_types::{BackendProcessId, StageId, TaskId, UniqueId};
    use std::num::NonZeroU64;
    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, units: usize) -> bool {
            if self.0 < units {
                false
            } else {
                self.0 -= units;
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
            panic!("pure graph derivation never acquires a transport window");
        }
    }
    fn shared_producers() -> TaskGraph {
        let sample = crate::test_support::remote(1);
        let execution = sample.identity().query_execution_id();
        let frontend = sample.participation().frontend_process_id();
        let mut tasks = BTreeMap::new();
        let mut stages = BTreeMap::<StageId, Vec<TaskIdentity>>::new();
        let mut ids = Vec::new();
        for value in 1..=4 {
            let stage = StageId::new(if value <= 2 { 1 } else { 2 }).unwrap();
            let task = TaskId::new(value).unwrap();
            let identity = TaskIdentity::new(
                execution,
                stage,
                task,
                crate::test_support::backend(u64::from(value)),
            );
            ids.push(identity);
            tasks.insert(
                task,
                FrozenTask {
                    identity,
                    participation: ParticipationRef::new(
                        execution,
                        frontend,
                        identity.backend_process_id(),
                    ),
                    task_key: UniqueId::new(i64::from(value), 1),
                    fragment_id: stage.get() as i32,
                    split_plan_nodes: Box::new([]),
                    creation: FrozenCreation {
                        resource: PayloadRef(NonZeroU64::new(u64::from(value)).unwrap()),
                        queued_bytes: 256,
                        plan_carrier_bytes: 0,
                        descriptor_encoded_bytes: 0,
                    },
                },
            );
            stages.entry(stage).or_default().push(identity);
        }
        let mut edges = BTreeMap::new();
        for value in 1..=2 {
            let edge = ExchangeEdgeId::new(value).unwrap();
            edges.insert(
                edge,
                FrozenExchangeEdge {
                    edge,
                    producer_stage: ids[2].stage_id(),
                    consumer_stage: ids[0].stage_id(),
                    destination_node: PlanNodeId::new(value as i32).unwrap(),
                    delivery: ExchangeDeliveryRequirement::DemandDriven,
                    producers: ids[2..].into(),
                    destinations: Box::from([ids[value as usize - 1]]),
                },
            );
        }
        TaskGraph::from_frozen(FrozenTaskGraph {
            root: ids[0],
            tasks,
            stages: stages
                .into_iter()
                .map(|(key, value)| (key, value.into_boxed_slice()))
                .collect(),
            edges,
        })
        .unwrap()
    }
    fn drain(
        work: &mut EdgeDerivations,
        tracker: &mut EdgeOpenTracker,
        allow_open: bool,
    ) -> Vec<DerivedEdgeEffect> {
        let mut result = Vec::new();
        for _ in 0..1000 {
            let mut turn = Turn(1);
            match work.advance(tracker, allow_open, &mut turn).unwrap() {
                EdgeDerivationStep::Idle => {
                    assert!(!work.has_pending());
                    return result;
                }
                EdgeDerivationStep::Effect(effect) => result.push(effect),
                EdgeDerivationStep::Progress => {}
                EdgeDerivationStep::BudgetExhausted => panic!("one real member is affordable"),
            }
            assert_eq!(turn.0, 0);
            assert_eq!(
                work.advance(tracker, allow_open, &mut turn).unwrap(),
                if work.has_pending() {
                    EdgeDerivationStep::BudgetExhausted
                } else {
                    EdgeDerivationStep::Idle
                }
            );
        }
        panic!("finite graph work must drain");
    }
    #[test]
    fn shared_producer_receives_every_known_close_before_open_under_single_unit_turns() {
        let graph = shared_producers();
        let mut tracker = EdgeOpenTracker::from_graph(&graph);
        let mut work = EdgeDerivations::default();
        let destinations = graph
            .tasks()
            .take(2)
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        for &destination in &destinations {
            work.created(destination);
            work.normally_closed(destination);
        }
        let trace = drain(&mut work, &mut tracker, true);
        assert_eq!(trace.len(), 8);
        assert!(
            trace[..4]
                .iter()
                .all(|effect| matches!(effect, DerivedEdgeEffect::Close { .. }))
        );
        assert!(
            trace[4..]
                .iter()
                .all(|effect| matches!(effect, DerivedEdgeEffect::Open { .. }))
        );
        for producer in graph.tasks().skip(2).map(|task| task.identity()) {
            for value in 1..=2 {
                let edge = ExchangeEdgeId::new(value).unwrap();
                let destination = destinations[value as usize - 1];
                assert_eq!(
                    trace
                        .iter()
                        .filter(|&&effect| effect
                            == DerivedEdgeEffect::Close {
                                producer,
                                edge,
                                destination
                            })
                        .count(),
                    1
                );
                assert_eq!(
                    trace
                        .iter()
                        .filter(|&&effect| effect == DerivedEdgeEffect::Open { producer, edge })
                        .count(),
                    1
                );
            }
        }
    }
    #[test]
    fn decided_edge_still_emits_later_exact_close_and_duplicate_input_adds_no_work() {
        let graph = shared_producers();
        let root = graph.root_identity();
        let mut tracker = EdgeOpenTracker::from_graph(&graph);
        let mut work = EdgeDerivations::default();
        work.created(root);
        let opened = drain(&mut work, &mut tracker, true);
        assert_eq!(opened.len(), 2);
        assert!(
            opened
                .iter()
                .all(|effect| matches!(effect, DerivedEdgeEffect::Open { .. }))
        );
        work.normally_closed(root);
        let closed = drain(&mut work, &mut tracker, true);
        assert_eq!(closed.len(), 2);
        assert!(
            closed
                .iter()
                .all(|effect| matches!(effect, DerivedEdgeEffect::Close { .. }))
        );
        for _ in 0..10_000 {
            work.created(root);
            work.normally_closed(root);
        }
        assert!(!work.has_pending());
    }
    #[test]
    fn ordinary_withdrawal_never_opens_or_closes_strict_input_without_real_installation() {
        let (graph, _) = crate::test_support::graph_with_strict_input(false);
        let mut tracker = EdgeOpenTracker::from_graph(&graph);
        let mut work = EdgeDerivations::default();
        let destination = graph.task(TaskId::new(2).unwrap()).unwrap().identity();
        work.normally_closed(destination);
        assert!(drain(&mut work, &mut tracker, true).is_empty());
        work.created(destination);
        let trace = drain(&mut work, &mut tracker, true);
        assert_eq!(trace.len(), 1);
        assert_eq!(
            trace[0],
            DerivedEdgeEffect::Open {
                producer: graph.task(TaskId::new(3).unwrap()).unwrap().identity(),
                edge: ExchangeEdgeId::new(2).unwrap()
            }
        );
    }
    #[test]
    fn normal_drain_discards_unemitted_opens_but_retains_original_close_fanout() {
        let graph = shared_producers();
        let mut tracker = EdgeOpenTracker::from_graph(&graph);
        let mut work = EdgeDerivations::default();
        work.created(graph.root_identity());
        work.normally_closed(graph.root_identity());
        let trace = drain(&mut work, &mut tracker, false);
        assert_eq!(trace.len(), 2);
        assert!(
            trace
                .iter()
                .all(|effect| matches!(effect, DerivedEdgeEffect::Close { .. }))
        );
        let unknown = ExchangeEdgeId::new(999).unwrap();
        assert!(
            tracker
                .resolve_edge_member(graph.root_identity(), unknown, true)
                .is_err()
        );
    }
}
