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

//! Driver-owned K cursors. Dependency-blocked routes retain data custody;
//! each turn tries the original graph's eager claims before resuming a route.

use super::{build_pages::push_owned, build_state::build_contract};
use crate::{
    exec::{
        hash_table::key_index::{BulkClaim, BulkTask, BulkTaskAdvance, BulkTaskGraph, BulkWork},
        pipeline::step_context::TurnWork,
    },
    runtime::{
        fragment::{ExecutionFailure, ExecutionResult},
        observable::Observable,
    },
};
use novarocks_type_contract::ValueTypeStorageLifetime;

struct Retained {
    task: BulkTask,
    blocked: bool,
}
/// The original K graph bounds actual retained routes/workspaces and merge
/// positions. This container adds no count, byte, queue or scheduling quota.
pub(crate) struct KeyBuildWorker {
    retained: Vec<Retained>,
    next: usize,
    observed: u64,
    _storage: ValueTypeStorageLifetime,
    #[cfg(test)]
    claims: Vec<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyWorkerStep {
    Finished,
    Progressed,
    Blocked,
    Yielded,
}
impl KeyBuildWorker {
    pub(crate) fn new(work: &BulkWork<'_>) -> Self {
        Self {
            retained: Vec::new(),
            next: 0,
            observed: 0,
            _storage: work.owner.storage_lifetime(),
            #[cfg(test)]
            claims: Vec::new(),
        }
    }
    #[cfg(test)]
    pub(super) fn claim_phases(&self) -> &[String] {
        &self.claims
    }
    pub(crate) fn retained(&self) -> usize {
        self.retained.len()
    }
    pub(crate) fn step(
        &mut self,
        graph: &BulkTaskGraph,
        changed: &Observable,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<KeyWorkerStep> {
        self.step_inner(graph, changed, turn, work)
            .map_err(|cause| {
                let cause = graph.fail(cause);
                changed.notify_observers();
                cause
            })
    }
    fn step_inner(
        &mut self,
        graph: &BulkTaskGraph,
        changed: &Observable,
        turn: &mut TurnWork,
        work: &BulkWork<'_>,
    ) -> ExecutionResult<KeyWorkerStep> {
        work.control.checkpoint(0)?;
        if !turn.try_consume(1) {
            return Ok(KeyWorkerStep::Yielded);
        }
        let generation = changed.generation();
        if generation != self.observed {
            for retained in &mut self.retained {
                retained.blocked = false;
            }
            self.observed = generation;
        }
        // Even when every worker retains a route, K may issue an eager merge
        // using the same actual position. Never gate claim on retained.len().
        let pending = match graph.claim(work)? {
            BulkClaim::Task(task) => {
                #[cfg(test)]
                self.claims.push(task.phase_name_for_test());
                push_owned(
                    &mut self.retained,
                    Retained {
                        task,
                        blocked: false,
                    },
                    &mut self._storage,
                    work,
                )?;
                false
            }
            BulkClaim::Pending => true,
            BulkClaim::Finished => {
                if !self.retained.is_empty() {
                    return Err(build_contract(
                        "K reported finished with retained original worker cursors",
                    ));
                }
                return Ok(KeyWorkerStep::Finished);
            }
        };
        let mut progressed = !pending;
        while !self.retained.is_empty() && turn.try_consume(1) {
            let index = self.next % self.retained.len();
            self.next = (index + 1) % self.retained.len();
            match self.retained[index].task.advance(1, work)? {
                BulkTaskAdvance::Done => {
                    let retained = self.retained.swap_remove(index);
                    let mut completion = retained.task.finish()?;
                    graph.complete(&mut completion, work)?;
                    for task in &mut self.retained {
                        task.blocked = false;
                    }
                    changed.notify_observers();
                    self.observed = changed.generation();
                    progressed = true;
                }
                BulkTaskAdvance::Progressed => {
                    self.retained[index].blocked = false;
                    progressed = true;
                }
                BulkTaskAdvance::Blocked => {
                    self.retained[index].blocked = true;
                    if pending && self.retained.iter().all(|task| task.blocked) {
                        break;
                    }
                }
            }
        }
        if progressed {
            return Ok(KeyWorkerStep::Progressed);
        }
        // This is a predicate, not permission to sleep. Source registers its
        // original wakeup and rechecks graph/demand plus this generation.
        if pending && self.retained.iter().all(|task| task.blocked) {
            Ok(KeyWorkerStep::Blocked)
        } else {
            Ok(KeyWorkerStep::Yielded)
        }
    }
}

#[cfg(test)]
#[path = "key_build_worker_tests.rs"]
mod tests;
