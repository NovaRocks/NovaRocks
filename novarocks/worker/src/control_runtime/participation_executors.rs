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

//! Separate finite physical pools for Context materialization and cleanup.
//! A blocked M cannot occupy R/H's worker. Every submission moves the original
//! admitted job; queue ownership and thread join never stand in for role exit.

use super::{
    actors::WorkerActor,
    host_executor::{
        HostExecutorHandle, HostExecutorLimits, HostExecutorPair, HostPairShutdownError,
        HostShutdownError, HostSubmitError,
    },
    participation_host_jobs::{ParticipationHostJob, ParticipationHostJobs},
    preparation::{HostFairnessGroup, QueuedPreparation},
    task_capacity::TaskCapacitySignal,
};
use novarocks_control_runtime::{actor_turn::ActorRuntime, transaction::RuntimeError};
use novarocks_control_runtime_core::identity::{ActorId, ScopeId, WorkId};

struct Queued(ParticipationHostJob);
impl QueuedPreparation for Queued {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.0.queue_key()
    }
    fn fairness_group(&self, scope: ScopeId) -> HostFairnessGroup {
        self.0.fairness_group(scope)
    }
}
fn run(original: Queued) {
    original.0.run_on_host();
}

#[derive(Debug)]
pub(crate) enum ParticipationExecutorStartError {
    AlreadyStarted,
    Spawn(std::io::Error),
}
impl std::fmt::Display for ParticipationExecutorStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyStarted => {
                f.write_str("Participation host executors have already started")
            }
            Self::Spawn(error) => {
                write!(
                    f,
                    "Participation host executor thread could not start: {error}"
                )
            }
        }
    }
}
impl std::error::Error for ParticipationExecutorStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyStarted => None,
            Self::Spawn(error) => Some(error),
        }
    }
}

#[must_use = "A partial startup retains its actual physical workers and queues"]
pub(crate) struct ParticipationExecutorStartRefusal {
    pub error: ParticipationExecutorStartError,
    pub owner: Option<Box<ParticipationExecutors>>,
}
impl std::fmt::Debug for ParticipationExecutorStartRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParticipationExecutorStartRefusal")
            .field("error", &self.error)
            .field("retains_owner", &self.owner.is_some())
            .finish()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationSubmitError {
    WrongOwner,
    StopRequired,
    Runtime(RuntimeError),
    Host(HostSubmitError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationExecutorShutdownError {
    Materialization(HostShutdownError),
    Cleanup(HostShutdownError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationCancelError {
    WrongOwner,
    StopRequired,
    Runtime(RuntimeError),
    Queue(novarocks_control_runtime_core::fair_queue::FairQueueError),
}

#[must_use = "Participation producers drain before closing and joining these original pools"]
pub(crate) struct ParticipationExecutors {
    runtime: ActorRuntime<WorkerActor>,
    control: ActorId,
    pools: HostExecutorPair<Queued>,
    capacity: TaskCapacitySignal,
}
impl ParticipationExecutors {
    /// Called by the registered Service before publishing a request facade.
    /// Composition supplies actual bounded queue and worker windows. A busy
    /// queue retains the original Context job; directory size grants no slot.
    pub(super) fn start(
        runtime: ActorRuntime<WorkerActor>,
        control: ActorId,
        materialization: HostExecutorLimits,
        cleanup: HostExecutorLimits,
        capacity: TaskCapacitySignal,
    ) -> Result<Self, ParticipationExecutorStartRefusal> {
        match HostExecutorPair::start_with_waker(materialization, cleanup, run, capacity.waker()) {
            Ok(pools) => Ok(Self {
                runtime,
                control,
                pools,
                capacity,
            }),
            Err((error, pools)) => Err(ParticipationExecutorStartRefusal {
                error: ParticipationExecutorStartError::Spawn(error),
                owner: Some(Box::new(Self {
                    runtime,
                    control,
                    pools,
                    capacity,
                })),
            }),
        }
    }
    /// A partially started owner never yields an operational pair of handles.
    pub(crate) fn handles(&self) -> Option<ParticipationExecutorHandles> {
        let (materialization, cleanup) = self.pools.handles()?;
        Some(ParticipationExecutorHandles {
            runtime: self.runtime.clone(),
            control: self.control,
            materialization,
            cleanup,
            capacity: self.capacity.clone(),
        })
    }
    /// Caller has first drained the real Context scopes and producer leases.
    /// Cleanup stays open while the original materialization pool joins. A
    /// failed join returns every remaining physical owner rather than Drop.
    pub(crate) fn shutdown(self) -> Result<(), (ParticipationExecutorShutdownError, Self)> {
        let Self {
            runtime,
            control,
            pools,
            capacity,
        } = self;
        pools.shutdown().map_err(|(error, pools)| {
            let error = match error {
                HostPairShutdownError::Ordinary(error) => {
                    ParticipationExecutorShutdownError::Materialization(error)
                }
                HostPairShutdownError::Cleanup(error) => {
                    ParticipationExecutorShutdownError::Cleanup(error)
                }
            };
            (
                error,
                Self {
                    runtime,
                    control,
                    pools,
                    capacity,
                },
            )
        })
    }
}
#[derive(Clone)]
pub(crate) struct ParticipationExecutorHandles {
    runtime: ActorRuntime<WorkerActor>,
    control: ActorId,
    materialization: HostExecutorHandle<Queued>,
    cleanup: HostExecutorHandle<Queued>,
    capacity: TaskCapacitySignal,
}
impl ParticipationExecutorHandles {
    #[cfg(test)]
    pub(crate) fn original_materialization_queue_snapshot(
        &self,
    ) -> Result<
        Option<super::preparation::PreparationQueueSnapshot>,
        super::preparation::PreparationQueueSnapshotError,
    > {
        self.materialization.try_preparation_snapshot()
    }
    pub(crate) fn capacity_signal(&self) -> &TaskCapacitySignal {
        &self.capacity
    }
    pub(super) fn accepts_owner(&self, owner: &ParticipationHostJobs) -> bool {
        owner.belongs_to(&self.runtime, self.control)
    }
    pub(crate) fn try_submit(
        &self,
        job: ParticipationHostJob,
    ) -> Result<(), (ParticipationSubmitError, ParticipationHostJob)> {
        if !job.belongs_to(&self.runtime, self.control) {
            return Err((ParticipationSubmitError::WrongOwner, job));
        }
        let pool = if job.is_materialization() {
            &self.materialization
        } else {
            &self.cleanup
        };
        pool.try_submit(Queued(job))
            .map_err(|(error, original)| (ParticipationSubmitError::Host(error), original.0))
    }
    /// An already stopped queued M can leave a saturated ordinary pool without
    /// running business I/O. The monotonic actual Scope gate qualifies this
    /// transfer; the original role still returns its captures and report.
    pub(crate) fn try_submit_stopped(
        &self,
        job: ParticipationHostJob,
    ) -> Result<(), (ParticipationSubmitError, ParticipationHostJob)> {
        if !job.belongs_to(&self.runtime, self.control) {
            return Err((ParticipationSubmitError::WrongOwner, job));
        }
        if job.is_materialization() {
            match job.scope_stopped() {
                Ok(true) => {}
                Ok(false) => return Err((ParticipationSubmitError::StopRequired, job)),
                Err(error) => return Err((ParticipationSubmitError::Runtime(error), job)),
            }
        }
        self.cleanup
            .try_submit(Queued(job))
            .map_err(|(error, original)| (ParticipationSubmitError::Host(error), original.0))
    }
    /// The actual Context owner supplies its still-counted original Work key.
    /// Refusal returns the same queued job; cancellation alone proves no exit.
    pub(crate) fn cancel_materialization(
        &self,
        owner: &ParticipationHostJobs,
    ) -> Result<ParticipationHostJob, ParticipationCancelError> {
        if !owner.belongs_to(&self.runtime, self.control) {
            return Err(ParticipationCancelError::WrongOwner);
        }
        if !owner
            .scope_stopped()
            .map_err(ParticipationCancelError::Runtime)?
        {
            return Err(ParticipationCancelError::StopRequired);
        }
        let (scope, work) = owner.materialization_key();
        self.materialization
            .cancel_queued(scope, work)
            .map(|job| job.0)
            .map_err(ParticipationCancelError::Queue)
    }
}
