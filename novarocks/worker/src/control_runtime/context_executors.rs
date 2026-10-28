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
    context_host_jobs::{ContextHostJob, ContextHostJobs},
    host_executor::{
        HostExecutorHandle, HostExecutorLimits, HostExecutorPair, HostPairShutdownError,
        HostShutdownError, HostSubmitError,
    },
    preparation::{HostFairnessGroup, QueuedPreparation},
};
use novarocks_control_runtime::{driver::ActorRuntime, transaction::RuntimeError};
use novarocks_control_runtime_core::identity::{ActorId, ScopeId, WorkId};
use std::num::NonZeroUsize;

struct Queued(ContextHostJob);
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
pub(crate) enum ContextExecutorStartError {
    AlreadyStarted,
    GeometryOverflow,
    Spawn(std::io::Error),
}
#[must_use = "A partial startup retains its actual physical workers and queues"]
pub(crate) struct ContextExecutorStartRefusal {
    pub error: ContextExecutorStartError,
    pub owner: Option<Box<ContextExecutors>>,
}
impl std::fmt::Debug for ContextExecutorStartRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextExecutorStartRefusal")
            .field("error", &self.error)
            .field("retains_owner", &self.owner.is_some())
            .finish()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextSubmitError {
    WrongOwner,
    StopRequired,
    Runtime(RuntimeError),
    Host(HostSubmitError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextExecutorShutdownError {
    Materialization(HostShutdownError),
    Cleanup(HostShutdownError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextCancelError {
    WrongOwner,
    StopRequired,
    Runtime(RuntimeError),
    Queue(novarocks_control_runtime_core::fair_queue::FairQueueError),
}

/// Fixed physical tails can briefly outlive their original reported role.
/// Cover all C admitted original call holders plus at most N running tails;
/// the logical per-Context role bound is one M or the two successive R/H.
fn limits(
    contexts: NonZeroUsize,
    workers: NonZeroUsize,
    roles: usize,
) -> Option<HostExecutorLimits> {
    Some(HostExecutorLimits {
        contexts: NonZeroUsize::new(contexts.get().checked_add(workers.get())?)?,
        jobs: NonZeroUsize::new(
            contexts
                .get()
                .checked_mul(roles)?
                .checked_add(workers.get())?,
        )?,
        per_context: NonZeroUsize::new(roles.checked_add(workers.get())?)?,
        workers,
    })
}
#[must_use = "Context producers drain before closing and joining these original pools"]
pub(crate) struct ContextExecutors {
    runtime: ActorRuntime<WorkerActor>,
    control: ActorId,
    pools: HostExecutorPair<Queued>,
}
impl ContextExecutors {
    /// Called by the registered Service before publishing a request facade.
    /// The Context bound comes from that same Control's actual directory.
    pub(super) fn start(
        runtime: ActorRuntime<WorkerActor>,
        control: ActorId,
        contexts: NonZeroUsize,
        materialization_workers: NonZeroUsize,
        cleanup_workers: NonZeroUsize,
    ) -> Result<Self, ContextExecutorStartRefusal> {
        let (Some(m), Some(c)) = (
            limits(contexts, materialization_workers, 1),
            limits(contexts, cleanup_workers, 2),
        ) else {
            return Err(ContextExecutorStartRefusal {
                error: ContextExecutorStartError::GeometryOverflow,
                owner: None,
            });
        };
        match HostExecutorPair::start(m, c, run) {
            Ok(pools) => Ok(Self {
                runtime,
                control,
                pools,
            }),
            Err((error, pools)) => Err(ContextExecutorStartRefusal {
                error: ContextExecutorStartError::Spawn(error),
                owner: Some(Box::new(Self {
                    runtime,
                    control,
                    pools,
                })),
            }),
        }
    }
    /// A partially started owner never yields an operational pair of handles.
    pub(crate) fn handles(&self) -> Option<ContextExecutorHandles> {
        let (materialization, cleanup) = self.pools.handles()?;
        Some(ContextExecutorHandles {
            runtime: self.runtime.clone(),
            control: self.control,
            materialization,
            cleanup,
        })
    }
    /// Caller has first drained the real Context scopes and producer leases.
    /// Cleanup stays open while the original materialization pool joins. A
    /// failed join returns every remaining physical owner rather than Drop.
    pub(crate) fn shutdown(self) -> Result<(), (ContextExecutorShutdownError, Self)> {
        let Self {
            runtime,
            control,
            pools,
        } = self;
        pools.shutdown().map_err(|(error, pools)| {
            let error = match error {
                HostPairShutdownError::Ordinary(error) => {
                    ContextExecutorShutdownError::Materialization(error)
                }
                HostPairShutdownError::Cleanup(error) => {
                    ContextExecutorShutdownError::Cleanup(error)
                }
            };
            (
                error,
                Self {
                    runtime,
                    control,
                    pools,
                },
            )
        })
    }
}
#[derive(Clone)]
pub(crate) struct ContextExecutorHandles {
    runtime: ActorRuntime<WorkerActor>,
    control: ActorId,
    materialization: HostExecutorHandle<Queued>,
    cleanup: HostExecutorHandle<Queued>,
}
impl ContextExecutorHandles {
    pub(super) fn accepts_owner(&self, owner: &ContextHostJobs) -> bool {
        owner.belongs_to(&self.runtime, self.control)
    }
    pub(crate) fn try_submit(
        &self,
        job: ContextHostJob,
    ) -> Result<(), (ContextSubmitError, ContextHostJob)> {
        if !job.belongs_to(&self.runtime, self.control) {
            return Err((ContextSubmitError::WrongOwner, job));
        }
        let pool = if job.is_materialization() {
            &self.materialization
        } else {
            &self.cleanup
        };
        pool.try_submit(Queued(job))
            .map_err(|(error, original)| (ContextSubmitError::Host(error), original.0))
    }
    /// An already stopped queued M can leave a saturated ordinary pool without
    /// running business I/O. The monotonic actual Scope gate qualifies this
    /// transfer; the original role still returns its captures and report.
    pub(crate) fn try_submit_stopped(
        &self,
        job: ContextHostJob,
    ) -> Result<(), (ContextSubmitError, ContextHostJob)> {
        if !job.belongs_to(&self.runtime, self.control) {
            return Err((ContextSubmitError::WrongOwner, job));
        }
        if job.is_materialization() {
            match job.scope_stopped() {
                Ok(true) => {}
                Ok(false) => return Err((ContextSubmitError::StopRequired, job)),
                Err(error) => return Err((ContextSubmitError::Runtime(error), job)),
            }
        }
        self.cleanup
            .try_submit(Queued(job))
            .map_err(|(error, original)| (ContextSubmitError::Host(error), original.0))
    }
    /// The actual Context owner supplies its still-counted original Work key.
    /// Refusal returns the same queued job; cancellation alone proves no exit.
    pub(crate) fn cancel_materialization(
        &self,
        owner: &ContextHostJobs,
    ) -> Result<ContextHostJob, ContextCancelError> {
        if !owner.belongs_to(&self.runtime, self.control) {
            return Err(ContextCancelError::WrongOwner);
        }
        if !owner.scope_stopped().map_err(ContextCancelError::Runtime)? {
            return Err(ContextCancelError::StopRequired);
        }
        let (scope, work) = owner.materialization_key();
        self.materialization
            .cancel_queued(scope, work)
            .map(|job| job.0)
            .map_err(ContextCancelError::Queue)
    }
}
