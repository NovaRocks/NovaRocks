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

//! Fixed executors for original RPC getter and raw-source disposal jobs.
//! Their namespace and R bound come from the same Root's admitted reply index.
//! Queue drain is physical; only the original source role proves disposition.

use super::{
    control_request::ControlRequestReady,
    establish_ingress::{EstablishDisposalJob, EstablishIngressJob},
    host_executor::{
        HostExecutorHandle, HostExecutorLimits, HostExecutorPair, HostPairShutdownError,
        HostSubmitError,
    },
    preparation::{HostFairnessGroup, QueuedPreparation},
};
use novarocks_control_runtime_core::identity::{ScopeId, WorkId};
use std::num::NonZeroUsize;

enum Queued {
    Getter(EstablishIngressJob),
    Disposal(EstablishDisposalJob),
}
impl QueuedPreparation for Queued {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        match self {
            Self::Getter(job) => job.queue_key(),
            Self::Disposal(job) => job.queue_key(),
        }
    }
    fn fairness_group(&self, scope: ScopeId) -> HostFairnessGroup {
        match self {
            Self::Getter(job) => job.fairness_group(scope),
            Self::Disposal(job) => job.fairness_group(scope),
        }
    }
}
fn run(original: Queued) {
    match original {
        Queued::Getter(job) => job.run(),
        Queued::Disposal(job) => {
            // A false result leaves the original Source role Unresolved and
            // marks its original R. Root's ready consumer reports that fault;
            // returning here signs only the physical executor body return.
            let _source_exited = job.run();
        }
    }
}
fn limits(requests: NonZeroUsize, workers: NonZeroUsize) -> Option<HostExecutorLimits> {
    // At most one job per admitted R in each pool, plus N physical tails that
    // can outlive logical retirement. All R may name one Context, so neither
    // context count nor cumulative Task T is a valid per-group bound here.
    let bound = NonZeroUsize::new(requests.get().checked_add(workers.get())?)?;
    Some(HostExecutorLimits {
        contexts: bound,
        jobs: bound,
        per_context: bound,
        workers,
    })
}

#[derive(Debug)]
pub(crate) enum EstablishExecutorStartError {
    AlreadyStarted,
    GeometryOverflow,
    Spawn(std::io::Error),
}
#[must_use = "Partial startup retains the actual physical queues and workers"]
pub(crate) struct EstablishExecutorStartRefusal {
    pub error: EstablishExecutorStartError,
    pub owner: Option<Box<EstablishExecutors>>,
}
impl std::fmt::Debug for EstablishExecutorStartRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EstablishExecutorStartRefusal")
            .field("error", &self.error)
            .field("retains_owner", &self.owner.is_some())
            .finish()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishSubmitError {
    WrongOwner,
    Host(HostSubmitError),
}

#[must_use = "Drain original source producers before closing these physical pools"]
pub(crate) struct EstablishExecutors {
    ready: ControlRequestReady,
    pools: HostExecutorPair<Queued>,
}
impl EstablishExecutors {
    pub(super) fn start(
        ready: ControlRequestReady,
        getter_workers: NonZeroUsize,
        cleanup_workers: NonZeroUsize,
    ) -> Result<Self, EstablishExecutorStartRefusal> {
        let (Some(getters), Some(cleanup)) = (
            limits(ready.limit(), getter_workers),
            limits(ready.limit(), cleanup_workers),
        ) else {
            return Err(EstablishExecutorStartRefusal {
                error: EstablishExecutorStartError::GeometryOverflow,
                owner: None,
            });
        };
        match HostExecutorPair::start(getters, cleanup, run) {
            Ok(pools) => Ok(Self { ready, pools }),
            Err((error, pools)) => Err(EstablishExecutorStartRefusal {
                error: EstablishExecutorStartError::Spawn(error),
                owner: Some(Box::new(Self { ready, pools })),
            }),
        }
    }
    pub(crate) fn handles(&self) -> Option<EstablishExecutorHandles> {
        let (getters, cleanup) = self.pools.handles()?;
        Some(EstablishExecutorHandles {
            ready: self.ready.clone(),
            getters,
            cleanup,
        })
    }
    pub(crate) fn shutdown(self) -> Result<(), (HostPairShutdownError, Self)> {
        let Self { ready, pools } = self;
        pools
            .shutdown()
            .map_err(|(error, pools)| (error, Self { ready, pools }))
    }
}
#[derive(Clone)]
pub(crate) struct EstablishExecutorHandles {
    ready: ControlRequestReady,
    getters: HostExecutorHandle<Queued>,
    cleanup: HostExecutorHandle<Queued>,
}
impl EstablishExecutorHandles {
    pub(crate) fn try_submit_getter(
        &self,
        original: EstablishIngressJob,
    ) -> Result<(), (EstablishSubmitError, EstablishIngressJob)> {
        if !original.belongs_to(&self.ready) {
            return Err((EstablishSubmitError::WrongOwner, original));
        }
        self.getters
            .try_submit(Queued::Getter(original))
            .map_err(|(error, original)| {
                let Queued::Getter(original) = original else {
                    unreachable!("getter submission preserves its variant")
                };
                (EstablishSubmitError::Host(error), original)
            })
    }
    pub(crate) fn try_submit_disposal(
        &self,
        original: EstablishDisposalJob,
    ) -> Result<(), (EstablishSubmitError, EstablishDisposalJob)> {
        if !original.belongs_to(&self.ready) {
            return Err((EstablishSubmitError::WrongOwner, original));
        }
        self.cleanup
            .try_submit(Queued::Disposal(original))
            .map_err(|(error, original)| {
                let Queued::Disposal(original) = original else {
                    unreachable!("disposal submission preserves its variant")
                };
                (EstablishSubmitError::Host(error), original)
            })
    }
}
