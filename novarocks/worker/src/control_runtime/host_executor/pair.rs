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

//! Physical ordinary/cleanup ownership, shared by typed host adapters. It
//! creates no logical work, signs no exit and makes no protocol decision.

use super::{HostExecutor, HostExecutorHandle, HostExecutorLimits, HostShutdownError};
use crate::control_runtime::preparation::QueuedPreparation;

type Pool<J> = HostExecutor<J, fn(J)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostPairShutdownError {
    Ordinary(HostShutdownError),
    Cleanup(HostShutdownError),
}

#[must_use = "Drain both original producer domains before closing these physical pools"]
pub(crate) struct HostExecutorPair<J: QueuedPreparation> {
    ordinary: Option<Pool<J>>,
    cleanup: Option<Pool<J>>,
    startup_complete: bool,
}
impl<J: QueuedPreparation> HostExecutorPair<J> {
    /// Fixed runner functions carry no opaque captures to dispose on failure.
    /// Partial startup returns every actual queue/thread owner. Only complete
    /// startup publishes operational handles.
    pub(crate) fn start(
        ordinary: HostExecutorLimits,
        cleanup: HostExecutorLimits,
        run: fn(J),
    ) -> Result<Self, (std::io::Error, Self)> {
        Self::start_inner(ordinary, cleanup, run, None)
    }
    pub(crate) fn start_with_waker(
        ordinary: HostExecutorLimits,
        cleanup: HostExecutorLimits,
        run: fn(J),
        waker: std::task::Waker,
    ) -> Result<Self, (std::io::Error, Self)> {
        Self::start_inner(ordinary, cleanup, run, Some(waker))
    }
    fn start_inner(
        ordinary: HostExecutorLimits,
        cleanup: HostExecutorLimits,
        run: fn(J),
        waker: Option<std::task::Waker>,
    ) -> Result<Self, (std::io::Error, Self)> {
        let mut owner = Self {
            ordinary: None,
            cleanup: None,
            startup_complete: false,
        };
        let started = match waker.as_ref() {
            Some(waker) => HostExecutor::start_with_waker(ordinary, run, waker.clone()),
            None => HostExecutor::start(ordinary, run),
        };
        match started {
            Ok(pool) => owner.ordinary = Some(pool),
            Err((error, pool)) => {
                owner.ordinary = Some(pool);
                return Err((error, owner));
            }
        }
        let started = match waker {
            Some(waker) => HostExecutor::start_with_waker(cleanup, run, waker),
            None => HostExecutor::start(cleanup, run),
        };
        match started {
            Ok(pool) => owner.cleanup = Some(pool),
            Err((error, pool)) => {
                owner.cleanup = Some(pool);
                return Err((error, owner));
            }
        }
        owner.startup_complete = true;
        Ok(owner)
    }
    pub(crate) fn handles(&self) -> Option<(HostExecutorHandle<J>, HostExecutorHandle<J>)> {
        if !self.startup_complete {
            return None;
        }
        Some((
            self.ordinary.as_ref()?.handle(),
            self.cleanup.as_ref()?.handle(),
        ))
    }
    /// Keep cleanup operational while ordinary bodies drain and join. Any
    /// error returns the same partial owner; it cannot expose fresh handles
    /// or disguise unresolved job custody as a successful shutdown.
    pub(crate) fn shutdown(mut self) -> Result<(), (HostPairShutdownError, Self)> {
        self.startup_complete = false;
        if let Some(pool) = self.ordinary.take() {
            match pool.shutdown() {
                Ok(runner) => drop(runner),
                Err((error, pool)) => {
                    self.ordinary = Some(pool);
                    return Err((HostPairShutdownError::Ordinary(error), self));
                }
            }
        }
        if let Some(pool) = self.cleanup.take() {
            match pool.shutdown() {
                Ok(runner) => drop(runner),
                Err((error, pool)) => {
                    self.cleanup = Some(pool);
                    return Err((HostPairShutdownError::Cleanup(error), self));
                }
            }
        }
        Ok(())
    }
}
