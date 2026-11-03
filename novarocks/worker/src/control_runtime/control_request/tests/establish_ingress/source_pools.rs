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

//! Original source roles remain the oracle; physical pools only run jobs.

use super::*;
use crate::control_runtime::establish_executors::{EstablishExecutorHandles, EstablishExecutors};
use crate::control_runtime::establish_ingress::EstablishDisposalJob;
use std::time::Instant;

const WATCHDOG: Duration = Duration::from_secs(5);

mod owner_turn;
mod tests;

fn join(owner: EstablishExecutors) {
    if let Err((reason, _original)) = owner.shutdown() {
        eprintln!("source pool shutdown retains an original owner: {reason:?}");
        std::process::abort();
    }
}

pub(super) struct SourcePoolGuard {
    owner: Option<EstablishExecutors>,
    handles: Option<EstablishExecutorHandles>,
    refused_getters: Vec<EstablishIngressJob>,
    refused_disposals: Vec<EstablishDisposalJob>,
}
impl SourcePoolGuard {
    pub(super) fn handles(&self) -> &EstablishExecutorHandles {
        self.handles.as_ref().unwrap()
    }
    pub(super) fn start(f: &mut Fixture) -> Self {
        let owner = match f
            .root
            .start_establish_executors(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap())
        {
            Ok(owner) => owner,
            Err(refusal) => {
                if let Some(partial) = refusal.owner {
                    join(*partial);
                }
                panic!("source physical startup refused: {:?}", refusal.error);
            }
        };
        let handles = owner.handles().unwrap();
        Self {
            owner: Some(owner),
            handles: Some(handles),
            refused_getters: Vec::new(),
            refused_disposals: Vec::new(),
        }
    }
    pub(super) fn getter(&mut self, original: EstablishIngressJob) {
        if let Err((error, original)) = self.handles.as_ref().unwrap().try_submit_getter(original) {
            self.refused_getters.push(original);
            panic!("original source getter submission refused: {error:?}");
        }
    }
    pub(super) fn disposal(&mut self, original: EstablishDisposalJob) {
        if let Err((error, original)) = self.handles.as_ref().unwrap().try_submit_disposal(original)
        {
            self.refused_disposals.push(original);
            panic!("original source disposal submission refused: {error:?}");
        }
    }
    pub(super) fn finish(mut self) {
        self.close_and_join();
    }
    fn close_and_join(&mut self) {
        self.handles = None;
        if let Some(owner) = self.owner.take() {
            join(owner);
        }
        if !self.refused_getters.is_empty() || !self.refused_disposals.is_empty() {
            eprintln!("source pool test retains refused original jobs");
            std::process::abort();
        }
    }
}
impl Drop for SourcePoolGuard {
    fn drop(&mut self) {
        self.close_and_join();
    }
}

pub(super) fn wait_ingress(f: &mut Fixture, input: InputId) -> EstablishIngressOutcome {
    let deadline = Instant::now() + WATCHDOG;
    loop {
        let version = f.runtime.change_version();
        if let Some(outcome) = f.root.take_establish_ingress(input).unwrap() {
            return outcome;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "original getter return watchdog expired"
        );
        f.runtime.wait_after_timeout(version, remaining);
    }
}
pub(super) fn wait_disposal(f: &Fixture, input: InputId) -> ExitCustodyPhase {
    let deadline = Instant::now() + WATCHDOG;
    loop {
        let version = f.runtime.change_version();
        let phase = f.root.establish_source_phase(input).unwrap().unwrap();
        if matches!(
            phase,
            ExitCustodyPhase::Exited | ExitCustodyPhase::Retired | ExitCustodyPhase::Unresolved
        ) {
            return phase;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "original source disposition watchdog expired"
        );
        f.runtime.wait_after_timeout(version, remaining);
    }
}
