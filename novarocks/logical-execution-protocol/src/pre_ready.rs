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

//! The SAME original refusal's observation window and one latest watch per
//! activation. Queries never produce hints; only predicate changes do.

#![cfg_attr(not(test), expect(dead_code, reason = "wired in the cutover commit"))]

use crate::recovery::{RecoveryWindowInput, recovery_window_refusal};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptRequestToken, ExecutionFailureInput,
};
use novarocks_types::QueryExecutionId;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Unbounded},
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct PreReadyOpportunity {
    pub current_running: Option<AttemptActivationIdentity>,
    pub cancellation_pending: bool,
    pub concluded: bool,
    pub window: RecoveryWindowInput,
}
impl PreReadyOpportunity {
    pub(crate) fn allows(
        self,
        activation: AttemptActivationIdentity,
        ledger_correlated: bool,
    ) -> bool {
        self.current_running == Some(activation)
            && !self.cancellation_pending
            && !self.concluded
            && ledger_correlated
            && recovery_window_refusal(self.window).is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PreReadyObservationError {
    StaleActivation,
    ConflictingRefusal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PreReadyChange {
    pub activation: AttemptActivationIdentity,
    pub token: AttemptRequestToken,
}

#[derive(Debug)]
struct Watch {
    activation: AttemptActivationIdentity,
    token: AttemptRequestToken,
    refusal: ExecutionFailureInput,
    allowed: bool,
}

#[derive(Debug, Default)]
pub(crate) struct PreReadyObservations {
    watches: BTreeMap<QueryExecutionId, Watch>,
    changed: BTreeSet<QueryExecutionId>,
}
impl PreReadyObservations {
    /// AP retains only its most recent permission_watch for this activation.
    /// Re-query replaces that token; historical queries cannot accumulate.
    pub(crate) fn query(
        &mut self,
        activation: AttemptActivationIdentity,
        token: AttemptRequestToken,
        refusal: &ExecutionFailureInput,
        opportunity: PreReadyOpportunity,
        ledger_correlated: bool,
    ) -> Result<bool, PreReadyObservationError> {
        let execution = activation.execution();
        if let Some(held) = self.watches.get(&execution) {
            if held.activation != activation {
                return Err(PreReadyObservationError::StaleActivation);
            }
            if held.refusal != *refusal {
                return Err(PreReadyObservationError::ConflictingRefusal);
            }
        }
        let allowed = opportunity.allows(activation, ledger_correlated);
        self.changed.remove(&execution);
        if allowed {
            self.watches.insert(
                execution,
                Watch {
                    activation,
                    token,
                    refusal: refusal.clone(),
                    allowed,
                },
            );
        } else {
            // This very query receives the refusal. It creates no new watch
            // and needs no follow-up hint about the answer it just received.
            self.watches.remove(&execution);
        }
        Ok(allowed)
    }

    pub(crate) fn len(&self) -> usize {
        self.watches.len()
    }

    pub(crate) fn refusal(&self, execution: QueryExecutionId) -> Option<&ExecutionFailureInput> {
        self.watches.get(&execution).map(|watch| &watch.refusal)
    }

    /// A caller-owned cursor visits at most one activation per charged step.
    /// Do not collect every watch into an uncharged temporary Vec.
    pub(crate) fn next_watch_after(
        &self,
        after: Option<QueryExecutionId>,
    ) -> Option<QueryExecutionId> {
        match after {
            Some(after) => self.watches.range((Excluded(after), Unbounded)).next(),
            None => self.watches.iter().next(),
        }
        .map(|(execution, _)| *execution)
    }

    /// The step owner supplies the current SAME ledger correlation, including
    /// late Establish settlement, rather than caching an earlier permission.
    pub(crate) fn refresh_one(
        &mut self,
        execution: QueryExecutionId,
        opportunity: PreReadyOpportunity,
        ledger_correlated: bool,
    ) -> bool {
        let Some(watch) = self.watches.get_mut(&execution) else {
            return false;
        };
        let allowed = opportunity.allows(watch.activation, ledger_correlated);
        if watch.allowed && !allowed {
            watch.allowed = false;
            self.changed.insert(execution);
            return true;
        }
        false
    }

    /// One hint per call. The core charges the returned effect independently.
    /// A hint is not permission to keep observing; AP must query again.
    pub(crate) fn take_change(&mut self) -> Option<PreReadyChange> {
        let execution = self.changed.pop_first()?;
        let watch = self
            .watches
            .get(&execution)
            .expect("pending change retains its original watch");
        Some(PreReadyChange {
            activation: watch.activation,
            token: watch.token,
        })
    }

    pub(crate) fn has_changes(&self) -> bool {
        !self.changed.is_empty()
    }

    pub(crate) fn remove(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), PreReadyObservationError> {
        let execution = activation.execution();
        if self
            .watches
            .get(&execution)
            .is_some_and(|watch| watch.activation != activation)
        {
            return Err(PreReadyObservationError::StaleActivation);
        }
        self.watches.remove(&execution);
        self.changed.remove(&execution);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
