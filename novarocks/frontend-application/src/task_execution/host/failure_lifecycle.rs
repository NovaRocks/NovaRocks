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

//! The attempt's frozen abort requests and actual Native start evidence.

use novarocks_execution_contract::{
    AbortCause, AbortParticipation, ParticipationRef, TaskOperationId,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Member {
    abort: AbortParticipation,
    started: AtomicBool,
}

/// The host transfers this sole authority to its failure cleanup owner.
#[must_use]
pub(crate) struct FailureLifecycle(Arc<Box<[Member]>>);

/// A launched send can record start evidence, but cannot take abort requests.
#[derive(Clone)]
pub(crate) struct LifecycleStarts(Arc<Box<[Member]>>);

impl FailureLifecycle {
    pub(crate) fn freeze(participations: impl ExactSizeIterator<Item = ParticipationRef>) -> Self {
        let mut members = participations
            .map(|participation| Member {
                abort: AbortParticipation::new(
                    TaskOperationId::new_v7(),
                    participation,
                    AbortCause::QueryFailed,
                ),
                started: AtomicBool::new(false),
            })
            .collect::<Box<[_]>>();
        members.sort_unstable_by_key(|member| member.abort.participation());
        assert!(
            members
                .windows(2)
                .all(|pair| pair[0].abort.participation() != pair[1].abort.participation())
        );
        Self(Arc::new(members))
    }

    pub(crate) fn starts(&self) -> LifecycleStarts {
        LifecycleStarts(self.0.clone())
    }

    /// A delayed sender may reach its first poll after the Core failed. Wait
    /// for all original start authors to leave before finalizing the roster.
    /// Ownership moves once, so exact replays still yield a single abort.
    pub(crate) fn take_started_aborts(
        self,
    ) -> Result<impl Iterator<Item = AbortParticipation>, Self> {
        let members = Arc::try_unwrap(self.0).map_err(Self)?;
        Ok(members
            .into_vec()
            .into_iter()
            .filter_map(|member| member.started.into_inner().then_some(member.abort)))
    }
}

impl LifecycleStarts {
    fn member(&self, participation: ParticipationRef) -> &Member {
        let index = self
            .0
            .binary_search_by_key(&participation, |row| row.abort.participation())
            .expect("Native lifecycle operation belongs to the original frozen roster");
        &self.0[index]
    }

    pub(crate) fn record(&self, participation: ParticipationRef) {
        self.member(participation)
            .started
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn was_started(&self, participation: ParticipationRef) -> bool {
        self.member(participation).started.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    #[test]
    fn delayed_start_and_replay_preserve_one_frozen_abort() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let sent = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let untouched = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let lifecycle = FailureLifecycle::freeze([sent, untouched].into_iter());
        let first = lifecycle.starts();
        let replay = lifecycle.starts();
        let expected = first.member(sent).abort.envelope().operation_id();
        let lifecycle = match lifecycle.take_started_aborts() {
            Err(original) => original,
            Ok(_) => panic!("an original sender can still start"),
        };

        first.record(sent);
        replay.record(sent);
        drop(first);
        drop(replay);

        let mut aborts = lifecycle
            .take_started_aborts()
            .unwrap_or_else(|_| panic!("both send aliases exited"));
        let abort = aborts.next().unwrap();
        assert_eq!(abort.participation(), sent);
        assert_eq!(abort.envelope().operation_id(), expected);
        assert_eq!(abort.cause(), AbortCause::QueryFailed);
        assert!(aborts.next().is_none());
    }
}
