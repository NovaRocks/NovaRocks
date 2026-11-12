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

use futures::task::AtomicWaker;
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, ParticipationRef, TaskOperationId,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

struct Member {
    abort: AbortParticipation,
    started: AtomicBool,
}

struct LifecycleState {
    members: Box<[Member]>,
    authors: AtomicUsize,
    quiesced: AtomicWaker,
}

/// The host transfers this sole authority to its failure cleanup owner.
#[must_use]
pub(crate) struct FailureLifecycle(Arc<LifecycleState>);

/// A launched send can record start evidence, but cannot take abort requests.
pub(crate) struct LifecycleStarts(Arc<LifecycleState>);

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
        Self(Arc::new(LifecycleState {
            members,
            authors: AtomicUsize::new(0),
            quiesced: AtomicWaker::new(),
        }))
    }

    pub(crate) fn starts(&self) -> LifecycleStarts {
        self.0.authors.fetch_add(1, Ordering::Relaxed);
        LifecycleStarts(self.0.clone())
    }

    pub(crate) fn started_participations(&self) -> Box<[ParticipationRef]> {
        self.0
            .members
            .iter()
            .filter_map(|member| {
                member
                    .started
                    .load(Ordering::Acquire)
                    .then_some(member.abort.participation())
            })
            .collect()
    }

    /// A delayed sender may reach its first poll after the Core failed. Wait
    /// for all original start authors to leave before finalizing the roster.
    /// Ownership moves once, so exact replays still yield a single abort.
    pub(crate) async fn started_aborts(self) -> Box<[AbortParticipation]> {
        std::future::poll_fn(|cx| {
            self.0.quiesced.register(cx.waker());
            if self.0.authors.load(Ordering::Acquire) == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        self.0.quiesced.take();
        self.0
            .members
            .iter()
            .filter_map(|member| {
                member
                    .started
                    .load(Ordering::Acquire)
                    .then_some(member.abort)
            })
            .collect()
    }
}

impl LifecycleStarts {
    fn member(&self, participation: ParticipationRef) -> &Member {
        let index = self
            .0
            .members
            .binary_search_by_key(&participation, |row| row.abort.participation())
            .expect("Native lifecycle operation belongs to the original frozen roster");
        &self.0.members[index]
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

impl Drop for LifecycleStarts {
    fn drop(&mut self) {
        if self.0.authors.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.quiesced.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    #[tokio::test]
    async fn delayed_start_and_replay_preserve_one_frozen_abort() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let sent = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let untouched = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let lifecycle = FailureLifecycle::freeze([sent, untouched].into_iter());
        let first = lifecycle.starts();
        let replay = lifecycle.starts();
        let expected = first.member(sent).abort.envelope().operation_id();
        let future = lifecycle.started_aborts();
        tokio::pin!(future);
        assert!(futures::poll!(future.as_mut()).is_pending());

        first.record(sent);
        replay.record(sent);
        drop(first);
        drop(replay);

        let aborts = future.await;
        let [abort] = &*aborts else {
            panic!("one original abort")
        };
        assert_eq!(abort.participation(), sent);
        assert_eq!(abort.envelope().operation_id(), expected);
        assert_eq!(abort.cause(), AbortCause::QueryFailed);
    }
}
