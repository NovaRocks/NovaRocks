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

//! One consistent read confirmation. No host receive time or requested-time
//! interval is substituted for the snapshot's own clipped pause union.
use super::opening::{COVERED_LIVENESS_BUDGET, COVERED_OPENING_BUDGET};
use crate::{
    error::TaskExecutionError,
    input::{ReadStateSnapshot, ReceptionCoverage},
};
use novarocks_coordination_contract::{AttemptRequestToken, MonotonicInstant};
use novarocks_execution_contract::ParticipationRef;
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadConfirmation {
    pub token: AttemptRequestToken,
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub requested_at: MonotonicInstant,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStateDecision {
    Incomplete,
    Closed,
    AwaitStart,
    Opening {
        deadline: MonotonicInstant,
    },
    OpeningExpired,
    Live {
        deadline: MonotonicInstant,
        had_liveness: bool,
    },
    SilenceExpired {
        had_liveness: bool,
    },
}
impl ReadConfirmation {
    pub fn evaluate(
        self,
        snapshot: ReadStateSnapshot,
    ) -> Result<ReadStateDecision, TaskExecutionError> {
        let invalid = || {
            TaskExecutionError::Schedule(
                "read-state confirmation disagrees with its original request or timeline".into(),
            )
        };
        if snapshot.request != self.token
            || snapshot.participation != self.participation
            || snapshot.generation != self.generation
            || snapshot.snapshot_at < self.requested_at
        {
            return Err(invalid());
        }
        if snapshot.received_through_snapshot == ReceptionCoverage::Incomplete {
            return Ok(ReadStateDecision::Incomplete);
        }
        let at = snapshot.snapshot_at;
        if snapshot.last_liveness_at > at
            || snapshot.pauses.from != snapshot.last_liveness_at
            || snapshot.pauses.through != at
            || snapshot.pauses.paused > at.saturating_duration_since(snapshot.last_liveness_at)
            || snapshot
                .opening_started_at
                .is_some_and(|started| started > at)
            || snapshot.opened_at.is_some_and(|opened| {
                snapshot
                    .opening_started_at
                    .is_none_or(|started| opened < started)
                    || opened > at
            })
        {
            return Err(invalid());
        }
        if snapshot.closed {
            return Ok(ReadStateDecision::Closed);
        }
        let Some(started) = snapshot.opening_started_at else {
            return Ok(ReadStateDecision::AwaitStart);
        };
        let opening_deadline = started.saturating_add(COVERED_OPENING_BUDGET);
        let Some(opened) = snapshot.opened_at else {
            return Ok(if at.has_reached(opening_deadline) {
                ReadStateDecision::OpeningExpired
            } else {
                ReadStateDecision::Opening {
                    deadline: opening_deadline,
                }
            });
        };
        if opened > opening_deadline {
            return Ok(ReadStateDecision::OpeningExpired);
        }
        if snapshot.last_liveness_at < opened
            || (!snapshot.had_liveness && snapshot.last_liveness_at != opened)
        {
            return Err(invalid());
        }
        let elapsed = at.saturating_duration_since(snapshot.last_liveness_at);
        let active = elapsed
            .checked_sub(snapshot.pauses.paused)
            .ok_or_else(invalid)?;
        Ok(if active >= COVERED_LIVENESS_BUDGET {
            ReadStateDecision::SilenceExpired {
                had_liveness: snapshot.had_liveness,
            }
        } else {
            ReadStateDecision::Live {
                deadline: at.saturating_add(COVERED_LIVENESS_BUDGET - active),
                had_liveness: snapshot.had_liveness,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::ReadPauseCoverage;
    use std::time::Duration;
    fn at(ms: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_millis(ms))
    }
    fn original() -> (ReadConfirmation, ReadStateSnapshot) {
        let task = crate::test_support::remote(1);
        let token = AttemptRequestToken::new(NonZeroU64::new(1).unwrap());
        let generation = NonZeroU64::new(1).unwrap();
        let request = ReadConfirmation {
            token,
            participation: task.participation(),
            generation,
            requested_at: at(5000),
        };
        let snapshot = ReadStateSnapshot {
            request: token,
            participation: task.participation(),
            generation,
            snapshot_at: at(8000),
            opening_started_at: Some(at(0)),
            opened_at: Some(at(1000)),
            had_liveness: false,
            last_liveness_at: at(1000),
            pauses: ReadPauseCoverage {
                from: at(1000),
                through: at(8000),
                paused: Duration::from_secs(3),
            },
            closed: false,
            received_through_snapshot: ReceptionCoverage::Complete,
        };
        (request, snapshot)
    }
    #[test]
    fn delayed_snapshot_uses_its_own_elapsed_and_exact_pause_union() {
        let (request, snapshot) = original();
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::Live {
                deadline: at(9000),
                had_liveness: false
            }
        );
        let mut expired = snapshot;
        expired.pauses.paused = Duration::from_secs(2);
        assert_eq!(
            request.evaluate(expired).unwrap(),
            ReadStateDecision::SilenceExpired {
                had_liveness: false
            }
        );
    }
    #[test]
    fn actual_opening_phase_overrides_late_independent_phase_delivery() {
        let (request, mut snapshot) = original();
        snapshot.snapshot_at = at(5000);
        snapshot.opened_at = Some(at(4000));
        snapshot.last_liveness_at = at(4000);
        snapshot.pauses = ReadPauseCoverage {
            from: at(4000),
            through: at(5000),
            paused: Duration::ZERO,
        };
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::Live {
                deadline: at(9000),
                had_liveness: false
            }
        );
        snapshot.opened_at = None;
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::OpeningExpired
        );
        snapshot.opening_started_at = None;
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::AwaitStart
        );
    }
    #[test]
    fn same_tick_marker_is_not_inferred_from_timestamp_order() {
        let (request, mut snapshot) = original();
        snapshot.had_liveness = true;
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::Live {
                deadline: at(9000),
                had_liveness: true
            }
        );
    }
    #[test]
    fn incomplete_or_closed_never_authorizes_silence_and_wrong_bounds_are_rejected() {
        let (request, mut snapshot) = original();
        snapshot.received_through_snapshot = ReceptionCoverage::Incomplete;
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::Incomplete
        );
        snapshot.received_through_snapshot = ReceptionCoverage::Complete;
        snapshot.closed = true;
        assert_eq!(
            request.evaluate(snapshot).unwrap(),
            ReadStateDecision::Closed
        );
        snapshot.closed = false;
        snapshot.pauses.through = at(5000);
        assert!(request.evaluate(snapshot).is_err());
        snapshot.pauses.through = snapshot.snapshot_at;
        snapshot.pauses.paused = Duration::from_secs(8);
        assert!(request.evaluate(snapshot).is_err());
        let (_, mut snapshot) = original();
        snapshot.request = AttemptRequestToken::new(NonZeroU64::new(2).unwrap());
        assert!(request.evaluate(snapshot).is_err());
    }
}
