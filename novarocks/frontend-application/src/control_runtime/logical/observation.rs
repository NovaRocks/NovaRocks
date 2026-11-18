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

//! Normal Core-return diagnostics, not runtime commitment or physical exit.
//! No observation grants admission, scheduling, Work, or retirement rights.

use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalObservationSnapshot {
    /// Normal Core rounds, excluding carrier-only HostSettled deliveries.
    pub(crate) completed_steps: u64,
    /// Actual ActorStep emissions, including a join-readiness notice if any.
    pub(crate) last_step_effects: u64,
    pub(crate) total_effects: u64,
    pub(crate) max_step_effects: u64,
    /// Core actions awaiting their original result/exit, not Host claims.
    pub(crate) outstanding_actions: u64,
    /// The Core's own monotonic origin, not a renewed external deadline.
    pub(crate) next_deadline_ns: Option<u64>,
}

#[derive(Default)]
struct State {
    completed_steps: u64,
    total_effects: u64,
    max_step_effects: u64,
    overflowed: bool,
    current: Option<LogicalObservationSnapshot>,
}

/// Shared only as a passive diagnostic header; the lock contains plain values.
#[derive(Default)]
pub(crate) struct LogicalObservations {
    state: Mutex<State>,
}
impl LogicalObservations {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Busy, poison, overflow, or an unfinished step never replays old values.
    pub(crate) fn try_sample(&self) -> Option<LogicalObservationSnapshot> {
        let state = self.state.try_lock().ok()?;
        if state.overflowed {
            None
        } else {
            state.current
        }
    }

    pub(super) fn begin_step(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.current = None;
        }
    }

    /// Called only after Core returned normally and all actual emits succeeded.
    /// The surrounding runtime may still reject or fail its later batch commit.
    pub(super) fn complete_step(
        &self,
        effects: usize,
        outstanding_actions: usize,
        next_deadline_ns: Option<u128>,
    ) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.overflowed {
            return;
        }
        let values = (|| {
            let effects = u64::try_from(effects).ok()?;
            let outstanding_actions = u64::try_from(outstanding_actions).ok()?;
            let next_deadline_ns = next_deadline_ns.map(u64::try_from).transpose().ok()?;
            Some(LogicalObservationSnapshot {
                completed_steps: state.completed_steps.checked_add(1)?,
                last_step_effects: effects,
                total_effects: state.total_effects.checked_add(effects)?,
                max_step_effects: state.max_step_effects.max(effects),
                outstanding_actions,
                next_deadline_ns,
            })
        })();
        match values {
            Some(snapshot) => {
                state.completed_steps = snapshot.completed_steps;
                state.total_effects = snapshot.total_effects;
                state.max_step_effects = snapshot.max_step_effects;
                state.current = Some(snapshot);
            }
            None => {
                state.current = None;
                state.overflowed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_incomplete_or_busy_step_never_replays_the_previous_sample() {
        let observations = LogicalObservations::new();
        assert_eq!(observations.try_sample(), None);
        observations.begin_step();
        observations.complete_step(3, 2, Some(41));
        let first = observations.try_sample().unwrap();
        assert_eq!(first.last_step_effects, 3);
        assert_eq!(first.outstanding_actions, 2);
        assert_eq!(first.next_deadline_ns, Some(41));
        {
            let _original_lock = observations.state.lock().unwrap();
            assert_eq!(observations.try_sample(), None);
        }
        observations.begin_step();
        assert_eq!(observations.try_sample(), None);
        observations.complete_step(1, 0, None);
        let second = observations.try_sample().unwrap();
        assert_eq!(second.completed_steps, 2);
        assert_eq!(second.last_step_effects, 1);
        assert_eq!(second.total_effects, 4);
        assert_eq!(second.max_step_effects, 3);
        assert_eq!(second.outstanding_actions, 0);
        assert_eq!(second.next_deadline_ns, None);
    }

    #[test]
    fn an_unrepresentable_deadline_invalidates_the_sample_permanently() {
        let observations = LogicalObservations::new();
        observations.begin_step();
        observations.complete_step(1, 1, Some(u128::from(u64::MAX) + 1));
        assert_eq!(observations.try_sample(), None);
        observations.begin_step();
        observations.complete_step(1, 0, None);
        assert_eq!(observations.try_sample(), None);
    }

    #[test]
    fn poison_does_not_recover_an_old_sample_or_panic_the_observer() {
        let observations = LogicalObservations::new();
        observations.begin_step();
        observations.complete_step(1, 1, None);
        let original = std::panic::catch_unwind(|| {
            let _original_lock = observations.state.lock().unwrap();
            panic!("original diagnostic lock poison");
        });
        assert!(original.is_err());
        assert_eq!(observations.try_sample(), None);
        observations.begin_step();
        observations.complete_step(1, 0, None);
        assert_eq!(observations.try_sample(), None);
    }
}
