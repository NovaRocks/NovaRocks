// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::cell::Cell;
use std::time::Instant;

/// Accumulates wall time across distinct cooperative-scheduler wait intervals.
///
/// Operators are single-driver owned, so interior `Cell` state lets their
/// read-only readiness callbacks open and close an interval without imposing a
/// lock or making the metric itself a scheduling dependency.
#[derive(Debug, Default)]
pub(super) struct BlockedDuration {
    started_at: Cell<Option<Instant>>,
    elapsed_ns: Cell<u128>,
    intervals: Cell<u64>,
}

impl BlockedDuration {
    pub(super) fn observe(&self, blocked: bool) {
        self.observe_at(blocked, Instant::now());
    }

    pub(super) fn elapsed_ns(&self) -> u128 {
        self.elapsed_ns_at(Instant::now())
    }

    pub(super) fn intervals(&self) -> u64 {
        self.intervals.get()
    }

    fn observe_at(&self, blocked: bool, now: Instant) {
        match (blocked, self.started_at.get()) {
            (true, None) => {
                self.started_at.set(Some(now));
                self.intervals.set(self.intervals.get().saturating_add(1));
            }
            (false, Some(started_at)) => {
                self.elapsed_ns.set(
                    self.elapsed_ns
                        .get()
                        .saturating_add(now.saturating_duration_since(started_at).as_nanos()),
                );
                self.started_at.set(None);
            }
            _ => {}
        }
    }

    fn elapsed_ns_at(&self, now: Instant) -> u128 {
        self.started_at
            .get()
            .map_or(self.elapsed_ns.get(), |started_at| {
                self.elapsed_ns
                    .get()
                    .saturating_add(now.saturating_duration_since(started_at).as_nanos())
            })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn repeated_observation_counts_distinct_intervals_and_live_elapsed_time() {
        let timer = BlockedDuration::default();
        let start = Instant::now();

        timer.observe_at(true, start);
        timer.observe_at(true, start + Duration::from_nanos(3));
        assert_eq!(timer.intervals(), 1);
        assert_eq!(timer.elapsed_ns_at(start + Duration::from_nanos(7)), 7);

        timer.observe_at(false, start + Duration::from_nanos(11));
        timer.observe_at(false, start + Duration::from_nanos(13));
        assert_eq!(timer.elapsed_ns_at(start + Duration::from_nanos(17)), 11);

        timer.observe_at(true, start + Duration::from_nanos(20));
        timer.observe_at(false, start + Duration::from_nanos(25));
        assert_eq!(timer.intervals(), 2);
        assert_eq!(timer.elapsed_ns(), 16);
    }
}
