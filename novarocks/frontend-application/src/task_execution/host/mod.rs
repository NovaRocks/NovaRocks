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

pub(crate) mod adapter;
pub(crate) mod observations;
pub(crate) mod startup;
pub(crate) mod subscriptions;

pub(crate) mod publication;
pub(crate) mod pumps;

pub(crate) mod auxiliary;
pub(crate) mod custody;
pub(crate) mod failure_bridge;
pub(crate) mod failure_lifecycle;
pub(crate) mod failure_publication;
pub(crate) mod failure_supervisor;
pub(crate) mod funding;
pub(crate) mod geometry;
pub(crate) mod install;

pub(crate) mod transport;

pub(crate) mod replacement_client;

#[cfg(test)]
mod terminal_priority_tests;

/// A paid pass resumes across turns. A new mark arriving during a pass
/// survives until the next pass, since it may concern an earlier position.
#[derive(Default)]
struct RetirementScan {
    remaining: usize,
    requested: bool,
}

impl RetirementScan {
    fn request(&mut self) {
        self.requested = true;
    }

    fn take_visits(&mut self, inventory: usize, budget: usize) -> usize {
        if self.remaining == 0 && std::mem::take(&mut self.requested) {
            self.remaining = inventory;
        }
        let visits = self.remaining.min(budget);
        self.remaining -= visits;
        visits
    }

    fn continue_visit(&mut self) {
        self.remaining += 1;
    }

    fn has_work(&self, inventory: usize) -> bool {
        self.remaining != 0 || self.requested && inventory != 0
    }
}

#[cfg(test)]
mod tests {
    use super::RetirementScan;
    use novarocks_control_runtime_core::{identity::ActorId, scheduler::ReadyQueue};

    #[test]
    fn staggered_retirement_passes_park_and_preserve_notifications() {
        let actor = ActorId::new(1).unwrap();
        let mut queue = ReadyQueue::new();
        queue.register(actor).unwrap();
        let mut short = RetirementScan::default();
        let mut long = RetirementScan::default();
        queue.notify(actor).unwrap();

        // Each pass visits only one blocked position per turn. The longer
        // pass starts two turns later, as if its first original arrived late.
        let mut visits = [0, 0];
        for turn in 0..11 {
            assert_eq!(queue.claim_with_notification(), Some((actor, turn == 0)));
            if turn == 0 {
                short.request();
            }
            if turn == 2 {
                long.request();
            }
            visits[0] += short.take_visits(3, 1);
            visits[1] += long.take_visits(9, 1);
            queue
                .release(actor, short.has_work(3) || long.has_work(9))
                .unwrap();
        }
        assert_eq!(visits, [3, 9]);
        assert!(queue.claim_with_notification().is_none());

        // A real mark racing a continuation must request another pass over
        // the earlier blocked positions, even though the actor is runnable.
        queue.notify(actor).unwrap();
        for turn in 0..6 {
            let (_, notified) = queue.claim_with_notification().unwrap();
            assert_eq!(notified, turn < 2);
            if notified {
                short.request();
            }
            assert_eq!(short.take_visits(3, 1), 1);
            if turn == 0 {
                queue.notify(actor).unwrap();
            }
            queue.release(actor, short.has_work(3)).unwrap();
        }
        assert!(queue.claim_with_notification().is_none());
    }
}

#[cfg(all(test, novarocks_dev_controls))]
mod strict_tail_tests;
