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

use std::sync::atomic::{AtomicI32, Ordering};

const LAZY_COUNTER_MIN: i32 = -64;
const LAZY_COUNTER_MAX: i32 = 64;

#[derive(Debug)]
pub struct AdaptiveCoalesceController {
    lazy_counter: AtomicI32,
}

impl AdaptiveCoalesceController {
    pub const fn new() -> Self {
        Self {
            lazy_counter: AtomicI32::new(0),
        }
    }

    pub fn decide_and_record(&self, adaptive_enabled: bool, lazy_needed: bool) -> bool {
        let coalesce_together = !adaptive_enabled || self.lazy_counter.load(Ordering::Relaxed) >= 0;
        self.record_lazy_needed(lazy_needed);
        coalesce_together
    }

    fn record_lazy_needed(&self, lazy_needed: bool) {
        let delta = if lazy_needed { 1 } else { -1 };
        let mut current = self.lazy_counter.load(Ordering::Relaxed);
        loop {
            let next = (current + delta).clamp(LAZY_COUNTER_MIN, LAZY_COUNTER_MAX);
            match self.lazy_counter.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AdaptiveCoalesceController;

    #[test]
    fn adaptive_controller_prefers_together_then_split_when_lazy_absent() {
        let controller = AdaptiveCoalesceController::new();
        assert!(controller.decide_and_record(true, false));
        assert!(!controller.decide_and_record(true, false));
    }

    #[test]
    fn adaptive_controller_disabled_always_together() {
        let controller = AdaptiveCoalesceController::new();
        for _ in 0..8 {
            assert!(controller.decide_and_record(false, false));
        }
    }
}
