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

//! Why a driver-polled scan stream ended a poll without a page.
//!
//! A scan stream returns `Pending` for one of two reasons. Either its turn
//! budget ran out, so it yields the driver's turn and wakes itself, or it
//! waits on something only another party can complete: an object-store
//! read, the next split, or the exit of a closing read. The two are counted
//! apart, because only the first is CPU cooperation and only the second is
//! a wait that gives the driver's worker to other work.

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry};

pub const SCAN_STREAM_PENDING_TOTAL: &str = "novarocks_scan_stream_pending_total";

static PENDING: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            SCAN_STREAM_PENDING_TOTAL,
            "Scan stream polls that returned no page, by whether the turn budget ran out or the stream waits.",
        ),
        &["reason"],
    )
    .expect("construct scan stream pending metrics")
});

/// Why one poll of a scan stream returned `Pending`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScanStreamPending {
    /// The turn budget ran out: the stream yielded and woke itself.
    BudgetYield,
    /// The stream waits on an event outside the driver.
    Wait,
}

impl ScanStreamPending {
    const fn label(self) -> &'static str {
        match self {
            Self::BudgetYield => "budget_yield",
            Self::Wait => "wait",
        }
    }
}

pub(crate) fn observe_scan_stream_pending(reason: ScanStreamPending) {
    PENDING.with_label_values(&[reason.label()]).inc();
}

pub fn register_scan_stream_metrics(registry: &Registry) -> Result<(), String> {
    registry
        .register(Box::new(Lazy::force(&PENDING).clone()))
        .map_err(|error| format!("register scan stream pending metrics: {error}"))
}

#[cfg(test)]
pub(crate) fn scan_stream_pending_count(reason: ScanStreamPending) -> u64 {
    PENDING.with_label_values(&[reason.label()]).get()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered_count(registry: &Registry, reason: &str) -> u64 {
        registry
            .gather()
            .iter()
            .find(|family| family.get_name() == SCAN_STREAM_PENDING_TOTAL)
            .and_then(|family| {
                family.get_metric().iter().find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.get_value() == reason)
                })
            })
            .map_or(0, |metric| metric.get_counter().get_value() as u64)
    }

    #[test]
    fn each_reason_is_counted_under_its_own_label() {
        let registry = Registry::new();
        register_scan_stream_metrics(&registry).expect("register");
        let yields = registered_count(&registry, "budget_yield");
        let waits = registered_count(&registry, "wait");
        observe_scan_stream_pending(ScanStreamPending::BudgetYield);
        assert!(registered_count(&registry, "budget_yield") > yields);
        observe_scan_stream_pending(ScanStreamPending::Wait);
        assert!(registered_count(&registry, "wait") > waits);
    }
}
