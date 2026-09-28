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

//! How long a driver's wake-up takes to become work.
//!
//! Three transitions are measured separately: an accepted notification
//! until the driver is on the ready queue, a due deadline until the driver
//! is on the ready queue, and the ready queue until a worker starts the
//! driver. Coalesced wake-ups keep the earliest time; notifications and
//! deadlines that no longer match a parked driver are counted apart.

use std::time::Duration;

use once_cell::sync::Lazy;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};

pub const DRIVER_DISPATCH_LATENCY_SECONDS: &str = "novarocks_driver_dispatch_latency_seconds";
pub const DRIVER_DISPATCH_STALE_EVENTS: &str = "novarocks_driver_dispatch_stale_events_total";

static LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            DRIVER_DISPATCH_LATENCY_SECONDS,
            "Latency from a driver's wake-up to its ready-queue entry, and from that entry to a worker start.",
        )
        // 1 microsecond to about 4 seconds.
        .buckets(prometheus::exponential_buckets(1e-6, 4.0, 12).expect("dispatch buckets")),
        &["transition"],
    )
    .expect("construct driver dispatch latency metrics")
});

static STALE: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            DRIVER_DISPATCH_STALE_EVENTS,
            "Notifications and deadlines that reached no parked driver.",
        ),
        &["kind"],
    )
    .expect("construct driver dispatch stale event metrics")
});

/// A measured step between a driver's wake-up and its next turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DispatchTransition {
    /// An accepted notification until the driver entered the ready queue.
    WakeToEnqueue,
    /// A due deadline until the driver entered the ready queue.
    DeadlineToEnqueue,
    /// The ready queue until a worker started the driver.
    EnqueueToWorker,
}

impl DispatchTransition {
    const fn label(self) -> &'static str {
        match self {
            Self::WakeToEnqueue => "wake_to_enqueue",
            Self::DeadlineToEnqueue => "deadline_to_enqueue",
            Self::EnqueueToWorker => "enqueue_to_worker",
        }
    }
}

/// A wake-up that reached no parked driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StaleDispatchEvent {
    Notification,
    Deadline,
}

impl StaleDispatchEvent {
    const fn label(self) -> &'static str {
        match self {
            Self::Notification => "notification",
            Self::Deadline => "deadline",
        }
    }
}

pub(crate) fn observe_dispatch(transition: DispatchTransition, elapsed: Duration) {
    LATENCY
        .with_label_values(&[transition.label()])
        .observe(elapsed.as_secs_f64());
}

pub(crate) fn observe_stale_dispatch(event: StaleDispatchEvent) {
    STALE.with_label_values(&[event.label()]).inc();
}

pub fn register_driver_dispatch_metrics(registry: &Registry) -> Result<(), String> {
    registry
        .register(Box::new(Lazy::force(&LATENCY).clone()))
        .map_err(|error| format!("register driver dispatch latency metrics: {error}"))?;
    registry
        .register(Box::new(Lazy::force(&STALE).clone()))
        .map_err(|error| format!("register driver dispatch stale event metrics: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn sample_count(registry: &Registry, transition: &str) -> u64 {
        registry
            .gather()
            .iter()
            .find(|family| family.get_name() == DRIVER_DISPATCH_LATENCY_SECONDS)
            .and_then(|family| {
                family.get_metric().iter().find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.get_value() == transition)
                })
            })
            .map_or(0, |metric| metric.get_histogram().get_sample_count())
    }

    #[test]
    fn each_transition_is_observed_under_its_own_label() {
        let registry = Registry::new();
        register_driver_dispatch_metrics(&registry).expect("register");
        let before = sample_count(&registry, "deadline_to_enqueue");
        observe_dispatch(
            DispatchTransition::DeadlineToEnqueue,
            Duration::from_micros(30),
        );
        assert_eq!(sample_count(&registry, "deadline_to_enqueue"), before + 1);
    }
}
