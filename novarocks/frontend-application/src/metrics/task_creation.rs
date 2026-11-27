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

//! How often task creation payloads are built, and how many are alive.
//!
//! The counters say how many times a static plan was frozen, a creation was
//! priced, and a creation's metadata was frozen. The gauges say how many of
//! those frozen payloads are alive right now and how many bytes they hold.
//! Static fragment bytes keep a [`RetainedPayload`] in their shared backing,
//! so queued, replayed and in-flight byte aliases keep the gauges alive through
//! actual release. Creation metadata keeps its own retained observation.
//! The histogram records the total encoded fragment bytes once per completed
//! statement freeze.

use std::fmt;

use once_cell::sync::Lazy;
use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry};

static STATIC_FRAGMENTS_FROZEN: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_task_static_fragments_frozen_total",
        "Static fragment plans encoded into frozen bytes.",
    ))
    .expect("register novarocks_task_static_fragments_frozen_total")
});

static STATIC_FRAGMENTS_RETAINED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_task_static_fragments_retained",
        "Frozen static fragment plans alive in this frontend.",
    ))
    .expect("register novarocks_task_static_fragments_retained")
});

static STATIC_FRAGMENT_BYTES_RETAINED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_task_static_fragment_bytes_retained",
        "Bytes of frozen static fragment plans alive in this frontend.",
    ))
    .expect("register novarocks_task_static_fragment_bytes_retained")
});

static STATEMENT_STATIC_FRAGMENT_BYTES: Lazy<Histogram> = Lazy::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "novarocks_task_statement_static_fragment_bytes",
            "Total encoded static fragment bytes in one completed statement freeze.",
        )
        .buckets(prometheus::exponential_buckets(4096.0, 2.0, 20).expect("valid byte buckets")),
    )
    .expect("register novarocks_task_statement_static_fragment_bytes")
});

static CREATES_PRICED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_task_creates_priced_total",
        "Task creations measured for queue admission before being frozen.",
    ))
    .expect("register novarocks_task_creates_priced_total")
});

static CREATES_FROZEN: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_task_creates_frozen_total",
        "Task creation metadata carriers encoded into frozen bytes.",
    ))
    .expect("register novarocks_task_creates_frozen_total")
});

static CREATE_PAYLOADS_RETAINED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_task_create_payloads_retained",
        "Frozen task creation payloads alive in this frontend.",
    ))
    .expect("register novarocks_task_create_payloads_retained")
});

static CREATE_PAYLOAD_BYTES_RETAINED: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_task_create_payload_bytes_retained",
        "Bytes of frozen task creation metadata alive in this frontend.",
    ))
    .expect("register novarocks_task_create_payload_bytes_retained")
});

pub(crate) fn register_collectors(registry: &Registry) -> Result<(), String> {
    for collector in [
        Box::new(STATIC_FRAGMENTS_FROZEN.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(STATIC_FRAGMENTS_RETAINED.clone()),
        Box::new(STATIC_FRAGMENT_BYTES_RETAINED.clone()),
        Box::new(STATEMENT_STATIC_FRAGMENT_BYTES.clone()),
        Box::new(CREATES_PRICED.clone()),
        Box::new(CREATES_FROZEN.clone()),
        Box::new(CREATE_PAYLOADS_RETAINED.clone()),
        Box::new(CREATE_PAYLOAD_BYTES_RETAINED.clone()),
    ] {
        registry
            .register(collector)
            .map_err(|error| format!("register task creation metrics failed: {error}"))?;
    }
    Ok(())
}

/// One frozen payload's share of a retained gauge pair.
///
/// The payload holds it for exactly as long as the payload exists; dropping
/// it is the release the gauges report.
pub(crate) struct RetainedPayload {
    count: &'static IntGauge,
    bytes: &'static IntGauge,
    len: i64,
}

impl RetainedPayload {
    fn retain(count: &'static IntGauge, bytes: &'static IntGauge, len: usize) -> Self {
        let len = i64::try_from(len).unwrap_or(i64::MAX);
        count.inc();
        bytes.add(len);
        Self { count, bytes, len }
    }
}

impl Drop for RetainedPayload {
    fn drop(&mut self) {
        self.count.dec();
        self.bytes.sub(self.len);
    }
}

impl fmt::Debug for RetainedPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedPayload")
            .field("len", &self.len)
            .finish()
    }
}

/// One static fragment plan was frozen into `len` bytes.
pub(crate) fn static_fragment_frozen(len: usize) -> RetainedPayload {
    STATIC_FRAGMENTS_FROZEN.inc();
    RetainedPayload::retain(
        &STATIC_FRAGMENTS_RETAINED,
        &STATIC_FRAGMENT_BYTES_RETAINED,
        len,
    )
}

pub(crate) fn statement_static_fragments_frozen(lengths: impl Iterator<Item = usize>) {
    STATEMENT_STATIC_FRAGMENT_BYTES.observe(lengths.map(|len| len as f64).sum());
}

#[cfg(test)]
pub(crate) fn retained_static_fragments() -> (i64, i64) {
    (
        STATIC_FRAGMENTS_RETAINED.get(),
        STATIC_FRAGMENT_BYTES_RETAINED.get(),
    )
}

/// One creation was measured for admission.
pub(crate) fn create_priced() {
    CREATES_PRICED.inc();
}

/// One creation's metadata was frozen into `len` bytes.
pub(crate) fn create_frozen(len: usize) -> RetainedPayload {
    CREATES_FROZEN.inc();
    RetainedPayload::retain(
        &CREATE_PAYLOADS_RETAINED,
        &CREATE_PAYLOAD_BYTES_RETAINED,
        len,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_collector_registers_under_one_frontend_registry() {
        let registry = Registry::new();
        register_collectors(&registry).expect("register task creation metrics");
        let names = registry
            .gather()
            .into_iter()
            .map(|family| family.get_name().to_owned())
            .collect::<Vec<_>>();
        for expected in [
            "novarocks_task_static_fragments_frozen_total",
            "novarocks_task_static_fragments_retained",
            "novarocks_task_static_fragment_bytes_retained",
            "novarocks_task_statement_static_fragment_bytes",
            "novarocks_task_creates_priced_total",
            "novarocks_task_creates_frozen_total",
            "novarocks_task_create_payloads_retained",
            "novarocks_task_create_payload_bytes_retained",
        ] {
            assert!(names.iter().any(|name| name == expected), "{expected}");
        }
    }
}
