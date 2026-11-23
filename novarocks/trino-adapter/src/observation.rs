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

//! Listener-local protocol facts. Observing these counters grants no query authority.
use crate::settlement::Disposition;
use novarocks_query_application::query_control::{ClientQueryText, ClientQueryTextTracker};
use prometheus::{
    IntCounterVec, IntGauge, Opts, Registry,
    core::{Collector, Desc},
    proto::MetricFamily,
};
#[derive(Clone)]
pub struct TrinoProtocolObservation {
    active: IntGauge,
    unclaimed: IntGauge,
    original_text: OriginalTextMetrics,
    settled: IntCounterVec,
    write_failures: IntCounterVec,
}
#[derive(Clone)]
struct OriginalTextMetrics {
    tracker: ClientQueryTextTracker,
    backing_bytes: IntGauge,
    backings: IntGauge,
}

impl OriginalTextMetrics {
    fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            tracker: ClientQueryTextTracker::default(),
            backing_bytes: IntGauge::new(
                "novarocks_trino_original_query_text_backing_bytes",
                "Requested backing bytes of original accepted SQL text, through its last alias.",
            )?,
            backings: IntGauge::new(
                "novarocks_trino_original_query_text_backings",
                "Original accepted SQL text allocations still retained by a request, history, or reader.",
            )?,
        })
    }
}

impl Collector for OriginalTextMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut values = self.backing_bytes.desc();
        values.extend(self.backings.desc());
        values
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let status = self.tracker.status();
        let mut values = self.backing_bytes.collect();
        values.extend(self.backings.collect());
        for (family, value) in values
            .iter_mut()
            .zip([status.requested_bytes, status.backings])
        {
            family.mut_metric()[0].mut_gauge().set_value(value as f64);
        }
        values
    }
}

pub(crate) struct UnclaimedQueryObservation(IntGauge);

impl Drop for UnclaimedQueryObservation {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ResponseWriteFailure {
    Timeout,
    Interrupted,
}
impl TrinoProtocolObservation {
    pub(crate) fn new() -> Result<Self, prometheus::Error> {
        let observation = Self {
            active: IntGauge::new(
                "novarocks_trino_queries_active",
                "Executing Trino entries, including sealed output awaiting acknowledgement.",
            )?,
            unclaimed: IntGauge::new(
                "novarocks_trino_queries_unclaimed",
                "Accepted Trino requests whose original statement request is still in the POST queue.",
            )?,
            original_text: OriginalTextMetrics::new()?,
            settled: IntCounterVec::new(
                Opts::new(
                    "novarocks_trino_query_settlements_total",
                    "Trino protocol settlements by first terminal disposition.",
                ),
                &["outcome"],
            )?,
            write_failures: IntCounterVec::new(
                Opts::new(
                    "novarocks_trino_response_write_failures_total",
                    "Trino response writes interrupted or closed by their write deadline.",
                ),
                &["reason"],
            )?,
        };
        // Publish the listener's finite label domains before any query or write
        // occurs, so absence never has to stand in for an observed zero.
        for outcome in ["complete", "failed", "client_cancel", "abandoned"] {
            observation
                .settled
                .get_metric_with_label_values(&[outcome])?;
        }
        for reason in ["timeout", "interrupted"] {
            observation
                .write_failures
                .get_metric_with_label_values(&[reason])?;
        }
        Ok(observation)
    }
    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.active.clone()))?;
        registry.register(Box::new(self.unclaimed.clone()))?;
        registry.register(Box::new(self.original_text.clone()))?;
        registry.register(Box::new(self.settled.clone()))?;
        registry.register(Box::new(self.write_failures.clone()))?;
        Ok(())
    }
    pub(crate) fn observe_original_text(&self, text: &ClientQueryText) {
        text.observe_original_backing(&self.original_text.tracker);
    }

    pub(crate) fn unclaimed(&self) -> UnclaimedQueryObservation {
        self.unclaimed.inc();
        UnclaimedQueryObservation(self.unclaimed.clone())
    }

    pub(crate) fn write_failed(&self, reason: ResponseWriteFailure) {
        self.write_failures
            .with_label_values(&[match reason {
                ResponseWriteFailure::Timeout => "timeout",
                ResponseWriteFailure::Interrupted => "interrupted",
            }])
            .inc();
    }
    pub(crate) fn start(&self) {
        self.active.inc();
    }
    pub(crate) fn settle(&self, executing: bool, disposition: Disposition) {
        if executing {
            self.active.dec();
        }
        self.settled
            .with_label_values(&[match disposition {
                Disposition::Complete => "complete",
                Disposition::Fail | Disposition::Cancelled => "failed",
                Disposition::ClientCancel => "client_cancel",
                Disposition::Abandon => "abandoned",
            }])
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(registry: &Registry, name: &str, label: &str) -> Vec<(String, u64)> {
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.get_name() == name)
            .expect("the listener must publish its known metric family");
        let mut values = family
            .get_metric()
            .iter()
            .map(|metric| {
                assert_eq!(metric.get_label().len(), 1);
                let actual = &metric.get_label()[0];
                assert_eq!(actual.get_name(), label);
                (
                    actual.get_value().to_owned(),
                    metric.get_counter().get_value() as u64,
                )
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        values
    }

    #[test]
    fn every_known_counter_is_published_at_zero_before_listener_activity() {
        let observation = TrinoProtocolObservation::new().unwrap();
        let registry = Registry::new();
        observation.register(&registry).unwrap();
        assert_eq!(registry.gather().len(), 6);
        assert_eq!(observation.active.get(), 0);
        assert_eq!(
            counters(
                &registry,
                "novarocks_trino_query_settlements_total",
                "outcome"
            ),
            vec![
                ("abandoned".into(), 0),
                ("client_cancel".into(), 0),
                ("complete".into(), 0),
                ("failed".into(), 0),
            ]
        );
        assert_eq!(
            counters(
                &registry,
                "novarocks_trino_response_write_failures_total",
                "reason"
            ),
            vec![("interrupted".into(), 0), ("timeout".into(), 0)]
        );
    }

    #[test]
    fn original_sql_metrics_refund_on_the_last_alias_and_unclaimed_queue_exits_separately() {
        let observation = TrinoProtocolObservation::new().unwrap();
        let registry = Registry::new();
        observation.register(&registry).unwrap();
        let sample = |name| {
            registry
                .gather()
                .iter()
                .find(|family| family.get_name() == name)
                .unwrap()
                .get_metric()[0]
                .get_gauge()
                .get_value()
        };
        let first = ClientQueryText::new("SELECT fixture".into());
        let alias = first.clone();
        let queued = observation.unclaimed();
        observation.observe_original_text(&first);
        observation.observe_original_text(&alias);
        let size = sample("novarocks_trino_original_query_text_backing_bytes");
        assert!(size > first.as_str().len() as f64);
        assert_eq!(sample("novarocks_trino_original_query_text_backings"), 1.0);
        assert_eq!(sample("novarocks_trino_queries_unclaimed"), 1.0);
        drop(queued);
        drop(first);
        assert_eq!(sample("novarocks_trino_queries_unclaimed"), 0.0);
        assert_eq!(
            sample("novarocks_trino_original_query_text_backing_bytes"),
            size
        );
        drop(alias);
        assert_eq!(
            sample("novarocks_trino_original_query_text_backing_bytes"),
            0.0
        );
        assert_eq!(sample("novarocks_trino_original_query_text_backings"), 0.0);
    }

    #[test]
    fn listener_clones_share_their_registry_without_sharing_another_listener() {
        let first = TrinoProtocolObservation::new().unwrap();
        let first_registry = Registry::new();
        first.register(&first_registry).unwrap();
        let second = TrinoProtocolObservation::new().unwrap();
        let second_registry = Registry::new();
        second.register(&second_registry).unwrap();

        let original = first.clone();
        original.start();
        assert_eq!(first.active.get(), 1);
        assert_eq!(second.active.get(), 0);
        original.settle(true, Disposition::Complete);
        original.write_failed(ResponseWriteFailure::Interrupted);
        assert_eq!(first.active.get(), 0);
        assert_eq!(
            counters(
                &first_registry,
                "novarocks_trino_query_settlements_total",
                "outcome"
            ),
            vec![
                ("abandoned".into(), 0),
                ("client_cancel".into(), 0),
                ("complete".into(), 1),
                ("failed".into(), 0),
            ]
        );
        assert_eq!(
            counters(
                &first_registry,
                "novarocks_trino_response_write_failures_total",
                "reason"
            ),
            vec![("interrupted".into(), 1), ("timeout".into(), 0)]
        );
        assert!(
            counters(
                &second_registry,
                "novarocks_trino_query_settlements_total",
                "outcome"
            )
            .iter()
            .all(|(_, value)| *value == 0)
        );
        assert_eq!(
            counters(
                &second_registry,
                "novarocks_trino_response_write_failures_total",
                "reason"
            ),
            vec![("interrupted".into(), 0), ("timeout".into(), 0)]
        );
    }
}
