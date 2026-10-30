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
use prometheus::{IntCounterVec, IntGauge, Opts, Registry};
#[derive(Clone)]
pub struct TrinoProtocolObservation {
    active: IntGauge,
    settled: IntCounterVec,
    write_failures: IntCounterVec,
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
        registry.register(Box::new(self.settled.clone()))?;
        registry.register(Box::new(self.write_failures.clone()))?;
        Ok(())
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
        assert_eq!(registry.gather().len(), 3);
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
