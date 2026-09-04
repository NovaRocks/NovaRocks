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

//! Process-neutral observability for `TableWriter` sparse partial output.

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts, Registry};

pub const TABLE_WRITER_PARTIAL_TOTALS: &str = "novarocks_table_writer_partial_totals";

static PARTIAL_TOTALS: Lazy<IntCounterVec> = Lazy::new(|| {
    IntCounterVec::new(
        Opts::new(
            TABLE_WRITER_PARTIAL_TOTALS,
            "Cumulative sparse aggregate-partial output produced by TableWriter.",
        ),
        &["unit"],
    )
    .expect("construct TableWriter partial metrics")
});

pub(crate) fn observe_partial_output(
    rows: usize,
    bytes: usize,
    non_null_channels: usize,
    sparse_rows: usize,
) {
    for (unit, value) in [
        ("batches", 1usize),
        ("rows", rows),
        ("bytes", bytes),
        ("non_null_channels", non_null_channels),
        ("sparse_rows", sparse_rows),
    ] {
        PARTIAL_TOTALS
            .with_label_values(&[unit])
            .inc_by(u64::try_from(value).unwrap_or(u64::MAX));
    }
}

/// Register execution-owned writer collectors with the role registry that
/// composes execution. Execution does not select a process or registry.
pub fn register_table_writer_metrics(registry: &Registry) -> Result<(), String> {
    registry
        .register(Box::new(Lazy::force(&PARTIAL_TOTALS).clone()))
        .map_err(|error| format!("register TableWriter partial metrics: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        TABLE_WRITER_PARTIAL_TOTALS, observe_partial_output, register_table_writer_metrics,
    };

    #[test]
    fn explicit_registry_observes_all_partial_dimensions() {
        let registry = prometheus::Registry::new();
        register_table_writer_metrics(&registry).expect("register collector");
        let before = registry.gather();
        observe_partial_output(1, 17, 3, 1);
        let after = registry.gather();

        let value = |families: &[prometheus::proto::MetricFamily], unit: &str| {
            families
                .iter()
                .find(|family| family.get_name() == TABLE_WRITER_PARTIAL_TOTALS)
                .and_then(|family| {
                    family.get_metric().iter().find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.get_name() == "unit" && label.get_value() == unit)
                    })
                })
                .map(|metric| metric.get_counter().get_value())
                .unwrap_or_default()
        };
        for (unit, delta) in [
            ("batches", 1.0),
            ("rows", 1.0),
            ("bytes", 17.0),
            ("non_null_channels", 3.0),
            ("sparse_rows", 1.0),
        ] {
            assert_eq!(value(&after, unit) - value(&before, unit), delta);
        }
    }
}
