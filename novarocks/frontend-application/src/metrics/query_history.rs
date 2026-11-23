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

use novarocks_query_application::session_control::QueryControlService;
use prometheus::{IntGauge, Registry};

pub(super) struct QueryHistoryGauges {
    owner: QueryControlService,
    retained_bytes: IntGauge,
    entries: IntGauge,
    oldest_finished_age_ms: IntGauge,
}

impl QueryHistoryGauges {
    pub(super) fn register(
        registry: &Registry,
        owner: QueryControlService,
    ) -> Result<Self, String> {
        let register = |name, help| -> Result<IntGauge, prometheus::Error> {
            let gauge = IntGauge::new(name, help)?;
            registry.register(Box::new(gauge.clone()))?;
            Ok(gauge)
        };
        let gauges = || -> Result<Self, prometheus::Error> {
            Ok(Self {
                owner,
                retained_bytes: register(
                    "novarocks_trino_query_history_retained_bytes",
                    "Original query history byte receipts, including backing still held by readers.",
                )?,
                entries: register(
                    "novarocks_trino_query_history_entries",
                    "Completed Trino queries currently kept in recent history.",
                )?,
                oldest_finished_age_ms: register(
                    "novarocks_trino_query_history_oldest_finished_age_ms",
                    "Age of the earliest completion retained in query history, in milliseconds.",
                )?,
            })
        };
        gauges().map_err(|error| error.to_string())
    }

    pub(super) fn sample(&self) {
        if let Some(status) = self.owner.client_query_history_status() {
            self.retained_bytes
                .set(i64::try_from(status.retained_bytes).unwrap_or(i64::MAX));
            self.entries
                .set(i64::try_from(status.rows).unwrap_or(i64::MAX));
            let age = status
                .oldest_finished
                .map_or(0, |finished| finished.elapsed().as_millis());
            self.oldest_finished_age_ms
                .set(i64::try_from(age).unwrap_or(i64::MAX));
        }
    }
}
