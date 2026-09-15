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

use once_cell::sync::Lazy;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};

use crate::catalog_application::CatalogProjectionMetricsSnapshot;

static FRONTEND_CATALOG_PROJECTION_CATALOGS: Lazy<IntGauge> = Lazy::new(|| {
    IntGauge::with_opts(Opts::new(
        "novarocks_frontend_catalog_projection_catalogs",
        "Number of catalog attachments currently projected into this Frontend runtime.",
    ))
    .expect("register novarocks_frontend_catalog_projection_catalogs")
});

static FRONTEND_CATALOG_PROJECTION_EVENTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "novarocks_frontend_catalog_projection_events_total",
            "Cumulative Frontend catalog projection controller events.",
        ),
        &["outcome"],
    )
    .expect("register novarocks_frontend_catalog_projection_events_total")
});

pub(crate) fn register_collectors(registry: &Registry) -> Result<(), String> {
    for collector in [
        Box::new(FRONTEND_CATALOG_PROJECTION_CATALOGS.clone())
            as Box<dyn prometheus::core::Collector>,
        Box::new(FRONTEND_CATALOG_PROJECTION_EVENTS.clone()),
    ] {
        registry.register(collector).map_err(|error| {
            format!("register catalog projection metric collector failed: {error}")
        })?;
    }
    Ok(())
}

pub(crate) fn publish(snapshot: CatalogProjectionMetricsSnapshot) {
    Lazy::force(&FRONTEND_CATALOG_PROJECTION_CATALOGS).set(snapshot.projected_catalogs as i64);
    for (outcome, count) in [
        ("round_success", snapshot.successful_rounds),
        ("round_failure", snapshot.failed_rounds),
        ("resync", snapshot.resyncs),
        ("freshness_expiry", snapshot.freshness_expiries),
    ] {
        FRONTEND_CATALOG_PROJECTION_EVENTS
            .with_label_values(&[outcome])
            .set(count as i64);
    }
}
