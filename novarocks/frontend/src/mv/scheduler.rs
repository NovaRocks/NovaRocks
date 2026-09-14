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

//! Frontend provider-observation adapter for product-owned MV scheduling.

use super::background::MvBackgroundEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::{
    product::MvTarget as ProductMvTarget,
    scheduler::{MvRefreshScheduler, MvScheduledRefreshRequest, MvSchedulerConfig},
    scheduler_runtime::{MvRefreshDisposition, MvRefreshRuntimeDecision},
};
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

pub(crate) type ScheduledRefreshDisposition = MvRefreshDisposition;
pub(crate) type ScheduledRefreshRuntimeDecision = MvRefreshRuntimeDecision;

/// An immutable product-admitted refresh plus the SQL target required only by
/// the outer provider/query-execution adapter.
#[derive(Clone, Debug)]
pub(crate) struct ScheduledRefreshRequest {
    product: MvScheduledRefreshRequest,
    pub(crate) target: MvTarget,
}

impl ScheduledRefreshRequest {
    pub(crate) fn definition(
        &self,
    ) -> &novarocks_mv_application::persistence::definition::StoredMvDefinition {
        self.product.definition()
    }
}

/// The worker-owned execution seam. Implementations acquire the activity gate,
/// build the bounded request context, and run provider/query effects. Scheduler
/// policy never observes an effect adapter display string.
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) trait ScheduledRefreshRunner: Send + Sync {
    fn execute(&self, request: ScheduledRefreshRequest) -> ScheduledRefreshDisposition;
}

/// Frontend has no queue, backoff, source revision, or policy state. It only
/// drives the product's explicit current-snapshot observation transition.
#[derive(Debug)]
pub(crate) struct FrontendMvScheduler {
    product: MvRefreshScheduler,
}

impl FrontendMvScheduler {
    pub(crate) fn new(config: MvSchedulerConfig) -> Self {
        Self {
            product: MvRefreshScheduler::new(config),
        }
    }

    pub(crate) fn poll(
        &mut self,
        readiness: &MvReadinessPort,
        engine: &dyn MvBackgroundEngine,
        now_ms: i64,
    ) -> Result<Vec<ScheduledRefreshRequest>, MvRepositoryError> {
        if !self.product.enabled() {
            return Ok(Vec::new());
        }

        for projection in readiness.list_ready_projections()? {
            let Some(observation) = self
                .product
                .observe_definition(projection.definition, now_ms)
            else {
                continue;
            };
            let target = sql_target(observation.target());
            match engine.current_base_snapshots(&target) {
                Ok(current_base_snapshots) => self.product.resolve_current_base_snapshots(
                    observation,
                    current_base_snapshots,
                    now_ms,
                ),
                Err(error) => self.product.record_observation_failure(
                    &observation,
                    ScheduledRefreshDisposition::from_background_error(error),
                    now_ms,
                ),
            }
        }

        Ok(self
            .product
            .take_ready()
            .into_iter()
            .map(|product| ScheduledRefreshRequest {
                target: sql_target(product.target()),
                product,
            })
            .collect())
    }

    pub(crate) fn mark_started(&mut self, mv_id: i64) -> bool {
        self.product.mark_started(mv_id)
    }

    pub(crate) fn requeue(&mut self, request: ScheduledRefreshRequest) {
        self.product.requeue(request.product);
    }

    pub(crate) fn complete(
        &mut self,
        request: &ScheduledRefreshRequest,
        disposition: ScheduledRefreshDisposition,
        now_ms: i64,
    ) -> Result<ScheduledRefreshRuntimeDecision, MvRepositoryError> {
        Ok(self.product.complete(&request.product, disposition, now_ms))
    }
}

fn sql_target(target: &ProductMvTarget) -> MvTarget {
    MvTarget {
        catalog: target.catalog().map(str::to_owned),
        database: target.namespace().to_owned(),
        name: target.name().to_owned(),
    }
}
