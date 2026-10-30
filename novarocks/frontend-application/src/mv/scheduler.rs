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

use novarocks_mv_application::repository::MvRepositoryError;
use novarocks_mv_application::{
    product::MvTarget as ProductMvTarget,
    scheduler::MvScheduledRefreshRequest,
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
    pub(crate) fn projection(
        &self,
    ) -> &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection {
        self.product.projection()
    }
}

/// The same policy snapshots are borrowed only for pure transitions. No
/// StateStore, Provider, future poll or foreign destructor runs under its lock.
pub(crate) async fn poll_async(
    product: &novarocks_mv_application::service::MvProductOperations,
    readiness: &crate::mv::domain::readiness::MvReadinessExecution<'_>,
    ports: &crate::mv::domain::iceberg_refresh::IcebergMvCorePorts,
    context: &novarocks_spi::connector::ConnectorRequestContext,
    factory: &crate::mv::domain::staged_create::CurrentProjectionMetadataCpu,
    capture: &std::sync::Arc<crate::mv::domain::staged_create::CurrentMetadataCapture>,
    now_ms: i64,
) -> Result<Vec<ScheduledRefreshRequest>, MvRepositoryError> {
    if !product.with_refresh_scheduler(|scheduler| scheduler.enabled()) {
        return Ok(Vec::new());
    }
    for projection in readiness.list_ready_projections().await? {
        let observation = product.with_refresh_scheduler(|scheduler| {
            scheduler.observe_projection(projection.projection, now_ms)
        });
        let Some(observation) = observation else {
            continue;
        };
        let current = super::background_engine::current_source_inputs_in(
            ports,
            observation.occurrences(),
            context,
            factory,
            capture,
        )
        .await;
        match current {
            Ok(inputs) => product.with_refresh_scheduler(|scheduler| {
                scheduler.resolve_current_inputs(observation, inputs, now_ms)
            }),
            Err(error) => {
                let disposition = ScheduledRefreshDisposition::from_background_error(error);
                product.with_refresh_scheduler(|scheduler| {
                    scheduler.record_observation_failure(&observation, disposition, now_ms)
                });
            }
        }
    }
    Ok(product
        .with_refresh_scheduler(|scheduler| scheduler.take_ready())
        .into_iter()
        .map(|product| ScheduledRefreshRequest {
            target: sql_target(product.target()),
            product,
        })
        .collect())
}

pub(crate) fn mark_started(
    product: &novarocks_mv_application::service::MvProductOperations,
    mv_id: i64,
) -> bool {
    product.with_refresh_scheduler(|scheduler| scheduler.mark_started(mv_id))
}

pub(crate) fn requeue(
    product: &novarocks_mv_application::service::MvProductOperations,
    request: ScheduledRefreshRequest,
) {
    product.with_refresh_scheduler(|scheduler| scheduler.requeue(request.product));
}

pub(crate) fn complete(
    product: &novarocks_mv_application::service::MvProductOperations,
    request: &ScheduledRefreshRequest,
    disposition: ScheduledRefreshDisposition,
    now_ms: i64,
) -> ScheduledRefreshRuntimeDecision {
    product.with_refresh_scheduler(|scheduler| {
        scheduler.complete(&request.product, disposition, now_ms)
    })
}

fn sql_target(target: &ProductMvTarget) -> MvTarget {
    MvTarget {
        catalog: target.catalog().map(str::to_owned),
        database: target.namespace().to_owned(),
        name: target.name().to_owned(),
    }
}
