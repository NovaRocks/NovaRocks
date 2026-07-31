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

//! Durable ownership for the frontend ANALYZE worker.
//!
//! Job records remain in the repository. This module owns only the global
//! worker lease and the fence validator injected into repository mutations.
//! It never opens writes or starts a restore: an existing coordination plane
//! is respected exactly as it was found.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use novarocks_spi::state_store::StateStore;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, ClockHealth, CoordinationError, CoordinationErrorKind, HolderId,
    IncarnationGate, LeaseClock, LeaseFence, LeaseManager, LeaseSettings, ResourceKey,
};
use uuid::Uuid;

use super::repository::FenceValidator;

/// One process-wide lease protects all durable ANALYZE attempts for a frontend
/// deployment. It is deliberately not keyed by table or session.
pub const STATISTICS_ANALYZE_WORKER_RESOURCE: &str = "frontend/statistics/analyze-worker/v1";
const STATISTICS_ANALYZE_WORKER_RESOURCE_BYTES: &[u8] = b"frontend/statistics/analyze-worker/v1";

pub const STATISTICS_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const STATISTICS_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub const STATISTICS_MAX_CLOCK_SKEW: Duration = Duration::from_secs(1);
pub const STATISTICS_TAKEOVER_OBSERVATION: Duration = Duration::from_secs(2);

/// Production wall/monotonic clock for the statistics worker. Tests inject a
/// deterministic `LeaseClock` directly through `open_with_clock`.
#[derive(Debug)]
pub struct SystemStatisticsLeaseClock {
    monotonic_origin: Instant,
}

impl Default for SystemStatisticsLeaseClock {
    fn default() -> Self {
        Self {
            monotonic_origin: Instant::now(),
        }
    }
}

impl LeaseClock for SystemStatisticsLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CoordinationError::clock_unsafe())
            .and_then(|duration| {
                u64::try_from(duration.as_millis()).map_err(|_| CoordinationError::clock_unsafe())
            })
    }

    fn monotonic_time_millis(&self) -> u64 {
        u64::try_from(self.monotonic_origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn health(&self) -> ClockHealth {
        ClockHealth::Healthy
    }
}

/// Coordination facade used by the durable worker. Opening it may bootstrap a
/// missing coordination record, but never mutates an already bootstrapped
/// incarnation's restore/write mode.
#[derive(Clone)]
pub struct StatisticsAnalyzeWorkerCoordination {
    manager: LeaseManager,
    resource: ResourceKey,
}

impl StatisticsAnalyzeWorkerCoordination {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        Self::open_with_clock(store, Arc::new(SystemStatisticsLeaseClock::default())).await
    }

    pub async fn open_with_clock(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn LeaseClock>,
    ) -> Result<Self, CoordinationError> {
        let gate = IncarnationGate::new(Arc::clone(&store));
        match gate.load().await {
            Ok(_) => {}
            Err(error) if error.kind() == CoordinationErrorKind::NotBootstrapped => {
                gate.bootstrap(OperationId::new_v7()).await?;
            }
            Err(error) => return Err(error),
        }
        let holder = HolderId::try_from(Bytes::from(Uuid::now_v7().to_string()))?;
        let settings = LeaseSettings::new(
            STATISTICS_LEASE_DURATION,
            STATISTICS_LEASE_RENEW_INTERVAL,
            STATISTICS_MAX_CLOCK_SKEW,
            STATISTICS_TAKEOVER_OBSERVATION,
        )?;
        Ok(Self {
            manager: LeaseManager::new(store, holder, clock, settings)?,
            resource: ResourceKey::try_from(Bytes::from_static(
                STATISTICS_ANALYZE_WORKER_RESOURCE_BYTES,
            ))?,
        })
    }

    pub async fn acquire(&self) -> Result<AcquireOutcome, CoordinationError> {
        self.manager
            .acquire(
                self.resource.clone(),
                AttemptId::try_from(Uuid::now_v7())?,
                OperationId::new_v7(),
            )
            .await
    }

    /// Creates the validator used by repository mutation transactions. The
    /// fence is checked in the same transaction as the job-record CAS and
    /// state-index transition, so a lost worker can never publish a stale job
    /// state after takeover.
    pub fn fence_validator(fence: LeaseFence) -> FenceValidator {
        Arc::new(move |transaction| {
            let fence = fence.clone();
            Box::pin(async move {
                fence
                    .validate_in(transaction)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
    }
}
