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

//! Product policy configuration for asynchronous MV refresh scheduling.

use std::collections::BTreeMap;

use crate::persistence::definition::MvDesiredRefreshPolicy;
use crate::persistence::semantic::MvRefreshDesiredConfiguration;
use crate::repository::{MvPublishedProjection, MvPublishedWaterline};

/// The semantic result of interpreting one durable refresh policy against its
/// exact published and current-base facts. Provider observation remains an
/// outer adapter responsibility; this decision performs no I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvSchedulerSemanticDecision {
    Paused,
    Manual,
    IntervalNotDue { eligible_at_ms: i64 },
    IntervalDue,
    OnChangeNotDue,
    OnChangeDue,
    Invalid { reason: String },
}

/// Derive scheduler eligibility from complete durable semantics. For
/// `ASYNC_ON_CHANGE`, the caller must pass one exact provider observation.
pub fn mv_scheduler_semantic_decision(
    refresh: &MvRefreshDesiredConfiguration,
    publication: &MvPublishedProjection,
    now_ms: i64,
    current_base_snapshots: Option<&BTreeMap<String, Option<i64>>>,
) -> MvSchedulerSemanticDecision {
    if refresh.paused {
        return MvSchedulerSemanticDecision::Paused;
    }
    if let Err(error) = refresh.validate() {
        return MvSchedulerSemanticDecision::Invalid { reason: error };
    }

    match &refresh.policy {
        MvDesiredRefreshPolicy::Manual => MvSchedulerSemanticDecision::Manual,
        MvDesiredRefreshPolicy::AsyncInterval => {
            let interval_ms = refresh.interval_ms.expect("validated above");
            match publication {
                MvPublishedProjection::NeverPublished => MvSchedulerSemanticDecision::IntervalDue,
                MvPublishedProjection::Published(MvPublishedWaterline {
                    last_refresh_ms, ..
                }) => {
                    let eligible_at_ms = last_refresh_ms.saturating_add(interval_ms);
                    if now_ms >= eligible_at_ms {
                        MvSchedulerSemanticDecision::IntervalDue
                    } else {
                        MvSchedulerSemanticDecision::IntervalNotDue { eligible_at_ms }
                    }
                }
            }
        }
        MvDesiredRefreshPolicy::AsyncOnChange => {
            let Some(current_base_snapshots) = current_base_snapshots else {
                return MvSchedulerSemanticDecision::Invalid {
                    reason:
                        "ASYNC_ON_CHANGE scheduler decision requires exact current base snapshots"
                            .to_string(),
                };
            };
            match publication {
                MvPublishedProjection::NeverPublished => MvSchedulerSemanticDecision::OnChangeDue,
                MvPublishedProjection::Published(MvPublishedWaterline {
                    base_snapshots, ..
                }) if current_base_snapshots_match(base_snapshots, current_base_snapshots) => {
                    MvSchedulerSemanticDecision::OnChangeNotDue
                }
                MvPublishedProjection::Published(_) => MvSchedulerSemanticDecision::OnChangeDue,
            }
        }
    }
}

fn current_base_snapshots_match(
    published: &BTreeMap<String, i64>,
    current: &BTreeMap<String, Option<i64>>,
) -> bool {
    published.len() == current.len()
        && current
            .iter()
            .all(|(base, snapshot)| published.get(base).copied() == *snapshot)
}

/// Frozen process-local policy for asynchronous materialized-view refresh.
///
/// It does not own a queue, thread, provider handle, or persisted record. The
/// role-local scheduler owns those resources and consumes these product bounds
/// after configuration has been resolved once at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvSchedulerConfig {
    enabled: bool,
    tick_interval_ms: u64,
    max_concurrent_refreshes: usize,
    failure_backoff_ms: i64,
    max_failure_backoff_ms: i64,
}

impl MvSchedulerConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent_refreshes: usize,
        failure_backoff_ms: i64,
        max_failure_backoff_ms: i64,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent_refreshes,
            failure_backoff_ms,
            max_failure_backoff_ms,
        }
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub const fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    pub const fn max_concurrent_refreshes(&self) -> usize {
        self.max_concurrent_refreshes
    }

    pub const fn failure_backoff_ms(&self) -> i64 {
        self.failure_backoff_ms
    }

    pub const fn max_failure_backoff_ms(&self) -> i64 {
        self.max_failure_backoff_ms
    }
}

impl Default for MvSchedulerConfig {
    fn default() -> Self {
        Self::new(false, 30_000, 1, 60_000, 1_800_000)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{MvSchedulerConfig, MvSchedulerSemanticDecision, mv_scheduler_semantic_decision};
    use crate::persistence::definition::MvDesiredRefreshPolicy;
    use crate::persistence::semantic::MvRefreshDesiredConfiguration;
    use crate::repository::{MvPublishedProjection, MvPublishedWaterline};

    #[test]
    fn default_policy_preserves_the_deployed_scheduler_bounds() {
        assert_eq!(
            MvSchedulerConfig::default(),
            MvSchedulerConfig::new(false, 30_000, 1, 60_000, 1_800_000)
        );
    }

    #[test]
    fn never_published_async_on_change_is_due_for_an_exact_empty_observation() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &refresh,
                &MvPublishedProjection::NeverPublished,
                100,
                Some(&BTreeMap::new()),
            ),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }

    #[test]
    fn on_change_compares_exact_current_vector_to_complete_published_projection() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        let published = MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 10,
            last_refresh_rows: 1,
            last_refreshed_iceberg_snapshot_id: 20,
            base_snapshots: BTreeMap::from([("iceberg.db.base".to_string(), 11)]),
            base_table_object_ids: BTreeMap::new(),
        });
        let same = BTreeMap::from([("iceberg.db.base".to_string(), Some(11))]);
        let changed = BTreeMap::from([("iceberg.db.base".to_string(), Some(12))]);
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 100, Some(&same)),
            MvSchedulerSemanticDecision::OnChangeNotDue
        );
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 100, Some(&changed)),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }

    #[test]
    fn interval_uses_published_refresh_timestamp_not_runtime_next_run_state() {
        let refresh = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            false,
            Some(100),
            None,
        )
        .expect("valid desired refresh");
        let published = MvPublishedProjection::Published(MvPublishedWaterline {
            last_refresh_ms: 1_000,
            last_refresh_rows: 1,
            last_refreshed_iceberg_snapshot_id: 20,
            base_snapshots: BTreeMap::new(),
            base_table_object_ids: BTreeMap::new(),
        });
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 1_099, None),
            MvSchedulerSemanticDecision::IntervalNotDue {
                eligible_at_ms: 1_100,
            }
        );
        assert_eq!(
            mv_scheduler_semantic_decision(&refresh, &published, 1_100, None),
            MvSchedulerSemanticDecision::IntervalDue
        );
    }

    #[test]
    fn paused_manual_and_missing_on_change_observation_are_not_reinterpreted() {
        let paused = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncInterval,
            true,
            Some(100),
            None,
        )
        .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &paused,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Paused
        );

        let manual =
            MvRefreshDesiredConfiguration::new(MvDesiredRefreshPolicy::Manual, false, None, None)
                .expect("valid desired refresh");
        assert_eq!(
            mv_scheduler_semantic_decision(
                &manual,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Manual
        );

        let on_change = MvRefreshDesiredConfiguration::new(
            MvDesiredRefreshPolicy::AsyncOnChange,
            false,
            None,
            None,
        )
        .expect("valid desired refresh");
        assert!(matches!(
            mv_scheduler_semantic_decision(
                &on_change,
                &MvPublishedProjection::NeverPublished,
                100,
                None,
            ),
            MvSchedulerSemanticDecision::Invalid { .. }
        ));
    }
}
