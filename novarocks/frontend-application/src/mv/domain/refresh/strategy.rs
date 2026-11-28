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

//! Refresh strategy selection from one exact set of source and target facts.

use novarocks_mv_application::persistence::codec::PendingVerificationRequirement;
use novarocks_mv_application::product::{MvIncrementalJoinMode, MvIncrementalWriteMode};
use novarocks_spi::connector::{ConnectorChangeWindowFacts, ConnectorChangeWindowNegativeWeight};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RefreshStrategy {
    MetadataOnly,
    AppendOnly,
    ContentLocate(PendingVerificationRequirement),
    PartitionReplace,
    FullReplace,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RefreshStrategyDecision {
    pub(crate) strategy: RefreshStrategy,
    pub(crate) content_rejection: Option<String>,
    pub(crate) costs: Vec<(RefreshStrategy, f64)>,
}

pub(crate) struct RefreshStrategyFacts<'a> {
    pub(crate) windows: &'a [&'a ConnectorChangeWindowFacts],
    pub(crate) force_full: bool,
    pub(crate) pending_verification: bool,
    pub(crate) can_incrementally_refresh: bool,
    pub(crate) append_admitted: bool,
    pub(crate) partition_admitted: bool,
    pub(crate) complete_to_available: bool,
}

impl RefreshStrategyFacts<'_> {
    pub(crate) fn can_locate_content(&self) -> bool {
        use ConnectorChangeWindowFacts as Window;

        !self.force_full
            && !self.pending_verification
            && self.can_incrementally_refresh
            && self
                .windows
                .iter()
                .all(|window| !matches!(window, Window::NotIncremental(_)))
            && self.windows.iter().any(|window| {
                matches!(
                    window,
                    Window::Difference {
                        negative_weight: ConnectorChangeWindowNegativeWeight::Possible,
                        ..
                    }
                )
            })
    }

    pub(crate) fn decide(
        &self,
        admit_content: impl FnOnce() -> Result<PendingVerificationRequirement, String>,
    ) -> Result<RefreshStrategyDecision, String> {
        use ConnectorChangeWindowFacts as Window;
        use ConnectorChangeWindowNegativeWeight::Absent;

        if self.pending_verification {
            return Err(if self.force_full {
                "MV has unresolved content verification and requires an explicit rebuild"
            } else {
                "MV has unresolved content verification and requires strict recovery"
            }
            .into());
        }
        let mut content_rejection = None;
        if !self.force_full && self.can_incrementally_refresh && !self.windows.is_empty() {
            if self
                .windows
                .iter()
                .all(|window| matches!(window, Window::NetZero(_)))
            {
                return Ok(RefreshStrategyDecision {
                    strategy: RefreshStrategy::MetadataOnly,
                    content_rejection: None,
                    costs: Vec::new(),
                });
            }
            if self.append_admitted
                && self.windows.iter().all(|window| {
                    matches!(
                        window,
                        Window::NetZero(_)
                            | Window::Difference {
                                negative_weight: Absent,
                                ..
                            }
                    )
                })
            {
                return Ok(RefreshStrategyDecision {
                    strategy: RefreshStrategy::AppendOnly,
                    content_rejection: None,
                    costs: Vec::new(),
                });
            }
            if self.can_locate_content() {
                match admit_content() {
                    Ok(requirement) => {
                        return Ok(RefreshStrategyDecision {
                            strategy: RefreshStrategy::ContentLocate(requirement),
                            content_rejection: None,
                            costs: Vec::new(),
                        });
                    }
                    Err(cause) => content_rejection = Some(cause),
                }
            }
        }
        if !self.force_full && self.can_incrementally_refresh && self.partition_admitted {
            return Ok(RefreshStrategyDecision {
                strategy: RefreshStrategy::PartitionReplace,
                content_rejection,
                costs: Vec::new(),
            });
        }
        if !self.complete_to_available {
            return Err("MV has no supported refresh strategy over its exact observations".into());
        }
        Ok(RefreshStrategyDecision {
            strategy: RefreshStrategy::FullReplace,
            content_rejection,
            costs: Vec::new(),
        })
    }
}

pub fn non_join_incremental_write_mode(
    is_aggregate: bool,
    source_negative_weight_possible: bool,
) -> MvIncrementalWriteMode {
    if is_aggregate || source_negative_weight_possible {
        MvIncrementalWriteMode::RowDelta
    } else {
        MvIncrementalWriteMode::FastAppend
    }
}

pub fn select_join_incremental_execution_mode(
    left_source_negative_weight_possible: bool,
    right_source_negative_weight_possible: bool,
) -> MvIncrementalJoinMode {
    if left_source_negative_weight_possible || right_source_negative_weight_possible {
        MvIncrementalJoinMode::Coalesce
    } else {
        MvIncrementalJoinMode::AppendOnly
    }
}

#[cfg(test)]
pub fn should_use_join_delta_append_only_fast_path(
    query: &novarocks_parser::ast::Query,
    left_has_delete_changes: bool,
    right_has_delete_changes: bool,
) -> bool {
    !left_has_delete_changes
        && !right_has_delete_changes
        && crate::mv::domain::iceberg_join_branch::is_append_only_join_delta_eligible(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_or_deletes_require_row_delta() {
        assert_eq!(
            non_join_incremental_write_mode(true, false),
            MvIncrementalWriteMode::RowDelta
        );
        assert_eq!(
            non_join_incremental_write_mode(false, true),
            MvIncrementalWriteMode::RowDelta
        );
        assert_eq!(
            non_join_incremental_write_mode(false, false),
            MvIncrementalWriteMode::FastAppend
        );
    }

    #[test]
    fn deletes_select_join_coalesce_mode() {
        assert_eq!(
            select_join_incremental_execution_mode(false, false),
            MvIncrementalJoinMode::AppendOnly
        );
        assert_eq!(
            select_join_incremental_execution_mode(true, false),
            MvIncrementalJoinMode::Coalesce
        );
        assert_eq!(
            select_join_incremental_execution_mode(false, true),
            MvIncrementalJoinMode::Coalesce
        );
    }

    #[test]
    fn selects_content_only_over_exact_eligible_windows() {
        use novarocks_spi::connector::{
            ConnectorChangeWindowNegativeWeight as Weight, ConnectorChangeWindowNetZeroBasis,
            ConnectorChangeWindowPartitionImpact, ConnectorChangeWindowVolume,
        };

        let net_zero =
            ConnectorChangeWindowFacts::NetZero(ConnectorChangeWindowNetZeroBasis::SameSnapshot);
        let difference = |negative_weight| ConnectorChangeWindowFacts::Difference {
            negative_weight,
            partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
            volume: ConnectorChangeWindowVolume {
                source_event_rows_upper_bound: 1,
                source_data_file_bytes: 2,
            },
        };
        let append = difference(Weight::Absent);
        let mixed = difference(Weight::Possible);
        let mut facts = RefreshStrategyFacts {
            windows: &[&net_zero],
            force_full: false,
            pending_verification: false,
            can_incrementally_refresh: true,
            append_admitted: true,
            partition_admitted: false,
            complete_to_available: true,
        };
        assert_eq!(
            facts.decide(|| unreachable!()).unwrap().strategy,
            RefreshStrategy::MetadataOnly
        );
        let append_windows = [&net_zero, &append];
        facts.windows = &append_windows;
        assert_eq!(
            facts.decide(|| unreachable!()).unwrap().strategy,
            RefreshStrategy::AppendOnly
        );
        let mixed_windows = [&append, &mixed];
        facts.windows = &mixed_windows;
        let requirement = PendingVerificationRequirement::content_locate(12, 24).unwrap();
        assert_eq!(
            facts.decide(|| Ok(requirement)).unwrap().strategy,
            RefreshStrategy::ContentLocate(requirement),
        );
        let refused = facts
            .decide(|| Err("No observed storage width".into()))
            .unwrap();
        assert_eq!(refused.strategy, RefreshStrategy::FullReplace);
        assert_eq!(
            refused.content_rejection.as_deref(),
            Some("No observed storage width")
        );
        facts.force_full = true;
        assert_eq!(
            facts.decide(|| unreachable!()).unwrap().strategy,
            RefreshStrategy::FullReplace
        );
        facts.pending_verification = true;
        assert!(
            facts
                .decide(|| unreachable!())
                .unwrap_err()
                .contains("explicit rebuild")
        );
    }
}
