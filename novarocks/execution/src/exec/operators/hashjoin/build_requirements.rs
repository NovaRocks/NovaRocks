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

use crate::exec::node::join::JoinType;
use crate::exec::operators::hashjoin::join_hash_map::method::JoinHashMapBuildPurpose;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LookupRequirement {
    NotNeeded,
    Membership,
    RowMatches,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowPayloadRequirement {
    NotNeeded,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchFlagRequirement {
    NotNeeded,
    Needed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NullKeyRequirement {
    NotNeeded,
    HasAnyNullKey,
    NullKeyRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuildComponentRequirements {
    pub(crate) lookup: LookupRequirement,
    pub(crate) row_payload: RowPayloadRequirement,
    pub(crate) match_flags: MatchFlagRequirement,
    pub(crate) null_keys: NullKeyRequirement,
}

impl BuildComponentRequirements {
    pub(crate) fn requires_row_payload(self) -> bool {
        self.row_payload == RowPayloadRequirement::Required
    }

    pub(crate) fn requires_match_flags(self) -> bool {
        self.match_flags == MatchFlagRequirement::Needed
    }

    pub(crate) fn requires_lookup_table(self) -> bool {
        matches!(
            self.lookup,
            LookupRequirement::Membership | LookupRequirement::RowMatches
        )
    }

    pub(crate) fn join_hash_map_purpose(self) -> Option<JoinHashMapBuildPurpose> {
        match self.lookup {
            LookupRequirement::NotNeeded => None,
            LookupRequirement::Membership => Some(JoinHashMapBuildPurpose::PresenceOnly),
            LookupRequirement::RowMatches => Some(JoinHashMapBuildPurpose::RowMatches),
        }
    }
}

fn row_matches(
    match_flags: MatchFlagRequirement,
    null_keys: NullKeyRequirement,
) -> BuildComponentRequirements {
    BuildComponentRequirements {
        lookup: LookupRequirement::RowMatches,
        row_payload: RowPayloadRequirement::Required,
        match_flags,
        null_keys,
    }
}

pub(crate) fn required_build_components(
    join_type: JoinType,
    has_residual_predicate: bool,
    probe_is_left: bool,
    has_equi_keys: bool,
) -> BuildComponentRequirements {
    let mut requirements = if !probe_is_left {
        row_matches(
            MatchFlagRequirement::NotNeeded,
            NullKeyRequirement::NotNeeded,
        )
    } else {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter => row_matches(
                MatchFlagRequirement::NotNeeded,
                NullKeyRequirement::NotNeeded,
            ),
            JoinType::RightOuter | JoinType::FullOuter => {
                row_matches(MatchFlagRequirement::Needed, NullKeyRequirement::NotNeeded)
            }
            JoinType::LeftSemi if !has_residual_predicate => BuildComponentRequirements {
                lookup: LookupRequirement::Membership,
                row_payload: RowPayloadRequirement::NotNeeded,
                match_flags: MatchFlagRequirement::NotNeeded,
                null_keys: NullKeyRequirement::NotNeeded,
            },
            JoinType::LeftAnti if !has_residual_predicate => BuildComponentRequirements {
                lookup: LookupRequirement::Membership,
                row_payload: RowPayloadRequirement::NotNeeded,
                match_flags: MatchFlagRequirement::NotNeeded,
                null_keys: NullKeyRequirement::NotNeeded,
            },
            JoinType::LeftSemi | JoinType::LeftAnti => row_matches(
                MatchFlagRequirement::NotNeeded,
                NullKeyRequirement::NotNeeded,
            ),
            JoinType::RightSemi | JoinType::RightAnti => {
                row_matches(MatchFlagRequirement::Needed, NullKeyRequirement::NotNeeded)
            }
            JoinType::NullAwareLeftAnti if has_residual_predicate => row_matches(
                MatchFlagRequirement::NotNeeded,
                NullKeyRequirement::NullKeyRows,
            ),
            JoinType::NullAwareLeftAnti => BuildComponentRequirements {
                // Current probe code calls lookup_group_ids() in the no-residual NAAJ path,
                // so membership-only DirectIntSet is not sufficient here.
                lookup: LookupRequirement::RowMatches,
                row_payload: RowPayloadRequirement::NotNeeded,
                match_flags: MatchFlagRequirement::NotNeeded,
                null_keys: NullKeyRequirement::HasAnyNullKey,
            },
        }
    };

    if !has_equi_keys {
        requirements.lookup = LookupRequirement::NotNeeded;
        requirements.row_payload = RowPayloadRequirement::Required;
    }

    requirements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn left_semi_without_residual_needs_membership_only() {
        let req = required_build_components(JoinType::LeftSemi, false, true, true);
        assert_eq!(req.lookup, LookupRequirement::Membership);
        assert_eq!(req.row_payload, RowPayloadRequirement::NotNeeded);
        assert_eq!(req.match_flags, MatchFlagRequirement::NotNeeded);
        assert_eq!(
            req.join_hash_map_purpose(),
            Some(JoinHashMapBuildPurpose::PresenceOnly)
        );
    }

    #[test]
    fn left_anti_without_residual_needs_membership_only() {
        let req = required_build_components(JoinType::LeftAnti, false, true, true);
        assert_eq!(req.lookup, LookupRequirement::Membership);
        assert_eq!(req.row_payload, RowPayloadRequirement::NotNeeded);
        assert_eq!(req.match_flags, MatchFlagRequirement::NotNeeded);
        assert_eq!(
            req.join_hash_map_purpose(),
            Some(JoinHashMapBuildPurpose::PresenceOnly)
        );
    }

    #[test]
    fn residual_left_semi_requires_row_matches_and_payload() {
        let req = required_build_components(JoinType::LeftSemi, true, true, true);
        assert_eq!(req.lookup, LookupRequirement::RowMatches);
        assert_eq!(req.row_payload, RowPayloadRequirement::Required);
        assert_eq!(
            req.join_hash_map_purpose(),
            Some(JoinHashMapBuildPurpose::RowMatches)
        );
    }

    #[test]
    fn right_semi_requires_match_flags_and_payload() {
        let req = required_build_components(JoinType::RightSemi, false, true, true);
        assert_eq!(req.lookup, LookupRequirement::RowMatches);
        assert_eq!(req.row_payload, RowPayloadRequirement::Required);
        assert_eq!(req.match_flags, MatchFlagRequirement::Needed);
    }

    #[test]
    fn null_aware_anti_without_residual_needs_group_lookup_not_payload() {
        let req = required_build_components(JoinType::NullAwareLeftAnti, false, true, true);
        assert_eq!(req.lookup, LookupRequirement::RowMatches);
        assert_eq!(req.row_payload, RowPayloadRequirement::NotNeeded);
        assert_eq!(req.null_keys, NullKeyRequirement::HasAnyNullKey);
        assert_eq!(
            req.join_hash_map_purpose(),
            Some(JoinHashMapBuildPurpose::RowMatches)
        );
    }

    #[test]
    fn null_aware_anti_with_residual_needs_null_key_rows_and_payload() {
        let req = required_build_components(JoinType::NullAwareLeftAnti, true, true, true);
        assert_eq!(req.lookup, LookupRequirement::RowMatches);
        assert_eq!(req.row_payload, RowPayloadRequirement::Required);
        assert_eq!(req.null_keys, NullKeyRequirement::NullKeyRows);
    }

    #[test]
    fn no_equi_key_join_does_not_require_lookup_table() {
        let req = required_build_components(JoinType::Inner, false, true, false);
        assert_eq!(req.lookup, LookupRequirement::NotNeeded);
        assert_eq!(req.row_payload, RowPayloadRequirement::Required);
        assert_eq!(req.join_hash_map_purpose(), None);
    }

    #[test]
    fn right_probe_path_is_conservative() {
        let req = required_build_components(JoinType::LeftSemi, false, false, true);
        assert_eq!(req.lookup, LookupRequirement::RowMatches);
        assert_eq!(req.row_payload, RowPayloadRequirement::Required);
    }
}
