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

use crate::mv::domain::refresh::apply_key::ApplyKeyValueType;
use crate::mv::domain::refresh::snapshot::BaseSnapshotPolicy;
use novarocks_mv_application::persistence::codec::{ApplyKeyKind, InterpretationDocument};
use novarocks_mv_application::persistence::runtime_bindings::MvRuntimeBindings;

/// What a NotDerivable partition derivation outcome means for the refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionPruningPolicy {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    Required,
    BestEffort,
}

/// The compact row-identity discriminant needed by refresh execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshIdentity {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
    BranchScoped(Box<RefreshIdentity>),
}

/// Refresh-time capabilities reconstructed from a persisted MV schema contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshCapabilities {
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    pub has_agg_state: bool,
    pub(crate) identity: RefreshIdentity,
    pub apply_key_column: String,
    pub(crate) apply_key_value_type: ApplyKeyValueType,
    pub(crate) partition_pruning: PartitionPruningPolicy,
}

impl RefreshCapabilities {
    /// Reconstruct refresh capabilities from canonical D/L facts and the exact
    /// provider observation that L was validated against.
    ///
    /// `relation_occurrence_count` and `has_join` describe D's immutable
    /// relation shape: a join pair may start with one empty side, while a
    /// fan-in of independent relations may not. The physical apply-key column
    /// name is never persisted by the Accelerator; it comes from the same
    /// exact target observation that produced `bindings`.
    pub fn from_canonical_facts(
        interpretation: &InterpretationDocument,
        bindings: &MvRuntimeBindings,
        relation_occurrence_count: usize,
        has_join: bool,
    ) -> Result<RefreshCapabilities, String> {
        let has_agg = !interpretation.aggregates.is_empty();
        let has_branch = !interpretation.branches.is_empty();

        match (has_join, has_agg, has_branch) {
            (false, false, false)
            | (true, false, false)
            | (false, false, true)
            | (false, true, false)
            | (true, true, false)
            | (false, true, true)
            | (true, true, true) => {}
            _ => {
                return Err(format!(
                    "unsupported materialized-view interpretation shape \
                     (join={has_join}, agg={has_agg}, branch={has_branch})"
                ));
            }
        }

        let snapshot_policy = if has_branch {
            BaseSnapshotPolicy::AllBasesRequired
        } else if has_join {
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        } else if relation_occurrence_count > 1 {
            BaseSnapshotPolicy::AllBasesRequired
        } else {
            BaseSnapshotPolicy::SingleBase
        };

        let apply_key_kind = interpretation
            .apply_key_kind()
            .ok_or("keyed refresh requires an apply key")?;
        let kind_identity = apply_key_kind_to_refresh_identity(apply_key_kind);
        let identity = if has_branch {
            RefreshIdentity::BranchScoped(Box::new(kind_identity))
        } else {
            kind_identity
        };

        let apply_key_value_type = match (apply_key_kind, has_branch) {
            (ApplyKeyKind::BaseRowId, false) => ApplyKeyValueType::Int64,
            (ApplyKeyKind::BaseRowId, true) => ApplyKeyValueType::BranchInt64,
            (ApplyKeyKind::JoinRowKey, _) => ApplyKeyValueType::Utf8,
            (ApplyKeyKind::GroupRowId, false) => ApplyKeyValueType::Utf8,
            (ApplyKeyKind::GroupRowId, true) => ApplyKeyValueType::BranchUtf8,
        };

        let [apply_key] = bindings.apply_key.as_slice() else {
            return Err(
                "materialized-view refresh requires exactly one physical apply-key column"
                    .to_string(),
            );
        };

        Ok(RefreshCapabilities {
            snapshot_policy,
            has_agg_state: has_agg,
            identity,
            apply_key_column: apply_key.name.clone(),
            apply_key_value_type,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        })
    }
}

const fn apply_key_kind_to_refresh_identity(kind: ApplyKeyKind) -> RefreshIdentity {
    match kind {
        ApplyKeyKind::BaseRowId => RefreshIdentity::BaseRowId,
        ApplyKeyKind::JoinRowKey => RefreshIdentity::JoinRowKey,
        ApplyKeyKind::GroupRowId => RefreshIdentity::GroupRowId,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::persistence::runtime_bindings::MvPhysicalFieldFacts;
    use novarocks_mv_application::product::MvTarget;

    fn interpretation() -> InterpretationDocument {
        ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(11))
            .interpretation
    }

    fn physical(name: &str) -> MvPhysicalFieldFacts {
        MvPhysicalFieldFacts {
            field_id: novarocks_mv_application::persistence::identity::FieldIdentity::try_new(
                vec![0xfe, 1],
            )
            .expect("test field identity"),
            name: name.to_string(),
            ordinal: 0,
            source_type: novarocks_mv_application::persistence::test_support::fixture_type(
                "bigint",
            ),
            nullable: false,
        }
    }

    fn bindings(apply_key: Vec<MvPhysicalFieldFacts>) -> MvRuntimeBindings {
        MvRuntimeBindings {
            outputs: Vec::new(),
            aggregates: Vec::new(),
            apply_key,
            branches: Vec::new(),
        }
    }

    #[test]
    fn single_relation_projection_keeps_one_base_and_its_physical_apply_key() {
        let mut interpretation = interpretation();
        interpretation.apply_key.as_mut().unwrap().kind = ApplyKeyKind::BaseRowId;
        interpretation.aggregates.clear();
        interpretation.branches.clear();

        let capabilities = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_row_id")]),
            1,
            false,
        )
        .expect("projection capabilities");

        assert_eq!(capabilities.snapshot_policy, BaseSnapshotPolicy::SingleBase);
        assert!(!capabilities.has_agg_state);
        assert_eq!(capabilities.identity, RefreshIdentity::BaseRowId);
        assert_eq!(capabilities.apply_key_value_type, ApplyKeyValueType::Int64);
        // The physical name is the provider's, never a persisted one.
        assert_eq!(capabilities.apply_key_column, "__nova_row_id");
    }

    #[test]
    fn a_join_pair_and_a_fan_in_of_the_same_arity_choose_different_policies() {
        let mut interpretation = interpretation();
        interpretation.apply_key.as_mut().unwrap().kind = ApplyKeyKind::JoinRowKey;
        interpretation.aggregates.clear();
        interpretation.branches.clear();

        let join = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_join_key")]),
            2,
            true,
        )
        .expect("join capabilities");
        assert_eq!(
            join.snapshot_policy,
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        );
        assert_eq!(join.identity, RefreshIdentity::JoinRowKey);
        assert_eq!(join.apply_key_value_type, ApplyKeyValueType::Utf8);

        let fan_in = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_join_key")]),
            2,
            false,
        )
        .expect("fan-in capabilities");
        assert_eq!(
            fan_in.snapshot_policy,
            BaseSnapshotPolicy::AllBasesRequired,
            "a fan-in of independent relations may not start with one empty side"
        );
    }

    #[test]
    fn branch_union_aggregate_scopes_its_identity_and_requires_every_base() {
        let interpretation = interpretation();
        assert_eq!(
            interpretation.apply_key_kind(),
            Some(ApplyKeyKind::GroupRowId)
        );
        assert!(!interpretation.aggregates.is_empty());
        assert!(!interpretation.branches.is_empty());

        let capabilities = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_group_row_id")]),
            2,
            false,
        )
        .expect("branch aggregate capabilities");

        assert_eq!(
            capabilities.snapshot_policy,
            BaseSnapshotPolicy::AllBasesRequired
        );
        assert!(capabilities.has_agg_state);
        assert_eq!(
            capabilities.identity,
            RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId))
        );
        assert_eq!(
            capabilities.apply_key_value_type,
            ApplyKeyValueType::BranchUtf8
        );
    }

    #[test]
    fn join_over_branches_without_aggregate_state_is_rejected() {
        let mut interpretation = interpretation();
        interpretation.apply_key.as_mut().unwrap().kind = ApplyKeyKind::BaseRowId;
        interpretation.aggregates.clear();

        let error = RefreshCapabilities::from_canonical_facts(
            &interpretation,
            &bindings(vec![physical("__nova_row_id")]),
            2,
            true,
        )
        .expect_err("join + branch without aggregate must be rejected");

        assert_eq!(
            error,
            "unsupported materialized-view interpretation shape (join=true, agg=false, branch=true)"
        );
    }

    #[test]
    fn a_target_without_exactly_one_physical_apply_key_is_rejected() {
        let mut interpretation = interpretation();
        interpretation.aggregates.clear();
        interpretation.branches.clear();

        assert_eq!(
            RefreshCapabilities::from_canonical_facts(
                &interpretation,
                &bindings(Vec::new()),
                1,
                false
            )
            .expect_err("no apply key"),
            "materialized-view refresh requires exactly one physical apply-key column"
        );
        assert!(
            RefreshCapabilities::from_canonical_facts(
                &interpretation,
                &bindings(vec![physical("a"), physical("b")]),
                1,
                false
            )
            .is_err(),
            "two physical apply-key columns must be rejected"
        );
    }
}
