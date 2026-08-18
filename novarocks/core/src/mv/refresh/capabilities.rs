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

use novarocks_sql::planning::mv::ApplyKeySource;

use crate::mv::persistence::schema::MvSchemaContract;
use crate::mv::refresh::apply_key::ApplyKeyValueType;
use crate::mv::refresh::snapshot::BaseSnapshotPolicy;

/// What a NotDerivable partition derivation outcome means for the refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionPruningPolicy {
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
    pub snapshot_policy: BaseSnapshotPolicy,
    pub has_agg_state: bool,
    pub identity: RefreshIdentity,
    pub apply_key_column: String,
    pub apply_key_value_type: ApplyKeyValueType,
    pub partition_pruning: PartitionPruningPolicy,
}

impl RefreshCapabilities {
    pub fn from_schema_contract(
        contract: &MvSchemaContract,
    ) -> Result<RefreshCapabilities, String> {
        let has_join = contract.join.is_some();
        let has_agg = contract.aggregate.is_some();
        let has_branch = contract.branch.is_some();
        let has_extra_bases = !contract.bases.is_empty();

        let snapshot_policy = if has_branch {
            BaseSnapshotPolicy::AllBasesRequired
        } else if has_join {
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        } else if has_extra_bases {
            BaseSnapshotPolicy::AllBasesRequired
        } else {
            BaseSnapshotPolicy::SingleBase
        };

        let identity = if let Some(branch) = &contract.branch {
            RefreshIdentity::BranchScoped(Box::new(apply_key_source_to_refresh_identity(
                branch.inner_apply_key_source,
            )))
        } else {
            apply_key_source_to_refresh_identity(contract.target.hidden_apply_key.source)
        };

        let apply_key_value_type = match (contract.target.hidden_apply_key.source, has_branch) {
            (ApplyKeySource::BaseRowId, false) => ApplyKeyValueType::Int64,
            (ApplyKeySource::BaseRowId, true) => ApplyKeyValueType::BranchInt64,
            (ApplyKeySource::JoinRowKey, _) => ApplyKeyValueType::Utf8,
            (ApplyKeySource::GroupRowId, false) => ApplyKeyValueType::Utf8,
            (ApplyKeySource::GroupRowId, true) => ApplyKeyValueType::BranchUtf8,
        };

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
                    "unsupported schema contract shape \
                     (join={has_join}, agg={has_agg}, branch={has_branch})"
                ));
            }
        }

        Ok(RefreshCapabilities {
            snapshot_policy,
            has_agg_state: has_agg,
            identity,
            apply_key_column: contract.target.hidden_apply_key.column_name.clone(),
            apply_key_value_type,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        })
    }
}

fn apply_key_source_to_refresh_identity(source: ApplyKeySource) -> RefreshIdentity {
    match source {
        ApplyKeySource::BaseRowId => RefreshIdentity::BaseRowId,
        ApplyKeySource::JoinRowKey => RefreshIdentity::JoinRowKey,
        ApplyKeySource::GroupRowId => RefreshIdentity::GroupRowId,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::persistence::schema::{
        AggregateStateContract, BaseContract, BaseSchemaSnapshot, BranchIdColumnContract,
        BranchUnionContract, HiddenApplyKeyContract, JoinContract, JoinContractKind,
        OutputContract, TargetContract,
    };
    use novarocks_sql::planning::mv::{
        MV_GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME as GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
        SqlMvApplyKeySourceFacts,
    };

    fn schema_contract(source: SqlMvApplyKeySourceFacts) -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 2,
            base: BaseContract {
                table_fqn: "ice.db.base".to_string(),
                table_uuid: "base-uuid".to_string(),
                alias_at_create: None,
                schema_id_at_create: 1,
                schema_at_create: BaseSchemaSnapshot { fields: vec![] },
            },
            bases: vec![],
            output: OutputContract {
                columns: vec![],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "ice.db.mv".to_string(),
                table_uuid: "mv-uuid".to_string(),
                schema_id_at_create: 1,
                visible_columns: vec![],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: source.column_name().to_string(),
                    target_field_id: 1,
                    source: source.into(),
                },
                partition: None,
            },
        }
    }

    #[test]
    fn apply_key_sources_map_to_runtime_identities() {
        assert_eq!(
            apply_key_source_to_refresh_identity(SqlMvApplyKeySourceFacts::BaseRowId.into()),
            RefreshIdentity::BaseRowId
        );
        assert_eq!(
            apply_key_source_to_refresh_identity(SqlMvApplyKeySourceFacts::JoinRowKey.into()),
            RefreshIdentity::JoinRowKey
        );
        assert_eq!(
            apply_key_source_to_refresh_identity(SqlMvApplyKeySourceFacts::GroupRowId.into()),
            RefreshIdentity::GroupRowId
        );
    }

    #[test]
    fn schema_shapes_reconstruct_refresh_capabilities() {
        let projection = schema_contract(SqlMvApplyKeySourceFacts::BaseRowId);

        let mut join = schema_contract(SqlMvApplyKeySourceFacts::JoinRowKey);
        join.join = Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![],
        });

        let mut branch_aggregate = schema_contract(SqlMvApplyKeySourceFacts::GroupRowId);
        branch_aggregate.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            state_columns: vec![],
        });
        branch_aggregate.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: "__nova_branch_id".to_string(),
                target_field_id: 2,
            },
            branch_count: 2,
            inner_apply_key_source: SqlMvApplyKeySourceFacts::GroupRowId.into(),
        });

        let cases = [
            (
                "projection",
                projection,
                BaseSnapshotPolicy::SingleBase,
                false,
                RefreshIdentity::BaseRowId,
                ApplyKeyValueType::Int64,
            ),
            (
                "join",
                join,
                BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                false,
                RefreshIdentity::JoinRowKey,
                ApplyKeyValueType::Utf8,
            ),
            (
                "branch aggregate",
                branch_aggregate,
                BaseSnapshotPolicy::AllBasesRequired,
                true,
                RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId)),
                ApplyKeyValueType::BranchUtf8,
            ),
        ];

        for (label, contract, snapshot_policy, has_agg_state, identity, value_type) in cases {
            let capabilities = RefreshCapabilities::from_schema_contract(&contract)
                .unwrap_or_else(|error| panic!("{label}: {error}"));
            assert_eq!(capabilities.snapshot_policy, snapshot_policy, "{label}");
            assert_eq!(capabilities.has_agg_state, has_agg_state, "{label}");
            assert_eq!(capabilities.identity, identity, "{label}");
            assert_eq!(capabilities.apply_key_value_type, value_type, "{label}");
            assert_eq!(
                capabilities.partition_pruning,
                PartitionPruningPolicy::BestEffort,
                "{label}"
            );
        }
    }

    #[test]
    fn unsupported_join_branch_shape_fails_fast() {
        let mut contract = schema_contract(SqlMvApplyKeySourceFacts::BaseRowId);
        contract.join = Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![],
        });
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: "__nova_branch_id".to_string(),
                target_field_id: 2,
            },
            branch_count: 2,
            inner_apply_key_source: SqlMvApplyKeySourceFacts::BaseRowId.into(),
        });

        let error = RefreshCapabilities::from_schema_contract(&contract)
            .expect_err("join + branch without aggregate must be rejected");
        assert_eq!(
            error,
            "unsupported schema contract shape (join=true, agg=false, branch=true)"
        );
    }
}
