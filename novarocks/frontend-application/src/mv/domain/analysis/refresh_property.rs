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

//! Core mapping for SQL-owned Iceberg IMV refresh facts.

use crate::mv::domain::refresh::apply_key::ApplyKeyContract;
use crate::mv::domain::refresh::contract::{
    AggregateRefreshContract, BranchRefreshContract, ImvBaseRelationOccurrence, ImvRefreshContract,
    JoinRefreshContract,
};
pub(crate) use novarocks_sql::planning::mv::{RefreshFragmentProperty, TargetIdentity};
use novarocks_sql::planning::mv::{SqlImvRefreshContractFacts, SqlImvRefreshShape};

pub(crate) fn derive_imv_refresh_contract(
    analysis: &crate::mv::domain::analysis::MvAnalysis,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ImvRefreshContract, novarocks_sql::compiler::SqlCompileError> {
    Ok(map_sql_imv_refresh_contract(
        analysis.refresh_input.refresh_contract(control)?,
    ))
}

pub fn derive_fragment_property(
    analysis: &crate::mv::domain::analysis::MvAnalysis,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<RefreshFragmentProperty, novarocks_sql::compiler::SqlCompileError> {
    analysis.refresh_input.refresh_property(control)
}

pub(crate) fn map_sql_imv_refresh_contract(
    value: SqlImvRefreshContractFacts,
) -> ImvRefreshContract {
    let apply_key = match value.shape {
        SqlImvRefreshShape::ProjectionFilter => ApplyKeyContract::projection_filter(),
        SqlImvRefreshShape::UnionProjectionFilter => ApplyKeyContract::union_projection_filter(),
        SqlImvRefreshShape::JoinProjectionFilter => ApplyKeyContract::join_projection_filter(),
        SqlImvRefreshShape::Aggregate => ApplyKeyContract::aggregate_group_row(),
        SqlImvRefreshShape::JoinAggregate => ApplyKeyContract::join_aggregate_group_row(),
        SqlImvRefreshShape::BranchUnionAggregate => {
            ApplyKeyContract::branch_union_aggregate_group_row()
        }
    };
    ImvRefreshContract {
        base_refs: value
            .base_refs
            .into_iter()
            .map(|base| ImvBaseRelationOccurrence {
                occurrence_id: base.occurrence_id,
                table: base.table,
            })
            .collect(),
        apply_key,
        aggregate: value.aggregate.map(|value| AggregateRefreshContract {
            group_key_count: value.group_key_count,
            aggregate_count: value.aggregate_count,
        }),
        join: value.join.map(|value| JoinRefreshContract {
            join_key_count: value.join_key_count,
        }),
        branch: value.branch.map(|value| BranchRefreshContract {
            branch_count: value.branch_count,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::planning::mv::{SqlImvAggregateFacts, SqlImvBranchFacts};
    #[test]
    fn maps_sql_refresh_contract_to_execution_contract() {
        let mapped = map_sql_imv_refresh_contract(SqlImvRefreshContractFacts {
            base_refs: Vec::new(),
            shape: SqlImvRefreshShape::BranchUnionAggregate,
            aggregate: Some(SqlImvAggregateFacts {
                group_key_count: 2,
                aggregate_count: 3,
            }),
            join: None,
            branch: Some(SqlImvBranchFacts { branch_count: 4 }),
        });
        assert_eq!(mapped.aggregate.unwrap().aggregate_count, 3);
        assert_eq!(mapped.branch.unwrap().branch_count, 4);
        assert_eq!(
            mapped.apply_key,
            ApplyKeyContract::branch_union_aggregate_group_row()
        );
    }
}
