// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Core mapping for SQL-owned Iceberg IMV refresh facts.

use crate::mv::domain::refresh::apply_key::ApplyKeyContract;
use crate::mv::domain::refresh::contract::{
    AggregateRefreshContract, BranchRefreshContract, ImvRefreshContract, JoinRefreshContract,
};
pub(crate) use novarocks_sql::planning::mv::{RefreshFragmentProperty, TargetIdentity};
use novarocks_sql::planning::mv::{SqlImvApplyKeyFacts, SqlImvRefreshContractFacts};

pub(crate) fn derive_imv_refresh_contract(
    analysis: &crate::mv::domain::analysis::MvAnalysis,
) -> Result<ImvRefreshContract, String> {
    Ok(map_sql_imv_refresh_contract(
        analysis.refresh_input.refresh_contract()?,
    ))
}

pub fn derive_fragment_property(
    analysis: &crate::mv::domain::analysis::MvAnalysis,
) -> Result<RefreshFragmentProperty, String> {
    analysis.refresh_input.refresh_property()
}

pub(crate) fn map_sql_imv_refresh_contract(
    value: SqlImvRefreshContractFacts,
) -> ImvRefreshContract {
    let apply_key = match value.apply_key {
        SqlImvApplyKeyFacts::ProjectionFilter => ApplyKeyContract::projection_filter(),
        SqlImvApplyKeyFacts::UnionProjectionFilter => ApplyKeyContract::union_projection_filter(),
        SqlImvApplyKeyFacts::JoinProjectionFilter => ApplyKeyContract::join_projection_filter(),
        SqlImvApplyKeyFacts::AggregateGroupRow => ApplyKeyContract::aggregate_group_row(),
        SqlImvApplyKeyFacts::JoinAggregateGroupRow => ApplyKeyContract::join_aggregate_group_row(),
        SqlImvApplyKeyFacts::BranchUnionAggregateGroupRow => {
            ApplyKeyContract::branch_union_aggregate_group_row()
        }
    };
    ImvRefreshContract {
        base_refs: value.base_refs,
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
            apply_key: SqlImvApplyKeyFacts::BranchUnionAggregateGroupRow,
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
