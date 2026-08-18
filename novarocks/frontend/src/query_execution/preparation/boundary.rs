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

use std::collections::{BTreeMap, BTreeSet};

use novarocks_sql::plan_read::{BoundaryContract, BoundaryKind, FragmentEdgeKind, FragmentId};

type BoundaryKey = (FragmentId, Option<i32>, BoundaryKind);

pub(super) fn validate_and_group_boundary_contracts(
    result_fragment_id: Option<FragmentId>,
    write_contract_fragment_ids: &BTreeSet<FragmentId>,
    edges: &[novarocks_sql::plan_read::FragmentEdge],
    contracts: &[BoundaryContract],
    sealed_ids: &BTreeSet<FragmentId>,
) -> Result<BTreeMap<FragmentId, Vec<BoundaryContract>>, String> {
    let mut expected = BTreeSet::<BoundaryKey>::new();
    if let Some(fragment_id) = result_fragment_id {
        expected.insert((fragment_id, None, BoundaryKind::ResultOutput));
    }
    for &fragment_id in write_contract_fragment_ids {
        expected.insert((fragment_id, None, BoundaryKind::IcebergWriteInput));
    }
    for edge in edges {
        expected.insert((
            edge.source_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeSend,
        ));
        expected.insert((
            edge.target_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeReceive,
        ));
        if matches!(edge.edge_kind, FragmentEdgeKind::ChangeStreamRouter { .. }) {
            expected.insert((
                edge.source_fragment_id,
                None,
                BoundaryKind::ChangeStreamRouterInput,
            ));
        }
    }

    let mut actual = BTreeMap::<BoundaryKey, &BoundaryContract>::new();
    let mut occurrences = BTreeSet::new();
    for contract in contracts {
        let key = (contract.fragment_id, contract.node_id, contract.kind);
        if !sealed_ids.contains(&contract.fragment_id) {
            return Err(format!(
                "prepared boundary {key:?} references unknown fragment id"
            ));
        }
        if actual.insert(key, contract).is_some() {
            return Err(format!(
                "prepared boundary group occurs more than once: {key:?}"
            ));
        }
        for (ordinal, column) in contract.columns.iter().enumerate() {
            if column.output_ordinal != ordinal {
                return Err(format!(
                    "prepared boundary {key:?} column ordinal mismatch: expected={ordinal} actual={}",
                    column.output_ordinal
                ));
            }
            if !occurrences.insert(column.execution_column_id) {
                return Err(format!(
                    "prepared boundary occurrence id={} is duplicated",
                    column.execution_column_id.value()
                ));
            }
        }
    }
    let actual_keys = actual.keys().copied().collect::<BTreeSet<_>>();
    if actual_keys != expected {
        return Err(format!(
            "prepared boundary groups mismatch: expected={expected:?} actual={actual_keys:?} missing={:?} unknown={:?}",
            expected.difference(&actual_keys).collect::<Vec<_>>(),
            actual_keys.difference(&expected).collect::<Vec<_>>()
        ));
    }
    for edge in edges {
        let send = actual[&(
            edge.source_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeSend,
        )];
        let receive = actual[&(
            edge.target_fragment_id,
            Some(edge.target_exchange_node_id),
            BoundaryKind::ExchangeReceive,
        )];
        if send.columns.len() != receive.columns.len()
            || send
                .columns
                .iter()
                .zip(&receive.columns)
                .any(|(send, receive)| {
                    send.column_id != receive.column_id
                        || send.output_ordinal != receive.output_ordinal
                        || send.name != receive.name
                        || send.data_type != receive.data_type
                        || send.nullable != receive.nullable
                        || send.is_internal != receive.is_internal
                })
        {
            return Err(format!(
                "prepared exchange boundary columns differ for target fragment={} node_id={}",
                edge.target_fragment_id, edge.target_exchange_node_id
            ));
        }
    }

    let mut by_fragment = BTreeMap::<FragmentId, Vec<BoundaryContract>>::new();
    for contract in contracts {
        by_fragment
            .entry(contract.fragment_id)
            .or_default()
            .push(contract.clone());
    }
    Ok(by_fragment)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::validate_and_group_boundary_contracts;
    use novarocks_sql::plan_read::{BoundaryContract, FragmentId};

    fn validate_contracts(
        plan: &novarocks_sql::plan_read::DistributedPlan,
        contracts: &[BoundaryContract],
    ) -> Result<BTreeMap<FragmentId, Vec<BoundaryContract>>, String> {
        let facts =
            novarocks_sql::planning::query_execution::project_execution_preparation_facts(plan);
        validate_and_group_boundary_contracts(
            facts.result_fragment_id(),
            &BTreeSet::new(),
            plan.edges(),
            contracts,
            &plan
                .fragments()
                .iter()
                .map(|fragment| fragment.fragment_id)
                .collect(),
        )
    }

    #[test]
    fn malformed_boundary_groups_and_occurrences_use_production_validation() {
        let plan = super::super::test_support::result_plan();
        let facts =
            novarocks_sql::planning::query_execution::project_execution_preparation_facts(&plan);
        let valid = facts.boundary_contracts();
        validate_contracts(&plan, valid).expect("sealed boundary catalog");

        let missing = validate_contracts(&plan, &[]).expect_err("missing group must fail");
        assert!(missing.contains("boundary groups mismatch"), "{missing}");

        let duplicate = vec![valid[0].clone(), valid[0].clone()];
        let duplicate_error =
            validate_contracts(&plan, &duplicate).expect_err("duplicate group must fail");
        assert!(
            duplicate_error.contains("boundary group occurs more than once"),
            "{duplicate_error}"
        );

        let mut unknown = valid.to_vec();
        unknown[0].node_id = Some(999);
        let unknown_error =
            validate_contracts(&plan, &unknown).expect_err("unknown group must fail");
        assert!(unknown_error.contains("unknown="), "{unknown_error}");

        let mut duplicate_occurrence = valid.to_vec();
        duplicate_occurrence[0].columns[1].execution_column_id =
            duplicate_occurrence[0].columns[0].execution_column_id;
        let occurrence_error = validate_contracts(&plan, &duplicate_occurrence)
            .expect_err("duplicate occurrence must fail");
        assert!(
            occurrence_error.contains("occurrence id=") && occurrence_error.contains("duplicated"),
            "{occurrence_error}"
        );
    }
}
