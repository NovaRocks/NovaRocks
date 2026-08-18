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

use std::collections::BTreeSet;

use novarocks_sql::plan_read::FragmentEdgeKind;

pub(super) fn sealed_cte_projection(
    edges: &[novarocks_sql::plan_read::FragmentEdge],
    fragment: &novarocks_sql::plan_read::PlanFragment,
) -> Result<
    (
        Option<novarocks_sql::plan_read::CteId>,
        Vec<(
            novarocks_sql::plan_read::CteId,
            i32,
            Vec<novarocks_sql::plan_read::ColumnId>,
        )>,
    ),
    String,
> {
    let producer_ids = edges
        .iter()
        .filter_map(|edge| {
            (edge.source_fragment_id == fragment.fragment_id)
                .then_some(&edge.edge_kind)
                .and_then(|kind| match kind {
                    FragmentEdgeKind::CteMulticast { cte_id, .. } => Some(*cte_id),
                    _ => None,
                })
        })
        .collect::<BTreeSet<_>>();
    let cte_id = match producer_ids.len() {
        0 => None,
        1 => producer_ids.iter().next().copied(),
        _ => {
            return Err(format!(
                "prepared fragment {} has multiple sealed CTE producer ids {producer_ids:?}",
                fragment.fragment_id
            ));
        }
    };
    if fragment.cte_id != cte_id {
        return Err(format!(
            "prepared fragment {} CTE producer mismatch: declared={:?} sealed={cte_id:?}",
            fragment.fragment_id, fragment.cte_id
        ));
    }
    let mut consumers = edges
        .iter()
        .filter_map(|edge| match &edge.edge_kind {
            FragmentEdgeKind::CteMulticast {
                cte_id,
                receive_producer_column_ids,
            } if edge.target_fragment_id == fragment.fragment_id => Some((
                *cte_id,
                edge.target_exchange_node_id,
                receive_producer_column_ids.clone(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    consumers.sort();
    let mut declared = fragment.cte_exchange_nodes.clone();
    declared.sort();
    if declared != consumers {
        return Err(format!(
            "prepared fragment {} CTE consumers mismatch: declared={declared:?} sealed={consumers:?}",
            fragment.fragment_id
        ));
    }
    Ok((cte_id, consumers))
}

#[cfg(test)]
mod tests {
    use super::sealed_cte_projection;
    use novarocks_sql::test_support::{NativeBuildFixture, native_build_plan};

    #[test]
    fn sealed_cte_multicast_projection_sorts_edges_and_preserves_receive_occurrence_order() {
        let plan = native_build_plan(NativeBuildFixture::CteMulticastOrdering)
            .expect("sealed CTE ordering fixture");
        let fragment = plan
            .fragments()
            .iter()
            .find(|fragment| fragment.fragment_id == plan.root_fragment_id())
            .expect("result fragment");

        let (producer, consumers) =
            sealed_cte_projection(plan.edges(), fragment).expect("sealed CTE projection");

        assert_eq!(producer, None);
        assert_eq!(consumers.len(), 2);
        assert_eq!(
            consumers
                .iter()
                .map(|(cte_id, exchange_node_id, column_ids)| {
                    (
                        *cte_id,
                        *exchange_node_id,
                        column_ids.iter().map(|id| id.0).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![(42, 3, vec![3, 1]), (42, 11, vec![4, 2])]
        );
    }
}
