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

use crate::mv::domain::dependency::model::{MvDependencyObjectRef, MvDependencyObjectType};

pub(crate) fn validate_no_cycle_for_edges(
    new_target: &MvDependencyObjectRef,
    new_upstreams: &[MvDependencyObjectRef],
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<(), String> {
    let mut graph: BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> = BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }
    graph.insert(new_target.clone(), new_upstreams.to_vec());

    fn visit(
        graph: &BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        node: &MvDependencyObjectRef,
        target: &MvDependencyObjectRef,
        path: &mut Vec<MvDependencyObjectRef>,
    ) -> Option<Vec<MvDependencyObjectRef>> {
        if path.contains(node) {
            return None;
        }
        path.push(node.clone());
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if &upstream == target {
                let mut cycle = path.clone();
                cycle.push(upstream);
                return Some(cycle);
            }
            if upstream.object_type == MvDependencyObjectType::MaterializedView
                && let Some(cycle) = visit(graph, &upstream, target, path)
            {
                return Some(cycle);
            }
        }
        path.pop();
        None
    }

    if let Some(cycle) = visit(&graph, new_target, new_target, &mut Vec::new()) {
        let display = cycle
            .iter()
            .map(MvDependencyObjectRef::display_name)
            .collect::<Vec<_>>()
            .join(" -> ");
        return Err(format!("dependency cycle detected: {display}"));
    }
    Ok(())
}

pub(crate) fn topological_upstream_order_for_edges(
    target: &MvDependencyObjectRef,
    existing_edges: &[(MvDependencyObjectRef, Vec<MvDependencyObjectRef>)],
) -> Result<Vec<MvDependencyObjectRef>, String> {
    let mut graph: BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>> = BTreeMap::new();
    for (downstream, upstreams) in existing_edges {
        graph.insert(downstream.clone(), upstreams.clone());
    }

    let mut permanent = BTreeSet::new();
    let mut temporary = BTreeSet::new();
    let mut ordered = Vec::new();

    fn visit(
        node: &MvDependencyObjectRef,
        graph: &BTreeMap<MvDependencyObjectRef, Vec<MvDependencyObjectRef>>,
        permanent: &mut BTreeSet<MvDependencyObjectRef>,
        temporary: &mut BTreeSet<MvDependencyObjectRef>,
        ordered: &mut Vec<MvDependencyObjectRef>,
    ) -> Result<(), String> {
        if permanent.contains(node) {
            return Ok(());
        }
        if !temporary.insert(node.clone()) {
            return Err(format!(
                "dependency cycle detected while planning refresh at {}",
                node.display_name()
            ));
        }
        for upstream in graph.get(node).cloned().unwrap_or_default() {
            if upstream.object_type == MvDependencyObjectType::MaterializedView {
                visit(&upstream, graph, permanent, temporary, ordered)?;
            }
        }
        temporary.remove(node);
        permanent.insert(node.clone());
        ordered.push(node.clone());
        Ok(())
    }

    visit(target, &graph, &mut permanent, &mut temporary, &mut ordered)?;
    Ok(ordered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::dependency::model::iceberg_mv_dependency_ref;

    #[test]
    fn dependency_cycle_detector_rejects_new_back_edge() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![
            (mv_a.clone(), vec![mv_b.clone()]),
            (mv_b.clone(), vec![mv_c.clone()]),
        ];

        let err = validate_no_cycle_for_edges(&mv_c, &[mv_a.clone()], &existing)
            .expect_err("c -> a should form a cycle");
        assert_eq!(
            err,
            "dependency cycle detected: mv:ice.sales.mv_c -> mv:ice.sales.mv_a -> mv:ice.sales.mv_b -> mv:ice.sales.mv_c"
        );
    }

    #[test]
    fn dependency_cycle_detector_accepts_dag() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let existing = vec![(mv_b.clone(), vec![mv_a.clone()])];

        validate_no_cycle_for_edges(&mv_c, &[mv_b], &existing).expect("dag should be accepted");
        let _ = mv_a;
    }

    #[test]
    fn topological_upstream_order_runs_deepest_first() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let edges = vec![
            (mv_b.clone(), vec![mv_a.clone()]),
            (mv_c.clone(), vec![mv_b.clone()]),
        ];

        let order = topological_upstream_order_for_edges(&mv_c, &edges).expect("order");
        assert_eq!(order, vec![mv_a, mv_b, mv_c]);
    }

    #[test]
    fn topological_upstream_order_deduplicates_shared_dependencies() {
        let mv_a = iceberg_mv_dependency_ref("ice", "sales", "mv_a");
        let mv_b = iceberg_mv_dependency_ref("ice", "sales", "mv_b");
        let mv_c = iceberg_mv_dependency_ref("ice", "sales", "mv_c");
        let mv_d = iceberg_mv_dependency_ref("ice", "sales", "mv_d");
        let edges = vec![
            (mv_b.clone(), vec![mv_a.clone()]),
            (mv_c.clone(), vec![mv_a.clone()]),
            (mv_d.clone(), vec![mv_b.clone(), mv_c.clone()]),
        ];

        let order = topological_upstream_order_for_edges(&mv_d, &edges).expect("order");
        assert_eq!(order, vec![mv_a, mv_b, mv_c, mv_d]);
    }
}
