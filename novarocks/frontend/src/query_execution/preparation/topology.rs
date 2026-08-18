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

use novarocks_sql::plan_read::table::ScanSource;
use novarocks_sql::plan_read::{DistributedNode, DistributedNodeKind, FragmentId};

pub(super) fn validate_topology_roles(
    sealed_ids: &BTreeSet<FragmentId>,
    result_fragment_id: Option<FragmentId>,
    terminal_write_fragment_ids: &BTreeSet<FragmentId>,
    producer_fragment_ids: &BTreeSet<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
) -> Result<(), String> {
    for (label, ids) in [
        ("terminal write", terminal_write_fragment_ids),
        ("producer", producer_fragment_ids),
    ] {
        if !ids.is_subset(sealed_ids) {
            return Err(format!(
                "prepared {label} fragment ids {ids:?} are not a subset of sealed fragment ids {sealed_ids:?}"
            ));
        }
    }
    if let Some(result_fragment_id) = result_fragment_id {
        if !sealed_ids.contains(&result_fragment_id) {
            return Err(format!(
                "prepared result fragment {result_fragment_id} is not among sealed fragment ids {sealed_ids:?}"
            ));
        }
        if terminal_write_fragment_ids.contains(&result_fragment_id)
            || producer_fragment_ids.contains(&result_fragment_id)
        {
            return Err(format!(
                "prepared result fragment {result_fragment_id} overlaps terminal-write or producer roles"
            ));
        }
    }
    if !terminal_write_fragment_ids.is_disjoint(producer_fragment_ids) {
        return Err(format!(
            "prepared terminal-write and producer roles overlap: terminal={terminal_write_fragment_ids:?} producer={producer_fragment_ids:?}"
        ));
    }
    let classified = producer_fragment_ids
        .iter()
        .chain(terminal_write_fragment_ids)
        .copied()
        .chain(result_fragment_id)
        .collect::<BTreeSet<_>>();
    let allowed_unclassified = BTreeSet::from([execution_anchor_fragment_id]);
    let unclassified = sealed_ids
        .difference(&classified)
        .copied()
        .collect::<BTreeSet<_>>();
    if !unclassified.is_subset(&allowed_unclassified) {
        return Err(format!(
            "prepared fragments have no sealed topology role: {unclassified:?}"
        ));
    }
    Ok(())
}

pub(super) fn validate_binding_keys<T>(
    label: &str,
    expected: &BTreeSet<T>,
    actual: &BTreeSet<T>,
) -> Result<(), String>
where
    T: Copy + Ord + std::fmt::Debug,
{
    if actual == expected {
        return Ok(());
    }
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    Err(format!(
        "prepared {label} mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    ))
}

pub(super) fn collect_scan_nodes<'a>(
    fragment_id: FragmentId,
    node: &'a DistributedNode,
    out: &mut Vec<(i32, &'a ScanSource)>,
) {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        out.push((node.node_id, &scan.table.source));
    }
    for child in &node.children {
        if child.fragment_id == fragment_id {
            collect_scan_nodes(fragment_id, child, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::validate_topology_roles;

    #[test]
    fn sealed_topology_roles_reject_unclassified_fragments() {
        let error = validate_topology_roles(
            &BTreeSet::from([7, 8]),
            Some(7),
            &BTreeSet::new(),
            &BTreeSet::new(),
            7,
        )
        .expect_err("unclassified sealed fragment must fail");

        assert_eq!(
            error,
            "prepared fragments have no sealed topology role: {8}"
        );
    }
}
