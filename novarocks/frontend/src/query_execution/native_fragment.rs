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

//! Request-local native fragment facts and their consuming attachment.
//!
//! This is intentionally a query-execution capability rather than a native
//! encoder type.  The Frontend will read the view and produce native DTOs;
//! Core only verifies that those DTOs still belong to the exact prepared
//! artifact before it finalizes the distributed request.

use std::collections::{BTreeMap, BTreeSet, btree_map};

use novarocks_protocol::plan::PlanFragment as NativePlanFragment;
use novarocks_sql::plan_read::{DistributedPlan, FragmentId};

use crate::query_execution::preparation::{NativeScanFactsView, PreparedFragmentSet};

/// Immutable, borrow-only native encoding facts for one sealed plan and its
/// exact prepared bindings.  It has no public constructor and cannot acquire
/// newer connector, topology, or planning state.
pub struct NativeFragmentEncodingView<'a> {
    plan: &'a DistributedPlan,
    prepared: &'a PreparedFragmentSet,
    provenance: Option<u64>,
}

impl<'a> NativeFragmentEncodingView<'a> {
    pub(crate) fn sealed(
        plan: &'a DistributedPlan,
        prepared: &'a PreparedFragmentSet,
        provenance: u64,
    ) -> Self {
        Self {
            plan,
            prepared,
            provenance: Some(provenance),
        }
    }

    pub(crate) fn unsealed(plan: &'a DistributedPlan, prepared: &'a PreparedFragmentSet) -> Self {
        Self {
            plan,
            prepared,
            provenance: None,
        }
    }

    pub fn distributed_plan(&self) -> &DistributedPlan {
        self.plan
    }

    pub fn scan_facts(&self) -> NativeScanFactsView<'a> {
        NativeScanFactsView::new(self.prepared.scan_bindings())
    }

    pub(crate) fn prepared(&self) -> &PreparedFragmentSet {
        self.prepared
    }

    /// Consume one complete set of Frontend-produced native fragments into an
    /// artifact-bound attachment.  The validation deliberately lives here so
    /// no encoder implementation can forge or reuse a bundle for another
    /// prepared query.
    pub fn seal(
        &self,
        fragments: impl IntoIterator<Item = NativePlanFragment>,
    ) -> Result<NativeFragmentAttachment, String> {
        let sealed_ids = self
            .plan
            .fragments()
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect::<BTreeSet<_>>();
        let prepared_ids = self.prepared.fragment_ids();
        if prepared_ids != sealed_ids {
            return Err(fragment_set_error("prepared", &sealed_ids, &prepared_ids));
        }

        let mut by_fragment = BTreeMap::new();
        for fragment in fragments {
            let fragment_id = fragment.fragment_id;
            if by_fragment.insert(fragment_id, fragment).is_some() {
                return Err(format!(
                    "native fragment bundle encoded duplicate fragment id={fragment_id}"
                ));
            }
        }
        let native_ids = by_fragment.keys().copied().collect::<BTreeSet<_>>();
        if native_ids != sealed_ids {
            return Err(fragment_set_error("native", &sealed_ids, &native_ids));
        }

        Ok(NativeFragmentAttachment {
            by_fragment,
            provenance: self.provenance,
        })
    }
}

/// Complete native payload for one exact encoding view.  Only its view can
/// construct it; Core consumes it exactly once while assembling the request.
#[derive(Debug)]
pub struct NativeFragmentAttachment {
    by_fragment: BTreeMap<FragmentId, NativePlanFragment>,
    provenance: Option<u64>,
}

impl NativeFragmentAttachment {
    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }

    pub(crate) fn fragments_in_id_order(
        &self,
    ) -> impl ExactSizeIterator<Item = (FragmentId, &NativePlanFragment)> + '_ {
        self.by_fragment
            .iter()
            .map(|(&fragment_id, fragment)| (fragment_id, fragment))
    }

    pub(crate) fn get(&self, fragment_id: FragmentId) -> Option<&NativePlanFragment> {
        self.by_fragment.get(&fragment_id)
    }

    pub(crate) fn into_fragments(self) -> btree_map::IntoIter<FragmentId, NativePlanFragment> {
        self.by_fragment.into_iter()
    }

    pub(crate) fn matches_provenance(&self, provenance: u64) -> bool {
        self.provenance == Some(provenance)
    }

    /// Bind the RF-specific payload after generic plan encoding.  This is
    /// consuming so one artifact cannot receive two runtime-filter tables.
    pub(crate) fn bind_runtime_filter_tables(
        mut self,
        tables: BTreeMap<FragmentId, novarocks_protocol::plan::RuntimeFilterBindingTable>,
    ) -> Result<Self, String> {
        let expected = self.by_fragment.keys().copied().collect::<BTreeSet<_>>();
        let actual = tables.keys().copied().collect::<BTreeSet<_>>();
        if expected != actual {
            return Err(fragment_set_error(
                "runtime filter attachment",
                &expected,
                &actual,
            ));
        }
        for (fragment_id, fragment) in &mut self.by_fragment {
            let table = tables
                .get(fragment_id)
                .expect("validated runtime-filter table key set");
            if table.fragment_id != *fragment_id {
                return Err(format!(
                    "runtime filter attachment table fragment mismatch: key={fragment_id} table_fragment_id={}",
                    table.fragment_id
                ));
            }
            if fragment.runtime_filter_bindings.is_some() {
                return Err(format!(
                    "native fragment {fragment_id} already has runtime filter bindings"
                ));
            }
            fragment.runtime_filter_bindings = Some(table.clone());
        }
        Ok(self)
    }
}

#[cfg(test)]
pub(crate) fn native_fragment_attachment_for_contract_test(
    fragments: Vec<NativePlanFragment>,
) -> Result<NativeFragmentAttachment, String> {
    let expected_ids = fragments
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<BTreeSet<_>>();
    let mut by_fragment = BTreeMap::new();
    for fragment in fragments {
        let fragment_id = fragment.fragment_id;
        if by_fragment.insert(fragment_id, fragment).is_some() {
            return Err(format!(
                "native fragment bundle encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    let actual = by_fragment.keys().copied().collect::<BTreeSet<_>>();
    if actual != expected_ids {
        return Err(fragment_set_error("native", &expected_ids, &actual));
    }
    Ok(NativeFragmentAttachment {
        by_fragment,
        provenance: None,
    })
}

#[cfg(test)]
pub(crate) fn native_fragment_attachment_for_test(
    fragments: impl IntoIterator<Item = NativePlanFragment>,
    expected_ids: &BTreeSet<FragmentId>,
    provenance: Option<u64>,
) -> Result<NativeFragmentAttachment, String> {
    let mut by_fragment = BTreeMap::new();
    for fragment in fragments {
        let fragment_id = fragment.fragment_id;
        if by_fragment.insert(fragment_id, fragment).is_some() {
            return Err(format!(
                "native fragment bundle encoded duplicate fragment id={fragment_id}"
            ));
        }
    }
    let actual = by_fragment.keys().copied().collect::<BTreeSet<_>>();
    if actual != *expected_ids {
        return Err(fragment_set_error("native", expected_ids, &actual));
    }
    Ok(NativeFragmentAttachment {
        by_fragment,
        provenance,
    })
}

fn fragment_set_error(
    label: &str,
    expected: &BTreeSet<FragmentId>,
    actual: &BTreeSet<FragmentId>,
) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
    format!(
        "{label} fragment ids mismatch: expected={expected:?} actual={actual:?} missing={missing:?} unknown={unknown:?}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fragment(fragment_id: FragmentId) -> NativePlanFragment {
        NativePlanFragment {
            fragment_id,
            ..Default::default()
        }
    }

    #[test]
    fn test_fixture_rejects_duplicate_ids() {
        let error = native_fragment_attachment_for_test(
            vec![fragment(3), fragment(3)],
            &BTreeSet::from([3]),
            None,
        )
        .expect_err("duplicate attachment ids must fail");
        assert_eq!(
            error,
            "native fragment bundle encoded duplicate fragment id=3"
        );
    }
}
