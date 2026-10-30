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

//! What deciding how many backends run each fragment actually needs.
//!
//! Scheduling reads a narrow set of facts: which fragments exist and in what
//! order, which of them read a provider and how that read receives its work,
//! and how rows move between fragments. It reads none of the plan's semantics,
//! so it is given none - which is what lets one scheduler serve a plan in any
//! shape, and what keeps a change in plan representation from reaching it.

use std::collections::BTreeMap;

use crate::query_execution::artifact::FragmentId;
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_query_application::api::RuntimeSplitPlacement;
use novarocks_query_application::api::{
    ExecutionSchedulingFacts, FragmentSchedulingFacts as QueryApplicationFragmentSchedulingFacts,
    NativeScanWork, PlanScanIdentity, PlanSeal, ScanSchedulingFacts,
    SchedulingEdgeFacts as QueryApplicationEdgeFacts,
    SchedulingStreamKind as QueryApplicationStreamKind,
};

/// How rows move across one fragment boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulingStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

/// One provider read, as scheduling sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulingScanFacts {
    pub node_id: i32,
    /// The pieces of work this scan is already known to have, to spread across
    /// its instances.
    ///
    /// Ordinarily empty: enumeration belongs to the attempt, not to
    /// preparation, and a scan that starts with no work still has to be
    /// admitted so it can be told there is none.
    pub ranges: Vec<ScanRangeParams>,
    /// Runtime splits can use every admitted task; a whole relation has no
    /// split and must run on exactly one backend. Losing the distinction here
    /// would duplicate a direct metadata read on every live backend.
    pub runtime_splits: Option<RuntimeSplitPlacement>,
}

/// One fragment, as scheduling sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct SchedulingFragmentFacts {
    pub placement: novarocks_physical_plan::StagePlacementPolicy,
    pub scans: Vec<SchedulingScanFacts>,
}

/// One edge, as scheduling sees it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulingEdgeFacts {
    pub source: FragmentId,
    pub target: FragmentId,
    /// The exchange node in the target fragment that receives this edge.
    pub target_exchange_node_id: i32,
    pub native_hash_partitioned: bool,
    pub stream_kind: SchedulingStreamKind,
}

/// Everything scheduling reads about one plan.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentSchedulingFacts {
    /// The artifact these facts were projected from, so a schedule cannot be
    /// paired with another preparation's fragments.
    pub handoff_id: u64,
    /// Producers before consumers. Scheduling walks this order so a fragment's
    /// parallelism can depend on the fragments that feed it.
    pub order: Vec<FragmentId>,
    pub anchor: FragmentId,
    pub fragments: BTreeMap<FragmentId, SchedulingFragmentFacts>,
    pub edges: Vec<SchedulingEdgeFacts>,
}

impl FragmentSchedulingFacts {
    pub fn fragment(&self, fragment_id: FragmentId) -> Option<&SchedulingFragmentFacts> {
        self.fragments.get(&fragment_id)
    }

    /// The work one scan is already known to have. Absent means the plan has
    /// no such scan; empty means it has one that starts with no work, which
    /// still has to be admitted so it can be told there is none.
    pub fn scan_ranges(&self, fragment_id: FragmentId, node_id: i32) -> Option<&[ScanRangeParams]> {
        self.fragment(fragment_id)?
            .scans
            .iter()
            .find(|scan| scan.node_id == node_id)
            .map(|scan| scan.ranges.as_slice())
    }
}

impl FragmentSchedulingFacts {
    /// The same facts, as the owner that places tasks reads them.
    ///
    /// That owner asks a narrower question than this projection answers: it
    /// needs how much work each read starts with, not which pieces of work
    /// those are, and it addresses a read by identity rather than by
    /// position. Deriving its view from this one means a sealed plan and a
    /// completed plan reach it the same way, because they already reach this
    /// one the same way.
    pub(crate) fn attempt_scheduling_facts(
        &self,
        plan: PlanSeal,
    ) -> Result<ExecutionSchedulingFacts, String> {
        Ok(ExecutionSchedulingFacts {
            topological_fragment_order: self.order.clone(),
            execution_anchor_fragment_id: self.anchor,
            fragments: self
                .fragments
                .iter()
                .map(|(&fragment_id, fragment)| {
                    Ok(QueryApplicationFragmentSchedulingFacts {
                        fragment_id,
                        placement: fragment.placement.clone(),
                        scans: fragment
                            .scans
                            .iter()
                            .map(|scan| {
                                Ok(ScanSchedulingFacts {
                                    scan: PlanScanIdentity::new(plan, fragment_id, scan.node_id),
                                    work: attempt_scan_work(fragment_id, scan)?,
                                })
                            })
                            .collect::<Result<Vec<_>, String>>()?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            edges: self
                .edges
                .iter()
                .map(|edge| QueryApplicationEdgeFacts {
                    source_fragment_id: edge.source,
                    target_fragment_id: edge.target,
                    target_exchange_node_id: edge.target_exchange_node_id,
                    stream_kind: match edge.stream_kind {
                        SchedulingStreamKind::Gather => QueryApplicationStreamKind::Gather,
                        SchedulingStreamKind::Broadcast => QueryApplicationStreamKind::Broadcast,
                        SchedulingStreamKind::Partitioned => {
                            QueryApplicationStreamKind::Partitioned
                        }
                        SchedulingStreamKind::Other => QueryApplicationStreamKind::Other,
                    },
                    hash_partitioned: edge.native_hash_partitioned,
                })
                .collect(),
        })
    }
}

/// How much work one read starts with.
///
/// A provider freezes its placement hint before enumerating runtime splits.
/// Otherwise work is the pieces already known, which may legitimately be none -- a scan
/// that starts with nothing still has to be admitted so it can be told there
/// is nothing.
fn attempt_scan_work(
    fragment_id: FragmentId,
    scan: &SchedulingScanFacts,
) -> Result<NativeScanWork, String> {
    Ok(match scan.runtime_splits {
        Some(placement) => NativeScanWork::RuntimeSplits { placement },
        None => match std::num::NonZeroUsize::new(scan.ranges.len()) {
            Some(count) => NativeScanWork::FrozenUnits { count },
            None => {
                let _ = fragment_id;
                NativeScanWork::Empty
            }
        },
    })
}

impl SchedulingFragmentFacts {
    #[cfg(test)]
    pub fn has_scans(&self) -> bool {
        !self.scans.is_empty()
    }
}
