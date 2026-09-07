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

//! Role-neutral values produced by a query execution scheduler.
//!
//! Scheduling policy belongs to the frontend. Core only consumes this sealed
//! description while preparing protocol payloads and runtime-filter routes.

use std::collections::BTreeMap;

use novarocks_execution::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_sql::plan_read::FragmentId;
use novarocks_types::UniqueId;

/// Placement information for one fragment instance.
#[derive(Clone, Debug)]
pub struct FragmentInstancePlacement {
    pub fragment_id: FragmentId,
    pub instance_index: usize,
    pub finst_id: UniqueId,
    pub backend_idx: usize,
    pub endpoint: RuntimeEndpoint,
    pub scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    pub destinations: Vec<FragmentDestination>,
    pub per_exch_num_senders: BTreeMap<i32, i32>,
}

/// A sealed, role-neutral scheduling result.
#[derive(Clone, Debug)]
pub struct SchedulingPlan {
    pub root_fragment_id: FragmentId,
    pub by_fragment: BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub root_finst_id: UniqueId,
    pub root_backend_idx: usize,
}

impl SchedulingPlan {
    pub(crate) fn fragment_ids(&self) -> impl ExactSizeIterator<Item = FragmentId> + '_ {
        self.by_fragment.keys().copied()
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Scheduling test helper preserves per-fragment placement assertions."
    )]
    pub(crate) fn placements_for_fragment_for_test(
        &self,
        fragment_id: FragmentId,
    ) -> Option<&[FragmentInstancePlacement]> {
        self.by_fragment.get(&fragment_id).map(Vec::as_slice)
    }
}
