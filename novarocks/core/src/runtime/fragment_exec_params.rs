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

use std::collections::BTreeMap;

use crate::common::types::UniqueId;
use crate::runtime::scan_range::ScanRangeParams;
use novarocks_execution::runtime::endpoint::FragmentDestination;

#[derive(Clone, Debug)]
pub(crate) struct FragmentExecParams {
    query_id: UniqueId,
    fragment_instance_id: UniqueId,
    per_node_scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
    per_exch_num_senders: BTreeMap<i32, i32>,
    destinations: Vec<FragmentDestination>,
}

impl FragmentExecParams {
    pub(crate) fn new(
        query_id: UniqueId,
        fragment_instance_id: UniqueId,
        per_node_scan_ranges: BTreeMap<i32, Vec<ScanRangeParams>>,
        per_exch_num_senders: BTreeMap<i32, i32>,
        destinations: Vec<FragmentDestination>,
    ) -> Result<Self, String> {
        validate_sender_counts(&per_exch_num_senders)?;
        Ok(Self {
            query_id,
            fragment_instance_id,
            per_node_scan_ranges,
            per_exch_num_senders,
            destinations,
        })
    }

    pub(crate) fn query_id(&self) -> UniqueId {
        self.query_id
    }

    pub(crate) fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub(crate) fn per_node_scan_ranges(&self) -> &BTreeMap<i32, Vec<ScanRangeParams>> {
        &self.per_node_scan_ranges
    }

    pub(crate) fn per_exch_num_senders(&self) -> &BTreeMap<i32, i32> {
        &self.per_exch_num_senders
    }

    pub(crate) fn destinations(&self) -> &[FragmentDestination] {
        &self.destinations
    }
}

fn validate_sender_counts(per_exch_num_senders: &BTreeMap<i32, i32>) -> Result<(), String> {
    for (node_id, count) in per_exch_num_senders {
        if *count <= 0 {
            return Err(format!(
                "native FragmentExecParams per_exch_num_senders node_id={} must be positive, got {}",
                node_id, count
            ));
        }
    }
    Ok(())
}
