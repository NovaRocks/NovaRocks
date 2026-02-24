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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::common::ids::SlotId;
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, StarrocksRuntimeFilterType,
    decode_starrocks_in_filter, decode_starrocks_membership_filter, encode_starrocks_bloom_filter,
    encode_starrocks_empty_filter, encode_starrocks_in_filter, peek_starrocks_filter_type,
};
use crate::runtime::query_context::QueryId;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime_filter;
use crate::service::exchange_sender;
use crate::service::grpc_client::proto::starrocks::PTransmitRuntimeFilterParams;
use crate::novarocks_logging::{debug, warn};

pub(crate) struct RuntimeFilterWorker {
    query_id: QueryId,
    params: runtime_filter::TRuntimeFilterParams,
    hub: Arc<RuntimeFilterHub>,
    merge_states: Mutex<HashMap<i32, MergeState>>,
}

struct MergeState {
    expected: usize,
    received: HashMap<i32, RuntimeFilterPayload>,
    done: bool,
}

impl MergeState {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            received: HashMap::new(),
            done: false,
        }
    }
}

impl RuntimeFilterWorker {
    pub(crate) fn new(
        query_id: QueryId,
        params: runtime_filter::TRuntimeFilterParams,
        hub: Arc<RuntimeFilterHub>,
    ) -> Self {
        Self {
            query_id,
            params,
            hub,
            merge_states: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn receive_partial(
        &self,
        filter_id: i32,
        data: &[u8],
        build_be_number: i32,
    ) -> Result<(), String> {
        let slot_id = match self.hub.filter_spec_slot_id(filter_id) {
            Some(slot_id) => slot_id,
            None => {
                warn!(
                    "runtime filter spec missing on merge node: filter_id={}",
                    filter_id
                );
                SlotId::new(0)
            }
        };
        let rf_type = peek_starrocks_filter_type(data)?;
        debug!(
            "runtime filter receive partial: filter_id={} type={:?} build_be={} bytes={}",
            filter_id,
            rf_type,
            build_be_number,
            data.len()
        );
        let filter = match rf_type {
            StarrocksRuntimeFilterType::In => {
                RuntimeFilterPayload::In(decode_starrocks_in_filter(filter_id, slot_id, data)?)
            }
            _ => RuntimeFilterPayload::Membership(decode_starrocks_membership_filter(
                filter_id, slot_id, data,
            )?),
        };
        let expected = self.expected_builders(filter_id);
        debug!(
            "runtime filter merge state: filter_id={} expected_builders={}",
            filter_id, expected
        );
        let ready = {
            let mut guard = self.merge_states.lock().expect("runtime filter merge lock");
            let state = guard
                .entry(filter_id)
                .or_insert_with(|| MergeState::new(expected));
            if state.done {
                return Ok(());
            }
            if state.expected != expected {
                state.expected = expected;
            }
            if state.received.contains_key(&build_be_number) {
                return Ok(());
            }
            state.received.insert(build_be_number, filter);
            if state.received.len() < state.expected {
                return Ok(());
            }
            let mut parts = Vec::with_capacity(state.received.len());
            for value in state.received.values() {
                parts.push(value.clone());
            }
            state.received.clear();
            state.done = true;
            Some(parts)
        };

        if let Some(parts) = ready {
            let max_size = self.params.runtime_filter_max_size.filter(|v| *v > 0);
            let data = merge_and_encode_filters(parts, max_size)?;
            debug!(
                "runtime filter merged final: filter_id={} bytes={}",
                filter_id,
                data.len()
            );
            self.broadcast_final_filter(filter_id, data);
        }
        Ok(())
    }

    fn expected_builders(&self, filter_id: i32) -> usize {
        self.params
            .runtime_filter_builder_number
            .as_ref()
            .and_then(|m| m.get(&filter_id).copied())
            .unwrap_or(1)
            .max(1) as usize
    }

    fn broadcast_final_filter(&self, filter_id: i32, data: Vec<u8>) {
        let Some(id_to_probers) = self.params.id_to_prober_params.as_ref() else {
            return;
        };
        let Some(probers) = id_to_probers.get(&filter_id) else {
            return;
        };
        debug!(
            "runtime filter broadcast final: filter_id={} bytes={} targets={}",
            filter_id,
            data.len(),
            probers.len()
        );

        let mut seen_hosts = HashSet::new();
        for prober in probers {
            let Some(addr) = prober.fragment_instance_address.as_ref() else {
                continue;
            };
            if addr.hostname.is_empty() {
                continue;
            }
            if !seen_hosts.insert(addr.hostname.clone()) {
                continue;
            }
            let req = PTransmitRuntimeFilterParams {
                is_partial: Some(false),
                query_id: Some(crate::service::grpc_client::proto::starrocks::PUniqueId {
                    hi: self.query_id.hi,
                    lo: self.query_id.lo,
                }),
                filter_id: Some(filter_id),
                data: Some(data.clone()),
                ..Default::default()
            };
            let dest_port = addr.port as u16;
            if let Err(e) = exchange_sender::send_runtime_filter(&addr.hostname, dest_port, req) {
                warn!(
                    "send runtime filter failed: dest={} filter_id={} err={}",
                    addr.hostname, filter_id, e
                );
            }
        }
    }
}

#[derive(Clone)]
enum RuntimeFilterPayload {
    In(RuntimeInFilter),
    Membership(RuntimeMembershipFilter),
}

fn merge_and_encode_filters(
    parts: Vec<RuntimeFilterPayload>,
    max_size: Option<i64>,
) -> Result<Vec<u8>, String> {
    let mut iter = parts.into_iter();
    let first = iter
        .next()
        .ok_or_else(|| "runtime filter merge requires at least one part".to_string())?;
    match first {
        RuntimeFilterPayload::In(mut merged) => {
            for part in iter {
                match part {
                    RuntimeFilterPayload::In(filter) => merged.merge_from(&filter)?,
                    _ => return Err("runtime filter merge type mismatch".to_string()),
                }
            }
            let mut data = encode_starrocks_in_filter(&merged)?;
            if let Some(limit) = max_size {
                if data.len() as i64 > limit {
                    merged = merged.empty_like();
                    data = encode_starrocks_in_filter(&merged)?;
                }
            }
            Ok(data)
        }
        RuntimeFilterPayload::Membership(merged) => {
            let mut total_size = merged.size();
            let mut force_empty = !merged.can_use_for_merge();
            let mut min_max = merged.min_max().clone();
            let mut merged_membership = if matches!(merged, RuntimeMembershipFilter::Empty(_)) {
                None
            } else {
                Some(merged.clone())
            };
            for part in iter {
                match part {
                    RuntimeFilterPayload::Membership(filter) => {
                        total_size = total_size.saturating_add(filter.size());
                        min_max.merge_from(filter.min_max())?;
                        if !filter.can_use_for_merge() {
                            force_empty = true;
                        }
                        match (&mut merged_membership, &filter) {
                            (Some(_current), RuntimeMembershipFilter::Empty(_)) => {}
                            (Some(current), _) => {
                                current.merge_membership_from(&filter)?;
                            }
                            (None, RuntimeMembershipFilter::Empty(_)) => {}
                            (None, _) => {
                                merged_membership = Some(filter.clone());
                            }
                        }
                    }
                    _ => return Err("runtime filter merge type mismatch".to_string()),
                }
            }
            if let Some(limit) = max_size {
                if total_size as i64 > limit {
                    force_empty = true;
                }
            }
            let mut result = if force_empty {
                let mut base = merged_membership.unwrap_or(merged);
                base.set_min_max(min_max.clone());
                base.to_empty()
            } else {
                let mut base = merged_membership.unwrap_or_else(|| merged.to_empty());
                base.set_min_max(min_max.clone());
                base
            };
            let mut data = encode_membership_filter(&result)?;
            if let Some(limit) = max_size {
                if data.len() as i64 > limit {
                    result = result.to_empty();
                    result.set_min_max(min_max);
                    data = encode_membership_filter(&result)?;
                }
            }
            Ok(data)
        }
    }
}

fn encode_membership_filter(filter: &RuntimeMembershipFilter) -> Result<Vec<u8>, String> {
    match filter {
        RuntimeMembershipFilter::Bloom(bloom) => encode_starrocks_bloom_filter(bloom),
        RuntimeMembershipFilter::Empty(empty) => encode_starrocks_empty_filter(empty),
        RuntimeMembershipFilter::Bitset(_) => {
            Err("runtime bitset filter encode not supported".to_string())
        }
    }
}
