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
//! Exchange source for receiving distributed upstream data.
//!
//! Responsibilities:
//! - Fetches remote stream pages from exchange service and reconstructs chunks for local pipeline processing.
//! - Handles end-of-stream coordination, sender completion tracking, and error propagation.
//!
//! Key exported interfaces:
//! - Types: `ExchangeSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, filter_chunk_by_in_filters_with_exprs,
    filter_chunk_by_membership_filters_with_exprs,
};
use crate::metrics;
use crate::runtime::exchange;
use crate::runtime::runtime_filter_hub::{RuntimeFilterHub, RuntimeFilterProbe};
use crate::runtime::runtime_state::RuntimeState;
use crate::novarocks_logging::debug;

static EXCHANGE_SOURCE_READY_LOG_COUNT: AtomicU64 = AtomicU64::new(0);

fn should_log_exchange_source_ready() -> bool {
    let count = EXCHANGE_SOURCE_READY_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    count % 1024 == 0
}

const JOIN_RUNTIME_FILTER_TIME: &str = "JoinRuntimeFilterTime";
const JOIN_RUNTIME_FILTER_HASH_TIME: &str = "JoinRuntimeFilterHashTime";
const JOIN_RUNTIME_FILTER_INPUT_ROWS: &str = "JoinRuntimeFilterInputRows";
const JOIN_RUNTIME_FILTER_OUTPUT_ROWS: &str = "JoinRuntimeFilterOutputRows";
const JOIN_RUNTIME_FILTER_EVALUATE: &str = "JoinRuntimeFilterEvaluate";
const RUNTIME_FILTER_NUM: &str = "RuntimeFilterNum";
const RUNTIME_IN_FILTER_NUM: &str = "RuntimeInFilterNum";
const RUNTIME_FILTER_DEBUG_EVERY: u64 = 256;

/// Factory for exchange source operators that fetch and decode remote stream pages.
pub struct ExchangeSourceFactory {
    name: String,
    node: ExchangeSourceNode,
    #[allow(dead_code)]
    runtime_filter_specs: Vec<crate::exec::node::RuntimeFilterProbeSpec>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    local_rf_waiting_set: Vec<i32>,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    arena: Arc<ExprArena>,
}

impl ExchangeSourceFactory {
    pub(crate) fn new(
        node: ExchangeSourceNode,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
        arena: Arc<ExprArena>,
    ) -> Self {
        let name = node.profile_name();
        let runtime_filter_specs = node.runtime_filter_specs().to_vec();
        let runtime_filter_exprs = runtime_filter_specs
            .iter()
            .map(|spec| (spec.filter_id, spec.expr_id))
            .collect();
        let runtime_filters_expected = runtime_filter_specs.len();
        if runtime_filters_expected > 0 {
            runtime_filter_hub.register_probe_specs(node.key.node_id, &runtime_filter_specs);
        }
        let local_rf_waiting_set = node.local_rf_waiting_set().to_vec();
        Self {
            name,
            node,
            runtime_filter_specs,
            runtime_filter_exprs,
            runtime_filters_expected,
            local_rf_waiting_set,
            runtime_filter_hub,
            arena,
        }
    }

    fn local_rf_deps(&self) -> Vec<crate::exec::pipeline::dependency::DependencyHandle> {
        if self.local_rf_waiting_set.is_empty() {
            return Vec::new();
        }
        let mut deps = Vec::new();
        let mut seen = HashSet::new();
        for node_id in &self.local_rf_waiting_set {
            if seen.insert(*node_id) {
                if let Some(dep) = self.runtime_filter_hub.local_dependency_if_exists(*node_id) {
                    deps.push(dep);
                } else {
                    debug!(
                        "exchange skip unknown local RF dependency: node_id={} waiting_build_node_id={}",
                        self.node.key.node_id, node_id
                    );
                }
            }
        }
        deps
    }
}

impl OperatorFactory for ExchangeSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        if !self.local_rf_waiting_set.is_empty() {
            debug!(
                "ExchangeSource local RF wait: finst={}:{} node_id={} driver_id={} waiting_set={:?}",
                self.node.key.finst_id_hi,
                self.node.key.finst_id_lo,
                self.node.key.node_id,
                driver_id,
                self.local_rf_waiting_set
            );
        }
        Box::new(ExchangeSourceOperator {
            name: self.name.clone(),
            node: self.node.clone(),
            driver_id,
            receiver: None,
            start: None,
            finished: false,
            logged_first_pull: false,
            logged_first_none: false,
            runtime_filter_probe: if self.runtime_filters_expected > 0 {
                Some(ExchangeRuntimeFilterProbe {
                    probe: self
                        .runtime_filter_hub
                        .register_probe(self.node.key.node_id),
                })
            } else {
                None
            },
            local_rf_deps: self.local_rf_deps(),
            runtime_filter_exprs: self.runtime_filter_exprs.clone(),
            runtime_filters_expected: self.runtime_filters_expected,
            runtime_filters_loaded: false,
            rf_debug_counter: 0,
            rf_debug_last_version: 0,
            arena: Arc::clone(&self.arena),
            profiles: None,
            receiver_mem_tracker_ready: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ExchangeSourceOperator {
    name: String,
    node: ExchangeSourceNode,
    driver_id: i32,
    receiver: Option<exchange::ExchangeReceiverHandle>,
    start: Option<Instant>,
    finished: bool,
    logged_first_pull: bool,
    logged_first_none: bool,
    runtime_filter_probe: Option<ExchangeRuntimeFilterProbe>,
    local_rf_deps: Vec<crate::exec::pipeline::dependency::DependencyHandle>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    runtime_filters_loaded: bool,
    rf_debug_counter: u64,
    rf_debug_last_version: u64,
    arena: Arc<ExprArena>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    receiver_mem_tracker_ready: bool,
}

struct ExchangeRuntimeFilterProbe {
    probe: RuntimeFilterProbe,
}

impl ExchangeRuntimeFilterProbe {
    fn dependency_or_timeout(&self) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        self.probe.dependency_or_timeout(false)
    }

    fn dependency(&self) -> crate::exec::pipeline::dependency::DependencyHandle {
        self.probe.dependency()
    }

    fn snapshot(&self) -> crate::runtime::runtime_filter_hub::RuntimeFilterSnapshot {
        self.probe.snapshot()
    }

    fn mark_ready(&self) -> Option<Duration> {
        self.probe.mark_ready()
    }
}

impl ExchangeSourceOperator {
    fn local_rf_dependency(&self) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        for dep in &self.local_rf_deps {
            if !dep.is_ready() {
                return Some(dep.clone());
            }
        }
        None
    }
}

impl Operator for ExchangeSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        if self.receiver.is_some() {
            return Ok(());
        }
        let receiver = exchange::get_receiver_handle(self.node.key, self.node.expected_senders)?;
        self.receiver = Some(receiver);
        self.start = Some(Instant::now());
        debug!(
            "ExchangeSource prepared: finst={}:{} node_id={} expected_senders={} timeout={:?}",
            self.node.key.finst_id_hi,
            self.node.key.finst_id_lo,
            self.node.key.node_id,
            self.node.expected_senders,
            self.node.timeout
        );
        Ok(())
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for ExchangeSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if let Some(start) = self.start {
            if start.elapsed() >= self.node.timeout {
                if should_log_exchange_source_ready() {
                    debug!(
                        "ExchangeSource has_output due to timeout: finst={}:{} node_id={} elapsed={:?} timeout={:?}",
                        self.node.key.finst_id_hi,
                        self.node.key.finst_id_lo,
                        self.node.key.node_id,
                        start.elapsed(),
                        self.node.timeout
                    );
                }
                return true;
            }
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return false;
        };
        let ready = receiver.has_output_or_finished(self.node.expected_senders);
        if ready && should_log_exchange_source_ready() {
            debug!(
                "ExchangeSource has_output due to receiver: finst={}:{} node_id={} expected_senders={}",
                self.node.key.finst_id_hi,
                self.node.key.finst_id_lo,
                self.node.key.node_id,
                self.node.expected_senders
            );
        }
        ready
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("exchange source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        if self.receiver.is_none() {
            return Err("exchange source operator not prepared".to_string());
        }

        if !self.receiver_mem_tracker_ready {
            self.receiver_mem_tracker_ready = true;
            if let Some(root) = state.mem_tracker() {
                let _ = exchange::ensure_receiver_mem_tracker(self.node.key, &root)?;
            }
        }

        if !self.logged_first_pull {
            self.logged_first_pull = true;
            debug!(
                "ExchangeSource first pull: node_id={} driver_id={}",
                self.node.key.node_id, self.driver_id
            );
        }

        self.load_runtime_filters_if_ready();

        let start = self.start.get_or_insert_with(Instant::now);
        if start.elapsed() >= self.node.timeout {
            debug!(
                "ExchangeSource timeout waiting for senders: finst_id={}:{} node_id={} elapsed={:?} timeout={:?}",
                self.node.key.finst_id_hi,
                self.node.key.finst_id_lo,
                self.node.key.node_id,
                start.elapsed(),
                self.node.timeout
            );
            return Err(format!(
                "exchange timeout waiting for senders: finst_id={}:{} node_id={}",
                self.node.key.finst_id_hi, self.node.key.finst_id_lo, self.node.key.node_id
            ));
        }

        loop {
            let out = {
                let receiver = self.receiver.as_ref().expect("receiver");
                receiver
                    .try_pop_next_with_stats(self.node.expected_senders)
                    .map_err(|e| e.to_string())?
            };

            match out {
                Some(exchange::ExchangePopResult::Chunk(chunk)) => {
                    let input_rows = chunk.len();
                    if let Some(filtered) = self.apply_runtime_filters(chunk)? {
                        if filtered.is_empty() {
                            debug!(
                                "ExchangeSource filtered empty chunk: node_id={} driver_id={} input_rows={}",
                                self.node.key.node_id, self.driver_id, input_rows
                            );
                            continue;
                        }
                        debug!(
                            "ExchangeSource output chunk: node_id={} driver_id={} input_rows={} output_rows={}",
                            self.node.key.node_id,
                            self.driver_id,
                            input_rows,
                            filtered.len()
                        );
                        return Ok(Some(filtered));
                    } else {
                        debug!(
                            "ExchangeSource filtered to None: node_id={} driver_id={} input_rows={}",
                            self.node.key.node_id, self.driver_id, input_rows
                        );
                    }
                    continue;
                }
                Some(exchange::ExchangePopResult::Finished(stats)) => {
                    debug!(
                        "ExchangeSource finished: finst={}:{} node_id={} driver_id={} request_received={} bytes_received={} deserialize_ns={} chunks_received={} rows_received={}",
                        self.node.key.finst_id_hi,
                        self.node.key.finst_id_lo,
                        self.node.key.node_id,
                        self.driver_id,
                        stats.request_received,
                        stats.bytes_received,
                        stats.deserialize_ns,
                        stats.chunks_received,
                        stats.rows_received
                    );
                    self.finished = true;
                    return Ok(None);
                }
                None => {
                    if !self.logged_first_none {
                        self.logged_first_none = true;
                        debug!(
                            "ExchangeSource no output yet: node_id={} driver_id={}",
                            self.node.key.node_id, self.driver_id
                        );
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        if let Some(dep) = self.local_rf_dependency() {
            return Some(dep);
        }
        if let Some(rf) = self.runtime_filter_probe.as_ref() {
            if let Some(dep) = rf.dependency_or_timeout() {
                if !dep.is_ready() {
                    if self.finished {
                        return None;
                    }
                    if let Some(receiver) = self.receiver.as_ref() {
                        // If data is already available, let the driver consume it instead of
                        // waiting on runtime filters.
                        if receiver.has_output_or_finished(self.node.expected_senders) {
                            return None;
                        }
                    }
                    return Some(dep);
                }
            }
        }
        None
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        self.receiver.as_ref().map(|r| r.observable())
    }
}

impl ExchangeSourceOperator {
    fn load_runtime_filters_if_ready(&mut self) {
        let Some(rf) = self.runtime_filter_probe.as_ref() else {
            self.runtime_filters_loaded = true;
            return;
        };
        if self.runtime_filters_loaded {
            return;
        }
        if self.runtime_filters_expected == 0 {
            self.runtime_filters_loaded = true;
            return;
        }
        let dep = rf.dependency();
        if !dep.is_ready() {
            return;
        }
        if let Some(profile) = self.profiles.as_ref() {
            profile.common.add_timer(JOIN_RUNTIME_FILTER_TIME);
            profile.common.add_timer(JOIN_RUNTIME_FILTER_HASH_TIME);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_INPUT_ROWS, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_OUTPUT_ROWS, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_EVALUATE, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(RUNTIME_FILTER_NUM, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(RUNTIME_IN_FILTER_NUM, metrics::TUnit::UNIT);
        }
        let snapshot = rf.snapshot();
        if let Some(elapsed) = rf.mark_ready() {
            if let Some(profile) = self.profiles.as_ref() {
                let latency_ns = elapsed.as_nanos().min(i64::MAX as u128) as i64;
                for filter in snapshot.in_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, metrics::TUnit::TIME_NS, latency_ns);
                }
                for filter in snapshot.membership_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, metrics::TUnit::TIME_NS, latency_ns);
                }
            }
        }
        self.log_runtime_filters_loaded(snapshot.in_filters(), snapshot.membership_filters());
        if let Some(profile) = self.profiles.as_ref() {
            let filter_num = snapshot.membership_filters().len();
            let in_filter_num = snapshot.in_filters().len();
            profile
                .common
                .counter_set(RUNTIME_FILTER_NUM, metrics::TUnit::UNIT, filter_num as i64);
            profile.common.counter_set(
                RUNTIME_IN_FILTER_NUM,
                metrics::TUnit::UNIT,
                in_filter_num as i64,
            );
        }
        self.runtime_filters_loaded = true;
    }

    fn log_runtime_filters_loaded(
        &self,
        in_filters: &[Arc<RuntimeInFilter>],
        membership_filters: &[Arc<RuntimeMembershipFilter>],
    ) {
        debug!(
            "exchange runtime filters loaded: node_id={} expected={} in_filters={} membership_filters={}",
            self.node.key.node_id,
            self.runtime_filters_expected,
            in_filters.len(),
            membership_filters.len()
        );
        for filter in in_filters {
            let filter = filter.as_ref();
            debug!(
                "exchange runtime in filter: node_id={} filter_id={} slot_id={:?} empty={}",
                self.node.key.node_id,
                filter.filter_id(),
                filter.slot_id(),
                filter.is_empty()
            );
        }
        for filter in membership_filters {
            let filter = filter.as_ref();
            let kind = match filter {
                RuntimeMembershipFilter::Bloom(_) => "bloom",
                RuntimeMembershipFilter::Bitset(_) => "bitset",
                RuntimeMembershipFilter::Empty(_) => "empty",
            };
            debug!(
                "exchange runtime membership filter: node_id={} filter_id={} kind={} slot_id={:?} ltype={:?} size={} has_null={} join_mode={} empty={}",
                self.node.key.node_id,
                filter.filter_id(),
                kind,
                filter.slot_id(),
                filter.ltype(),
                filter.size(),
                filter.has_null(),
                filter.join_mode(),
                filter.is_empty()
            );
        }
    }

    fn apply_runtime_filters(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let expected_filters = self.runtime_filters_expected;
        let Some(rf) = self.runtime_filter_probe.as_ref() else {
            return Ok(Some(chunk));
        };
        if expected_filters == 0 {
            return Ok(Some(chunk));
        }
        let snapshot = rf.snapshot();
        self.rf_debug_counter = self.rf_debug_counter.wrapping_add(1);
        let version = rf.probe.handle().version();
        if version != self.rf_debug_last_version
            || self.rf_debug_counter % RUNTIME_FILTER_DEBUG_EVERY == 0
        {
            debug!(
                "exchange runtime filter progress: node_id={} driver_id={} version={} in_filters={} membership_filters={} expected={} counter={}",
                self.node.key.node_id,
                self.driver_id,
                version,
                snapshot.in_filters().len(),
                snapshot.membership_filters().len(),
                expected_filters,
                self.rf_debug_counter
            );
            self.rf_debug_last_version = version;
        }
        let input_rows = chunk.len();
        if snapshot.is_empty() {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_INPUT_ROWS,
                    metrics::TUnit::UNIT,
                    input_rows as i64,
                );
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                    metrics::TUnit::UNIT,
                    input_rows as i64,
                );
            }
            return Ok(Some(chunk));
        }
        let filters_len =
            (snapshot.in_filters().len() + snapshot.membership_filters().len()) as i64;
        let result = if let Some(profile) = self.profiles.as_ref() {
            let _timer = profile.common.scoped_timer(JOIN_RUNTIME_FILTER_TIME);
            let chunk = filter_chunk_by_membership_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                snapshot.membership_filters(),
                chunk,
            )?;
            match chunk {
                Some(chunk) => filter_chunk_by_in_filters_with_exprs(
                    &self.arena,
                    &self.runtime_filter_exprs,
                    snapshot.in_filters(),
                    chunk,
                ),
                None => Ok(None),
            }
        } else {
            let chunk = filter_chunk_by_membership_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                snapshot.membership_filters(),
                chunk,
            )?;
            match chunk {
                Some(chunk) => filter_chunk_by_in_filters_with_exprs(
                    &self.arena,
                    &self.runtime_filter_exprs,
                    snapshot.in_filters(),
                    chunk,
                ),
                None => Ok(None),
            }
        }?;
        if let Some(profile) = self.profiles.as_ref() {
            let output_rows = result.as_ref().map(|c| c.len()).unwrap_or(0) as i64;
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_INPUT_ROWS,
                metrics::TUnit::UNIT,
                input_rows as i64,
            );
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                metrics::TUnit::UNIT,
                output_rows,
            );
            if filters_len > 0 {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_EVALUATE,
                    metrics::TUnit::UNIT,
                    filters_len,
                );
            }
        }
        Ok(result)
    }
}
