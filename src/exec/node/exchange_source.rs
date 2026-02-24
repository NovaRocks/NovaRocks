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
use std::time::Duration;

use crate::common::ids::SlotId;
use crate::exec::node::RuntimeFilterProbeSpec;
use crate::runtime::exchange::ExchangeKey;

#[derive(Clone, Debug)]
pub struct ExchangeSourceNode {
    pub key: ExchangeKey,
    pub expected_senders: usize,
    pub timeout: Duration,
    pub runtime_filter_specs: Vec<RuntimeFilterProbeSpec>,
    pub output_slots: Vec<SlotId>,
    pub local_rf_waiting_set: Vec<i32>,
}

impl ExchangeSourceNode {
    pub fn new(
        key: ExchangeKey,
        expected_senders: usize,
        timeout: Duration,
        output_slots: Vec<SlotId>,
    ) -> Self {
        Self {
            key,
            expected_senders,
            timeout,
            runtime_filter_specs: Vec::new(),
            output_slots,
            local_rf_waiting_set: Vec::new(),
        }
    }

    pub fn profile_name(&self) -> String {
        format!("EXCHANGE_SOURCE (id={})", self.key.node_id)
    }

    pub fn runtime_filter_specs(&self) -> &[RuntimeFilterProbeSpec] {
        &self.runtime_filter_specs
    }

    pub fn output_slots(&self) -> &[SlotId] {
        &self.output_slots
    }

    pub fn local_rf_waiting_set(&self) -> &[i32] {
        &self.local_rf_waiting_set
    }

    pub fn with_local_rf_waiting_set(mut self, waiting_set: Vec<i32>) -> Self {
        if waiting_set.is_empty() {
            return self;
        }
        let mut seen = std::collections::HashMap::new();
        for id in waiting_set {
            seen.entry(id).or_insert(());
        }
        self.local_rf_waiting_set = seen.keys().copied().collect();
        self.local_rf_waiting_set.sort_unstable();
        self
    }

    pub fn add_runtime_filter_specs(&mut self, specs: &[RuntimeFilterProbeSpec]) {
        if specs.is_empty() {
            return;
        }
        let mut seen: std::collections::HashMap<i32, RuntimeFilterProbeSpec> = self
            .runtime_filter_specs
            .iter()
            .map(|spec| (spec.filter_id, spec.clone()))
            .collect();
        for spec in specs {
            if seen.contains_key(&spec.filter_id) {
                continue;
            }
            self.runtime_filter_specs.push(spec.clone());
            seen.insert(spec.filter_id, spec.clone());
        }
    }
}
