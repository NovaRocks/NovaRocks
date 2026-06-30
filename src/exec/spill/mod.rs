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
pub mod block_manager;
pub mod dir_manager;
pub mod ipc_serde;
pub(crate) mod query_options_wire;
pub mod spill_channel;
pub mod spill_stream;
pub mod spiller;

use crate::runtime::profile::{CounterRef, Profiler, RuntimeProfile};

pub use spill_channel::{SpillChannelHandle, SpillIoExecutor, SpillTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillMode {
    None,
    Force,
    Auto,
    Random,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpillConfig {
    pub enable_spill: bool,
    pub spill_mode: SpillMode,
    pub spill_mem_limit_threshold: Option<f64>,
    pub spill_operator_min_bytes: Option<i64>,
    pub spill_operator_max_bytes: Option<i64>,
    pub spill_encode_level: Option<i32>,
    pub enable_spill_buffer_read: Option<bool>,
    pub max_spill_read_buffer_bytes_per_driver: Option<i64>,
    pub spill_mem_table_size: Option<i32>,
    pub spill_mem_table_num: Option<i32>,
}

#[derive(Clone)]
pub struct SpillContext {
    pub config: SpillConfig,
    pub spiller: spiller::SpillerHandle,
    pub channel: SpillChannelHandle,
}

pub trait SpillableOperator {
    fn spillable(&self) -> bool;
    fn estimated_revocable_bytes(&self) -> i64;
    fn set_spill_context(&mut self, ctx: SpillContext);
    fn trigger_spill(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> Result<(), String>;
    fn restore_next(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> Result<Option<crate::exec::chunk::Chunk>, String>;
    fn spill_finished(&self) -> bool;
}

#[derive(Clone, Debug)]
pub struct QuerySpillManager {
    config: SpillConfig,
    channel: SpillChannelHandle,
    profile: Option<SpillProfile>,
}

impl QuerySpillManager {
    pub fn new(config: SpillConfig, profiler: Option<&Profiler>) -> Self {
        let profile = profiler.map(SpillProfile::new);
        Self {
            config,
            channel: SpillChannelHandle::new(),
            profile,
        }
    }

    pub fn config(&self) -> &SpillConfig {
        &self.config
    }

    pub fn channel(&self) -> SpillChannelHandle {
        self.channel.clone()
    }

    pub fn profile(&self) -> Option<SpillProfile> {
        self.profile.clone()
    }
}

#[derive(Clone, Debug)]
pub struct SpillProfile {
    pub spill_rows: CounterRef,
    pub spill_bytes: CounterRef,
    pub spill_time: CounterRef,
    pub restore_rows: CounterRef,
    pub restore_bytes: CounterRef,
    pub restore_time: CounterRef,
    pub spill_block_count: CounterRef,
    pub spill_read_io_count: CounterRef,
}

impl SpillProfile {
    pub fn new(profile: &RuntimeProfile) -> Self {
        let profile = profile.child("Spill");
        Self {
            spill_rows: profile.add_unit_counter("SpillRows"),
            spill_bytes: profile.add_bytes_counter("SpillBytes"),
            spill_time: profile.add_timer("SpillTime"),
            restore_rows: profile.add_unit_counter("RestoreRows"),
            restore_bytes: profile.add_bytes_counter("RestoreBytes"),
            restore_time: profile.add_timer("RestoreTime"),
            spill_block_count: profile.add_unit_counter("SpillBlockCount"),
            spill_read_io_count: profile.add_unit_counter("SpillReadIoCount"),
        }
    }
}
