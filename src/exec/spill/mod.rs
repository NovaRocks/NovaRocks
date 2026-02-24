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
pub mod spill_channel;
pub mod spill_stream;
pub mod spiller;

use crate::internal_service;
use crate::metrics;
use crate::runtime::profile::{CounterRef, Profiler, RuntimeProfile};

pub use spill_channel::{SpillChannelHandle, SpillIoExecutor, SpillTask};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpillMode {
    None,
    Force,
    Auto,
    Random,
}

impl TryFrom<internal_service::TSpillMode> for SpillMode {
    type Error = String;

    fn try_from(value: internal_service::TSpillMode) -> Result<Self, Self::Error> {
        match value {
            internal_service::TSpillMode::NONE => Ok(SpillMode::None),
            internal_service::TSpillMode::FORCE => Ok(SpillMode::Force),
            internal_service::TSpillMode::AUTO => Ok(SpillMode::Auto),
            internal_service::TSpillMode::RANDOM => Ok(SpillMode::Random),
            internal_service::TSpillMode(value) => {
                Err(format!("unknown spill_mode value: {value}"))
            }
        }
    }
}

#[derive(Clone, Debug)]
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

impl SpillConfig {
    pub fn from_query_options(
        query_opts: Option<&internal_service::TQueryOptions>,
    ) -> Result<Option<Self>, String> {
        let Some(opts) = query_opts else {
            return Ok(None);
        };
        let enable_spill = opts.enable_spill.unwrap_or(false);
        if !enable_spill {
            return Ok(None);
        }

        let spill_opts = opts.spill_options.as_ref();

        let spill_mode = spill_opts
            .and_then(|v| v.spill_mode)
            .or(opts.spill_mode)
            .ok_or_else(|| "spill_mode is required when enable_spill=true".to_string())?
            .try_into()?;
        if spill_mode == SpillMode::Random {
            return Err("spill_mode RANDOM is not supported yet".to_string());
        }

        let spill_enable_direct_io = spill_opts
            .and_then(|v| v.spill_enable_direct_io)
            .or(opts.spill_enable_direct_io)
            .unwrap_or(false);
        if spill_enable_direct_io {
            return Err("spill_enable_direct_io=true is not supported".to_string());
        }

        let enable_spill_to_remote_storage = spill_opts
            .and_then(|v| v.enable_spill_to_remote_storage)
            .unwrap_or(false);
        if enable_spill_to_remote_storage {
            return Err("spill to remote storage is not supported".to_string());
        }

        if let Some(opts) = spill_opts.and_then(|v| v.spill_to_remote_storage_options.as_ref()) {
            if opts.disable_spill_to_local_disk.unwrap_or(false) {
                return Err(
                    "spill_to_remote_storage_options.disable_spill_to_local_disk=true is not supported"
                        .to_string(),
                );
            }
        }

        let spill_mem_table_size = spill_opts
            .and_then(|v| v.spill_mem_table_size)
            .or(opts.spill_mem_table_size);
        let spill_mem_table_num = spill_opts
            .and_then(|v| v.spill_mem_table_num)
            .or(opts.spill_mem_table_num);
        let spill_mem_limit_threshold = spill_opts
            .and_then(|v| v.spill_mem_limit_threshold.map(|v| v.into_inner()))
            .or_else(|| opts.spill_mem_limit_threshold.map(|v| v.into_inner()));
        let spill_operator_min_bytes = spill_opts
            .and_then(|v| v.spill_operator_min_bytes)
            .or(opts.spill_operator_min_bytes);
        let spill_operator_max_bytes = spill_opts
            .and_then(|v| v.spill_operator_max_bytes)
            .or(opts.spill_operator_max_bytes);
        let spill_encode_level = spill_opts
            .and_then(|v| v.spill_encode_level)
            .or(opts.spill_encode_level);
        let enable_spill_buffer_read = spill_opts.and_then(|v| v.enable_spill_buffer_read);
        let max_spill_read_buffer_bytes_per_driver =
            spill_opts.and_then(|v| v.max_spill_read_buffer_bytes_per_driver);

        Ok(Some(SpillConfig {
            enable_spill,
            spill_mode,
            spill_mem_limit_threshold,
            spill_operator_min_bytes,
            spill_operator_max_bytes,
            spill_encode_level,
            enable_spill_buffer_read,
            max_spill_read_buffer_bytes_per_driver,
            spill_mem_table_size,
            spill_mem_table_num,
        }))
    }
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
            spill_rows: profile.add_counter("SpillRows", metrics::TUnit::UNIT),
            spill_bytes: profile.add_counter("SpillBytes", metrics::TUnit::BYTES),
            spill_time: profile.add_timer("SpillTime"),
            restore_rows: profile.add_counter("RestoreRows", metrics::TUnit::UNIT),
            restore_bytes: profile.add_counter("RestoreBytes", metrics::TUnit::BYTES),
            restore_time: profile.add_timer("RestoreTime"),
            spill_block_count: profile.add_counter("SpillBlockCount", metrics::TUnit::UNIT),
            spill_read_io_count: profile.add_counter("SpillReadIoCount", metrics::TUnit::UNIT),
        }
    }
}
