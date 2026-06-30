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

use crate::exec::spill::{SpillConfig, SpillMode};
use crate::thrift::internal_service::{TQueryOptions, TSpillMode};

pub(crate) fn spill_mode_from_thrift(mode: TSpillMode) -> Result<SpillMode, String> {
    match mode {
        TSpillMode::NONE => Ok(SpillMode::None),
        TSpillMode::FORCE => Ok(SpillMode::Force),
        TSpillMode::AUTO => Ok(SpillMode::Auto),
        TSpillMode::RANDOM => Ok(SpillMode::Random),
        TSpillMode(value) => Err(format!("unknown spill_mode value: {value}")),
    }
}

pub(crate) fn spill_config_from_query_options(
    query_opts: Option<&TQueryOptions>,
) -> Result<Option<SpillConfig>, String> {
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
        .ok_or_else(|| "spill_mode is required when enable_spill=true".to_string())
        .and_then(spill_mode_from_thrift)?;
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

    if let Some(opts) = spill_opts.and_then(|v| v.spill_to_remote_storage_options.as_ref())
        && opts.disable_spill_to_local_disk.unwrap_or(false)
    {
        return Err(
            "spill_to_remote_storage_options.disable_spill_to_local_disk=true is not supported"
                .to_string(),
        );
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
