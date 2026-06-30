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

use crate::engine::query_options::StandaloneQueryOptions;
use crate::exec::spill::{SpillConfig, SpillMode};
use crate::thrift::internal_service::{TQueryOptions, TSpillMode, TSpillOptions};
use thrift::OrderedFloat;

pub(crate) fn standalone_query_options_from_thrift(
    opts: Option<&TQueryOptions>,
) -> Result<StandaloneQueryOptions, String> {
    let Some(opts) = opts else {
        return Ok(StandaloneQueryOptions::default());
    };

    Ok(StandaloneQueryOptions {
        pipeline_dop: opts.pipeline_dop,
        query_timeout: opts.query_timeout,
        batch_size: opts.batch_size,
        enable_profile: opts.enable_profile.unwrap_or(false),
        exec_mem_limit: opts.query_mem_limit.or(opts.mem_limit),
        connector_io_tasks_per_scan_operator: opts.connector_io_tasks_per_scan_operator,
        allow_throw_exception: opts.allow_throw_exception.unwrap_or(false),
        group_concat_max_len: opts.group_concat_max_len,
        spill: crate::exec::spill::query_options_wire::spill_config_from_query_options(Some(opts))?,
    })
}

pub(crate) fn standalone_query_options_to_thrift(opts: &StandaloneQueryOptions) -> TQueryOptions {
    let mut thrift = TQueryOptions {
        pipeline_dop: opts.pipeline_dop,
        query_timeout: opts.query_timeout,
        batch_size: opts.batch_size,
        enable_profile: Some(opts.enable_profile),
        query_mem_limit: opts.exec_mem_limit,
        connector_io_tasks_per_scan_operator: opts.connector_io_tasks_per_scan_operator,
        allow_throw_exception: opts.allow_throw_exception.then_some(true),
        group_concat_max_len: opts.group_concat_max_len,
        enable_spill: Some(opts.spill.is_some()),
        ..Default::default()
    };

    if let Some(spill) = opts.spill.as_ref() {
        apply_spill_config_to_thrift(spill, &mut thrift);
    }

    thrift
}

pub(crate) fn standalone_query_options_to_optional_thrift(
    opts: Option<&StandaloneQueryOptions>,
) -> Option<TQueryOptions> {
    opts.map(standalone_query_options_to_thrift)
}

fn apply_spill_config_to_thrift(spill: &SpillConfig, thrift: &mut TQueryOptions) {
    thrift.enable_spill = Some(spill.enable_spill);
    thrift.spill_options = Some(TSpillOptions {
        spill_mode: Some(spill_mode_to_thrift(spill.spill_mode)),
        spill_mem_limit_threshold: spill.spill_mem_limit_threshold.map(OrderedFloat),
        spill_operator_min_bytes: spill.spill_operator_min_bytes,
        spill_operator_max_bytes: spill.spill_operator_max_bytes,
        spill_encode_level: spill.spill_encode_level,
        enable_spill_buffer_read: spill.enable_spill_buffer_read,
        max_spill_read_buffer_bytes_per_driver: spill.max_spill_read_buffer_bytes_per_driver,
        spill_mem_table_size: spill.spill_mem_table_size,
        spill_mem_table_num: spill.spill_mem_table_num,
        ..Default::default()
    });
}

fn spill_mode_to_thrift(mode: SpillMode) -> TSpillMode {
    match mode {
        SpillMode::None => TSpillMode::NONE,
        SpillMode::Force => TSpillMode::FORCE,
        SpillMode::Auto => TSpillMode::AUTO,
        SpillMode::Random => TSpillMode::RANDOM,
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::spill::SpillMode;
    use crate::thrift::internal_service::{TQueryOptions, TSpillMode, TSpillOptions};
    use thrift::OrderedFloat;

    use crate::engine::query_options::StandaloneQueryOptions;

    use super::{standalone_query_options_from_thrift, standalone_query_options_to_thrift};

    #[test]
    fn query_options_defaults_are_thrift_free_and_spill_disabled() {
        let opts = standalone_query_options_from_thrift(None).expect("convert defaults");

        assert_eq!(opts, StandaloneQueryOptions::default());
        assert!(opts.spill.is_none());

        let thrift = standalone_query_options_to_thrift(&opts);
        assert_eq!(thrift.pipeline_dop, None);
        assert_eq!(thrift.query_timeout, None);
        assert_eq!(thrift.batch_size, None);
        assert_eq!(thrift.enable_profile, Some(false));
        assert_eq!(thrift.enable_spill, Some(false));
    }

    #[test]
    fn query_options_round_trip_execution_fields() {
        let thrift = TQueryOptions {
            pipeline_dop: Some(8),
            query_timeout: Some(60),
            batch_size: Some(4096),
            enable_profile: Some(true),
            query_mem_limit: Some(1 << 30),
            connector_io_tasks_per_scan_operator: Some(12),
            allow_throw_exception: Some(true),
            group_concat_max_len: Some(65_535),
            ..Default::default()
        };

        let opts = standalone_query_options_from_thrift(Some(&thrift)).expect("convert options");

        assert_eq!(opts.pipeline_dop, Some(8));
        assert_eq!(opts.query_timeout, Some(60));
        assert_eq!(opts.batch_size, Some(4096));
        assert!(opts.enable_profile);
        assert_eq!(opts.exec_mem_limit, Some(1 << 30));
        assert_eq!(opts.connector_io_tasks_per_scan_operator, Some(12));
        assert!(opts.allow_throw_exception);
        assert_eq!(opts.group_concat_max_len, Some(65_535));

        let thrift = standalone_query_options_to_thrift(&opts);
        assert_eq!(thrift.pipeline_dop, Some(8));
        assert_eq!(thrift.query_timeout, Some(60));
        assert_eq!(thrift.batch_size, Some(4096));
        assert_eq!(thrift.enable_profile, Some(true));
        assert_eq!(thrift.query_mem_limit, Some(1 << 30));
        assert_eq!(thrift.connector_io_tasks_per_scan_operator, Some(12));
        assert_eq!(thrift.allow_throw_exception, Some(true));
        assert_eq!(thrift.group_concat_max_len, Some(65_535));
    }

    #[test]
    fn query_options_mem_limit_uses_legacy_mem_limit_when_query_limit_absent() {
        let thrift = TQueryOptions {
            mem_limit: Some(512),
            query_mem_limit: None,
            ..Default::default()
        };

        let opts = standalone_query_options_from_thrift(Some(&thrift)).expect("convert options");

        assert_eq!(opts.exec_mem_limit, Some(512));
        assert_eq!(
            standalone_query_options_to_thrift(&opts).query_mem_limit,
            Some(512)
        );
    }

    #[test]
    fn query_options_spill_uses_nested_options_before_legacy_fields() {
        let thrift = TQueryOptions {
            enable_spill: Some(true),
            spill_mode: Some(TSpillMode::FORCE),
            spill_mem_limit_threshold: Some(OrderedFloat(0.1)),
            spill_operator_min_bytes: Some(10),
            spill_operator_max_bytes: Some(20),
            spill_encode_level: Some(1),
            spill_mem_table_size: Some(128),
            spill_mem_table_num: Some(2),
            spill_options: Some(TSpillOptions {
                spill_mode: Some(TSpillMode::AUTO),
                spill_mem_limit_threshold: Some(OrderedFloat(0.7)),
                spill_operator_min_bytes: Some(70),
                spill_operator_max_bytes: Some(700),
                spill_encode_level: Some(3),
                enable_spill_buffer_read: Some(true),
                max_spill_read_buffer_bytes_per_driver: Some(4096),
                spill_mem_table_size: Some(256),
                spill_mem_table_num: Some(4),
                ..Default::default()
            }),
            ..Default::default()
        };

        let opts = standalone_query_options_from_thrift(Some(&thrift)).expect("convert options");
        let spill = opts.spill.as_ref().expect("spill config");

        assert!(spill.enable_spill);
        assert_eq!(spill.spill_mode, SpillMode::Auto);
        assert_eq!(spill.spill_mem_limit_threshold, Some(0.7));
        assert_eq!(spill.spill_operator_min_bytes, Some(70));
        assert_eq!(spill.spill_operator_max_bytes, Some(700));
        assert_eq!(spill.spill_encode_level, Some(3));
        assert_eq!(spill.enable_spill_buffer_read, Some(true));
        assert_eq!(spill.max_spill_read_buffer_bytes_per_driver, Some(4096));
        assert_eq!(spill.spill_mem_table_size, Some(256));
        assert_eq!(spill.spill_mem_table_num, Some(4));

        let thrift = standalone_query_options_to_thrift(&opts);
        assert_eq!(thrift.enable_spill, Some(true));
        assert_eq!(
            thrift.spill_options.as_ref().and_then(|v| v.spill_mode),
            Some(TSpillMode::AUTO)
        );
        assert_eq!(
            thrift
                .spill_options
                .as_ref()
                .and_then(|v| v.spill_mem_limit_threshold)
                .map(|v: OrderedFloat<f64>| v.into_inner()),
            Some(0.7)
        );
    }
}
