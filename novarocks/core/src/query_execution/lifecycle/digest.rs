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

use sha2::{Digest, Sha256};

use crate::exec::spill::SpillMode;
use crate::runtime::query_options::QueryOptions;

use super::manifest::{
    ExchangeRouteManifest, ParticipantManifest, ParticipantManifestDigest, ParticipantRole,
    QueryControlEndpoint,
};

const PARTICIPANT_MANIFEST_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.participant-manifest.v1\0";

pub fn digest_v1(manifest: &ParticipantManifest) -> ParticipantManifestDigest {
    let mut projection = DigestProjection::new(PARTICIPANT_MANIFEST_V1_DOMAIN);
    let execution_id = manifest.execution_id();
    projection.i64(execution_id.query_id().high());
    projection.i64(execution_id.query_id().low());
    projection.u64(execution_id.attempt_id().get());

    let backend = manifest.backend();
    projection.u64(backend.backend_id());
    endpoint(&mut projection, backend.endpoint());
    projection.u64(backend.start_epoch());

    projection.u64(manifest.roles().len() as u64);
    for role in manifest.roles() {
        projection.u8(match role {
            ParticipantRole::FragmentExecutor => 1,
            ParticipantRole::RuntimeFilterService => 2,
        });
    }

    projection.u64(manifest.expected_fragment_instance_ids().len() as u64);
    for fragment_instance_id in manifest.expected_fragment_instance_ids() {
        projection.i64(fragment_instance_id.high());
        projection.i64(fragment_instance_id.low());
    }

    query_options(&mut projection, manifest.query_options().native());
    projection.u64(manifest.query_deadline_unix_ms());

    projection.u64(manifest.exchange_routes().len() as u64);
    for route in manifest.exchange_routes() {
        exchange_route(&mut projection, route);
    }

    match manifest.runtime_filter() {
        Some(contribution) => {
            projection.u8(1);
            projection.bytes(contribution.digest());
        }
        None => projection.u8(0),
    }

    projection.u64(
        u64::try_from(manifest.pre_start_timeout().as_millis())
            .expect("validated pre-start timeout fits in u64 milliseconds"),
    );
    endpoint(&mut projection, manifest.report_endpoint());
    ParticipantManifestDigest::new(projection.finish())
}

fn endpoint(projection: &mut DigestProjection, endpoint: &QueryControlEndpoint) {
    projection.string(endpoint.host());
    projection.u16(endpoint.port());
}

fn exchange_route(projection: &mut DigestProjection, route: &ExchangeRouteManifest) {
    let source = route.source_fragment_instance_id();
    projection.i64(source.high());
    projection.i64(source.low());
    let destination = route.destination_fragment_instance_id();
    projection.i64(destination.high());
    projection.i64(destination.low());
    projection.i32(route.destination_node_id());
    projection.u32(route.sender_ordinal());
    projection.u32(route.sender_count());
}

fn query_options(projection: &mut DigestProjection, options: &QueryOptions) {
    projection.option_i32(options.batch_size);
    projection.option_i32(options.query_timeout);
    projection.option_i32(options.query_delivery_timeout);
    projection.bool(options.enable_profile);
    projection.option_i64(options.runtime_profile_report_interval);
    projection.option_i32(options.pipeline_dop);
    projection.option_i64(options.exec_mem_limit);
    projection.option_i32(options.connector_io_tasks_per_scan_operator);
    projection.bool(options.orc_use_column_names);
    projection.bool(options.enable_file_metacache);
    projection.bool(options.enable_file_pagecache);
    projection.bool(options.enable_parquet_reader_page_index);
    projection.option_i64(options.runtime_filter_scan_wait_time_ms);
    projection.option_i32(options.runtime_filter_wait_timeout_ms);
    projection.bool(options.allow_throw_exception);
    projection.option_i64(options.group_concat_max_len);
    projection.option_bool(options.enable_join_runtime_bitset_filter);
    projection.option_i64(options.global_runtime_filter_build_max_size);

    projection.bool(options.cache.enable_scan_datacache);
    projection.bool(options.cache.enable_populate_datacache);
    projection.bool(options.cache.enable_datacache_async_populate_mode);
    projection.bool(options.cache.enable_datacache_io_adaptor);
    projection.bool(options.cache.enable_cache_select);
    projection.option_i32(options.cache.datacache_evict_probability);
    projection.option_i32(options.cache.datacache_priority);
    projection.option_i64(options.cache.datacache_ttl_seconds);
    projection.option_i64(options.cache.datacache_sharing_work_period);

    match options.spill.as_ref() {
        Some(spill) => {
            projection.u8(1);
            projection.bool(spill.enable_spill);
            projection.u8(match spill.spill_mode {
                SpillMode::Auto => 0,
                SpillMode::Force => 1,
                SpillMode::None => 2,
                SpillMode::Random => 3,
            });
            projection.option_u64(spill.spill_mem_limit_threshold.map(f64::to_bits));
            projection.option_i64(spill.spill_operator_min_bytes);
            projection.option_i64(spill.spill_operator_max_bytes);
            projection.option_i32(spill.spill_encode_level);
            projection.option_bool(spill.enable_spill_buffer_read);
            projection.option_i64(spill.max_spill_read_buffer_bytes_per_driver);
            projection.option_i32(spill.spill_mem_table_size);
            projection.option_i32(spill.spill_mem_table_num);
        }
        None => projection.u8(0),
    }
}

struct DigestProjection(Sha256);

impl DigestProjection {
    fn new(domain: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(domain);
        Self(digest)
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.update(value);
    }

    fn string(&mut self, value: &str) {
        self.u64(value.len() as u64);
        self.bytes(value.as_bytes());
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u16(&mut self, value: u16) {
        self.0.update(value.to_be_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    fn option_bool(&mut self, value: Option<bool>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.bool(value);
            }
            None => self.u8(0),
        }
    }

    fn option_i32(&mut self, value: Option<i32>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.i32(value);
            }
            None => self.u8(0),
        }
    }

    fn option_i64(&mut self, value: Option<i64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.i64(value);
            }
            None => self.u8(0),
        }
    }

    fn option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.u8(1);
                self.u64(value);
            }
            None => self.u8(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::digest_v1;
    use crate::common::types::UniqueId;
    use crate::exec::spill::{SpillConfig, SpillMode};
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::identity::{AttemptId, QueryExecutionId};
    use crate::query_execution::lifecycle::manifest::{
        ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions, ParticipantRole,
        QueryControlEndpoint,
    };
    use crate::runtime::query_options::QueryOptions;

    fn manifest_with_orders(
        attempt: u64,
        roles: impl IntoIterator<Item = ParticipantRole>,
        fragment_lows: impl IntoIterator<Item = i64>,
        spill_threshold: f64,
    ) -> ParticipantManifest {
        let execution_id = QueryExecutionId::new(
            QueryId::new(5, 6),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        let backend_endpoint =
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint");
        let backend = ParticipantBackendIdentity::new(3, backend_endpoint, 11)
            .expect("valid backend identity");
        let query_options = QueryOptions {
            spill: Some(SpillConfig {
                enable_spill: true,
                spill_mode: SpillMode::Auto,
                spill_mem_limit_threshold: Some(spill_threshold),
                spill_operator_min_bytes: Some(1024),
                spill_operator_max_bytes: Some(4096),
                spill_encode_level: Some(3),
                enable_spill_buffer_read: Some(true),
                max_spill_read_buffer_bytes_per_driver: Some(8192),
                spill_mem_table_size: Some(512),
                spill_mem_table_num: Some(2),
            }),
            ..Default::default()
        };
        ParticipantManifest::new(
            execution_id,
            backend,
            roles,
            fragment_lows.into_iter().map(|lo| UniqueId::new(7, lo)),
            ParticipantQueryOptions::new(query_options),
            10_000,
            [],
            None,
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
        )
        .expect("valid participant manifest")
    }

    #[test]
    fn participant_manifest_digest_is_order_independent_but_attempt_sensitive() {
        let left = manifest_with_orders(1, [ParticipantRole::FragmentExecutor], [9, 7], 0.75);
        let right = manifest_with_orders(1, [ParticipantRole::FragmentExecutor], [7, 9], 0.75);
        assert_eq!(digest_v1(&left), digest_v1(&right));

        let next_attempt = right
            .with_execution_id(
                QueryExecutionId::new(
                    right.execution_id().query_id(),
                    AttemptId::new(2).expect("nonzero attempt"),
                )
                .expect("valid execution id"),
            )
            .expect("manifest without runtime filter accepts a new attempt");
        assert_ne!(digest_v1(&left), digest_v1(&next_attempt));
    }

    #[test]
    fn participant_manifest_digest_uses_spill_threshold_bits() {
        let left = manifest_with_orders(
            1,
            [ParticipantRole::FragmentExecutor],
            [7],
            f64::from_bits(0x3fe8_0000_0000_0000),
        );
        let changed = manifest_with_orders(
            1,
            [ParticipantRole::FragmentExecutor],
            [7],
            f64::from_bits(0x3fe8_0000_0000_0001),
        );

        assert_ne!(digest_v1(&left), digest_v1(&changed));
    }
}
