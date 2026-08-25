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

//! Fragment-instance wire decoding.

use novarocks_execution::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use novarocks_proto::FieldPath;
use novarocks_proto::lifecycle::ScanRangeParams;
use novarocks_types::UniqueId;

use novarocks_proto::novarocks as native_proto;

use super::error::NativeFragmentDecodeError;

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) struct NativeSubmissionMetadata {
    backend_num: i32,
    typed_result_sink: bool,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativeSubmissionMetadata {
    pub(crate) fn new(backend_num: i32, typed_result_sink: bool) -> Self {
        Self {
            backend_num,
            typed_result_sink,
        }
    }

    pub(crate) fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub(crate) fn typed_result_sink(&self) -> bool {
        self.typed_result_sink
    }
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) fn decode_destinations(
    src: &[native_proto::Destination],
) -> Result<Vec<FragmentDestination>, NativeFragmentDecodeError> {
    src.iter()
        .enumerate()
        .map(|(index, destination)| {
            let path = FieldPath::root("instance_params")
                .field("destinations")
                .index(index);
            let finst_id = destination.finst_id.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("finst_id"),
                    "native Destination requires finst_id",
                )
            })?;
            Ok(FragmentDestination::new(
                unique_id(finst_id),
                decode_endpoint_at(&destination.endpoint, path.field("endpoint"))?,
            ))
        })
        .collect()
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) fn decode_scan_range_params(
    src: &native_proto::ScanRangeParams,
) -> Result<ScanRangeParams, NativeFragmentDecodeError> {
    decode_scan_range_params_at(
        src,
        FieldPath::root("instance_params").field("per_node_scan_ranges"),
    )
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
fn decode_endpoint_at(
    src: &str,
    path: FieldPath,
) -> Result<RuntimeEndpoint, NativeFragmentDecodeError> {
    RuntimeEndpoint::parse(src)
        .map_err(|detail| NativeFragmentDecodeError::invalid_value(path, detail))
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(super) fn decode_scan_range_params_at(
    src: &native_proto::ScanRangeParams,
    path: FieldPath,
) -> Result<ScanRangeParams, NativeFragmentDecodeError> {
    let range = src.range.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("range"),
            "native ScanRangeParams requires range",
        )
    })?;
    range.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("range").field("kind"),
            "native ScanRange requires kind",
        )
    })?;
    ScanRangeParams::parse(src.clone())
        .map_err(|error| NativeFragmentDecodeError::invalid_value(path, error.detail()))
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
fn unique_id(src: &novarocks_proto::common::UniqueId) -> UniqueId {
    UniqueId::new(src.hi, src.lo)
}

#[cfg(test)]
mod tests {
    use super::decode_destinations;
    use crate::fragment::decode::query_options::decode_query_options;
    use novarocks_proto::ProtocolErrorKind;
    use novarocks_proto::novarocks as native_proto;

    #[test]
    fn query_options_decode_is_owned_by_native_protocol() {
        let decoded = decode_query_options(&native_proto::QueryOptions {
            batch_size: 1024,
            pipeline_dop: 4,
            ..Default::default()
        })
        .expect("native query options");
        assert_eq!(decoded.batch_size(), Some(1024));
        assert_eq!(decoded.pipeline_dop(), Some(4));
    }

    #[test]
    fn destination_missing_id_has_typed_path() {
        let error = decode_destinations(&[native_proto::Destination {
            finst_id: None,
            endpoint: "127.0.0.1:9070".to_string(),
        }])
        .expect_err("missing finst id");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.path().to_string(),
            "instance_params.destinations[0].finst_id"
        );
    }

    #[test]
    fn query_options_preserve_explicit_zero_and_absent_bitset() {
        let options = native_proto::QueryOptions {
            runtime_filter_scan_wait_time_ms: Some(0),
            runtime_filter_wait_timeout_ms: Some(0),
            group_concat_max_len: Some(0),
            datacache_evict_probability: Some(0),
            ..Default::default()
        };

        let decoded = decode_query_options(&options).expect("round trip native query options");

        assert_eq!(decoded.runtime_filter_scan_wait_time_ms(), Some(0));
        assert_eq!(decoded.runtime_filter_wait_timeout_ms(), Some(0));
        assert_eq!(decoded.group_concat_max_len(), Some(0));
        assert_eq!(decoded.cache().datacache_evict_probability, Some(0));
        assert_eq!(decoded.enable_join_runtime_bitset_filter(), None);
    }

    #[test]
    fn query_options_round_trip_preserves_file_reader_flags() {
        let options = native_proto::QueryOptions {
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            ..Default::default()
        };

        let decoded = decode_query_options(&options).expect("round trip native query options");

        assert!(decoded.orc_use_column_names());
        assert!(decoded.enable_file_metacache());
        assert!(decoded.enable_file_pagecache());
        assert!(decoded.enable_parquet_reader_page_index());
    }

    #[test]
    fn query_options_reject_spill_without_options() {
        let error = decode_query_options(&native_proto::QueryOptions {
            enable_spill: true,
            ..Default::default()
        })
        .expect_err("spill options are required");

        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert!(error.to_string().contains("spill_options"), "{error}");
    }
}
