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

//! Evidence markers and file metrics of the page streams a typed scan reads.

use novarocks_execution::connector::ScheduledSplitFacts;
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
use novarocks_spi::connector::read_stack::PageSourceFileMetrics;

use crate::read_attempt::ReceivedReadSplit;

/// Test-only identity emitted with one provider-owned reader lifecycle.
#[derive(Clone)]
pub struct TypedConnectorReaderMarker {
    provider_id: String,
    instance_id: String,
    catalog_version: String,
    scheduled_split_sequence_id: u64,
}

impl TypedConnectorReaderMarker {
    pub fn for_split(split: &ReceivedReadSplit, enabled: bool) -> Option<Self> {
        if !enabled {
            return None;
        }
        let binding = split.split().binding();
        Some(Self {
            provider_id: binding.descriptor().provider_id.as_str().to_string(),
            instance_id: binding.descriptor().instance_id.as_str().to_string(),
            catalog_version: hex_encode(binding.catalog_handle().version().as_bytes()),
            scheduled_split_sequence_id: split.sequence_id(),
        })
    }

    pub(crate) fn emit(&self, event: &str) {
        println!(
            "NOVAROCKS_CONNECTOR_UNIT_READER_{event} provider={} instance={} catalog_version={} scheduled_split_sequence_id={}",
            self.provider_id,
            self.instance_id,
            self.catalog_version,
            self.scheduled_split_sequence_id,
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

pub fn flush_page_source_file_metrics(
    profile: Option<&RuntimeProfile>,
    last: &mut PageSourceFileMetrics,
    snapshot: PageSourceFileMetrics,
) {
    let delta = snapshot.saturating_delta_since(*last);
    *last = snapshot;
    let Some(profile) = profile else {
        return;
    };
    for (name, unit, value) in [
        (
            "ConnectorFileBytesRead",
            ProfileUnit::Bytes,
            delta.bytes_read,
        ),
        (
            "ConnectorFileReadRequests",
            ProfileUnit::Unit,
            delta.read_requests,
        ),
        (
            "ConnectorFileRowsDecoded",
            ProfileUnit::Unit,
            delta.rows_decoded,
        ),
        (
            "ConnectorFileBatchesDelivered",
            ProfileUnit::Unit,
            delta.batches_delivered,
        ),
        (
            "ConnectorFileCacheHits",
            ProfileUnit::Unit,
            delta.cache_hits,
        ),
        (
            "ConnectorFileCacheMisses",
            ProfileUnit::Unit,
            delta.cache_misses,
        ),
        ("ConnectorFileIoTime", ProfileUnit::TimeNs, delta.io_time_ns),
        (
            "ConnectorFileDecodeTime",
            ProfileUnit::TimeNs,
            delta.decode_time_ns,
        ),
        (
            "ConnectorFileRowGroupsRead",
            ProfileUnit::Unit,
            delta.row_groups_read,
        ),
        (
            "ConnectorFileRowGroupsPruned",
            ProfileUnit::Unit,
            delta.row_groups_pruned,
        ),
        (
            "ConnectorFileDelayedMaterializationRanges",
            ProfileUnit::Unit,
            delta.delayed_materialization_ranges,
        ),
        (
            "ConnectorFilePageIndexAttempts",
            ProfileUnit::Unit,
            delta.page_index_attempts,
        ),
        (
            "ConnectorFilePageIndexFallbacks",
            ProfileUnit::Unit,
            delta.page_index_fallbacks,
        ),
        (
            "ConnectorFilePageIndexRowsConsidered",
            ProfileUnit::Unit,
            delta.page_index_rows_considered,
        ),
        (
            "ConnectorFilePageIndexRowsPruned",
            ProfileUnit::Unit,
            delta.page_index_rows_pruned,
        ),
    ] {
        if value > 0 {
            profile.counter_add(name, unit, value.min(i64::MAX as u64) as i64);
        }
    }
}
