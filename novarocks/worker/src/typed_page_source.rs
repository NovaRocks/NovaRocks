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

//! Worker ownership of live typed connector page sources.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use novarocks_execution::connector::{ConnectorPageAdapter, PageConversion, ScheduledSplitFacts};
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

    fn emit(&self, event: &str) {
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

/// Fragment-local ownership of every page source one typed scan opened.
#[derive(Default)]
pub struct TypedPageSourceGroup {
    state: Mutex<TypedPageSourceGroupState>,
}

#[derive(Default)]
struct TypedPageSourceGroupState {
    phase: TypedPageSourcePhase,
    next_id: usize,
    open: BTreeMap<usize, SharedPageSource>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum TypedPageSourcePhase {
    #[default]
    Open,
    Terminating,
    Closed,
}

struct TypedPageSourceSlot {
    adapter: Option<ConnectorPageAdapter>,
    marker: Option<TypedConnectorReaderMarker>,
    profile: Option<RuntimeProfile>,
    last_file_metrics: PageSourceFileMetrics,
}

type SharedPageSource = Arc<Mutex<TypedPageSourceSlot>>;

impl TypedPageSourceGroup {
    pub fn register(
        self: &Arc<Self>,
        adapter: ConnectorPageAdapter,
        marker: Option<TypedConnectorReaderMarker>,
        profile: Option<RuntimeProfile>,
    ) -> Result<RegisteredPageSource, String> {
        let slot: SharedPageSource = Arc::new(Mutex::new(TypedPageSourceSlot {
            adapter: Some(adapter),
            marker: marker.clone(),
            profile,
            last_file_metrics: PageSourceFileMetrics::default(),
        }));
        let id = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "typed connector page source group lock poisoned".to_string())?;
            if state.phase != TypedPageSourcePhase::Open {
                return Err(format!(
                    "typed connector page source group is {:?}",
                    state.phase
                ));
            }
            let id = state.next_id;
            state.next_id = state.next_id.saturating_add(1);
            state.open.insert(id, Arc::clone(&slot));
            id
        };
        if let Some(marker) = marker.as_ref() {
            marker.emit("OPEN");
        }
        Ok(RegisteredPageSource {
            slot,
            group: Arc::downgrade(self),
            id,
        })
    }

    fn unregister(&self, id: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.open.remove(&id);
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.phase != TypedPageSourcePhase::Open)
            .unwrap_or(true)
    }

    pub fn terminate(&self) -> Result<(), String> {
        let open = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "typed connector page source group lock poisoned".to_string())?;
            if state.phase != TypedPageSourcePhase::Open {
                return Ok(());
            }
            state.phase = TypedPageSourcePhase::Terminating;
            std::mem::take(&mut state.open)
                .into_values()
                .collect::<Vec<_>>()
        };
        let mut cleanup_errors = Vec::new();
        for slot in open {
            if let Err(error) = close_slot(&slot) {
                cleanup_errors.push(error);
            }
        }
        match self.state.lock() {
            Ok(mut state) => state.phase = TypedPageSourcePhase::Closed,
            Err(_) => {
                cleanup_errors.push("typed connector page source group lock poisoned".to_string())
            }
        }
        if cleanup_errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "typed connector page source cleanup failed: {}",
                cleanup_errors.join("; ")
            ))
        }
    }
}

fn close_slot(slot: &SharedPageSource) -> Result<(), String> {
    let mut guard = slot
        .lock()
        .map_err(|_| "typed connector page source lock poisoned".to_string())?;
    let Some(adapter) = guard.adapter.as_mut() else {
        return Ok(());
    };
    let result = adapter.close().map_err(|error| error.to_string());
    let metrics = adapter.metrics().file;
    let profile = guard.profile.clone();
    flush_page_source_file_metrics(profile.as_ref(), &mut guard.last_file_metrics, metrics);
    guard.adapter = None;
    if let Some(marker) = guard.marker.as_ref() {
        marker.emit("CLOSE");
    }
    result
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

pub struct RegisteredPageSource {
    slot: SharedPageSource,
    group: Weak<TypedPageSourceGroup>,
    id: usize,
}

impl RegisteredPageSource {
    pub fn pull(&self) -> Result<PageConversion, String> {
        let mut guard = self
            .slot
            .lock()
            .map_err(|_| "typed connector page source lock poisoned".to_string())?;
        let result = match guard.adapter.as_mut() {
            None => return Ok(PageConversion::Finished),
            Some(adapter) => adapter.pull().map_err(|error| error.to_string()),
        };
        let metrics = guard
            .adapter
            .as_ref()
            .expect("typed page source remains installed after pull")
            .metrics()
            .file;
        let profile = guard.profile.clone();
        flush_page_source_file_metrics(profile.as_ref(), &mut guard.last_file_metrics, metrics);
        result
    }

    pub fn is_blocked(&self) -> bool {
        self.slot
            .lock()
            .ok()
            .and_then(|guard| {
                guard
                    .adapter
                    .as_ref()
                    .map(ConnectorPageAdapter::source_is_blocked)
            })
            .unwrap_or(false)
    }

    pub fn close(self) -> Result<(), String> {
        let result = close_slot(&self.slot);
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
        result
    }
}

impl Drop for RegisteredPageSource {
    fn drop(&mut self) {
        let _ = close_slot(&self.slot);
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.id);
        }
    }
}
