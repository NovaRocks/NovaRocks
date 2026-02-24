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
use std::time::{Duration, Instant};

use crate::exec::chunk::Chunk;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanOp};
use crate::novarocks_logging::debug;

use crate::runtime::exchange;
use crate::runtime::profile::{RuntimeProfile, clamp_u128_to_i64};

pub struct ExchangeScanOp {
    key: exchange::ExchangeKey,
    expected_senders: usize,
    timeout: Duration,
}

impl ExchangeScanOp {
    pub fn new(key: exchange::ExchangeKey, expected_senders: usize, timeout: Duration) -> Self {
        debug!(
            "ExchangeScanOp::new: finst={}:{} node_id={} expected_senders={} timeout={:?}",
            key.finst_id_hi, key.finst_id_lo, key.node_id, expected_senders, timeout
        );
        Self {
            key,
            expected_senders,
            timeout,
        }
    }
}

impl ScanOp for ExchangeScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        match morsel {
            ScanMorsel::Exchange => {}
            _ => return Err("exchange scan received unexpected morsel".to_string()),
        }
        let handle = exchange::get_receiver_handle(self.key, self.expected_senders)?;
        let iter = ExchangeScanIter::new(
            self.key,
            self.expected_senders,
            self.timeout,
            handle,
            profile,
        );
        Ok(Box::new(iter))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        Ok(ScanMorsels::new(vec![ScanMorsel::Exchange], false))
    }

    fn profile_name(&self) -> Option<String> {
        Some(format!("EXCHANGE_SOURCE (id={})", self.key.node_id))
    }
}

struct ExchangeScanIter {
    key: exchange::ExchangeKey,
    expected_senders: usize,
    timeout: Duration,
    start: Instant,
    handle: exchange::ExchangeReceiverHandle,
    profile: Option<RuntimeProfile>,
    completed: bool,
    errored: bool,
    seen_chunks: usize,
    seen_rows: usize,
}

impl Iterator for ExchangeScanIter {
    type Item = Result<Chunk, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.completed || self.errored {
            return None;
        }
        match self.handle.pop_next_with_stats_blocking(
            self.expected_senders,
            self.start,
            self.timeout,
        ) {
            Ok(exchange::ExchangePopResult::Chunk(chunk)) => {
                self.seen_chunks = self.seen_chunks.saturating_add(1);
                self.seen_rows = self.seen_rows.saturating_add(chunk.len());
                Some(Ok(chunk))
            }
            Ok(exchange::ExchangePopResult::Finished(stats)) => {
                self.completed = true;
                self.finish_profile(stats);
                None
            }
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
    }
}

impl Drop for ExchangeScanIter {
    fn drop(&mut self) {
        if self.completed {
            debug!(
                "ExchangeScanIter completed: finst={}:{} node_id={} chunks={} rows={}",
                self.key.finst_id_hi,
                self.key.finst_id_lo,
                self.key.node_id,
                self.seen_chunks,
                self.seen_rows
            );
            return;
        }

        if !self.errored {
            debug!(
                "ExchangeScanIter dropped early: finst={}:{} node_id={} seen_chunks={} seen_rows={}",
                self.key.finst_id_hi,
                self.key.finst_id_lo,
                self.key.node_id,
                self.seen_chunks,
                self.seen_rows
            );
        }

        exchange::cancel_exchange_key(self.key);
    }
}

impl ExchangeScanIter {
    fn new(
        key: exchange::ExchangeKey,
        expected_senders: usize,
        timeout: Duration,
        handle: exchange::ExchangeReceiverHandle,
        profile: Option<RuntimeProfile>,
    ) -> Self {
        debug!(
            "ExchangeScanIter::new: finst={}:{} node_id={} expected_senders={} timeout={:?}",
            key.finst_id_hi, key.finst_id_lo, key.node_id, expected_senders, timeout
        );
        Self {
            key,
            expected_senders,
            timeout,
            start: Instant::now(),
            handle,
            profile,
            completed: false,
            errored: false,
            seen_chunks: 0,
            seen_rows: 0,
        }
    }

    fn finish_profile(&self, stats: exchange::ExchangeRecvStats) {
        let Some(profile) = self.profile.as_ref() else {
            return;
        };

        let elapsed = self.start.elapsed();
        profile.add_info_string("DestID", format!("{}", self.key.node_id));
        profile.counter_add(
            "RequestReceived",
            crate::metrics::TUnit::UNIT,
            clamp_u128_to_i64(stats.request_received),
        );
        profile.counter_add(
            "BytesReceived",
            crate::metrics::TUnit::BYTES,
            clamp_u128_to_i64(stats.bytes_received),
        );
        profile.counter_add(
            "DeserializeChunkTime",
            crate::metrics::TUnit::TIME_NS,
            clamp_u128_to_i64(stats.deserialize_ns),
        );
        let elapsed_ns = clamp_u128_to_i64(elapsed.as_nanos());
        profile.counter_add(
            "ReceiverProcessTotalTime",
            crate::metrics::TUnit::TIME_NS,
            elapsed_ns,
        );
        let wait_ns = clamp_u128_to_i64(elapsed.as_nanos().saturating_sub(stats.deserialize_ns));
        profile.counter_add("WaitTime", crate::metrics::TUnit::TIME_NS, wait_ns);
    }
}
