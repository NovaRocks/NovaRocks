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

use std::io;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::fs::opendal::{OpendalRangeReader, RemoteReadKind};
use crate::fs::range_plan::{IoRange, IoRangeKind, PlannedIoRanges, merge_adjacent_ranges};

pub trait RemoteRangeSource: Clone + Send + Sync + 'static {
    fn file_len(&self) -> u64;
    fn read_remote_range(&self, start: u64, end: u64) -> io::Result<Bytes>;

    fn read_remote_range_with_kind(
        &self,
        start: u64,
        end: u64,
        kind: RemoteReadKind,
    ) -> io::Result<Bytes> {
        let _ = kind;
        self.read_remote_range(start, end)
    }
}

impl RemoteRangeSource for OpendalRangeReader {
    fn file_len(&self) -> u64 {
        OpendalRangeReader::len(self)
    }

    fn read_remote_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        OpendalRangeReader::read_remote_range(self, start, end)
    }

    fn read_remote_range_with_kind(
        &self,
        start: u64,
        end: u64,
        kind: RemoteReadKind,
    ) -> io::Result<Bytes> {
        OpendalRangeReader::read_remote_range_with_kind(self, start, end, kind)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoalescedReadOptions {
    pub enable: bool,
    pub max_distance: u64,
    pub max_buffer_size: u64,
}

impl Default for CoalescedReadOptions {
    fn default() -> Self {
        Self {
            enable: true,
            max_distance: 1024 * 1024,
            max_buffer_size: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CoalescedRangeReader<R: RemoteRangeSource> {
    inner: R,
    options: CoalescedReadOptions,
    state: Arc<Mutex<CoalescedState>>,
}

#[derive(Debug, Default)]
struct CoalescedState {
    plans: Vec<PlannedBuffer>,
}

#[derive(Clone, Debug)]
struct PlannedBuffer {
    range: IoRange,
    data: Option<Bytes>,
}

impl PlannedBuffer {
    fn contains(&self, start: u64, end: u64) -> bool {
        self.range.contains(start, end)
    }
}

impl<R: RemoteRangeSource> CoalescedRangeReader<R> {
    pub fn new(inner: R, options: CoalescedReadOptions) -> Self {
        Self {
            inner,
            options,
            state: Arc::new(Mutex::new(CoalescedState::default())),
        }
    }

    pub fn set_io_ranges(&self, ranges: PlannedIoRanges, coalesce_active_lazy_together: bool) {
        if !self.options.enable || self.options.max_buffer_size == 0 {
            let mut state = self.state.lock().expect("coalesced reader state lock");
            state.plans.clear();
            return;
        }

        let mut raw_ranges = ranges.into_sorted();
        if has_overlap(&mut raw_ranges) {
            let mut state = self.state.lock().expect("coalesced reader state lock");
            state.plans.clear();
            return;
        }

        let merged = if coalesce_active_lazy_together {
            merge_adjacent_ranges(
                raw_ranges
                    .into_iter()
                    .map(|range| IoRange::active(range.offset, range.size))
                    .collect(),
                self.options.max_distance,
                self.options.max_buffer_size,
            )
        } else {
            build_active_lazy_plans(
                raw_ranges,
                self.options.max_distance,
                self.options.max_buffer_size,
            )
        };
        let mut state = self.state.lock().expect("coalesced reader state lock");
        state.plans = merged
            .into_iter()
            .map(|range| PlannedBuffer { range, data: None })
            .collect();
    }

    pub fn read_bytes(&self, start: u64, length: usize) -> io::Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range overflow"))?;
        let file_len = self.inner.file_len();
        if start > file_len || end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "expected to read {} bytes at offset {}, while file has length {}",
                    length, start, file_len
                ),
            ));
        }
        if length == 0 {
            return Ok(Bytes::new());
        }
        if !self.options.enable || self.options.max_buffer_size == 0 {
            return self
                .inner
                .read_remote_range_with_kind(start, end, RemoteReadKind::Direct);
        }

        // StarRocks-like behavior: coalesce only for planned ranges.
        // If no planned range contains the request, fallback to exact remote read.
        let candidate = {
            let state = self.state.lock().expect("coalesced reader state lock");
            state
                .plans
                .iter()
                .find(|plan| plan.contains(start, end))
                .map(|plan| (plan.range, plan.data.clone()))
        };

        let Some((range, maybe_data)) = candidate else {
            return self
                .inner
                .read_remote_range_with_kind(start, end, RemoteReadKind::Direct);
        };

        if let Some(data) = maybe_data {
            return slice_range(&range, &data, start, end);
        }

        let fetched = self.inner.read_remote_range_with_kind(
            range.offset,
            range.end(),
            RemoteReadKind::SharedBuffered,
        )?;
        let out = slice_range(&range, &fetched, start, end)?;
        let mut state = self.state.lock().expect("coalesced reader state lock");
        if let Some(plan) = state
            .plans
            .iter_mut()
            .find(|plan| plan.range == range && plan.data.is_none())
        {
            plan.data = Some(fetched);
        }
        Ok(out)
    }
}

fn slice_range(range: &IoRange, data: &Bytes, start: u64, end: u64) -> io::Result<Bytes> {
    let from = usize::try_from(start.saturating_sub(range.offset)).unwrap_or(usize::MAX);
    let to = usize::try_from(end.saturating_sub(range.offset)).unwrap_or(usize::MAX);
    if to > data.len() || from > to {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "coalesced buffer out of bound: range={}..{} request={}..{} data_len={}",
                range.offset,
                range.end(),
                start,
                end,
                data.len()
            ),
        ));
    }
    Ok(data.slice(from..to))
}

fn has_overlap(ranges: &mut [IoRange]) -> bool {
    ranges.sort_by(|a, b| {
        if a.offset != b.offset {
            return a.offset.cmp(&b.offset);
        }
        a.size.cmp(&b.size)
    });
    ranges
        .windows(2)
        .any(|window| window[1].offset < window[0].end())
}

fn build_active_lazy_plans(
    ranges: Vec<IoRange>,
    max_distance: u64,
    max_buffer_size: u64,
) -> Vec<IoRange> {
    let mut active = Vec::new();
    for range in &ranges {
        if range.kind == IoRangeKind::Active {
            active.push(*range);
        }
    }
    let mut plans = merge_adjacent_ranges(active, max_distance, max_buffer_size);

    let mut lazy_batch = Vec::new();
    for range in ranges {
        if range.kind == IoRangeKind::Active {
            if !lazy_batch.is_empty() {
                plans.extend(merge_adjacent_ranges(
                    std::mem::take(&mut lazy_batch),
                    max_distance,
                    max_buffer_size,
                ));
            }
            continue;
        }
        if covered_by_any_plan(&plans, range.offset, range.end()) {
            continue;
        }
        lazy_batch.push(range);
    }
    if !lazy_batch.is_empty() {
        plans.extend(merge_adjacent_ranges(
            lazy_batch,
            max_distance,
            max_buffer_size,
        ));
    }
    plans.sort_by_key(|range| range.offset);
    plans
}

fn covered_by_any_plan(plans: &[IoRange], start: u64, end: u64) -> bool {
    plans.iter().any(|plan| plan.contains(start, end))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;

    use super::{CoalescedRangeReader, CoalescedReadOptions, RemoteRangeSource};
    use crate::fs::range_plan::PlannedIoRanges;

    #[derive(Clone, Debug)]
    struct MockRangeReader {
        data: Bytes,
        calls: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    impl MockRangeReader {
        fn new(size: usize) -> Self {
            Self {
                data: Bytes::from(vec![b'x'; size]),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(u64, u64)> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    impl RemoteRangeSource for MockRangeReader {
        fn file_len(&self) -> u64 {
            self.data.len() as u64
        }

        fn read_remote_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
            self.calls.lock().expect("calls lock").push((start, end));
            let from = usize::try_from(start).unwrap_or(usize::MAX);
            let to = usize::try_from(end).unwrap_or(usize::MAX);
            Ok(self.data.slice(from..to))
        }
    }

    #[test]
    fn coalesced_reader_without_hints_uses_exact_reads() {
        let inner = MockRangeReader::new(4096);
        let reader = CoalescedRangeReader::new(
            inner.clone(),
            CoalescedReadOptions {
                enable: true,
                max_distance: 1024,
                max_buffer_size: 1024,
            },
        );
        let _ = reader.read_bytes(100, 64).expect("first read");
        let _ = reader.read_bytes(200, 64).expect("second read");
        assert_eq!(inner.calls(), vec![(100, 164), (200, 264)]);
    }

    #[test]
    fn coalesced_reader_honors_io_ranges() {
        let inner = MockRangeReader::new(4096);
        let reader = CoalescedRangeReader::new(
            inner.clone(),
            CoalescedReadOptions {
                enable: true,
                max_distance: 32,
                max_buffer_size: 256,
            },
        );
        let mut plan = PlannedIoRanges::default();
        plan.push_active(0, 64);
        plan.push_active(80, 64);
        reader.set_io_ranges(plan, true);
        let _ = reader.read_bytes(88, 16).expect("hinted read");
        assert_eq!(inner.calls().len(), 1);
        assert_eq!(inner.calls(), vec![(0, 144)]);

        let _ = reader.read_bytes(96, 8).expect("reuse hinted buffer");
        assert_eq!(inner.calls(), vec![(0, 144)]);
    }

    #[test]
    fn coalesced_reader_can_be_disabled() {
        let inner = MockRangeReader::new(4096);
        let reader = CoalescedRangeReader::new(
            inner.clone(),
            CoalescedReadOptions {
                enable: false,
                max_distance: 1024,
                max_buffer_size: 1024,
            },
        );
        let _ = reader.read_bytes(10, 10).expect("first direct read");
        let _ = reader.read_bytes(20, 10).expect("second direct read");
        assert_eq!(inner.calls().len(), 2);
    }

    #[test]
    fn coalesced_reader_separates_active_and_lazy_when_requested() {
        let inner = MockRangeReader::new(4096);
        let reader = CoalescedRangeReader::new(
            inner.clone(),
            CoalescedReadOptions {
                enable: true,
                max_distance: 16,
                max_buffer_size: 256,
            },
        );
        let mut plan = PlannedIoRanges::default();
        plan.push_active(0, 32);
        plan.push_lazy(40, 32);
        plan.push_active(80, 32);
        reader.set_io_ranges(plan, false);

        let _ = reader.read_bytes(42, 8).expect("lazy read");
        assert_eq!(inner.calls(), vec![(40, 72)]);
    }

    #[test]
    fn coalesced_reader_deduplicates_lazy_when_covered_by_active() {
        let inner = MockRangeReader::new(4096);
        let reader = CoalescedRangeReader::new(
            inner.clone(),
            CoalescedReadOptions {
                enable: true,
                max_distance: 64,
                max_buffer_size: 512,
            },
        );
        let mut plan = PlannedIoRanges::default();
        plan.push_active(0, 32);
        plan.push_active(80, 32);
        plan.push_lazy(40, 16);
        reader.set_io_ranges(plan, false);

        let _ = reader.read_bytes(44, 4).expect("covered lazy read");
        assert_eq!(inner.calls(), vec![(0, 112)]);
    }
}
