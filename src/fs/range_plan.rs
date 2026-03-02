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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoRangeKind {
    Active,
    Lazy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoRange {
    pub offset: u64,
    pub size: u64,
    pub kind: IoRangeKind,
}

impl IoRange {
    pub fn new(offset: u64, size: u64) -> Self {
        Self::active(offset, size)
    }

    pub fn active(offset: u64, size: u64) -> Self {
        Self {
            offset,
            size,
            kind: IoRangeKind::Active,
        }
    }

    pub fn lazy(offset: u64, size: u64) -> Self {
        Self {
            offset,
            size,
            kind: IoRangeKind::Lazy,
        }
    }

    pub fn end(&self) -> u64 {
        self.offset.saturating_add(self.size)
    }

    pub fn contains(&self, start: u64, end: u64) -> bool {
        self.offset <= start && self.end() >= end
    }

    pub fn is_active(&self) -> bool {
        matches!(self.kind, IoRangeKind::Active)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlannedIoRanges {
    pub active: Vec<IoRange>,
    pub lazy: Vec<IoRange>,
}

impl PlannedIoRanges {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.lazy.is_empty()
    }

    pub fn all_active(ranges: Vec<IoRange>) -> Self {
        let mut active = Vec::with_capacity(ranges.len());
        for range in ranges {
            active.push(IoRange::active(range.offset, range.size));
        }
        Self {
            active,
            lazy: Vec::new(),
        }
    }

    pub fn push_active(&mut self, offset: u64, size: u64) {
        self.active.push(IoRange::active(offset, size));
    }

    pub fn push_lazy(&mut self, offset: u64, size: u64) {
        self.lazy.push(IoRange::lazy(offset, size));
    }

    pub fn into_sorted(self) -> Vec<IoRange> {
        let mut all = self.active;
        all.extend(self.lazy);
        all.sort_by(|a, b| {
            if a.offset != b.offset {
                return a.offset.cmp(&b.offset);
            }
            if a.size != b.size {
                return a.size.cmp(&b.size);
            }
            // Keep active range before lazy range under same (offset, size).
            if a.is_active() == b.is_active() {
                std::cmp::Ordering::Equal
            } else if a.is_active() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });
        all
    }
}

// Merge adjacent ranges when the hole is within `max_distance` and merged size
// does not exceed `max_merged_size`.
pub fn merge_adjacent_ranges(
    mut ranges: Vec<IoRange>,
    max_distance: u64,
    max_merged_size: u64,
) -> Vec<IoRange> {
    ranges.retain(|r| r.size > 0);
    if ranges.is_empty() {
        return ranges;
    }

    ranges.sort_by_key(|r| r.offset);
    let mut merged = Vec::with_capacity(ranges.len());
    let mut last = ranges[0];

    for current in ranges.into_iter().skip(1) {
        if current.kind != last.kind {
            merged.push(last);
            last = current;
            continue;
        }
        let merged_end = last.end().max(current.end());
        let merged_size = merged_end.saturating_sub(last.offset);
        let close_enough = last.end().saturating_add(max_distance) >= current.offset;
        if close_enough && merged_size <= max_merged_size {
            last = IoRange {
                offset: last.offset,
                size: merged_size,
                kind: last.kind,
            };
        } else {
            merged.push(last);
            last = current;
        }
    }
    merged.push(last);
    merged
}

#[cfg(test)]
mod tests {
    use super::{IoRange, IoRangeKind, PlannedIoRanges, merge_adjacent_ranges};

    #[test]
    fn merge_adjacent_ranges_merges_close_ranges() {
        let ranges = vec![
            IoRange::new(0, 32),
            IoRange::new(40, 32),
            IoRange::new(200, 16),
        ];
        let merged = merge_adjacent_ranges(ranges, 16, 256);
        assert_eq!(merged, vec![IoRange::new(0, 72), IoRange::new(200, 16)]);
    }

    #[test]
    fn merge_adjacent_ranges_respects_max_merged_size() {
        let ranges = vec![IoRange::new(0, 80), IoRange::new(88, 80)];
        let merged = merge_adjacent_ranges(ranges, 16, 128);
        assert_eq!(merged, vec![IoRange::new(0, 80), IoRange::new(88, 80)]);
    }

    #[test]
    fn merge_adjacent_ranges_ignores_empty_ranges() {
        let ranges = vec![IoRange::new(0, 0), IoRange::new(16, 16)];
        let merged = merge_adjacent_ranges(ranges, 16, 128);
        assert_eq!(merged, vec![IoRange::new(16, 16)]);
    }

    #[test]
    fn merge_adjacent_ranges_keeps_active_lazy_split() {
        let ranges = vec![
            IoRange::active(0, 32),
            IoRange::lazy(40, 32),
            IoRange::active(80, 32),
        ];
        let merged = merge_adjacent_ranges(ranges, 64, 256);
        assert_eq!(
            merged,
            vec![
                IoRange::active(0, 32),
                IoRange::lazy(40, 32),
                IoRange::active(80, 32),
            ]
        );
    }

    #[test]
    fn planned_io_ranges_sorts_all_ranges() {
        let plan = PlannedIoRanges {
            active: vec![IoRange::active(100, 10)],
            lazy: vec![IoRange::lazy(20, 5), IoRange::lazy(10, 3)],
        };
        let sorted = plan.into_sorted();
        assert_eq!(
            sorted,
            vec![
                IoRange::lazy(10, 3),
                IoRange::lazy(20, 5),
                IoRange::active(100, 10)
            ]
        );
        assert!(matches!(sorted[0].kind, IoRangeKind::Lazy));
    }
}
