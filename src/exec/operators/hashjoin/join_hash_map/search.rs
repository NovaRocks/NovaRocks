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
//! Selection-vector and probe-mask primitives for hash join search.

use arrow::array::{Array, BooleanArray};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct JoinSelection {
    pub(crate) probe: Vec<u32>,
    pub(crate) build: Vec<u32>,
}

impl JoinSelection {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&mut self, probe_row: u32, build_row: u32) {
        self.probe.push(probe_row);
        self.build.push(build_row);
    }

    pub(crate) fn len(&self) -> usize {
        self.probe.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.probe.is_empty()
    }

    pub(crate) fn compact_by_mask(&mut self, mask: &BooleanArray) -> Result<(), String> {
        if self.probe.len() != self.build.len() {
            return Err(format!(
                "join selection length mismatch: probe={} build={}",
                self.probe.len(),
                self.build.len()
            ));
        }
        if mask.len() != self.len() {
            return Err(format!(
                "join residual mask length mismatch: mask={} selection={}",
                mask.len(),
                self.len()
            ));
        }
        let mut write = 0usize;
        for read in 0..mask.len() {
            if mask.is_valid(read) && mask.value(read) {
                self.probe[write] = self.probe[read];
                self.build[write] = self.build[read];
                write += 1;
            }
        }
        self.probe.truncate(write);
        self.build.truncate(write);
        Ok(())
    }
}

pub(crate) fn append_cross_selection(
    selection: &mut JoinSelection,
    probe_rows: &[u32],
    build_rows: &[u32],
    max_pairs: usize,
) -> bool {
    if selection.len() >= max_pairs {
        return true;
    }

    for &probe_row in probe_rows {
        for &build_row in build_rows {
            selection.push(probe_row, build_row);
            if selection.len() >= max_pairs {
                return true;
            }
        }
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchStats {
    pub(crate) lookup_hit_rows: u64,
    pub(crate) lookup_miss_rows: u64,
}

impl SearchStats {
    pub(crate) fn from_group_ids(group_ids: &[Option<usize>]) -> Self {
        let mut stats = Self {
            lookup_hit_rows: 0,
            lookup_miss_rows: 0,
        };
        for group_id in group_ids {
            if group_id.is_some() {
                stats.lookup_hit_rows += 1;
            } else {
                stats.lookup_miss_rows += 1;
            }
        }
        stats
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeMask {
    keep: Vec<bool>,
}

impl ProbeMask {
    pub(crate) fn new(len: usize, value: bool) -> Self {
        Self {
            keep: vec![value; len],
        }
    }

    pub(crate) fn set(&mut self, row: usize, value: bool) -> Result<(), String> {
        let len = self.keep.len();
        let Some(slot) = self.keep.get_mut(row) else {
            return Err(format!(
                "join probe mask row out of bounds: row={row} len={len}"
            ));
        };
        *slot = value;
        Ok(())
    }

    pub(crate) fn as_slice(&self) -> &[bool] {
        &self.keep
    }

    pub(crate) fn into_vec(self) -> Vec<bool> {
        self.keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_pair_compacts_boolean_mask() {
        let mut selection = JoinSelection {
            probe: vec![0, 0, 2, 3, 3],
            build: vec![0, 2, 3, 0, 2],
        };
        let mask = BooleanArray::from(vec![Some(true), Some(false), None, Some(true), Some(true)]);

        selection.compact_by_mask(&mask).expect("compact");

        assert_eq!(selection.probe, vec![0, 3, 3]);
        assert_eq!(selection.build, vec![0, 0, 2]);
    }

    #[test]
    fn selection_compaction_rejects_mismatched_pair_lengths() {
        let mut selection = JoinSelection {
            probe: vec![0, 1],
            build: vec![0],
        };
        let mask = BooleanArray::from(vec![Some(true), Some(true)]);

        let err = selection.compact_by_mask(&mask).expect_err("mismatch");

        assert_eq!(err, "join selection length mismatch: probe=2 build=1");
    }

    #[test]
    fn probe_mask_set_updates_selected_row() {
        let mut mask = ProbeMask::new(3, false);

        mask.set(1, true).expect("set");

        assert_eq!(mask.as_slice(), &[false, true, false]);
    }

    #[test]
    fn probe_mask_rejects_out_of_bounds_row() {
        let mut mask = ProbeMask::new(2, false);

        let err = mask.set(2, true).expect_err("out of bounds");

        assert_eq!(err, "join probe mask row out of bounds: row=2 len=2");
    }

    #[test]
    fn cross_selection_stops_at_pair_limit() {
        let mut selection = JoinSelection::new();

        let stopped = append_cross_selection(&mut selection, &[1, 2], &[10, 11, 12], 5);

        assert!(stopped);
        assert_eq!(selection.probe, vec![1, 1, 1, 2, 2]);
        assert_eq!(selection.build, vec![10, 11, 12, 10, 11]);
    }

    #[test]
    fn search_stats_counts_hits_and_misses() {
        let stats = SearchStats::from_group_ids(&[Some(0), None, Some(3), None]);

        assert_eq!(stats.lookup_hit_rows, 2);
        assert_eq!(stats.lookup_miss_rows, 2);
    }
}
