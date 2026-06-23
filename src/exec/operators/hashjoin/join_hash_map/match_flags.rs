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
//! Flat build-row match tracking and broadcast merge accumulator.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildMatchFlags {
    flags: Vec<bool>,
}

impl BuildMatchFlags {
    pub(crate) fn new(row_count: usize) -> Self {
        Self {
            flags: vec![false; row_count],
        }
    }

    pub(crate) fn mark(&mut self, build_row: u32) -> Result<bool, String> {
        let row = build_row as usize;
        let len = self.flags.len();
        let Some(slot) = self.flags.get_mut(row) else {
            return Err(format!(
                "join build match row out of bounds: row={} len={}",
                row, len
            ));
        };
        let was_new = !*slot;
        *slot = true;
        Ok(was_new)
    }

    pub(crate) fn is_matched(&self, build_row: usize) -> bool {
        self.flags.get(build_row).copied().unwrap_or(false)
    }

    pub(crate) fn matched_indices(&self) -> Vec<u32> {
        self.flags
            .iter()
            .enumerate()
            .filter_map(|(row, matched)| {
                matched.then(|| u32::try_from(row).expect("join build row id exceeds u32"))
            })
            .collect()
    }

    pub(crate) fn unmatched_indices(&self) -> Vec<u32> {
        self.flags
            .iter()
            .enumerate()
            .filter_map(|(row, matched)| {
                (!matched).then(|| u32::try_from(row).expect("join build row id exceeds u32"))
            })
            .collect()
    }

    pub(crate) fn into_vec(self) -> Vec<bool> {
        self.flags
    }
}

#[derive(Debug)]
pub(crate) struct BuildMatchMerge {
    merged: Vec<bool>,
    drivers_merged: usize,
    total_drivers: usize,
}

impl BuildMatchMerge {
    pub(crate) fn new(total_drivers: usize, row_count: usize) -> Self {
        Self {
            merged: vec![false; row_count],
            drivers_merged: 0,
            total_drivers,
        }
    }

    pub(crate) fn merge_one(&mut self, local: Vec<bool>) -> Result<Option<Vec<bool>>, String> {
        if self.drivers_merged >= self.total_drivers {
            return Err("join build match merge already complete".to_string());
        }
        if local.len() != self.merged.len() {
            return Err(format!(
                "join build match merge length mismatch: local={} merged={}",
                local.len(),
                self.merged.len()
            ));
        }
        for (slot, matched) in self.merged.iter_mut().zip(local.into_iter()) {
            *slot = *slot || matched;
        }
        self.drivers_merged += 1;
        if self.drivers_merged == self.total_drivers {
            Ok(Some(std::mem::take(&mut self.merged)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_flags_mark_and_emit_matched_and_unmatched_indices() {
        let mut flags = BuildMatchFlags::new(4);

        assert!(flags.mark(1).expect("mark"));
        assert!(!flags.mark(1).expect("duplicate mark"));
        assert!(flags.mark(3).expect("mark"));

        assert_eq!(flags.matched_indices(), vec![1, 3]);
        assert_eq!(flags.unmatched_indices(), vec![0, 2]);
    }

    #[test]
    fn merge_or_accumulates_flat_flags_across_drivers() {
        let mut merge = BuildMatchMerge::new(3, 4);

        assert_eq!(
            merge
                .merge_one(vec![true, false, false, false])
                .expect("merge"),
            None
        );
        assert_eq!(
            merge
                .merge_one(vec![false, false, true, false])
                .expect("merge"),
            None
        );
        assert_eq!(
            merge
                .merge_one(vec![false, true, false, false])
                .expect("merge"),
            Some(vec![true, true, true, false])
        );
    }

    #[test]
    fn merge_rejects_local_flags_after_completion() {
        let mut merge = BuildMatchMerge::new(1, 2);
        assert_eq!(
            merge.merge_one(vec![true, false]).expect("merge"),
            Some(vec![true, false])
        );

        let err = merge
            .merge_one(vec![false, true])
            .expect_err("already complete");

        assert_eq!(err, "join build match merge already complete");
    }

    #[test]
    fn merge_rejects_zero_driver_merge() {
        let mut merge = BuildMatchMerge::new(0, 2);

        let err = merge
            .merge_one(vec![false, true])
            .expect_err("already complete");

        assert_eq!(err, "join build match merge already complete");
    }
}
