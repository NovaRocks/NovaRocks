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
//! Pure finalize helpers for join selection vectors and match flags.

use super::match_flags::BuildMatchFlags;
use super::search::JoinSelection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeFinalize {
    pub(crate) matched: Vec<bool>,
    pub(crate) selected: Vec<u32>,
}

fn validate_selection_pair_lengths(selection: &JoinSelection) -> Result<(), String> {
    if selection.probe.len() != selection.build.len() {
        return Err(format!(
            "join selection length mismatch: probe={} build={}",
            selection.probe.len(),
            selection.build.len()
        ));
    }
    Ok(())
}

pub(crate) fn probe_matched_flags(
    probe_len: usize,
    selection: &JoinSelection,
    context: &str,
) -> Result<Vec<bool>, String> {
    validate_selection_pair_lengths(selection)?;
    let mut matched = vec![false; probe_len];
    for &probe_row in &selection.probe {
        let row = probe_row as usize;
        let Some(slot) = matched.get_mut(row) else {
            return Err(format!(
                "{context} probe row out of bounds: row={probe_row} rows={probe_len}"
            ));
        };
        *slot = true;
    }
    Ok(matched)
}

pub(crate) fn select_probe_rows_from_flags(flags: &[bool], want_matched: bool) -> Vec<u32> {
    flags
        .iter()
        .enumerate()
        .filter_map(|(row, matched)| {
            (*matched == want_matched)
                .then(|| u32::try_from(row).expect("join probe row id exceeds u32"))
        })
        .collect()
}

pub(crate) fn finalize_probe_rows(
    probe_len: usize,
    selection: &JoinSelection,
    want_matched: bool,
    context: &str,
) -> Result<ProbeFinalize, String> {
    let matched = probe_matched_flags(probe_len, selection, context)?;
    let selected = select_probe_rows_from_flags(&matched, want_matched);
    Ok(ProbeFinalize { matched, selected })
}

pub(crate) fn mark_build_matches(
    flags: &mut BuildMatchFlags,
    selection: &JoinSelection,
) -> Result<u64, String> {
    validate_selection_pair_lengths(selection)?;
    let mut newly_marked = 0u64;
    for &build_row in &selection.build {
        if flags.mark(build_row)? {
            newly_marked += 1;
        }
    }
    Ok(newly_marked)
}

pub(crate) fn is_all_match_one(selection: &JoinSelection, probe_len: usize) -> bool {
    selection.len() == probe_len
        && selection.build.len() == selection.probe.len()
        && selection
            .probe
            .iter()
            .enumerate()
            .all(|(expected, &row)| row as usize == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalize_probe_rows_selects_matched_and_unmatched() {
        let selection = JoinSelection {
            probe: vec![0, 2, 2, 4],
            build: vec![10, 11, 12, 13],
        };

        let matched =
            finalize_probe_rows(5, &selection, true, "left semi").expect("matched finalize");
        assert_eq!(matched.matched, vec![true, false, true, false, true]);
        assert_eq!(matched.selected, vec![0, 2, 4]);

        let unmatched =
            finalize_probe_rows(5, &selection, false, "left anti").expect("unmatched finalize");
        assert_eq!(unmatched.matched, vec![true, false, true, false, true]);
        assert_eq!(unmatched.selected, vec![1, 3]);
    }

    #[test]
    fn finalize_probe_rows_rejects_out_of_bounds_probe_row() {
        let selection = JoinSelection {
            probe: vec![0, 3],
            build: vec![10, 11],
        };

        let err = finalize_probe_rows(3, &selection, true, "left semi").expect_err("oob probe");

        assert_eq!(err, "left semi probe row out of bounds: row=3 rows=3");
    }

    #[test]
    fn finalize_probe_rows_rejects_mismatched_selection_lengths() {
        let selection = JoinSelection {
            probe: vec![0],
            build: vec![10, 11],
        };

        let err =
            finalize_probe_rows(2, &selection, true, "left semi").expect_err("length mismatch");

        assert_eq!(err, "join selection length mismatch: probe=1 build=2");
    }

    #[test]
    fn mark_build_matches_returns_unique_new_marks() {
        let mut flags = BuildMatchFlags::new(5);
        let selection = JoinSelection {
            probe: vec![0, 1, 2, 3],
            build: vec![1, 3, 1, 4],
        };

        let marked = mark_build_matches(&mut flags, &selection).expect("mark build matches");
        assert_eq!(marked, 3);
        assert!(flags.is_matched(1));
        assert!(flags.is_matched(3));
        assert!(flags.is_matched(4));

        let marked_again =
            mark_build_matches(&mut flags, &selection).expect("mark duplicate build matches");
        assert_eq!(marked_again, 0);
    }

    #[test]
    fn mark_build_matches_rejects_mismatched_selection_lengths() {
        let mut flags = BuildMatchFlags::new(5);
        let selection = JoinSelection {
            probe: vec![0],
            build: vec![1, 2],
        };

        let err = mark_build_matches(&mut flags, &selection).expect_err("length mismatch");

        assert_eq!(err, "join selection length mismatch: probe=1 build=2");
    }

    #[test]
    fn all_match_one_requires_identity_probe_selection() {
        let identity = JoinSelection {
            probe: vec![0, 1, 2],
            build: vec![8, 7, 6],
        };
        assert!(is_all_match_one(&identity, 3));

        let shorter = JoinSelection {
            probe: vec![0, 1],
            build: vec![8, 7],
        };
        assert!(!is_all_match_one(&shorter, 3));

        let mismatched_lengths = JoinSelection {
            probe: vec![0, 1, 2],
            build: vec![8, 7],
        };
        assert!(!is_all_match_one(&mismatched_lengths, 3));

        let non_identity = JoinSelection {
            probe: vec![0, 2, 1],
            build: vec![8, 7, 6],
        };
        assert!(!is_all_match_one(&non_identity, 3));
    }
}
