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

use std::collections::BTreeMap;

use novarocks::runtime_filter_transition::model::contract::CoverageWitnessId;
use novarocks::runtime_filter_transition::model::coverage::Coverage;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoverageProgress {
    Pending,
    Satisfied,
    Impossible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WitnessProgress {
    Pending,
    Satisfied,
    Impossible,
}

impl WitnessProgress {
    pub(crate) fn advance(&mut self, next: Self) -> bool {
        if *self != Self::Pending || next == Self::Pending {
            return false;
        }
        *self = next;
        true
    }
}

pub(crate) fn evaluate(
    coverage: &Coverage,
    witnesses: &BTreeMap<CoverageWitnessId, WitnessProgress>,
) -> CoverageProgress {
    match coverage {
        Coverage::Leaf(witness_id) => match witnesses
            .get(witness_id)
            .copied()
            .expect("validated coverage witness must have runtime progress")
        {
            WitnessProgress::Pending => CoverageProgress::Pending,
            WitnessProgress::Satisfied => CoverageProgress::Satisfied,
            WitnessProgress::Impossible => CoverageProgress::Impossible,
        },
        Coverage::AllOf(children) => {
            let mut all_satisfied = true;
            for child in children {
                match evaluate(child, witnesses) {
                    CoverageProgress::Impossible => return CoverageProgress::Impossible,
                    CoverageProgress::Pending => all_satisfied = false,
                    CoverageProgress::Satisfied => {}
                }
            }
            if all_satisfied {
                CoverageProgress::Satisfied
            } else {
                CoverageProgress::Pending
            }
        }
        Coverage::AnyOf(children) => {
            let mut all_impossible = true;
            for child in children {
                match evaluate(child, witnesses) {
                    CoverageProgress::Satisfied => return CoverageProgress::Satisfied,
                    CoverageProgress::Pending => all_impossible = false,
                    CoverageProgress::Impossible => {}
                }
            }
            if all_impossible {
                CoverageProgress::Impossible
            } else {
                CoverageProgress::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use novarocks::runtime_filter_transition::model::contract::CoverageWitnessId;
    use novarocks::runtime_filter_transition::model::coverage::Coverage;

    use super::{CoverageProgress, WitnessProgress, evaluate};

    fn leaf(id: u32) -> Coverage {
        Coverage::Leaf(CoverageWitnessId::new(id))
    }

    #[test]
    fn nested_all_of_any_of_matches_every_witness_subset() {
        let coverage = Coverage::AllOf(vec![Coverage::AnyOf(vec![leaf(1), leaf(2)]), leaf(3)]);
        for bits in 0..8 {
            let states = (1..=3)
                .map(|id| {
                    let state = if bits & (1 << (id - 1)) != 0 {
                        WitnessProgress::Satisfied
                    } else {
                        WitnessProgress::Pending
                    };
                    (CoverageWitnessId::new(id), state)
                })
                .collect::<BTreeMap<_, _>>();
            let expected = if bits & 0b100 != 0 && bits & 0b011 != 0 {
                CoverageProgress::Satisfied
            } else {
                CoverageProgress::Pending
            };
            assert_eq!(evaluate(&coverage, &states), expected, "bits={bits:03b}");
        }
    }

    #[test]
    fn any_of_first_valid_witness_satisfies_coverage() {
        let states = BTreeMap::from([
            (CoverageWitnessId::new(1), WitnessProgress::Impossible),
            (CoverageWitnessId::new(2), WitnessProgress::Satisfied),
            (CoverageWitnessId::new(3), WitnessProgress::Pending),
        ]);
        assert_eq!(
            evaluate(&Coverage::AnyOf(vec![leaf(1), leaf(2), leaf(3)]), &states),
            CoverageProgress::Satisfied
        );
    }

    #[test]
    fn all_of_waits_for_every_disjoint_witness() {
        let mut states = BTreeMap::from([
            (CoverageWitnessId::new(1), WitnessProgress::Satisfied),
            (CoverageWitnessId::new(2), WitnessProgress::Pending),
        ]);
        let coverage = Coverage::AllOf(vec![leaf(1), leaf(2)]);
        assert_eq!(evaluate(&coverage, &states), CoverageProgress::Pending);
        states.insert(CoverageWitnessId::new(2), WitnessProgress::Satisfied);
        assert_eq!(evaluate(&coverage, &states), CoverageProgress::Satisfied);
    }

    #[test]
    fn coverage_impossible_is_three_valued() {
        let all = Coverage::AllOf(vec![leaf(1), leaf(2)]);
        let any = Coverage::AnyOf(vec![leaf(1), leaf(2)]);
        let states = BTreeMap::from([
            (CoverageWitnessId::new(1), WitnessProgress::Impossible),
            (CoverageWitnessId::new(2), WitnessProgress::Pending),
        ]);
        assert_eq!(evaluate(&all, &states), CoverageProgress::Impossible);
        assert_eq!(evaluate(&any, &states), CoverageProgress::Pending);
        let states = BTreeMap::from([
            (CoverageWitnessId::new(1), WitnessProgress::Impossible),
            (CoverageWitnessId::new(2), WitnessProgress::Impossible),
        ]);
        assert_eq!(evaluate(&any, &states), CoverageProgress::Impossible);
    }

    #[test]
    fn coverage_result_is_invariant_to_witness_arrival_order() {
        let coverage = Coverage::AllOf(vec![Coverage::AnyOf(vec![leaf(1), leaf(2)]), leaf(3)]);
        let left = BTreeMap::from([
            (CoverageWitnessId::new(1), WitnessProgress::Satisfied),
            (CoverageWitnessId::new(2), WitnessProgress::Pending),
            (CoverageWitnessId::new(3), WitnessProgress::Satisfied),
        ]);
        let right = left
            .iter()
            .rev()
            .map(|(id, status)| (*id, *status))
            .collect();
        assert_eq!(evaluate(&coverage, &left), evaluate(&coverage, &right));
    }

    #[test]
    fn witness_terminal_progress_never_reverses() {
        let mut progress = WitnessProgress::Pending;
        assert!(progress.advance(WitnessProgress::Satisfied));
        assert!(!progress.advance(WitnessProgress::Impossible));
        assert_eq!(progress, WitnessProgress::Satisfied);
    }

    #[test]
    #[should_panic(expected = "validated coverage witness must have runtime progress")]
    fn missing_validated_witness_is_not_silently_pending() {
        evaluate(&leaf(1), &BTreeMap::new());
    }
}
