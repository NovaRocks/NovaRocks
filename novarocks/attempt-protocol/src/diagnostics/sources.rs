// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Constant-time Source selection at the diagnostic boundary. Each original
//! Participation installs at most one immutable RF reference. Later release
//! facts cannot enter an earlier frontier while its output is assembled.
use crate::{
    capacity::StepContext,
    error::TaskExecutionError,
    publication::{DiagnosticSources, RuntimeFilterDiagnostic, SplitDiagnosticObservation},
    start::PayloadRef,
};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::ParticipationRef;
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
};

#[derive(Clone, Copy, Debug, Default)]
struct ReleaseObservation {
    released: bool,
    contribution: Option<(usize, PayloadRef)>,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct DiagnosticSourceFrontier {
    selected_at: MonotonicInstant,
    contributions: usize,
    complete: bool,
    split: SplitDiagnosticObservation,
}
#[derive(Debug)]
pub(crate) struct DiagnosticSourcesState {
    participations: BTreeMap<ParticipationRef, ReleaseObservation>,
    released: usize,
    contributions: usize,
    split_source: Option<PayloadRef>,
    split: SplitDiagnosticObservation,
}
impl DiagnosticSourcesState {
    pub(crate) fn new(
        participations: impl IntoIterator<Item = ParticipationRef>,
        split_source: Option<PayloadRef>,
    ) -> Self {
        Self {
            participations: participations
                .into_iter()
                .map(|p| (p, ReleaseObservation::default()))
                .collect(),
            released: 0,
            contributions: 0,
            split_source,
            split: if split_source.is_some() {
                SplitDiagnosticObservation::Pending
            } else {
                SplitDiagnosticObservation::Absent
            },
        }
    }
    pub(crate) fn observe_release(
        &mut self,
        participation: ParticipationRef,
        released: bool,
        contribution: Option<PayloadRef>,
    ) -> Result<(), TaskExecutionError> {
        let row = self
            .participations
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if (row.released && !released)
            || (!released && contribution.is_some())
            || row
                .contribution
                .is_some_and(|(_, held)| contribution != Some(held))
        {
            return Err(TaskExecutionError::DomainReceipt("immutable release diagnostic observation regressed or changed its original reference".into()));
        }
        if released && !row.released {
            row.released = true;
            self.released += 1;
        }
        if let Some(reference) = contribution
            && row.contribution.is_none()
        {
            self.contributions += 1;
            row.contribution = Some((self.contributions, reference));
        }
        Ok(())
    }
    pub(crate) fn observe_split(
        &mut self,
        source: PayloadRef,
        observation: SplitDiagnosticObservation,
    ) -> Result<(), TaskExecutionError> {
        if self.split_source != Some(source) || observation == SplitDiagnosticObservation::Absent {
            return Err(TaskExecutionError::UnknownOperation);
        }
        // Available/Failed are immutable selected outcomes, even if the old
        // Weak observer later disappears. A Source disappearance cannot erase
        // already retained original profile data or a selected failure.
        match self.split {
            SplitDiagnosticObservation::Pending => self.split = observation,
            held if held == observation => {}
            SplitDiagnosticObservation::Available(_) | SplitDiagnosticObservation::Failed
                if observation == SplitDiagnosticObservation::SourceGone => {}
            _ => {
                return Err(TaskExecutionError::DomainReceipt(
                    "split profile outcome changed after its original selection".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn all_released(&self) -> bool {
        self.released == self.participations.len()
    }
    pub(crate) fn split_pending(&self) -> bool {
        self.split == SplitDiagnosticObservation::Pending
    }
    pub(crate) fn freeze(&self, selected_at: MonotonicInstant) -> DiagnosticSourceFrontier {
        DiagnosticSourceFrontier {
            selected_at,
            contributions: self.contributions,
            complete: self.all_released(),
            split: self.split,
        }
    }
}
#[derive(Debug)]
pub(crate) struct DiagnosticSourceAssembly {
    frontier: DiagnosticSourceFrontier,
    after: Option<ParticipationRef>,
    output: Vec<(novarocks_types::BackendProcessId, PayloadRef)>,
    done: bool,
}
impl DiagnosticSourceAssembly {
    pub(crate) fn new(frontier: DiagnosticSourceFrontier) -> Self {
        Self {
            frontier,
            after: None,
            output: Vec::with_capacity(frontier.contributions),
            done: false,
        }
    }
    pub(crate) fn complete(&self) -> bool {
        self.done
    }
    pub(crate) fn advance(
        &mut self,
        state: &DiagnosticSourcesState,
        context: &mut dyn StepContext,
    ) -> bool {
        if self.done || !context.charge(1) {
            return false;
        }
        let next = match self.after {
            Some(after) => state
                .participations
                .range((Excluded(after), Unbounded))
                .next(),
            None => state.participations.first_key_value(),
        };
        let Some((&participation, row)) = next else {
            self.done = true;
            return true;
        };
        self.after = Some(participation);
        if let Some((ordinal, reference)) = row.contribution
            && ordinal <= self.frontier.contributions
        {
            self.output
                .push((participation.backend_process_id(), reference));
        }
        true
    }
    pub(crate) fn finish(self) -> Result<DiagnosticSources, Self> {
        if !self.done {
            return Err(self);
        }
        assert_eq!(
            self.output.len(),
            self.frontier.contributions,
            "frozen original contribution frontier"
        );
        Ok(DiagnosticSources {
            selected_at: self.frontier.selected_at,
            runtime_filters: RuntimeFilterDiagnostic {
                contributions: self.output,
                complete: self.frontier.complete,
            },
            split: self.frontier.split,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{capacity::QueueStage, config::DispatchLane};
    use novarocks_types::BackendProcessId;
    use std::{num::NonZeroU64, time::Duration};

    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.0 {
                return false;
            }
            self.0 -= units;
            true
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("diagnostic selection grants no transport capacity")
        }
    }
    fn reference(value: u64) -> PayloadRef {
        PayloadRef(NonZeroU64::new(value).unwrap())
    }
    fn now(value: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(value))
    }
    fn parts() -> Vec<ParticipationRef> {
        (1..=3)
            .map(|n| crate::test_support::remote(n).participation())
            .collect()
    }
    fn finish(
        frontier: DiagnosticSourceFrontier,
        state: &DiagnosticSourcesState,
    ) -> DiagnosticSources {
        let mut assembly = DiagnosticSourceAssembly::new(frontier);
        assert!(!assembly.advance(state, &mut Turn(0)));
        let mut steps = 0;
        while !assembly.complete() {
            assert!(assembly.advance(state, &mut Turn(1)));
            steps += 1;
            assert!(steps <= state.participations.len() + 1);
        }
        assert_eq!(steps, state.participations.len() + 1);
        assembly.finish().unwrap()
    }
    #[test]
    fn first_cut_excludes_later_release_and_split_even_during_single_unit_assembly() {
        let p = parts();
        let mut state = DiagnosticSourcesState::new(p.clone(), Some(reference(99)));
        state
            .observe_release(p[2], true, Some(reference(3)))
            .unwrap();
        let frontier = state.freeze(now(5));
        let mut assembly = DiagnosticSourceAssembly::new(frontier);
        assert!(assembly.advance(&state, &mut Turn(1)));
        state
            .observe_release(p[0], true, Some(reference(1)))
            .unwrap();
        state.observe_release(p[1], true, None).unwrap();
        state
            .observe_split(
                reference(99),
                SplitDiagnosticObservation::Available(reference(100)),
            )
            .unwrap();
        while !assembly.complete() {
            assert!(assembly.advance(&state, &mut Turn(1)));
        }
        let cut = assembly.finish().unwrap();
        assert_eq!(cut.selected_at, now(5));
        assert_eq!(
            cut.runtime_filters.contributions,
            vec![(p[2].backend_process_id(), reference(3))]
        );
        assert!(!cut.runtime_filters.complete);
        assert_eq!(cut.split, SplitDiagnosticObservation::Pending);
        let later = finish(state.freeze(now(6)), &state);
        assert!(later.runtime_filters.complete);
        assert_eq!(
            later.runtime_filters.contributions,
            vec![
                (p[0].backend_process_id(), reference(1)),
                (p[2].backend_process_id(), reference(3))
            ]
        );
        assert_eq!(
            later.split,
            SplitDiagnosticObservation::Available(reference(100))
        );
    }
    #[test]
    fn complete_empty_release_and_absent_split_are_positive_facts() {
        let p = parts();
        let mut state = DiagnosticSourcesState::new(p.clone(), None);
        for part in p {
            state.observe_release(part, true, None).unwrap();
        }
        let cut = finish(state.freeze(now(2)), &state);
        assert!(cut.runtime_filters.complete);
        assert!(cut.runtime_filters.contributions.is_empty());
        assert_eq!(cut.split, SplitDiagnosticObservation::Absent);
        assert!(!state.split_pending());
        assert!(
            state
                .observe_split(reference(9), SplitDiagnosticObservation::Failed)
                .is_err()
        );
    }
    #[test]
    fn original_split_outcome_survives_source_disappearance_without_becoming_actual_exit() {
        for outcome in [
            SplitDiagnosticObservation::Available(reference(8)),
            SplitDiagnosticObservation::Failed,
        ] {
            let mut state = DiagnosticSourcesState::new(parts(), Some(reference(7)));
            state.observe_split(reference(7), outcome).unwrap();
            state
                .observe_split(reference(7), SplitDiagnosticObservation::SourceGone)
                .unwrap();
            assert_eq!(finish(state.freeze(now(1)), &state).split, outcome);
            assert!(
                state
                    .observe_split(reference(7), SplitDiagnosticObservation::Pending)
                    .is_err()
            );
            assert!(
                state
                    .observe_split(
                        reference(7),
                        SplitDiagnosticObservation::Available(reference(10))
                    )
                    .is_err()
            );
        }
        let mut state = DiagnosticSourcesState::new(parts(), Some(reference(7)));
        state
            .observe_split(reference(7), SplitDiagnosticObservation::SourceGone)
            .unwrap();
        assert_eq!(
            finish(state.freeze(now(1)), &state).split,
            SplitDiagnosticObservation::SourceGone
        );
        assert!(
            state
                .observe_split(
                    reference(7),
                    SplitDiagnosticObservation::Available(reference(8))
                )
                .is_err()
        );
    }
    #[test]
    fn foreign_sources_and_regressing_or_changed_release_refs_are_rejected() {
        let p = parts();
        let mut state = DiagnosticSourcesState::new(p.clone(), Some(reference(7)));
        assert!(
            state
                .observe_release(p[0], false, Some(reference(2)))
                .is_err()
        );
        state
            .observe_release(p[0], true, Some(reference(2)))
            .unwrap();
        for (released, contribution) in [
            (false, Some(reference(2))),
            (true, None),
            (true, Some(reference(3))),
        ] {
            assert!(state.observe_release(p[0], released, contribution).is_err());
        }
        assert!(
            state
                .observe_release(crate::test_support::remote(4).participation(), true, None)
                .is_err()
        );
        assert!(
            state
                .observe_split(reference(8), SplitDiagnosticObservation::Failed)
                .is_err()
        );
        assert!(
            state
                .observe_split(reference(7), SplitDiagnosticObservation::Absent)
                .is_err()
        );
        assert_eq!(
            finish(state.freeze(now(1)), &state)
                .runtime_filters
                .contributions,
            vec![(p[0].backend_process_id(), reference(2))]
        );
    }
}
