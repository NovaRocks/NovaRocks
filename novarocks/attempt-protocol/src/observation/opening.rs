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

//! Validate immutable opening facts before adopting any entity contribution.
use crate::error::TaskExecutionError;
use crate::input::{ObservationEvent, ReceivedObservation};
use std::time::Duration;

pub const COVERED_OPENING_BUDGET: Duration = Duration::from_secs(5);
pub const COVERED_LIVENESS_BUDGET: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpeningEligibility {
    Opened,
    OpeningFailure,
    /// This generation owes its one ordinary retry-budget settlement. The
    /// current frame is consumed completely without adopting its facts.
    TimedOut,
}

pub fn opening_eligibility(
    observation: &ReceivedObservation,
) -> Result<OpeningEligibility, TaskExecutionError> {
    let opening = observation.opening;
    if opening.started_at > observation.received_at
        || opening
            .opened_at
            .is_some_and(|opened| opened < opening.started_at || opened > observation.received_at)
    {
        return Err(TaskExecutionError::Schedule(
            "covered observation contains inconsistent actual opening times".into(),
        ));
    }
    let deadline = opening.started_at.saturating_add(COVERED_OPENING_BUDGET);
    match opening.opened_at {
        Some(opened) if opened > deadline => Ok(OpeningEligibility::TimedOut),
        Some(_) => Ok(OpeningEligibility::Opened),
        None if matches!(observation.event, ObservationEvent::Failed(_)) => {
            if observation.received_at > deadline {
                Ok(OpeningEligibility::TimedOut)
            } else {
                Ok(OpeningEligibility::OpeningFailure)
            }
        }
        None => Err(TaskExecutionError::Schedule(
            "covered stream entity arrived without an actual successful opening".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{ObservationFact, ObservationOpening};
    use novarocks_coordination_contract::MonotonicInstant;
    use std::num::NonZeroU64;
    fn at(seconds: u64) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_secs(seconds))
    }
    fn entity(opened_at: Option<MonotonicInstant>) -> ReceivedObservation {
        let task = crate::test_support::remote(1);
        ReceivedObservation {
            participation: task.participation(),
            generation: NonZeroU64::new(1).unwrap(),
            received_at: at(7),
            opening: ObservationOpening {
                started_at: at(0),
                opened_at,
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Unknown(task.identity()),
            },
        }
    }
    #[test]
    fn late_success_cannot_supply_entity_facts_before_a_late_phase_or_confirmation() {
        assert_eq!(
            opening_eligibility(&entity(Some(at(6)))).unwrap(),
            OpeningEligibility::TimedOut
        );
        assert_eq!(
            opening_eligibility(&entity(Some(at(4)))).unwrap(),
            OpeningEligibility::Opened
        );
        // A legal old generation remains eligible for its positive facts.
        let mut old = entity(Some(at(4)));
        old.generation = NonZeroU64::new(10).unwrap();
        old.received_at = at(100);
        assert_eq!(
            opening_eligibility(&old).unwrap(),
            OpeningEligibility::Opened
        );
    }
    #[test]
    fn exact_opening_boundary_and_missing_or_impossible_phase_are_distinct() {
        assert_eq!(
            opening_eligibility(&entity(Some(at(5)))).unwrap(),
            OpeningEligibility::Opened
        );
        assert!(opening_eligibility(&entity(None)).is_err());
        assert!(opening_eligibility(&entity(Some(at(8)))).is_err());
        let mut invalid = entity(Some(at(4)));
        invalid.opening.started_at = at(5);
        assert!(opening_eligibility(&invalid).is_err());
    }
    #[test]
    fn opening_failure_retains_its_actual_start_without_fabricating_success() {
        use crate::failure::{
            AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
        };
        use crate::input::{ObservationFailure, ObservationFailureKind};
        use novarocks_coordination_contract::AttemptFailureClass;
        let mut failure = entity(None);
        failure.event = ObservationEvent::Failed(ObservationFailure {
            kind: ObservationFailureKind::RecoverableTransport {
                observed_connection_loss: true,
            },
            cause: HostProjectedFailure {
                original: HostFailureRef(NonZeroU64::new(1).unwrap()),
                kind: HostFailureKind::NativeTransport,
                class: AttemptFailureClass::RecoverableInfrastructure,
                site: AttemptFailureSite::Attempt,
            },
        });
        failure.received_at = at(3);
        assert_eq!(
            opening_eligibility(&failure).unwrap(),
            OpeningEligibility::OpeningFailure
        );
        failure.received_at = at(7);
        assert_eq!(
            opening_eligibility(&failure).unwrap(),
            OpeningEligibility::TimedOut
        );
    }
}
