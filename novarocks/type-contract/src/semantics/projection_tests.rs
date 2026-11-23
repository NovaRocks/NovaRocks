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

use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Control {
    checkpoints: Mutex<Vec<(CompilePhase, u32)>>,
    entry_failure: Option<CompileControlError>,
    positive_failure: Option<(u32, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work_units: u32) -> Result<(), CompileControlError> {
        let mut checkpoints = self.checkpoints.lock().unwrap();
        checkpoints.push((phase, work_units));
        if let Some(error) = self.entry_failure.clone() {
            return Err(error);
        }
        if let Some((limit, error)) = self.positive_failure.clone()
            && checkpoints.iter().map(|(_, units)| units).sum::<u32>() >= limit
        {
            return Err(error);
        }
        Ok(())
    }
}
fn reference(id: u32, expected_key: SemanticParameterKey) -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(id),
        expected_key,
    }
}
fn table() -> SemanticParameters {
    SemanticParameters::try_new([
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::StatementStartUtc(
                crate::StatementStartUtc::try_new(-17, 0).unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(1),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
        ),
        (
            SemanticParameterId::new(2),
            SemanticParameterValue::AllowThrowException(false),
        ),
        (
            SemanticParameterId::new(3),
            SemanticParameterValue::GroupConcatLegacy(true),
        ),
        (
            SemanticParameterId::new(4),
            SemanticParameterValue::GroupConcatLegacy(false),
        ),
        (
            SemanticParameterId::new(u32::MAX),
            SemanticParameterValue::GroupConcatMaxLen(-23),
        ),
    ])
    .unwrap()
}

#[test]
fn exact_values_sparse_ids_and_input_are_preserved() {
    let input = table();
    let before = input.clone();
    let control = Control::default();
    let refs = input
        .entries()
        .iter()
        .map(|(id, value)| reference(id.get(), value.key()));
    let output = input
        .project_observed(refs, CompilePhase::Validate, &control)
        .unwrap();
    assert_eq!(output, before);
    assert_eq!(input, before);
    assert_eq!(
        output.get(SemanticParameterId::new(0)),
        Some(&SemanticParameterValue::StatementStartUtc(
            crate::StatementStartUtc::try_new(-17, 0).unwrap()
        ))
    );
    assert_eq!(
        output.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::GroupConcatMaxLen(-23))
    );
    assert_eq!(
        control.checkpoints.into_inner().unwrap(),
        vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 6)]
    );
}

#[test]
fn every_repeated_reference_is_checked_and_one_definition_is_retained() {
    let input = SemanticParameters::try_new([
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
        ),
        (
            SemanticParameterId::new(u32::MAX),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("+08:00").unwrap()),
        ),
    ])
    .unwrap();
    let before = input.clone();
    let control = Control::default();
    let refs = std::iter::repeat_n(reference(0, SemanticParameterKey::TimeZone), 1025)
        .chain([reference(u32::MAX, SemanticParameterKey::TimeZone)]);
    let output = input
        .project_observed(refs, CompilePhase::LowerProgram, &control)
        .unwrap();
    assert_eq!(output, before);
    assert_eq!(output.entries().len(), 2);
    assert_eq!(input, before);
    let Some(SemanticParameterValue::TimeZone(input_zone)) = input.get(SemanticParameterId::new(0))
    else {
        panic!("expected time zone")
    };
    let Some(SemanticParameterValue::TimeZone(output_zone)) =
        output.get(SemanticParameterId::new(0))
    else {
        panic!("expected time zone")
    };
    assert_eq!(input_zone, output_zone);
    assert_eq!(
        control.checkpoints.into_inner().unwrap(),
        vec![
            (CompilePhase::LowerProgram, 0),
            (CompilePhase::LowerProgram, 256),
            (CompilePhase::LowerProgram, 256),
            (CompilePhase::LowerProgram, 256),
            (CompilePhase::LowerProgram, 256),
            (CompilePhase::LowerProgram, 2),
        ]
    );
    // A repeated ID never skips the expected-key check.
    let wrong = reference(0, SemanticParameterKey::GroupConcatMaxLen);
    assert_eq!(
        input.project_observed(
            [reference(0, SemanticParameterKey::TimeZone), wrong,],
            CompilePhase::Validate,
            &Control::default()
        ),
        Err(SemanticParameterProjectionError::Parameter(
            SemanticParameterError::KeyMismatch(wrong)
        ))
    );
    assert_eq!(input, before);
}

#[test]
fn missing_and_wrong_key_remain_parameter_errors() {
    let input = table();
    let before = input.clone();
    let missing = reference(99, SemanticParameterKey::TimeZone);
    assert_eq!(
        input.project_observed([missing], CompilePhase::Validate, &Control::default()),
        Err(SemanticParameterProjectionError::Parameter(
            SemanticParameterError::MissingId(missing.id)
        ))
    );
    let wrong = reference(1, SemanticParameterKey::AllowThrowException);
    assert_eq!(
        input.project_observed([wrong], CompilePhase::Validate, &Control::default()),
        Err(SemanticParameterProjectionError::Parameter(
            SemanticParameterError::KeyMismatch(wrong)
        ))
    );
    assert_eq!(input, before);
}

#[test]
fn empty_projection_observes_entry_and_has_no_work() {
    let input = table();
    let control = Control::default();
    assert_eq!(
        input
            .project_observed([], CompilePhase::ProviderValidation, &control)
            .unwrap(),
        SemanticParameters::default()
    );
    let checkpoints = control.checkpoints.into_inner().unwrap();
    assert_eq!(
        checkpoints.first(),
        Some(&(CompilePhase::ProviderValidation, 0))
    );
    assert!(
        checkpoints
            .iter()
            .all(|&(phase, units)| phase == CompilePhase::ProviderValidation && units == 0)
    );
}

#[test]
fn typed_control_failures_precede_input_iteration_and_stop_at_the_quantum() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let input = table();
        let before = input.clone();
        let control = Control {
            entry_failure: Some(failure.clone()),
            ..Default::default()
        };
        let refs = std::iter::from_fn(|| -> Option<SemanticParameterRef> {
            panic!("entry failure must precede iteration")
        });
        assert_eq!(
            input.project_observed(refs, CompilePhase::Validate, &control),
            Err(SemanticParameterProjectionError::Control(failure.clone()))
        );
        assert_eq!(
            control.checkpoints.into_inner().unwrap(),
            vec![(CompilePhase::Validate, 0)]
        );
        let control = Control {
            entry_failure: Some(failure.clone()),
            ..Default::default()
        };
        assert_eq!(
            input.project_observed([], CompilePhase::Validate, &control),
            Err(SemanticParameterProjectionError::Control(failure.clone()))
        );
        assert_eq!(
            control.checkpoints.into_inner().unwrap(),
            vec![(CompilePhase::Validate, 0)]
        );
        let control = Control {
            positive_failure: Some((256, failure.clone())),
            ..Default::default()
        };
        let consumed = AtomicUsize::new(0);
        let refs =
            std::iter::repeat_n(reference(1, SemanticParameterKey::TimeZone), 1000).inspect(|_| {
                consumed.fetch_add(1, Ordering::Relaxed);
            });
        assert_eq!(
            input.project_observed(refs, CompilePhase::Validate, &control),
            Err(SemanticParameterProjectionError::Control(failure))
        );
        assert_eq!(consumed.load(Ordering::Relaxed), 256);
        assert_eq!(
            control.checkpoints.into_inner().unwrap(),
            vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 256)]
        );
        assert_eq!(input, before);
    }
}

#[test]
fn intermediate_failure_accounts_all_completed_references_without_overreading() {
    let input = table();
    let control = Control {
        positive_failure: Some((512, CompileControlError::ResourceExhausted)),
        ..Default::default()
    };
    let consumed = AtomicUsize::new(0);
    let refs = std::iter::repeat_n(
        reference(u32::MAX, SemanticParameterKey::GroupConcatMaxLen),
        1025,
    )
    .inspect(|_| {
        consumed.fetch_add(1, Ordering::Relaxed);
    });
    assert_eq!(
        input.project_observed(refs, CompilePhase::Encode, &control),
        Err(SemanticParameterProjectionError::Control(
            CompileControlError::ResourceExhausted
        ))
    );
    let checkpoints = control.checkpoints.into_inner().unwrap();
    assert_eq!(consumed.load(Ordering::Relaxed), 512);
    assert_eq!(checkpoints.iter().map(|(_, units)| units).sum::<u32>(), 512);
    assert!(
        checkpoints
            .iter()
            .all(|&(phase, units)| phase == CompilePhase::Encode
                && units <= crate::MAX_UNOBSERVED_COMPILE_WORK)
    );
}

#[test]
fn maximum_valid_definition_table_projects_without_a_second_validation_pass() {
    let input = SemanticParameters::try_new((0..MAX_SEMANTIC_PARAMETERS).map(|id| {
        (
            SemanticParameterId::new(id as u32),
            SemanticParameterValue::GroupConcatMaxLen(id as i64),
        )
    }))
    .unwrap();
    let before = input.clone();
    let control = Control::default();
    let output = input
        .project_observed(
            input
                .entries()
                .keys()
                .map(|id| reference(id.get(), SemanticParameterKey::GroupConcatMaxLen)),
            CompilePhase::Decode,
            &control,
        )
        .unwrap();
    assert_eq!(output, before);
    assert_eq!(input, before);
    let checkpoints = control.checkpoints.into_inner().unwrap();
    assert_eq!(
        checkpoints
            .iter()
            .map(|(_, units)| *units as usize)
            .sum::<usize>(),
        MAX_SEMANTIC_PARAMETERS
    );
    assert!(
        checkpoints
            .iter()
            .all(|&(phase, units)| phase == CompilePhase::Decode
                && units <= crate::MAX_UNOBSERVED_COMPILE_WORK)
    );
}
