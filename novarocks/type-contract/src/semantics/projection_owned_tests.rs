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
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let stop = self.stop.lock().unwrap().clone();
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((end, _)) = stop {
            assert!(at <= end, "callback after first refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((end, cause)) if end == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn reference(id: u32, key: SemanticParameterKey) -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(id),
        expected_key: key,
    }
}
fn source() -> SemanticParameters {
    SemanticParameters::try_new([
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::StatementStartUtc(
                crate::StatementStartUtc::try_new(-17, 0).unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(1),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("Asia/Kolkata").unwrap()),
        ),
        (
            SemanticParameterId::new(2),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
        ),
        (
            SemanticParameterId::new(3),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("+08:00").unwrap()),
        ),
        (
            SemanticParameterId::new(4),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
        ),
        (
            SemanticParameterId::new(5),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("+08:00").unwrap()),
        ),
        (
            SemanticParameterId::new(u32::MAX),
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("-07:30").unwrap()),
        ),
    ])
    .unwrap()
}
fn finish<T>(
    work: CompileCheckpoints<'_>,
    outcome: Result<T, SemanticParameterProjectionError>,
) -> Result<T, SemanticParameterProjectionError> {
    if matches!(outcome, Err(SemanticParameterProjectionError::Control(_))) {
        return outcome;
    }
    work.finish()
        .map_err(SemanticParameterProjectionError::Control)?;
    outcome
}
fn run(
    input: &SemanticParameters,
    refs: &[SemanticParameterRef],
    c: &Control,
) -> Result<SemanticParameters, SemanticParameterProjectionError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)
        .map_err(SemanticParameterProjectionError::Control)?;
    let outcome = input.project_in(
        refs.iter().copied(),
        &mut |_| Ok::<(), SemanticParameterProjectionError>(()),
        &mut work,
    );
    finish(work, outcome)
}

#[test]
fn immutable_projection_actual_unique_entry_loans_preserve_full_values_after_source_drop() {
    let c = Control::default();
    let output = {
        let input = source();
        let before = input.clone();
        let mut refs: Vec<_> = input
            .entries()
            .iter()
            .map(|(id, value)| reference(id.get(), value.key()))
            .collect();
        refs.push(reference(1, SemanticParameterKey::TimeZone));
        refs.push(reference(u32::MAX, SemanticParameterKey::TimeZone));
        let mut captured = Vec::new();
        let mut header_counts = Vec::new();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
        let outcome = input.project_in(
            refs,
            &mut |event| {
                match event {
                    SemanticParameterProjectionVisit::BeforeLookup {
                        source_definition_count,
                        output_definition_count,
                        ..
                    } => {
                        assert_eq!(source_definition_count, 7);
                        header_counts.push(output_definition_count);
                    }
                    SemanticParameterProjectionVisit::CapturedValue {
                        reference,
                        value,
                        is_new,
                        output_definition_count,
                    } => {
                        assert!(std::ptr::eq(value, input.require(reference).unwrap()));
                        captured.push((reference.id.get(), is_new, output_definition_count));
                    }
                }
                Ok::<(), SemanticParameterProjectionError>(())
            },
            &mut work,
        );
        let output = finish(work, outcome).unwrap();
        assert_eq!(output, before);
        assert_eq!(input, before);
        assert_eq!(header_counts, [0, 1, 2, 3, 4, 5, 6, 7, 7]);
        assert_eq!(
            captured,
            [
                (0, true, 0),
                (1, true, 1),
                (2, true, 2),
                (3, true, 3),
                (4, true, 4),
                (5, true, 5),
                (u32::MAX, true, 6),
                (1, false, 7),
                (u32::MAX, false, 7)
            ]
        );
        output
    };
    assert_eq!(
        output.get(SemanticParameterId::new(0)),
        Some(&SemanticParameterValue::StatementStartUtc(
            crate::StatementStartUtc::try_new(-17, 0).unwrap()
        ))
    );
    assert_eq!(
        output.get(SemanticParameterId::new(2)),
        Some(&SemanticParameterValue::TimeZone(
            crate::TimeZoneKey::parse("UTC").unwrap()
        ))
    );
    assert_eq!(
        output.get(SemanticParameterId::new(3)),
        Some(&SemanticParameterValue::TimeZone(
            crate::TimeZoneKey::parse("+08:00").unwrap()
        ))
    );
    assert_eq!(
        output.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::TimeZone(
            crate::TimeZoneKey::parse("-07:30").unwrap()
        ))
    );
    let Some(SemanticParameterValue::TimeZone(zone)) = output.get(SemanticParameterId::new(1))
    else {
        panic!("expected actual timezone");
    };
    assert_eq!(*zone, crate::TimeZoneKey::parse("Asia/Kolkata").unwrap());
    let trace = c.trace();
    assert_eq!(trace[0], (CompilePhase::LowerProgram, 0));
    assert!(
        trace
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::LowerProgram)
    );
    // Seven unique require/entry/clone/insert/reference operations, followed
    // by two duplicate require/entry/reference operations.
    assert_eq!(trace.iter().map(|(_, units)| *units).sum::<u32>(), 41);
}

#[test]
fn immutable_projection_success_and_ordinary_actual_callbacks_preserve_first_cause() {
    let input = source();
    let c = Control::default();
    let good = reference(1, SemanticParameterKey::TimeZone);
    for refs in [
        vec![good, reference(3, SemanticParameterKey::TimeZone), good],
        vec![good, reference(100, SemanticParameterKey::TimeZone)],
        vec![good, reference(1, SemanticParameterKey::StatementStartUtc)],
    ] {
        c.arm(None);
        let outcome = run(&input, &refs, &c);
        let trace = c.trace();
        if refs[1].id.get() == 3 {
            assert!(outcome.is_ok());
        } else if refs[1].id.get() == 100 {
            assert_eq!(
                outcome,
                Err(SemanticParameterProjectionError::Parameter(
                    SemanticParameterError::MissingId(SemanticParameterId::new(100))
                ))
            );
        } else {
            assert_eq!(
                outcome,
                Err(SemanticParameterProjectionError::Parameter(
                    SemanticParameterError::KeyMismatch(refs[1])
                ))
            );
        }
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in CAUSES {
                c.arm(Some((at, cause.clone())));
                assert_eq!(
                    run(&input, &refs, &c),
                    Err(SemanticParameterProjectionError::Control(cause))
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn immutable_projection_captured_clone_and_header_refusals_have_no_after_callback() {
    let input = source();
    let c = Control::default();
    let r = reference(1, SemanticParameterKey::TimeZone);
    for captured in [false, true] {
        for cause in CAUSES {
            c.arm(None);
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let mut before_refusal = vec![];
            let outcome = input.project_in(
                [r],
                &mut |event| {
                    let reject = if captured {
                        matches!(
                            event,
                            SemanticParameterProjectionVisit::CapturedValue { is_new: true, .. }
                        )
                    } else {
                        matches!(event, SemanticParameterProjectionVisit::BeforeLookup { .. })
                    };
                    if reject {
                        before_refusal = c.trace();
                        return Err(SemanticParameterProjectionError::Control(cause.clone()));
                    }
                    Ok(())
                },
                &mut work,
            );
            assert_eq!(
                finish(work, outcome),
                Err(SemanticParameterProjectionError::Control(cause))
            );
            assert_eq!(c.trace(), before_refusal);
            assert_eq!(
                before_refusal,
                if captured {
                    vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
                } else {
                    vec![(CompilePhase::Decode, 0)]
                },
                "captured request admission precedes lookup completed observations"
            );
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    enum Outer {
        Projection(SemanticParameterProjectionError),
        SourceModel,
    }
    impl From<SemanticParameterProjectionError> for Outer {
        fn from(e: SemanticParameterProjectionError) -> Self {
            Self::Projection(e)
        }
    }
    c.arm(None);
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let outcome = input.project_in([r], &mut |_| Err(Outer::SourceModel), &mut work);
    assert_eq!(outcome, Err(Outer::SourceModel));
    assert_eq!(c.trace(), [(CompilePhase::Decode, 0)]);
}

#[test]
fn immutable_projection_header_admission_precedes_pending255_without_own_scope() {
    let input = source();
    let c = Control::default();
    let r = reference(u32::MAX, SemanticParameterKey::TimeZone);
    for cause in CAUSES {
        c.arm(Some((1, cause)));
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let outcome = input.project_in(
            [r],
            &mut |event| match event {
                SemanticParameterProjectionVisit::BeforeLookup {
                    reference,
                    source_definition_count,
                    output_definition_count,
                } => {
                    assert_eq!(reference, r);
                    assert_eq!(source_definition_count, 7);
                    assert_eq!(output_definition_count, 0);
                    Err(SemanticParameterProjectionError::Control(
                        CompileControlError::ResourceExhausted,
                    ))
                }
                _ => panic!("header refusal must precede require or clone"),
            },
            &mut work,
        );
        assert_eq!(
            finish(work, outcome),
            Err(SemanticParameterProjectionError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(c.trace(), [(CompilePhase::Encode, 0)]);
    }
    c.arm(None);
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
    let outcome = input.project_in(
        [],
        &mut |_| -> Result<(), SemanticParameterProjectionError> {
            panic!("empty subset has no source operation")
        },
        &mut work,
    );
    assert!(finish(work, outcome).unwrap().entries().is_empty());
    assert_eq!(
        c.trace(),
        [(CompilePhase::Encode, 0), (CompilePhase::Encode, 0)],
        "empty subset borrows caller entry and caller footer only"
    );
}
