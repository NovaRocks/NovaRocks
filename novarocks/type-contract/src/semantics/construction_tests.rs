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
use std::cell::Cell;
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        trace.push((phase, units));
        if let Some((at, cause)) = self.refusal.clone()
            && at == ordinal
        {
            return Err(cause);
        }
        Ok(())
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn entry(id: u32, value: SemanticParameterValue) -> (SemanticParameterId, SemanticParameterValue) {
    (SemanticParameterId::new(id), value)
}
fn construct(
    entries: impl IntoIterator<Item = (SemanticParameterId, SemanticParameterValue)>,
    control: &Control,
) -> Result<SemanticParameters, SemanticParameterProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)
        .map_err(SemanticParameterProjectionError::Control)?;
    let result = SemanticParameters::try_new_observed(entries, &mut work);
    if matches!(&result, Err(SemanticParameterProjectionError::Control(_))) {
        return result;
    }
    work.finish()
        .map_err(SemanticParameterProjectionError::Control)?;
    result
}

#[test]
fn legacy_and_observed_constructor_preserve_exact_error_precedence() {
    use SemanticParameterValue::*;
    let id = SemanticParameterId::new(0);
    let mut full: Vec<_> = (0..MAX_SEMANTIC_PARAMETERS as u32)
        .map(|id| entry(id, AllowThrowException(false)))
        .collect();
    let mut duplicate_full = full.clone();
    duplicate_full.push(entry(
        0,
        TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
    ));
    full.push(entry(
        u32::MAX,
        TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
    ));
    let cases = [
        (
            vec![
                entry(0, AllowThrowException(true)),
                entry(0, TimeZone(crate::TimeZoneKey::parse("UTC").unwrap())),
            ],
            SemanticParameterError::DuplicateId(id),
        ),
        (duplicate_full, SemanticParameterError::DuplicateId(id)),
        (full, SemanticParameterError::TooManyParameters),
        (
            vec![
                entry(
                    0,
                    StatementStartUtc(crate::StatementStartUtc::try_new(-1, 0).unwrap()),
                ),
                entry(
                    u32::MAX,
                    StatementStartUtc(crate::StatementStartUtc::try_new(2, 0).unwrap()),
                ),
            ],
            SemanticParameterError::DuplicateStatementStart,
        ),
        (
            vec![
                entry(
                    0,
                    StatementStartUtc(crate::StatementStartUtc::try_new(-1, 0).unwrap()),
                ),
                entry(
                    0,
                    StatementStartUtc(crate::StatementStartUtc::try_new(2, 0).unwrap()),
                ),
            ],
            SemanticParameterError::DuplicateId(id),
        ),
    ];
    for (entries, expected) in cases {
        assert_eq!(
            SemanticParameters::try_new(entries.clone()),
            Err(expected.clone())
        );
        let c = Control::default();
        assert_eq!(
            construct(entries, &c),
            Err(SemanticParameterProjectionError::Parameter(expected))
        );
        assert_eq!(c.trace().last().unwrap().0, CompilePhase::Decode);
    }
}

#[test]
fn scoped_raw_values_sparse_ids_and_canonical_zone_identity_are_retained() {
    use SemanticParameterValue::*;
    let original = crate::TimeZoneKey::parse("+08:00").unwrap();
    let entries = vec![
        entry(
            u32::MAX,
            SemanticParameterValue::StatementStartUtc(
                crate::StatementStartUtc::try_new(i64::MIN, 0).unwrap(),
            ),
        ),
        entry(
            0,
            StatementStartUtc(crate::StatementStartUtc::try_new(i64::MAX, 0).unwrap()),
        ),
        entry(15, TimeZone(original)),
        entry(9, AllowThrowException(false)),
        entry(8, AllowThrowException(true)),
        entry(7, AllowThrowException(false)),
        entry(16, TimeZone(crate::TimeZoneKey::parse("UTC").unwrap())),
        entry(19, AllowThrowException(true)),
    ];
    let expected = SemanticParameters::try_new(entries.clone()).unwrap();
    let c = Control::default();
    let output = construct(entries, &c).unwrap();
    assert_eq!(output, expected);
    let Some(TimeZone(zone)) = output.get(SemanticParameterId::new(15)) else {
        panic!("missing zone")
    };
    assert_eq!(*zone, original);
    assert_eq!(zone.name(), "+08:00");
    assert_eq!(
        output.get(SemanticParameterId::new(19)),
        Some(&AllowThrowException(true))
    );
    let trace = c.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
    // The constructor already flushed terminating next(); caller still owns
    // and observes its actual zero ordinary/success tail.
    assert_eq!(trace.last(), Some(&(CompilePhase::Decode, 0)));
}

#[test]
fn every_actual_success_error_and_empty_prefix_preserves_control() {
    for entries in [
        vec![],
        vec![entry(
            0,
            SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
        )],
        vec![
            entry(
                0,
                SemanticParameterValue::StatementStartUtc(
                    crate::StatementStartUtc::try_new(1, 0).unwrap(),
                ),
            ),
            entry(
                1,
                SemanticParameterValue::StatementStartUtc(
                    crate::StatementStartUtc::try_new(2, 0).unwrap(),
                ),
            ),
        ],
    ] {
        let baseline = Control::default();
        let result = construct(entries.clone(), &baseline);
        assert_eq!(
            result,
            SemanticParameters::try_new(entries.clone())
                .map_err(SemanticParameterProjectionError::Parameter)
        );
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(
                    construct(entries.clone(), &c),
                    Err(SemanticParameterProjectionError::Control(cause))
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum LazyError {
    Owner(SemanticParameterProjectionError),
    MissingValue,
}
fn lazy_case(
    c: &Control,
    pulls: &Cell<usize>,
    boxes: &Cell<usize>,
    failed_next_post: &Cell<usize>,
) -> Result<SemanticParameters, LazyError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)
        .map_err(|e| LazyError::Owner(SemanticParameterProjectionError::Control(e)))?;
    let projection_failed = Cell::new(false);
    let entries = std::iter::from_fn(|| {
        let pull = pulls.get();
        pulls.set(pull + 1);
        if pull == 0 {
            // Actual lazy owned projection, bounded by the receiver's prior
            // resource gate; this constructor must not clone it again.
            boxes.set(boxes.get() + 1);
            Some(entry(
                0,
                SemanticParameterValue::TimeZone(crate::TimeZoneKey::parse("UTC").unwrap()),
            ))
        } else {
            projection_failed.set(true);
            failed_next_post.set(c.trace().len());
            None
        }
    });
    let checked = SemanticParameters::try_new_observed(entries, &mut work);
    // Original constructor refusal takes precedence over the lazy codec
    // latch, including a refusal at post-next after the latch was populated.
    if matches!(&checked, Err(SemanticParameterProjectionError::Control(_))) {
        return checked.map_err(LazyError::Owner);
    }
    let result = if projection_failed.get() {
        Err(LazyError::MissingValue)
    } else {
        checked.map_err(LazyError::Owner)
    };
    work.finish()
        .map_err(|e| LazyError::Owner(SemanticParameterProjectionError::Control(e)))?;
    result
}
#[test]
fn actual_lazy_pull_is_bracketed_and_invalid_prefix_never_publishes() {
    let baseline = Control::default();
    let pulls = Cell::new(0);
    let boxes = Cell::new(0);
    let boundary = Cell::new(usize::MAX);
    assert_eq!(
        lazy_case(&baseline, &pulls, &boxes, &boundary),
        Err(LazyError::MissingValue)
    );
    assert_eq!(pulls.get(), 2);
    assert_eq!(boxes.get(), 1);
    let trace = baseline.trace();
    assert_eq!(trace[boundary.get()], (CompilePhase::Decode, 1));
    assert_eq!(trace.last(), Some(&(CompilePhase::Decode, 0)));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            let pulls = Cell::new(0);
            let boxes = Cell::new(0);
            let boundary = Cell::new(usize::MAX);
            assert_eq!(
                lazy_case(&c, &pulls, &boxes, &boundary),
                Err(LazyError::Owner(SemanticParameterProjectionError::Control(
                    cause
                )))
            );
            assert_eq!(c.trace(), trace[..=at]);
            assert!(pulls.get() <= 2);
            assert!(boxes.get() <= 1);
        }
    }
}
