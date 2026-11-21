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
use novarocks_type_contract::{CompileControlError, SemanticParameterKey};
use std::sync::Mutex;

const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn record(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        self.record(phase, units)
    }
}
#[derive(Default)]
struct EncodeControl(Control);
impl PureCompileControl for EncodeControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        self.0.record(phase, units)
    }
}

fn limits() -> ParameterProjectionLimits {
    ParameterProjectionLimits {
        max_parameters: MAX_SEMANTIC_PARAMETERS,
        max_timezone_request_bytes: 1 << 20,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: 1 << 30,
    }
}
fn entry(id: u32, value: wire::semantic_parameter::Value) -> wire::SemanticParameter {
    wire::SemanticParameter {
        id,
        value: Some(value),
    }
}
fn source() -> wire::SemanticParameters {
    use wire::semantic_parameter::Value::*;
    wire::SemanticParameters {
        entries: vec![
            entry(u32::MAX, StatementStartUtcMicros(i64::MIN)),
            entry(0, TimeZone("雪Z".into())),
            entry(2, AllowThrowException(true)),
            entry(4, GroupConcatLegacy(true)),
            entry(5, GroupConcatMaxLen(i64::MIN)),
            entry(6, AllowThrowException(false)),
        ],
    }
}
#[test]
fn bounded_parameters_keep_all_raw_variants_scoped_keys_and_sparse_ids() {
    let input = source();
    let (table, facts) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(table.entries().len(), 6);
    assert_eq!(
        table.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::StatementStartUtc(i64::MIN))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(0)),
        Some(&SemanticParameterValue::TimeZone("雪Z".into()))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(2)),
        Some(&SemanticParameterValue::AllowThrowException(true))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(4)),
        Some(&SemanticParameterValue::GroupConcatLegacy(true))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(5)),
        Some(&SemanticParameterValue::GroupConcatMaxLen(i64::MIN))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(6)),
        Some(&SemanticParameterValue::AllowThrowException(false))
    );
    assert_eq!(
        table.get(SemanticParameterId::new(6)).unwrap().key(),
        SemanticParameterKey::AllowThrowException
    );
    // This is a cumulative node-request bound, not n actual allocated nodes.
    assert_eq!(facts.parameter_count, 6);
    assert_eq!(facts.timezone_request_bytes_upper_bound, 4);
    assert_eq!(facts.allocation_requests_upper_bound, 7);
    let control = Control::default();
    let prepared = prepare_semantic_parameters_decode(&input, SOURCE, limits(), &control).unwrap();
    assert_eq!(*prepared.facts(), facts);
    let (prepared_table, prepared_facts) = prepared.emit().unwrap();
    assert_eq!(prepared_table, table);
    assert_eq!(prepared_facts, facts);
}
#[test]
fn bounded_parameters_all_six_resource_axes_and_actual_source_capacity_are_admitted() {
    let input = source();
    let (_, facts) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    let exact = ParameterProjectionLimits {
        max_parameters: facts.parameter_count,
        max_timezone_request_bytes: facts.timezone_request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(decode_semantic_parameters(&input, SOURCE, exact, &Control::default()).is_ok());
    for axis in 0..6 {
        let mut cap = exact;
        match axis {
            0 => cap.max_parameters -= 1,
            1 => cap.max_timezone_request_bytes -= 1,
            2 => cap.max_allocation_requests -= 1,
            3 => cap.max_allocation_request_bytes -= 1,
            4 => cap.max_coexisting_source_and_request_bytes -= 1,
            5 => cap.max_work -= 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            decode_semantic_parameters(&input, SOURCE, cap, &Control::default()),
            Err(Error::InvalidShape(_))
        ));
    }
    let mut excess = source();
    if let Some(wire::semantic_parameter::Value::TimeZone(zone)) = &mut excess.entries[1].value {
        zone.reserve_exact(SOURCE * 2);
    }
    assert!(matches!(
        prepare_semantic_parameters_decode(&excess, SOURCE, limits(), &Control::default()),
        Err(Error::InvalidShape(_))
    ));
    let empty = wire::SemanticParameters {
        entries: Vec::with_capacity(100),
    };
    assert!(matches!(
        prepare_semantic_parameters_decode(
            &empty,
            size_of::<wire::SemanticParameters>(),
            limits(),
            &Control::default()
        ),
        Err(Error::InvalidShape(_))
    ));
    let wide = wire::SemanticParameters {
        entries: (0..320)
            .map(|id| {
                entry(
                    id,
                    wire::semantic_parameter::Value::AllowThrowException(false),
                )
            })
            .collect(),
    };
    let control = Control::default();
    assert!(matches!(
        prepare_semantic_parameters_decode(
            &wide,
            SOURCE,
            ParameterProjectionLimits {
                max_work: 256,
                ..limits()
            },
            &control
        ),
        Err(Error::InvalidShape(_))
    ));
    assert!(
        !control
            .trace
            .lock()
            .unwrap()
            .iter()
            .any(|(_, units)| *units == 256)
    );
}
#[test]
fn bounded_parameters_preserve_constructor_and_lazy_decoder_error_order() {
    use wire::semantic_parameter::Value::*;
    let cases = [
        (
            vec![
                entry(0, AllowThrowException(false)),
                entry(0, TimeZone("".into())),
                wire::SemanticParameter { id: 9, value: None },
            ],
            SemanticParameterError::DuplicateId(SemanticParameterId::new(0)),
        ),
        (
            vec![
                entry(0, StatementStartUtcMicros(1)),
                entry(1, StatementStartUtcMicros(2)),
                entry(2, TimeZone("".into())),
            ],
            SemanticParameterError::DuplicateStatementStart,
        ),
        (
            vec![
                entry(0, AllowThrowException(false)),
                entry(0, TimeZone("x".repeat(256))),
            ],
            SemanticParameterError::InvalidTimeZone,
        ),
    ];
    for (entries, expected) in cases {
        let input = wire::SemanticParameters { entries };
        assert!(
            matches!(decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()), Err(Error::Parameter(actual)) if actual == expected)
        );
    }
    let missing = wire::SemanticParameters {
        entries: vec![
            entry(0, AllowThrowException(true)),
            wire::SemanticParameter { id: 1, value: None },
        ],
    };
    assert!(matches!(
        decode_semantic_parameters(&missing, SOURCE, limits(), &Control::default()),
        Err(Error::InvalidShape(
            "semantic parameter is missing its value variant"
        ))
    ));
    let too_many = wire::SemanticParameters {
        entries: vec![wire::SemanticParameter { id: 0, value: None }; MAX_SEMANTIC_PARAMETERS + 1],
    };
    assert!(matches!(
        decode_semantic_parameters(&too_many, SOURCE, limits(), &Control::default()),
        Err(Error::Parameter(SemanticParameterError::TooManyParameters))
    ));
}
#[test]
fn bounded_parameters_timezone_byte_edges_and_empty_request_counts_follow_original_owner() {
    for zone in ["Z".to_string(), "x".repeat(255), "雪".repeat(85)] {
        let input = wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone(zone.clone()),
            )],
        };
        let (table, facts) =
            decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
        assert_eq!(facts.timezone_request_bytes_upper_bound, zone.len());
        assert_eq!(facts.allocation_requests_upper_bound, 2);
        assert_eq!(
            table.get(SemanticParameterId::new(0)),
            Some(&SemanticParameterValue::TimeZone(zone.into_boxed_str()))
        );
    }
    for zone in ["".to_string(), "x".repeat(256), "a\nb".to_string()] {
        let input = wire::SemanticParameters {
            entries: vec![entry(0, wire::semantic_parameter::Value::TimeZone(zone))],
        };
        assert!(matches!(
            decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()),
            Err(Error::Parameter(SemanticParameterError::InvalidTimeZone))
        ));
    }
    let empty = wire::SemanticParameters { entries: vec![] };
    let (_, facts) =
        decode_semantic_parameters(&empty, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(facts.allocation_requests_upper_bound, 0);
    assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
}
#[test]
fn bounded_parameters_every_actual_callback_retains_first_control_and_never_publishes_a_prefix() {
    let cases = [
        source(),
        wire::SemanticParameters { entries: vec![] },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("x".repeat(255)),
            )],
        },
        wire::SemanticParameters {
            entries: vec![
                entry(
                    0,
                    wire::semantic_parameter::Value::AllowThrowException(true),
                ),
                wire::SemanticParameter { id: 1, value: None },
            ],
        },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("a\nb".into()),
            )],
        },
    ];
    for (case, input) in cases.iter().enumerate() {
        let control = Control::default();
        let result = decode_semantic_parameters(input, SOURCE, limits(), &control);
        assert_eq!(result.is_err(), case >= 3);
        let trace = control.trace.into_inner().unwrap();
        assert_eq!(trace[0], (CompilePhase::Decode, 0));
        if case == 2 {
            assert!(trace.iter().any(|(_, units)| *units == 256));
        }
        if case == 3 {
            assert_eq!(trace.last().unwrap().1, 0);
        }
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(decode_semantic_parameters(input, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn bounded_parameter_sending_matches_independent_canonical_wire_and_prepared_loan() {
    let raw = source();
    let (table, _) =
        decode_semantic_parameters(&raw, SOURCE, limits(), &Control::default()).unwrap();
    let mut expected = raw;
    expected.entries.sort_by_key(|entry| entry.id);
    let control = EncodeControl::default();
    let (wire, facts) = encode_semantic_parameters(&table, SOURCE, limits(), &control).unwrap();
    assert_eq!(wire, expected);
    if let Some(wire::semantic_parameter::Value::TimeZone(zone)) = &wire.entries[0].value {
        assert_eq!(zone.capacity(), zone.len());
    } else {
        panic!("expected original timezone spelling");
    }
    assert_eq!(facts.parameter_count, 6);
    assert_eq!(facts.timezone_request_bytes_upper_bound, 4);
    assert_eq!(facts.allocation_requests_upper_bound, 2);
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        std::alloc::Layout::array::<wire::SemanticParameter>(6)
            .unwrap()
            .size()
            + 4
    );
    let prepared = prepare_semantic_parameters_encode(&table, SOURCE, limits(), &control).unwrap();
    assert_eq!(*prepared.facts(), facts);
    let (emitted, emitted_facts) = prepared.emit().unwrap();
    assert_eq!(emitted, expected);
    assert_eq!(emitted_facts, facts);
    let (again, _) =
        decode_semantic_parameters(&wire, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(again, table);
}
#[test]
fn bounded_parameter_sending_six_exact_caps_empty_and_occupied_source_floors() {
    let (table, _) =
        decode_semantic_parameters(&source(), SOURCE, limits(), &Control::default()).unwrap();
    let (_, facts) =
        encode_semantic_parameters(&table, SOURCE, limits(), &EncodeControl::default()).unwrap();
    let exact = ParameterProjectionLimits {
        max_parameters: facts.parameter_count,
        max_timezone_request_bytes: facts.timezone_request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(encode_semantic_parameters(&table, SOURCE, exact, &EncodeControl::default()).is_ok());
    for axis in 0..6 {
        let mut cap = exact;
        match axis {
            0 => cap.max_parameters -= 1,
            1 => cap.max_timezone_request_bytes -= 1,
            2 => cap.max_allocation_requests -= 1,
            3 => cap.max_allocation_request_bytes -= 1,
            4 => cap.max_coexisting_source_and_request_bytes -= 1,
            5 => cap.max_work -= 1,
            _ => unreachable!(),
        }
        assert!(matches!(
            encode_semantic_parameters(&table, SOURCE, cap, &EncodeControl::default()),
            Err(Error::InvalidShape(_))
        ));
    }
    let known = size_of::<SemanticParameters>()
        + 6 * (size_of::<SemanticParameterId>() + size_of::<SemanticParameterValue>())
        + 4;
    assert!(matches!(
        prepare_semantic_parameters_encode(&table, known - 1, limits(), &EncodeControl::default()),
        Err(Error::InvalidShape(_))
    ));
    // This lower floor does not assert the actual BTree headers/capacity fit;
    // the original caller still owes its complete retained-source invoice.
    let empty = SemanticParameters::default();
    let (_, facts) =
        encode_semantic_parameters(&empty, SOURCE, limits(), &EncodeControl::default()).unwrap();
    assert_eq!(facts.allocation_requests_upper_bound, 0);
    assert_eq!(facts.allocation_request_bytes_upper_bound, 0);
}
#[test]
fn bounded_parameter_sending_every_callback_and_real_source_quantum_use_original_control() {
    let empty = SemanticParameters::default();
    let (raw, _) =
        decode_semantic_parameters(&source(), SOURCE, limits(), &Control::default()).unwrap();
    let zone = SemanticParameters::try_new([(
        SemanticParameterId::new(0),
        SemanticParameterValue::TimeZone("x".repeat(255).into_boxed_str()),
    )])
    .unwrap();
    let short = SemanticParameters::try_new([(
        SemanticParameterId::new(0),
        SemanticParameterValue::TimeZone("UTC".into()),
    )])
    .unwrap();
    let unicode = SemanticParameters::try_new([(
        SemanticParameterId::new(0),
        SemanticParameterValue::TimeZone("雪".repeat(85).into_boxed_str()),
    )])
    .unwrap();
    for table in [&empty, &raw, &zone, &short, &unicode] {
        let control = EncodeControl::default();
        let (wire, facts) = encode_semantic_parameters(table, SOURCE, limits(), &control).unwrap();
        for entry in &wire.entries {
            if let Some(wire::semantic_parameter::Value::TimeZone(zone)) = &entry.value {
                assert_eq!(zone.capacity(), zone.len());
                assert_eq!(zone.len(), facts.timezone_request_bytes_upper_bound);
            }
        }
        let trace = control.0.trace.into_inner().unwrap();
        assert_eq!(trace[0], (CompilePhase::Encode, 0));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = EncodeControl(Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                });
                assert!(
                    matches!(encode_semantic_parameters(table, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.0.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let wide = SemanticParameters::try_new((0..320).map(|id| {
        (
            SemanticParameterId::new(id),
            SemanticParameterValue::AllowThrowException(false),
        )
    }))
    .unwrap();
    let control = EncodeControl::default();
    assert!(encode_semantic_parameters(&wide, SOURCE, limits(), &control).is_ok());
    let trace = control.0.trace.into_inner().unwrap();
    assert!(trace.iter().map(|(_, n)| n).sum::<u32>() > 256);
    // Each actual opaque tree pull is flushed; count/source work need not be
    // batched into a synthetic 256-unit callback just to hit a round number.
    for at in [0, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let control = EncodeControl(Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            });
            assert!(
                matches!(encode_semantic_parameters(&wide, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.0.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[derive(Default)]
struct ValidateControl(Control);
impl PureCompileControl for ValidateControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        self.0.record(phase, units)
    }
}
fn observed_decode(
    input: &wire::SemanticParameters,
    control: &ValidateControl,
    admit: &mut dyn FnMut(&ParameterProjectionFacts) -> Result<(), CompileControlError>,
) -> Result<(SemanticParameters, ParameterProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let prepared = prepare_semantic_parameters_decode_observed_in(
            input,
            SOURCE,
            limits(),
            admit,
            &mut work,
        )?;
        prepared.emit_observed_in(admit, &mut work)
    })();
    finish_projection(work, result)
}
fn observed_encode(
    input: &SemanticParameters,
    control: &ValidateControl,
    admit: &mut dyn FnMut(&ParameterProjectionFacts) -> Result<(), CompileControlError>,
) -> Result<(wire::SemanticParameters, ParameterProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let prepared = prepare_semantic_parameters_encode_observed_in(
            input,
            SOURCE,
            limits(),
            admit,
            &mut work,
        )?;
        prepared.emit_observed_in(admit, &mut work)
    })();
    finish_projection(work, result)
}

#[test]
fn observed_parameters_preserve_original_facts_sparse_ids_layout_and_caller_phase() {
    let input = source();
    let (original, receiving) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    let (old_wire, sending) =
        encode_semantic_parameters(&original, SOURCE, limits(), &EncodeControl::default()).unwrap();
    let mut expected = input.clone();
    expected.entries.sort_by_key(|entry| entry.id);
    let control = ValidateControl::default();
    let mut contributions = Vec::new();
    let mut admit = |facts: &ParameterProjectionFacts| {
        assert_eq!(
            facts.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + facts.allocation_request_bytes_upper_bound,
        );
        contributions.push(*facts);
        Ok(())
    };
    let (table, facts) = observed_decode(&input, &control, &mut admit).unwrap();
    assert_eq!(table, original);
    assert_eq!(facts, receiving);
    assert_eq!(facts.parameter_count, 6);
    assert_eq!(facts.timezone_request_bytes_upper_bound, "雪Z".len());
    assert_eq!(facts.allocation_requests_upper_bound, 7);
    assert_eq!(contributions.last(), Some(&facts));
    assert!(contributions.windows(2).all(|pair| {
        pair[0].allocation_request_bytes_upper_bound <= pair[1].allocation_request_bytes_upper_bound
            && pair[0].cumulative_work_upper_bound <= pair[1].cumulative_work_upper_bound
    }));
    assert!(
        control
            .0
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Validate)
    );
    let control = ValidateControl::default();
    let (encoded, facts) = observed_encode(&table, &control, &mut |_| Ok(())).unwrap();
    assert_eq!(encoded, expected);
    assert_eq!(encoded, old_wire);
    assert_eq!(facts, sending);
    assert_eq!(facts.allocation_requests_upper_bound, 2);
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        Layout::array::<wire::SemanticParameter>(6).unwrap().size() + "雪Z".len()
    );
    assert!(
        control
            .0
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Validate)
    );

    // Actual final contribution is accepted exactly on every axis. Each
    // independently lowered axis refuses through the new typed numerical
    // gate, rather than the legacy ordinary envelope diagnostic.
    for (sending_source, expected_facts) in [(false, receiving), (true, sending)] {
        let exact = ParameterProjectionLimits {
            max_parameters: expected_facts.parameter_count,
            max_timezone_request_bytes: expected_facts.timezone_request_bytes_upper_bound,
            max_allocation_requests: expected_facts.allocation_requests_upper_bound,
            max_allocation_request_bytes: expected_facts.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: expected_facts
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: expected_facts.cumulative_work_upper_bound,
        };
        for lowered in 0..=6 {
            let mut cap = exact;
            match lowered {
                0 => {}
                1 => cap.max_parameters -= 1,
                2 => cap.max_timezone_request_bytes -= 1,
                3 => cap.max_allocation_requests -= 1,
                4 => cap.max_allocation_request_bytes -= 1,
                5 => cap.max_coexisting_source_and_request_bytes -= 1,
                6 => cap.max_work -= 1,
                _ => unreachable!(),
            }
            let control = ValidateControl::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            let result = if sending_source {
                prepare_semantic_parameters_encode_observed_in(
                    &original,
                    SOURCE,
                    cap,
                    &mut |_| Ok(()),
                    &mut work,
                )
                .and_then(|token| token.emit_observed_in(&mut |_| Ok(()), &mut work))
                .map(|_| ())
            } else {
                prepare_semantic_parameters_decode_observed_in(
                    &input,
                    SOURCE,
                    cap,
                    &mut |_| Ok(()),
                    &mut work,
                )
                .and_then(|token| token.emit_observed_in(&mut |_| Ok(()), &mut work))
                .map(|_| ())
            };
            let result = finish_projection(work, result);
            if lowered == 0 {
                result.unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ));
            }
        }
    }

    let original_control = ValidateControl::default();
    let mut work = CompileCheckpoints::try_new(&original_control, CompilePhase::Validate).unwrap();
    let prepared = prepare_semantic_parameters_decode_observed_in(
        &input,
        SOURCE,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let foreign = ValidateControl::default();
    let mut foreign_work = CompileCheckpoints::try_new(&foreign, CompilePhase::Validate).unwrap();
    assert!(matches!(
        prepared.emit_observed_in(&mut |_| Ok(()), &mut foreign_work),
        Err(Error::InvalidShape(_))
    ));
    assert_eq!(
        *foreign.0.trace.lock().unwrap(),
        [(CompilePhase::Validate, 0)]
    );
}

#[test]
fn observed_parameter_header_zone_and_emission_requests_precede_late_control() {
    let input = wire::SemanticParameters {
        entries: vec![entry(
            0,
            wire::semantic_parameter::Value::TimeZone("Z".into()),
        )],
    };
    let (table, _) =
        decode_semantic_parameters(&input, SOURCE, limits(), &Control::default()).unwrap();
    for sending in [false, true] {
        for cause in CAUSES {
            let control = ValidateControl(Control {
                stop: Some((1, cause)),
                ..Control::default()
            });
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut cap = limits();
            cap.max_allocation_requests = 0;
            let mut called = false;
            let mut admit = |_: &ParameterProjectionFacts| {
                called = true;
                Ok(())
            };
            let result = if sending {
                prepare_semantic_parameters_encode_observed_in(
                    &table, SOURCE, cap, &mut admit, &mut work,
                )
                .map(|_| ())
            } else {
                prepare_semantic_parameters_decode_observed_in(
                    &input, SOURCE, cap, &mut admit, &mut work,
                )
                .map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert!(
                !called,
                "the known component header must pass before parent admission"
            );
            assert_eq!(
                *control.0.trace.lock().unwrap(),
                [(CompilePhase::Validate, 0)]
            );
        }
        // Zone length is charged at discovery, before its completed callback.
        // Sending uses the real flushed BTree pull; no fabricated pending
        // work is attributed to opaque library iteration.
        let mut refuse_zone = |facts: &ParameterProjectionFacts| {
            if facts.timezone_request_bytes_upper_bound > 0 {
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        };
        let baseline = ValidateControl::default();
        let result = if sending {
            observed_encode(&table, &baseline, &mut refuse_zone).map(|_| ())
        } else {
            observed_decode(&input, &baseline, &mut refuse_zone).map(|_| ())
        };
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        let prefix = baseline.0.trace.into_inner().unwrap();
        for cause in CAUSES {
            let control = ValidateControl(Control {
                stop: Some((prefix.len(), cause)),
                ..Control::default()
            });
            let result = if sending {
                observed_encode(&table, &control, &mut refuse_zone).map(|_| ())
            } else {
                observed_decode(&input, &control, &mut refuse_zone).map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.0.trace.lock().unwrap(), prefix);
        }
    }
    // Completed original header work leaves the receiver at a real pending255
    // seam when its first valid zone length becomes known.
    for cause in CAUSES {
        let control = ValidateControl(Control {
            stop: Some((1, cause)),
            ..Control::default()
        });
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..251 {
            work.step().unwrap();
        }
        assert!(matches!(
            prepare_semantic_parameters_decode_observed_in(
                &input,
                SOURCE,
                limits(),
                &mut |facts| if facts.timezone_request_bytes_upper_bound > 0 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                },
                &mut work,
            ),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(
            *control.0.trace.lock().unwrap(),
            [(CompilePhase::Validate, 0)]
        );
    }
    for sending in [false, true] {
        let baseline = ValidateControl::default();
        let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::Validate).unwrap();
        if sending {
            let _ = prepare_semantic_parameters_encode_observed_in(
                &table,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
        } else {
            let _ = prepare_semantic_parameters_decode_observed_in(
                &input,
                SOURCE,
                limits(),
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
        }
        work.flush().unwrap();
        let prefix = baseline.0.trace.into_inner().unwrap();
        for cause in CAUSES {
            let control = ValidateControl(Control {
                stop: Some((prefix.len(), cause)),
                ..Control::default()
            });
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            let result = if sending {
                let prepared = prepare_semantic_parameters_encode_observed_in(
                    &table,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .unwrap();
                work.flush().unwrap();
                prepared
                    .emit_observed_in(
                        &mut |_| Err(CompileControlError::ResourceExhausted),
                        &mut work,
                    )
                    .map(|_| ())
            } else {
                let prepared = prepare_semantic_parameters_decode_observed_in(
                    &input,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .unwrap();
                work.flush().unwrap();
                prepared
                    .emit_observed_in(
                        &mut |_| Err(CompileControlError::ResourceExhausted),
                        &mut work,
                    )
                    .map(|_| ())
            };
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*control.0.trace.lock().unwrap(), prefix);
        }
    }
}

#[test]
fn observed_parameters_every_actual_callback_keeps_primary_cause_and_caller_footer() {
    let mut excess_zone = String::with_capacity(SOURCE + 1);
    excess_zone.push('Z');
    let inputs = [
        source(),
        wire::SemanticParameters { entries: vec![] },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("x".repeat(255)),
            )],
        },
        wire::SemanticParameters {
            entries: vec![
                entry(
                    0,
                    wire::semantic_parameter::Value::AllowThrowException(true),
                ),
                wire::SemanticParameter { id: 1, value: None },
            ],
        },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone("a\nb".into()),
            )],
        },
        wire::SemanticParameters {
            entries: vec![entry(
                0,
                wire::semantic_parameter::Value::TimeZone(excess_zone),
            )],
        },
    ];
    for (case, input) in inputs.iter().enumerate() {
        let control = ValidateControl::default();
        let result = observed_decode(input, &control, &mut |_| Ok(()));
        assert_eq!(result.is_err(), case >= 3);
        if case >= 3 {
            assert!(
                !matches!(result, Err(Error::Control(_))),
                "original law/source floor must remain ordinary"
            );
        }
        let trace = control.0.trace.into_inner().unwrap();
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        if case == 2 {
            assert!(trace.iter().any(|(_, n)| *n == 256));
        }
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = ValidateControl(Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                });
                assert!(
                    matches!(observed_decode(input, &control, &mut |_| Ok(())), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.0.trace.lock().unwrap(), trace[..=at]);
            }
        }
        if case < 3 {
            let (table, _) =
                decode_semantic_parameters(input, SOURCE, limits(), &Control::default()).unwrap();
            let control = ValidateControl::default();
            observed_encode(&table, &control, &mut |_| Ok(())).unwrap();
            let trace = control.0.trace.into_inner().unwrap();
            for at in 0..trace.len() {
                for cause in CAUSES {
                    let control = ValidateControl(Control {
                        stop: Some((at, cause.clone())),
                        ..Control::default()
                    });
                    assert!(
                        matches!(observed_encode(&table, &control, &mut |_| Ok(())), Err(Error::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.0.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
