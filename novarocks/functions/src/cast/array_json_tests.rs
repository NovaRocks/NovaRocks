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
use super::*;
use crate::{KernelDiagnostic, KernelEvaluationControl};
use novarocks_failure_contract::{FailureClass, FailureProjection};
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        calls.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pure JSON CAST never waits")
    }
}
fn run(op: Operation, raw: &str, control: &Control) -> Result<Option<String>, LeafError> {
    let mut work = EvaluationCheckpoints::new(control);
    let result = convert_non_null_observed(op, raw, &mut work);
    // Same borrowed value boundary: outer refusals do not add a footer;
    // conversion Data is a successful value fact until the caller journals it.
    if matches!(&result, Err(LeafError::Outer(_))) {
        return result;
    }
    work.finish().map_err(LeafError::Outer)?;
    result
}
fn json(raw: &str) -> Result<Option<String>, LeafError> {
    run(Operation::JsonToVarchar, raw, &Control::default())
}
fn is_conversion(result: Result<Option<String>, LeafError>) {
    let Err(LeafError::Conversion(cause)) = result else {
        panic!("expected own conversion Data");
    };
    assert_eq!(cause.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        cause.failure_fact().reason(),
        novarocks_function_contract::kernel_failure::INVALID_CAST_ARGUMENT
    );
}

fn is_parse(result: Result<Option<String>, LeafError>) {
    let Err(LeafError::Parse(cause)) = result else {
        panic!("expected raw-entry parse Data");
    };
    assert_eq!(cause.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        cause.failure_fact().reason(),
        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
    );
}

#[test]
fn json_leaf_profile_requires_authored_domains_and_successful_null_declarations() {
    use super::super::{CastPrepareError, PreparedCastRecipe};
    use novarocks_type_contract::TrinoCastMode;
    let json = |nullable| {
        FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            nullable,
            ValueLogicalType::Json,
        )
        .unwrap()
    };
    let text = |nullable| FunctionValueType::new(DataType::Utf8, nullable);
    let prepare = |source: &FunctionValueType, target: &FunctionValueType| {
        PreparedCastRecipe::try_new_trino(
            source,
            target,
            TrinoCastMode::Explicit,
            crate::binding_test_control(),
        )
    };
    assert!(prepare(&json(false), &text(true)).is_ok());
    assert!(matches!(
        prepare(&json(false), &text(false)),
        Err(CastPrepareError::TypeMismatch)
    ));
    assert!(prepare(&text(false), &json(false)).is_ok());
    assert!(matches!(
        prepare(&text(true), &json(false)),
        Err(CastPrepareError::TypeMismatch)
    ));
    assert!(prepare(&text(true), &text(true)).unwrap().is_identity());
    assert!(prepare(&json(true), &json(true)).unwrap().is_identity());
    assert!(
        prepare(
            &FunctionValueType::new(DataType::Binary, false),
            &json(true)
        )
        .is_err()
    );
}

#[test]
fn raw_json_scalar_values_match_pinned_reference_and_keep_json_null_success() {
    for (raw, expected) in [
        (r#""plain""#, Some("plain")),
        (r#""a\"b\\c\n\u4e2d""#, Some("a\"b\\c\n中")),
        (r#""\uD83D\uDE00""#, Some("😀")),
        (r#""\uD834\uDD1E""#, Some("𝄞")),
        ("true", Some("true")),
        ("false", Some("false")),
        ("null", None),
        ("123", Some("123")),
        ("9007199254740993", Some("9007199254740993")),
        ("1e2", Some("1.0E2")),
        ("100.0", Some("100.0")),
        ("1.2300", Some("1.2300")),
        ("-0.0", Some("0.0")),
        ("-0", Some("0")),
        ("0e0", Some("0E0")),
        ("-0e0", Some("-0E0")),
        ("1.0000000000000001e18", Some("1.00000000000000013E18")),
    ] {
        assert_eq!(
            json(raw).unwrap(),
            expected.map(str::to_owned),
            "raw input {raw}"
        );
    }
    for raw in [
        r#"{"key":"value1"}"#,
        r#"[1,2]"#,
        r#" [ null, {"x": [true]} ] "#,
    ] {
        is_conversion(json(raw));
    }
}

#[test]
fn varchar_json_encoding_never_parses_json_looking_text() {
    for (text, expected) in [
        ("abc", r#""abc""#),
        (r#"{"k":1}"#, r#""{\"k\":1}""#),
        ("null", r#""null""#),
        ("", r#""""#),
        ("a\"b\\c\n", r#""a\"b\\c\n""#),
        ("中😀", "\"中😀\""),
        // Pinned Airbase418/Jackson2.22.3 default WRITE_HEX_UPPER_CASE.
        ("\x1a", r#""\u001A""#),
        ("\x1f", r#""\u001F""#),
    ] {
        assert_eq!(
            run(Operation::VarcharToJson, text, &Control::default()).unwrap(),
            Some(expected.into())
        );
    }
    let controls: String = (0_u8..32).map(char::from).collect();
    let actual = run(Operation::VarcharToJson, &controls, &Control::default())
        .unwrap()
        .unwrap();
    let decoded: String = serde_json::from_str(&actual).unwrap();
    assert_eq!(
        decoded, controls,
        "independent serde string decoding of all escaped controls"
    );
    assert_eq!(json(&actual).unwrap(), Some(controls));
}

#[test]
fn raw_json_validation_checks_whole_value_trailing_tokens_and_surrogates() {
    for raw in [
        "",
        " ",
        "NULL",
        "+1",
        "01",
        "1.",
        ".1",
        "1e",
        "1e+",
        "1e9999",
        "1e309",
        "[1e309]",
        "{\"x\":1e309}",
        "truefalse",
        "null trailing",
        r#""\uD800""#,
        r#""\uDC00""#,
        r#""\uD800\u0041""#,
        r#""\uD834""#,
        r#""\uDD1E""#,
        r#"["\uD834"]"#,
        r#"["\uDD1E"]"#,
        r#"{"value":"\uD834"}"#,
        r#"{"value":"\uDD1E"}"#,
        r#"{"\uD834":null}"#,
        r#"{"\uDD1E":null}"#,
        r#""\x41""#,
        "[1,]",
        "{\"x\":}",
        "{\"x\":1,}",
        "[null] trailing",
        "\"raw\nnewline\"",
    ] {
        is_parse(json(raw));
    }
    let raw = format!("{{\"huge\":\"{}\",\"tail\":[true,null]}}", "x".repeat(8192));
    let control = Control::default();
    is_conversion(run(Operation::JsonToVarchar, &raw, &control));
    assert!(
        control
            .calls
            .lock()
            .unwrap()
            .iter()
            .copied()
            .map(u64::from)
            .sum::<u64>()
            >= raw.len() as u64
    );
    let maximum = format!("{}0{}", "[".repeat(1024), "]".repeat(1024));
    is_conversion(json(&maximum));
    let too_deep = format!("{}0{}", "[".repeat(1025), "]".repeat(1025));
    is_parse(json(&too_deep));
    let tiny_outside_number_scale = format!("0.{}1", "0".repeat(16383));
    is_parse(json(&tiny_outside_number_scale));
    is_parse(json(&format!("{{\"x\":{tiny_outside_number_scale}}}")));
}

#[test]
fn exact_decimal_and_number_text_preserve_scale_beyond_binary64_and_decimal38() {
    let integer = "12345678901234567890123456789012345678901234567890";
    assert_eq!(
        json(integer).unwrap(),
        Some("1.234567890123456789012345678901234567890123456789E+49".into())
    );
    let precise = "123456789012345678901234567890123456789.1234500";
    assert_eq!(
        json(precise).unwrap(),
        Some("123456789012345678901234567890123456789.12345".into())
    );
    let tiny = format!("0.{}1200", "0".repeat(40));
    assert_eq!(json(&tiny).unwrap(), Some("1.2E-41".into()));
    let zero = format!("-0.{}", "0".repeat(64));
    assert_eq!(json(&zero).unwrap(), Some("0".into()));
    let decimal38 = format!("0.{}1", "0".repeat(37));
    assert_eq!(json(&decimal38).unwrap(), Some(decimal38));
    // Exact TrinoNumber.from normalization uses 200-digit HALF_UP precision.
    let carry = "9".repeat(201);
    assert_eq!(json(&carry).unwrap(), Some("1E+201".into()));
    let halfway = format!("1{}5", "0".repeat(199));
    assert_eq!(
        json(&halfway).unwrap(),
        Some(format!("1.{}1E+200", "0".repeat(198)))
    );
    let lower = format!("1{}4", "0".repeat(199));
    assert_eq!(json(&lower).unwrap(), Some("1E+200".into()));
    let highest_scale = format!("0.{}1", "0".repeat(16382));
    assert_eq!(json(&highest_scale).unwrap(), Some("1E-16383".into()));
    let lowest_scale = format!("1{}", "0".repeat(16384));
    assert_eq!(json(&lowest_scale).unwrap(), Some("1E+16384".into()));
    is_parse(json(&format!("1{}", "0".repeat(16385))));
}

#[test]
fn double_legacy_decimal_format_edges_use_independent_java25_oracle_values() {
    for (bits, expected) in [
        (0x0000000000000001, "4.9E-324"),
        (0x0000000000000002, "1.0E-323"),
        (0x0000000000000014, "1.0E-322"),
        (0x0010000000000000, "2.2250738585072014E-308"),
        (0x3fb999999999999a, "1.0E-1"),
        (0x4059000000000000, "1.0E2"),
        (0x43abc16d674ec801, "1.00000000000000013E18"),
        (0x7fefffffffffffff, "1.7976931348623157E308"),
    ] {
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        let result = double_text(f64::from_bits(bits), &mut work).unwrap();
        work.finish().unwrap();
        assert_eq!(result, expected, "input bits {bits:016x}");
    }
}

#[test]
fn large_double_input_is_observed_completely_and_parsed_with_bounded_midpoint_prefix() {
    // The same value with a long zero coefficient. This does not set a text quota.
    let raw = format!("1{}e-20000", "0".repeat(20000));
    let control = Control::default();
    assert_eq!(
        run(Operation::JsonToVarchar, &raw, &control).unwrap(),
        Some("1.0E0".into())
    );
    assert!(
        control
            .calls
            .lock()
            .unwrap()
            .iter()
            .copied()
            .map(u64::from)
            .sum::<u64>()
            >= raw.len() as u64
    );
    assert_eq!(
        json("1e-99999999999999999999999999999999999999999999999").unwrap(),
        Some("0E0".into())
    );
    assert_eq!(
        json("-0e99999999999999999999999999999999999999999999999").unwrap(),
        Some("-0E0".into())
    );
}

#[test]
fn variable_json_text_is_not_limited_by_the_old_scalar_64_byte_render_bound() {
    let original = "中\\\"\n".repeat(2048);
    let encoded = run(Operation::VarcharToJson, &original, &Control::default())
        .unwrap()
        .unwrap();
    assert!(encoded.len() > 64);
    assert_eq!(json(&encoded).unwrap(), Some(original));
}

#[derive(Debug)]
struct OriginalSource;
impl fmt::Display for OriginalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original JSON cast control source")
    }
}
impl Error for OriginalSource {}
impl FailureProjection for OriginalSource {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            FailureClass::Invariant,
            novarocks_failure_contract::FailureReason::new(
                novarocks_failure_contract::domain::KERNEL,
                std::num::NonZeroU16::new(65001).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for OriginalSource {}
#[test]
fn every_observation_refusal_keeps_original_outer_source_without_a_later_footer() {
    let source = KernelFailure::from_domain(OriginalSource);
    for (op, text) in [
        (Operation::VarcharToJson, "a\\\"中\n".repeat(192)),
        (
            Operation::JsonToVarchar,
            format!("\"{}\"", "\\u4e2d".repeat(192)),
        ),
        (Operation::JsonToVarchar, "4.9406564584124654e-324".into()),
        (Operation::JsonToVarchar, "{\"a\":[1,2,3]} trailing".into()),
    ] {
        let observed = Control::default();
        let _ = run(op, &text, &observed);
        let count = observed.calls.lock().unwrap().len();
        assert!(count > 0);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original contract refusal")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
            KernelFailure::Operational(KernelDiagnostic::new("original IO refusal")),
            KernelFailure::InstanceFailed,
            source.clone(),
        ] {
            for at in 0..count {
                let control = Control {
                    calls: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let Err(LeafError::Outer(actual)) = run(op, &text, &control) else {
                    panic!("refusal became a successful value/Data")
                };
                assert_eq!(actual, cause);
                assert_eq!(control.calls.lock().unwrap().len(), at + 1);
            }
        }
    }
}
