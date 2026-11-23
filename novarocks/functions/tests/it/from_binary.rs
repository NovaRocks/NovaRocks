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

use std::{sync::Arc, time::Duration};

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray};
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, DataFailureReason, EvaluatedArgument, FunctionArgument,
    FunctionBindingRequest, FunctionKind, KernelEvaluationControl, KernelFailure,
    MAX_UNOBSERVED_KERNEL_WORK, PreparedPureKernel, PureCallPreparation, RowDataError,
    ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameters,
};

struct Control;

impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Byte functions must not wait");
    }
}

fn evaluate_text(name: &str, arrays: Vec<ArrayRef>) -> Vec<Option<String>> {
    let (values, errors) = evaluate_call(name, arrays);
    assert!(errors.is_empty());
    values
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}

fn evaluate_call(name: &str, arrays: Vec<ArrayRef>) -> (ArrayRef, Box<[RowDataError]>) {
    let rows = arrays[0].len();
    let arguments = arrays
        .iter()
        .map(|array| FunctionArgument::Value {
            value_type: FunctionValueType::new(array.data_type().clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let uses = [Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))];
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(3),
        region: EvaluationRegionId::new(1),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let specialized = catalog
        .prepare_fresh_selected(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses[..arguments.len()]),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &Control,
        )
        .unwrap();
    let PreparedPureKernel::Scalar(prepared) = specialized.into_prepared() else {
        panic!("{name} must prepare a scalar");
    };
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let output = instance
        .evaluate(Selection::all(rows), &arguments, &Control)
        .unwrap();
    let (_, values, errors) = output.into_parts();
    (values, errors)
}

#[test]
fn trino_byte_overloads_preserve_raw_bytes_and_typed_selected_errors() {
    let raw = BinaryArray::from(vec![Some(&[0xff, 0x00][..]), None, Some(&[][..])]);
    let (values, errors) = evaluate_call("crc32", vec![Arc::new(raw)]);
    assert!(errors.is_empty());
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(3_539_857_293), None, Some(0)]
    );

    let text = StringArray::from(vec![Some("/wA="), Some("=a"), None, Some("")]);
    let (values, errors) = evaluate_call("from_base64", vec![Arc::new(text)]);
    let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some(&[0xff, 0x00][..]), None, None, Some(&[][..])]
    );
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].selected_ordinal(), 1);
    assert_eq!(
        errors[0].reason(),
        DataFailureReason::Domain(
            novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT
        )
    );
}

#[test]
fn trino_uri_overloads_decode_values_after_matching_raw_parameter_names() {
    let sources = Arc::new(StringArray::from(vec![
        Some("/a?%71=first+name&q=second%26name"),
        Some("/a?%71=first+name&q=second%26name"),
        None,
    ]));
    let keys = Arc::new(StringArray::from(vec![Some("q"), Some("%71"), Some("q")]));
    let (values, errors) = evaluate_call("url_extract_parameter", vec![sources, keys]);
    assert!(errors.is_empty());
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some("second&name"), Some("first name"), None]
    );

    let (values, errors) =
        evaluate_call("url_encode", vec![Arc::new(StringArray::from(vec!["a ~"]))]);
    assert!(errors.is_empty());
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a+%7E"
    );
}

#[test]
fn from_utf8_replacement_is_per_row_and_keeps_empty_bytes_distinct_from_null() {
    let bytes = Arc::new(BinaryArray::from(vec![
        Some(&b"\xff"[..]),
        Some(&b"\xff"[..]),
        Some(&b"\xff"[..]),
        Some(&b""[..]),
        None,
        Some("é😀".as_bytes()),
    ]));
    let replacements = Arc::new(StringArray::from(vec![
        None,
        Some(""),
        Some("中"),
        Some("�"),
        Some("�"),
        Some(""),
    ]));
    assert_eq!(
        evaluate_text("from_utf8", vec![bytes, replacements]),
        [
            None,
            Some("".into()),
            Some("中".into()),
            Some("".into()),
            None,
            Some("é😀".into())
        ]
    );
}

#[test]
fn utf8_replacement_groups_invalid_prefixes_like_trino_484() {
    // Trino 484's SliceUtf8 2.9 replaces a lead byte plus its valid
    // continuations, including invalid but complete encodings.
    let vectors: &[(&[u8], &str)] = &[
        (b"\xc0\xaf", "\u{fffd}"),                             // Overlong.
        (b"\xed\xa0\x80", "\u{fffd}"),                         // Surrogate.
        (b"\xf4\x90\x80\x80", "\u{fffd}"),                     // Above the Unicode range.
        (b"\xf8\x80\x80\x80\x80", "\u{fffd}"),                 // Obsolete five-byte sequence.
        (b"\xfc\x80\x80\x80\x80\x80", "\u{fffd}"),             // Obsolete six-byte sequence.
        (b"a\xe2\x82b\x80\x80", "a\u{fffd}b\u{fffd}\u{fffd}"), // Truncated, then stray continuations.
        (b"x\xf0\x9f\x9a", "x\u{fffd}"),                       // Truncated at the end.
        (b"\xe2\x82\xac", "€"),
    ];
    assert_eq!(
        evaluate_text(
            "from_utf8",
            vec![Arc::new(BinaryArray::from(
                vectors
                    .iter()
                    .map(|&(input, _)| Some(input))
                    .collect::<Vec<_>>()
            ))],
        ),
        vectors
            .iter()
            .map(|&(_, output)| Some(output.to_owned()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn base64_padding_applies_only_at_the_end_of_the_value() {
    let input = b"abc".repeat(65);
    let input = [input.as_slice(), b"z"].concat();
    assert_eq!(
        evaluate_text(
            "to_base64",
            vec![Arc::new(BinaryArray::from(vec![Some(input.as_slice())]))]
        ),
        [Some("YWJj".repeat(65) + "eg==")]
    );
}
