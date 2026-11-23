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
//! Selected expression execution for the installed byte, URI and regexp functions.

use arrow::{
    array::{Array, ArrayRef, BinaryArray, Int64Array, StringArray},
    datatypes::{DataType, Schema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_expression::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure,
    ScopeLayouts, compile_expr_set_pure,
    testkit::{
        CallbackControl, CountingAllocator, HarnessControl, PassThroughCallHost, callback_causes,
        constant_policy,
        expression_text::{TextColumn, TextContext, TextError, type_expression_text},
    },
};
use novarocks_expression_contract::{RootOrdinal, ValueId};
use novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT;
use novarocks_functions::{
    DataFailureReason, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    ScalarInvocationScope, SelectedValues, Selection,
};
use novarocks_type_contract::{
    CharacterKind, CharacterType, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy,
    FunctionValueType, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameters, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::{convert::Infallible, sync::Arc};

const ROOT: RootOrdinal = RootOrdinal::new(0);

fn varchar_kind(width: u32) -> ValueKind {
    ValueKind::BoundedCharacter(CharacterType::new(CharacterKind::Varchar, width).unwrap())
}

fn varchar(name: &str, ordinal: u32, width: u32) -> TextColumn {
    let value =
        ValueTypeRef::try_new(varchar_kind(width), true, ValueTypeStorageLifetime::pure()).unwrap();
    let layout =
        ValueLayout::try_from_normalized_arrow(DataType::Utf8, ValueTypeStorageLifetime::pure())
            .unwrap();
    TextColumn {
        name: name.to_owned(),
        value: ValueId::new(ordinal),
        value_type: FunctionValueType::try_from_value(value, layout).unwrap(),
    }
}

fn batch(columns: &[TextColumn], arrays: Vec<ArrayRef>) -> RecordBatch {
    let fields = columns
        .iter()
        .map(|col| col.value_type.try_to_field(&col.name).unwrap())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn text_program(
    sql: &str,
    columns: &[TextColumn],
) -> Result<(Arc<CompiledExprSet>, FunctionValueType), TextError> {
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::default();
    let context = TextContext {
        functions: &functions,
        constant_policy: constant_policy(),
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        parameters: &parameters,
        allow_throw_exception: SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::AllowThrowException,
        },
    };
    let typed = type_expression_text(sql, columns, &context, &HarnessControl)?;
    let unit = typed.unit();
    let result = unit
        .definition(unit.root(ROOT).unwrap().definition)
        .unwrap()
        .ty
        .clone();
    let mut work =
        CompileCheckpoints::try_new(&HarnessControl, CompilePhase::LowerProgram).unwrap();
    let program = compile_expr_set_pure(
        unit,
        typed.environment(),
        ScopeLayouts {
            ports: typed.layout().map_or(&[], std::slice::from_ref),
            all_ports: None,
        },
        context.constant_policy,
        &functions,
        &mut work,
    )
    .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    work.finish().unwrap();
    Ok((program, result))
}

fn instance(
    program: &Arc<CompiledExprSet>,
    allocation: &Arc<CountingAllocator>,
) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new_with_allocator(
        Arc::clone(program),
        ROOT,
        &HarnessControl,
        Some(allocation.clone()),
    )
    .unwrap()
}

fn evaluate<'a>(
    instance: &mut CompiledExpressionInstance,
    batch: &RecordBatch,
    selection: Selection<'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<Infallible>> {
    instance.evaluate(
        EvaluationInput::Batch {
            batch,
            source: None,
        },
        ScalarInvocationScope::new(selection, selection),
        ScalarInvocationActivation::Activated,
        control,
        &mut PassThroughCallHost,
    )
}

fn strings(out: &SelectedValues<'_>) -> Vec<Option<String>> {
    out.values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(str::to_owned))
        .collect()
}

#[test]
fn base64_uses_the_original_binary_carrier_through_selected_calls() {
    let columns = [TextColumn {
        name: "b".to_owned(),
        value: ValueId::new(11),
        value_type: FunctionValueType::new(DataType::Binary, true),
    }];
    let array: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"guard"[..]),
        Some(&[0xff, 0x00][..]),
        Some(&b"unselected"[..]),
        None,
        Some(&b""[..]),
    ]));
    let batch = batch(&columns, vec![array.slice(1, 4)]);
    let selected = Selection::try_sparse(4, &[0, 2, 3]).unwrap();
    let allocation = Arc::new(CountingAllocator::new(usize::MAX));
    let (encode, ty) = text_program("to_base64(b)", &columns).unwrap();
    assert_eq!(ty.value_type().kind(), &ValueKind::String);
    let encoded = evaluate(
        &mut instance(&encode, &allocation),
        &batch,
        selected,
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(encoded.selection(), selected);
    assert_eq!(
        strings(&encoded),
        [Some("/wA=".into()), None, Some(String::new())]
    );
    assert!(encoded.errors().is_empty());

    let (round_trip, ty) = text_program("from_base64(to_base64(b))", &columns).unwrap();
    assert_eq!(ty.value_type().kind(), &ValueKind::Binary);
    let decoded = evaluate(
        &mut instance(&round_trip, &allocation),
        &batch,
        selected,
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(
        decoded
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(&[0xff, 0x00][..]), None, Some(&b""[..])]
    );
    assert!(decoded.errors().is_empty());
    assert!(matches!(
        text_program("to_base64('abc')", &[]),
        Err(TextError::Typing { .. })
    ));

    let (literal, _) = text_program("to_base64(X'FF00')", &[]).unwrap();
    let unit = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let actual = evaluate(
        &mut instance(&literal, &allocation),
        &unit,
        Selection::all(1),
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(strings(&actual), [Some("/wA=".into())]);
}

#[test]
fn url_extraction_keeps_declared_width_and_compares_raw_parameter_keys() {
    let source = varchar("uri", 11, 128);
    let uri: ArrayRef = Arc::new(StringArray::from(vec![
        Some("/a?%71=first+name&q=second%26name"),
        Some("unselected"),
        Some("HTTP://ExAmPle.test:80/a/../b"),
        Some("http://["),
        None,
    ]));
    let key = varchar("key", 12, 16);
    let keys: ArrayRef = Arc::new(StringArray::from(vec![
        Some("q"),
        None,
        Some("q"),
        Some("q"),
        Some("q"),
    ]));
    let selection = Selection::try_sparse(5, &[0, 2, 3, 4]).unwrap();
    let allocation = Arc::new(CountingAllocator::new(usize::MAX));
    for (sql, columns, arrays, expected) in [
        (
            "url_extract_host(uri)",
            vec![source.clone()],
            vec![uri.clone()],
            [Some(""), Some("ExAmPle.test"), None, None],
        ),
        (
            "url_extract_path(uri)",
            vec![source.clone()],
            vec![uri.clone()],
            [Some("/a"), Some("/a/../b"), None, None],
        ),
        (
            "url_extract_parameter(uri,key)",
            vec![source.clone(), key],
            vec![uri.clone(), keys],
            [Some("second&name"), None, None, None],
        ),
        (
            "url_extract_parameter(uri,'%71')",
            vec![source],
            vec![uri],
            [Some("first name"), None, None, None],
        ),
    ] {
        let (program, ty) = text_program(sql, &columns).unwrap();
        assert_eq!(ty.value_type().kind(), &varchar_kind(128), "{sql}");
        let actual = evaluate(
            &mut instance(&program, &allocation),
            &batch(&columns, arrays),
            selection,
            &HarnessControl,
        )
        .unwrap();
        assert_eq!(actual.selection(), selection);
        assert_eq!(
            strings(&actual),
            expected.map(|value| value.map(str::to_owned)),
            "{sql}"
        );
        assert!(actual.errors().is_empty(), "{sql}");
    }
}

#[test]
fn regexp_literal_and_column_patterns_have_the_same_data_error_policy() {
    let source = varchar("s", 11, 16);
    let pattern = varchar("p", 12, 4);
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("éé"),
        Some("ababa"),
        Some("ignored"),
    ]));
    let patterns: ArrayRef = Arc::new(StringArray::from(vec![
        Some("["),
        Some("["),
        Some("["),
        Some("["),
    ]));
    let selection = Selection::try_sparse(4, &[0, 1, 2]).unwrap();
    let allocation = Arc::new(CountingAllocator::new(usize::MAX));
    for (sql, columns, arrays) in [
        (
            "regexp_count(s,'[')",
            vec![source.clone()],
            vec![input.clone()],
        ),
        (
            "regexp_count(s,p)",
            vec![source.clone(), pattern],
            vec![input.clone(), patterns],
        ),
    ] {
        let (program, ty) = text_program(sql, &columns).unwrap();
        assert_eq!(ty.data_type(), &DataType::Int64);
        let mut evaluator = instance(&program, &allocation);
        let batch = batch(&columns, arrays);
        let result = evaluate(&mut evaluator, &batch, selection, &HarnessControl).unwrap();
        assert_eq!(result.values().null_count(), 3);
        assert_eq!(
            result
                .errors()
                .iter()
                .map(|error| (error.selected_ordinal(), error.reason()))
                .collect::<Vec<_>>(),
            [
                (1, DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)),
                (2, DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT))
            ]
        );
        let empty = evaluate(
            &mut evaluator,
            &batch,
            Selection::try_sparse(4, &[]).unwrap(),
            &HarnessControl,
        )
        .unwrap();
        assert_eq!(empty.values().len(), 0);
        assert_eq!(empty.values().data_type(), &DataType::Int64);
        assert!(empty.errors().is_empty());
    }
    let (program, _) = text_program("regexp_count(s,'.')", &[source.clone()]).unwrap();
    let actual = evaluate(
        &mut instance(&program, &allocation),
        &batch(&[source], vec![input]),
        selection,
        &HarnessControl,
    )
    .unwrap();
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [None, Some(2), Some(5)]
    );
}

#[test]
fn regexp_replace_preserves_selected_row_order_and_try_scope() {
    let columns = [
        varchar("s", 11, 16),
        varchar("p", 12, 8),
        varchar("r", 13, 8),
    ];
    let batch = batch(
        &columns,
        vec![
            Arc::new(StringArray::from(vec![
                Some("unselected"),
                None,
                Some("a"),
                Some("abc"),
                Some("a"),
                Some("a"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("["),
                Some("["),
                Some("["),
                Some("z"),
                Some("(a)"),
                Some("(a)"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("$2"),
                Some("$2"),
                None,
                Some("$2"),
                Some("$2"),
                Some("$1x"),
            ])),
        ],
    );
    let selection = Selection::try_sparse(6, &[1, 2, 3, 4, 5]).unwrap();
    let allocation = Arc::new(CountingAllocator::new(usize::MAX));
    for (sql, error_ordinals) in [
        ("regexp_replace(s,p,r)", &[1, 3][..]),
        ("TRY(regexp_replace(s,p,r))", &[][..]),
    ] {
        let (program, _) = text_program(sql, &columns).unwrap();
        let result = evaluate(
            &mut instance(&program, &allocation),
            &batch,
            selection,
            &HarnessControl,
        )
        .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            strings(&result),
            [None, None, Some("abc".into()), None, Some("ax".into())]
        );
        assert_eq!(
            result
                .errors()
                .iter()
                .map(|error| error.selected_ordinal())
                .collect::<Vec<_>>(),
            error_ordinals
        );
        assert!(result.errors().iter().all(|error| error.reason() == DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)));
    }
    let (budget, _) = text_program("TRY(regexp_replace(s,p,r))", &columns).unwrap();
    let allocation = Arc::new(CountingAllocator::new(0));
    assert!(matches!(
        evaluate(
            &mut instance(&budget, &allocation),
            &batch,
            selection,
            &HarnessControl
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::ResourceExhausted
        ))
    ));
    assert_eq!(allocation.outstanding(), 0);
}

#[test]
fn ordered_regexp_keeps_the_first_checkpoint_failure_and_releases_its_allocations() {
    let columns = [
        varchar("s", 11, 1024),
        varchar("p", 12, 8),
        varchar("r", 13, 8),
    ];
    let source = "x".repeat(700);
    let batch = batch(
        &columns,
        vec![
            Arc::new(StringArray::from(vec![source.as_str()])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["b"])),
        ],
    );
    let (program, _) = text_program("regexp_replace(s,p,r)", &columns).unwrap();
    let allocation = Arc::new(CountingAllocator::new(usize::MAX));
    let trace_control = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let all = Selection::all(1);
    evaluate(
        &mut instance(&program, &allocation),
        &batch,
        all,
        &trace_control,
    )
    .unwrap();
    assert_eq!(allocation.outstanding(), 0);
    let trace = trace_control.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    let causes = callback_causes([
        "guarded-origin-invalid",
        "guarded-origin-internal",
        "guarded-origin-operational",
    ]);
    for index in 1..=trace.len() {
        for cause in &causes {
            let mut evaluator = instance(&program, &allocation);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(matches!(
                evaluate(&mut evaluator, &batch, all, &control),
                Err(ExprEvaluationFailure::Kernel(actual)) if &actual == cause
            ));
            assert_eq!(control.trace(), trace[..index]);
            assert_eq!(allocation.outstanding(), 0);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluate(&mut evaluator, &batch, all, &after),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
        }
    }
    let (reserved, refunded) = allocation.reservation_totals();
    assert!(reserved > 0);
    assert_eq!(reserved, refunded);
}
