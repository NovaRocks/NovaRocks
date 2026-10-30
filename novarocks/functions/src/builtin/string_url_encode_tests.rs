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

use super::super::string_url_encode_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionBindingResolver, FunctionResultType, FunctionSpecializationFailure, FunctionValueType,
    PureFunctionMetadataOwner, PureScalarImplementation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionVolatility, PureCompileControl, SemanticParameters,
};
use std::{sync::Mutex, time::Duration};

const NAMES: [&str; 1] = ["url_encode"];
// Independent byte-percent oracles, never computed with the candidate mapper.
const ORACLES: [(&str, &str); 10] = [
    ("", ""),
    ("AZaz09-_.~", "AZaz09-_.~"),
    (" /+%?&=:#[]@", "%20%2F%2B%25%3F%26%3D%3A%23%5B%5D%40"),
    ("\0\t\n\r\u{7f}", "%00%09%0A%0D%7F"),
    ("é中", "%C3%A9%E4%B8%AD"),
    ("a\u{301}", "a%CC%81"),
    ("👩‍💻", "%F0%9F%91%A9%E2%80%8D%F0%9F%92%BB"),
    ("%2f", "%252f"),
    ("!$'()*;,", "%21%24%27%28%29%2A%3B%2C"),
    ("Straße İΣ", "Stra%C3%9Fe%20%C4%B0%CE%A3"),
];
fn expected(_op: usize, oracle: &(&str, &str)) -> String {
    oracle.1.into()
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("URL encoding never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn instance(name: &str, nullable: bool) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(name, &source(nullable), DecimalOverflowPolicy::OutputNull)
            .unwrap(),
    )
    .unwrap()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<String>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = source(true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("original")
                .unwrap()
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "source-note".into(),
                    "kept selected backing".into(),
                )])),
        ),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 2 * 1024 * 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 4 * 1024 * 1024,
            max_library_validation_bytes: 4 * 1024 * 1024,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

#[test]
fn string_url_encode_actual_profiles_preserve_independent_byte_percent_case_and_allowed_set() {
    let array = strings(ORACLES.iter().map(|(text, _)| Some(*text)).collect());
    for (op, name) in NAMES.into_iter().enumerate() {
        for nullable in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared =
                    prepared_for_test_with_policy(name, &source(nullable), policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let arguments = [EvaluatedArgument::Column(&array)];
                let result = kernel
                    .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                    .unwrap();
                assert_eq!(
                    output(&result),
                    ORACLES
                        .iter()
                        .map(|oracle| Some(expected(op, oracle)))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    for name in NAMES {
        let all_null = strings(vec![None; 3]);
        let arguments = [EvaluatedArgument::Column(&all_null)];
        let mut kernel = instance(name, true);
        let result = kernel
            .evaluate(Selection::all(3), &arguments, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![None; 3]);
    }
    let declarations = super::super::registry::builtin_scalar_declarations();
    for alias in ["urlencode", "encode_uri"] {
        assert!(operation(alias).is_none());
        assert!(!declarations.iter().any(|(name, _)| name == alias));
    }
}

#[test]
fn string_url_encode_sparse_sliced_compact_scalar_and_selected_constant_addresses_are_independent()
{
    let backing = strings(vec![
        Some("unused"),
        Some("aé中"),
        None,
        Some("👩‍💻"),
        Some("unused"),
    ]);
    let sliced = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("aé中"), Some("👩‍💻")]),
        Box::default(),
    )
    .unwrap();
    let constants = pool(strings(vec![None, Some("unused"), Some("aé中")]));
    let constant = constants.value(2).unwrap();
    let scalar = strings(vec![Some("aé中")]);
    for arg in [
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let mut kernel = instance("url_encode", true);
        let arguments = [arg];
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            output(&result),
            vec![
                Some("a%C3%A9%E4%B8%AD".into()),
                Some("%F0%9F%91%A9%E2%80%8D%F0%9F%92%BB".into())
            ]
        );
    }
    for arg in [
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Constant(&constant),
    ] {
        let mut kernel = instance("url_encode", true);
        let arguments = [arg];
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![Some("a%C3%A9%E4%B8%AD".into()); 2]);
    }
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    assert_eq!(constant.ordinal(), 2);
    assert_eq!(
        constant.pool().field().metadata()["source-note"],
        "kept selected backing"
    );
}

#[test]
fn string_url_encode_skips_inactive_and_hidden_null_payload_and_preserves_nullability() {
    let huge = "a".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'B');
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let inactive = strings(vec![Some(&huge), Some("B")]);
    let rows = [1];
    let selected = Selection::try_sparse(2, &rows).unwrap();
    for name in NAMES {
        let control = Control::default();
        let mut kernel = instance(name, true);
        let hidden_arguments = [EvaluatedArgument::Column(&hidden)];
        let result = kernel
            .evaluate(Selection::all(2), &hidden_arguments, &control)
            .unwrap();
        assert_eq!(output(&result), vec![None, Some("B".into())]);
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units < 256)
        );
        let inactive_arguments = [EvaluatedArgument::Column(&inactive)];
        let result = kernel
            .evaluate(selected, &inactive_arguments, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![Some("B".into())]);
        let mut nonnull = instance(name, false);
        assert!(matches!(
            nonnull.evaluate(
                Selection::all(2),
                &[EvaluatedArgument::Column(&hidden)],
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let after = Control::default();
        assert_eq!(
            nonnull
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
}

#[test]
fn string_url_encode_empty_selection_bypasses_body_and_selected_shape_errors_poison_once() {
    let array = strings(vec![Some("ΟΣ")]);
    let rows = [];
    let selection = Selection::try_sparse(1, &rows).unwrap();
    for name in NAMES {
        let mut kernel = instance(name, false);
        let arguments = [EvaluatedArgument::Column(&array)];
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert!(output(&result).is_empty());
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(1),
                        &[EvaluatedArgument::Column(&array)],
                        &Control::default()
                    )
                    .unwrap()
            )
            .len(),
            1
        );
        let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
        assert!(matches!(
            kernel.evaluate(
                Selection::all(1),
                &[EvaluatedArgument::Column(&wrong)],
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[EvaluatedArgument::Column(&array)],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
}

#[test]
fn string_url_encode_child_error_is_refused_before_the_ordinary_body() {
    let selection = Selection::all(1);
    let failed_child = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::SelectedColumn(&failed_child)];
    let mut kernel = instance("url_encode", true);
    assert!(matches!(
        kernel.evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(selection, &arguments, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}

#[test]
fn string_url_encode_output_layout_refuses_before_requests() {
    assert_eq!(
        output_layout(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_layout(1, i32::MAX as usize + 1).is_ok());
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    let failure = value_extent(i32::MAX as u128 + 1).unwrap_err();
    assert_eq!(failure.failure_fact().class(), FailureClass::Representation);
    assert_eq!(failure.failure_fact().reason().code(), 363);
    let extent = failure.failure_fact().representation().unwrap();
    assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
    assert_eq!(extent.required, Some(i32::MAX as u64 + 1));
    assert_eq!(extent.extent_limit, Some(i32::MAX as u64));
    assert!(failure.failure_fact().resource().is_none());
    assert!(output_layout(0, 0).is_ok());
    assert!(output_layout(3, 12).is_ok());
}

#[test]
fn string_url_encode_runtime_all_small_callbacks_and_real_256_keep_first_cause_and_latch() {
    for (array, all) in [
        (strings(vec![Some("ΟΣ"), None, Some("ß")]), true),
        (
            strings(vec![Some(&format!(
                "{}x{}",
                "\u{2003}".repeat(320),
                " ".repeat(320)
            ))]),
            false,
        ),
    ] {
        for name in NAMES {
            let good = Control::default();
            let mut kernel = instance(name, true);
            kernel
                .evaluate(
                    Selection::all(array.len()),
                    &[EvaluatedArgument::Column(&array)],
                    &good,
                )
                .unwrap();
            let trace = good.trace.lock().unwrap().clone();
            if !all {
                assert!(trace.contains(&256));
            }
            for (at, units) in trace.iter().enumerate() {
                if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
                    continue;
                }
                for cause in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                ] {
                    let control = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let mut kernel = instance(name, true);
                    assert_eq!(
                        kernel
                            .evaluate(
                                Selection::all(array.len()),
                                &[EvaluatedArgument::Column(&array)],
                                &control
                            )
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel
                            .evaluate(
                                Selection::all(array.len()),
                                &[EvaluatedArgument::Column(&array)],
                                &after
                            )
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.trace.lock().unwrap().is_empty());
                }
            }
        }
    }
}

#[test]
fn string_url_encode_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let owner = owner_for_test(name);
        let arguments = [FunctionArgument::Value {
            value_type: source(false),
            constant: None,
        }];
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: 1,
        };
        let selected = Arc::new(
            owner
                .resolve(request, crate::binding_test_control())
                .unwrap(),
        );
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(source(true))
        );
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            domain: EvaluationDomainId::new(7),
            demand: EvaluationDemand::Value,
        };
        let uses = [Some(ExpressionUseId::new(42))];
        let params = SemanticParameters::try_new([]).unwrap();
        let input = crate::CallEffectInput {
            context,
            argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
            function_id: owner.binding_declaration().function_id(),
            kind: crate::FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &params,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Domain(context.domain),
        };
        let fresh = specialize_scalar(
            &owner,
            input,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control(),
        )
        .unwrap();
        let canonical = fresh.prepared().contract().clone();
        let direct = owner
            .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
            .unwrap();
        assert!(Arc::ptr_eq(direct.contract(), &canonical));
        let frozen = specialize_frozen_scalar(
            &owner,
            input,
            selected.clone(),
            canonical.effects(),
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control(),
        )
        .unwrap();
        assert_eq!(frozen.prepared().contract().effects(), canonical.effects());
        assert_eq!(
            canonical.effects().value_stability,
            FunctionVolatility::Immutable
        );
        assert_eq!(
            canonical.effects().instance_state,
            FunctionInstanceState::None
        );
        assert_eq!(
            canonical.effects().null_behavior,
            FunctionNullBehavior::Strict
        );
        assert!(canonical.effects().observable_effects.is_empty());
        assert_eq!(
            canonical.effects().own_row_error,
            crate::FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            canonical.effects().argument_control,
            novarocks_type_contract::ArgumentControl::Eager
        );
        assert_eq!(
            canonical.effects().failure_behavior,
            crate::FunctionFailureBehavior::Propagate
        );
        assert!(canonical.effects().environment.is_empty());
        assert_eq!(
            owner.binding_declaration().function_id().as_str(),
            "builtin.scalar/url_encode/v1"
        );
        assert_eq!(owner.implementation_declarations().len(), 1);
        assert_eq!(
            owner.implementation_declarations()[0]
                .implementation
                .as_str(),
            "builtin.scalar/url_encode/selected-v1"
        );
        assert_eq!(
            owner.binding_declaration().overloads()[0].effects.as_ref(),
            Some(&effects())
        );
        let pattern_arguments = [arguments[0].clone(), arguments[0].clone()];
        assert!(
            owner
                .resolve(
                    FunctionBindingRequest {
                        arguments: &pattern_arguments,
                        logical_argument_count: 2,
                        expected_result_type: None,
                    },
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
        let mut forged = (*selected).clone();
        forged.result_type = FunctionResultType::Scalar(source(false));
        assert!(
            owner
                .validate_selected(&forged, request, crate::binding_test_control())
                .is_err()
        );
        let foreign_id = crate::FunctionId::try_new("builtin.scalar/trim/v1").unwrap();
        let mut foreign = input;
        foreign.function_id = &foreign_id;
        assert!(
            owner
                .prepare_scalar(foreign, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let copy = (*selected).clone();
        let mut stale = input;
        stale.selected = &copy;
        assert!(
            owner
                .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        stale = input;
        stale.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull;
        assert!(
            owner
                .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        assert_eq!(fresh.prepared().instance_retained_upper_bound(), 0);
        stale = input;
        stale.context.use_id = ExpressionUseId::new(999);
        assert!(
            owner
                .prepare_scalar(stale, canonical, crate::binding_test_control())
                .is_err()
        );
        for source in [
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::from_legacy_declared_projection(
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    true,
                ))),
                true,
            ),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            assert!(
                prepared_for_test_with_policy(name, &source, DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
}

#[test]
fn string_url_encode_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "url_encode",
                &ty,
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "url_encode",
                    &ty,
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .and_then(|error| match error {
                    FunctionSpecializationFailure::Control(actual) => Some(actual),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                });
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn required_url_encoding_shares_oracles_and_exact_scalar_broadcast() {
    for (input, expected) in ORACLES {
        let source: ArrayRef = Arc::new(StringArray::from(vec![input]));
        let output = evaluate_required(&source, 3).unwrap();
        let values = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(expected); 3]);
    }
    let source: ArrayRef = Arc::new(StringArray::from(vec![Some("/+"), None, Some("")]));
    let output = evaluate_required(&source, 3).unwrap();
    let values = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some("%2F%2B"), None, Some("")]
    );
}
