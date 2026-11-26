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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use super::*;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ValueKind, ValueLayout, ValueTypeRef,
    ValueTypeStorageLifetime,
};
use std::sync::Mutex;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn source_map() -> FunctionValueType {
    let key = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let value = FunctionValueType::new(DataType::Int64, true);
    let source = declared_map(&key, &value, false).unwrap();
    let DataType::Map(entries, _) = source.data_type() else {
        panic!("actual map");
    };
    // A real complete sorted-map input. map_keys must preserve this original
    // argument binding, while its output is the original List<key> recipe.
    FunctionValueType::try_from_value(
        source.value_type().clone(),
        ValueLayout::try_new(
            &DataType::Map(entries.clone(), true),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn arguments(source: &FunctionValueType) -> [FunctionArgument; 1] {
    [FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    }]
}
fn request(args: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments: args,
        logical_argument_count: args.len(),
        expected_result_type: None,
    }
}
#[test]
fn fixed_map_key_result_replays_complete_domain_and_child_null_recipe() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = source_map();
    let args = arguments(&source);
    let bound = catalog
        .resolve_bound_user("map_keys", FunctionKind::Scalar, request(&args), &Control)
        .unwrap();
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &Control,
        )
        .unwrap();
    let FunctionArgumentType::Value(actual_source) = &bound.selected.argument_types[0] else {
        panic!("value channel")
    };
    assert_eq!(actual_source, &source);
    let FunctionResultType::Scalar(output) = &bound.selected.result_type else {
        panic!("scalar list")
    };
    let item = output.typed_binding().borrowed().child_at(0).unwrap();
    assert_eq!(
        item.value_type().kind(),
        &ValueKind::Nominal(ValueLogicalType::Json)
    );
    assert!(item.value_type().nullable());

    let mut wrong_domain = bound.selected.clone();
    wrong_domain.result_type = FunctionResultType::Scalar(FunctionValueType::new(
        output.data_type().clone(),
        output.nullable(),
    ));
    let error = catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &wrong_domain,
            request(&args),
            &Control,
        )
        .unwrap_err();
    assert!(error.to_string().contains("result type:"), "{error}");

    let item = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let storage = ValueTypeStorageLifetime::pure();
    let semantics = ValueTypeRef::try_new(
        ValueKind::List(item.value_type().clone()),
        output.nullable(),
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::LargeBinary,
            false,
        ))),
        storage,
    )
    .unwrap();
    let mut wrong_null = bound.selected.clone();
    wrong_null.result_type =
        FunctionResultType::Scalar(FunctionValueType::try_from_value(semantics, layout).unwrap());
    let error = catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &wrong_null,
            request(&args),
            &Control,
        )
        .unwrap_err();
    assert!(error.to_string().contains("result type:"), "{error}");
    assert_eq!(
        bound.selected.result_type,
        FunctionResultType::Scalar(output.clone())
    );
}
#[test]
fn fixed_recipe_first_cancel_has_no_tail_and_preserves_original_selection() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let args = arguments(&source_map());
    let bound = catalog
        .resolve_bound_user("map_keys", FunctionKind::Scalar, request(&args), &Control)
        .unwrap();
    struct Cancel(Mutex<Vec<u32>>);
    impl PureCompileControl for Cancel {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            self.0.lock().unwrap().push(units);
            Err(CompileControlError::Cancelled)
        }
    }
    let control = Cancel(Mutex::new(Vec::new()));
    let failure = catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &control,
        )
        .unwrap_err();
    assert!(matches!(
        failure,
        FunctionBindingError::Control(CompileControlError::Cancelled)
    ));
    assert_eq!(control.0.lock().unwrap().len(), 1);
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &Control,
        )
        .unwrap();
}

// This finite caller-owned request scope is a real rejecting constructor port;
// it records operation requests and releases them at the final actual hold.
// It does not observe allocator bytes or supply a production policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreatedRefusal {
    FirstCause,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestEvent {
    Exact(usize, usize),
    Opaque(usize),
}
#[derive(Default)]
struct ScopeFacts {
    requests: Mutex<Vec<RequestEvent>>,
    held: std::sync::atomic::AtomicUsize,
    exits: std::sync::atomic::AtomicUsize,
}
struct RequestLifetime(std::sync::Arc<ScopeFacts>);
impl novarocks_type_contract::ValueTypeStorageHold for RequestLifetime {}
impl Drop for RequestLifetime {
    fn drop(&mut self) {
        self.0.held.store(0, std::sync::atomic::Ordering::SeqCst);
        self.0
            .exits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct RequestScope {
    life: std::sync::Arc<RequestLifetime>,
    stop: Option<usize>,
    limit: usize,
}
impl RequestScope {
    fn new(stop: Option<usize>) -> (std::sync::Arc<ScopeFacts>, Self) {
        let facts = std::sync::Arc::new(ScopeFacts::default());
        let life = std::sync::Arc::new(RequestLifetime(facts.clone()));
        (
            facts,
            Self {
                life,
                stop,
                limit: 65536,
            },
        )
    }
    fn admit(&mut self, event: RequestEvent, bytes: usize) -> Result<(), CreatedRefusal> {
        let mut requests = self.life.0.requests.lock().unwrap();
        requests.push(event);
        if self.stop == Some(requests.len()) {
            return Err(CreatedRefusal::FirstCause);
        }
        let held = self.life.0.held.load(std::sync::atomic::Ordering::SeqCst);
        let next = held
            .checked_add(bytes)
            .filter(|value| *value <= self.limit)
            .ok_or(CreatedRefusal::FirstCause)?;
        self.life
            .0
            .held
            .store(next, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
impl novarocks_type_contract::ValueSchemaCreationScope<CreatedRefusal> for RequestScope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.life.clone())
    }
    fn request_layout(&mut self, layout: std::alloc::Layout) -> Result<(), CreatedRefusal> {
        self.admit(
            RequestEvent::Exact(layout.size(), layout.align()),
            layout.size(),
        )
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), CreatedRefusal> {
        self.admit(RequestEvent::Opaque(bytes), bytes)
    }
}
fn created_map_validation(
    catalog: &crate::EngineFunctionCatalog,
    bound: &crate::ResolvedFunctionBinding,
    args: &[FunctionArgument],
    scope: &mut RequestScope,
    control: &dyn PureCompileControl,
) -> Result<(), crate::FunctionBindingCreationFailure<CreatedRefusal>> {
    catalog.validate_frozen_selection_with_creator_in(
        &bound.function_id,
        bound.kind,
        &bound.selected,
        request(args),
        scope,
        control,
    )
}
#[test]
fn fixed_created_validation_uses_original_map_recipe_and_temporary_hold() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = source_map();
    let args = arguments(&source);
    let bound = catalog
        .resolve_bound_user("map_keys", FunctionKind::Scalar, request(&args), &Control)
        .unwrap();
    let (facts, mut scope) = RequestScope::new(None);
    created_map_validation(&catalog, &bound, &args, &mut scope, &Control).unwrap();
    assert!(!facts.requests.lock().unwrap().is_empty());
    assert!(facts.held.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert_eq!(facts.exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    let FunctionResultType::Scalar(output) = &bound.selected.result_type else {
        panic!("scalar result")
    };
    let item = output.typed_binding().borrowed().child_at(0).unwrap();
    assert_eq!(
        item.value_type().kind(),
        &ValueKind::Nominal(ValueLogicalType::Json)
    );
    assert!(item.value_type().nullable());
    let mut wrong = bound.clone();
    wrong.selected.result_type = FunctionResultType::Scalar(FunctionValueType::new(
        output.data_type().clone(),
        output.nullable(),
    ));
    let error = created_map_validation(&catalog, &wrong, &args, &mut scope, &Control).unwrap_err();
    assert!(
        matches!(error,crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::InvalidBinding(ref message)) if message.contains("result type:"))
    );
    drop(scope);
    assert_eq!(facts.exits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    // The original selected result is independent of this validation lifetime.
    assert_eq!(
        output
            .typed_binding()
            .borrowed()
            .child_at(0)
            .unwrap()
            .value_type()
            .kind(),
        &ValueKind::Nominal(ValueLogicalType::Json)
    );
}
#[test]
fn fixed_created_validation_preserves_first_h_and_control_prefix_without_tail() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = source_map();
    let args = arguments(&source);
    let bound = catalog
        .resolve_bound_user("map_keys", FunctionKind::Scalar, request(&args), &Control)
        .unwrap();
    let (facts, mut scope) = RequestScope::new(None);
    let baseline = Trace::default();
    created_map_validation(&catalog, &bound, &args, &mut scope, &baseline).unwrap();
    let requests = facts.requests.lock().unwrap().clone();
    let calls = baseline.calls.lock().unwrap().clone();
    assert!(requests.len() > 2);
    drop(scope);
    for at in [1, requests.len() / 2, requests.len()] {
        let (facts, mut scope) = RequestScope::new(Some(at));
        let control = Trace::default();
        let result = created_map_validation(&catalog, &bound, &args, &mut scope, &control);
        assert!(matches!(
            result,
            Err(crate::FunctionBindingCreationFailure::Creator(
                CreatedRefusal::FirstCause
            ))
        ));
        assert_eq!(*facts.requests.lock().unwrap(), requests[..at]);
        let observed = control.calls.lock().unwrap().clone();
        assert_eq!(observed, calls[..observed.len()]);
        assert!(
            observed.len() < calls.len(),
            "originating creator failure completed a tail"
        );
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [1, calls.len() / 2, calls.len()] {
            let (facts, mut scope) = RequestScope::new(None);
            let control = Trace {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            let result = created_map_validation(&catalog, &bound, &args, &mut scope, &control);
            assert!(
                matches!(result,Err(crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual))) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            drop(scope);
            assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}

#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push((phase, units));
        if let Some((at, cause)) = self.stop.clone() {
            assert!(calls.len() <= at, "control observation after first refusal");
            if calls.len() == at {
                return Err(cause);
            }
        }
        Ok(())
    }
}

fn dynamic_named_arguments() -> [FunctionArgument; 2] {
    [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, false),
            constant: Some(constant_binding_tests::utf8("payload", false)),
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                false,
                ValueLogicalType::Json,
            )
            .unwrap(),
            constant: None,
        },
    ]
}
fn trusted_named_binding(
    catalog: &crate::EngineFunctionCatalog,
    args: &[FunctionArgument],
) -> crate::ResolvedFunctionBinding {
    let function = crate::FunctionId::try_new("builtin.scalar/named_struct/v1").unwrap();
    let overload =
        crate::FunctionOverloadId::try_new("builtin.scalar/named_struct/dynamic-v1").unwrap();
    catalog
        .resolve_bound_trusted_at_overload(
            &function,
            FunctionKind::Scalar,
            &overload,
            request(args),
            &Control,
        )
        .unwrap()
}

#[test]
fn dynamic_created_validation_retains_original_named_and_map_value_domains() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let named = dynamic_named_arguments();
    let key = FunctionValueType::new(DataType::Utf8, false);
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let map = [
        FunctionArgument::Value {
            value_type: declared_list(&key, false).unwrap(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: declared_list(&json, false).unwrap(),
            constant: None,
        },
    ];
    let named_binding = trusted_named_binding(&catalog, &named);
    let map_binding = catalog
        .resolve_bound_user("map", FunctionKind::Scalar, request(&map), &Control)
        .unwrap();
    for (bound, args, child_index) in [
        (named_binding, named.as_slice(), 0),
        (map_binding, map.as_slice(), 1),
    ] {
        let before = bound.selected.clone();
        let (facts, mut scope) = RequestScope::new(None);
        created_map_validation(&catalog, &bound, args, &mut scope, &Control).unwrap();
        assert_eq!(bound.selected, before);
        assert!(!facts.requests.lock().unwrap().is_empty());
        let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
            panic!("scalar result")
        };
        let child = result
            .typed_binding()
            .borrowed()
            .child_at(child_index)
            .unwrap();
        assert_eq!(
            child.value_type().kind(),
            &ValueKind::Nominal(ValueLogicalType::Json)
        );
        assert!(child.value_type().nullable());
        let mut wrong = bound.clone();
        wrong.selected.result_type = FunctionResultType::Scalar(FunctionValueType::new(
            result.data_type().clone(),
            result.nullable(),
        ));
        assert!(matches!(
            created_map_validation(&catalog, &wrong, args, &mut scope, &Control),
            Err(crate::FunctionBindingCreationFailure::Binding(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(facts.exits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
#[test]
fn dynamic_created_publisher_keeps_independent_argument_and_result_holds() {
    let args = dynamic_named_arguments();
    let (_, resolver) = dynamic_definition_parts("named_struct").unwrap();
    let (facts, mut scope) = RequestScope::new(None);
    let selected = resolver
        .resolve_with_creation(request(&args), Some(&mut scope), &Control)
        .unwrap();
    let independent_arguments = selected.argument_types.clone();
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar result")
    };
    let independent_result = result.clone();
    drop(selected);
    drop(scope);
    assert!(facts.held.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert_eq!(facts.exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(independent_arguments);
    assert!(facts.held.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert_eq!(
        independent_result
            .typed_binding()
            .borrowed()
            .child_at(0)
            .unwrap()
            .value_type()
            .kind(),
        &ValueKind::Nominal(ValueLogicalType::Json)
    );
    drop(independent_result);
    assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(facts.exits.load(std::sync::atomic::Ordering::SeqCst), 1);
}
struct RefusalAtControl<'a> {
    inner: RequestScope,
    control: &'a Trace,
    at: Option<usize>,
}
impl novarocks_type_contract::ValueSchemaCreationScope<CreatedRefusal> for RefusalAtControl<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        novarocks_type_contract::ValueSchemaCreationScope::storage_lifetime(&self.inner)
    }
    fn request_layout(&mut self, layout: std::alloc::Layout) -> Result<(), CreatedRefusal> {
        let result = novarocks_type_contract::ValueSchemaCreationScope::request_layout(
            &mut self.inner,
            layout,
        );
        if result.is_err() {
            self.at = Some(self.control.calls.lock().unwrap().len());
        }
        result
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), CreatedRefusal> {
        let result = novarocks_type_contract::ValueSchemaCreationScope::observe_opaque_upper_bound(
            &mut self.inner,
            bytes,
        );
        if result.is_err() {
            self.at = Some(self.control.calls.lock().unwrap().len());
        }
        result
    }
}
#[test]
fn dynamic_created_validation_first_refusal_and_control_have_no_tail() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let args = dynamic_named_arguments();
    let bound = trusted_named_binding(&catalog, &args);
    let (facts, mut scope) = RequestScope::new(None);
    let control = Trace::default();
    created_map_validation(&catalog, &bound, &args, &mut scope, &control).unwrap();
    let requests = facts.requests.lock().unwrap().clone();
    let calls = control.calls.lock().unwrap().clone();
    drop(scope);
    for at in [1, requests.len() / 2, requests.len()] {
        let (facts, inner) = RequestScope::new(Some(at));
        let control = Trace::default();
        let mut scope = RefusalAtControl {
            inner,
            control: &control,
            at: None,
        };
        let error = catalog
            .validate_frozen_selection_with_creator_in(
                &bound.function_id,
                bound.kind,
                &bound.selected,
                request(&args),
                &mut scope,
                &control,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            crate::FunctionBindingCreationFailure::Creator(CreatedRefusal::FirstCause)
        ));
        assert_eq!(*facts.requests.lock().unwrap(), requests[..at]);
        // The exact same prefix is observed through the refused operation;
        // no semantic comparison/footer may observe after first H.
        assert_eq!(
            scope.at,
            Some(control.calls.lock().unwrap().len()),
            "control/footer after actual first H"
        );
        let got = control.calls.lock().unwrap().clone();
        assert_eq!(got, calls[..got.len()]);
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [1, calls.len() / 2, calls.len()] {
            let (facts, mut scope) = RequestScope::new(None);
            let control = Trace {
                calls: Mutex::new(Vec::new()),
                stop: Some((at, cause.clone())),
            };
            let error =
                created_map_validation(&catalog, &bound, &args, &mut scope, &control).unwrap_err();
            assert!(
                matches!(error,crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            drop(scope);
            assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}
#[test]
fn round_created_validation_preserves_declared_scale_and_real_wrapper_loan() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let args = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Decimal128(18, 6), false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: Some(constant_binding_tests::i32(2, false)),
        },
    ];
    for (name, precision) in [("round", 19), ("truncate", 18)] {
        let bound = catalog
            .resolve_bound_user(name, FunctionKind::Scalar, request(&args), &Control)
            .unwrap();
        let (facts, mut scope) = RequestScope::new(None);
        created_map_validation(&catalog, &bound, &args, &mut scope, &Control).unwrap();
        let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
            panic!("scalar result")
        };
        assert_eq!(result.data_type(), &DataType::Decimal128(precision, 6));
        assert!(!result.nullable());
        let mut stale = bound.clone();
        stale.selected.result_type = FunctionResultType::Scalar(FunctionValueType::new(
            DataType::Decimal128(precision, 2),
            false,
        ));
        assert!(matches!(
            created_map_validation(&catalog, &stale, &args, &mut scope, &Control),
            Err(crate::FunctionBindingCreationFailure::Binding(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
        let (facts, mut scope) = RequestScope::new(Some(1));
        let control = Trace::default();
        assert!(matches!(
            created_map_validation(&catalog, &bound, &args, &mut scope, &control),
            Err(crate::FunctionBindingCreationFailure::Creator(
                CreatedRefusal::FirstCause
            ))
        ));
        assert_eq!(facts.requests.lock().unwrap().len(), 1);
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
#[test]
fn dynamic_created_empty_array_keeps_independent_unknown_declaration_and_real_creator() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let item = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let expected = declared_list(&item, false).unwrap();
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: Some(&expected),
    };
    let bound = catalog
        .resolve_bound_user("__array_literal", FunctionKind::Scalar, request, &Control)
        .unwrap();
    let (facts, mut scope) = RequestScope::new(None);
    catalog
        .validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request,
            &mut scope,
            &Control,
        )
        .unwrap();
    // The constructor authors its unknown child independently. An outer
    // expected JSON result cannot supply the constructor's semantic domain.
    let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
        panic!("scalar result")
    };
    assert_eq!(result.child_at(0).unwrap().data_type(), &DataType::Null);
    assert_eq!(
        result.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Physical
    );
    assert!(!result.nullable());
    assert!(bound.selected.argument_types.is_empty());
    let wrong = declared_list(&FunctionValueType::new(DataType::Utf8, true), false).unwrap();
    catalog
        .validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            FunctionBindingRequest {
                expected_result_type: Some(&wrong),
                ..request
            },
            &mut scope,
            &Control,
        )
        .unwrap();
    let mut forged = bound.selected.clone();
    forged.result_type = FunctionResultType::Scalar(expected.clone());
    assert!(matches!(
        catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &forged,
            request,
            &mut scope,
            &Control
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    drop(scope);
    assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn borrowed_aggregate_created_validation_uses_original_recipe_without_expected_backing() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    for (name, source) in [
        ("count", FunctionValueType::new(DataType::Int64, false)),
        ("array_agg", json),
    ] {
        let args = arguments(&source);
        let bound = catalog
            .resolve_bound_user(name, FunctionKind::Aggregate, request(&args), &Control)
            .unwrap();
        let (facts, mut scope) = RequestScope::new(Some(1));
        catalog
            .validate_frozen_selection_with_creator_in(
                &bound.function_id,
                bound.kind,
                &bound.selected,
                request(&args),
                &mut scope,
                &Control,
            )
            .unwrap();
        assert!(facts.requests.lock().unwrap().is_empty());
        let mut wrong = bound.selected.clone();
        let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
            panic!("scalar aggregate result")
        };
        wrong.result_type =
            FunctionResultType::Scalar(result.with_nullable(!result.nullable()).unwrap());
        assert!(matches!(
            catalog.validate_frozen_selection_with_creator_in(
                &bound.function_id,
                bound.kind,
                &wrong,
                request(&args),
                &mut scope,
                &Control,
            ),
            Err(crate::FunctionBindingCreationFailure::Binding(
                FunctionBindingError::InvalidBinding(_)
            ))
        ));
        assert!(facts.requests.lock().unwrap().is_empty());
        drop(scope);
        assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
#[test]
fn borrowed_conversion_created_validation_keeps_exact_domains_and_original_control() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let target = FunctionValueType::new(DataType::LargeBinary, false);
    let args = arguments(&source);
    let req = FunctionBindingRequest {
        expected_result_type: Some(&target),
        ..request(&args)
    };
    let bound = catalog
        .resolve_bound_trusted(
            super::super::value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            req,
            &Control,
        )
        .unwrap();
    let (facts, mut scope) = RequestScope::new(Some(1));
    let trace = Trace::default();
    catalog
        .validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            req,
            &mut scope,
            &trace,
        )
        .unwrap();
    let calls = trace.calls.lock().unwrap().clone();
    assert!(!calls.is_empty());
    assert!(facts.requests.lock().unwrap().is_empty());
    let mut wrong = bound.selected.clone();
    wrong.result_type = FunctionResultType::Scalar(source.clone());
    assert!(matches!(
        catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &wrong,
            req,
            &mut scope,
            &Control,
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [1, calls.len() / 2, calls.len()] {
            let control = Trace {
                calls: Mutex::new(Vec::new()),
                stop: Some((at, cause.clone())),
            };
            assert!(matches!(catalog.validate_frozen_selection_with_creator_in(
                &bound.function_id, bound.kind, &bound.selected, req, &mut scope, &control,
            ), Err(crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual))) if actual == cause));
            assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            assert!(facts.requests.lock().unwrap().is_empty());
        }
    }
    drop(scope);
    assert_eq!(facts.held.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn factory_validation_keeps_original_headers_before_scope_and_primary_refusal_without_tail() {
    use novarocks_type_contract::{ValueSchemaCreationFactory, ValueSchemaScopeBeginFailure};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct RefusingFactory(AtomicUsize);
    impl ValueSchemaCreationFactory<CreatedRefusal> for RefusingFactory {
        type Scope<'a> = RequestScope;
        fn begin_scope(&self) -> Result<RequestScope, CreatedRefusal> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(CreatedRefusal::FirstCause)
        }
        fn begin_scope_observed<O>(
            &self,
            _: &mut dyn FnMut(std::alloc::Layout) -> Result<(), O>,
        ) -> Result<RequestScope, ValueSchemaScopeBeginFailure<O, CreatedRefusal>> {
            self.begin_scope()
                .map_err(ValueSchemaScopeBeginFailure::Creator)
        }
    }
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let args = arguments(&source_map());
    let bound = catalog
        .resolve_bound_user("map_keys", FunctionKind::Scalar, request(&args), &Control)
        .unwrap();
    let factory = RefusingFactory(AtomicUsize::new(0));
    let unknown = crate::FunctionId::try_new("test/unknown/factory-header-v1").unwrap();
    assert!(matches!(
        catalog.validate_frozen_selection_with_factory_in(
            &unknown,
            bound.kind,
            &bound.selected,
            request(&args),
            &factory,
            &Control,
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::UnknownFunction
        ))
    ));
    assert_eq!(factory.0.load(Ordering::SeqCst), 0);
    let mut malformed = bound.selected.clone();
    malformed.argument_types = novarocks_type_contract::FunctionArgumentTypes::default();
    assert!(matches!(
        catalog.validate_frozen_selection_with_factory_in(
            &bound.function_id,
            bound.kind,
            &malformed,
            request(&args),
            &factory,
            &Control,
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(_))
    ));
    assert_eq!(factory.0.load(Ordering::SeqCst), 0);
    let control = Trace::default();
    assert!(matches!(
        catalog.validate_frozen_selection_with_factory_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &factory,
            &control,
        ),
        Err(crate::FunctionBindingCreationFailure::Creator(
            CreatedRefusal::FirstCause
        ))
    ));
    assert_eq!(factory.0.load(Ordering::SeqCst), 1);
    let calls = control.calls.lock().unwrap().clone();
    assert!(!calls.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..calls.len() {
            let factory = RefusingFactory(AtomicUsize::new(0));
            let control = Trace {
                calls: Mutex::new(Vec::new()),
                stop: Some((at + 1, cause.clone())),
            };
            assert!(matches!(catalog.validate_frozen_selection_with_factory_in(
                &bound.function_id, bound.kind, &bound.selected, request(&args), &factory, &control,
            ), Err(crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual))) if actual == cause));
            assert_eq!(factory.0.load(Ordering::SeqCst), 0);
            assert_eq!(*control.calls.lock().unwrap(), calls[..=at]);
        }
    }
}
