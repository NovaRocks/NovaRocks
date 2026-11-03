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
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    EvaluatedArgument, FunctionArgument, FunctionResultType, FunctionValueType,
    InvocationScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    specialize_invocation_scalar,
};
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};
fn owner() -> JsonCanonicalOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(name, _)| name == crate::json_canonical::FUNCTION_NAME)
        .unwrap()
        .1;
    let (decl, resolver) = super::super::catalogue::scalar_definition_parts(
        crate::json_canonical::FUNCTION_NAME,
        &signatures,
        FunctionKind::Scalar,
    )
    .unwrap();
    JsonCanonicalOwner::new(decl, resolver).unwrap()
}
fn args(nullable: bool, logical: ValueLogicalType) -> [FunctionArgument; 1] {
    [FunctionArgument::Value {
        value_type: FunctionValueType::try_with_logical_type(DataType::Utf8, nullable, logical)
            .unwrap(),
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
#[derive(Default)]
struct Host {
    owned: AtomicUsize,
    opaque: AtomicUsize,
}
impl crate::AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        let p = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        self.owned.fetch_add(layout.size(), Ordering::SeqCst);
        Ok(p)
    }
    unsafe fn release(&self, p: NonNull<u8>, layout: Layout) {
        self.owned.fetch_sub(layout.size(), Ordering::SeqCst);
        unsafe { std::alloc::dealloc(p.as_ptr(), layout) };
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.opaque.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let before = self.opaque.fetch_sub(bytes, Ordering::SeqCst);
        assert!(before >= bytes);
    }
}
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("JSON canonicalizer does not wait")
    }
}
fn instance(host: Arc<Host>) -> InvocationScalarEvaluationInstance {
    let owner = owner();
    let args = args(true, ValueLogicalType::Json);
    let control = crate::binding_test_control();
    let selected = Arc::new(owner.resolve(request(&args), control).unwrap());
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = [Some(ExpressionUseId::new(42))];
    let specialized = specialize_invocation_scalar(
        &owner,
        CallEffectInput {
            context,
            argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
            function_id: owner.declaration.function_id(),
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: request(&args),
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        },
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        control,
    )
    .unwrap();
    let effects = specialized.effects().for_use(context).unwrap();
    assert!(effects.may_raise_invocation_data);
    assert!(!effects.permits_boolean_reordering());
    assert!(!effects.permits_earlier_evaluation());
    InvocationScalarEvaluationInstance::instantiate_with_allocator(
        specialized.into_prepared(),
        Some(host),
    )
    .unwrap()
}
#[test]
fn canonical_key_hidden_exact_json_profile_and_nullability() {
    let catalogue = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let owner = owner();
    let control = crate::binding_test_control();
    assert_eq!(
        catalogue
            .definition_by_id(owner.declaration.function_id())
            .unwrap()
            .visibility(),
        FunctionVisibility::Hidden
    );
    for nullable in [false, true] {
        let args = args(nullable, ValueLogicalType::Json);
        assert!(matches!(
            catalogue.resolve_bound_user(
                crate::json_canonical::FUNCTION_NAME,
                FunctionKind::Scalar,
                request(&args),
                control
            ),
            Err(FunctionBindingError::HiddenFunction)
        ));
        let binding = catalogue
            .resolve_bound_trusted(
                crate::json_canonical::FUNCTION_NAME,
                FunctionKind::Scalar,
                request(&args),
                control,
            )
            .unwrap();
        let FunctionResultType::Scalar(result) = binding.selected.result_type else {
            panic!("scalar result")
        };
        assert_eq!(result.data_type(), &DataType::Utf8);
        assert_eq!(result.logical_type(), ValueLogicalType::Physical);
        assert_eq!(result.nullable(), nullable);
    }
    let physical = args(true, ValueLogicalType::Physical);
    assert!(
        catalogue
            .resolve_bound_trusted(
                crate::json_canonical::FUNCTION_NAME,
                FunctionKind::Scalar,
                request(&physical),
                control
            )
            .is_err()
    );
}
#[test]
fn canonical_selected_sparse_and_last_buffer_alias_keep_original_host() {
    let host = Arc::new(Host::default());
    let mut kernel = instance(host.clone());
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unselected invalid"),
        Some(r#"{"b":1,"a":2}"#),
        None,
        Some("1e2"),
    ]));
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let arguments = [EvaluatedArgument::Column(&source)];
    let out = kernel
        .evaluate(
            selection,
            &arguments,
            crate::ScalarInvocationActivation::Activated,
            &Control,
        )
        .unwrap();
    let array = out.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        array.iter().collect::<Vec<_>>(),
        vec![Some(r#"{"a":2,"b":1}"#), None, Some("1E+2")]
    );
    let alias = array.values().clone();
    drop(out);
    drop(kernel);
    assert!(host.opaque.load(Ordering::SeqCst) > 0);
    drop(alias);
    assert_eq!(host.opaque.load(Ordering::SeqCst), 0);
    assert_eq!(host.owned.load(Ordering::SeqCst), 0);
}
#[test]
fn canonical_invalid_json_preserves_data_and_failed_instance_lifecycle() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let host = Arc::new(Host::default());
    let mut kernel = instance(host.clone());
    let source: ArrayRef = Arc::new(StringArray::from(vec![Some("not JSON")]));
    let failure = kernel
        .evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Column(&source)],
            crate::ScalarInvocationActivation::Activated,
            &Control,
        )
        .unwrap_err();
    let crate::ScalarInvocationFailure::Kernel(cause) = &failure else {
        panic!("canonical malformed JSON preserves its original typed Kernel Data");
    };
    assert_eq!(cause.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        kernel
            .evaluate(
                Selection::all(1),
                &[EvaluatedArgument::Column(&source)],
                crate::ScalarInvocationActivation::Activated,
                &Control
            )
            .unwrap_err(),
        crate::ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    drop(kernel);
    drop(failure);
    assert_eq!(host.opaque.load(Ordering::SeqCst), 0);
    assert_eq!(host.owned.load(Ordering::SeqCst), 0);
}

#[test]
fn direct_helper_uses_original_opaque_journal_and_releases_it_with_the_key() {
    let host = Arc::new(Host::default());
    let key = crate::json_canonical::normalize_json_key("1", host.clone(), &Control).unwrap();
    let journal = crate::aggregate_helper_memory::private_mutex_backing_bytes().unwrap();
    assert_eq!(host.opaque.load(Ordering::SeqCst), journal);
    drop(key);
    assert_eq!(host.opaque.load(Ordering::SeqCst), 0);
    assert_eq!(host.owned.load(Ordering::SeqCst), 0);
}
#[test]
fn opaque_journal_refusal_precedes_first_owned_allocation_and_preserves_cause() {
    struct Refuse;
    impl crate::AggregateStateAllocator for Refuse {
        fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
            Some(self)
        }
        fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
            panic!("owned allocation must not precede refused journal admission")
        }
        unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
            panic!("no allocation was admitted")
        }
    }
    impl OpaqueAllocationHost for Refuse {
        fn reserve_opaque(&self, _: usize) -> Result<(), KernelFailure> {
            Err(KernelFailure::DeadlineExceeded)
        }
        fn release_opaque(&self, bytes: usize) {
            assert_eq!(bytes, 0, "refused journal was not admitted");
        }
    }
    let host: Arc<dyn crate::AggregateStateAllocator> = Arc::new(Refuse);
    assert!(matches!(
        crate::json_canonical::normalize_json_key("1", host.clone(), &Control),
        Err(KernelFailure::DeadlineExceeded)
    ));
}
