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

// Exercise the real Frame copy authors, without Chunk/Slot/global input
// custody. The type creator is the only finite source of this computed value.
use super::*;
use arrow::array::{Int64Array, ListArray, StructArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::Field;
use novarocks_type_contract::{
    FunctionValueType, ValueLayout, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("copy must not wait")
    }
}
struct Creator(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Creator {}
impl Drop for Creator {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn type_with_creator(drops: Option<Arc<AtomicUsize>>) -> FunctionValueType {
    // Every call authors distinct nested Fields, even when the complete types
    // compare equal. The held layout represents their independent creator.
    let ty = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(vec![Arc::new(Field::new("actual_child", DataType::Int64, true))].into()),
        true,
    )));
    let semantic = FunctionValueType::new(ty.clone(), true);
    FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &ty,
            match drops {
                Some(drops) => ValueTypeStorageLifetime::held(Arc::new(Creator(drops))),
                None => ValueTypeStorageLifetime::pure(),
            },
        )
        .unwrap(),
    )
    .unwrap()
}
fn child_buffer(array: &ArrayRef) -> arrow::buffer::Buffer {
    array.to_data().child_data()[0].child_data()[0].buffers()[0].clone()
}
fn computed_array(ty: &FunctionValueType, control: &Control) -> ArrayRef {
    let DataType::List(field) = ty.data_type() else {
        panic!("List fixture")
    };
    let DataType::Struct(fields) = field.data_type() else {
        panic!("Struct fixture")
    };
    let child: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))],
        None,
    ));
    let raw: ArrayRef = Arc::new(ListArray::new(
        field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3, 5])),
        child,
        None,
    ));
    // This is a computed result from the actual assembly author, with no Slot
    // input, record batch, source authority or manually supplied continuation.
    novarocks_functions::control_values::assemble_values(
        novarocks_functions::control_values::AssemblyPlan::Indexed {
            result_type: ty.data_type(),
            sources: &[Some(&raw)],
            choices: &[Some((0, 0)), Some((0, 1)), Some((0, 2))],
        },
        control,
    )
    .unwrap()
}
fn copy_chain(all_true: bool, finite: bool) {
    let a_drops = Arc::new(AtomicUsize::new(0));
    let b_drops = Arc::new(AtomicUsize::new(0));
    let a = type_with_creator(finite.then(|| a_drops.clone()));
    let b = type_with_creator(finite.then(|| b_drops.clone()));
    assert_eq!(a, b);
    if finite {
        assert!(
            !a.typed_binding()
                .layout()
                .shares_storage(b.typed_binding().layout())
        );
    } else {
        assert!(a.typed_binding().layout().storage_lifetime().is_pure());
        assert!(
            a.typed_binding()
                .layout()
                .arrow_backing_storage_lifetime()
                .is_pure()
        );
    }
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("computed-copy-actual-creator");
    let host: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>> = finite.then(|| {
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone())
            as Arc<dyn novarocks_functions::AggregateStateAllocator>
    });
    let observed = ObservedControl {
        original: &Control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let actual_rows = [1, 3, 5];
    let demand_rows = [1, 5];
    let actual = Selection::try_sparse(6, &actual_rows).unwrap();
    let demand = Selection::try_sparse(6, &demand_rows).unwrap();
    let computed = computed_array(&a, &Control);
    let before_copy = child_buffer(&computed);
    let produced = OwnedValue::from_selected(
        SelectedValues::try_new(actual, a.data_type(), computed, Box::default()).unwrap(),
    )
    .retaining_result_type(&a, host.as_ref(), &Control)
    .unwrap();
    let empty = || OwnedValue::Selected(new_empty_array(b.data_type()), Box::default());
    let (yes, no) = if all_true {
        (produced, empty())
    } else {
        (empty(), produced)
    };
    let children = [
        Child {
            ordinals: vec![0, 1, 2],
            value: OwnedValue::Selected(
                Arc::new(BooleanArray::from(vec![all_true; 3])),
                Box::default(),
            ),
        },
        Child {
            ordinals: vec![0, 1, 2],
            value: yes,
        },
        Child {
            ordinals: vec![0, 1, 2],
            value: no,
        },
    ];
    let selected = finish_if(
        &children,
        &[],
        &mut BTreeMap::new(),
        &[Some(all_true); 3],
        false,
        b.data_type(),
        actual,
        &mut work,
    )
    .unwrap();
    let copied = project_owned(selected, &actual_rows, &demand_rows, 6, &mut work)
        .unwrap()
        .retaining_result_type(&b, host.as_ref(), &Control)
        .unwrap();
    let output = copied
        .into_value(demand, &mut work)
        .unwrap()
        .materialize(demand, b.data_type(), &mut work)
        .unwrap();
    let result = output.values().clone();
    let list = result.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 4]);
    let fields = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let values = fields
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(10), Some(20), Some(40), Some(50)]
    );
    let alias = child_buffer(&result);
    assert_ne!(
        before_copy.as_ptr(),
        alias.as_ptr(),
        "this must be an actual take payload copy, not a source alias"
    );
    drop((before_copy, children, a, b, output, host));
    if finite {
        assert_eq!(
            a_drops.load(Ordering::SeqCst),
            0,
            "actual A creator must survive copied output; equal B is insufficient"
        );
        assert_eq!(
            b_drops.load(Ordering::SeqCst),
            0,
            "independent canonical B must also survive"
        );
    }
    let last = result.clone();
    drop(result);
    assert_eq!(a_drops.load(Ordering::SeqCst), 0);
    drop(last);
    assert_eq!(
        a_drops.load(Ordering::SeqCst),
        0,
        "last child Buffer still carries actual A"
    );
    assert_eq!(b_drops.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(a_drops.load(Ordering::SeqCst), usize::from(finite));
    assert_eq!(b_drops.load(Ordering::SeqCst), usize::from(finite));
    assert_eq!(tracker.current(), 0);
    assert_eq!(tracker.allocated(), tracker.deallocated());
}
#[test]
fn computed_all_true_if_take_keeps_actual_a_and_independent_b_until_last_buffer() {
    copy_chain(true, true);
}
#[test]
fn computed_all_false_if_take_keeps_actual_a_and_independent_b_until_last_buffer() {
    copy_chain(false, true);
}
#[test]
fn computed_if_take_pure_types_preserve_no_host_ability() {
    copy_chain(true, false);
    copy_chain(false, false);
}

// A real ScalarV1 wrapper validates exact arguments/result, while this private
// instance returns the selected input alias. It owns no arena or source API.
#[derive(Debug)]
struct AliasPrepared(Arc<novarocks_functions::ScalarCallContract>);
impl novarocks_functions::PreparedScalarKernel for AliasPrepared {
    fn contract(&self) -> &Arc<novarocks_functions::ScalarCallContract> {
        &self.0
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(
        &self,
    ) -> Result<Box<dyn novarocks_functions::ScalarKernelInstance>, KernelFailure> {
        Ok(Box::new(AliasInstance))
    }
}
struct AliasInstance;
impl novarocks_functions::ScalarKernelInstance for AliasInstance {
    fn evaluate<'a>(
        &mut self,
        input: novarocks_functions::ScalarCallInput<'_, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        SelectedValues::try_new(
            input.selection(),
            input.contract().result_type().data_type(),
            input.arguments()[0].array().clone(),
            Box::default(),
        )
        .map_err(KernelFailure::from)
    }
    fn retained_bytes(&self) -> usize {
        0
    }
}
fn alias_prepared(ty: &FunctionValueType) -> Arc<dyn novarocks_functions::PreparedScalarKernel> {
    use novarocks_functions::{
        CallEffectInput, FunctionArgument, FunctionArgumentType, FunctionBindingError,
        FunctionBindingRequest, FunctionBindingSelection, FunctionEffectOwner,
        FunctionEffectOwnerError, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
        ScalarCallContract,
    };
    use novarocks_type_contract::{
        ArgumentControl, CallEffects, CallProofScope, CompileControlError, CompilePhase,
        DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
        ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
        FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
        PureCompileControl, SemanticParameters,
    };
    struct CompileControl;
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    struct Owner(FunctionEffectDeclaration);
    impl FunctionEffectOwner for Owner {
        type Error = FunctionBindingError;
        fn declaration(
            &self,
            _: &FunctionId,
            _: &FunctionBindingSelection,
        ) -> Result<&FunctionEffectDeclaration, Self::Error> {
            Ok(&self.0)
        }
        fn validate_and_refine(
            &self,
            input: CallEffectInput<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
            Ok(CallEffects {
                value_stability: self.0.value_stability,
                own_row_error: self.0.own_row_error,
                failure_behavior: self.0.failure_behavior,
                null_behavior: self.0.null_behavior,
                argument_control: self.0.argument_control,
                instance_state: self.0.instance_state,
                observable_effects: self.0.observable_effects,
                environment: Box::default(),
                proof_scope: input.proof_scope,
            })
        }
    }
    let owner = Owner(FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::ScalarInstance,
        observable_effects: ObservableEffects {
            rng_sampling: false,
            warnings: false,
            controlled_wait: false,
        },
        environment_dependencies: Box::default(),
    });
    let function_id = FunctionId::try_new("fixture/actual-alias/v1").unwrap();
    let selected = Arc::new(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new("fixture/list-alias/v1").unwrap(),
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            vec![FunctionArgumentType::Value(ty.clone())].into_boxed_slice(),
        ),
        result_type: FunctionResultType::Scalar(ty.clone()),
        aggregate: None,
    });
    let arguments = [FunctionArgument::Value {
        value_type: ty.clone(),
        constant: None,
    }];
    let argument_uses = [Some(ExpressionUseId::new(2))];
    let parameters = SemanticParameters::default();
    let input = CallEffectInput {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(1),
            domain: EvaluationDomainId::new(17),
            demand: EvaluationDemand::Value,
        },
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
        function_id: &function_id,
        kind: FunctionKind::Scalar,
        selected: selected.as_ref(),
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: 1,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    let receipt = novarocks_functions::refine_call_effects(&owner, input, &CompileControl).unwrap();
    Arc::new(AliasPrepared(Arc::new(
        ScalarCallContract::from_refined(input, &receipt, selected.clone(), &CompileControl)
            .unwrap(),
    )))
}
#[test]
fn scalar_v1_alias_into_pure_b_then_take_keeps_actual_finite_a() {
    let a_drops = Arc::new(AtomicUsize::new(0));
    let a = type_with_creator(Some(a_drops.clone()));
    let b = type_with_creator(None);
    assert_eq!(a, b);
    assert!(b.typed_binding().layout().storage_lifetime().is_pure());
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("scalar-alias-actual-creator");
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> =
        crate::exec::operators::compiled_aggregate::expression_allocation_host(tracker.clone());
    let observed = ObservedControl {
        original: &Control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let actual_rows = [1, 3, 5];
    let demand_rows = [1, 5];
    let actual = Selection::try_sparse(6, &actual_rows).unwrap();
    let demand = Selection::try_sparse(6, &demand_rows).unwrap();
    let computed = computed_array(&a, &Control);
    let original_buffer = child_buffer(&computed);
    let source = OwnedValue::from_selected(
        SelectedValues::try_new(actual, a.data_type(), computed, Box::default()).unwrap(),
    )
    .retaining_result_type(&a, Some(&host), &Control)
    .unwrap();
    // This is precisely the success boundary used by evaluate_tree: collect
    // finite facts before consuming children, request custody only afterward.
    let finite_children = vec![source.continuation().unwrap()];
    let aliased = {
        let value = source.into_value(actual, &mut work).unwrap();
        let arguments = [value.argument()];
        let mut instance =
            novarocks_functions::ScalarEvaluationInstance::instantiate_with_allocator(
                alias_prepared(&b),
                Some(host.clone()),
            )
            .unwrap();
        let output = instance.evaluate(actual, &arguments, &Control).unwrap();
        assert_eq!(
            output.values().to_data(),
            value.argument().array().to_data()
        );
        OwnedValue::from_selected(output)
    };
    let aliased = aliased
        .retaining_children(finite_children, Some(&host), &Control)
        .unwrap()
        .retaining_result_type(&b, Some(&host), &Control)
        .unwrap();
    let output = project_owned(aliased, &actual_rows, &demand_rows, 6, &mut work)
        .unwrap()
        .into_value(demand, &mut work)
        .unwrap()
        .materialize(demand, b.data_type(), &mut work)
        .unwrap();
    let result = output.values().clone();
    let list = result.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 4]);
    let fields = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        fields
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(10), Some(20), Some(40), Some(50)]
    );
    let alias = child_buffer(&result);
    assert_ne!(alias.as_ptr(), original_buffer.as_ptr());
    drop((output, original_buffer, a, b, host, result));
    assert_eq!(
        a_drops.load(Ordering::SeqCst),
        0,
        "ScalarV1 success must not erase its actual finite input continuation"
    );
    std::thread::spawn(move || drop(alias)).join().unwrap();
    assert_eq!(a_drops.load(Ordering::SeqCst), 1);
    assert_eq!(tracker.current(), 0);
}

fn checked_pool(ty: &FunctionValueType) -> novarocks_functions::ConstantValue {
    struct CompileControl;
    impl novarocks_type_contract::PureCompileControl for CompileControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    let array = computed_array(ty, &Control);
    let policy = novarocks_functions::ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    novarocks_functions::ConstantPool::try_new(
        Arc::new(ty.try_to_field("pool").unwrap()),
        ty.clone(),
        array.to_data(),
        policy,
        novarocks_type_contract::CompilePhase::Validate,
        &CompileControl,
    )
    .unwrap()
    .value(1)
    .unwrap()
}
#[test]
fn constant_nonzero_pool_ordinal_take_keeps_actual_a_even_when_current_b_is_pure() {
    for finite in [true, false] {
        let drops = Arc::new(AtomicUsize::new(0));
        let a = type_with_creator(finite.then(|| drops.clone()));
        let b = type_with_creator(None);
        assert_eq!(a, b);
        let value = checked_pool(&a);
        assert_eq!(value.ordinal(), 1);
        let original = child_buffer(value.pool().array());
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("constant-copy-actual-pool");
        let host: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>> =
            finite.then(|| {
                crate::exec::operators::compiled_aggregate::expression_allocation_host(
                    tracker.clone(),
                ) as Arc<dyn novarocks_functions::AggregateStateAllocator>
            });
        let observed = ObservedControl {
            original: &Control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: None,
        };
        let actual_rows = [1, 3, 5];
        let demand_rows = [1, 5];
        let actual = Selection::try_sparse(6, &actual_rows).unwrap();
        let demand = Selection::try_sparse(6, &demand_rows).unwrap();
        let value = OwnedValue::Constant(value)
            .retaining_result_type(&b, host.as_ref(), &Control)
            .unwrap()
            .into_value(actual, &mut work)
            .unwrap();
        let expanded = value.materialize(actual, b.data_type(), &mut work).unwrap();
        let value = OwnedValue::from_materialized(expanded);
        let output = project_owned(value, &actual_rows, &demand_rows, 6, &mut work)
            .unwrap()
            .into_value(demand, &mut work)
            .unwrap()
            .materialize(demand, b.data_type(), &mut work)
            .unwrap();
        let result = output.values().clone();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_offsets(), &[0, 1, 2]);
        let fields = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            fields
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(30), Some(30)]
        );
        let last = child_buffer(&result);
        assert_ne!(last.as_ptr(), original.as_ptr());
        drop((original, a, b, output, result, host));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "the actual finite checked pool A must follow copied output"
        );
        std::thread::spawn(move || drop(last)).join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), usize::from(finite));
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn finite_constant_needs_host_only_when_its_actual_array_is_copied() {
    let drops = Arc::new(AtomicUsize::new(0));
    let a = type_with_creator(Some(drops.clone()));
    let b = type_with_creator(None);
    let constant = checked_pool(&a);
    let observed = ObservedControl {
        original: &Control,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work = Work {
        control: &observed,
        pending: 0,
        scalar_scope: None,
    };
    let value = OwnedValue::Constant(constant)
        .retaining_result_type(&b, None, &Control)
        .unwrap()
        .into_value(Selection::all(2), &mut work)
        .unwrap();
    assert!(
        matches!(value.argument(), EvaluatedArgument::Constant(_)),
        "borrowing the formal checked Constant must not eagerly require a new host"
    );
    let error = match value.materialize(Selection::all(2), b.data_type(), &mut work) {
        Ok(_) => panic!("finite copy requires actual host"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        invalid("compiled finite Array source requires its actual allocation host")
    );
    drop((a, b));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
