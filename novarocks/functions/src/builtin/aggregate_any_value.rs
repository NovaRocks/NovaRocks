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

//! Exact nullable-result ANY_VALUE with allocation-tracked shared storage.
use super::aggregate_any_value_core::AnyValueState;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{self as scalar, ScalarWork};
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use std::sync::Arc;
#[derive(Debug)]
pub(super) struct AnyValueKernel {
    pub(super) contract: Arc<AggregateCallContract>,
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    f: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = f(&mut work);
    let result = work.finish_result(result);
    observation.finish(result)
}
impl AnyValueKernel {
    fn output_type(&self) -> &DataType {
        let FunctionResultType::Scalar(output) = &self.contract.call().selected().result_type
        else {
            unreachable!("validated result")
        };
        output.data_type()
    }
    fn build<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s AnyValueState<HostAggregateAllocator>>,
    {
        observed(control, |work| {
            let expected = states.len();
            let mut values = Vec::new();
            values.try_reserve_exact(expected).map_err(|source| {
                KernelFailure::from_domain(crate::allocation_failure::AllocationRefusal::new(
                    "any_value emission values",
                    source,
                ))
            })?;
            let mut work = ScalarWork::new(Some(work));
            for state in states {
                if values.len() == expected {
                    return Err(internal("any_value state iterator exceeded its extent"));
                }
                if state.failed {
                    return Err(KernelFailure::InstanceFailed);
                }
                work.step()?;
                values.push(state.output(&mut work)?);
            }
            if values.len() != expected {
                return Err(internal("any_value state iterator shortened its extent"));
            }
            scalar::build_scalar_array(self.output_type(), values, &mut work)
        })
    }
}
impl PreparedAggregateKernel for AnyValueKernel {
    fn clone_for_local_phase(
        &self,
        contract: Arc<AggregateCallContract>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Arc<Self>, KernelFailure> {
        control
            .checkpoint(
                novarocks_type_contract::CompilePhase::FunctionSpecialization,
                0,
            )
            .map_err(crate::kernel_control::compile_failure)?;
        Ok(Arc::new(Self { contract }))
    }
    type State = AnyValueState<HostAggregateAllocator>;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::AllocationTracked
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        state.allocator.metadata_bytes() + state.retained_bytes()
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "allocation-tracked any_value requires a host allocator",
        ))
    }
    fn create_state_with_allocator(
        &self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            let allocator = allocator
                .ok_or_else(|| invalid("allocation-tracked any_value requires a host allocator"))?;
            work.step()?;
            Ok(AnyValueState::new(HostAggregateAllocator::try_new(
                allocator,
            )?))
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: Self::PreparedUpdateBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || !self.contract.phase().consumes_logical_arguments()
                || input.logical_arguments().len() != 1
                || !input.order_arguments().is_empty()
            {
                return Err(invalid(
                    "any_value update differs from its exact phase or channels",
                ));
            }
            Ok(input)
        })
    }
    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("any_value selected ordinal is out of bounds"))?;
            work.step()?;
            if state.has_value {
                return Ok(());
            }
            let argument = input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("any_value selected address is out of bounds"));
            }
            state.update_from_array(argument.array(), address, &mut ScalarWork::new(Some(work)))
        });
        if result.is_err() {
            state.latch_failure();
        }
        result
    }
    fn prepare_merge<'a>(
        &'a self,
        input: Self::PreparedMergeBatch<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        observed(control, |work| {
            work.step()?;
            if !std::ptr::eq(input.contract(), self.contract.as_ref())
                || self.contract.phase().consumes_logical_arguments()
                || input.state().array().data_type() != self.output_type()
            {
                return Err(invalid(
                    "any_value merge differs from its exact phase or value state",
                ));
            }
            Ok(input)
        })
    }
    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        input: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if state.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work| {
            let row = input
                .selection()
                .row(ordinal)
                .ok_or_else(|| invalid("any_value merge ordinal is out of bounds"))?;
            work.step()?;
            if state.has_value {
                return Ok(());
            }
            let argument = input.state();
            let address = argument.value_row(ordinal, row);
            if address >= argument.array().len() {
                return Err(internal("any_value merge address is out of bounds"));
            }
            state.update_from_array(argument.array(), address, &mut ScalarWork::new(Some(work)))
        });
        if result.is_err() {
            state.latch_failure();
        }
        result
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.build(states, control)
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s Self::State>,
    {
        self.build(states, control)
    }
}
pub(super) fn supported(
    ty: &DataType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step().map_err(compile_failure)?;
    Ok(match ty {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => true,
        DataType::FixedSizeBinary(16) => true,
        DataType::List(item) => supported(item.data_type(), work)?,
        DataType::Struct(fields) => {
            for field in fields {
                if !supported(field.data_type(), work)? {
                    return Ok(false);
                }
            }
            true
        }
        DataType::Map(field, _) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Ok(false);
            };
            if fields.len() != 2 {
                return Ok(false);
            };
            supported(fields[0].data_type(), work)? && supported(fields[1].data_type(), work)?
        }
        _ => false,
    })
}
/// Compare the original nullable result/state law against complete borrowed
/// bindings. Only the source root NULL is replaced; every child promise,
/// nominal domain and exact physical layout remains part of the comparison.
pub(super) fn matches_nullable_source(
    source: &FunctionValueType,
    actual: &FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    work.step().map_err(compile_failure)?;
    if !actual.nullable() {
        return Ok(false);
    }
    source.same_physical_binding_ignoring_root_nullability_observed(actual, || {
        work.step().map_err(compile_failure)
    })
}

/// The fixed Binary state is the same canonical recipe used by the aggregate
/// type author. This comparison owns neither an expected FVT nor a layout.
pub(super) fn matches_nullable_binary(
    actual: &FunctionValueType,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    crate::aggregate_types::AggregateTypeRecipe::Scalar(DataType::Binary)
        .matches_value_observed(
            actual.typed_binding().borrowed(),
            true,
            novarocks_type_contract::ValueLogicalType::Physical,
            work,
        )
        .map_err(|error| match error {
            FunctionBindingError::Control(cause) => compile_failure(cause),
            _ => invalid("aggregate canonical Binary comparison failed"),
        })
}

pub(super) fn validate_contract(
    contract: &AggregateCallContract,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    let [FunctionArgumentType::Value(source)] = contract.call().selected().argument_types.as_ref()
    else {
        return Err(invalid("any_value requires exactly one value argument"));
    };
    let FunctionResultType::Scalar(output) = &contract.call().selected().result_type else {
        return Err(invalid("any_value requires a scalar result"));
    };
    if !output.nullable() || !supported(source.data_type(), work)? {
        return Err(invalid(&format!(
            "{} has no installed nullable any_value input profile for {:?}",
            contract.call().function_id().as_str(),
            source
        )));
    }
    // The original result author changes only root NULL for empty groups.
    // Borrow every semantic child and layout fact instead of publishing a
    // nullable expected node that preparation immediately discards.
    if !matches_nullable_source(source, output, work)?
        || !matches_nullable_source(source, contract.intermediate_type(), work)?
    {
        return Err(invalid(
            "any_value selected source, result and intermediate must be the same exact value type",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::aggregate_scalar::AggScalarValue as V;
    use arrow_schema::{Field, Fields};
    use novarocks_type_contract::{
        CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, ExpressionUseId,
        PureCompileControl, SemanticParameters,
    };
    use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};

    struct Compile;
    impl PureCompileControl for Compile {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            Ok(())
        }
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
                assert!(at <= *stop, "callback after primary refusal");
            }
            trace.push(units);
            match &self.refusal {
                Some((stop, cause)) if *stop == at => Err(cause.clone()),
                _ => Ok(()),
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("ANY_VALUE never waits")
        }
    }
    #[derive(Default)]
    struct Ledger {
        attempts: usize,
        bytes: usize,
        live: Vec<(usize, Layout)>,
        metadata: Option<(usize, Layout)>,
    }
    #[derive(Default)]
    struct Host {
        ledger: Mutex<Ledger>,
        refusal: Mutex<Option<(usize, KernelFailure)>>,
    }
    impl AggregateStateAllocator for Host {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            assert_ne!(layout.size(), 0);
            let mut ledger = self.ledger.lock().unwrap();
            let at = ledger.attempts;
            ledger.attempts += 1;
            if let Some((stop, cause)) = &*self.refusal.lock().unwrap()
                && *stop == at
            {
                return Err(cause.clone());
            }
            let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
                .ok_or(KernelFailure::ResourceExhausted)?;
            ledger.bytes += layout.size();
            let block = (pointer.as_ptr().addr(), layout);
            if ledger.metadata.is_none() {
                ledger.metadata = Some(block);
            }
            ledger.live.push(block);
            Ok(pointer)
        }
        unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
            let mut ledger = self.ledger.lock().unwrap();
            let at = ledger
                .live
                .iter()
                .position(|(address, actual)| {
                    *address == pointer.as_ptr().addr() && *actual == layout
                })
                .expect("exact block released once");
            ledger.live.swap_remove(at);
            ledger.bytes -= layout.size();
            unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
        }
    }
    fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
        let next = host.ledger.lock().unwrap().attempts;
        *host.refusal.lock().unwrap() = Some((next + offset, cause));
    }
    fn assert_metadata_only(host: &Host) {
        let ledger = host.ledger.lock().unwrap();
        let metadata = ledger
            .metadata
            .expect("successful constructor allocated metadata");
        assert_eq!(ledger.live.as_slice(), &[metadata]);
        assert_eq!(ledger.bytes, metadata.1.size());
    }
    fn causes() -> [KernelFailure; 7] {
        [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            invalid("host original"),
            internal("host original"),
            KernelFailure::Operational(KernelDiagnostic::new("host original")),
            KernelFailure::InstanceFailed,
        ]
    }
    fn kernel(source: FunctionValueType) -> AnyValueKernel {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let args = [FunctionArgument::Value {
            value_type: source,
            constant: None,
        }];
        let request = FunctionBindingRequest {
            arguments: &args,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let bound = catalog
            .resolve_bound_user("any_value", FunctionKind::Aggregate, request, &Compile)
            .unwrap();
        let selected = Arc::new(bound.selected);
        let context = novarocks_type_contract::RelationEffectContext::call(
            novarocks_type_contract::RelationCallId::new(331),
        );
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(332))];
        let prepared = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    function_id: &bound.function_id,
                    kind: FunctionKind::Aggregate,
                    selected: &selected,
                    request,
                    argument_uses: CallArgumentUses::SelectedChannels(&uses),
                    context: context.into(),
                    parameters: &parameters,
                    environment: &[],
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                selected.clone(),
                PureCallPreparation::Aggregate {
                    arguments: ScopedRelationalEffects::pure_value(context),
                    options: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Single,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                },
                &Compile,
            )
            .unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("aggregate handle")
        };
        AnyValueKernel {
            contract: handle.contract().clone(),
        }
    }
    #[test]
    fn borrowed_prepare_preserves_complete_nullable_source_and_fixed_state() {
        let source = FunctionValueType::new(
            DataType::Struct(Fields::from(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            false,
        );
        // The genuine installed owner is still prepared from the original
        // full source. The borrowed checker sees its actual final/state ports.
        let prepared = kernel(source.clone());
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &Compile,
            CompilePhase::FunctionSpecialization,
        )
        .unwrap();
        validate_contract(&prepared.contract, &mut work).unwrap();
        assert!(
            matches_nullable_source(&source, prepared.contract.final_type(), &mut work).unwrap()
        );
        assert!(!matches_nullable_source(&source, &source, &mut work).unwrap());
        let wrong_child_null = FunctionValueType::new(
            DataType::Struct(Fields::from(vec![Field::new(
                "value",
                DataType::Utf8,
                true,
            )])),
            true,
        );
        assert!(!matches_nullable_source(&source, &wrong_child_null, &mut work).unwrap());
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            false,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let text = FunctionValueType::new(DataType::Utf8, true);
        assert!(!matches_nullable_source(&json, &text, &mut work).unwrap());
        let json_child =
            super::super::signature::declared_struct(vec![("value".into(), json)], false).unwrap();
        let text_child =
            super::super::signature::declared_struct(vec![("value".into(), text)], true).unwrap();
        assert_eq!(json_child.data_type(), text_child.data_type());
        assert!(!matches_nullable_source(&json_child, &text_child, &mut work).unwrap());
        assert!(
            matches_nullable_binary(&FunctionValueType::new(DataType::Binary, true), &mut work)
                .unwrap()
        );
        assert!(
            !matches_nullable_binary(&FunctionValueType::new(DataType::Binary, false), &mut work)
                .unwrap()
        );
        let opaque = FunctionValueType::try_with_logical_type(
            DataType::Binary,
            true,
            novarocks_type_contract::ValueLogicalType::Percentile,
        )
        .unwrap();
        assert!(!matches_nullable_binary(&opaque, &mut work).unwrap());
        work.finish().unwrap();
    }

    #[test]
    fn borrowed_prepare_complete_comparison_keeps_three_primary_causes_without_tail() {
        struct Observe {
            trace: Mutex<Vec<u32>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Observe {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let mut trace = self.trace.lock().unwrap();
                let at = trace.len();
                if let Some((stop, _)) = &self.refusal {
                    assert!(at <= *stop, "callback after original preparation refusal");
                }
                trace.push(units);
                match &self.refusal {
                    Some((stop, cause)) if *stop == at => Err(cause.clone()),
                    _ => Ok(()),
                }
            }
        }
        let source = FunctionValueType::new(
            DataType::Struct(Fields::from(
                (0..320)
                    .map(|index| Field::new(format!("field{index}"), DataType::Utf8, false))
                    .collect::<Vec<_>>(),
            )),
            false,
        );
        let target = source.with_nullable(true).unwrap();
        let compare = |control: &Observe| {
            let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                control,
                CompilePhase::FunctionSpecialization,
            )
            .map_err(compile_failure)?;
            let same = matches_nullable_source(&source, &target, &mut work)?;
            work.finish().map_err(compile_failure)?;
            Ok::<_, KernelFailure>(same)
        };
        let success = Observe {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        };
        assert!(compare(&success).unwrap());
        let trace = success.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in 0..trace.len() {
                let control = Observe {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    compare(&control).unwrap_err(),
                    compile_failure(cause.clone())
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }

    fn nested() -> (FunctionValueType, ArrayRef) {
        let ty = DataType::Struct(Fields::from(vec![
            Field::new("first", DataType::Utf8, false),
            Field::new("second", DataType::Utf8, true),
        ]));
        let values = scalar::build_scalar_array(
            &ty,
            vec![Some(V::Struct(vec![
                Some(V::Utf8("a".into())),
                Some(V::Utf8("z".repeat(513))),
            ]))],
            &mut ScalarWork::new(None),
        )
        .unwrap();
        (
            FunctionValueType::from_legacy_declared_projection(ty, false),
            values,
        )
    }
    fn update(
        kernel: &AnyValueKernel,
        state: &mut AnyValueState<HostAggregateAllocator>,
        values: &ArrayRef,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let args = [EvaluatedArgument::Column(values)];
        let setup = Control::default();
        let input = SelectedAggregateUpdateInput::try_new(
            &kernel.contract,
            Selection::all(1),
            &args,
            &[],
            &setup,
        )
        .unwrap();
        let input = kernel.prepare_update(input, &setup).unwrap();
        kernel.update_row(state, &input, 0, control)
    }
    fn assert_released(host: &Host) {
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.bytes, 0);
        assert!(ledger.live.is_empty());
    }
    #[test]
    fn any_value_missing_host_is_explicit_and_construct_refusal_stays_typed() {
        let kernel = kernel(FunctionValueType::new(DataType::Utf8, false));
        for absent in [
            kernel.create_state(&Control::default()),
            kernel.create_state_with_allocator(None, &Control::default()),
        ] {
            assert!(
                matches!(absent, Err(KernelFailure::InvalidProgram(error)) if error.message() == "allocation-tracked any_value requires a host allocator")
            );
        }
        for cause in causes() {
            for stop in [0, 1] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                let host = Arc::new(Host::default());
                assert!(
                    matches!(kernel.create_state_with_allocator(Some(host.clone()), &control), Err(error) if error == cause)
                );
                assert_released(&host);
            }
        }
    }
    #[test]
    fn any_value_actual_recursive_allocation_refusal_is_atomic_and_keeps_typed_cause() {
        let (source, values) = nested();
        let kernel = kernel(source);
        // Root recursive vector, first UTF8 child, then second UTF8 child.
        for stop in [0, 1, 2] {
            for cause in causes() {
                let host = Arc::new(Host::default());
                let mut state = kernel
                    .create_state_with_allocator(Some(host.clone()), &Control::default())
                    .unwrap();
                let initial = host.ledger.lock().unwrap().attempts;
                arm_refusal(&host, stop, cause.clone());
                assert_eq!(
                    update(&kernel, &mut state, &values, &Control::default()),
                    Err(cause)
                );
                assert!(state.failed);
                assert!(!state.has_value);
                assert!(state.value.is_none());
                assert_eq!(
                    kernel.retained_bytes(&state),
                    state.allocator.metadata_bytes()
                );
                assert_eq!(host.ledger.lock().unwrap().attempts, initial + stop + 1);
                assert_metadata_only(&host);
                let attempts = host.ledger.lock().unwrap().attempts;
                assert_eq!(
                    update(&kernel, &mut state, &values, &Control::default()),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
                assert!(matches!(
                    kernel.build_final(std::iter::once(&state), &Control::default()),
                    Err(KernelFailure::InstanceFailed)
                ));
                drop(state);
                assert_released(&host);
            }
        }
    }
    #[test]
    fn any_value_every_recursive_update_control_refusal_releases_and_latches() {
        let (source, values) = nested();
        let kernel = kernel(source);
        let host = Arc::new(Host::default());
        let control = Control::default();
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update(&kernel, &mut state, &values, &control).unwrap();
        let steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        assert_eq!(
            kernel.retained_bytes(&state),
            host.ledger.lock().unwrap().bytes
        );
        let attempts = host.ledger.lock().unwrap().attempts;
        update(&kernel, &mut state, &values, &Control::default()).unwrap();
        assert_eq!(host.ledger.lock().unwrap().attempts, attempts);
        drop(state);
        assert_released(&host);
        for cause in causes() {
            for stop in 0..steps {
                let host = Arc::new(Host::default());
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                let mut state = kernel
                    .create_state_with_allocator(Some(host.clone()), &Control::default())
                    .unwrap();
                assert_eq!(
                    update(&kernel, &mut state, &values, &control),
                    Err(cause.clone())
                );
                assert!(state.failed);
                assert!(!state.has_value);
                assert_eq!(
                    kernel.retained_bytes(&state),
                    state.allocator.metadata_bytes()
                );
                assert_metadata_only(&host);
                assert_eq!(
                    update(&kernel, &mut state, &values, &Control::default()),
                    Err(KernelFailure::InstanceFailed)
                );
                drop(state);
                assert_released(&host);
            }
        }
    }
    #[test]
    fn any_value_every_recursive_output_control_refusal_keeps_original_and_storage() {
        let (source, values) = nested();
        let kernel = kernel(source);
        let host = Arc::new(Host::default());
        let mut state = kernel
            .create_state_with_allocator(Some(host.clone()), &Control::default())
            .unwrap();
        update(&kernel, &mut state, &values, &Control::default()).unwrap();
        let control = Control::default();
        let output = kernel
            .build_final(std::iter::once(&state), &control)
            .unwrap();
        assert_eq!(output.len(), 1);
        let steps = control.trace.lock().unwrap().len();
        assert!(control.trace.lock().unwrap().contains(&256));
        let stable = host.ledger.lock().unwrap().bytes;
        for cause in causes() {
            for stop in 0..steps {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(kernel.build_final(std::iter::once(&state), &control), Err(error) if error == cause)
                );
                assert_eq!(host.ledger.lock().unwrap().bytes, stable);
                assert!(!state.failed);
            }
        }
        drop(state);
        assert_released(&host);
    }
    #[test]
    fn any_value_actual_metadata_constructor_refusal_is_typed_and_never_published() {
        let kernel = kernel(FunctionValueType::new(DataType::Utf8, false));
        for cause in causes() {
            let host = Arc::new(Host::default());
            arm_refusal(&host, 0, cause.clone());
            assert!(
                matches!(kernel.create_state_with_allocator(Some(host.clone()), &Control::default()), Err(error) if error == cause)
            );
            assert_eq!(host.ledger.lock().unwrap().attempts, 1);
            assert_released(&host);
        }
    }
}
