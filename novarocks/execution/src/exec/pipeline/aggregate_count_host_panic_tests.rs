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

//! Actual installed COUNT Final -> erased state -> production driver panic owner.
//! The test allocator measures real blocks; it grants no invocation capacity.
use super::*;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{ArrayRef, Int64Array};
use novarocks_functions as f;
use novarocks_type_contract as t;
use std::{alloc::Layout, num::NonZeroUsize, ptr::NonNull};

struct Control;
impl t::PureCompileControl for Control {
    fn checkpoint(&self, _: t::CompilePhase, _: u32) -> Result<(), t::CompileControlError> {
        Ok(())
    }
}
impl f::KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), f::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), f::KernelFailure> {
        panic!("COUNT never waits")
    }
}
#[derive(Default)]
struct Ledger {
    live: AtomicUsize,
    releases: AtomicUsize,
}
impl f::AggregateStateAllocator for Ledger {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, f::KernelFailure> {
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(f::KernelFailure::ResourceExhausted)?;
        self.live.fetch_add(layout.size(), Ordering::AcqRel);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe {
            std::alloc::dealloc(pointer.as_ptr(), layout);
        }
        self.live.fetch_sub(layout.size(), Ordering::AcqRel);
        self.releases.fetch_add(1, Ordering::AcqRel);
    }
}
fn prepare_final() -> f::PreparedAggregateHandle {
    let catalog = f::builtin::catalogue::builtin_engine_function_catalog();
    let resolved = catalog
        .resolve_bound_user(
            "count",
            t::FunctionKind::Aggregate,
            f::FunctionBindingRequest {
                arguments: &[],
                logical_argument_count: 0,
                expected_result_type: None,
            },
            &Control,
        )
        .unwrap();
    let selected = Arc::new(resolved.selected);
    let context = t::ExpressionEffectContext {
        use_id: t::ExpressionUseId::new(31),
        domain: t::EvaluationDomainId::new(9),
        demand: t::EvaluationDemand::Value,
    };
    let state_context = t::ExpressionEffectContext {
        use_id: t::ExpressionUseId::new(32),
        domain: t::EvaluationDomainId::new(10),
        demand: t::EvaluationDemand::Value,
    };
    let state_type = t::FunctionValueType::new(arrow::datatypes::DataType::Int64, true);
    let parameters = t::SemanticParameters::try_new([]).unwrap();
    let input = f::CallEffectInput {
        context,
        argument_uses: f::CallArgumentUses::AggregateMerge {
            phase: f::AggregateKernelPhase::Final,
            state_context,
            state_input_type: &state_type,
        },
        function_id: &resolved.function_id,
        kind: t::FunctionKind::Aggregate,
        selected: &selected,
        request: f::FunctionBindingRequest {
            arguments: &[],
            logical_argument_count: 0,
            expected_result_type: None,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: t::DecimalOverflowPolicy::OutputNull,
        proof_scope: t::CallProofScope::Domain(context.domain),
    };
    let result = catalog
        .prepare_fresh_selected(
            input,
            selected.clone(),
            f::PureCallPreparation::Aggregate {
                arguments: f::ScopedExpressionEffects::pure_value(context),
                options: f::AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: f::AggregateKernelPhase::Final,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: Some(state_type.clone()),
                },
            },
            &Control,
        )
        .unwrap();
    match result.into_prepared() {
        f::PreparedPureKernel::Aggregate(handle) => handle,
        _ => panic!("COUNT actual aggregate ABI"),
    }
}
struct CountMergeSource {
    handle: f::PreparedAggregateHandle,
    ledger: Arc<Ledger>,
    activations: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl OperatorLifecycle for CountMergeSource {
    fn name(&self) -> &str {
        "original_count_merge_overflow"
    }
    fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
        self.activations.fetch_add(1, Ordering::AcqRel);
        let mut states = f::AggregateStateColumn::try_new(
            self.handle.clone(),
            self.ledger.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        states.push(&Control).unwrap();
        assert_eq!(self.ledger.live.load(Ordering::Acquire), size_of::<i64>());
        let values: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1]));
        let input = f::SelectedAggregateMergeInput::try_new(
            self.handle.contract(),
            f::Selection::all(2),
            f::EvaluatedArgument::Column(&values),
            &Control,
        )
        .unwrap();
        let mapping = [0, 0];
        let mut invocation = states
            .prepare_merge_batch(&mapping, input, &Control)
            .unwrap();
        // The production executor, rather than this operator, owns the unwind.
        invocation.run(&Control).unwrap();
        Ok(())
    }
}
impl crate::exec::pipeline::shape::SourceKernel for CountMergeSource {
    fn poll_output(
        &mut self,
        _: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
        panic!("the activation failure must prevent payload work");
    }
}
impl Drop for CountMergeSource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
#[test]
#[cfg(debug_assertions)]
fn original_count_overflow_real_driver_owner_stops_without_replay_and_releases_backing() {
    let executor = GlobalDriverExecutor::new(1);
    let ledger = Arc::new(Ledger::default());
    let activations = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(RuntimeState::default());
    let fragment = Arc::new(TaskContext::new(
        None,
        runtime.clone(),
        Some((39001, 39002)),
        None,
        None,
        None,
    ));
    let completion = DriverGroupCompletion::new(1, runtime.error_state());
    let task = DriverRunner::new(
        PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                CountMergeSource {
                    handle: prepare_final(),
                    ledger: ledger.clone(),
                    activations: activations.clone(),
                    drops: drops.clone(),
                },
            ))],
            None,
            Vec::new(),
            runtime,
            Some((39001, 39002)),
        ),
        completion.clone(),
        fragment,
        Duration::from_millis(10),
    );
    assert!(executor.submit(vec![task]));
    let error = completion
        .wait_timeout(
            Duration::from_secs(5),
            novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new("COUNT host test timed out"),
            )
            .into(),
        )
        .unwrap_err();
    assert!(
        error
            .detail()
            .contains("panic in driver execution: attempt to add with overflow"),
        "{error}"
    );
    // stopped can precede destructor: observe the actual drop independently.
    let deadline = Instant::now() + Duration::from_secs(5);
    while drops.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(drops.load(Ordering::Acquire), 1);
    executor.shutdown().unwrap();
    assert_eq!(activations.load(Ordering::Acquire), 1);
    assert_eq!(ledger.live.load(Ordering::Acquire), 0);
    assert_eq!(ledger.releases.load(Ordering::Acquire), 1);
}
