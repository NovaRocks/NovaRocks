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

// Child of the original carrier tests: borrow its genuine finite host ledger,
// actual immutable call and seven causes; do not create another wallet/fixture.
use super::*;
use crate::{
    InvocationScalarEvaluationInstance, InvocationScalarKernelInstance,
    PreparedInvocationScalarKernel, ScalarCallInput, ScalarInvocationActivation,
};
use arrow_array::{Array, Int64Array};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug)]
enum Behavior {
    Data,
    Kernel(KernelFailure),
}
#[derive(Debug)]
struct Prepared {
    call: Arc<ScalarCallContract>,
    behavior: Behavior,
    enters: Arc<AtomicUsize>,
}
struct Instance {
    call: Arc<ScalarCallContract>,
    behavior: Behavior,
    enters: Arc<AtomicUsize>,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
}
impl PreparedInvocationScalarKernel for Prepared {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.call
    }
    fn instance_retained_upper_bound(&self) -> usize {
        HostAggregateAllocator::metadata_allocation_bytes()
    }
    fn instance_inline_allocation_bytes(&self) -> usize {
        std::mem::size_of::<Instance>()
    }
    fn create_instance_with_allocator(
        &self,
        host: Arc<dyn AggregateStateAllocator>,
    ) -> Result<Box<dyn InvocationScalarKernelInstance>, KernelFailure> {
        let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
        Ok(Box::new(Instance {
            call: self.call.clone(),
            behavior: self.behavior.clone(),
            enters: self.enters.clone(),
            host,
            allocator,
        }))
    }
}
impl InvocationScalarKernelInstance for Instance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, ScalarInvocationFailure> {
        self.enters.fetch_add(1, Ordering::SeqCst);
        match &self.behavior {
            Behavior::Kernel(cause) => {
                control.checkpoint(17)?;
                Err(cause.clone().into())
            }
            Behavior::Data => {
                // This is an explicit diagnostic-fixture source bound, not an
                // ARRAY output/formatter bound or production default capacity.
                let scope =
                    OpaqueRetainedCharge::try_new(self.host.clone())?.reserve_operation(8192)?;
                let mut work = EvaluationCheckpoints::new(control);
                let slot = ScalarDataSlot::prepare_for_actual_invocation(
                    self.call.clone(),
                    input.selection(),
                    &self.allocator,
                    scope,
                    &mut work,
                )?;
                work.flush()?;
                let original = "original whole scalar µ diagnostic: ".repeat(101);
                assert!(original.capacity() <= 8192);
                control.checkpoint(19)?;
                Err(ScalarInvocationFailure::Data(slot.publish_original(
                    crate::DataFailureReason::InvalidValue,
                    original,
                )))
            }
        }
    }
    fn retained_bytes(&self) -> usize {
        self.allocator.metadata_bytes()
    }
}
fn prepared(
    behavior: Behavior,
    enters: &Arc<AtomicUsize>,
) -> Arc<dyn PreparedInvocationScalarKernel> {
    Arc::new(Prepared {
        call: contract(),
        behavior,
        enters: enters.clone(),
    })
}
#[test]
fn scalar_invocation_lifecycle_full_data_has_no_footer_latches_and_retains_actual_grant() {
    let host = Host::with_limit(128 * 1024);
    let enters = Arc::new(AtomicUsize::new(0));
    let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(Behavior::Data, &enters),
        Some(host.clone()),
    )
    .unwrap();
    let input: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![Some(42)]));
    let arguments = [crate::EvaluatedArgument::Column(&input)];
    let control = Control::default();
    let result = instance.evaluate(
        Selection::all(1),
        &arguments,
        ScalarInvocationActivation::Activated,
        &control,
    );
    let Err(ScalarInvocationFailure::Data(data)) = result else {
        panic!("full original Data");
    };
    assert_eq!(
        data.message(),
        "original whole scalar µ diagnostic: ".repeat(101)
    );
    assert!(data.message().len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    let count = control.trace.lock().unwrap().len();
    assert_eq!(control.trace.lock().unwrap().last(), Some(&19));
    assert_eq!(
        instance
            .evaluate(
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                &control
            )
            .unwrap_err(),
        ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert_eq!(control.trace.lock().unwrap().len(), count);
    assert_eq!(enters.load(Ordering::SeqCst), 1);
    drop(instance);
    assert!(host.ledger.lock().unwrap().opaque >= 8192);
    drop(data);
    host.released();
}
#[test]
fn scalar_invocation_lifecycle_all_seven_owner_causes_return_without_tail_or_replay() {
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        let enters = Arc::new(AtomicUsize::new(0));
        let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared(Behavior::Kernel(cause.clone()), &enters),
            Some(host.clone()),
        )
        .unwrap();
        let input: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![Some(42)]));
        let arguments = [crate::EvaluatedArgument::Column(&input)];
        let control = Control::default();
        assert_eq!(
            instance
                .evaluate(
                    Selection::all(1),
                    &arguments,
                    ScalarInvocationActivation::Activated,
                    &control
                )
                .unwrap_err(),
            ScalarInvocationFailure::Kernel(cause)
        );
        let count = control.trace.lock().unwrap().len();
        // Entry and exact argument validation are the only actual callbacks.
        assert_eq!(control.trace.lock().unwrap().last(), Some(&17));
        assert_eq!(
            instance
                .evaluate(
                    Selection::all(1),
                    &arguments,
                    ScalarInvocationActivation::Activated,
                    &control
                )
                .unwrap_err(),
            ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert_eq!(control.trace.lock().unwrap().len(), count);
        assert_eq!(enters.load(Ordering::SeqCst), 1);
        drop(instance);
        host.released();
    }
}
#[test]
fn scalar_invocation_lifecycle_actual_empty_activation_is_distinct_from_validation_only() {
    for activated in [false, true] {
        let host = Host::with_limit(128 * 1024);
        let enters = Arc::new(AtomicUsize::new(0));
        let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared(Behavior::Data, &enters),
            Some(host.clone()),
        )
        .unwrap();
        let input: arrow_array::ArrayRef = Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
        let arguments = [crate::EvaluatedArgument::Column(&input)];
        let activation = if activated {
            ScalarInvocationActivation::Activated
        } else {
            ScalarInvocationActivation::ValidateOnly
        };
        let result = instance.evaluate(
            Selection::all(0),
            &arguments,
            activation,
            &Control::default(),
        );
        if activated {
            assert!(
                matches!(&result, Err(ScalarInvocationFailure::Data(data)) if data.source_len()==0)
            );
            assert_eq!(enters.load(Ordering::SeqCst), 1);
        } else {
            assert!(matches!(&result, Ok(out) if out.values().is_empty()));
            assert_eq!(enters.load(Ordering::SeqCst), 0);
        }
        drop(result);
        drop(instance);
        host.released();
    }
}
#[test]
fn scalar_invocation_lifecycle_missing_host_refuses_before_instance_creation() {
    let enters = Arc::new(AtomicUsize::new(0));
    let result = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(Behavior::Data, &enters),
        None,
    );
    assert!(matches!(result, Err(KernelFailure::InvalidProgram(_))));
    assert_eq!(enters.load(Ordering::SeqCst), 0);
}

#[test]
fn scalar_invocation_lifecycle_actual_constructor_host_refusal_keeps_each_origin() {
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        *host.opaque_refusal.lock().unwrap() = Some(cause.clone());
        let enters = Arc::new(AtomicUsize::new(0));
        let result = InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared(Behavior::Data, &enters),
            Some(host.clone()),
        );
        assert!(matches!(result, Err(ref actual) if actual == &cause));
        assert_eq!(enters.load(Ordering::SeqCst), 0);
        host.released();
    }
}
#[test]
fn scalar_invocation_lifecycle_every_actual_callback_cause_has_no_tail() {
    let host = Host::with_limit(128 * 1024);
    let enters = Arc::new(AtomicUsize::new(0));
    let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
        prepared(Behavior::Data, &enters),
        Some(host.clone()),
    )
    .unwrap();
    let input: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![Some(42)]));
    let arguments = [crate::EvaluatedArgument::Column(&input)];
    let control = Control::default();
    let result = instance.evaluate(
        Selection::all(1),
        &arguments,
        ScalarInvocationActivation::Activated,
        &control,
    );
    assert!(matches!(result, Err(ScalarInvocationFailure::Data(_))));
    let callbacks = control.trace.lock().unwrap().len();
    drop(result);
    drop(instance);
    host.released();
    for stop in 0..callbacks {
        for cause in causes() {
            let host = Host::with_limit(128 * 1024);
            let enters = Arc::new(AtomicUsize::new(0));
            let mut instance = InvocationScalarEvaluationInstance::instantiate_with_allocator(
                prepared(Behavior::Data, &enters),
                Some(host.clone()),
            )
            .unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let result = instance.evaluate(
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                &control,
            );
            assert!(
                matches!(result, Err(ScalarInvocationFailure::Kernel(ref actual)) if actual == &cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            drop(result);
            drop(instance);
            host.released();
        }
    }
}
#[test]
fn scalar_invocation_lifecycle_atomic_effect_forbids_replay_earlier_and_boolean_reorder() {
    let effects = novarocks_type_contract::ExpressionEffects {
        may_raise_invocation_data: true,
        ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
    };
    assert!(!effects.permits_boolean_reordering());
    assert!(!effects.permits_same_row_kernel_replay());
    assert!(!effects.permits_earlier_evaluation());
    assert!(
        novarocks_type_contract::ExpressionEffects::PURE_VALUE
            .join(effects)
            .may_raise_invocation_data
    );
    assert!(crate::PureKernelAbi::ScalarInvocationV1.may_raise_invocation_data());
    assert!(!crate::PureKernelAbi::ScalarV1.may_raise_invocation_data());
}
